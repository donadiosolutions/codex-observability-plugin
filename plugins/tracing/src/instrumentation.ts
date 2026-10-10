import { createHash, randomBytes } from "node:crypto";

import { LangfuseSpanProcessor } from "@langfuse/otel";
import { setLangfuseTracerProvider } from "@langfuse/tracing";
import { ExportResultCode, type ExportResult } from "@opentelemetry/core";
import { JsonTraceSerializer } from "@opentelemetry/otlp-transformer";
import {
  defaultResource,
  detectResources,
  envDetector,
  resourceFromAttributes,
} from "@opentelemetry/resources";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import type { ReadableSpan, SpanExporter, SpanProcessor } from "@opentelemetry/sdk-trace-base";

import type { Config } from "./config.js";

const TRACE_ID_HEX_CHARS = 32;
const SPAN_ID_HEX_CHARS = 16;
const LEGACY_UPLOADED = "langfuse.observation.metadata.codex.legacy_uploaded";

let idSeed: string | undefined;
let idCounter = 0;

export function seedIds(seed: string | undefined): void {
  idSeed = seed;
  idCounter = 0;
}

export function currentIdSeed(): string | undefined {
  return idSeed;
}

function seededId(kind: "trace" | "span"): string {
  const hexChars = kind === "trace" ? TRACE_ID_HEX_CHARS : SPAN_ID_HEX_CHARS;
  if (idSeed === undefined) return randomBytes(hexChars / 2).toString("hex");
  return createHash("sha256")
    .update(`${idSeed}:${kind}:${idCounter++}`)
    .digest("hex")
    .slice(0, hexChars);
}

export type Instrumentation = {
  /** Flush buffered spans and tear down the tracer provider. */
  shutdown: () => Promise<void>;
  /** Only server-acknowledged contents are eligible for a durable checkpoint. */
  acknowledged: () => Record<string, string>;
  flush: () => Promise<void>;
};

let activeFlush: (() => Promise<void>) | undefined;
let activeBackpressure: (() => Promise<void>) | undefined;

/** Await at conversion boundaries to keep preparation and delivery bounded. */
export async function flushInstrumentation(): Promise<void> {
  await activeFlush?.();
}

/** Wait for preparation, draining only full batches or an existing request. */
export async function backpressureInstrumentation(): Promise<void> {
  await activeBackpressure?.();
}

type DeliveryOptions = {
  acknowledged?: Readonly<Record<string, string>>;
  maxBatchBytes?: number;
  maxSingleSpanBytes?: number;
  maxQueueSpans?: number;
  maxQueueBytes?: number;
  flushIntervalMillis?: number;
};

type PendingSpan = { span: ReadableSpan; key: string; fingerprint: string; bytes: number };

function spanKey(span: ReadableSpan): string {
  const { traceId, spanId } = span.spanContext();
  return `${traceId}:${spanId}`;
}

function serialize(spans: ReadableSpan[]): Uint8Array {
  const encoded = JsonTraceSerializer.serializeRequest(spans);
  if (!encoded) throw new Error("OTLP serialization failed");
  return encoded;
}

/**
 * The SDK prepares spans (including masking and media) using its public
 * immediate exporter option. This exporter owns batching and acknowledgments:
 * a queue admission is never mistaken for successful delivery at shutdown.
 */
export class BoundedSpanExporter implements SpanExporter {
  private readonly queue: PendingSpan[] = [];
  private readonly delivered: Record<string, string>;
  private readonly received = new Set<string>();
  private readonly maxBatchBytes: number;
  private readonly maxSingleSpanBytes: number;
  private readonly maxQueueSpans: number;
  private readonly maxQueueBytes: number;
  private readonly flushIntervalMillis: number;
  private queuedBytes = 0;
  private inFlightCount = 0;
  private timer?: NodeJS.Timeout;
  private draining?: Promise<void>;
  private failure?: Error;
  private closed = false;
  private capacityFlush = false;

  constructor(
    private readonly config: Config,
    options: DeliveryOptions = {},
  ) {
    this.delivered = { ...options.acknowledged };
    this.maxBatchBytes = options.maxBatchBytes ?? 8 * 1024 * 1024;
    this.maxSingleSpanBytes = options.maxSingleSpanBytes ?? 256 * 1024 * 1024;
    this.maxQueueSpans = options.maxQueueSpans ?? 2048;
    this.maxQueueBytes = options.maxQueueBytes ?? 64 * 1024 * 1024;
    this.flushIntervalMillis = options.flushIntervalMillis ?? 1000;
  }

  acknowledged(): Record<string, string> {
    return { ...this.delivered };
  }

  receivedSpan(key: string): boolean {
    return this.received.has(key);
  }

  export(spans: ReadableSpan[], callback: (result: ExportResult) => void): void {
    try {
      if (this.closed) throw new Error("OTLP exporter is shut down");
      if (this.failure) throw this.failure;
      for (const span of spans) {
        const key = spanKey(span);
        this.received.add(key);
        const body = serialize([span]);
        // Process/resource attributes may change between hook processes. Hash
        // observation contents only; timestamps and IDs remain part of the hash.
        const request = JSON.parse(Buffer.from(body).toString("utf8"));
        const contents = request.resourceSpans.flatMap(
          (resource: { scopeSpans: unknown[] }) => resource.scopeSpans,
        );
        const fingerprint = createHash("sha256").update(JSON.stringify(contents)).digest("hex");
        if (this.delivered[key] === fingerprint) continue;
        if (body.byteLength > this.maxSingleSpanBytes) {
          throw new Error(`OTLP observation exceeds ${this.maxSingleSpanBytes} byte limit`);
        }
        const empty = this.queue.length === 0 && this.inFlightCount === 0;
        if (
          this.queue.length + this.inFlightCount >= this.maxQueueSpans ||
          (!empty && this.queuedBytes + body.byteLength > this.maxQueueBytes)
        ) {
          throw new Error("OTLP delivery queue capacity exceeded; observations remain retryable");
        }
        this.queue.push({ span, key, fingerprint, bytes: body.byteLength });
        this.queuedBytes += body.byteLength;
      }
      if (this.queue.length >= 16 || this.queuedBytes >= this.maxBatchBytes) {
        void this.drain();
      } else if (this.queue.length > 0 && !this.timer && !this.draining) {
        this.timer = setTimeout(() => {
          this.timer = undefined;
          void this.drain();
        }, this.flushIntervalMillis);
        this.timer.unref();
      }
      callback({ code: ExportResultCode.SUCCESS });
    } catch (error) {
      this.fail(error);
      callback({ code: ExportResultCode.FAILED, error: this.failure });
    }
  }

  private fail(error: unknown): void {
    this.failure ??= error instanceof Error ? error : new Error(String(error));
  }

  private drain(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (!this.draining) {
      this.draining = this.sendQueued()
        .catch((error) => this.fail(error))
        .finally(() => {
          this.draining = undefined;
        });
    }
    return this.draining;
  }

  private async sendQueued(): Promise<void> {
    while (this.queue.length > 0 && !this.failure) {
      const batch: PendingSpan[] = [this.queue[0]];
      let body = serialize(batch.map((entry) => entry.span));
      while (batch.length < 16 && batch.length < this.queue.length) {
        const candidate = [...batch, this.queue[batch.length]];
        const encoded = serialize(candidate.map((entry) => entry.span));
        if (encoded.byteLength > this.maxBatchBytes) break;
        batch.push(this.queue[batch.length]);
        body = encoded;
      }
      this.queue.splice(0, batch.length);
      this.inFlightCount = batch.length;
      try {
        await this.send(body);
        for (const entry of batch) this.delivered[entry.key] = entry.fingerprint;
      } finally {
        this.queuedBytes -= batch.reduce((total, entry) => total + entry.bytes, 0);
        this.inFlightCount = 0;
      }
    }
  }

  private async send(body: Uint8Array): Promise<void> {
    for (let attempt = 0; attempt < 2; attempt++) {
      const response = await fetch(
        `${this.config.base_url.replace(/\/$/, "")}/api/public/otel/v1/traces`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Basic ${Buffer.from(`${this.config.public_key}:${this.config.secret_key}`).toString("base64")}`,
            "x-langfuse-public-key": this.config.public_key ?? "",
            "x-langfuse-sdk-name": "codex-observability-plugin",
          },
          body: Buffer.from(body),
          signal: AbortSignal.timeout(5000),
        },
      );
      // Release the response before retrying; every request and retry is bounded.
      const text = await response.text();
      if (!response.ok) {
        if (attempt === 0 && (response.status === 429 || response.status >= 500)) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          continue;
        }
        throw new Error(`OTLP export rejected with HTTP ${response.status}`);
      }
      const result: unknown = text.length === 0 ? {} : JSON.parse(text);
      if (result === null || typeof result !== "object" || Array.isArray(result)) {
        throw new Error("Invalid OTLP export acknowledgment");
      }
      const partial = (result as { partialSuccess?: { rejectedSpans?: string | number } })
        .partialSuccess;
      if (partial?.rejectedSpans !== undefined && BigInt(partial.rejectedSpans) !== 0n) {
        throw new Error(`OTLP partial success rejected ${partial.rejectedSpans} observation(s)`);
      }
      return;
    }
  }

  async forceFlush(): Promise<void> {
    if (this.failure) throw this.failure;
    if (
      this.capacityFlush &&
      !this.draining &&
      this.queuedBytes < this.maxBatchBytes &&
      this.queue.length < 16
    )
      return;
    await this.drain();
    if (this.failure) throw this.failure;
  }

  /** SDK forceFlush waits for preparation; avoid sending tiny batches each time. */
  async withBackpressure(prepare: () => Promise<void>): Promise<void> {
    this.capacityFlush = true;
    try {
      await prepare();
    } finally {
      this.capacityFlush = false;
    }
  }

  async shutdown(): Promise<void> {
    this.closed = true;
    await this.forceFlush();
  }
}

const DEFAULT_SERVICE_NAME = "codex";

function buildResource() {
  return defaultResource()
    .merge(resourceFromAttributes({ "service.name": DEFAULT_SERVICE_NAME }))
    .merge(detectResources({ detectors: [envDetector] }));
}

/**
 * Configure an isolated OpenTelemetry tracer provider wired to Langfuse.
 *
 * We register a dedicated `NodeTracerProvider` (rather than the full auto-
 * instrumenting `NodeSDK`) so the bundle stays small and free of dynamic
 * instrumentation loading. Registering the provider also installs the
 * AsyncLocalStorage context manager that `propagateAttributes` relies on.
 *
 * The SDK prepares observations in immediate mode; our acknowledged exporter
 * batches the resulting spans by count, exact encoded size, and a short timer.
 * Shutdown must succeed before callers persist observation checkpoints.
 */
export function setupInstrumentation(
  config: Config,
  options: DeliveryOptions = {},
): Instrumentation {
  const exporter = new BoundedSpanExporter(config, options);
  const ended = new Set<string>();
  const spanProcessor = new LangfuseSpanProcessor({
    publicKey: config.public_key,
    secretKey: config.secret_key,
    baseUrl: config.base_url,
    environment: config.environment,
    exportMode: "immediate",
    exporter,
    shouldExportSpan: ({ otelSpan }) => otelSpan.attributes[LEGACY_UPLOADED] !== "true",
  });

  // Langfuse logs and swallows asynchronous preparation failures. Count spans
  // before preparation and demand that each reached the acknowledged exporter.
  const checkedProcessor: SpanProcessor = {
    onStart: (span, context) => spanProcessor.onStart(span, context),
    onEnd: (span) => {
      if (span.attributes[LEGACY_UPLOADED] !== "true") ended.add(spanKey(span));
      spanProcessor.onEnd(span);
    },
    forceFlush: async () => {
      await spanProcessor.forceFlush();
      await exporter.forceFlush();
      for (const key of ended) {
        if (!exporter.receivedSpan(key))
          throw new Error("Langfuse observation preparation failed before export");
      }
    },
    shutdown: () => spanProcessor.shutdown(),
  };

  const provider = new NodeTracerProvider({
    resource: buildResource(),
    spanProcessors: [checkedProcessor],
    idGenerator: {
      generateTraceId: () => seededId("trace"),
      generateSpanId: () => seededId("span"),
    },
  });
  provider.register();

  // Bind explicitly: the global registry can refuse the registration silently.
  setLangfuseTracerProvider(provider);
  activeFlush = () => checkedProcessor.forceFlush();
  activeBackpressure = () => exporter.withBackpressure(() => checkedProcessor.forceFlush());

  return {
    acknowledged: () => exporter.acknowledged(),
    flush: () => checkedProcessor.forceFlush(),
    shutdown: async () => {
      try {
        await checkedProcessor.forceFlush();
      } finally {
        try {
          await provider.shutdown();
        } finally {
          setLangfuseTracerProvider(null);
          activeFlush = undefined;
          activeBackpressure = undefined;
        }
      }
    },
  };
}
