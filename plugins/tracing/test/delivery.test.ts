import * as http from "node:http";

import { getLangfuseTracerProvider } from "@langfuse/tracing";
import { ExportResultCode } from "@opentelemetry/core";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import type { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { afterEach, describe, expect, it } from "vitest";

import { emitHookObservation } from "../src/trace.js";
import type { Config } from "../src/config.js";
import {
  backpressureInstrumentation,
  BoundedSpanExporter,
  setupInstrumentation,
} from "../src/instrumentation.js";

const config: Config = {
  enabled: true,
  public_key: "pk-test",
  secret_key: "sk-test",
  base_url: "http://127.0.0.1",
  skill_tags: true,
  debug: false,
  fail_on_error: false,
};
const servers: http.Server[] = [];
const providers: NodeTracerProvider[] = [];
const exporters: BoundedSpanExporter[] = [];

async function endpoint(
  respond: (request: number, body: Buffer, res: http.ServerResponse) => void,
) {
  let requests = 0;
  const server = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    respond(++requests, Buffer.concat(chunks), res);
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as { port: number };
  return { ...config, base_url: `http://127.0.0.1:${address.port}` };
}

function spans(count: number, payload = "x"): ReadableSpan[] {
  const captured: ReadableSpan[] = [];
  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({ "service.name": "test" }),
    spanProcessors: [
      {
        onStart() {},
        onEnd(span) {
          captured.push(span);
        },
        async forceFlush() {},
        async shutdown() {},
      },
    ],
  });
  providers.push(provider);
  const tracer = provider.getTracer("test");
  for (let i = 0; i < count; i++) tracer.startSpan(`span-${i}`, { attributes: { payload } }).end();
  return captured;
}

function enqueue(exporter: BoundedSpanExporter, span: ReadableSpan) {
  exporter.export([span], (result) => expect(result.code).toBe(ExportResultCode.SUCCESS));
}

function spanCount(body: Buffer): number {
  const request = JSON.parse(body.toString());
  return request.resourceSpans.reduce(
    (n: number, resource: { scopeSpans: { spans: unknown[] }[] }) =>
      n + resource.scopeSpans.reduce((m, scope) => m + scope.spans.length, 0),
    0,
  );
}

afterEach(async () => {
  for (const exporter of exporters.splice(0)) await exporter.shutdown().catch(() => {});
  for (const provider of providers.splice(0)) await provider.shutdown();
  await Promise.all(
    servers
      .splice(0)
      .map((server) => new Promise<void>((resolve) => server.close(() => resolve()))),
  );
});

describe("bounded acknowledged delivery", () => {
  it("does not report success when an automatic batch was rejected with HTTP 413", async () => {
    let accepted = 0;
    const cfg = await endpoint((request, body, res) => {
      if (request === 1) {
        res.writeHead(413);
        res.end();
        return;
      }
      accepted += spanCount(body);
      res.end("{}");
    });
    const instrumentation = setupInstrumentation(cfg);
    const tracer = getLangfuseTracerProvider().getTracer("delivery-test");
    for (let i = 0; i < 600; i++) tracer.startSpan(`span-${i}`).end();
    await expect(instrumentation.shutdown()).rejects.toThrow("413");
    expect(accepted).toBeLessThan(600);
  });

  it("delivers all 600 observations in sequential batches of at most 16", async () => {
    const sizes: number[] = [];
    let active = 0;
    let maximumActive = 0;
    const cfg = await endpoint((_request, body, res) => {
      active++;
      maximumActive = Math.max(active, maximumActive);
      sizes.push(spanCount(body));
      setTimeout(() => {
        active--;
        res.end("{}");
      }, 1);
    });
    const exporter = new BoundedSpanExporter(cfg);
    exporters.push(exporter);
    for (const span of spans(600)) enqueue(exporter, span);
    await exporter.forceFlush();
    expect(sizes.reduce((a, b) => a + b, 0)).toBe(600);
    expect(Math.max(...sizes)).toBeLessThanOrEqual(16);
    expect(maximumActive).toBe(1);
    expect(Object.keys(exporter.acknowledged())).toHaveLength(600);
  });

  it("batches by actual JSON bytes including escaping and the OTLP envelope", async () => {
    const bytes: number[] = [];
    const cfg = await endpoint((_request, body, res) => {
      bytes.push(body.length);
      res.end("{}");
    });
    const exporter = new BoundedSpanExporter(cfg, { maxBatchBytes: 3000 });
    exporters.push(exporter);
    for (const span of spans(8, '\\"'.repeat(150))) enqueue(exporter, span);
    await exporter.forceFlush();
    expect(bytes.length).toBeGreaterThan(1);
    expect(Math.max(...bytes)).toBeLessThanOrEqual(3000);
    expect(Object.keys(exporter.acknowledged())).toHaveLength(8);
  });

  it("permits an untruncated singleton larger than the regular batch target", async () => {
    let received: Buffer | undefined;
    const cfg = await endpoint((_request, body, res) => {
      received = body;
      res.end("{}");
    });
    const exporter = new BoundedSpanExporter(cfg, {
      maxBatchBytes: 1024,
      maxSingleSpanBytes: 8192,
    });
    exporters.push(exporter);
    enqueue(exporter, spans(1, "x".repeat(2048))[0]);
    await exporter.forceFlush();
    expect(received!.length).toBeGreaterThan(1024);
    expect(received!.toString()).toContain("x".repeat(2048));
  });

  it("rejects shutdown after queue overflow without acknowledging lost observations", async () => {
    const cfg = await endpoint((_request, _body, res) => res.end("{}"));
    const exporter = new BoundedSpanExporter(cfg, { maxQueueSpans: 2 });
    exporters.push(exporter);
    for (const span of spans(3)) exporter.export([span], () => {});
    await expect(exporter.shutdown()).rejects.toThrow("queue");
    expect(Object.keys(exporter.acknowledged())).toHaveLength(0);
  });

  it("rejects OTLP partial success and malformed successful responses", async () => {
    for (const response of ['{"partialSuccess":{"rejectedSpans":"1"}}', "not-json"]) {
      const cfg = await endpoint((_request, _body, res) => res.end(response));
      const exporter = new BoundedSpanExporter(cfg);
      exporters.push(exporter);
      enqueue(exporter, spans(1)[0]);
      await expect(exporter.shutdown()).rejects.toThrow();
      expect(Object.keys(exporter.acknowledged())).toHaveLength(0);
    }
  });

  it("flushes a small batch on the interval without waiting for shutdown", async () => {
    let received = 0;
    const cfg = await endpoint((_request, body, res) => {
      received += spanCount(body);
      res.end("{}");
    });
    const exporter = new BoundedSpanExporter(cfg, { flushIntervalMillis: 20 });
    exporters.push(exporter);
    enqueue(exporter, spans(1)[0]);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(received).toBe(1);
  });

  it("skips acknowledged observations but uploads changed content using the same ID", async () => {
    let received = 0;
    const cfg = await endpoint((_request, body, res) => {
      received += spanCount(body);
      res.end("{}");
    });
    const [span] = spans(1);
    const first = new BoundedSpanExporter(cfg);
    exporters.push(first);
    enqueue(first, span);
    await first.shutdown();
    const second = new BoundedSpanExporter(cfg, { acknowledged: first.acknowledged() });
    exporters.push(second);
    enqueue(second, span);
    await second.forceFlush();
    expect(received).toBe(1);
    const movedProcess = Object.create(span) as ReadableSpan;
    Object.defineProperty(movedProcess, "resource", {
      value: resourceFromAttributes({ ...span.resource.attributes, "process.pid": 12345 }),
    });
    enqueue(second, movedProcess);
    await second.forceFlush();
    expect(received).toBe(1);
    const changed = Object.create(span) as ReadableSpan;
    Object.defineProperty(changed, "attributes", { value: { payload: "changed" } });
    enqueue(second, changed);
    await second.shutdown();
    expect(received).toBe(2);
  });

  it("applies backpressure across a backlog larger than the bounded queue", async () => {
    const counts: number[] = [];
    const cfg = await endpoint((_request, body, res) => {
      counts.push(spanCount(body));
      res.end("{}");
    });
    const instrumentation = setupInstrumentation(cfg, { maxQueueSpans: 32 });
    const tracer = getLangfuseTracerProvider().getTracer("test");
    for (let i = 0; i < 100; i++) {
      tracer.startSpan(`span-${i}`).end();
      await backpressureInstrumentation();
    }
    await instrumentation.shutdown();
    expect(counts.reduce((a, b) => a + b, 0)).toBe(100);
    expect(Math.max(...counts)).toBeLessThanOrEqual(16);
    expect(counts.length).toBeLessThan(10);
  });

  it("rejects observations above the singleton limit without sending or acknowledging them", async () => {
    let received = 0;
    const cfg = await endpoint((_request, _body, res) => {
      received++;
      res.end("{}");
    });
    const exporter = new BoundedSpanExporter(cfg, { maxSingleSpanBytes: 1024 });
    exporters.push(exporter);
    exporter.export(spans(1, "x".repeat(2048)), () => {});
    await expect(exporter.shutdown()).rejects.toThrow("byte limit");
    expect(received).toBe(0);
    expect(exporter.acknowledged()).toEqual({});
  });

  it("retries a transient server rejection once and preserves final failures", async () => {
    let requests = 0;
    const cfg = await endpoint((_request, _body, res) => {
      requests++;
      res.writeHead(503);
      res.end();
    });
    const exporter = new BoundedSpanExporter(cfg);
    exporters.push(exporter);
    enqueue(exporter, spans(1)[0]);
    await expect(exporter.shutdown()).rejects.toThrow("503");
    expect(requests).toBe(2);
    expect(exporter.acknowledged()).toEqual({});
  });

  it("suppresses explicitly acknowledged legacy spans without treating them as lost preparation", async () => {
    let received = 0;
    const cfg = await endpoint((_request, body, res) => {
      received += spanCount(body);
      res.end("{}");
    });
    const instrumentation = setupInstrumentation(cfg);
    const tracer = getLangfuseTracerProvider().getTracer("test");
    tracer
      .startSpan("legacy", {
        attributes: { "langfuse.observation.metadata.codex.legacy_uploaded": "true" },
      })
      .end();
    tracer.startSpan("new").end();
    await instrumentation.shutdown();
    expect(received).toBe(1);
    expect(Object.keys(instrumentation.acknowledged())).toHaveLength(1);
  });
});

it("exports both session attributes for an auto-ended standalone SessionStart event", async () => {
  const attributes: Array<{ key: string; value: { stringValue?: string } }> = [];
  const cfg = await endpoint((_request, body, res) => {
    const data = JSON.parse(body.toString()) as {
      resourceSpans: Array<{
        scopeSpans: Array<{ spans: Array<{ attributes: typeof attributes }> }>;
      }>;
    };
    for (const resource of data.resourceSpans)
      for (const scope of resource.scopeSpans)
        for (const span of scope.spans) attributes.push(...span.attributes);
    res.end("{}");
  });
  const instrumentation = setupInstrumentation(cfg);
  emitHookObservation({
    id: "standalone-session-start",
    timestamp: Date.UTC(2026, 5, 3, 12, 0, 0),
    standalone: true,
    input: { hook_event_name: "SessionStart", session_id: "new-session" },
  });
  await instrumentation.shutdown();
  expect(attributes.find((attribute) => attribute.key === "session.id")?.value.stringValue).toBe(
    "new-session",
  );
  expect(
    attributes.find((attribute) => attribute.key === "langfuse.session.id")?.value.stringValue,
  ).toBe("new-session");
});
