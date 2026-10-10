import { createHash } from "node:crypto";
import { createReadStream, type Dirent } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import { createInterface } from "node:readline";

import {
  createTraceId,
  LangfuseOtelSpanAttributes,
  propagateAttributes,
  startObservation,
  type LangfuseGenerationAttributes,
  type LangfuseObservation,
} from "@langfuse/tracing";
import { TraceFlags, type SpanContext } from "@opentelemetry/api";

import type { Config } from "./config.js";
import {
  currentIdSeed,
  seedIds,
  flushInstrumentation,
  backpressureInstrumentation,
} from "./instrumentation.js";
import { parseArgs, parseSession } from "./parse.js";
import { loadUploadedTurnIds, loadLegacyTurnIds } from "./sidecar.js";
import { skillsForToolCall, traceTags } from "./skills.js";
import type {
  EventMsgPayload,
  HookEvent,
  ModelStep,
  RolloutLine,
  SessionMeta,
  SystemPrompt,
  TokenUsage,
  ToolCall,
  ToolDefinition,
  Turn,
} from "./types.js";
import { debugLog, toText } from "./utils.js";

async function loadSession(file: string): Promise<RolloutLine[]> {
  const lines: RolloutLine[] = [];
  const input = createReadStream(file, { encoding: "utf-8" });
  for await (const raw of createInterface({ input, crlfDelay: Infinity })) {
    const trimmed = raw.trim();
    if (!trimmed) continue;
    try {
      lines.push(JSON.parse(trimmed) as RolloutLine);
    } catch {
      // skip malformed lines rather than aborting the whole upload
    }
  }
  return lines;
}

type SubagentRollout = {
  threadId: string;
  parentThreadId?: string;
  file: string;
  startTime: number;
  nickname?: string;
};

export type SubagentIndex = {
  byParent: Map<string, SubagentRollout[]>;
  byThread: Map<string, SubagentRollout>;
};

async function readSessionMeta(
  file: string,
): Promise<
  { threadId: string; parentThreadId?: string; startTime: number; nickname?: string } | undefined
> {
  let handle;
  try {
    handle = await fs.open(file, "r");
    const buffer = Buffer.alloc(64 * 1024);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const text = buffer.subarray(0, bytesRead).toString("utf-8");
    const newline = text.indexOf("\n");
    const line = newline === -1 ? text : text.slice(0, newline);
    const parsed = JSON.parse(line) as RolloutLine;
    if (parsed.type !== "session_meta") return undefined;
    const p = parsed.payload as {
      id?: string;
      parent_thread_id?: string | null;
      agent_nickname?: string | null;
      source?: { subagent?: { thread_spawn?: { agent_nickname?: string | null } } };
    };
    if (typeof p.id !== "string") return undefined;
    const ts = Date.parse(parsed.timestamp);
    const nickname = p.agent_nickname ?? p.source?.subagent?.thread_spawn?.agent_nickname;
    return {
      threadId: p.id,
      parentThreadId: typeof p.parent_thread_id === "string" ? p.parent_thread_id : undefined,
      startTime: Number.isFinite(ts) ? ts : 0,
      nickname: typeof nickname === "string" && nickname ? nickname : undefined,
    };
  } catch {
    return undefined;
  } finally {
    await handle?.close();
  }
}

export async function buildSubagentIndex(
  rolloutFile: string,
  options: { includeEarlierDays?: boolean } = {},
): Promise<SubagentIndex> {
  const rolloutDirectory = path.dirname(rolloutFile);
  const dated = /[\/]\d{4}[\/]\d{2}[\/]\d{2}$/.test(rolloutDirectory);
  const root = dated ? path.resolve(rolloutDirectory, "../../..") : rolloutDirectory;
  const fromDay = path.relative(root, path.dirname(rolloutFile));
  const bounded = !options.includeEarlierDays && /^\d{4}\/\d{2}\/\d{2}$/.test(fromDay);
  const index: SubagentIndex = { byParent: new Map(), byThread: new Map() };

  async function walk(dir: string, rel: string, recursive = true): Promise<void> {
    if (bounded && rel && rel < fromDay.slice(0, rel.length)) return;
    let entries: Dirent[];
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (!recursive) continue;
        await walk(full, rel ? `${rel}/${entry.name}` : entry.name);
        continue;
      }
      if (!entry.isFile() || !entry.name.endsWith(".jsonl")) continue;
      const meta = await readSessionMeta(full);
      if (!meta) continue;
      if (index.byThread.has(meta.threadId)) continue;
      const rollout: SubagentRollout = {
        threadId: meta.threadId,
        parentThreadId: meta.parentThreadId,
        file: full,
        startTime: meta.startTime,
        nickname: meta.nickname,
      };
      index.byThread.set(meta.threadId, rollout);
      if (!meta.parentThreadId) continue;
      index.byParent.set(meta.parentThreadId, [
        ...(index.byParent.get(meta.parentThreadId) ?? []),
        rollout,
      ]);
    }
  }

  await walk(root, "", dated);
  const codexHome = path.resolve(process.env.CODEX_HOME ?? path.join(os.homedir(), ".codex"));
  if (
    path.dirname(path.resolve(root)) === codexHome &&
    (path.basename(root) === "sessions" || path.basename(root) === "archived_sessions")
  ) {
    if (path.basename(root) !== "sessions") await walk(path.join(codexHome, "sessions"), "", true);
    if (path.basename(root) !== "archived_sessions")
      await walk(path.join(codexHome, "archived_sessions"), "", false);
  }
  return index;
}

function turnIndexByNickname(turns: Turn[]): Map<string, number> {
  const byNickname = new Map<string, number>();
  const ambiguous = new Set<string>();
  turns.forEach((turn, index) => {
    for (const tc of turn.steps.flatMap((s) => s.toolCalls)) {
      if (tc.name !== "spawn_agent" || tc.output == null) continue;
      const out = parseArgs(toText(tc.output));
      const nickname =
        out !== null && typeof out === "object"
          ? (out as { nickname?: unknown }).nickname
          : undefined;
      if (typeof nickname !== "string" || !nickname) continue;
      if (byNickname.has(nickname) && byNickname.get(nickname) !== index) {
        ambiguous.add(nickname);
      }
      byNickname.set(nickname, index);
    }
  });
  for (const nickname of ambiguous) byNickname.delete(nickname);
  return byNickname;
}

function turnIndexAt(turns: Turn[], startTime: number): number {
  const running = turns.findIndex((t) => startTime >= t.startTime && startTime <= t.endTime);
  if (running !== -1) return running;
  let last = 0;
  for (let i = 0; i < turns.length; i++) {
    if (turns[i].startTime <= startTime) last = i;
  }
  return last;
}

/**
 * Placeholder parent span id used to pin a deterministic trace id on a root
 * span (the pattern the Langfuse SDK documents for custom trace ids). The id
 * never exists as a real span, so Langfuse still renders the turn as the
 * trace root.
 */
const SEED_PARENT_SPAN_ID = "0123456789abcdef";

/**
 * Derive the deterministic trace id for a turn from `config.trace_seed`.
 *
 * Main-thread turn N (1-based, rollout order):  createTraceId(`${seed}:${N}`)
 * Subagent-thread turn N:                       createTraceId(`${seed}:${threadId}:${N}`)
 *
 * The main-thread form deliberately excludes the thread id so external systems
 * can precompute trace ids (hex(sha256(seed)).slice(0, 32)) before the Codex
 * thread exists. Returns `undefined` (auto-generated ids) when no seed is set
 * or derivation fails — the hook must never block an upload.
 */
async function seededTraceParent(
  config: Config,
  sessionMeta: SessionMeta,
  turnNumber: number,
): Promise<SpanContext | undefined> {
  if (!config.trace_seed) return undefined;
  try {
    const seed = sessionMeta.isSubagentThread
      ? `${config.trace_seed}:${sessionMeta.sessionId}:${turnNumber}`
      : `${config.trace_seed}:${turnNumber}`;
    return {
      traceId: await createTraceId(seed),
      spanId: SEED_PARENT_SPAN_ID,
      traceFlags: TraceFlags.SAMPLED,
      isRemote: true,
    };
  } catch (error) {
    debugLog("failed to derive seeded trace id; falling back to auto-generated:", error);
    if (config.fail_on_error) throw error;
    return undefined;
  }
}

function isTokenCount(value: number | undefined): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function toUsageDetails(
  usage: TokenUsage | undefined,
): LangfuseGenerationAttributes["usageDetails"] {
  if (!usage) return undefined;
  const {
    input_tokens: input,
    output_tokens: output,
    total_tokens: total,
    cached_input_tokens: cached,
    reasoning_output_tokens: reasoning,
  } = usage;

  if (
    !isTokenCount(input) ||
    !isTokenCount(output) ||
    !isTokenCount(total) ||
    total !== input + output
  ) {
    debugLog("dropping usage: missing or inconsistent token counts", usage);
    return undefined;
  }
  if (
    (cached !== undefined && (!isTokenCount(cached) || cached > input)) ||
    (reasoning !== undefined && (!isTokenCount(reasoning) || reasoning > output))
  ) {
    debugLog("dropping usage: implausible cached/reasoning details", usage);
    return undefined;
  }

  // The runtime supports this documented shape, but the SDK type still
  // exposes only its legacy camelCase usage interface.
  return {
    prompt_tokens: input,
    completion_tokens: output,
    total_tokens: total,
    ...(cached !== undefined ? { prompt_tokens_details: { cached_tokens: cached } } : {}),
    ...(reasoning !== undefined
      ? { completion_tokens_details: { reasoning_tokens: reasoning } }
      : {}),
  } as unknown as LangfuseGenerationAttributes["usageDetails"];
}

function buildGenerationOutput(step: ModelStep): Record<string, unknown> | undefined {
  const output: Record<string, unknown> = {};
  if (step.text) output.content = step.text;
  if (step.reasoning) output.reasoning = step.reasoning;
  if (step.toolCalls.length > 0) {
    output.tool_calls = step.toolCalls.map((tc) => ({
      id: tc.callId,
      name: tc.name,
      arguments: tc.args,
    }));
  }
  return Object.keys(output).length > 0 ? output : undefined;
}

function toolObservationName(tc: ToolCall): string {
  const skill = skillsForToolCall(tc)[0];
  if (skill) return `skill:${skill}`;
  if (tc.mcp) return `${tc.mcp.server}.${tc.mcp.tool}`;
  return tc.name || "tool";
}

function systemPromptText(systemPrompt: SystemPrompt | undefined): string | undefined {
  if (!systemPrompt) return undefined;
  const segments = [
    systemPrompt.baseInstructions,
    ...systemPrompt.developerMessages,
    ...systemPrompt.injectedContext,
  ].filter((segment): segment is string => typeof segment === "string" && segment.length > 0);
  return segments.length > 0 ? segments.join("\n\n") : undefined;
}

function systemPromptMetadata(systemPrompt: SystemPrompt): Record<string, unknown> {
  const baseChars = systemPrompt.baseInstructions?.length ?? 0;
  const developerChars = systemPrompt.developerMessages.reduce((n, m) => n + m.length, 0);
  const injectedChars = systemPrompt.injectedContext.reduce((n, m) => n + m.length, 0);
  return {
    "codex.system_prompt.total_chars": baseChars + developerChars + injectedChars,
    "codex.system_prompt.base_instructions_chars": baseChars,
    "codex.system_prompt.developer_chars": developerChars,
    "codex.system_prompt.developer_message_count": systemPrompt.developerMessages.length,
    "codex.system_prompt.injected_context_chars": injectedChars,
    "codex.system_prompt.injected_context_count": systemPrompt.injectedContext.length,
    "codex.system_prompt.changed_this_turn": systemPrompt.changed,
  };
}

type ChatMlToolCall = {
  id: string;
  type: "function";
  function: { name: string; arguments?: string };
};
type ChatMlThinkingPart = { type: "thinking"; content: string };
type ChatMlMessage =
  | { role: "system"; content: string }
  | { role: "user"; content: string | ContentPart[] }
  | {
      role: "assistant";
      content?: string;
      thinking?: ChatMlThinkingPart[];
      tool_calls?: ChatMlToolCall[];
    }
  | { role: "tool"; tool_call_id: string; name: string; content: string; is_error?: true };

function assistantMessage(step: ModelStep): ChatMlMessage {
  return {
    role: "assistant",
    ...(step.text ? { content: step.text } : {}),
    ...(step.reasoning ? { thinking: [{ type: "thinking", content: step.reasoning }] } : {}),
    ...(step.toolCalls.length > 0
      ? {
          tool_calls: step.toolCalls.map((tc) => ({
            id: tc.callId,
            type: "function" as const,
            function: {
              name: toolObservationName(tc),
              ...(tc.args !== undefined ? { arguments: toText(tc.args) } : {}),
            },
          })),
        }
      : {}),
  };
}

function toolMessages(step: ModelStep): ChatMlMessage[] {
  return step.toolCalls.map((tc) => ({
    role: "tool" as const,
    tool_call_id: tc.callId,
    name: toolObservationName(tc),
    content: tc.output != null ? toText(tc.output) : (tc.error ?? ""),
    ...(tc.error ? { is_error: true as const } : {}),
  }));
}

function turnHistoryMessages(turn: Turn): ChatMlMessage[] {
  const messages: ChatMlMessage[] = [];
  const user = userMessage(turn);
  if (user) messages.push(user);
  for (const step of turn.steps) {
    messages.push(assistantMessage(step));
    messages.push(...toolMessages(step));
  }
  return messages;
}

function generationInput(
  systemMessage: string | undefined,
  historyPrefix: ChatMlMessage[],
  turn: Turn,
  stepIndex: number,
): ChatMlMessage[] | undefined {
  const messages: ChatMlMessage[] = [];
  if (systemMessage) messages.push({ role: "system", content: systemMessage });
  messages.push(...historyPrefix);
  const user = userMessage(turn);
  if (user) messages.push(user);
  for (let j = 0; j < stepIndex; j++) {
    messages.push(assistantMessage(turn.steps[j]));
    messages.push(...toolMessages(turn.steps[j]));
  }
  return messages.length > 0 ? messages : undefined;
}

type ContentPart =
  { type: "text"; text: string } | { type: "image_url"; image_url: { url: string } };

function toMultimodalContent(
  text: string | undefined,
  images: readonly string[],
): string | ContentPart[] | undefined {
  if (images.length === 0) return text;
  return [
    ...(text ? [{ type: "text" as const, text }] : []),
    ...images.map((url) => ({ type: "image_url" as const, image_url: { url } })),
  ];
}

function userMessage(turn: Turn): ChatMlMessage | undefined {
  const content = toMultimodalContent(turn.userInput, turn.userImages);
  return content === undefined ? undefined : { role: "user", content };
}

function attachToolDefinitions(
  input: ChatMlMessage[] | undefined,
  tools: ToolDefinition[],
): unknown {
  if (!input || tools.length === 0) return input;
  const [first, ...rest] = input;
  return first ? [{ ...first, tools }, ...rest] : input;
}

function generationEnd(step: ModelStep): number {
  const firstToolCall = step.toolCalls.reduce<number | undefined>(
    (earliest, tc) => (earliest === undefined ? tc.startTime : Math.min(earliest, tc.startTime)),
    undefined,
  );
  return Math.max(step.startTime, Math.min(firstToolCall ?? step.endTime, step.endTime));
}

async function emitTurn(
  turn: Turn,
  sessionMeta: SessionMeta,
  ctx: {
    config: Config;
    rolloutFile: string;
    parentObservation?: LangfuseObservation;
    parentSpanContext?: SpanContext;
    attached?: boolean;
    subagentIndex: SubagentIndex;
    seenThreadIds: Set<string>;
    unannouncedSubagents?: SubagentRollout[];
    inheritableTurnIds?: ReadonlySet<string>;
    historyPrefix?: ChatMlMessage[];
    incremental?: boolean;
    hookEvents?: HookEvent[];
    final?: boolean;
    emittedHookIds?: Set<string>;
    legacyIdMode?: boolean;
    legacyCutoff?: number;
    legacyUploaded?: boolean;
    unchanged?: boolean;
    acknowledgedTurns?: Readonly<Record<string, string>>;
    pendingTurnFingerprints?: Record<string, string>;
  },
): Promise<void> {
  const isSubagent = sessionMeta.isSubagentThread === true || ctx.parentObservation != null;
  const skipPayloads = ctx.legacyUploaded || ctx.unchanged;

  const outerSeed = currentIdSeed();
  seedIds(`${sessionMeta.sessionId}:${turn.turnId ?? "no-turn-id"}`);

  const root = startObservation(
    isSubagent ? "Codex Subagent Turn" : "Codex Turn",
    {
      input: skipPayloads ? undefined : toMultimodalContent(turn.userInput, turn.userImages),
      output: skipPayloads ? undefined : turn.finalOutput,
      level: turn.aborted ? "WARNING" : undefined,
      statusMessage: turn.aborted ? "Turn interrupted by user" : undefined,
      metadata: {
        "codex.turn_id": turn.turnId,
        "codex.thread_id": sessionMeta.sessionId,
        "codex.model": turn.model,
        "codex.reasoning_effort": turn.reasoningEffort,
        "codex.model_provider": sessionMeta.modelProvider,
        "codex.cli_version": sessionMeta.cliVersion,
        "codex.aborted": turn.aborted,
        ...(skipPayloads ? { "codex.legacy_uploaded": true } : {}),
        ...(ctx.incremental ? { "codex.completed": ctx.final ?? turn.completed } : {}),
        "codex.tool_call_count": turn.steps.reduce((n, s) => n + s.toolCalls.length, 0),
        ...(ctx.attached && ctx.parentSpanContext
          ? {
              "codex.parent_trace_id": ctx.parentSpanContext.traceId,
              "codex.parent_span_id": ctx.parentSpanContext.spanId,
            }
          : {}),
        ...(turn.systemPrompt ? systemPromptMetadata(turn.systemPrompt) : {}),
        ...(turn.userImages.length ? { "codex.image_count": turn.userImages.length } : {}),
        ...(turn.toolDefinitions.length
          ? { "codex.tool_definition_count": turn.toolDefinitions.length }
          : {}),
      },
    },
    {
      asType: "agent",
      startTime: new Date(turn.startTime),
      parentSpanContext: ctx.parentObservation?.otelSpan.spanContext() ?? ctx.parentSpanContext,
    },
  );

  if (ctx.attached) {
    root.otelSpan.setAttribute(LangfuseOtelSpanAttributes.IS_APP_ROOT, false);
  }

  let failure: unknown;
  try {
    const systemMessage = skipPayloads ? undefined : systemPromptText(turn.systemPrompt);
    const historyPrefix = ctx.historyPrefix ?? [];
    const spawnObservations = new Map<string, LangfuseObservation>();

    let pending = 0;
    let previousStepFingerprint = createHash("sha256")
      .update(
        JSON.stringify({
          historyPrefix,
          systemPrompt: turn.systemPrompt,
          tools: turn.toolDefinitions,
          model: turn.model,
          userInput: turn.userInput,
          userImages: turn.userImages,
          config: ctx.config,
          parent: root.otelSpan.spanContext(),
        }),
      )
      .digest("hex");
    for (let i = 0; i < turn.steps.length; i++) {
      const step = turn.steps[i];
      const stepFingerprintKey = `${sessionMeta.sessionId}:${turn.turnId ?? turn.startTime}:step:${i}`;
      const stepFingerprint = createHash("sha256")
        .update(previousStepFingerprint)
        .update(JSON.stringify(step))
        .digest("hex");
      previousStepFingerprint = stepFingerprint;
      const skipStep =
        skipPayloads || ctx.acknowledgedTurns?.[stepFingerprintKey] === stepFingerprint;
      if (ctx.incremental && !ctx.legacyIdMode)
        seedIds(`${sessionMeta.sessionId}:${turn.turnId ?? turn.startTime}:step:${i}`);
      const generation = startObservation(
        isSubagent ? "LLM Subagent" : "LLM",
        {
          input: skipStep
            ? undefined
            : attachToolDefinitions(
                generationInput(systemMessage, historyPrefix, turn, i),
                turn.toolDefinitions,
              ),
          output: skipStep ? undefined : buildGenerationOutput(step),
          model: turn.model,
          ...(turn.reasoningEffort
            ? { modelParameters: { reasoning_effort: turn.reasoningEffort } }
            : {}),
          usageDetails: toUsageDetails(step.usage),
          metadata: {
            "codex.step_index": i,
            ...(skipStep ? { "codex.legacy_uploaded": true } : {}),
            "codex.reasoning_effort": turn.reasoningEffort,
          },
        },
        {
          asType: "generation",
          startTime: new Date(step.startTime),
          parentSpanContext: root.otelSpan.spanContext(),
        },
      );

      for (const tc of step.toolCalls) {
        if (ctx.incremental && !ctx.legacyIdMode)
          seedIds(`${sessionMeta.sessionId}:${turn.turnId ?? turn.startTime}:tool:${tc.callId}`);
        const observation = emitToolCall(tc, root, step.endTime, ctx.incremental, skipPayloads);
        if (tc.name === "spawn_agent") spawnObservations.set(tc.callId, observation);
        if (ctx.incremental && !skipStep) await backpressureInstrumentation();
        if (ctx.incremental && !skipStep && ++pending >= 16) {
          await flushInstrumentation();
          pending = 0;
        }
      }

      generation.end(new Date(generationEnd(step)));
      if (ctx.pendingTurnFingerprints)
        ctx.pendingTurnFingerprints[stepFingerprintKey] = stepFingerprint;
      if (ctx.incremental && !skipStep && ++pending >= 16) {
        await flushInstrumentation();
        pending = 0;
      }
    }

    for (const hook of ctx.hookEvents ?? []) {
      if (
        !hook.replayOnly &&
        hook.input.session_id === sessionMeta.sessionId &&
        hook.input.turn_id === turn.turnId &&
        !ctx.emittedHookIds?.has(hook.id)
      ) {
        emitHookObservation(hook, root.otelSpan.spanContext());
        ctx.emittedHookIds?.add(hook.id);
      }
    }

    const announced: SubagentRollout[] = [];
    for (const threadId of turn.subagentThreadIds) {
      const rollout = ctx.subagentIndex.byThread.get(threadId);
      if (!rollout) {
        debugLog(`subagent rollout not found for thread ${threadId}`);
        continue;
      }
      announced.push(rollout);
    }
    for (const sub of [...announced, ...(ctx.unannouncedSubagents ?? [])]) {
      if (ctx.seenThreadIds.has(sub.threadId)) continue;
      ctx.seenThreadIds.add(sub.threadId);
      const seedBeforeChild = currentIdSeed();
      await convertRollout(sub.file, {
        config: ctx.config,
        parentObservation: spawnObservations.get(turn.subagentSpawnCallIds[sub.threadId]) ?? root,
        subagentIndex: ctx.subagentIndex,
        seenThreadIds: ctx.seenThreadIds,
        ancestorTurnIds: ctx.inheritableTurnIds,
        incremental: ctx.incremental,
        hookEvents: ctx.hookEvents,
        emittedHookIds: ctx.emittedHookIds,
        legacyIdMode: ctx.legacyIdMode,
        legacyCutoff: ctx.legacyCutoff,
        acknowledgedTurns: ctx.acknowledgedTurns,
        pendingTurnFingerprints: ctx.pendingTurnFingerprints,
      });
      seedIds(seedBeforeChild);
    }
  } catch (error) {
    failure = error;
    debugLog(`failed to convert turn ${turn.turnId ?? "(no turn id)"}:`, error);
    root.update({
      level: "ERROR",
      statusMessage: `Trace conversion failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    });
  }

  root.end(new Date(turn.endTime));
  seedIds(outerSeed);
  if (failure && (ctx.incremental || ctx.config.fail_on_error)) throw failure;
}

function emitToolCall(
  tc: ToolCall,
  parent: LangfuseObservation,
  fallbackEnd: number,
  incremental?: boolean,
  legacyUploaded?: boolean,
): LangfuseObservation {
  const tool = startObservation(
    toolObservationName(tc),
    {
      input: legacyUploaded ? undefined : tc.args,
      output: legacyUploaded ? undefined : tc.output != null ? toText(tc.output) : undefined,
      level: tc.error ? "ERROR" : undefined,
      statusMessage: tc.error,
      metadata: {
        ...(legacyUploaded ? { "codex.legacy_uploaded": true } : {}),
        "codex.call_id": tc.callId,
        "codex.tool_name": tc.name || "tool",
        ...(incremental ? { "codex.completed": tc.endTime != null } : {}),
      },
    },
    {
      asType: "tool",
      startTime: new Date(tc.startTime),
      parentSpanContext: parent.otelSpan.spanContext(),
    },
  );
  tool.end(new Date(tc.endTime ?? fallbackEnd));
  return tool;
}

function isFinal(
  turn: Turn,
  stoppedTurnId: string | undefined,
  supersededByLaterTurn: boolean,
): turn is Turn & { turnId: string } {
  if (turn.turnId == null) return false;
  return turn.completed || supersededByLaterTurn || turn.turnId === stoppedTurnId;
}

async function readTurnIds(file: string): Promise<Set<string>> {
  const ids = new Set<string>();
  for (const line of await loadSession(file)) {
    if (line.type !== "event_msg") continue;
    const p = line.payload as EventMsgPayload;
    if (p.type === "task_started" && typeof p.turn_id === "string") ids.add(p.turn_id);
  }
  return ids;
}

async function ancestorTurnIdsOf(
  sessionMeta: SessionMeta,
  index: SubagentIndex,
): Promise<Set<string>> {
  const owned = new Set<string>();
  const seen = new Set<string>([sessionMeta.sessionId]);
  let ancestor = sessionMeta.parentThreadId;
  while (ancestor && !seen.has(ancestor)) {
    seen.add(ancestor);
    const rollout = index.byThread.get(ancestor);
    if (rollout) {
      try {
        const before = owned.size;
        for (const id of await readTurnIds(rollout.file)) owned.add(id);
        debugLog(`ancestor ${ancestor} owns ${owned.size - before} turn(s)`);
      } catch (error) {
        debugLog(`failed to read ancestor ${ancestor}; not skipping its turns:`, error);
        break;
      }
    }
    ancestor = rollout?.parentThreadId;
  }
  return owned;
}

export async function convertRollout(
  rolloutFile: string,
  options: {
    config: Config;
    parentObservation?: LangfuseObservation;
    parentSpanContext?: SpanContext;
    subagentIndex?: SubagentIndex;
    seenThreadIds?: Set<string>;
    ancestorTurnIds?: ReadonlySet<string>;
    stoppedTurnId?: string;
    incremental?: boolean;
    hookEvents?: HookEvent[];
    emittedHookIds?: Set<string>;
    legacyIdMode?: boolean;
    legacyCutoff?: number;
    legacyUploaded?: boolean;
    unchanged?: boolean;
    acknowledgedTurns?: Readonly<Record<string, string>>;
    pendingTurnFingerprints?: Record<string, string>;
  },
): Promise<string[]> {
  const { sessionMeta, turns } = parseSession(await loadSession(rolloutFile));
  const emittedHookIds = options.emittedHookIds ?? new Set<string>();
  const legacyTurnIds = options.incremental
    ? await loadLegacyTurnIds(rolloutFile)
    : new Set<string>();
  const hooks = (options.hookEvents ?? []).filter(
    (hook) => hook.input.session_id === sessionMeta.sessionId,
  );
  for (const hook of hooks) {
    hook.contextNeeded = false;
    const turn = hook.input.turn_id
      ? turns.find((t) => t.turnId === hook.input.turn_id)
      : turns.filter((candidate) => candidate.startTime <= hook.timestamp).at(-1);
    if (!turn) {
      hook.contextNeeded = [
        "PostToolUse",
        "Stop",
        "SubagentStop",
        "Interrupt",
        "SessionEnd",
      ].includes(hook.input.hook_event_name ?? "");
      continue;
    }
    hook.input.turn_id ??= turn.turnId;
    if (
      hook.input.hook_event_name === "Stop" ||
      hook.input.hook_event_name === "Interrupt" ||
      hook.input.hook_event_name === "SessionEnd" ||
      (hook.input.hook_event_name === "SubagentStop" &&
        (sessionMeta.isSubagentThread || options.parentObservation != null))
    ) {
      hook.contextNeeded = !turn.completed;
      if (!turn.completed) turn.endTime = Math.max(turn.endTime, hook.timestamp);
      turn.completed = true;
      turn.aborted ||= hook.input.hook_event_name === "Interrupt";
      if (
        typeof hook.input.last_assistant_message === "string" &&
        turn.finalOutput !== hook.input.last_assistant_message
      ) {
        turn.finalOutput = hook.input.last_assistant_message;
        hook.contextNeeded = true;
      }
    }
    // Native hooks run before the rollout writer necessarily flushes tool outputs.
    const toolId = hook.input.tool_use_id ?? hook.input.tool_call_id;
    if (hook.input.hook_event_name === "PostToolUse" && toolId) {
      const tool = turn.steps.flatMap((step) => step.toolCalls).find((tc) => tc.callId === toolId);
      if (!tool) hook.contextNeeded = true;
      if (tool && tool.output == null && hook.input.tool_response !== undefined) {
        hook.contextNeeded = true;
        tool.output = hook.input.tool_response;
        tool.endTime = hook.timestamp;
        turn.endTime = Math.max(turn.endTime, hook.timestamp);
      }
    }
  }

  const historyPrefixes: ChatMlMessage[][] = [];
  const turnFingerprints = turns.map((turn) =>
    createHash("sha256")
      .update(
        JSON.stringify({
          turn,
          config: options.config,
          parent: options.parentObservation?.otelSpan.spanContext() ?? options.parentSpanContext,
        }),
      )
      .digest("hex"),
  );
  for (let i = 0; i < turns.length; i++) {
    // Include earlier conversation changes without rebuilding every generation input.
    if (i > 0)
      turnFingerprints[i] = createHash("sha256")
        .update(`${turnFingerprints[i - 1]}:${turnFingerprints[i]}`)
        .digest("hex");
  }
  {
    const seen: ChatMlMessage[] = [];
    for (const turn of turns) {
      historyPrefixes.push([...seen]);
      seen.push(...turnHistoryMessages(turn));
    }
  }
  debugLog(`parsed ${turns.length} turn(s) from ${path.basename(rolloutFile)}`);

  const subagentIndex =
    options.subagentIndex ??
    (await buildSubagentIndex(rolloutFile, {
      includeEarlierDays: sessionMeta.isSubagentThread === true,
    }));
  const seenThreadIds = options.seenThreadIds ?? new Set<string>();
  seenThreadIds.add(sessionMeta.sessionId);

  const announced = new Set(turns.flatMap((t) => t.subagentThreadIds));
  const unannounced = (subagentIndex.byParent.get(sessionMeta.sessionId) ?? []).filter(
    (s) => !announced.has(s.threadId) && !seenThreadIds.has(s.threadId),
  );
  const spawnTurnOf =
    unannounced.length > 0 ? turnIndexByNickname(turns) : new Map<string, number>();
  const byTurn = new Map<number, SubagentRollout[]>();
  for (const sub of unannounced) {
    const viaNickname = sub.nickname !== undefined ? spawnTurnOf.get(sub.nickname) : undefined;
    const i = viaNickname ?? turnIndexAt(turns, sub.startTime);
    debugLog(
      `recovered unannounced subagent ${sub.threadId} for thread ${sessionMeta.sessionId}: ` +
        `turn ${i + 1} via ${viaNickname !== undefined ? `nickname ${sub.nickname}` : "start time"}`,
    );
    byTurn.set(i, [...(byTurn.get(i) ?? []), sub]);
  }

  const ancestorTurnIds =
    options.ancestorTurnIds ??
    (sessionMeta.isSubagentThread
      ? await ancestorTurnIdsOf(sessionMeta, subagentIndex)
      : undefined);

  const carriedSubagents: SubagentRollout[] = [];
  const subagentsFor = (turnIndex: number): SubagentRollout[] | undefined => {
    const own = byTurn.get(turnIndex) ?? [];
    const all = carriedSubagents.length > 0 ? [...carriedSubagents, ...own] : own;
    carriedSubagents.length = 0;
    return all.length > 0 ? all : undefined;
  };

  const skipInherited = (turn: Turn, turnIndex: number): boolean => {
    if (!turn.turnId || !ancestorTurnIds?.has(turn.turnId)) return false;
    debugLog(`skipping turn ${turn.turnId}: inherited from an ancestor thread`);
    carriedSubagents.push(...(byTurn.get(turnIndex) ?? []));
    return true;
  };

  const inheritableTurnIds = new Set(ancestorTurnIds);
  for (const turn of turns) {
    if (turn.turnId) inheritableTurnIds.add(turn.turnId);
  }

  if (options.parentObservation) {
    for (let turnIndex = 0; turnIndex < turns.length; turnIndex++) {
      const turn = turns[turnIndex];
      if (skipInherited(turn, turnIndex)) continue;
      const fingerprintKey = `${sessionMeta.sessionId}:${turn.turnId ?? turn.startTime}`;
      await emitTurn(turn, sessionMeta, {
        config: options.config,
        rolloutFile,
        unchanged: options.acknowledgedTurns?.[fingerprintKey] === turnFingerprints[turnIndex],
        acknowledgedTurns: options.acknowledgedTurns,
        pendingTurnFingerprints: options.pendingTurnFingerprints,
        parentObservation: options.parentObservation,
        subagentIndex,
        seenThreadIds,
        unannouncedSubagents: subagentsFor(turnIndex),
        inheritableTurnIds,
        historyPrefix: historyPrefixes[turnIndex],
        incremental: options.incremental,
        hookEvents: options.hookEvents,
        emittedHookIds,
        legacyIdMode: options.legacyIdMode,
        legacyCutoff: options.legacyCutoff,
        legacyUploaded:
          options.legacyIdMode && turn.completed && turn.endTime <= (options.legacyCutoff ?? 0),
        final: isFinal(turn, options.stoppedTurnId, turnIndex < turns.length - 1),
      });
      if (options.pendingTurnFingerprints)
        options.pendingTurnFingerprints[fingerprintKey] = turnFingerprints[turnIndex];
    }
    return [];
  }

  const uploaded = await loadUploadedTurnIds(rolloutFile);
  const exportedTurnIds: string[] = [];
  const attached = options.parentSpanContext != null;

  for (let turnIndex = 0; turnIndex < turns.length; turnIndex++) {
    const turn = turns[turnIndex];

    if (skipInherited(turn, turnIndex)) continue;

    const final = isFinal(turn, options.stoppedTurnId, turnIndex < turns.length - 1);
    if (!options.incremental && !final) {
      debugLog(`skipping turn ${turn.turnId ?? "(no turn id)"}: not final`);
      continue;
    }
    if (!options.incremental && turn.turnId && uploaded.has(turn.turnId)) {
      continue;
    }

    const parentSpanContext =
      options.parentSpanContext ??
      (await seededTraceParent(options.config, sessionMeta, turnIndex + 1));

    const fingerprintKey = `${sessionMeta.sessionId}:${turn.turnId ?? turn.startTime}`;
    const emit = () =>
      emitTurn(turn, sessionMeta, {
        config: options.config,
        rolloutFile,
        unchanged: options.acknowledgedTurns?.[fingerprintKey] === turnFingerprints[turnIndex],
        acknowledgedTurns: options.acknowledgedTurns,
        pendingTurnFingerprints: options.pendingTurnFingerprints,
        parentSpanContext,
        attached,
        subagentIndex,
        seenThreadIds,
        unannouncedSubagents: subagentsFor(turnIndex),
        inheritableTurnIds,
        historyPrefix: historyPrefixes[turnIndex],
        incremental: options.incremental,
        hookEvents: options.hookEvents,
        emittedHookIds,
        legacyIdMode: legacyTurnIds.has(turn.turnId ?? ""),
        legacyCutoff: legacyTurnIds.has(turn.turnId ?? "") ? turn.endTime : undefined,
        legacyUploaded: legacyTurnIds.has(turn.turnId ?? ""),
        final: isFinal(turn, options.stoppedTurnId, turnIndex < turns.length - 1),
      });

    if (attached) {
      await emit();
    } else {
      const tags = traceTags(options.config, turn);
      await propagateAttributes(
        {
          sessionId: sessionMeta.sessionId,
          traceName: sessionMeta.isSubagentThread ? "Codex Subagent Turn" : "Codex Turn",
          ...(options.config.user_id ? { userId: options.config.user_id } : {}),
          ...(tags.length > 0 ? { tags } : {}),
          ...(options.config.metadata ? { metadata: options.config.metadata } : {}),
        },
        emit,
      );
    }

    if (options.pendingTurnFingerprints)
      options.pendingTurnFingerprints[fingerprintKey] = turnFingerprints[turnIndex];
    if (final && turn.turnId && !uploaded.has(turn.turnId)) {
      uploaded.add(turn.turnId);
      exportedTurnIds.push(turn.turnId);
    }
  }

  if (options.incremental && !options.parentObservation) {
    for (const hook of options.hookEvents ?? []) {
      if (!hook.replayOnly && !emittedHookIds.has(hook.id)) {
        emitHookObservation(hook);
        emittedHookIds.add(hook.id);
      }
    }
  }
  return exportedTurnIds;
}

export function emitHookObservation(hook: HookEvent, parentSpanContext?: SpanContext): void {
  const previous = currentIdSeed();
  seedIds(`hook:${hook.id}`);
  const emit = () =>
    startObservation(
      `Codex Hook: ${hook.input.hook_event_name ?? "Unknown"}`,
      {
        input: hook.input,
        metadata: {
          "codex.hook_event_name": hook.input.hook_event_name,
          "codex.thread_id": hook.input.session_id,
          "codex.turn_id": hook.input.turn_id,
        },
      },
      {
        asType: "span",
        startTime: new Date(hook.timestamp),
        parentSpanContext: hook.standalone ? undefined : parentSpanContext,
      },
    );
  const observation = emit();
  observation.otelSpan.setAttribute(LangfuseOtelSpanAttributes.OBSERVATION_TYPE, "event");
  if (hook.input.session_id && (hook.standalone || !parentSpanContext)) {
    observation.otelSpan.setAttribute(
      LangfuseOtelSpanAttributes.TRACE_SESSION_ID,
      hook.input.session_id,
    );
    observation.otelSpan.setAttribute(
      LangfuseOtelSpanAttributes.TRACE_COMPAT_SESSION_ID,
      hook.input.session_id,
    );
  }
  observation.end(new Date(hook.timestamp));
  seedIds(previous);
}

/** Child hooks use the same root checkpoint and parenting as parent hooks. */
export async function resolveRootRollout(
  rolloutFile: string,
): Promise<{ file: string; index: SubagentIndex }> {
  const meta = await readSessionMeta(rolloutFile);
  const index = await buildSubagentIndex(rolloutFile, {
    includeEarlierDays: !!meta?.parentThreadId,
  });
  let file = rolloutFile;
  let parent = meta?.parentThreadId;
  const seen = new Set<string>();
  while (parent && !seen.has(parent)) {
    seen.add(parent);
    const ancestor = index.byThread.get(parent);
    if (!ancestor) break;
    file = ancestor.file;
    parent = ancestor.parentThreadId;
  }
  return { file, index };
}
