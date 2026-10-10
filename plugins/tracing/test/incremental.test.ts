import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import { setLangfuseTracerProvider } from "@langfuse/tracing";
import { afterAll, beforeAll, beforeEach, expect, it, vi } from "vitest";
import type { Config } from "../src/config.js";
import { currentIdSeed } from "../src/instrumentation.js";
import { buildSubagentIndex, convertRollout, resolveRootRollout } from "../src/trace.js";
import { markTurnUploaded } from "../src/sidecar.js";

const exporter = new InMemorySpanExporter();
const config: Config = {
  enabled: true,
  public_key: "pk",
  secret_key: "sk",
  base_url: "http://localhost",
  skill_tags: true,
  debug: false,
  fail_on_error: true,
};
const id = (kind: string, size: number) =>
  createHash("sha256").update(`${currentIdSeed()}:${kind}`).digest("hex").slice(0, size);
const provider = new NodeTracerProvider({
  spanProcessors: [new SimpleSpanProcessor(exporter)],
  idGenerator: { generateTraceId: () => id("trace", 32), generateSpanId: () => id("span", 16) },
});
beforeAll(() => {
  provider.register();
  setLangfuseTracerProvider(provider);
});
afterAll(async () => {
  await provider.shutdown();
  setLangfuseTracerProvider(null);
});
beforeEach(() => exporter.reset());
const line = (second: number, type: string, payload: unknown) =>
  JSON.stringify({
    timestamp: new Date(Date.UTC(2026, 5, 3, 12, 0, second)).toISOString(),
    type,
    payload,
  });
function stage() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "lf-incremental-"));
  const day = path.join(dir, "sessions/2026/06/03");
  fs.mkdirSync(day, { recursive: true });
  const file = path.join(day, "rollout-parent.jsonl");
  fs.writeFileSync(
    file,
    [
      line(0, "session_meta", { id: "parent" }),
      line(1, "event_msg", { type: "task_started", turn_id: "parent-turn" }),
      line(2, "event_msg", { type: "user_message", message: "work" }),
      line(3, "response_item", {
        type: "function_call",
        name: "exec_command",
        call_id: "call-1",
        arguments: "{}",
      }),
    ].join("\n") + "\n",
  );
  return { file, day };
}
it("uploads an open turn without finalizing it and updates stable observations", async () => {
  const { file } = stage();
  expect(await convertRollout(file, { config, incremental: true })).toEqual([]);
  const initial = exporter.getFinishedSpans();
  expect(initial.map((s) => s.name)).toContain("Codex Turn");
  const tool = initial.find((s) => s.name === "exec_command")!;
  expect(tool.attributes["langfuse.observation.metadata.codex.completed"]).toBe("false");
  fs.appendFileSync(
    file,
    line(4, "response_item", { type: "function_call_output", call_id: "call-1", output: "done" }) +
      "\n" +
      line(5, "event_msg", { type: "task_complete", turn_id: "parent-turn" }) +
      "\n",
  );
  exporter.reset();
  expect(await convertRollout(file, { config, incremental: true })).toEqual(["parent-turn"]);
  const updated = exporter.getFinishedSpans().find((s) => s.name === "exec_command")!;
  expect(updated.spanContext()).toEqual(tool.spanContext());
  expect(updated.attributes["langfuse.observation.output"]).toContain("done");
});
it("uses PostToolUse payload while transcript output has not been persisted", async () => {
  const { file } = stage();
  await convertRollout(file, {
    config,
    incremental: true,
    hookEvents: [
      {
        id: "hook-1",
        timestamp: Date.UTC(2026, 5, 3, 12, 0, 4),
        input: {
          hook_event_name: "PostToolUse",
          session_id: "parent",
          turn_id: "parent-turn",
          tool_use_id: "call-1",
          tool_response: "from hook",
        },
      },
    ],
  });
  const spans = exporter.getFinishedSpans();
  expect(
    spans.find((s) => s.name === "exec_command")?.attributes["langfuse.observation.output"],
  ).toContain("from hook");
  expect(spans.some((s) => s.name === "Codex Hook: PostToolUse")).toBe(true);
});
it("revisits children after a legacy parent checkpoint", async () => {
  const { file, day } = stage();
  fs.appendFileSync(
    file,
    line(4, "event_msg", { type: "task_complete", turn_id: "parent-turn" }) + "\n",
  );
  await markTurnUploaded(file, "parent-turn");
  const child = path.join(day, "rollout-child.jsonl");
  fs.writeFileSync(
    child,
    [
      line(3, "session_meta", { id: "child", parent_thread_id: "parent" }),
      line(4, "event_msg", { type: "task_started", turn_id: "child-turn" }),
      line(5, "event_msg", { type: "agent_message", message: "late child" }),
    ].join("\n") + "\n",
  );
  await convertRollout(file, { config, incremental: true });
  expect(exporter.getFinishedSpans().some((s) => s.name === "Codex Subagent Turn")).toBe(true);
});

it("retains hook output and completion until the rollout catches up", async () => {
  const { file } = stage();
  const hooks = [
    {
      id: "pending-output",
      timestamp: Date.UTC(2026, 5, 3, 12, 0, 4),
      input: {
        hook_event_name: "PostToolUse",
        session_id: "parent",
        turn_id: "parent-turn",
        tool_use_id: "call-1",
        tool_response: "not on disk",
      },
    },
    {
      id: "pending-stop",
      timestamp: Date.UTC(2026, 5, 3, 12, 0, 5),
      input: { hook_event_name: "Stop", session_id: "parent", turn_id: "parent-turn" },
    },
  ];
  await convertRollout(file, { config, incremental: true, hookEvents: hooks });
  expect(hooks.every((hook) => (hook as { contextNeeded?: boolean }).contextNeeded)).toBe(true);
  exporter.reset();
  await convertRollout(file, {
    config,
    incremental: true,
    hookEvents: hooks.map((hook) => ({ ...hook, replayOnly: true })),
  });
  const spans = exporter.getFinishedSpans();
  expect(
    spans.find((s) => s.name === "exec_command")?.attributes["langfuse.observation.output"],
  ).toContain("not on disk");
  expect(
    spans.find((s) => s.name === "Codex Turn")?.attributes[
      "langfuse.observation.metadata.codex.completed"
    ],
  ).toBe("true");
  expect(spans.some((s) => s.name.startsWith("Codex Hook:"))).toBe(false);
});

it("keeps tool IDs stable when new tools extend an existing model step", async () => {
  const { file } = stage();
  await convertRollout(file, { config, incremental: true });
  const initial = exporter.getFinishedSpans();
  fs.appendFileSync(
    file,
    line(4, "response_item", {
      type: "function_call",
      name: "extra",
      call_id: "call-2",
      arguments: "{}",
    }) + "\n",
  );
  exporter.reset();
  await convertRollout(file, { config, incremental: true });
  for (const span of initial) {
    const updated = exporter.getFinishedSpans().find((s) => s.name === span.name)!;
    expect(updated.spanContext()).toEqual(span.spanContext());
  }
});

it("retains a native tool_use_id result until its call row appears", async () => {
  const { file } = stage();
  fs.writeFileSync(
    file,
    fs
      .readFileSync(file, "utf8")
      .split("\n")
      .filter((raw) => !raw.includes('"type":"function_call"'))
      .join("\n"),
  );
  const hook = {
    id: "race-tool",
    timestamp: Date.UTC(2026, 5, 3, 12, 0, 4),
    input: {
      session_id: "parent",
      turn_id: "parent-turn",
      hook_event_name: "PostToolUse",
      tool_use_id: "call-1",
      tool_response: "raced output",
    },
    contextNeeded: false,
  };
  await convertRollout(file, { config, incremental: true, hookEvents: [hook] });
  expect(hook.contextNeeded).toBe(true);
  fs.appendFileSync(
    file,
    line(3, "response_item", {
      type: "function_call",
      name: "exec_command",
      call_id: "call-1",
      arguments: "{}",
    }) + "\n",
  );
  exporter.reset();
  await convertRollout(file, {
    config,
    incremental: true,
    hookEvents: [{ ...hook, replayOnly: true }],
  });
  expect(
    exporter.getFinishedSpans().find((span) => span.name === "exec_command")?.attributes[
      "langfuse.observation.output"
    ],
  ).toContain("raced output");
});

it("captures the native final assistant message before the rollout writer", async () => {
  const { file } = stage();
  const hook = {
    id: "race-stop",
    timestamp: Date.UTC(2026, 5, 3, 12, 0, 5),
    input: {
      session_id: "parent",
      turn_id: "parent-turn",
      hook_event_name: "Stop",
      last_assistant_message: "latest final answer",
    },
    contextNeeded: false,
  };
  await convertRollout(file, { config, incremental: true, hookEvents: [hook] });
  expect(
    exporter.getFinishedSpans().find((span) => span.name === "Codex Turn")?.attributes[
      "langfuse.observation.output"
    ],
  ).toContain("latest final answer");
  expect(hook.contextNeeded).toBe(true);
});

it("restricts a flat rollout to its sibling directory and supports configured archives", async () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "lf-archive-index-"));
  const archive = path.join(base, "archived_sessions");
  fs.mkdirSync(archive);
  const file = path.join(archive, "rollout-child.jsonl");
  fs.writeFileSync(
    file,
    line(0, "session_meta", { id: "archived-child", parent_thread_id: "active-parent" }) + "\n",
  );
  fs.writeFileSync(
    path.join(base, "unrelated.jsonl"),
    line(0, "session_meta", { id: "unrelated" }) + "\n",
  );
  const nested = path.join(archive, "unrelated-tree");
  fs.mkdirSync(nested);
  fs.writeFileSync(
    path.join(nested, "nested.jsonl"),
    line(0, "session_meta", { id: "nested-unrelated" }) + "\n",
  );
  const flatIndex = await buildSubagentIndex(file);
  expect(flatIndex.byThread.has("unrelated")).toBe(false);
  expect(flatIndex.byThread.has("nested-unrelated")).toBe(false);
  const day = path.join(base, "sessions/2026/06/03");
  fs.mkdirSync(day, { recursive: true });
  const parent = path.join(day, "rollout-parent.jsonl");
  fs.writeFileSync(parent, line(0, "session_meta", { id: "active-parent" }) + "\n");
  vi.stubEnv("CODEX_HOME", base);
  try {
    const resolved = await resolveRootRollout(file);
    expect(resolved.file).toBe(parent);
    expect(resolved.index.byThread.has("unrelated")).toBe(false);
  } finally {
    vi.unstubAllEnvs();
  }
});

it("skips unchanged payload preparation but resumes when the transcript grows", async () => {
  const { file } = stage();
  const fingerprints: Record<string, string> = {};
  await convertRollout(file, { config, incremental: true, pendingTurnFingerprints: fingerprints });
  expect(Object.keys(fingerprints)).toContain("parent:parent-turn");
  exporter.reset();
  await convertRollout(file, { config, incremental: true, acknowledgedTurns: fingerprints });
  const cached = exporter.getFinishedSpans();
  expect(
    cached.every(
      (span) => span.attributes["langfuse.observation.metadata.codex.legacy_uploaded"] === "true",
    ),
  ).toBe(true);
  expect(cached.every((span) => span.attributes["langfuse.observation.input"] === undefined)).toBe(
    true,
  );
  fs.appendFileSync(
    file,
    line(4, "response_item", {
      type: "function_call_output",
      call_id: "call-1",
      output: "new activity",
    }) + "\n",
  );
  exporter.reset();
  await convertRollout(file, { config, incremental: true, acknowledgedTurns: fingerprints });
  const changed = exporter.getFinishedSpans().find((span) => span.name === "exec_command")!;
  expect(changed.attributes["langfuse.observation.output"]).toContain("new activity");
  expect(changed.attributes["langfuse.observation.metadata.codex.legacy_uploaded"]).toBeUndefined();
});

it("skips acknowledged earlier steps when a running turn gains a new model step", async () => {
  const { file } = stage();
  fs.appendFileSync(file, line(4, "event_msg", { type: "token_count" }) + "\n");
  const fingerprints: Record<string, string> = {};
  await convertRollout(file, { config, incremental: true, pendingTurnFingerprints: fingerprints });
  fs.appendFileSync(
    file,
    line(5, "response_item", {
      type: "function_call",
      name: "new_tool",
      call_id: "call-2",
      arguments: "{}",
    }) + "\n",
  );
  exporter.reset();
  await convertRollout(file, { config, incremental: true, acknowledgedTurns: fingerprints });
  const generations = exporter.getFinishedSpans().filter((span) => span.name === "LLM");
  expect(generations).toHaveLength(2);
  const first = generations.find(
    (span) => span.attributes["langfuse.observation.metadata.codex.step_index"] === "0",
  )!;
  const second = generations.find(
    (span) => span.attributes["langfuse.observation.metadata.codex.step_index"] === "1",
  )!;
  expect(first.attributes["langfuse.observation.metadata.codex.legacy_uploaded"]).toBe("true");
  expect(first.attributes["langfuse.observation.input"]).toBeUndefined();
  expect(second.attributes["langfuse.observation.metadata.codex.legacy_uploaded"]).toBeUndefined();
  expect(second.attributes["langfuse.observation.input"]).toBeDefined();
});

it("keeps a turn-less hook attached to its original turn when a later turn appears", async () => {
  const { file } = stage();
  const persisted = {
    id: "delayed-stop",
    timestamp: Date.UTC(2026, 5, 3, 12, 0, 4),
    input: { session_id: "parent", hook_event_name: "Stop" },
  };
  await convertRollout(file, {
    config,
    incremental: true,
    hookEvents: [structuredClone(persisted)],
  });
  const first = exporter.getFinishedSpans().find((span) => span.name === "Codex Hook: Stop")!;
  fs.appendFileSync(
    file,
    line(5, "event_msg", { type: "task_complete", turn_id: "parent-turn" }) +
      "\n" +
      line(6, "event_msg", { type: "task_started", turn_id: "next-turn" }) +
      "\n" +
      line(7, "event_msg", { type: "user_message", message: "next request" }) +
      "\n",
  );
  exporter.reset();
  await convertRollout(file, {
    config,
    incremental: true,
    hookEvents: [structuredClone(persisted)],
  });
  const spans = exporter.getFinishedSpans();
  const replay = spans.find((span) => span.name === "Codex Hook: Stop")!;
  expect(replay.spanContext()).toEqual(first.spanContext());
  const next = spans.find(
    (span) => span.attributes["langfuse.observation.metadata.codex.turn_id"] === "next-turn",
  )!;
  expect(next.attributes["langfuse.observation.metadata.codex.completed"]).toBe("false");
  expect(replay.parentSpanContext?.spanId).not.toBe(next.spanContext().spanId);
});
