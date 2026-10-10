import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, describe, expect, it } from "vitest";
import { acquireRolloutLock } from "../src/sidecar.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const hookConfigFile = path.join(repoRoot, "plugins/tracing/hooks/hooks.json");
const pluginRootDir = path.join(repoRoot, "plugins/tracing");

const tmpDirs: string[] = [];

function makeTempDir(prefix: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tmpDirs.push(dir);
  return dir;
}

function readHookCommand(): string {
  const config = JSON.parse(fs.readFileSync(hookConfigFile, "utf-8")) as {
    hooks: { Stop: Array<{ hooks: Array<{ command: string }> }> };
  };
  return config.hooks.Stop[0].hooks[0].command;
}

function runShellCommand(
  command: string,
  options: { cwd: string; env: NodeJS.ProcessEnv; input: string },
): Promise<{ code: number | null; stderr: string; stdout: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, {
      cwd: options.cwd,
      env: options.env,
      shell: true,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => {
      child.kill("SIGTERM");
      reject(new Error("hook command timed out"));
    }, 10_000);

    child.stdout.setEncoding("utf-8");
    child.stderr.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.once("close", (code) => {
      clearTimeout(timeout);
      resolve({ code, stdout, stderr });
    });
    child.stdin.end(options.input);
  });
}

afterEach(() => {
  while (tmpDirs.length) {
    fs.rmSync(tmpDirs.pop()!, { recursive: true, force: true });
  }
});

describe("bundled Stop hook command", () => {
  it("runs from an arbitrary session cwd via PLUGIN_ROOT instead of a relative repo path", async () => {
    const codexHome = makeTempDir("lf-codex-home-");
    const sessionCwd = makeTempDir("lf-codex-cwd-");

    const { code, stderr, stdout } = await runShellCommand(readHookCommand(), {
      cwd: sessionCwd,
      env: {
        ...process.env,
        PLUGIN_ROOT: pluginRootDir,
        CODEX_HOME: codexHome,
        HOME: codexHome,
      },
      input: JSON.stringify({
        hook_event_name: "Stop",
        transcript_path: path.join(sessionCwd, "rollout.jsonl"),
      }),
    });

    expect(code).toBe(0);
    expect(stdout).toBe("");
    expect(stderr).toBe("");
  });

  it("does not depend on the old marketplace-root relative path", () => {
    expect(readHookCommand()).not.toContain("./plugins/tracing/dist/index.mjs");
  });

  it("delivers and marks the final turn of a single-turn session", async () => {
    const codexHome = makeTempDir("lf-codex-home-");
    const sessionCwd = makeTempDir("lf-codex-cwd-");
    // Mirror the real `sessions/YYYY/MM/DD` layout: the hook derives the
    // sessions root from the rollout path to discover subagent threads.
    const sessionsDir = path.join(sessionCwd, "sessions", "2026", "06", "03");
    fs.mkdirSync(sessionsDir, { recursive: true });
    const rollout = path.join(sessionsDir, "rollout.jsonl");

    // A one-turn session as the Stop hook sees it: the turn is still open on
    // disk, and no later Stop hook will ever fire to finalize it.
    const event = (payload: Record<string, unknown>) =>
      JSON.stringify({ timestamp: "2026-06-03T12:00:00.000Z", type: "event_msg", payload });
    fs.writeFileSync(
      rollout,
      [
        JSON.stringify({
          timestamp: "2026-06-03T12:00:00.000Z",
          type: "session_meta",
          payload: { id: "sess-final", cli_version: "0.149.0" },
        }),
        event({ type: "task_started", turn_id: "turn-final" }),
        event({ type: "user_message", message: "What is 1 + 1?" }),
        event({ type: "agent_message", message: "1 + 1 = 2." }),
      ].join("\n") + "\n",
    );

    const received: string[] = [];
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(chunk as Buffer));
      req.on("end", () => {
        received.push(Buffer.concat(chunks).toString("utf-8"));
        res.writeHead(200, { "content-type": "application/json" });
        res.end("{}");
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as { port: number };

    try {
      const { code } = await runShellCommand(readHookCommand(), {
        cwd: sessionCwd,
        env: {
          ...process.env,
          PLUGIN_ROOT: pluginRootDir,
          CODEX_HOME: codexHome,
          HOME: codexHome,
          TRACE_TO_LANGFUSE: "true",
          LANGFUSE_PUBLIC_KEY: "pk-lf-test",
          LANGFUSE_SECRET_KEY: "sk-lf-test",
          LANGFUSE_BASE_URL: `http://127.0.0.1:${port}`,
        },
        input: JSON.stringify({
          hook_event_name: "Stop",
          session_id: "sess-final",
          turn_id: "turn-final",
          transcript_path: rollout,
        }),
      });

      expect(code).toBe(0);
      expect(received.join("")).toContain("turn-final");
      expect(fs.readFileSync(`${rollout}.langfuse`, "utf-8").trim()).toBe("turn-final");
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("exports only a fresh hook event when observations are unchanged", async () => {
    const codexHome = makeTempDir("lf-codex-home-");
    const sessionCwd = makeTempDir("lf-codex-cwd-");
    const sessionsDir = path.join(sessionCwd, "sessions", "2026", "06", "03");
    fs.mkdirSync(sessionsDir, { recursive: true });
    const rollout = path.join(sessionsDir, "rollout.jsonl");

    const event = (payload: Record<string, unknown>) =>
      JSON.stringify({ timestamp: "2026-06-03T12:00:00.000Z", type: "event_msg", payload });
    fs.writeFileSync(
      rollout,
      [
        JSON.stringify({
          timestamp: "2026-06-03T12:00:00.000Z",
          type: "session_meta",
          payload: { id: "sess-idem", cli_version: "0.155.1" },
        }),
        event({ type: "task_started", turn_id: "turn-idem" }),
        event({ type: "user_message", message: "What is 1 + 1?" }),
        event({ type: "agent_message", message: "1 + 1 = 2." }),
      ].join("\n") + "\n",
    );

    const runs: Buffer[][] = [];
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(chunk as Buffer));
      req.on("end", () => {
        runs[runs.length - 1].push(Buffer.concat(chunks));
        res.writeHead(200, { "content-type": "application/json" });
        res.end("{}");
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as { port: number };

    try {
      for (let attempt = 0; attempt < 2; attempt++) {
        runs.push([]);
        const { code } = await runShellCommand(readHookCommand(), {
          cwd: sessionCwd,
          env: {
            ...process.env,
            PLUGIN_ROOT: pluginRootDir,
            CODEX_HOME: codexHome,
            HOME: codexHome,
            TRACE_TO_LANGFUSE: "true",
            LANGFUSE_PUBLIC_KEY: "pk-lf-test",
            LANGFUSE_SECRET_KEY: "sk-lf-test",
            LANGFUSE_BASE_URL: `http://127.0.0.1:${port}`,
          },
          input: JSON.stringify({
            hook_event_name: "Stop",
            session_id: "sess-idem",
            turn_id: "turn-idem",
            transcript_path: rollout,
          }),
        });
        expect(code).toBe(0);
      }

      expect(runs[0].length).toBeGreaterThan(0);
      const spansIn = (requests: Buffer[]) =>
        requests.flatMap((request) => {
          const body = JSON.parse(request.toString("utf8")) as {
            resourceSpans: Array<{
              scopeSpans: Array<{ spans: Array<{ name: string; spanId: string }> }>;
            }>;
          };
          return body.resourceSpans.flatMap((resource) =>
            resource.scopeSpans.flatMap((scope) => scope.spans),
          );
        });
      const first = spansIn(runs[0]);
      const second = spansIn(runs[1]);
      expect(first.some((span) => span.name === "Codex Turn")).toBe(true);
      expect(second.map((span) => span.name)).toEqual(["Codex Hook: Stop"]);
      expect(second[0].spanId).not.toBe(
        first.find((span) => span.name === "Codex Hook: Stop")!.spanId,
      );
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("uses no shell syntax beyond the placeholder Codex substitutes itself", () => {
    expect(readHookCommand().replaceAll("${PLUGIN_ROOT}", "")).not.toContain("$");
  });
});

it("retries a failed SessionStart event after the rollout becomes available", async () => {
  const codexHome = makeTempDir("lf-codex-early-home-");
  const sessionCwd = makeTempDir("lf-codex-early-cwd-");
  const day = path.join(sessionCwd, "sessions/2026/06/03");
  fs.mkdirSync(day, { recursive: true });
  const file = path.join(day, "rollout.jsonl");
  const line = (type: string, payload: unknown) =>
    JSON.stringify({ timestamp: "2026-06-03T12:00:00.000Z", type, payload });
  fs.writeFileSync(
    file,
    [
      line("session_meta", { id: "early-session" }),
      line("event_msg", { type: "task_started", turn_id: "early-turn" }),
      line("event_msg", { type: "user_message", message: "work" }),
    ].join("\n") + "\n",
  );
  let accepting = false;
  const delivered: string[] = [];
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      if (accepting) delivered.push(Buffer.concat(chunks).toString("utf8"));
      res.writeHead(accepting ? 200 : 503, { "content-type": "application/json" });
      res.end("{}");
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as { port: number };
  const env = {
    ...process.env,
    PLUGIN_ROOT: pluginRootDir,
    CODEX_HOME: codexHome,
    HOME: codexHome,
    TRACE_TO_LANGFUSE: "true",
    LANGFUSE_PUBLIC_KEY: "pk-lf-test",
    LANGFUSE_SECRET_KEY: "sk-lf-test",
    LANGFUSE_BASE_URL: `http://127.0.0.1:${port}`,
    LANGFUSE_CODEX_FAIL_ON_ERROR: "true",
  };
  try {
    const failed = await runShellCommand(readHookCommand(), {
      cwd: sessionCwd,
      env,
      input: JSON.stringify({ hook_event_name: "SessionStart", session_id: "early-session" }),
    });
    expect(failed.code).toBe(1);
    const spoolRoot = path.join(codexHome, "langfuse-hooks");
    const pending = fs.readdirSync(spoolRoot).find((name) => name.endsWith(".langfuse-pending"))!;
    expect(fs.readdirSync(path.join(spoolRoot, pending))).toHaveLength(1);
    accepting = true;
    const recovered = await runShellCommand(readHookCommand(), {
      cwd: sessionCwd,
      env,
      input: JSON.stringify({
        hook_event_name: "PostToolUse",
        session_id: "early-session",
        turn_id: "early-turn",
        transcript_path: file,
      }),
    });
    expect(recovered.code).toBe(0);
    expect(delivered.join("")).toContain("Codex Hook: SessionStart");
    expect(delivered.join("")).toContain("Codex Hook: PostToolUse");
    expect(fs.readdirSync(path.join(spoolRoot, pending))).toHaveLength(0);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

it.each(["native", "legacy"])(
  "finalizes the child for %s SubagentStop payloads without finalizing its parent",
  async (shape) => {
    const codexHome = makeTempDir("lf-codex-child-home-");
    const cwd = makeTempDir("lf-codex-child-cwd-");
    const day = path.join(cwd, "sessions/2026/06/03");
    fs.mkdirSync(day, { recursive: true });
    const parentFile = path.join(day, "rollout-parent.jsonl");
    const childFile = path.join(day, "rollout-child.jsonl");
    const line = (second: number, type: string, payload: unknown) =>
      JSON.stringify({
        timestamp: new Date(Date.UTC(2026, 5, 3, 12, 0, second)).toISOString(),
        type,
        payload,
      });
    fs.writeFileSync(
      parentFile,
      [
        line(0, "session_meta", { id: "native-parent" }),
        line(1, "event_msg", { type: "task_started", turn_id: "parent-turn" }),
        line(2, "event_msg", { type: "user_message", message: "delegate" }),
      ].join("\n") + "\n",
    );
    fs.writeFileSync(
      childFile,
      [
        line(3, "session_meta", { id: "native-child", parent_thread_id: "native-parent" }),
        line(4, "event_msg", { type: "task_started", turn_id: "child-turn" }),
        line(5, "event_msg", { type: "agent_message", message: "child done" }),
      ].join("\n") + "\n",
    );
    if (shape === "native") {
      fs.appendFileSync(
        childFile,
        [
          line(6, "event_msg", { type: "task_started", turn_id: "child-next-turn" }),
          line(7, "event_msg", {
            type: "user_message",
            message: "child resumed before delayed hook dispatch",
          }),
        ].join("\n") + "\n",
      );
    }
    type Span = {
      name: string;
      spanId: string;
      parentSpanId?: string;
      attributes: Array<{ key: string; value: { stringValue?: string } }>;
    };
    const received: Span[] = [];
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(chunk));
      req.on("end", () => {
        const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as {
          resourceSpans: Array<{ scopeSpans: Array<{ spans: Span[] }> }>;
        };
        received.push(
          ...body.resourceSpans.flatMap((resource) =>
            resource.scopeSpans.flatMap((scope) => scope.spans),
          ),
        );
        res.writeHead(200, { "content-type": "application/json" });
        res.end("{}");
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as { port: number };
    try {
      const result = await runShellCommand(readHookCommand(), {
        cwd,
        env: {
          ...process.env,
          PLUGIN_ROOT: pluginRootDir,
          CODEX_HOME: codexHome,
          HOME: codexHome,
          TRACE_TO_LANGFUSE: "true",
          LANGFUSE_PUBLIC_KEY: "pk-lf-test",
          LANGFUSE_SECRET_KEY: "sk-lf-test",
          LANGFUSE_BASE_URL: `http://127.0.0.1:${port}`,
          LANGFUSE_CODEX_FAIL_ON_ERROR: "true",
        },
        input: JSON.stringify({
          hook_event_name: "SubagentStop",
          session_id: shape === "native" ? "native-child" : "native-parent",
          turn_id: shape === "native" ? "child-turn" : "parent-turn",
          transcript_path: parentFile,
          agent_id: "native-child",
          agent_transcript_path: childFile,
        }),
      });
      expect(result.code).toBe(0);
      const parent = received.find((span) => span.name === "Codex Turn")!;
      const child = received.find((span) => span.name === "Codex Subagent Turn")!;
      const hook = received.find((span) => span.name === "Codex Hook: SubagentStop")!;
      const completed = (span: Span) =>
        span.attributes.find((attr) => attr.key === "langfuse.observation.metadata.codex.completed")
          ?.value.stringValue;
      expect(completed(parent)).toBe("false");
      expect(completed(child)).toBe("true");
      expect(hook.parentSpanId).toBe(child.spanId);
      expect(fs.existsSync(`${parentFile}.langfuse`)).toBe(false);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  },
);

it("waits briefly for an active uploader before the final SessionEnd drain", async () => {
  const codexHome = makeTempDir("lf-codex-end-home-");
  const cwd = makeTempDir("lf-codex-end-cwd-");
  const day = path.join(cwd, "sessions/2026/06/03");
  fs.mkdirSync(day, { recursive: true });
  const file = path.join(day, "rollout.jsonl");
  const line = (type: string, payload: unknown) =>
    JSON.stringify({ timestamp: "2026-06-03T12:00:00.000Z", type, payload });
  fs.writeFileSync(
    file,
    [
      line("session_meta", { id: "end-session" }),
      line("event_msg", { type: "task_started", turn_id: "end-turn" }),
      line("event_msg", { type: "user_message", message: "work" }),
    ].join("\n") + "\n",
  );
  const received: string[] = [];
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      received.push(Buffer.concat(chunks).toString("utf8"));
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as { port: number };
  const release = await acquireRolloutLock(file);
  expect(release).toBeDefined();
  let released = false;
  let releasePromise: Promise<void> | undefined;
  const timer = setTimeout(() => {
    released = true;
    releasePromise = release!();
  }, 600);
  try {
    const result = await runShellCommand(readHookCommand(), {
      cwd,
      env: {
        ...process.env,
        PLUGIN_ROOT: pluginRootDir,
        CODEX_HOME: codexHome,
        HOME: codexHome,
        TRACE_TO_LANGFUSE: "true",
        LANGFUSE_PUBLIC_KEY: "pk-lf-test",
        LANGFUSE_SECRET_KEY: "sk-lf-test",
        LANGFUSE_BASE_URL: `http://127.0.0.1:${port}`,
        LANGFUSE_CODEX_FAIL_ON_ERROR: "true",
      },
      input: JSON.stringify({
        hook_event_name: "SessionEnd",
        session_id: "end-session",
        transcript_path: file,
      }),
    });
    expect(result.code).toBe(0);
    expect(released).toBe(true);
    expect(received.join("")).toContain("Codex Hook: SessionEnd");
    expect(fs.readFileSync(`${file}.langfuse`, "utf8").trim()).toBe("end-turn");
  } finally {
    clearTimeout(timer);
    if (!released) await release!();
    else await releasePromise;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
