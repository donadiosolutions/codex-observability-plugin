import { randomUUID, createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { getConfig } from "./config.js";
import { setupInstrumentation } from "./instrumentation.js";
import { readExternalParentSpanContext } from "./parent-context.js";
import {
  acquireRolloutLock,
  acknowledgeHooks,
  enqueueHook,
  loadObservationCheckpoint,
  loadHookContext,
  loadTurnCheckpoint,
  markTurnUploaded,
  pendingHooks,
  saveObservationCheckpoint,
} from "./sidecar.js";
import {
  buildSubagentIndex,
  convertRollout,
  emitHookObservation,
  resolveRootRollout,
} from "./trace.js";
import type { HookInput } from "./types.js";
import { debugLog, readStdin, setDebug } from "./utils.js";

let failOnError = process.env.LANGFUSE_CODEX_FAIL_ON_ERROR === "true";

function spoolFor(sessionId: string): string {
  return path.join(
    process.env.CODEX_HOME ?? path.join(os.homedir(), ".codex"),
    "langfuse-hooks",
    createHash("sha256").update(sessionId).digest("hex"),
  );
}

async function importSessionSpool(spool: string, file: string, waitSeconds = 0): Promise<void> {
  if ((await pendingHooks(spool)).length === 0) return;
  const release = await acquireRolloutLock(spool, waitSeconds);
  if (!release) return;
  try {
    for (const event of await pendingHooks(spool)) {
      await enqueueHook(file, event.input, event);
      await acknowledgeHooks(spool, [event]);
    }
  } finally {
    await release();
  }
}

/** Every hook queues durably before a bounded, serialized incremental upload. */
export async function runHook(): Promise<void> {
  let hookInput: HookInput;
  try {
    hookInput = await readStdin<HookInput>();
  } catch {
    return;
  }
  const config = await getConfig();
  setDebug(config.debug);
  failOnError = config.fail_on_error;
  if (!config.enabled || !config.public_key || !config.secret_key) return;

  // Queue before discovering rollouts: native Interrupt/SessionEnd have a 3s cap.
  const started = Date.now();
  const shortHook =
    hookInput.hook_event_name === "Interrupt" || hookInput.hook_event_name === "SessionEnd";
  if (hookInput.hook_event_name === "SubagentStop" && hookInput.agent_id) {
    hookInput = {
      ...hookInput,
      session_id: hookInput.agent_id,
      turn_id: hookInput.session_id === hookInput.agent_id ? hookInput.turn_id : undefined,
    };
  }
  const sessionSpool = spoolFor(hookInput.session_id ?? "unknown");
  await fs.mkdir(path.dirname(sessionSpool), { recursive: true, mode: 0o700 });
  await enqueueHook(sessionSpool, hookInput, {
    id: randomUUID(),
    timestamp: started,
    input: hookInput,
    standalone: !hookInput.transcript_path,
  });
  if (!hookInput.transcript_path) {
    const release = await acquireRolloutLock(
      sessionSpool,
      shortHook ? Math.max(0, (2500 - Date.now() + started) / 1000) : 10,
    );
    if (!release) return;
    try {
      const events = await pendingHooks(sessionSpool);
      const instrumentation = setupInstrumentation(config, {
        acknowledged: await loadObservationCheckpoint(sessionSpool),
      });
      let failure: unknown;
      try {
        for (const event of events) emitHookObservation(event);
      } catch (error) {
        failure = error;
      }
      try {
        await instrumentation.shutdown();
      } catch (error) {
        failure ??= error;
      }
      await saveObservationCheckpoint(sessionSpool, instrumentation.acknowledged());
      if (failure) {
        if (config.fail_on_error) throw failure;
        debugLog("early hook remains retryable", failure);
        return;
      }
      await acknowledgeHooks(sessionSpool, events);
    } finally {
      await release();
    }
    return;
  }

  const requestedFile = hookInput.agent_transcript_path ?? hookInput.transcript_path;
  const resolved = await resolveRootRollout(requestedFile);
  const file = resolved.file;
  let index = resolved.index;
  if (shortHook && Date.now() - started >= 2500) return;
  const waitBudget = () => (shortHook ? Math.max(0, (2500 - Date.now() + started) / 1000) : 10);
  await importSessionSpool(sessionSpool, file, waitBudget());
  const release = await acquireRolloutLock(file, waitBudget());
  if (!release) {
    debugLog("uploader busy; hook remains in durable inbox");
    return;
  }

  try {
    const parentSpanContext = readExternalParentSpanContext(process.env, config.fail_on_error);
    // Bounded passes catch hooks queued during export without starting a daemon.
    for (let pass = 0; pass < 4; pass++) {
      if (shortHook && Date.now() - started >= 2500) return;
      if (pass > 0) index = await buildSubagentIndex(file);
      const rootThread = [...index.byThread.values()].find(
        (rollout) => rollout.file === file,
      )?.threadId;
      const sessions = new Set<string>([
        hookInput.session_id ?? "unknown",
        ...(rootThread ? [rootThread] : []),
      ]);
      const parents = rootThread ? [rootThread] : [];
      for (let cursor = 0; cursor < parents.length; cursor++) {
        for (const child of index.byParent.get(parents[cursor]) ?? []) {
          if (!sessions.has(child.threadId)) {
            sessions.add(child.threadId);
            parents.push(child.threadId);
          }
        }
      }
      for (const session of sessions)
        await importSessionSpool(spoolFor(session), file, waitBudget());
      const events = await pendingHooks(file);
      if (events.length === 0) break;
      const contextualEvents = [...(await loadHookContext(file)), ...events];
      const instrumentation = setupInstrumentation(config, {
        acknowledged: await loadObservationCheckpoint(file),
      });
      const acknowledgedTurns = await loadTurnCheckpoint(file);
      const pendingTurnFingerprints: Record<string, string> = {};
      let exportedTurnIds: string[] = [];
      let failure: unknown;
      try {
        exportedTurnIds = await convertRollout(file, {
          config,
          parentSpanContext,
          subagentIndex: index,
          incremental: true,
          hookEvents: contextualEvents,
          acknowledgedTurns,
          pendingTurnFingerprints,
        });
      } catch (error) {
        failure = error;
        debugLog("rollout conversion failed", error);
      }
      try {
        await instrumentation.shutdown();
      } catch (error) {
        failure ??= error;
      }
      const retainedByKey = new Map<string, (typeof contextualEvents)[number]>();
      for (const event of contextualEvents.filter((event) => event.contextNeeded)) {
        const tool = event.input.tool_use_id ?? event.input.tool_call_id;
        const key = `${event.input.session_id}:${event.input.turn_id}:${tool ? `tool:${tool}` : "final"}`;
        retainedByKey.set(key, { ...event, replayOnly: true });
      }
      const retained = [...retainedByKey.values()];
      // Successful prefix batches remain acknowledged even when a later batch fails.
      await saveObservationCheckpoint(
        file,
        instrumentation.acknowledged(),
        retained,
        failure ? acknowledgedTurns : { ...acknowledgedTurns, ...pendingTurnFingerprints },
      );
      if (failure) {
        if (config.fail_on_error) throw failure;
        debugLog("export incomplete; durable hooks and observations remain retryable", failure);
        return;
      }
      for (const turnId of exportedTurnIds) await markTurnUploaded(file, turnId);
      await acknowledgeHooks(file, events);
    }
  } finally {
    await release();
  }
}

runHook().catch((error) => {
  if (process.env.LANGFUSE_CODEX_DEBUG === "true") console.error("[langfuse-codex] fatal:", error);
  if (failOnError) process.exitCode = 1;
});
