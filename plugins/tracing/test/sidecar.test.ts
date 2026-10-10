import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { expect, it } from "vitest";
import {
  acquireRolloutLock,
  acknowledgeHooks,
  enqueueHook,
  loadHookContext,
  loadObservationCheckpoint,
  loadUploadedTurnIds,
  markTurnUploaded,
  pendingHooks,
  saveObservationCheckpoint,
} from "../src/sidecar.js";

it("keeps concurrent hooks durable while serializing one uploader", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lf-sidecar-"));
  const file = path.join(dir, "rollout.jsonl");
  await enqueueHook(file, { session_id: "session", hook_event_name: "PostToolUse" });
  const release = await acquireRolloutLock(file);
  expect(release).toBeDefined();
  await enqueueHook(file, { session_id: "session", hook_event_name: "Stop" });
  expect(await acquireRolloutLock(file)).toBeUndefined();
  const events = await pendingHooks(file);
  expect(events).toHaveLength(2);
  expect(new Set(events.map((event) => event.id)).size).toBe(2);
  await saveObservationCheckpoint(file, { "trace:span": "fingerprint" }, [
    { ...events[0], replayOnly: true },
  ]);
  expect(await loadObservationCheckpoint(file)).toEqual({ "trace:span": "fingerprint" });
  expect(await loadHookContext(file)).toHaveLength(1);
  await acknowledgeHooks(file, [events[0]]);
  expect(await pendingHooks(file)).toHaveLength(1);
  await release!();
  const nextRelease = await acquireRolloutLock(file);
  expect(nextRelease).toBeDefined();
  await nextRelease!();
});
it("keeps legacy newline checkpoint compatibility", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lf-sidecar-"));
  const file = path.join(dir, "rollout.jsonl");
  await markTurnUploaded(file, "old-turn");
  expect(await loadUploadedTurnIds(file)).toEqual(new Set(["old-turn"]));
  expect(await loadObservationCheckpoint(file)).toEqual({});
  await saveObservationCheckpoint(file, { "new:observation": "new-content" });
  expect(await loadUploadedTurnIds(file)).toEqual(new Set(["old-turn"]));
});

it("ignores stale lock-file contents because ownership belongs to the kernel", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lf-sidecar-kernel-"));
  const file = path.join(dir, "rollout.jsonl");
  await fs.writeFile(`${file}.langfuse-lock`, "dead uploader 999999\n");
  const release = await acquireRolloutLock(file);
  expect(release).toBeDefined();
  await release!();
  const acquiredAgain = await acquireRolloutLock(file);
  expect(acquiredAgain).toBeDefined();
  await acquiredAgain!();
});

it("lets an already queued contender drain after the owner releases without another hook", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "lf-sidecar-handoff-"));
  const file = path.join(dir, "rollout.jsonl");
  const release = await acquireRolloutLock(file);
  expect(release).toBeDefined();
  expect(await pendingHooks(file)).toHaveLength(0);
  await enqueueHook(file, { hook_event_name: "PostToolUse", session_id: "handoff-session" });
  const waiting = acquireRolloutLock(file, 1);
  await release!();
  const contenderRelease = await waiting;
  expect(contenderRelease).toBeDefined();
  try {
    const events = await pendingHooks(file);
    expect(events).toHaveLength(1);
    await acknowledgeHooks(file, events);
    expect(await pendingHooks(file)).toHaveLength(0);
  } finally {
    await contenderRelease!();
  }
});
