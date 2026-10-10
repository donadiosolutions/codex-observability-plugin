import * as fs from "node:fs/promises";

/**
 * The `Stop` hook reads the rollout file again after each Codex turn.
 * The sidecar file (`<rolloutFile>.langfuse`) holds the ids of the sent turns.
 * The hook adds an id only after a flush. Retry a turn that has no id.
 */
export async function loadUploadedTurnIds(rolloutFile: string): Promise<Set<string>> {
  try {
    const data = await fs.readFile(`${rolloutFile}.langfuse`, "utf-8");
    return new Set(data.split("\n").filter(Boolean));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return new Set();
    throw error;
  }
}

export async function markTurnUploaded(rolloutFile: string, turnId: string): Promise<void> {
  try {
    await fs.appendFile(`${rolloutFile}.langfuse`, `${turnId}\n`, "utf-8");
  } catch {
    // Best-effort: a failed write only risks a duplicate upload next time.
  }
}

import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import * as path from "node:path";
import type { HookEvent, HookInput } from "./types.js";

/** A separate file keeps old newline turn checkpoints readable during upgrades. */
export async function loadObservationCheckpoint(
  rolloutFile: string,
): Promise<Record<string, string>> {
  try {
    const state = JSON.parse(await fs.readFile(`${rolloutFile}.langfuse-observations`, "utf8"));
    if (
      state.version !== 1 ||
      typeof state.observations !== "object" ||
      state.observations === null
    )
      throw new Error("Invalid observation checkpoint");
    return state.observations;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
    throw error;
  }
}

export async function saveObservationCheckpoint(
  rolloutFile: string,
  observations: Record<string, string>,
  hooks: HookEvent[] = [],
  turns: Record<string, string> = {},
): Promise<void> {
  const temporary = `${rolloutFile}.langfuse-observations.${randomUUID()}`;
  await fs.writeFile(
    temporary,
    JSON.stringify({
      version: 1,
      observations,
      hooks,
      turns,
      legacyTurnIds: [...(await loadLegacyTurnIds(rolloutFile))],
    }),
    {
      mode: 0o600,
    },
  );
  await fs.rename(temporary, `${rolloutFile}.langfuse-observations`);
}

/** Durable inbox: a busy hook exits promptly; its owner drains this on its next pass. */
export async function enqueueHook(
  rolloutFile: string,
  input: HookInput,
  persisted?: HookEvent,
): Promise<void> {
  const dir = `${rolloutFile}.langfuse-pending`;
  await fs.mkdir(dir, { recursive: true, mode: 0o700 });
  const event: HookEvent = persisted ?? { id: randomUUID(), timestamp: Date.now(), input };
  const temporary = path.join(dir, `${event.id}.tmp`);
  await fs.writeFile(temporary, JSON.stringify(event), { mode: 0o600 });
  await fs.rename(temporary, path.join(dir, `${event.id}.json`));
}

export async function pendingHooks(rolloutFile: string): Promise<HookEvent[]> {
  const dir = `${rolloutFile}.langfuse-pending`;
  try {
    const names = (await fs.readdir(dir)).filter((name) => name.endsWith(".json"));
    const events: HookEvent[] = [];
    for (const name of names)
      events.push(JSON.parse(await fs.readFile(path.join(dir, name), "utf8")));
    return events.sort((a, b) => a.timestamp - b.timestamp || a.id.localeCompare(b.id));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}

export async function acknowledgeHooks(rolloutFile: string, events: HookEvent[]): Promise<void> {
  for (const event of events)
    await fs.unlink(path.join(`${rolloutFile}.langfuse-pending`, `${event.id}.json`));
}

/** Linux kernel lock: one bounded helper, released on EOF or parent death. */
export async function acquireRolloutLock(
  rolloutFile: string,
  waitSeconds = 0,
): Promise<(() => Promise<void>) | undefined> {
  const helper = spawn(
    "flock",
    [
      "--timeout",
      Math.max(0, Math.min(10, waitSeconds)).toFixed(3),
      "--",
      `${rolloutFile}.langfuse-lock`,
      process.execPath,
      "-e",
      `
    const timer = setTimeout(() => {try {process.kill(${process.pid},"SIGTERM");} catch {} process.exit(1);},35000);
    process.stdin.resume();
    process.stdin.once("end",()=>{clearTimeout(timer);process.exit(0);});
    process.stdout.write("locked\\n");
  `,
    ],
    { stdio: ["pipe", "pipe", "pipe"], env: { ...process.env, LC_ALL: "C" } },
  );
  let spawnError: Error | undefined;
  helper.once("error", (error) => {
    spawnError = error;
  });
  const closed = new Promise<void>((resolve) => helper.once("close", () => resolve()));
  const acquired = await new Promise<boolean>((resolve) => {
    const timeout = setTimeout(
      () => {
        helper.kill("SIGTERM");
        resolve(false);
      },
      Math.max(0, Math.min(10, waitSeconds)) * 1000 + 1500,
    );
    helper.stdout.once("data", () => {
      clearTimeout(timeout);
      resolve(true);
    });
    helper.once("close", () => {
      clearTimeout(timeout);
      resolve(false);
    });
  });
  if (!acquired) {
    await closed;
    if (spawnError) throw spawnError;
    return undefined;
  }
  return async () => {
    helper.stdin.end();
    await closed;
  };
}

export async function loadHookContext(rolloutFile: string): Promise<HookEvent[]> {
  try {
    const state = JSON.parse(await fs.readFile(`${rolloutFile}.langfuse-observations`, "utf8"));
    return state.hooks ?? [];
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}

export async function loadLegacyTurnIds(rolloutFile: string): Promise<Set<string>> {
  try {
    const state = JSON.parse(await fs.readFile(`${rolloutFile}.langfuse-observations`, "utf8"));
    return new Set(state.legacyTurnIds ?? []);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return loadUploadedTurnIds(rolloutFile);
    throw error;
  }
}

export async function loadTurnCheckpoint(rolloutFile: string): Promise<Record<string, string>> {
  try {
    return (
      JSON.parse(await fs.readFile(`${rolloutFile}.langfuse-observations`, "utf8")).turns ?? {}
    );
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
    throw error;
  }
}
