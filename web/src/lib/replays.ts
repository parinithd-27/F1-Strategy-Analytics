/** Replay helpers (pure): API errors and replay states as text. */

import { ApiError, type Replay, type ReplayStatus } from "./api";

export type Speed = 1 | 10 | 60;

export function replayMessage(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.status === 403) return err.message.includes("disabled") ? "Replay is disabled on the API (ENABLE_REPLAY=1)." : err.message;
    if (err.status === 409) return err.message;
    if (err.status === 503) return "The clean zone is not available right now.";
    if (err.status === null) return "The API is not reachable.";
    return err.message;
  }
  return "Unexpected error.";
}

/** The replay of this race to show: the running one, else the most recent one. */
export function replayFor(status: ReplayStatus | null, season: number, round: number): Replay | null {
  if (!status) return null;
  const same = (r: Replay) => r.season === season && r.round === round;
  return status.running.find(same) ?? status.recent.find(same) ?? null;
}

export function replayStatusText(current: Replay | null, anyRunning: boolean, enabled: boolean | null): string {
  if (enabled === false) return "Replay is disabled on the API.";
  if (current?.state === "starting") return "Starting: waiting for the stream job…";
  if (current?.state === "running") return `Running at ${current.speed}×.`;
  if (anyRunning) return "Another race is replaying.";
  if (current?.state === "finished") return "Finished.";
  if (current?.state === "failed") return `Failed: ${current.error ?? "unknown error"}`;
  return "Idle.";
}

