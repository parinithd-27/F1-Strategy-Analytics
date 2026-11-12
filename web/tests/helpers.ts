/** Builders for SSE data lines and API snapshots, shaped exactly like the Step 13 API's output. */

import type { DriverState, RaceSnapshot } from "@/lib/api";

let counter = 0;
export function uuid(): string {
  counter += 1;
  const hex = counter.toString(16).padStart(12, "0");
  return `00000000-0000-4000-8000-${hex}`;
}

export function msg(type: string, payload: Record<string, unknown>, extra: Record<string, unknown> = {}): string {
  const id = (payload.event_id as string | undefined) ?? uuid();
  return JSON.stringify({
    type,
    season: 2024,
    round: 5,
    event_ts: null,
    published_at: "2024-05-05T07:00:00.000Z",
    payload: { event_id: id, ...payload },
    event_id: id,
    sse_sent_at_ms: 1_700_000_000_000,
    ...extra,
  });
}

export function lap(driver_id: string, lap: number, race_time_s: number, position: number, more: Record<string, unknown> = {}): string {
  return msg("lap", {
    driver_id,
    lap,
    race_time_s,
    position,
    lap_time_s: 95.5,
    compound: "MEDIUM",
    tyre_age: lap,
    produced_at_ms: 1,
    ...more,
  });
}

export function alertMsg(
  attacker_id: string,
  defender_id: string,
  reason: "opportunity" | "attempt",
  trigger: "new_candidate" | "probability_crossed" | "pit_in",
  undercut_prob: number,
  decision_lap = 10,
): string {
  // Alerts carry no payload.event_id (the API derives a uuid5 for the SSE id).
  const id = uuid();
  return JSON.stringify({
    type: "undercut_alert",
    season: 2024,
    round: 5,
    event_ts: null,
    published_at: "2024-05-05T07:00:00.000Z",
    payload: {
      reason,
      trigger,
      season: 2024,
      round: 5,
      attacker_id,
      defender_id,
      decision_lap,
      attempt_lap: decision_lap + 1,
      gap_s: 1.2,
      undercut_prob,
      threshold: 0.5,
      inputs: {},
      trigger_event_id: null,
    },
    event_id: id,
    sse_sent_at_ms: 1,
  });
}

export function driver(position: number, more: Partial<DriverState> = {}): DriverState {
  return {
    position,
    gap_ahead_s: position === 1 ? null : 1.5,
    gap_leader_s: position === 1 ? 0 : 1.5 * (position - 1),
    tyre_compound: "HARD",
    tyre_age_laps: 12,
    last_lap_time_s: 96.1,
    undercut_flag: false,
    undercut_prob: null,
    updated_at: "2024-05-05T07:00:00.000Z",
    ...more,
  };
}

export function snapshot(order: string[], more: Partial<RaceSnapshot> = {}): RaceSnapshot {
  return {
    season: 2024,
    round: 5,
    drivers: Object.fromEntries(order.map((id, i) => [id, driver(i + 1)])),
    order: order.map((driver_id, i) => ({ driver_id, position: i + 1 })),
    ...more,
  };
}

/** An ``event: snapshot`` data line as the API pushes it (after connect and once per leader lap). */
export function snapshotMsg(order: string[], lap: number, flags: Record<string, boolean> = {}, extra: Record<string, unknown> = {}): string {
  const snap = snapshot(order);
  for (const [id, flag] of Object.entries(flags)) {
    const d = snap.drivers[id];
    if (d) snap.drivers[id] = { ...d, undercut_flag: flag, undercut_prob: flag ? 0.66 : null };
  }
  return JSON.stringify({ type: "snapshot", lap, ...snap, event_id: `snapshot-${lap}`, sse_sent_at_ms: 1, ...extra });
}
