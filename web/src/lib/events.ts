/** SSE messages from ``GET /api/races/{s}/{r}/events``, parsed and validated.
 *
 * A race event's data line is the stored Redis message plus ``event_id`` and ``sse_sent_at_ms``:
 * ``{type, season, round, event_ts, published_at, payload, event_id, sse_sent_at_ms}``.
 * A ``snapshot`` (pushed by the API after connect and once per leader lap) carries the race's full
 * Redis state: ``{type: "snapshot", season, round, lap, drivers, order, event_id: "snapshot-<lap>",
 * sse_sent_at_ms}``.
 * ``parseEvent`` returns ``null`` for anything that does not match: the reducer counts it as
 * malformed and carries on. */

import { type Compound, isCompound, type RaceSnapshot, toSnapshot } from "./api";
import { isDriverId, isFiniteNumber, isInt, isObj, isStr, nullable, type Obj, UUID } from "./guard";

export type AlertReason = "opportunity" | "attempt";
export type AlertTrigger = "new_candidate" | "probability_crossed" | "pit_in";

interface Base {
  eventId: string;
  season: number;
  round: number;
  sseSentAtMs: number | null;
}

export interface LapEvent extends Base {
  type: "lap";
  driverId: string;
  lap: number;
  raceTimeS: number;
  position: number | null;
  lapTimeS: number | null;
  compound: Compound;
  tyreAge: number | null;
}

export interface PitEvent extends Base {
  type: "pit_in" | "pit_out";
  driverId: string;
  lap: number;
  raceTimeS: number | null;
}

export interface AlertEvent extends Base {
  type: "undercut_alert";
  reason: AlertReason;
  trigger: AlertTrigger;
  attackerId: string;
  defenderId: string;
  decisionLap: number;
  gapS: number;
  prob: number;
  threshold: number;
}

export interface SessionEndEvent extends Base {
  type: "session_end";
}

export interface SnapshotEvent extends Base {
  type: "snapshot";
  lap: number;
  data: RaceSnapshot;
}

export type RaceEvent = LapEvent | PitEvent | AlertEvent | SessionEndEvent | SnapshotEvent;

const SNAPSHOT_ID = /^snapshot-\d+$/;

function snapshot(msg: Obj): SnapshotEvent | null {
  if (!isStr(msg.event_id) || !SNAPSHOT_ID.test(msg.event_id) || !isInt(msg.lap) || msg.lap < 0) return null;
  const data = toSnapshot(msg);
  if (data === undefined) return null;
  const sent = nullable(msg.sse_sent_at_ms, isInt);
  return {
    type: "snapshot",
    eventId: msg.event_id,
    season: data.season,
    round: data.round,
    sseSentAtMs: sent ?? null,
    lap: msg.lap,
    data,
  };
}

const TRIGGERS: Record<AlertReason, readonly AlertTrigger[]> = {
  opportunity: ["new_candidate", "probability_crossed"],
  attempt: ["pit_in"],
};

const isProb = (v: unknown): v is number => isFiniteNumber(v) && v >= 0 && v <= 1;

function lap(payload: Obj, base: Base): LapEvent | null {
  const { driver_id, lap: n, race_time_s, compound } = payload;
  if (!isDriverId(driver_id) || !isInt(n) || n < 1) return null;
  if (!isFiniteNumber(race_time_s) || race_time_s < 0) return null;
  const position = nullable(payload.position, isInt);
  const lapTime = nullable(payload.lap_time_s, isFiniteNumber);
  const tyreAge = nullable(payload.tyre_age, isInt);
  if (position === undefined || lapTime === undefined || tyreAge === undefined) return null;
  if (compound !== null && compound !== undefined && !isStr(compound)) return null;
  return {
    ...base,
    type: "lap",
    driverId: driver_id,
    lap: n,
    raceTimeS: race_time_s,
    position,
    lapTimeS: lapTime,
    compound: isCompound(compound) ? compound : "UNKNOWN",
    tyreAge,
  };
}

function pit(type: "pit_in" | "pit_out", payload: Obj, base: Base): PitEvent | null {
  if (!isDriverId(payload.driver_id) || !isInt(payload.lap) || payload.lap < 1) return null;
  const raceTime = nullable(payload.race_time_s, isFiniteNumber);
  if (raceTime === undefined) return null;
  return { ...base, type, driverId: payload.driver_id, lap: payload.lap, raceTimeS: raceTime };
}

function alert(payload: Obj, base: Base): AlertEvent | null {
  const { reason, trigger, attacker_id, defender_id, decision_lap, gap_s, undercut_prob, threshold } =
    payload;
  if (reason !== "opportunity" && reason !== "attempt") return null;
  if (!isStr(trigger) || !(TRIGGERS[reason] as readonly string[]).includes(trigger)) return null;
  if (!isDriverId(attacker_id) || !isDriverId(defender_id) || attacker_id === defender_id) return null;
  if (!isInt(decision_lap) || decision_lap < 1 || !isFiniteNumber(gap_s) || gap_s < 0) return null;
  if (!isProb(undercut_prob) || !isProb(threshold)) return null;
  return {
    ...base,
    type: "undercut_alert",
    reason,
    trigger: trigger as AlertTrigger,
    attackerId: attacker_id,
    defenderId: defender_id,
    decisionLap: decision_lap,
    gapS: gap_s,
    prob: undercut_prob,
    threshold,
  };
}

/** Parse one SSE ``data`` line; ``null`` when it is not a valid race event. Never throws. */
export function parseEvent(raw: string): RaceEvent | null {
  let msg: unknown;
  try {
    msg = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isObj(msg) || !isStr(msg.type) || !isInt(msg.season) || !isInt(msg.round)) return null;
  if (msg.type === "snapshot") return snapshot(msg);
  if (!isStr(msg.event_id) || !UUID.test(msg.event_id) || !isObj(msg.payload)) return null;
  const sent = nullable(msg.sse_sent_at_ms, isInt);
  const base: Base = {
    eventId: msg.event_id,
    season: msg.season,
    round: msg.round,
    sseSentAtMs: sent ?? null,
  };
  switch (msg.type) {
    case "lap":
      return lap(msg.payload, base);
    case "pit_in":
    case "pit_out":
      return pit(msg.type, msg.payload, base);
    case "undercut_alert":
      return alert(msg.payload, base);
    case "session_end":
      return { ...base, type: "session_end" };
    default:
      return null;
  }
}
