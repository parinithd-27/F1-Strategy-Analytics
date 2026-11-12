/** The one race reducer: a pure function from (state, action) to state. Components only render it.
 *
 * Sources of truth:
 * - **Snapshot**: the 9 hash fields and the order sorted set from Redis. It is pushed by the API on
 *   the SSE stream (an ``event: snapshot`` after connect and once per leader lap, never polled),
 *   and loaded from ``GET /state`` on a reconnect. It replaces the tower values (stale
 *   ``undercut_flag`` / ``undercut_prob`` included) and the order, and keeps the client-built lap
 *   history and alert groups (Pub/Sub has no history, so they cannot be reloaded).
 * - **SSE events** between snapshots:
 *   - `lap` updates the driver's latest lap, compound, tyre age and last lap time. Gaps come from
 *     ``race_time_s`` at the same lap number, the ``timing_gaps`` definition the stream job uses:
 *     the leader is the car with Jolpica position 1, and the car ahead has position − 1.
 *   - The order follows the stream job's live rank (``live_state.live_order``): laps completed
 *     desc, then crossing time, then Jolpica position.
 *   - `pit_in` / `pit_out` mark the car in the pit lane, `undercut_alert` updates the
 *     (attacker, defender) group, and `session_end` closes the race.
 *
 * Robustness: malformed messages, other races' messages and duplicate event ids change nothing but
 * counters. A lap older than the driver's latest one goes into the history only, so the tower never
 * regresses. An unknown driver gets a row. Nothing in here throws on input. */

import { announcementFor } from "./announce";
import type { Compound, RaceSnapshot } from "./api";
import { type AlertEvent, type LapEvent, parseEvent, type PitEvent, type RaceEvent } from "./events";

export const HISTORY_LAPS = 20;
export const MAX_SEEN_IDS = 5000;
export const MAX_ANNOUNCEMENTS = 5;

export type Connection = "connecting" | "connected" | "reconnecting" | "closed";

export interface DriverRow {
  driverId: string;
  position: number | null;
  gapAheadS: number | null;
  gapLeaderS: number | null;
  lastLapTimeS: number | null;
  compound: Compound;
  tyreAgeLaps: number | null;
  undercutFlag: boolean;
  undercutProb: number | null;
  /** Latest lap seen on the stream (null until the first lap event). */
  lap: number | null;
  raceTimeS: number | null;
  jolpicaPosition: number | null;
  inPit: boolean;
  stops: number;
  lastPitLap: number | null;
  /** True once the driver appeared in a snapshot. */
  known: boolean;
}

export interface LapPoint {
  raceTimeS: number;
  position: number | null;
  lapTimeS: number | null;
}

export interface AlertGroup {
  key: string;
  attackerId: string;
  defenderId: string;
  reason: AlertEvent["reason"];
  trigger: AlertEvent["trigger"];
  prob: number;
  threshold: number;
  decisionLap: number;
  gapS: number;
  count: number;
  /** Increases with every alert, so the feed can sort newest first. */
  seq: number;
}

export interface Announcement {
  id: string;
  text: string;
}

export interface RaceState {
  season: number;
  round: number;
  drivers: Record<string, DriverRow>;
  order: string[];
  /** lap -> driver -> crossing, for the gap chart (the last ``HISTORY_LAPS`` laps). */
  history: Record<number, Record<string, LapPoint>>;
  maxLap: number;
  alerts: Record<string, AlertGroup>;
  alertSeq: number;
  announcements: Announcement[];
  connection: Connection;
  sessionEnded: boolean;
  malformed: number;
  foreign: number;
  duplicates: number;
  unknownDrivers: number;
  seen: string[];
  lastEvent: { id: string; type: RaceEvent["type"] } | null;
  snapshots: number;
}

export type RaceAction =
  | { type: "snapshot"; data: RaceSnapshot }
  | { type: "event"; raw: string }
  | { type: "connection"; status: Connection }
  | { type: "reset"; season: number; round: number };

export function initialState(season: number, round: number): RaceState {
  return {
    season,
    round,
    drivers: {},
    order: [],
    history: {},
    maxLap: 0,
    alerts: {},
    alertSeq: 0,
    announcements: [],
    connection: "connecting",
    sessionEnded: false,
    malformed: 0,
    foreign: 0,
    duplicates: 0,
    unknownDrivers: 0,
    seen: [],
    lastEvent: null,
    snapshots: 0,
  };
}

function emptyRow(driverId: string): DriverRow {
  return {
    driverId,
    position: null,
    gapAheadS: null,
    gapLeaderS: null,
    lastLapTimeS: null,
    compound: "UNKNOWN",
    tyreAgeLaps: null,
    undercutFlag: false,
    undercutProb: null,
    lap: null,
    raceTimeS: null,
    jolpicaPosition: null,
    inPit: false,
    stops: 0,
    lastPitLap: null,
    known: false,
  };
}

const round3 = (x: number): number => Math.round(x * 1000) / 1000;

/** Positions 1..n from ``order``, written into the rows. */
function withPositions(drivers: Record<string, DriverRow>, order: string[]): Record<string, DriverRow> {
  const out: Record<string, DriverRow> = { ...drivers };
  order.forEach((id, i) => {
    const row = out[id];
    if (row && row.position !== i + 1) out[id] = { ...row, position: i + 1 };
  });
  return out;
}

/** The stream job's live rank for drivers with lap data; the others keep their previous order. */
export function liveOrder(drivers: Record<string, DriverRow>, previous: string[]): string[] {
  const timed = Object.values(drivers).filter((d) => d.lap !== null && d.raceTimeS !== null);
  timed.sort(
    (a, b) =>
      (b.lap ?? 0) - (a.lap ?? 0) ||
      (a.raceTimeS ?? 0) - (b.raceTimeS ?? 0) ||
      (a.jolpicaPosition ?? 1e6) - (b.jolpicaPosition ?? 1e6) ||
      a.driverId.localeCompare(b.driverId),
  );
  const ranked = timed.map((d) => d.driverId);
  const rankedSet = new Set(ranked);
  const rest = [
    ...previous.filter((id) => !rankedSet.has(id) && id in drivers),
    ...Object.keys(drivers)
      .filter((id) => !rankedSet.has(id) && !previous.includes(id))
      .sort(),
  ];
  return [...ranked, ...rest];
}

/** Gap to the leader and to the car ahead at ``lap`` (same lap number), from the history. */
export function gapsAt(
  history: RaceState["history"],
  lap: number,
  driverId: string,
): { leader: number | null; ahead: number | null } {
  const atLap = history[lap];
  const me = atLap?.[driverId];
  if (!atLap || !me) return { leader: null, ahead: null };
  const points = Object.values(atLap);
  const leader = points.find((p) => p.position === 1);
  const ahead =
    me.position !== null && me.position > 1
      ? points.find((p) => p.position === (me.position as number) - 1)
      : undefined;
  return {
    leader: leader ? round3(me.raceTimeS - leader.raceTimeS) : me.position === 1 ? 0 : null,
    ahead: me.position === 1 ? null : ahead ? round3(me.raceTimeS - ahead.raceTimeS) : null,
  };
}

function trimHistory(history: RaceState["history"], maxLap: number): RaceState["history"] {
  const minLap = maxLap - HISTORY_LAPS + 1;
  const out: RaceState["history"] = {};
  for (const [k, v] of Object.entries(history)) {
    if (Number(k) >= minLap) out[Number(k)] = v;
  }
  return out;
}

function ensureRow(state: RaceState, driverId: string): { row: DriverRow; unknown: boolean } {
  const row = state.drivers[driverId];
  return row ? { row, unknown: false } : { row: emptyRow(driverId), unknown: true };
}

function applyLap(state: RaceState, e: LapEvent): RaceState {
  const point: LapPoint = { raceTimeS: e.raceTimeS, position: e.position, lapTimeS: e.lapTimeS };
  const maxLap = Math.max(state.maxLap, e.lap);
  const history = trimHistory(
    { ...state.history, [e.lap]: { ...(state.history[e.lap] ?? {}), [e.driverId]: point } },
    maxLap,
  );
  const { row, unknown } = ensureRow(state, e.driverId);
  const drivers: Record<string, DriverRow> = { ...state.drivers };
  if (row.lap === null || e.lap >= row.lap) {
    drivers[e.driverId] = {
      ...row,
      lap: e.lap,
      raceTimeS: e.raceTimeS,
      jolpicaPosition: e.position,
      lastLapTimeS: e.lapTimeS,
      compound: e.compound,
      tyreAgeLaps: e.tyreAge,
    };
  } else {
    drivers[e.driverId] = row; // an older lap: history only, the tower never regresses
  }
  // Recompute the gaps of every car whose latest lap is this lap (a late leader event fixes them).
  for (const [id, r] of Object.entries(drivers)) {
    if (r.lap !== e.lap) continue;
    const g = gapsAt(history, e.lap, id);
    drivers[id] = {
      ...r,
      gapLeaderS: g.leader ?? r.gapLeaderS,
      gapAheadS: r.jolpicaPosition === 1 ? null : (g.ahead ?? r.gapAheadS),
    };
  }
  const order = liveOrder(drivers, state.order);
  return {
    ...state,
    history,
    maxLap,
    drivers: withPositions(drivers, order),
    order,
    unknownDrivers: state.unknownDrivers + (unknown ? 1 : 0),
  };
}

function applyPit(state: RaceState, e: PitEvent): RaceState {
  const { row, unknown } = ensureRow(state, e.driverId);
  const next: DriverRow =
    e.type === "pit_in"
      ? { ...row, inPit: true, lastPitLap: e.lap, stops: row.stops + 1 }
      : { ...row, inPit: false };
  const drivers = { ...state.drivers, [e.driverId]: next };
  const order = unknown ? liveOrder(drivers, state.order) : state.order;
  return {
    ...state,
    drivers: unknown ? withPositions(drivers, order) : drivers,
    order,
    unknownDrivers: state.unknownDrivers + (unknown ? 1 : 0),
  };
}

export const alertKey = (attackerId: string, defenderId: string): string =>
  `${attackerId}>${defenderId}`;

function applyAlert(state: RaceState, e: AlertEvent): RaceState {
  const key = alertKey(e.attackerId, e.defenderId);
  const seq = state.alertSeq + 1;
  const previous = state.alerts[key];
  const group: AlertGroup = {
    key,
    attackerId: e.attackerId,
    defenderId: e.defenderId,
    reason: e.reason,
    trigger: e.trigger,
    prob: e.prob,
    threshold: e.threshold,
    decisionLap: e.decisionLap,
    gapS: e.gapS,
    count: (previous?.count ?? 0) + 1,
    seq,
  };
  const { row, unknown } = ensureRow(state, e.attackerId);
  let drivers: Record<string, DriverRow> = {
    ...state.drivers,
    [e.attackerId]: { ...row, undercutFlag: true, undercutProb: e.prob },
  };
  let order = state.order;
  if (unknown) {
    order = liveOrder(drivers, state.order);
    drivers = withPositions(drivers, order);
  }
  const text = announcementFor(e);
  const announcements = text
    ? [...state.announcements, { id: e.eventId, text }].slice(-MAX_ANNOUNCEMENTS)
    : state.announcements;
  return {
    ...state,
    alerts: { ...state.alerts, [key]: group },
    alertSeq: seq,
    drivers,
    order,
    announcements,
    unknownDrivers: state.unknownDrivers + (unknown ? 1 : 0),
  };
}

function applySnapshot(state: RaceState, data: RaceSnapshot): RaceState {
  if (data.season !== state.season || data.round !== state.round) {
    return { ...state, foreign: state.foreign + 1 };
  }
  const drivers: Record<string, DriverRow> = { ...state.drivers };
  for (const [id, h] of Object.entries(data.drivers)) {
    const row = drivers[id] ?? emptyRow(id);
    drivers[id] = {
      ...row,
      known: true,
      gapAheadS: h.gap_ahead_s,
      gapLeaderS: h.gap_leader_s,
      lastLapTimeS: h.last_lap_time_s,
      compound: h.tyre_compound,
      tyreAgeLaps: h.tyre_age_laps,
      undercutFlag: h.undercut_flag,
      undercutProb: h.undercut_prob,
    };
  }
  // The order sorted set is authoritative; drivers it does not (yet) hold follow in their old order.
  const zset = [...data.order].sort((a, b) => a.position - b.position).map((o) => o.driver_id);
  for (const id of zset) if (!(id in drivers)) drivers[id] = { ...emptyRow(id), known: true };
  const inZset = new Set(zset);
  const rest = [
    ...state.order.filter((id) => !inZset.has(id) && id in drivers),
    ...Object.keys(drivers)
      .filter((id) => !inZset.has(id) && !state.order.includes(id))
      .sort(),
  ];
  const order = [...zset, ...rest];
  return {
    ...state,
    drivers: withPositions(drivers, order),
    order,
    snapshots: state.snapshots + 1,
  };
}

function remember(seen: string[], id: string): string[] {
  const next = [...seen, id];
  return next.length > MAX_SEEN_IDS ? next.slice(next.length - MAX_SEEN_IDS) : next;
}

function applyEvent(state: RaceState, raw: string): RaceState {
  const e = parseEvent(raw);
  if (e === null) return { ...state, malformed: state.malformed + 1 };
  if (e.season !== state.season || e.round !== state.round) {
    return { ...state, foreign: state.foreign + 1 };
  }
  if (state.sessionEnded) return { ...state, foreign: state.foreign + 1 };
  // Snapshots replace state; their ids repeat across connections, so they are not deduplicated.
  if (e.type === "snapshot") return applySnapshot(state, e.data);
  if (state.seen.includes(e.eventId)) return { ...state, duplicates: state.duplicates + 1 };
  let next: RaceState;
  switch (e.type) {
    case "lap":
      next = applyLap(state, e);
      break;
    case "pit_in":
    case "pit_out":
      next = applyPit(state, e);
      break;
    case "undercut_alert":
      next = applyAlert(state, e);
      break;
    case "session_end":
      next = { ...state, sessionEnded: true, connection: "closed" };
      break;
  }
  return { ...next, seen: remember(state.seen, e.eventId), lastEvent: { id: e.eventId, type: e.type } };
}

export function raceReducer(state: RaceState, action: RaceAction): RaceState {
  switch (action.type) {
    case "snapshot":
      return applySnapshot(state, action.data);
    case "event":
      return applyEvent(state, action.raw);
    case "connection":
      if (state.sessionEnded) return state.connection === "closed" ? state : { ...state, connection: "closed" };
      return state.connection === action.status ? state : { ...state, connection: action.status };
    case "reset":
      return initialState(action.season, action.round);
  }
}

/** Tower rows in display order (unknown ids in ``order`` are skipped). */
export function towerRows(state: RaceState): DriverRow[] {
  return state.order.map((id) => state.drivers[id]).filter((r): r is DriverRow => r !== undefined);
}

/** Alert groups, newest first. */
export function alertGroups(state: RaceState): AlertGroup[] {
  return Object.values(state.alerts).sort((a, b) => b.seq - a.seq);
}

/** Laps a car is down on the leader. A car is ``k`` laps down when the leader completed lap
 * ``car.lap + k`` before the car completed ``car.lap``: being a lap number behind while the
 * leader has just crossed the line first is not "lapped". Uses the crossings in the history. */
export function lapsDown(state: RaceState, driverId: string): number {
  const row = state.drivers[driverId];
  const leaderId = state.order.find((id) => state.drivers[id]?.lap !== null);
  if (!row || row.lap === null || row.raceTimeS === null || !leaderId || leaderId === driverId) return 0;
  let k = 0;
  for (;;) {
    const leaderCrossing = state.history[row.lap + k + 1]?.[leaderId];
    if (!leaderCrossing || leaderCrossing.raceTimeS >= row.raceTimeS) return k;
    k += 1;
  }
}
