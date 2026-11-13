/** Typed client for the Step 13 API. Every response is checked at runtime, so a bad payload is an
 * ``ApiError`` instead of a crash. Types mirror ``src/f1sa/api/models.py``. */

import { API_URL } from "./config";
import { isBool, isDriverId, isFiniteNumber, isInt, isObj, isStr, nullable } from "./guard";

export const COMPOUNDS = ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "UNKNOWN"] as const;
export type Compound = (typeof COMPOUNDS)[number];
export const isCompound = (v: unknown): v is Compound =>
  isStr(v) && (COMPOUNDS as readonly string[]).includes(v);

export interface Race {
  season: number;
  round: number;
  name: string | null;
  laps: number;
  synthetic: boolean;
}

/** The 9 fields of the Redis driver hash (``""`` arrives as null). */
export interface DriverState {
  position: number;
  gap_ahead_s: number | null;
  gap_leader_s: number | null;
  tyre_compound: Compound;
  tyre_age_laps: number | null;
  last_lap_time_s: number | null;
  undercut_flag: boolean;
  undercut_prob: number | null;
  updated_at: string;
}

export interface RaceSnapshot {
  season: number;
  round: number;
  drivers: Record<string, DriverState>;
  order: { driver_id: string; position: number }[];
}

export interface Lake {
  scheme: "file" | "hdfs";
  mode: "sample" | "full" | null;
  synthetic: boolean | null;
}

export interface Health {
  status: "ok" | "degraded";
  lake: Lake;
}

export type ReplayState = "starting" | "running" | "finished" | "failed";

export interface Replay {
  season: number;
  round: number;
  speed: number;
  run_id: string;
  state: ReplayState;
  error: string | null;
  started_at: string | null;
  ended_at: string | null;
}

export interface ReplayStatus {
  enabled: boolean;
  max_running: number;
  running: Replay[];
  recent: Replay[];
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

// ------------------------------------------------------------------ guards (exported for tests)
export function toRace(v: unknown): Race | undefined {
  if (!isObj(v) || !isInt(v.season) || !isInt(v.round) || !isInt(v.laps) || !isBool(v.synthetic)) {
    return undefined;
  }
  const name = nullable(v.name, isStr);
  if (name === undefined) return undefined;
  return { season: v.season, round: v.round, name, laps: v.laps, synthetic: v.synthetic };
}

export function toRaces(v: unknown): Race[] | undefined {
  if (!Array.isArray(v)) return undefined;
  const out: Race[] = [];
  for (const item of v) {
    const race = toRace(item);
    if (race === undefined) return undefined;
    out.push(race);
  }
  return out;
}

export function toDriverState(v: unknown): DriverState | undefined {
  if (!isObj(v) || !isInt(v.position) || !isCompound(v.tyre_compound) || !isBool(v.undercut_flag)) {
    return undefined;
  }
  if (!isStr(v.updated_at)) return undefined;
  const gapAhead = nullable(v.gap_ahead_s, isFiniteNumber);
  const gapLeader = nullable(v.gap_leader_s, isFiniteNumber);
  const tyreAge = nullable(v.tyre_age_laps, isInt);
  const lastLap = nullable(v.last_lap_time_s, isFiniteNumber);
  const prob = nullable(v.undercut_prob, isFiniteNumber);
  if ([gapAhead, gapLeader, tyreAge, lastLap, prob].some((x) => x === undefined)) return undefined;
  return {
    position: v.position,
    gap_ahead_s: gapAhead ?? null,
    gap_leader_s: gapLeader ?? null,
    tyre_compound: v.tyre_compound,
    tyre_age_laps: tyreAge ?? null,
    last_lap_time_s: lastLap ?? null,
    undercut_flag: v.undercut_flag,
    undercut_prob: prob ?? null,
    updated_at: v.updated_at,
  };
}

export function toSnapshot(v: unknown): RaceSnapshot | undefined {
  if (!isObj(v) || !isInt(v.season) || !isInt(v.round) || !isObj(v.drivers)) return undefined;
  if (!Array.isArray(v.order)) return undefined;
  const drivers: Record<string, DriverState> = {};
  for (const [id, raw] of Object.entries(v.drivers)) {
    const d = toDriverState(raw);
    if (!isDriverId(id) || d === undefined) return undefined;
    drivers[id] = d;
  }
  const order: RaceSnapshot["order"] = [];
  for (const o of v.order) {
    if (!isObj(o) || !isDriverId(o.driver_id) || !isInt(o.position)) return undefined;
    order.push({ driver_id: o.driver_id, position: o.position });
  }
  return { season: v.season, round: v.round, drivers, order };
}

export function toHealth(v: unknown): Health | undefined {
  if (!isObj(v) || (v.status !== "ok" && v.status !== "degraded") || !isObj(v.lake)) {
    return undefined;
  }
  const { scheme, mode, synthetic } = v.lake;
  if (scheme !== "file" && scheme !== "hdfs") return undefined;
  if (mode !== "sample" && mode !== "full" && mode !== null) return undefined;
  if (synthetic !== null && !isBool(synthetic)) return undefined;
  return { status: v.status, lake: { scheme, mode, synthetic } };
}

const REPLAY_STATES: readonly ReplayState[] = ["starting", "running", "finished", "failed"];

export function toReplay(v: unknown): Replay | undefined {
  if (!isObj(v) || !isInt(v.season) || !isInt(v.round) || !isInt(v.speed) || !isStr(v.run_id)) {
    return undefined;
  }
  if (!isStr(v.state) || !(REPLAY_STATES as readonly string[]).includes(v.state)) return undefined;
  const error = nullable(v.error, isStr);
  const started = nullable(v.started_at, isStr);
  const ended = nullable(v.ended_at, isStr);
  if (error === undefined || started === undefined || ended === undefined) return undefined;
  return {
    season: v.season,
    round: v.round,
    speed: v.speed,
    run_id: v.run_id,
    state: v.state as ReplayState,
    error,
    started_at: started,
    ended_at: ended,
  };
}

export function toReplayStatus(v: unknown): ReplayStatus | undefined {
  if (!isObj(v) || !isBool(v.enabled) || !isInt(v.max_running)) return undefined;
  if (!Array.isArray(v.running) || !Array.isArray(v.recent)) return undefined;
  const running = v.running.map(toReplay);
  const recent = v.recent.map(toReplay);
  if (running.includes(undefined) || recent.includes(undefined)) return undefined;
  return {
    enabled: v.enabled,
    max_running: v.max_running,
    running: running as Replay[],
    recent: recent as Replay[],
  };
}

// ------------------------------------------------------------------ client
function detailOf(body: unknown): string | null {
  return isObj(body) && isStr(body.detail) ? body.detail : null;
}

export async function request<T>(
  path: string,
  parse: (v: unknown) => T | undefined,
  init: RequestInit & { okStatuses?: number[] } = {},
): Promise<T> {
  let resp: Response;
  try {
    resp = await fetch(`${API_URL}${path}`, { cache: "no-store", ...init });
  } catch {
    throw new ApiError("API not reachable", null);
  }
  let body: unknown = null;
  try {
    body = await resp.json();
  } catch {
    body = null;
  }
  const ok = init.okStatuses ?? [200];
  if (!ok.includes(resp.status)) {
    throw new ApiError(detailOf(body) ?? `HTTP ${resp.status}`, resp.status);
  }
  const parsed = parse(body);
  if (parsed === undefined) throw new ApiError("unexpected response from the API", resp.status);
  return parsed;
}

export const racePath = (season: number, round: number): string => `/api/races/${season}/${round}`;

export const api = {
  races: (signal?: AbortSignal) => request("/api/races", toRaces, signal ? { signal } : {}),
  /** Health answers 503 with the same body when a dependency is down; both are fine here. */
  health: (signal?: AbortSignal) =>
    request("/api/health", toHealth, { okStatuses: [200, 503], ...(signal ? { signal } : {}) }),
  state: (season: number, round: number, signal?: AbortSignal) =>
    request(`${racePath(season, round)}/state`, toSnapshot, signal ? { signal } : {}),
  replays: (signal?: AbortSignal) => request("/api/replays", toReplayStatus, signal ? { signal } : {}),
  startReplay: (season: number, round: number, speed: 1 | 10 | 60) =>
    request(`${racePath(season, round)}/replay`, toReplay, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ speed }),
      okStatuses: [202],
    }),
  postLatency: async (eventId: string, uiRenderedAtMs: number): Promise<number> => {
    try {
      const resp = await fetch(`${API_URL}/api/debug/latency`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event_id: eventId, ui_rendered_at_ms: Math.round(uiRenderedAtMs) }),
      });
      return resp.status;
    } catch {
      return 0;
    }
  },
  eventsUrl: (season: number, round: number) => `${API_URL}${racePath(season, round)}/events`,
};
