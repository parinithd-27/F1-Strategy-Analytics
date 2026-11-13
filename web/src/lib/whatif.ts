/** What-if (Step 15): types and runtime guard of ``POST /api/races/{s}/{r}/whatif`` (mirrors
 * ``WhatIfOut`` in ``src/f1sa/api/models.py``) and the pure helpers of the panel. */

import { ApiError, isCompound, racePath, request, type Compound } from "./api";
import { DASH } from "./format";
import { isBool, isDriverId, isFiniteNumber, isInt, isObj, isStr, nullable } from "./guard";

export const STRATEGIES = ["pit_now", "pit_next_lap", "stay_out"] as const;
export type StrategyName = (typeof STRATEGIES)[number];
export type ResponseName = "primary" | "sensitivity";

export const STRATEGY_LABEL: Record<StrategyName, string> = {
  pit_now: "Pit now",
  pit_next_lap: "Pit next lap",
  stay_out: "Stay out (overcut)",
};

export interface Strategy {
  strategy: StrategyName;
  response: ResponseName;
  attacker_pit_lap: number;
  defender_pit_lap: number;
  evaluation_lap: number;
  feasible: boolean;
  reason: string | null;
  p_attacker_ahead: number | null;
  expected_gap_s: number | null;
  interval80_s: [number, number] | null;
  mc_se: number | null;
  attacker_new_compound: Compound;
  defender_new_compound: Compound;
}

export interface CarInputs {
  driver_id: string;
  compound: Compound;
  new_compound: Compound;
  tyre_age: number;
  compound_source: string;
  noise_source: string;
  stop_loss_s: number | null;
}

export interface WhatIfInputs {
  current_lap: number | null;
  decision_lap: number | null;
  race_laps: number | null;
  gap_s: number;
  history: string;
  traffic_cars: number;
  pit_loss_prior_s: number | null;
  pit_loss_source: string | null;
  synthetic: boolean;
  warnings: string[];
  attacker: CarInputs;
  defender: CarInputs;
}

export interface WhatIf {
  season: number;
  round: number;
  attacker: string;
  defender: string;
  n: number;
  seed: number;
  strategies: Strategy[];
  sensitivity: Strategy[];
  model: { probability: number | null; undercut_flag: boolean; model_version: string };
  inputs: WhatIfInputs;
  runtime_ms: number;
}

const isProb = (v: unknown): v is number => isFiniteNumber(v) && v >= 0 && v <= 1;
const isStrategyName = (v: unknown): v is StrategyName =>
  isStr(v) && (STRATEGIES as readonly string[]).includes(v);

export function toStrategy(v: unknown, response: ResponseName): Strategy | undefined {
  if (!isObj(v) || !isStrategyName(v.strategy) || v.response !== response) return undefined;
  for (const k of ["attacker_pit_lap", "defender_pit_lap", "evaluation_lap"]) if (!isInt(v[k])) return undefined;
  if (!isBool(v.feasible) || !isCompound(v.attacker_new_compound) || !isCompound(v.defender_new_compound)) return undefined;
  const reason = nullable(v.reason, isStr);
  const p = nullable(v.p_attacker_ahead, isProb);
  const gap = nullable(v.expected_gap_s, isFiniteNumber);
  const se = nullable(v.mc_se, isFiniteNumber);
  let interval: [number, number] | null | undefined;
  if (v.interval80_s === null) interval = null;
  else if (Array.isArray(v.interval80_s) && v.interval80_s.length === 2 && v.interval80_s.every(isFiniteNumber))
    interval = [v.interval80_s[0] as number, v.interval80_s[1] as number];
  if ([reason, p, gap, se, interval].includes(undefined)) return undefined;
  if (v.feasible && (p === null || gap === null || interval === null)) return undefined;
  return {
    strategy: v.strategy,
    response,
    attacker_pit_lap: v.attacker_pit_lap as number,
    defender_pit_lap: v.defender_pit_lap as number,
    evaluation_lap: v.evaluation_lap as number,
    feasible: v.feasible,
    reason: reason ?? null,
    p_attacker_ahead: p ?? null,
    expected_gap_s: gap ?? null,
    interval80_s: interval ?? null,
    mc_se: se ?? null,
    attacker_new_compound: v.attacker_new_compound,
    defender_new_compound: v.defender_new_compound,
  };
}

function toCar(v: unknown): CarInputs | undefined {
  if (!isObj(v) || !isDriverId(v.driver_id) || !isCompound(v.compound) || !isCompound(v.new_compound)) return undefined;
  if (!isInt(v.tyre_age) || !isStr(v.compound_source) || !isStr(v.noise_source)) return undefined;
  const loss = nullable(v.stop_loss_s, isFiniteNumber);
  if (loss === undefined) return undefined;
  return {
    driver_id: v.driver_id,
    compound: v.compound,
    new_compound: v.new_compound,
    tyre_age: v.tyre_age,
    compound_source: v.compound_source,
    noise_source: v.noise_source,
    stop_loss_s: loss,
  };
}

function toInputs(v: unknown): WhatIfInputs | undefined {
  if (!isObj(v)) return undefined;
  const current = nullable(v.current_lap, isInt);
  const decision = nullable(v.decision_lap, isInt);
  const raceLaps = nullable(v.race_laps, isInt);
  const loss = nullable(v.pit_loss_prior_s, isFiniteNumber);
  const source = nullable(v.pit_loss_source, isStr);
  const attacker = toCar(v.attacker);
  const defender = toCar(v.defender);
  if ([current, decision, raceLaps, loss, source].includes(undefined) || !attacker || !defender) return undefined;
  if (!isFiniteNumber(v.gap_s) || !isStr(v.history) || !isInt(v.traffic_cars) || !isBool(v.synthetic)) return undefined;
  if (!Array.isArray(v.warnings) || !v.warnings.every(isStr)) return undefined;
  return {
    current_lap: current ?? null,
    decision_lap: decision ?? null,
    race_laps: raceLaps ?? null,
    gap_s: v.gap_s,
    history: v.history,
    traffic_cars: v.traffic_cars,
    pit_loss_prior_s: loss ?? null,
    pit_loss_source: source ?? null,
    synthetic: v.synthetic,
    warnings: v.warnings as string[],
    attacker,
    defender,
  };
}

/** The whole response, or ``undefined`` if anything is off. */
export function toWhatIf(v: unknown): WhatIf | undefined {
  if (!isObj(v) || !isInt(v.season) || !isInt(v.round) || !isDriverId(v.attacker) || !isDriverId(v.defender)) return undefined;
  if (!isInt(v.n) || !isInt(v.seed) || !isFiniteNumber(v.runtime_ms)) return undefined;
  if (!Array.isArray(v.strategies) || !Array.isArray(v.sensitivity)) return undefined;
  const strategies = v.strategies.map((s) => toStrategy(s, "primary"));
  const sensitivity = v.sensitivity.map((s) => toStrategy(s, "sensitivity"));
  if (strategies.length !== 3 || sensitivity.length !== 3) return undefined;
  if (strategies.includes(undefined) || sensitivity.includes(undefined)) return undefined;
  const m = v.model;
  if (!isObj(m) || !isBool(m.undercut_flag) || !isStr(m.model_version)) return undefined;
  const prob = nullable(m.probability, isProb);
  const inputs = toInputs(v.inputs);
  if (prob === undefined || !inputs) return undefined;
  return {
    season: v.season,
    round: v.round,
    attacker: v.attacker,
    defender: v.defender,
    n: v.n,
    seed: v.seed,
    strategies: strategies as Strategy[],
    sensitivity: sensitivity as Strategy[],
    model: { probability: prob, undercut_flag: m.undercut_flag, model_version: m.model_version },
    inputs,
    runtime_ms: v.runtime_ms,
  };
}

// ------------------------------------------------------------------ view helpers
/** The car directly ahead of ``attacker`` in the live order, or null (leader / unknown). */
export function defaultDefender(order: readonly string[], attacker: string): string | null {
  const i = order.indexOf(attacker);
  return i > 0 ? (order[i - 1] ?? null) : null;
}

/** Cars that can attack: every car with a car ahead. */
export function attackerChoices(order: readonly string[]): string[] {
  return order.slice(1);
}

/** Bar width in percent, clamped to 0-100 (0 for unknown). */
export function barWidth(p: number | null): number {
  if (p === null || !Number.isFinite(p)) return 0;
  return Math.min(100, Math.max(0, p * 100));
}

/** ``cum_D - cum_A``: positive means the attacker is ahead. */
export function gapOutcome(seconds: number | null): string {
  if (seconds === null) return DASH;
  const s = Math.abs(seconds).toFixed(1);
  if (Math.abs(seconds) < 0.05) return "level";
  return seconds > 0 ? `${s} s ahead` : `${s} s behind`;
}

const signed = (x: number): string => `${x >= 0 ? "+" : "−"}${Math.abs(x).toFixed(1)}`;

export function intervalText(interval: [number, number] | null): string {
  if (interval === null) return DASH;
  return `${signed(interval[0])} to ${signed(interval[1])} s`;
}

export function timelineText(s: Strategy): string {
  return `attacker in L${s.attacker_pit_lap}, defender in L${s.defender_pit_lap}, compared on L${s.evaluation_lap}`;
}

export const REASON_TEXT: Record<string, string> = {
  race_end: "Not possible: the race ends first",
  no_pit_loss_prior: "Not available: no pit-loss prior for this race",
};

export function reasonText(reason: string | null): string {
  return reason === null ? "" : (REASON_TEXT[reason] ?? reason);
}

export const WARNING_TEXT: Record<string, string> = {
  no_lap_history:
    "No lap history yet (the page connected mid-race): pace and noise come from the latest lap only.",
  attacker_on_pit_lap: "The attacker is on a pit lap.",
  defender_on_pit_lap: "The defender is on a pit lap.",
};

export function warningTexts(warnings: readonly string[]): string[] {
  return warnings.filter((w) => w in WARNING_TEXT).map((w) => WARNING_TEXT[w] as string);
}

export function whatifMessage(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.status === 409) return err.message.includes("not directly ahead") ? `${err.message}. Pick the car directly ahead.` : err.message;
    if (err.status === 404) return err.message;
    if (err.status === 422) return "The request was not valid (pick two different cars).";
    if (err.status === 503) return `The simulator is not available: ${err.message}`;
    if (err.status === null) return "The API is not reachable.";
    return err.message;
  }
  return "Unexpected error.";
}

export interface WhatIfRequest {
  attacker: string;
  defender: string;
  n?: number;
  seed?: number;
}

/** ``POST /api/races/{s}/{r}/whatif``. */
export function fetchWhatIf(season: number, round: number, body: WhatIfRequest, signal?: AbortSignal): Promise<WhatIf> {
  return request(`${racePath(season, round)}/whatif`, toWhatIf, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    ...(signal ? { signal } : {}),
  });
}
