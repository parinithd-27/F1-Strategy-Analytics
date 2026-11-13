import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "@/lib/api";
import {
  attackerChoices,
  barWidth,
  defaultDefender,
  fetchWhatIf,
  gapOutcome,
  intervalText,
  reasonText,
  STRATEGY_LABEL,
  timelineText,
  toStrategy,
  toWhatIf,
  warningTexts,
  whatifMessage,
} from "@/lib/whatif";

const strategy = (name: string, response: string, extra: Record<string, unknown> = {}) => ({
  strategy: name,
  response,
  attacker_pit_lap: 13,
  defender_pit_lap: 14,
  evaluation_lap: 15,
  feasible: true,
  reason: null,
  p_attacker_ahead: 0.62,
  expected_gap_s: 0.8,
  interval80_s: [-2.1, 3.4],
  mc_se: 0.02,
  attacker_new_compound: "HARD",
  defender_new_compound: "HARD",
  ...extra,
});

const car = (id: string) => ({
  driver_id: id,
  position: 3,
  compound: "MEDIUM",
  compound_source: "redis",
  tyre_age: 20,
  new_compound: "HARD",
  pace_laps: 5,
  noise_source: "driver_recent",
  noise_sd_s: 0.6,
  pace_level_s: 90.1,
  stop_loss_s: 21.4,
  stop_delta_s: null,
  outlap_residual_s: null,
  tyre_fit_source: "compound",
  tyre_fit_form: "linear",
});

export const body = (): Record<string, unknown> => ({
  season: 2024,
  round: 5,
  attacker: "hulkenberg",
  defender: "bottas",
  n: 500,
  seed: 42,
  strategies: ["pit_now", "pit_next_lap", "stay_out"].map((s) => strategy(s, "primary")),
  sensitivity: ["pit_now", "pit_next_lap", "stay_out"].map((s) => strategy(s, "sensitivity")),
  model: { probability: 0.44, undercut_flag: true, model_version: "deploy_real_G3.5_k2", source: "redis:undercut_prob" },
  inputs: {
    current_lap: 12,
    decision_lap: 13,
    race_laps: 56,
    gap_s: 1.4,
    gap_source: "race_channel",
    history: "race_channel",
    traffic_cars: 2,
    attacker: car("hulkenberg"),
    defender: car("bottas"),
    pit_loss_prior_s: 21.5,
    pit_loss_source: "global_prior",
    stop_noise_sd_s: 1.2,
    synthetic: true,
    warnings: ["no_lap_history"],
  },
  runtime_ms: 4.2,
});

describe("toWhatIf", () => {
  it("accepts a full response", () => {
    const w = toWhatIf(body());
    expect(w?.strategies.map((s) => s.strategy)).toEqual(["pit_now", "pit_next_lap", "stay_out"]);
    expect(w?.sensitivity.every((s) => s.response === "sensitivity")).toBe(true);
    expect(w?.model.probability).toBe(0.44);
    expect(w?.inputs.synthetic).toBe(true);
    expect(w?.strategies[0]?.interval80_s).toEqual([-2.1, 3.4]);
  });

  it("accepts infeasible strategies with nulls", () => {
    const b = body();
    (b.strategies as unknown[])[2] = strategy("stay_out", "primary", {
      feasible: false,
      reason: "race_end",
      p_attacker_ahead: null,
      expected_gap_s: null,
      interval80_s: null,
      mc_se: null,
    });
    expect(toWhatIf(b)?.strategies[2]?.feasible).toBe(false);
  });

  it.each([
    ["missing model", (b: Record<string, unknown>) => delete b.model],
    ["two strategies", (b: Record<string, unknown>) => (b.strategies = (b.strategies as unknown[]).slice(0, 2))],
    ["bad strategy name", (b: Record<string, unknown>) => ((b.strategies as Record<string, unknown>[])[0]!.strategy = "box")],
    ["probability > 1", (b: Record<string, unknown>) => ((b.strategies as Record<string, unknown>[])[0]!.p_attacker_ahead = 1.2)],
    ["NaN gap", (b: Record<string, unknown>) => ((b.strategies as Record<string, unknown>[])[0]!.expected_gap_s = Number.NaN)],
    ["feasible without p", (b: Record<string, unknown>) => ((b.strategies as Record<string, unknown>[])[0]!.p_attacker_ahead = null)],
    ["response mix-up", (b: Record<string, unknown>) => ((b.sensitivity as Record<string, unknown>[])[0]!.response = "primary")],
    ["bad interval", (b: Record<string, unknown>) => ((b.strategies as Record<string, unknown>[])[0]!.interval80_s = [1])],
    ["bad driver id", (b: Record<string, unknown>) => (b.attacker = "Not An Id")],
    ["bad compound", (b: Record<string, unknown>) => (((b.inputs as Record<string, unknown>).attacker as Record<string, unknown>).compound = "C5")],
    ["warnings not strings", (b: Record<string, unknown>) => ((b.inputs as Record<string, unknown>).warnings = [1])],
  ])("rejects %s", (_name, mutate) => {
    const b = body();
    mutate(b);
    expect(toWhatIf(b)).toBeUndefined();
  });

  it("checks the response of a single strategy", () => {
    expect(toStrategy(strategy("pit_now", "primary"), "sensitivity")).toBeUndefined();
    expect(toStrategy(null, "primary")).toBeUndefined();
  });
});

describe("what-if view helpers", () => {
  const order = ["verstappen", "bottas", "hulkenberg", "ocon"];

  it("defaults the defender to the car ahead", () => {
    expect(defaultDefender(order, "hulkenberg")).toBe("bottas");
    expect(defaultDefender(order, "verstappen")).toBeNull();
    expect(defaultDefender(order, "ghost")).toBeNull();
    expect(attackerChoices(order)).toEqual(["bottas", "hulkenberg", "ocon"]);
    expect(attackerChoices([])).toEqual([]);
  });

  it("formats bars, gaps, intervals, timelines and reasons", () => {
    expect([barWidth(0.62), barWidth(null), barWidth(1.5), barWidth(-0.1)]).toEqual([62, 0, 100, 0]);
    expect([gapOutcome(0.83), gapOutcome(-1.26), gapOutcome(0.01), gapOutcome(null)]).toEqual([
      "0.8 s ahead",
      "1.3 s behind",
      "level",
      "—",
    ]);
    expect(intervalText([-2.14, 3.4])).toBe("−2.1 to +3.4 s");
    expect(intervalText(null)).toBe("—");
    const s = toStrategy(strategy("pit_now", "primary"), "primary")!;
    expect(timelineText(s)).toBe("attacker in L13, defender in L14, compared on L15");
    expect(STRATEGY_LABEL.stay_out).toBe("Stay out (overcut)");
    expect(reasonText("race_end")).toContain("race ends");
    expect(reasonText("other")).toBe("other");
    expect(reasonText(null)).toBe("");
    expect(warningTexts(["no_lap_history", "unknown_flag"])).toHaveLength(1);
  });

  it("turns API errors into text", () => {
    expect(whatifMessage(new ApiError("bottas (P2) is not directly ahead of ocon (P4)", 409))).toContain("Pick the car directly ahead");
    expect(whatifMessage(new ApiError("no decision state", 409))).toBe("no decision state");
    expect(whatifMessage(new ApiError("driver 'x' has no live state", 404))).toContain("no live state");
    expect(whatifMessage(new ApiError("x", 422))).toContain("not valid");
    expect(whatifMessage(new ApiError("tyre model not available", 503))).toContain("not available");
    expect(whatifMessage(new ApiError("API not reachable", null))).toBe("The API is not reachable.");
    expect(whatifMessage(new Error("boom"))).toBe("Unexpected error.");
  });
});

describe("fetchWhatIf", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("posts the pair and parses the response", async () => {
    const calls: [string, RequestInit][] = [];
    vi.stubGlobal("fetch", async (url: string, init: RequestInit) => {
      calls.push([url, init]);
      return new Response(JSON.stringify(body()), { status: 200 });
    });
    const w = await fetchWhatIf(2024, 5, { attacker: "hulkenberg", defender: "bottas" });
    expect(w.attacker).toBe("hulkenberg");
    expect(calls[0]![0]).toMatch(/\/api\/races\/2024\/5\/whatif$/);
    expect(calls[0]![1].method).toBe("POST");
    expect(JSON.parse(calls[0]![1].body as string)).toEqual({ attacker: "hulkenberg", defender: "bottas" });
  });

  it("raises the API's detail on 409", async () => {
    vi.stubGlobal("fetch", async () => new Response(JSON.stringify({ detail: "not directly ahead" }), { status: 409 }));
    await expect(fetchWhatIf(2024, 5, { attacker: "a", defender: "b" })).rejects.toMatchObject({ status: 409, message: "not directly ahead" });
  });
});
