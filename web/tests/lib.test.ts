import { describe, expect, it } from "vitest";

import { toHealth, toRaces, toReplayStatus, toSnapshot } from "@/lib/api";
import { gapSeries, layoutLabels, linePath, scales } from "@/lib/chart";
import { parseApiUrl } from "@/lib/config";
import { newIds } from "@/lib/debugLatency";
import { driverCode, driverName, gap, lapTime, probability } from "@/lib/format";
import { parseRaceParams } from "@/lib/params";
import { initialState, raceReducer } from "@/lib/reducer";
import { replayFor, replayMessage, replayStatusText } from "@/lib/replays";
import { ApiError } from "@/lib/api";
import { filterRaces, groupBySeason } from "@/lib/races";

import { lap, snapshot } from "./helpers";

describe("format", () => {
  it("formats gaps, lap times and probabilities", () => {
    expect([gap(null), gap(0), gap(1.2345)]).toEqual(["—", "Leader", "+1.234"]);
    expect([lapTime(92.456), lapTime(59.1), lapTime(null)]).toEqual(["1:32.456", "59.100", "—"]);
    expect([probability(0.623), probability(null)]).toEqual(["62%", "—"]);
    expect([driverName("max_verstappen"), driverCode("max_verstappen"), driverName("de_vries")]).toEqual(["Verstappen", "VER", "De Vries"]);
  });
});

describe("config and params", () => {
  it("validates the API URL", () => {
    expect(parseApiUrl(undefined)).toBe("http://localhost:8000");
    expect(parseApiUrl("http://127.0.0.1:8001/")).toBe("http://127.0.0.1:8001");
    expect(parseApiUrl("javascript:alert(1)")).toBe("http://localhost:8000");
    expect(parseApiUrl("not a url")).toBe("http://localhost:8000");
  });

  it("uses the key rules for route params", () => {
    expect(parseRaceParams("2024", "5")).toEqual({ season: 2024, round: 5 });
    for (const [s, r] of [["1949", "5"], ["2101", "5"], ["2024", "0"], ["2024", "31"], ["2024", "5x"], ["abcd", "5"]] as const) {
      expect(parseRaceParams(s, r)).toBeNull();
    }
  });
});

describe("api guards", () => {
  it("accepts the API shapes and rejects broken ones", () => {
    expect(toRaces([{ season: 2024, round: 5, name: "Chinese Grand Prix", laps: 56, synthetic: true }])).toHaveLength(1);
    expect(toRaces([{ season: 2024, round: 5, name: "x", laps: 56 }])).toBeUndefined();
    expect(toSnapshot(snapshot(["a", "b"]))?.order).toHaveLength(2);
    expect(toSnapshot({ ...snapshot(["a"]), drivers: { "A!": snapshot(["a"]).drivers.a } })).toBeUndefined();
    expect(toHealth({ status: "ok", checks: {}, lake: { scheme: "file", mode: "sample", synthetic: true } })?.lake.mode).toBe("sample");
    expect(toHealth({ status: "ok", lake: { scheme: "s3", mode: null, synthetic: null } })).toBeUndefined();
    const replay = { season: 2024, round: 5, speed: 60, run_id: "r", state: "failed", error: "stream job not ready within 60 s", started_at: null, ended_at: null };
    expect(toReplayStatus({ enabled: true, max_running: 1, running: [], recent: [replay] })?.recent[0]?.error).toContain("not ready");
    expect(toReplayStatus({ enabled: true, max_running: 1, running: [{ ...replay, state: "weird" }], recent: [] })).toBeUndefined();
  });
});

describe("chart geometry", () => {
  it("builds the last 15 laps of gaps to the leader", () => {
    let s = initialState(2024, 5);
    for (let n = 1; n <= 20; n++) {
      s = raceReducer(s, { type: "event", raw: lap("a", n, n * 90, 1) });
      s = raceReducer(s, { type: "event", raw: lap("b", n, n * 90 + n * 0.5, 2) });
    }
    const { lapRange, series } = gapSeries(s.history, s.maxLap, s.order);
    expect(lapRange).toEqual([6, 20]);
    expect(series.find((x) => x.driverId === "b")?.points.at(-1)).toEqual({ lap: 20, gapS: 10 });
    expect(series.find((x) => x.driverId === "a")?.points.every((p) => p.gapS === 0)).toBe(true);
  });

  it("maps the leader to the top and caps far gaps", () => {
    const { x, y, yMax } = scales({ width: 100, height: 100, left: 0, right: 0, top: 0, bottom: 0 }, [1, 11], 200);
    expect([x(1), x(11), y(0), y(yMax), y(1000)]).toEqual([0, 100, 0, 100, 100]);
    expect(linePath([{ lap: 1, gapS: 0 }, { lap: 11, gapS: 60 }], x, y)).toBe("M0.00 0.00 L100.00 100.00");
  });

  it("keeps end labels apart", () => {
    const placed = layoutLabels([{ id: "a", y: 50 }, { id: "b", y: 51 }, { id: "c", y: 52 }], 11, 0, 300);
    expect(placed.map((p) => p.y)).toEqual([50, 61, 72]);
    expect(layoutLabels([], 11, 0, 300)).toEqual([]);
  });

  it("stacks labels clustered at the bottom upwards without overlap", () => {
    const ys = Array.from({ length: 12 }, (_, i) => ({ id: `d${String(i).padStart(2, "0")}`, y: 256 }));
    const placed = layoutLabels(ys, 11, 16, 256);
    const sortedY = placed.map((p) => p.y);
    for (let i = 1; i < sortedY.length; i++) expect(sortedY[i]! - sortedY[i - 1]!).toBeCloseTo(11, 6);
    expect(Math.max(...sortedY)).toBe(256);
    expect(Math.min(...sortedY)).toBeGreaterThanOrEqual(16);
  });

  it("shrinks the spacing evenly when the labels cannot fit", () => {
    const ys = Array.from({ length: 30 }, (_, i) => ({ id: `d${String(i).padStart(2, "0")}`, y: 100 }));
    const placed = layoutLabels(ys, 11, 0, 290);
    expect(placed[0]!.y).toBeCloseTo(0, 6);
    expect(placed.at(-1)!.y).toBeCloseTo(290, 6);
    expect(placed[1]!.y - placed[0]!.y).toBeCloseTo(10, 6);
  });
});

describe("race picker and replay helpers", () => {
  const races = [
    { season: 2023, round: 5, name: "Miami Grand Prix", laps: 57, synthetic: true },
    { season: 2024, round: 5, name: "Chinese Grand Prix", laps: 56, synthetic: true },
    { season: 2024, round: 8, name: "Monaco Grand Prix", laps: 78, synthetic: false },
  ];
  it("filters by name, season and round, and groups newest season first", () => {
    expect(filterRaces(races, "monaco").map((r) => r.round)).toEqual([8]);
    expect(filterRaces(races, "2023")).toHaveLength(1);
    expect(filterRaces(races, " r5 ")).toHaveLength(2);
    expect(groupBySeason(races).map(([s, l]) => [s, l.length])).toEqual([[2024, 2], [2023, 1]]);
  });

  it("shows replay status and API errors as text", () => {
    const failed = { season: 2024, round: 5, speed: 60, run_id: "r", state: "failed" as const, error: "stream job not ready within 60 s", started_at: null, ended_at: null };
    expect(replayStatusText(failed, false, true)).toBe("Failed: stream job not ready within 60 s");
    expect(replayStatusText({ ...failed, state: "running" }, true, true)).toBe("Running at 60×.");
    expect(replayStatusText(null, true, true)).toBe("Another race is replaying.");
    expect(replayStatusText(null, false, false)).toBe("Replay is disabled on the API.");
    expect(replayFor({ enabled: true, max_running: 1, running: [], recent: [failed] }, 2024, 5)).toBe(failed);
    expect(replayMessage(new ApiError("a replay of 2024 R5 is running", 409))).toBe("a replay of 2024 R5 is running");
    expect(replayMessage(new ApiError("replay is disabled (set ENABLE_REPLAY=1)", 403))).toContain("disabled");
    expect(replayMessage(new ApiError("x", null))).toBe("The API is not reachable.");
  });

  it("finds the event ids applied since the last paint", () => {
    expect(newIds(["a", "b", "c"], null)).toEqual(["a", "b", "c"]);
    expect(newIds(["a", "b", "c"], "b")).toEqual(["c"]);
    expect(newIds(["d", "e"], "gone")).toEqual(["d", "e"]);
  });
});
