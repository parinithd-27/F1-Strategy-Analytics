import { describe, expect, it } from "vitest";

import { parseEvent } from "@/lib/events";

import { alertMsg, lap, msg, snapshotMsg } from "./helpers";

describe("parseEvent", () => {
  it("parses a lap with the SSE envelope", () => {
    expect(parseEvent(lap("max_verstappen", 3, 280.5, 1))).toMatchObject({
      type: "lap",
      driverId: "max_verstappen",
      lap: 3,
      raceTimeS: 280.5,
      position: 1,
      compound: "MEDIUM",
      tyreAge: 3,
      season: 2024,
      round: 5,
    });
  });

  it("accepts nulls where the API sends them", () => {
    expect(parseEvent(lap("a", 1, 1, 1, { lap_time_s: null, position: null, tyre_age: null, compound: null }))).toMatchObject({
      lapTimeS: null,
      position: null,
      tyreAge: null,
      compound: "UNKNOWN",
    });
  });

  it("parses alerts and rejects inconsistent reason/trigger pairs", () => {
    expect(parseEvent(alertMsg("a", "b", "attempt", "pit_in", 0.4))).toMatchObject({ type: "undercut_alert", reason: "attempt" });
    expect(parseEvent(alertMsg("a", "b", "attempt", "new_candidate", 0.4))).toBeNull();
    expect(parseEvent(alertMsg("a", "a", "opportunity", "new_candidate", 0.4))).toBeNull();
    expect(parseEvent(alertMsg("a", "b", "opportunity", "new_candidate", 1.4))).toBeNull();
  });

  it("parses session_end with an empty payload", () => {
    expect(parseEvent(msg("session_end", {}))).toMatchObject({ type: "session_end" });
  });

  it("parses a pushed snapshot", () => {
    const e = parseEvent(snapshotMsg(["a", "b"], 12, { b: true }));
    expect(e).toMatchObject({ type: "snapshot", eventId: "snapshot-12", lap: 12, season: 2024, round: 5 });
    expect(e?.type === "snapshot" && e.data.drivers.b?.undercut_flag).toBe(true);
  });

  it("rejects anything else", () => {
    for (const raw of ["", "{", "[]", msg("weather", {}), msg("lap", {})]) expect(parseEvent(raw)).toBeNull();
  });
});
