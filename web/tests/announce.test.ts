import { describe, expect, it } from "vitest";

import { announcementFor, shouldAnnounce } from "@/lib/announce";
import type { AlertEvent } from "@/lib/events";

const base: AlertEvent = {
  type: "undercut_alert",
  eventId: "00000000-0000-4000-8000-000000000001",
  season: 2024,
  round: 5,
  sseSentAtMs: null,
  reason: "opportunity",
  trigger: "new_candidate",
  attackerId: "hulkenberg",
  defenderId: "bottas",
  decisionLap: 7,
  gapS: 1.1,
  prob: 0.62,
  threshold: 0.5,
};

describe("aria-live filter", () => {
  it.each([
    ["opportunity", "new_candidate", false],
    ["opportunity", "probability_crossed", true],
    ["attempt", "pit_in", true],
  ] as const)("%s / %s → announce %s", (reason, trigger, expected) => {
    expect(shouldAnnounce({ reason, trigger })).toBe(expected);
    const text = announcementFor({ ...base, reason, trigger });
    expect(text !== null).toBe(expected);
  });

  it("speaks names and the probability", () => {
    expect(announcementFor({ ...base, reason: "attempt", trigger: "pit_in" })).toBe(
      "Undercut attempt: Hulkenberg pits to undercut Bottas, success probability 62%.",
    );
    expect(announcementFor({ ...base, trigger: "probability_crossed" })).toBe(
      "Undercut chance up: Hulkenberg on Bottas now 62%, lap 7.",
    );
  });
});
