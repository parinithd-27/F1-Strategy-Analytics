/** What the alert feed's polite live region announces. Only an undercut **attempt** (the attacker
 * pits) and an opportunity whose probability **crossed** the threshold are spoken; new candidates
 * update the feed silently, so a screen-reader user is not flooded on every lap. */

import type { AlertEvent } from "./events";
import { driverName, probability } from "./format";

export function shouldAnnounce(a: Pick<AlertEvent, "reason" | "trigger">): boolean {
  return a.reason === "attempt" || a.trigger === "probability_crossed";
}

export function announcementFor(a: AlertEvent): string | null {
  if (!shouldAnnounce(a)) return null;
  const attacker = driverName(a.attackerId);
  const defender = driverName(a.defenderId);
  const p = probability(a.prob);
  if (a.reason === "attempt") {
    return `Undercut attempt: ${attacker} pits to undercut ${defender}, success probability ${p}.`;
  }
  return `Undercut chance up: ${attacker} on ${defender} now ${p}, lap ${a.decisionLap}.`;
}
