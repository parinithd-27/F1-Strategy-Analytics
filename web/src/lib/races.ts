/** Race picker helpers (pure). */

import type { Race } from "./api";

/** Races whose name, season or ``r<round>`` contains ``query`` (case-insensitive). */
export function filterRaces(races: readonly Race[], query: string): Race[] {
  const q = query.trim().toLowerCase();
  if (!q) return [...races];
  return races.filter((r) => `${r.name ?? ""} ${r.season} r${r.round}`.toLowerCase().includes(q));
}

/** ``[season, races]`` pairs, newest season first, races in round order as given. */
export function groupBySeason(races: readonly Race[]): [number, Race[]][] {
  const groups = new Map<number, Race[]>();
  for (const r of races) groups.set(r.season, [...(groups.get(r.season) ?? []), r]);
  return [...groups.entries()].sort((a, b) => b[0] - a[0]);
}
