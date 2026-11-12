/** Geometry of the gap-to-leader chart (pure, unit-tested). The chart is hand-written SVG. */

import type { RaceState } from "./reducer";

export const CHART_LAPS = 15;
export const GAP_CAP_S = 60;

export interface Series {
  driverId: string;
  points: { lap: number; gapS: number }[];
}

/** Gap to the leader (the car with Jolpica position 1 at the same lap) for the last ``laps`` laps,
 * one series per driver in ``order``. A lap without a known leader is left out. */
export function gapSeries(
  history: RaceState["history"],
  maxLap: number,
  order: readonly string[],
  laps = CHART_LAPS,
): { lapRange: [number, number]; series: Series[] } {
  const first = Math.max(1, maxLap - laps + 1);
  const series: Series[] = order.map((driverId) => ({ driverId, points: [] }));
  const byId = new Map(series.map((s) => [s.driverId, s]));
  for (let lap = first; lap <= maxLap; lap++) {
    const atLap = history[lap];
    if (!atLap) continue;
    const leader = Object.values(atLap).find((p) => p.position === 1);
    if (!leader) continue;
    for (const [id, p] of Object.entries(atLap)) {
      byId.get(id)?.points.push({ lap, gapS: Math.round((p.raceTimeS - leader.raceTimeS) * 1000) / 1000 });
    }
  }
  return { lapRange: [first, Math.max(first, maxLap)], series: series.filter((s) => s.points.length > 0) };
}

export interface Frame {
  width: number;
  height: number;
  left: number;
  right: number;
  top: number;
  bottom: number;
}

/** Linear scales: laps left to right, gap 0 (the leader) at the top, capped at ``cap`` seconds. */
export function scales(frame: Frame, lapRange: [number, number], maxGapS: number, cap = GAP_CAP_S) {
  const [a, b] = lapRange;
  const yMax = Math.max(1, Math.min(cap, Math.ceil(maxGapS)));
  const innerW = frame.width - frame.left - frame.right;
  const innerH = frame.height - frame.top - frame.bottom;
  const x = (lap: number): number => frame.left + (b === a ? innerW / 2 : ((lap - a) / (b - a)) * innerW);
  const y = (gap: number): number => frame.top + (Math.min(Math.max(gap, 0), yMax) / yMax) * innerH;
  return { x, y, yMax };
}

/** SVG path ``M x y L x y …`` (2 decimals). */
export function linePath(points: { lap: number; gapS: number }[], x: (l: number) => number, y: (g: number) => number): string {
  return points
    .map((p, i) => `${i === 0 ? "M" : "L"}${x(p.lap).toFixed(2)} ${y(p.gapS).toFixed(2)}`)
    .join(" ");
}

/** End-of-line label positions, at least ``minGap`` px apart and inside ``[top, bottom]``. Labels
 * keep their order by y. When they do not fit (many cars at the cap), the spacing shrinks evenly to
 * ``(bottom - top) / (n - 1)`` instead of piling labels onto one edge. */
export function layoutLabels(ys: { id: string; y: number }[], minGap: number, top: number, bottom: number) {
  const sorted = [...ys].sort((p, q) => p.y - q.y || p.id.localeCompare(q.id));
  const n = sorted.length;
  if (n === 0) return [];
  const gapPx = n > 1 ? Math.min(minGap, (bottom - top) / (n - 1)) : minGap;
  // Downward pass: no closer than gapPx to the label above.
  const placed = sorted.map((item) => ({ id: item.id, y: Math.min(Math.max(item.y, top), bottom) }));
  for (let i = 1; i < n; i++) {
    const prev = placed[i - 1] as { y: number };
    const cur = placed[i] as { y: number };
    cur.y = Math.max(cur.y, prev.y + gapPx);
  }
  // Upward pass: pull back inside the bottom edge, keeping the spacing.
  const last = placed[n - 1] as { y: number };
  last.y = Math.min(last.y, bottom);
  for (let i = n - 2; i >= 0; i--) {
    const next = placed[i + 1] as { y: number };
    const cur = placed[i] as { y: number };
    cur.y = Math.min(cur.y, next.y - gapPx);
  }
  return placed;
}
