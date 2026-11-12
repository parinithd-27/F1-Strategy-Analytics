"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";

import { CHART_LAPS, gapSeries, layoutLabels, linePath, scales } from "@/lib/chart";
import { driverCode, driverName, gap } from "@/lib/format";
import type { RaceState } from "@/lib/reducer";

const BASE = { width: 720, height: 300, left: 44, right: 56, top: 16, bottom: 28 };
const MIN_WIDTH = 280;

/** The element's content width, kept up to date (the first render uses ``fallback``). */
function useWidth<T extends HTMLElement>(fallback: number) {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(fallback);
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w && Math.abs(w - width) >= 1) setWidth(Math.max(MIN_WIDTH, Math.round(w)));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [width]);
  return { ref, width };
}
const DASHES = ["", "6 4", "2 3", "10 3 2 3"];

/** Gap to the leader over the last 15 laps (hand-written SVG). Lines differ by colour (10) **and**
 * dash pattern (4), so all 20 cars get a unique pair, and carry their driver code at the end; the chosen driver is drawn thicker. A visually
 * hidden table carries the same numbers for screen readers. */
export function GapChart({ state }: { state: RaceState }) {
  const titleId = useId();
  const descId = useId();
  const [focus, setFocus] = useState<string>("");
  const { ref: boxRef, width } = useWidth<HTMLDivElement>(BASE.width);
  const FRAME = { ...BASE, width };
  const { lapRange, series } = useMemo(
    () => gapSeries(state.history, state.maxLap, state.order, CHART_LAPS),
    [state.history, state.maxLap, state.order],
  );
  const maxGap = Math.max(1, ...series.flatMap((s) => s.points.map((p) => p.gapS)));
  const { x, y, yMax } = scales(FRAME, lapRange, maxGap);
  const ticks = [0, yMax / 4, yMax / 2, (3 * yMax) / 4, yMax].map((t) => Math.round(t));
  const labels = layoutLabels(
    series.map((s) => ({ id: s.driverId, y: y(s.points[s.points.length - 1]?.gapS ?? 0) })),
    11,
    FRAME.top,
    FRAME.height - FRAME.bottom,
  );
  const labelY = new Map(labels.map((l) => [l.id, l.y]));
  const lapsShown = Array.from({ length: lapRange[1] - lapRange[0] + 1 }, (_, i) => lapRange[0] + i);

  return (
    <section className="panel chart-panel" aria-labelledby={`${titleId}-h`}>
      <div className="panel-head">
        <h2 id={`${titleId}-h`}>Gap to leader</h2>
        <label className="inline-field">
          <span>Highlight</span>
          <select value={focus} onChange={(e) => setFocus(e.target.value)}>
            <option value="">None</option>
            {state.order.map((id) => (
              <option key={id} value={id}>
                {driverCode(id)} · {driverName(id)}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="chart-box" ref={boxRef}>
        <svg viewBox={`0 0 ${FRAME.width} ${FRAME.height}`} role="img" aria-labelledby={`${titleId} ${descId}`} className="chart">
          <title id={titleId}>Gap to leader, laps {lapRange[0]} to {lapRange[1]}</title>
          <desc id={descId}>
            {series.length === 0
              ? "No laps received yet."
              : `${series.length} drivers. Leader at the top; lower lines are further behind, capped at ${yMax} seconds.`}
          </desc>
          {ticks.map((t) => (
            <g key={t} className="chart-grid">
              <line x1={FRAME.left} x2={FRAME.width - FRAME.right} y1={y(t)} y2={y(t)} />
              <text x={FRAME.left - 6} y={y(t) + 4} textAnchor="end">{t}s</text>
            </g>
          ))}
          {lapsShown.map((lap) => (
            <text key={lap} className="chart-axis" x={x(lap)} y={FRAME.height - 8} textAnchor="middle">
              {lap}
            </text>
          ))}
          {series.map((s, i) => {
            const chosen = focus === s.driverId;
            const dim = focus !== "" && !chosen;
            const last = s.points[s.points.length - 1];
            return (
              <g key={s.driverId} className={`chart-line${dim ? " is-dim" : ""}${chosen ? " is-focus" : ""}`} style={{ color: `var(--line-${i % 10})` }}>
                <path d={linePath(s.points, x, y)} fill="none" stroke="currentColor" strokeWidth={chosen ? 3.5 : 1.75} strokeDasharray={DASHES[i % DASHES.length] || undefined} />
                {last ? <circle cx={x(last.lap)} cy={y(last.gapS)} r={chosen ? 4 : 2.5} fill="currentColor" /> : null}
                <text x={FRAME.width - FRAME.right + 6} y={(labelY.get(s.driverId) ?? 0) + 4} className="chart-label" fill="currentColor">
                  {driverCode(s.driverId)}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      {/* A <table> ignores the 1px width of the visually-hidden pattern: hide a wrapper instead. */}
      <div className="visually-hidden">
        <table>
          <caption>Gap to leader in seconds by lap</caption>
          <thead>
            <tr>
              <th scope="col">Driver</th>
              {lapsShown.map((lap) => (
                <th key={lap} scope="col">Lap {lap}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {series.map((s) => {
              const byLap = new Map(s.points.map((p) => [p.lap, p.gapS]));
              return (
                <tr key={s.driverId}>
                  <th scope="row">{driverName(s.driverId)}</th>
                  {lapsShown.map((lap) => (
                    <td key={lap}>{byLap.has(lap) ? gap(byLap.get(lap) ?? null) : "—"}</td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
