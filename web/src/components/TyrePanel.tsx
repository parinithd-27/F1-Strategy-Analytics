import { driverCode, driverName } from "@/lib/format";
import type { DriverRow } from "@/lib/reducer";

import { COMPOUND_LABEL, TyreGlyph } from "./TyreGlyph";

export const AGE_SCALE_LAPS = 40;

/** Compound as text plus a shape, and tyre age as a bar with its number. */
export function TyrePanel({ rows }: { rows: DriverRow[] }) {
  return (
    <section className="panel tyre-panel" aria-labelledby="tyres-h">
      <h2 id="tyres-h">Tyres</h2>
      <ul className="tyres">
        {rows.map((r) => {
          const age = r.tyreAgeLaps;
          const pct = age === null ? 0 : Math.min(100, (age / AGE_SCALE_LAPS) * 100);
          return (
            <li key={r.driverId} className="tyre-row">
              <span className="code" title={driverName(r.driverId)}>{driverCode(r.driverId)}</span>
              <span className={`compound compound-${r.compound.toLowerCase()}`}>
                <TyreGlyph compound={r.compound} /> {COMPOUND_LABEL[r.compound]}
              </span>
              <span className="age-bar" aria-hidden="true">
                <span className="age-fill" style={{ width: `${pct}%` }} />
              </span>
              <span className="age-num">{age === null ? "— laps" : `${age} ${age === 1 ? "lap" : "laps"}`}</span>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
