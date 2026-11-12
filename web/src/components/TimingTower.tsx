import { driverCode, driverName, gap, lapTime, laps, probability } from "@/lib/format";
import type { DriverRow } from "@/lib/reducer";

import { COMPOUND_LABEL, TyreGlyph } from "./TyreGlyph";

export const TOWER_ROWS = 22;

/** The timing tower: a real table in order-set order. Fixed row height and reserved space for
 * ``TOWER_ROWS`` rows, so live updates never shift the page. */
export function TimingTower({ rows, lapsDown }: { rows: DriverRow[]; lapsDown: (driverId: string) => number }) {
  return (
    <div className="tower-wrap" role="region" aria-labelledby="tower-title" tabIndex={0}>
      <table className="tower">
        <caption id="tower-title">
          Timing tower <span className="caption-note">gaps at the timing line, live from the race stream</span>
        </caption>
        <thead>
          <tr>
            <th scope="col" className="c-pos"><abbr title="Position">Pos</abbr></th>
            <th scope="col" className="c-drv">Driver</th>
            <th scope="col" className="c-num"><abbr title="Gap to the car ahead">Int</abbr></th>
            <th scope="col" className="c-num">Gap</th>
            <th scope="col" className="c-num">Last lap</th>
            <th scope="col" className="c-tyre">Tyre</th>
            <th scope="col" className="c-age">Age</th>
            <th scope="col" className="c-num"><abbr title="Calibrated undercut success probability">Undercut</abbr></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const down = lapsDown(r.driverId);
            return (
              <tr key={r.driverId} data-driver={r.driverId} className={r.undercutFlag ? "is-flagged" : undefined}>
                <th scope="row" className="c-pos">{r.position ?? "—"}</th>
                <td className="c-drv">
                  <span className="code">{driverCode(r.driverId)}</span>{" "}
                  <span className="name">{driverName(r.driverId)}</span>
                  {r.inPit ? <span className="tag tag-pit">IN PIT</span> : null}
                  {down > 0 ? (
                    <span className="tag" title={`${down} ${down === 1 ? "lap" : "laps"} down`}>
                      {`+${down} L`}
                      <span className="visually-hidden">{` (${down} ${down === 1 ? "lap" : "laps"} down)`}</span>
                    </span>
                  ) : null}
                </td>
                <td className="c-num">{r.position === 1 ? "Leader" : gap(r.gapAheadS)}</td>
                <td className="c-num">{r.position === 1 ? "Leader" : gap(r.gapLeaderS)}</td>
                <td className="c-num" data-col="last-lap">{lapTime(r.lastLapTimeS)}</td>
                <td className="c-tyre">
                  <TyreGlyph compound={r.compound} /> <span>{COMPOUND_LABEL[r.compound]}</span>
                </td>
                <td className="c-age">{laps(r.tyreAgeLaps)}</td>
                <td className="c-num">
                  {r.undercutFlag ? (
                    <span className="flag" title="Rule-eligible undercut attacker">
                      <span aria-hidden="true">▲ </span>
                      <span className="visually-hidden">Undercut candidate, </span>
                    </span>
                  ) : null}
                  {probability(r.undercutProb)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {rows.length === 0 ? (
        <p className="tower-empty">No timing yet. Start a replay to fill the tower.</p>
      ) : null}
    </div>
  );
}
