import { driverCode, driverName, gap, probability } from "@/lib/format";
import type { AlertGroup, Announcement } from "@/lib/reducer";

const REASON: Record<AlertGroup["reason"], string> = { opportunity: "Opportunity", attempt: "Attempt" };
const TRIGGER: Record<AlertGroup["trigger"], string> = {
  new_candidate: "new candidate",
  probability_crossed: "probability crossed threshold",
  pit_in: "attacker pitted",
};

/** Alerts grouped by (attacker, defender), newest first. The polite live region speaks only the
 * reducer's announcements (attempts and threshold crossings); other updates stay silent. */
export function AlertFeed({ groups, announcement }: { groups: AlertGroup[]; announcement: Announcement | null }) {
  return (
    <section className="panel alert-panel" aria-labelledby="alerts-h">
      <h2 id="alerts-h">Undercut alerts</h2>
      <div className="visually-hidden" aria-live="polite" aria-atomic="true">
        {announcement?.text ?? ""}
      </div>
      <div className="feed" role="region" aria-label="Undercut alert list" tabIndex={0}>
        {groups.length === 0 ? (
          <p className="muted">No undercut alerts yet.</p>
        ) : (
          <ol className="alerts">
            {groups.map((g) => (
              <li key={g.key} className={`alert alert-${g.reason}`}>
                <p className="alert-pair">
                  <span className="code">{driverCode(g.attackerId)}</span>
                  <span aria-hidden="true"> → </span>
                  <span className="visually-hidden"> attacking </span>
                  <span className="code">{driverCode(g.defenderId)}</span>
                  <span className="alert-prob">{probability(g.prob)}</span>
                </p>
                <p className="alert-meta">
                  <span className={`badge badge-${g.reason}`}>{REASON[g.reason]}</span> {TRIGGER[g.trigger]} · lap {g.decisionLap} · gap{" "}
                  {gap(g.gapS)} · {g.count}×
                  <span className="visually-hidden">
                    {` ${driverName(g.attackerId)} on ${driverName(g.defenderId)}`}
                  </span>
                </p>
              </li>
            ))}
          </ol>
        )}
      </div>
    </section>
  );
}
