"use client";

import { useEffect, useRef, useState } from "react";

import { driverCode, driverName, probability } from "@/lib/format";
import {
  attackerChoices,
  barWidth,
  defaultDefender,
  fetchWhatIf,
  gapOutcome,
  intervalText,
  reasonText,
  STRATEGY_LABEL,
  timelineText,
  warningTexts,
  whatifMessage,
  type Strategy,
  type WhatIf,
} from "@/lib/whatif";

const label = (id: string): string => `${driverCode(id)} · ${driverName(id)}`;

function StrategyRow({ s }: { s: Strategy }) {
  const p = s.p_attacker_ahead;
  return (
    <tr className={s.feasible ? undefined : "is-infeasible"}>
      <th scope="row">
        <span className="wi-name">{STRATEGY_LABEL[s.strategy]}</span>
        <span className="wi-timeline">{timelineText(s)}</span>
      </th>
      {s.feasible ? (
        <>
          <td className="wi-prob">
            <span className="wi-prob-num">{probability(p)}</span>
            <span className="wi-bar" aria-hidden="true">
              <span className="wi-fill" style={{ width: `${barWidth(p)}%` }} />
            </span>
          </td>
          <td className="wi-num">{gapOutcome(s.expected_gap_s)}</td>
          <td className="wi-num">{intervalText(s.interval80_s)}</td>
        </>
      ) : (
        <td colSpan={3} className="wi-reason">
          {reasonText(s.reason)}
        </td>
      )}
    </tr>
  );
}

/** Pit now / pit next lap / stay out for an attacker and the car ahead, next to the model's P. */
export function WhatIfPanel({ season, round, order }: { season: number; round: number; order: string[] }) {
  const choices = attackerChoices(order);
  // null = the default: the first car with a car ahead, and the car directly ahead of the attacker.
  const [chosenAttacker, setAttacker] = useState<string | null>(null);
  const [chosenDefender, setDefender] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<WhatIf | null>(null);
  const ctl = useRef<AbortController | null>(null);

  const attacker = chosenAttacker ?? choices[0] ?? "";
  const defender = chosenDefender ?? defaultDefender(order, attacker) ?? "";

  useEffect(() => () => ctl.current?.abort(), []);

  const chooseAttacker = (id: string) => {
    setAttacker(id);
    setDefender(null); // follow the car ahead of the new attacker
  };

  const run = async () => {
    if (!attacker || !defender) return;
    ctl.current?.abort();
    const c = new AbortController();
    ctl.current = c;
    setPending(true);
    setError(null);
    try {
      const r = await fetchWhatIf(season, round, { attacker, defender }, c.signal);
      if (!c.signal.aborted) setResult(r);
    } catch (err) {
      if (!c.signal.aborted) setError(whatifMessage(err));
    } finally {
      if (ctl.current === c) setPending(false);
    }
  };

  const disabled = pending || !attacker || !defender;
  const status = pending
    ? "Simulating…"
    : result
      ? `${driverCode(result.attacker)} on ${driverCode(result.defender)} at lap ${result.inputs.current_lap ?? "?"} · n=${result.n}, seed ${result.seed} · ${Math.round(result.runtime_ms)} ms`
      : choices.length === 0
        ? "Waiting for the race order…"
        : "Pick an attacker and run.";

  return (
    <section className="panel whatif-panel" aria-labelledby="whatif-h">
      <div className="panel-head">
        <h2 id="whatif-h">What-if: pit strategy</h2>
        {result?.inputs.synthetic ? <span className="badge">Synthetic tyres</span> : null}
      </div>
      <form
        className="whatif-form"
        onSubmit={(e) => {
          e.preventDefault();
          if (!disabled) void run();
        }}
      >
        <label className="wi-field">
          <span>Attacker</span>
          <select value={attacker} onChange={(e) => chooseAttacker(e.target.value)} disabled={choices.length === 0}>
            {choices.map((id) => (
              <option key={id} value={id}>
                {label(id)}
              </option>
            ))}
          </select>
        </label>
        <label className="wi-field">
          <span>Defender (car ahead)</span>
          <select value={defender} onChange={(e) => setDefender(e.target.value)} disabled={choices.length === 0}>
            {order
              .filter((id) => id !== attacker)
              .map((id) => (
                <option key={id} value={id}>
                  {label(id)}
                </option>
              ))}
          </select>
        </label>
        <button type="submit" className="btn" disabled={disabled} aria-describedby="whatif-status">
          {pending ? "Running…" : "Run what-if"}
        </button>
      </form>
      <p id="whatif-status" className="whatif-status" role="status">
        {status}
      </p>
      <p className="replay-error" role="alert">
        {error ?? ""}
      </p>
      <div className="whatif-results">
        {result ? (
          <>
            <table className="whatif-table">
              <caption>
                Chance the attacker is ahead once both cars have stopped, expected gap and 80% interval
              </caption>
              <thead>
                <tr>
                  <th scope="col">Strategy</th>
                  <th scope="col">P(attacker ahead)</th>
                  <th scope="col">Expected gap</th>
                  <th scope="col">80% interval</th>
                </tr>
              </thead>
              <tbody>
                {result.strategies.map((s) => (
                  <StrategyRow key={s.strategy} s={s} />
                ))}
              </tbody>
              <tbody className="wi-sensitivity">
                <tr>
                  <th scope="rowgroup" colSpan={4}>
                    Sensitivity: the defender covers 2 laps later
                  </th>
                </tr>
                {result.sensitivity.map((s) => (
                  <StrategyRow key={`s-${s.strategy}`} s={s} />
                ))}
              </tbody>
            </table>
            <p className="whatif-model">
              <span className="wi-model-label">Model (calibrated LR, from the stream):</span>{" "}
              <strong>{probability(result.model.probability)}</strong>
              {result.model.undercut_flag ? " · flagged as an undercut opportunity" : ""}
            </p>
            <p className="whatif-inputs muted">
              Gap {result.inputs.gap_s.toFixed(2)} s · {result.inputs.attacker.compound} → {result.inputs.attacker.new_compound} vs{" "}
              {result.inputs.defender.compound} → {result.inputs.defender.new_compound} · pit loss{" "}
              {result.inputs.pit_loss_prior_s === null ? "unknown" : `${result.inputs.pit_loss_prior_s.toFixed(1)} s`} (
              {result.inputs.pit_loss_source ?? "none"})
            </p>
            {warningTexts(result.inputs.warnings).map((w) => (
              <p key={w} className="whatif-warning">
                Note: {w}
              </p>
            ))}
          </>
        ) : (
          <p className="muted whatif-empty">No simulation yet.</p>
        )}
      </div>
    </section>
  );
}
