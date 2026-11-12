"use client";

import { useState } from "react";

import type { Replay } from "@/lib/api";
import { replayStatusText, type Speed } from "@/lib/replays";

const SPEEDS: readonly Speed[] = [1, 10, 60];

export function ReplayControl(props: {
  current: Replay | null;
  anyRunning: boolean;
  enabled: boolean | null;
  pending: boolean;
  error: string | null;
  onStart: (speed: Speed) => void;
}) {
  const [speed, setSpeed] = useState<Speed>(60);
  const disabled = props.pending || props.anyRunning || props.enabled === false;
  const status = replayStatusText(props.current, props.anyRunning, props.enabled);
  return (
    <section className="panel replay-panel" aria-labelledby="replay-h">
      <h2 id="replay-h">Replay</h2>
      <form
        className="replay-form"
        onSubmit={(e) => {
          e.preventDefault();
          if (!disabled) props.onStart(speed);
        }}
      >
        <fieldset className="speeds">
          <legend>Speed</legend>
          {SPEEDS.map((s) => (
            <label key={s} className="speed">
              <input type="radio" name="speed" value={s} checked={speed === s} onChange={() => setSpeed(s)} />
              <span>{s}×</span>
            </label>
          ))}
        </fieldset>
        <button type="submit" className="btn" disabled={disabled} aria-describedby="replay-status">
          {props.pending ? "Starting…" : "Start replay"}
        </button>
      </form>
      <p id="replay-status" className={`replay-status state-${props.current?.state ?? "idle"}`} role="status">
        {status}
      </p>
      <p className="replay-error" role="alert">
        {props.error ?? ""}
      </p>
    </section>
  );
}
