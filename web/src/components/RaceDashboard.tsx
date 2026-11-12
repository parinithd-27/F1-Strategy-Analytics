"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { api, type Race } from "@/lib/api";
import { useLatencyReporter } from "@/lib/debugLatency";
import { alertGroups, lapsDown, towerRows } from "@/lib/reducer";
import { useRaceStream } from "@/lib/useRaceStream";
import { useReplays } from "@/lib/useReplays";

import { AlertFeed } from "./AlertFeed";
import { ConnectionIndicator } from "./ConnectionIndicator";
import { GapChart } from "./GapChart";
import { ReplayControl } from "./ReplayControl";
import { TimingTower } from "./TimingTower";
import { TyrePanel } from "./TyrePanel";

export function RaceDashboard({ season, round }: { season: number; round: number }) {
  const { state, snapshotError } = useRaceStream(season, round);
  const replays = useReplays(season, round);
  const [race, setRace] = useState<Race | null>(null);
  useLatencyReporter(state.seen);

  useEffect(() => {
    const ctl = new AbortController();
    api
      .races(ctl.signal)
      .then((list) => setRace(list.find((r) => r.season === season && r.round === round) ?? null))
      .catch(() => setRace(null));
    return () => ctl.abort();
  }, [season, round]);

  const rows = towerRows(state);
  const leaderLap = rows[0]?.lap ?? null;
  const groups = alertGroups(state);
  const announcement = state.announcements[state.announcements.length - 1] ?? null;
  const anyRunning = (replays.status?.running.length ?? 0) > 0;

  return (
    <>
      <div className="race-head">
        <nav aria-label="Breadcrumb" className="crumbs">
          <Link href="/">All races</Link>
        </nav>
        <h1>
          <span className="race-season">{season}</span>{" "}
          {race?.name ?? `Round ${round}`}
          <span className="muted"> · R{round}</span>
        </h1>
        <p className="lap-counter" aria-label={`Lap ${leaderLap ?? 0} of ${race?.laps ?? "unknown"}`}>
          <span className="lc-label">LAP</span> <span className="lc-now">{leaderLap ?? "—"}</span>
          <span className="lc-total"> / {race?.laps ?? "—"}</span>
        </p>
        <ConnectionIndicator status={state.connection} sessionEnded={state.sessionEnded} />
      </div>
      {snapshotError ? (
        <p className="error" role="alert">
          Could not load the race state: {snapshotError}
        </p>
      ) : null}
      <div className="dash">
        <div className="dash-main">
          <TimingTower rows={rows} lapsDown={(id) => lapsDown(state, id)} />
          <GapChart state={state} />
        </div>
        <div className="dash-side">
          <ReplayControl
            current={replays.current}
            anyRunning={anyRunning}
            enabled={replays.status?.enabled ?? null}
            pending={replays.pending}
            error={replays.error}
            onStart={(speed) => void replays.start(speed)}
          />
          <AlertFeed groups={groups} announcement={announcement} />
          <TyrePanel rows={rows} />
        </div>
      </div>
      <p className="stream-stats muted">
        {state.seen.length} events applied · {state.malformed} malformed · {state.unknownDrivers} unknown drivers
      </p>
    </>
  );
}
