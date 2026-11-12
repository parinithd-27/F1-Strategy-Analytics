"use client";

import Link from "next/link";
import { type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";

import { api, type Race } from "@/lib/api";
import { filterRaces, groupBySeason } from "@/lib/races";

/** Races grouped by season, filtered by name. Arrow keys / Home / End move between races; Tab
 * leaves the list. */
export function RacePicker() {
  const [races, setRaces] = useState<Race[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const ctl = new AbortController();
    api
      .races(ctl.signal)
      .then(setRaces)
      .catch((err: unknown) => {
        if (!ctl.signal.aborted) setError((err as Error).message);
      });
    return () => ctl.abort();
  }, []);

  const shown = useMemo(() => filterRaces(races ?? [], query), [races, query]);
  const groups = useMemo(() => groupBySeason(shown), [shown]);

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    const links = Array.from(listRef.current?.querySelectorAll<HTMLAnchorElement>("a.race-link") ?? []);
    const at = links.indexOf(document.activeElement as HTMLAnchorElement);
    const go = (i: number) => {
      e.preventDefault();
      links[Math.max(0, Math.min(links.length - 1, i))]?.focus();
    };
    if (e.key === "ArrowDown") go(at + 1);
    else if (e.key === "ArrowUp") go(at <= 0 ? 0 : at - 1);
    else if (e.key === "Home") go(0);
    else if (e.key === "End") go(links.length - 1);
  };

  return (
    <div className="picker">
      <search className="picker-search">
        <label htmlFor="race-search">Search races</label>
        <input
          id="race-search"
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="e.g. Monaco, 2023, r5"
          aria-controls="race-list"
          aria-describedby="race-count"
          autoComplete="off"
        />
        <p id="race-count" className="muted" aria-live="polite">
          {races === null ? (error ? "" : "Loading races…") : `${shown.length} of ${races.length} races`}
        </p>
      </search>
      {error ? (
        <p className="error" role="alert">
          Could not load races: {error}
        </p>
      ) : null}
      <div id="race-list" ref={listRef} onKeyDown={onKeyDown} className="race-list">
        {groups.map(([season, list]) => (
          <section key={season} aria-labelledby={`season-${season}`} className="season">
            <h2 id={`season-${season}`}>
              {season} <span className="muted">· {list.length} {list.length === 1 ? "race" : "races"}</span>
            </h2>
            <ul>
              {list.map((r) => (
                <li key={`${r.season}-${r.round}`}>
                  <Link className="race-link" href={`/race/${r.season}/${r.round}`}>
                    <span className="race-round">R{String(r.round).padStart(2, "0")}</span>
                    <span className="race-name">{r.name ?? `Round ${r.round}`}</span>
                    <span className="race-laps">{r.laps} laps</span>
                    {r.synthetic ? <span className="tag">synthetic tyres</span> : null}
                  </Link>
                </li>
              ))}
            </ul>
          </section>
        ))}
      </div>
    </div>
  );
}
