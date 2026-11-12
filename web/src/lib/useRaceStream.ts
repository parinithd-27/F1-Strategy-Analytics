"use client";

/** EventSource lifecycle around the one reducer. Updates are pushed, never polled.
 *
 * - The API sends an ``event: snapshot`` (the full Redis state) right after it subscribes, and
 *   again once per leader lap; with the race events it is all the dashboard needs.
 * - Reconnect (after an error): open the EventSource, and on ``open`` also reload ``/state`` once
 *   before applying the events that arrived meanwhile (events older than that state are
 *   idempotent). The API's own connect snapshot follows on the stream anyway.
 * - Errors retry with backoff 1, 2, 4, 8, 15 s. After ``session_end`` the stream closes for good:
 *   the race is over and its keys expire.
 */

import { useEffect, useReducer, useRef, useState } from "react";

import { api } from "./api";
import { initialState, raceReducer, type RaceState } from "./reducer";

const BACKOFF_S = [1, 2, 4, 8, 15] as const;

export function useRaceStream(season: number, round: number): {
  state: RaceState;
  snapshotError: string | null;
} {
  const [state, dispatch] = useReducer(raceReducer, initialState(season, round));
  const [snapshotError, setSnapshotError] = useState<string | null>(null);
  const endedRef = useRef(false);
  useEffect(() => {
    endedRef.current = state.sessionEnded;
  }, [state.sessionEnded]);

  useEffect(() => {
    endedRef.current = false;
    dispatch({ type: "reset", season, round });
    let source: EventSource | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let attempt = 0;
    let closed = false;
    let abort: AbortController | null = null;
    let opened = false;

    const connect = () => {
      if (closed || endedRef.current) return;
      dispatch({ type: "connection", status: attempt === 0 ? "connecting" : "reconnecting" });
      const es = new EventSource(api.eventsUrl(season, round));
      source = es;
      let loading = true;
      const buffer: string[] = [];
      const deliver = (raw: string) => {
        if (loading) buffer.push(raw);
        else dispatch({ type: "event", raw });
      };
      for (const type of ["snapshot", "lap", "pit_in", "pit_out", "undercut_alert", "session_end"]) {
        es.addEventListener(type, (ev) => {
          deliver((ev as MessageEvent<string>).data);
          if (type === "session_end") {
            endedRef.current = true;
            es.close();
          }
        });
      }
      es.onopen = () => {
        attempt = 0;
        if (!opened) {
          // First connect: the API's connect snapshot arrives on the stream; nothing to load.
          opened = true;
          loading = false;
          for (const raw of buffer.splice(0)) dispatch({ type: "event", raw });
          dispatch({ type: "connection", status: endedRef.current ? "closed" : "connected" });
          return;
        }
        abort?.abort();
        abort = new AbortController();
        loading = true;
        api
          .state(season, round, abort.signal)
          .then((data) => {
            dispatch({ type: "snapshot", data });
            setSnapshotError(null);
          })
          .catch((err: unknown) => {
            // 404 = no live keys (no replay running): the stream still fills the tower.
            const status = (err as { status?: number | null }).status;
            setSnapshotError(status === 404 ? null : String((err as Error).message ?? err));
          })
          .finally(() => {
            loading = false;
            for (const raw of buffer.splice(0)) dispatch({ type: "event", raw });
            dispatch({ type: "connection", status: endedRef.current ? "closed" : "connected" });
          });
      };
      es.onerror = () => {
        es.close();
        if (closed || endedRef.current) {
          dispatch({ type: "connection", status: "closed" });
          return;
        }
        dispatch({ type: "connection", status: "reconnecting" });
        const delay = BACKOFF_S[Math.min(attempt, BACKOFF_S.length - 1)] ?? 15;
        attempt += 1;
        timer = setTimeout(connect, delay * 1000);
      };
    };

    connect();
    return () => {
      closed = true;
      abort?.abort();
      if (timer) clearTimeout(timer);
      source?.close();
    };
  }, [season, round]);

  return { state, snapshotError };
}
