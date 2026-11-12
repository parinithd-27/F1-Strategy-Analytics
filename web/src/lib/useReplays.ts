"use client";

/** Replay status from ``GET /api/replays`` (every 1 s while a replay runs, else every 5 s) and the
 * start action. Errors become one human-readable message. */

import { useCallback, useEffect, useState } from "react";

import { api, type ReplayStatus } from "./api";
import { replayFor, replayMessage, type Speed } from "./replays";

export type { Speed } from "./replays";

export function useReplays(season: number, round: number) {
  const [status, setStatus] = useState<ReplayStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setStatus(await api.replays());
    } catch (err) {
      setError(replayMessage(err));
    }
  }, []);

  const running = (status?.running.length ?? 0) > 0;
  useEffect(() => {
    let alive = true;
    const poll = () => {
      api
        .replays()
        .then((s) => {
          if (alive) setStatus(s);
        })
        .catch((err: unknown) => {
          if (alive) setError(replayMessage(err));
        });
    };
    poll();
    const id = setInterval(poll, running || pending ? 1000 : 5000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [running, pending]);

  const start = useCallback(
    async (speed: Speed) => {
      setPending(true);
      setError(null);
      try {
        await api.startReplay(season, round, speed);
      } catch (err) {
        setError(replayMessage(err));
      } finally {
        await refresh();
        setPending(false);
      }
    },
    [season, round, refresh],
  );

  return { status, error, pending, start, current: replayFor(status, season, round) };
}
