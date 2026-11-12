"use client";

/** UI latency samples (``NEXT_PUBLIC_DEBUG_LATENCY=1``; the API needs ENABLE_DEBUG=1 too).
 *
 * For every event id the reducer applied since the last paint, ``ui_rendered_at_ms`` is posted
 * once that update is painted: two animation frames (the first runs before paint, the second
 * after it). React batches several events into one render, so all ids of the batch share that
 * paint time. A 403 means the API's debug endpoint is off: posting stops for the rest of the
 * page's life. At most ``MAX_IN_FLIGHT`` posts run at once; ids beyond that are dropped. */

import { useEffect, useRef } from "react";

import { api } from "./api";
import { DEBUG_LATENCY } from "./config";

export const MAX_IN_FLIGHT = 50;

/** The ids of ``seen`` after ``lastId`` (all of them when ``lastId`` is null or no longer there). */
export function newIds(seen: readonly string[], lastId: string | null): string[] {
  if (lastId === null) return [...seen];
  const at = seen.lastIndexOf(lastId);
  return at < 0 ? [...seen] : seen.slice(at + 1);
}

export function useLatencyReporter(seen: readonly string[], enabled = DEBUG_LATENCY): void {
  const disabled = useRef(false);
  const inFlight = useRef(0);
  const lastId = useRef<string | null>(null);
  const pending = useRef<string[]>([]);
  const frames = useRef<number[]>([]);

  // Collect the ids applied by this render; one frame pair flushes everything queued so far.
  useEffect(() => {
    if (!enabled || disabled.current || seen.length === 0) return;
    pending.current.push(...newIds(seen, lastId.current));
    lastId.current = seen[seen.length - 1] ?? lastId.current;
    if (frames.current.length > 0 || pending.current.length === 0) return;
    const outer = requestAnimationFrame(() => {
      const inner = requestAnimationFrame(() => {
        frames.current = [];
        const paintedAt = performance.timeOrigin + performance.now();
        for (const id of pending.current.splice(0)) {
          if (disabled.current || inFlight.current >= MAX_IN_FLIGHT) continue;
          inFlight.current += 1;
          void api.postLatency(id, paintedAt).then((status) => {
            inFlight.current -= 1;
            if (status === 403) disabled.current = true;
          });
        }
      });
      frames.current = [inner];
    });
    frames.current = [outer];
  }, [seen, enabled]);

  // Unmount only: cancel a frame that has not fired yet.
  useEffect(
    () => () => {
      for (const f of frames.current) cancelAnimationFrame(f);
    },
    [],
  );
}
