"use client";

import { useEffect, useState } from "react";

import { api, type Health } from "./api";

/** ``/api/health`` once per page (lake mode for the synthetic banner). */
export function useHealth(): Health | null {
  const [health, setHealth] = useState<Health | null>(null);
  useEffect(() => {
    const ctl = new AbortController();
    api.health(ctl.signal).then(setHealth).catch(() => setHealth(null));
    return () => ctl.abort();
  }, []);
  return health;
}
