"use client";

import { useHealth } from "@/lib/useHealth";

/** A fixed-height strip under the top bar (always rendered, so it never shifts the page). In sample
 * mode it carries the SYNTHETIC DEMO DATA warning. */
export function DataBanner() {
  const health = useHealth();
  const mode = health?.lake.mode ?? null;
  if (mode === "sample") {
    return (
      <div className="data-banner is-synthetic" role="note" aria-label="Data notice">
        <strong>SYNTHETIC DEMO DATA</strong>
        <span> · sample races: real Jolpica timing, generated tyre and telemetry values</span>
      </div>
    );
  }
  return (
    <div className="data-banner" role="note" aria-label="Data notice">
      {health === null
        ? "Checking the data source…"
        : mode === "full"
          ? `Full lake (${health.lake.scheme === "hdfs" ? "HDFS" : "local"}): real timing data`
          : "Data source unavailable"}
    </div>
  );
}
