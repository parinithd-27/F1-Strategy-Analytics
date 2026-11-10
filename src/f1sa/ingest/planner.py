"""Work planning and request/duration estimates for bulk ingestion.

The schedule (one Jolpica request per season, cached) defines the race weekends. Each season
yields one Jolpica *season item* (schedule, drivers, constructors: fetched once per season) and one
*race item* per round and source. Sprint results are planned only for weekends whose schedule
entry has a ``Sprint`` session.

Budget: Jolpica allows 500 req/h per IP. FastF1 makes its own Jolpica calls (about 3 per race), so
when both sources are requested our Jolpica client runs at ``JOLPICA_MAX_PER_HOUR`` minus a
reserve for FastF1, FastF1 races are paced to ``FASTF1_MAX_RACES_PER_HOUR``, and the two passes
run one after the other (Jolpica first). The two sources are never at full budget together.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from f1sa.ingest.jolpica_client import JsonPage
from f1sa.ingest.manifest import Manifest
from f1sa.lake.paths import JOLPICA_SEASON_TABLES

SOURCES_ORDER = ("jolpica", "fastf1")
JOLPICA_RACE_TABLES = ("results", "qualifying", "pitstops", "laps")
JOLPICA_SPRINT_TABLE = "sprint_results"
JOLPICA_PATHS = {
    "schedule": "{season}/races",
    "drivers": "{season}/drivers",
    "constructors": "{season}/constructors",
    "results": "{season}/{round}/results",
    "qualifying": "{season}/{round}/qualifying",
    "pitstops": "{season}/{round}/pitstops",
    "laps": "{season}/{round}/laps",
    "sprint_results": "{season}/{round}/sprint",
}
# Pages per table when nothing is cached yet. Laps: about 1,100-1,400 timings at 100 per page.
DEFAULT_PAGES = {
    "schedule": 1,
    "drivers": 1,
    "constructors": 1,
    "results": 1,
    "qualifying": 1,
    "pitstops": 1,
    "laps": 12,
    "sprint_results": 1,
}
JOLPICA_LATENCY_S = 1.0
"""Observed average wall time per uncached Jolpica request (Step 3: 18 requests in about 20 s)."""
FASTF1_CALLS_PER_RACE = 12
"""Approximate requests per uncached FastF1 race load (about 10 livetiming + 2-3 jolpi.ca)."""
FASTF1_JOLPICA_CALLS_PER_RACE = 3
FASTF1_CALLS_PER_SEASON = 2
"""FastF1 event-schedule requests per season (then cached by FastF1)."""
FASTF1_LOAD_S = 60.0
"""Rough wall time to load and write one race with telemetry."""


@dataclass(frozen=True)
class RaceWeekend:
    season: int
    round: int
    name: str
    race_date: date | None
    has_sprint: bool


@dataclass(frozen=True)
class WorkItem:
    source: str
    season: int
    round: int | None  # None = season-level item
    tables: tuple[str, ...]
    weekend: RaceWeekend | None = None
    skip_reason: str | None = None

    @property
    def label(self) -> str:
        where = "season tables" if self.round is None else f"round {self.round:>2}"
        name = f" {self.weekend.name}" if self.weekend else ""
        return f"{self.source} {self.season} {where}{name}"


def parse_schedule(pages: Iterable[JsonPage]) -> list[RaceWeekend]:
    """Race weekends from raw ``{season}/races`` pages (keeps the ``Sprint`` marker)."""
    out: list[RaceWeekend] = []
    for page in pages:
        for race in page["MRData"].get("RaceTable", {}).get("Races", []):
            raw_date = race.get("date")
            out.append(
                RaceWeekend(
                    season=int(race["season"]),
                    round=int(race["round"]),
                    name=str(race.get("raceName", "")),
                    race_date=date.fromisoformat(raw_date) if raw_date else None,
                    has_sprint="Sprint" in race,
                )
            )
    rounds = [w.round for w in out]
    if len(rounds) != len(set(rounds)):
        raise ValueError("schedule contains duplicate rounds")
    return sorted(out, key=lambda w: w.round)


def build_plan(
    seasons: Sequence[int],
    sources: Sequence[str],
    schedules: dict[int, list[RaceWeekend]],
    today: date,
    rounds: set[int] | None = None,
) -> list[WorkItem]:
    """All work items, Jolpica pass first, then FastF1; seasons and rounds in order.

    A race counts as complete the day after it was run; later ones are skipped as ``future``.
    ``rounds`` restricts race items (the season-level Jolpica item is always included).
    """
    unknown = set(sources) - set(SOURCES_ORDER)
    if unknown or not sources:
        raise ValueError(f"sources must be a subset of {SOURCES_ORDER}, got {list(sources)}")
    plan: list[WorkItem] = []
    for source in (s for s in SOURCES_ORDER if s in sources):
        for season in seasons:
            weekends = schedules[season]
            if source == "jolpica":
                plan.append(WorkItem("jolpica", season, None, JOLPICA_SEASON_TABLES))
            for wk in weekends:
                if rounds is not None and wk.round not in rounds:
                    continue
                skip = None
                if wk.race_date is None or wk.race_date >= today:
                    skip = "future (race not completed yet)"
                if source == "jolpica":
                    tables = JOLPICA_RACE_TABLES + (
                        (JOLPICA_SPRINT_TABLE,) if wk.has_sprint else ()
                    )
                else:
                    tables = ("laps", "session", "telemetry", "position")
                plan.append(WorkItem(source, season, wk.round, tables, wk, skip))
        if rounds is not None:
            found = {i.round for i in plan if i.source == source and i.round is not None}
            missing = rounds - found
            if missing:
                raise ValueError(f"round(s) {sorted(missing)} not in the schedule")
    return plan


def jolpica_budget(
    settings_per_hour: int, sources: Sequence[str], fastf1_races_per_hour: int
) -> int:
    """Our Jolpica client's hourly budget, with a reserve for FastF1 when both sources run."""
    if "fastf1" in sources and "jolpica" in sources:
        reserve = fastf1_races_per_hour * FASTF1_JOLPICA_CALLS_PER_RACE
        return max(1, settings_per_hour - reserve)
    return settings_per_hour


def jolpica_eta_s(
    requests: int,
    per_hour: int,
    available: int | None = None,
    latency_s: float = JOLPICA_LATENCY_S,
) -> float:
    """Wall time for ``requests`` network calls when ``available`` (default: all ``per_hour``)
    are still allowed in the current rolling hour; beyond that the rate is ``per_hour``/h."""
    if requests <= 0:
        return 0.0
    free = per_hour if available is None else max(0, min(available, per_hour))
    throttled = max(0, requests - free) * 3600.0 / per_hour
    return max(requests * latency_s, throttled)


def fastf1_eta_s(races: int, races_per_hour: int, load_s: float = FASTF1_LOAD_S) -> float:
    if races <= 0:
        return 0.0
    return max(races * load_s, (races - 1) * 3600.0 / races_per_hour + load_s)


def format_duration(seconds: float) -> str:
    total = math.ceil(seconds)
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def fastf1_session_cached(cache_dir: Path, weekend: RaceWeekend) -> bool:
    """Heuristic: FastF1 has cached car data for this race (no requests needed)."""
    if weekend.race_date is None:
        return False
    pattern = f"{weekend.race_date.isoformat()}_*/*_Race/car_data.ff1pkl"
    return any((cache_dir / str(weekend.season)).glob(pattern))


@dataclass
class SourceEstimate:
    items_total: int = 0
    items_to_run: int = 0
    items_done: int = 0
    items_failed: int = 0
    items_skipped: int = 0
    requests: int = 0
    eta_s: float = 0.0
    sprint_weekends: int = 0
    by_status: dict[str, int] = field(default_factory=dict)


def estimate(
    plan: Sequence[WorkItem],
    manifest: Manifest,
    sink: str,
    *,
    only_failed: bool,
    jolpica_requests_needed: Callable[[str, int], int],
    jolpica_per_hour: int,
    fastf1_races_per_hour: int,
    fastf1_cache_dir: Path,
    jolpica_available: int | None = None,
) -> dict[str, SourceEstimate]:
    """Per-source counts of items and expected network requests / duration for this run."""
    out: dict[str, SourceEstimate] = {}
    fastf1_seasons: set[int] = set()
    for item in plan:
        est = out.setdefault(item.source, SourceEstimate())
        est.items_total += 1
        existing = manifest.peek(sink, item.source, item.season, item.round)
        status = existing.status if existing else "new"
        est.by_status[status] = est.by_status.get(status, 0) + 1
        if item.weekend and item.weekend.has_sprint and item.source == "jolpica":
            est.sprint_weekends += 1
        if status == "done":
            est.items_done += 1
        if status == "failed":
            est.items_failed += 1
        if item.skip_reason is not None:
            est.items_skipped += 1
            continue
        if not Manifest.should_run(existing, only_failed=only_failed):
            continue
        est.items_to_run += 1
        if item.source == "jolpica":
            for table in item.tables:
                path = JOLPICA_PATHS[table].format(season=item.season, round=item.round)
                est.requests += jolpica_requests_needed(path, DEFAULT_PAGES[table])
        elif item.weekend is not None and not fastf1_session_cached(fastf1_cache_dir, item.weekend):
            est.requests += FASTF1_CALLS_PER_RACE
            if item.season not in fastf1_seasons:
                fastf1_seasons.add(item.season)
                est.requests += FASTF1_CALLS_PER_SEASON
    if "jolpica" in out:
        out["jolpica"].eta_s = jolpica_eta_s(
            out["jolpica"].requests, jolpica_per_hour, jolpica_available
        )
    if "fastf1" in out:
        out["fastf1"].eta_s = fastf1_eta_s(out["fastf1"].items_to_run, fastf1_races_per_hour)
    return out


def describe(
    estimates: dict[str, SourceEstimate],
    *,
    sources: Sequence[str],
    jolpica_per_hour: int,
    settings_per_hour: int,
    fastf1_races_per_hour: int,
) -> list[str]:
    """Human-readable plan summary lines (printed before every run and by --dry-run)."""
    lines: list[str] = []
    for source in SOURCES_ORDER:
        if source not in estimates:
            continue
        e = estimates[source]
        statuses = ", ".join(f"{k} {v}" for k, v in sorted(e.by_status.items()))
        lines.append(
            f"  {source:<7} items {e.items_total} ({statuses}); to run now: {e.items_to_run}; "
            f"skipped (future): {e.items_skipped}"
        )
        if source == "jolpica":
            lines.append(
                f"          sprint weekends: {e.sprint_weekends}; expected network requests: "
                f"~{e.requests} at {jolpica_per_hour}/h -> ETA ~{format_duration(e.eta_s)}"
            )
        else:
            lines.append(
                f"          expected FastF1 requests: ~{e.requests} "
                f"(about {FASTF1_CALLS_PER_RACE}/race uncached); "
                f"paced at {fastf1_races_per_hour} races/h -> "
                f"ETA ~{format_duration(e.eta_s)}"
            )
    if "jolpica" in sources and "fastf1" in sources:
        reserve = settings_per_hour - jolpica_per_hour
        lines.append(
            f"  budget split: our Jolpica client {jolpica_per_hour}/h + {reserve}/h reserved for "
            f"FastF1's own jolpi.ca calls (~{FASTF1_JOLPICA_CALLS_PER_RACE}/race x "
            f"{fastf1_races_per_hour} races/h) = {settings_per_hour}/h (Jolpica limit 500/h); "
            "passes run sequentially: jolpica, then fastf1"
        )
        total = sum(e.eta_s for e in estimates.values())
        lines.append(f"  total ETA ~{format_duration(total)}")
    return lines


def schedule_summary(schedules: dict[int, list[RaceWeekend]]) -> dict[str, Any]:
    return {
        str(season): {"races": len(w), "sprints": sum(x.has_sprint for x in w)}
        for season, w in schedules.items()
    }
