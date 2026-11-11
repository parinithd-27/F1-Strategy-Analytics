"""Reporting-only split of ``excluded_neutralised`` attempts by what neutralised the window.

This module never changes which attempts are labelled. It reads the finished
``undercut_attempts`` table and the clean laps, and only the counts CSV uses its result
(``n_excluded_neutralised_sc_vsc_red_flag`` and ``n_excluded_neutralised_sustained_slow``,
which add up to ``n_excluded_neutralised``).

Timing-only rule, per neutralised lap (a lap with any SC/VSC/red-flag/neutralised flag):

* a **run** is a maximal block of consecutive neutralised laps in one race;
* a lap is **sustained slow running** (``SUSTAINED``) when its run is at least
  ``SUSTAINED_MIN_LAPS`` long, contains no red-flag lap and has a median racing-car ratio
  (``clean.track_ratios``) below ``SUSTAINED_MAX_RUN_MEDIAN``, its flags come from the Jolpica
  heuristic (not FastF1 TrackStatus), and its own ratio is known and below
  ``SUSTAINED_MAX_RATIO``. This is the wet or drying pattern: every car 1.1-1.3x slower for many
  laps, and a long safety car (median 1.3x or more) is not mistaken for it;
* every other neutralised lap is **SC/VSC/red flag** (``SC_LIKE``): short runs, red-flag runs,
  laps at safety-car pace (>= 1.35x), laps where every car pits (ratio unknown) and exact FastF1
  flags.

A restart lap (``undercut._is_restart``) takes the kind of the last neutralised lap before it,
so the 3-lap red-flag restart period is always ``SC_LIKE``. An attempt is ``SUSTAINED`` when
every neutralised or restart lap in its window (``undercut.attempt_window``) is sustained slow
running, and ``SC_LIKE`` otherwise.

The thresholds were set from the 2022-2023 spot-check races (2022 R4, R7; 2023 R6, R13) and the
2021-2024 run profile, never from 2025: 9 runs of 8 or more laps, with medians 1.10-1.20 for the
six wet races (2021 R2, 2022 R4, R7, 2023 R6, R13, 2024 R12) and 1.31-1.44 for the three long
safety cars (2022 R19, R21, 2024 R5). It is a heuristic for reporting only.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from statistics import median

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from f1sa.etl.clean import track_ratios
from f1sa.features.undercut import RED_FLAG_RESTART_LAPS, _is_restart, attempt_window

SUSTAINED_MIN_LAPS = 8
SUSTAINED_MAX_RATIO = 1.35
SUSTAINED_MAX_RUN_MEDIAN = 1.25
SC_LIKE = "sc_vsc_red_flag"
SUSTAINED = "sustained_slow"
KINDS = (SC_LIKE, SUSTAINED)

AttemptKey = tuple[int, int, int, str, str, float, int]
"""(season, round, lap, attacker_id, defender_id, gap_threshold_s, response_window_laps)."""


@dataclass(frozen=True)
class TrackLap:
    """One lap of one race: its flags and the racing-car ratio."""

    lap: int
    neutral: bool
    red_flag: bool = False
    ratio: float | None = None
    heuristic: bool = True


def classify_laps(laps: Iterable[TrackLap]) -> dict[int, str]:
    """The kind (``SC_LIKE`` or ``SUSTAINED``) of every neutralised lap of one race."""
    neutral = sorted((t for t in laps if t.neutral), key=lambda t: t.lap)
    runs: list[list[TrackLap]] = []
    for t in neutral:
        if runs and runs[-1][-1].lap == t.lap - 1:
            runs[-1].append(t)
        else:
            runs.append([t])
    kinds: dict[int, str] = {}
    for run in runs:
        known = [t.ratio for t in run if t.ratio is not None]
        long_run = (
            len(run) >= SUSTAINED_MIN_LAPS
            and not any(t.red_flag for t in run)
            and bool(known)
            and median(known) < SUSTAINED_MAX_RUN_MEDIAN
        )
        for t in run:
            slow = (
                long_run and t.heuristic and t.ratio is not None and t.ratio < SUSTAINED_MAX_RATIO
            )
            kinds[t.lap] = SUSTAINED if slow else SC_LIKE
    return kinds


def window_kind(
    kinds: Mapping[int, str],
    neutral: Mapping[int, bool],
    red_flag: frozenset[int],
    window: Iterable[int],
) -> str | None:
    """What neutralised ``window``: ``SUSTAINED`` only if every cause is sustained slow running,
    ``SC_LIKE`` if any is not, None if no lap in the window is neutralised or a restart lap."""
    causes: set[str] = set()
    for j in window:
        if neutral.get(j, False):
            causes.add(kinds.get(j, SC_LIKE))
        elif _is_restart(neutral, j, red_flag):
            last = next(
                (j - b for b in range(1, RED_FLAG_RESTART_LAPS + 1) if neutral.get(j - b, False)),
                None,
            )
            causes.add(kinds.get(last, SC_LIKE) if last is not None else SC_LIKE)
    if not causes:
        return None
    return SUSTAINED if causes == {SUSTAINED} else SC_LIKE


def classify_attempts(
    attempts: Sequence[Mapping[str, object]],
    track: Mapping[tuple[int, int], Sequence[TrackLap]],
    pit_laps: Mapping[tuple[int, int, str], frozenset[int]],
) -> dict[AttemptKey, str]:
    """Kind of every ``excluded_neutralised`` attempt (pure Python; see the module docstring)."""
    per_race: dict[tuple[int, int], tuple[dict[int, str], dict[int, bool], frozenset[int]]] = {}
    for race, laps in track.items():
        laps = list(laps)
        per_race[race] = (
            classify_laps(laps),
            {t.lap: t.neutral for t in laps},
            frozenset(t.lap for t in laps if t.red_flag),
        )
    out: dict[AttemptKey, str] = {}
    for a in attempts:
        if a["outcome_category"] != "excluded_neutralised":
            continue
        season, round_, lap = int(a["season"]), int(a["round"]), int(a["lap"])  # type: ignore[call-overload]
        k = int(a["response_window_laps"])  # type: ignore[call-overload]
        kinds, neutral, red = per_race.get((season, round_), ({}, {}, frozenset()))
        d_pits = pit_laps.get((season, round_, str(a["defender_id"])), frozenset())
        kind = window_kind(kinds, neutral, red, attempt_window(lap, k, d_pits))
        key: AttemptKey = (
            season,
            round_,
            lap,
            str(a["attacker_id"]),
            str(a["defender_id"]),
            float(a["gap_threshold_s"]),  # type: ignore[arg-type]
            k,
        )
        out[key] = kind if kind is not None else SC_LIKE
    return out


def neutralised_kinds(
    attempts: DataFrame, laps: DataFrame, pitstops: DataFrame
) -> dict[AttemptKey, str]:
    """``classify_attempts`` on Spark inputs: stored attempts, clean laps and clean pit stops.

    Only excluded-neutralised attempts and per-race-lap summaries are collected (small).
    """
    rows = [
        r.asDict()
        for r in attempts.where(F.col("outcome_category") == "excluded_neutralised")
        .select(
            "season",
            "round",
            "lap",
            "attacker_id",
            "defender_id",
            "gap_threshold_s",
            "response_window_laps",
            "outcome_category",
        )
        .collect()
    ]
    if not rows:
        return {}
    flags = laps.groupBy("season", "round", "lap").agg(
        F.max(
            F.coalesce(F.col("is_neutralised_lap"), F.lit(False))
            | F.coalesce(F.col("is_sc_lap"), F.lit(False))
            | F.coalesce(F.col("is_vsc_lap"), F.lit(False))
            | F.coalesce(F.col("is_red_flag_lap"), F.lit(False))
        ).alias("neutral"),
        F.max(F.coalesce(F.col("is_red_flag_lap"), F.lit(False))).alias("red_flag"),
        F.min(F.coalesce(F.col("track_status_source") == "heuristic", F.lit(True))).alias(
            "heuristic"
        ),
    )
    ratios = track_ratios(laps).select("season", "round", "lap", "ratio")
    track: dict[tuple[int, int], list[TrackLap]] = defaultdict(list)
    for r in flags.join(ratios, ["season", "round", "lap"], "left").collect():
        track[(int(r["season"]), int(r["round"]))].append(
            TrackLap(
                lap=int(r["lap"]),
                neutral=bool(r["neutral"]),
                red_flag=bool(r["red_flag"]),
                ratio=float(r["ratio"]) if r["ratio"] is not None else None,
                heuristic=bool(r["heuristic"]),
            )
        )
    pits: dict[tuple[int, int, str], set[int]] = defaultdict(set)
    for r in (
        pitstops.where(F.col("lap").isNotNull())
        .select("season", "round", "driver_id", "lap")
        .collect()
    ):
        pits[(int(r["season"]), int(r["round"]), str(r["driver_id"]))].add(int(r["lap"]))
    return classify_attempts(rows, track, {k: frozenset(v) for k, v in pits.items()})
