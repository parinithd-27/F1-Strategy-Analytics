"""Tests for f1sa.ingest.planner: schedule parsing, plan building, estimates and budget split."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from _fakes import load_fixture

from f1sa.ingest import planner as pl
from f1sa.ingest.manifest import Manifest
from f1sa.ingest.planner import (
    RaceWeekend,
    build_plan,
    describe,
    estimate,
    fastf1_eta_s,
    format_duration,
    jolpica_budget,
    jolpica_eta_s,
    parse_schedule,
)

TODAY = date(2026, 11, 10)


@pytest.fixture
def schedule() -> list[RaceWeekend]:
    return parse_schedule([load_fixture("races_2023")])


def test_parse_schedule_marks_sprint_weekends(schedule: list[RaceWeekend]) -> None:
    assert [(w.round, w.has_sprint) for w in schedule] == [(4, True), (5, False), (6, False)]
    assert schedule[1].name == "Miami Grand Prix" and schedule[1].race_date == date(2023, 5, 7)


def test_parse_schedule_rejects_duplicate_rounds() -> None:
    page = load_fixture("races_2023")
    races = page["MRData"]["RaceTable"]["Races"]
    races.append(dict(races[0]))
    with pytest.raises(ValueError, match="duplicate"):
        parse_schedule([page])


def test_plan_order_season_items_once_and_sprint_tables(schedule: list[RaceWeekend]) -> None:
    plan = build_plan([2023], ["fastf1", "jolpica"], {2023: schedule}, TODAY)
    labels = [(i.source, i.round) for i in plan]
    assert labels == [
        ("jolpica", None),
        ("jolpica", 4),
        ("jolpica", 5),
        ("jolpica", 6),
        ("fastf1", 4),
        ("fastf1", 5),
        ("fastf1", 6),
    ]  # jolpica pass first, one season item per season
    tables = {i.round: i.tables for i in plan if i.source == "jolpica"}
    assert tables[None] == ("schedule", "drivers", "constructors")
    assert "sprint_results" in tables[4] and "sprint_results" not in tables[5]


def test_future_races_are_skipped(schedule: list[RaceWeekend]) -> None:
    plan = build_plan([2023], ["jolpica"], {2023: schedule}, date(2023, 5, 7))
    skips = {i.round: i.skip_reason for i in plan}
    assert skips[4] is None
    assert skips[5] is not None and "future" in skips[5]  # race day itself: not complete yet
    assert skips[6] is not None


def test_round_filter(schedule: list[RaceWeekend]) -> None:
    plan = build_plan([2023], ["jolpica"], {2023: schedule}, TODAY, rounds={5})
    assert [i.round for i in plan] == [None, 5]
    with pytest.raises(ValueError, match="not in the schedule"):
        build_plan([2023], ["jolpica"], {2023: schedule}, TODAY, rounds={9})
    with pytest.raises(ValueError):
        build_plan([2023], ["ergast"], {2023: schedule}, TODAY)


def test_budget_split() -> None:
    assert jolpica_budget(450, ["jolpica"], 20) == 450
    assert jolpica_budget(450, ["jolpica", "fastf1"], 20) == 390
    assert jolpica_budget(450, ["fastf1"], 20) == 450
    assert jolpica_budget(50, ["jolpica", "fastf1"], 60) == 1


def test_eta_math() -> None:
    assert jolpica_eta_s(0, 450) == 0
    assert jolpica_eta_s(360, 450) == pytest.approx(360.0)  # within the full bucket: latency bound
    assert jolpica_eta_s(1350, 450) == pytest.approx(2 * 3600.0)  # 900 beyond the bucket at 450/h
    assert fastf1_eta_s(0, 20) == 0
    assert fastf1_eta_s(21, 20) == pytest.approx(3600.0 + 60.0)
    assert format_duration(59.2) == "1m 00s"
    assert format_duration(3725) == "1h 02m"
    assert format_duration(5) == "5s"


def test_estimate_counts_only_uncached_and_undone(
    schedule: list[RaceWeekend], tmp_path: Path
) -> None:
    plan = build_plan([2023], ["jolpica", "fastf1"], {2023: schedule}, TODAY)
    manifest = Manifest(Path("data/manifest.json"))
    manifest.mark_done(manifest.get("file", "jolpica", 2023, 4), [])
    manifest.mark_failed(manifest.get("file", "jolpica", 2023, 5), "x")
    probed: list[str] = []

    def needed(path: str, default: int) -> int:
        probed.append(path)
        return 0 if path.endswith("/results") else default

    est = estimate(
        plan,
        manifest,
        "file",
        only_failed=False,
        jolpica_requests_needed=needed,
        jolpica_per_hour=390,
        fastf1_races_per_hour=20,
        fastf1_cache_dir=Path("data/cache/fastf1"),
    )
    j = est["jolpica"]
    assert j.items_total == 4 and j.items_done == 1 and j.items_failed == 1 and j.items_to_run == 3
    assert j.sprint_weekends == 1
    # season item (3 x 1) + rounds 5 and 6 (results cached -> 0, qualifying 1, pitstops 1, laps 12)
    assert j.requests == 3 + 2 * (0 + 1 + 1 + 12)
    assert not any("/4/" in p for p in probed)  # done items are not probed
    f = est["fastf1"]
    assert f.items_to_run == 3
    assert f.requests == 3 * pl.FASTF1_CALLS_PER_RACE + pl.FASTF1_CALLS_PER_SEASON

    only_failed = estimate(
        plan,
        manifest,
        "file",
        only_failed=True,
        jolpica_requests_needed=needed,
        jolpica_per_hour=390,
        fastf1_races_per_hour=20,
        fastf1_cache_dir=Path("data/cache/fastf1"),
    )
    assert only_failed["jolpica"].items_to_run == 1 and only_failed["fastf1"].items_to_run == 0

    lines = describe(
        est,
        sources=["jolpica", "fastf1"],
        jolpica_per_hour=390,
        settings_per_hour=450,
        fastf1_races_per_hour=20,
    )
    text = "\n".join(lines)
    assert "390/h" in text and "60/h reserved for FastF1" in text and "sequentially" in text


def test_fastf1_cache_detection(tmp_path: Path) -> None:
    wk = RaceWeekend(2023, 5, "Miami Grand Prix", date(2023, 5, 7), False)
    cache = tmp_path / "ff1"
    assert not pl.fastf1_session_cached(cache, wk)
    folder = cache / "2023" / "2023-05-07_Miami_Grand_Prix" / "2023-05-07_Race"
    folder.mkdir(parents=True)
    (folder / "car_data.ff1pkl").write_bytes(b"x")
    assert pl.fastf1_session_cached(cache, wk)
