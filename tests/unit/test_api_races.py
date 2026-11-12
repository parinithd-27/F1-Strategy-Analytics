"""``GET /api/races`` and the clean-zone readers (file:// and read-only HttpFS)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from _api_fixtures import (
    DEFAULT_RACES,
    DirFakeHttpFS,
    LakeRace,
    httpfs_session,
    lake_files,
    make_api,
    make_settings,
    write_file_lake,
)

from f1sa.api.catalog import (
    ALLOWED_OPS,
    CatalogError,
    FileLakeReader,
    HttpFsLakeReader,
    RaceCatalog,
    RaceInfo,
    load_races,
    race_laps,
    reader_for,
)

pytestmark = pytest.mark.anyio

EXPECTED = [
    {"season": 2023, "round": 5, "name": "Miami Grand Prix", "laps": 57, "synthetic": False},
    {"season": 2024, "round": 5, "name": "Chinese Grand Prix", "laps": 56, "synthetic": True},
]
WITH_2025 = [
    *EXPECTED,
    {"season": 2025, "round": 6, "name": "Miami Grand Prix", "laps": 57, "synthetic": False},
]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _rows(races: list[RaceInfo]) -> list[dict[str, Any]]:
    return [
        {
            "season": r.season,
            "round": r.round,
            "name": r.name,
            "laps": r.laps,
            "synthetic": r.synthetic,
        }
        for r in races
    ]


# ------------------------------------------------------------------ laps rule
def test_race_laps_is_the_winners_laps() -> None:
    assert race_laps([{"position": 2, "laps": 58}, {"position": 1, "laps": 57}], []) == 57


def test_race_laps_without_a_winner_is_the_highest_lap() -> None:
    assert race_laps([{"position": None, "laps": 9}], [{"lap": 3}, {"lap": 12}]) == 12
    assert race_laps([], [{"lap": None}]) is None


# ------------------------------------------------------------------ file reader
def test_file_reader_lists_races_and_hides_2025(tmp_path: Path) -> None:
    write_file_lake(tmp_path / "lake")
    reader = FileLakeReader(tmp_path / "lake" / "clean", allow_test_season=False)
    assert _rows(load_races(reader, allow_test_season=False)) == EXPECTED
    assert reader.partitions("races") == [(2023, 5), (2024, 5)]


def test_file_reader_shows_2025_when_allowed(tmp_path: Path) -> None:
    write_file_lake(tmp_path / "lake")
    reader = FileLakeReader(tmp_path / "lake" / "clean", allow_test_season=True)
    assert _rows(load_races(reader, allow_test_season=True)) == WITH_2025


def test_file_reader_never_opens_2025_unless_allowed(tmp_path: Path) -> None:
    write_file_lake(tmp_path / "lake")
    reader = FileLakeReader(tmp_path / "lake" / "clean", allow_test_season=False)
    with pytest.raises(CatalogError, match="sealed"):
        reader.read_rows("races", 2025, 6, ["race_name"])


def test_race_without_laps_partition_or_winner(tmp_path: Path) -> None:
    races = (
        LakeRace(2022, 1, "No laps", laps=()),
        LakeRace(2022, 2, "No winner", results=((None, 40), (2, 40)), laps=(1, 2, 44)),
        LakeRace(2022, 3, None, results=()),
    )
    write_file_lake(tmp_path / "lake", races)
    reader = FileLakeReader(tmp_path / "lake" / "clean", allow_test_season=False)
    assert _rows(load_races(reader, allow_test_season=False)) == [
        {"season": 2022, "round": 2, "name": "No winner", "laps": 44, "synthetic": False},
        {"season": 2022, "round": 3, "name": None, "laps": 3, "synthetic": False},
    ]


def test_only_the_four_tables_are_readable(tmp_path: Path) -> None:
    reader = FileLakeReader(tmp_path, allow_test_season=True)
    for table in ("pitstops", "telemetry_lap", "../raw", "telemetry_raw", "stints_tracinginsights"):
        with pytest.raises(CatalogError, match="not readable"):
            reader.partitions(table)


def test_missing_clean_zone_is_an_error(tmp_path: Path) -> None:
    reader = FileLakeReader(tmp_path / "nothing", allow_test_season=False)
    with pytest.raises(CatalogError, match="not found"):
        load_races(reader, allow_test_season=False)


def test_reader_for_picks_the_scheme(tmp_path: Path) -> None:
    assert isinstance(reader_for(make_settings(tmp_path)), FileLakeReader)
    hdfs = reader_for(make_settings(tmp_path, data_lake_uri="hdfs://namenode:8020/f1"))
    assert isinstance(hdfs, HttpFsLakeReader) and hdfs.clean == "/f1/clean"


# ------------------------------------------------------------------ HttpFS reader
def _httpfs(
    tmp_path: Path, *, allow: bool, races: tuple[LakeRace, ...] = DEFAULT_RACES
) -> tuple[DirFakeHttpFS, HttpFsLakeReader]:
    fake = DirFakeHttpFS()
    fake.add_tree("/f1/clean", lake_files(races))
    fake.add_tree("/f1/raw", {"jolpica/laps/season=2024/round=5/laps.json": b"[]"})
    fake.add_tree("/f1/features", {"pit_loss/season=2024/round=5/part-0.parquet": b"x"})
    settings = make_settings(tmp_path, data_lake_uri="hdfs://namenode:8020/f1")
    reader = HttpFsLakeReader(
        settings, "/f1", allow_test_season=allow, session=httpfs_session(fake)
    )
    return fake, reader


def test_httpfs_reader_equals_the_file_reader(tmp_path: Path) -> None:
    write_file_lake(tmp_path / "lake")
    for allow in (False, True):
        _fake, http = _httpfs(tmp_path, allow=allow)
        local = FileLakeReader(tmp_path / "lake" / "clean", allow_test_season=allow)
        assert load_races(http, allow_test_season=allow) == load_races(
            local, allow_test_season=allow
        )


def test_httpfs_reader_is_read_only(tmp_path: Path) -> None:
    fake, reader = _httpfs(tmp_path, allow=True)
    load_races(reader, allow_test_season=True)
    assert fake.ops, "the reader made no request"
    assert {op for op, _ in fake.ops} <= ALLOWED_OPS == {"LISTSTATUS", "OPEN"}
    assert set(fake.methods) == {"GET"}
    before = len(fake.ops)
    for op in ("DELETE", "CREATE", "RENAME", "MKDIRS", "SETPERMISSION", "APPEND"):
        with pytest.raises(CatalogError, match="not allowed"):
            reader._get(op, "/f1/clean/races")
    assert len(fake.ops) == before, "a refused op reached the server"


def test_httpfs_reader_stays_in_the_clean_zone(tmp_path: Path) -> None:
    fake, reader = _httpfs(tmp_path, allow=True)
    load_races(reader, allow_test_season=True)
    assert all(path.startswith("/f1/clean/") for _, path in fake.ops)
    assert {path.split("/")[3] for _, path in fake.ops} == {"races", "results", "laps", "stints"}
    before = len(fake.ops)
    for path in (
        "/f1/raw/jolpica/laps/season=2024/round=5/laps.json",
        "/f1/clean/../raw/jolpica",
        "/f1/clean/races/../../raw",
        "/f1/features/pit_loss",
        "/f1/clean",
        "/f1/clean/pitstops",
        "/f1/clean/telemetry_raw/season=2024",
        "/f1/cleaner/races",
        "f1/clean/races",
    ):
        for op in ("LISTSTATUS", "OPEN"):
            with pytest.raises(CatalogError):
                reader._get(op, path)
    assert len(fake.ops) == before, "a refused path reached the server"


def test_httpfs_reader_never_lists_or_opens_2025_unless_allowed(tmp_path: Path) -> None:
    fake, reader = _httpfs(tmp_path, allow=False)
    assert _rows(load_races(reader, allow_test_season=False)) == EXPECTED
    assert not [p for _, p in fake.ops if "season=2025" in p]
    with pytest.raises(CatalogError, match="sealed"):
        reader.read_rows("results", 2025, 6, ["position", "laps"])
    assert not [p for _, p in fake.ops if "season=2025" in p]

    fake2, allowed = _httpfs(tmp_path, allow=True)
    assert _rows(load_races(allowed, allow_test_season=True)) == WITH_2025
    assert [p for _, p in fake2.ops if "season=2025" in p]


def test_httpfs_errors_become_catalog_errors(tmp_path: Path) -> None:
    fake, reader = _httpfs(tmp_path, allow=False)
    fake.faults["LISTSTATUS"] = [500]
    with pytest.raises(CatalogError, match="HTTP 500"):
        load_races(reader, allow_test_season=False)


# ------------------------------------------------------------------ catalog cache
async def test_catalog_caches_for_the_ttl(tmp_path: Path) -> None:
    write_file_lake(tmp_path / "lake")
    now = [0.0]
    catalog = RaceCatalog(
        lambda: FileLakeReader(tmp_path / "lake" / "clean", allow_test_season=False),
        allow_test_season=False,
        ttl_s=10.0,
        clock=lambda: now[0],
    )
    assert len(await catalog.races()) == 2
    now[0] = 9.0
    await catalog.races()
    assert catalog.loads == 1
    now[0] = 10.5
    await catalog.races()
    assert catalog.loads == 2
    assert (await catalog.get(2024, 5)) == RaceInfo(2024, 5, "Chinese Grand Prix", 56, True)
    assert await catalog.get(2025, 6) is None


# ------------------------------------------------------------------ endpoint
async def test_get_races_hides_2025_by_default(tmp_path: Path) -> None:
    write_file_lake(tmp_path / "lake")
    api = make_api(make_settings(tmp_path))
    async with api.client() as c:
        resp = await c.get("/api/races")
    assert resp.status_code == 200
    assert resp.json() == EXPECTED


async def test_get_races_shows_2025_with_allow_test_season(tmp_path: Path) -> None:
    write_file_lake(tmp_path / "lake")
    api = make_api(make_settings(tmp_path, allow_test_season=True))
    async with api.client() as c:
        resp = await c.get("/api/races")
    assert resp.json() == WITH_2025


async def test_get_races_over_httpfs(tmp_path: Path) -> None:
    fake = DirFakeHttpFS()
    fake.add_tree("/f1/clean", lake_files(DEFAULT_RACES))
    api = make_api(
        make_settings(tmp_path, data_lake_uri="hdfs://namenode:8020/f1"),
        http_session=httpfs_session(fake),
    )
    async with api.client() as c:
        resp = await c.get("/api/races")
    assert resp.status_code == 200 and resp.json() == EXPECTED
    assert {op for op, _ in fake.ops} <= {"LISTSTATUS", "OPEN"}
    assert not [p for _, p in fake.ops if "season=2025" in p]


async def test_get_races_503_without_a_clean_zone(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    async with api.client() as c:
        resp = await c.get("/api/races")
    assert resp.status_code == 503
    assert resp.json() == {"detail": "clean zone unavailable"}


# ------------------------------------------------------------------ synthetic flag
def test_synthetic_comes_from_the_stints_of_each_race(tmp_path: Path) -> None:
    races = (
        LakeRace(2022, 1, "All synthetic", stints_synthetic=(True, True)),
        LakeRace(2022, 2, "Mixed", stints_synthetic=(False, True)),
        LakeRace(2022, 3, "Real", stints_synthetic=(False, False)),
        LakeRace(2022, 4, "Null flags", stints_synthetic=(None, None)),
        LakeRace(2022, 5, "No stints", stints_synthetic=None),
    )
    write_file_lake(tmp_path / "lake", races)
    local = FileLakeReader(tmp_path / "lake" / "clean", allow_test_season=False)
    flags = {r.round: r.synthetic for r in load_races(local, allow_test_season=False)}
    assert flags == {1: True, 2: True, 3: False, 4: False, 5: False}
    fake = DirFakeHttpFS()
    fake.add_tree("/f1/clean", lake_files(races))
    http = HttpFsLakeReader(
        make_settings(tmp_path, data_lake_uri="hdfs://namenode:8020/f1"),
        "/f1",
        allow_test_season=False,
        session=httpfs_session(fake),
    )
    assert load_races(http, allow_test_season=False) == load_races(local, allow_test_season=False)
    assert {op for op, _ in fake.ops} <= {"LISTSTATUS", "OPEN"}
    assert {p.split("/")[3] for _, p in fake.ops} == {"races", "results", "laps", "stints"}


def test_stints_of_2025_are_never_read_unless_allowed(tmp_path: Path) -> None:
    fake, reader = _httpfs(tmp_path, allow=False)
    load_races(reader, allow_test_season=False)
    assert not [p for _, p in fake.ops if "season=2025" in p]
    assert any("/stints/season=2024/round=5" in p for _, p in fake.ops)
