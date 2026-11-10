"""Tests for f1sa.ingest.cli (fixture-backed Jolpica client, fake FastF1 loader)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from _fakes import FixtureAdapter

from f1sa.config import Settings
from f1sa.ingest import cli
from f1sa.ingest.fastf1_loader import FastF1AuthError, RaceData
from f1sa.ingest.jolpica_client import JolpicaClient

PART = Path("data/lake/raw/{source}/season=2023/round=5")


@pytest.fixture
def fixture_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli,
        "JolpicaClient",
        lambda s: JolpicaClient(s, transport=FixtureAdapter(), cache_backend="memory"),
    )


def test_parser_defaults_and_sources() -> None:
    args = cli.build_parser().parse_args(["race", "--season", "2023", "--round", "5"])
    assert args.sources == ["jolpica", "fastf1"] and args.sink == "file"
    args = cli.build_parser().parse_args(
        ["race", "--season", "2023", "--round", "5", "--sources", "fastf1,jolpica,fastf1"]
    )
    assert args.sources == ["fastf1", "jolpica"]


@pytest.mark.parametrize(
    "argv",
    [
        ["race", "--season", "2023", "--round", "5", "--sources", "ergast"],
        ["race", "--season", "2023", "--round", "5", "--sources", ""],
        ["race", "--season", "2023", "--round", "5", "--sink", "s3"],
        ["race", "--round", "5"],
        [],
    ],
)
def test_parser_rejects_bad_args(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2


def test_hdfs_sink_is_explicitly_not_available(capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(["race", "--season", "2023", "--round", "5", "--sink", "hdfs"])
    assert code == 2
    assert "Step 4" in capsys.readouterr().err
    assert not Path("data").exists()


def test_jolpica_race_writes_raw_partition(
    fixture_client: None, capsys: pytest.CaptureFixture[str]
) -> None:
    code = cli.main(["race", "--season", "2023", "--round", "5", "--sources", "jolpica"])
    assert code == 0
    part = Path(str(PART).format(source="jolpica"))
    names = sorted(p.name for p in part.iterdir())
    assert names == sorted([f"{n}.json" for n in cli.JOLPICA_ENDPOINTS] + ["_manifest.json"])
    laps_pages = json.loads((part / "laps.json").read_text())
    assert len(laps_pages) == 3 and laps_pages[0]["MRData"]["offset"] == "0"  # raw pages kept
    manifest = json.loads((part / "_manifest.json").read_text())
    assert manifest["licence"].startswith("CC BY-NC-SA")
    assert {f["name"]: f["rows"] for f in manifest["files"]}["laps.json"] == 9
    out = capsys.readouterr().out
    assert "network=" in out and "laps.json" in out


def test_jolpica_required_endpoint_empty_fails_without_writing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        cli,
        "JolpicaClient",
        lambda s: JolpicaClient(s, transport=FixtureAdapter(), cache_backend="memory"),
    )
    # round 6 is not in the fixtures -> 404 on the first endpoint
    code = cli.main(["race", "--season", "2023", "--round", "6", "--sources", "jolpica"])
    assert code == 1
    assert "FAILED: JolpicaHTTPError" in capsys.readouterr().err
    assert not Path("data/lake/raw/jolpica/season=2023/round=6").exists()


def test_fastf1_auth_failure_exits_1_and_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail(*_a: Any, **_k: Any) -> RaceData:
        raise FastF1AuthError("FastF1 needs an F1 TV Access login")

    monkeypatch.setattr(cli.fastf1_loader, "load_race", fail)
    code = cli.main(["race", "--season", "2023", "--round", "5", "--sources", "fastf1"])
    assert code == 1
    assert "FAILED: FastF1AuthError: FastF1 needs an F1 TV Access login" in capsys.readouterr().err
    assert not Path(str(PART).format(source="fastf1")).exists()


def test_fastf1_success_writes_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    data = RaceData(
        2023,
        5,
        laps=pd.DataFrame({"Driver": ["VER"], "LapNumber": [1.0]}),
        car_data=pd.DataFrame({"DriverNumber": ["1"], "Speed": [300.0]}),
        pos_data=pd.DataFrame({"DriverNumber": ["1"], "X": [1.0]}),
        session_info={"event_name": "Miami Grand Prix"},
    )
    seen: dict[str, Any] = {}

    def loader(season: int, round_: int, settings: Settings, *, offline: bool) -> RaceData:
        seen.update(season=season, round=round_, offline=offline)
        return data

    monkeypatch.setattr(cli.fastf1_loader, "load_race", loader)
    code = cli.main(
        ["race", "--season", "2023", "--round", "5", "--sources", "fastf1", "--offline"]
    )
    assert code == 0 and seen == {"season": 2023, "round": 5, "offline": True}
    part = Path(str(PART).format(source="fastf1"))
    assert pd.read_parquet(part / "car_data.parquet")["Speed"].tolist() == [300.0]
    assert json.loads((part / "session.json").read_text())["event_name"] == "Miami Grand Prix"


def test_one_failing_source_does_not_block_the_other(
    fixture_client: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*_a: Any, **_k: Any) -> RaceData:
        raise FastF1AuthError("no auth")

    monkeypatch.setattr(cli.fastf1_loader, "load_race", fail)
    code = cli.main(["race", "--season", "2023", "--round", "5"])
    assert code == 1
    assert (Path(str(PART).format(source="jolpica")) / "_manifest.json").exists()
    assert not Path(str(PART).format(source="fastf1")).exists()
