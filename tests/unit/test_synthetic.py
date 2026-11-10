"""Tests for the SYNTHETIC tyre/telemetry generator (determinism, schema, edge cases)."""

from __future__ import annotations

import ast
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from f1sa.ingest import synthetic as sy
from f1sa.ingest.jolpica_client import LAPS_SCHEMA, PITSTOPS_SCHEMA, parse_laps, parse_pitstops

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "jolpica"


def laps_frame(
    spec: dict[str, int], season: int = 2023, round_: int = 5, base: float = 90.0
) -> pd.DataFrame:
    """Laps for each driver: driver_id -> number of laps (1..n), lap time = base + small offset."""
    rows = []
    for d, n in spec.items():
        for lap in range(1, n + 1):
            rows.append(
                {
                    "season": season,
                    "round": round_,
                    "lap": lap,
                    "driver_id": d,
                    "position": 1,
                    "time_s": base + (lap % 3) * 0.2,
                }
            )
    return pd.DataFrame(rows, columns=list(LAPS_SCHEMA)).astype(LAPS_SCHEMA)


def stops_frame(stops: list[tuple[str, int]], season: int = 2023, round_: int = 5) -> pd.DataFrame:
    """Pit stops (driver_id, lap); stop numbers count per driver, as in Jolpica."""
    counts: dict[str, int] = {}
    rows = []
    for d, lap in stops:
        counts[d] = counts.get(d, 0) + 1
        rows.append(
            {
                "season": season,
                "round": round_,
                "driver_id": d,
                "stop": counts[d],
                "lap": lap,
                "time_of_day": "15:00:00",
                "duration_s": 22.0,
            }
        )
    return pd.DataFrame(rows, columns=list(PITSTOPS_SCHEMA)).astype(PITSTOPS_SCHEMA)


def parquet_sha(df: pd.DataFrame) -> str:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return hashlib.sha256(buf.getvalue()).hexdigest()


def stints_of(df: pd.DataFrame, driver: str) -> list[tuple[int, int]]:
    d = df[df["driver_id"] == driver].sort_values("stint")
    return list(zip(d["start_lap"].tolist(), d["end_lap"].tolist(), strict=True))


# ---------------------------------------------------------------- stint inference
def test_stints_basic_cases() -> None:
    laps = laps_frame({"a": 50, "b": 50, "c": 50, "dnf": 20})
    stops = stops_frame([("b", 20), ("c", 10), ("c", 25), ("c", 40), ("dnf", 12)])
    st = sy.infer_stints(laps, stops)
    assert {c: str(t) for c, t in st.dtypes.items()} == sy.STINTS_SCHEMA
    assert stints_of(st, "a") == [(1, 50)]  # no stops: one stint
    assert stints_of(st, "b") == [(1, 20), (21, 50)]
    assert stints_of(st, "c") == [(1, 10), (11, 25), (26, 40), (41, 50)]
    assert stints_of(st, "dnf") == [(1, 12), (13, 20)]  # DNF: race ends at last timed lap
    assert (st["laps"] == st["end_lap"] - st["start_lap"] + 1).all()


def test_stop_on_last_lap_duplicates_unsorted_and_dns() -> None:
    laps = laps_frame({"a": 30, "b": 30}).sample(frac=1.0, random_state=1)  # unsorted
    stops = stops_frame([("a", 30), ("a", 31), ("b", 15), ("b", 15), ("ghost", 5)])
    st = sy.infer_stints(laps, stops)
    assert stints_of(st, "a") == [(1, 30)]  # stopping on/after the last lap starts no stint
    assert stints_of(st, "b") == [(1, 15), (16, 30)]  # duplicate stop collapsed
    assert "ghost" not in set(st["driver_id"])  # DNS / no laps: no stints


def test_missing_columns_raise() -> None:
    with pytest.raises(ValueError, match="missing columns"):
        sy.infer_stints(pd.DataFrame({"lap": [1]}), stops_frame([]))


# ---------------------------------------------------------------- tyres
def test_tyre_stints_schema_flags_and_rules() -> None:
    laps = laps_frame({f"d{i}": 50 for i in range(30)})
    stops = stops_frame([(f"d{i}", 15 + i % 10) for i in range(30)] + [("d0", 35), ("d1", 40)])
    tyres = sy.synth_tyre_stints(sy.infer_stints(laps, stops), seed=7)
    assert {c: str(t) for c, t in tyres.dtypes.items()} == sy.TYRE_STINTS_SCHEMA
    assert tyres["is_synthetic"].all() and (tyres["seed"] == 7).all()
    assert (tyres["generator_version"] == sy.GENERATOR_VERSION).all()
    assert set(tyres["compound"]) <= set(sy.DRY_COMPOUNDS)
    per_driver = tyres.groupby("driver_id")["compound"].nunique()
    assert (per_driver >= 2).all()  # everyone stopped -> at least two compounds
    assert tyres["tyre_age_at_start"].between(0, 3).all()


def test_tyre_compound_distribution_is_plausible() -> None:
    laps = laps_frame({f"d{i}": 57 for i in range(400)})
    stops = stops_frame([(f"d{i}", 20) for i in range(400)])
    tyres = sy.synth_tyre_stints(sy.infer_stints(laps, stops), seed=1)
    first = tyres[tyres["stint"] == 1]["compound"].value_counts(normalize=True)
    assert first["MEDIUM"] == pytest.approx(0.55, abs=0.08)  # 20/57 laps: base start mix
    second = tyres[tyres["stint"] == 2]["compound"].value_counts(normalize=True)
    assert second.get("HARD", 0) > second.get("SOFT", 0)  # long final stint favours HARD


# ---------------------------------------------------------------- telemetry
def test_telemetry_schema_bounds_and_one_row_per_lap() -> None:
    laps = laps_frame({"a": 40, "b": 40})
    tyres = sy.synth_tyre_stints(sy.infer_stints(laps, stops_frame([("a", 20)])), seed=3)
    tel = sy.synth_lap_telemetry(laps, tyres, seed=3)
    assert {c: str(t) for c, t in tel.dtypes.items()} == sy.LAP_TELEMETRY_SCHEMA
    assert len(tel) == len(laps) and not tel.duplicated(["driver_id", "lap"]).any()
    assert tel["is_synthetic"].all()
    assert (
        tel["pct_full_throttle"].between(0, 1).all() and tel["brake_fraction"].between(0, 1).all()
    )
    assert (tel["pct_full_throttle"] + tel["brake_fraction"] <= 1.0 + 1e-9).all()
    assert (tel["max_speed_kph"] >= tel["mean_speed_kph"] + 20 - 1e-9).all()
    assert tel["mean_speed_kph"].between(60, 260).all() and tel["max_speed_kph"].le(360).all()
    assert (tel["gear_changes"] >= 0).all() and tel["mean_rpm"].between(7000, 13500).all()


def test_slow_laps_get_lower_speed_and_missing_time_gives_na() -> None:
    laps = laps_frame({"a": 30})
    laps.loc[laps["lap"] == 10, "time_s"] = 140.0  # safety-car lap
    laps.loc[laps["lap"] == 20, "time_s"] = pd.NA  # missing timing
    tyres = sy.synth_tyre_stints(sy.infer_stints(laps, stops_frame([])), seed=5)
    tel = sy.synth_lap_telemetry(laps, tyres, seed=5).set_index("lap")
    normal = tel.drop(index=[10, 20])
    assert tel.loc[10, "mean_speed_kph"] < normal["mean_speed_kph"].min()
    assert tel.loc[10, "pct_full_throttle"] < normal["pct_full_throttle"].min()
    assert tel.loc[20, ["mean_speed_kph", "gear_changes", "mean_rpm"]].isna().all()
    assert bool(tel.loc[20, "is_synthetic"]) is True  # row kept, still flagged


def test_empty_input_gives_empty_typed_frames() -> None:
    empty_laps = laps_frame({})
    out = sy.generate(empty_laps, stops_frame([]), seed=1)
    assert out["tyre_stints"].empty and out["lap_telemetry"].empty
    assert {c: str(t) for c, t in out["lap_telemetry"].dtypes.items()} == sy.LAP_TELEMETRY_SCHEMA


# ---------------------------------------------------------------- determinism
def _fixture_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    pages = [json.loads((FIXTURES / f"laps_2023_5_p{i}.json").read_text()) for i in range(3)]
    pits = [json.loads((FIXTURES / "pitstops_2023_5.json").read_text())]
    return parse_laps(pages), parse_pitstops(pits)


def test_same_seed_same_bytes_different_seed_differs() -> None:
    laps, pits = _fixture_inputs()
    a, b, c = sy.generate(laps, pits, 42), sy.generate(laps, pits, 42), sy.generate(laps, pits, 43)
    for name in ("tyre_stints", "lap_telemetry"):
        pd.testing.assert_frame_equal(a[name], b[name])
        assert parquet_sha(a[name]) == parquet_sha(b[name])
    assert not a["lap_telemetry"]["mean_speed_kph"].equals(c["lap_telemetry"]["mean_speed_kph"])


def test_driver_rows_independent_of_order_and_other_drivers() -> None:
    laps = laps_frame({"a": 40, "b": 40, "c": 40})
    stops = stops_frame([("a", 15), ("b", 20), ("c", 25)])
    full = sy.generate(laps, stops, 9)
    shuffled = sy.generate(laps.sample(frac=1.0, random_state=3), stops.iloc[::-1], 9)
    only_b = sy.generate(laps[laps["driver_id"] == "b"], stops[stops["driver_id"] == "b"], 9)
    for name in ("tyre_stints", "lap_telemetry"):
        pd.testing.assert_frame_equal(full[name], shuffled[name])
        b_full = full[name][full[name]["driver_id"] == "b"].reset_index(drop=True)
        pd.testing.assert_frame_equal(b_full, only_b[name])


def test_compound_probabilities_are_distributions() -> None:
    for stint in (1, 2, 3):
        for rel in (0.1, 0.3, 0.5):
            p = sy._compound_probs(stint, rel)
            assert p.shape == (3,) and np.isclose(p.sum(), 1.0) and (p > 0).all()


def test_generator_module_does_not_use_fastf1() -> None:
    tree = ast.parse(Path(sy.__file__).read_text())
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        for alias in (
            node.names if isinstance(node, ast.Import) else [ast.alias(node.module or "")]
        )
    }
    assert "fastf1" not in imported


def test_forced_second_compound_respects_stint_length() -> None:
    """When a driver would run one compound all race, the switched long stint favours HARD."""
    laps = laps_frame({f"d{i}": 56 for i in range(600)})
    stops = stops_frame([x for i in range(600) for x in ((f"d{i}", 13), (f"d{i}", 23))])
    tyres = sy.synth_tyre_stints(sy.infer_stints(laps, stops), seed=11)
    long_final = tyres[tyres["stint"] == 3]  # 33 of 56 laps
    shares = long_final["compound"].value_counts(normalize=True)
    assert shares.get("SOFT", 0.0) < 0.15 and shares.get("HARD", 0.0) > 0.45
