"""Tyre degradation model (``sim/tyre_model.py``): fit, form choice, fallbacks, leakage, I/O."""

from __future__ import annotations

import json
import math
import os
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from _sim_fixtures import RaceSpec, lake_readers, linear_truth, race_rows, write_parquet_lake

from f1sa.api.catalog import sim_readers
from f1sa.config import Settings
from f1sa.sim import tyre_model as T

SPECS = [RaceSpec(2023, r) for r in range(1, 5)]
REAL = [RaceSpec(2023, r, synthetic=False) for r in range(1, 5)]


def _artefact(specs: list[RaceSpec], **kw: object) -> T.TyreModelArtefact:
    clean, features = lake_readers(specs, **kw)  # type: ignore[arg-type]
    return T.build_artefact(
        clean, features, source="sample", lake_scheme="file", now=datetime(2026, 1, 1, tzinfo=UTC)
    )


# ------------------------------------------------------------------ green laps
def test_green_laps_filter_and_tyre_age() -> None:
    rows = race_rows(RaceSpec(2023, 1, drivers=2, noise_sd=0.0))
    g = T.green_laps(2023, 1, "circ", rows["laps"], rows["stints"])
    laps0 = {x.lap for x in g if x.stint_key[1] == "drv0"}
    # lap 1, in-lap 18, out-lap 19 and neutralised lap 30 are never green
    assert {1, 18, 19, 30}.isdisjoint(laps0) and 2 in laps0 and 40 in laps0
    first = next(x for x in g if x.stint_key[1] == "drv1" and x.lap == 5)
    assert first.age == 2 + 5 and first.compound == "MEDIUM"  # tyre_age_at_start = 2
    after = next(x for x in g if x.stint_key[1] == "drv1" and x.lap == 25)
    assert after.age == 25 - 19 and after.compound == "HARD"  # in-lap 19 for drv1


def test_green_laps_trims_incidents_and_needs_five_laps() -> None:
    laps = [
        dict(driver_id="a", lap=n, lap_time_s=90.0, is_pit_in_lap=False, is_pit_out_lap=False)
        for n in range(2, 9)
    ]
    laps[3]["lap_time_s"] = 90.0 * 1.08  # above 1.07 x the median
    laps += [dict(driver_id="b", lap=n, lap_time_s=90.0) for n in range(2, 6)]  # 4 green laps
    stints = [dict(driver_id=d, stint=1, start_lap=1, end_lap=10, compound="soft") for d in "ab"]
    g = T.green_laps(2023, 1, None, laps, stints)
    assert {x.stint_key[1] for x in g} == {"a"} and len(g) == 6
    assert all(x.compound == "SOFT" and x.circuit_id is None for x in g)


def test_laps_without_a_stint_are_skipped() -> None:
    laps = [dict(driver_id="a", lap=n, lap_time_s=90.0) for n in range(2, 9)]
    assert T.green_laps(2023, 1, "c", laps, []) == []


# ------------------------------------------------------------------ fitting
def _fit_rows(
    truth: T.DegFit, races: int = 4, seed: int = 1
) -> tuple[list[T._Stats], list[T._FuelStats]]:
    import numpy as np

    rng = np.random.default_rng(seed)
    out, fuel = [], []
    for r in range(races):
        laps = []
        for stint in range(4):
            start = 2 + 9 * stint + r
            for age in range(1, 15):
                lap = start + age
                y = float(truth.predict(age, lap)) + float(rng.normal(0.0, 0.1))
                laps.append(T.GreenLap(r, "c", (r, "d", stint), "MEDIUM", age, lap, y))
        st, fs = T._Stats(), T._FuelStats()
        st.add(laps)
        fs.add(laps)
        out.append(st)
        fuel.append(fs)
    return out, fuel


def test_linear_truth_is_recovered_and_kept_linear() -> None:
    truth = T.DegFit(source="global", form="linear", alpha=0.7, beta=0.06, phi=-0.04)
    stats, fuel = _fit_rows(truth)
    phi = T.fuel_trend(fuel)
    assert phi == pytest.approx(-0.04, abs=0.01)
    fit = T.fit_level(stats, "circuit_compound", phi)
    assert fit is not None and fit.source == "circuit_compound" and fit.form == "linear"
    assert (fit.alpha, fit.beta, fit.phi) == pytest.approx((0.7, 0.06, -0.04), abs=0.03)
    assert fit.n_races == 4 and fit.n_stints == 16 and fit.n_laps == 4 * 4 * 14
    assert fit.residual_sd_s == pytest.approx(0.1, abs=0.02)
    assert fit.rmse_linear_loro is not None and fit.rmse_quadratic_loro is not None
    assert fit.rmse_linear_loro <= fit.rmse_quadratic_loro


def test_fuel_trend_needs_a_lap_term_that_is_identified() -> None:
    assert T.fuel_trend([]) is None
    one_stint = T._FuelStats()
    one_stint.add(
        [T.GreenLap(1, "c", (1, "d", 1), "SOFT", a, a + 1, 0.1 * a) for a in range(1, 20)]
    )
    assert T.fuel_trend([one_stint]) is None  # age and lap collinear: not identified


def test_end_to_end_circuit_fit_and_compound_offset() -> None:
    art = _artefact(SPECS)
    fit = art.races[(2023, 4)].fit_for("MEDIUM")
    assert fit.source == "circuit_compound" and fit.n_races == 3
    hard = art.races[(2023, 4)].fit_for("HARD")
    assert hard.source == "circuit_compound"
    # HARD laps are 0.4 s slower at equal age and lap in the generated races
    assert float(hard.predict(10, 30) - fit.predict(10, 30)) == pytest.approx(0.4, abs=0.25)


def test_quadratic_only_when_it_wins_out_of_sample() -> None:
    def quad(age: int, lap: int, compound: str) -> float:
        return 0.02 * age + 0.004 * age * age - 0.04 * lap

    clean, features = lake_readers(SPECS, truth=quad)
    art = T.build_artefact(clean, features, source="sample", lake_scheme="file")
    fit = art.races[(2023, 4)].fit_for("MEDIUM")
    assert fit.form == "quadratic"
    assert fit.rmse_quadratic_loro is not None and fit.rmse_linear_loro is not None
    assert fit.rmse_quadratic_loro < fit.rmse_linear_loro
    assert fit.gamma == pytest.approx(0.004, abs=0.001)


def test_one_earlier_race_is_always_linear() -> None:
    def quad(age: int, lap: int, compound: str) -> float:
        return 0.004 * age * age

    clean, features = lake_readers(SPECS[:2], truth=quad)
    art = T.build_artefact(clean, features, source="sample", lake_scheme="file")
    fit = art.races[(2023, 2)].fit_for("MEDIUM")
    assert fit.form == "linear" and fit.rmse_quadratic_loro is None


def test_fallbacks_circuit_then_compound_then_global_then_none() -> None:
    specs = [
        RaceSpec(2023, 1, circuit="a", compounds=("MEDIUM", "HARD")),
        RaceSpec(2023, 2, circuit="b", compounds=("MEDIUM", "HARD")),
        RaceSpec(2023, 3, circuit="c", compounds=("SOFT", "MEDIUM")),
    ]
    art = _artefact(specs)
    first = art.races[(2023, 1)]
    assert all(f.source == "none" and f.form == "none" for f in first.fits.values())
    m = art.races[(2023, 3)]
    assert m.fit_for("MEDIUM").source == "compound"  # circuit c has no history
    assert m.fit_for("SOFT").source == "global"  # no earlier soft laps at all
    assert m.fit_for("WET").source == "global"
    assert m.fit_for(None).source == "global" and m.fit_for("bogus").source == "global"
    again = _artefact([*specs, RaceSpec(2023, 4, circuit="a")]).races[(2023, 4)]
    assert again.fit_for("MEDIUM").source == "circuit_compound"


def test_thin_levels_fall_back() -> None:
    specs = [RaceSpec(2023, 1, drivers=1, laps=12, stop_lap=6), RaceSpec(2023, 2)]
    art = _artefact(specs)
    assert art.races[(2023, 2)].fit_for("MEDIUM").source == "none"  # 1 stint, < 30 laps


def test_target_race_and_later_races_never_change_the_fit() -> None:
    """x100 on the target race and every later race: the target's entry is unchanged."""
    base = _artefact(SPECS).races[(2023, 3)]
    scaled = [SPECS[0], SPECS[1], RaceSpec(2023, 3, scale=100.0), RaceSpec(2023, 4, scale=100.0)]
    other = _artefact(scaled).races[(2023, 3)]
    assert other == base
    changed = _artefact([RaceSpec(2023, 1, scale=1.05), *SPECS[1:]]).races[(2023, 3)]
    assert changed != base  # an earlier race does feed the fit


def test_stop_noise_is_a_robust_sd_of_earlier_races() -> None:
    art = _artefact(SPECS)
    assert art.races[(2023, 1)].stop_delta_sd_s is None  # no earlier race
    m = art.races[(2023, 2)]
    deltas = [0.3, -0.2, 0.1, 9.0, -0.4, 0.0]
    assert m.stop_delta_sd_s == pytest.approx(T.robust_sd(deltas))
    assert m.stop_delta_sd_s < 1.0  # the 9.0 s outlier does not blow it up
    assert m.stop_noise_sd_s == pytest.approx(
        math.hypot(m.stop_delta_sd_s, m.outlap_residual_sd_s or 0)
    )


def test_robust_sd() -> None:
    assert T.robust_sd([1.0]) is None
    assert T.robust_sd([1.0, 1.0, 1.0]) == 0.0
    assert T.robust_sd([-1.0, 1.0]) == pytest.approx(1.4826)


def test_full_source_uses_tracinginsights_compounds() -> None:
    clean, features = lake_readers(REAL, ti=True)
    art = T.build_artefact(clean, features, source="full", lake_scheme="hdfs")
    assert art.races[(2023, 4)].fit_for("MEDIUM").source == "circuit_compound"
    assert not art.synthetic
    assert ("stints_tracinginsights", 2023, 1) in clean.reads


def test_full_source_without_ti_stints_has_no_compound() -> None:
    clean, features = lake_readers(REAL, ti=True)
    clean.tables["stints_tracinginsights"].clear()
    art = T.build_artefact(clean, features, source="full", lake_scheme="hdfs")
    m = art.races[(2023, 4)]
    assert m.fit_for("MEDIUM").source == "global"  # inferred stints carry no compound


def test_empty_lake_is_an_error() -> None:
    clean, features = lake_readers([])
    with pytest.raises(T.TyreModelError, match="no race"):
        T.build_artefact(clean, features, source="sample", lake_scheme="file")


# ------------------------------------------------------------------ I/O
def test_json_round_trip_and_version_check() -> None:
    art = _artefact(SPECS)
    back = T.TyreModelArtefact.from_json(art.to_json())
    assert back == art
    doc = json.loads(art.to_json())
    doc["version"] = 99
    with pytest.raises(T.TyreModelError, match="version"):
        T.TyreModelArtefact.from_json(json.dumps(doc))
    with pytest.raises(T.TyreModelError, match="malformed"):
        T.TyreModelArtefact.from_json(json.dumps({"version": 1}))
    with pytest.raises(T.TyreModelError, match="not readable"):
        T.TyreModelArtefact.load(Path("/nonexistent/x.json"))


def test_write_only_into_models_sim(tmp_path: Path) -> None:
    art = _artefact(SPECS[:2])
    out = T.write_artefact(art, T.default_output("sample", tmp_path), root=tmp_path)
    assert out == (tmp_path / "models" / "sim" / "tyre_model_sample.json").resolve()
    assert stat.S_IMODE(os.stat(out).st_mode) == 0o644
    assert T.TyreModelArtefact.load(out) == art
    assert [p.name for p in out.parent.iterdir()] == ["tyre_model_sample.json"]
    for bad in (
        tmp_path / "x.json",
        tmp_path / "models" / "sim" / "x.txt",
        tmp_path / "models" / "x.json",
    ):
        with pytest.raises(T.TyreModelError, match="refusing"):
            T.write_artefact(art, bad, root=tmp_path)


def test_parquet_lake_hides_2025_unless_allowed(tmp_path: Path) -> None:
    specs = [RaceSpec(2024, 1), RaceSpec(2024, 2), RaceSpec(2025, 1)]
    write_parquet_lake(tmp_path / "lake", *lake_readers(specs))
    settings = Settings(data_lake_uri=f"file://{tmp_path / 'lake'}")
    sealed = sim_readers(settings, allow_test_season=False)
    art = T.build_artefact(sealed.clean, sealed.features, source="sample", lake_scheme="file")
    assert sorted(art.races) == [(2024, 1), (2024, 2)] and art.seasons == (2024,)
    opened = sim_readers(settings, allow_test_season=True)
    art25 = T.build_artefact(opened.clean, opened.features, source="sample", lake_scheme="file")
    assert (2025, 1) in art25.races and art25.races[(2024, 2)] == art.races[(2024, 2)]


def test_cli_writes_the_sample_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_parquet_lake(tmp_path / "lake", *lake_readers(SPECS))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{tmp_path / 'lake'}")
    monkeypatch.setenv("F1SA_GIT_SHA", "abc")
    monkeypatch.setenv("F1SA_GIT_DIRTY", "false")
    from f1sa.config import get_settings

    get_settings.cache_clear()
    assert T.main(["--source", "sample"]) == 0
    art = T.TyreModelArtefact.load(tmp_path / "models" / "sim" / "tyre_model_sample.json")
    assert art.git_sha == "abc" and art.git_dirty is False and art.synthetic
    assert T.main(["--source", "sample", "--out", str(tmp_path / "elsewhere.json")]) == 1
    monkeypatch.setenv("DATA_LAKE_URI", "hdfs://namenode:8020/f1")
    get_settings.cache_clear()
    assert T.main(["--source", "sample"]) == 2


def test_sample_source_refuses_real_stints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_parquet_lake(tmp_path / "lake", *lake_readers([RaceSpec(2023, 1, synthetic=False)]))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{tmp_path / 'lake'}")
    from f1sa.config import get_settings

    get_settings.cache_clear()
    assert T.main(["--source", "sample"]) == 1
    assert not (tmp_path / "models" / "sim" / "tyre_model_sample.json").exists()


def test_predict_works_on_arrays() -> None:
    import numpy as np

    f = T.DegFit(source="global", form="quadratic", alpha=1.0, beta=0.1, gamma=0.01, phi=-0.05)
    assert f.predict(10, 20) == pytest.approx(1.0 + 1.0 + 1.0 - 1.0)
    assert list(f.predict(np.array([0, 10]), np.array([0, 20]))) == pytest.approx([1.0, 2.0])
    assert T.NO_FIT.predict(5, 5) == 0.0


def test_no_spark_import() -> None:
    code = (
        "import sys, f1sa.sim.tyre_model, f1sa.sim.race_sim, f1sa.sim.whatif; "
        "print('pyspark' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


def test_linear_truth_fixture_is_what_the_tests_assume() -> None:
    assert linear_truth(10, 20, "MEDIUM") == pytest.approx(0.4 + 0.6 - 0.8)


def test_conflicting_tracinginsights_compounds_are_not_used() -> None:
    clean, features = lake_readers(REAL, ti=True)
    for rows in clean.tables["stints_tracinginsights"].values():
        for r in rows:
            r["compound_conflict"] = True
    art = T.build_artefact(clean, features, source="full", lake_scheme="hdfs")
    assert art.races[(2023, 4)].fit_for("MEDIUM").source == "global"
