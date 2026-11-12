"""Feature set ``real_ti``: TracingInsights tyre and telemetry inputs at the decision lap.

Coverage policy under test: every null is 0.0 plus ``_missing`` (a categorical's ``unknown``
level), ``partial`` telemetry keeps its values plus a flag, telemetry is read only through the
Jolpica-keyed left join, and no race or attempt is dropped or down-weighted for low coverage.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from _model_fixtures import frame, rehearsal_rows
from pyspark.sql import DataFrame, DataFrameReader, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from f1sa.lake import schemas as S
from f1sa.lake import schemas_tracinginsights as TS
from f1sa.model import dataset as D
from f1sa.model import features as MF

_TEL = {
    "mean_speed_kph": 200.0,
    "max_speed_kph": 320.0,
    "pct_full_throttle": 0.6,
    "brake_fraction": 0.2,
    "gear_changes": 40,
    "mean_rpm": 11000.0,
}


def _df(spark: SparkSession, schema: StructType, rows: list[dict[str, Any]]) -> DataFrame:
    return spark.createDataFrame(
        [tuple(r.get(f.name) for f in schema.fields) for r in rows], schema
    )


def _laps(spark: SparkSession, keys: list[tuple[int, int, str, int]]) -> DataFrame:
    rows = [{"season": s, "round": r, "driver_id": d, "lap": lap} for s, r, d, lap in keys]
    return _df(spark, S.LAPS, rows)


def _tel(spark: SparkSession, rows: list[tuple[int, int, str, int, str]]) -> DataFrame:
    out = []
    for s, r, d, lap, cov in rows:
        vals = {} if cov == "none" else _TEL
        out.append(
            {
                "season": s,
                "round": r,
                "driver_id": d,
                "lap": lap,
                **vals,
                "n_samples": 0 if cov == "none" else 100,
                "telemetry_coverage": cov,
            }
        )
    return _df(spark, TS.TELEMETRY_LAP_REAL, out)


def _stints(
    spark: SparkSession, rows: list[tuple[int, int, str, int, int, str | None, int | None]]
) -> DataFrame:
    return _df(
        spark,
        TS.STINTS_TRACINGINSIGHTS,
        [
            {
                "season": s,
                "round": r,
                "driver_id": d,
                "stint": i,
                "start_lap": a,
                "end_lap": b,
                "compound": c,
                "tyre_age_at_start": age,
            }
            for i, (s, r, d, a, b, c, age) in enumerate(rows, start=1)
        ],
    )


def _lap_compounds_from_stints(spark: SparkSession, stints: DataFrame) -> DataFrame:
    """Every lap start_lap..end_lap of a stint carries the stint's compound label."""
    rows = [
        (r["season"], r["round"], r["driver_id"], lap, r["compound"])
        for r in stints.collect()
        if r["start_lap"] is not None and r["end_lap"] is not None
        for lap in range(r["start_lap"], r["end_lap"] + 1)
    ]
    return spark.createDataFrame(
        rows, "season int, round int, driver_id string, lap int, compound string"
    )


def _ti(
    spark: SparkSession,
    laps: DataFrame,
    stints: DataFrame,
    tel: DataFrame,
    lap_compounds: DataFrame | None = None,
) -> D.TiInputs:
    lc = lap_compounds if lap_compounds is not None else _lap_compounds_from_stints(spark, stints)
    return D.TiInputs(laps, stints, tel, lc)


def _attempts(spark: SparkSession, rows: list[tuple[int, int, str, str, int]]) -> DataFrame:
    return spark.createDataFrame(
        rows, "season int, round int, attacker_id string, defender_id string, decision_lap int"
    )


def _one(df: DataFrame) -> dict[str, Any]:
    (row,) = df.collect()
    return row.asDict()


# ============================================================================== registry
def test_real_and_with_synthetic_columns_are_unchanged_and_real_ti_appends() -> None:
    real = MF.assembled_columns("real")
    assert len(real) == 52 and len(MF.assembled_columns("with_synthetic")) == 94
    ti = MF.assembled_columns("real_ti")
    assert ti[: len(real)] == real
    extra = ti[len(real) :]
    assert len(extra) == 46 and all(c.startswith("f_") and "_ti" in c for c in extra)
    assert {MF.base_feature_of(c) for c in extra} == set(MF.ti_feature_names())
    assert "f_attacker_compound_ti__unknown" in extra
    assert "f_defender_telemetry_partial_ti_missing" in extra
    assert MF.uses_tracinginsights("real_ti") and not MF.uses_tracinginsights("real")
    assert MF.required_flags("real_ti") == ()
    MF.check_registry()


# ============================================================================== inputs
def test_tyre_inputs_at_the_decision_lap(spark: SparkSession) -> None:
    att = _attempts(spark, [(2024, 1, "a", "d", 10)])
    ti = _ti(
        spark,
        _laps(spark, [(2024, 1, "a", 10), (2024, 1, "d", 10)]),
        _stints(
            spark,
            [
                (2024, 1, "a", 1, 9, "SOFT", 0),
                (2024, 1, "a", 10, 30, "HARD", 2),  # L-1 = 10 is the stint's first lap
                (2024, 1, "d", 1, 20, "MEDIUM", None),  # tyre life unknown: never assumed new
            ],
        ),
        _tel(spark, []),
    )
    r = _one(D.with_ti_inputs(att, ti))
    assert r["attacker_compound_ti"] == "HARD" and r["attacker_tyre_age_laps_ti"] == 3
    assert r["defender_compound_ti"] == "MEDIUM" and r["defender_tyre_age_laps_ti"] is None
    assert r["tyre_age_delta_laps_ti"] is None


def test_telemetry_full_partial_none_and_absent(spark: SparkSession) -> None:
    att = _attempts(spark, [(2024, 1, "a", "d", 10), (2024, 1, "b", "c", 10)])
    laps = _laps(spark, [(2024, 1, x, 10) for x in "adbc"])
    tel = _tel(
        spark,
        [(2024, 1, "a", 10, "full"), (2024, 1, "d", 10, "partial"), (2024, 1, "b", 10, "none")],
    )
    out = D.with_ti_inputs(att, _ti(spark, laps, _stints(spark, []), tel))
    rows = {r["attacker_id"]: r.asDict() for r in out.collect()}
    ad, bc = rows["a"], rows["b"]
    assert ad["attacker_mean_speed_kph_ti"] == 200.0 and ad["attacker_telemetry_partial_ti"] == 0.0
    assert ad["defender_mean_speed_kph_ti"] == 200.0 and ad["defender_telemetry_partial_ti"] == 1.0
    assert bc["attacker_mean_speed_kph_ti"] is None and bc["attacker_telemetry_partial_ti"] == 0.0
    assert bc["defender_mean_speed_kph_ti"] is None and bc["defender_telemetry_partial_ti"] is None

    assembled = out.select("attacker_id", *D.feature_exprs("real_ti")[-46:])
    got = {r["attacker_id"]: r.asDict() for r in assembled.collect()}
    b = got["b"]
    # none lap: 0.0 + missing for the aggregates, partial flag known (0.0)
    assert (
        b["f_attacker_mean_speed_kph_ti"] == 0.0
        and b["f_attacker_mean_speed_kph_ti_missing"] == 1.0
    )
    assert b["f_attacker_telemetry_partial_ti_missing"] == 0.0
    # no TracingInsights row: everything 0.0 + missing
    assert b["f_defender_telemetry_partial_ti"] == 0.0
    assert b["f_defender_telemetry_partial_ti_missing"] == 1.0
    assert b["f_defender_gear_changes_ti_missing"] == 1.0
    # no stint: all one-hot levels 0, unknown (= missing) 1
    assert b["f_attacker_compound_ti__unknown"] == 1.0
    assert all(b[f"f_attacker_compound_ti__{c.lower()}"] == 0.0 for c in MF.COMPOUNDS)
    assert got["a"]["f_defender_telemetry_partial_ti"] == 1.0
    assert got["a"]["f_defender_mean_speed_kph_ti"] == 200.0


def test_telemetry_only_on_jolpica_laps(spark: SparkSession) -> None:
    """A TracingInsights lap without a Jolpica lap never reaches the model."""
    att = _attempts(spark, [(2024, 1, "a", "d", 10)])
    tel = _tel(spark, [(2024, 1, "a", 10, "full"), (2024, 1, "d", 10, "full")])
    r = _one(
        D.with_ti_inputs(
            att, _ti(spark, _laps(spark, [(2024, 1, "a", 10)]), _stints(spark, []), tel)
        )
    )
    assert r["attacker_mean_speed_kph_ti"] == 200.0 and r["defender_mean_speed_kph_ti"] is None


def test_fanout_guards(spark: SparkSession) -> None:
    att = _attempts(spark, [(2024, 1, "a", "d", 10)])
    laps = _laps(spark, [(2024, 1, "a", 10), (2024, 1, "d", 10)])
    dup_tel = _tel(spark, [(2024, 1, "a", 10, "full"), (2024, 1, "a", 10, "partial")])
    with pytest.raises(D.DatasetError, match="fans out the Jolpica laps: 2 -> 3"):
        D.with_ti_inputs(att, _ti(spark, laps, _stints(spark, []), dup_tel))
    overlap = _stints(spark, [(2024, 1, "a", 1, 10, "SOFT", 0), (2024, 1, "a", 10, 20, "HARD", 0)])
    with pytest.raises(D.DatasetError, match="changed the attempt count: 1 -> 2"):
        D.with_ti_inputs(att, _ti(spark, laps, overlap, _tel(spark, [])))


# ============================================================================== no drop
def _ti_for(spark: SparkSession, base: DataFrame) -> D.TiInputs:
    """TracingInsights inputs for a rehearsal frame: round 1 of every season has no compound at
    all (0% coverage) and no telemetry; other races have full data, odd attackers partial."""
    keys = (
        base.select("season", "round", F.col("attacker_id").alias("d"), "decision_lap")
        .union(base.select("season", "round", F.col("defender_id").alias("d"), "decision_lap"))
        .distinct()
        .collect()
    )
    laps = [(r["season"], r["round"], r["d"], r["decision_lap"]) for r in keys]
    tel = [
        (s, rnd, d, lap, "partial" if d.endswith(("1", "3")) else "full")
        for s, rnd, d, lap in laps
        if rnd != 1
    ]
    stints = [(s, rnd, d, 1, lap + 5, None if rnd == 1 else "MEDIUM", 0) for s, rnd, d, lap in laps]
    return _ti(spark, _laps(spark, laps), _stints(spark, stints), _tel(spark, tel))


def test_low_coverage_drops_and_down_weights_nothing(spark: SparkSession) -> None:
    base = frame(spark, rehearsal_rows())
    with_ti = D.with_ti_inputs(base, _ti_for(spark, base))
    assert with_ti.count() == base.count()
    split = D.SplitSpec(test=())
    real = D.build_dataset(base, feature_set="real", split=split)
    ti = D.build_dataset(with_ti, feature_set="real_ti", split=split)
    assert real.counts == ti.counts and real.class_weights == ti.class_weights
    for part in ("train", "validation"):
        cols = [*D.KEY, "label", D.WEIGHT]
        a = sorted(tuple(r) for r in getattr(real, part).select(*cols).collect())
        b = sorted(tuple(r) for r in getattr(ti, part).select(*cols).collect())
        assert a == b, part
    # round 1 (0% compound coverage) is still there, with compound = unknown
    r1 = ti.train.where(F.col("round") == 1)
    assert r1.count() > 0
    assert r1.where(F.col("f_attacker_compound_ti__unknown") != 1.0).isEmpty()
    assert r1.where(F.col("f_attacker_mean_rpm_ti_missing") != 1.0).isEmpty()


def _plan(df: DataFrame) -> str:
    """The logical plan (a cached frame's optimized plan is only ``InMemoryRelation``)."""
    return str(df._jdf.queryExecution().logical().toString())


@pytest.mark.parametrize("feature_set", ["real_ti", "real_tyre"])
def test_ti_sets_have_one_sorted_partition_whatever_the_input_order(
    spark: SparkSession, feature_set: str
) -> None:
    base = frame(spark, rehearsal_rows())
    with_ti = D.with_ti_inputs(base, _ti_for(spark, base))
    shuffled = with_ti.repartition(7, F.rand(seed=3)).orderBy(F.rand(seed=11))
    split = D.SplitSpec(test=())
    a = D.build_dataset(with_ti, feature_set=feature_set, split=split)
    b = D.build_dataset(shuffled, feature_set=feature_set, split=split)
    for part in ("train", "validation"):
        x, y = getattr(a, part), getattr(b, part)
        assert x.rdd.getNumPartitions() == y.rdd.getNumPartitions() == 1, part
        rows = x.collect()
        assert rows == y.collect(), part  # same rows, same order, same values
        keys = [tuple(r[c] for c in D.ORDER_KEY) for r in rows]
        assert keys == sorted(keys) and len(set(keys)) == len(keys), part


def test_real_plan_is_not_reordered(spark: SparkSession) -> None:
    real = D.build_dataset(frame(spark, rehearsal_rows()), split=D.SplitSpec(test=()))
    for part in (real.train, real.validation):
        plan = _plan(part)
        assert "Repartition" not in plan and "Sort " not in plan
    base = frame(spark, rehearsal_rows())
    ti = D.build_dataset(
        D.with_ti_inputs(base, _ti_for(spark, base)),
        feature_set="real_ti",
        split=D.SplitSpec(test=()),
    )
    assert "Repartition" in _plan(ti.train) and "Sort " in _plan(ti.train)  # the check can fail


def test_ti_output_is_cached_so_setting_filters_stay_small(spark: SparkSession) -> None:
    """Regression: a (G, k) filter pushed into the TI joins over partitioned scans made dynamic
    partition pruning nest exponentially (a 32 MB physical plan and an OOM on the sample lake)."""
    base = frame(spark, rehearsal_rows())
    out = D.with_ti_inputs(base, _ti_for(spark, base))
    assert out.is_cached
    sel = out.where(F.col("gap_threshold_s") == F.lit(3.5))
    assert "InMemoryRelation" in str(sel._jdf.queryExecution().optimizedPlan().toString())


def test_canonical_order_rejects_duplicate_keys(spark: SparkSession) -> None:
    base = frame(spark, rehearsal_rows())
    one = base.limit(1)
    with pytest.raises(D.DatasetError, match="not unique"):
        D.canonical_order(one.unionByName(one))
    assert D.canonical_order(one).count() == 1


def test_missing_rates_per_season(spark: SparkSession) -> None:
    base = frame(spark, rehearsal_rows(seasons=(2023, 2024)))
    df = D.with_ti_inputs(base, _ti_for(spark, base))
    rates = D.missing_rates(df, "real_ti", 3.5, 2)
    by = {(r["season"], r["feature"]): r for r in rates}
    labelled = {
        int(r["season"]): int(r["count"])
        for r in D.select_setting(df, 3.5, 2)
        .where(F.col("outcome_category").isin(*D.LABELLED) & F.col("label").isNotNull())
        .groupBy("season")
        .count()
        .collect()
    }
    for season in (2023, 2024):
        n = labelled[season]
        r1 = (
            D.select_setting(df, 3.5, 2)
            .where(F.col("outcome_category").isin(*D.LABELLED) & (F.col("season") == season))
            .where(F.col("round") == 1)
            .count()
        )
        c = by[(season, "attacker_compound_ti")]
        assert (c["n_attempts"], c["n_missing"]) == (n, r1)
        assert c["missing_rate"] == round(r1 / n, 4)
        assert by[(season, "attacker_tyre_age_laps_ti")]["n_missing"] == 0
        assert by[(season, "gap_s")]["provenance"] == MF.JOLPICA
        p = by[(season, "attacker_telemetry_partial_ti")]
        assert p["n_missing"] == r1 and p["n_telemetry_partial"] != ""
        assert by[(season, "attacker_mean_rpm_ti")]["n_telemetry_partial"] == ""
    assert {r["feature"] for r in rates} == {s.name for s in MF.specs_for("real_ti")}
    assert set(D.MISSING_RATE_COLUMNS) == set(rates[0])
    assert not any("label" in k or "outcome" in k for k in rates[0])


def test_missing_rate_extremes(spark: SparkSession) -> None:
    base = frame(spark, rehearsal_rows(seasons=(2024,)))
    empty = _ti(spark, _laps(spark, []), _stints(spark, []), _tel(spark, []))
    rates = D.missing_rates(D.with_ti_inputs(base, empty), "real_ti", 3.5, 2)
    ti_rates = [r for r in rates if r["provenance"] == MF.TRACINGINSIGHTS]
    assert ti_rates and all(r["missing_rate"] == 1.0 for r in ti_rates)
    assert all(r["missing_rate"] == 0.0 for r in rates if r["feature"] == "gap_s")


# ============================================================================== season isolation
def test_read_clean_seasons_never_touches_other_seasons(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = f"file://{tmp_path}/clean"
    tel = _tel(spark, [(2024, 1, "a", 5, "full")])
    tel.write.partitionBy("season", "round").parquet(f"{root}/telemetry_lap_real")
    junk = tmp_path / "clean" / "telemetry_lap_real" / "season=2025" / "round=1"
    junk.mkdir(parents=True)
    (junk / "part-00000-junk.parquet").write_bytes(b"not parquet: the test season")
    seen: list[str] = []
    original = DataFrameReader.parquet

    def spy(self: Any, *p: str, **kw: Any) -> Any:
        seen.extend(str(x) for x in p)
        return original(self, *p, **kw)

    monkeypatch.setattr(DataFrameReader, "parquet", spy)
    df = D.read_clean_seasons(spark, root, "telemetry_lap_real", (2023, 2024))
    assert df.count() == 1 and seen and not [p for p in seen if "2025" in p]
    assert all("/season=" in p for p in seen)
    missing = D.read_clean_seasons(spark, root, "stints_tracinginsights", (2024,))
    assert missing.isEmpty() and missing.columns == TS.STINTS_TRACINGINSIGHTS.fieldNames()
    with pytest.raises(D.DatasetError, match="explicit races"):
        D.load_attempts(spark, root, None, ti_clean_root=root)


# ============================================================================== real_tyre
def test_real_tyre_is_real_plus_tyre_inputs_and_delta() -> None:
    real, tyre = MF.assembled_columns("real"), MF.assembled_columns("real_tyre")
    ti = MF.assembled_columns("real_ti")
    assert tyre[: len(real)] == real and len(tyre) == 70 and len(real) == 52
    names = [s.name for s in MF.specs_for("real_tyre")][-5:]
    assert names == [
        "attacker_compound_ti",
        "defender_compound_ti",
        "attacker_tyre_age_laps_ti",
        "defender_tyre_age_laps_ti",
        "tyre_age_delta_laps_ti",
    ]
    assert tyre[-2:] == ("f_tyre_age_delta_laps_ti", "f_tyre_age_delta_laps_ti_missing")
    assert set(tyre) <= set(ti)
    # real_tyre and real_ti differ only by the telemetry inputs and the partial flag
    extra = {MF.base_feature_of(c) for c in set(ti) - set(tyre)}
    assert extra == {f"{side}_{c}_ti" for side in MF.SIDES for c in MF.TI_TELEMETRY_COLUMNS}
    assert MF.uses_tracinginsights("real_tyre") and "real_tyre" in MF.DEV_ONLY_FEATURE_SETS
    assert len(ti) == 98  # unchanged by real_tyre


# ============================================================================== leakage
_STINTS = {
    "a": ((1, 9, "SOFT", 0), (10, 20, "HARD", 2)),  # L-1 = 10 is the first lap of HARD
    "d": ((1, 15, "MEDIUM", 1), (16, 20, "HARD", 0)),  # L-1 = 10 is mid-stint
}


def _future_label(lap: int) -> str | None:
    """A different compound or a null on same-stint laps after L-1."""
    return "SOFT" if lap % 2 else None


def _leak_world(
    spark: SparkSession, scaled: bool, relabel: bool = False
) -> tuple[DataFrame, D.TiInputs]:
    """Target race 2024 R1 (decision lap L-1 = 10) and a later race 2024 R2.

    ``scaled``: every value after lap 10 in R1 and every value in R2 is multiplied by 100 (lap
    numbers, stint bounds, tyre ages, telemetry); future telemetry turns partial and future stints
    WET. ``relabel`` (also implied by ``scaled``): every same-stint lap after L-1 carries a
    different or null compound label, and the stint row's whole-stint compound becomes null (a
    conflict, as the clean job would aggregate it). Lap numbers are unchanged by ``relabel``."""
    m = 100 if scaled else 1
    relabel = relabel or scaled
    laps, tel, stints, lc = [], [], [], []
    for rnd in (1, 2):
        for d in ("a", "d"):
            for lap in range(1, 21):
                future = rnd == 2 or lap > 10
                k = m if future else 1
                laps.append({"season": 2024, "round": rnd, "driver_id": d, "lap": lap * k})
                tel.append(
                    {
                        "season": 2024,
                        "round": rnd,
                        "driver_id": d,
                        "lap": lap * k,
                        **{c: (v * k) for c, v in _TEL.items()},
                        "n_samples": 100 * k,
                        "telemetry_coverage": "partial"
                        if (future and scaled) or lap == 7
                        else "full",
                    }
                )
        for d, bounds in _STINTS.items():
            for i, (start, end, comp, age) in enumerate(bounds, start=1):
                whole_future = rnd == 2 or start > 10
                spans_l1 = start <= 10 < end
                k = m if whole_future else 1
                stint_comp = "WET" if (whole_future and scaled) else comp
                if relabel and spans_l1:
                    stint_comp = None
                stints.append(
                    {
                        "season": 2024,
                        "round": rnd,
                        "driver_id": d,
                        "stint": i,
                        "start_lap": start * k,
                        "end_lap": end * (m if (end > 10 or rnd == 2) else 1),
                        "compound": stint_comp,
                        "tyre_age_at_start": age * k,
                    }
                )
                for lap in range(start, end + 1):
                    future = rnd == 2 or lap > 10
                    label: str | None = comp
                    if future and relabel:
                        label = "WET" if whole_future else _future_label(lap)
                    lap_k = lap * (m if future else 1)
                    lc.append((2024, rnd, d, lap_k, label))
    ti = D.TiInputs(
        _df(spark, S.LAPS, laps),
        _df(spark, TS.STINTS_TRACINGINSIGHTS, stints),
        _df(spark, TS.TELEMETRY_LAP_REAL, tel),
        spark.createDataFrame(
            lc, "season int, round int, driver_id string, lap int, compound string"
        ),
    )
    att = _attempts(spark, [(2024, 1, "a", "d", 10), (2024, 2, "a", "d", 10)])
    return att, ti


@pytest.mark.parametrize("feature_set", ["real_tyre", "real_ti"])
def test_ti_inputs_unchanged_when_future_laps_and_later_races_are_scaled_x100(
    spark: SparkSession, feature_set: str
) -> None:
    cols = [c for c in MF.assembled_columns(feature_set) if "_ti" in c]
    exprs = D.feature_exprs(feature_set)[-len(cols) :]

    def features(scaled: bool, relabel: bool = False) -> dict[int, dict[str, Any]]:
        att, ti = _leak_world(spark, scaled, relabel)
        out = D.with_ti_inputs(att, ti).select("round", *exprs)
        return {r["round"]: r.asDict() for r in out.collect()}

    base, scaled, relabelled = features(False), features(True), features(False, relabel=True)
    assert list(base[1]) == ["round", *cols]
    assert base[1] == scaled[1]  # target race: decision-time inputs identical
    assert base[2] != scaled[2]  # the scaling is real: the later race's own inputs move
    # same-stint laps after L-1 with a different or null compound (and the whole-stint
    # aggregate nulled by that conflict) leave the decision-time compound unchanged
    assert base[1] == relabelled[1]
    for side, comp in (("attacker", "hard"), ("defender", "medium")):
        assert base[1][f"f_{side}_compound_ti__{comp}"] == 1.0
        assert base[1][f"f_{side}_compound_ti__unknown"] == 0.0
    assert base[1]["f_attacker_tyre_age_laps_ti"] == 3.0


def test_compound_uses_only_laps_up_to_decision_lap(spark: SparkSession) -> None:
    att = _attempts(spark, [(2024, 1, "a", "d", 10), (2024, 1, "b", "c", 10)])
    stints = _stints(
        spark,
        [
            (2024, 1, "a", 5, 20, "SOFT", 0),  # whole-stint aggregate says SOFT
            (2024, 1, "d", 5, 20, None, 0),  # whole-stint aggregate null (conflict later)
            (2024, 1, "b", 5, 20, "HARD", 0),
            (2024, 1, "c", 5, 20, "HARD", 0),
        ],
    )
    labels = (
        [(2024, 1, "a", lap, None) for lap in range(5, 11)]  # unknown up to L-1
        + [(2024, 1, "a", lap, "SOFT") for lap in range(11, 21)]  # known only later
        + [(2024, 1, "d", lap, "MEDIUM" if lap != 9 else None) for lap in range(5, 11)]
        + [(2024, 1, "d", lap, "HARD") for lap in range(11, 21)]
        + [(2024, 1, "b", 5, "HARD"), (2024, 1, "b", 6, "SOFT")]  # conflict before L-1
        + [(2024, 1, "c", lap, "HARD") for lap in range(5, 21)]
    )
    lc = spark.createDataFrame(
        labels, "season int, round int, driver_id string, lap int, compound string"
    )
    laps = _laps(spark, [(2024, 1, x, 10) for x in "adbc"])
    out = D.with_ti_inputs(att, D.TiInputs(laps, stints, _tel(spark, []), lc))
    got = {r["attacker_id"]: r.asDict() for r in out.collect()}
    assert got["a"]["attacker_compound_ti"] is None  # later SOFT labels are not used
    assert got["a"]["defender_compound_ti"] == "MEDIUM"  # later HARD labels are not used
    assert got["b"]["attacker_compound_ti"] is None  # conflict up to L-1
    assert got["b"]["defender_compound_ti"] == "HARD"
    assert got["a"]["attacker_tyre_age_laps_ti"] == 6  # 0 + 10 - 5 + 1


def test_lap_compounds_mapping_and_raw_root(spark: SparkSession) -> None:
    raw = _df(
        spark,
        TS.RAW_LAPS,
        [
            {"season": 2024, "round": 1, "driver_code": "AAA", "lap": 3, "compound": "soft"},
            {"season": 2024, "round": 1, "driver_code": "AAA", "lap": 4, "compound": "UNKNOWN"},
            {"season": 2024, "round": 1, "driver_code": "ZZZ", "lap": 4, "compound": "HARD"},
        ],
    )
    stints = _df(
        spark,
        TS.STINTS_TRACINGINSIGHTS,
        [{"season": 2024, "round": 1, "driver_id": "aaa", "driver_code": "AAA", "stint": 1}],
    )
    got = sorted(tuple(r) for r in D.lap_compounds(raw, stints).collect())
    assert got == [(2024, 1, "aaa", 3, "SOFT"), (2024, 1, "aaa", 4, None)]  # ZZZ unmapped
    assert D.ti_raw_root("hdfs://nn:8020/f1/clean") == "hdfs://nn:8020/f1/raw"
    assert D.ti_raw_root("file:///x/data/lake/clean/") == "file:///x/data/lake/raw"
    with pytest.raises(D.DatasetError, match="clean root"):
        D.ti_raw_root("file:///x/features")


def test_load_ti_inputs_reads_raw_laps_of_requested_seasons_only(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_clean_tracinginsights import _ingest, _settings, _write_drivers

    from f1sa.etl import clean_tracinginsights as C

    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    _ingest(spark, tmp_path, 2024, 1)
    _write_drivers(spark, tmp_path, [("aaa", "AAA", 1), ("bbb", "BBB", 2), ("ccc", None, 3)])
    C.run_clean_ti(spark, _settings(tmp_path), [2024])
    junk = tmp_path / "data/lake/raw/tracinginsights/laps/season=2025/round=1"
    junk.mkdir(parents=True)
    (junk / "laps.parquet").write_bytes(b"not parquet: the test season")
    seen: list[str] = []
    original = DataFrameReader.parquet

    def spy(self: Any, *p: str, **kw: Any) -> Any:
        seen.extend(str(x) for x in p)
        return original(self, *p, **kw)

    monkeypatch.setattr(DataFrameReader, "parquet", spy)
    ti = D.load_ti_inputs(spark, f"file://{tmp_path}/data/lake/clean", [2023, 2024])
    lc = ti.lap_compounds.collect()
    assert len(lc) == 15 and {r["driver_id"] for r in lc} == {"aaa", "bbb", "ccc"}
    assert any("/raw/tracinginsights/laps/season=2024/" in p for p in seen)
    assert not [p for p in seen if "2025" in p]
