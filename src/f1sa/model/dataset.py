"""Model dataset (#16): one (G, k) setting, labelled attempts only, chronological season split.

``load_attempts`` reads ``undercut_attempts`` and joins the derived inputs (stint length at the
decision lap, per-lap telemetry at the decision lap). ``build_dataset``:

1. keeps exactly one (G, k) combination (default 3.5 s, 2 laps) and asserts that no attempt key
   (season, round, attacker_id, lap) is duplicated;
2. keeps labelled rows only (``outcome_category`` in {success, fail}); ``defender_stayed_out``
   and every ``excluded_*`` row are dropped;
3. builds the assembled feature columns of the feature set (``model.features``). Missing values are
   never imputed from data: a numeric value is set to the constant 0.0 and its ``_missing``
   indicator to 1, and categorical sources (the Step 7/8 ``*_source`` flags) are one-hot encoded
   with an ``unknown`` level;
4. splits by season (``SplitSpec``: train 2021-2023, validation 2024, test 2025). The test split
   is a ``SealedSplit`` that only ``evaluate.evaluate_test`` can open, once;
5. computes class weights (``weight = n / (2 * n_class)``) from the **training rows only**.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.etl.clean import select_races
from f1sa.features.build import read_feature
from f1sa.features.undercut_rule import DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS
from f1sa.model import features as MF

KEY = ("season", "round", "attacker_id", "lap")
LABELLED = ("success", "fail")
LABEL = "label_d"
WEIGHT = "class_weight"
RACE_ID = "race_id"
RaceKey = tuple[int, int]


class DatasetError(RuntimeError):
    """The model dataset cannot be built (no rows for the setting, duplicates, bad split)."""


@dataclass(frozen=True)
class SplitSpec:
    train: tuple[int, ...] = (2021, 2022, 2023)
    validation: tuple[int, ...] = (2024,)
    test: tuple[int, ...] = (2025,)

    def __post_init__(self) -> None:
        sets = [set(self.train), set(self.validation), set(self.test)]
        if not all(sets[:2]):
            raise DatasetError("train and validation seasons must not be empty")
        if sets[0] & sets[1] or sets[0] & sets[2] or sets[1] & sets[2]:
            raise DatasetError(f"split seasons overlap: {self}")
        if max(self.train) >= min(self.validation) or (
            self.test and max(self.validation) >= min(self.test)
        ):
            raise DatasetError(f"split is not chronological: {self}")


class _TestToken:
    """Proof that the caller is the single test-season evaluation (see ``evaluate.py``)."""

    def __init__(self, _key: object) -> None:
        if _key is not _TOKEN_KEY:
            raise PermissionError("test-season tokens are issued only by evaluate.evaluate_test")


_TOKEN_KEY = object()


def _issue_test_token() -> _TestToken:
    """Only ``f1sa.model.evaluate.evaluate_test`` may call this (enforced by a test)."""
    return _TestToken(_TOKEN_KEY)


class SealedSplit:
    """The test split. Its rows can be read once, with a token from ``evaluate.evaluate_test``."""

    def __init__(self, df: DataFrame, seasons: tuple[int, ...]) -> None:
        self._df = df
        self.seasons = seasons
        self.opened = 0

    def unseal(self, token: _TestToken) -> DataFrame:
        if not isinstance(token, _TestToken):
            raise PermissionError("the test split needs a token from evaluate.evaluate_test")
        if self.opened:
            raise PermissionError("the test split was already opened in this run")
        self.opened += 1
        return self._df


@dataclass
class ModelDataset:
    train: DataFrame
    validation: DataFrame
    test: SealedSplit
    feature_set: str
    columns: tuple[str, ...]
    g: float
    k: int
    split: SplitSpec
    class_weights: dict[int, float]
    counts: dict[str, dict[str, int]] = field(default_factory=dict)


def _side_join(df: DataFrame, other: DataFrame, side: str, cols: Iterable[str]) -> DataFrame:
    o = other.select(
        "season",
        "round",
        F.col("driver_id").alias(f"{side}_id"),
        F.col("lap").alias("decision_lap"),
        *[F.col(c).alias(f"{side}_{c}") for c in cols],
    )
    return df.join(o, ["season", "round", f"{side}_id", "decision_lap"], "left")


def with_derived_inputs(attempts: DataFrame, stints: DataFrame, telemetry: DataFrame) -> DataFrame:
    """Add stint length and telemetry at the decision lap for attacker and defender."""
    out = attempts
    for side in ("attacker", "defender"):
        s = stints.select(
            F.col("season").alias("_s"),
            F.col("round").alias("_r"),
            F.col("driver_id").alias("_d"),
            F.col("start_lap").alias("_start"),
            F.col("end_lap").alias("_end"),
        )
        cond = (
            (F.col("season") == F.col("_s"))
            & (F.col("round") == F.col("_r"))
            & (F.col(f"{side}_id") == F.col("_d"))
            & F.col("decision_lap").between(F.col("_start"), F.col("_end"))
        )
        out = (
            out.join(s, cond, "left")
            .withColumn(
                f"{side}_stint_laps", (F.col("decision_lap") - F.col("_start") + 1).cast("int")
            )
            .drop("_s", "_r", "_d", "_start", "_end")
        )
    tel = telemetry.withColumn("telemetry_is_synthetic", F.col("is_synthetic"))
    for side in ("attacker", "defender"):
        out = _side_join(out, tel, side, (*MF.TELEMETRY_COLUMNS, "telemetry_is_synthetic"))
    return out


def load_attempts(
    spark: SparkSession, features_root: str, races: set[RaceKey] | None = None
) -> DataFrame:
    """``undercut_attempts`` (optionally limited to ``races``) with the derived model inputs."""

    def read(table: str) -> DataFrame:
        df = read_feature(spark, features_root, table)
        return select_races(df, None, races) if races is not None else df

    return with_derived_inputs(
        read("undercut_attempts"), read("stints"), read("telemetry_lap_features")
    )


def feature_exprs(feature_set: str) -> list[Column]:
    """The assembled columns of ``feature_set`` (constant fill + indicators; one-hot)."""
    out: list[Column] = []
    for spec in MF.specs_for(feature_set):
        src = F.col(spec.name)
        if spec.kind == "numeric":
            value, missing = MF.model_columns(spec)
            out.append(F.coalesce(src.cast("double"), F.lit(0.0)).alias(value))
            out.append(src.isNull().cast("double").alias(missing))
        else:
            names = MF.model_columns(spec)
            known = F.lit(False)
            for cat, name in zip(spec.categories, names, strict=False):
                hit = F.coalesce(src == F.lit(cat), F.lit(False))
                known = known | hit
                out.append(hit.cast("double").alias(name))
            out.append((~known).cast("double").alias(names[-1]))
    return out


def select_setting(df: DataFrame, g: float, k: int) -> DataFrame:
    """Rows of one (G, k) setting; raises on no rows or on a duplicated attempt key."""
    sel = df.where(
        (F.col("gap_threshold_s") == F.lit(float(g))) & (F.col("response_window_laps") == k)
    )
    if sel.isEmpty():
        settings = sorted(
            (r[0], r[1])
            for r in df.select("gap_threshold_s", "response_window_laps").distinct().collect()
        )
        raise DatasetError(f"no undercut attempts for G={g}, k={k}; available: {settings}")
    dup = sel.groupBy(*KEY).count().where("count > 1")
    if not dup.isEmpty():
        raise DatasetError(
            f"duplicate attempt keys after the (G, k) filter: {dup.limit(5).collect()}"
        )
    return sel


def class_weights(train: DataFrame) -> dict[int, float]:
    """``n / (2 * n_class)`` per class, from the training rows only; 1.0 for an absent class."""
    counts = {int(r[0]): int(r[1]) for r in train.groupBy("label").count().collect()}
    n = sum(counts.values())
    return {c: (n / (2.0 * counts[c]) if counts.get(c) else 1.0) for c in (0, 1)}


def build_dataset(
    df: DataFrame,
    *,
    g: float = DEFAULT_GAP_S,
    k: int = DEFAULT_WINDOW_LAPS,
    feature_set: str = MF.DEFAULT_FEATURE_SET,
    split: SplitSpec | None = None,
) -> ModelDataset:
    spec = split or SplitSpec()
    columns = MF.assembled_columns(feature_set)
    flags = [f for p in MF.FEATURE_SETS[feature_set] for f in MF.SYNTHETIC_FLAGS.get(p, ())]
    sel = select_setting(df, g, k).where(F.col("outcome_category").isin(*LABELLED))
    sel = sel.where(F.col("label").isNotNull())
    rows = sel.select(
        *KEY,
        "defender_id",
        F.concat_ws("-", F.col("season").cast("string"), F.col("round").cast("string")).alias(
            RACE_ID
        ),
        F.col("label").cast("int").alias("label"),
        F.col("label").cast("double").alias(LABEL),
        *[F.col(c) for c in MF.BASELINE_COLUMNS],
        *[F.col(f) for f in flags],
        *feature_exprs(feature_set),
    )
    train = rows.where(F.col("season").isin(*spec.train))
    validation = rows.where(F.col("season").isin(*spec.validation))
    test = rows.where(F.col("season").isin(*spec.test))
    weights = class_weights(train)
    w = F.when(F.col("label") == 1, F.lit(weights[1])).otherwise(F.lit(weights[0]))
    train = train.withColumn(WEIGHT, w).cache()
    validation = validation.withColumn(WEIGHT, F.lit(1.0)).cache()
    test = test.withColumn(WEIGHT, F.lit(1.0))
    counts = {name: _counts(part) for name, part in (("train", train), ("validation", validation))}
    _check_disjoint(train, validation)
    return ModelDataset(
        train=train,
        validation=validation,
        test=SealedSplit(test, spec.test),
        feature_set=feature_set,
        columns=columns,
        g=float(g),
        k=int(k),
        split=spec,
        class_weights=weights,
        counts=counts,
    )


def _counts(df: DataFrame) -> dict[str, int]:
    r = df.agg(
        F.count(F.lit(1)).alias("n"),
        F.coalesce(F.sum("label"), F.lit(0)).alias("pos"),
        F.countDistinct(RACE_ID).alias("races"),
    ).first()
    assert r is not None
    return {"n": int(r["n"]), "positive": int(r["pos"]), "races": int(r["races"])}


def _check_disjoint(train: DataFrame, validation: DataFrame) -> None:
    a = {r[0] for r in train.select(RACE_ID).distinct().collect()}
    b = {r[0] for r in validation.select(RACE_ID).distinct().collect()}
    if a & b:
        raise DatasetError(f"races in both train and validation: {sorted(a & b)[:5]}")
