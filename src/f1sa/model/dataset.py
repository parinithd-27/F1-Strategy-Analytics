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

TracingInsights inputs (feature set ``real_ti``, development mode only) are added by
``with_ti_inputs``: compound and tyre age at the decision lap from ``stints_tracinginsights``, and
telemetry from ``telemetry_lap_real`` **only through the left join from the Jolpica laps** (a
fan-out raises). Low coverage never drops, excludes or down-weights a race or an attempt: a
missing value is 0.0 plus its ``_missing`` indicator like every other input (``missing_rates``
reports the per-season rates).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from f1sa.etl.clean import read_parquet_table, select_races
from f1sa.features import schemas as FS
from f1sa.features.build import read_feature
from f1sa.features.undercut_rule import DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS
from f1sa.lake import schemas as S
from f1sa.lake import schemas_tracinginsights as TS
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


def season_partition_paths(
    spark: SparkSession, features_root: str, table: str, seasons: Iterable[int]
) -> list[str]:
    """The existing ``{root}/{table}/season=S`` directories for ``seasons`` (only those paths are
    checked; no other directory of the table is listed)."""
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise DatasetError("Spark JVM gateway unavailable")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    paths = []
    for season in sorted(set(seasons)):
        uri = f"{features_root}/{table}/season={int(season)}"
        path = jvm.org.apache.hadoop.fs.Path(uri)
        if path.getFileSystem(conf).exists(path):
            paths.append(uri)
    return paths


def read_seasons(
    spark: SparkSession, root: str, table: str, schema: StructType, seasons: Iterable[int]
) -> DataFrame:
    """``{root}/{table}`` restricted to ``seasons`` at the file level (see
    ``read_feature_seasons``); empty with ``schema`` if no requested season exists."""
    base = f"{root}/{table}"
    paths = season_partition_paths(spark, root, table, seasons)
    cols = [f.name for f in schema.fields]
    if not paths:
        return spark.createDataFrame([], schema).select(cols)
    return spark.read.schema(schema).option("basePath", base).parquet(*paths).select(cols)


def read_feature_seasons(
    spark: SparkSession, features_root: str, table: str, seasons: Iterable[int]
) -> DataFrame:
    """A stored feature table restricted to ``seasons`` **at the file level**.

    Only the partition directories ``{root}/{table}/season=S`` of the requested seasons are
    listed and read (``basePath`` keeps the partition columns); no other season's directory is
    listed, opened or inspected, and the schema is explicit. A requested season without a
    directory (for example an empty sparse table) is skipped.
    """
    return read_seasons(spark, features_root, table, FS.FEATURE_SCHEMAS[table], seasons)


# ------------------------------------------------------------------ TracingInsights inputs
_LAP_KEY = ["season", "round", "driver_id", "lap"]
TI_CLEAN_TABLES: dict[str, StructType] = {
    "laps": S.CLEAN_SCHEMAS["laps"],
    "stints_tracinginsights": TS.CLEAN_SCHEMAS_TI["stints_tracinginsights"],
    "telemetry_lap_real": TS.CLEAN_SCHEMAS_TI["telemetry_lap_real"],
}
"""Clean tables read for ``real_ti`` (season partitions only, like the feature tables)."""


@dataclass(frozen=True)
class TiInputs:
    """``lap_compounds``: (season, round, driver_id, lap, compound) with one TracingInsights
    compound label per lap (a dry/wet compound or null). The decision-time compound is built
    from these labels on laps ``start_lap..L-1`` only, never from the whole-stint aggregate."""

    jolpica_laps: DataFrame
    stints: DataFrame
    telemetry: DataFrame
    lap_compounds: DataFrame


def read_clean_seasons(
    spark: SparkSession, clean_root: str, table: str, seasons: Iterable[int]
) -> DataFrame:
    """A ``TI_CLEAN_TABLES`` clean table, only the ``season=S`` directories of ``seasons``."""
    return read_seasons(spark, clean_root, table, TI_CLEAN_TABLES[table], seasons)


def load_ti_inputs(
    spark: SparkSession,
    clean_root: str,
    seasons: Iterable[int],
    races: set[RaceKey] | None = None,
) -> TiInputs:
    wanted = tuple(seasons)

    def read(table: str) -> DataFrame:
        df = read_clean_seasons(spark, clean_root, table, wanted)
        return select_races(df, None, races) if races is not None else df

    stints = read("stints_tracinginsights")
    raw_root = ti_raw_root(clean_root)
    raw = [
        read_parquet_table(
            spark,
            f"{raw_root}/tracinginsights/laps/season={int(s)}/round=*/laps.parquet",
            TS.RAW_LAPS,
            "TI laps",
        )
        for s in sorted(set(wanted))
    ]
    laps_raw = reduce(DataFrame.unionByName, raw) if raw else spark.createDataFrame([], TS.RAW_LAPS)
    if races is not None:
        laps_raw = select_races(laps_raw, None, races)
    return TiInputs(
        read("laps"),
        stints,
        read("telemetry_lap_real"),
        lap_compounds(laps_raw, stints),
    )


def ti_raw_root(clean_root: str) -> str:
    """The raw zone next to ``clean_root`` (``{lake}/clean`` -> ``{lake}/raw``)."""
    if not clean_root.rstrip("/").endswith("/clean"):
        raise DatasetError(f"expected a .../clean root, got {clean_root}")
    return clean_root.rstrip("/").removesuffix("/clean") + "/raw"


def known_compound(col: str) -> Column:
    """Upper-cased dry/wet compound, null for anything else (as the clean job's stints)."""
    return F.when(F.upper(F.col(col)).isin(*TS.KNOWN_COMPOUNDS), F.upper(F.col(col)))


def lap_compounds(raw_laps: DataFrame, stints: DataFrame) -> DataFrame:
    """Per-lap compound labels keyed by driver_id. The driver_code -> driver_id map is the one
    the clean job used (``stints_tracinginsights``); a code with no stint is left out."""
    mapping = stints.select("season", "round", "driver_code", "driver_id").distinct()
    return (
        raw_laps.select(
            "season",
            "round",
            "driver_code",
            F.col("lap").cast("int").alias("lap"),
            known_compound("compound").alias("compound"),
        )
        .join(mapping, ["season", "round", "driver_code"], "inner")
        .select("season", "round", "driver_id", "lap", "compound")
    )


def tyre_at_decision(attempts: DataFrame, ti: TiInputs, side: str) -> DataFrame:
    """Per (season, round, ``{side}_id``, decision_lap): compound and tyre age at L-1.

    The stint is the ``stints_tracinginsights`` row with start_lap <= L-1 <= end_lap (its start
    and starting tyre life are known at L-1). The compound is the single known label over that
    stint's laps start_lap..L-1 (``ti.lap_compounds``), null when there is none or when they
    disagree; labels after L-1 are never read. Tyre age = tyre_age_at_start + L-1 - start_lap + 1.
    """
    keys = attempts.select(
        "season", "round", F.col(f"{side}_id").alias("driver_id"), "decision_lap"
    ).distinct()
    st = ti.stints.select(
        "season",
        "round",
        "driver_id",
        F.col("start_lap").alias("_start"),
        F.col("end_lap").alias("_end"),
        F.col("tyre_age_at_start").alias("_age0"),
    )
    ks = keys.join(st, ["season", "round", "driver_id"], "inner").where(
        F.col("decision_lap").between(F.col("_start"), F.col("_end"))
    )
    lc = ti.lap_compounds.select(
        F.col("season").alias("_s"),
        F.col("round").alias("_r"),
        F.col("driver_id").alias("_d"),
        F.col("lap").alias("_lap"),
        F.col("compound").alias("_c"),
    )
    cond = (
        (F.col("season") == F.col("_s"))
        & (F.col("round") == F.col("_r"))
        & (F.col("driver_id") == F.col("_d"))
        & F.col("_lap").between(F.col("_start"), F.col("decision_lap"))
    )
    grouped = (
        ks.join(lc, cond, "left")
        .groupBy("season", "round", "driver_id", "decision_lap", "_start", "_age0")
        .agg(F.array_sort(F.collect_set("_c")).alias("_set"))
    )
    age = F.col("_age0") + F.col("decision_lap") - F.col("_start") + 1
    return grouped.select(
        "season",
        "round",
        F.col("driver_id").alias(f"{side}_id"),
        "decision_lap",
        F.when(F.size("_set") == 1, F.col("_set")[0]).alias(f"{side}_compound{MF.TI_SUFFIX}"),
        age.cast("int").alias(f"{side}_tyre_age_laps{MF.TI_SUFFIX}"),
    )


def with_ti_inputs(attempts: DataFrame, ti: TiInputs) -> DataFrame:
    """Add the ``tracinginsights`` model inputs at the decision lap for attacker and defender.

    Telemetry comes only from ``jolpica_laps LEFT JOIN telemetry_lap_real`` on (season, round,
    driver_id, lap); a lap without a TracingInsights row keeps nulls. ``partial`` laps keep their
    aggregates and set ``telemetry_partial`` = 1.0 (0.0 for full or none). Raises
    ``DatasetError`` if the left join or any attempt join changes a row count (fan-out).
    """
    jol = ti.jolpica_laps.select(*_LAP_KEY)
    cov = F.col("telemetry_coverage")
    tel = ti.telemetry.select(
        *_LAP_KEY,
        *MF.TELEMETRY_COLUMNS,
        F.when(cov == "partial", F.lit(1.0))
        .when(cov.isin("full", "none"), F.lit(0.0))
        .alias("telemetry_partial"),
    )
    lap_ti = jol.join(tel, _LAP_KEY, "left")
    n_laps, n_joined = jol.count(), lap_ti.count()
    if n_joined != n_laps:
        raise DatasetError(
            f"telemetry_lap_real fans out the Jolpica laps: {n_laps} -> {n_joined} rows"
        )
    n_attempts = attempts.count()
    out = attempts
    for side in MF.SIDES:
        o = lap_ti.select(
            "season",
            "round",
            F.col("driver_id").alias(f"{side}_id"),
            F.col("lap").alias("decision_lap"),
            *[F.col(c).alias(f"{side}_{c}{MF.TI_SUFFIX}") for c in MF.TI_TELEMETRY_COLUMNS],
        )
        out = out.join(o, ["season", "round", f"{side}_id", "decision_lap"], "left")
        out = out.join(
            tyre_at_decision(attempts, ti, side),  # keys from attempts: linear plan
            ["season", "round", f"{side}_id", "decision_lap"],
            "left",
        )
    out = out.withColumn(
        f"tyre_age_delta_laps{MF.TI_SUFFIX}",
        F.col(f"defender_tyre_age_laps{MF.TI_SUFFIX}")
        - F.col(f"attacker_tyre_age_laps{MF.TI_SUFFIX}"),
    )
    # Cached: a later filter (the (G, k) setting) then stops at the cached relation. Pushed into
    # these joins over season/round-partitioned scans, dynamic partition pruning nests its own
    # pruning subqueries (each holding the same joins) and the plan grows exponentially.
    out = out.cache()
    n_out = out.count()
    if n_out != n_attempts:
        raise DatasetError(
            f"TracingInsights joins changed the attempt count: {n_attempts} -> {n_out} "
            "(duplicate or overlapping stints)"
        )
    return out


def load_attempts_for_seasons(
    spark: SparkSession,
    features_root: str,
    seasons: Iterable[int],
    ti_clean_root: str | None = None,
) -> DataFrame:
    """``load_attempts`` for ``seasons`` only, read with ``read_feature_seasons`` (the other
    seasons' partitions are never touched). With ``ti_clean_root`` the TracingInsights inputs
    are added from the same seasons' clean partitions (``with_ti_inputs``)."""
    wanted = tuple(seasons)

    def read(table: str) -> DataFrame:
        return read_feature_seasons(spark, features_root, table, wanted)

    df = with_derived_inputs(
        read("undercut_attempts"), read("stints"), read("telemetry_lap_features")
    )
    if ti_clean_root is None:
        return df
    return with_ti_inputs(df, load_ti_inputs(spark, ti_clean_root, wanted))


def load_attempts(
    spark: SparkSession,
    features_root: str,
    races: set[RaceKey] | None = None,
    ti_clean_root: str | None = None,
) -> DataFrame:
    """``undercut_attempts`` (optionally limited to ``races``) with the derived model inputs.
    TracingInsights inputs (``ti_clean_root``) need ``races``: their seasons bound the read."""

    if ti_clean_root is not None and races is None:
        raise DatasetError("TracingInsights inputs need explicit races or seasons")

    def read(table: str) -> DataFrame:
        df = read_feature(spark, features_root, table)
        return select_races(df, None, races) if races is not None else df

    df = with_derived_inputs(
        read("undercut_attempts"), read("stints"), read("telemetry_lap_features")
    )
    if ti_clean_root is None or races is None:
        return df
    seasons = sorted({s for s, _ in races})
    return with_ti_inputs(df, load_ti_inputs(spark, ti_clean_root, seasons, races))


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


MISSING_RATE_COLUMNS = (
    "season",
    "feature",
    "provenance",
    "n_attempts",
    "n_missing",
    "missing_rate",
    "n_telemetry_partial",
)


def missing_rates(df: DataFrame, feature_set: str, g: float, k: int) -> list[dict[str, object]]:
    """Per season and feature of ``feature_set``: labelled attempts at (G, k), how many have the
    raw input null (filled with 0.0 + ``_missing`` by ``feature_exprs``) and the share. Counts
    only: no label or outcome value. ``n_telemetry_partial`` is filled for the partial flags."""
    specs = MF.specs_for(feature_set)
    sel = select_setting(df, g, k).where(
        F.col("outcome_category").isin(*LABELLED) & F.col("label").isNotNull()
    )
    aggs = [F.count(F.lit(1)).alias("_n")]
    for i, spec in enumerate(specs):
        aggs.append(F.sum(F.col(spec.name).isNull().cast("int")).alias(f"_m{i}"))
        if spec.name.endswith(f"telemetry_partial{MF.TI_SUFFIX}"):
            aggs.append(F.sum((F.col(spec.name) == 1.0).cast("int")).alias(f"_p{i}"))
    out: list[dict[str, object]] = []
    for r in sel.groupBy("season").agg(*aggs).orderBy("season").collect():
        n = int(r["_n"])
        for i, spec in enumerate(specs):
            m = int(r[f"_m{i}"] or 0)
            partial = f"_p{i}" in r.asDict()
            out.append(
                {
                    "season": int(r["season"]),
                    "feature": spec.name,
                    "provenance": spec.provenance,
                    "n_attempts": n,
                    "n_missing": m,
                    "missing_rate": round(m / n, 4) if n else 0.0,
                    "n_telemetry_partial": int(r[f"_p{i}"] or 0) if partial else "",
                }
            )
    return out


def class_weights(train: DataFrame) -> dict[int, float]:
    """``n / (2 * n_class)`` per class, from the training rows only; 1.0 for an absent class."""
    counts = {int(r[0]): int(r[1]) for r in train.groupBy("label").count().collect()}
    n = sum(counts.values())
    return {c: (n / (2.0 * counts[c]) if counts.get(c) else 1.0) for c in (0, 1)}


ORDER_KEY = ("season", "round", "attacker_id", "defender_id", "lap")


def canonical_order(df: DataFrame) -> DataFrame:
    """One partition, rows sorted by the attempt key ``ORDER_KEY``.

    Used for the TracingInsights development sets only (``real`` keeps its plan unchanged). Their
    inputs come from range joins and group-bys whose output row order varies run to run, and GBT
    (quantile binning, partition-wise tree aggregation) is sensitive to row order and partitioning.
    A single sorted partition makes every fit, the development run's and the permutation refit's,
    see the same rows in the same order. ``DatasetError`` if the key is not unique, since ties
    would leave the order undefined."""
    if df.groupBy(*ORDER_KEY).count().where(F.col("count") > 1).limit(1).count():
        raise DatasetError(f"attempt key {ORDER_KEY} is not unique; row order would be undefined")
    return df.repartition(1).sortWithinPartitions(*ORDER_KEY)


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
    train = train.withColumn(WEIGHT, w)
    validation = validation.withColumn(WEIGHT, F.lit(1.0))
    if MF.uses_tracinginsights(feature_set):
        train, validation = canonical_order(train), canonical_order(validation)
    train = train.cache()
    validation = validation.cache()
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
