"""Data-quality checks for the clean zone, producing a JSON-serialisable report.

Hard checks fail the clean job (exit 1); soft checks are reported only. Each check records the
number of offending rows/groups and up to five examples.

Hard: null primary keys, duplicate primary keys, a selected race without results or laps, and
pit-stop laps outside 1..(last lap + 1).
Soft: lap numbers not contiguous from 1 (gaps; retirements simply end early), positions per lap
not exactly 1..N, pit-stop laps missing from the laps table (except last lap + 1, i.e. retired
in the pits), results.laps differing from the driver's last lap, unparseable time strings,
FastF1 drivers that could not be mapped to a Jolpica driverId, and stints that do not tile the
driver's laps from 1 to the last lap.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from f1sa.lake.schemas import PRIMARY_KEYS

Severity = Literal["hard", "soft"]
MAX_EXAMPLES = 5
_RACE = ["season", "round"]
_DRIVER = ["season", "round", "driver_id"]


@dataclass
class CheckResult:
    name: str
    severity: Severity
    count: int
    examples: list[dict[str, Any]] = field(default_factory=list)
    detail: str = ""

    @property
    def passed(self) -> bool:
        return self.count == 0


@dataclass
class QualityReport:
    checks: list[CheckResult]

    @property
    def hard_failures(self) -> list[CheckResult]:
        return [c for c in self.checks if c.severity == "hard" and not c.passed]

    @property
    def soft_issues(self) -> list[CheckResult]:
        return [c for c in self.checks if c.severity == "soft" and not c.passed]

    @property
    def ok(self) -> bool:
        return not self.hard_failures

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "PASS" if self.ok else "FAIL",
            "hard_failures": len(self.hard_failures),
            "soft_issues": len(self.soft_issues),
            "checks": [{**asdict(c), "passed": c.passed} for c in self.checks],
        }

    def summary_lines(self) -> list[str]:
        status = "PASS" if self.ok else "FAIL"
        lines = [
            f"quality: {status} ({len(self.checks)} checks, {len(self.hard_failures)} hard "
            f"failures, {len(self.soft_issues)} soft issues)"
        ]
        for c in self.hard_failures + self.soft_issues:
            lines.append(f"  [{c.severity}] {c.name}: {c.count} {c.detail} e.g. {c.examples[:2]}")
        return lines


def _examples(df: DataFrame) -> list[dict[str, Any]]:
    return [
        {
            k: (v if isinstance(v, int | float | bool) or v is None else str(v))
            for k, v in row.asDict().items()
        }
        for row in df.limit(MAX_EXAMPLES).collect()
    ]


def _result(name: str, severity: Severity, bad: DataFrame, detail: str = "") -> CheckResult:
    bad = bad.cache()
    count = bad.count()
    result = CheckResult(name, severity, count, _examples(bad) if count else [], detail)
    bad.unpersist()
    return result


def check_null_keys(name: str, df: DataFrame) -> CheckResult:
    keys = PRIMARY_KEYS[name]
    cond = F.lit(False)
    for k in keys:
        cond = cond | F.col(k).isNull()
    return _result(f"null_keys:{name}", "hard", df.where(cond).select(*keys), "rows")


def check_duplicate_keys(name: str, df: DataFrame) -> CheckResult:
    keys = list(PRIMARY_KEYS[name])
    dups = df.groupBy(*keys).count().where(F.col("count") > 1)
    return _result(f"duplicate_keys:{name}", "hard", dups, "keys")


def check_required_tables(
    expected: Sequence[tuple[int, int]], results: DataFrame, laps: DataFrame, spark_df: DataFrame
) -> CheckResult:
    spark = spark_df.sparkSession
    exp = spark.createDataFrame([(s, r) for s, r in expected], "season int, round int")
    have_res = results.select(*_RACE).distinct().withColumn("has_results", F.lit(True))
    have_laps = laps.select(*_RACE).distinct().withColumn("has_laps", F.lit(True))
    j = exp.join(have_res, _RACE, "left").join(have_laps, _RACE, "left")
    bad = j.where(F.col("has_results").isNull() | F.col("has_laps").isNull())
    return _result("races_have_results_and_laps", "hard", bad, "races")


def _last_laps(laps: DataFrame) -> DataFrame:
    return laps.groupBy(*_DRIVER).agg(
        F.min("lap").alias("first_lap"),
        F.max("lap").alias("last_lap"),
        F.count("*").alias("n_laps"),
    )


def check_pit_lap_range(pitstops: DataFrame, laps: DataFrame) -> CheckResult:
    j = pitstops.join(_last_laps(laps), _DRIVER, "left")
    bad = j.where(
        (F.col("lap") < 1) | F.col("last_lap").isNull() | (F.col("lap") > F.col("last_lap") + 1)
    ).select(*_DRIVER, "stop", "lap", "last_lap")
    return _result("pit_lap_in_range", "hard", bad, "pit stops")


def check_lap_contiguity(laps: DataFrame) -> CheckResult:
    bad = _last_laps(laps).where(
        (F.col("first_lap") != 1) | (F.col("n_laps") != F.col("last_lap") - F.col("first_lap") + 1)
    )
    return _result("laps_contiguous_per_driver", "soft", bad, "drivers")


def check_positions(laps: DataFrame) -> CheckResult:
    per_lap = laps.groupBy(*_RACE, "lap").agg(
        F.count("*").alias("n"),
        F.min("position").alias("min_pos"),
        F.max("position").alias("max_pos"),
        F.countDistinct("position").alias("distinct_pos"),
    )
    bad = per_lap.where(
        (F.col("min_pos") != 1)
        | (F.col("max_pos") != F.col("n"))
        | (F.col("distinct_pos") != F.col("n"))
    )
    return _result("positions_1_to_n_per_lap", "soft", bad, "laps")


def check_pit_laps_exist(pitstops: DataFrame, laps: DataFrame) -> CheckResult:
    lap_keys = laps.select(*_DRIVER, "lap").withColumn("_present", F.lit(True))
    j = pitstops.join(lap_keys, [*_DRIVER, "lap"], "left").join(_last_laps(laps), _DRIVER, "left")
    bad = j.where(F.col("_present").isNull() & ~(F.col("lap") == F.col("last_lap") + 1)).select(
        *_DRIVER, "stop", "lap"
    )
    return _result("pit_lap_exists_in_laps", "soft", bad, "pit stops")


def check_results_laps(results: DataFrame, laps: DataFrame) -> CheckResult:
    j = results.join(_last_laps(laps), _DRIVER, "inner")
    bad = j.where(F.col("laps") != F.col("last_lap")).select(*_DRIVER, "laps", "last_lap")
    return _result("results_laps_match_lap_table", "soft", bad, "drivers")


def check_stint_tiling(stints: DataFrame, laps: DataFrame) -> CheckResult:
    agg = stints.groupBy(*_DRIVER).agg(
        F.min("start_lap").alias("s_first"),
        F.max("end_lap").alias("s_last"),
        F.sum("laps").alias("s_laps"),
    )
    j = agg.join(_last_laps(laps), _DRIVER, "inner")
    bad = j.where(
        (F.col("s_first") != 1)
        | (F.col("s_last") != F.col("last_lap"))
        | (F.col("s_laps") != F.col("last_lap"))
    ).select(*_DRIVER, "s_first", "s_last", "s_laps", "last_lap")
    return _result("stints_tile_laps", "soft", bad, "drivers")


def run_checks(
    tables: dict[str, DataFrame],
    *,
    expected_races: Sequence[tuple[int, int]],
    unmapped_fastf1: DataFrame | None = None,
    unparseable_times: int = 0,
) -> QualityReport:
    """Run every check over the stored clean tables (already restricted to the selected races)."""
    checks: list[CheckResult] = []
    for name in PRIMARY_KEYS:
        if name in tables:
            checks.append(check_null_keys(name, tables[name]))
            checks.append(check_duplicate_keys(name, tables[name]))
    laps, results, pits = tables["laps"], tables["results"], tables["pitstops"]
    checks += [
        check_required_tables(expected_races, results, laps, laps),
        check_pit_lap_range(pits, laps),
        check_lap_contiguity(laps),
        check_positions(laps),
        check_pit_laps_exist(pits, laps),
        check_results_laps(results, laps),
        check_stint_tiling(tables["stints"], laps),
        CheckResult("time_strings_parse", "soft", unparseable_times, [], "lap times"),
    ]
    if unmapped_fastf1 is not None:
        checks.append(
            _result("fastf1_drivers_mapped", "soft", unmapped_fastf1, "FastF1 drivers unmapped")
        )
    return QualityReport(checks)
