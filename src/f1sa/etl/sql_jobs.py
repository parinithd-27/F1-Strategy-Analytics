"""Spark SQL jobs (commitment #11). The SQL lives in ``etl/sql/*.sql``, one SELECT per file.

``run_sql_views`` registers the clean tables as temp views (``races``, ``results``, ``laps``,
``pitstops``). It then runs the files in ``SQL_VIEWS`` order, and each result becomes a temp view
named after its file, so later files can query earlier ones. The leakage-safe history views
(``pit_loss_prior`` and ``team_outlap_prior``) use window frames
``RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`` over the chronological ``race_seq``, which
means only strictly earlier races are used.
"""

from __future__ import annotations

from collections.abc import Mapping
from importlib.resources import files

from pyspark.sql import DataFrame, SparkSession

from f1sa.etl.clean import CleanError

SQL_INPUTS = ("races", "results", "laps", "pitstops")
SQL_VIEWS = (
    "race_index",
    "driver_green_pace",
    "pit_cycles",
    "pit_loss_stops",
    "pit_loss_prior",
    "outlap_deltas",
    "team_outlap_prior",
    "race_summary",
)
MIN_GREEN_LAPS = 5
"""Green laps a driver needs for a reference pace (without one, their stops are skipped)."""
MAX_ROUND = 99


def load_sql(name: str) -> str:
    """Text of ``etl/sql/{name}.sql`` (packaged with the module)."""
    if name not in SQL_VIEWS:
        raise CleanError(f"unknown SQL view {name!r}; expected one of {SQL_VIEWS}")
    return (files("f1sa.etl") / "sql" / f"{name}.sql").read_text(encoding="utf-8")


def _params(text: str) -> dict[str, object]:
    return {"min_green_laps": MIN_GREEN_LAPS} if ":min_green_laps" in text else {}


def run_sql_views(spark: SparkSession, tables: Mapping[str, DataFrame]) -> dict[str, DataFrame]:
    """Register the clean inputs and every SQL view. Returns all views by name.

    Raises ``CleanError`` if an input is missing or a round number would break ``race_seq``.
    """
    missing = [t for t in SQL_INPUTS if t not in tables]
    if missing:
        raise CleanError(f"SQL jobs need clean tables {missing}")
    max_round = tables["races"].agg({"round": "max"}).first()
    if max_round is not None and max_round[0] is not None and int(max_round[0]) > MAX_ROUND:
        raise CleanError(f"round {max_round[0]} > {MAX_ROUND} breaks race_seq = season*100+round")
    for name in SQL_INPUTS:
        tables[name].createOrReplaceTempView(name)
    views: dict[str, DataFrame] = {}
    for name in SQL_VIEWS:
        text = load_sql(name)
        df = spark.sql(text, args=_params(text))
        df.createOrReplaceTempView(name)
        views[name] = df
    return views
