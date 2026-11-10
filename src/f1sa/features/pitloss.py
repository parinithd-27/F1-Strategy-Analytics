"""Pit-loss and team out-lap history features (#12: pit loss, out-lap pace history). These are thin
typed wrappers over the Spark SQL views in ``etl/sql``, and they only use races before the
target race."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame

from f1sa.etl.clean import CleanError, conform
from f1sa.features import schemas as FS


def _view(views: Mapping[str, DataFrame], name: str) -> DataFrame:
    if name not in views:
        raise CleanError(f"SQL view {name!r} not built; call etl.sql_jobs.run_sql_views first")
    return views[name]


def pit_loss_features(views: Mapping[str, DataFrame]) -> DataFrame:
    """Per race: the prior median pit loss with its fallback source and missing flag."""
    return conform(_view(views, "pit_loss_prior"), FS.PIT_LOSS, "pit_loss")


def team_outlap_features(views: Mapping[str, DataFrame]) -> DataFrame:
    """Per (race, constructor): the prior median out-lap delta with its fallback source and flag."""
    return conform(_view(views, "team_outlap_prior"), FS.TEAM_OUTLAP_HISTORY, "team_outlap_history")


def race_summary_features(views: Mapping[str, DataFrame]) -> DataFrame:
    """Per race summary aggregates (reporting, not a pre-race feature)."""
    return conform(_view(views, "race_summary"), FS.RACE_SUMMARY, "race_summary")
