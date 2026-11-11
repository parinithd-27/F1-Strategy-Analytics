"""Constructor lineage: team renames that share one history.

Jolpica gives a new ``constructorId`` when a team is renamed. Any history keyed by team (the
Step 6 ``team_outlap_history`` and the Step 7 ``outlap_features``) therefore has to map the old id
to the current one, or the renamed team starts with no history.

``LINEAGE`` maps an old Jolpica constructorId to the id that continues it:

* ``alphatauri`` (2021-2023) → ``rb`` (2024-2025)
* ``alfa`` (Alfa Romeo, 2021-2023) → ``sauber`` (2024-2025)

``STABLE_CONSTRUCTORS`` lists the ids that are not renamed within 2021-2025, plus the current id
of every lineage. A clean-zone id that is in neither ``LINEAGE`` nor ``STABLE_CONSTRUCTORS`` is
reported by ``unknown_constructor_ids``. The feature build then fails, so a missed rename cannot
silently split a team's history.

Status: verified against the full 2021-2025 clean zone in Step 8b. Every constructor id is in the
map, and ``results/constructors_full.csv`` (``make report-full``) records them by season.
"""

from __future__ import annotations

from itertools import chain

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

LINEAGE: dict[str, str] = {
    "alphatauri": "rb",
    "alfa": "sauber",
}
STABLE_CONSTRUCTORS: frozenset[str] = frozenset(
    {
        "red_bull",
        "mercedes",
        "ferrari",
        "mclaren",
        "aston_martin",
        "alpine",
        "williams",
        "haas",
        "rb",
        "sauber",
    }
)


def history_key(constructor_id: str) -> str:
    """The id whose history ``constructor_id`` shares (itself unless it was renamed)."""
    return LINEAGE.get(constructor_id, constructor_id)


def history_key_col(col: str = "constructor_id") -> Column:
    """Column expression for ``history_key`` (a literal map lookup, no join)."""
    mapping = F.create_map(*[F.lit(x) for x in chain.from_iterable(LINEAGE.items())])
    return F.coalesce(mapping[F.col(col)], F.col(col))


def with_history_key(df: DataFrame, col: str = "constructor_id") -> DataFrame:
    """``df`` with a ``team_history_key`` column."""
    return df.withColumn("team_history_key", history_key_col(col))


def lineage_rows() -> list[tuple[str, str]]:
    """(constructor_id, history_key) pairs for the SQL ``constructor_lineage`` view."""
    return sorted(LINEAGE.items())


def unknown_constructor_ids(df: DataFrame, col: str = "constructor_id") -> list[str]:
    """Distinct ids in ``df`` that are neither a lineage key nor a stable id (nulls ignored)."""
    known = set(LINEAGE) | STABLE_CONSTRUCTORS
    ids = {r[0] for r in df.select(col).where(F.col(col).isNotNull()).distinct().collect()}
    return sorted(ids - known)
