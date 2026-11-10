"""Data-lake path construction. The lake root comes only from ``Settings.data_lake_uri``."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from f1sa.config import Settings

RAW_SOURCES = frozenset({"jolpica", "fastf1"})
MIN_SEASON = 1950
MAX_SEASON = 2100


def _check_partition(season: int, round_: int) -> None:
    if not MIN_SEASON <= season <= MAX_SEASON:
        raise ValueError(f"season must be in {MIN_SEASON}..{MAX_SEASON}, got {season}")
    if round_ < 1:
        raise ValueError(f"round must be >= 1, got {round_}")


def raw_partition_uri(settings: Settings, source: str, season: int, round_: int) -> str:
    """URI of a raw-zone partition: ``{lake}/raw/{source}/season={season}/round={round}``."""
    if source not in RAW_SOURCES:
        raise ValueError(f"unknown raw source {source!r}; expected one of {sorted(RAW_SOURCES)}")
    _check_partition(season, round_)
    return f"{settings.data_lake_uri}/raw/{source}/season={season}/round={round_}"


def local_path(uri: str) -> Path:
    """Filesystem path for a ``file://`` URI (relative paths like ``file://./x`` stay relative)."""
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"not a file:// URI: {uri!r}")
    rest = uri[len("file://") :]
    if not rest:
        raise ValueError(f"file URI has no path: {uri!r}")
    return Path(rest)
