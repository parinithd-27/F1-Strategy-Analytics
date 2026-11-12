"""Data-lake path construction. The lake root comes only from ``Settings.data_lake_uri``.

Raw zone layout (table first, Hive-style partitions)::

    {lake}/raw/{source}/{table}/season={S}[/round={R}][/driver={DRV}]/
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

from f1sa.config import Settings

MIN_SEASON = 1950
MAX_SEASON = 2100

JOLPICA_SEASON_TABLES = ("schedule", "drivers", "constructors")
JOLPICA_RACE_TABLES = ("results", "qualifying", "pitstops", "laps", "sprint_results")
FASTF1_RACE_TABLES = ("laps", "session")
FASTF1_DRIVER_TABLES = ("telemetry", "position")
SYNTHETIC_RACE_TABLES = ("tyre_stints", "lap_telemetry")
TRACINGINSIGHTS_RACE_TABLES = ("laps", "rcm", "session_info", "drivers", "telemetry_lap_agg")
TRACINGINSIGHTS_DRIVER_TABLES = ("telemetry",)

RAW_TABLES: dict[str, frozenset[str]] = {
    "jolpica": frozenset(JOLPICA_SEASON_TABLES + JOLPICA_RACE_TABLES),
    "fastf1": frozenset(FASTF1_RACE_TABLES + FASTF1_DRIVER_TABLES),
    "synthetic": frozenset(SYNTHETIC_RACE_TABLES),
    "tracinginsights": frozenset(TRACINGINSIGHTS_RACE_TABLES + TRACINGINSIGHTS_DRIVER_TABLES),
}
_DRIVER = re.compile(r"^[A-Z0-9]{2,4}$")


def _check_partition(season: int, round_: int | None) -> None:
    if not MIN_SEASON <= season <= MAX_SEASON:
        raise ValueError(f"season must be in {MIN_SEASON}..{MAX_SEASON}, got {season}")
    if round_ is not None and round_ < 1:
        raise ValueError(f"round must be >= 1, got {round_}")


def raw_table_uri(
    settings: Settings,
    source: str,
    table: str,
    season: int,
    round_: int | None = None,
    driver: str | None = None,
) -> str:
    """URI of one raw-zone partition directory, e.g.
    ``{lake}/raw/jolpica/laps/season=2021/round=5`` or
    ``{lake}/raw/fastf1/telemetry/season=2021/round=5/driver=VER``."""
    if source not in RAW_TABLES:
        raise ValueError(f"unknown raw source {source!r}; expected one of {sorted(RAW_TABLES)}")
    if table not in RAW_TABLES[source]:
        raise ValueError(f"unknown {source} table {table!r}; expected {sorted(RAW_TABLES[source])}")
    _check_partition(season, round_)
    if driver is not None and round_ is None:
        raise ValueError("a driver partition needs a round")
    uri = f"{settings.data_lake_uri}/raw/{source}/{table}/season={season}"
    if round_ is not None:
        uri += f"/round={round_}"
    if driver is not None:
        if not _DRIVER.match(driver):
            raise ValueError(f"invalid driver partition value {driver!r}")
        uri += f"/driver={driver}"
    return uri


def local_path(uri: str) -> Path:
    """Filesystem path for a ``file://`` URI (relative paths like ``file://./x`` stay relative)."""
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"not a file:// URI: {uri!r}")
    rest = uri[len("file://") :]
    if not rest:
        raise ValueError(f"file URI has no path: {uri!r}")
    return Path(rest)


def hdfs_path(uri: str) -> str:
    """Absolute HDFS path of an ``hdfs://host:port/path`` URI (the authority is not used:
    bytes go through the HttpFS gateway, which talks to its own configured NameNode)."""
    parsed = urlparse(uri)
    if parsed.scheme != "hdfs":
        raise ValueError(f"not an hdfs:// URI: {uri!r}")
    path = parsed.path.rstrip("/")
    if not path.startswith("/") or ".." in path.split("/"):
        raise ValueError(f"hdfs URI needs an absolute path: {uri!r}")
    return path


def is_within(path: Path, root: Path) -> bool:
    """True if ``path`` resolves to ``root`` or somewhere below it."""
    resolved, base = path.resolve(), root.resolve()
    return resolved == base or base in resolved.parents
