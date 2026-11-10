"""Build or validate the offline sample bundle in ``Settings.sample_dir`` (``data/sample``).

    python -m f1sa.ingest.sample_builder            # validate if present (offline), else build
    python -m f1sa.ingest.sample_builder --rebuild  # force a rebuild

The bundle is a read-only mini raw lake with the same table-first layout as the raw zone:
real Jolpica pages (CC BY-NC-SA 4.0) for two races plus SYNTHETIC tyre and telemetry tables
(``is_synthetic=true``). It contains no FastF1 data.

Building reads each Jolpica page from, in order: the raw zone at ``DATA_LAKE_URI``, the local
HTTP cache (cache-only lookup), and only then the network, through the shared rate limiter (safe
next to a running bulk ingestion). Output is assembled in a temporary directory, validated, and
only then swapped into place, so a failed build never leaves a half-built bundle.

Validation is fully offline: it never creates an HTTP client.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import os
import shutil
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from f1sa import __version__
from f1sa.config import Settings, get_settings
from f1sa.ingest import synthetic
from f1sa.ingest.bulk import PARSERS
from f1sa.ingest.jolpica_client import JolpicaClient, JsonPage, parse_laps, parse_pitstops
from f1sa.ingest.planner import JOLPICA_PATHS
from f1sa.ingest.sinks import FileSink, make_sink
from f1sa.lake.paths import is_within, raw_table_uri

logger = logging.getLogger(__name__)

SAMPLE_RACES: tuple[tuple[int, int], ...] = ((2023, 5), (2024, 5))
DEFAULT_SEED = 4242
SIZE_LIMIT_BYTES = 5 * 1024 * 1024
SEASON_TABLES = ("schedule", "drivers", "constructors")
RACE_TABLES = ("results", "qualifying", "pitstops", "laps")
REQUIRED_NON_EMPTY = frozenset({"schedule", "drivers", "constructors", "results", "laps"})
MANIFEST_NAME = "MANIFEST.json"
MANIFEST_VERSION = 1
FIXED_FILES = ("LICENSE", "README.md", MANIFEST_NAME)
FASTF1_COLUMNS = frozenset(
    {
        "Speed",
        "Throttle",
        "Brake",
        "nGear",
        "RPM",
        "DRS",
        "Compound",
        "TyreLife",
        "Stint",
        "PitInTime",
        "PitOutTime",
        "TrackStatus",
        "LapStartTime",
        "SessionTime",
    }
)
JOLPICA_URL = "https://github.com/jolpica/jolpica-f1"
LICENCE_URL = "https://creativecommons.org/licenses/by-nc-sa/4.0/"

ClientFactory = Callable[[Settings], JolpicaClient]


class SampleError(RuntimeError):
    """The sample bundle could not be built or is invalid."""


@dataclass
class FileEntry:
    path: str
    bytes: int
    sha256: str
    rows: int | None
    kind: str  # "real" | "synthetic"
    origin: str  # "raw" | "cache" | "network" | "generated"


@dataclass
class BuildReport:
    files: list[FileEntry] = field(default_factory=list)
    origins: dict[str, int] = field(default_factory=dict)
    network_requests: int = 0


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


# ============================================================================== page source
class PageSource:
    """Jolpica pages from the raw zone, then the HTTP cache, then (only if needed) the network."""

    def __init__(self, settings: Settings, client_factory: ClientFactory) -> None:
        self.settings = settings
        self._client_factory = client_factory
        self._client: JolpicaClient | None = None
        self.report = BuildReport()

    @property
    def client(self) -> JolpicaClient:
        if self._client is None:
            self._client = self._client_factory(self.settings)
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def _from_raw(self, table: str, season: int, round_: int | None) -> list[JsonPage] | None:
        uri = raw_table_uri(self.settings, "jolpica", table, season, round_)
        scheme = self.settings.data_lake_uri.split("://", 1)[0]
        try:
            sink = make_sink(self.settings, scheme, uri)
            if not sink.exists(f"{table}.json"):
                return None
            pages = json.loads(sink.read_bytes(f"{table}.json"))
        except Exception as exc:  # raw zone unavailable (e.g. HDFS down): fall back
            logger.info("raw zone not readable for %s: %s", uri, exc)
            return None
        return pages if isinstance(pages, list) and pages else None

    def _from_cache(self, path: str) -> list[JsonPage] | None:
        first = self.client.cached_page(path)
        if first is None:
            return None
        total, limit = int(first["MRData"]["total"]), int(first["MRData"]["limit"])
        pages = [first]
        for offset in range(limit, total, limit) if limit > 0 else ():
            page = self.client.cached_page(path, offset)
            if page is None:
                return None
            pages.append(page)
        return pages

    def get(self, table: str, season: int, round_: int | None) -> tuple[list[JsonPage], str]:
        path = JOLPICA_PATHS[table].format(season=season, round=round_)
        pages = self._from_raw(table, season, round_)
        origin = "raw"
        if pages is None:
            pages = self._from_cache(path)
            origin = "cache"
        if pages is None:
            before = self.client.stats.network
            pages = self.client.get_pages(path)
            self.report.network_requests += self.client.stats.network - before
            origin = "network"
        self.report.origins[origin] = self.report.origins.get(origin, 0) + 1
        return pages, origin


# ============================================================================== build
def _license_text() -> str:
    return f"""Sample data licence
===================

The files under raw/jolpica/ are data from the Jolpica-F1 API ({JOLPICA_URL}),
(c) Jolpica-F1 contributors, licensed under the Creative Commons
Attribution-NonCommercial-ShareAlike 4.0 International licence (CC BY-NC-SA 4.0):
{LICENCE_URL} (full legal code: {LICENCE_URL}legalcode).

The SYNTHETIC tables under raw/synthetic/ are generated by this project from that data
(stints are inferred from the Jolpica laps and pit stops). As adaptations they are shared under
the same licence, CC BY-NC-SA 4.0.

Use: non-commercial only; give attribution to Jolpica-F1; share adaptations under the same
licence. No Formula 1 / FastF1 timing or telemetry data is included in this bundle.
"""


def _readme_text(entries: Sequence[FileEntry], seed: int) -> str:
    real = sorted({e.path.split("/")[2] for e in entries if e.kind == "real"})
    races = ", ".join(f"{s} round {r}" for s, r in SAMPLE_RACES)
    total = sum(e.bytes for e in entries)
    return f"""# Sample bundle (offline sample mode)

> **Warning:** the `raw/synthetic/` tables are **NOT real Formula 1 compound or telemetry
> data**. They are generated from fixed, documented assumptions so the pipeline can run
> offline. Any model result computed in sample mode is a **smoke test only and must never be
> reported as a finding**.

## What is in it
- Races: {races}.
- **Real** data (Jolpica-F1, CC BY-NC-SA 4.0): `raw/jolpica/` tables {", ".join(real)}.
  These are the JSON pages exactly as the API returned them. Season-level tables (schedule,
  drivers, constructors) cover the whole season.
- **Synthetic** data (`is_synthetic=true` on every row): `raw/synthetic/tyre_stints` (a compound
  and starting tyre age per stint; stints are inferred from the real pit stops and laps) and
  `raw/synthetic/lap_telemetry` (per-lap speed, throttle, brake, gear and RPM aggregates).
  The generator is `f1sa.ingest.synthetic` ({synthetic.GENERATOR_VERSION}) with seed {seed}.
  The assumptions are listed in `docs/data_dictionary.md`.
- **No FastF1 data.**
- {len(entries)} data files, {total / 1024:.0f} KiB. `MANIFEST.json` lists every file with its
  size, sha256 and origin.

## How it was built
`make sample-data` (`python -m f1sa.ingest.sample_builder`). Each Jolpica table is read
from the local raw zone, then from the local HTTP cache, and only if both miss, from the
rate-limited API. The synthetic tables are then generated, and the whole bundle is
validated before it replaces the previous one.

When the bundle exists, `make sample-data` validates it offline: hashes, schemas,
`is_synthetic`, no FastF1 data, the size limit, and that the synthetic tables regenerate
identically from the seed. `make sample-data REBUILD=1` rebuilds it.

## Licence
See `LICENSE`: Jolpica-F1 data under CC BY-NC-SA 4.0 (non-commercial, attribution,
share-alike).
"""


def build(
    settings: Settings,
    *,
    seed: int = DEFAULT_SEED,
    client_factory: ClientFactory | None = None,
    size_limit: int = SIZE_LIMIT_BYTES,
    races: Sequence[tuple[int, int]] = SAMPLE_RACES,
    out: Callable[[str], None] = print,
) -> BuildReport:
    """Build the bundle into a temp dir, validate it, then swap it into ``sample_dir``."""
    target = settings.sample_dir
    _check_sample_dir(target)
    tmp = target.with_name(f"{target.name}.tmp-{os.getpid()}")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp_settings = settings.model_copy(update={"data_lake_uri": f"file://{tmp}"})
    source = PageSource(settings, client_factory or JolpicaClient)
    entries: list[FileEntry] = []
    try:
        for season in sorted({s for s, _ in races}):
            for table in SEASON_TABLES:
                entries.append(_write_real(tmp_settings, tmp, source, table, season, None))
        for season, round_ in races:
            pages_by_table: dict[str, list[JsonPage]] = {}
            for table in RACE_TABLES:
                entry, pages = _write_real_pages(tmp_settings, tmp, source, table, season, round_)
                entries.append(entry)
                pages_by_table[table] = pages
            tables = synthetic.generate(
                parse_laps(pages_by_table["laps"]), parse_pitstops(pages_by_table["pitstops"]), seed
            )
            for name, frame in tables.items():
                entries.append(_write_synthetic(tmp_settings, tmp, name, frame, season, round_))
        (tmp / "LICENSE").write_text(_license_text(), encoding="utf-8")
        (tmp / "README.md").write_text(_readme_text(entries, seed), encoding="utf-8")
        manifest = {
            "version": MANIFEST_VERSION,
            "built_at": datetime.now(UTC).isoformat(),
            "f1sa_version": __version__,
            "seed": seed,
            "generator_version": synthetic.GENERATOR_VERSION,
            "races": [{"season": s, "round": r} for s, r in races],
            "files": [e.__dict__ for e in entries],
        }
        (tmp / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1) + "\n", encoding="utf-8")
        problems = validate(tmp, size_limit=size_limit)
        if problems:
            raise SampleError("built bundle failed validation:\n  " + "\n  ".join(problems))
        _swap(tmp, target)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    finally:
        source.close()
    source.report.files = entries
    origins = ", ".join(f"{k} {v}" for k, v in sorted(source.report.origins.items()))
    out(
        f"built {target}: {len(entries)} data files ({origins} tables; "
        f"{source.report.network_requests} network requests)"
    )
    return source.report


def _check_sample_dir(path: Path) -> None:
    if not is_within(path, Path("data")) or path.resolve() == Path("data").resolve():
        raise SampleError(f"sample_dir must be a directory inside ./data, got {path}")


def _rel(tmp: Path, uri: str, name: str) -> str:
    return str((Path(uri.removeprefix("file://")) / name).relative_to(tmp))


def _write_real_pages(
    settings: Settings, tmp: Path, source: PageSource, table: str, season: int, round_: int | None
) -> tuple[FileEntry, list[JsonPage]]:
    pages, origin = source.get(table, season, round_)
    rows = len(PARSERS[table](pages))
    if table in REQUIRED_NON_EMPTY and rows == 0:
        raise SampleError(f"Jolpica {table} for {season} round {round_} is empty")
    uri = raw_table_uri(settings, "jolpica", table, season, round_)
    written = FileSink(uri).write_json(f"{table}.json", pages, rows=rows)
    data = (Path(uri.removeprefix("file://")) / written.name).read_bytes()
    entry = FileEntry(_rel(tmp, uri, written.name), len(data), _sha(data), rows, "real", origin)
    return entry, pages


def _write_real(
    settings: Settings, tmp: Path, source: PageSource, table: str, season: int, round_: int | None
) -> FileEntry:
    return _write_real_pages(settings, tmp, source, table, season, round_)[0]


def _write_synthetic(
    settings: Settings, tmp: Path, name: str, frame: pd.DataFrame, season: int, round_: int
) -> FileEntry:
    if not bool(frame["is_synthetic"].all()):
        raise SampleError(f"synthetic table {name} has rows without is_synthetic=true")
    uri = raw_table_uri(settings, "synthetic", name, season, round_)
    data = _parquet_bytes(frame)
    FileSink(uri).write_bytes(f"{name}.parquet", data, rows=len(frame))
    return FileEntry(
        _rel(tmp, uri, f"{name}.parquet"),
        len(data),
        _sha(data),
        len(frame),
        "synthetic",
        "generated",
    )


def _swap(tmp: Path, target: Path) -> None:
    """Replace ``target`` with ``tmp`` (two renames; the old bundle is removed last)."""
    old = target.with_name(f"{target.name}.old-{os.getpid()}")
    if target.exists():
        os.replace(target, old)
    os.replace(tmp, target)
    shutil.rmtree(old, ignore_errors=True)


# ============================================================================== validate
SYNTHETIC_SCHEMAS = {
    "tyre_stints": synthetic.TYRE_STINTS_SCHEMA,
    "lap_telemetry": synthetic.LAP_TELEMETRY_SCHEMA,
}


def validate(root: Path, *, size_limit: int = SIZE_LIMIT_BYTES) -> list[str]:
    """Problems with the bundle at ``root`` (empty list = valid). Never uses the network."""
    problems: list[str] = []
    for name in FIXED_FILES:
        if not (root / name).is_file():
            problems.append(f"missing {name}")
    if problems:
        return problems
    try:
        manifest: dict[str, Any] = json.loads((root / MANIFEST_NAME).read_text(encoding="utf-8"))
        entries = [FileEntry(**f) for f in manifest["files"]]
        seed = int(manifest["seed"])
        if manifest.get("version") != MANIFEST_VERSION:
            problems.append(f"unsupported manifest version {manifest.get('version')!r}")
    except (ValueError, KeyError, TypeError) as exc:
        return [f"unreadable {MANIFEST_NAME}: {exc}"]

    on_disk = {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}
    listed = {e.path for e in entries}
    for extra in sorted(on_disk - listed - set(FIXED_FILES)):
        problems.append(f"unlisted file {extra}")
    total = sum((root / p).stat().st_size for p in on_disk)
    if total >= size_limit:
        problems.append(f"bundle is {total} bytes, limit {size_limit}")
    for path in on_disk:
        parts = Path(path).parts
        if "fastf1" in parts or path.endswith(".ff1pkl"):
            problems.append(f"FastF1 data is not allowed in the sample: {path}")

    raw: dict[tuple[str, int, int | None], list[JsonPage]] = {}
    for e in entries:
        file = root / e.path
        if not file.is_file():
            problems.append(f"missing {e.path}")
            continue
        data = file.read_bytes()
        if len(data) != e.bytes or _sha(data) != e.sha256:
            problems.append(f"checksum mismatch for {e.path}")
            continue
        table, season, round_ = _partition_of(e.path)
        if e.kind == "real":
            try:
                pages = json.loads(data)
                rows = len(PARSERS[table](pages))
            except Exception as exc:
                problems.append(f"{e.path} does not parse: {type(exc).__name__}: {exc}")
                continue
            if table in REQUIRED_NON_EMPTY and rows == 0:
                problems.append(f"{e.path} has no rows")
            raw[(table, season, round_)] = pages
        else:
            problems += _check_synthetic(e, data, table)

    # determinism: the synthetic tables must regenerate byte-identically from the real data
    for e in (x for x in entries if x.kind == "synthetic"):
        table, season, round_ = _partition_of(e.path)
        laps, pits = raw.get(("laps", season, round_)), raw.get(("pitstops", season, round_))
        if laps is None or pits is None:
            problems.append(f"cannot regenerate {e.path}: real laps/pitstops missing")
            continue
        regenerated = synthetic.generate(parse_laps(laps), parse_pitstops(pits), seed)[table]
        if _sha(_parquet_bytes(regenerated)) != e.sha256:
            problems.append(f"{e.path} does not regenerate identically from seed {seed}")
    return problems


def _partition_of(path: str) -> tuple[str, int, int | None]:
    parts = Path(path).parts  # raw/<source>/<table>/season=S[/round=R]/<file>
    if len(parts) < 5 or parts[0] != "raw":
        raise SampleError(f"unexpected bundle path {path}")
    season = int(parts[3].removeprefix("season="))
    round_ = int(parts[4].removeprefix("round=")) if parts[4].startswith("round=") else None
    return parts[2], season, round_


def _check_synthetic(entry: FileEntry, data: bytes, table: str) -> list[str]:
    try:
        frame = pd.read_parquet(io.BytesIO(data))
    except Exception as exc:
        return [f"{entry.path} is not readable Parquet: {exc}"]
    problems: list[str] = []
    schema = SYNTHETIC_SCHEMAS.get(table)
    if schema is None:
        return [f"unknown synthetic table {table}"]
    actual = {c: str(t) for c, t in frame.dtypes.items()}
    if actual != schema:
        problems.append(f"{entry.path} schema {actual} != {schema}")
    if FASTF1_COLUMNS & set(frame.columns):
        problems.append(
            f"{entry.path} has FastF1 columns {sorted(FASTF1_COLUMNS & set(frame.columns))}"
        )
    if "is_synthetic" not in frame.columns or not bool(frame["is_synthetic"].fillna(False).all()):
        problems.append(f"{entry.path} has rows without is_synthetic=true")
    if entry.rows is not None and len(frame) != entry.rows:
        problems.append(f"{entry.path} has {len(frame)} rows, manifest says {entry.rows}")
    return problems


# ============================================================================== CLI
def run(
    settings: Settings,
    *,
    rebuild: bool = False,
    seed: int = DEFAULT_SEED,
    client_factory: ClientFactory | None = None,
    races: Sequence[tuple[int, int]] = SAMPLE_RACES,
    out: Callable[[str], None] = print,
) -> int:
    """Validate (offline) when the bundle exists, else build. Returns a process exit code."""
    target = settings.sample_dir
    try:
        _check_sample_dir(target)
    except SampleError as exc:
        out(f"error: {exc}")
        return 2
    if rebuild or not (target / MANIFEST_NAME).exists():
        try:
            build(settings, seed=seed, client_factory=client_factory, races=races, out=out)
        except SampleError as exc:
            out(f"error: {exc}")
            return 1
    problems = validate(target)
    if problems:
        out(f"INVALID sample bundle {target}:")
        for p in problems:
            out(f"  - {p}")
        return 1
    manifest = json.loads((target / MANIFEST_NAME).read_text(encoding="utf-8"))
    total = sum(p.stat().st_size for p in target.rglob("*") if p.is_file())
    n_syn = sum(1 for f in manifest["files"] if f["kind"] == "synthetic")
    out(
        f"OK sample bundle {target}: {len(manifest['files'])} data files ({n_syn} synthetic), "
        f"{total / 1024:.0f} KiB (< {SIZE_LIMIT_BYTES // (1024 * 1024)} MiB), seed "
        f"{manifest['seed']}; hashes, schemas, is_synthetic, no-FastF1 and regeneration checked "
        "offline"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m f1sa.ingest.sample_builder")
    parser.add_argument("--rebuild", action="store_true", help="rebuild even if present")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    return run(get_settings(), rebuild=args.rebuild, seed=args.seed)


if __name__ == "__main__":
    sys.exit(main())
