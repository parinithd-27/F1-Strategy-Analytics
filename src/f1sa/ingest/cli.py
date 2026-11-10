"""Ingestion CLI.

    f1sa-ingest race --season 2023 --round 5 --sources jolpica,fastf1 --sink file

Writes raw files to ``{lake}/raw/{source}/season={season}/round={round}/`` plus a
``_manifest.json`` per partition. Each source is fetched completely before anything is written,
so a failing source leaves no partial partition behind.

Exit codes: 0 success, 1 a source failed, 2 usage error (including ``--sink hdfs`` until Step 4).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import version

from f1sa.config import Settings, get_settings
from f1sa.ingest import fastf1_loader
from f1sa.ingest.fastf1_loader import FastF1AuthError, FastF1DataError, RaceData
from f1sa.ingest.jolpica_client import (
    JolpicaClient,
    JolpicaError,
    JsonPage,
    parse_constructors,
    parse_drivers,
    parse_laps,
    parse_pitstops,
    parse_qualifying,
    parse_races,
    parse_results,
)
from f1sa.ingest.sinks import FileSink, RawSink
from f1sa.lake.paths import raw_partition_uri

logger = logging.getLogger("f1sa.ingest")

SOURCES = ("jolpica", "fastf1")
SINKS = ("file", "hdfs")
HDFS_NOT_READY = "hdfs sink is implemented in Step 4 (HttpFS gateway); use --sink file"

# endpoint name -> (path template, parser, must be non-empty)
JOLPICA_ENDPOINTS: dict[str, tuple[str, Callable[[list[JsonPage]], object], bool]] = {
    "race": ("{season}/{round}", parse_races, True),
    "results": ("{season}/{round}/results", parse_results, True),
    "qualifying": ("{season}/{round}/qualifying", parse_qualifying, False),
    "drivers": ("{season}/{round}/drivers", parse_drivers, True),
    "constructors": ("{season}/{round}/constructors", parse_constructors, True),
    "pitstops": ("{season}/{round}/pitstops", parse_pitstops, False),
    "laps": ("{season}/{round}/laps", parse_laps, True),
}


class IngestError(RuntimeError):
    """A source could not be ingested."""


@dataclass
class SourceReport:
    source: str
    partition: str
    files: list[tuple[str, int | None]]
    detail: str


def _parse_sources(value: str) -> list[str]:
    items = [s.strip() for s in value.split(",") if s.strip()]
    unknown = sorted(set(items) - set(SOURCES))
    if not items or unknown:
        raise argparse.ArgumentTypeError(
            f"--sources must be a comma list of {', '.join(SOURCES)} (got {value!r})"
        )
    return list(dict.fromkeys(items))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="f1sa-ingest", description=__doc__.split("\n\n")[0])
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)
    race = sub.add_parser("race", help="ingest one race weekend's race session")
    race.add_argument("--season", type=int, required=True)
    race.add_argument("--round", type=int, required=True, dest="round_", metavar="ROUND")
    race.add_argument("--sources", type=_parse_sources, default=["jolpica", "fastf1"])
    race.add_argument("--sink", choices=SINKS, default="file")
    race.add_argument("--offline", action="store_true", help="FastF1: use only the local cache")
    race.add_argument("--refresh", action="store_true", help="Jolpica: bypass the HTTP cache")
    return parser


def _make_sink(settings: Settings, sink: str, source: str, season: int, round_: int) -> RawSink:
    uri = raw_partition_uri(settings, source, season, round_)
    if sink == "file":
        return FileSink(uri)
    raise IngestError(HDFS_NOT_READY)


def ingest_jolpica(
    settings: Settings,
    sink: RawSink,
    season: int,
    round_: int,
    *,
    refresh: bool = False,
    client_factory: Callable[[Settings], JolpicaClient] | None = None,
) -> SourceReport:
    """Fetch every race endpoint, validate it parses, then write raw JSON pages."""
    fetched: dict[str, tuple[list[JsonPage], int, str]] = {}
    with (client_factory or JolpicaClient)(settings) as client:
        for name, (template, parser, required) in JOLPICA_ENDPOINTS.items():
            path = template.format(season=season, round=round_)
            pages = client.get_pages(path, refresh=refresh)
            rows = len(parser(pages))  # type: ignore[arg-type]
            if required and rows == 0:
                raise IngestError(f"Jolpica returned no {name} rows for {season} round {round_}")
            fetched[name] = (pages, rows, client.url_for(path))
        stats = client.stats
    fetched_at = datetime.now(UTC).isoformat()
    for name, (pages, rows, _url) in fetched.items():
        sink.write_json(f"{name}.json", pages, rows=rows)
    sink.write_manifest(
        {
            "source": "jolpica",
            "season": season,
            "round": round_,
            "fetched_at": fetched_at,
            "licence": "CC BY-NC-SA 4.0 (Jolpica-F1)",
            "endpoints": {name: url for name, (_p, _r, url) in fetched.items()},
            "is_synthetic": False,
        }
    )
    detail = f"network={stats.network} cache_hits={stats.cache_hits} retries={stats.retries}"
    return SourceReport(
        "jolpica",
        sink.partition_uri,
        [(f"{n}.json", r) for n, (_p, r, _u) in fetched.items()],
        detail,
    )


def ingest_fastf1(
    settings: Settings,
    sink: RawSink,
    season: int,
    round_: int,
    *,
    offline: bool = False,
    loader: Callable[..., RaceData] | None = None,
) -> SourceReport:
    """Load and validate the FastF1 race session, then write Parquet + session.json."""
    data = (loader or fastf1_loader.load_race)(season, round_, settings, offline=offline)
    sink.write_parquet("laps.parquet", data.laps)
    sink.write_parquet("car_data.parquet", data.car_data)
    sink.write_parquet("pos_data.parquet", data.pos_data)
    sink.write_json("session.json", data.session_info)
    sink.write_manifest(
        {
            "source": "fastf1",
            "season": season,
            "round": round_,
            "fastf1_version": version("fastf1"),
            "offline": offline,
            "is_synthetic": False,
        }
    )
    files = [
        ("laps.parquet", len(data.laps)),
        ("car_data.parquet", len(data.car_data)),
        ("pos_data.parquet", len(data.pos_data)),
        ("session.json", None),
    ]
    return SourceReport("fastf1", sink.partition_uri, files, f"offline={offline}")


def _print_report(report: SourceReport) -> None:
    print(f"[{report.source}] {report.partition}  ({report.detail})")
    for name, rows in report.files:
        print(f"    {name:<22} {'' if rows is None else f'{rows:>8} rows'}")


def run_race(args: argparse.Namespace, settings: Settings) -> int:
    if args.sink == "hdfs":
        print(f"error: {HDFS_NOT_READY}", file=sys.stderr)
        return 2
    failures = 0
    for source in args.sources:
        started = time.perf_counter()
        try:
            sink = _make_sink(settings, args.sink, source, args.season, args.round_)
            if source == "jolpica":
                report = ingest_jolpica(
                    settings, sink, args.season, args.round_, refresh=args.refresh
                )
            else:
                report = ingest_fastf1(
                    settings, sink, args.season, args.round_, offline=args.offline
                )
        except (JolpicaError, IngestError, FastF1AuthError, FastF1DataError, ValueError) as exc:
            failures += 1
            print(f"[{source}] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        _print_report(report)
        print(f"    done in {time.perf_counter() - started:.1f}s")
    return 1 if failures else 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if not args.verbose:
        for noisy in ("fastf1", "urllib3", "requests_cache"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
    settings = get_settings()
    if args.command == "race":
        return run_race(args, settings)
    raise AssertionError(f"unhandled command {args.command}")  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
