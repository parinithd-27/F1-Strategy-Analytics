"""Ingestion CLI (raw zone, resumable via ``data/manifest.json``).

    f1sa-ingest season-range --seasons 2021-2025 --sources jolpica,fastf1 --sink file
    f1sa-ingest race --season 2023 --round 5 --sources jolpica --sink file

Layout: ``{DATA_LAKE_URI}/raw/{source}/{table}/season=S[/round=R][/driver=DRV]/``. The sink must
match the scheme of ``DATA_LAKE_URI`` (``--sink hdfs`` needs e.g.
``DATA_LAKE_URI=hdfs://namenode:8020/f1``; bytes are sent through the HttpFS gateway).

Exit codes: 0 ok | 1 some items failed (recorded in the manifest) | 2 usage error |
3 aborted (FastF1 auth or hard rate limit) | 130 interrupted (manifest saved; rerun to resume).
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from f1sa.config import get_settings
from f1sa.ingest.bulk import EXIT_USAGE, BulkIngest, RunOptions
from f1sa.ingest.manifest import ManifestLockedError

SOURCES = ("jolpica", "fastf1")
SINKS = ("file", "hdfs")
FIRST_SEASON = 2018  # FastF1 timing data starts in 2018


def _parse_sources(value: str) -> list[str]:
    items = [s.strip() for s in value.split(",") if s.strip()]
    unknown = sorted(set(items) - set(SOURCES))
    if not items or unknown:
        raise argparse.ArgumentTypeError(
            f"--sources must be a comma list of {', '.join(SOURCES)} (got {value!r})"
        )
    return list(dict.fromkeys(items))


def parse_seasons(value: str, *, current_year: int | None = None) -> list[int]:
    """``2021-2025`` or ``2021,2023`` -> sorted unique seasons within 2018..current year."""
    last = current_year or datetime.now(UTC).year
    seasons: set[int] = set()
    try:
        for part in (p.strip() for p in value.split(",")):
            if not part:
                raise ValueError("empty item")
            if "-" in part:
                lo, hi = (int(x) for x in part.split("-", 1))
                if lo > hi:
                    raise ValueError(f"descending range {part}")
                seasons.update(range(lo, hi + 1))
            else:
                seasons.add(int(part))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--seasons must look like 2021-2025 or 2021,2023 ({exc})"
        ) from exc
    bad = [s for s in seasons if not FIRST_SEASON <= s <= last]
    if bad:
        raise argparse.ArgumentTypeError(f"seasons must be in {FIRST_SEASON}..{last}, got {bad}")
    return sorted(seasons)


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--sources", type=_parse_sources, default=["jolpica", "fastf1"])
    p.add_argument("--sink", choices=SINKS, default="file")
    p.add_argument("--offline", action="store_true", help="FastF1: read only from the local cache")
    p.add_argument("--refresh", action="store_true", help="Jolpica: bypass the HTTP cache")
    p.add_argument("--dry-run", action="store_true", help="print the plan and estimate only")
    p.add_argument("--only-failed", action="store_true", help="retry only items marked failed")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="f1sa-ingest", description=__doc__.split("\n\n")[0])
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)
    rng = sub.add_parser("season-range", help="ingest every race weekend of the given seasons")
    rng.add_argument("--seasons", type=parse_seasons, required=True)
    _common(rng)
    race = sub.add_parser("race", help="ingest one race weekend (plus its season tables)")
    race.add_argument("--season", type=int, required=True)
    race.add_argument("--round", type=int, required=True, dest="round_", metavar="ROUND")
    _common(race)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if args.command == "race":
        try:
            seasons = parse_seasons(str(args.season))
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))
        if args.round_ < 1:
            parser.error("--round must be >= 1")
        rounds: set[int] | None = {args.round_}
    else:
        seasons, rounds = args.seasons, None
    options = RunOptions(
        seasons=seasons,
        sources=args.sources,
        sink=args.sink,
        rounds=rounds,
        only_failed=args.only_failed,
        dry_run=args.dry_run,
        offline=args.offline,
        refresh=args.refresh,
    )
    try:
        return BulkIngest(get_settings(), options).run()
    except ManifestLockedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
