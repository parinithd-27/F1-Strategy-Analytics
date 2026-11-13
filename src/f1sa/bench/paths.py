"""Where benchmark data may live, run ids and output folders, and ``make bench-clean``.

Benchmark data may be written only to:

* ``file://<cwd>/data/bench/...`` (the repo's ``./data/bench`` on the host,
  ``/workspace/data/bench`` in the app container; ``data/`` is gitignored), and
* ``hdfs://namenode:8020/f1/bench/...``.

:func:`check_bench_uri` is called by every benchmark writer. It refuses every other scheme, host
and path, ``..`` segments, and a symlinked ``data`` or ``data/bench`` folder, so a benchmark can
never write to ``/f1/raw``, ``/f1/clean``, ``/f1/features``, ``./data/lake`` or ``./results``.

Outputs (CSV, plots, JSONL) go to ``./data/bench/<run_id>/``. Only a record run (``RECORD=1``)
writes to ``./results/``.

``python -m f1sa.bench.paths [--yes]`` (``make bench-clean [CONFIRM=yes]``) lists, or with ``--yes``
deletes, every entry of ``./data/bench`` and every child of HDFS ``/f1/bench`` (through HttpFS).
Any other root is refused with exit 2. Without ``--yes`` it only lists (exit 1).
"""

from __future__ import annotations

import argparse
import posixpath
import shutil
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

import requests

BENCH_DIR = Path("data/bench")
RESULTS_DIR = Path("results")
HDFS_AUTHORITY = "namenode:8020"
HDFS_BENCH = "/f1/bench"
HDFS_BENCH_URI = f"hdfs://{HDFS_AUTHORITY}{HDFS_BENCH}"
PROTECTED_HDFS = ("/f1/raw", "/f1/clean", "/f1/features")
"""Named only in messages: every path outside ``/f1/bench`` is refused anyway."""


class BenchPathError(ValueError):
    """A benchmark path outside ``./data/bench`` or HDFS ``/f1/bench``."""


def _check_local_root(base: Path) -> Path:
    """``base/data/bench`` resolved, refusing a symlinked ``data`` or ``data/bench``."""
    cur = base
    for part in BENCH_DIR.parts:
        cur = cur / part
        if cur.is_symlink():
            raise BenchPathError(f"refusing: {cur} is a symlink")
    return (base / BENCH_DIR).resolve()


def bench_local_root(base: Path | None = None) -> Path:
    """The absolute ``./data/bench`` of ``base`` (default: the working directory)."""
    return _check_local_root((base or Path.cwd()).resolve())


def check_bench_uri(uri: str, *, base: Path | None = None) -> str:
    """``uri`` if it is inside ``file://<base>/data/bench`` or ``hdfs://namenode:8020/f1/bench``;
    otherwise :class:`BenchPathError`. The root itself is allowed (``bench-clean`` lists it)."""
    parsed = urlparse(uri)
    if parsed.scheme not in ("file", "hdfs"):
        raise BenchPathError(f"refusing {uri!r}: only file:// and hdfs:// benchmark paths")
    raw = parsed.path
    if not raw.startswith("/"):
        raise BenchPathError(f"refusing {uri!r}: the path must be absolute")
    if ".." in PurePosixPath(raw).parts:
        raise BenchPathError(f"refusing {uri!r}: '..' is not allowed")
    if parsed.scheme == "hdfs":
        if parsed.netloc != HDFS_AUTHORITY:
            raise BenchPathError(f"refusing {uri!r}: HDFS benchmark data only on {HDFS_AUTHORITY}")
        path = posixpath.normpath(raw)
        if path != HDFS_BENCH and not path.startswith(HDFS_BENCH + "/"):
            raise BenchPathError(
                f"refusing {uri!r}: HDFS benchmark data only under {HDFS_BENCH} "
                f"(never {', '.join(PROTECTED_HDFS)})"
            )
        return f"hdfs://{HDFS_AUTHORITY}{path}"
    if parsed.netloc not in ("", "localhost"):
        raise BenchPathError(f"refusing {uri!r}: file:// with a host")
    root = bench_local_root(base)
    target = Path(raw).resolve()
    if target != root and root not in target.parents:
        raise BenchPathError(f"refusing {uri!r}: local benchmark data only under {root}")
    return f"file://{target}"


def bench_file_uri(*parts: str, base: Path | None = None) -> str:
    """``file://<base>/data/bench/<parts...>``, checked."""
    return check_bench_uri(f"file://{bench_local_root(base).joinpath(*parts)}", base=base)


def bench_hdfs_uri(*parts: str) -> str:
    """``hdfs://namenode:8020/f1/bench/<parts...>``, checked."""
    return check_bench_uri("/".join([HDFS_BENCH_URI, *parts]))


def new_run_id(now: datetime | None = None) -> str:
    """``bench_<UTC yyyymmddThhmmssZ>``, sortable."""
    ts = (now or datetime.now(UTC)).astimezone(UTC)
    return f"bench_{ts:%Y%m%dT%H%M%SZ}"


def run_dir(run_id: str, base: Path | None = None) -> Path:
    """``./data/bench/<run_id>`` for a plain name (no separators, no dots at the start)."""
    if not run_id or "/" in run_id or run_id.startswith(".") or run_id != Path(run_id).name:
        raise BenchPathError(f"malformed run id {run_id!r}")
    return bench_local_root(base) / run_id


def output_dir(run_id: str, *, record: bool, base: Path | None = None) -> Path:
    """Where a run's CSV and plots go: ``./results`` only for a record run, else
    ``./data/bench/<run_id>``."""
    if record:
        return ((base or Path.cwd()).resolve() / RESULTS_DIR).resolve()
    return run_dir(run_id, base)


# ============================================================================== bench-clean
@dataclass
class CleanPlan:
    local: list[Path] = field(default_factory=list)
    skipped: list[tuple[Path, str]] = field(default_factory=list)
    hdfs: list[str] = field(default_factory=list)
    hdfs_error: str | None = None


class HttpFsBench:
    """The two HttpFS calls ``bench-clean`` needs, limited to children of ``/f1/bench``."""

    def __init__(self, url: str, user: str, session: requests.Session | None = None) -> None:
        self._url = url.rstrip("/") + "/webhdfs/v1"
        self._user = user
        self._session = session or requests.Session()

    def _call(self, method: str, path: str, op: str, **params: str) -> requests.Response:
        return self._session.request(
            method,
            f"{self._url}{path}",
            params={"op": op, "user.name": self._user, **params},
            timeout=30,
        )

    def children(self, path: str = HDFS_BENCH) -> list[str]:
        """Absolute paths of the children of ``path`` (empty if it does not exist)."""
        check_bench_uri(f"hdfs://{HDFS_AUTHORITY}{path}")
        resp = self._call("GET", path, "LISTSTATUS")
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        statuses: list[dict[str, Any]] = resp.json()["FileStatuses"]["FileStatus"]
        return sorted(f"{path.rstrip('/')}/{s['pathSuffix']}" for s in statuses)

    def delete(self, path: str) -> None:
        """Recursive delete of a strict child of ``/f1/bench``."""
        checked = urlparse(check_bench_uri(f"hdfs://{HDFS_AUTHORITY}{path}")).path
        if checked == HDFS_BENCH:
            raise BenchPathError("refusing to delete the /f1/bench root itself")
        resp = self._call("DELETE", checked, "DELETE", recursive="true")
        resp.raise_for_status()
        if resp.json().get("boolean") is not True:
            raise BenchPathError(f"HttpFS did not delete {checked}")


def plan_clean(
    root: Path = BENCH_DIR,
    hdfs_root: str = HDFS_BENCH,
    *,
    base: Path | None = None,
    hdfs: HttpFsBench | None = None,
) -> CleanPlan:
    """What ``bench-clean`` would delete. ``root`` must be exactly ``./data/bench`` and
    ``hdfs_root`` exactly ``/f1/bench``; anything else is refused."""
    base = (base or Path.cwd()).resolve()
    want = bench_local_root(base)
    given = root if root.is_absolute() else base / root
    if given.resolve() != want or (given.is_symlink()):
        raise BenchPathError(f"refusing {root}: only ./{BENCH_DIR} may be cleaned")
    if posixpath.normpath(hdfs_root) != HDFS_BENCH or ".." in PurePosixPath(hdfs_root).parts:
        raise BenchPathError(f"refusing HDFS root {hdfs_root!r}: only {HDFS_BENCH} may be cleaned")
    plan = CleanPlan()
    if want.is_dir():
        for entry in sorted(want.iterdir()):
            if entry.is_symlink():
                plan.skipped.append((entry, "symlink"))
            else:
                plan.local.append(entry)
    if hdfs is not None:
        try:
            plan.hdfs = hdfs.children(HDFS_BENCH)
        except (requests.RequestException, KeyError, ValueError) as exc:
            plan.hdfs_error = f"{type(exc).__name__}: {exc}"
    return plan


def clean(plan: CleanPlan, hdfs: HttpFsBench | None = None) -> list[str]:
    """Delete the planned entries; returns what was deleted."""
    done: list[str] = []
    for entry in plan.local:
        if entry.is_symlink():  # changed since planning: never follow
            continue
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()
        done.append(str(entry))
    if hdfs is not None:
        for path in plan.hdfs:
            hdfs.delete(path)
            done.append(f"hdfs://{HDFS_AUTHORITY}{path}")
    return done


def main(argv: Iterable[str] | None = None, *, session: requests.Session | None = None) -> int:
    from f1sa.config import get_settings

    p = argparse.ArgumentParser(prog="f1sa-bench-clean", description="Delete benchmark data.")
    p.add_argument("--yes", action="store_true", help="delete (otherwise only list)")
    p.add_argument("--root", type=Path, default=BENCH_DIR, help="must be ./data/bench")
    p.add_argument("--hdfs-root", default=HDFS_BENCH, help="must be /f1/bench")
    p.add_argument("--local-only", action="store_true", help="leave HDFS alone")
    args = p.parse_args(list(argv) if argv is not None else None)
    settings = get_settings()
    hdfs = (
        None
        if args.local_only
        else HttpFsBench(settings.hdfs_httpfs_url, settings.hdfs_user, session)
    )
    try:
        plan = plan_clean(args.root, args.hdfs_root, hdfs=hdfs)
    except BenchPathError as exc:
        print(f"bench-clean: {exc}", file=sys.stderr)
        return 2
    _print_plan(plan)
    if not args.yes:
        print("bench-clean: nothing deleted (re-run with CONFIRM=yes)")
        return 1
    deleted = clean(plan, hdfs if plan.hdfs_error is None else None)
    for item in deleted:
        print(f"deleted {item}")
    if plan.hdfs_error is not None:
        print(f"bench-clean: HDFS not cleaned ({plan.hdfs_error})", file=sys.stderr)
        return 1
    return 0


def _print_plan(plan: CleanPlan) -> None:
    for entry in plan.local:
        print(f"would delete {entry}")
    for entry, why in plan.skipped:
        print(f"skip {entry} ({why})")
    for path in plan.hdfs:
        print(f"would delete hdfs://{HDFS_AUTHORITY}{path}")
    if plan.hdfs_error is not None:
        print(f"HDFS {HDFS_BENCH} not listed: {plan.hdfs_error}")


def format_bytes(n: float | None) -> str:
    """``1.5 GiB`` style (``n/a`` for None)."""
    if n is None:
        return "n/a"
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} GiB"


if __name__ == "__main__":
    sys.exit(main())
