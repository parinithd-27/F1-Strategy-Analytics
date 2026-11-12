"""Run ids and output folders of demo runs (``make stream-demo``, API replays).

A demo run writes to ``{demo_dir}/<run_id>/`` (default ``data/demo/``, gitignored), so the committed
``results/`` stay as recorded. Only an explicit record run (``make stream-demo RECORD=1``) writes to
``results/``. ``run_id`` = ``<season>_<round>_<UTC yyyymmddThhmmss.mmmZ>``, for example
``2024_5_20261112T175001.123Z``: sortable, and unique per race because a race replays at most once
at a time.

``python -m f1sa.demo_runs [--yes]`` (``make demo-clean [CONFIRM=yes]``) deletes what demo runs left
behind, and nothing else:

* every ``<run_id>`` folder in ``./data/demo``;
* replay checkpoints ``./data/checkpoints/api_<run_id>`` (+ ``.ready``) and ``make stream-demo``
  checkpoints ``./data/checkpoints/demo_<season>_<round>_<unix>`` (+ ``.ready``).

The two roots must be exactly ``./data/demo`` and ``./data/checkpoints`` of the working directory
(resolved, no symlink), or it refuses (exit 2). Entries with other names (such as the default
``stream_job`` checkpoint) and symlinks are kept and listed as skipped. Without ``--yes`` it only
lists (exit 1). Run it while no demo or replay is running.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

RUN_ID_RE = re.compile(r"(\d{4})_(\d{1,2})_(\d{8}T\d{6}\.\d{3}Z)")
_RUN = r"\d{4}_\d{1,2}_\d{8}T\d{6}\.\d{3}Z"
CHECKPOINT_RE = re.compile(rf"(api_{_RUN}|demo_\d{{4}}_\d{{1,2}}_\d{{9,11}})(\.ready)?")
"""Checkpoint names created by API replays (``api_<run_id>``) and ``make stream-demo``
(``demo_<S>_<R>_<unix>``), each with its ``.ready`` file."""
DEMO_ROOT = Path("data/demo")
CHECKPOINT_ROOT = Path("data/checkpoints")


def run_id(season: int, round_: int, now: datetime) -> str:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    ts = now.astimezone(UTC)
    return f"{season}_{round_}_{ts:%Y%m%dT%H%M%S}.{ts.microsecond // 1000:03d}Z"


def run_dir(demo_dir: Path, rid: str) -> Path:
    """``demo_dir / rid`` for a well-formed ``rid`` (never a path outside ``demo_dir``)."""
    if RUN_ID_RE.fullmatch(rid) is None:
        raise ValueError(f"malformed run id {rid!r}")
    return demo_dir / rid


class CleanRefusedError(ValueError):
    """A root other than ./data/demo or ./data/checkpoints."""


@dataclass
class CleanPlan:
    delete: list[Path] = field(default_factory=list)
    skipped: list[tuple[Path, str]] = field(default_factory=list)


def _check_root(root: Path, expected: Path, base: Path) -> Path:
    """``root`` if it resolves to exactly ``base / expected`` and no folder between ``base`` and it
    is a symlink; else ``CleanRefusedError``."""
    want = (base / expected).resolve()
    given = root if root.is_absolute() else base / root
    if given.resolve() != want:
        raise CleanRefusedError(f"refusing {root}: only ./{expected} may be cleaned")
    cur = base
    for part in expected.parts:
        cur = cur / part
        if cur.is_symlink():
            raise CleanRefusedError(f"refusing {root}: {cur} is a symlink")
    return want


def plan_clean(
    demo_dir: Path = DEMO_ROOT, checkpoint_root: Path = CHECKPOINT_ROOT, *, base: Path | None = None
) -> CleanPlan:
    base = (base or Path.cwd()).resolve()
    roots = (
        (_check_root(demo_dir, DEMO_ROOT, base), RUN_ID_RE),
        (_check_root(checkpoint_root, CHECKPOINT_ROOT, base), CHECKPOINT_RE),
    )
    plan = CleanPlan()
    for root, pattern in roots:
        if not root.is_dir():
            continue
        for entry in sorted(root.iterdir()):
            if entry.is_symlink():
                plan.skipped.append((entry, "symlink"))
            elif pattern.fullmatch(entry.name) is None:
                plan.skipped.append((entry, "not created by a demo run"))
            else:
                plan.delete.append(entry)
    return plan


def _size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file() and not p.is_symlink())


def clean(plan: CleanPlan) -> list[tuple[Path, int]]:
    """Delete the planned entries; returns ``(path, bytes)`` per deleted entry."""
    done: list[tuple[Path, int]] = []
    for entry in plan.delete:
        if entry.is_symlink():  # changed since planning: never follow
            continue
        size = _size(entry)
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()
        done.append((entry, size))
    return done


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(path)


def main(argv: Iterable[str] | None = None) -> int:
    from f1sa.config import get_settings

    p = argparse.ArgumentParser(prog="f1sa-demo-clean", description="Delete demo-run leftovers.")
    p.add_argument("--yes", action="store_true", help="delete (otherwise only list)")
    p.add_argument("--demo-dir", type=Path, default=None, help="must be ./data/demo")
    p.add_argument("--checkpoint-root", type=Path, default=CHECKPOINT_ROOT)
    args = p.parse_args(list(argv) if argv is not None else None)
    demo_dir: Path = args.demo_dir or get_settings().demo_dir
    try:
        plan = plan_clean(demo_dir, args.checkpoint_root)
    except CleanRefusedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    for entry, why in plan.skipped:
        print(f"keep    {_rel(entry)}  ({why})")
    if not plan.delete:
        print("nothing to delete")
        return 0
    if not args.yes:
        for entry in plan.delete:
            print(f"would delete  {_rel(entry)}")
        print(f"{len(plan.delete)} entries; re-run as 'make demo-clean CONFIRM=yes' to delete them")
        return 1
    total = 0
    for entry, size in clean(plan):
        total += size
        print(f"deleted {_rel(entry)}  ({size} bytes)")
    print(f"deleted {len(plan.delete)} entries, {total} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
