"""Replay one race through Kafka, Structured Streaming and Redis, and measure the event-to-result
delay (Step 12, traceability #31; S16 reports #35/#36 from it).

    make up                                                    # kafka + redis
    make stream-demo SEASON=2024 ROUND=5 SPEED=10              # sample lake on the host
    make stream-demo SEASON=2024 ROUND=5 SPEED=10 FULL=1       # HDFS lake in the app container
    make stream-demo ... RECORD=1                              # the recorded run (Step 16)

Writes ``streaming_latency.csv``, ``stream_progress.jsonl`` and ``stream_batches.jsonl``:

* by default to ``data/demo/<run_id>/`` (``Settings.demo_dir``, gitignored), so a demo run never
  overwrites the committed results;
* with ``--record`` (``make stream-demo RECORD=1``) to ``results/``.

An explicit ``--results-dir`` is used as given, and cannot be combined with ``--record``. The logic
lives in ``f1sa.streaming.demo`` (tested in ``tests/unit/test_demo.py``).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from f1sa.config import get_settings
from f1sa.demo_runs import run_dir, run_id
from f1sa.streaming.demo import main as demo_main

RECORD_DIR = Path("results")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def demo_argv(argv: Sequence[str], *, now: Callable[[], datetime] | None = None) -> list[str]:
    """``argv`` for ``f1sa.streaming.demo`` with the output folder decided (see the docstring)."""
    args = list(argv)
    record = "--record" in args
    args = [a for a in args if a != "--record"]
    explicit = any(a == "--results-dir" or a.startswith("--results-dir=") for a in args)
    if explicit and record:
        raise SystemExit("error: --record writes to results/; do not combine it with --results-dir")
    if explicit:
        return args
    if record:
        return [*args, "--results-dir", str(RECORD_DIR)]
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--round", type=int, required=True)
    known, _ = p.parse_known_args(args)
    stamp = (now or _utc_now)()
    out = run_dir(get_settings().demo_dir, run_id(known.season, known.round, stamp))
    return [*args, "--results-dir", str(out)]


def main(argv: Sequence[str] | None = None, **kwargs: Any) -> int:
    return demo_main(demo_argv(sys.argv[1:] if argv is None else argv), **kwargs)


if __name__ == "__main__":
    sys.exit(main())
