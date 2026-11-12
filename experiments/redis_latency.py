"""Redis read/write latency at streaming rates (Step 11, traceability #37).

    make up                                  # core profile: redis must be running
    uv run python experiments/redis_latency.py [--rates 1 10 60] [--duration 60] [--skip-cli]

Writes ``results/redis_latency.csv`` and appends the environment fingerprint to
``results/README.md``. The logic lives in ``f1sa.serving.latency_bench`` (tested in
``tests/unit/test_latency_bench.py``).
"""

from __future__ import annotations

import sys

from f1sa.serving.latency_bench import main

if __name__ == "__main__":
    sys.exit(main())
