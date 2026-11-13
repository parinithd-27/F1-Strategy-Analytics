"""Partitioning and caching experiment (Step 10, traceability #28, #34).

    make bench-smoke                         # laps x1, 5 telemetry rounds, 1 run + the estimate
    make experiments-spark [RECORD=1]

Runs on the host: samples ``docker stats``, runs ``python -m f1sa.bench.partitioning`` in the
running app container, and writes ``partitioning.csv`` and a PNG to ``data/bench/<run_id>/``
(``results/`` only with ``--record``). Logic: ``f1sa.bench.host`` and ``f1sa.bench.partitioning``.
"""

from __future__ import annotations

import sys

from f1sa.bench.host import main_partitioning

if __name__ == "__main__":
    sys.exit(main_partitioning())
