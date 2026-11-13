"""Spark scalability grid (Step 10, traceability #27, #33, #38).

    make bench-smoke                         # 1 season, x1, all masters, 1 run + the estimate
    make experiments-spark [RECORD=1] [GRID_ARGS="..."]

Runs on the host: samples ``docker stats``, runs ``python -m f1sa.bench.grid`` in the running app
container (no service is restarted), and writes ``scalability.csv`` and PNGs to
``data/bench/<run_id>/`` (``results/`` only with ``--record``). Logic: ``f1sa.bench.host``.
"""

from __future__ import annotations

import sys

from f1sa.bench.host import main_scalability

if __name__ == "__main__":
    sys.exit(main_scalability())
