"""Batch vs stream feature parity on the full lake (Step 12), 2021-2024 only (2025 never read).

    make stream-parity-full        # in the app container, HDFS lake (needs make up-full)

Replays every race through the stream's foreachBatch logic in simulated micro-batches (one
partition lagging), with the deployed calibrated LR, and checks candidate sets, the 21 inputs
and the probability against the batch pipeline. Writes ``results/stream_parity_full.csv``; exit 1
on any mismatch. The logic lives in ``f1sa.streaming.parity`` (tested in
``tests/unit/test_stream_parity.py``).
"""

from __future__ import annotations

import sys

from f1sa.streaming.parity import main

if __name__ == "__main__":
    sys.exit(main())
