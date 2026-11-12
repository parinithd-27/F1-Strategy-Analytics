"""Replay one race through Kafka, Structured Streaming and Redis, and measure the event-to-result
delay (Step 12, traceability #31; S16 reports #35/#36 from it).

    make up                                            # kafka + redis
    make stream-demo SEASON=2024 ROUND=5 SPEED=10      # sample lake on the host
    make stream-demo SEASON=2024 ROUND=5 SPEED=10 FULL=1   # HDFS lake in the app container

Writes ``results/streaming_latency.csv``, ``results/stream_progress.jsonl`` and
``results/stream_batches.jsonl``. The logic lives in ``f1sa.streaming.demo`` (tested in
``tests/unit/test_demo.py``).
"""

from __future__ import annotations

import sys

from f1sa.streaming.demo import main

if __name__ == "__main__":
    sys.exit(main())
