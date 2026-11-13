"""Step 10 Spark benchmarks: scalability, partitioning and caching (docs/experiments.md).

Benchmark data lives only under ``./data/bench`` (host, gitignored) and ``hdfs://namenode:8020/f1/bench``
(:func:`f1sa.bench.paths.check_bench_uri`). Nothing here writes to ``/f1/raw``, ``/f1/clean`` or
``/f1/features``, and nothing reads a label or an outcome column.
"""
