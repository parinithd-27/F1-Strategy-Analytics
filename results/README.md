# Results

Environment fingerprints of the experiments that wrote files here.

## `redis_latency.csv` (2026-11-12T11:04:14Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-12T11:04:14Z |
| git_sha | bbf91f8a6902799f2263e06a70e0545fb83b4b6a |
| git_dirty | true |
| python | 3.12.16 |
| redis_py | 8.1.0 |
| redis_server | 8.10.3 |
| redis_mode | standalone, Linux 6.18.58.1-microsoft-standard-WSL2 x86_64 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (6 logical CPUs) |
| memory | 9.7 GiB |
| redis_container_mem_limit_bytes | 402653184 |
| client_endpoint | localhost:6379 db 0 |
| params | rates_per_s=1/10/60, duration_s=60, drivers=20, redis_cli=30 x 1 s intervals |

## `scalability.csv` (2026-11-13T15:27:43Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-13T15:27:43Z |
| git_sha | 5d8ecfbf5b4f89c9362af046ae65442ae09fa458 |
| git_dirty | false |
| python | 3.12.16 |
| pyspark | 4.2.1 |
| java_in_app | openjdk version "17.0.21" 2026-10-20 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| container_mem_limits_bytes | app=6442450944, spark-master=536870912, spark-worker-1=1610612736, spark-worker-2=1610612736, namenode=805306368, datanode=805306368 |
| topology | one host: app container (driver / local[N]); spark-master + 2 workers (2 cores, 1g each); 1 namenode + 1 datanode, replication 1 |
| params | run_id=bench_20261113T152740Z, seasons=1/3/5, factors=1/4/16, masters=local1/local2/local4/cluster, warmup=1, repeats=3, large_from=16, large_repeats=3, large_masters=local4/cluster |

## `partitioning.csv` (2026-11-13T18:02:28Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-13T18:02:28Z |
| git_sha | 5d8ecfbf5b4f89c9362af046ae65442ae09fa458 |
| git_dirty | true |
| python | 3.12.16 |
| pyspark | 4.2.1 |
| java_in_app | openjdk version "17.0.21" 2026-10-20 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| container_mem_limits_bytes | app=6442450944, spark-master=536870912, spark-worker-1=1610612736, spark-worker-2=1610612736, namenode=805306368, datanode=805306368 |
| topology | one host: app container (driver / local[N]); spark-master + 2 workers (2 cores, 1g each); 1 namenode + 1 datanode, replication 1 |
| params | run_id=bench_20261113T152740Z_part, laps_factor=16, telemetry_rounds=all, warmup=1, repeats=5, master=local[4] |

## `scalability.csv` (2026-11-14T05:20:35Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T05:20:35Z |
| git_sha | d18a3335a069bea90cde23ebbd35e4196ec51416 |
| git_dirty | false |
| python | 3.12.16 |
| pyspark | 4.2.1 |
| java_in_app | openjdk version "17.0.21" 2026-10-20 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| container_mem_limits_bytes | app=6442450944, spark-master=536870912, spark-worker-1=1610612736, spark-worker-2=1610612736, namenode=805306368, datanode=805306368 |
| topology | one host: app container (driver / local[N]); spark-master + 2 workers (2 cores, 1g each); 1 namenode + 1 datanode, replication 1 |
| params | run_id=bench_20261114T052031Z_supp, supplement_of=bench_20261113T152740Z, cells=build_s1_x16_local4/build_s1_x16_cluster/build_s5_x4_local1/build_s5_x4_local2/build_s5_x4_local4/build_s5_x4_cluster, seasons=1/3/5, factors=1/4/16, masters=local1/local2/local4/cluster, warmup=1, repeats=3, large_from=16, large_repeats=3, large_masters=local4/cluster |

## `ablation_paired.csv` (2026-11-14T10:50:56Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T10:50:56Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.3 |
| pyspark | 4.2.1 |
| numpy | 2.5.4 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 12 logical CPUs |
| committed_inputs | model_eval_G3.5_k2.csv sha256 03a056738265, model_eval_real_tyre_G3.5_k2.csv sha256 e89c63c4a8cb, model_eval_real_ti_G3.5_k2.csv sha256 e813f4769293, model_eval_G5.0_k3.csv sha256 356159473e6a, model_eval_real_tyre_G5.0_k3.csv sha256 df96e1f61975, model_eval_real_ti_G5.0_k3.csv sha256 7b1e2532e44d |
| frozen_models | full_real_G3.5_k2/metadata.json sha256 92af79b93a13, full_real_tyre_G3.5_k2/metadata.json sha256 480199c50189, full_real_ti_G3.5_k2/metadata.json sha256 97b5bac47974, full_real_G5.0_k3/metadata.json sha256 d5ca4ffd4bea, full_real_tyre_G5.0_k3/metadata.json sha256 39903ec753f0, full_real_ti_G5.0_k3/metadata.json sha256 5495a9d3e693 |
| params | n_boot=1000, seed=42, ci=95% percentile, race-cluster, shared resamples across feature sets, pairs=real_tyre - real; real_ti - real; real_ti - real_tyre, models=lr_calibrated/gbt_calibrated, metrics=pr_auc/brier/roc_auc/ece, earlier_run=full_20261114T101856Z reproduced exactly, scores=data/ablation/full_20261114T105056Z |

## `streaming_latency_s10.csv` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `stream_batches_s10.jsonl` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `stream_progress_s10.jsonl` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `streaming_latency_s60.csv` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `stream_batches_s60.jsonl` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `stream_progress_s60.jsonl` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `streaming_latency_s1w900.csv` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `stream_batches_s1w900.jsonl` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `stream_progress_s1w900.jsonl` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `streaming_latency_summary.csv` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `streaming_throughput.csv` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `streaming_resources.csv` (2026-11-14T11:21:59Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:21:59Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1, f1sa-web-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s10 speed 10 full race; s60 speed 60 full race; s1w900 speed 1 window 900 s, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T112159Z |

## `redis_latency_quiet.csv` (2026-11-14T11:54:11Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:11Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| redis_py | 8.1.0 |
| redis_server | 8.10.3 |
| redis_mode | standalone, Linux 6.18.58.1-microsoft-standard-WSL2 x86_64 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| redis_container_mem_limit_bytes | 402653184 |
| client_endpoint | localhost:6379 db 0 |
| params | rates_per_s=1/10/60, duration_s=60, drivers=20, redis_cli=30 x 1 s intervals |

## `streaming_latency_s60ui.csv` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |

## `stream_batches_s60ui.jsonl` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |

## `stream_progress_s60ui.jsonl` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |

## `ui_streaming_latency_summary.csv` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |

## `ui_streaming_throughput.csv` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |

## `ui_streaming_resources.csv` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |

## `ui_latency.csv` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |

## `ui_latency_summary.csv` (2026-11-14T11:54:29Z)

| Item | Value |
|---|---|
| measured_at_utc | 2026-11-14T11:54:29Z |
| git_sha | 20e18296738fb0b7a25ab3b2af3d35971079a6ec |
| git_dirty | true |
| python | 3.12.16 |
| os | Linux 6.18.58.1-microsoft-standard-WSL2 |
| cpu | 13th Gen Intel(R) Core(TM) i7-13650HX (12 logical CPUs) |
| memory | 13.7 GiB |
| containers_running | f1sa-api-1, f1sa-kafka-1, f1sa-redis-1 |
| clock | producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop) |
| params | race=2024 R5 (sample lake: real Jolpica laps/pit stops, synthetic tyres), runs=s60ui speed 60 full race, segments=produced->batch->Redis->Pub/Sub and Redis->SSE (host client), resources=docker stats 2 s per container; psutil 0.5 s host job tree, suite=data/demo/streamlat_20261114T115429Z_ui |
