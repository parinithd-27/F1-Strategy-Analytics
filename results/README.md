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
