# Spark experiments: scalability, partitioning and caching (Step 10)

Traceability: #27 (runtime for 1/3/5 seasons), #28 (unpartitioned vs partitioned, with and without
caching), #33 (runtime and scaling trend), #34 (speed-up vs baseline), #38 (resource usage).
Code: `src/f1sa/bench/` (entry points `experiments/scalability.py`, `experiments/partitioning.py`,
`experiments/_resources.py`). Tests: `tests/unit/test_bench_*.py`, `tests/unit/test_makefile_bench.py`.

## How to run

| Command | What it does | Writes |
|---|---|---|
| `make bench-smoke` | scalability at 1 season, x1, every master, 0 warm-up + 1 run; partitioning at laps x1, 5 telemetry rounds, 1 run; prints the full-grid estimate and, over 8 h, a reduced grid | `data/bench/<run_id>/` only |
| `make experiments-spark` | full grid + full partitioning run | `data/bench/<run_id>/` |
| `make experiments-spark RECORD=1` | the recorded run (overnight, nothing else running) | `results/scalability.csv`, `scalability_build.png`, `scalability_telemetry.png`, `partitioning.csv`, `partitioning_speedup.png`; fingerprints appended to `results/README.md` |
| `make experiments-spark RECORD=1 GRID_ARGS="..."` | a reduced grid, e.g. the one `bench-smoke` proposes | as above |
| `make experiments-spark [RECORD=1] RESUME=<run_id>` | continues an interrupted run (the command is printed at the start of every run): reuses its prepared data when its marker exists (or prepares it again), skips cells already in its CSV, reruns an interrupted cell from its warm-up | the same run folders |
| `make bench-supplement SUPPLEMENT_OF=<run_id> CELLS="<cell ids>" [RECORD=1] [RESUME=<id>]` | re-runs only the named build/telemetry cells of a recorded grid (and their prep cells) with that grid's settings (`data/bench/<run_id>/grid_spec.json`); no partitioning run | `data/bench/<id>_supp/`; with `RECORD=1` appends the re-run rows to `results/scalability.csv` (new columns `rerun_of`, `note`; the original failed rows stay), a fingerprint to `results/README.md`, and redraws the plots. Refused before running if a cell's recorded row is `ok` or already re-run |
| `make bench-replot [RECORD=1]` | redraws the build plot from `results/scalability.csv` (no measurement) | `data/bench/replot/`; with `RECORD=1` only `results/scalability_build.png` |
| `make bench-clean [CONFIRM=yes]` | lists (exit 1), or deletes, every entry of `./data/bench` and every child of HDFS `/f1/bench` | deletes only there |

### Interruptions and resume
- Each finished grid cell's row (also skipped and aborted cells) is appended to
  `data/bench/<run_id>/scalability.csv` at once, atomically (the file is rewritten to a temp file and
  renamed). A cell without its row is not done. Partitioning commits a whole group
  (dataset, file system, layout, variant) at a time to `data/bench/<run_id>_part/partitioning.jsonl`.
- Prepared data (replicated copies, partitioning layouts) get a `_BENCH_PREP.json` marker as their last
  write. On resume a marked copy is reused; an unmarked one (interrupted prep) is deleted and prepared
  again. A prep whose cells are all done is not rerun; a rerun prep adds no second row.
- Only one grid runs per run folder (a `flock` on `.grid.lock`, released by the kernel even after a
  SIGKILL). On resume the grid first kills cell processes of the same run that an earlier, hard-killed
  grid left behind, then continues in grid order. The memory guard's skips are restored from the CSV.
- `make experiments-spark` gives both scripts one run id; `RESUME=<run_id>` resumes the grid and then
  the partitioning run (started fresh if it never began). `GRID_ARGS` must not change on resume (the
  stored `grid_spec.json` is used).
- With `RECORD=1`, `results/` is written only when the whole grid is complete (and partitioning only
  when it is complete), and each run id's fingerprint is appended once. HDFS data are deleted only
  after a complete run, so an incomplete run keeps everything it needs to resume.

Prerequisites: `make up-full` (HDFS and the standalone cluster healthy) and the app container at
`mem_limit: 6144m` (api and web stopped during the overnight grid). The targets use `docker compose exec` into the running app container: no
service is started, stopped, rebuilt or recreated. `docker stats` is sampled on the host.

## Data the benchmarks touch

- Read only: `/f1/clean` (clean tables), `/f1/raw/tracinginsights/telemetry/season=2024` (raw
  telemetry samples, Step 4C). HDFS keeps `/f1/raw` hadoop-owned, and every benchmark writer calls
  `bench.paths.check_bench_uri`, which refuses everything outside `hdfs://namenode:8020/f1/bench`
  and `./data/bench` (`/f1/raw`, `/f1/clean`, `/f1/features`, `data/lake`, `results/`, `..`, other
  hosts and schemes, a symlinked `data/bench`).
- Written: `/f1/bench/<run_id>/` (replicated copies, feature outputs, partitioning layouts; deleted
  at the end of each run unless `--keep-data`) and `./data/bench/<run_id>/` (JSONL, CSV, PNG, the
  shipped source zip; gitignored via `data/*`).
- Labels and outcomes: never read. The feature build computes `undercut_attempts` (it is part of
  the build) but writes it to Spark's `noop` sink: it is executed in full and never collected,
  read back or persisted. The 2025 timing data are used for scale only (the 5-season set).

## Workloads

### Feature build (scalability, every cell)
`features.build.compute_features` unchanged (RDD, DataFrame and SQL jobs of Steps 6-8), over every
race of the input, each table written with the production writer `etl.io.write_partitioned`
(partitioned by season/round) to `/f1/bench/<run_id>/scal/<cell>/<run>`, deleted untimed after
each run. The clean inputs are cached per run as in `run_build`, then released, so every run starts
from HDFS.

### Synthetic replication of the lap-level tables (`bench/replicate.py`)
The nine clean tables the build reads are replicated x1, x4, x16 and x64, once per (seasons,
factor), untimed (`prep` cells), to `/f1/bench/<run_id>/replicated/s<n>_x<f>/`:

- replica 0 is the input unchanged, `is_replicated = false`;
- replica r >= 1: `season + 100·r` (so `race_seq = season*100 + round` stays unique and replicas
  sort after the real seasons), `race_date` / `race_start_utc` + 100·r years, `driver_id + "__r<r>"`,
  `is_replicated = true`; every value (lap times, gaps, stops) and the constructor ids unchanged;
- row counts are exactly factor x input with unique primary keys (tested);
- the flag is dropped before the build (which expects exactly the clean schemas).

Seasons axis: 1 = 2024, 3 = 2022-2024, 5 = 2021-2025.

**Can show:** how runtime, shuffle volume and memory of the real feature build grow with rows,
races and partitions on this host, including costs that grow faster than the data: the many small
season/round partitions (about 23·n·f per table) and the global prior windows that have no
partition key (single-partition sort; the Known issue noted in Step 6).

**Cannot show:** real data diversity. Replicas are exact copies, so skew, join cardinalities,
compression and history distributions are idealised, and history windows grow across replicas in
a way real seasons would not. Nothing here says anything about model quality.

### Telemetry aggregation (scalability, x1 cells only)
Over the real 2024 raw telemetry (1.94 GB in 503 Parquet files, about 46,000 samples per
driver-race file): `features.telemetry_agg.aggregate_samples` unchanged (a lag window per lap and a
groupBy with seven aggregates) per (driver, lap), joined to the clean laps via the driver code and
summarised per round, into `noop`. Telemetry cannot be replicated (x64 would be about 120 GB), so
the seasons axis chooses the first 5 / 14 / 24 rounds of 2024 (about 20 / 60 / 100 % of the
bytes), and the replication factors do not apply.

**Can show:** scan, decode, shuffle and window cost of real bytes up to about 2 GB, and how
it changes with the number of cores. **Cannot show:** behaviour beyond 2 GB or across
seasons, or HDFS data locality (one datanode).

### Partitioning and caching (`bench/partitioning.py`, master `local[4]`)
Two datasets, each copied once per layout and file system:

- `laps`: clean laps + results of 2021-2025 replicated x16 (synthetic);
- `telemetry`: the real 2024 raw samples (10 columns) + the 2024 clean laps and drivers.

Layouts: `unpartitioned` (one folder, `repartition(N)` files: 8 laps, 16 telemetry) and
`partitioned` (`partitionBy(season, round)`, one file per partition). File systems: `file://`
(`data/bench`) and `hdfs://` (`/f1/bench`). Variants: `none`, `cache` (`cache()` + `count()`),
`persist_disk` (`persist(DISK_ONLY)` + `count()`); the materialisation time is reported apart from
the query time. Queries: `one_race` (2024 R5), `one_season` (2024; laps only, because for the one
telemetry season it would equal `full_scan`), `full_scan`, `join_agg` (laps ⋈ results per
constructor and season; telemetry per lap ⋈ drivers ⋈ laps per round). Averages are taken over
`decimal(20,6)` so the result does not depend on the summation order. Every result has a checksum
that must be identical across layouts, variants and file systems (the run fails otherwise).
Speed-up = baseline median / cell median, against the simplest baseline (file://, unpartitioned,
no cache) and against unpartitioned without cache on the same file system.

## Grid, repetitions and statistics
Seasons {1, 3, 5} x replication {1, 4, 16, 64} x master {`local[1]`, `local[2]`, `local[4]`,
`spark://spark-master:7077`}: 72 cells (12 prep, 48 build, 12 telemetry). Each build and telemetry
cell runs in a fresh process (fresh JVM): 1 warm-up (discarded) + 5 timed runs, each in its own
Spark job group. Per cell: median, Q1, Q3, IQR (numpy linear percentiles), min and max wall time;
median input, output and shuffle bytes over the timed runs (Spark REST API of the driver UI, summed
over the group's stage attempts); peak RSS and CPU of the cell's process tree (psutil, 0.5 s),
peak container memory (cgroup v2 `memory.current − inactive_file`), and peak memory and CPU per
container from `docker stats` (2 s, host) cut to the cell's window; git sha and dirty flag.
`spark.sql.shuffle.partitions = 8` (2 x the largest core count) in every cell.

### Memory per cell

| Master | Driver | Executors | Where |
|---|---|---|---|
| `local[1]`, `local[2]`, `local[4]` | `spark.driver.memory = 2g` (all local cells), `MALLOC_ARENA_MAX=2` | in the driver | app container, 6 GiB (guard 5.4 GiB) |
| `spark://spark-master:7077` | 1g, client mode, driver on the app container's IP | one per worker: 768m, 2 cores; `spark.cores.max = 4` | workers 1g / 1.5 GiB containers |

The heap is 2g, not the 2560m first planned: at 2560m the driver JVM's RSS was 3.4 GB (about
0.9 GB off-heap), and the memory guard killed the first smoke cell (1 season, x1) at 3.61 GiB.
The cluster executors have less memory (2 x 768m) than a local[4] driver (2g). That difference
is part of what a cluster cell measures and is stated next to every cluster number. Cluster cells
ship the current `src/f1sa` as a zip (`addPyFile`), because the worker image holds the `f1sa` of its
build. Each cluster cell checks that the executors import the zip and fails otherwise.

### Memory guard
A running cell is **killed** (`aborted_memory_guard`, process-group SIGKILL) when the app
container's memory (cgroup v2, `memory.current − inactive_file`) passes 90 % of its limit, or the
VM's `MemAvailable` drops below 1.5 GiB. A cell that runs longer than 4 h is killed (`timeout`).
After a kill or timeout, every larger factor of the same (phase, seasons, master) is **skipped**
(`skipped_memory_guard`, with the factor that ended). Skipped, aborted and failed cells stay in the
CSV with the reason. Executor memory in the workers is not guarded from the app container: an
executor OOM fails the cell (`failed`, with the error).

There is no projection of the peak. The first smoke attempt projected the x1 peak (3.16 GiB)
linearly to 12.65 GiB at x4 and skipped the cell, which then ran at 3.55 GiB. The driver's
footprint is mostly its fixed 2g heap plus about 1 GB off-heap and Python workers. It grows with
the input, but far less than linearly, because Spark spills when the heap is full. That puts x16
near or over the 90 % guard for local cells: the runtime guard decides, and the CSV records it.

## What these results can and cannot show

- **Single host.** Every "node" is a container on one WSL 2 VM (12 logical CPUs, 14 GB), so
  `local[N]` and the standalone cluster compete for the same cores, memory bandwidth and disk.
  The numbers say how this pipeline behaves on this machine, not on a cluster.
- **No network shuffle.** Shuffle blocks move between containers over the Docker bridge on one
  kernel, which is far faster and more reliable than a real network. Cluster shuffle costs are
  understated.
- **No data locality.** HDFS has one datanode (replication 1), so every read is "local" or goes
  through the same datanode. Locality-aware scheduling, rack awareness and replica choice are not
  exercised.
- **Shared page cache.** The OS page cache is shared by every container and the host and cannot be
  dropped without root. After the warm-up, `file://` and HDFS block reads are mostly served from
  memory. Timed runs therefore measure warm reads. The warm-up time (recorded, discarded) is the
  closest thing to a cold read.
- **Two workers on one machine.** The "cluster" is a standalone master plus 2 workers with
  2 cores and 1g each, on the same CPUs as the driver. `spark://` vs `local[4]` compares scheduling
  and serialization overhead at equal cores, not scale-out.
- **Replication is synthetic.** Larger lap inputs are exact copies with shifted keys (above), and
  telemetry is real but at most one season. Trends with input size are therefore indicative. The
  absolute costs of skew, cardinality and compression are not measured.
- **Memory limits decide the largest cells.** The app container has 6 GiB and a 5.40 GiB guard, so local-mode
  cells at the largest sizes have no timing, and a cluster cell can finish only because its executors sit in other
  containers (see the supplement). That says something about this host's limits, not about speed.
- **Executor loss.** Two supplement cluster cells lost an executor (heartbeat timeout) and Spark recovered, so
  their medians may include recovery time.
- **Trends only.** Read the results as relative trends with their IQR: how a cell scales against
  its neighbours and which choices help (partition pruning, caching). They are not absolute
  throughput figures to compare with other systems. Run-to-run variance on this WSL2/Docker Desktop
  host is real (see the Step 11 Redis notes); every cell reports its IQR, min and max.

## Results
The recorded run writes `results/scalability.csv`, `results/partitioning.csv` and the three PNGs,
and appends the environment fingerprint (git sha, CPU, memory, container limits, Spark and Python
versions, topology, grid parameters) to `results/README.md`. The smoke run below is plumbing
evidence only (one repetition, no warm-up) and is not a result.

The recorded grid is the reduced one proposed after the smoke run (1 warm-up + 3 timed runs per cell, 1/3/5
seasons, x1/x4, x16 for 1 season on `local[4]` and the cluster, no x64), run as `bench_20261113T152740Z`; six
build cells failed and were re-run (see the supplement). All numbers below are medians read from
`results/scalability.csv` and `results/partitioning.csv`. Every statement is for this single host (see the
limits above), and speed-ups are ratios of medians.

### Final findings

#### Telemetry aggregation (real 2024 data; `results/scalability_telemetry.png`)
| Rounds (about % of bytes) | local[1] | local[2] | local[4] | 2-worker cluster |
|---|---|---|---|---|
| 5 (20 %) | 3.85 s | 2.53 s (1.52x) | 1.72 s (2.24x) | 2.17 s (1.77x) |
| 14 (60 %) | 11.01 s | 6.46 s (1.70x) | 4.53 s (2.43x) | 5.13 s (2.15x) |
| 24 (100 %) | 18.01 s | 10.44 s (1.72x) | 7.49 s (2.40x) | 8.24 s (2.19x) |

Speed-up in brackets is over local[1] at the same rounds. The cost grows about linearly with the rounds
(local[1]: 3.85, 11.01, 18.01 s for 5, 14, 24 rounds), and the speed-up from more cores is about 1.7x for two
and 2.4x for four at 14 and 24 rounds, i.e. 85 % and 60 % per core. The cluster (2 x 2 cores) is about 10 %
slower than `local[4]` at equal cores, which is the scheduling and serialisation overhead (no network here). At
5 rounds the fixed cost of a job is a larger share, so the speed-ups are lower.

#### Feature build (`results/scalability_build.png`; cells before the `select_races` fix)
Real seasons at x1 (laps: 26,574 / 74,489 / 124,834):

| Seasons | local[1] | local[2] | local[4] | cluster |
|---|---|---|---|---|
| 1 (2024) | 80.5 s | 51.4 s | 48.0 s | 40.3 s |
| 3 (2022-24) | 57.9 s | 55.9 s | 49.4 s | 50.7 s |
| 5 (2021-25) | 84.0 s | 70.8 s | 65.8 s | 69.0 s |

Replication at 1 and 3 seasons (laps 106,296 at s1 x4 and 297,956 at s3 x4):

| Cell | local[1] | local[2] | local[4] | cluster |
|---|---|---|---|---|
| 1 season x4 | 131.8 s | 79.2 s | 64.7 s | 71.7 s |
| 3 seasons x4 | 257.8 s | 230.5 s | 200.5 s | 161.9 s |

- **Fixed overhead dominates at small sizes.** Going from 1 to 5 real seasons multiplies the lap rows by 4.7 but
  the `local[4]` time only by 1.37 (48.0 to 65.8 s), and at x1 the four masters are within 1.2x to 2x of each
  other. The job's fixed cost (JVM start, reading and planning the nine tables, many small season/round
  partitions) is most of the run. The 1-season `local[1]` value (80.5 s, IQR 7.8 s) is above its 3-season value
  (57.9 s); the series is not monotonic there, and this is not explained here.
- **Per-core scaling shows only with more work.** At 3 seasons x4 the order is the expected one: `local[1]`
  257.8 s, `local[2]` 230.5 s (1.12x), `local[4]` 200.5 s (1.29x), and **the cluster was fastest at 161.9 s
  [160.9, 164.8] (1.59x over `local[1]`)**. At 1 season x4 `local[4]` (64.7 s) was still ahead of the cluster
  (71.7 s). The cluster spreads the work over separate executor JVMs as well as 4 cores, so its lead is not only
  a core count effect.
- **Growth with rows.** `local[1]` took 1.64x as long for 4x the rows at 1 season (80.5 to 131.8 s) and 4.45x at
  3 seasons (57.9 to 257.8 s): once the fixed cost is paid, it grows at least linearly, with the single-partition
  prior windows and the many small partitions as the costs that can grow faster (Known issue of Step 6).
- The cells at 1 season x16 and 5 seasons x4 are in the supplement below.

#### Partitioning and caching (`results/partitioning.csv`, `results/partitioning_speedup.png`)
Master `local[4]`, 1 warm-up + 5 timed runs, 84 rows. Every result checksum matches across layouts, variants and
file systems. Speed-ups are against the simplest baseline (file://, unpartitioned, no cache) unless stated;
"same fs" is against unpartitioned without cache on the same file system.

- **Pruning helps the narrow lap queries, mostly on file://.** `partitionBy(season, round)`, laps x16, no cache:

  | Query | file:// | HDFS |
  |---|---|---|
  | one race, time | 0.164 to 0.095 s (1.74x) | 0.127 to 0.106 s (1.20x same fs; 1.56x vs simplest) |
  | one race, input | 1.29 MB to 4 KB | 11.1 MB to 8.7 KB |
  | one season, time | 0.147 to 0.091 s (1.62x) | 0.121 to 0.120 s (1.005x same fs; 1.23x vs simplest) |
  | one season, input | 764 KB to 96 KB | 8.6 MB to 208 KB |

  The input bytes fall by a large factor on both file systems, but the time gain is the 1.6-1.7x on file:// and
  only up to 1.2x on HDFS. All these queries run in 0.1-0.2 s, where the fixed per-query cost is a large share.
  For the 2024 telemetry (one season, 24 rounds) pruning gives no speed-up (0.93-1.04x), although the one-race
  input falls from 1.7 MB to 27 KB on file://.
- **Full scans and joins get slower on the partitioned layout** because it has many small files: laps
  `full_scan` and `join_agg` at 0.30x and 0.32x on file://, and 0.14x and 0.12x on HDFS against the simplest
  baseline (0.145x and 0.109x against unpartitioned on the same HDFS). HDFS is the worst case: each small file
  is a separate open and read through the namenode and datanode (the mechanism is not measured separately).
- **Caching helps only the small lap queries.** Unpartitioned file:// laps: `cache()` gives 1.41x (one race) and
  1.25x (one season), about 0.03-0.05 s per query, while `cache()` + `count()` itself takes 1.13 s, so it pays
  back only after roughly 25-40 queries of that kind. It gives 1.00x on `full_scan` and 1.06x on `join_agg`
  (HDFS unpartitioned laps: 1.01-1.08x). On the partitioned layout it hurts the pruned queries (one race 0.160 s
  cached against 0.095 s uncached), because it caches everything, and it recovers only part of the full-scan
  loss (0.85x and 0.82x on file://, against 0.30x and 0.32x uncached).
- **Caching does not help telemetry**: 0.93-1.05x in every query, layout and file system, and materialising the
  cache costs about 4.3 s (4.25-4.40 s) against queries of 0.3-2.6 s.
- **`persist(DISK_ONLY)` is mostly slower than no caching.** On telemetry and on HDFS laps it is slower in every
  query (0.71-0.95x, same fs), with 4.5-4.7 s materialisation on telemetry. The exception is the small file://
  lap queries, unpartitioned: 1.29x and 1.33x (and 1.08x on the join, 0.89x on the full scan), with 1.08 s
  materialisation.

### Memory re-check at 6 GiB (2026-11-13)
After the app container went to 6144m, `build_s1_x4_local4` alone (run `bench_20261113T095530Z`)
peaked at 3.49 GiB container memory (tree RSS 3.61 GiB) against the 5.40 GiB guard, in 74.5 s. At
4 GiB the same cell had passed once at 3.55 GiB and been killed once at 3.64 GiB (guard 3.60 GiB).
api and web are stopped during the overnight grid so the full profile's limits stay within the VM.

### Smoke run and time estimate (2026-11-13; plumbing evidence only, one run per cell)
- Scalability (`bench_20261113T072050Z_smoke`, 11/11 cells ok): feature build at 1 season x1:
  `local[1]` 44.5 s, `local[2]` 37.0 s, `local[4]` 33.1 s, cluster 42.3 s; `local[4]` at x4 94.0 s.
  Telemetry, 5 rounds: 8.5 / 6.8 / 5.5 / 7.7 s. Peak app container memory 2.9-3.1 GiB at x1 and 3.55 GiB
  at x4 (guard 3.6 GiB). Earlier single runs of the same cell varied by about 20 %.
- Partitioning (`bench_20261113T072933Z_psmoke`, laps x1, 5 telemetry rounds): 84 rows, identical result
  checksums everywhere.
- Estimate (affine fit through x1 and x4, about 25 s fixed + 17 s per x1 of rows per build run):
  full grid ~119 h, so it is not run as is. **Proposed reduced grid (~6.3 h + ~0.5 h partitioning):**
  1/3/5 seasons x every master at x1 and x4; x16 for 1 season on `local[4]` and the cluster; no x64
  (local x4 at 1 season already peaks at about 3.6 GiB); 1 warm-up + 3 timed runs per cell:

      make experiments-spark RECORD=1 GRID_ARGS="--seasons 1 3 5 --factors 1 4 16 --masters local1 local2 local4 cluster --warmup 1 --repeats 3 --large-from 16 --large-repeats 3 --large-masters local4 cluster --large-seasons 1"

### StackOverflowError in six build cells, and the fix (2026-11-14)
**What failed.** Six build cells of the recorded run `bench_20261113T152740Z` failed 15–20 s after their start:
- `build_s1_x16_{local4, cluster}`: 384 races;
- `build_s5_x4_{local1, local2, local4, cluster}`: 456 races.

Each failed with `java.lang.StackOverflowError` at `o81/o99.filter`, at low memory. `build_s3_x4` (264 races) ran.

**Root cause.**
- `bench/workloads.feature_build` passes every race of its input as the targets of the unchanged
  `features.build.compute_features`.
- Through `etl/io.restrict` and its own `sel()`, that calls `etl/clean.select_races`.
- `select_races` built `reduce(|, [(season == s) & (round == r) ...])`, a left-deep chain with one `OR` per race.
- Spark 4 converts a Column recursively when it is used. The trace (`data/bench.log`) is the same 8-frame group
  of `ColumnNodeToExpressionConverter.apply` repeated until the stack ran out, at the `.filter` call, before
  any job.

Reproduced on local Spark with the old code: 264 race keys pass, 384 and 456 overflow.

**Fix.** `select_races` now uses one flat `struct(season, round) IN (...)`. Spark optimises it to an `InSet`,
and the expression depth no longer depends on the number of races.
- The rows kept are the same: a listed pair, or nothing for a null season or round.
- Partition pruning is kept: it is still a `PartitionFilter`.
- No JVM option (`-Xss`) was changed.
- With the fix: 456 and 2000 keys pass, and the whole `compute_features` runs on the real s5_x4 input
  (456 races) in the app container.

**Same result.** `compute_features` was run with the old and with the fixed `select_races` on the bench's
replicated inputs:
- s1_x4: 96 races;
- s3_x1: 68 races, where both versions work.

The order-independent checksum of every persisted feature table (9 tables; the map column hashed as sorted
entries) is identical. `undercut_attempts` is never read, as everywhere in the benchmark.

**Timing of two previously-OK cells** (`make bench-supplement` without `RECORD`, run
`bench_20261114T042747Z_supp`, same grid settings):

| Cell | Recorded median [IQR] | With the fix | output bytes | input / shuffle bytes |
|---|---|---|---|---|
| `build_s1_x4_local4` | 64.68 s [64.15, 70.63] | 54.03 s [49.44, 57.42] | identical | within 0.31 % |
| `build_s3_x1_local4` | 49.36 s [48.56, 51.80] | 43.71 s [42.20, 44.09] | identical | within 0.01 % |

Both are below their recorded IQR, so the fix itself makes the build faster:
- The predicate alone, filtering all nine cached input tables once, takes 5.38 s with the OR chain and 3.08 s with
  `IN` at 96 races (3.71 s against 2.19 s at 68 races). That includes analysing and optimising the deep tree.
- The build plans these filters again for several of its writes, so the per-run saving scales with the number of
  races.
- Part of the difference may still be the environment: the unaffected prep cell also ran faster (27 s against
  93 s).

**Consequence for the results.** Cells timed with the fix are not strictly comparable with cells timed before
it: about 10 s per run at 96 races, less at fewer races. The supplementary rows say so in their `note`.

### The build plot (`results/scalability_build.png`)
It shows the two axes separately, from the same rows (`bench/report.plot_build_axes`):
- (a) real seasons 1/3/5 at x1;
- (b)–(d) replication x1/x4/x16 for each season count, with a shared y.

Each master is one line: the median, with IQR bars. A hollow marker is a merged re-run. Cells without a timing
are marked in the panel and named in the note. The plot was redrawn with `make bench-replot RECORD=1`, and only
that file changed.

### Supplementary re-run of the six cells (run 2026-11-14)
Run `bench_20261114T052031Z_supp` (`make bench-supplement RECORD=1 SUPPLEMENT_OF=bench_20261113T152740Z ...`, api
and web stopped): 8 cells, the 2 prep cells and the 6 build cells that had failed with the StackOverflowError, with
the recorded grid's settings (1 warm-up, 3 timed runs). Its 6 rows are in `results/scalability.csv` (`rerun_of`
and `note` set; the original failed rows stay). Log: `data/bench_supp.log` (not committed).

| Cell | Result | Median [Q1, Q3] | App container peak |
|---|---|---|---|
| `build_s1_x16_local4` | `aborted_memory_guard` (5.46 GiB) | none | 5.46 GiB |
| `build_s5_x4_local1` | `aborted_memory_guard` (5.50 GiB) | none | 5.50 GiB |
| `build_s5_x4_local2` | `aborted_memory_guard` (5.56 GiB) | none | 5.56 GiB |
| `build_s5_x4_local4` | `aborted_memory_guard` (5.44 GiB) | none | 5.44 GiB |
| `build_s1_x16_cluster` | ok | 175.6 s [169.6, 186.7] | 5.20 GiB |
| `build_s5_x4_cluster` | ok | 213.6 s [211.2, 216.6] | 2.48 GiB |

(The guard is 5.40 GiB of the 6 GiB container; the figures in the reasons are the values at the kill.)

**Read this as a single-host scale-out observation, not a speed claim.** After the fix the build ran in the
standalone cluster at both of the largest sizes, but all four local[N] cells at those sizes were stopped by the
memory guard. In local mode the driver and all task execution share one JVM in the app container, so the
container is what fills up. In the cluster the executors live in the worker containers, and the driver
container holds the driver only. That explains `s5_x4` clearly (2.48 GiB in the app container). It explains
`s1_x16` only partly: its driver container also reached 5.20 GiB, 0.2 GiB under the guard, and the lowest host
`MemAvailable` was 1.51 GiB against a 1.5 GiB guard. The cluster cell finished with very little margin. Nothing
here says the cluster is faster than local mode, because the local cells at these sizes have no timing.

**Comparability.** The re-run cells use the flat `IN` filter; the original cells use the OR chain (about 10 s
per run faster at 96 races, see above). Compare the re-run cells only with each other, not with the neighbours
in the plot. The two cluster medians (175.6 s at 1 season x16, 213.6 s at 5 seasons x4) differ in both rows and
races, so they are two points, not a scaling line.

**Executor loss in both completed cluster cells.** `data/bench_supp.log` records
`Lost executor 0 ... Executor heartbeat timed out` during `build_s1_x16_cluster` (05:35:34, heartbeat silent for 169.4 s) and
during `build_s5_x4_cluster` (06:21:34, silent for 166.7 s). Spark replaced the executor and the cells finished, but
each median may include recovery time. The IQRs (17.1 s and 5.4 s) do not show it separately.

The final plot footnote names the four aborted cells and says the first attempt failed with the
StackOverflowError (fixed). Hollow markers in the plot are the two merged re-runs.

# Step 16: final experiments

Traceability: #30 (ablation), #31 (replay one race, event-to-result delay), #35 (streaming latency),
#36 (event throughput), #37 (Redis latency at streaming rate), #38 (resource usage). Model-side
results (paired ablation intervals, ORACLE) are read in `docs/model_results.md`.

## Experiments and how to run them

| Experiment | Command | Runtime (this host) | Outputs | Can show | Cannot show |
|---|---|---|---|---|---|
| Paired ablation intervals (#30) | `make ablation-full RECORD=1` (host precheck, then the app container) | 1 min 45 s | `data/ablation/<run>/` (per-attempt scores, `exact_match.csv`, self-check); RECORD: `results/ablation_paired.{csv,png}`, README section | Feature-set differences on 2025 with one shared set of race resamples, from the frozen 9f models; it stops before any interval unless all 720 committed `model_eval` rows reproduce exactly | Significance (none claimed); any change of the primary set or the deployed model; GBT differences ≤ 0.043 PR-AUC (row order alone, spec v2 §5) |
| ORACLE on development data (#30) | `make oracle-dev-full RECORD=1` (app container) | 2 min 49 s | `data/oracle/<run>/`; RECORD: `results/model_dev_{eval,loso}_oracle{,_G5.0_k3}.csv` | How much the realised out-lap delta adds on 2024 validation and 2021-2024 LOSO | Anything about 2025 (never loaded) or a deployable model: the input is **not usable at decision time** |
| Streaming replay suite (#31, #35, #36, #38) | `make stream-latency-suite RECORD=1` (host, sample lake, core profile only) | 28 min 28 s | `data/demo/<suite>/`; RECORD: `results/streaming_latency_{s10,s60,s1w900}.csv`, `stream_{batches,progress}_{tag}.jsonl`, `streaming_latency_summary.csv`, `streaming_throughput.csv`, `streaming_resources.csv`, README sections | Per-hop delay (Kafka + trigger, batch work, Redis Pub/Sub, API SSE) and its tail at 1x/10x/60x, the first (prior-loading) batch apart, rates and peak resources on this host | Network costs between machines (one host, one clock), real 2025 data, races other than 2024 R5 |
| UI render segment (#35) | `make api-up DEBUG=1 UI_LATENCY_CSV=data/demo/ui_latency/ui_latency.csv`, web container stopped, then `make stream-latency-ui RECORD=1` | 2 min 9 s (incl. the dashboard build) | `data/demo/<suite>_ui/`; RECORD: `results/ui_latency.csv`, `ui_latency_summary.csv`, `stream_*_s60ui.*`, `ui_streaming_*.csv` | SSE send → browser paint for the events the page sampled | Every event: the page posts at most 50 samples at once and drops ids beyond that (a subset); one browser on the same host |
| Redis latency, quiet stack (#37) | `make redis-latency RECORD=1` | 3 min 31 s | default `data/redis_latency/<stamp>/`; RECORD: `results/redis_latency_quiet.csv`, README section | Write/read latency of the serving contract at 1/10/60 batches/s on a quiet host | Behaviour under the streaming job's load (it runs alone) |
| Regenerable generators (all) | `make experiments` (app container) / `make experiments-sample` (host, SAMPLE) | 19 min 26 s / 1 min 27 s | `data/experiments/<run_id>/` only (`results/` is never written); `summary.txt` | Whether each committed non-2025 output is reproduced byte for byte (OK / DIFF / MISSING / NEW) | Generators that open 2025 or refuse to overwrite (skipped, printed); measurements (skipped, printed) |

`make experiments` runs, each with its working directory set to the sandbox (its own `./results`,
`./models`, `./data`): `model-dev-full`, `model-dev-tyre-full`, `model-dev-ti-full` (both settings),
`model-dev-permutation-ti-full`, `oracle-dev-full`, `whatif-consistency-full` (without RECORD: no 2025 row) and
`stream-parity-full`. It never runs `model-test-full`, `whatif-consistency-full RECORD=1`, `ablation-full`
(they open 2025 and refuse to overwrite), `model-test-summary` (covered by a unit test) or the measurements
(`experiments-spark`, the streaming suite, the UI run, Redis); it prints each with its command. A what-if
check that fails (the pre-registered ordering cell, exit 1) is a recorded result, not a crash.
`make experiments-sample` runs `etl-sample`, `features-sample`, `model-eval-sample`, `sim-tyre-model-sample` and
`whatif-consistency-sample` on the host against a sandbox copy of the sample lake (so `data/lake` is not
rewritten), stamped SAMPLE.

## `make experiments` (full lake), run `exp_20261114T105954Z`

19 min 26 s in the app container (generator times: `model-dev-full` 211 s, `model-dev-tyre-full` 227 s,
`model-dev-ti-full` 247 s, `model-dev-permutation-ti-full` 204 s, `oracle-dev-full` 161 s,
`whatif-consistency-full` 17 s, `stream-parity-full` 98 s). `results/` was not written.

- **66 OK, 2 DIFF, 0 MISSING.** Every development model file of `real`, `real_tyre` and `real_ti` (CSV, JSON and
  PNG, both settings), both permutation files, the four ORACLE files and `stream_parity_full.csv` are
  byte-identical to the committed files.
- The two DIFFs are both what-if outputs, and they are expected. Without RECORD the what-if run has no 2025
  held-out row, and the committed `whatif_consistency.csv` and `.png` have it. With the 2025 rows dropped, the
  committed CSV is byte-identical to the sandbox CSV (OK). The PNG draws the 2025 row, so it differs.
- `whatif-consistency-full` exited 1 because of the pre-registered ordering check (Step 15 Known issue).
  `make experiments` counts that as a recorded result, not a crash.

## Streaming latency and throughput on a quiet stack (#31, #35, #36, #38)

**Setup.**
- 2024 R5 from the sample lake on the host: real Jolpica laps and pit stops, synthetic tyres. It has 1,113
  events, and 1,252 messages are published (laps, pit_in, pit_out, 141 undercut alerts, session_end).
- Only the core profile was running (kafka, redis, api, web). HDFS, Spark and the app container were stopped
  for these runs and restored afterwards with `make up-full`.
- Producer, stream job (`local[2]`, 500 ms trigger), Pub/Sub subscriber and SSE client all run on the WSL
  host, and the API runs in its container. All of them share one kernel clock.
- Suite `streamlat_20261114T112159Z`, 28 min 28 s, three runs one after another:
  - `s10`: 10x, the full race;
  - `s60`: 60x, the full race;
  - `s1w900`: 1x, the first 900 wall seconds.
- Every message of every run was seen on both Pub/Sub and SSE (1252/1252, 1252/1252, 232/232).

**Latency (ms), steady state, without the first race batch**
(`results/streaming_latency_summary.csv`, scope `steady`):

| Run | produced → batch p50 / p95 / p99 | batch → Redis write p50 / p99 | Redis → Pub/Sub p50 / p99 | Redis → SSE client p50 / p99 | end to end (Pub/Sub) p50 / p95 / p99 | n |
|---|---|---|---|---|---|---|
| s10 | 780.5 / 1222.9 / 1584.1 | 4 / 12 | 1 / 6.6 | 2 / 8 | 786.5 / 1227.0 / 1586.5 | 1242 |
| s60 | 790 / 1454 / 1752 | 5 / 10 | 1 / 3 | 2 / 5 | 798 / 1461.5 / 1757 | 1231 |
| s1w900 | 726 / 1037.5 / 1113.7 | 1 / 5 | 1 / 4.7 | 2 / 5.4 | 729 / 1039 / 1115.4 | 231 |

**The first race batch.** This batch loads the race's pit-loss and out-lap priors with Spark (scope
`first_batch`):

| Run | rows | batch → Redis write | end to end p50 | all rows end to end p99 |
|---|---|---|---|---|
| s10 | 9 | 1326 ms | 2825 ms | 2871.5 ms |
| s60 | 20 | 1310 ms | 3315.5 ms | 3287.5 ms |
| s1w900 | 1 | 1406 ms | 3380 ms | 1426.5 ms |

- **The trigger and the Kafka hop dominate.** About 0.73–0.80 s at the median is producer → micro-batch start:
  the 500 ms trigger interval, the Kafka read and Spark planning.
- **The serving hops are milliseconds.** The batch's work is 1–5 ms at the median, Redis → Pub/Sub 1 ms, and
  Redis → API → SSE client 2 ms, all under 13 ms at p99 in the steady state.
- **The tail grows with the replay speed.** The steady-state p95 is 1.04 s at 1x, 1.23 s at 10x and 1.46 s at
  60x: faster replay puts more events behind each trigger.
- **The first race batch costs about 1.3–1.4 s more**, once per race, because it loads the priors. It is the
  whole p99 of the "all" rows at 10x and 60x (2.9 s and 3.3 s against 1.6 s and 1.8 s without it).
- **Run-to-run comparison.** Step 12's recorded 60x run (other services up) had end-to-end p50 814 / p95 1481 /
  p99 3303 ms. The quiet 60x run has p50 807 / p95 1510.5 / p99 3287.5 ms, so the numbers agree within run-to-run
  variance. Over the three 60x runs of this step (an unrecorded smoke run, s60 and s60ui), the steady-state
  end-to-end p50 was 841, 798 and 813 ms.

**Throughput** (`results/streaming_throughput.csv`): received messages/s, lap messages/s and produced events/s.

| Run | messages/s | lap messages/s | produced events/s |
|---|---|---|---|
| s10 | 2.08 | 1.71 | 2.07 |
| s60 | 12.71 | 10.46 | 12.43 |
| s1w900 | 0.27 | 0.21 | 0.27 |

The received rate equals the produced rate at every speed, so no backlog builds up. At 60x this replay offers
only about 12 events/s, so these runs do not show the pipeline's maximum throughput. That would need a
synthetic load test, which was not run.

**Resources** (`results/streaming_resources.csv`; peaks per run window):

| Run | host job tree (demo + stream-job JVM + producer): peak RSS / peak CPU / mean CPU | kafka | api | redis | web |
|---|---|---|---|---|---|
| s10 | 1.85 GiB / 494 % / 23 % | 639 MiB, 170 % | 82 MiB, 8.7 % | 28 MiB, 2.7 % | 110 MiB, 12.7 % |
| s60 | 1.62 GiB / 502 % / 70 % | 641 MiB, 171 % | 73 MiB, 7.6 % | 23 MiB, 2.7 % | 109 MiB, 9.9 % |
| s1w900 | 1.63 GiB / 511 % / 11 % | 642 MiB, 166 % | 73 MiB, 7.7 % | 27 MiB, 2.8 % | 132 MiB, 13.6 % |

- CPU % is per core (100 % = one core).
- The host job's peaks come from the JVM and Spark start-up, and its mean depends on the run's length.
- The kafka CPU peak (about 170 %) is a `docker stats` sample at start-up.
- Lowest host `MemAvailable` was 8.05 GiB of about 14 GiB during the suite, and 7.80 GiB during the UI run.

**UI render segment** (suite `streamlat_20261114T115429Z_ui`; `results/ui_latency.csv`,
`ui_latency_summary.csv`).

Setup:
- One more 60x run (`s60ui`) with the dashboard open in headless Chromium: a Next.js production build with
  `NEXT_PUBLIC_DEBUG_LATENCY=1` on the host (the web container was stopped).
- The API was recreated with `ENABLE_DEBUG=1` and its samples written to `data/demo/ui_latency/`. Afterwards
  the API was recreated with its default settings and the web container was started again.

| Segment | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| API SSE send → browser paint | 1252 | 28 ms | 37 ms | 40 ms | 41 ms |
| Redis write → browser paint | 1251 | 34 ms | 41 ms | 293 ms | 293 ms |
| produced → browser paint | 1251 | 855 ms | 1519.5 ms | 3443.5 ms | 3579 ms |

- **The UI samples are a subset by design.** The page posts at most 50 samples at once and drops ids beyond
  that, and all ids applied in one render share one paint time (two animation frames after the update).
- At 60x only about 12 events/s arrive, so nothing was dropped in this run: 1252 samples for 1252 messages.
- 1110 lap and pit samples join the harness rows by `event_id`. Alerts and `session_end` carry ids made by the
  API, so they cannot be joined that way; their server-side timings come from the API row itself.
- The host Next.js server and the browser are not in the resource table (`results/ui_streaming_resources.csv`
  covers the replay and the containers).
- Correction: the first `ui_latency_summary.csv` written by the recorded run divided by the lap and pit count
  (1110) and showed `subset_fraction` 1.128. It was rebuilt from the same raw samples with
  `streaming_runs.resummarise_ui` (no new measurement). `ui_latency.csv` was byte-identical, and the summary
  now shows 1.0.

## Redis latency: first run and quiet rerun (#37)

| Rate | Operation | First run (Step 11, 2026-11-12; noisy, other services up) p50 / p95 / p99 ms | Quiet rerun (Step 16, 2026-11-14) p50 / p95 / p99 ms | Achieved rate / late ticks (first → quiet) |
|---|---|---|---|---|
| 1/s | write (20-driver batch) | 1.31 / 2.04 / 2.24 | 1.69 / 2.57 / 2.59 | 1.00/0 → 1.00/0 |
| 1/s | HGETALL | 0.25 / 0.49 / 0.63 | 0.23 / 0.54 / 0.71 | |
| 1/s | ZRANGE | 0.28 / 0.52 / 0.68 | 0.24 / 0.34 / 0.36 | |
| 10/s | write | 2.06 / 3.48 / 25.75 | 1.61 / 2.64 / 3.06 | 10.00/8 → 10.00/0 |
| 10/s | HGETALL | 0.41 / 0.67 / 1.42 | 0.22 / 0.52 / 0.73 | |
| 10/s | ZRANGE | 0.50 / 0.82 / 4.10 | 0.23 / 0.36 / 0.43 | |
| 60/s | write | 2.10 / 9.20 / 19.77 | 1.21 / 1.59 / 1.78 | 51.09/2876 → 60.00/0 |
| 60/s | HGETALL | 0.44 / 1.09 / 8.17 | 0.18 / 0.32 / 0.41 | |
| 60/s | ZRANGE | 0.54 / 1.70 / 8.77 | 0.24 / 0.34 / 0.41 | |
| — | `redis-cli --latency` PING in the container (min / avg / max) | 0.053 / 0.282 / 10.02 | 0.068 / 0.219 / 1.33 | |

- Files: the first run is `results/redis_latency.csv` (kept byte-identical), and the quiet rerun is
  `results/redis_latency_quiet.csv` (`make redis-latency RECORD=1`).
- **Different hosts.** The Step 11 fingerprint was taken under the old WSL configuration (6 logical CPUs,
  9.7 GiB). The rerun used the current one (12 logical CPUs, about 14 GiB). The two runs differ in host as well
  as in background load, so the comparison is not only "quiet against noisy".
- On the quiet stack every rate met its schedule (60.00/s, 0 late ticks), and every p99 is below 3.1 ms.
- The tail outliers of the first run (write p99 19.8–25.8 ms, 2876 late ticks at 60/s) are therefore load on
  that host, not Redis.
- These numbers are client-side over the Docker port mapping on one host. They do not include a network.

## `make experiments-sample`, run `exp_20261114T115905Z_SAMPLE` (SAMPLE stamp)

1 min 27 s on the host, against a sandbox copy of the sample lake (`data/lake` is not rewritten). Result:
**7 OK, 1 OK (metadata differs), 0 DIFF, 0 MISSING, 2 NEW**, exit 0.

- `undercut_counts_sample.csv` and all six `model-eval-sample` outputs are byte-identical to the committed
  files.
- `models/sim/tyre_model_sample.json` is `OK (metadata differs)`.
  - Comparison rule: for outputs declared with metadata fields, the string values of `created_utc` and `git_sha`
    are masked, and the rest of the file is compared byte for byte. A file that then matches is reported as
    `OK (metadata differs)` with the ignored fields named. Any other difference is still `DIFF`, including
    `git_dirty`.
  - `models/` is not committed, so the comparison is with the local file.
- The two what-if sample files are `NEW`: Step 15 never committed sample what-if outputs.
- The first sample run (`exp_20261114T105801Z_SAMPLE`) showed `model_eval_sample.csv` and
  `calibration_sample.csv` as DIFF.
  - Cause: the committed files dated from 2026-11-11, before the `decision_threshold` column and the
    `ece_5bin_*` calibration columns existed.
  - They are sample smoke outputs, not frozen findings. They were regenerated with the normal target
    `make model-eval-sample`, which always writes `results/*_sample` (it has no RECORD switch).
  - Only those two files changed. The other four sample outputs were rewritten byte-identically, and no
    full-data or 2025 file changed.
