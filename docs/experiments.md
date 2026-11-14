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
- **Trends only.** Read the results as relative trends with their IQR: how a cell scales against
  its neighbours and which choices help (partition pruning, caching). They are not absolute
  throughput figures to compare with other systems. Run-to-run variance on this WSL2/Docker Desktop
  host is real (see the Step 11 Redis notes); every cell reports its IQR, min and max.

## Results
The recorded run writes `results/scalability.csv`, `results/partitioning.csv` and the three PNGs,
and appends the environment fingerprint (git sha, CPU, memory, container limits, Spark and Python
versions, topology, grid parameters) to `results/README.md`. The smoke run below is plumbing
evidence only (one repetition, no warm-up) and is not a result.

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

### Supplementary re-run of the six cells (not yet run)
Stop api and web first, as for the overnight grid:

    make bench-supplement RECORD=1 SUPPLEMENT_OF=bench_20261113T152740Z CELLS="build_s1_x16_local4 build_s1_x16_cluster build_s5_x4_local1 build_s5_x4_local2 build_s5_x4_local4 build_s5_x4_cluster"

**Cells.** 8 in all: 2 prep cells (they rebuild the replicated input, which was deleted) and the 6 build cells.
The settings come from the recorded grid: 1 warm-up and 3 timed runs.

**Estimate.** The recorded s3_x4 times, scaled linearly in lap rows (298k → s1_x16 425k, s5_x4 499k):
- s5_x4: about 430 / 390 / 335 / 270 s per run on local[1] / local[2] / local[4] / the cluster;
- s1_x16: about 285 / 230 s on local[4] / the cluster.

With 4 runs per cell and the two preps, that is about 2.3 h; allow 3 h, since growth was superlinear in places and
the fix makes each run somewhat faster. `build_s1_x16_local4` may reach the memory guard (s3_x4 local[4]
peaked at about 4.1 GiB container memory, guard 5.4 GiB). If it does, it is recorded as `aborted_memory_guard`,
not hidden.

