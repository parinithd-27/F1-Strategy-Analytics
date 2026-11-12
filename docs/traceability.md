# Traceability matrix: proposal commitments -> build step -> file

| # | Commitment (proposal) | Step | File or module |
|---|---|---|---|
| 1 | Partitioned multi-season data lake (timing, results, pit stops, telemetry) | S4, S5 | `lake/writer.py`, `lake/paths.py`, `etl/clean.py` |
| 2 | Raw data in HDFS | S2, S4 | `docker-compose.yml` (hdfs profile), `ingest/cli.py --sink hdfs` |
| 3 | Clean Parquet partitioned by season and race | S5 | `etl/clean.py`, `lake/schemas.py` |
| 4 | FastF1 ingestion (timing and telemetry: speed, throttle, brake, gear, RPM) | S3, S4, S4C | FastF1-derived data via TracingInsights (FastF1 itself needs an F1 TV login since Oct 2026): `ingest/tracinginsights_loader.py`, `ingest/tracinginsights_parse.py`, `ingest/tracinginsights_pins.json`; `ingest/fastf1_loader.py` kept as the optional direct path. Audit: `docs/data_source_audit.md` |
| 5 | Jolpica ingestion (race, driver, constructor, qualifying, pit stop, lap) | S3, S4 | `ingest/jolpica_client.py`, `ingest/ratelimit.py` |
| 6 | Scope 2021-2025, ~50-100+ races | S4 | `ingest/cli.py --seasons 2021-2025`, `data/manifest.json` |
| 7 | Entities: Race, Driver, Lap, Tyre, Pit Stop, Position, Gap, Telemetry | S5, S6 | `lake/schemas.py`, `docs/data_dictionary.md` |
| 8 | Spark distributed ETL, joins, aggregations | S6 | `etl/df_jobs.py` |
| 9 | RDD API demonstrated | S6 | `etl/rdd_jobs.py` |
| 10 | DataFrame API demonstrated | S6 | `etl/df_jobs.py` |
| 11 | Spark SQL demonstrated | S6 | `etl/sql/*.sql`, `etl/sql_jobs.py` |
| 12 | Feature tables: gap change, tyre age, out-lap pace, pit loss | S6, S7, S4C | `features/gaps.py`, `stints.py`, `outlap.py`, `pitloss.py`, `telemetry_agg.py`; real tyre age/compound and telemetry inputs: `etl/clean_tracinginsights.py` (`stints_tracinginsights`, `telemetry_lap_real`), `features/telemetry_agg_ti.py` |
| 13 | Undercut detection | S8 | `features/undercut.py`, `docs/label_definition.md` |
| 14 | MLlib undercut-success model | S9 | `model/train.py` |
| 15 | Explain main factors | S9 | `model/explain.py` |
| 16 | Chronological split (train 2021-23, validate 2024, test 2025) | S9 | `model/dataset.py` |
| 17 | F1 vs. majority-class baseline | S9 | `model/baselines.py`, `experiments/model_eval.py` |
| 18 | Probability calibration | S9 | `model/calibrate.py` |
| 19 | Kafka carries race events | S12 | `streaming/producer.py`, topic `f1.race.events` |
| 20 | Structured Streaming replay and opportunity detection | S12 | `streaming/stream_job.py`, `streaming/detector.py` |
| 21 | Redis hash `race:{season}:{round}:driver:{driverId}` with 9 exact fields | S11 | `serving/keys.py`, `serving/redis_store.py` |
| 22 | Sorted set `race:{season}:{round}:order`, score = position | S11 | `serving/redis_store.py` |
| 23 | Pub/Sub `race:{season}:{round}:events` (lap, pit, undercut alert) | S11, S12 | `streaming/redis_sink.py` |
| 24 | Namespaced keys with TTL after session ends | S11 | `serving/redis_store.py` (`expire_race`) |
| 25 | Redis-backed dashboard: state, gaps, tyres, events, predictions, comparison | S13, S14, S15 | `api/`, `web/` |
| 26 | What-if: pit now / pit next lap / stay out | S15 | `sim/race_sim.py`, `sim/whatif.py`, `api/routes/whatif.py` |
| 27 | Experiment: runtime for 1/3/5 seasons | S10 | `experiments/scalability.py` |
| 28 | Experiment: unpartitioned vs. partitioned, with and without caching | S10 | `experiments/partitioning.py`; large input: raw 2024 telemetry samples `raw/tracinginsights/telemetry/season=2024/round=/driver=/` (S4C) |
| 29 | Experiment: model evaluation (chronological split) | S9 | `experiments/model_eval.py` |
| 30 | Experiment: ablation with vs. without telemetry/out-lap features | S16 | `experiments/ablation.py`; telemetry input `clean/telemetry_lap_real` (all 2021-2025 races, S4C) |
| 31 | Experiment: replay one race, event-to-result delay | S12, S16 | `experiments/streaming_latency.py` |
| 32 | Experiment: what-if comparisons from the same state | S15 | `experiments/whatif_consistency.py` |
| 33 | Metric: Spark runtime and scaling trend | S10 | `results/scalability.*` |
| 34 | Metric: speed-up from partitioning and caching vs. baseline | S10 | `results/partitioning.*` |
| 35 | Metric: streaming latency | S16 | `results/streaming_latency.*` |
| 36 | Metric: event throughput | S16 | `results/streaming_latency.*` |
| 37 | Metric: Redis read/write latency at streaming rate | S11, S16 | `experiments/redis_latency.py` |
| 38 | Metric: resource usage | S10, S16 | `experiments/_resources.py` |
| 39 | Metric: calibration | S9 | `results/calibration.*` |
| 40 | Metric: useful and consistent what-if comparisons | S15 | `results/whatif_consistency.*` |
| 41 | Metric: interactive dashboard | S14, S15 | `web/` |
| 42 | Reproducible end-to-end pipeline | S17 | `Makefile`, `docs/reproduce.md`, `.github/workflows/ci.yml` |
| 43 | Timeline W1-W8 | all | build plan step headers |
| 44 | Report, docs, demo rehearsal | S18 | `docs/report/`, `docs/demo_script.md` |
