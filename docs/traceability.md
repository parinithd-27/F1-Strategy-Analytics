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
| 12 | Feature tables: gap change, tyre age, out-lap pace, pit loss | S6, S7, S4C | `features/gaps.py`, `stints.py`, `outlap.py`, `pitloss.py`, `telemetry_agg.py`; real tyre age/compound and telemetry inputs: `etl/clean_tracinginsights.py` (`stints_tracinginsights`, `telemetry_lap_real`), `features/telemetry_agg_ti.py`; model inputs: feature set `real_ti` (`model/features.py`, `model/dataset.with_ti_inputs`, development mode) |
| 13 | Undercut detection | S8 | `features/undercut.py`, `docs/label_definition.md` |
| 14 | MLlib undercut-success model | S9 | `model/train.py` |
| 15 | Explain main factors | S9 | `model/explain.py` |
| 16 | Chronological split (train 2021-23, validate 2024, test 2025) | S9 | `model/dataset.py` |
| 17 | F1 vs. majority-class baseline | S9 | `model/baselines.py`, `experiments/model_eval.py` |
| 18 | Probability calibration | S9 | `model/calibrate.py` |
| 19 | Kafka carries race events | S12 | `streaming/producer.py` (race replay at a speed factor, acks=all, idempotent, key=driver_id), `streaming/events.py` (schema, JSON, Spark StructType), topic `f1.race.events`; tests `test_producer.py`, `test_events.py`, `tests/integration/test_streaming.py` |
| 20 | Structured Streaming replay and opportunity detection | S12 | `streaming/stream_job.py` (Kafka readStream, trigger 500 ms, foreachBatch), `streaming/processor.py`, `streaming/live_state.py`, `streaming/live_features.py`, `streaming/priors.py` (shared rule `features/undercut_rule.py`, calibrated LR `model/lr_scorer.py`), `streaming/detector.py`; parity `streaming/parity.py`, `tests/unit/test_stream_parity.py`, `make stream-parity-full` (`results/stream_parity_full.csv`) |
| 21 | Redis hash `race:{season}:{round}:driver:{driverId}` with 9 exact fields | S11 | `serving/keys.py` (`driver_key`, `DRIVER_FIELDS`), `serving/redis_store.py` (`upsert_driver_state`, `encode_state`/`decode_state`, sync `RaceStore` + async `AsyncRaceStore`); tests `tests/unit/test_keys.py`, `tests/unit/test_redis_store.py`, `tests/integration/test_redis_store.py` |
| 22 | Sorted set `race:{season}:{round}:order`, score = position | S11 | `serving/keys.order_key`, `serving/redis_store.py` (`set_order`, `get_order`) |
| 23 | Pub/Sub `race:{season}:{round}:events` (lap, pit, undercut alert) | S11, S12 | `serving/keys.events_channel`, `serving/redis_store.publish_event` (JSON; types lap, pit_in, pit_out, undercut_alert, session_end); S12 caller `streaming/redis_sink.py` (one `write_batch` per micro-batch and race, `end_session` on `session_end`, no write after it) |
| 24 | Namespaced keys with TTL after session ends | S11 | `serving/redis_store.py` (`expire_race`: SCAN `race:{s}:{r}:*` + EXPIRE `REDIS_TTL_AFTER_SESSION_S`; `end_session`; safety TTL `REDIS_SAFETY_TTL_S` on every write, `config.py`) |
| 25 | Redis-backed dashboard: state, gaps, tyres, events, predictions, comparison | S13, S14, S15 | `api/`, `web/` |
| 26 | What-if: pit now / pit next lap / stay out | S15 | `sim/race_sim.py`, `sim/whatif.py`, `api/routes/whatif.py` |
| 27 | Experiment: runtime for 1/3/5 seasons | S10 | `experiments/scalability.py` |
| 28 | Experiment: unpartitioned vs. partitioned, with and without caching | S10 | `experiments/partitioning.py`; large input: raw 2024 telemetry samples `raw/tracinginsights/telemetry/season=2024/round=/driver=/` (S4C) |
| 29 | Experiment: model evaluation (chronological split) | S9 | `experiments/model_eval.py` |
| 30 | Experiment: ablation with vs. without telemetry/out-lap features | S16 | `experiments/ablation.py`; telemetry input `clean/telemetry_lap_real` (all 2021-2025 races, S4C); development comparison `real` vs `real_tyre` vs `real_ti` (`make model-dev-tyre-full`, `make model-dev-ti-full`, `make model-dev-permutation-ti-full`; `model/permutation_dev.py`); leakage test `test_model_ti_inputs.py::test_ti_inputs_unchanged_when_future_laps_and_later_races_are_scaled_x100`. Development: 2024 validation, 2021-2024 LOSO. Test-season ablation (`real` primary, `real_tyre` and `real_ti` ablation, both settings) pre-registered in `docs/model_spec_v2.md` (tag `model-spec-v2`), run by `make model-test-full CONFIRM=yes`, guard `model/features.TEST_FEATURE_SETS` |
| 31 | Experiment: replay one race, event-to-result delay | S12, S16 | `experiments/streaming_latency.py`, `streaming/demo.py`, `streaming/latency.py`, `make stream-demo` (`results/streaming_latency.csv`, `stream_progress.jsonl`, `stream_batches.jsonl`) |
| 32 | Experiment: what-if comparisons from the same state | S15 | `experiments/whatif_consistency.py` |
| 33 | Metric: Spark runtime and scaling trend | S10 | `results/scalability.*` |
| 34 | Metric: speed-up from partitioning and caching vs. baseline | S10 | `results/partitioning.*` |
| 35 | Metric: streaming latency | S16 | `results/streaming_latency.*` |
| 36 | Metric: event throughput | S16 | `results/streaming_latency.*` |
| 37 | Metric: Redis read/write latency at streaming rate | S11, S16 | `experiments/redis_latency.py`, `serving/latency_bench.py`; `results/redis_latency.csv`, fingerprint in `results/README.md` |
| 38 | Metric: resource usage | S10, S16 | `experiments/_resources.py` |
| 39 | Metric: calibration | S9 | `results/calibration.*` |
| 40 | Metric: useful and consistent what-if comparisons | S15 | `results/whatif_consistency.*` |
| 41 | Metric: interactive dashboard | S14, S15 | `web/` |
| 42 | Reproducible end-to-end pipeline | S17 | `Makefile`, `docs/reproduce.md`, `.github/workflows/ci.yml` |
| 43 | Timeline W1-W8 | all | build plan step headers |
| 44 | Report, docs, demo rehearsal | S18 | `docs/report/`, `docs/demo_script.md` |
