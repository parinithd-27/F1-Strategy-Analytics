# F1 Strategy Analytics at Scale: Claude Code (Pro) Playbook and Build Plan — v2

**Version:** v2, 12 Nov 2026. Supersedes v1 (the original research report). v2 keeps the v1 structure but records what was actually built, what changed and why, what is frozen, and the exact prompts for the work that remains.
**Project:** IISc MTech big-data project, team Data Wranglers. Repo: github.com/parinithd-27/F1-Strategy-Analytics.
**How to read this:** Section 0 is the status and the decision log. Part A is how to drive Claude Code on a Pro plan. Part B is the build plan (as built, then remaining). Part C is the runbook of commands. Part D lists open items and known issues.

---

## 0. Status, decisions and frozen artefacts

### 0.1 Step status

| Step | What | Status |
|---|---|---|
| 0 | Environment, repo, CLAUDE.md, permissions | Done |
| 1 | Scaffold, tooling, CI | Done |
| 2 | Docker Compose (Kafka, Redis, HDFS, HttpFS, Spark standalone) | Done |
| 3 | Rate-limited Jolpica client, FastF1 loader (auth-failure path), ingest CLI | Done |
| 4A | Resumable season-range ingestion, manifest, HttpFS sink | Done: 119 of 119 items, 0 failed |
| 4B | Seeded synthetic generator and offline sample bundle (502 KiB) | Done |
| 4C | Replace FastF1 as the source of compounds, tyre life, race control, telemetry | Phase 1 (audit) done, go. **Phase 2 (loader and tables) in progress** |
| 5 | Clean zone (typed, partitioned by season/round, 28 quality checks) | Done |
| 6 | Spark RDD, DataFrame and SQL feature tables | Done |
| 7 | Telemetry aggregation and out-lap pace (+ team lineage map) | Done |
| 8 | Undercut attempts, labels, shared real-time rule | Done |
| 8b | Full-data run in HDFS mode (inside the `app` container) | Done |
| 8c | Red-flag fix, wet-running reporting split, early-lap rule L<5 | Done |
| 9 | MLlib model, baselines, Platt calibration, explanations | Done |
| 9b | Development mode that never opens the test season | Done |
| 9c | Saved deployment model and pure-Python LR scorer | Done (re-save for a clean git sha is optional) |
| 9d | Real tyre/telemetry features, development results only | Pending (after 4C) |
| 9e | Model spec v2 and freeze | Pending |
| 9f | The single 2025 test run | Pending (needs team sign-off) |
| 10 | Scalability, partitioning, caching benchmarks | Pending |
| 11 to 18 | Redis, streaming, API, dashboard, simulator, ablation, hardening, report | Pending |

### 0.2 What changed against v1 (decision log)

1. **FastF1 is no longer the data source.** Since mid-October 2026 FastF1 downloads need an F1 TV Access login, and nobody on the team has one. The replacement is the public TracingInsights season repositories (FastF1-derived JSON, no login), with OpenF1 as a cross-check. FastF1 stays as an optional loader for anyone with an account. Reason and evidence: `docs/data_source_audit.md`.
2. **Jolpica caps `limit` at 100, not 1000.** The bulk ingest therefore took about 3 hours, not 2 to 3 hours of 1000-row pages. `total` for laps counts lap timings, so pages can split a lap; the client merges by lap number.
3. **Much less labelled data than v1 assumed.** About 32 to 49 labelled attempts per season at the primary setting (v1 expected a few hundred). Confidence intervals are wide, and the report must say so.
4. **The label definition grew** (all changes decided from counts only, before any model result): an `in_pit` exclusion, a tighter lap-time neutralisation rule, red-flag detection before the pit-lap exclusion with a 3-lap restart period, wet running counted as neutralised, and an early-lap exclusion for stop lap L<5.
5. **The out-lap feature was decomposed** into stop time and pace loss, because the realised out-lap delta (about 15 to 17 s) mostly measured pit-stop time. Decision-time features use only an expected value from earlier races, with shrinkage; the realised value is allowed only in the oracle ablation.
6. **Extra steps and guards** were added: 8b, 8c, 9b, 9c, 4C, and refuse-to-overwrite guards on the test run.
7. **Discipline:** label and model specification are tagged and frozen before any test-season number is seen. The 2025 season is opened once.
8. **`.env` deny rule narrowed** to `.env` and `.env.local` so `.env.example` stays readable. The FastF1 token paths are also denied.
9. **Windows/WSL lessons** are in Part A6 and Part C.

### 0.3 Frozen artefacts and rules (do not change after seeing a test number)

| Artefact | Where | Notes |
|---|---|---|
| Label definition v1 | `docs/label_definition.md`, git tag `label-definition-v1` | Timing-only. Primary G=3.5 s, k=2. Sensitivity G=5.0 s, k=3. |
| Model specification v1 | `docs/model_spec.md`, git tag `model-spec-v1` | `real` feature set (21 inputs), LR and GBT grids, Platt calibration below 300 validation rows, fixed threshold 0.5 (commit 64739ca), race-cluster bootstrap (1000 resamples, seed 42), baselines as fixed references. |
| Test season | 2025 | Opened exactly once, by `make model-test-full CONFIRM=yes`. A crashed run may be rerun only if no test number was seen. |
| Output names | `results/model_eval_G3.5_k2.*`, `results/model_eval_G5.0_k3.*` | A test run refuses to overwrite existing files. |

Verify the tags exist with `git tag -l` (you should see `label-definition-v1` and `model-spec-v1`). If either is missing, create it before anything else (Part C).

### 0.4 Key numbers (as built)

| Item | Value |
|---|---|
| Races and laps (rows in the laps table) | 2021: 22 races, 23,688; 2022: 22, 23,529; 2023: 22, 24,386; 2024: 24, 26,574; 2025: 24, 26,657 |
| Ingest | 119 manifest items, 0 failed; 1,247 network calls, 252 retried after HTTP 429; raw zone 20.3 MB in HDFS |
| Labelled attempts, G=3.5, k=2 | 2021: 32, 2022: 32, 2023: 44, 2024: 49, 2025: 49 → train 108, validation 49, test 49 |
| Labelled attempts, G=5.0, k=3 | 2021: 48, 2022: 62, 2023: 74, 2024: 64, 2025: 60 → train 184, validation 64, test 60 |
| Class balance (G=3.5, k=2) | 2021: 8 of 32 positive; 2022: 11 of 32; 2023: 13 of 44; 2024: 16 of 49 |
| Quality report | PASS, 28 checks, 0 hard failures, 1 soft issue (5 disqualified drivers with 0 laps in results) |
| Unit tests | 649 passed at Step 9c; lint, format and mypy clean |

### 0.5 Development results (validation season 2024 only; seen before the freeze; NOT findings)

Primary setting G=3.5, k=2, `real` feature set, race-cluster 95% intervals:

| Model | F1 (positive) | PR-AUC | ROC-AUC | Brier | ECE (10 bins) |
|---|---|---|---|---|---|
| Calibrated LR | 0.600 [0.462, 0.737] | 0.594 [0.444, 0.817] | 0.794 | 0.168 | 0.060 |
| Calibrated GBT | 0.400 [0.100, 0.605] | 0.560 [0.356, 0.755] | 0.645 | 0.205 | 0.091 |
| Majority baseline | 0.000 | 0.327 | 0.500 | 0.221 | n/a |
| Rule baseline | 0.211 | 0.579 [0.379, 0.790] | 0.742 | 0.221 | n/a |

Leave-one-season-out PR-AUC (LR / GBT / majority / rule): hold-out 2021: 0.312 / 0.296 / 0.250 / 0.501; 2022: 0.422 / 0.514 / 0.344 / 0.633; 2023: 0.438 / 0.573 / 0.295 / 0.524; 2024: 0.479 / 0.547 / 0.327 / 0.579.

How to read these: the model beats the majority baseline, but the evidence so far does not show it beating the simple rule baseline (the intervals overlap, and the rule has the best or near-best PR-AUC in most folds). 2024 selects hyperparameters and fits the calibrator, so its numbers are optimistic. This is reported as it is; do not tune toward 2025.

### 0.6 Honest limitations to carry into the report

- About 108 training and 49 test attempts at the primary setting. No significance claim against the rule baseline.
- v1 is timing-only. Real compounds and telemetry (4C, 9d) are a pre-declared v2 extension.
- Wet and drying running is counted as neutralised, because Jolpica has no compounds and v1 uses no race-control data.
- Single-host scalability experiments show trends only, not cluster behaviour.
- Single-node HDFS gives integration, not locality or replication benefits.

---

## Part A — Driving Claude Code in VS Code on a Pro plan

### A1. Models and limits

| Alias | Resolves to | Notes |
|---|---|---|
| `opus` | Opus 5.5 | Account default; highest cost. |
| `sonnet` | Sonnet 5.5 | Daily coding. Default for implementation. |
| `haiku` | Haiku 5.5 | Tests, docs, boilerplate. |
| `opusplan` | Opus in Plan mode, Sonnet when executing | Best fit for Pro budgets. |
| `fable` / `best` | Fable models | Not included in Pro plan limits (pay-as-you-go credits). Avoid. |

- Pro has a five-hour session limit plus a weekly limit, shared across Claude chat, Claude Code and the IDE extension. Anthropic does not publish per-model multipliers; treat figures you see online as anecdotal.
- Recommended use: plan with `opusplan`, implement with Sonnet at medium effort, docs with Haiku or Sonnet at low effort. Use Opus at high effort only for the few hardest decisions (label definition, simulator design, streaming design).
- If `ANTHROPIC_API_KEY` is set, Claude Code bills that key instead of your subscription. Check with `[ -z "$ANTHROPIC_API_KEY" ] && echo ok`.
- Check `/status` for allocation; version-dependent details (model names, defaults) change often, so confirm in your installed version.

### A2. Effort levels

Levels: low, medium, high, xhigh, max. Use medium by default; high for debugging and the hardest design reviews; avoid xhigh and max on Pro. Set with `/effort`. Effort controls how much work Claude does on a turn, not what it knows; switch to a larger model if it tried hard and was still wrong. Keywords such as "ultrathink" are community-documented only; prefer `/effort high`.

### A3. Permission modes

| Mode | Use it for |
|---|---|
| Plan | Start of every step. Claude reads and plans; edits are blocked until you approve. |
| Edit automatically | Implementation you review in diffs. |
| Auto | Long implementation and test loops after the plan is approved. |
| Manual | Sensitive steps. |
| Bypass permissions | Do not use. |

Deny rules in `.claude/settings.json` hold in every mode. Claude Code will ask for permission for network calls it has no rule for; approve only the hosts the step names. Git is your real undo button: rewind does not undo Docker, HDFS or shell side effects.

### A4. Workflow for a long multi-session project

1. One step per session; `/clear` between steps; `/model` and `/effort` at the start; `/context` shows CLAUDE.md loaded.
2. Plan mode first, read the plan against the checklist in each step, approve, switch to Auto.
3. You run the verification commands and the commits (`git add -A`, commit, push). Check CI on GitHub.
4. Keep CLAUDE.md under 200 lines. `/compact <focus>` inside a long step. Reference files with `@path`; never ask Claude to read the whole repo.
5. Custom commands in `.claude/commands/` (`/step`, `/verify`, `/progress`, `/review`) and a Haiku `test-runner` agent are set up in Step 0.
6. After a limit or a shutdown: `/clear`, then "Read @CLAUDE.md and @docs/PROGRESS.md. Continue Step N from 'In progress'. Run the failing test first."
7. For constrained steps, state the guard rails in the prompt: what must stay byte-identical, what must never be read (2025 outcome values), what Docker commands are forbidden.

### A5. Living with Pro limits

Stretch the allowance by planning with Opus and implementing with Sonnet, scoping prompts to one module, running targeted tests while iterating and the full suite once at the end, and doing mechanical edits yourself. If you hit a limit: ask Claude to update `docs/PROGRESS.md`, commit work in progress to a WIP branch, wait for the reset, then resume from the progress file. Usage credits are the documented escape hatch; decide as a team, with a cap. Each team member should use their own Pro account on their own steps.

### A6. Windows setup and lessons learned

- Run everything in WSL2 Ubuntu with Docker Desktop's WSL backend; keep the repo in `~/code`, not `/mnt/c`.
- **Use real VS Code, not Cursor.** Cursor has no Microsoft WSL extension, so "WSL: Connect to WSL" does not exist there. Install VS Code on Windows, install the WSL extension with the full path to VS Code's `code.cmd`, then connect to WSL and open the repo. Install the Claude Code extension "in WSL: Ubuntu".
- **Know which shell you are in.** PowerShell commands (`Get-PSDrive`, `.wslconfig` edits) do not work in the Ubuntu terminal. For Windows C: space from Ubuntu use `df -h /mnt/c`.
- `.wslconfig` (in `%UserProfile%`): the machine has about 23.7 GB RAM. 10 GB was used for most of the project; raise to `memory=14GB`, `processors=12` before the Spark benchmarks, then `wsl --shutdown`.
- The `app` container has its own 1.5 GiB limit (`mem_limit: 1536m`); the WSL cap does not change it. The benchmarks need it raised.
- WSL's "927 GB remaining" in an HDFS report is the virtual disk's maximum, not real free space. The real limit is free space on Windows C: (`df -h /mnt/c`).
- Docker or WSL restarted unexpectedly several times (no OOM; cause unexplained). The `app` container has `restart: unless-stopped` but did not always return, so after any restart run `make up-full` and check `docker compose ps`. Data in volumes survives.
- Long runs go in `tmux` with a log (`tee -a`), with Windows set to never sleep while plugged in.
- CRLF: `.gitattributes` has `* text=auto eol=lf`.

---

## Part B — The build plan

### B1. Data sources (v2)

| Source | Provides | Access | Licence and terms |
|---|---|---|---|
| Jolpica-F1 | Results, qualifying, laps, pit stops, drivers, constructors, schedule | Free, no login. Limit 4 req/s and 500 req/h; page size capped at 100. | CC BY-NC-SA 4.0, non-commercial. The ingest used 400/h and still saw 252 HTTP 429 responses. |
| TracingInsights season repos (2021 to 2025) | Per-lap compound, tyre life, stint; race-control messages (SC, VSC, red flags with lap numbers); per-lap telemetry (18 channels); weather | Public GitHub, no login. Pin one commit SHA per season. | Repositories are MIT (2021 to 2024) and Apache-2.0 (2025) per the audit. Neither licence covers the underlying F1 data: download on demand, never commit, never redistribute. |
| OpenF1 | Stints (compound, lap range, tyre age at start), race control, car data | Free, no login; 2023 onwards only | Intended for educational, non-commercial use. Reported rate limits conflict (3 req/s and 30/min in one source, 30 per 10 s in another): check the docs. Used only as a cross-check. |
| FastF1 3.9.2 | Same data, directly from F1's servers | Needs an F1 TV Access login (tokens last about 4 days) | MIT code; F1 data not redistributed. Optional loader. |

**Why FastF1 is not the source.** FastF1 is a free open-source client; the data comes from F1's servers, which began requiring authentication for the livetiming archive in October 2026, including past seasons. FastF1 3.9 does not raise an error when it cannot sign in (it returns empty sessions), so the loader fails loudly. An older docs page that says post-session data needs no login predates the change. Do not use FastF1 2.x from PyPI: it requests the same servers.

**Audit result (Phase 1, `docs/data_source_audit.md`).**

| Season | Races present | Laps with compound | Laps with tyre life |
|---|---|---|---|
| 2021 | 22 of 22 | 99.75% | 100% |
| 2022 | 22 of 22 | 98.58% | 97.79% |
| 2023 | 22 of 22 | 99.86% | 99.86% |
| 2024 | 24 of 24 | 100% | 100% |
| 2025 | 24 of 24 | 98.12% | 98.03% |

- Race names match Jolpica for 114 of 114 races; driver codes match for 2,272 of 2,272 driver-races.
- Every race has race-control messages (at least 99.95% with a lap number) including an exact SC/VSC timeline.
- Compounds agree 100% with OpenF1 on three races (2023 R5, 2024 R5, 2025 R5). Both come from the same F1 feed, so this shows the conversion is faithful, not that the data is independent.
- Gaps: 2022 Austria (R11) has compounds on 77% of laps; 2021 Belgium (R12) has no racing laps.
- Fields were inferred from the files (no documentation): 42 lap fields, 9 race-control fields, 18 telemetry channels. Missing values are the string `"None"` and must be converted to nulls and counted.
- Size: Race sessions come to 18.7 GB, of which 18.6 GB is telemetry (one file per driver per lap, about 125k files). Everything except telemetry is about 81 MB.
- The PitStops repo holds fastest-stop award data (one row per team per race), not full pit timing. Cross-check only.

**Telemetry plan (B4).** For every race of 2021 to 2025, fetch that race's telemetry, compute per-(driver, lap) aggregates with the same definitions as Step 7 (reuse the logic), store the aggregates and delete the raw files (peak disk = one race). Keep the raw telemetry of one season (2024, about 4 GB) as Parquet in the lake for the data-lake and partitioning commitments. Guard on free space of the Windows drive (`df /mnt/c`, refuse under 20 GB). Measure one race first; if one race takes more than 10 minutes, switch to a decision-time-only selection of laps (never using labels or outcomes).

**Sample mode.** The committed bundle in `data/sample/` (502 KiB) is real Jolpica data for 2023 R5 and 2024 R5 plus clearly flagged synthetic tyre and telemetry tables (`is_synthetic=true`). It is the clone-and-run default. No real TracingInsights or F1 data is committed. Sample-mode model results are smoke tests only. The dashboard must show a visible "SYNTHETIC DEMO DATA" label in sample mode.

### B2. Repository structure (as built, with planned additions marked)

```
F1-Strategy-Analytics/
├── CLAUDE.md  README.md  LICENSE (MIT)  DATA_LICENSES.md  CITATION.cff (planned)
├── .env.example  .gitignore  .gitattributes  .editorconfig  .dockerignore  .pre-commit-config.yaml
├── Makefile      # up, up-full, down, nuke, sample-data, etl-sample, pipeline-full, model-dev-full,
│                 # model-save-full (FORCE=1), model-test-full (CONFIRM=yes); planned: stream-demo, demo, experiments*, bench*
├── pyproject.toml  uv.lock          # Python 3.12, pyspark 4.2.1, fastf1 3.9.2, confluent-kafka, ...
├── docker-compose.yml               # profiles core | hdfs | spark | full; HttpFS gateway; ./models mounted into app
├── docker/{spark,hadoop}/           # app image (Temurin 17 + python3.12 + pre-fetched Kafka jars), Hadoop image
├── src/f1sa/
│   ├── config.py
│   ├── ingest/   ratelimit, jolpica_client, fastf1_loader (optional), cli, manifest, synthetic, sample_builder,
│   │             tracinginsights_loader (4C Phase 2)
│   ├── lake/     schemas, quality, paths, writer
│   ├── etl/      clean, rdd_jobs, df_jobs, sql_jobs + sql/*.sql
│   ├── features/ gaps, stints, pitloss, lineage, telemetry_agg, outlap, undercut, undercut_rule, build
│   ├── model/    dataset, baselines, train, calibrate, evaluate, explain, metrics, deploy, model_save, lr_scorer
│   ├── streaming/ detector (shared rule); planned: events, producer, stream_job, redis_sink, latency
│   ├── serving/  (planned) keys, redis_store
│   ├── sim/      (planned) tyre_model, race_sim, whatif
│   └── api/      (planned) FastAPI app and SSE routes
├── web/          (planned) Next.js + TypeScript dashboard
├── experiments/  model_eval.py (done); planned: scalability, partitioning, ablation, streaming_latency, redis_latency, whatif_consistency
├── data/sample/  committed (CC BY-NC-SA 4.0, synthetic tables flagged)
├── data/         everything else gitignored (lake, cache, manifest, bench)
├── models/       gitignored (models/undercut/deploy_real_G3.5_k2/ etc.)
├── results/      committed CSV/PNG and results/README.md (env fingerprint)
├── tests/{unit,integration,e2e}/
├── docs/         architecture, data_dictionary, label_definition, model_spec, data_source_audit,
│                 traceability, PROGRESS, experiments (planned), reproduce (planned), report/ (planned)
└── .github/workflows/ci.yml
```

### B3. Architecture decisions (v2)

**D1. Running the stack from a fresh clone.** Docker Compose with profiles. `core` = Kafka (KRaft, single broker), Redis, `app`; `hdfs` = NameNode, DataNode and an HttpFS gateway; `spark` = master and 2 workers; `full` = all. Storage root only via `DATA_LAKE_URI` (`file://` for sample and tests, `hdfs://namenode:8020/f1` for full mode). **HDFS mode must run inside the `app` container**, because Spark on the WSL host cannot resolve the DataNode hostname; the host-side ingest CLI writes to HDFS through HttpFS. `/f1/raw` is read-only for everything except ingestion. Single-node HDFS is a storage-layer integration, not a performance claim.

**D2. Pinned versions.** Python 3.12, PySpark 4.2.1, `spark-sql-kafka-0-10_2.13:4.2.1`, Java 17 (Temurin), Kafka 4.3.x, Redis 8.10, Hadoop 3.4.3, FastF1 3.9.2 (optional now). Do not change without asking. Docker image tags were pinned by digest after the first build.

**D3. The undercut label (frozen v1).** Source of truth: `docs/label_definition.md` at tag `label-definition-v1`. Summary:

- An attempt is (season, round, attacker A, defender D, stop lap L): at the end of lap L-1, A is directly behind D within G seconds (G in {2.5, 3.5, 5.0}); A pits on L and D does not; D pits within k laps (k in {1, 2, 3}). Primary: G=3.5, k=2. Sensitivity: G=5.0, k=3.
- Label = 1 if A is ahead of D at the end of the first lap on which both have completed their stops.
- Excluded outcome categories (counted, never labelled): `defender_stayed_out`, and the exclusion reasons `missing_data`, `inconsistent_timing`, `early_lap` (stop lap L<5, constant `MIN_ATTEMPT_LAP=5` in `undercut_rule.py`), `teammates`, `traffic_between`, `lapped`, `in_pit`, `neutralised`, `same_lap_stop`, `retired`, `long_stop`, `penalty_suspected`, `attacker_restop`, `incident`, `wet`, `race_end`. The priority order is in the document.
- Neutralisation (Jolpica has no track status): a lap-time heuristic. A lap is a red-flag lap when the median lap time of all cars on it, pit laps and lap 1 included, exceeds 3.0 times the race's green median. After a stretch of SC or red-flag laps that includes a red flag, the next 3 green laps count as neutralised; after an SC or VSC without a red flag, 1 lap.
- Wet and drying running is counted as neutralised. A reporting-only split (sustained run of at least 8 laps, run median below 1.25 times, no red flag) estimates how much is wet; it never affects labels. Four races have unusually high flagged shares (2022 R7 and R4, 2023 R6 and R13).
- Leakage rules: features use information up to the end of lap L-1 only. The proposal's out-lap pace is split into stop time and pace loss; the decision-time feature is an expected value from strictly earlier races (driver-team, then team via the lineage map, then circuit, then global, with shrinkage and an explicit source flag); the realised value is allowed only in the labelled ORACLE ablation. Pit-loss and degradation priors use expanding windows over earlier races.
- Missing features get the constant 0.0 plus a `_missing` indicator; no imputation from later data.
- **Compounds and telemetry (v2).** The compound at the stop and later is chosen at the stop, so only the compound and tyre age of the current stint at lap L-1 are decision-time features. Race-control messages could replace the lap-time neutralisation heuristic, but that changes which attempts are labelled, so it is a label v2 decision, not a feature.

**D4. Calibration, threshold, metrics.** Platt scaling is used without comparing when there are fewer than 300 validation rows (49 and 64 here, so Platt for both models and settings); from 300 rows isotonic vs Platt is chosen by validation Brier. F1 uses a fixed threshold of 0.5 on calibrated probabilities (set in commit 64739ca before any full-data model ran; conservative given a base rate of about 0.33). Headline metrics: PR-AUC and Brier. Also F1 (positive class), macro-F1, ROC-AUC, ECE with 10 bins (primary) and 5 bins (supplementary). Intervals: race-cluster bootstrap, 1000 resamples, seed 42, as primary; attempt-level bootstrap for comparison. Majority and rule baselines are fixed references scored on the test season; they are never candidates.

**D5. Explanation.** `GBTClassifier.featureImportances`, permutation importance on the validation season (5 repeats, PR-AUC drop), and standardised logistic-regression coefficients.

**D6. Gaps.** Timing-line gaps from cumulative lap times plus a documented start offset (about 180 s from the scheduled start to lights out, error about plus or minus 1 minute), with lapped cars compared against the leader's time at the same lap number.

**D7. Telemetry volume.** See B1 (B4 plan). Never stream raw telemetry through Kafka.

**D8. What-if simulation.** A seeded Monte Carlo lap-time simulator is the what-if engine, with the calibrated model probability shown alongside. Consistency checks: monotonicity in pit loss, determinism with a seed, pit-now versus pit-next-lap ordering, backtest against historical attempts (stop and ask if the simulator and model disagree systematically by more than 20 percentage points).

**D9. Honest single-machine scalability.** Real data is small (about 130k laps, 20 MB raw Jolpica), so use synthetic replication (flagged) and, for the heavy aggregation, the real 2024 raw telemetry slice. 1 warm-up and 5 timed runs per cell; median and IQR. Conclusions are limited to trends on one host: nothing about network shuffles, locality or fault tolerance.

**D10. Latency measurement.** Every event carries `event_id` and `produced_at_ms`; segment timestamps through batch, Redis write, SSE and UI; p50/p95/p99 and throughput at replay speeds 1, 10 and 60; all clocks on one host.

**D11. Dashboard and API.** FastAPI with `redis.asyncio` Pub/Sub, Server-Sent Events, Next.js (TypeScript, App Router) with `EventSource`. Accessible, non-generic UI; compound pattern and text labels, `aria-live` alerts, keyboard navigation, `prefers-reduced-motion`. A visible "SYNTHETIC DEMO DATA" banner in sample mode. Streamlit polling is a fallback only (it breaks the no-polling commitment).

**D12. Rate limits.** Jolpica: token bucket (4/s burst, hourly budget shared across processes through a file-locked request log), on-disk cache, exponential backoff on 429 and 5xx, resumable manifest. The real limit proved tighter than 400/h; expect throttling. OpenF1: at most 2 requests per second and a few dozen requests for cross-checks.

### B4. Traceability matrix (with status)

| # | Commitment | Step | File or module | Status |
|---|---|---|---|---|
| 1 | Partitioned multi-season data lake (timing, results, pit stops, telemetry) | 4, 5, 4C | `lake/`, `etl/clean.py` | Done (telemetry in 4C) |
| 2 | Raw data in HDFS | 2, 4A | compose `hdfs` profile, `ingest/cli.py --sink hdfs` | Done |
| 3 | Clean Parquet partitioned by season and race | 5 | `etl/clean.py`, `lake/schemas.py` | Done |
| 4 | Tyre and telemetry ingestion (**source changed: TracingInsights, FastF1-derived; FastF1 optional**) | 3, 4C | `ingest/tracinginsights_loader.py`, `ingest/fastf1_loader.py` | In progress |
| 5 | Jolpica ingestion (race, driver, constructor, qualifying, pit stop, lap) | 3, 4A | `ingest/jolpica_client.py`, `ratelimit.py` | Done |
| 6 | Scope 2021 to 2025, 114 races | 4A | `data/manifest.json` | Done |
| 7 | Entities: Race, Driver, Lap, Tyre, Pit Stop, Position, Gap, Telemetry | 5, 6, 4C | `lake/schemas.py`, `docs/data_dictionary.md` | Done; real Tyre/Telemetry in 4C |
| 8 | Spark distributed ETL, joins, aggregations | 6 | `etl/df_jobs.py` | Done |
| 9 | RDD API demonstrated | 6 | `etl/rdd_jobs.py` | Done |
| 10 | DataFrame API demonstrated | 6 | `etl/df_jobs.py` | Done |
| 11 | Spark SQL demonstrated | 6 | `etl/sql/*.sql`, `sql_jobs.py` | Done |
| 12 | Feature tables: gap change, tyre age, out-lap pace, pit loss | 6, 7, 9d | `features/*` | Partial: timing features done; real compound/telemetry features in 9d |
| 13 | Undercut detection | 8 | `features/undercut.py`, `undercut_rule.py` | Done |
| 14 | MLlib undercut-success model | 9 | `model/train.py` | Done (development results only) |
| 15 | Explain main factors | 9 | `model/explain.py` | Done (development) |
| 16 | Chronological split (train 2021 to 2023, validate 2024, test 2025) | 9 | `model/dataset.py` | Done; test run pending (9f) |
| 17 | F1 against majority baseline | 9 | `model/baselines.py`, `experiments/model_eval.py` | Baselines done; test run pending |
| 18 | Probability calibration | 9 | `model/calibrate.py` | Done (Platt); test pending |
| 19 | Kafka carries race events | 12 | `streaming/producer.py` | Pending |
| 20 | Structured Streaming replay and opportunity detection | 12 | `streaming/stream_job.py`, `detector.py` | Pending |
| 21 | Redis hash `race:{season}:{round}:driver:{driverId}` with 9 exact fields | 11 | `serving/keys.py`, `redis_store.py` | Pending |
| 22 | Sorted set `race:{season}:{round}:order` | 11 | `serving/redis_store.py` | Pending |
| 23 | Pub/Sub `race:{season}:{round}:events` | 11, 12 | `streaming/redis_sink.py` | Pending |
| 24 | Namespaced keys with TTL after session end | 11 | `redis_store.expire_race()` | Pending |
| 25 | Redis-backed dashboard | 13, 14, 15 | `api/`, `web/` | Pending |
| 26 | What-if: pit now / next lap / stay out | 15 | `sim/*` | Pending |
| 27 | Experiment: runtime for 1, 3 and 5 seasons | 10 | `experiments/scalability.py` | Pending |
| 28 | Experiment: unpartitioned vs partitioned, with and without caching | 10 | `experiments/partitioning.py` | Pending |
| 29 | Experiment: model evaluation (chronological split) | 9, 9f | `experiments/model_eval.py` | Dev done; test pending |
| 30 | Experiment: ablation with vs without telemetry/out-lap features | 9d, 16 | `experiments/ablation.py` | Pending |
| 31 | Experiment: replay one race, event-to-result delay | 12, 16 | `experiments/streaming_latency.py` | Pending |
| 32 | Experiment: what-if comparisons from the same state | 15 | `experiments/whatif_consistency.py` | Pending |
| 33 | Metric: Spark runtime and scaling trend | 10 | `results/scalability.*` | Pending |
| 34 | Metric: speed-up from partitioning and caching | 10 | `results/partitioning.*` | Pending |
| 35 | Metric: streaming latency | 16 | `results/streaming_latency.*` | Pending |
| 36 | Metric: event throughput | 16 | same | Pending |
| 37 | Metric: Redis read/write latency | 11, 16 | `experiments/redis_latency.py` | Pending |
| 38 | Metric: resource usage | 10, 16 | `experiments/_resources.py` | Pending |
| 39 | Metric: calibration | 9, 9f | `results/model_eval_*` | Dev done; test pending |
| 40 | Metric: useful and consistent what-if comparisons | 15 | `results/whatif_consistency.*` | Pending |
| 41 | Metric: interactive dashboard | 14, 15 | `web/` | Pending |
| 42 | Reproducible end-to-end pipeline | 17 | `Makefile`, `docs/reproduce.md`, CI | Partial (`sample-data`, `pipeline-full` exist) |
| 43 | Timeline W1 to W8 | all | step headers | n/a |
| 44 | Report, docs, demo rehearsal | 18 | `docs/report/`, `docs/demo_script.md` | Pending |

### B5. Steps

**Standard rules line (end of every prompt):** "Follow CLAUDE.md Rules: complete runnable typed code (no placeholders or TODO stubs), tests for every public function, explicit error handling and edge cases, never commit secrets or data outside data/sample, and STOP and ask me if a decision would change the approach."

**Standard per-step routine:** `/clear`, set model and effort, Plan mode, paste the prompt, read the plan against the checklist, approve, switch to Auto, verify yourself, commit and push, check CI, update `docs/PROGRESS.md`.

**Standing constraints for every step from 4C onward (put them in the prompt):** do not change `label-definition-v1` or `model-spec-v1`; v1 outputs (labelled-set checksums, `undercut_attempts`, `model_dev_*`) stay byte-identical unless the step says otherwise; never read or print 2025 outcome values; do not run any Docker command that deletes data or volumes (`down -v`, volume rm or prune); do not stop or rebuild a service without telling me; do not read `.env` or token files; run targeted tests while iterating and the full suite once at the end.

#### Steps 0 to 3 (done) — as built

- **Step 0:** repo, `CLAUDE.md`, `.claude/settings.json`, four slash commands, `test-runner` agent.
- **Step 1:** `pyproject.toml` (uv, Python 3.12, pinned), `Settings`, CI (ruff, mypy, pytest unit), licences, `docs/traceability.md`.
- **Step 2:** compose profiles, health checks, a small Hadoop Dockerfile (daemons run as the `hadoop` user), `app` runs as UID 1000, `data/` and `results/` mounted read-write (later `models/` too), `make nuke` needs `CONFIRM=yes`, integration tests.
- **Step 3:** dual token-bucket rate limiter (injectable clock), Jolpica client (SQLite cache, backoff), FastF1 loader that raises `FastF1AuthError` on empty sessions, `f1sa-ingest` CLI.

#### Step 4A and 4B (done) — as built

- Resumable `season-range` ingestion with an atomic manifest, `--dry-run`, `--only-failed`, HttpFS sink (atomic temp-file-then-rename), shared hourly budget across processes.
- Synthetic generator (seeded, `is_synthetic=true`), `make sample-data` (offline validation, 502 KiB, CC BY-NC-SA 4.0 licence file).

#### Step 4C — Replace FastF1 (Phase 1 done, Phase 2 in progress)

Phase 1 (audit) is complete; see B1. The Phase 2 prompt that was sent (kept here for the record and for teammates):

```
/step
Step 4C Phase 2: GO. Telemetry plan B4.
- Aggregate every race of 2021-2025: fetch that race's telemetry (partial sparse git fetch or the cheapest measured way), compute per-(driver, lap) aggregates with the SAME definitions and thresholds as features/telemetry_agg.py (reuse its logic, do not reimplement; throttle >= 98), plus n_samples and a coverage flag, write them, then delete the raw files. Peak disk = one race. Measure one race first; if one race takes more than 10 minutes, stop and propose a decision-time-only selection rule (never labels or outcomes).
- Keep the raw telemetry of ONE season (2024) as Parquet under raw/tracinginsights/telemetry/season=/round=/driver=/ through the existing sinks. Flag to skip it.
- Disk guard on the WINDOWS drive that backs WSL (df on /mnt/c); refuse below 20 GB free and print the number seen.
- Everything else in the repos (laps, race control, about 81 MB): all races, pinned to the audit's SHAs, sha256 manifest, resumable, "None" strings converted to nulls and counted.
- Additive tables only: stints_tracinginsights (real compound, tyre life, stint, source flag), race_control (lap numbers, SC/VSC/red-flag timeline), telemetry_lap_real (is_synthetic=false), compound coverage report per race (2022 Austria 77%: null plus missing indicator, never imputation).
- HDFS replication: dfsadmin shows under-replicated blocks, probably jobs in the app container writing with replication 3 on a one-datanode cluster. Find the cause; if confirmed set replication 1 for app-container jobs and the HttpFS path. Do not rewrite existing data; give me the `hdfs dfs -setrep` command for /f1 to run myself. No table content or checksum may change.
Hard constraints: v1 outputs byte-identical (the clean-zone stints precedence FastF1 -> synthetic -> inferred must NOT pick up these rows; do not use race control for the v1 neutralisation flag). For 2025 only aggregates and coverage counts. No model training. Downloaded data never committed; data/sample unchanged.
Before building: give me a plan with the file list, how the host download reaches HDFS, time and disk estimate from the one-race measurement, and tests (tiny hand-trimmed fixtures, aggregator equality with the Spark telemetry_agg on the same fixture, resume and kill-safety, "None" handling). Update DATA_LICENSES.md (MIT 2021 to 2024, Apache-2.0 for 2025, neither covers the underlying F1 data), docs/traceability.md (#4, #12, #28, #30) and docs/PROGRESS.md.
```

Plan checklist: reuses Step 7 logic; additive tables only; one-race speed test; `/mnt/c` disk guard; cause of under-replication explained with evidence. Run the full download in tmux with a log; keep Windows awake and plugged in. Commit: `feat(ingest): tracinginsights loader, additive tyre, race-control and telemetry tables`. Then tell your guide about the source change.

#### Step 5 to 7 (done) — as built

- **Step 5:** typed schemas, `etl/clean.py` (file mode on the host; HDFS mode inside `app`), `lake/quality.py` (28 checks, JSON report in `clean/_quality/`, never read as a table).
- **Step 6:** RDD stint reconstruction and lap-time distribution, DataFrame enrichment and gap windows, Spark SQL views (pit loss from prior races only, team out-lap history, race summary), 7 feature tables, leakage tests.
- **Step 7:** telemetry aggregation (synthetic pass-through), out-lap decomposition and shrinkage, team lineage map (`alphatauri` to `rb`, `alfa` to `sauber`), `clean lap` definition with a 107% rule.

#### Step 8, 8b, 8c (done) — as built

- Shared candidate rule `undercut_rule.py` (no Spark or pandas imports) used by the batch label and the live detector; agreement tests.
- 8b: `make pipeline-full` runs clean, features and attempts inside the `app` container against HDFS; reruns give identical checksums; `/f1/raw` and the manifest unchanged.
- 8c: red-flag fix, wet-running reporting split, early-lap rule L<5 (labelled set identical at all 9 (G, k) settings; 213 rows moved to `excluded_early_lap`, none previously labelled).

#### Step 9, 9b, 9c (done) — as built

- **9:** dataset split by season (2021 to 2023 / 2024 / 2025) with class weights from training rows only; LR and GBT grids; Platt and isotonic; explanations; cluster bootstrap; refuses synthetic features in full mode; test season guarded by `--allow-test-season`.
- **9b:** `make model-dev-full` (never loads 2025) with leave-one-season-out summary; outputs stamped DEVELOPMENT; file names include G and k for test runs.
- **9c:** `make model-save-full` writes `models/undercut/deploy_real_G3.5_k2/` (Spark pipelines, calibrator JSON, metadata, LR `scorer.json`, `reproduction.json`); reproduction check matches development (calibrated LR Brier 0.168438, PR-AUC 0.593775). A pure-Python LR scorer (no Spark) matches Spark to 2e-16. Streaming deploys only the calibrated LR through the scorer; GBT stays loadable for batch comparison.
- Test-suite fix: shared Spark session had the default 1 GB heap; `tests/conftest.py` now sets a 2 GB heap, caps job history and clears caches per module.

#### Step 9d — Real tyre and telemetry features (development only)

Model: Sonnet 5.5, medium; Plan, then Auto. Prerequisite: 4C committed.

```
/step
Step 9d: real tyre and telemetry features as NEW feature sets, development results only (2021-2024).
Context: label-definition-v1 and model-spec-v1 are frozen. 4C added stints_tracinginsights, race_control, telemetry_lap_real. Do NOT change any v1 label, DECISION_FEATURES, grid, threshold, calibration rule or output; v1 outputs must stay byte-identical (show the hashes).
- Feature sets (config, not code forks): real_timing (= v1 `real`, 21 inputs), real_tyre (adds attacker and defender compound and tyre life of the CURRENT stint at lap L-1, compound-pair delta, stint length), real_tyre_telemetry (adds per-lap telemetry aggregates for laps <= L-1, for example the mean of the last 3 laps). The compound chosen AT the stop (the next stint) and anything from lap L or later is NOT known at decision time and must not appear; add a leakage test that scales all data after lap L-1 and every later race by 100 and checks the features are unchanged.
- Missing values (for example 2022 Austria compounds, laps without telemetry): 0.0 plus a _missing indicator; no imputation from later data. Report missing rates per season.
- Race-control messages and the neutralisation flag: NOT used for labels or eligibility in this step (that would change the labelled set; it is a possible label v2). You may report, as a sensitivity table only, how many v1 labelled attempts fall in race-control SC/VSC/red-flag windows.
- Extend experiments/model_eval.py --no-test (development mode) to run the three feature sets at the primary setting G=3.5, k=2 and report validation-season metrics with race-cluster intervals plus leave-one-season-out on 2021-2024, stamped DEVELOPMENT. Do not choose a winner by validation score; just report. 2025 must not be loaded.
- Tests: no test-season read (spy), feature-set refusal of is_synthetic in full mode, leakage test above, deterministic outputs (rerun = same checksums).
Constraints: standing constraints apply (never print 2025 outcome values). Show the command output. Follow CLAUDE.md Rules.
```

Commit: `feat(model): real tyre and telemetry feature sets (development only)`. Paste the development table to review before 9e.

#### Step 9e — Model specification v2 and freeze

Model: `opusplan`, high for the primary-selection rule; Plan.

```
/step
Step 9e: write docs/model_spec_v2.md (frozen once committed; it adds to, and does not replace, model-spec-v1).
Declare BEFORE the test run: (1) the feature sets evaluated on 2025 (real_timing, real_tyre, real_tyre_telemetry) and the models (LR and GBT), both settings, baselines as fixed references; (2) a pre-declared rule that picks the PRIMARY feature set from development results only (for example, leave-one-season-out mean PR-AUC across 2021-2024 for calibrated LR, with a stated minimum margin over real_timing, otherwise real_timing stays primary); apply the rule now, from development files only, and write the outcome into the spec; (3) that every other v1 rule is unchanged (threshold 0.5, Platt below 300 rows, cluster bootstrap, headline metrics PR-AUC and Brier, rerun rule); (4) the ablation (#30) design: which comparisons are reported, that it uses the same split, and that differences are reported with intervals and no significance claim; (5) that the development results (including the 9d tables) were seen before the freeze.
Do not train, do not read 2025. Show me the file.
```

Then: commit, tag `model-spec-v2`, send both spec documents to the team for written sign-off.

```bash
git tag -a model-spec-v2 -m "Model spec v2 frozen before the test season is opened"
git push origin model-spec-v2
```

#### Step 9f — The single 2025 test run

Preconditions: both documents signed off in writing by the team; no pending changes to features or model code; Docker stack healthy; run in tmux with a log. Then:

```bash
make model-test-full CONFIRM=yes 2>&1 | tee -a data/test_run.log
```

Report the results as they come out, including if the model does not beat the rule baseline. No reruns except after a crash that produced no test number. The model and features are frozen for good after this.

#### Step 10 — Scalability, partitioning and caching experiments

Model: Sonnet 5.5, medium; Plan, then Auto. Prerequisites: the 9f run is done or deliberately postponed; WSL cap raised to 14 GB; `app` container limit raised; the laptop is quiet (no Spark tests, no model runs, no browser tabs) when the full grid runs.

```
/step
Step 10: Spark scalability, partitioning and caching experiments with honest single-host conclusions.
Context: real data is small (about 130k laps; raw Jolpica 20 MB). Heavy table: the real 2024 raw telemetry slice from 4C (plus clearly flagged replication). Propose in your plan how to reach a regime where shuffles matter: (a) synthetic replication of lap-level tables (x1, x4, x16, x64 if memory allows), perturbed ids and race keys, flagged is_replicated; (b) replication of the telemetry slice. Say what each can and cannot show. Also propose the new app-container memory limit (currently mem_limit 1536m) and WSL settings.
- experiments/_resources.py: process RSS and CPU (psutil) and per-container usage (docker stats) at a fixed interval; return peak values.
- experiments/scalability.py: workload = full feature build plus one heavy aggregation over telemetry. Grid: seasons {1, 3, 5} x replication {1, 4, 16, 64 if memory allows} x master {local[1], local[2], local[4], spark://spark-master:7077}. Per cell: 1 warm-up (discarded) and 5 timed runs; median, IQR, min, max wall time, input bytes, shuffle bytes (Spark REST API), peak RSS/CPU, git sha. Skip and record cells that exceed a memory guard.
- experiments/partitioning.py: same queries (one race, one season, full scan, a join with aggregation) on an unpartitioned dataset versus partitioned season/round, each with and without cache or persist plus count; on file:// and hdfs://. Report speed-up against the simplest baseline.
- Benchmark data only under /f1/bench (HDFS) and data/bench (host), gitignored; `make bench-clean` refuses any other path. NEVER write to /f1/raw, /f1/clean, /f1/features.
- Write results/scalability.csv, results/partitioning.csv, PNG plots, docs/experiments.md with "What these results can and cannot show" (single host, no network shuffle, no locality, shared page cache, 2 workers on one machine, replication is synthetic, trends only), and the environment fingerprint in results/README.md.
- Targets: `make bench-smoke` (1 season, x1, 1 repetition, minutes; you may run it) and `make experiments-spark` (full grid; DO NOT run it yourself, I will run it with nothing else running).
- Tests: timing harness with a trivial job, median/IQR, memory-guard skip, bench-clean path refusal, replication keeps row counts exactly x factor with unique ids.
Constraints: standing constraints apply. Rebuilding or restarting a service needs my yes. Show the command output. Follow CLAUDE.md Rules.
```

Run the full grid in tmux, plugged in, nothing else running: `make experiments-spark 2>&1 | tee data/bench.log`. A flat curve at small replication is expected.

#### Step 11 — Redis serving layer

Model: Sonnet 5.5, medium. Implements the Redis contract exactly (see D-section and CLAUDE.md): hash `race:{season}:{round}:driver:{driverId}` with 9 fields (position, gap_ahead_s, gap_leader_s, tyre_compound, tyre_age_laps, last_lap_time_s, undercut_flag, undercut_prob, updated_at), sorted set `race:{season}:{round}:order`, Pub/Sub `race:{season}:{round}:events` (lap, pit_in, pit_out, undercut_alert, session_end), TTL after session end plus a safety TTL on every write.

```
/step
Step 11: serving/keys.py and serving/redis_store.py implementing EXACTLY the Redis contract in CLAUDE.md.
- keys.py: pure functions driver_key, order_key, events_channel with input validation.
- redis_store.py (sync and asyncio): upsert_driver_state (HSET all 9 fields, updated_at ISO-8601 UTC ms), set_order (ZADD score=position), publish_event (JSON with type and timestamps), expire_race (SCAN MATCH race:{s}:{r}:* then EXPIRE, including the sorted set), safety TTL on each write, all writes per micro-batch in one pipeline (transaction=False).
- experiments/redis_latency.py: p50/p95/p99 of pipelined batch writes and HGETALL/ZRANGE reads at 20 drivers x N updates/s for N in {1, 10, 60}; also `redis-cli --latency -i 1` for 30 s via docker compose exec; results/redis_latency.csv.
- Tests with fakeredis: exact field names, sorted order, TTL after session_end, channel names.
Constraints: standing constraints apply. Follow CLAUDE.md Rules.
```

Verify: `uv run pytest tests/unit -q -k "keys or redis"`, then `make up-full && uv run python experiments/redis_latency.py`. Commit: `feat(serving): Redis hash/zset/pubsub contract with TTL and latency benchmark`.

#### Step 12 — Kafka replay and Structured Streaming detector

Model: `opusplan`, high for the design, medium to implement.

```
/step
Step 12: streaming/.
- events.py: typed event schema (event_id uuid, type lap|pit_in|pit_out|session_end, season, round, driver_id, lap, lap_time_s, position, compound, tyre_age, produced_at_ms) with JSON (de)serialisation and a Spark StructType. Note: the integration test message (header test=infra) already sits in topic f1.race.events; consumers must filter it.
- producer.py: replay one race from the clean Parquet in race order at a speed factor (1, 10, 60 race-seconds per wall-second), key=driver_id, acks=all, idempotent, session_end at the end.
- stream_job.py: readStream kafka (startingOffsets latest, maxOffsetsPerTrigger), trigger 500 ms, foreachBatch: merge with current state from Redis, compute gaps and positions, apply the SHARED candidate rule (streaming/detector.py, MIN_ATTEMPT_LAP), score candidates with the pure-Python calibrated-LR scorer loaded from models/undercut/deploy_real_G3.5_k2 (no Spark call per batch; say in the plan whether to also measure GBT as an optional comparison), write through redis_store in one pipeline, publish lap/pit/undercut_alert events with batch_received_at_ms and redis_written_at_ms, expire_race on session_end. Checkpoint under data/checkpoints (gitignored). Log StreamingQueryProgress to results/stream_progress.jsonl.
- latency.py: a subscriber recording receive times into results/streaming_latency.csv.
- `make stream-demo SEASON=2024 ROUND=5 SPEED=10`.
- Integration test (marked) replaying the sample race at speed 60, asserting Redis state for all drivers and at least one undercut_alert if the sample contains an attempt.
Constraints: standing constraints apply. Ask before introducing stateful operators instead of foreachBatch. Follow CLAUDE.md Rules.
```

Verify: `make up-full && make stream-demo SEASON=2023 ROUND=5 SPEED=60` then `docker compose exec redis redis-cli ZRANGE race:2023:5:order 0 -1 WITHSCORES`. Commit: `feat(streaming): Kafka race replay and Structured Streaming undercut detector to Redis`.

#### Step 13 — FastAPI backend with SSE

Model: Sonnet 5.5, medium. Endpoints: `GET /api/health`, `GET /api/races`, `GET /api/races/{season}/{round}/state`, `GET /api/races/{season}/{round}/events` (SSE with heartbeat, disconnect handling, `sse_sent_at_ms`), `POST /api/races/{season}/{round}/replay` (guarded by an env flag), `POST /api/races/{s}/{r}/whatif` (Step 15), `POST /api/debug/latency`. Pydantic models mirror the Redis contract; CORS for localhost:3000; the API container mounts `./models` read-only. Tests with httpx and fakeredis (the SSE test reads the first two events). Verify: `uv run pytest tests/unit -q -k api` and `curl -N localhost:8000/api/races/2023/5/events`.

#### Step 14 — Next.js dashboard

Model: Sonnet 5.5, medium. Race picker; live race view with timing tower, gap-to-leader chart (last 15 laps), tyre panel with compound text and pattern (not colour alone) and age bars, `aria-live="polite"` alert feed with calibrated probability text, replay control. A visible "SYNTHETIC DEMO DATA" banner in sample mode. Accessible (keyboard, focus styles, WCAG AA, `prefers-reduced-motion`), distinctive pit-wall visual identity, Vitest for the event reducer, a Playwright smoke test against the sample replay, `docker/web`, added to the compose `core` profile. Ask before adding UI libraries beyond a chart library. Verify: `cd web && npm ci && npm run lint && npm run typecheck && npm test`, then `make demo` and open http://localhost:3000.

#### Step 15 — What-if simulator, API and UI

Model: `opusplan`, high for the simulator design. `tyre_model.py` (per circuit and compound degradation, linear or quadratic by validation error, prior races only; real compounds in full mode, synthetic and flagged in sample mode), `race_sim.py` (seeded Monte Carlo, n=500, strategies pit_now, pit_next_lap, stay_out, with pit loss, expected out-lap penalty, traffic penalty and residual noise), `whatif.py` (probability the attacker is ahead after the defender's stop, expected gap, 80% interval, calibrated model probability alongside), the API route and a side-by-side UI panel, and `experiments/whatif_consistency.py` (monotonicity, determinism, ordering, backtest) writing `results/whatif_consistency.csv/.png`. Stop and ask if the simulator and model disagree systematically by more than 20 percentage points. Backtests read only 2021 to 2024 outcomes until the test season has been opened by 9f.

#### Step 16 — Ablation, streaming experiments, `make experiments`

Model: Sonnet 5.5, medium. `experiments/ablation.py` reports the three feature sets from the frozen `model_spec_v2.md` (`real_timing`, `real_tyre`, `real_tyre_telemetry`) plus a clearly labelled ORACLE variant using the realised out-lap delta, with the same split, calibration and race-cluster intervals; test-season numbers only as pre-declared in the spec, never re-run after seeing them. `experiments/streaming_latency.py` replays at speeds 1, 10 and 60 and reports segment latencies (produced, batch, Redis, SSE, UI) as p50/p95/p99 plus throughput and Redis latency at those rates. `experiments/run_all.sh`, `make experiments` (full) and `make experiments-sample` (under 10 minutes, smoke only, stamped). Every script writes `results/<name>.csv` and `.png` and appends the environment fingerprint (CPU, RAM, versions, git sha) to `results/README.md`.

#### Step 17 — Hardening, CI end-to-end, README, docs

Model: Haiku for docs, Sonnet for CI and end-to-end. `make demo` (up core, load sample, small model on sample or a committed tiny model under 5 MB — ask first — then stream replay at speed 10 and print the dashboard URL); CI job running `make sample-data`, sample ETL and features, unit tests and a headless streaming smoke test with the `core` profile; README with badges, mermaid architecture diagram, quickstart for Windows with WSL2 and Linux/macOS, full-data mode (how to fetch the TracingInsights data at the pinned SHAs, optional F1 TV for FastF1, rate-limit etiquette, expected ingest time of about 3 hours), the experiment reproduction table, limitations, and data licensing (Jolpica CC BY-NC-SA 4.0 non-commercial; TracingInsights MIT or Apache-2.0 for their files but not for the underlying F1 data; FastF1 MIT code; F1 data not redistributed); `docs/reproduce.md`, final `docs/architecture.md`, `CITATION.cff`, and `scripts/check_traceability.py` asserting every file in `docs/traceability.md` exists. Verify on a fresh clone: `time (make up && make sample-data && make demo)` should finish in under 15 minutes without credentials.

#### Step 18 — Report and demo rehearsal

Model: Opus medium to review, Haiku for edits. Draft `docs/report/report.md` (problem, architecture, data and the source change, label, model results with intervals, calibration, ablation including the oracle caveat, scalability with single-host limits, streaming and Redis latency, what-if consistency, threats to validity, future work) and `docs/demo_script.md` (7 minutes with a fallback of pre-recorded results and the sample replay). Every claim cites a `results/` file; no numbers that are not in `results/`.

### B6. Splitting the work across four people

The work so far was done by one person. The plan still holds if more people join: contracts are frozen (label, model spec, Redis keys, event schema), each member uses their own Pro account on their own branch, and `main` is protected (CI must pass, one reviewer). Suggested roles: infra and ingestion; Spark features and ML; streaming and Redis; API, UI and simulator. Everyone shares Steps 17 and 18. Conventional-commit PR titles; the PR template lists traceability rows covered, test evidence, UI screenshots, a "data committed? no" checkbox and the Claude model and effort used. **Team sign-off on `docs/label_definition.md` and the model specifications is still outstanding and gates Step 9f.**

### B7. Repo-ready checklist

- ☐ README quickstart works on a fresh clone: `make up`, `make sample-data`, `make demo` in under 15 minutes with no credentials.
- ☐ `.env.example` complete; no secrets in history; no F1-derived or TracingInsights data committed.
- ☐ Pinned versions and digests: `uv.lock`, `package-lock.json`, image tags, the Kafka connector jar matching PySpark; TracingInsights commit SHAs recorded.
- ☐ `make experiments-sample` and `make experiments` regenerate every `results/*.csv` and `.png`; `results/README.md` has the environment fingerprint and git sha.
- ☐ `docs/reproduce.md`: one row per experiment (command, runtime, output files, expected shape).
- ☐ Tests: unit in CI, integration locally (`-m integration`), e2e with Playwright on the sample replay; coverage at least 70% on `src/f1sa` excluding the CLI.
- ☐ Licensing in `DATA_LICENSES.md`: MIT code; Jolpica CC BY-NC-SA 4.0 (attribution, non-commercial, share-alike for `data/sample`); TracingInsights files and the fact that neither their licence nor ours covers the underlying F1 data; FastF1 optional with F1 TV Access; OpenF1 terms.
- ☐ Tags exist: `label-definition-v1`, `model-spec-v1`, `model-spec-v2`.
- ☐ `scripts/check_traceability.py` passes.
- ☐ Report and demo: 7-minute script, rehearsed fallback, screenshots, limitations slide (small labelled sets, synthetic sample data, single host).
- ☐ Stranger test: a teammate follows the README from scratch.

### B8. Risks and what to do

| Risk | Mitigation |
|---|---|
| Upstream TracingInsights repo moves, changes or is removed | Pin commit SHAs; keep a sha256 manifest; document the source in the README; the synthetic sample keeps the demo working. |
| Licence or terms concern about F1-derived data | Download on demand, never commit or redistribute; academic use; tell the guide; keep the FastF1 and OpenF1 paths documented. |
| Telemetry download too slow or large | Measure one race first; aggregate and delete; fall back to a decision-time-only lap selection. |
| Jolpica throttling or downtime | Token bucket, cache, resumable manifest; sample still runs the whole app. |
| Too few labelled attempts for stable metrics | Race-cluster intervals, sensitivity setting, no significance claims; say so in the report. |
| Model does not beat the rule baseline | Report it as the result; do not tune toward 2025. |
| Test-season contamination | Tags, spy tests, refuse-to-overwrite guard, single run, first complete run reported. |
| Docker or WSL restarts mid-run | Idempotent pipelines; `make up-full` and `docker compose ps` after any restart; tmux logs. |
| Benchmark results distorted | Quiet laptop, 5 timed runs, median and IQR, state single-host limits. |
| Pro limit hit mid-step | Ask Claude to update `docs/PROGRESS.md`, commit WIP, resume from the file after the reset; credits only as a team decision with a cap. |

---

## Part C — Runbook (copy-paste)

**C1. CLAUDE.md revisions to apply now (keep it under 200 lines).**

- Data rules: replace "FastF1: Cache.configure()..." with "Tyre, race-control and telemetry data come from TracingInsights season repos pinned by SHA (FastF1 optional, needs F1 TV Access); never commit F1-derived data."
- Add: "Label definition and model spec are frozen (tags `label-definition-v1`, `model-spec-v1`, later `model-spec-v2`). Never read or print 2025 outcome values. The test run is `make model-test-full CONFIRM=yes`, once."
- Add: "HDFS mode runs inside the `app` container; never write to /f1/raw; benchmark data only under /f1/bench and data/bench."
- Gotchas: "After any Docker or WSL restart run `make up-full`. Disk guard checks `df /mnt/c`. Sample mode is smoke-test only."

**C2. `.claude/settings.json` deny list (current intent).**

```json
"deny": [
  "Read(./.env)", "Read(./.env.local)", "Read(./**/secrets/**)",
  "Read(~/.fastf1/**)", "Read(~/.local/share/fastf1/**)",
  "Bash(git push --force*)", "Bash(git push -f*)", "Bash(git reset --hard*)", "Bash(git clean -fd*)",
  "Bash(docker compose down -v*)", "Bash(docker volume rm*)", "Bash(docker system prune*)",
  "Bash(rm -rf /*)", "Bash(rm -rf ~*)", "Bash(sudo *)", "Bash(curl * | sh*)",
  "Bash(cat .env)", "Bash(cat .env.local)"
]
```

Note: these rules guard Claude, not your own terminal. Never run `docker volume prune`, `docker system prune --volumes`, `make nuke` or `make down -v` unless you mean to delete HDFS, Kafka and Redis state.

**C3. After a laptop shutdown or restart.**

```bash
# 1. Start Docker Desktop; open VS Code; WSL: Connect to WSL; open ~/code/F1-Strategy-Analytics
cd ~/code/F1-Strategy-Analytics
git status -sb && git log --oneline | head -5
make up-full
docker compose ps --format "table {{.Service}}\t{{.Status}}"
docker compose exec namenode hdfs dfs -ls /f1          # raw, clean, features should exist
make lint && make typecheck                              # quick check; full tests take about 7-8 minutes
```

**C4. Commit and tag.**

```bash
git status -sb          # nothing under data/ or models/
git add -A
git commit -m "<conventional message>"
git push
git tag -l              # label-definition-v1, model-spec-v1 (and model-spec-v2 after 9e)
```

**C5. Long runs in tmux.**

```bash
tmux new -s job
# run the command with: 2>&1 | tee -a data/<name>.log
# detach: Ctrl+B then D     reattach: tmux attach -t job     list: tmux ls
```

**C6. Disk and memory checks.**

```bash
df -h /mnt/c              # real free space on Windows C: (Avail column)
free -h | head -2         # WSL memory
docker stats --no-stream
docker compose exec namenode hdfs dfsadmin -report | head -12
```

**C7. Useful verification of byte-identity (v1 outputs).** Before and after any step that must not change v1: `sha256sum results/model_dev_* results/*_full.* results/undercut_counts_sample.csv` and compare the `labelled_set_checksums` and `labels_checksum` in `results/full_data_quality.json`.

---

## Part D — Open items and known issues

1. **Team sign-off** on `docs/label_definition.md` and `docs/model_spec.md` (and v2 when written), in writing. Gates Step 9f.
2. **4C Phase 2** (loader, additive tables, one-race speed test, replication fix for HDFS) is in progress. Tell the project guide about the data-source change.
3. **Saved model metadata** records `git_dirty: true` from the first save; optional clean re-save with `make model-save-full FORCE=1` after committing (the reproduction check must match again).
4. **Under-replicated HDFS blocks** (about 1,900): suspected replication 3 on a one-datanode cluster; confirm the cause in 4C and fix new writes; run `hdfs dfs -setrep` yourself for existing data.
5. **Docker or WSL restarts** with an unexplained cause (no OOM, container exit code 0). Check Windows event IDs 41, 1074 and 6008 if it recurs; keep the laptop plugged in.
6. **Pro allowance and timing:** Step 10 needs a quiet laptop, a 14 GB WSL cap and a higher `app` container memory limit.
7. **Renamed teams** (`alphatauri` to `rb`, `alfa` to `sauber`) are handled by the lineage map; the Step 7 test fails if a new constructor id is neither stable nor mapped.
8. **PySpark `BrokenPipeError` noise** in worker stderr is harmless.
9. **Things to re-verify before relying on them:** OpenF1's current terms and rate limits, TracingInsights repository locations (some snapshots moved to an Archive organisation), Claude Code model names and defaults, and Pro usage limits.
10. **Label v2 (optional, after the test run):** use race-control messages for SC/VSC/red-flag windows and real compounds for the wet exclusion; this would change which attempts are labelled and needs a fresh review on 2021 to 2024 only.
