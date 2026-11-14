#!/usr/bin/env bash
# make experiments / make experiments-sample (Step 16): rerun every regenerable generator in a
# sandbox data/experiments/<run_id>/ and compare its outputs with the committed results/ by sha256.
# results/ is never written; generators that open 2025 or refuse to overwrite are skipped (printed).
#
#   experiments/run_all.sh            # full lake: app container (needs make up-full)
#   experiments/run_all.sh --sample   # sample bundle on the host (SAMPLE stamp, < 10 min)
#   experiments/run_all.sh --dry-run  # print the plan only
#
# The logic lives in f1sa.bench.experiments (tests/unit/test_experiments_runner.py).
set -euo pipefail
cd "$(dirname "$0")/.."
exec uv run python -m f1sa.bench.experiments "$@"
