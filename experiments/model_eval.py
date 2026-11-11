"""Model evaluation experiment (#29): chronological split, baselines, calibration, explanations.

    uv run python experiments/model_eval.py --source sample      # smoke test, no test season
    uv run python experiments/model_eval.py --source rehearsal   # GENERATED data, whole pipeline
    uv run python experiments/model_eval.py --source full --allow-test-season   # opens 2025 once
    make model-dev-full   # DEVELOPMENT: --source full --no-test (2021-2024 only), app container

The logic lives in ``f1sa.model.pipeline`` (tested in ``tests/unit/test_model_pipeline.py``).
"""

from __future__ import annotations

import sys

from f1sa.model.pipeline import main

if __name__ == "__main__":
    sys.exit(main())
