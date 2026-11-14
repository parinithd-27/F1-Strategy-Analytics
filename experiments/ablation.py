"""Paired feature-set ablation on the 2025 test season from the saved frozen 9f models (Step 16,
traceability #30, ``docs/model_spec_v2.md`` §4).

    make ablation-full              # app container; outputs in data/ablation/<run_id>/ only
    make ablation-full RECORD=1     # + results/ablation_paired.{csv,png} and a README fingerprint

Nothing is retrained, re-tuned or re-calibrated. The run first checks that it reproduces every
committed ``model_eval`` value exactly (and the earlier run of 2026-11-14T10:18Z) and stops before
any interval otherwise. The logic lives in ``f1sa.model.ablation``
(tested in ``tests/unit/test_ablation.py``).
"""

from __future__ import annotations

import sys

from f1sa.model.ablation import main

if __name__ == "__main__":
    sys.exit(main())
