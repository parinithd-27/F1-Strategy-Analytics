"""DEVELOPMENT: permutation importance for LR and GBT on calibrated validation scores.

    make model-dev-permutation-ti-full   # app container, 2021-2024 only (2025 never read)
    python experiments/model_dev_permutation.py --feature-set real_ti [--g 5.0 --k 3]

The logic lives in ``f1sa.model.permutation_dev`` (tests: ``tests/unit/test_permutation_dev.py``).
"""

from __future__ import annotations

import sys

from f1sa.model.permutation_dev import main

if __name__ == "__main__":
    sys.exit(main())
