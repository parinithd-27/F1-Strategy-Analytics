"""ORACLE variant (realised out-lap delta, NOT usable at decision time) on development data only:
2024 validation and 2021-2024 leave-one-season-out; 2025 is never loaded (Step 16, #30).

    make oracle-dev-full              # app container; outputs in data/oracle/<run_id>/
    make oracle-dev-full RECORD=1     # ./results/model_dev_{eval,loso}_oracle{,_G5.0_k3}.csv

The logic lives in ``f1sa.model.oracle_dev`` (tested in ``tests/unit/test_oracle_dev.py``).
"""

from __future__ import annotations

import sys

from f1sa.model.oracle_dev import main

if __name__ == "__main__":
    sys.exit(main())
