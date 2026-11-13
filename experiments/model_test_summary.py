"""2025 test-season summary (Step 9f) from the committed result files; no evaluation.

    make model-test-summary

writes results/model_test_summary.csv and the Results tables of docs/model_results.md.

The logic lives in ``f1sa.model.test_summary``
(tested in ``tests/unit/test_model_test_summary.py``).
"""

from __future__ import annotations

import sys

from f1sa.model.test_summary import main

if __name__ == "__main__":
    sys.exit(main())
