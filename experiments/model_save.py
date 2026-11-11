"""Deployment model (Step 9c): the frozen specification trained once and saved for streaming.

    make model-save-full   # app container, HDFS features, 2021-2024 only (2025 never read)
    python experiments/model_save.py --source full [--g 5.0 --k 3] [--force]

Writes ``models/undercut/deploy_real_G{g}_k{k}/`` and checks it against the development run.
The logic lives in ``f1sa.model.deploy`` (tested in ``tests/unit/test_model_deploy.py``).
"""

from __future__ import annotations

import sys

from f1sa.model.deploy import main

if __name__ == "__main__":
    sys.exit(main())
