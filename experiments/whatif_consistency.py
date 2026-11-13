"""What-if consistency checks and backtest (Step 15, #32 / #40; ``docs/sim_spec.md``).

    make whatif-consistency-sample           # smoke test on the sample lake (data/whatif/ only)
    make whatif-consistency-full             # app container, HDFS lake, 2021-2024 (data/whatif/)
    make whatif-consistency-full RECORD=1    # the recorded run: + 2025 held-out row -> results/

Exit codes: 0 every check passed, 1 a pass/fail check failed, 2 refused, 3 the pre-registered
stop rule (|mean P_sim - P_model| > 0.20 on 2021-2024; 2025 is then not read). The logic lives
in ``f1sa.sim.consistency`` and ``f1sa.sim.backtest``.
"""

from __future__ import annotations

import sys

from f1sa.sim.consistency import main

if __name__ == "__main__":
    sys.exit(main())
