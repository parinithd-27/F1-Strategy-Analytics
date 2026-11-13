"""A stand-in for ``python -m f1sa.bench.cell`` (no Spark): writes a plausible result at once, or
sleeps first when its cell id is ``$FAKE_SLOW_CELL`` (so a test can kill the grid mid-cell)."""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any


def main(argv: list[str]) -> int:
    with open(argv[argv.index("--spec") + 1], encoding="utf-8") as fh:
        spec = json.load(fh)
    out = argv[argv.index("--out") + 1]
    if spec["cell_id"] == os.environ.get("FAKE_SLOW_CELL"):
        time.sleep(120)
    res: dict[str, Any] = {"status": "ok", "error": "", "startup_s": 0.1, "conf": {}}
    if spec["phase"] == "prep":
        res |= {
            "prep_s": 0.1,
            "rows": {"laps": 10 * spec["factor"]},
            "laps_by_season": {"2024": 10},
            "reused": False,
        }
    else:
        res |= {
            "warmup_s": [0.2] * spec["warmup"],
            "times_s": [0.1] * spec["repeats"],
            "metrics": [],
            "laps_rows": 10 * spec["factor"],
        }
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(res, fh)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
