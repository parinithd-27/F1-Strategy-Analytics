"""Writes the TracingInsights-format test fixtures in this directory (invented values).

    uv run python tests/fixtures/tracinginsights/make_fixtures.py

The files copy the *structure* of a TracingInsights Race folder (42 lap fields, 9 race-control
fields, 18 telemetry channels, "None" strings for missing values). Every value is made up: no
Formula 1 timing data is committed (CLAUDE.md, DATA_LICENSES.md). ``test_ti_fixtures_regenerate``
checks that rerunning this script reproduces the committed bytes.

Race (season 2024, round 1, event "Testland Grand Prix"), 3 drivers x 5 laps, 90 s laps from
session time 1000 s:

* AAA (#1): MEDIUM stint 1 (laps 1-3, pits in on lap 3), HARD stint 2 (laps 4-5, used set: life
  starts at 3). Position 1 every lap.
* BBB (#2): SOFT, one stint; lap 2 has compound and tyre life "None". Position 2.
* CCC (#3): compound UNKNOWN on every lap; lap 5 has lap time "None" (retired). Position 3.

Track status: AllClear, SCDeployed at 1150 s (leader lap 2), AllClear at 1300 s (lap 4).
Race control: SC deployed (lap 2), VSC deployed (lap 3), red flag (lap 4), a penalty for car 2
with ``dNum`` "None" (lap 4), a blue flag with ``dNum`` "3" (lap 5).
Telemetry: AAA and BBB, laps 1-5; consecutive lap files share their boundary sample; AAA lap 4
has a 24 s gap (coverage ``partial``); CCC has no telemetry (coverage ``none``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
RACE = HERE / "Testland Grand Prix" / "Race"
START = 1000.0
LAP_S = 90.0
DRIVERS = (("AAA", "1", "Team A", 0.0), ("BBB", "2", "Team B", 0.5), ("CCC", "3", "Team C", 1.0))
N = "None"


def _laps() -> dict[str, list[Any]]:
    cols: dict[str, list[Any]] = {}

    def add(**row: Any) -> None:
        for key, value in row.items():
            cols.setdefault(key, []).append(value)

    for code, num, team, offset in DRIVERS:
        for lap in range(1, 6):
            lst = START + offset + LAP_S * (lap - 1)
            ses = lst + LAP_S
            if code == "AAA":
                compound, stint = ("MEDIUM", 1) if lap <= 3 else ("HARD", 2)
                life: Any = lap if lap <= 3 else lap - 1
                fresh = lap <= 3
            elif code == "BBB":
                compound, stint, life, fresh = "SOFT", 1, lap, True
                if lap == 2:
                    compound, life = N, N
            else:
                compound, stint, life, fresh = "UNKNOWN", 1, lap, True
            add(
                time=N if (code == "CCC" and lap == 5) else LAP_S,
                lap=lap,
                compound=compound,
                stint=stint,
                s1=N if lap == 1 else 30.0,
                s2=30.0,
                s3=30.0,
                ms1="111",
                ms2=N,
                ms3="111",
                life=life,
                pos={"AAA": 1, "BBB": 2, "CCC": 3}[code],
                status="1" if lap in (1, 5) else "4",
                pb=False,
                sesT=ses,
                drv=code,
                dNum=num,
                pout=(lst + 5.0) if (code == "AAA" and lap == 4) else N,
                pin=(ses - 5.0) if (code == "AAA" and lap == 3) else N,
                s1T=N if lap == 1 else lst + 30.0,
                s2T=lst + 60.0,
                s3T=ses,
                vi1=250.0,
                vi2=260.0,
                vfl=N if lap == 1 else 270.0,
                vst=300.0,
                fresh=fresh,
                team=team,
                lST=lst,
                lSD="2024-03-01T15:00:00.000000000",
                **{"del": False},
                delR=N,
                ff1G=False,
                iacc=lap not in (1, 3, 4),
                wT=lst,
                wAT=25.0,
                wH=50.0,
                wP=1010.0,
                wR=False,
                wTT=35.0,
                wWD=180,
                wWS=2.0,
            )
    return cols


def _telemetry(code: str, lap: int) -> dict[str, Any]:
    """Samples at 0, 15, 30, 45, 60, 75, 90 s (the last one is the next lap's first)."""
    times = [0.0, 15.0, 30.0, 45.0, 60.0, 75.0, 90.0]
    if code == "AAA" and lap == 4:
        times = [0.0, 15.0, 30.0, 33.0, 36.0, 60.0, 75.0, 90.0]  # 24 s gap (36 -> 60)
    base = 200.0 + 10 * lap + (5.0 if code == "BBB" else 0.0)
    speed = [base + i for i, _ in enumerate(times)]
    throttle = [100.0, 97.9, 98.0, 0.0, 100.0, 99.5, 100.0, 100.0][: len(times)]
    brake = [0, 0, 0, 1, 0, 0, 0, 0][: len(times)]
    gear = [7, 7, 8, 3, 3, 7, 8, 8][: len(times)]
    rpm = [11000.0 + 10 * i for i, _ in enumerate(times)]
    n = len(times)
    return {
        "tel": {
            "time": times,
            "rpm": rpm,
            "speed": speed,
            "gear": gear,
            "throttle": throttle,
            "brake": brake,
            "drs": [0] * n,
            "distance": [t * 55.0 for t in times],
            "rel_distance": [t / LAP_S for t in times],
            "DriverAhead": [N] * n,
            "DistanceToDriverAhead": [N] * n,
            "acc_x": [0.1] * n,
            "acc_y": [0.0] * n,
            "acc_z": [0.0] * n,
            "x": [1.0] * n,
            "y": [2.0] * n,
            "z": [3.0] * n,
            "dataKey": f"2024-Testland Grand Prix-Race-{code}-{lap}",
        }
    }


def files() -> dict[Path, bytes]:
    def dump(obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    out: dict[Path, bytes] = {
        RACE / "session_laptimes.json": dump(_laps()),
        RACE / "rcm.json": dump(
            {
                "time": [
                    "2024-03-01T15:10:00.000000000",
                    "2024-03-01T15:12:00.000000000",
                    "2024-03-01T15:14:00.000000000",
                    "2024-03-01T15:15:00.000000000",
                    "2024-03-01T15:16:00.000000000",
                ],
                "cat": ["SafetyCar", "SafetyCar", "Flag", "Other", "Flag"],
                "msg": [
                    "SAFETY CAR DEPLOYED",
                    "VIRTUAL SAFETY CAR DEPLOYED",
                    "RED FLAG",
                    "FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR 2 (BBB) - SPEEDING IN PIT LANE",
                    "WAVED BLUE FLAG FOR CAR 3 (CCC) TIMED AT 15:16:00",
                ],
                "status": [N, N, N, N, N],
                "flag": [N, N, "RED", N, "BLUE"],
                "scope": [N, N, "Track", N, "Driver"],
                "sector": [N, N, N, N, N],
                "dNum": [N, N, N, N, "3"],
                "lap": [2, 3, 4, 4, 5],
            }
        ),
        RACE / "session_info.json": dump(
            {
                "t0_date": "2024-03-01T14:00:00.000000",
                "session_start_time": START,
                "total_laps": 5,
                "track_status": {
                    "Time": [0.0, 1150.0, 1300.0],
                    "Status": ["1", "4", "1"],
                    "Message": ["AllClear", "SCDeployed", "AllClear"],
                },
                "session_status": {"Time": [5.0, START], "Status": ["Inactive", "Started"]},
                "session_info": {"Meeting": {"Key": 1, "Name": "Testland Grand Prix"}},
            }
        ),
        RACE / "drivers.json": dump(
            {"drivers": [{"driver": c, "team": t} for c, _, t, _ in DRIVERS]}
        ),
    }
    for code in ("AAA", "BBB"):
        for lap in range(1, 6):
            out[RACE / code / f"{lap}_tel.json"] = dump(_telemetry(code, lap))
    return out


def main() -> None:
    for path, data in files().items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


if __name__ == "__main__":
    main()
