"""Seeded generator of SYNTHETIC tyre and telemetry tables for the offline sample bundle.

!!! These tables are NOT real Formula 1 compound or telemetry data. !!!
They exist so the pipeline can run end to end without F1 TV credentials. Any model result
computed from them is a smoke test only and must never be reported as a finding. Every row
carries ``is_synthetic=True``. The generating assumptions are documented in
``docs/data_dictionary.md`` (section "Synthetic data").

Inputs are real Jolpica tables (typed frames from ``jolpica_client.parse_laps`` /
``parse_pitstops``); nothing here performs I/O or uses FastF1.

Determinism: every random draw comes from a ``numpy.random.SeedSequence`` keyed by
``(seed, season, round, crc32(driver_id), stream)``, so a driver's rows depend only on the seed and
that driver's own data, never on row order or on which other drivers are present.
"""

from __future__ import annotations

import zlib
from collections.abc import Sequence

import numpy as np
import pandas as pd

GENERATOR_VERSION = "synthetic-v1"
DRY_COMPOUNDS = ("SOFT", "MEDIUM", "HARD")
_STREAM_RACE, _STREAM_TYRES, _STREAM_TELEMETRY = 0, 1, 2

STINTS_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "driver_id": "string",
    "stint": "Int8",
    "start_lap": "Int16",
    "end_lap": "Int16",
    "laps": "Int16",
}
TYRE_STINTS_SCHEMA = {
    **STINTS_SCHEMA,
    "compound": "string",
    "tyre_age_at_start": "Int16",
    "is_synthetic": "boolean",
    "generator_version": "string",
    "seed": "Int64",
}
LAP_TELEMETRY_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "driver_id": "string",
    "lap": "Int16",
    "mean_speed_kph": "Float64",
    "max_speed_kph": "Float64",
    "pct_full_throttle": "Float64",
    "brake_fraction": "Float64",
    "gear_changes": "Int16",
    "mean_rpm": "Float64",
    "is_synthetic": "boolean",
    "generator_version": "string",
    "seed": "Int64",
}

# --- documented generating assumptions (mirrored in docs/data_dictionary.md) ----------------
START_COMPOUND_P = {"SOFT": 0.25, "MEDIUM": 0.55, "HARD": 0.20}
USED_SET_P = 0.2
"""Probability that a stint starts on a used set (age 1-3 laps); otherwise a new set (age 0)."""
CIRCUIT_RANGES = {
    "mean_speed_kph": (190.0, 230.0),
    "max_speed_kph": (315.0, 340.0),
    "pct_full_throttle": (0.55, 0.75),
    "brake_fraction": (0.12, 0.22),
    "gear_changes": (40.0, 70.0),
    "mean_rpm": (10500.0, 11500.0),
}
TYRE_AGE_SPEED_LOSS_KPH = 0.03
"""Mean-speed loss per lap of tyre age (a mild degradation effect)."""


def _rng(seed: int, season: int, round_: int, driver_id: str, stream: int) -> np.random.Generator:
    key = zlib.crc32(driver_id.encode("utf-8")) if driver_id else 0
    return np.random.default_rng(np.random.SeedSequence([seed, season, round_, key, stream]))


def _check_columns(df: pd.DataFrame, columns: Sequence[str], what: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{what} is missing columns {missing}")


def _frame(rows: list[dict[str, object]], schema: dict[str, str]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(schema)).astype(schema).reset_index(drop=True)


# ============================================================================== stints
def infer_stints(laps: pd.DataFrame, pitstops: pd.DataFrame) -> pd.DataFrame:
    """Stints per driver from real Jolpica laps and pit stops.

    * A driver's race ends at their last lap with a timing (DNFs stop early; lapped cars have
      fewer laps). Drivers without any lap (DNS) get no stints.
    * A stop on lap ``p`` ends a stint at ``p``; the next stint starts at ``p + 1``.
    * Stops on or after the driver's last lap (e.g. retiring in the pit lane) start no stint;
      duplicate stop laps are collapsed.
    * Limitation: tyre changes under a red flag are not pit stops in Jolpica and are invisible.
    """
    _check_columns(laps, ("season", "round", "lap", "driver_id"), "laps")
    _check_columns(pitstops, ("season", "round", "driver_id", "lap"), "pitstops")
    rows: list[dict[str, object]] = []
    timed = laps.dropna(subset=["lap", "driver_id"])
    last_laps = timed.groupby(["season", "round", "driver_id"], sort=True)["lap"].max()
    stop_laps: dict[tuple[int, int, str], list[int]] = {}
    for rec in pitstops.dropna(subset=["lap", "driver_id"]).itertuples(index=False):
        key = (int(rec.season), int(rec.round), str(rec.driver_id))
        stop_laps.setdefault(key, []).append(int(rec.lap))
    for (season, round_, driver), last in last_laps.items():
        key = (int(season), int(round_), str(driver))
        last_lap = int(last)
        stops = sorted({p for p in stop_laps.get(key, []) if 1 <= p < last_lap})
        starts = [1] + [p + 1 for p in stops]
        ends = [*stops, last_lap]
        for i, (start, end) in enumerate(zip(starts, ends, strict=True), start=1):
            rows.append(
                {
                    "season": key[0],
                    "round": key[1],
                    "driver_id": key[2],
                    "stint": i,
                    "start_lap": start,
                    "end_lap": end,
                    "laps": end - start + 1,
                }
            )
    return _frame(rows, STINTS_SCHEMA)


# ============================================================================== tyres
def _compound_probs(stint: int, rel_len: float) -> np.ndarray:
    """P(SOFT, MEDIUM, HARD) for a stint of relative length ``rel_len`` (laps / race laps)."""
    if stint == 1:
        base = np.array([START_COMPOUND_P[c] for c in DRY_COMPOUNDS])
        if rel_len > 0.45:
            base = np.array([0.10, 0.45, 0.45])
        elif rel_len < 0.20:
            base = np.array([0.45, 0.45, 0.10])
    elif rel_len > 0.40:
        base = np.array([0.05, 0.35, 0.60])
    elif rel_len < 0.20:
        base = np.array([0.55, 0.35, 0.10])
    else:
        base = np.array([0.20, 0.50, 0.30])
    return np.asarray(base / base.sum(), dtype=float)


def synth_tyre_stints(stints: pd.DataFrame, seed: int) -> pd.DataFrame:
    """Assign a SYNTHETIC dry compound and starting tyre age to every stint.

    Every driver with two or more stints uses at least two different compounds (mirrors the
    FIA dry-race rule). Wet/intermediate tyres are never generated.
    """
    _check_columns(stints, tuple(STINTS_SCHEMA), "stints")
    rows: list[dict[str, object]] = []
    race_laps = stints.groupby(["season", "round"])["end_lap"].max()
    for (season, round_, driver), group in stints.groupby(
        ["season", "round", "driver_id"], sort=True
    ):
        g = group.sort_values("stint")
        rng = _rng(seed, int(season), int(round_), str(driver), _STREAM_TYRES)
        total = max(1, int(race_laps.loc[(season, round_)]))
        compounds: list[str] = []
        ages: list[int] = []
        for rec in g.itertuples(index=False):
            probs = _compound_probs(int(rec.stint), int(rec.laps) / total)
            compounds.append(DRY_COMPOUNDS[int(rng.choice(3, p=probs))])
            ages.append(int(rng.integers(1, 4)) if rng.random() < USED_SET_P else 0)
        if len(compounds) >= 2 and len(set(compounds)) == 1:
            # longest stint after the first switches to another compound
            later = list(range(1, len(compounds)))
            idx = max(later, key=lambda i: (int(g.iloc[i]["laps"]), -i))
            # pick the replacement with that stint's own length-based odds (long -> HARD)
            others = [i for i, c in enumerate(DRY_COMPOUNDS) if c != compounds[0]]
            rel = int(g.iloc[idx]["laps"]) / total
            weights = _compound_probs(idx + 1, rel)[others]
            choice = int(rng.choice(len(others), p=weights / weights.sum()))
            compounds[idx] = DRY_COMPOUNDS[others[choice]]
        for rec, compound, age in zip(g.itertuples(index=False), compounds, ages, strict=True):
            rows.append(
                {
                    **{k: getattr(rec, k) for k in STINTS_SCHEMA},
                    "compound": compound,
                    "tyre_age_at_start": age,
                    "is_synthetic": True,
                    "generator_version": GENERATOR_VERSION,
                    "seed": seed,
                }
            )
    return _frame(rows, TYRE_STINTS_SCHEMA)


# ============================================================================== telemetry
def _tyre_age_per_lap(tyre_stints: pd.DataFrame, driver_laps: np.ndarray) -> np.ndarray:
    age = np.zeros(len(driver_laps), dtype=float)
    for rec in tyre_stints.itertuples(index=False):
        mask = (driver_laps >= int(rec.start_lap)) & (driver_laps <= int(rec.end_lap))
        age[mask] = int(rec.tyre_age_at_start) + (driver_laps[mask] - int(rec.start_lap))
    return age


def synth_lap_telemetry(laps: pd.DataFrame, tyre_stints: pd.DataFrame, seed: int) -> pd.DataFrame:
    """SYNTHETIC per-lap telemetry aggregates, one row per real (driver, lap) timing.

    Per race, circuit baselines are drawn uniformly from ``CIRCUIT_RANGES``. Each lap scales
    them by ``pace = driver_median_lap_time / lap_time`` (slow laps such as lap 1, pit in/out
    and safety-car laps get lower speeds, less full throttle and fewer gear changes), applies a
    mild tyre-age speed loss and small Gaussian noise, and clips to physical bounds. A missing
    lap time yields missing aggregates (the row is kept).
    """
    _check_columns(laps, ("season", "round", "lap", "driver_id", "time_s"), "laps")
    _check_columns(tyre_stints, tuple(TYRE_STINTS_SCHEMA), "tyre_stints")
    frames: list[pd.DataFrame] = []
    for (season, round_), race in laps.groupby(["season", "round"], sort=True):
        race_rng = _rng(seed, int(season), int(round_), "", _STREAM_RACE)
        base = {k: float(race_rng.uniform(lo, hi)) for k, (lo, hi) in CIRCUIT_RANGES.items()}
        stints_race = tyre_stints[
            (tyre_stints["season"] == season) & (tyre_stints["round"] == round_)
        ]
        for driver, dlaps in race.groupby("driver_id", sort=True):
            d = dlaps.drop_duplicates(["lap"]).sort_values("lap")
            lap_no = d["lap"].to_numpy(dtype=float)
            t = d["time_s"].to_numpy(dtype=float, na_value=np.nan)
            n = len(d)
            rng = _rng(seed, int(season), int(round_), str(driver), _STREAM_TELEMETRY)
            noise = rng.standard_normal((6, n))
            speed_offset = float(rng.normal(0.0, 1.0))
            median = np.nanmedian(t) if np.isfinite(t).any() else np.nan
            with np.errstate(invalid="ignore", divide="ignore"):
                pace = np.clip(median / t, 0.5, 1.1)
            age = _tyre_age_per_lap(stints_race[stints_race["driver_id"] == driver], lap_no)
            mean_speed = np.clip(
                base["mean_speed_kph"] * pace
                + speed_offset
                - TYRE_AGE_SPEED_LOSS_KPH * age
                + 0.8 * noise[0],
                60.0,
                260.0,
            )
            max_speed = np.clip(
                base["max_speed_kph"] * (0.92 + 0.08 * pace) + 2.0 * noise[1],
                mean_speed + 20.0,
                360.0,
            )
            throttle = np.clip(base["pct_full_throttle"] * pace**2 + 0.015 * noise[2], 0.0, 1.0)
            brake = np.clip(base["brake_fraction"] * pace + 0.01 * noise[3], 0.0, 1.0 - throttle)
            gears = np.clip(np.rint(base["gear_changes"] * pace + 2.0 * noise[4]), 0.0, None)
            rpm = np.clip(base["mean_rpm"] * (0.9 + 0.1 * pace) + 60.0 * noise[5], 7000.0, 13500.0)
            frame = pd.DataFrame(
                {
                    "season": int(season),
                    "round": int(round_),
                    "driver_id": str(driver),
                    "lap": lap_no,
                    "mean_speed_kph": np.round(mean_speed, 3),
                    "max_speed_kph": np.round(max_speed, 3),
                    "pct_full_throttle": np.round(throttle, 4),
                    "brake_fraction": np.round(brake, 4),
                    "gear_changes": gears,
                    "mean_rpm": np.round(rpm, 1),
                }
            )
            missing = ~np.isfinite(t)
            value_cols = list(LAP_TELEMETRY_SCHEMA)[4:10]
            frame.loc[missing, value_cols] = np.nan
            frames.append(frame)
    if not frames:
        return _frame([], LAP_TELEMETRY_SCHEMA)
    out = pd.concat(frames, ignore_index=True)
    out["is_synthetic"] = True
    out["generator_version"] = GENERATOR_VERSION
    out["seed"] = seed
    return out[list(LAP_TELEMETRY_SCHEMA)].astype(LAP_TELEMETRY_SCHEMA).reset_index(drop=True)


def generate(laps: pd.DataFrame, pitstops: pd.DataFrame, seed: int) -> dict[str, pd.DataFrame]:
    """Both SYNTHETIC tables for the given real laps and pit stops."""
    tyres = synth_tyre_stints(infer_stints(laps, pitstops), seed)
    return {"tyre_stints": tyres, "lap_telemetry": synth_lap_telemetry(laps, tyres, seed)}
