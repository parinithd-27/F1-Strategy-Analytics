"""Pydantic models of the API. ``DriverStateOut`` mirrors the Redis hash contract exactly: the 9
fields of ``serving.keys.DRIVER_FIELDS``, with ``""`` (unknown) as ``null``."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

from f1sa.serving.redis_store import StoredDriverState

Compound = Literal["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "UNKNOWN"]
UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


class _Out(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DriverStateOut(_Out):
    position: int = Field(ge=1)
    gap_ahead_s: float | None
    gap_leader_s: float | None
    tyre_compound: Compound
    tyre_age_laps: int | None
    last_lap_time_s: float | None
    undercut_flag: bool
    undercut_prob: float | None
    updated_at: str = Field(description="ISO-8601 UTC with milliseconds, as stored")

    @classmethod
    def from_stored(cls, stored: StoredDriverState) -> DriverStateOut:
        s = stored.state
        return cls(
            position=s.position,
            gap_ahead_s=s.gap_ahead_s,
            gap_leader_s=s.gap_leader_s,
            tyre_compound=s.tyre_compound,  # type: ignore[arg-type]  # validated by DriverState
            tyre_age_laps=s.tyre_age_laps,
            last_lap_time_s=s.last_lap_time_s,
            undercut_flag=s.undercut_flag,
            undercut_prob=s.undercut_prob,
            updated_at=stored.updated_at,
        )


class OrderEntry(_Out):
    driver_id: str
    position: int


class RaceStateOut(_Out):
    season: int
    round: int
    drivers: dict[str, DriverStateOut]
    order: list[OrderEntry]


class RaceOut(_Out):
    season: int
    round: int
    name: str | None
    laps: int
    synthetic: bool = Field(description="tyre/telemetry data of this race is generated (sample)")


class CheckOut(_Out):
    ok: bool
    error: str | None = None


class ModelCheckOut(CheckOut):
    model_version: str
    git_sha: str | None = None
    git_dirty: bool | None = None
    scorer_sha256: str | None = None
    created_utc: str | None = None
    feature_set: str | None = None
    g: float | None = None
    k: int | None = None


class HealthChecks(_Out):
    redis: CheckOut
    kafka: CheckOut
    model: ModelCheckOut


class LakeOut(_Out):
    """Where the races come from. Informational: it never changes the health status."""

    scheme: Literal["file", "hdfs"]
    mode: Literal["sample", "full"] | None = Field(
        description="sample when any listed race is synthetic; null if the clean zone is unreadable"
    )
    synthetic: bool | None


class HealthOut(_Out):
    status: Literal["ok", "degraded"]
    checks: HealthChecks
    lake: LakeOut


class ReplayIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    speed: Literal[1, 10, 60] = Field(description="race-seconds per wall-second")


class ReplayOut(_Out):
    season: int
    round: int
    speed: int
    run_id: str
    results_dir: str
    state: Literal["starting", "running", "finished", "failed"]
    error: str | None
    started_at: str | None
    ended_at: str | None
    stream_returncode: int | None
    producer_returncode: int | None


class ReplayStatusOut(_Out):
    enabled: bool
    max_running: int
    running: list[ReplayOut]
    recent: list[ReplayOut] = Field(description="finished or failed replays, newest first")


class LatencyIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str = Field(pattern=UUID_PATTERN)
    ui_rendered_at_ms: StrictInt = Field(gt=0)


class LatencyOut(_Out):
    event_id: str
    event_type: str | None


class ErrorOut(BaseModel):
    detail: str


# ------------------------------------------------------------------ what-if (Step 15)
DRIVER_ID_PATTERN = r"^[a-z0-9_]{1,64}$"
SimCompound = Literal["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "UNKNOWN"]


class WhatIfIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attacker: str = Field(pattern=DRIVER_ID_PATTERN, description="driverId of the car behind")
    defender: str = Field(
        pattern=DRIVER_ID_PATTERN, description="driverId of the car directly ahead"
    )
    n: StrictInt = Field(default=500, ge=100, le=5000, description="Monte Carlo draws")
    seed: StrictInt = Field(default=42, ge=0, le=2**32 - 1)

    @model_validator(mode="after")
    def _distinct(self) -> WhatIfIn:
        if self.attacker == self.defender:
            raise ValueError("attacker and defender must differ")
        return self


class StrategyOut(_Out):
    strategy: Literal["pit_now", "pit_next_lap", "stay_out"]
    response: Literal["primary", "sensitivity"]
    attacker_pit_lap: int
    defender_pit_lap: int
    evaluation_lap: int = Field(description="the later of the two out-laps")
    feasible: bool
    reason: str | None = Field(description="race_end | no_pit_loss_prior when not feasible")
    p_attacker_ahead: float | None
    expected_gap_s: float | None = Field(description="cum_D - cum_A; positive = attacker ahead")
    interval80_s: tuple[float, float] | None = Field(description="10th and 90th percentile")
    mc_se: float | None
    attacker_new_compound: SimCompound
    defender_new_compound: SimCompound


class CarInputsOut(_Out):
    driver_id: str
    position: int
    compound: SimCompound
    compound_source: str
    tyre_age: int
    new_compound: SimCompound
    pace_laps: int
    noise_source: str
    noise_sd_s: float
    pace_level_s: float
    stop_loss_s: float | None
    stop_delta_s: float | None
    outlap_residual_s: float | None
    tyre_fit_source: str
    tyre_fit_form: str


class WhatIfInputsOut(_Out):
    current_lap: int | None
    decision_lap: int | None
    race_laps: int | None
    gap_s: float
    gap_source: str
    history: str
    traffic_cars: int
    attacker: CarInputsOut
    defender: CarInputsOut
    pit_loss_prior_s: float | None
    pit_loss_source: str | None
    stop_noise_sd_s: float
    synthetic: bool
    warnings: list[str]


class WhatIfModelOut(_Out):
    probability: float | None = Field(description="the stream's calibrated P for the attacker")
    undercut_flag: bool
    model_version: str
    source: Literal["redis:undercut_prob"] = "redis:undercut_prob"


class WhatIfOut(_Out):
    season: int
    round: int
    attacker: str
    defender: str
    n: int
    seed: int
    strategies: list[StrategyOut] = Field(description="defender covers on the next lap")
    sensitivity: list[StrategyOut] = Field(description="defender covers k = 2 laps later")
    model: WhatIfModelOut
    inputs: WhatIfInputsOut
    runtime_ms: float
