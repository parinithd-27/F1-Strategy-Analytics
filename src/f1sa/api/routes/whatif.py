"""``POST /api/races/{season}/{round}/whatif``: pit now / pit next lap / stay out (#26, S15).

The decision state is read at request time: the race's Redis hashes and order (``api.state``, the
serving contract) and its recorded lap history (``api.lap_log``). The simulation itself
(``sim.whatif``, pure Python + NumPy) runs in a worker thread.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, HTTPException

from f1sa.api.catalog import CatalogError
from f1sa.api.deps import Deps, Race
from f1sa.api.models import (
    CarInputsOut,
    ErrorOut,
    StrategyOut,
    WhatIfIn,
    WhatIfInputsOut,
    WhatIfModelOut,
    WhatIfOut,
)
from f1sa.api.state import StateContractError, driver_ids_from_keys, read_race_state
from f1sa.sim.race_sim import StrategyResult
from f1sa.sim.whatif import (
    CarInputs,
    LiveDriver,
    NoDecisionState,
    NotAdjacent,
    SimUnavailable,
    UnknownDriver,
    WhatIfError,
    WhatIfResult,
)

log = logging.getLogger(__name__)
router = APIRouter(tags=["whatif"])


def _strategy(r: StrategyResult) -> StrategyOut:
    return StrategyOut(
        strategy=r.strategy,
        response=r.response,
        attacker_pit_lap=r.attacker_pit_lap,
        defender_pit_lap=r.defender_pit_lap,
        evaluation_lap=r.evaluation_lap,
        feasible=r.feasible,
        reason=r.reason,
        p_attacker_ahead=r.p_attacker_ahead,
        expected_gap_s=r.expected_gap_s,
        interval80_s=r.interval80_s,
        mc_se=r.mc_se,
        attacker_new_compound=r.attacker_new_compound,  # type: ignore[arg-type]
        defender_new_compound=r.defender_new_compound,  # type: ignore[arg-type]
    )


def _car(c: CarInputs) -> CarInputsOut:
    return CarInputsOut(**{k: getattr(c, k) for k in CarInputsOut.model_fields})


def to_out(result: WhatIfResult, *, model_version: str) -> WhatIfOut:
    i = result.inputs
    return WhatIfOut(
        season=result.season,
        round=result.round,
        attacker=result.attacker,
        defender=result.defender,
        n=result.sim.n,
        seed=result.sim.seed,
        strategies=[_strategy(r) for r in result.sim.results if r.response == "primary"],
        sensitivity=[_strategy(r) for r in result.sim.results if r.response == "sensitivity"],
        model=WhatIfModelOut(
            probability=result.model_probability,
            undercut_flag=result.model_flag,
            model_version=model_version,
        ),
        inputs=WhatIfInputsOut(
            current_lap=i.current_lap,
            decision_lap=i.decision_lap,
            race_laps=i.race_laps,
            gap_s=i.gap_s,
            gap_source=i.gap_source,
            history=i.history,
            traffic_cars=i.traffic_cars,
            attacker=_car(i.attacker),
            defender=_car(i.defender),
            pit_loss_prior_s=i.pit_loss_prior_s,
            pit_loss_source=i.pit_loss_source,
            stop_noise_sd_s=i.stop_noise_sd_s,
            synthetic=i.synthetic,
            warnings=list(i.warnings),
        ),
        runtime_ms=result.runtime_ms,
    )


@router.post(
    "/races/{season}/{round}/whatif",
    response_model=WhatIfOut,
    responses={
        404: {"model": ErrorOut, "description": "unknown race, no live state, or unknown driver"},
        409: {
            "model": ErrorOut,
            "description": "the defender is not directly ahead, or no decision state yet",
        },
        500: {"model": ErrorOut, "description": "a stored hash breaks the contract"},
        503: {"model": ErrorOut, "description": "Redis, the lake or the tyre model unavailable"},
    },
)
async def whatif(body: WhatIfIn, race: Race, deps: Deps) -> WhatIfOut:
    """Compare pit now / pit next lap / stay out (overcut) for the attacker and the car directly
    ahead at the attacker's latest lap, next to the stream's calibrated model probability."""
    s, r = race.season, race.round
    try:
        info = await deps.catalog.get(s, r)
    except CatalogError as exc:
        log.error("race catalog unavailable", extra={"reason": str(exc)})
        raise HTTPException(503, "clean zone unavailable") from exc
    if info is None:
        raise HTTPException(404, f"{s} R{r} is not in the clean zone")
    found = await deps.store.race_keys(s, r)
    if not found:
        raise HTTPException(404, f"no live state for {s} R{r}")
    try:
        state = await read_race_state(deps.redis, s, r, driver_ids_from_keys(found, s, r))
    except StateContractError as exc:
        log.error(
            "driver hash breaks the contract", extra={"season": s, "driver_id": exc.driver_id}
        )
        raise HTTPException(500, "stored driver state breaks the contract") from exc
    drivers = {
        d: LiveDriver(
            position=v.position,
            gap_ahead_s=v.gap_ahead_s,
            gap_leader_s=v.gap_leader_s,
            tyre_compound=v.tyre_compound,
            tyre_age_laps=v.tyre_age_laps,
            last_lap_time_s=v.last_lap_time_s,
            undercut_flag=v.undercut_flag,
            undercut_prob=v.undercut_prob,
        )
        for d, v in state.drivers.items()
    }
    lap_log = await deps.lap_logs.ensure(s, r)
    try:
        result = await asyncio.to_thread(
            deps.whatif.run,
            s,
            r,
            body.attacker,
            body.defender,
            drivers,
            lap_log.records(),
            race_laps=info.laps,
            synthetic=info.synthetic,
            n=body.n,
            seed=body.seed,
        )
    except UnknownDriver as exc:
        raise HTTPException(404, str(exc)) from exc
    except (NotAdjacent, NoDecisionState) as exc:
        raise HTTPException(409, str(exc)) from exc
    except SimUnavailable as exc:
        log.error("what-if unavailable", extra={"reason": str(exc)})
        raise HTTPException(503, str(exc)) from exc
    except CatalogError as exc:
        log.error("lake unavailable for what-if", extra={"reason": str(exc)})
        raise HTTPException(503, "lake unavailable") from exc
    except WhatIfError as exc:
        log.error("what-if failed", extra={"reason": str(exc)})
        raise HTTPException(500, "what-if failed") from exc
    log.info(
        "whatif",
        extra={"season": s, "round": r, "runtime_ms": round(result.runtime_ms, 1), "n": body.n},
    )
    return to_out(result, model_version=deps.model.model_version)
