from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from f1sa.api.catalog import CatalogError
from f1sa.api.deps import Deps, Race
from f1sa.api.models import ErrorOut, RaceOut, RaceStateOut
from f1sa.api.state import StateContractError, driver_ids_from_keys, read_race_state

log = logging.getLogger(__name__)
router = APIRouter(tags=["races"])


@router.get(
    "/races",
    response_model=list[RaceOut],
    responses={503: {"model": ErrorOut, "description": "clean zone unavailable"}},
)
async def list_races(deps: Deps) -> list[RaceOut]:
    """Races in the clean zone that can be replayed. 2025 only with ``ALLOW_TEST_SEASON=1``."""
    try:
        races = await deps.catalog.races()
    except CatalogError as exc:
        log.error("race catalog unavailable", extra={"reason": str(exc)})
        raise HTTPException(503, "clean zone unavailable") from exc
    return [
        RaceOut(season=r.season, round=r.round, name=r.name, laps=r.laps, synthetic=r.synthetic)
        for r in races
    ]


@router.get(
    "/races/{season}/{round}/state",
    response_model=RaceStateOut,
    responses={
        404: {"model": ErrorOut, "description": "the race has no keys in Redis"},
        500: {"model": ErrorOut, "description": "a stored hash breaks the contract"},
        503: {"model": ErrorOut, "description": "Redis unavailable"},
    },
)
async def race_state(race: Race, deps: Deps) -> RaceStateOut:
    """Every driver hash and the order sorted set of the race."""
    s, r = race.season, race.round
    found = await deps.store.race_keys(s, r)
    if not found:
        raise HTTPException(404, f"no live state for {s} R{r}")
    try:
        return await read_race_state(deps.redis, s, r, driver_ids_from_keys(found, s, r))
    except StateContractError as exc:
        log.error(
            "driver hash breaks the contract",
            extra={"season": s, "round": r, "driver_id": exc.driver_id},
        )
        raise HTTPException(500, "stored driver state breaks the contract") from exc
