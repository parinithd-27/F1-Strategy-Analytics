from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from f1sa.api.catalog import CatalogError
from f1sa.api.deps import Deps, Race
from f1sa.api.models import DriverStateOut, ErrorOut, OrderEntry, RaceOut, RaceStateOut
from f1sa.serving import keys as K
from f1sa.serving.redis_store import StoreError, decode_state

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
    return [RaceOut(season=r.season, round=r.round, name=r.name, laps=r.laps) for r in races]


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
    prefix = K.driver_key(s, r, "x")[:-1]
    driver_ids: list[str] = []
    for key in found:
        if not key.startswith(prefix):
            continue
        try:
            driver_ids.append(K.validate_driver_id(key[len(prefix) :]))
        except K.InvalidKeyError:
            log.warning("ignored key outside the contract", extra={"season": s, "round": r})
    pipe = deps.redis.pipeline(transaction=False)
    for d in driver_ids:
        pipe.hgetall(K.driver_key(s, r, d))
    hashes = await pipe.execute() if driver_ids else []
    drivers: dict[str, DriverStateOut] = {}
    for d, raw in zip(driver_ids, hashes, strict=True):
        try:
            stored = decode_state(raw)
        except (StoreError, ValueError) as exc:
            log.error(
                "driver hash breaks the contract",
                extra={"season": s, "round": r, "driver_id": d, "exc_class": type(exc).__name__},
            )
            raise HTTPException(500, "stored driver state breaks the contract") from exc
        if stored is not None:  # expired between SCAN and HGETALL
            drivers[d] = DriverStateOut.from_stored(stored)
    order = [OrderEntry(driver_id=d, position=p) for d, p in await deps.store.get_order(s, r)]
    return RaceStateOut(season=s, round=r, drivers=drivers, order=order)
