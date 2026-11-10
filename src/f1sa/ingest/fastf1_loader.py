"""FastF1 3.9.2 race-session loader with loud failure on missing auth or empty data.

FastF1 3.9 needs an F1 TV Access login for livetiming data. When sign-in fails, its transport
adapter only logs a warning and the session comes back with *no laps* instead of raising. This
module checks authentication up front and validates what was loaded, raising
:class:`FastF1AuthError` / :class:`FastF1DataError` rather than continuing silently.

Credentials are never logged or included in messages. They are taken from ``Settings``
(``F1_USERNAME``/``F1_PASSWORD`` in the environment or ``.env``), or FastF1 falls back to its
stored token / interactive prompt (``uv run python -m fastf1 auth f1tv --authenticate``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import Any

import fastf1
import pandas as pd
from fastf1.exceptions import F1AuthError
from fastf1.internals import f1account
from fastf1.internals.f1account.login import login as f1_login

from f1sa.config import Settings

logger = logging.getLogger(__name__)

LAP_COLUMNS = (
    "Driver",
    "LapNumber",
    "LapTime",
    "Compound",
    "TyreLife",
    "Stint",
    "PitInTime",
    "PitOutTime",
    "TrackStatus",
    "Position",
    "Time",
    "LapStartTime",
)
CAR_COLUMNS = ("Date", "SessionTime", "Time", "Speed", "Throttle", "Brake", "nGear", "RPM")
POS_COLUMNS = ("Date", "SessionTime", "Time", "X", "Y", "Z", "Status")

AUTH_HELP = (
    "FastF1 needs an F1 TV Access login to load timing data. Either set F1_USERNAME and "
    "F1_PASSWORD (environment or .env), or sign in once with "
    "`uv run python -m fastf1 auth f1tv --authenticate`, then retry. "
    "Use --offline to read only from the local FastF1 cache."
)


class FastF1AuthError(RuntimeError):
    """FastF1 could not authenticate, or returned an empty session (its symptom of no auth)."""


class FastF1DataError(RuntimeError):
    """The session loaded but required data or columns are missing."""


@dataclass(frozen=True, eq=False)
class RaceData:
    """Validated FastF1 race-session data."""

    season: int
    round: int
    laps: pd.DataFrame
    car_data: pd.DataFrame
    pos_data: pd.DataFrame
    session_info: dict[str, Any] = field(default_factory=dict)


def configure_cache(settings: Settings, offline: bool = False) -> None:
    """Point FastF1's cache at ``settings.fastf1_cache_dir`` (created if missing).

    ``Cache.configure`` requires an existing directory; offline mode must be set afterwards.
    """
    cache_dir = settings.fastf1_cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.configure(cache_dir=str(cache_dir))
    fastf1.Cache.offline_mode(offline)


def ensure_authenticated(settings: Settings) -> None:
    """Make sure a valid F1 TV token exists, signing in if needed. Raises FastF1AuthError."""
    try:
        if settings.f1_username is not None and settings.f1_password is not None:
            username, password = settings.f1_username, settings.f1_password

            def _login() -> Any:
                return f1_login(username.get_secret_value(), password.get_secret_value())

            f1account.token_manager.get(auth_func=_login)
        else:
            f1account.get_token()
    except F1AuthError as exc:
        # FastF1's messages describe the failure without echoing credentials.
        raise FastF1AuthError(f"F1 TV sign-in failed ({exc}). {AUTH_HELP}") from None


def _select(df: pd.DataFrame, columns: tuple[str, ...], what: str) -> pd.DataFrame:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise FastF1DataError(f"{what} is missing columns {missing}")
    return pd.DataFrame(df.loc[:, list(columns)]).reset_index(drop=True)


def _concat_by_driver(
    per_driver: Mapping[str, pd.DataFrame], columns: tuple[str, ...], what: str
) -> pd.DataFrame:
    frames = []
    for driver, tel in per_driver.items():
        if tel is None or len(tel) == 0:
            continue
        frame = _select(tel, columns, f"{what} for driver {driver}")
        frame.insert(0, "DriverNumber", str(driver))
        frames.append(frame)
    if not frames:
        raise FastF1DataError(f"{what} is empty for every driver")
    return pd.concat(frames, ignore_index=True)


def _session_info(session: Any, season: int, round_: int) -> dict[str, Any]:
    event = getattr(session, "event", None)

    def ev(key: str) -> Any:
        value: Any
        try:
            value = event[key] if event is not None else None
        except (KeyError, TypeError):
            return None
        return value.isoformat() if hasattr(value, "isoformat") else value

    return {
        "season": season,
        "round": round_,
        "session": getattr(session, "name", "Race"),
        "event_name": ev("EventName"),
        "event_date": ev("EventDate"),
        "drivers": [str(d) for d in getattr(session, "drivers", [])],
        "total_laps": getattr(session, "total_laps", None),
        "fastf1_version": version("fastf1"),
        "is_synthetic": False,
    }


def load_race(
    season: int,
    round_: int,
    settings: Settings,
    *,
    offline: bool = False,
    get_session: Callable[..., Any] | None = None,
) -> RaceData:
    """Load a race session's laps, car telemetry and position data, validated.

    Raises:
        FastF1AuthError: sign-in failed, or (online) the session has no laps.
        FastF1DataError: laps exist but telemetry/position data or columns are missing, or
            (offline) nothing is cached for this session.
    """
    if season < 2018:
        raise ValueError(f"FastF1 timing data starts in 2018, got season {season}")
    if round_ < 1:
        raise ValueError(f"round must be >= 1, got {round_}")
    configure_cache(settings, offline=offline)
    if not offline:
        ensure_authenticated(settings)

    session = (get_session or fastf1.get_session)(season, round_, "R")
    session.load(laps=True, telemetry=True, weather=False, messages=True)

    try:
        laps = session.laps
    except Exception as exc:  # DataNotLoadedError and friends: treat as "no laps"
        laps = None
        logger.debug("session.laps unavailable: %s", type(exc).__name__)
    if laps is None or len(laps) == 0:
        if offline:
            raise FastF1DataError(
                f"no cached FastF1 laps for {season} round {round_}; run once without --offline"
            )
        raise FastF1AuthError(
            f"FastF1 returned no laps for {season} round {round_}. FastF1 3.9 does this instead "
            f"of raising when it is not signed in to F1 TV. {AUTH_HELP}"
        )

    try:
        car_data = session.car_data
        pos_data = session.pos_data
    except Exception as exc:
        raise FastF1DataError(
            f"telemetry not loaded for {season} round {round_}: {type(exc).__name__}"
        ) from exc

    return RaceData(
        season=season,
        round=round_,
        laps=_select(laps, LAP_COLUMNS, "laps"),
        car_data=_concat_by_driver(car_data, CAR_COLUMNS, "car telemetry"),
        pos_data=_concat_by_driver(pos_data, POS_COLUMNS, "position data"),
        session_info=_session_info(session, season, round_),
    )
