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
from pathlib import Path
from typing import Any

import fastf1
import pandas as pd
from fastf1.exceptions import F1AuthError, RateLimitExceededError
from fastf1.internals import f1account
from fastf1.internals.f1account.login import login as f1_login

from f1sa.config import Settings
from f1sa.lake.paths import is_within

logger = logging.getLogger(__name__)

LAP_COLUMNS = (
    "Driver",
    "DriverNumber",
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


class FastF1RateLimitError(RuntimeError):
    """FastF1 hit one of its hard API rate limits; continuing would fail every later race."""


@dataclass(frozen=True, eq=False)
class RaceData:
    """Validated FastF1 race-session data."""

    season: int
    round: int
    laps: pd.DataFrame
    car_data: dict[str, pd.DataFrame]
    """Per driver abbreviation (e.g. ``"VER"``) -> car telemetry for the race."""
    pos_data: dict[str, pd.DataFrame]
    """Per driver abbreviation -> position data for the race."""
    session_info: dict[str, Any] = field(default_factory=dict)


def configure_cache(settings: Settings, offline: bool = False) -> None:
    """Point FastF1's cache at ``settings.fastf1_cache_dir`` (created if missing).

    ``Cache.configure`` requires an existing directory; offline mode must be set afterwards.
    """
    cache_dir = settings.fastf1_cache_dir
    if not is_within(cache_dir, Path("data")):
        raise ValueError(
            f"FASTF1_CACHE_DIR must be inside ./data (gitignored); got {cache_dir}. "
            "FastF1 data may never be written outside data/."
        )
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


def _by_abbreviation(
    per_driver: Mapping[str, pd.DataFrame],
    abbreviations: Mapping[str, str],
    columns: tuple[str, ...],
    what: str,
) -> dict[str, pd.DataFrame]:
    """Select ``columns`` per driver and key the result by abbreviation (e.g. ``VER``)."""
    out: dict[str, pd.DataFrame] = {}
    for number, tel in per_driver.items():
        if tel is None or len(tel) == 0:
            continue
        abbr = abbreviations.get(str(number))
        if abbr is None:
            logger.warning("%s: driver number %s has no laps; skipped", what, number)
            continue
        frame = _select(tel, columns, f"{what} for driver {abbr}")
        frame.insert(0, "DriverNumber", str(number))
        frame.insert(0, "Driver", abbr)
        out[abbr] = frame
    if not out:
        raise FastF1DataError(f"{what} is empty for every driver")
    return out


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
    check_auth: bool = True,
    get_session: Callable[..., Any] | None = None,
) -> RaceData:
    """Load a race session's laps, car telemetry and position data, validated.

    Raises:
        FastF1AuthError: sign-in failed, or (online) the session has no laps.
        FastF1DataError: laps exist but telemetry/position data or columns are missing, or
            (offline) nothing is cached for this session.
        FastF1RateLimitError: FastF1 refused to send more requests (its hard limits).
    """
    if season < 2018:
        raise ValueError(f"FastF1 timing data starts in 2018, got season {season}")
    if round_ < 1:
        raise ValueError(f"round must be >= 1, got {round_}")
    configure_cache(settings, offline=offline)
    if not offline and check_auth:
        ensure_authenticated(settings)

    try:
        session = (get_session or fastf1.get_session)(season, round_, "R")
        session.load(laps=True, telemetry=True, weather=False, messages=True)
    except RateLimitExceededError as exc:
        raise FastF1RateLimitError(f"FastF1 rate limit reached: {exc}") from exc

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

    lap_table = _select(laps, LAP_COLUMNS, "laps")
    abbreviations = {
        str(num): str(abbr)
        for num, abbr in zip(lap_table["DriverNumber"], lap_table["Driver"], strict=True)
        if pd.notna(num) and pd.notna(abbr)
    }
    return RaceData(
        season=season,
        round=round_,
        laps=lap_table,
        car_data=_by_abbreviation(car_data, abbreviations, CAR_COLUMNS, "car telemetry"),
        pos_data=_by_abbreviation(pos_data, abbreviations, POS_COLUMNS, "position data"),
        session_info=_session_info(session, season, round_),
    )
