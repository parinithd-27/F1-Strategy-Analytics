"""Tests for f1sa.ingest.fastf1_loader. FastF1 network/auth calls are monkeypatched."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from fastf1.exceptions import F1AuthError

from f1sa.config import Settings
from f1sa.ingest import fastf1_loader as fl
from f1sa.ingest.fastf1_loader import (
    CAR_COLUMNS,
    LAP_COLUMNS,
    POS_COLUMNS,
    FastF1AuthError,
    FastF1DataError,
    load_race,
)

FAKE_PASSWORD = "not-a-real-password-123"


def _laps(n: int = 2) -> pd.DataFrame:
    data: dict[str, list[Any]] = {c: [None] * n for c in LAP_COLUMNS}
    data["Driver"] = ["VER", "PER"][:n]
    data["LapNumber"] = [1.0, 1.0][:n]
    data["Compound"] = ["MEDIUM", "HARD"][:n]
    data["LapTime"] = list(pd.to_timedelta([96.1, 96.4][:n], unit="s"))
    data["Extra"] = [0] * n  # extra columns must be dropped
    return pd.DataFrame(data)


def _tel(columns: tuple[str, ...], n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({c: list(range(n)) for c in columns})


class FakeSession:
    def __init__(
        self,
        laps: pd.DataFrame | None = None,
        car: dict[str, pd.DataFrame] | None = None,
        pos: dict[str, pd.DataFrame] | None = None,
        laps_error: Exception | None = None,
    ) -> None:
        self._laps = _laps() if laps is None else laps
        self._laps_error = laps_error
        self.car_data = {"1": _tel(CAR_COLUMNS), "11": _tel(CAR_COLUMNS)} if car is None else car
        self.pos_data = {"1": _tel(POS_COLUMNS), "11": _tel(POS_COLUMNS)} if pos is None else pos
        self.load_kwargs: dict[str, Any] = {}
        self.name = "Race"
        self.drivers = ["1", "11"]
        self.total_laps = 57
        self.event = {"EventName": "Miami Grand Prix", "EventDate": pd.Timestamp("2023-05-07")}

    @property
    def laps(self) -> pd.DataFrame:
        if self._laps_error is not None:
            raise self._laps_error
        return self._laps

    def load(self, **kwargs: Any) -> None:
        self.load_kwargs = kwargs


@pytest.fixture
def cache_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any]]:
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        fl.fastf1.Cache, "configure", lambda **kw: calls.append(("configure", kw["cache_dir"]))
    )
    monkeypatch.setattr(fl.fastf1.Cache, "offline_mode", lambda on: calls.append(("offline", on)))
    return calls


@pytest.fixture
def token_ok(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(fl.f1account, "get_token", lambda: seen.append("get_token"))
    return seen


def _no_auth(*_a: Any, **_k: Any) -> None:
    raise F1AuthError(
        "No F1 account credentials available. Set the environment variables F1_USERNAME "
        "and F1_PASSWORD."
    )


def test_configure_cache_creates_dir_then_sets_offline(
    cache_calls: list[tuple[str, Any]],
) -> None:
    settings = Settings()
    assert not settings.fastf1_cache_dir.exists()
    fl.configure_cache(settings, offline=True)
    assert settings.fastf1_cache_dir.is_dir()
    assert cache_calls == [("configure", str(settings.fastf1_cache_dir)), ("offline", True)]


def test_missing_auth_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch, cache_calls: list[tuple[str, Any]]
) -> None:
    monkeypatch.setattr(fl.f1account, "get_token", _no_auth)
    called: list[bool] = []
    with pytest.raises(FastF1AuthError) as err:
        load_race(2023, 5, Settings(), get_session=lambda *a: called.append(True))
    message = str(err.value)
    assert "F1 TV" in message and "python -m fastf1 auth f1tv --authenticate" in message
    assert err.value.__cause__ is None  # not chained to FastF1 internals
    assert called == []  # never tried to load data without auth


def test_settings_credentials_used_and_never_leaked(
    monkeypatch: pytest.MonkeyPatch, cache_calls: list[tuple[str, Any]]
) -> None:
    monkeypatch.setenv("F1_USERNAME", "someone@example.invalid")
    monkeypatch.setenv("F1_PASSWORD", FAKE_PASSWORD)
    received: list[tuple[str, str]] = []

    def fake_login(user: str, pw: str) -> None:
        received.append((user, pw))
        raise F1AuthError("Sign-in failed: invalid credentials.")

    def fake_get(auth_func: Any = None, force_renew: bool = False) -> None:
        auth_func()

    monkeypatch.setattr(fl, "f1_login", fake_login)
    monkeypatch.setattr(fl.f1account.token_manager, "get", fake_get)
    with pytest.raises(FastF1AuthError) as err:
        fl.ensure_authenticated(Settings())
    assert received == [("someone@example.invalid", FAKE_PASSWORD)]
    assert FAKE_PASSWORD not in str(err.value)
    assert "someone@example.invalid" not in str(err.value)


def test_empty_laps_raise_auth_error(
    cache_calls: list[tuple[str, Any]], token_ok: list[str]
) -> None:
    session = FakeSession(laps=pd.DataFrame(columns=list(LAP_COLUMNS)))
    with pytest.raises(FastF1AuthError, match="returned no laps"):
        load_race(2023, 5, Settings(), get_session=lambda *a: session)
    assert token_ok == ["get_token"]


def test_laps_not_loaded_treated_as_empty(
    cache_calls: list[tuple[str, Any]], token_ok: list[str]
) -> None:
    session = FakeSession(laps_error=RuntimeError("DataNotLoadedError"))
    with pytest.raises(FastF1AuthError):
        load_race(2023, 5, Settings(), get_session=lambda *a: session)


def test_offline_skips_auth_and_reports_missing_cache(
    monkeypatch: pytest.MonkeyPatch, cache_calls: list[tuple[str, Any]]
) -> None:
    monkeypatch.setattr(fl.f1account, "get_token", _no_auth)  # would fail if called
    session = FakeSession(laps=pd.DataFrame())
    with pytest.raises(FastF1DataError, match="no cached FastF1 laps"):
        load_race(2023, 5, Settings(), offline=True, get_session=lambda *a: session)
    assert ("offline", True) in cache_calls


def test_successful_load_selects_columns(
    cache_calls: list[tuple[str, Any]], token_ok: list[str]
) -> None:
    session = FakeSession()
    args: list[tuple[Any, ...]] = []

    def get_session(*a: Any) -> FakeSession:
        args.append(a)
        return session

    data = load_race(2023, 5, Settings(), get_session=get_session)
    assert args == [(2023, 5, "R")]
    assert session.load_kwargs == {
        "laps": True,
        "telemetry": True,
        "weather": False,
        "messages": True,
    }
    assert tuple(data.laps.columns) == LAP_COLUMNS
    assert tuple(data.car_data.columns) == ("DriverNumber", *CAR_COLUMNS)
    assert tuple(data.pos_data.columns) == ("DriverNumber", *POS_COLUMNS)
    assert len(data.car_data) == 6 and set(data.car_data["DriverNumber"]) == {"1", "11"}
    assert data.session_info["event_name"] == "Miami Grand Prix"
    assert data.session_info["event_date"].startswith("2023-05-07")
    assert data.session_info["is_synthetic"] is False
    assert data.session_info["fastf1_version"] == "3.9.2"


def test_missing_lap_column_raises(cache_calls: list[tuple[str, Any]], token_ok: list[str]) -> None:
    session = FakeSession(laps=_laps().drop(columns=["TyreLife"]))
    with pytest.raises(FastF1DataError, match="TyreLife"):
        load_race(2023, 5, Settings(), get_session=lambda *a: session)


def test_empty_telemetry_raises(cache_calls: list[tuple[str, Any]], token_ok: list[str]) -> None:
    session = FakeSession(car={"1": pd.DataFrame(), "11": pd.DataFrame()})
    with pytest.raises(FastF1DataError, match="car telemetry is empty"):
        load_race(2023, 5, Settings(), get_session=lambda *a: session)


def test_missing_telemetry_column_raises(
    cache_calls: list[tuple[str, Any]], token_ok: list[str]
) -> None:
    session = FakeSession(car={"1": _tel(CAR_COLUMNS[:-1])})
    with pytest.raises(FastF1DataError, match="RPM"):
        load_race(2023, 5, Settings(), get_session=lambda *a: session)


@pytest.mark.parametrize(("season", "round_"), [(2017, 1), (2023, 0)])
def test_invalid_race_args(season: int, round_: int) -> None:
    with pytest.raises(ValueError):
        load_race(season, round_, Settings(), get_session=lambda *a: None)
