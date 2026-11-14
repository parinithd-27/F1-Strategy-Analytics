"""``make demo`` (Step 17): start a sample replay through the API and wait until it shows data.

Runs in the app container against ``http://api:8000`` (stdlib only, no Spark):

1. ``GET /api/health``: Redis, Kafka and the model must be ok, the model must be the one the demo
   built (``sample_deploy_real_G3.5_k2``), and the lake must be the synthetic sample lake (that
   flag is what the dashboard's SYNTHETIC DEMO DATA banner shows);
2. ``GET /api/races``: the race must be listed and marked ``synthetic``;
3. ``GET /api/replays`` (replay enabled, the race not already running), then
   ``POST /api/races/{s}/{r}/replay {"speed": V}``;
4. wait until ``/state`` holds at least ``--min-drivers`` cars (``--wait state``, the default), or
   also until the replay has ``finished`` (``--wait finished``, used by CI). A replay that
   ``failed`` stops the wait with its error;
5. print the dashboard URL.

Exit codes: 0 ok, 1 a check failed or timed out (the message says which), 2 usage.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

DEFAULT_API = "http://api:8000"
DEFAULT_DASHBOARD = "http://localhost:3000"
DEFAULT_MODEL_VERSION = "sample_deploy_real_G3.5_k2"
SPEEDS = (1, 10, 60)
WAITS = ("state", "finished")
BANNER = "SYNTHETIC DEMO DATA"

# (method, url, json body or None) -> (HTTP status, parsed JSON or None); status 0 = no answer
Fetch = Callable[[str, str, Mapping[str, Any] | None], tuple[int, Any]]


class DemoError(RuntimeError):
    """A demo check failed; the message says what to do."""


def http_json(
    method: str, url: str, body: Mapping[str, Any] | None = None, timeout_s: float = 10.0
) -> tuple[int, Any]:
    """One JSON request. HTTP errors return their status and parsed body; a connection error
    returns status 0 (the API may still be starting)."""
    data = None if body is None else json.dumps(body).encode()
    headers = {"accept": "application/json"}
    if data is not None:
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return int(resp.status), _parse(resp.read())
    except urllib.error.HTTPError as exc:
        return int(exc.code), _parse(exc.read())
    except (urllib.error.URLError, OSError):
        return 0, None


def _parse(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode() or "null")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


@dataclass(frozen=True)
class DemoConfig:
    api: str = DEFAULT_API
    season: int = 2024
    round: int = 5
    speed: int = 60
    wait: str = "state"
    model_version: str = DEFAULT_MODEL_VERSION
    min_drivers: int = 20
    timeout_s: float = 600.0
    poll_s: float = 1.0
    dashboard: str = DEFAULT_DASHBOARD

    def __post_init__(self) -> None:
        if self.speed not in SPEEDS:
            raise ValueError(f"speed must be one of {SPEEDS}, got {self.speed}")
        if self.wait not in WAITS:
            raise ValueError(f"wait must be one of {WAITS}, got {self.wait!r}")
        if self.min_drivers < 1 or self.timeout_s <= 0 or self.poll_s <= 0:
            raise ValueError("min_drivers, timeout_s and poll_s must be positive")

    def url(self, path: str) -> str:
        return self.api.rstrip("/") + path

    @property
    def race_path(self) -> str:
        return f"/api/races/{self.season}/{self.round}"

    @property
    def dashboard_url(self) -> str:
        return f"{self.dashboard.rstrip('/')}/race/{self.season}/{self.round}"


def _detail(body: Any) -> str:
    if isinstance(body, Mapping) and isinstance(body.get("detail"), str):
        return str(body["detail"])
    return json.dumps(body)[:300]


@dataclass
class Demo:
    cfg: DemoConfig
    fetch: Fetch = http_json
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    out: Callable[[str], None] = print

    def _deadline(self) -> float:
        return self.clock() + self.cfg.timeout_s

    def _expired(self, deadline: float, what: str) -> None:
        if self.clock() >= deadline:
            raise DemoError(f"timed out after {self.cfg.timeout_s:g} s waiting for {what}")
        self.sleep(self.cfg.poll_s)

    # ------------------------------------------------------------------ checks
    def check_health(self) -> Mapping[str, Any]:
        """Wait until the API answers (200 or 503), then require every check to pass."""
        deadline = self._deadline()
        while True:
            status, body = self.fetch("GET", self.cfg.url("/api/health"), None)
            if status in (200, 503) and isinstance(body, Mapping):
                break
            self._expired(deadline, f"the API at {self.cfg.api} (last status {status})")
        checks = body.get("checks", {})
        for name in ("redis", "kafka", "model"):
            c = checks.get(name, {})
            if not c.get("ok"):
                raise DemoError(f"/api/health: {name} not ok: {c.get('error')}")
        version = checks["model"].get("model_version")
        if version != self.cfg.model_version:
            raise DemoError(
                f"/api/health: model {version!r}, expected {self.cfg.model_version!r} "
                "(make demo recreates the api with MODEL_DIR=models/undercut/<that>)"
            )
        lake = body.get("lake", {})
        if lake.get("synthetic") is not True:
            raise DemoError(
                f"/api/health: the lake is not the synthetic sample lake ({lake}); make demo "
                "runs on the sample only"
            )
        self.out(f"api: healthy, model {version}, lake {lake.get('mode')} (synthetic)")
        return body

    def check_race(self) -> Mapping[str, Any]:
        status, body = self.fetch("GET", self.cfg.url("/api/races"), None)
        if status != 200 or not isinstance(body, list):
            raise DemoError(f"/api/races: status {status}: {_detail(body)}")
        for race in body:
            if race.get("season") == self.cfg.season and race.get("round") == self.cfg.round:
                if race.get("synthetic") is not True:
                    raise DemoError(f"{self.cfg.season} R{self.cfg.round} is not synthetic")
                self.out(
                    f"race: {self.cfg.season} R{self.cfg.round} {race.get('name')} (synthetic)"
                )
                return dict(race)
        raise DemoError(
            f"{self.cfg.season} R{self.cfg.round} is not in /api/races "
            "(the sample ETL writes data/lake/clean; run make demo again)"
        )

    def start_replay(self) -> str:
        status, body = self.fetch("GET", self.cfg.url("/api/replays"), None)
        if status != 200 or not isinstance(body, Mapping):
            raise DemoError(f"/api/replays: status {status}: {_detail(body)}")
        if not body.get("enabled"):
            raise DemoError("replay is disabled on the API (make demo runs make api-up REPLAY=1)")
        status, body = self.fetch(
            "POST", self.cfg.url(self.cfg.race_path + "/replay"), {"speed": self.cfg.speed}
        )
        if status == 409:
            raise DemoError(f"replay refused: {_detail(body)}; wait for it to finish, then retry")
        if status != 202 or not isinstance(body, Mapping) or "run_id" not in body:
            raise DemoError(f"replay not started: status {status}: {_detail(body)}")
        run_id = str(body["run_id"])
        self.out(f"replay: {run_id} at {self.cfg.speed}x, outputs in {body.get('results_dir')}")
        return run_id

    def replay_state(self, run_id: str) -> Mapping[str, Any] | None:
        status, body = self.fetch("GET", self.cfg.url("/api/replays"), None)
        if status != 200 or not isinstance(body, Mapping):
            return None
        for job in [*body.get("running", []), *body.get("recent", [])]:
            if job.get("run_id") == run_id:
                return dict(job)
        return None

    def _check_not_failed(self, job: Mapping[str, Any] | None) -> None:
        if job is not None and job.get("state") == "failed":
            raise DemoError(f"replay {job.get('run_id')} failed: {job.get('error')}")

    def wait_for_state(self, run_id: str) -> int:
        """Poll ``/state`` until it holds ``min_drivers`` cars; returns the count."""
        deadline = self._deadline()
        while True:
            self._check_not_failed(self.replay_state(run_id))
            status, body = self.fetch("GET", self.cfg.url(self.cfg.race_path + "/state"), None)
            n = len(body.get("drivers", {})) if status == 200 and isinstance(body, Mapping) else 0
            if n >= self.cfg.min_drivers:
                self.out(f"state: {n} cars in Redis")
                return n
            self._expired(deadline, f"{self.cfg.min_drivers} cars in /state (have {n})")

    def wait_for_finished(self, run_id: str) -> Mapping[str, Any]:
        deadline = self._deadline()
        while True:
            job = self.replay_state(run_id)
            self._check_not_failed(job)
            if job is not None and job.get("state") == "finished":
                self.out(f"replay: finished at {job.get('ended_at')}")
                return job
            state = None if job is None else job.get("state")
            self._expired(deadline, f"replay {run_id} to finish (state {state})")

    def run(self) -> None:
        self.check_health()
        self.check_race()
        run_id = self.start_replay()
        self.wait_for_state(run_id)
        if self.cfg.wait == "finished":
            self.wait_for_finished(run_id)
        self.out(f"Dashboard: {self.cfg.dashboard_url}  ({BANNER})")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m f1sa.demo_replay",
        description="Start a sample replay through the API and wait until it shows data.",
    )
    p.add_argument("--api", default=DEFAULT_API)
    p.add_argument("--season", type=int, default=2024)
    p.add_argument("--round", type=int, default=5)
    p.add_argument("--speed", type=int, choices=SPEEDS, default=60)
    p.add_argument("--wait", choices=WAITS, default="state")
    p.add_argument("--model-version", default=DEFAULT_MODEL_VERSION)
    p.add_argument("--min-drivers", type=int, default=20)
    p.add_argument("--timeout-s", type=float, default=600.0)
    p.add_argument("--dashboard", default=DEFAULT_DASHBOARD)
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None, fetch: Fetch = http_json) -> int:
    args = parse_args(argv)
    try:
        cfg = DemoConfig(
            api=args.api,
            season=args.season,
            round=args.round,
            speed=args.speed,
            wait=args.wait,
            model_version=args.model_version,
            min_drivers=args.min_drivers,
            timeout_s=args.timeout_s,
            dashboard=args.dashboard,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        Demo(cfg, fetch=fetch).run()
    except DemoError as exc:
        print(f"demo failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
