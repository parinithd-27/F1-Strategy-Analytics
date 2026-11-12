"""``POST /api/races/{season}/{round}/replay`` and ``GET /api/replays``: the whole-pipeline replay
(stream job, ready file, producer, session_end), its 409s, the cap, 2025, the 60 s readiness
timeout and that both subprocesses (and what they spawn) are gone afterwards."""

from __future__ import annotations

import asyncio
import os
import sys
import textwrap
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import psutil
import pytest
from _api_fixtures import (
    ApiHarness,
    FakeLauncher,
    make_api,
    make_settings,
    settle,
    write_file_lake,
)

from f1sa.api.replay import (
    ProcessLike,
    ReplayConflictError,
    ReplayManager,
    ReplayRefusedError,
    producer_argv,
    stream_job_argv,
    subprocess_launcher,
)

pytestmark = pytest.mark.anyio

T0 = datetime(2026, 11, 12, 18, 0, 0, 123000, tzinfo=UTC)
RID = "2024_5_20261112T180000.123Z"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _api(tmp_path: Path, launcher: FakeLauncher | None = None, **overrides: object) -> ApiHarness:
    write_file_lake(tmp_path / "lake")
    settings = make_settings(tmp_path, **{"enable_replay": True, **overrides})
    api = make_api(settings, launcher=launcher)
    api.deps.replays._clock = lambda: T0
    return api


async def _post(api: ApiHarness, path: str = "2024/5", speed: int = 60) -> tuple[int, dict]:  # type: ignore[type-arg]
    async with api.client() as c:
        resp = await c.post(f"/api/races/{path}/replay", json={"speed": speed})
    return resp.status_code, resp.json()


async def _status(api: ApiHarness) -> dict:  # type: ignore[type-arg]
    async with api.client() as c:
        resp = await c.get("/api/replays")
    assert resp.status_code == 200
    body: dict = resp.json()  # type: ignore[type-arg]
    return body


# ------------------------------------------------------------------ flags and validation
async def test_403_when_replay_is_disabled(tmp_path: Path) -> None:
    api = _api(tmp_path, enable_replay=False)
    async with api.client() as c:
        resp = await c.post("/api/races/2024/5/replay", json={"speed": 10})
        bad_body = await c.post("/api/races/2024/5/replay", json={"speed": 5})
    assert resp.status_code == 403
    assert resp.json() == {"detail": "replay is disabled (set ENABLE_REPLAY=1)"}
    assert bad_body.status_code == 403
    assert api.launcher.processes == []
    status = await _status(api)
    assert status["enabled"] is False and status["running"] == []


@pytest.mark.parametrize(
    "body", [{"speed": 5}, {"speed": "10"}, {}, {"speed": 10, "extra": 1}, {"speed": None}]
)
async def test_422_for_a_bad_body(tmp_path: Path, body: dict[str, object]) -> None:
    api = _api(tmp_path)
    async with api.client() as c:
        resp = await c.post("/api/races/2024/5/replay", json=body)
    assert resp.status_code == 422
    assert api.launcher.processes == []


async def test_422_for_bad_path_and_404_for_unknown_race(tmp_path: Path) -> None:
    api = _api(tmp_path)
    async with api.client() as c:
        bad = await c.post("/api/races/2024/0/replay", json={"speed": 10})
        missing = await c.post("/api/races/2022/1/replay", json={"speed": 10})
    assert bad.status_code == 422
    assert missing.status_code == 404
    assert missing.json() == {"detail": "2022 R1 is not in the clean zone"}


async def test_503_without_a_clean_zone(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path, enable_replay=True))
    async with api.client() as c:
        resp = await c.post("/api/races/2024/5/replay", json={"speed": 10})
    assert resp.status_code == 503


# ------------------------------------------------------------------ the pipeline
async def test_202_runs_stream_job_then_producer_and_finishes(tmp_path: Path) -> None:
    launcher = FakeLauncher(hold_producer=True)
    api = _api(tmp_path, launcher)
    code, body = await _post(api)
    assert code == 202
    assert body["run_id"] == RID and body["state"] == "starting"
    assert body["results_dir"] == f"data/demo/{RID}"
    assert body["started_at"] == "2026-11-12T18:00:00.123Z"
    stream = launcher.of("stream_job")[0]
    assert stream.argv == (
        sys.executable, "-m", "f1sa.streaming.stream_job",
        "--checkpoint", f"data/checkpoints/api_{RID}",
        "--stop-after-session-end", "2024:5",
        "--ready-file", f"data/checkpoints/api_{RID}.ready",
        "--results-dir", f"data/demo/{RID}",
        "--timeout-s", str(int(5 + 7200 / 60 + 5)),
    )  # fmt: skip
    for _ in range(100):
        if launcher.of("producer"):
            break
        await asyncio.sleep(0.01)
    producer = launcher.of("producer")[0]
    assert producer.argv == (
        sys.executable, "-m", "f1sa.streaming.producer",
        "--season", "2024", "--round", "5", "--speed", "60",
    )  # fmt: skip
    running = await _status(api)
    assert [j["state"] for j in running["running"]] == ["running"]
    launcher.end_producer(producer, 0)
    await settle(api)
    status = await _status(api)
    assert status["running"] == []
    last = status["recent"][0]
    assert last["state"] == "finished" and last["error"] is None
    assert (last["stream_returncode"], last["producer_returncode"]) == (0, 0)
    assert stream.group_swept and producer.group_swept
    assert (tmp_path / "data" / "demo" / RID).is_dir()
    assert not (tmp_path / "results" / "stream_progress.jsonl").exists()


async def test_409_same_race_and_limit_then_free_again(tmp_path: Path) -> None:
    api = _api(tmp_path, FakeLauncher(hold_producer=True))
    assert (await _post(api, "2024/5"))[0] == 202
    same = await _post(api, "2024/5", 1)
    other = await _post(api, "2023/5", 1)
    assert same[0] == 409 and "is running" in same[1]["detail"]
    assert other[0] == 409 and "limit reached" in other[1]["detail"]
    for _ in range(100):
        if api.launcher.of("producer"):
            break
        await asyncio.sleep(0.01)
    api.launcher.end_producer(api.launcher.of("producer")[0], 0)
    await settle(api)
    assert (await _post(api, "2023/5", 1))[0] == 202


async def test_two_running_with_a_cap_of_two(tmp_path: Path) -> None:
    api = _api(tmp_path, FakeLauncher(hold_producer=True), api_max_replays=2)
    a, b, dup = await _post(api, "2024/5"), await _post(api, "2023/5"), await _post(api, "2023/5")
    assert (a[0], b[0], dup[0]) == (202, 202, 409)
    await api.deps.replays.shutdown()


async def test_concurrent_requests_start_one(tmp_path: Path) -> None:
    api = _api(tmp_path, FakeLauncher(hold_producer=True))
    async with api.client() as c:
        results = await asyncio.gather(
            *(c.post("/api/races/2024/5/replay", json={"speed": 10}) for _ in range(5))
        )
    assert sorted(r.status_code for r in results) == [202, 409, 409, 409, 409]
    assert len(api.launcher.of("stream_job")) == 1
    await api.deps.replays.shutdown()


async def test_2025_refused_unless_allowed(tmp_path: Path) -> None:
    api = _api(tmp_path)
    code, body = await _post(api, "2025/6")
    assert code == 403 and "sealed" in body["detail"]
    assert api.launcher.processes == []

    allowed = _api(tmp_path, allow_test_season=True)
    assert (await _post(allowed, "2025/6", 10))[0] == 202
    await settle(allowed)
    assert all(p.argv[-1] == "--allow-test-season" for p in allowed.launcher.processes)
    assert {p.kind for p in allowed.launcher.processes} == {"stream_job", "producer"}


async def test_500_and_a_free_slot_when_the_launch_fails(tmp_path: Path) -> None:
    api = _api(tmp_path)
    api.launcher.fail = FileNotFoundError("python")
    code, body = await _post(api)
    assert code == 500 and body == {"detail": "the producer could not be started"}
    api.launcher.fail = None
    api.deps.replays._clock = lambda: datetime(2026, 11, 12, 18, 0, 1, tzinfo=UTC)
    assert (await _post(api))[0] == 202
    await settle(api)


# ------------------------------------------------------------------ failure paths
async def test_stream_job_not_ready_in_time_is_killed_and_reported(tmp_path: Path) -> None:
    launcher = FakeLauncher(stream_ready=False)
    api = _api(tmp_path, launcher, api_replay_ready_timeout_s=0.2)
    t0 = time.monotonic()
    assert (await _post(api))[0] == 202
    await settle(api)
    assert time.monotonic() - t0 < 3
    stream = launcher.of("stream_job")[0]
    assert stream.terminated and stream.group_swept and stream.returncode is not None
    assert launcher.of("producer") == [], "the producer must not start before the job is ready"
    status = await _status(api)
    assert status["running"] == []
    last = status["recent"][0]
    assert last["state"] == "failed"
    assert last["error"] == "stream job not ready within 0.2 s"
    api.deps.replays._clock = lambda: datetime(2026, 11, 12, 18, 0, 1, tzinfo=UTC)
    assert (await _post(api))[0] == 202, "the slot must be idle again"
    await settle(api)


async def test_stream_job_that_ignores_sigterm_is_killed(tmp_path: Path) -> None:
    launcher = FakeLauncher(stream_ready=False, stream_ignores_term=True)
    api = _api(tmp_path, launcher, api_replay_ready_timeout_s=0.1)
    api.deps.replays.grace_s = 0.1
    await _post(api)
    await settle(api)
    stream = launcher.of("stream_job")[0]
    assert stream.terminated and stream.killed and stream.returncode == -9


async def test_stream_job_exiting_before_ready(tmp_path: Path) -> None:
    launcher = FakeLauncher(stream_ready=False)
    api = _api(tmp_path, launcher)
    await _post(api)
    launcher.of("stream_job")[0].finish(2)
    await settle(api)
    last = (await _status(api))["recent"][0]
    assert last["error"] == "stream job exited with code 2 before it was ready"
    assert launcher.of("producer") == []


async def test_producer_failure_stops_the_stream_job(tmp_path: Path) -> None:
    launcher = FakeLauncher(producer_rc=3)
    api = _api(tmp_path, launcher)
    await _post(api)
    await settle(api)
    stream = launcher.of("stream_job")[0]
    assert stream.terminated and stream.group_swept
    last = (await _status(api))["recent"][0]
    assert last["state"] == "failed" and last["error"] == "producer exited with code 3"
    assert last["producer_returncode"] == 3


async def test_stream_job_that_never_stops_after_the_producer(tmp_path: Path) -> None:
    launcher = FakeLauncher(auto_end=False)
    api = _api(tmp_path, launcher, api_replay_end_timeout_s=0.2)
    await _post(api)
    await settle(api)
    last = (await _status(api))["recent"][0]
    assert last["error"] == "stream job did not stop within 0.2 s after the producer"
    assert launcher.of("stream_job")[0].terminated


async def test_stream_job_failure_after_the_race(tmp_path: Path) -> None:
    api = _api(tmp_path, FakeLauncher(stream_rc=1))
    await _post(api)
    await settle(api)
    last = (await _status(api))["recent"][0]
    assert last["state"] == "failed" and last["error"] == "stream job exited with code 1"


async def test_shutdown_stops_both_and_reports(tmp_path: Path) -> None:
    launcher = FakeLauncher(hold_producer=True)
    api = _api(tmp_path, launcher)
    await _post(api)
    for _ in range(100):
        if launcher.of("producer"):
            break
        await asyncio.sleep(0.01)
    async with api.app.router.lifespan_context(api.app):
        pass
    assert all(p.terminated and p.group_swept for p in launcher.processes)
    assert len(launcher.processes) == 2
    last = api.deps.replays.status()["recent"][0]
    assert last["state"] == "failed" and last["error"].startswith("API shutdown")


# ------------------------------------------------------------------ manager and argv
async def test_manager_refuses_like_the_producer(tmp_path: Path) -> None:
    mgr = ReplayManager(FakeLauncher(), max_running=1, allow_test_season=False)
    with pytest.raises(ReplayRefusedError, match="sealed"):
        await mgr.start(2025, 1, 10)
    with pytest.raises(ValueError, match="speed"):
        await mgr.start(2024, 1, 5)
    with pytest.raises(ValueError):
        ReplayManager(FakeLauncher(), max_running=0, allow_test_season=False)


async def test_manager_conflict_reasons(tmp_path: Path) -> None:
    mgr = ReplayManager(FakeLauncher(hold_producer=True), max_running=1, allow_test_season=False)
    await mgr.start(2024, 5, 10)
    with pytest.raises(ReplayConflictError) as same:
        await mgr.start(2024, 5, 10)
    with pytest.raises(ReplayConflictError) as limit:
        await mgr.start(2024, 6, 10)
    assert (same.value.reason, limit.value.reason) == ("running", "limit")
    await mgr.shutdown()


def test_argv_builders() -> None:
    assert producer_argv(2024, 5, 10, allow_test_season=False, python="py") == [
        "py", "-m", "f1sa.streaming.producer", "--season", "2024", "--round", "5", "--speed", "10"
    ]  # fmt: skip
    argv = stream_job_argv(
        2025, 1, checkpoint=Path("c"), ready_file=Path("r"), results_dir=Path("d"),
        timeout_s=99.9, allow_test_season=True, python="py",
    )  # fmt: skip
    assert argv == [
        "py", "-m", "f1sa.streaming.stream_job", "--checkpoint", "c",
        "--stop-after-session-end", "2025:1", "--ready-file", "r", "--results-dir", "d",
        "--timeout-s", "99", "--allow-test-season",
    ]  # fmt: skip


# ------------------------------------------------------------------ real subprocesses: no orphans
STAND_IN_JOB = textwrap.dedent(
    """
    import subprocess, sys, time, pathlib
    ready, pidfile, mode = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
    jvm = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
    pidfile.write_text(str(jvm.pid))
    print("stand-in stream job up", flush=True)
    if mode == "ready":
        ready.write_text("q")
        end = pathlib.Path(str(ready) + ".session_end")
        while not end.exists():
            time.sleep(0.02)
        print("session_end processed; stopping", flush=True)
        sys.exit(0)  # leaves its 'JVM' behind in the process group
    time.sleep(300)
    """
)
STAND_IN_PRODUCER = textwrap.dedent(
    """
    import pathlib, sys
    pathlib.Path(sys.argv[1] + ".session_end").write_text("x")
    print("sent 3 events, 3 delivered", flush=True)
    """
)


def _stand_in_launcher(tmp_path: Path, mode: str, pids: list[int]):  # type: ignore[no-untyped-def]
    async def launch(argv: Sequence[str]) -> ProcessLike:
        if "f1sa.streaming.stream_job" in argv:
            ready = argv[list(argv).index("--ready-file") + 1]
            pidfile = tmp_path / f"jvm_{len(pids)}.pid"
            proc = await subprocess_launcher(
                [sys.executable, "-c", STAND_IN_JOB, ready, str(pidfile), mode]
            )
            for _ in range(500):
                if pidfile.exists() and pidfile.read_text():
                    break
                await asyncio.sleep(0.01)
            pids.extend([proc.pid, int(pidfile.read_text())])  # type: ignore[attr-defined]
            return proc
        ready = str(next(tmp_path.rglob("api_*.ready")))
        return await subprocess_launcher([sys.executable, "-c", STAND_IN_PRODUCER, ready])

    return launch


def _gone(pid: int, timeout_s: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                return True
        except psutil.NoSuchProcess:
            return True
        time.sleep(0.05)
    return False


async def test_real_timeout_kills_the_job_and_its_children(tmp_path: Path) -> None:
    os.chdir(tmp_path)
    pids: list[int] = []
    mgr = ReplayManager(
        _stand_in_launcher(tmp_path, "never-ready", pids),
        max_running=1,
        allow_test_season=False,
        ready_timeout_s=1.0,
        grace_s=2.0,
    )
    job = await mgr.start(2024, 5, 60)
    assert job.task is not None
    await asyncio.wait_for(job.task, 20)
    assert job.state == "failed" and job.error == "stream job not ready within 1 s"
    assert len(pids) == 2
    assert all(_gone(pid) for pid in pids), "stream job or its child survived"
    assert mgr.running() == [] and mgr.jobs == {}


async def test_real_finished_replay_leaves_no_orphans(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    os.chdir(tmp_path)
    pids: list[int] = []
    mgr = ReplayManager(
        _stand_in_launcher(tmp_path, "ready", pids),
        max_running=1,
        allow_test_season=False,
        ready_timeout_s=10.0,
        end_timeout_s=10.0,
        grace_s=2.0,
    )
    with caplog.at_level("INFO", logger="f1sa.api.replay"):
        job = await mgr.start(2024, 5, 60)
        assert job.task is not None
        await asyncio.wait_for(job.task, 30)
    assert job.state == "finished", job.error
    assert (job.stream_returncode, job.producer_returncode) == (0, 0)
    assert all(_gone(pid) for pid in pids), "the stream job's child outlived the replay"
    lines = {getattr(r, "line", None) for r in caplog.records if r.getMessage() == "replay output"}
    assert {"stand-in stream job up", "sent 3 events, 3 delivered"} <= lines
