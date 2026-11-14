"""The stream demo orchestrator (``make stream-demo``) with fake processes and services."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from f1sa.config import Settings
from f1sa.streaming import demo as DM


class FakeJob:
    def __init__(self, cmd: list[str], *, make_ready: bool = True, rc: int = 0) -> None:
        self.cmd, self.returncode, self.terminated = cmd, None, False
        self._rc = rc
        if make_ready:
            ready = Path(cmd[cmd.index("--ready-file") + 1])
            ready.parent.mkdir(parents=True, exist_ok=True)
            ready.write_text("q\n")

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float) -> int:
        self.returncode = self._rc
        return self._rc

    def terminate(self) -> None:
        self.terminated = True


def rows() -> list[dict[str, Any]]:
    lap = {"type": "lap", "end_to_end_ms": 120, "subscriber_received_at_ms": 1000}
    return [lap, {**lap, "subscriber_received_at_ms": 2000}, {"type": "session_end"}]


def run_demo(tmp_path: Path, **kw: Any) -> tuple[int, list[Any], list[list[str]]]:
    jobs: list[Any] = []
    producer_cmds: list[list[str]] = []

    def popen(cmd: list[str]) -> FakeJob:
        jobs.append(kw.pop("job", None) or FakeJob(cmd))
        return jobs[-1]

    def run(cmd: list[str]) -> int:
        producer_cmds.append(cmd)
        return int(kw.get("producer_rc", 0))

    rc = DM.main(
        [
            "--season",
            "2024",
            "--round",
            "5",
            "--speed",
            "60",
            "--results-dir",
            str(tmp_path / "res"),
        ],
        popen=popen,
        run=run,
        check=kw.get("check", lambda s: 3),
        subscriber=lambda s, season, rnd, t: kw.get("rows", rows()),
        state=lambda s, season, rnd: "redis: ok",
    )
    return rc, jobs, producer_cmds


def test_commands() -> None:
    job = DM.job_command(2024, 5, Path("data/checkpoints/x"), Path("r"), Path("results"), 99.5)
    assert job[:3] == [sys.executable, "-m", "f1sa.streaming.stream_job"]
    assert job[job.index("--stop-after-session-end") + 1] == "2024:5"
    assert job[job.index("--timeout-s") + 1] == "99"
    prod = DM.producer_command(2024, 5, 60.0)
    assert prod[2:] == [
        "f1sa.streaming.producer",
        "--season",
        "2024",
        "--round",
        "5",
        "--speed",
        "60",
    ]


def test_a_successful_demo_writes_the_latency_csv(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc, jobs, prods = run_demo(tmp_path)
    assert rc == 0 and len(prods) == 1
    assert "data/checkpoints/demo_2024_5_" in " ".join(jobs[0].cmd)
    assert (tmp_path / "res" / "streaming_latency.csv").is_file()
    out = capsys.readouterr().out
    assert "end_to_end_ms" in out and "redis: ok" in out and "lap=2" in out


def test_unreachable_services_exit_2(tmp_path: Path) -> None:
    def down(s: Settings) -> int:
        raise DM.DemoError("Redis is not reachable")

    assert run_demo(tmp_path, check=down)[0] == 2


def test_a_failing_producer_stops_the_job(tmp_path: Path) -> None:
    rc, jobs, _ = run_demo(tmp_path, producer_rc=1)
    assert rc == 1 and jobs[0].terminated


def test_a_job_that_dies_before_ready_fails(tmp_path: Path) -> None:
    job = FakeJob(["x", "--ready-file", str(tmp_path / "never")], make_ready=False)
    job.returncode = 2
    (tmp_path / "never").unlink(missing_ok=True)
    rc, _, prods = run_demo(tmp_path, job=job)
    assert rc == 1 and prods == []


def test_missing_session_end_fails(tmp_path: Path) -> None:
    assert run_demo(tmp_path, rows=rows()[:2])[0] == 1


def test_wait_ready_times_out(tmp_path: Path) -> None:
    class Alive:
        returncode = None

        def poll(self) -> None:
            return None

    with pytest.raises(DM.DemoError, match="not ready"):
        DM.wait_ready(Alive(), tmp_path / "nope", 0.0, sleep=lambda s: None)


def test_bad_speed_exits_2(tmp_path: Path) -> None:
    assert DM.main(["--season", "2024", "--round", "5", "--speed", "0"], check=lambda s: 1) == 2


def test_report_prints_segments() -> None:
    lines: list[str] = []
    DM.report(rows(), lines.append)
    assert lines[0] == "3 messages received" and any("p95" in x for x in lines)


# ------------------------------------------------------------------ Step 16: window and SSE
class FakeProducer:
    def __init__(self, cmd: list[str], *, finishes: bool = False) -> None:
        self.cmd, self.returncode, self.terminated = cmd, (0 if finishes else None), False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated, self.returncode = True, -15

    def wait(self, timeout: float) -> int:
        return int(self.returncode or 0)

    def kill(self) -> None:
        self.returncode = -9


class SignalJob(FakeJob):
    def __init__(self, cmd: list[str]) -> None:
        super().__init__(cmd)
        self.signals: list[int] = []

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)
        self._rc = 130


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def _lap(eid: str, batch: int, received: int) -> dict[str, Any]:
    return {
        "type": "lap", "event_id": eid, "driver_id": "ver", "lap": 1, "produced_at_ms": 100,
        "batch_received_at_ms": batch, "redis_written_at_ms": batch + 50,
        "subscriber_received_at_ms": received, "end_to_end_ms": received - 100,
    }  # fmt: skip


def test_window_run_stops_producer_then_job_and_needs_no_session_end(tmp_path: Path) -> None:
    import signal

    clock = FakeClock()
    procs: list[Any] = []

    def popen(cmd: list[str]) -> Any:
        procs.append(SignalJob(cmd) if "f1sa.streaming.stream_job" in cmd else FakeProducer(cmd))
        return procs[-1]

    def subscriber(s: Any, season: int, rnd: int, t: float, *, stop: Any, live: Any) -> Any:
        got = [_lap("a", 1000, 1200)]
        live.extend(got)
        while not stop():
            pass
        return got

    rc = DM.main(
        ["--season", "2024", "--round", "5", "--speed", "1", "--window-s", "5", "--idle-s", "2",
         "--results-dir", str(tmp_path / "res")],
        popen=popen,
        run=lambda cmd: pytest.fail("the window run starts the producer with popen"),
        check=lambda s: 3,
        subscriber=subscriber,
        state=lambda s, season, rnd: "redis: ok",
        sleep=clock.sleep,
        clock=clock,
    )  # fmt: skip
    job, producer = procs
    assert rc == 0
    assert producer.terminated and job.signals == [signal.SIGINT]
    assert clock.t >= 7.0  # the window, then the quiet time
    assert (tmp_path / "res" / "streaming_latency.csv").is_file()


def test_window_must_be_positive(tmp_path: Path) -> None:
    argv = ["--season", "2024", "--round", "5", "--window-s", "0"]
    assert DM.main(argv, check=lambda s: 1) == 2


def test_sse_rows_are_joined_and_the_first_batch_flagged(tmp_path: Path) -> None:
    import json

    res = tmp_path / "res"
    res.mkdir()
    (res / "stream_batches.jsonl").write_text(
        json.dumps({"batch_received_at_ms": 900, "races": []}) + "\n"
        + json.dumps({"batch_received_at_ms": 1000, "races": [{"n_states": 20}]}) + "\n"
    )  # fmt: skip
    pubsub = [_lap("a", 1000, 1200), _lap("b", 2000, 2100), {"type": "session_end"}]

    def sse(url: str, timeout: float, stop: Any, connected: Any) -> list[dict[str, Any]]:
        connected.set()
        return [{**_lap("a", 1000, 1200), "subscriber_received_at_ms": 1230}]

    rc = DM.main(
        ["--season", "2024", "--round", "5", "--results-dir", str(res), "--sse-url", "http://x"],
        popen=lambda cmd: FakeJob(cmd),
        run=lambda cmd: 0,
        check=lambda s: 3,
        subscriber=lambda s, season, rnd, t: pubsub,
        state=lambda s, season, rnd: "redis: ok",
        sse=sse,
    )
    assert rc == 0
    from f1sa.streaming import latency as L

    ext = L.read_rows(res / "streaming_latency_sse.csv")
    assert [r["sse_received_at_ms"] for r in ext] == [1230, None, None]
    assert ext[0]["written_to_sse_ms"] == 180 and ext[0]["produced_to_sse_ms"] == 1130
    assert [r["first_batch"] for r in ext] == [True, False, False]


def test_sse_that_never_connects_exits_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(DM, "SSE_CONNECT_TIMEOUT_S", 0.05)
    rc = DM.main(
        ["--season", "2024", "--round", "5", "--results-dir", str(tmp_path), "--sse-url", "http://x"],
        popen=lambda cmd: pytest.fail("no job before the SSE stream is connected"),
        check=lambda s: 3,
        subscriber=lambda s, season, rnd, t: [],
        sse=lambda url, timeout, stop, connected: [],
    )  # fmt: skip
    assert rc == 2


def test_run_window_when_the_producer_finishes_first() -> None:
    clock = FakeClock()
    p = FakeProducer(["p"], finishes=True)
    msg = DM.run_window(p, 100.0, [], 1.0, sleep=clock.sleep, clock=clock)
    assert msg.startswith("producer finished") and not p.terminated and clock.t < 5


def test_stop_job_signals_then_terminates_a_hanging_job() -> None:
    import subprocess

    class Hanging:
        returncode = None

        def __init__(self) -> None:
            self.terminated = False

        def poll(self) -> None:
            return None

        def send_signal(self, sig: int) -> None:
            pass

        def wait(self, timeout: float) -> int:
            raise subprocess.TimeoutExpired("job", timeout)

        def terminate(self) -> None:
            self.terminated = True

    h = Hanging()
    assert DM.stop_job(h, timeout_s=0.01) is None and h.terminated
    done = FakeJob(["--ready-file", "/dev/null"], make_ready=False)
    done.returncode = 0
    assert DM.stop_job(done) == 0
