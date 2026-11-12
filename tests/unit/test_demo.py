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
