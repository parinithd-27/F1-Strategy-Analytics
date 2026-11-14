"""``f1sa.bench.streaming_runs`` (``make stream-latency-suite`` / ``stream-latency-ui``) with fake
processes and samplers: names, record refusal, resources, the UI join. No service is touched."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from f1sa.bench import streaming_runs as S
from f1sa.bench.resources import ContainerPeak
from f1sa.streaming import latency as L


def test_suite_runs_and_new_record_names() -> None:
    assert [(r.tag, r.speed, r.window_s) for r in S.SUITE] == [
        ("s10", 10.0, None),
        ("s60", 60.0, None),
        ("s1w900", 1.0, 900.0),
    ]
    names = S.record_targets(list(S.SUITE), ui=False)
    assert "streaming_latency_s10.csv" in names and "stream_batches_s1w900.jsonl" in names
    assert "streaming_latency_summary.csv" in names and "streaming_resources.csv" in names
    assert not set(names) & set(S.STEP12_FILES)  # the Step 12 files are never targets
    ui = S.record_targets([S.UI_RUN], ui=True)
    assert {"ui_latency.csv", "ui_latency_summary.csv", "ui_streaming_resources.csv"} <= set(ui)
    assert not set(ui) & set(names)


def test_record_refuses_existing_targets(tmp_path: Path) -> None:
    (tmp_path / "streaming_latency_s60.csv").write_text("x")
    with pytest.raises(S.SuiteError, match="never overwritten"):
        S.check_record(tmp_path, S.record_targets(list(S.SUITE), ui=False))


def test_demo_command_has_sse_and_the_window() -> None:
    cmd = S.demo_command(S.SUITE[2], Path("d/s1w900"), "http://x/events")
    assert cmd[1] == str(S.DEMO_SCRIPT) and S.DEMO_SCRIPT.is_file()
    assert cmd[cmd.index("--speed") + 1] == "1" and cmd[cmd.index("--window-s") + 1] == "900"
    assert cmd[cmd.index("--sse-url") + 1] == "http://x/events"
    assert "--window-s" not in S.demo_command(S.SUITE[0], Path("d"), "u")


@dataclass
class Peaks:
    samples: int = 4
    peak_rss_bytes: int = 2**30
    peak_cpu_pct: float = 250.0
    mean_cpu_pct: float = 90.0
    peak_cgroup_bytes: int | None = None
    min_host_available_bytes: int | None = 8 * 2**30


class FakeSampler:
    def stop(self) -> Peaks:
        return Peaks()


class FakeProc:
    pid = 4242

    def __init__(self, cmd: list[str]) -> None:
        out = Path(cmd[cmd.index("--results-dir") + 1])
        rows = [
            {"type": "lap", "event_id": "e1", "produced_at_ms": 0, "batch_received_at_ms": 5,
             "redis_written_at_ms": 9, "subscriber_received_at_ms": 20, "end_to_end_ms": 20,
             "sse_received_at_ms": 25, "written_to_sse_ms": 16, "first_batch": True},
            {"type": "lap", "event_id": "e2", "produced_at_ms": 1000, "batch_received_at_ms": 1500,
             "redis_written_at_ms": 1510, "subscriber_received_at_ms": 1520,
             "end_to_end_ms": 520, "sse_received_at_ms": 1530, "written_to_sse_ms": 20,
             "first_batch": False},
        ]  # fmt: skip
        L.write_rows(rows, out / "streaming_latency_sse.csv", L.SSE_COLUMNS)
        for name in ("stream_batches.jsonl", "stream_progress.jsonl"):
            (out / name).write_text("{}\n")

    def wait(self) -> int:
        return 0


def _run(tmp_path: Path, spec: S.RunSpec) -> S.RunResult:
    times = iter([100.0, 160.0])
    return S.run_one(
        spec,
        tmp_path,
        sse_url="http://x",
        launch=FakeProc,
        sampler=lambda pid: FakeSampler(),
        clock=lambda: next(times),
        out=lambda s: None,
    )


def test_run_one_and_the_summaries(tmp_path: Path) -> None:
    r = _run(tmp_path, S.SUITE[0])
    assert r.rc == 0 and (r.start, r.end) == (100.0, 160.0) and r.process["peak_cpu_pct"] == 250.0
    docker = tmp_path / "docker.jsonl"
    docker.write_text(
        json.dumps({"ts": 120.0, "name": "f1sa-redis-1", "mem_bytes": 50 * 2**20,
                    "mem_limit_bytes": None, "cpu_pct": 3.5}) + "\n"
        + json.dumps({"ts": 500.0, "name": "f1sa-redis-1", "mem_bytes": 9 * 2**30,
                      "mem_limit_bytes": None, "cpu_pct": 99.0}) + "\n"
    )  # fmt: skip
    tables = S.summarise(tmp_path, [r], docker)
    res = {x["name"]: x for x in tables["resources"]}
    assert res["f1sa-redis-1"]["peak_mem_bytes"] == 50 * 2**20  # outside the window: ignored
    host = next(x for x in tables["resources"] if x["scope"] == "host_job")
    assert host["peak_rss_bytes"] == 2**30 and host["window_s"] == 60.0
    first = [x for x in tables["segments"] if x["scope"] == "first_batch"]
    assert first and all(x["n"] == 1 for x in first)
    assert (tmp_path / "streaming_latency_summary.csv").is_file()
    assert tables["throughput"][0]["lap_messages"] == 2
    with pytest.raises(FileExistsError):
        _run(tmp_path, S.SUITE[0])  # a run folder is never reused


def test_record_copies_to_new_names_and_appends_one_section_each(tmp_path: Path) -> None:
    suite = tmp_path / "suite"
    r = _run(suite, S.SUITE[1])
    S.summarise(suite, [r], tmp_path / "none.jsonl")
    results = tmp_path / "results"
    results.mkdir()
    (results / "streaming_latency.csv").write_text("step 12\n")
    written = S.record(suite, [r], False, [("measured_at_utc", "T"), ("x", "y")], results)
    assert sorted(written) == sorted(S.record_targets([S.SUITE[1]], ui=False))
    assert (results / "streaming_latency.csv").read_text() == "step 12\n"
    readme = (results / "README.md").read_text()
    assert all(readme.count(f"## `{n}` (T)") == 1 for n in written)
    with pytest.raises(S.SuiteError):
        S.record(suite, [r], False, [("measured_at_utc", "T")], results)


def test_ui_rows_join_by_event_id_and_report_a_subset(tmp_path: Path) -> None:
    ui = tmp_path / "ui.csv"
    ui.write_text(
        "event_id,event_type,season,round,produced_at_ms,batch_received_at_ms,"
        "redis_written_at_ms,sse_sent_at_ms,ui_rendered_at_ms,recorded_at_ms\n"
        "e1,lap,2024,5,0,5,9,12,40,41\n"
        "zz,undercut_alert,2024,5,,,,30,70,71\n"
    )
    harness = [
        {"type": "lap", "event_id": "e1", "sse_received_at_ms": 25},
        {"type": "lap", "event_id": "e2"},
        {"type": "pit_in", "event_id": "e3"},
        {"type": "pit_out", "event_id": "e4"},
    ]
    rows = S.ui_rows(ui, harness)
    assert rows[0]["sse_to_ui_ms"] == 28 and rows[0]["produced_to_ui_ms"] == 40
    assert rows[0]["harness_matched"] and rows[0]["harness_sse_received_at_ms"] == 25
    assert rows[1]["produced_to_ui_ms"] is None and not rows[1]["harness_matched"]
    summary = {r["segment"]: r for r in S.ui_summary(rows, harness, "s60ui")}
    assert summary["sse_to_ui_ms"]["n"] == 2 and summary["sse_to_ui_ms"]["subset_fraction"] == 0.5
    assert summary["sse_to_ui_ms"]["events_in_run"] == 4  # every message, alerts included
    assert summary["produced_to_ui_ms"]["n"] == 1
    assert all(r["ui_subset"] == "true" and "SUBSET" in r["note"] for r in summary.values())


def test_resource_rows_without_container_samples() -> None:
    r = S.RunResult(S.SUITE[0], Path("x"), 0, 0.0, 10.0, {"samples": 1, "peak_cpu_pct": 5.0})
    rows = S.resource_rows(r, {"f1sa-kafka-1": ContainerPeak("f1sa-kafka-1", 3, 1024, 7.0)})
    assert [x["scope"] for x in rows] == ["host_job", "container"]
    assert (
        rows[1]["peak_mem_bytes"] == 1024 and rows[0]["window_start_utc"] == "1970-01-01T00:00:00Z"
    )


def test_main_refuses_a_record_run_whose_targets_exist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "streaming_resources.csv").write_text("x")
    out: list[str] = []
    assert S.main(["--suite", "--record"], out=out.append) == 2
    assert "never overwritten" in out[0] and not (tmp_path / "data").exists()


def test_main_ui_refuses_without_debug_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    def off(api_url: str = "") -> None:
        raise S.SuiteError("the API's debug endpoint is off")

    monkeypatch.setattr(S, "check_debug_api", off)
    out: list[str] = []
    assert S.main(["--ui"], out=out.append) == 2 and "debug endpoint is off" in out[0]


def test_describe_and_suite_id() -> None:
    from datetime import UTC, datetime

    assert S.describe(S.SUITE[2]) == "s1w900 speed 1 window 900 s"
    assert S.describe(S.SUITE[0]) == "s10 speed 10 full race"
    now = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
    assert S.suite_id(now, True) == "streamlat_20261114T120000Z_ui"


def test_experiments_script_dispatches_suite_and_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.util

    path = Path(__file__).resolve().parents[2] / "experiments/streaming_latency.py"
    spec = importlib.util.spec_from_file_location("streaming_latency_script", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    seen: list[Any] = []
    monkeypatch.setattr(S, "main", lambda argv: seen.append(list(argv)) or 0)
    assert mod.main(["--suite", "--record"]) == 0 and seen == [["--suite", "--record"]]


def test_resummarise_ui_rebuilds_from_the_raw_samples(tmp_path: Path) -> None:
    run = tmp_path / S.UI_RUN.tag
    run.mkdir()
    harness = [{"type": "lap", "event_id": "e1"}, {"type": "undercut_alert", "event_id": "t"}]
    L.write_rows(harness, run / "streaming_latency_sse.csv", L.SSE_COLUMNS)
    (tmp_path / "ui_latency_raw.csv").write_text(
        "event_id,event_type,season,round,produced_at_ms,batch_received_at_ms,"
        "redis_written_at_ms,sse_sent_at_ms,ui_rendered_at_ms,recorded_at_ms\n"
        "e1,lap,2024,5,0,5,9,12,40,41\nz,undercut_alert,2024,5,1,2,3,30,70,71\n"
    )
    summary = S.resummarise_ui(tmp_path)
    assert {r["subset_fraction"] for r in summary} == {1.0}
    assert (tmp_path / "ui_latency.csv").is_file()
