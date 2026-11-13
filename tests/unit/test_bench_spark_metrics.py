"""Spark REST metrics per job group (Step 10)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession

from f1sa.bench.spark_metrics import RestMetrics, totals_from_json, ui_base

JOBS = [
    {"jobId": 0, "jobGroup": "run-0", "status": "SUCCEEDED", "stageIds": [0, 1]},
    {"jobId": 1, "jobGroup": "run-0", "status": "SUCCEEDED", "stageIds": [2]},
    {"jobId": 2, "jobGroup": "run-1", "status": "SUCCEEDED", "stageIds": [3]},
    {"jobId": 3, "status": "SUCCEEDED", "stageIds": [4]},
]
STAGES = [
    {"stageId": 0, "attemptId": 0, "status": "COMPLETE", "inputBytes": 100, "shuffleWriteBytes": 7},
    {"stageId": 0, "attemptId": 1, "status": "COMPLETE", "inputBytes": 100},
    {"stageId": 1, "attemptId": 0, "status": "COMPLETE", "shuffleReadBytes": 7, "outputBytes": 3},
    {"stageId": 2, "attemptId": 0, "status": "SKIPPED"},
    {"stageId": 3, "attemptId": 0, "status": "COMPLETE", "inputBytes": 999},
    {"stageId": 4, "attemptId": 0, "status": "COMPLETE", "inputBytes": 555},
]


def test_totals_sum_only_the_groups_stages_including_retries() -> None:
    t = totals_from_json(JOBS, STAGES, "run-0")
    assert (t.input_bytes, t.output_bytes) == (200, 3)
    assert (t.shuffle_read_bytes, t.shuffle_write_bytes) == (7, 7)
    assert (t.n_jobs, t.n_stages, t.complete) == (2, 4, True)
    assert totals_from_json(JOBS, STAGES, "run-1").input_bytes == 999
    none = totals_from_json(JOBS, STAGES, "missing")
    assert (none.n_jobs, none.input_bytes, none.complete) == (0, 0, True)


def test_running_or_unlisted_stage_is_incomplete() -> None:
    running = [{**JOBS[0], "status": "RUNNING"}]
    assert not totals_from_json(running, STAGES, "run-0").complete
    assert not totals_from_json(JOBS[:1], STAGES[:1], "run-0").complete


def test_rest_client_polls_until_complete() -> None:
    calls: list[str] = []
    replies = iter(
        [
            [{**JOBS[0], "status": "RUNNING"}],
            STAGES,
            JOBS,
            STAGES,
        ]
    )

    def get(url: str) -> Any:
        calls.append(url)
        return next(replies)

    sleeps: list[float] = []
    rest = RestMetrics("http://127.0.0.1:4040/", "app-1", get=get, sleep=sleeps.append)
    t = rest.totals_for_group("run-0")
    assert t.complete and t.input_bytes == 200 and sleeps == [0.25]
    assert calls[0] == "http://127.0.0.1:4040/api/v1/applications/app-1/jobs"
    assert calls[1].endswith("/stages")


def test_rest_client_gives_up_after_the_timeout() -> None:
    rest = RestMetrics(
        "http://x",
        "a",
        get=lambda url: [{**JOBS[0], "status": "RUNNING"}] if url.endswith("jobs") else STAGES,
        sleep=lambda _s: None,
    )
    t = rest.totals_for_group("run-0", timeout_s=1.0, poll_s=0.5)
    assert not t.complete


def test_ui_base_needs_the_ui(spark: SparkSession) -> None:
    with pytest.raises(RuntimeError, match=r"spark\.ui\.enabled"):
        ui_base(spark)  # the test session runs without a UI
