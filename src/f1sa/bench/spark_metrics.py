"""Input, output and shuffle bytes per timed run, from the Spark REST API (driver UI).

Each timed run sets a job group (``SparkContext.setJobGroup``). Afterwards
:meth:`RestMetrics.totals_for_group` reads ``/api/v1/applications/<id>/jobs`` and ``/stages`` and
sums the stage metrics of that group's jobs (every attempt, so a retried stage counts twice).
Skipped stages (reused shuffle output) have zero metrics.

Bench sessions enable the UI only inside the container (it is not published) with a high
retention, so that a run's stages are still listed when it ends (:data:`UI_CONF`).
"""

from __future__ import annotations

import json
import time
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from pyspark.sql import SparkSession

UI_CONF: dict[str, str] = {
    "spark.ui.enabled": "true",
    "spark.ui.showConsoleProgress": "false",
    "spark.ui.retainedJobs": "20000",
    "spark.ui.retainedStages": "20000",
    "spark.ui.retainedTasks": "1000",
    "spark.sql.ui.retainedExecutions": "200",
}
_DONE_JOB = frozenset({"SUCCEEDED", "FAILED"})
_DONE_STAGE = frozenset({"COMPLETE", "FAILED", "SKIPPED"})


@dataclass(frozen=True)
class StageTotals:
    input_bytes: int
    output_bytes: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    n_jobs: int
    n_stages: int
    complete: bool
    """False if some job or stage of the group was still running when the totals were taken."""


def totals_from_json(
    jobs: Iterable[Mapping[str, Any]], stages: Iterable[Mapping[str, Any]], group: str
) -> StageTotals:
    """Sum the metrics of every stage attempt that belongs to a job of ``group``."""
    group_jobs = [j for j in jobs if j.get("jobGroup") == group]
    stage_ids = {int(s) for j in group_jobs for s in j.get("stageIds", [])}
    rows = [s for s in stages if int(s.get("stageId", -1)) in stage_ids]

    def total(key: str) -> int:
        return sum(int(s.get(key, 0) or 0) for s in rows)

    listed = {int(s["stageId"]) for s in rows}
    complete = (
        all(j.get("status") in _DONE_JOB for j in group_jobs)
        and all(s.get("status") in _DONE_STAGE for s in rows)
        and stage_ids <= listed
    )
    return StageTotals(
        input_bytes=total("inputBytes"),
        output_bytes=total("outputBytes"),
        shuffle_read_bytes=total("shuffleReadBytes"),
        shuffle_write_bytes=total("shuffleWriteBytes"),
        n_jobs=len(group_jobs),
        n_stages=len(rows),
        complete=complete,
    )


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def ui_base(spark: SparkSession) -> str:
    """The driver UI's base URL, addressed via 127.0.0.1 (the cell runs in the same container)."""
    url = spark.sparkContext.uiWebUrl
    if not url:
        raise RuntimeError("the Spark UI is disabled; bench sessions need spark.ui.enabled=true")
    port = urlparse(url).port
    return f"http://127.0.0.1:{port}"


class RestMetrics:
    """Spark REST API reader for one application."""

    def __init__(
        self,
        base: str,
        app_id: str,
        *,
        get: Callable[[str], Any] = _get_json,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._api = f"{base.rstrip('/')}/api/v1/applications/{app_id}"
        self._get = get
        self._sleep = sleep

    @classmethod
    def for_session(cls, spark: SparkSession) -> RestMetrics:
        return cls(ui_base(spark), spark.sparkContext.applicationId)

    def totals_for_group(
        self, group: str, *, timeout_s: float = 15.0, poll_s: float = 0.25
    ) -> StageTotals:
        """Totals for ``group`` once every job and stage of it is finished (the listener bus
        updates the status store asynchronously), or the last reading after ``timeout_s``."""
        waited = 0.0
        while True:
            totals = totals_from_json(
                self._get(f"{self._api}/jobs"), self._get(f"{self._api}/stages"), group
            )
            if totals.complete or waited >= timeout_s:
                return totals
            self._sleep(poll_s)
            waited += poll_s
