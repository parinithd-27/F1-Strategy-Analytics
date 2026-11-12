"""Shared helpers for the API tests: settings, a tiny clean lake, a fake replay process and an
app wired to fakeredis."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import fakeredis
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from _fakes import FakeHttpFS, _build
from _stream_fixtures import scenario_doc_dir
from fastapi import FastAPI

from f1sa.api.app import create_app
from f1sa.api.deps import ApiDeps
from f1sa.api.health import ModelStatus
from f1sa.api.replay import ProcessLike
from f1sa.config import Settings

GOOD_MODEL = ModelStatus(
    ok=True,
    model_version="deploy_real_G3.5_k2",
    git_sha="0" * 40,
    git_dirty=False,
    scorer_sha256="a" * 64,
    created_utc="2026-11-01T00:00:00Z",
    feature_set="real",
    g=3.5,
    k=2,
)


@dataclass(frozen=True)
class LakeRace:
    season: int
    round: int
    name: str | None
    results: tuple[tuple[int | None, int | None], ...] = ((1, 57), (2, 57), (3, 56))
    """(position, laps) rows."""
    laps: tuple[int, ...] = (1, 2, 3)
    stints_synthetic: tuple[bool | None, ...] | None = (False, False)
    """``is_synthetic`` of the race's stint rows; ``None`` = no stints partition."""


DEFAULT_RACES = (
    LakeRace(2023, 5, "Miami Grand Prix"),
    LakeRace(
        2024, 5, "Chinese Grand Prix", results=((1, 56), (2, 56)), stints_synthetic=(True, True)
    ),
    LakeRace(2025, 6, "Miami Grand Prix", results=((1, 57),), stints_synthetic=None),
)


def _parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return bytes(sink.getvalue().to_pybytes())


def lake_files(races: Iterable[LakeRace]) -> dict[str, bytes]:
    """``{relative path under clean/: parquet bytes}`` in Spark's layout (partition columns only
    in the directory names, ``part-*.parquet`` plus a ``.crc`` and ``_SUCCESS`` to be ignored)."""
    out: dict[str, bytes] = {}
    for r in races:
        part = f"season={r.season}/round={r.round}"
        out[f"races/{part}/part-00000-x.c000.snappy.parquet"] = _parquet_bytes(
            pa.table({"race_name": pa.array([r.name], pa.string()), "circuit_id": pa.array(["c"])})
        )
        out[f"races/{part}/.part-00000-x.c000.snappy.parquet.crc"] = b"junk"
        if r.results:
            out[f"results/{part}/part-00000-y.c000.snappy.parquet"] = _parquet_bytes(
                pa.table(
                    {
                        "driver_id": pa.array([f"d{i}" for i in range(len(r.results))]),
                        "position": pa.array([p for p, _ in r.results], pa.int32()),
                        "laps": pa.array([n for _, n in r.results], pa.int32()),
                    }
                )
            )
        if r.laps:
            out[f"laps/{part}/part-00000-z.c000.snappy.parquet"] = _parquet_bytes(
                pa.table(
                    {
                        "driver_id": pa.array(["d0"] * len(r.laps)),
                        "lap": pa.array(list(r.laps), pa.int32()),
                    }
                )
            )
        if r.stints_synthetic is not None:
            out[f"stints/{part}/part-00000-s.c000.snappy.parquet"] = _parquet_bytes(
                pa.table(
                    {
                        "driver_id": pa.array([f"d{i}" for i in range(len(r.stints_synthetic))]),
                        "is_synthetic": pa.array(list(r.stints_synthetic), pa.bool_()),
                    }
                )
            )
    out["races/_SUCCESS"] = b""
    return out


def write_file_lake(lake_root: Path, races: Iterable[LakeRace] = DEFAULT_RACES) -> Path:
    clean = lake_root / "clean"
    for rel, data in lake_files(races).items():
        p = clean / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return lake_root


class DirFakeHttpFS(FakeHttpFS):
    """``FakeHttpFS`` whose LISTSTATUS also lists sub-directories (as real HttpFS does) and which
    records the HTTP method of every request."""

    def __init__(self) -> None:
        super().__init__()
        self.methods: list[str] = []

    def add_tree(self, root: str, files: Mapping[str, bytes]) -> None:
        for rel, data in files.items():
            path = f"{root}/{rel}"
            self.files[path] = data
            parts = path.strip("/").split("/")[:-1]
            for i in range(1, len(parts) + 1):
                self.dirs.add("/" + "/".join(parts[:i]))

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.methods.append(request.method or "")
        parsed = urlparse(request.url or "")
        q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        path = parsed.path.removeprefix("/webhdfs/v1") or "/"
        if q.get("op") == "LISTSTATUS" and path in self.dirs and not self.faults.get("LISTSTATUS"):
            self.ops.append(("LISTSTATUS", path))
            prefix = path.rstrip("/") + "/"
            entries = [
                {"pathSuffix": f[len(prefix) :], "type": "FILE", "length": len(b)}
                for f, b in self.files.items()
                if f.startswith(prefix) and "/" not in f[len(prefix) :]
            ] + [
                {"pathSuffix": d[len(prefix) :], "type": "DIRECTORY", "length": 0}
                for d in self.dirs
                if d.startswith(prefix) and "/" not in d[len(prefix) :]
            ]
            body = json.dumps({"FileStatuses": {"FileStatus": entries}}).encode()
            return _build(self, request, 200, body)
        return super().send(request, **kwargs)


def httpfs_session(fake: FakeHttpFS) -> requests.Session:
    session = requests.Session()
    session.mount("http://", fake)
    return session


@dataclass
class FakeProcess:
    """A ``ProcessLike`` that runs until ``finish()``."""

    argv: tuple[str, ...]
    returncode: int | None = None
    stdout: asyncio.StreamReader | None = None
    terminated: bool = False
    killed: bool = False
    group_swept: bool = False
    ignore_term: bool = False
    _done: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def kind(self) -> str:
        return "stream_job" if "f1sa.streaming.stream_job" in self.argv else "producer"

    def arg(self, name: str) -> str:
        return self.argv[self.argv.index(name) + 1]

    def finish(self, code: int = 0) -> None:
        if self.returncode is None:
            self.returncode = code
        self._done.set()

    async def wait(self) -> int:
        await self._done.wait()
        assert self.returncode is not None
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        if not self.ignore_term:
            self.finish(-15)

    def kill(self) -> None:
        self.killed = True
        self.finish(-9)

    def kill_group(self) -> None:
        self.group_swept = True


@dataclass
class FakeLauncher:
    """The stream job writes its ready file at launch (unless ``stream_ready`` is False); the
    producer exits at once with ``producer_rc`` and, when ``auto_end``, the stream job then exits
    with ``stream_rc`` (it processed ``session_end``). ``hold_producer`` keeps the producer
    running until a test finishes it."""

    processes: list[FakeProcess] = field(default_factory=list)
    fail: BaseException | None = None
    stream_ready: bool = True
    producer_rc: int = 0
    stream_rc: int = 0
    auto_end: bool = True
    hold_producer: bool = False
    stream_ignores_term: bool = False

    def of(self, kind: str) -> list[FakeProcess]:
        return [p for p in self.processes if p.kind == kind]

    async def __call__(self, argv: Sequence[str]) -> ProcessLike:
        if self.fail is not None:
            raise self.fail
        proc = FakeProcess(tuple(argv))
        self.processes.append(proc)
        if proc.kind == "stream_job":
            proc.ignore_term = self.stream_ignores_term
            if self.stream_ready:
                ready = Path(proc.arg("--ready-file"))
                ready.parent.mkdir(parents=True, exist_ok=True)
                ready.write_text("q\n")
        elif not self.hold_producer:
            self.end_producer(proc, self.producer_rc)
        return proc

    def end_producer(self, proc: FakeProcess, rc: int) -> None:
        proc.finish(rc)
        if rc == 0 and self.auto_end:
            for stream in self.of("stream_job"):
                stream.finish(self.stream_rc)


async def settle(api_or_manager: Any, timeout_s: float = 5.0) -> None:
    """Wait until every replay task of the manager has ended."""
    mgr = getattr(getattr(api_or_manager, "deps", None), "replays", api_or_manager)
    tasks = [j.task for j in list(mgr.jobs.values()) if j.task is not None]
    if tasks:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout_s)


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "data_lake_uri": f"file://{tmp_path / 'lake'}",
        "api_ui_latency_csv": tmp_path / "results" / "ui_latency.csv",
        "api_model_dir": tmp_path / "models" / "deploy_real_G3.5_k2",
        "api_sse_heartbeat_s": 15.0,
        "api_replay_ready_timeout_s": 5.0,
        "api_replay_end_timeout_s": 5.0,
    }
    base.update(overrides)
    return Settings(**base)


async def kafka_ok() -> None:
    return None


@dataclass
class ApiHarness:
    app: FastAPI
    deps: ApiDeps
    redis: fakeredis.FakeAsyncRedis
    server: fakeredis.FakeServer
    launcher: FakeLauncher

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://testserver"
        )


def make_api(
    settings: Settings,
    *,
    model: ModelStatus | None = GOOD_MODEL,
    kafka_check: Callable[[], Any] = kafka_ok,
    launcher: FakeLauncher | None = None,
    server: fakeredis.FakeServer | None = None,
    **kwargs: Any,
) -> ApiHarness:
    server = server or fakeredis.FakeServer()
    redis = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    launcher = launcher or FakeLauncher()
    deps = ApiDeps.from_settings(
        settings, redis=redis, kafka_check=kafka_check, launcher=launcher, model=model, **kwargs
    )
    return ApiHarness(create_app(settings, deps=deps), deps, redis, server, launcher)


def model_folder(path: Path, **meta: Any) -> Path:
    """A valid deployment-model folder (the stream-job fixture) with extra metadata keys."""
    scenario_doc_dir(path)
    meta_path = path / "lr" / "metadata.json"
    doc = json.loads(meta_path.read_text(encoding="utf-8"))
    doc.update(meta)
    meta_path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def parse_sse(body: str) -> list[dict[str, Any]]:
    """SSE frames as dicts: ``{"comment": ...}`` or ``{"event", "id", "data"}`` (+ ``retry``)."""
    frames: list[dict[str, Any]] = []
    for block in body.split("\n\n"):
        if not block:
            continue
        frame: dict[str, Any] = {}
        for line in block.split("\n"):
            if line.startswith(":"):
                frame.setdefault("comments", []).append(line[1:].strip())
            else:
                key, _, value = line.partition(": ")
                frame[key] = json.loads(value) if key == "data" else value
        frames.append(frame)
    return frames
