"""Benchmark path guard, run ids, output folders and ``make bench-clean`` (Step 10)."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from requests.adapters import HTTPAdapter

from f1sa.bench.paths import (
    BenchPathError,
    HttpFsBench,
    bench_file_uri,
    bench_hdfs_uri,
    check_bench_uri,
    clean,
    main,
    new_run_id,
    output_dir,
    plan_clean,
    run_dir,
)

REPO = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "uri",
    [
        "hdfs://namenode:8020/f1/raw",
        "hdfs://namenode:8020/f1/raw/tracinginsights/telemetry",
        "hdfs://namenode:8020/f1/clean/laps",
        "hdfs://namenode:8020/f1/features",
        "hdfs://namenode:8020/f1/bench/../raw",
        "hdfs://namenode:8020/f1/benchmark",
        "hdfs://namenode:8020/f1",
        "hdfs://other:8020/f1/bench/x",
        "s3://bucket/f1/bench",
        "/f1/bench/x",
        "hdfs://namenode:8020relative",
    ],
)
def test_refuses_every_hdfs_path_outside_f1_bench(uri: str) -> None:
    with pytest.raises(BenchPathError):
        check_bench_uri(uri)


def test_allows_f1_bench_and_normalises() -> None:
    assert check_bench_uri("hdfs://namenode:8020/f1/bench") == "hdfs://namenode:8020/f1/bench"
    assert (
        check_bench_uri("hdfs://namenode:8020/f1/bench//r1/scal/")
        == "hdfs://namenode:8020/f1/bench/r1/scal"
    )
    assert bench_hdfs_uri("r1", "x") == "hdfs://namenode:8020/f1/bench/r1/x"


def test_local_paths_only_under_data_bench(tmp_path: Path) -> None:
    ok = check_bench_uri(f"file://{tmp_path}/data/bench/r1/x")
    assert ok == f"file://{tmp_path}/data/bench/r1/x"
    assert bench_file_uri("r1", "data") == f"file://{tmp_path}/data/bench/r1/data"
    for bad in (
        f"file://{tmp_path}/data/lake/clean",
        f"file://{tmp_path}/results",
        f"file://{tmp_path}/data/bench/../lake",
        f"file://{tmp_path}/data/benchmark",
        "file://host/data/bench",
        "file:///etc",
    ):
        with pytest.raises(BenchPathError):
            check_bench_uri(bad)


def test_refuses_a_symlinked_bench_root(tmp_path: Path) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "bench").symlink_to(elsewhere)
    with pytest.raises(BenchPathError, match="symlink"):
        check_bench_uri(f"file://{tmp_path}/data/bench/x")


def test_run_ids_and_output_dirs(tmp_path: Path) -> None:
    rid = new_run_id(datetime(2026, 11, 13, 22, 1, 2, tzinfo=UTC))
    assert rid == "bench_20261113T220102Z"
    assert output_dir(rid, record=False) == tmp_path / "data" / "bench" / rid
    assert output_dir(rid, record=True) == tmp_path / "results"
    for bad in ("", "../x", "a/b", ".hidden"):
        with pytest.raises(BenchPathError):
            run_dir(bad)


class FakeHttpFs(HTTPAdapter):
    """LISTSTATUS and recursive DELETE over an in-memory set of paths."""

    def __init__(self, paths: set[str]) -> None:
        super().__init__()
        self.paths = set(paths)
        self.ops: list[tuple[str, str, dict[str, str]]] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        parsed = urlparse(request.url or "")
        q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        path = parsed.path.removeprefix("/webhdfs/v1")
        self.ops.append((q["op"], path, q))
        resp = requests.Response()
        resp.request = request
        if q["op"] == "LISTSTATUS":
            prefix = path.rstrip("/") + "/"
            kids = sorted(
                {p[len(prefix) :].split("/")[0] for p in self.paths if p.startswith(prefix)}
            )
            if not kids and path not in self.paths:
                resp.status_code, body = 404, {}
            else:
                resp.status_code = 200
                body = {
                    "FileStatuses": {
                        "FileStatus": [{"pathSuffix": k, "type": "DIRECTORY"} for k in kids]
                    }
                }
        elif q["op"] == "DELETE":
            assert q.get("recursive") == "true"
            gone = {p for p in self.paths if p == path or p.startswith(path + "/")}
            self.paths -= gone
            resp.status_code, body = 200, {"boolean": bool(gone)}
        else:
            resp.status_code, body = 400, {}
        resp._content = json.dumps(body).encode()
        return resp


def _client(fake: FakeHttpFs) -> HttpFsBench:
    session = requests.Session()
    session.mount("http://", fake)
    return HttpFsBench("http://httpfs:14000", "hadoop", session)


def test_httpfs_client_deletes_only_strict_children_of_f1_bench() -> None:
    fake = FakeHttpFs({"/f1/bench/r1/a", "/f1/bench/r2", "/f1/raw/x"})
    client = _client(fake)
    assert client.children() == ["/f1/bench/r1", "/f1/bench/r2"]
    client.delete("/f1/bench/r1")
    assert fake.paths == {"/f1/bench/r2", "/f1/raw/x"}
    for bad in ("/f1/bench", "/f1/raw/x", "/f1/bench/../raw", "/f1/clean"):
        with pytest.raises(BenchPathError):
            client.delete(bad)
    assert "/f1/raw/x" in fake.paths
    assert [op for op, _p, _q in fake.ops].count("DELETE") == 1


def _bench_tree(base: Path) -> Path:
    root = base / "data" / "bench"
    (root / "bench_1" / "cells").mkdir(parents=True)
    (root / "bench_1" / "cells" / "a.json").write_text("{}")
    (root / "loose.txt").write_text("x")
    (base / "data" / "lake").mkdir(parents=True)
    (base / "data" / "lake" / "keep").write_text("keep")
    (base / "outside").mkdir()
    (root / "link").symlink_to(base / "outside")
    return root


def test_plan_lists_entries_and_clean_deletes_only_inside_the_root(tmp_path: Path) -> None:
    root = _bench_tree(tmp_path)
    fake = FakeHttpFs({"/f1/bench/r1/x", "/f1/raw/y"})
    client = _client(fake)
    plan = plan_clean(hdfs=client)
    assert plan.local == [root / "bench_1", root / "loose.txt"]
    assert plan.skipped == [(root / "link", "symlink")]
    assert plan.hdfs == ["/f1/bench/r1"]
    done = clean(plan, client)
    assert sorted(p.name for p in root.iterdir()) == ["link"]
    assert (tmp_path / "outside").is_dir() and (tmp_path / "data" / "lake" / "keep").exists()
    assert fake.paths == {"/f1/raw/y"}
    assert "hdfs://namenode:8020/f1/bench/r1" in done


@pytest.mark.parametrize(
    ("root", "hdfs_root"),
    [
        (Path("data/lake"), "/f1/bench"),
        (Path("data"), "/f1/bench"),
        (Path("results"), "/f1/bench"),
        (Path("data/bench/.."), "/f1/bench"),
        (Path("/tmp"), "/f1/bench"),
        (Path("data/bench"), "/f1/raw"),
        (Path("data/bench"), "/f1/clean"),
        (Path("data/bench"), "/f1/features"),
        (Path("data/bench"), "/f1/bench/../raw"),
        (Path("data/bench"), "/f1"),
    ],
)
def test_plan_refuses_any_other_root(tmp_path: Path, root: Path, hdfs_root: str) -> None:
    _bench_tree(tmp_path)
    with pytest.raises(BenchPathError):
        plan_clean(root, hdfs_root)


def test_plan_refuses_a_symlinked_data_bench(tmp_path: Path) -> None:
    (tmp_path / "data").mkdir()
    (tmp_path / "real").mkdir()
    (tmp_path / "data" / "bench").symlink_to(tmp_path / "real")
    with pytest.raises(BenchPathError, match="symlink"):
        plan_clean()


def test_main_lists_without_yes_and_deletes_with_yes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _bench_tree(tmp_path)
    fake = FakeHttpFs({"/f1/bench/r1/x"})
    session = requests.Session()
    session.mount("http://", fake)
    assert main([], session=session) == 1
    assert (root / "bench_1").exists() and fake.paths == {"/f1/bench/r1/x"}
    assert "would delete" in capsys.readouterr().out
    assert main(["--yes"], session=session) == 0
    assert not (root / "bench_1").exists() and fake.paths == set()
    assert main(["--yes", "--root", "data/lake"], session=session) == 2
    assert main(["--yes", "--hdfs-root", "/f1/raw"], session=session) == 2
    assert (tmp_path / "data" / "lake" / "keep").exists()


def test_main_reports_unreachable_hdfs_but_cleans_local(tmp_path: Path) -> None:
    root = _bench_tree(tmp_path)

    class Down(HTTPAdapter):
        def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
            raise requests.ConnectionError("down")

    session = requests.Session()
    session.mount("http://", Down())
    assert main(["--yes"], session=session) == 1
    assert not (root / "bench_1").exists()
    assert main(["--yes", "--local-only"], session=session) == 0


def _make(*args: str) -> subprocess.CompletedProcess[str]:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    return subprocess.run(
        ["make", "-n", "-C", str(REPO), *args], capture_output=True, text=True, check=False
    )


def test_make_bench_clean_needs_confirm() -> None:
    dry = _make("bench-clean").stdout
    assert "python -m f1sa.bench.paths" in dry and "--yes" not in dry
    assert "python -m f1sa.bench.paths --yes" in _make("bench-clean", "CONFIRM=yes").stdout
    assert "--yes" not in _make("bench-clean", "CONFIRM=y").stdout
