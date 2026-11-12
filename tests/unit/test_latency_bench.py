"""The Redis latency benchmark logic (#37) on fakeredis: stats, parsing, runs, outputs, guards."""

from __future__ import annotations

import csv
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import redis

from f1sa.config import Settings
from f1sa.serving import keys as K
from f1sa.serving import latency_bench as LB
from f1sa.serving.redis_store import RaceStore


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


@pytest.fixture
def store(server: fakeredis.FakeServer) -> Iterator[RaceStore]:
    s = RaceStore(fakeredis.FakeRedis(server=server, decode_responses=True), Settings())
    yield s
    s.client.close()


@pytest.fixture
def reader(server: fakeredis.FakeServer) -> Iterator[fakeredis.FakeRedis]:
    c = fakeredis.FakeRedis(server=server, decode_responses=True)
    yield c
    c.close()


CLI_30 = "".join(f"0.{100 + i} 0.{700 + i} 0.200 {100 if i % 2 else 98}\n" for i in range(30))


def _cli_ok(stdout: str = CLI_30, rc: int = 0) -> Any:
    def runner(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        assert cmd == list(LB.CLI_COMMAND)
        return subprocess.CompletedProcess(cmd, rc, stdout=stdout, stderr="boom" if rc else "")

    return runner


def test_summarise_ms() -> None:
    got = LB.summarise_ms([1_000_000, 2_000_000, 3_000_000, 4_000_000, 100_000_000])
    assert got["n"] == 5 and got["p50_ms"] == 3.0 and got["min_ms"] == 1.0
    assert got["max_ms"] == 100.0 and got["mean_ms"] == pytest.approx(22.0)
    assert got["p95_ms"] == pytest.approx(80.8) and got["p99_ms"] == pytest.approx(96.16)
    with pytest.raises(LB.BenchError, match="no samples"):
        LB.summarise_ms([])


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0.109 0.725 0.248 98\n", [LB.CliLatency(0.109, 0.725, 0.248, 98)]),
        ("min: 0, max: 1, avg: 0.11 (2900 samples)", [LB.CliLatency(0.0, 1.0, 0.11, 2900)]),
        (
            "\rmin: 0, max: 3, avg: 0.20 (10 samples)\rmin: 0, max: 4, avg: 0.21 (20 samples)",
            [LB.CliLatency(0.0, 3.0, 0.2, 10), LB.CliLatency(0.0, 4.0, 0.21, 20)],
        ),
        (
            "1 2 1.5 3\nnoise\n0.1 0.9 0.3 2958\n",
            [LB.CliLatency(1.0, 2.0, 1.5, 3), LB.CliLatency(0.1, 0.9, 0.3, 2958)],
        ),
    ],
)
def test_parse_redis_cli(text: str, expected: list[LB.CliLatency]) -> None:
    assert LB.parse_redis_cli(text) == expected


@pytest.mark.parametrize(
    "text", ["", "Could not connect to Redis", "0.1 0.2 0.3 0\n", "a b c d", "1 2 1.5 3\n1 2 1 0\n"]
)
def test_parse_redis_cli_rejects_bad_output(text: str) -> None:
    with pytest.raises(LB.BenchError, match="cannot parse"):
        LB.parse_redis_cli(text)


def test_combine_cli_weights_by_samples() -> None:
    got = LB.combine_cli([LB.CliLatency(0.1, 0.5, 0.2, 100), LB.CliLatency(0.05, 0.9, 0.5, 300)])
    assert got == LB.CliLatency(0.05, 0.9, pytest.approx(0.425), 400)
    with pytest.raises(LB.BenchError, match="no redis-cli intervals"):
        LB.combine_cli([])


def test_cli_command_repeats_one_second_intervals_in_the_container() -> None:
    cmd = LB.CLI_COMMAND
    assert cmd[:5] == ("docker", "compose", "exec", "-T", "redis")
    assert cmd[-1] == "for i in $(seq 30); do redis-cli --latency -i 1 || exit 1; done"


def test_run_rate_rows_and_counts(store: RaceStore, reader: fakeredis.FakeRedis) -> None:
    rows = LB.run_rate(store, reader, rate=20.0, duration_s=0.2)
    assert [r["operation"] for r in rows] == ["write", "hgetall", "zrange"]
    assert [r["n"] for r in rows] == [4, 4 * LB.N_DRIVERS, 4]
    for r in rows:
        assert set(r) == set(LB.COLUMNS)
        assert 0 <= r["min_ms"] <= r["p50_ms"] <= r["p95_ms"] <= r["p99_ms"] <= r["max_ms"]
        assert r["rate_per_s"] == 20.0 and r["drivers"] == 20 and r["source"] == "redis-py"
    assert rows[0]["achieved_rate_per_s"] == pytest.approx(20.0, rel=0.5)
    order = store.get_order(LB.BENCH_SEASON, LB.BENCH_ROUND)
    assert [d for d, _ in order] == list(LB.DRIVERS)
    leader = store.get_driver_state(LB.BENCH_SEASON, LB.BENCH_ROUND, "bench_01")
    assert leader is not None and leader.state.gap_ahead_s is None


def test_run_rate_counts_late_ticks(store: RaceStore, reader: fakeredis.FakeRedis) -> None:
    t = iter(float(i) for i in range(1000))  # every clock read is 1 s later: period 0.1 s
    rows = LB.run_rate(store, reader, 10.0, 0.5, clock=lambda: next(t), sleep=lambda _s: None)
    # clock reads 0 (start), 1..5 (ticks, targets 0.0..0.4: all more than a period late), 6 (end)
    assert rows[0]["n"] == 5 and rows[0]["late_ticks"] == 5
    assert rows[0]["duration_s"] == 6.0 and rows[0]["achieved_rate_per_s"] == pytest.approx(5 / 6)


@pytest.mark.parametrize(("rate", "duration"), [(0.0, 1.0), (-1.0, 1.0), (10.0, 0.0)])
def test_run_rate_rejects_bad_parameters(
    rate: float, duration: float, store: RaceStore, reader: fakeredis.FakeRedis
) -> None:
    with pytest.raises(LB.BenchError, match="> 0"):
        LB.run_rate(store, reader, rate, duration)


def test_benchmark_cleans_up_and_adds_the_cli_row(
    store: RaceStore, reader: fakeredis.FakeRedis
) -> None:
    reader.set("race:2024:5:order", "untouched")
    rows = LB.run_benchmark(
        store,
        reader,
        rates=(50.0, 100.0),
        duration_s=0.05,
        cli_runner=_cli_ok(),
        out=lambda _: None,
    )
    assert [(r["rate_per_s"], r["operation"]) for r in rows[:6]] == [
        (50.0, "write"),
        (50.0, "hgetall"),
        (50.0, "zrange"),
        (100.0, "write"),
        (100.0, "hgetall"),
        (100.0, "zrange"),
    ]
    cli = rows[6]
    assert cli["operation"] == "ping" and cli["n"] == 15 * 98 + 15 * 100
    assert (cli["min_ms"], cli["max_ms"]) == (0.1, 0.729) and cli["mean_ms"] == pytest.approx(0.2)
    assert cli["duration_s"] == 30 and cli["p50_ms"] == ""
    assert store.race_keys(LB.BENCH_SEASON, LB.BENCH_ROUND) == []
    assert reader.get("race:2024:5:order") == "untouched"


def test_benchmark_refuses_an_occupied_namespace(
    store: RaceStore, reader: fakeredis.FakeRedis
) -> None:
    key = K.order_key(LB.BENCH_SEASON, LB.BENCH_ROUND)
    reader.set(key, "someone else's")
    with pytest.raises(LB.BenchError, match="refusing"):
        LB.run_benchmark(store, reader, rates=(10.0,), duration_s=0.1, with_cli=False)
    assert reader.get(key) == "someone else's"


@pytest.mark.parametrize(
    "runner",
    [
        _cli_ok(rc=1),
        _cli_ok(stdout="Could not connect\n"),
        _cli_ok(stdout="0.1 0.2 0.15 99\n"),  # 1 interval instead of 30
        "missing",
    ],
)
def test_cli_failure_is_an_error_after_cleanup(
    runner: Any, store: RaceStore, reader: fakeredis.FakeRedis
) -> None:
    if runner == "missing":

        def runner(*_a: Any, **_k: Any) -> subprocess.CompletedProcess[str]:
            raise FileNotFoundError("docker")

    with pytest.raises(LB.BenchError):
        LB.run_benchmark(
            store, reader, rates=(50.0,), duration_s=0.02, cli_runner=runner, out=lambda _: None
        )
    assert store.race_keys(LB.BENCH_SEASON, LB.BENCH_ROUND) == []


def test_csv_and_readme(tmp_path: Path, store: RaceStore) -> None:
    rows = [
        {"source": "redis-py", "rate_per_s": 1.0, "operation": "write", "n": 3, "p50_ms": 0.123456},
        {"source": "redis-cli --latency (in container)", "operation": "ping", "n": 98},
    ]
    path = tmp_path / "results" / LB.CSV_NAME
    LB.write_csv(path, rows)
    got = list(csv.DictReader(path.open()))
    assert list(got[0]) == list(LB.COLUMNS) and got[0]["p50_ms"] == "0.1235"
    assert got[1]["p50_ms"] == "" and not path.with_suffix(".csv.tmp").exists()

    def fake_run(cmd: Any) -> str:
        return {"rev-parse": "abc123", "status": ""}.get(cmd[1], "unavailable")

    pairs = LB.fingerprint(
        store.client, now=datetime(2026, 11, 12, 9, 0, tzinfo=UTC), run=fake_run, params={"x": 1}
    )
    d = dict(pairs)
    assert d["measured_at_utc"] == "2026-11-12T09:00:00Z" and d["git_sha"] == "abc123"
    assert d["git_dirty"] == "false" and d["redis_py"] == redis.__version__
    assert d["params"] == "x=1" and d["redis_container_mem_limit_bytes"] == "unavailable"
    readme = tmp_path / "results" / LB.README_NAME
    LB.append_readme(readme, LB.CSV_NAME, pairs)
    LB.append_readme(readme, LB.CSV_NAME, pairs)
    text = readme.read_text()
    assert text.startswith("# Results\n") and text.count("# Results") == 1
    assert text.count("## `redis_latency.csv` (2026-11-12T09:00:00Z)") == 2
    assert "| git_sha | abc123 |" in text


def test_main_end_to_end_on_fakeredis(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, server: fakeredis.FakeServer
) -> None:
    monkeypatch.chdir(tmp_path)

    def from_url(url: str, **kw: Any) -> fakeredis.FakeRedis:
        assert url == Settings().redis_url
        return fakeredis.FakeRedis(server=server, **kw)

    monkeypatch.setattr(redis.Redis, "from_url", staticmethod(from_url))
    argv = ["--rates", "40", "--duration", "0.05", "--skip-cli"]
    assert LB.main(argv) == 0
    rows = list(csv.DictReader((tmp_path / "results" / LB.CSV_NAME).open()))
    assert [r["operation"] for r in rows] == ["write", "hgetall", "zrange"]
    assert "redis_cli=skipped" in (tmp_path / "results" / LB.README_NAME).read_text()
    assert LB.main(argv) == 0  # the namespace was cleaned, so a rerun works
    assert LB.main([*argv, "--results-dir", str(tmp_path / "elsewhere")]) == 2


def test_main_reports_unreachable_redis(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/0")
    from f1sa.config import get_settings

    get_settings.cache_clear()
    assert LB.main(["--rates", "1", "--duration", "0.01", "--skip-cli"]) == 3
    assert "not reachable" in capsys.readouterr().err
    assert not (tmp_path / "results" / LB.CSV_NAME).exists()
