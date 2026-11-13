"""Resource samplers: process tree (psutil + cgroup) and docker stats (Step 10, #38)."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import psutil  # type: ignore[import-untyped]
import pytest

from f1sa.bench.resources import (
    ContainerSample,
    DockerStatsSampler,
    ProcessSample,
    ProcessSampler,
    cgroup_memory_bytes,
    cgroup_memory_limit,
    host_available_bytes,
    parse_percent,
    parse_size,
    parse_stats_line,
    process_peaks,
    read_container_samples,
    window_peaks,
)

ALLOC = (
    "import sys, time\n"
    "block = bytearray(100 * 1024 * 1024)\n"
    "for i in range(0, len(block), 4096): block[i] = 1\n"
    "print('ready', flush=True)\n"
    "time.sleep(30)\n"
)


def test_process_sampler_sees_a_child_allocating_100_mb(tmp_path: Path) -> None:
    child = subprocess.Popen([sys.executable, "-c", ALLOC], stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout is not None and child.stdout.readline().strip() == "ready"
        seen: list[ProcessSample] = []
        sampler = ProcessSampler(child.pid, interval_s=0.05, on_sample=seen.append).start()
        time.sleep(0.4)
        peaks = sampler.stop()
    finally:
        child.kill()
        child.wait()
    assert peaks.samples >= 3 and len(seen) == peaks.samples
    assert peaks.peak_rss_bytes >= 100 * 1024 * 1024
    assert peaks.peak_cpu_pct >= 0.0


def test_sampler_on_a_process_tree_counts_children() -> None:
    parent = subprocess.Popen(
        [
            sys.executable,
            "-c",
            f"import subprocess,sys; subprocess.run([sys.executable,'-c',{ALLOC!r}])",
        ],
    )
    try:
        time.sleep(1.5)
        sampler = ProcessSampler(parent.pid, interval_s=0.05)
        sample = sampler.sample_once()
    finally:
        kids = psutil.Process(parent.pid).children(recursive=True)
        for proc in [*kids, psutil.Process(parent.pid)]:
            proc.kill()
        parent.wait()
    assert sample.n_procs >= 2
    assert sample.rss_bytes >= 100 * 1024 * 1024


def test_cgroup_and_meminfo_readers(tmp_path: Path) -> None:
    (tmp_path / "memory.current").write_text("1000\n")
    (tmp_path / "memory.stat").write_text("anon 500\ninactive_file 300\nactive_file 10\n")
    (tmp_path / "memory.max").write_text("4294967296\n")
    assert cgroup_memory_bytes(tmp_path) == 700
    assert cgroup_memory_limit(tmp_path) == 4294967296
    (tmp_path / "memory.max").write_text("max\n")
    assert cgroup_memory_limit(tmp_path) is None
    assert cgroup_memory_bytes(tmp_path / "missing") is None
    assert cgroup_memory_limit(tmp_path / "missing") is None
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal: 100 kB\nMemAvailable:   2048 kB\n")
    assert host_available_bytes(meminfo) == 2048 * 1024
    assert host_available_bytes(tmp_path / "nope") is None


def test_process_peaks_of_samples() -> None:
    assert process_peaks([]).samples == 0
    s = [
        ProcessSample(1.0, 100, 50.0, 2, 1000, 9000),
        ProcessSample(2.0, 300, 150.0, 3, None, 8000),
        ProcessSample(3.0, 200, 100.0, 3, 1500, None),
    ]
    p = process_peaks(s)
    assert (p.peak_rss_bytes, p.peak_cpu_pct, p.mean_cpu_pct) == (300, 150.0, 100.0)
    assert (p.peak_cgroup_bytes, p.min_host_available_bytes) == (1500, 8000)
    with pytest.raises(ValueError):
        ProcessSampler(1, interval_s=0)


@pytest.mark.parametrize(
    ("text", "value"),
    [
        ("649.3MiB", int(649.3 * 1024**2)),
        ("1.5GiB", int(1.5 * 1024**3)),
        ("384MiB ", 384 * 1024**2),
        ("12kB", 12000),
        ("0B", 0),
        ("--", None),
        ("", None),
        ("5XB", None),
    ],
)
def test_parse_size(text: str, value: int | None) -> None:
    assert parse_size(text) == value


def test_parse_stats_line_and_percent() -> None:
    line = json.dumps(
        {"Name": "f1sa-app-1", "MemUsage": "1.2GiB / 4GiB", "CPUPerc": "169.85%", "ID": "x"}
    )
    s = parse_stats_line(line, 7.0)
    assert s == ContainerSample(7.0, "f1sa-app-1", int(1.2 * 1024**3), 4 * 1024**3, 169.85)
    assert parse_stats_line("  ", 1.0) is None
    missing = parse_stats_line(json.dumps({"Name": "x", "MemUsage": "-- / --", "CPUPerc": "--"}), 1)
    assert missing is not None and missing.mem_bytes is None and missing.cpu_pct is None
    assert parse_percent("12.5%") == 12.5 and parse_percent("--") is None


def test_docker_sampler_with_a_fake_runner_writes_jsonl_and_window_peaks(tmp_path: Path) -> None:
    outputs = iter(
        [
            '{"Name":"f1sa-app-1","MemUsage":"1GiB / 4GiB","CPUPerc":"50%"}\n'
            '{"Name":"f1sa-spark-worker-1-1","MemUsage":"500MiB / 1.5GiB","CPUPerc":"10%"}\n',
            '{"Name":"f1sa-app-1","MemUsage":"2GiB / 4GiB","CPUPerc":"300%"}\n',
        ]
    )

    def runner() -> str:
        try:
            return next(outputs)
        except StopIteration:
            raise subprocess.CalledProcessError(1, "docker") from None

    ticks = iter([10.0, 20.0, 30.0])
    out = tmp_path / "stats.jsonl"
    sampler = DockerStatsSampler(out, runner=runner, clock=lambda: next(ticks))
    assert len(sampler.sample_once()) == 2
    assert len(sampler.sample_once()) == 1
    assert sampler.sample_once() == [] and sampler.errors == 1
    samples = read_container_samples(out)
    assert len(samples) == 3, "the error line is skipped"
    peaks = window_peaks(samples, 0.0, 25.0)
    assert peaks["f1sa-app-1"].peak_mem_bytes == 2 * 1024**3
    assert peaks["f1sa-app-1"].peak_cpu_pct == 300.0
    assert peaks["f1sa-spark-worker-1-1"].samples == 1
    assert window_peaks(samples, 11.0, 15.0) == {}
    with pytest.raises(ValueError):
        window_peaks(samples, 2.0, 1.0)
    assert read_container_samples(tmp_path / "none.jsonl") == []


def test_docker_sampler_thread_starts_and_stops(tmp_path: Path) -> None:
    out = tmp_path / "s.jsonl"
    with DockerStatsSampler(
        out, interval_s=0.01, runner=lambda: '{"Name":"a","MemUsage":"1MiB / 2MiB","CPUPerc":"1%"}'
    ):
        time.sleep(0.1)
    assert len(read_container_samples(out)) >= 2


def test_experiments_resources_reexports_the_samplers() -> None:
    import importlib.util

    path = Path(__file__).resolve().parents[2] / "experiments" / "_resources.py"
    spec = importlib.util.spec_from_file_location("exp_resources", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.ProcessSampler is ProcessSampler and mod.DockerStatsSampler is DockerStatsSampler
