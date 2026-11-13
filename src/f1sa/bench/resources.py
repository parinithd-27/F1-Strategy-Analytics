"""Resource usage while a benchmark runs (traceability #38; ``experiments/_resources.py``).

Two samplers, each at a fixed interval in a background thread, each returning peaks:

* :class:`ProcessSampler`: RSS and CPU% (psutil) of a process tree (the cell's Python driver, its
  JVM and the PySpark Python workers), plus the container's cgroup v2 memory (``memory.current``
  minus ``inactive_file``, the same formula ``docker stats`` uses) and the host's ``MemAvailable``.
  It runs inside the app container. An ``on_sample`` callback lets the memory guard kill a cell.
* :class:`DockerStatsSampler`: memory and CPU% per container from ``docker stats --no-stream``.
  It runs on the host (the app container has no Docker socket) and appends JSON lines with a
  timestamp, so the peaks can be cut per cell window later (:func:`window_peaks`; host and
  containers share one kernel clock).

CPU% is per process summed over the tree, so 100% is one full core (``local[4]`` can reach 400%).
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import psutil  # type: ignore[import-untyped]  # no stubs (types-psutil not a dependency)

CGROUP_ROOT = Path("/sys/fs/cgroup")
MEMINFO = Path("/proc/meminfo")


# ============================================================================== process tree
@dataclass(frozen=True)
class ProcessSample:
    ts: float
    rss_bytes: int
    cpu_pct: float
    n_procs: int
    cgroup_bytes: int | None
    host_available_bytes: int | None


@dataclass(frozen=True)
class ProcessPeaks:
    samples: int
    peak_rss_bytes: int
    peak_cpu_pct: float
    mean_cpu_pct: float
    peak_cgroup_bytes: int | None
    min_host_available_bytes: int | None


def cgroup_memory_bytes(root: Path = CGROUP_ROOT) -> int | None:
    """Container memory in use (cgroup v2 ``memory.current`` minus ``inactive_file``), or None
    outside a cgroup v2 container."""
    try:
        current = int((root / "memory.current").read_text().strip())
        inactive = 0
        for line in (root / "memory.stat").read_text().splitlines():
            key, _, value = line.partition(" ")
            if key == "inactive_file":
                inactive = int(value)
                break
    except (OSError, ValueError):
        return None
    return max(current - inactive, 0)


def cgroup_memory_limit(root: Path = CGROUP_ROOT) -> int | None:
    """The cgroup v2 ``memory.max`` in bytes, or None when unlimited or unavailable."""
    try:
        text = (root / "memory.max").read_text().strip()
    except OSError:
        return None
    return None if text == "max" else int(text)


def host_available_bytes(meminfo: Path = MEMINFO) -> int | None:
    """``MemAvailable`` of the (WSL) VM in bytes, or None."""
    try:
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


class _Tree:
    """psutil Process objects kept across samples, so ``cpu_percent`` has a previous reading."""

    def __init__(self, pid: int) -> None:
        self.root = psutil.Process(pid)
        self.procs: dict[int, psutil.Process] = {}

    def sample(self) -> tuple[int, float, int]:
        try:
            current = [self.root, *self.root.children(recursive=True)]
        except psutil.NoSuchProcess:
            return 0, 0.0, 0
        rss, cpu, n = 0, 0.0, 0
        seen: set[int] = set()
        for proc in current:
            known = self.procs.setdefault(proc.pid, proc)
            seen.add(proc.pid)
            try:
                rss += int(known.memory_info().rss)
                cpu += float(known.cpu_percent(None))
                n += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        for pid in set(self.procs) - seen:
            del self.procs[pid]
        return rss, cpu, n


class ProcessSampler:
    """Sample a process tree every ``interval_s`` seconds until :meth:`stop`."""

    def __init__(
        self,
        pid: int,
        *,
        interval_s: float = 0.5,
        on_sample: Callable[[ProcessSample], None] | None = None,
        cgroup_root: Path = CGROUP_ROOT,
        meminfo: Path = MEMINFO,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if interval_s <= 0:
            raise ValueError(f"interval_s must be > 0, got {interval_s}")
        self._tree = _Tree(pid)
        self._interval = interval_s
        self._on_sample = on_sample
        self._cgroup_root = cgroup_root
        self._meminfo = meminfo
        self._clock = clock
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=f"sampler-{pid}", daemon=True)
        self.samples: list[ProcessSample] = []

    def sample_once(self) -> ProcessSample:
        rss, cpu, n = self._tree.sample()
        sample = ProcessSample(
            ts=self._clock(),
            rss_bytes=rss,
            cpu_pct=cpu,
            n_procs=n,
            cgroup_bytes=cgroup_memory_bytes(self._cgroup_root),
            host_available_bytes=host_available_bytes(self._meminfo),
        )
        self.samples.append(sample)
        if self._on_sample is not None:
            self._on_sample(sample)
        return sample

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.sample_once()
            self._stop.wait(self._interval)

    def start(self) -> ProcessSampler:
        self._thread.start()
        return self

    def stop(self) -> ProcessPeaks:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=10 * self._interval + 5)
        return process_peaks(self.samples)

    def __enter__(self) -> ProcessSampler:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


def process_peaks(samples: Sequence[ProcessSample]) -> ProcessPeaks:
    """Peaks over ``samples``. The first CPU reading of a new process is psutil's 0.0 and is kept
    (it can only lower the mean)."""
    if not samples:
        return ProcessPeaks(0, 0, 0.0, 0.0, None, None)
    cg = [s.cgroup_bytes for s in samples if s.cgroup_bytes is not None]
    host = [s.host_available_bytes for s in samples if s.host_available_bytes is not None]
    return ProcessPeaks(
        samples=len(samples),
        peak_rss_bytes=max(s.rss_bytes for s in samples),
        peak_cpu_pct=max(s.cpu_pct for s in samples),
        mean_cpu_pct=sum(s.cpu_pct for s in samples) / len(samples),
        peak_cgroup_bytes=max(cg) if cg else None,
        min_host_available_bytes=min(host) if host else None,
    )


# ============================================================================== docker stats
_SIZE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*$")
_UNITS = {
    "": 1,
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


def parse_size(text: str) -> int | None:
    """``"1.5GiB"`` -> bytes; ``"--"`` / empty / unknown units -> None."""
    m = _SIZE.match(text)
    if m is None:
        return None
    unit = _UNITS.get(m.group(2).lower())
    return None if unit is None else int(float(m.group(1)) * unit)


def parse_percent(text: str) -> float | None:
    """``"169.85%"`` -> 169.85; ``"--"`` -> None."""
    try:
        return float(text.strip().rstrip("%"))
    except ValueError:
        return None


@dataclass(frozen=True)
class ContainerSample:
    ts: float
    name: str
    mem_bytes: int | None
    mem_limit_bytes: int | None
    cpu_pct: float | None


def parse_stats_line(line: str, ts: float) -> ContainerSample | None:
    """One ``docker stats --format '{{json .}}'`` line, or None for a blank line."""
    if not line.strip():
        return None
    obj: dict[str, Any] = json.loads(line)
    used, _, limit = str(obj.get("MemUsage", "")).partition("/")
    return ContainerSample(
        ts=ts,
        name=str(obj.get("Name", "?")),
        mem_bytes=parse_size(used),
        mem_limit_bytes=parse_size(limit),
        cpu_pct=parse_percent(str(obj.get("CPUPerc", ""))),
    )


def _docker_stats() -> str:
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return out.stdout


class DockerStatsSampler:
    """Append one JSON line per container every ``interval_s`` seconds to ``out_path``.

    ``docker stats --no-stream`` itself takes about 1-2 s, so the effective interval is the larger
    of the two. A failing call is recorded as an ``error`` line and sampling continues."""

    def __init__(
        self,
        out_path: Path,
        *,
        interval_s: float = 2.0,
        runner: Callable[[], str] = _docker_stats,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if interval_s <= 0:
            raise ValueError(f"interval_s must be > 0, got {interval_s}")
        self.out_path = out_path
        self._interval = interval_s
        self._runner = runner
        self._clock = clock
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="docker-stats", daemon=True)
        self.errors = 0

    def sample_once(self) -> list[ContainerSample]:
        ts = self._clock()
        try:
            text = self._runner()
        except (OSError, subprocess.SubprocessError) as exc:
            self.errors += 1
            self._append([{"ts": ts, "error": f"{type(exc).__name__}: {exc}"}])
            return []
        samples = [s for s in (parse_stats_line(ln, ts) for ln in text.splitlines()) if s]
        self._append([asdict(s) for s in samples])
        return samples

    def _append(self, rows: Iterable[dict[str, Any]]) -> None:
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        with self.out_path.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, sort_keys=True) + "\n")

    def _loop(self) -> None:
        while not self._stop.is_set():
            started = self._clock()
            self.sample_once()
            self._stop.wait(max(self._interval - (self._clock() - started), 0.0))

    def start(self) -> DockerStatsSampler:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=90)

    def __enter__(self) -> DockerStatsSampler:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


def read_container_samples(path: Path) -> list[ContainerSample]:
    """The samples a :class:`DockerStatsSampler` wrote (error lines skipped). The file may be read
    while the host is still appending to it, so a line that does not parse (a partly written last
    line) is skipped."""
    if not path.exists():
        return []
    out: list[ContainerSample] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "error" in obj:
            continue
        out.append(ContainerSample(**obj))
    return out


@dataclass(frozen=True)
class ContainerPeak:
    name: str
    samples: int
    peak_mem_bytes: int | None
    peak_cpu_pct: float | None


def window_peaks(
    samples: Iterable[ContainerSample], start: float, end: float
) -> dict[str, ContainerPeak]:
    """Peak memory and CPU per container over samples taken in ``[start, end]``."""
    if end < start:
        raise ValueError(f"window ends before it starts: {start} > {end}")
    by_name: dict[str, list[ContainerSample]] = {}
    for s in samples:
        if start <= s.ts <= end:
            by_name.setdefault(s.name, []).append(s)
    out: dict[str, ContainerPeak] = {}
    for name, rows in sorted(by_name.items()):
        mem = [r.mem_bytes for r in rows if r.mem_bytes is not None]
        cpu = [r.cpu_pct for r in rows if r.cpu_pct is not None]
        out[name] = ContainerPeak(
            name=name,
            samples=len(rows),
            peak_mem_bytes=max(mem) if mem else None,
            peak_cpu_pct=max(cpu) if cpu else None,
        )
    return out
