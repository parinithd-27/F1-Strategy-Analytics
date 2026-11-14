"""CSV rows, plots and the environment fingerprint of the Step 10 benchmarks.

``scalability.csv`` has one row per grid cell (prep, build and telemetry phases; skipped, aborted
and failed cells too, with the reason). Wall-time statistics are over the timed runs only; byte
counts are the median over the timed runs; peaks combine the in-container psutil/cgroup sampler
with the host's ``docker stats`` samples cut to the cell's time window.

``partitioning.csv`` has one row per (dataset, file system, layout, variant, query) with the
speed-ups against the simplest baseline and against unpartitioned without cache on the same file
system (:func:`f1sa.bench.partitioning.speedups`).
"""

from __future__ import annotations

import csv
import io
import os
import platform
import subprocess
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from f1sa.bench.resources import ContainerSample, window_peaks
from f1sa.bench.timing import summarise

CONTAINERS = ("app", "spark_master", "spark_worker_1", "spark_worker_2", "namenode", "datanode")
SCALABILITY_COLUMNS: tuple[str, ...] = (
    "run_id",
    "git_sha",
    "git_dirty",
    "cell_id",
    "phase",
    "status",
    "reason",
    "master",
    "master_url",
    "n_seasons",
    "factor",
    "telemetry_rounds",
    "laps_rows",
    "warmup_runs",
    "n_runs",
    "median_s",
    "q1_s",
    "q3_s",
    "iqr_s",
    "min_s",
    "max_s",
    "prep_s",
    "startup_s",
    "input_bytes",
    "output_bytes",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "metrics_complete",
    "peak_tree_rss_bytes",
    "peak_tree_cpu_pct",
    "mean_tree_cpu_pct",
    "peak_app_cgroup_bytes",
    "min_host_available_bytes",
    *(f"peak_mem_{c}_bytes" for c in CONTAINERS),
    *(f"peak_cpu_{c}_pct" for c in CONTAINERS),
    "peak_all_containers_mem_bytes",
    "driver_memory",
    "executor_memory",
    "shuffle_partitions",
    "started_at_utc",
    "ended_at_utc",
)
SUPPLEMENT_COLUMNS: tuple[str, ...] = (*SCALABILITY_COLUMNS, "rerun_of", "note")
"""``results/scalability.csv`` once a supplementary re-run is merged into it: ``rerun_of`` is the
run whose failed row the re-run replaces, ``note`` says why (both empty on every other row). The
grid's own run CSVs keep :data:`SCALABILITY_COLUMNS`."""
PARTITIONING_COLUMNS: tuple[str, ...] = (
    "run_id",
    "git_sha",
    "git_dirty",
    "dataset",
    "fs",
    "layout",
    "variant",
    "query",
    "rows_in",
    "warmup_runs",
    "n_runs",
    "median_s",
    "q1_s",
    "q3_s",
    "iqr_s",
    "min_s",
    "max_s",
    "materialise_s",
    "input_bytes",
    "shuffle_read_bytes",
    "result_rows",
    "result_checksum",
    "checksum_match",
    "speedup_vs_simplest",
    "speedup_vs_unpartitioned_same_fs",
    "master",
    "files_unpartitioned",
)
SERIES_COLORS = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300")
"""Categorical slots 1-6 of the reference palette (light mode), in fixed order."""
MASTER_ORDER = ("local1", "local2", "local4", "cluster")
MASTER_LABEL = {
    "local1": "local[1]",
    "local2": "local[2]",
    "local4": "local[4]",
    "cluster": "standalone 2x2 cores",
}


def container_key(name: str) -> str:
    """``f1sa-spark-worker-1-1`` -> ``spark_worker_1`` (compose project prefix and replica
    suffix removed)."""
    base = name.removeprefix("f1sa-")
    if base.endswith("-1"):
        base = base[: -len("-1")]
    return base.replace("-", "_")


def _utc(ts: float | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _median_int(values: Iterable[Any]) -> int | str:
    nums = [int(v) for v in values if v is not None]
    return int(median(nums)) if nums else ""


def all_containers_peak(samples: Sequence[ContainerSample], start: float, end: float) -> int | str:
    """The largest sum of container memory at one sampling instant in ``[start, end]``."""
    by_ts: dict[float, int] = {}
    for s in samples:
        if start <= s.ts <= end and s.mem_bytes is not None:
            by_ts[s.ts] = by_ts.get(s.ts, 0) + s.mem_bytes
    return max(by_ts.values()) if by_ts else ""


def scalability_row(
    record: Mapping[str, Any],
    samples: Sequence[ContainerSample],
    *,
    git_sha: str,
    git_dirty: str,
) -> dict[str, Any]:
    """One CSV row from a ``cells.jsonl`` record plus the host's docker samples."""
    cell = record["cell"]
    res: Mapping[str, Any] = record.get("result") or {}
    peaks: Mapping[str, Any] = record.get("peaks") or {}
    conf: Mapping[str, Any] = res.get("conf") or {}
    row: dict[str, Any] = {
        "run_id": record.get("run_id", ""),
        "git_sha": git_sha,
        "git_dirty": git_dirty,
        "cell_id": record["cell_id"],
        "phase": cell["phase"],
        "status": record.get("status", ""),
        "reason": record.get("reason", ""),
        "master": cell["master"],
        "master_url": res.get("master_url", ""),
        "n_seasons": cell["n_seasons"],
        "factor": cell["factor"],
        "telemetry_rounds": len(res.get("rounds", [])) or "",
        "laps_rows": res.get("laps_rows", ""),
        "warmup_runs": len(res.get("warmup_s", [])),
        "prep_s": res.get("prep_s", ""),
        "startup_s": res.get("startup_s", ""),
        "peak_tree_rss_bytes": peaks.get("peak_rss_bytes", ""),
        "peak_tree_cpu_pct": peaks.get("peak_cpu_pct", ""),
        "mean_tree_cpu_pct": peaks.get("mean_cpu_pct", ""),
        "peak_app_cgroup_bytes": peaks.get("peak_cgroup_bytes") or "",
        "min_host_available_bytes": peaks.get("min_host_available_bytes") or "",
        "driver_memory": conf.get("spark.driver.memory", ""),
        "executor_memory": conf.get("spark.executor.memory", ""),
        "shuffle_partitions": conf.get("spark.sql.shuffle.partitions", ""),
        "started_at_utc": _utc(record.get("started_at")),
        "ended_at_utc": _utc(record.get("ended_at")),
    }
    if cell["phase"] == "prep" and res.get("rows"):
        row["laps_rows"] = res["rows"].get("laps", "")
    times = res.get("times_s") or []
    row["n_runs"] = len(times)
    if times:
        s = summarise(times)
        row |= {
            "median_s": s.median_s,
            "q1_s": s.q1_s,
            "q3_s": s.q3_s,
            "iqr_s": s.iqr_s,
            "min_s": s.min_s,
            "max_s": s.max_s,
        }
    metrics = res.get("metrics") or []
    for key in ("input_bytes", "output_bytes", "shuffle_read_bytes", "shuffle_write_bytes"):
        row[key] = _median_int(m.get(key) for m in metrics)
    if metrics:
        row["metrics_complete"] = str(all(m.get("complete") for m in metrics))
    start, end = record.get("started_at"), record.get("ended_at")
    if start is not None and end is not None:
        win = {container_key(k): v for k, v in window_peaks(samples, start, end).items()}
        for c in CONTAINERS:
            peak = win.get(c)
            row[f"peak_mem_{c}_bytes"] = "" if peak is None else (peak.peak_mem_bytes or "")
            row[f"peak_cpu_{c}_pct"] = "" if peak is None else (peak.peak_cpu_pct or "")
        row["peak_all_containers_mem_bytes"] = all_containers_peak(samples, start, end)
    return row


def _fmt(v: Any) -> Any:
    return f"{v:.4f}" if isinstance(v, float) else v


def _csv_text(rows: Sequence[Mapping[str, Any]], columns: Sequence[str], header: bool) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(columns), lineterminator="\n")
    if header:
        w.writeheader()
    for r in rows:
        w.writerow({c: _fmt(r.get(c, "")) for c in columns})
    return buf.getvalue()


def replace_atomically(path: Path, text: str) -> None:
    """Write ``text`` to a temp file next to ``path``, fsync it, rename it over ``path`` and fsync
    the folder: a reader (or a crash) sees the old file or the new one, never a partial one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
    """Atomic CSV write with a fixed column order (missing values empty)."""
    replace_atomically(path, _csv_text(rows, columns, header=True))


def append_rows_atomic(
    path: Path, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]
) -> None:
    """Append ``rows`` to the CSV at ``path`` atomically (the whole file is rewritten to a temp file
    and renamed), creating it with a header if needed. Refuses a file with a different header."""
    if path.exists():
        current = path.read_text(encoding="utf-8")
        first = current.split("\n", 1)[0]
        if first != ",".join(columns):
            raise ValueError(f"{path} has a different header; refusing to append")
        if current and not current.endswith("\n"):
            current += "\n"
        text = current + _csv_text(rows, columns, header=False)
    else:
        text = _csv_text(rows, columns, header=True)
    replace_atomically(path, text)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Rows of a CSV written by :func:`write_csv` / :func:`append_rows_atomic` (empty if absent)."""
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ============================================================================== supplement
class SupplementError(ValueError):
    """A supplementary re-run cannot be merged into the recorded results."""


def supplement_note(now: datetime) -> str:
    return (
        f"re-run {now:%Y-%m-%d} after the select_races fix (one flat IN instead of one OR per "
        "race; the original cell failed with StackOverflowError)"
    )


def merge_supplement(
    recorded: Sequence[Mapping[str, Any]],
    rerun: Sequence[Mapping[str, Any]],
    *,
    original_run_id: str,
    cells: Sequence[str],
    note: str,
) -> list[dict[str, Any]]:
    """``recorded`` plus the re-run rows of ``cells``, marked ``rerun_of = original_run_id``.

    The recorded rows are kept as they are (the failed rows stay: they are what happened). Each
    cell must have exactly one recorded row of ``original_run_id`` that is not ``ok``, no earlier
    re-run row, and exactly one row in ``rerun``; anything else is refused before writing."""
    out = [dict(r) for r in recorded]
    new: list[dict[str, Any]] = []
    for cid in cells:
        orig = [r for r in recorded if r.get("cell_id") == cid and not r.get("rerun_of")]
        if len(orig) != 1 or orig[0].get("run_id") != original_run_id:
            raise SupplementError(f"{cid}: expected one recorded row of {original_run_id}")
        if orig[0].get("status") == "ok":
            raise SupplementError(f"{cid}: the recorded row is ok; only failed cells are re-run")
        if any(r.get("cell_id") == cid and r.get("rerun_of") for r in recorded):
            raise SupplementError(f"{cid}: a re-run of this cell is already merged")
        got = [r for r in rerun if r.get("cell_id") == cid]
        if len(got) != 1:
            raise SupplementError(f"{cid}: expected one row in the re-run, found {len(got)}")
        new.append({**got[0], "rerun_of": original_run_id, "note": note})
    return out + new


# ============================================================================== plots
def _style(ax: Any) -> None:
    ax.grid(True, which="major", color="#e4e3df", linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#b5b4ae")
    ax.tick_params(colors="#52514e", labelsize=9)


def _num(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def plot_scaling(
    rows: Sequence[Mapping[str, Any]], path: Path, *, phase: str, x_col: str, x_label: str
) -> bool:
    """Median wall time (IQR bars) against ``x_col`` for ``phase``, one line per master.
    Returns False (no file) when there is nothing to plot."""
    ok = [
        r
        for r in rows
        if r["phase"] == phase
        and r["status"] == "ok"
        and _num(r.get("median_s")) is not None
        and _num(r.get(x_col)) is not None
        and r.get("master") in MASTER_ORDER
    ]
    if not ok:
        return False
    fig, ax = plt.subplots(figsize=(7.5, 4.6), dpi=150)
    for i, master in enumerate(MASTER_ORDER):
        pts = sorted(
            (float(r[x_col]), float(r["median_s"]), float(r["q1_s"]), float(r["q3_s"]))
            for r in ok
            if r["master"] == master and _num(r.get(x_col))
        )
        if not pts:
            continue
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        err = [
            [y - p[2] for y, p in zip(ys, pts, strict=True)],
            [p[3] - y for y, p in zip(ys, pts, strict=True)],
        ]
        color = SERIES_COLORS[i]
        ax.errorbar(
            xs,
            ys,
            yerr=err,
            color=color,
            linewidth=2,
            marker="o",
            markersize=6,
            markeredgecolor="white",
            markeredgewidth=1.5,
            capsize=3,
            label=MASTER_LABEL[master],
        )
        ax.annotate(
            MASTER_LABEL[master],
            (xs[-1], ys[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=8,
            color="#52514e",
        )
    if len({float(r[x_col]) for r in ok if _num(r.get(x_col))}) > 2:
        ax.set_xscale("log")
        ax.set_yscale("log")
    ax.set_xlabel(x_label, color="#52514e")
    ax.set_ylabel("median wall time per run (s), IQR bars", color="#52514e")
    title = "Feature build" if phase == "build" else "Telemetry aggregation (real 2024 samples)"
    ax.set_title(f"{title}: single host, trends only", loc="left", fontsize=11, color="#0b0b0b")
    _style(ax)
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)
    return True


SEASONS_LABEL = {1: "1 (2024)", 3: "3 (2022-24)", 5: "5 (2021-25)"}
SEASONS_TITLE = {1: "1 season (2024)", 3: "3 seasons (2022-24)", 5: "5 seasons (2021-25)"}
MUTED = "#52514e"


def _build_points(
    rows: Sequence[Mapping[str, Any]],
    master: str,
    *,
    factor: float | None = None,
    n_seasons: float | None = None,
) -> list[tuple[float, float, float, float, bool, float]]:
    """``(n_seasons, median, q1, q3, is_rerun, factor)`` of the ok build rows of ``master``,
    limited to one ``factor`` and/or one ``n_seasons`` when given."""
    pts = []
    for r in rows:
        if r.get("phase") != "build" or r.get("status") != "ok" or r.get("master") != master:
            continue
        if factor is not None and _num(r.get("factor")) != factor:
            continue
        if n_seasons is not None and _num(r.get("n_seasons")) != n_seasons:
            continue
        vals = [_num(r.get(c)) for c in ("n_seasons", "median_s", "q1_s", "q3_s", "factor")]
        if any(v is None for v in vals):
            continue
        n, med, q1, q3, f = (float(v) for v in vals)  # type: ignore[arg-type]
        pts.append((n, med, q1, q3, bool(r.get("rerun_of")), f))
    return pts


def _draw(
    ax: Any, xs: Sequence[float], pts: Sequence[tuple[float, ...]], color: str, label: str
) -> None:
    ys = [p[1] for p in pts]
    err = [
        [y - p[2] for y, p in zip(ys, pts, strict=True)],
        [p[3] - y for y, p in zip(ys, pts, strict=True)],
    ]
    ax.errorbar(
        xs,
        ys,
        yerr=err,
        color=color,
        linewidth=2,
        marker="o",
        markersize=6,
        markeredgecolor="white",
        markeredgewidth=1.5,
        capsize=3,
        label=label,
    )
    rerun = [(x, y) for x, y, p in zip(xs, ys, pts, strict=True) if p[4]]
    if rerun:  # re-run points: hollow marker over the line
        ax.plot(
            [x for x, _ in rerun],
            [y for _, y in rerun],
            linestyle="none",
            marker="o",
            markersize=9,
            markerfacecolor="white",
            markeredgecolor=color,
            markeredgewidth=2,
        )


def _missing_build_cells(
    rows: Sequence[Mapping[str, Any]],
) -> list[tuple[Mapping[str, Any], str]]:
    """Build rows without a timing (failed, aborted, skipped) that no merged ok re-run replaces,
    each with a short reason."""
    rerun_ids = {r.get("cell_id") for r in rows if r.get("rerun_of")}
    first_overflow = {
        r.get("cell_id")
        for r in rows
        if not r.get("rerun_of") and "StackOverflowError" in str(r.get("reason", ""))
    }
    out = []
    for r in rows:
        if r.get("phase") != "build" or r.get("status") == "ok":
            continue
        cell = r.get("cell_id")
        if r.get("rerun_of"):
            why = f"{r.get('status')} in the supplementary re-run"
            if cell in first_overflow:
                why += " (the first attempt failed with StackOverflowError in select_races, fixed)"
        elif cell in rerun_ids:
            continue  # superseded: described once, by its re-run row
        elif "StackOverflowError" in str(r.get("reason", "")):
            why = (
                "failed with StackOverflowError in select_races (fixed after this run; not re-run)"
            )
        else:
            why = str(r.get("status"))
        out.append((r, why))
    return out


def not_drawn(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Footnote lines for build cells without a timing, one line per reason, with the cells of
    one (seasons, factor) written together: ``build_s5_x4_{local1, local2}``."""
    by_reason: dict[str, dict[str, list[str]]] = {}
    for r, why in _missing_build_cells(rows):
        stem, _, master = str(r.get("cell_id")).rpartition("_")
        by_reason.setdefault(why, {}).setdefault(stem, []).append(master)
    lines = []
    for why, groups in by_reason.items():
        cells = [
            f"{stem}_{ms[0]}" if len(ms) == 1 else f"{stem}_{{{', '.join(ms)}}}"
            for stem, ms in groups.items()
        ]
        lines.append(f"{', '.join(cells)}: {why}")
    return lines


def plot_build_axes(rows: Sequence[Mapping[str, Any]], path: Path) -> bool:
    """The feature build on its two axes, from the same rows (no new measurement):
    (a) real seasons 1/3/5 at x1; (b)-(d) replication x1/x4/x16 for 1, 3 and 5 seasons (shared
    y). One line per master, median with IQR bars; a hollow marker is a merged re-run; cells
    without a timing are named in the footnote. Returns False when there is nothing to plot."""
    builds = [r for r in rows if r.get("phase") == "build" and r.get("status") == "ok"]
    if not builds:
        return False
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.2), dpi=150)
    ax_s, ax_r = axes[0][0], [axes[0][1], axes[1][0], axes[1][1]]
    for ax in ax_r[1:]:
        ax.sharey(ax_r[0])
    seasons = sorted({int(float(r["n_seasons"])) for r in builds})
    factors = sorted(
        {int(f) for r in rows if r.get("phase") == "build" and (f := _num(r.get("factor")))}
    )
    for i, master in enumerate(MASTER_ORDER):
        color, label = SERIES_COLORS[i], MASTER_LABEL[master]
        pts = sorted(_build_points(rows, master, factor=1.0))
        if pts:
            _draw(ax_s, [p[0] for p in pts], pts, color, label)
        for ax, n in zip(ax_r, (1, 3, 5), strict=True):
            sel = sorted(_build_points(rows, master, n_seasons=float(n)), key=lambda p: p[5])
            if sel:
                _draw(ax, [p[5] for p in sel], sel, color, label)
    for ax, n in zip(ax_r, (1, 3, 5), strict=True):
        gone = sorted(
            {
                f
                for r, _why in _missing_build_cells(rows)
                if _num(r.get("n_seasons")) == n and (f := _num(r.get("factor"))) is not None
            }
        )
        for f in gone:  # where a timing is missing, say so in the panel (named in the footnote)
            ax.axvline(f, color="#d0cfca", linewidth=1, linestyle=":", zorder=0)
            ax.annotate(
                "no timing\n(see note)",
                (f, 0.04),
                xycoords=("data", "axes fraction"),
                ha="center",
                fontsize=7,
                color=MUTED,
            )
    ax_s.set_xticks(seasons, [SEASONS_LABEL.get(n, str(n)) for n in seasons])
    ax_s.set_xlabel("real seasons (replication x1)", color="#52514e")
    ax_s.set_title("(a) Real seasons at x1", loc="left", fontsize=10, color="#0b0b0b")
    for ax, n in zip(ax_r, (1, 3, 5), strict=True):
        ax.set_xscale("log", base=2)
        own = (
            sorted(
                {
                    int(f)
                    for r in rows
                    if r.get("phase") == "build"
                    and _num(r.get("n_seasons")) == n
                    and (f := _num(r.get("factor")))
                }
            )
            or factors
        )  # this season count's own factors (failed cells included)
        ax.set_xticks(own, [f"x{f}" for f in own])
        ax.minorticks_off()
        ax.set_xlabel("replication factor (synthetic copies)", color="#52514e")
        ax.set_title(
            f"({'bcd'[(1, 3, 5).index(n)]}) Replication at {SEASONS_TITLE[n]}",
            loc="left",
            fontsize=10,
            color="#0b0b0b",
        )
    for ax in (ax_s, *ax_r):
        ax.set_ylabel("median wall time per run (s), IQR bars", color="#52514e")
        _style(ax)
    # the legend goes on panel (a), or on the first panel with lines when (a) has none
    with_lines = [a for a in (ax_s, *ax_r) if a.get_legend_handles_labels()[0]]
    if with_lines:
        with_lines[0].legend(frameon=False, fontsize=8, loc="upper left")
    notes = not_drawn(rows)
    if any(r.get("rerun_of") for r in builds):
        notes.append(
            "hollow marker: supplementary re-run after the select_races fix (rerun_of, note)"
        )
    fig.suptitle(
        "Feature build: real seasons and replication, single host, trends only",
        x=0.01,
        ha="left",
        fontsize=11,
        color="#0b0b0b",
    )
    if notes:
        fig.text(0.01, 0.01, "Note: " + "\n".join(notes), fontsize=7.5, color=MUTED, va="bottom")
    fig.tight_layout(rect=(0, 0.02 + 0.022 * len(notes), 1, 0.97))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.stem + ".tmp.png")
    fig.savefig(tmp)
    plt.close(fig)
    tmp.replace(path)
    return True


def plot_partitioning(rows: Sequence[Mapping[str, Any]], path: Path) -> bool:
    """Speed-up against the simplest baseline, one panel per (dataset, query), bars per
    (file system, layout), colour = caching variant."""
    ok = [r for r in rows if _num(r.get("speedup_vs_simplest"))]
    if not ok:
        return False
    panels = sorted({(str(r["dataset"]), str(r["query"])) for r in ok})
    variants = ("none", "cache", "persist_disk")
    combos = [(fs, lay) for fs in ("file", "hdfs") for lay in ("unpartitioned", "partitioned")]
    ncols = 2
    nrows = (len(panels) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(10, 2.6 * nrows), dpi=150, squeeze=False)
    width = 0.26
    for idx, (dataset, query) in enumerate(panels):
        ax = axes[idx // ncols][idx % ncols]
        for vi, variant in enumerate(variants):
            ys = []
            for fs, lay in combos:
                match = [
                    r
                    for r in ok
                    if (r["dataset"], r["query"], r["fs"], r["layout"], r["variant"])
                    == (dataset, query, fs, lay, variant)
                ]
                ys.append(float(match[0]["speedup_vs_simplest"]) if match else 0.0)
            xs = [c + (vi - 1) * width for c in range(len(combos))]
            ax.bar(xs, ys, width=width - 0.02, color=SERIES_COLORS[vi], label=variant)
        ax.axhline(1.0, color="#52514e", linewidth=1, linestyle="--")
        ax.set_xticks(range(len(combos)))
        ax.set_xticklabels([f"{fs}\n{lay}" for fs, lay in combos], fontsize=8)
        ax.set_title(f"{dataset} / {query}", loc="left", fontsize=10, color="#0b0b0b")
        ax.set_ylabel("speed-up", fontsize=8, color="#52514e")
        _style(ax)
    for idx in range(len(panels), nrows * ncols):
        axes[idx // ncols][idx % ncols].set_visible(False)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", frameon=False, fontsize=8, ncols=3)
    fig.suptitle(
        "Speed-up vs file:// unpartitioned, no cache (dashed = 1x); single host, trends only",
        x=0.01,
        ha="left",
        fontsize=11,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)
    return True


# ============================================================================== fingerprint
def _run(cmd: Sequence[str]) -> str:
    try:
        out = subprocess.run(list(cmd), capture_output=True, text=True, timeout=60, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"
    return out.stdout.strip() if out.returncode == 0 and out.stdout.strip() else "unavailable"


def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unavailable"


def _mem_total() -> str:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return f"{int(line.split()[1]) / 1024 / 1024:.1f} GiB"
    except (OSError, ValueError):
        pass
    return "unavailable"


def container_limits(run: Callable[[Sequence[str]], str] = _run) -> str:
    """``service=limit`` for the containers the benchmark uses."""
    parts: list[str] = []
    for svc in ("app", "spark-master", "spark-worker-1", "spark-worker-2", "namenode", "datanode"):
        cid = run(["docker", "compose", "ps", "-q", svc])
        if cid == "unavailable":
            parts.append(f"{svc}=unavailable")
            continue
        mem = run(["docker", "inspect", "-f", "{{.HostConfig.Memory}}", cid.splitlines()[0]])
        parts.append(f"{svc}={mem}")
    return ", ".join(parts)


def fingerprint(
    *,
    now: datetime,
    git_sha: str,
    git_dirty: str,
    params: Mapping[str, Any],
    run: Callable[[Sequence[str]], str] = _run,
) -> list[tuple[str, str]]:
    """``(name, value)`` pairs describing where the numbers were measured."""
    import pyspark

    return [
        ("measured_at_utc", now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("git_sha", git_sha),
        ("git_dirty", git_dirty),
        ("python", platform.python_version()),
        ("pyspark", pyspark.__version__),
        (
            "java_in_app",
            run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "app",
                    "sh",
                    "-c",
                    "java -version 2>&1 | head -1",
                ]
            ),
        ),
        ("os", f"{platform.system()} {platform.release()}"),
        ("cpu", f"{_cpu_model()} ({os.cpu_count()} logical CPUs)"),
        ("memory", _mem_total()),
        ("container_mem_limits_bytes", container_limits(run)),
        (
            "topology",
            "one host: app container (driver / local[N]); spark-master + 2 workers "
            "(2 cores, 1g each); 1 namenode + 1 datanode, replication 1",
        ),
        ("params", ", ".join(f"{k}={v}" for k, v in params.items())),
    ]


def append_readme(path: Path, csv_name: str, pairs: Sequence[tuple[str, str]]) -> None:
    """Append a fingerprint section to ``results/README.md`` (same format as Step 11)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", encoding="utf-8") as fh:
        if new:
            fh.write(
                "# Results\n\nEnvironment fingerprints of the experiments that wrote files here.\n"
            )
        fh.write(f"\n## `{csv_name}` ({pairs[0][1]})\n\n| Item | Value |\n|---|---|\n")
        for k, v in pairs:
            fh.write(f"| {k} | {v.replace('|', '/')} |\n")


# ============================================================================== replot
def main(argv: Sequence[str] | None = None) -> int:
    """``python -m f1sa.bench.report --replot <scalability.csv> [--out <png>]``: redraw the build
    plot from a recorded CSV (no measurement). Default output: ``scalability_build.png`` next to
    the CSV; only that file is written."""
    import argparse

    p = argparse.ArgumentParser(prog="f1sa-bench-report", description=main.__doc__)
    p.add_argument("--replot", type=Path, required=True, help="a scalability.csv")
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args(list(argv) if argv is not None else None)
    rows = read_csv_rows(args.replot)
    if not rows:
        print(f"replot: no rows in {args.replot}")
        return 2
    out = args.out or args.replot.with_name("scalability_build.png")
    if out.suffix != ".png":
        print(f"replot: {out} is not a .png")
        return 2
    if not plot_build_axes(rows, out):
        print("replot: no ok build rows to plot")
        return 2
    print(f"replot: wrote {out} from {len(rows)} rows of {args.replot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
