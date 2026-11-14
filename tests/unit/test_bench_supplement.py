"""Step 10 follow-up: re-running selected failed cells of a recorded grid (``--only-cells``,
``--supplement-of``), merging them into the recorded CSV, and the two-axis build plot.
No test writes the repo's results/: recorded runs use a tmp repo."""

from __future__ import annotations

import csv
import json
from collections.abc import Sequence
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pytest
from test_bench_report import NOW, REPO_SRC, fake_cell_result, fake_host  # noqa: F401

from f1sa.bench import grid as G
from f1sa.bench import host as H
from f1sa.bench import report as R
from f1sa.bench.timing import MemoryGuard

ORIG = "bench_20261113T152740Z"
GRID = ["--seasons", "1", "--factors", "1", "4", "--masters", "local1", "local2"]
SOE = "Py4JJavaError: An error occurred while calling o81.filter.\n: java.lang.StackOverflowError"


class Container:
    """The grid in-process with instant fake cells; ``fail`` cells end ``failed`` (as the
    StackOverflowError cells did). Records every cell it runs."""

    def __init__(self, repo: Path, fail: Sequence[str] = ()) -> None:
        self.repo, self.fail = repo, set(fail)
        self.cmds: list[list[str]] = []
        self.ran: list[str] = []

    def __call__(self, cmd: Sequence[str]) -> int:
        self.cmds.append(list(cmd))
        i = cmd.index("f1sa.bench.grid")
        args = G.build_parser().parse_args(
            [a.replace("/workspace", str(self.repo), 1) for a in cmd[i + 1 :]]
        )

        def cell(_argv: Sequence[str], result_path: Path, _guard: Any) -> G.CellOutcome:
            cid = result_path.name.removesuffix(".result.json")
            self.ran.append(cid)
            if cid in self.fail:
                return G.CellOutcome("failed", SOE, None, {}, 1.0, 2.0)
            return G.CellOutcome("ok", "", fake_cell_result(cid), {}, 1.0, 2.0)

        G.run_grid(
            G.spec_from_args(args),
            args.run_dir,
            args.run_dir.name,
            guard=MemoryGuard(64 * 2**30, host_reserve_bytes=0),
            src=REPO_SRC,
            runner=cell,
            resume=args.resume,
            out=lambda _m: None,
            git_sha="fixsha",
            git_dirty="false",
            docker_wait_s=0.0,
        )
        return 0


def _results(repo: Path) -> list[dict[str, str]]:
    return list(csv.DictReader((repo / "results" / "scalability.csv").open()))


@pytest.fixture
def recorded(fake_host: Path) -> Path:  # noqa: F811
    """A recorded grid whose ``build_s1_x4_local2`` failed (StackOverflowError)."""
    code = H.main_scalability(
        ["--record", "--run-id", ORIG, "--keep-data", "--repo", str(fake_host), *GRID],
        runner=Container(fake_host, fail=["build_s1_x4_local2"]),
        out=lambda _m: None,
        now=NOW,
    )
    assert code == 0
    assert {r["cell_id"]: r["status"] for r in _results(fake_host)}[
        "build_s1_x4_local2"
    ] == "failed"
    return fake_host


def _supp(repo: Path, *extra: str, runner: Any = None, lines: list[str] | None = None) -> int:
    return H.main_scalability(
        ["--supplement-of", ORIG, "--keep-data", "--repo", str(repo), *extra],
        runner=runner or Container(repo),
        out=(lines if lines is not None else []).append,
        now=NOW,
    )


# ------------------------------------------------------------------ grid filter
def test_only_cells_keeps_the_cells_and_their_prep_with_the_grid_settings() -> None:
    spec = G.GridSpec(
        seasons=(1, 5),
        factors=(1, 4, 16),
        repeats=3,
        large_repeats=3,
        large_masters=("local4", "cluster"),
        large_seasons=(1,),
    )
    only = ("build_s1_x16_local4", "build_s1_x16_cluster", "build_s5_x4_local1")
    sel = replace(spec, only=only)
    cells = [(c.cell_id, c.warmup, c.repeats) for c in G.cells(sel)]
    assert cells == [
        ("prep_s1_x16_local4", 0, 1),
        ("build_s1_x16_local4", 1, 3),
        ("build_s1_x16_cluster", 1, 3),
        ("prep_s5_x4_local4", 0, 1),
        ("build_s5_x4_local1", 1, 3),
    ]
    assert sel.cli_args()[-4:] == ["--only-cells", *only]
    args = G.build_parser().parse_args(["--run-dir", "x", *sel.cli_args()])
    assert G.spec_from_args(args) == sel
    assert G.spec_from_json(json.loads(json.dumps(asdict(sel)))) == sel
    full = [c.cell_id for c in G.cells(spec)]
    assert len(full) == 31 and full[:3] == [
        "prep_s1_x1_local4",
        "build_s1_x1_local1",
        "build_s1_x1_local2",
    ]


@pytest.mark.parametrize(
    "only", [(), ("build_s3_x1_local1",), ("prep_s1_x1_local4",), ("build_s1_x16_local1",)]
)
def test_only_cells_outside_the_grid_are_refused(only: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        G.GridSpec(seasons=(1,), factors=(1, 16), large_masters=("local4",), only=only)


# ------------------------------------------------------------------ merge
def _row(cid: str, status: str, run: str = ORIG, **kw: str) -> dict[str, str]:
    return {"cell_id": cid, "status": status, "run_id": run, **kw}


def test_merge_appends_marked_rows_and_keeps_the_failed_ones() -> None:
    recorded = [_row("a", "ok"), _row("b", "failed")]
    merged = R.merge_supplement(
        recorded,
        [_row("prep", "ok", "s"), _row("b", "ok", "s")],
        original_run_id=ORIG,
        cells=["b"],
        note="n",
    )
    assert merged[:2] == recorded
    assert merged[2] == {**_row("b", "ok", "s"), "rerun_of": ORIG, "note": "n"}
    assert len(merged) == 3, "prep rows are not merged"


@pytest.mark.parametrize(
    ("recorded", "rerun", "match"),
    [
        ([_row("b", "ok")], [_row("b", "ok", "s")], "is ok"),
        ([_row("b", "failed", "other")], [_row("b", "ok", "s")], "expected one recorded row"),
        ([], [_row("b", "ok", "s")], "expected one recorded row"),
        (
            [_row("b", "failed"), _row("b", "ok", "s", rerun_of=ORIG)],
            [_row("b", "ok", "t")],
            "already merged",
        ),
        ([_row("b", "failed")], [], "found 0"),
    ],
)
def test_merge_refusals(
    recorded: list[dict[str, str]], rerun: list[dict[str, str]], match: str
) -> None:
    with pytest.raises(R.SupplementError, match=match):
        R.merge_supplement(recorded, rerun, original_run_id=ORIG, cells=["b"], note="n")


# ------------------------------------------------------------------ host
def test_supplement_without_record_leaves_results_alone(recorded: Path) -> None:
    before = (recorded / "results" / "scalability.csv").read_bytes()
    pngs = {p.name: p.read_bytes() for p in (recorded / "results").glob("*.png")}
    box = Container(recorded)
    lines: list[str] = []
    assert _supp(recorded, "--only-cells", "build_s1_x4_local2", runner=box, lines=lines) == 0
    assert box.ran == ["prep_s1_x4_local4", "build_s1_x4_local2"]
    assert (recorded / "results" / "scalability.csv").read_bytes() == before
    assert {p.name: p.read_bytes() for p in (recorded / "results").glob("*.png")} == pngs
    rid = "bench_20261113T220000Z_supp"
    rows = list(csv.DictReader((recorded / "data" / "bench" / rid / "scalability.csv").open()))
    assert [r["cell_id"] for r in rows] == ["prep_s1_x4_local4", "build_s1_x4_local2"]
    spec = G.spec_from_json(
        json.loads((recorded / "data" / "bench" / rid / G.SPEC_NAME).read_text())
    )
    assert spec.only == ("build_s1_x4_local2",) and spec.repeats == 5, (
        "the original grid's settings"
    )
    assert lines[1] == (
        "scalability: to resume this run: make bench-supplement "
        f'SUPPLEMENT_OF={ORIG} CELLS="build_s1_x4_local2" RESUME={rid}'
    )


def test_recorded_supplement_merges_and_marks_the_rows(recorded: Path) -> None:
    lines: list[str] = []
    assert _supp(recorded, "--record", "--only-cells", "build_s1_x4_local2", lines=lines) == 0
    rows = _results(recorded)
    assert list(rows[0]) == list(R.SUPPLEMENT_COLUMNS)
    same = [r for r in rows if r["cell_id"] == "build_s1_x4_local2"]
    assert [(r["run_id"], r["status"], r["rerun_of"]) for r in same] == [
        (ORIG, "failed", ""),
        ("bench_20261113T220000Z_supp", "ok", ORIG),
    ]
    assert "select_races fix" in same[1]["note"] and same[1]["git_sha"] == "fixsha"
    assert all(r["rerun_of"] == "" for r in rows if r is not same[1])
    readme = (recorded / "results" / "README.md").read_text()
    assert "run_id=bench_20261113T220000Z_supp," in readme and f"supplement_of={ORIG}" in readme
    assert (recorded / "results" / "scalability_build.png").exists()
    assert "merged 1 re-run row(s)" in lines[-1]
    # a second merge of the same cell is refused before anything runs
    box = Container(recorded)
    again: list[str] = []
    assert (
        _supp(
            recorded,
            "--record",
            "--run-id",
            "bench_x_supp",
            "--only-cells",
            "build_s1_x4_local2",
            runner=box,
            lines=again,
        )
        == 2
    )
    assert "already merged" in again[-1] and box.cmds == []


@pytest.mark.parametrize(
    ("extra", "match"),
    [
        (["--record", "--only-cells", "build_s1_x4_local1"], "is ok"),
        (["--only-cells", "build_s1_x4_local2", "--factors", "1"], "drop GRID_ARGS"),
        ([], "needs --only-cells"),
        (["--only-cells", "build_s3_x1_local1"], "not in this grid"),
    ],
)
def test_supplement_refusals(recorded: Path, extra: list[str], match: str) -> None:
    box = Container(recorded)
    lines: list[str] = []
    assert _supp(recorded, *extra, runner=box, lines=lines) == 2
    assert match in lines[-1] and box.cmds == []


def test_supplement_of_an_unknown_run_or_a_mismatched_resume_is_refused(recorded: Path) -> None:
    lines: list[str] = []
    code = H.main_scalability(
        [
            "--supplement-of",
            "bench_nope",
            "--only-cells",
            "build_s1_x4_local2",
            "--repo",
            str(recorded),
        ],
        runner=Container(recorded),
        out=lines.append,
    )
    assert code == 2 and "original run not found" in lines[-1]
    rid = "bench_s_supp"
    assert _supp(recorded, "--run-id", rid, "--only-cells", "build_s1_x4_local2") == 0
    assert (
        _supp(
            recorded, "--resume", "--run-id", rid, "--only-cells", "build_s1_x1_local2", lines=lines
        )
        == 2
    )
    assert "use the same CELLS" in lines[-1]
    assert _supp(recorded, "--resume", "--run-id", rid, "--only-cells", "build_s1_x4_local2") == 0


# ------------------------------------------------------------------ plot
def _build(
    cid: str, n: int, f: int, master: str, med: float, status: str = "ok", **kw: str
) -> dict[str, Any]:
    return {
        "cell_id": cid,
        "phase": "build",
        "status": status,
        "master": master,
        "n_seasons": str(n),
        "factor": str(f),
        "median_s": str(med) if status == "ok" else "",
        "q1_s": str(med - 1),
        "q3_s": str(med + 1),
        **kw,
    }


ROWS = [
    _build("build_s1_x1_local4", 1, 1, "local4", 48.0),
    _build("build_s3_x1_local4", 3, 1, "local4", 49.0),
    _build("build_s5_x1_local4", 5, 1, "local4", 66.0),
    _build("build_s1_x4_local4", 1, 4, "local4", 65.0),
    _build("build_s1_x16_local4", 1, 16, "local4", 0.0, "failed", reason=SOE),
    _build("build_s5_x4_local1", 5, 4, "local1", 0.0, "failed", reason=SOE),
    _build("build_s5_x4_local2", 5, 4, "local2", 0.0, "failed", reason=SOE),
    {"cell_id": "prep_s1_x1_local4", "phase": "prep", "status": "ok"},
]


def test_not_drawn_groups_cells_by_reason() -> None:
    lines = R.not_drawn(ROWS)
    assert len(lines) == 1
    assert lines[0].startswith("build_s1_x16_local4, build_s5_x4_{local1, local2}: failed with ")
    rerun = [*ROWS, _build("build_s1_x16_local4", 1, 16, "local4", 280.0, rerun_of=ORIG)]
    assert R.not_drawn(rerun)[0].startswith("build_s5_x4_{local1, local2}:")
    again = [
        *ROWS,
        _build("build_s5_x4_local1", 5, 4, "local1", 0.0, "aborted_memory_guard", rerun_of=ORIG),
    ]
    assert any("again in the supplementary re-run" in ln for ln in R.not_drawn(again))


def test_build_plot_and_replot_write_only_the_png(tmp_path: Path) -> None:
    assert R.plot_build_axes(ROWS, tmp_path / "a.png")
    assert (tmp_path / "a.png").read_bytes()[:4] == b"\x89PNG"
    assert not R.plot_build_axes([r for r in ROWS if r["status"] != "ok"], tmp_path / "b.png")
    rerun = [*ROWS, _build("build_s1_x16_local4", 1, 16, "local4", 280.0, rerun_of=ORIG)]
    assert R.plot_build_axes(rerun, tmp_path / "c.png")
    d = tmp_path / "res"
    d.mkdir()
    R.write_csv(d / "scalability.csv", ROWS, R.SUPPLEMENT_COLUMNS)
    before = (d / "scalability.csv").read_bytes()
    assert R.main(["--replot", str(d / "scalability.csv")]) == 0
    assert sorted(p.name for p in d.iterdir()) == ["scalability.csv", "scalability_build.png"]
    assert (d / "scalability.csv").read_bytes() == before
    assert R.main(["--replot", str(d / "scalability.csv"), "--out", str(d / "x.jpg")]) == 2
    (tmp_path / "empty.csv").write_text("cell_id\n")
    assert R.main(["--replot", str(tmp_path / "empty.csv")]) == 2
