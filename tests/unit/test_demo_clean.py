"""``make demo-clean`` (``python -m f1sa.demo_runs``): deletes only demo-run leftovers in
./data/demo and ./data/checkpoints, refuses every other path, needs ``--yes``."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from f1sa.demo_runs import CleanRefusedError, main, plan_clean

REPO = Path(__file__).resolve().parents[2]
RUNS = ("2024_5_20261112T183005.042Z", "2023_10_20261112T183040.742Z")
CKPTS = (
    "api_2023_10_20261112T183040.742Z",
    "api_2023_10_20261112T183040.742Z.ready",
    "demo_2024_5_1794487657",
    "demo_2024_5_1794487657.ready",
)
KEPT_CKPTS = ("stream_job", "api_notarun", "demo_2024_5_x", "mine.txt")


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """A working directory (tests run in tmp_path) with demo leftovers and things to keep."""
    demo, ck = tmp_path / "data" / "demo", tmp_path / "data" / "checkpoints"
    for run in RUNS:
        (demo / run).mkdir(parents=True)
        (demo / run / "stream_batches.jsonl").write_text("x" * 10)
    (demo / "notes.txt").write_text("keep me")
    for name in CKPTS + KEPT_CKPTS:
        path = ck / name
        if name.endswith((".ready", ".txt")):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("q")
        else:
            (path / "spark").mkdir(parents=True)
            (path / "spark" / "metadata").write_text("{}")
    (tmp_path / "data" / "manifest.json").write_text("{}")
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "streaming_latency.csv").write_text("committed")
    return tmp_path


def _all_files(root: Path) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob("*")}


def test_lists_without_confirm_and_deletes_nothing(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    before = _all_files(tree)
    assert main([]) == 1
    out = capsys.readouterr().out
    assert _all_files(tree) == before
    for name in RUNS:
        assert f"would delete  data/demo/{name}" in out
    for name in CKPTS:
        assert f"would delete  data/checkpoints/{name}" in out
    assert "make demo-clean CONFIRM=yes" in out


def test_deletes_only_demo_leftovers_and_prints_them(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["--yes"]) == 0
    out = capsys.readouterr().out
    assert sorted(p.name for p in (tree / "data" / "demo").iterdir()) == ["notes.txt"]
    assert sorted(p.name for p in (tree / "data" / "checkpoints").iterdir()) == sorted(KEPT_CKPTS)
    assert (tree / "data" / "manifest.json").read_text() == "{}"
    assert (tree / "results" / "streaming_latency.csv").read_text() == "committed"
    assert f"deleted data/demo/{RUNS[0]}  (10 bytes)" in out
    assert f"deleted {len(RUNS) + len(CKPTS)} entries" in out
    assert "keep    data/checkpoints/stream_job  (not created by a demo run)" in out
    assert main(["--yes"]) == 0
    assert "nothing to delete" in capsys.readouterr().out


@pytest.mark.parametrize(
    "args",
    [
        ["--demo-dir", "results"],
        ["--demo-dir", "data"],
        ["--demo-dir", "data/lake"],
        ["--demo-dir", "data/demo/../lake"],
        ["--demo-dir", "/tmp"],
        ["--demo-dir", "."],
        ["--checkpoint-root", "data"],
        ["--checkpoint-root", "results"],
        ["--checkpoint-root", "data/checkpoints/stream_job"],
        ["--demo-dir", "data/checkpoints"],
    ],
)
def test_refuses_any_other_path(
    tree: Path, args: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    before = _all_files(tree)
    assert main([*args, "--yes"]) == 2
    assert "refusing" in capsys.readouterr().err
    assert _all_files(tree) == before


def test_refuses_demo_dir_setting_outside(tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEMO_DIR", "results")
    assert main(["--yes"]) == 2
    assert (tree / "results" / "streaming_latency.csv").exists()


def test_refuses_a_symlinked_root(tree: Path) -> None:
    outside = tree / "elsewhere"
    outside.mkdir()
    (outside / RUNS[0]).mkdir()
    shutil.rmtree(tree / "data" / "demo")
    os.symlink(outside, tree / "data" / "demo")
    with pytest.raises(CleanRefusedError, match="symlink"):
        plan_clean()
    assert main(["--yes"]) == 2
    assert (outside / RUNS[0]).is_dir()


def test_never_follows_a_symlinked_entry(tree: Path) -> None:
    precious = tree / "precious"
    precious.mkdir()
    (precious / "keep").write_text("x")
    link = tree / "data" / "demo" / "2024_6_20261112T190000.000Z"
    os.symlink(precious, link)
    plan = plan_clean()
    assert (link, "symlink") in plan.skipped and link not in plan.delete
    assert main(["--yes"]) == 0
    assert (precious / "keep").read_text() == "x" and link.is_symlink()


def test_missing_roots_are_fine(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--yes"]) == 0
    assert "nothing to delete" in capsys.readouterr().out


def test_make_target_needs_confirm() -> None:
    if shutil.which("make") is None:
        pytest.skip("make not installed")

    def dry(*args: str) -> str:
        return subprocess.run(
            ["make", "-n", "--no-print-directory", "-C", str(REPO), "demo-clean", *args],
            capture_output=True,
            text=True,
            check=True,
        ).stdout

    assert dry().strip().endswith("python -m f1sa.demo_runs")
    assert dry("CONFIRM=yes").strip().endswith("python -m f1sa.demo_runs --yes")
    assert "--yes" not in dry("CONFIRM=1")
