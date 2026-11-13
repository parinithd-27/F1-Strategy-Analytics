"""The code that ran while the 9f ``real`` results were lost (2026-11-13 04:44-05:30 UTC) never
deletes or overwrites a pre-existing file in ``results/``.

The loss itself was a ``git clean -f -q -- results/...`` run by the editor's git integration (see
docs/PROGRESS.md), not repo code. These tests pin that down for the repo code that ran in that
window, against a temporary copy of the results folder:

* the six ``--allow-test-season`` runs (``make model-test-full``): refused, nothing touched;
* the ``real_tyre`` / ``real_ti`` development runs (``make model-dev-tyre-full model-dev-ti-full``):
  they write their own declared files and nothing else;
* the make recipes: none deletes, moves or cleans anything.

The temporary copy mirrors the NAMES of the repo's ``results/`` with placeholder bytes; the real
files are never opened.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from _model_fixtures import SMALL
from pyspark.sql import SparkSession

from _results_guard import describe_changes, results_changes, results_snapshot
from conftest import REPO_RESULTS, SampleLake
from f1sa.model import features as MF
from f1sa.model import pipeline as P

REPO_ROOT = REPO_RESULTS.parent
SETTINGS = ((3.5, 2), (5.0, 3))
TEST_RUNS = [
    P.RunConfig(source="full", allow_test_season=True, feature_set=fs, g=g, k=k)
    for fs in MF.TEST_FEATURE_SETS
    for g, k in SETTINGS
]


def _placeholder(name: str) -> bytes:
    return f"placeholder {name}\n".encode()


def _contents(root: Path) -> dict[str, bytes]:
    """Bytes of every file under ``root/results`` and ``root/models`` (keys relative to root)."""
    return {
        p.relative_to(root).as_posix(): p.read_bytes()
        for sub in ("results", "models")
        for p in (root / sub).rglob("*")
        if p.is_file()
    }


@pytest.fixture
def results_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A working directory whose ``results/`` holds one placeholder per file name in the repo's
    ``results/`` plus every 9f test-run file, and whose ``models/`` holds each test run's
    ``metadata.json``. Returns the working directory (already the cwd)."""
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()
    names = set(os.listdir(REPO_RESULTS)) if REPO_RESULTS.is_dir() else set()
    names |= {p.name for cfg in TEST_RUNS for p in cfg.result_paths() if p.parts[0] == "results"}
    for name in sorted(names):
        if not (REPO_RESULTS / name).is_dir():
            (results / name).write_bytes(_placeholder(name))
    for cfg in TEST_RUNS:
        meta = cfg.model_subdir / "metadata.json"
        meta.parent.mkdir(parents=True, exist_ok=True)
        meta.write_bytes(_placeholder(str(meta)))
    return tmp_path


def test_results_copy_holds_every_test_run_file(results_copy: Path) -> None:
    for cfg in TEST_RUNS:
        assert all((results_copy / p).is_file() for p in cfg.result_paths())


def test_test_mode_runs_refuse_and_touch_nothing(
    results_copy: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    before_bytes = _contents(results_copy)
    before = results_snapshot(results_copy / "results")
    for cfg in TEST_RUNS:
        argv = ["--source", "full", "--allow-test-season", "--g", str(cfg.g), "--k", str(cfg.k)]
        if cfg.feature_set != "real":
            argv += ["--feature-set", cfg.feature_set]
        assert P.main(argv) == 2, argv
        assert "never overwritten" in capsys.readouterr().err
    assert results_changes(before, results_snapshot(results_copy / "results")) == {
        "deleted": [],
        "changed": [],
        "added": [],
    }
    assert _contents(results_copy) == before_bytes


@pytest.mark.parametrize("feature_set", ["real_tyre", "real_ti"])
def test_dev_runs_write_only_their_own_files(
    spark: SparkSession,
    sample_clean_lake: SampleLake,
    sample_features_root: str,
    results_copy: Path,
    monkeypatch: pytest.MonkeyPatch,
    feature_set: str,
) -> None:
    """``make model-dev-tyre-full model-dev-ti-full`` (``--source full --no-test``) on a copy of the
    sample lake: the run rewrites exactly its own ``result_paths()``; every other file keeps its
    bytes and inode, the 9f test-run files included."""
    lake = results_copy / "data" / "lake"
    shutil.copytree(Path(sample_features_root.removeprefix("file://")), lake / "features")
    shutil.copytree(sample_clean_lake.lake / "clean" / "laps", lake / "clean" / "laps")
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{lake}")
    from f1sa.config import get_settings

    get_settings.cache_clear()
    cfg = P.RunConfig(source="full", no_test=True, feature_set=feature_set, n_boot=10, train=SMALL)
    own = {p.as_posix() for p in cfg.result_paths()}
    before_bytes = {k: v for k, v in _contents(results_copy).items() if k not in own}
    before = results_snapshot(results_copy / "results")

    P.run(spark, cfg, out=lambda _m: None)

    after_bytes = _contents(results_copy)
    assert {k: after_bytes.get(k) for k in before_bytes} == before_bytes
    changes = results_changes(before, results_snapshot(results_copy / "results"))
    assert not changes["deleted"]
    touched = {f"results/{n}" for n in changes["changed"] + changes["added"]}
    assert touched and touched <= own, sorted(touched - own)
    for test_cfg in TEST_RUNS:  # the 9f test-run files and their metadata, still the placeholders
        for p in test_cfg.result_paths():
            want = _placeholder(p.name if p.parts[0] == "results" else str(p))
            assert after_bytes[p.as_posix()] == want, p


def test_results_snapshot_detects_delete_overwrite_and_add(tmp_path: Path) -> None:
    root = tmp_path / "results"
    assert results_snapshot(root) == {}
    (root / "sub").mkdir(parents=True)
    for name in ("keep.csv", "gone.csv", "inplace.csv", "replaced.csv", "sub/nested.png"):
        (root / name).write_text("v1")
    before = results_snapshot(root)
    assert sorted(before) == [
        "gone.csv",
        "inplace.csv",
        "keep.csv",
        "replaced.csv",
        "sub/nested.png",
    ]
    assert results_changes(before, results_snapshot(root)) == {
        "deleted": [],
        "changed": [],
        "added": [],
    }
    assert describe_changes(results_changes(before, before)) == ""

    (root / "gone.csv").unlink()
    with (root / "inplace.csv").open("a") as fh:
        fh.write("more")
    tmp = root / "replaced.csv.tmp"
    tmp.write_text("v1")
    tmp.replace(root / "replaced.csv")  # same bytes, new inode: an overwrite all the same
    (root / "new.csv").write_text("x")
    changes = results_changes(before, results_snapshot(root))
    assert changes == {
        "deleted": ["gone.csv"],
        "changed": ["inplace.csv", "replaced.csv"],
        "added": ["new.csv"],
    }
    text = describe_changes(changes)
    assert "deleted: results/gone.csv" in text and "added: results/new.csv" in text
    assert "v1" not in text  # names only, never contents


def test_results_snapshot_never_reads_file_contents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "results"
    root.mkdir()
    (root / "model_eval_G3.5_k2.csv").write_text("protected")

    def refuse(*_a: object, **_k: object) -> None:
        raise AssertionError("the guard opened a results file")

    monkeypatch.setattr("builtins.open", refuse)
    monkeypatch.setattr(Path, "read_bytes", refuse)
    monkeypatch.setattr(Path, "read_text", refuse)
    assert list(results_snapshot(root)) == ["model_eval_G3.5_k2.csv"]


# ------------------------------------------------------------------ make targets
DESTRUCTIVE = re.compile(
    r"(?<![\w-])(rm|mv|unlink|rmdir|shred|truncate)\s"
    r"|git\s+(clean|checkout|stash|restore|reset|rm)\b"
    r"|-delete\b|rmtree|os\.remove|>\s*results/"
)
WINDOW_TARGETS = [
    ("model-test-full", "CONFIRM=yes"),
    ("model-test-full", "CONFIRM=yes", "FEATURE_SET=real_tyre"),
    ("model-test-full", "CONFIRM=yes", "FEATURE_SET=real_ti"),
    ("model-dev-full",),
    ("model-dev-tyre-full",),
    ("model-dev-ti-full",),
    ("model-dev-permutation-ti-full",),
    ("demo-clean", "CONFIRM=yes"),
    ("stream-demo",),
    ("stream-demo", "FULL=1"),
]


def _dry(*args: str) -> str:
    return subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout


def test_no_makefile_recipe_deletes_moves_or_cleans() -> None:
    recipes = [x for x in (REPO_ROOT / "Makefile").read_text().splitlines() if x.startswith("\t")]
    assert recipes
    assert [x for x in recipes if DESTRUCTIVE.search(x)] == []


@pytest.mark.parametrize("args", WINDOW_TARGETS, ids=" ".join)
def test_window_targets_never_delete_or_move(args: tuple[str, ...]) -> None:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    out = _dry(*args)
    assert out.strip()
    assert [x for x in out.splitlines() if DESTRUCTIVE.search(x)] == []


def test_only_a_record_run_of_stream_demo_targets_results() -> None:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    assert "--record" not in _dry("stream-demo") and "--results-dir" not in _dry("stream-demo")
    assert "--record" in _dry("stream-demo", "RECORD=1")
