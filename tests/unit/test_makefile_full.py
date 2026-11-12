"""The full-data make targets run in HDFS mode inside compose and stay away from raw and volumes."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGETS = (
    "hdfs-dirs",
    "etl-full",
    "features-full",
    "report-full",
    "pipeline-full",
    "model-dev-full",
    "model-dev-ti-full",
    "model-dev-tyre-full",
    "model-dev-permutation-ti-full",
    "model-save-full",
)


def _dry_run(target: str) -> str:
    result = subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO_ROOT), target],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return result.stdout


@pytest.mark.parametrize("target", TARGETS)
def test_target_never_rebuilds_deletes_or_touches_raw(target: str) -> None:
    out = _dry_run(target)
    assert out.strip()
    for forbidden in ("--build", " down", "prune", " -rm", " rm ", "/f1/raw", "manifest.json"):
        assert forbidden not in out, (target, forbidden)


@pytest.mark.parametrize(
    ("target", "module"),
    [
        ("etl-full", "f1sa.etl.clean --source lake"),
        ("features-full", "f1sa.features.build --source lake --full-counts"),
        ("report-full", "f1sa.etl.full_report"),
    ],
)
def test_target_runs_in_app_against_hdfs(target: str, module: str) -> None:
    line = next(x for x in _dry_run(target).splitlines() if "python -m" in x)
    assert "exec -T" in line
    assert "PYTHONPATH=/workspace/src" in line
    assert "DATA_LAKE_URI=hdfs://namenode:8020/f1" in line
    assert line.endswith(f"app python -m {module}")


def test_hdfs_dirs_only_prepares_clean_and_features() -> None:
    lines = _dry_run("hdfs-dirs").splitlines()
    assert len(lines) == 2
    assert lines[0].endswith("hdfs dfs -mkdir -p /f1/clean /f1/features")
    assert lines[1].endswith("hdfs dfs -chown f1sa /f1/clean /f1/features")


def test_pipeline_runs_clean_then_features_then_report() -> None:
    out = _dry_run("pipeline-full")
    assert out.index("f1sa.etl.clean") < out.index("f1sa.features.build")
    assert out.index("f1sa.features.build") < out.index("f1sa.etl.full_report")


def test_model_dev_full_runs_primary_then_sensitivity_without_the_test_season() -> None:
    lines = [x for x in _dry_run("model-dev-full").splitlines() if "model_eval.py" in x]
    assert len(lines) == 2
    assert all("--source full --no-test" in x and "--allow-test-season" not in x for x in lines)
    assert all("DATA_LAKE_URI=hdfs://namenode:8020/f1" in x for x in lines)
    assert "--g" not in lines[0] and lines[1].endswith("--g 5.0 --k 3")


def test_model_dev_ti_full_is_development_only_real_ti() -> None:
    lines = [x for x in _dry_run("model-dev-ti-full").splitlines() if "model_eval.py" in x]
    assert len(lines) == 2
    assert all(
        "--source full --no-test --feature-set real_ti" in x and "--allow-test-season" not in x
        for x in lines
    )
    assert all("DATA_LAKE_URI=hdfs://namenode:8020/f1" in x for x in lines)
    assert "--g" not in lines[0] and lines[1].endswith("--g 5.0 --k 3")


@pytest.mark.parametrize(
    ("target", "script", "feature_set"),
    [
        ("model-dev-tyre-full", "model_eval.py --source full --no-test", "real_tyre"),
        ("model-dev-permutation-ti-full", "model_dev_permutation.py", "real_ti"),
    ],
)
def test_dev_feature_set_targets_run_both_settings_without_the_test_season(
    target: str, script: str, feature_set: str
) -> None:
    lines = [x for x in _dry_run(target).splitlines() if script.split()[0] in x]
    assert len(lines) == 2
    assert all(script in x and f"--feature-set {feature_set}" in x for x in lines)
    assert all("--allow-test-season" not in x for x in lines)
    assert "--g" not in lines[0] and lines[1].endswith("--g 5.0 --k 3")


def _test_full(*args: str, dry: bool = True) -> subprocess.CompletedProcess[str]:
    cmd = ["make", *(["-n"] if dry else []), "--no-print-directory", "-C", str(REPO_ROOT)]
    return subprocess.run(
        [*cmd, "model-test-full", *args], capture_output=True, text=True, timeout=60, check=False
    )


def _eval_lines(out: str) -> list[str]:
    return [x.split(" app ", 1)[1] for x in out.splitlines() if "model_eval.py" in x]


REAL_V1 = [
    "python experiments/model_eval.py --source full --allow-test-season",
    "python experiments/model_eval.py --source full --allow-test-season --g 5.0 --k 3",
]


def _set_lines(fs: str) -> list[str]:
    base = f"python experiments/model_eval.py --source full --allow-test-season --feature-set {fs}"
    return [base, f"{base} --g 5.0 --k 3"]


def test_model_test_full_needs_confirm() -> None:
    for args in ((), ("CONFIRM=no",), ("FEATURE_SET=real",)):
        refused = _test_full(*args, dry=False)
        assert refused.returncode != 0 and "Refusing" in refused.stderr
        assert "model_spec_v2.md" in refused.stderr


def test_model_test_full_runs_the_three_spec_v2_sets_at_both_settings_once() -> None:
    out = _test_full("CONFIRM=yes")
    assert out.returncode == 0
    lines = _eval_lines(out.stdout)
    assert lines == REAL_V1 + _set_lines("real_tyre") + _set_lines("real_ti")
    assert all(
        "DATA_LAKE_URI=hdfs://namenode:8020/f1" in x
        for x in out.stdout.splitlines()
        if "model_eval.py" in x
    )


@pytest.mark.parametrize("fs", ["real", "real_tyre", "real_ti"])
def test_model_test_full_feature_set_runs_that_set_only(fs: str) -> None:
    out = _test_full("CONFIRM=yes", f"FEATURE_SET={fs}")
    assert out.returncode == 0
    assert _eval_lines(out.stdout) == (REAL_V1 if fs == "real" else _set_lines(fs))


@pytest.mark.parametrize("fs", ["with_synthetic", "bogus", "real real_ti"])
def test_model_test_full_refuses_feature_sets_outside_spec_v2(fs: str) -> None:
    refused = _test_full("CONFIRM=yes", f"FEATURE_SET={fs}", dry=False)
    assert refused.returncode != 0 and "Refusing: FEATURE_SET" in refused.stderr
    assert "model_eval.py" not in refused.stdout


def test_model_save_full_runs_the_deployment_entry_point_without_the_test_season() -> None:
    lines = _dry_run("model-save-full").splitlines()
    assert "test -w /workspace/models" in lines[0]  # refuses before the ./models mount exists
    runs = [x for x in lines if "model_save.py" in x]
    assert len(runs) == 1
    line = runs[0]
    assert "exec -T" in line and "DATA_LAKE_URI=hdfs://namenode:8020/f1" in line
    assert "F1SA_GIT_SHA=" in line and "F1SA_GIT_DIRTY=" in line
    assert line.endswith("app python experiments/model_save.py --source full")
    for forbidden in ("--allow-test-season", "model_eval.py", "--g", "--force"):
        assert forbidden not in line


def test_model_save_full_force_is_opt_in() -> None:
    out = subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO_ROOT), "model-save-full", "FORCE=1"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout
    assert out.splitlines()[-1].endswith("--source full --force")
