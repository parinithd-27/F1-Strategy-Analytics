"""``f1sa.bench.experiments`` (``make experiments`` / ``make experiments-sample``) with a fake
runner: sandbox commands, the skip list, exit-code mapping and the sha256 comparison. Nothing is run
and the repository's ``results/`` is never touched (the fake repo lives in tmp_path)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from f1sa.bench import experiments as E

NOW = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)
REPO_ROOT = Path(__file__).resolve().parents[2]


def _fake_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "results").mkdir(parents=True)
    return repo


def test_full_generators_cover_the_regenerable_targets_and_never_open_2025() -> None:
    names = [g.name for g in E.full_generators()]
    assert names == [
        "model-dev-full",
        "model-dev-tyre-full",
        "model-dev-ti-full",
        "model-dev-permutation-ti-full",
        "oracle-dev-full",
        "whatif-consistency-full",
        "stream-parity-full",
    ]
    for g in E.full_generators():
        assert g.where == "app"
        for argv in g.commands:
            joined = " ".join(argv)
            assert "--allow-test-season" not in joined and "--record" not in joined.replace(
                "oracle_dev.py --source full --record", ""
            )
            assert "model_test_summary" not in joined and "ablation.py" not in joined
            if "model_eval.py" in joined:
                assert "--no-test" in argv and "./results" in argv and "./models" in argv


def test_sample_generators_run_on_the_host() -> None:
    gens = E.sample_generators()
    assert [g.name for g in gens] == [
        "etl-sample",
        "features-sample",
        "model-eval-sample",
        "sim-tyre-model-sample",
        "whatif-consistency-sample",
    ]
    assert all(g.where == "host" for g in gens)


def test_skip_list_names_the_2025_and_measurement_generators() -> None:
    names = {s.name for s in E.skipped(sample=False)}
    assert {
        "model-test-full",
        "whatif-consistency-full RECORD=1",
        "ablation-full RECORD=1",
    } <= names
    assert {"experiments-spark", "stream-latency-suite", "redis-latency"} <= names
    assert all(s.command for s in E.skipped(sample=False))


def test_dev_outputs_follow_the_pipeline_names() -> None:
    names = {o.produced for o in E.dev_outputs("real_ti")}
    assert "results/model_dev_eval_real_ti.csv" in names
    assert "results/model_dev_loso_real_ti_G5.0_k3.csv" in names
    assert "results/model_dev_missing_rates_real_ti.csv" in names


def test_app_commands_run_in_the_sandbox_inside_the_container(tmp_path: Path) -> None:
    repo = _fake_repo(tmp_path)
    sandbox = E.sandbox_dir("exp_x", repo)
    g = E.full_generators()[0]
    cmd = E.command_line(g, g.commands[0], sandbox, repo)
    assert cmd[:6] == ["docker", "compose", "exec", "-T", "-w", "/workspace/data/experiments/exp_x"]
    assert "app" in cmd


def test_commands_naming_the_committed_results_are_refused(tmp_path: Path) -> None:
    repo = _fake_repo(tmp_path)
    with pytest.raises(ValueError, match="committed results"):
        E.check_not_repo_results(["python", "x.py", "--out", f"{repo}/results/a.csv"], repo)
    with pytest.raises(ValueError):
        E.check_not_repo_results(["--results-dir", "/workspace/results"], repo)
    E.check_not_repo_results(["--results-dir", "./results"], repo)


def test_host_env_points_at_the_sandbox_lake(tmp_path: Path) -> None:
    env = E.host_env(tmp_path / "sb", tmp_path / "repo")
    assert env["DATA_LAKE_URI"] == f"file://{tmp_path / 'sb' / 'data' / 'lake'}"
    assert env["SAMPLE_DIR"] == str(tmp_path / "repo" / "data" / "sample")


def test_recorded_check_failure_is_not_a_crash(tmp_path: Path) -> None:
    g = E.Generator("w", "host", (("a",), ("b",)), (), recorded_failure_codes=frozenset({1}))
    codes = iter([1, 0])
    o = E.run_generator(
        g, tmp_path, repo=tmp_path, runner=lambda c, d, e: next(codes), out=lambda s: None
    )
    assert o.status == "recorded check failure" and o.exit_codes == [1, 0]
    crash = E.run_generator(
        g, tmp_path, repo=tmp_path, runner=lambda c, d, e: 3, out=lambda s: None
    )
    assert crash.status == "crashed" and crash.exit_codes == [3]  # stops at the first crash
    assert E.exit_code([crash], []) == E.EXIT_CRASH


def test_compare_statuses_and_drop_rows(tmp_path: Path) -> None:
    repo = _fake_repo(tmp_path)
    sb = tmp_path / "sb"
    (sb / "results").mkdir(parents=True)
    (repo / "results/a.csv").write_text("x,subset\n1,2024\n2,2025\n")
    (sb / "results/a.csv").write_text("x,subset\n1,2024\n")
    (repo / "results/b.csv").write_text("same\n")
    (sb / "results/b.csv").write_text("same\n")
    g = E.Generator(
        "g",
        "host",
        (),
        (
            E.Output("results/a.csv", "results/a.csv", drop_rows_with=("subset", "2025")),
            E.Output("results/b.csv", "results/b.csv"),
            E.Output("results/c.csv", "results/c.csv"),
            E.Output("results/b.csv", None),
        ),
    )
    got = [(c.status, c.committed) for c in E.compare(g, sb, repo)]
    assert got == [
        ("DIFF", "results/a.csv"),
        ("OK", "results/a.csv without subset=2025 rows"),
        ("OK", "results/b.csv"),
        ("MISSING", "results/c.csv"),
        ("NEW", "-"),
    ]
    assert E.exit_code([], E.compare(g, sb, repo)) == E.EXIT_DIFF


def test_metadata_only_differences_are_ok_and_named(tmp_path: Path) -> None:
    repo = _fake_repo(tmp_path)
    sb = tmp_path / "sb"
    (sb / "models").mkdir(parents=True)
    (repo / "models").mkdir()
    committed = (
        '{\n "created_utc": "2026-11-13T14:13:33Z",\n "git_dirty": true,\n'
        ' "git_sha": "43ac",\n "x": [1]\n}\n'
    )
    (repo / "models/m.json").write_text(committed)
    (sb / "models/m.json").write_text(
        committed.replace("2026-11-13T14:13:33Z", "2026-11-14T10:59:18Z").replace("43ac", "20e1")
    )
    out = E.Output("models/m.json", "models/m.json", ignore_json_keys=E.PROVENANCE_KEYS)
    g = E.Generator("g", "host", (), (out,))
    (c,) = E.compare(g, sb, repo)
    assert c.status == "OK (metadata differs)" == E.OK_METADATA
    assert "ignored fields: created_utc, git_sha" in c.detail
    assert E.exit_code([], [c]) == E.EXIT_OK
    # any other difference, git_dirty included, is still a DIFF
    (sb / "models/m.json").write_text(committed.replace('"git_dirty": true', '"git_dirty": false'))
    (c,) = E.compare(g, sb, repo)
    assert c.status == "DIFF" and "content differs too" in c.detail
    (sb / "models/m.json").write_text(committed.replace("[1]", "[2]").replace("43ac", "ffff"))
    assert E.compare(g, sb, repo)[0].status == "DIFF"


def test_mask_keeps_every_other_byte() -> None:
    data = b'{"created_utc": "a\\"b", "git_sha":"c", "other": "created_utc"}'
    masked = E.mask_json_strings(data, E.PROVENANCE_KEYS)
    assert masked == b'{"created_utc": "<ignored>", "git_sha":"<ignored>", "other": "created_utc"}'
    assert E.mask_json_strings(data, ()) == data


def test_drop_rows_refuses_an_unknown_column() -> None:
    with pytest.raises(ValueError):
        E.drop_rows("a,b\n1,2\n", "c", "1")


def test_main_runs_in_a_new_sandbox_and_never_writes_results(tmp_path: Path) -> None:
    repo = _fake_repo(tmp_path)
    (repo / "results/undercut_counts_sample.csv").write_text("n\n1\n")
    seen: list[tuple[list[str], Path, Mapping[str, str] | None]] = []

    def runner(cmd: list[str], cwd: Path, env: Mapping[str, str] | None) -> int:
        seen.append((cmd, cwd, env))
        if "f1sa.features.build" in cmd:
            (cwd / "results/undercut_counts_sample.csv").write_text("n\n1\n")
        return 0

    out: list[str] = []
    rc = E.main(["--sample", "--only", "features-sample"], runner=runner, now=lambda: NOW,
                out=out.append, repo=repo)  # fmt: skip
    sandbox = repo / "data/experiments/exp_20261114T120000Z_SAMPLE"
    assert rc == E.EXIT_OK
    assert seen[0][1] == sandbox and (sandbox / "SAMPLE").is_file()
    assert sorted(p.name for p in (repo / "results").iterdir()) == ["undercut_counts_sample.csv"]
    assert any(line.startswith("*** SAMPLE") for line in out)
    assert any(re.match(r"\s+OK\s+features-sample", line) for line in out)
    assert (sandbox / "summary.txt").read_text().startswith(E.SAMPLE_STAMP)


def test_unknown_generator_is_an_error(tmp_path: Path) -> None:
    out: list[str] = []
    assert E.main(["--only", "nope"], out=out.append, repo=_fake_repo(tmp_path)) == E.EXIT_CRASH


def test_dry_run_creates_nothing(tmp_path: Path) -> None:
    repo = _fake_repo(tmp_path)
    out: list[str] = []
    assert E.main(["--dry-run"], now=lambda: NOW, out=out.append, repo=repo) == E.EXIT_OK
    assert not (repo / "data").exists()
    assert sum(line.startswith("[dry-run]") for line in out) == 11  # 6 dev + 2 perm + 3


def test_make_targets_call_the_runner() -> None:
    text = (REPO_ROOT / "Makefile").read_text()
    assert re.search(r"^experiments:.*\n\texperiments/run_all\.sh\n", text, re.M)
    assert re.search(r"^experiments-sample:.*\n\texperiments/run_all\.sh --sample\n", text, re.M)
    script = (REPO_ROOT / "experiments/run_all.sh").read_text()
    assert "f1sa.bench.experiments" in script and "results/" not in script.split("set -euo")[1]
