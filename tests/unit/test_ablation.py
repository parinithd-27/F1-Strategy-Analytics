"""``f1sa.model.ablation`` (``make ablation-full``) on invented scores in tmp_path; no Spark.

The fake ``score`` callable stands in for loading the frozen models: it returns invented scored
splits, and the "committed" ``model_eval`` files in the temporary ``results/`` are written from the
same rows, so the exact-match check passes unless a test tampers with them.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from f1sa.model import ablation as A
from f1sa.model import features as MF
from f1sa.model.baselines import MajorityBaseline, RuleBaseline
from f1sa.model.evaluate import ScoredSplit, bootstrap
from f1sa.model.pipeline import EVAL_COLUMNS, write_csv

N_BOOT = 25
NOW = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)
ARGS = ["--source", "full", "--allow-test-season"]


def _split(name: str, fs: str, g: float, seed: int, n_races: int = 8) -> tuple[ScoredSplit, Any]:
    """An invented scored split; the attempts and labels depend only on ``name`` and ``g``."""
    base = np.random.default_rng(int(g * 10) + (0 if name == "test" else 1000))
    rows = []
    for race in range(n_races):
        for j in range(3):
            rows.append(
                {
                    "season": 2025 if name == "test" else 2024,
                    "round": race + 1,
                    "attacker_id": f"drv{j}",
                    "lap": 10 + j,
                    "race_id": f"{2025 if name == 'test' else 2024}-{race + 1}",
                    "label": int(base.random() < 0.4),
                    "gap_s": float(base.uniform(0.5, 3.0)),
                    "defender_deg_slope_s_per_lap": float(base.uniform(0, 0.2)),
                    "defender_stint_laps": float(base.integers(5, 25)),
                    "attacker_expected_outlap_pace_residual_s": float(base.normal()),
                }
            )
    pdf = pd.DataFrame(rows).sort_values(["season", "round", "attacker_id", "lap"])
    pdf = pdf.reset_index(drop=True)
    rng = np.random.default_rng(seed)
    y = pdf["label"].to_numpy(dtype=np.float64)
    scores: dict[str, Any] = {}
    for kind in ("lr", "gbt"):
        raw = np.clip(0.3 * y + rng.uniform(0, 0.7, y.size), 0.01, 0.99)
        scores[kind] = raw
        scores[f"{kind}_calibrated"] = np.clip(raw * 0.9 + 0.05, 0.0, 1.0)
    majority = MajorityBaseline.fit([0, 0, 1])
    rule = RuleBaseline(fallback_rate=majority.positive_rate)
    scores["majority"] = majority.predict_proba(len(pdf))
    scores["rule"] = rule.predict_proba(pdf)
    return ScoredSplit(name, pdf, scores), rule


def fake_score(
    spark: Any, models_root: Path, fs: str, g: float, k: int, *, n_boot: int
) -> A.Scored:
    seed = {"real": 1, "real_tyre": 2, "real_ti": 3}[fs] + int(g * 10)
    val, _ = _split("validation", fs, g, seed)
    test, rule = _split("test", fs, g, seed + 100)
    rows = A.build_eval_rows(val, test, rule, fs, g, k, n_boot=n_boot)
    return A.Scored(fs, g, k, rows, A.set_scores(fs, test.rows, test.scores))


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A cwd with "committed" model_eval files and an earlier run that this code reproduces."""
    monkeypatch.chdir(tmp_path)
    results = tmp_path / "results"
    results.mkdir()
    (results / "README.md").write_text("# Results\n", encoding="utf-8")
    earlier = tmp_path / "data/ablation/full_20261114T101856Z"
    per_setting = {}
    for g, k in A.SETTINGS:
        per_set = {}
        for fs in A.SETS:
            s = fake_score(None, Path("models"), fs, g, k, n_boot=N_BOOT)
            write_csv(A.eval_file(results, fs, g, k), s.eval_rows, EVAL_COLUMNS, "")
            A.write_scores(earlier / A.scores_name(fs, g, k), s.test)
            folder = A.model_dir(tmp_path / "models", fs, g, k)
            folder.mkdir(parents=True)
            (folder / "metadata.json").write_text("{}\n")
            per_set[fs] = s.test
        per_setting[(g, k)] = per_set
    lines = []
    for (g, k), per_set in per_setting.items():
        y, groups = A.align(per_set)
        scores = {fs: {m: t.scores[m] for m in A.M_ALL} for fs, t in per_set.items()}
        draws = A.paired_draws(y, scores, A.race_resamples(groups, N_BOOT), A.DIFF_METRICS)
        rows = A.difference_rows(y, groups, scores, draws, g, k, n_boot=N_BOOT)
        lines += [ln for ln in A.diff_lines(rows)[1:] if "pr_auc" in ln or "brier" in ln]
    (tmp_path / "data/ablation_record.log").write_text("\n".join(lines) + "\nexit 0\n")
    return tmp_path


def _main(*extra: str, **kw: Any) -> tuple[int, list[str]]:
    out: list[str] = []
    rc = A.main(
        [*ARGS, *extra],
        spark_factory=lambda master: None,
        score=kw.pop("score", fake_score),
        n_boot=N_BOOT,
        out=out.append,
        now=lambda: NOW,
    )
    return rc, out


def _diff_rows(path: Path) -> list[dict[str, str]]:
    text = "\n".join(ln for ln in path.read_text().splitlines() if not ln.startswith("#"))
    return list(csv.DictReader(io.StringIO(text)))


# ------------------------------------------------------------------ main
def test_record_run_writes_only_its_new_files(workdir: Path) -> None:
    before = {p.name for p in (workdir / "results").iterdir()}
    rc, out = _main("--record")
    assert rc == A.EXIT_OK, "\n".join(out)
    after = {p.name for p in (workdir / "results").iterdir()}
    assert after - before == {A.RECORD_CSV, A.RECORD_PNG}
    rows = _diff_rows(workdir / "results" / A.RECORD_CSV)
    assert len(rows) == 2 * len(A.DIFF_MODELS) * len(A.DIFF_METRICS) * len(A.PAIRS) == 48
    assert {r["metric"] for r in rows} == {"pr_auc", "brier", "roc_auc", "ece10"}
    assert {r["difference"] for r in rows} == {
        "real_tyre - real",
        "real_ti - real",
        "real_ti - real_tyre",
    }
    assert all("significan" not in c for c in rows[0])  # no significance column
    readme = (workdir / "results/README.md").read_text()
    assert readme.count(f"## `{A.RECORD_CSV}` (2026-11-14T12:00:00Z)") == 1
    run_dir = workdir / "data/ablation/full_20261114T120000Z"
    assert (run_dir / "exact_match.csv").is_file()
    assert len(list(run_dir.glob("scores_*.csv"))) == 6
    joined = "\n".join(out)
    assert joined.index("exact match:") < joined.index("paired differences")


def test_without_record_results_is_untouched(workdir: Path) -> None:
    before = {p: p.read_bytes() for p in (workdir / "results").iterdir()}
    rc, out = _main()
    assert rc == A.EXIT_OK, "\n".join(out)
    assert {p: p.read_bytes() for p in (workdir / "results").iterdir()} == before
    assert (workdir / "data/ablation/full_20261114T120000Z" / A.RECORD_CSV).is_file()


def test_exact_mismatch_stops_before_any_interval(workdir: Path) -> None:
    path = A.eval_file(workdir / "results", "real_ti", 5.0, 3)
    lines = path.read_text().splitlines()
    i = next(
        n
        for n, ln in enumerate(lines)
        if ln.split(",")[5:10:4] == ["lr_calibrated", "race_cluster"]
        and ln.split(",")[6:8] == ["test", "pr_auc"]
    )
    cells = lines[i].split(",")
    cells[10] = "0.123456"  # ci_low
    lines[i] = ",".join(cells)
    path.write_text("\n".join(lines) + "\n")
    before = {p: p.read_bytes() for p in (workdir / "results").iterdir()}
    rc, out = _main("--record")
    assert rc == A.EXIT_MISMATCH
    joined = "\n".join(out)
    assert "MISMATCH" in joined and "0.123456" in joined
    assert "paired differences" not in joined
    assert {p: p.read_bytes() for p in (workdir / "results").iterdir()} == before


def test_earlier_scores_that_differ_stop_the_run(workdir: Path) -> None:
    p = workdir / "data/ablation/full_20261114T101856Z" / A.scores_name("real", 3.5, 2)
    lines = p.read_text().splitlines()
    lines[2] = lines[2][:-1] + ("0" if lines[2][-1] != "0" else "1")
    p.write_text("\n".join(lines) + "\n")
    rc, _out = _main("--record")
    assert rc == A.EXIT_EARLIER
    assert not (workdir / "results" / A.RECORD_CSV).exists()


def test_earlier_printed_differences_must_be_reproduced(workdir: Path) -> None:
    log = workdir / "data/ablation_record.log"
    text = log.read_text().splitlines()
    first = text[0]
    j = first.index("[")
    text[0] = first[: j - 7] + "+9.999 " + first[j:]
    log.write_text("\n".join(text) + "\n")
    rc, _out = _main("--record")
    assert rc == A.EXIT_EARLIER
    assert not (workdir / "results" / A.RECORD_CSV).exists()


def test_record_refuses_existing_outputs_before_scoring(workdir: Path) -> None:
    (workdir / "results" / A.RECORD_PNG).write_bytes(b"x")

    def boom(*a: Any, **k: Any) -> A.Scored:
        raise AssertionError("scored although the record run is refused")

    rc, out = _main("--record", score=boom)
    assert rc == A.EXIT_REFUSED
    assert "never overwritten" in "\n".join(out)


def test_needs_allow_test_season_and_the_earlier_run(workdir: Path) -> None:
    out: list[str] = []
    assert A.main(["--source", "full"], out=out.append) == A.EXIT_REFUSED
    (workdir / "data/ablation_record.log").unlink()
    rc, out = _main()
    assert rc == A.EXIT_REFUSED and "earlier run" in "\n".join(out)


def test_precheck_uses_the_committed_check(workdir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from f1sa.model import test_summary as T

    seen: list[list[Path]] = []
    monkeypatch.setattr(T, "check_committed", lambda paths: seen.append(list(paths)))
    rc, _out = _main("--precheck")
    assert rc == A.EXIT_OK and len(seen[0]) == 6

    def fail(paths: Any) -> None:
        raise T.SummaryError("inputs differ from HEAD")

    monkeypatch.setattr(T, "check_committed", fail)
    rc, _out = _main("--precheck")
    assert rc == A.EXIT_REFUSED


# ------------------------------------------------------------------ paired bootstrap
def _tests(g: float = 3.5) -> dict[str, A.SetScores]:
    return {fs: fake_score(None, Path(), fs, g, 2, n_boot=1).test for fs in A.SETS}


def test_one_draw_list_for_every_set_reproduces_each_sets_bootstrap() -> None:
    tests = _tests()
    y, groups = A.align(tests)
    draws = A.race_resamples(groups, N_BOOT)
    again = A.race_resamples(groups, N_BOOT)
    assert all(np.array_equal(a, b) for a, b in zip(draws, again, strict=True))
    scores = {fs: {m: t.scores[m] for m in A.M_ALL} for fs, t in tests.items()}
    values = A.paired_draws(y, scores, draws, ("pr_auc", "brier"))
    for fs, t in tests.items():
        ref = bootstrap(y, t.scores, groups, kind="race_cluster", n=N_BOOT, seed=A.SEED)
        for model in A.M_ALL:
            for metric in ("pr_auc", "brier"):
                lo, hi, nv = A.percentile_ci(values[(fs, model, metric)])
                rlo, rhi, rnv = ref[(model, metric)]
                assert (lo, hi, nv) == pytest.approx((rlo, rhi, rnv), nan_ok=True)


def test_difference_is_b_minus_a_on_the_same_draws() -> None:
    tests = _tests()
    y, groups = A.align(tests)
    scores = {fs: {m: t.scores[m] for m in A.M_ALL} for fs, t in tests.items()}
    draws = A.race_resamples(groups, N_BOOT)
    values = A.paired_draws(y, scores, draws, A.DIFF_METRICS)
    rows = A.difference_rows(y, groups, scores, values, 3.5, 2, n_boot=N_BOOT)
    r = next(x for x in rows if x["model"] == "lr_calibrated" and x["metric"] == "brier"
             and x["difference"] == "real_ti - real_tyre")  # fmt: skip
    d = (
        values[("real_ti", "lr_calibrated", "brier")]
        - values[("real_tyre", "lr_calibrated", "brier")]
    )
    assert (r["ci_low"], r["ci_high"]) == pytest.approx(A.percentile_ci(d)[:2])
    assert r["diff"] == pytest.approx(r["value_b"] - r["value_a"])
    assert r["n_races"] == 8 and r["n_attempts"] == 24 and r["note"] == ""
    gbt = next(x for x in rows if x["model"] == "gbt_calibrated")
    assert "not interpretable" in gbt["note"]


def test_align_refuses_different_attempts_or_labels() -> None:
    tests = _tests()
    t = tests["real_ti"]
    tests["real_ti"] = A.SetScores(
        t.feature_set, t.keys[:-1], t.race_ids[:-1], t.labels[:-1], t.scores
    )
    with pytest.raises(A.AblationError, match="different attempts"):
        A.align(tests)
    tests = _tests()
    t = tests["real_tyre"]
    tests["real_tyre"] = A.SetScores(t.feature_set, t.keys, t.race_ids, 1 - t.labels, t.scores)
    with pytest.raises(A.AblationError, match="labels"):
        A.align(tests)


def test_percentile_ci_skips_undefined_draws() -> None:
    lo, hi, n = A.percentile_ci(np.array([np.nan, 1.0, 2.0, 3.0]))
    assert n == 3 and lo == pytest.approx(1.05) and hi == pytest.approx(2.95)
    assert A.percentile_ci(np.array([np.nan]))[2] == 0


# ------------------------------------------------------------------ helpers
def test_only_the_test_season_sets_are_accepted() -> None:
    assert A.SETS == MF.TEST_FEATURE_SETS
    with pytest.raises(A.AblationError, match="not a test-season set"):
        A.model_dir(Path("models"), "with_synthetic", 3.5, 2)
    assert A.model_dir(Path("m"), "real_ti", 5.0, 3) == Path("m/undercut/full_real_ti_G5.0_k3")
    assert A.model_dir(Path("m"), "real", 3.5, 2) == Path("m/undercut/full_real_G3.5_k2")
    assert A.eval_file(Path("r"), "real", 5.0, 3) == Path("r/model_eval_G5.0_k3.csv")
    assert A.eval_file(Path("r"), "real_tyre", 3.5, 2) == Path("r/model_eval_real_tyre_G3.5_k2.csv")


def test_frozen_metadata_must_be_the_test_run() -> None:
    meta = {
        "source": "full",
        "feature_set": "real",
        "test_season_evaluated": True,
        "g": 3.5,
        "k": 2,
        "calibrators": {"lr": {}, "gbt": {}},
    }
    A.check_metadata(meta, "real", 3.5, 2)
    for key, bad in (("test_season_evaluated", False), ("feature_set", "real_ti"), ("k", 3)):
        with pytest.raises(A.AblationError):
            A.check_metadata({**meta, key: bad}, "real", 3.5, 2)
    with pytest.raises(A.AblationError, match="calibrator"):
        A.check_metadata({**meta, "calibrators": {"lr": {}}}, "real", 3.5, 2)


def test_compare_eval_marks_missing_rows_and_precision() -> None:
    committed = [{"model": "lr", "split": "test", "metric": "brier", "ci_kind": "race_cluster",
                  "value": "0.100000", "ci_low": "", "ci_high": "0.200000"}]  # fmt: skip
    recomputed = [{"model": "lr", "split": "test", "metric": "brier", "ci_kind": "race_cluster",
                   "value": 0.1000001, "ci_low": float("nan"), "ci_high": 0.2},
                  {"model": "gbt", "split": "test", "metric": "brier", "ci_kind": "race_cluster",
                   "value": 0.3}]  # fmt: skip
    matches = A.compare_eval(committed, recomputed, "real", "G3.5_k2")
    by = {(m.model, m.column): m for m in matches}
    assert by[("lr", "value")].ok and by[("lr", "ci_low")].ok and by[("lr", "ci_high")].ok
    assert not by[("gbt", "value")].ok and by[("gbt", "value")].committed == "<missing>"
    table = A.match_table(matches)
    assert "lr" in table and "NO" in table


def test_parse_earlier_log_reads_the_printed_format() -> None:
    text = (
        "paired differences b - a, race-cluster 95% interval (no significance claim):\n"
        "  G=3.5 k=2 lr_calibrated   pr_auc real_tyre - real       +0.021 [-0.136, +0.128]\n"
        "  G=5 k=3 gbt_calibrated  brier  real_ti - real_tyre    +0.004 [-0.015, +0.023]\n"
        "exit 0\n"
    )
    got = A.parse_earlier_log(text)
    assert got == {
        ("3.5", 2, "lr_calibrated", "pr_auc", "real", "real_tyre"): ("+0.021", "-0.136", "+0.128"),
        ("5", 3, "gbt_calibrated", "brier", "real_tyre", "real_ti"): ("+0.004", "-0.015", "+0.023"),
    }
    rows = [{"g": "3.5", "k": 2, "model": "lr_calibrated", "metric": "pr_auc", "set_a": "real",
             "set_b": "real_tyre", "diff": 0.0214, "ci_low": -0.1357,
             "ci_high": 0.1281}]  # fmt: skip
    problems = A.compare_earlier(rows, got)
    assert len(problems) == 1 and "gbt_calibrated" in problems[0]
    assert A.compare_earlier(rows, {}) == ["the earlier log has no difference lines"]


def test_scores_round_trip_and_comparison(tmp_path: Path) -> None:
    t = _tests()["real"]
    path = tmp_path / "s.csv"
    A.write_scores(path, t)
    assert path.read_text().startswith(f"# {A.STAMP}\n")
    assert A.compare_scores(A.score_lines(t), path) == []
    assert A.compare_scores(A.score_lines(t)[:-1], path)
    assert A.compare_scores([], tmp_path / "missing.csv") == [
        f"{tmp_path / 'missing.csv'}: missing"
    ]


def test_set_scores_refuses_duplicate_attempts() -> None:
    rows = pd.DataFrame(
        {"season": [2025, 2025], "round": [1, 1], "attacker_id": ["a", "a"], "lap": [3, 3],
         "race_id": ["2025-1", "2025-1"], "label": [0, 1]}
    )  # fmt: skip
    with pytest.raises(A.AblationError, match="duplicate"):
        A.set_scores("real", rows, {"lr": [0.1, 0.2]})


def test_fmt_matches_the_csv_writer() -> None:
    assert A.fmt(0.5) == "0.500000" and A.fmt(float("nan")) == "" and A.fmt(3) == "3"
    assert A.fmt("x") == "x"


def test_forest_plot_is_written(tmp_path: Path) -> None:
    tests = _tests()
    y, groups = A.align(tests)
    scores = {fs: {m: t.scores[m] for m in A.M_ALL} for fs, t in tests.items()}
    values = A.paired_draws(y, scores, A.race_resamples(groups, 5), A.DIFF_METRICS)
    rows = A.difference_rows(y, groups, scores, values, 3.5, 2, n_boot=5)
    A.plot_forest(tmp_path / "f.png", rows)
    assert (tmp_path / "f.png").read_bytes()[:4] == b"\x89PNG"
