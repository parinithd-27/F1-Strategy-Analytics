"""``make experiments`` / ``make experiments-sample`` (Step 16): rerun every regenerable
generator in a sandbox and compare its outputs with the committed ``results/`` files.
``results/`` is never written.

* Each generator runs with its working directory set to the sandbox
  ``data/experiments/<run_id>/`` (``<run_id>_SAMPLE`` for the sample bundle), so its ``./results``,
  ``./models`` and ``./data`` are the sandbox's own. Inputs (the deployed model, ``data/sample``)
  are passed as absolute repo paths. Full generators run in the app container (HDFS lake,
  ``docker compose exec -w``), sample generators on the host against a sandbox lake.
* Generators that open the 2025 test season or that refuse to overwrite their committed results
  are never run; they are printed as skipped, as are the measurements tied to a service state or
  hours of runtime (each with the command that produces it).
* Every declared output is compared by sha256 with the committed file: ``OK``, ``DIFF``,
  ``MISSING`` (the generator did not write it) or ``NEW`` (no committed file to compare with).
* A generator whose exit code is a recorded check failure (the what-if ordering check, exit 1)
  is a recorded result, not a crash.

Exit code: 0 everything ran and every comparison is OK, 1 a DIFF or MISSING, 2 a generator crashed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import re
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from f1sa.features.undercut_rule import DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS

REPO = Path(__file__).resolve().parents[3]
"""The repository on the host (``src/f1sa/bench/experiments.py`` -> repo root)."""
APP_REPO = Path("/workspace")
"""The repository inside the app container."""
SANDBOX_ROOT = Path("data/experiments")
HDFS_LAKE = "hdfs://namenode:8020/f1"
DEPLOY_MODEL = "models/undercut/deploy_real_G3.5_k2"
SAMPLE_STAMP = "SAMPLE: 2 sample races, synthetic tyres/telemetry; plumbing check, not findings"
SETTINGS: tuple[tuple[float, int], ...] = ((DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS), (5.0, 3))
OK_METADATA = "OK (metadata differs)"
STATUSES = ("OK", OK_METADATA, "DIFF", "MISSING", "NEW")
EXIT_OK, EXIT_DIFF, EXIT_CRASH = 0, 1, 2


@dataclass(frozen=True)
class Output:
    """A file the generator writes (``produced``, relative to the sandbox; a ``*`` is globbed)
    and the committed file it is compared with (relative to the repo; ``None`` = none)."""

    produced: str
    committed: str | None
    drop_rows_with: tuple[str, str] | None = None
    """``(column, value)``: also compare with the committed CSV minus these rows."""
    ignore_json_keys: tuple[str, ...] = ()
    """JSON keys whose string values (a timestamp, the git sha) are masked before the byte-for-byte
    comparison: a file that differs only there is ``OK (metadata differs)``."""


@dataclass(frozen=True)
class Generator:
    name: str
    where: str  # "app" (container, HDFS lake) or "host"
    commands: tuple[tuple[str, ...], ...]
    outputs: tuple[Output, ...]
    recorded_failure_codes: frozenset[int] = frozenset()
    note: str = ""


@dataclass(frozen=True)
class Skipped:
    name: str
    reason: str
    command: str


@dataclass
class Comparison:
    generator: str
    produced: str
    committed: str
    status: str
    detail: str = ""


@dataclass
class RunOutcome:
    generator: str
    status: str  # ran / recorded check failure / crashed
    seconds: float
    exit_codes: list[int] = field(default_factory=list)


# ------------------------------------------------------------------ registry
def _suffix(feature_set: str, g: float, k: int) -> str:
    fs = "" if feature_set == "real" else f"_{feature_set}"
    st = (
        ""
        if (float(g), int(k)) == (float(DEFAULT_GAP_S), int(DEFAULT_WINDOW_LAPS))
        else (f"_G{float(g):.1f}_k{int(k)}")
    )
    return f"{fs}{st}"


def dev_outputs(feature_set: str) -> tuple[Output, ...]:
    """The files of a development model run (``RunConfig.result_paths`` names, both settings)."""
    from f1sa.model.pipeline import RunConfig

    out = []
    for g, k in SETTINGS:
        cfg = RunConfig(source="full", no_test=True, feature_set=feature_set, g=g, k=k)
        out += [Output(f"results/{p.name}", f"results/{p.name}") for p in cfg.result_paths()]
    return tuple(out)


def _setting_args(g: float, k: int) -> tuple[str, ...]:
    if (float(g), int(k)) == (float(DEFAULT_GAP_S), int(DEFAULT_WINDOW_LAPS)):
        return ()
    return ("--g", f"{g:g}", "--k", str(k))


def full_generators(repo: Path = APP_REPO) -> list[Generator]:
    """The regenerable non-2025 generators of the full lake (app container)."""
    py = ("python",)
    gens = []
    for fs, target in (("real", "model-dev-full"), ("real_tyre", "model-dev-tyre-full"),
                       ("real_ti", "model-dev-ti-full")):  # fmt: skip
        fs_args = () if fs == "real" else ("--feature-set", fs)
        gens.append(
            Generator(
                target,
                "app",
                tuple(
                    (
                        *py,
                        str(repo / "experiments/model_eval.py"),
                        "--source",
                        "full",
                        "--no-test",
                        *fs_args,
                        *_setting_args(g, k),
                        "--results-dir",
                        "./results",
                        "--models-dir",
                        "./models",
                    )
                    for g, k in SETTINGS
                ),
                dev_outputs(fs),
            )
        )
    gens.append(
        Generator(
            "model-dev-permutation-ti-full",
            "app",
            tuple(
                (
                    *py,
                    str(repo / "experiments/model_dev_permutation.py"),
                    "--feature-set",
                    "real_ti",
                    *_setting_args(g, k),
                )
                for g, k in SETTINGS
            ),
            tuple(
                Output(
                    f"results/model_dev_permutation{_suffix('real_ti', g, k)}.csv",
                    f"results/model_dev_permutation{_suffix('real_ti', g, k)}.csv",
                )
                for g, k in SETTINGS
            ),
            note="reads the real_ti development run of the same sandbox",
        )
    )
    gens.append(
        Generator(
            "oracle-dev-full",
            "app",
            ((*py, str(repo / "experiments/oracle_dev.py"), "--source", "full", "--record"),),
            tuple(
                Output(
                    f"results/{n}{_suffix('real', g, k)}.csv",
                    f"results/{n}{_suffix('real', g, k)}.csv",
                )
                for g, k in SETTINGS
                for n in ("model_dev_eval_oracle", "model_dev_loso_oracle")
            ),
            note="ORACLE: realised out-lap delta, not usable at decision time; ./results is the "
            "sandbox's",
        )
    )
    gens.append(
        Generator(
            "whatif-consistency-full",
            "app",
            (
                (
                    *py,
                    str(repo / "experiments/whatif_consistency.py"),
                    "--source",
                    "full",
                    "--model-dir",
                    str(repo / DEPLOY_MODEL),
                    "--sim-dir",
                    str(repo / "models/sim"),
                ),
            ),
            (
                Output(
                    "data/whatif/full_*/whatif_consistency.csv",
                    "results/whatif_consistency.csv",
                    drop_rows_with=("subset", "2025"),
                ),
                Output(
                    "data/whatif/full_*/whatif_consistency.png", "results/whatif_consistency.png"
                ),
            ),
            recorded_failure_codes=frozenset({1}),
            note="without RECORD: no 2025 held-out row (the committed CSV has it, so the plain "
            "sha256 differs; the comparison without the 2025 rows is the meaningful one)",
        )
    )
    gens.append(
        Generator(
            "stream-parity-full",
            "app",
            (
                (
                    *py,
                    str(repo / "experiments/stream_parity.py"),
                    "--seasons",
                    "2021-2024",
                    "--model-dir",
                    str(repo / DEPLOY_MODEL),
                    "--out",
                    "./results/stream_parity_full.csv",
                ),
            ),
            (Output("results/stream_parity_full.csv", "results/stream_parity_full.csv"),),
        )
    )
    return gens


SAMPLE_RESULT_FILES = (
    "model_eval_sample.csv",
    "calibration_sample.csv",
    "feature_importance_sample.csv",
    "feature_importance_sample.png",
    "reliability_sample.png",
    "confusion_matrix_sample.png",
)


def sample_generators(repo: Path = REPO) -> list[Generator]:
    """The sample bundle on the host, into a sandbox lake (``data/lake`` is not touched)."""
    uv = ("uv", "run", "--project", str(repo), "python")
    return [
        Generator("etl-sample", "host", ((*uv, "-m", "f1sa.etl.clean", "--source", "sample"),), ()),
        Generator(
            "features-sample",
            "host",
            (
                (
                    *uv,
                    "-m",
                    "f1sa.features.build",
                    "--source",
                    "sample",
                    "--results-dir",
                    "./results",
                ),
            ),
            (Output("results/undercut_counts_sample.csv", "results/undercut_counts_sample.csv"),),
        ),
        Generator(
            "model-eval-sample",
            "host",
            (
                (
                    *uv,
                    str(repo / "experiments/model_eval.py"),
                    "--source",
                    "sample",
                    "--results-dir",
                    "./results",
                    "--models-dir",
                    "./models",
                ),
            ),
            tuple(Output(f"results/{n}", f"results/{n}") for n in SAMPLE_RESULT_FILES),
        ),
        Generator(
            "sim-tyre-model-sample",
            "host",
            ((*uv, "-m", "f1sa.sim.tyre_model", "--source", "sample"),),
            (
                Output(
                    "models/sim/tyre_model_sample.json",
                    "models/sim/tyre_model_sample.json",
                    ignore_json_keys=PROVENANCE_KEYS,
                ),
            ),
            note="models/ is not committed: compared with the local repo file",
        ),
        Generator(
            "whatif-consistency-sample",
            "host",
            (
                (
                    *uv,
                    str(repo / "experiments/whatif_consistency.py"),
                    "--source",
                    "sample",
                    "--model-dir",
                    str(repo / DEPLOY_MODEL),
                    "--sim-dir",
                    "./models/sim",
                ),
            ),
            (
                Output("data/whatif/sample_*/whatif_consistency.csv", None),
                Output("data/whatif/sample_*/whatif_consistency.png", None),
            ),
            recorded_failure_codes=frozenset({1}),
            note="no committed sample what-if file (Step 15 kept it in data/whatif/)",
        ),
    ]


def skipped(sample: bool) -> list[Skipped]:
    """Never run here, with the reason and the command that produces them."""
    if sample:
        return [
            Skipped("stream-demo (sample)", "measurement: needs kafka/redis and a quiet host",
                    "make stream-demo SEASON=2024 ROUND=5 SPEED=10"),
        ]  # fmt: skip
    opens_2025 = "opens the 2025 test season; committed results exist and it refuses to overwrite"
    measurement = "measurement tied to a service state or hours of runtime; not regenerable here"
    return [
        Skipped("model-test-full", opens_2025, "make model-test-full CONFIRM=yes  (refuses)"),
        Skipped("whatif-consistency-full RECORD=1", opens_2025 + " (adds the 2025 row)",
                "make whatif-consistency-full RECORD=1  (refuses)"),
        Skipped("ablation-full RECORD=1", opens_2025 + " (frozen 9f models)",
                "make ablation-full RECORD=1  (refuses)"),
        Skipped("model-test-summary", "copies the committed 9f files; covered by the unit test "
                "test_model_test_summary::test_committed_outputs_match_the_committed_inputs",
                "make model-test-summary"),
        Skipped("experiments-spark", measurement + " (~6.3 h reduced grid)",
                "make experiments-spark RECORD=1 GRID_ARGS=\"...\" (docs/experiments.md)"),
        Skipped("stream-latency-suite", measurement + " (core profile only, ~30 min)",
                "make stream-latency-suite RECORD=1"),
        Skipped("stream-latency-ui", measurement + " (api DEBUG=1, host Next.js)",
                "make stream-latency-ui RECORD=1"),
        Skipped("redis-latency", measurement, "make redis-latency RECORD=1"),
    ]  # fmt: skip


# ------------------------------------------------------------------ commands
def run_id(now: datetime, sample: bool) -> str:
    return f"exp_{now.strftime('%Y%m%dT%H%M%SZ')}" + ("_SAMPLE" if sample else "")


def sandbox_dir(rid: str, repo: Path = REPO) -> Path:
    return repo / SANDBOX_ROOT / rid


def command_line(
    gen: Generator, argv: Sequence[str], sandbox: Path, repo: Path = REPO
) -> list[str]:
    """The full command: ``docker compose exec -w <sandbox in the container>`` for ``app``; the
    argv itself (run with ``cwd=sandbox``) for ``host``."""
    if gen.where == "host":
        return list(argv)
    inner = APP_REPO / sandbox.relative_to(repo)
    return [
        "docker", "compose", "exec", "-T", "-w", str(inner),
        "-e", "PYTHONPATH=/workspace/src", "-e", f"DATA_LAKE_URI={HDFS_LAKE}",
        "app", *argv,
    ]  # fmt: skip


def host_env(sandbox: Path, repo: Path = REPO) -> dict[str, str]:
    """Host generators: the sandbox lake (``etl.clean`` accepts only ``./data/lake``) and the
    repo's sample bundle."""
    env = dict(os.environ)
    env["DATA_LAKE_URI"] = f"file://{sandbox / 'data' / 'lake'}"
    env["SAMPLE_DIR"] = str(repo / "data" / "sample")
    return env


def check_not_repo_results(cmd: Sequence[str], repo: Path = REPO) -> None:
    """No argument may name the repository's ``results/`` (or ``models/undercut/full_*``)."""
    bad = [str(repo / "results"), str(APP_REPO / "results")]
    for a in cmd:
        if any(a == b or a.startswith(b + "/") for b in bad):
            raise ValueError(f"refusing a command that names the committed results: {a}")


# ------------------------------------------------------------------ comparison
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def drop_rows(text: str, column: str, value: str) -> str:
    """The CSV ``text`` without the rows whose ``column`` equals ``value`` (byte layout kept)."""
    lines = text.splitlines(keepends=True)
    if not lines:
        return text
    header = next(csv.reader(io.StringIO(lines[0])))
    if column not in header:
        raise ValueError(f"no column {column!r}")
    i = header.index(column)
    kept = [lines[0]]
    for ln in lines[1:]:
        cells = next(csv.reader(io.StringIO(ln)), [])
        if len(cells) > i and cells[i] == value:
            continue
        kept.append(ln)
    return "".join(kept)


PROVENANCE_KEYS = ("created_utc", "git_sha")
"""The metadata fields a regenerated artefact may differ in: when it was made, and from which
commit. Everything else (``git_dirty`` included) is compared byte for byte."""


def mask_json_strings(data: bytes, keys: Sequence[str]) -> bytes:
    """``data`` with the string value of every ``"<key>": "..."`` for ``keys`` replaced by
    ``"<ignored>"``; every other byte is kept, so the rest compares byte for byte."""
    if not keys:
        return data
    names = "|".join(re.escape(k) for k in keys)
    pattern = re.compile(rb'("(?:' + names.encode() + rb')"\s*:\s*)"(?:[^"\\]|\\.)*"')
    return pattern.sub(rb'\1"<ignored>"', data)


def resolve_produced(sandbox: Path, pattern: str) -> Path | None:
    hits = sorted(sandbox.glob(pattern)) if "*" in pattern else [sandbox / pattern]
    hits = [h for h in hits if h.is_file()]
    return hits[-1] if hits else None


def compare(gen: Generator, sandbox: Path, repo: Path = REPO) -> list[Comparison]:
    out = []
    for o in gen.outputs:
        produced = resolve_produced(sandbox, o.produced)
        committed = repo / o.committed if o.committed else None
        shown_c = o.committed or "-"
        if produced is None:
            out.append(Comparison(gen.name, o.produced, shown_c, "MISSING"))
            continue
        rel = str(produced.relative_to(sandbox))
        if committed is None or not committed.is_file():
            out.append(Comparison(gen.name, rel, shown_c, "NEW", "no committed file"))
            continue
        a, b = produced.read_bytes(), committed.read_bytes()
        same = sha256_bytes(a) == sha256_bytes(b)
        detail = f"sha256 {sha256_bytes(a)[:12]}" + ("" if same else f" vs {sha256_bytes(b)[:12]}")
        status = "OK" if same else "DIFF"
        if not same and o.ignore_json_keys:
            ma, mb = (
                mask_json_strings(a, o.ignore_json_keys),
                mask_json_strings(b, o.ignore_json_keys),
            )
            if ma == mb:
                status = OK_METADATA
            detail += f"; ignored fields: {', '.join(o.ignore_json_keys)}"
            detail += "; everything else byte-identical" if ma == mb else "; content differs too"
        out.append(Comparison(gen.name, rel, shown_c, status, detail))
        if o.drop_rows_with is not None:
            col, val = o.drop_rows_with
            reduced = drop_rows(b.decode("utf-8"), col, val).encode("utf-8")
            ok = sha256_bytes(a) == sha256_bytes(reduced)
            out.append(
                Comparison(gen.name, rel, f"{shown_c} without {col}={val} rows",
                           "OK" if ok else "DIFF", "the meaningful comparison for this generator")
            )  # fmt: skip
    return out


# ------------------------------------------------------------------ run
Runner = Callable[[list[str], Path, Mapping[str, str] | None], int]


def _default_runner(cmd: list[str], cwd: Path, env: Mapping[str, str] | None) -> int:
    return subprocess.run(cmd, cwd=cwd, env=dict(env) if env else None, check=False).returncode


def run_generator(
    gen: Generator,
    sandbox: Path,
    *,
    repo: Path = REPO,
    runner: Runner = _default_runner,
    out: Callable[[str], None] = print,
    clock: Callable[[], float] = time.monotonic,
) -> RunOutcome:
    t0 = clock()
    codes: list[int] = []
    status = "ran"
    for argv in gen.commands:
        cmd = command_line(gen, argv, sandbox, repo)
        check_not_repo_results(cmd, repo)
        out(f"$ {shlex.join(cmd)}" + (f"   (cwd {sandbox})" if gen.where == "host" else ""))
        cwd = sandbox if gen.where == "host" else repo
        rc = runner(cmd, cwd, host_env(sandbox, repo) if gen.where == "host" else None)
        codes.append(rc)
        if rc == 0:
            continue
        if rc in gen.recorded_failure_codes:
            status = "recorded check failure"
            continue
        status = "crashed"
        break
    return RunOutcome(gen.name, status, clock() - t0, codes)


def summary_lines(
    outcomes: Sequence[RunOutcome], comparisons: Sequence[Comparison], skips: Sequence[Skipped]
) -> list[str]:
    lines = ["", "== generators"]
    for o in outcomes:
        lines.append(f"  {o.generator:32s} {o.status:24s} {o.seconds:7.1f} s  exit {o.exit_codes}")
    lines.append("== outputs vs committed results/ (sha256)")
    for c in comparisons:
        lines.append(f"  {c.status:21s} {c.generator:30s} {c.produced}  <->  {c.committed}"
                     + (f"  [{c.detail}]" if c.detail else ""))  # fmt: skip
    lines.append("== skipped (never run by make experiments)")
    for s in skips:
        lines.append(f"  {s.name:34s} {s.reason}\n  {'':34s} -> {s.command}")
    counts = {st: sum(c.status == st for c in comparisons) for st in STATUSES}
    lines.append("== " + ", ".join(f"{k} {v}" for k, v in counts.items()))
    return lines


def exit_code(outcomes: Sequence[RunOutcome], comparisons: Sequence[Comparison]) -> int:
    if any(o.status == "crashed" for o in outcomes):
        return EXIT_CRASH
    if any(c.status in ("DIFF", "MISSING") for c in comparisons):
        return EXIT_DIFF
    return EXIT_OK


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: Runner = _default_runner,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    out: Callable[[str], None] = print,
    repo: Path = REPO,
) -> int:
    p = argparse.ArgumentParser(prog="experiments/run_all.sh", description=__doc__.split("\n\n")[0])
    p.add_argument("--sample", action="store_true", help="the sample bundle (host, SAMPLE stamp)")
    p.add_argument("--dry-run", action="store_true", help="print the plan, run nothing")
    p.add_argument("--only", nargs="+", default=None, help="run only these generators")
    args = p.parse_args(list(argv) if argv is not None else None)
    gens = sample_generators(repo) if args.sample else full_generators()
    if args.only:
        unknown = set(args.only) - {g.name for g in gens}
        if unknown:
            out(f"error: unknown generators {sorted(unknown)}")
            return EXIT_CRASH
        gens = [g for g in gens if g.name in args.only]
    rid = run_id(now(), args.sample)
    sandbox = sandbox_dir(rid, repo)
    if args.sample:
        out(f"*** {SAMPLE_STAMP} ***")
    out(f"run {rid}: sandbox {sandbox} (results/ is never written)")
    skips = skipped(args.sample)
    if args.dry_run:
        for g in gens:
            for a in g.commands:
                cmd = command_line(g, a, sandbox, repo)
                check_not_repo_results(cmd, repo)
                out(f"[dry-run] {g.name}: {shlex.join(cmd)}")
        for line in summary_lines([], [], skips):
            out(line)
        return EXIT_OK
    sandbox.mkdir(parents=True, exist_ok=False)
    for sub in ("results", "models", "data"):
        (sandbox / sub).mkdir()
    if args.sample:
        (sandbox / "SAMPLE").write_text(SAMPLE_STAMP + "\n", encoding="utf-8")
    outcomes: list[RunOutcome] = []
    comparisons: list[Comparison] = []
    for g in gens:
        out(f"== {g.name}" + (f"  ({g.note})" if g.note else ""))
        o = run_generator(g, sandbox, repo=repo, runner=runner, out=out)
        outcomes.append(o)
        comparisons += compare(g, sandbox, repo)
    lines = summary_lines(outcomes, comparisons, skips)
    (sandbox / "summary.txt").write_text(
        (f"{SAMPLE_STAMP}\n" if args.sample else "") + "\n".join(lines) + "\n", encoding="utf-8"
    )
    for line in lines:
        out(line)
    rc = exit_code(outcomes, comparisons)
    out(
        f"exit {rc} (0 all OK, 1 DIFF/MISSING, 2 a generator crashed); "
        f"summary {sandbox}/summary.txt"
    )
    return rc


if __name__ == "__main__":
    sys.exit(main())
