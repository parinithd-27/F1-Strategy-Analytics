#!/usr/bin/env python3
"""Check that every file named in ``docs/traceability.md`` exists (Step 17, commitment #42).

Reads the matrix's last column ("File or module") and checks each backticked reference:

* **paths** (``lake/writer.py``, ``tests/unit/test_keys.py``, ``web/src/components/``,
  ``docker/web/Dockerfile``): tried at the repo root and under ``src/f1sa``, ``tests/unit``,
  ``tests/integration``, ``experiments`` and ``web``, and next to the previous path of the same cell
  (``features/gaps.py``, ``stints.py``). ``{a,b}`` expands (an empty alternative too), other
  ``{name}`` / ``<name>`` placeholders and ``*`` match anything, a trailing ``/`` needs a folder;
* ``file.py::test_name``: the test must be defined in that file;
* ``module.attr`` (``serving/keys.order_key``): ``module.py`` must define ``attr``;
* ``make <target>``: the Makefile must define the target.

Not failed, but printed with the reason (nothing is skipped silently):

* GENERATED: gitignored outputs that a clone does not have (``data/...`` except ``data/sample``,
  ``models/...``); they are written by make targets;
* PENDING: rows of build steps not done yet (``--pending``, default ``S18``).

Other backticked text (Redis keys, topic and table names, flags, lake paths such as ``/f1/bench``
or ``raw/.../season=2024/``) is not a file reference and is not checked. Exit 0 if every
reference resolves, 1 otherwise, 2 if the matrix cannot be read.
"""

from __future__ import annotations

import argparse
import itertools
import re
import sys
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MATRIX = Path("docs/traceability.md")
BASES = ("", "src/f1sa", "tests/unit", "tests/integration", "experiments", "web")
EXTENSIONS = (
    ".py", ".sql", ".md", ".json", ".csv", ".png", ".jsonl", ".yml", ".yaml", ".ts", ".tsx",
    ".mjs", ".sh", ".cff", ".txt", ".toml", ".lock",
)  # fmt: skip
GENERATED_PREFIXES = ("data/", "models/")
NOT_GENERATED = ("data/sample/",)
DEFAULT_PENDING = ("S18",)
TICKS = re.compile(r"`([^`]+)`")
BRACES = re.compile(r"\{([^{}]*)\}")
PLACEHOLDER = re.compile(r"<[^<>]*>")
STEP = re.compile(r"\bS\d+[A-Z]?\b")
DEFINITION = r"^\s*(?:async\s+def|def|class)\s+{name}\b|^\s*{name}\s*(?::[^=]*)?="
MAKE_TARGET = r"^{name}\s*:(?!=)|^[^#\n:=]*\s{name}\s*:(?!=)"


@dataclass(frozen=True)
class Finding:
    row: str
    ref: str
    status: str  # OK | MISSING | GENERATED | PENDING
    detail: str = ""


# ------------------------------------------------------------------ parsing
def rows(text: str) -> Iterator[tuple[str, str, str]]:
    """``(number, steps, last cell)`` for every data row of the matrix table."""
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 4 or not cells[0].isdigit():
            continue
        yield cells[0], cells[2], cells[-1]


def expand_braces(ref: str) -> list[str]:
    """``a_{x,y}{,_z}.csv`` -> every combination; a brace group without a comma is a
    placeholder and becomes ``*``."""
    parts: list[list[str]] = []
    pos = 0
    for m in BRACES.finditer(ref):
        parts.append([ref[pos : m.start()]])
        body = m.group(1)
        parts.append(body.split(",") if "," in body else ["*"])
        pos = m.end()
    parts.append([ref[pos:]])
    return ["".join(p) for p in itertools.product(*parts)]


def _has_extension(name: str) -> bool:
    return name.endswith(EXTENSIONS) or name.endswith(".*") or name in ("Makefile", "Dockerfile")


def attr_parts(ref: str) -> tuple[str, str] | None:
    """``pkg/module.attr`` -> (``pkg/module``, ``attr``); ``None`` for anything else."""
    if "/" not in ref or ref.endswith("/") or ":" in ref or " " in ref:
        return None
    name = ref.rsplit("/", 1)[-1]
    if _has_extension(name) or name.count(".") != 1:
        return None
    module, attr = ref.rsplit(".", 1)
    return (module, attr) if attr.isidentifier() else None


def is_path_like(ref: str) -> bool:
    """A repository path: not a lake path (``/f1/...``, ``raw/...``, ``season=``), flag, key or
    sentence, and either containing ``/`` or a known file name."""
    if ref.startswith(("/", "-", ".", "raw/")) or any(c in ref for c in "=: "):
        return False
    if "/" in ref:
        return True
    return _has_extension(ref)


def clean_ref(raw: str) -> str:
    """Drop trailing arguments: ``ingest/cli.py --sink hdfs`` -> ``ingest/cli.py``."""
    head = raw.strip().split(" ", 1)[0]
    return head if _has_extension(head.rsplit("/", 1)[-1]) else raw.strip()


# ------------------------------------------------------------------ resolution
def _glob(root: Path, pattern: str) -> list[Path]:
    pattern = PLACEHOLDER.sub("*", pattern)
    if any(ch in pattern for ch in "*?["):
        return sorted(root.glob(pattern))
    p = root / pattern
    return [p] if p.exists() else []


def resolve(repo: Path, ref: str, prev_dir: str | None) -> list[Path]:
    """Every match of a path reference; empty unless every ``{a,b}`` alternative matches.

    A bare name (no ``/``) is tried next to the previous path of the cell first."""
    want_dir = ref.endswith("/")
    bases = list(BASES)
    if prev_dir is not None and "/" not in ref.rstrip("/"):
        bases.insert(0, prev_dir)
    matches: list[Path] = []
    for alt in expand_braces(ref.rstrip("/")):
        for base in bases:
            hits = [h for h in _glob(repo / base, alt) if h.is_dir() or not want_dir]
            if hits:
                matches.extend(hits)
                break
        else:
            return []
    return matches


def _defines(path: Path, name: str) -> bool:
    text = path.read_text(encoding="utf-8")
    return re.search(DEFINITION.format(name=re.escape(name)), text, re.MULTILINE) is not None


def check_test_ref(repo: Path, ref: str, prev_dir: str | None) -> tuple[bool, str]:
    file_part, test = ref.split("::", 1)
    hits = resolve(repo, file_part, prev_dir)
    if not hits:
        return False, f"{file_part} not found"
    name = test.split("[", 1)[0]
    if not _defines(hits[0], name):
        return False, f"{name} not defined in {hits[0].relative_to(repo)}"
    return True, str(hits[0].relative_to(repo))


def check_attr_ref(repo: Path, module: str, attr: str, prev_dir: str | None) -> tuple[bool, str]:
    hits = resolve(repo, module + ".py", prev_dir)
    if not hits:
        return False, f"{module}.py not found"
    if not _defines(hits[0], attr):
        return False, f"{attr} not defined in {hits[0].relative_to(repo)}"
    return True, str(hits[0].relative_to(repo))


def make_targets(repo: Path) -> set[str]:
    text = (repo / "Makefile").read_text(encoding="utf-8")
    targets: set[str] = set()
    for line in text.splitlines():
        if line.startswith(("\t", "#", " ")) or ":" not in line:
            continue
        lhs, _, rest = line.partition(":")
        if rest.startswith("="):
            continue
        targets.update(t for t in lhs.split() if not t.startswith(".") and "$" not in t)
    return targets


def is_generated(ref: str) -> bool:
    return ref.startswith(GENERATED_PREFIXES) and not ref.startswith(NOT_GENERATED)


def check_cell(repo: Path, row: str, cell: str, targets: set[str], pending: bool) -> list[Finding]:
    findings: list[Finding] = []
    prev_dir: str | None = None
    for raw in TICKS.findall(cell):
        ref = raw.strip()
        if ref.startswith("make "):
            words = ref.split()
            if len(words) < 2:
                continue
            target = words[1]
            if pending:
                findings.append(Finding(row, ref, "PENDING"))
            elif target in targets:
                findings.append(Finding(row, ref, "OK", "Makefile"))
            else:
                findings.append(Finding(row, ref, "MISSING", f"no Makefile target {target!r}"))
            continue
        ref = clean_ref(ref)
        if "::" in ref:
            if pending:
                findings.append(Finding(row, ref, "PENDING"))
                continue
            ok, detail = check_test_ref(repo, ref, prev_dir)
            findings.append(Finding(row, ref, "OK" if ok else "MISSING", detail))
            continue
        parts = attr_parts(ref)
        if parts is not None:
            if pending:
                findings.append(Finding(row, ref, "PENDING"))
                continue
            ok, detail = check_attr_ref(repo, *parts, prev_dir)
            findings.append(Finding(row, ref, "OK" if ok else "MISSING", detail))
            continue
        if not is_path_like(ref):
            continue
        if is_generated(ref):
            findings.append(Finding(row, ref, "GENERATED", "gitignored output of a make target"))
            continue
        if pending:
            findings.append(Finding(row, ref, "PENDING"))
            continue
        hits = resolve(repo, ref, prev_dir)
        if hits:
            first = hits[0]
            findings.append(Finding(row, ref, "OK", str(first.relative_to(repo))))
            parent = first if first.is_dir() else first.parent
            prev_dir = str(parent.relative_to(repo)) if parent != repo else ""
        else:
            findings.append(Finding(row, ref, "MISSING", "no such file or folder"))
    return findings


def check(repo: Path, matrix: Path, pending_steps: Sequence[str]) -> list[Finding]:
    text = (repo / matrix).read_text(encoding="utf-8")
    targets = make_targets(repo)
    findings: list[Finding] = []
    for number, steps, cell in rows(text):
        row_steps = set(STEP.findall(steps))
        pending = bool(row_steps) and row_steps <= set(pending_steps)
        findings.extend(check_cell(repo, number, cell, targets, pending))
    return findings


def report(findings: Iterable[Finding]) -> tuple[list[str], int]:
    lines: list[str] = []
    counts: dict[str, int] = {}
    for f in findings:
        counts[f.status] = counts.get(f.status, 0) + 1
        if f.status != "OK":
            suffix = f" ({f.detail})" if f.detail else ""
            lines.append(f"  {f.status:<9} #{f.row:<3} {f.ref}{suffix}")
    summary = ", ".join(
        f"{k} {counts[k]}" for k in ("OK", "GENERATED", "PENDING", "MISSING") if k in counts
    )
    lines.append(f"traceability: {summary}")
    return lines, counts.get("MISSING", 0)


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    p.add_argument("--repo", type=Path, default=REPO)
    p.add_argument("--matrix", type=Path, default=MATRIX)
    p.add_argument(
        "--pending",
        nargs="*",
        default=list(DEFAULT_PENDING),
        help="build steps not done yet; rows only of these steps are reported as PENDING",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="also list every OK reference")
    args = p.parse_args(list(argv) if argv is not None else None)
    try:
        findings = check(args.repo, args.matrix, args.pending)
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    lines, missing = report(findings)
    if args.verbose:
        for f in findings:
            if f.status == "OK":
                print(f"  OK        #{f.row:<3} {f.ref} -> {f.detail}")
    for line in lines:
        print(line)
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
