"""Guard for the committed ``results/`` folder: any test run that deletes, overwrites or adds a file
there fails (``tests/conftest.py`` snapshots it at session start and compares at session end).

The snapshot uses ``os.stat`` only (size, mtime in ns, inode): it never opens a results file, so
protected test-run outputs are never read by the guard. An atomic tmp-and-replace write changes
the inode, an in-place write changes size or mtime, a delete removes the path.
"""

from __future__ import annotations

import os
from pathlib import Path

Signature = tuple[int, int, int]
"""(size in bytes, mtime in ns, inode)"""


def results_snapshot(root: Path) -> dict[str, Signature]:
    """Every file under ``root`` (relative POSIX path -> signature); empty if ``root`` is absent.
    Symlinks are recorded by their own ``lstat`` and never followed."""
    if not root.is_dir():
        return {}
    snap: dict[str, Signature] = {}
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(dirpath)
        for name in [*filenames, *(d for d in dirnames if (base / d).is_symlink())]:
            path = base / name
            st = path.lstat()
            snap[path.relative_to(root).as_posix()] = (st.st_size, st.st_mtime_ns, st.st_ino)
    return snap


def results_changes(
    before: dict[str, Signature], after: dict[str, Signature]
) -> dict[str, list[str]]:
    """``{"deleted": [...], "changed": [...], "added": [...]}``: sorted names, empty if equal."""
    return {
        "deleted": sorted(before.keys() - after.keys()),
        "changed": sorted(k for k in before.keys() & after.keys() if before[k] != after[k]),
        "added": sorted(after.keys() - before.keys()),
    }


def describe_changes(changes: dict[str, list[str]]) -> str:
    """One line per touched file (names only), or '' when nothing changed."""
    lines = [f"  {kind}: results/{name}" for kind, names in changes.items() for name in names]
    if not lines:
        return ""
    return "\n".join(
        [
            "the test session touched the committed results/ folder (names only):",
            *lines,
            "No test may delete, overwrite or add anything in results/. If a make target was "
            "writing results/ while the tests ran, rerun the tests after it finishes.",
        ]
    )
