"""Ingestion state in ``data/manifest.json``: one item per sink/source/season/round.

The manifest is rewritten atomically (temp file in the same directory, fsync, ``os.replace``,
fsync of the directory) after every status change, so a process killed at any point leaves either
the previous or the new version on disk, never a torn file. Items found in state ``running`` on
load belong to a process that died and are treated as not done.

An exclusive ``fcntl.flock`` on ``<manifest>.lock`` prevents two runs from updating the same
manifest concurrently; the OS releases it automatically if the process is killed.
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, Literal

from f1sa.lake.paths import is_within

Status = Literal["pending", "running", "done", "failed", "interrupted", "skipped"]
STATUSES: frozenset[str] = frozenset(
    {"pending", "running", "done", "failed", "interrupted", "skipped"}
)
MANIFEST_VERSION = 1
MAX_ERROR_CHARS = 2000
SEASON_SCOPE = "season"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def item_key(sink: str, source: str, season: int, round_: int | None) -> str:
    """``hdfs:jolpica:2021:5`` for a race item, ``hdfs:jolpica:2021:season`` for a season item."""
    return f"{sink}:{source}:{season}:{SEASON_SCOPE if round_ is None else round_}"


@dataclass
class FileRecord:
    uri: str
    rows: int | None
    bytes: int
    sha256: str


@dataclass
class Item:
    sink: str
    source: str
    season: int
    round: int | None
    status: Status = "pending"
    attempts: int = 0
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    note: str | None = None
    files: list[FileRecord] = field(default_factory=list)

    @property
    def key(self) -> str:
        return item_key(self.sink, self.source, self.season, self.round)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Item:
        status = data.get("status", "pending")
        if status not in STATUSES:
            raise ValueError(f"invalid status {status!r} in manifest")
        files = [FileRecord(**f) for f in data.get("files", [])]
        return cls(
            sink=data["sink"],
            source=data["source"],
            season=int(data["season"]),
            round=None if data.get("round") is None else int(data["round"]),
            status=status,
            attempts=int(data.get("attempts", 0)),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            error=data.get("error"),
            note=data.get("note"),
            files=files,
        )


class ManifestLockedError(RuntimeError):
    """Another ingestion process holds the manifest lock."""


class Manifest:
    """Load/update/save the ingestion manifest. Use as a context manager to hold the lock."""

    def __init__(self, path: Path, *, data_root: Path = Path("data")) -> None:
        if not is_within(path, data_root):
            raise ValueError(f"manifest path {path} must be inside {data_root}/")
        self.path = path
        self.items: dict[str, Item] = {}
        self.created_at = _now()
        self._lock_fh: IO[str] | None = None
        self._load()

    # ------------------------------------------------------------------ persistence
    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest {self.path} is not valid JSON: {exc}") from exc
        if raw.get("version") != MANIFEST_VERSION:
            raise ValueError(f"unsupported manifest version {raw.get('version')!r}")
        self.created_at = raw.get("created_at", self.created_at)
        for key, data in raw.get("items", {}).items():
            item = Item.from_dict(data)
            if item.key != key:
                raise ValueError(f"manifest key {key!r} does not match item {item.key!r}")
            self.items[key] = item

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": MANIFEST_VERSION,
            "created_at": self.created_at,
            "updated_at": _now(),
            "items": {k: asdict(v) for k, v in sorted(self.items.items())},
        }

    def save(self) -> None:
        """Atomically replace the manifest file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(self.to_dict(), indent=1, ensure_ascii=False).encode("utf-8") + b"\n"
        fd, tmp = tempfile.mkstemp(prefix=".manifest.", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.chmod(tmp, 0o644)
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        dir_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    # ------------------------------------------------------------------ locking
    def __enter__(self) -> Manifest:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path.with_name(self.path.name + ".lock"), "w")
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fh.close()
            raise ManifestLockedError(
                f"another ingestion run holds {self.path}.lock; wait for it to finish"
            ) from exc
        self._lock_fh = fh
        # reload under the lock: another run may have finished since we were constructed
        self.items.clear()
        self._load()
        # stale temp files from a killed save are harmless; remove them
        for stale in self.path.parent.glob(".manifest.*.tmp"):
            stale.unlink(missing_ok=True)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._lock_fh is not None:
            fcntl.flock(self._lock_fh, fcntl.LOCK_UN)
            self._lock_fh.close()
            self._lock_fh = None

    # ------------------------------------------------------------------ item state
    def get(self, sink: str, source: str, season: int, round_: int | None) -> Item:
        key = item_key(sink, source, season, round_)
        if key not in self.items:
            self.items[key] = Item(sink, source, season, round_)
        return self.items[key]

    def peek(self, sink: str, source: str, season: int, round_: int | None) -> Item | None:
        """The item if it exists, without creating it (used by --dry-run)."""
        return self.items.get(item_key(sink, source, season, round_))

    def mark_running(self, item: Item) -> None:
        item.status = "running"
        item.attempts += 1
        item.started_at = _now()
        item.finished_at = None
        item.error = None
        item.note = None
        self.save()

    def mark_done(self, item: Item, files: list[FileRecord]) -> None:
        item.status = "done"
        item.files = files
        item.finished_at = _now()
        item.error = None
        self.save()

    def mark_failed(self, item: Item, error: BaseException | str) -> None:
        text = error if isinstance(error, str) else f"{type(error).__name__}: {error}"
        item.status = "failed"
        item.error = text[:MAX_ERROR_CHARS]
        item.finished_at = _now()
        self.save()

    def mark_interrupted(self, item: Item) -> None:
        item.status = "interrupted"
        item.finished_at = _now()
        self.save()

    def mark_skipped(self, item: Item, note: str) -> None:
        item.status = "skipped"
        item.note = note
        item.finished_at = _now()
        self.save()

    @staticmethod
    def should_run(item: Item | None, *, only_failed: bool) -> bool:
        """Resume policy: default runs everything not done; --only-failed runs failed items only."""
        if item is None:
            return not only_failed
        if only_failed:
            return item.status == "failed"
        return item.status != "done"

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for item in self.items.values():
            out[item.status] = out.get(item.status, 0) + 1
        return out

    def __iter__(self) -> Iterator[Item]:
        return iter(self.items.values())
