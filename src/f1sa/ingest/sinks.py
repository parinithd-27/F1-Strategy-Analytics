"""Raw-zone sinks. Each sink writes one partition directory plus a ``_manifest.json``.

Only the ``file`` sink exists in this step; the HDFS sink (via HttpFS) is added in Step 4.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from f1sa import __version__
from f1sa.lake.paths import local_path

MANIFEST_NAME = "_manifest.json"
FILE_MODE = 0o644


@dataclass(frozen=True)
class WrittenFile:
    name: str
    bytes: int
    sha256: str
    rows: int | None


class RawSink(Protocol):
    """Writes files into one raw partition."""

    partition_uri: str

    def write_json(self, name: str, obj: Any, rows: int | None = None) -> WrittenFile: ...

    def write_parquet(self, name: str, df: pd.DataFrame) -> WrittenFile: ...

    def write_manifest(self, meta: dict[str, Any]) -> WrittenFile: ...


def _check_name(name: str) -> None:
    if not name or "/" in name or "\\" in name or name.startswith("."):
        raise ValueError(f"invalid file name {name!r}")


class FileSink:
    """Local filesystem sink for a ``file://`` partition URI. Writes are atomic (temp + rename)."""

    def __init__(self, partition_uri: str) -> None:
        self.partition_uri = partition_uri
        self.directory: Path = local_path(partition_uri)
        self.files: list[WrittenFile] = []

    def _write_bytes(self, name: str, data: bytes, rows: int | None) -> WrittenFile:
        _check_name(name)
        self.directory.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=f".{name}.", suffix=".tmp", dir=self.directory)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.chmod(tmp, FILE_MODE)  # mkstemp creates 0600; raw files are world-readable
            os.replace(tmp, self.directory / name)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        written = WrittenFile(name, len(data), hashlib.sha256(data).hexdigest(), rows)
        if name != MANIFEST_NAME:
            self.files = [f for f in self.files if f.name != name] + [written]
        return written

    def write_json(self, name: str, obj: Any, rows: int | None = None) -> WrittenFile:
        data = json.dumps(obj, ensure_ascii=False, indent=1, default=str).encode("utf-8")
        return self._write_bytes(name, data + b"\n", rows)

    def write_parquet(self, name: str, df: pd.DataFrame) -> WrittenFile:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        return self._write_bytes(name, buf.getvalue(), len(df))

    def write_manifest(self, meta: dict[str, Any]) -> WrittenFile:
        manifest = {
            "written_at": datetime.now(UTC).isoformat(),
            "f1sa_version": __version__,
            "partition": self.partition_uri,
            **meta,
            "files": [asdict(f) for f in sorted(self.files, key=lambda f: f.name)],
        }
        return self.write_json(MANIFEST_NAME, manifest)
