"""Raw-zone sinks. A sink writes files into one partition directory plus a ``_manifest.json``.

Both sinks share one interface and the same guarantees:

* **atomic**: bytes go to a temporary name first and are renamed onto the final name only once
  complete, so readers never see a partial file; the temp file is removed on any failure,
  including Ctrl+C;
* **repeatable**: writing the same name again replaces the file;
* **verifiable**: every write returns size and sha256 for the ingestion manifest.

``FileSink`` writes ``file://`` partitions. ``HdfsSink`` writes ``hdfs://`` partitions through
the HttpFS gateway (WebHDFS REST that proxies the bytes itself, so the host never has to resolve
datanode hostnames). HttpFS's RENAME does not overwrite (it answers ``{"boolean": false}``), so
replacing an existing file is DELETE + RENAME, with the boolean result checked.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import tempfile
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import requests

from f1sa import __version__
from f1sa.config import Settings
from f1sa.lake.paths import hdfs_path, local_path

logger = logging.getLogger(__name__)

MANIFEST_NAME = "_manifest.json"
FILE_MODE = 0o644
_HDFS_TMP_PREFIX = "_tmp."
_HTTPFS_RETRY_STATUSES = frozenset({500, 502, 503, 504})


@dataclass(frozen=True)
class WrittenFile:
    name: str
    bytes: int
    sha256: str
    rows: int | None


class SinkError(RuntimeError):
    """A sink operation failed permanently."""


def _check_name(name: str) -> None:
    if not name or "/" in name or "\\" in name or name.startswith((".", "_tmp")):
        raise ValueError(f"invalid file name {name!r}")


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=1, default=str).encode("utf-8") + b"\n"


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


class RawSink(Protocol):
    """Writes files into one raw partition."""

    partition_uri: str
    files: list[WrittenFile]

    def write_bytes(self, name: str, data: bytes, rows: int | None = None) -> WrittenFile: ...

    def exists(self, name: str) -> bool: ...

    def cleanup_temp(self) -> int: ...

    def write_json(self, name: str, obj: Any, rows: int | None = None) -> WrittenFile: ...

    def write_parquet(self, name: str, df: pd.DataFrame) -> WrittenFile: ...

    def write_manifest(self, meta: dict[str, Any]) -> WrittenFile: ...


class _SinkHelpers:
    """Format helpers shared by the concrete sinks (they only implement ``write_bytes``)."""

    partition_uri: str
    files: list[WrittenFile]

    def write_bytes(self, name: str, data: bytes, rows: int | None = None) -> WrittenFile:
        raise NotImplementedError  # pragma: no cover - abstract

    def _record(self, written: WrittenFile) -> WrittenFile:
        if written.name != MANIFEST_NAME:
            self.files = [f for f in self.files if f.name != written.name] + [written]
        return written

    def write_json(self, name: str, obj: Any, rows: int | None = None) -> WrittenFile:
        return self.write_bytes(name, _json_bytes(obj), rows)

    def write_parquet(self, name: str, df: pd.DataFrame) -> WrittenFile:
        return self.write_bytes(name, _parquet_bytes(df), len(df))

    def write_manifest(self, meta: dict[str, Any]) -> WrittenFile:
        manifest = {
            "written_at": datetime.now(UTC).isoformat(),
            "f1sa_version": __version__,
            "partition": self.partition_uri,
            **meta,
            "files": [asdict(f) for f in sorted(self.files, key=lambda f: f.name)],
        }
        return self.write_bytes(MANIFEST_NAME, _json_bytes(manifest))


class FileSink(_SinkHelpers):
    """Local filesystem sink for a ``file://`` partition URI. Writes are atomic (temp + rename)."""

    def __init__(self, partition_uri: str) -> None:
        self.partition_uri = partition_uri
        self.directory: Path = local_path(partition_uri)
        self.files = []

    def write_bytes(self, name: str, data: bytes, rows: int | None = None) -> WrittenFile:
        if name != MANIFEST_NAME:
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
        return self._record(WrittenFile(name, len(data), hashlib.sha256(data).hexdigest(), rows))

    def exists(self, name: str) -> bool:
        return (self.directory / name).is_file()

    def cleanup_temp(self) -> int:
        """Remove temp files left behind by a killed process. Returns how many were removed."""
        if not self.directory.is_dir():
            return 0
        stale = list(self.directory.glob(".*.tmp"))
        for path in stale:
            path.unlink(missing_ok=True)
        return len(stale)


class HdfsSink(_SinkHelpers):
    """HDFS sink for an ``hdfs://`` partition URI, writing through the HttpFS REST gateway."""

    def __init__(
        self,
        partition_uri: str,
        settings: Settings,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        max_retries: int = 5,
        backoff_base_s: float = 0.5,
        timeout_s: float = 60.0,
    ) -> None:
        self.partition_uri = partition_uri
        self.path = hdfs_path(partition_uri)
        self.files = []
        self._base = f"{settings.hdfs_httpfs_url}/webhdfs/v1"
        self._user = settings.hdfs_user
        self._session = session or requests.Session()
        self._sleep = sleep
        self._max_retries = max_retries
        self._backoff_base_s = backoff_base_s
        self._timeout_s = timeout_s
        self._dir_ready = False

    # ------------------------------------------------------------------ REST plumbing
    def _call(
        self,
        method: str,
        path: str,
        op: str,
        *,
        params: dict[str, str] | None = None,
        data: bytes | None = None,
        ok: tuple[int, ...] = (200,),
    ) -> requests.Response:
        query = {"op": op, "user.name": self._user, **(params or {})}
        headers = {"Content-Type": "application/octet-stream"} if data is not None else {}
        url = f"{self._base}{path}"
        for attempt in range(self._max_retries + 1):
            last = attempt == self._max_retries
            try:
                resp = self._session.request(
                    method,
                    url,
                    params=query,
                    data=data,
                    headers=headers,
                    timeout=self._timeout_s,
                    allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if last:
                    raise SinkError(f"HttpFS {op} {path}: {type(exc).__name__}") from exc
                self._backoff(attempt, op, path, type(exc).__name__)
                continue
            if resp.status_code in ok:
                return resp
            if resp.status_code in _HTTPFS_RETRY_STATUSES and not last:
                self._backoff(attempt, op, path, f"HTTP {resp.status_code}")
                continue
            raise SinkError(
                f"HttpFS {op} {path} failed: HTTP {resp.status_code} {_remote_message(resp)}"
            )
        raise AssertionError("unreachable")  # pragma: no cover

    def _backoff(self, attempt: int, op: str, path: str, why: str) -> None:
        delay = self._backoff_base_s * (2**attempt)
        logger.warning("HttpFS %s %s: %s; retry in %.1fs", op, path, why, delay)
        self._sleep(delay)

    def _boolean(self, resp: requests.Response, what: str) -> bool:
        try:
            value = resp.json()["boolean"]
        except (ValueError, KeyError, TypeError) as exc:
            raise SinkError(f"HttpFS {what}: unexpected response {resp.text[:200]!r}") from exc
        return bool(value)

    def _status(self, path: str) -> dict[str, Any] | None:
        resp = self._call("GET", path, "GETFILESTATUS", ok=(200, 404))
        if resp.status_code == 404:
            return None
        status = resp.json().get("FileStatus")
        if not isinstance(status, dict):
            raise SinkError(f"HttpFS GETFILESTATUS {path}: unexpected response")
        return status

    def _delete(self, path: str) -> None:
        self._call("DELETE", path, "DELETE", params={"recursive": "false"})

    def _ensure_dir(self) -> None:
        if not self._dir_ready:
            resp = self._call("PUT", self.path, "MKDIRS")
            if not self._boolean(resp, f"MKDIRS {self.path}"):
                raise SinkError(f"HttpFS MKDIRS {self.path} returned false")
            self._dir_ready = True

    # ------------------------------------------------------------------ sink interface
    def write_bytes(self, name: str, data: bytes, rows: int | None = None) -> WrittenFile:
        if name != MANIFEST_NAME:
            _check_name(name)
        self._ensure_dir()
        final = f"{self.path}/{name}"
        tmp = f"{self.path}/{_HDFS_TMP_PREFIX}{uuid.uuid4().hex}.{name}"
        committed = False
        try:
            self._call(
                "PUT",
                tmp,
                "CREATE",
                params={"overwrite": "true", "data": "true"},
                data=data,
                ok=(200, 201),
            )
            status = self._status(tmp)
            if status is None or int(status.get("length", -1)) != len(data):
                raise SinkError(f"HttpFS wrote {status and status.get('length')} of {len(data)} B")
            if self._status(final) is not None:
                self._delete(final)  # RENAME never overwrites on HttpFS
            renamed = self._call("PUT", tmp, "RENAME", params={"destination": final})
            if not self._boolean(renamed, f"RENAME {tmp}"):
                raise SinkError(f"HttpFS RENAME {tmp} -> {final} returned false")
            committed = True
        finally:
            if not committed:
                try:
                    self._delete(tmp)
                except Exception as exc:  # best effort; cleanup_temp() catches leftovers
                    logger.warning("could not remove HDFS temp file %s: %s", tmp, exc)
        return self._record(WrittenFile(name, len(data), hashlib.sha256(data).hexdigest(), rows))

    def exists(self, name: str) -> bool:
        return self._status(f"{self.path}/{name}") is not None

    def read_bytes(self, name: str) -> bytes:
        """Read a file back (used by verification and integration tests)."""
        resp = self._call("GET", f"{self.path}/{name}", "OPEN")
        return resp.content

    def cleanup_temp(self) -> int:
        """Delete ``_tmp.*`` files left in this partition by a killed process."""
        resp = self._call("GET", self.path, "LISTSTATUS", ok=(200, 404))
        if resp.status_code == 404:
            return 0
        entries = resp.json().get("FileStatuses", {}).get("FileStatus", [])
        stale = [e["pathSuffix"] for e in entries if e["pathSuffix"].startswith(_HDFS_TMP_PREFIX)]
        for suffix in stale:
            self._delete(f"{self.path}/{suffix}")
        return len(stale)


def _remote_message(resp: requests.Response) -> str:
    try:
        return str(resp.json()["RemoteException"]["message"])[:300]
    except (ValueError, KeyError, TypeError):
        return resp.text[:300]


def make_sink(settings: Settings, sink: str, partition_uri: str) -> FileSink | HdfsSink:
    """Sink for ``partition_uri``. ``sink`` must agree with the URI scheme (from DATA_LAKE_URI)."""
    if sink == "file":
        return FileSink(partition_uri)
    if sink == "hdfs":
        return HdfsSink(partition_uri, settings)
    raise ValueError(f"unknown sink {sink!r}")


def check_sink_matches_lake(settings: Settings, sink: str) -> None:
    """Raise ValueError unless DATA_LAKE_URI's scheme matches the requested sink."""
    scheme = settings.data_lake_uri.split("://", 1)[0]
    if sink == "file" and scheme != "file":
        raise ValueError(
            f"--sink file needs a file:// DATA_LAKE_URI (got {settings.data_lake_uri})"
        )
    if sink == "hdfs" and scheme != "hdfs":
        raise ValueError(
            "--sink hdfs needs an hdfs:// DATA_LAKE_URI, e.g. "
            "DATA_LAKE_URI=hdfs://namenode:8020/f1 (bytes are sent through HDFS_HTTPFS_URL)"
        )
