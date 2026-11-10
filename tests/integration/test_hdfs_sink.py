"""HdfsSink against the real HttpFS gateway from the host (needs the hdfs profile up).

This proves the host never has to resolve datanode hostnames: all bytes flow through HttpFS.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from pathlib import Path

import pandas as pd
import pytest
import requests

from f1sa.config import Settings
from f1sa.ingest.sinks import MANIFEST_NAME, HdfsSink

pytestmark = pytest.mark.integration
REPO_ROOT = Path(__file__).resolve().parents[2]


def _httpfs_up(settings: Settings) -> bool:
    try:
        resp = requests.get(
            f"{settings.hdfs_httpfs_url}/webhdfs/v1/",
            params={"op": "GETFILESTATUS", "user.name": settings.hdfs_user},
            timeout=5,
        )
    except requests.RequestException:
        return False
    return resp.status_code == 200


@pytest.fixture
def sink() -> HdfsSink:
    settings = Settings()
    if not _httpfs_up(settings):
        pytest.skip("HttpFS not reachable (start the hdfs profile: `make up-full`)")
    return HdfsSink(f"hdfs://namenode:8020/f1-it/{uuid.uuid4().hex}", settings)


def _cleanup(sink: HdfsSink) -> None:
    sink._call("DELETE", sink.path, "DELETE", params={"recursive": "true"})


def test_write_read_roundtrip_through_httpfs(sink: HdfsSink) -> None:
    try:
        payload = f"f1sa {uuid.uuid4()}".encode()
        written = sink.write_bytes("probe.txt", payload)
        assert sink.read_bytes("probe.txt") == payload
        assert written.bytes == len(payload)

        sink.write_bytes("probe.txt", b"replaced")  # overwrite path (DELETE + RENAME)
        assert sink.read_bytes("probe.txt") == b"replaced"

        df = pd.DataFrame({"lap": [1, 2, 3], "driver_id": ["a", "b", "c"]})
        sink.write_parquet("laps.parquet", df)
        sink.write_manifest({"source": "integration-test"})
        manifest = json.loads(sink.read_bytes(MANIFEST_NAME))
        assert {f["name"] for f in manifest["files"]} == {"probe.txt", "laps.parquet"}
        assert sink.cleanup_temp() == 0  # no temp files left behind
    finally:
        _cleanup(sink)


def test_file_is_visible_to_hdfs_cli(sink: HdfsSink) -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not installed")
    try:
        sink.write_bytes("cli.txt", b"seen by namenode\n")
        cat = subprocess.run(
            [
                "docker",
                "compose",
                "--profile",
                "full",
                "exec",
                "-T",
                "namenode",
                "hdfs",
                "dfs",
                "-cat",
                f"{sink.path}/cli.txt",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert cat.returncode == 0, cat.stderr
        assert cat.stdout == "seen by namenode\n"
    finally:
        _cleanup(sink)
