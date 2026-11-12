"""``python -m f1sa.api`` (``make api``, the compose ``api`` service): uvicorn on
``API_HOST:API_PORT`` with JSON logging.

Before uvicorn starts, the port is checked. If something already listens there (typically the
compose ``api`` container on 8000 while ``make api`` runs on the host), the process holding it is
named, if it can be found, together with what to do, and the exit code is 2 instead of uvicorn's
bare "address already in use". ``make api`` uses ``API_PORT=8001`` by default for that reason.
"""

from __future__ import annotations

import errno
import logging
import shutil
import socket
import subprocess
import sys
from collections.abc import Callable
from typing import Any

import uvicorn

from f1sa.api.app import create_app
from f1sa.api.log import configure_logging
from f1sa.config import Settings, get_settings

log = logging.getLogger("f1sa.api")


class PortInUseError(RuntimeError):
    """The API port is already bound by another process."""


def uvicorn_options(settings: Settings) -> dict[str, Any]:
    return {
        "host": settings.api_host,
        "port": settings.api_port,
        "log_config": None,
        "access_log": False,
        "server_header": False,
        "proxy_headers": False,
    }


def port_holder(port: int) -> str | None:
    """Who listens on ``port``: a local process (psutil) or a Docker container publishing it."""
    try:
        import psutil  # type: ignore[import-untyped]  # no stubs (types-psutil not a dependency)

        for conn in psutil.net_connections(kind="tcp"):
            if conn.status == psutil.CONN_LISTEN and conn.laddr and conn.laddr.port == port:
                if conn.pid is None:
                    continue
                proc = psutil.Process(conn.pid)
                return f"pid {conn.pid} ({' '.join(proc.cmdline())[:200] or proc.name()})"
    except Exception:  # psutil may lack permission (other users' sockets)
        pass
    docker = shutil.which("docker")
    if docker is not None:
        try:
            out = subprocess.run(
                [docker, "ps", "--filter", f"publish={port}", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            ).stdout.split()
        except (OSError, subprocess.TimeoutExpired):
            out = []
        if out:
            return f"Docker container {', '.join(out)}"
    return None


def check_port_free(
    host: str, port: int, *, holder: Callable[[int], str | None] = port_holder
) -> None:
    """Raise ``PortInUseError`` with the holder and the remedy if ``host:port`` cannot be bound."""
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise
            who = holder(port) or f"an unknown process (try: ss -ltnp 'sport = :{port}')"
            raise PortInUseError(
                f"port {host}:{port} is already in use by {who}. "
                f"Run on another port (make api API_PORT={port + 1}), or stop the holder "
                "(for the compose service: docker compose stop api)."
            ) from exc


def main(
    *,
    run: Callable[..., None] = uvicorn.run,
    check: Callable[[str, int], None] = check_port_free,
) -> int:
    configure_logging()
    settings = get_settings()
    try:
        check(settings.api_host, settings.api_port)
    except PortInUseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    log.info(
        "starting API",
        extra={
            "host": settings.api_host,
            "port": settings.api_port,
            "lake_scheme": settings.data_lake_uri.split("://", 1)[0],
            "replay_enabled": settings.enable_replay,
            "debug_enabled": settings.enable_debug,
            "allow_test_season": settings.allow_test_season,
        },
    )
    run(create_app(settings), **uvicorn_options(settings))
    return 0


if __name__ == "__main__":
    sys.exit(main())
