#!/usr/bin/env bash
# Wait until docker compose services are healthy (or running, if they define no healthcheck).
# Usage: scripts/wait_for.sh [-t TIMEOUT_SECONDS] SERVICE [SERVICE...]
# Exit codes: 0 all ready | 1 timeout / unhealthy / exited / unknown service | 2 usage error
set -euo pipefail

usage() { echo "usage: $(basename "$0") [-t TIMEOUT_SECONDS] SERVICE [SERVICE...]" >&2; exit 2; }

TIMEOUT=180
while getopts ":t:h" opt; do
  case "$opt" in
    t) TIMEOUT="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))
[[ "$TIMEOUT" =~ ^[0-9]+$ && "$TIMEOUT" -gt 0 ]] || { echo "timeout must be a positive integer" >&2; usage; }
[[ $# -ge 1 ]] || usage

command -v docker >/dev/null 2>&1 || { echo "docker not found on PATH" >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/.."
COMPOSE=(docker compose --profile full)

declare -A CID
for svc in "$@"; do
  if ! id="$("${COMPOSE[@]}" ps -a -q "$svc" 2>/dev/null)" || [[ -z "$id" ]]; then
    echo "[wait_for] no container for service '$svc' (is it started?)" >&2
    exit 1
  fi
  CID[$svc]="${id%%$'\n'*}"
done

deadline=$(( $(date +%s) + TIMEOUT ))
pending=("$@")
while ((${#pending[@]})); do
  still=()
  for svc in "${pending[@]}"; do
    read -r state health < <(docker inspect -f \
      '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${CID[$svc]}")
    if [[ "$health" == healthy || ( "$health" == none && "$state" == running ) ]]; then
      echo "[wait_for] $svc: ready ($state/$health)"
    elif [[ "$health" == unhealthy || "$state" == exited || "$state" == dead ]]; then
      echo "[wait_for] $svc: FAILED ($state/$health); last health log:" >&2
      docker inspect -f '{{if .State.Health}}{{range .State.Health.Log}}{{.Output}}{{end}}{{end}}' \
        "${CID[$svc]}" | tail -n 20 >&2 || true
      "${COMPOSE[@]}" logs --tail 30 "$svc" >&2 || true
      exit 1
    else
      still+=("$svc")
    fi
  done
  pending=("${still[@]+"${still[@]}"}")
  ((${#pending[@]})) || break
  if (( $(date +%s) >= deadline )); then
    echo "[wait_for] timed out after ${TIMEOUT}s waiting for: ${pending[*]}" >&2
    exit 1
  fi
  sleep 2
done
echo "[wait_for] all ready: $*"
