#!/usr/bin/env bash
# Format the NameNode on first start only (empty volume), then run it in the foreground.
set -euo pipefail
NAME_DIR=/opt/hadoop/data/name
if [[ ! -f "${NAME_DIR}/current/VERSION" ]]; then
  echo "[start-namenode] no existing namespace in ${NAME_DIR}; formatting"
  hdfs namenode -format -nonInteractive -clusterId "${HDFS_CLUSTER_ID:-f1sa-dev}"
else
  echo "[start-namenode] existing namespace found; skipping format"
fi
exec hdfs namenode
