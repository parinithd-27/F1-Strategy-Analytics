#!/usr/bin/env bash
# Idempotently create the race-events topic and verify it exists with the expected partitions.
set -euo pipefail
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:?KAFKA_INTERNAL_BOOTSTRAP is required}"
TOPIC="${KAFKA_TOPIC_EVENTS:?KAFKA_TOPIC_EVENTS is required}"
PARTITIONS="${KAFKA_TOPIC_EVENTS_PARTITIONS:-3}"
BIN=/opt/kafka/bin

"${BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
  --topic "${TOPIC}" --partitions "${PARTITIONS}" --replication-factor 1

desc="$("${BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}")"
echo "${desc}"
actual="$(grep -o 'PartitionCount: *[0-9]*' <<<"${desc}" | grep -o '[0-9]*$' || true)"
if [[ "${actual}" != "${PARTITIONS}" ]]; then
  echo "[create-topics] ERROR: ${TOPIC} has '${actual:-none}' partitions, expected ${PARTITIONS}" >&2
  exit 1
fi
echo "[create-topics] ${TOPIC} ready with ${PARTITIONS} partitions"
