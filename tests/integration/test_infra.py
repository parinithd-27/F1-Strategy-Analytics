"""Integration checks for the Docker Compose infra (run after `make up` or `make up-full`)."""

from __future__ import annotations

import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest
import redis
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from f1sa.config import Settings

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_PARTITIONS = 3


def _compose(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--profile", "full", *args],
        cwd=REPO_ROOT,
        input=stdin,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _service_healthy(service: str) -> bool:
    if shutil.which("docker") is None:
        return False
    ps = _compose("ps", "-q", "--status", "running", service)
    cid = ps.stdout.strip()
    if ps.returncode != 0 or not cid:
        return False
    inspect = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Health.Status}}", cid.splitlines()[0]],
        capture_output=True,
        text=True,
        check=False,
    )
    return inspect.stdout.strip() == "healthy"


@pytest.fixture
def settings() -> Settings:
    return Settings()


def test_compose_config_valid() -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not installed")
    result = _compose("config", "--quiet")
    assert result.returncode == 0, result.stderr


def test_kafka_topic_exists_with_partitions(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    metadata = admin.list_topics(topic=settings.kafka_topic_events, timeout=15)
    topic = metadata.topics[settings.kafka_topic_events]
    assert topic.error is None, f"topic {settings.kafka_topic_events} missing: {topic.error}"
    assert len(topic.partitions) == EXPECTED_PARTITIONS


def test_kafka_roundtrip(settings: Settings) -> None:
    key = f"infra-test-{uuid.uuid4()}".encode()
    payload = f'{{"type":"infra_test","id":"{uuid.uuid4()}"}}'.encode()
    delivered: list[Message] = []

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        assert err is None, f"delivery failed: {err}"
        delivered.append(msg)

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "acks": "all"})
    producer.produce(
        settings.kafka_topic_events,
        key=key,
        value=payload,
        headers=[("test", b"infra")],
        on_delivery=on_delivery,
    )
    assert producer.flush(15) == 0, "producer did not flush within 15s"
    assert len(delivered) == 1
    partition, offset = delivered[0].partition(), delivered[0].offset()
    assert partition is not None and offset is not None

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"f1sa-infra-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign([TopicPartition(settings.kafka_topic_events, partition, offset)])
        deadline = time.monotonic() + 30
        received: Message | None = None
        while time.monotonic() < deadline and received is None:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            assert msg.error() is None, f"consume error: {msg.error()}"
            if msg.key() == key:
                received = msg
        assert received is not None, "message not consumed within 30s"
        assert received.value() == payload
        assert ("test", b"infra") in (received.headers() or [])
    finally:
        consumer.close()


def test_redis_ping(settings: Settings) -> None:
    client = redis.Redis.from_url(settings.redis_url, socket_timeout=5)
    try:
        assert client.ping() is True
    finally:
        client.close()


def test_hdfs_write_read() -> None:
    if not _service_healthy("namenode") or not _service_healthy("datanode"):
        pytest.skip("hdfs profile not running (start it with `make up-full`)")
    path = f"/tmp/f1sa-it/{uuid.uuid4()}.txt"
    payload = f"f1sa hdfs check {uuid.uuid4()}\n"
    put = _compose("exec", "-T", "namenode", "hdfs", "dfs", "-put", "-f", "-", path, stdin=payload)
    assert put.returncode == 0, put.stderr
    try:
        cat = _compose("exec", "-T", "namenode", "hdfs", "dfs", "-cat", path)
        assert cat.returncode == 0, cat.stderr
        assert cat.stdout == payload
    finally:
        _compose("exec", "-T", "namenode", "hdfs", "dfs", "-rm", "-f", "-skipTrash", path)


def test_httpfs_healthy_when_hdfs_profile_up() -> None:
    if not _service_healthy("namenode"):
        pytest.skip("hdfs profile not running (start it with `make up-full`)")
    assert _service_healthy("httpfs"), "httpfs must be healthy whenever the hdfs profile is up"
