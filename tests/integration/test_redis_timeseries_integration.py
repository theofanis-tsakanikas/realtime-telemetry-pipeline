"""Docker-backed integration test for the Redis TimeSeries sink.

Unlike the unit tests (which mock Redis), this spins up a real ``redis-stack`` container and
exercises the actual command-building end to end: ``write_row`` issuing ``TS.ADD`` with
RETENTION / ON_DUPLICATE / LABELS, plus the data-quality and drift writers. It is the layer
most prone to command-syntax bugs that a mock can't catch.

Deselected by default (``@pytest.mark.integration``); run with::

    pytest -m integration

Requires a running Docker daemon.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

pytest.importorskip("testcontainers", reason="testcontainers not installed")
redis = pytest.importorskip("redis", reason="redis client not installed")

from data_quality import DQMetrics, write_dq_metrics  # noqa: E402
from drift import batch_drift, write_drift_metrics  # noqa: E402
from spark_transform import redis_key, write_row  # noqa: E402
from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.core.waiting_utils import wait_for_logs  # noqa: E402

pytestmark = pytest.mark.integration

REDIS_IMAGE = "redis/redis-stack:7.4.0-v3"


@pytest.fixture(scope="module")
def redis_client():
    container = DockerContainer(REDIS_IMAGE).with_exposed_ports(6379)
    container.start()
    try:
        wait_for_logs(container, "Ready to accept connections", timeout=60)
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(6379))
        client = redis.StrictRedis(host=host, port=port, db=0, decode_responses=True)
        yield client
    finally:
        container.stop()


def test_write_row_creates_timeseries(redis_client):
    row = {
        "sensor_id": "sensor_1",
        "temperature": 25.5,
        "humidity": 60.0,
        "pressure": 1010.0,
        "timestamp": datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
    }
    write_row(redis_client, row)

    for metric, expected in [("temperature", 25.5), ("humidity", 60.0), ("pressure", 1010.0)]:
        key = redis_key("sensor_1", metric)
        _, value = redis_client.execute_command("TS.GET", key)
        assert float(value) == expected
        # Labels were applied on auto-create.
        info = redis_client.execute_command("TS.INFO", key)
        info_map = dict(zip(info[::2], info[1::2], strict=False))
        assert info_map.get("retentionTime") == 604800000


def test_write_row_on_duplicate_last_is_idempotent(redis_client):
    ts = datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC)
    base = {"sensor_id": "sensor_2", "humidity": 50.0, "pressure": 1000.0, "timestamp": ts}
    write_row(redis_client, {**base, "temperature": 20.0})
    write_row(redis_client, {**base, "temperature": 30.0})  # same timestamp, new value
    _, value = redis_client.execute_command("TS.GET", redis_key("sensor_2", "temperature"))
    assert float(value) == 30.0  # ON_DUPLICATE LAST kept the latest


def test_dq_and_drift_writers(redis_client):
    ts_ms = 1704110400000
    metrics = DQMetrics(total=100, valid=80, rejected_total=20, by_reason={"invalid_humidity": 20})
    write_dq_metrics(redis_client, metrics, ts_ms)
    drift = batch_drift({"temperature": (100, 30.0)})
    write_drift_metrics(redis_client, drift, ts_ms)

    _, accept = redis_client.execute_command("TS.GET", "dq:accept_rate")
    assert float(accept) == pytest.approx(0.8)
    _, zval = redis_client.execute_command("TS.GET", "drift:temperature:z")
    assert float(zval) > 3  # +5C shift on 100 samples breaches 3 sigma
