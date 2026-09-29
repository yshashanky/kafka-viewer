from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from kafka import TopicPartition

from kafka_viewer.certification.adapters.kafka import KafkaAdapter, load_connections
from kafka_viewer.certification.models import InfrastructureError, IncompleteScope
from kafka_viewer.certification.scope import Scope, to_millis


class Consumer:
    def __init__(self, records=None, beginnings=None):
        self.records = records or {}
        self.beginnings = beginnings or {0: 0}
        self.positions, self.paused = {}, set()
        self.closed, self.calls = False, []

    def topics(self):
        return {"topic", "other"}

    def partitions_for_topic(self, topic):
        return set(self.beginnings) if topic in self.topics() else None

    def assign(self, partitions):
        self.calls.append(("assign", partitions))

    def beginning_offsets(self, partitions):
        return {p: self.beginnings[p.partition] for p in partitions}

    def end_offsets(self, partitions):
        return {p: max((r.offset + 1 for r in self.records.get(p.partition, [])), default=self.beginnings[p.partition]) for p in partitions}

    def offsets_for_times(self, times):
        self.calls.append(("times", times))
        return {p: next((SimpleNamespace(offset=r.offset) for r in self.records.get(p.partition, [])
                         if r.timestamp >= t), None) for p, t in times.items()}

    def seek(self, p, offset):
        self.positions[p] = offset

    def position(self, p):
        return self.positions[p]

    def pause(self, *partitions):
        self.paused.update(partitions)

    def poll(self, timeout_ms, max_records):
        self.calls.append(("poll", max_records))
        batch = {}
        remaining = max_records
        for p, offset in self.positions.items():
            if p in self.paused:
                continue
            rows = [r for r in self.records.get(p.partition, []) if r.offset >= offset][:remaining]
            if rows:
                batch[p] = rows
                self.positions[p] = rows[-1].offset + 1
                remaining -= len(rows)
        return batch

    def close(self):
        self.closed = True

    def commit(self):
        pytest.fail("Kafka offset commits are forbidden")


def message(offset, timestamp, partition=0):
    return SimpleNamespace(offset=offset, timestamp=timestamp, partition=partition, value=str(offset).encode())


def adapter(consumer, **kwargs):
    def factory(**config):
        assert config["enable_auto_commit"] is False
        assert config["allow_auto_create_topics"] is False
        assert config["auto_offset_reset"] == "none"
        assert config["value_deserializer"] is None
        consumer.config = config
        return consumer
    return KafkaAdapter({"kafka.bootstrap.servers": "localhost:9092"}, consumer_factory=factory, **kwargs)


def test_entire_retained_multiple_partitions_and_batching():
    consumer = Consumer({0: [message(i, i) for i in range(5, 1205)],
                         1: [message(20, 1, 1)]}, {0: 5, 1: 20})
    rows = list(adapter(consumer, batch_size=100).read("topic", Scope("ENTIRE_RETAINED_DATA")))
    assert len(rows) == 1201
    assert rows[0].metadata["offset"] == 5
    assert rows[-1].metadata["partition"] == 1
    assert len([c for c in consumer.calls if c[0] == "poll"]) == 13
    assert consumer.closed


def test_time_range_boundaries_and_out_of_order_timestamps():
    consumer = Consumer({0: [message(0, 10), message(1, 20), message(2, 30), message(3, 25)]})
    rows = list(adapter(consumer).read("topic", Scope(start_ms=20, end_ms=30)))
    assert [r.metadata["offset"] for r in rows] == [1, 3]
    assert consumer.calls[1][0] == "times"
    assert consumer.closed


def test_independent_windows():
    source = Consumer({0: [message(0, 100)]})
    destination = Consumer({0: [message(0, 300)]})
    assert len(list(adapter(source).read("topic", Scope(start_ms=100, end_ms=200)))) == 1
    assert len(list(adapter(destination).read("topic", Scope(start_ms=300, end_ms=400)))) == 1


@pytest.mark.parametrize("scope", [Scope("ENTIRE_RETAINED_DATA"), Scope(start_ms=100, end_ms=200)])
def test_empty_topic_and_window(scope):
    consumer = Consumer()
    assert list(adapter(consumer).read("topic", scope)) == []
    assert consumer.closed
    assert not any(c[0] == "poll" for c in consumer.calls)


def test_unavailable_topic_and_topic_listing():
    consumer = Consumer()
    assert adapter(consumer).topics() == {"topic", "other"}
    assert consumer.closed
    consumer = Consumer()
    with pytest.raises(InfrastructureError):
        list(adapter(consumer).read("unavailable", Scope("ENTIRE_RETAINED_DATA")))
    assert consumer.closed


def test_stalled_scan_cannot_pass():
    consumer = Consumer({0: [message(0, 100)]})
    consumer.poll = lambda **kwargs: {}
    with pytest.raises(IncompleteScope):
        list(adapter(consumer, max_idle_polls=2).read("topic", Scope("ENTIRE_RETAINED_DATA")))
    assert consumer.closed


def test_snapshot_ignores_new_records():
    consumer = Consumer({0: [message(0, 100)]})
    original_poll = consumer.poll
    def poll(**kwargs):
        consumer.records[0].append(message(1, 101))
        return original_poll(**kwargs)
    consumer.poll = poll
    rows = list(adapter(consumer).read("topic", Scope("ENTIRE_RETAINED_DATA")))
    assert len(rows) == 1


def test_retention_change_is_incomplete():
    consumer = Consumer({0: [message(0, 100)]})
    original = consumer.beginning_offsets
    calls = []
    def beginnings(partitions):
        calls.append(True)
        if len(calls) == 2:
            consumer.beginnings[0] = 1
        return original(partitions)
    consumer.beginning_offsets = beginnings
    with pytest.raises(IncompleteScope):
        list(adapter(consumer).read("topic", Scope("ENTIRE_RETAINED_DATA")))


def test_connection_modes_security_independence_and_group_ids(tmp_path):
    source_path, destination_path = tmp_path / "source.properties", tmp_path / "destination.properties"
    source_path.write_text("bootstrap.servers=source.invalid:9092\nschema.registry.url=https://sr-source.invalid\n", encoding="utf-8")
    destination_path.write_text("kafka.bootstrap.servers=destination.invalid:9092\nschema.registry.url=https://sr-dest.invalid\n", encoding="utf-8")
    source, same = load_connections(source_path)
    assert source == same and source is not same
    source, destination = load_connections(source_path, destination_path)
    assert source["kafka.bootstrap.servers"] != destination["kafka.bootstrap.servers"]
    assert source["schema.registry.url"] != destination["schema.registry.url"]
    consumer = Consumer()
    configured = KafkaAdapter({**source, "kafka.group.id": "allowed-config"}, "allowed-override",
                              consumer_factory=lambda **kwargs: (setattr(consumer, "config", kwargs) or consumer))
    configured.topics()
    assert consumer.config["group_id"] == "allowed-override"
    assert "schema.registry.url" not in consumer.config
    assert KafkaAdapter({**source, "kafka.group.id": "allowed-config"}).config["group_id"] == "allowed-config"
    assert "group_id" not in KafkaAdapter(source).config


def test_timezone_conversion_and_dst():
    assert to_millis(datetime(2026, 1, 1, 5, 30), "Asia/Kolkata") == to_millis(datetime(2026, 1, 1), "UTC")
    for date in (datetime(2026, 3, 8, 2, 30), datetime(2026, 11, 1, 1, 30)):
        with pytest.raises(ValueError):
            to_millis(date, "America/New_York")
    aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert to_millis(aware) == int(aware.timestamp() * 1000)
    local = datetime(2026, 1, 1)
    assert to_millis(local) == int(local.astimezone().timestamp() * 1000)
    with pytest.raises(ValueError):
        to_millis(local, "No/SuchZone")


@pytest.mark.parametrize("kwargs", [{}, {"start_ms": 10, "end_ms": 10}, {"start_ms": 20, "end_ms": 10}, {"mode": "MAX_RECORDS"}])
def test_invalid_scopes(kwargs):
    with pytest.raises(ValueError):
        Scope(**kwargs)


def test_missing_timestamp_in_time_scan_and_sanitized_close():
    consumer = Consumer({0: [message(0, 100), message(1, -1)]})
    with pytest.raises(IncompleteScope):
        list(adapter(consumer).read("topic", Scope(start_ms=100, end_ms=200)))
    consumer = Consumer()
    def broken_close():
        raise RuntimeError("password=PRIVATE-CONFIG")
    consumer.close = broken_close
    with pytest.raises(InfrastructureError) as error:
        adapter(consumer).topics()
    assert "PRIVATE-CONFIG" not in str(error.value)


def test_scan_interrupt_closes_consumer():
    consumer = Consumer({0: [message(0, 10), message(1, 20)]})
    stream = adapter(consumer).read("topic", Scope("ENTIRE_RETAINED_DATA"))
    next(stream)
    stream.close()
    assert consumer.closed


def test_partition_change_is_incomplete():
    consumer = Consumer({0: [message(0, 100)]})
    calls = []
    def partitions(topic):
        calls.append(True)
        return {0} if len(calls) == 1 else {0, 1}
    consumer.partitions_for_topic = partitions
    with pytest.raises(IncompleteScope):
        list(adapter(consumer).read("topic", Scope("ENTIRE_RETAINED_DATA")))
