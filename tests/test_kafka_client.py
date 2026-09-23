from datetime import datetime, timezone

import pytest
from kafka import TopicPartition

from kafka_viewer.kafka_client import (
    KafkaClient,
    format_value,
    generate_group_id,
    validate_count,
)


class Record:
    def __init__(self, topic, partition, offset, timestamp, key, value):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.key = key
        self.value = value


class Offset:
    def __init__(self, offset):
        self.offset = offset


class FakeConsumer:
    records = {
        TopicPartition("events", 0): [Record("events", 0, 0, 1000, None, b'{"a":1}'), Record("events", 0, 1, 3000, b"k", b"last-0")],
        TopicPartition("events", 1): [Record("events", 1, 0, 2000, b"k", b"last-1")],
    }
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.assigned = []
        self.positions = {}
        self.closed = False
        FakeConsumer.instances.append(self)

    def partitions_for_topic(self, topic):
        return {0, 1}

    def end_offsets(self, partitions):
        return {partition: len(self.records[partition]) for partition in partitions}

    def beginning_offsets(self, partitions):
        return {partition: 0 for partition in partitions}

    def offsets_for_times(self, requested):
        return {partition: Offset(0) for partition in requested}

    def assign(self, partitions):
        self.assigned = partitions

    def seek(self, partition, offset):
        self.positions[partition] = offset

    def poll(self, timeout_ms):
        batch = {}
        for partition in self.assigned:
            position = self.positions[partition]
            records = [record for record in self.records[partition] if record.offset >= position]
            if records:
                record = records[0]
                self.positions[partition] = record.offset + 1
                batch[partition] = [record]
        return batch

    def position(self, partition):
        return self.positions[partition]

    def close(self):
        self.closed = True

    def topics(self):
        return {"events"}


def setup_function():
    FakeConsumer.instances.clear()


def test_latest_reads_count_total_across_partitions():
    messages = KafkaClient("broker:9092", FakeConsumer).load_messages("events", "group", "latest", 2)

    assert [(message.partition, message.offset) for message in messages] == [(1, 0), (0, 1)]
    assert FakeConsumer.instances[-1].kwargs["enable_auto_commit"] is False
    assert FakeConsumer.instances[-1].closed is True


def test_from_beginning_is_bounded():
    messages = KafkaClient("broker:9092", FakeConsumer).load_messages("events", "group", "beginning", 2)

    assert len(messages) == 2


def test_date_range_filters_record_timestamps():
    messages = KafkaClient("broker:9092", FakeConsumer).load_messages(
        "events",
        "group",
        "range",
        10,
        datetime.fromtimestamp(1.5, timezone.utc),
        datetime.fromtimestamp(2.5, timezone.utc),
    )

    assert [(message.partition, message.offset) for message in messages] == [(1, 0)]


def test_format_value_json_and_fallback():
    assert format_value(b'{"name":"Ada"}') == '{\n  "name": "Ada"\n}'
    assert format_value(b"plain text") == "plain text"
    assert format_value(None) == "<null>"


def test_group_id_is_recognizable():
    assert generate_group_id().startswith("kafka-viewer-")


@pytest.mark.parametrize("value", [0, -1, "nope"])
def test_validate_count_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="positive integer"):
        validate_count(value)


def test_validate_count_accepts_positive_integer():
    assert validate_count("3") == 3
