from datetime import datetime, timezone

import pytest
from kafka import TopicPartition

from kafka_viewer.kafka_client import (
    KafkaClient,
    format_value,
    format_message_value,
    generate_group_id,
    safe_raw_payload,
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
    default_records = {
        TopicPartition("events", 0): [Record("events", 0, 0, 1000, None, b'{"a":1}'), Record("events", 0, 1, 3000, b"k", b"last-0")],
        TopicPartition("events", 1): [Record("events", 1, 0, 2000, b"k", b"last-1")],
    }
    records = default_records
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
    FakeConsumer.records = {partition: list(records) for partition, records in FakeConsumer.default_records.items()}
    FakeSchemaRegistry.instances.clear()
    FakeAvroDeserializer.calls.clear()


class FakeSchemaRegistry:
    instances = []

    def __init__(self, config):
        self.config = config
        self.__class__.instances.append(self)


class FakeAvroDeserializer:
    calls = []

    def __init__(self, registry):
        self.registry = registry

    def __call__(self, value, context):
        self.__class__.calls.append(value)
        if value == b"invalid":
            raise ValueError("invalid payload")
        return {"decoded": value.decode("utf-8")}


def test_latest_reads_count_total_across_partitions():
    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=FakeConsumer).load_messages("events", "group", "latest", 2)

    assert [(message.partition, message.offset) for message in messages] == [(1, 0), (0, 1)]
    assert FakeConsumer.instances[-1].kwargs["enable_auto_commit"] is False
    assert FakeConsumer.instances[-1].closed is True


def test_client_passes_only_explicit_consumer_configuration():
    KafkaClient(
        {"bootstrap_servers": "broker:9092", "unsupported_setting": "ignored"},
        consumer_factory=FakeConsumer,
    ).topics()

    assert "unsupported_setting" not in FakeConsumer.instances[-1].kwargs


def test_schema_registry_deserializer_is_not_created_when_unconfigured():
    def fail_if_created(_config):
        raise AssertionError("Schema Registry should be optional")

    KafkaClient({"bootstrap_servers": "broker:9092"}, schema_registry_factory=fail_if_created, consumer_factory=FakeConsumer)


def test_schema_registry_deserializes_values_and_receives_config():
    client = KafkaClient(
        {"bootstrap_servers": "broker:9092"},
        {"url": "https://registry.example", "basic.auth.user.info": "test-user:test-password"},
        FakeConsumer,
        FakeSchemaRegistry,
        FakeAvroDeserializer,
    )

    messages = client.load_messages("events", "group", "beginning", 3)

    assert {"decoded": '{"a":1}'} in [message.value for message in messages]
    assert b'{"a":1}' in FakeAvroDeserializer.calls
    assert FakeSchemaRegistry.instances[-1].config["url"] == "https://registry.example"
    assert "schema_id" not in FakeSchemaRegistry.instances[-1].config


def test_schema_registry_properties_never_reach_kafka_consumer():
    client = KafkaClient(
        {"bootstrap_servers": "broker:9092", "enable_auto_commit": False},
        {"url": "https://registry.example"},
        consumer_factory=FakeConsumer,
        schema_registry_factory=FakeSchemaRegistry,
        avro_deserializer_factory=FakeAvroDeserializer,
    )

    client.topics()

    assert all(not key.startswith("schema.registry") for key in FakeConsumer.instances[-1].kwargs)


def test_avro_failure_keeps_message_and_processes_subsequent_records():
    FakeConsumer.records[TopicPartition("events", 0)] = [
        Record("events", 0, 0, 1000, None, b"invalid"),
        Record("events", 0, 1, 2000, None, b"valid"),
    ]
    client = KafkaClient(
        {"bootstrap_servers": "broker:9092"},
        {"url": "https://registry.example"},
        FakeConsumer,
        FakeSchemaRegistry,
        FakeAvroDeserializer,
    )

    messages = client.load_messages("events", "group", "beginning", 3)

    assert messages[0].value_error == "Unable to deserialize Avro message value"
    assert messages[0].raw_value == "invalid"
    assert any(message.value == {"decoded": "valid"} for message in messages)
    assert "Avro deserialization failed" in format_message_value(messages[0])


def test_binary_and_large_raw_payloads_are_safe_and_bounded():
    raw = bytes([0, 255]) * 5000

    representation = safe_raw_payload(raw)

    assert len(representation) <= 8192
    assert representation.endswith("...[truncated]")


def test_from_beginning_is_bounded():
    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=FakeConsumer).load_messages("events", "group", "beginning", 2)

    assert len(messages) == 2


def test_date_range_filters_record_timestamps():
    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=FakeConsumer).load_messages(
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
