import logging
from datetime import datetime, timedelta, timezone

import pytest
from kafka import TopicPartition

from kafka_viewer.kafka_client import (
    KafkaClient,
    FILTER_SCAN_CAP,
    Message,
    export_messages,
    format_value,
    format_message_value,
    generate_group_id,
    message_matches_filter,
    parse_filter,
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


class StatisticsConsumer:
    records = {}
    beginning = {}
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.assigned = []
        self.positions = {}
        self.closed = False
        self.assign_calls = []
        self.poll_calls = 0
        self.__class__.instances.append(self)

    def partitions_for_topic(self, topic):
        return {partition.partition for partition in self.records if partition.topic == topic}

    def beginning_offsets(self, partitions):
        return {partition: self.beginning[partition] for partition in partitions}

    def end_offsets(self, partitions):
        return {partition: self.beginning[partition] + len(self.records[partition]) for partition in partitions}

    def offsets_for_times(self, requested):
        result = {}
        for partition, timestamp in requested.items():
            first_offset = self.beginning[partition]
            for index, record in enumerate(self.records[partition]):
                if record.timestamp >= timestamp:
                    first_offset = self.beginning[partition] + index
                    break
            else:
                result[partition] = None
                continue
            result[partition] = Offset(first_offset)
        return result

    def assign(self, partitions):
        self.assigned = partitions
        self.assign_calls.append(list(partitions))

    def seek(self, partition, offset):
        self.positions[partition] = offset

    def poll(self, timeout_ms):
        self.poll_calls += 1
        batch = {}
        for partition in self.assigned:
            position = self.positions[partition]
            index = position - self.beginning[partition]
            if 0 <= index < len(self.records[partition]):
                record = self.records[partition][index]
                self.positions[partition] = position + 1
                batch[partition] = [record]
        return batch

    def close(self):
        self.closed = True


class GapStatisticsConsumer(StatisticsConsumer):
    def end_offsets(self, partitions):
        return {partition: 110 for partition in partitions}

    def poll(self, timeout_ms):
        batch = {}
        for partition in self.assigned:
            position = self.positions[partition]
            record = next((item for item in self.records[partition] if item.offset >= position), None)
            if record is not None:
                self.positions[partition] = record.offset + 1
                batch[partition] = [record]
        return batch


class PartitionedConsumer:
    records = {}
    beginning = {}
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.assigned = []
        self.positions = {}
        self.seeks = []
        self.__class__.instances.append(self)

    def partitions_for_topic(self, topic):
        return {partition.partition for partition in self.records if partition.topic == topic}

    def beginning_offsets(self, partitions):
        return {partition: self.beginning[partition] for partition in partitions}

    def end_offsets(self, partitions):
        return {partition: self.beginning[partition] + len(self.records[partition]) for partition in partitions}

    def assign(self, partitions):
        self.assigned = partitions

    def seek(self, partition, offset):
        self.positions[partition] = offset
        self.seeks.append((partition, offset))

    def poll(self, timeout_ms):
        batch = {}
        for partition in self.assigned:
            position = self.positions[partition]
            record = next((record for record in self.records[partition] if record.offset >= position), None)
            if record is not None:
                self.positions[partition] = record.offset + 1
                batch[partition] = [record]
        return batch

    def position(self, partition):
        return self.positions[partition]

    def close(self):
        pass


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
    StatisticsConsumer.instances.clear()
    PartitionedConsumer.instances.clear()


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


def test_message_filter_matches_payload_content():
    message = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=FakeConsumer).load_messages("events", "group", "beginning", 2)[1]

    assert message_matches_filter(message, "last-1") is True
    assert message_matches_filter(message, "missing") is False


@pytest.mark.parametrize(
    ("filter_text", "terms", "operator"),
    [
        ("payment", ("payment",), None),
        ("payment?failed?timeout", ("payment", "failed", "timeout"), "or"),
        (" payment ? failed ? timeout ", ("payment", "failed", "timeout"), "or"),
        ("payment&failed&timeout", ("payment", "failed", "timeout"), "and"),
        ("payment??failed", ("payment", "failed"), "or"),
        ("payment&&failed", ("payment", "failed"), "and"),
        ("?payment?failed?", ("payment", "failed"), "or"),
        ("&payment&failed&", ("payment", "failed"), "and"),
    ],
)
def test_parse_filter_terms_and_operator(filter_text, terms, operator):
    expression = parse_filter(filter_text)

    assert expression is not None
    assert expression.terms == terms
    assert expression.operator == operator


@pytest.mark.parametrize("filter_text", [None, "", "   ", "???", "&&&"])
def test_parse_filter_empty_expression_uses_no_filter_semantics(filter_text):
    assert parse_filter(filter_text) is None


def test_parse_filter_rejects_mixed_operators():
    with pytest.raises(ValueError, match="either.*OR"):
        parse_filter("payment&failed?timeout")


def test_filter_matching_supports_or_and_case_insensitive_literal_terms():
    payment = Message(0, 0, 1, None, b"Payment processed")
    failed = Message(0, 1, 2, None, b"FAILED request")
    both = Message(0, 2, 3, None, b"payment failed")
    unrelated = Message(0, 3, 4, None, b"complete")

    assert message_matches_filter(payment, "PAYMENT?timeout") is True
    assert message_matches_filter(failed, "payment?failed") is True
    assert message_matches_filter(both, "payment&failed") is True
    assert message_matches_filter(payment, "payment&failed") is False
    assert message_matches_filter(unrelated, "payment?failed") is False
    assert message_matches_filter(Message(0, 4, 5, None, b"literal .*+?[]()"), ".*+?[]()") is True


def test_filter_scan_cap_is_enforced_for_latest_mode():
    messages = [
        Record("events", 0, index, 1000 + index, None, f'{{"value": "other-{index}"}}'.encode("utf-8"))
        for index in range(FILTER_SCAN_CAP + 10)
    ]
    FakeConsumer.records[TopicPartition("events", 0)] = messages

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=FakeConsumer)
    filtered = client.load_messages("events", "group", "latest", 10, filter_text="match")

    assert len(filtered) <= 10
    assert client.last_scan_metadata["cap_reached"] is True
    assert client.last_scan_metadata["scanned"] >= FILTER_SCAN_CAP


def test_latest_filter_returns_newest_matches_across_partitions_by_timestamp():
    PartitionedConsumer.beginning = {
        TopicPartition("events", 0): 100,
        TopicPartition("events", 1): 10,
    }
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, 100, 1000, None, b"match-p0-old"),
            Record("events", 0, 101, 2000, None, b"match-p0-new"),
            Record("events", 0, 102, 3000, None, b"other"),
        ],
        TopicPartition("events", 1): [
            Record("events", 1, 10, 5000, None, b"match-p1-old-offset"),
            Record("events", 1, 11, 6000, None, b"match-p1-new"),
        ],
    }

    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer).load_messages(
        "events", "group", "latest", 2, filter_text="match"
    )

    assert [(message.partition, message.offset) for message in messages] == [(1, 10), (1, 11)]


def test_latest_filter_or_and_predicates_use_one_combined_scan():
    PartitionedConsumer.beginning = {TopicPartition("events", 0): 0}
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, 0, 1000, None, b"payment processed"),
            Record("events", 0, 1, 2000, None, b"request failed"),
            Record("events", 0, 2, 3000, None, b"payment failed"),
        ]
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)
    or_messages = client.load_messages("events", "group", "latest", 3, filter_text="payment?failed")
    and_messages = client.load_messages("events", "group", "latest", 3, filter_text="payment&failed")

    assert [message.offset for message in or_messages] == [0, 1, 2]
    assert [message.offset for message in and_messages] == [2]


def test_mixed_filter_fails_before_kafka_scan():
    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)

    with pytest.raises(ValueError, match="either.*OR"):
        client.load_messages("events", "group", "latest", 1, filter_text="payment&failed?timeout")

    assert PartitionedConsumer.instances == []


def test_latest_filter_does_not_stop_when_first_partition_has_enough_matches():
    PartitionedConsumer.beginning = {
        TopicPartition("events", 0): 100,
        TopicPartition("events", 1): 10,
    }
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, 100, 1000, None, b"match-first-a"),
            Record("events", 0, 101, 2000, None, b"match-first-b"),
        ],
        TopicPartition("events", 1): [
            Record("events", 1, 10, 9000, None, b"match-newer-a"),
            Record("events", 1, 11, 10000, None, b"match-newer-b"),
        ],
    }

    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer).load_messages(
        "events", "group", "latest", 2, filter_text="match"
    )

    assert [(message.partition, message.offset) for message in messages] == [(1, 10), (1, 11)]


def test_latest_filter_returns_exact_requested_count_from_larger_match_set():
    PartitionedConsumer.beginning = {TopicPartition("events", 0): 0}
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, offset, 1000 + offset, None, f"match-{offset}".encode())
            for offset in range(20)
        ]
    }

    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer).load_messages(
        "events", "group", "latest", 10, filter_text="match"
    )

    assert len(messages) == 10
    assert [message.offset for message in messages] == list(range(10, 20))


def test_latest_filter_count_100_returns_newest_100_matches():
    PartitionedConsumer.beginning = {TopicPartition("events", 0): 0}
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, offset, offset, None, f"match-{offset}".encode())
            for offset in range(150)
        ]
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)
    messages = client.load_messages(
        "events", "group", "latest", 100, filter_text="match"
    )

    assert len(messages) == 100
    assert [message.offset for message in messages] == list(range(50, 150))
    assert client.last_scan_metadata["scanned"] == 150


def test_message_filter_treats_special_characters_as_literal_text():
    PartitionedConsumer.beginning = {TopicPartition("events", 0): 0}
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [Record("events", 0, 0, 1000, None, b"literal .*+?[]()")]
    }

    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer).load_messages(
        "events", "group", "latest", 1, filter_text=".*+?[]()"
    )

    assert len(messages) == 1


def test_latest_filter_uses_fixed_window_for_small_requested_count():
    PartitionedConsumer.beginning = {TopicPartition("events", 0): 0}
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, offset, offset, None, b"other")
            for offset in range(150)
        ]
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)
    client.load_messages("events", "group", "latest", 10, filter_text="match")

    assert PartitionedConsumer.instances[-1].seeks[0] == (TopicPartition("events", 0), 0)


def test_latest_filter_stops_after_early_cross_partition_matches():
    PartitionedConsumer.beginning = {
        TopicPartition("events", 0): 0,
        TopicPartition("events", 1): 0,
    }
    PartitionedConsumer.records = {
        TopicPartition("events", partition): [
            Record("events", partition, offset, offset, None, b"match")
            for offset in range(150)
        ]
        for partition in (0, 1)
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)
    messages = client.load_messages("events", "group", "latest", 10, filter_text="match")

    assert len(messages) == 10
    assert client.last_scan_metadata["scanned"] == 300
    assert client.last_scan_metadata["scanned"] < FILTER_SCAN_CAP


def test_latest_filter_initial_target_is_500_for_count_10():
    PartitionedConsumer.beginning = {TopicPartition("events", 0): 0}
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, offset, offset, None, b"match")
            for offset in range(600)
        ]
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)
    messages = client.load_messages("events", "group", "latest", 10, filter_text="match")

    assert len(messages) == 10
    assert client.last_scan_metadata["scanned"] == 500


def test_latest_filter_expands_only_when_initial_target_has_too_few_matches():
    PartitionedConsumer.beginning = {TopicPartition("events", 0): 0}
    PartitionedConsumer.records = {
        TopicPartition("events", 0): [
            Record("events", 0, offset, offset, None, b"match" if offset < 3 or offset >= 993 else b"other")
            for offset in range(1000)
        ]
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)
    messages = client.load_messages("events", "group", "latest", 10, filter_text="match")

    assert len(messages) == 10
    assert client.last_scan_metadata["scanned"] == 1000
    assert client.last_scan_metadata["scanned"] < FILTER_SCAN_CAP
    assert PartitionedConsumer.instances[-1].seeks == [
        (TopicPartition("events", 0), 500),
        (TopicPartition("events", 0), 0),
    ]


def test_partition_scan_targets_are_global_and_skip_empty_partitions():
    targets = KafkaClient._partition_scan_targets(
        {
            TopicPartition("events", 0): 0,
            TopicPartition("events", 1): 10,
            TopicPartition("events", 2): 0,
        },
        {
            TopicPartition("events", 0): 1000,
            TopicPartition("events", 1): 10,
            TopicPartition("events", 2): 0,
        },
        500,
    )

    assert sum(targets.values()) == 500
    assert TopicPartition("events", 1) not in targets
    assert TopicPartition("events", 2) not in targets


def test_latest_filter_scan_cap_is_global_across_partitions():
    PartitionedConsumer.beginning = {
        TopicPartition("events", 0): 0,
        TopicPartition("events", 1): 0,
    }
    PartitionedConsumer.records = {
        TopicPartition("events", partition): [
            Record("events", partition, offset, offset, None, b"other")
            for offset in range(3000)
        ]
        for partition in (0, 1)
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=PartitionedConsumer)
    messages = client.load_messages("events", "group", "latest", 1, filter_text="match")

    assert messages == []
    assert client.last_scan_metadata["scanned"] == FILTER_SCAN_CAP
    assert client.last_scan_metadata["cap_reached"] is True


def test_export_messages_returns_json_ready_rows():
    messages = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=FakeConsumer).load_messages("events", "group", "beginning", 2)

    exported = export_messages(messages)

    assert exported[0]["partition"] == 0
    assert exported[0]["message"] == {"a": 1}
    assert exported[1]["key"] == "k"


def test_topic_statistics_counts_retained_records_from_partition_offsets():
    now = datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc)
    StatisticsConsumer.beginning = {
        TopicPartition("stats", 0): 100,
        TopicPartition("stats", 1): 200,
    }
    StatisticsConsumer.records = {
        TopicPartition("stats", 0): [Record("stats", 0, 100 + index, int((now - timedelta(hours=index)).timestamp() * 1000), None, b"") for index in range(50)],
        TopicPartition("stats", 1): [Record("stats", 1, 200 + index, int((now - timedelta(days=2, hours=index)).timestamp() * 1000), None, b"") for index in range(25)],
    }

    statistics = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_topic_statistics("stats", now)

    assert statistics.total_records == 75
    assert statistics.partitions == 2


def test_topic_statistics_handles_empty_partitions_and_timestamp_boundaries():
    local_timezone = datetime.now().astimezone().tzinfo
    now = datetime(2026, 9, 26, 12, 0, tzinfo=local_timezone)
    start_of_today = datetime(2026, 9, 26, tzinfo=local_timezone)
    StatisticsConsumer.beginning = {
        TopicPartition("stats", 0): 10,
        TopicPartition("stats", 1): 50,
    }
    StatisticsConsumer.records = {
        TopicPartition("stats", 0): [
            Record("stats", 0, 10, int((start_of_today - timedelta(milliseconds=1)).timestamp() * 1000), None, b""),
            Record("stats", 0, 11, int(start_of_today.timestamp() * 1000), None, b""),
            Record("stats", 0, 12, int((start_of_today + timedelta(hours=1)).timestamp() * 1000), None, b""),
            Record("stats", 0, 13, int((now - timedelta(hours=1)).timestamp() * 1000), None, b""),
            Record("stats", 0, 14, int((start_of_today + timedelta(days=1)).timestamp() * 1000), None, b""),
        ],
        TopicPartition("stats", 1): [],
    }

    statistics = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_topic_statistics("stats", now)

    assert statistics.total_records == 5
    assert statistics.published_today == 3
    assert statistics.published_last_hour == 1


def test_topic_statistics_selects_latest_timestamp_and_refreshes():
    first_now = datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc)
    StatisticsConsumer.beginning = {TopicPartition("stats", 0): 0}
    StatisticsConsumer.records = {
        TopicPartition("stats", 0): [Record("stats", 0, 0, 1000, None, b"")]
    }
    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer)

    first = client.get_topic_statistics("stats", first_now)
    StatisticsConsumer.records[TopicPartition("stats", 0)].append(Record("stats", 0, 1, 2000, None, b""))
    second = client.get_topic_statistics("stats", first_now)

    assert first.latest_timestamp == 1000
    assert second.total_records == 2
    assert second.latest_timestamp == 2000


def test_topic_statistics_uses_newest_timestamp_across_partitions():
    StatisticsConsumer.beginning = {
        TopicPartition("stats", 0): 100,
        TopicPartition("stats", 1): 10,
    }
    StatisticsConsumer.records = {
        TopicPartition("stats", 0): [Record("stats", 0, 100, 1000, None, b"")],
        TopicPartition("stats", 1): [Record("stats", 1, 10, 2000, None, b"")],
    }

    statistics = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_topic_statistics("stats")

    assert statistics.latest_timestamp == 2000


def test_topic_statistics_empty_topic_preserves_partition_count():
    StatisticsConsumer.beginning = {TopicPartition("empty", 0): 7, TopicPartition("empty", 1): 3}
    StatisticsConsumer.records = {TopicPartition("empty", 0): [], TopicPartition("empty", 1): []}

    statistics = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_topic_statistics("empty")

    assert statistics.total_records == 0
    assert statistics.published_today == 0
    assert statistics.published_last_hour == 0
    assert statistics.latest_timestamp is None
    assert statistics.partitions == 2


def test_topic_statistics_uses_read_only_consumer_and_offset_apis():
    StatisticsConsumer.beginning = {TopicPartition("stats", 0): 100}
    StatisticsConsumer.records = {TopicPartition("stats", 0): []}

    KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_topic_statistics("stats")

    assert StatisticsConsumer.instances[-1].kwargs["enable_auto_commit"] is False


def test_topic_statistics_uses_selected_consumer_group_id():
    StatisticsConsumer.beginning = {TopicPartition("stats", 0): 0}
    StatisticsConsumer.records = {TopicPartition("stats", 0): [Record("stats", 0, 0, 1000, None, b"")]}

    KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_topic_statistics(
        "stats", group_id="approved-viewer-group"
    )

    assert StatisticsConsumer.instances[-1].kwargs["group_id"] == "approved-viewer-group"


def test_topic_statistics_preserves_configured_group_id_without_override():
    StatisticsConsumer.beginning = {TopicPartition("stats", 0): 0}
    StatisticsConsumer.records = {TopicPartition("stats", 0): [Record("stats", 0, 0, 1000, None, b"")]}

    KafkaClient(
        {"bootstrap_servers": "broker:9092", "group_id": "configured-viewer-group"},
        consumer_factory=StatisticsConsumer,
    ).get_topic_statistics("stats")

    assert StatisticsConsumer.instances[-1].kwargs["group_id"] == "configured-viewer-group"


def test_latest_record_timestamp_lookup_uses_selected_group_id():
    StatisticsConsumer.beginning = {TopicPartition("stats", 0): 500}
    StatisticsConsumer.records = {TopicPartition("stats", 0): [Record("stats", 0, 500, 9000, None, b"")]}

    timestamp = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_latest_record_timestamp(
        "stats", group_id="approved-viewer-group"
    )

    assert timestamp == 9000
    assert StatisticsConsumer.instances[-1].kwargs["group_id"] == "approved-viewer-group"


def test_latest_record_timestamp_accepts_offset_gap_before_end_offset():
    GapStatisticsConsumer.beginning = {TopicPartition("stats", 0): 100}
    GapStatisticsConsumer.records = {
        TopicPartition("stats", 0): [
            Record("stats", 0, 100, 1000, None, b""),
            Record("stats", 0, 105, 5000, None, b""),
            Record("stats", 0, 109, 9000, None, b""),
        ]
    }

    timestamp = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=GapStatisticsConsumer).get_latest_record_timestamp("stats")

    assert timestamp == 9000


def test_latest_timestamp_diagnostics_capture_safe_poll_evidence(caplog):
    StatisticsConsumer.beginning = {TopicPartition("stats", 0): 0}
    StatisticsConsumer.records = {TopicPartition("stats", 0): [Record("stats", 0, 0, 9000, None, b"payload")]}

    with caplog.at_level(logging.DEBUG, logger="kafka_viewer.kafka_client"):
        timestamp = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer).get_latest_record_timestamp(
            "stats", group_id="approved-viewer-group"
        )

    diagnostics = "\n".join(record.message for record in caplog.records)
    assert timestamp == 9000
    assert "latest_timestamp assign succeeded" in diagnostics
    assert "latest_timestamp seek partition=0" in diagnostics
    assert "latest_timestamp poll=1 returned_records=1" in diagnostics
    assert "timestamp=9000" in diagnostics
    assert "payload" not in diagnostics
    assert "approved-viewer-group" not in diagnostics


def test_topic_statistics_batches_latest_partition_reads():
    StatisticsConsumer.beginning = {
        TopicPartition("stats", 0): 10,
        TopicPartition("stats", 1): 20,
    }
    StatisticsConsumer.records = {
        TopicPartition("stats", 0): [Record("stats", 0, 10, 1000, None, b"")],
        TopicPartition("stats", 1): [Record("stats", 1, 20, 2000, None, b"")],
    }

    client = KafkaClient({"bootstrap_servers": "broker:9092"}, consumer_factory=StatisticsConsumer)
    statistics = client.get_topic_statistics("stats")
    consumer = StatisticsConsumer.instances[-1]

    assert statistics.latest_timestamp == 2000
    assert len(consumer.assign_calls) == 1
    assert consumer.poll_calls <= 3


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


def test_group_id_prefix_is_optional():
    assert generate_group_id("").startswith("kafka-viewer-")


def test_group_id_includes_prefix_when_provided():
    generated = generate_group_id("payments-")

    assert generated.startswith("payments-kafka-viewer-")


@pytest.mark.parametrize("value", [0, -1, "nope"])
def test_validate_count_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="positive integer"):
        validate_count(value)


def test_validate_count_accepts_positive_integer():
    assert validate_count("3") == 3
