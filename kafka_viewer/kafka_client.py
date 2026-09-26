from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Literal

from kafka import KafkaConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

FILTER_SCAN_CAP = 5000
# Keep filtered Latest responsive while retaining a bounded global search budget.
MAX_SCAN = FILTER_SCAN_CAP
MIN_SCAN = 500
SCAN_MULTIPLIER = 50


@dataclass(frozen=True)
class Message:
    partition: int
    offset: int
    timestamp: int | None
    key: Any
    value: Any
    value_error: str | None = None
    raw_value: str | None = None


@dataclass(frozen=True)
class TopicStatistics:
    total_records: int
    published_today: int | None
    published_last_hour: int | None
    latest_timestamp: int | None
    partitions: int
    timestamp_error: str | None = None


@dataclass(frozen=True)
class FilterExpression:
    terms: tuple[str, ...]
    operator: Literal["or", "and"] | None = None


def validate_count(value: Any) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Message count must be a positive integer") from exc
    if count <= 0:
        raise ValueError("Message count must be a positive integer")
    return count


def generate_group_id(prefix: str = "") -> str:
    return f"{prefix}kafka-viewer-{uuid.uuid4().hex[:12]}"


def _readable(value: Any) -> str:
    if value is None:
        return "<null>"
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def format_value(value: Any) -> str:
    if value is None:
        return "<null>"
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, ensure_ascii=False, default=str)
    readable = _readable(value)
    try:
        parsed = json.loads(readable)
    except (TypeError, json.JSONDecodeError):
        return readable
    return json.dumps(parsed, indent=2, ensure_ascii=False)


def format_key(value: Any) -> str:
    return _readable(value)


def format_message_value(message: Message) -> str:
    if message.value_error:
        return f"Avro deserialization failed\n\nRaw message:\n{message.raw_value}\n\nError:\n{message.value_error}"
    return format_value(message.value)


def parse_filter(filter_text: str | None) -> FilterExpression | None:
    normalized = (filter_text or "").strip()
    if not normalized:
        return None
    has_or = "?" in normalized
    has_and = "&" in normalized
    if has_or and has_and:
        raise ValueError("Use either '?' for OR or '&' for AND, not both in the same filter.")
    operator = "or" if has_or else "and" if has_and else None
    separator = "?" if has_or else "&"
    terms = tuple(term.strip() for term in normalized.split(separator) if term.strip())
    if not terms:
        return None
    return FilterExpression(terms, operator)


def message_matches_filter(message: Message, filter_text: str | FilterExpression | None) -> bool:
    expression = parse_filter(filter_text) if isinstance(filter_text, str) or filter_text is None else filter_text
    if expression is None:
        return True
    body = (message.raw_value if message.value_error and message.raw_value is not None else format_value(message.value))
    if body is None:
        return False
    searchable = str(body).casefold()
    terms = (term.casefold() for term in expression.terms)
    if expression.operator == "and":
        return all(term in searchable for term in terms)
    return any(term in searchable for term in terms)


def export_messages(messages: Iterable[Message]) -> list[dict[str, Any]]:
    exported: list[dict[str, Any]] = []
    for message in messages:
        value = None if message.value is None else message.value
        if message.value_error and message.raw_value is not None:
            value = message.raw_value
        else:
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    pass
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                value = str(value)
        exported.append(
            {
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp,
                "key": format_key(message.key),
                "message": value,
            }
        )
    return exported


def safe_raw_payload(value: Any, limit: int = 4096) -> str:
    if value is None:
        return "<null>"
    if not isinstance(value, bytes):
        return str(value)[:limit]
    bounded = value[:limit // 2] if any(byte > 127 or byte == 0 for byte in value) else value[:limit]
    suffix = "\n...[truncated]" if len(value) > limit else ""
    try:
        return bounded.decode("utf-8") + suffix
    except UnicodeDecodeError:
        return bounded.hex() + suffix


def datetime_to_millis(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _local_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.astimezone()
    return value.astimezone()


def _statistics_time_windows(now: datetime) -> tuple[tuple[int, int], tuple[int, int]]:
    local_now = _local_datetime(now)
    start_of_today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_tomorrow = start_of_today + timedelta(days=1)
    start_of_last_hour = local_now - timedelta(hours=1)
    return (
        (datetime_to_millis(start_of_today), datetime_to_millis(start_of_tomorrow)),
        (datetime_to_millis(start_of_last_hour), datetime_to_millis(local_now)),
    )


class KafkaClient:
    def __init__(
        self,
        consumer_config: dict[str, Any],
        schema_registry_config: dict[str, str] | None = None,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
        schema_registry_factory: Callable[..., Any] = SchemaRegistryClient,
        avro_deserializer_factory: Callable[..., Any] = AvroDeserializer,
    ):
        self.consumer_config = consumer_config
        self.consumer_factory = consumer_factory
        self.value_deserializer = None
        self.last_scan_metadata: dict[str, Any] = {
            "requested": 0,
            "matches": 0,
            "scanned": 0,
            "cap_reached": False,
        }
        if schema_registry_config:
            registry = schema_registry_factory(schema_registry_config)
            self.value_deserializer = avro_deserializer_factory(registry)

    def topics(self) -> set[str]:
        consumer = self._consumer(request_timeout_ms=5000)
        try:
            return set(consumer.topics())
        finally:
            consumer.close()

    def get_topic_statistics(
        self,
        topic: str,
        now: datetime | None = None,
        group_id: str | None = None,
    ) -> TopicStatistics:
        """Return retained-topic metrics without committing offsets or consuming the topic."""
        consumer_overrides: dict[str, Any] = {
            "enable_auto_commit": False,
            "consumer_timeout_ms": 1500,
            "request_timeout_ms": 5000,
        }
        if group_id is not None:
            consumer_overrides["group_id"] = group_id or None
        consumer = self._consumer(**consumer_overrides)
        try:
            partitions = [TopicPartition(topic, number) for number in (consumer.partitions_for_topic(topic) or set())]
            if not partitions:
                return TopicStatistics(0, 0, 0, None, 0)

            beginning_offsets = consumer.beginning_offsets(partitions)
            end_offsets = consumer.end_offsets(partitions)
            total_records = sum(max(0, end_offsets[partition] - beginning_offsets[partition]) for partition in partitions)
            timestamp_error = None
            published_today: int | None = 0
            published_last_hour: int | None = 0
            latest_timestamp: int | None = None
            current_time = now or datetime.now().astimezone()
            today_window, last_hour_window = _statistics_time_windows(current_time)
            timestamp_failures = 0
            try:
                published_today = self._count_timestamp_window(consumer, partitions, end_offsets, *today_window)
            except Exception:
                published_today = None
                timestamp_failures += 1
            try:
                published_last_hour = self._count_timestamp_window(consumer, partitions, end_offsets, *last_hour_window)
            except Exception:
                published_last_hour = None
                timestamp_failures += 1
            try:
                latest_timestamp = self._latest_timestamp(consumer, partitions, beginning_offsets, end_offsets)
            except Exception:
                latest_timestamp = None
                timestamp_failures += 1
            if timestamp_failures:
                timestamp_error = "Some timestamp-based statistics are unavailable for this topic"
            return TopicStatistics(
                total_records,
                published_today,
                published_last_hour,
                latest_timestamp,
                len(partitions),
                timestamp_error,
            )
        finally:
            consumer.close()

    def get_latest_record_timestamp(self, topic: str, group_id: str | None = None) -> int | None:
        """Return the maximum timestamp from the latest retained record of each partition."""
        consumer_overrides: dict[str, Any] = {
            "enable_auto_commit": False,
            "consumer_timeout_ms": 1500,
            "request_timeout_ms": 5000,
        }
        if group_id is not None:
            consumer_overrides["group_id"] = group_id or None
        consumer = self._consumer(**consumer_overrides)
        try:
            partitions = [TopicPartition(topic, number) for number in (consumer.partitions_for_topic(topic) or set())]
            if not partitions:
                return None
            beginning_offsets = consumer.beginning_offsets(partitions)
            end_offsets = consumer.end_offsets(partitions)
            return self._latest_timestamp(consumer, partitions, beginning_offsets, end_offsets)
        finally:
            consumer.close()

    def _count_timestamp_window(
        self,
        consumer: Any,
        partitions: list[TopicPartition],
        end_offsets: dict[TopicPartition, int],
        start_millis: int,
        end_millis: int,
    ) -> int:
        start_offsets = consumer.offsets_for_times({partition: start_millis for partition in partitions})
        finish_offsets = consumer.offsets_for_times({partition: end_millis for partition in partitions})
        total = 0
        for partition in partitions:
            start_offset = start_offsets[partition]
            finish_offset = finish_offsets[partition]
            start_value = end_offsets[partition] if start_offset is None else start_offset.offset
            finish_value = end_offsets[partition] if finish_offset is None else finish_offset.offset
            total += max(0, finish_value - start_value)
        return total

    def _latest_timestamp(
        self,
        consumer: Any,
        partitions: list[TopicPartition],
        beginning_offsets: dict[TopicPartition, int],
        end_offsets: dict[TopicPartition, int],
    ) -> int | None:
        latest_records = self._latest_retained_records(consumer, partitions, beginning_offsets, end_offsets)
        timestamps = [record.timestamp for record in latest_records if record.timestamp is not None]
        return max(timestamps) if timestamps else None

    def _latest_retained_records(
        self,
        consumer: Any,
        partitions: list[TopicPartition],
        beginning_offsets: dict[TopicPartition, int],
        end_offsets: dict[TopicPartition, int],
    ) -> list[Any]:
        latest_partitions = [partition for partition in partitions if end_offsets[partition] > beginning_offsets[partition]]
        if not latest_partitions:
            return []
        records: dict[TopicPartition, Any] = {}
        consumer.assign(latest_partitions)
        for partition in latest_partitions:
            consumer.seek(partition, end_offsets[partition] - 1)
        for _ in range(3):
            batch = consumer.poll(timeout_ms=1000)
            for record in _records(batch):
                partition = TopicPartition(record.topic, record.partition)
                if partition in latest_partitions and record.offset == end_offsets[partition] - 1:
                    if partition not in records:
                        records[partition] = record
            if len(records) == len(latest_partitions):
                break
        return [records[partition] for partition in latest_partitions if partition in records]

    def _load_latest_messages_with_filter(
        self,
        consumer: Any,
        partitions: list[TopicPartition],
        beginning_offsets: dict[TopicPartition, int],
        end_offsets: dict[TopicPartition, int],
        count: int,
        filter_expression: FilterExpression,
    ) -> list[Message]:
        initial_target = min(MAX_SCAN, max(MIN_SCAN, count * SCAN_MULTIPLIER))
        scan_target = initial_target
        scan_ends = dict(end_offsets)
        matches: list[Message] = []
        while scan_ends and self.last_scan_metadata["scanned"] < FILTER_SCAN_CAP:
            partition_targets = self._partition_scan_targets(
                beginning_offsets,
                end_offsets,
                scan_target,
            )
            windows = {
                partition: (
                    max(beginning_offsets[partition], end_offsets[partition] - partition_targets[partition]),
                    scan_ends[partition],
                )
                for partition in scan_ends
                if partition in partition_targets and scan_ends[partition] > beginning_offsets[partition]
            }
            if not windows:
                break
            active = set(windows)
            consumer.assign(list(active))
            for partition, (window_start, _) in windows.items():
                consumer.seek(partition, window_start)
            idle_polls = 0
            while active and self.last_scan_metadata["scanned"] < FILTER_SCAN_CAP:
                batch = consumer.poll(timeout_ms=1000)
                if not batch:
                    idle_polls += 1
                    if idle_polls >= 2:
                        break
                    continue
                idle_polls = 0
                for record in _records(batch):
                    partition = TopicPartition(record.topic, record.partition)
                    window = windows.get(partition)
                    if window is None or not (window[0] <= record.offset < window[1]):
                        continue
                    self.last_scan_metadata["scanned"] += 1
                    message = self._message(record, record.timestamp)
                    if message_matches_filter(message, filter_expression):
                        matches.append(message)
                    if self.last_scan_metadata["scanned"] >= FILTER_SCAN_CAP:
                        self.last_scan_metadata["cap_reached"] = True
                        break
                if self.last_scan_metadata["scanned"] >= FILTER_SCAN_CAP:
                    break
                for partition in list(active):
                    if consumer.position(partition) >= windows[partition][1]:
                        active.discard(partition)
            if len(matches) >= count:
                break
            for partition, (window_start, _) in windows.items():
                if window_start >= scan_ends[partition]:
                    scan_ends.pop(partition, None)
                    continue
                scan_ends[partition] = window_start
            scan_ends = {partition: end for partition, end in scan_ends.items() if end > beginning_offsets[partition]}
            if self.last_scan_metadata["scanned"] >= FILTER_SCAN_CAP:
                break
            next_target = min(MAX_SCAN, scan_target * 2)
            if next_target <= scan_target:
                break
            scan_target = next_target

        matches.sort(key=lambda item: (item.timestamp if item.timestamp is not None else -1, item.partition, item.offset))
        return matches[-count:]

    @staticmethod
    def _partition_scan_targets(
        beginning_offsets: dict[TopicPartition, int],
        end_offsets: dict[TopicPartition, int],
        total_target: int,
    ) -> dict[TopicPartition, int]:
        available = {
            partition: max(0, end_offsets[partition] - beginning_offsets[partition])
            for partition in end_offsets
        }
        available = {partition: size for partition, size in available.items() if size > 0}
        total_available = sum(available.values())
        target = min(total_target, total_available)
        if not available or target <= 0:
            return {}

        targets = {partition: 0 for partition in available}
        remaining_target = target
        remaining_available = total_available
        for partition, size in available.items():
            if remaining_target <= 0:
                break
            share = max(1, (remaining_target * size) // remaining_available)
            share = min(size, share, remaining_target)
            targets[partition] = share
            remaining_target -= share
            remaining_available -= size

        while remaining_target:
            progressed = False
            for partition, size in available.items():
                if targets[partition] < size:
                    targets[partition] += 1
                    remaining_target -= 1
                    progressed = True
                    if not remaining_target:
                        break
            if not progressed:
                break
        return targets

    def load_messages(
        self,
        topic: str,
        group_id: str,
        mode: str,
        count: int,
        start: datetime | None = None,
        end: datetime | None = None,
        filter_text: str | None = None,
    ) -> list[Message]:
        count = validate_count(count)
        if mode not in {"latest", "beginning", "from_date", "range"}:
            raise ValueError("Unknown loading mode")
        if mode in {"from_date", "range"} and start is None:
            raise ValueError("A start date/time is required")
        if mode == "range" and end is None:
            raise ValueError("An end date/time is required")
        if mode == "range" and datetime_to_millis(start) > datetime_to_millis(end):
            raise ValueError("End date/time must not be before start date/time")

        filter_expression = parse_filter(filter_text)
        if filter_expression is None:
            return self._load_messages_without_filter(topic, group_id, mode, count, start, end)
        return self._load_messages_with_filter(topic, group_id, mode, count, filter_expression, start, end)

    def _load_messages_without_filter(
        self,
        topic: str,
        group_id: str,
        mode: str,
        count: int,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Message]:
        consumer = self._consumer(
            group_id=group_id or None,
            enable_auto_commit=False,
            consumer_timeout_ms=1500,
            request_timeout_ms=5000,
        )
        self.last_scan_metadata = {"requested": count, "matches": 0, "scanned": 0, "cap_reached": False}
        try:
            partitions = [TopicPartition(topic, number) for number in (consumer.partitions_for_topic(topic) or set())]
            if not partitions:
                return []
            end_offsets = consumer.end_offsets(partitions)
            if mode == "latest":
                starts = {partition: max(0, end_offsets[partition] - count) for partition in partitions}
            elif mode == "beginning":
                starts = consumer.beginning_offsets(partitions)
            else:
                starts = consumer.offsets_for_times({partition: datetime_to_millis(start) for partition in partitions})
                starts = {partition: offset.offset for partition, offset in starts.items() if offset is not None}

            active = {partition for partition, offset in starts.items() if offset < end_offsets[partition]}
            if not active:
                return []
            consumer.assign(list(active))
            for partition, offset in starts.items():
                if partition not in active:
                    continue
                consumer.seek(partition, offset)

            records: list[Message] = []
            idle_polls = 0
            while active and (mode == "latest" or len(records) < count):
                batch = consumer.poll(timeout_ms=1000)
                if not batch:
                    idle_polls += 1
                    if idle_polls >= 2:
                        break
                    continue
                idle_polls = 0
                for record in _records(batch):
                    partition = TopicPartition(record.topic, record.partition)
                    if record.offset >= end_offsets[partition]:
                        continue
                    timestamp = record.timestamp
                    if mode == "from_date" and (timestamp is None or timestamp < datetime_to_millis(start)):
                        continue
                    if mode == "range" and (timestamp is None or timestamp < datetime_to_millis(start) or timestamp > datetime_to_millis(end)):
                        if timestamp is not None and timestamp > datetime_to_millis(end):
                            active.discard(partition)
                        continue
                    records.append(self._message(record, timestamp))
                    if mode == "range" and timestamp is not None and timestamp >= datetime_to_millis(end):
                        active.discard(partition)
                    if len(records) >= count:
                        break
                for partition in list(active):
                    if consumer.position(partition) >= end_offsets[partition]:
                        active.discard(partition)
            records.sort(key=lambda item: (item.timestamp if item.timestamp is not None else -1, item.partition, item.offset))
            self.last_scan_metadata["matches"] = len(records)
            return records[-count:]
        finally:
            consumer.close()

    def _load_messages_with_filter(
        self,
        topic: str,
        group_id: str,
        mode: str,
        count: int,
        filter_expression: FilterExpression,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Message]:
        consumer = self._consumer(
            group_id=group_id or None,
            enable_auto_commit=False,
            consumer_timeout_ms=1500,
            request_timeout_ms=5000,
        )
        self.last_scan_metadata = {"requested": count, "matches": 0, "scanned": 0, "cap_reached": False}
        try:
            partitions = [TopicPartition(topic, number) for number in (consumer.partitions_for_topic(topic) or set())]
            if not partitions:
                return []
            end_offsets = consumer.end_offsets(partitions)
            if mode == "latest":
                beginning_offsets = consumer.beginning_offsets(partitions)
                matches = self._load_latest_messages_with_filter(
                    consumer,
                    partitions,
                    beginning_offsets,
                    end_offsets,
                    count,
                    filter_expression,
                )
                self.last_scan_metadata["matches"] = len(matches)
                return matches[-count:]

            if mode == "beginning":
                starts = consumer.beginning_offsets(partitions)
            else:
                starts = consumer.offsets_for_times({partition: datetime_to_millis(start) for partition in partitions})
                starts = {partition: offset.offset for partition, offset in starts.items() if offset is not None}

            active = {partition for partition, offset in starts.items() if offset < end_offsets[partition]}
            if not active:
                return []
            consumer.assign(list(active))
            for partition, offset in starts.items():
                if partition in active:
                    consumer.seek(partition, offset)

            matches: list[Message] = []
            idle_polls = 0
            while active and len(matches) < count and self.last_scan_metadata["scanned"] < FILTER_SCAN_CAP:
                batch = consumer.poll(timeout_ms=1000)
                if not batch:
                    idle_polls += 1
                    if idle_polls >= 2:
                        break
                    continue
                idle_polls = 0
                for record in _records(batch):
                    partition = TopicPartition(record.topic, record.partition)
                    if record.offset >= end_offsets[partition]:
                        continue
                    timestamp = record.timestamp
                    if mode == "from_date" and (timestamp is None or timestamp < datetime_to_millis(start)):
                        continue
                    if mode == "range" and (timestamp is None or timestamp < datetime_to_millis(start) or timestamp > datetime_to_millis(end)):
                        if timestamp is not None and timestamp > datetime_to_millis(end):
                            active.discard(partition)
                        continue
                    self.last_scan_metadata["scanned"] += 1
                    message = self._message(record, timestamp)
                    if message_matches_filter(message, filter_expression):
                        matches.append(message)
                    if self.last_scan_metadata["scanned"] >= FILTER_SCAN_CAP:
                        self.last_scan_metadata["cap_reached"] = True
                        break
                    if mode == "range" and timestamp is not None and timestamp >= datetime_to_millis(end):
                        active.discard(partition)
                    if len(matches) >= count:
                        break
                if self.last_scan_metadata["scanned"] >= FILTER_SCAN_CAP:
                    self.last_scan_metadata["cap_reached"] = True
                    break
                for partition in list(active):
                    if consumer.position(partition) >= end_offsets[partition]:
                        active.discard(partition)
            self.last_scan_metadata["matches"] = len(matches)
            matches.sort(key=lambda item: (item.timestamp if item.timestamp is not None else -1, item.partition, item.offset))
            return matches[-count:]
        finally:
            consumer.close()

    def _consumer(self, **overrides: Any) -> Any:
        consumer_config = {**self.consumer_config, **overrides}
        supported = set(KafkaConsumer.DEFAULT_CONFIG)
        return self.consumer_factory(**{key: value for key, value in consumer_config.items() if key in supported})

    def _message(self, record: Any, timestamp: int | None) -> Message:
        if self.value_deserializer is None:
            return Message(record.partition, record.offset, timestamp, record.key, record.value)
        try:
            value = self.value_deserializer(record.value, None)
            return Message(record.partition, record.offset, timestamp, record.key, value)
        except Exception:
            return Message(
                record.partition,
                record.offset,
                timestamp,
                record.key,
                None,
                "Unable to deserialize Avro message value",
                safe_raw_payload(record.value),
            )


def _records(batch: dict[Any, Iterable[Any]]) -> Iterable[Any]:
    for records in batch.values():
        yield from records
