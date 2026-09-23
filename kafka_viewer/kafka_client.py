from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from kafka import KafkaConsumer, TopicPartition


@dataclass(frozen=True)
class Message:
    partition: int
    offset: int
    timestamp: int | None
    key: Any
    value: Any


def validate_count(value: Any) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Message count must be a positive integer") from exc
    if count <= 0:
        raise ValueError("Message count must be a positive integer")
    return count


def generate_group_id() -> str:
    return f"kafka-viewer-{uuid.uuid4().hex[:12]}"


def _readable(value: Any) -> str:
    if value is None:
        return "<null>"
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def format_value(value: Any) -> str:
    if value is None:
        return "<null>"
    readable = _readable(value)
    try:
        parsed = json.loads(readable)
    except (TypeError, json.JSONDecodeError):
        return readable
    return json.dumps(parsed, indent=2, ensure_ascii=False)


def format_key(value: Any) -> str:
    return _readable(value)


def datetime_to_millis(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


class KafkaClient:
    def __init__(self, bootstrap_servers: str, consumer_factory: Callable[..., Any] = KafkaConsumer):
        self.bootstrap_servers = bootstrap_servers
        self.consumer_factory = consumer_factory

    def topics(self) -> set[str]:
        consumer = self.consumer_factory(
            bootstrap_servers=self.bootstrap_servers,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=3000,
        )
        try:
            return set(consumer.topics())
        finally:
            consumer.close()

    def load_messages(
        self,
        topic: str,
        group_id: str,
        mode: str,
        count: int,
        start: datetime | None = None,
        end: datetime | None = None,
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

        consumer = self.consumer_factory(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id or None,
            enable_auto_commit=False,
            consumer_timeout_ms=1500,
            request_timeout_ms=5000,
        )
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
                    records.append(Message(record.partition, record.offset, timestamp, record.key, record.value))
                    if mode == "range" and timestamp is not None and timestamp >= datetime_to_millis(end):
                        active.discard(partition)
                    if len(records) >= count:
                        break
                for partition in list(active):
                    if consumer.position(partition) >= end_offsets[partition]:
                        active.discard(partition)
            records.sort(key=lambda item: (item.timestamp if item.timestamp is not None else -1, item.partition, item.offset))
            return records[-count:]
        finally:
            consumer.close()


def _records(batch: dict[Any, Iterable[Any]]) -> Iterable[Any]:
    for records in batch.values():
        yield from records
