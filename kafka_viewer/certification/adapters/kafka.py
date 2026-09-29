from __future__ import annotations

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import OffsetOutOfRangeError

from kafka_viewer.config import build_consumer_config, build_schema_registry_config, load_properties
from ..models import IncompleteScope, InfrastructureError
from .base import InputRecord


def load_connections(source_path, destination_path=None):
    source = load_properties(source_path)
    destination = load_properties(destination_path) if destination_path else dict(source)
    # Validate independently, using precisely the existing security/property helpers.
    for properties in (source, destination):
        build_consumer_config(properties)
        build_schema_registry_config(properties)
    return source, destination


class KafkaAdapter:
    def __init__(self, properties, group_id=None, consumer_factory=None, batch_size=500, max_idle_polls=10):
        self.config, self.unsupported = build_consumer_config(dict(properties))
        self.config.update(enable_auto_commit=False, auto_offset_reset="none",
                           allow_auto_create_topics=False,
                           key_deserializer=None, value_deserializer=None)
        if group_id:
            self.config["group_id"] = group_id
        self.consumer_factory = consumer_factory or KafkaConsumer
        if batch_size <= 0 or max_idle_polls <= 0:
            raise ValueError("Batch and idle-poll bounds must be positive")
        self.batch_size, self.max_idle_polls = batch_size, max_idle_polls

    def _consumer(self):
        # No subscription or group coordination: explicit assignment and seeks only.
        config = {k: v for k, v in self.config.items() if k in KafkaConsumer.DEFAULT_CONFIG}
        return self.consumer_factory(**config)

    def topics(self):
        consumer = None
        try:
            consumer = self._consumer()
            return set(consumer.topics())
        except Exception:
            raise InfrastructureError("Kafka topic discovery failed") from None
        finally:
            if consumer is not None:
                self._close(consumer)

    @staticmethod
    def _close(consumer):
        try:
            consumer.close()
        except Exception:
            raise InfrastructureError("Kafka consumer cleanup failed") from None

    def read(self, topic, scope):
        consumer = None
        try:
            consumer = self._consumer()
            partitions = consumer.partitions_for_topic(topic)
            if partitions is None:
                raise InfrastructureError("Selected Kafka topic is unavailable")
            assigned = [TopicPartition(topic, p) for p in sorted(partitions)]
            if not assigned:
                return
            consumer.assign(assigned)
            beginnings = consumer.beginning_offsets(assigned)
            ends = consumer.end_offsets(assigned)
            starts = dict(beginnings)
            if scope.mode == "TIME_RANGE":
                offsets = consumer.offsets_for_times({p: scope.start_ms for p in assigned})
                starts = {p: max(beginnings[p], min(ends[p], offsets[p].offset))
                          if offsets[p] is not None else ends[p] for p in assigned}
            for p in assigned:
                consumer.seek(p, starts[p])
            active = {p for p in assigned if starts[p] < ends[p]}
            if set(assigned) - active:
                consumer.pause(*(set(assigned) - active))
            idle = 0
            while active:
                before = {p: consumer.position(p) for p in active}
                batch = consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                for p in sorted(batch, key=lambda p: (p.topic, p.partition)):
                    if p not in active:
                        continue
                    for record in batch[p]:
                        if not starts[p] <= record.offset < ends[p]:
                            continue
                        if scope.mode == "TIME_RANGE" and (record.timestamp is None or record.timestamp < 0):
                            raise IncompleteScope("A record has no usable timestamp")
                        if scope.includes(record.timestamp):
                            yield InputRecord(record.value, {"topic": topic, "partition": record.partition,
                                                            "offset": record.offset, "timestamp": record.timestamp})
                positions = {p: consumer.position(p) for p in active}
                completed = {p for p in active if positions[p] >= ends[p]}
                if completed:
                    consumer.pause(*completed)
                active -= completed
                progress = any(positions[p] > before[p] for p in positions)
                idle = 0 if progress else idle + 1
                if active and idle >= self.max_idle_polls:
                    raise IncompleteScope("Kafka scan stopped before the snapshot end")
            # Offset reset is disabled; detect retention/partition changes during scanning.
            current = consumer.beginning_offsets(assigned)
            if any(current[p] > starts[p] for p in assigned) or consumer.partitions_for_topic(topic) != partitions:
                raise IncompleteScope("Kafka retention or partitions changed during the scan")
        except (IncompleteScope, InfrastructureError):
            raise
        except OffsetOutOfRangeError:
            raise IncompleteScope("Kafka retained offsets changed during the scan") from None
        except Exception:
            raise InfrastructureError("Kafka read failed; check connection and authorization") from None
        finally:
            if consumer is not None:
                self._close(consumer)
