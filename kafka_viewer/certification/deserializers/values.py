from __future__ import annotations

import json
from decimal import Decimal

from .base import DeserializationError
from ..models import InfrastructureError


class RawString:
    def decode(self, payload, topic=""):
        try:
            return payload.decode("utf-8", errors="strict")
        except (AttributeError, UnicodeError):
            raise DeserializationError("Invalid UTF-8 payload") from None

    def close(self):
        pass


class JsonValue(RawString):
    def decode(self, payload, topic=""):
        def invalid_constant(value):
            raise ValueError("Non-finite JSON number")
        def unique_object(pairs):
            obj = {}
            for key, value in pairs:
                if key in obj:
                    raise ValueError("Duplicate JSON key")
                obj[key] = value
            return obj
        try:
            value = json.loads(super().decode(payload), parse_float=Decimal,
                               parse_constant=invalid_constant, object_pairs_hook=unique_object)
            if not isinstance(value, (dict, list)):
                raise ValueError("Structured root required")
            return value
        except (ValueError, RecursionError):
            raise DeserializationError("Invalid structured JSON payload") from None


class XmlValue(RawString):
    def decode(self, payload, topic=""):
        from defusedxml.ElementTree import fromstring

        def convert(element):
            if "{" in element.tag:
                raise ValueError("XML namespaces are not supported in V1")
            result = {"@" + k: v for k, v in element.attrib.items()}
            children = list(element)
            for child in children:
                converted = convert(child)
                if child.tag in result:
                    if not isinstance(result[child.tag], list):
                        result[child.tag] = [result[child.tag]]
                    result[child.tag].append(converted)
                else:
                    result[child.tag] = converted
            text = element.text or ""
            if not result:
                return text
            if text.strip():
                result["#text"] = text
            if any((c.tail or "").strip() for c in children):
                raise ValueError("Mixed XML content is unsupported")
            return result
        try:
            root = fromstring(payload, forbid_dtd=True, forbid_entities=True, forbid_external=True)
            return {root.tag: convert(root)}
        except Exception:
            raise DeserializationError("Invalid or unsupported safe XML payload") from None


class AvroValue:
    def __init__(self, registry_config, registry_factory=None, deserializer_factory=None):
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        if not registry_config:
            raise ValueError("Confluent Avro requires Schema Registry properties")
        self.registry = None
        try:
            self.registry = (registry_factory or SchemaRegistryClient)(dict(registry_config))
            self.deserializer = (deserializer_factory or AvroDeserializer)(self.registry)
        except Exception:
            self.close()
            raise InfrastructureError("Unable to initialize Schema Registry") from None

    def test_connection(self):
        try:
            self.registry.get_subjects()
        except Exception:
            raise InfrastructureError("Schema Registry connection test failed") from None

    def decode(self, payload, topic=""):
        from confluent_kafka.schema_registry.error import SchemaRegistryError
        from httpx import TransportError

        if not isinstance(payload, bytes) or len(payload) < 5 or payload[0] != 0:
            raise DeserializationError("Invalid Confluent Avro framing")
        try:
            # Match the viewer: writer schema ID only, without topic-association
            # lookup or topic-specific Schema Registry transformation rules.
            return self.deserializer(payload, None)
        except SchemaRegistryError as exc:
            if exc.http_status_code != 404:
                raise InfrastructureError("Schema Registry request failed") from None
            raise DeserializationError("Schema ID could not be resolved") from None
        except (TransportError, OSError):
            raise InfrastructureError("Schema Registry connection failed") from None
        except Exception:
            raise DeserializationError("Invalid Avro payload") from None

    def close(self):
        if self.registry is not None:
            close = getattr(self.registry, "close", None)
            self.registry = None
            if close:
                try:
                    close()
                except Exception:
                    raise InfrastructureError("Schema Registry cleanup failed") from None


class ProtobufValue(RawString):
    def __init__(self, descriptor_bytes, message_type):
        from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

        try:
            files = descriptor_pb2.FileDescriptorSet.FromString(descriptor_bytes)
            pool = descriptor_pool.DescriptorPool()
            pending = list(files.file)
            while pending:
                remaining = []
                for file in pending:
                    try:
                        pool.Add(file)
                    except (TypeError, ValueError):
                        remaining.append(file)
                if len(remaining) == len(pending):
                    raise ValueError("Unresolved descriptor dependencies")
                pending = remaining
            self.message_class = message_factory.GetMessageClass(pool.FindMessageTypeByName(message_type))
        except Exception:
            raise ValueError("Invalid descriptor set or fully qualified message type; include all imports") from None

    def decode(self, payload, topic=""):
        from google.protobuf.json_format import MessageToDict
        try:
            message = self.message_class()
            message.ParseFromString(payload)
            if not message.IsInitialized():
                raise ValueError("Required protobuf fields missing")
            return MessageToDict(message, preserving_proto_field_name=True)
        except Exception:
            raise DeserializationError("Invalid Protobuf payload") from None
