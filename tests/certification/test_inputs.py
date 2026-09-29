import io
import json
from types import SimpleNamespace

import pytest
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
from fastavro import schemaless_writer
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from kafka_viewer.config import build_schema_registry_config
from kafka_viewer.certification.adapters.base import InputRecord
from kafka_viewer.certification.deserializers import AvroValue, JsonValue, ProtobufValue, RawString, XmlValue
from kafka_viewer.certification.deserializers.base import DeserializationError
from kafka_viewer.certification.models import InfrastructureError
from kafka_viewer.certification.normalization import Mapping, Normalizer
from kafka_viewer.certification.pipeline import normalized_stream


def test_raw_stays_raw_and_utf8():
    assert RawString().decode(b'{"id":1}') == '{"id":1}'
    assert RawString().decode("café".encode()) == "café"
    with pytest.raises(DeserializationError):
        RawString().decode(b"\xff")


@pytest.mark.parametrize("payload", [b"{", b"null", b"100", b'{"x":NaN}', b'{"x":1,"x":2}', b"\xff"])
def test_bad_json(payload):
    with pytest.raises(DeserializationError):
        JsonValue().decode(payload)


def test_json_nested_pipeline_retains_errors():
    rows = [InputRecord(b"{", {"offset": 1}), InputRecord(b'{"txn":{"id":"A","amount":12.3}}', {"offset": 2})]
    normalizer = Normalizer([Mapping("txn.id", "id"), Mapping("txn.amount", "amount")])
    error, record = normalized_stream(rows, JsonValue(), normalizer)
    assert error.category == "DESERIALIZATION_ERROR"
    assert error.metadata == {"offset": 1}
    assert record.id == "A"
    assert str(record.data["amount"]) == "12.3"


def test_xml_paths_attributes_repeated_children_and_extras():
    decoded = XmlValue().decode(b'<txn id="A"><amount>100</amount><line>x</line><line>y</line><extra>z</extra></txn>')
    record = Normalizer([Mapping("txn/@id", "id"), Mapping("txn/amount", "amount"),
                         Mapping("txn/line/1", "line")], True).normalize(decoded)
    assert record.id == "A"
    assert record.data == {"amount": "100", "line": "y"}
    assert record.extra_fields == ("txn.extra", "txn.line.0")


@pytest.mark.parametrize("payload", [
    b"<broken>",
    b'<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><foo>&xxe;</foo>',
    b'<!DOCTYPE foo SYSTEM "https://example.invalid/evil.dtd"><foo/>',
    b'<!DOCTYPE foo [<!ENTITY a "abc"><!ENTITY b "&a;&a;">]><foo>&b;</foo>',
    b'<ns:txn xmlns:ns="urn:test"><ns:id>A</ns:id></ns:txn>',
    b'<txn><id>A</id>mixed tail</txn>',
])
def test_unsafe_or_unsupported_xml(payload):
    with pytest.raises(DeserializationError):
        XmlValue().decode(payload)


AVRO_SCHEMA = {"type": "record", "name": "Txn", "fields": [
    {"name": "id", "type": "string"}, {"name": "amount", "type": "long"}]}


def avro_payload():
    buffer = io.BytesIO(b"\x00\x00\x00\x00\x07")
    buffer.seek(5)
    schemaless_writer(buffer, AVRO_SCHEMA, {"id": "A", "amount": 100})
    return buffer.getvalue()


class Registry:
    def __init__(self, config):
        self.config, self.ids, self.closed = config, [], False

    def get_schema(self, schema_id, *args, **kwargs):
        self.ids.append(schema_id)
        return Schema(json.dumps(AVRO_SCHEMA), "AVRO")

    def close(self):
        self.closed = True

    def get_subjects(self):
        return ["test-value"]


def test_avro_real_wire_decode_registry_lookup_and_security_config():
    config = build_schema_registry_config({"schema.registry.url": "https://registry.invalid",
                                          "schema.registry.basic.auth.user.info": "user:secret",
                                          "schema.registry.ssl.verify": "false"})
    decoder = AvroValue(config, registry_factory=Registry)
    registry = decoder.registry
    try:
        assert decoder.decode(avro_payload(), "transactions") == {"id": "A", "amount": 100}
        assert registry.ids == [7]
        assert registry.config["ssl.ca.location"] is False
        assert registry.config["basic.auth.user.info"] == "user:secret"
        decoder.test_connection()
    finally:
        decoder.close()
    assert registry.closed


@pytest.mark.parametrize("status,expected", [(404, DeserializationError), (401, InfrastructureError), (500, InfrastructureError)])
def test_avro_registry_errors_are_sanitized(status, expected):
    class BrokenRegistry(Registry):
        def get_schema(self, *args, **kwargs):
            raise SchemaRegistryError(status, 123, "password=DO-NOT-EXPOSE")
    decoder = AvroValue({"url": "https://registry.invalid"}, registry_factory=BrokenRegistry)
    try:
        with pytest.raises(expected) as error:
            decoder.decode(avro_payload())
        assert "DO-NOT-EXPOSE" not in str(error.value)
    finally:
        decoder.close()


@pytest.mark.parametrize("payload", [b"", b"12345", b"\x00\x00\x00\x00\x07\xff", None])
def test_avro_bad_payload_no_raw_fallback(payload):
    decoder = AvroValue({"url": "https://registry.invalid"}, registry_factory=Registry)
    try:
        with pytest.raises(DeserializationError):
            decoder.decode(payload)
    finally:
        decoder.close()


def descriptor_fixture():
    file = descriptor_pb2.FileDescriptorProto(name="txn.proto", package="test", syntax="proto3")
    message = file.message_type.add(name="Txn")
    message.field.add(name="id", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    message.field.add(name="amount", number=2, type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("test.Txn"))
    descriptor = descriptor_pb2.FileDescriptorSet()
    descriptor.file.append(file)
    return descriptor.SerializeToString(), cls


def test_protobuf_descriptor_decode():
    descriptor, cls = descriptor_fixture()
    decoder = ProtobufValue(descriptor, "test.Txn")
    assert decoder.decode(cls(id="A", amount=100).SerializeToString()) == {"id": "A", "amount": 100}
    with pytest.raises(DeserializationError):
        decoder.decode(b"\xff")


def test_protobuf_invalid_type_descriptor_and_missing_import():
    descriptor, _ = descriptor_fixture()
    with pytest.raises(ValueError):
        ProtobufValue(descriptor, "unknown.Type")
    with pytest.raises(ValueError):
        ProtobufValue(b"\xff", "test.Txn")
    files = descriptor_pb2.FileDescriptorSet.FromString(descriptor)
    files.file[0].dependency.append("missing.proto")
    with pytest.raises(ValueError):
        ProtobufValue(files.SerializeToString(), "test.Txn")


def test_pipeline_closes_input_on_interrupt():
    closed = []
    def records():
        try:
            yield InputRecord(b"A", {})
            yield InputRecord(b"B", {})
        finally:
            closed.append(True)
    stream = normalized_stream(records(), RawString(), Normalizer([Mapping("$value", "id")]))
    assert next(stream).id == "A"
    stream.close()
    assert closed == [True]


def test_protobuf_descriptor_imports_can_be_out_of_order():
    files = descriptor_pb2.FileDescriptorSet()
    txn = files.file.add(name="txn.proto", package="test", syntax="proto3")
    txn.dependency.append("id.proto")
    txn.message_type.add(name="Txn").field.add(name="identifier", number=1, type=11, type_name=".test.Id")
    identifier = files.file.add(name="id.proto", package="test", syntax="proto3")
    identifier.message_type.add(name="Id").field.add(name="value", number=1, type=9)
    decoder = ProtobufValue(files.SerializeToString(), "test.Txn")
    assert decoder.decode(b"\x0a\x03\x0a\x01A") == {"identifier": {"value": "A"}}


def test_avro_transport_failure_aborts_instead_of_becoming_bad_record():
    from httpx import ConnectError
    class OfflineRegistry(Registry):
        def get_schema(self, *args, **kwargs):
            raise ConnectError("https://user:PRIVATE-CONFIG@registry.invalid")
    decoder = AvroValue({"url": "https://registry.invalid"}, registry_factory=OfflineRegistry)
    try:
        with pytest.raises(InfrastructureError) as error:
            decoder.decode(avro_payload())
        assert "PRIVATE-CONFIG" not in str(error.value)
    finally:
        decoder.close()
