"""Explicit input formats. No auto-detection or executable transformations."""

from .values import AvroValue, JsonValue, ProtobufValue, RawString, XmlValue

__all__ = ["AvroValue", "JsonValue", "ProtobufValue", "RawString", "XmlValue"]
