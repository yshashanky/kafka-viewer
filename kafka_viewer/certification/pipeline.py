from .deserializers.base import DeserializationError
from .models import RecordError


def normalized_stream(records, deserializer, normalizer):
    """Record failures retain coordinates, never raw payloads or library exceptions."""
    try:
        for record in records:
            try:
                value = deserializer.decode(record.value, record.metadata.get("topic", ""))
            except DeserializationError:
                yield RecordError("DESERIALIZATION_ERROR", "INVALID_PAYLOAD", record.metadata)
                continue
            yield normalizer.normalize(value, record.metadata)
    finally:
        close = getattr(records, "close", None)
        if close:
            close()
