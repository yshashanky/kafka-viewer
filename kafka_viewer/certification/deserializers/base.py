from typing import Protocol, Any


class DeserializationError(ValueError):
    pass


class Deserializer(Protocol):
    def decode(self, payload: bytes, topic: str = "") -> Any: ...
    def close(self) -> None: ...
