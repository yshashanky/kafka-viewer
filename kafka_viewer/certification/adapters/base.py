from dataclasses import dataclass
from typing import Protocol, Iterable

from ..scope import Scope


@dataclass(frozen=True)
class InputRecord:
    value: bytes | None
    metadata: dict


class ReadOnlyAdapter(Protocol):
    def topics(self) -> set[str]: ...
    def read(self, topic: str, scope: Scope) -> Iterable[InputRecord]: ...
