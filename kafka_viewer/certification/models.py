from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any


class Missing(Enum):
    VALUE = "MISSING"


MISSING = Missing.VALUE
FIELD_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
METADATA_FIELDS = {"topic", "partition", "offset", "timestamp"}


def scalar(value: Any) -> bool:
    if value is MISSING or value is None or type(value) in (str, bool, int):
        return True
    if type(value) is float:
        return math.isfinite(value)
    return isinstance(value, Decimal) and value.is_finite()


def safe_metadata(metadata: dict) -> dict:
    return {k: v for k, v in metadata.items() if k in METADATA_FIELDS and scalar(v)}


@dataclass(frozen=True)
class NormalizedRecord:
    id: Any
    data: dict[str, Any]
    metadata: dict = field(default_factory=dict)
    extra_fields: tuple[str, ...] = ()

    def __post_init__(self):
        if self.id is MISSING or self.id is None or self.id == "" or not scalar(self.id):
            raise ValueError("MISSING_ID: a nonempty scalar ID is required")
        if any(not isinstance(k, str) or not FIELD_NAME.fullmatch(k) or k == "id" or not scalar(v)
               for k, v in self.data.items()):
            raise ValueError("Normalized data requires flat, named scalar fields")
        object.__setattr__(self, "metadata", safe_metadata(self.metadata))


@dataclass(frozen=True)
class RecordError:
    category: str
    code: str
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if (self.category, self.code) not in {
            ("DESERIALIZATION_ERROR", "INVALID_PAYLOAD"),
            ("NORMALIZATION_ERROR", "MISSING_ID"),
            ("NORMALIZATION_ERROR", "NON_SCALAR"),
        }:
            raise ValueError("Unsupported record error code")
        object.__setattr__(self, "metadata", safe_metadata(self.metadata))


class InfrastructureError(RuntimeError):
    """An infrastructure failure; callers must not expose its underlying cause."""


class IncompleteScope(RuntimeError):
    """A bounded scan could not finish the requested snapshot."""
