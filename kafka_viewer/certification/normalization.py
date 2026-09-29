from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

from .models import FIELD_NAME, MISSING, NormalizedRecord, RecordError, scalar

PATH_PART = re.compile(r"(?:@?[A-Za-z_][A-Za-z0-9_-]*|[0-9]+|#text)\Z")


@dataclass(frozen=True)
class Mapping:
    path: str
    field: str


def path_parts(path: str) -> tuple[str, ...]:
    if path == "$value":
        return ()
    parts = tuple(path.split("/" if "/" in path else "."))
    if not parts or any(not PATH_PART.fullmatch(p) for p in parts):
        raise ValueError("Use literal dot/slash paths with optional numeric array indices")
    return parts


def validate_mapping(mappings: Iterable[Mapping]) -> tuple[Mapping, ...]:
    mappings = tuple(mappings)
    fields = [m.field for m in mappings]
    if fields.count("id") != 1:
        raise ValueError("Exactly one explicit mapping to id is required")
    if len(fields) != len(set(fields)) or any(not FIELD_NAME.fullmatch(f) for f in fields):
        raise ValueError("Normalized names must be unique flat identifiers")
    paths = [path_parts(m.path) for m in mappings]
    if len(paths) != len(set(paths)):
        raise ValueError("Each input path may be mapped only once")
    return mappings


def validate_pair(source, destination) -> tuple[tuple[Mapping, ...], tuple[Mapping, ...]]:
    source, destination = validate_mapping(source), validate_mapping(destination)
    if {m.field for m in source} != {m.field for m in destination}:
        raise ValueError("Source and destination comparison field sets must match")
    return source, destination


def extract(value: Any, parts: tuple[str, ...]) -> Any:
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part, MISSING)
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return MISSING
    return value


def leaves(value: Any, prefix: tuple[str, ...] = ()):
    if isinstance(value, dict) and value:
        for key in sorted(value):
            yield from leaves(value[key], (*prefix, key))
    elif isinstance(value, list) and value:
        for i, item in enumerate(value):
            yield from leaves(item, (*prefix, str(i)))
    else:
        yield prefix


class Normalizer:
    def __init__(self, mappings: Iterable[Mapping], destination: bool = False):
        self.mappings = validate_mapping(mappings)
        self.paths = {m.field: path_parts(m.path) for m in self.mappings}
        self.destination = destination

    def normalize(self, value: Any, metadata: dict | None = None):
        metadata = metadata or {}
        identifier = extract(value, self.paths["id"])
        if identifier is MISSING or identifier is None or identifier == "":
            return RecordError("NORMALIZATION_ERROR", "MISSING_ID", metadata)
        data = {name: extract(value, path) for name, path in self.paths.items() if name != "id"}
        if not scalar(identifier) or any(not scalar(v) for v in data.values()):
            return RecordError("NORMALIZATION_ERROR", "NON_SCALAR", metadata)
        extras = ()
        if self.destination and isinstance(value, (dict, list)):
            mapped = set(self.paths.values())
            extras = tuple(".".join(p) for p in leaves(value) if p not in mapped)
        return NormalizedRecord(identifier, data, metadata, extras)
