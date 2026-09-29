from dataclasses import dataclass
from typing import Any

from .models import MISSING, scalar

OPERATORS = ("equals", "not_equals", "contains", "starts_with", "ends_with", "exists", "not_exists")


@dataclass(frozen=True)
class Rule:
    field: str
    operator: str
    value: Any = None

    def matches(self, data):
        value = data.get(self.field, MISSING)
        if self.operator == "exists":
            return value is not MISSING
        if self.operator == "not_exists":
            return value is MISSING
        equal = type(value) is type(self.value) and value == self.value
        if self.operator == "equals":
            return equal
        if self.operator == "not_equals":
            return not equal
        if not isinstance(value, str) or not isinstance(self.value, str):
            return False
        if self.operator == "contains":
            return self.value in value
        if self.operator == "starts_with":
            return value.startswith(self.value)
        if self.operator == "ends_with":
            return value.endswith(self.value)
        raise ValueError("Unsupported filter operator")


@dataclass(frozen=True)
class Filter:
    rules: tuple[Rule, ...] = ()
    connector: str = "AND"

    def validate(self, fields):
        if self.connector not in ("AND", "OR"):
            raise ValueError("Filter connector must be AND or OR")
        for rule in self.rules:
            if rule.field == "id" or rule.field not in fields:
                raise ValueError("Filter fields must be explicitly mapped data fields")
            if rule.operator not in OPERATORS or not scalar(rule.value):
                raise ValueError("Invalid filter operator or scalar value")

    def matches(self, record):
        if not self.rules:
            return True
        matches = (rule.matches(record.data) for rule in self.rules)
        return all(matches) if self.connector == "AND" else any(matches)
