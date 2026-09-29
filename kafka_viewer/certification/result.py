from dataclasses import dataclass, field
from datetime import datetime, timezone

from .storage.base import RecordIndex


@dataclass
class CertificationResult:
    """Evidence is iterable while this context is open; summary survives close()."""

    index: RecordIndex = field(repr=False)
    status: str = "PASSED"
    scope: dict = field(default_factory=dict)
    source_statistics: dict = field(default_factory=lambda: {"records": 0, "normalized": 0, "eligible": 0})
    destination_statistics: dict = field(default_factory=lambda: {"records": 0, "normalized": 0})
    counts: dict = field(default_factory=lambda: dict.fromkeys((
        "matched", "mismatched", "missing_in_destination", "extra_in_destination", "duplicate_ids",
        "filter_violations", "excluded", "deserialization_errors", "normalization_errors",
        "destination_extra_fields", "field_mismatches", "type_mismatches", "unique_expected",
    ), 0))
    quality_percentage: float = 100.0
    execution_error: str | None = None
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def summary(self):
        return {"status": self.status, "scope": self.scope, "generated_at": self.generated_at,
                "source_statistics": dict(self.source_statistics),
                "destination_statistics": dict(self.destination_statistics), **self.counts,
                "quality_percentage": self.quality_percentage, "execution_error": self.execution_error}

    def close(self):
        self.index.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
