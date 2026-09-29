from __future__ import annotations

from .comparison import compare
from .filtering import Filter
from .models import FIELD_NAME, IncompleteScope, NormalizedRecord, RecordError
from .result import CertificationResult
from .storage.sqlite_index import SQLiteIndex, loads


def certify(source, destination, *, fields, source_filter=None, scope=None, index=None):
    """Consume normalized streams once. Caller must close the returned result.

    Duplicate classification takes precedence, but an excluded ID's destination
    occurrence is independently flagged as a filter violation, even for duplicates.
    """
    source_filter = source_filter or Filter()
    fields = set(fields)
    if any(not isinstance(f, str) or f == "id" or not FIELD_NAME.fullmatch(f) for f in fields):
        raise ValueError("Comparison fields must be flat data field names")
    source_filter.validate(fields)
    result = CertificationResult(index if index is not None else SQLiteIndex(), scope=scope or {})
    complete = True
    try:
        for side, stream in (("source", source), ("destination", destination)):
            stats = result.source_statistics if side == "source" else result.destination_statistics
            try:
                for record in stream:
                    stats["records"] += 1
                    if isinstance(record, RecordError):
                        result.counts["deserialization_errors" if record.category == "DESERIALIZATION_ERROR"
                                      else "normalization_errors"] += 1
                        result.index.insert(side, record, False)
                        continue
                    if not isinstance(record, NormalizedRecord) or not set(record.data) <= fields:
                        raise ValueError("Engine input must use the declared normalized schema")
                    stats["normalized"] += 1
                    eligible = side != "source" or source_filter.matches(record)
                    if side == "source":
                        stats["eligible"] += int(eligible)
                    else:
                        result.counts["destination_extra_fields"] += len(record.extra_fields)
                    result.index.insert(side, record, eligible)
            except IncompleteScope:
                result.status, complete = "INCOMPLETE", False
                result.execution_error = "Requested snapshot was not fully read"
            except Exception:
                result.status, complete = "ERROR", False
                result.execution_error = "Input processing failed; check connection and format configuration"
                break
            finally:
                close = getattr(stream, "close", None)
                if close:
                    try:
                        close()
                    except Exception:
                        result.status, complete = "ERROR", False
                        result.execution_error = "Input resource cleanup failed"
        if result.counts["deserialization_errors"] or result.counts["normalization_errors"]:
            if result.status != "ERROR":
                result.status = "INCOMPLETE"
            complete = False
        result.index.flush()
        for key, source_count, destination_count, eligible_count, excluded_count in result.index.groups():
            evidence = {"key": key, "id": loads(key), "source_occurrences": source_count,
                        "destination_occurrences": destination_count, "fields": [],
                        "filter_violation": bool(excluded_count and destination_count)}
            if source_count == 1 and eligible_count:
                result.counts["unique_expected"] += 1
            if evidence["filter_violation"]:
                result.counts["filter_violations"] += 1
            if source_count > 1 or destination_count > 1:
                category, counter = "DUPLICATE", "duplicate_ids"
            elif excluded_count:
                category, counter = ("FILTER_VIOLATION", None) if destination_count else ("EXCLUDED", "excluded")
            elif not source_count:
                category, counter = "EXTRA_IN_DESTINATION", "extra_in_destination"
            elif not destination_count:
                category, counter = "MISSING_IN_DESTINATION", "missing_in_destination"
            else:
                def one(side):
                    payload = next(iter(result.index.occurrences(key, side)))
                    payload.pop("eligible")
                    return NormalizedRecord(**payload)
                evidence["fields"] = compare(one("source"), one("destination"))
                mismatch = sum(not f["value_equal"] for f in evidence["fields"])
                result.counts["field_mismatches"] += mismatch
                result.counts["type_mismatches"] += sum(not f["type_match"] for f in evidence["fields"])
                category, counter = ("MISMATCHED", "mismatched") if mismatch else ("MATCHED", "matched")
            evidence["category"] = category
            evidence["provisional"] = not complete
            if counter:
                result.counts[counter] += 1
            result.index.add_evidence(evidence)
        result.index.flush()
        denominator = result.counts["unique_expected"]
        result.quality_percentage = 100 * result.counts["matched"] / denominator if denominator else 100.0
        if complete:
            if result.counts["deserialization_errors"] or result.counts["normalization_errors"]:
                result.status = "INCOMPLETE"
            elif any(result.counts[k] for k in (
                "mismatched", "missing_in_destination", "extra_in_destination", "duplicate_ids",
                "filter_violations", "destination_extra_fields",
            )):
                result.status = "FAILED"
        return result
    except BaseException:
        result.close()
        raise
