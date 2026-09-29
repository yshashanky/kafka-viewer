from decimal import Decimal

import pytest

from kafka_viewer.certification import certify, NormalizedRecord, RecordError
from kafka_viewer.certification.comparison import value_equal
from kafka_viewer.certification.filtering import Filter, Rule
from kafka_viewer.certification.models import MISSING, IncompleteScope, InfrastructureError
from kafka_viewer.certification.normalization import Mapping, Normalizer, validate_pair
from kafka_viewer.certification.storage.sqlite_index import SQLiteIndex, id_key, dumps, loads


def record(identifier="a", value=100, **kwargs):
    return NormalizedRecord(identifier, {"amount": value}, **kwargs)


@pytest.mark.parametrize("left,right,match", [
    (100, "100", True), (Decimal("100.00"), 100, True), (100, "101", False),
    (True, 1, False), (False, "0", False), ("abc", "ABC", False),
    (MISSING, None, True), (None, "", True), (MISSING, "", True),
    ("", 0, False), ("0.1", Decimal("0.10"), True), ("NaN", 0, False),
    ("Infinity", 10, False), (" 1e2 ", 100, True),
])
def test_value_comparison(left, right, match):
    assert value_equal(left, right) is match


@pytest.mark.parametrize("sources,destinations,category,status", [
    ([record()], [record()], "MATCHED", "PASSED"),
    ([record()], [record(value=101)], "MISMATCHED", "FAILED"),
    ([record()], [], "MISSING_IN_DESTINATION", "FAILED"),
    ([], [record()], "EXTRA_IN_DESTINATION", "FAILED"),
    ([record(), record()], [record()], "DUPLICATE", "FAILED"),
    ([record()], [record(), record()], "DUPLICATE", "FAILED"),
    ([record(), record()], [record(), record()], "DUPLICATE", "FAILED"),
    ([record()], [record(value="100")], "MATCHED", "PASSED"),
])
def test_engine_classification(sources, destinations, category, status):
    with certify(sources, destinations, fields=["amount"]) as result:
        evidence, = result.index.evidence()
        assert evidence["category"] == category
        assert evidence["source_occurrences"] == len(sources)
        assert evidence["destination_occurrences"] == len(destinations)
        assert result.status == status
        if category == "DUPLICATE":
            assert evidence["fields"] == []


@pytest.mark.parametrize("a,b", [(1, "1"), (True, 1), (1.0, 1), (Decimal(1), 1)])
def test_ids_never_coerced(a, b):
    with certify([record(a)], [record(b)], fields=["amount"]) as result:
        assert result.counts["missing_in_destination"] == 1
        assert result.counts["extra_in_destination"] == 1


def test_type_evidence_missing_and_quality():
    with certify([record("a", MISSING), record("b"), record("c")],
                 [record("a", None), record("b", "100")], fields=["amount"]) as result:
        evidence = list(result.index.evidence())
        assert evidence[0]["fields"][0]["source"] is MISSING
        assert evidence[0]["fields"][0]["destination"] is None
        assert result.counts["type_mismatches"] == 2
        assert result.counts["field_mismatches"] == 0
        assert result.quality_percentage == pytest.approx(200 / 3)


def test_empty_and_duplicate_denominator():
    with certify([], [], fields=[]) as result:
        assert result.status == "PASSED"
        assert result.quality_percentage == 100
    with certify([record(), record()], [], fields=["amount"]) as result:
        assert result.counts["unique_expected"] == 0
        assert result.counts["duplicate_ids"] == 1


def test_filter_violation_also_retained_for_duplicates():
    source_filter = Filter((Rule("amount", "equals", 100),))
    with certify([record("a", 1), record("a", 100), record("b", 2)],
                 [record("a")], fields=["amount"], source_filter=source_filter) as result:
        assert result.counts["duplicate_ids"] == 1
        assert result.counts["filter_violations"] == 1
        assert result.counts["excluded"] == 1
        assert result.source_statistics["eligible"] == 1
        assert next(result.index.evidence())["filter_violation"]


def test_single_filter_violation():
    with certify([record(value=0)], [record()], fields=["amount"],
                 source_filter=Filter((Rule("amount", "equals", 100),))) as result:
        assert next(result.index.evidence())["category"] == "FILTER_VIOLATION"
        assert result.counts["filter_violations"] == 1


@pytest.mark.parametrize("operator,value,expected", [
    ("equals", "pending", True), ("not_equals", "pending", False),
    ("contains", "end", True), ("starts_with", "pen", True), ("ends_with", "ing", True),
    ("exists", None, True), ("not_exists", None, False), ("contains", ".*", False),
])
def test_filters(operator, value, expected):
    rule = Filter((Rule("amount", operator, value),))
    rule.validate(["amount"])
    assert rule.matches(record(value="pending")) is expected


def test_filter_logic_missing_and_validation():
    rules = (Rule("amount", "equals", 100), Rule("amount", "equals", 1))
    assert not Filter(rules, "AND").matches(record())
    assert Filter(rules, "OR").matches(record())
    assert Filter().matches(record())
    assert Rule("absent", "not_exists").matches({})
    assert Rule("x", "exists").matches({"x": None})
    assert not Rule("x", "equals", True).matches({"x": 1})
    for rule in (Rule("id", "equals", 1), Rule("offset", "equals", 1), Rule("amount", "regex", "x")):
        with pytest.raises(ValueError):
            Filter((rule,)).validate(["amount"])
    with pytest.raises(ValueError):
        Filter((), "XOR").validate([])


def test_normalization_is_explicit_and_asymmetric():
    mappings = [Mapping("transaction.id", "id"), Mapping("payment.amount", "amount"), Mapping("absent", "empty")]
    value = {"transaction": {"id": "A"}, "payment": {"amount": 10}, "internal": "ignored"}
    source = Normalizer(mappings).normalize(value)
    destination = Normalizer(mappings, True).normalize(value)
    assert source.data == {"amount": 10, "empty": MISSING}
    assert source.extra_fields == ()
    assert destination.extra_fields == ("internal",)
    with certify([source], [destination], fields=["amount", "empty"]) as result:
        assert result.status == "FAILED"
        assert result.counts["destination_extra_fields"] == 1
        assert result.counts["matched"] == 1


@pytest.mark.parametrize("mappings", [
    [], [Mapping("a", "x")], [Mapping("a", "id"), Mapping("b", "id")],
    [Mapping("a", "id"), Mapping("a", "x")], [Mapping("a", "id"), Mapping("b", "x.y")],
    [Mapping("__import__('os')", "id")], [Mapping("a//b", "id")],
])
def test_invalid_mappings(mappings):
    with pytest.raises(ValueError):
        Normalizer(mappings)


def test_mapping_pair_requires_same_schema():
    with pytest.raises(ValueError):
        validate_pair([Mapping("x", "id"), Mapping("y", "amount")], [Mapping("z", "id")])
    assert validate_pair([Mapping("x", "id")], [Mapping("z", "id")])


@pytest.mark.parametrize("value", [{}, {"id": None}, {"id": ""}])
def test_missing_id(value):
    error = Normalizer([Mapping("id", "id")]).normalize(value)
    assert error.code == "MISSING_ID"


@pytest.mark.parametrize("value", [[], {}, float("inf"), float("nan")])
def test_non_scalar_mapped_fields(value):
    error = Normalizer([Mapping("id", "id"), Mapping("amount", "amount")]).normalize({"id": "a", "amount": value})
    assert error.code == "NON_SCALAR"


def test_raw_and_index_paths():
    assert Normalizer([Mapping("$value", "id")]).normalize('{"id":"a"}').id == '{"id":"a"}'
    assert Normalizer([Mapping("items.0.id", "id")]).normalize({"items": [{"id": 0}]}).id == 0


@pytest.mark.parametrize("exception,status", [(IncompleteScope, "INCOMPLETE"), (InfrastructureError, "ERROR")])
def test_partial_scan_and_infrastructure_errors(exception, status):
    def source():
        yield record()
        raise exception("password=secret-test-token")
    with certify(source(), [record()], fields=["amount"]) as result:
        assert result.status == status
        assert "secret-test-token" not in str(result.summary())
        assert next(result.index.evidence())["provisional"] is True


def test_record_errors_continue():
    with certify([RecordError("NORMALIZATION_ERROR", "MISSING_ID"), record()],
                 [RecordError("DESERIALIZATION_ERROR", "INVALID_PAYLOAD"), record()], fields=["amount"]) as result:
        assert result.status == "INCOMPLETE"
        assert result.counts["matched"] == 1
        assert len(list(result.index.errors())) == 2
        assert next(result.index.evidence())["provisional"]


def test_index_batched_insert_lookup_cleanup_and_metadata_allowlist():
    with SQLiteIndex() as index:
        path = index.path
        for i in range(1501):
            index.insert("source", record(str(i), metadata={"offset": i, "password": "DO-NOT-STORE"}))
        index.insert("destination", record("1500"))
        groups = list(index.groups())
        assert len(groups) == 1501
        assert next(index.occurrences(id_key("1500"), "source"))["metadata"] == {"offset": 1500}
        index.flush()
        assert b"DO-NOT-STORE" not in path.read_bytes()
    assert not path.exists()
    assert not path.parent.exists()


def test_typed_codec_and_decimal_ids_preserve_precision():
    value = {"a": MISSING, "b": Decimal("123456789012345678901234567890.001"), "c": [True, None, ""]}
    assert loads(dumps(value)) == value
    assert id_key(Decimal("1.00")) == id_key(Decimal("1"))
    assert id_key(Decimal("123456789012345678901234567890.001")) != id_key(Decimal("123456789012345678901234567890.002"))


def test_deterministic_evidence_and_cleanup():
    def run():
        with certify([record("b"), record("a")], [record("a"), record("b")], fields=["amount"]) as result:
            return list(result.index.evidence())
    assert run() == run()


def test_cleanup_failure_sets_error_and_invalid_fields_rejected():
    class Stream:
        def __iter__(self):
            return iter([record()])
        def close(self):
            raise RuntimeError("PRIVATE-CONFIG")
    with certify(Stream(), [], fields=["amount"]) as result:
        assert result.status == "ERROR"
        assert "PRIVATE-CONFIG" not in str(result.summary())
    with pytest.raises(ValueError):
        certify([], [], fields=["id"])


def test_index_cleanup_after_exception():
    with pytest.raises(RuntimeError):
        with SQLiteIndex() as index:
            path = index.path
            index.insert("source", record())
            raise RuntimeError("interrupted")
    assert not path.exists()
