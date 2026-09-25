from importlib.metadata import distribution

import snappy
from kafka.codec import snappy_decode, snappy_encode
from packaging.requirements import Requirement


def test_python_snappy_is_declared_as_a_runtime_dependency():
    requirements = distribution("kafka-viewer").requires or []

    snappy_requirements = [Requirement(requirement) for requirement in requirements if Requirement(requirement).name == "python-snappy"]

    assert len(snappy_requirements) == 1
    assert ">=0.6" in str(snappy_requirements[0].specifier)
    assert "<1" in str(snappy_requirements[0].specifier)


def test_python_snappy_is_importable_and_round_trips_payload():
    payload = b"kafka-viewer snappy payload"

    compressed = snappy.compress(payload)

    assert snappy.decompress(compressed) == payload


def test_kafka_python_snappy_codec_round_trips_payload():
    payload = b"kafka-viewer kafka codec payload"

    assert snappy_decode(snappy_encode(payload)) == payload