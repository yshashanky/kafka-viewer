import pytest

from kafka_viewer.config import ConfigError, load_properties


def test_load_properties_trims_and_ignores_comments(tmp_path):
    path = tmp_path / "viewer.properties"
    path.write_text("\n # comment\n kafka.bootstrap.servers = broker:9092 \n", encoding="utf-8")

    assert load_properties(path) == {"kafka.bootstrap.servers": "broker:9092"}


def test_load_properties_requires_bootstrap_servers(tmp_path):
    path = tmp_path / "viewer.properties"
    path.write_text("other=value\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="kafka.bootstrap.servers"):
        load_properties(path)


def test_load_properties_reports_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="not found"):
        load_properties(tmp_path / "missing.properties")


def test_load_properties_rejects_malformed_line(tmp_path):
    path = tmp_path / "viewer.properties"
    path.write_text("not-a-property\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="key=value"):
        load_properties(path)
