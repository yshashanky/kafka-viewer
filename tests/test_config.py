import pytest

from kafka_viewer.config import (
    ConfigError,
    build_consumer_config,
    build_schema_registry_config,
    load_properties,
)


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


def test_build_consumer_config_maps_unsecured_properties():
    config, unsupported = build_consumer_config({"kafka.bootstrap.servers": "broker:9092"})

    assert config == {"bootstrap_servers": "broker:9092", "enable_auto_commit": False}
    assert unsupported == []


def test_build_consumer_config_maps_sasl_plain_properties():
    config, unsupported = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9093",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.username": "user",
            "kafka.sasl.password": "password",
        }
    )

    assert config["security_protocol"] == "SASL_SSL"
    assert config["sasl_mechanism"] == "PLAIN"
    assert config["sasl_plain_username"] == "user"
    assert config["sasl_plain_password"] == "password"
    assert config["enable_auto_commit"] is False
    assert unsupported == []


def test_build_consumer_config_reports_unsupported_kafka_properties():
    config, unsupported = build_consumer_config(
        {"kafka.bootstrap.servers": "broker:9092", "kafka.unsupported.setting": "value"}
    )

    assert "kafka.unsupported.setting" in unsupported
    assert "unsupported" not in config


def test_build_consumer_config_rejects_incomplete_sasl_configuration():
    with pytest.raises(ConfigError, match="sasl.mechanism"):
        build_consumer_config({"kafka.bootstrap.servers": "broker:9092", "kafka.security.protocol": "SASL_SSL"})


def test_schema_registry_is_optional_and_separate_from_kafka_config():
    properties = {"kafka.bootstrap.servers": "broker:9092"}

    assert build_schema_registry_config(properties) is None
    consumer_config, _ = build_consumer_config(properties)
    assert all(not key.startswith("schema.registry") for key in consumer_config)


def test_schema_registry_config_supports_anonymous_access():
    config = build_schema_registry_config(
        {"kafka.bootstrap.servers": "broker:9092", "schema.registry.url": "https://registry.example"}
    )

    assert config == {"url": "https://registry.example"}


def test_schema_registry_config_supports_basic_auth_without_exposing_it():
    config = build_schema_registry_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "schema.registry.url": "https://registry.example",
            "schema.registry.basic.auth.user.info": "test-user:test-password",
        }
    )

    assert config["url"] == "https://registry.example"
    assert config["basic.auth.user.info"] == "test-user:test-password"


def test_schema_registry_auth_requires_url():
    with pytest.raises(ConfigError, match="schema.registry.url"):
        build_schema_registry_config({"schema.registry.basic.auth.user.info": "test-user:test-password"})


def test_schema_registry_auth_rejects_malformed_value():
    with pytest.raises(ConfigError, match="authentication configuration"):
        build_schema_registry_config(
            {
                "schema.registry.url": "https://registry.example",
                "schema.registry.basic.auth.user.info": "malformed",
            }
        )
