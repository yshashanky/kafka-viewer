from pathlib import Path

import pytest

from kafka_viewer import config
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


def test_load_properties_normalizes_java_and_spring_names(tmp_path):
    path = tmp_path / "viewer.properties"
    path.write_text(
        "\n".join(
            [
                "spring.kafka.bootstrap-servers=broker:9092",
                "spring.kafka.properties.security.protocol=SASL_SSL",
                "spring.kafka.properties.sasl.mechanism=PLAIN",
                'spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p";',
                "ssl.endpoint.identification.algorithm=https",
            ]
        ),
        encoding="utf-8",
    )

    loaded = load_properties(path)

    assert loaded["kafka.bootstrap.servers"] == "broker:9092"
    assert loaded["kafka.security.protocol"] == "SASL_SSL"
    assert loaded["kafka.sasl.mechanism"] == "PLAIN"
    assert loaded["kafka.sasl.jaas.config"].startswith("org.apache.kafka")
    assert loaded["kafka.ssl.endpoint.identification.algorithm"] == "https"


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
    consumer_config, unsupported = build_consumer_config({"kafka.bootstrap.servers": "broker:9092"})

    assert consumer_config == {"bootstrap_servers": "broker:9092", "enable_auto_commit": False}
    assert unsupported == []


def test_build_consumer_config_maps_ssl_properties():
    consumer_config, unsupported = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9093",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.protocol": "TLSv1.2",
            "kafka.ssl.cipher.suites": "TLS_AES_128_GCM_SHA256",
            "kafka.ssl.endpoint.identification.algorithm": "https",
        }
    )

    assert consumer_config["security_protocol"] == "SSL"
    assert consumer_config["ssl_protocol"] == "TLSv1.2"
    assert consumer_config["ssl_ciphers"] == "TLS_AES_128_GCM_SHA256"
    assert consumer_config["ssl_check_hostname"] is True
    assert unsupported == []


def test_build_consumer_config_supports_sasl_plain_from_jaas():
    consumer_config, unsupported = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9093",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="password";',
        }
    )

    assert consumer_config["sasl_plain_username"] == "user"
    assert consumer_config["sasl_plain_password"] == "password"
    assert unsupported == []


def test_build_consumer_config_maps_sasl_plain_properties():
    consumer_config, unsupported = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9093",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.username": "user",
            "kafka.sasl.password": "password",
        }
    )

    assert consumer_config["security_protocol"] == "SASL_SSL"
    assert consumer_config["sasl_mechanism"] == "PLAIN"
    assert consumer_config["sasl_plain_username"] == "user"
    assert consumer_config["sasl_plain_password"] == "password"
    assert consumer_config["enable_auto_commit"] is False
    assert unsupported == []


def test_build_consumer_config_reports_unsupported_kafka_properties():
    consumer_config, unsupported = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "kafka.unsupported.setting": "value",
            "spring.kafka.properties.unknown.setting": "value",
        }
    )

    assert "kafka.unsupported.setting" in unsupported
    assert "spring.kafka.properties.unknown.setting" in unsupported
    assert "unsupported" not in consumer_config


def test_build_consumer_config_rejects_incomplete_sasl_configuration():
    with pytest.raises(ConfigError, match="sasl.mechanism"):
        build_consumer_config({"kafka.bootstrap.servers": "broker:9092", "kafka.security.protocol": "SASL_SSL"})


def test_build_consumer_config_rejects_invalid_endpoint_identification_algorithm():
    with pytest.raises(ConfigError, match="endpoint.identification.algorithm"):
        build_consumer_config(
            {
                "kafka.bootstrap.servers": "broker:9092",
                "kafka.security.protocol": "SSL",
                "kafka.ssl.endpoint.identification.algorithm": "ldap",
            }
        )


def test_build_consumer_config_rejects_conflicting_hostname_checks():
    with pytest.raises(ConfigError, match="conflicts"):
        build_consumer_config(
            {
                "kafka.bootstrap.servers": "broker:9092",
                "kafka.security.protocol": "SSL",
                "kafka.ssl.endpoint.identification.algorithm": "none",
                "kafka.ssl.check.hostname": "true",
            }
        )


def test_build_consumer_config_reports_missing_truststore_file():
    with pytest.raises(ConfigError, match="kafka.ssl.truststore.location"):
        build_consumer_config(
            {
                "kafka.bootstrap.servers": "broker:9092",
                "kafka.security.protocol": "SSL",
                "kafka.ssl.truststore.location": "missing-truststore.p12",
            }
        )


def test_build_consumer_config_rejects_orphaned_truststore_password():
    with pytest.raises(ConfigError, match="kafka.ssl.truststore.password"):
        build_consumer_config(
            {
                "kafka.bootstrap.servers": "broker:9092",
                "kafka.ssl.truststore.password": "top-secret",
            }
        )


def test_build_consumer_config_maps_pkcs12_truststore(tmp_path, monkeypatch):
    truststore = tmp_path / "truststore.p12"
    truststore.write_bytes(b"dummy")

    monkeypatch.setattr(config, "_extract_pkcs12_truststore_pem", lambda *_args, **_kwargs: b"ca")
    monkeypatch.setattr(config, "_write_temp_ssl_file", lambda content, suffix: f"generated{suffix}")

    consumer_config, unsupported = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.truststore.location": str(truststore),
            "kafka.ssl.truststore.password": "store-password",
            "kafka.ssl.truststore.type": "PKCS12",
        }
    )

    assert consumer_config["ssl_cafile"] == "generated-truststore.pem"
    assert unsupported == []


def test_build_consumer_config_maps_jks_truststore(tmp_path, monkeypatch):
    truststore = tmp_path / "truststore.jks"
    truststore.write_bytes(b"dummy")

    monkeypatch.setattr(config, "_extract_jks_truststore_pem", lambda *_args, **_kwargs: b"ca")
    monkeypatch.setattr(config, "_write_temp_ssl_file", lambda content, suffix: f"generated{suffix}")

    consumer_config, _ = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.truststore.location": str(truststore),
            "kafka.ssl.truststore.password": "store-password",
            "kafka.ssl.truststore.type": "JKS",
        }
    )

    assert consumer_config["ssl_cafile"] == "generated-truststore.pem"


def test_build_consumer_config_uses_pem_truststore_and_flags_password(tmp_path):
    truststore = tmp_path / "ca.pem"
    truststore.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n", encoding="utf-8")

    consumer_config, unsupported = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.truststore.location": str(truststore),
            "kafka.ssl.truststore.type": "PEM",
            "kafka.ssl.truststore.password": "unused",
        }
    )

    assert consumer_config["ssl_cafile"] == str(truststore)
    assert "kafka.ssl.truststore.password" in unsupported


def test_build_consumer_config_maps_pkcs12_keystore(tmp_path, monkeypatch):
    keystore = tmp_path / "keystore.p12"
    keystore.write_bytes(b"dummy")

    monkeypatch.setattr(config, "_extract_pkcs12_keystore_pem", lambda *_args, **_kwargs: (b"cert", b"key"))

    generated_paths = iter(["generated-cert.pem", "generated-key.pem"])
    monkeypatch.setattr(config, "_write_temp_ssl_file", lambda content, suffix: next(generated_paths))

    consumer_config, _ = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.keystore.location": str(keystore),
            "kafka.ssl.keystore.password": "keystore-password",
            "kafka.ssl.key.password": "key-password",
            "kafka.ssl.keystore.type": "PKCS12",
        }
    )

    assert consumer_config["ssl_certfile"] == "generated-cert.pem"
    assert consumer_config["ssl_keyfile"] == "generated-key.pem"
    assert "ssl_password" not in consumer_config


def test_build_consumer_config_maps_jks_keystore(tmp_path, monkeypatch):
    keystore = tmp_path / "keystore.jks"
    keystore.write_bytes(b"dummy")

    monkeypatch.setattr(config, "_extract_jks_keystore_pem", lambda *_args, **_kwargs: (b"cert", b"key"))

    generated_paths = iter(["generated-cert.pem", "generated-key.pem"])
    monkeypatch.setattr(config, "_write_temp_ssl_file", lambda content, suffix: next(generated_paths))

    consumer_config, _ = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.keystore.location": str(keystore),
            "kafka.ssl.keystore.password": "keystore-password",
            "kafka.ssl.keystore.type": "JKS",
        }
    )

    assert consumer_config["ssl_certfile"] == "generated-cert.pem"
    assert consumer_config["ssl_keyfile"] == "generated-key.pem"


def test_build_consumer_config_uses_pem_keystore_and_explicit_key_file(tmp_path):
    cert = tmp_path / "client.pem"
    key = tmp_path / "client.key"
    cert.write_text("CERT", encoding="utf-8")
    key.write_text("KEY", encoding="utf-8")

    consumer_config, _ = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "kafka.security.protocol": "SSL",
            "kafka.ssl.keystore.location": str(cert),
            "kafka.ssl.keystore.key.location": str(key),
            "kafka.ssl.key.password": "key-password",
            "kafka.ssl.keystore.type": "PEM",
        }
    )

    assert consumer_config["ssl_certfile"] == str(cert)
    assert consumer_config["ssl_keyfile"] == str(key)
    assert consumer_config["ssl_password"] == "key-password"


def test_build_consumer_config_keystore_parser_error_hides_password(tmp_path, monkeypatch):
    keystore = tmp_path / "keystore.p12"
    keystore.write_bytes(b"dummy")

    def fail(*_args, **_kwargs):
        raise ConfigError("Unable to parse kafka.ssl.keystore.location as PKCS12. Check keystore file and password.")

    monkeypatch.setattr(config, "_extract_pkcs12_keystore_pem", fail)

    with pytest.raises(ConfigError) as error:
        build_consumer_config(
            {
                "kafka.bootstrap.servers": "broker:9092",
                "kafka.security.protocol": "SSL",
                "kafka.ssl.keystore.location": str(keystore),
                "kafka.ssl.keystore.password": "ultra-secret",
                "kafka.ssl.keystore.type": "PKCS12",
            }
        )

    assert "ultra-secret" not in str(error.value)


def test_schema_registry_is_optional_and_separate_from_kafka_config():
    properties = {"kafka.bootstrap.servers": "broker:9092"}

    assert build_schema_registry_config(properties) is None
    consumer_config, _ = build_consumer_config(properties)
    assert all(not key.startswith("schema.registry") for key in consumer_config)


def test_schema_registry_config_supports_anonymous_access():
    schema_registry_config = build_schema_registry_config(
        {"kafka.bootstrap.servers": "broker:9092", "schema.registry.url": "https://registry.example"}
    )

    assert schema_registry_config == {"url": "https://registry.example"}


def test_schema_registry_config_supports_basic_auth_without_exposing_it():
    schema_registry_config = build_schema_registry_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "schema.registry.url": "https://registry.example",
            "schema.registry.basic.auth.user.info": "test-user:test-password",
        }
    )

    assert schema_registry_config["url"] == "https://registry.example"
    assert schema_registry_config["basic.auth.user.info"] == "test-user:test-password"


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


def test_schema_registry_https_url_with_default_verify_is_supported():
    config = build_schema_registry_config({"schema.registry.url": "https://registry.example:8082"})
    assert config == {"url": "https://registry.example:8082"}


def test_schema_registry_http_url_without_auth_is_supported():
    config = build_schema_registry_config({"schema.registry.url": "http://registry.example:8081"})
    assert config == {"url": "http://registry.example:8081"}


def test_schema_registry_custom_ca_path_is_supported(tmp_path):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n", encoding="utf-8")

    config = build_schema_registry_config(
        {
            "schema.registry.url": "https://registry.example:8082",
            "schema.registry.ssl.ca.location": str(ca_file),
        }
    )

    assert config["url"] == "https://registry.example:8082"
    assert config["ssl.ca.location"] == str(ca_file)


def test_schema_registry_verify_false_disables_tls_verification():
    config = build_schema_registry_config(
        {
            "schema.registry.url": "https://registry.example:8082",
            "schema.registry.ssl.verify": "false",
        }
    )

    assert config["ssl.ca.location"] is False


def test_schema_registry_verify_true_is_validated_and_kept_explicit():
    config = build_schema_registry_config(
        {
            "schema.registry.url": "https://registry.example:8082",
            "schema.registry.ssl.verify": "true",
        }
    )

    assert config["url"] == "https://registry.example:8082"


def test_schema_registry_invalid_verify_value_raises_clear_error():
    with pytest.raises(ConfigError, match="schema.registry.ssl.verify"):
        build_schema_registry_config(
            {
                "schema.registry.url": "https://registry.example:8082",
                "schema.registry.ssl.verify": "maybe",
            }
        )


def test_schema_registry_no_revoke_false_is_accepted():
    config = build_schema_registry_config(
        {
            "schema.registry.url": "https://registry.example:8082",
            "schema.registry.ssl.no.revoke": "false",
        }
    )

    assert config["url"] == "https://registry.example:8082"


def test_schema_registry_no_revoke_true_is_not_supported_in_python_stack():
    with pytest.raises(ConfigError, match="schema.registry.ssl.no.revoke"):
        build_schema_registry_config(
            {
                "schema.registry.url": "https://registry.example:8082",
                "schema.registry.ssl.no.revoke": "true",
            }
        )


def test_schema_registry_missing_ca_file_raises_clear_error(tmp_path):
    missing = tmp_path / "missing-ca.pem"
    with pytest.raises(ConfigError, match="schema.registry.ssl.ca.location"):
        build_schema_registry_config(
            {
                "schema.registry.url": "https://registry.example:8082",
                "schema.registry.ssl.ca.location": str(missing),
            }
        )


def test_schema_registry_client_cert_and_key_are_preserved(tmp_path):
    cert = tmp_path / "client-cert.pem"
    key = tmp_path / "client-key.pem"
    cert.write_text("-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n", encoding="utf-8")
    key.write_text("-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n", encoding="utf-8")

    config = build_schema_registry_config(
        {
            "schema.registry.url": "https://registry.example:8082",
            "schema.registry.ssl.certificate.location": str(cert),
            "schema.registry.ssl.key.location": str(key),
            "schema.registry.ssl.key.password": "key-password",
        }
    )

    assert config["ssl.certificate.location"] == str(cert)
    assert config["ssl.key.location"] == str(key)
    assert config["ssl.key.password"] == "key-password"


def test_schema_registry_properties_do_not_leak_into_kafka_consumer_config():
    consumer_config, _ = build_consumer_config(
        {
            "kafka.bootstrap.servers": "broker:9092",
            "schema.registry.url": "https://registry.example:8082",
            "schema.registry.ssl.verify": "false",
            "schema.registry.basic.auth.user.info": "user:pass",
        }
    )

    assert all(not key.startswith("schema.registry") for key in consumer_config)
    assert consumer_config["bootstrap_servers"] == "broker:9092"


def test_extract_pkcs12_keystore_error_does_not_expose_password(tmp_path):
    keystore = tmp_path / "keystore.p12"
    keystore.write_bytes(b"invalid")

    with pytest.raises(ConfigError) as error:
        config._extract_pkcs12_keystore_pem(Path(keystore), "super-secret", "super-secret")

    assert "super-secret" not in str(error.value)


def test_extract_pkcs12_truststore_error_does_not_expose_password(tmp_path):
    truststore = tmp_path / "truststore.p12"
    truststore.write_bytes(b"invalid")

    with pytest.raises(ConfigError) as error:
        config._extract_pkcs12_truststore_pem(Path(truststore), "super-secret")

    assert "super-secret" not in str(error.value)


def test_jks_conversion_without_keytool_is_reported_safely(tmp_path, monkeypatch):
    jks_store = tmp_path / "store.jks"
    jks_store.write_bytes(b"dummy")

    def fail_run(*_args, **_kwargs):
        raise FileNotFoundError("keytool")

    monkeypatch.setattr(config.subprocess, "run", fail_run)

    with pytest.raises(ConfigError) as error:
        config._convert_jks_to_pkcs12(Path(jks_store), "super-secret", "", None)

    assert "keytool" in str(error.value).lower()
    assert "super-secret" not in str(error.value)


def test_extract_jks_truststore_uses_intermediate_password(monkeypatch, tmp_path):
    jks_store = tmp_path / "store.jks"
    jks_store.write_bytes(b"dummy")

    monkeypatch.setattr(config, "_convert_jks_to_pkcs12", lambda *_args, **_kwargs: Path("converted.p12"))

    captured = {}

    def fake_extract(path, password):
        captured["path"] = path
        captured["password"] = password
        return b"ca"

    monkeypatch.setattr(config, "_extract_pkcs12_truststore_pem", fake_extract)

    assert config._extract_jks_truststore_pem(Path(jks_store), "store-pass", None) == b"ca"
    assert captured["password"] == config._INTERMEDIATE_PKCS12_PASSWORD
