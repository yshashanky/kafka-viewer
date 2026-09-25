from __future__ import annotations

import atexit
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12


class ConfigError(ValueError):
    """Raised when the viewer configuration is invalid."""


KAFKA_PROPERTY_MAP = {
    "kafka.bootstrap.servers": "bootstrap_servers",
    "kafka.api.version": "api_version",
    "kafka.auto.commit.interval.ms": "auto_commit_interval_ms",
    "kafka.auto.offset.reset": "auto_offset_reset",
    "kafka.check.crcs": "check_crcs",
    "kafka.client.id": "client_id",
    "kafka.connections.max.idle.ms": "connections_max_idle_ms",
    "kafka.consumer.timeout.ms": "consumer_timeout_ms",
    "kafka.enable.auto.commit": "enable_auto_commit",
    "kafka.exclude.internal.topics": "exclude_internal_topics",
    "kafka.fetch.max.bytes": "fetch_max_bytes",
    "kafka.fetch.max.wait.ms": "fetch_max_wait_ms",
    "kafka.fetch.min.bytes": "fetch_min_bytes",
    "kafka.group.id": "group_id",
    "kafka.group.instance.id": "group_instance_id",
    "kafka.heartbeat.interval.ms": "heartbeat_interval_ms",
    "kafka.isolation.level": "isolation_level",
    "kafka.max.in.flight.requests.per.connection": "max_in_flight_requests_per_connection",
    "kafka.max.partition.fetch.bytes": "max_partition_fetch_bytes",
    "kafka.max.poll.interval.ms": "max_poll_interval_ms",
    "kafka.max.poll.records": "max_poll_records",
    "kafka.metadata.max.age.ms": "metadata_max_age_ms",
    "kafka.metrics.enabled": "metrics_enabled",
    "kafka.metrics.num.samples": "metrics_num_samples",
    "kafka.metrics.sample.window.ms": "metrics_sample_window_ms",
    "kafka.receive.buffer.bytes": "receive_buffer_bytes",
    "kafka.receive.message.max.bytes": "receive_message_max_bytes",
    "kafka.reconnect.backoff.max.ms": "reconnect_backoff_max_ms",
    "kafka.reconnect.backoff.ms": "reconnect_backoff_ms",
    "kafka.request.timeout.ms": "request_timeout_ms",
    "kafka.retry.backoff.ms": "retry_backoff_ms",
    "kafka.sasl.mechanism": "sasl_mechanism",
    "kafka.security.protocol": "security_protocol",
    "kafka.session.timeout.ms": "session_timeout_ms",
    "kafka.send.buffer.bytes": "send_buffer_bytes",
    "kafka.ssl.cafile": "ssl_cafile",
    "kafka.ssl.certfile": "ssl_certfile",
    "kafka.ssl.check.hostname": "ssl_check_hostname",
    "kafka.ssl.cipher.suites": "ssl_ciphers",
    "kafka.ssl.ciphers": "ssl_ciphers",
    "kafka.ssl.crlfile": "ssl_crlfile",
    "kafka.ssl.key.password": "ssl_password",
    "kafka.ssl.keyfile": "ssl_keyfile",
    "kafka.ssl.password": "ssl_password",
    "kafka.ssl.protocol": "ssl_protocol",
    "kafka.socks5.proxy": "socks5_proxy",
}

_BOOLEAN_PROPERTIES = {
    "kafka.check.crcs",
    "kafka.enable.auto.commit",
    "kafka.exclude.internal.topics",
    "kafka.metrics.enabled",
    "kafka.ssl.check.hostname",
}

_INTEGER_PROPERTIES = {
    key
    for key, value in KAFKA_PROPERTY_MAP.items()
    if value.endswith("_ms")
    or value.endswith("_bytes")
    or value.endswith("_records")
    or value.endswith("_samples")
}

STORE_CONFIGURATION_PROPERTIES = {
    "kafka.ssl.truststore.location",
    "kafka.ssl.truststore.password",
    "kafka.ssl.truststore.type",
    "kafka.ssl.truststore.cert.alias",
    "kafka.ssl.keystore.location",
    "kafka.ssl.keystore.password",
    "kafka.ssl.keystore.type",
    "kafka.ssl.keystore.key.alias",
    "kafka.ssl.keystore.key.location",
    "kafka.ssl.endpoint.identification.algorithm",
}

_INTERNAL_CONFIGURATION_PROPERTIES = {
    "kafka.sasl.username",
    "kafka.sasl.password",
    "kafka.sasl.jaas.config",
    *STORE_CONFIGURATION_PROPERTIES,
}

_SASL_MECHANISMS_WITH_PLAIN_CREDENTIALS = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
_SUPPORTED_SASL_MECHANISMS = _SASL_MECHANISMS_WITH_PLAIN_CREDENTIALS | {"GSSAPI", "OAUTHBEARER"}
_SUPPORTED_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}

SCHEMA_REGISTRY_URL = "schema.registry.url"
SCHEMA_REGISTRY_AUTH = "schema.registry.basic.auth.user.info"
SCHEMA_REGISTRY_VERIFY = "schema.registry.ssl.verify"
SCHEMA_REGISTRY_CA = "schema.registry.ssl.ca.location"
SCHEMA_REGISTRY_NO_REVOKE = "schema.registry.ssl.no.revoke"
SCHEMA_REGISTRY_CLIENT_CERT = "schema.registry.ssl.certificate.location"
SCHEMA_REGISTRY_CLIENT_KEY = "schema.registry.ssl.key.location"
SCHEMA_REGISTRY_CLIENT_KEY_PASSWORD = "schema.registry.ssl.key.password"
SCHEMA_REGISTRY_LEGACY_VERIFY = "schema.registry.ssl.certificate.verification"

_DIRECT_ALIASES = {
    "bootstrap.servers": "kafka.bootstrap.servers",
    "security.protocol": "kafka.security.protocol",
    "sasl.mechanism": "kafka.sasl.mechanism",
    "sasl.jaas.config": "kafka.sasl.jaas.config",
    "ssl.truststore.location": "kafka.ssl.truststore.location",
    "ssl.truststore.password": "kafka.ssl.truststore.password",
    "ssl.truststore.type": "kafka.ssl.truststore.type",
    "ssl.keystore.location": "kafka.ssl.keystore.location",
    "ssl.keystore.password": "kafka.ssl.keystore.password",
    "ssl.keystore.type": "kafka.ssl.keystore.type",
    "ssl.key.password": "kafka.ssl.key.password",
    "ssl.endpoint.identification.algorithm": "kafka.ssl.endpoint.identification.algorithm",
    "spring.kafka.bootstrap-servers": "kafka.bootstrap.servers",
    "spring.kafka.consumer.group-id": "kafka.group.id",
    "spring.kafka.consumer.auto-offset-reset": "kafka.auto.offset.reset",
}

_SPRING_PROPERTIES_PREFIXES = (
    "spring.kafka.properties.",
    "spring.kafka.consumer.properties.",
    "spring.kafka.producer.properties.",
    "spring.kafka.admin.properties.",
)

_JAVA_COMPATIBILITY_KEYS = {name.removeprefix("kafka.") for name in KAFKA_PROPERTY_MAP}
_JAVA_COMPATIBILITY_KEYS.update(
    {
        "sasl.username",
        "sasl.password",
        "sasl.jaas.config",
        "ssl.endpoint.identification.algorithm",
        "ssl.truststore.location",
        "ssl.truststore.password",
        "ssl.truststore.type",
        "ssl.truststore.cert.alias",
        "ssl.keystore.location",
        "ssl.keystore.password",
        "ssl.keystore.type",
        "ssl.keystore.key.alias",
        "ssl.keystore.key.location",
    }
)

_TEMP_SSL_FILES: set[str] = set()
_INTERMEDIATE_PKCS12_PASSWORD = "kafka-viewer-temp"


@atexit.register
def _cleanup_temp_ssl_files() -> None:
    for path in list(_TEMP_SSL_FILES):
        try:
            Path(path).unlink(missing_ok=True)
        finally:
            _TEMP_SSL_FILES.discard(path)


def load_properties(path: str | Path) -> dict[str, str]:
    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    properties: dict[str, str] = {}
    for line_number, raw_line in enumerate(config_path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ConfigError(f"Invalid configuration line {line_number}: expected key=value")
        key, value = (part.strip() for part in line.split("=", 1))
        if not key:
            raise ConfigError(f"Invalid configuration line {line_number}: empty key")
        canonical_name = _canonical_property_name(key)
        properties[canonical_name or key] = value

    bootstrap_servers = properties.get("kafka.bootstrap.servers", "").strip()
    if not bootstrap_servers:
        raise ConfigError("Missing required property: kafka.bootstrap.servers")
    properties["kafka.bootstrap.servers"] = bootstrap_servers
    return properties


def build_consumer_config(properties: dict[str, str]) -> tuple[dict[str, Any], list[str]]:
    consumer_config: dict[str, Any] = {}
    unsupported: list[str] = []
    for property_name, value in properties.items():
        if property_name in _INTERNAL_CONFIGURATION_PROPERTIES:
            continue
        consumer_name = KAFKA_PROPERTY_MAP.get(property_name)
        if consumer_name is None:
            if _is_kafka_related_property(property_name):
                unsupported.append(property_name)
            continue
        consumer_config[consumer_name] = _convert_value(property_name, value)

    _apply_sasl_configuration(properties, consumer_config, unsupported)
    _apply_ssl_configuration(properties, consumer_config, unsupported)
    consumer_config["enable_auto_commit"] = False
    return consumer_config, sorted(set(unsupported))


def build_schema_registry_config(properties: dict[str, str]) -> dict[str, str] | None:
    schema_properties = [
        key
        for key in properties
        if key.startswith("schema.registry.") and key not in {SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_AUTH}
    ]

    url = properties.get(SCHEMA_REGISTRY_URL, "").strip()
    auth = properties.get(SCHEMA_REGISTRY_AUTH, "")
    legacy_verify = properties.get(SCHEMA_REGISTRY_LEGACY_VERIFY, "").strip()
    if legacy_verify and SCHEMA_REGISTRY_VERIFY in properties:
        raise ConfigError("Use only one Schema Registry TLS verification property: schema.registry.ssl.verify")
    if legacy_verify:
        properties[SCHEMA_REGISTRY_VERIFY] = legacy_verify

    if not url:
        if auth or schema_properties:
            raise ConfigError("Schema Registry configuration requires schema.registry.url")
        return None

    if auth and (":" not in auth or not all(auth.split(":", 1))):
        raise ConfigError("Invalid schema registry authentication configuration")

    config: dict[str, str | bool] = {"url": url}
    if auth:
        config["basic.auth.user.info"] = auth

    verify_value = properties.get(SCHEMA_REGISTRY_VERIFY, "").strip()
    if verify_value:
        normalized = verify_value.lower()
        if normalized not in {"true", "false"}:
            raise ConfigError("Invalid value for schema.registry.ssl.verify; expected true or false")
        if normalized == "false":
            config["ssl.ca.location"] = False

    ca_location = properties.get(SCHEMA_REGISTRY_CA, "").strip()
    if ca_location:
        ca_path = _require_file(SCHEMA_REGISTRY_CA, ca_location)
        if not os.access(ca_path, os.R_OK):
            raise ConfigError(f"Unable to read {SCHEMA_REGISTRY_CA}: file is not readable")
        config["ssl.ca.location"] = str(ca_path)

    no_revoke_value = properties.get(SCHEMA_REGISTRY_NO_REVOKE, "").strip()
    if no_revoke_value:
        normalized = no_revoke_value.lower()
        if normalized not in {"true", "false"}:
            raise ConfigError("Invalid value for schema.registry.ssl.no.revoke; expected true or false")
        if normalized == "true":
            raise ConfigError(
                "schema.registry.ssl.no.revoke=true is not supported by the installed "
                "confluent-kafka/Python SSL stack; use schema.registry.ssl.verify=false "
                "to intentionally disable Schema Registry certificate verification."
            )

    client_cert_location = properties.get(SCHEMA_REGISTRY_CLIENT_CERT, "").strip()
    if client_cert_location:
        config["ssl.certificate.location"] = str(_require_file(SCHEMA_REGISTRY_CLIENT_CERT, client_cert_location))

    client_key_location = properties.get(SCHEMA_REGISTRY_CLIENT_KEY, "").strip()
    if client_key_location:
        key_path = _require_file(SCHEMA_REGISTRY_CLIENT_KEY, client_key_location)
        config["ssl.key.location"] = str(key_path)

    client_key_password = properties.get(SCHEMA_REGISTRY_CLIENT_KEY_PASSWORD, "").strip()
    if client_key_password:
        if not client_cert_location and not client_key_location:
            raise ConfigError(
                "schema.registry.ssl.key.password requires schema.registry.ssl.certificate.location "
                "and/or schema.registry.ssl.key.location"
            )
        config["ssl.key.password"] = client_key_password

    if (client_key_location or client_key_password) and not client_cert_location:
        raise ConfigError(
            "schema.registry.ssl.certificate.location is required when configuring "
            "schema.registry.ssl.key.location or schema.registry.ssl.key.password"
        )

    return dict(config)


def _canonical_property_name(name: str) -> str | None:
    if name.startswith("kafka."):
        return name
    if name in _DIRECT_ALIASES:
        return _DIRECT_ALIASES[name]
    if name in _JAVA_COMPATIBILITY_KEYS:
        return f"kafka.{name}"
    for prefix in _SPRING_PROPERTIES_PREFIXES:
        if name.startswith(prefix):
            suffix = name[len(prefix) :].replace("-", ".")
            if suffix in _JAVA_COMPATIBILITY_KEYS:
                return f"kafka.{suffix}"
    return None


def _apply_sasl_configuration(
    properties: dict[str, str],
    consumer_config: dict[str, Any],
    unsupported: list[str],
) -> None:
    security_protocol = str(consumer_config.get("security_protocol", "PLAINTEXT")).upper()
    if security_protocol not in _SUPPORTED_SECURITY_PROTOCOLS:
        raise ConfigError("Invalid kafka.security.protocol")

    mechanism = properties.get("kafka.sasl.mechanism", "").upper()
    has_sasl = security_protocol.startswith("SASL_")

    username = properties.get("kafka.sasl.username", "")
    password = properties.get("kafka.sasl.password", "")
    if (not username or not password) and "kafka.sasl.jaas.config" in properties:
        jaas_username, jaas_password = _parse_jaas_plain_credentials(properties["kafka.sasl.jaas.config"])
        username = username or jaas_username
        password = password or jaas_password

    has_credentials = bool(username or password)
    if (mechanism or has_credentials) and not has_sasl:
        raise ConfigError("SASL properties require kafka.security.protocol to use SASL")
    if has_sasl and not mechanism:
        raise ConfigError("kafka.sasl.mechanism is required for SASL security")
    if mechanism and mechanism not in _SUPPORTED_SASL_MECHANISMS:
        raise ConfigError("Unsupported kafka.sasl.mechanism")

    if mechanism in _SASL_MECHANISMS_WITH_PLAIN_CREDENTIALS:
        if not username or not password:
            raise ConfigError("kafka.sasl.username and kafka.sasl.password are required for SASL credentials")
        consumer_config["sasl_plain_username"] = username
        consumer_config["sasl_plain_password"] = password
    elif mechanism == "GSSAPI":
        if not username:
            raise ConfigError("kafka.sasl.username is required for GSSAPI")
        consumer_config["sasl_kerberos_name"] = username
        if password:
            unsupported.append("kafka.sasl.password")
    elif has_credentials:
        unsupported.extend(name for name in ("kafka.sasl.username", "kafka.sasl.password") if properties.get(name))


def _apply_ssl_configuration(
    properties: dict[str, str],
    consumer_config: dict[str, Any],
    unsupported: list[str],
) -> None:
    endpoint_identification_algorithm = properties.get("kafka.ssl.endpoint.identification.algorithm")
    if endpoint_identification_algorithm is not None:
        _apply_endpoint_identification_algorithm(endpoint_identification_algorithm, consumer_config, properties)

    truststore_location = properties.get("kafka.ssl.truststore.location", "").strip()
    truststore_type = properties.get("kafka.ssl.truststore.type", "").strip()
    truststore_password = properties.get("kafka.ssl.truststore.password", "")
    truststore_alias = properties.get("kafka.ssl.truststore.cert.alias", "").strip() or None

    if (truststore_password or truststore_type) and not truststore_location:
        raise ConfigError("kafka.ssl.truststore.password and kafka.ssl.truststore.type require kafka.ssl.truststore.location")

    if truststore_location:
        truststore_path = _require_file("kafka.ssl.truststore.location", truststore_location)
        resolved_truststore_type = _resolve_store_type(truststore_type, truststore_path)
        if resolved_truststore_type == "PEM":
            if truststore_password:
                unsupported.append("kafka.ssl.truststore.password")
            consumer_config["ssl_cafile"] = str(truststore_path)
        elif resolved_truststore_type == "PKCS12":
            ca_pem = _extract_pkcs12_truststore_pem(truststore_path, truststore_password)
            consumer_config["ssl_cafile"] = _write_temp_ssl_file(ca_pem, "-truststore.pem")
        elif resolved_truststore_type == "JKS":
            ca_pem = _extract_jks_truststore_pem(truststore_path, truststore_password, truststore_alias)
            consumer_config["ssl_cafile"] = _write_temp_ssl_file(ca_pem, "-truststore.pem")
        else:
            raise ConfigError("Unsupported kafka.ssl.truststore.type")

    keystore_location = properties.get("kafka.ssl.keystore.location", "").strip()
    keystore_type = properties.get("kafka.ssl.keystore.type", "").strip()
    keystore_password = properties.get("kafka.ssl.keystore.password", "")
    key_password = properties.get("kafka.ssl.key.password", properties.get("kafka.ssl.password", ""))
    key_alias = properties.get("kafka.ssl.keystore.key.alias", "").strip() or None

    if (keystore_password or keystore_type) and not keystore_location:
        raise ConfigError("kafka.ssl.keystore.password and kafka.ssl.keystore.type require kafka.ssl.keystore.location")

    if keystore_location:
        keystore_path = _require_file("kafka.ssl.keystore.location", keystore_location)
        resolved_keystore_type = _resolve_store_type(keystore_type, keystore_path)
        if resolved_keystore_type == "PEM":
            _apply_pem_keystore(keystore_path, properties, consumer_config, unsupported)
        elif resolved_keystore_type == "PKCS12":
            cert_pem, key_pem = _extract_pkcs12_keystore_pem(keystore_path, keystore_password, key_password)
            consumer_config["ssl_certfile"] = _write_temp_ssl_file(cert_pem, "-cert.pem")
            consumer_config["ssl_keyfile"] = _write_temp_ssl_file(key_pem, "-key.pem")
            consumer_config.pop("ssl_password", None)
        elif resolved_keystore_type == "JKS":
            cert_pem, key_pem = _extract_jks_keystore_pem(keystore_path, keystore_password, key_password, key_alias)
            consumer_config["ssl_certfile"] = _write_temp_ssl_file(cert_pem, "-cert.pem")
            consumer_config["ssl_keyfile"] = _write_temp_ssl_file(key_pem, "-key.pem")
            consumer_config.pop("ssl_password", None)
        else:
            raise ConfigError("Unsupported kafka.ssl.keystore.type")


def _apply_endpoint_identification_algorithm(
    raw_value: str,
    consumer_config: dict[str, Any],
    properties: dict[str, str],
) -> None:
    normalized = raw_value.strip().lower()
    if normalized in {"", "none"}:
        endpoint_value = False
    elif normalized == "https":
        endpoint_value = True
    else:
        raise ConfigError("Unsupported kafka.ssl.endpoint.identification.algorithm")

    explicit_hostname_check = properties.get("kafka.ssl.check.hostname")
    if explicit_hostname_check is not None:
        configured_hostname_check = _convert_value("kafka.ssl.check.hostname", explicit_hostname_check)
        if configured_hostname_check != endpoint_value:
            raise ConfigError(
                "kafka.ssl.check.hostname conflicts with kafka.ssl.endpoint.identification.algorithm"
            )
    consumer_config["ssl_check_hostname"] = endpoint_value


def _apply_pem_keystore(
    keystore_path: Path,
    properties: dict[str, str],
    consumer_config: dict[str, Any],
    unsupported: list[str],
) -> None:
    key_location = properties.get("kafka.ssl.keystore.key.location", "").strip()
    keyfile = _require_file("kafka.ssl.keystore.key.location", key_location) if key_location else None

    consumer_config["ssl_certfile"] = str(keystore_path)
    if keyfile is not None:
        consumer_config["ssl_keyfile"] = str(keyfile)
    elif "ssl_keyfile" not in consumer_config:
        consumer_config["ssl_keyfile"] = str(keystore_path)

    if properties.get("kafka.ssl.keystore.password"):
        unsupported.append("kafka.ssl.keystore.password")


def _parse_jaas_plain_credentials(value: str) -> tuple[str, str]:
    username_match = re.search(r'username\s*=\s*"([^"]+)"', value)
    password_match = re.search(r'password\s*=\s*"([^"]+)"', value)
    if not username_match or not password_match:
        return "", ""
    return username_match.group(1), password_match.group(1)


def _is_kafka_related_property(name: str) -> bool:
    return (
        name.startswith("kafka.")
        or name.startswith("spring.kafka.")
        or name in _JAVA_COMPATIBILITY_KEYS
    )


def _resolve_store_type(configured_store_type: str, location: Path) -> str:
    if configured_store_type:
        normalized = configured_store_type.strip().upper()
        if normalized == "PFX":
            return "PKCS12"
        return normalized

    suffix = location.suffix.lower()
    if suffix == ".jks":
        return "JKS"
    if suffix in {".p12", ".pfx", ".pkcs12"}:
        return "PKCS12"
    return "PEM"


def _extract_pkcs12_truststore_pem(path: Path, password: str) -> bytes:
    try:
        private_key, certificate, additional_certificates = pkcs12.load_key_and_certificates(
            path.read_bytes(), password.encode("utf-8") if password else None
        )
    except Exception as exc:
        raise ConfigError(
            "Unable to parse kafka.ssl.truststore.location as PKCS12. Check truststore file and password."
        ) from exc

    certificates = []
    if certificate is not None:
        certificates.append(certificate)
    certificates.extend(additional_certificates or [])

    if not certificates:
        raise ConfigError("PKCS12 truststore does not contain certificates")

    return b"".join(certificate.public_bytes(serialization.Encoding.PEM) for certificate in certificates)


def _extract_pkcs12_keystore_pem(path: Path, keystore_password: str, key_password: str) -> tuple[bytes, bytes]:
    passwords_to_try = [keystore_password]
    if key_password and key_password != keystore_password:
        passwords_to_try.append(key_password)
    if "" not in passwords_to_try:
        passwords_to_try.append("")

    last_error: Exception | None = None
    for candidate in passwords_to_try:
        try:
            private_key, certificate, additional_certificates = pkcs12.load_key_and_certificates(
                path.read_bytes(), candidate.encode("utf-8") if candidate else None
            )
        except Exception as exc:
            last_error = exc
            continue
        if private_key is None or certificate is None:
            raise ConfigError("PKCS12 keystore is missing private key or certificate")

        certificate_chain = [certificate]
        certificate_chain.extend(additional_certificates or [])
        certificate_pem = b"".join(cert.public_bytes(serialization.Encoding.PEM) for cert in certificate_chain)
        key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return certificate_pem, key_pem

    raise ConfigError(
        "Unable to parse kafka.ssl.keystore.location as PKCS12. Check keystore file and password."
    ) from last_error


def _extract_jks_truststore_pem(path: Path, password: str, alias: str | None) -> bytes:
    converted_pkcs12 = _convert_jks_to_pkcs12(path, password, "", alias)
    return _extract_pkcs12_truststore_pem(converted_pkcs12, _INTERMEDIATE_PKCS12_PASSWORD)


def _extract_jks_keystore_pem(
    path: Path,
    keystore_password: str,
    key_password: str,
    alias: str | None,
) -> tuple[bytes, bytes]:
    converted_pkcs12 = _convert_jks_to_pkcs12(path, keystore_password, key_password, alias)
    return _extract_pkcs12_keystore_pem(
        converted_pkcs12,
        _INTERMEDIATE_PKCS12_PASSWORD,
        _INTERMEDIATE_PKCS12_PASSWORD,
    )


def _convert_jks_to_pkcs12(
    source_path: Path,
    store_password: str,
    key_password: str,
    alias: str | None,
) -> Path:
    destination_fd, destination_path = tempfile.mkstemp(prefix="kafka-viewer-jks-", suffix=".p12")
    os.close(destination_fd)
    _TEMP_SSL_FILES.add(destination_path)

    command = [
        "keytool",
        "-importkeystore",
        "-noprompt",
        "-srckeystore",
        str(source_path),
        "-srcstoretype",
        "JKS",
        "-destkeystore",
        destination_path,
        "-deststoretype",
        "PKCS12",
        "-deststorepass",
        _INTERMEDIATE_PKCS12_PASSWORD,
    ]

    if store_password:
        command.extend(["-srcstorepass", store_password])
    if key_password:
        command.extend(["-srckeypass", key_password])
    if alias:
        command.extend(["-srcalias", alias])

    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise ConfigError(
            "JKS conversion requires the Java keytool executable on PATH"
        ) from exc

    if result.returncode != 0:
        raise ConfigError(
            "Unable to convert JKS store. Verify file format, alias, and provided passwords."
        )

    return Path(destination_path)


def _require_file(property_name: str, path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_file():
        raise ConfigError(f"Invalid {property_name}: file does not exist")
    return path


def _write_temp_ssl_file(content: bytes, suffix: str) -> str:
    fd, path = tempfile.mkstemp(prefix="kafka-viewer-", suffix=suffix)
    with os.fdopen(fd, "wb") as temporary_file:
        temporary_file.write(content)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    _TEMP_SSL_FILES.add(path)
    return path


def _convert_value(property_name: str, value: str) -> Any:
    if property_name in _BOOLEAN_PROPERTIES:
        normalized = value.lower()
        if normalized not in {"true", "false"}:
            raise ConfigError(f"Invalid boolean value for {property_name}")
        return normalized == "true"
    if property_name in _INTEGER_PROPERTIES:
        try:
            return int(value)
        except ValueError as exc:
            raise ConfigError(f"Invalid integer value for {property_name}") from exc
    return value
