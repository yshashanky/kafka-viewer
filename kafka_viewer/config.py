from pathlib import Path
from typing import Any


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
    "kafka.ssl.ciphers": "ssl_ciphers",
    "kafka.ssl.crlfile": "ssl_crlfile",
    "kafka.ssl.keyfile": "ssl_keyfile",
    "kafka.ssl.password": "ssl_password",
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
    key for key, value in KAFKA_PROPERTY_MAP.items() if value.endswith("_ms") or value.endswith("_bytes") or value.endswith("_records") or value.endswith("_samples")
}
_SASL_MECHANISMS_WITH_PLAIN_CREDENTIALS = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
SCHEMA_REGISTRY_URL = "schema.registry.url"
SCHEMA_REGISTRY_AUTH = "schema.registry.basic.auth.user.info"


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
        properties[key] = value

    bootstrap_servers = properties.get("kafka.bootstrap.servers", "").strip()
    if not bootstrap_servers:
        raise ConfigError("Missing required property: kafka.bootstrap.servers")
    properties["kafka.bootstrap.servers"] = bootstrap_servers
    return properties


def build_consumer_config(properties: dict[str, str]) -> tuple[dict[str, Any], list[str]]:
    consumer_config: dict[str, Any] = {}
    unsupported: list[str] = []
    for property_name, value in properties.items():
        if property_name == "kafka.sasl.username" or property_name == "kafka.sasl.password":
            continue
        consumer_name = KAFKA_PROPERTY_MAP.get(property_name)
        if consumer_name is None:
            if property_name.startswith("kafka."):
                unsupported.append(property_name)
            continue
        consumer_config[consumer_name] = _convert_value(property_name, value)

    security_protocol = consumer_config.get("security_protocol", "PLAINTEXT")
    if security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
        raise ConfigError("Invalid kafka.security.protocol")
    mechanism = properties.get("kafka.sasl.mechanism", "").upper()
    has_sasl = security_protocol.startswith("SASL_")
    has_credentials = "kafka.sasl.username" in properties or "kafka.sasl.password" in properties
    if (mechanism or has_credentials) and not has_sasl:
        raise ConfigError("SASL properties require kafka.security.protocol to use SASL")
    if has_sasl and not mechanism:
        raise ConfigError("kafka.sasl.mechanism is required for SASL security")
    if mechanism and mechanism not in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"}:
        raise ConfigError("Unsupported kafka.sasl.mechanism")
    if mechanism in _SASL_MECHANISMS_WITH_PLAIN_CREDENTIALS:
        username = properties.get("kafka.sasl.username", "")
        password = properties.get("kafka.sasl.password", "")
        if not username or not password:
            raise ConfigError("kafka.sasl.username and kafka.sasl.password are required for SASL credentials")
        consumer_config["sasl_plain_username"] = username
        consumer_config["sasl_plain_password"] = password
    elif mechanism == "GSSAPI":
        username = properties.get("kafka.sasl.username", "")
        if not username:
            raise ConfigError("kafka.sasl.username is required for GSSAPI")
        consumer_config["sasl_kerberos_name"] = username
        if "kafka.sasl.password" in properties:
            unsupported.append("kafka.sasl.password")
    elif has_credentials:
        unsupported.extend(name for name in ("kafka.sasl.username", "kafka.sasl.password") if name in properties)
    consumer_config["enable_auto_commit"] = False
    return consumer_config, unsupported


def build_schema_registry_config(properties: dict[str, str]) -> dict[str, str] | None:
    url = properties.get(SCHEMA_REGISTRY_URL, "").strip()
    auth = properties.get(SCHEMA_REGISTRY_AUTH, "")
    if not url:
        if auth:
            raise ConfigError("schema.registry.basic.auth.user.info requires schema.registry.url")
        return None
    if auth and (":" not in auth or not all(auth.split(":", 1))):
        raise ConfigError("Invalid schema registry authentication configuration")
    config = {"url": url}
    if auth:
        config["basic.auth.user.info"] = auth
    return config


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
