from pathlib import Path


class ConfigError(ValueError):
    """Raised when the viewer configuration is invalid."""


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
