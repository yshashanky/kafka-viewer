import argparse
import os
import subprocess
import sys
from pathlib import Path

from .config import build_consumer_config, build_schema_registry_config, load_properties


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the Kafka viewer")
    parser.add_argument("--config", required=True, help="Path to a properties file")
    args = parser.parse_args()
    config_path = Path(args.config)
    properties = load_properties(config_path)
    build_consumer_config(properties)
    build_schema_registry_config(properties)
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(Path(__file__).with_name("ui.py")),
        "--",
        "--config",
        str(config_path),
    ]
    environment = os.environ.copy()
    environment["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    raise SystemExit(subprocess.call(command, env=environment))


if __name__ == "__main__":
    main()
