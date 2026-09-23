import argparse
import os
import subprocess
import sys
from pathlib import Path

from .config import load_properties


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the unsecured Kafka viewer")
    parser.add_argument("--config", required=True, help="Path to a properties file")
    args = parser.parse_args()
    config_path = Path(args.config)
    load_properties(config_path)
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
