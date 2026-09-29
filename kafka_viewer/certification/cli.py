import argparse
import os
import subprocess
import sys
from pathlib import Path

from .adapters.kafka import load_connections


def parser():
    result = argparse.ArgumentParser(description="Start the read-only Data Certification UI")
    result.add_argument("--source-config", required=True, help="Source Kafka properties file")
    result.add_argument("--destination-config", help="Destination properties; defaults to source connection")
    return result


def main():
    argument_parser = parser()
    args = argument_parser.parse_args()
    try:
        load_connections(args.source_config, args.destination_config)
    except Exception:
        argument_parser.error("Unable to load connection properties; verify file paths and security configuration")
    command = [sys.executable, "-m", "streamlit", "run", str(Path(__file__).with_name("ui.py")),
               "--", "--source-config", str(Path(args.source_config).resolve())]
    if args.destination_config:
        command.extend(["--destination-config", str(Path(args.destination_config).resolve())])
    environment = os.environ.copy()
    environment["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    raise SystemExit(subprocess.call(command, env=environment))


if __name__ == "__main__":
    main()
