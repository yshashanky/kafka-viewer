from pathlib import Path

import pytest

from kafka_viewer import cli


def test_cli_requires_config_option(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["kafka-viewer-unsecured"])

    with pytest.raises(SystemExit) as result:
        cli.main()

    assert result.value.code == 2
    assert "the following arguments are required: --config" in capsys.readouterr().err


def test_cli_fails_before_starting_ui_for_missing_config_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.subprocess, "call", lambda command: pytest.fail("UI should not start"))
    monkeypatch.setattr("sys.argv", ["kafka-viewer-unsecured", "--config", "missing.properties"])

    with pytest.raises(FileNotFoundError, match="not found"):
        cli.main()


def test_cli_passes_config_to_streamlit(tmp_path, monkeypatch):
    config = tmp_path / "viewer.properties"
    config.write_text("kafka.bootstrap.servers=broker:9092\n", encoding="utf-8")
    calls = []
    monkeypatch.setattr(cli.subprocess, "call", lambda command: calls.append(command) or 0)
    monkeypatch.setattr("sys.argv", ["kafka-viewer-unsecured", "--config", str(config)])

    with pytest.raises(SystemExit) as result:
        cli.main()

    assert result.value.code == 0
    assert calls and calls[0][-1] == str(config)
