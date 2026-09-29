import io
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest

from kafka_viewer.certification import certify, NormalizedRecord, RecordError
from kafka_viewer.certification import cli
from kafka_viewer.certification.adapters.base import InputRecord
from kafka_viewer.certification.filtering import Filter, Rule
from kafka_viewer.certification.models import MISSING
from kafka_viewer.certification.report import write_html


def r(identifier, value="yes", **kwargs):
    return NormalizedRecord(identifier, {"value": value}, **kwargs)


def test_html_all_categories_occurrences_fields_escaping_and_errors(tmp_path):
    source = [r("match<script>alert(1)</script>"), r("mismatch"), r("missing"), r("duplicate"),
              r("duplicate"), r("filtered", "no"), r("type", 100), r("empty", MISSING),
              RecordError("NORMALIZATION_ERROR", "MISSING_ID", {"offset": 77, "password": "CONFIG-SECRET"})]
    destination = [r("match<script>alert(1)</script>"), r("mismatch", "changed"), r("extra"),
                   r("filtered", "yes"), r("type", "100"), r("empty", None, extra_fields=("unexpected",)),
                   RecordError("DESERIALIZATION_ERROR", "INVALID_PAYLOAD", {"offset": 88})]
    with certify(source, destination, fields=["value"],
                 source_filter=Filter((Rule("value", "not_equals", "no"),))) as result:
        path = tmp_path / "report.html"
        with path.open("w", encoding="utf-8") as output:
            write_html(result, output)
        html = path.read_text(encoding="utf-8")
        for category in ("MATCHED", "MISMATCHED", "MISSING_IN_DESTINATION", "EXTRA_IN_DESTINATION",
                         "DUPLICATE", "FILTER_VIOLATION", "MISSING_ID", "DESERIALIZATION_ERROR", "EXTRA_DESTINATION_FIELD"):
            assert category in html
        assert "<script>" not in html
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
        assert "CONFIG-SECRET" not in html
        assert "source_type" in html and "type_match" in html and "value_equal" in html
        assert "MISSING" in html and "null" in html
        assert "Occurrence 2" in html
        assert html.count("<details open>") == 14
        assert "77" in html and "88" in html


def test_streaming_report_has_no_evidence_limit():
    class CountingWriter:
        def __init__(self):
            self.calls, self.size, self.largest, self.occurrences = 0, 0, 0, 0
        def write(self, value):
            self.calls += 1
            self.size += len(value)
            self.largest = max(self.largest, len(value))
            self.occurrences += value.count("<details open>")
    with certify((r(str(i)) for i in range(2000)), (r(str(i)) for i in range(2000)), fields=["value"]) as result:
        writer = CountingWriter()
        write_html(result, writer)
        assert writer.occurrences == 4000
        assert writer.calls > 20000
        assert writer.largest < 10000
        assert result.counts["matched"] == 2000


@pytest.mark.parametrize("separate", [False, True])
def test_cli_launches_separate_streamlit_ui(tmp_path, monkeypatch, separate):
    source = tmp_path / "source config.properties"
    destination = tmp_path / "destination config.properties"
    source.write_text("kafka.bootstrap.servers=localhost:9092", encoding="utf-8")
    destination.write_text("kafka.bootstrap.servers=localhost:9093", encoding="utf-8")
    args = ["kafka-viewer-certify", "--source-config", str(source)]
    if separate:
        args += ["--destination-config", str(destination)]
    monkeypatch.setattr(sys, "argv", args)
    calls = []
    monkeypatch.setattr(cli.subprocess, "call", lambda command, env: calls.append((command, env)) or 0)
    with pytest.raises(SystemExit) as exit_info:
        cli.main()
    assert exit_info.value.code == 0
    command, env = calls[0]
    assert command[:4] == [sys.executable, "-m", "streamlit", "run"]
    assert Path(command[4]).parent.name == "certification"
    assert str(source) in command
    assert ("--destination-config" in command) is separate
    assert env["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] == "false"


@pytest.mark.parametrize("args", [[], ["--source-config", "missing.properties"]])
def test_cli_missing_source_clean_error(monkeypatch, capsys, args):
    monkeypatch.setattr(sys, "argv", ["kafka-viewer-certify", *args])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "Traceback" not in capsys.readouterr().err


def test_cli_help(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["kafka-viewer-certify", "--help"])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 0
    assert "--source-config" in capsys.readouterr().out


UI = Path(__file__).parents[2] / "kafka_viewer" / "certification" / "ui.py"


def ui_setup(monkeypatch, tmp_path):
    from kafka_viewer.certification.adapters.kafka import KafkaAdapter
    path = tmp_path / "connection.properties"
    path.write_text("kafka.bootstrap.servers=localhost:9092", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [str(UI), "--source-config", str(path)])
    monkeypatch.setattr(KafkaAdapter, "topics", lambda self: {"source-topic", "destination-topic"})
    return AppTest.from_file(str(UI), default_timeout=60)


def test_ui_startup_default_scope_and_invalid_mapping_blocks_reads(monkeypatch, tmp_path):
    from kafka_viewer.certification.adapters.kafka import KafkaAdapter
    app = ui_setup(monkeypatch, tmp_path).run()
    assert not app.exception
    assert app.radio[0].value == "Time Range"
    assert app.title[0].value == "Data Certification"
    calls = []
    monkeypatch.setattr(KafkaAdapter, "read", lambda *args: calls.append(True))
    next(b for b in app.button if b.label == "Run Certification").click().run()
    assert not app.exception
    assert "Exactly one" in app.error[0].value
    assert not calls


def test_ui_complete_raw_run_and_download(monkeypatch, tmp_path):
    from kafka_viewer.certification.adapters.kafka import KafkaAdapter
    # AppTest cannot edit data_editor cells directly; supply Streamlit's documented edit state.
    app = ui_setup(monkeypatch, tmp_path).run()
    app.button(key="Source-test").click().run()
    app.button(key="Destination-test").click().run()
    app.selectbox(key="Source-topic").select("source-topic")
    app.selectbox(key="Destination-topic").select("destination-topic")
    app.radio[0].set_value("Entire Retained Data")
    for side in ("Source", "Destination"):
        app.session_state[side + "-mapping"] = {
            "edited_rows": {0: {"path": "$value", "field": "id"}}, "added_rows": [], "deleted_rows": [],
        }
    monkeypatch.setattr(KafkaAdapter, "read", lambda self, topic, scope: iter([InputRecord(b"A", {"topic": topic, "offset": 0})]))
    next(b for b in app.button if b.label == "Run Certification").click().run()
    assert not app.exception
    assert app.session_state["certification-summary"]["status"] == "PASSED"
    assert b"MATCHED" in app.session_state["certification-report"]
    assert len(app.get("download_button")) == 1


def test_core_has_no_provider_or_ui_imports():
    import ast
    package = UI.parent
    for name in ("engine.py", "models.py", "comparison.py", "result.py", "filtering.py"):
        tree = ast.parse((package / name).read_text(encoding="utf-8"))
        imports = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        imports += [alias.name for n in ast.walk(tree) if isinstance(n, ast.Import) for alias in n.names]
        assert not any(module.startswith(("kafka.", "confluent_kafka", "streamlit", "google.protobuf", "defusedxml")) for module in imports)


def test_no_kafka_mutation_or_executable_transforms():
    import ast
    for path in UI.parent.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                function = node.func
                if isinstance(function, ast.Name):
                    assert function.id not in {"eval", "exec", "KafkaProducer", "KafkaAdminClient"}
                if isinstance(function, ast.Attribute) and function.attr == "commit":
                    # SQLite transactions are the only permitted commit calls.
                    assert path.name == "sqlite_index.py"
