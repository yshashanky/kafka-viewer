from kafka_viewer.config import build_consumer_config
from kafka_viewer.ui import LOAD_BUTTON_COLOR, REFRESH_STATISTICS_COLOR, truncate_broker_display
from pathlib import Path

import pytest


UI_SOURCE = Path(__file__).parents[1] / "kafka_viewer" / "ui.py"


def test_short_broker_display_is_unchanged():
    assert truncate_broker_display("localhost:9092") == "localhost:9092"


def test_long_broker_display_is_truncated_with_ellipsis():
    brokers = "broker-1.example.com:9093,broker-2.example.com:9093,broker-3.example.com:9093"

    displayed = truncate_broker_display(brokers, max_length=48)

    assert displayed.endswith("...")
    assert len(displayed) == 48
    assert displayed != brokers


def test_multiple_endpoints_are_truncated_without_reconstructing_them():
    brokers = "broker-1:9093,broker-2:9093,broker-3:9093"

    displayed = truncate_broker_display(brokers, max_length=24)

    assert displayed == "broker-1:9093,broker-..."


def test_truncation_does_not_change_underlying_consumer_broker_value():
    brokers = "broker-1:9093,broker-2:9093,broker-3:9093"

    consumer_config, _ = build_consumer_config({"kafka.bootstrap.servers": brokers})

    assert truncate_broker_display(brokers, max_length=24) != consumer_config["bootstrap_servers"]
    assert consumer_config["bootstrap_servers"] == brokers


def test_empty_broker_display_is_safe():
    assert truncate_broker_display("") == ""
    assert truncate_broker_display(None) == ""


def test_action_button_colors_match_requested_palette():
    assert LOAD_BUTTON_COLOR == "#86EFAC"
    assert REFRESH_STATISTICS_COLOR == "#93C5FD"


def test_status_controls_use_equal_nested_columns():
    source = UI_SOURCE.read_text(encoding="utf-8")

    assert "connection_col, topics_col = controls_col.columns(2)" in source


def test_load_messages_updates_shared_latest_timestamp_state():
    source = UI_SOURCE.read_text(encoding="utf-8")

    assert "client.get_latest_record_timestamp(topic, group_id=group_id or None)" in source
    assert "replace(statistics, latest_timestamp=latest_timestamp)" in source
    assert "st.session_state.latest_record_timestamp = latest_timestamp" in source
    assert "st.session_state.latest_record_timestamp = st.session_state.topic_statistics.latest_timestamp" in source


@pytest.mark.parametrize("timestamp,total,expected", [
    (1790677194123, 12, None),
    (None, 12, "Unavailable"),
    (None, 0, "No records"),
])
def test_latest_timestamp_display_preserves_value_and_other_metrics(monkeypatch, timestamp, total, expected):
    from datetime import datetime, timezone
    import sys

    from streamlit.testing.v1 import AppTest
    from kafka_viewer.kafka_client import KafkaClient, TopicStatistics
    import kafka_viewer.config as config

    monkeypatch.setattr(sys, "argv", [str(UI_SOURCE), "--config", "display-test.properties"])
    monkeypatch.setattr(config, "load_properties", lambda path: {"kafka.bootstrap.servers": "localhost:9092"})
    monkeypatch.setattr(KafkaClient, "topics", lambda self: {"test-topic"})
    monkeypatch.setattr(KafkaClient, "get_topic_statistics", lambda *args, **kwargs: TopicStatistics(total, 7, 3, timestamp, 2))

    app = AppTest.from_file(str(UI_SOURCE), default_timeout=60).run()
    assert not app.exception
    if expected is None:
        expected = datetime.fromtimestamp(timestamp / 1000, timezone.utc).astimezone().strftime("%d-%b-%Y %H:%M:%S")
        assert expected.endswith(":54")
    timestamp_markup, = [m.value for m in app.markdown if '<div class="kv-latest-record-timestamp">' in m.value]
    assert f'title="{expected}"' in timestamp_markup
    assert f'>{expected}</div>' in timestamp_markup
    assert [(m.label, m.value) for m in app.metric] == [
        ("Total Records", str(total)), ("Published Today", "7"),
        ("Last 1 Hour", "3"), ("Partitions", "2"),
    ]


def test_viewer_wires_configured_budget_and_displays_effective_cap(monkeypatch):
    import sys
    from streamlit.testing.v1 import AppTest
    from kafka_viewer.kafka_client import KafkaClient, TopicStatistics
    import kafka_viewer.config as config

    monkeypatch.setattr(sys, "argv", [str(UI_SOURCE), "--config", "display-test.properties"])
    monkeypatch.setattr(config, "load_properties", lambda path: {
        "kafka.bootstrap.servers": "localhost:9092", "kafka.viewer.filter.scan.max.records": "20000",
    })
    budgets = []
    def topics(client):
        budgets.append(client.filter_scan_max_records)
        return {"test-topic"}
    monkeypatch.setattr(KafkaClient, "topics", topics)
    monkeypatch.setattr(KafkaClient, "get_topic_statistics", lambda *args, **kwargs: TopicStatistics(0, 0, 0, None, 1))
    app = AppTest.from_file(str(UI_SOURCE), default_timeout=60).run()
    assert not app.exception
    assert budgets == [20000]
    next(t for t in app.text_input if t.label == "Message Filter (optional)").set_value("match").run()
    assert any("20,000 inspected records" in c.value for c in app.caption)
    app.radio[0].set_value("From beginning").run()
    assert not app.exception
    assert any("5,000 inspected records" in c.value for c in app.caption)
