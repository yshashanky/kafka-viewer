from kafka_viewer.config import build_consumer_config
from kafka_viewer.ui import truncate_broker_display


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