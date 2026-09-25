from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, time, timezone

import streamlit as st

from kafka_viewer.config import build_consumer_config, build_schema_registry_config, load_properties
from kafka_viewer.kafka_client import (
    KafkaClient,
    export_messages,
    format_key,
    format_message_value,
    generate_group_id,
    validate_count,
)

BROKER_DISPLAY_MAX_LENGTH = 96


def truncate_broker_display(value: str | None, max_length: int = BROKER_DISPLAY_MAX_LENGTH) -> str:
    if value is None:
        return ""
    if len(value) <= max_length:
        return value
    if max_length <= 3:
        return "." * max_length
    return f"{value[:max_length - 3]}..."


def _config_path() -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default="kafka-viewer.properties")
    return parser.parse_known_args(sys.argv[1:])[0].config


def _date_time(label: str, key: str) -> datetime:
    selected_date = st.date_input(label, key=f"{key}-date")
    selected_time = st.time_input(f"{label} time", value=time(0, 0), key=f"{key}-time")
    return datetime.combine(selected_date, selected_time)


def main() -> None:
    st.set_page_config(page_title="Kafka Viewer", layout="wide")
    st.title("Kafka Viewer")
    config_path = _config_path()
    st.caption(f"Configuration: {config_path}")

    try:
        properties = load_properties(config_path)
    except (OSError, ValueError) as exc:
        st.error(str(exc))
        return

    try:
        consumer_config, unsupported = build_consumer_config(properties)
        schema_registry_config = build_schema_registry_config(properties)
    except ValueError as exc:
        st.error(str(exc))
        return
    for property_name in unsupported:
        st.warning(f"Unsupported Kafka property ignored: {property_name}")
    bootstrap_servers = properties["kafka.bootstrap.servers"]
    st.write(f"Broker: `{truncate_broker_display(bootstrap_servers)}`")
    try:
        client = KafkaClient(consumer_config, schema_registry_config)
    except Exception:
        st.error("Unable to initialize Schema Registry Avro deserialization")
        return
    if "connection_status" not in st.session_state:
        st.session_state.connection_status = "Connecting"
    if "topics" not in st.session_state:
        st.session_state.topics = None
        try:
            st.session_state.topics = sorted(client.topics())
            st.session_state.connection_status = "Connected"
        except Exception as exc:
            st.session_state.connection_status = "Disconnected"
            st.session_state.connection_error = str(exc)

    status = st.session_state.connection_status
    status_col, connection_col, topics_col = st.columns([2, 1, 1])
    status_col.subheader(f"Status: {status}")
    if connection_col.button("Test / Refresh Connection"):
        try:
            st.session_state.topics = sorted(client.topics())
            st.session_state.connection_status = "Connected"
            st.session_state.pop("connection_error", None)
        except Exception as exc:
            st.session_state.connection_status = "Disconnected"
            st.session_state.connection_error = str(exc)
        st.rerun()
    if topics_col.button("Refresh Topic"):
        try:
            st.session_state.topics = sorted(client.topics())
        except Exception as exc:
            st.error(f"Unable to discover topics: {exc}")
        st.rerun()
    if status == "Disconnected":
        st.error(f"Kafka connection failed: {st.session_state.get('connection_error', 'unknown error')}")
    if status != "Connected":
        return

    topics = st.session_state.get("topics") or []
    if not topics:
        st.info("No topics are available.")
        return

    topic_col, group_id_col = st.columns(2)
    topic = topic_col.selectbox("Topic", topics)
    group_id = group_id_col.text_input("Consumer group ID", value=st.session_state.get("group_id", ""))
    prefix_col, generate_col = st.columns(2)
    group_prefix = prefix_col.text_input("Group ID Prefix (optional)", value=st.session_state.get("group_id_prefix", ""))
    if generate_col.button("Generate Temporary Group ID", use_container_width=True):
        st.session_state.group_id_prefix = group_prefix
        st.session_state.group_id = generate_group_id(group_prefix)
        st.rerun()
    group_id = st.session_state.get("group_id", group_id)
    modes = {"Latest messages": "latest", "From beginning": "beginning", "From date/time": "from_date", "Date/time range": "range"}
    mode_label = st.radio("Loading mode", list(modes), index=0, horizontal=True)
    mode = modes[mode_label]
    start = end = None
    if mode == "from_date":
        start_date_col, start_time_col = st.columns(2)
        selected_date = start_date_col.date_input("Start Date", key="start-date")
        selected_time = start_time_col.time_input("Start Date Time", value=time(0, 0), key="start-time")
        start = datetime.combine(selected_date, selected_time)
    elif mode == "range":
        start_date_col, start_time_col, end_date_col, end_time_col = st.columns(4)
        start_date = start_date_col.date_input("Start Date", key="range-start-date")
        start_time = start_time_col.time_input("Start Date Time", value=time(0, 0), key="range-start-time")
        end_date = end_date_col.date_input("End Date", key="range-end-date")
        end_time = end_time_col.time_input("End Date Time", value=time(0, 0), key="range-end-time")
        start = datetime.combine(start_date, start_time)
        end = datetime.combine(end_date, end_time)

    count_col, filter_col = st.columns(2)
    count_label = "Message count" if mode == "latest" else "Maximum message count"
    count = count_col.number_input(count_label, min_value=1, value=100, step=1)
    filter_text = filter_col.text_input(
        "Message Filter (optional)",
        value=st.session_state.get("message_filter", ""),
        placeholder="e.g. error, status, \"ready\"",
    )
    st.session_state.message_filter = filter_text

    if filter_text.strip():
        st.caption("Filter-aware scans stop after 5,000 inspected records to keep loading responsive.")

    load_col, clear_col, refresh_statistics_col = st.columns(3)
    if load_col.button("Load Messages", type="primary"):
        try:
            validate_count(count)
            st.session_state.messages = client.load_messages(topic, group_id, mode, int(count), start, end, filter_text)
        except Exception as exc:
            st.error(f"Unable to load messages: {exc}")
    if clear_col.button("Clear Loaded Messages"):
        st.session_state.messages = []

    if st.session_state.get("topic_statistics_topic") != topic:
        st.session_state.pop("topic_statistics", None)
        st.session_state.pop("topic_statistics_refreshed_at", None)
        st.session_state.topic_statistics_topic = topic

    st.subheader("Topic Statistics")
    refresh_statistics = refresh_statistics_col.button("Refresh Statistics")
    if refresh_statistics or "topic_statistics" not in st.session_state:
        try:
            st.session_state.topic_statistics = client.get_topic_statistics(topic)
            st.session_state.topic_statistics_refreshed_at = datetime.now().astimezone()
        except Exception:
            st.session_state.pop("topic_statistics", None)
            st.session_state.pop("topic_statistics_refreshed_at", None)
            st.error("Unable to retrieve topic statistics. Check the Kafka connection and try again.")

    statistics = st.session_state.get("topic_statistics")
    if statistics is None:
        st.info("Select a connected Kafka topic to view statistics.")
    else:
        metric_columns = st.columns(5)
        metric_columns[0].metric("Total Records", statistics.total_records)
        metric_columns[1].metric("Published Today", statistics.published_today if statistics.published_today is not None else "Unavailable")
        metric_columns[2].metric("Last 1 Hour", statistics.published_last_hour if statistics.published_last_hour is not None else "Unavailable")
        metric_columns[3].metric("Partitions", statistics.partitions)
        latest_value = "No records" if statistics.latest_timestamp is None and statistics.total_records == 0 else (
            "Unavailable" if statistics.latest_timestamp is None else datetime.fromtimestamp(statistics.latest_timestamp / 1000, timezone.utc).astimezone().strftime("%d-%b-%Y %H:%M:%S")
        )
        metric_columns[4].metric("Latest Record Timestamp", latest_value)
        if statistics.timestamp_error:
            st.warning(statistics.timestamp_error)
        refreshed_at = st.session_state.get("topic_statistics_refreshed_at")
        if refreshed_at:
            st.caption(f"Last statistics refresh: {refreshed_at.strftime('%d-%b-%Y %H:%M:%S')}")

    messages = st.session_state.get("messages", [])
    if not messages:
        st.info("No messages loaded.")
        return
    if filter_text.strip():
        metadata = getattr(client, "last_scan_metadata", {})
        if metadata.get("cap_reached"):
            st.warning("The filter reached the 5,000-record scan cap. Results may be partial.")
        else:
            st.caption(f"Scanned {metadata.get('scanned', 0)} record(s) matching the filter.")
    json_payload = json.dumps(export_messages(messages), ensure_ascii=False, indent=2)
    st.download_button(
        "Download Messages as JSON",
        data=json_payload,
        file_name=f"{topic}-messages.json",
        mime="application/json",
        use_container_width=True,
    )
    st.subheader("Message Details")
    for index, message in enumerate(messages, 1):
        with st.expander(f"#{index} partition={message.partition} offset={message.offset}"):
            st.write({"Partition": message.partition, "Offset": message.offset, "Timestamp": message.timestamp, "Key": format_key(message.key)})
            formatted_value = format_message_value(message)
            st.code(formatted_value, language="json" if formatted_value.lstrip().startswith(("{", "[")) else None)


if __name__ == "__main__":
    main()
