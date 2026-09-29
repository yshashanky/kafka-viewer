from __future__ import annotations

import json
import tempfile
from contextlib import ExitStack
from datetime import datetime, time

import streamlit as st

from kafka_viewer.config import build_schema_registry_config
from kafka_viewer.certification.adapters.kafka import KafkaAdapter, load_connections
from kafka_viewer.certification.cli import parser
from kafka_viewer.certification.deserializers import AvroValue, JsonValue, ProtobufValue, RawString, XmlValue
from kafka_viewer.certification.engine import certify
from kafka_viewer.certification.filtering import Filter, Rule, OPERATORS
from kafka_viewer.certification.normalization import Mapping, Normalizer, validate_pair
from kafka_viewer.certification.pipeline import normalized_stream
from kafka_viewer.certification.report import QUALITY_FORMULA, write_html
from kafka_viewer.certification.scope import Scope, to_millis

FORMATS = ("STRING", "JSON", "XML", "CONFLUENT_AVRO", "PROTOBUF")


def mapping_editor(side):
    st.caption("Add explicit input path / normalized field rows. Exactly one must map to id.")
    rows = st.data_editor(
        [{"path": "", "field": ""}], num_rows="dynamic", key=side + "-mapping",
        column_config={"path": "Input path", "field": "Normalized field"},
        use_container_width=True,
    )
    return tuple(Mapping(str(r["path"] or "").strip(), str(r["field"] or "").strip())
                 for r in rows if r.get("path") or r.get("field"))


def scope_editor(side, mode):
    if mode == "Entire Retained Data":
        return ("ENTIRE_RETAINED_DATA", None, None, "local")
    zone = st.text_input("IANA timezone (blank = machine local)", key=side + "-zone")
    start_date = st.date_input("Start date", key=side + "-start-date")
    start_time = st.time_input("Start time", value=time(0), key=side + "-start-time")
    end_date = st.date_input("End date", key=side + "-end-date")
    end_time = st.time_input("End time", value=time(1), key=side + "-end-time")
    return ("TIME_RANGE", datetime.combine(start_date, start_time), datetime.combine(end_date, end_time), zone)


def make_scope(settings):
    mode, start, end, zone = settings
    return Scope(mode, to_millis(start, zone) if start else None,
                 to_millis(end, zone) if end else None, zone or "local")


def format_editor(side, properties):
    format_name = st.selectbox("Format", FORMATS, key=side + "-format")
    descriptor, message_type = None, ""
    if format_name == "PROTOBUF":
        descriptor = st.file_uploader("FileDescriptorSet (include imports)", type=["desc", "pb", "bin"], key=side + "-descriptor")
        message_type = st.text_input("Fully qualified message type", key=side + "-message-type")
    elif format_name == "CONFLUENT_AVRO":
        registry = build_schema_registry_config(dict(properties))
        st.caption("Schema Registry: " + ("configured" if registry else "not configured"))
        if registry:
            st.caption("Registry TLS verification: " + ("disabled" if registry.get("ssl.ca.location") is False else "enabled"))
        if st.button("Test Schema Registry", key=side + "-registry-test"):
            try:
                with ExitStack() as stack:
                    decoder = AvroValue(registry)
                    stack.callback(decoder.close)
                    decoder.test_connection()
                st.success("Schema Registry connected")
            except Exception:
                st.error("Schema Registry connection failed; check properties and authorization")
    elif format_name == "XML":
        st.caption("Safe literal paths, e.g. transaction/id or transaction/@id. DTDs/entities are forbidden.")
    elif format_name == "STRING":
        st.caption("Literal UTF-8 only. Use $value for the whole string; no JSON auto-detection.")
    else:
        st.caption("Structured JSON with explicit paths. JSON Schema validation is not part of V1.")
    return format_name, descriptor.getvalue() if descriptor else None, message_type


def make_decoder(settings, properties):
    name, descriptor, message_type = settings
    if name == "CONFLUENT_AVRO":
        return AvroValue(build_schema_registry_config(dict(properties)))
    if name == "PROTOBUF":
        if not descriptor or not message_type:
            raise ValueError("Protobuf requires a descriptor and fully qualified message type")
        return ProtobufValue(descriptor, message_type)
    return {"STRING": RawString, "JSON": JsonValue, "XML": XmlValue}[name]()


def filter_editor(fields):
    if not st.checkbox("Filter source eligibility"):
        return Filter()
    if not fields:
        st.info("Map comparison fields to configure a filter")
        return Filter()
    connector = st.selectbox("Combine rules", ("AND", "OR"))
    rows = st.data_editor(
        [{"field": fields[0], "operator": "equals", "value": ""}], num_rows="dynamic",
        key="source-filter", column_config={
            "field": st.column_config.SelectboxColumn("Normalized field", options=fields, required=True),
            "operator": st.column_config.SelectboxColumn("Operator", options=OPERATORS, required=True),
            "value": "Value (JSON scalar, or literal text)",
        },
    )
    rules = []
    for row in rows:
        value = row.get("value", "")
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            pass
        rules.append(Rule(row.get("field", ""), row.get("operator", ""), value))
    return Filter(tuple(rules), connector)


def main():
    st.set_page_config(page_title="Data Certification", layout="wide")
    st.title("Data Certification")
    st.caption("Read-only reconciliation with explicit mappings and complete HTML evidence")
    args = parser().parse_args()
    try:
        properties = load_connections(args.source_config, args.destination_config)
    except Exception:
        st.error("Unable to load connection configuration; verify properties and security settings")
        return
    st.caption("Separate source/destination connections" if args.destination_config else "Same connection for source and destination")
    mode = st.radio("Validation scope", ("Time Range", "Entire Retained Data"), horizontal=True)
    if mode == "Entire Retained Data":
        st.warning("Entire retained data may process a large number of records.")
    sides = []
    for column, side, props in zip(st.columns(2), ("Source", "Destination"), properties):
        with column:
            st.subheader(side + " connection")
            st.caption("Kafka security: " + props.get("kafka.security.protocol", "PLAINTEXT"))
            group = st.text_input("Consumer group ID (optional)", value=props.get("kafka.group.id", ""), key=side + "-group")
            adapter = KafkaAdapter(props, group)
            if st.button("Test connection / Refresh topics", key=side + "-test"):
                try:
                    st.session_state[side + "-topics"] = sorted(adapter.topics())
                    st.success("Connected")
                except Exception:
                    st.session_state[side + "-topics"] = []
                    st.error("Kafka connection failed; check properties and authorization")
            topic = st.selectbox(side + " Topic", st.session_state.get(side + "-topics", []), key=side + "-topic")
            settings = format_editor(side, props)
            mappings = mapping_editor(side)
            scope_settings = scope_editor(side, mode)
            sides.append((adapter, topic, settings, mappings, scope_settings))
    if not args.destination_config and sides[0][1] and sides[0][1] == sides[1][1]:
        st.warning("Source and destination are the same topic. This is supported; verify that it is intentional.")
    source_filter = filter_editor(sorted({m.field for m in sides[0][3] if m.field and m.field != "id"}))
    if st.button("Run Certification", type="primary"):
        st.session_state.pop("certification-report", None)
        st.session_state.pop("certification-summary", None)
        try:
            mappings = validate_pair(sides[0][3], sides[1][3])
            fields = {m.field for m in mappings[0]} - {"id"}
            source_filter.validate(fields)
            scopes = [make_scope(side[4]) for side in sides]
            if any(not side[1] for side in sides):
                raise ValueError("Test both connections and select available topics first")
        except ValueError as exc:
            st.error(str(exc))  # Only locally generated validation messages.
            return
        try:
            with ExitStack() as stack:
                decoders = []
                for side, props in zip(sides, properties):
                    decoder = make_decoder(side[2], props)
                    decoders.append(decoder)
                    stack.callback(decoder.close)
                # Recheck selections before consuming a potentially stale topic list.
                if any(side[1] not in side[0].topics() for side in sides):
                    st.error("A selected topic is no longer available. Refresh topics.")
                    return
                streams = [normalized_stream(side[0].read(side[1], scope), decoder, Normalizer(mapping, i == 1))
                           for i, (side, scope, decoder, mapping) in enumerate(zip(sides, scopes, decoders, mappings))]
                summary_scope = {name: {"topic": side[1], **scope.summary()} for name, side, scope in
                                 zip(("source", "destination"), sides, scopes)}
                with st.spinner("Reading and normalizing source/destination; comparing indexed records..."):
                    result = stack.enter_context(certify(*streams, fields=fields, source_filter=source_filter, scope=summary_scope))
                with st.spinner("Generating complete HTML evidence..."):
                    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as report:
                        write_html(result, report)
                        report.seek(0)
                        # Streamlit's download API holds bytes; engine/report writing remain streamed.
                        st.session_state["certification-report"] = report.read().encode("utf-8")
                    st.session_state["certification-summary"] = result.summary()
        except Exception:
            st.error("Certification could not run. Check connection, descriptor, mapping and local disk configuration.")
            return
    if "certification-summary" in st.session_state:
        summary = st.session_state["certification-summary"]
        st.subheader("Status: " + summary["status"])
        metrics = {
            "Source Records": summary["source_statistics"]["records"],
            "Destination Records": summary["destination_statistics"]["records"],
            "Eligible Source Records": summary["source_statistics"]["eligible"],
            "Matched": summary["matched"], "Mismatched": summary["mismatched"],
            "Missing": summary["missing_in_destination"], "Extra": summary["extra_in_destination"],
            "Duplicates": summary["duplicate_ids"], "Filter Violations": summary["filter_violations"],
            "Quality %": round(summary["quality_percentage"], 2),
        }
        columns = st.columns(5)
        for i, (label, value) in enumerate(metrics.items()):
            columns[i % 5].metric(label, value)
        st.caption(QUALITY_FORMULA)
        with st.expander("Detailed counters and scope"):
            st.json(summary)
        report = st.session_state["certification-report"]
        st.caption(f"Complete report: {len(report):,} bytes. Expand records in the downloaded HTML for field evidence.")
        if len(report) > 50 * 1024 * 1024:
            st.warning("Large HTML report: downloading/opening it may require substantial memory.")
        st.download_button("Download Certification Report", report, "certification.html", "text/html")


if __name__ == "__main__":
    main()
