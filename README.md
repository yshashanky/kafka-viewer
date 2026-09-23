# kafka-viewer

`kafka-viewer` is a small local Streamlit UI for inspecting messages in an unsecured Kafka cluster. It reads connection details from a local properties file and never commits consumer offsets.

## Installation

```text
python -m pip install -e .
```

Create `kafka-viewer.properties`:

```properties
kafka.bootstrap.servers=localhost:9092
```

Start the viewer with `kafka-viewer-unsecured --config /path/to/file.properties`. The `--config` option is required. Use the UI to test the connection, choose a topic, enter or generate a temporary consumer group ID, select a loading mode, and load messages. Expand a row to inspect the complete value.

V1 supports unsecured Kafka only. It does not support publishing, administration, authentication, schemas, or multiple clusters.