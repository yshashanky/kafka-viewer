# kafka-viewer

`kafka-viewer` is a lightweight local Streamlit UI for inspecting messages in an unsecured Kafka cluster. Kafka connection details come from a local properties file, and the viewer does not commit consumer offsets.

## Installation

```bash
pip install kafka-viewer
```

## Configuration

Create a local properties file named `kafka-viewer.properties`:

```properties
kafka.bootstrap.servers=localhost:9092
```

The configuration remains local to your machine.

## Usage

The `--config` option is required:

```bash
kafka-viewer-unsecured --config /path/to/kafka-viewer.properties
```

Windows:

```text
kafka-viewer-unsecured --config C:\path\to\kafka-viewer.properties
```

The UI lets you check Kafka connection status, discover and select topics, enter or generate a temporary consumer group ID, and load:

- The latest N messages.
- Messages from the beginning.
- Messages from a date/time.
- Messages within a date/time range.

Loaded messages show the partition, offset, timestamp, key, and message value. Messages can be expanded to view the complete message value, including formatted JSON when applicable.

## V1 limitations

V1 supports unsecured Kafka only. It does not support:

- Kafka authentication or security configuration.
- Avro, Schema Registry, or Protobuf.
- Publishing messages.
- Topic or consumer-group administration.
- Multiple Kafka clusters.

## Development

```bash
python -m pytest
python -m build
```
