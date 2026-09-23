# kafka-viewer

`kafka-viewer` is a lightweight local Streamlit UI for reading Kafka messages for inspection. Kafka connection details come from a local properties file. Consumer offsets are not committed or modified.

## Installation

```bash
pip install kafka-viewer
```

## Configuration

Create a local Java-style `.properties` file named `kafka-viewer.properties`. The `--config` option is mandatory:

```bash
kafka-viewer --config /path/to/kafka-viewer.properties
```

PowerShell:

```powershell
kafka-viewer --config C:\path\to\kafka-viewer.properties
```

Kafka connection properties use the `kafka.` prefix. Schema Registry properties use the `schema.registry.` prefix. The topic and consumer group are selected in the UI; they are not required in the properties file. The application determines unsecured or secured Kafka from the configured Kafka properties.

## Kafka security

Java-style Kafka property names are mapped to the corresponding `kafka-python` consumer configuration names. Supported security protocols are `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL`. The implemented SASL mechanisms are `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`, and `OAUTHBEARER`; username/password mapping is provided for PLAIN and SCRAM, while GSSAPI uses the configured username as the Kerberos name.

Unsecured Kafka:

```properties
kafka.bootstrap.servers=localhost:9092
```

SASL/SSL with placeholder credentials:

```properties
kafka.bootstrap.servers=localhost:9093
kafka.security.protocol=SASL_SSL
kafka.sasl.mechanism=PLAIN
kafka.sasl.username=YOUR_USERNAME
kafka.sasl.password=YOUR_PASSWORD
```

Only supported `kafka.*` properties are passed to `kafka-python`. Unsupported Kafka properties are reported rather than silently passed through. `kafka.bootstrap.servers` is required.

## Schema Registry / Confluent Avro

Schema Registry support is optional. Without `schema.registry.url`, the existing Kafka value handling is used. When it is configured, kafka-viewer attempts Confluent Avro deserialization. The deserializer reads the Schema ID from the Confluent Avro payload and retrieves the corresponding schema; users do not configure a schema ID, version, or subject. The topic continues to be selected through the existing UI.

Unauthenticated Schema Registry:

```properties
schema.registry.url=https://schema-registry.example.com
```

Optional Schema Registry Basic Authentication:

```properties
schema.registry.url=https://schema-registry.example.com
schema.registry.basic.auth.user.info=YOUR_USERNAME:YOUR_PASSWORD
```

Schema Registry authentication is used only when the authentication property is provided. Schema Registry properties are handled separately and are never passed to `KafkaConsumer`.

If Avro deserialization fails, the Kafka message is not discarded. The UI continues to show its metadata, a failure indication, a safe bounded raw-payload representation, and a safe error message. This distinguishes messages that did not arrive from messages that arrived but could not be decoded.

The UI lets you check Kafka connection status, discover and select topics, enter or generate a temporary consumer group ID, and load:

- The latest N messages.
- Messages from the beginning.
- Messages from a date/time.
- Messages within a date/time range.

Multiple Kafka partitions are supported. All reads have a bounded message count. Loaded messages show the partition, offset, timestamp, key, and message value. Messages can be expanded to view the complete message value, including formatted JSON when applicable.

## Offset behavior

kafka-viewer is intended for inspection. It does not commit consumer offsets or modify existing consumer-group progress.

## Safe configuration

Keep real `.properties` files containing credentials out of source control. Use `kafka-viewer.properties.example` as a template only, and never paste credentials into documentation.

## Limitations

The implementation supports the Kafka security protocols and SASL mechanisms listed above, plus optional Confluent Avro deserialization. It does not support:

- Protobuf.
- Publishing messages.
- Topic or consumer-group administration.
- Multiple Kafka clusters.

## Development

```bash
python -m pytest
python -m build
```
