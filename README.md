# kafka-viewer

`kafka-viewer` is a lightweight local Streamlit UI for reading Kafka messages for inspection. Kafka connection details come from a local properties file. Consumer offsets are not committed or modified.

## Installation

```bash
pip install kafka-viewer
```

## Run

`--config` is mandatory.

```bash
kafka-viewer --config /path/to/kafka-viewer.properties
```

PowerShell:

```powershell
kafka-viewer --config C:\path\to\kafka-viewer.properties
```

## Configuration model

kafka-viewer uses one canonical naming convention:

- Kafka connection and consumer properties: `kafka.*`
- Schema Registry properties: `schema.registry.*`

The parser also accepts selected Java/Spring-style aliases for compatibility (for example `bootstrap.servers`, `ssl.truststore.location`, `spring.kafka.properties.*`), then normalizes them into the canonical `kafka.*` format internally.

Configuration flow:

1. Parse `.properties`
2. Normalize aliases to `kafka.*`
3. Validate security/SSL/SASL requirements
4. Translate only supported settings into `kafka-python` consumer config
5. Report unsupported Kafka-related properties as warnings

Unknown properties are never blindly passed through to `KafkaConsumer`.

## Supported Kafka security and connection capabilities

Supported protocols:

- `PLAINTEXT`
- `SSL`
- `SASL_PLAINTEXT`
- `SASL_SSL`

Supported SASL mechanisms:

- `PLAIN`
- `SCRAM-SHA-256`
- `SCRAM-SHA-512`
- `GSSAPI`
- `OAUTHBEARER`

SASL credentials can be provided with either:

- `kafka.sasl.username` + `kafka.sasl.password`
- `kafka.sasl.jaas.config` (username/password are extracted for PLAIN/SCRAM)

## SSL, truststore, and keystore support

kafka-viewer supports both direct PEM paths and Java enterprise store formats.

Truststore properties:

- `kafka.ssl.truststore.location`
- `kafka.ssl.truststore.type` (`PEM`, `JKS`, `PKCS12`/`PFX`; optional, inferred from extension if omitted)
- `kafka.ssl.truststore.password` (used for JKS/PKCS12)
- `kafka.ssl.truststore.cert.alias` (optional JKS alias)

Keystore properties:

- `kafka.ssl.keystore.location`
- `kafka.ssl.keystore.type` (`PEM`, `JKS`, `PKCS12`/`PFX`; optional, inferred from extension if omitted)
- `kafka.ssl.keystore.password` (used for JKS/PKCS12)
- `kafka.ssl.key.password` (private key password or fallback decryption password)
- `kafka.ssl.keystore.key.alias` (optional JKS key alias)
- `kafka.ssl.keystore.key.location` (optional separate PEM key path)

Additional SSL properties:

- `kafka.ssl.endpoint.identification.algorithm` (`https` or `none`/empty)
- `kafka.ssl.check.hostname`
- `kafka.ssl.protocol`
- `kafka.ssl.cipher.suites`
- `kafka.ssl.cafile`
- `kafka.ssl.certfile`
- `kafka.ssl.keyfile`
- `kafka.ssl.password`
- `kafka.ssl.crlfile`

For JKS/PKCS12 stores, kafka-viewer securely converts certificate and key material into short-lived local files for `kafka-python`. JKS conversion uses the Java `keytool` executable (must be available on `PATH`). Passwords and private key contents are never printed.

## Configuration examples

Unsecured Kafka:

```properties
kafka.bootstrap.servers=localhost:9092
kafka.security.protocol=PLAINTEXT
```

SSL with truststore and PKCS12 keystore:

```properties
kafka.bootstrap.servers=localhost:9093
kafka.security.protocol=SSL
kafka.ssl.truststore.location=/path/to/truststore.jks
kafka.ssl.truststore.type=JKS
kafka.ssl.truststore.password=YOUR_TRUSTSTORE_PASSWORD
kafka.ssl.keystore.location=/path/to/client.p12
kafka.ssl.keystore.type=PKCS12
kafka.ssl.keystore.password=YOUR_KEYSTORE_PASSWORD
kafka.ssl.key.password=YOUR_KEY_PASSWORD
kafka.ssl.endpoint.identification.algorithm=https
```

SASL_SSL with PLAIN:

```properties
kafka.bootstrap.servers=localhost:9093
kafka.security.protocol=SASL_SSL
kafka.sasl.mechanism=PLAIN
kafka.sasl.username=YOUR_USERNAME
kafka.sasl.password=YOUR_PASSWORD
kafka.ssl.truststore.location=/path/to/ca.pem
kafka.ssl.truststore.type=PEM
```

SASL_SSL with JAAS-style credentials:

```properties
kafka.bootstrap.servers=localhost:9093
kafka.security.protocol=SASL_SSL
kafka.sasl.mechanism=SCRAM-SHA-512
kafka.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="YOUR_USERNAME" password="YOUR_PASSWORD";
```

## Schema Registry / Confluent Avro

Schema Registry support is optional.

- If `schema.registry.url` is missing, kafka-viewer uses raw/string/JSON value display behavior.
- If configured, kafka-viewer attempts Confluent Avro deserialization.
- Schema Registry config stays separate from Kafka consumer config and is never passed to `KafkaConsumer`.

Schema Registry example:

```properties
schema.registry.url=https://schema-registry.example.com
schema.registry.basic.auth.user.info=YOUR_USERNAME:YOUR_PASSWORD
```

Avro failures are non-fatal: kafka-viewer continues processing messages and shows bounded safe raw payload + safe error text for failed records.

## UI consumer group generation

The UI supports:

- Manual consumer group entry
- Optional prefix input (`Group ID Prefix`)
- `Generate Temporary Group ID`

If the prefix is empty, existing generation behavior is unchanged. If a prefix is set, the generated value is `prefix + generated-id`.

## Offset behavior

kafka-viewer is intended for inspection. It does not commit consumer offsets or modify existing consumer-group progress.

## Safety guidance

Keep real `.properties` files containing credentials out of source control. Use `kafka-viewer.properties.example` as a template only.

## Limitations

This release provides broad practical connection/security compatibility for Kafka/Java/Spring-style deployment concepts, but not full parity with every Java client property.

Not supported:

- Protobuf deserialization
- Publishing messages
- Topic or consumer-group administration
- Arbitrary non-Kafka Java library properties

## Development

```bash
python -m pytest
python -m build
```