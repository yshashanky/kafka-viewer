# kafka-viewer

`kafka-viewer` is a lightweight local Streamlit UI for reading Kafka messages for inspection. Kafka connection details come from a local properties file. Consumer offsets are not committed or modified.

Snappy-compressed Kafka records are supported through the normal `kafka-viewer` installation; no separate compression package installation is required.

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
- Authentication is optional. HTTPS does not require Basic Authentication unless it is explicitly configured.
- Certificate verification defaults to enabled for Schema Registry.

Supported Schema Registry properties:

- `schema.registry.url`
- `schema.registry.basic.auth.user.info` (optional)
- `schema.registry.ssl.ca.location` (optional custom CA file)
- `schema.registry.ssl.verify=true|false` (default: true)
- `schema.registry.ssl.no.revoke=true|false` (accepted for compatibility; current Python/Confluent stack does not expose equivalent revocation control, so `true` is rejected explicitly)
- `schema.registry.ssl.certificate.location` (optional client certificate)
- `schema.registry.ssl.key.location` (optional client private key)
- `schema.registry.ssl.key.password` (optional client key password)

Schema Registry TLS examples:

```properties
schema.registry.url=https://schema-registry.company.com:8082

# Optional - trust an internal/private CA
schema.registry.ssl.ca.location=/path/to/company-ca.pem

# Optional - defaults to true
schema.registry.ssl.verify=true

# Optional - compatibility only; current Python/Confluent stack does not support equivalent revocation control
schema.registry.ssl.no.revoke=false

# Optional - Basic Authentication
schema.registry.basic.auth.user.info=username:password
```

For HTTP Schema Registry endpoints:

```properties
schema.registry.url=http://schema-registry.company.com:8081
```

When `schema.registry.ssl.verify` is set to `false`, kafka-viewer explicitly disables TLS certificate verification for the Schema Registry client only. This is equivalent to an intentional `curl -k` scenario and does not change Kafka broker TLS behavior.

When using a custom CA, the file path is validated before the Schema Registry client is created and the file contents are never logged.

Confluent Avro deserialization uses the Schema ID embedded in the Confluent wire format and then looks that schema up via the configured Schema Registry. If deserialization fails, kafka-viewer retains the raw message safely, continues processing, and displays a sanitized error instead of crashing the app.

## UI consumer group generation

The UI supports:

- Manual consumer group entry
- Optional prefix input (`Group ID Prefix`)
- `Generate Temporary Group ID`

If the prefix is empty, existing generation behavior is unchanged. If a prefix is set, the generated value is `prefix + generated-id`.

## Topic Statistics

The dashboard includes a read-only Topic Statistics section for the selected topic. It reports:

- **Total Records**: records currently retained, calculated as the sum of each partition's end offset minus beginning offset. This is not the lifetime number of records ever published.
- **Published Today**: records whose Kafka timestamps fall from local midnight through the start of the next local day.
- **Last 1 Hour**: records whose Kafka timestamps fall within the previous hour, including the boundary.
- **Latest Record Timestamp**: the newest Kafka record timestamp currently retained.
- **Partitions**: the selected topic's partition count.

Use **Refresh Statistics** to obtain fresh values without clearing loaded messages, filters, or loading controls. The refresh time is shown separately from the latest record timestamp. Offset-based totals do not consume the topic; timestamp metrics use Kafka timestamp-to-offset lookup and inspect the final retained record in each non-empty partition using bounded batched reads. kafka-python 3.0.11 does not expose a MAX_TIMESTAMP/ListOffsets helper, so the viewer does not claim a broker-side maximum-timestamp query. The bounded fallback assumes the latest retained record in each partition represents that partition's latest timestamp; deployments with non-monotonic record timestamps should treat this metric as a best-effort limitation. If Kafka cannot provide a usable timestamp, timestamp metrics are shown as unavailable while retained totals remain available. Displayed times use the machine's local timezone.

When a message filter is used with Latest mode, kafka-viewer scans partition-local latest regions and returns the newest matching records across the topic. Kafka record timestamps determine cross-partition recency; partition and offset provide deterministic tie-breaking. The global filter scan cap remains 5,000 records.

### Message Filter Syntax

The Message Filter is a case-insensitive literal substring filter applied to the searchable message value, not Kafka metadata such as keys, timestamps, partitions, offsets, or headers.

- `payment` matches messages containing `payment`.
- `payment?failed?timeout` uses `?` as OR and matches a message containing any term.
- `payment&failed&timeout` uses `&` as AND and requires all terms in the same message.

Whitespace around terms is ignored and empty terms are discarded, so `payment??failed` and ` payment ? failed ` are valid. Terms remain literal text; regular-expression syntax is not interpreted. Mixing `?` and `&` is not supported and is rejected with a validation message. Blank or operator-only filters use the existing unfiltered behavior. Filtered Latest retrieval continues to use the adaptive scan strategy and its global 5,000-record safety cap rather than scanning the cap unnecessarily.

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