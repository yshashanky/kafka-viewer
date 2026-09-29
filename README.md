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

For JKS/PKCS12 stores, kafka-viewer extracts the required certificate and key material, converts it to short-lived PEM files, and passes those PEM files to `kafka-python`. JKS conversion uses the Java `keytool` executable (must be available on `PATH`). Passwords and private key contents are never printed.

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

The dashboard reports these read-only statistics for the selected topic:

- **Total Records**: records currently retained.
- **Published Today**: records with Kafka timestamps from local midnight to the next local midnight.
- **Last 1 Hour**: records with Kafka timestamps from the previous hour.
- **Latest Record Timestamp**: the newest retained Kafka record timestamp.
- **Partitions**: the selected topic's partition count.

Use **Refresh Statistics** to update the values without clearing loaded messages, filters, or loading controls. Timestamp-based values use Kafka record timestamps and are displayed in the machine's local timezone; they may be unavailable when the required timestamp information cannot be retrieved.

In Latest mode, filtered results return the newest matching records across the topic using Kafka record timestamps for cross-partition recency.

### Message Filter Syntax

Message Filter matches the searchable message value using case-insensitive literal substring matching. It does not search Kafka metadata.

- `payment` matches messages containing `payment`.
- `payment?failed?timeout` uses `?` for OR.
- `payment&failed&timeout` uses `&` for AND in the same message.

Whitespace around terms is ignored, empty terms are discarded, and regular expressions are not interpreted. Mixing `?` and `&` is rejected. Blank or operator-only filters behave as an unfiltered load.

Filtered Latest scans have an optional global inspection budget:

```properties
kafka.viewer.filter.scan.max.records=20000
```

The default is **5000** when omitted. The value must be a positive integer;
zero, negative, fractional, nonnumeric and empty values fail viewer configuration
validation. Smaller values are respected, including values below the initial
adaptive target of 500. No application-specific upper limit is imposed.

This setting limits the **total records inspected across all partitions** during
a filtered Latest request. Message Count still controls how many matching records
are returned; this is not a per-partition cap, fetch size, or `max.poll.records`.
The scanner retains its adaptive expansion and timestamp ordering. Higher caps
may find matches farther back but increase read time and resource use; they do not
guarantee unbounded global recency. Unfiltered Latest, other loading modes, Topic
Statistics and certification scans are unaffected. Other filtered loading modes
retain their existing 5000-record budget.

## Offset behavior

kafka-viewer is intended for inspection. It does not commit consumer offsets or modify existing consumer-group progress.

## Safety guidance

Keep real `.properties` files containing credentials out of source control. Use `kafka-viewer.properties.example` as a template only.

## Limitations

This release provides broad practical connection/security compatibility for Kafka/Java/Spring-style deployment concepts, but not full parity with every Java client property.

Not supported in the original Kafka Viewer UI (the separate certification utility below supports Protobuf):

- Protobuf deserialization
- Publishing messages
- Topic or consumer-group administration
- Arbitrary non-Kafka Java library properties

## Development

```bash
python -m pytest
python -m build
```

## Data Certification Engine

`kafka-viewer-certify` launches a separate read-only Streamlit application for
reconciling two Kafka datasets by explicitly mapped business IDs. The original
`kafka-viewer --config ...` command, UI, filters, security and loading modes are
unchanged. No certification settings are required by the original viewer.

Same cluster (destination uses a copy of the source connection configuration):

```bash
kafka-viewer-certify --source-config /path/to/kafka.properties
```

Different clusters, each with its own Kafka and Schema Registry configuration:

```bash
kafka-viewer-certify \
  --source-config /path/to/source.properties \
  --destination-config /path/to/destination.properties
```

PowerShell:

```powershell
kafka-viewer-certify --source-config C:\configs\source.properties `
  --destination-config C:\configs\destination.properties
```

Use the same existing `.properties` model and aliases documented above. Kafka
SSL/SASL, PEM/JKS/PKCS12 and Schema Registry settings use the existing configuration
builders. Registry credentials and TLS settings remain independent of Kafka.
There are no new certification properties in these files. The UI displays a
connection mode and security summary without displaying credentials or URLs.

### Workflow and formats

1. Test each connection / refresh its topic list, then select Source Topic and
   Destination Topic. Same-topic comparison is supported with a warning.
2. Select each side's format independently and supply any format-specific inputs.
3. Add explicit mapping rows on each side, including exactly one `id` mapping.
4. Optionally configure source eligibility filters using normalized data fields.
5. Choose independent time windows (the default) or Entire Retained Data.
6. Run Certification and download the complete HTML evidence report.

| Format | Behavior / required inputs |
| --- | --- |
| STRING / RAW | Strict UTF-8 text; `$value` means the entire string. JSON-looking strings stay strings. The UI calls this STRING. |
| JSON | Structured object/array root, nested literal paths; duplicate keys, non-finite numbers and malformed payloads are record errors. JSON Schema validation is not implemented. |
| XML | `defusedxml` parser with DTDs, entities and external access forbidden. Literal root/child paths and attributes, e.g. `transaction/id`, `transaction/@id`. |
| CONFLUENT_AVRO | Confluent wire-format schema ID and the configured Schema Registry, using the same libraries/security helpers as the viewer. No pasted schema required. |
| PROTOBUF | Upload a binary `FileDescriptorSet` containing all imports and enter a fully qualified message type. Raw Protobuf wire payloads only; no runtime `protoc` or generated Python files. |

XML repeated children become indexed sequences, e.g. `transaction/line/0/amount`;
text on elements with attributes uses `#text`. XML namespaces and mixed content
are rejected in V1. JSON/Avro/Protobuf paths use dots and numeric array indices,
e.g. `payments.0.amount`. Paths are literal identifiers, never expressions or
XPath. Keys containing path separators are not addressable in V1. Protobuf uses
its standard JSON representation with original field names (including strings
for 64-bit integers and enum names); unset fields and unknown binary fields are
not materialized. Avro logical values must resolve to supported scalar types.

### Explicit normalization

The Kafka-independent engine accepts `NormalizedRecord(id, data, metadata)`:

- `id`: one required, explicitly mapped nonempty scalar; no fallback or inference.
- `data`: only mapped flat scalar fields (strings, integers, finite floats/decimals,
  booleans, null, or a distinct missing marker).
- `metadata`: allowlisted topic, partition, offset and timestamp evidence, excluded
  from comparisons and filters.

Example rows (enter these yourself; the UI never auto-maps):

| Source path | Destination path | Normalized field |
| --- | --- | --- |
| transactionId | transaction_id | id |
| payment.amount | txn_amount | amount |
| payment.currency | currency | currency |
| status | state | status |

Both mapping sets must produce the same comparison field names. Duplicate names,
duplicate input paths, missing ID mappings and nested normalized field names are
rejected before consumption. Mapped objects/arrays are record-level
`NORMALIZATION_ERROR / NON_SCALAR`; missing/null/empty IDs produce
`NORMALIZATION_ERROR / MISSING_ID`. Unmapped source fields are ignored. Unmapped
destination leaf paths are retained as `EXTRA_DESTINATION_FIELD` evidence and
fail certification. Extra-field detection does not apply to raw strings.

### Source filters

Rules support `equals`, `not_equals`, `contains`, `starts_with`, `ends_with`,
`exists`, and `not_exists`, combined by a single AND or OR connector. Rules operate
only on explicitly mapped `data`, never on the ID or Kafka metadata. Matching is
case-sensitive and literal; equality is type-strict. `exists` means a field is
present, even when null or empty. Enter filter values as JSON scalars (`100`,
`true`, `"100"`, `null`) or unquoted literal text. Text operators require strings.

Every excluded source ID is retained. Any occurrence of that ID in destination
is a `FILTER_VIOLATION`, including IDs that are also duplicates. No filter means
all successfully normalized source records are eligible.

### Scope and read-only Kafka access

Time windows are independent on each side and use Kafka record timestamps with
`start <= timestamp < end`. Each side accepts an IANA timezone; blank uses the
machine's local timezone. Explicit IANA DST gaps and ambiguous wall times are
rejected; use UTC for an ambiguous instant. Windows timezone data is supplied by
`tzdata`. Empty or reversed windows are invalid.

Entire Retained Data starts at each partition's current beginning offset, which
may be nonzero. Both scopes capture fixed end offsets per side before scanning;
new records beyond those boundaries are excluded. Time-range scans seek to the
start timestamp and filter records individually through the captured end offset,
so an early record beyond the time end does not hide later out-of-order records.
The two sides are sequential snapshots, not a cross-cluster atomic snapshot.

Consumers use explicit assignment/seek, `enable_auto_commit=False`, disabled
automatic topic creation and no offset reset. They never commit offsets, publish,
or administer topics/groups. Enter independent authorized consumer group IDs in
the UI when required; otherwise the configured group ID is retained. No random
group ID is invented. Topic lists are rechecked before consumption.

Reads are batched. Ten consecutive polls without offset progress, unavailable
timestamps encountered in a time scan, changed retention, or changed partitions
produce `INCOMPLETE`. Broker/authentication failures produce `ERROR`. Compacting
topics and timestamp-less historical data cannot provide immutable historical
snapshots; broker timestamp lookup and retained data define the available scope.

### Matching, results and quality

ID equality is type-strict: `100`, `"100"`, and `100.0` are different IDs. If either
side has multiple occurrences of an ID, classify it as `DUPLICATE`, retain all
occurrences/counts, and do not choose an arbitrary record for comparison.

Unique IDs compare mapped fields one-to-one. Missing, null and empty string are
value-equivalent, but their original representations remain visible. Finite
numeric values and numeric strings compare safely using Decimal semantics;
booleans never compare as numbers. For example, integer `100` versus string
`"100"` is value-equal with a type discrepancy. Type-only differences are warnings
and do not fail an otherwise matching run.

Result categories include `MATCHED`, `MISMATCHED`, `MISSING_IN_DESTINATION`,
`EXTRA_IN_DESTINATION`, `DUPLICATE`, `EXCLUDED`, `FILTER_VIOLATION`,
`NORMALIZATION_ERROR` and `DESERIALIZATION_ERROR`.

| Status | Meaning |
| --- | --- |
| PASSED | Scope completed with no value/business discrepancies. |
| FAILED | Completed with mismatches, missing/extra records, duplicates, filter violations or unexpected destination fields. |
| INCOMPLETE | Record decoding/normalization errors or an incomplete snapshot prevent full certification. |
| ERROR | Input/infrastructure processing failed. |

Quality is `value-matched unique eligible source IDs / nonduplicated eligible
source IDs * 100`. Zero denominator is 100% with no unique expected records.
Duplicates, destination extras and filter violations are separately visible
failures; quality alone is not a pass/fail verdict. Counts in partial runs are
provisional. Unrecoverable Schema Registry failures mark the run ERROR; unknown
schema IDs and malformed Avro become per-record errors, without silently falling
back to raw matching.

### Architecture, storage and reports

`Adapter -> Deserializer -> Normalizer -> Source Filter -> Certification Engine
-> CertificationResult -> HTML Report`

The core engine does not import Kafka, Streamlit or format libraries. Its
UI-independent result exposes source/destination statistics, all classification
counts, field/type discrepancy counts, scope, status, quality and lazy evidence.
An internal index protocol is implemented with standard-library SQLite:
type-tagged IDs, indexed lookups, batches of 500 writes, and disk-backed evidence
avoid a source-by-destination comparison or loading all records into memory.
Even duplicate occurrences and report rows are iterated from disk.

SQLite lives in a private temporary directory and is closed/removed when the
result context ends. Use the programmatic result as a context manager:

```python
from kafka_viewer.certification import NormalizedRecord, certify
from kafka_viewer.certification.report import write_html

source = [NormalizedRecord("TX100", {"amount": 100})]
destination = [NormalizedRecord("TX100", {"amount": "100"})]
with certify(source, destination, fields=["amount"]) as result:
    with open("certification.html", "w", encoding="utf-8") as output:
        write_html(result, output)
```

The self-contained HTML includes the summary, scope/ranges/topics, generation
timestamp, every evaluated record, every duplicate occurrence, safe error
coordinates, original field values, value equality and type equality. All
dynamic content is HTML-escaped; no payload is executable markup and no external
CDN is required. Report writing is incremental and evidence is never truncated.
Streamlit's download API does hold the final report in memory; its size is shown
and a warning appears above 50 MiB. Use programmatic file output for very large
reports. The UI retains the downloaded report in its session, not the SQLite file.

Connection configuration, credentials, certificates, raw failure payloads and
library exception strings are never passed to the index/report. Record failures
use fixed safe codes and allowlisted coordinates. Reports intentionally include
mapped business values; treat them as sensitive if those values are sensitive.
Temporary normalized data is not encrypted; use a trusted local machine and disk.

### Dependencies, validation and V1 limits

New direct dependencies are `defusedxml` for secure XML, `protobuf` for descriptor
decoding (already a Streamlit transitive dependency), and `tzdata` for portable
IANA timezone support. Existing dependency ranges are unchanged. SQLite, numeric
comparison and HTML generation use the standard library. No second Avro stack
is introduced.

V1 excludes JSON Schema validation, arbitrary scripts/serializers/transformations,
automatic mapping/schema inference, fuzzy matching, nested normalized fields,
maximum-record/core offset scopes, distributed execution, PDF reports and Kafka
mutations. Protobuf Confluent framing is not supported. Extremely large single
records still require memory to decode; disk capacity limits total evidence.

The dedicated `tests/certification/` suite covers mocked Kafka scopes/read-only
behavior, real format decoding with a fake Schema Registry, mapping/filter/engine
semantics, SQLite cleanup, streamed HTML escaping, CLI launch arguments and
Streamlit app runs. These tests do not claim validation against a real cluster.

```powershell
python -m pip install -e ".[test,build,security]" "twine>=6,<7"
python -m pytest
python -m build
python -m compileall -q kafka_viewer tests
python -m pip_audit
python -m twine check dist/*
git diff --check
```
