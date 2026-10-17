# Native file temporal types — private candidate

The candidate file standards publish reviewed `DATE` fields as calendar dates
and `TIMESTAMP` fields as explicit Spark `TIMESTAMP_NTZ`. Session timezones do
not assign an offset to these clocks. This differs from Spark `TIMESTAMP`, which
has timezone semantics. Existing target schemas and Delta table features require
review before adopting this provider version; generation does not migrate them.

For text input, temporal fields remain strings during reading. Generated checks
require a full valid calendar date, or a timezone-less date and time with up to
six fractional digits. Whitespace, invalid dates, offsets and excessive precision
fail with fixed guidance. Nullable fields retain nulls. The native projection
casts only after checking the original text; it cannot silently round an invalid
source into an acceptable timestamp. Snapshot checks run before quality evaluation
and native CDC receives any data. Source and output column mappings remain separate.

For complete Parquet snapshots, the generated callback reads merged source footer
schemas before applying an explicit read schema. Calendar fields must be native
dates; clock fields must be native timezone-less timestamps. Zoned timestamps,
INT96 representations without proven timezone meaning, nanosecond precision and
mixed timezone schemas require a separately reviewed mapping. An explicit
`TIMESTAMP_NTZ` read schema alone is insufficient: Spark can otherwise discard a
source timezone annotation. Calendar/clock values must remain within years 1–9999.

Streamed Parquet temporal fields remain unsupported until a policy verifies each
new file's timezone and precision before conversion. The candidate explains this
at generation; complete snapshots and supported text profiles are available.
Managed query/API ingestion and JDBC timezone/version policies require their own
qualification. Local Spark acceptance is separate from Databricks workspace
execution and does not authorise cloud deployment.

File quality rules compare `DATE` and `TIMESTAMP` allow-lists and missing-value
lists using exact typed literals. Invalid listed calendar dates, offsets or
excess precision cannot match a source value. Nullable values remain distinct
from required-field checks. Physical source names can map to logical output
names; the reader, projection and quality evaluator use the corresponding scope.
This policy does not establish managed/JDBC timestamp equivalence.

The permanent installed acceptance suite is `scripts/acceptance.mjs`. It takes
`INGESTRON_ACCEPTANCE_CLI_PACKAGE`, `INGESTRON_ACCEPTANCE_OUTPUT` and an optional
`INGESTRON_ACCEPTANCE_PROOF` path, freezes the current provider into a synthetic
Git snapshot, and installs it with the matching immutable CLI/core pair. Set
`INGESTRON_ACCEPTANCE_SPARK_PYTHON` to an isolated Python environment with Spark
4.0.0 and PyArrow 23.0.1, and `INGESTRON_ACCEPTANCE_SPARK_RUNTIME` to a JSON file
containing `jdk.home`, to execute the exact generated snapshot callbacks locally.
The harness retains generated notebooks and calendar/precision/footer/quality
proofs under the supplied output directory. It mocks only Lakeflow registration,
uses synthetic rows and loopback Spark, and stops Spark in a final cleanup block.
Passing this check establishes local generated callback semantics; workspace
identity, native CDC state and deployment still require separate qualification.
