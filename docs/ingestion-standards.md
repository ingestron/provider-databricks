# Choose an ingestion standard

An ingestion standard defines what the source delivers, how it becomes a dataset,
and what can be reconstructed later. It is more than a sequence of activities.
Use this guide before selecting an architecture or enabling source history.

The first catalogue targets **native Databricks**. ADF and Fabric implementations
will have their own platform-specific catalogues; they do not inherit Databricks
features. Existing procedural recipes remain available for earlier experiments,
but are not the new ingestion-standard model.

## Separate meaning, policy and implementation

- **Source meaning:** complete snapshots, full-row change events, or immutable files.
- **Architecture policy:** retain upstream deliveries or depend on processed state;
  maintain history where the selected standard requires it.
- **Provider implementation:** Auto Loader, AUTO CDC, native expectations and
  materialised views. These choices are owned by Databricks, not a universal
  cross-platform step sequence.

| Standard                      | Required source                                                               | Databricks output                                              | Recovery limit                                                                |
| ----------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `snapshot-with-history@v1`    | Complete versioned snapshots and completed delivery index                     | AUTO CDC FROM SNAPSHOT history and a current materialised view | Only changes observed between snapshots; intermediate changes are unavailable |
| `change-feed-with-history@v1` | Baseline rows followed by full-row I/U/D events, keys and one sequence column | AUTO CDC history and a current materialised view               | Rebuilding requires the baseline and all change events                        |
| `append-only@v1`              | Immutable JSON or Parquet files                                               | Auto Loader streaming table                                    | Rebuilding requires all original files; updates/deletes are not implemented   |

Timestamp-window extraction is **not CDC**. It may miss deletions and intermediate
changes. Incremental-current-state ingestion, CSV reader policies and managed
Lakeflow Connect ingestion are not in this catalogue yet. Do not relabel them as
one of the supported delivery types.

## Configure a flow once, then add tables

```yaml
ingestion:
  standard: snapshot-with-history@v1
  pipeline: erp
  target:
    schema: current
    historySchema: source_history
  source:
    delivery: complete-snapshot
    scope: full-table
    deletes: missing-keys
  retention:
    sourceDeliveries: externally-retained
    period: P90D

tables:
  customers:
    contract:
      $resolve: ./customers/contract.yaml
    ingestion:
      keys: [customer_id]
```

`pipeline` is a logical alias for a generated Lakeflow pipeline. See
[operational settings and ownership](databricks-operations.md).
Schemas and Unity Catalog access must already exist. Keys belong to each table;
if omitted, the compiler uses the contract's primary keys. A standard never supplies
business keys. Table-specific `trackedColumns` optionally limits SCD2 tracking.
For change feeds, add `sequenceBy: [change_sequence]` and `operationColumn: operation`
to each table's `ingestion` settings. The operation values must be `I`, `U`, or `D`.
Operation and sequence fields are ingestion metadata and are omitted from published
business columns. History exposes native `__START_AT` and `__END_AT` instead.
Use a non-null sortable sequence with unambiguous ordering per key. Partial update
payloads and multiple sequence columns are not currently supported.

The source binding and path remain under `defaults.source` or `tables.<name>.source`.
Paths may use the normal environment and table placeholders. The current provider
reads JSON or Parquet from existing ADLS/Unity Catalog Volume directories. It does
not extract directly from SQL Server or PostgreSQL. An upstream extractor must
produce the agreed delivery format.

## Retention is an explicit responsibility

`externally-retained` with `period: P90D` records an upstream commitment, not a
storage lifecycle rule deployed by the CLI. The owner must retain immutable files
and snapshot indexes for that period. Keeping them costs storage; replay costs
compute. The compiler cannot verify that commitment offline.

`not-retained` must omit `period`. It provides no source replay guarantee. Native
checkpoints support normal processing retries; SCD2 is not an original-input
archive. Do not full-refresh when required original data has expired. A retained
90-day change feed without its baseline does not support a complete rebuild.

Generated plans record the standard, delivery meaning, retention responsibility,
replay source and assumptions. No archive copy or automated deletion is generated.

## Complete snapshot delivery index

The upstream system atomically publishes one JSON file at `source.deliveryIndex`
after the snapshot files are complete. See the
[worked delivery index](../examples/ingestion/README.md).

Inspect its JSON schema with `ingestron schema delivery-index` (also available through MCP).
Version and row-count integers must not exceed 9007199254740991.

The index uses `apiVersion: ingestron.delivery-index/v1`, a dataset identity such
as `retail.erp.customers`, and a `deliveries` array. Every entry contains a unique
`id`, increasing non-negative integer `version`, `complete: true`,
`scope: full-table`, `capturedAt` with timezone, matching `contractVersion`,
expected `rowCount`, and immutable `path` beneath the source
root. Versions order snapshots, not processing timestamps. Never reuse or modify
an already published version. Keep every unprocessed delivery in the index.

The generated callback processes the next version, validates row count and
required values, and rejects duplicate keys. It returns no data when caught up.
Empty snapshots are rejected by default. Explicit `snapshotPolicy.allowEmpty: true`
permits `rowCount: 0` to expire every key. Row-drop and freshness limits are also available.
Partial snapshots must never use this standard: missing keys represent deletions.
Count checks do not prove business completeness; upstream completion must be truthful.

This delivery boundary lets a future ADF extractor and the Databricks consumer
operate independently without requiring identical activity catalogues.

## Generated project and operations

Append and change-feed definitions are SQL files. Snapshot history uses a documented
ipynb definition because AUTO CDC FROM SNAPSHOT requires Python. Domain SQL can be
expressed as `materialized-view@v1` activities, including dependencies in the same
pipeline. Query aliases become local CTEs, not extra published tables.

The bundle includes managed serverless pipelines with source attachments and job
refresh dependencies. Configure permissions, notifications and environment settings
using [the operations guide](databricks-operations.md). External pipelines
remain an explicit ownership choice. Generate the complete project together; partial
exports cannot safely replace a pipeline's source list.

There is no Ingestron runtime, per-run accumulated-table capture or custom event
transport on this path. Use native event logs and Unity Catalog lineage. Required
value and schema checks fail updates; they do not discard snapshot rows and
accidentally turn bad data into deletions.

Use [the walkthrough](../examples/ingestion/README.md) next. CLI and
MCP share the same validation and generation operations. Inspect the provider schema
with `ingestron schema databricks-ingestion`.

## Evidence and platform references

Generation and snapshot callback tests are synthetic and offline. Native Databricks
acceptance, source permissions and recovery under real concurrency remain open.
The provider requires an AUTO CDC-capable pipeline for history. Current platform
references, checked 2026-09-12:

- [Lakeflow SQL](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/sql-dev)
- [AUTO CDC](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc)
- [Snapshot callback](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/ldp-python-ref-apply-changes-from-snapshot)
- [Pipeline event log](https://learn.microsoft.com/en-us/azure/databricks/ldp/monitor-event-logs)
