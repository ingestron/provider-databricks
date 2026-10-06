# Column mappings in the private candidate

File-based native ingestion reads each contract property's `physicalName` and
publishes its `name`. For example:

```yaml
properties:
  - name: order_id
    physicalName: source_order_id
    logicalType: integer
    physicalType: BIGINT
    required: true
    primaryKey: true
```

The reader uses `source_order_id BIGINT`; the output contains `order_id BIGINT`.
All columns are projected in one select, so swapping two names retains their
original values. This preserves reviewed types; use a separate transformation for
computed expressions or casts.

Set ingestion keys, sequence/operation/tracking fields and freshness columns using
physical source names. Generation resolves those settings to output names.
Library quality rules use contract names. Write SQL rules, Databricks predicates
and explicit ingestion expectations against output names; generation does not
rewrite SQL text. Snapshot checks run after projection and before AUTO CDC applies
the delivery. Native streaming expectations evaluate projected rows, including
mapped deletion and sequencing metadata. Source schema checks retain the explicit
physical reader schema and rescued-data failure.

Published current/history contracts and newly authored downstream transformation
contracts describe the output names. Event sequencing and operation fields stay
out of business history/current outputs. Native SCD2 metadata remains unchanged.
Case-insensitive name collisions and reserved native metadata names are rejected.

Managed query, SharePoint and SaaS routes preserve physical names and reject
renames in their ingestion contract. Publish renamed columns through a separate
materialised view. This file mapping change belongs to the private candidate;
public installation references remain 3.7.0. Installed generation and synthetic
callback checks do not establish native workspace execution.

Reference checked 2026-10-04:
[Native streaming query columns and expectations](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-create-streaming-table).
