# Ingest deliveries with native Databricks standards

Learn how source semantics select processing: snapshots, change events and immutable
files. A reporting view consumes the snapshot current dataset in the same pipeline.
All data and resource names are synthetic. Nothing connects to a workspace.

## Review and generate

1. Read [the standard choices](../../docs/ingestion-standards.md).
2. Open each flow.yaml in flows/01-snapshots, 02-changes and 03-events. Compare
   ingestion.source and retention; inspect table-specific keys and sequencing.
3. Replace the workspace/catalogue values in environments/dev.yaml before native
   execution. Flow pipeline names are logical aliases; no pre-created pipeline ID
   is needed. Read [operations](../../docs/databricks-operations.md) and compare
   environments/prod.yaml for permissions, alerts, schedule and event logs.
4. Run from this directory:

```sh
ingestron plugin install databricks@2.0.0
ingestron --env dev validate
ingestron --env dev plan --out build/dev.plan.json
ingestron --env dev generate --plan build/dev.plan.json --out generated/dev --ownership team
ingestron validate-output generated/dev
```

Expect three pipeline refresh tasks, SQL files for append/change ingestion and
reporting, and one snapshot definition notebook. No private runtime wheel or
procedural helper folders are needed. The bundle includes managed serverless
pipeline resources and attaches these sources on deployment. Review generated/dev/README.md
and operations/README.md. Generation does not deploy or contact a workspace.

Repeat the commands with `--env prod`, `build/prod.plan.json` and `generated/prod`
to inspect the production variant. Replace its synthetic identities and recipients
before use. Its schedule starts paused, and its optional GitHub workflow only validates.

The sample delivery-index.example.json teaches the upstream completion format;
it is not consumed by local generation and is not a fabricated live delivery.
An upstream process must publish its own truthful index and completed files.

## Start a new project

```sh
ingestron init retail --platform databricks --implementation native
cd retail
ingestron flow add erp --kind ingestion --standard snapshot-with-history@v1
ingestron table add customers --flow erp --contract contracts/customers.yaml
ingestron config fill
```

Create or import the reviewed contract first. `flow add` declares pipeline and source
root inputs; fill them per environment. The pipeline defaults to the flow ID. It defaults to `not-retained` rather than
inventing a retention commitment. Change retention only after deciding who owns it.
Change-feed tables also need sequencing and operation-column settings.

To add domain SQL, use `flow add reporting --kind transformation --input-dataset
retail.erp.customers.current`. It creates a materialised-view draft for this path;
set its target table and edit its SQL and output contract. Native dependencies
are derived from the declared inputs, including dependencies within one pipeline.

## Before accepting the generated project

Check first load, later snapshots/events, deletes, duplicate/null keys and a failed
update. Test repair and full refresh only with sufficient retained inputs. Empty
snapshots are blocked unless explicitly permitted; this example also blocks drops
greater than 25 percent compared with the previous retained delivery entry. Native execution has not been
verified by the offline example gate. ADF standards live in their independent provider repository; Fabric remains future work.
