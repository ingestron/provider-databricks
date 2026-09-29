# Prepare a Databricks project for operations

Use this guide after choosing [an ingestion standard](ingestion-standards.md).
It covers the native Databricks ingestion path. The CLI generates a complete bundle
without connecting to a workspace. Your platform team supplies the existing
workspace, catalogues, schemas, source storage, identities and data privileges.
Native deployment and execution acceptance remain required before production use.

## Configure once per environment

Keep table logic in flow folders. Put operational settings in the provider's
`options.deployment`; it can resolve a whole object from the selected environment:

```yaml
providers:
  configurations:
    engineering:
      package: dbx
      binding: platform
      options:
        deployment: "{{values.deployment}}"
```

Define `values.deployment` in each environment file. `--env prod` selects the
`environments.prod` entry, whose `environment` must also be `prod`. No long environment
variable name is required. Local `$resolve` files and JSON Pointers work here too.
Use `$env` for externally supplied non-secret inputs such as a principal ID. Never
put an access token in these settings: identities are identifiers, not credentials.

The [worked production environment](../examples/ingestion/README.md)
contains all supported operational fields. Its names and email addresses are synthetic.
Replace them with actual workspace principals, groups and recipients before deployment.
A development environment can start with `deployment: {mode: development}`.

| Setting     | Meaning                                                                                          |
| ----------- | ------------------------------------------------------------------------------------------------ |
| `mode`      | `development` or `production`; independent of the environment's name                             |
| `rootPath`  | Stable workspace directory for this project's bundle state and files                             |
| `runAs`     | Native `service_principal_name` or `user_name`; production requires a service principal          |
| `job`       | Name, permissions, email notifications, timeout, duration warning, retries and optional schedule |
| `pipelines` | Map keyed by the logical pipeline aliases used by flows                                          |
| `ci`        | Optional GitHub native-validation workflow with a pinned CLI version                             |

Production validation requires a shared `rootPath`, service-principal `runAs`, job
permissions, failure recipients and timeout. Each managed/adopted pipeline also
requires permissions, failure notifications and a distinct event-log destination.
These checks establish configuration completeness, not whether the identities or
privileges exist. Development defaults are conveniences, not production approval.

Use a separate catalogue or schema namespace for each developer if they deploy
independently. A developer-specific bundle state directory alone does not isolate
fully qualified output table names. Never let two bundles own the same tables.

## Choose who owns each pipeline

A flow's `ingestion.pipeline` is now a logical alias such as `erp`. In a declarative
transformation, `pipelineId` refers to the same alias. These names group source files
into native pipeline resources. They are not workspace IDs unless explicitly mapped
as an external reference.

- `managed` (default): generate a project-owned serverless pipeline, including SQL and
  notebook attachments. Its ID is resolved by the bundle. No pre-created pipeline ID
  is needed. Set `name` if you want a custom display name; otherwise it uses
  `<environment>_<project>_<alias>`.
- `adopt`: supply `existingId` and the full reviewed settings. The generated README
  gives the native binding command. The existing owner must approve adoption and bind
  it before the first deployment. Adoption replaces the resource configuration; do not
  use it for a shared pipeline whose other source files belong to another project.
- `external`: supply `existingId` only. No pipeline resource or settings are emitted.
  Its owner remains responsible for source attachment and compatible processing.
  This is deliberately not a configuration-only deployment of that pipeline.

One complete project export owns the bundle. Partial `--flow`, `--table` or `--step`
exports are rejected for this path: replacing a resource with only part of its source
list would silently remove other definitions. To deploy flows independently, give them
separate project ownership boundaries and explicit external dataset handoffs.

Pipelines use serverless compute, channel CURRENT and triggered updates. Continuous
pipelines and custom classic compute are outside this catalogue. A job refreshes each
pipeline once; dependencies inside a pipeline are evaluated by Lakeflow. Cross-pipeline
dependencies become job task dependencies. Concurrency is one with queuing enabled to
avoid overlapping updates; external schedulers must obey the same ownership rule.

## Separate execution permissions from data permissions

The deployment identity authenticates the Databricks CLI. `runAs` is the identity
that executes generated jobs and pipelines. A pipeline can override the common
identity using its own `runAs`. Grant each runner only the sources and targets it needs.

Job `permissions` uses native CAN_VIEW, CAN_MANAGE_RUN or CAN_MANAGE; pipeline
permissions uses CAN_VIEW, CAN_RUN or CAN_MANAGE. These settings give operators
access to the resource. They do not grant access to the underlying data.

Before deployment, your administrator must verify:

- Deployment identity can create/manage these project resources and use the runner.
- Runner has USE CATALOG and USE SCHEMA on referenced namespaces, SELECT on upstream
  tables, and the required CREATE TABLE / CREATE MATERIALIZED VIEW permissions on
  target and event-log schemas.
- Source access is READ VOLUME for Unity Catalog volumes or the appropriate external
  location/storage access for ADLS paths. The cloud identity behind that access is
  already configured.
- Observability readers can access the configured event-log tables and Unity Catalog
  lineage within the platform's access rules.

The generator never emits broad GRANT statements or changes shared infrastructure.
Inspect `operations/README.md` in the export for the actual source and target inventory.

## Notifications and no-delivery detection

Job `notifications` uses native email notification keys. A duration warning also
needs `durationWarningSeconds`. Pipeline notifications separately use alert names
such as `on-update-failure` and `on-flow-failure`. Job failures and pipeline failures
are related but not interchangeable. Native job streaming-backlog alerts do not
support pipeline tasks, so this provider does not expose that misleading option.

A successful update can mean no new files arrived. For snapshots,
`ingestion.snapshotPolicy.maximumAgeHours` checks the newest completed delivery's
`capturedAt`, even when the callback has already processed every version. For a
published DATE or TIMESTAMP column, each table can instead/additionally declare:

```yaml
ingestion:
  freshness:
    column: source_updated_at
    maximumAgeHours: 48
```

This generates a small documented serverless job notebook after the pipeline tasks.
It checks MAX(column) against the current time and fails for stale or empty datasets.
Choose the timestamp carefully: business event time measures something different
from extraction time. A successful post-load query is not a delivery-count audit.
A scheduled job must actually run for these checks to alert. Your central scheduler
or platform monitoring must detect a disabled schedule or a job that never starts.

Schedules use native Quartz syntax, a timezone ID and explicit PAUSED/UNPAUSED.
The example is paused. Set the intended state in the source YAML before regeneration.
Retry intervals are seconds; the generator converts them to native milliseconds.
A job timeout limits the full workflow, including task retries.

## Protect history from bad snapshots

The delivery index now requires `capturedAt` (ISO timestamp with timezone) and
`contractVersion` (matching the generated source contract). Versions still define
SCD2 boundaries: `__START_AT` and `__END_AT` are delivery versions for snapshots,
not business-time timestamps. Keep the delivery index as the version-to-time record.

Set `snapshotPolicy` at flow level and override individual fields at table level:

```yaml
snapshotPolicy:
  allowEmpty: false
  minimumRows: 100
  maximumDropPercent: 25
  maximumAgeHours: 48
```

Empty snapshots are blocked by default. Allowing an empty snapshot explicitly means
all current keys may expire. `minimumRows` still applies; set it to zero if an
empty delivery is deliberately permitted. A drop limit compares the next delivery's count with the
previous processed version's retained index entry; missing comparison evidence fails.
The first snapshot has no previous count, so use `minimumRows` for that boundary.
Freshness checks the newest completed capture, allowing retained older versions to
be replayed when there is a fresh latest capture. For a deliberately historical
rebuild, review and temporarily remove the freshness limit in a separate environment.

Required values and custom `table.ingestion.expectations` are SQL predicates that fail
updates. For example, `positive_amount: "amount >= 0"`. They do not drop or quarantine
snapshot rows, because dropping them would turn bad data into apparent deletions.
For change events, custom predicates must explicitly accommodate delete payloads
when appropriate, for example `operation = 'D' OR amount >= 0`. SQL predicates are
rendered as source, never executed by the CLI. Native SQL acceptance is still needed.
Snapshot checks use one combined predicate scan, plus count and duplicate-key checks;
large snapshots therefore need representative performance testing.

The CLI cannot prove a producer's completion claim. The source owner must retain
immutable files and a durable delivery ledger, publish the index atomically, and
prevent edits to previously processed entries. Count checks do not detect replacing
values while preserving row counts. No callback writes an audit table as a side
effect: Lakeflow may evaluate definition functions repeatedly. Native event logs
record pipeline execution; they are not an immutable source-content audit.

## CI and native acceptance

`deployment.ci` emits `.github/workflows/databricks.yml` for a **team-owned export**
committed at a Git repository root. It runs only through workflow_dispatch, uses
commit-pinned actions, and installs the explicitly selected Databricks CLI version.
It validates the native bundle; it does not deploy, run data processing or bind resources.
The compiler does not execute this workflow.

The repository administrator configures the matching GitHub Environment, its protection
rules and `DATABRICKS_CLIENT_ID` variable. The Databricks service principal needs a
federation policy for that repository/environment. Authentication is OIDC; no stored
personal token is generated. Review and promote the exact exported commit using the
team's existing deployment process. Bundle validation may contact the workspace and
is distinct from `ingestron validate-output`, which is entirely offline.

Before calling a project production-ready, record native evidence for:

1. First deploy, second deploy with no duplicate resources, and a reviewed code update.
2. Actual job/pipeline identities, operator ACLs and denied access for an unauthorised user.
3. Successful ingestion, updates/deletes, out-of-order input and correct history boundaries.
4. Schema/quality/count failure, empty and abnormal-drop rejection, and delivery freshness
   failure with received alerts. Check pipeline alerts and job alerts separately.
5. Retry after failure without duplicate business results; schedule timezone and timeout.
6. Recovery into an isolated target from retained baseline plus all required deliveries;
   compare the result and history before changing production ownership.
7. Native event-log visibility, expected lineage and representative volume/cost behaviour.

Repair an ordinary failed update by fixing its input/configuration and retrying. Never
rewrite an already accepted snapshot. Do not full-refresh if retained source coverage
is insufficient. Preserve bundle state and resource identity during code rollback;
reverting source code does not undo an already committed data/schema change.

## Platform references

Checked 2026-09-12:

- [Bundle resources and permissions](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources)
- [Run identities](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/run-as)
- [Development and production modes](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes)
- [Job notifications](https://learn.microsoft.com/en-us/azure/databricks/jobs/notifications)
- [GitHub federation](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/provider-github)
