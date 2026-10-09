# Ingestron Databricks provider

Generates native Databricks assets from reviewed Ingestron projects: Asset Bundle
configuration, Lakeflow ingestion (snapshot with history, change feed with history,
append-only), materialised views and verified snapshot publication. Output is plain
files you review, commit and deploy with your own Databricks tooling.

The provider only generates files. It does not deploy, run jobs or connect to your
workspace or data.

## Install

With the Ingestron CLI, inside a project:

```sh
ingestron provider install ingestron/provider-databricks/plugin/provider.yaml@3.7.0
```

Then select the provider in a flow and run `ingestron check` and `ingestron build`.
See the [Ingestron documentation](https://docs.ingestron.io) for projects,
contracts and plugins.

## Standards

| Standard                      | What it generates                                                 |
| ----------------------------- | ----------------------------------------------------------------- |
| `snapshot-with-history@v1`    | Complete versioned snapshots to native history and a current view |
| `change-feed-with-history@v1` | Baseline and full-row change events to history and a current view |
| `append-only@v1`              | Immutable files to a streaming table                              |
| `query-ingestion@v1`          | Managed database tables through a Unity Catalog connection        |
| `sharepoint-ingestion@v1`     | Managed SharePoint folders or lists                               |
| `saas-ingestion@v1`           | Managed Salesforce, HubSpot or Jira objects                       |
| `snapshot-publication@v1`     | Verify immutable Parquet snapshots, then publish a delivery index |

## Contract quality rules

ODCS library rules in the contract (`nullValues`, `missingValues`,
`invalidValues`, `duplicateValues`, `rowCount`) become native checks:

| Standard                                        | Enforcement                                                                                                                                    |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `append-only@v1`, `change-feed-with-history@v1` | Row rules (`mustBe: 0`) as Lakeflow expectations: `ON VIOLATION FAIL UPDATE` for `severity: error`, retained and recorded in metrics otherwise |
| `snapshot-with-history@v1`                      | Every library rule is counted on the delivered snapshot; an error rule stops the update before the snapshot is applied                         |

`type: sql` rules run on complete snapshots through `spark.sql`, where `${table}`
is the delivered snapshot and `${column}` the rule's column; the query must return
one number. `type: custom` rules with `engine: databricks` take a Spark SQL row
predicate as `implementation` and become expectations (or are counted on
snapshots). Queries and predicates are checked offline for shape only: one
read-only statement without comments. Databricks compiles them when the check
runs.

Rules are never used to drop rows. Duplicate, row-count and SQL rules on streaming
standards, and rules for other engines, are reported as not enforced. Requires
Ingestron core 0.12.12 or later. In public 3.7.0, managed query, SharePoint and SaaS routes declare
contract quality unsupported. A required error rule blocks the project build;
`defaults.quality.unsupported: report` explicitly accepts reporting and does not
enforce the rule.

Guides: [choose an ingestion standard](docs/ingestion-standards.md),
[prepare a project for operations](docs/databricks-operations.md),
[snapshot publication](docs/snapshot-publication.md) and
[SQL and Python templates](docs/sql-python-templates.md).
Examples are in [examples](examples); all data in them is synthetic.

## Private candidate 3.8.0

The private candidate adds after-load contract checks for managed database,
SharePoint and SaaS ingestion. It generates one native quality notebook per
pipeline. Required failures stop dependent native job tasks. Checks read one
committed Delta version per table and evaluate current rows for SCD2.
They do not roll back already ingested data or the native checkpoint. Public
installation references above remain 3.7.0; the candidate is not a release.
See [managed quality semantics](docs/managed-quality.md).

## Status

Generation is tested offline against synthetic projects. Deployment and execution
on a real Databricks workspace have not been qualified by this repository; review
generated assets before using them.

## Develop

Use Node 22, pnpm 10.15.0 and Python 3.12, then run `pnpm install` and
`pnpm validate`. The `plugin` folder is build output; rebuild it with `pnpm build`.

Run the separate offline installed-package gate with an existing qualified CLI:

```sh
INGESTRON_ACCEPTANCE_CLI_PACKAGE=/absolute/path/to/node_modules/ingestron \
INGESTRON_ACCEPTANCE_OUTPUT=/absolute/path/to/isolated/acceptance \
pnpm acceptance
```

The gate snapshots the built provider into a temporary local Git repository and
installs its exact version through the installed compiler. It downloads no packages
and makes no source or workspace calls. See [acceptance scope](docs/installed-native-acceptance.md).

## Licence

Apache-2.0, copyright Otrera Limited. Third-party material keeps its own terms;
see [NOTICE](NOTICE). Report security issues as described in [SECURITY.md](SECURITY.md).

Private candidate file ingestion applies [column mappings](docs/column-mapping.md) to native output, quality/history keys and downstream contracts.

Private candidate complete snapshots use [bounded native quality checks](docs/native-snapshot-quality.md) before their CDC callback applies a delivery.
Private candidate file temporal fields use [exact calendar and timezone-less timestamp checks](docs/native-temporal-types.md), including source Parquet footer inspection. Streamed Parquet temporal qualification remains open.

Private candidate materialised views support [reviewed Python batch transformations](docs/python-transformations.md) with typed output and after-load quality checks.

## Reviewed shared-platform binding

The local 3.14.0 candidate declares the `programme-platform-binding` host feature.
Its offline `platform_binding` authoring operation translates a reviewed shared
Databricks profile plus explicit `workspaceUrl` and `catalog` resource references
into project provider options and environment bindings. Shared resources remain
externally owned. Bundle state uses a separate project/environment workspace path.

The current adapter supports `databricks-cli` authentication, non-production
profiles and no secret references. Other authentication methods, production
profiles, unknown resource fields and URL credentials are refused. It does not
create a workspace or catalogue, resolve credentials, or apply flow standards and
naming preferences. Those remain explicit pending intent in the binding review.
