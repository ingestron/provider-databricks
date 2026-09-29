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
ingestron provider install ingestron/provider-databricks/plugin/provider.yaml@3.2.0
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
| `snapshot-publication@v1`     | Verify immutable Parquet snapshots, then publish a delivery index |

Guides: [choose an ingestion standard](docs/ingestion-standards.md),
[prepare a project for operations](docs/databricks-operations.md),
[snapshot publication](docs/snapshot-publication.md) and
[SQL and Python templates](docs/sql-python-templates.md).
Examples are in [examples](examples); all data in them is synthetic.

## Status

Generation is tested offline against synthetic projects. Deployment and execution
on a real Databricks workspace have not been qualified by this repository; review
generated assets before using them.

## Develop

Use Node 22, pnpm 10.15.0 and Python 3.12, then run `pnpm install` and
`pnpm validate`. The `plugin` folder is build output; rebuild it with `pnpm build`.

## Licence

Apache-2.0, copyright Otrera Limited. Third-party material keeps its own terms;
see [NOTICE](NOTICE). Report security issues as described in [SECURITY.md](SECURITY.md).
