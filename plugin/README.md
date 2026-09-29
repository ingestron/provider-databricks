# Native Databricks provider 1.1.0

This built-in module owns the current declarative ingestion catalogue and native
Databricks bundle assembly. `provider.yaml` exports lakeflow-ingest and materialized-view;
the activity directories hold their real SQL/Python templates. There is no separate
provider npm package or Ingestron Python runtime.

The owning implementation is `src/providers/databricks/ingestion`. Build the CLI root
to emit the self-contained renderer and its third-party notices. The renderer exposes
synchronous validate(plan) and render(plan) through ingestron.provider-generation/v1.
It runs without host filesystem/network access.

Choose builtin:databricks-native with version v1. Provider options use the adjacent
configuration.schema.json; production operations are configured under deployment.
Only complete project exports are supported. Native execution acceptance is separate
from schema, syntax and synthetic tests.
