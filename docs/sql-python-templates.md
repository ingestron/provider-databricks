# SQL and Python templates

Built-in activities keep Jinja-style `.sql.j2` and `.py.j2` templates beside their
manifest. The Databricks provider renders them using Nunjucks in an isolated
JavaScript engine. This resembles dbt's template syntax, but is not dbt compilation:
`ref()`, adapters, packages and dbt execution are not supplied.

Templates support variables, loops, conditions and packaged macros. Missing values
fail generation. Pure generator inputs describe contracts, settings and bindings;
the renderer has no host filesystem, network, clock or random access.

A transformation's ordinary `.sql` file is literal SQL. Choose `.sql.j2` explicitly
to resolve `environment`, `options`, `node`, `binding` and `columns` during generation.
For example: `SELECT * FROM {{ binding.catalog | identifier }}.reporting.customers`.
Never template credentials. Generated queries are reviewed source, not executed by
Ingestron. Native Lakeflow SQL acceptance remains a separate platform test.

Snapshot history uses a documented Python notebook because the native snapshot API
requires Python. SQL definitions are preferred for append/change ingestion and
materialised views. No Ingestron Python package is generated or installed.
