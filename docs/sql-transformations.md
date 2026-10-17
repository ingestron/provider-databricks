# SQL transformations

Create a SQL transformation from a published input dataset. Choose a pipeline,
output table and reviewed output contract, then review the creation proposal.
The transformation owns a separate copy of its output contract. Editing that
copy does not change the upstream dataset contract.

Use the declared input aliases in the query. A single query may contain joins,
CTEs, subqueries, aggregates, windows and scalar expressions. Physical tables,
external table-valued readers, dynamic identifiers, shell-script transformations,
time-travel overrides, PIVOT and commands are refused. CTEs cannot shadow input
aliases. Templates are expanded using the same context before validation and
rendering.

Strict planning and preparation parse the saved query using the pinned Spark SQL
grammar. A changed source invalidates its previous plan. Parsing establishes
syntax and declared relation boundaries; it does not execute expressions or
prove that vendor functions, expression types or permissions are valid.

The generated materialised view includes native column names and types from the
reviewed output contract. Native columns label query results by position; the
compiler does not infer expression names or types. Review the query against its
output contract. Databricks must still qualify expression compatibility and
execution. After refresh, generated quality checks inspect one committed Delta
version, check declared column types and evaluate supported contract rules,
including required values, primary-key uniqueness and row counts. Failed required
checks prevent dependent job tasks from running.

The parser accepts at most 1 MiB of text, 50000 tokens and bounded query structure.
The existing compiler sandbox limits remain unchanged. SQL is evidence and is
never executed by the compiler.

Implementation sources accessed on 2026-10-06:

- [Parser repository](https://github.com/DTStack/dt-sql-parser).
- [Databricks materialised-view columns](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-create-materialized-view).
- [Databricks SQL development constraints](https://docs.databricks.com/aws/en/ldp/developer/sql-dev).

Local synthetic verification does not establish workspace deployment or
independent engineer usability. Full bundled licences and the deterministic
runtime modification are recorded in the plugin notices and parser provenance.
