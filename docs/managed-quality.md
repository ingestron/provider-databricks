# Managed quality in the private 3.8.0 candidate

Managed database, SharePoint and SaaS ingestion generate native Delta tables.
The private candidate generates a serverless notebook task after each managed
pipeline update. Each downstream pipeline job task waits for that quality task to
succeed. Run the complete generated job to include the quality gate. Running a
pipeline directly bypasses job tasks; it does not execute these checks.

Checks read the latest committed Delta version after the pipeline succeeds, then
read that fixed version for all counts. Required columns and reviewed physical
column types must match. SCD2 checks cover current rows where `__END_AT IS NULL`;
history is retained. Library rules, read-only SQL scalar rules and Databricks row
predicates are evaluated on that same current table snapshot. Unsupported engines
remain unsupported and required error rules still block the build.

A failed error rule fails the quality task, blocks its dependent tasks and uses the
native job failure notification. A warning records aggregate results and retains
rows. Logs contain rule identities, counts, pass/fail and table/version identity;
business rows are not collected or printed. Large integer and decimal metric
values are represented as exact strings.

Coverage is **after-load**. Ingestion has already committed when the check runs.
A failure does not undo target data, reset native checkpoints, isolate outside
consumers or make the table a validated publication. Repair the source/rule and
rerun the job under your operational policy. Other writers can still change the
table; measurements cover the recorded version, not all future versions. Delta
history retention and SELECT/history access are required for the quality identity.

Managed routes preserve physical source column names. A contract requesting a
rename is rejected during generation; use a separate materialised view to publish
renamed business columns. No silently ignored alias is counted as a valid mapping.

The provider only generates these assets. Synthetic evaluator and installed-package
checks are separate from actual Databricks execution, permissions and recovery
qualification. Public installation references remain 3.7.0.

References checked 2026-10-04:

- [Serverless notebook jobs](https://docs.databricks.com/aws/en/dev-tools/bundles/examples)
- [Native job task dependencies](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types)
- [Managed SCD2 columns](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/scd)
