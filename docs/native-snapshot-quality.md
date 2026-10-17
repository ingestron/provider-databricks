# Quality on native complete snapshots

The private Databricks 3.8 candidate checks the complete delivered snapshot before
returning it to Lakeflow's snapshot CDC callback. Required failures stop that
snapshot before native ingestion. The check never drops invalid rows: dropping a
record could incorrectly expire its key as a deletion. Warning rules retain the
snapshot and report their measured result.

Library metrics and read-only SQL rules use the same aggregate evaluator as
managed ingestion checks. SQL must return exactly one finite numeric cell. The
evaluator requests at most two result rows to detect invalid cardinality. Empty,
multiple-row, multiple-column, boolean, text, NaN and infinite results fail. Decimal
and int64 metric values retain exact strings in result JSON; decimal comparisons
use decimal thresholds. Reports contain metric results and rule IDs, without
business row values. Required failure messages list IDs rather than result values.

Reports identify `before-load` for complete snapshots. Managed ingestion reports
identify `after-load`: that later check cannot undo an already committed table or
native checkpoint. Neither generated notebook proves a live Spark or workspace
run. Installed tests execute the generated Python with synthetic frames; native
Spark SQL behaviour requires separately authorised target qualification.
