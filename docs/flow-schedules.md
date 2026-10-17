# Review flow-owned Databricks schedules

Private candidate: Databricks provider 3.10.0 with core 0.12.23 and CLI 0.17.10.
This reference describes locally verified generation. These versions have not
been published together, and native scheduled execution has not been qualified.

A schedule belongs to a complete flow. Add it to the same flow file that declares
its tables:

```yaml
schedule:
  apiVersion: ingestron.schedule/v1
  enabled: false
  timeZone: UTC
  start: 2026-10-05T00:07:17Z
  end: 2026-10-06T12:00:17Z
  recurrence:
    kind: daily
    times: ["09:15", "17:45"]
  overlap: native
  missed: native
```

Generation writes `resources/schedules.job.yml` and
`databricks-schedules.json`. Each paired calendar time owns a separate job. The
example creates 09:15 and 17:45 jobs; it does not also create 09:45 or 17:15 jobs.
`enabled: false` keeps every generated schedule paused. Generation makes no
workspace calls and does not create a native schedule.

The generated condition tasks accept periodic triggers at or after `start` and
strictly before `end`. Every refresh, quality and freshness task depends on these
guards. A manual run of a scheduled job does not pass its periodic-trigger guard.
The separate `processing` job remains available for reviewed manual refreshes.
An omitted `end` generates no end guard. An export-wide
`deployment.job.schedule` cannot be combined with flow-owned schedules.

The guard uses the documented trigger timestamp, rather than the actual job
start time, so a queued run is evaluated against its trigger time. Databricks
rounds cron trigger timestamps down to the minute. For an interval with a fixed
seconds component, generated numeric bounds subtract that component from the
reviewed boundary. Native trigger delays, repaired runs and the platform's clock
behaviour still require workspace qualification.

## Choose the recurrence

- `daily`: one or more unique `times` in `HH:mm` format.
- `weekly`: `times` and unique `weekDays`, such as `Monday` and `Friday`.
- `monthly`: `times` and unique `monthDays` from 1 to 31. Databricks skips missing
  calendar dates, such as 31 February.
- `interval`: an integer `minutes` that divides 1,440 exactly, using `UTC`.
  Generation retains the start's minute phase and seconds. Other elapsed
  intervals require portable dispatch, which this candidate does not implement;
  they are rejected instead of approximated with a cron that resets each day.

Native schedules require `overlap: native` and `missed: native`. Portable queue,
skip, latest, and explicit daylight-saving policies cannot be translated into
these native calendars. The job's queue and maximum concurrent runs are per job;
multiple calendar jobs do not establish one shared flow queue.

## Review shared pipelines

A scheduled job includes the flow's complete table and step selection and its
explicit upstream dependency closure. Downstream flows and unrelated manual
members are excluded. All pipeline definitions remain in the bundle.

When selected work shares a pipeline with unrelated members, generation requires
this explicit provider configuration option:

```yaml
options:
  deployment:
    mode: development
    job:
      selectiveRefresh: native-beta
```

In the shared Studio/editor candidate, the saved schedule shows **Target settings**.
Choose **Development** and **Selected tables · native beta**, review the affected
flows, and save the proposed provider configuration. Leaving **Shared pipeline
refresh** unset does not accept selective refresh. Removing it from a mixed
pipeline makes preparation fail while retaining the previous generated assets.
The form preserves other options and does not change either flow's definition.
The isolated Luna Databricks3.10 project exposes these candidate controls. Its
three-table example uses the whole pipeline and leaves selective refresh unset;
it does not silently accept the beta.

The generated pipeline task names the selected qualified current, history and
materialised-view targets in `refresh_selection`. This is a native beta feature.
Confirm its availability and private upstream table behaviour in your workspace
before activation. A complete pipeline selection uses the ordinary pipeline task.
Managed quality and freshness notebooks are scoped to selected nodes.

Table-only and step-only selections cannot create schedules. Each flow supports
at most 100 calendar groups, and each job supports at most 100 tasks, including
its guards. Scheduled job names are compiler resource claims, so another flow or
project cannot silently take over the same owned job.

## Current boundaries

This candidate generates bounded schedules for declarative pipeline flows.
Snapshot publication and connector job schedules still need their own bounded
handover and are rejected. Core 0.12.23 also prevents native scheduling capability
from authorising an unscheduled connector preparation. The provider declares that
host feature explicitly; earlier core hosts reject the candidate.

The installed acceptance uses immutable packages and compares SDK, CLI and
read-only MCP plans, CLI builds, exact table selections and resource claims.
It does not qualify cloud deployment, scheduled execution, source consistency,
selective refresh behaviour, missed runs, cancellation, or recovery.

The native formats and trigger values are documented in the
[Databricks Jobs API](https://docs.databricks.com/api/jobs/v2/create-job) and
[dynamic value references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references).
