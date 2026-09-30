// Native resource composition. Activity templates contain table logic only.
import { ingestionDefinition } from "./query-ingestion.mjs";
const check = (condition, message) => {
  if (!condition) throw new Error(message);
};
export function deploymentSettings(plan) {
  const settings = plan.nodes[0].runtime.options.deployment ?? {
    mode: "development",
  };
  for (const node of plan.nodes) {
    check(
      JSON.stringify(
        node.runtime.options.deployment ?? { mode: "development" },
      ) === JSON.stringify(settings),
      "All pipelines in one export must use the same deployment settings",
    );
  }
  if (settings.ci) {
    const [major, minor] = settings.ci.cliVersion.split(".").map(Number);
    check(
      major > 0 || minor >= 294,
      "ci.cliVersion must be at least 0.294.0 for this bundle catalogue",
    );
  }
  if (settings.mode === "production") {
    check(
      settings.runAs?.service_principal_name,
      "Production requires deployment.runAs.service_principal_name",
    );
    check(
      settings.rootPath &&
        !settings.rootPath.includes("/Users/") &&
        !settings.rootPath.includes("current_user"),
      "Production requires a stable shared deployment.rootPath",
    );
    check(
      settings.job?.permissions?.length,
      "Production requires job permissions",
    );
    check(
      settings.job?.notifications?.on_failure?.length,
      "Production requires job failure notifications",
    );
    check(
      settings.job?.timeoutSeconds,
      "Production requires job.timeoutSeconds",
    );
  }
  check(
    !Object.keys(plan.selection ?? {}).length,
    "Deploy complete flows; a partial selection cannot replace pipeline resources safely",
  );
  return settings;
}
export function renderResources(plan, pipelines, put) {
  const settings = deploymentSettings(plan);
  const task = (key) => "refresh_" + key.replaceAll("-", "_");
  const resource = (key) => "pipeline_" + key.replaceAll("-", "_");
  check(
    new Set([...pipelines.keys()].map(task)).size === pipelines.size,
    "Pipeline aliases collide after resource naming",
  );
  const bindings = new Set([...pipelines.values()].map((p) => p.binding));
  check(
    bindings.size === 1,
    "One export targets one Databricks workspace binding",
  );
  const binding = plan.bindings[[...bindings][0]];
  for (const alias of Object.keys(settings.pipelines ?? {}))
    check(
      pipelines.has(alias),
      `Unknown pipeline alias in deployment settings: ${alias}`,
    );
  const ids = new Map();
  const ownership = [];
  const logs = new Set();
  const existingIds = new Set();
  for (const [alias, group] of pipelines) {
    check(
      !group.managed?.length || !group.sources.length,
      `${alias}: a pipeline cannot mix managed ingestion with file or transformation sources`,
    );
    const config = settings.pipelines?.[alias] ?? { ownership: "managed" };
    const external = config.ownership === "external";
    const adopted = config.ownership === "adopt";
    check(
      external || adopted ? !!config.existingId : !config.existingId,
      `${alias}: existingId is required only for external/adopt ownership`,
    );
    if (config.existingId) {
      check(
        !existingIds.has(config.existingId),
        "An existing pipeline ID cannot be used by multiple aliases",
      );
      existingIds.add(config.existingId);
    }
    if (external) {
      check(
        Object.keys(config).every((k) =>
          ["ownership", "existingId"].includes(k),
        ),
        `${alias}: external pipelines cannot receive generated settings`,
      );
      ids.set(alias, config.existingId);
      ownership.push(
        `- ${alias}: externally managed (${config.existingId}). Its owner must attach the listed sources and configure matching targets, permissions and monitoring. This export cannot verify that external implementation.`,
      );
      continue;
    }
    if (settings.mode === "production") {
      check(
        config.permissions?.length,
        `${alias}: production pipelines require permissions`,
      );
      check(
        config.notifications?.some(
          (n) =>
            n.alerts.includes("on-update-failure") ||
            n.alerts.includes("on-update-fatal-failure"),
        ),
        `${alias}: production pipelines require failure notifications`,
      );
      check(
        config.eventLog,
        `${alias}: production pipelines require an eventLog destination`,
      );
      check(
        !config.runAs || config.runAs.service_principal_name,
        `${alias}: production pipeline runAs must be a service principal`,
      );
    }
    if (config.eventLog) {
      const name = [
        config.eventLog.catalog,
        config.eventLog.schema,
        config.eventLog.name,
      ].join(".");
      check(!logs.has(name), "Each pipeline needs a distinct event-log table");
      check(
        !plan.nodes.some(
          (n) =>
            n.with.targetTable === name ||
            [binding.catalog, n.with.schema, n.with.table].join(".") === name ||
            [binding.catalog, n.with.target?.historySchema, n.with.table].join(
              ".",
            ) === name,
        ),
        "Event-log target collides with a data target",
      );
      logs.add(name);
    }
    const key = resource(alias);
    ids.set(alias, "${resources.pipelines." + key + ".id}");
    const first = plan.nodes.find(
      (n) => (n.with.pipeline ?? n.with.pipelineId) === alias,
    );
    const schema =
      config.schema ??
      first.with.schema ??
      first.with.targetTable?.split(".")[1];
    put(`resources/${key}.pipeline.yml`, "yaml", {
      resources: {
        pipelines: {
          [key]: {
            name: config.name ?? `${plan.environment}_${plan.project}_${alias}`,
            catalog: binding.catalog,
            schema,
            serverless: true,
            channel: group.channel ?? "CURRENT",
            continuous: false,
            development: settings.mode === "development",
            ...(group.managed?.length
              ? { ingestion_definition: ingestionDefinition(group.managed) }
              : {
                  libraries: group.sources.map((path) => ({
                    [path.endsWith(".ipynb") ? "notebook" : "file"]: {
                      path: "../" + path,
                    },
                  })),
                }),
            ...(config.runAs ? { run_as: config.runAs } : {}),
            ...(config.permissions ? { permissions: config.permissions } : {}),
            ...(config.notifications
              ? { notifications: config.notifications }
              : {}),
            ...(config.eventLog ? { event_log: config.eventLog } : {}),
          },
        },
      },
    });
    ownership.push(
      adopted
        ? `- ${alias}: adopt existing pipeline ${config.existingId}. Before the FIRST deployment, its owner must review the complete resource replacement and run \`databricks bundle deployment bind ${key} ${config.existingId} -t ${plan.environment}\` from this bundle. Do not deploy unbound; that would create a different pipeline.`
        : `- ${alias}: project-managed resource ${key}; source files are attached by the bundle.`,
    );
  }
  const freshness = plan.nodes.filter((n) => n.with.freshness);
  if (freshness.length) {
    const cells = [
      {
        cell_type: "markdown",
        metadata: {},
        source: [
          "# Check data freshness\n\nRuns after pipeline updates, including updates with no new files. A stale or empty dataset fails this job task and uses the job failure notification. Keep the job scheduled: this cannot alert while the job itself is not running. Native pipeline backlog notifications are not supported by this job task type.\n",
        ],
      },
    ];
    for (const n of freshness) {
      const name = [binding.catalog, n.with.schema, n.with.table]
        .map((v) => "`" + v + "`")
        .join(".");
      const seconds = Math.ceil(n.with.freshness.maximumAgeHours * 3600);
      const query = `SELECT MAX(\`${n.with.freshness.column}\`) >= current_timestamp() - INTERVAL ${seconds} SECONDS AS fresh FROM ${name}`;
      cells.push({
        cell_type: "code",
        metadata: {},
        execution_count: null,
        outputs: [],
        source: [
          `# ${n.flow}.${n.table}: latest business timestamp must be within ${seconds} seconds.\nif spark.sql(${JSON.stringify(query)}).first()["fresh"] is not True:\n    raise ValueError(${JSON.stringify("Stale or empty dataset: " + name)})\n`,
        ],
      });
    }
    put("operations/freshness.ipynb", "json", {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {
        kernelspec: {
          display_name: "Python 3",
          language: "python",
          name: "python3",
        },
        language_info: { name: "python" },
      },
      cells,
    });
  }
  const job = settings.job ?? {};
  check(
    !job.notifications?.on_duration_warning_threshold_exceeded ||
      job.durationWarningSeconds,
    "Duration notifications require job.durationWarningSeconds",
  );
  put("databricks.yml", "yaml", {
    bundle: { name: plan.project, databricks_cli_version: ">=0.294.0" },
    include: ["resources/*.yml", "targets/*.yml"],
  });
  put("resources/ingestion.job.yml", "yaml", {
    resources: {
      jobs: {
        processing: {
          name: job.name ?? plan.names.jobs[plan.nodes[0].flow],
          max_concurrent_runs: 1,
          queue: { enabled: true },
          ...(job.timeoutSeconds
            ? { timeout_seconds: job.timeoutSeconds }
            : {}),
          ...(job.permissions ? { permissions: job.permissions } : {}),
          ...(job.notifications
            ? { email_notifications: job.notifications }
            : {}),
          ...(job.durationWarningSeconds
            ? {
                health: {
                  rules: [
                    {
                      metric: "RUN_DURATION_SECONDS",
                      op: "GREATER_THAN",
                      value: job.durationWarningSeconds,
                    },
                  ],
                },
              }
            : {}),
          ...(job.schedule ? { schedule: job.schedule } : {}),
          tasks: [
            ...[...pipelines].map(([alias, group]) => ({
              task_key: task(alias),
              pipeline_task: {
                pipeline_id: ids.get(alias),
                full_refresh: false,
              },
              max_retries: job.maxRetries ?? 2,
              min_retry_interval_millis:
                (job.retryIntervalSeconds ?? 60) * 1000,
              ...(group.needs.size
                ? {
                    depends_on: [...group.needs].map((p) => ({
                      task_key: task(p),
                    })),
                  }
                : {}),
            })),
            ...(freshness.length
              ? [
                  {
                    task_key: "check_freshness",
                    notebook_task: {
                      notebook_path: "../operations/freshness.ipynb",
                    },
                    depends_on: [...pipelines.keys()].map((p) => ({
                      task_key: task(p),
                    })),
                    max_retries: 0,
                  },
                ]
              : []),
          ],
        },
      },
    },
  });
  put(`targets/${plan.environment}.yml`, "yaml", {
    targets: {
      [plan.environment]: {
        default: true,
        mode: settings.mode,
        workspace: {
          host: binding.host,
          root_path:
            settings.rootPath ??
            `/Workspace/Users/\${workspace.current_user.userName}/.bundle/${plan.project}/${plan.environment}`,
        },
        ...(settings.runAs ? { run_as: settings.runAs } : {}),
      },
    },
  });
  if (settings.ci) {
    // Deliberately validate-only. Promotion remains an explicit platform-owner action.
    put(".github/workflows/databricks.yml", "yaml", {
      name: "Validate Databricks bundle",
      on: { workflow_dispatch: {} },
      permissions: { contents: "read", "id-token": "write" },
      concurrency: {
        group: `bundle-${plan.project}-${plan.environment}`,
        "cancel-in-progress": false,
      },
      jobs: {
        validate: {
          "runs-on": "ubuntu-latest",
          environment: plan.environment,
          env: {
            DATABRICKS_HOST: binding.host,
            DATABRICKS_CLIENT_ID: "${{ vars.DATABRICKS_CLIENT_ID }}",
            DATABRICKS_AUTH_TYPE: "github-oidc",
          },
          steps: [
            {
              uses: "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
            },
            {
              uses: "databricks/setup-cli@cfd9223558b9082c2aabb0c8fa47f1c3db2b7cbd",
              with: { version: settings.ci.cliVersion },
            },
            { run: `databricks bundle validate -t ${plan.environment}` },
          ],
        },
      },
    });
  }
  put(
    "operations/README.md",
    "text",
    `# Operate ${plan.project} (${plan.environment})

## Before deployment

The workspace, catalogues, schemas, source access and principals must exist. Resource
permissions do not grant data privileges. The deployment identity must be allowed to
manage this bundle and use the configured runner. Runner: ${JSON.stringify(settings.runAs ?? "deployment user (development)")}.

Give each runner USE CATALOG / USE SCHEMA and source SELECT or READ VOLUME / external
location access as appropriate. Targets require CREATE TABLE / CREATE MATERIALIZED VIEW.
Event-log schemas need write privileges; operational readers need access to their logs.
Do not grant broad rights to unrelated schemas. Serverless availability and network
access must be checked by the platform owner.

### Source and target inventory

${plan.nodes.map((n) => `- ${n.flow}.${n.table ?? n.step}: source ${n.source?.path ?? "declared upstream datasets"}; target ${n.with.targetTable ?? [binding.catalog, n.with.schema, n.with.table].join(".")}${n.with.target?.historySchema ? "; history " + [binding.catalog, n.with.target.historySchema, n.with.table].join(".") : ""}.`).join("\n")}

## Promotion and monitoring

Review all generated files as one bundle and retain its state/root path across deployments.
Do not deploy two bundles to the same tables. Schedules and notification recipients come
from source configuration; change YAML and regenerate before deployment. Use native
pipeline event logs for quality/update failures and Unity Catalog for lineage. Confirm
actual alert delivery; a successfully completed update may contain no new data.
Freshness checks run only when the job runs. Central monitoring must detect missing job
starts or disabled schedules. Snapshot maximumAgeHours checks capture time; a table
freshness task checks the chosen business column. Neither proves complete source capture.

If the optional GitHub workflow is present, commit this team export at a repository root,
configure the ${plan.environment} GitHub Environment and DATABRICKS_CLIENT_ID variable,
and configure matching Databricks OIDC federation. The manually started workflow only
validates the bundle. It does not deploy or adopt resources. Use your existing reviewed
promotion process to deploy the exact exported commit.

## Failure and recovery

Fix a failed input/configuration and retry the native job. Empty snapshots are blocked
unless allowEmpty is explicit. Never alter accepted snapshot versions or files. Retain
an immutable producer delivery ledger and the baseline plus required deliveries. Counts
and predicates cannot detect same-count content replacement. Pipeline event logs are
execution evidence, not a source-content audit. Snapshot SCD2 boundaries are integer
versions; preserve capturedAt mapping in the delivery ledger.

Do not full-refresh after required source inputs expire. Rebuild into isolated targets
and reconcile the result/history first. Reverting code does not revert committed data.
Adoption requires the separate reviewed bind described in the root README.

## Acceptance record

Record native CLI/workspace versions, deploy and redeploy IDs, effective run identities,
operator access checks, first/update/delete results, duplicate/out-of-order handling,
failed DQ and delivery guards, received notifications, stale/no-input behaviour, retry,
and an isolated recovery comparison. Test representative data volume and cost.
Offline schema, syntax and synthetic tests do not establish any of those native results.
`,
  );
  return ownership.join("\n");
}
