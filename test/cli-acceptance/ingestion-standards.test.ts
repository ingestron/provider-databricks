import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { parse } from "yaml";
import { test } from "node:test";
import assert from "node:assert/strict";
import { fixture, installWorking, providerRoot } from "../support/project.js";
import { planProject } from "../../../cli/src/core/planner.js";
import { renderProject } from "../../../cli/src/core/generate.js";
function setup(t: any, standard = "snapshot-with-history@v1") {
  const f = fixture(t);
  f.project.providers.packages.dbx.source = "databricks";
  f.project.environments.dev.bindings.files = { kind: "adls" };
  delete f.flow.standard;
  const snapshot = standard.startsWith("snapshot");
  f.flow.ingestion = {
    standard,
    pipeline: "existing_pipeline",
    target: {
      schema: "current",
      ...(snapshot ? { historySchema: "history" } : {}),
    },
    source: {
      delivery: snapshot ? "complete-snapshot" : "immutable-files",
      deletes: snapshot ? "missing-keys" : "not-applicable",
      ...(snapshot ? { scope: "full-table" } : {}),
    },
    retention: { sourceDeliveries: "externally-retained", period: "P90D" },
  };
  f.flow.defaults.source = {
    binding: "files",
    format: "parquet",
    path: "/Volumes/raw/erp/deliveries/{{table.id}}",
    ...(snapshot
      ? {
          deliveryIndex:
            "/Volumes/raw/erp/deliveries/{{table.id}}/deliveries.json",
        }
      : {}),
  };
  for (const value of Object.values(f.flow.tables) as any[]) value.source = {};
  const save = () => {
    f.put("project.yaml", f.project);
    f.put("flows/source/flow.yaml", f.flow);
  };
  save();
  return { ...f, save };
}
test("snapshot standards compile table-specific native history without runtime or per-run copies", (t) => {
  const f = setup(t),
    plan = planProject(f.root);
  assert.equal(plan.nodes.length, 2);
  assert.equal(plan.recovery[0].replaySource, "external-deliveries");
  const files = renderProject(f.root, plan);
  const notebooks = Object.entries(files).filter(([p]) => p.endsWith(".ipynb"));
  assert.equal(notebooks.length, 2);
  for (const [, source] of notebooks) {
    const code = JSON.parse(source).cells.at(-1).source.join("");
    assert.match(code, /create_auto_cdc_from_snapshot_flow/);
    assert.match(code, /rowCount/);
    assert.doesNotMatch(code, /from ingestron|\.write\.format/);
  }
  assert.ok(files["resources/ingestion.job.yml"]);
});
test("append standards emit SQL Auto Loader and no Python scaffolding", (t) => {
  const f = setup(t, "append-only@v1"),
    files = renderProject(f.root, planProject(f.root));
  assert.match(
    files["pipelines/source/customers.sql"],
    /FROM STREAM read_files/,
  );
  assert.equal(
    Object.keys(files).filter((p) => p.endsWith(".ipynb") || p.endsWith(".py"))
      .length,
    0,
  );
});
test("standards reject false semantic promises and platform substitution", (t) => {
  const f = setup(t);
  f.flow.ingestion.source.delivery = "change-events";
  f.save();
  assert.throws(() => planProject(f.root), /delivery does not match/);
  f.flow.ingestion.source.delivery = "complete-snapshot";
  f.flow.ingestion.source.scope = "partial";
  f.save();
  assert.throws(() => planProject(f.root), /INGESTION|full-table/);
  f.flow.ingestion.source.scope = "full-table";
  f.project.providers.packages.dbx.source = "builtin:fabric-native";
  f.save();
  assert.throws(() => planProject(f.root), /Bundled providers have retired/);
});
test("dependent materialised views share a pipeline with ingestion", (t) => {
  const f = setup(t, "append-only@v1");
  f.project.flows.push({ $resolve: "./flows/report/flow.yaml" });
  f.put("flows/report/flow.yaml", {
    apiVersion: "ingestron.flow/v1",
    kind: "transformation",
    id: "report",
    requires: {
      customers: {
        dataset: "retail.source.customers.current",
        select: { mode: "same-run" },
      },
    },
    steps: [
      {
        id: "report",
        uses: "materialized-view@v1",
        with: {
          pipelineId: "existing_pipeline",
          targetTable: "dev_retail.reporting.customers",
          sources: { customers: { from: "requires.customers" } },
          sqlFile: "./report.sql",
        },
      },
    ],
    publishes: {
      customers: {
        from: "steps.report.outputs.result",
        contract: { $resolve: "../source/contracts/customers.yaml" },
      },
    },
  });
  f.put("flows/report/report.sql", "SELECT id, name FROM customers");
  f.save();
  const files = renderProject(f.root, planProject(f.root));
  assert.match(
    files["pipelines/report/report.sql"],
    /CREATE OR REFRESH MATERIALIZED VIEW/,
  );
});
test("change feeds require explicit baseline, sequencing and delete semantics", (t) => {
  const f = setup(t, "append-only@v1");
  f.flow.ingestion.standard = "change-feed-with-history@v1";
  f.flow.ingestion.target.historySchema = "history";
  f.flow.ingestion.source = {
    delivery: "change-events",
    deletes: "explicit-events",
    baseline: "included",
  };
  for (const value of Object.values(f.flow.tables) as any[])
    value.ingestion = {
      keys: ["id"],
      sequenceBy: ["change_sequence"],
      operationColumn: "name",
    };
  for (const name of ["customers", "orders"]) {
    const c = parse(
      readFileSync(
        resolve(f.root, `flows/source/contracts/${name}.yaml`),
        "utf8",
      ),
    );
    c.schema[0].properties.push({
      name: "change_sequence",
      logicalType: "integer",
      physicalType: "BIGINT",
      required: true,
    });
    f.put(`flows/source/contracts/${name}.yaml`, c);
  }
  f.save();
  const plan = planProject(f.root),
    files = renderProject(f.root, plan);
  assert.match(files["pipelines/source/customers.sql"], /AUTO CDC INTO/);
  assert.match(files["pipelines/source/customers.sql"], /APPLY AS DELETE/);
  const published =
    plan.datasets["retail.source.customers.current"].contract.schema[0]
      .properties;
  assert.equal(
    published.some((p: any) => p.name === "name"),
    false,
  );
  assert.ok(plan.datasets["retail.source.customers.history"]);
  delete f.flow.ingestion.source.baseline;
  f.save();
  assert.throws(() => planProject(f.root), /included baseline/);
});
test("generated snapshot callback rejects incomplete and corrupt deliveries before CDC", async (t) => {
  const { execFileSync } = await import("node:child_process");
  const { resolve } = await import("node:path");
  const f = setup(t),
    files = renderProject(f.root, planProject(f.root));
  f.put("snapshot.ipynb", files["pipelines/source/customers.ipynb"]);
  const output = execFileSync(
    "python3",
    [
      resolve(providerRoot, "test/support/lakeflow-snapshot-check.py"),
      resolve(f.root, "snapshot.ipynb"),
    ],
    { encoding: "utf8" },
  );
  assert.match(output, /semantics passed/);
});

function production(f: ReturnType<typeof setup>) {
  const deployment: any = {
    mode: "production",
    rootPath: "/Workspace/Shared/retail/prod",
    runAs: { service_principal_name: "retail-runner" },
    job: {
      permissions: [{ group_name: "operators", level: "CAN_MANAGE_RUN" }],
      notifications: { on_failure: ["operations@example.com"] },
      timeoutSeconds: 3600,
      durationWarningSeconds: 1800,
      schedule: {
        quartz_cron_expression: "0 0 6 * * ?",
        timezone_id: "UTC",
        pause_status: "PAUSED",
      },
    },
    pipelines: {
      existing_pipeline: {
        ownership: "managed",
        permissions: [{ group_name: "operators", level: "CAN_RUN" }],
        notifications: [
          {
            alerts: ["on-update-failure"],
            email_recipients: ["operations@example.com"],
          },
        ],
        eventLog: {
          catalog: "dev_retail",
          schema: "operations",
          name: "source_events",
        },
      },
    },
    ci: { provider: "github", cliVersion: "0.294.0" },
  };
  f.project.providers.configurations.engineering.options = { deployment };
  f.save();
  return deployment;
}
test("managed bundles attach every source and configure production operations", (t) => {
  const f = setup(t);
  production(f);
  const files = renderProject(f.root, planProject(f.root));
  const pipeline = parse(
    files["resources/pipeline_existing_pipeline.pipeline.yml"],
  ).resources.pipelines.pipeline_existing_pipeline;
  assert.equal(pipeline.serverless, true);
  assert.equal(pipeline.development, false);
  assert.equal(pipeline.libraries.length, 2);
  for (const library of pipeline.libraries)
    assert.ok(files[library.notebook.path.slice(3)]);
  assert.equal(pipeline.permissions[0].level, "CAN_RUN");
  const job = parse(files["resources/ingestion.job.yml"]).resources.jobs
    .processing;
  assert.equal(
    job.tasks[0].pipeline_task.pipeline_id,
    "${resources.pipelines.pipeline_existing_pipeline.id}",
  );
  assert.equal(job.timeout_seconds, 3600);
  assert.equal(job.email_notifications.on_failure[0], "operations@example.com");
  assert.equal(job.health.rules[0].value, 1800);
  assert.equal(job.schedule.pause_status, "PAUSED");
  assert.equal(
    parse(files["targets/dev.yml"]).targets.dev.run_as.service_principal_name,
    "retail-runner",
  );
  const ci = parse(files[".github/workflows/databricks.yml"]);
  assert.equal(ci.jobs.validate.env.DATABRICKS_AUTH_TYPE, "github-oidc");
  assert.equal(ci.jobs.validate.steps[1].with.version, "0.294.0");
  assert.doesNotMatch(
    files[".github/workflows/databricks.yml"],
    /bundle deploy|@main|@v4/,
  );
});
test("production gates reject missing identity, ACLs, notification and timeout settings", (t) => {
  const f = setup(t);
  const deployment = production(f);
  for (const field of ["runAs", "rootPath", "job"]) {
    const saved = deployment[field];
    delete deployment[field];
    f.save();
    assert.throws(
      () => renderProject(f.root, planProject(f.root)),
      /Production requires/,
    );
    deployment[field] = saved;
  }
  delete deployment.pipelines.existing_pipeline.permissions;
  f.save();
  assert.throws(
    () => renderProject(f.root, planProject(f.root)),
    /production pipelines require permissions/,
  );
});
test("external ownership never rewrites a shared pipeline and adoption is explicit", (t) => {
  const f = setup(t);
  const deployment = production(f);
  deployment.pipelines.existing_pipeline = {
    ownership: "external",
    existingId: "shared-id",
  };
  f.save();
  let files = renderProject(f.root, planProject(f.root));
  assert.equal(
    files["resources/pipeline_existing_pipeline.pipeline.yml"],
    undefined,
  );
  assert.equal(
    parse(files["resources/ingestion.job.yml"]).resources.jobs.processing
      .tasks[0].pipeline_task.pipeline_id,
    "shared-id",
  );
  deployment.pipelines.existing_pipeline.permissions = [
    { group_name: "x", level: "CAN_RUN" },
  ];
  f.save();
  assert.throws(
    () => renderProject(f.root, planProject(f.root)),
    /external pipelines cannot receive/,
  );
  const managed = production(f);
  managed.pipelines.existing_pipeline.ownership = "adopt";
  managed.pipelines.existing_pipeline.existingId = "existing-id";
  f.save();
  files = renderProject(f.root, planProject(f.root));
  assert.match(
    files["README.md"],
    /deployment bind pipeline_existing_pipeline existing-id/,
  );
});
test("provider rejects ignored fields, wrong notification formats and unsafe partial exports", (t) => {
  const f = setup(t);
  const deployment = production(f);
  deployment.job.notifications.on_failure = ["not-email"];
  f.save();
  assert.throws(() => planProject(f.root), /email/);
  deployment.job.notifications.on_failure = ["ops@example.com"];
  deployment.job.mystery = true;
  f.save();
  assert.throws(() => planProject(f.root), /additional properties/);
  delete deployment.job.mystery;
  f.save();
  assert.throws(
    () => planProject(f.root, "dev", { flow: "source", table: "customers" }),
    /partial selection/,
  );
});
test("SQL business expectations and snapshot guard settings reach the generated table logic", (t) => {
  const f = setup(t);
  f.flow.ingestion.snapshotPolicy = {
    maximumDropPercent: 30,
    maximumAgeHours: 48,
  };
  f.flow.tables.customers.ingestion = {
    snapshotPolicy: { allowEmpty: true },
    expectations: { positive_id: "id > 0" },
  };
  f.save();
  const files = renderProject(f.root, planProject(f.root));
  const code = JSON.parse(files["pipelines/source/customers.ipynb"])
    .cells.at(-1)
    .source.join("");
  assert.match(code, /maximumDropPercent/);
  assert.match(code, /id > 0/);
  assert.match(code, /contractVersion/);
  assert.match(code, /maximumAgeHours/);
  f.flow.tables.customers.ingestion.expectations.positive_id =
    "id > 0; DROP TABLE x";
  f.save();
  assert.throws(
    () => renderProject(f.root, planProject(f.root)),
    /single SQL predicate/,
  );
});

test("freshness is a dependent serverless task and rejects non-timestamp columns", (t) => {
  const f = setup(t, "append-only@v1");
  const contract = parse(
    readFileSync(
      resolve(f.root, "flows/source/contracts/customers.yaml"),
      "utf8",
    ),
  );
  contract.schema[0].properties.push({
    name: "captured_at",
    logicalType: "timestamp",
    physicalType: "TIMESTAMP",
  });
  f.put("flows/source/contracts/customers.yaml", contract);
  f.flow.tables.customers.ingestion = {
    freshness: { column: "captured_at", maximumAgeHours: 24 },
  };
  f.save();
  const files = renderProject(f.root, planProject(f.root));
  const job = parse(files["resources/ingestion.job.yml"]).resources.jobs
    .processing;
  assert.equal(job.tasks.at(-1).task_key, "check_freshness");
  assert.deepEqual(job.tasks.at(-1).depends_on, [
    { task_key: "refresh_existing_pipeline" },
  ]);
  assert.match(files["operations/freshness.ipynb"], /MAX|Stale or empty/);
  f.flow.tables.customers.ingestion.freshness.column = "id";
  f.save();
  assert.throws(() => planProject(f.root), /published DATE or TIMESTAMP/);
});

test("production example resolves operational settings without editing generated source", async (t) => {
  const f = fixture(t),
    root = f.root;
  const { cpSync } = await import("node:fs");
  cpSync(resolve(providerRoot, "examples/ingestion"), root, {
    recursive: true,
  });
  const files = renderProject(root, planProject(root, "prod"));
  const target = parse(files["targets/prod.yml"]).targets.prod;
  assert.equal(target.mode, "production");
  assert.equal(
    target.run_as.service_principal_name,
    "retail-production-runner",
  );
  assert.equal(
    Object.keys(files).filter((p) => p.endsWith(".pipeline.yml")).length,
    3,
  );
  assert.ok(files["operations/README.md"]);
});

test("generated snapshot safeguards reject abnormal drops and stale caught-up indexes", async (t) => {
  const { execFileSync } = await import("node:child_process");
  const f = setup(t);
  f.flow.ingestion.snapshotPolicy = {
    maximumDropPercent: 30,
    maximumAgeHours: 48,
  };
  f.save();
  const files = renderProject(f.root, planProject(f.root));
  f.put("snapshot.ipynb", files["pipelines/source/customers.ipynb"]);
  assert.match(
    execFileSync(
      "python3",
      [
        resolve(providerRoot, "test/support/lakeflow-snapshot-check.py"),
        resolve(f.root, "snapshot.ipynb"),
        "--guards",
      ],
      { encoding: "utf8" },
    ),
    /semantics passed/,
  );
});
