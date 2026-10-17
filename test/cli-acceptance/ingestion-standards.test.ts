import { readFileSync, cpSync } from "node:fs";
import { resolve } from "node:path";
import { execFileSync } from "node:child_process";
import { parse } from "yaml";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  fixture,
  providerRoot,
  providerReference,
  planProject,
  renderProject,
  execute,
  addFlow,
  apply,
} from "../support/project.ts";
function setup(t: any, standard = "snapshot-with-history@v1") {
  const f = fixture(t);
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
  f.project.environments.dev.bindings.lakehouse.kind = "fabric";
  f.save();
  assert.throws(
    () => planProject(f.root),
    /Databricks|databricks|binding|kind/,
  );
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
  f.flow.defaults.source.format = "json"; // Strict text clocks; streamed Parquet needs per-file footer qualification.
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
  const example = parse(readFileSync(resolve(root, "project.yaml"), "utf8"));
  example.packages.dbx = providerReference;
  f.put("project.yaml", example);
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

function managed(t: any, standard: string, source: any) {
  const f = setup(t, "append-only@v1");
  f.flow.ingestion = {
    standard,
    pipeline: "existing_pipeline",
    target: { schema: "bronze" },
  };
  f.flow.defaults.source = source;
  if (standard === "query-ingestion@v1") {
    f.flow.ingestion.history = "SCD_TYPE_2";
    for (const table of Object.values(f.flow.tables) as any[])
      table.ingestion = { cursor: "id" };
  }
  f.save();
  return f;
}
function nativePipeline(files: Record<string, string>) {
  const resources = Object.entries(files).filter(([p]) =>
    p.endsWith(".pipeline.yml"),
  );
  assert.equal(resources.length, 1);
  return Object.values(parse(resources[0][1]).resources.pipelines)[0] as any;
}
test("installed managed database ingestion retains source identity, keys, cursor and native SCD history", (t) => {
  const f = managed(t, "query-ingestion@v1", {
    kind: "uc-connection",
    connection: "synthetic_sql",
    database: "erp",
    schema: "dbo",
    table: "{{table.id}}",
  });
  const plan = planProject(f.root),
    files = renderProject(f.root, plan),
    pipeline = nativePipeline(files);
  assert.equal(pipeline.ingestion_definition.connection_name, "synthetic_sql");
  assert.equal(pipeline.ingestion_definition.objects.length, 2);
  for (const object of pipeline.ingestion_definition.objects) {
    const table = object.table;
    assert.equal(table.source_catalog, "erp");
    assert.equal(table.source_schema, "dbo");
    assert.equal(table.destination_schema, "bronze");
    assert.deepEqual(table.table_configuration, {
      scd_type: "SCD_TYPE_2",
      primary_keys: ["id"],
      query_based_connector_config: { cursor_columns: ["id"] },
    });
  }
  f.flow.tables.customers.ingestion.cursor = "absent";
  f.save();
  assert.throws(() => planProject(f.root), /unknown cursor/);
});
test("installed SharePoint file/list routes retain distinct native object types and contract hints", (t) => {
  const f = managed(t, "sharepoint-ingestion@v1", {
    kind: "sharepoint",
    connection: "synthetic_m365",
    site: "https://example.sharepoint.com/sites/synthetic",
    path: "Shared Documents/{{table.id}}",
    format: "csv",
  });
  f.flow.tables.orders.source = { entity: "list", path: "Lists/Orders" };
  // Lists omit the file format inherited from defaults, so use complete per-table source definitions.
  delete f.flow.defaults.source.format;
  f.flow.tables.customers.source = { format: "csv" };
  f.save();
  const pipeline = nativePipeline(renderProject(f.root, planProject(f.root)));
  assert.equal(pipeline.channel, "PREVIEW");
  const objects = pipeline.ingestion_definition.objects.map(
    (o: any) => o.table.connector_options.sharepoint_options,
  );
  assert.equal(objects[0].entity_type, "FILE");
  assert.match(objects[0].file_ingestion_options.schema_hints, /`id` BIGINT/);
  assert.match(objects[0].url, /Shared%20Documents\/customers$/);
  assert.deepEqual(objects[1], {
    entity_type: "LIST",
    url: "https://example.sharepoint.com/sites/synthetic/Lists/Orders",
  });
  f.flow.tables.orders.source.path = "../Other";
  f.save();
  assert.throws(
    () => renderProject(f.root, planProject(f.root)),
    /relative folder/,
  );
});
test("installed Salesforce, HubSpot and Jira native ingestion preserves declared application object boundaries", (t) => {
  const f = managed(t, "saas-ingestion@v1", {
    kind: "salesforce",
    connection: "synthetic_saas",
    object: "{{table.id}}",
  });
  for (const [kind, objects, schema] of [
    ["salesforce", ["Account", "Invoice__c"], "objects"],
    ["hubspot", ["contacts", "deals"], "default"],
    ["jira", ["issues", "projects"], "default"],
  ] as const) {
    f.flow.defaults.source.kind = kind;
    f.flow.tables.customers.source = { object: objects[0] };
    f.flow.tables.orders.source = { object: objects[1] };
    f.save();
    const pipeline = nativePipeline(renderProject(f.root, planProject(f.root)));
    assert.equal(
      pipeline.ingestion_definition.connection_name,
      "synthetic_saas",
    );
    assert.deepEqual(
      pipeline.ingestion_definition.objects.map((o: any) => [
        o.table.source_schema,
        o.table.source_table,
        o.table.table_configuration.include_columns,
      ]),
      objects.map((object) => [schema, object, ["id", "name"]]),
    );
  }
  f.flow.tables.customers.source.object = "Account;DROP";
  f.save();
  assert.throws(() => planProject(f.root), /Jira object/);
});
test("installed publication example retains native conditional receipt boundary and explicit deployment ownership", (t) => {
  const f = fixture(t);
  cpSync(resolve(providerRoot, "examples/snapshot-publication"), f.root, {
    recursive: true,
  });
  const example = parse(readFileSync(resolve(f.root, "project.yaml"), "utf8"));
  example.packages.provider = providerReference;
  f.put("project.yaml", example);
  const plan = planProject(f.root),
    files = renderProject(f.root, plan);
  assert.match(
    files["notebooks/retail_source_customers.py"],
    /IfNotModified|etag|match_condition/,
  );
  assert.ok(files["handover/retail_source_customers.json"]);
  const job = parse(files["resources/publication.yml"]).resources.jobs
    .retail_source_customers;
  assert.equal(job.run_as.service_principal_name, "retail-publisher-dev");
  assert.equal(job.tasks[0].max_retries, 2);
  assert.doesNotMatch(
    JSON.stringify(files),
    /clientSecret\s*[:=]\s*["']literal/,
  );
});

test("installed project builds enforce required quality and preserve last output on unsupported rules", (t) => {
  const f = setup(t),
    path = "flows/source/contracts/customers.yaml";
  const contract = parse(readFileSync(resolve(f.root, path), "utf8"));
  contract.schema[0].properties[1].quality = [
    { id: "name-present", metric: "nullValues", mustBe: 0, severity: "error" },
  ];
  f.put(path, contract);
  const build = () =>
    execute(
      {
        root: f.root,
        environment: "dev",
        allowWrite: true,
        allowNetwork: false,
        allowExecute: false,
      },
      "build",
      { out: "build/accepted" },
    );
  let result = build();
  assert.equal(result.ok, true, JSON.stringify(result.diagnostics));
  const metadata = () =>
    JSON.parse(
      readFileSync(
        resolve(f.root, "build/accepted/ingestron-project.json"),
        "utf8",
      ),
    );
  assert.ok(
    metadata().quality.rules.some(
      (rule: any) =>
        rule.id === "name-present" &&
        rule.mode === "at-load" &&
        rule.outcome === "fail",
    ),
  );
  const saved = readFileSync(
    resolve(f.root, "build/accepted/ingestron-project.json"),
    "utf8",
  );
  contract.schema[0].properties[1].quality = [
    {
      id: "external-engine",
      type: "custom",
      engine: "soda",
      implementation: { checks: [] },
      severity: "error",
    },
  ];
  f.put(path, contract);
  f.flow.ingestion = {
    standard: "query-ingestion@v1",
    pipeline: "existing_pipeline",
    target: { schema: "bronze" },
    history: "SCD_TYPE_1",
  };
  f.flow.defaults.source = {
    kind: "uc-connection",
    connection: "synthetic_sql",
    database: "erp",
    schema: "dbo",
    table: "{{table.id}}",
  };
  for (const table of Object.values(f.flow.tables) as any[])
    table.ingestion = { cursor: "id" };
  f.save();
  result = build();
  assert.equal(result.ok, false);
  assert.equal(result.diagnostics[0].code, "QUALITY");
  assert.match(result.diagnostics[0].message, /cannot be enforced/);
  assert.equal(
    readFileSync(
      resolve(f.root, "build/accepted/ingestron-project.json"),
      "utf8",
    ),
    saved,
  );
  f.project.defaults.quality = { unsupported: "report" };
  f.save();
  result = build();
  assert.equal(result.ok, true, JSON.stringify(result.diagnostics));
  assert.ok(
    metadata().quality.rules.some(
      (rule: any) =>
        rule.id === "external-engine" && rule.mode === "unsupported",
    ),
  );
  assert.equal(metadata().deployment.executed, false);
  delete f.project.defaults.quality;
  f.save();
  for (const [standard, source] of [
    [
      "sharepoint-ingestion@v1",
      {
        kind: "sharepoint",
        connection: "synthetic_m365",
        site: "https://example.sharepoint.com/sites/synthetic",
        path: "Shared Documents/{{table.id}}",
        format: "csv",
      },
    ],
    [
      "saas-ingestion@v1",
      {
        kind: "salesforce",
        connection: "synthetic_saas",
        object: "{{table.id}}",
      },
    ],
  ] as const) {
    f.flow.ingestion = {
      standard,
      pipeline: "existing_pipeline",
      target: { schema: "bronze" },
    };
    f.flow.defaults.source = source;
    for (const table of Object.values(f.flow.tables) as any[])
      delete table.ingestion;
    f.save();
    result = build();
    assert.equal(result.ok, false);
    assert.equal(result.diagnostics[0].code, "QUALITY");
  }
});

test("installed managed builds declare after-load checks and gate dependent native transformations", (t) => {
  const f = managed(t, "query-ingestion@v1", {
    kind: "uc-connection",
    connection: "synthetic_sql",
    database: "erp",
    schema: "dbo",
    table: "{{table.id}}",
  });
  const contract = parse(
    readFileSync(
      resolve(f.root, "flows/source/contracts/customers.yaml"),
      "utf8",
    ),
  );
  contract.schema[0].quality = [
    {
      id: "nonempty",
      metric: "rowCount",
      mustBeGreaterThan: 0,
      severity: "error",
    },
  ];
  contract.schema[0].properties[1].quality = [
    { id: "present", metric: "nullValues", mustBe: 0, severity: "error" },
  ];
  f.put("flows/source/contracts/customers.yaml", contract);
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
          pipelineId: "reporting",
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
  const result = execute(
    {
      root: f.root,
      environment: "dev",
      allowWrite: true,
      allowNetwork: false,
      allowExecute: false,
    },
    "build",
    { out: "build/managed-quality" },
  );
  assert.equal(result.ok, true, JSON.stringify(result.diagnostics));
  const metadata = JSON.parse(
    readFileSync(
      resolve(f.root, "build/managed-quality/ingestron-project.json"),
      "utf8",
    ),
  );
  assert.ok(
    metadata.quality.rules.some(
      (q: any) =>
        q.flow === "source" && q.id === "nonempty" && q.mode === "after-load",
    ),
  );
  assert.ok(
    metadata.quality.rules.some(
      (q: any) =>
        q.flow === "source" && q.id === "present" && q.mode === "after-load",
    ),
  );
  const job = parse(
    readFileSync(
      resolve(f.root, "build/managed-quality/resources/ingestion.job.yml"),
      "utf8",
    ),
  ).resources.jobs.processing;
  assert.deepEqual(
    job.tasks.find((task: any) => task.task_key === "refresh_reporting")
      .depends_on,
    [{ task_key: "quality_existing_pipeline" }],
  );
  assert.ok(
    readFileSync(
      resolve(
        f.root,
        "build/managed-quality/operations/quality_existing_pipeline.ipynb",
      ),
      "utf8",
    ).includes("versionAsOf"),
  );
  assert.equal(metadata.deployment.executed, false);
});

test("installed native managed routes reject unsupported renames before replacing output", (t) => {
  const f = managed(t, "query-ingestion@v1", {
    kind: "uc-connection",
    connection: "synthetic_sql",
    database: "erp",
    schema: "dbo",
    table: "{{table.id}}",
  });
  const build = () =>
    execute(
      {
        root: f.root,
        environment: "dev",
        allowWrite: true,
        allowExecute: false,
        allowNetwork: false,
      },
      "build",
      { out: "build/rename-guard" },
    );
  assert.equal(build().ok, true);
  const path = resolve(f.root, "build/rename-guard/ingestron-project.json"),
    saved = readFileSync(path, "utf8");
  const contract = parse(
    readFileSync(
      resolve(f.root, "flows/source/contracts/customers.yaml"),
      "utf8",
    ),
  );
  contract.schema[0].properties[1].physicalName = "original_name";
  f.put("flows/source/contracts/customers.yaml", contract);
  const result = build();
  assert.equal(result.ok, false);
  assert.match(
    result.diagnostics[0].message,
    /materialized-view transformation/,
  );
  assert.equal(readFileSync(path, "utf8"), saved);
});

test("mapped file outputs publish native aliases, quality, freshness and accurate downstream contracts", (t) => {
  const f = setup(t, "append-only@v1");
  f.flow.defaults.source.format = "json"; // Strict text clocks; streamed Parquet needs per-file footer qualification.
  const contract = parse(
    readFileSync(
      resolve(f.root, "flows/source/contracts/customers.yaml"),
      "utf8",
    ),
  );
  contract.schema[0].properties[0].name = "order_id";
  contract.schema[0].properties[0].physicalName = "id";
  contract.schema[0].properties[1].name = "customer_name";
  contract.schema[0].properties[1].physicalName = "name";
  contract.schema[0].properties[1].quality = [
    { id: "present", metric: "nullValues", mustBe: 0, severity: "error" },
  ];
  contract.schema[0].properties.push({
    name: "updated_at",
    physicalName: "source_time",
    logicalType: "timestamp",
    physicalType: "TIMESTAMP",
  });
  f.put("flows/source/contracts/customers.yaml", contract);
  f.flow.tables.customers.ingestion = {
    freshness: { column: "source_time", maximumAgeHours: 2 },
    expectations: { positive: "order_id > 0" },
  };
  f.save();
  const plan = planProject(f.root);
  const source = plan.nodes.find((n: any) => n.table === "customers");
  const published =
    plan.datasets["retail.source.customers.current"].contract.schema[0]
      .properties;
  assert.deepEqual(
    published.map((p: any) => [p.name, p.physicalName]),
    [
      ["order_id", undefined],
      ["customer_name", undefined],
      ["updated_at", undefined],
    ],
  );
  assert.equal(source.contract.schema[0].properties[0].physicalName, "id");
  const files = renderProject(f.root, plan);
  const sql = files["pipelines/source/customers.sql"];
  assert.match(
    sql,
    /SELECT `id` AS `order_id`, `name` AS `customer_name`, CASE WHEN `source_time`/,
  );
  assert.match(
    sql,
    /schema => '`id` BIGINT, `name` STRING, `source_time` STRING'/,
  );
  assert.match(
    sql,
    /CONSTRAINT q_present EXPECT \(`customer_name` IS NOT NULL\) ON VIOLATION FAIL UPDATE/,
  );
  assert.match(sql, /CONSTRAINT business_positive EXPECT \(order_id > 0\)/);
  assert.match(sql, /AS TIMESTAMP_NTZ/);
  assert.match(sql, /END AS `updated_at`/);
  assert.match(files["operations/freshness.ipynb"], /MAX\(`updated_at`\)/);
  f.put("environments/dev.yaml", f.project.environments.dev);
  f.project.environments.dev = { $resolve: "./environments/dev.yaml" };
  f.save();
  const draft = addFlow(f.root, {
    id: "mapped_report",
    kind: "transformation",
    inputDataset: "retail.source.customers.current",
  });

  const outputContract = draft.changes.find((c: any) =>
    c.path.endsWith("result.odcs.yaml"),
  );
  assert.ok(outputContract);
  assert.doesNotMatch(
    outputContract.after,
    /physicalName: (id|name|source_time)/,
  );
  apply(f.root, draft);
  const report = parse(
    readFileSync(resolve(f.root, "flows/mapped_report/flow.yaml"), "utf8"),
  );
  report.steps[0].with.targetTable = "dev_retail.reporting.mapped_customers";
  f.put("flows/mapped_report/flow.yaml", report);
  f.put(
    "flows/mapped_report/sql/transform.sql",
    "SELECT order_id, customer_name, updated_at FROM source",
  );
  const final = renderProject(f.root, planProject(f.root));
  assert.match(
    final["pipelines/mapped_report/transform.sql"],
    /SELECT order_id, customer_name, updated_at FROM source/,
  );
});

test("mapped complete snapshot executes source projection before native keys, quality and history", (t) => {
  const f = setup(t);
  const contract = parse(
    readFileSync(
      resolve(f.root, "flows/source/contracts/customers.yaml"),
      "utf8",
    ),
  );
  contract.schema[0].properties[0].name = "order_id";
  contract.schema[0].properties[0].physicalName = "id";
  contract.schema[0].properties[1].name = "customer_name";
  contract.schema[0].properties[1].physicalName = "name";
  f.put("flows/source/contracts/customers.yaml", contract);
  f.flow.tables.customers.ingestion = { trackedColumns: ["name"] };
  f.save();
  const plan = planProject(f.root),
    files = renderProject(f.root, plan);
  const history =
    plan.datasets["retail.source.customers.history"].contract.schema[0]
      .properties;
  assert.deepEqual(
    history.map((p: any) => p.name),
    ["order_id", "customer_name", "__START_AT", "__END_AT"],
  );
  assert.ok(history.every((p: any) => p.physicalName === undefined));
  f.put("mapped.ipynb", files["pipelines/source/customers.ipynb"]);
  const outcome = execFileSync(
    "python3",
    [
      resolve(providerRoot, "test/support/mapped-snapshot-check.py"),
      resolve(f.root, "mapped.ipynb"),
    ],
    { encoding: "utf8" },
  );
  assert.match(outcome, /projection-before-checks/);
});

test("mapped change feeds resolve sequencing, deletion metadata, tracked columns and output history", (t) => {
  const f = setup(t, "append-only@v1");
  f.flow.ingestion.standard = "change-feed-with-history@v1";
  f.flow.ingestion.target.historySchema = "history";
  f.flow.ingestion.source = {
    delivery: "change-events",
    deletes: "explicit-events",
    baseline: "included",
  };
  for (const name of ["customers", "orders"]) {
    const contract = parse(
      readFileSync(
        resolve(f.root, `flows/source/contracts/${name}.yaml`),
        "utf8",
      ),
    );
    contract.schema[0].properties[0].name = "business_id";
    contract.schema[0].properties[0].physicalName = "id";
    contract.schema[0].properties[1].name = "business_name";
    contract.schema[0].properties[1].physicalName = "name";
    contract.schema[0].properties.push(
      {
        name: "event_order",
        physicalName: "source_sequence",
        logicalType: "integer",
        physicalType: "BIGINT",
        required: true,
      },
      {
        name: "event_operation",
        physicalName: "source_operation",
        logicalType: "string",
        physicalType: "STRING",
        required: true,
      },
    );
    f.put(`flows/source/contracts/${name}.yaml`, contract);
    f.flow.tables[name].ingestion = {
      keys: ["id"],
      sequenceBy: ["source_sequence"],
      operationColumn: "source_operation",
      trackedColumns: ["name"],
    };
  }
  f.save();
  const plan = planProject(f.root),
    files = renderProject(f.root, plan);
  const sql = files["pipelines/source/customers.sql"];
  assert.match(sql, /`id` AS `business_id`/);
  assert.match(sql, /`source_sequence` AS `event_order`/);
  assert.match(sql, /operation_valid EXPECT \(`event_operation` IN/);
  assert.match(sql, /KEYS \(`business_id`\)/);
  assert.match(sql, /SEQUENCE BY `event_order`/);
  assert.match(sql, /APPLY AS DELETE WHEN `event_operation` = 'D'/);
  assert.match(sql, /TRACK HISTORY ON \(`business_name`\)/);
  assert.match(sql, /SELECT `business_id`, `business_name` FROM/);
  const current =
    plan.datasets["retail.source.customers.current"].contract.schema[0]
      .properties;
  assert.deepEqual(
    current.map((p: any) => p.name),
    ["business_id", "business_name"],
  );
  assert.ok(current.every((p: any) => p.physicalName === undefined));
});

test("invalid mapped output columns block installed builds without replacing prepared assets", (t) => {
  const f = setup(t, "append-only@v1"),
    path = "flows/source/contracts/customers.yaml";
  const build = () =>
    execute(
      {
        root: f.root,
        environment: "dev",
        allowWrite: true,
        allowNetwork: false,
        allowExecute: false,
      },
      "build",
      { out: "build/mapping-fence" },
    );
  assert.equal(build().ok, true);
  const artifact = resolve(
      f.root,
      "build/mapping-fence/pipelines/source/customers.sql",
    ),
    original = readFileSync(artifact, "utf8");
  const contract = parse(readFileSync(resolve(f.root, path), "utf8"));
  contract.schema[0].properties[0].physicalName = "id";
  for (const names of [
    ["key", "KEY"],
    ["__START_AT", "name"],
  ]) {
    contract.schema[0].properties[0].name = names[0];
    contract.schema[0].properties[1].name = names[1];
    contract.schema[0].properties[1].physicalName = "name";
    f.put(path, contract);
    const failure = build();
    assert.equal(failure.ok, false);
    assert.match(
      JSON.stringify(failure.diagnostics),
      /Invalid, reserved or duplicate output column/,
    );
    assert.equal(readFileSync(artifact, "utf8"), original);
  }
});

test("installed complete-snapshot quality bounds SQL results and preserves exact metrics before CDC", (t) => {
  const f = setup(t),
    files = renderProject(f.root, planProject(f.root));
  f.put("snapshot-quality.ipynb", files["pipelines/source/customers.ipynb"]);
  const output = execFileSync(
    "python3",
    [
      resolve(providerRoot, "test/support/snapshot-quality-check.py"),
      resolve(f.root, "snapshot-quality.ipynb"),
    ],
    { encoding: "utf8" },
  );
  assert.match(output, /before-load denial and metric privacy checks passed/);
});
