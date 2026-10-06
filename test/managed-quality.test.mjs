import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { resolve } from "node:path";
import { tmpdir } from "node:os";
import { expand, render } from "../plugin/index.mjs";
import { qualityNotebook } from "../src/managed-quality.mjs";
const columns = [
  { name: "order_id", type: "BIGINT", key: true, required: true },
  { name: "status", type: "STRING", key: false, required: false },
];
const contract = {
  apiVersion: "v3.1.0",
  kind: "DataContract",
  id: "orders",
  version: "1.0.0",
  schema: [
    {
      name: "orders",
      logicalType: "object",
      properties: [
        {
          name: "id",
          physicalName: "order_id",
          logicalType: "integer",
          physicalType: "BIGINT",
          primaryKey: true,
          required: true,
        },
        {
          name: "label",
          physicalName: "status",
          logicalType: "string",
          physicalType: "STRING",
          quality: [
            {
              id: "present",
              metric: "nullValues",
              mustBe: 0,
              severity: "error",
            },
          ],
        },
      ],
    },
  ],
};
const step = (standard) =>
  expand({
    providerSource: "synthetic",
    columns: { orders: columns },
    flow: {
      id: "source",
      kind: "ingestion",
      ingestion: {
        standard,
        pipeline: "source",
        target: { schema: "bronze" },
        ...(standard === "query-ingestion@v1" ? { history: "SCD_TYPE_2" } : {}),
      },
      defaults: { with: {} },
      tables: {
        orders: {
          contract,
          source: {},
          steps: {},
          ...(standard === "query-ingestion@v1"
            ? { ingestion: { cursor: "order_id" } }
            : {}),
        },
      },
    },
  }).steps[0];
const plan = () => {
  const s = step("query-ingestion@v1");
  return {
    apiVersion: "ingestron.plan/v1",
    project: "retail",
    environment: "dev",
    pending: [],
    names: { jobs: { source: "retail_source" } },
    bindings: {
      dbx: {
        kind: "databricks",
        catalog: "retail",
        host: "https://example.invalid",
      },
    },
    flows: [{ id: "source", ingestion: s.with, requires: {}, publishes: {} }],
    nodes: [
      {
        id: "source/orders/ingest_orders",
        flow: "source",
        table: "orders",
        uses: s.uses,
        platform: "databricks",
        needs: [],
        binding: "dbx",
        runtime: { implementation: "native", options: {} },
        generation: { kind: "native-notebook" },
        columns,
        source: {
          kind: "uc-connection",
          connection: "synthetic_sql",
          database: "erp",
          schema: "dbo",
          table: "Orders",
        },
        with: { ...s.with, schema: "bronze", table: "orders" },
        implementation: { templates: [] },
      },
    ],
  };
};
test("managed routes compile whole-table quality with physical columns and implied keys", () => {
  for (const standard of [
    "query-ingestion@v1",
    "sharepoint-ingestion@v1",
    "saas-ingestion@v1",
  ]) {
    const s = step(standard);
    assert.equal(
      s.with.quality.find((q) => q.id === "present").column,
      "status",
    );
    assert.ok(s.with.quality.some((q) => q.id === "orders.key-unique"));
  }
});
test("native managed job checks committed versions before dependent pipeline tasks", () => {
  const p = plan();
  p.flows.push({
    id: "report",
    requires: {},
    publishes: { result: { from: "steps.view.outputs.result", contract } },
  });
  p.nodes.push({
    id: "report/view",
    flow: "report",
    step: "view",
    uses: "materialized-view@v1",
    platform: "databricks",
    needs: [p.nodes[0].id],
    binding: "dbx",
    runtime: { implementation: "native", options: {} },
    generation: { kind: "native-notebook" },
    columns: columns.map((column, index) => ({
      ...column,
      target: contract.schema[0].properties[index].name,
    })),
    with: {
      pipelineId: "report",
      targetTable: "retail.reporting.orders",
      sources: {},
    },
    sql: "SELECT CAST(1 AS BIGINT) AS id, 'x' AS label",
    implementation: {
      templates: [
        {
          output: "view.sql",
          content:
            "CREATE OR REFRESH MATERIALIZED VIEW {{ target }} AS {{ query }}",
        },
      ],
    },
  });
  const files = render(p),
    job = files["resources/ingestion.job.yml"].value.resources.jobs.processing;
  assert.deepEqual(
    job.tasks.find((t) => t.task_key === "quality_source").depends_on,
    [{ task_key: "refresh_source" }],
  );
  assert.equal(
    job.tasks.find((t) => t.task_key === "quality_source").run_if,
    "ALL_SUCCESS",
  );
  assert.deepEqual(
    job.tasks.find((t) => t.task_key === "refresh_report").depends_on,
    [{ task_key: "quality_source" }],
  );
  assert.equal(
    job.tasks.find((t) => t.task_key === "quality_source").max_retries,
    0,
  );
  const text = files["operations/quality_source.ipynb"].value.cells
    .at(-1)
    .source.join("");
  assert.match(text, /versionAsOf/);
  assert.match(text, /__END_AT IS NULL/);
  assert.doesNotMatch(text, /\.collect\(/);
  const root = mkdtempSync(resolve(tmpdir(), "managed-quality-"));
  try {
    writeFileSync(
      resolve(root, "quality.ipynb"),
      JSON.stringify(files["operations/quality_source.ipynb"].value),
    );
    assert.match(
      execFileSync(
        "python3",
        [
          "test/support/managed-quality-check.py",
          resolve(root, "quality.ipynb"),
        ],
        { encoding: "utf8" },
      ),
      /semantics passed/,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("managed routes reject silent contract column renames", () => {
  const c = columns.map((column) => ({
    ...column,
    ...(column.name === "status" ? { target: "label" } : {}),
  }));
  for (const standard of [
    "query-ingestion@v1",
    "sharepoint-ingestion@v1",
    "saas-ingestion@v1",
  ])
    assert.throws(
      () =>
        expand({
          providerSource: "synthetic",
          columns: { orders: c },
          flow: {
            id: "source",
            kind: "ingestion",
            ingestion: {
              standard,
              pipeline: "source",
              target: { schema: "bronze" },
            },
            defaults: { with: {} },
            tables: { orders: { contract, source: {}, steps: {} } },
          },
        }),
      /preserves physical source column names/,
    );
});
