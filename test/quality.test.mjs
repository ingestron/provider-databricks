import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { expand, render } from "../plugin/index.mjs";

const columns = [
  { name: "order_id", type: "BIGINT", required: true, key: true },
  { name: "status", type: "STRING", required: false, key: false },
  { name: "amount", type: "DECIMAL(10,2)", required: false, key: false },
];
const contract = (extra = {}) => ({
  apiVersion: "v3.1.0",
  kind: "DataContract",
  id: "orders",
  version: "1.0.0",
  status: "draft",
  schema: [
    {
      name: "orders",
      logicalType: "object",
      physicalType: "table",
      properties: [
        {
          name: "order_id",
          logicalType: "integer",
          physicalType: "BIGINT",
          required: true,
          primaryKey: true,
        },
        {
          name: "order_status",
          physicalName: "status",
          logicalType: "string",
          physicalType: "STRING",
          quality: [
            {
              id: "status-valid",
              metric: "invalidValues",
              arguments: { validValues: ["open", "closed", 3] },
              mustBe: 0,
              severity: "error",
            },
            { id: "status-present", metric: "nullValues", mustBe: 0 },
          ],
        },
        {
          name: "amount",
          logicalType: "number",
          physicalType: "DECIMAL(10,2)",
        },
      ],
      ...extra,
    },
  ],
});
const ingestion = (standard) => {
  const snapshot = standard.startsWith("snapshot");
  return {
    standard,
    pipeline: "orders",
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
};
const expandFor = (standard, value = contract()) =>
  expand({
    providerSource: "test",
    columns: { orders: columns },
    flow: {
      id: "sales",
      kind: "ingestion",
      ingestion: ingestion(standard),
      defaults: { with: {} },
      tables: { orders: { contract: value, steps: {} } },
    },
  }).steps[0];
const plan = (standard, step) => ({
  apiVersion: "ingestron.plan/v1",
  project: "retail",
  environment: "dev",
  pending: [],
  names: { jobs: { sales: "dev_retail_sales" } },
  flows: [{ id: "sales", requires: {}, publishes: {}, ingestion: step.with }],
  bindings: {
    dbx: {
      kind: "databricks",
      host: "https://example.invalid",
      catalog: "retail",
    },
    files: { kind: "adls" },
  },
  nodes: [
    {
      id: "sales/orders/ingest_orders",
      flow: "sales",
      table: "orders",
      uses: step.uses,
      platform: "databricks",
      needs: [],
      binding: "dbx",
      runtime: { implementation: "native", options: {} },
      generation: { kind: "native-notebook" },
      columns,
      source: {
        binding: "files",
        format: "parquet",
        path: "/Volumes/raw/erp/orders",
        ...(standard.startsWith("snapshot")
          ? { deliveryIndex: "/Volumes/raw/erp/orders/deliveries.json" }
          : {}),
      },
      with: { ...step.with, schema: "current", table: "orders" },
      implementation: {
        templates: ["append.sql", "events.sql", "snapshot.py"].map(
          (output) => ({
            output,
            content: readFileSync(
              `plugin/activities/lakeflow-ingest/${output}.j2`,
              "utf8",
            ),
          }),
        ),
      },
    },
  ],
});

test("streaming standards map row-level rules to native expectations", () => {
  const step = expandFor("append-only@v1");
  assert.deepEqual(
    step.with.quality.map((q) => [q.id, q.name, q.outcome]),
    [
      ["status-valid", "q_status_valid", "fail"],
      ["status-present", "q_status_present", "warn"],
    ],
  );
  const sql = render(plan("append-only@v1", step))["pipelines/sales/orders.sql"]
    .value;
  assert.match(
    sql,
    /CONSTRAINT q_status_valid EXPECT \(`status` IS NULL OR `status` IN \('open', 'closed'\)\) ON VIOLATION FAIL UPDATE/,
  );
  assert.match(
    sql,
    /CONSTRAINT q_status_present EXPECT \(`status` IS NOT NULL\)\)\nAS SELECT/,
  );
  assert.doesNotMatch(sql, /key-unique|DROP ROW/);
});

test("streaming standards reject aggregate thresholds instead of misreporting them", () => {
  const value = contract();
  value.schema[0].properties[1].quality = [
    { metric: "nullValues", mustBeLessThan: 5, severity: "error" },
  ];
  assert.throws(() => expandFor("append-only@v1", value), /mustBe: 0/);
  // Table rules the standard cannot check stay out of the output; core
  // coverage reports them from the per-standard declaration.
  const counted = contract({
    quality: [{ metric: "rowCount", mustBeGreaterThan: 0, severity: "error" }],
  });
  assert.equal(
    expandFor("append-only@v1", counted).with.quality.some(
      (q) => q.metric === "rowCount",
    ),
    false,
  );
});

test("complete snapshots evaluate every library rule before applying the snapshot", () => {
  const value = contract({
    quality: [
      {
        id: "volume",
        metric: "rowCount",
        mustBeGreaterThan: 0,
        severity: "error",
      },
    ],
  });
  value.schema[0].properties[1].quality.push({
    id: "few-missing",
    metric: "missingValues",
    mustBeLessThan: 10,
    unit: "percent",
  });
  const step = expandFor("snapshot-with-history@v1", value);
  const ids = step.with.quality.map((q) => q.id);
  assert.deepEqual(ids, [
    "volume",
    "status-valid",
    "status-present",
    "few-missing",
  ]);
  const notebook = render(plan("snapshot-with-history@v1", step))[
    "pipelines/sales/orders.ipynb"
  ].value;
  const code = notebook.cells.at(-1).source.join("");
  assert.match(code, /def check_sales_orders_ingest_orders_quality/);
  assert.match(
    code,
    /check_sales_orders_ingest_orders_quality\(data, delivery\["rowCount"\]\)/,
  );
  assert.match(code, /snapshot not applied/);
  assert.match(
    code,
    /\\"id\\": \\"few-missing\\"|"id":"few-missing"|few-missing/,
  );
  assert.doesNotMatch(code, /status-valid.*print\(data/);
});

test("the generated snapshot evaluator counts, compares and fails without row values", () => {
  const value = contract({
    quality: [
      {
        id: "volume",
        metric: "rowCount",
        mustBeGreaterThan: 5,
        severity: "error",
      },
    ],
  });
  const step = expandFor("snapshot-with-history@v1", value);
  const code = render(plan("snapshot-with-history@v1", step))
    ["pipelines/sales/orders.ipynb"].value.cells.at(-1)
    .source.join("");
  const valid = step.with.quality.find(
    (q) => q.id === "status-valid",
  ).expression;
  const present = step.with.quality.find(
    (q) => q.id === "status-present",
  ).expression;
  const script = (failing) => `
import json, sys
sys.path.insert(0, "test/support")
import fake_spark
rows = [{"order_id": 1, "status": "open"}, {"order_id": 2, "status": "secret-value"}]
try:
    fake_spark.run(sys.stdin.read(), "check_sales_orders_ingest_orders_quality", rows, ${JSON.stringify(failing)}, 2)
    print("PASSED")
except ValueError as error:
    print("FAILED", error)
`;
  const run = (failing) =>
    execFileSync("python3", ["-c", script(failing)], {
      input: code,
      encoding: "utf8",
    });
  const failed = run({ [valid]: 1, [present]: 1 });
  assert.match(
    failed,
    /"id": "status-present", "outcome": "warn", "value": 1, "passed": false/,
  );
  assert.match(
    failed,
    /FAILED Contract quality rules failed; snapshot not applied: volume \(2\), status-valid \(1\)/,
  );
  assert.doesNotMatch(failed, /secret-value/);
});

test("sql rules run on snapshots and databricks predicates become expectations", () => {
  const value = contract({
    quality: [
      {
        id: "no-negative",
        type: "sql",
        query: "SELECT COUNT(*) FROM ${table} WHERE amount < 0",
        mustBe: 0,
        severity: "error",
      },
      {
        id: "dq-positive",
        type: "custom",
        engine: "databricks",
        implementation: "amount IS NULL OR amount >= 0",
        severity: "error",
      },
      { id: "soda-check", type: "custom", engine: "soda", implementation: {} },
    ],
  });
  value.schema[0].properties[1].quality.push({
    id: "status-short",
    type: "sql",
    query: "SELECT MAX(LENGTH(${column})) FROM ${table}",
    mustBeLessOrEqualTo: 10,
  });
  const snapshot = expandFor("snapshot-with-history@v1", value).with.quality;
  const byId = Object.fromEntries(snapshot.map((q) => [q.id, q]));
  assert.equal(
    byId["status-short"].query,
    "SELECT MAX(LENGTH(`status`)) FROM ${table}",
  );
  assert.equal(byId["dq-positive"].metric, "predicate");
  assert.equal(byId["soda-check"], undefined);
  const streaming = expandFor("append-only@v1", value);
  assert.deepEqual(
    streaming.with.quality.map((q) => q.id),
    ["dq-positive", "status-valid", "status-present"],
  );
  const sql = render(plan("append-only@v1", streaming))[
    "pipelines/sales/orders.sql"
  ].value;
  assert.match(
    sql,
    /CONSTRAINT q_dq_positive EXPECT \(amount IS NULL OR amount >= 0\) ON VIOLATION FAIL UPDATE/,
  );
  const code = render(
    plan(
      "snapshot-with-history@v1",
      expandFor("snapshot-with-history@v1", value),
    ),
  )
    ["pipelines/sales/orders.ipynb"].value.cells.at(-1)
    .source.join("");
  assert.match(
    code,
    /spark\.sql\(rule\["query"\]\.replace\("\$\{table\}", view\)\)/,
  );
});

test("sql and predicate text is checked offline for a single read-only statement", () => {
  for (const [rule, message] of [
    [{ type: "sql", query: "SELECT 1; DROP TABLE x", mustBe: 1 }, /semicolons/],
    [{ type: "sql", query: "DELETE FROM ${table}", mustBe: 0 }, /only read/],
    [{ type: "sql", query: "SELECT 1 -- hidden", mustBe: 1 }, /comments/],
    [{ type: "sql", query: "VALUES (1)", mustBe: 1 }, /SELECT or WITH/],
    [
      {
        type: "custom",
        engine: "databricks",
        implementation: "id IN (SELECT id FROM t)",
      },
      /without subqueries/,
    ],
  ])
    assert.throws(
      () =>
        expandFor("snapshot-with-history@v1", contract({ quality: [rule] })),
      message,
    );
  assert.doesNotThrow(() =>
    expandFor(
      "snapshot-with-history@v1",
      contract({
        quality: [
          {
            type: "sql",
            query:
              "SELECT COUNT(*) FROM ${table} WHERE status = 'drop; -- update'",
            mustBe: 0,
          },
        ],
      }),
    ),
  );
});

test("the snapshot evaluator runs sql rules against the delivered snapshot view", () => {
  const value = contract({
    quality: [
      {
        id: "no-negative",
        type: "sql",
        query: "SELECT COUNT(*) FROM ${table} WHERE amount < 0",
        mustBe: 0,
        severity: "error",
      },
    ],
  });
  value.schema[0].properties[1].quality = [];
  const step = expandFor("snapshot-with-history@v1", value);
  const code = render(plan("snapshot-with-history@v1", step))
    ["pipelines/sales/orders.ipynb"].value.cells.at(-1)
    .source.join("");
  const view = "ingestron_quality_sales_orders_ingest_orders";
  const script = `
import sys
sys.path.insert(0, "test/support")
import fake_spark
try:
    fake_spark.run(sys.stdin.read(), "check_sales_orders_ingest_orders_quality", [{"order_id": 1}], {}, 1,
                   {"SELECT COUNT(*) FROM ${view} WHERE amount < 0": 2})
    print("PASSED")
except ValueError as error:
    print("FAILED", error)
`;
  const output = execFileSync("python3", ["-c", script], {
    input: code,
    encoding: "utf8",
  });
  assert.match(
    output,
    /FAILED Contract quality rules failed; snapshot not applied: no-negative \(2\)/,
  );
});
