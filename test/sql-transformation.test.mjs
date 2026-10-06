import { test } from "node:test";
import assert from "node:assert/strict";
import {
  validateSqlSource,
  sqlTransformation,
} from "../src/sql-transformation.mjs";
import { qualityNotebook } from "../src/managed-quality.mjs";

test("SQL parser accepts declared inputs, lexical CTEs, joins, subqueries, windows and expressions", () => {
  for (const sql of [
    "select * from orders",
    "SELECT o.id, sum(o.value) OVER (PARTITION BY o.id) FROM orders o JOIN customers c ON o.id = c.id",
    "WITH selected AS (SELECT id FROM orders), report AS (SELECT id FROM selected) SELECT * FROM report",
    "SELECT * FROM (WITH inner_rows AS (SELECT * FROM orders) SELECT * FROM inner_rows) x UNION ALL SELECT * FROM orders",
    "SELECT transform(array(1,2), x -> x + 1) AS result FROM orders",
    "SELECT * FROM `Orders` WHERE EXISTS (SELECT 1 FROM customers WHERE customers.id = Orders.id)",
  ])
    assert.equal(validateSqlSource(sql, ["orders", "customers"]), sql);
});

test("SQL parser refuses malformed queries, commands, input bypasses and unsupported native clauses", () => {
  for (const sql of [
    "",
    "-- comment",
    "SELECT FROM",
    "SELECT * FROM orders /* unfinished",
    "SELECT * FROM orders; DROP TABLE orders",
    "INSERT INTO orders SELECT 1",
    "DELETE FROM orders",
    "CREATE TABLE unsafe AS SELECT * FROM orders",
    "SELECT * FROM retail.bronze.orders",
    "SELECT * FROM undeclared",
    "SELECT * FROM read_files('/private/data')",
    "SELECT * FROM IDENTIFIER('orders')",
    "SELECT TRANSFORM(id) USING 'cat' FROM orders",
    "SELECT * FROM orders VERSION AS OF 1",
    "SELECT * FROM orders PIVOT(sum(value) FOR id IN (1))",
    "WITH orders AS (SELECT 1) SELECT * FROM orders",
    "WITH x AS (SELECT * FROM orders), x AS (SELECT * FROM orders) SELECT * FROM x",
    "WITH x AS (SELECT * FROM later), later AS (SELECT * FROM orders) SELECT * FROM x",
    "WITH x AS (SELECT * FROM orders) SELECT * FROM orders WHERE EXISTS (SELECT * FROM hidden)",
  ])
    assert.throws(() => validateSqlSource(sql, ["orders"]), undefined, sql);
  assert.throws(
    () => validateSqlSource("SELECT * FROM retail.secret.orders", ["orders"]),
    /not a declared input/,
  );
  assert.throws(
    () => validateSqlSource("SELECT * FROM IDENTIFIER('orders')", ["orders"]),
    /Dynamic SQL identifiers/,
  );
  assert.throws(
    () => validateSqlSource("SELECT * FROM orders", ["orders", "Orders"]),
    /distinct/,
  );
});

test("terminal semicolons are removed by token offsets without damaging Unicode, literals or comments", () => {
  const sql = "SELECT '🦆;雪' AS label FROM orders; -- trailing ; comment\n";
  assert.equal(
    validateSqlSource(sql, ["orders"]),
    "SELECT '🦆;雪' AS label FROM orders -- trailing ; comment\n",
  );
  assert.equal(
    validateSqlSource("SELECT ';' AS label FROM orders", ["orders"]),
    "SELECT ';' AS label FROM orders",
  );
});

test("SQL output carries reviewed native columns and complete after-load quality checks", () => {
  const contract = {
    schema: [
      {
        name: "report",
        properties: [
          {
            name: "id",
            physicalType: "BIGINT",
            primaryKey: true,
            required: true,
          },
        ],
        quality: [
          {
            id: "nonempty",
            metric: "rowCount",
            mustBeGreaterThan: 0,
            severity: "error",
          },
        ],
      },
    ],
  };
  const node = {
    id: "report/transform",
    flow: "report",
    step: "transform",
    columns: [{ name: "id", type: "BIGINT", key: true, required: true }],
    sql: "SELECT id FROM orders;",
    with: { sources: { orders: {} }, targetTable: "retail.silver.report" },
  };
  const plan = {
    project: "retail",
    environment: "dev",
    bindings: {},
    flows: [
      {
        id: "report",
        publishes: {
          report: { from: "steps.transform.outputs.result", contract },
        },
      },
    ],
  };
  const result = sqlTransformation(plan, node);
  assert.equal(result.columns, "`id` BIGINT");
  assert.equal(result.query, "SELECT id FROM orders");
  assert.ok(
    result.qualityNode.with.quality.some(
      (rule) => rule.metric === "duplicateValues",
    ),
  );
  assert.ok(
    result.qualityNode.with.quality.some((rule) => rule.id === "nonempty"),
  );
  const native = qualityNotebook(plan, [result.qualityNode])
    .cells.at(-1)
    .source.join("");
  assert.match(native, /DESCRIBE HISTORY/);
  assert.match(native, /versionAsOf/);
  node.columns = [];
  assert.throws(() => sqlTransformation(plan, node), /typed/);
  plan.flows[0].publishes = {};
  assert.throws(() => sqlTransformation(plan, node), /consistent/);
});
