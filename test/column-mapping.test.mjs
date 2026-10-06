import { test } from "node:test";
import assert from "node:assert/strict";
import {
  outputColumns,
  outputSetting,
  publishedContract,
} from "../src/column-mapping.mjs";
import { qualityChecks } from "../src/quality.mjs";

test("native projections preserve types and resolve simultaneous swapped names without sequential renames", () => {
  const columns = [
    { name: "left", target: "right", type: "BIGINT", key: true },
    { name: "right", target: "left", type: "STRING" },
  ];
  assert.deepEqual(outputColumns(columns), [
    { name: "right", type: "BIGINT", key: true },
    { name: "left", type: "STRING" },
  ]);
  assert.equal(outputSetting(columns, "left"), "right");
  assert.throws(
    () => outputSetting(columns, "missing"),
    /Unknown physical source column/,
  );
  assert.equal(columns[0].name, "left");
});
test("native projections reject ambiguous Spark names, metadata collisions and invalid output names", () => {
  for (const columns of [
    [{ name: "id" }, { name: "ID" }],
    [
      { name: "a", target: "key" },
      { name: "b", target: "KEY" },
    ],
    [{ name: "source", target: "__End_at" }],
    [{ name: "_RESCUED_DATA" }],
    [{ name: "id", target: "not-a-column" }],
  ])
    assert.throws(
      () => outputColumns(columns),
      /Invalid, reserved or duplicate/,
    );
});
test("library, SQL placeholders and native predicates describe the projected contract schema", () => {
  const contract = {
    schema: [
      {
        name: "orders",
        properties: [
          {
            name: "order_id",
            physicalName: "raw_id",
            primaryKey: true,
            quality: [
              {
                id: "present",
                metric: "nullValues",
                mustBe: 0,
                severity: "error",
              },
              {
                id: "scalar",
                type: "sql",
                query: "SELECT COUNT(${column}) FROM ${table}",
                mustBeGreaterThan: 0,
                severity: "error",
              },
            ],
          },
        ],
        quality: [
          {
            id: "predicate",
            type: "custom",
            engine: "databricks",
            implementation: "order_id > 0",
            severity: "error",
          },
        ],
      },
    ],
  };
  const columns = [
    {
      name: "raw_id",
      target: "order_id",
      type: "BIGINT",
      required: true,
      key: true,
    },
  ];
  const checks = qualityChecks(
    contract,
    columns,
    "snapshot-with-history@v1",
    "output",
  );
  assert.equal(checks.find((q) => q.id === "present").column, "order_id");
  assert.equal(
    checks.find((q) => q.id === "present").expression,
    "`order_id` IS NOT NULL",
  );
  assert.equal(
    checks.find((q) => q.id === "scalar").query,
    "SELECT COUNT(`order_id`) FROM ${table}",
  );
  assert.equal(
    checks.find((q) => q.id === "predicate").expression,
    "order_id > 0",
  );
  const published = publishedContract(contract);
  assert.equal(published.schema[0].properties[0].physicalName, undefined);
  assert.equal(contract.schema[0].properties[0].physicalName, "raw_id");
  assert.equal(published.schema[0].properties[0].quality[0].id, "present");
});
