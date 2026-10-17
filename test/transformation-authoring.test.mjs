import test from "node:test";
import assert from "node:assert/strict";
import { transformationSettings } from "../src/transformation-authoring.mjs";

test("reviewed native output settings accept logical pipeline aliases and bounded environment naming", () => {
  for (const targetTable of [
    "dev_retail.silver.orders",
    "{{env}}_retail.silver.orders",
    "{{values.catalogue}}.silver.orders",
  ]) {
    const input = { pipelineId: "reporting", targetTable };
    assert.deepEqual(transformationSettings(input), input);
  }
});
test("native output settings reject missing, extra, partial or executable destination text", () => {
  for (const value of [
    null,
    [],
    {},
    { pipelineId: "reporting", targetTable: "orders" },
    { pipelineId: "reporting", targetTable: "a.b.c; DROP TABLE source" },
    { pipelineId: "reporting", targetTable: "{{unknown}}.b.c" },
    { pipelineId: "../outside", targetTable: "a.b.c" },
    { pipelineId: "reporting", targetTable: "a.b.c", extra: true },
  ])
    assert.throws(() => transformationSettings(value), /pipeline alias/);
});
