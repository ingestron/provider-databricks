/** Installed compiler/native provider temporal workflows; no source/vendor access. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { execFileSync } from "node:child_process";
import {
  fixture,
  providerRoot,
  planProject,
  renderProject,
} from "../support/project.ts";
const temporalContract = {
  apiVersion: "v3.1.0",
  kind: "DataContract",
  id: "orders",
  name: "orders",
  version: "1.0.0",
  status: "draft",
  schema: [
    {
      name: "orders",
      logicalType: "object",
      physicalType: "table",
      properties: [
        {
          name: "id",
          logicalType: "integer",
          physicalType: "BIGINT",
          primaryKey: true,
          required: true,
        },
        {
          name: "day",
          logicalType: "date",
          physicalType: "DATE",
          required: true,
          quality: [
            {
              id: "day-valid",
              metric: "invalidValues",
              arguments: {
                validValues: ["2024-02-29", "2024-03-01", "2023-02-29"],
              },
              mustBe: 0,
              severity: "error",
            },
          ],
        },
        {
          name: "event_time",
          physicalName: "event",
          logicalType: "timestamp",
          physicalType: "TIMESTAMP",
          quality: [
            {
              id: "clock-valid",
              metric: "invalidValues",
              arguments: {
                validValues: [
                  "2024-02-29T01:02:03.123456",
                  "2024-02-29T23:59:59",
                  "2024-02-29T01:02:03.123456+13:00",
                ],
              },
              mustBe: 0,
              severity: "error",
            },
          ],
        },
        { name: "label", logicalType: "string", physicalType: "STRING" },
      ],
    },
  ],
};
function setup(t: any, format: string, standard = "snapshot-with-history@v1") {
  const f = fixture(t),
    snapshot = standard === "snapshot-with-history@v1";
  f.flow.ingestion = {
    standard,
    pipeline: "source",
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
  f.flow.defaults.source.format = format;
  if (!snapshot) delete f.flow.defaults.source.deliveryIndex;
  f.put("flows/source/flow.yaml", f.flow);
  f.put("flows/source/contracts/orders.yaml", temporalContract);
  return f;
}
test("installed native snapshots retain typed contracts, exact source/mapped clock semantics, footer preflight and typed calendar quality", (t) => {
  for (const format of ["json", "parquet"]) {
    const f = setup(t, format),
      plan = planProject(f.root),
      files = renderProject(f.root, plan);
    const source = files["pipelines/source/orders.ipynb"],
      code = JSON.parse(source).cells.at(-1).source.join("");
    assert.match(code, /mergeSchema/);
    assert.match(code, /temporal_invalid/);
    assert.match(code, /event_time/);
    assert.match(code, /DATE '2024-02-29'/);
    assert.doesNotMatch(code, /DATE '2023-02-29'/);
    if (format === "json") {
      assert.match(code, /`event` STRING/);
      assert.match(code, /AS TIMESTAMP_NTZ/);
      assert.match(code, /raise_error/);
    } else {
      assert.match(code, /`event` TIMESTAMP_NTZ/);
      assert.match(code, /timestamp_ntz/);
    }
    const clock = plan.nodes
      .find((node: any) => node.table === "orders")
      .contract.schema[0].properties.find(
        (column: any) => column.name === "event_time",
      );
    assert.equal(clock.physicalType, "TIMESTAMP");
    assert.equal(clock.physicalName, "event");
    const published = plan.datasets[
      "retail.source.orders.current"
    ].contract.schema[0].properties.find(
      (column: any) => column.name === "event_time",
    );
    assert.equal(published.physicalName, undefined);
    assert.equal(published.physicalType, "TIMESTAMP");
    if (process.env.INGESTRON_ACCEPTANCE_SPARK_PYTHON) {
      assert.ok(process.env.INGESTRON_ACCEPTANCE_SPARK_RUNTIME);
      assert.ok(process.env.INGESTRON_ACCEPTANCE_OUTPUT);
      const out = resolve(
        process.env.INGESTRON_ACCEPTANCE_OUTPUT,
        "temporal-spark-" + format,
      );
      mkdirSync(out, { recursive: true });
      const path = resolve(out, "generated.ipynb");
      writeFileSync(path, source);
      const result = execFileSync(
        process.env.INGESTRON_ACCEPTANCE_SPARK_PYTHON,
        [
          resolve(providerRoot, "test/support/temporal-spark-check.py"),
          path,
          process.env.INGESTRON_ACCEPTANCE_SPARK_RUNTIME,
          out,
          format,
        ],
        { encoding: "utf8", timeout: 300000, maxBuffer: 2 * 1024 * 1024 },
      );
      assert.match(result, /"result": "passed"/);
    }
  }
});
test("installed native JSON append emits guarded DATE/TIMESTAMP_NTZ projections while retaining logical quality and mapping", (t) => {
  const f = setup(t, "json", "append-only@v1"),
    files = renderProject(f.root, planProject(f.root)),
    sql = files["pipelines/source/orders.sql"];
  assert.match(sql, /`day` STRING/);
  assert.match(sql, /`event` STRING/);
  assert.match(sql, /AS DATE/);
  assert.match(sql, /AS TIMESTAMP_NTZ/);
  assert.match(sql, /END AS `event_time`/);
  assert.match(sql, /DATE '2024-02-29'/);
  assert.match(sql, /ON VIOLATION FAIL UPDATE/);
});
test("installed native streaming Parquet temporal scope is refused until each arriving footer has a verified policy", (t) => {
  const f = setup(t, "parquet", "append-only@v1"),
    original = readFileSync(resolve(f.root, "project.yaml"), "utf8");
  assert.throws(
    () => renderProject(f.root, planProject(f.root)),
    /verified per-file timezone and precision policy/,
  );
  assert.equal(readFileSync(resolve(f.root, "project.yaml"), "utf8"), original);
});
