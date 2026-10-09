import test from "node:test";
import assert from "node:assert/strict";
import { parse } from "yaml";
import { author } from "../plugin/lifecycle.mjs";
import { flowAuthoring } from "../src/flow-authoring.mjs";

test("declared creation standards match lifecycle and exclude render-only standards", () => {
  assert.equal(flowAuthoring.ingestionStandards.length, 3);
  for (const standard of [
    ...flowAuthoring.ingestionStandards,
    "unsupported@v1",
  ]) {
    const request = {
      operation: "flow",
      options: { id: "stock", kind: "ingestion", standard },
      project: {},
      environments: [],
    };
    if (standard === "unsupported@v1")
      assert.throws(() => author(request), /supported/);
    else
      assert.equal(
        parse(author(request).files["flows/stock/flow.yaml"]).ingestion
          .standard,
        standard,
      );
  }
});
test("new transformations have independent draft identities and do not inherit ownership", () => {
  const contract = {
    apiVersion: "v3.1.0",
    kind: "DataContract",
    id: "upstream",
    name: "Source",
    status: "active",
    version: "2.0.0",
    description: { purpose: "Source purpose" },
    team: { members: [{ username: "Source owner", role: "owner" }] },
    schema: [
      {
        name: "source",
        logicalType: "object",
        properties: [
          { name: "id", logicalType: "integer", physicalType: "BIGINT" },
        ],
      },
    ],
  };
  const request = {
    operation: "flow",
    configuration: "dbx",
    options: {
      id: "replenishment",
      kind: "transformation",
      inputDataset: "retail.stock.items.current",
      transformationSettings: {
        targetTable: "retail.gold.replenishment",
        pipelineId: "stock",
      },
    },
    project: {
      id: "retail",
      defaults: { provider: "dbx" },
      flows: [
        {
          id: "stock",
          ingestion: { pipeline: "stock" },
          tables: { items: { contract } },
        },
      ],
    },
  };
  const result = parse(
    author(request).files["flows/replenishment/contracts/result.odcs.yaml"],
  );
  assert.equal(result.id, "retail.replenishment.result");
  assert.equal(result.status, "draft");
  assert.equal(result.version, "1.0.0");
  assert.equal(result.team, undefined);
  assert.equal(result.description, undefined);
  assert.equal(contract.id, "upstream");
  assert.equal(contract.status, "active");
});
