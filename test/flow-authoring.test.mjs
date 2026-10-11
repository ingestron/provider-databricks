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

test("input inspection and creation agree on configuration and pipeline boundaries", () => {
  const contract = {
    id: "input",
    schema: [
      {
        properties: [
          { name: "id", logicalType: "integer", physicalType: "BIGINT" },
        ],
      },
    ],
  };
  const project = {
    id: "retail",
    defaults: { provider: "dbx" },
    flows: [
      {
        id: "landing",
        provider: "adf",
        ingestion: { pipeline: "not-a-dbx-pipeline" },
        tables: { orders: { contract } },
      },
      { id: "unbound", tables: { orders: { contract } } },
      {
        id: "source",
        ingestion: { pipeline: "bronze" },
        tables: { orders: { contract } },
      },
      {
        id: "silver",
        steps: [{ id: "view", with: { pipelineId: "silver" } }],
        publishes: { result: { from: "steps.view.outputs.result", contract } },
      },
      {
        id: "broken",
        publishes: {
          result: { from: "steps.missing.outputs.result", contract },
        },
      },
    ],
  };
  const before = JSON.stringify(project);
  const request = { configuration: "dbx", project };
  const inspected = JSON.parse(
    author({ ...request, operation: "flow_inputs" }).files["inputs.json"],
  );
  assert.deepEqual(inspected, {
    apiVersion: "ingestron.flow-inputs/v1",
    datasets: ["retail.source.orders.current", "retail.silver.result"],
  });
  for (const inputDataset of [
    ...inspected.datasets,
    "retail.landing.orders.current",
    "retail.unbound.orders.current",
    "retail.broken.result",
  ]) {
    const create = () =>
      author({
        ...request,
        operation: "flow",
        options: {
          id: "result",
          kind: "transformation",
          inputDataset,
          transformationSettings: {
            pipelineId: "gold",
            targetTable: "retail.gold.result",
          },
        },
      });
    if (inspected.datasets.includes(inputDataset))
      assert.ok(create().files["flows/result/flow.yaml"]);
    else assert.throws(create, /configuration with a pipeline/);
  }
  assert.deepEqual(
    JSON.parse(
      author({ ...request, configuration: "other", operation: "flow_inputs" })
        .files["inputs.json"],
    ).datasets,
    [],
  );
  assert.equal(JSON.stringify(project), before);
});
