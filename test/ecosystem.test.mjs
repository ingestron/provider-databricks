import { test } from "node:test";
import assert from "node:assert/strict";
import { model } from "../plugin/lifecycle.mjs";
const request = () => ({
  project: "retail",
  environment: "dev",
  bindings: {
    engine: {
      kind: "databricks",
      host: "https://example.invalid",
      catalog: "retail",
    },
    files: { kind: "adls" },
  },
  node: {
    id: "publish/customers/publish",
    uses: "snapshot-publish-input@v1",
    binding: "engine",
    flow: "publish",
    table: "customers",
    source: { from: "requires.snapshot" },
    contract: { version: "1.0.0", schema: [] },
    with: {},
    runtime: {
      options: {
        publication: { workspaceRoot: "/Workspace/Shared/publication" },
      },
    },
  },
  inputs: {
    snapshot: {
      handover: "files",
      contract: { version: "1.0.0", schema: [] },
      location: {
        kind: "files",
        binding: "files",
        name: "abfss://landing@sample.dfs.core.windows.net/retail/source/customers",
        format: "parquet",
        protocol: "ingestron.snapshot-landing/v1",
        completion: "requires-successful-adf-run",
        dataset: "retail.source.customers",
      },
    },
  },
});
test("publisher preserves source identity and derives the completed index", () => {
  const result = model(request());
  assert.equal(result.with.dataset, "retail.source.customers");
  assert.equal(
    result.datasets.completed.location.deliveryIndex,
    result.source.path + "/_ingestron/deliveries.json",
  );
  assert.equal(
    result.datasets.completed.location.completion,
    "requires-publication-receipt",
  );
});
test("input protocols, reviewed contract versions and ambiguous source overrides fail closed", () => {
  let r = request();
  r.inputs.snapshot.location.protocol = "ingestron.snapshot-publication/v1";
  assert.throws(() => model(r), /completion protocol/);
  r = request();
  r.inputs.snapshot.contract.version = "2.0.0";
  assert.throws(() => model(r), /contract differs/);
  r = request();
  r.node.source.path = "override";
  assert.throws(() => model(r), /only requires/);
});
test("the history consumer checks the published dataset name, not its own flow's", () => {
  const r = request();
  const contract = {
    version: "1.0.0",
    schema: [
      {
        name: "customers",
        properties: [
          {
            name: "id",
            logicalType: "integer",
            physicalType: "BIGINT",
            primaryKey: true,
          },
        ],
      },
    ],
  };
  r.node = {
    ...r.node,
    id: "current/customers/ingest",
    uses: "lakeflow-ingest-input@v1",
    flow: "current",
    source: { from: "requires.completed" },
    contract,
    with: {
      standard: "snapshot-with-history@v1",
      pipeline: "source",
      target: { schema: "current", historySchema: "history" },
      keys: ["id"],
    },
    runtime: { options: { deployment: {} } },
  };
  const root =
    "abfss://landing@sample.dfs.core.windows.net/retail/source/customers";
  r.inputs = {
    completed: {
      handover: "files",
      contract,
      location: {
        kind: "files",
        binding: "files",
        name: root,
        format: "parquet",
        protocol: "ingestron.snapshot-publication/v1",
        completion: "requires-publication-receipt",
        dataset: "retail.source.customers",
        deliveryIndex: root + "/_ingestron/deliveries.json",
      },
    },
  };
  const result = model(r);
  assert.equal(result.source.dataset, "retail.source.customers");
  assert.equal(
    result.source.deliveryIndex,
    root + "/_ingestron/deliveries.json",
  );
});
