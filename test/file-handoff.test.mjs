import { test } from "node:test";
import assert from "node:assert/strict";
import { author, model } from "../plugin/lifecycle.mjs";
test("native file consumer keeps exact contract and explicit ADF dependency", () => {
  const contract = {
    version: "1.0.0",
    schema: [{ name: "products", properties: [] }],
  };
  const result = JSON.parse(
    author({
      operation: "file_handoff",
      options: {
        id: "selected_delivery",
        project: "demo",
        origin: "selected",
        provider: "dbx",
        producerFlow: "selected_land",
        tables: [{ id: "products", format: "csv", contract }],
      },
    }).files["handoff.json"],
  );
  assert.deepEqual(result.flow.requires.products, {
    dataset: "demo.selected_land.products.snapshot",
    select: { mode: "same-run" },
    handover: "files",
  });
  assert.equal(result.flow.ingestion.standard, "append-only@v1");
  const request = {
    project: "demo",
    environment: "dev",
    bindings: {
      dbx: {
        kind: "databricks",
        host: "https://design.example.invalid",
        catalog: "design_catalog",
      },
      files: { kind: "adls" },
    },
    node: {
      id: "selected_delivery/products/ingest",
      uses: "lakeflow-ingest-input@v1",
      flow: "selected_delivery",
      table: "products",
      binding: "dbx",
      source: { from: "requires.products" },
      contract,
      with: { ...result.flow.ingestion, keys: [] },
      runtime: { options: {} },
    },
    inputs: {
      products: {
        handover: "files",
        contract,
        location: {
          kind: "files",
          binding: "files",
          name: "abfss://landing@designlanding.dfs.core.windows.net/selected/products",
          format: "csv",
          reader: { header: true, delimiter: "," },
          protocol: "ingestron.immutable-file-landing/v1",
          completion: "requires-successful-adf-run",
        },
      },
    },
  };
  const r = model(request);
  assert.equal(r.source.format, "csv");
  assert.deepEqual(r.source.reader, { header: true, delimiter: "," });
  request.node.source = { from: "requires.products" };
  request.inputs.products.location.completion = "unverified";
  assert.throws(() => model(request), /completion protocol/);
});
