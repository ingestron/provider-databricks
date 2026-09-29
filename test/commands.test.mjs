import { test } from "node:test";
import assert from "node:assert/strict";
import { command } from "../src/commands.mjs";
const request = (name, input) =>
  command({
    apiVersion: "ingestron.provider-command-request/v1",
    command: name,
    context: { project: "retail", environment: "dev" },
    input,
  });
test("metadata import returns a review proposal and rejects lossy unknown types", () => {
  const input = {
    tables: [
      {
        name: "customers",
        columns: [{ name: "id", type: "bigint", nullable: false }],
      },
    ],
  };
  const result = request("discover import", input);
  assert.equal(result.applied, false);
  assert.equal(result.tables[0].columns[0].type, "BIGINT");
  input.tables[0].columns[0].type = "decimal(38,18)";
  assert.throws(
    () => request("discover import", input),
    /Unsupported source type/,
  );
  assert.throws(
    () => request("deploy apply", {}),
    /Unsupported provider command/,
  );
});
test("deployment inspection inventories supplied resources without deployment claims", () => {
  const result = request("deploy inspect", {
    artifact: { resources: { jobs: { publish: { name: "Publish" } } } },
  });
  assert.equal(result.resources.length, 1);
  assert.equal(result.deployed, false);
  assert.throws(() => request("deploy inspect", { artifact: {} }));
});
