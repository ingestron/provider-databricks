import { test } from "node:test";
import assert from "node:assert/strict";
import { prepare } from "../src/connectors.mjs";
const input = () => ({
  apiVersion: "ingestron.connection-request/v1",
  specificationSha256: "synthetic",
  runtimeContract: "ingestron.snapshot/python/v1",
  runtimeAssetSha256: "locked",
  connector: "fixture:example@1.0.0",
  sourceId: "fixture",
  tenantId: "demo",
  timeoutSeconds: 120,
  settings: { count: 3, seed: 42, parallelism: 1 },
  selection: {
    users: {
      name: "users",
      fields: { id: { type: "integer", nullable: false } },
    },
  },
  execution: {
    mode: "databricks",
    existingClusterId: "1234-demo",
    bundleDirectory: "/Workspace/Shared/ingestron/faker",
    notebookPath: "/Workspace/Shared/ingestron/faker-job",
    pythonExecutable: "/opt/ingestron/faker/bin/python",
    azure: {
      storageAccount: "syntheticstore",
      container: "landing",
      prefix: "faker",
      tenantId: "00000000-0000-0000-0000-000000000001",
      clientId: "00000000-0000-0000-0000-000000000002",
      clientSecretEnv: "INGESTRON_STORAGE_SECRET",
    },
    secrets: {
      INGESTRON_STORAGE_SECRET: { scope: "demo", key: "storage-secret" },
    },
  },
  runtimeAssets: {
    "settings.schema.json": JSON.stringify({
      type: "object",
      additionalProperties: true,
    }),
    "runtime.lock.json": JSON.stringify({
      connector: "example@1.0.0",
      files: {},
    }),
  },
});
test("existing cluster job retains stable retry identity and secrets remain references", () => {
  const r = prepare(input());
  const j = JSON.parse(r.artifacts["job.json"]);
  assert.equal(j.tasks[0].existing_cluster_id, "1234-demo");
  assert.equal(j.tasks[0].new_cluster, undefined);
  assert.equal(
    j.tasks[0].notebook_task.base_parameters.run_id,
    "{{job.parameters.run_id}}",
  );
  assert.equal(j.tasks[0].max_retries, 2);
  assert.equal(
    JSON.parse(r.artifacts["connector.json"]).projectLock.runtimeAssets,
    undefined,
  );
  assert.ok(
    JSON.parse(r.artifacts["runtime.lock.json"]).files["databricks_launch.py"],
  );
});
test("reject missing runtime, mismatched connector, unsafe paths and incomplete or extra secret mappings", () => {
  for (const mutate of [
    (i) => delete i.runtimeAssets,
    (i) => (i.runtimeContract = "unknown/v2"),
    (i) =>
      (i.runtimeAssets["runtime.lock.json"] =
        '{"connector":"other","files":{}}'),
    (i) => (i.execution.bundleDirectory = "/Workspace/../bad"),
    (i) => (i.execution.pythonExecutable = "/opt/python;bad"),
    (i) => delete i.execution.secrets.INGESTRON_STORAGE_SECRET,
    (i) => (i.execution.secrets.EXTRA = { scope: "a", key: "b" }),
    (i) => (i.execution.azure.clientSecretEnv = "PATH"),
  ]) {
    const i = input();
    mutate(i);
    assert.throws(() => prepare(i));
  }
});
