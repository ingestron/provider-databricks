import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { render, validate, expand } from "../plugin/index.mjs";
export const fixture = () => ({
  project: "retail",
  environment: "dev",
  flows: [{ id: "source", requires: {} }],
  bindings: {
    dbx: {
      kind: "databricks",
      host: "https://example.invalid",
      catalog: "retail",
    },
  },
  nodes: [
    {
      id: "source/customers/publish_customers",
      flow: "source",
      table: "customers",
      uses: "snapshot-publish@v1",
      platform: "databricks",
      needs: [],
      binding: "dbx",
      runtime: {
        options: {
          publication: {
            workspaceRoot: "/Workspace/Shared/retail/publisher/dev",
            existingClusterId: "0123-example",
            runAsServicePrincipal: "example-principal",
            permissions: [{ group_name: "engineers", level: "CAN_MANAGE_RUN" }],
          },
        },
      },
      columns: [{ name: "id", type: "BIGINT", required: true, key: true }],
      source: {
        binding: "files",
        format: "parquet",
        path: "abfss://landing@retailstore.dfs.core.windows.net/retail/source/customers",
      },
      with: {
        standard: "snapshot-publication@v1",
        keys: ["id"],
        contractVersion: "1.0.0",
        credential: {
          secretScope: "storage",
          tenantIdKey: "tenant",
          clientIdKey: "client",
          clientSecretKey: "secret",
        },
      },
      implementation: {
        templates: [
          {
            output: "publication.py",
            content: readFileSync(
              "plugin/activities/snapshot-publish/publication.py",
              "utf8",
            ),
          },
        ],
      },
    },
  ],
});
test("publication notebook and recovery job are complete Python/native artefacts", () => {
  const assets = render(fixture());
  const code = assets["notebooks/retail_source_customers.py"].value;
  execFileSync(
    "python3",
    ["-c", "import ast,sys; ast.parse(sys.stdin.read())"],
    { input: code },
  );
  assert.match(code, /contract columns differ/);
  assert.match(code, /max_single_put_size=MAX_INDEX_BYTES/);
  assert.match(code, /retry_total=0/);
  const job =
    assets["resources/publication.yml"].value.resources.jobs
      .retail_source_customers;
  assert.equal(job.tasks[0].max_retries, 2);
  assert.equal(job.tasks[0].libraries.length, 2);
  assert.equal(
    job.tasks[0].notebook_task.base_parameters.protocol,
    "ingestron.snapshot-publication/v1",
  );
  const handover = assets["handover/retail_source_customers.json"].value;
  assert.match(handover.deliveryIndex, /_ingestron\/deliveries.json$/);
});
test("publication rejects mixed renderers, unsafe paths and unsupported operational settings", () => {
  for (const change of [
    (p) => (p.nodes[0].source.path += "?sas=secret"),
    (p) => (p.nodes[0].runtime.options.deployment = {}),
    (p) => (p.nodes[0].runtime.options.publication.permissions = []),
    (p) => (p.nodes[0].with.credential.secret = "literal"),
    (p) => (p.nodes[0].needs = ["upstream"]),
    (p) => (p.nodes[0].uses = "lakeflow-ingest@v1"),
  ]) {
    const p = fixture();
    change(p);
    assert.throws(() => validate(p));
  }
});
