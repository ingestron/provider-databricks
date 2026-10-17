import { fileURLToPath } from "node:url";
import { pathToFileURL } from "node:url";
import assert from "node:assert/strict";
import { readFileSync, realpathSync } from "node:fs";
const cliRoot = realpathSync(process.env.INGESTRON_ACCEPTANCE_CLI_PACKAGE!);
const cliManifest = JSON.parse(
  readFileSync(resolve(cliRoot, "package.json"), "utf8"),
);
assert.equal(cliManifest.name, "ingestron");
const coreRoot = realpathSync(resolve(cliRoot, "../@ingestron/core"));
const coreManifestPath = resolve(coreRoot, "package.json");
const coreManifest = JSON.parse(readFileSync(coreManifestPath, "utf8"));
assert.equal(coreManifest.name, "@ingestron/core");
assert.ok(
  [
    "0.17.8/0.12.14",
    "0.17.9/0.12.15",
    "0.17.19/0.12.45",
    "0.17.20/0.12.46",
    "0.17.20/0.12.47",
  ].includes(cliManifest.version + "/" + coreManifest.version),
);
export const { execute } = await import(
  pathToFileURL(resolve(coreRoot, "dist/sdk/index.js")).href
);
const adapter = await import(
  pathToFileURL(resolve(coreRoot, "dist/sdk/adapter.js")).href
);
export const {
  installPackage,
  planProject,
  renderProject,
  initialise,
  apply,
  addFlow,
  addTable,
} = adapter;
export const providerReference =
  process.env.INGESTRON_ACCEPTANCE_PROVIDER_REFERENCE!;
export const providerRoot = fileURLToPath(new URL("../../", import.meta.url));
export function installWorking(root: string, t: any) {
  mkdirSync(root, { recursive: true });
  const origin = process.env.INGESTRON_ACCEPTANCE_PROVIDER;
  assert.ok(
    origin,
    "Use the immutable offline provider snapshot from scripts/acceptance.mjs",
  );
  installPackage(root, providerReference, { fromGit: origin });
}
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve, dirname } from "node:path";
import { stringify } from "yaml";
const contract = (id: string) => ({
  apiVersion: "v3.1.0",
  kind: "DataContract",
  id,
  name: id,
  version: "1.0.0",
  status: "draft",
  schema: [
    {
      name: id,
      logicalType: "object",
      physicalType: "table",
      properties: [
        {
          name: "id",
          logicalType: "integer",
          physicalType: "BIGINT",
          required: true,
          primaryKey: true,
        },
        { name: "name", logicalType: "string", physicalType: "STRING" },
      ],
    },
  ],
});
export function fixture(t: any) {
  const root = mkdtempSync(resolve(tmpdir(), "ingestron-core-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const put = (name: string, value: any) => {
    const file = resolve(root, name);
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, typeof value === "string" ? value : stringify(value));
  };
  const flow: any = {
    apiVersion: "ingestron.flow/v1",
    kind: "ingestion",
    id: "source",
    ingestion: {
      standard: "snapshot-with-history@v1",
      pipeline: "source",
      target: { schema: "current", historySchema: "history" },
      source: {
        delivery: "complete-snapshot",
        scope: "full-table",
        deletes: "missing-keys",
      },
      retention: { sourceDeliveries: "not-retained" },
    },
    defaults: {
      source: {
        binding: "files",
        format: "parquet",
        path: "/Volumes/dev_retail/landing/source/{{table.id}}",
        deliveryIndex:
          "/Volumes/dev_retail/landing/source/{{table.id}}/deliveries.json",
      },
    },
    tables: {
      customers: {
        source: {},
        contract: { $resolve: "./contracts/customers.yaml" },
      },
      orders: {
        source: {},
        contract: { $resolve: "./contracts/orders.yaml" },
      },
    },
  };
  const project: any = {
    apiVersion: "ingestron.project/v1",
    id: "retail",
    packages: { dbx: providerReference },
    providers: {
      configurations: { engineering: { package: "dbx", binding: "lakehouse" } },
    },
    defaults: { provider: "engineering" },
    environments: {
      dev: {
        apiVersion: "ingestron.environment/v1",
        environment: "dev",
        values: { catalog: "{{env}}_{{project.id}}" },
        bindings: {
          lakehouse: {
            kind: "databricks",
            host: "https://example.invalid",
            catalog: "{{values.catalog}}",
          },
          files: { kind: "adls" },
        },
      },
    },
    flows: [{ $resolve: "./flows/source/flow.yaml" }],
  };
  installWorking(root, t);
  put("project.yaml", project);
  put("flows/source/flow.yaml", flow);
  put("flows/source/contracts/customers.yaml", contract("customers"));
  put("flows/source/contracts/orders.yaml", contract("orders"));
  return { root, put, flow, project };
}
