import { cpSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { installPackage } from "../../../cli/src/core/packages.js";
export const providerRoot = fileURLToPath(new URL("../../", import.meta.url));
export function installWorking(root: string, t: any) {
  mkdirSync(root, { recursive: true });
  const origin = mkdtempSync(resolve(tmpdir(), "ingestron-working-provider-"));
  t.after(() => rmSync(origin, { recursive: true, force: true }));
  cpSync(resolve(providerRoot, "plugin"), resolve(origin, "plugin"), {
    recursive: true,
    filter: (p) => !p.includes("__pycache__"),
  });
  const git = (...args: string[]) =>
    execFileSync("git", args, { cwd: origin, stdio: "pipe" });
  git("init", "--quiet");
  git("add", ".");
  git(
    "-c",
    "user.name=Fixture",
    "-c",
    "user.email=fixture@example.invalid",
    "commit",
    "-qm",
    "fixture",
  );
  git("tag", "2.2.0");
  installPackage(root, "databricks@2.2.0", { fromGit: origin });
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
    providers: {
      packages: { dbx: { source: "databricks", version: "2.2.0" } },
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
