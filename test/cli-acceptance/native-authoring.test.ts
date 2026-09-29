import { test } from "node:test";
import assert from "node:assert/strict";
import { resolve } from "node:path";
import { readFileSync, writeFileSync, copyFileSync } from "node:fs";
import { fixture, installWorking, providerRoot } from "../support/project.js";
import {
  initialise,
  apply,
  addFlow,
  addTable,
} from "../../../cli/src/core/authoring.js";
import { planProject } from "../../../cli/src/core/planner.js";
import { renderProject } from "../../../cli/src/core/generate.js";
import { parse, stringify } from "yaml";
test("semantic ingestion authoring needs no cluster or per-run staging configuration", (t) => {
  const f = fixture(t),
    root = resolve(f.root, "semantic");
  installWorking(root, t);
  apply(
    root,
    initialise(root, {
      id: "demo",
      provider: "databricks@2.2.0",
      environments: ["dev"],
    }),
  );
  apply(
    root,
    addFlow(root, {
      id: "source",
      kind: "ingestion",
      standard: "snapshot-with-history@v1",
    }),
  );
  copyFileSync(
    resolve(f.root, "flows/source/contracts/customers.yaml"),
    resolve(root, "customer.yaml"),
  );
  apply(
    root,
    addTable(root, {
      flow: "source",
      id: "customers",
      contract: "customer.yaml",
    }),
  );
  const envFile = resolve(root, "environments/dev.yaml");
  const env = parse(readFileSync(envFile, "utf8"));
  assert.equal(env.bindings.platform.existingClusterId, undefined);
  env.bindings.platform.host = "https://example.invalid";
  env.values.source_root = "/Volumes/raw/erp/deliveries";
  writeFileSync(envFile, stringify(env));
  const flowFile = resolve(root, "flows/source/flow.yaml");
  const flow = parse(readFileSync(flowFile, "utf8"));
  flow.ingestion.pipeline = "existing_pipeline";
  writeFileSync(flowFile, stringify(flow));
  const plan = planProject(root);
  assert.equal(plan.nodes.length, 1);
  apply(
    root,
    addFlow(root, {
      id: "reporting",
      kind: "transformation",
      inputDataset: "demo.source.customers.current",
    }),
  );
  const reportFile = resolve(root, "flows/reporting/flow.yaml");
  const report = parse(readFileSync(reportFile, "utf8"));
  report.steps[0].with.targetTable = "dev_demo.reporting.customers";
  writeFileSync(reportFile, stringify(report));
  const files = renderProject(root, planProject(root));
  assert.ok(files["pipelines/reporting/transform.sql"]);
});
