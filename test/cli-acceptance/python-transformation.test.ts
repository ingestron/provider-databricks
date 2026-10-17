import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { parse } from "yaml";
import {
  fixture,
  addFlow,
  apply,
  planProject,
  renderProject,
  execute,
} from "../support/project.ts";

test("installed SDK and CLI author the same reviewed Python flow, then render native code and committed-table quality checks without executing the function", (t) => {
  const f = fixture(t);
  f.put("environments/dev.yaml", f.project.environments.dev);
  f.project.environments.dev = { $resolve: "./environments/dev.yaml" };
  f.put("project.yaml", f.project);
  const options = {
    id: "report",
    kind: "transformation" as const,
    inputDataset: "retail.source.orders.current",
    language: "python" as const,
  };
  const proposal = addFlow(f.root, options);
  const cli = resolve(
    process.env.INGESTRON_ACCEPTANCE_CLI_PACKAGE!,
    "build/cli/cli/index.js",
  );
  const preview = JSON.parse(
    execFileSync(
      process.execPath,
      [
        cli,
        "--project",
        f.root,
        "--json",
        "--dry-run",
        "flow",
        "add",
        "report",
        "--kind",
        "transformation",
        "--input-dataset",
        options.inputDataset,
        "--language",
        "python",
      ],
      { encoding: "utf8" },
    ),
  );
  assert.equal(preview.ok, true, JSON.stringify(preview.diagnostics));
  assert.deepEqual(preview.result, proposal);
  assert.equal(existsSync(resolve(f.root, "flows/report/flow.yaml")), false);
  apply(f.root, proposal);
  const pythonFile = "flows/report/python/transform.py";
  const original = readFileSync(resolve(f.root, pythonFile), "utf8");
  assert.match(original, /def transform\(inputs\)/);
  const flow = parse(
    readFileSync(resolve(f.root, "flows/report/flow.yaml"), "utf8"),
  );
  flow.steps[0].with.targetTable = "dev_retail.silver.report";
  flow.steps[0].with.pipelineId = "reporting";
  f.put("flows/report/flow.yaml", flow);
  const sentinel = resolve(f.root, "EXECUTED");
  const source = `from pathlib import Path\ndef transform(inputs):\n    Path(${JSON.stringify(sentinel)}).write_text("not compiler input")\n    return inputs["source"]\n`;
  f.put(pythonFile, source);
  const plan = planProject(f.root);
  const node = plan.nodes.find((node: any) => node.flow === "report")!;
  assert.ok(node.needs.includes("source/orders/ingest_orders"));
  const files = renderProject(f.root, plan);
  assert.ok(files["pipelines/report/transform.py"].includes(source));
  const quality = JSON.parse(files["operations/quality_reporting.ipynb"])
    .cells.at(-1)
    .source.join("");
  assert.match(quality, /duplicateValues/);
  assert.match(quality, /versionAsOf/);
  const job = parse(files["resources/ingestion.job.yml"]);
  assert.ok(
    Object.values<any>(job.resources.jobs)[0].tasks.some(
      (task: any) => task.task_key === "quality_reporting",
    ),
  );
  assert.equal(existsSync(sentinel), false);
  f.put(pythonFile, source + "# concurrent edit\n");
  assert.throws(() => renderProject(f.root, plan), /changed|stale/i);
  assert.equal(existsSync(sentinel), false);
  f.put(pythonFile, "def transform(inputs)\n    return inputs\n");
  assert.throws(
    () => renderProject(f.root, planProject(f.root)),
    /Python syntax/,
  );
  assert.equal(existsSync(sentinel), false);
});

test("installed authoring refuses a language on ingestion before writing a definition", (t) => {
  const f = fixture(t);
  const result = execute(
    { root: f.root, environment: "dev", allowWrite: true },
    "flow_create",
    { id: "bad", kind: "ingestion", language: "python" },
  );
  assert.equal(result.ok, false);
  assert.equal(existsSync(resolve(f.root, "flows/bad/flow.yaml")), false);
});

test("installed creation supplies native output settings and a reviewed output contract in one proposal, with stale and ownership refusal", (t) => {
  const f = fixture(t);
  f.put("environments/dev.yaml", f.project.environments.dev);
  f.project.environments.dev = { $resolve: "./environments/dev.yaml" };
  f.put("project.yaml", f.project);
  const output = parse(
    readFileSync(resolve(f.root, "flows/source/contracts/orders.yaml"), "utf8"),
  );
  output.id = "reviewed_report";
  f.put("contracts/reviewed.yaml", output);
  const ctx = { root: f.root, environment: "dev", allowWrite: true },
    args = {
      id: "guided",
      kind: "transformation",
      inputDataset: "retail.source.orders.current",
      language: "python",
      contract: "contracts/reviewed.yaml",
      transformationSettings: {
        pipelineId: "reporting",
        targetTable: "{{env}}_retail.silver.guided",
      },
    };
  const proposal = execute(ctx, "flow_create", args);
  assert.equal(proposal.ok, true, JSON.stringify(proposal.diagnostics));
  assert.equal(existsSync(resolve(f.root, "flows/guided/flow.yaml")), false);
  assert.ok(proposal.result.inputs["contracts/reviewed.yaml"]);
  f.put("contracts/reviewed.yaml", { ...output, version: "2.0.0" });
  assert.equal(execute(ctx, "apply", { proposal: proposal.result }).ok, false);
  f.put("contracts/reviewed.yaml", output);
  apply(f.root, proposal.result);
  const flow = parse(
    readFileSync(resolve(f.root, "flows/guided/flow.yaml"), "utf8"),
  );
  assert.equal(flow.steps[0].with.pipelineId, "reporting");
  assert.equal(flow.steps[0].with.targetTable, "{{env}}_retail.silver.guided");
  assert.equal(
    parse(
      readFileSync(
        resolve(f.root, "flows/guided/contracts/result.odcs.yaml"),
        "utf8",
      ),
    ).id,
    output.id,
  );
  assert.equal(
    parse(readFileSync(resolve(f.root, "contracts/reviewed.yaml"), "utf8")).id,
    output.id,
  );
  const files = renderProject(f.root, planProject(f.root));
  assert.match(
    files["pipelines/guided/transform.py"],
    /dev_retail.silver.guided/,
  );
  for (const transformationSettings of [
    { pipelineId: "reporting" },
    { pipelineId: "../bad", targetTable: "a.b.c" },
    { pipelineId: "reporting", targetTable: "a.b.c", unknown: true },
  ])
    assert.equal(
      execute(ctx, "flow_create", {
        ...args,
        id: "invalid",
        transformationSettings,
      }).ok,
      false,
    );
  f.put("flows/orphan/python/transform.py", "# existing reviewed source\n");
  const orphan = execute(ctx, "flow_create", { ...args, id: "orphan" });
  assert.equal(orphan.ok, false);
  assert.match(JSON.stringify(orphan.diagnostics), /replace existing/);
  assert.equal(
    readFileSync(resolve(f.root, "flows/orphan/python/transform.py"), "utf8"),
    "# existing reviewed source\n",
  );
});
