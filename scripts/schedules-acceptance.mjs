/** Installed offline scheduling acceptance. No source or cloud execution. */
import assert from "node:assert/strict";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  writeFileSync,
  readdirSync,
} from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
const required = (key) => {
  const value = process.env["INGESTRON_ACCEPTANCE_" + key];
  assert.ok(value, key);
  return resolve(value);
};
const cliRoot = realpathSync(required("CLI_PACKAGE")),
  coreRoot = realpathSync(resolve(cliRoot, "../@ingestron/core"));
assert.equal(
  JSON.parse(readFileSync(resolve(coreRoot, "package.json"))).version,
  "0.12.23",
);
assert.equal(
  JSON.parse(readFileSync(resolve(cliRoot, "package.json"))).version,
  "0.17.10",
);
const { execute } = await import(
  pathToFileURL(resolve(coreRoot, "dist/sdk/index.js")).href
);
const { installPackage, renderProject } = await import(
  pathToFileURL(resolve(coreRoot, "dist/sdk/adapter.js")).href
);
const require = createRequire(resolve(cliRoot, "package.json")),
  { parse, stringify } = require("yaml");
const { Client } = await import(
  pathToFileURL(require.resolve("@modelcontextprotocol/sdk/client/index.js"))
    .href
);
const { StdioClientTransport } = await import(
  pathToFileURL(require.resolve("@modelcontextprotocol/sdk/client/stdio.js"))
    .href
);
const hash = (p) => createHash("sha256").update(readFileSync(p)).digest("hex");
mkdirSync(required("OUTPUT"), { recursive: true });
const root = mkdtempSync(resolve(required("OUTPUT"), "schedules-"));
cpSync(required("SEED"), root, {
  recursive: true,
  filter: (p) =>
    !["build", ".git", ".ingestron", "packages.lock.yaml"].includes(
      p.split("/").at(-1),
    ),
});
const projectFile = resolve(root, "project.yaml"),
  flowFile = resolve(root, "flows/retail_bronze/flow.yaml");
const project = parse(readFileSync(projectFile, "utf8")),
  flow = parse(readFileSync(flowFile, "utf8"));
project.packages.target =
  "ingestron/provider-databricks/plugin/provider.yaml@3.10.0";
project.providers.configurations.workspace.options = {
  deployment: { mode: "development", job: { selectiveRefresh: "native-beta" } },
};
const manual = structuredClone(flow);
manual.id = "manual";
manual.ingestion.target = {
  schema: "manual_current",
  historySchema: "manual_history",
};
manual.tables = {
  customers: {
    contract: parse(
      readFileSync(resolve(root, "contracts/customers.odcs.yaml"), "utf8"),
    ),
  },
};
project.flows.push(manual);
writeFileSync(projectFile, stringify(project));
installPackage(root, project.packages.target, {
  fromGit: required("PROVIDER"),
});
const context = {
  root,
  environment: "dev",
  allowWrite: false,
  allowExecute: false,
  allowNetwork: false,
};
const cli = resolve(cliRoot, "build/cli/cli/index.js");
const command = (method, args, write = false) => {
  const p = spawnSync(
    process.execPath,
    [
      cli,
      "--project",
      root,
      "--env",
      "dev",
      "--json",
      "--no-input",
      "operation",
      method,
      ...(write ? ["--allow-write"] : []),
    ],
    {
      input: JSON.stringify(args),
      encoding: "utf8",
      timeout: 120000,
      maxBuffer: 8 * 1024 * 1024,
    },
  );
  assert.equal(p.error, undefined);
  assert.ok(p.stdout, p.stderr);
  return JSON.parse(p.stdout);
};
const client = new Client({
  name: "databricks-schedules-installed",
  version: "1.0.0",
});
const checks = [];
let result = "failed";
const success = (value) => {
  assert.equal(value.ok, true, JSON.stringify(value));
  return value.result;
};
const schedule = (recurrence) => ({
  apiVersion: "ingestron.schedule/v1",
  enabled: true,
  timeZone: "UTC",
  start: "2026-10-05T00:07:17Z",
  end: "2026-10-06T12:00:17Z",
  recurrence,
  overlap: "native",
  missed: "native",
});
const saved = [];
try {
  await client.connect(
    new StdioClientTransport({
      command: process.execPath,
      args: [cli, "--project", root, "advanced", "mcp", "serve"],
      stderr: "pipe",
    }),
  );
  assert.ok(
    (await client.listTools()).tools.some((t) => t.name === "ingestron_plan"),
  );
  for (const recurrence of [
    { kind: "interval", minutes: 15 },
    { kind: "daily", times: ["09:15", "17:45"] },
    {
      kind: "weekly",
      times: ["09:15", "17:45"],
      weekDays: ["Monday", "Friday"],
    },
    { kind: "monthly", times: ["09:15", "17:45"], monthDays: [1, 31] },
  ]) {
    flow.schedule = schedule(recurrence);
    writeFileSync(flowFile, stringify(flow));
    const sdk = execute(context, "plan", {}),
      plan = success(sdk);
    assert.deepEqual(command("plan", {}), JSON.parse(JSON.stringify(sdk)));
    const wire = await client.callTool({
      name: "ingestron_plan",
      arguments: {},
    });
    assert.deepEqual(
      JSON.parse(wire.content[0].text),
      JSON.parse(JSON.stringify(sdk)),
    );
    const files = renderProject(root, plan);
    const manifest = JSON.parse(files["databricks-schedules.json"]),
      jobs = parse(files["resources/schedules.job.yml"]).resources.jobs;
    assert.equal(
      manifest.schedules.length,
      recurrence.kind === "interval" ? 1 : 2,
    );
    for (const entry of manifest.schedules) {
      assert.deepEqual(entry.definition, flow.schedule);
      assert.deepEqual(entry.tables, ["customers", "orders", "products"]);
      assert.equal(entry.pipelines[0].launchStage, "native-beta");
      const job = jobs[entry.job],
        refresh = job.tasks.find((t) => t.pipeline_task);
      assert.equal(job.schedule.pause_status, "UNPAUSED");
      assert.equal(job.schedule.quartz_cron_expression, entry.cron);
      assert.equal(
        job.tasks.find((t) => t.task_key === "schedule_end").condition_task.op,
        "LESS_THAN",
      );
      assert.deepEqual(refresh.pipeline_task.refresh_selection, [
        "luna_retail.current.customers",
        "luna_retail.current.orders",
        "luna_retail.current.products",
        "luna_retail.history.customers",
        "luna_retail.history.orders",
        "luna_retail.history.products",
      ]);
      assert.ok(!JSON.stringify(job).includes("manual_current"));
      assert.ok(
        plan.nodes
          .filter((n) => n.flow === "retail_bronze")
          .every((n) =>
            n.resources.some((r) => r.kind === "job" && r.name === entry.name),
          ),
      );
    }
    assert.ok(
      files["resources/pipeline_retail.pipeline.yml"].includes("manual"),
    );
    const out = "build/" + recurrence.kind;
    success(command("build", { out, ownership: "team" }, true));
    const walk = (dir, prefix = "") =>
      readdirSync(dir, { withFileTypes: true }).flatMap((e) =>
        e.isDirectory()
          ? walk(resolve(dir, e.name), prefix + e.name + "/")
          : [prefix + e.name],
      );
    const names = walk(resolve(root, out));
    const manifestPath = names.find((p) =>
      p.endsWith("databricks-schedules.json"),
    );
    assert.ok(manifestPath, names.join(","));
    assert.deepEqual(
      JSON.parse(readFileSync(resolve(root, out, manifestPath), "utf8")),
      manifest,
    );
    saved.push({
      kind: recurrence.kind,
      manifest: resolve(root, out, manifestPath),
      sha256: hash(resolve(root, out, manifestPath)),
      jobs: Object.keys(jobs),
    });
  }
  checks.push(
    "All four recurrences match actual installed SDK, CLI and read-only MCP plans; CLI team builds preserve exact generated schedule manifests and job resource claims",
  );
  flow.schedule = schedule({ kind: "daily", times: ["09:15", "17:45"] });
  flow.schedule.enabled = false;
  writeFileSync(flowFile, stringify(flow));
  let files = renderProject(root, success(execute(context, "plan", {})));
  assert.ok(
    Object.values(
      parse(files["resources/schedules.job.yml"]).resources.jobs,
    ).every((j) => j.schedule.pause_status === "PAUSED"),
  );
  checks.push("Paused intent retained");
  const oldOptions = structuredClone(
    project.providers.configurations.workspace.options,
  );
  project.providers.configurations.workspace.options = {};
  writeFileSync(projectFile, stringify(project));
  const denied = execute(context, "plan", {});
  assert.equal(denied.ok, false);
  assert.match(JSON.stringify(denied.diagnostics), /Shared pipeline/);
  project.providers.configurations.workspace.options = oldOptions;
  writeFileSync(projectFile, stringify(project));
  for (const args of [
    { flow: "retail_bronze", table: "orders" },
    { flow: "retail_bronze", step: "ingest_orders" },
  ])
    assert.equal(execute(context, "plan", args).ok, false);
  flow.schedule = schedule({ kind: "interval", minutes: 7 });
  writeFileSync(flowFile, stringify(flow));
  assert.equal(execute(context, "plan", {}).ok, false);
  checks.push(
    "Unreviewed shared pipeline selection, partial builds and unrepresentable interval phases refuse before native generation",
  );
  flow.schedule = schedule({ kind: "daily", times: ["09:15", "17:45"] });
  writeFileSync(flowFile, stringify(flow));
  assert.equal(execute(context, "plan", {}).ok, true);
  checks.push(
    "Correcting configuration recovers without source access or cloud execution",
  );
  result = "passed";
} finally {
  await client.close();
  writeFileSync(
    required("PROOF"),
    JSON.stringify(
      {
        result,
        checkedAt: new Date().toISOString(),
        root,
        checks,
        saved,
        archives: JSON.parse(readFileSync(required("ARCHIVES"), "utf8")),
        provider: JSON.parse(readFileSync(required("PROVIDER_PROOF"), "utf8")),
        scope:
          "Actual immutable installed SDK/CLI/read-only MCP and generated native files; not browser, cloud deployment, trigger-clock or selective native execution qualification",
      },
      null,
      2,
    ) + "\n",
  );
}
console.log(JSON.stringify({ result, checks: checks.length, root }));
