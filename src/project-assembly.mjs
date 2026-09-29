import {
  bundleConfiguration,
  validateReferences,
} from "./output-validation.mjs";
import { parse, stringify } from "yaml";
const check = (v, m) => {
  if (!v) throw Error(m);
};
const identity = (i) =>
  [
    i.project,
    i.environment,
    i.configuration,
    ...(i.scope.partial ? [i.scope.id] : []),
  ].join("_");
const root = (i) => {
  const configured =
    i.options?.deployment?.rootPath ?? i.options?.publication?.workspaceRoot;
  return configured
    ? configured + (i.scope.partial ? "/_ingestron/" + i.scope.id : "")
    : `/Workspace/Shared/ingestron/${i.project}/${i.environment}/${i.configuration}/${i.scope.id}`;
};
export function assemble(input) {
  const name = identity(input),
    workspaceRoot = root(input);
  if (input.phase === "configure") {
    check(
      input.execution.mode === "databricks",
      "Databricks project requires Databricks execution",
    );
    return {
      execution: {
        ...input.execution,
        bundleDirectory: workspaceRoot + "/files/flows/" + input.flow,
        notebookPath:
          workspaceRoot + "/files/flows/" + input.flow + "/connector-notebook",
      },
    };
  }
  check(input.phase === "assemble", "Unknown project assembly phase");
  check(
    typeof input.binding?.host === "string" &&
      /^https:\/\/[^\s/]+$/.test(input.binding.host),
    "Configure an HTTPS workspace host",
  );
  check(
    !input.connections.length ||
      input.options?.deployment?.mode !== "production",
    "Project connector jobs do not yet implement production deployment policies; use a reviewed development target",
  );
  const artifacts = { ...input.nativeFiles };
  const put = (file, value) => {
    check(
      !Object.hasOwn(artifacts, file) || artifacts[file] === value,
      "Conflicting project asset: " + file,
    );
    artifacts[file] = value;
  };
  check(
    artifacts["databricks.yml"] ||
      !Object.keys(input.options?.deployment ?? {}).length,
    "Connector-only bundles do not yet support deployment policy options; remove them rather than silently ignoring them",
  );
  const bundle = artifacts["databricks.yml"]
    ? parse(artifacts["databricks.yml"])
    : { bundle: { databricks_cli_version: ">=0.294.0" } };
  bundle.bundle.name = name;
  bundle.include = [
    ...new Set([...(bundle.include ?? []), "resources/*.yml", "targets/*.yml"]),
  ];
  bundle.sync = {
    ...(bundle.sync ?? {}),
    include: [...new Set([...(bundle.sync?.include ?? []), "flows/**"])],
  };
  const targetFile = `targets/${input.environment}.yml`;
  const targetDocument = artifacts[targetFile]
    ? parse(artifacts[targetFile])
    : { targets: { [input.environment]: { default: true } } };
  const target = targetDocument.targets[input.environment];
  if (input.scope.partial) {
    check(
      !Object.values(input.options?.deployment?.pipelines ?? {}).some(
        (p) => p.ownership === "adopt",
      ),
      "Partial bundles cannot adopt existing pipelines",
    );
    target.mode = "development";
  }
  // Every provider configuration and partial selection owns a distinct deployment state.
  target.workspace = {
    ...(target.workspace ?? {}),
    host: input.binding.host,
    root_path: workspaceRoot,
    file_path: workspaceRoot + "/files",
    state_path: workspaceRoot + "/state",
  };
  const jobs = {};
  for (const c of input.connections) {
    const config = JSON.parse(c.artifacts["connector.json"]);
    check(
      config.projectLock.execution.bundleDirectory ===
        workspaceRoot + "/files/flows/" + c.flow,
      "Runtime path does not match bundle sync destination",
    );
    for (const [file, value] of Object.entries(c.artifacts))
      if (file !== "job.json") put(`flows/${c.flow}/${file}`, value);
    const job = JSON.parse(c.artifacts["job.json"]);
    job.name = name + "_" + c.flow;
    job.tasks[0].notebook_task.notebook_path =
      "../flows/" + c.flow + "/connector-notebook.py";
    delete job.tasks[0].notebook_task.source;
    jobs["connector_" + c.flow] = job;
  }
  // Detect collisions across the complete bundle rather than letting include order decide.
  const claims = new Set();
  for (const [file, text] of Object.entries(artifacts))
    if (/^resources\/.*\.ya?ml$/.test(file)) {
      const doc = parse(text);
      for (const [kind, members] of Object.entries(doc.resources ?? {}))
        for (const [key, resource] of Object.entries(members)) {
          const id = kind + "/" + key;
          check(!claims.has(id), "Duplicate native bundle resource: " + id);
          claims.add(id);
          if (input.scope.partial && kind === "jobs" && resource.schedule)
            resource.schedule.pause_status = "PAUSED";
        }
      artifacts[file] = stringify(doc);
    }
  for (const key of Object.keys(jobs))
    check(
      !claims.has("jobs/" + key),
      "Connector job conflicts with native resource",
    );
  if (Object.keys(jobs).length)
    artifacts["resources/connectors.yml"] = stringify({ resources: { jobs } });
  artifacts["databricks.yml"] = stringify(bundle);
  artifacts[targetFile] = stringify(targetDocument);
  artifacts["PROJECT.md"] =
    "# Databricks project bundle\n\nThis is one Asset Bundle for this configured target. Native resources and connector jobs share its deployment configuration. Connector runtimes require preinstalled locked Python environments and approved review.json files beside their connector.json before deployment. The bundle syncs flows/** to workspace.file_path; generated notebooks use that exact locked path. Supply a stable run_id when starting a connector job.\n\nEach partial selection has a separate bundle name and workspace state directory. Never bind a partial bundle to resources owned by a complete bundle. Native schedules in partial bundles are paused. Full bundle deletion/lifecycle changes still require reviewed native deployment; generation never deploys or runs jobs. Existing workspace, compute, storage and secret scopes remain customer-owned.\n";
  bundleConfiguration(artifacts, "databricks.yml");
  validateReferences(artifacts);
  markGenerated(artifacts, input);
  return {
    apiVersion: "ingestron.project-package/v1",
    artifacts,
    details: {
      entryPoint: "databricks.yml",
      bundle: name,
      workspaceRoot,
      omissionMeansDeletion: false,
      partialDeploymentIsolated: input.scope.partial,
      connectorJobs: Object.keys(jobs),
      nativeResources: [...claims],
    },
  };
}

// Mark provider-native text files as generated. Connector runtimes under flows/
// are hash-locked and JSON cannot hold comments; the manifest covers both.
export function markGenerated(artifacts, input) {
  const note = `Generated by Ingestron for project ${input.project}, target ${input.configuration}. Do not edit; change the project and rebuild. See ingestron-manifest.json.`;
  for (const [file, text] of Object.entries(artifacts)) {
    if (file.startsWith("flows/") || typeof text !== "string") continue;
    const prefix = /\.ya?ml$|\.py$/.test(file)
      ? "# "
      : /\.sql$/.test(file)
        ? "-- "
        : undefined;
    if (!prefix) continue;
    const notebook = "# Databricks notebook source\n";
    artifacts[file] = text.startsWith(notebook)
      ? notebook + prefix + note + "\n" + text.slice(notebook.length)
      : prefix + note + "\n" + text;
  }
  return artifacts;
}
