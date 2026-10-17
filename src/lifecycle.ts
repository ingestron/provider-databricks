import { authorFileHandoff } from "./file-handoff.mjs";
import { bindPlatform } from "./platform-binding.mjs";
import { scheduledJobNames } from "./schedules.mjs";
import { publishedContract as outputContract } from "./column-mapping.mjs";
const structuredClone = (value: any) => JSON.parse(JSON.stringify(value));
import { stringify, parseDocument } from "yaml";
import { transformationSettings } from "./transformation-authoring.mjs";
import { flowAuthoring } from "./flow-authoring.mjs";
import {
  bundleConfiguration,
  validateReferences,
} from "./output-validation.mjs";
const check = (ok: any, ...messages: any[]) => {
  if (!ok) throw new Error(messages[messages.length - 1]);
};
/** Used by both discovery and creation so the picker cannot promise a different input. */
function transformationInputs(project: any, configuration: string) {
  const inputs: {
    dataset: string;
    contract: any;
    pipeline: any;
    metadata: string[];
  }[] = [];
  for (const flow of project.flows ?? []) {
    if ((flow.provider ?? project.defaults?.provider) !== configuration)
      continue;
    for (const [name, table] of Object.entries<any>(flow.tables ?? {})) {
      if (table.contract && flow.ingestion?.pipeline)
        inputs.push({
          dataset: `${project.id}.${flow.id}.${name}.current`,
          contract: table.contract,
          pipeline: flow.ingestion.pipeline,
          metadata: [
            table.ingestion?.operationColumn,
            ...(table.ingestion?.sequenceBy ?? []),
          ].filter(Boolean),
        });
    }
    for (const [name, output] of Object.entries<any>(flow.publishes ?? {})) {
      const step = flow.steps?.find(
        (s: any) =>
          output.from === `steps.${s.id}.outputs.result` ||
          output.from === `steps.${s.id}.outputs.table`,
      );
      if (output.contract && step?.with?.pipelineId)
        inputs.push({
          dataset: `${project.id}.${flow.id}.${name}`,
          contract: output.contract,
          pipeline: step.with.pipelineId,
          metadata: [],
        });
    }
  }
  return inputs;
}
export function author(request: any) {
  if (request.operation === "file_handoff")
    return {
      files: {
        "handoff.json": JSON.stringify(authorFileHandoff(request.options)),
      },
    };
  const { options } = request;
  if (request.operation === "flow_inputs")
    return {
      files: {
        "inputs.json": JSON.stringify({
          apiVersion: "ingestron.flow-inputs/v1",
          datasets: transformationInputs(
            request.project,
            request.configuration,
          ).map((input) => input.dataset),
        }),
      },
    };
  if (request.operation === "platform_binding")
    return { files: { "binding.yaml": stringify(bindPlatform(options)) } };
  if (request.operation === "initialise") {
    const environments = options.environments;
    const files: Record<string, string> = {
      "project.yaml": stringify({
        apiVersion: "ingestron.project/v1",
        id: options.id,
        providers: {
          packages: {
            native: {
              source: request.provider.source,
              version: request.provider.version,
            },
          },
          configurations: {
            default: {
              package: "native",
              binding: "platform",
              options: { deployment: "{{values.deployment}}" },
            },
          },
        },
        defaults: {
          provider: "default",
          naming: {
            table: "{{table.id}}",
            job: "{{env}}_{{project.id}}_{{flow.id}}",
          },
        },
        environments: Object.fromEntries(
          environments.map((e) => [
            e,
            { $resolve: `./environments/${e}.yaml` },
          ]),
        ),
        flows: [],
      }),
      "README.md": `# ${options.id}\n\nConfigure existing Databricks resources in environments/*.yaml. Add an ingestion flow using snapshot-with-history@v1, change-feed-with-history@v1 or append-only@v1, then add tables with reviewed contracts. Use flow add --kind transformation --input-dataset for domain SQL. The CLI generates native pipelines and their source attachments; it never contacts a platform. Production operational settings must be completed before generation.\n`,
      ".gitignore":
        "generated/\nbuild/\n.ingestron/\n*.local.yaml\n.env\n.env.*\n",
    };
    for (const environment of environments)
      files[`environments/${environment}.yaml`] = stringify({
        apiVersion: "ingestron.environment/v1",
        environment,
        values: {
          deployment: {
            mode: environment === "prod" ? "production" : "development",
          },
        },
        bindings: {
          platform: {
            kind: "databricks",
            host: { $env: "DATABRICKS_HOST" },
            catalog: "{{env}}_{{project.id}}",
          },
        },
      });
    return { files };
  }
  if (request.operation !== "flow")
    throw new Error("Unsupported authoring operation");
  const data = request.project,
    resolved = request.project,
    files: Record<string, string> = {},
    file = `flows/${options.id}/flow.yaml`;
  if (options.kind === "ingestion") {
    check(
      options.transformationSettings === undefined && !options.outputContract,
      "Output settings apply only to transformations",
    );
    check(
      !options.sourceKind || options.sourceKind === "adls",
      "This standard expects existing ADLS/volume deliveries",
    );
    const standard = options.standard ?? "append-only@v1";
    check(
      flowAuthoring.ingestionStandards.includes(standard),
      "STANDARD",
      "Choose a supported Databricks ingestion standard",
    );
    const snapshot = standard === "snapshot-with-history@v1",
      events = standard === "change-feed-with-history@v1";
    const binding = options.sourceBinding ?? "source",
      value = binding.replaceAll("-", "_") + "_root",
      sourceRoot = "{{values." + value + "}}/{{table.id}}";
    files[file] = stringify({
      apiVersion: "ingestron.flow/v1",
      kind: "ingestion",
      id: options.id,
      ingestion: {
        standard,
        pipeline: options.id,
        target: {
          schema: "current",
          ...(snapshot || events ? { historySchema: "history" } : {}),
        },
        source: {
          delivery: snapshot
            ? "complete-snapshot"
            : events
              ? "change-events"
              : "immutable-files",
          deletes: snapshot
            ? "missing-keys"
            : events
              ? "explicit-events"
              : "not-applicable",
          ...(snapshot ? { scope: "full-table" } : {}),
          ...(events ? { baseline: "included" } : {}),
        },
        retention: { sourceDeliveries: "not-retained" },
      },
      defaults: {
        source: {
          binding,
          format: options.format ?? "parquet",
          path: sourceRoot,
          ...(snapshot
            ? { deliveryIndex: sourceRoot + "/deliveries.json" }
            : {}),
        },
      },
      tables: {},
    });
    for (const env of request.environments) {
      const doc = parseDocument(env.text);
      if (!doc.hasIn(["bindings", binding]))
        doc.setIn(["bindings", binding], { kind: "adls" });
      if (!doc.hasIn(["values", value]))
        doc.setIn(["values", value], {
          $env: binding.toUpperCase().replaceAll("-", "_") + "_ROOT",
        });
      files[env.path] = doc.toString();
    }
  } else {
    check(
      options.inputDataset,
      "DATASET",
      "Supply --input-dataset from dataset list",
    );
    const input = transformationInputs(resolved, request.configuration).find(
      (input) => input.dataset === options.inputDataset,
    );
    check(
      input,
      "DATASET",
      "Choose a published current or transformation dataset on this configuration with a pipeline",
    );
    const contract = structuredClone(input!.contract),
      pipeline = input!.pipeline;
    contract.schema[0].properties = contract.schema[0].properties.filter(
      (c: any) => !input!.metadata.includes(c.physicalName ?? c.name),
    );
    const settings = transformationSettings(options.transformationSettings);
    const authoredContract = outputContract(options.outputContract ?? contract);
    if (!options.outputContract) {
      authoredContract.id = `${data.id}.${options.id}.result`;
      authoredContract.name = options.id;
      authoredContract.version = "1.0.0";
      authoredContract.status = "draft";
      delete authoredContract.team;
      delete authoredContract.description;
    }
    check(
      !resolved.flows.some((flow: any) =>
        [
          ...Object.values<any>(flow.tables ?? {}),
          ...Object.values<any>(flow.publishes ?? {}),
        ].some((o: any) => o.contract?.id === authoredContract.id),
      ),
      "Choose a distinct output contract identity",
    );
    files[`flows/${options.id}/contracts/result.odcs.yaml`] =
      stringify(authoredContract);
    const python = options.language === "python";
    files[
      `flows/${options.id}/${python ? "python/transform.py" : "sql/transform.sql"}`
    ] = python
      ? 'def transform(inputs):\n    return inputs["source"]\n'
      : "SELECT * FROM source\n";
    files[file] = stringify({
      apiVersion: "ingestron.flow/v1",
      kind: "transformation",
      id: options.id,
      requires: {
        source: { dataset: options.inputDataset, select: { mode: "same-run" } },
      },
      steps: [
        {
          id: "transform",
          uses: "materialized-view@v1",
          with: {
            pipelineId: settings?.pipelineId ?? pipeline,
            targetTable: settings?.targetTable ?? {
              $env:
                options.id.toUpperCase().replaceAll("-", "_") + "_TARGET_TABLE",
            },
            sources: { source: { from: "requires.source" } },
            ...(python
              ? { pythonFile: "./python/transform.py", entrypoint: "transform" }
              : { sqlFile: "./sql/transform.sql" }),
          },
        },
      ],
      publishes: {
        result: {
          from: "steps.transform.outputs.result",
          contract: { $resolve: "./contracts/result.odcs.yaml" },
        },
      },
    });
  }
  const doc = parseDocument(files[file]);
  doc.set("provider", request.configuration);
  files[file] = doc.toString();
  return { files };
}
export function model(request: any) {
  const node = request.node,
    settings = node.with,
    datasets: any = {};
  if (!request.draft) {
    const binding = request.bindings[node.binding];
    check(
      binding?.kind === "databricks" &&
        typeof binding.host === "string" &&
        /^https:\/\/[^\s/]+\/?$/.test(binding.host),
      "Supply an HTTPS Databricks workspace host",
    );
    check(
      typeof binding.catalog === "string" &&
        /^[A-Za-z_]\w*$/.test(binding.catalog),
      "Supply an existing catalogue identifier",
    );
  }
  const binding = request.bindings[node.binding];
  const scope = (binding.host ?? "draft").toLowerCase().replace(/\/$/, "");
  const resources: any[] = [];
  const claim = (kind: string, name: string) =>
    resources.push({ scope, kind, name });
  if (node.uses.endsWith("-input@v1")) {
    check(
      typeof node.source?.from === "string" &&
        /^requires\.[\w-]+$/.test(node.source.from) &&
        Object.keys(node.source).length === 1,
      "Imported source must contain only requires.<alias>",
    );
    const input = request.inputs[node.source.from.slice(9)],
      location = input?.location;
    const publishing = node.uses === "snapshot-publish-input@v1";
    const immutable =
      settings.standard === "append-only@v1" &&
      location?.protocol === "ingestron.immutable-file-landing/v1";
    check(
      input?.handover === "files" &&
        location?.kind === "files" &&
        (immutable
          ? ["json", "csv"].includes(location.format)
          : location.format === "parquet"),
      "Requires an explicit Parquet file handover",
    );
    check(
      location.protocol ===
        (immutable
          ? "ingestron.immutable-file-landing/v1"
          : publishing
            ? "ingestron.snapshot-landing/v1"
            : "ingestron.snapshot-publication/v1") &&
        location.completion ===
          (publishing || immutable
            ? "requires-successful-adf-run"
            : "requires-publication-receipt"),
      "Incompatible snapshot completion protocol",
    );
    check(
      JSON.stringify(input.contract.schema) ===
        JSON.stringify(node.contract.schema) &&
        input.contract.version === node.contract.version,
      "Imported snapshot contract differs from reviewed input contract",
    );
    node.source = {
      binding: location.binding,
      path: location.name,
      format: location.format,
      ...(immutable ? { reader: location.reader } : {}),
      ...(!publishing && !immutable
        ? {
            deliveryIndex: location.deliveryIndex,
            // The published index names the upstream dataset; check that name.
            ...(typeof location.dataset === "string"
              ? { dataset: location.dataset }
              : {}),
          }
        : {}),
    };
    if (publishing) settings.dataset = location.dataset;
  }
  if (node.uses.startsWith("snapshot-publish")) {
    check(
      !request.flowContext?.schedule,
      "Publication scheduling requires its own bounded job preparation",
    );
    const root = node.runtime.options.publication?.workspaceRoot;
    if (root) claim("workspace-root", root);
    claim("job", `${request.project}_${node.flow}_${node.table}`);
    datasets.completed = {
      contract: node.contract,
      location: {
        kind: "files",
        binding: node.source.binding,
        name: node.source.path,
        format: "parquet",
        protocol: "ingestron.snapshot-publication/v1",
        completion: "requires-publication-receipt",
        dataset:
          settings.dataset ?? `${request.project}.${node.flow}.${node.table}`,
        deliveryIndex: node.source.path + "/_ingestron/deliveries.json",
      },
    };
    return {
      with: settings,
      source: node.source,
      datasets,
      targets: [],
      resources,
    };
  }
  if (request.flowContext?.schedule) {
    check(
      !node.uses.startsWith("snapshot-publish"),
      "Publication scheduling requires its own bounded job preparation",
    );
    for (const job of scheduledJobNames(
      request.project,
      request.environment,
      node.flow,
      request.flowContext.schedule,
    ))
      claim("job", job.name);
  }
  const deployment = node.runtime.options.deployment ?? {},
    alias = settings.pipeline ?? settings.pipelineId;
  claim(
    "workspace-root",
    deployment.rootPath ??
      `/Workspace/Users/\${workspace.current_user.userName}/.bundle/${request.project}/${request.environment}`,
  );
  const pipeline = deployment.pipelines?.[alias] ?? {};
  if (pipeline.ownership !== "external") {
    claim(
      "pipeline",
      pipeline.name ?? `${request.environment}_${request.project}_${alias}`,
    );
    if (pipeline.existingId) claim("pipeline-id", pipeline.existingId);
    if (pipeline.eventLog)
      claim(
        "relation",
        [
          pipeline.eventLog.catalog,
          pipeline.eventLog.schema,
          pipeline.eventLog.name,
        ]
          .join(".")
          .toLowerCase(),
      );
  }
  if (node.uses === "materialized-view@v1") {
    claim("relation", settings.targetTable.toLowerCase());
    for (const [name, output] of Object.entries(request.outputs ?? {}) as [
      string,
      any,
    ][])
      datasets[name] = {
        contract: output.contract,
        location: {
          kind: "relation",
          binding: node.binding,
          name: settings.targetTable,
        },
      };
    return { with: settings, datasets, targets: [], resources };
  }
  check(
    ["lakeflow-ingest@v1", "lakeflow-ingest-input@v1"].includes(node.uses),
    "Unknown model activity",
  );
  const schema = settings.target?.schema ?? settings.schema ?? "prepared",
    tableName = request.tableName;
  check(
    /^[A-Za-z_]\w*$/.test(schema) && /^[A-Za-z_]\w*$/.test(tableName),
    `Invalid target name ${schema}.${tableName}`,
  );
  node.with.schema = schema;
  node.with.table = tableName;
  const publishedContract = structuredClone(node.contract);
  if (settings.operationColumn)
    publishedContract.schema[0].properties =
      publishedContract.schema[0].properties.filter(
        (c: any) =>
          (c.physicalName ?? c.name) !== settings.operationColumn &&
          !settings.sequenceBy?.includes(c.physicalName ?? c.name),
      );
  // The source mapping has already been applied by native ingestion.
  for (const property of publishedContract.schema[0].properties)
    delete property.physicalName;
  if (settings.target.historySchema) {
    const history = structuredClone(publishedContract);
    const sequence = settings.sequenceBy?.[0];
    const metadata = sequence
      ? node.contract.schema[0].properties.find(
          (c: any) => (c.physicalName ?? c.name) === sequence,
        )
      : { logicalType: "integer", physicalType: "BIGINT" };
    history.schema[0].properties.forEach((c: any) => {
      delete c.primaryKey;
      delete c.primaryKeyPosition;
    });
    history.schema[0].properties.push(
      ...["__START_AT", "__END_AT"].map((name) => ({
        name,
        logicalType: metadata.logicalType,
        physicalType: metadata.physicalType,
        required: name === "__START_AT",
      })),
    );
    datasets.history = {
      producer: node.id,
      port: "history",
      contract: history,
      location: {
        kind: "relation",
        name: `${binding.catalog}.${settings.target.historySchema}.${tableName}`,
        binding: node.binding,
        schema: settings.target.historySchema,
        table: tableName,
      },
    };
  }
  datasets.current = {
    producer: node.id,
    contract: publishedContract,
    location: {
      kind: "relation",
      name: `${binding.catalog}.${schema}.${tableName}`,
      binding: node.binding,
      schema,
      table: tableName,
    },
  };
  claim("relation", `${binding.catalog}.${schema}.${tableName}`.toLowerCase());
  if (settings.target.historySchema)
    claim(
      "relation",
      `${binding.catalog}.${settings.target.historySchema}.${tableName}`.toLowerCase(),
    );
  return {
    with: node.with,
    source: node.source,
    resources,
    datasets,
    targets: [
      `${schema}.${tableName}`,
      ...(settings.target.historySchema
        ? [`${settings.target.historySchema}.${tableName}`]
        : []),
    ],
  };
}
export function validateOutput(request: any) {
  validateReferences(request.files);
  return {
    documents: Object.keys(request.files)
      .filter(
        (n) =>
          /(^|\/)databricks(?:-[^/]+)?\.ya?ml$/.test(n) &&
          !n.includes(".github/"),
      )
      .map((name) => ({
        schema: "bundle",
        value: bundleConfiguration(request.files, name),
      })),
  };
}
