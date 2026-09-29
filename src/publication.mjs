const check = (ok, message) => {
  if (!ok) throw new Error(message);
};
const id = (value) =>
  typeof value === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
const strict = (value, keys, label) =>
  check(
    value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      Object.keys(value).every((k) => keys.includes(k)),
    `Unsupported ${label} settings`,
  );
export const publicationLibraries = [
  { pypi: { package: "azure-storage-blob==12.30.1" } },
  { pypi: { package: "azure-identity==1.25.3" } },
];
export function expandPublication({ flow, columns }) {
  check(
    flow.kind === "ingestion",
    "Snapshot publication requires an ingestion flow",
  );
  const config = flow.ingestion;
  strict(
    config,
    ["standard", "credential", "allowEmpty", "maximumDropPercent"],
    "publication",
  );
  check(
    !flow.steps && !Object.keys(flow.defaults.with ?? {}).length,
    "Publication owns the flow steps",
  );
  const steps = Object.entries(flow.tables ?? {}).map(([table, value]) => {
    check(
      !Object.keys(value.steps ?? {}).length,
      "Publication does not allow step overrides",
    );
    strict(value.ingestion ?? {}, ["keys"], "table publication");
    const keys =
      value.ingestion?.keys ??
      columns[table].filter((c) => c.key).map((c) => c.name);
    check(
      keys.length &&
        keys.every((key) => columns[table].some((c) => c.name === key)),
      "Publication requires reviewed contract keys",
    );
    check(
      typeof value.contract.version === "string" && value.contract.version,
      "Publication requires a contract version",
    );
    return {
      id: `publish_${table}`,
      uses: "snapshot-publish@v1",
      select: [table],
      with: { ...config, keys, contractVersion: value.contract.version },
    };
  });
  return {
    steps,
    recovery: {
      standard: "snapshot-publication@v1",
      capture: "complete-snapshot",
      replaySource: "retained-immutable-deliveries",
      actualCompleteness: "unverified",
      detail:
        "Conditional Blob writes retain every ordered receipt; replay publication with the identical receipt after an uncertain response.",
      assumptions: [
        "Source versions start at one and are contiguous in capture order.",
        "Landing data remains immutable after copy; only the publication identity writes the index.",
        "Native Spark and Azure permissions are separately accepted.",
      ],
    },
  };
}
function source(n) {
  strict(n.source, ["binding", "path", "format"], "publication source");
  check(
    n.source.format === "parquet",
    "Publication verifies Parquet snapshots only",
  );
  const match =
    /^abfss:\/\/([a-z0-9][a-z0-9-]{1,61}[a-z0-9])@([a-z0-9]{3,24})\.dfs\.core\.windows\.net\/([A-Za-z0-9_-][A-Za-z0-9_./-]*)$/.exec(
      n.source.path,
    );
  check(
    match && match[3].split("/").every((p) => p && p !== "." && p !== ".."),
    "Publication needs a literal Azure public-cloud ABFS source root",
  );
  return { container: match[1], account: match[2], prefix: match[3] };
}
export function validatePublication(plan) {
  check(plan.nodes.length > 0, "Publication export requires nodes");
  const settings = plan.nodes[0].runtime.options;
  strict(settings, ["publication"], "provider");
  const deployment = settings.publication;
  strict(
    deployment,
    [
      "workspaceRoot",
      "existingClusterId",
      "runAsServicePrincipal",
      "permissions",
    ],
    "publication deployment",
  );
  check(
    typeof deployment.workspaceRoot === "string" &&
      /^\/Workspace\/[A-Za-z0-9_/-]+$/.test(deployment.workspaceRoot) &&
      !deployment.workspaceRoot.endsWith("/"),
    "Supply a stable workspaceRoot",
  );
  check(
    typeof deployment.existingClusterId === "string" &&
      /^[A-Za-z0-9-]+$/.test(deployment.existingClusterId),
    "Publication requires an existing authorised classic cluster",
  );
  check(
    typeof deployment.runAsServicePrincipal === "string" &&
      deployment.runAsServicePrincipal,
    "Publication requires an explicit run-as service principal",
  );
  check(
    Array.isArray(deployment.permissions) && deployment.permissions.length,
    "Publication requires explicit job permissions",
  );
  for (const permission of deployment.permissions) {
    strict(permission, ["group_name", "level"], "permission");
    check(
      typeof permission.group_name === "string" &&
        permission.group_name &&
        ["CAN_VIEW", "CAN_MANAGE_RUN", "CAN_MANAGE"].includes(permission.level),
      "Declare supported group permissions",
    );
  }
  const names = new Set();
  for (const n of plan.nodes) {
    check(
      n.platform === "databricks" &&
        n.uses === "snapshot-publish@v1" &&
        !n.needs.length,
      "Publication must use its own export without Lakeflow or cross-flow tasks",
    );
    check(
      JSON.stringify(n.runtime.options) === JSON.stringify(settings),
      "One deployment configuration per publication export",
    );
    check(
      Object.values(
        plan.flows.find((f) => f.id === n.flow)?.requires ?? {},
      ).every((r) =>
        plan.delivery?.imports.some(
          (i) =>
            i.dataset === r.dataset &&
            i.kind === "files" &&
            i.location.protocol === "ingestron.snapshot-landing/v1",
        ),
      ),
      "Publication cannot satisfy same-run inputs",
    );
    const name = `${plan.project}_${n.flow}_${n.table}`;
    check(
      /^[A-Za-z_][A-Za-z0-9_-]{0,100}$/.test(name) && !names.has(name),
      "Invalid publication resource name",
    );
    names.add(name);
    source(n);
    const w = n.with;
    strict(
      w,
      [
        "standard",
        "credential",
        "keys",
        "contractVersion",
        "allowEmpty",
        "maximumDropPercent",
        "dataset",
      ],
      "publication standard",
    );
    check(
      w.standard === "snapshot-publication@v1",
      "Unsupported publication standard",
    );
    strict(
      w.credential,
      ["secretScope", "tenantIdKey", "clientIdKey", "clientSecretKey"],
      "credential",
    );
    check(
      ["secretScope", "tenantIdKey", "clientIdKey", "clientSecretKey"].every(
        (k) =>
          typeof w.credential[k] === "string" &&
          /^[A-Za-z0-9_.-]+$/.test(w.credential[k]),
      ),
      "Use existing secret-scope key references; credentials are never literal values",
    );
    check(
      w.allowEmpty === undefined || typeof w.allowEmpty === "boolean",
      "allowEmpty must be boolean",
    );
    check(
      w.maximumDropPercent === undefined ||
        (typeof w.maximumDropPercent === "number" &&
          w.maximumDropPercent >= 0 &&
          w.maximumDropPercent <= 100),
      "Invalid row-drop threshold",
    );
    check(
      Array.isArray(w.keys) &&
        w.keys.length &&
        w.keys.every((key) => id(key) && n.columns.some((c) => c.name === key)),
      "Publication requires valid keys",
    );
    check(
      n.implementation?.templates.length === 1 &&
        n.implementation.templates[0].output === "publication.py",
      "Publication requires its versioned implementation template",
    );
  }
  check(
    new Set(plan.nodes.map((n) => n.binding)).size === 1,
    "One workspace binding per publication export",
  );
}
export function renderPublication(plan) {
  validatePublication(plan);
  const deployment = plan.nodes[0].runtime.options.publication;
  const assets = {},
    jobs = {};
  for (const n of plan.nodes) {
    const name = `${plan.project}_${n.flow}_${n.table}`,
      s = source(n),
      dataset = n.with.dataset ?? `${plan.project}.${n.flow}.${n.table}`;
    const config = {
      sourceRoot: n.source.path,
      dataset,
      columns: n.columns,
      keys: n.with.keys,
      contractVersion: n.with.contractVersion,
      allowEmpty: n.with.allowEmpty ?? false,
      maximumDropPercent: n.with.maximumDropPercent ?? null,
      credential: n.with.credential,
      ...s,
    };
    const code =
      "# Databricks notebook source\n# Generated by Ingestron: project-owned native snapshot publisher.\n" +
      n.implementation.templates[0].content +
      "\n\nCONFIG = json.loads(" +
      JSON.stringify(JSON.stringify(config)) +
      ")\n" +
      String.raw`
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient

for parameter in ("deliveryId", "version", "capturedAt", "expectedRowCount", "runId", "dataset", "sourceRoot", "contractVersion", "protocol", "initialiseIndex", "contractShape"):
    dbutils.widgets.text(parameter, "")
require(dbutils.widgets.get("protocol") == "ingestron.snapshot-publication/v1", "Publication protocol mismatch")
for key in ("dataset", "sourceRoot", "contractVersion"):
    require(dbutils.widgets.get(key) == CONFIG[key], "Publication notebook does not match the requested " + key)
require(json.loads(dbutils.widgets.get("contractShape")) == CONFIG["columns"], "Publication contract columns differ from the producer")
initialise_index = dbutils.widgets.get("initialiseIndex")
require(initialise_index in ("true", "false"), "initialiseIndex must be true or false")
run_id = dbutils.widgets.get("runId")
require(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id), "Supply the original ADF copy RunId")
receipt = {
    "id": dbutils.widgets.get("deliveryId"),
    "version": int(dbutils.widgets.get("version")),
    "capturedAt": dbutils.widgets.get("capturedAt"),
    "contractVersion": CONFIG["contractVersion"],
    "complete": True,
    "scope": "full-table",
    "rowCount": int(dbutils.widgets.get("expectedRowCount")),
    "path": CONFIG["sourceRoot"] + "/" + run_id,
}
validate_receipt(receipt, CONFIG["sourceRoot"])
references = CONFIG["credential"]
credential = ClientSecretCredential(
    tenant_id=dbutils.secrets.get(references["secretScope"], references["tenantIdKey"]),
    client_id=dbutils.secrets.get(references["secretScope"], references["clientIdKey"]),
    client_secret=dbutils.secrets.get(references["secretScope"], references["clientSecretKey"]),
)
blob = BlobClient(
    account_url="https://" + CONFIG["account"] + ".blob.core.windows.net",
    container_name=CONFIG["container"], blob_name=CONFIG["prefix"] + "/_ingestron/deliveries.json",
    credential=credential, max_single_put_size=MAX_INDEX_BYTES,
    connection_timeout=30, read_timeout=60, retry_total=0,
)
def verify_landed_snapshot():
    data = spark.read.format("parquet").load(receipt["path"]).cache()
    try:
        validate_rows(data, CONFIG["columns"], CONFIG["keys"], receipt["rowCount"], CONFIG["allowEmpty"])
    finally:
        data.unpersist()
try:
    outcome = publish_snapshot(AzureBlobIndex(blob), receipt, CONFIG["dataset"], CONFIG["sourceRoot"], verify_landed_snapshot, CONFIG["allowEmpty"], CONFIG["maximumDropPercent"], initialise=initialise_index == "true")
finally:
    blob.close()
    credential.close()
dbutils.notebook.exit(json.dumps(outcome))
`;
    assets[`notebooks/${name}.py`] = { format: "text", value: code };
    const parameters = [
      "deliveryId",
      "version",
      "capturedAt",
      "expectedRowCount",
      "runId",
      "initialiseIndex",
    ];
    const identity = {
      dataset,
      sourceRoot: n.source.path,
      contractVersion: n.with.contractVersion,
      protocol: "ingestron.snapshot-publication/v1",
      contractShape: JSON.stringify(n.columns),
    };
    jobs[name] = {
      name,
      run_as: { service_principal_name: deployment.runAsServicePrincipal },
      permissions: deployment.permissions,
      max_concurrent_runs: 1,
      parameters: parameters.map((name) => ({
        name,
        default: name === "initialiseIndex" ? "false" : "",
      })),
      tasks: [
        {
          task_key: "publish",
          existing_cluster_id: deployment.existingClusterId,
          notebook_task: {
            notebook_path: `../notebooks/${name}.py`,
            base_parameters: {
              ...identity,
              ...Object.fromEntries(
                parameters.map((p) => [p, `{{job.parameters.${p}}}`]),
              ),
            },
          },
          libraries: publicationLibraries,
          max_retries: 2,
          min_retry_interval_millis: 10000,
          timeout_seconds: 3600,
        },
      ],
    };
    assets[`handover/${name}.json`] = {
      format: "json",
      value: {
        apiVersion: "ingestron.snapshot-publication/v1",
        dataset,
        contractVersion: n.with.contractVersion,
        sourceRoot: n.source.path,
        deliveryIndex: n.source.path + "/_ingestron/deliveries.json",
        notebookSource: `notebooks/${name}.py`,
        parameters,
        libraries: publicationLibraries,
        sourceVersionPolicy: "contiguous-from-one",
        adfNotebookIdentity:
          "ADF linked-service identity must match the accepted publisher identity; job run_as does not apply to direct ADF notebook invocations",
      },
    };
  }
  assets["databricks.yml"] = {
    format: "yaml",
    value: {
      bundle: { name: plan.project + "-publication" },
      workspace: {
        host: plan.bindings[plan.nodes[0].binding].host,
        root_path: deployment.workspaceRoot,
      },
      include: ["resources/*.yml"],
    },
  };
  assets["resources/publication.yml"] = {
    format: "yaml",
    value: { resources: { jobs } },
  };
  assets["README.md"] = {
    format: "text",
    value:
      "# Snapshot publication\n\nNative Databricks publisher; no Data Flow or Ingestron runtime package. Deploy/import on authorised existing compute. Grant the chosen identity read access to immutable landing data and conditional-write access to its delivery index. Secret-scope references are credentials for the Blob SDK; Spark/Unity Catalog source access is separately configured. ADF invokes the deployed notebook using its linked-service identity, not the generated job run_as. Verify both identities before use.\n\nSupply source deliveryId/version/capturedAt, expectedRowCount and the original copy runId. Set initialiseIndex=true only for the first authorised publication; normal/recovery calls default to false. Prepare the _ingestron directory with write access only to that metadata directory; landing data remains read-only to the publication credential. Versions start at one and are contiguous in source capture order. Repeating identical metadata is idempotent; changing metadata for an existing ID/version/path fails. Do not rerun Copy to recover publication: rerun the publisher with the same receipt. Keep source files and all index entries immutable/retained. SDK writes use conditional ETags and a single block-blob PUT; readers see the previous or next complete index. Limits: 10,000 receipts/4 MiB per dataset; reaching a limit fails without pruning. Keep protected index backups and deny deletion; loss of the index requires restore, not a new baseline.\n\nNative cloud acceptance is outstanding. Test permissions, private network routing, concurrent publication, lost responses, restart, reader visibility and source immutability before production.\n",
  };
  return assets;
}
