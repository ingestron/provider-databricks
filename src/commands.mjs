import { assemble } from "./project-assembly.mjs";
import { prepare } from "./connectors.mjs";
import { connectorContracts } from "./vendor/connectors/connector-contract-export.mjs";
const require = (condition, message) => {
  if (!condition) throw new Error(message);
};
const safeName = (value) =>
  typeof value === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
const types = {
  string: "STRING",
  varchar: "STRING",
  nvarchar: "STRING",
  bigint: "BIGINT",
  int: "INT",
  integer: "INT",
  smallint: "SMALLINT",
  boolean: "BOOLEAN",
  bit: "BOOLEAN",
  date: "DATE",
  timestamp: "TIMESTAMP",
  datetime2: "TIMESTAMP",
  double: "DOUBLE",
  binary: "BINARY",
};
function discovery(input, context) {
  require(Array.isArray(input.tables) &&
    input.tables.length > 0, "Supply table metadata");
  const names = new Set();
  const tables = input.tables.map((table) => {
    require(safeName(table.name) &&
      !names.has(table.name), "Table names must be unique simple identifiers");
    names.add(table.name);
    const columns = new Set();
    require(Array.isArray(table.columns) &&
      table.columns.length > 0, "Supply columns");
    return {
      name: table.name,
      columns: table.columns.map((column) => {
        require(safeName(column.name) &&
          !columns.has(
            column.name,
          ), "Column names must be unique simple identifiers");
        columns.add(column.name);
        require(typeof column.type === "string" &&
          Object.hasOwn(
            types,
            column.type.toLowerCase(),
          ), "Unsupported source type; review the mapping before importing");
        require(typeof column.nullable ===
          "boolean", "Column nullability must be explicit");
        return {
          name: column.name,
          type: types[column.type.toLowerCase()],
          required: !column.nullable,
        };
      }),
    };
  });
  return {
    apiVersion: "ingestron.discovery-proposal/v1",
    platform: PLATFORM,
    project: context.project,
    environment: context.environment,
    evidence: "supplied-metadata",
    tables,
    review: [
      "Verify source identity and metadata freshness",
      "Review type mappings, precision and timezone semantics",
      "Select keys and contract versions; no keys are inferred",
    ],
    applied: false,
  };
}
export function command(request) {
  require(request.apiVersion ===
    "ingestron.provider-command-request/v1", "Unsupported command protocol");
  if (request.command === "project assemble") return assemble(request.input);
  if (request.command === "connection prepare") return prepare(request.input);
  if (request.command === "connector contracts")
    return connectorContracts(request.input);
  if (request.command === "discover import")
    return discovery(request.input, request.context);
  if (request.command === "deploy inspect")
    return inspect(request.input.artifact, request.context);
  throw new Error("Unsupported provider command");
}
const PLATFORM = "databricks";
function inspect(artifact, context) {
  require(artifact &&
    artifact.resources &&
    typeof artifact.resources === "object" &&
    !Array.isArray(artifact.resources), "Supply resolved bundle resources");
  const resources = [];
  for (const [kind, entries] of Object.entries(artifact.resources)) {
    require(["jobs", "pipelines"].includes(
      kind,
    ), "Only jobs and pipelines are supported");
    require(entries &&
      typeof entries === "object" &&
      !Array.isArray(entries), "Resources must be keyed objects");
    for (const [key, value] of Object.entries(entries)) {
      require(safeName(key) &&
        value &&
        typeof value === "object" &&
        !Array.isArray(value), "Invalid resource key or settings");
      resources.push({
        key,
        kind,
        name: typeof value.name === "string" ? value.name : key,
      });
    }
  }
  require(resources.length > 0 &&
    resources.length <= 1000, "Supply at most 1000 resources");
  return {
    apiVersion: "ingestron.deployment-inspection/v1",
    platform: PLATFORM,
    project: context.project,
    environment: context.environment,
    resources,
    evidence: "supplied-artifact",
    deployed: false,
    limitations: [
      "Includes and variables must already be resolved",
      "No workspace identity, permissions or remote state verified",
      "Not bundle validation or a deployment change plan",
    ],
  };
}
