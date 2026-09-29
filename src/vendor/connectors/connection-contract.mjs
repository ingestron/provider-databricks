import { object, text } from "./shape.mjs";
export const runtimeContract = "ingestron.snapshot/python/v1";
const field = object(
  {
    type: {
      type: "string",
      enum: ["integer", "string", "boolean", "number", "decimal", "json"],
    },
    nullable: { type: "boolean" },
    precision: { type: "integer" },
    scale: { type: "integer" },
  },
  ["type", "nullable"],
);
export const selectionSchema = {
  type: "object",
  additionalProperties: object({
    name: text,
    fields: { type: "object", additionalProperties: field },
  }),
};
export const projectConnectionDefinition = {
  name: "connection prepare",
  description:
    "Prepare a project-resolved connection, selected fields and runtime-only secrets",
  inputSchema: { type: "object", additionalProperties: true },
};
export function validateProjectConnection(input, descriptor) {
  if (
    input.apiVersion !== "ingestron.connection-request/v1" ||
    !/^[a-z][a-z0-9-]*:[a-z][a-z0-9-]*@\d+\.\d+\.\d+$/.test(input.connector) ||
    input.runtimeContract !== runtimeContract ||
    !input.specificationSha256
  )
    throw Error("Invalid project connection request");
  if (
    Object.keys(input).some(
      (k) =>
        ![
          "apiVersion",
          "project",
          "environment",
          "flow",
          "connection",
          "connector",
          "sourceId",
          "tenantId",
          "settings",
          "selection",
          "tables",
          "execution",
          "timeoutSeconds",
          "sourcePackage",
          "executionPackage",
          "specificationSha256",
          "runtimeAssetSha256",
          "runtimeContract",
        ].includes(k),
    )
  )
    throw Error("Unknown project connection setting");
  if (input.tables && input.selection)
    throw Error("Use ODCS tables or legacy selection, never both");
  const selection = input.tables
    ? selectionFromTables(input.tables)
    : input.selection;
  if (
    !conforms(descriptor.settingsSchema, input.settings) ||
    !conforms(descriptor.selectionSchema, selection) ||
    !conforms(descriptor.executionSchema, input.execution)
  )
    throw Error("Invalid connector settings, selection or execution");
  const values = [];
  function visit(value) {
    if (!value || typeof value !== "object") return;
    if (value.$secret) values.push(value);
    else Object.values(value).forEach(visit);
  }
  visit(input.settings);
  if (input.execution.mode === "adf-batch" && values.some((v) => v.$secret.env))
    throw Error("Batch execution requires Key Vault secret references");
  for (const value of values)
    if (value?.$secret) {
      const ref = value.$secret;
      if (
        ref.env
          ? !/^[A-Za-z_][A-Za-z0-9_]*$/.test(ref.env)
          : !(
              /^https:\/\/[a-zA-Z0-9-]+\.vault\.azure\.net$/.test(
                ref.vaultUrl,
              ) &&
              /^[A-Za-z0-9-]{1,127}$/.test(ref.name) &&
              /^[a-fA-F0-9-]{36}$/.test(ref.identityClientId)
            )
      )
        throw Error("Invalid runtime secret reference");
    }
  const names = Object.values(selection).map((table) => table.name);
  if (
    new Set(names).size !== names.length ||
    names.some((name) => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))
  )
    throw Error("Invalid or duplicate selected table name");
  if (!Object.keys(selection).length)
    throw Error("Select at least one source stream");
  for (const [stream, table] of Object.entries(selection)) {
    if (!/^[A-Za-z0-9_-]+$/.test(stream) || !Object.keys(table.fields).length)
      throw Error("Select safe stream IDs and fields");
  }
  for (const table of Object.values(selection))
    for (const [name, field] of Object.entries(table.fields)) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))
        throw Error("Invalid selected field");
      if (
        field.type === "decimal"
          ? !(
              Number.isInteger(field.precision) &&
              field.precision >= 1 &&
              field.precision <= 38 &&
              Number.isInteger(field.scale) &&
              field.scale >= 0 &&
              field.scale <= field.precision
            )
          : field.precision !== undefined || field.scale !== undefined
      )
        throw Error("Invalid decimal precision/scale");
    }
  return selection;
}

export function conforms(schema, value) {
  const allowed = new Set([
    "type",
    "oneOf",
    "properties",
    "items",
    "required",
    "additionalProperties",
    "enum",
    "minItems",
    "maxItems",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
  ]);
  if (
    !schema ||
    typeof schema !== "object" ||
    Object.keys(schema).some((k) => !allowed.has(k))
  )
    throw Error("Unsupported connector schema keyword");
  if (
    schema.oneOf &&
    schema.oneOf.filter((s) => conforms(s, value)).length !== 1
  )
    return false;
  if (
    schema.enum &&
    !schema.enum.some((v) => JSON.stringify(v) === JSON.stringify(value))
  )
    return false;
  const isObject =
    value !== null && typeof value === "object" && !Array.isArray(value);
  const types = {
    object: isObject,
    array: Array.isArray(value),
    integer: Number.isInteger(value),
    number: typeof value === "number" && Number.isFinite(value),
    string: typeof value === "string",
    boolean: typeof value === "boolean",
    null: value === null,
  };
  if (schema.type && !types[schema.type]) return false;
  if (
    typeof value === "number" &&
    ((schema.minimum !== undefined && value < schema.minimum) ||
      (schema.maximum !== undefined && value > schema.maximum))
  )
    return false;
  if (
    typeof value === "string" &&
    (Array.from(value).length < (schema.minLength ?? 0) ||
      Array.from(value).length > (schema.maxLength ?? Infinity))
  )
    return false;
  if (
    Array.isArray(value) &&
    (value.length < (schema.minItems ?? 0) ||
      value.length > (schema.maxItems ?? Infinity) ||
      (schema.items && !value.every((v) => conforms(schema.items, v))))
  )
    return false;
  if (isObject) {
    if (!(schema.required ?? []).every((k) => Object.hasOwn(value, k)))
      return false;
    for (const [key, child] of Object.entries(value)) {
      if (schema.properties && Object.hasOwn(schema.properties, key)) {
        if (!conforms(schema.properties[key], child)) return false;
      } else if (schema.additionalProperties === false) return false;
      else if (
        typeof schema.additionalProperties === "object" &&
        !conforms(schema.additionalProperties, child)
      )
        return false;
    }
  }
  return true;
}

export function selectionFromTables(tables) {
  if (!tables || typeof tables !== "object" || Array.isArray(tables))
    throw Error("Invalid ODCS table map");
  const selected = {};
  for (const [name, table] of Object.entries(tables)) {
    if (
      !table.contract ||
      !Array.isArray(table.columns) ||
      !table.columns.length ||
      !table.source?.stream ||
      selected[table.source.stream]
    )
      throw Error("Invalid or duplicate contracted stream");
    const fields = {};
    for (const column of table.columns) {
      const type = column.type.toUpperCase();
      const mapped = {
        STRING: "string",
        BIGINT: "integer",
        INT: "integer",
        INTEGER: "integer",
        SMALLINT: "integer",
        DOUBLE: "number",
        FLOAT: "number",
        BOOLEAN: "boolean",
      }[type];
      const decimal = /^DECIMAL\((\d+),\s*(\d+)\)$/.exec(type);
      if (!mapped && !decimal)
        throw Error(
          `Snapshot projection does not support ODCS physical type ${type}`,
        );
      fields[column.name] = decimal
        ? {
            type: "decimal",
            precision: Number(decimal[1]),
            scale: Number(decimal[2]),
            nullable: !column.required,
          }
        : { type: mapped, nullable: !column.required };
    }
    selected[table.source.stream] = { name, fields };
  }
  return selected;
}
