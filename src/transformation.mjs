import { parser } from "@lezer/python";
import { outputColumns } from "./column-mapping.mjs";
import { qualityChecks } from "./quality.mjs";

const check = (condition, message) => {
  if (!condition) throw Error(message);
};
const canonical = (value) =>
  JSON.stringify(value, (_, item) =>
    item && typeof item === "object" && !Array.isArray(item)
      ? Object.fromEntries(
          Object.entries(item).sort(([a], [b]) => a.localeCompare(b)),
        )
      : item,
  );
export const transformationSourceSchema = {
  pythonFile: { type: "string", pattern: "\\.py$" },
  entrypoint: { type: "string", pattern: "^[A-Za-z_][A-Za-z0-9_]*$" },
};
export const transformationLanguageSchema = {
  oneOf: [
    {
      required: ["sqlFile"],
      not: {
        anyOf: [{ required: ["pythonFile"] }, { required: ["entrypoint"] }],
      },
    },
    { required: ["pythonFile"], not: { required: ["sqlFile"] } },
  ],
};

/** Parse source as evidence. No Python interpreter or customer function is invoked. */
export function validatePythonSource(source, entrypoint = "transform") {
  check(
    typeof source === "string" &&
      source.length > 0 &&
      source.length <= 1024 * 1024,
    "Python source must contain at most 1 MiB of text",
  );
  check(
    /^[A-Za-z_]\w*$/.test(entrypoint) && !entrypoint.startsWith("_ingestron_"),
    "Choose a Python function name outside the reserved _ingestron_ namespace",
  );
  const tree = parser.parse(source);
  tree.iterate({
    enter(node) {
      check(
        !node.type.isError,
        `Invalid Python syntax near character ${node.from}`,
      );
      check(
        node.name !== "VariableName" ||
          !source.slice(node.from, node.to).startsWith("_ingestron_"),
        "Python source uses the reserved _ingestron_ namespace",
      );
    },
  });
  const definitions = [];
  for (let node = tree.topNode.firstChild; node; node = node.nextSibling) {
    if (node.name !== "FunctionDefinition") continue;
    const name = node.getChild("VariableName");
    if (name && source.slice(name.from, name.to) === entrypoint)
      definitions.push(node);
  }
  check(
    definitions.length === 1,
    `Declare exactly one top-level function ${entrypoint}(inputs)`,
  );
  const definition = definitions[0];
  check(
    definition.firstChild.name === "def",
    "The transformation function must be synchronous",
  );
  const parameters = definition.getChild("ParamList");
  const children = [];
  for (let node = parameters.firstChild; node; node = node.nextSibling)
    children.push(node);
  check(
    (children.length === 3 ||
      (children.length === 4 && children[2].name === "TypeDef")) &&
      children[1].name === "VariableName",
    "The transformation function must take one positional input mapping without defaults",
  );
  const body = definition.getChild("Body");
  let returns = false;
  tree.iterate({
    from: body.from,
    to: body.to,
    enter(node) {
      if (
        node.from >= body.from &&
        ["FunctionDefinition", "ClassDefinition"].includes(node.name)
      )
        return false;
      check(
        !["YieldStatement", "YieldExpression"].includes(node.name),
        "The transformation function must return a batch DataFrame, not a generator",
      );
      if (node.name === "ReturnStatement") returns = true;
    },
  });
  check(returns, "The transformation function must return a batch DataFrame");
}

export function transformationContract(plan, node) {
  const flow = plan.flows.find((flow) => flow.id === node.flow);
  const outputs = Object.values(flow?.publishes ?? {}).filter(
    (output) => output.from === `steps.${node.step}.outputs.result`,
  );
  const contracts = [
    ...new Set(outputs.map((output) => canonical(output.contract))),
  ];
  check(
    contracts.length === 1,
    "Transformations require one consistent published output contract",
  );
  check(
    !node.with.outputContract ||
      canonical(node.with.outputContract) === contracts[0],
    "Transformation activity outputContract must match its published output contract",
  );
  return outputs[0].contract;
}

export function pythonTransformation(plan, node, sources, target) {
  const entrypoint = node.with.entrypoint ?? "transform";
  validatePythonSource(node.python, entrypoint);
  const columns = outputColumns(node.columns);
  check(
    columns.length > 0,
    "Python transformations require typed output columns",
  );
  const contract = transformationContract(plan, node);
  // Complete committed-table checks, including primary-key uniqueness, run after refresh.
  const quality = qualityChecks(
    contract,
    columns,
    "query-ingestion@v1",
    "output",
  );
  const fields = columns.map((column) => [
    column.name,
    (column.type === "INTEGER" ? "INT" : column.type).replaceAll(" ", ""),
  ]);
  const schema = columns
    .map((column) => `\`${column.name}\` ${column.type}`)
    .join(", ");
  const source = node.python + (node.python.endsWith("\n") ? "" : "\n");
  return {
    qualityNode: { ...node, columns, with: { ...node.with, quality } },
    text:
      `# Generated by Ingestron. Review the source and contract before workspace execution.\n` +
      source +
      `\n# Provider-owned native definition; source above is evaluated by Lakeflow, never by the compiler.\n` +
      `from pyspark import pipelines as _ingestron_dp\n` +
      `_ingestron_transform = ${entrypoint}\n` +
      `@_ingestron_dp.materialized_view(name=${JSON.stringify(target)}, schema=${JSON.stringify(schema)})\n` +
      `def _ingestron_materialized_view():\n` +
      `    _ingestron_inputs = {\n` +
      Object.entries(sources)
        .map(
          ([alias, relation]) =>
            `        ${JSON.stringify(alias)}: spark.read.table(${JSON.stringify(relation)}),\n`,
        )
        .join("") +
      `    }\n` +
      `    _ingestron_data = _ingestron_transform(_ingestron_inputs)\n` +
      `    if getattr(_ingestron_data, "isStreaming", True):\n` +
      `        raise ValueError("Python transformations must return a batch DataFrame")\n` +
      `    _ingestron_fields = [[field.name, field.dataType.simpleString().upper().replace(" ", "")] for field in _ingestron_data.schema.fields]\n` +
      `    if _ingestron_fields != ${JSON.stringify(fields)}:\n` +
      `        raise ValueError("Python output columns or types differ from the reviewed contract")\n` +
      `    return _ingestron_data\n`,
  };
}
