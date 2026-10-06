/** Native post-ingestion checks use one committed Delta version per table. */
import { evaluator } from "../plugin/quality-assets.mjs";
const py = JSON.stringify;
export function qualityNotebook(plan, nodes) {
  const code = [evaluator];
  for (const n of nodes) {
    const target = (
      n.with.targetTable
        ? n.with.targetTable.split(".")
        : [
            plan.bindings[n.binding].catalog,
            n.with.schema ?? n.with.target.schema,
            n.with.table,
          ]
    )
      .map((x) => "`" + x + "`")
      .join(".");
    const context = {
      project: plan.project,
      environment: plan.environment,
      flow: n.flow,
      table: n.table,
      target,
      identity: n.id.replace(/[^A-Za-z0-9_]/g, "_"),
    };
    code.push(`context = json.loads(${py(JSON.stringify(context))})
history = spark.sql(${py("DESCRIBE HISTORY " + target + " LIMIT 1")}).take(1)
if len(history) != 1 or type(history[0]["version"]) is not int or history[0]["version"] < 0:
    raise ValueError("Expected a committed Delta version for declared quality target")
context["deltaVersion"] = str(history[0]["version"])
data = spark.read.option("versionAsOf", context["deltaVersion"]).table(${py(target)})
expected = json.loads(${py(JSON.stringify(n.columns.map((c) => c.name)))})
if any(column not in data.columns for column in expected):
    raise ValueError("Managed destination is missing declared contract columns; quality cannot be evaluated")
${n.with.history === "SCD_TYPE_2" ? 'if "__END_AT" not in data.columns:\n    raise ValueError("SCD2 quality requires native __END_AT current-row boundary")\ndata = data.where("__END_AT IS NULL")' : ""}
types = {field.name: field.dataType.simpleString().upper().replace(" ", "") for field in data.schema.fields}
expected_types = json.loads(${py(JSON.stringify(Object.fromEntries(n.columns.map((c) => [c.name, c.type === "INTEGER" ? "INT" : c.type]))))})
if any(types.get(column) != value.upper().replace(" ", "") for column, value in expected_types.items()):
    raise ValueError("Managed destination types differ from reviewed contract; quality cannot be evaluated")
required = json.loads(${py(JSON.stringify(n.columns.filter((c) => c.required).map((c) => c.name)))})
if any(data.where(F.col(column).isNull()).limit(1).count() for column in required):
    raise ValueError("Required contract values failed after ingestion; committed data and native checkpoint are unchanged")
rows = data.count()
check_quality(data, rows, json.loads(${py(JSON.stringify(n.with.quality ?? []))}), context)
`);
  }
  return {
    nbformat: 4,
    nbformat_minor: 5,
    metadata: {
      kernelspec: {
        display_name: "Python 3",
        language: "python",
        name: "python3",
      },
      language_info: { name: "python" },
    },
    cells: [
      {
        cell_type: "markdown",
        metadata: {},
        source: [
          "# Check managed ingestion quality\n\nRuns after the pipeline commits. A required failure blocks dependent job tasks; it does not roll back ingested data or the native checkpoint. SCD2 checks cover current rows. Metrics are measured against one committed Delta version per table.\n",
        ],
      },
      {
        cell_type: "code",
        metadata: {},
        execution_count: null,
        outputs: [],
        source: [code.join("\n")],
      },
    ],
  };
}
