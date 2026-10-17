import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  validatePythonSource,
  pythonTransformation,
} from "../src/transformation.mjs";
import { qualityNotebook } from "../src/managed-quality.mjs";

const contract = {
  apiVersion: "v3.1.0",
  kind: "DataContract",
  id: "report",
  version: "1.0.0",
  schema: [
    {
      name: "report",
      properties: [
        {
          name: "id",
          physicalType: "BIGINT",
          primaryKey: true,
          required: true,
        },
      ],
      quality: [
        {
          id: "nonempty",
          metric: "rowCount",
          mustBeGreaterThan: 0,
          severity: "error",
        },
      ],
    },
  ],
};
const fixture = () => {
  const node = {
    id: "report/transform",
    flow: "report",
    step: "transform",
    binding: "dbx",
    python: 'def transform(inputs):\n    return inputs["source"]\n',
    columns: [{ name: "id", type: "BIGINT", key: true, required: true }],
    with: {
      targetTable: "retail.silver.report",
      pythonFile: "flows/report/python/transform.py",
    },
  };
  const plan = {
    project: "retail",
    environment: "dev",
    bindings: {},
    flows: [
      {
        id: "report",
        publishes: {
          renamed: { from: "steps.transform.outputs.result", contract },
        },
      },
    ],
  };
  return { plan, node };
};

test("offline Python validation preserves source and never invokes its module or function", () => {
  const { plan, node } = fixture();
  node.python =
    'from __future__ import annotations\nraise RuntimeError("compiler must not evaluate source")\ndef transform(inputs: dict):\n    return inputs["source"]\n';
  const output = pythonTransformation(
    plan,
    node,
    { source: "retail.bronze.orders" },
    node.with.targetTable,
  );
  assert.ok(output.text.includes(node.python));
  execFileSync(
    "python3",
    ["-c", "import ast,sys; ast.parse(sys.stdin.read())"],
    { input: output.text },
  );
  assert.ok(
    output.qualityNode.with.quality.some(
      (rule) => rule.metric === "duplicateValues",
    ),
  );
  assert.ok(
    output.qualityNode.with.quality.some((rule) => rule.id === "nonempty"),
  );
  const notebook = qualityNotebook(plan, [output.qualityNode]);
  const code = notebook.cells.at(-1).source.join("");
  assert.match(code, /DESCRIBE HISTORY `retail`.`silver`.`report`/);
  assert.match(code, /versionAsOf/);
  assert.match(code, /check_quality/);
  execFileSync(
    "python3",
    ["-c", "import ast,sys; ast.parse(sys.stdin.read())"],
    { input: code },
  );
});

test("source validation refuses invalid, ambiguous or unsupported function definitions", () => {
  for (const source of [
    "def transform(inputs)\n    return inputs\n",
    "async def transform(inputs):\n    return inputs\n",
    "@decorator\ndef transform(inputs):\n    return inputs\n",
    "def helper():\n    def transform(inputs):\n        return inputs\n",
    "def transform(inputs, extra):\n    return inputs\n",
    "def transform(inputs=None):\n    return inputs\n",
    "def transform(*inputs):\n    return inputs\n",
    "def transform(inputs):\n    yield inputs\n",
    "def transform(inputs):\n    def nested():\n        return inputs\n",
    "def transform(inputs):\n    return inputs\ndef transform(inputs):\n    return inputs\n",
    "_ingestron_data = 1\ndef transform(inputs):\n    return inputs\n",
  ])
    assert.throws(() => validatePythonSource(source), undefined, source);
  validatePythonSource(
    'def transform(inputs):\n    def nested():\n        yield 1\n    return inputs["source"]\n',
  );
});

test("native definition reads declared inputs and refuses streaming, type, order and extra-column mismatches without row actions", () => {
  const { plan, node } = fixture();
  const output = pythonTransformation(
    plan,
    node,
    { source: "retail.bronze.orders" },
    node.with.targetTable,
  );
  const script = `import sys,types\nfrom types import SimpleNamespace as N\npyspark=types.ModuleType("pyspark")\npyspark.pipelines=N(materialized_view=lambda **kw: (lambda f: f))\nsys.modules["pyspark"]=pyspark\nreads=[]\ndata=N(isStreaming=False,schema=N(fields=[N(name="id",dataType=N(simpleString=lambda:"bigint"))]))\nspark=N(read=N(table=lambda name:(reads.append(name) or data)))\nnamespace={"spark":spark}\nexec(sys.stdin.read(),namespace)\nview=namespace["_ingestron_materialized_view"]\nassert view() is data\nassert reads==["retail.bronze.orders"]\nfor broken in [N(isStreaming=True),N(isStreaming=False,schema=N(fields=[])),N(isStreaming=False,schema=N(fields=[N(name="id",dataType=N(simpleString=lambda:"string"))])),N(isStreaming=False,schema=N(fields=[N(name="extra",dataType=N(simpleString=lambda:"bigint"))]))]:\n    data=broken\n    try: view()\n    except ValueError: pass\n    else: raise AssertionError("invalid output accepted")\n`;
  execFileSync("python3", ["-c", script], { input: output.text });
  assert.doesNotMatch(
    output.text,
    /\.collect\(|\.count\(|\.toPandas\(|\.write\./,
  );
});

test("Python generation refuses missing or conflicting published contracts", () => {
  const { plan, node } = fixture();
  plan.flows[0].publishes.second = {
    from: "steps.transform.outputs.result",
    contract: { ...contract, id: "other" },
  };
  assert.throws(
    () => pythonTransformation(plan, node, {}, node.with.targetTable),
    /consistent/,
  );
  plan.flows[0].publishes = {};
  assert.throws(
    () => pythonTransformation(plan, node, {}, node.with.targetTable),
    /consistent/,
  );
});
