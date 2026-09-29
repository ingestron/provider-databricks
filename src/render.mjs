import { deploymentSettings, renderResources } from "./resources.mjs";
import { templateRenderer } from "./templates/engine.mjs";
const check = (ok, message) => {
  if (!ok) throw new Error(message);
};
const ident = (s) => {
  check(
    typeof s === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(s),
    `Invalid identifier ${s}`,
  );
  return "`" + s + "`";
};
const relation = (s) => s.split(".").map(ident).join(".");
const sqlString = (s) =>
  "'" + s.replaceAll("\\", "\\\\").replaceAll("'", "\\'") + "'";
const py = (s) => JSON.stringify(s);
const kind = (n) => n.uses.split("@")[0];
const pipeline = (n) => n.with.pipeline ?? n.with.pipelineId;
const target = (plan, n, history = false) =>
  n.externalTarget ??
  (kind(n) === "materialized-view"
    ? n.with.targetTable
    : [
        plan.bindings[n.binding].catalog,
        history ? n.with.target.historySchema : n.with.schema,
        n.with.table,
      ].join("."));
const inputNode = (plan, n, ref) => {
  const required = ref.startsWith("requires.")
    ? plan.flows.find((f) => f.id === n.flow).requires[ref.slice(9)]
    : undefined;
  const dataset = required ? plan.datasets[required.dataset] : undefined;
  const step = /^steps\.([\w-]+)\.outputs\.(result|table)$/.exec(ref);
  const found = dataset
    ? plan.nodes.find((x) => x.id === dataset.producer)
    : step
      ? plan.nodes.find(
          (x) => x.flow === n.flow && x.table === n.table && x.step === step[1],
        )
      : undefined;
  if (!found && dataset && required.handover === "relation") {
    const imported = plan.delivery?.imports.find(
      (i) => i.dataset === required.dataset && i.kind === "relation",
    );
    const location = imported?.location,
      binding = plan.bindings[location?.binding];
    check(
      location &&
        binding?.kind === "databricks" &&
        binding.host.replace(/\/$/, "").toLowerCase() ===
          plan.bindings[n.binding].host.replace(/\/$/, "").toLowerCase() &&
        /^[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*$/.test(location.name),
      "Relation handover requires a qualified relation in the same workspace",
    );
    return { externalTarget: location.name };
  }
  return found && n.needs.includes(found.id)
    ? { ...found, historyInput: dataset?.port === "history" }
    : undefined;
};
export function validateIngestionPlan(plan) {
  check(
    plan.apiVersion === "ingestron.plan/v1" && plan.nodes.length,
    "Invalid ingestion plan",
  );
  check(!plan.pending.length, "Resolve all inputs before ingestion generation");
  deploymentSettings(plan);
  const outputs = new Set();
  const internalNames = new Set();
  const pipelineBindings = new Map();
  for (const flow of plan.flows) {
    for (const required of Object.values(flow.requires))
      check(
        required.select.mode === "same-run",
        "Declarative inputs support same-run dependencies only",
      );
    check(
      !Object.values(flow.publishes).some((p) => p.location),
      "Declare output locations in the provider activity",
    );
  }
  for (const n of plan.nodes) {
    check(
      n.platform === "databricks" && n.runtime?.implementation === "native",
      "Ingestion standards require native Databricks output",
    );
    check(
      ["lakeflow-ingest", "materialized-view"].includes(kind(n)),
      "Declarative ingestion accepts materialised-view transformations; procedural activities need a separate export",
    );
    check(
      /^[A-Za-z0-9_-]+$/.test(pipeline(n)),
      "A logical pipeline alias is required",
    );
    check(
      !pipelineBindings.has(pipeline(n)) ||
        pipelineBindings.get(pipeline(n)) === n.binding,
      "A pipeline cannot span platform bindings",
    );
    pipelineBindings.set(pipeline(n), n.binding);
    check(
      n.generation.kind === "native-notebook" &&
        !n.generation.code &&
        !n.generation.runtime,
      "This catalogue requires provider-owned native definitions without procedural code",
    );
    const internal = pipeline(n) + ":" + n.id.replace(/[^A-Za-z0-9_]/g, "_");
    check(
      !internalNames.has(internal),
      "Generated pipeline definition names collide; rename the flow/table",
    );
    internalNames.add(internal);
    const name = target(plan, n);
    relation(name);
    check(!outputs.has(name), `Duplicate dataset target ${name}`);
    outputs.add(name);
    if (kind(n) === "lakeflow-ingest") {
      check(
        plan.flows.find((f) => f.id === n.flow)?.ingestion,
        "Use flow.ingestion to select lakeflow-ingest",
      );
      const s = n.source;
      check(
        s && ["files", "adls"].includes(plan.bindings[s.binding]?.kind),
        "Databricks ingestion standards consume files in existing ADLS or Volumes; database extraction belongs upstream",
      );
      check(
        typeof s.path === "string" &&
          /^(abfss:\/\/|\/Volumes\/).+[^/]$/.test(s.path),
        "source.path must be a cloud or Volume directory without a trailing slash",
      );
      check(
        ["json", "parquet"].includes(s.format),
        "Ingestion standards currently accept JSON or Parquet; CSV requires an explicit reader policy",
      );
      check(
        Object.keys(s).every((k) =>
          ["binding", "path", "format", "deliveryIndex"].includes(k),
        ),
        "Unsupported source setting; ingestion standards do not silently ignore source options",
      );
      if (n.with.standard === "snapshot-with-history@v1")
        check(
          typeof s.deliveryIndex === "string" &&
            s.deliveryIndex.startsWith(s.path + "/") &&
            s.deliveryIndex.endsWith(".json"),
          "Snapshot source requires deliveryIndex JSON inside source.path",
        );
      else
        check(
          !s.deliveryIndex,
          "deliveryIndex applies only to complete snapshots",
        );
      if (n.with.target.historySchema) {
        const h = target(plan, n, true);
        relation(h);
        check(!outputs.has(h), `Duplicate history target ${h}`);
        outputs.add(h);
      }
    } else {
      for (const [alias, source] of Object.entries(n.with.sources ?? {})) {
        ident(alias);
        check(
          typeof source.from === "string" && inputNode(plan, n, source.from),
          "Transformation sources must resolve to declared dataset dependencies",
        );
      }
    }
  }
  // Same-pipeline table dependencies belong to Lakeflow. Only cross-pipeline edges form job tasks.
  const graph = new Map(
    [...pipelineBindings.keys()].map((p) => [p, new Set()]),
  );
  for (const n of plan.nodes)
    for (const ref of n.needs) {
      const parent = plan.nodes.find((x) => x.id === ref);
      check(parent, `Missing upstream ${ref}`);
      if (pipeline(n) !== pipeline(parent))
        graph.get(pipeline(n)).add(pipeline(parent));
    }
  const active = new Set(),
    done = new Set();
  const visit = (p) => {
    check(
      !active.has(p),
      "Pipeline dependencies form a cycle; regroup datasets",
    );
    if (done.has(p)) return;
    active.add(p);
    for (const q of graph.get(p)) visit(q);
    active.delete(p);
    done.add(p);
  };
  for (const p of graph.keys()) visit(p);
  renderResources(
    plan,
    new Map(
      [...pipelineBindings].map(([p, binding]) => [
        p,
        { binding, sources: [], needs: graph.get(p) },
      ]),
    ),
    () => {},
  );
}
export function renderIngestion(plan) {
  validateIngestionPlan(plan);
  const files = {},
    pipelines = new Map();
  const put = (path, format, value) => {
    check(!files[path], `Duplicate generated file ${path}`);
    files[path] = { format, value };
  };
  for (const n of plan.nodes) {
    const pid = pipeline(n);
    const group = pipelines.get(pid) ?? {
      sources: [],
      needs: new Set(),
      binding: n.binding,
    };
    for (const ref of n.needs) {
      const parent = plan.nodes.find((x) => x.id === ref);
      if (pipeline(parent) !== pid) group.needs.add(pipeline(parent));
    }
    pipelines.set(pid, group);
    const stem = `pipelines/${n.flow}/${n.table ?? n.step}`;
    if (kind(n) === "materialized-view") {
      // Query-scoped CTEs keep alias names local; no extra published staging tables.
      const sources = Object.entries(n.with.sources ?? {}).map(
        ([alias, ref]) =>
          `${ident(alias)} AS (SELECT * FROM ${relation(target(plan, inputNode(plan, n, ref.from), inputNode(plan, n, ref.from).historyInput))})`,
      );
      const query = n.sql.trim().replace(/;\s*$/, "");
      const renderView = templateRenderer(
        Object.fromEntries(
          n.implementation.templates.map((t) => [t.output, t.content]),
        ),
      );
      const text = renderView("view.sql", {
        target: relation(target(plan, n)),
        sources: sources.join(",\n"),
        query,
      });
      put(stem + ".sql", "text", text);
      group.sources.push(stem + ".sql");
      continue;
    }
    const w = n.with,
      s = n.source;
    const templates = Object.fromEntries(
      n.implementation.templates.map((t) => [t.output, t.content]),
    );
    const render = templateRenderer(templates);
    const isEvents = w.standard === "change-feed-with-history@v1";
    const isSnapshot = w.standard === "snapshot-with-history@v1";
    const required = new Set([
      ...n.columns.filter((c) => c.required).map((c) => c.name),
      ...w.keys,
      ...(w.sequenceBy ?? []),
    ]);
    const rules = [...required].map((column, i) => {
      let expression = `${ident(column)} IS NOT NULL`;
      if (isEvents && ![...w.keys, ...w.sequenceBy].includes(column))
        expression = `${ident(w.operationColumn)} = 'D' OR (${expression})`;
      return {
        name: `required_${i}`,
        expression,
        python: py(expression),
        message: py(`Required column check failed: ${column}`),
      };
    });
    for (const [name, expression] of Object.entries(w.expectations ?? {})) {
      ident(name);
      check(
        !/[;]|--|\/\*/.test(expression),
        "Expectations must be a single SQL predicate, without comments or statements",
      );
      rules.push({
        name: "business_" + name,
        expression,
        python: py(expression),
      });
    }
    const current = target(plan, n),
      history = w.target.historySchema ? target(plan, n, true) : "";
    const ddl = n.columns.map((c) => `${ident(c.name)} ${c.type}`).join(", ");
    if (isSnapshot) {
      const source = render("snapshot.py", {
        policy: py(JSON.stringify(w.snapshotPolicy ?? {})),
        contractVersion: py(w.contractVersion),
        invalidPredicate: py(
          rules
            .map((r) => `NOT coalesce((${r.expression}), false)`)
            .join(" OR ") || "false",
        ),
        root: py(s.path),
        index: py(s.deliveryIndex),
        schema: py(ddl),
        keys: py(w.keys),
        function: n.id.replace(/[^A-Za-z0-9_]/g, "_"),
        dataset: py(`${plan.project}.${n.flow}.${n.table}`),
        format: py(s.format),
        rules,
        history: py(history),
        current: py(current),
        tracked: w.trackedColumns ? py(w.trackedColumns) : "",
        select: n.columns.map((c) => py(c.name)).join(", "),
      });
      const notebook = {
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
              `# ${n.table}: complete snapshots to SCD2\n\nThis definition notebook belongs to pipeline ${pid}. Lakeflow calls the snapshot function; do not run it as a job notebook. The delivery index is an upstream completion contract. Empty snapshots are rejected unless explicitly permitted. Retention is externally managed.\n`,
            ],
          },
          {
            cell_type: "code",
            metadata: {},
            execution_count: null,
            outputs: [],
            source: [source],
          },
        ],
      };
      put(stem + ".ipynb", "json", notebook);
      group.sources.push(stem + ".ipynb");
    } else {
      const values = {
        current: relation(current),
        rules,
        columns: n.columns.map((c) => ident(c.name)).join(", "),
        path: sqlString(s.path),
        format: sqlString(s.format),
        ddl: sqlString(ddl),
      };
      if (isEvents)
        Object.assign(values, {
          history: relation(history),
          staging: ident(`__${n.id.replace(/[^A-Za-z0-9_]/g, "_")}_events`),
          operation: ident(w.operationColumn),
          flow_name: ident(`apply_${n.id.replace(/[^A-Za-z0-9_]/g, "_")}`),
          keys: w.keys.map(ident).join(", "),
          sequence_columns: w.sequenceBy.map(ident).join(", "),
          sequence:
            w.sequenceBy.length === 1
              ? ident(w.sequenceBy[0])
              : `STRUCT(${w.sequenceBy.map(ident).join(", ")})`,
          business_columns: n.columns
            .filter(
              (c) =>
                c.name !== w.operationColumn && !w.sequenceBy.includes(c.name),
            )
            .map((c) => ident(c.name))
            .join(", "),
          tracked: (w.trackedColumns ?? []).map(ident).join(", "),
        });
      put(
        stem + ".sql",
        "text",
        render(isEvents ? "events.sql" : "append.sql", values),
      );
      group.sources.push(stem + ".sql");
    }
  }
  const ownership = renderResources(plan, pipelines, put);
  put(
    "README.md",
    "text",
    `# ${plan.project}: native Databricks ingestion\n\nThis bundle includes serverless Lakeflow pipeline resources, their source files and a job for refresh dependencies. Configure your environment values before generation. Databricks creates these project resources when you deploy; the workspace, Unity Catalog schemas, source storage, identities and data grants must already exist. No Ingestron runtime is installed.\n\n## Resource ownership\n\n${ownership}\n\n${[...pipelines].map(([p, g]) => `## Pipeline ${p}\n\n${g.sources.map((s) => `- [${s}](${s})`).join("\n")}`).join("\n\n")}\n\n## Operations and recovery\n\nUse native pipeline event logs and Unity Catalog lineage. No Ingestron runtime is installed. Source schemas are explicit; bad schema or required values fail ingestion. Source delivery retention is an upstream responsibility; generated code does not implement archival or lifecycle deletion. Do not full-refresh after inputs have expired. History contains only observed changes; it is not a raw source archive. Repair normal failures by rerunning the native pipeline.\n\nSnapshot delivery indexes must retain immutable IDs, versions, paths and contents. Publish them atomically after files are complete. Counts and keys are checked at runtime, but producer truth and historical completeness cannot be proven offline. Event input contains baseline rows and full-row I/U/D records with non-null ordered sequencing; conflicting equal-sequence changes must be prevented upstream.\n\nThis export resolves ${plan.environment} only. Generate other environments from their reviewed configuration. Offline checks do not prove native workspace execution.\n`,
  );
  return files;
}
