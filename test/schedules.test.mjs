import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  nativeCalendar,
  scheduledClosures,
  renderScheduledJobs,
} from "../src/schedules.mjs";
import { renderResources } from "../src/resources.mjs";
const definition = (
  recurrence = { kind: "daily", times: ["09:15", "17:45"] },
) => ({
  apiVersion: "ingestron.schedule/v1",
  enabled: true,
  timeZone: "UTC",
  start: "2026-10-05T00:07:17Z",
  end: "2026-10-06T12:00:17Z",
  recurrence,
  overlap: "native",
  missed: "native",
});
const node = (flow, table, alias, needs = []) => ({
  id: `${flow}/${table}/ingest`,
  flow,
  table,
  step: "ingest",
  uses: "lakeflow-ingest@v1",
  binding: "dbx",
  columns: [{ name: "observed_at", type: "TIMESTAMP" }],
  needs,
  runtime: {
    options: { deployment: { job: { selectiveRefresh: "native-beta" } } },
  },
  with: {
    pipeline: alias,
    schema: "bronze",
    table,
    target: { historySchema: "history" },
  },
});
const fixture = () => {
  const upstream = node("upstream", "seed", "shared");
  const main = node("source", "orders", "primary", [upstream.id]);
  const manual = node("manual", "customers", "shared");
  const downstream = node("report", "summary", "reporting", [main.id]);
  const schedule = definition();
  return {
    project: "retail",
    environment: "dev",
    selection: {},
    bindings: {
      dbx: {
        kind: "databricks",
        catalog: "retail",
        host: "https://example.invalid",
      },
    },
    flows: [
      { id: "source", tables: { orders: {} }, schedule },
      { id: "upstream", tables: { seed: {} } },
      { id: "manual", tables: { customers: {} } },
      { id: "report", tables: { summary: {} } },
    ],
    scheduleBoundaries: {
      source: {
        apiVersion: "ingestron.schedule-boundaries/v1",
        definitionDigest: "a".repeat(64),
        timeZone: "UTC",
        start: { instant: schedule.start },
        end: { instant: schedule.end },
      },
    },
    nodes: [upstream, main, manual, downstream],
    names: { jobs: { source: "retail_source" } },
  };
};
const capture = (plan) => {
  const files = new Map();
  const pipelines = new Map(
    [...new Set(plan.nodes.map((n) => n.with.pipeline))].map((alias) => [
      alias,
      { quality: plan.nodes.filter((n) => n.with.pipeline === alias) },
    ]),
  );
  const ids = new Map(
    [...pipelines.keys()].map((alias) => [
      alias,
      `\${resources.pipelines.pipeline_${alias}.id}`,
    ]),
  );
  renderScheduledJobs(
    plan,
    pipelines,
    ids,
    {
      max_concurrent_runs: 1,
      queue: { enabled: true },
      tasks: [],
      timeout_seconds: 123,
    },
    (path, format, value) => files.set(path, { format, value }),
    (_plan, nodes) => ({
      nbformat: 4,
      cells: nodes.map((n) => ({ source: [n.id] })),
    }),
    (nodes) => ({ nbformat: 4, cells: nodes.map((n) => ({ source: [n.id] })) }),
  );
  return {
    files,
    jobs: files.get("resources/schedules.job.yml").value.resources.jobs,
    manifest: files.get("databricks-schedules.json").value.schedules,
  };
};
test("paired calendar slots never create extra hour/minute combinations", () => {
  assert.deepEqual(
    nativeCalendar(definition()).map((s) => s.cron),
    ["0 15 9 * * ?", "0 45 17 * * ?"],
  );
  assert.deepEqual(
    nativeCalendar(
      definition({
        kind: "weekly",
        times: ["09:15", "17:45"],
        weekDays: ["Monday", "Friday"],
      }),
    ).map((s) => s.cron),
    ["0 15 9 ? * MON,FRI", "0 45 17 ? * MON,FRI"],
  );
  assert.deepEqual(
    nativeCalendar(
      definition({ kind: "monthly", times: ["09:15"], monthDays: [31, 1] }),
    ).map((s) => s.cron),
    ["0 15 9 1,31 * ?"],
  );
});
test("elapsed native intervals preserve phase and seconds for every supported divisor", () => {
  for (let minutes = 1; minutes <= 1440; minutes++) {
    if (1440 % minutes) continue;
    const actual = [];
    for (const entry of nativeCalendar(
      definition({ kind: "interval", minutes }),
    )) {
      const [seconds, minuteList, hourList] = entry.cron.split(" ");
      assert.equal(seconds, "17");
      for (const hour of hourList.split(",").map(Number))
        for (const minute of minuteList.split(",").map(Number))
          actual.push(hour * 60 + minute);
    }
    actual.sort((a, b) => a - b);
    const expected = [];
    for (let minute = 0; minute < 1440; minute++)
      if ((minute - 7 + 1440) % minutes === 0) expected.push(minute);
    assert.deepEqual(
      actual,
      expected,
      `Exact occurrence set for ${minutes} minutes`,
    );
  }
});
test("unrepresentable phases, portable policies and malformed dates are explicit failures", () => {
  for (const schedule of [
    definition({ kind: "interval", minutes: 7 }),
    definition({ kind: "interval", minutes: 2880 }),
    { ...definition(), overlap: "queue" },
    { ...definition(), missed: "latest" },
    { ...definition(), start: "2026-02-30T00:00:00Z" },
    {
      ...definition(),
      recurrence: { kind: "daily", times: ["09:00", "09:00"] },
    },
    {
      ...definition(),
      recurrence: { kind: "weekly", times: ["09:00"], weekDays: ["Noday"] },
    },
    {
      ...definition(),
      recurrence: { kind: "monthly", times: ["09:00"], monthDays: [32] },
    },
  ])
    assert.throws(() => nativeCalendar(schedule));
});
test("closure includes upstream work and excludes unrelated manual members and downstream reports", () => {
  const plan = fixture(),
    [entry] = scheduledClosures(plan);
  assert.deepEqual(entry.nodes.map((n) => n.id).sort(), [
    "source/orders/ingest",
    "upstream/seed/ingest",
  ]);
  assert.deepEqual(entry.groups.find((g) => g.alias === "shared").selection, [
    "retail.bronze.seed",
    "retail.history.seed",
  ]);
  assert.equal(entry.groups.find((g) => g.alias === "shared").selective, true);
  assert.equal(
    entry.groups.find((g) => g.alias === "primary").selective,
    false,
  );
  const { files, jobs, manifest } = capture(plan);
  assert.equal(Object.keys(jobs).length, 2);
  for (const job of Object.values(jobs)) {
    assert.equal(job.timeout_seconds, 123);
    assert.deepEqual(
      job.tasks.find((t) => t.task_key === "refresh_shared").pipeline_task
        .refresh_selection,
      ["retail.bronze.seed", "retail.history.seed"],
    );
    assert.ok(
      job.tasks
        .find((t) => t.task_key === "refresh_primary")
        .depends_on.some((d) => d.task_key === "quality_shared"),
    );
    assert.ok(job.tasks.every((t) => !JSON.stringify(t).includes("reporting")));
  }
  assert.ok(!JSON.stringify([...files]).includes("manual/customers"));
  assert.deepEqual(manifest[0].tables, ["orders"]);
});
const admitted = (job, trigger, triggerType = "periodic") => {
  const tasks = new Map(job.tasks.map((t) => [t.task_key, t]));
  const evaluate = (key) => {
    const t = tasks.get(key);
    if ((t.depends_on ?? []).some((d) => !evaluate(d.task_key))) return false;
    if (!t.condition_task) return true;
    const { op, left, right } = t.condition_task;
    const value = left === "{{job.trigger.type}}" ? triggerType : trigger;
    return op === "EQUAL_TO"
      ? value === right
      : op === "GREATER_THAN_OR_EQUAL"
        ? value >= Number(right)
        : value < Number(right);
  };
  return job.tasks
    .filter((t) => t.pipeline_task || t.notebook_task)
    .every((t) => evaluate(t.task_key));
};
test("all data tasks obey exact trigger bounds, including seconds and queued occurrence semantics", () => {
  const plan = fixture();
  plan.nodes[0].with.freshness = { column: "observed_at", maximumAgeHours: 24 };
  for (const endSeconds of [17, 18]) {
    plan.flows[0].schedule = definition({ kind: "interval", minutes: 15 });
    plan.flows[0].schedule.end = `2026-10-06T12:00:${endSeconds}Z`;
    plan.scheduleBoundaries.source.end.instant = plan.flows[0].schedule.end;
    const { jobs, files } = capture(plan),
      start = Date.parse(plan.flows[0].schedule.start),
      end = Date.parse(plan.flows[0].schedule.end);
    const job = Object.values(jobs)[0];
    for (const t of job.tasks.filter((t) => t.pipeline_task || t.notebook_task))
      assert.ok(
        t.depends_on.some(
          (d) => d.task_key === "schedule_end" && d.outcome === "true",
        ),
      );
    assert.equal(admitted(job, start - 17000 - 60000), false);
    assert.equal(admitted(job, start - 17000), true);
    assert.equal(
      admitted(job, Date.parse("2026-10-06T12:00:00Z")),
      endSeconds === 18,
    );
    assert.equal(admitted(job, end - 17000), false);
    assert.equal(admitted(job, start - 17000, "one_time"), false);
    assert.ok(!JSON.stringify(job).includes("job.start_time"));
    assert.equal(
      files.get("operations/schedule_source_freshness.ipynb").value.cells
        .length,
      1,
    );
  }
});
test("partial, missing, cyclic, unreviewed selective and stale boundary plans cannot create schedules", () => {
  const cases = [
    (p) => (p.selection.table = "orders"),
    (p) => (p.selection.step = "ingest"),
    (p) => (p.flows[0].tables.absent = {}),
    (p) => delete p.nodes[0].runtime.options.deployment.job.selectiveRefresh,
    (p) => (p.nodes[0].needs = [p.nodes[1].id]),
    (p) => (p.nodes[1].needs = ["absent"]),
    (p) => (p.nodes[0].uses = "snapshot-publish@v1"),
    (p) => (p.scheduleBoundaries.source.end.instant = "2026-10-07T00:00:00Z"),
  ];
  for (const edit of cases) {
    const plan = fixture();
    edit(plan);
    assert.throws(() => capture(plan));
  }
  const legacy = fixture();
  legacy.nodes[0].runtime.options.deployment.job.schedule = {};
  assert.throws(() => capture(legacy), /legacy/);
});
test("paused intent and absent end remain explicit without starting platform execution", () => {
  const plan = fixture();
  plan.flows[0].schedule.enabled = false;
  delete plan.flows[0].schedule.end;
  delete plan.scheduleBoundaries.source.end;
  const { jobs, manifest } = capture(plan);
  for (const job of Object.values(jobs)) {
    assert.equal(job.schedule.pause_status, "PAUSED");
    assert.equal(
      job.tasks.some((t) => t.task_key === "schedule_end"),
      false,
    );
  }
  assert.equal(manifest[0].status, "not-created");
  assert.equal(manifest[0].endExclusive, null);
});
test("generated condition and selective pipeline task fields exist in the pinned native bundle schema", () => {
  const schema = JSON.parse(
    readFileSync("src/schemas/databricks-bundle.json", "utf8"),
  );
  const jobs =
    schema.$defs["github.com/databricks/databricks-sdk-go/service/jobs"];
  // The pinned upstream schema retains qualified dotted definition keys under package namespaces.
  const defs =
    schema.$defs["github.com"]?.databricks?.["databricks-sdk-go"]?.service ??
    jobs;
  assert.ok(defs);
  const pipeline = defs["jobs.PipelineTask"].oneOf.find(
      (branch) => branch.properties,
    ),
    condition = defs["jobs.ConditionTask"].oneOf.find(
      (branch) => branch.properties,
    );
  assert.ok(pipeline.properties.refresh_selection);
  assert.ok(condition.properties.op);
  for (const job of Object.values(capture(fixture()).jobs))
    for (const t of job.tasks) {
      if (t.pipeline_task)
        for (const key of Object.keys(t.pipeline_task))
          assert.ok(Object.hasOwn(pipeline.properties, key), key);
      if (t.condition_task)
        for (const key of Object.keys(t.condition_task))
          assert.ok(Object.hasOwn(condition.properties, key), key);
    }
});
test("global freshness notebook retains executable content after flow-scoped extraction", () => {
  const plan = fixture();
  plan.flows = [];
  plan.nodes = [plan.nodes[1]];
  plan.nodes[0].needs = [];
  plan.nodes[0].with.freshness = { column: "observed_at", maximumAgeHours: 24 };
  const files = new Map();
  renderResources(
    plan,
    new Map([
      [
        "primary",
        { binding: "dbx", sources: [], needs: new Set(), quality: [] },
      ],
    ]),
    (path, format, value) => files.set(path, { format, value }),
  );
  const notebook = files.get("operations/freshness.ipynb").value;
  assert.equal(notebook.nbformat, 4);
  assert.equal(notebook.cells.length, 2);
  assert.match(notebook.cells[1].source.join(""), /MAX\(`observed_at`\)/);
});
