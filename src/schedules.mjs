/** Flow-owned native Jobs calendars and dependency selections. No workspace calls. */
const check = (ok, message) => {
  if (!ok) throw Error(message);
};
const object = (v) => v && typeof v === "object" && !Array.isArray(v);
const compare = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
const identifier = (v) =>
  typeof v === "string" && /^[A-Za-z_][A-Za-z0-9_-]{0,100}$/.test(v);
// Calendar arithmetic is deterministic: provider isolation deliberately removes Date.
const utc = (value) => {
  if (
    typeof value !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(value)
  )
    return null;
  const [year, month, day, hour, minute, second] = value
    .match(/\d+/g)
    .map(Number);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const months = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > months[month - 1] ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  )
    return null;
  const days =
    365 * year +
    Math.floor((year + 3) / 4) -
    Math.floor((year + 99) / 100) +
    Math.floor((year + 399) / 400) -
    719528 +
    months.slice(0, month - 1).reduce((a, b) => a + b, 0) +
    day -
    1;
  return {
    hour,
    minute,
    second,
    milliseconds: ((days * 24 + hour) * 60 * 60 + minute * 60 + second) * 1000,
  };
};
const instant = (value) => utc(value) !== null;
const weekday = {
  Monday: "MON",
  Tuesday: "TUE",
  Wednesday: "WED",
  Thursday: "THU",
  Friday: "FRI",
  Saturday: "SAT",
  Sunday: "SUN",
};
export const selectiveRefreshSchema = {
  type: "string",
  enum: ["native-beta"],
  "x-ingestron-ui": {
    section: "schedule",
    label: "Shared pipeline refresh",
    enumLabels: { "native-beta": "Selected tables · native beta" },
  },
  description:
    "Explicitly reviewed Jobs pipeline-task selective refresh (native beta). Required when a scheduled dependency closure uses only part of a shared pipeline. Validate workspace availability and native exclusions before activation.",
};
export function nativeCalendar(schedule) {
  check(
    object(schedule) &&
      Object.keys(schedule).every((k) =>
        [
          "apiVersion",
          "enabled",
          "timeZone",
          "start",
          "end",
          "recurrence",
          "overlap",
          "missed",
          "daylightSaving",
        ].includes(k),
      ),
    "Unsupported Databricks schedule fields",
  );
  check(
    schedule.apiVersion === "ingestron.schedule/v1" &&
      typeof schedule.enabled === "boolean" &&
      instant(schedule.start) &&
      (!schedule.end ||
        (instant(schedule.end) &&
          utc(schedule.end).milliseconds > utc(schedule.start).milliseconds)),
    "Use a reviewed schedule with exact UTC start/exclusive end",
  );
  check(
    schedule.overlap === "native" &&
      schedule.missed === "native" &&
      !schedule.daylightSaving,
    "Databricks schedules require explicitly native clock, overlap and missed-run policies",
  );
  check(
    typeof schedule.timeZone === "string" &&
      schedule.timeZone.length >= 1 &&
      schedule.timeZone.length <= 128,
    "Use the reviewed IANA time zone",
  );
  const r = schedule.recurrence;
  check(object(r), "Choose a schedule recurrence");
  const result = [];
  if (r.kind === "interval") {
    check(
      Object.keys(r).every((k) => ["kind", "minutes"].includes(k)) &&
        Number.isInteger(r.minutes) &&
        r.minutes >= 1 &&
        r.minutes <= 1440 &&
        1440 % r.minutes === 0,
      "Native Databricks elapsed intervals must divide a UTC day exactly; other phases require portable dispatch, never a month-resetting cron approximation",
    );
    check(
      ["UTC", "Etc/UTC", "Etc/GMT", "GMT"].includes(schedule.timeZone),
      "Native elapsed intervals require UTC to preserve elapsed time across daylight saving",
    );
    const start = utc(schedule.start),
      phase = (start.hour * 60 + start.minute) % r.minutes;
    const minutes = new Map();
    for (let slot = phase; slot < 1440; slot += r.minutes) {
      const minute = slot % 60;
      minutes.set(minute, [
        ...(minutes.get(minute) ?? []),
        Math.floor(slot / 60),
      ]);
    }
    const groups = new Map();
    for (const [minute, hours] of minutes) {
      const key = hours.join(",");
      groups.set(key, [...(groups.get(key) ?? []), minute]);
    }
    for (const [hours, mins] of groups)
      result.push({
        cron: `${start.second} ${mins.sort((a, b) => a - b).join(",")} ${hours} * * ?`,
        second: start.second,
      });
  } else {
    const allowed =
      r.kind === "daily"
        ? ["kind", "times"]
        : r.kind === "weekly"
          ? ["kind", "times", "weekDays"]
          : r.kind === "monthly"
            ? ["kind", "times", "monthDays"]
            : [];
    check(
      allowed.length && Object.keys(r).every((k) => allowed.includes(k)),
      "Unsupported Databricks recurrence fields",
    );
    check(
      Array.isArray(r.times) &&
        r.times.length >= 1 &&
        r.times.length <= 100 &&
        new Set(r.times).size === r.times.length &&
        r.times.every(
          (t) => typeof t === "string" && /^([01]\d|2[0-3]):[0-5]\d$/.test(t),
        ),
      "Review one to 100 unique paired calendar slots",
    );
    let day = "*",
      week = "?";
    if (r.kind === "weekly") {
      check(
        Array.isArray(r.weekDays) &&
          r.weekDays.length >= 1 &&
          new Set(r.weekDays).size === r.weekDays.length &&
          r.weekDays.every((d) => weekday[d]),
        "Review valid unique weekdays",
      );
      day = "?";
      week = r.weekDays.map((d) => weekday[d]).join(",");
    } else if (r.kind === "monthly") {
      check(
        Array.isArray(r.monthDays) &&
          r.monthDays.length >= 1 &&
          new Set(r.monthDays).size === r.monthDays.length &&
          r.monthDays.every((d) => Number.isInteger(d) && d >= 1 && d <= 31),
        "Review valid unique month days; missing dates are skipped",
      );
      day = [...r.monthDays].sort((a, b) => a - b).join(",");
    }
    for (const time of [...r.times].sort(compare)) {
      const [hour, minute] = time.split(":").map(Number);
      result.push({ cron: `0 ${minute} ${hour} ${day} * ${week}`, second: 0 });
    }
  }
  check(
    result.length > 0 && result.length <= 100,
    "Native schedule exceeds 100 calendar groups",
  );
  return result;
}
export function scheduledJobNames(project, environment, flow, schedule) {
  check(
    [project, environment, flow].every(identifier),
    "Use safe native scheduled job identities",
  );
  return nativeCalendar(schedule).map((entry, index) => ({
    ...entry,
    key: `schedule_${flow}_${index + 1}`,
    name: `${environment}_${project}_${flow}_schedule_${index + 1}`,
  }));
}
const pipeline = (n) => n.with.pipeline ?? n.with.pipelineId;
const targets = (plan, n) =>
  n.uses.startsWith("materialized-view")
    ? [n.with.targetTable]
    : [
        [plan.bindings[n.binding].catalog, n.with.schema, n.with.table].join(
          ".",
        ),
        ...(n.with.target?.historySchema
          ? [
              [
                plan.bindings[n.binding].catalog,
                n.with.target.historySchema,
                n.with.table,
              ].join("."),
            ]
          : []),
      ];
export function scheduledClosures(plan) {
  const schedules = [];
  for (const flow of plan.flows ?? []) {
    if (!flow.schedule) continue;
    check(
      !plan.selection?.table && !plan.selection?.step,
      "Native schedules require complete flow selection",
    );
    const direct = plan.nodes.filter((n) => n.flow === flow.id),
      tables = Object.keys(flow.tables ?? {}).sort(compare);
    check(
      direct.length > 0 &&
        tables.every((t) => direct.some((n) => n.table === t)),
      "Native schedules require every reviewed table",
    );
    check(
      direct.every((n) =>
        ["lakeflow-ingest", "materialized-view"].includes(n.uses.split("@")[0]),
      ),
      "This native calendar requires declarative pipelines; publication and connector schedules need their own bounded job preparation",
    );
    const metadata = plan.scheduleBoundaries?.[flow.id];
    check(
      metadata?.apiVersion === "ingestron.schedule-boundaries/v1" &&
        metadata.timeZone === flow.schedule.timeZone &&
        /^[a-f0-9]{64}$/.test(metadata.definitionDigest ?? "") &&
        metadata.start?.instant === flow.schedule.start &&
        metadata.end?.instant === flow.schedule.end,
      "Native schedules require compiler-resolved exact boundary metadata",
    );
    const nodes = new Map(),
      active = new Set();
    const visit = (n) => {
      check(!active.has(n.id), "Scheduled dependency closure is cyclic");
      if (nodes.has(n.id)) return;
      active.add(n.id);
      for (const id of n.needs ?? []) {
        const parent = plan.nodes.find((p) => p.id === id);
        check(parent, "Missing scheduled upstream dependency " + id);
        visit(parent);
      }
      active.delete(n.id);
      nodes.set(n.id, n);
    };
    direct.forEach(visit);
    const selected = [...nodes.values()];
    check(
      selected.every((n) =>
        ["lakeflow-ingest", "materialized-view"].includes(n.uses.split("@")[0]),
      ),
      "Scheduled dependencies must use declarative pipelines",
    );
    check(
      selected.length <= 1000,
      "Scheduled dependency closure exceeds native task limits",
    );
    const aliases = [...new Set(selected.map(pipeline))].sort(compare),
      groups = [];
    for (const alias of aliases) {
      const selectedNodes = selected.filter((n) => pipeline(n) === alias),
        allNodes = plan.nodes.filter((n) => pipeline(n) === alias);
      const mixed = allNodes.some((n) => !nodes.has(n.id));
      const beta =
        selectedNodes[0].runtime?.options?.deployment?.job?.selectiveRefresh;
      check(
        !mixed || beta === "native-beta",
        "Shared pipeline " +
          alias +
          " includes unrelated members; review deployment.job.selectiveRefresh: native-beta to generate an exact native selection",
      );
      const selection = [
        ...new Set(selectedNodes.flatMap((n) => targets(plan, n))),
      ].sort(compare);
      check(
        selection.length > 0 &&
          selection.every(
            (s) =>
              typeof s === "string" &&
              /^[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*$/.test(s),
          ),
        "Scheduled pipeline selection needs exact qualified targets",
      );
      groups.push({ alias, nodes: selectedNodes, selective: mixed, selection });
    }
    schedules.push({
      flow,
      groups,
      nodes: selected,
      calendars: scheduledJobNames(
        plan.project,
        plan.environment,
        flow.id,
        flow.schedule,
      ),
      metadata,
    });
  }
  return schedules;
}
export function renderScheduledJobs(
  plan,
  pipelines,
  ids,
  baseJob,
  put,
  quality,
  freshness,
) {
  const entries = scheduledClosures(plan);
  if (!entries.length) return;
  check(
    !plan.nodes[0].runtime.options.deployment?.job?.schedule,
    "Do not combine flow-owned schedules with the legacy export-wide job schedule",
  );
  const task = (alias) => "refresh_" + alias.replaceAll("-", "_"),
    qualityTask = (alias) => "quality_" + alias.replaceAll("-", "_");
  const jobs = {},
    manifest = [];
  for (const entry of entries) {
    const aliases = new Set(entry.groups.map((g) => g.alias));
    check(
      new Set([...aliases].map(task)).size === aliases.size,
      "Scheduled pipeline task keys collide",
    );
    const selectedQuality = new Map();
    for (const group of entry.groups) {
      const nodes = (pipelines.get(group.alias).quality ?? []).filter((n) =>
        entry.nodes.some((x) => x.id === n.id),
      );
      if (nodes.length) {
        const key = `schedule_${entry.flow.id}_${qualityTask(group.alias)}`;
        put(`operations/${key}.ipynb`, "json", quality(plan, nodes));
        selectedQuality.set(group.alias, key);
      }
    }
    const completion = (alias) =>
      selectedQuality.has(alias) ? qualityTask(alias) : task(alias);
    const freshNodes = entry.nodes.filter((n) => n.with.freshness);
    const freshKey = `schedule_${entry.flow.id}_freshness`;
    if (freshNodes.length)
      put(`operations/${freshKey}.ipynb`, "json", freshness(freshNodes));
    for (const calendar of entry.calendars) {
      check(!jobs[calendar.key], "Scheduled job keys collide");
      const gates = [
        {
          task_key: "schedule_trigger",
          condition_task: {
            op: "EQUAL_TO",
            left: "{{job.trigger.type}}",
            right: "periodic",
          },
        },
      ];
      const lower = String(
        utc(entry.flow.schedule.start).milliseconds - calendar.second * 1000,
      );
      gates.push({
        task_key: "schedule_start",
        depends_on: [{ task_key: "schedule_trigger", outcome: "true" }],
        condition_task: {
          op: "GREATER_THAN_OR_EQUAL",
          left: "{{job.trigger.time.timestamp_ms}}",
          right: lower,
        },
      });
      let guard = "schedule_start";
      if (entry.flow.schedule.end) {
        guard = "schedule_end";
        gates.push({
          task_key: guard,
          depends_on: [{ task_key: "schedule_start", outcome: "true" }],
          condition_task: {
            op: "LESS_THAN",
            left: "{{job.trigger.time.timestamp_ms}}",
            right: String(
              utc(entry.flow.schedule.end).milliseconds -
                calendar.second * 1000,
            ),
          },
        });
      }
      const gate = { task_key: guard, outcome: "true" };
      const tasks = [
        ...gates,
        ...entry.groups.map((group) => {
          const deps = [
            ...new Set(
              group.nodes.flatMap((n) =>
                (n.needs ?? [])
                  .map((id) => pipeline(plan.nodes.find((p) => p.id === id)))
                  .filter((p) => p !== group.alias),
              ),
            ),
          ];
          check(
            deps.every((p) => aliases.has(p)),
            "Scheduled cross-pipeline dependency is outside its closure",
          );
          return {
            task_key: task(group.alias),
            pipeline_task: {
              pipeline_id: ids.get(group.alias),
              full_refresh: false,
              ...(group.selective
                ? { refresh_selection: group.selection }
                : {}),
            },
            max_retries:
              baseJob.tasks.find((t) => t.task_key === task(group.alias))
                ?.max_retries ?? 0,
            min_retry_interval_millis:
              baseJob.tasks.find((t) => t.task_key === task(group.alias))
                ?.min_retry_interval_millis ?? 0,
            depends_on: [
              gate,
              ...deps.map((p) => ({ task_key: completion(p) })),
            ],
          };
        }),
        ...entry.groups
          .filter((g) => selectedQuality.has(g.alias))
          .map((g) => ({
            task_key: qualityTask(g.alias),
            notebook_task: {
              notebook_path: `../operations/${selectedQuality.get(g.alias)}.ipynb`,
            },
            depends_on: [gate, { task_key: task(g.alias) }],
            run_if: "ALL_SUCCESS",
            max_retries: 0,
          })),
        ...(freshNodes.length
          ? [
              {
                task_key: "check_freshness",
                notebook_task: {
                  notebook_path: `../operations/${freshKey}.ipynb`,
                },
                depends_on: [
                  gate,
                  ...entry.groups.map((g) => ({
                    task_key: completion(g.alias),
                  })),
                ],
                max_retries: 0,
              },
            ]
          : []),
      ];
      check(tasks.length <= 100, "Scheduled job exceeds 100 native tasks");
      jobs[calendar.key] = {
        ...baseJob,
        name: calendar.name,
        schedule: {
          quartz_cron_expression: calendar.cron,
          timezone_id: entry.flow.schedule.timeZone,
          pause_status: entry.flow.schedule.enabled ? "UNPAUSED" : "PAUSED",
        },
        tasks,
      };
      manifest.push({
        flow: entry.flow.id,
        job: calendar.key,
        name: calendar.name,
        definition: entry.flow.schedule,
        cron: calendar.cron,
        triggerTimeNormalizationSeconds: calendar.second,
        startInclusive: entry.flow.schedule.start,
        endExclusive: entry.flow.schedule.end ?? null,
        clock: "platform-native",
        status: "not-created",
        activationIntent: entry.flow.schedule.enabled ? "UNPAUSED" : "PAUSED",
        tables: Object.keys(entry.flow.tables ?? {}).sort(compare),
        nodes: entry.nodes.map((n) => n.id).sort(compare),
        pipelines: entry.groups.map((g) => ({
          alias: g.alias,
          refreshSelection: g.selective ? g.selection : null,
          launchStage: g.selective ? "native-beta" : "native-ga",
        })),
        guardTasks: gates.map((t) => t.task_key),
      });
    }
  }
  put("resources/schedules.job.yml", "yaml", { resources: { jobs } });
  put("databricks-schedules.json", "json", {
    apiVersion: "ingestron.databricks-schedules/v1",
    schedules: manifest,
  });
}
