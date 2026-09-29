import { snapshotPolicySchema } from "./snapshot-policy.js";
import { qualityChecks } from "./quality.mjs";
import { z } from "zod";
const check = (ok: unknown, _code: string, message: string): asserts ok => {
  if (!ok) throw new Error(message);
};

type Flow = any;
type Step = any;
const identifier = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]*$/);
const tableIngestionSchema = z
  .object({
    freshness: z
      .object({ column: identifier, maximumAgeHours: z.number().positive() })
      .strict()
      .optional(),
    snapshotPolicy: snapshotPolicySchema.optional(),
    expectations: z.record(identifier, z.string().min(1)).optional(),
    keys: z.array(identifier).min(1).optional(),
    sequenceBy: z.array(identifier).min(1).max(1).optional(),
    operationColumn: identifier.optional(),
    trackedColumns: z.array(identifier).min(1).optional(),
  })
  .strict()
  .optional();
export const ingestionSchema = z
  .object({
    standard: z.enum([
      "snapshot-with-history@v1",
      "change-feed-with-history@v1",
      "append-only@v1",
    ]),
    snapshotPolicy: snapshotPolicySchema.optional(),
    pipeline: z.string().regex(/^[A-Za-z0-9_-]+$/),
    target: z
      .object({ schema: identifier, historySchema: identifier.optional() })
      .strict(),
    source: z
      .object({
        delivery: z.enum([
          "complete-snapshot",
          "change-events",
          "immutable-files",
        ]),
        scope: z.literal("full-table").optional(),
        deletes: z.enum(["missing-keys", "explicit-events", "not-applicable"]),
        baseline: z.literal("included").optional(),
      })
      .strict(),
    retention: z
      .object({
        sourceDeliveries: z.enum(["externally-retained", "not-retained"]),
        period: z
          .string()
          .regex(/^P[1-9][0-9]*D$/)
          .optional(),
      })
      .strict(),
  })
  .strict();
export const ingestionStandards = [
  {
    id: "snapshot-with-history@v1",
    provider: "builtin:databricks-native",
    delivery: "complete-snapshot",
    description:
      "Complete versioned snapshots → native SCD2 and current view; missing keys expire",
    method: "AUTO CDC FROM SNAPSHOT",
    evidence: "offline",
  },
  {
    id: "change-feed-with-history@v1",
    provider: "builtin:databricks-native",
    delivery: "change-events",
    description:
      "Baseline plus full-row change events → native SCD2 and current view",
    method: "AUTO CDC",
    evidence: "offline",
  },
  {
    id: "append-only@v1",
    provider: "builtin:databricks-native",
    delivery: "immutable-files",
    description: "Immutable files → accumulated streaming table",
    method: "Auto Loader",
    evidence: "offline",
  },
];
/** Databricks owns the catalogue. Other providers do not inherit these semantics. */
export function expandIngestion(
  flow: Flow,
  providerSource: string,
  columnsByTable: Record<string, any[]>,
) {
  check(
    flow.kind === "ingestion",
    "INGESTION",
    "Only ingestion flows select ingestion standards",
  );
  const parsed = ingestionSchema.safeParse(flow.ingestion);
  check(
    parsed.success,
    "INGESTION",
    parsed.success ? "" : parsed.error.message,
  );
  const config = parsed.data;
  const snapshot = config.standard === "snapshot-with-history@v1";
  const events = config.standard === "change-feed-with-history@v1";
  check(
    config.source.delivery ===
      (snapshot
        ? "complete-snapshot"
        : events
          ? "change-events"
          : "immutable-files"),
    "INGESTION",
    "Source delivery does not match the selected standard; incremental extracts are not CDC",
  );
  check(
    config.source.deletes ===
      (snapshot
        ? "missing-keys"
        : events
          ? "explicit-events"
          : "not-applicable"),
    "INGESTION",
    "Declare the standard's delete semantics explicitly",
  );
  check(
    snapshot ? config.source.scope === "full-table" : !config.source.scope,
    "INGESTION",
    "Snapshots require a consistently scoped full-table delivery; scope is snapshot-only",
  );
  check(
    events ? config.source.baseline === "included" : !config.source.baseline,
    "INGESTION",
    "Change feeds require an included baseline; baseline applies only to change feeds",
  );
  check(
    config.retention.sourceDeliveries === "externally-retained"
      ? !!config.retention.period
      : !config.retention.period,
    "INGESTION",
    "Externally retained deliveries require period (for example P90D); not-retained must omit period",
  );
  check(
    snapshot || events
      ? !!config.target.historySchema &&
          config.target.historySchema !== config.target.schema
      : !config.target.historySchema,
    "INGESTION",
    "History standards require a distinct historySchema; append-only must omit it",
  );
  check(
    !Object.keys(flow.defaults.with).length,
    "INGESTION",
    "Ingestion standards own their processing; use a separate transformation flow instead of step extensions/defaults.with",
  );
  const steps: Step[] = [];
  for (const [table, value] of Object.entries<any>(flow.tables ?? {})) {
    const columns = columnsByTable[table];
    const options = tableIngestionSchema.parse(value.ingestion);
    check(
      !Object.keys(value.steps).length,
      "INGESTION",
      `${table}: use table.ingestion settings, not history or step overrides`,
    );
    const keys =
      options?.keys ?? columns.filter((c) => c.key).map((c) => c.name);
    check(
      !(snapshot || events) || keys.length > 0,
      "INGESTION",
      `${table}: declare keys in table.ingestion.keys or the contract`,
    );
    for (const name of [
      ...keys,
      ...(options?.sequenceBy ?? []),
      ...(options?.trackedColumns ?? []),
      ...(options?.operationColumn ? [options.operationColumn] : []),
    ])
      check(
        columns.some((c) => c.name === name),
        "INGESTION",
        `${table}: unknown contract column ${name}`,
      );
    check(
      !columns.some((c) =>
        ["__START_AT", "__END_AT", "_rescued_data"].includes(c.name),
      ),
      "INGESTION",
      `${table}: contract uses a reserved pipeline column`,
    );
    check(
      events
        ? !!options?.sequenceBy?.length && !!options.operationColumn
        : !options?.sequenceBy && !options?.operationColumn,
      "INGESTION",
      `${table}: sequenceBy and operationColumn are required only for change feeds`,
    );
    check(
      snapshot || events || !options?.trackedColumns,
      "INGESTION",
      `${table}: trackedColumns requires history`,
    );
    if (events) {
      check(
        !keys.includes(options!.sequenceBy![0]) &&
          !options?.trackedColumns?.includes(options!.sequenceBy![0]),
        "INGESTION",
        `${table}: sequencing metadata cannot be a business key or tracked column`,
      );
      check(
        columns.find((c) => c.name === options!.operationColumn)!.type ===
          "STRING",
        "INGESTION",
        `${table}: operationColumn must be a STRING with I/U/D values`,
      );
      const sequenceType = columns.find(
        (c) => c.name === options!.sequenceBy![0],
      )!.type;
      check(
        !["BOOLEAN", "BINARY"].includes(sequenceType),
        "INGESTION",
        `${table}: sequence column must have a sortable numeric, date, timestamp or string type`,
      );
    }
    if (events)
      check(
        ![
          ...keys,
          ...(options?.sequenceBy ?? []),
          ...(options?.trackedColumns ?? []),
        ].includes(options!.operationColumn!),
        "INGESTION",
        `${table}: operationColumn cannot be a key, sequence or tracked history column`,
      );
    check(
      snapshot || (!config.snapshotPolicy && !options?.snapshotPolicy),
      "INGESTION",
      `${table}: snapshotPolicy applies only to complete snapshots`,
    );
    check(
      !snapshot || typeof value.contract.version === "string",
      "INGESTION",
      `${table}: snapshots require a contract version`,
    );
    if (options?.freshness) {
      const column = columns.find((c) => c.name === options.freshness!.column);
      check(
        column &&
          ["TIMESTAMP", "DATE"].includes(column.type) &&
          !options.sequenceBy?.includes(column.name) &&
          options.operationColumn !== column.name,
        "INGESTION",
        `${table}: freshness requires a published DATE or TIMESTAMP column`,
      );
    }
    const quality = qualityChecks(value.contract, columns, config.standard);
    steps.push({
      id: `ingest_${table}`,
      uses: "lakeflow-ingest@v1",
      select: [table],
      with: {
        ...config,
        keys,
        ...options,
        ...(quality.length ? { quality } : {}),
        ...(snapshot
          ? {
              contractVersion: value.contract.version,
              snapshotPolicy: {
                ...config.snapshotPolicy,
                ...options?.snapshotPolicy,
              },
            }
          : {}),
      },
    });
  }
  return {
    steps,
    recovery: {
      standard: config.standard,
      provider: providerSource,
      capture: config.source.delivery,
      replaySource:
        config.retention.sourceDeliveries === "externally-retained"
          ? "external-deliveries"
          : "unavailable",
      retention: config.retention.period ?? "not-retained",
      actualCompleteness: "unverified",
      detail: snapshot
        ? "Only observed snapshots; changes between snapshots cannot be reconstructed."
        : events
          ? "Reconstruction requires the baseline and every change event, including deletes."
          : "Reconstruction requires all original immutable files.",
      assumptions: [
        "Source owner enforces delivery retention; the CLI does not archive or delete source data.",
        "Native state and checkpoints support normal retries, not unlimited historical reconstruction.",
        "One pipeline owns each destination; no concurrent external writers.",
      ],
    },
  };
}
