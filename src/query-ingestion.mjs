/** Lakeflow Connect query-based ingestion (PB-064 phase 2).
 *
 * Reads a database through a Unity Catalog connection by querying source tables
 * with a cursor column: no gateway, serverless by default. Generates a pipeline
 * resource with an `ingestion_definition`; Databricks performs the reads.
 * Supported sources for foreign connection ingestion (Microsoft Learn,
 * 2026-09-18): Oracle, Teradata, SQL Server, MySQL, MariaDB and PostgreSQL.
 */
const fail = (message) => {
  throw new Error(message);
};
const identifier = (v) =>
  typeof v === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(v);
const name = (v) =>
  typeof v === "string" &&
  v.length > 0 &&
  v.length <= 255 &&
  !/[\n\r`]/.test(v);
export const queryStandard = "query-ingestion@v1";
const histories = ["SCD_TYPE_1", "SCD_TYPE_2", "APPEND_ONLY"];

export function expandQueryIngestion(flow, providerSource, columnsByTable) {
  const config = flow.ingestion ?? {};
  for (const key of Object.keys(config))
    ["standard", "pipeline", "target", "history"].includes(key) ||
      fail(`Unsupported query ingestion setting ${key}`);
  (typeof config.pipeline === "string" &&
    /^[A-Za-z0-9_-]+$/.test(config.pipeline)) ||
    fail("A logical pipeline alias is required");
  (config.target &&
    Object.keys(config.target).every((k) => k === "schema") &&
    identifier(config.target.schema)) ||
    fail("Query ingestion target needs only a destination schema");
  const history = config.history ?? "SCD_TYPE_1";
  histories.includes(history) ||
    fail(`history must be one of ${histories.join(", ")}`);
  Object.keys(flow.defaults?.with ?? {}).length &&
    fail("Ingestion standards own their processing");
  const steps = Object.entries(flow.tables ?? {}).map(([table, value]) => {
    const columns = columnsByTable[table] ?? [];
    const options = value.ingestion ?? {};
    for (const key of Object.keys(options))
      key === "cursor" ||
        fail(`${table}: query ingestion tables accept ingestion.cursor only`);
    Object.keys(value.steps ?? {}).length &&
      fail(`${table}: use table.ingestion settings, not step overrides`);
    const cursor = options.cursor;
    cursor === undefined ||
      columns.some((c) => c.name === cursor) ||
      fail(`${table}: unknown cursor column ${cursor}`);
    const keys = columns.filter((c) => c.key).map((c) => c.name);
    history === "APPEND_ONLY" ||
      keys.length ||
      fail(`${table}: ${history} needs primary keys in the contract`);
    return {
      id: `ingest_${table}`,
      uses: "lakeflow-ingest@v1",
      select: [table],
      with: {
        standard: queryStandard,
        pipeline: config.pipeline,
        target: config.target,
        history,
        keys,
        ...(cursor ? { cursor } : {}),
      },
    };
  });
  return {
    steps,
    recovery: {
      standard: queryStandard,
      provider: providerSource,
      capture: "query",
      replaySource: "source-database",
      retention: "source-owned",
      actualCompleteness: "unverified",
      detail:
        "Lakeflow Connect keeps the cursor high-water mark; rows with a NULL cursor are not ingested, and without a cursor every run is a full load.",
      assumptions: [
        "The cursor column increases monotonically and is never set back.",
        "Serverless compute can reach the source database through the Unity Catalog connection.",
      ],
    },
  };
}

/** Checks one query-ingestion node's source during plan validation. */
export function validateQueryNode(n) {
  const s = n.source ?? {};
  for (const key of Object.keys(s))
    ["kind", "connection", "database", "schema", "table"].includes(key) ||
      fail(`Unsupported query source setting ${key}`);
  (s.kind === "uc-connection" &&
    name(s.connection) &&
    name(s.database) &&
    name(s.schema) &&
    name(s.table)) ||
    fail(
      "Query ingestion needs a Unity Catalog connection and source database, schema and table",
    );
}

/** The ingestion_definition object for one pipeline's query tables. */
export function ingestionDefinition(tables, catalog) {
  const connections = new Set(tables.map((t) => t.connection));
  connections.size === 1 ||
    fail(
      "One query ingestion pipeline reads through one Unity Catalog connection",
    );
  return {
    connection_name: [...connections][0],
    objects: tables.map((t) => ({
      table: {
        source_catalog: t.database,
        source_schema: t.schema,
        source_table: t.table,
        destination_catalog: catalog,
        destination_schema: t.destinationSchema,
        destination_table: t.destinationTable,
        table_configuration: {
          scd_type: t.history,
          ...(t.keys.length ? { primary_keys: t.keys } : {}),
          ...(t.cursor
            ? { query_based_connector_config: { cursor_columns: [t.cursor] } }
            : {}),
        },
      },
    })),
  };
}
