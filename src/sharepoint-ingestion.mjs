/** Lakeflow Connect managed SharePoint ingestion (PB-064 phase 2).
 *
 * Structured files in a SharePoint folder are parsed into one table each, with
 * column types from the reviewed contract as schema hints; a SharePoint list
 * becomes one table (list ingestion is Beta). Serverless only; the documented
 * bundle examples use the PREVIEW channel (Microsoft Learn, 2026-09-11).
 */
const fail = (message) => {
  throw new Error(message);
};
const identifier = (v) =>
  typeof v === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(v);
export const sharePointStandard = "sharepoint-ingestion@v1";
const formats = ["csv", "json", "excel", "parquet", "xml", "avro", "text"];

export function expandSharePointIngestion(flow, providerSource) {
  const config = flow.ingestion ?? {};
  for (const key of Object.keys(config))
    ["standard", "pipeline", "target"].includes(key) ||
      fail(`Unsupported SharePoint ingestion setting ${key}`);
  (typeof config.pipeline === "string" &&
    /^[A-Za-z0-9_-]+$/.test(config.pipeline)) ||
    fail("A logical pipeline alias is required");
  (config.target &&
    Object.keys(config.target).every((k) => k === "schema") &&
    identifier(config.target.schema)) ||
    fail("SharePoint ingestion target needs only a destination schema");
  Object.keys(flow.defaults?.with ?? {}).length &&
    fail("Ingestion standards own their processing");
  const steps = Object.entries(flow.tables ?? {}).map(([table, value]) => {
    (value.ingestion === undefined && !Object.keys(value.steps ?? {}).length) ||
      fail(`${table}: SharePoint ingestion has no table settings`);
    return {
      id: `ingest_${table}`,
      uses: "lakeflow-ingest@v1",
      select: [table],
      with: {
        standard: sharePointStandard,
        pipeline: config.pipeline,
        target: config.target,
        keys: [],
      },
    };
  });
  return {
    steps,
    recovery: {
      standard: sharePointStandard,
      provider: providerSource,
      capture: "managed-incremental",
      replaySource: "source-site",
      retention: "source-owned",
      actualCompleteness: "unverified",
      detail:
        "Lakeflow Connect tracks which files it has read; history tracking (SCD type 2) is not supported by this connector.",
      assumptions: [
        "The Unity Catalog connection has read access to the site through OAuth.",
      ],
    },
  };
}

/** Checks one SharePoint node's source during plan validation. */
export function validateSharePointNode(n) {
  const s = n.source ?? {};
  for (const key of Object.keys(s))
    ["kind", "connection", "site", "path", "entity", "format"].includes(key) ||
      fail(`Unsupported SharePoint source setting ${key}`);
  (s.kind === "sharepoint" &&
    typeof s.connection === "string" &&
    s.connection.length > 0) ||
    fail("SharePoint ingestion needs a Unity Catalog connection");
  (typeof s.site === "string" &&
    /^https:\/\/[^\s/]+\/sites\/[^\s]+[^/]$/.test(s.site)) ||
    fail("site must be an https SharePoint site URL without a trailing slash");
  (typeof s.path === "string" &&
    s.path.length > 0 &&
    !s.path.startsWith("/") &&
    !s.path.split("/").some((part) => part === ".." || part === "")) ||
    fail("path must be a relative folder or Lists/<name> inside the site");
  ["file", "list"].includes(s.entity ?? "file") ||
    fail("entity must be file or list");
  (s.entity ?? "file") === "list"
    ? s.format === undefined || fail("format applies to files only")
    : formats.includes(s.format) ||
      fail(`format must be one of ${formats.join(", ")}`);
}

/** Encodes each path segment once, whether or not the user already encoded it. */
const encodePath = (path) =>
  path
    .split("/")
    .map((segment) => encodeURIComponent(decodeURIComponent(segment)))
    .join("/");

/** Spark types for schema hints; contract types are already validated. */
const hint = (c) => "`" + c.name + "` " + c.type;

/** The Lakeflow Connect object for one SharePoint table. */
export function sharePointObject(s, columns, destination) {
  const list = (s.entity ?? "file") === "list";
  return {
    connection: s.connection,
    object: {
      table: {
        destination_catalog: destination.catalog,
        destination_schema: destination.schema,
        destination_table: destination.table,
        connector_options: {
          sharepoint_options: {
            entity_type: list ? "LIST" : "FILE",
            url: `${s.site}/${encodePath(s.path)}`,
            ...(list
              ? {}
              : {
                  file_ingestion_options: {
                    format: s.format.toUpperCase(),
                    schema_evolution_mode: "NONE",
                    schema_hints: columns.map(hint).join(", "),
                  },
                }),
          },
        },
      },
    },
  };
}
