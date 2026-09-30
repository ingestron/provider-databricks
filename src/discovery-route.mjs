/** Metadata discovery for native database routes (PB-064 phase 6).
 *
 * When the source is not reachable from the machine running Ingestron,
 * Databricks reads its catalogue through a Lakehouse Federation foreign
 * catalog over the same Unity Catalog connection. The generated query runs on
 * a SQL warehouse; its exported rows return through
 * `ingestron discover --from`. SaaS and file routes are discovered through
 * their portable connectors instead.
 */
const fail = (message) => {
  throw new Error(message);
};
const identifier = (v) =>
  typeof v === "string" && /^[A-Za-z_][A-Za-z0-9_$-]{0,127}$/.test(v);
const literal = (v) => `'${String(v).replace(/'/g, "''")}'`;

export function discoveryRoute(input) {
  const { flow, source, tables, connectionBinding } = input;
  source?.kind === "uc-connection" ||
    fail(
      "Databricks metadata discovery covers database routes; discover SaaS and file sources through their portable connectors",
    );
  const catalog = connectionBinding?.foreignCatalog;
  identifier(catalog) ||
    fail(
      `Set foreignCatalog on the connection binding: a Lakehouse Federation catalog over connection ${source.connection} (CREATE FOREIGN CATALOG ... USING CONNECTION ${source.connection})`,
    );
  const entries = Object.values(tables ?? {});
  (entries.length > 0 &&
    entries.every((t) => identifier(t.schema) && identifier(t.table))) ||
    fail("Each table needs a source schema and table");
  const schemas = [...new Set(entries.map((t) => t.schema))];
  const names = [...new Set(entries.map((t) => t.table))];
  const query = `-- Ingestron metadata discovery for flow ${flow}; read-only.
SELECT c.table_schema AS schema_name, c.table_name, c.column_name, c.ordinal_position,
  c.data_type, c.numeric_precision, c.numeric_scale, c.is_nullable AS nullable
FROM \`${catalog}\`.information_schema.columns AS c
WHERE c.table_schema IN (${schemas.map(literal).join(", ")})
  AND c.table_name IN (${names.map(literal).join(", ")})
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
`;
  return {
    apiVersion: "ingestron.artifact-proposal/v1",
    artifacts: {
      "discovery.sql": query,
      "README.md": `# Metadata discovery for ${flow}\n\nRun \`discovery.sql\` on a SQL warehouse with USE CATALOG on \`${catalog}\` and read access to the listed tables. Download the result as JSON (an array of rows) and pass it to \`ingestron discover --flow ${flow} --from <file>\`. The query reads catalogue metadata only. Primary keys are not exposed through foreign catalogs; mark them in review.\n`,
    },
    review: [
      "Foreign catalog names and column types follow Lakehouse Federation's mapping of the source",
      "Primary keys are not in the export; accept keys during review",
    ],
    next: `Run build/discovery/${flow}/discovery.sql on a SQL warehouse and download the rows as JSON.`,
    deployed: false,
  };
}
