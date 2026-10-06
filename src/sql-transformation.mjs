import { validateSqlSource } from "../plugin/sql-parser.mjs";
import { outputColumns } from "./column-mapping.mjs";
import { qualityChecks } from "./quality.mjs";
import { transformationContract } from "./transformation.mjs";

export { validateSqlSource };
export function sqlTransformation(plan, node) {
  const query = validateSqlSource(
    node.sql,
    Object.keys(node.with.sources ?? {}),
  );
  const contract = transformationContract(plan, node);
  const columns = outputColumns(node.columns);
  if (!columns.length)
    throw Error("SQL transformations require typed output columns");
  const quality = qualityChecks(
    contract,
    columns,
    "query-ingestion@v1",
    "output",
  );
  return {
    query,
    columns: columns
      .map((column) => `\`${column.name}\` ${column.type}`)
      .join(", "),
    qualityNode: { ...node, columns, with: { ...node.with, quality } },
  };
}
