/** File temporal semantics belong to this native provider, with no implicit timezone. */
import { outputName } from "./column-mapping.mjs";
const ident = (name) => {
  if (typeof name !== "string" || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))
    throw Error("Invalid temporal source column");
  return "`" + name + "`";
};
const temporal = (column) => ["DATE", "TIMESTAMP"].includes(column.type);
const nativeType = (column) =>
  column.type === "TIMESTAMP" ? "TIMESTAMP_NTZ" : column.type;
const validText = (column) => {
  const name = ident(column.name),
    type = nativeType(column);
  const pattern =
    column.type === "DATE"
      ? "[0-9]{4}-[0-9]{2}-[0-9]{2}"
      : "[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?";
  return `regexp_like(${name}, r'^${pattern}\\z') AND substring(${name}, 1, 4) BETWEEN '0001' AND '9999' AND try_cast(${name} AS ${type}) IS NOT NULL`;
};
const message =
  "Source temporal value requires an exact calendar date or timezone-less timestamp with at most six fractional digits; review the source or contract mapping";
export function fileTemporal(columns, format, standard) {
  const fields = columns.filter(temporal),
    parquet = format === "parquet";
  if (parquet && fields.length && standard !== "snapshot-with-history@v1")
    throw Error(
      "Streamed Parquet temporal fields require a verified per-file timezone and precision policy; use reviewed complete snapshots or a supported text format",
    );
  const expression = (column) => {
    if (!temporal(column) || parquet) return ident(column.name);
    const name = ident(column.name);
    return `CASE WHEN ${name} IS NOT NULL AND NOT (${validText(column)}) THEN cast(raise_error('${message}') AS ${nativeType(column)}) ELSE try_cast(${name} AS ${nativeType(column)}) END`;
  };
  return {
    schema: columns
      .map(
        (column) =>
          `${ident(column.name)} ${temporal(column) && !parquet ? "STRING" : nativeType(column)}`,
      )
      .join(", "),
    columns: columns
      .map((column) => {
        const value = expression(column),
          target = outputName(column);
        return value === ident(target) ? value : `${value} AS ${ident(target)}`;
      })
      .join(", "),
    projection:
      fields.length ||
      columns.some((column) => outputName(column) !== column.name)
        ? columns
            .map(
              (column) =>
                `${temporal(column) && !parquet ? "F.expr(" + JSON.stringify(expression(column)) + ")" : "F.col(" + JSON.stringify(column.name) + ")"}.alias(${JSON.stringify(outputName(column))})`,
            )
            .join(", ")
        : "",
    invalid: fields
      .map((column) =>
        parquet
          ? `${ident(column.name)} IS NOT NULL AND NOT (year(${ident(column.name)}) BETWEEN 1 AND 9999)`
          : `${ident(column.name)} IS NOT NULL AND NOT (${validText(column)})`,
      )
      .join(" OR "),
    parquet: parquet
      ? fields.map((column) => ({
          name: column.name,
          type: column.type === "DATE" ? "date" : "timestamp_ntz",
        }))
      : [],
    message,
  };
}

/** Invalid temporal rule values cannot match a row, matching portable quality. */
export function temporalLiteral(type, value) {
  if (typeof value !== "string" || value.length > 26) return undefined;
  const date = /^(\d{4})-(\d{2})-(\d{2})/.exec(value);
  if (!date) return undefined;
  const [year, month, day] = date.slice(1).map(Number);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const maximum = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][
    month - 1
  ];
  if (year < 1 || month < 1 || month > 12 || day < 1 || day > maximum)
    return undefined;
  if (type === "DATE")
    return value.length === 10 ? "DATE '" + value + "'" : undefined;
  if (type !== "TIMESTAMP") return undefined;
  const time =
    /^(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2}):(\d{2})([.][0-9]{1,6})?$/.exec(
      value,
    );
  if (
    !time ||
    time[0].length !== value.length ||
    Number(time[2]) > 23 ||
    Number(time[3]) > 59 ||
    Number(time[4]) > 59
  )
    return undefined;
  return "TIMESTAMP_NTZ '" + value.replace("T", " ") + "'";
}
