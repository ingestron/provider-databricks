/** File ingestion validates and publishes the projected contract schema. */
const reserved = new Set(["__start_at", "__end_at", "_rescued_data"]);
export const outputName = (column) => column.target ?? column.name;
export function outputColumns(columns) {
  const source = new Set(),
    target = new Set();
  return columns.map((column) => {
    for (const [name, names, boundary] of [
      [column.name, source, "source"],
      [outputName(column), target, "output"],
    ]) {
      if (
        typeof name !== "string" ||
        !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ||
        reserved.has(name.toLowerCase()) ||
        names.has(name.toLowerCase())
      )
        throw new Error(
          `Invalid, reserved or duplicate ${boundary} column ${name}`,
        );
      names.add(name.toLowerCase());
    }
    const { target: _, ...value } = column;
    return { ...value, name: outputName(column) };
  });
}
export function outputSetting(columns, source) {
  const column = columns.find((c) => c.name === source);
  if (!column) throw new Error(`Unknown physical source column ${source}`);
  return outputName(column);
}
export function publishedContract(contract) {
  const value = JSON.parse(JSON.stringify(contract));
  for (const object of value.schema ?? [])
    for (const property of object.properties ?? [])
      delete property.physicalName;
  return value;
}
