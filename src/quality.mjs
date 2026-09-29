/** ODCS v3.1 library quality rules mapped to native Lakeflow checks (PB-063 phase 4).
 *
 * Rule identities match @ingestron/core: explicit `id`, otherwise
 * `table[.column].metric`; `primaryKey: true` implies key-not-null and key-unique.
 * Streaming standards (append-only, change feeds) enforce row-level rules as
 * expectations. Complete snapshots evaluate every library rule on the delivered
 * snapshot before it is applied. SQL and engine rules are not generated here.
 */
const METRICS = [
  "nullValues",
  "missingValues",
  "invalidValues",
  "duplicateValues",
  "rowCount",
];
const OPERATORS = [
  "mustBe",
  "mustNotBe",
  "mustBeGreaterThan",
  "mustBeGreaterOrEqualTo",
  "mustBeLessThan",
  "mustBeLessOrEqualTo",
  "mustBeBetween",
  "mustNotBeBetween",
];
const ROW_LEVEL = new Set(["nullValues", "missingValues", "invalidValues"]);
// Contract rules of other types (sql, custom, text) are parsed below; only library
// metrics use METRICS.
const fail = (message) => {
  throw new Error(message);
};

/** Offline shape check; the platform compiles the SQL when the check runs. */
export function sqlShape(id, text, kind) {
  const stripped = String(text)
    .replace(/'(?:[^'\\]|\\.|'')*'/g, "''")
    .replace(/`[^`]*`/g, "``")
    .replace(/"[^"]*"/g, '""');
  if (/--|\/\*|;/.test(stripped))
    fail(`${id}: ${kind} must be one statement without comments or semicolons`);
  if (
    /\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE|CALL|USE|SET|OPTIMIZE|VACUUM|COPY|REFRESH|CACHE|UNCACHE|INTO|RESTORE|MSCK|LOAD)\b/i.test(
      stripped,
    )
  )
    fail(`${id}: ${kind} must only read data`);
  if (kind === "query" && !/^\s*(SELECT|WITH)\b/i.test(stripped))
    fail(`${id}: a sql rule query must start with SELECT or WITH`);
  if (kind === "predicate" && /\bSELECT\b/i.test(stripped))
    fail(`${id}: a databricks rule is a row predicate without subqueries`);
  return text;
}

function parse(rule, table, column) {
  if (!rule || typeof rule !== "object") return undefined;
  const type = rule.type ?? "library";
  const where = table + (column ? "." + column : "");
  const outcome = /^error$/i.test(String(rule.severity ?? ""))
    ? "fail"
    : "warn";
  if (type === "sql" || type === "custom") {
    const present = OPERATORS.filter((o) => rule[o] !== undefined);
    if (type === "sql" && present.length !== 1)
      fail(`${where}: a sql rule needs exactly one comparison`);
    return {
      id: String(rule.id ?? `${where}.${type === "sql" ? "sql" : rule.engine}`),
      table,
      ...(column ? { column } : {}),
      metric: type,
      ...(type === "sql"
        ? {
            query: rule.query,
            operator: present[0],
            threshold: rule[present[0]],
          }
        : { engine: rule.engine, implementation: rule.implementation }),
      arguments: {},
      unit: "rows",
      outcome,
      source: "contract",
    };
  }
  if (type !== "library") return undefined;
  if (!METRICS.includes(rule.metric))
    fail(`${where}: unsupported library metric ${rule.metric}`);
  const present = OPERATORS.filter((o) => rule[o] !== undefined);
  if (present.length !== 1)
    fail(`${where}: a library rule needs exactly one comparison`);
  return {
    id: String(rule.id ?? `${where}.${rule.metric}`),
    table,
    ...(column ? { column } : {}),
    metric: rule.metric,
    operator: present[0],
    threshold: rule[present[0]],
    arguments: rule.arguments ?? {},
    unit: rule.unit ?? "rows",
    outcome,
    source: "contract",
  };
}

/** Explicit rules plus key-implied rules, as core computes them. */
export function contractRules(contract) {
  const object = contract.schema?.[0] ?? {};
  const table = String(object.name ?? "table");
  const rules = [];
  const add = (r) => r && rules.push(r);
  for (const q of contract.quality ?? []) add(parse(q, "*"));
  for (const q of object.quality ?? []) add(parse(q, table));
  const keys = [];
  for (const p of object.properties ?? []) {
    for (const q of p.quality ?? []) add(parse(q, table, p.name));
    if (p.primaryKey === true) keys.push(p.name);
  }
  for (const column of keys)
    if (!rules.some((r) => r.column === column && r.metric === "nullValues"))
      rules.push({
        id: `${table}.${column}.key-not-null`,
        table,
        column,
        metric: "nullValues",
        operator: "mustBe",
        threshold: 0,
        arguments: {},
        unit: "rows",
        outcome: "fail",
        source: "primary-key",
      });
  if (
    keys.length &&
    !rules.some((r) => !r.column && r.metric === "duplicateValues")
  )
    rules.push({
      id: `${table}.key-unique`,
      table,
      metric: "duplicateValues",
      operator: "mustBe",
      threshold: 0,
      arguments: { properties: keys },
      unit: "rows",
      outcome: "fail",
      source: "primary-key",
    });
  return rules;
}

const numeric = (type) =>
  /^(BIGINT|INT|INTEGER|SMALLINT|DOUBLE|FLOAT|DECIMAL)/.test(type);
/** Listed values that cannot take the column type never match a row. */
function literals(values, type) {
  return (Array.isArray(values) ? values : [])
    .filter((v) =>
      v === null
        ? false
        : type === "STRING"
          ? typeof v === "string"
          : numeric(type)
            ? typeof v === "number" && Number.isFinite(v)
            : type === "BOOLEAN"
              ? typeof v === "boolean"
              : false,
    )
    .map((v) =>
      typeof v === "string"
        ? `'${v.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`
        : String(v).toUpperCase(),
    );
}

/** A row-level predicate that is true for rows passing the rule. */
function predicate(rule, column) {
  const c = "`" + column.name + "`";
  if (rule.metric === "nullValues") return `${c} IS NOT NULL`;
  if (rule.metric === "missingValues") {
    const missing = literals(
      rule.arguments.missingValues ?? [null, ""],
      column.type,
    );
    return missing.length
      ? `${c} IS NOT NULL AND ${c} NOT IN (${missing.join(", ")})`
      : `${c} IS NOT NULL`;
  }
  if (rule.arguments.validValues !== undefined) {
    const valid = literals(rule.arguments.validValues, column.type);
    return valid.length
      ? `${c} IS NULL OR ${c} IN (${valid.join(", ")})`
      : `${c} IS NULL`;
  }
  if (typeof rule.arguments.pattern !== "string")
    fail(
      `${rule.id}: invalidValues needs arguments.validValues or arguments.pattern`,
    );
  const pattern = `^(?:${rule.arguments.pattern})$`;
  return `${c} IS NULL OR CAST(${c} AS STRING) RLIKE '${pattern
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'")}'`;
}

const constraint = (id, used) => {
  let name = "q_" + id.replace(/[^A-Za-z0-9_]/g, "_");
  for (let i = 2; used.has(name); i++)
    name = `q_${id.replace(/[^A-Za-z0-9_]/g, "_")}_${i}`;
  used.add(name);
  return name;
};

/** Native checks for one table. Throws for explicit rules the standard cannot express. */
export function qualityChecks(contract, columns, standard) {
  const object = contract.schema?.[0] ?? {};
  const physical = new Map(
    (object.properties ?? []).map((p) => [p.name, p.physicalName ?? p.name]),
  );
  const byName = new Map(columns.map((c) => [c.name, c]));
  const column = (rule, name) =>
    byName.get(physical.get(name) ?? name) ??
    fail(`${rule.id}: unknown contract column ${name}`);
  const snapshot = standard === "snapshot-with-history@v1";
  const used = new Set();
  const checks = [];
  for (const rule of contractRules(contract)) {
    // Keys are already required and, for snapshots, checked unique.
    if (rule.source === "primary-key") continue;
    if (rule.metric === "custom") {
      // Databricks engine rules are Spark SQL row predicates; other engines are
      // reported as not enforced.
      if (rule.engine !== "databricks") continue;
      if (typeof rule.implementation !== "string")
        fail(
          `${rule.id}: a databricks rule implementation is a SQL predicate string`,
        );
      checks.push({
        id: rule.id,
        name: constraint(rule.id, used),
        metric: "predicate",
        outcome: rule.outcome,
        expression: sqlShape(rule.id, rule.implementation, "predicate"),
        operator: "mustBe",
        threshold: 0,
        unit: "rows",
      });
      continue;
    }
    if (rule.metric === "sql") {
      if (!snapshot) continue; // reported as not enforced on streaming tables
      sqlShape(rule.id, rule.query, "query");
      checks.push({
        id: rule.id,
        name: constraint(rule.id, used),
        metric: "sql",
        outcome: rule.outcome,
        query: rule.query.replaceAll(
          "${column}",
          rule.column
            ? "`" + column(rule, rule.column).name + "`"
            : "${column}",
        ),
        operator: rule.operator,
        threshold: rule.threshold,
        unit: "rows",
      });
      continue;
    }
    const zero = rule.operator === "mustBe" && rule.threshold === 0;
    if (!snapshot) {
      if (!ROW_LEVEL.has(rule.metric)) continue; // reported as not enforced
      if (!rule.column)
        fail(
          `${rule.id}: ${rule.metric} needs a column on streaming standards`,
        );
      if (!zero)
        fail(
          `${rule.id}: streaming Lakeflow expectations check each row; use mustBe: 0 or a complete-snapshot standard`,
        );
    }
    const target = rule.column ? column(rule, rule.column) : undefined;
    const properties = (rule.arguments.properties ?? []).map(
      (p) => column(rule, p).name,
    );
    if (rule.metric === "duplicateValues" && !target && !properties.length)
      fail(`${rule.id}: table duplicateValues needs arguments.properties`);
    if (ROW_LEVEL.has(rule.metric) && !target)
      fail(`${rule.id}: ${rule.metric} needs a column`);
    checks.push({
      id: rule.id,
      name: constraint(rule.id, used),
      metric: rule.metric,
      outcome: rule.outcome,
      ...(target ? { column: target.name } : {}),
      ...(properties.length ? { properties } : {}),
      ...(ROW_LEVEL.has(rule.metric)
        ? { expression: predicate(rule, target) }
        : {}),
      operator: rule.operator,
      threshold: rule.threshold,
      unit: rule.unit,
    });
  }
  return checks;
}
