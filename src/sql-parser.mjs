import { CharStreams, CommonTokenStream } from "antlr4ng";
import { SparkSqlLexer } from "dt-sql-parser/dist/lib/spark/SparkSqlLexer.js";
import { SparkSqlParser } from "dt-sql-parser/dist/lib/spark/SparkSqlParser.js";

const check = (condition, message) => {
  if (!condition) throw Error(message);
};
const name = (text) =>
  text.replace(/^`|`$/g, "").replaceAll("``", "`").toLowerCase();

/** Parse only. Expressions and customer SQL are never evaluated by the compiler. */
export function validateSqlSource(source, aliases = []) {
  check(
    typeof source === "string" &&
      source.length > 0 &&
      source.length <= 1024 * 1024,
    "SQL source must contain at most 1 MiB of text",
  );
  const declared = new Set(aliases.map(name));
  check(
    declared.size === aliases.length,
    "SQL input aliases must be distinct without case differences",
  );
  const errors = [];
  const listener = {
    syntaxError(_recognizer, _token, line, column, message) {
      if (errors.length < 3)
        errors.push(
          `line ${line}, column ${column + 1}: ${message.slice(0, 240)}`,
        );
    },
    reportAmbiguity() {},
    reportAttemptingFullContext() {},
    reportContextSensitivity() {},
  };
  const input = CharStreams.fromString(source);
  const lexer = new SparkSqlLexer(input);
  lexer.removeErrorListeners();
  lexer.addErrorListener(listener);
  const tokens = new CommonTokenStream(lexer);
  tokens.fill();
  check(tokens.size <= 50000, "SQL source exceeds the 50000-token limit");
  const parser = new SparkSqlParser(tokens);
  parser.removeErrorListeners();
  parser.addErrorListener(listener);
  const tree = parser.program();
  check(
    !lexer.has_unclosed_bracketed_comment,
    "SQL source contains an unclosed comment",
  );
  check(errors.length === 0, `Invalid SQL syntax: ${errors[0]}`);
  const statements = tree.singleStatement();
  check(
    statements.length === 1 &&
      statements[0].statement().query?.() &&
      statements[0].statement().children?.length === 1 &&
      statements[0].statement().children[0] ===
        statements[0].statement().query(),
    "SQL transformations require exactly one query; commands and multiple statements are not supported",
  );
  let count = 0;
  const visit = (node, scope, depth = 0) => {
    check(
      ++count <= 100000 && depth <= 250,
      "SQL query exceeds the supported structure limits",
    );
    const rule = parser.ruleNames[node.ruleIndex];
    const unsupported = {
      transformClause: "shell-script transformations",
      pivotClause: "PIVOT",
      temporalClause: "time-travel overrides",
      functionTable: "table-valued readers",
    }[rule];
    check(
      !unsupported,
      `SQL ${unsupported} are not supported in transformations; use reviewed declared inputs`,
    );
    if (["tableName", "viewName", "identifierReference"].includes(rule)) {
      check(
        rule !== "identifierReference" || !node.KW_IDENTIFIER(),
        "Dynamic SQL identifiers are not supported; use reviewed declared inputs",
      );
      const relation = name(node.getText());
      check(
        declared.has(relation) || scope.has(relation),
        `SQL relation ${node.getText()} is not a declared input or local CTE`,
      );
      return;
    }
    const ctes = rule === "query" ? node.ctes?.() : undefined;
    let next = scope;
    if (ctes) {
      next = new Set(scope);
      const local = new Set();
      for (const cte of ctes.namedQuery()) {
        const alias = name(cte.errorCapturingIdentifier().getText());
        check(
          !declared.has(alias) && !local.has(alias),
          `SQL CTE ${alias} duplicates a local CTE or declared input`,
        );
        visit(cte.query(), next, depth + 1);
        next.add(alias);
        local.add(alias);
      }
    }
    for (const child of node.children ?? [])
      if (child !== ctes) visit(child, next, depth + 1);
  };
  visit(statements[0].statement().query(), new Set());
  const terminator = statements[0].SEMICOLON()?.symbol;
  // ANTLR offsets count Unicode code points, not JavaScript UTF-16 units.
  return terminator
    ? input.getText(0, terminator.start - 1) +
        input.getText(terminator.stop + 1, input.size - 1)
    : source;
}
