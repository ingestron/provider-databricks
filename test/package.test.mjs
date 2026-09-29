import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { expand } from "../plugin/index.mjs";
test("standards reject incomplete snapshot meaning", () => {
  assert.throws(
    () =>
      expand({
        providerSource: "test",
        columns: {},
        flow: {
          kind: "ingestion",
          ingestion: { standard: "snapshot-with-history@v1" },
          tables: {},
        },
      }),
    /pipeline|target|source|retention/,
  );
});
test("plugin is a self-contained bounded release asset", () => {
  const source = readFileSync("plugin/index.mjs", "utf8");
  assert(Buffer.byteLength(source) < 2000000);
  assert(!/^import .* from /m.test(source));
});
