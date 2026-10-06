import { test } from "node:test";
import assert from "node:assert/strict";
import { fileTemporal, temporalLiteral } from "../src/temporal-types.mjs";
const columns = [
  { name: "id", type: "BIGINT", key: true },
  { name: "day", type: "DATE", required: true },
  { name: "event", target: "event_time", type: "TIMESTAMP" },
];
test("native text projections preserve temporal source text until strict calendar/precision checks and publish explicit timezone-less types", () => {
  const value = fileTemporal(columns, "json", "snapshot-with-history@v1");
  assert.equal(value.schema, "`id` BIGINT, `day` STRING, `event` STRING");
  assert.match(value.columns, /AS TIMESTAMP_NTZ/);
  assert.match(value.columns, /AS `event_time`/);
  assert.match(value.columns, /raise_error/);
  assert.match(value.invalid, /try_cast/);
  assert.match(value.projection, /F.expr/);
  assert.deepEqual(value.parquet, []);
  assert.equal(columns[2].type, "TIMESTAMP");
});
test("native Parquet snapshots require source footer types before forced schemas and retain the native calendar/clock range", () => {
  const value = fileTemporal(columns, "parquet", "snapshot-with-history@v1");
  assert.equal(value.schema, "`id` BIGINT, `day` DATE, `event` TIMESTAMP_NTZ");
  assert.deepEqual(value.parquet, [
    { name: "day", type: "date" },
    { name: "event", type: "timestamp_ntz" },
  ]);
  assert.match(value.invalid, /year\(`event`\) BETWEEN 1 AND 9999/);
  assert.match(value.projection, /F.col\("event"\).alias\("event_time"\)/);
});
test("streaming standards reject unverified arriving Parquet temporal schemas and support guarded text projections", () => {
  for (const standard of ["append-only@v1", "change-feed-with-history@v1"]) {
    assert.throws(
      () => fileTemporal(columns, "parquet", standard),
      /verified per-file timezone and precision policy/,
    );
    assert.match(
      fileTemporal(columns, "json", standard).columns,
      /raise_error/,
    );
  }
  const plain = fileTemporal(
    [{ name: "id", type: "BIGINT" }],
    "parquet",
    "append-only@v1",
  );
  assert.equal(plain.columns, "`id`");
  assert.equal(plain.projection, "");
  assert.equal(plain.invalid, "");
  assert.throws(
    () =>
      fileTemporal(
        [{ name: "raw;value", type: "TIMESTAMP" }],
        "json",
        "append-only@v1",
      ),
    /Invalid temporal source column/,
  );
});

test("native temporal quality values use exact typed literals and ignore invalid dates, offsets and overprecision", () => {
  assert.equal(temporalLiteral("DATE", "2024-02-29"), "DATE '2024-02-29'");
  assert.equal(
    temporalLiteral("TIMESTAMP", "2024-02-29T01:02:03.123456"),
    "TIMESTAMP_NTZ '2024-02-29 01:02:03.123456'",
  );
  for (const value of [
    "2023-02-29",
    "0000-01-01",
    "2024-02-29\n",
    "2024-02-29T01:02:03",
  ])
    assert.equal(temporalLiteral("DATE", value), undefined);
  for (const value of [
    "2024-02-30T01:02:03",
    "2024-02-29T24:02:03",
    "2024-02-29T01:02:60",
    "2024-02-29T01:02:03Z",
    "2024-02-29T01:02:03.1234567",
    "2024-02-29T01:02:03\n",
    true,
    null,
  ])
    assert.equal(temporalLiteral("TIMESTAMP", value), undefined);
});
