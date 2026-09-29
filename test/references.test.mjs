import { test } from "node:test";
import assert from "node:assert/strict";
import { validateReferences as validateProjectReferences } from "../src/output-validation.mjs";
test("bundle attachments must resolve to generated files or explicit workspace paths", () => {
  const source =
    "resources:\n  pipelines:\n    source:\n      libraries:\n        - file:\n            path: ../pipelines/source.sql\n";
  assert.throws(
    () => validateProjectReferences({ "resources/source.yml": source }),
    /missing generated source/,
  );
  validateProjectReferences({
    "resources/source.yml": source,
    "pipelines/source.sql": "SELECT 1",
  });
  validateProjectReferences({
    "resources/source.yml": source.replace(
      "../pipelines/source.sql",
      "/Workspace/shared/source.sql",
    ),
  });
});
