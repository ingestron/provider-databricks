import { test } from "node:test";
import assert from "node:assert/strict";
import { parse } from "yaml";
import { assemble } from "../src/project-assembly.mjs";
const input = {
  phase: "assemble",
  project: "demo",
  environment: "dev",
  configuration: "workspace",
  scope: { partial: false, id: "full" },
  binding: { host: "https://example.invalid" },
  options: {},
  nativeFiles: {
    "databricks.yml":
      "bundle:\n  name: demo\ninclude: [resources/*.yml, targets/*.yml]\n",
    "resources/native.yml":
      "resources:\n  jobs:\n    native:\n      name: native\n      tasks: []\n",
  },
  connections: [],
};
test("project and partial bundle states are separate, native resources retained", () => {
  const a = assemble(input),
    b = assemble({ ...input, scope: { partial: true, id: "subset_test" } });
  assert.ok(a.artifacts["resources/native.yml"]);
  assert.notEqual(
    parse(a.artifacts["databricks.yml"]).bundle.name,
    parse(b.artifacts["databricks.yml"]).bundle.name,
  );
  assert.notEqual(
    parse(a.artifacts["targets/dev.yml"]).targets.dev.workspace.state_path,
    parse(b.artifacts["targets/dev.yml"]).targets.dev.workspace.state_path,
  );
  assert.throws(
    () =>
      assemble({
        ...input,
        scope: { partial: true, id: "subset" },
        options: { deployment: { pipelines: { one: { ownership: "adopt" } } } },
      }),
    /cannot adopt/,
  );
});
test("bundle collisions and missing relative sources fail before writing", () => {
  assert.throws(
    () =>
      assemble({
        ...input,
        nativeFiles: {
          ...input.nativeFiles,
          "resources/duplicate.yml": input.nativeFiles["resources/native.yml"],
        },
      }),
    /Duplicate/,
  );
  assert.throws(
    () =>
      assemble({
        ...input,
        nativeFiles: {
          ...input.nativeFiles,
          "resources/native.yml":
            "resources:\n  jobs:\n    native:\n      tasks:\n        - notebook_task:\n            notebook_path: ../missing.py\n",
        },
      }),
    /missing generated source/,
  );
});

test("full publication paths stay stable and partial state is isolated", () => {
  const configured = {
    ...input,
    options: {
      publication: { workspaceRoot: "/Workspace/Shared/publication" },
    },
  };
  const full = assemble(configured),
    partial = assemble({
      ...configured,
      scope: { partial: true, id: "subset_test" },
    });
  assert.equal(full.details.workspaceRoot, "/Workspace/Shared/publication");
  assert.equal(
    partial.details.workspaceRoot,
    "/Workspace/Shared/publication/_ingestron/subset_test",
  );
});
