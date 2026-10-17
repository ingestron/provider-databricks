import test from "node:test";
import assert from "node:assert/strict";
import { deploymentSettings } from "../src/resources.mjs";

const plan = (selection) => ({
  nodes: [{ runtime: { options: {} } }],
  selection,
});

test("complete project builds remain eligible while partial selections are rejected", () => {
  assert.deepEqual(deploymentSettings(plan({})), { mode: "development" });
  assert.deepEqual(deploymentSettings(plan({ projectBuild: true })), {
    mode: "development",
  });
  for (const selection of [
    { projectBuild: false },
    { flow: "reporting" },
    { projectBuild: true, flow: "reporting" },
    { table: "customers" },
    { step: "clean" },
    { profile: "preview" },
    { unknown: true },
  ]) {
    assert.throws(
      () => deploymentSettings(plan(selection)),
      /partial selection/,
    );
  }
});
