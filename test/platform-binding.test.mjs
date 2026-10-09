import { test } from "node:test";
import assert from "node:assert/strict";
import { author } from "../plugin/lifecycle.mjs";
import { parse } from "yaml";
const options = () => ({
  projectId: "sales-margin",
  environment: "review",
  profile: {
    purpose: "development",
    authentication: "databricks-cli",
    secrets: [],
  },
  resources: {
    workspaceUrl: "https://synthetic.invalid/",
    catalog: "synthetic_retail",
  },
});
test("reviewed binding translates resource references and scopes bundle state without execution", () => {
  const input = options(),
    before = structuredClone(input);
  const files = author({ operation: "platform_binding", options: input }).files;
  assert.deepEqual(Object.keys(files), ["binding.yaml"]);
  const result = parse(files["binding.yaml"]);
  assert.deepEqual(result.binding, {
    kind: "databricks",
    host: "https://synthetic.invalid",
    catalog: "synthetic_retail",
  });
  assert.equal(
    result.options.deployment.rootPath,
    "/Workspace/Shared/ingestron/sales-margin/review",
  );
  assert.ok(result.pendingIntent.length);
  assert.deepEqual(input, before);
});
test("missing, unsupported, credential-bearing and production settings fail explicitly", () => {
  const mutate = [
    (o) => delete o.resources.catalog,
    (o) => (o.resources.password = "synthetic"),
    (o) =>
      (o.resources.workspaceUrl = "https://user:password@synthetic.invalid"),
    (o) => (o.profile.authentication = "oauth-m2m"),
    (o) => o.profile.secrets.push({ name: "TOKEN", source: "local-env" }),
    (o) => (o.profile.purpose = "production"),
    (o) => (o.projectId = "../shared"),
  ];
  for (const edit of mutate) {
    const o = options();
    edit(o);
    assert.throws(() => author({ operation: "platform_binding", options: o }));
  }
});
