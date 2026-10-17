/** Offline acceptance uses installed packages and immutable local provider snapshots. */
import assert from "node:assert/strict";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
const owner = resolve(dirname(fileURLToPath(import.meta.url)), "..");
assert.ok(
  process.env.INGESTRON_ACCEPTANCE_CLI_PACKAGE,
  "Set INGESTRON_ACCEPTANCE_CLI_PACKAGE to the installed CLI directory; no downloads or source imports",
);
assert.ok(
  process.env.INGESTRON_ACCEPTANCE_OUTPUT,
  "Set INGESTRON_ACCEPTANCE_OUTPUT to an isolated acceptance directory",
);
const cli = realpathSync(process.env.INGESTRON_ACCEPTANCE_CLI_PACKAGE);
const core = realpathSync(resolve(cli, "../@ingestron/core"));
const manifest = (p) =>
  JSON.parse(readFileSync(resolve(p, "package.json"), "utf8"));
const cliManifest = manifest(cli),
  coreManifest = manifest(core);
assert.equal(cliManifest.name, "ingestron");
assert.equal(coreManifest.name, "@ingestron/core");
assert.ok(
  [
    "0.17.8/0.12.14",
    "0.17.9/0.12.15",
    "0.17.19/0.12.45",
    "0.17.20/0.12.46",
    "0.17.20/0.12.47",
  ].includes(cliManifest.version + "/" + coreManifest.version),
  "Use a qualified installed pair",
);
const base = resolve(process.env.INGESTRON_ACCEPTANCE_OUTPUT);
mkdirSync(base, { recursive: true });
const snapshot = mkdtempSync(resolve(base, "provider-snapshot-"));
cpSync(resolve(owner, "plugin"), resolve(snapshot, "plugin"), {
  recursive: true,
  filter: (p) => !p.includes("__pycache__"),
});
const git = (...args) =>
  execFileSync("git", args, { cwd: snapshot, encoding: "utf8", stdio: "pipe" });
git("init", "--quiet");
git("add", ".");
git(
  "-c",
  "user.name=Fixture",
  "-c",
  "user.email=fixture@example.invalid",
  "commit",
  "-qm",
  "Synthetic acceptance snapshot",
);
git("tag", manifest(owner).version);
const result = spawnSync(
  process.execPath,
  [
    "--experimental-strip-types",
    "--test",
    "--test-reporter=tap",
    resolve(owner, "test/cli-acceptance/ingestion-standards.test.ts"),
    resolve(owner, "test/cli-acceptance/native-authoring.test.ts"),
    resolve(owner, "test/cli-acceptance/native-temporal.test.ts"),
    resolve(owner, "test/cli-acceptance/python-transformation.test.ts"),
  ],
  {
    cwd: owner,
    env: {
      ...process.env,
      INGESTRON_ACCEPTANCE_CLI_PACKAGE: cli,
      INGESTRON_ACCEPTANCE_PROVIDER: snapshot,
      INGESTRON_ACCEPTANCE_PROVIDER_REFERENCE:
        "ingestron/provider-databricks/plugin/provider.yaml@" +
        manifest(owner).version,
    },
    encoding: "utf8",
    maxBuffer: 8 * 1024 * 1024,
  },
);
const output = (result.stdout ?? "") + (result.stderr ?? "");
writeFileSync(resolve(base, "native-acceptance.tap"), output);
process.stdout.write(output);
const evidence = {
  apiVersion: "ingestron.databricks-native-acceptance/v1",
  result: result.status === 0 ? "passed" : "failed",
  cli: cliManifest.version,
  core: coreManifest.version,
  provider: manifest(owner).version,
  providerCommit: git("rev-parse", "HEAD").trim(),
  providerModuleSha256: createHash("sha256")
    .update(readFileSync(resolve(snapshot, "plugin/index.mjs")))
    .digest("hex"),
  snapshot,
  tests: Number(output.match(/# tests (\d+)/)?.[1] ?? 0),
  passed: Number(output.match(/# pass (\d+)/)?.[1] ?? 0),
  limits: [
    "Synthetic offline generation and callback semantics; no source or Databricks workspace execution",
    "Fixtures do not establish vendor SDK or cloud qualification",
  ],
};
writeFileSync(
  resolve(base, "native-acceptance.json"),
  JSON.stringify(evidence, null, 2) + "\n",
);
if (process.env.INGESTRON_ACCEPTANCE_PROOF)
  writeFileSync(
    resolve(process.env.INGESTRON_ACCEPTANCE_PROOF),
    JSON.stringify(evidence, null, 2) + "\n",
  );
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
