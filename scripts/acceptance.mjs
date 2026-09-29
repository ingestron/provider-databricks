import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
if (!process.env.INGESTRON_CLI_REPO)
  throw new Error(
    "Set INGESTRON_CLI_REPO to the tested CLI 0.10.0 checkout; see docs/release-process.md",
  );
execFileSync(
  process.execPath,
  [
    resolve(
      process.env.INGESTRON_CLI_REPO,
      "scripts/check-provider-release.mjs",
    ),
    process.cwd(),
  ],
  { stdio: "inherit" },
);
