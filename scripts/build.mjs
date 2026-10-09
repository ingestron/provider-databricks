import { createHash } from "node:crypto";
import {
  transformationSourceSchema,
  transformationLanguageSchema,
} from "../src/transformation.mjs";
const provenance = JSON.parse(
  readFileSync("src/vendor/connectors/provenance.json", "utf8"),
);
for (const [name, record] of Object.entries(provenance.files)) {
  if (
    createHash("sha256")
      .update(readFileSync("src/vendor/connectors/" + name))
      .digest("hex") !== record.sha256
  )
    throw Error("Shared connector contract differs from pinned source");
}
const wrapper = readFileSync("runtime/connectors/databricks_launch.py", "utf8");
writeFileSync(
  "plugin/connector-assets.mjs",
  "export const wrapper=" +
    JSON.stringify(wrapper) +
    ";\nexport const wrapperSha256=" +
    JSON.stringify(createHash("sha256").update(wrapper).digest("hex")) +
    ";\n",
);
import { build } from "esbuild";
import { writeFileSync, readFileSync, realpathSync } from "node:fs";
import { dirname, resolve } from "node:path";
writeFileSync(
  "plugin/quality-assets.mjs",
  "export const evaluator=" +
    JSON.stringify(readFileSync("src/templates/quality-evaluator.py", "utf8")) +
    ";\n",
);
// The pinned runtime seeds string hashing randomly. A fixed seed preserves the
// provider sandbox's determinism without changing parsing or global Math.random.
const seedStatement =
  "var stringSeedHashCode = Math.round(Math.random() * Math.pow(2, 32));";
const runtimeSource = readFileSync(
  "node_modules/antlr4ng/dist/index.mjs",
  "utf8",
);
if (runtimeSource.split(seedStatement).length !== 2)
  throw Error(
    "Pinned ANTLR hashing initialiser changed; review before building",
  );
await build({
  entryPoints: ["src/sql-parser.mjs"],
  outfile: "plugin/sql-parser.mjs",
  bundle: true,
  minify: true,
  format: "esm",
  platform: "neutral",
  target: "es2022",
  plugins: [
    {
      name: "deterministic-antlr-hash",
      setup(builder) {
        builder.onLoad({ filter: /antlr4ng\/dist\/index\.mjs$/ }, () => ({
          contents: runtimeSource.replace(
            seedStatement,
            "var stringSeedHashCode = 0;",
          ),
          loader: "js",
        }));
      },
    },
  ],
});
writeFileSync(
  "plugin/sql-parser-provenance.json",
  JSON.stringify(
    {
      grammar:
        "dt-sql-parser@4.5.1 Spark SQL (adapted from Apache Spark 3.5.0)",
      runtime: "antlr4ng@2.0.11",
      originalRuntimeSha256: createHash("sha256")
        .update(runtimeSource)
        .digest("hex"),
      modification: {
        original: seedStatement,
        replacement: "var stringSeedHashCode = 0;",
      },
    },
    null,
    2,
  ) + "\n",
);
const result = await build({
  entryPoints: ["src/index.mjs"],
  minify: true,
  bundle: true,
  write: false,
  format: "esm",
  platform: "neutral",
  target: "es2022",
});
writeFileSync(
  "plugin/index.mjs",
  result.outputFiles[0].text.replace(/^[ \t]+(?=\r?$)/gm, ""),
);
writeFileSync(
  "plugin/THIRD-PARTY-NOTICES.txt",
  readFileSync("src/templates/THIRD-PARTY-NOTICES.txt", "utf8") +
    "\n\ndt-sql-parser 4.5.1 (MIT)\n\n" +
    readFileSync("node_modules/dt-sql-parser/LICENSE", "utf8") +
    "\n\nantlr4ng 2.0.11 (BSD-3-Clause); deterministic string hash seed modification, see sql-parser-provenance.json\n\n" +
    readFileSync("node_modules/antlr4ng/LICENSE.txt", "utf8") +
    "\n\nSpark SQL grammar adapted from Apache Spark 3.5.0 (Apache-2.0)\n\n" +
    readFileSync("LICENSE", "utf8") +
    "\n\nZod (bundled schema validation)\n\n" +
    readFileSync("node_modules/zod/LICENSE", "utf8") +
    "\n\nLezer Python parser (MIT)\n\n" +
    readFileSync("node_modules/@lezer/python/LICENSE", "utf8") +
    "\n\nLezer common runtime (MIT)\n\n" +
    readFileSync(
      resolve(
        dirname(realpathSync("node_modules/@lezer/python")),
        "common/LICENSE",
      ),
      "utf8",
    ) +
    "\n\nLezer LR runtime (MIT)\n\n" +
    readFileSync(
      resolve(
        dirname(realpathSync("node_modules/@lezer/python")),
        "lr/LICENSE",
      ),
      "utf8",
    ),
);

await build({
  entryPoints: ["src/commands.mjs"],
  outfile: "plugin/commands.mjs",
  bundle: true,
  format: "esm",
  platform: "neutral",
  target: "es2022",
});

await build({
  entryPoints: ["src/lifecycle.ts"],
  outfile: "plugin/lifecycle.mjs",
  bundle: true,
  format: "esm",
  platform: "neutral",
  target: "es2022",
});

const { cpSync, mkdirSync } = await import("node:fs");
mkdirSync("plugin/schemas", { recursive: true });
cpSync("src/schemas", "plugin/schemas", { recursive: true });

const fsNotices = await import("node:fs");
const noticesPath = "plugin/THIRD-PARTY-NOTICES.txt";
const baseNotices = fsNotices.existsSync(noticesPath)
  ? fsNotices
      .readFileSync(noticesPath, "utf8")
      .split("\n\nYAML (bundled parser)")[0]
  : "";
fsNotices.writeFileSync(
  noticesPath,
  baseNotices +
    "\n\nYAML (bundled parser)\n\n" +
    fsNotices.readFileSync("node_modules/yaml/LICENSE", "utf8"),
);

const { parse, stringify } = await import("yaml");
const { connectorRuntimes, connectionDefinition } =
  await import("../src/connectors.mjs");
const manifest = parse(readFileSync("plugin/provider.yaml", "utf8"));
const { transformationSettingsSchema } =
  await import("../src/transformation-authoring.mjs");
const { flowAuthoring } = await import("../src/flow-authoring.mjs");
manifest.authoring = { transformationSettingsSchema, flows: flowAuthoring };
manifest.compatibility.requiredFeatures.push("flow-authoring-capabilities");
const { platformBinding } = await import("../src/platform-binding.mjs");
manifest.platformBinding = platformBinding;
manifest.compatibility.requiredFeatures.push("programme-platform-binding");
manifest.compatibility.requiredFeatures.push(
  "transformation-authoring-settings",
);
manifest.capabilities.transformationLanguages = ["sql", "python"];
manifest.capabilities.typedTransformationLanguages = ["sql", "python"];
manifest.compatibility.requiredFeatures.push("typed-transformation-outputs");
manifest.quality.transformations = {
  python: {
    library: Object.fromEntries(
      [
        "nullValues",
        "missingValues",
        "invalidValues",
        "duplicateValues",
        "rowCount",
      ].map((metric) => [metric, "after-load"]),
    ),
    sql: "after-load",
    engines: { databricks: "after-load" },
  },
};
manifest.quality.transformations.sql = manifest.quality.transformations.python;
manifest.compatibility.requiredFeatures.push("python-source-artifacts");
manifest.compatibility.requiredFeatures.push(
  "published-transformation-quality",
);
const viewPath = "plugin/activities/materialized-view/activity.yaml";
const view = parse(readFileSync(viewPath, "utf8"));
Object.assign(view.optionsSchema.properties, transformationSourceSchema);
view.optionsSchema.required = view.optionsSchema.required.filter(
  (key) => key !== "sqlFile",
);
Object.assign(view.optionsSchema, transformationLanguageSchema);
writeFileSync(
  viewPath,
  await (await import("prettier")).format(stringify(view), { parser: "yaml" }),
);
manifest.capabilities.schedules = ["interval", "daily", "weekly", "monthly"];
manifest.compatibility.requiredFeatures.push(
  "flow-schedules",
  "connector-schedule-fencing",
  "resolved-schedule-boundaries",
  "flow-schedule-resource-claims",
);
const configurationSchema = JSON.parse(
  readFileSync("plugin/configuration.schema.json", "utf8"),
);
const { selectiveRefreshSchema } = await import("../src/schedules.mjs");
configurationSchema.properties.deployment.properties.mode["x-ingestron-ui"] = {
  section: "schedule",
  label: "Deployment mode",
  enumLabels: { development: "Development", production: "Production" },
};
configurationSchema.properties.deployment.properties.mode.description =
  "Mode for the complete shared target configuration. Selective scheduled refresh currently requires Development. Changing this setting affects every flow using this configuration.";
configurationSchema.properties.deployment.properties.job.properties.selectiveRefresh =
  selectiveRefreshSchema;
writeFileSync(
  "plugin/configuration.schema.json",
  await (
    await import("prettier")
  ).format(JSON.stringify(configurationSchema), { parser: "json" }),
);
manifest.version = JSON.parse(readFileSync("package.json", "utf8")).version;
delete manifest.connectors;
manifest.connectorRuntimes = connectorRuntimes;
manifest.compatibility.requiredFeatures = [
  ...new Set([
    ...manifest.compatibility.requiredFeatures,
    "project-connections",
    "odcs-connections",
    "connector-runtime-assets",
    "connector-runtime-capabilities",
  ]),
];
manifest.commands.definitions = manifest.commands.definitions.filter(
  (d) => !["connection prepare", "connector contracts"].includes(d.name),
);
manifest.commands.definitions.push(connectionDefinition, {
  name: "connector contracts",
  description: "Export reviewed ODCS contracts",
  inputSchema: {
    type: "object",
    properties: { review: { type: "object" } },
    required: ["review"],
    additionalProperties: false,
  },
});

manifest.projectAssembly = {
  apiVersion: "ingestron.project-assembly/v1",
  command: "project assemble",
};
manifest.compatibility.requiredFeatures = [
  ...new Set([...manifest.compatibility.requiredFeatures, "project-assembly"]),
];
manifest.commands.definitions = manifest.commands.definitions.filter(
  (d) => d.name !== "project assemble",
);
manifest.commands.definitions.push({
  name: "project assemble",
  description: "Assemble one scoped platform project without deployment",
  inputSchema: { type: "object", additionalProperties: true },
});
const { format } = await import("prettier");
writeFileSync(
  "plugin/provider.yaml",
  await format(stringify(JSON.parse(JSON.stringify(manifest))), {
    parser: "yaml",
  }),
);
