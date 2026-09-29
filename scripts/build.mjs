import { createHash } from "node:crypto";
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
import { writeFileSync, readFileSync } from "node:fs";
const result = await build({
  entryPoints: ["src/index.mjs"],
  bundle: true,
  write: false,
  format: "esm",
  platform: "neutral",
  target: "es2022",
});
writeFileSync("plugin/index.mjs", result.outputFiles[0].text);
writeFileSync(
  "plugin/THIRD-PARTY-NOTICES.txt",
  readFileSync("src/templates/THIRD-PARTY-NOTICES.txt", "utf8") +
    "\n\nZod (bundled schema validation)\n\n" +
    readFileSync("node_modules/zod/LICENSE", "utf8"),
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
