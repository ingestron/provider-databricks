import { readdirSync, readFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
const walk = (directory) =>
  readdirSync(directory, { withFileTypes: true })
    .filter(
      (entry) =>
        !["node_modules", ".git", ".venv", "__pycache__"].includes(entry.name),
    )
    .flatMap((entry) =>
      entry.isDirectory()
        ? walk(resolve(directory, entry.name))
        : [resolve(directory, entry.name)],
    );
let links = 0;
for (const file of walk(".").filter((file) => file.endsWith(".md"))) {
  for (const match of readFileSync(file, "utf8").matchAll(
    /\[[^\]]+\]\(([^)]+)\)/g,
  )) {
    const target = match[1].split("#")[0];
    if (!target || /^[a-z]+:/i.test(target)) continue;
    if (!existsSync(resolve(dirname(file), decodeURIComponent(target))))
      throw new Error(`Missing documentation link in ${file}: ${target}`);
    links++;
  }
}
console.log(`${links} local documentation links passed`);
