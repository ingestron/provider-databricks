import { execFileSync } from "node:child_process";
import { readFileSync, existsSync } from "node:fs";
const files = execFileSync(
  "git",
  ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
  { encoding: "utf8" },
)
  .split("\0")
  .filter(Boolean);
const patterns = [
  /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,
  /\bgh[pousr]_[A-Za-z0-9]{30,}\b/,
  /\bAKIA[A-Z0-9]{16}\b/,
];
let bad = false;
for (const file of files) {
  if (!existsSync(file)) continue;
  const text = readFileSync(file, "utf8");
  if (patterns.some((p) => p.test(text))) {
    console.error("Potential secret: " + file);
    bad = true;
  }
}
if (bad) process.exit(1);
console.log(
  "Secret-pattern scan passed (not a comprehensive secret detector).",
);
