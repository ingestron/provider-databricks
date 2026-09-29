import { parseDocument } from "yaml";
const check = (ok, code, message) => {
  if (!ok) throw new Error(message);
};
const posix = {
  dirname: (p) => (p.includes("/") ? p.slice(0, p.lastIndexOf("/")) : "."),
  join: (...p) => p.join("/"),
  normalize: (p) => {
    const out = [];
    for (const part of p.split("/")) {
      if (part === "..") out.pop();
      else if (part && part !== ".") out.push(part);
    }
    return out.join("/");
  },
};
export function bundleConfiguration(files, root) {
  const result = parseDocument(files[root]).toJS();
  const prefix = root.includes("/")
    ? root.slice(0, root.lastIndexOf("/") + 1)
    : "";
  for (const pattern of result.include ?? []) {
    check(
      typeof pattern === "string" &&
        /^(resources|targets)\/\*\.yml$/.test(pattern),
      "DAB",
      "Unsupported generated bundle include",
    );
    const directory = prefix + pattern.slice(0, -5);
    const matches = Object.keys(files).filter(
      (p) =>
        p.startsWith(directory) &&
        p.endsWith(".yml") &&
        !p.slice(directory.length).includes("/"),
    );
    check(matches.length > 0, "DAB", `Missing bundle include ${pattern}`);
    for (const file of matches) {
      const value = parseDocument(files[file]).toJS();
      for (const [section, entries] of Object.entries(value)) {
        result[section] ??= {};
        if (section === "resources")
          for (const [kind, resources] of Object.entries(entries)) {
            result.resources[kind] ??= {};
            for (const [id, resource] of Object.entries(resources)) {
              check(
                !Object.hasOwn(result.resources[kind], id),
                "DAB",
                `Duplicate bundle resource ${id}`,
              );
              result.resources[kind][id] = resource;
            }
          }
        else
          for (const [id, entry] of Object.entries(entries)) {
            check(
              !Object.hasOwn(result[section], id),
              "DAB",
              `Duplicate bundle ${section}.${id}`,
            );
            result[section][id] = entry;
          }
      }
    }
  }
  return result;
}

export function validateReferences(files) {
  const requireFile = (path, owner) => {
    if (path.startsWith("/") || path.startsWith("${")) return;
    const resolved = posix.normalize(posix.join(posix.dirname(owner), path));
    check(
      Object.hasOwn(files, resolved),
      "REFERENCE",
      `${owner}: missing generated source ${path}`,
    );
  };
  for (const [name, source] of Object.entries(files)) {
    if (!/\.ya?ml$/.test(name) || !name.startsWith("resources/")) continue;
    const visit = (value) => {
      if (!value || typeof value !== "object") return;
      const task = value.notebook_task?.notebook_path;
      if (typeof task === "string") requireFile(task, name);
      if (Array.isArray(value.libraries))
        for (const library of value.libraries) {
          const path = library.notebook?.path ?? library.file?.path;
          if (typeof path === "string") requireFile(path, name);
        }
      Object.values(value).forEach(visit);
    };
    visit(parseDocument(source).toJS());
  }
}
