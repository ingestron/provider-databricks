import nunjucks from "nunjucks/browser/nunjucks.js";

/** Runs only inside the provider VM. Never render customer templates on the host. */
export function templateRenderer(templates) {
  const loader = new (nunjucks.Loader.extend({
    getSource(name) {
      if (typeof name !== "string" || !Object.hasOwn(templates, name))
        throw new Error(`Unknown packaged template: ${name}`);
      if (/\[\[[A-Za-z_][A-Za-z0-9_]*\]\]/.test(templates[name]))
        throw new Error(
          "Replace legacy [[name]] placeholders with {{ name }} in " + name,
        );
      return { src: templates[name], path: name, noCache: false };
    },
  }))();
  const env = new nunjucks.Environment(loader, {
    autoescape: false,
    throwOnUndefined: true,
    trimBlocks: true,
    lstripBlocks: true,
  });
  const identifier = (value) => {
    if (typeof value !== "string" || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value))
      throw new Error(`Invalid SQL identifier: ${value}`);
    return "`" + value + "`";
  };
  env.addFilter("identifier", identifier);
  env.addFilter("relation", (value) => {
    if (typeof value !== "string")
      throw new Error("relation requires a dotted name");
    return value.split(".").map(identifier).join(".");
  });
  env.addFilter("sql_string", (value) => {
    if (typeof value !== "string")
      throw new Error("sql_string requires a string");
    return "'" + value.replaceAll("\\", "\\\\").replaceAll("'", "\\'") + "'";
  });
  env.addFilter("python", (value) => {
    if (value === null) return "None";
    if (value === true) return "True";
    if (value === false) return "False";
    if (
      typeof value === "string" ||
      (typeof value === "number" && Number.isFinite(value))
    )
      return JSON.stringify(value);
    throw new Error(
      "python requires a scalar; pass structured values explicitly",
    );
  });
  // Do not give templates nondeterministic helpers or filesystem/network loaders.
  delete env.filters.random;
  return (name, values = {}) =>
    env.render(name, JSON.parse(JSON.stringify(values)));
}
export function validate(input) {
  if (
    input.apiVersion !== "ingestron.template-render/v1" ||
    !Array.isArray(input.jobs)
  )
    throw new Error("Invalid template render request");
}
export function render(input) {
  validate(input);
  const files = {};
  for (const job of input.jobs) {
    const renderTemplate = templateRenderer(job.templates);
    for (const name of job.outputs)
      files[`${job.id}/${name}`] = {
        format: "text",
        value: renderTemplate(name, job.values),
      };
  }
  return files;
}
