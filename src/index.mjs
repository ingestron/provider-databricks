import { templateRenderer } from "./templates/engine.mjs";
import { validateIngestionPlan, renderIngestion } from "./render.mjs";
import { expandIngestion } from "./standards.ts";
import {
  expandPublication,
  validatePublication,
  renderPublication,
} from "./publication.mjs";
const publication = (plan) =>
  plan.nodes.some((n) => n.uses === "snapshot-publish@v1");
const normalise = (plan) => ({
  ...plan,
  nodes: plan.nodes.map((n) => ({
    ...n,
    uses: n.uses.replace("-input@", "@"),
  })),
});
export function validate(plan) {
  plan = normalise(plan);
  return publication(plan)
    ? validatePublication(plan)
    : validateIngestionPlan(plan);
}
export function render(plan) {
  plan = normalise(plan);
  plan = {
    ...plan,
    nodes: plan.nodes.map((n) => ({
      ...n,
      sql: n.sqlFile?.endsWith(".j2")
        ? templateRenderer({ main: n.sql })("main", {
            environment: plan.environment,
            options: n.with,
            node: n,
            binding: plan.bindings[n.binding],
            columns: n.columns ?? [],
          })
        : n.sql,
    })),
  };
  return publication(plan) ? renderPublication(plan) : renderIngestion(plan);
}
export function expand(request) {
  const expanded =
    request.flow.ingestion?.standard === "snapshot-publication@v1"
      ? expandPublication(request)
      : expandIngestion(request.flow, request.providerSource, request.columns);
  expanded.steps = expanded.steps.map((step) => ({
    ...step,
    uses: step.select?.some(
      (table) =>
        request.flow.tables[table].source?.from ??
        request.flow.defaults.source?.from,
    )
      ? step.uses.replace("@", "-input@")
      : step.uses,
  }));
  return expanded;
}
