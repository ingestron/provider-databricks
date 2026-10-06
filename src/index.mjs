import { templateRenderer } from "./templates/engine.mjs";
import { validateIngestionPlan, renderIngestion } from "./render.mjs";
import { qualityChecks } from "./quality.mjs";
import { expandIngestion } from "./standards.ts";
import { expandQueryIngestion, queryStandard } from "./query-ingestion.mjs";
import {
  expandSharePointIngestion,
  sharePointStandard,
} from "./sharepoint-ingestion.mjs";
import { expandSaasIngestion, saasStandard } from "./saas-ingestion.mjs";
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
const sourcePlan = (plan) => ({
  ...normalise(plan),
  nodes: normalise(plan).nodes.map((n) => ({
    ...n,
    sql:
      n.sqlFile?.endsWith(".j2") && plan.sourceFilesChecked
        ? templateRenderer({ main: n.sql })("main", {
            environment: plan.environment,
            options: n.with,
            node: n,
            binding: plan.bindings[n.binding],
            columns: n.columns ?? [],
          })
        : n.sql,
  })),
});
export function validate(plan) {
  plan = sourcePlan(plan);
  return publication(plan)
    ? validatePublication(plan)
    : validateIngestionPlan(plan);
}
export function render(plan) {
  plan = sourcePlan({ ...plan, sourceFilesChecked: true });
  return publication(plan) ? renderPublication(plan) : renderIngestion(plan);
}
export function expand(request) {
  const standard = request.flow.ingestion?.standard;
  if ([queryStandard, sharePointStandard, saasStandard].includes(standard)) {
    for (const [table, columns] of Object.entries(request.columns))
      if (columns.some((c) => c.target && c.target !== c.name))
        throw new Error(
          `${table}: managed ingestion preserves physical source column names; use a separate materialized-view transformation for column renames`,
        );
  }
  const expanded =
    standard === "snapshot-publication@v1"
      ? expandPublication(request)
      : standard === sharePointStandard
        ? expandSharePointIngestion(request.flow, request.providerSource)
        : standard === saasStandard
          ? expandSaasIngestion(request.flow, request.providerSource)
          : standard === queryStandard
            ? expandQueryIngestion(
                request.flow,
                request.providerSource,
                request.columns,
              )
            : expandIngestion(
                request.flow,
                request.providerSource,
                request.columns,
              );
  expanded.steps = expanded.steps.map((step) => ({
    ...step,
    ...([queryStandard, sharePointStandard, saasStandard].includes(standard)
      ? {
          with: {
            ...step.with,
            quality: qualityChecks(
              request.flow.tables[step.select[0]].contract,
              request.columns[step.select[0]],
              standard,
            ),
          },
        }
      : {}),
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
