/** Explicit native file dependency; no source access or deployment. */
export function authorFileHandoff(o) {
  if (!o.tables?.length) throw Error("Select reviewed files");
  const requires = {},
    tables = {};
  for (const t of o.tables) {
    requires[t.id] = {
      dataset: `${o.project}.${o.producerFlow}.${t.id}.snapshot`,
      select: { mode: "same-run" },
      handover: "files",
    };
    tables[t.id] = {
      source: { from: `requires.${t.id}` },
      contract: t.contract,
    };
  }
  return {
    flow: {
      apiVersion: "ingestron.flow/v1",
      id: o.id,
      kind: "ingestion",
      provider: o.provider,
      description: `Design delivery of ${o.origin}; append immutable files only after the ADF batch succeeds. Deployment preparation must isolate completed batches from incomplete copies.`,
      requires,
      ingestion: {
        standard: "append-only@v1",
        pipeline: o.id,
        target: { schema: "current" },
        source: { delivery: "immutable-files", deletes: "not-applicable" },
        retention: { sourceDeliveries: "not-retained" },
      },
      tables,
    },
    bindings: {},
  };
}
