/** Creation capabilities, distinct from the broader renderer catalogue. */
export const flowAuthoring = {
  apiVersion: "ingestron.flow-authoring/v1",
  kinds: ["ingestion", "transformation"],
  ingestionStandards: [
    "snapshot-with-history@v1",
    "change-feed-with-history@v1",
    "append-only@v1",
  ],
  prerequisites: [
    "Ingestion needs existing ADLS or volume deliveries and a configured source binding.",
    "Transformation needs a published dataset on the same provider and a pipeline.",
    "Review output contracts, target resources and environment bindings before generation.",
  ],
};
