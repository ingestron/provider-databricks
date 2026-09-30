/** Lakeflow Connect managed SaaS ingestion (PB-064 phase 4).
 *
 * Salesforce, HubSpot and Jira are read by Databricks-managed connectors through
 * a Unity Catalog connection; the pipeline resource carries an
 * `ingestion_definition` and Databricks performs the reads. Table names and
 * source schemas follow Microsoft Learn (accessed 2026-09-30): Salesforce uses
 * the `objects` schema and object API names (GA); HubSpot and Jira use the
 * `default` schema with documented table names (HubSpot CRM tables and Jira are
 * Beta).
 */
const fail = (message) => {
  throw new Error(message);
};
const identifier = (v) =>
  typeof v === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(v);
export const saasStandard = "saas-ingestion@v1";
const histories = ["SCD_TYPE_1", "SCD_TYPE_2"];

/** Documented source tables per application. */
export const saasApps = {
  salesforce: {
    label: "Salesforce",
    schema: "objects",
    // Standard and custom object API names, such as Account or Invoice__c.
    object: (v) => /^[A-Za-z][A-Za-z0-9_]{0,79}$/.test(v),
    hint: "a Salesforce object API name such as Account or Invoice__c",
  },
  hubspot: {
    label: "HubSpot",
    schema: "default",
    tables: [
      // Marketing Hub
      "email_events",
      "email_subscription_change",
      "marketing_emails",
      "email_campaign",
      "email_campaign_list",
      "email_subscriptions",
      "form_submissions",
      "forms",
      "marketing_campaign_asset",
      "marketing_campaign_budget",
      "marketing_campaign_spend",
      "marketing_campaigns",
      "marketing_event_list",
      "marketing_events",
      // CRM Hub (Beta)
      "calls",
      "companies",
      "contacts",
      "deals",
      "emails",
      "leads",
      "line_items",
      "meetings",
      "notes",
      "orders",
      "products",
      "tasks",
      "tickets",
      "deals_pipelines",
      "owners",
      "tickets_pipelines",
    ],
  },
  jira: {
    label: "Jira",
    schema: "default",
    tables: [
      "application_roles",
      "boards",
      "issue_comments",
      "issue_field_values",
      "issue_fields",
      "issue_links",
      "issue_types",
      "issue_watchers",
      "issue_worklogs",
      "issues",
      "permission_schemes",
      "priority",
      "project_board",
      "project_categories",
      "project_components",
      "project_permissions",
      "project_role_actor",
      "project_roles",
      "projects",
      "resolutions",
      "security_level",
      "security_schemes",
      "sprints",
      "status",
      "status_category",
      "user_group",
      "users",
      "version",
    ],
  },
};

export function expandSaasIngestion(flow, providerSource) {
  const config = flow.ingestion ?? {};
  for (const key of Object.keys(config))
    ["standard", "pipeline", "target", "history"].includes(key) ||
      fail(`Unsupported SaaS ingestion setting ${key}`);
  (typeof config.pipeline === "string" &&
    /^[A-Za-z0-9_-]+$/.test(config.pipeline)) ||
    fail("A logical pipeline alias is required");
  (config.target &&
    Object.keys(config.target).every((k) => k === "schema") &&
    identifier(config.target.schema)) ||
    fail("SaaS ingestion target needs only a destination schema");
  const history = config.history ?? "SCD_TYPE_1";
  histories.includes(history) ||
    fail(`history must be one of ${histories.join(", ")}`);
  Object.keys(flow.defaults?.with ?? {}).length &&
    fail("Ingestion standards own their processing");
  const steps = Object.keys(flow.tables ?? {}).map((table) => {
    const value = flow.tables[table];
    (value.ingestion === undefined && !Object.keys(value.steps ?? {}).length) ||
      fail(`${table}: SaaS ingestion has no table settings`);
    return {
      id: `ingest_${table}`,
      uses: "lakeflow-ingest@v1",
      select: [table],
      with: {
        standard: saasStandard,
        pipeline: config.pipeline,
        target: config.target,
        history,
        keys: [],
      },
    };
  });
  return {
    steps,
    recovery: {
      standard: saasStandard,
      provider: providerSource,
      capture: "managed-incremental",
      replaySource: "source-application",
      retention: "source-owned",
      actualCompleteness: "unverified",
      detail:
        "Lakeflow Connect keeps each table's cursor; tables without one are fully refreshed on every run. Deletes are propagated only where the connector supports it.",
      assumptions: [
        "The Unity Catalog connection's OAuth identity can read every selected table.",
        "The application's API limits allow each scheduled run to complete.",
      ],
    },
  };
}

/** Checks one SaaS node's source during plan validation. */
export function validateSaasNode(n) {
  const s = n.source ?? {};
  for (const key of Object.keys(s))
    ["kind", "connection", "object"].includes(key) ||
      fail(`Unsupported SaaS source setting ${key}`);
  const app = saasApps[s.kind];
  app ||
    fail(
      `SaaS ingestion reads ${Object.keys(saasApps).join(", ")}, not ${s.kind}`,
    );
  (typeof s.connection === "string" &&
    s.connection.length > 0 &&
    s.connection.length <= 255) ||
    fail(`${app.label} ingestion needs a Unity Catalog connection`);
  const known = app.tables
    ? app.tables.includes(s.object)
    : typeof s.object === "string" && app.object(s.object);
  known ||
    fail(
      `${app.label} object must be ${app.hint ?? `one of ${app.tables.join(", ")}`}`,
    );
}

/** The Lakeflow Connect object for one SaaS table; reads only contract columns. */
export function saasObject(s, columns, history, destination) {
  columns.every((c) => c.target === undefined || c.target === c.name) ||
    fail(
      `${saasApps[s.kind].label} ingestion keeps source field names; remove target renames`,
    );
  return {
    connection: s.connection,
    object: {
      table: {
        source_schema: saasApps[s.kind].schema,
        source_table: s.object,
        destination_catalog: destination.catalog,
        destination_schema: destination.schema,
        destination_table: destination.table,
        table_configuration: {
          scd_type: history,
          include_columns: columns.map((c) => c.name),
        },
      },
    },
  };
}
