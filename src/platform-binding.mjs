/** Translate reviewed resource references; no discovery, authentication or resource creation. */
export const platformBinding = {
  apiVersion: "ingestron.platform-binding-authoring/v1",
  designResources: {
    workspaceUrl: "https://design.example.invalid",
    catalog: "design_catalog",
  },
  resourcesSchema: {
    type: "object",
    additionalProperties: false,
    required: ["workspaceUrl", "catalog"],
    properties: {
      workspaceUrl: {
        type: "string",
        pattern: "^https://[A-Za-z0-9.-]+/?$",
        title: "Existing workspace URL",
      },
      catalog: {
        type: "string",
        pattern: "^[A-Za-z_][A-Za-z0-9_]*$",
        title: "Existing Unity Catalog catalogue",
      },
    },
  },
};
export function bindPlatform(options) {
  const { profile, resources, projectId, environment } = options;
  if (profile.authentication !== "databricks-cli" || profile.secrets.length)
    throw Error(
      "This offline binding supports native Databricks CLI authentication without copied secret references; review other identity methods separately.",
    );
  if (profile.purpose === "production")
    throw Error(
      "Production needs reviewed run identity, permissions and operational settings before binding.",
    );
  if (
    Object.keys(resources).some(
      (k) => !["workspaceUrl", "catalog"].includes(k),
    ) ||
    !/^https:\/\/[A-Za-z0-9.-]+\/?$/.test(resources.workspaceUrl ?? "") ||
    !/^[A-Za-z_]\w*$/.test(resources.catalog ?? "")
  )
    throw Error(
      "Choose an HTTPS workspace URL and existing catalogue; unsupported resource fields cannot be ignored.",
    );
  if (
    ![projectId, environment].every(
      (v) => typeof v === "string" && /^[A-Za-z_][A-Za-z0-9_-]{0,79}$/.test(v),
    )
  )
    throw Error("Invalid project or environment identity.");
  return {
    apiVersion: "ingestron.provider-binding/v1",
    binding: {
      kind: "databricks",
      host: resources.workspaceUrl.replace(/\/$/, ""),
      catalog: resources.catalog,
    },
    options: {
      deployment: {
        mode: "development",
        rootPath: `/Workspace/Shared/ingestron/${projectId}/${environment}`,
      },
    },
    ownership:
      "Existing workspace, catalogue and schemas remain external. Generated bundle state and assets belong to this project and environment.",
    pendingIntent: [
      "Flow standards, naming and quality are reviewed when defining the deliverable; resource binding alone does not apply them.",
    ],
  };
}
