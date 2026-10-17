export const transformationSettingsSchema = {
  type: "object",
  required: ["pipelineId", "targetTable"],
  additionalProperties: false,
  properties: {
    pipelineId: {
      type: "string",
      minLength: 1,
      maxLength: 100,
      title: "Pipeline alias",
      description: "Logical alias in this project's generated bundle.",
    },
    targetTable: {
      type: "string",
      minLength: 1,
      maxLength: 500,
      title: "Output table",
      description:
        "catalogue.schema.table, for example {{env}}_retail.silver.orders.",
    },
  },
};

export function transformationSettings(value) {
  if (value === undefined) return undefined;
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).some(
      (key) => !["pipelineId", "targetTable"].includes(key),
    ) ||
    typeof value.pipelineId !== "string" ||
    !/^[A-Za-z0-9_-]{1,100}$/.test(value.pipelineId) ||
    typeof value.targetTable !== "string" ||
    value.targetTable.length > 500 ||
    !/^[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*$/.test(
      value.targetTable.replace(
        /{{\s*(?:env|project\.id|values\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*}}/g,
        "reviewed",
      ),
    )
  )
    throw Error(
      "Supply a pipeline alias and catalogue.schema.table for the transformation output",
    );
  return value;
}
