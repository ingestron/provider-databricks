# Write a Python transformation

Private candidate: provider 3.12.0 with CLI 0.17.20 and core 0.12.46. Installed
offline acceptance covers generation; Databricks workspace qualification is pending.

Use Python to return a batch DataFrame from reviewed input datasets. In a configured
project, preview a new flow:

```sh
ingestron --dry-run flow add report --kind transformation --language python --input-dataset retail.source.orders.current --transformation-settings '{"pipelineId":"reporting","targetTable":"{{env}}_retail.silver.report"}'
```

Replace the input with a published current or transformation dataset from your
project. The proposal adds a flow, a copied output contract and
`flows/report/python/transform.py`:

```python
def transform(inputs):
    return inputs["source"]
```

In Studio, choose the provider, published input and **Python** when adding a
transformation. Enter a pipeline alias and qualified output table. Copy the input
contract or select an existing output contract; the new flow receives its own copy.
Review the proposal before saving, then review the output columns and edit the
function. A separate pipeline alias is useful when the transformation has its own
refresh schedule or quality gate.

The target uses `catalogue.schema.table`. `{{env}}`, `{{project.id}}` and reviewed
`{{values.name}}` placeholders resolve for the selected environment. Use its
configured catalogue and an existing schema. The compiler creates no cloud
resources. CLI `--contract contracts/reviewed.odcs.yaml` selects an existing output
contract; concurrent changes to that contract invalidate the proposal. Existing
flow, source and contract files are preserved when creation is refused.

The activity uses `materialized-view@v1` with `pythonFile` instead of `sqlFile`.
`entrypoint` defaults to `transform`. Declare each input alias in `with.sources`
and `flow.requires`; the generated native definition reads those relations and
passes them to your function as a mapping. Publish the result with one consistent
ODCS output contract.

Use one top-level synchronous function taking one ordinary positional argument.
Type annotations, imports and helper functions are supported. The selected
entrypoint cannot be decorated, asynchronous or a generator. Names starting with
`_ingestron_` are reserved for the generated definition. Source is literal Python,
with a limit of 1,048,576 characters; it is not a template.

Return a lazy batch DataFrame with the exact reviewed column names, order and
types. Streaming output or a schema mismatch fails the native definition.
Follow [Databricks' definition-function rules](https://docs.databricks.com/aws/en/ldp/developer/definition-function):
avoid row-collection actions, writes and other side effects in the function.
Generation parses and packages source without importing its module or calling
the function. It does not verify arbitrary Spark behaviour or dependencies.

The generated quality job checks required values, keys and supported contract
rules against one committed Delta version after refresh. An error blocks dependent
job tasks; it does not undo committed data. Required unsupported rules block the
build unless the project explicitly selects reporting. SQL predicate shape checks
remain offline checks; Databricks compiles the predicates during execution.

Save reviewed logic before preparing output. If generation reports invalid Python,
correct the source and prepare again. If a source file changes after planning,
replan before generation. Failed preparation preserves the last accepted output.

Native reference: [Python materialised views](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-materialized-view),
accessed 2026-10-06. Source functions are evaluated by Lakeflow during authorised
workspace execution; review their imports, dependencies and effects before deployment.
