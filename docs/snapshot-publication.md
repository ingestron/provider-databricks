# Verify and publish an ADF snapshot

The `snapshot-publication@v1` standard generates customer-owned native Python and
a job on existing classic Databricks compute. It does not install an Ingestron
runtime, require Data Flow, or host a service. The standard is implemented inside
the provider plugin.

Start with [the publisher project](../examples/snapshot-publication/project.yaml)
and [the paired history consumer](../examples/snapshot-history-from-adf/project.yaml).
The ADF provider owns the end-to-end
[Copy and recovery procedure](https://github.com/ingestron/provider-adf/blob/main/docs/snapshot-handover.md).
All examples are synthetic. Local checks pass; native Azure/Spark,
network, performance and independent engineer acceptance remain outstanding.

## Prepare the existing environment

1. Select a classic cluster permitted to read the landing ADLS path through its
   existing Spark/Unity Catalog authorisation. Put its cluster ID in
   `publication.existingClusterId`. There is no generated cluster or metastore.
2. Choose the job run-as service principal and allowed job groups. Populate
   `publication.runAsServicePrincipal` and `publication.permissions`. The workspace
   root is the product-owned path where bundle files are deployed; include the
   environment in it to keep exports separate.
3. Obtain an existing Microsoft Entra service principal credential for conditional
   Blob writes. Store its tenant ID, client ID and client secret in an existing
   Databricks secret scope. Configure only the scope/key names in `credential`.
   The notebook reads them with `dbutils.secrets.get`; no value is emitted into
   generated files or job parameters. The scope name and key names identify where
   to retrieve secrets, not the secret values themselves.
4. Prepare `<source-root>/_ingestron` and protect the landing run directories.
   Accept least-privilege access to update the index metadata, with no ability for
   ordinary ingestion identities to change completed data or index history. Check
   effective RBAC and ADLS ACLs; broad grants can override intended restrictions.
   Keep protected backups of the index and prohibit routine index deletion.
5. Permit the execution route to reach both the Spark storage path and the Azure
   Blob endpoint. Existing integration-runtime connectivity for Copy does not
   establish notebook or SDK connectivity. The job/ADF activity declares
   `azure-storage-blob==12.30.1` and `azure-identity==1.25.3`; accept their package
   installation or approved mirror on the existing cluster before use.

The generated job's run-as identity and ADF's Databricks linked-service identity
are separate. Direct ADF notebook execution uses the latter. Both must be accepted
for their intended route, including source and secret-scope access. A Unity Catalog
storage credential is not implicitly a token for the Blob SDK; the explicit
secret-scope credential supplies SDK authorisation in this standard.

## Generate and verify the export

From a copy of the publisher example:

```sh
ingestron plugin install databricks@2.0.0
ingestron validate
ingestron plan --out build/plan.json
ingestron generate --plan build/plan.json --out build/publication
ingestron validate-output build/publication
```

Expect a native notebook under `notebooks/`, a job under `resources/`, and a
handover JSON file. The handover records the dataset, contract version, source
root, delivery index and library pins. Source and contract identity parameters are
checked by the notebook before it retrieves credentials. Inspect the actual
notebook path after an authorised import/deployment and set that exact path in ADF.

## Publication behaviour

The publisher checks the actual Parquet column names, order and Spark types against
the reviewed contract. It does not silently cast types. It verifies the expected
count, required values and non-null unique keys without collecting the full dataset
to the driver. It never filters bad rows out of a snapshot: doing so could turn a
quality failure into a downstream deletion.

The source owner supplies a stable delivery ID, capture timestamp and contiguous
integer version beginning at one. Capture times cannot decrease. Completion order
must not invent source ordering. A missing version fails rather than being skipped.
Only an explicit first-time `initialiseIndex: true` can create a missing index;
normal calls default to false. Source/landing immutability and business completeness
remain owner commitments, not guarantees proven by row counts.

For each update the notebook reads the index and its ETag from the same Blob
response. It retains every existing receipt and conditionally writes the entire
new index. The payload is bounded to a single Put Blob operation. Competing writes
cannot unconditionally overwrite a newer index. A conditional conflict causes a
bounded re-read; conflicting receipt identities fail. If a successful write loses
its response, retrying the identical receipt returns `already-published` without
writing again.

## Parameters and limits

| Parameter or setting                                                    | Meaning and constraints                                                                                                                  |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `deliveryId`                                                            | Source-issued stable identity; 1–128 letters, digits, underscores or hyphens                                                             |
| `version`                                                               | Next source version, starting at 1, contiguous, no greater than 9007199254740991                                                         |
| `capturedAt`                                                            | Original snapshot capture timestamp with timezone, not retry time; cannot be in the future                                               |
| `expectedRowCount`                                                      | Independently determined whole-snapshot count; non-negative safe integer                                                                 |
| `runId`                                                                 | Original ADF copy RunId; selects one directory beneath the fixed source root                                                             |
| `initialiseIndex`                                                       | False by default; true only for deliberate first publication                                                                             |
| `dataset`, `sourceRoot`, `contractVersion`, `contractShape`, `protocol` | Bound by generated ADF/job configuration and checked against notebook constants; changing one requires regenerating the matching exports |
| `allowEmpty`                                                            | False by default. Even when true, an empty snapshot must have readable schema-bearing Parquet. Missing/unreadable files fail.            |
| `maximumDropPercent`                                                    | Optional 0–100 threshold against the preceding committed receipt; does not establish business completeness                               |
| Index retention                                                         | Every version remains in order; at most 10,000 receipts and 4 MiB. Limits fail without pruning.                                          |
| SDK conflict retries                                                    | At most five compare-and-swap attempts; job/activity retry repeats the identical receipt                                                 |

The index conforms to `ingestron.delivery-index/v1`. Only complete full-table
receipts appear. The existing snapshot-with-history consumer revalidates data and
keys when consuming them. A count, schema or access failure before publication
leaves the prior index readable. An unsuccessful publication can leave a valid
unpublished landing folder; recover publication with the original run ID.

## Recovery and native acceptance

Do not recopy to recover an already committed delivery, change its metadata, prune
entries, or treat a missing historical index as a new dataset. Restore/reconcile a
lost index before resuming. Failed bootstrap before any committed write requires
an explicit operator decision to retry initialisation. Protected backups and
source retention are external responsibilities; this provider creates no backup,
retention or storage-deletion policy.

Before production, test actual Spark typing, null/duplicate rejection, zero-row
file behaviour, Copy and publication failures, private networking, both execution
identities, ETag contention, a lost-response retry, index reader visibility,
restart/replay, monitoring and representative volumes/costs. Source mutation after
validation remains unsafe; enforce immutable completed landing data operationally.
Azure execution and independent-user evidence have not yet been collected.

## Evidence and primary sources

Offline tests cover the publication state machine, synthetic Spark checks and the
actual Azure SDK through an in-memory HTTP transport. CLI tests install provider
source into temporary Git locks and pass generated index bytes into the existing
snapshot callback. No Azure credentials, source data or network calls are used by
those tests. Downloading development dependencies is separate from test execution.

Primary sources accessed 2026-09-12:

- [Blob optimistic concurrency](https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage)
- [BlobClient SDK](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python)
- [Put Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob)
- [ADF Databricks Notebook activity](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-notebook)
- [Databricks secret management](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/)
- [Azure Storage Blob 12.30.1](https://pypi.org/project/azure-storage-blob/12.30.1/)
- [Azure Identity 1.25.3](https://pypi.org/project/azure-identity/1.25.3/)
