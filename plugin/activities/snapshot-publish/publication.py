"""Project-owned snapshot publication. Only AzureBlobIndex performs cloud I/O."""
import json
import re
from datetime import datetime, timezone

MAX_INTEGER = 9007199254740991
MAX_INDEX_BYTES = 4 * 1024 * 1024
MAX_DELIVERIES = 10000
RECEIPT_KEYS = {"id", "version", "capturedAt", "contractVersion", "complete", "scope", "rowCount", "path"}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def timestamp(value):
    require(isinstance(value, str), "capturedAt must be an ISO timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "capturedAt requires a timezone")
    return parsed


def validate_receipt(receipt, source_root):
    require(isinstance(receipt, dict) and set(receipt) == RECEIPT_KEYS, "Invalid delivery receipt fields")
    require(isinstance(receipt["id"], str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", receipt["id"]), "Invalid delivery ID")
    for key in ("version", "rowCount"):
        require(type(receipt[key]) is int and 0 <= receipt[key] <= MAX_INTEGER, "Invalid delivery integer")
    require(receipt["version"] >= 1, "Publication versions start at one")
    require(receipt["complete"] is True and receipt["scope"] == "full-table", "Only complete full-table snapshots may be published")
    require(isinstance(receipt["contractVersion"], str) and receipt["contractVersion"], "Contract version is required")
    timestamp(receipt["capturedAt"])
    require(isinstance(receipt["path"], str) and receipt["path"].startswith(source_root + "/"), "Delivery path is outside source root")
    relative = receipt["path"][len(source_root) + 1:]
    require(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", relative), "Delivery path must name one immutable run directory")


def parse_index(raw, dataset, source_root):
    require(len(raw) <= MAX_INDEX_BYTES, "Delivery index exceeds size limit")
    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "Duplicate JSON key in delivery index")
            result[key] = value
        return result
    index = json.loads(raw, object_pairs_hook=unique_keys)
    require(isinstance(index, dict) and set(index) == {"apiVersion", "dataset", "deliveries"}, "Invalid delivery index fields")
    require(index["apiVersion"] == "ingestron.delivery-index/v1" and index["dataset"] == dataset, "Delivery index identity mismatch")
    entries = index["deliveries"]
    require(isinstance(entries, list) and 1 <= len(entries) <= MAX_DELIVERIES, "Invalid retained delivery count")
    ids, paths = set(), set()
    previous_time = None
    for version, item in enumerate(entries, 1):
        validate_receipt(item, source_root)
        require(item["version"] == version, "Delivery index must retain every version in order, starting at one")
        require(item["id"] not in ids and item["path"] not in paths, "Delivery identities and paths must be unique")
        captured = timestamp(item["capturedAt"])
        require(previous_time is None or captured >= previous_time, "Capture times must follow version order")
        ids.add(item["id"])
        paths.add(item["path"])
        previous_time = captured
    return index


def validate_rows(data, columns, keys, expected_count, allow_empty=False):
    """Spark DataFrame checks; never collect a whole dataset to the driver."""
    types = {"STRING": "string", "BIGINT": "bigint", "INT": "int", "INTEGER": "int", "SMALLINT": "smallint", "DOUBLE": "double", "FLOAT": "float", "BOOLEAN": "boolean", "DATE": "date", "TIMESTAMP": "timestamp", "BINARY": "binary"}
    expected = [(c["name"], types.get(c["type"], c["type"].lower().replace(" ", ""))) for c in columns]
    require(data.dtypes == expected, "Landed Parquet schema differs from the reviewed contract; implicit casts are forbidden")
    require(type(expected_count) is int and 0 <= expected_count <= MAX_INTEGER, "Invalid expected row count")
    require(expected_count > 0 or allow_empty, "Empty snapshots require explicit allowEmpty")
    require(data.count() == expected_count, "Landed row count differs from expected row count")
    required = [c["name"] for c in columns if c["required"] or c["name"] in keys]
    if required:
        require(data.filter(" OR ".join("`" + name + "` IS NULL" for name in required)).limit(1).count() == 0, "Snapshot contains null required values or keys")
    require(data.groupBy(*keys).count().filter("count > 1").limit(1).count() == 0, "Snapshot keys are not unique")


class ConcurrentUpdate(Exception):
    """A conditional index write lost a race; no unconditional overwrite is safe."""


def publish_snapshot(store, receipt, dataset, source_root, verify, allow_empty=False, maximum_drop_percent=None, attempts=5, now=None, initialise=False):
    validate_receipt(receipt, source_root)
    require(re.fullmatch(r"[A-Za-z_][\w-]*\.[A-Za-z_][\w-]*\.[A-Za-z_][\w-]*", dataset), "Invalid dataset identity")
    require(receipt["rowCount"] > 0 or allow_empty, "Empty snapshots require explicit allowEmpty")
    require(timestamp(receipt["capturedAt"]) <= (now or datetime.now(timezone.utc)), "Future snapshots cannot be published")
    require(maximum_drop_percent is None or type(maximum_drop_percent) in (int, float) and 0 <= maximum_drop_percent <= 100, "Invalid maximum row-drop policy")
    require(type(attempts) is int and 1 <= attempts <= 10, "Publication attempts must be bounded")
    verified = False
    for _ in range(attempts):
        raw, etag = store.read()
        require(raw is not None or initialise and receipt["version"] == 1, "Missing index: explicit one-time initialisation is required; restore a lost index")
        index = parse_index(raw, dataset, source_root) if raw is not None else {"apiVersion": "ingestron.delivery-index/v1", "dataset": dataset, "deliveries": []}
        require((raw is None) == (etag is None), "Index content and ETag must come from the same read")
        entries = index["deliveries"]
        for existing in entries:
            if existing["id"] == receipt["id"] or existing["version"] == receipt["version"] or existing["path"] == receipt["path"]:
                require(existing == receipt, "A delivery ID, version or path is already committed with different metadata")
                return {"status": "already-published", "version": existing["version"], "dataset": dataset}
        require(receipt["version"] == len(entries) + 1, "Publish the next source version; out-of-order or missing versions require recovery")
        require(len(entries) < MAX_DELIVERIES, "Index retention limit reached; do not prune unprocessed deliveries")
        if entries:
            previous = entries[-1]
            require(timestamp(receipt["capturedAt"]) >= timestamp(previous["capturedAt"]), "Snapshot capture time precedes the committed version")
            if maximum_drop_percent is not None and previous["rowCount"]:
                require(100 * (previous["rowCount"] - receipt["rowCount"]) / previous["rowCount"] <= maximum_drop_percent, "Snapshot row-count drop exceeds policy")
        if not verified:
            verify()
            verified = True
        body = json.dumps({**index, "deliveries": entries + [receipt]}, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        require(len(body) <= MAX_INDEX_BYTES, "Delivery index exceeds size limit; no index was changed")
        try:
            store.compare_and_swap(body, etag)
            return {"status": "published", "version": receipt["version"], "dataset": dataset}
        except ConcurrentUpdate:
            continue
    raise RuntimeError("Publication contention exceeded its retry limit; rerun publication with the same receipt")


class AzureBlobIndex:
    def __init__(self, blob):
        self.blob = blob

    def read(self):
        from azure.core.exceptions import ResourceNotFoundError
        try:
            download = self.blob.download_blob(max_concurrency=1)
            require(download.properties.size <= MAX_INDEX_BYTES, "Delivery index exceeds size limit")
            return download.readall(), download.properties.etag
        except ResourceNotFoundError as error:
            # A missing container/account is not an empty index.
            if error.error_code != "BlobNotFound":
                raise
            return None, None

    def compare_and_swap(self, body, etag):
        from azure.core import MatchConditions
        from azure.core.exceptions import ResourceExistsError, ResourceModifiedError
        from azure.storage.blob import ContentSettings
        try:
            kwargs = {"overwrite": False} if etag is None else {"overwrite": True, "etag": etag, "match_condition": MatchConditions.IfNotModified}
            self.blob.upload_blob(body, blob_type="BlockBlob", content_settings=ContentSettings(content_type="application/json"), **kwargs)
        except (ResourceExistsError, ResourceModifiedError) as error:
            raise ConcurrentUpdate() from error
