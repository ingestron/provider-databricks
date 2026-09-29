"""Synthetic publication state machine and Spark-boundary tests; no cloud calls."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("publication", Path(__file__).parents[1] / "plugin/activities/snapshot-publish/publication.py")
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
ROOT = "abfss://landing@retailstore.dfs.core.windows.net/retail/source/customers"
DATASET = "retail.source.customers"
NOW = datetime(2026, 9, 12, tzinfo=timezone.utc)


def receipt(version=1, **updates):
    return {"id": "delivery_" + str(version), "version": version, "capturedAt": "2026-09-11T00:00:00Z", "contractVersion": "1.0.0", "complete": True, "scope": "full-table", "rowCount": 2, "path": ROOT + "/run_" + str(version), **updates}


class Store:
    def __init__(self):
        self.raw, self.etag, self.writes = None, None, 0
        self.before_write = None
        self.lose_response = False

    def read(self):
        return self.raw, self.etag

    def compare_and_swap(self, body, etag):
        if self.before_write:
            callback, self.before_write = self.before_write, None
            callback()
        if etag != self.etag:
            raise p.ConcurrentUpdate()
        self.raw = body
        self.writes += 1
        self.etag = str(self.writes)
        if self.lose_response:
            self.lose_response = False
            raise TimeoutError("synthetic response lost after atomic commit")


def publish(store, item=None, verify=lambda: None, **options):
    return p.publish_snapshot(store, item or receipt(), DATASET, ROOT, verify, now=NOW, **options)


class PublicationTests(unittest.TestCase):
    def test_bootstrap_is_explicit(self):
        store = Store()
        with self.assertRaisesRegex(ValueError, "initialisation"):
            publish(store)
        self.assertEqual(store.writes, 0)
        self.assertEqual(publish(store, initialise=True)["status"], "published")

    def test_ordered_cumulative_publication_preserves_prior_entries(self):
        store = Store()
        publish(store, initialise=True)
        first = json.loads(store.raw)["deliveries"][0]
        for version in range(2, 8):
            publish(store, receipt(version))
        index = p.parse_index(store.raw, DATASET, ROOT)
        self.assertEqual([x["version"] for x in index["deliveries"]], list(range(1, 8)))
        self.assertEqual(index["deliveries"][0], first)

    def test_verification_failure_never_creates_or_updates_index(self):
        store = Store()
        def invalid():
            raise ValueError("invalid rows")
        with self.assertRaisesRegex(ValueError, "invalid rows"):
            publish(store, verify=invalid, initialise=True)
        self.assertIsNone(store.raw)
        publish(store, initialise=True)
        prior = store.read()
        with self.assertRaises(ValueError):
            publish(store, receipt(2), verify=invalid)
        self.assertEqual(store.read(), prior)

    def test_response_loss_retries_are_idempotent(self):
        store = Store()
        store.lose_response = True
        with self.assertRaises(TimeoutError):
            publish(store, initialise=True)
        self.assertEqual(publish(store)["status"], "already-published")
        self.assertEqual(store.writes, 1)

    def test_same_receipt_concurrent_writers_converge(self):
        store = Store()
        store.before_write = lambda: publish(store, initialise=True)
        self.assertEqual(publish(store, initialise=True)["status"], "already-published")
        self.assertEqual(store.writes, 1)

    def test_conflicting_concurrent_writer_cannot_overwrite_winner(self):
        store = Store()
        publish(store, initialise=True)
        winner = receipt(2, id="other_delivery", path=ROOT + "/other_run")
        store.before_write = lambda: publish(store, winner)
        with self.assertRaisesRegex(ValueError, "different metadata"):
            publish(store, receipt(2))
        self.assertEqual(json.loads(store.raw)["deliveries"][-1], winner)
        self.assertEqual(store.writes, 2)

    def test_out_of_order_and_reused_identifiers_fail_closed(self):
        store = Store()
        publish(store, initialise=True)
        for item in [receipt(3), receipt(2, id="delivery_1"), receipt(2, path=ROOT + "/run_1"), receipt(1, rowCount=3), receipt(2, capturedAt="2026-09-10T00:00:00Z")]:
            with self.subTest(item=item), self.assertRaises(ValueError):
                publish(store, item)
        self.assertEqual(store.writes, 1)

    def test_empty_drop_and_future_policies(self):
        store = Store()
        publish(store, initialise=True)
        for item, options in [(receipt(2, rowCount=0), {}), (receipt(2, rowCount=1), {"maximum_drop_percent": 40}), (receipt(2, capturedAt="2027-01-01T00:00:00Z"), {})]:
            with self.assertRaises(ValueError):
                publish(store, item, **options)
        publish(store, receipt(2, rowCount=0), allow_empty=True)
        self.assertEqual(store.writes, 2)

    def test_invalid_receipts_rejected_before_store_access(self):
        for updates in [{"version": True}, {"version": 0}, {"rowCount": -1}, {"rowCount": 2**53}, {"complete": False}, {"scope": "partial"}, {"capturedAt": "2026-09-11"}, {"path": ROOT + "/../escape"}, {"path": ROOT + "/run_1/extra"}, {"extra": "ignored"}]:
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                publish(Store(), receipt(**updates), initialise=True)

    def test_corrupt_wrong_dataset_pruned_and_duplicate_json_indexes_rejected(self):
        store = Store()
        publish(store, initialise=True)
        original = json.loads(store.raw)
        corrupt = [{**original, "dataset": "wrong.source.customers"}, {**original, "deliveries": [receipt(2)]}, {**original, "deliveries": [receipt(), receipt()]}, {**original, "extra": True}]
        for index in corrupt:
            store.raw = json.dumps(index).encode()
            with self.assertRaises(ValueError):
                publish(store, receipt(2))
        store.raw = b'{"dataset":"a","dataset":"b"}'
        with self.assertRaisesRegex(ValueError, "Duplicate JSON"):
            publish(store, receipt(2))
        self.assertEqual(store.writes, 1)

    def test_missing_index_during_normal_recovery_requires_restore(self):
        store = Store()
        publish(store, initialise=True)
        store.raw, store.etag = None, None
        with self.assertRaisesRegex(ValueError, "restore"):
            publish(store)
        with self.assertRaises(ValueError):
            publish(store, receipt(2), initialise=True)

    def test_retention_capacity_never_prunes_old_receipts(self):
        store = Store()
        publish(store, initialise=True)
        previous = store.read()
        with patch.object(p, "MAX_DELIVERIES", 1), self.assertRaisesRegex(ValueError, "retention"):
            publish(store, receipt(2))
        self.assertEqual(store.read(), previous)

    def test_persistent_contention_is_bounded(self):
        store = Store()
        store.compare_and_swap = lambda *_: (_ for _ in ()).throw(p.ConcurrentUpdate())
        with self.assertRaisesRegex(RuntimeError, "retry limit"):
            publish(store, initialise=True, attempts=2)
        self.assertIsNone(store.raw)


COLUMNS = [{"name": "id", "type": "BIGINT", "required": True, "key": True}, {"name": "name", "type": "STRING", "required": False, "key": False}]


class Frame:
    def __init__(self, rows, dtypes=None):
        self.rows = rows
        self.dtypes = dtypes or [("id", "bigint"), ("name", "string")]

    def count(self): return len(self.rows)
    def limit(self, count): return Frame(self.rows[:count], self.dtypes)
    def filter(self, expression):
        if expression == "count > 1":
            return Frame([row for row in self.rows if row["count"] > 1])
        names = [part.split("`")[1] for part in expression.split(" OR ")]
        return Frame([row for row in self.rows if any(row[name] is None for name in names)], self.dtypes)
    def groupBy(self, *keys):
        rows = self.rows
        class Group:
            def count(self):
                groups = {}
                for row in rows:
                    key = tuple(row[name] for name in keys)
                    groups[key] = groups.get(key, 0) + 1
                return Frame([{"count": count} for count in groups.values()])
        return Group()


class RowTests(unittest.TestCase):
    def test_valid_rows(self):
        p.validate_rows(Frame([{"id": 1, "name": "one"}, {"id": 2, "name": None}]), COLUMNS, ["id"], 2)

    def test_schema_null_duplicate_and_count_failures(self):
        frames = [Frame([{"id": 1, "name": "one"}], [("id", "int"), ("name", "string")]), Frame([{"id": None, "name": "one"}]), Frame([{"id": 1, "name": "one"}, {"id": 1, "name": "two"}])]
        for frame in frames:
            with self.assertRaises(ValueError):
                p.validate_rows(frame, COLUMNS, ["id"], frame.count())
        with self.assertRaises(ValueError):
            p.validate_rows(Frame([]), COLUMNS, ["id"], 1)

    def test_empty_is_explicit_and_schema_must_still_exist(self):
        with self.assertRaises(ValueError):
            p.validate_rows(Frame([]), COLUMNS, ["id"], 0)
        p.validate_rows(Frame([]), COLUMNS, ["id"], 0, True)


class AdapterTests(unittest.TestCase):
    def setUp(self):
        class Missing(Exception): pass
        class Exists(Exception): pass
        class Modified(Exception): pass
        self.Missing, self.Exists, self.Modified = Missing, Exists, Modified
        self.modules = {name: types.ModuleType(name) for name in ['azure', 'azure.core', 'azure.core.exceptions', 'azure.storage', 'azure.storage.blob']}
        self.modules['azure.core'].MatchConditions = types.SimpleNamespace(IfNotModified='if-match')
        self.modules['azure.core.exceptions'].ResourceNotFoundError = Missing
        self.modules['azure.core.exceptions'].ResourceExistsError = Exists
        self.modules['azure.core.exceptions'].ResourceModifiedError = Modified
        self.modules['azure.storage.blob'].ContentSettings = lambda **kwargs: kwargs
        self.patch = patch.dict(sys.modules, self.modules)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_conditional_headers_and_etag_are_never_omitted_on_update(self):
        calls = []
        blob = types.SimpleNamespace(upload_blob=lambda *args, **kwargs: calls.append(kwargs))
        store = p.AzureBlobIndex(blob)
        store.compare_and_swap(b'{}', None)
        store.compare_and_swap(b'{}', 'etag_1')
        self.assertIs(calls[0]['overwrite'], False)
        self.assertEqual(calls[1]['etag'], 'etag_1')
        self.assertEqual(calls[1]['match_condition'], 'if-match')
        self.assertEqual(calls[1]['content_settings']['content_type'], 'application/json')

    def test_missing_container_is_not_treated_as_a_new_index(self):
        error = self.Missing()
        error.error_code = 'ContainerNotFound'
        def missing(**kwargs): raise error
        store = p.AzureBlobIndex(types.SimpleNamespace(download_blob=missing))
        with self.assertRaises(self.Missing): store.read()
        error.error_code = 'BlobNotFound'
        self.assertEqual(store.read(), (None, None))

    def test_existing_blob_and_failed_etag_become_concurrency_retries(self):
        for error in [self.Exists(), self.Modified()]:
            def fail(*args, **kwargs): raise error
            store = p.AzureBlobIndex(types.SimpleNamespace(upload_blob=fail))
            with self.assertRaises(p.ConcurrentUpdate): store.compare_and_swap(b'{}', 'etag')


if __name__ == '__main__':
    unittest.main()
