"""Synthetic evaluator checks; no native Spark or workspace calls."""
import contextlib
import io
from pathlib import Path
import sys
import types
import unittest
from decimal import Decimal
sys.path.insert(0, str(Path(__file__).parent / "support"))
import fake_spark
SOURCE = (Path(__file__).parents[1] / "src/templates/quality-evaluator.py").read_text()


def rule(metric="rowCount", threshold=0, operator="mustBe", **extra):
    return {"id": "synthetic-rule", "metric": metric, "threshold": threshold,
            "operator": operator, "unit": "rows", "outcome": "fail", **extra}


def run(rules, rows=None, failures=None, sql_value=0, enforcement="after-load"):
    output = io.StringIO()
    calls = []
    def query(text):
        calls.append(text)
        values = sql_value if isinstance(sql_value, list) else [(sql_value,)]
        return types.SimpleNamespace(take=lambda n: values[:n])
    ns = {"spark": types.SimpleNamespace(sql=query)}
    exec(compile(SOURCE, "quality-evaluator.py", "exec"), ns)
    rows = [{"id": 1, "name": "secret-business-value"}] if rows is None else rows
    with contextlib.redirect_stdout(output):
        result = ns["check_quality"](fake_spark.Frame(rows, failures or {}), len(rows), rules, {"identity": "synthetic", "deltaVersion": "3"}, enforcement=enforcement)
    return result, output.getvalue(), calls


class ManagedQualityTests(unittest.TestCase):
    def test_comparisons_percent_and_warning_do_not_drop_rows(self):
        for operator, threshold in [("mustBe", 1), ("mustNotBe", 0), ("mustBeGreaterThan", 0), ("mustBeGreaterOrEqualTo", 1), ("mustBeLessThan", 2), ("mustBeLessOrEqualTo", 1), ("mustBeBetween", [1, 2]), ("mustNotBeBetween", [2, 3])]:
            self.assertTrue(run([rule(threshold=threshold, operator=operator)])[0][0]["passed"])
        result, output, _ = run([rule(metric="nullValues", threshold=0, expression="id IS NOT NULL", unit="percent", outcome="warn")], failures={"id IS NOT NULL": 1})
        self.assertEqual(result[0]["value"], 100)
        self.assertFalse(result[0]["passed"])
        self.assertNotIn("secret-business-value", output)
        self.assertIn('"enforcement": "after-load"', output)

    def test_duplicates_and_required_failure_keep_explicit_after_load_semantics(self):
        with self.assertRaisesRegex(ValueError, "after ingestion; committed data and native checkpoint are unchanged"):
            run([rule(metric="duplicateValues", properties=["id"])], rows=[{"id": 1}, {"id": 1}])
        self.assertEqual(run([rule(metric="duplicateValues", column="id")], rows=[{"id": 1}, {"id": None}, {"id": None}])[0][0]["value"], 0)
        self.assertTrue(run([rule(metric="rowCount", threshold=0)], rows=[])[0][0]["passed"])

    def test_sql_reads_only_bounded_numeric_results_and_keeps_exact_decimal(self):
        result, output, calls = run([rule(metric="sql", query="SELECT COUNT(*) FROM ${table}", threshold=Decimal("0.1"))], sql_value=Decimal("0.1"))
        self.assertEqual(result[0]["value"], "0.1")
        self.assertEqual(calls, ["SELECT COUNT(*) FROM ingestron_quality_synthetic"])
        self.assertNotIn("secret-business-value", output)
        for value in [True, "business-value", float("nan"), float("inf"), Decimal("NaN"), [], [(1,), (2,)], [(1, 2)]]:
            with self.subTest(value=str(value)), self.assertRaisesRegex(ValueError, "numeric value"):
                run([rule(metric="sql", query="SELECT COUNT(*) FROM ${table}")], sql_value=value)

    def test_large_counts_are_exact_strings_in_metrics(self):
        result, output, _ = run([rule(metric="sql", query="SELECT COUNT(*) FROM ${table}", threshold=9223372036854775807)], sql_value=9223372036854775807)
        self.assertEqual(result[0]["value"], "9223372036854775807")


    def test_before_load_boundary_preserves_finite_decimal_and_exact_integer_metrics(self):
        result, output, _ = run([rule(metric="sql", query="SELECT COUNT(*) FROM ${table}", threshold=0.1)], sql_value=Decimal("0.1"), enforcement="before-load")
        self.assertTrue(result[0]["passed"])
        self.assertIn('"enforcement": "before-load"', output)
        self.assertEqual(result[0]["value"], "0.1")
        with self.assertRaisesRegex(ValueError, "snapshot not applied: synthetic-rule"):
            run([rule(threshold=0)], enforcement="before-load")
        for value in [Decimal("1e1000"), 10**1000]:
            result, _, _ = run([rule(metric="sql", query="SELECT COUNT(*) FROM ${table}", threshold=0, operator="mustBeGreaterThan")], sql_value=value)
            self.assertTrue(result[0]["passed"])
            self.assertEqual(result[0]["value"], str(value))
        with self.assertRaisesRegex(ValueError, "Unknown quality enforcement"):
            run([], enforcement="invented")
