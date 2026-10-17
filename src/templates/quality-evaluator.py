"""Whole dataset quality evaluation; counts only, never row values."""
import json
import math
from decimal import Decimal
from pyspark.sql import functions as F


def check_quality(data, rows, rules, context, enforcement="after-load"):
    if enforcement not in ("before-load", "after-load"):
        raise ValueError("Unknown quality enforcement boundary")
    results = []
    for rule in rules:
        metric = rule["metric"]
        if metric == "sql":
            view = "ingestron_quality_" + context["identity"]
            data.createOrReplaceTempView(view)
            result = spark.sql(rule["query"].replace("${table}", view)).take(2)
            if len(result) != 1 or len(result[0]) != 1:
                raise ValueError("Contract SQL rule must return one numeric value: " + rule["id"])
            value = result[0][0]
            finite = value.is_finite() if isinstance(value, Decimal) else (math.isfinite(value) if isinstance(value, float) else isinstance(value, int))
            if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)) or not finite:
                raise ValueError("Contract SQL rule must return one finite numeric value: " + rule["id"])
        elif metric == "rowCount":
            value = rows
        elif metric == "duplicateValues":
            columns = [rule["column"]] if "column" in rule else rule["properties"]
            subset = data.select(*columns)
            if "column" in rule:
                subset = subset.where(F.col(rule["column"]).isNotNull())
            value = subset.count() - subset.distinct().count()
        else:
            value = data.where(~F.coalesce(F.expr(rule["expression"]), F.lit(False))).count()
        if rule["unit"] == "percent" and metric != "rowCount":
            value = round(100 * value / rows, 4) if rows else 0
        threshold = rule["threshold"]
        if isinstance(value, Decimal):
            threshold = [Decimal(str(item)) for item in threshold] if isinstance(threshold, list) else Decimal(str(threshold))
        low_high = threshold if isinstance(threshold, list) else [None, None]
        passed = {
            "mustBe": lambda: value == threshold,
            "mustNotBe": lambda: value != threshold,
            "mustBeGreaterThan": lambda: value > threshold,
            "mustBeGreaterOrEqualTo": lambda: value >= threshold,
            "mustBeLessThan": lambda: value < threshold,
            "mustBeLessOrEqualTo": lambda: value <= threshold,
            "mustBeBetween": lambda: low_high[0] <= value <= low_high[1],
            "mustNotBeBetween": lambda: not (low_high[0] <= value <= low_high[1]),
        }[rule["operator"]]()
        numeric = str(value) if isinstance(value, Decimal) or (isinstance(value, int) and abs(value) > 9007199254740991) else value
        results.append({"id": rule["id"], "outcome": rule["outcome"], "value": numeric, "passed": passed})
    print(json.dumps({"ingestronQuality": results, "context": context, "enforcement": enforcement}))
    failed = [r for r in results if r["outcome"] == "fail" and not r["passed"]]
    if failed:
        message = "Contract quality rules failed; snapshot not applied: " if enforcement == "before-load" else "Contract quality failed after ingestion; committed data and native checkpoint are unchanged: "
        raise ValueError(message + ", ".join(r["id"] for r in failed[:10]))
    return results
