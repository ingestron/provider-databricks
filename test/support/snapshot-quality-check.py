"""Execute installed generated snapshot SQL metrics with bounded synthetic Spark."""
import contextlib
import io
import json
import sys
import types
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import fake_spark
notebook = json.load(open(sys.argv[1]))
source = "".join(notebook["cells"][-1]["source"])
assert ".collect()" not in source
state = {"value": 0, "take": []}
def query(text):
    values = state["value"] if isinstance(state["value"], list) else [(state["value"],)]
    def take(n):
        assert n == 2
        state["take"].append(n)
        return values[:n]
    return types.SimpleNamespace(take=take)
namespace = {"spark": types.SimpleNamespace(sql=query)}
exec(compile(source, "installed-snapshot.ipynb", "exec"), namespace)
check = next(value for name, value in namespace.items() if name != "check_quality" and name.startswith("check_") and name.endswith("_quality"))
frame = fake_spark.Frame([{"id": 1, "name": "private-business-value"}], {})
rule = {"id": "scalar-rule", "metric": "sql", "query": "SELECT COUNT(*) FROM ${table}", "operator": "mustBe", "threshold": 0, "unit": "rows", "outcome": "fail"}
def run(value, threshold=0, outcome="fail"):
    state["value"] = value
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        result = check(frame, 1, [rule | {"threshold": threshold, "outcome": outcome}])
    assert "private-business-value" not in output.getvalue()
    assert '"enforcement": "before-load"' in output.getvalue()
    return result, output.getvalue()
assert run(0)[0][0]["passed"]
result, output = run(Decimal("0.1"), 0.1)
assert result[0]["passed"] and result[0]["value"] == "0.1"
result, _ = run(9223372036854775807, 9223372036854775807)
assert result[0]["value"] == "9223372036854775807"
assert not run(1, outcome="warn")[0][0]["passed"]
try:
    run(1)
    raise AssertionError("required SQL metric did not block the snapshot")
except ValueError as error:
    assert str(error) == "Contract quality rules failed; snapshot not applied: scalar-rule"
for value in [True, "private-business-value", float("nan"), float("inf"), Decimal("NaN"), Decimal("Infinity"), [], [(0,), (0,), (0,)], [(0, 1)]]:
    try:
        run(value)
        raise AssertionError("invalid scalar SQL metric accepted")
    except ValueError as error:
        assert "numeric value" in str(error)
        assert "private-business-value" not in str(error)
assert state["take"] and set(state["take"]) == {2}
print("Installed snapshot bounded scalar, finite numeric, exact decimal/int64, before-load denial and metric privacy checks passed (synthetic)")
