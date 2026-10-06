"""Exercise generated managed notebook version and SCD2 boundaries offline."""
import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import types
import fake_spark
source = "\n".join("".join(c["source"]) for c in json.loads(Path(sys.argv[1]).read_text())["cells"] if c["cell_type"] == "code")
compile(source, "managed-notebook.py", "exec")
ROWS = [{"order_id": 1, "status": "open", "__END_AT": None}, {"order_id": 1, "status": "old-secret-value", "__END_AT": 1}]
class Type:
    def __init__(self, name): self.name=name
    def simpleString(self): return self.name
fake_spark.Expr.isNull = lambda self: fake_spark.Expr("isnull", self)
class Frame(fake_spark.Frame):
    wrong_type = False
    @property
    def schema(self): return types.SimpleNamespace(fields=[types.SimpleNamespace(name="order_id",dataType=Type("bigint")),types.SimpleNamespace(name="status",dataType=Type("bigint" if self.wrong_type else "string"))])
    def limit(self, n): return Frame(self.rows[:n], self.failing)
    @property
    def columns(self): return list(self.rows[0]) if self.rows else ["order_id", "status", "__END_AT"]
    def where(self, condition):
        if condition == "__END_AT IS NULL": return Frame([r for r in self.rows if r["__END_AT"] is None], self.failing)
        if isinstance(condition, fake_spark.Expr) and condition.kind == "isnull":
            return Frame([r for r in self.rows if r[condition.value.value] is None], self.failing)
        return super().where(condition)
class Reader:
    def __init__(self, version, missing, invalid=None): self.version, self.missing, self.invalid, self.options, self.reads = version, missing, invalid, {}, []
    def option(self, name, value): self.options[name] = value; return self
    def table(self, name):
        assert self.options == {"versionAsOf": str(self.version)}
        self.reads.append(name)
        rows = copy.deepcopy(ROWS)
        Frame.wrong_type = self.invalid == "type"
        if self.invalid == "required": rows[0]["order_id"] = None
        if self.invalid == "scd":
            for row in rows: row.pop("__END_AT")
        if self.missing:
            for row in rows: row.pop("status")
        return Frame(rows, {"`status` IS NOT NULL": 0, "`order_id` IS NOT NULL": 0})
class Spark:
    def __init__(self, version=7, missing=False, invalid=None): self.version=version;self.read=Reader(version,missing,invalid)
    def sql(self, query):
        assert query.startswith("DESCRIBE HISTORY ") and query.endswith(" LIMIT 1")
        return types.SimpleNamespace(take=lambda count: [{"version": self.version}][:count])
def run(spark):
    output=io.StringIO()
    with contextlib.redirect_stdout(output): exec(compile(source,"managed-notebook.py","exec"),{"spark":spark})
    assert "old-secret-value" not in output.getvalue()
    return output.getvalue()
spark=Spark();output=run(spark)
assert spark.read.reads == ["`retail`.`bronze`.`orders`"]
assert '"deltaVersion": "7"' in output and '"passed": true' in output
for spark, message in [(Spark(missing=True),"missing declared"),(Spark(version=True),"committed Delta version"),(Spark(version=-1),"committed Delta version"),(Spark(invalid="type"),"types differ"),(Spark(invalid="required"),"Required contract values failed after ingestion"),(Spark(invalid="scd"),"current-row boundary")]:
    try: run(spark)
    except ValueError as error: assert message in str(error)
    else: raise AssertionError("Expected denial")
print("Managed quality committed-version and current-row semantics passed")
