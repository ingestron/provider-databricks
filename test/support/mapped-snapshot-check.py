"""Execute generated snapshot projection with synthetic frames, without Spark."""
import json
import sys
import types
from datetime import datetime, timezone, timedelta
notebook = json.load(open(sys.argv[1]))
source = "".join(notebook["cells"][-1]["source"])
state = {"index": None, "keys": None, "cdc": None, "current": None}
rows = [{"id": 1, "name": "private-one"}, {"id": 2, "name": "private-two"}]
class Expr:
    def __init__(self, name): self.name = name
    def alias(self, target): return (self.name, target)
    def __gt__(self, value): return self
class Small:
    def count(self): return 0
    def limit(self, value): return self
class Group:
    def count(self): return self
    def filter(self, expression): return Small()
class Frame:
    def __init__(self, values): self.rows = values
    def count(self): return len(self.rows)
    def select(self, *columns):
        if columns and isinstance(columns[0], tuple):
            return Frame([{target: row[physical] for physical, target in columns} for row in self.rows])
        for column in columns: assert column in self.rows[0], column
        return Frame([{column: row[column] for column in columns} for row in self.rows])
    def filter(self, expression):
        assert "order_id" in self.rows[0] and "customer_name" in self.rows[0]
        return Small()
    def groupBy(self, *keys):
        state["keys"] = keys
        assert keys == ("order_id",)
        for key in keys: assert key in self.rows[0]
        return Group()
    def where(self, expression): return self
class Reader:
    def option(self, *values): return self
    def text(self, path): return self
    def take(self, limit): return [types.SimpleNamespace(value=json.dumps(state["index"]))]
    def format(self, value): return self
    def schema(self, value):
        assert "`id` BIGINT" in value and "`name` STRING" in value
        assert "order_id" not in value and "customer_name" not in value
        return self
    def load(self, path): return Frame(rows)
    def table(self, name):
        assert name.endswith(".history.customers")
        return Frame([{ "order_id":1, "customer_name":"private-one", "__END_AT":None }])
dp=types.SimpleNamespace(create_streaming_table=lambda **values:None, create_auto_cdc_from_snapshot_flow=lambda **values:state.update(cdc=values), materialized_view=lambda **values:lambda f:f)
functions=types.SimpleNamespace(col=lambda name:Expr(name),expr=lambda value:value)
sys.modules["pyspark"]=types.SimpleNamespace(pipelines=dp)
sys.modules["pyspark.sql"]=types.SimpleNamespace(functions=functions)
namespace={"spark":types.SimpleNamespace(read=Reader())}
exec(compile(source,"mapped.ipynb","exec"),namespace)
root=namespace["SOURCE_ROOT"]
state["index"]={"apiVersion":"ingestron.delivery-index/v1","dataset":"retail.source.customers","deliveries":[{"id":"complete-1","version":1,"complete":True,"scope":"full-table","rowCount":2,"path":root+"/complete-1","contractVersion":"1.0.0","capturedAt":(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()}]}
callback=state["cdc"]["source"]
frame,version=callback(None)
assert frame.rows==[{"order_id":1,"customer_name":"private-one"},{"order_id":2,"customer_name":"private-two"}]
assert version==1 and state["cdc"]["keys"]==["order_id"]
assert state["cdc"]["track_history_column_list"]==["customer_name"]
current=next(value for name,value in namespace.items() if name.startswith("current_") and callable(value))()
assert list(current.rows[0])==["order_id","customer_name"]
assert callback(1) is None
print("Generated snapshot source schema, projection-before-checks, mapped CDC keys/tracking and current view passed (synthetic)")
