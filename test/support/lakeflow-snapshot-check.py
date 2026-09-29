"""Synthetic callback checks; no Spark or workspace execution."""
import json
import sys
import types
from datetime import datetime, timezone, timedelta

notebook = json.load(open(sys.argv[1]))
source = "".join(notebook["cells"][-1]["source"])
state = {"rows": 2, "invalid": False, "duplicates": False}
class Frame:
    def count(self): return state["rows"]
    def filter(self, _): return Small(state["invalid"])
    def limit(self, _): return self
    def groupBy(self, *_): return Group()
class Small:
    def __init__(self, value): self.value = value
    def count(self): return int(self.value)
    def limit(self, _): return self
class Group:
    def count(self): return self
    def filter(self, _): return Small(state["duplicates"])
class Expression:
    def __invert__(self): return self
    def __gt__(self, _): return self
class Reader:
    def option(self, *_): return self
    def text(self, _): return self
    def take(self, _): return [types.SimpleNamespace(value=json.dumps(state["index"]))]
    def format(self, _): return self
    def schema(self, _): return self
    def load(self, path): state["loaded"] = path; return Frame()
dp = types.SimpleNamespace(create_streaming_table=lambda **_:None, create_auto_cdc_from_snapshot_flow=lambda **_:None, materialized_view=lambda **_:lambda f:f)
functions = types.SimpleNamespace(expr=lambda _:Expression(), lit=lambda _:Expression(), coalesce=lambda *_:Expression(),col=lambda _:Expression())
sys.modules["pyspark"] = types.SimpleNamespace(pipelines=dp)
sys.modules["pyspark.sql"] = types.SimpleNamespace(functions=functions)
namespace = {"spark":types.SimpleNamespace(read=Reader())}
exec(compile(source, "snapshot.ipynb", "exec"), namespace)
callback = next(v for k,v in namespace.items() if k.startswith("next_") and callable(v))
root = namespace["SOURCE_ROOT"]
# Definitions in another notebook must not retarget an already registered callback.
namespace["SOURCE_ROOT"] = "/Volumes/other/source"
namespace["DELIVERY_INDEX"] = "/Volumes/other/index.json"
namespace["KEYS"] = ["wrong_key"]
def delivery(version): return dict(capturedAt=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat(),contractVersion="1.0.0",id=f"snapshot-{version}",version=version,complete=True,scope="full-table",rowCount=2,path=f"{root}/snapshot-{version}")
state["index"] = dict(apiVersion="ingestron.delivery-index/v1",dataset="retail.source.customers",deliveries=[delivery(2),delivery(1)])
assert callback(None)[1] == 1
assert callback(1)[1] == 2
assert callback(2) is None
state["index"]["deliveries"] = [delivery(1)|dict(contractVersion="0.9.0"), delivery(2)]
assert callback(1)[1] == 2
state["index"]["deliveries"] = [delivery(2), delivery(1)]
for update, message in [(dict(complete=False),'incomplete'),(dict(rowCount=3),'row count'),(dict(path='/outside/data'),'source root'),(dict(path=root+'/x/..'),'source root'),(dict(contractVersion='9.0'),'contractVersion'),(dict(capturedAt='invalid'),'capturedAt')]:
    state['index']['deliveries']=[delivery(1)|update]
    try: callback(None); raise AssertionError('invalid delivery accepted')
    except ValueError as e: assert message in str(e)
state['index']['deliveries']=[delivery(1),delivery(1)]
try: callback(None); raise AssertionError('duplicate version accepted')
except ValueError: pass
state['index']['deliveries']=[delivery(1)]
state['duplicates']=True
try: callback(None); raise AssertionError('duplicate keys accepted')
except ValueError: pass
state['duplicates']=False
state['invalid']=True
try: callback(None); raise AssertionError('null keys accepted')
except ValueError: pass
state['invalid']=False
state['rows']=0
state['index']['deliveries']=[delivery(1)|dict(rowCount=0)]
try: callback(None); raise AssertionError("empty snapshot accepted")
except ValueError as error: assert "Empty snapshot blocked" in str(error)
if "--guards" in sys.argv:
    state['rows'] = 2
    state['index']['deliveries'] = [delivery(1)|dict(rowCount=100), delivery(2)]
    try: callback(1); raise AssertionError('abnormal drop accepted')
    except ValueError as error: assert 'maximumDropPercent' in str(error)
    state['index']['deliveries'] = [delivery(2)]
    try: callback(1); raise AssertionError('missing comparison accepted')
    except ValueError as error: assert 'previous delivery' in str(error)
    state['index']['deliveries'] = [delivery(2)|dict(capturedAt='2020-01-01T00:00:00Z')]
    try: callback(2); raise AssertionError('stale caught-up source accepted')
    except ValueError as error: assert 'stale' in str(error)
print('Snapshot callback ordering, completion, count, keys and empty-snapshot semantics passed (synthetic).')
