"""Execute immutable installed generated callbacks with actual local Spark, no vendor."""
from pathlib import Path
from datetime import date,datetime,timezone
import contextlib,io,json,os,sys,types
notebook,runtime,output,format=sys.argv[1:]
root=Path(output).resolve();root.mkdir(parents=True,exist_ok=True)
info=json.loads(Path(runtime).read_text());os.environ['JAVA_HOME']=info['jdk']['home'];os.environ['SPARK_LOCAL_IP']='127.0.0.1';os.environ['PYSPARK_PYTHON']=sys.executable
from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq
spark=SparkSession.builder.master('local[1]').appName('Ingestron generated temporal callback').config('spark.ui.enabled','false').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1').config('spark.sql.shuffle.partitions','1').config('spark.local.dir',str(root/'spark-scratch')).config('spark.sql.warehouse.dir',str(root/'warehouse')).getOrCreate()
spark.sparkContext.setLogLevel('OFF')
registered=[]
dp=types.ModuleType('pyspark.pipelines')
dp.create_streaming_table=lambda **kw:None
dp.create_auto_cdc_from_snapshot_flow=lambda **kw:registered.append(kw['source'])
dp.materialized_view=lambda **kw:lambda fn:fn
sys.modules['pyspark.pipelines']=dp
code=''.join(json.loads(Path(notebook).read_text())['cells'][-1]['source']);namespace={'spark':spark}
records=[{'id':1,'day':'2024-02-29','event':'2024-02-29T01:02:03.123456','label':'PRIVATE_NATIVE_RECORD'},{'id':2,'day':'2024-03-01','event':None,'label':'second'},{'id':3,'day':'2024-02-29','event':'2024-02-29T23:59:59','label':'third'}]
checks=[]
try:
    exec(compile(code,'immutable-generated-temporal.ipynb','exec'),namespace)
    assert len(registered)==1;callback=registered[0]
    sequence=0
    def fixture(values,unit='us',zone=None,mixed=False):
        global sequence
        sequence+=1;folder=root/('case-'+str(sequence));folder.mkdir();path=folder/('records.'+('json' if format=='json' else 'parquet'))
        if format=='json':path.write_text('\n'.join(json.dumps(value) for value in values)+'\n')
        else:
            clock=pa.array([datetime.fromisoformat(value['event']) if value['event'] else None for value in values],type=pa.timestamp(unit,tz=zone))
            table=pa.table({'id':pa.array([value['id'] for value in values],type=pa.int64()),'day':pa.array([date.fromisoformat(value['day']) for value in values],type=pa.date32()),'event':clock,'label':[value['label'] for value in values]});pq.write_table(table,path)
            if mixed:
                mixed_folder=folder/'mixed';mixed_folder.mkdir();pq.write_table(table,mixed_folder/'first.parquet')
                table=table.set_column(table.schema.get_field_index('event'),'event',clock.cast(pa.timestamp(unit,tz='UTC')));pq.write_table(table,mixed_folder/'later.parquet');path=mixed_folder
        index=folder/'delivery.json';index.write_text(json.dumps({'apiVersion':'ingestron.delivery-index/v1','dataset':'retail.source.orders','deliveries':[{'id':'case-'+str(sequence),'version':sequence,'contractVersion':'1.0.0','capturedAt':datetime.now(timezone.utc).isoformat(),'complete':True,'scope':'full-table','rowCount':len(values),'path':str(path)}]}))
        return index,sequence
    def invoke(index,latest=None):
        receipt=io.StringIO()
        with contextlib.redirect_stdout(receipt):result=callback(latest,source_root=str(root),delivery_index=str(index))
        assert 'PRIVATE_NATIVE_RECORD' not in receipt.getvalue()
        return result,receipt.getvalue()
    index,version=fixture(records)
    for zone in ['UTC','Pacific/Auckland','America/Los_Angeles']:
        spark.conf.set('spark.sql.session.timeZone',zone)
        (data,actual),receipt=invoke(index)
        assert actual==version;assert data.schema['day'].dataType.simpleString()=='date';assert data.schema['event_time'].dataType.simpleString()=='timestamp_ntz'
        rows=data.orderBy('id').collect();assert rows[0].day==date(2024,2,29);assert rows[0].event_time==datetime(2024,2,29,1,2,3,123456);assert rows[1].event_time is None
        assert '"enforcement": "before-load"' in receipt
    assert invoke(index,version)[0] is None
    checks.append('Installed generated callback returns exact mapped DATE/TIMESTAMP_NTZ/null values with typed day/clock quality across three session timezones; caught-up callback returns no snapshot')
    bad=[{**value,'day':'2024-03-02' if value['id']==2 else value['day']} for value in records];index,_=fixture(bad)
    try:invoke(index);raise AssertionError('Native temporal quality did not block')
    except ValueError as error:
        assert 'day-valid' in str(error);assert '2024-03-02' not in str(error);assert 'PRIVATE_NATIVE_RECORD' not in str(error)
    checks.append('A valid but disallowed source calendar day fails typed whole-snapshot quality before callback delivery with rule identities and no record payload')
    if format=='json':
        for field,value in [('day','2023-02-29'),('day','0000-01-01'),('event','2024-02-29T01:02:03.1234567'),('event','2024-02-29T01:02:03+13:00')]:
            bad=[{**row,field:value if row['id']==1 else row[field]} for row in records];index,_=fixture(bad)
            try:invoke(index);raise AssertionError('Invalid temporal text delivered')
            except ValueError as error:
                assert 'exact calendar date' in str(error);assert value not in str(error);assert 'PRIVATE_NATIVE_RECORD' not in str(error)
        checks.append('All calendar/range/overprecision/offset text violations are rejected before native quality or CDC receives data, using fixed guidance')
    else:
        for unit,zone,mixed in [('us','UTC',False),('ns',None,False),('us',None,True)]:
            index,_=fixture(records,unit,zone,mixed)
            try:invoke(index);raise AssertionError('Unsafe temporal footer delivered')
            except Exception as error:
                assert not isinstance(error,AssertionError);assert 'PRIVATE_NATIVE_RECORD' not in str(error)
        checks.append('Native source footer preflight rejects zoned, nanosecond and mixed-late-file schemas before an explicit schema can erase their distinctions')
    proof={'apiVersion':'ingestron.native-temporal-spark-acceptance/v1','result':'passed','spark':spark.version,'format':format,'generatedCodeSha256':__import__('hashlib').sha256(code.encode()).hexdigest(),'checks':checks,'limits':['Actual local Spark and immutable compiler/provider generated callback only','Lakeflow registration is a fixture; no Databricks workspace/CDC/deployment qualification']}
    (root/'acceptance.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps({'result':'passed','checks':len(checks),'format':format}))
finally:spark.stop()
