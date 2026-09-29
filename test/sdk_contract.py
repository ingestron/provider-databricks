"""Real Azure SDK over a memory-only transport. No credentials or network."""
import importlib.util
import json
from pathlib import Path
import unittest
from datetime import datetime, timezone
from azure.core.pipeline.transport import HttpTransport, HttpResponse
from azure.core.utils import CaseInsensitiveDict
from azure.core.exceptions import ServiceResponseError
from azure.storage.blob import BlobClient

spec = importlib.util.spec_from_file_location('publication', Path(__file__).parents[1] / 'plugin/activities/snapshot-publish/publication.py')
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
ROOT = 'abfss://landing@retailstore.dfs.core.windows.net/retail/source/customers'


class Response(HttpResponse):
    def __init__(self, request, status, headers, body=b''):
        super().__init__(request, None)
        self.status_code, self.headers, self._body = status, CaseInsensitiveDict(headers), body
        self.reason = 'Synthetic response'
        self.content_type = headers.get('Content-Type')
    def read(self): return self._body
    def close(self): pass
    @property
    def content(self): return self._body
    def json(self): return json.loads(self._body)
    def body(self): return self._body
    def text(self, encoding=None): return self._body.decode(encoding or 'utf-8')
    def stream_download(self, pipeline, **kwargs):
        class Stream:
            def __init__(self, body): self.chunks = iter([body]); self.content_length = len(body)
            def __iter__(self): return self
            def __next__(self): return next(self.chunks)
        return Stream(self._body)


class MemoryTransport(HttpTransport):
    def __init__(self):
        self.data, self.version, self.calls, self.lose_response = None, 0, [], False
    def open(self): pass
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def send(self, request, **kwargs):
        headers = {k.lower(): v for k, v in request.headers.items()}
        self.calls.append((request.method, headers))
        response_headers = {'x-ms-request-id':'offline', 'x-ms-version':'2026-06-06', 'ETag':'"'+str(self.version)+'"', 'Last-Modified':'Fri, 11 Sep 2026 00:00:00 GMT', 'x-ms-blob-type':'BlockBlob'}
        if request.method == 'GET':
            if self.data is None:
                return Response(request, 404, {**response_headers, 'x-ms-error-code':'BlobNotFound'})
            return Response(request, 206, {**response_headers, 'Content-Length':str(len(self.data)), 'Content-Range':f'bytes 0-{len(self.data)-1}/{len(self.data)}', 'Content-Type':'application/json'}, self.data)
        if request.method == 'PUT':
            if (headers.get('if-none-match') == '*' and self.data is not None) or ('if-match' in headers and headers['if-match'] != '"'+str(self.version)+'"'):
                return Response(request, 412, {**response_headers, 'x-ms-error-code':'ConditionNotMet'})
            assert headers.get('x-ms-blob-type') == 'BlockBlob'
            assert headers.get('if-none-match') == '*' or 'if-match' in headers
            assert 'comp=' not in request.url, 'Expected one Put Blob, not a staged multipart write'
            self.data = request.body
            if not isinstance(self.data, bytes): self.data = self.data.read()
            self.version += 1
            if self.lose_response:
                self.lose_response = False
                raise ServiceResponseError('Synthetic lost response')
            return Response(request, 201, {**response_headers, 'ETag':'"'+str(self.version)+'"'})
        raise AssertionError('Unexpected SDK request '+request.method)


class SdkContract(unittest.TestCase):
    def setUp(self):
        self.transport = MemoryTransport()
        self.client = BlobClient('https://retailstore.blob.core.windows.net', 'landing', 'index.json', transport=self.transport, retry_total=0, max_single_put_size=p.MAX_INDEX_BYTES)
        self.store = p.AzureBlobIndex(self.client)
        self.addCleanup(self.client.close)
    def receipt(self, version):
        return dict(id='delivery_'+str(version),version=version,capturedAt='2026-09-11T00:00:00Z',contractVersion='1.0.0',complete=True,scope='full-table',rowCount=2,path=ROOT+'/run_'+str(version))
    def publish(self, version=1, initialise=False):
        return p.publish_snapshot(self.store,self.receipt(version),'retail.source.customers',ROOT,lambda:None,now=datetime(2026,9,12,tzinfo=timezone.utc),initialise=initialise)
    def test_real_sdk_put_blob_headers_and_download_etag(self):
        self.publish(initialise=True)
        raw, etag = self.store.read()
        self.assertEqual(etag,'"1"')
        self.assertEqual(json.loads(raw)['deliveries'][0],self.receipt(1))
        self.publish(2)
        puts=[headers for method,headers in self.transport.calls if method=='PUT']
        self.assertEqual(puts[0]['if-none-match'],'*')
        self.assertEqual(puts[1]['if-match'],'"1"')
        self.assertEqual(len(json.loads(self.transport.data)['deliveries']),2)
    def test_real_sdk_maps_stale_etag_to_retryable_conflict(self):
        self.publish(initialise=True)
        with self.assertRaises(p.ConcurrentUpdate): self.store.compare_and_swap(b'{}','"stale"')
        self.assertEqual(self.transport.version,1)
    def test_lost_put_response_recovers_with_read_without_second_put(self):
        self.transport.lose_response=True
        with self.assertRaises(ServiceResponseError): self.publish(initialise=True)
        self.assertEqual(self.publish()['status'],'already-published')
        self.assertEqual(len([c for c in self.transport.calls if c[0]=='PUT']),1)


if __name__ == '__main__': unittest.main()
