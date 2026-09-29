import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
spec=importlib.util.spec_from_file_location('launch',Path(__file__).parents[1]/'runtime/connectors/databricks_launch.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class LauncherTests(unittest.TestCase):
    def fixture(self, directory):
        p=Path(directory)
        project={'timeoutSeconds':10,'execution':{'mode':'databricks','bundleDirectory':directory,'pythonExecutable':'/opt/faker/bin/python','secrets':{'INGESTRON_STORAGE_SECRET':{'scope':'test','key':'secret'}}}}
        project['specificationSha256']=hashlib.sha256(json.dumps(project,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        (p/'connector.json').write_text(json.dumps({'projectLock':project}))
        (p/'runner.py').write_text('fixture')
        (p/'runtime.lock.json').write_text(json.dumps({'files':{'runner.py':hashlib.sha256(b'fixture').hexdigest()}}))
        dbutils=Mock();dbutils.secrets.get.return_value='synthetic-secret'
        return dbutils
    def test_secret_only_in_child_env_and_stable_run_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            utils=self.fixture(directory); process=Mock(pid=123);process.wait.return_value=0
            with patch.object(module.subprocess,'Popen',return_value=process) as popen, patch.object(module.os,'killpg'), patch.dict(module.os.environ,{'UNRELATED_SECRET':'excluded'}):
                result=module.launch(utils,directory,'/opt/faker/bin/python','business-42')
            args,kwargs=popen.call_args
            self.assertNotIn('synthetic-secret',str(args));self.assertEqual(kwargs['env']['INGESTRON_STORAGE_SECRET'],'synthetic-secret');self.assertNotIn('UNRELATED_SECRET',kwargs['env']);self.assertEqual(result,{'status':'Succeeded','runId':'business-42'})
    def test_tamper_rejected_before_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            utils=self.fixture(directory);(Path(directory)/'runner.py').write_text('tampered')
            with self.assertRaises(RuntimeError): module.launch(utils,directory,'/opt/faker/bin/python','business-42')
            utils.secrets.get.assert_not_called()
    def test_timeout_kills_process_group_and_suppresses_details(self):
        with tempfile.TemporaryDirectory() as directory:
            utils=self.fixture(directory); process=Mock(pid=123);process.wait.side_effect=[subprocess.TimeoutExpired('sensitive detail',1),0]
            with patch.object(module.subprocess,'Popen',return_value=process), patch.object(module.os,'killpg') as kill:
                with self.assertRaisesRegex(RuntimeError,'Diagnostics withheld') as raised: module.launch(utils,directory,'/opt/faker/bin/python','business-42')
                self.assertNotIn('sensitive detail',str(raised.exception));kill.assert_called_once()
