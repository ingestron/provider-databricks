"""Launch frozen connector environment from a customer Databricks notebook."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import signal


def launch(dbutils, directory, python, run_id):
    # No upstream imports in the notebook interpreter; its packages are unrelated.
    try:
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,100}', run_id):
            raise ValueError('Invalid run identity')
        folder = Path(directory)
        config = json.loads((folder/'connector.json').read_text())
        project = dict(config['projectLock'])
        expected = project.pop('specificationSha256')
        canonical = json.dumps(project, sort_keys=True, separators=(',',':'), ensure_ascii=False)
        if hashlib.sha256(canonical.encode()).hexdigest() != expected:
            raise ValueError('Project identity mismatch')
        execution = project['execution']
        if execution['mode'] != 'databricks' or execution['bundleDirectory'] != directory or execution['pythonExecutable'] != python:
            raise ValueError('Execution identity mismatch')
        lock = json.loads((folder/'runtime.lock.json').read_text())
        for name, digest in lock['files'].items():
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', name) or hashlib.sha256((folder/name).read_bytes()).hexdigest() != digest:
                raise ValueError('Runtime asset mismatch')
        env = {k:os.environ[k] for k in ('PATH','LANG','SSL_CERT_FILE') if k in os.environ}
        env.update(PYTHONNOUSERSITE='1', PYTHONDONTWRITEBYTECODE='1')
        for name, ref in execution['secrets'].items():
            if not re.fullmatch(r'INGESTRON_[A-Z0-9_]+',name):
                raise ValueError('Use application-specific secret names')
            env[name] = dbutils.secrets.get(scope=ref['scope'], key=ref['key'])
        # Never forward upstream diagnostics or emit credentials into notebook results.
        process = subprocess.Popen([python, str(folder/'singer_azure.py'), '--config', str(folder/'connector.json'), '--run-id',run_id], cwd=folder, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            if process.wait(timeout=2*project['timeoutSeconds']+180):
                raise ValueError('Runtime failed')
        finally:
            try: os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            process.wait()
    except Exception:
        raise RuntimeError('Connector workload failed; inspect the logical commit before retrying. Diagnostics withheld.') from None
    return {'status':'Succeeded', 'runId':run_id}
