"""Internal S3 service proof for tools/demo.py; not a second benchmark CLI."""
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
import threading
import time


def configure(state, env):
    state.update(s3_access='demo' + secrets.token_hex(8), s3_secret=secrets.token_hex(24))
    env.update(GREPTIME_STORAGE_TYPE='S3', S3_ENDPOINT_URL='https://s3meter:9000',
               S3_BUCKET='demo-data', S3_REGION='us-east-1', S3_ROOT='greptime',
               S3_ACCESS_KEY_ID=state['s3_access'], S3_SECRET_ACCESS_KEY=state['s3_secret'],
               S3_BACKUP_PREFIX='backups', STORAGE_METRICS_INTERVAL_SEC='0',
               RAW_INVENTORY_INTERVAL_SEC='0')


def extend_override(state, env, override):
    from tools import demo as d
    from tools import benchmark_greptime_storage as b
    directory = Path(state['directory'])
    b.fetch_minio_binary(str(directory))
    image = env.get('PYTHON_IMAGE', b.PYTHON_IMAGE_DEFAULT)
    services = {}
    services['minio'] = dict(image=image, profiles=['proof-storage'],
        entrypoint=['/minio-bin'], command=['server', '/data', '--address', ':9000'],
        environment={'MINIO_ROOT_USER': state['s3_access'], 'MINIO_ROOT_PASSWORD': state['s3_secret']},
        ports=['127.0.0.1::9000'], volumes=['minio-data:/data', str(directory / 'minio-bin') + ':/minio-bin:ro'])
    for name in ('s3meter', 'rawmeter'):
        services[name] = dict(image=image, profiles=['proof-storage'],
            command=['python', '/meter.py', '--target', 'http://minio:9000', '--bind', '0.0.0.0',
                     '--port', '9000', '--status-bind', '0.0.0.0', '--status-port', '8080', '--allow-non-loopback'],
            ports=['127.0.0.1::8080'], volumes=[str(d.ROOT / 'tools/s3_meter.py') + ':/meter.py:ro'])
    # Same explicitly recorded, local-only endpoint substitution as the core harness.
    init = ('from scripts.database import greptime_preflight as p; p.main(); '
            'from pathlib import Path; f=Path("/out/greptimedb.toml"); '
            'f.write_text(f.read_text().replace("https://s3meter:9000", "http://s3meter:9000"))')
    services['db-init'] = {'command': ['python', '-c', init]}
    # The HTTP exception is confined to this owned synthetic project, not production source.
    backup = ('import sys; from scripts.storage import backup as b; '
              'b.require_https_endpoint=lambda value,name: value if value == "http://s3meter:9000" '
              'else (_ for _ in ()).throw(ValueError("not owned endpoint")); '
              'raise SystemExit(b.main(["backup",sys.argv[1]]))')
    services['backup'] = {'entrypoint': ['/opt/venv/bin/python', '-c', backup],
                          'environment': {'S3_ENDPOINT_URL': 'http://s3meter:9000'}}
    for line in override.splitlines():
        if line.startswith('  demo-wire: '):
            wire = json.loads(line.removeprefix('  demo-wire: '))
            wire['volumes'].append(str(d.ROOT) + ':/repo:ro')
            wire['environment'].update(S3_ACCESS_KEY_ID=state['s3_access'],
                                       S3_SECRET_ACCESS_KEY=state['s3_secret'])
            override = override.replace(line, '  demo-wire: ' + json.dumps(wire))
            break
    # Insert mappings into the existing YAML sections, retaining native !override tags.
    override = override.replace('volumes:\n  demo-fixtures:', ''.join('  ' + k + ': ' + json.dumps(v) + '\n' for k,v in services.items()) + 'volumes:\n  minio-data: {}\n  demo-fixtures:')
    d.private_write(directory / 'restore-s3.yaml', json.dumps({'networks': {'default':
        {'external': True, 'name': state['project'] + '_default'}}}))
    d.save_state(state)
    return override


def start_storage(state):
    from tools import demo as d
    from tools import benchmark_greptime_storage as b
    d.compose(state, 'up', '-d', 'minio', 's3meter', 'rawmeter')
    port = d.compose(state, 'port', 'minio', '9000', capture=True).strip()
    endpoint = 'http://' + port
    b.wait_minio(endpoint, state['s3_access'], state['s3_secret'])
    status, _ = b.s3_request(endpoint, 'PUT', '/demo-data', state['s3_access'], state['s3_secret'])
    assert status in (200, 204), status
    state['meter_urls'] = {name: 'http://' + d.compose(state, 'port', name, '8080', capture=True).strip()
                           for name in ('s3meter', 'rawmeter')}
    d.wait_for(lambda: meters(state), 'S3 response meters')
    state['startup_meter'] = meters(state)
    d.save_state(state)


def meters(state):
    from tools import demo as d
    return {name: d.request(url + '/snapshot') for name,url in state['meter_urls'].items()}


def cost(before, after):
    from tools.benchmark_greptime_egress import meter_delta, combine_costs
    parts = {name: meter_delta(before[name], after[name]) for name in before}
    operations = {name: {op: count - before[name].get('op_requests', {}).get(op, 0)
                         for op, count in after[name].get('op_requests', {}).items()} for name in before}
    return {'clients': parts, 'operations': operations, 'all_clients': combine_costs(list(parts.values()))}


def check(state):
    from tools import demo as d
    phases = {}
    previous = state['startup_meter']
    def phase(name, action):
        nonlocal previous
        before = meters(state)
        phases[name + '_preceding_gap'] = cost(previous, before)
        result = action()
        after = meters(state)
        phases[name] = cost(before, after)
        previous = after
        return result
    d.discover(state)
    running = d.compose(state, 'ps', '--services', '--status', 'running', capture=True).splitlines()
    assert 'aggregate' in running, running
    def idle():
        # Scheduled aggregation, exporter and Alloy scrapes run throughout; no dashboard queries here.
        for _ in range(4):
            d.compose(state, 'exec', '-T', 'storage-metrics', 'python', '-c',
                      'import urllib.request; r=urllib.request.urlopen("http://127.0.0.1:9105/metrics"); assert r.read()')
            time.sleep(15)
    phase('scheduled_aggregation_idle_and_metrics_60s', idle)
    def restart():
        d.compose(state, 'restart', 'greptimedb', 'storage-metrics')
        d.discover(state)
        # Health probe contains no table scan.
        d.wait_for(lambda: d.sql(state, 'SELECT 1'), 'restart health')
        time.sleep(15)
    phase('scheduled_aggregation_restart', restart)
    raw = phase('raw_upload_inventory_off_then_on', lambda: d.compose(state, 'exec', '-T', 'demo-wire',
        'sh', '-ec', 'pip install --disable-pip-version-check --no-cache-dir boto3==1.43.98 python-can==4.6.1 asammdf==8.8.27 zstd==1.5.6.1 >/dev/null && PYTHONPATH=/repo python /repo/tools/benchmark_background_services.py _raw', capture=True))
    def summaries():
        d.wait_for(lambda: d.summaries_ready(state, queued=state.get('queued_verified', False)),
                   'scheduled aggregation summaries')
    # Existing demo oracles include durable queue ACK, Fleet, privacy, dashboard and backup/restore.
    # Restore keeps the owned S3 service online; only the database/collectors are frozen.
    state['_service_oracles'] = True
    try:
        phase('scheduled_aggregation_summaries', summaries)
        phase('scheduled_service_oracles_and_backup_restore', lambda: d.check(state))
    finally:
        state.pop('_service_oracles', None)
        d.save_state(state)
    running = d.compose(state, 'ps', '--services', '--status', 'running', capture=True).splitlines()
    assert 'aggregate' in running, running
    final = meters(state)
    phases['final_gap'] = cost(previous, final)
    result = {'phases': phases, 'lifetime': cost(state['startup_meter'], final),
              'raw_oracles': raw, 'service_oracles': state['last_check'],
              'grafana_url': state['grafana_url'],
              'scope': 'All response bodies including unknowns; scheduled aggregation, dashboard and backup/restore costs are included, not zero claims.'}
    state['s3_proof'] = result
    d.private_write(Path(state['directory']) / 'proof.json', json.dumps(result, indent=2) + '\n')
    d.save_state(state)
    print(json.dumps(result, indent=2))
    return state
def stop(state):
    from tools import demo as d
    before = meters(state)
    services = d.compose(state, 'ps', '--services', '--status', 'running', capture=True).splitlines()
    clients = [service for service in services if service not in ('minio', 's3meter', 'rawmeter')]
    if clients:
        d.compose(state, 'stop', *clients)
    after = meters(state)
    result = {'shutdown': cost(before, after),
              'lifetime_through_client_shutdown': cost(state['startup_meter'], after)}
    state.setdefault('s3_proof', {}).update(result)
    d.private_write(Path(state['directory']) / 'proof.json', json.dumps(state['s3_proof'], indent=2) + '\n')
    d.save_state(state)
    print(json.dumps(result))




def raw_proof():
    import boto3
    from botocore.config import Config
    sys.path.insert(0, '/repo/tests')
    import test_raw
    from scripts.vehicle.raw import raw_upload as raw
    import urllib.request
    def snap():
        with urllib.request.urlopen('http://rawmeter:8080/snapshot') as r:
            return json.load(r)
    client = boto3.client('s3', endpoint_url='http://rawmeter:9000', region_name='us-east-1',
        aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
        config=Config(s3={'addressing_style':'path'}, request_checksum_calculation='when_required',
                      response_checksum_validation='when_required'))
    before = snap()
    with tempfile.TemporaryDirectory() as spool:
        paths = test_raw.UploadTest()._triple(spool)
        assert raw.run_once(client, 'demo-data', spool, 'm3', 0) == 1
        assert all(not os.path.exists(path) for path in paths), 'checksum ACK must precede local GC'
    raw.inventory_loop(client, 'demo-data', 0, [])
    disabled = snap()
    assert disabled['op_requests'].get('LIST', 0) == before['op_requests'].get('LIST', 0), 'disabled uploader inventory issued LIST'
    stop = []
    worker = threading.Thread(target=raw.inventory_loop, args=(client, 'demo-data', 1, stop))
    worker.start()
    try:
        deadline = time.monotonic() + 15
        while snap()['op_requests'].get('LIST', 0) == disabled['op_requests'].get('LIST', 0):
            assert time.monotonic() < deadline, 'enabled inventory never listed'
            time.sleep(.1)
    finally:
        stop.append(True)
        worker.join(10)
    from tools.benchmark_greptime_egress import meter_delta
    enabled = snap()
    print(json.dumps({'checksum_ack_gc': True, 'inventory_off_list_requests': 0,
                      'inventory_on_list_requests': enabled['op_requests'].get('LIST', 0) - disabled['op_requests'].get('LIST', 0),
                      'inventory_off': meter_delta(before, disabled),
                      'inventory_on': meter_delta(disabled, enabled)}))


if __name__ == '__main__':
    if sys.argv[1:] != ['_raw']:
        raise SystemExit('internal helper: use tools/demo.py start/check/stop --storage S3')
    raw_proof()
