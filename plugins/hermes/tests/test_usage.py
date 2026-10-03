import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('hermes_usage', ROOT / '__init__.py', submodule_search_locations=[str(ROOT)])
plugin = importlib.util.module_from_spec(spec) if spec else None
if (ROOT / '__init__.py').exists():
    sys.modules[spec.name] = plugin
    spec.loader.exec_module(plugin)


def payload(**extra):
    data = dict(session_id='session-1', api_request_id='request-1', model='gpt-test', provider='openai',
                started_at=1700000000.125, ended_at=1700000001.25,
                usage=dict(input_tokens=10, output_tokens=5, cache_read_tokens=3,
                           cache_write_tokens=2, reasoning_tokens=4, prompt_tokens=15, total_tokens=20))
    data.update(extra)
    return data


def attributes(body):
    return {item['key']: next(iter(item['value'].values())) for item in body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]['attributes']}


class ProjectionTests(unittest.TestCase):
    def test_metadata_only_canonical_usage(self):
        self.assertTrue(callable(getattr(plugin, 'project', None)), 'metadata projector missing')
        body = plugin.project(payload(response={'content': 'PRIVATE'}, assistant_message='PRIVATE',
                                      base_url='https://PRIVATE', headers={'authorization': 'PRIVATE'}), 'scope-1')
        a = attributes(body)
        self.assertEqual(a['coding_agent.client'], 'hermes')
        self.assertEqual(a['gen_ai.usage.input_tokens'], '15')
        self.assertEqual(a['gen_ai.usage.output_tokens'], '5')
        self.assertEqual(a['gen_ai.usage.reasoning.output_tokens'], '4')
        self.assertNotIn('cost_usd', a)
        self.assertNotIn('PRIVATE', json.dumps(body))
        self.assertEqual(body, plugin.project(payload(), 'scope-1'))

    def test_untrusted_metadata_rejected_and_unknown_not_zero(self):
        for changes in [dict(model='https://secret'), dict(provider='api key'),
                        dict(session_id='bad\nidentity'), dict(started_at=float('nan')),
                        dict(started_at=10 ** 1000), dict(ended_at=10 ** 1000),
                        dict(ended_at=1), dict(usage={'input_tokens': True}),
                        dict(usage={'prompt_tokens': -1}), dict(usage={'output_tokens': 1.5})]:
            with self.subTest(changes=changes):
                self.assertIsNone(plugin.project(payload(**changes), 'scope-1'))
        a = attributes(plugin.project(payload(usage={'prompt_tokens': 0, 'output_tokens': 0}), 'scope-1'))
        self.assertEqual(a['gen_ai.usage.input_tokens'], '0')
        self.assertNotIn('gen_ai.usage.reasoning.output_tokens', a)
        self.assertIsNone(plugin.project(payload(usage=None), 'scope-1'))
        self.assertIsNone(plugin.project(payload(streaming=True), 'scope-1', auxiliary=True))
        self.assertIsNone(plugin.project(payload(error='PRIVATE'), 'scope-1', auxiliary=True))
        self.assertIsNotNone(plugin.project(payload(), 'scope-1', auxiliary=True))

    def test_canonical_ids_match_javascript_and_retry_attempts_differ(self):
        import subprocess
        body = plugin.project(payload(), 'scope-1')
        span = body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        session = attributes(body)['coding_agent.session.id']
        event = json.dumps(['post_api_request', 'request-1', 0], separators=(',', ':'))
        script = "import {serializeEvent} from './plugins/otel.mjs'; console.log(serializeEvent('hermes'," + json.dumps(dict(kind='llm.turn', sessionId=session, eventId=event, startTimeMs=1700000000125, endTimeMs=1700000001250)) + ").body);"
        oracle = json.loads(subprocess.check_output(['node', '--input-type=module', '-e', script], cwd=ROOT.parents[1], text=True))
        other = oracle['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        for key in ('traceId', 'spanId', 'startTimeUnixNano', 'endTimeUnixNano', 'name', 'kind', 'status'):
            self.assertEqual(span[key], other[key])
        fractional = plugin.project(payload(started_at=1700000000.123456, ended_at=1700000001.234567), 'scope-1')
        fractional_script = script.replace('1700000000125', str(1700000000.123456 * 1000)).replace('1700000001250', str(1700000001.234567 * 1000))
        fractional_oracle = json.loads(subprocess.check_output(['node', '--input-type=module', '-e', fractional_script], cwd=ROOT.parents[1], text=True))
        expected = fractional_oracle['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        actual = fractional['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        self.assertEqual(actual['startTimeUnixNano'], expected['startTimeUnixNano'])
        self.assertEqual(actual['endTimeUnixNano'], expected['endTimeUnixNano'])
        retried = plugin.project(payload(retry_count=1), 'scope-1')
        self.assertNotEqual(span['spanId'], retried['resourceSpans'][0]['scopeSpans'][0]['spans'][0]['spanId'])
        self.assertNotEqual(span['spanId'], plugin.project(payload(), 'scope-1', auxiliary=True)['resourceSpans'][0]['scopeSpans'][0]['spans'][0]['spanId'])

    def test_rejects_inconsistent_usage_and_poison_objects(self):
        class Poison:
            def __str__(self):
                raise AssertionError('content must never be read')
            def __getattr__(self, name):
                raise AssertionError('response must never be read')
        body = plugin.project(payload(response=Poison(), request_messages=Poison(), usage=dict(payload()['usage'], raw_usage=Poison())), 'scope-1')
        self.assertIsNotNone(body)
        for usage in [dict(payload()['usage'], prompt_tokens=100),
                      dict(payload()['usage'], total_tokens=100),
                      {'input_tokens': 1}, {'total_tokens': 42}]:
            with self.subTest(usage=usage):
                self.assertIsNone(plugin.project(payload(usage=usage), 'scope-1'))

    def test_no_raw_identity_and_optional_response_model_timing(self):
        body = plugin.project(payload(response_model='gpt-response', first_chunk_at=1700000000.625), 'scope-1')
        a = attributes(body)
        self.assertEqual(a.get('gen_ai.response.model'), 'gpt-response')
        self.assertEqual(a.get('ttft_ms'), '500')
        self.assertEqual(a.get('duration_ms'), '1125')
        self.assertEqual(len(a.get('request_id', '')), 64)
        self.assertNotIn('request-1', json.dumps(body))
        self.assertNotIn('session-1', json.dumps(body))
        self.assertIsNone(plugin.project(payload(response_model='https://PRIVATE'), 'scope-1'))


class OutboxTests(unittest.TestCase):
    def test_durable_immutable_dedup_and_exclusive_claim(self):
        import tempfile
        self.assertTrue(callable(getattr(plugin, 'Outbox', None)), 'durable outbox missing')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'outbox.sqlite3'
            first = plugin.Outbox(path, capacity=2)
            body = plugin.project(payload(), 'scope-1')
            self.assertTrue(first.put(body))
            body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]['endTimeUnixNano'] = '999'
            self.assertFalse(first.put(body))
            second = plugin.Outbox(path, capacity=2)
            claim = second.claim(now=10)
            self.assertIsNotNone(claim)
            self.assertNotIn('"endTimeUnixNano":"999"', claim[2])
            self.assertIsNone(first.claim(now=10))
            self.assertIsNone(first.claim(now=39))
            reclaimed = first.claim(now=41)
            self.assertEqual(claim[2], reclaimed[2])
            self.assertFalse(second.finish(claim, success=True, now=41))
            self.assertTrue(first.finish(reclaimed, success=True, now=41))
            self.assertFalse(first.put(body))
            self.assertIsNone(second.claim(now=100))
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertTrue(first.put(plugin.project(payload(api_request_id='request-2'), 'scope-1')))
            self.assertFalse(first.put(plugin.project(payload(api_request_id='request-3'), 'scope-1')))

    def test_private_outbox_rejects_symlink_directory_and_shared_files(self):
        import os
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'target'
            target.mkdir(mode=0o700)
            linked = root / 'linked'
            linked.symlink_to(target, target_is_directory=True)
            with self.assertRaises(ValueError):
                plugin.Outbox(linked / 'events.db')
            unsafe = root / 'unsafe'
            unsafe.mkdir(mode=0o755)
            with self.assertRaises(ValueError):
                plugin.Outbox(unsafe / 'events.db')
            original = target / 'original.db'
            original.touch(mode=0o600)
            hardlink = target / 'linked.db'
            os.link(original, hardlink)
            with self.assertRaises(ValueError):
                plugin.Outbox(hardlink)

    def test_independent_processes_share_one_immutable_event(self):
        import subprocess
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events.db'
            plugin.Outbox(path)
            script = "import importlib.util,json,sys; s=importlib.util.spec_from_file_location('candidate',sys.argv[1]); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); o=m.Outbox(sys.argv[2]); inserted=o.put(m.project(json.loads(sys.argv[3]),'scope-1')); c=o.claim(now=10); print(json.dumps([inserted, c]))"
            processes = [subprocess.Popen([sys.executable, '-c', script, str(ROOT / '__init__.py'), str(path), json.dumps(payload())], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(4)]
            results = []
            for process in processes:
                stdout, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 0, stderr)
                results.append(json.loads(stdout))
            self.assertEqual(sum(r[0] for r in results), 1)
            claims = [r[1] for r in results if r[1] is not None]
            self.assertEqual(len(claims), 1)
            replay = plugin.Outbox(path).claim(now=41)
            self.assertEqual(replay[2], claims[0][2])


class TransportTests(unittest.TestCase):
    def test_paused_export_preserves_pending_events_without_transport(self):
        import tempfile
        import time
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(plugin.Exporter, 'send_once', return_value=False) as send:
                runtime = plugin.Runtime(Path(directory), dict(capacity=100, endpoint='http://127.0.0.1:1/v1/traces', headers={}, timeout=0.1, export_paused=True))
                try:
                    runtime.observe(payload())
                    time.sleep(0.3)
                    self.assertIsNotNone(runtime.outbox.claim(now=0))
                finally:
                    runtime.close()
                send.assert_not_called()

    def test_local_http_retry_partial_success_replays_identical_bytes(self):
        import tempfile
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        self.assertTrue(callable(getattr(plugin, 'Exporter', None)), 'exporter missing')
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(503 if len(received) == 1 else 200)
                self.end_headers()
                self.wfile.write(b'{"partialSuccess":{"rejectedSpans":"1","errorMessage":"PRIVATE"}}' if len(received) == 2 else b'{}')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                outbox = plugin.Outbox(Path(directory) / 'outbox.sqlite3')
                outbox.put(plugin.project(payload(response='PRIVATE'), 'scope-1'))
                exporter = plugin.Exporter(outbox, f'http://127.0.0.1:{server.server_port}/v1/traces', {}, timeout=0.2)
                with self.assertLogs('datalake.hermes', level='WARNING') as logs:
                    self.assertFalse(exporter.send_once(now=10))
                    self.assertFalse(exporter.send_once(now=11))
                self.assertNotIn('PRIVATE', str(logs.output))
                self.assertTrue(exporter.send_once(now=13))
                self.assertFalse(exporter.send_once(now=100))
                self.assertEqual(len(received), 3)
                self.assertEqual(received[0], received[1])
                self.assertEqual(received[1], received[2])
                self.assertNotIn(b'PRIVATE', received[0])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_trickling_response_cannot_extend_export_deadline(self):
        import tempfile
        import threading
        import time
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200)
                self.end_headers()
                try:
                    for _ in range(20):
                        self.wfile.write(b' ')
                        self.wfile.flush()
                        time.sleep(0.04)
                    self.wfile.write(b'{}')
                except (BrokenPipeError, ConnectionResetError):
                    pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                outbox = plugin.Outbox(Path(directory) / 'events.db')
                outbox.put(plugin.project(payload(), 'scope-1'))
                exporter = plugin.Exporter(outbox, f'http://127.0.0.1:{server.server_port}/v1/traces', {}, timeout=0.1)
                start = time.monotonic()
                with self.assertLogs('datalake.hermes', level='WARNING'):
                    self.assertFalse(exporter.send_once(now=10))
                self.assertLess(time.monotonic() - start, 0.5)
                self.assertIsNotNone(outbox.claim(now=11))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_fractional_partial_rejection_is_not_acknowledged(self):
        import tempfile
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"partialSuccess":{"rejectedSpans":0.5}}')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                outbox = plugin.Outbox(Path(directory) / 'events.db')
                outbox.put(plugin.project(payload(), 'scope-1'))
                exporter = plugin.Exporter(outbox, f'http://127.0.0.1:{server.server_port}/v1/traces', {}, timeout=0.1)
                with self.assertLogs('datalake.hermes', level='WARNING'):
                    self.assertFalse(exporter.send_once(now=10))
                self.assertIsNotNone(outbox.claim(now=11))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


class ConfigTests(unittest.TestCase):
    def test_auxiliary_capture_requires_explicit_boolean_opt_in(self):
        import tempfile
        class Context:
            def __init__(self, value):
                self.value = value
            def get_config(self, key, default=None):
                return {'endpoint': 'http://127.0.0.1:4318', 'capture_auxiliary': self.value}.get(key, default)
        with tempfile.TemporaryDirectory() as directory:
            for value in ('false', 'true', 0, 1, None, [], {}):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    plugin.settings(Context(value), Path(directory), environ={})
            self.assertFalse(plugin.settings(Context(False), Path(directory), environ={})['capture_auxiliary'])
            self.assertTrue(plugin.settings(Context(True), Path(directory), environ={})['capture_auxiliary'])

    def test_export_pause_is_boolean_and_defaults_to_active(self):
        import tempfile
        class Context:
            def __init__(self, extra):
                self.values = dict(endpoint='http://127.0.0.1:4318', **extra)
            def get_config(self, key, default=None):
                return self.values.get(key, default)
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            self.assertIs(plugin.settings(Context({}), home, environ={}).get('export_paused'), False)
            self.assertTrue(plugin.settings(Context({'export_paused': True}), home, environ={})['export_paused'])
            for value in ('false', 0, None):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    plugin.settings(Context({'export_paused': value}), home, environ={})

    def test_profile_settings_secret_file_permissions_and_endpoint_validation(self):
        import os
        import tempfile
        self.assertTrue(callable(getattr(plugin, 'settings', None)), 'profile configuration missing')
        class Context:
            def __init__(self, values):
                self.values = values
            def get_config(self, key, default=None):
                return self.values.get(key, default)
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            ctx = Context({'endpoint': 'http://127.0.0.1:4318'})
            creds = home / 'usage-otel.credentials.json'
            creds.write_text('{"Authorization":"Bearer PRIVATE"}')
            os.chmod(creds, 0o600)
            config = plugin.settings(ctx, home, environ={})
            self.assertEqual(config['endpoint'], 'http://127.0.0.1:4318/v1/traces')
            self.assertEqual(config['headers'], {'Authorization': 'Bearer PRIVATE'})
            os.chmod(creds, 0o644)
            with self.assertRaises(ValueError):
                plugin.settings(ctx, home, environ={})
            creds.unlink()
            for endpoint in ['http://example.org', 'https://user:secret@example.org',
                             'https://example.org?secret=yes', 'file:///tmp/secret', 'https://example.org/#secret']:
                with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                    plugin.settings(Context({'endpoint': endpoint}), home, environ={})
            for headers in ['{"Cookie":"PRIVATE"}', json.dumps({'Authorization': 'x\nsecret'}), '[]']:
                with self.subTest(headers=headers), self.assertRaises(ValueError):
                    plugin.settings(ctx, home, environ={'DATALAKE_HERMES_OTLP_HEADERS': headers})
            with self.assertRaises(ValueError):
                plugin.settings(Context({'endpoint': 'https://example.org', 'headers': {'Authorization': 'PRIVATE'}}), home, environ={})
            self.assertIsNone(plugin.settings(Context({}), home, environ={}))


if __name__ == '__main__':
    unittest.main()
