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


class DirectTransportTests(unittest.TestCase):
    def test_direct_success_posts_exactly_one_body(self):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        self.assertTrue(callable(getattr(plugin, 'Exporter', None)), 'exporter missing')
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            exporter = plugin.Exporter(f'http://127.0.0.1:{server.server_port}/v1/traces', {}, timeout=2)
            body = plugin.project(payload(response='PRIVATE'), 'scope-1')
            self.assertTrue(exporter.send(body))
            self.assertEqual(len(received), 1)
            self.assertNotIn(b'PRIVATE', received[0])
            self.assertEqual(json.loads(received[0]), body)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_http_failure_partial_and_malformed_are_dropped_without_retry(self):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        cases = [
            ('http-503', 503, b'{}'),
            ('partial-rejection', 200, b'{"partialSuccess":{"rejectedSpans":"1","errorMessage":"PRIVATE"}}'),
            ('fractional-rejection', 200, b'{"partialSuccess":{"rejectedSpans":0.5}}'),
            ('oversized', 200, b' ' * 65537),
        ]
        for name, status, reply in cases:
            with self.subTest(case=name):
                received = []
                class Handler(BaseHTTPRequestHandler):
                    def log_message(self, *args):
                        pass
                    def do_POST(self):
                        received.append(self.rfile.read(int(self.headers['Content-Length'])))
                        self.send_response(status)
                        self.end_headers()
                        try:
                            self.wfile.write(reply)
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    exporter = plugin.Exporter(f'http://127.0.0.1:{server.server_port}/v1/traces', {}, timeout=2)
                    self.assertFalse(exporter.send(plugin.project(payload(), 'scope-1')))
                    self.assertEqual(len(received), 1)
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join()

    def test_trickling_response_cannot_extend_export_deadline(self):
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
            exporter = plugin.Exporter(f'http://127.0.0.1:{server.server_port}/v1/traces', {}, timeout=0.1)
            start = time.monotonic()
            self.assertFalse(exporter.send(plugin.project(payload(), 'scope-1')))
            self.assertLess(time.monotonic() - start, 0.5)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_hook_duplicate_is_suppressed_in_memory_without_resend(self):
        import tempfile
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                received.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                home = Path(directory)
                runtime = plugin.Runtime(home, dict(endpoint=f'http://127.0.0.1:{server.server_port}/v1/traces', headers={}, timeout=2))
                runtime.observe(payload())
                with self.assertLogs('datalake.hermes', level='WARNING') as logs:
                    runtime.observe(payload())
                self.assertNotIn('PRIVATE', str(logs.output))
                self.assertEqual(len(received), 1)
                self.assertEqual(list(home.rglob('*.sqlite3')), [])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_failed_hook_send_is_dropped_without_retry(self):
        import tempfile
        runtime = plugin.Runtime(Path(tempfile.mkdtemp()), dict(endpoint='http://127.0.0.1:1/v1/traces', headers={}, timeout=0.1))
        with self.assertLogs('datalake.hermes', level='WARNING') as logs:
            runtime.observe(payload())
        self.assertNotIn('PRIVATE', str(logs.output))

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

    def test_legacy_capacity_and_pause_keys_are_ignored(self):
        import tempfile
        class Context:
            def __init__(self, extra):
                self.values = dict(endpoint='http://127.0.0.1:4318', **extra)
            def get_config(self, key, default=None):
                return self.values.get(key, default)
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            config = plugin.settings(Context({'capacity': 100, 'export_paused': True}), home, environ={})
            self.assertNotIn('capacity', config)
            self.assertNotIn('export_paused', config)
            self.assertEqual(config['endpoint'], 'http://127.0.0.1:4318/v1/traces')

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
