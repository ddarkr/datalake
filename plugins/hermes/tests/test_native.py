"""Opt-in compatibility test against installed Hermes; only scratch profiles/loopback."""
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SOURCE = os.environ.get('HERMES_SOURCE')
ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(SOURCE, 'set HERMES_SOURCE to a verified Hermes checkout')
class NativeLoadingTests(unittest.TestCase):
    def test_real_loader_hooks_worker_and_profile_binding(self):
        sys.path.insert(0, SOURCE)
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        from hermes_cli.plugins import PluginManager
        from hermes_cli.plugins_manifest import PluginManifest
        received = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass
            def do_POST(self):
                received.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        manager = None
        try:
            with tempfile.TemporaryDirectory() as directory:
                home = Path(directory)
                (home / 'config.yaml').write_text('plugins:\n  entries:\n    datalake-usage:\n      settings:\n        capture_auxiliary: true\n        endpoint: http://127.0.0.1:' + str(server.server_port) + '\n')
                token = set_hermes_home_override(home)
                try:
                    manager = PluginManager()
                    from hermes_cli.plugins_discovery import scan_directory
                    import shutil
                    candidate = home / 'candidate' / 'datalake-usage'
                    shutil.copytree(ROOT, candidate, ignore=shutil.ignore_patterns('tests', '__pycache__'))
                    matches = scan_directory(candidate.parent, 'user')
                    self.assertTrue(matches, 'native manifest missing')
                    manifest = matches[0]
                    self.assertEqual(manifest.name, 'datalake-usage')
                    from unittest.mock import patch
                    with patch.dict(os.environ, {'DATALAKE_HERMES_OTLP_HEADERS': '{}'}):
                        manager._load_plugin(manifest)
                finally:
                    reset_hermes_home_override(token)
                loaded = manager._plugins['datalake-usage']
                self.assertIsNone(loaded.error)
                self.assertTrue(manager._hooks.get('post_api_request'), 'native usage hooks not registered')
                data = dict(session_id='session-1', api_request_id='request-1', model='gpt-test', provider='openai',
                            started_at=1700000000.125, ended_at=1700000001.25,
                            usage=dict(input_tokens=10, output_tokens=5, prompt_tokens=10),
                            response={'assistant_message': 'PRIVATE'}, request_messages=['PRIVATE'])
                manager.invoke_hook('post_api_request', **data)
                with self.assertLogs('datalake.hermes', level='WARNING') as logs:
                    manager.invoke_hook('post_api_request', **data)
                    manager.invoke_hook('post_api_request', **dict(data, auxiliary=True))
                self.assertNotIn('PRIVATE', str(logs.output))
                manager.invoke_hook('post_auxiliary_call', **dict(data, streaming=True))
                # Explicit opt-in retains standalone MoA synthesis, not a task blacklist.
                manager.invoke_hook('post_auxiliary_call', **dict(data, api_request_id='aux-1', aux_task='moa_aggregator', streaming=False))
                import sqlite3
                with sqlite3.connect(home / 'usage-otel' / 'outbox.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 2)
                deadline = time.monotonic() + 3
                while len(received) < 2 and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertEqual(len(received), 2)
                self.assertNotIn(b'PRIVATE', b''.join(received))
                self.assertTrue((home / 'usage-otel' / 'outbox.sqlite3').exists())
                start = time.monotonic()
                manager.unload('datalake-usage')
                self.assertLess(time.monotonic() - start, 0.5)
                self.assertFalse(manager._hooks.get('post_api_request'))
        finally:
            if manager:
                manager.unload('datalake-usage')
            server.shutdown()
            server.server_close()
            thread.join()

    def test_default_moa_overlap_bills_only_main_completion(self):
        """Real aux emitter + native hooks + projector + downstream billable()."""
        sys.path.insert(0, SOURCE)
        sys.path.insert(0, str(ROOT.parents[1] / 'scripts'))
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        from hermes_cli.plugins import PluginManager
        from hermes_cli.plugins_discovery import scan_directory
        from agent.auxiliary_hooks import _AuxCallHooks
        from agent.api_request_hooks import ApiRequestHooksMixin
        from types import SimpleNamespace
        from unittest.mock import patch
        import aggregate
        import json
        import shutil
        import sqlite3
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / 'config.yaml').write_text('plugins:\n  entries:\n    datalake-usage:\n      settings:\n        export_paused: true\n        endpoint: http://127.0.0.1:1\n')
            candidate = home / 'candidate' / 'datalake-usage'
            shutil.copytree(ROOT, candidate, ignore=shutil.ignore_patterns('tests', '__pycache__'))
            token = set_hermes_home_override(home)
            manager = PluginManager()
            try:
                # Scratch outbox with export paused; no transport or production sends.
                with patch.dict(os.environ, {'DATALAKE_HERMES_OTLP_HEADERS': '{}'}):
                    manager._load_plugin(scan_directory(candidate.parent, 'user')[0])
                self.assertIsNone(manager._plugins['datalake-usage'].error)
                response = SimpleNamespace(model='gpt-test', choices=[], usage=dict(prompt_tokens=15, completion_tokens=5))
                parent = dict(session_id='s', task_id='t', turn_id='turn', platform='cli')
                with patch('agent.auxiliary_hooks._parent_turn_identity', return_value=parent), patch('agent.auxiliary_hooks._has_hook', return_value=True), patch('agent.auxiliary_hooks._fire', side_effect=manager.invoke_hook):
                    # Same aux_task is used by main facade and standalone synthesizer:
                    # installed auxiliary payload has no actor/call-site discriminator.
                    for request in ('facade-aggregator', 'standalone-synthesizer'):
                        hooks = _AuxCallHooks(aux_task='moa_aggregator', metadata=dict(api_request_id=request), client=SimpleNamespace(base_url=''), kwargs={}, provider='openai', model='gpt-test', api_mode='chat_completions', streaming=False)
                        hooks.post(response)
                main = dict(parent, api_request_id='main-request', model='gpt-test', provider='openai', started_at=1, ended_at=2, moa_references=[], usage=ApiRequestHooksMixin._usage_summary_for_api_request_hook(SimpleNamespace(provider='openai', api_mode='chat_completions'), response))
                manager.invoke_hook('post_api_request', **main)
                with sqlite3.connect(home / 'usage-otel' / 'outbox.sqlite3') as db:
                    bodies = [json.loads(r[0]) for r in db.execute('SELECT body FROM events')]
                spans = []
                for body in bodies:
                    span = body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
                    row = dict(trace_id=span['traceId'], span_id=span['spanId'], span_name=span['name'])
                    for attr in span['attributes']:
                        value = attr['value']
                        row['span_attributes.' + attr['key']] = int(value['intValue']) if 'intValue' in value else next(iter(value.values()))
                    spans.append(aggregate.norm_span(row))
                billed = aggregate.billable(spans)
                self.assertEqual((len(billed), sum(s['input'] for s in billed), sum(s['output'] for s in billed)), (1, 15, 5))
            finally:
                manager.unload('datalake-usage')
                reset_hermes_home_override(token)

    def test_installed_main_and_aux_normalizers_preserve_provider_buckets(self):
        sys.path.insert(0, str(SOURCE))
        from agent.api_request_hooks import ApiRequestHooksMixin
        from agent.auxiliary_hooks import _usage_summary
        from types import SimpleNamespace
        import importlib.util
        spec = importlib.util.spec_from_file_location('normalizer_candidate', ROOT / '__init__.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        fixtures = [
            ('openai', 'chat_completions', dict(prompt_tokens=15, completion_tokens=5, prompt_tokens_details=dict(cached_tokens=3, cache_write_tokens=2), completion_tokens_details=dict(reasoning_tokens=4))),
            ('openai-codex', 'codex_responses', dict(input_tokens=15, output_tokens=5, input_tokens_details=dict(cached_tokens=3, cache_write_tokens=2), output_tokens_details=dict(reasoning_tokens=4))),
            ('anthropic', 'anthropic_messages', dict(input_tokens=10, output_tokens=5, cache_read_input_tokens=3, cache_creation_input_tokens=2, output_tokens_details=dict(reasoning_tokens=4))),
        ]
        for provider, mode, raw in fixtures:
            with self.subTest(provider=provider):
                response = SimpleNamespace(usage=raw)
                agent = SimpleNamespace(provider=provider, api_mode=mode)
                main = ApiRequestHooksMixin._usage_summary_for_api_request_hook(agent, response)
                aux = _usage_summary(response, provider=provider, api_mode=mode)
                self.assertEqual(main, aux)
                self.assertEqual(main['input_tokens'], 10)
                self.assertEqual(main['prompt_tokens'], 15)
                self.assertEqual(main['total_tokens'], 20)
                body = module.project(dict(session_id='s', api_request_id='r', model='test', provider=provider, started_at=1, ended_at=2, usage=main), 'scope')
                a = {i['key']: next(iter(i['value'].values())) for i in body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]['attributes']}
                self.assertEqual(a['gen_ai.usage.input_tokens'], '15')
                self.assertEqual(a['gen_ai.usage.reasoning.output_tokens'], '4')
                self.assertEqual(a['gen_ai.usage.cache_write.input_tokens'], '2')


if __name__ == '__main__':
    unittest.main()
