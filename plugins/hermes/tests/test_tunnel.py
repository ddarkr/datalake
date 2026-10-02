import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).parents[1] / 'deployment' / 'vault_tunnel.py'

class TunnelTests(unittest.TestCase):
    def test_loopback_strict_password_only_command_without_secret(self):
        self.assertTrue(MODULE.exists(), 'durable vault-backed tunnel implementation missing')
        spec = importlib.util.spec_from_file_location('vault_tunnel', MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        command = module.ssh_command('root', 'server.example.com', 24318)
        self.assertIn('127.0.0.1:24318:192.168.99.10:14318', command)
        for option in ('StrictHostKeyChecking=yes', 'ExitOnForwardFailure=yes', 'PubkeyAuthentication=no', 'PreferredAuthentications=password', 'ServerAliveInterval=30', 'ServerAliveCountMax=3', 'ControlMaster=no', 'ControlPath=none'):
            self.assertIn(option, command)
        self.assertEqual(command[-1], 'root@server.example.com')
        self.assertIn('-N', command)
        self.assertNotIn('-f', command)

    def test_private_session_read_and_sanitized_askpass(self):
        import tempfile, os
        spec = importlib.util.spec_from_file_location('vault_tunnel', MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertTrue(hasattr(module, 'read_login'), 'just-in-time vault retrieval missing')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'session'
            path.write_text('fixture-session')
            path.chmod(0o600)
            calls = []
            def runner(command, **kwargs):
                import subprocess
                calls.append((command, kwargs))
                return subprocess.CompletedProcess(command, 0, '{"login":{"username":"root","password":"fixture-password"}}', '')
            login = module.read_login(path, '/fake/bw', 'fixture-item', runner)
            self.assertEqual(login, {'username': 'root', 'password': 'fixture-password'})
            self.assertNotIn('fixture-session', calls[0][0])
            self.assertEqual(calls[0][1]['env']['BW_SESSION'], 'fixture-session')
            self.assertNotIn('BW_SESSION', os.environ)
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                module.read_login(path, '/fake/bw', 'fixture-item', runner)
            path.chmod(0o600)
            symlink = Path(directory) / 'link'
            symlink.symlink_to(path)
            with self.assertRaises((ValueError, OSError)):
                module.read_login(symlink, '/fake/bw', 'fixture-item', runner)

    def test_service_environment_contains_no_vault_or_password_secret(self):
        spec = importlib.util.spec_from_file_location('vault_tunnel', MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertTrue(hasattr(module, 'tunnel_environment'), 'clean durable transport environment missing')
        env = module.tunnel_environment('/private/askpass', {'BW_SESSION':'secret', 'SSH_PASSWORD':'secret', 'OTHER':'ok'})
        self.assertNotIn('BW_SESSION', env)
        self.assertNotIn('SSH_PASSWORD', env)
        self.assertEqual(env['SSH_ASKPASS'], '/private/askpass')
        self.assertEqual(env['SSH_ASKPASS_REQUIRE'], 'force')
        self.assertEqual(env['OTHER'], 'ok')

    def test_main_askpass_only_prints_password_to_ssh_and_exec_uses_clean_env(self):
        import io
        from unittest.mock import patch
        spec = importlib.util.spec_from_file_location('vault_tunnel', MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertTrue(hasattr(module, 'main'), 'launchd/askpass entrypoint missing')
        config = dict(session='/fixture/session', bw='/fake/bw', item='fixture-id', host='server.example.com', port=24318, askpass='/private/askpass')
        with patch.object(module, 'read_login', return_value=dict(username='root', password='fixture-password')):
            stream = io.StringIO()
            self.assertEqual(module.main(config, askpass=True, output=stream), 0)
            self.assertEqual(stream.getvalue(), 'fixture-password\n')
            calls = []
            module.main(config, exec_fn=lambda *args: calls.append(args))
            self.assertEqual(calls[0][0], '/usr/bin/ssh')
            self.assertNotIn('fixture-password', repr(calls))

if __name__ == '__main__':
    unittest.main()
