import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).parents[1] / 'deployment' / 'vault_tunnel.py'

SSH_USER = 'fixture-user'
SSH_HOST = 'ssh.example.com'
LOCAL_PORT = 24418
REMOTE_HOST = '192.168.99.10'
REMOTE_PORT = 4318


def load():
    spec = importlib.util.spec_from_file_location('vault_tunnel', MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TunnelTests(unittest.TestCase):
    def test_loopback_strict_password_only_command_without_secret(self):
        module = load()
        command = module.ssh_command(SSH_USER, SSH_HOST, LOCAL_PORT, REMOTE_HOST, REMOTE_PORT)
        self.assertIn(f'127.0.0.1:{LOCAL_PORT}:{REMOTE_HOST}:{REMOTE_PORT}', command)
        for option in ('StrictHostKeyChecking=yes', 'ExitOnForwardFailure=yes', 'PubkeyAuthentication=no', 'PreferredAuthentications=password', 'ServerAliveInterval=30', 'ServerAliveCountMax=3', 'ControlMaster=no', 'ControlPath=none'):
            self.assertIn(option, command)
        self.assertEqual(command[-1], f'{SSH_USER}@{SSH_HOST}')
        self.assertIn('-N', command)
        self.assertNotIn('-f', command)


    def test_untrusted_destination_or_identity_rejected(self):
        module = load()
        for bad in ('evil:22', 'a b', '-oProxyCommand=touch', 'a@b', 'a/b', '', 'x' * 254, '2001:db8::1'):
            with self.subTest(value=bad):
                with self.assertRaises(ValueError):
                    module.ssh_command(SSH_USER, bad, LOCAL_PORT, REMOTE_HOST, REMOTE_PORT)
                with self.assertRaises(ValueError):
                    module.ssh_command(SSH_USER, SSH_HOST, LOCAL_PORT, bad, REMOTE_PORT)
        for bad_user in ('a@b', '-ssh', 'a b', 'a:b', '', 'a/b'):
            with self.subTest(value=bad_user):
                with self.assertRaises(ValueError):
                    module.ssh_command(bad_user, SSH_HOST, LOCAL_PORT, REMOTE_HOST, REMOTE_PORT)
        for bad_port in (0, 65536, -1, 'x', '', None, True):
            with self.subTest(value=bad_port):
                with self.assertRaises(ValueError):
                    module.ssh_command(SSH_USER, SSH_HOST, bad_port, REMOTE_HOST, REMOTE_PORT)
                with self.assertRaises(ValueError):
                    module.ssh_command(SSH_USER, SSH_HOST, LOCAL_PORT, REMOTE_HOST, bad_port)

    def test_private_session_read_and_sanitized_askpass(self):
        import tempfile, os
        module = load()
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
        module = load()
        env = module.tunnel_environment('/private/askpass', {'BW_SESSION':'secret', 'SSH_PASSWORD':'secret', 'OTHER':'ok'})
        self.assertNotIn('BW_SESSION', env)
        self.assertNotIn('SSH_PASSWORD', env)
        self.assertEqual(env['SSH_ASKPASS'], '/private/askpass')
        self.assertEqual(env['SSH_ASKPASS_REQUIRE'], 'force')
        self.assertEqual(env['OTHER'], 'ok')

    def test_main_askpass_only_prints_password_to_ssh_and_exec_uses_clean_env(self):
        import io
        from unittest.mock import patch
        module = load()
        config = dict(session='/fixture/session', bw='/fake/bw', item='fixture-id', host=SSH_HOST, port=LOCAL_PORT, askpass='/private/askpass', remote_host=REMOTE_HOST, remote_port=REMOTE_PORT)
        with patch.object(module, 'read_login', return_value=dict(username='root', password='fixture-password')):
            stream = io.StringIO()
            self.assertEqual(module.main(config, askpass=True, output=stream), 0)
            self.assertEqual(stream.getvalue(), 'fixture-password\n')
            calls = []
            module.main(config, exec_fn=lambda *args: calls.append(args))
            self.assertEqual(calls[0][0], '/usr/bin/ssh')
            self.assertNotIn('fixture-password', repr(calls))

    def test_main_rejects_bad_network_config_before_vault_access(self):
        from unittest.mock import patch
        module = load()
        with self.assertRaises(ValueError):
            module.main({})
        base = dict(session='/fixture/session', bw='/fake/bw', item='fixture-id', host=SSH_HOST, port=LOCAL_PORT, askpass='/private/askpass', remote_host=REMOTE_HOST, remote_port=REMOTE_PORT)
        for missing in ('remote_host', 'remote_port', 'host', 'port'):
            with self.subTest(missing=missing):
                config = dict(base)
                del config[missing]
                with patch.object(module, 'read_login', side_effect=AssertionError('must not read vault')):
                    with self.assertRaises(ValueError):
                        module.main(config)
        for field, invalid in (('host', '.example'), ('host', 'host:22'),
                               ('remote_host', 'target:22'), ('port', True),
                               ('remote_port', 65536)):
            with self.subTest(field=field):
                config = dict(base)
                config[field] = invalid
                with patch.object(module, 'read_login', side_effect=AssertionError('must not read vault')):
                    with self.assertRaises(ValueError):
                        module.main(config)

if __name__ == '__main__':
    unittest.main()
