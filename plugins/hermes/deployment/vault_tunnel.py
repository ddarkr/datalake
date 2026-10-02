"""Foreground SSH transport for a user launchd job; no credential persistence."""

import json
import os
import re
import stat
import subprocess

_HOST_RE = re.compile(r'(?![-.])(?!.*\.\.)[A-Za-z0-9.-]{1,253}(?<![-.])')
_USER_RE = re.compile(r'[A-Za-z0-9_][A-Za-z0-9._-]{0,31}')


def _valid_host(value):
    return isinstance(value, str) and _HOST_RE.fullmatch(value) is not None


def _valid_port(value):
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        port = value
    elif isinstance(value, str) and value.isdigit():
        port = int(value, 10)
    else:
        return False
    return 1 <= port <= 65535


def read_login(session_path, bw, item, runner=subprocess.run):
    fd = os.open(session_path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid() or info.st_size > 65536:
            raise ValueError('unsafe established vault session file')
        session = stream.read().strip()
    env = os.environ.copy()
    env['BW_SESSION'] = session
    result = runner([bw, 'get', 'item', item], env=env, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise ValueError('vault retrieval failed')
    login = json.loads(result.stdout).get('login', {})
    if not all(type(login.get(k)) is str and login[k] for k in ('username', 'password')):
        raise ValueError('vault login unavailable')
    return {k: login[k] for k in ('username', 'password')}


def tunnel_environment(askpass, source=None):
    env = dict(os.environ if source is None else source)
    for key in ('BW_SESSION', 'SSH_PASSWORD'):
        env.pop(key, None)
    env.update(SSH_ASKPASS=str(askpass), SSH_ASKPASS_REQUIRE='force', DISPLAY=':0')
    return env


def ssh_command(username, host, port, remote_host, remote_port):
    """Build the foreground SSH argv. Every network field is validated so the
    `-L local:remote_host:remote_port` grammar cannot be injected: hostnames
    are letters/digits/dot/hyphen only (no `:`, `@`, `/`, whitespace, or
    leading `-`; raw IPv6 is rejected), usernames exclude `@:/` and leading
    `-`, and both ports are integers 1-65535. Local binding stays literal
    loopback; the list form (no shell) carries the validated values."""
    if not (isinstance(username, str) and _USER_RE.fullmatch(username) is not None):
        raise ValueError('invalid SSH username')
    if not _valid_host(host):
        raise ValueError('invalid SSH host')
    if not _valid_host(remote_host):
        raise ValueError('invalid tunnel remote host')
    if not _valid_port(port):
        raise ValueError('invalid tunnel local port')
    if not _valid_port(remote_port):
        raise ValueError('invalid tunnel remote port')
    return ['/usr/bin/ssh', '-N', '-T', '-L', f'127.0.0.1:{int(port)}:{remote_host}:{int(remote_port)}',
            '-o', 'StrictHostKeyChecking=yes', '-o', 'ExitOnForwardFailure=yes',
            '-o', 'PubkeyAuthentication=no', '-o', 'PreferredAuthentications=password',
            '-o', 'ServerAliveInterval=30', '-o', 'ServerAliveCountMax=3',
            '-o', 'ConnectTimeout=10', '-o', 'ControlMaster=no', '-o', 'ControlPath=none',
            username + '@' + host]


def main(config, askpass=False, output=None, exec_fn=os.execve):
    import sys
    for key in ('session', 'bw', 'item', 'host', 'askpass', 'remote_host'):
        if not isinstance(config.get(key), str) or not config[key]:
            raise ValueError(f'tunnel configuration missing {key}')
    for key in ('port', 'remote_port'):
        if not _valid_port(config.get(key)):
            raise ValueError(f'tunnel configuration invalid {key}')
    if not _valid_host(config['host']) or not _valid_host(config['remote_host']):
        raise ValueError('tunnel configuration invalid host')
    login = read_login(config['session'], config['bw'], config['item'])
    if askpass:
        print(login['password'], file=sys.stdout if output is None else output)
        return 0
    command = ssh_command(login['username'], config['host'], config['port'],
                          config['remote_host'], config['remote_port'])
    del login
    exec_fn(command[0], command, tunnel_environment(config['askpass']))
    return 0


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--askpass', action='store_true')
    args = parser.parse_args()
    try:
        result = main(json.loads(Path(args.config).read_text()), args.askpass)
    except Exception:
        # Do not echo vault errors, password, session, or SSH authentication reply.
        import sys
        print('Vault-backed tunnel unavailable; launchd will retry.', file=sys.stderr)
        result = 1
    raise SystemExit(result)
