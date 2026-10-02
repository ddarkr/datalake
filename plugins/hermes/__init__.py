"""Native metadata-only Hermes usage observer; no SDK or core patches."""
import hashlib
import json
import math
import re
from contextlib import contextmanager
from decimal import Decimal
import os
from pathlib import Path
import sqlite3
import time
import uuid
import logging
import urllib.request
from urllib.parse import urlsplit, urlunsplit
import stat
import threading
import atexit


class Runtime:
    """Capture profile once; hooks do bounded local writes, worker owns HTTP."""
    def __init__(self, home, config):
        self.scope = hashlib.sha256(str(home.resolve()).encode()).hexdigest()
        self.outbox = Outbox(home / 'usage-otel' / 'outbox.sqlite3', capacity=config['capacity'])
        self.exporter = Exporter(self.outbox, config['endpoint'], config['headers'], config['timeout'])
        self.export_paused = config.get('export_paused', False)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name='datalake-usage', daemon=True)
        self.thread.start()
        atexit.register(self.close)

    def observe(self, data, auxiliary=False):
        try:
            body = project(data, self.scope, auxiliary)
            if body is not None and not self.outbox.put(body):
                _LOG.warning('Usage event duplicate or ledger capacity reached; not appended')
        except Exception:
            _LOG.warning('Usage event not persisted (details suppressed)')

    def _run(self):
        while not self.stop.is_set():
            try:
                if not self.export_paused:
                    self.exporter.send_once()
            except Exception:
                _LOG.warning('Usage outbox unavailable; retry deferred (details suppressed)')
            self.stop.wait(0.25)

    def close(self):
        # Give the single worker a short opportunity to drain; never block exit
        # on network timeout. In-flight events stay leased and replay on restart.
        if self.stop.is_set():
            return
        self.thread.join(0.25)
        self.stop.set()
        atexit.unregister(self.close)


def register(ctx):
    """Native CLI/gateway contract; no asyncio loop or global-profile assumptions."""
    from hermes_constants import get_hermes_home
    try:
        home = Path(get_hermes_home())
        config = settings(ctx, home)
        if config is None:
            return
        runtime = Runtime(home, config)
    except Exception:
        _LOG.warning('Usage exporter disabled: invalid configuration or private outbox (details suppressed)')
        return
    ctx.on_unload(runtime.close)
    def main(**kwargs):
        runtime.observe(kwargs)
    def auxiliary(**kwargs):
        runtime.observe(kwargs, auxiliary=True)
    ctx.register_hook('post_api_request', main)
    # f42f579 emits the main MoA aggregator through BOTH hooks with distinct
    # request IDs; aux_task cannot distinguish it from standalone synthesis.
    # Conservative coverage is main-only unless explicitly opted into overlap.
    if config['capture_auxiliary']:
        ctx.register_hook('post_auxiliary_call', auxiliary)



def settings(ctx, home, environ=None):
    """Settings from config.yaml via native context; secrets never from YAML."""
    environ = os.environ if environ is None else environ
    endpoint = ctx.get_config('endpoint', '')
    if not endpoint:
        return None
    if ctx.get_config('headers', None) is not None:
        raise ValueError('headers belong in credentials file or environment')
    if type(endpoint) is not str or len(endpoint) > 2048 or re.search(r'[\s\x00-\x1f\x7f]', endpoint):
        raise ValueError('invalid endpoint')
    url = urlsplit(endpoint)
    if url.username or url.password or url.query or url.fragment or not url.hostname:
        raise ValueError('invalid endpoint')
    if url.scheme != 'https' and not (url.scheme == 'http' and url.hostname in ('localhost', '127.0.0.1', '::1')):
        raise ValueError('HTTPS required outside loopback')
    path = url.path.rstrip('/')
    endpoint = urlunsplit((url.scheme, url.netloc, path if path.endswith('/v1/traces') else path + '/v1/traces', '', ''))
    secret = environ.get('DATALAKE_HERMES_OTLP_HEADERS')
    credential_path = Path(home) / 'usage-otel.credentials.json'
    if secret is None:
        try:
            fd = os.open(credential_path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, 'r') as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 65536 or info.st_uid != os.getuid():
                    raise ValueError('unsafe credentials')
                secret = stream.read(65537)
        except FileNotFoundError:
            secret = '{}'
        except OSError:
            raise ValueError('unsafe credentials') from None
    try:
        headers = json.loads(secret)
    except (ValueError, TypeError):
        raise ValueError('invalid headers') from None
    if type(headers) is not dict or len(headers) > 16:
        raise ValueError('invalid headers')
    forbidden = {'host', 'content-length', 'content-type', 'connection', 'transfer-encoding', 'cookie', 'proxy-authorization'}
    for key, value in headers.items():
        if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}", key) or key.lower() in forbidden or type(value) is not str or len(value) > 8192 or re.search(r'[\x00-\x1f\x7f]', value):
            raise ValueError('invalid headers')
    timeout = ctx.get_config('timeout_seconds', 2)
    capacity = ctx.get_config('capacity', 100000)
    if type(timeout) not in (int, float) or not 0.1 <= timeout <= 5 or type(capacity) is not int or not 1 <= capacity <= 1000000:
        raise ValueError('invalid limits')
    capture_auxiliary = ctx.get_config('capture_auxiliary', False)
    if type(capture_auxiliary) is not bool:
        raise ValueError('capture_auxiliary must be boolean')
    export_paused = ctx.get_config('export_paused', False)
    if type(export_paused) is not bool:
        raise ValueError('export_paused must be boolean')
    return dict(endpoint=endpoint, headers=headers, timeout=timeout, capacity=capacity, capture_auxiliary=capture_auxiliary, export_paused=export_paused)


_LOG = logging.getLogger('datalake.hermes')


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Exporter:
    """One span/request, no redirects or ambient HTTP proxies, bounded replies."""
    def __init__(self, outbox, endpoint, headers, timeout=2):
        self.outbox, self.endpoint, self.headers, self.timeout = outbox, endpoint, headers, timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def send_once(self, now=None):
        claim = self.outbox.claim(now)
        if claim is None:
            return False
        success = False
        try:
            request = urllib.request.Request(self.endpoint, data=claim[2].encode(), method='POST',
                                             headers={**self.headers, 'Content-Type': 'application/json'})
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise ValueError('oversized response')
                result = json.loads(raw) if raw else {}
                if type(result) is not dict:
                    raise ValueError('invalid response')
                partial = result.get('partialSuccess', {})
                if type(partial) is not dict:
                    raise ValueError('invalid partial response')
                rejected = partial.get('rejectedSpans', 0)
                if not ((type(rejected) is int and rejected == 0) or (type(rejected) is str and re.fullmatch(r'0+', rejected))):
                    raise ValueError('partial rejection')
                success = 200 <= response.status < 300
        except Exception:
            _LOG.warning('Usage export not acknowledged; retained for retry (details suppressed)')
        self.outbox.finish(claim, success, now)
        return success



class Outbox:
    """Private SQLite ledger: immutable payloads, transactional leases, tombstones.

    A connection per operation supports hook/worker threads and multiple processes.
    Capacity includes tombstones: never silently evict a pending event or dedup ID.
    """
    def __init__(self, path, capacity=100000):
        self.path = Path(path)
        self.capacity = capacity
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = self.path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077 or parent.st_uid != os.getuid():
            raise ValueError('unsafe outbox directory')
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise ValueError('unsafe outbox permissions')
        finally:
            os.close(fd)
        with self._connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, body TEXT, state TEXT NOT NULL, lease TEXT, due REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0)')

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(str(self.path), timeout=0.05)
        try:
            db.execute('PRAGMA synchronous=FULL')
            with db:
                yield db
        finally:
            db.close()

    def put(self, body):
        span = body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        identity = span['traceId'] + span['spanId']
        encoded = json.dumps(body, separators=(',', ':'), allow_nan=False)
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT COUNT(*) FROM events').fetchone()[0] >= self.capacity:
                return False
            return db.execute('INSERT OR IGNORE INTO events(id,body,state) VALUES (?, ?, ?)', (identity, encoded, 'pending')).rowcount == 1

    def claim(self, now=None):
        now = time.time() if now is None else now
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT id,body,attempts FROM events WHERE state != 'done' AND due <= ? ORDER BY due,id LIMIT 1", (now,)).fetchone()
            if row is None:
                return None
            lease = uuid.uuid4().hex
            db.execute("UPDATE events SET state='leased',lease=?,due=? WHERE id=?", (lease, now + 30, row[0]))
            return row[0], lease, row[1], row[2]

    def finish(self, claim, success, now=None):
        now = time.time() if now is None else now
        with self._connect() as db:
            if success:
                result = db.execute("UPDATE events SET state='done',body=NULL,lease=NULL WHERE id=? AND lease=?", claim[:2])
            else:
                result = db.execute("UPDATE events SET state='pending',lease=NULL,due=?,attempts=attempts+1 WHERE id=? AND lease=?",
                                    (now + min(60, 2 ** min(claim[3], 6)), claim[0], claim[1]))
            return result.rowcount == 1



def _identity(value):
    return type(value) is str and re.fullmatch(r'[!-~]{1,512}', value) is not None


def _label(value):
    return type(value) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}', value) is not None and '://' not in value


def _hash(value, length):
    return hashlib.sha256(json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()[:length]


def _nanoseconds(seconds):
    # Exact port of otel.mjs's millisecond-to-nanosecond representation.
    milliseconds = seconds * 1000
    return str(math.trunc(milliseconds) * 1000000 + math.floor((milliseconds % 1) * 1000000 + 0.5))


def project(data, scope, auxiliary=False):
    """Project canonical Hermes buckets (input excludes cache) into OTLP.

    Unknown fields stay absent. Never inspect response/content objects.
    """
    if type(data) is not dict or not _identity(scope):
        return None
    if auxiliary and (data.get('streaming') is True or data.get('error') is not None):
        return None
    if not all(_identity(data.get(k)) for k in ('session_id', 'api_request_id')):
        return None
    if not all(_label(data.get(k)) for k in ('model', 'provider')):
        return None
    start, end = data.get('started_at'), data.get('ended_at')
    if not all(type(t) in (int, float) and 0 <= t <= 9007199254 and math.isfinite(t) for t in (start, end)) or end < start:
        return None
    usage = data.get('usage')
    if type(usage) is not dict:
        return None
    keys = ('input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_write_tokens', 'reasoning_tokens', 'prompt_tokens', 'total_tokens')
    counts = {k: usage[k] for k in keys if k in usage and usage[k] is not None}
    if not counts or any(type(v) is not int or not 0 <= v <= 9007199254740991 for v in counts.values()):
        return None
    if not any(k in counts for k in ('prompt_tokens', 'output_tokens')):
        return None
    buckets = ('input_tokens', 'cache_read_tokens', 'cache_write_tokens')
    if all(k in counts for k in (*buckets, 'prompt_tokens')) and sum(counts[k] for k in buckets) != counts['prompt_tokens']:
        return None
    if all(k in counts for k in ('prompt_tokens', 'output_tokens', 'total_tokens')) and counts['prompt_tokens'] + counts['output_tokens'] != counts['total_tokens']:
        return None
    response_model = data.get('response_model')
    if response_model is not None and not _label(response_model):
        return None
    session = _hash([scope, data['session_id']], 64)
    attempt = data.get('retry_count', 0)
    if type(attempt) is not int or not 0 <= attempt <= 1000000:
        return None
    event = json.dumps(['post_auxiliary_call' if auxiliary else 'post_api_request', data['api_request_id'], attempt], separators=(',', ':'))
    mapping = {'prompt_tokens': 'gen_ai.usage.input_tokens', 'output_tokens': 'gen_ai.usage.output_tokens',
               'cache_read_tokens': 'gen_ai.usage.cache_read.input_tokens',
               'cache_write_tokens': 'gen_ai.usage.cache_write.input_tokens',
               'reasoning_tokens': 'gen_ai.usage.reasoning.output_tokens'}
    metadata = {dest: counts[src] for src, dest in mapping.items() if src in counts}
    metadata.update({
        'gen_ai.provider.name': data['provider'],
        'gen_ai.request.model': data['model'],
        'coding_agent.client': 'hermes',
        'coding_agent.session.id': session,
        'coding_agent.content_capture_mode': 'metadata_only',
        'coding_agent.signal_source': 'datalake-plugin',
    })
    if response_model is not None:
        metadata['gen_ai.response.model'] = response_model
    metadata['request_id'] = _hash([scope, data['session_id'], event], 64)
    duration = (Decimal(str(end)) - Decimal(str(start))) * 1000
    metadata['duration_ms'] = int(duration) if duration == int(duration) else float(duration)
    first = data.get('first_chunk_at')
    if type(first) in (int, float) and math.isfinite(first) and start <= first <= end:
        ttft = (Decimal(str(first)) - Decimal(str(start))) * 1000
        metadata['ttft_ms'] = int(ttft) if ttft == int(ttft) else float(ttft)
    attrs = [{'key': k, 'value': {'intValue': str(v)} if type(v) is int else {'doubleValue': v} if type(v) is float else {'stringValue': v}} for k, v in metadata.items()]
    span = dict(traceId=_hash(['hermes', session], 32), spanId=_hash(['hermes', session, event], 16),
                name='coding_agent.llm.turn', kind=1,
                startTimeUnixNano=_nanoseconds(start),
                endTimeUnixNano=_nanoseconds(end),
                attributes=attrs, status={'code': 1})
    return {'resourceSpans': [{'resource': {'attributes': [{'key': 'service.name', 'value': {'stringValue': 'hermes'}}]},
                              'scopeSpans': [{'scope': {'name': 'doda-datalake', 'version': '1.0.0'}, 'spans': [span]}]}]}
