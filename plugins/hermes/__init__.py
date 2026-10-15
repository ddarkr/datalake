"""Native metadata-only Hermes usage observer; no SDK or core patches."""
import hashlib
import json
import math
import re
import stat
import os
from decimal import Decimal
from collections import OrderedDict
from pathlib import Path
import time
import logging
import urllib.request
from urllib.parse import urlsplit, urlunsplit


class Runtime:
    """Capture profile once; hooks send direct bounded HTTP, no queue or worker."""
    def __init__(self, home, config):
        self.scope = hashlib.sha256(str(home.resolve()).encode()).hexdigest()
        self.exporter = Exporter(config['endpoint'], config['headers'], config['timeout'])
        # Stable in-memory dedup only: suppress repeated hook deliveries of the
        # same identity within this process. Never persisted, never replayed.
        self.seen = set()
        self.pending = OrderedDict()

    def _request_key(self, data):
        if type(data) is dict and all(_identity(data.get(k)) for k in ('session_id', 'api_request_id')):
            return _hash([self.scope, data['session_id'], data['api_request_id']], 64)
        return None

    def prepare(self, data):
        key = self._request_key(data)
        if key is None:
            return
        self.pending.pop(key, None)
        projected = request_settings(data.get('request'))
        if projected:
            # Bounded scalar-only snapshots; abandoned requests evict oldest first.
            self.pending[key] = projected
            if len(self.pending) > 256:
                self.pending.popitem(last=False)

    def discard(self, data):
        return self.pending.pop(self._request_key(data), {})

    def observe(self, data, auxiliary=False):
        try:
            requested = {} if auxiliary else self.discard(data)
            body = project(data, self.scope, auxiliary)
            if body is not None and requested:
                body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]['attributes'].extend(
                    {'key': k, 'value': {'intValue': str(v)} if type(v) is int else {'stringValue': v}}
                    for k, v in requested.items())
            if body is None:
                return
            span = body['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
            identity = span['traceId'] + span['spanId']
            if identity in self.seen:
                _LOG.warning('Usage event duplicate; not resent')
                return
            self.seen.add(identity)
            if not self.exporter.send(body):
                _LOG.warning('Usage export not acknowledged; dropped without retry (details suppressed)')
        except Exception:
            _LOG.warning('Usage event not exported (details suppressed)')



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
        _LOG.warning('Usage exporter disabled: invalid configuration (details suppressed)')
        return
    def main(**kwargs):
        runtime.observe(kwargs)
    def auxiliary(**kwargs):
        runtime.observe(kwargs, auxiliary=True)
    def before(**kwargs):
        runtime.prepare(kwargs)
    def failed(**kwargs):
        runtime.discard(kwargs)
    ctx.register_hook('pre_api_request', before)
    ctx.register_hook('api_request_error', failed)
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
    if type(timeout) not in (int, float) or not 0.1 <= timeout <= 5:
        raise ValueError('invalid timeout')
    capture_auxiliary = ctx.get_config('capture_auxiliary', False)
    if type(capture_auxiliary) is not bool:
        raise ValueError('capture_auxiliary must be boolean')
    # Legacy capacity/export_paused keys from queue-era profiles are ignored:
    # there is no ledger to size or pause, and old databases are never opened.
    return dict(endpoint=endpoint, headers=headers, timeout=timeout, capture_auxiliary=capture_auxiliary)


_LOG = logging.getLogger('datalake.hermes')


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Exporter:
    """One span/request, no redirects or ambient HTTP proxies, bounded replies.

    Direct fire-and-report: send() returns collector full ACK as a boolean and
    never retries. Callers drop failures; there is no queue or replay.
    """
    def __init__(self, endpoint, headers, timeout=2):
        self.endpoint, self.headers, self.timeout = endpoint, headers, timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def send(self, body):
        try:
            payload = json.dumps(body, separators=(',', ':'), allow_nan=False).encode()
            request = urllib.request.Request(self.endpoint, data=payload, method='POST',
                                             headers={**self.headers, 'Content-Type': 'application/json'})
            deadline = time.monotonic() + self.timeout
            with self.opener.open(request, timeout=self.timeout) as response:
                chunks = []
                size = 0
                while True:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('response deadline')
                    chunk = response.read1(min(8192, 65537 - size))
                    if time.monotonic() >= deadline:
                        raise TimeoutError('response deadline')
                    if not chunk:
                        break
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > 65536:
                        raise ValueError('oversized response')
                raw = b''.join(chunks)
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
                return 200 <= response.status < 300
        except Exception:
            return False



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

def request_settings(request):
    """Pinned Hermes wraps the final request as {method, body}; retain scalars only."""
    if type(request) is not dict or type(request.get('body')) is not dict:
        return {}
    body = request['body']
    result = {}
    tier = body.get('service_tier')
    if type(tier) is str and tier in ('auto', 'default', 'flex', 'scale', 'priority', 'ultrafast', 'fast', 'normal'):
        result['coding_agent.request.service_tier'] = tier
    reasoning = body.get('reasoning')
    output = body.get('output_config')
    thinking = body.get('thinking')
    effort = body.get('reasoning_effort')
    if effort is None and type(reasoning) is dict:
        effort = reasoning.get('effort')
    if effort is None and type(output) is dict:
        effort = output.get('effort')
    if type(effort) is str and effort in ('none', 'off', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'):
        result['coding_agent.request.reasoning.effort'] = effort
    budget = thinking.get('budget_tokens') if type(thinking) is dict else None
    if type(budget) is int and 0 <= budget <= 9007199254740991:
        result['coding_agent.request.reasoning.budget_tokens'] = budget
    if result:
        result['coding_agent.request.settings_source'] = 'wire'
    return result



def project(data, scope, auxiliary=False):
    """Project canonical Hermes buckets (input excludes cache) into OTLP.

    Unknown fields stay absent. Never inspect response/content objects.
    """
    if type(data) is not dict or not _identity(scope):
        return None
    if data.get('error') is not None:
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
    keys = ('input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_write_tokens', 'reasoning_tokens', 'prompt_tokens', 'total_tokens')
    counts = {k: usage[k] for k in keys if k in usage and usage[k] is not None} if type(usage) is dict else {}
    buckets = ('input_tokens', 'cache_read_tokens', 'cache_write_tokens')
    if (any(type(v) is not int or not 0 <= v <= 9007199254740991 for v in counts.values())
            or not any(k in counts for k in ('prompt_tokens', 'output_tokens'))
            or (all(k in counts for k in (*buckets, 'prompt_tokens')) and sum(counts[k] for k in buckets) != counts['prompt_tokens'])
            or (all(k in counts for k in ('prompt_tokens', 'output_tokens', 'total_tokens')) and counts['prompt_tokens'] + counts['output_tokens'] != counts['total_tokens'])):
        counts = {}
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
    if counts:
        metadata['coding_agent.usage.source'] = 'reported'
    if response_model is not None:
        metadata['gen_ai.response.model'] = response_model
    metadata['request_id'] = _hash([scope, data['session_id'], event], 64)
    duration = (Decimal(str(end)) - Decimal(str(start))) * 1000
    metadata['duration_ms'] = int(duration) if duration == int(duration) else float(duration)
    metadata['coding_agent.request.duration_ms'] = metadata['duration_ms']
    metadata['coding_agent.duration.kind'] = 'logical_call'
    first = data.get('first_chunk_at')
    if type(first) in (int, float) and start <= first <= end and math.isfinite(first):
        ttft = (Decimal(str(first)) - Decimal(str(start))) * 1000
        metadata['ttft_ms'] = int(ttft) if ttft == int(ttft) else float(ttft)
        metadata['coding_agent.ttft.kind'] = 'first_stream_event'
    attrs = [{'key': k, 'value': {'intValue': str(v)} if type(v) is int else {'doubleValue': v} if type(v) is float else {'stringValue': v}} for k, v in metadata.items()]
    span = dict(traceId=_hash(['hermes', session], 32), spanId=_hash(['hermes', session, event], 16),
                name='coding_agent.llm.turn', kind=1,
                startTimeUnixNano=_nanoseconds(start),
                endTimeUnixNano=_nanoseconds(end),
                attributes=attrs, status={'code': 1})
    return {'resourceSpans': [{'resource': {'attributes': [{'key': 'service.name', 'value': {'stringValue': 'hermes'}}]},
                              'scopeSpans': [{'scope': {'name': 'doda-datalake', 'version': '1.0.0'}, 'spans': [span]}]}]}
