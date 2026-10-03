import { createHash, randomUUID } from 'node:crypto';
import { readFileSync, realpathSync, statSync } from 'node:fs';
import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

export function isMain(url) {
  try { return fileURLToPath(url) === realpathSync(process.argv[1]); }
  catch { return false; }
}

// One native hook per session; never evict a live process just because export is slow.
export async function withStateLock(directory, action) {
  const lock = join(directory, 'hook.lock');
  const candidate = `${lock}.${randomUUID()}`;
  const ownerPath = path => join(path, 'owner.json');
  const dead = pid => {
    if (!Number.isSafeInteger(pid) || pid <= 0) return false;
    try { process.kill(pid, 0); return false; }
    catch (error) { return error.code === 'ESRCH'; }
  };
  const owner = async () => {
    try { return JSON.parse(await readFile(ownerPath(lock), 'utf8')).pid; }
    catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
  };
  await mkdir(candidate, { mode: 0o700 });
  let acquired = false;
  try {
    await writeFile(ownerPath(candidate), JSON.stringify({ pid: process.pid }), { mode: 0o600, flag: 'wx' });
    for (let attempt = 0; attempt < 25; attempt++) {
      try { await rename(candidate, lock); acquired = true; break; }
      catch (error) { if (!['EEXIST', 'ENOTEMPTY'].includes(error.code)) throw error; }
      if (dead(await owner())) {
        // Only one contender reaps; recheck after claiming in case another replaced the old lock.
        const reaping = join(lock, 'reaping');
        try {
          await mkdir(reaping, { mode: 0o700 });
          let retired = false;
          try {
            if (dead(await owner())) {
              const removed = `${lock}.${randomUUID()}`;
              // Rename before deleting: never expose an empty live lock to a new contender.
              await rename(lock, removed);
              retired = true;
              await rm(removed, { recursive: true, force: true });
            }
          } finally { if (!retired) await rm(reaping, { recursive: true, force: true }); }
        } catch (error) { if (!['EEXIST', 'ENOENT'].includes(error.code)) throw error; }
      }
      await delay(10);
    }
    if (acquired) return await action();
    // Contention leaves checkpoints untouched; a later native hook retries the same records.
  } finally {
    if (acquired) {
      const removed = `${lock}.${randomUUID()}`;
      await rename(lock, removed);
      await rm(removed, { recursive: true, force: true });
    } else await rm(candidate, { recursive: true, force: true });
  }
}

export const defaultConfigPath = () => process.env.DATALAKE_OTEL_CONFIG || join(homedir(), '.config', 'doda-datalake', 'otel.json');
const kinds = new Set(['session', 'llm.turn', 'tool.call', 'edit.decision', 'subagent', 'git.commit', 'git.pull_request']);
const numericKeys = new Set([
  'gen_ai.usage.input_tokens', 'gen_ai.usage.output_tokens',
  'gen_ai.usage.cache_read.input_tokens', 'gen_ai.usage.cache_write.input_tokens',
  'gen_ai.usage.cache_creation.input_tokens', 'gen_ai.usage.reasoning.output_tokens',
  'cost_usd', 'pi.gen_ai.usage.total_tokens', 'pi.gen_ai.cost.estimated_usd',
  'pi.gen_ai.cost.input_usd', 'pi.gen_ai.cost.output_usd',
  'coding_agent.session.subagent_count', 'coding_agent.session.lines.added',
  'coding_agent.session.lines.removed', 'coding_agent.session.lines.accepted',
  'coding_agent.session.lines.rejected', 'coding_agent.session.edit.accept_count',
  'coding_agent.session.edit.reject_count', 'coding_agent.session.commit_count',
  'coding_agent.session.pr_count', 'coding_agent.session.duration_ms',
  'coding_agent.subagent.duration_ms', 'coding_agent.edit.lines.added',
  'coding_agent.edit.lines.removed', 'duration_ms', 'ttft_ms', 'attempt',
]);
const stringKeys = new Set([
  'gen_ai.operation.name', 'gen_ai.provider.name', 'gen_ai.request.model',
  'gen_ai.response.model', 'gen_ai.response.id', 'gen_ai.tool.name',
  'gen_ai.tool.call.id', 'gen_ai.conversation.id', 'gen_ai.agent.id',
  'coding_agent.agent.id', 'coding_agent.agent.parent_id',
  'coding_agent.signal_source', 'coding_agent.subagent.type',
  'coding_agent.subagent.status', 'coding_agent.edit.decision',
  'coding_agent.edit.tool.name', 'coding_agent.edit.language',
  'coding_agent.session.outcome', 'coding_agent.repository.id',
  'vcs.ref.head.name', 'vcs.ref.head.revision', 'error.type',
  'pi.gen_ai.tool.status', 'turn.id', 'request_id', 'tool_use_id',
]);
const forbiddenHeaders = new Set(['host', 'content-length', 'content-type', 'connection', 'transfer-encoding', 'cookie']);
const hash = (value, length) => createHash('sha256').update(value).digest('hex').slice(0, length);
const diagnostic = message => console.error(`[datalake-otel] ${message}`);

export function parseHeaders(value = '') {
  const headers = {};
  for (const part of value.split(',')) {
    if (!part.trim()) continue;
    const separator = part.indexOf('=');
    if (separator < 1) throw new Error('Invalid OTLP header configuration');
    try {
      headers[part.slice(0, separator).trim()] = decodeURIComponent(part.slice(separator + 1).trim());
    } catch {
      throw new Error('Invalid OTLP header configuration');
    }
  }
  return headers;
}

export function validateConfig(raw) {
  if (!raw || typeof raw !== 'object' || typeof raw.endpoint !== 'string') throw new Error('OTLP endpoint is required');
  let endpoint;
  try { endpoint = new URL(raw.endpoint); } catch { throw new Error('Invalid OTLP endpoint'); }
  if (endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new Error('OTLP endpoint must not contain credentials, query, or fragment');
  if (raw.allowInsecureHttp !== undefined && typeof raw.allowInsecureHttp !== 'boolean') throw new Error('OTLP allowInsecureHttp must be a boolean');
  const privateIpv4 = /^(?:10\.\d+\.\d+\.\d+|172\.(?:1[6-9]|2\d|3[01])\.\d+\.\d+|192\.168\.\d+\.\d+)$/.test(endpoint.hostname);
  const httpAllowed = ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname) || (raw.allowInsecureHttp === true && privateIpv4);
  if (endpoint.protocol !== 'https:' && !(endpoint.protocol === 'http:' && httpAllowed)) {
    throw new Error('OTLP requires HTTPS or loopback HTTP; private IPv4 HTTP requires allowInsecureHttp');
  }
  const path = endpoint.pathname.replace(/\/+$/, '');
  endpoint.pathname = path.endsWith('/v1/traces') ? path : `${path}/v1/traces`;
  const headers = {};
  if (raw.headers !== undefined && (!raw.headers || typeof raw.headers !== 'object' || Array.isArray(raw.headers))) throw new Error('Invalid OTLP headers');
  for (const [name, value] of Object.entries(raw.headers || {})) {
    if (!/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(name) || forbiddenHeaders.has(name.toLowerCase()) || typeof value !== 'string' || /[\r\n]/.test(value)) throw new Error('Invalid OTLP headers');
    headers[name] = value;
  }
  const timeoutMs = raw.timeoutMs ?? 2000;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 30000) throw new Error('OTLP timeoutMs must be between 100 and 30000');
  return { endpoint: endpoint.href, headers, timeoutMs, ...(raw.allowInsecureHttp === true ? { allowInsecureHttp: true } : {}) };
}

export function loadConfig(options = {}) {
  if (options.endpoint !== undefined) return validateConfig(options);
  const path = options.configPath || defaultConfigPath();
  let raw;
  try {
    const info = statSync(path);
    if (!info.isFile() || info.size > 65536 || (process.platform !== 'win32' && (info.mode & 0o077))) throw new Error('unsafe-config');
    raw = JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT' && !options.configPath && !process.env.DATALAKE_OTEL_CONFIG) {
      if (!process.env.OTEL_EXPORTER_OTLP_ENDPOINT) return null;
      return validateConfig({ endpoint: process.env.OTEL_EXPORTER_OTLP_ENDPOINT, headers: parseHeaders(process.env.OTEL_EXPORTER_OTLP_HEADERS) });
    }
    throw new Error('Cannot read private OTLP configuration; check JSON and file permissions (0600)');
  }
  return validateConfig(raw);
}

// Provider IDs are opaque, not URL paths (OpenAI tool IDs include call_id|item_id).
// Bound printable ASCII; OTLP trace/span IDs are hashes of these identities.
const validIdentity = value => typeof value === 'string' && /^[\x21-\x7e]{1,512}$/.test(value);

function spanFor(client, event) {
  if (!event || !kinds.has(event.kind) || ![event.sessionId, event.eventId].every(validIdentity)) throw new Error('Invalid telemetry event identity');
  if (![event.startTimeMs, event.endTimeMs].every(time => Number.isFinite(time) && time >= 0 && time <= Number.MAX_SAFE_INTEGER / 1e3) || event.endTimeMs < event.startTimeMs) throw new Error('Invalid telemetry event interval');
  const metadata = {};
  for (const key of numericKeys) {
    const value = event.attributes?.[key];
    if (typeof value === 'number' && Number.isFinite(value) && value >= 0) metadata[key] = value;
  }
  for (const key of stringKeys) {
    const value = event.attributes?.[key];
    if (typeof value === 'string' && value.length <= 256 && !/[\x00-\x1f\x7f]/.test(value)) metadata[key] = value;
  }
  if (metadata['coding_agent.repository.id'] && !/^[a-f0-9]{64}$/.test(metadata['coding_agent.repository.id'])) delete metadata['coding_agent.repository.id'];
  Object.assign(metadata, {
    'coding_agent.client': client,
    'coding_agent.session.id': event.sessionId,
    'coding_agent.content_capture_mode': 'metadata_only',
    'coding_agent.signal_source': metadata['coding_agent.signal_source'] || 'datalake-plugin',
  });
  const attributes = Object.entries(metadata).map(([key, value]) => ({
    key, value: typeof value === 'number'
      ? (Number.isSafeInteger(value) ? { intValue: String(value) } : { doubleValue: value })
      : { stringValue: value },
  }));
  const ns = time => String(BigInt(Math.trunc(time)) * 1000000n + BigInt(Math.round((time % 1) * 1e6)));
  const span = {
    traceId: hash(JSON.stringify([client, event.sessionId]), 32),
    spanId: hash(JSON.stringify([client, event.sessionId, event.eventId]), 16),
    name: `coding_agent.${event.kind}`, kind: 1,
    startTimeUnixNano: ns(event.startTimeMs), endTimeUnixNano: ns(event.endTimeMs),
    attributes, status: { code: event.error ? 2 : 1 },
  };
  if (validIdentity(event.parentEventId) && event.parentEventId !== event.eventId) {
    span.parentSpanId = hash(JSON.stringify([client, event.sessionId, event.parentEventId]), 16);
  }
  return span;
}

export function createTelemetry(client, options = {}) {
  if (typeof client !== 'string' || !/^[a-z0-9][a-z0-9-]{0,63}$/.test(client)) throw new Error('Invalid telemetry client');
  let config;
  try { config = loadConfig(options); } catch {
    diagnostic('Export disabled: invalid private configuration');
  }
  if (config === null) diagnostic('Export disabled: configure DATALAKE_OTEL_CONFIG or the private default config');
  const pending = new Set();
  async function send(event) {
    try {
      const serviceName = event?.attributes?.['service.name'];
      const resourceName = typeof serviceName === 'string' && /^[A-Za-z0-9_.-]{1,128}$/.test(serviceName) ? serviceName : client;
      const body = JSON.stringify({ resourceSpans: [{
        resource: { attributes: [{ key: 'service.name', value: { stringValue: resourceName } }] },
        scopeSpans: [{ scope: { name: 'doda-datalake', version: '1.0.0' }, spans: [spanFor(client, event)] }],
      }] });
      const response = await fetch(config.endpoint, {
        method: 'POST', redirect: 'error',
        headers: { ...config.headers, 'Content-Type': 'application/json' },
        body, signal: AbortSignal.timeout(config.timeoutMs),
      });
      if (!response.ok) {
        await response.body?.cancel();
        diagnostic(`Export rejected (HTTP ${response.status})`);
        return false;
      }
      // An OTLP partial success is not a successful delivery of this span.
      let text = '';
      let bytes = 0;
      const decoder = new TextDecoder();
      for await (const chunk of response.body || []) {
        bytes += chunk.byteLength;
        if (bytes > 65536) throw new Error('oversized-response');
        text += decoder.decode(chunk, { stream: true });
      }
      text += decoder.decode();
      if (text) {
        const result = JSON.parse(text);
        if (Number(result.partialSuccess?.rejectedSpans || 0) > 0) {
          diagnostic('Collector rejected the telemetry span');
          return false;
        }
      }
      return true;
    } catch {
      diagnostic('Export failed; event not delivered (details suppressed to protect credentials)');
      return false;
    }
  }
  return {
    emit(event) {
      if (!config) return Promise.resolve(false);
      // ponytail: bounded in-flight export, not an offline spool; use a local OTel collector when durable client delivery is required.
      if (pending.size >= 128) {
        diagnostic('Export queue full; event not delivered');
        return Promise.resolve(false);
      }
      const request = send(event);
      pending.add(request);
      request.finally(() => pending.delete(request));
      return request;
    },
    async flush() { await Promise.all([...pending]); },
  };
}

export function repositoryContext(cwd) {
  if (typeof cwd !== 'string' || !cwd) return {};
  const git = args => spawnSync('git', ['-C', cwd, ...args], { encoding: 'utf8', timeout: 1000, stdio: ['ignore', 'pipe', 'ignore'] });
  const root = git(['rev-parse', '--show-toplevel']);
  if (root.status !== 0 || !root.stdout.trim()) return {};
  const metadata = { 'coding_agent.repository.id': hash(root.stdout.trim(), 64) };
  const branch = git(['symbolic-ref', '--quiet', '--short', 'HEAD']);
  if (branch.status === 0 && branch.stdout.trim().length <= 256) metadata['vcs.ref.head.name'] = branch.stdout.trim();
  return metadata;
}
