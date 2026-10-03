import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, readFile, writeFile, appendFile, rm, stat, readdir, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { handleHook, usageEvent } from '../plugins/claude-code/hook.mjs';
import { install } from '../plugins/claude-code/install.mjs';
import { openOutbox, readRoute } from '../plugins/outbox.mjs';

const secret = 'PRIVATE_PROMPT_RESPONSE_TOOL_SECRET';
const hash = value => createHash('sha256').update(value).digest('hex');
const run = promisify(execFile);

async function until(predicate, description) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) return value;
    await delay(20);
  }
  assert.fail(`Timed out waiting for ${description}`);
}
const record = (id = 'msg_real', time = 1500) => ({
  type: 'assistant', sessionId: 'session_1', timestamp: new Date(time).toISOString(),
  cwd: '/private/source', message: { id, model: 'claude-sonnet-4-6', stop_reason: 'end_turn',
    content: [{ type: 'text', text: secret }], usage: { input_tokens: 20, output_tokens: 7, cache_read_input_tokens: 100, cache_creation_input_tokens: 30, secret } },
});

async function sandbox(t) {
  const path = await mkdtemp(join(tmpdir(), 'datalake-claude-'));
  t.after(() => rm(path, { recursive: true, force: true }));
  return path;
}

test('Claude usage only exports finalized, actual numeric API metadata', () => {
  const options = { sessionId: 'session_1', sinceMs: 1000 };
  const event = usageEvent(record(), options);
  assert.equal(event.attributes['gen_ai.usage.input_tokens'], 20);
  assert.equal(event.attributes['gen_ai.usage.output_tokens'], 7);
  assert.equal(event.attributes['gen_ai.usage.cache_read.input_tokens'], 100);
  assert.equal(event.attributes['gen_ai.usage.cache_creation.input_tokens'], 30);
  assert.equal(event.startTimeMs, event.endTimeMs);
  assert.equal(JSON.stringify(event).includes(secret), false);
  assert.equal(JSON.stringify(event).includes('/private'), false);
  assert.equal(Object.keys(event.attributes).some(key => key.includes('cost')), false);
  assert.equal(usageEvent(record(), { ...options, sessionId: 'unrelated' }), undefined);
  assert.equal(usageEvent(record('old', 500), options), undefined);
  const invalid = record();
  invalid.message.usage = { input_tokens: '20', output_tokens: -1, cache_read_input_tokens: NaN };
  assert.equal(usageEvent(invalid, options), undefined);
  const streaming = record(); streaming.message.stop_reason = null;
  assert.equal(usageEvent(streaming, options), undefined);
});

test('Claude lifecycle joins stable tool/subagent IDs, records real durations and excludes hook content', async t => {
  const path = await sandbox(t), output = [];
  const invoke = (hook_event_name, now, fields = {}) => handleHook({ session_id: 'session_1', hook_event_name, cwd: '/private/source', prompt: secret, tool_input: { command: secret }, tool_response: { content: secret }, last_assistant_message: secret, ...fields }, {
    stateRoot: path, now, context: async () => ({}), telemetry: { enqueue: async event => { output.push(event); return true; }, flushLocal: async () => {} },
  });
  await invoke('SessionStart', 1000);
  await invoke('SubagentStart', 1100, { agent_id: 'agent_1', agent_type: secret });
  await invoke('PreToolUse', 1200, { tool_use_id: 'tool_1', agent_id: 'agent_1', tool_name: `mcp__${secret}` });
  await invoke('PostToolUseFailure', 1300, { tool_use_id: 'tool_1', agent_id: 'agent_1', error: secret });
  await invoke('PostToolUseFailure', 1400, { tool_use_id: 'tool_1', error: secret });
  await invoke('SubagentStop', 1500, { agent_id: 'agent_1' });
  await invoke('SessionEnd', 2000);
  await invoke('SessionEnd', 3000);
  assert.deepEqual(output.map(event => event.kind), ['tool.call', 'subagent', 'session']);
  assert.equal(output[0].parentEventId, output[1].eventId);
  assert.equal(output[0].endTimeMs - output[0].startTimeMs, 100);
  assert.equal(output[0].error, true);
  assert.equal(output[0].attributes['gen_ai.tool.name'], 'custom');
  assert.equal(output[1].attributes['coding_agent.subagent.type'], 'custom');
  assert.equal(output[1].attributes['coding_agent.subagent.duration_ms'], 400);
  assert.equal(output[2].attributes['coding_agent.session.duration_ms'], 1000);
  assert.equal(JSON.stringify(output).includes(secret), false);
  assert.equal(JSON.stringify(output).includes('/private'), false);
  await invoke('SessionStart', 4000);
  await invoke('SessionEnd', 4500);
  assert.notEqual(output[3].eventId, output[2].eventId);
  assert.equal(output[3].attributes['coding_agent.session.duration_ms'], 500);
});

test('Claude reads only newly appended current-session records, deduplicates requests and waits for complete lines', async t => {
  const path = await sandbox(t), transcript = join(path, 'active.jsonl'), output = [];
  // Even an old record newer than the hook clock is deliberately excluded by the EOF seed.
  await writeFile(transcript, JSON.stringify(record('historical', 1500)) + '\n');
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot: join(path, 'state'), now: 1000, context: async () => ({}), telemetry: { enqueue: async event => { output.push(event); return true; }, flushLocal: async () => {} },
  });
  await invoke('SessionStart');
  const unrelated = record('foreign'); unrelated.sessionId = 'other_session';
  const line = JSON.stringify(record());
  await appendFile(transcript, JSON.stringify(unrelated) + '\n' + line + '\n' + line + '\n' + JSON.stringify(record('next')).slice(0, 50));
  await invoke('SessionStart'); // A repeated start must not reseed past these new records.
  await invoke('Stop');
  assert.deepEqual(output.map(event => event.eventId), ['message:msg_real']);
  await appendFile(transcript, JSON.stringify(record('next')).slice(50) + '\n');
  await invoke('Stop');
  await invoke('Stop');
  assert.deepEqual(output.map(event => event.eventId), ['message:msg_real', 'message:next']);
  const previous = process.env.CLAUDE_CODE_ENABLE_TELEMETRY;
  process.env.CLAUDE_CODE_ENABLE_TELEMETRY = '1';
  try {
    await appendFile(transcript, JSON.stringify(record('with_native_enabled')) + '\n');
    await invoke('Stop');
    assert.deepEqual(output.map(event => event.eventId), ['message:msg_real', 'message:next', 'message:with_native_enabled']);
  } finally {
    if (previous === undefined) delete process.env.CLAUDE_CODE_ENABLE_TELEMETRY;
    else process.env.CLAUDE_CODE_ENABLE_TELEMETRY = previous;
  }
  assert.equal(JSON.stringify(output).includes(secret), false);
});

for (const failure of ['false', 'throw']) {
  test(`Claude retries a mixed ${failure} batch across concurrent hooks without skipped or duplicate usage`, async t => {
    const path = await sandbox(t), transcript = join(path, 'active.jsonl'), accepted = [], attempts = [];
    await writeFile(transcript, '');
    let release, entered;
    const blocked = new Promise(resolve => { release = resolve; });
    const started = new Promise(resolve => { entered = resolve; });
    let fail = true;
    const localFailure = new Error('synthetic local persistence failure');
    const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
      stateRoot: join(path, 'state'), now: 1000, context: async () => ({}),
      telemetry: { enqueue: async event => {
        attempts.push(event.eventId);
        if (event.eventId === 'message:retry' && fail) {
          fail = false;
          entered();
          await blocked;
          if (failure === 'throw') throw localFailure;
          return false;
        }
        accepted.push(event);
        return true;
      }, flushLocal: async () => {} },
    });
    await invoke('SessionStart');
    await appendFile(transcript, [record('accepted'), record('retry')].map(row => JSON.stringify(row)).join('\n') + '\n');
    const first = invoke('Stop').then(() => undefined, error => error);
    await started;
    await appendFile(transcript, JSON.stringify(record('appended')) + '\n');
    const concurrent = invoke('Stop');
    release();
    const result = await first;
    if (failure === 'throw') assert.equal(result, localFailure);
    else assert.equal(result, undefined);
    await concurrent;
    await invoke('Stop');
    assert.deepEqual(new Set(accepted.map(event => event.eventId)), new Set(['message:accepted', 'message:retry', 'message:appended']));
    assert.deepEqual(attempts.filter(id => id === 'message:accepted'), ['message:accepted']);
    assert.deepEqual(attempts.filter(id => id === 'message:retry'), ['message:retry', 'message:retry']);
    assert.equal(accepted.reduce((total, event) => total + event.attributes['gen_ai.usage.output_tokens'], 0), 21);
  });
}

test('Claude honors legacy collector ACKs and creates queued receipts only after local acceptance', async t => {
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  const directory = join(stateRoot, hash('session_1')), attempts = [];
  let accepted = false, release, entered;
  const blocked = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { entered = resolve; });
  const telemetry = { enqueue: async event => {
    attempts.push(event.eventId);
    if (!accepted) { entered(); await blocked; }
    return accepted;
  }, flushLocal: async () => {} };
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry,
  });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  const legacy = join(directory, `sent-${hash('message:legacy')}.json`);
  const queued = join(directory, `queued-v2-${hash('message:new')}.json`);
  const cursor = join(directory, `cursor-${hash(transcript)}.json`);
  await writeFile(legacy, '{}');
  await appendFile(transcript, [record('legacy'), record('new')].map(row => JSON.stringify(row)).join('\n') + '\n');
  const first = invoke('Stop');
  try {
    await started;
    await assert.rejects(stat(queued), { code: 'ENOENT' });
  } finally { release(); await first; }
  assert.equal(JSON.parse(await readFile(cursor, 'utf8')).offset, 0);
  await assert.rejects(stat(queued), { code: 'ENOENT' });
  accepted = true;
  await invoke('Stop');
  await invoke('Stop');
  assert.deepEqual(attempts, ['message:new', 'message:new']);
  await stat(queued);
  assert.equal(await readFile(legacy, 'utf8'), '{}');
  await assert.rejects(stat(join(directory, `queued-v2-${hash('message:legacy')}.json`)), { code: 'ENOENT' });
  await assert.rejects(stat(join(directory, `sent-${hash('message:new')}.json`)), { code: 'ENOENT' });
  assert.equal(JSON.parse(await readFile(cursor, 'utf8')).offset, (await stat(transcript)).size);
});

test('Claude retries immutable first-completion snapshots without changing observed starts, errors or durations', async t => {
  const stateRoot = await sandbox(t), attempts = [], failures = new Set();
  const invoke = (hook_event_name, now, fields = {}) => handleHook({ session_id: 'session_1', hook_event_name, ...fields }, {
    stateRoot, now, context: async () => ({}),
    telemetry: { enqueue: async event => {
      attempts.push(structuredClone(event));
      if (!failures.has(event.eventId)) { failures.add(event.eventId); return false; }
      return true;
    }, flushLocal: async () => {} },
  });
  await invoke('SessionStart', 1000);
  await invoke('SessionStart', 1050);
  await invoke('SubagentStart', 1100, { agent_id: 'agent_1', agent_type: 'Explore' });
  await invoke('SubagentStart', 1150, { agent_id: 'agent_1', agent_type: 'Plan' });
  await invoke('PreToolUse', 1200, { tool_use_id: 'tool_1', tool_name: 'Read', agent_id: 'agent_1' });
  await invoke('PreToolUse', 1250, { tool_use_id: 'tool_1', tool_name: 'Write' });
  await invoke('PostToolUse', 1300, { tool_use_id: 'tool_1' });
  await invoke('PostToolUseFailure', 2000, { tool_use_id: 'tool_1' });
  await invoke('SubagentStop', 1500, { agent_id: 'agent_1' });
  await invoke('SubagentStop', 2500, { agent_id: 'agent_1' });
  await invoke('SessionEnd', 3000);
  await invoke('SessionEnd', 4000);
  for (const kind of ['tool.call', 'subagent', 'session']) {
    const pair = attempts.filter(event => event.kind === kind);
    assert.equal(pair.length, 2);
    assert.deepEqual(pair[1], pair[0]);
  }
  assert.equal(attempts[0].startTimeMs, 1200);
  assert.equal(attempts[0].endTimeMs, 1300);
  assert.equal(attempts[0].attributes.duration_ms, 100);
  assert.equal(attempts[0].attributes['gen_ai.tool.name'], 'Read');
  assert.equal(attempts[0].error, undefined);
  assert.equal(attempts[2].attributes['coding_agent.subagent.duration_ms'], 400);
  assert.equal(attempts[2].attributes['coding_agent.subagent.type'], 'Explore');
  assert.equal(attempts[4].attributes['coding_agent.session.duration_ms'], 2000);
});

test('Claude retains mixed false/throw batch receipts and its lock until every local enqueue settles', async t => {
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  const directory = join(stateRoot, hash('session_1')), attempts = new Map(), accepted = [];
  const failure = new Error('synthetic local persistence failure');
  let entered, release, settled = false;
  const started = new Promise(resolve => { entered = resolve; });
  const blocked = new Promise(resolve => { release = resolve; });
  const invoke = () => handleHook({ session_id: 'session_1', hook_event_name: 'Stop', transcript_path: transcript }, {
    stateRoot, now: 2000, context: async () => ({}),
    telemetry: { enqueue: async event => {
      const count = (attempts.get(event.eventId) || 0) + 1;
      attempts.set(event.eventId, count);
      if (count === 1 && event.eventId === 'message:throw') throw failure;
      if (count === 1 && event.eventId === 'message:false') return false;
      if (count === 1 && event.eventId === 'message:held') { entered(); await blocked; }
      accepted.push(event.eventId);
      return true;
    }, flushLocal: async () => {} },
  });
  await writeFile(transcript, '');
  await handleHook({ session_id: 'session_1', hook_event_name: 'SessionStart', transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}),
  });
  await appendFile(transcript, ['accepted', 'throw', 'false', 'held'].map(id => JSON.stringify(record(id))).join('\n') + '\n');
  const first = invoke().then(() => { settled = true; }, error => { settled = true; return error; });
  let result;
  try {
    await started;
    await invoke(); // Existing live-owner timeout must leave this batch entirely untouched.
    assert.equal(settled, false);
    assert.equal(attempts.get('message:held'), 1);
    await stat(join(directory, 'hook.lock'));
    assert.equal(JSON.parse(await readFile(join(directory, `cursor-${hash(transcript)}.json`), 'utf8')).offset, 0);
  } finally { release(); result = await first; }
  assert.equal(result, failure);
  await invoke();
  await invoke();
  assert.deepEqual(accepted.sort(), ['message:accepted', 'message:false', 'message:held', 'message:throw']);
  assert.equal(attempts.get('message:accepted'), 1);
  assert.equal(attempts.get('message:held'), 1);
  assert.equal(attempts.get('message:false'), 2);
  assert.equal(attempts.get('message:throw'), 2);
  assert.equal(JSON.parse(await readFile(join(directory, `cursor-${hash(transcript)}.json`), 'utf8')).offset, (await stat(transcript)).size);
});

test('Claude installer is dry-run by default, preserves settings, copies a self-contained native plugin and removes only owned files', async t => {
  const home = await sandbox(t), settings = join(home, '.claude', 'settings.json');
  await mkdir(join(home, '.claude'), { recursive: true });
  const original = '{\n  // user comment\n  "permissions": {"deny": ["Bash(rm *)"]},\n  "enabledPlugins": {"unrelated@marketplace": true}\n}\n';
  await writeFile(settings, original);
  const args = ['--home', home, '--config', join(home, 'private-otel.json')];
  const destination = join(home, '.claude', 'skills', 'doda-datalake-otel');
  await install(args);
  await assert.rejects(stat(destination), { code: 'ENOENT' });
  await install([...args, '--apply']);
  assert.equal(await readFile(settings, 'utf8'), original);
  assert.equal(JSON.parse(await readFile(join(destination, '.claude-plugin', 'plugin.json'), 'utf8')).name, 'doda-datalake-otel');
  const config = JSON.parse(await readFile(join(destination, 'claude-code', 'config-path.json'), 'utf8'));
  assert.equal(config.configPath, join(home, 'private-otel.json'));
  await writeFile(join(destination, 'personal.txt'), 'keep me');
  await mkdir(join(destination, 'outbox'), { mode: 0o700 });
  await writeFile(join(destination, 'outbox', 'accepted-state.json'), 'preserve local queue state');
  await install([...args, '--uninstall', '--apply']);
  assert.equal(await readFile(join(destination, 'personal.txt'), 'utf8'), 'keep me');
  assert.equal(await readFile(join(destination, 'outbox', 'accepted-state.json'), 'utf8'), 'preserve local queue state');
  assert.equal(await readFile(settings, 'utf8'), original);
  await assert.rejects(stat(join(destination, '.claude-plugin', 'plugin.json')), { code: 'ENOENT' });
  await assert.rejects(install([...args, '--apply']));
});

test('Copied Claude hooks isolate source imports and advance local checkpoints while a collector ACK is held', { timeout: 30_000 }, async t => {
  const home = await realpath(await mkdtemp(join(tmpdir(), 'datalake-claude-copy-')));
  const destination = join(home, '.claude', 'skills', 'doda-datalake-otel');
  const dataRoot = join(home, 'plugin-data'), base = join(dataRoot, 'outbox');
  const registry = join(home, 'worker-pids'), bin = join(home, 'bin'), loader = join(home, 'isolated-loader.mjs');
  const senderPath = join(destination, 'sender.mjs'), children = new Set(), workers = new Map(), requests = [];
  let held = true, entered;
  const firstRequest = new Promise(resolve => { entered = resolve; });
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push({ body: JSON.parse(body), response });
    entered();
    if (!held) response.end('{}');
  });
  async function running(pid, root) {
    try {
      const { stdout } = await run('ps', ['-p', String(pid), '-o', 'stat=', '-o', 'command='], { timeout: 5000, maxBuffer: 16384 });
      const match = /^\s*(\S+)\s+([\s\S]*)$/.exec(stdout);
      return Boolean(match && !match[1].startsWith('Z') && match[2].includes(senderPath) && match[2].includes(root));
    } catch (error) {
      if (error.code === 1 || error.code === 'ESRCH') return false;
      throw error;
    }
  }
  t.after(async () => {
    const errors = [];
    const attempt = async operation => {
      try { await operation(); } catch (error) { errors.push(error); }
    };
    for (const child of children) {
      await attempt(async () => {
        if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
        for (const stream of child.stdio) stream?.destroy();
      });
    }
    await attempt(() => until(() => [...children].every(child => child.exitCode !== null || child.signalCode !== null), 'isolated hook exits'));
    await attempt(async () => {
      try {
        for (const line of (await readFile(registry, 'utf8')).trim().split('\n')) {
          const [text, root] = line.split('\t'), pid = Number(text);
          if (Number.isSafeInteger(pid) && pid > 0 && pid !== process.pid && root.startsWith(`${base}/`)) workers.set(pid, root);
        }
      } catch (error) { if (error.code !== 'ENOENT') throw error; }
    });
    for (const [pid, root] of workers) {
      await attempt(async () => {
        if (await running(pid, root)) {
          try { process.kill(pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
        }
      });
    }
    server.closeAllConnections();
    await attempt(() => new Promise(resolve => server.close(resolve)));
    await attempt(() => until(async () => (await Promise.all([...workers].map(([pid, root]) => running(pid, root)))).every(value => !value), 'isolated sender exits'));
    await attempt(() => rm(home, { recursive: true, force: true }));
    if (errors.length) throw new AggregateError(errors, 'Isolated Claude fixture cleanup failed');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const config = join(home, 'private-otel.json'), transcript = join(home, 'active.jsonl');
  await writeFile(config, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}/v1/traces`, timeoutMs: 30_000 }), { mode: 0o600 });
  await writeFile(transcript, '');
  await mkdir(bin, { mode: 0o700 });
  // Both producer and detached sender reject module imports outside this isolated installation.
  await writeFile(loader, `export async function resolve(specifier, context, next) {
  const result = await next(specifier, context);
  if (result.url.startsWith('file:') && !result.url.startsWith(${JSON.stringify(pathToFileURL(`${home}/`).href)})) throw new Error('Outside isolated installation');
  return result;
}\n`);
  const quote = value => `'${value.replaceAll("'", "'\\''")}'`;
  await writeFile(join(bin, 'node'), `#!/bin/sh\numask 077\nprintf '%s\\t%s\\n' "$$" "$2" >> ${quote(registry)}\nexec ${quote(process.execPath)} --experimental-loader ${quote(loader)} "$@"\n`, { mode: 0o700 });
  await install(['--home', home, '--config', config, '--apply']);
  const env = { HOME: home, PATH: `${bin}:${process.env.PATH || ''}`, CLAUDE_CONFIG_DIR: join(home, '.claude'), CLAUDE_PLUGIN_DATA: dataRoot };
  async function invoke(hook_event_name) {
    const child = spawn(process.execPath, ['--experimental-loader', loader, join(destination, 'claude-code', 'hook.mjs')], {
      cwd: home, env, stdio: ['pipe', 'pipe', 'pipe'],
    });
    children.add(child);
    let diagnostics = '';
    child.stdout.on('data', chunk => { diagnostics += chunk; });
    child.stderr.on('data', chunk => { diagnostics += chunk; });
    const closed = once(child, 'close');
    const inputWritten = new Promise((resolve, reject) => {
      child.stdin.on('error', reject);
      child.stdin.end(JSON.stringify({ session_id: 'session_1', hook_event_name, transcript_path: transcript, prompt: secret }), resolve);
    });
    const [[code, signal]] = await Promise.all([closed, inputWritten]);
    assert.equal(code, 0);
    assert.equal(signal, null);
    assert.equal(diagnostics.includes(secret), false);
  }
  await invoke('SessionStart');
  await appendFile(transcript, JSON.stringify(record('first', Date.now())) + '\n');
  await invoke('Stop'); // Natural producer exit precedes collector ACK.
  await firstRequest;
  await appendFile(transcript, JSON.stringify(record('second', Date.now())) + '\n');
  await Promise.all([invoke('Stop'), invoke('Stop')]);
  const scope = (await readdir(base)).find(name => /^[a-f0-9]{64}$/.test(name));
  assert.ok(scope);
  const root = join(base, scope), outbox = await openOutbox({ root, route: await readRoute(root) });
  const pending = await outbox.status();
  assert.equal(pending.pending, 2);
  assert.equal(pending.done, 0);
  assert.equal(requests.length, 1);
  const directory = join(dataRoot, 'sessions', hash('session_1'));
  assert.equal(JSON.parse(await readFile(join(directory, `cursor-${hash(transcript)}.json`), 'utf8')).offset, (await stat(transcript)).size);
  await assert.rejects(stat(join(directory, 'hook.lock')), { code: 'ENOENT' });
  held = false;
  for (const entry of requests) entry.response.end('{}');
  await until(async () => (await outbox.status()).done === 2, 'copied sender durable delivery');
  await until(async () => {
    try { await stat(join(root, 'worker.lock')); return false; }
    catch (error) { if (error.code === 'ENOENT') return true; throw error; }
  }, 'copied sender empty release');
  const spans = requests.flatMap(entry => entry.body.resourceSpans.flatMap(resource => resource.scopeSpans.flatMap(scope => scope.spans)));
  assert.equal(spans.reduce((sum, span) => sum + Number(span.attributes.find(attribute => attribute.key === 'gen_ai.usage.output_tokens').value.intValue), 0), 14);
  assert.equal(JSON.stringify(requests.map(entry => entry.body)).includes(secret), false);
});
