import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, readFile, writeFile, appendFile, rm, stat, readdir, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { handleHook, usageEvent } from '../plugins/claude-code/hook.mjs';
import { install } from '../plugins/claude-code/install.mjs';
import { createDirectTelemetry } from '../plugins/otel.mjs';

const secret = 'PRIVATE_PROMPT_RESPONSE_TOOL_SECRET';
const hash = value => createHash('sha256').update(value).digest('hex');

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

const direct = () => { const output = []; return { output, telemetry: { send: async event => { output.push(event); return true; }, flush: async () => {} } }; };
// Direct-mode loopback: one HTTP request per send, no sender process, no queue.
async function wireFixture(t, respond = null, timeoutMs = 30_000) {
  const path = await sandbox(t);
  const bodies = [];
  const server = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    bodies.push(JSON.parse(body));
    const action = typeof respond === 'function' ? await respond(bodies.length, bodies[bodies.length - 1]) : respond;
    const { status = 200, payload = '{}' } = action ?? {};
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(payload);
  });
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const configPath = join(path, 'otel.json');
  await writeFile(configPath, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs }), { mode: 0o600 });
  const telemetry = createDirectTelemetry('claude-code', { configPath });
  return { path, bodies, configPath, telemetry };
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
  const path = await sandbox(t), { output, telemetry } = direct();
  const invoke = (hook_event_name, now, fields = {}) => handleHook({ session_id: 'session_1', hook_event_name, cwd: '/private/source', prompt: secret, tool_input: { command: secret }, tool_response: { content: secret }, last_assistant_message: secret, ...fields }, {
    stateRoot: path, now, context: async () => ({}), telemetry,
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
  const path = await sandbox(t), transcript = join(path, 'active.jsonl'), { output, telemetry } = direct();
  // Even an old record newer than the hook clock is deliberately excluded by the EOF seed.
  await writeFile(transcript, JSON.stringify(record('historical', 1500)) + '\n');
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot: join(path, 'state'), now: 1000, context: async () => ({}), telemetry,
  });
  await invoke('SessionStart');
  const unrelated = record('foreign'); unrelated.sessionId = 'other_session';
  const currentLine = JSON.stringify(record());
  await appendFile(transcript, JSON.stringify(unrelated) + '\n' + currentLine + '\n' + currentLine + '\n' + JSON.stringify(record('next')).slice(0, 50));
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

test('Claude failed sends are attempted once and checkpoints still advance without replay', async t => {
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  const attempts = [];
  let fail = true;
  const telemetry = { send: async event => {
    attempts.push(event.eventId);
    if (fail) { fail = false; return false; }
    return true;
  }, flush: async () => {} };
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry,
  });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  await appendFile(transcript, [record('accepted'), record('retry')].map(row => JSON.stringify(row)).join('\n') + '\n');
  await invoke('Stop');
  await invoke('Stop');
  await invoke('Stop');
  assert.deepEqual(attempts.filter(id => id === 'message:accepted').length, 1);
  assert.deepEqual(attempts.filter(id => id === 'message:retry').length, 1, 'failed send is attempted once, never replayed');
});

test('Claude bounds transcript send bursts and never replays failed attempts', async t => {
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  let active = 0, peak = 0;
  const attempts = [];
  const telemetry = { send: async event => {
    attempts.push(event.eventId);
    peak = Math.max(peak, ++active);
    try { await delay(15); return false; }
    finally { active--; }
  } };
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript },
    { stateRoot, now: 1000, context: async () => ({}), telemetry });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  attempts.length = 0;
  await appendFile(transcript, Array.from({ length: 13 }, (_, i) => JSON.stringify(record(`bounded_${i}`))).join('\n') + '\n');
  await invoke('Stop');
  assert.equal(attempts.length, 13);
  assert.equal(peak, 4);
  await invoke('Stop');
  assert.equal(attempts.length, 13);
  assert.equal(active, 0);
});

test('Claude honors legacy receipts as attempted and writes attempted markers only after direct sends', async t => {
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  const directory = join(stateRoot, hash('session_1')), attempts = [];
  const telemetry = { send: async event => { attempts.push(event.eventId); return true; }, flush: async () => {} };
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry,
  });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  const legacy = join(directory, `sent-${hash('message:legacy')}.json`);
  const queued = join(directory, `queued-v2-${hash('message:legacy-queued')}.json`);
  const cursor = join(directory, `cursor-${hash(transcript)}.json`);
  await writeFile(legacy, '{}');
  await writeFile(queued, '{}');
  await appendFile(transcript, [record('legacy'), record('legacy-queued'), record('new')].map(row => JSON.stringify(row)).join('\n') + '\n');
  await invoke('Stop');
  await invoke('Stop');
  assert.deepEqual(attempts, ['message:new', 'message:new'].slice(0, 1));
  assert.equal(await readFile(legacy, 'utf8'), '{}');
  assert.equal(await readFile(queued, 'utf8'), '{}');
  await assert.rejects(stat(join(directory, `queued-v2-${hash('message:new')}.json`)), { code: 'ENOENT' });
  await assert.rejects(stat(join(directory, `sent-${hash('message:new')}.json`)), { code: 'ENOENT' });
  await stat(join(directory, `attempted-${hash('message:new')}.json`));
  assert.equal(JSON.parse(await readFile(cursor, 'utf8')).offset, (await stat(transcript)).size);
});

test('Claude first-completion snapshots stay immutable and duplicates never resend', async t => {
  const stateRoot = await sandbox(t), attempts = [];
  const invoke = (hook_event_name, now, fields = {}) => handleHook({ session_id: 'session_1', hook_event_name, ...fields }, {
    stateRoot, now, context: async () => ({}),
    telemetry: { send: async event => { attempts.push(structuredClone(event)); return true; }, flush: async () => {} },
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
    assert.equal(pair.length, 1, `${kind} is attempted once`);
  }
  assert.equal(attempts[0].startTimeMs, 1200);
  assert.equal(attempts[0].endTimeMs, 1300);
  assert.equal(attempts[0].attributes.duration_ms, 100);
  assert.equal(attempts[0].attributes['gen_ai.tool.name'], 'Read');
  assert.equal(attempts[0].error, undefined);
  assert.equal(attempts[1].attributes['coding_agent.subagent.duration_ms'], 400);
  assert.equal(attempts[1].attributes['coding_agent.subagent.type'], 'Explore');
  assert.equal(attempts[2].attributes['coding_agent.session.duration_ms'], 2000);
});

test('Claude loopback HTTP rejection and partial success attempt once without replay', async t => {
  const f = await wireFixture(t, { status: 500, payload: '{}' });
  const stateRoot = join(f.path, 'state'), transcript = join(f.path, 'active.jsonl');
  await writeFile(transcript, '');
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry: f.telemetry,
  });
  await invoke('SessionStart');
  await appendFile(transcript, JSON.stringify(record('rejected')) + '\n');
  await invoke('Stop');
  await invoke('Stop');
  await f.telemetry.flush();
  await until(() => f.bodies.length >= 1, 'direct HTTP rejection attempt');
  await delay(100);
  assert.equal(f.bodies.length, 1);
  const g = await wireFixture(t, { status: 200, payload: '{"partialSuccess":{"rejectedSpans":"1"}}' });
  const gTranscript = join(g.path, 'active.jsonl');
  await writeFile(gTranscript, '');
  const gInvoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: gTranscript }, {
    stateRoot: join(g.path, 'state'), now: 1000, context: async () => ({}), telemetry: g.telemetry,
  });
  await gInvoke('SessionStart');
  await appendFile(gTranscript, JSON.stringify(record('partial')) + '\n');
  await gInvoke('Stop');
  await gInvoke('Stop');
  await g.telemetry.flush();
  await until(() => g.bodies.length >= 1, 'direct HTTP partial-success attempt');
  await delay(100);
  assert.equal(g.bodies.length, 1);
});

test('Claude loopback timeout bounds the hook and duplicate hooks do not resend', async t => {
  const path = await sandbox(t);
  let requests = 0;
  const server = createServer(async request => {
    for await (const chunk of request) { /* Drain the real request; deliberately withhold ACK. */ }
    requests++;
  });
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const configPath = join(path, 'otel.json');
  await writeFile(configPath, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 300 }), { mode: 0o600 });
  const telemetry = createDirectTelemetry('claude-code', { configPath });
  const stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  await writeFile(transcript, '');
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry,
  });
  await invoke('SessionStart');
  await appendFile(transcript, JSON.stringify(record('slow')) + '\n');
  const started = Date.now();
  await invoke('Stop');
  const elapsed = Date.now() - started;
  assert.ok(elapsed >= 250 && elapsed < 10_000, `elapsed=${elapsed}`);
  assert.equal(requests, 1);
  await telemetry.flush().catch(() => {});
  await invoke('Stop');
  assert.equal(requests, 1, 'Duplicate hooks do not retry the timed-out event');
});

test('Claude shares a sub-five-second send budget across slow waves despite a 30s exporter timeout', async t => {
  const path = await sandbox(t);
  let requests = 0, active = 0, peak = 0;
  const server = createServer(async request => {
    for await (const chunk of request) { /* Drain without acknowledging. */ }
    requests++;
  });
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const configPath = join(path, 'otel.json');
  await writeFile(configPath, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 30_000 }), { mode: 0o600 });
  const exporter = createDirectTelemetry('claude-code', { configPath });
  const telemetry = {
    send: async (event, options) => {
      peak = Math.max(peak, ++active);
      try { return await exporter.send(event, options); }
      finally { active--; }
    },
    flush: () => exporter.flush(),
  };
  const stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript },
    { stateRoot, now: 1000, context: async () => ({}), telemetry });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  await appendFile(transcript, Array.from({ length: 13 }, (_, i) => JSON.stringify(record(`deadline_${i}`))).join('\n') + '\n');
  const started = Date.now();
  await invoke('Stop');
  await telemetry.flush();
  assert.ok(Date.now() - started < 4500, 'all waves and receipts finish before the installed 5s hook deadline');
  assert.equal(requests, 13);
  assert.ok(peak <= 4);
  const directory = join(stateRoot, hash('session_1'));
  for (let i = 0; i < 13; i++) {
    assert.deepEqual(JSON.parse(await readFile(join(directory, `attempted-${hash(`message:deadline_${i}`)}.json`), 'utf8')), {});
  }
  await invoke('Stop');
  assert.equal(requests, 13, 'timed-out attempts must never replay');
});

test('Claude leaves deferred events eligible without advancing a partial transcript checkpoint', async t => {
  let clock = 0, exhaust = false;
  t.mock.method(performance, 'now', () => clock);
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl');
  const attempts = [];
  const telemetry = { send: async event => {
    attempts.push(event.eventId);
    if (exhaust) clock = 4000;
    return false;
  } };
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript },
    { stateRoot, now: 1000, context: async () => ({}), telemetry });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  await appendFile(transcript, Array.from({ length: 9 }, (_, i) => JSON.stringify(record(`deferred_${i}`))).join('\n') + '\n');
  exhaust = true;
  await invoke('Stop');
  const partial = attempts.length;
  assert.ok(partial > 0 && partial < 9);
  exhaust = false;
  clock = 0;
  await invoke('Stop');
  assert.equal(attempts.length, 9);
  assert.equal(new Set(attempts).size, 9, 'only unattempted work remains eligible');
  await invoke('Stop');
  assert.equal(attempts.length, 9);
});

test('Claude direct mode leaves legacy runtime queue data byte-unchanged', async t => {
  const f = await wireFixture(t);
  const legacy = join(f.path, 'legacy-route');
  await mkdir(legacy, { recursive: true });
  const obstacle = join(legacy, 'queue.sqlite');
  await writeFile(obstacle, Buffer.from([0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x00]));
  const before = await readFile(obstacle);
  const stateRoot = join(f.path, 'state');
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry: f.telemetry,
  });
  await invoke('SessionStart');
  await f.telemetry.flush();
  assert.deepEqual(await readFile(obstacle), before);
  assert.deepEqual(await readdir(legacy), ['queue.sqlite']);
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
  await writeFile(join(destination, 'outbox.mjs'), '// obsolete managed copy');
  await writeFile(join(destination, 'sender.mjs'), '// obsolete managed copy');
  await mkdir(join(destination, 'sessions'), { mode: 0o700 });
  await writeFile(join(destination, 'sessions', 'accepted-state.json'), 'preserve local session state');
  await install([...args, '--apply']);
  await assert.rejects(stat(join(destination, 'outbox.mjs')), { code: 'ENOENT' });
  await assert.rejects(stat(join(destination, 'sender.mjs')), { code: 'ENOENT' });
  assert.equal(await readFile(join(destination, 'sessions', 'accepted-state.json'), 'utf8'), 'preserve local session state');
  await install([...args, '--uninstall', '--apply']);
  assert.equal(await readFile(join(destination, 'personal.txt'), 'utf8'), 'keep me');
  assert.equal(await readFile(join(destination, 'sessions', 'accepted-state.json'), 'utf8'), 'preserve local session state');
  assert.equal(await readFile(settings, 'utf8'), original);
  await assert.rejects(stat(join(destination, '.claude-plugin', 'plugin.json')), { code: 'ENOENT' });
  await assert.rejects(stat(join(destination, 'outbox.mjs')), { code: 'ENOENT' });
  await assert.rejects(install([...args, '--apply']));
});

test('Copied Claude hooks send direct HTTP without a bundled queue and exit bounded', { timeout: 30_000 }, async t => {
  const home = await realpath(await mkdtemp(join(tmpdir(), 'datalake-claude-copy-')));
  const destination = join(home, '.claude', 'skills', 'doda-datalake-otel');
  const dataRoot = join(home, 'plugin-data');
  const children = new Set(), requests = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push(JSON.parse(body));
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end('{}');
  });
  t.after(async () => {
    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      for (const stream of child.stdio) stream?.destroy();
    }
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(home, { recursive: true, force: true });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const config = join(home, 'private-otel.json'), transcript = join(home, 'active.jsonl');
  await writeFile(config, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}/v1/traces`, timeoutMs: 30_000 }), { mode: 0o600 });
  await writeFile(transcript, '');
  await install(['--home', home, '--config', config, '--apply']);
  // No bundled queue modules ship with the direct-mode installation.
  await assert.rejects(stat(join(destination, 'outbox.mjs')), { code: 'ENOENT' });
  await assert.rejects(stat(join(destination, 'sender.mjs')), { code: 'ENOENT' });
  const env = { HOME: home, PATH: process.env.PATH || '', CLAUDE_CONFIG_DIR: join(home, '.claude'), CLAUDE_PLUGIN_DATA: dataRoot };
  async function invoke(hook_event_name) {
    const child = spawn(process.execPath, [join(destination, 'claude-code', 'hook.mjs')], {
      cwd: home, env, stdio: ['pipe', 'pipe', 'pipe'],
    });
    children.add(child);
    let diagnostics = '';
    child.stdout.on('data', chunk => { diagnostics += chunk; });
    child.stderr.on('data', chunk => { diagnostics += chunk; });
    const closed = once(child, 'close');
    const inputWritten = new Promise((resolvePromise, reject) => {
      child.stdin.on('error', reject);
      child.stdin.end(JSON.stringify({ session_id: 'session_1', hook_event_name, transcript_path: transcript, prompt: secret }), resolvePromise);
    });
    const [[code, signal]] = await Promise.all([closed, inputWritten]);
    assert.equal(code, 0);
    assert.equal(signal, null);
    assert.equal(diagnostics.includes(secret), false);
  }
  await invoke('SessionStart');
  await appendFile(transcript, JSON.stringify(record('first', Date.now())) + '\n');
  await invoke('Stop'); // Hook exits after its direct sends settle; no detached sender.
  await invoke('Stop'); // Duplicate completion does not resend.
  await until(() => requests.length >= 1, 'direct hook HTTP delivery');
  await delay(200);
  assert.equal(requests.length, 1);
  const spans = requests.flatMap(entry => entry.resourceSpans.flatMap(resource => resource.scopeSpans.flatMap(scope => scope.spans)));
  assert.equal(spans.reduce((sum, span) => sum + Number(span.attributes.find(attribute => attribute.key === 'gen_ai.usage.output_tokens').value.intValue), 0), 7);
  assert.equal(JSON.stringify(requests.map(entry => entry)).includes(secret), false);
  await install(['--home', home, '--config', config, '--source', 'native', '--apply']);
  await appendFile(transcript, JSON.stringify(record('native_owned', Date.now())) + '\n');
  await invoke('Stop'); // Child has no OTEL environment; the private pointer owns selection.
  assert.equal(requests.length, 1);
});

test('Claude native ownership suppresses transcript usage but retains tools and deferred display observations', async t => {
  const stateRoot = await sandbox(t), transcript = join(stateRoot, 'current.jsonl');
  await writeFile(transcript, '');
  const { output, telemetry } = direct();
  const run = (hook_event_name, now, extra = {}) => handleHook({
    session_id: 'session_1', hook_event_name, transcript_path: transcript, ...extra,
  }, { telemetry, stateRoot, now, source: 'native', context: async () => ({}) });
  await run('SessionStart', 1000);
  await appendFile(transcript, JSON.stringify(record()) + '\n');
  await run('UserPromptSubmit', 1100, { prompt_id: 'prompt_1', prompt: secret });
  await run('MessageDisplay', 1300, { prompt_id: 'prompt_1', turn_id: 'turn_1', message_id: 'message_1', index: 0, final: false, delta: secret });
  assert.equal(output.length, 0);
  await run('PreToolUse', 1400, { tool_use_id: 'tool_1', tool_name: 'Read' });
  await run('PostToolUse', 1500, { tool_use_id: 'tool_1', tool_name: 'Read' });
  await run('Stop', 1600);
  assert.deepEqual(output.map(event => event.kind).sort(), ['interaction', 'tool.call']);
  const interaction = output.find(event => event.kind === 'interaction');
  assert.equal(interaction.attributes['coding_agent.interaction.first_response_ms'], 200);
  assert.equal(interaction.attributes['coding_agent.interaction.latency.kind'], 'hook_submit_to_display_ready');
  assert.equal(Object.keys(interaction.attributes).some(key => key.includes('usage')), false);
  const observationDir = join(stateRoot, hash('session_1'), 'display');
  for (const file of await readdir(observationDir)) assert.equal((await readFile(join(observationDir, file), 'utf8')).includes(secret), false);
});

test('Claude omits ambiguous display batches and missing associations without inferring human origin', async t => {
  for (const variant of ['final', 'empty', 'missing-prompt', 'mismatch', 'overlap', 'late-index']) {
    const stateRoot = await sandbox(t), { output, telemetry } = direct();
    const run = (hook_event_name, now, extra = {}) => handleHook({ session_id: 'session_1', hook_event_name, ...extra },
      { telemetry, stateRoot, now, context: async () => ({}) });
    await run('SessionStart', 1000);
    await run('UserPromptSubmit', 1100, variant === 'missing-prompt' ? {} : { prompt_id: 'prompt_1' });
    if (variant === 'overlap') await run('UserPromptSubmit', 1150, { prompt_id: 'prompt_2' });
    await run('MessageDisplay', 1200, { prompt_id: variant === 'mismatch' ? 'other' : 'prompt_1',
      turn_id: 'turn_1', message_id: 'message_1', index: variant === 'late-index' ? 1 : 0,
      final: variant === 'final', delta: variant === 'empty' ? '' : secret });
    await run('Stop', 1300);
    assert.equal(output.length, 0, variant);
  }
});

test('Claude display checkpoint never waits for an in-flight telemetry ACK', async t => {
  const stateRoot = await sandbox(t);
  let release, started;
  const sending = new Promise(resolve => { started = resolve; });
  const ack = new Promise(resolve => { release = resolve; });
  const telemetry = { send: async () => { started(); await ack; return true; } };
  const run = (hook_event_name, now, extra = {}) => handleHook({ session_id: 'session_1', hook_event_name, ...extra },
    { telemetry, stateRoot, now, context: async () => ({}) });
  await run('SessionStart', 1000);
  await run('PreToolUse', 1050, { tool_use_id: 'tool_1' });
  const completion = run('PostToolUse', 1100, { tool_use_id: 'tool_1' });
  await sending;
  try {
    await run('UserPromptSubmit', 1200, { prompt_id: 'prompt_1' });
    await run('MessageDisplay', 1300, { prompt_id: 'prompt_1', turn_id: 'turn_1', message_id: 'message_1', index: 0, final: false, delta: secret });
    assert.equal(JSON.parse(await readFile(join(stateRoot, hash('session_1'), 'display', 'ready.json'), 'utf8')).endTimeMs, 1300);
  } finally { release(); await completion; }
});

test('Claude installer persists explicit ownership across upgrades without changing settings', async t => {
  const home = await sandbox(t), settings = join(home, '.claude', 'settings.json');
  await mkdir(join(home, '.claude'));
  const original = JSON.stringify({ hooks: { Stop: [{ hooks: [{ type: 'command', command: 'personal-hook' }] }] } });
  await writeFile(settings, original);
  const args = ['--home', home, '--apply'];
  const pointer = join(home, '.claude', 'skills', 'doda-datalake-otel', 'claude-code', 'config-path.json');
  await install([...args, '--source', 'native']);
  await install(args);
  assert.equal(JSON.parse(await readFile(pointer, 'utf8')).source, 'native');
  assert.equal((await stat(pointer)).mode & 0o777, 0o600);
  await install([...args, '--source', 'plugin']);
  assert.equal(JSON.parse(await readFile(pointer, 'utf8')).source, 'plugin');
  assert.equal(await readFile(settings, 'utf8'), original);
  await assert.rejects(install([...args, '--source', 'invalid']));
});
