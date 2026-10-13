import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, writeFile, rm, stat, mkdir, symlink } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { handleHook } from '../plugins/agy/hook.mjs';
import { createDirectTelemetry } from '../plugins/otel.mjs';
import { installAgy } from '../plugins/agy/install.mjs';

async function until(predicate, description) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await delay(20);
  }
  throw new Error(`Timed out: ${description}`);
}

async function sandbox(t, steps = []) {
  const path = await mkdtemp(join(tmpdir(), 'datalake-agy-'));
  const sessionId = `agy-${randomUUID()}`;
  const state = join(tmpdir(), 'doda-datalake-agy', sessionId);
  const transcript = join(path, 'transcript.jsonl');
  await writeFile(transcript, steps.map(step => JSON.stringify(step)).join('\n') + '\n');
  t.after(async () => { await rm(path, { recursive: true, force: true }); await rm(state, { recursive: true, force: true }); });
  return { path, state, sessionId, transcript };
}

const steps = () => [
  { type: 'USER_INPUT', step_index: 0, created_at: new Date(1000).toISOString(), content: 'synthetic request' },
  { type: 'PLANNER_RESPONSE', step_index: 1, created_at: new Date(1100).toISOString(), content: 'synthetic response', tool_calls: [{ name: 'invoke_subagent', args: { Subagents: [{ Role: 'Explore' }, { Role: 'Plan' }] } }] },
  { type: 'PLANNER_RESPONSE', step_index: 2, created_at: new Date(1200).toISOString(), content: 'synthetic follow-up' },
];

const direct = () => { const accepted = []; return { accepted, telemetry: { send: async event => { accepted.push(structuredClone(event)); return true; }, flush: async () => {} } }; };
// Direct-mode loopback: one HTTP request per send, no sender process, no queue.
async function wireFixture(t, respond = null, timeoutMs = 30_000) {
  const bodies = [];
  const server = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    bodies.push(JSON.parse(body));
    const action = typeof respond === 'function' ? respond(bodies.length, bodies[bodies.length - 1]) : respond;
    const { status = 200, payload = '{}' } = action ?? {};
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(payload);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  return { bodies, endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs };
}

for (const failure of ['false', 'throw']) {
  test(`AGY failed ${failure} on turn:1 still sends turn:2 once, never retries turn:1`, async t => {
    const box = await sandbox(t, steps()), acknowledged = [], attempts = [];
    const child0 = `subagent:${box.sessionId}:1:0`, child1 = `subagent:${box.sessionId}:1:1`;
    let fail = true;
    const telemetry = {
      send: async event => {
        attempts.push(event.eventId);
        if (event.eventId === 'turn:1' && fail) {
          fail = false;
          if (failure === 'throw') throw new Error('synthetic transport failure');
          return false;
        }
        acknowledged.push(event);
        return true;
      },
      flush: async () => {},
    };
    const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
    await invoke();
    assert.deepEqual(attempts.filter(id => id === 'turn:1'), ['turn:1'], 'failed turn attempted once, then dropped');
    assert.deepEqual(attempts.filter(id => id === 'turn:2'), ['turn:2'], 'later independent turn still sent');
    assert.deepEqual(acknowledged.filter(event => event.kind === 'llm.turn').map(event => event.eventId), ['turn:2']);
    assert.deepEqual(acknowledged.filter(event => event.kind === 'subagent').map(event => event.eventId), [child0, child1]);
    const cursor = JSON.parse(await readFile(join(box.state, 'cursor.json'), 'utf8'));
    assert.equal(cursor.version, 3);
    assert.equal(cursor.lastProcessedIndex, 2, 'attempted-identity checkpoint advances regardless of ACK');
    assert.ok(cursor.attemptedEventIds?.includes('turn:1'));
    // A repeated hook with changed transcript content never retries the attempted turn.
    const changed = steps();
    changed[1].content = 'different later transcript observation';
    changed[1].tool_calls[0].args.Subagents[1].Role = 'LaterRole';
    await writeFile(box.transcript, changed.map(step => JSON.stringify(step)).join('\n') + '\n');
    await invoke();
    await invoke();
    assert.deepEqual(attempts.filter(id => id === 'turn:1'), ['turn:1']);
    assert.deepEqual(attempts.filter(id => id === 'turn:2'), ['turn:2']);
    assert.ok(!attempts.includes(`${child1}-retry`), 'no synthetic replay identities appear');
    void child0;
  });

  test(`AGY preserves the observed tool completion and duration through ${failure} without replay`, async t => {
    const box = await sandbox(t), attempted = [];
    let fail = true;
    const telemetry = {
      send: async event => {
        attempted.push(structuredClone(event));
        if (fail) {
          fail = false;
          if (failure === 'throw') throw new Error('synthetic transport failure');
          return false;
        }
        return true;
      }, flush: async () => {},
    };
    const payload = { conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 7, toolCall: { name: 'Bash' } };
    await handleHook(payload, { telemetry, cwd: box.path, now: () => 1000, isPreTool: true });
    await handleHook({ ...payload, error: 'SYNTHETIC_PRIVATE_ERROR_MARKER' }, { telemetry, cwd: box.path, now: () => 1100 });
    assert.equal(attempted[0].attributes.duration_ms, 100);
    assert.equal(attempted[0].endTimeMs, 1100);
    const readPayload = { ...payload, toolCall: { name: 'Read' } };
    await handleHook(readPayload, { telemetry, cwd: box.path, now: () => 1200, isPreTool: true });
    await handleHook(readPayload, { telemetry, cwd: box.path, now: () => 1400 });
    const retried = await handleHook(payload, { telemetry, cwd: box.path, now: () => 2000 });
    assert.deepEqual(retried, [], 'attempted tool completion is never replayed');
    await handleHook({ ...readPayload, error: 'later changed completion' }, { telemetry, cwd: box.path, now: () => 2500 });
    await handleHook(payload, { telemetry, cwd: box.path, now: () => 3000 });
    assert.deepEqual(attempted.map(event => event.eventId), ['tool:7:Bash', 'tool:7:Read']);
    assert.equal(attempted[1].attributes.duration_ms, 200);
  });
}

for (const stepIdx of [7, undefined, '../escaped', 'nested/../../escaped']) {
  test(`AGY isolates tool names sharing step ${stepIdx ?? 'omitted'} and retains their first completion`, async t => {
    const box = await sandbox(t), { accepted, telemetry } = direct();
    const payload = { conversationId: box.sessionId, transcriptPath: box.transcript, ...(stepIdx === undefined ? {} : { stepIdx }) };
    for (const [toolName, start, end] of [['Bash', 1000, 1100], ['Read', 1200, 1400]]) {
      await handleHook({ ...payload, toolCall: { name: toolName } }, { telemetry, cwd: box.path, now: () => start, isPreTool: true });
      await handleHook({ ...payload, toolName }, { telemetry, cwd: box.path, now: () => end });
    }
    const originals = structuredClone(accepted);
    for (const toolName of ['Bash', 'Read']) {
      await handleHook({ ...payload, toolName }, { telemetry, cwd: box.path, now: () => 2000, isPreTool: true });
      await handleHook({ ...payload, toolCall: { name: toolName }, error: 'changed later completion' }, { telemetry, cwd: box.path, now: () => 3000 });
    }
    assert.deepEqual(accepted, originals);
    assert.deepEqual(accepted.map(event => event.eventId), [`tool:${stepIdx ?? 0}:Bash`, `tool:${stepIdx ?? 0}:Read`]);
    assert.deepEqual(accepted.map(event => event.attributes.duration_ms), [100, 200]);
    assert.deepEqual(accepted.map(event => [event.startTimeMs, event.endTimeMs, event.error]), [[1000, 1100, false], [1200, 1400, false]]);
  });
}

for (const legacy of ['start', 'pending', 'queued', 'mismatched-event']) {
  test(`AGY reads only matching ${legacy} step-only state without losing another tool`, async t => {
    const box = await sandbox(t), { accepted, telemetry } = direct();
    const firstBody = {
      kind: 'tool.call', sessionId: box.sessionId, eventId: 'tool:7:Bash',
      startTimeMs: 1000, endTimeMs: 1100, error: true,
      attributes: { 'gen_ai.tool.name': 'Bash', duration_ms: 100, 'error.type': 'tool_error' },
    };
    const state = legacy === 'start' ? { startTimeMs: 1000, toolName: 'Bash' } : {
      version: legacy === 'queued' ? 2 : 3, startTimeMs: 1000, toolName: 'Bash',
      event: legacy === 'mismatched-event' ? { ...firstBody, eventId: 'tool:7:Read' } : firstBody,
      ...(legacy === 'queued' ? { queued: true } : { attempted: legacy !== 'pending' ? true : false }),
    };
    await mkdir(box.state, { recursive: true, mode: 0o700 });
    await writeFile(join(box.state, 'tool-7.json'), JSON.stringify(state), { mode: 0o600 });
    const payload = { conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 7 };
    await handleHook({ ...payload, toolName: 'Read' }, { telemetry, cwd: box.path, now: () => 1200, isPreTool: true });
    await handleHook({ ...payload, toolName: 'Read' }, { telemetry, cwd: box.path, now: () => 1400 });
    await handleHook({ ...payload, toolName: 'Bash' }, { telemetry, cwd: box.path, now: () => 1050, isPreTool: true });
    await handleHook({ ...payload, toolName: 'Bash' }, { telemetry, cwd: box.path, now: () => 1100 });
    await handleHook({ ...payload, toolName: 'Bash', error: 'changed later completion' }, { telemetry, cwd: box.path, now: () => 3000 });
    assert.equal(accepted[0].eventId, 'tool:7:Read');
    assert.equal(accepted[0].attributes.duration_ms, 200);
    const bashEvents = accepted.filter(event => event.eventId === 'tool:7:Bash');
    // Legacy queued:true and previously attempted bodies are never replayed.
    assert.equal(bashEvents.length, legacy === 'queued' ? 0 : 1);
    if (legacy === 'pending') assert.deepEqual(bashEvents[0], firstBody);
    if (legacy === 'start') assert.equal(bashEvents[0].attributes.duration_ms, 100);
    if (legacy === 'mismatched-event') {
      // A recorded completion for another tool is not this tool's state: fresh observation wins.
      assert.equal(bashEvents[0].attributes.duration_ms, 50);
      assert.equal(bashEvents[0].error, false);
    }
  });
}
test('AGY retains stable tool identity even when its name contains path components', async t => {
  const box = await sandbox(t), { accepted, telemetry } = direct();
  const payload = { conversationId: box.sessionId, transcriptPath: box.transcript, toolName: '../../synthetic/Read' };
  await handleHook(payload, { telemetry, cwd: box.path, now: () => 1000, isPreTool: true });
  await handleHook(payload, { telemetry, cwd: box.path, now: () => 1200 });
  await handleHook({ ...payload, error: 'changed later completion' }, { telemetry, cwd: box.path, now: () => 3000 });
  assert.equal(accepted.length, 1);
  assert.equal(accepted[0].eventId, 'tool:0:../../synthetic/Read');
  assert.equal(accepted[0].attributes.duration_ms, 200);
  assert.equal(accepted[0].error, false);
});

test('AGY serializes concurrent transcript drains without duplicate usage', async t => {
  const box = await sandbox(t, steps()), acknowledged = [];
  let entered, release, first = true;
  const started = new Promise(resolve => { entered = resolve; });
  const blocked = new Promise(resolve => { release = resolve; });
  const telemetry = {
    send: async event => {
      if (event.kind === 'llm.turn' && first) { first = false; entered(); await blocked; }
      acknowledged.push(event);
      return true;
    }, flush: async () => {},
  };
  const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
  const one = invoke();
  await started;
  const two = invoke();
  await delay(20);
  release();
  await Promise.all([one, two]);
  assert.deepEqual(acknowledged.filter(event => event.kind === 'llm.turn').map(event => event.eventId), ['turn:1', 'turn:2']);
});

test('AGY tool errors export only a fixed category on the direct OTLP wire', async t => {
  const box = await sandbox(t), bodies = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    bodies.push(body);
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end('{}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  const telemetry = createDirectTelemetry('agy', { endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 30_000 });
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 1, toolName: 'Bash', error: 'SYNTHETIC_PRIVATE_ERROR_MARKER credential=fixture' }, { telemetry, cwd: box.path, now: () => 1100 });
  await telemetry.flush();
  await until(() => bodies.length === 1, 'actual HTTP delivery');
  await delay(100);
  assert.equal(bodies.length, 1, 'one direct HTTP request, no queue redelivery');
  const spans = bodies.flatMap(body => JSON.parse(body).resourceSpans.flatMap(resource => resource.scopeSpans.flatMap(scope => scope.spans)));
  assert.deepEqual(spans.map(span => span.status.code), [2]);
  assert.deepEqual(spans[0].attributes.find(attribute => attribute.key === 'error.type').value, { stringValue: 'tool_error' });
  assert.doesNotMatch(bodies.join(''), /SYNTHETIC_PRIVATE_ERROR_MARKER|credential=fixture/);
  // A repeated hook never replays the attempted tool event over HTTP.
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 1, toolName: 'Bash' }, { telemetry, cwd: box.path, now: () => 2000 });
  await telemetry.flush();
  await delay(100);
  assert.equal(bodies.length, 1);
});

test('AGY treats legacy sent IDs as attempted without replaying historical turns', async t => {
  const box = await sandbox(t, steps()), accepted = [];
  await mkdir(box.state, { recursive: true, mode: 0o700 });
  const legacy = ['turn:1', `subagent:${box.sessionId}:1:0`];
  await writeFile(join(box.state, 'cursor.json'), JSON.stringify({ lastProcessedIndex: -1, sentEventIds: legacy }), { mode: 0o600 });
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, {
    telemetry: { send: async event => { accepted.push(event); return true; }, flush: async () => {} },
    cwd: box.path, now: () => 1500,
  });
  assert.deepEqual(accepted.filter(event => event.kind === 'llm.turn').map(event => event.eventId), ['turn:2']);
  assert.deepEqual(accepted.filter(event => event.kind === 'subagent').map(event => event.eventId), [`subagent:${box.sessionId}:1:1`]);
  const cursor = JSON.parse(await readFile(join(box.state, 'cursor.json'), 'utf8'));
  assert.equal(cursor.version, 3);
  assert.deepEqual(cursor.sentEventIds, legacy);
  assert.equal(cursor.lastProcessedIndex, 2);
});

for (const failure of ['false', 'throw']) {
  test(`AGY Stop keeps its first body and timestamp through ${failure} without replay`, async t => {
    const box = await sandbox(t, steps()), attempts = [];
    let fail = true, clock = 1500;
    const telemetry = {
      send: async event => {
        if (event.kind !== 'session') return true;
        attempts.push(event);
        if (fail) { fail = false; if (failure === 'throw') throw new Error('synthetic transport failure'); return false; }
        return true;
      }, flush: async () => {},
    };
    const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => clock });
    await invoke();
    await writeFile(box.transcript, '');
    clock = 9000;
    await invoke();
    await invoke();
    assert.equal(attempts.length, 1, 'failed stop is attempted once, never replayed');
    assert.equal(attempts[0].endTimeMs, 1500);
    assert.equal(attempts[0].attributes['coding_agent.session.duration_ms'], 500);
    assert.equal(JSON.parse(await readFile(join(box.state, 'stop.json'), 'utf8')).attempted, true);
  });
}

test('AGY loopback HTTP rejection and partial success attempt once without replay', async t => {
  const rejected = await wireFixture(t, { status: 500, payload: '{}' });
  const box = await sandbox(t, steps());
  const telemetry = createDirectTelemetry('agy', { endpoint: rejected.endpoint, timeoutMs: 30_000 });
  const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
  await invoke();
  await invoke();
  await telemetry.flush();
  await until(() => rejected.bodies.length >= 1, 'direct HTTP rejection attempt');
  await delay(100);
  const firstCount = rejected.bodies.length;
  await invoke();
  await telemetry.flush();
  await delay(100);
  assert.equal(rejected.bodies.length, firstCount, 'no automatic replay after rejection');

  const partial = await wireFixture(t, { status: 200, payload: '{"partialSuccess":{"rejectedSpans":"1"}}' });
  const box2 = await sandbox(t, steps());
  const telemetry2 = createDirectTelemetry('agy', { endpoint: partial.endpoint, timeoutMs: 30_000 });
  const invoke2 = () => handleHook({ conversationId: box2.sessionId, transcriptPath: box2.transcript, terminationReason: 'model_stop' }, { telemetry: telemetry2, cwd: box2.path, now: () => 1500 });
  await invoke2();
  await invoke2();
  await telemetry2.flush();
  await until(() => partial.bodies.length >= 1, 'direct HTTP partial-success attempt');
  await delay(100);
  assert.equal(partial.bodies.length >= 1, true);
  const after = partial.bodies.length;
  await invoke2();
  await telemetry2.flush();
  await delay(100);
  assert.equal(partial.bodies.length, after, 'no automatic replay after partial success');
});

test('AGY loopback timeout bounds the hook and duplicate hooks do not resend', async t => {
  const box = await sandbox(t, steps());
  const server = createServer(() => {});
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const telemetry = createDirectTelemetry('agy', { endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 300 });
  const started = Date.now();
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
  assert.ok(Date.now() - started < 10_000);
  await telemetry.flush().catch(() => {});
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 2000 });
});

test('AGY direct mode leaves legacy runtime queue data byte-unchanged', async t => {
  const f = await wireFixture(t);
  const box = await sandbox(t, steps());
  const legacy = join(box.path, 'legacy-route');
  await mkdir(legacy, { recursive: true });
  const obstacle = join(legacy, 'queue.sqlite');
  await writeFile(obstacle, Buffer.from([0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x00]));
  const before = await readFile(obstacle);
  const telemetry = createDirectTelemetry('agy', { endpoint: f.endpoint, timeoutMs: 30_000 });
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
  await telemetry.flush();
  await until(() => f.bodies.length >= 1, 'direct delivery beside legacy state');
  assert.deepEqual(await readFile(obstacle), before);
});

test('AGY installer dry-run/install/uninstall preserve unrelated JSON and safely quote generated commands', async t => {
  const box = await sandbox(t);
  const home = join(box.path, 'synthetic home');
  const configFile = join(home, '.gemini', 'config', 'config.json');
  const original = { theme: 'unchanged', plugins: { unrelated: { enabled: false }, 'doda-datalake': { note: 'user-owned' } } };
  await mkdir(join(home, '.gemini', 'config'), { recursive: true, mode: 0o700 });
  await writeFile(configFile, JSON.stringify(original), { mode: 0o600 });
  const config = join(home, "selected config ' $(touch injected-canary).json");
  await writeFile(config, JSON.stringify({ endpoint: 'http://127.0.0.1:1' }), { mode: 0o600 });
  const preview = await installAgy({ home, config });
  assert.deepEqual(JSON.parse(await readFile(configFile, 'utf8')), original);
  await assert.rejects(stat(join(preview.targetPluginDir, 'plugin.json')), { code: 'ENOENT' });
  const installed = await installAgy({ home, config, apply: true });
  const manifest = JSON.parse(await readFile(join(installed.targetPluginDir, 'plugin.json'), 'utf8'));
  assert.deepEqual(Object.keys(manifest).sort(), ['description', 'name']);
  const hooks = JSON.parse(await readFile(join(installed.targetPluginDir, 'hooks.json'), 'utf8'));
  const command = hooks['datalake-telemetry'].PreToolUse[0].hooks[0].command;
  const child = spawn('/bin/sh', ['-c', command], {
    cwd: home, env: { PATH: process.env.PATH, HOME: home, XDG_STATE_HOME: join(home, 'state'), DATALAKE_OTEL_CONFIG: join(home, 'absent.json') },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  const closed = once(child, 'close');
  let stdout = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.resume();
  child.stdin.end(JSON.stringify({ conversationId: box.sessionId, stepIdx: 8, toolCall: { name: 'Bash' } }));
  const [code] = await closed;
  assert.equal(code, 0);
  assert.deepEqual(JSON.parse(stdout), { decision: 'allow' });
  await assert.rejects(stat(join(home, 'injected-canary')), { code: 'ENOENT' });
  const registry = JSON.parse(await readFile(configFile, 'utf8'));
  assert.deepEqual(registry.plugins.unrelated, original.plugins.unrelated);
  assert.equal(registry.plugins['doda-datalake'].note, 'user-owned');
  await writeFile(join(installed.targetPluginDir, 'user-state.json'), '{"keep":true}');
  await installAgy({ home, config, apply: true, uninstall: true });
  assert.deepEqual(JSON.parse(await readFile(configFile, 'utf8')), original);
  assert.deepEqual(JSON.parse(await readFile(join(installed.targetPluginDir, 'user-state.json'), 'utf8')), { keep: true });
  await assert.rejects(stat(join(installed.targetPluginDir, 'plugin.json')), { code: 'ENOENT' });
});

for (const unsafe of ['malformed', 'symlink']) {
  test(`AGY installer refuses ${unsafe} existing config without overwriting it`, async t => {
    const box = await sandbox(t);
    const home = join(box.path, 'home');
    const directory = join(home, '.gemini', 'config');
    const configFile = join(directory, 'config.json');
    await mkdir(directory, { recursive: true, mode: 0o700 });
    const original = unsafe === 'malformed' ? '{broken user JSON' : '{"theme":"unchanged"}';
    if (unsafe === 'symlink') {
      await writeFile(join(box.path, 'external.json'), original, { mode: 0o600 });
      await symlink(join(box.path, 'external.json'), configFile);
    } else await writeFile(configFile, original, { mode: 0o600 });
    for (const flags of [{}, { apply: true }, { apply: true, uninstall: true }]) {
      await assert.rejects(installAgy({ home, config: join(home, 'telemetry.json'), ...flags }), /Malformed|Unsafe/);
    }
    assert.equal(await readFile(configFile, 'utf8'), original);
    await assert.rejects(stat(join(directory, 'plugins', 'doda-datalake', 'plugin.json')), { code: 'ENOENT' });
  });
}

test('AGY bounds hook stdin and keeps neutral output without recording private oversized input', async t => {
  const box = await sandbox(t);
  const child = spawn(process.execPath, [
    fileURLToPath(new URL('../plugins/agy/hook.mjs', import.meta.url)),
    '--pre-tool', '--config', join(box.path, 'missing.json'),
  ], {
    cwd: box.path, env: { PATH: process.env.PATH, HOME: box.path, XDG_STATE_HOME: join(box.path, 'state') },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  const closed = once(child, 'close');
  let stdout = '', stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  child.stdin.on('error', error => { if (error.code !== 'EPIPE') throw error; });
  child.stdin.end(JSON.stringify({ conversationId: box.sessionId, private: 'STDIN_PRIVATE_CANARY' + 'x'.repeat(1024 * 1024) }));
  const [code] = await closed;
  assert.equal(code, 0);
  assert.deepEqual(JSON.parse(stdout), { decision: 'allow' });
  assert.doesNotMatch(stdout + stderr, /STDIN_PRIVATE_CANARY/);
  await assert.rejects(stat(box.state), { code: 'ENOENT' });
  await assert.rejects(stat(join(box.path, 'state')), { code: 'ENOENT' });
});
