import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { execFile, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, writeFile, rm, stat, mkdir, symlink, readdir } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { handleHook } from '../plugins/agy/hook.mjs';
import { createTelemetry } from '../plugins/otel.mjs';
import { installAgy } from '../plugins/agy/install.mjs';
import { openOutbox, readRoute } from '../plugins/outbox.mjs';
import { readSenderStatus } from '../plugins/sender.mjs';
const execFileAsync = promisify(execFile);

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

for (const failure of ['false', 'throw']) {
  for (const failedKind of ['llm.turn', 'subagent']) {
    test(`AGY retries ${failedKind} ${failure} without advancing past failures or duplicating queued usage`, async t => {
      const box = await sandbox(t, steps()), acknowledged = [], attempts = [], failedBodies = [];
      const child0 = `subagent:${box.sessionId}:1:0`, child1 = `subagent:${box.sessionId}:1:1`;
      const failingId = failedKind === 'llm.turn' ? 'turn:1' : child1;
      let fail = true;
      const telemetry = {
        enqueue: async event => {
          attempts.push(event.eventId);
          if (event.eventId === failingId) failedBodies.push(structuredClone(event));
          if (event.eventId === failingId && fail) {
            fail = false;
            if (failure === 'throw') throw new Error('synthetic transport failure');
            return false;
          }
          acknowledged.push(event);
          return true;
        },
        flushLocal: async () => {},
      };
      const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
      await invoke();
      const cursor = JSON.parse(await readFile(join(box.state, 'cursor.json'), 'utf8'));
      assert.equal(cursor.version, 2);
      assert.equal(cursor.lastProcessedIndex, -1);
      assert.deepEqual(cursor.sentEventIds, []);
      assert.deepEqual(cursor.queuedEventIds, failedKind === 'subagent' ? ['turn:1', child0] : []);
      const changed = steps();
      changed[1].content = 'different later transcript observation';
      changed[1].tool_calls[0].args.Subagents[1].Role = 'LaterRole';
      await writeFile(box.transcript, changed.map(step => JSON.stringify(step)).join('\n') + '\n');
      await invoke();
      await invoke();
      assert.deepEqual(acknowledged.filter(event => event.kind === 'llm.turn').map(event => event.eventId), ['turn:1', 'turn:2']);
      assert.deepEqual(acknowledged.filter(event => event.kind === 'subagent').map(event => event.eventId), [child0, child1]);
      assert.deepEqual(attempts.filter(id => id === failingId), [failingId, failingId]);
      assert.deepEqual(failedBodies[1], failedBodies[0]);
      if (failedKind === 'subagent') assert.deepEqual(attempts.filter(id => id === 'turn:1'), ['turn:1']);
      assert.equal(JSON.parse(await readFile(join(box.state, 'cursor.json'), 'utf8')).lastProcessedIndex, 2);
    });
  }

  test(`AGY preserves the observed tool completion and duration through ${failure} retry`, async t => {
    const box = await sandbox(t), attempted = [];
    let fail = true;
    const telemetry = {
      enqueue: async event => {
        attempted.push(structuredClone(event));
        if (fail) {
          fail = false;
          if (failure === 'throw') throw new Error('synthetic transport failure');
          return false;
        }
        return true;
      }, flushLocal: async () => {},
    };
    const payload = { conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 7, toolCall: { name: 'Bash' } };
    await handleHook(payload, { telemetry, cwd: box.path, now: () => 1000, isPreTool: true });
    const first = handleHook({ ...payload, error: 'SYNTHETIC_PRIVATE_ERROR_MARKER' }, { telemetry, cwd: box.path, now: () => 1100 });
    if (failure === 'throw') await assert.rejects(first, /synthetic transport failure/);
    else await first;
    assert.equal(attempted[0].attributes.duration_ms, 100);
    assert.equal(attempted[0].endTimeMs, 1100);
    const readPayload = { ...payload, toolCall: { name: 'Read' } };
    await handleHook(readPayload, { telemetry, cwd: box.path, now: () => 1200, isPreTool: true });
    await handleHook(readPayload, { telemetry, cwd: box.path, now: () => 1400 });
    const retried = await handleHook(payload, { telemetry, cwd: box.path, now: () => 2000 });
    assert.deepEqual(attempted[2], attempted[0]);
    assert.equal(retried[0].attributes.duration_ms, 100);
    assert.equal(retried[0].error, true);
    await handleHook({ ...readPayload, error: 'later changed completion' }, { telemetry, cwd: box.path, now: () => 2500 });
    await handleHook(payload, { telemetry, cwd: box.path, now: () => 3000 });
    assert.deepEqual(attempted.map(event => event.eventId), ['tool:7:Bash', 'tool:7:Read', 'tool:7:Bash']);
    assert.equal(attempted[1].attributes.duration_ms, 200);
  });
}

for (const stepIdx of [7, undefined, '../escaped', 'nested/../../escaped']) {
  test(`AGY isolates tool names sharing step ${stepIdx ?? 'omitted'} and retains their first completion`, async t => {
    const box = await sandbox(t), accepted = [];
    const telemetry = {
      enqueue: async event => { accepted.push(structuredClone(event)); return true; },
      flushLocal: async () => {},
    };
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
    const box = await sandbox(t), accepted = [];
    const telemetry = {
      enqueue: async event => { accepted.push(structuredClone(event)); return true; },
      flushLocal: async () => {},
    };
    const firstBody = {
      kind: 'tool.call', sessionId: box.sessionId, eventId: 'tool:7:Bash',
      startTimeMs: 1000, endTimeMs: 1100, error: true,
      attributes: { 'gen_ai.tool.name': 'Bash', duration_ms: 100, 'error.type': 'tool_error' },
    };
    const state = legacy === 'start' ? { startTimeMs: 1000, toolName: 'Bash' } : {
      version: 2, startTimeMs: 1000, toolName: 'Bash',
      event: legacy === 'mismatched-event' ? { ...firstBody, eventId: 'tool:7:Read' } : firstBody,
      queued: legacy === 'queued',
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
    assert.equal(bashEvents.length, legacy === 'queued' ? 0 : 1);
    if (legacy === 'pending') assert.deepEqual(bashEvents[0], firstBody);
    if (legacy === 'start') assert.equal(bashEvents[0].attributes.duration_ms, 100);
    if (legacy === 'mismatched-event') {
      assert.equal(bashEvents[0].attributes.duration_ms, 50);
      assert.equal(bashEvents[0].error, false);
    }
  });
}

test('AGY retains stable tool identity even when its name contains path components', async t => {
  const box = await sandbox(t), accepted = [];
  const telemetry = {
    enqueue: async event => { accepted.push(structuredClone(event)); return true; },
    flushLocal: async () => {},
  };
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
    enqueue: async event => {
      if (event.kind === 'llm.turn' && first) { first = false; entered(); await blocked; }
      acknowledged.push(event);
      return true;
    }, flushLocal: async () => {},
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

test('AGY tool errors export only a fixed category on the serialized OTLP wire', async t => {
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
  const stateRoot = await mkdtemp(join(tmpdir(), 'datalake-agy-wire-'));
  let ledger;
  t.after(async () => {
    try {
      ledger ??= (await readdir(stateRoot)).map(name => join(stateRoot, name))[0];
      if (ledger) {
        try {
          await until(async () => (await readSenderStatus(ledger))?.state === 'idle', 'isolated sender shutdown');
        } finally {
          let owner;
          try { owner = JSON.parse(await readFile(join(ledger, 'worker.lock', 'owner.json'), 'utf8')); }
          catch (error) { if (error.code !== 'ENOENT') throw error; }
          if (owner && owner.pid !== process.pid) {
            const command = await execFileAsync('ps', ['-p', String(owner.pid), '-o', 'command=']).catch(() => ({ stdout: '' }));
            if (command.stdout.includes(fileURLToPath(new URL('../plugins/sender.mjs', import.meta.url))) && command.stdout.includes(ledger)) {
              process.kill(owner.pid, 'SIGKILL');
            }
          }
        }
      }
    } finally {
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
      await rm(stateRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 });
    }
  });
  const telemetry = createTelemetry('agy', { endpoint: `http://127.0.0.1:${server.address().port}`, stateRoot });
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 1, toolName: 'Bash', error: 'SYNTHETIC_PRIVATE_ERROR_MARKER credential=fixture' }, { telemetry, cwd: box.path, now: () => 1100 });
  await until(() => bodies.length === 1, 'actual HTTP delivery');
  ledger = join(stateRoot, (await readdir(stateRoot))[0]);
  const outbox = await openOutbox({ root: ledger, route: await readRoute(ledger) });
  await until(async () => (await outbox.status()).done === 1 && (await readSenderStatus(ledger))?.state === 'idle', 'durable ACK and worker release');
  const spans = bodies.flatMap(body => JSON.parse(body).resourceSpans.flatMap(resource => resource.scopeSpans.flatMap(scope => scope.spans)));
  assert.deepEqual(spans.map(span => span.status.code), [2]);
  assert.deepEqual(spans[0].attributes.find(attribute => attribute.key === 'error.type').value, { stringValue: 'tool_error' });
  assert.doesNotMatch(bodies.join(''), /SYNTHETIC_PRIVATE_ERROR_MARKER|credential=fixture/);
});

test('AGY treats legacy sent IDs as ACK evidence without replaying historical turns', async t => {
  const box = await sandbox(t, steps()), accepted = [];
  await mkdir(box.state, { recursive: true, mode: 0o700 });
  const legacy = ['turn:1', `subagent:${box.sessionId}:1:0`];
  await writeFile(join(box.state, 'cursor.json'), JSON.stringify({ lastProcessedIndex: -1, sentEventIds: legacy }), { mode: 0o600 });
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, {
    telemetry: { enqueue: async event => { accepted.push(event); return true; }, flushLocal: async () => {} },
    cwd: box.path, now: () => 1500,
  });
  assert.deepEqual(accepted.filter(event => event.kind === 'llm.turn').map(event => event.eventId), ['turn:2']);
  assert.deepEqual(accepted.filter(event => event.kind === 'subagent').map(event => event.eventId), [`subagent:${box.sessionId}:1:1`]);
  const cursor = JSON.parse(await readFile(join(box.state, 'cursor.json'), 'utf8'));
  assert.equal(cursor.version, 2);
  assert.deepEqual(cursor.sentEventIds, legacy);
  assert.equal(cursor.lastProcessedIndex, 2);
});

for (const failure of ['false', 'throw']) {
  test(`AGY Stop keeps its first body and timestamp through ${failure} and later duplicates`, async t => {
    const box = await sandbox(t, steps()), attempts = [];
    let fail = true, clock = 1500;
    const telemetry = {
      enqueue: async event => {
        if (event.kind !== 'session') return true;
        attempts.push(event);
        if (fail) { fail = false; if (failure === 'throw') throw new Error('synthetic local failure'); return false; }
        return true;
      }, flushLocal: async () => {},
    };
    const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => clock });
    if (failure === 'throw') await assert.rejects(invoke(), /synthetic local failure/);
    else await invoke();
    await writeFile(box.transcript, '');
    clock = 9000;
    await invoke();
    await invoke();
    assert.equal(attempts.length, 2);
    assert.deepEqual(attempts[1], attempts[0]);
    assert.equal(attempts[1].endTimeMs, 1500);
    assert.equal(attempts[1].attributes['coding_agent.session.duration_ms'], 500);
    assert.equal(JSON.parse(await readFile(join(box.state, 'stop.json'), 'utf8')).queued, true);
  });
}

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
