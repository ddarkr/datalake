import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, access, symlink, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { openOutbox, readRoute } from '../plugins/outbox.mjs';
import { createTelemetry } from '../plugins/otel.mjs';
import ompTelemetry, { nativeTelemetryEnabled } from '../plugins/omp/index.mjs';
import { install, agentDirectory } from '../plugins/omp/install.mjs';

const secret = 'DO_NOT_EXPORT_prompt_thinking_tool_args_result';
const usage = { input: 11, output: 23, cacheRead: 7, cacheWrite: 3, reasoningTokens: 5, cost: { total: 0.125 } };
function harness(options = {}, id = 'root-session', file = '/private/session.jsonl') {
  const handlers = new Map();
  const bus = new Map();
  const emitted = [];
  const ctx = { cwd: '/private/repository', sessionManager: {
    getSessionId: () => id, getSessionFile: () => file,
  } };
  const telemetry = { enqueue: async event => { emitted.push(event); return true; }, flushLocal: async () => {} };
  let now = 1_700_000_000_000;
  ompTelemetry({
    on: (name, fn) => handlers.set(name, fn),
    events: { on: (name, fn) => { bus.set(name, fn); return () => bus.delete(name); } },
  }, { telemetry, env: {}, clock: () => now++, repositoryContext: () => ({}), ...options });
  return { emitted, ctx,
    event(name, payload = {}) { return handlers.get(name)?.({ type: name, ...payload }, ctx); },
    lifecycle(payload) { return bus.get('task:subagent:lifecycle')?.(payload); },
  };
}
function message(timestamp = 1_700_000_000_010) {
  return { role: 'assistant', provider: 'test-provider', model: 'test-model', timestamp,
    responseId: 'response-native', completedAt: timestamp + 50, usage, stopReason: 'stop',
    content: [{ type: 'text', text: secret }, { type: 'thinking', thinking: secret }],
    errorMessage: secret, providerPayload: { secret },
  };
}

async function bounded(operation, description, timeoutMs = 30000) {
  let timer;
  try {
    return await Promise.race([
      operation,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(description)), timeoutMs); }),
    ]);
  } finally { clearTimeout(timer); }
}

async function until(predicate, description) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    if (await bounded(Promise.resolve().then(predicate), description)) return;
    await delay(20);
  }
  assert.fail(description);
}

async function wireFixture(t, holdAck = false) {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-wire-'));
  const stateRoot = join(home, 'outboxes');
  const configPath = join(home, 'otel.json');
  const bodies = [], held = [], workers = new Map();
  const execute = promisify(execFile);
  const senderPath = fileURLToPath(new URL('../plugins/sender.mjs', import.meta.url));
  const server = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    for (const { root } of await ledgers()) await owner(root);
    bodies.push(JSON.parse(body));
    if (holdAck) held.push(res);
    else { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); }
  });
  async function ledgers() {
    let entries;
    try { entries = await readdir(stateRoot, { withFileTypes: true }); }
    catch (error) { if (['ENOENT', 'ENOTDIR'].includes(error.code)) return []; throw error; }
    return Promise.all(entries.filter(entry => entry.isDirectory()).map(async entry => {
      const root = join(stateRoot, entry.name);
      return { root, outbox: await openOutbox({ root, route: await readRoute(root) }) };
    }));
  }
  async function owner(root) {
    try {
      const { pid } = JSON.parse(await readFile(join(root, 'worker.lock', 'owner.json'), 'utf8'));
      assert.ok(Number.isSafeInteger(pid) && pid > 0 && pid !== process.pid);
      workers.set(pid, root);
      return pid;
    } catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  }
  async function owns(root, pid) {
    try {
      const { stdout } = await execute('ps', ['-p', String(pid), '-o', 'stat=', '-o', 'command='], { timeout: 5000 });
      const match = /^\s*(\S+)\s+([\s\S]*)$/.exec(stdout);
      return Boolean(match && !match[1].startsWith('Z') && match[2].includes(senderPath) && match[2].includes(root));
    } catch (error) { if (error.code === 1 || error.code === 'ESRCH') return false; throw error; }
  }
  t.after(async () => {
    const errors = [];
    const cleanup = async operation => { try { await operation(); } catch (error) { errors.push(error); } };
    await cleanup(async () => { for (const { root } of await ledgers()) await owner(root); });
    await cleanup(async () => {
      for (const [pid, root] of workers) if (await owns(root, pid)) {
        try { process.kill(pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
      }
    });
    server.closeAllConnections();
    await cleanup(() => new Promise(resolve => server.close(resolve)));
    await cleanup(() => until(async () => {
      for (const [pid, root] of workers) if (await owns(root, pid)) return false;
      return true;
    }, 'isolated OMP sender cleanup'));
    await cleanup(() => rm(home, { recursive: true, force: true }));
    if (errors.length) throw new AggregateError(errors, 'OMP fixture cleanup failed');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  await writeFile(configPath, JSON.stringify({
    endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 30000,
  }), { mode: 0o600 });
  async function delivered(expected) {
    await until(async () => {
      const items = await ledgers();
      if (!items.length) return false;
      const statuses = await Promise.all(items.map(item => item.outbox.status()));
      return statuses.reduce((sum, status) => sum + status.done, 0) === expected
        && statuses.every(status => status.pending === 0);
    }, 'actual OMP durable delivery');
    await until(async () => {
      for (const { root } of await ledgers()) if (await owner(root)) return false;
      return true;
    }, 'empty OMP sender release');
  }
  return { home, stateRoot, configPath, bodies, held, ledgers, delivered,
    releaseAck() {
      holdAck = false;
      for (const res of held) { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); }
    },
    harness: options => harness({ telemetry: undefined, configPath, stateRoot, ...options }),
    spans: () => bodies.flatMap(body => body.resourceSpans ?? []).flatMap(resource => resource.scopeSpans ?? []).flatMap(scope => scope.spans ?? []),
  };
}

test('OMP final notifications count usage once and early tools retain their LLM parent', async () => {
  const h = harness();
  await h.event('session_start');
  const m = message();
  await h.event('message_start', { message: m });
  await h.event('tool_execution_start', { toolCallId: 'call-1', toolName: 'read', args: { path: secret } });
  await h.event('message_end', { message: m });
  await h.event('message_end', { message: structuredClone(m) });
  await h.event('turn_end', { message: m });
  await h.event('agent_end', { messages: [m] });
  await h.event('tool_execution_end', { toolCallId: 'call-1', toolName: 'read', isError: false, result: { content: secret, usage } });
  await h.event('tool_execution_end', { toolCallId: 'call-1', toolName: 'read', isError: false });
  await h.event('session_shutdown');
  await h.event('session_shutdown');
  const llm = h.emitted.filter(e => e.kind === 'llm.turn');
  assert.equal(llm.length, 1);
  assert.equal(llm[0].attributes['gen_ai.usage.input_tokens'], 21);
  assert.equal(llm[0].attributes['gen_ai.usage.output_tokens'], 23);
  assert.equal(llm[0].attributes['gen_ai.usage.reasoning.output_tokens'], 5);
  assert.equal(llm[0].attributes['pi.gen_ai.cost.estimated_usd'], 0.125);
  const tools = h.emitted.filter(e => e.kind === 'tool.call');
  assert.equal(tools.length, 1);
  assert.equal(tools[0].parentEventId, llm[0].eventId);
  assert.equal(h.emitted.filter(e => e.kind === 'session').length, 1);
  assert.ok(!JSON.stringify(h.emitted).includes(secret));
  assert.ok(!JSON.stringify(h.emitted).includes('/private/'));
});

test('OMP child factories share root trace and task aggregates never duplicate child usage', async () => {
  const root = harness();
  await root.event('session_start');
  await root.event('message_end', { message: message() });
  await root.event('tool_execution_start', { toolCallId: 'spawn-call', toolName: 'task' });
  const lifecycle = { id: 'Child', agent: 'scout', parentToolCallId: 'spawn-call', sessionFile: '/private/child.jsonl', description: secret };
  await root.lifecycle({ ...lifecycle, status: 'started' });
  const child = harness({}, 'native-child-session', lifecycle.sessionFile);
  await child.event('session_start');
  await child.event('message_end', { message: message() });
  await child.event('tool_execution_start', { toolCallId: 'spawn-call', toolName: 'read' });
  await child.event('tool_execution_end', { toolCallId: 'spawn-call', toolName: 'read' });
  await child.event('session_shutdown');
  await root.event('tool_execution_end', { toolCallId: 'spawn-call', toolName: 'task', result: { details: { usage } } });
  await root.lifecycle({ ...lifecycle, status: 'completed' });
  await root.lifecycle({ ...lifecycle, status: 'completed' });
  await root.event('session_shutdown');
  const subagents = root.emitted.filter(e => e.kind === 'subagent');
  assert.equal(subagents.length, 1);
  const parentTool = root.emitted.find(e => e.kind === 'tool.call');
  assert.equal(subagents[0].parentEventId, parentTool.eventId);
  assert.notEqual(child.emitted.find(e => e.kind === 'tool.call').eventId, parentTool.eventId);
  assert.notEqual(child.emitted[0].eventId, root.emitted.find(e => e.kind === 'llm.turn').eventId);
  assert.equal(child.emitted[0].sessionId, 'root-session');
  assert.equal(child.emitted[0].parentEventId, subagents[0].eventId);
  assert.equal(child.emitted[0].attributes['coding_agent.agent.parent_id'], 'root-session');
  assert.equal(root.emitted.filter(e => e.attributes['gen_ai.usage.input_tokens'] !== undefined).length, 1);
  assert.equal(child.emitted.filter(e => e.kind === 'session').length, 0);
  assert.ok(!JSON.stringify([...root.emitted, ...child.emitted]).includes(secret));
});

test('OMP same-collector native traces own usage across the inherited tree', async t => {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-native-'));
  const configPath = join(home, 'otel.json');
  const endpoint = 'http://localhost:4318/pipeline/v1/traces';
  const env = { OTEL_EXPORTER_OTLP_ENDPOINT: 'http://localhost:4318/pipeline', OTEL_EXPORTER_OTLP_HEADERS: `authorization=${secret}` };
  const diagnostics = [];
  t.mock.method(console, 'error', line => diagnostics.push(line));
  await writeFile(configPath, JSON.stringify({ endpoint }), { mode: 0o600 });
  try {
    const root = harness({ env, configPath });
    await root.event('session_start');
    await root.event('message_end', { message: message() });
    const spawn = { id: 'NativeChild', agent: 'scout', parentToolCallId: 'native-spawn', sessionFile: '/private/native-child.jsonl' };
    await root.lifecycle({ ...spawn, status: 'started' });
    // Even a changed child environment cannot switch source halfway through a tree.
    const child = harness({ env: {}, configPath }, 'native-child-id', spawn.sessionFile);
    await child.event('session_start');
    await child.event('message_end', { message: message() });
    await child.event('session_shutdown');
    await root.lifecycle({ ...spawn, status: 'completed' });
    await root.event('session_shutdown');
    assert.equal([...root.emitted, ...child.emitted].filter(e => e.kind === 'llm.turn').length, 0);
    assert.equal(root.emitted.filter(e => e.kind === 'subagent').length, 1);
    assert.ok(diagnostics.some(line => line.includes('native traces')));
    assert.ok(!diagnostics.join('').includes(secret));
    assert.ok(!diagnostics.join('').includes('localhost'));
    assert.equal(env.OTEL_EXPORTER_OTLP_HEADERS, `authorization=${secret}`);
    assert.equal(nativeTelemetryEnabled({ ...env, OTEL_EXPORTER_OTLP_PROTOCOL: 'http/json' }, endpoint), false);
    assert.equal(nativeTelemetryEnabled({ ...env, OTEL_SDK_DISABLED: 'true' }, endpoint), false);
    assert.equal(nativeTelemetryEnabled({ OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: endpoint }, endpoint), true);
    // Signal endpoints are literal; a missing /v1/traces must not match.
    assert.equal(nativeTelemetryEnabled({ OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: 'http://localhost:4318/pipeline' }, endpoint), false);
  } finally { await rm(home, { recursive: true, force: true }); }
});

test('OMP retains plugin usage for metrics/logs-only or another native trace collector', async () => {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-ownership-'));
  const configPath = join(home, 'otel.json');
  await writeFile(configPath, JSON.stringify({ endpoint: 'http://localhost:4318/plugin' }), { mode: 0o600 });
  try {
    for (const env of [
      { OTEL_EXPORTER_OTLP_ENDPOINT: 'http://localhost:4318/plugin', OTEL_TRACES_EXPORTER: 'none' },
      { OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: 'http://localhost:4318/plugin/v1/metrics' },
      { OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: 'http://localhost:4318/plugin/v1/logs' },
      { OTEL_EXPORTER_OTLP_ENDPOINT: 'http://localhost:4318/other' },
      { OTEL_EXPORTER_OTLP_ENDPOINT: 'http://localhost:4318/plugin', OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: 'http://localhost:4318/other/v1/traces' },
    ]) {
      const h = harness({ env, configPath });
      await h.event('session_start');
      await h.event('message_end', { message: message() });
      await h.event('session_shutdown');
      assert.equal(h.emitted.filter(e => e.kind === 'llm.turn').length, 1);
    }
  } finally { await rm(home, { recursive: true, force: true }); }
});

test('OMP metadata-only events reach OTLP without content, paths or tool arguments', async t => {
  const f = await wireFixture(t);
  const h = f.harness();
  assert.equal(h.event('session_start'), undefined);
  assert.equal(h.event('message_end', { message: message() }), undefined);
  assert.equal(h.event('tool_execution_start', { toolCallId: 'call_wire|fc_item', toolName: 'read', args: { secret } }), undefined);
  assert.equal(h.event('tool_execution_end', { toolCallId: 'call_wire|fc_item', toolName: 'read', result: { content: secret } }), undefined);
  await bounded(h.event('session_shutdown'), 'OMP local shutdown');
  await f.delivered(3);
  const spans = f.spans();
  assert.equal(spans.filter(span => span.name === 'coding_agent.llm.turn').length, 1);
  assert.equal(spans.filter(span => span.name === 'coding_agent.tool.call').length, 1);
  assert.ok(!JSON.stringify(f.bodies).includes(secret));
  assert.ok(!JSON.stringify(f.bodies).includes('/private/'));
});

test('OMP shutdown completes with the collector ACK still held', async t => {
  const f = await wireFixture(t, true);
  const h = f.harness();
  assert.equal(h.event('message_start', { message: message() }), undefined);
  assert.equal(h.event('message_end', { message: message() }), undefined);
  assert.equal(h.event('tool_execution_start', { toolCallId: 'held-tool', toolName: 'read' }), undefined);
  assert.equal(h.event('tool_execution_end', { toolCallId: 'held-tool', toolName: 'read' }), undefined);
  await until(() => f.held.length > 0, 'collector has an unacknowledged request');
  await bounded(h.event('session_shutdown'), 'shutdown must not wait for collector ACK');
  assert.ok(f.held.every(res => !res.writableEnded && !res.destroyed));
  const [{ outbox }] = await f.ledgers();
  assert.equal((await outbox.status()).done, 0);
  assert.equal((await outbox.pending()).length, 3);
  f.releaseAck();
  await f.delivered(3);
});

test('OMP bounds slow Git and exits before sender readiness without losing final usage', async t => {
  const f = await wireFixture(t);
  const nodeBinary = join(f.home, 'slow-node');
  const quotedNode = `'${process.execPath.replaceAll("'", "'\\''")}'`;
  await writeFile(nodeBinary, `#!/bin/sh\nsleep 3\nexec ${quotedNode} "$@"\n`, { mode: 0o700 });
  const telemetry = createTelemetry('oh-my-pi', { configPath: f.configPath, stateRoot: f.stateRoot, nodeBinary, waitForSenderReady: false });
  const h = f.harness({
    telemetry,
    repositoryContext: () => delay(3000, { 'vcs.ref.head.name': 'slow-branch' }),
  });
  h.event('message_end', { message: message() });
  const shutdown = h.event('session_shutdown');
  try {
    await bounded(shutdown, 'shutdown exceeded its local persistence budget', 1800);
    const [{ outbox }] = await f.ledgers();
    assert.equal((await outbox.status()).pending, 2);
  } finally {
    await shutdown;
    await writeFile(nodeBinary, `#!/bin/sh\nexec ${quotedNode} "$@"\n`, { mode: 0o700 });
    const replay = f.harness({ telemetry });
    replay.event('message_end', { message: message() });
    await replay.event('session_shutdown');
    await f.delivered(2);
  }
  const llm = f.spans().find(span => span.name === 'coding_agent.llm.turn');
  const attrs = Object.fromEntries(llm.attributes.map(attr => [attr.key, attr.value.intValue]));
  assert.equal(Number(attrs['gen_ai.usage.input_tokens']), 21);
  assert.equal(Number(attrs['gen_ai.usage.output_tokens']), 23);
});

test('OMP a recoverable local outbox failure retries original usage and tool duration on the wire', async t => {
  const f = await wireFixture(t);
  await writeFile(f.stateRoot, 'temporary local filesystem obstacle', { mode: 0o600 });
  let now = 1000, refused = 0;
  const exporter = createTelemetry('oh-my-pi', { configPath: f.configPath, stateRoot: f.stateRoot });
  const h = f.harness({
    clock: () => now,
    telemetry: {
      async enqueue(event) {
        const accepted = await exporter.enqueue(event);
        if (!accepted) refused++;
        return accepted;
      },
      flushLocal: () => exporter.flushLocal(),
    },
  });
  const m = message(1100);
  m.usage = structuredClone(usage);
  assert.equal(h.event('message_start', { message: m }), undefined);
  now = 1200;
  assert.equal(h.event('tool_execution_start', { toolCallId: 'retry-tool', toolName: 'read', args: { secret } }), undefined);
  assert.equal(h.event('message_end', { message: m }), undefined);
  now = 1300;
  assert.equal(h.event('tool_execution_end', { toolCallId: 'retry-tool', toolName: 'read', result: { content: secret } }), undefined);
  await until(() => refused === 2, 'both completed events are refused locally');
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(f.bodies, []);
  await rm(f.stateRoot);
  now = 9000;
  m.usage.input = 999;
  m.usage.output = 999;
  m.provider = 'changed-provider';
  m.completedAt += 1000;
  h.event('message_end', { message: m });
  h.event('tool_execution_end', { toolCallId: 'retry-tool', toolName: 'write', isError: true });
  await bounded(h.event('session_shutdown'), 'recovered local writes finish shutdown');
  await f.delivered(3);
  const llm = f.spans().find(span => span.name === 'coding_agent.llm.turn');
  const tool = f.spans().find(span => span.name === 'coding_agent.tool.call');
  const attrs = span => Object.fromEntries(span.attributes.map(attr => [attr.key, attr.value.intValue ?? attr.value.doubleValue ?? attr.value.stringValue]));
  assert.equal(Number(attrs(llm)['gen_ai.usage.input_tokens']), 21);
  assert.equal(Number(attrs(llm)['gen_ai.usage.output_tokens']), 23);
  assert.equal(attrs(llm)['gen_ai.provider.name'], 'test-provider');
  assert.equal(BigInt(llm.endTimeUnixNano) - BigInt(llm.startTimeUnixNano), 50_000_000n);
  assert.equal(BigInt(tool.endTimeUnixNano) - BigInt(tool.startTimeUnixNano), 100_000_000n);
  assert.equal(attrs(tool)['gen_ai.tool.name'], 'read');
  assert.equal(tool.status.code, 1);
  assert.equal(tool.parentSpanId, llm.spanId);
  assert.ok(!JSON.stringify(f.bodies).includes(secret));
});

test('OMP false or throwing local acceptance retains immutable projected metadata for later notifications', async () => {
  for (const failure of [false, new Error('synthetic local write failure')]) {
    const accepted = [], first = new Map();
    let resolveRepository;
    const repository = new Promise(resolve => { resolveRepository = resolve; });
    const h = harness({
      repositoryContext: () => repository,
      telemetry: {
        async enqueue(event) {
          if (event.kind !== 'session' && !first.has(event.eventId)) {
            first.set(event.eventId, structuredClone(event));
            if (failure instanceof Error) throw failure;
            return failure;
          }
          accepted.push(event);
          return true;
        },
        async flushLocal() {},
      },
    });
    const m = message();
    m.usage = structuredClone(usage);
    assert.equal(h.event('message_end', { message: m }), undefined);
    assert.equal(h.event('tool_execution_start', { toolCallId: 'immutable-tool', toolName: 'read', args: { secret } }), undefined);
    assert.equal(h.event('tool_execution_end', { toolCallId: 'immutable-tool', toolName: 'read', result: { content: secret } }), undefined);
    assert.deepEqual(accepted, []);
    resolveRepository({ 'coding_agent.repository.id': 'a'.repeat(64) });
    await until(() => first.size === 2, 'projected events fail local acceptance');
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(accepted.filter(event => event.kind !== 'session').length, 0);
    m.usage.input = 999;
    m.completedAt += 1000;
    h.ctx.sessionManager.getSessionId = () => 'temporary-session';
    h.event('session_switch');
    h.ctx.sessionManager.getSessionId = () => 'root-session';
    h.event('session_switch');
    h.event('message_end', { message: m });
    h.event('tool_execution_end', { toolCallId: 'immutable-tool', toolName: 'write', isError: true });
    await h.event('session_shutdown');
    const delivered = accepted.filter(event => event.kind !== 'session');
    assert.deepEqual(delivered, [...first.values()]);
    assert.equal(delivered.find(event => event.kind === 'llm.turn').attributes['gen_ai.usage.input_tokens'], 21);
    assert.ok(delivered.every(event => event.attributes['coding_agent.repository.id'] === 'a'.repeat(64)));
    assert.ok(!JSON.stringify(delivered).includes(secret));
    assert.ok(!JSON.stringify(delivered).includes('/private/'));
  }
});

test('OMP early child notifications retain repository metadata and their observed parent route', async () => {
  let resolveRepository;
  const repository = new Promise(resolve => { resolveRepository = resolve; });
  const root = harness({ repositoryContext: () => repository });
  root.event('session_start');
  const spawn = { id: 'EarlyChild', agent: 'scout', parentToolCallId: 'early-spawn', sessionFile: '/private/early-child.jsonl' };
  assert.equal(root.lifecycle({ ...spawn, status: 'started' }), undefined);
  const child = harness({ repositoryContext: () => repository }, 'early-child-id', spawn.sessionFile);
  assert.equal(child.event('message_end', { message: message() }), undefined);
  const childShutdown = child.event('session_shutdown');
  root.lifecycle({ ...spawn, status: 'completed' });
  const rootShutdown = root.event('session_shutdown');
  assert.deepEqual(child.emitted, []);
  resolveRepository({ 'coding_agent.repository.id': 'b'.repeat(64) });
  await Promise.all([childShutdown, rootShutdown]);
  const childTurn = child.emitted.find(event => event.kind === 'llm.turn');
  const observed = root.emitted.find(event => event.kind === 'subagent');
  assert.equal(childTurn.sessionId, 'root-session');
  assert.equal(childTurn.parentEventId, observed.eventId);
  assert.equal(childTurn.attributes['coding_agent.agent.parent_id'], 'root-session');
  assert.equal(childTurn.attributes['coding_agent.repository.id'], 'b'.repeat(64));
});

test('OMP outboxes follow the factory-captured active profile', async t => {
  const f = await wireFixture(t);
  const env = { OMP_PROFILE: 'alpha' };
  const first = f.harness({ env });
  env.OMP_PROFILE = 'beta';
  first.event('message_end', { message: message() });
  first.ctx.sessionManager.getSessionId = () => 'another-alpha-session';
  first.event('session_switch');
  first.event('message_end', { message: message(1_700_000_000_020) });
  await first.event('session_shutdown');
  await f.delivered(4);
  const second = f.harness({ env });
  second.event('message_end', { message: message() });
  await second.event('session_shutdown');
  await f.delivered(6);
  const ledgers = await f.ledgers();
  assert.equal(ledgers.length, 2);
  const statuses = await Promise.all(ledgers.map(item => item.outbox.status()));
  assert.deepEqual(statuses.map(status => status.done).sort(), [2, 4]);
});

test('OMP installer preserves profile settings and unrelated extensions through install/uninstall', async () => {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-install-'));
  const env = { OMP_PROFILE: 'work', PI_CODING_AGENT_DIR: '/unrelated/agent' };
  const dir = join(home, '.omp/profiles/work/agent');
  await mkdir(join(dir, 'extensions'), { recursive: true });
  const settings = '# preserve comments\nmodel: my-private-model\nextensions:\n  - ./other.js\n';
  await writeFile(join(dir, 'config.yml'), settings);
  await writeFile(join(dir, 'extensions/other.js'), 'export default () => {};\n');
  try {
    const args = ['--home', home, '--config', join(home, 'otel.json')];
    const preview = await install(args, env);
    await assert.rejects(access(preview.path), { code: 'ENOENT' });
    const first = await install([...args, '--apply'], env);
    assert.equal(first.changed, true);
    const second = await install([...args, '--apply'], env);
    assert.equal(second.changed, false);
    const legacy = '// Managed by doda-datalake OMP telemetry installer.\n'
      + `import extension from ${JSON.stringify(new URL('../plugins/omp/index.mjs', import.meta.url).href)};\n`
      + `export default pi => extension(pi, ${JSON.stringify({ configPath: join(home, 'otel.json') })});\n`;
    await writeFile(first.path, legacy, { mode: 0o600 });
    await install(args, env);
    const migrated = await install([...args, '--apply'], env);
    assert.equal(migrated.changed, true);
    assert.equal((await install([...args, '--apply'], env)).changed, false);
    await assert.rejects(install(['--home', home, '--config', join(home, 'another.json'), '--apply'], env));
    assert.equal(await readFile(join(dir, 'config.yml'), 'utf8'), settings);
    await install(['--home', home, '--uninstall', '--apply'], env);
    await assert.rejects(access(first.path), { code: 'ENOENT' });
    assert.equal(await readFile(join(dir, 'extensions/other.js'), 'utf8'), 'export default () => {};\n');
    assert.equal(await readFile(join(dir, 'config.yml'), 'utf8'), settings);
    await writeFile(first.path, '// user-owned loader\n');
    await assert.rejects(install([...args, '--apply'], env));
    await assert.rejects(install(['--home', home, '--uninstall', '--apply'], env));
    assert.equal(await readFile(first.path, 'utf8'), '// user-owned loader\n');
  } finally { await rm(home, { recursive: true, force: true }); }
});

test('OMP installer honors default-profile override and refuses escaped/symlinked homes', async () => {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-isolation-'));
  try {
    const custom = join(home, 'custom-agent');
    assert.equal(agentDirectory({ home, env: { PI_CODING_AGENT_DIR: custom } }), custom);
    assert.equal(agentDirectory({ home, env: { OMP_PROFILE: '', PI_PROFILE: 'work', PI_CODING_AGENT_DIR: join(home, '.omp/profiles/work/agent') } }), join(home, '.omp/agent'));
    await assert.rejects(install(['--home', home, '--apply'], { PI_CODING_AGENT_DIR: '/outside/home' }));
    await assert.rejects(install(['--home', home, '--profile', '../escape', '--apply'], {}));
    await mkdir(join(home, '.omp/agent'), { recursive: true });
    await mkdir(join(home, 'other-extensions'));
    await symlink(join(home, 'other-extensions'), join(home, '.omp/agent/extensions'));
    await assert.rejects(install(['--home', home, '--apply'], {}));
  } finally { await rm(home, { recursive: true, force: true }); }
});
