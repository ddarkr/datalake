import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, access, symlink, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
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
  const telemetry = { send: async event => { emitted.push(event); return true; }, flush: async () => {} };
  let now = 1_700_000_000_000;
  ompTelemetry({
    on: (name, fn) => handlers.set(name, fn),
    getThinkingLevel: () => options.settings?.effort,
    getServiceTiers: () => options.settings?.tiers ?? {},
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

test('OMP separates provider timing, observed first text and unambiguous input with scalar snapshots', async () => {
  let now = 100;
  const settings = { effort: 'high', tiers: { openai: 'priority' } };
  const h = harness({ clock: () => now, settings });
  h.ctx.model = { provider: 'openai', api: 'openai-responses' };
  h.event('session_start');
  h.event('input', { source: 'interactive', text: secret });
  h.event('before_agent_start', { prompt: secret });
  now = 110;
  const payload = { service_tier: 'flex', reasoning: { effort: 'low' }, thinking: { budget_tokens: 0 }, input: secret };
  h.event('before_provider_request', { payload });
  const m = { ...message(110), provider: 'openai', api: 'openai-responses', duration: 20, ttft: 0,
    completedAt: 140, serviceTier: 'flex' };
  h.event('message_start', { message: m });
  settings.effort = 'max';
  settings.tiers.openai = 'ultrafast';
  payload.reasoning.effort = 'max';
  now = 115;
  h.event('message_update', { message: m, assistantMessageEvent: { type: 'thinking_delta', delta: secret, partial: m } });
  h.event('message_update', { message: m, assistantMessageEvent: { type: 'text_delta', delta: '', partial: m } });
  now = 120;
  h.event('message_update', { message: m, assistantMessageEvent: { type: 'text_delta', delta: secret, contentIndex: 0, partial: m } });
  now = 140;
  h.event('message_end', { message: m });
  h.event('message_end', { message: m });
  await h.event('session_shutdown');
  const request = h.emitted.find(e => e.kind === 'llm.turn');
  assert.equal(request.endTimeMs - request.startTimeMs, 30);
  assert.equal(request.attributes['coding_agent.request.duration_ms'], 20);
  assert.equal(request.attributes['coding_agent.duration.kind'], 'provider_call');
  assert.equal(request.attributes.ttft_ms, 0);
  assert.equal(request.attributes['coding_agent.ttft.kind'], 'first_output_item');
  assert.equal(request.attributes['coding_agent.response.first_text_ms'], 10);
  assert.equal(request.attributes['coding_agent.request.service_tier'], 'flex');
  assert.equal(request.attributes['coding_agent.harness.service_tier'], 'flex');
  assert.equal(request.attributes['coding_agent.response.service_tier'], undefined);
  assert.equal(request.attributes['coding_agent.request.reasoning.effort'], 'low');
  assert.equal(request.attributes['coding_agent.request.reasoning.budget_tokens'], 0);
  const interactions = h.emitted.filter(e => e.kind === 'interaction');
  assert.equal(interactions.length, 1);
  assert.equal(interactions[0].attributes['coding_agent.interaction.first_response_ms'], 20);
  assert.ok(!Object.keys(interactions[0].attributes).some(k => k.startsWith('gen_ai.usage.')));
  assert.ok(!JSON.stringify(h.emitted).includes(secret));
});

test('OMP omits ambiguous, switched and aborted input associations and unavailable timing', async () => {
  for (const scenario of ['concurrent', 'steered', 'switched', 'aborted', 'missing']) {
    let now = 100;
    const h = harness({ clock: () => now });
    h.event('session_start');
    h.event('input', { source: 'interactive', text: secret });
    if (scenario === 'concurrent') h.event('input', { source: 'interactive', text: secret });
    h.event('before_agent_start', { prompt: secret });
    const m = { ...message(110), completedAt: 150 };
    if (scenario === 'aborted') m.stopReason = 'aborted';
    h.event('message_start', { message: m });
    if (scenario === 'steered') h.event('input', { source: 'interactive', text: secret });
    if (scenario === 'switched') h.event('session_switch');
    now = 120;
    if (scenario !== 'missing') h.event('message_update', {
      message: m, assistantMessageEvent: { type: 'text_delta', delta: secret, contentIndex: 0, partial: m },
    });
    h.event('message_end', { message: m });
    await h.event('session_shutdown');
    assert.equal(h.emitted.filter(e => e.kind === 'interaction').length, 0, scenario);
    const attrs = h.emitted.find(e => e.kind === 'llm.turn').attributes;
    assert.equal(attrs.ttft_ms, undefined);
    assert.equal(attrs['coding_agent.ttft.kind'], undefined);
    assert.equal(attrs['coding_agent.request.duration_ms'], undefined);
    if (scenario === 'missing' || scenario === 'switched') assert.equal(attrs['coding_agent.response.first_text_ms'], undefined);
    assert.ok(!JSON.stringify(h.emitted).includes(secret));
  }
});

test('OMP freezes harness settings and preserves zero while rejecting malformed timing', async () => {
  const settings = { effort: 'medium', tiers: { openai: 'priority' } };
  const h = harness({ settings, clock: () => 100 });
  const first = { ...message(100), provider: 'openai', api: 'openai-responses', ttft: 0, duration: 0, completedAt: 100 };
  h.event('message_start', { message: first });
  settings.effort = 'high';
  settings.tiers.openai = 'flex';
  h.event('message_end', { message: first });
  const second = { ...message(200), provider: 'openai', ttft: 11, duration: 10 };
  h.event('message_start', { message: second });
  h.event('message_end', { message: second });
  for (const [i, invalid] of [true, -1, Infinity, NaN].entries()) {
    h.event('message_end', { message: { ...message(300 + i), ttft: invalid, duration: invalid } });
  }
  await h.event('session_shutdown');
  const requests = h.emitted.filter(e => e.kind === 'llm.turn');
  assert.equal(requests[0].attributes['coding_agent.request.reasoning.effort'], 'medium');
  assert.equal(requests[0].attributes['coding_agent.request.service_tier'], 'priority');
  assert.equal(requests[0].attributes['coding_agent.request.settings_source'], 'harness');
  assert.equal(requests[0].attributes['coding_agent.request.duration_ms'], 0);
  assert.equal(requests[0].attributes.ttft_ms, 0);
  assert.equal(requests[1].attributes['coding_agent.request.reasoning.effort'], 'high');
  assert.equal(requests[1].attributes['coding_agent.request.service_tier'], 'flex');
  for (const request of requests.slice(1)) assert.equal(request.attributes.ttft_ms, undefined);
  for (const request of requests.slice(2)) assert.equal(request.attributes['coding_agent.request.duration_ms'], undefined);
});

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

// Direct-mode loopback fixture: one HTTP request per send, no sender process,
// no durable queue, no process cleanup. stateRoot is isolation only so the
// suite never touches the real production queue; it also hosts the legacy
// outbox fixture. respond is a fixed action or (count, body) => action, where
// an action is 'hold' or { status, payload }.
async function wireFixture(t, respond = null, timeoutMs = 30000) {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-direct-'));
  const stateRoot = join(home, 'outboxes');
  const configPath = join(home, 'otel.json');
  const bodies = [], held = [];
  const server = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    bodies.push(JSON.parse(body));
    const action = typeof respond === 'function' ? await respond(bodies.length, bodies[bodies.length - 1]) : respond;
    if (action === 'hold') held.push(res);
    else {
      const { status = 200, payload = '{}' } = action ?? {};
      res.writeHead(status, { 'content-type': 'application/json' });
      res.end(payload);
    }
  });
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(home, { recursive: true, force: true });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  await writeFile(configPath, JSON.stringify({
    endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs,
  }), { mode: 0o600 });
  return { home, stateRoot, configPath, bodies, held,
    releaseAck() {
      for (const res of held.splice(0)) { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); }
    },
    harness: options => harness({ telemetry: undefined, configPath, stateRoot, ...options }),
    spans: () => bodies.flatMap(body => body.resourceSpans ?? []).flatMap(resource => resource.scopeSpans ?? []).flatMap(scope => scope.spans ?? []),
  };
}

async function settledRequests(f, expected) {
  await until(() => f.bodies.length >= expected, 'direct OMP HTTP delivery');
  await delay(100);
  assert.equal(f.bodies.length, expected);
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
    root.event('input', { source: 'interactive', text: secret });
    root.event('before_agent_start', { prompt: secret });
    const nativeMessage = message(1_700_000_000_000);
    root.event('message_start', { message: nativeMessage });
    root.event('message_update', { message: nativeMessage, assistantMessageEvent: {
      type: 'text_delta', delta: secret, contentIndex: 0, partial: nativeMessage,
    } });
    root.event('message_end', { message: nativeMessage });
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
    assert.equal(root.emitted.filter(e => e.kind === 'interaction').length, 1);
    assert.ok(![...root.emitted, ...child.emitted].some(e => Object.keys(e.attributes).some(k => k.startsWith('gen_ai.usage.'))));
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
  await bounded(h.event('session_shutdown'), 'OMP direct shutdown');
  await settledRequests(f, 3);
  const spans = f.spans();
  assert.equal(spans.filter(span => span.name === 'coding_agent.llm.turn').length, 1);
  assert.equal(spans.filter(span => span.name === 'coding_agent.tool.call').length, 1);
  assert.ok(!JSON.stringify(f.bodies).includes(secret));
  assert.ok(!JSON.stringify(f.bodies).includes('/private/'));
});

test('OMP HTTP rejection attempts each event once without a second notification send', async t => {
  const f = await wireFixture(t, { status: 500, payload: '{}' });
  const h = f.harness();
  const m = message();
  assert.equal(h.event('message_end', { message: m }), undefined);
  assert.equal(h.event('tool_execution_start', { toolCallId: 'rejected-tool', toolName: 'read' }), undefined);
  assert.equal(h.event('tool_execution_end', { toolCallId: 'rejected-tool', toolName: 'read' }), undefined);
  // Duplicate notifications for already-attempted identities must not resend.
  assert.equal(h.event('message_end', { message: structuredClone(m) }), undefined);
  assert.equal(h.event('tool_execution_end', { toolCallId: 'rejected-tool', toolName: 'write', isError: true }), undefined);
  await bounded(h.event('session_shutdown'), 'OMP direct shutdown after rejection');
  await settledRequests(f, 3);
});

test('OMP collector partial success is a rejection and never retried on duplicate notification', async t => {
  const f = await wireFixture(t, { status: 200, payload: '{"partialSuccess":{"rejectedSpans":"1"}}' });
  const h = f.harness();
  const m = message();
  assert.equal(h.event('message_end', { message: m }), undefined);
  assert.equal(h.event('message_end', { message: structuredClone(m) }), undefined);
  await bounded(h.event('session_shutdown'), 'OMP direct shutdown after partial success');
  await settledRequests(f, 2);
});

test('OMP shutdown waits for held collector ACKs only until the configured timeout', async t => {
  const f = await wireFixture(t, 'hold', 300);
  const h = f.harness();
  assert.equal(h.event('message_end', { message: message() }), undefined);
  assert.equal(h.event('tool_execution_start', { toolCallId: 'held-tool', toolName: 'read' }), undefined);
  assert.equal(h.event('tool_execution_end', { toolCallId: 'held-tool', toolName: 'read' }), undefined);
  await until(() => f.bodies.length > 0, 'collector received a direct request');
  const startedAt = Date.now();
  await bounded(h.event('session_shutdown'), 'shutdown waits until the configured timeout', 10000);
  assert.ok(Date.now() - startedAt >= 250);
  assert.ok(f.held.length > 0);
  f.releaseAck();
});

test('OMP bounds slow Git metadata without blocking tool hooks', async t => {
  const f = await wireFixture(t);
  const h = f.harness({
    repositoryContext: () => delay(3000, { 'vcs.ref.head.name': 'slow-branch' }),
  });
  assert.equal(h.event('message_end', { message: message() }), undefined);
  assert.equal(h.event('tool_execution_start', { toolCallId: 'slow-tool', toolName: 'read' }), undefined);
  assert.equal(h.event('tool_execution_end', { toolCallId: 'slow-tool', toolName: 'read' }), undefined);
  await bounded(h.event('session_shutdown'), 'shutdown exceeded its metadata budget', 10000);
  await settledRequests(f, 3);
  const llm = f.spans().find(span => span.name === 'coding_agent.llm.turn');
  const attrs = Object.fromEntries(llm.attributes.map(attr => [attr.key, attr.value.intValue]));
  assert.equal(Number(attrs['gen_ai.usage.input_tokens']), 21);
  assert.equal(Number(attrs['gen_ai.usage.output_tokens']), 23);
});

test('OMP direct mode leaves a legacy outbox byte-unchanged', async t => {
  const f = await wireFixture(t);
  const legacy = join(f.stateRoot, 'legacy-route');
  await mkdir(legacy, { recursive: true });
  const obstacle = join(legacy, 'queue.sqlite');
  await writeFile(obstacle, Buffer.from([0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x00]));
  const before = await readFile(obstacle);
  const h = f.harness();
  assert.equal(h.event('message_end', { message: message() }), undefined);
  await bounded(h.event('session_shutdown'), 'OMP direct shutdown beside legacy state');
  await settledRequests(f, 2);
  assert.deepEqual(await readFile(obstacle), before);
  assert.deepEqual(await readdir(legacy), ['queue.sqlite']);
});

test('OMP failed sends are attempted once with immutable first-projected metadata', async () => {
  for (const failure of [false, new Error('synthetic send failure')]) {
    const first = new Map();
    let attempts = 0;
    let resolveRepository;
    const repository = new Promise(resolve => { resolveRepository = resolve; });
    const h = harness({
      repositoryContext: () => repository,
      telemetry: {
        async send(event) {
          if (event.kind === 'session') return true;
          attempts++;
          if (!first.has(event.eventId)) first.set(event.eventId, structuredClone(event));
          if (failure instanceof Error) throw failure;
          return failure;
        },
        async flush() {},
      },
    });
    const m = message();
    m.usage = structuredClone(usage);
    assert.equal(h.event('message_end', { message: m }), undefined);
    assert.equal(h.event('tool_execution_start', { toolCallId: 'immutable-tool', toolName: 'read', args: { secret } }), undefined);
    assert.equal(h.event('tool_execution_end', { toolCallId: 'immutable-tool', toolName: 'read', result: { content: secret } }), undefined);
    resolveRepository({ 'coding_agent.repository.id': 'a'.repeat(64) });
    await until(() => first.size === 2, 'projected events fail acceptance');
    await new Promise(resolve => setImmediate(resolve));
    // Duplicate notifications for already-attempted identities never resend,
    // so the first-projected payloads stay immutable despite later mutations.
    m.usage.input = 999;
    m.completedAt += 1000;
    h.event('message_end', { message: m });
    h.event('tool_execution_end', { toolCallId: 'immutable-tool', toolName: 'write', isError: true });
    await h.event('session_shutdown');
    assert.equal(attempts, 2);
    const delivered = [...first.values()];
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

test('OMP installer emits no runtime profile and migrates only same-config legacy loaders', async () => {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-install-'));
  const env = { OMP_PROFILE: 'work', PI_CODING_AGENT_DIR: '/unrelated/agent' };
  const dir = join(home, '.omp/profiles/work/agent');
  await mkdir(join(dir, 'extensions'), { recursive: true });
  const settings = '# preserve comments\nmodel: my-private-model\nextensions:\n  - ./other.js\n';
  await writeFile(join(dir, 'config.yml'), settings);
  await writeFile(join(dir, 'extensions/other.js'), 'export default () => {};\n');
  const extensionURL = new URL('../plugins/omp/index.mjs', import.meta.url).href;
  const configPath = join(home, 'otel.json');
  const legacyFor = profile => '// Managed by doda-datalake OMP telemetry installer.\n'
    + `import extension from ${JSON.stringify(extensionURL)};\n`
    + `export default pi => extension(pi, ${JSON.stringify({ configPath, profile })});\n`;
  let probeSeq = 0;
  const receivedOptions = async path => {
    const stub = join(home, `probe-extension-${probeSeq}.mjs`);
    const probe = join(home, `probe-loader-${probeSeq++}.mjs`);
    await writeFile(stub, 'export default (pi, options) => { globalThis.__ompProbeOptions = options; };\n');
    const text = (await readFile(path, 'utf8')).replaceAll(extensionURL, pathToFileURL(stub).href);
    assert.ok(!text.includes(extensionURL));
    await writeFile(probe, text);
    const mod = await import(pathToFileURL(probe).href);
    globalThis.__ompProbeOptions = undefined;
    await mod.default({ on() {}, events: { on() { return () => {}; } } });
    return globalThis.__ompProbeOptions;
  };
  try {
    const args = ['--home', home, '--config', configPath];
    const preview = await install(args, env);
    await assert.rejects(access(preview.path), { code: 'ENOENT' });
    const first = await install([...args, '--apply'], env);
    assert.equal(first.changed, true);
    assert.deepEqual(await receivedOptions(first.path), { configPath });
    const second = await install([...args, '--apply'], env);
    assert.equal(second.changed, false);
    await writeFile(first.path, legacyFor(dir), { mode: 0o600 });
    assert.deepEqual(await receivedOptions(first.path), { configPath, profile: dir });
    await install(args, env);
    const migrated = await install([...args, '--apply'], env);
    assert.equal(migrated.changed, true);
    assert.deepEqual(await receivedOptions(first.path), { configPath });
    assert.equal((await install([...args, '--apply'], env)).changed, false);
    await assert.rejects(install(['--home', home, '--config', join(home, 'another.json'), '--apply'], env));
    assert.deepEqual(await receivedOptions(first.path), { configPath });
    await writeFile(first.path, legacyFor(join(home, '.omp/agent')), { mode: 0o600 });
    await assert.rejects(install([...args, '--apply'], env));
    assert.equal(await readFile(first.path, 'utf8'), legacyFor(join(home, '.omp/agent')));
    await writeFile(first.path, legacyFor(dir), { mode: 0o600 });
    assert.equal((await install([...args, '--apply'], env)).changed, true);
    assert.deepEqual(await receivedOptions(first.path), { configPath });
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
