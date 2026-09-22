import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, access, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
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
  const telemetry = { emit: async event => { emitted.push(event); return true; }, flush: async () => {} };
  let now = 1_700_000_000_000;
  ompTelemetry({
    on: (name, fn) => handlers.set(name, fn),
    events: { on: (name, fn) => { bus.set(name, fn); return () => bus.delete(name); } },
  }, { telemetry, env: {}, clock: () => now++, repositoryContext: () => ({}), ...options });
  return { emitted, ctx,
    async event(name, payload = {}) { await handlers.get(name)?.({ type: name, ...payload }, ctx); },
    async lifecycle(payload) { await bus.get('task:subagent:lifecycle')?.(payload); },
  };
}
function message(timestamp = 1_700_000_000_010) {
  return { role: 'assistant', provider: 'test-provider', model: 'test-model', timestamp,
    responseId: 'response-native', completedAt: timestamp + 50, usage, stopReason: 'stop',
    content: [{ type: 'text', text: secret }, { type: 'thinking', thinking: secret }],
    errorMessage: secret, providerPayload: { secret },
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

test('OMP metadata-only events reach OTLP without content, paths or tool arguments', async () => {
  const home = await mkdtemp(join(tmpdir(), 'omp-otel-wire-'));
  const bodies = [];
  const server = createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    bodies.push(JSON.parse(body));
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  try {
    const configPath = join(home, 'otel.json');
    await writeFile(configPath, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}` }), { mode: 0o600 });
    const h = harness({ telemetry: undefined, configPath });
    await h.event('session_start');
    await h.event('message_end', { message: message() });
    await h.event('tool_execution_start', { toolCallId: 'call_wire|fc_item', toolName: 'read', args: { secret } });
    await h.event('tool_execution_end', { toolCallId: 'call_wire|fc_item', toolName: 'read', result: { content: secret } });
    await h.event('session_shutdown');
    const spans = bodies.flatMap(body => body.resourceSpans ?? []).flatMap(resource => resource.scopeSpans ?? []).flatMap(scope => scope.spans ?? []);
    assert.equal(spans.filter(span => span.name === 'coding_agent.llm.turn').length, 1);
    assert.equal(spans.filter(span => span.name === 'coding_agent.tool.call').length, 1);
    assert.ok(!JSON.stringify(bodies).includes(secret));
    assert.ok(!JSON.stringify(bodies).includes('/private/'));
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(home, { recursive: true, force: true });
  }
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
