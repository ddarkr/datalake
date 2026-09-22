import assert from 'node:assert/strict';
import { execFile, spawn, spawnSync } from 'node:child_process';
import { createServer } from 'node:http';
import { appendFile, mkdtemp, mkdir, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import test from 'node:test';
import { handleHook } from '../plugins/codex/hook.mjs';
import { collectUsage } from '../plugins/codex/usage.mjs';
import { install, installationPlan } from '../plugins/codex/install.mjs';

const execute = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const secret = 'PRIVATE_CONTENT_SENTINEL';
const timestamp = '2026-09-22T01:02:03.004Z';
const line = (type, payload) => JSON.stringify({ timestamp, type, payload }) + '\n';
const tokens = (input, output, cached = 0, reasoning = 0) => ({ input_tokens: input, cached_input_tokens: cached, output_tokens: output, reasoning_output_tokens: reasoning, total_tokens: input + output });
const usage = (total, last = total) => line('event_msg', { type: 'token_count', info: { total_token_usage: total, last_token_usage: last } });
const context = turn => line('turn_context', { turn_id: turn, model: 'gpt-test', cwd: secret, base_instructions: secret });
const metadata = id => line('session_meta', { id, session_id: id, model_provider: 'openai', cli_version: '0.142.4', base_instructions: secret });
const capture = () => { const events = []; return { events, telemetry: { emit: async event => { events.push(event); return true; }, flush: async () => {} } }; };
async function temporary(t) { const dir = await mkdtemp(join(tmpdir(), 'datalake-codex-')); t.after(() => rm(dir, { recursive: true, force: true })); return dir; }
async function persisted(dir) { let text = ''; for (const entry of await readdir(dir, { withFileTypes: true })) text += entry.isDirectory() ? await persisted(join(dir, entry.name)) : await readFile(join(dir, entry.name), 'utf8'); return text; }

// Exercises native hook boundaries rather than making text/argument/result content a metric.
test('hook lifecycles retain stable identities and reject content and invented outcomes', async t => {
  const stateDir = await temporary(t);
  const { telemetry, events } = capture();
  const input = { session_id: 'session-1', turn_id: 'turn-1', model: 'gpt-test', cwd: secret, prompt: secret, last_assistant_message: secret, tool_input: { command: secret }, tool_response: secret };
  const hook = (hook_event_name, time, extra = {}) => handleHook({ ...input, hook_event_name, ...extra }, { telemetry, stateDir, now: () => time });
  await hook('UserPromptSubmit', 1000);
  await hook('PreToolUse', 1100, { tool_use_id: 'call-1', tool_name: 'Bash' });
  await hook('PostToolUse', 1400, { tool_use_id: 'call-1', tool_name: 'Bash' });
  await hook('PostToolUse', 1800, { tool_use_id: 'call-1', tool_name: 'Bash' });
  assert.deepEqual(events[0], events[1]);
  assert.equal(events[0].endTimeMs - events[0].startTimeMs, 300);
  await hook('SubagentStart', 1500, { agent_id: 'child-1', agent_type: 'worker' });
  await hook('SubagentStop', 2000, { agent_id: 'child-1', agent_type: 'worker' });
  await hook('Stop', 2100);
  assert.equal(events[2].attributes['coding_agent.agent.id'], 'child-1');
  assert.equal(events[2].attributes['coding_agent.subagent.duration_ms'], 500);
  assert.equal(events[2].attributes['coding_agent.subagent.status'], undefined);
  assert.equal(events[3].startTimeMs, 1000);
  assert.equal(events[3].endTimeMs, 2100);
  assert.equal(events[3].attributes['coding_agent.session.outcome'], undefined);
  assert.equal(JSON.stringify(events).includes(secret), false);
  assert.equal((await persisted(stateDir)).includes(secret), false);
});

test('numeric rollout increments are deduplicated, incremental and limited to the active native turn', async t => {
  const dir = await temporary(t);
  const transcript = join(dir, 'active.jsonl');
  const stateDir = join(dir, 'state');
  const { telemetry, events } = capture();
  const old = tokens(100, 10, 20, 2);
  const next = tokens(130, 15, 24, 3);
  await writeFile(transcript, metadata('session-1') + context('old-turn') + usage(old));
  const input = { session_id: 'session-1', turn_id: 'turn-1', hook_event_name: 'Stop', transcript_path: transcript, model: 'gpt-test' };
  await collectUsage(input, { telemetry, stateDir, prime: true });
  assert.deepEqual(events, []);
  await appendFile(transcript, context('turn-1') + line('response_item', { type: 'message', content: secret }) + usage(next, tokens(30, 5, 4, 1)) + usage(next, tokens(30, 5, 4, 1)));
  await collectUsage(input, { telemetry, stateDir });
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 1);
  assert.equal(events[0].attributes['gen_ai.usage.input_tokens'], 30);
  assert.equal(events[0].attributes['gen_ai.usage.output_tokens'], 5);
  assert.equal(events[0].attributes['gen_ai.usage.cache_read.input_tokens'], 4);
  assert.equal(events[0].attributes['gen_ai.usage.reasoning.output_tokens'], 1);
  assert.equal(events[0].startTimeMs, Date.parse(timestamp));
  assert.equal(events[0].endTimeMs, Date.parse(timestamp));
  const partial = usage(tokens(150, 20, 26, 4), tokens(20, 5, 2, 1));
  await appendFile(transcript, partial.slice(0, -1));
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 1);
  await appendFile(transcript, '\n');
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 2);
  assert.notEqual(events[0].eventId, events[1].eventId);
  assert.equal(events[1].attributes['gen_ai.usage.input_tokens'], 20);
  assert.equal(JSON.stringify(events).includes(secret), false);
  assert.equal((await persisted(stateDir)).includes(secret), false);
  assert.equal(Object.keys(events[0].attributes).some(key => /cost/i.test(key)), false);
});

test('failed exports remain retryable, mismatched sessions and synthetic counter adjustments are not billed', async t => {
  const dir = await temporary(t);
  const transcript = join(dir, 'active.jsonl');
  const stateDir = join(dir, 'state');
  const input = { session_id: 'session-1', turn_id: 'turn-1', hook_event_name: 'Stop', transcript_path: transcript };
  const { telemetry, events } = capture();
  await writeFile(transcript, metadata('someone-else') + context('turn-1') + usage(tokens(20, 5)));
  await collectUsage(input, { telemetry, stateDir });
  assert.deepEqual(events, []);
  await rm(stateDir, { recursive: true });
  await writeFile(transcript, metadata('session-1') + context('turn-1') + usage(tokens(20, 5)));
  await collectUsage(input, { telemetry: { emit: async () => false }, stateDir });
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 1);
  assert.equal(events[0].attributes['gen_ai.usage.input_tokens'], 20);
  await appendFile(transcript, usage(tokens(1000, 5), tokens(20, 5)));
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 1);
});

test('symlinked hook executable emits OTLP metadata and never changes Codex hook decisions', async t => {
  const dir = await temporary(t);
  const requests = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push({ path: request.url, body });
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end('{}');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const configPath = join(dir, 'otel.json');
  await writeFile(configPath, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 1000 }), { mode: 0o600 });
  const transcript = join(dir, 'active.jsonl');
  await writeFile(transcript, metadata('session-1') + context('turn-1') + usage(tokens(13, 1, 2)));
  const input = { hook_event_name: 'PostToolUse', session_id: 'session-1', turn_id: 'turn-1', tool_use_id: 'call-1', tool_name: 'Bash', model: 'gpt-test', tool_input: { command: secret }, tool_response: secret };
  input.transcript_path = transcript;
  const executable = join(dir, 'hook-entry.mjs');
  await symlink(join(root, 'plugins/codex/hook.mjs'), executable);
  const result = await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [executable], { env: { ...process.env, PLUGIN_DATA: join(dir, 'state'), DATALAKE_OTEL_CONFIG: configPath }, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = ''; let stderr = '';
    child.stdout.on('data', value => { stdout += value; });
    child.stderr.on('data', value => { stderr += value; });
    child.on('error', reject);
    child.on('exit', code => resolve({ code, stdout, stderr }));
    child.stdin.end(JSON.stringify(input));
  });
  assert.equal(result.code, 0);
  assert.deepEqual(JSON.parse(result.stdout), {});
  assert.equal(requests.length, 2);
  assert.equal(requests[0].path, '/v1/traces');
  assert.equal(JSON.stringify(requests).includes(secret), false);
  assert.equal(result.stderr.includes(secret), false);
  const exported = requests.flatMap(request => JSON.parse(request.body).resourceSpans.flatMap(resource => resource.scopeSpans.flatMap(scope => scope.spans)));
  assert.equal(exported[0].name, 'coding_agent.tool.call');
  const llm = exported.find(span => span.name === 'coding_agent.llm.turn');
  const attrs = Object.fromEntries(llm.attributes.map(({ key, value }) => [key, value]));
  assert.deepEqual(attrs['gen_ai.usage.input_tokens'], { intValue: '13' });
  assert.deepEqual(attrs['gen_ai.usage.output_tokens'], { intValue: '1' });
  assert.deepEqual(attrs['gen_ai.usage.cache_read.input_tokens'], { intValue: '2' });
});

test('native installer dry-run, apply and removal preserve unrelated TOML settings and comments', async t => {
  const binary = process.env.CODEX_BINARY ?? 'codex';
  const available = spawnSync(binary, ['--version'], { encoding: 'utf8' });
  if (available.error || available.status !== 0) { t.skip('Codex CLI is required for the native installer preservation check'); return; }
  const dir = await temporary(t);
  const codexHome = join(dir, '.codex');
  await mkdir(codexHome);
  const before = '# user comment must survive\nmodel = "gpt-test"\napproval_policy = "on-request"\n\n[otel]\nexporter = "none" # user telemetry choice\n\n[plugins."unrelated@example"]\nenabled = false\n';
  const configFile = join(codexHome, 'config.toml');
  await writeFile(configFile, before);
  const installer = join(root, 'plugins/codex/install.mjs');
  const args = [installer, '--home', dir, '--codex', binary];
  await execute(process.execPath, args, { cwd: dir });
  assert.equal(await readFile(configFile, 'utf8'), before);
  assert.deepEqual((await readdir(codexHome)).sort(), ['config.toml']);
  await execute(process.execPath, [...args, '--apply'], { cwd: dir });
  let config = await readFile(configFile, 'utf8');
  for (const preserved of ['# user comment must survive', 'model = "gpt-test"', 'approval_policy = "on-request"', 'exporter = "none" # user telemetry choice', '[plugins."unrelated@example"]', 'enabled = false']) assert.ok(config.includes(preserved));
  const env = { ...process.env, HOME: dir, CODEX_HOME: codexHome };
  const listed = JSON.parse((await execute(binary, ['plugin', 'list', '--json'], { cwd: dir, env })).stdout);
  assert.ok(listed.installed.some(plugin => plugin.name === 'doda-datalake-codex' && plugin.enabled));
  await execute(process.execPath, [...args, '--uninstall', '--apply'], { cwd: dir });
  config = await readFile(configFile, 'utf8');
  assert.ok(config.includes('[plugins."unrelated@example"]'));
  assert.ok(config.includes('exporter = "none" # user telemetry choice'));
  assert.ok(config.includes('# user comment must survive'));
  assert.equal(installationPlan(['--home', dir, '--source', 'native']).source, 'native');
  assert.throws(() => installationPlan(['--apply', '--dry-run']));
});

test('isolated Codex installation refuses a symlink into unrelated settings', async t => {
  const dir = await temporary(t);
  const outside = join(dir, 'unrelated');
  const home = join(dir, 'home');
  await mkdir(outside);
  await mkdir(home);
  await writeFile(join(outside, 'config.toml'), '# unrelated settings\n');
  await symlink(outside, join(home, '.codex'), 'dir');
  await assert.rejects(install(['--home', home, '--apply']), /심볼릭 링크/);
  assert.deepEqual(await readdir(outside), ['config.toml']);
  assert.equal(await readFile(join(outside, 'config.toml'), 'utf8'), '# unrelated settings\n');
});
