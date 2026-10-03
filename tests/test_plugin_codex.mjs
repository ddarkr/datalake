import assert from 'node:assert/strict';
import { execFile, spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { appendFile, mkdtemp, mkdir, readFile, readdir, rm, symlink, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import test from 'node:test';
import { handleHook } from '../plugins/codex/hook.mjs';
import { collectUsage } from '../plugins/codex/usage.mjs';
import { install, installationPlan } from '../plugins/codex/install.mjs';
import { serializeEvent } from '../plugins/otel.mjs';
import { openOutbox, readRoute } from '../plugins/outbox.mjs';

const execute = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const secret = 'PRIVATE_CONTENT_SENTINEL';
const timestamp = '2026-09-22T01:02:03.004Z';
const line = (type, payload) => JSON.stringify({ timestamp, type, payload }) + '\n';
const tokens = (input, output, cached = 0, reasoning = 0) => ({ input_tokens: input, cached_input_tokens: cached, output_tokens: output, reasoning_output_tokens: reasoning, total_tokens: input + output });
const usage = (total, last = total) => line('event_msg', { type: 'token_count', info: { total_token_usage: total, last_token_usage: last } });
const context = turn => line('turn_context', { turn_id: turn, model: 'gpt-test', cwd: secret, base_instructions: secret });
const metadata = id => line('session_meta', { id, session_id: id, model_provider: 'openai', cli_version: '0.142.4', base_instructions: secret });
const capture = () => { const events = []; return { events, telemetry: { enqueue: async event => { events.push(event); return true; }, flushLocal: async () => {} } }; };
async function until(predicate) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) return value;
    await delay(20);
  }
  assert.fail('Isolated asynchronous operation did not complete');
}
async function stopWorkers(directory) {
  let entries;
  try { entries = await readdir(directory, { withFileTypes: true }); }
  catch (error) { if (error.code === 'ENOENT') return; throw error; }
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const path = join(directory, entry.name);
    if (entry.name !== 'worker.lock') { await stopWorkers(path); continue; }
    try {
      const { pid } = JSON.parse(await readFile(join(path, 'owner.json'), 'utf8'));
      if (!Number.isSafeInteger(pid) || pid <= 0 || pid === process.pid) continue;
      const running = async () => {
        try {
          const { stdout } = await execute('ps', ['-p', String(pid), '-o', 'stat=', '-o', 'command='], { timeout: 5000 });
          const match = /^\s*(\S+)\s+([\s\S]*)$/.exec(stdout);
          return Boolean(match && !match[1].startsWith('Z') && match[2].includes('sender.mjs') && match[2].includes(directory));
        } catch (error) { if (error.code === 1 || error.code === 'ESRCH') return false; throw error; }
      };
      if (await running()) {
        try { process.kill(pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
        await until(async () => !await running());
      }
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
}
async function temporary(t) {
  const dir = await mkdtemp(join(tmpdir(), 'datalake-codex-'));
  t.after(async () => { try { await stopWorkers(dir); } finally { await rm(dir, { recursive: true, force: true }); } });
  return dir;
}
async function hookExecutable(executable, input, directory, extra = {}) {
  const env = {
    ...process.env, HOME: directory, CODEX_HOME: join(directory, '.codex'),
    XDG_STATE_HOME: join(directory, 'xdg-state'), PLUGIN_DATA: join(directory, 'state'), ...extra,
  };
  return await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [executable], { env, stdio: ['pipe', 'pipe', 'pipe'], timeout: 15_000 });
    let stdout = ''; let stderr = '';
    child.stdout.on('data', value => { stdout += value; });
    child.stderr.on('data', value => { stderr += value; });
    child.on('error', reject);
    child.on('close', (code, signal) => resolve({ code, signal, stdout, stderr }));
    child.stdin.end(JSON.stringify(input));
  });
}
async function drained(dataRoot, done) {
  const base = join(dataRoot, 'outbox');
  const roots = await readdir(base);
  const root = join(base, roots[0]);
  const outbox = await openOutbox({ root, route: await readRoute(root) });
  await until(async () => {
    const status = await outbox.status();
    return status.pending === 0 && status.done === done;
  });
  await until(async () => {
    try { await readFile(join(root, 'worker.lock', 'owner.json'), 'utf8'); return false; }
    catch (error) { if (error.code === 'ENOENT') return true; throw error; }
  });
}
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
  assert.deepEqual(await hook('PostToolUse', 1800, { tool_use_id: 'call-1', tool_name: 'Bash' }), []);
  assert.equal(events.length, 1);
  assert.equal(events[0].endTimeMs - events[0].startTimeMs, 300);
  await hook('SubagentStart', 1500, { agent_id: 'child-1', agent_type: 'worker' });
  await hook('SubagentStop', 2000, { agent_id: 'child-1', agent_type: 'worker' });
  await hook('Stop', 2100);
  assert.equal(events[1].attributes['coding_agent.agent.id'], 'child-1');
  assert.equal(events[1].attributes['coding_agent.subagent.duration_ms'], 500);
  assert.equal(events[1].attributes['coding_agent.subagent.status'], undefined);
  assert.equal(events[2].startTimeMs, 1000);
  assert.equal(events[2].endTimeMs, 2100);
  assert.equal(events[2].attributes['coding_agent.session.outcome'], undefined);
  assert.equal(JSON.stringify(events).includes(secret), false);
  assert.equal((await persisted(stateDir)).includes(secret), false);
});

test('metadata retries retain the first full event and accepted duplicates do not enqueue again', async t => {
  const stateDir = await temporary(t);
  const attempts = [];
  let outcome = false;
  const telemetry = { enqueue: async event => {
    attempts.push(structuredClone(event));
    if (outcome instanceof Error) throw outcome;
    return outcome;
  } };
  const input = { session_id: 'session-retry', turn_id: 'turn-1', hook_event_name: 'PostToolUse', tool_use_id: 'call-1', tool_name: 'Bash', model: 'first-model' };
  assert.deepEqual(await handleHook(input, { telemetry, stateDir, now: () => 1000 }), []);
  outcome = new Error('synthetic enqueue failure');
  assert.deepEqual(await handleHook({ ...input, model: 'later-model' }, { telemetry, stateDir, now: () => 2000 }), []);
  outcome = true;
  assert.deepEqual(await handleHook({ ...input, tool_name: 'changed-tool' }, { telemetry, stateDir, now: () => 3000 }), [attempts[0]]);
  assert.deepEqual(attempts, [attempts[0], attempts[0], attempts[0]]);
  assert.equal(attempts[0].startTimeMs, 1000);
  assert.equal(attempts[0].endTimeMs, 1000);
  assert.equal(attempts[0].attributes['gen_ai.request.model'], 'first-model');
  const acceptedState = await persisted(stateDir);
  assert.deepEqual(await handleHook(input, { telemetry, stateDir, now: () => 4000 }), []);
  assert.equal(attempts.length, 3);
  assert.equal(await persisted(stateDir), acceptedState);
});

test('held metadata enqueue does not block a distinct completion or duplicate its accepted wire body', { timeout: 5000 }, async t => {
  const dir = await temporary(t);
  const stateDir = join(dir, 'metadata');
  const queue = await openOutbox({ root: join(dir, 'outbox'), route: { key: '1'.repeat(64), endpointHash: '2'.repeat(64) } });
  const attempts = [];
  let release;
  let entered;
  const held = new Promise(resolve => { release = resolve; });
  const firstEnqueued = new Promise(resolve => { entered = resolve; });
  const telemetry = { enqueue: async event => {
    attempts.push(structuredClone(event));
    const accepted = await queue.put(serializeEvent('codex', event));
    if (event.eventId === 'session-start') { entered(); await held; }
    return accepted;
  } };
  const input = { session_id: 'session-held', model: 'first-model', prompt: secret };
  const start = { ...input, hook_event_name: 'SessionStart' };
  const completion = { ...input, hook_event_name: 'PostToolUse', turn_id: 'turn-1', tool_use_id: 'call-1', tool_name: 'Bash', tool_response: secret };
  const first = handleHook(start, { telemetry, stateDir, now: () => 1000 });
  let firstResult;
  try {
    await firstEnqueued;
    assert.equal((await queue.status()).pending, 1);
    assert.deepEqual(await handleHook(completion, { telemetry, stateDir, now: () => 2000 }), [attempts[1]]);
    assert.equal(attempts.length, 2, 'Both identities enqueue while the first is still held');
    assert.equal((await queue.status()).pending, 2);
    assert.deepEqual(await handleHook({ ...start, model: 'changed-model' }, { telemetry, stateDir, now: () => 3000 }), []);
    assert.deepEqual(await handleHook({ ...completion, tool_name: 'changed-tool' }, { telemetry, stateDir, now: () => 3000 }), []);
    assert.equal(attempts.length, 2, 'A held or accepted duplicate does not enqueue another body');
  } finally {
    release();
    firstResult = await first;
  }
  assert.deepEqual(firstResult, [attempts[0]]);
  const acceptedState = await persisted(stateDir);
  assert.deepEqual(await handleHook({ ...start, model: 'changed-model' }, { telemetry, stateDir, now: () => 4000 }), []);
  assert.deepEqual(await handleHook({ ...completion, tool_name: 'changed-tool' }, { telemetry, stateDir, now: () => 4000 }), []);
  assert.equal(await persisted(stateDir), acceptedState);
  assert.equal(attempts.length, 2);
  const pending = await queue.pending();
  assert.deepEqual(pending.map(event => event.body).sort(), attempts.map(event => serializeEvent('codex', event).body).sort());
  assert.equal(pending.some(event => event.body.includes(secret)), false);
  for (const event of pending) await queue.finish(event.identity, true);
  assert.equal((await queue.status()).done, 2);
  assert.deepEqual(await queue.pending(), []);
});

test('event-scoped handoff retains existing source snapshots and queued receipts', async t => {
  const stateDir = await temporary(t);
  const hash = value => createHash('sha256').update(value).digest('hex');
  const sessionId = 'session-migrated';
  const directory = join(stateDir, hash(sessionId));
  await mkdir(directory);
  const path = key => join(directory, `${hash(key)}.json`);
  const source = { kind: 'session', sessionId, eventId: 'session-start', startTimeMs: 1000, endTimeMs: 1000, attributes: { 'gen_ai.request.model': 'first-model' } };
  const queued = { version: 1, queued: true };
  await writeFile(path('session-start'), JSON.stringify(1000), { mode: 0o600 });
  await writeFile(path('event:session-start'), JSON.stringify(source), { mode: 0o600 });
  await writeFile(path('queued:session-start'), JSON.stringify(queued), { mode: 0o600 });
  const tool = { kind: 'tool.call', sessionId, eventId: 'tool:root:turn-1:call-1', startTimeMs: 2000, endTimeMs: 2000, attributes: { 'gen_ai.tool.call.id': 'call-1', 'gen_ai.tool.name': 'Bash' } };
  await writeFile(path(`event:${tool.eventId}`), JSON.stringify(tool), { mode: 0o600 });
  const { telemetry, events } = capture();
  assert.deepEqual(await handleHook({ session_id: sessionId, hook_event_name: 'SessionStart', model: 'changed-model' }, { telemetry, stateDir, now: () => 3000 }), []);
  assert.deepEqual(await handleHook({ session_id: sessionId, hook_event_name: 'PostToolUse', turn_id: 'turn-1', tool_use_id: 'call-1', tool_name: 'changed-tool' }, { telemetry, stateDir, now: () => 4000 }), [tool]);
  assert.deepEqual(events, [tool]);
  assert.deepEqual(JSON.parse(await readFile(path('event:session-start'), 'utf8')), source);
  assert.deepEqual(JSON.parse(await readFile(path('queued:session-start'), 'utf8')), queued);
  assert.deepEqual(JSON.parse(await readFile(path(`queued:${tool.eventId}`), 'utf8')), queued);
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
  const prefix = metadata('session-1') + context('turn-1');
  await writeFile(transcript, prefix + usage(tokens(20, 5)));
  const attempts = [];
  const rejected = { enqueue: async event => { attempts.push(structuredClone(event)); return false; } };
  assert.deepEqual(await collectUsage(input, { telemetry: rejected, stateDir }), []);
  const failedState = await persisted(stateDir);
  const cursorName = (await readdir(stateDir)).find(name => name.endsWith('.json'));
  const failedCursor = JSON.parse(await readFile(join(stateDir, cursorName), 'utf8'));
  assert.deepEqual(failedCursor.total, [0, 0, 0, 0, 0]);
  assert.equal(failedCursor.offset, Buffer.byteLength(prefix));
  assert.deepEqual(await collectUsage(input, { telemetry: { enqueue: async event => { attempts.push(structuredClone(event)); throw new Error('synthetic enqueue failure'); } }, stateDir }), []);
  assert.equal(await persisted(stateDir), failedState);
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 1);
  assert.equal(events[0].attributes['gen_ai.usage.input_tokens'], 20);
  assert.deepEqual(attempts, [events[0], events[0]]);
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 1);
  await appendFile(transcript, usage(tokens(1000, 5), tokens(20, 5)));
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 1);
});

test('an old live usage owner retains its cursor lock while independent cursors remain available', async t => {
  const dir = await temporary(t);
  const transcript = join(dir, 'active.jsonl');
  const stateDir = join(dir, 'state');
  const { telemetry, events } = capture();
  const input = { session_id: 'session-1', turn_id: 'turn-1', hook_event_name: 'Stop', transcript_path: transcript };
  await writeFile(transcript, metadata('session-1') + context('turn-1'));
  await collectUsage(input, { telemetry, stateDir, prime: true });
  const cursorFile = (await readdir(stateDir)).find(name => name.endsWith('.json'));
  const cursorBefore = await readFile(join(stateDir, cursorFile), 'utf8');
  const lock = join(stateDir, `${cursorFile}.lock`, 'hook.lock');
  await mkdir(lock, { mode: 0o700 });
  const owner = JSON.stringify({ pid: process.pid });
  await writeFile(join(lock, 'owner.json'), owner, { mode: 0o600 });
  const old = new Date(1000);
  await utimes(join(stateDir, `${cursorFile}.lock`), old, old);
  await utimes(lock, old, old);
  await appendFile(transcript, usage(tokens(20, 5)));
  assert.deepEqual(await collectUsage(input, { telemetry, stateDir }), []);
  assert.equal(await readFile(join(stateDir, cursorFile), 'utf8'), cursorBefore);
  assert.equal(await readFile(join(lock, 'owner.json'), 'utf8'), owner);
  const independent = join(dir, 'independent.jsonl');
  await writeFile(independent, metadata('session-2') + context('turn-2') + usage(tokens(7, 2)));
  await collectUsage({ ...input, session_id: 'session-2', turn_id: 'turn-2', transcript_path: independent }, { telemetry, stateDir });
  assert.equal(events.length, 1);
  assert.equal(events[0].sessionId, 'session-2');
  await rm(lock, { recursive: true });
  await collectUsage(input, { telemetry, stateDir });
  assert.equal(events.length, 2);
  assert.equal(events[1].attributes['gen_ai.usage.input_tokens'], 20);
});

test('symlinked hook executable emits OTLP metadata and never changes Codex hook decisions', async t => {
  const dir = await temporary(t);
  const requests = [];
  let acknowledge = false;
  const reply = response => { response.writeHead(200, { 'content-type': 'application/json' }); response.end('{}'); };
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push({ path: request.url, body, response });
    if (acknowledge) reply(response);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  const configPath = join(dir, 'otel.json');
  await writeFile(configPath, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 30_000 }), { mode: 0o600 });
  const transcript = join(dir, 'active.jsonl');
  await writeFile(transcript, metadata('session-1') + context('turn-1') + usage(tokens(13, 1, 2)));
  const input = { hook_event_name: 'PostToolUse', session_id: 'session-1', turn_id: 'turn-1', tool_use_id: 'call-1', tool_name: 'Bash', model: 'gpt-test', tool_input: { command: secret }, tool_response: secret };
  input.transcript_path = transcript;
  const executable = join(dir, 'hook-entry.mjs');
  await symlink(join(root, 'plugins/codex/hook.mjs'), executable);
  const result = await hookExecutable(executable, input, dir, { DATALAKE_OTEL_CONFIG: configPath });
  assert.equal(result.code, 0);
  assert.deepEqual(JSON.parse(result.stdout), {});
  await until(() => requests.length > 0);
  acknowledge = true;
  for (const { response } of requests) reply(response);
  await until(() => requests.length === 2);
  await drained(join(dir, 'state'), 2);
  assert.ok(requests.every(request => request.path === '/v1/traces'));
  assert.equal(JSON.stringify(requests.map(({ path, body }) => ({ path, body }))).includes(secret), false);
  assert.equal(result.stderr.includes(secret), false);
  const exported = requests.flatMap(request => JSON.parse(request.body).resourceSpans.flatMap(resource => resource.scopeSpans.flatMap(scope => scope.spans)));
  const tool = exported.find(span => span.name === 'coding_agent.tool.call');
  assert.ok(tool);
  const expectedTool = serializeEvent('codex', { kind: 'tool.call', sessionId: 'session-1', eventId: 'tool:root:turn-1:call-1', startTimeMs: 0, endTimeMs: 0, attributes: {} }).identity;
  assert.equal(tool.traceId + tool.spanId, expectedTool);
  const llm = exported.find(span => span.name === 'coding_agent.llm.turn');
  const attrs = Object.fromEntries(llm.attributes.map(({ key, value }) => [key, value]));
  assert.deepEqual(attrs['gen_ai.usage.input_tokens'], { intValue: '13' });
  assert.deepEqual(attrs['gen_ai.usage.output_tokens'], { intValue: '1' });
  assert.deepEqual(attrs['gen_ai.usage.cache_read.input_tokens'], { intValue: '2' });
  const expectedUsage = serializeEvent('codex', { kind: 'llm.turn', sessionId: 'session-1', eventId: 'usage:session-1:turn-1:13:2:1:0:14', startTimeMs: Date.parse(timestamp), endTimeMs: Date.parse(timestamp), attributes: {} }).identity;
  assert.equal(llm.traceId + llm.spanId, expectedUsage);
  const bodies = requests.map(request => request.body);
  const duplicate = await hookExecutable(executable, { ...input, model: 'changed-model', tool_name: 'changed-tool' }, dir, { DATALAKE_OTEL_CONFIG: configPath });
  assert.equal(duplicate.code, 0);
  assert.deepEqual(JSON.parse(duplicate.stdout), {});
  await drained(join(dir, 'state'), 2);
  assert.deepEqual(requests.map(request => request.body), bodies);
});

test('installed sources deliver without repository modules and native mode leaves telemetry untouched', async t => {
  const dir = await temporary(t);
  const home = join(dir, 'home');
  const codexHome = join(home, '.codex');
  await mkdir(codexHome, { recursive: true });
  const toml = '# keep my comment\nmodel = "gpt-test"\napproval_policy = "on-request"\n\n[otel]\nexporter = "none" # keep my own source\n\n[plugins."unrelated@example"]\nenabled = false\n';
  const configFile = join(codexHome, 'config.toml');
  await writeFile(configFile, toml);
  const binary = join(dir, 'isolated-codex');
  await writeFile(binary, `#!${process.execPath}\nprocess.exit(0);\n`, { mode: 0o700 });
  const requests = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push(body);
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end('{}');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  const configPath = join(dir, 'private-config.json');
  await writeFile(configPath, JSON.stringify({ endpoint: `http://127.0.0.1:${server.address().port}`, timeoutMs: 30_000 }), { mode: 0o600 });
  const args = ['--home', home, '--config', configPath, '--codex', binary, '--apply'];
  const plan = await install(args);
  assert.equal(await readFile(configFile, 'utf8'), toml);
  const executable = join(plan.pluginRoot, 'codex', 'hook.mjs');
  const input = { hook_event_name: 'SessionStart', session_id: 'installed-session', model: 'gpt-test', prompt: secret };
  const result = await hookExecutable(executable, input, home);
  assert.equal(result.code, 0);
  assert.deepEqual(JSON.parse(result.stdout), {});
  await until(() => requests.length === 1);
  await drained(join(home, 'state'), 1);
  const span = JSON.parse(requests[0]).resourceSpans[0].scopeSpans[0].spans[0];
  assert.equal(span.name, 'coding_agent.session');
  const expected = serializeEvent('codex', { kind: 'session', sessionId: 'installed-session', eventId: 'session-start', startTimeMs: 0, endTimeMs: 0, attributes: {} }).identity;
  assert.equal(span.traceId + span.spanId, expected);
  assert.equal(requests[0].includes(secret), false);
  const previous = await persisted(join(home, 'state', 'metadata'));
  const neutral = await hookExecutable(executable, {}, home);
  assert.equal(neutral.code, 0);
  assert.deepEqual(JSON.parse(neutral.stdout), {});
  assert.equal(await persisted(join(home, 'state', 'metadata')), previous);
  assert.equal(requests.length, 1);
  await install([...args, '--source', 'native']);
  const native = await hookExecutable(executable, { ...input, session_id: 'native-session' }, home);
  assert.equal(native.code, 0);
  assert.deepEqual(JSON.parse(native.stdout), {});
  assert.equal(await persisted(join(home, 'state', 'metadata')), previous);
  assert.equal(requests.length, 1);
  assert.equal(await readFile(configFile, 'utf8'), toml);
  await install([...args, '--uninstall']);
  assert.equal(await readFile(configFile, 'utf8'), toml);
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
