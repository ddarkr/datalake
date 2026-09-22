import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { createEventMapper } from '../plugins/opencode2/index.js';
import { install } from '../plugins/opencode2/install.mjs';

const native = (seq, type, data, created = 1000 + seq * 10) => ({
  id: `evt_${seq}`, created, type, durable: { aggregateID: data.sessionID, seq, version: 1 }, data,
});
const sessionID = 'ses_main';
const assistantMessageID = 'msg_answer';
const tokens = { input: 10, output: 4, reasoning: 2, cache: { read: 7, write: 3 } };
const pluginDirectory = resolve(dirname(fileURLToPath(import.meta.url)), '../plugins/opencode2');

test('v2 event lifecycle exports usage deltas once and excludes private content', () => {
  const map = createEventMapper({ 'service.name': 'opencode-cli' });
  const privateValue = 'PRIVATE-CONTENT-DO-NOT-EXPORT';
  const events = [
    native(1, 'session.created', { sessionID, title: privateValue, metadata: { secret: privateValue } }),
    native(2, 'session.execution.started', { sessionID }),
    native(3, 'session.step.started', { sessionID, assistantMessageID, started: 1025, agent: 'build', model: { id: 'test-model', providerID: 'openai' } }),
    native(4, 'session.text.delta', { sessionID, assistantMessageID, delta: privateValue }),
    native(5, 'session.tool.input.started', { sessionID, assistantMessageID, id: 'call_read', name: 'read' }),
    native(6, 'session.tool.called', { sessionID, assistantMessageID, id: 'call_read', input: { path: privateValue }, executed: true }),
    native(7, 'session.tool.success', { sessionID, assistantMessageID, id: 'call_read', content: [{ text: privateValue }], executed: true }),
    native(8, 'session.step.ended', { sessionID, assistantMessageID, finish: 'stop', tokens, cost: 0.012, files: [privateValue], providerState: { secret: privateValue } }),
    native(9, 'session.usage.updated', { sessionID, tokens, cost: 0.012 }),
    native(10, 'session.execution.succeeded', { sessionID }),
    native(11, 'session.usage.recorded', { sessionID, source: 'title', tokens: { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } }, cost: 0.001 }),
  ];
  const spans = events.flatMap(map);
  assert.equal(JSON.stringify(spans).includes(privateValue), false);
  assert.deepEqual(events.flatMap(map), []);
  assert.equal(spans.length, 4);
  const turn = spans.find(span => span.eventId === 'step:msg_answer');
  assert.equal(turn.attributes['gen_ai.usage.input_tokens'], 20);
  assert.equal(turn.attributes['gen_ai.usage.output_tokens'], 6);
  assert.equal(turn.attributes['gen_ai.usage.cache_read.input_tokens'], 7);
  assert.equal(turn.attributes.cost_usd, 0.012);
  assert.equal(turn.attributes['gen_ai.request.model'], 'test-model');
  assert.equal(turn.startTimeMs, 1025);
  assert.equal(turn.endTimeMs, 1080);
  assert.equal(turn.parentEventId, 'execution:evt_2');
  const tool = spans.find(span => span.kind === 'tool.call');
  assert.equal(tool.startTimeMs, 1060);
  assert.equal(tool.endTimeMs, 1070);
  assert.equal(tool.parentEventId, turn.eventId);
  assert.equal(tool.attributes['gen_ai.tool.name'], 'read');
  const session = spans.find(span => span.kind === 'session');
  assert.equal(session.startTimeMs, 1020);
  assert.equal(session.endTimeMs, 1100);
  assert.equal(session.attributes['coding_agent.session.outcome'], 'succeeded');
  assert.deepEqual(spans.filter(span => span.kind === 'llm.turn').map(span => span.attributes.cost_usd), [0.012, 0.001]);
});

test('interleaved sessions, repeated executions, failures and absent usage remain distinct', () => {
  const map = createEventMapper();
  map(native(1, 'session.created', { sessionID: 'ses_child', parentID: sessionID, agent: 'explore' }));
  map(native(2, 'session.execution.started', { sessionID: 'ses_child' }));
  map(native(1, 'session.execution.started', { sessionID }));
  map(native(3, 'session.step.started', { sessionID: 'ses_child', assistantMessageID: 'msg_failed', model: { id: 'm', providerID: 'p' } }));
  const [failed] = map(native(4, 'session.step.failed', { sessionID: 'ses_child', assistantMessageID: 'msg_failed', error: { message: 'SECRET' } }));
  assert.equal(failed.error, true);
  assert.equal('cost_usd' in failed.attributes, false);
  assert.equal('gen_ai.usage.input_tokens' in failed.attributes, false);
  assert.equal(JSON.stringify(failed).includes('SECRET'), false);
  const child = map(native(5, 'session.execution.failed', { sessionID: 'ses_child', error: { message: 'SECRET' } }));
  assert.equal(child[1].kind, 'subagent');
  assert.equal(child[1].sessionId, sessionID);
  assert.equal(child[1].attributes['coding_agent.agent.id'], 'ses_child');
  assert.equal(child[1].error, true);
  const [first] = map(native(2, 'session.execution.succeeded', { sessionID }));
  map(native(3, 'session.execution.started', { sessionID }, 10000));
  const [second] = map(native(4, 'session.execution.interrupted', { sessionID }, 10050));
  assert.notEqual(first.eventId, second.eventId);
  assert.equal(second.startTimeMs, 10000);
  assert.equal(second.attributes['coding_agent.session.duration_ms'], 50);
  assert.equal(second.attributes['coding_agent.session.outcome'], 'interrupted');
  const [midSession] = createEventMapper()(native(30, 'session.step.ended', { sessionID, assistantMessageID, tokens, cost: 0 }));
  assert.equal(midSession.startTimeMs, midSession.endTimeMs);
  assert.equal(midSession.attributes.cost_usd, 0);
});

test('installer defaults to dry-run and preserves JSONC, unrelated plugins and file mode', async t => {
  const home = await mkdtemp(join(tmpdir(), 'datalake-opencode2-'));
  t.after(() => rm(home, { recursive: true, force: true }));
  const target = join(home, '.config', 'opencode', 'opencode.jsonc');
  await mkdir(dirname(target), { recursive: true });
  const original = '{\n  // retain this comment\n  "model": "provider/model",\n  "plugins": [\n    /* existing plugin */ { "package": "existing-plugin", "options": { "enabled": true } },\n  ],\n}\n';
  await writeFile(target, original, { mode: 0o640 });
  const config = join(home, 'private-otel.json');
  const args = ['--home', home, '--config', config];
  assert.equal((await install(args)).apply, false);
  assert.equal(await readFile(target, 'utf8'), original);
  await install([...args, '--apply']);
  const installed = await readFile(target, 'utf8');
  assert.ok(installed.includes('// retain this comment'));
  assert.ok(installed.includes('/* existing plugin */'));
  assert.ok(installed.includes('"model": "provider/model"'));
  assert.ok(installed.includes('existing-plugin'));
  assert.ok(installed.includes(JSON.stringify(config)));
  assert.equal((await stat(target)).mode & 0o777, 0o640);
  assert.deepEqual((await install([...args, '--apply'])).changed, []);
  await install([...args, '--uninstall', '--apply']);
  const removed = await readFile(target, 'utf8');
  assert.ok(removed.includes('existing-plugin'));
  assert.ok(removed.includes('/* existing plugin */'));
  assert.equal(removed.includes(JSON.stringify(pluginDirectory)), false);
  assert.deepEqual((await install([...args, '--uninstall', '--apply'])).changed, []);
});

test('installer handles both native config files and refuses malformed/symlinked configs', async t => {
  const home = await mkdtemp(join(tmpdir(), 'datalake-opencode2-safety-'));
  t.after(() => rm(home, { recursive: true, force: true }));
  const configDirectory = join(home, '.config', 'opencode');
  await mkdir(configDirectory, { recursive: true });
  const json = join(configDirectory, 'opencode.json');
  const jsonc = join(configDirectory, 'opencode.jsonc');
  await writeFile(json, JSON.stringify({ plugins: [{ package: pluginDirectory, options: { preserved: true } }] }));
  await writeFile(jsonc, '{ "plugins": ["another-plugin"], /* comment */ }');
  const args = ['--home', home, '--apply'];
  await install(args);
  assert.equal(JSON.parse(await readFile(json, 'utf8')).plugins[0].options.preserved, true);
  assert.equal(await readFile(jsonc, 'utf8'), '{ "plugins": ["another-plugin"], /* comment */ }');
  await writeFile(jsonc, '{ "plugins": [] , INVALID }');
  const before = await readFile(json, 'utf8');
  await assert.rejects(install(args), /valid JSONC/);
  assert.equal(await readFile(json, 'utf8'), before);
  await rm(jsonc);
  const outside = join(home, 'outside.json');
  await writeFile(outside, '{}');
  await symlink(outside, jsonc);
  await assert.rejects(install(args), /symlinked/);
  assert.equal(await readFile(outside, 'utf8'), '{}');
});
