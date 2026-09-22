import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, appendFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { handleHook, usageEvent } from '../plugins/claude-code/hook.mjs';
import { install } from '../plugins/claude-code/install.mjs';

const secret = 'PRIVATE_PROMPT_RESPONSE_TOOL_SECRET';
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
    stateRoot: path, now, context: async () => ({}), telemetry: { emit: async event => { output.push(event); return true; } },
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
  await writeFile(transcript, JSON.stringify(record('historical', 500)) + '\n');
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot: join(path, 'state'), now: 1000, context: async () => ({}), telemetry: { emit: async event => { output.push(event); return true; } },
  });
  await invoke('SessionStart');
  const unrelated = record('foreign'); unrelated.sessionId = 'other_session';
  const line = JSON.stringify(record());
  await appendFile(transcript, JSON.stringify(unrelated) + '\n' + line + '\n' + line + '\n' + JSON.stringify(record('next')).slice(0, 50));
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
  await stat(join(destination, 'otel.mjs'));
  const config = JSON.parse(await readFile(join(destination, 'claude-code', 'config-path.json'), 'utf8'));
  assert.equal(config.configPath, join(home, 'private-otel.json'));
  await writeFile(join(destination, 'personal.txt'), 'keep me');
  await install([...args, '--uninstall', '--apply']);
  assert.equal(await readFile(join(destination, 'personal.txt'), 'utf8'), 'keep me');
  assert.equal(await readFile(settings, 'utf8'), original);
  await assert.rejects(stat(join(destination, '.claude-plugin', 'plugin.json')), { code: 'ENOENT' });
  await assert.rejects(install([...args, '--apply']), /소유하지 않은/);
});
