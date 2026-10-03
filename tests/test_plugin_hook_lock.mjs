import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, writeFile, rm, utimes } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';
import { handleHook } from '../plugins/claude-code/hook.mjs';
import { withStateLock } from '../plugins/otel.mjs';

const run = promisify(execFile);
async function sandbox(t) {
  const path = await mkdtemp(join(tmpdir(), 'datalake-hook-lock-'));
  t.after(() => rm(path, { recursive: true, force: true }));
  return path;
}

test('A live lock owner is not evicted by age or a contender timeout', async t => {
  const path = await sandbox(t), checkpoint = join(path, 'checkpoint.json');
  await writeFile(checkpoint, 'original checkpoint');
  let entered, release;
  const started = new Promise(resolve => { entered = resolve; });
  const blocked = new Promise(resolve => { release = resolve; });
  const active = withStateLock(path, async () => { entered(); await blocked; return 'owner finished'; });
  await started;
  const old = new Date(Date.now() - 60_000);
  await utimes(join(path, 'hook.lock'), old, old);
  try {
    assert.equal(await withStateLock(path, () => writeFile(checkpoint, 'incorrectly advanced')), undefined);
    assert.equal(await readFile(checkpoint, 'utf8'), 'original checkpoint');
  } finally { release(); await active; }
  await withStateLock(path, () => writeFile(checkpoint, 'next acknowledged checkpoint'));
  assert.equal(await readFile(checkpoint, 'utf8'), 'next acknowledged checkpoint');
});

test('Concurrent hooks recover a crashed owner and drain usage once', async t => {
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl'), acknowledged = [];
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry: { emit: async event => { acknowledged.push(event); return true; } },
  });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  const directory = join(stateRoot, createHash('sha256').update('session_1').digest('hex'));
  const moduleUrl = new URL('../plugins/otel.mjs', import.meta.url).href;
  await run(process.execPath, ['--input-type=module', '-e', `import { withStateLock } from ${JSON.stringify(moduleUrl)}; await withStateLock(${JSON.stringify(directory)}, async () => { process.exit(0); });`]);
  await writeFile(transcript, JSON.stringify({ type: 'assistant', sessionId: 'session_1', timestamp: new Date(1500).toISOString(), message: { id: 'after_crash', model: 'claude-sonnet-4-6', stop_reason: 'end_turn', usage: { input_tokens: 20, output_tokens: 7 } } }) + '\n');
  await Promise.all([invoke('Stop'), invoke('Stop')]);
  await invoke('Stop');
  assert.deepEqual(acknowledged.map(event => event.eventId), ['message:after_crash']);
  assert.equal(acknowledged[0].attributes['gen_ai.usage.output_tokens'], 7);
});
