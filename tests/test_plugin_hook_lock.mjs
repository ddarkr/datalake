import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile, writeFile, rm, utimes } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';
import { handleHook } from '../plugins/claude-code/hook.mjs';
import { withStateLock } from '../plugins/otel.mjs';
import { processIncarnation } from '../plugins/outbox.mjs';

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
  await withStateLock(path, () => writeFile(checkpoint, 'next locally accepted checkpoint'));
  assert.equal(await readFile(checkpoint, 'utf8'), 'next locally accepted checkpoint');
});

test('Source locks publish the shared incarnation and recover stale starts and boots without advancing a legacy live owner', async t => {
  const path = await sandbox(t);
  const incarnation = await processIncarnation();
  assert.ok(incarnation);
  await withStateLock(path, async () => {
    const published = JSON.parse(await readFile(join(path, 'hook.lock', 'owner.json'), 'utf8'));
    assert.equal(published.pid, process.pid);
    assert.deepEqual(published.incarnation, incarnation);
  });
  const lock = join(path, 'hook.lock');
  for (const stale of [{ ...incarnation, boot: randomUUID() },
    { ...incarnation, start: incarnation.platform === 'linux' ? String(BigInt(incarnation.start) + 1n)
      : `${BigInt(incarnation.start.split('.')[0]) + 1n}.${incarnation.start.split('.')[1]}` }]) {
    await mkdir(lock, { mode: 0o700 });
    await writeFile(join(lock, 'owner.json'), JSON.stringify({ pid: process.pid, token: randomUUID(), incarnation: stale }), { mode: 0o600 });
    assert.equal(await withStateLock(path, () => 'recovered'), 'recovered');
  }
  await mkdir(lock, { mode: 0o700 });
  const legacy = JSON.stringify({ pid: process.pid });
  await writeFile(join(lock, 'owner.json'), legacy, { mode: 0o600 });
  assert.equal(await withStateLock(path, () => assert.fail('live legacy owner must not be evicted')), undefined);
  assert.equal(await readFile(join(lock, 'owner.json'), 'utf8'), legacy);
  await writeFile(join(lock, 'owner.json'), JSON.stringify({ pid: process.pid, incarnation }), { mode: 0o600 });
  await assert.rejects(withStateLock(path, () => assert.fail('partial modern owner must not be accepted')));
  const departed = await run(process.execPath, ['-e', 'console.log(process.pid)']);
  await writeFile(join(lock, 'owner.json'), JSON.stringify({ pid: Number(departed.stdout.trim()) }), { mode: 0o600 });
  assert.equal(await withStateLock(path, () => 'legacy migrated'), 'legacy migrated');
});

test('Source owner reads reject a regular-file-to-FIFO race within a deadline', { skip: process.platform === 'win32' }, async t => {
  const path = await sandbox(t);
  const moduleUrl = new URL('../plugins/otel.mjs', import.meta.url).href;
  const result = await run(process.execPath, ['--input-type=module', '-e', `
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { withStateLock } from ${JSON.stringify(moduleUrl)};
let entered, release;
const started = new Promise(resolve => { entered = resolve; });
const blocked = new Promise(resolve => { release = resolve; });
const active = withStateLock(${JSON.stringify(path)}, async () => { entered(); await blocked; });
await started;
const target = ${JSON.stringify(join(path, 'hook.lock', 'owner.json'))};
const originalOwner = await fs.readFile(target, 'utf8');
const originalOpen = fs.open;
let replaced = false;
fs.open = async (...args) => {
  if (args[0] === target && !replaced) {
    replaced = true;
    await fs.unlink(target);
    await promisify(execFile)('/usr/bin/mkfifo', ['-m', '600', target], { timeout: 1000 });
  }
  return originalOpen(...args);
};
await assert.rejects(withStateLock(${JSON.stringify(path)}, () => assert.fail('unsafe owner accepted')));
assert.equal(replaced, true);
fs.open = originalOpen;
await fs.unlink(target);
await fs.writeFile(target, originalOwner, { mode: 0o600 });
release();
await active;
console.log('bounded unsafe-owner rejection');`], { timeout: 5000, killSignal: 'SIGKILL', maxBuffer: 2048 });
  assert.equal(result.stdout.trim(), 'bounded unsafe-owner rejection');
});

test('Concurrent hooks recover a crashed owner and drain usage once', async t => {
  const path = await sandbox(t), stateRoot = join(path, 'state'), transcript = join(path, 'active.jsonl'), accepted = [];
  const invoke = hook_event_name => handleHook({ session_id: 'session_1', hook_event_name, transcript_path: transcript }, {
    stateRoot, now: 1000, context: async () => ({}), telemetry: { enqueue: async event => { accepted.push(event); return true; }, flushLocal: async () => {} },
  });
  await writeFile(transcript, '');
  await invoke('SessionStart');
  const directory = join(stateRoot, createHash('sha256').update('session_1').digest('hex'));
  const moduleUrl = new URL('../plugins/otel.mjs', import.meta.url).href;
  await run(process.execPath, ['--input-type=module', '-e', `import { withStateLock } from ${JSON.stringify(moduleUrl)}; await withStateLock(${JSON.stringify(directory)}, async () => { process.exit(0); });`]);
  await writeFile(transcript, JSON.stringify({ type: 'assistant', sessionId: 'session_1', timestamp: new Date(1500).toISOString(), message: { id: 'after_crash', model: 'claude-sonnet-4-6', stop_reason: 'end_turn', usage: { input_tokens: 20, output_tokens: 7 } } }) + '\n');
  await Promise.all([invoke('Stop'), invoke('Stop')]);
  await invoke('Stop');
  assert.deepEqual(accepted.map(event => event.eventId), ['message:after_crash']);
  assert.equal(accepted[0].attributes['gen_ai.usage.output_tokens'], 7);
});
