import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import test from 'node:test';
import { openOutbox, ownerIsDead, processIncarnation, readRoute } from '../plugins/outbox.mjs';

const moduleURL = new URL('../plugins/outbox.mjs', import.meta.url).href;
const route = { key: '1'.repeat(64), endpointHash: '2'.repeat(64) };
const fixture = (number = 1, time = '1780000000000000000') => {
  const identity = number.toString(16).padStart(48, '0');
  return { identity, body: JSON.stringify({ resourceSpans: [{
    resource: { attributes: [{ key: 'service.name', value: { stringValue: 'outbox-fixture' } }] },
    scopeSpans: [{ scope: { name: 'doda-datalake', version: '1.0.0' }, spans: [{
      traceId: identity.slice(0, 32), spanId: identity.slice(32), name: 'coding_agent.llm.turn',
      startTimeUnixNano: time, endTimeUnixNano: time, attributes: [],
    }] }],
  }] }) };
};

async function sandbox(t) {
  const path = await fs.mkdtemp(join(tmpdir(), 'datalake-outbox-'));
  t.after(() => fs.rm(path, { recursive: true, force: true }));
  return path;
}

function child(t, script) {
  const process = spawn(globalThis.process.execPath, ['--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  let stderr = '', ended = false;
  const messages = [], waiters = [];
  process.stderr.on('data', data => { stderr += data; });
  process.on('message', message => {
    if (waiters.length) waiters.shift().resolve(message);
    else messages.push(message);
  });
  const closed = new Promise((resolve, reject) => {
    process.once('error', error => {
      ended = true;
      for (const waiter of waiters.splice(0)) waiter.reject(error);
      reject(error);
    });
    process.once('close', (code, signal) => {
      ended = true;
      for (const waiter of waiters.splice(0)) waiter.reject(new Error(`Child closed before handshake: ${stderr}`));
      resolve({ code, signal, stderr });
    });
  });
  t.after(async () => {
    if (process.exitCode === null && process.signalCode === null) process.kill('SIGKILL');
    await closed;
  });
  return { process, closed, message: () => {
    if (messages.length) return Promise.resolve(messages.shift());
    if (ended) return Promise.reject(new Error(`Child already closed: ${stderr}`));
    return new Promise((resolve, reject) => waiters.push({ resolve, reject }));
  } };
}

const preamble = root => `import { openOutbox } from ${JSON.stringify(moduleURL)};
const outbox = await openOutbox({root:${JSON.stringify(root)},route:${JSON.stringify(route)}});`;

async function successful(child) {
  const result = await child.closed;
  assert.equal(result.code, 0, result.stderr);
}

test('First body and times survive duplicate, retry, delivery and reopen without tombstone pruning', async t => {
  const root = join(await sandbox(t), 'queue');
  const outbox = await openOutbox({ root, route });
  const event = fixture();
  const before = Date.now();
  assert.equal(await outbox.put(event), true);
  const [first] = await outbox.pending();
  assert.equal(first.body, event.body);
  assert.equal(first.attempts, 0);
  assert.ok(first.due >= before && first.due <= Date.now());
  assert.equal(await outbox.put(fixture(1, '1780000000000999999')), true);
  assert.deepEqual(await outbox.pending(), [first]);
  const originalNow = Date.now;
  try {
    Date.now = () => 1780000000000;
    for (let attempts = 1; attempts <= 9; attempts++) {
      await outbox.finish(event.identity, false);
      const [retry] = await outbox.pending();
      assert.equal(retry.body, event.body);
      assert.equal(retry.identity, event.identity);
      assert.equal(retry.attempts, attempts);
      assert.equal(retry.due, 1780000000000 + Math.min(60000, 1000 * 2 ** (attempts - 1)));
    }
  } finally { Date.now = originalNow; }
  const pendingBytes = (await outbox.status()).bytes;
  await outbox.finish(event.identity, true);
  await outbox.finish(event.identity, false);
  assert.deepEqual(await outbox.pending(), []);
  const done = await outbox.status();
  assert.equal(done.pending, 0);
  assert.equal(done.done, 1);
  assert.ok(done.bytes < pendingBytes);
  const tombstone = await fs.readFile(join(root, 'records', `${event.identity}.json`), 'utf8');
  assert.doesNotMatch(tombstone, /startTimeUnixNano|resourceSpans|outbox-fixture/);
  const reopened = await openOutbox({ root, route });
  assert.equal(await reopened.put(event), true);
  assert.equal(await reopened.put(fixture(1, '1780000000000999999')), true);
  assert.deepEqual(await reopened.status(), done);
  assert.deepEqual(await reopened.pending(), []);
});

test('Route and explicit capacity are pinned; pending plus done identities and actual record bytes are bounded', async t => {
  const base = await sandbox(t);
  const root = join(base, 'entries');
  const outbox = await openOutbox({ root, route, maxEntries: 2 });
  assert.deepEqual(await readRoute(root), route);
  await assert.rejects(openOutbox({ root, route: { ...route, endpointHash: '3'.repeat(64) } }));
  await assert.rejects(openOutbox({ root, route: { ...route, key: '4'.repeat(64) } }));
  await assert.rejects(openOutbox({ root, route, maxEntries: 3 }));
  assert.equal(await outbox.put(fixture(1)), true);
  assert.equal(await outbox.put(fixture(2)), true);
  await outbox.finish(fixture(1).identity, true);
  const reopened = await openOutbox({ root, route });
  assert.equal(await reopened.put(fixture(3)), false);
  assert.equal(await reopened.put(fixture(1)), true);
  const status = await reopened.status();
  assert.deepEqual({ pending: status.pending, done: status.done }, { pending: 1, done: 1 });
  const files = await fs.readdir(join(root, 'records'));
  const sizes = await Promise.all(files.map(name => fs.stat(join(root, 'records', name)).then(stat => stat.size)));
  assert.equal(status.bytes, sizes.reduce((sum, size) => sum + size, 0));

  const measure = await openOutbox({ root: join(base, 'measure'), route });
  assert.equal(await measure.put(fixture()), true);
  const bytes = (await measure.status()).bytes;
  const bounded = await openOutbox({ root: join(base, 'bytes'), route, maxBytes: bytes * 2 - 1 });
  assert.equal(await bounded.put(fixture(1)), true);
  assert.equal(await bounded.put(fixture(2)), false);
  const originalBytes = (await bounded.status()).bytes;
  await bounded.finish(fixture(1).identity, false);
  assert.equal((await bounded.status()).bytes, originalBytes);
  await bounded.finish(fixture(1).identity, true);
  assert.equal(await bounded.put(fixture(2)), true);
  assert.ok((await bounded.status()).bytes <= bytes * 2 - 1);

  const fileRoute = { ...route, configPath: join(base, 'nonsecret-profile.json') };
  const fileRoot = join(base, 'file-route');
  await openOutbox({ root: fileRoot, route: fileRoute });
  assert.deepEqual(await readRoute(fileRoot), fileRoute);
  await assert.rejects(openOutbox({ root: fileRoot, route }));
  await assert.rejects(openOutbox({ root: join(base, 'bad-route'), route: { ...route, headers: { Authorization: 'NEVER_PERSIST' } } }));
  await assert.rejects(openOutbox({ root: join(base, 'bad-path'), route: { ...route, configPath: 'relative.json' } }));
});

test('Identity, single-span relationship, bounded reads and private roots/files reject unsafe input', async t => {
  const base = await sandbox(t);
  const root = join(base, 'queue');
  const outbox = await openOutbox({ root, route });
  const event = fixture();
  for (const invalid of [
    { ...event, identity: 'A'.repeat(48) }, { ...event, identity: '0'.repeat(47) },
    { ...event, identity: fixture(2).identity }, { ...event, body: '{}' }, { ...event, body: 'not JSON' },
    { ...event, body: JSON.stringify({ resourceSpans: [{ scopeSpans: [{ spans: [JSON.parse(event.body).resourceSpans[0].scopeSpans[0].spans[0], {}] }] }] }) },
    { ...event, body: 'x'.repeat(1024 * 1024 + 1) },
  ]) assert.equal(await outbox.put(invalid), false);
  assert.deepEqual(await outbox.status(), { pending: 0, done: 0, bytes: 0 });
  assert.equal(await outbox.put(event), true);
  assert.equal((await fs.stat(root)).mode & 0o7777, 0o700);
  assert.equal((await fs.stat(join(root, 'records'))).mode & 0o7777, 0o700);
  for (const name of ['manifest.json', 'ledger.json', `records/${event.identity}.json`]) {
    assert.equal((await fs.stat(join(root, name))).mode & 0o7777, 0o600);
  }
  await fs.chmod(root, 0o755);
  await assert.rejects(openOutbox({ root, route }));
  await fs.chmod(root, 0o700);
  const alias = join(base, 'alias');
  await fs.symlink(root, alias);
  await assert.rejects(openOutbox({ root: alias, route }));

  const recordPath = join(root, 'records', `${event.identity}.json`);
  await fs.chmod(recordPath, 0o644);
  assert.equal(await outbox.put(event), false);
  await assert.rejects(outbox.pending());
  await fs.chmod(recordPath, 0o600);
  const external = join(base, 'external.json');
  await fs.rename(recordPath, external);
  await fs.symlink(external, recordPath);
  assert.equal(await outbox.put(event), false);
  await assert.rejects(outbox.pending());
  await fs.rm(recordPath);
  await fs.link(external, recordPath);
  assert.equal(await outbox.put(event), false);
  await assert.rejects(outbox.pending());
  await fs.rm(external);
  assert.equal(await outbox.put(event), true);
  await fs.writeFile(recordPath, ' '.repeat(1024 * 1024 + 1), { mode: 0o600 });
  await assert.rejects(outbox.pending());

  const manifestRoot = join(base, 'manifest-link');
  await openOutbox({ root: manifestRoot, route });
  const manifestPath = join(manifestRoot, 'manifest.json');
  const copiedManifest = join(base, 'manifest-copy.json');
  await fs.rename(manifestPath, copiedManifest);
  await fs.symlink(copiedManifest, manifestPath);
  await assert.rejects(readRoute(manifestRoot));
  await fs.rm(manifestPath);
  await fs.link(copiedManifest, manifestPath);
  await assert.rejects(openOutbox({ root: manifestRoot, route }));

  const originalLstat = fs.lstat;
  try {
    fs.lstat = async (...args) => {
      const stat = await originalLstat(...args);
      return args[0] === root ? new Proxy(stat, { get: (target, key) => key === 'uid' ? process.getuid() + 1 : Reflect.get(target, key) }) : stat;
    };
    await assert.rejects(openOutbox({ root, route }));
  } finally { fs.lstat = originalLstat; }
});

test('Real concurrent producers publish one immutable duplicate and count distinct entries exactly', async t => {
  const root = join(await sandbox(t), 'queue');
  await openOutbox({ root, route });
  const duplicate = fixture(1);
  const producers = Array.from({ length: 8 }, (_, index) => child(t, `${preamble(root)}
process.send('ready'); await new Promise(resolve => process.once('message', resolve));
if (!await outbox.put(${JSON.stringify(duplicate)}) || !await outbox.put(${JSON.stringify(fixture(index + 2))})) process.exit(2);
process.disconnect();`));
  await Promise.all(producers.map(producer => producer.message()));
  for (const producer of producers) producer.process.send('go');
  await Promise.all(producers.map(successful));
  const outbox = await openOutbox({ root, route });
  const pending = await outbox.pending();
  assert.equal(pending.length, 9);
  assert.equal(new Set(pending.map(entry => entry.identity)).size, 9);
  assert.equal(pending.find(entry => entry.identity === duplicate.identity).body, duplicate.body);
  assert.deepEqual({ ...(await outbox.status()), bytes: 0 }, { pending: 9, done: 0, bytes: 0 });
});

test('Producer turnover during directory or owner-file validation retries without losing accepted records', async t => {
  const base = await sandbox(t);
  for (const boundary of ['directory', 'owner-file']) {
    const root = join(base, boundary);
    const outbox = await openOutbox({ root, route });
    const lockPath = join(root, 'producer.lock');
    const producers = [1, 2].map(number => child(t, `${preamble(root)}
import fs from 'node:fs/promises';
process.send('ready'); await new Promise(resolve => process.once('message', resolve));
const originalRename = fs.rename;
fs.rename = async (from, to) => {
  await originalRename(from, to);
  if (to === ${JSON.stringify(lockPath)}) {
    process.send('owned'); await new Promise(resolve => process.once('message', resolve));
  }
};
if (!await outbox.put(${JSON.stringify(fixture(number))})) process.exit(2);
process.disconnect();`));
    await Promise.all(producers.map(producer => producer.message()));
    producers[0].process.send('start');
    assert.equal(await producers[0].message(), 'owned');
    const originalLstat = fs.lstat, originalOpen = fs.open;
    let injected = false, successorReleased = false;
    const replaceOwner = async () => {
      injected = true;
      producers[0].process.send('release');
      await successful(producers[0]);
      producers[1].process.send('start');
      assert.equal(await producers[1].message(), 'owned');
    };
    const releaseSuccessor = async () => {
      successorReleased = true;
      producers[1].process.send('release');
      await successful(producers[1]);
    };
    try {
      fs.lstat = async (...args) => {
        const stat = await originalLstat(...args);
        if (boundary === 'directory' && args[0] === lockPath && !injected) await replaceOwner();
        return stat;
      };
      fs.open = async (...args) => {
        const handle = await originalOpen(...args);
        if (boundary === 'owner-file' && args[0] === join(lockPath, 'owner.json') && !injected) {
          await replaceOwner(); // The real open owner file is unlinked while this handle remains alive.
          await releaseSuccessor();
        } else if (boundary === 'directory' && args[0] === lockPath && injected && !successorReleased) {
          await releaseSuccessor(); // lstat observed the first owner, while open observed its successor.
        }
        return handle;
      };
      assert.equal(await outbox.put(fixture(3)), true);
      assert.equal(injected, true);
    } finally { fs.lstat = originalLstat; fs.open = originalOpen; }
    assert.deepEqual((await outbox.pending()).map(entry => ({ identity: entry.identity, body: entry.body })), [1, 2, 3].map(number => fixture(number)));
    const status = await outbox.status();
    assert.deepEqual({ pending: status.pending, done: status.done }, { pending: 3, done: 0 });
  }
});

test('Live worker ownership survives age, does not block enqueue, and empty release has both enqueue boundaries', async t => {
  const root = join(await sandbox(t), 'queue');
  const outbox = await openOutbox({ root, route });
  const owner = child(t, `${preamble(root)}
const release = await outbox.acquireWorker(); if (!release) process.exit(2);
process.send('owned'); await new Promise(resolve => process.once('message', resolve));
await release(); process.disconnect();`);
  assert.equal(await owner.message(), 'owned');
  const old = new Date(1000);
  await fs.utimes(join(root, 'worker.lock'), old, old);
  assert.equal(await outbox.acquireWorker(), null);
  assert.equal(await outbox.put(fixture()), true); // Child ownership spans this operation without blocking it.
  owner.process.send('release');
  await successful(owner);
  const release = await outbox.acquireWorker();
  assert.equal(typeof release, 'function');
  assert.equal(await release.releaseIfEmpty(), false);
  await outbox.finish(fixture().identity, true);
  assert.equal(await release.releaseIfEmpty(), true);
  assert.equal(await outbox.put(fixture(2)), true); // An enqueue after atomic release starts a successor.
  const successor = await outbox.acquireWorker();
  assert.equal(typeof successor, 'function');
  assert.equal(await successor.releaseIfEmpty(), false);
  await outbox.finish(fixture(2).identity, true);
  assert.equal(await outbox.put(fixture(3)), true); // An enqueue before atomic release keeps the owner alive.
  assert.equal(await successor.releaseIfEmpty(), false);
  assert.equal(await outbox.acquireWorker(), null);
  await successor();
});

test('Recycled live PIDs and earlier boots cannot retain worker or producer ownership or erase the ledger', async t => {
  const root = join(await sandbox(t), 'queue');
  const outbox = await openOutbox({ root, route });
  assert.equal(await outbox.put(fixture()), true);
  const incarnation = await processIncarnation();
  assert.ok(incarnation, 'The supported OS must provide a kernel process incarnation');
  for (const name of ['worker.lock', 'producer.lock']) {
    for (const boundary of ['start', 'boot']) {
      const stale = { ...incarnation, [boundary]: boundary === 'boot' ? randomUUID() : incarnation.platform === 'linux'
        ? String(BigInt(incarnation.start) + 1n) : `${BigInt(incarnation.start.split('.')[0]) + 1n}.${incarnation.start.split('.')[1]}` };
      const path = join(root, name);
      await fs.mkdir(path, { mode: 0o700 });
      await fs.writeFile(join(path, 'owner.json'), JSON.stringify({ pid: process.pid, token: randomUUID(), incarnation: stale }), { mode: 0o600 });
      if (name === 'worker.lock') {
        const release = await outbox.acquireWorker();
        assert.equal(typeof release, 'function');
        const published = JSON.parse(await fs.readFile(join(path, 'owner.json'), 'utf8'));
        assert.deepEqual(published.incarnation, incarnation);
        await release();
      } else assert.equal(await outbox.put(fixture()), true);
      assert.deepEqual((await outbox.pending()).map(({ identity, body }) => ({ identity, body })), [fixture()]);
    }
  }
});

test('Legacy live owners and failed platform probes stay conservative; malformed incarnations fail closed', async t => {
  const root = join(await sandbox(t), 'queue');
  const outbox = await openOutbox({ root, route });
  const path = join(root, 'worker.lock');
  await fs.mkdir(path, { mode: 0o700 });
  const legacy = { pid: process.pid, token: randomUUID() };
  await fs.writeFile(join(path, 'owner.json'), JSON.stringify(legacy), { mode: 0o600 });
  assert.equal(await outbox.acquireWorker(), null);
  assert.deepEqual(JSON.parse(await fs.readFile(join(path, 'owner.json'), 'utf8')), legacy);
  const incarnation = await processIncarnation();
  const platform = Object.getOwnPropertyDescriptor(process, 'platform');
  try {
    Object.defineProperty(process, 'platform', { ...platform, value: 'unavailable-probe' });
    assert.equal(await ownerIsDead({ pid: process.pid, incarnation }), false);
  } finally { Object.defineProperty(process, 'platform', platform); }
  for (const unsafe of [null, {}, { ...incarnation, boot: '../../not-a-boot' }, { ...incarnation, start: 'NaN' }, { ...incarnation, extra: true }]) {
    await fs.writeFile(join(path, 'owner.json'), JSON.stringify({ ...legacy, incarnation: unsafe }), { mode: 0o600 });
    await assert.rejects(outbox.acquireWorker());
  }
  await fs.writeFile(join(path, 'owner.json'), JSON.stringify(legacy), { mode: 0o600 });
  assert.equal(await outbox.put(fixture()), true);
  assert.equal((await outbox.status()).pending, 1);
});

test('Regular ledger, manifest, record and owner reads reject a replacement FIFO within a deadline', { skip: process.platform === 'win32' }, async t => {
  const base = await sandbox(t);
  const run = promisify(execFile);
  for (const boundary of ['manifest.json', 'ledger.json', `records/${fixture().identity}.json`, 'worker.lock/owner.json']) {
    const root = join(base, boundary.replaceAll('/', '-'));
    const result = await run(process.execPath, ['--input-type=module', '-e', `${preamble(root)}
import fs from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import assert from 'node:assert/strict';
await outbox.put(${JSON.stringify(fixture())});
if (${JSON.stringify(boundary)} === 'worker.lock/owner.json') await outbox.acquireWorker();
const target = ${JSON.stringify(join(root, boundary))};
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
try {
  await assert.rejects(${JSON.stringify(boundary)} === 'worker.lock/owner.json' ? outbox.acquireWorker() : outbox.pending());
  assert.equal(replaced, true);
  console.log('bounded unsafe-file rejection');
} finally { fs.open = originalOpen; }`], { timeout: 5000, killSignal: 'SIGKILL', maxBuffer: 2048 });
    assert.equal(result.stdout.trim(), 'bounded unsafe-file rejection');
  }
});

test('Concurrent real starters have one owner; dead worker and producer owners are recoverable', async t => {
  const root = join(await sandbox(t), 'queue');
  const outbox = await openOutbox({ root, route });
  const starters = Array.from({ length: 6 }, () => child(t, `${preamble(root)}
process.send('ready'); await new Promise(resolve => process.once('message', resolve));
const release = await outbox.acquireWorker(); process.send(Boolean(release));
await new Promise(resolve => process.once('message', resolve));
if (release) await release(); process.disconnect();`));
  await Promise.all(starters.map(starter => starter.message()));
  const acquired = starters.map(starter => starter.message());
  starters.forEach(starter => starter.process.send('start'));
  assert.equal((await Promise.all(acquired)).filter(Boolean).length, 1);
  starters.forEach(starter => starter.process.send('exit'));
  await Promise.all(starters.map(successful));

  for (const legacy of [false, true]) {
    const crashedWorker = child(t, `${preamble(root)}
if (!await outbox.acquireWorker()) process.exit(2); process.exit(0);`);
    await successful(crashedWorker);
    if (legacy) {
      const path = join(root, 'worker.lock', 'owner.json');
      const { pid, token } = JSON.parse(await fs.readFile(path, 'utf8'));
      await fs.writeFile(path, JSON.stringify({ pid, token }), { mode: 0o600 });
    }
    const replacement = await outbox.acquireWorker();
    assert.equal(typeof replacement, 'function');
    await replacement();
  }

  const crashedProducer = child(t, `${preamble(root)}
import fs from 'node:fs/promises';
const originalRename = fs.rename;
fs.rename = async (from, to) => { await originalRename(from, to); if (to === ${JSON.stringify(join(root, 'producer.lock'))}) process.exit(0); };
await outbox.put(${JSON.stringify(fixture())}); process.exit(2);`);
  await successful(crashedProducer);
  assert.equal(await outbox.put(fixture()), true);
  assert.equal((await outbox.status()).pending, 1);
});

test('A zombie worker cannot retain ownership while its real parent is stopped', { skip: process.platform === 'win32' }, async t => {
  const root = join(await sandbox(t), 'queue');
  const outbox = await openOutbox({ root, route });
  const workerScript = `${preamble(root)}
const release = await outbox.acquireWorker(); if (!release) process.exit(2);
process.send('owned'); await new Promise(resolve => process.once('message', resolve));
await release(); process.disconnect();`;
  const parent = child(t, `import { spawn } from 'node:child_process';
const worker = spawn(process.execPath, ['--input-type=module', '-e', ${JSON.stringify(workerScript)}], { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
worker.once('message', () => process.send(worker.pid));
worker.once('close', () => process.disconnect());`);
  const pid = await parent.message();
  const execFileAsync = promisify(execFile);
  const waitState = async (pid, expected) => {
    const until = Date.now() + 10000;
    for (;;) {
      const { stdout } = await execFileAsync('ps', ['-o', 'stat=', '-p', String(pid)], { timeout: 1000, maxBuffer: 1024 });
      if (expected.test(stdout.trim())) return;
      assert.ok(Date.now() < until, 'Synthetic process did not reach its ownership-test state');
      await delay(20);
    }
  };
  try {
    parent.process.kill('SIGSTOP');
    await waitState(parent.process.pid, /^[Tt]/);
    process.kill(pid, 'SIGKILL');
    await waitState(pid, /^Z/);
    const replacement = await outbox.acquireWorker();
    assert.equal(typeof replacement, 'function');
    assert.equal(await outbox.put(fixture()), true);
    await replacement();
  } finally {
    try { process.kill(pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
    parent.process.kill('SIGCONT');
  }
  await successful(parent);
  assert.equal((await outbox.status()).pending, 1);
});

test('A live producer is not evicted by age and a contender proceeds only after its short transaction', async t => {
  const root = join(await sandbox(t), 'queue');
  const outbox = await openOutbox({ root, route });
  const producer = child(t, `${preamble(root)}
import fs from 'node:fs/promises';
const originalRename = fs.rename;
fs.rename = async (from, to) => {
  await originalRename(from, to);
  if (to === ${JSON.stringify(join(root, 'producer.lock'))}) {
    process.send('locked'); await new Promise(resolve => process.once('message', resolve));
  }
};
if (!await outbox.put(${JSON.stringify(fixture(1))})) process.exit(2); process.disconnect();`);
  assert.equal(await producer.message(), 'locked');
  await fs.utimes(join(root, 'producer.lock'), new Date(1000), new Date(1000));
  const originalRename = fs.rename;
  let attempted;
  const contended = new Promise(resolve => { attempted = resolve; });
  let inserted;
  try {
    fs.rename = async (from, to) => {
      try { return await originalRename(from, to); }
      catch (error) {
        if (to === join(root, 'producer.lock') && ['EEXIST', 'ENOTEMPTY'].includes(error.code)) attempted();
        throw error;
      }
    };
    inserted = outbox.put(fixture(2));
    await contended;
    const owner = JSON.parse(await fs.readFile(join(root, 'producer.lock', 'owner.json'), 'utf8'));
    assert.equal(owner.pid, producer.process.pid);
  } finally {
    fs.rename = originalRename;
    producer.process.send('release');
  }
  assert.equal(await inserted, true);
  await successful(producer);
  assert.equal((await outbox.status()).pending, 2);
});

test('File and directory sync failures cannot falsely acknowledge; crash temps and published records recover', async t => {
  const base = await sandbox(t);
  for (const failure of ['file', 'directory']) {
    const root = join(base, failure);
    const outbox = await openOutbox({ root, route });
    const originalOpen = fs.open;
    let injected = false;
    try {
      fs.open = async (...args) => {
        const handle = await originalOpen(...args);
        const path = String(args[0]);
        const target = failure === 'file' ? path.startsWith(join(root, 'records', '.tmp-')) : path === join(root, 'records');
        if (target && !injected) {
          const originalSync = handle.sync.bind(handle);
          handle.sync = async () => {
            if (!injected) { injected = true; throw Object.assign(new Error('injected sync failure'), { code: 'EIO' }); }
            return originalSync();
          };
        }
        return handle;
      };
      assert.equal(await outbox.put(fixture()), false);
      assert.equal(injected, true);
    } finally { fs.open = originalOpen; }
    const reopened = await openOutbox({ root, route });
    assert.equal(await reopened.put(fixture()), true);
    assert.equal((await reopened.status()).pending, 1);
    assert.equal((await reopened.pending())[0].body, fixture().body);
  }

  const root = join(base, 'crashes');
  await openOutbox({ root, route });
  const partial = child(t, `${preamble(root)}
import fs from 'node:fs/promises';
const originalOpen = fs.open;
fs.open = async (...args) => {
  const handle = await originalOpen(...args);
  if (String(args[0]).startsWith(${JSON.stringify(join(root, 'records', '.tmp-'))})) {
    handle.writeFile = async () => { await handle.write('{"identity":'); process.exit(0); };
  }
  return handle;
};
await outbox.put(${JSON.stringify(fixture())}); process.exit(2);`);
  await successful(partial);
  const recovered = await openOutbox({ root, route });
  assert.deepEqual(await recovered.pending(), []);
  assert.equal((await fs.readdir(join(root, 'records'))).length, 0);

  const published = child(t, `${preamble(root)}
import fs from 'node:fs/promises';
const originalRename = fs.rename;
fs.rename = async (from, to) => {
  await originalRename(from, to);
  if (to === ${JSON.stringify(join(root, 'records', `${fixture().identity}.json`))}) process.exit(0);
};
await outbox.put(${JSON.stringify(fixture())}); process.exit(2);`);
  await successful(published);
  const final = await openOutbox({ root, route });
  assert.equal((await final.pending())[0].body, fixture().body);
  assert.equal((await final.status()).pending, 1);
  assert.equal(await final.put(fixture()), true);
  assert.equal((await final.pending()).length, 1);
});
