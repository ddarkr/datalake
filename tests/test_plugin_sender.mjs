import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { chmod, copyFile, link, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rename, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { delimiter, dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { openOutbox, readRoute } from '../plugins/outbox.mjs';
import { createTelemetry, serializeEvent, validateConfig } from '../plugins/otel.mjs';
import { readSenderStatus, startSender } from '../plugins/sender.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const syntheticSecret = 'Basic synthetic-sender-fixture-only';
const rawCanary = 'SENDER_PRIVATE_PROMPT_CANARY';
const senderUrl = new URL('../plugins/sender.mjs', import.meta.url).href;
const senderPath = fileURLToPath(senderUrl);
const outboxUrl = new URL('../plugins/outbox.mjs', import.meta.url).href;
const execFileAsync = promisify(execFile);
const shellQuote = value => `'${value.replaceAll("'", "'\\''")}'`;

async function bounded(operation, description, timeout = 30000) {
  let timer;
  try {
    return await Promise.race([
      operation,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Timed out: ${description}`)), timeout); }),
    ]);
  } finally { clearTimeout(timer); }
}

async function until(predicate, description, timeout = 30000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await bounded(Promise.resolve().then(predicate), description, Math.max(1, deadline - Date.now()));
    if (result) return result;
    await delay(20);
  }
  assert.fail(`Timed out: ${description}`);
}

function event(name) {
  return serializeEvent('codex', {
    kind: 'llm.turn', sessionId: `fixture-${name}`, eventId: `fixture-turn-${name}`,
    startTimeMs: 1780000000000, endTimeMs: 1780000000010,
    attributes: { 'gen_ai.usage.input_tokens': 7, 'gen_ai.usage.output_tokens': 3 },
    prompt: rawCanary,
  });
}

async function replaceConfig(path, config) {
  const temporary = `${path}.replacement`;
  await writeFile(temporary, JSON.stringify(config), { mode: 0o600 });
  await rename(temporary, path);
}

function reply(entry, status = 200, body = '{}') {
  entry.response.writeHead(status, { 'Content-Type': 'application/json' });
  entry.response.end(body);
}

async function isTaskWorker(root, pid) {
  try {
    const { stdout } = await execFileAsync('ps', ['-p', String(pid), '-o', 'stat=', '-o', 'command='], {
      timeout: 5000, maxBuffer: 16384,
    });
    const match = /^\s*(\S+)\s+([\s\S]*)$/.exec(stdout);
    // kill(pid, 0) succeeds for zombies, but they can no longer own or send work.
    return Boolean(match && !match[1].startsWith('Z') && match[2].includes(senderPath) && match[2].includes(root));
  } catch (error) {
    if (error.code === 1 || error.code === 'ESRCH') return false;
    throw new Error('Unable to inspect isolated task worker');
  }
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'datalake-sender-test-'));
  const roots = new Set();
  const children = new Set();
  const workers = new Map();
  const handlers = new Map();
  const requests = new Map();
  const registry = join(directory, 'detached-pids');
  const nodeBinary = join(directory, 'tracked-node');
  // The shim records only PID/root metadata before exec; duplicate losing starters are tracked too.
  await writeFile(nodeBinary, `#!/bin/sh\numask 077\nprintf '%s\\t%s\\n' "$$" "$2" >> ${shellQuote(registry)}\nexec ${shellQuote(process.execPath)} "$@"\n`, { mode: 0o700 });
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const entry = { body, authorization: request.headers.authorization, response };
    const list = requests.get(request.url) || [];
    list.push(entry);
    requests.set(request.url, list);
    const handler = handlers.get(request.url);
    if (!handler) { response.writeHead(404); response.end(); return; }
    handler(entry, list.length);
  });
  t.after(async () => {
    const errors = [];
    const attempt = async (operation, description) => {
      try { await bounded(operation, description, 10000); }
      catch (error) { errors.push(error); }
    };
    // Cleanup failures must not skip closing HTTP/pipes or hide the scenario's failure.
    await attempt((async () => {
      try {
        for (const line of (await readFile(registry, 'utf8')).trim().split('\n')) {
          const [text, root] = line.split('\t');
          const pid = Number(text);
          if (Number.isSafeInteger(pid) && pid > 0 && pid !== process.pid && roots.has(root)) workers.set(pid, root);
        }
      } catch (error) { if (error.code !== 'ENOENT') throw error; }
      await Promise.all([...roots].map(root => owner(root)));
    })(), 'isolated PID metadata');
    for (const child of children) {
      try { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); }
      catch (error) { if (error.code !== 'ESRCH') errors.push(error); }
      for (const stream of child.stdio) stream?.destroy();
    }
    await attempt(Promise.all([...workers].map(async ([pid, root]) => {
      if (await isTaskWorker(root, pid)) {
        try { process.kill(pid, 'SIGKILL'); }
        catch (error) { if (error.code !== 'ESRCH') throw error; }
      }
    })), 'isolated worker termination');
    server.closeAllConnections();
    await attempt(new Promise(resolve => server.close(resolve)), 'HTTP fixture close');
    await attempt(until(async () => {
      const running = await Promise.all([...workers].map(([pid, root]) => isTaskWorker(root, pid)));
      return running.every(value => !value) && [...children].every(child => child.exitCode !== null || child.signalCode !== null);
    }, 'isolated PID cleanup', 10000), 'isolated PID cleanup');
    await attempt(rm(directory, { recursive: true, force: true }), 'private fixture removal');
    if (errors.length) throw new AggregateError(errors, 'Isolated sender cleanup failed');
  });
  server.listen(0, '127.0.0.1');
  await bounded(once(server, 'listening'), 'HTTP fixture listening');
  const base = `http://127.0.0.1:${server.address().port}`;

  async function scope(name, fileConfig = false) {
    const root = join(directory, name);
    roots.add(root);
    const config = validateConfig({ endpoint: `${base}/${name}`, headers: { Authorization: syntheticSecret }, timeoutMs: 30000 });
    const route = { key: hash(name), endpointHash: hash(config.endpoint) };
    if (fileConfig) {
      route.configPath = join(directory, `${name}.json`);
      await replaceConfig(route.configPath, config);
    }
    const outbox = await openOutbox({ root, route });
    const path = new URL(config.endpoint).pathname;
    return { root, route, config, outbox, path, nodeBinary, entries: () => requests.get(path) || [] };
  }

  async function owner(root) {
    try {
      const value = JSON.parse(await readFile(join(root, 'worker.lock', 'owner.json'), 'utf8'));
      assert.ok(Number.isSafeInteger(value.pid) && value.pid > 0, 'Isolated owner PID is valid');
      if (value.pid !== process.pid) workers.set(value.pid, root);
      return value.pid;
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      throw error;
    }
  }

  async function killOwner(root) {
    const pid = await owner(root);
    assert.ok(pid && pid !== process.pid, 'Task worker owns this isolated outbox');
    assert.ok(await isTaskWorker(root, pid), 'Kill only this test root sender');
    process.kill(pid, 'SIGKILL');
    await until(async () => !(await isTaskWorker(root, pid)), 'task worker stopped');
    return pid;
  }

  async function done(item, count = 1) {
    await until(async () => {
      const status = await item.outbox.status();
      return status.pending === 0 && status.done === count;
    }, 'durable delivery');
    await until(async () => !(await owner(item.root)), 'empty worker release');
    const status = await readSenderStatus(item.root);
    assert.ok(status?.state === 'idle', 'Successful empty drain resets diagnostic state');
  }

  function safeOutput(...texts) {
    for (const text of texts) {
      assert.ok(!text.includes(syntheticSecret) && !text.includes('synthetic-rotated-fixture')
        && !text.includes(rawCanary) && !text.includes('SYNTHETIC_BOOTSTRAP_SECRET')
        && !text.includes(base) && !text.includes('synthetic-unrelated-environment-secret') && !text.includes('x'.repeat(100)),
      'Process diagnostics contain no credentials, endpoint or raw input');
    }
  }

  async function privateSpool(root) {
    async function contents(path) {
      try {
        for (const entry of await readdir(path, { withFileTypes: true })) {
          const next = join(path, entry.name);
          if (entry.isDirectory()) await contents(next);
          else if (entry.isFile()) {
            try { safeOutput(await readFile(next, 'utf8')); }
            catch (error) { if (error.code !== 'ENOENT') throw error; }
          }
        }
      } catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
    await contents(root);
  }

  async function pendingState(item, state) {
    const status = await until(async () => {
      const value = await readSenderStatus(item.root);
      return value?.state === state && value;
    }, `safe ${state} diagnostic`);
    assert.deepEqual(Object.keys(status).sort(), ['state', 'timestamp']);
    assert.ok(Number.isSafeInteger(status.timestamp) && status.timestamp >= 0);
    const file = join(item.root, 'sender-status.json');
    const stat = await lstat(file);
    assert.equal(stat.mode & 0o7777, 0o600);
    assert.equal(stat.uid, process.getuid());
    assert.equal(stat.nlink, 1);
    assert.ok(stat.size <= 256, 'Latest sender diagnostic is bounded');
    await privateSpool(item.root);
    return status;
  }

  return { directory, children, handlers, requests, base, scope, owner, killOwner, done, safeOutput, privateSpool, pendingState };
}

const scenario = (name, run) => test(name, { timeout: 90000 }, async t => run(await fixture(t)));

function producer(f, item, record, { cwd = f.directory, path = process.env.PATH, execPath, bun = false, nodeBinary, binary = process.execPath, nativeNode = binary === process.execPath, runtimeEnv = {} } = {}) {
  const code = `
    import {openOutbox} from ${JSON.stringify(outboxUrl)};
    import {startSender} from ${JSON.stringify(senderUrl)};
    let input=''; for await (const chunk of process.stdin) input+=chunk;
    const {root,route,config,record,execPath,bun,nodeBinary,runtimeEnv}=JSON.parse(input);
    const outbox=await openOutbox({root,route});
    const accepted=await outbox.put(record);
    if(execPath!==undefined) process.execPath=execPath;
    if(bun) Object.defineProperty(process.versions,'bun',{value:'fixture'});
    Object.assign(process.env,runtimeEnv);
    await startSender({root,route,config,nodeBinary});
    console.log(JSON.stringify({accepted}));
  `;
  const child = spawn(binary, [...(nativeNode ? ['--input-type=module', '-e'] : ['--eval']), code], {
    cwd, env: { PATH: path }, stdio: ['pipe', 'pipe', 'pipe'],
  });
  f.children.add(child);
  const exited = once(child, 'exit');
  const closed = once(child, 'close');
  const result = { code, output: '', errors: '' };
  child.stdout.on('data', chunk => { result.output += chunk; });
  child.stderr.on('data', chunk => { result.errors += chunk; });
  child.stdin.end(JSON.stringify({ root: item.root, route: item.route, config: item.config, record, execPath, bun, nodeBinary, runtimeEnv }));
  result.complete = async () => {
    assert.equal((await bounded(exited, 'producer exit without collector ACK', 5000))[0], 0);
    await bounded(closed, 'producer stdio close without collector ACK', 5000);
    f.safeOutput(result.output, result.errors, code);
    assert.deepEqual(JSON.parse(result.output.trim()), { accepted: true });
  };
  return result;
}

scenario('Held collector ACK does not retain producer exit or stdio', async f => {
  const held = await f.scope('held-parent-exit');
  const record = event('held-parent-exit');
  f.handlers.set(held.path, () => {});
  const parent = producer(f, held, record);
  await until(() => held.entries()[0], 'detached collector request');
  assert.ok(await f.owner(held.root), 'Detached worker owns held request');
  await parent.complete();
  assert.deepEqual(await held.outbox.status().then(({ pending, done }) => ({ pending, done })), { pending: 1, done: 0 });
  assert.equal(held.entries()[0].body, record.body);
  await f.privateSpool(held.root);
  reply(held.entries()[0]);
  await f.done(held);
});

test('Linux running Node delivers from writable toolcache without trusting project runtimes or preloads', { timeout: 90000 }, async t => {
  if (process.platform !== 'linux') return t.skip('Linux kernel executable pinning');
  const f = await fixture(t);
  const cache = join(f.directory, 'hostedtoolcache');
  const project = join(f.directory, 'project');
  const bin = join(project, 'bin');
  await mkdir(cache);
  await chmod(cache, 0o777);
  const runtime = join(cache, 'node');
  await copyFile(process.execPath, runtime);
  await chmod(runtime, 0o777);
  await mkdir(bin, { recursive: true });
  await mkdir(join(project, '.git'));
  const marker = join(f.directory, 'untrusted-runtime-marker');
  const fake = join(bin, 'node');
  const preload = join(project, 'preload.cjs');
  await writeFile(fake, `#!/bin/sh\nprintf executed > ${shellQuote(marker)}\n`, { mode: 0o700 });
  await writeFile(preload, `require('node:fs').writeFileSync(${JSON.stringify(marker)}, 'preloaded');process.exit(1);`, { mode: 0o600 });
  const held = await f.scope('writable-toolcache');
  const record = event('writable-toolcache');
  f.handlers.set(held.path, () => {});
  await producer(f, held, record, {
    cwd: project, path: `${bin}${delimiter}${cache}`, binary: runtime, nativeNode: true,
    runtimeEnv: { NODE_OPTIONS: `--require=${preload}`, LD_PRELOAD: preload, LD_LIBRARY_PATH: project },
  }).complete();
  await until(() => held.entries().length === 1, 'kernel-pinned Node held HTTP request');
  assert.equal(held.entries()[0].authorization, syntheticSecret);
  assert.equal(held.entries()[0].body, record.body);
  assert.deepEqual(await held.outbox.status().then(({ pending, done }) => ({ pending, done })), { pending: 1, done: 0 });
  await assert.rejects(readFile(marker), error => error.code === 'ENOENT');
  reply(held.entries()[0]);
  await f.done(held);

  // Even an actual native Node image is not eligible when launched from the project.
  await copyFile(process.execPath, fake);
  const refused = await f.scope('project-native-node');
  f.handlers.set(refused.path, entry => reply(entry));
  await producer(f, refused, event('project-native-node'), { cwd: project, path: bin, binary: fake, nativeNode: true }).complete();
  assert.deepEqual(await refused.outbox.status().then(({ pending, done }) => ({ pending, done })), { pending: 1, done: 0 });
  assert.equal(refused.entries().length, 0);
  await f.pendingState(refused, 'pending-launch');
});

scenario('Project PATH never executes fake Node or receives credential bootstrap', async f => {
  const project = join(f.directory, 'project');
  const bin = join(project, 'bin');
  const subdir = join(project, 'nested', 'work');
  await mkdir(bin, { recursive: true });
  await mkdir(subdir, { recursive: true });
  await mkdir(join(project, '.git'));
  const fake = join(bin, 'node');
  const marker = join(f.directory, 'fake-node-executed');
  const capture = join(f.directory, 'fake-node-bootstrap');
  await writeFile(fake, `#!/bin/sh\nprintf executed > ${shellQuote(marker)}\n/bin/cat > ${shellQuote(capture)}\n`, { mode: 0o700 });
  const linked = join(f.directory, 'linked-runtime');
  await symlink(bin, linked);
  const installed = join(f.directory, 'trusted-runtime');
  await mkdir(installed, { mode: 0o700 });
  await copyFile(process.execPath, join(installed, 'node'));
  await chmod(join(installed, 'node'), 0o700);
  for (const [name, cwd, path, execPath, bun] of [
    ['native-relative', project, `./bin${delimiter}${installed}`, undefined, false],
    ['fallback-relative', project, `./bin${delimiter}${installed}`, fake, false],
    ['fallback-absolute', subdir, `${bin}${delimiter}${installed}`, fake, false],
    ['bun-absolute', subdir, `${bin}${delimiter}${installed}`, fake, true],
    ['bun-symlink', subdir, `${linked}${delimiter}${installed}`, fake, true],
  ]) {
    const item = await f.scope(name);
    const record = event(name);
    f.handlers.set(item.path, () => {});
    const parent = producer(f, item, record, { cwd, path, execPath, bun });
    await parent.complete();
    await until(() => item.entries().length === 1, 'trusted runtime held HTTP request');
    assert.equal(item.entries()[0].authorization, syntheticSecret);
    assert.equal(item.entries()[0].body, record.body);
    assert.equal((await item.outbox.status()).pending, 1);
    for (const file of [marker, capture]) await assert.rejects(readFile(file), error => error.code === 'ENOENT');
    reply(item.entries()[0]);
    await f.done(item);
  }
});

scenario('Missing or writable discovery runtime leaves bounded pending status without executing project Node', async f => {
  const project = join(f.directory, 'package-project');
  const bin = join(project, 'bin');
  const subdir = join(project, 'nested');
  await mkdir(bin, { recursive: true });
  await mkdir(subdir);
  await writeFile(join(project, 'package.json'), '{}');
  const marker = join(f.directory, 'missing-fake-node-executed');
  const fake = join(bin, 'node');
  await writeFile(fake, `#!/bin/sh\nprintf executed > ${shellQuote(marker)}\n`, { mode: 0o700 });
  const writable = join(f.directory, 'writable-node-bin');
  await mkdir(writable);
  await copyFile(process.execPath, join(writable, 'node'));
  await chmod(join(writable, 'node'), 0o700);
  await chmod(writable, 0o777);
  for (const [name, path, bun, nodeBinary] of [
    ['missing-runtime', `../bin${delimiter}${bin}`, true, undefined],
    ['writable-runtime', writable, true, undefined],
    ['invalid-override', dirname(process.execPath), false, join(f.directory, 'no-explicit-node')],
  ]) {
    const item = await f.scope(name);
    f.handlers.set(item.path, entry => reply(entry));
    await producer(f, item, event(name), { cwd: subdir, path, execPath: fake, bun, nodeBinary }).complete();
    assert.equal((await item.outbox.status()).pending, 1);
    assert.equal(item.entries().length, 0);
    await f.pendingState(item, 'pending-launch');
    await assert.rejects(readFile(marker), error => error.code === 'ENOENT');
  }
});

test('Actual Bun caller bypasses project fake Node while the collector ACK stays held', { timeout: 90000 }, async t => {
  const binary = process.env.DATALAKE_TEST_BUN;
  if (!binary) return t.skip('Set DATALAKE_TEST_BUN to a trusted absolute Bun executable');
  const f = await fixture(t);
  const project = join(f.directory, 'bun-project');
  const bin = join(project, 'bin');
  await mkdir(bin, { recursive: true });
  await mkdir(join(project, '.git'));
  const marker = join(f.directory, 'bun-fake-node-executed');
  const capture = join(f.directory, 'bun-fake-node-bootstrap');
  const fake = join(bin, 'node');
  await writeFile(fake, `#!/bin/sh\nprintf executed > ${shellQuote(marker)}\n/bin/cat > ${shellQuote(capture)}\n`, { mode: 0o700 });
  const installed = join(f.directory, 'bun-trusted-node');
  await mkdir(installed, { mode: 0o700 });
  await copyFile(process.execPath, join(installed, 'node'));
  await chmod(join(installed, 'node'), 0o700);
  for (const [name, path] of [
    ['actual-bun-relative', `./bin${delimiter}${installed}`],
    ['actual-bun-absolute', `${bin}${delimiter}${installed}`],
  ]) {
    const item = await f.scope(name);
    f.handlers.set(item.path, () => {});
    await producer(f, item, event(name), { binary, cwd: project, path, execPath: fake }).complete();
    await until(() => item.entries().length === 1, 'actual Bun trusted Node held request');
    assert.equal(item.entries()[0].authorization, syntheticSecret);
    assert.equal((await item.outbox.status()).pending, 1);
    for (const file of [marker, capture]) await assert.rejects(readFile(file), error => error.code === 'ENOENT');
    reply(item.entries()[0]);
    await f.done(item);
  }
});

scenario('Duplicate starters preserve sole HTTP ownership while ACK is held', async f => {
  const concurrent = await f.scope('multiple-starters');
  f.handlers.set(concurrent.path, () => {});
  assert.equal(await concurrent.outbox.put(event('multiple-starters')), true);
  await bounded(Promise.all(Array.from({ length: 12 }, () => startSender(concurrent))), 'duplicate starter readiness');
  await until(() => concurrent.entries().length, 'sole-owner request');
  const workerPid = await f.owner(concurrent.root);
  assert.equal(concurrent.entries().length, 1);
  assert.equal(await concurrent.outbox.put(event('while-owner-busy')), true);
  await bounded(Promise.all(Array.from({ length: 12 }, () => startSender(concurrent))), 'busy-owner starter readiness');
  assert.equal(await f.owner(concurrent.root), workerPid);
  assert.equal(concurrent.entries().length, 1);
  f.handlers.set(concurrent.path, entry => reply(entry));
  reply(concurrent.entries()[0]);
  await f.done(concurrent, 2);
  assert.equal(concurrent.entries().length, 2);
});

for (const [name, status, body] of [
  ['unauthorized', 401, '{}'], ['unavailable', 503, '{}'],
  ['partial', 200, '{"partialSuccess":{"rejectedSpans":"1","errorMessage":"SYNTHETIC_BOOTSTRAP_SECRET"}}'],
  ['malformed', 200, '{not-json-SYNTHETIC_BOOTSTRAP_SECRET'],
]) {
  scenario(`Immutable retry and private pending diagnostic after ${name} response`, async f => {
    const item = await f.scope(name);
    const original = event(name);
    const heldRetries = [];
    f.handlers.set(item.path, (entry, count) => {
      if (count === 1) reply(entry, status, body);
      else heldRetries.push(entry);
    });
    assert.equal(await item.outbox.put(original), true);
    await startSender(item);
    await until(async () => (await item.outbox.pending())[0]?.attempts >= 1, `${name} retry persisted`);
    const before = (await item.outbox.pending())[0];
    assert.equal(before.body, original.body);
    assert.equal((await item.outbox.status()).done, 0);
    await f.pendingState(item, 'pending-http');
    assert.ok(await f.owner(item.root));
    if (name === 'unauthorized') {
      await f.killOwner(item.root);
      const reopened = await openOutbox({ root: item.root, route: item.route });
      assert.deepEqual((await reopened.pending())[0], before);
    }
    f.handlers.set(item.path, entry => reply(entry));
    for (const entry of heldRetries) {
      if (name === 'unauthorized') entry.response.destroy();
      else reply(entry);
    }
    if (name === 'unauthorized') await startSender(item);
    await f.done(item);
    assert.ok(item.entries().length >= 2);
    assert.ok(item.entries().every(entry => entry.body === original.body && entry.authorization === syntheticSecret), 'Retry preserves payload and expected fixture authentication');
  });
}

scenario('Sender SIGKILL before ACK preserves accepted span for real HTTP replay', async f => {
  const replay = await f.scope('killed-worker');
  const replayRecord = event('killed-worker');
  f.handlers.set(replay.path, () => {});
  assert.equal(await replay.outbox.put(replayRecord), true);
  await startSender(replay);
  await until(() => replay.entries().length === 1, 'first held request');
  const deadPid = await f.killOwner(replay.root);
  replay.entries()[0].response.destroy();
  assert.equal((await replay.outbox.pending())[0].body, replayRecord.body);
  await startSender(replay);
  await until(() => replay.entries().length === 2, 'replayed held request');
  const replayPid = await f.owner(replay.root);
  reply(replay.entries()[1]);
  await f.done(replay);
  assert.notEqual(replayPid, deadPid);
  assert.equal(replay.entries().length, 2);
  assert.ok(replay.entries().every(entry => entry.body === replayRecord.body));
});

scenario('File authentication rotates between real HTTP attempts', async f => {
  const rotation = await f.scope('auth-rotation', true);
  f.handlers.set(rotation.path, entry => {
    if (entry.authorization === 'Basic synthetic-rotated-fixture') reply(entry);
  });
  assert.equal(await rotation.outbox.put(event('auth-rotation')), true);
  await startSender(rotation);
  await until(() => rotation.entries().length === 1, 'initial credential request');
  await replaceConfig(rotation.route.configPath, { ...rotation.config, headers: { Authorization: 'Basic synthetic-rotated-fixture' } });
  reply(rotation.entries()[0], 401);
  await f.done(rotation);
  assert.ok(rotation.entries().length === 2 && rotation.entries()[0].authorization === syntheticSecret
    && rotation.entries()[1].authorization === 'Basic synthetic-rotated-fixture', 'Next attempt reloads only the rotated fixture credential');
});

scenario('Endpoint pinning retains pending telemetry and private config-failure status', async f => {
  const pinned = await f.scope('endpoint-pinning', true);
  f.handlers.set(pinned.path, () => {});
  const changedPath = '/different-destination/v1/traces';
  f.handlers.set(changedPath, entry => reply(entry));
  const record = event('endpoint-pinning');
  assert.equal(await pinned.outbox.put(record), true);
  await startSender(pinned);
  await until(() => pinned.entries().length === 1, 'initial pinned request');
  await replaceConfig(pinned.route.configPath, { ...pinned.config, endpoint: `${f.base}/different-destination` });
  reply(pinned.entries()[0], 503);
  await until(async () => (await pinned.outbox.pending())[0]?.attempts >= 2, 'changed endpoint retained');
  await f.pendingState(pinned, 'pending-config');
  assert.equal((f.requests.get(changedPath) || []).length, 0);
  assert.equal(pinned.entries().length, 1);
  assert.equal((await pinned.outbox.pending())[0].body, record.body);
  f.handlers.set(pinned.path, entry => reply(entry));
  await replaceConfig(pinned.route.configPath, pinned.config);
  await f.done(pinned);
  assert.equal(pinned.entries().length, 2);
});

for (const unsafe of ['missing', 'symlink']) {
  scenario(`A ${unsafe} file configuration stays pending until safe replacement`, async f => {
    const item = await f.scope(`config-${unsafe}`, true);
    f.handlers.set(item.path, entry => reply(entry));
    assert.equal(await item.outbox.put(event(`config-${unsafe}`)), true);
    await rm(item.route.configPath);
    if (unsafe === 'symlink') {
      const target = join(f.directory, 'synthetic-symlink-target.json');
      await replaceConfig(target, item.config);
      await symlink(target, item.route.configPath);
    }
    await startSender(item);
    await until(async () => (await item.outbox.pending())[0]?.attempts >= 1, `${unsafe} configuration retained`);
    await f.pendingState(item, 'pending-config');
    assert.equal(item.entries().length, 0);
    await rm(item.route.configPath, { force: true });
    await replaceConfig(item.route.configPath, item.config);
    await f.done(item);
    assert.equal(item.entries().length, 1);
  });
}

scenario('Empty owner handoff and repeated real empty-exit races do not strand enqueue', async f => {
  const handoff = await f.scope('empty-exit-handoff');
  f.handlers.set(handoff.path, entry => reply(entry));
  const oldOwner = await handoff.outbox.acquireWorker();
  assert.ok(oldOwner);
  assert.equal((await handoff.outbox.pending()).length, 0);
  assert.equal(await handoff.outbox.put(event('enqueue-before-release')), true);
  await startSender(handoff);
  assert.equal(await oldOwner.releaseIfEmpty(), false);
  await oldOwner();
  await startSender(handoff);
  await f.done(handoff);
  const emptyOwner = await handoff.outbox.acquireWorker();
  assert.ok(emptyOwner);
  assert.equal(await emptyOwner.releaseIfEmpty(), true);
  assert.equal(await handoff.outbox.put(event('enqueue-after-release')), true);
  await startSender(handoff);
  await f.done(handoff, 2);
  for (let index = 0; index < 8; index++) {
    assert.equal(await handoff.outbox.put(event(`real-empty-exit-${index}`)), true);
    await startSender(handoff);
    // The next enqueue deliberately does not wait for worker-lock removal.
    await until(async () => (await handoff.outbox.status()).done === index + 3, 'real empty-exit delivery');
  }
  await f.done(handoff, 10);
  assert.equal(handoff.entries().length, 10);
});

async function rejectedBootstrap(f, item, payload) {
  const child = spawn(process.execPath, [senderPath, item.root], {
    shell: false, env: { PATH: process.env.PATH }, stdio: ['pipe', 'pipe', 'pipe', 'pipe'],
  });
  f.children.add(child);
  const closed = once(child, 'close');
  let output = '', errors = '', ready = '';
  child.stdout.on('data', chunk => { output += chunk; });
  child.stderr.on('data', chunk => { errors += chunk; });
  child.stdio[3].on('data', chunk => { ready += chunk; });
  child.stdin.on('error', () => {});
  child.stdin.end(payload);
  assert.equal((await bounded(closed, 'rejected native bootstrap close'))[0], 1);
  f.safeOutput(output, errors, ready);
  assert.ok(output === '' && ready === '', 'Rejected bootstrap emits no stdout or readiness metadata');
  assert.equal((await item.outbox.status()).pending, 1);
  assert.equal((await item.outbox.status()).done, 0);
  await f.pendingState(item, 'pending-launch');
}

scenario('Spawn and bootstrap failures retain acceptance without exposing input in diagnostics', async f => {
  const failure = await f.scope('launch-failure');
  assert.equal(await failure.outbox.put(event('launch-failure')), true);
  const warnings = [];
  const originalError = console.error;
  console.error = message => warnings.push(String(message));
  try {
    await startSender({ ...failure, nodeBinary: join(f.directory, 'nonexistent-node') });
    const failedBootstrap = join(f.directory, 'bootstrap-failure');
    await writeFile(failedBootstrap, '#!/bin/sh\nexit 0\n', { mode: 0o700 });
    await startSender({ ...failure, nodeBinary: failedBootstrap });
    await startSender({ ...failure, config: { ...failure.config, headers: { Authorization: 'x'.repeat(70000) } } });
  } finally { console.error = originalError; }
  f.safeOutput(...warnings);
  assert.equal((await failure.outbox.status()).pending, 1);
  assert.equal((await failure.outbox.status()).done, 0);
  await f.pendingState(failure, 'pending-launch');
  await rejectedBootstrap(f, failure, '{SYNTHETIC_BOOTSTRAP_SECRET');
  await rejectedBootstrap(f, failure, JSON.stringify({ config: { ...failure.config, headers: { Authorization: 'x'.repeat(70000) } } }));
  await rejectedBootstrap(f, failure, JSON.stringify({ config: { ...failure.config, endpoint: `${f.base}/wrong-bootstrap-route` } }));
  assert.equal(failure.entries().length, 0);
  f.handlers.set(failure.path, entry => reply(entry));
  await startSender(failure);
  await f.done(failure);
});

scenario('Status reader accepts only bounded owner-private fixed metadata', async f => {
  const item = await f.scope('status-reader');
  assert.equal(await readSenderStatus(item.root), null);
  const path = join(item.root, 'sender-status.json');
  const safe = JSON.stringify({ state: 'pending-http', timestamp: 1780000000000 });
  await writeFile(path, safe, { mode: 0o600 });
  assert.deepEqual(await readSenderStatus(item.root), { state: 'pending-http', timestamp: 1780000000000 });
  for (const text of [
    JSON.stringify({ state: 'pending-http', timestamp: 1780000000000, details: syntheticSecret }),
    JSON.stringify({ state: syntheticSecret, timestamp: 1780000000000 }),
    JSON.stringify({ state: 'pending-http', timestamp: -1 }),
    'SYNTHETIC_BOOTSTRAP_SECRET'.repeat(100),
  ]) {
    await writeFile(path, text);
    assert.equal(await readSenderStatus(item.root), null);
  }
  await rm(path);
  const target = join(f.directory, 'status-target');
  await writeFile(target, safe, { mode: 0o600 });
  await symlink(target, path);
  assert.equal(await readSenderStatus(item.root), null);
  await rm(path);
  await link(target, path);
  assert.equal(await readSenderStatus(item.root), null);
  await rm(path);
  await rm(target);
  await writeFile(path, safe, { mode: 0o644 });
  assert.equal(await readSenderStatus(item.root), null);
  await rm(path);
  assert.deepEqual(await item.outbox.status(), { pending: 0, done: 0, bytes: 0 });
});

scenario('FIFO status without a writer returns promptly and does not gate durable producer acceptance', async f => {
  const seed = await f.scope('fifo-status');
  const stateRoot = join(f.directory, 'fifo-state');
  const telemetry = createTelemetry('codex', {
    ...seed.config, stateRoot, nodeBinary: join(f.directory, 'no-fifo-runtime'),
  });
  const turn = id => ({ kind: 'llm.turn', sessionId: 'fifo-session', eventId: id, startTimeMs: 1780000000000, endTimeMs: 1780000000010 });
  assert.equal(await telemetry.enqueue(turn('fifo-seed')), true);
  const directories = await readdir(stateRoot);
  assert.equal(directories.length, 1);
  const root = join(stateRoot, directories[0]);
  const path = join(root, 'sender-status.json');
  await rm(path);
  await execFileAsync('mkfifo', ['-m', '600', path], { timeout: 5000 });
  assert.equal(await bounded(readSenderStatus(root), 'FIFO status without writer', 1000), null);
  assert.equal(await bounded(telemetry.enqueue(turn('fifo-next')), 'enqueue past FIFO status without writer', 5000), true);
  await bounded(telemetry.flushLocal(), 'FIFO producer local flush', 1000);
  const outbox = await openOutbox({ root, route: await readRoute(root) });
  assert.equal((await outbox.status()).pending, 2);
  assert.equal((await lstat(path)).isFIFO(), true);
  assert.equal(seed.entries().length, 0);
  await f.privateSpool(root);
});

scenario('Detached worker excludes executable preloads and unrelated environment secrets', async f => {
  const environment = await f.scope('runtime-environment');
  f.handlers.set(environment.path, () => {});
  const marker = join(f.directory, 'inherited-preload-marker');
  const preload = join(f.directory, 'synthetic-preload.cjs');
  const script = `require('node:fs').writeFileSync(${JSON.stringify(marker)},'unexpected preload');`;
  await writeFile(preload, script, { mode: 0o600 });
  f.safeOutput(script);
  const snapshot = join(f.directory, 'runtime-snapshot.json');
  const wrapper = join(f.directory, 'explicit-trusted-runtime');
  const capture = `import fs from 'node:fs';fs.writeFileSync(${JSON.stringify(snapshot)},JSON.stringify({cwd:process.cwd(),env:process.env}),{mode:0o600});await import(${JSON.stringify(senderUrl)});`;
  await writeFile(wrapper, `#!/bin/sh\nexec ${shellQuote(process.execPath)} --input-type=module -e ${shellQuote(capture)} "$@"\n`, { mode: 0o700 });
  const retained = { SSL_CERT_FILE: join(f.directory, 'fixture-ca.pem'), NODE_USE_SYSTEM_CA: '1' };
  const rejected = {
    NODE_TLS_REJECT_UNAUTHORIZED: '0', HTTP_PROXY: 'http://synthetic-proxy.invalid',
    HTTPS_PROXY: 'http://synthetic-proxy.invalid', ALL_PROXY: 'http://synthetic-proxy.invalid',
    NODE_PATH: f.directory, LD_PRELOAD: preload, DYLD_INSERT_LIBRARIES: preload,
  };
  const previousRuntimeEnv = Object.fromEntries(Object.keys({ ...retained, ...rejected }).map(name => [name, process.env[name]]));
  Object.assign(process.env, retained, rejected);
  const previousOptions = process.env.NODE_OPTIONS;
  const previousSecret = process.env.SENDER_UNRELATED_SECRET;
  process.env.NODE_OPTIONS = `--require=${preload}`;
  process.env.SENDER_UNRELATED_SECRET = 'synthetic-unrelated-environment-secret';
  try {
    assert.equal(await environment.outbox.put(event('runtime-environment')), true);
    await startSender({ ...environment, nodeBinary: wrapper });
    await until(() => environment.entries().length, 'narrow runtime environment delivery');
    assert.ok(await f.owner(environment.root));
    await assert.rejects(readFile(marker), error => error.code === 'ENOENT');
    const observed = JSON.parse(await readFile(snapshot, 'utf8'));
    assert.equal(await realpath(observed.cwd), await realpath(environment.root));
    assert.equal(observed.env.PATH, '/usr/bin:/bin');
    for (const [name, value] of Object.entries(retained)) assert.equal(observed.env[name], value);
    for (const name of ['NODE_OPTIONS', 'SENDER_UNRELATED_SECRET', ...Object.keys(rejected)]) assert.equal(observed.env[name], undefined);
    await f.privateSpool(environment.root);
    reply(environment.entries()[0]);
    await f.done(environment);
  } finally {
    if (previousOptions === undefined) delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = previousOptions;
    if (previousSecret === undefined) delete process.env.SENDER_UNRELATED_SECRET;
    else process.env.SENDER_UNRELATED_SECRET = previousSecret;
    for (const [name, value] of Object.entries(previousRuntimeEnv)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
});
