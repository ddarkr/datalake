import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs/promises';
import { createServer } from 'node:http';
import { chmod, mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import opencode, { createEventMapper } from '../plugins/opencode2/index.js';
import { install } from '../plugins/opencode2/install.mjs';
import { openOutbox, readRoute } from '../plugins/outbox.mjs';
import { serializeEvent } from '../plugins/otel.mjs';

const native = (seq, type, data, created = 1000 + seq * 10) => ({
  id: `evt_${seq}`, created, type, durable: { aggregateID: data.sessionID, seq, version: 1 }, data,
});
const sessionID = 'ses_main';
const assistantMessageID = 'msg_answer';
const tokens = { input: 10, output: 4, reasoning: 2, cache: { read: 7, write: 3 } };
const pluginDirectory = resolve(dirname(fileURLToPath(import.meta.url)), '../plugins/opencode2');
const execFileAsync = promisify(execFile);
const senderPath = fileURLToPath(new URL('../plugins/sender.mjs', import.meta.url));
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
    const value = await bounded(Promise.resolve().then(predicate), description, Math.max(1, deadline - Date.now()));
    if (value) return value;
    await delay(20);
  }
  assert.fail(`Timed out: ${description}`);
}

// Model the native unbounded pubsub: only a live subscription retains raw items.
function nativeBus() {
  const subscribers = new Set();
  return {
    get unread() { return [...subscribers].reduce((sum, subscriber) => sum + subscriber.queue.length, 0); },
    publish(event) {
      for (const subscriber of subscribers) {
        if (subscriber.closed) continue;
        if (subscriber.waiting) {
          const resolveNext = subscriber.waiting;
          subscriber.waiting = undefined;
          resolveNext({ done: false, value: event });
        } else subscriber.queue.push(event);
      }
    },
    close() {
      for (const subscriber of subscribers) {
        subscriber.closed = true;
        subscriber.waiting?.({ done: true });
        subscriber.waiting = undefined;
      }
    },
    subscribe({ signal }) {
      const subscriber = { queue: [], closed: false };
      subscribers.add(subscriber);
      const finish = () => {
        subscriber.queue.length = 0;
        subscriber.closed = true;
        subscriber.waiting?.({ done: true });
        subscriber.waiting = undefined;
        subscribers.delete(subscriber);
        signal.removeEventListener('abort', finish);
      };
      signal.addEventListener('abort', finish, { once: true });
      return {
        [Symbol.asyncIterator]() { return this; },
        next() {
          if (subscriber.queue.length) return Promise.resolve({ done: false, value: subscriber.queue.shift() });
          if (subscriber.closed) return Promise.resolve({ done: true });
          return new Promise(resolveNext => { subscriber.waiting = resolveNext; });
        },
        return() { finish(); return Promise.resolve({ done: true }); },
      };
    },
  };
}

async function taskWorker(pid, root) {
  try {
    const { stdout } = await execFileAsync('ps', ['-p', String(pid), '-o', 'stat=', '-o', 'command='], {
      timeout: 5000, maxBuffer: 16384,
    });
    const match = /^\s*(\S+)\s+([\s\S]*)$/.exec(stdout);
    return Boolean(match && !match[1].startsWith('Z') && match[2].includes(senderPath) && match[2].includes(root));
  } catch (error) {
    if (error.code === 1 || error.code === 'ESRCH') return false;
    throw error;
  }
}

async function consumerFixture(t, held = false) {
  const directory = await mkdtemp(join(tmpdir(), 'datalake-opencode2-consumer-'));
  const stateRoot = join(directory, 'state');
  const location = { directory: join(directory, 'location'), workspaceID: 'synthetic-workspace' };
  await mkdir(location.directory, { mode: 0o700 });
  const registry = join(directory, 'worker-pids');
  const nodeBinary = join(directory, 'tracked-node');
  await writeFile(nodeBinary, `#!/bin/sh\numask 077\nprintf '%s\\t%s\\n' "$$" "$2" >> ${shellQuote(registry)}\nexec ${shellQuote(process.execPath)} "$@"\n`, { mode: 0o700 });
  const requests = [];
  const reply = response => { if (!response.destroyed && !response.writableEnded) response.writeHead(200).end('{}'); };
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push({ body, response });
    if (!held) reply(response);
  });
  let cleanup;
  t.after(async () => {
    const errors = [];
    try { if (cleanup) await bounded(cleanup(), 'consumer cleanup'); } catch (error) { errors.push(error); }
    const workers = [];
    try {
      for (const line of (await readFile(registry, 'utf8')).trim().split('\n')) {
        const [text, root] = line.split('\t');
        const pid = Number(text);
        assert.ok(Number.isSafeInteger(pid) && pid > 0 && pid !== process.pid && dirname(root) === stateRoot,
          'Only isolated fixture worker metadata may be used');
        workers.push([pid, root]);
        if (await taskWorker(pid, root)) process.kill(pid, 'SIGKILL');
      }
    } catch (error) { if (error.code !== 'ENOENT' && error.code !== 'ESRCH') errors.push(error); }
    server.closeAllConnections();
    try {
      await bounded(new Promise(resolveClose => server.close(resolveClose)), 'collector close');
      await until(async () => (await Promise.all(workers.map(([pid, root]) => taskWorker(pid, root)))).every(value => !value),
        'owned worker exit');
    } catch (error) { errors.push(error); }
    try { await rm(directory, { recursive: true, force: true }); } catch (error) { errors.push(error); }
    if (errors.length) throw new AggregateError(errors, 'Isolated OpenCode consumer cleanup failed');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const configPath = join(directory, 'otel.json');
  await writeFile(configPath, JSON.stringify({
    endpoint: `http://127.0.0.1:${server.address().port}/v1/traces`, timeoutMs: 30000,
  }), { mode: 0o600 });

  function start(events) {
    let index = 0;
    const finished = Promise.withResolvers();
    cleanup = opencode.setup({
      location, app: { name: 'opencode-cli' },
      options: { configPath, stateRoot, nodeBinary, profile: 'synthetic-profile' },
      event: {
        subscribe({ signal }) {
          if (typeof events.subscribe === 'function') {
            const iterator = events.subscribe({ signal });
            return {
              [Symbol.asyncIterator]() { return this; },
              async next() {
                const item = await iterator.next();
                if (item.done) finished.resolve();
                return item;
              },
              return() { finished.resolve(); return iterator.return(); },
            };
          }
          return {
            [Symbol.asyncIterator]() { return this; },
            next() {
              if (signal.aborted || index === events.length) {
                finished.resolve();
                return Promise.resolve({ done: true });
              }
              return Promise.resolve({ done: false, value: events[index++] });
            },
            return() { finished.resolve(); return Promise.resolve({ done: true }); },
          };
        },
      },
    });
    assert.equal(typeof cleanup, 'function', 'Native setup synchronously returns its cleanup function');
    return { finished: finished.promise, cleanup };
  }
  async function ledger() {
    const names = await readdir(stateRoot);
    const scopes = names.filter(name => /^[a-f0-9]{64}$/.test(name));
    assert.equal(scopes.length, 1, 'This fixture owns one isolated native profile ledger');
    const root = join(stateRoot, scopes[0]);
    return { root, outbox: await openOutbox({ root, route: await readRoute(root) }) };
  }
  return {
    location, configPath, stateRoot, requests, start, ledger,
    release() { held = false; for (const { response } of requests) reply(response); },
  };
}

test('native consumer retries only unaccepted projections and preserves usage across later duplicates', async t => {
  const fixture = await consumerFixture(t);
  const child = 'ses_child';
  const privateValue = 'OPENCODE_PRIVATE_NATIVE_CANARY';
  const step = native(4, 'session.step.ended', {
    sessionID: child, assistantMessageID, tokens, cost: 0.012,
    text: privateValue, input: { secret: privateValue }, providerState: { secret: privateValue },
  });
  const terminal = native(7, 'session.execution.succeeded', { sessionID: child });
  const events = [
    native(1, 'session.created', { sessionID: child, parentID: sessionID, agent: 'explore' }),
    native(2, 'session.execution.started', { sessionID: child }),
    native(3, 'session.step.started', { sessionID: child, assistantMessageID, started: 1025, model: { id: 'm', providerID: 'p' } }),
    step, step,
    native(5, 'session.usage.recorded', { sessionID: child, source: 'title', tokens, cost: 0.001 }),
    native(6, 'session.usage.recorded', { sessionID: child, source: 'compaction', tokens, cost: 0.002 }),
    terminal, terminal,
  ];
  const projections = events.flatMap(createEventMapper({ 'service.name': 'opencode-cli' }));
  const expected = projections.map(span => serializeEvent('opencode', span));
  const stepIdentity = expected[projections.findIndex(span => span.eventId === `step:${assistantMessageID}`)].identity;
  const childIdentity = expected[projections.findIndex(span => span.kind === 'subagent')].identity;
  const sessionIdentity = expected[projections.findIndex(span => span.kind === 'session')].identity;
  const failures = new Map([stepIdentity, childIdentity].map(identity => [identity, Promise.withResolvers()]));
  const blocked = new Set(failures.keys());
  const rename = fs.rename;
  t.mock.method(fs, 'rename', async (source, target) => {
    const identity = basename(target, '.json');
    if (blocked.has(identity)) {
      failures.get(identity).resolve();
      throw Object.assign(new Error('Synthetic durable publication failure'), { code: 'EIO' });
    }
    return rename(source, target);
  });
  const foreign = {
    ...native(100, 'session.usage.recorded', { sessionID: child, tokens, cost: 99 }),
    location: { ...fixture.location, workspaceID: 'other-workspace' },
  };
  const consumer = fixture.start([events[0], foreign, ...events.slice(1)]);
  await bounded(failures.get(stepIdentity).promise, 'failed usage local handoff');
  const { root, outbox } = await fixture.ledger();
  const failedStatus = await outbox.status();
  assert.equal(failedStatus.pending + failedStatus.done, 0,
    'A failed billable projection is not a queued watermark');
  blocked.delete(stepIdentity);
  await bounded(failures.get(childIdentity).promise, 'failed second span local handoff');
  const records = (await readdir(join(root, 'records'))).filter(name => /^[a-f0-9]{48}\.json$/.test(name));
  assert.ok(records.includes(`${sessionIdentity}.json`), 'The first span of a partial batch is durably accepted');
  assert.equal(records.includes(`${childIdentity}.json`), false, 'A failed second span remains unaccepted');
  for (const name of records) {
    assert.equal((await readFile(join(root, 'records', name), 'utf8')).includes(privateValue), false);
  }
  blocked.delete(childIdentity);
  await bounded(consumer.finished, 'native stream completion after local recovery');
  await until(async () => (await outbox.status()).done === expected.length, 'durable delivery of recovered projections');
  assert.deepEqual(fixture.requests.map(({ body }) => body).sort(), expected.map(({ body }) => body).sort(),
    'Usage, title, compaction and parent spans each bill once with their original times and IDs');
  assert.equal(fixture.requests.some(({ body }) => body.includes(privateValue)), false);
  await consumer.cleanup();
});

test('native consumer and cleanup finish while collector HTTP acknowledgement is held', async t => {
  const fixture = await consumerFixture(t, true);
  const events = [
    native(1, 'session.execution.started', { sessionID }),
    native(2, 'session.step.started', { sessionID, assistantMessageID, started: 1005 }),
    native(3, 'session.step.ended', { sessionID, assistantMessageID, tokens, cost: 0.012 }),
    native(4, 'session.execution.succeeded', { sessionID }),
  ];
  const expected = events.flatMap(createEventMapper({ 'service.name': 'opencode-cli' }))
    .map(span => serializeEvent('opencode', span));
  const consumer = fixture.start(events);
  await until(() => fixture.requests.length > 0, 'held collector request');
  await bounded(consumer.finished, 'native model event stream independent of HTTP');
  const { outbox } = await fixture.ledger();
  await bounded(consumer.cleanup(), 'local-only cleanup before HTTP acknowledgement');
  assert.equal((await outbox.status()).pending, expected.length, 'Cleanup hands off remaining metadata locally');
  assert.equal(fixture.requests[0].response.writableEnded, false);
  assert.equal((await outbox.status()).done, 0);
  fixture.release();
  await until(async () => (await outbox.status()).done === expected.length, 'delivery after releasing collector');
  assert.deepEqual(fixture.requests.map(({ body }) => body).sort(), expected.map(({ body }) => body).sort());
});

test('cleanup aborts failed local acceptance without waiting for a later retry', async t => {
  const fixture = await consumerFixture(t);
  const event = native(1, 'session.usage.recorded', { sessionID, source: 'title', tokens, cost: 0.001 });
  const [span] = createEventMapper({ 'service.name': 'opencode-cli' })(event);
  const { identity } = serializeEvent('opencode', span);
  const failed = Promise.withResolvers();
  const rename = fs.rename;
  t.mock.method(fs, 'rename', async (source, target) => {
    if (basename(target) === `${identity}.json`) {
      failed.resolve();
      throw Object.assign(new Error('Synthetic full-duration local failure'), { code: 'EIO' });
    }
    return rename(source, target);
  });
  const consumer = fixture.start([event, event]);
  await bounded(failed.promise, 'local acceptance failure');
  await bounded(consumer.cleanup(), 'aborting local retry');
  const { outbox } = await fixture.ledger();
  const status = await outbox.status();
  assert.equal(status.pending + status.done, 0);
  assert.deepEqual(fixture.requests, []);
});

test('disabled private configuration leaves no raw native backlog or local export resources', async t => {
  for (const scenario of ['missing', 'malformed', 'invalid-endpoint', 'unsafe-mode', 'symlink']) {
    await t.test(scenario, async child => {
      const fixture = await consumerFixture(child);
      if (scenario === 'missing') await rm(fixture.configPath);
      if (scenario === 'malformed') await writeFile(fixture.configPath, '{ invalid');
      if (scenario === 'invalid-endpoint') await writeFile(fixture.configPath, '{"endpoint":"http://public.example/v1/traces"}');
      if (scenario === 'unsafe-mode') await chmod(fixture.configPath, 0o644);
      if (scenario === 'symlink') {
        const target = `${fixture.configPath}.target`;
        await fs.rename(fixture.configPath, target);
        await symlink(target, fixture.configPath);
      }
      const bus = nativeBus();
      const consumer = fixture.start(bus);
      for (let seq = 1; seq <= 4096; seq++) {
        bus.publish(native(seq, 'session.text.delta', { sessionID, delta: 'PRIVATE_DISABLED_NATIVE_CONTENT' }));
      }
      // A subscription that pulls one item and then retries would leave this
      // native raw backlog behind even though no telemetry was accepted.
      assert.equal(bus.unread, 0);
      await bounded(consumer.cleanup(), 'disabled export lifecycle cleanup');
      await assert.rejects(readdir(fixture.stateRoot), { code: 'ENOENT' });
      assert.deepEqual(fixture.requests, []);
    });
  }
});

test('local refusal keeps draining private native content and recovers the identical billable projection', async t => {
  const fixture = await consumerFixture(t);
  const privateValue = 'OPENCODE_RAW_PUBSUB_PRIVATE_CANARY';
  const event = native(1, 'session.usage.recorded', {
    sessionID, tokens, cost: 0.001, text: privateValue,
    input: { secret: privateValue }, providerState: { secret: privateValue },
  });
  const [span] = createEventMapper({ 'service.name': 'opencode-cli' })(event);
  const expected = serializeEvent('opencode', span);
  let blocked = true;
  const failed = Promise.withResolvers();
  const rename = fs.rename;
  t.mock.method(fs, 'rename', async (source, target) => {
    if (blocked && basename(target) === `${expected.identity}.json`) {
      failed.resolve();
      throw Object.assign(new Error('Synthetic temporary filesystem refusal'), { code: 'EIO' });
    }
    return rename(source, target);
  });
  const bus = nativeBus();
  const consumer = fixture.start(bus);
  bus.publish(event);
  await bounded(failed.promise, 'initial refused billable projection');
  for (let seq = 2; seq <= 6000; seq++) {
    bus.publish(native(seq, 'session.text.delta', {
      sessionID, assistantMessageID, delta: privateValue.repeat(64),
      input: { secret: privateValue }, providerState: { secret: privateValue },
    }));
  }
  bus.publish(event);
  bus.close();
  await bounded(consumer.finished, 'native pubsub drain while local acceptance is refused');
  assert.equal(bus.unread, 0, 'Private native messages are consumed rather than accumulating behind local retries');
  const { root, outbox } = await fixture.ledger();
  const refused = await outbox.status();
  assert.equal(refused.pending + refused.done, 0, 'Observed native usage is not falsely reported as accepted');
  blocked = false;
  await until(async () => (await outbox.status()).done === 1, 'billable recovery after filesystem repair');
  assert.deepEqual(fixture.requests.map(({ body }) => body), [expected.body]);
  for (const name of await readdir(join(root, 'records'))) {
    if (/^[a-f0-9]{48}\.json$/.test(name)) {
      assert.equal((await readFile(join(root, 'records', name), 'utf8')).includes(privateValue), false);
    }
  }
  await consumer.cleanup();
});

test('bounded metadata overflow visibly refuses new IDs without evicting or replaying pending billing', async t => {
  const fixture = await consumerFixture(t);
  const events = Array.from({ length: 400 }, (_, index) => native(index + 1, 'session.usage.recorded', {
    sessionID, tokens, cost: (index + 1) / 1000,
    text: 'PRIVATE_OVERFLOW_NATIVE_CONTENT',
  }));
  const expected = events.slice(0, 256).flatMap(createEventMapper({ 'service.name': 'opencode-cli' }))
    .map(span => serializeEvent('opencode', span));
  let blocked = true;
  const failed = Promise.withResolvers();
  const rename = fs.rename;
  t.mock.method(fs, 'rename', async (source, target) => {
    if (blocked && basename(target) === `${expected[0].identity}.json`) {
      failed.resolve();
      throw Object.assign(new Error('Synthetic prolonged local refusal'), { code: 'EIO' });
    }
    return rename(source, target);
  });
  const warnings = [];
  t.mock.method(console, 'error', (...args) => warnings.push(args.join(' ')));
  const bus = nativeBus();
  const consumer = fixture.start(bus);
  bus.publish(events[0]);
  await bounded(failed.promise, 'first pending metadata span');
  for (const event of events.slice(1)) bus.publish(event);
  for (const event of events.slice(0, 10)) bus.publish(event);
  bus.close();
  await bounded(consumer.finished, 'raw native drain at sanitized metadata capacity');
  assert.equal(bus.unread, 0);
  assert.equal(warnings.filter(warning => warning.includes('metadata buffer full')).length, 1,
    'Overflow visibly refuses new metadata once instead of silently evicting earlier billing');
  assert.equal(warnings.some(warning => warning.includes('PRIVATE_OVERFLOW_NATIVE_CONTENT')), false);
  const { outbox } = await fixture.ledger();
  const refused = await outbox.status();
  assert.equal(refused.pending + refused.done, 0);
  blocked = false;
  await until(async () => (await outbox.status()).done === expected.length, 'accepted prefix billing recovery', 120000);
  assert.deepEqual(fixture.requests.map(({ body }) => body).sort(), expected.map(({ body }) => body).sort(),
    'Earlier pending IDs bill once, including the refused head; overflow IDs never claim acceptance');
  await consumer.cleanup();
});



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
