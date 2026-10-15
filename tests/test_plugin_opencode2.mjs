import assert from 'node:assert/strict';
import { once } from 'node:events';
import { chmod, mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import opencode, { createEventMapper } from '../plugins/opencode2/index.js';
import { install } from '../plugins/opencode2/install.mjs';
import { serializeEvent } from '../plugins/otel.mjs';

const native = (seq, type, data, created = 1000 + seq * 10) => ({
  id: `evt_${seq}`, created, type, durable: { aggregateID: data.sessionID, seq, version: 1 }, data,
});
const sessionID = 'ses_main';
const assistantMessageID = 'msg_answer';
const tokens = { input: 10, output: 4, reasoning: 2, cache: { read: 7, write: 3 } };
const pluginDirectory = resolve(dirname(fileURLToPath(import.meta.url)), '../plugins/opencode2');

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

// Direct-mode loopback fixture: one HTTP request per send, no sender process,
// no durable queue, no process cleanup.
async function consumerFixture(t, respond = null, timeoutMs = 30000) {
  const directory = await mkdtemp(join(tmpdir(), 'datalake-opencode2-consumer-'));
  const legacyRoot = join(directory, 'state');
  const location = { directory: join(directory, 'location'), workspaceID: 'synthetic-workspace' };
  await mkdir(location.directory, { mode: 0o700 });
  const bodies = [];
  const held = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    bodies.push(body);
    const action = typeof respond === 'function' ? await respond(bodies.length, body) : respond;
    if (action === 'hold') held.push(response);
    else if (action) {
      response.writeHead(action.status ?? 200, { 'content-type': 'application/json' });
      response.end(action.payload ?? '{}');
    } else response.writeHead(200, { 'content-type': 'application/json' }).end('{}');
  });
  let cleanup;
  t.after(async () => {
    if (cleanup) await bounded(cleanup(), 'consumer cleanup');
    server.closeAllConnections();
    await bounded(new Promise(resolveClose => server.close(resolveClose)), 'collector close');
    await rm(directory, { recursive: true, force: true });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const configPath = join(directory, 'otel.json');
  await writeFile(configPath, JSON.stringify({
    endpoint: `http://127.0.0.1:${server.address().port}/v1/traces`, timeoutMs,
  }), { mode: 0o600 });

  function start(events) {
    let index = 0;
    const finished = Promise.withResolvers();
    cleanup = opencode.setup({
      location, app: { name: 'opencode-cli' },
      options: { configPath },
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
  return {
    location, configPath, legacyRoot, bodies, held, start,
    release() {
      for (const response of held.splice(0)) {
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end('{}');
      }
    },
  };
}

test('direct consumer attempts each projection once over actual HTTP without private content', async t => {
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
  const foreign = {
    ...native(100, 'session.usage.recorded', { sessionID: child, tokens, cost: 99 }),
    location: { ...fixture.location, workspaceID: 'other-workspace' },
  };
  const consumer = fixture.start([events[0], foreign, ...events.slice(1)]);
  await bounded(consumer.finished, 'native stream completion');
  await bounded(consumer.cleanup(), 'direct shutdown after native completion');
  await until(() => fixture.bodies.length >= expected.length, 'direct HTTP delivery of each projection');
  assert.deepEqual([...fixture.bodies].sort(), expected.map(({ body }) => body).sort(),
    'Usage, title, compaction and parent spans each bill once with their original times and IDs');
  assert.equal(fixture.bodies.some(body => body.includes(privateValue)), false);
  assert.ok(!fixture.bodies.some(body => body.includes('other-workspace')));
});

test('direct consumer never retries HTTP failures and duplicates send once', async t => {
  const fixture = await consumerFixture(t, { status: 500, payload: '{}' });
  const events = [
    native(1, 'session.execution.started', { sessionID }),
    native(2, 'session.step.started', { sessionID, assistantMessageID, started: 1005 }),
    native(3, 'session.step.ended', { sessionID, assistantMessageID, tokens, cost: 0.012 }),
    native(3, 'session.step.ended', { sessionID, assistantMessageID, tokens, cost: 0.012 }),
    native(4, 'session.execution.succeeded', { sessionID }),
    native(4, 'session.execution.succeeded', { sessionID }),
  ];
  const expected = new Map(
    events.flatMap(createEventMapper({ 'service.name': 'opencode-cli' }))
      .map(span => serializeEvent('opencode', span))
      .map(serialized => [serialized.identity, serialized.body]),
  );
  const consumer = fixture.start(events);
  await bounded(consumer.finished, 'native stream completion despite HTTP rejection');
  await bounded(consumer.cleanup(), 'direct shutdown after HTTP rejection');
  await until(() => fixture.bodies.length >= expected.size, 'one HTTP attempt per identity');
  await delay(100);
  assert.equal(fixture.bodies.length, expected.size);
  assert.deepEqual([...fixture.bodies].sort(), [...expected.values()].sort());
});

test('direct consumer treats collector partial success as a single-attempt rejection', async t => {
  const fixture = await consumerFixture(t, { status: 200, payload: '{"partialSuccess":{"rejectedSpans":"1"}}' });
  const event = native(1, 'session.usage.recorded', { sessionID, source: 'title', tokens, cost: 0.001 });
  const [span] = createEventMapper({ 'service.name': 'opencode-cli' })(event);
  const expected = serializeEvent('opencode', span);
  const consumer = fixture.start([event, event]);
  await bounded(consumer.finished, 'native stream completion despite partial success');
  await bounded(consumer.cleanup(), 'direct shutdown after partial success');
  await until(() => fixture.bodies.length >= 1, 'partial-success HTTP attempt');
  await delay(100);
  assert.equal(fixture.bodies.length, 1);
  assert.deepEqual(fixture.bodies, [expected.body]);
});

test('direct shutdown waits for held collector ACKs only until the configured timeout', async t => {
  const fixture = await consumerFixture(t, 'hold', 300);
  const events = [
    native(1, 'session.execution.started', { sessionID }),
    native(2, 'session.step.ended', { sessionID, assistantMessageID, tokens, cost: 0.012 }),
    native(3, 'session.execution.succeeded', { sessionID }),
  ];
  const consumer = fixture.start(events);
  await until(() => fixture.bodies.length > 0, 'collector received a direct request');
  const startedAt = Date.now();
  await bounded(consumer.cleanup(), 'shutdown waits for in-flight HTTP until the configured timeout', 10000);
  assert.ok(Date.now() - startedAt >= 250);
  assert.ok(fixture.held.length > 0);
  fixture.release();
});

test('disabled private configuration leaves no local export resources', async t => {
  for (const scenario of ['missing', 'malformed', 'invalid-endpoint', 'unsafe-mode', 'symlink']) {
    await t.test(scenario, async child => {
      const fixture = await consumerFixture(child);
      if (scenario === 'missing') await rm(fixture.configPath);
      if (scenario === 'malformed') await writeFile(fixture.configPath, '{ invalid');
      if (scenario === 'invalid-endpoint') await writeFile(fixture.configPath, '{"endpoint":"http://public.example/v1/traces"}');
      if (scenario === 'unsafe-mode') await chmod(fixture.configPath, 0o644);
      if (scenario === 'symlink') {
        const target = `${fixture.configPath}.target`;
        await writeFile(target, await readFile(fixture.configPath, 'utf8'));
        await rm(fixture.configPath);
        await symlink(target, fixture.configPath);
      }
      const bus = nativeBus();
      let subscribed = false;
      const tracked = { subscribe(...args) { subscribed = true; return bus.subscribe(...args); } };
      const consumer = fixture.start(tracked);
      for (let seq = 1; seq <= 64; seq++) {
        bus.publish(native(seq, 'session.text.delta', { sessionID, delta: 'PRIVATE_DISABLED_NATIVE_CONTENT' }));
      }
      bus.close();
      await bounded(consumer.cleanup(), 'disabled export lifecycle cleanup');
      assert.equal(subscribed, false, 'Disabled setup must not subscribe to the native bus');
      await assert.rejects(readdir(fixture.legacyRoot), { code: 'ENOENT' });
      assert.deepEqual(fixture.bodies, []);
    });
  }
});

test('direct mode leaves a legacy queue byte-unchanged and spawns no sender', async t => {
  const fixture = await consumerFixture(t);
  const legacy = join(fixture.legacyRoot, 'legacy-route');
  await mkdir(legacy, { recursive: true });
  const obstacle = join(legacy, 'queue.sqlite');
  await writeFile(obstacle, Buffer.from([0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x00]));
  const before = await readFile(obstacle);
  const event = native(1, 'session.usage.recorded', { sessionID, source: 'title', tokens, cost: 0.001 });
  const consumer = fixture.start([event]);
  await bounded(consumer.finished, 'native stream completion beside legacy state');
  await bounded(consumer.cleanup(), 'direct shutdown beside legacy state');
  await until(() => fixture.bodies.length >= 1, 'direct HTTP delivery beside legacy state');
  assert.deepEqual(await readFile(obstacle), before);
  assert.deepEqual(await readdir(legacy), ['queue.sqlite']);
  assert.deepEqual(await readdir(dirname(legacy)), ['legacy-route']);
});



test('v2 event lifecycle exports usage deltas once and excludes private content', () => {
  const map = createEventMapper({ 'service.name': 'opencode-cli' });
  const privateValue = 'PRIVATE-CONTENT-DO-NOT-EXPORT';
  const events = [
    native(1, 'session.created', { sessionID, title: privateValue, metadata: { secret: privateValue } }),
    native(2, 'session.execution.started', { sessionID }),
    native(3, 'session.step.started', { sessionID, assistantMessageID, started: 1025, agent: 'build', model: { id: 'test-model', providerID: 'openai' } }),
    { id: 'evt_delta', type: 'session.text.delta', created: 1040, data: { sessionID, assistantMessageID, ordinal: 0, delta: privateValue } },
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

test('native ephemeral text, interleaving and tool settlement keep stream timing private and distinct', () => {
  const map = createEventMapper();
  const secret = 'PRIVATE-STREAM-CANARY';
  const a = { sessionID, assistantMessageID: 'msg_a' };
  const b = { sessionID, assistantMessageID: 'msg_b' };
  const delta = (id, data, created, text) => ({
    id, type: 'session.text.delta', created, data: { ...data, ordinal: 0, delta: text },
  });
  map(native(1, 'session.step.started', { ...a, started: 100, model: { id: 'm', variant: 'fast' } }, 110));
  map(native(2, 'session.step.started', { ...b, started: 120 }, 125));
  map(native(3, 'session.reasoning.started', { ...a, ordinal: 0, state: { secret } }, 130));
  map(delta('evt_empty', a, 135, ''));
  map(native(4, 'session.text.started', { ...a, ordinal: 0 }, 140));
  const text = delta('evt_text', a, 160, secret);
  map(text);
  map(text);
  map(delta('evt_b', b, 170, secret));
  // A late durable replay cannot move the observed watermark or reset dispatch.
  map(native(2, 'session.step.started', { ...a, started: 0 }, 180));
  map(native(5, 'session.tool.input.started', { ...a, id: 'call', name: 'read' }, 180));
  map(native(6, 'session.step.streamed', a, 200));
  map(delta('evt_late', a, 250, secret));
  const [turnB] = map(native(7, 'session.step.ended', { ...b, tokens }, 300));
  const ended = native(8, 'session.step.ended', {
    ...a, tokens, providerState: { service_tier: 'priority', secret },
  }, 10000);
  const [turnA] = map(ended);
  assert.deepEqual(map(ended), []);
  map(delta('evt_after_end', a, 150, secret));
  const attrs = turnA.attributes;
  assert.equal(attrs.ttft_ms, 30);
  assert.equal(attrs['coding_agent.ttft.kind'], 'first_output_part');
  assert.equal(attrs['coding_agent.response.first_text_ms'], 60);
  assert.equal(attrs['coding_agent.request.duration_ms'], 100);
  assert.equal(attrs['coding_agent.duration.kind'], 'stream');
  assert.equal(attrs['coding_agent.usage.source'], 'reported');
  assert.equal(attrs['gen_ai.usage.output_tokens'], 6);
  assert.equal(attrs['gen_ai.usage.input_tokens'], 20);
  assert.equal(attrs['coding_agent.model.variant'], 'fast');
  for (const key of ['coding_agent.request.service_tier', 'coding_agent.response.service_tier',
    'coding_agent.request.reasoning.effort', 'coding_agent.interaction.first_response_ms']) {
    assert.equal(key in attrs, false);
  }
  assert.equal(turnB.attributes['coding_agent.response.first_text_ms'], 50);
  assert.equal('coding_agent.request.duration_ms' in turnB.attributes, false);
  assert.equal(JSON.stringify([turnA, turnB]).includes(secret), false);
});

test('tool output part and missing or invalid dispatch never fabricate duration', () => {
  for (const started of [undefined, -1, true, Infinity, 200]) {
    const map = createEventMapper();
    const data = { sessionID, assistantMessageID };
    map(native(1, 'session.step.started', { ...data, started }, 100));
    map(native(2, 'session.tool.input.started', { ...data, id: 'tool', name: 'read' }, 110));
    map(native(3, 'session.step.streamed', data, 120));
    const [turn] = map(native(4, 'session.step.ended', { ...data, tokens }, 1000));
    assert.equal('coding_agent.request.duration_ms' in turn.attributes, false);
    assert.equal('ttft_ms' in turn.attributes, false);
    assert.equal(turn.attributes['gen_ai.usage.output_tokens'], 6);
  }
  const map = createEventMapper();
  const data = { sessionID, assistantMessageID };
  map(native(1, 'session.step.started', { ...data, started: 0 }, 0));
  map(native(2, 'session.tool.input.started', { ...data, id: 'tool', name: 'read' }, 0));
  map(native(3, 'session.step.streamed', data, 20));
  const [turn] = map(native(4, 'session.step.ended', { ...data, tokens }, 200));
  assert.equal(turn.attributes.ttft_ms, 0);
  assert.equal(turn.attributes['coding_agent.request.duration_ms'], 20);
  assert.equal('coding_agent.response.first_text_ms' in turn.attributes, false);
});

test('execution cleanup discards timing and unsafe variants without retaining delta content', () => {
  for (const terminal of ['session.execution.succeeded', 'session.deleted']) {
    const map = createEventMapper();
    const data = { sessionID, assistantMessageID };
    map(native(1, 'session.step.started', {
      ...data, started: 10, model: { variant: 'https://private.example/secret' },
    }, 10));
    map(native(2, 'session.text.started', { ...data, ordinal: 0 }, 20));
    map(native(3, terminal, { sessionID }, 30));
    map({ id: 'evt_late_delta', created: 25, type: 'session.text.delta',
      data: { ...data, ordinal: 0, delta: 'PRIVATE-CLEANUP-CANARY' } });
    const [turn] = map(native(4, 'session.step.ended', { ...data, tokens }, 40));
    for (const key of ['ttft_ms', 'coding_agent.response.first_text_ms',
      'coding_agent.request.duration_ms', 'coding_agent.model.variant']) {
      assert.equal(key in turn.attributes, false);
    }
    assert.equal(JSON.stringify(turn).includes('PRIVATE-CLEANUP-CANARY'), false);
  }
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
  assert.equal(JSON.parse(await readFile(json, 'utf8')).plugins[0].options.preserved, true);
  // Obsolete runtime queue options migrate away; the real install path keeps configPath.
  await writeFile(json, JSON.stringify({ plugins: [{ package: pluginDirectory,
    options: { configPath: join(home, 'otel.json'), stateRoot: '/tmp/legacy', profile: 'legacy', nodeBinary: '/tmp/legacy-node' } }] }));
  await install(args);
  const migrated = JSON.parse(await readFile(json, 'utf8')).plugins[0].options;
  assert.ok(migrated.configPath);
  assert.equal('stateRoot' in migrated, false);
  assert.equal('profile' in migrated, false);
  assert.equal('nodeBinary' in migrated, false);
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
