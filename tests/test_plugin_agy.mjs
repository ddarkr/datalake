import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, writeFile, rm, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { handleHook } from '../plugins/agy/hook.mjs';
import { createTelemetry } from '../plugins/otel.mjs';

async function sandbox(t, steps = []) {
  const path = await mkdtemp(join(tmpdir(), 'datalake-agy-'));
  const sessionId = `agy-${randomUUID()}`;
  const state = join(tmpdir(), 'doda-datalake-agy', sessionId);
  const transcript = join(path, 'transcript.jsonl');
  await writeFile(transcript, steps.map(step => JSON.stringify(step)).join('\n') + '\n');
  t.after(async () => { await rm(path, { recursive: true, force: true }); await rm(state, { recursive: true, force: true }); });
  return { path, state, sessionId, transcript };
}

const steps = () => [
  { type: 'USER_INPUT', step_index: 0, created_at: new Date(1000).toISOString(), content: 'synthetic request' },
  { type: 'PLANNER_RESPONSE', step_index: 1, created_at: new Date(1100).toISOString(), content: 'synthetic response', tool_calls: [{ name: 'invoke_subagent', args: { Subagents: [{ Role: 'Explore' }, { Role: 'Plan' }] } }] },
  { type: 'PLANNER_RESPONSE', step_index: 2, created_at: new Date(1200).toISOString(), content: 'synthetic follow-up' },
];

for (const failure of ['false', 'throw']) {
  for (const failedKind of ['llm.turn', 'subagent']) {
    test(`AGY retries ${failedKind} ${failure} without advancing past failures or duplicating ACKed usage`, async t => {
      const box = await sandbox(t, steps()), acknowledged = [], attempts = [];
      const child0 = `subagent:${box.sessionId}:1:0`, child1 = `subagent:${box.sessionId}:1:1`;
      const failingId = failedKind === 'llm.turn' ? 'turn:1' : child1;
      let fail = true;
      const telemetry = {
        emit: async event => {
          attempts.push(event.eventId);
          if (event.eventId === failingId && fail) {
            fail = false;
            if (failure === 'throw') throw new Error('synthetic transport failure');
            return false;
          }
          acknowledged.push(event);
          return true;
        },
        flush: async () => {},
      };
      const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
      await invoke();
      if (failedKind === 'subagent') {
        const cursor = JSON.parse(await readFile(join(box.state, 'cursor.json'), 'utf8'));
        assert.equal(cursor.lastProcessedIndex, -1);
        assert.deepEqual(cursor.sentEventIds, ['turn:1', child0]);
      }
      await invoke();
      await invoke();
      assert.deepEqual(acknowledged.filter(event => event.kind === 'llm.turn').map(event => event.eventId), ['turn:1', 'turn:2']);
      assert.deepEqual(acknowledged.filter(event => event.kind === 'subagent').map(event => event.eventId), [child0, child1]);
      assert.deepEqual(attempts.filter(id => id === failingId), [failingId, failingId]);
      if (failedKind === 'subagent') assert.deepEqual(attempts.filter(id => id === 'turn:1'), ['turn:1']);
      assert.equal(JSON.parse(await readFile(join(box.state, 'cursor.json'), 'utf8')).lastProcessedIndex, 2);
    });
  }

  test(`AGY preserves the observed tool completion and duration through ${failure} retry`, async t => {
    const box = await sandbox(t), attempted = [];
    let fail = true;
    const telemetry = {
      emit: async event => {
        attempted.push(event);
        if (fail) {
          fail = false;
          if (failure === 'throw') throw new Error('synthetic transport failure');
          return false;
        }
        return true;
      }, flush: async () => {},
    };
    const payload = { conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 7, toolCall: { name: 'Bash' } };
    await handleHook(payload, { telemetry, cwd: box.path, now: () => 1000, isPreTool: true });
    const first = handleHook({ ...payload, error: 'SYNTHETIC_PRIVATE_ERROR_MARKER' }, { telemetry, cwd: box.path, now: () => 1100 });
    if (failure === 'throw') await assert.rejects(first, /synthetic transport failure/);
    else await first;
    const retained = JSON.parse(await readFile(join(box.state, 'tool-7.json'), 'utf8'));
    assert.equal(retained.event.attributes.duration_ms, 100);
    assert.equal(retained.event.endTimeMs, 1100);
    const retried = await handleHook(payload, { telemetry, cwd: box.path, now: () => 2000 });
    assert.deepEqual(attempted[1], attempted[0]);
    assert.equal(retried[0].attributes.duration_ms, 100);
    assert.equal(retried[0].error, true);
    await assert.rejects(stat(join(box.state, 'tool-7.json')), { code: 'ENOENT' });
  });
}

test('AGY serializes concurrent transcript drains without duplicate usage', async t => {
  const box = await sandbox(t, steps()), acknowledged = [];
  let entered, release, first = true;
  const started = new Promise(resolve => { entered = resolve; });
  const blocked = new Promise(resolve => { release = resolve; });
  const telemetry = {
    emit: async event => {
      if (event.kind === 'llm.turn' && first) { first = false; entered(); await blocked; }
      acknowledged.push(event);
      return true;
    }, flush: async () => {},
  };
  const invoke = () => handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, terminationReason: 'model_stop' }, { telemetry, cwd: box.path, now: () => 1500 });
  const one = invoke();
  await started;
  const two = invoke();
  await delay(20);
  release();
  await Promise.all([one, two]);
  assert.deepEqual(acknowledged.filter(event => event.kind === 'llm.turn').map(event => event.eventId), ['turn:1', 'turn:2']);
});

test('AGY tool errors export only a fixed category on the serialized OTLP wire', async t => {
  const box = await sandbox(t), bodies = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    bodies.push(body);
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end('{}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const telemetry = createTelemetry('agy', { endpoint: `http://127.0.0.1:${server.address().port}` });
  await handleHook({ conversationId: box.sessionId, transcriptPath: box.transcript, stepIdx: 1, toolName: 'Bash', error: 'SYNTHETIC_PRIVATE_ERROR_MARKER credential=fixture' }, { telemetry, cwd: box.path, now: () => 1100 });
  const spans = bodies.flatMap(body => JSON.parse(body).resourceSpans.flatMap(resource => resource.scopeSpans.flatMap(scope => scope.spans)));
  assert.deepEqual(spans.map(span => span.status.code), [2]);
  assert.deepEqual(spans[0].attributes.find(attribute => attribute.key === 'error.type').value, { stringValue: 'tool_error' });
  assert.doesNotMatch(bodies.join(''), /SYNTHETIC_PRIVATE_ERROR_MARKER|credential=fixture/);
});
