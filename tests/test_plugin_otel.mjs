import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, writeFileSync, statSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createTelemetry, loadConfig, validateConfig, parseHeaders } from '../plugins/otel.mjs';

const requests = [];
let responseBody = '{}';
let responseStatus = 200;
const server = createServer(async (request, response) => {
  let body = '';
  for await (const chunk of request) body += chunk;
  requests.push({ path: request.url, authorization: request.headers.authorization, body: JSON.parse(body) });
  response.writeHead(responseStatus, { 'Content-Type': 'application/json' });
  response.end(responseBody);
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const endpoint = `http://127.0.0.1:${server.address().port}/otel`;
const directory = mkdtempSync(join(tmpdir(), 'datalake-plugin-test-'));
try {
  const client = createTelemetry('codex', { endpoint, headers: { Authorization: 'Basic fixture-only' } });
  const event = {
    kind: 'llm.turn', sessionId: 'session-1', eventId: 'turn-1|provider-item', parentEventId: 'session-1|provider-root',
    startTimeMs: 1780000000000, endTimeMs: 1780000000012.5,
    attributes: {
      'gen_ai.request.model': 'model-fixture', 'gen_ai.usage.input_tokens': 7,
      'gen_ai.usage.output_tokens': 3, cost_usd: 0.0123,
      'coding_agent.client': 'spoofed', 'coding_agent.content_capture_mode': 'full',
      'coding_agent.repository.id': '/private/repository',
      'gen_ai.prompt': 'PRIVATE_PROMPT_CANARY', 'tool.arguments': { token: 'PRIVATE_ARGUMENT_CANARY' },
      'error.message': 'PRIVATE_ERROR_CANARY', 'gen_ai.usage.cache_read.input_tokens': -1,
      'gen_ai.usage.reasoning.output_tokens': Number.NaN,
    },
    events: [{ name: 'PRIVATE_EVENT_CANARY' }], prompt: 'PRIVATE_ROOT_CANARY',
  };
  assert.equal(await client.emit(event), true);
  assert.equal(await client.emit(event), true);
  await client.flush();
  assert.equal(requests.length, 2);
  const span = requests[0].body.resourceSpans[0].scopeSpans[0].spans[0];
  const second = requests[1].body.resourceSpans[0].scopeSpans[0].spans[0];
  const attributes = Object.fromEntries(span.attributes.map(({ key, value }) => [key, value]));
  assert.equal(requests[0].path, '/otel/v1/traces');
  assert.equal(requests[0].authorization, 'Basic fixture-only');
  assert.equal(span.traceId, second.traceId);
  assert.equal(span.spanId, second.spanId);
  assert.match(span.traceId, /^[a-f0-9]{32}$/);
  assert.match(span.parentSpanId, /^[a-f0-9]{16}$/);
  assert.equal(span.endTimeUnixNano, '1780000000012500000');
  assert.deepEqual(attributes['gen_ai.usage.input_tokens'], { intValue: '7' });
  assert.deepEqual(attributes.cost_usd, { doubleValue: 0.0123 });
  assert.deepEqual(attributes['coding_agent.client'], { stringValue: 'codex' });
  assert.deepEqual(attributes['coding_agent.content_capture_mode'], { stringValue: 'metadata_only' });
  assert.equal(attributes['coding_agent.repository.id'], undefined);
  assert.equal(attributes['gen_ai.usage.cache_read.input_tokens'], undefined);
  assert.equal(attributes['gen_ai.usage.reasoning.output_tokens'], undefined);
  assert.doesNotMatch(JSON.stringify(requests), /PRIVATE_|spoofed|\/private\/repository/);
  responseBody = JSON.stringify({ partialSuccess: { rejectedSpans: '1', errorMessage: 'PRIVATE_UPSTREAM_CANARY' } });
  assert.equal(await client.emit({ ...event, eventId: 'rejected' }), false);
  responseStatus = 401;
  assert.equal(await client.emit({ ...event, eventId: 'unauthorized' }), false);
  const count = requests.length;
  assert.equal(await client.emit({ ...event, endTimeMs: 1 }), false);
  assert.equal(requests.length, count);
  for (const eventId of ['', 'line\nbreak', 'tab\tid', 'has space', 'x'.repeat(513)]) {
    assert.equal(await client.emit({ ...event, eventId }), false);
  }
  assert.equal(requests.length, count);

  for (const bad of ['http://remote.example:4318', 'https://user:secret@example.org', 'https://example.org?secret=canary', 'file:///private']) {
    assert.throws(() => validateConfig({ endpoint: bad }));
  }
  assert.throws(() => validateConfig({ endpoint, headers: { Host: 'other.example' } }));
  assert.throws(() => validateConfig({ endpoint, headers: { Authorization: 'line\r\ninjection' } }));
  assert.deepEqual(parseHeaders('Authorization=Basic%20a%3Db%3D'), { Authorization: 'Basic a=b=' });
  assert.equal(validateConfig({ endpoint: endpoint + '/v1/traces' }).endpoint, endpoint + '/v1/traces');
  const lanEndpoint = 'http://192.168.99.10:4318';
  assert.throws(() => validateConfig({ endpoint: lanEndpoint }));
  assert.throws(() => validateConfig({ endpoint: lanEndpoint, allowInsecureHttp: 'true' }));
  assert.equal(validateConfig({ endpoint: lanEndpoint, allowInsecureHttp: true }).endpoint, lanEndpoint + '/v1/traces');
  for (const unsafe of ['http://8.8.8.8', 'http://192.168.99.10.example.org', 'http://172.32.0.1']) {
    assert.throws(() => validateConfig({ endpoint: unsafe, allowInsecureHttp: true }));
  }

  const envPath = join(directory, '.env');
  const configPath = join(directory, 'private', 'otel.json');
  writeFileSync(envPath, "OTLP_USER=fixture\nOTLP_PASSWORD='fixture$secret'\nS3_SECRET_ACCESS_KEY=DO_NOT_COPY_STORAGE_SECRET\n", { mode: 0o600 });
  const args = [resolve('plugins/configure.mjs'), '--endpoint', `${endpoint}/PRIVATE_ENDPOINT_TOKEN`, '--from-env-file', envPath, '--config', configPath];
  const configure = spawnSync(process.execPath, [...args, '--apply'], { encoding: 'utf8' });
  assert.equal(configure.status, 0, configure.stderr);
  assert.doesNotMatch(configure.stdout + configure.stderr, /fixture\$secret|DO_NOT_COPY_STORAGE_SECRET|PRIVATE_ENDPOINT_TOKEN/);
  assert.equal(statSync(configPath).mode & 0o777, 0o600);
  const config = loadConfig({ configPath });
  assert.equal(config.headers.Authorization, 'Basic ' + Buffer.from('fixture:fixture$secret').toString('base64'));
  assert.doesNotMatch(readFileSync(configPath, 'utf8'), /S3_|DO_NOT_COPY/);
  const before = readFileSync(configPath, 'utf8');
  assert.notEqual(spawnSync(process.execPath, [...args, '--apply'], { encoding: 'utf8' }).status, 0);
  assert.equal(readFileSync(configPath, 'utf8'), before);
  const lanArgs = [resolve('plugins/configure.mjs'), '--endpoint', lanEndpoint, '--from-env-file', envPath, '--config', configPath, '--apply', '--replace'];
  assert.notEqual(spawnSync(process.execPath, lanArgs, { encoding: 'utf8' }).status, 0);
  assert.equal(readFileSync(configPath, 'utf8'), before);
  const allowLan = spawnSync(process.execPath, [...lanArgs, '--allow-insecure-http'], { encoding: 'utf8' });
  assert.equal(allowLan.status, 0, allowLan.stderr);
  assert.equal(loadConfig({ configPath }).endpoint, lanEndpoint + '/v1/traces');
  console.log('test_plugin_otel: real OTLP wire privacy, stable IDs, rejection, URL safety, and private config preservation passed');
} finally {
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
  rmSync(directory, { recursive: true, force: true });
}
