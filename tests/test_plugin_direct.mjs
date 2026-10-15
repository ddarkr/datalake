import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFile, spawnSync } from 'node:child_process';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { chownSync, chmodSync, existsSync, linkSync, mkdtempSync, readFileSync, realpathSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { mkdtemp, rm, writeFile, readFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';
import { createDirectTelemetry, loadConfig, parseHeaders, repositoryContext, sendPayload, serializeEvent, validateConfig, withStateLock } from '../plugins/otel.mjs';
import { acquireProcessLock, ownerIsDead, processIncarnation } from '../plugins/process-lock.mjs';

const execFileAsync = promisify(execFile);

test('performance metadata preserves observations without fabricating timing or tier provenance', () => {
  const project = (attributes, kind = 'llm.turn') => {
    const result = serializeEvent('codex', { kind, sessionId: 'session', eventId: 'event', startTimeMs: 100, endTimeMs: 100, attributes });
    const span = JSON.parse(result.body).resourceSpans[0].scopeSpans[0].spans[0];
    return { span, body: result.body, attrs: Object.fromEntries(span.attributes.map(({ key, value }) => [key, value])) };
  };
  const numeric = ['ttft_ms', 'first_content_ms', 'coding_agent.request.duration_ms', 'coding_agent.response.first_text_ms', 'coding_agent.interaction.first_response_ms', 'coding_agent.request.reasoning.budget_tokens'];
  for (const key of numeric) {
    for (const value of [undefined, null, true, false, '0', -1, NaN, Infinity, -Infinity]) {
      assert.equal(project({ [key]: value, 'coding_agent.ttft.kind': 'provider_reported' }).attrs[key], undefined);
    }
    assert.deepEqual(project({ [key]: 0, 'coding_agent.ttft.kind': 'provider_reported' }).attrs[key], { intValue: '0' });
  }
  assert.equal(project({ 'coding_agent.request.reasoning.budget_tokens': 1.5 }).attrs['coding_agent.request.reasoning.budget_tokens'], undefined);
  assert.equal(project({ ttft_ms: 1 }).attrs.ttft_ms, undefined);
  const observed = {
    'coding_agent.request.duration_ms': 10, 'coding_agent.duration.kind': 'stream',
    ttft_ms: 2, 'coding_agent.ttft.kind': 'first_output_part', first_content_ms: 3,
    'coding_agent.response.first_text_ms': 4, 'coding_agent.usage.source': 'reported',
    'coding_agent.request.service_tier': 'auto', 'coding_agent.harness.service_tier': 'priority',
    'coding_agent.request.reasoning.effort': 'high', 'coding_agent.request.settings_source': 'wire',
    'coding_agent.model.variant': 'model-high', 'coding_agent.harness.version': '1.2.3',
  };
  const valid = project(observed).attrs;
  for (const [key, value] of Object.entries(observed)) {
    assert.deepEqual(valid[key], typeof value === 'string' ? { stringValue: value } : { intValue: String(value) });
  }
  assert.equal(valid['coding_agent.response.service_tier'], undefined);
  assert.deepEqual(project({ 'coding_agent.response.service_tier': 'flex' }).attrs['coding_agent.response.service_tier'], { stringValue: 'flex' });
  for (const key of ['coding_agent.ttft.kind', 'coding_agent.duration.kind', 'coding_agent.request.service_tier', 'coding_agent.response.service_tier', 'coding_agent.harness.service_tier', 'coding_agent.request.reasoning.effort', 'coding_agent.request.settings_source', 'coding_agent.usage.source', 'coding_agent.interaction.latency.kind']) {
    assert.equal(project({ [key]: 'unknown' }).attrs[key], undefined);
  }
  assert.deepEqual(project({ 'coding_agent.interaction.latency.kind': 'hook_submit_to_display_ready' }, 'interaction').attrs['coding_agent.interaction.latency.kind'], { stringValue: 'hook_submit_to_display_ready' });
  for (const key of Object.keys(observed).filter(key => typeof observed[key] === 'string')) {
    const invalid = project({ [key]: 'PRIVATE BODY\nsecret', 'gen_ai.prompt': 'PRIVATE BODY' });
    assert.equal(invalid.attrs[key], undefined);
    assert.doesNotMatch(invalid.body, /PRIVATE BODY/);
  }
  for (const key of ['ttft_ms', 'first_content_ms', 'coding_agent.response.first_text_ms']) {
    assert.equal(project({ ...observed, [key]: 11 }).attrs[key], undefined);
  }
  const interaction = project({
    'coding_agent.interaction.first_response_ms': 500,
    'coding_agent.interaction.latency.kind': 'submit_to_display_ready',
    'coding_agent.request.duration_ms': 10, 'gen_ai.usage.output_tokens': 9,
    'pi.gen_ai.usage.total_tokens': 12,
  }, 'interaction');
  assert.equal(interaction.span.name, 'coding_agent.interaction');
  assert.deepEqual(interaction.attrs['coding_agent.interaction.first_response_ms'], { intValue: '500' });
  assert.equal(interaction.attrs['gen_ai.usage.output_tokens'], undefined);
  assert.equal(interaction.attrs['pi.gen_ai.usage.total_tokens'], undefined);
});

// Shared contract: stable identity/privacy/ID boundaries, repository context,
// protected config, sendPayload response parsing, and direct HTTP regression.
// Real loopback collector; no durable queue, no sender, no disk spool.
test('shared otel contract and direct HTTP regression', async t => {
  const diagnostics = [];
  const originalError = console.error;
  console.error = message => diagnostics.push(String(message));
  const requests = [];
  let responseBody = '{}';
  let responseStatus = 200;
  let responseLocation;
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push({ path: request.url, authorization: request.headers.authorization, rawBody: body, body: JSON.parse(body) });
    response.writeHead(responseStatus, { 'Content-Type': 'application/json', ...(responseLocation ? { Location: responseLocation } : {}) });
    response.end(responseBody);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const endpoint = `http://127.0.0.1:${server.address().port}/otel`;
  const directory = mkdtempSync(join(tmpdir(), 'datalake-plugin-test-'));
  const home = await mkdtemp(join(tmpdir(), 'datalake-direct-regression-'));
  t.after(async () => {
    console.error = originalError;
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
    await rm(home, { recursive: true, force: true });
  });
  try {
    // serializeEvent: stable identity, privacy projection, wire shape, ID boundaries.
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
    const serialized = serializeEvent('codex', event);
    const wire = JSON.parse(serialized.body);
    const projected = wire.resourceSpans[0].scopeSpans[0].spans[0];
    const stableHash = (value, length) => createHash('sha256').update(JSON.stringify(value)).digest('hex').slice(0, length);
    assert.equal(projected.traceId, stableHash(['codex', event.sessionId], 32));
    assert.equal(projected.spanId, stableHash(['codex', event.sessionId, event.eventId], 16));
    assert.equal(projected.parentSpanId, stableHash(['codex', event.sessionId, event.parentEventId], 16));
    assert.equal(serialized.identity, projected.traceId + projected.spanId);
    assert.match(serialized.identity, /^[a-f0-9]{48}$/);
    assert.deepEqual(serializeEvent('codex', event), serialized);
    assert.equal(projected.startTimeUnixNano, '1780000000000000000');
    assert.equal(projected.endTimeUnixNano, '1780000000012500000');
    assert.deepEqual(wire.resourceSpans[0].resource.attributes, [{ key: 'service.name', value: { stringValue: 'codex' } }]);
    assert.deepEqual(wire.resourceSpans[0].scopeSpans[0].scope, { name: 'doda-datalake', version: '1.0.0' });
    const named = JSON.parse(serializeEvent('codex', { ...event, attributes: { ...event.attributes, 'service.name': 'fixture-service' } }).body);
    assert.equal(named.resourceSpans[0].resource.attributes[0].value.stringValue, 'fixture-service');
    for (const invalid of [null, {}, { ...event, eventId: 'PRIVATE_INVALID_ID\n' }, { ...event, endTimeMs: 1 }]) {
      assert.throws(() => serializeEvent('codex', invalid));
    }
    assert.throws(() => serializeEvent('PRIVATE_INVALID_CLIENT', event));
    const attributes = Object.fromEntries(projected.attributes.map(({ key, value }) => [key, value]));
    assert.deepEqual(attributes['gen_ai.usage.input_tokens'], { intValue: '7' });
    assert.deepEqual(attributes.cost_usd, { doubleValue: 0.0123 });
    assert.deepEqual(attributes['coding_agent.client'], { stringValue: 'codex' });
    assert.deepEqual(attributes['coding_agent.content_capture_mode'], { stringValue: 'metadata_only' });
    assert.equal(attributes['coding_agent.repository.id'], undefined);
    assert.equal(attributes['gen_ai.usage.cache_read.input_tokens'], undefined);
    assert.equal(attributes['gen_ai.usage.reasoning.output_tokens'], undefined);
    assert.doesNotMatch(JSON.stringify(wire), /PRIVATE_|spoofed|\/private\/repository/);

    // Direct send: one HTTP request per send, collector path/auth/body, no retry.
    const telemetry = createDirectTelemetry('codex', { endpoint, headers: { Authorization: 'Basic fixture-only' }, timeoutMs: 30000 });
    assert.equal(telemetry.enabled, true);
    assert.equal(await telemetry.send(event), true);
    await telemetry.flush();
    assert.equal(requests.length, 1);
    const span = requests[0].body.resourceSpans[0].scopeSpans[0].spans[0];
    assert.equal(requests[0].path, '/otel/v1/traces');
    assert.equal(requests[0].authorization, 'Basic fixture-only');
    assert.equal(requests[0].rawBody, serialized.body);
    assert.match(span.traceId, /^[a-f0-9]{32}$/);
    assert.match(span.parentSpanId, /^[a-f0-9]{16}$/);
    assert.doesNotMatch(JSON.stringify(requests), /PRIVATE_|spoofed|\/private\/repository/);
    const count = requests.length;
    assert.equal(await telemetry.send({ ...event, endTimeMs: 1 }), false);
    for (const eventId of ['', 'line\nbreak', 'tab\tid', 'has space', 'x'.repeat(513)]) {
      assert.equal(await telemetry.send({ ...event, eventId }), false);
    }
    await telemetry.flush();
    assert.equal(requests.length, count, 'invalid events make no HTTP attempt');
    const disabled = createDirectTelemetry('codex', { configPath: join(home, 'missing-config.json') });
    assert.equal(disabled.enabled, false);
    assert.equal(await disabled.send(event), false);
    await disabled.flush();

    // sendPayload response parsing: partial success, malformed, oversize, redirect, HTTP failure.
    responseBody = JSON.stringify({ partialSuccess: { rejectedSpans: '1', errorMessage: 'PRIVATE_UPSTREAM_CANARY' } });
    const transportConfig = validateConfig({ endpoint, headers: { Authorization: 'Basic fixture-only' } });
    assert.equal(await sendPayload(transportConfig, serialized.body), false);
    for (const result of [{}, { partialSuccess: {} }, { partialSuccess: { rejectedSpans: 0 } }, { partialSuccess: { rejectedSpans: '0', errorMessage: 'PRIVATE_UPSTREAM_CANARY' } }]) {
      responseBody = JSON.stringify(result);
      assert.equal(await sendPayload(transportConfig, serialized.body), true);
      assert.equal(requests.at(-1).rawBody, serialized.body);
    }
    responseBody = '';
    assert.equal(await sendPayload(transportConfig, serialized.body), true);
    for (const result of [
      null, [], 0, true, 'PRIVATE_UPSTREAM_CANARY',
      { partialSuccess: null }, { partialSuccess: [] }, { partialSuccess: 'invalid' },
      { partialSuccess: { errorMessage: 1 } },
      ...[1, '1', -1, '-1', 0.5, '0.5', '1e0', '', ' ', null, false, [], {}, 'invalid'].map(rejectedSpans => ({ partialSuccess: { rejectedSpans } })),
    ]) {
      responseBody = JSON.stringify(result);
      assert.equal(await sendPayload(transportConfig, serialized.body), false, `Invalid collector response: ${JSON.stringify(result)}`);
    }
    for (const malformed of ['{', 'PRIVATE_UPSTREAM_CANARY', '   ']) {
      responseBody = malformed;
      assert.equal(await sendPayload(transportConfig, serialized.body), false);
    }
    responseBody = JSON.stringify({ padding: 'x'.repeat(65536 - '{"padding":""}'.length) });
    assert.equal(Buffer.byteLength(responseBody), 65536);
    assert.equal(await sendPayload(transportConfig, serialized.body), true);
    responseBody = JSON.stringify({ padding: 'x'.repeat(65537 - '{"padding":""}'.length) });
    assert.equal(await sendPayload(transportConfig, serialized.body), false);
    responseBody = '{}';
    responseLocation = endpoint + '/redirected';
    for (const status of [301, 302, 303, 307, 308]) {
      responseStatus = status;
      const beforeRedirect = requests.length;
      assert.equal(await sendPayload(transportConfig, serialized.body), false);
      assert.equal(requests.length, beforeRedirect + 1, 'redirect must not make a second request');
    }
    responseLocation = undefined;
    for (const status of [401, 503]) {
      responseStatus = status;
      assert.equal(await sendPayload(transportConfig, serialized.body), false);
    }
    responseStatus = 200;

    // repositoryContext: real git repo boundary vs non-repo/null.
    const repository = join(directory, 'repository');
    await mkdir(repository, { mode: 0o700 });
    await execFileAsync('git', ['init', '--initial-branch=fixture-branch', repository]);
    assert.deepEqual(await repositoryContext(repository), {
      'coding_agent.repository.id': createHash('sha256').update(realpathSync(repository)).digest('hex'),
      'vcs.ref.head.name': 'fixture-branch',
    });
    assert.deepEqual(await repositoryContext(join(directory, 'not-a-repository')), {});
    assert.deepEqual(await repositoryContext(null), {});

    // validateConfig/parseHeaders boundaries: credentials/query, headers, LAN rule.
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

    // Protected config: file ownership/no-follow/size/mode, env-file credential flow.
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
    const unsafeConfig = join(directory, 'unsafe.json');
    symlinkSync(configPath, unsafeConfig);
    assert.throws(() => loadConfig({ configPath: unsafeConfig }));
    rmSync(unsafeConfig);
    linkSync(configPath, unsafeConfig);
    assert.throws(() => loadConfig({ configPath: unsafeConfig }));
    assert.throws(() => loadConfig({ configPath }));
    rmSync(unsafeConfig);
    assert.deepEqual(loadConfig({ configPath }), config);
    writeFileSync(unsafeConfig, before, { mode: 0o600 });
    chmodSync(unsafeConfig, 0o644);
    assert.throws(() => loadConfig({ configPath: unsafeConfig }));
    chmodSync(unsafeConfig, 0o600);
    writeFileSync(unsafeConfig, JSON.stringify({ endpoint, padding: 'x'.repeat(65536) }));
    assert.throws(() => loadConfig({ configPath: unsafeConfig }));
    writeFileSync(unsafeConfig, '{PRIVATE_MALFORMED_CONFIG');
    assert.throws(() => loadConfig({ configPath: unsafeConfig }));
    assert.throws(() => loadConfig({ configPath: directory }));
    if (process.getuid?.() === 0) {
      writeFileSync(unsafeConfig, before);
      chownSync(unsafeConfig, 1, 1);
      assert.throws(() => loadConfig({ configPath: unsafeConfig }));
    }
    assert.notEqual(spawnSync(process.execPath, [...args, '--apply'], { encoding: 'utf8' }).status, 0);
    assert.equal(readFileSync(configPath, 'utf8'), before);
    const lanArgs = [resolve('plugins/configure.mjs'), '--endpoint', lanEndpoint, '--from-env-file', envPath, '--config', configPath, '--apply', '--replace'];
    assert.notEqual(spawnSync(process.execPath, lanArgs, { encoding: 'utf8' }).status, 0);
    assert.equal(readFileSync(configPath, 'utf8'), before);
    const allowLan = spawnSync(process.execPath, [...lanArgs, '--allow-insecure-http'], { encoding: 'utf8' });
    assert.equal(allowLan.status, 0, allowLan.stderr);
    assert.equal(loadConfig({ configPath }).endpoint, lanEndpoint + '/v1/traces');
    assert.ok(!existsSync(join(home, 'legacy-spool')), 'direct mode creates no disk spool');
    assert.doesNotMatch(diagnostics.join('\n'), /PRIVATE_|fixture-only|fixture\$secret|DO_NOT_COPY_STORAGE_SECRET|resourceSpans/);

    // Metadata lock still serializes the hook checkpoint.
    const lockDir = join(home, 'checkpoint');
    await mkdir(lockDir, { recursive: true, mode: 0o700 });
    const checkpoint = join(lockDir, 'cursor.json');
    await writeFile(checkpoint, 'original');
    let releaseGate;
    const gate = new Promise(r => { releaseGate = r; });
    let entered;
    const startedLock = new Promise(r => { entered = r; });
    const active = withStateLock(lockDir, async () => { entered(); await gate; });
    await startedLock;
    assert.equal(await withStateLock(lockDir, () => writeFile(checkpoint, 'wrong')), undefined);
    assert.equal(await readFile(checkpoint, 'utf8'), 'original');
    releaseGate();
    await active;
    assert.equal(await withStateLock(lockDir, async () => { await writeFile(checkpoint, 'advanced'); return 'advanced'; }), 'advanced');
    const incarnation = await processIncarnation();
    assert.ok(incarnation);
    assert.equal(await ownerIsDead({ pid: process.pid }), false);
    const second = await acquireProcessLock(lockDir, join(lockDir, 'probe.lock'));
    assert.ok(second);
    await second();
    console.log('test_plugin_direct: shared contract, direct HTTP regression, protected config, metadata lock passed');
  } finally {
    console.error = originalError;
  }
});
