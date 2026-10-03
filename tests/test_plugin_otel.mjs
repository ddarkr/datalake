import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { chownSync, chmodSync, existsSync, linkSync, mkdtempSync, readFileSync, realpathSync, symlinkSync, writeFileSync, statSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';
import { execFile, spawn, spawnSync } from 'node:child_process';
import { mkdir, readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { createTelemetry, loadConfig, validateConfig, parseHeaders, repositoryContext, serializeEvent, sendPayload } from '../plugins/otel.mjs';
import { openOutbox, readRoute } from '../plugins/outbox.mjs';
import { readSenderStatus } from '../plugins/sender.mjs';

const requests = [];
let responseBody = '{}';
let responseStatus = 200;
let responseLocation;
let holdResponses = false;
const heldResponses = [];
const diagnostics = [];
const originalError = console.error;
console.error = message => diagnostics.push(String(message));
const server = createServer(async (request, response) => {
  let body = '';
  for await (const chunk of request) body += chunk;
  requests.push({ path: request.url, authorization: request.headers.authorization, rawBody: body, body: JSON.parse(body) });
  if (holdResponses) { heldResponses.push(response); return; }
  response.writeHead(responseStatus, { 'Content-Type': 'application/json', ...(responseLocation ? { Location: responseLocation } : {}) });
  response.end(responseBody);
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const endpoint = `http://127.0.0.1:${server.address().port}/otel`;
const directory = mkdtempSync(join(tmpdir(), 'datalake-plugin-test-'));
const roots = new Set(), children = new Set();
const senderPath = fileURLToPath(new URL('../plugins/sender.mjs', import.meta.url));
const otelUrl = new URL('../plugins/otel.mjs', import.meta.url).href;
const execFileAsync = promisify(execFile);
const registry = join(directory, 'worker-pids');
const nodeBinary = join(directory, 'tracked-node');
const shellQuote = value => `'${value.replaceAll("'", "'\\''")}'`;
writeFileSync(nodeBinary, `#!/bin/sh\numask 077\nprintf '%s\\t%s\\n' "$$" "$2" >> ${shellQuote(registry)}\nexec ${shellQuote(process.execPath)} "$@"\n`, { mode: 0o700 });

async function bounded(operation, description, timeout = 30000) {
  let timer;
  try {
    return await Promise.race([operation, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out: ${description}`)), timeout);
    })]);
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

function producer(name, options = {}) {
  const stateRoot = join(directory, name);
  roots.add(stateRoot);
  return createTelemetry('codex', { endpoint, headers: { Authorization: 'Basic fixture-only' }, timeoutMs: 30000, stateRoot, nodeBinary, ...options });
}

async function ledger(stateRoot) {
  const names = await readdir(stateRoot);
  assert.equal(names.length, 1);
  const root = join(stateRoot, names[0]);
  const route = await readRoute(root);
  return { root, route, queue: await openOutbox({ root, route }) };
}

async function drained(item, count) {
  await until(async () => {
    const status = await item.queue.status();
    return status.pending === 0 && status.done === count && !existsSync(join(item.root, 'worker.lock'));
  }, 'durable delivery and worker release');
  assert.equal((await readSenderStatus(item.root))?.state, 'idle');
}

async function privateSpool(root) {
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) await privateSpool(path);
    else {
      try { assert.doesNotMatch(await readFile(path, 'utf8'), /PRIVATE_|fixture-only|fixture\$secret|resourceSpans.*PRIVATE_|http:\/\/127\.0\.0\.1:/); }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
  }
}

async function ownedWorker(pid, root) {
  try {
    const { stdout } = await execFileAsync('ps', ['-p', String(pid), '-o', 'stat=', '-o', 'command='], { timeout: 5000, maxBuffer: 16384 });
    const match = /^\s*(\S+)\s+([\s\S]*)$/.exec(stdout);
    return Boolean(match && !match[1].startsWith('Z') && match[2].includes(senderPath) && match[2].includes(root));
  } catch (error) {
    if (error.code === 1 || error.code === 'ESRCH') return false;
    throw error;
  }
}
try {
  const client = producer('wire');
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
  assert.deepEqual(wire.resourceSpans[0].resource.attributes, [{ key: 'service.name', value: { stringValue: 'codex' } }]);
  assert.deepEqual(wire.resourceSpans[0].scopeSpans[0].scope, { name: 'doda-datalake', version: '1.0.0' });
  const named = JSON.parse(serializeEvent('codex', { ...event, attributes: { ...event.attributes, 'service.name': 'fixture-service' } }).body);
  assert.equal(named.resourceSpans[0].resource.attributes[0].value.stringValue, 'fixture-service');
  for (const invalid of [null, {}, { ...event, eventId: 'PRIVATE_INVALID_ID\n' }, { ...event, endTimeMs: 1 }]) {
    assert.throws(() => serializeEvent('codex', invalid));
  }
  assert.throws(() => serializeEvent('PRIVATE_INVALID_CLIENT', event));
  const invalidRoot = join(directory, 'invalid');
  const invalidClient = producer('invalid');
  assert.equal(await invalidClient.enqueue({ ...event, endTimeMs: 1 }), false);
  await invalidClient.flushLocal();
  assert.equal(existsSync(invalidRoot), false, 'Projection rejects invalid metadata before creating disk state');

  holdResponses = true;
  assert.equal(await client.enqueue(event), true);
  await bounded(client.flushLocal(), 'local flush while collector ACK is held');
  await until(() => heldResponses.length === 1, 'real sender request');
  const item = await ledger(join(directory, 'wire'));
  assert.equal((await item.queue.status()).pending, 1);
  assert.equal((await item.queue.status()).done, 0);
  assert.equal((await item.queue.pending())[0].body, serialized.body);
  assert.equal(await client.enqueue({ ...event, attributes: { ...event.attributes, cost_usd: 99 } }), true);
  assert.equal((await item.queue.pending())[0].body, serialized.body, 'First accepted body is immutable');
  await client.flushLocal();
  await privateSpool(item.root);
  assert.equal(heldResponses.length, 1, 'Duplicate enqueue does not start a duplicate HTTP owner');
  holdResponses = false;
  heldResponses.splice(0).forEach(response => { response.writeHead(200, { 'Content-Type': 'application/json' }); response.end('{}'); });
  await drained(item, 1);
  assert.equal(requests.length, 1);
  const span = requests[0].body.resourceSpans[0].scopeSpans[0].spans[0];
  const attributes = Object.fromEntries(span.attributes.map(({ key, value }) => [key, value]));
  assert.equal(requests[0].path, '/otel/v1/traces');
  assert.equal(requests[0].authorization, 'Basic fixture-only');
  assert.equal(requests[0].rawBody, serialized.body);
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
  const count = requests.length;
  assert.equal(await client.enqueue({ ...event, endTimeMs: 1 }), false);
  for (const eventId of ['', 'line\nbreak', 'tab\tid', 'has space', 'x'.repeat(513)]) {
    assert.equal(await client.enqueue({ ...event, eventId }), false);
  }
  await client.flushLocal();
  assert.equal(requests.length, count);

  // A real producer process exits naturally while its detached sender owns a held ACK.
  holdResponses = true;
  const exitRoot = join(directory, 'natural-exit');
  roots.add(exitRoot);
  const exitOptions = { endpoint, headers: { Authorization: 'Basic fixture-only' }, stateRoot: exitRoot, nodeBinary, profile: 'captured-profile', timeoutMs: 30000 };
  const child = spawn(process.execPath, ['--input-type=module', '-e', `
    import {createTelemetry} from ${JSON.stringify(otelUrl)};
    const telemetry=createTelemetry('codex',${JSON.stringify(exitOptions)});
    const accepted=telemetry.enqueue(${JSON.stringify({ ...event, eventId: 'natural-exit' })});
    await telemetry.flushLocal();
    if(!await accepted) process.exitCode=1;
  `], { stdio: ['ignore', 'pipe', 'pipe'] });
  children.add(child);
  let childOutput = '';
  child.stdout.on('data', chunk => { childOutput += chunk; });
  child.stderr.on('data', chunk => { childOutput += chunk; });
  const childClosed = once(child, 'close');
  await until(() => heldResponses.length === 1, 'detached producer HTTP while ACK held');
  const [exitCode, signal] = await bounded(childClosed, 'natural producer exit before HTTP acknowledgment');
  assert.equal(exitCode, 0, childOutput);
  assert.equal(signal, null);
  assert.doesNotMatch(childOutput, /PRIVATE_|fixture-only|http:\/\//);
  const exitLedger = await ledger(exitRoot);
  assert.equal((await exitLedger.queue.status()).pending, 1);
  holdResponses = false;
  heldResponses.splice(0).forEach(response => { response.writeHead(200); response.end('{}'); });
  await drained(exitLedger, 1);

  // A launch failure cannot revoke durable acceptance; capacity and in-flight refusal stay false.
  const capacityClient = producer('capacity', { maxEntries: 1, nodeBinary: join(directory, 'nonexistent-node') });
  const localOperations = Array.from({ length: 129 }, (_, index) => capacityClient.enqueue({ ...event, eventId: `capacity-${index}` }));
  assert.equal(await localOperations.at(-1), false, 'Bounded local work refuses rather than claiming queued');
  await capacityClient.flushLocal();
  const accepted = await Promise.all(localOperations);
  assert.equal(accepted.filter(Boolean).length, 1);
  const capacityLedger = await ledger(join(directory, 'capacity'));
  assert.equal((await capacityLedger.queue.status()).pending, 1);
  assert.equal((await readSenderStatus(capacityLedger.root))?.state, 'pending-launch');
  assert.equal(await capacityClient.enqueue({ ...event, eventId: `capacity-${accepted.findIndex(Boolean)}` }), true, 'Durable duplicate remains accepted at capacity');
  assert.equal(await capacityClient.enqueue({ ...event, eventId: 'capacity-extra' }), false);
  const changedCapacity = producer('capacity', { maxEntries: 2, nodeBinary: join(directory, 'nonexistent-node') });
  assert.equal(await changedCapacity.enqueue({ ...event, eventId: 'capacity-expanded' }), false, 'Pinned limits do not hot-resize an existing ledger');
  await changedCapacity.flushLocal();
  await privateSpool(capacityLedger.root);

  const disabled = producer('disabled', { endpoint: undefined, configPath: join(directory, 'missing-config.json') });
  assert.equal(await disabled.enqueue(event), false);
  await disabled.flushLocal();
  assert.equal(existsSync(join(directory, 'disabled')), false);

  const recoveryRoot = join(directory, 'recoverable-state');
  writeFileSync(recoveryRoot, 'local obstacle', { mode: 0o600 });
  const recoveringClient = producer('recoverable-state');
  assert.equal(await recoveringClient.enqueue({ ...event, eventId: 'recoverable-local-failure' }), false);
  await recoveringClient.flushLocal();
  rmSync(recoveryRoot);
  assert.equal(await recoveringClient.enqueue({ ...event, eventId: 'recoverable-local-failure' }), true);
  await recoveringClient.flushLocal();
  const recoveryLedger = await ledger(recoveryRoot);
  await drained(recoveryLedger, 1);

  const repository = join(directory, 'repository');
  await mkdir(repository, { mode: 0o700 });
  await execFileAsync('git', ['init', '--initial-branch=fixture-branch', repository]);
  const repositoryAttributes = await repositoryContext(repository);
  assert.deepEqual(repositoryAttributes, {
    'coding_agent.repository.id': createHash('sha256').update(realpathSync(repository)).digest('hex'),
    'vcs.ref.head.name': 'fixture-branch',
  });
  assert.deepEqual(await repositoryContext(join(directory, 'not-a-repository')), {});
  assert.deepEqual(await repositoryContext(null), {});

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

  // Destination, client and captured profile keep independent durable dedup scopes.
  const sharedBase = join(directory, 'scopes');
  roots.add(sharedBase);
  const scopeOptions = { endpoint, stateRoot: sharedBase, nodeBinary };
  const profiles = [
    createTelemetry('codex', { ...scopeOptions, profile: 'profile-a' }),
    createTelemetry('codex', { ...scopeOptions, profile: 'profile-b' }),
    createTelemetry('agy', { ...scopeOptions, profile: 'profile-a' }),
    createTelemetry('codex', { ...scopeOptions, profile: 'profile-a', endpoint: `${endpoint}/other-destination` }),
  ];
  for (const telemetry of profiles) assert.equal(await telemetry.enqueue(event), true);
  for (const telemetry of profiles) await telemetry.flushLocal();
  const scopedRoots = await readdir(sharedBase);
  assert.equal(scopedRoots.length, profiles.length);
  for (const name of scopedRoots) {
    const root = join(sharedBase, name), route = await readRoute(root);
    await drained({ root, queue: await openOutbox({ root, route }) }, 1);
    await privateSpool(root);
  }

  const protectedPath = join(directory, 'captured-protected.json');
  writeFileSync(protectedPath, JSON.stringify({ endpoint, headers: { Authorization: 'Basic fixture-only' } }), { mode: 0o600 });
  const savedEnv = { DATALAKE_OTEL_CONFIG: process.env.DATALAKE_OTEL_CONFIG, OTEL_EXPORTER_OTLP_ENDPOINT: process.env.OTEL_EXPORTER_OTLP_ENDPOINT };
  let protectedClient;
  try {
    process.env.DATALAKE_OTEL_CONFIG = protectedPath;
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = `${endpoint}/PRIVATE_AMBIENT_CONFIG_CANARY`;
    protectedClient = producer('protected-source', { endpoint: undefined, profile: 'captured-profile' });
    process.env.DATALAKE_OTEL_CONFIG = join(directory, 'missing-config-after-capture.json');
    assert.equal(await protectedClient.enqueue({ ...event, eventId: 'protected-source' }), true);
    await protectedClient.flushLocal();
  } finally {
    for (const [key, value] of Object.entries(savedEnv)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
  const protectedLedger = await ledger(join(directory, 'protected-source'));
  assert.equal(protectedLedger.route.configPath, protectedPath);
  await drained(protectedLedger, 1);
  await privateSpool(protectedLedger.root);
  const protectedSpanId = serializeEvent('codex', { ...event, eventId: 'protected-source' }).identity.slice(32);
  const protectedRequest = requests.find(request => request.body.resourceSpans[0].scopeSpans[0].spans[0].spanId === protectedSpanId);
  assert.equal(protectedRequest.authorization, 'Basic fixture-only');
  assert.equal(protectedRequest.path, '/otel/v1/traces');
  const fileScopeClient = createTelemetry('codex', { stateRoot: sharedBase, configPath: protectedPath, nodeBinary, profile: 'profile-a' });
  assert.equal(await fileScopeClient.enqueue(event), true);
  await fileScopeClient.flushLocal();
  const normalizedFileClient = createTelemetry('codex', { stateRoot: sharedBase, configPath: relative(process.cwd(), protectedPath), nodeBinary, profile: 'profile-a' });
  assert.equal(await normalizedFileClient.enqueue({ ...event, attributes: { ...event.attributes, cost_usd: 42 } }), true);
  await normalizedFileClient.flushLocal();
  const fileScopes = await readdir(sharedBase);
  assert.equal(fileScopes.length, scopedRoots.length + 1, 'File source separates inline config while normalized paths share dedup');
  const fileRoot = join(sharedBase, fileScopes.find(name => !scopedRoots.includes(name)));
  const fileRoute = await readRoute(fileRoot);
  await drained({ root: fileRoot, queue: await openOutbox({ root: fileRoot, route: fileRoute }) }, 1);

  // Environment credentials cross only transient bootstrap; both source and default state home are captured.
  const scratchHome = join(directory, 'scratch-home');
  const capturedStateHome = join(directory, 'captured-state-home');
  const envBase = join(capturedStateHome, 'doda-datalake', 'otel');
  roots.add(envBase);
  await mkdir(scratchHome, { mode: 0o700 });
  const childEnv = { ...process.env, HOME: scratchHome, XDG_STATE_HOME: capturedStateHome, OTEL_EXPORTER_OTLP_ENDPOINT: endpoint, OTEL_EXPORTER_OTLP_HEADERS: 'Authorization=Basic%20fixture-only' };
  delete childEnv.DATALAKE_OTEL_CONFIG;
  const envChild = spawn(process.execPath, ['--input-type=module', '-e', `
    import {createTelemetry} from ${JSON.stringify(otelUrl)};
    const telemetry=createTelemetry('codex',{nodeBinary:${JSON.stringify(nodeBinary)}});
    process.env.HOME=${JSON.stringify(join(directory, 'changed-home'))};
    process.env.XDG_STATE_HOME=${JSON.stringify(join(directory, 'changed-state-home'))};
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT=${JSON.stringify(`${endpoint}/PRIVATE_CHANGED_ENDPOINT_CANARY`)};
    process.env.OTEL_EXPORTER_OTLP_HEADERS='Authorization=PRIVATE_CHANGED_HEADER_CANARY';
    if(!await telemetry.enqueue(${JSON.stringify({ ...event, eventId: 'environment-source' })})) process.exitCode=1;
    await telemetry.flushLocal();
  `], { env: childEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  children.add(envChild);
  let envOutput = '';
  envChild.stdout.on('data', chunk => { envOutput += chunk; });
  envChild.stderr.on('data', chunk => { envOutput += chunk; });
  const [envCode] = await bounded(once(envChild, 'close'), 'isolated environment producer exit');
  assert.equal(envCode, 0, envOutput);
  assert.doesNotMatch(envOutput, /PRIVATE_|fixture-only|http:\/\//);
  const envLedger = await ledger(envBase);
  assert.equal(Object.hasOwn(envLedger.route, 'configPath'), false);
  await drained(envLedger, 1);
  await privateSpool(envLedger.root);
  const envSpanId = serializeEvent('codex', { ...event, eventId: 'environment-source' }).identity.slice(32);
  const envRequest = requests.find(request => request.body.resourceSpans[0].scopeSpans[0].spans[0].spanId === envSpanId);
  assert.equal(envRequest.authorization, 'Basic fixture-only');
  assert.equal(envRequest.path, '/otel/v1/traces');
  assert.equal(existsSync(join(directory, 'changed-state-home')), false);

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
  assert.doesNotMatch(diagnostics.join('\n'), /PRIVATE_|fixture-only|fixture\$secret|DO_NOT_COPY_STORAGE_SECRET|resourceSpans/);
  console.log('test_plugin_otel: durable local acceptance, detached delivery, OTLP privacy and protected config passed');
} finally {
  const workers = [];
  try {
    for (const line of readFileSync(registry, 'utf8').trim().split('\n')) {
      const [text, root] = line.split('\t');
      const pid = Number(text);
      if (Number.isSafeInteger(pid) && pid > 0 && [...roots].some(base => root.startsWith(`${base}/`))) workers.push({ pid, root });
    }
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  try {
    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      for (const stream of child.stdio) stream?.destroy();
    }
    for (const { pid, root } of workers) {
      if (await ownedWorker(pid, root)) {
        try { process.kill(pid, 'SIGKILL'); }
        catch (error) { if (error.code !== 'ESRCH') throw error; }
      }
    }
    await until(async () => (await Promise.all(workers.map(({ pid, root }) => ownedWorker(pid, root)))).every(value => !value), 'owned worker cleanup');
  } finally {
    console.error = originalError;
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}
