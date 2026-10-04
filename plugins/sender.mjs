import { execFile, spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, constants, writeSync } from 'node:fs';
import fs from 'node:fs/promises';
import { delimiter, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { isMain, loadConfig, sendPayload, validateConfig } from './otel.mjs';
import { openOutbox, readRoute } from './outbox.mjs';

const bootstrapLimit = 65536;
const bootstrapTimeout = 5000;
const endpointHash = endpoint => createHash('sha256').update(endpoint).digest('hex');
const warning = () => console.error('[datalake-otel] Sender unavailable; accepted telemetry remains pending');

const senderStates = new Set(['pending-http', 'pending-config', 'pending-launch', 'idle']);
const statusLimit = 256;
const uid = process.getuid?.();

function privateStatusStat(stat, directory = false) {
  if (uid === undefined || stat.uid !== uid || (stat.mode & 0o7777) !== (directory ? 0o700 : 0o600)
    || (directory ? !stat.isDirectory() : !stat.isFile() || stat.nlink !== 1)) throw new Error('sender-status');
}

async function statusDirectory(root) {
  await readRoute(root);
  const handle = await fs.open(root, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try { privateStatusStat(await handle.stat(), true); return handle; }
  catch (error) { await handle.close(); throw error; }
}

// Only fixed metadata crosses back to producers; malformed or unsafe files reveal nothing.
export async function readSenderStatus(root) {
  let directory, handle;
  try {
    directory = await statusDirectory(root);
    handle = await fs.open(join(root, 'sender-status.json'), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const before = await handle.stat();
    privateStatusStat(before);
    if (before.size > statusLimit) return null;
    const buffer = Buffer.alloc(before.size + 1);
    let length = 0;
    while (length < buffer.length) {
      const { bytesRead } = await handle.read(buffer, length, buffer.length - length, length);
      if (!bytesRead) break;
      length += bytesRead;
    }
    const after = await handle.stat();
    privateStatusStat(after);
    if (length !== before.size || after.size !== before.size) return null;
    const value = JSON.parse(buffer.subarray(0, length).toString('utf8'));
    if (!value || Array.isArray(value) || Object.keys(value).length !== 2
      || !senderStates.has(value.state) || !Number.isSafeInteger(value.timestamp) || value.timestamp < 0) return null;
    return { state: value.state, timestamp: value.timestamp };
  } catch { return null; }
  finally { await handle?.close(); await directory?.close(); }
}

async function writeSenderStatus(root, state) {
  if (!senderStates.has(state)) throw new Error('sender-status');
  const directory = await statusDirectory(root);
  const path = join(root, 'sender-status.json');
  const temporary = join(root, `.tmp-${process.pid}-${randomUUID()}`);
  let handle;
  try {
    try { privateStatusStat(await fs.lstat(path)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    handle = await fs.open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    privateStatusStat(await handle.stat());
    await handle.writeFile(JSON.stringify({ state, timestamp: Date.now() }), 'utf8');
    await handle.sync();
    await handle.close();
    handle = undefined;
    await fs.rename(temporary, path);
    await directory.sync();
  } finally {
    await handle?.close();
    await fs.rm(temporary, { force: true });
    await directory.close();
  }
}

function runtimeEnvironment() {
  const env = { PATH: '/usr/bin:/bin' };
  // Never inherit agent secrets, preload flags, proxies, TLS-disable or key-log settings.
  for (const name of ['NODE_EXTRA_CA_CERTS', 'NODE_USE_SYSTEM_CA', 'SSL_CERT_FILE', 'SSL_CERT_DIR']) {
    if (process.env[name] !== undefined) env[name] = process.env[name];
  }
  // Relative CA paths must not change meaning when the child enters its private outbox.
  for (const name of ['NODE_EXTRA_CA_CERTS', 'SSL_CERT_FILE', 'SSL_CERT_DIR']) {
    if (env[name]) env[name] = resolve(env[name]);
  }
  return env;
}

const inside = (path, root) => {
  const suffix = relative(root, path);
  return suffix === '' || (suffix !== '..' && !suffix.startsWith(`..${sep}`) && !isAbsolute(suffix));
};

async function projectRoots(cwd) {
  const roots = [cwd];
  for (let directory = cwd; ; directory = dirname(directory)) {
    for (const marker of ['.git', 'package.json']) {
      try {
        await fs.lstat(join(directory, marker));
        roots.push(directory);
        if (marker === '.git') return roots;
        break;
      }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
    if (dirname(directory) === directory) return roots;
  }
}

async function trustedExecutable(path, roots) {
  if (typeof path !== 'string' || !isAbsolute(path) || roots.some(root => inside(resolve(path), root))) throw new Error('sender-runtime');
  const canonical = await fs.realpath(path);
  if (roots.some(root => inside(canonical, root))) throw new Error('sender-runtime');
  // Check the lookup path and symlink target, including their writable ancestors.
  for (const start of new Set([resolve(path), canonical])) {
    for (let current = start; ; current = dirname(current)) {
      const stat = await fs.stat(current);
      const stickySystemDirectory = stat.isDirectory() && stat.uid === 0 && (stat.mode & 0o1000);
      if ((stat.uid !== 0 && stat.uid !== uid) || ((stat.mode & 0o022) && !stickySystemDirectory)
        || (current === start ? !stat.isFile() : !stat.isDirectory())) throw new Error('sender-runtime');
      if (dirname(current) === current) break;
    }
  }
  await fs.access(canonical, constants.X_OK);
  // Discovery never executes scripts masquerading as a runtime.
  const handle = await fs.open(canonical, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    if (!(await handle.stat()).isFile()) throw new Error('sender-runtime');
    const magic = Buffer.alloc(4);
    if ((await handle.read(magic, 0, 4, 0)).bytesRead !== 4
      || !['7f454c46', 'feedface', 'feedfacf', 'cefaedfe', 'cffaedfe', 'cafebabe', 'bebafeca', 'cafebabf', 'bfbafeca'].includes(magic.toString('hex'))) throw new Error('sender-runtime');
  } finally { await handle.close(); }
  return canonical;
}

async function actualNode(path, root, env, timeout) {
  const output = await new Promise((resolveOutput, reject) => {
    execFile(path, ['--input-type=module', '-e', 'console.log(JSON.stringify({node:process.versions.node,bun:!!process.versions.bun,path:process.execPath}))'], {
      cwd: root, env, shell: false, timeout, maxBuffer: 1024,
    }, (error, stdout) => error ? reject(new Error('sender-runtime')) : resolveOutput(stdout));
  });
  const value = JSON.parse(output);
  if (typeof value.node !== 'string' || value.bun || await fs.realpath(value.path) !== path) throw new Error('sender-runtime');
  return path;
}

async function currentNodeImage(roots) {
  if (process.platform !== 'linux' || !process.versions.node || process.versions.bun || !isAbsolute(process.execPath)) throw new Error('sender-runtime');
  const image = '/proc/self/exe';
  const canonical = await fs.realpath(image);
  if (roots.some(root => inside(resolve(process.execPath), root) || inside(canonical, root))) throw new Error('sender-runtime');
  const running = await fs.stat(image);
  const reported = await fs.stat(process.execPath);
  if (!running.isFile() || (running.uid !== 0 && running.uid !== uid)
    || running.dev !== reported.dev || running.ino !== reported.ino) throw new Error('sender-runtime');
  // Linux pins the executing inode and forbids writes to it (ETXTBSY). Exec through
  // procfs, not its replaceable tool-cache pathname; PATH discovery stays strict.
  return image;
}

let pinnedNode;
async function defaultNode(root, env) {
  if (pinnedNode) return pinnedNode;
  const roots = await projectRoots(await fs.realpath(process.cwd()));
  try { pinnedNode = await currentNodeImage(roots); return pinnedNode; }
  catch { /* Other hosts and spoofed/project runtimes use strict path discovery. */ }
  const candidates = [];
  if (process.versions.node && !process.versions.bun) candidates.push(process.execPath);
  for (const directory of (process.env.PATH || '').split(delimiter)) {
    if (isAbsolute(directory)) candidates.push(join(directory, 'node'));
  }
  const deadline = Date.now() + bootstrapTimeout;
  for (const candidate of new Set(candidates)) {
    if (Date.now() >= deadline) break;
    try {
      const path = await trustedExecutable(candidate, roots);
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;
      pinnedNode = await actualNode(path, root, env, remaining);
      return pinnedNode;
    } catch { /* Unsafe or unavailable candidates never receive a credential bootstrap. */ }
  }
  throw new Error('sender-runtime');
}

function bootstrapChild(child, body, waitForReady) {
  return new Promise((resolveReady, reject) => {
    let settled = false;
    let written = false;
    let ready = false;
    let received = '';
    const timer = setTimeout(() => finish(new Error('sender-bootstrap')), bootstrapTimeout);
    const finish = error => {
      if (settled) return;
      if (!error && (!written || (waitForReady && !ready))) return;
      settled = true;
      clearTimeout(timer);
      child.stdin?.destroy();
      child.stdio[3]?.destroy();
      if (error) reject(error);
      else resolveReady();
    };
    child.on('error', () => finish(new Error('sender-spawn')));
    child.once('close', () => finish(new Error('sender-bootstrap')));
    if (!child.stdin || !child.stdio[3]) { finish(new Error('sender-bootstrap')); return; }
    child.stdin.on('error', () => finish(new Error('sender-bootstrap')));
    child.stdio[3].on('error', () => finish(new Error('sender-bootstrap')));
    child.stdio[3].on('data', chunk => {
      received += chunk.toString('utf8');
      if (received.length > 6 || !'ready\n'.startsWith(received)) return finish(new Error('sender-bootstrap'));
      ready = received === 'ready\n';
      finish();
    });
    child.stdio[3].once('end', () => { if (!ready) finish(new Error('sender-bootstrap')); });
    child.stdin.end(body, () => { written = true; finish(); });
  });
}

export async function startSender({ root, route, config, nodeBinary, waitForReady = true }) {
  let child;
  try {
    const outbox = await openOutbox({ root, route });
    if (!(await outbox.status()).pending) return;
    const release = await outbox.acquireWorker();
    if (!release) return;
    await release();
    // A live owner handles every new enqueue; only startup races can launch losing candidates.
    const env = runtimeEnvironment();
    // Resolve the runtime before serializing or handing credentials to any child.
    const runtime = nodeBinary === undefined ? await defaultNode(resolve(root), env) : await fs.realpath(resolve(nodeBinary));
    await fs.access(runtime, constants.X_OK);
    const bootstrap = route.configPath ? {} : { config: validateConfig(config) };
    if (!route.configPath && endpointHash(bootstrap.config.endpoint) !== route.endpointHash) throw new Error('sender-route');
    const body = JSON.stringify(bootstrap);
    if (Buffer.byteLength(body) > bootstrapLimit) throw new Error('sender-bootstrap');
    child = spawn(runtime, [fileURLToPath(import.meta.url), resolve(root)], {
      cwd: resolve(root), shell: false, detached: true, stdio: ['pipe', 'ignore', 'ignore', 'pipe'], env,
    });
    child.unref();
    // Pipe completion guarantees transfer even if a short-lived host exits before worker readiness.
    await bootstrapChild(child, body, waitForReady);
  } catch {
    if (child?.pid) child.kill('SIGTERM');
    await writeSenderStatus(root, 'pending-launch').catch(() => {});
    warning();
  }
}

async function readBootstrap() {
  const timeout = setTimeout(() => process.stdin.destroy(new Error('sender-bootstrap')), bootstrapTimeout);
  const chunks = [];
  let bytes = 0;
  try {
    for await (const chunk of process.stdin) {
      bytes += chunk.byteLength;
      if (bytes > bootstrapLimit) throw new Error('sender-bootstrap');
      chunks.push(chunk);
    }
    const value = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => key !== 'config')) throw new Error('sender-bootstrap');
    return value;
  } finally {
    clearTimeout(timeout);
    process.stdin.destroy();
  }
}

async function runSender() {
  if (!process.versions.node || process.versions.bun || process.argv.length !== 3) throw new Error('sender-runtime');
  const bootstrap = await readBootstrap();
  const root = process.argv[2];
  const route = await readRoute(root);
  let inlineConfig;
  if (route.configPath) {
    if (Object.hasOwn(bootstrap, 'config')) throw new Error('sender-bootstrap');
  } else {
    inlineConfig = validateConfig(bootstrap.config);
    if (endpointHash(inlineConfig.endpoint) !== route.endpointHash) throw new Error('sender-route');
  }
  const outbox = await openOutbox({ root, route });
  const release = await outbox.acquireWorker();
  let owned = Boolean(release);
  try {
    try { writeSync(3, 'ready\n'); }
    catch (error) { if (error.code !== 'EPIPE') throw error; }
    finally { closeSync(3); }
    if (!release) return;
    while (true) {
      const pending = await outbox.pending();
      if (!pending.length) {
        await writeSenderStatus(root, 'idle');
        if (await release.releaseIfEmpty()) { owned = false; return; }
        continue;
      }
      const now = Date.now();
      const due = pending.filter(record => record.due <= now);
      if (!due.length) {
        // Poll at most once a second so a new entry need not wait behind an older retry.
        const nextDue = pending.reduce((earliest, record) => Math.min(earliest, record.due), Infinity);
        await delay(Math.min(1000, Math.max(1, nextDue - now)));
        continue;
      }
      for (const record of due) {
        let config;
        try {
          config = route.configPath ? loadConfig({ configPath: route.configPath }) : inlineConfig;
          if (!config || endpointHash(config.endpoint) !== route.endpointHash) throw new Error('sender-route');
        } catch {
          config = undefined;
        }
        const delivered = config ? await sendPayload(config, record.body) : false;
        if (!delivered) await writeSenderStatus(root, config ? 'pending-http' : 'pending-config');
        await outbox.finish(record.identity, delivered);
      }
    }
  } finally {
    if (owned) await release();
  }
}

if (isMain(import.meta.url)) {
  try { await runSender(); }
  catch {
    await writeSenderStatus(process.argv[2], 'pending-launch').catch(() => {});
    warning();
    process.exitCode = 1;
  }
}
