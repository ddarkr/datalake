import fs from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { constants } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';

const HEX48 = /^[a-f0-9]{48}$/;
const HEX64 = /^[a-f0-9]{64}$/;
const RECORD = /^([a-f0-9]{48})\.json$/;
const TEMP = /^\.(?:tmp|lock)-(\d+)-[a-f0-9-]{36}$/;
const uid = process.getuid?.();
const localTransactions = new Map();
// ponytail: a 100k-identity/128MiB ledger never evicts; retire a whole profile explicitly when full.
// Individual records are bounded to 1MiB; raise this only with a larger validated event contract.
const RECORD_LIMIT = 1024 * 1024;
const DEFAULT_ENTRIES = 100000;
const DEFAULT_BYTES = 128 * 1024 * 1024;
const invalid = () => new Error('Unsafe or invalid telemetry outbox');
// Only validated replacement/unlink windows are retryable; unsafe owners, modes and hardlinks still fail closed.
const changed = () => Object.assign(invalid(), { code: 'ESTALE' });
const absent = error => error?.code === 'ENOENT';
const retiredError = error => absent(error) || error?.code === 'ESTALE';
const execFileAsync = promisify(execFile);
const keysAre = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));

function checkStat(stat, directory) {
  if (uid === undefined || stat.uid !== uid || (stat.mode & 0o7777) !== (directory ? 0o700 : 0o600)
    || (directory ? !stat.isDirectory() : !stat.isFile())) throw invalid();
  if (!directory && stat.nlink !== 1) throw stat.nlink === 0 ? changed() : invalid();
}

async function directory(path) {
  const before = await fs.lstat(path);
  checkStat(before, true);
  const handle = await fs.open(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat();
    checkStat(stat, true);
    if (stat.ino !== before.ino || stat.dev !== before.dev) throw changed();
  } catch (error) { await handle.close(); throw error; }
  return handle;
}

async function syncDirectory(path) {
  const handle = await directory(path);
  try { await handle.sync(); } finally { await handle.close(); }
}

async function ensureRoot(root) {
  try { const handle = await directory(root); await handle.close(); return; }
  catch (error) { if (!absent(error)) throw error; }
  const first = await fs.mkdir(root, { recursive: true, mode: 0o700 });
  // Newly created ancestors also need durable directory entries; existing system ancestors may be public.
  if (first) {
    let path = root;
    for (;;) {
      await syncDirectory(path);
      const parent = dirname(path);
      const handle = await fs.open(parent, constants.O_RDONLY | constants.O_DIRECTORY);
      try { await handle.sync(); } finally { await handle.close(); }
      if (path === first) break;
      path = parent;
    }
  }
  const handle = await directory(root);
  await handle.close();
}

async function readText(path, limit = RECORD_LIMIT, sync = false) {
  const before = await fs.lstat(path);
  checkStat(before, false);
  if (before.size > limit) throw invalid();
  const handle = await fs.open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = await handle.stat();
    checkStat(stat, false);
    if (stat.ino !== before.ino || stat.dev !== before.dev) throw changed();
    if (stat.size > limit) throw invalid();
    const buffer = Buffer.alloc(stat.size + 1);
    let length = 0;
    while (length < buffer.length) {
      const { bytesRead } = await handle.read(buffer, length, buffer.length - length, length);
      if (!bytesRead) break;
      length += bytesRead;
    }
    const after = await handle.stat();
    checkStat(after, false);
    if (length !== stat.size || after.size !== stat.size) throw invalid();
    if (sync) await handle.sync();
    return { text: buffer.subarray(0, length).toString('utf8'), bytes: length };
  } finally { await handle.close(); }
}

async function readJSON(path, limit) {
  const result = await readText(path, limit);
  try { return { ...result, value: JSON.parse(result.text) }; } catch { throw invalid(); }
}

async function atomicWrite(path, text) {
  const parent = dirname(path);
  const temporary = join(parent, `.tmp-${process.pid}-${randomUUID()}`);
  let handle;
  try {
    handle = await fs.open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    checkStat(await handle.stat(), false);
    await handle.writeFile(text, 'utf8');
    await handle.sync();
    await handle.close();
    handle = undefined;
    await fs.rename(temporary, path);
    await syncDirectory(parent);
  } finally {
    await handle?.close();
    await fs.rm(temporary, { force: true });
  }
}

function routeValue(route) {
  const fields = ['key', 'endpointHash', ...(Object.hasOwn(route ?? {}, 'configPath') ? ['configPath'] : [])];
  if (!keysAre(route, fields) || typeof route.key !== 'string' || typeof route.endpointHash !== 'string'
    || !HEX64.test(route.key) || !HEX64.test(route.endpointHash)
    || (fields.length === 3 && (typeof route.configPath !== 'string' || !isAbsolute(route.configPath)
      || route.configPath.length > 4096 || /[\x00-\x1f\x7f]/.test(route.configPath)))) throw invalid();
  return { key: route.key, endpointHash: route.endpointHash, ...(fields.length === 3 ? { configPath: route.configPath } : {}) };
}

function validLimit(value) { return Number.isSafeInteger(value) && value > 0; }

async function manifest(root) {
  const { value } = await readJSON(join(root, 'manifest.json'), 8192);
  if (!keysAre(value, ['version', 'route', 'maxEntries', 'maxBytes']) || value.version !== 1
    || !validLimit(value.maxEntries) || !validLimit(value.maxBytes)) throw invalid();
  return { ...value, route: routeValue(value.route) };
}

export async function readRoute(root) {
  if (typeof root !== 'string' || !isAbsolute(root)) throw invalid();
  const handle = await directory(root);
  try { return (await manifest(root)).route; } finally { await handle.close(); }
}

function validateBody(identity, body) {
  if (typeof identity !== 'string' || !HEX48.test(identity) || typeof body !== 'string'
    || Buffer.byteLength(body) > RECORD_LIMIT - 1024) throw invalid();
  let request;
  try { request = JSON.parse(body); } catch { throw invalid(); }
  const resources = request?.resourceSpans;
  const scopes = resources?.[0]?.scopeSpans;
  const spans = scopes?.[0]?.spans;
  if (!request || typeof request !== 'object' || Array.isArray(request)
    || !Array.isArray(resources) || resources.length !== 1
    || !Array.isArray(scopes) || scopes.length !== 1 || !Array.isArray(spans) || spans.length !== 1
    || !spans[0] || typeof spans[0] !== 'object' || Array.isArray(spans[0])
    || spans[0].traceId !== identity.slice(0, 32) || spans[0].spanId !== identity.slice(32)) throw invalid();
}

const decimal = value => String(value).padStart(16, '0');
function counter(value) {
  if (typeof value !== 'string' || !/^\d{16}$/.test(value) || !Number.isSafeInteger(Number(value))) throw invalid();
  return Number(value);
}

async function record(path, identity, sync = false) {
  const { text, bytes } = await readText(path, RECORD_LIMIT, sync);
  let value;
  try { value = JSON.parse(text); } catch { throw invalid(); }
  if (value?.identity !== identity) throw invalid();
  if (value.done === true) {
    if (!keysAre(value, ['identity', 'done'])) throw invalid();
  } else {
    if (!keysAre(value, ['identity', 'body', 'attempts', 'due', 'done']) || value.done !== false) throw invalid();
    validateBody(identity, value.body);
    value.attempts = counter(value.attempts);
    value.due = counter(value.due);
  }
  return { value, bytes };
}

const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
function validIncarnation(value) {
  return keysAre(value, ['platform', 'boot', 'start']) && typeof value.boot === 'string' && UUID.test(value.boot)
    && typeof value.start === 'string' && (value.platform === 'linux' ? /^[1-9]\d{0,19}$/.test(value.start)
      : value.platform === 'darwin' && /^[1-9]\d{0,19}\.\d{6}$/.test(value.start));
}

async function kernelText(path) {
  const handle = await fs.open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    if (!(await handle.stat()).isFile()) throw invalid();
    const buffer = Buffer.alloc(8193);
    let length = 0;
    while (length < buffer.length) {
      const { bytesRead } = await handle.read(buffer, length, buffer.length - length, null);
      if (!bytesRead) break;
      length += bytesRead;
    }
    if (length === buffer.length) throw invalid();
    return buffer.subarray(0, length).toString('utf8');
  } finally { await handle.close(); }
}

// Darwin's ps start time truncates to seconds. Use the kernel's microsecond BSD start timeval instead.
// Fixed system JXA bridges libproc without a dependency/build; only PID, status and start bytes leave it.
const DARWIN_PROCESS = `ObjC.import('Foundation');
ObjC.bindFunction('proc_pidinfo', ['int', ['int', 'int', 'uint64_t', 'void *', 'int']]);
ObjC.bindFunction('malloc', ['void *', ['unsigned long']]);
ObjC.bindFunction('free', ['void', ['void *']]);
function run(argv) {
  var buffer = $.malloc(136);
  if (!buffer) throw Error('process probe failed');
  try {
    if ($.proc_pidinfo(Number(argv[0]), 3, 0, buffer, 136) !== 136) throw Error('process probe failed');
    var data = $.NSData.dataWithBytesLength(buffer, 136);
    function part(offset, length) { return ObjC.unwrap(data.subdataWithRange($.NSMakeRange(offset, length)).base64EncodedStringWithOptions(0)); }
    return JSON.stringify({pid: part(12, 4), state: part(4, 4), start: part(120, 16)});
  } finally { $.free(buffer); }
}`;
const probeOptions = { env: { PATH: '/usr/bin:/bin', LC_ALL: 'C' }, timeout: 1000, killSignal: 'SIGKILL', maxBuffer: 1024 };

async function processSnapshot(pid) {
  if (process.platform === 'linux') {
    const [stat, bootText] = await Promise.all([
      kernelText(`/proc/${pid}/stat`), kernelText('/proc/sys/kernel/random/boot_id'),
    ]);
    const end = stat.lastIndexOf(')');
    if (!stat.startsWith(`${pid} (`) || end < 0) throw invalid();
    const fields = stat.slice(end + 2).trim().split(/\s+/);
    if (!/^[RSDZTtWXxIKP]$/.test(fields[0])) throw invalid();
    const incarnation = { platform: 'linux', boot: bootText.trim().toLowerCase(), start: fields[19] };
    if (!validIncarnation(incarnation)) throw invalid();
    return { incarnation, zombie: fields[0] === 'Z' || fields[0] === 'X' || fields[0] === 'x' };
  }
  if (process.platform === 'darwin' && ['arm64', 'x64'].includes(process.arch)) {
    const [proc, boot] = await Promise.all([
      execFileAsync('/usr/bin/osascript', ['-l', 'JavaScript', '-e', DARWIN_PROCESS, String(pid)], probeOptions),
      execFileAsync('/usr/sbin/sysctl', ['-n', 'kern.bootsessionuuid'], probeOptions),
    ]);
    const value = JSON.parse(proc.stdout);
    if (!keysAre(value, ['pid', 'state', 'start']) || typeof value.pid !== 'string'
      || typeof value.state !== 'string' || typeof value.start !== 'string') throw invalid();
    const actualPid = Buffer.from(value.pid, 'base64'), state = Buffer.from(value.state, 'base64'), start = Buffer.from(value.start, 'base64');
    if (actualPid.length !== 4 || state.length !== 4 || start.length !== 16 || actualPid.readUInt32LE() !== pid) throw invalid();
    const status = state.readUInt32LE();
    const usec = start.readBigUInt64LE(8);
    if (status < 1 || status > 5 || usec > 999999n) throw invalid();
    const incarnation = { platform: 'darwin', boot: boot.stdout.trim().toLowerCase(),
      start: `${start.readBigUInt64LE()}.${String(usec).padStart(6, '0')}` };
    if (!validIncarnation(incarnation)) throw invalid();
    return { incarnation, zombie: status === 5 };
  }
  throw invalid();
}

let ownIncarnation;
export async function processIncarnation(pid = process.pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) throw invalid();
  if (pid === process.pid && ownIncarnation) return { ...ownIncarnation };
  try {
    const { incarnation } = await processSnapshot(pid);
    if (pid === process.pid) ownIncarnation = incarnation;
    return { ...incarnation };
  } catch { return null; } // Probe failure publishes a legacy owner, never an invented identity.
}

export async function ownerIsDead(value) {
  if (!Number.isSafeInteger(value?.pid) || value.pid <= 0
    || (Object.hasOwn(value, 'incarnation') && !validIncarnation(value.incarnation))) throw invalid();
  if (value.pid === process.pid && !Object.hasOwn(value, 'incarnation')) return false;
  try { process.kill(value.pid, 0); }
  catch (error) { return error.code === 'ESRCH'; }
  try {
    const current = await processSnapshot(value.pid);
    return current.zombie || (Object.hasOwn(value, 'incarnation')
      && ['platform', 'boot', 'start'].some(key => value.incarnation[key] !== current.incarnation[key]));
  } catch {
    // Darwin proc_pidinfo returns no BSD info for zombies; status alone proves they cannot own live work.
    if (process.platform === 'darwin') {
      try {
        const { stdout } = await execFileAsync('/bin/ps', ['-p', String(value.pid), '-o', 'stat='], probeOptions);
        if (/^Z\S*$/.test(stdout.trim())) return true;
      } catch { /* An unavailable status probe is not proof of death. */ }
    }
    // It may have exited between kill(0) and the probe; every other failure stays conservative.
    try { process.kill(value.pid, 0); } catch (error) { return error.code === 'ESRCH'; }
    return false;
  }
}

async function privateTree(path) {
  const stat = await fs.lstat(path);
  checkStat(stat, stat.isDirectory());
  if (stat.isDirectory()) for (const name of await fs.readdir(path)) await privateTree(join(path, name));
}

async function owner(path, legacyPidOnly = false) {
  const handle = await directory(path);
  try {
    await privateTree(path);
    const { value } = await readJSON(join(path, 'owner.json'), 1024);
    const fields = ['pid', ...(Object.hasOwn(value ?? {}, 'token') ? ['token'] : []),
      ...(Object.hasOwn(value ?? {}, 'incarnation') ? ['incarnation'] : [])];
    if (!keysAre(value, fields) || !Number.isSafeInteger(value.pid) || value.pid <= 0
      || (Object.hasOwn(value, 'token') ? typeof value.token !== 'string' || !UUID.test(value.token)
        : !legacyPidOnly || Object.hasOwn(value, 'incarnation'))
      || (Object.hasOwn(value, 'incarnation') && !validIncarnation(value.incarnation))) throw invalid();
    const current = await fs.lstat(path);
    checkStat(current, true);
    const opened = await handle.stat();
    checkStat(opened, true);
    if (current.ino !== opened.ino || current.dev !== opened.dev) throw changed();
    return value;
  } finally { await handle.close(); }
}

async function removeLock(root, path, token, legacyPidOnly = false) {
  let current;
  try { current = await owner(path, legacyPidOnly); } catch (error) { if (retiredError(error)) return; throw error; }
  if (current.token !== token) return;
  const retired = join(root, `.lock-${process.pid}-${randomUUID()}`);
  await fs.rename(path, retired);
  await syncDirectory(dirname(path));
  await fs.rm(retired, { recursive: true });
  await syncDirectory(root);
}

export async function acquireProcessLock(root, path, { waitMs = 0, legacyPidOnly = false } = {}) {
  const parent = await directory(root);
  await parent.close();
  const token = randomUUID();
  const candidate = join(root, `.lock-${process.pid}-${token}`);
  await fs.mkdir(candidate, { mode: 0o700 });
  let acquired = false;
  try {
    const incarnation = await processIncarnation();
    await atomicWrite(join(candidate, 'owner.json'), JSON.stringify({ pid: process.pid, token, ...(incarnation ? { incarnation } : {}) }));
    // Waiting is bounded; a live owner is never evicted by age, even when its identity probe fails.
    const until = Date.now() + waitMs;
    for (;;) {
      try {
        await fs.rename(candidate, path);
        acquired = true;
        await syncDirectory(root);
        if (dirname(path) !== root) await syncDirectory(dirname(path));
        return async () => removeLock(root, path, token, legacyPidOnly);
      } catch (error) {
        if (acquired) { await removeLock(root, path, token, legacyPidOnly); throw error; }
        if (absent(error)) return null; // A recovering parent lock was atomically retired.
        if (!['EEXIST', 'ENOTEMPTY'].includes(error.code)) throw error;
      }
      let current;
      try { current = await owner(path, legacyPidOnly); } catch (error) { if (retiredError(error)) continue; throw error; }
      if (await ownerIsDead(current)) {
        // A published incarnation-owned reaper serializes recovery and can itself recover after a crash.
        const reaper = await acquireProcessLock(root, join(path, 'reaping'));
        if (reaper) {
          try {
            const latest = await owner(path, legacyPidOnly);
            if (await ownerIsDead(latest)) await removeLock(root, path, latest.token, legacyPidOnly);
          } catch (error) { if (!retiredError(error)) throw error; }
          finally { await reaper(); }
          continue;
        }
      }
      if (!waitMs || Date.now() >= until) return null;
      await delay(10);
    }
  } finally {
    if (!acquired) await fs.rm(candidate, { recursive: true, force: true });
  }
}

async function withProducer(root, action) {
  const previous = localTransactions.get(root) ?? Promise.resolve();
  const operation = previous.catch(() => {}).then(async () => {
    const handle = await directory(root);
    try {
      const release = await acquireProcessLock(root, join(root, 'producer.lock'), { waitMs: 30000 });
      if (!release) throw invalid();
      try { return await action(); } finally { await release(); }
    } finally { await handle.close(); }
  });
  localTransactions.set(root, operation);
  try { return await operation; }
  finally { if (localTransactions.get(root) === operation) localTransactions.delete(root); }
}

async function cleanTemporary(directoryPath) {
  for (const name of await fs.readdir(directoryPath)) {
    const match = TEMP.exec(name);
    if (!match) continue;
    const path = join(directoryPath, name);
    try {
      const stat = await fs.lstat(path);
      checkStat(stat, name.startsWith('.lock-'));
      if (stat.isDirectory()) await privateTree(path);
      if (await ownerIsDead({ pid: Number(match[1]) })) await fs.rm(path, { recursive: stat.isDirectory(), force: true });
    } catch (error) { if (!absent(error)) throw error; } // A live contender may retire its own candidate.
  }
}

async function names(root) {
  const path = join(root, 'records');
  const handle = await directory(path);
  try {
    const result = [];
    for (const name of await fs.readdir(path)) {
      if (RECORD.test(name)) result.push(name);
      else if (!TEMP.test(name)) throw invalid();
    }
    return result.sort();
  } finally { await handle.close(); }
}

async function summary(root) {
  const { value } = await readJSON(join(root, 'ledger.json'), 1024);
  if (!keysAre(value, ['pending', 'done', 'bytes'])
    || !Object.values(value).every(number => Number.isSafeInteger(number) && number >= 0)) throw invalid();
  return value;
}

async function repair(root) {
  const result = { pending: 0, done: 0, bytes: 0 };
  for (const name of await names(root)) {
    const entry = await record(join(root, 'records', name), RECORD.exec(name)[1], true);
    result[entry.value.done ? 'done' : 'pending']++;
    result.bytes += entry.bytes;
    if (!Number.isSafeInteger(result.bytes)) throw invalid();
  }
  await syncDirectory(join(root, 'records'));
  await atomicWrite(join(root, 'ledger.json'), JSON.stringify(result));
  await fs.rm(join(root, 'dirty.json'), { force: true });
  await syncDirectory(root);
  return result;
}

async function recover(root) {
  let dirty;
  try { dirty = (await readJSON(join(root, 'dirty.json'), 1024)).value; }
  catch (error) { if (!absent(error)) throw error; }
  if (dirty !== undefined) {
    if (!keysAre(dirty, ['version']) || dirty.version !== 1) throw invalid();
    await cleanTemporary(root);
    await cleanTemporary(join(root, 'records'));
    return repair(root);
  }
  try { return await summary(root); }
  catch (error) { if (!absent(error)) throw error; return repair(root); }
}

async function commitRecord(root, identity, value, totals) {
  await atomicWrite(join(root, 'dirty.json'), '{"version":1}');
  await atomicWrite(join(root, 'records', `${identity}.json`), JSON.stringify(value));
  await atomicWrite(join(root, 'ledger.json'), JSON.stringify(totals));
  await fs.rm(join(root, 'dirty.json'));
  await syncDirectory(root);
}

export async function openOutbox(options) {
  if (!options || typeof options.root !== 'string' || !isAbsolute(options.root)) throw invalid();
  const root = resolve(options.root);
  const route = routeValue(options.route);
  for (const key of ['maxEntries', 'maxBytes']) if (options[key] !== undefined && !validLimit(options[key])) throw invalid();
  await ensureRoot(root);
  const settings = await withProducer(root, async () => {
    const recordsPath = join(root, 'records');
    try { await fs.mkdir(recordsPath, { mode: 0o700 }); await syncDirectory(root); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
    const handle = await directory(recordsPath);
    await handle.close();
    await cleanTemporary(root);
    await cleanTemporary(recordsPath);
    let current;
    try { current = await manifest(root); }
    catch (error) {
      if (!absent(error)) throw error;
      // Missing manifests are safe only before any ledger identity has been accepted.
      if ((await names(root)).length) throw invalid();
      current = { version: 1, route, maxEntries: options.maxEntries ?? DEFAULT_ENTRIES, maxBytes: options.maxBytes ?? DEFAULT_BYTES };
      await atomicWrite(join(root, 'manifest.json'), JSON.stringify(current));
    }
    if (JSON.stringify(current.route) !== JSON.stringify(route)
      || ['maxEntries', 'maxBytes'].some(key => options[key] !== undefined && options[key] !== current[key])) throw invalid();
    await recover(root);
    return current;
  });

  const transaction = action => withProducer(root, async () => {
    const current = await manifest(root);
    if (JSON.stringify(current) !== JSON.stringify(settings)) throw invalid();
    const totals = await recover(root);
    return action(totals);
  });

  return {
    async put({ identity, body } = {}) {
      try {
        validateBody(identity, body);
        return await transaction(async totals => {
          const path = join(root, 'records', `${identity}.json`);
          try {
            await record(path, identity, true);
            await syncDirectory(join(root, 'records'));
            return true; // The first accepted body wins, including after delivery.
          } catch (error) { if (!absent(error)) throw error; }
          const value = { identity, body, attempts: decimal(0), due: decimal(Date.now()), done: false };
          const bytes = Buffer.byteLength(JSON.stringify(value));
          if (bytes > RECORD_LIMIT || totals.pending + totals.done >= settings.maxEntries
            || totals.bytes + bytes > settings.maxBytes) return false;
          await commitRecord(root, identity, value, { ...totals, pending: totals.pending + 1, bytes: totals.bytes + bytes });
          return true;
        });
      } catch { return false; }
    },
    async pending() {
      const snapshot = await transaction(() => names(root));
      const result = [];
      for (const name of snapshot) {
        const { value } = await record(join(root, 'records', name), RECORD.exec(name)[1]);
        if (!value.done) result.push({ identity: value.identity, body: value.body, attempts: value.attempts, due: value.due });
      }
      return result;
    },
    async finish(identity, delivered) {
      if (typeof identity !== 'string' || !HEX48.test(identity) || typeof delivered !== 'boolean') throw invalid();
      await transaction(async totals => {
        const previous = await record(join(root, 'records', `${identity}.json`), identity);
        if (previous.value.done) return;
        let value;
        if (delivered) value = { identity, done: true };
        else {
          const attempts = Math.min(Number.MAX_SAFE_INTEGER, previous.value.attempts + 1);
          const backoff = Math.min(60000, 1000 * 2 ** Math.min(attempts - 1, 6));
          value = { identity, body: previous.value.body, attempts: decimal(attempts), due: decimal(Date.now() + backoff), done: false };
        }
        const bytes = Buffer.byteLength(JSON.stringify(value));
        await commitRecord(root, identity, value, {
          pending: totals.pending - Number(delivered), done: totals.done + Number(delivered), bytes: totals.bytes - previous.bytes + bytes,
        });
      });
    },
    async acquireWorker() {
      const releaseOwner = await transaction(() => acquireProcessLock(root, join(root, 'worker.lock')));
      if (!releaseOwner) return null;
      let released = false;
      const release = async () => transaction(async () => {
        if (!released) { await releaseOwner(); released = true; }
      });
      release.releaseIfEmpty = async () => transaction(async totals => {
        if (released) return true;
        if (totals.pending !== 0) return false;
        await releaseOwner();
        released = true;
        return true;
      });
      return release;
    },
    async status() { return transaction(totals => ({ ...totals })); },
  };
}
