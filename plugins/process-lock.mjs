import fs from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { constants } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { dirname, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';

const invalid = () => new Error('Unsafe or invalid process lock');
// Only validated replacement/unlink windows are retryable; unsafe owners, modes and hardlinks still fail closed.
const changed = () => Object.assign(invalid(), { code: 'ESTALE' });
const absent = error => error?.code === 'ENOENT';
const retiredError = error => absent(error) || error?.code === 'ESTALE';
const execFileAsync = promisify(execFile);
const keysAre = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));

function checkStat(stat, directory) {
  if (process.getuid?.() === undefined || stat.uid !== process.getuid() || (stat.mode & 0o7777) !== (directory ? 0o700 : 0o600)
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

async function readText(path, limit = 1024 * 1024) {
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
