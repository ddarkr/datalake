#!/usr/bin/env node
import { mkdir, unlink, lstat, open, rename } from 'node:fs/promises';
import { constants } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { homedir } from 'node:os';
import { dirname, join, resolve, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { isMain } from '../otel.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const help = `사용법: node plugins/install.mjs agy [--home DIR] [--config PATH] [--apply] [--uninstall]
기본값은 미리보기입니다. --apply만 파일을 변경합니다.
수동 전역 플러그인: ~/.gemini/config/plugins/doda-datalake/ (https://antigravity.google/docs/plugins.md)
설정 우선순위: hook --config > DATALAKE_OTEL_CONFIG > agy-arcane.json (존재 시) > otel.json.
설치 시 모든 명령에 선택한 --config 절대 경로를 고정합니다.
`;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const quote = value => `'${String(value).replaceAll("'", "'\\''")}'`;

async function safePath(home, path, directory = false) {
  const parts = relative(home, path).split(sep).filter(Boolean);
  let current = home;
  for (let index = -1; index < parts.length; index++) {
    if (index >= 0) current = join(current, parts[index]);
    let info;
    try { info = await lstat(current); } catch (error) {
      if (error.code === 'ENOENT') return;
      throw error;
    }
    const isDirectory = index < parts.length - 1 || directory;
    if (info.isSymbolicLink() || (isDirectory ? !info.isDirectory() : !info.isFile()) ||
        (typeof process.getuid === 'function' && info.uid !== process.getuid()) ||
        (info.mode & 0o022) || (!isDirectory && (info.nlink !== 1 || info.size > 1024 * 1024))) {
      throw new Error('Unsafe existing AGY configuration path');
    }
  }
}

async function readObject(path) {
  let file;
  let text;
  try {
    file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const info = await file.stat();
    if (!info.isFile() || info.nlink !== 1 || info.uid !== process.getuid() || (info.mode & 0o022)) {
      throw new Error('Unsafe existing AGY configuration');
    }
    const buffer = Buffer.alloc(1024 * 1024 + 1);
    let size = 0;
    while (size < buffer.length) {
      const { bytesRead } = await file.read(buffer, size, buffer.length - size);
      if (!bytesRead) break;
      size += bytesRead;
    }
    if (size > 1024 * 1024) throw new Error('Unsafe existing AGY configuration');
    text = buffer.toString('utf8', 0, size);
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  } finally { await file?.close(); }
  let value;
  try { value = JSON.parse(text); } catch { throw new Error('Malformed existing AGY configuration'); }
  if (!object(value)) throw new Error('Malformed existing AGY configuration');
  return value;
}

async function publish(home, path, value) {
  await safePath(home, path);
  const temporary = `${path}.${randomUUID()}`;
  const file = await open(temporary, 'wx', 0o600);
  try { await file.writeFile(JSON.stringify(value, null, 2) + '\n'); await file.sync(); }
  finally { await file.close(); }
  try {
    await safePath(home, path);
    await rename(temporary, path);
    const directory = await open(dirname(path), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } finally { await unlink(temporary).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
}

export async function installAgy(options = {}) {
  const home = resolve(options.home || homedir());
  const apply = Boolean(options.apply);
  const uninstall = Boolean(options.uninstall);
  let configPath;
  if (options.config || process.env.DATALAKE_OTEL_CONFIG) {
    configPath = resolve(options.config || process.env.DATALAKE_OTEL_CONFIG);
  } else {
    const arcane = join(home, '.config', 'doda-datalake', 'agy-arcane.json');
    try {
      const info = await lstat(arcane);
      if (info.isSymbolicLink()) throw new Error('Unsafe existing AGY configuration path');
      configPath = info.isFile() ? arcane : undefined;
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    configPath ??= join(home, '.config', 'doda-datalake', 'otel.json');
  }
  const geminiDir = join(home, '.gemini', 'config');
  const targetPluginDir = join(geminiDir, 'plugins', 'doda-datalake');
  const configFile = join(geminiDir, 'config.json');
  const manifestFile = join(targetPluginDir, 'plugin.json');
  const hooksFile = join(targetPluginDir, 'hooks.json');

  // Validate every existing path and JSON before making any mutation, including uninstall.
  await safePath(home, targetPluginDir, true);
  for (const path of [configFile, manifestFile, hooksFile]) await safePath(home, path);
  const currentConfig = await readObject(configFile) ?? {};
  const existingManifest = await readObject(manifestFile);
  const existingHooks = await readObject(hooksFile);
  if (existingHooks && Object.values(existingHooks).some(value => !object(value))) throw new Error('Malformed existing AGY hooks');
  if (currentConfig.plugins !== undefined && !object(currentConfig.plugins)) throw new Error('Malformed existing AGY plugin registry');
  const entry = currentConfig.plugins?.['doda-datalake'];
  if (entry !== undefined && !object(entry)) throw new Error('Malformed existing AGY plugin registry');
  if ((existingManifest && existingManifest.name !== 'doda-datalake-telemetry') || (existingHooks && !existingManifest)) {
    throw new Error('Refusing to replace an unrelated AGY plugin');
  }

  console.log(`[AGY Datalake Plugin Installer] ${apply ? '--apply' : 'Dry-run'}${uninstall ? ' uninstall' : ''}`);
  console.log(`- 플러그인: ${targetPluginDir}`);
  console.log(`- 설정: ${configPath}`);
  if (!apply) return { targetPluginDir, configPath };

  if (uninstall) {
    if (existingManifest) {
      const remainingHooks = { ...existingHooks };
      delete remainingHooks['datalake-telemetry'];
      if (Object.keys(remainingHooks).length) await publish(home, hooksFile, remainingHooks);
      else {
        for (const path of [manifestFile, hooksFile]) {
          await unlink(path).catch(error => { if (error.code !== 'ENOENT') throw error; });
        }
      }
    }
    if (entry) {
      const remaining = { ...entry };
      delete remaining.enabled;
      if (Object.keys(remaining).length) currentConfig.plugins['doda-datalake'] = remaining;
      else delete currentConfig.plugins['doda-datalake'];
      await publish(home, configFile, currentConfig);
    }
    return { targetPluginDir, configPath };
  }

  const command = `node ${quote(join(__dirname, 'hook.mjs'))} --config ${quote(configPath)}`;
  const handler = suffix => ({ type: 'command', command: `${command}${suffix}`, timeout: suffix === ' --stop' ? 15 : 5 });
  const hooksConfig = {
    ...existingHooks,
    'datalake-telemetry': {
      ...existingHooks?.['datalake-telemetry'],
      PreToolUse: [{ matcher: '*', hooks: [handler(' --pre-tool')] }],
      PostToolUse: [{ matcher: '*', hooks: [handler('')] }],
      Stop: [handler(' --stop')],
    },
  };
  // Components are discovered by filename; the manifest schema allows only metadata.
  const pluginManifest = {
    name: 'doda-datalake-telemetry',
    description: 'Export AGY CLI AI coding telemetry to Doda DataLake (GreptimeDB + Alloy)',
  };
  await mkdir(targetPluginDir, { recursive: true, mode: 0o700 });
  await publish(home, manifestFile, pluginManifest);
  await publish(home, hooksFile, hooksConfig);
  currentConfig.plugins ??= {};
  currentConfig.plugins['doda-datalake'] = { ...entry, enabled: true };
  await publish(home, configFile, currentConfig);
  return { targetPluginDir, configPath };
}

if (isMain(import.meta.url)) {
  const { values } = parseArgs({
    options: {
      help: { type: 'boolean', short: 'h' }, apply: { type: 'boolean' },
      uninstall: { type: 'boolean' }, home: { type: 'string' }, config: { type: 'string' },
    },
  });
  if (values.help) console.log(help);
  else installAgy(values).catch(() => { console.error('AGY installation failed: invalid or unsafe configuration'); process.exitCode = 1; });
}
