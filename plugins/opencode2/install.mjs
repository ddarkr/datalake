#!/usr/bin/env node
import { lstat, mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { isMain } from '../otel.mjs';

const directory = dirname(fileURLToPath(import.meta.url));
const pluginId = 'doda.datalake.otel';
const help = `Usage: node plugins/opencode2/install.mjs [--home DIR] [--config PATH] [--apply] [--uninstall]
Registers the native OpenCode 2 plugin directory. Dry-run by default.
--home DIR   Use DIR/.config/opencode, ignoring host XDG/OpenCode overrides.
--config PATH  Private telemetry JSON path; only the path enters OpenCode config.
--apply      Apply the displayed plan; existing JSONC/comments/plugins survive.
--uninstall  Remove only this plugin's registrations (also dry-run by default).
Restart OpenCode after installation. Requires native OpenCode 2; not classic v1.`;

async function jsonc() {
  try { return await import('jsonc-parser'); }
  catch { throw new Error('Install dependencies first: npm install --prefix plugins'); }
}

async function safePath(path, boundary) {
  // An isolated --home must not escape through an existing config symlink.
  for (let current = path; current !== boundary && current !== dirname(current); current = dirname(current)) {
    try {
      if ((await lstat(current)).isSymbolicLink()) throw new Error('Refusing a symlinked OpenCode configuration path');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
}

function ownEntry(entry, path) {
  const target = typeof entry === 'string' ? entry : entry?.package;
  if (target === pluginId || target === `-${pluginId}`) return true;
  if (typeof target !== 'string') return false;
  try {
    const local = target.startsWith('file://') ? fileURLToPath(target)
      : isAbsolute(target) || target.startsWith('./') || target.startsWith('../') ? resolve(dirname(path), target) : undefined;
    return local === directory;
  } catch { return false; }
}

export async function install(args = process.argv.slice(2)) {
  const { values } = parseArgs({ args, options: {
    home: { type: 'string' }, config: { type: 'string' },
    apply: { type: 'boolean' }, uninstall: { type: 'boolean' }, help: { type: 'boolean' },
  }, strict: true, allowPositionals: false });
  if (values.help) return { help };
  const home = resolve(values.home ?? homedir());
  const configDirectory = values.home ? join(home, '.config', 'opencode')
    : resolve(process.env.OPENCODE_CONFIG_DIR || join(process.env.XDG_CONFIG_HOME || join(home, '.config'), 'opencode'));
  const configPath = resolve(values.config ?? (values.home ? undefined : process.env.DATALAKE_OTEL_CONFIG) ?? join(home, '.config', 'doda-datalake', 'otel.json'));
  const boundary = values.home ? home : dirname(dirname(configDirectory));
  const { parseTree, getNodeValue, modify, applyEdits } = await jsonc();
  const documents = [];
  for (const name of ['opencode.json', 'opencode.jsonc']) {
    const path = join(configDirectory, name);
    await safePath(path, boundary);
    let text;
    let mode = 0o600;
    try {
      const info = await lstat(path);
      if (!info.isFile() || info.size > 1024 * 1024) throw new Error('OpenCode configuration must be a regular file under 1 MiB');
      mode = info.mode & 0o777;
      text = await readFile(path, 'utf8');
    } catch (error) {
      if (error.code === 'ENOENT') continue;
      throw error;
    }
    const errors = [];
    const tree = parseTree(text, errors, { allowTrailingComma: true });
    if (errors.length || tree?.type !== 'object') throw new Error('OpenCode configuration is not a valid JSONC object; no changes made');
    const keys = tree.children.map(node => node.children[0].value);
    if (new Set(keys).size !== keys.length) throw new Error('OpenCode configuration has duplicate keys; no changes made');
    const info = getNodeValue(tree);
    if (info.plugins !== undefined && !Array.isArray(info.plugins)) throw new Error('OpenCode plugins must be an array; no changes made');
    documents.push({ path, text, next: text, mode, plugins: info.plugins ?? [] });
  }
  if (!documents.length && !values.uninstall) documents.push({
    path: join(configDirectory, 'opencode.json'), text: undefined, next: '{}\n', mode: 0o600, plugins: [],
  });
  const registered = documents.filter(doc => doc.plugins.some(entry => ownEntry(entry, doc.path)));
  const target = registered.at(-1) ?? documents.at(-1);
  const options = { formattingOptions: { insertSpaces: true, tabSize: 2 } };
  for (const doc of documents) {
    const indices = doc.plugins.flatMap((entry, index) => ownEntry(entry, doc.path) ? [index] : []);
    const keep = !values.uninstall && doc === target ? indices.at(-1) : undefined;
    if (keep !== undefined) {
      const existing = doc.plugins[keep];
      const replacement = {
        package: directory,
        options: { ...(typeof existing === 'object' ? existing.options : {}), configPath },
      };
      // Modify leaves unrelated plugin elements and their comments untouched.
      if (typeof existing === 'object') {
        if (existing.package !== directory) doc.next = applyEdits(doc.next, modify(doc.next, ['plugins', keep, 'package'], directory, options));
        if (existing.options?.configPath !== configPath) doc.next = applyEdits(doc.next, modify(doc.next, ['plugins', keep, 'options', 'configPath'], configPath, options));
      } else doc.next = applyEdits(doc.next, modify(doc.next, ['plugins', keep], replacement, options));
    }
    for (const index of indices.toReversed()) {
      if (index !== keep) doc.next = applyEdits(doc.next, modify(doc.next, ['plugins', index], undefined, options));
    }
    if (!values.uninstall && doc === target && keep === undefined) {
      doc.next = applyEdits(doc.next, modify(doc.next, ['plugins', -1], { package: directory, options: { configPath } }, options));
    }
  }
  const changed = documents.filter(doc => doc.next !== doc.text);
  const result = { client: 'opencode2', apply: !!values.apply, uninstall: !!values.uninstall, changed: changed.map(doc => doc.path) };
  if (!values.apply || !changed.length) return result;
  await mkdir(configDirectory, { recursive: true, mode: 0o700 });
  // Check the whole plan before writing, including concurrent config edits.
  for (const doc of changed) {
    await safePath(doc.path, boundary);
    const current = await readFile(doc.path, 'utf8').catch(error => {
      if (error.code === 'ENOENT') return undefined;
      throw error;
    });
    if (current !== doc.text) throw new Error('OpenCode configuration changed during installation; retry');
  }
  for (const doc of changed) {
    const temporary = `${doc.path}.datalake-${process.pid}-${crypto.randomUUID()}`;
    try {
      const handle = await open(temporary, 'wx', doc.mode);
      try { await handle.writeFile(doc.next); await handle.sync(); }
      finally { await handle.close(); }
      await rename(temporary, doc.path);
    } finally { await unlink(temporary).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
  }
  return result;
}

if (isMain(import.meta.url)) {
  try {
    const result = await install();
    console.log(result.help ?? JSON.stringify(result, null, 2));
  } catch (error) {
    // Filesystem exceptions can contain private paths, not file contents.
    console.error(`[datalake-otel] ${error.code ? 'OpenCode configuration could not be updated' : error.message}`);
    process.exitCode = 1;
  }
}
