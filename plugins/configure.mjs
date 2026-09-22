#!/usr/bin/env node
import { mkdirSync, readFileSync, writeFileSync, renameSync, unlinkSync, existsSync, lstatSync } from 'node:fs';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { parseEnv } from 'node:util';
import { defaultConfigPath, parseHeaders, validateConfig } from './otel.mjs';

const usage = `Usage: node plugins/configure.mjs --endpoint URL [options]

  --config FILE         Destination (default: ~/.config/doda-datalake/otel.json)
  --from-env-file FILE   Read OTLP_USER and OTLP_PASSWORD; other values are not copied
  --headers-env NAME    Read standard comma-separated OTLP headers from this environment variable
  --timeout-ms NUMBER   Export deadline, 100..30000 (default: 2000)
  --allow-insecure-http  Permit unencrypted HTTP to a private IPv4 address (credentials are exposed)
  --apply               Write private configuration; otherwise preview only
  --replace             Explicitly replace an existing configuration
  --help                Show this help

HTTPS is required except for loopback HTTP or explicitly permitted private IPv4 HTTP.
Credentials must come from a private file or environment, never command-line arguments.
No existing client settings are modified by this command.`;

function main() {
  const options = {};
  const args = process.argv.slice(2);
  const valueFlags = new Set(['--endpoint', '--config', '--from-env-file', '--headers-env', '--timeout-ms']);
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--help' || arg === '-h') { console.log(usage); return; }
    if (arg === '--apply' || arg === '--replace' || arg === '--allow-insecure-http') { options[arg] = true; continue; }
    if (!valueFlags.has(arg) || !args[i + 1] || args[i + 1].startsWith('--')) throw new Error('Invalid arguments; use --help');
    options[arg] = args[++i];
  }
  if (!options['--endpoint']) throw new Error('Specify --endpoint; use --help');
  if (options['--from-env-file'] && options['--headers-env']) throw new Error('Choose one credential source');
  let headers = {};
  if (options['--from-env-file']) {
    let env;
    try { env = parseEnv(readFileSync(options['--from-env-file'], 'utf8')); } catch { throw new Error('Cannot read the credential environment file'); }
    if (!env.OTLP_USER || !env.OTLP_PASSWORD) throw new Error('OTLP_USER and OTLP_PASSWORD are required in the environment file');
    headers.Authorization = 'Basic ' + Buffer.from(`${env.OTLP_USER}:${env.OTLP_PASSWORD}`).toString('base64');
  } else if (options['--headers-env']) {
    const value = process.env[options['--headers-env']];
    if (!value) throw new Error('The selected headers environment variable is empty');
    headers = parseHeaders(value);
  }
  const config = validateConfig({ endpoint: options['--endpoint'], headers, timeoutMs: options['--timeout-ms'] === undefined ? undefined : Number(options['--timeout-ms']), allowInsecureHttp: options['--allow-insecure-http'] });
  const path = options['--config'] || defaultConfigPath();
  if (existsSync(path) && lstatSync(path).isSymbolicLink()) throw new Error('Refusing to replace a symbolic-link configuration');
  console.log(`${options['--apply'] ? 'Write' : 'Preview'} private OTLP configuration: ${path}`);
  console.log(`Endpoint origin: ${new URL(config.endpoint).origin}; authentication: ${Object.keys(headers).length ? 'configured' : 'none'}; timeout: ${config.timeoutMs}ms`);
  if (config.allowInsecureHttp) console.warn('WARNING: private-network HTTP is allowed; HTTP sends credentials and telemetry without encryption.');
  if (!options['--apply']) return;
  if (existsSync(path) && !options['--replace']) throw new Error('Configuration already exists; use --replace to replace it explicitly');
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  if (!options['--replace']) {
    writeFileSync(path, JSON.stringify(config, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  } else {
    const temporary = `${path}.${randomUUID()}.tmp`;
    try {
      writeFileSync(temporary, JSON.stringify(config, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
      renameSync(temporary, path);
    } finally {
      if (existsSync(temporary)) unlinkSync(temporary);
    }
  }
  console.log('Configuration saved with mode 0600; contents were not printed.');
}

try { main(); } catch (error) {
  // Filesystem/parser diagnostics can contain credentials or file contents.
  console.error(error.code || error instanceof SyntaxError ? 'Configuration failed; check path, permissions, and input format' : error.message);
  process.exitCode = 2;
}
