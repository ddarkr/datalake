#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const clients = ['codex', 'omp', 'opencode2', 'claude-code'];
const [client, ...args] = process.argv.slice(2);
if (!client || client === '--help' || client === '-h') {
  console.log(`Usage: node plugins/install.mjs <${clients.join('|')}> [options]

Installs only the selected native plugin. Preview is the default; --apply is required.
  --home DIR      Isolated user home for installation or testing
  --config FILE   Private telemetry JSON created by configure.mjs
  --apply         Apply the selected installer's changes
  --help          Show tool-specific installation and trust requirements

Configure first:
  node plugins/configure.mjs --endpoint https://collector.example/otlp --from-env-file .env --apply
Then inspect and apply one adapter:
  node plugins/install.mjs omp --help
  node plugins/install.mjs omp --apply

Restart the client after installing. Native hook trust still applies.
Never commit the private telemetry configuration or deployment .env.`);
} else if (!clients.includes(client)) {
  console.error('Unknown client; use codex, omp, opencode2, or claude-code');
  process.exitCode = 2;
} else {
  const result = spawnSync(process.execPath, [fileURLToPath(new URL(`./${client}/install.mjs`, import.meta.url)), ...args], { stdio: 'inherit' });
  if (result.error) console.error('Could not start plugin installer');
  process.exitCode = result.status ?? 1;
}
