#!/usr/bin/env node
import { appendFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';

let raw = '';
process.stdin.setEncoding('utf8');
for await (const chunk of process.stdin) {
  raw += chunk;
}

try {
  const logFile = join(homedir(), '.config', 'doda-datalake', 'hook_debug.jsonl');
  const entry = JSON.stringify({
    time: new Date().toISOString(),
    argv: process.argv.slice(2),
    rawLength: raw.length,
    parsed: raw.trim() ? JSON.parse(raw) : null
  }) + '\n';
  await appendFile(logFile, entry, 'utf8');
} catch (e) {
  console.error(e);
}

console.log('{}');
