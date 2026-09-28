#!/usr/bin/env node
import { createHash, randomUUID } from 'node:crypto';
import { link, mkdir, readFile, unlink, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createTelemetry, isMain } from '../otel.mjs';
import { collectUsage } from './usage.mjs';

// Pinned wire contract: openai/codex rust-v0.142.4, codex-rs/hooks/src/schema.rs.
// This release has neither SessionEnd nor Interrupt, and hooks expose no usage/cost.
// The active transcript is read incrementally for numeric usage only; no conversation content persists.
const EVENTS = new Set(['SessionStart', 'UserPromptSubmit', 'Stop', 'PreToolUse', 'PostToolUse', 'SubagentStart', 'SubagentStop']);
const identity = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$/.test(value) ? value : undefined;
const label = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,159}$/.test(value) ? value : undefined;
const digest = value => createHash('sha256').update(value).digest('hex');

async function existing(path) {
  try { return JSON.parse(await readFile(path, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
}

// Atomic first-observation snapshots give repeated/concurrent notifications identical IDs/times.
async function firstObservation(path, value) {
  const previous = await existing(path);
  if (previous !== undefined) return previous;
  const temporary = `${path}.${randomUUID()}`;
  await writeFile(temporary, JSON.stringify(value), { mode: 0o600, flag: 'wx' });
  try {
    try { await link(temporary, path); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
    return await existing(path);
  } finally { await unlink(temporary); }
}

export async function handleHook(input, { telemetry, stateDir, now = Date.now } = {}) {
  const sessionId = identity(input?.session_id);
  const eventName = input?.hook_event_name;
  if (!sessionId || !EVENTS.has(eventName)) return [];
  const clock = now();
  if (!Number.isSafeInteger(clock) || clock < 0) return [];
  const directory = join(stateDir, digest(sessionId));
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const path = key => join(directory, `${digest(key)}.json`);
  const attributes = {};
  const model = label(input.model);
  const agentId = identity(input.agent_id);
  const agentType = label(input.agent_type);
  if (model) attributes['gen_ai.request.model'] = model;
  if (agentId) attributes['coding_agent.agent.id'] = agentId;
  if (agentType) attributes['coding_agent.subagent.type'] = agentType;
  const turnId = identity(input.turn_id);
  const scope = agentId ?? 'root';
  let event;

  if (eventName === 'SessionStart') {
    const started = await firstObservation(path('session-start'), clock);
    event = { kind: 'session', sessionId, eventId: 'session-start', startTimeMs: started, endTimeMs: started, attributes };
  } else if (eventName === 'UserPromptSubmit' && turnId) {
    await firstObservation(path(`turn-start:${scope}:${turnId}`), clock);
  } else if (eventName === 'Stop' && turnId) {
    const started = await existing(path(`turn-start:root:${turnId}`));
    // Completed observed turn windows, not fabricated whole-session shutdown/lifetime.
    // Per-request numeric usage is emitted separately from the native rollout, never from hook text.
    event = { kind: 'session', sessionId, eventId: `turn-window:${turnId}`, startTimeMs: started ?? clock, endTimeMs: Math.max(started ?? clock, clock), attributes };
  } else if ((eventName === 'PreToolUse' || eventName === 'PostToolUse') && turnId) {
    const callId = identity(input.tool_use_id);
    if (!callId) return [];
    const key = `tool:${scope}:${turnId}:${callId}`;
    if (eventName === 'PreToolUse') {
      await firstObservation(path(`${key}:start`), clock);
    } else {
      const started = await existing(path(`${key}:start`));
      const tool = label(input.tool_name);
      if (tool) attributes['gen_ai.tool.name'] = tool;
      attributes['gen_ai.tool.call.id'] = callId;
      if (started !== undefined) attributes['duration_ms'] = Math.max(0, clock - started);
      event = { kind: 'tool.call', sessionId, eventId: key, startTimeMs: started ?? clock, endTimeMs: Math.max(started ?? clock, clock), attributes };
    }
  } else if ((eventName === 'SubagentStart' || eventName === 'SubagentStop') && agentId && turnId) {
    const key = `subagent:${agentId}:${turnId}`;
    if (eventName === 'SubagentStart') {
      await firstObservation(path(`subagent:${agentId}:start`), clock);
    } else {
      const started = await existing(path(`subagent:${agentId}:start`));
      if (started !== undefined) attributes['coding_agent.subagent.duration_ms'] = Math.max(0, clock - started);
      // Stop hooks can be followed by another hook's continuation: never claim success/completion.
      event = { kind: 'subagent', sessionId, eventId: key, startTimeMs: started ?? clock, endTimeMs: Math.max(started ?? clock, clock), attributes };
    }
  }
  const events = [];
  if (event) {
    event = await firstObservation(path(`event:${event.eventId}`), event);
    await telemetry.emit(event);
    events.push(event);
  }
  if (eventName !== 'SubagentStart' && !(eventName === 'SessionStart' && input.source === 'compact')) {
    events.push(...await collectUsage(input, { telemetry, stateDir: directory, prime: eventName === 'SessionStart' }));
  }
  return events;
}

export async function runHook() {
  try {
    let size = 0;
    const chunks = [];
    for await (const chunk of process.stdin) {
      size += chunk.length;
      if (size > 8 * 1024 * 1024) throw new Error('hook input too large');
      chunks.push(chunk);
    }
    const input = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    const settings = await existing(join(dirname(fileURLToPath(import.meta.url)), 'settings.json'));
    if (settings?.source === 'native') {
      if (input.hook_event_name === 'SessionStart') process.stderr.write('[datalake] Codex source=native: 중복 방지를 위해 플러그인 내보내기를 생략합니다.\n');
      process.stdout.write('{}\n');
      return;
    }
    if (input.hook_event_name === 'SessionStart') process.stderr.write('[datalake] Codex source=plugin: 같은 세션의 네이티브 OTel을 같은 데이터레이크로 병행 수집하지 마세요.\n');
    const telemetry = createTelemetry('codex', { configPath: settings?.configPath });
    const dataRoot = process.env.PLUGIN_DATA ?? join(process.env.CODEX_HOME ?? join(homedir(), '.codex'), 'doda-datalake-state');
    await handleHook(input, { telemetry, stateDir: join(dataRoot, 'metadata') });
    await telemetry.flush();
  } catch {
    process.stderr.write('[datalake] Codex 메타데이터 처리 실패; 원문과 오류 상세는 기록하지 않았습니다.\n');
  }
  // Every supported event accepts empty JSON; never influence approval, model context or continuation.
  process.stdout.write('{}\n');
}

if (isMain(import.meta.url)) await runHook();
