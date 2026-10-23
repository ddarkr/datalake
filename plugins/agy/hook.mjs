#!/usr/bin/env node
import { createHash, randomUUID } from 'node:crypto';
import { readFile, writeFile, unlink, mkdir, stat, rename, open } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { dirname, join, isAbsolute } from 'node:path';
import { parseArgs } from 'node:util';
import { createDirectTelemetry, repositoryContext, isMain, withStateLock } from '../otel.mjs';

async function resolveConfigPath(explicit) {
  if (explicit) return explicit;
  if (process.env.DATALAKE_OTEL_CONFIG) return process.env.DATALAKE_OTEL_CONFIG;
  const arcane = join(homedir(), '.config', 'doda-datalake', 'agy-arcane.json');
  try {
    const s = await stat(arcane);
    if (s.isFile()) return arcane;
  } catch {}
  return join(homedir(), '.config', 'doda-datalake', 'otel.json');
}

// 크로스 플랫폼 지원 임시 디렉터리 (tmpdir() 사용)
function stateDir(sessionId) {
  const safeSession = String(sessionId).replace(/[^a-zA-Z0-9_-]/g, '_');
  return join(tmpdir(), 'doda-datalake-agy', safeSession);
}

function toolStatePath(sessionId, eventId) {
  const identity = createHash('sha256').update(eventId).digest('hex');
  return join(stateDir(sessionId), `tool-${identity}.json`);
}

async function readToolState(sessionId, stepIdx, toolName, eventId) {
  const paths = [toolStatePath(sessionId, eventId)];
  // Old step-only files are read only when their recorded identity matches.
  if (/^[a-zA-Z0-9_-]+$/.test(String(stepIdx))) {
    paths.push(join(stateDir(sessionId), `tool-${stepIdx}.json`));
  }
  for (const path of paths) {
    try {
      const state = JSON.parse(await readFile(path, 'utf8'));
      if (state?.toolName !== toolName || (state.eventId && state.eventId !== eventId)) continue;
      // A recorded completion for another tool identity is not this tool's state.
      if (state.event && (state.event.eventId !== eventId
        || state.event.sessionId !== String(sessionId) || state.event.kind !== 'tool.call')) continue;
      // Legacy durable receipts (queued:true) prove a prior attempt: never replay.
      if (state?.queued === true && state?.attempted === undefined) return { ...state, attempted: true };
      return state;
    } catch {}
  }
  return undefined;
}

function cursorPath(sessionId) {
  return join(stateDir(sessionId), 'cursor.json');
}

async function save(path, value) {
  const temporary = `${path}.${randomUUID()}`;
  const file = await open(temporary, 'wx', 0o600);
  try {
    await file.writeFile(JSON.stringify(value));
    await file.sync();
  } finally { await file.close(); }
  try {
    await rename(temporary, path);
    const directory = await open(dirname(path), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } finally { await unlink(temporary).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
}


// 트랜스크립트 파일 경로 자동 탐색 및 폴백 안전장치
async function resolveTranscriptPath(payload) {
  if (payload?.transcriptPath && typeof payload.transcriptPath === 'string' && isAbsolute(payload.transcriptPath)) {
    try {
      const s = await stat(payload.transcriptPath);
      if (s.isFile()) return payload.transcriptPath;
    } catch {}
  }
  const sessionId = payload?.conversationId || payload?.session_id;
  if (!sessionId) return undefined;

  const home = homedir();
  const candidates = [
    join(home, '.gemini', 'antigravity-cli', 'brain', sessionId, '.system_generated', 'logs', 'transcript_full.jsonl'),
    join(home, '.gemini', 'antigravity-cli', 'brain', sessionId, '.system_generated', 'logs', 'transcript.jsonl'),
    join(home, '.gemini', 'antigravity', 'brain', sessionId, '.system_generated', 'logs', 'transcript_full.jsonl'),
    join(home, '.gemini', 'antigravity', 'brain', sessionId, '.system_generated', 'logs', 'transcript.jsonl'),
    join(home, '.gemini', 'antigravity-ide', 'brain', sessionId, '.system_generated', 'logs', 'transcript.jsonl'),
  ];

  for (const candidate of candidates) {
    try {
      const s = await stat(candidate);
      if (s.isFile()) return candidate;
    } catch {}
  }
  return undefined;
}

// 커서 기반 증분(Incremental) 턴 및 서브에이전트 이벤트 전송
async function drainIncrementalTurns(sessionId, transcriptPath, telemetry, model, baseAttrs, clock, includeSummary = false) {
  const summary = { startTimeMs: clock, durationMs: 0, subagentCalls: 0 };
  if (!transcriptPath) return { turns: [], subagents: [], summary };

  const dir = stateDir(sessionId);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const cPath = cursorPath(sessionId);

  let cursor = { version: 3, lastProcessedIndex: -1 };
  try {
    cursor = { version: 3, lastProcessedIndex: -1, ...JSON.parse(await readFile(cPath, 'utf8')) };
  } catch {}
  cursor.version = 3;

  const turns = [];
  const subagents = [];
  // Attempted-identity checkpoint: legacy sent/queued IDs and prior direct attempts
  // all prove a prior attempt. A failed HTTP send is dropped with no replay, and the
  // checkpoint advances regardless of ACK, exactly like the Codex/Claude hooks.
  const attempted = new Set([...(cursor.sentEventIds ?? []), ...(cursor.queuedEventIds ?? []), ...(cursor.attemptedEventIds ?? [])]);
  async function attemptOnce(event, output) {
    if (attempted.has(event.eventId)) return true;
    attempted.add(event.eventId);
    let ok = false;
    try { ok = await telemetry.send(event); } catch {}
    cursor.attemptedEventIds = [...attempted].filter(id => !cursor.sentEventIds?.includes(id) && !cursor.queuedEventIds?.includes(id));
    await save(cPath, cursor);
    if (ok === true) output.push(event);
    return ok === true;
  }

  try {
    const raw = await readFile(transcriptPath, 'utf8');
    const lines = raw.split('\n').filter(Boolean);
    const steps = [];
    let earliestTimestamp;
    for (const line of lines) {
      try {
        const step = JSON.parse(line);
        steps.push(step);
        if (includeSummary && step.created_at) {
          const timestamp = new Date(step.created_at).getTime();
          earliestTimestamp = Math.min(earliestTimestamp ?? Infinity, timestamp);
        }
        if (includeSummary && Array.isArray(step.tool_calls)) {
          for (const tc of step.tool_calls) {
            if (tc.name === 'invoke_subagent') summary.subagentCalls++;
          }
        }
      } catch {}
    }
    if (earliestTimestamp !== undefined) {
      summary.startTimeMs = earliestTimestamp;
      summary.durationMs = Math.max(0, clock - earliestTimestamp);
    }

    for (let i = 0; i < steps.length; i++) {
      const step = steps[i];
      const stepIndex = step.step_index ?? i;

      if (step.type === 'PLANNER_RESPONSE') {
        if (stepIndex > cursor.lastProcessedIndex) {
          const turnTime = step.created_at ? new Date(step.created_at).getTime() : clock;
          const stepEvents = [];

          // 서브에이전트 호출 발견 시 계층 트레이스(subagent span) 생성
          if (step.tool_calls && Array.isArray(step.tool_calls)) {
            for (let tcIdx = 0; tcIdx < step.tool_calls.length; tcIdx++) {
              const tc = step.tool_calls[tcIdx];
              if (tc.name === 'invoke_subagent') {
                const subagentSpecs = tc.args?.Subagents || [];
                for (let saIdx = 0; saIdx < subagentSpecs.length; saIdx++) {
                  const sa = subagentSpecs[saIdx];
                  const subagentId = `subagent:${sessionId}:${stepIndex}:${saIdx}`;
                  const subagentEvent = {
                    kind: 'subagent',
                    sessionId: String(sessionId),
                    eventId: subagentId,
                    startTimeMs: turnTime,
                    endTimeMs: turnTime + 500,
                    attributes: {
                      ...baseAttrs,
                      'coding_agent.agent.id': subagentId,
                      'coding_agent.agent.parent_id': String(sessionId),
                      'coding_agent.subagent.type': String(sa.Role || sa.TypeName || 'subagent').slice(0, 128),
                      ...((sa.Model || model) ? { 'gen_ai.request.model': String(sa.Model || model).slice(0, 128) } : {}),
                    },
                  };
                  stepEvents.push(subagentEvent);
                }
              }
            }
          }

          for (const event of stepEvents) {
            await attemptOnce(event, subagents);
          }
          cursor.lastProcessedIndex = stepIndex;
          await save(cPath, cursor);
        }

      }
    }

  } catch {}

  return { turns, subagents, summary };
}

export async function handleHook(payload, options = {}) {
  const sessionId = payload?.conversationId || payload?.session_id;
  if (!sessionId) return [];
  const directory = stateDir(sessionId);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  return await withStateLock(directory, () => handleSerialized(payload, options)) ?? [];
}

async function handleSerialized(payload, { telemetry, now = Date.now, cwd = process.cwd(), isPreTool = false, isPreInvocation = false, isPostInvocation = false } = {}) {
  const sessionId = payload?.conversationId || payload?.session_id;
  if (!sessionId) return [];

  const clock = now();
  const model = payload.modelName;
  const events = [];

  const isStop = Boolean(payload.terminationReason) || process.argv.includes('--stop');
  const preToolFlag = isPreTool || process.argv.includes('--pre-tool');
  const postInvocFlag = isPostInvocation || process.argv.includes('--post-invocation');
  const preInvocFlag = isPreInvocation || process.argv.includes('--pre-invocation');
  if (preInvocFlag || postInvocFlag) {
    const number = payload.invocationNum;
    if (!Number.isSafeInteger(number) || number < 0 || !Number.isFinite(clock)) return [];
    const path = join(stateDir(sessionId), `invocation-${number}.json`);
    let state;
    try { state = JSON.parse(await readFile(path, 'utf8')); } catch {}
    const eventId = `invocation:${number}`;
    if (state && (state.sessionId !== String(sessionId) || state.eventId !== eventId)) return [];
    state ??= { sessionId: String(sessionId), eventId };
    if (state.attempted || state.closed) return [];
    if (preInvocFlag) {
      if (state.startTimeMs === undefined) {
        state.startTimeMs = clock;
        await save(path, state);
      }
      return [];
    }
    // A post observed before its pre cannot be paired with a later invocation.
    if (!Number.isFinite(state.startTimeMs) || clock < state.startTimeMs) {
      state.closed = true;
      await save(path, state);
      return [];
    }
    const event = {
      kind: 'llm.turn', sessionId: String(sessionId), eventId,
      startTimeMs: state.startTimeMs, endTimeMs: clock,
      attributes: {
        ...await repositoryContext(cwd),
        ...(model ? { 'gen_ai.request.model': String(model).slice(0, 128) } : {}),
        'coding_agent.request.duration_ms': clock - state.startTimeMs,
        'coding_agent.duration.kind': 'hook_invocation',
      },
    };
    state.attempted = true;
    await save(path, state);
    let ok = false;
    try { ok = await telemetry.send(event); } catch {}
    await telemetry.flush().catch(() => {});
    return ok === true ? [event] : [];
  }

  // 1. PreToolUse: 도구 실행 시작 시각 기록 (0ms duration 방지)
  if (preToolFlag) {
    const stepIdx = payload.stepIdx ?? 0;
    const toolName = payload.toolName || payload.toolCall?.name || 'unknown_tool';
    const eventId = `tool:${stepIdx}:${toolName}`;
    const filePath = toolStatePath(sessionId, eventId);
    if (!await readToolState(sessionId, stepIdx, toolName, eventId)) {
      await save(filePath, { version: 2, eventId, startTimeMs: clock, toolName });
    }
    return [];
  }

  const baseAttrs = {
    ...await repositoryContext(cwd),
    ...(model ? { 'gen_ai.request.model': String(model).slice(0, 128) } : {}),
  };

  // 트랜스크립트 경로 안전 해결
  const transcriptPath = await resolveTranscriptPath(payload);

  // 2. Stop 이벤트: 최종 세션 요약 및 남은 턴 증분 마감 전송
  if (isStop) {
    // 세션 종료 전 아직 전송되지 않은 증분 턴들 모두 전송
    const { turns, subagents, summary } = await drainIncrementalTurns(sessionId, transcriptPath, telemetry, model, baseAttrs, clock, true);
    for (const event of turns) events.push(event);
    for (const event of subagents) events.push(event);
    const { startTimeMs: startTime, durationMs } = summary;
    const subagentCount = subagents.length + summary.subagentCalls;

    const sessionEvent = {
      kind: 'session',
      sessionId: String(sessionId),
      eventId: `session:${sessionId}:stop`,
      startTimeMs: startTime,
      endTimeMs: clock,
      attributes: {
        ...baseAttrs,
        'coding_agent.session.duration_ms': durationMs,
        'coding_agent.session.subagent_count': subagentCount,
        'coding_agent.session.outcome': payload.terminationReason || 'model_stop',
      },
    };

    const stopPath = join(stateDir(sessionId), 'stop.json');
    let stopState;
    try { stopState = JSON.parse(await readFile(stopPath, 'utf8')); } catch {}
    if (!stopState) {
      stopState = { version: 3, event: sessionEvent, attempted: false };
      await save(stopPath, stopState);
    }
    if ((!stopState.attempted && !stopState.queued) || stopState.event?.eventId !== sessionEvent.eventId) {
      let ok = false;
      try { ok = await telemetry.send(stopState.event); } catch {}
      // Attempted once: never replayed by a repeated stop hook.
      stopState.attempted = true;
      await save(stopPath, stopState);
      if (ok === true) events.push(stopState.event);
    }
  } else {
    // 3. PostToolUse: 도구 실행 완료 시 실제 소요시간 계산 및 직전 턴 증분 즉시 전송
    const stepIdx = payload.stepIdx ?? 0;
    const toolName = payload.toolName || payload.toolCall?.name || 'unknown_tool';
    const eventId = `tool:${stepIdx}:${toolName}`;
    const isError = Boolean(payload.error);
    const filePath = toolStatePath(sessionId, eventId);
    const toolState = await readToolState(sessionId, stepIdx, toolName, eventId);
    const startTimeMs = Number.isFinite(toolState?.startTimeMs) ? toolState.startTimeMs : clock - 50;

    const endTimeMs = clock;
    const durationMs = Math.max(1, endTimeMs - startTimeMs);

    const toolEvent = toolState?.event ?? {
      kind: 'tool.call',
      sessionId: String(sessionId),
      eventId,
      startTimeMs,
      endTimeMs,
      error: isError,
      attributes: {
        ...baseAttrs,
        'gen_ai.tool.name': String(toolName).slice(0, 128),
        'duration_ms': durationMs,
        ...(isError ? { 'error.type': 'tool_error' } : {}),
      },
    };

    // Keep the first completion body; a failed direct send is attempted once, never replayed.
    if (!toolState?.attempted && !toolState?.queued) {
      const snapshot = { version: 3, eventId, startTimeMs, toolName, event: toolEvent, attempted: false };
      await save(filePath, snapshot);
      let ok = false;
      try { ok = await telemetry.send(toolEvent); } catch {}
      snapshot.attempted = true;
      await save(filePath, snapshot);
      if (ok === true) events.push(toolEvent);
    }

    // 실시간 증분 턴 전송 (세션 중 강제 종료 방지)
    const { turns, subagents } = await drainIncrementalTurns(sessionId, transcriptPath, telemetry, model, baseAttrs, clock);
    for (const event of turns) events.push(event);
    for (const event of subagents) events.push(event);
  }

  await telemetry.flush().catch(() => {});
  return events;
}

if (isMain(import.meta.url)) {
  try {
    const { values } = parseArgs({
      options: {
        config: { type: 'string' },
        'pre-tool': { type: 'boolean' },
        'pre-invocation': { type: 'boolean' },
        'post-invocation': { type: 'boolean' },
        stop: { type: 'boolean' },
      },
    });
    const chunks = [];
    let bytes = 0;
    for await (const chunk of process.stdin) {
      bytes += chunk.length;
      if (bytes > 1024 * 1024) throw new Error('Hook input too large');
      chunks.push(chunk);
    }
    const raw = Buffer.concat(chunks).toString('utf8');
    const payload = raw.trim() ? JSON.parse(raw) : {};
    const configPath = await resolveConfigPath(values.config);
    const telemetry = createDirectTelemetry('agy', { configPath });
    await handleHook(payload, {
      telemetry,
      isPreTool: values['pre-tool'],
      isPreInvocation: values['pre-invocation'],
      isPostInvocation: values['post-invocation'],
    });
  } catch {}

  console.log(process.argv.includes('--pre-tool') ? JSON.stringify({ decision: 'allow' }) : '{}');
}
