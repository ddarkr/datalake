#!/usr/bin/env node
import { createHash, randomUUID } from 'node:crypto';
import { readFile, writeFile, unlink, mkdir, stat, rename, open } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { dirname, join, isAbsolute } from 'node:path';
import { parseArgs } from 'node:util';
import { createTelemetry, repositoryContext, isMain, withStateLock } from '../otel.mjs';

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
      if (state.event && (state.event.eventId !== eventId
        || state.event.sessionId !== String(sessionId) || state.event.kind !== 'tool.call')) continue;
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

// 텍스트 길이 기반 고정밀 토큰 근사치 계산 (Gemini/Claude 표준: ~3.8 글자당 1토큰)
function estimateTokens(text) {
  if (!text) return 0;
  const len = typeof text === 'string' ? text.length : JSON.stringify(text).length;
  return Math.max(1, Math.round(len / 3.8));
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
async function drainIncrementalTurns(sessionId, transcriptPath, telemetry, model, baseAttrs, clock) {
  if (!transcriptPath) return { turns: [], subagents: [] };

  const dir = stateDir(sessionId);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const cPath = cursorPath(sessionId);

  let cursor = { version: 2, lastProcessedIndex: -1, sentEventIds: [], queuedEventIds: [] };
  try {
    cursor = JSON.parse(await readFile(cPath, 'utf8'));
  } catch {}
  cursor.version = 2;

  const turns = [];
  const subagents = [];
  // Historical sent IDs prove remote ACKs, not new local handoffs.
  const sent = new Set(cursor.sentEventIds ?? []);
  const queued = new Set(cursor.queuedEventIds ?? []);
  async function enqueueOnce(event, output) {
    if (sent.has(event.eventId) || queued.has(event.eventId)) return true;
    if (!await telemetry.enqueue(event)) return false;
    queued.add(event.eventId);
    cursor.queuedEventIds = [...queued];
    await save(cPath, cursor);
    output.push(event);
    return true;
  }

  try {
    const raw = await readFile(transcriptPath, 'utf8');
    const lines = raw.split('\n').filter(Boolean);
    const steps = [];

    for (const line of lines) {
      try { steps.push(JSON.parse(line)); } catch {}
    }

    let accumulatedPromptChars = 1000;

    for (let i = 0; i < steps.length; i++) {
      const step = steps[i];
      const stepIndex = step.step_index ?? i;

      if (step.type === 'USER_INPUT') {
        accumulatedPromptChars += (step.content?.length || 0);
      } else if (step.type === 'PLANNER_RESPONSE') {
        const thinkingText = step.thinking || '';
        const contentText = step.content || '';
        const toolCallsText = step.tool_calls ? JSON.stringify(step.tool_calls) : '';

        // 아직 전송되지 않은 새 턴인 경우 증분 전송
        if (stepIndex > cursor.lastProcessedIndex) {
          const turnTime = step.created_at ? new Date(step.created_at).getTime() : clock;
          const prevStep = steps[i - 1];
          const prevTime = prevStep?.created_at ? new Date(prevStep.created_at).getTime() : turnTime - 1000;
          const turnDurationMs = Math.max(100, turnTime - prevTime);

          const inputTokens = estimateTokens(accumulatedPromptChars);
          const reasoningTokens = estimateTokens(thinkingText);
          const outputTokens = estimateTokens(contentText + toolCallsText) + reasoningTokens;
          const cacheReadTokens = i > 1 ? Math.round(inputTokens * 0.75) : 0;

          const turnEvent = {
            kind: 'llm.turn',
            sessionId: String(sessionId),
            eventId: `turn:${stepIndex}`,
            startTimeMs: prevTime,
            endTimeMs: turnTime,
            attributes: {
              ...baseAttrs,
              'gen_ai.request.model': String(model).slice(0, 128),
              'gen_ai.usage.input_tokens': inputTokens,
              'gen_ai.usage.output_tokens': outputTokens,
              'gen_ai.usage.reasoning.output_tokens': reasoningTokens,
              'gen_ai.usage.cache_read.input_tokens': cacheReadTokens,
              'duration_ms': turnDurationMs,
              'turn.id': String(stepIndex),
            },
          };

          const stepEvents = [turnEvent];

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
                      'gen_ai.request.model': String(sa.Model || model).slice(0, 128),
                    },
                  };
                  stepEvents.push(subagentEvent);
                }
              }
            }
          }

          // Snapshot the entire step before its first local handoff, including retry times.
          if (!cursor.pendingStep || cursor.pendingStep.index !== stepIndex) {
            cursor.pendingStep = { index: stepIndex, events: stepEvents };
            await save(cPath, cursor);
          }
          for (const event of cursor.pendingStep.events) {
            if (!await enqueueOnce(event, event.kind === 'llm.turn' ? turns : subagents)) return { turns, subagents };
          }
          cursor.lastProcessedIndex = stepIndex;
          delete cursor.pendingStep;
          queued.clear();
          cursor.queuedEventIds = [];
          await save(cPath, cursor);
        }

        accumulatedPromptChars += (thinkingText.length + contentText.length + toolCallsText.length);
      }
    }

  } catch {}

  return { turns, subagents };
}

export async function handleHook(payload, options = {}) {
  const sessionId = payload?.conversationId || payload?.session_id;
  if (!sessionId) return [];
  const directory = stateDir(sessionId);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  return await withStateLock(directory, () => handleSerialized(payload, options)) ?? [];
}

async function handleSerialized(payload, { telemetry, now = Date.now, cwd = process.cwd(), isPreTool = false, isPostInvocation = false } = {}) {
  const sessionId = payload?.conversationId || payload?.session_id;
  if (!sessionId) return [];

  const clock = now();
  const repoAttrs = await repositoryContext(cwd);
  const model = payload.modelName || 'gemini-3.8-flash';
  const events = [];

  const baseAttrs = {
    ...repoAttrs,
    'gen_ai.request.model': String(model).slice(0, 128),
  };

  const isStop = Boolean(payload.terminationReason) || process.argv.includes('--stop');
  const preToolFlag = isPreTool || process.argv.includes('--pre-tool');
  const postInvocFlag = isPostInvocation || process.argv.includes('--post-invocation');

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

  // 트랜스크립트 경로 안전 해결
  const transcriptPath = await resolveTranscriptPath(payload);

  // 2. Stop 이벤트: 최종 세션 요약 및 남은 턴 증분 마감 전송
  if (isStop) {
    // 세션 종료 전 아직 전송되지 않은 증분 턴들 모두 전송
    const { turns, subagents } = await drainIncrementalTurns(sessionId, transcriptPath, telemetry, model, baseAttrs, clock);
    events.push(...turns, ...subagents);

    let durationMs = 0;
    let startTime = clock;
    let subagentCount = subagents.length;

    if (transcriptPath) {
      try {
        const content = await readFile(transcriptPath, 'utf8');
        const lines = content.split('\n').filter(Boolean);
        const timestamps = [];

        for (const line of lines) {
          try {
            const step = JSON.parse(line);
            if (step.created_at) timestamps.push(new Date(step.created_at).getTime());
            if (step.tool_calls && Array.isArray(step.tool_calls)) {
              for (const tc of step.tool_calls) {
                if (tc.name === 'invoke_subagent') subagentCount++;
              }
            }
          } catch {}
        }

        if (timestamps.length > 0) {
          startTime = Math.min(...timestamps);
          durationMs = Math.max(0, clock - startTime);
        }
      } catch {}
    }

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
      stopState = { version: 2, event: sessionEvent, queued: false };
      await save(stopPath, stopState);
    }
    if (!stopState.queued && await telemetry.enqueue(stopState.event)) {
      stopState.queued = true;
      await save(stopPath, stopState);
      events.push(stopState.event);
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

    // Keep the first completion body and a local receipt after durable acceptance.
    if (!toolState?.queued) {
      const receipt = { version: 2, eventId, startTimeMs, toolName, event: toolEvent, queued: false };
      await save(filePath, receipt);
      if (await telemetry.enqueue(toolEvent)) {
        receipt.queued = true;
        await save(filePath, receipt);
        events.push(toolEvent);
      }
    }

    // 실시간 증분 턴 전송 (세션 중 강제 종료 방지)
    const { turns, subagents } = await drainIncrementalTurns(sessionId, transcriptPath, telemetry, model, baseAttrs, clock);
    events.push(...turns, ...subagents);
  }

  await telemetry.flushLocal();
  return events;
}

if (isMain(import.meta.url)) {
  try {
    const { values } = parseArgs({
      options: {
        config: { type: 'string' },
        'pre-tool': { type: 'boolean' },
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
    const telemetry = createTelemetry('agy', { configPath, profile: homedir() });
    await handleHook(payload, {
      telemetry,
      isPreTool: values['pre-tool'],
      isPostInvocation: values['post-invocation'],
    });
  } catch {}

  console.log(process.argv.includes('--pre-tool') ? JSON.stringify({ decision: 'allow' }) : '{}');
}
