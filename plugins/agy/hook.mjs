#!/usr/bin/env node
import { readFile, writeFile, unlink, mkdir, stat, appendFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { join, isAbsolute } from 'node:path';
import { createTelemetry, repositoryContext, isMain } from '../otel.mjs';

async function resolveConfigPath() {
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

function toolStatePath(sessionId, stepIdx) {
  return join(stateDir(sessionId), `tool-${stepIdx}.json`);
}

function cursorPath(sessionId) {
  return join(stateDir(sessionId), 'cursor.json');
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

  let cursor = { lastProcessedIndex: -1, sentEventIds: [] };
  try {
    cursor = JSON.parse(await readFile(cPath, 'utf8'));
  } catch {}

  const turns = [];
  const subagents = [];

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

          await telemetry.emit(turnEvent);
          turns.push(turnEvent);

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
                  await telemetry.emit(subagentEvent);
                  subagents.push(subagentEvent);
                }
              }
            }
          }

          cursor.lastProcessedIndex = stepIndex;
        }

        accumulatedPromptChars += (thinkingText.length + contentText.length + toolCallsText.length);
      }
    }

    // 커서 상태 저장
    await writeFile(cPath, JSON.stringify(cursor), { mode: 0o600 });
  } catch {}

  return { turns, subagents };
}

export async function handleHook(payload, { telemetry, now = Date.now, cwd = process.cwd(), isPreTool = false, isPostInvocation = false } = {}) {
  const sessionId = payload?.conversationId || payload?.session_id;
  if (!sessionId) return [];

  const clock = now();
  const repoAttrs = repositoryContext(cwd);
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
    const toolName = payload.toolCall?.name || 'unknown_tool';
    const filePath = toolStatePath(sessionId, stepIdx);

    try {
      await mkdir(stateDir(sessionId), { recursive: true, mode: 0o700 });
      await writeFile(filePath, JSON.stringify({ startTimeMs: clock, toolName }), { mode: 0o600 });
    } catch {}
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

    await telemetry.emit(sessionEvent);
    events.push(sessionEvent);
  } else {
    // 3. PostToolUse: 도구 실행 완료 시 실제 소요시간 계산 및 직전 턴 증분 즉시 전송
    const stepIdx = payload.stepIdx ?? 0;
    let toolName = payload.toolName || (payload.toolCall?.name);
    const isError = Boolean(payload.error);
    const filePath = toolStatePath(sessionId, stepIdx);

    let startTimeMs = clock - 50;
    try {
      const data = JSON.parse(await readFile(filePath, 'utf8'));
      if (data?.startTimeMs && Number.isFinite(data.startTimeMs)) {
        startTimeMs = data.startTimeMs;
      }
      if (!toolName && data?.toolName) {
        toolName = data.toolName;
      }
      await unlink(filePath).catch(() => {});
    } catch {}
    if (!toolName) toolName = 'unknown_tool';

    const endTimeMs = clock;
    const durationMs = Math.max(1, endTimeMs - startTimeMs);

    const toolEvent = {
      kind: 'tool.call',
      sessionId: String(sessionId),
      eventId: `tool:${stepIdx}:${toolName}`,
      startTimeMs,
      endTimeMs,
      error: isError,
      attributes: {
        ...baseAttrs,
        'gen_ai.tool.name': String(toolName).slice(0, 128),
        'duration_ms': durationMs,
        ...(isError ? { 'error.type': String(payload.error).slice(0, 256) } : {}),
      },
    };

    await telemetry.emit(toolEvent);
    events.push(toolEvent);

    // 실시간 증분 턴 전송 (세션 중 강제 종료 방지)
    const { turns, subagents } = await drainIncrementalTurns(sessionId, transcriptPath, telemetry, model, baseAttrs, clock);
    events.push(...turns, ...subagents);
  }

  await telemetry.flush();
  return events;
}

if (isMain(import.meta.url)) {
  let raw = '';
  process.stdin.setEncoding('utf8');
  for await (const chunk of process.stdin) {
    raw += chunk;
  }

  try {
    const payload = raw.trim() ? JSON.parse(raw) : {};
    const configPath = await resolveConfigPath();
    const telemetry = createTelemetry('agy', { configPath });

    const isPreTool = process.argv.includes('--pre-tool');
    const isPostInvocation = process.argv.includes('--post-invocation');

    const events = await handleHook(payload, { telemetry, isPreTool, isPostInvocation });

    // 검증용 로컬 로그 기록
    try {
      const logFile = join(homedir(), '.config', 'doda-datalake', 'agy_telemetry.log');
      const logEntry = JSON.stringify({
        time: new Date().toISOString(),
        sessionId: payload?.conversationId || payload?.session_id,
        isStop: Boolean(payload.terminationReason),
        isPreTool,
        eventCount: events.length,
        events: events.map(e => `${e.kind}:${e.eventId}`)
      }) + '\n';
      await appendFile(logFile, logEntry, 'utf8');
    } catch {}
  } catch (err) {}

  if (process.argv.includes('--pre-tool')) {
    console.log(JSON.stringify({ decision: 'allow' }));
  } else {
    console.log('{}');
  }
}
