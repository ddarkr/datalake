import { createHash } from 'node:crypto';
import { mkdir, open, readFile, stat, unlink, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createTelemetry, repositoryContext, isMain } from '../otel.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const identifier = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,256}$/.test(value) ? value : undefined;
const number = value => Number.isSafeInteger(value) && value >= 0 ? value : undefined;
const builtins = new Set(['Bash', 'PowerShell', 'Read', 'Write', 'Edit', 'MultiEdit', 'NotebookEdit', 'Glob', 'Grep', 'Agent', 'Task', 'WebFetch', 'WebSearch', 'AskUserQuestion', 'ExitPlanMode', 'EnterPlanMode', 'TodoWrite', 'TaskCreate', 'TaskUpdate', 'TaskGet', 'TaskList', 'TaskOutput', 'TaskStop', 'Skill', 'ToolSearch']);
const agentTypes = new Set(['Explore', 'Plan', 'general-purpose', 'Bash', 'statusline-setup', 'claude-code-guide']);
const events = new Set(['SessionStart', 'SessionEnd', 'PreToolUse', 'PostToolUse', 'PostToolUseFailure', 'SubagentStart', 'SubagentStop', 'Stop', 'StopFailure']);
const finished = new Set(['end_turn', 'tool_use', 'max_tokens', 'stop_sequence', 'refusal', 'pause_turn', 'model_context_window_exceeded']);

// Hooks expose content-bearing objects: build fresh metadata, never spread hook input.
export function usageEvent(record, { sessionId, sinceMs, agentId }) {
  if (record?.type !== 'assistant' || record.sessionId !== sessionId || record.isApiErrorMessage) return;
  if (agentId ? record.agentId !== agentId : record.isSidechain || record.agentId) return;
  const message = record.message;
  const id = identifier(message?.id);
  const time = typeof record.timestamp === 'string' ? Date.parse(record.timestamp) : NaN;
  if (!id || !Number.isFinite(time) || time < sinceMs || !finished.has(message.stop_reason)) return;
  if (typeof message.model !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(message.model)) return;
  const attributes = { 'gen_ai.operation.name': 'chat', 'gen_ai.provider.name': 'anthropic', 'gen_ai.response.model': message.model };
  for (const [source, target] of [
    ['input_tokens', 'gen_ai.usage.input_tokens'],
    ['output_tokens', 'gen_ai.usage.output_tokens'],
    ['cache_read_input_tokens', 'gen_ai.usage.cache_read.input_tokens'],
    ['cache_creation_input_tokens', 'gen_ai.usage.cache_creation.input_tokens'],
  ]) {
    const value = number(message.usage?.[source]);
    if (value !== undefined) attributes[target] = value;
  }
  if (!Object.keys(attributes).some(key => key.startsWith('gen_ai.usage.'))) return;
  if (agentId) attributes['coding_agent.agent.id'] = agentId;
  // A transcript records a message timestamp, not request duration. Never invent latency or cost.
  return { kind: 'llm.turn', sessionId, eventId: `message:${id}`, parentEventId: agentId ? `subagent:${agentId}` : undefined, startTimeMs: time, endTimeMs: time, attributes };
}

async function json(path) {
  try { return JSON.parse(await readFile(path, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
}
async function save(path, value, exclusive = false) {
  try { await writeFile(path, JSON.stringify(value), { mode: 0o600, flag: exclusive ? 'wx' : 'w' }); return true; }
  catch (error) { if (exclusive && error.code === 'EEXIST') return false; throw error; }
}

// Only the transcript explicitly supplied by the current hook is opened; never discover sessions.
// ponytail: scan at most 16 MiB per hook and skip >1 MiB records; native telemetry covers unrecorded usage.
async function transcriptEvents(path, options, stateDir) {
  if (typeof path !== 'string' || !path) return [];
  let file;
  try { file = await open(path, 'r'); } catch (error) { if (error.code === 'ENOENT') return []; throw error; }
  try {
    const info = await file.stat();
    if (!info.isFile()) return [];
    const cursorPath = join(stateDir, `cursor-${hash(path)}.json`);
    const cursor = await json(cursorPath);
    let offset = cursor?.ino === info.ino && cursor?.offset <= info.size ? cursor.offset : 0;
    const end = Math.min(info.size, offset + 16 * 1024 * 1024);
    const buffer = Buffer.allocUnsafe(64 * 1024);
    let pending = Buffer.alloc(0), dropped = false, committed = offset;
    const found = new Map();
    while (offset < end) {
      const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, end - offset), offset);
      if (!bytesRead) break;
      let start = 0;
      for (let index = 0; index < bytesRead; index++) {
        if (buffer[index] !== 10) continue;
        const part = buffer.subarray(start, index);
        if (!dropped && pending.length + part.length <= 1024 * 1024) {
          const line = pending.length ? Buffer.concat([pending, part]) : part;
          try {
            const event = usageEvent(JSON.parse(line.toString('utf8')), options);
            if (event) found.set(event.eventId, event);
          } catch { /* Partial/corrupt transcript records are not telemetry. */ }
        }
        pending = Buffer.alloc(0); dropped = false; start = index + 1;
        committed = offset + start;
      }
      if (!dropped && start < bytesRead) {
        const part = buffer.subarray(start, bytesRead);
        if (pending.length + part.length > 1024 * 1024) { pending = Buffer.alloc(0); dropped = true; }
        else pending = Buffer.concat([pending, part]);
      }
      offset += bytesRead;
    }
    await save(cursorPath, { ino: info.ino, offset: committed });
    return [...found.values()];
  } finally { await file.close(); }
}

export async function handleHook(input, { telemetry, stateRoot, now = Date.now(), context = repositoryContext } = {}) {
  const sessionId = identifier(input?.session_id);
  const hook = input?.hook_event_name;
  if (!sessionId || !events.has(hook)) return;
  const stateDir = join(stateRoot, hash(sessionId));
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  const sessionPath = join(stateDir, 'session.json');
  let session = await json(sessionPath);
  if (hook === 'SessionStart') {
    if (!session || session.endTimeMs !== undefined) {
      session = { kind: 'session', sessionId, eventId: `session:${now}`, startTimeMs: now, attributes: await context(input.cwd) };
      await save(sessionPath, session);
      if (typeof input.transcript_path === 'string') {
        try {
          const info = await stat(input.transcript_path);
          await save(join(stateDir, `cursor-${hash(input.transcript_path)}.json`), { ino: info.ino, offset: info.size });
        } catch (error) { if (error.code !== 'ENOENT') throw error; }
      }
    }
    return;
  }
  // Without SessionStart there is no safe new-session boundary for transcript usage.
  const agentId = identifier(input.agent_id);
  const toolId = identifier(input.tool_use_id);
  const agentFile = agentId && join(stateDir, `agent-${hash(agentId)}.json`);
  const isTool = ['PreToolUse', 'PostToolUse', 'PostToolUseFailure'].includes(hook);
  const eventId = isTool ? toolId && `tool:${toolId}` : agentId && `subagent:${agentId}`;
  const eventPath = isTool ? eventId && join(stateDir, `tool-${hash(toolId)}.json`) : agentFile;
  if (hook === 'PreToolUse' || hook === 'SubagentStart') {
    if (!eventId) return;
    const attributes = isTool
      ? { 'gen_ai.tool.name': builtins.has(input.tool_name) ? input.tool_name : 'custom', 'gen_ai.tool.call.id': toolId }
      : { 'coding_agent.agent.id': agentId, 'coding_agent.subagent.type': agentTypes.has(input.agent_type) ? input.agent_type : 'custom' };
    if (isTool && agentId) attributes['coding_agent.agent.id'] = agentId;
    await save(eventPath, { kind: isTool ? 'tool.call' : 'subagent', sessionId, eventId, parentEventId: isTool && agentId ? `subagent:${agentId}` : session?.eventId, startTimeMs: now, attributes }, true);
    return;
  }
  async function emitOnce(event) {
    const claim = join(stateDir, `sent-${hash(event.eventId)}.json`);
    if (!await save(claim, {}, true)) return;
    try {
      if (!await telemetry.emit(event)) await unlink(claim);
    } catch (error) { await unlink(claim).catch(() => {}); throw error; }
  }
  const completed = [];
  if (isTool || hook === 'SubagentStop') {
    if (eventId) {
      let event = await json(eventPath);
      const observedStart = event !== undefined;
      // No pre-hook means no observed start: completion is still a real zero-duration event.
      if (!event) event = { kind: isTool ? 'tool.call' : 'subagent', sessionId, eventId, startTimeMs: now, parentEventId: session?.eventId, attributes: isTool ? { 'gen_ai.tool.name': builtins.has(input.tool_name) ? input.tool_name : 'custom', 'gen_ai.tool.call.id': toolId } : { 'coding_agent.agent.id': agentId, 'coding_agent.subagent.type': agentTypes.has(input.agent_type) ? input.agent_type : 'custom' } };
      if (isTool && hook === 'PostToolUseFailure') event.error = true;
      if (event.endTimeMs === undefined) {
        event.endTimeMs = Math.max(event.startTimeMs, now);
        if (observedStart) {
          const duration = Math.max(0, event.endTimeMs - event.startTimeMs);
          if (isTool) event.attributes['duration_ms'] = duration;
          else event.attributes['coding_agent.subagent.duration_ms'] = duration;
        }
        await save(eventPath, event);
      }
      completed.push(event);
    }
  }
  if (session && ['Stop', 'StopFailure', 'SessionEnd', 'SubagentStop', 'PostToolUse', 'PostToolUseFailure'].includes(hook)) {
    const agent = hook === 'SubagentStop' && agentFile ? await json(agentFile) : undefined;
    const transcript = hook === 'SubagentStop' ? input.agent_transcript_path : input.transcript_path;
    // Missing SubagentStart cannot distinguish a new run from historical resumed content.
    if (hook !== 'SubagentStop' || agent) {
      const usage = await transcriptEvents(transcript, { sessionId, sinceMs: agent?.startTimeMs ?? session.startTimeMs, agentId: hook === 'SubagentStop' ? agentId : undefined }, stateDir);
      completed.push(...usage.map(event => ({ ...event, parentEventId: event.parentEventId ?? session.eventId })));
    }
  }
  if (session && hook === 'SessionEnd') {
    if (session.endTimeMs === undefined) {
      session.endTimeMs = Math.max(session.startTimeMs, now);
      session.attributes['coding_agent.session.duration_ms'] = session.endTimeMs - session.startTimeMs;
      await save(sessionPath, session);
    }
    completed.unshift(session);
  }
  await Promise.all(completed.map(emitOnce));
}

async function main() {
  // OTEL_* is scrubbed from Claude hook children; installation stores only a private config path.
  const pointer = await json(fileURLToPath(new URL('./config-path.json', import.meta.url)));
  const telemetry = createTelemetry('claude-code', { configPath: process.env.DATALAKE_OTEL_CONFIG || pointer?.configPath });
  const stateRoot = join(process.env.CLAUDE_PLUGIN_DATA || join(process.env.CLAUDE_CONFIG_DIR || join(homedir(), '.claude'), 'plugins', 'data', 'doda-datalake-otel'), 'sessions');
  let input = '', size = 0;
  process.stdin.setEncoding('utf8');
  for await (const chunk of process.stdin) {
    size += Buffer.byteLength(chunk);
    if (size > 8 * 1024 * 1024) return;
    input += chunk;
  }
  try {
    // Native usage received by the datalake takes precedence; the native exporter may target
    // a different backend or emit only metrics, so its enable flag must not suppress this path.
    await handleHook(JSON.parse(input), { telemetry, stateRoot });
  } finally { await telemetry.flush(); }
}

if (isMain(import.meta.url)) {
  main().catch(() => { process.stderr.write('[datalake-otel] Claude hook telemetry unavailable; agent execution continues.\n'); });
}
