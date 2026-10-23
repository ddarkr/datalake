import { createHash, randomUUID } from 'node:crypto';
import { mkdir, open, readFile, rename, stat, unlink, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createDirectTelemetry, repositoryContext, isMain, withStateLock } from '../otel.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const identifier = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,256}$/.test(value) ? value : undefined;
const number = value => Number.isSafeInteger(value) && value >= 0 ? value : undefined;
const builtins = new Set(['Bash', 'PowerShell', 'Read', 'Write', 'Edit', 'MultiEdit', 'NotebookEdit', 'Glob', 'Grep', 'Agent', 'Task', 'WebFetch', 'WebSearch', 'AskUserQuestion', 'ExitPlanMode', 'EnterPlanMode', 'TodoWrite', 'TaskCreate', 'TaskUpdate', 'TaskGet', 'TaskList', 'TaskOutput', 'TaskStop', 'Skill', 'ToolSearch']);
const agentTypes = new Set(['Explore', 'Plan', 'general-purpose', 'Bash', 'statusline-setup', 'claude-code-guide']);
const events = new Set(['SessionStart', 'SessionEnd', 'PreToolUse', 'PostToolUse', 'PostToolUseFailure', 'SubagentStart', 'SubagentStop', 'Stop', 'StopFailure', 'UserPromptSubmit', 'MessageDisplay']);
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
  attributes['coding_agent.usage.source'] = 'reported';
  if (agentId) attributes['coding_agent.agent.id'] = agentId;
  // A transcript records a message timestamp, not request duration. Never invent latency or cost.
  return { kind: 'llm.turn', sessionId, eventId: `message:${id}`, parentEventId: agentId ? `subagent:${agentId}` : undefined, startTimeMs: time, endTimeMs: time, attributes };
}

async function json(path) {
  try { return JSON.parse(await readFile(path, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
}
async function save(path, value, exclusive = false) {
  if (exclusive) {
    try { await writeFile(path, JSON.stringify(value), { mode: 0o600, flag: 'wx' }); return true; }
    catch (error) { if (error.code === 'EEXIST') return false; throw error; }
  }
  const temporary = `${path}.${randomUUID()}`;
  await writeFile(temporary, JSON.stringify(value), { mode: 0o600, flag: 'wx' });
  try { await rename(temporary, path); return true; }
  finally { await unlink(temporary).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
}

// Only the transcript explicitly supplied by the current hook is opened; never discover sessions.
// ponytail: scan at most 16 MiB per hook and skip >1 MiB records; native telemetry covers unrecorded usage.
async function transcriptEvents(path, options, stateDir) {
  if (typeof path !== 'string' || !path) return { events: [] };
  let file;
  try { file = await open(path, 'r'); } catch (error) { if (error.code === 'ENOENT') return { events: [] }; throw error; }
  try {
    const info = await file.stat();
    if (!info.isFile()) return { events: [] };
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
    return { events: [...found.values()], checkpoint: { path: cursorPath, value: { ino: info.ino, offset: committed } } };
  } finally { await file.close(); }
}

export async function handleHook(input, { telemetry, stateRoot, now = Date.now(), context = repositoryContext, source = 'plugin' } = {}) {
  // Leave time for attempt receipts/checkpoint and process exit before Claude's 5s kill.
  const deadline = performance.now() + 3000;
  const sessionId = identifier(input?.session_id);
  const hook = input?.hook_event_name;
  if (!sessionId || !events.has(hook)) return;
  const stateDir = join(stateRoot, hash(sessionId));
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  if (hook === 'MessageDisplay' || hook === 'UserPromptSubmit') {
    const observationDir = join(stateDir, 'display');
    await mkdir(observationDir, { recursive: true, mode: 0o700 });
    // This lock protects disk checkpoints only, never HTTP or the session send lock.
    return withStateLock(observationDir, async () => {
      const path = join(observationDir, 'pending.json');
      const pending = await json(path);
      const promptId = identifier(input.prompt_id);
      if (hook === 'UserPromptSubmit') {
        await save(path, { promptId, submittedAtMs: now, ambiguous: !!pending || !promptId });
        return;
      }
      // Non-final batch zero proves streaming rather than completed SDK/-p output.
      // Prompt hooks also fire for automation: this is NOT human input-to-response time.
      if (!pending || pending.ambiguous || pending.observed || !promptId || promptId !== pending.promptId ||
          !identifier(input.turn_id) || !identifier(input.message_id) || input.index !== 0 ||
          input.final !== false || typeof input.delta !== 'string' || input.delta.length === 0 || now < pending.submittedAtMs) return;
      await save(join(observationDir, 'ready.json'), {
        kind: 'interaction', sessionId, eventId: `display:${promptId}`,
        startTimeMs: pending.submittedAtMs, endTimeMs: now,
        attributes: { 'coding_agent.interaction.first_response_ms': now - pending.submittedAtMs,
          'coding_agent.interaction.latency.kind': 'hook_submit_to_display_ready' },
      });
      await save(path, { ...pending, observed: true });
    });
  }
  return withStateLock(stateDir, () => handleSerialized(input, { telemetry, stateDir, now, context, source, deadline }, sessionId, hook));
}

async function handleSerialized(input, { telemetry, stateDir, now, context, source, deadline }, sessionId, hook) {
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
  let deferred = false;
  let remainingWaves = 1;
  const settledEvents = new Set();
  async function attemptOnce(event) {
    // Existing sent receipts prove collector ACK; legacy queued-v2 proves only durable
    // local acceptance. Both are treated as attempted: no automatic replay on failure.
    const legacy = join(stateDir, `sent-${hash(event.eventId)}.json`);
    const queued = join(stateDir, `queued-v2-${hash(event.eventId)}.json`);
    const attempted = join(stateDir, `attempted-${hash(event.eventId)}.json`);
    if ((await json(legacy)) !== undefined || (await json(queued)) !== undefined || (await json(attempted)) !== undefined) {
      settledEvents.add(event.eventId);
      return true;
    }
    const remaining = Math.floor(deadline - performance.now());
    if (remaining < 1) {
      deferred = true;
      return false; // No attempt/receipt: leave the transcript checkpoint for a later hook.
    }
    const timeoutMs = Math.max(1, Math.floor(remaining / remainingWaves));
    let ok = false;
    try { ok = await telemetry.send(event, { timeoutMs }); } catch {}
    await save(attempted, {});
    settledEvents.add(event.eventId);
    return ok === true;
  }
  const completed = [];
  let displayReady;
  if (!agentId) {
    const observationDir = join(stateDir, 'display');
    await mkdir(observationDir, { recursive: true, mode: 0o700 });
    await withStateLock(observationDir, async () => {
      const readyPath = join(observationDir, 'ready.json');
      const ready = await json(readyPath);
      if (ready) {
        completed.push({ ...ready, parentEventId: session?.eventId });
        displayReady = { path: readyPath, eventId: ready.eventId, directory: observationDir };
      }
      if (['Stop', 'StopFailure', 'SessionEnd'].includes(hook)) {
        await unlink(join(observationDir, 'pending.json')).catch(error => { if (error.code !== 'ENOENT') throw error; });
      }
    });
  }
  let checkpoint;
  if (isTool || hook === 'SubagentStop') {
    if (eventId) {
      let event = await json(eventPath);
      const observedStart = event !== undefined;
      // No pre-hook means no observed start: completion is still a real zero-duration event.
      if (!event) event = { kind: isTool ? 'tool.call' : 'subagent', sessionId, eventId, startTimeMs: now, parentEventId: session?.eventId, attributes: isTool ? { 'gen_ai.tool.name': builtins.has(input.tool_name) ? input.tool_name : 'custom', 'gen_ai.tool.call.id': toolId } : { 'coding_agent.agent.id': agentId, 'coding_agent.subagent.type': agentTypes.has(input.agent_type) ? input.agent_type : 'custom' } };
      if (event.endTimeMs === undefined) {
        if (isTool && hook === 'PostToolUseFailure') event.error = true;
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
  if (source !== 'native' && session && ['Stop', 'StopFailure', 'SessionEnd', 'SubagentStop', 'PostToolUse', 'PostToolUseFailure'].includes(hook)) {
    const agent = hook === 'SubagentStop' && agentFile ? await json(agentFile) : undefined;
    const transcript = hook === 'SubagentStop' ? input.agent_transcript_path : input.transcript_path;
    // Missing SubagentStart cannot distinguish a new run from historical resumed content.
    if (hook !== 'SubagentStop' || agent) {
      const usage = await transcriptEvents(transcript, { sessionId, sinceMs: agent?.startTimeMs ?? session.startTimeMs, agentId: hook === 'SubagentStop' ? agentId : undefined }, stateDir);
      checkpoint = usage.checkpoint;
      completed.push(...usage.events.map(event => ({ ...event, parentEventId: event.parentEventId ?? session.eventId })));
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
  // Divide one invocation-wide budget across bounded waves, including configured
  // exporter timeouts up to 30s. No retries, queues, or receipts for deferred work.
  const outcome = [];
  for (let offset = 0; offset < completed.length; offset += 4) {
    remainingWaves = Math.ceil((completed.length - offset) / 4);
    outcome.push(...await Promise.allSettled(completed.slice(offset, offset + 4).map(attemptOnce)));
  }
  if (displayReady && settledEvents.has(displayReady.eventId)) {
    await withStateLock(displayReady.directory, async () => {
      if ((await json(displayReady.path))?.eventId === displayReady.eventId) await unlink(displayReady.path);
    });
  }
  if (checkpoint && !deferred && outcome.every(result => result.status === 'fulfilled')) {
    await save(checkpoint.path, checkpoint.value);
  }
  const failure = outcome.find(result => result.status === 'rejected');
  if (failure) throw failure.reason;
}

async function main() {
  // OTEL_* is scrubbed from Claude hook children; installation stores only a private config path.
  const pointer = await json(fileURLToPath(new URL('./config-path.json', import.meta.url)));
  const dataRoot = process.env.CLAUDE_PLUGIN_DATA || join(process.env.CLAUDE_CONFIG_DIR || join(homedir(), '.claude'), 'plugins', 'data', 'doda-datalake-otel');
  const telemetry = createDirectTelemetry('claude-code', {
    configPath: process.env.DATALAKE_OTEL_CONFIG || pointer?.configPath,
  });
  const stateRoot = join(dataRoot, 'sessions');
  let input = '', size = 0;
  process.stdin.setEncoding('utf8');
  for await (const chunk of process.stdin) {
    size += Buffer.byteLength(chunk);
    if (size > 8 * 1024 * 1024) return;
    input += chunk;
  }
  try {
    await handleHook(JSON.parse(input), { telemetry, stateRoot, source: pointer?.source === 'native' ? 'native' : 'plugin' });
  } finally { await telemetry.flush().catch(() => {}); }
}

if (isMain(import.meta.url)) {
  main().catch(() => { process.stderr.write('[datalake-otel] Claude hook telemetry unavailable; agent execution continues.\n'); });
}
