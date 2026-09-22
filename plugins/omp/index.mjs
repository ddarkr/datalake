import { createTelemetry, loadConfig, repositoryContext } from '../otel.mjs';

// OMP 18.2.6: extensions/types.ts, task/executor.ts and
// tui/overlays/session-observer-registry.ts. Prepared factories are rebound
// in children; the spawning session's bus supplies the cross-session link.
// Coverage: live conversation messages, tool execution, task lifecycle and
// observed session activation intervals only. No history replay, oneshot
// title/compaction usage, LOC/acceptance inference, or git-command scraping.
// Cold child revival without an observed spawn keeps its own session trace.
// Unfinished operations on a hard kill cannot produce a final span.
// Native-mode CLI task/eval children inherit getTelemetry() through
// structured-subagent.ts -> executor.ts; native uses each child's session ID.
// Keep one source decision across the inherited tree, never mix root-native
// with root-attributed child-plugin usage. Custom SDK hosts that omit parent
// telemetry must use plugin-only configuration, not native coexistence mode.
const childRoutes = new Map();
const number = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;
const identity = value => typeof value === 'string' && value.length > 0 && value.length <= 512;
const messageId = (state, message) => `llm:${state.nativeId}:${message.timestamp}`;

// OMP's native TRACE exporter owns usage only when it sends to this collector.
// Generic OTLP endpoints append /v1/traces; signal-specific endpoints do not
// (OTLPTraceExporter defaults, used by telemetry-export-otlp.ts).
export function nativeTelemetryEnabled(env = process.env, pluginEndpoint) {
  if (!pluginEndpoint || env.OTEL_SDK_DISABLED?.trim().toLowerCase() === 'true') return false;
  const exporters = (env.OTEL_TRACES_EXPORTER ?? '').split(',').map(v => v.trim().toLowerCase()).filter(Boolean);
  const protocol = (env.OTEL_EXPORTER_OTLP_TRACES_PROTOCOL ?? env.OTEL_EXPORTER_OTLP_PROTOCOL)?.trim().toLowerCase();
  if (exporters.includes('none') || (exporters.length && !exporters.includes('otlp')) || (protocol && protocol !== 'http/protobuf')) return false;
  try {
    const specific = env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;
    const endpoint = new URL(specific ?? env.OTEL_EXPORTER_OTLP_ENDPOINT);
    if (specific === undefined) endpoint.pathname = `${endpoint.pathname.replace(/\/+$/, '')}/v1/traces`;
    return endpoint.href === new URL(pluginEndpoint).href;
  } catch { return false; }
}

export function usageAttributes(usage) {
  const attributes = {};
  if (!usage || typeof usage !== 'object') return attributes;
  // Native input is NON-cached; OTel input includes both cache buckets.
  if ([usage.input, usage.cacheRead, usage.cacheWrite].every(number)) {
    attributes['gen_ai.usage.input_tokens'] = usage.input + usage.cacheRead + usage.cacheWrite;
  }
  for (const [field, key] of [
    ['output', 'gen_ai.usage.output_tokens'],
    ['cacheRead', 'gen_ai.usage.cache_read.input_tokens'],
    ['cacheWrite', 'gen_ai.usage.cache_creation.input_tokens'],
    ['reasoningTokens', 'gen_ai.usage.reasoning.output_tokens'],
  ]) if (number(usage[field])) attributes[key] = usage[field];
  // OMP's supplied USD estimate, not locally fabricated rates or credit units.
  if (number(usage.cost?.total)) attributes['pi.gen_ai.cost.estimated_usd'] = usage.cost.total;
  return attributes;
}

/** Native default extension factory; second argument is used by the installed bootstrap. */
export default function ompTelemetry(pi, options = {}) {
  const telemetry = options.telemetry ?? createTelemetry('oh-my-pi', { configPath: options.configPath });
  const clock = options.clock ?? Date.now;
  const repoContext = options.repositoryContext ?? repositoryContext;
  let pluginEndpoint;
  try { pluginEndpoint = loadConfig({ configPath: options.configPath })?.endpoint; } catch {}
  const nativeUsage = nativeTelemetryEnabled(options.env ?? process.env, pluginEndpoint);
  let current;
  const states = new Map();
  const ownRoutes = new Set();

  // Do not return exporter promises from observability hooks: slow/offline OTLP
  // must not delay tools, prompt delivery, or detached message notifications.
  function emit(state, event) {
    if (state.sent.has(event.eventId)) return;
    state.sent.add(event.eventId);
    void Promise.resolve().then(() => telemetry.emit({
      ...event,
      sessionId: state.sessionId,
      attributes: { ...state.attributes, ...event.attributes },
    })).catch(() => {});
  }

  function stateFor(ctx) {
    const id = ctx.sessionManager.getSessionId();
    if (!identity(id)) return undefined;
    if (current?.nativeId === id) return current;
    if (current) close(current);
    const route = childRoutes.get(ctx.sessionManager.getSessionFile());
    current = {
      nativeId: id, sessionId: route?.sessionId ?? id,
      rootId: route?.eventId ?? `session:${id}:${clock()}`,
      start: clock(), sent: new Set(), tools: new Map(), children: new Map(),
      attributes: { 'coding_agent.agent.id': route?.agentId ?? id },
      child: Boolean(route), closed: false,
      nativeUsage: route?.nativeUsage ?? nativeUsage,
    };
    if (!route && current.nativeUsage) {
      console.error('[datalake-otel] OMP native traces own LLM usage at the same collector; plugin usage export is disabled for this session tree.');
    }
    if (route) current.attributes['coding_agent.agent.parent_id'] = route.parentAgentId;
    states.set(id, current);
    // Never retain the context, raw cwd, transcript, or native event object.
    const state = current;
    void Promise.resolve().then(() => repoContext(ctx.cwd)).then(attrs => {
      Object.assign(state.attributes, attrs);
    }).catch(() => {});
    return state;
  }

  function close(state) {
    if (state.closed) return;
    state.closed = true;
    if (!state.child) emit(state, {
      kind: 'session', eventId: state.rootId,
      startTimeMs: state.start, endTimeMs: clock(),
    });
  }

  for (const name of ['session_start', 'session_switch', 'session_branch']) {
    pi.on(name, (_event, ctx) => { stateFor(ctx); });
  }
  pi.on('message_start', (event, ctx) => {
    const message = event.message;
    if (message?.role !== 'assistant' || !number(message.timestamp)) return;
    const state = stateFor(ctx);
    if (state && !state.nativeUsage) state.lastMessage = messageId(state, message);
  });
  pi.on('message_end', (event, ctx) => {
    const message = event.message;
    if (message?.role !== 'assistant' || !number(message.timestamp)) return;
    const state = stateFor(ctx);
    if (!state) return;
    const eventId = messageId(state, message);
    state.lastMessage = state.nativeUsage ? undefined : eventId;
    if (state.nativeUsage) return;
    const attributes = usageAttributes(message.usage);
    for (const [key, value] of [
      ['gen_ai.provider.name', message.provider],
      ['gen_ai.request.model', message.model],
      ['gen_ai.response.model', message.upstreamModel ?? message.model],
      ['gen_ai.response.id', message.responseId],
    ]) if (identity(value)) attributes[key] = value;
    const end = number(message.completedAt) ? message.completedAt
      : number(message.duration) ? message.timestamp + message.duration : clock();
    emit(state, {
      kind: 'llm.turn', eventId, parentEventId: state.rootId,
      startTimeMs: message.timestamp, endTimeMs: Math.max(message.timestamp, end), attributes,
      error: message.stopReason === 'error' || message.stopReason === 'aborted',
    });
  });
  pi.on('tool_execution_start', (event, ctx) => {
    const state = stateFor(ctx);
    if (!state || !identity(event.toolCallId) || state.tools.has(event.toolCallId)) return;
    state.tools.set(event.toolCallId, { start: clock(), parent: state.lastMessage ?? state.rootId });
  });
  pi.on('tool_execution_end', (event, ctx) => {
    const state = stateFor(ctx);
    if (!state || !identity(event.toolCallId)) return;
    const tool = state.tools.get(event.toolCallId);
    const end = clock();
    const attributes = { 'gen_ai.tool.call.id': event.toolCallId };
    if (identity(event.toolName)) attributes['gen_ai.tool.name'] = event.toolName;
    emit(state, {
      kind: 'tool.call', eventId: `tool:${state.nativeId}:${event.toolCallId}`,
      parentEventId: tool?.parent ?? state.rootId,
      startTimeMs: tool?.start ?? end, endTimeMs: end,
      error: event.isError === true, attributes,
    });
    state.tools.delete(event.toolCallId);
    // Never consume task.result.details.usage: inherited child factories own
    // child LLM usage, so task aggregate usage would count it a second time.
  });

  const unsubscribe = pi.events.on('task:subagent:lifecycle', payload => {
    const state = current;
    if (!state || state.closed || !identity(payload?.id)) return;
    const key = `${payload.id}:${payload.parentToolCallId ?? ''}`;
    if (payload.status === 'started') {
      if (state.children.has(key)) return;
      const eventId = `subagent:${state.nativeId}:${key}`;
      const parent = identity(payload.parentToolCallId) ? `tool:${state.nativeId}:${payload.parentToolCallId}` : state.rootId;
      state.children.set(key, { eventId, parent, start: clock() });
      if (identity(payload.sessionFile)) {
        childRoutes.set(payload.sessionFile, {
          sessionId: state.sessionId, eventId,
          agentId: payload.id, parentAgentId: state.attributes['coding_agent.agent.id'],
          nativeUsage: state.nativeUsage,
        });
        ownRoutes.add(payload.sessionFile);
      }
      return;
    }
    if (!['completed', 'failed', 'aborted'].includes(payload.status)) return;
    const child = state.children.get(key);
    if (!child) return; // No invented duration/parent when startup was not observed.
    const end = clock();
    const attributes = {
      'coding_agent.agent.id': payload.id,
      'coding_agent.agent.parent_id': state.attributes['coding_agent.agent.id'],
      'coding_agent.subagent.status': payload.status,
      'coding_agent.subagent.duration_ms': Math.max(0, end - child.start),
    };
    if (identity(payload.agent)) attributes['coding_agent.subagent.type'] = payload.agent;
    emit(state, {
      kind: 'subagent', eventId: child.eventId, parentEventId: child.parent,
      startTimeMs: child.start, endTimeMs: end, attributes,
      error: payload.status !== 'completed',
    });
  });
  pi.on('session_shutdown', async () => {
    for (const state of states.values()) close(state);
    unsubscribe();
    for (const path of ownRoutes) childRoutes.delete(path);
    // Emit is queued in microtasks above; allow it to enter the sender first.
    await Promise.resolve();
    await telemetry.flush().catch(() => {});
  });
}
