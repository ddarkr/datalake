import { createDirectTelemetry, loadConfig, repositoryContext } from '../otel.mjs';
import { setTimeout as delay } from 'node:timers/promises';

// OMP source contract c2cb79e4: extensions/types.ts, task/executor.ts and
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
const tiers = new Set(['auto', 'default', 'flex', 'scale', 'priority', 'ultrafast', 'fast', 'normal']);
const efforts = new Set(['none', 'off', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max']);

function settings(pi, message) {
  const attrs = {};
  const effort = pi.getThinkingLevel?.();
  if (efforts.has(effort)) attrs['coding_agent.request.reasoning.effort'] = effort;
  const family = ['openai', 'openai-codex'].includes(message.provider) ? 'openai'
    : message.api === 'anthropic-messages' ? 'anthropic'
      : ['google', 'google-vertex'].includes(message.provider) ? 'google' : undefined;
  const tier = family && pi.getServiceTiers?.()?.[family];
  if (tiers.has(tier)) attrs['coding_agent.request.service_tier'] = tier;
  if (Object.keys(attrs).length) attrs['coding_agent.request.settings_source'] = 'harness';
  return attrs;
}

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
  const telemetry = options.telemetry ?? createDirectTelemetry('oh-my-pi', {
    configPath: options.configPath,
  });
  const clock = options.clock ?? Date.now;
  const repoContext = options.repositoryContext ?? repositoryContext;
  let pluginEndpoint;
  try { pluginEndpoint = loadConfig({ configPath: options.configPath })?.endpoint; } catch {}
  const nativeUsage = nativeTelemetryEnabled(options.env ?? process.env, pluginEndpoint);
  let current;
  const states = new Map();
  const ownRoutes = new Set();
  const scheduled = new Set();

  // Do not return exporter promises from observability hooks: slow/offline OTLP
  // must not delay tools, prompt delivery, or detached message notifications.
  function enqueue(state, event) {
    const id = event.eventId;
    if (state.attempted.has(id)) return;
    state.attempted.add(id);
    const metadata = { ...event, sessionId: state.sessionId, attributes: { ...event.attributes } };
    const projected = state.attributesReady.then(() => Object.freeze({
      ...metadata, attributes: Object.freeze({ ...state.attributes, ...metadata.attributes }),
    }));
    const operation = projected.then(event => telemetry.send(event)).catch(() => {}).finally(() => {
      scheduled.delete(operation);
    });
    scheduled.add(operation);
  }

  function stateFor(ctx) {
    const id = ctx.sessionManager.getSessionId();
    if (!identity(id)) return undefined;
    if (current?.nativeId === id) return current;
    if (current) close(current);
    const route = childRoutes.get(ctx.sessionManager.getSessionFile());
    const prior = states.get(id);
    const previous = prior?.sessionId === (route?.sessionId ?? id) ? prior : undefined;
    current = {
      nativeId: id, sessionId: route?.sessionId ?? id,
      rootId: route?.eventId ?? `session:${id}:${clock()}`,
      start: clock(), attempted: previous?.attempted ?? new Set(),
      tools: new Map(), children: new Map(),
      attributes: { 'coding_agent.agent.id': route?.agentId ?? id },
      child: Boolean(route), closed: false,
      nativeUsage: route?.nativeUsage ?? nativeUsage,
    };
    if (!route && current.nativeUsage) {
      console.error('[datalake-otel] OMP native traces own LLM usage at the same collector; plugin usage export is disabled for this session tree.');
    }
    if (route) current.attributes['coding_agent.agent.parent_id'] = route.parentAgentId;
    states.set(id, current);
    // Capture only the directory string, never the native context or event.
    const state = current;
    const cwd = ctx.cwd;
    // ponytail: optional Git metadata gets 500ms; use a cached snapshot if slow repositories need full coverage.
    const metadataBudget = new AbortController();
    state.attributesReady = Promise.race([
      Promise.resolve().then(() => repoContext(cwd)),
      delay(500, {}, { signal: metadataBudget.signal }),
    ]).then(attrs => {
      Object.assign(state.attributes, attrs);
    }).catch(() => {}).finally(() => metadataBudget.abort());
    return state;
  }

  function close(state) {
    state.input = state.interaction = state.request = state.nextSettings = undefined;
    if (!state.closed) {
      state.closed = true;
      state.end = clock();
    }
    if (!state.child) enqueue(state, {
      kind: 'session', eventId: state.rootId,
      startTimeMs: state.start, endTimeMs: state.end,
    });
  }

  for (const name of ['session_start', 'session_switch', 'session_branch']) {
    pi.on(name, (_event, ctx) => {
      if (current) current.input = current.interaction = current.request = current.nextSettings = undefined;
      const state = stateFor(ctx);
      if (state) state.busy = state.ambiguous = false;
    });
  }
  pi.on('input', (event, ctx) => {
    const state = stateFor(ctx);
    if (!state) return;
    if (event.source !== 'interactive' || state.busy || state.input !== undefined) {
      state.input = undefined;
      state.interaction = undefined;
      state.ambiguous = true;
    } else if (!state.ambiguous) state.input = clock();
  });
  pi.on('before_agent_start', (_event, ctx) => {
    const state = stateFor(ctx);
    if (!state) return;
    state.interaction = !state.busy && !state.ambiguous ? state.input : undefined;
    state.input = undefined;
    state.busy = true;
  });
  pi.on('agent_end', (_event, ctx) => {
    const state = stateFor(ctx);
    if (state) {
      state.busy = state.ambiguous = false;
      state.input = state.interaction = state.request = state.nextSettings = undefined;
    }
  });
  pi.on('message_start', (event, ctx) => {
    const message = event.message;
    if (message?.role !== 'assistant' || !number(message.timestamp)) return;
    const state = stateFor(ctx);
    if (!state) return;
    state.busy = true;
    if (state.request) state.interaction = undefined;
    state.request = { id: messageId(state, message), attributes: state.nextSettings ?? settings(pi, message) };
    state.nextSettings = undefined;
    if (!state.nativeUsage) state.lastMessage = state.request.id;
  });
  pi.on('before_provider_request', (event, ctx) => {
    const state = stateFor(ctx);
    if (!state) return;
    const attributes = settings(pi, ctx.model ?? {});
    const request = state.request ?? { attributes };
    if (!state.request) state.nextSettings = attributes;
    // Project only known scalar settings; never retain the provider body.
    const payload = event.payload;
    const tier = payload?.service_tier ?? payload?.serviceTier ?? payload?.speed;
    const effort = payload?.reasoning?.effort ?? payload?.reasoning_effort;
    const budget = payload?.thinking?.budget_tokens;
    if (tiers.has(tier)) request.attributes['coding_agent.request.service_tier'] = tier;
    if (efforts.has(effort)) request.attributes['coding_agent.request.reasoning.effort'] = effort;
    if (number(budget) && Number.isInteger(budget)) request.attributes['coding_agent.request.reasoning.budget_tokens'] = budget;
    if (tiers.has(tier) || efforts.has(effort) || (number(budget) && Number.isInteger(budget))) {
      request.attributes['coding_agent.request.settings_source'] = 'wire';
    }
  });
  pi.on('message_update', (event, ctx) => {
    const state = stateFor(ctx);
    const request = state?.request;
    const update = event.assistantMessageEvent;
    if (!request || event.message?.role !== 'assistant' ||
        messageId(state, event.message) !== request.id || request.firstText !== undefined ||
        update?.type !== 'text_delta' || typeof update.delta !== 'string' || !update.delta.length) return;
    request.firstText = clock();
  });
  pi.on('message_end', (event, ctx) => {
    const message = event.message;
    if (message?.role !== 'assistant' || !number(message.timestamp)) return;
    const state = stateFor(ctx);
    if (!state) return;
    const eventId = messageId(state, message);
    const request = state.request?.id === eventId ? state.request : undefined;
    const end = number(message.completedAt) ? message.completedAt : clock();
    const failed = message.stopReason === 'error' || message.stopReason === 'aborted';
    if (failed) state.interaction = state.input = undefined;
    if (!failed && request?.firstText !== undefined && number(state.interaction) &&
        request.firstText >= state.interaction) {
      enqueue(state, {
        kind: 'interaction', eventId: `interaction:${eventId}`, parentEventId: state.rootId,
        startTimeMs: state.interaction, endTimeMs: request.firstText,
        attributes: {
          'coding_agent.interaction.first_response_ms': request.firstText - state.interaction,
          'coding_agent.interaction.latency.kind': 'input_to_first_text',
        },
      });
      state.interaction = undefined;
    }
    if (request) state.request = undefined;
    state.lastMessage = state.nativeUsage ? undefined : eventId;
    if (state.nativeUsage) return;
    const usage = usageAttributes(message.usage);
    const attributes = { ...request?.attributes, ...usage };
    if (Object.keys(usage).some(key => key.startsWith('gen_ai.usage.'))) {
      attributes['coding_agent.usage.source'] = 'reported';
    }
    if (number(message.duration)) {
      attributes['coding_agent.request.duration_ms'] = message.duration;
      attributes['coding_agent.duration.kind'] = 'provider_call';
    }
    if (number(message.ttft) && (!number(message.duration) || message.ttft <= message.duration)) {
      attributes.ttft_ms = message.ttft;
      attributes['coding_agent.ttft.kind'] = ['openai-responses', 'openai-codex-responses'].includes(message.api)
        ? 'first_output_item' : 'provider_reported';
    }
    if (request?.firstText >= message.timestamp && request.firstText <= end &&
        (!number(message.duration) || request.firstText - message.timestamp <= message.duration)) {
      attributes['coding_agent.response.first_text_ms'] = request.firstText - message.timestamp;
    }
    if (tiers.has(message.serviceTier)) attributes['coding_agent.harness.service_tier'] = message.serviceTier;
    for (const [key, value] of [
      ['gen_ai.provider.name', message.provider],
      ['gen_ai.request.model', message.model],
      ['gen_ai.response.model', message.upstreamModel ?? message.model],
      ['gen_ai.response.id', message.responseId],
    ]) if (identity(value)) attributes[key] = value;
    enqueue(state, {
      kind: 'llm.turn', eventId, parentEventId: state.rootId,
      startTimeMs: message.timestamp, endTimeMs: Math.max(message.timestamp, end), attributes,
      error: failed,
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
    enqueue(state, {
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
    enqueue(state, {
      kind: 'subagent', eventId: child.eventId, parentEventId: child.parent,
      startTimeMs: child.start, endTimeMs: end, attributes,
      error: payload.status !== 'completed',
    });
  });
  pi.on('session_shutdown', async () => {
    for (const state of states.values()) close(state);
    unsubscribe();
    for (const path of ownRoutes) childRoutes.delete(path);
    while (scheduled.size) await Promise.all([...scheduled]);
    await telemetry.flush().catch(() => {});
  });
}
