import { setTimeout as delay } from 'node:timers/promises';
import { createTelemetry, repositoryContext } from '../otel.mjs';

// OpenCode 2.0.1/2.0.12: define() returns this exact { id, setup }
// object. Configured local plugins must be directories, not individual files.
export const pluginId = 'doda.datalake.otel';
const observed = new Set([
  'session.created', 'session.execution.started', 'session.execution.succeeded',
  'session.execution.failed', 'session.execution.interrupted', 'session.deleted',
  'session.step.started', 'session.step.ended', 'session.step.failed',
  'session.usage.recorded', 'session.tool.input.started', 'session.tool.called',
  'session.tool.success', 'session.tool.failed',
]);
const numeric = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;
const identifier = value => typeof value === 'string' && value.length > 0 && value.length <= 256;
const label = value => identifier(value) && /^[a-zA-Z0-9][a-zA-Z0-9._:/-]*$/.test(value) && !value.includes('://');

function modelAttributes(data) {
  const attributes = {};
  if (label(data.model?.id)) attributes['gen_ai.request.model'] = data.model.id;
  if (label(data.model?.providerID)) attributes['gen_ai.provider.name'] = data.model.providerID;
  if (label(data.agent)) attributes['coding_agent.agent.id'] = data.agent;
  return attributes;
}

function usageAttributes(data) {
  const attributes = {};
  const tokens = data.tokens;
  if (tokens && typeof tokens === 'object') {
    // Native TokenUsage.total adds five disjoint buckets; OTel totals include
    // cached input and reasoning output, with their breakdown retained below.
    if ([tokens.input, tokens.cache?.read, tokens.cache?.write].every(numeric)) {
      attributes['gen_ai.usage.input_tokens'] = tokens.input + tokens.cache.read + tokens.cache.write;
    }
    if ([tokens.output, tokens.reasoning].every(numeric)) {
      attributes['gen_ai.usage.output_tokens'] = tokens.output + tokens.reasoning;
    }
    for (const [key, value] of [
      ['gen_ai.usage.cache_read.input_tokens', tokens.cache?.read],
      ['gen_ai.usage.cache_write.input_tokens', tokens.cache?.write],
      ['gen_ai.usage.reasoning.output_tokens', tokens.reasoning],
    ]) if (numeric(value)) attributes[key] = value;
  }
  if (numeric(data.cost)) attributes.cost_usd = data.cost;
  return attributes;
}

// Only these metadata projections survive the callback. No transcript, prompt,
// input, result, exception message, filename, or arbitrary metadata is retained.
export function createEventMapper(baseAttributes = {}) {
  const sessions = new Map();
  return event => {
    if (!observed.has(event?.type) || !identifier(event.id) || !numeric(event.created)) return [];
    const data = event.data;
    if (!data || !identifier(data.sessionID) || !Number.isSafeInteger(event.durable?.seq)) return [];
    const sessionId = data.sessionID;
    let state = sessions.get(sessionId);
    if (!state) {
      state = { observedSeq: -1, steps: new Map(), tools: new Map() };
      sessions.set(sessionId, state);
    }
    // This is an observed-native watermark, not durable-local acceptance.
    // Pending projections retain their identities independently; a partial
    // acceptance is never reconstructed or replayed from a later native item.
    if (event.durable.seq <= state.observedSeq) return [];
    state.observedSeq = event.durable.seq;
    const endTimeMs = event.created;
    const make = (kind, eventId, startTimeMs, attributes, parentEventId) => ({
      kind, sessionId, eventId, startTimeMs, endTimeMs,
      ...(parentEventId ? { parentEventId } : {}),
      attributes: { ...baseAttributes, ...attributes },
      error: event.type.endsWith('.failed'),
    });
    if (event.type === 'session.created') {
      state.parentID = identifier(data.parentID) ? data.parentID : undefined;
      state.agent = label(data.agent) ? data.agent : undefined;
      return [];
    }
    if (event.type === 'session.execution.started') {
      state.execution = { id: `execution:${event.id}`, start: endTimeMs };
      return [];
    }
    if (['session.execution.succeeded', 'session.execution.failed', 'session.execution.interrupted'].includes(event.type)) {
      const execution = state.execution;
      state.execution = undefined;
      state.steps.clear();
      state.tools.clear();
      // Enabling mid-session cannot supply a historical start time. The point
      // event records the observed terminal transition without inventing time.
      const start = execution?.start ?? endTimeMs;
      const outcome = event.type.slice('session.execution.'.length);
      const attributes = {
        'coding_agent.session.outcome': outcome,
        'coding_agent.session.duration_ms': endTimeMs - start,
      };
      const spans = [make('session', execution?.id ?? `execution:${event.id}`, start, attributes)];
      if (state.parentID) {
        spans.push({
          ...make('subagent', `subagent:${event.id}`, start, {
            'coding_agent.agent.id': sessionId,
            'coding_agent.agent.parent_id': state.parentID,
            ...(state.agent ? { 'coding_agent.subagent.type': state.agent } : {}),
            'coding_agent.subagent.status': outcome,
            'coding_agent.subagent.duration_ms': endTimeMs - start,
          }),
          sessionId: state.parentID,
        });
      }
      return spans;
    }
    if (event.type === 'session.deleted') {
      // Retain only the sequence watermark, so replayed deletions/completions
      // remain idempotent without retaining the closed session's active state.
      state.steps.clear();
      state.tools.clear();
      state.execution = undefined;
      return [];
    }
    if (event.type === 'session.usage.recorded') {
      // Title/compaction charges are separate native deltas. Never export
      // session.usage.updated, which is a cumulative total of these and steps.
      return [make('llm.turn', `usage:${event.id}`, endTimeMs, {
        'gen_ai.operation.name': 'chat', ...usageAttributes(data),
      }, state.execution?.id)];
    }
    if (!identifier(data.assistantMessageID)) return [];
    const stepId = `step:${data.assistantMessageID}`;
    if (event.type === 'session.step.started') {
      // v2.0.12 supplies request dispatch time; event.created is the later
      // durable publication boundary and would omit provider wait latency.
      state.steps.set(stepId, {
        start: numeric(data.started) && data.started <= endTimeMs ? data.started : endTimeMs,
        attributes: modelAttributes(data),
      });
      return [];
    }
    if (event.type === 'session.step.ended' || event.type === 'session.step.failed') {
      const step = state.steps.get(stepId);
      state.steps.delete(stepId);
      return [make('llm.turn', stepId, step?.start ?? endTimeMs, {
        'gen_ai.operation.name': 'chat', ...step?.attributes, ...usageAttributes(data),
      }, state.execution?.id)];
    }
    if (!identifier(data.id)) return [];
    const toolId = `tool:${data.assistantMessageID}:${data.id}`;
    if (event.type === 'session.tool.input.started') {
      state.tools.set(toolId, { name: label(data.name) ? data.name : undefined });
      return [];
    }
    if (event.type === 'session.tool.called') {
      const tool = state.tools.get(toolId) ?? {};
      state.tools.set(toolId, { ...tool, start: endTimeMs });
      return [];
    }
    const tool = state.tools.get(toolId);
    state.tools.delete(toolId);
    return [make('tool.call', toolId, tool?.start ?? endTimeMs, {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.call.id': data.id,
      ...(tool?.name ? { 'gen_ai.tool.name': tool.name } : {}),
    }, stepId)];
  };
}

export default {
  id: pluginId,
  setup(ctx) {
    const telemetry = createTelemetry('opencode', {
      configPath: ctx.options?.configPath,
      stateRoot: ctx.options?.stateRoot,
      profile: ctx.options?.profile,
      nodeBinary: ctx.options?.nodeBinary,
    });
    if (telemetry.enabled === false) return async () => {};
    const controller = new AbortController();
    const context = Promise.race([
      repositoryContext(ctx.location.directory).catch(() => ({})),
      new Promise(resolve => controller.signal.addEventListener('abort', () => resolve({}), { once: true })),
    ]);
    const attributes = {};
    if (label(ctx.app?.name)) attributes['service.name'] = ctx.app.name;
    const mapEvent = createEventMapper(attributes);
    // Project synchronously before any await: only this sanitized batch, never
    // the native item's text, input, or provider state, survives local failure.
    const project = item => {
      if (item.done || controller.signal.aborted) return null;
      const event = item.value;
      // The host bus can include other locations; one location's plugin must
      // not export another location's activity a second time.
      if (event.location && (event.location.directory !== ctx.location.directory ||
          event.location.workspaceID !== ctx.location.workspaceID)) return [];
      return mapEvent(event);
    };
    // ponytail: 256 sanitized spans per plugin; refuse new projections at the
    // ceiling rather than retaining the host's unbounded raw pubsub backlog.
    const pending = [];
    const pendingLimit = 256;
    let overflowWarned = false;
    let subscriptionDone = false;
    let wakeHandoff;
    const handoff = (async () => {
      const repository = await context;
      let backoff = 100;
      try {
        while (pending.length || !controller.signal.aborted) {
          if (!pending.length) {
            if (subscriptionDone || controller.signal.aborted) break;
            await new Promise(resolve => { wakeHandoff = resolve; });
            wakeHandoff = undefined;
            continue;
          }
          const span = pending[0];
          span.attributes = { ...repository, ...span.attributes };
          let accepted = false;
          try { accepted = await telemetry.enqueue(span); } catch {}
          // Shutdown makes one final local attempt per pending span, with no
          // retry sleep and no wait for the detached sender's HTTP response.
          if (accepted === true || controller.signal.aborted) {
            pending.shift();
            backoff = 100;
          } else {
            try { await delay(backoff, undefined, { signal: controller.signal }); }
            catch (error) { if (!controller.signal.aborted) throw error; }
            backoff = Math.min(backoff * 2, 60000);
          }
        }
      } catch {
        if (!controller.signal.aborted) console.error('[datalake-otel] opencode2 local handoff stopped');
      }
    })();
    controller.signal.addEventListener('abort', () => wakeHandoff?.(), { once: true });
    const consume = (async () => {
      let iterator;
      try {
        iterator = ctx.event.subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
        while (!controller.signal.aborted) {
          const batch = project(await iterator.next());
          if (batch === null) break;
          if (!batch.length) continue;
          if (pending.length + batch.length > pendingLimit) {
            if (!overflowWarned) {
              overflowWarned = true;
              console.error('[datalake-otel] opencode2 local metadata buffer full; new telemetry not accepted');
            }
            continue;
          }
          pending.push(...batch);
          wakeHandoff?.();
        }
      } catch {
        if (!controller.signal.aborted) console.error('[datalake-otel] opencode2 event subscription stopped');
      } finally {
        subscriptionDone = true;
        wakeHandoff?.();
        await iterator?.return?.();
      }
    })();
    return async () => {
      controller.abort();
      try { await Promise.all([consume, handoff]); } finally { await telemetry.flushLocal(); }
    };
  },
};
