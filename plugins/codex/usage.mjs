import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { mkdir, open, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { withStateLock } from '../otel.mjs';

// rust-v0.142.4 protocol.rs: RolloutLine -> EventMsg::TokenCount -> TokenUsageInfo.
// total_token_usage is cumulative; last_token_usage is re-emitted on rate-limit updates.
// Export only actual increments matching last_token_usage, not repeated snapshots or estimates.
const FIELDS = ['input_tokens', 'cached_input_tokens', 'output_tokens', 'reasoning_output_tokens', 'total_tokens'];
const ATTRIBUTES = ['gen_ai.usage.input_tokens', 'gen_ai.usage.cache_read.input_tokens', 'gen_ai.usage.output_tokens', 'gen_ai.usage.reasoning.output_tokens'];
const hash = value => createHash('sha256').update(value).digest('hex');
const safeLabel = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,159}$/.test(value) ? value : undefined;
const safeId = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$/.test(value) ? value : undefined;
const counts = value => value && FIELDS.every(key => Number.isSafeInteger(value[key]) && value[key] >= 0) ? FIELDS.map(key => value[key]) : undefined;

async function load(path) {
  try { return JSON.parse(await readFile(path, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
}

async function save(path, value) {
  const temporary = `${path}.${randomUUID()}`;
  await writeFile(temporary, JSON.stringify(value), { mode: 0o600, flag: 'wx' });
  try { await rename(temporary, path); }
  finally { await rm(temporary, { force: true }); }
}

// Never scans directories or discovers historical files: only the native hook's one active path.
export async function collectUsage(input, { telemetry, stateDir, prime = false } = {}) {
  const child = input.hook_event_name === 'SubagentStop';
  const transcript = child ? input.agent_transcript_path : input.transcript_path;
  if (typeof transcript !== 'string' || !isAbsolute(transcript) || !transcript.endsWith('.jsonl')) return [];
  const expectedId = safeId(input.agent_id) ?? safeId(input.session_id);
  const activeTurn = safeId(input.turn_id);
  if (!expectedId || (!prime && !activeTurn)) return [];
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  const cursorPath = join(stateDir, `usage-${hash(`${expectedId}\0${transcript}`)}.json`);
  const lockDirectory = `${cursorPath}.lock`;
  await mkdir(lockDirectory, { recursive: true, mode: 0o700 });
  return await withStateLock(lockDirectory, async () => {
    let file;
    try {
      try { file = await open(transcript, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0)); }
      catch (error) { if (error.code === 'ENOENT' || error.code === 'ELOOP') return []; throw error; }
      const info = await file.stat();
      if (!info.isFile()) return [];
      let cursor = await load(cursorPath);
      if (!cursor || cursor.ino !== info.ino || cursor.dev !== info.dev || cursor.offset > info.size) {
        cursor = { offset: 0, ino: info.ino, dev: info.dev, verified: false, total: [0, 0, 0, 0, 0] };
      }
      const events = [];
      const buffer = Buffer.allocUnsafe(64 * 1024);
      let position = cursor.offset;
      let lineStart = position;
      let parts = [];
      let length = 0;
      let oversized = false;
      // ponytail: bound one hook to 64 MiB; a later hook resumes at the last complete line.
      const end = Math.min(info.size, position + 64 * 1024 * 1024);
      while (position < end) {
        const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, end - position), position);
        if (!bytesRead) break;
        let start = 0;
        for (let index = 0; index < bytesRead; index++) {
          if (buffer[index] !== 10) continue;
          const fragment = buffer.subarray(start, index);
          length += fragment.length;
          if (!oversized && length <= 8 * 1024 * 1024) {
            parts.push(Buffer.from(fragment));
            const line = Buffer.concat(parts).toString('utf8');
            if (/"type"\s*:\s*"(?:session_meta|turn_context|token_count|task_started|turn_started)"/.test(line)) {
              let record;
              try { record = JSON.parse(line); } catch { record = undefined; }
              const payload = record?.payload;
              if (record?.type === 'session_meta') {
                cursor.threadId = safeId(payload?.id);
                cursor.verified = Boolean(cursor.threadId) && (input.agent_id ? payload.id === expectedId : payload.id === expectedId || payload.session_id === expectedId);
                cursor.provider = safeLabel(payload?.model_provider);
              } else if (record?.type === 'turn_context') {
                cursor.turn = safeId(payload?.turn_id);
                cursor.model = safeLabel(payload?.model);
              } else if (record?.type === 'event_msg' && ['task_started', 'turn_started'].includes(payload?.type)) {
                cursor.turn = safeId(payload.turn_id);
              } else if (record?.type === 'event_msg' && payload?.type === 'token_count') {
                const total = counts(payload.info?.total_token_usage);
                const last = counts(payload.info?.last_token_usage);
                if (total) {
                  const delta = total.map((value, i) => value - cursor.total[i]);
                  const timestamp = Date.parse(record.timestamp);
                  if (!prime && cursor.verified && cursor.turn === activeTurn && last && Number.isFinite(timestamp) && delta.some(value => value > 0) && delta.every((value, i) => value >= 0 && value === last[i])) {
                    const attributes = {};
                    ATTRIBUTES.forEach((key, i) => { attributes[key] = delta[i]; });
                    const model = cursor.model ?? safeLabel(input.model);
                    if (model) attributes['gen_ai.request.model'] = model;
                    if (cursor.provider) attributes['gen_ai.provider.name'] = cursor.provider;
                    if (safeId(input.agent_id)) attributes['coding_agent.agent.id'] = input.agent_id;
                    const event = {
                      kind: 'llm.turn', sessionId: input.session_id,
                      eventId: `usage:${cursor.threadId}:${cursor.turn}:${total.join(':')}`,
                      startTimeMs: timestamp, endTimeMs: timestamp, attributes,
                    };
                    let accepted = false;
                    try { accepted = await telemetry.enqueue(event); } catch {}
                    if (accepted !== true) {
                      // Do not advance the numeric baseline or byte offset before durable local acceptance.
                      await save(cursorPath, cursor);
                      return events;
                    }
                    events.push(event);
                  }
                  cursor.total = total;
                }
              }
            }
          }
          cursor.offset = position + index + 1;
          lineStart = cursor.offset;
          parts = [];
          length = 0;
          oversized = false;
          start = index + 1;
        }
        if (start < bytesRead) {
          const fragment = buffer.subarray(start, bytesRead);
          length += fragment.length;
          if (!oversized && length <= 8 * 1024 * 1024) parts.push(Buffer.from(fragment));
          else { oversized = true; parts = []; }
        }
        position += bytesRead;
      }
      // Partial lines stay solely in the original file, never in plugin state.
      cursor.offset = lineStart;
      await save(cursorPath, cursor);
      return events;
    } finally {
      await file?.close();
    }
  }) ?? [];
}
