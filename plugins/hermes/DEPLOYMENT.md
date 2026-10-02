# Authorized production verification

Verified 2026-10-02 against Hermes `f42f579cf8bac4918ac9599bece71618afadd846`; native compatibility suite: **22 passed, no skips**. This record supersedes the earlier paused-export checkpoint in VERIFICATION.md. No GitHub push, Hermes core change, other-profile change, gateway restart, or manual aggregate run occurred.

## Running configuration and transport

Default-profile exporter: `capture_auxiliary=false`, `export_paused=false`, capacity `100000`, timeout `2` seconds. Credentials stayed in the protected profile-local header file; no secrets entered repository artifacts.

The user restarted the independent password-backed SSH LaunchAgent outside the supervised gateway. Authenticated GET returned **405** before export resumed and again after both normal aggregation waits. The same launchd-managed SSH process remained running with a literal IPv4 loopback-only listener. A listener alone was not counted as upstream proof. See deployment/README.md for the remote-interface binding constraint.

Resumption used native `hermes config set plugins.entries.datalake-usage.settings.export_paused false`, then supported `gateway.control_socket.reload_gateway_plugins()`. Response: `reloaded=true`, only `post_api_request` activated for datalake-usage, two adapters rewired. The existing gateway PID and start time remained unchanged. Readback confirmed settings and delivered outbox tombstones; candidate and installed plugin files matched byte-for-byte.

## Real event and replay result

The previously pending real minimal CLI completion was delivered **automatically** by the resumed worker before any manual replay. Raw storage retained client/service `hermes`, the hashed session, exact trace/span IDs, original timestamp, and small input/output counts with zero cache-read/reasoning. Exact counts, identifiers, and the immutable body hash are kept in private deployment evidence, not in this repository.

After one normal configured aggregation interval, direct Greptime SQL and Grafana's actual `greptime-mysql` datasource both returned the same closed session before and after one authorized manual replay:

| Field | Before replay | After replay and next normal cycle |
|---|---:|---:|
| Raw rows for exact trace/span | 1 | 2 |
| Distinct trace/span identities | 1 | 1 |
| Session input/output/total tokens | unchanged | unchanged |
| Session LLM spans | 1 | 1 |
| Reported cost USD | NULL | NULL |
| Estimated cost USD | NULL | NULL |
| Unpriced calls | 1 | 1 |

One authorized manual POST replayed the **identical actual body**, not a synthetic fixture. Collector returned HTTP **200**, `partialSuccess: {}`. Timestamp and IDs stayed identical; raw rows increased but the exact-session accounting did not. Both before/after datasource queries returned status 200 with identical tokens and null costs. Exact token counts are kept in private deployment evidence, not in this repository.

Whole-client daily totals changed during verification because unrelated real production continued (both span and token totals grew; both cost columns stayed null). These totals are time-bound snapshots, not the closed-session replay invariant. Do not compare global daily totals as though export were idle.

## Reproducible readback shape

Use actual exporter IDs privately; do not publish session identifiers or response content.

```sql
SELECT COUNT(*) AS raw_rows, COUNT(DISTINCT span_id) AS distinct_spans
FROM opentelemetry_traces
WHERE trace_id = '<actual-trace>' AND span_id = '<actual-span>';

SELECT session_start,session_id,client,provider,models,
       input_tokens,output_tokens,total_tokens,llm_spans,
       cost_usd,cost_estimated_usd,cost_unpriced_calls
FROM ai_session_summary
WHERE client = 'hermes' AND session_id = '<actual-session-hash>';

SELECT day_start,client,provider,model,input_tokens,output_tokens,
       total_tokens,llm_spans,cost_usd,cost_estimated_usd
FROM ai_daily_summary WHERE client = 'hermes' ORDER BY day_start DESC;
```

Private local evidence contains exact SQL, raw results, real immutable body, collector acknowledgment, before/after Grafana responses and identity keys. It is intentionally not copied into this repository.

## Limits and observed discrepancy

- This proves one actual OpenAI Codex model path, durable pending delivery, normal aggregation and downstream trace/span replay accounting. It does not prove every provider, auxiliary call, failure, stream, invoice, or MoA advisor.
- Main-only capture remains deliberate: auxiliary opt-in can double count the known nonstreaming main MoA completion.
- `gen_ai.response.id` read back **NULL** in the live raw table: this exporter emits its hashed identity under `request_id`, not that attribute. Do not claim logical response identity retention. Exact trace/span dedup was independently verified; no collector or projector configuration was changed.
- Grafana **datasource** results are verified. No browser screenshot or claim of a rendered Hermes-filtered dashboard is made; existing dashboard filtering remains a separate task.
- The exporter has a bounded lifetime identity ledger including tombstones. Monitor capacity; do not prune it silently. A vault-session expiration can prevent tunnel reconnection and requires the existing user unlock workflow.
