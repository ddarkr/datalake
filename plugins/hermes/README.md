# Native Hermes metadata-only usage exporter

`__init__.py` + `plugin.yaml` form a standalone native Hermes plugin. Python standard library only; no core edits, SDK, asyncio event loop, tool registration, or message rewriting. **Nothing in this repository automatically installs or enables it.**

## Contract and privacy

Register `pre_api_request`, `api_request_error`, and `post_api_request` through `register(ctx)` by default; `post_auxiliary_call` requires explicit `capture_auxiliary: true`. Callbacks accept `**kwargs`. The researched compatibility baseline is [Hermes f42f579cf8bac4918ac9599bece71618afadd846](https://github.com/NousResearch/hermes-agent/tree/f42f579cf8bac4918ac9599bece71618afadd846), not proof of installation in a live profile.

Sources checked:

- [Official observer contract](https://hermes-agent.nousresearch.com/docs/developer-guide/observer-hooks)
- [Official native plugins](https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins)
- `agent/turn_response_intake.py`, `agent/api_request_hooks.py`, `agent/auxiliary_hooks.py`, `agent/usage_pricing.py`, `hermes_cli/plugins*.py` in that revision.

The hook's "sanitized" response can still contain assistant text. This exporter **never reads it**. A new object is constructed from an explicit allowlist:

- canonical usage: `prompt_tokens`, `output_tokens`, cache read/write, reasoning; `input_tokens` and `total_tokens` are used only for consistency validation;
- request and session identity, auxiliary retry count;
- provider, requested model, optional response model;
- start/end times and optional first-chunk time; allowlisted requested service tier, reasoning effort, and explicit integer thinking budget from `pre_api_request`.

No messages, prompts, response objects, tools, complete request bodies, headers, raw base URLs, platform/sender metadata, cost snapshots, MoA references, state.db contents, filesystem paths, or exception text are exported or spooled. Only safe request-setting scalars are retained by request ID until completion/error; pending state is bounded. Model/provider labels have a restrictive 128-character ASCII identifier grammar; invalid labels, malformed identities and invalid intervals are discarded. Malformed usage is omitted without discarding valid latency. Arbitrary text placed by another plugin into an otherwise valid model identifier cannot be distinguished from a real identifier: this is metadata allowlisting, not a content-classification engine.

Session IDs are SHA-256 of `[profile_scope, session_id]`, where profile scope hashes the captured resolved Hermes home. Request IDs are hashed too. `traceId` and `spanId` use the same compact JSON/SHA-256 truncation contract as `serializeEvent()` in `plugins/otel.mjs`, with client `hermes`. The event identity is compact JSON `[hook_name, opaque_api_request_id, retry_count_or_zero]`. Do not parse Hermes IDs. Do not add a random process ID: it would defeat multiprocess replay deduplication. Moving a profile directory changes scope.

Spans are `coding_agent.llm.turn`, client `hermes`, service `hermes`, scope `doda-datalake/1.0.0`, `metadata_only`, `datalake-plugin`, successful status. Main and auxiliary hook events are **not necessarily independent provider requests**. There are no additive session snapshots.

### MoA overlap and conservative coverage

On the pinned Hermes revision, `agent/moa_loop.py:1166–1173,1195–1203` returns a nonstreaming `call_llm(task="moa_aggregator")` completion to the main loop. The auxiliary boundary emits `post_auxiliary_call`; `agent/turn_response_intake.py:69–97` emits the same completion again as `post_api_request`, with a different request ID. Hook/ID-based deduplication cannot pair these events. Main `moa_references` is advisor metadata, not a shared physical request identifier.

`agent/auxiliary_hooks.py:101–114,145–177` exposes `aux_task`, parent-turn identity, retry count and its own request ID, but no actor/call-site discriminator or main request ID. Standalone guidance synthesis also uses `task="moa_aggregator"` (`agent/moa_loop.py:901–904`). Dropping that task would silently lose genuine standalone calls; matching timing/model/counts or parsing opaque IDs is not reliable correlation.

**`capture_auxiliary` defaults to false**: only main completions bill, preventing this overlap without core edits. This is partial coverage, not complete MoA accounting: advisors, standalone synthesizers, compression and other auxiliary spend are omitted. No task blacklist is used. Boolean `capture_auxiliary: true` restores auxiliary capture, including standalone synthesis, but reintroduces the known main-MoA double count; use only when the overlapping path is absent or an upstream physical-request correlation contract is added.


### Token and cost semantics

Hermes canonical `input_tokens` excludes cache. OTLP `gen_ai.usage.input_tokens` is **`prompt_tokens` (uncached + cache read + cache write)**, matching the repository's canonical OTel convention. Output already includes reasoning; reasoning is a detail, never added again. Optional absent/null counts stay absent; explicit zero stays zero. Cost attributes are absent (downstream must retain SQL null), never inferred as zero and never priced by this plugin.

Upstream `CanonicalUsage` initializes missing buckets to zero before observer dispatch. The exporter cannot recover unknown-versus-zero information already erased by Hermes. It preserves distinctions that still exist in the hook dictionary, and never estimates tokens from character lengths.

### Performance observations

`coding_agent.request.duration_ms` is `(ended_at - started_at) * 1000`, kind `logical_call`: it includes retries, not just generation. `ttft_ms` is `(first_chunk_at - started_at) * 1000`, kind `first_stream_event`: the first SDK/SSE event may be metadata or reasoning, **not first text or UI paint**. Missing/`None` usage still permits latency-only spans; invalid explicit usage never becomes zero tokens. Valid usage is tagged `coding_agent.usage.source=reported`.

`pre_api_request` settings have `request.settings_source=wire`; requested tier is not an echoed response tier. No actual tier, first-text or input-to-response latency is fabricated. Some provider and auxiliary paths do not expose these observations; auxiliary events do not inherit a guessed main-request setting. Effort, explicit budget and model variant are distinct; none is derived from another.

Request-total output throughput means reported output tokens divided by positive logical-call seconds on the **same record**, not generation TPS. Compare only matching workloads, cache conditions, settings and timing provenance; absent values remain unknown rather than zero. See the [six-harness field matrix](../README.md#성능-필드와-해석).

Updating repository files does not update a copied plugin: recopy into the approved profile and restart Hermes to load changes. Collector projection, nullable schema and Grafana changes require separately authorized server deployment; neither profile installation nor server deployment is performed by this documentation update.

## Transport and lifecycle

- Hooks do a direct bounded HTTP POST per event: no redirects or ambient proxies, 64 KiB reply cap, whole-operation socket deadline 0.1–5 seconds (DNS may exceed it on some systems). Response/error bodies never enter diagnostics.
- Collector full ACK is 2xx with `partialSuccess.rejectedSpans == 0` (integer zero or all-zero string). Anything else — HTTP error, timeout, malformed/oversized reply, or any rejection — returns false and the event is dropped without retry.
- No background thread, no unload hook, no cross-process coordination: synchronous hook sends complete before the callback returns, so there is nothing to drain or release. Forced process termination loses only in-flight unsent events.
- Stable dedup is in-memory only: repeated hook deliveries of the same identity within the process are suppressed, never persisted, never replayed. Existing `$HERMES_HOME/usage-otel/outbox.sqlite3` files from queue-era installs are never opened, replayed, migrated, or deleted by this version.
- Delivery is **at most once per process per identity**. Two processes firing the same hook each send once; downstream dedup on trace/span IDs is not proven by this plugin's tests.

## Installation (only after separate approval)

These are instructions, not an automated installer. Choose the intended profile first and confirm its resolved `$HERMES_HOME`. Do not copy into another profile, or edit Hermes core.

```sh
# Run from the datalake repository after deployment approval.
# HERMES_HOME must point to the intended profile.
mkdir -p "$HERMES_HOME/plugins/datalake-usage"
cp plugins/hermes/__init__.py plugins/hermes/plugin.yaml "$HERMES_HOME/plugins/datalake-usage/"
hermes config set plugins.entries.datalake-usage.settings.endpoint 'https://collector.example/v1/traces'
hermes config set plugins.entries.datalake-usage.settings.timeout_seconds 2
hermes config set plugins.entries.datalake-usage.settings.capture_auxiliary false
```

Secrets **do not belong in config.yaml**. Supply either:

1. `$HERMES_HOME/usage-otel.credentials.json`: owner-only regular file, 0600, max 64 KiB, containing a JSON object of header-name/string-value pairs (e.g. Authorization plus collector-specific auth headers), written through a trusted secret-management workflow; or
2. `DATALAKE_HERMES_OTLP_HEADERS`: the same JSON object supplied securely to the actual CLI/gateway process environment. Environment takes precedence over the file.

Cookie, host, content-length/type, connection, transfer-encoding and proxy-authorization overrides are rejected, as are control characters. Settings are read using native `ctx.get_config`; profile home is captured using `get_hermes_home()` during registration, not resolved from a worker's ambient environment. The gateway must actually inherit environment-based credentials; interactive shell exports alone may not reach a service. Credential files avoid that inheritance dependency. Credential/config changes require plugin reload or CLI/gateway restart.

```sh
# Only after privacy, endpoint/authentication and downstream dedup review.
hermes plugins enable datalake-usage
# For a running compatible gateway, use the supported control socket reload:
HERMES_HOME="$HERMES_HOME" /path/to/hermes-agent/venv/bin/python -c 'from pathlib import Path; from hermes_constants import get_hermes_home; from gateway.control_socket import reload_gateway_plugins; print(reload_gateway_plugins(Path(get_hermes_home())))'
# Check reloaded=true and the datalake-usage activation hooks in the response.
```

A missing endpoint registers no observers. Invalid configuration disables the exporter with a generic warning. HTTPS is required except literal loopback HTTP; private-LAN plaintext HTTP is intentionally not supported. URLs may not contain credentials, query or fragment. Queue-era `capacity`/`export_paused` keys are ignored; existing profile outbox databases are left untouched (never opened, replayed, or deleted).

### Rollback

```sh
hermes plugins disable datalake-usage
# The native disable surface nudges the running gateway.
# Verify datalake-usage no longer appears in gateway activation summaries.
```


## Tests

From repository root, Python 3.9+ and Node 22+:

```sh
python3 -B -m unittest discover -s plugins/hermes/tests -v
# Core/HTTP suite; three optional native tests skip without HERMES_SOURCE.

HERMES_SOURCE=/path/to/verified/hermes-agent \
  /path/to/hermes-agent/venv/bin/python -B -m unittest discover -s plugins/hermes/tests -v
```

Native tests use a temporary candidate package and isolated scratch Hermes home, the real manifest scanner/PluginManager/observer dispatcher/unloader and actual installed main/aux usage normalizers. Only fixture metadata is posted to an ephemeral loopback HTTP server, and direct sends complete inside the hook with no worker drain. No provider call, production collector, live profile, installed plugin directory, gateway service, credentials or Wiki is touched.

Tests cover malicious/poisoned hook fields, no raw identities/content, unknown versus zero, consistency validation, JavaScript canonical IDs, direct success/failure/partial/malformed/oversized/trickling sends with no retry, in-memory duplicate suppression with no created database files, endpoint/credential validation and native profile binding. The shared ID/timestamp oracle calls only `serializeEvent('hermes', event).body` in Node; it does not create a Node exporter, enqueue records or mock a network fetch. Provider fixtures separately cover OpenAI Chat, Codex Responses and Anthropic canonical normalizers; these are **adapter-shape tests, not live vendor accounting validation**.

For existing repository CI, add `python -B -m unittest discover -s plugins/hermes/tests -v` alongside the coding-agent plugin privacy tests after Python and Node setup. Native compatibility requires an explicitly supplied, pinned Hermes checkout and its environment; the default CI should not download or activate Hermes implicitly. A missing `HERMES_SOURCE` skips the three native tests; passing core/HTTP and serializer-oracle checks does not satisfy that gate. The current local performance work passed 21 core tests and all three native loader/normalizer/MoA tests against pinned `f42f579c` in an isolated Python 3.14 environment. This is not live-provider or production validation. Node syntax checks do not substitute for an LSP check (none was configured).

## Coverage limits

- Auxiliary capture is disabled by default to avoid nonstreaming main-MoA overlap. Opting in can double count that completion; standalone auxiliary calls remain captured when opted in.
- Streamed auxiliary hooks fire before the stream is consumed and carry no final usage. They are omitted, even if a future payload happens to contain counts while `streaming=True`.
- Auxiliary failures, invalid identity/model/provider/timing, and turnless auxiliary calls with empty session/request IDs are omitted rather than fabricated or attributed to a synthetic session. Missing usage on an otherwise valid observation does not discard its latency.
- Optional providers/runtimes that bypass these hooks and provider adapters that discard details remain outside coverage; usage fields cannot be recovered from latency-only observations.
- No retrospective reconciliation, invoices, pricing, session hierarchy, or tool/edit/git spans. A separately authorized production deployment verified one real Codex session end to end and immutable replay accounting; see [DEPLOYMENT.md](DEPLOYMENT.md). This does not establish all-provider coverage or rendered dashboard filtering.
- Verified against the installed revision above; schema/hook changes require rerunning native tests. Unix/macOS/Linux permissions are assumed; Windows is not supported by this version.
