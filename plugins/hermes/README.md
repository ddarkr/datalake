# Native Hermes metadata-only usage exporter

`__init__.py` + `plugin.yaml` form a standalone native Hermes plugin. Python standard library only; no core edits, SDK, asyncio event loop, tool registration, or message rewriting. **Nothing in this repository automatically installs or enables it.**

## Contract and privacy

Register `post_api_request` through `register(ctx)` by default; `post_auxiliary_call` requires explicit `capture_auxiliary: true`. Both callbacks accept `**kwargs`. The installed compatibility baseline is Hermes `f42f579cf8bac4918ac9599bece71618afadd846`.

Sources checked:

- [Official observer contract](https://hermes-agent.nousresearch.com/docs/developer-guide/observer-hooks)
- [Official native plugins](https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins)
- `agent/turn_response_intake.py`, `agent/api_request_hooks.py`, `agent/auxiliary_hooks.py`, `agent/usage_pricing.py`, `hermes_cli/plugins*.py` in that revision.

The hook's "sanitized" response can still contain assistant text. This exporter **never reads it**. A new object is constructed from an explicit allowlist:

- canonical usage: `prompt_tokens`, `output_tokens`, cache read/write, reasoning; `input_tokens` and `total_tokens` are used only for consistency validation;
- request and session identity, auxiliary retry count;
- provider, requested model, optional response model;
- start/end times and optional first-chunk time.

No messages, prompts, response objects, tools, request bodies, headers, raw base URLs, platform/sender metadata, cost snapshots, MoA references, state.db contents, filesystem paths, or exception text are exported or spooled. Model/provider labels have a restrictive 128-character ASCII identifier grammar; invalid labels, malformed identities, negative/fractional/bool counts, inconsistent totals, and invalid intervals are discarded. Arbitrary text placed by another plugin into an otherwise valid model identifier cannot be distinguished from a real identifier: this is metadata allowlisting, not a content-classification engine.

Session IDs are SHA-256 of `[profile_scope, session_id]`, where profile scope hashes the captured resolved Hermes home. Request IDs are hashed too. `traceId` and `spanId` use the same compact JSON/SHA-256 truncation contract as `serializeEvent()` in `plugins/otel.mjs`, with client `hermes`. The event identity is compact JSON `[hook_name, opaque_api_request_id, retry_count_or_zero]`. Do not parse Hermes IDs. Do not add a random process ID: it would defeat multiprocess replay deduplication. Moving a profile directory changes scope.

Spans are `coding_agent.llm.turn`, client `hermes`, service `hermes`, scope `doda-datalake/1.0.0`, `metadata_only`, `datalake-plugin`, successful status. Main and auxiliary hook events are **not necessarily independent provider requests**. There are no additive session snapshots.

### MoA overlap and conservative coverage

On the pinned Hermes revision, `agent/moa_loop.py:1166–1173,1195–1203` returns a nonstreaming `call_llm(task="moa_aggregator")` completion to the main loop. The auxiliary boundary emits `post_auxiliary_call`; `agent/turn_response_intake.py:69–97` emits the same completion again as `post_api_request`, with a different request ID. Hook/ID-based deduplication cannot pair these events. Main `moa_references` is advisor metadata, not a shared physical request identifier.

`agent/auxiliary_hooks.py:101–114,145–177` exposes `aux_task`, parent-turn identity, retry count and its own request ID, but no actor/call-site discriminator or main request ID. Standalone guidance synthesis also uses `task="moa_aggregator"` (`agent/moa_loop.py:901–904`). Dropping that task would silently lose genuine standalone calls; matching timing/model/counts or parsing opaque IDs is not reliable correlation.

**`capture_auxiliary` defaults to false**: only main completions bill, preventing this overlap without core edits. This is partial coverage, not complete MoA accounting: advisors, standalone synthesizers, compression and other auxiliary spend are omitted. No task blacklist is used. Boolean `capture_auxiliary: true` restores auxiliary capture, including standalone synthesis, but reintroduces the known main-MoA double count; use only when the overlapping path is absent or an upstream physical-request correlation contract is added.

Boolean `export_paused` defaults to false. Set it true before a safe hot reload when production sends are prohibited: hooks continue spooling locally but the new worker neither claims nor sends events. Existing pending bodies and endpoint/credentials are preserved. Changing this setting requires another reload; it does not cancel requests in an already-running old worker.

### Token and cost semantics

Hermes canonical `input_tokens` excludes cache. OTLP `gen_ai.usage.input_tokens` is **`prompt_tokens` (uncached + cache read + cache write)**, matching the repository's canonical OTel convention. Output already includes reasoning; reasoning is a detail, never added again. Optional absent/null counts stay absent; explicit zero stays zero. Cost attributes are absent (downstream must retain SQL null), never inferred as zero and never priced by this plugin.

Upstream `CanonicalUsage` initializes missing buckets to zero before observer dispatch. The exporter cannot recover unknown-versus-zero information already erased by Hermes. It preserves distinctions that still exist in the hook dictionary, and never estimates tokens from character lengths.

## Outbox and lifecycle

- `$HERMES_HOME/usage-otel/outbox.sqlite3`, private owner-only directory/file (0700/0600); refuse symlink directories, symlink files, shared hardlinks, unsafe ownership/permissions.
- SQLite `synchronous=FULL`, per-operation connections, 50 ms lock wait, transactional insert/lease/ack. Hooks do a local durability write, **never network I/O**. Filesystem latency is not hard-real-time bounded. An event that cannot be persisted because of contention, disk failure or capacity is not recoverable from this plugin; diagnostics are sanitized and the agent continues.
- Persist the complete immutable OTLP body before transport; duplicate IDs never replace it. Delivered rows become compact tombstones (`body=NULL`) and still deduplicate later callbacks.
- One daemon worker per plugin instance, one request at a time, at most four request starts/second per process. Multiprocess instances share 30-second leases; stale claim holders cannot acknowledge another lease.
- HTTP failures, malformed/oversized responses and OTLP `partialSuccess.rejectedSpans != 0` keep the event. Exponential retry delay 1–60 seconds. Response/error bodies never enter diagnostics. No redirects or ambient proxies. Socket timeout 0.1–5 seconds; DNS resolution may exceed socket timeout on some systems.
- Unload/normal interpreter exit gives the existing worker 250 ms to drain, then prevents new work. A running HTTP request may finish later in its daemon thread; forced process termination leaves a lease that expires after 30 seconds. Short flush is best effort, not a delivery guarantee.
- Capacity defaults to **100,000 ledger identities including tombstones**, configurable up to 1,000,000. At capacity, new events are rejected with a sanitized warning: there is no silent eviction. Monitor capacity and increase deliberately. No automatic tombstone pruning/rotation is provided; deleting the ledger loses dedup history.

Delivery is **at least once**, not exactly once: collector acknowledgment can be lost, or a process can die after HTTP acceptance before SQLite acknowledgment. Replays retain exactly the same IDs/timestamps/body. The collector/database must deduplicate these IDs; that downstream behavior is not proven by this plugin's tests. Permanent 4xx responses are retained for retry, so correcting collector configuration is necessary to resume progress. Changing endpoint routes existing pending bodies to the new endpoint.

Both Hermes and the Node plugins separate local durable handoff from network work; local persistence is not collector acknowledgment. Their runtimes and storage are independent: Hermes keeps this SQLite ledger and an **in-process daemon thread**, not the Node file outbox or detached sender subprocess. The Node sender's ability to survive its producer's exit does not apply to Hermes, nor does Node endpoint pinning change Hermes's existing endpoint behavior above. Hermes deployment, authentication and access requirements are unchanged.

## Installation (only after separate approval)

These are instructions, not an automated installer. Choose the intended profile first and confirm its resolved `$HERMES_HOME`. Do not copy into another profile, or edit Hermes core.

```sh
# Run from the datalake repository after deployment approval.
# HERMES_HOME must point to the intended profile.
mkdir -p "$HERMES_HOME/plugins/datalake-usage"
cp plugins/hermes/__init__.py plugins/hermes/plugin.yaml "$HERMES_HOME/plugins/datalake-usage/"
hermes config set plugins.entries.datalake-usage.settings.endpoint 'https://collector.example/v1/traces'
hermes config set plugins.entries.datalake-usage.settings.timeout_seconds 2
hermes config set plugins.entries.datalake-usage.settings.capacity 100000
hermes config set plugins.entries.datalake-usage.settings.capture_auxiliary false
# Pause until transport/auth and deployment authorization are verified.
hermes config set plugins.entries.datalake-usage.settings.export_paused true
```

Secrets **do not belong in config.yaml**. Supply either:

1. `$HERMES_HOME/usage-otel.credentials.json`: owner-only regular file, 0600, max 64 KiB, containing a JSON object of header-name/string-value pairs (e.g. Authorization plus collector-specific auth headers), written through a trusted secret-management workflow; or
2. `DATALAKE_HERMES_OTLP_HEADERS`: the same JSON object supplied securely to the actual CLI/gateway process environment. Environment takes precedence over the file.

Cookie, host, content-length/type, connection, transfer-encoding and proxy-authorization overrides are rejected, as are control characters. Settings are read using native `ctx.get_config`; profile home is captured using `get_hermes_home()` during registration, not resolved from a worker's ambient environment. The gateway must actually inherit environment-based credentials; interactive shell exports alone may not reach a service. Credential files avoid that inheritance dependency. Credential/config changes require plugin reload or CLI/gateway restart.

```sh
# Only after privacy, endpoint/authentication and downstream dedup review.
hermes plugins enable datalake-usage
# For a running compatible gateway, use the supported control socket reload:
# Resume only after authenticated transport verification and authorization.
hermes config set plugins.entries.datalake-usage.settings.export_paused false
HERMES_HOME="$HERMES_HOME" /path/to/hermes-agent/venv/bin/python -c 'from pathlib import Path; from hermes_constants import get_hermes_home; from gateway.control_socket import reload_gateway_plugins; print(reload_gateway_plugins(Path(get_hermes_home())))'
# Check reloaded=true and the datalake-usage activation hooks in the response.
```

A missing endpoint registers no observers or worker. Invalid configuration disables the exporter with a generic warning. HTTPS is required except literal loopback HTTP; private-LAN plaintext HTTP is intentionally not supported. URLs may not contain credentials, query or fragment.

### Rollback

```sh
hermes plugins disable datalake-usage
# The native disable surface nudges the running gateway.
# Verify datalake-usage no longer appears in gateway activation summaries.
```

Keep the private outbox for later replay. To remove the plugin, first stop its owning processes, then remove only the two installed plugin files/directory and optionally its profile-local credentials using the normal approved file-removal workflow. Do not delete state.db or other profile data. Deleting the usage outbox is a deliberate loss of pending telemetry and dedup history.

## Tests

From repository root, Python 3.9+ and Node 22+:

```sh
python3 -B -m unittest discover -s plugins/hermes/tests -v
# Core/HTTP/multiprocess suite; three optional native tests skip without HERMES_SOURCE.

HERMES_SOURCE=/path/to/verified/hermes-agent \
  /path/to/hermes-agent/venv/bin/python -B -m unittest discover -s plugins/hermes/tests -v
```

Native tests use a temporary candidate package and isolated scratch Hermes home, the real manifest scanner/PluginManager/observer dispatcher/unloader and actual installed main/aux usage normalizers. Only fixture metadata is posted to an ephemeral loopback HTTP server. No provider call, production collector, live profile, installed plugin directory, gateway service, credentials or Wiki is touched.

Tests cover malicious/poisoned hook fields, no raw identities/content, unknown versus zero, consistency validation, JavaScript canonical IDs, durable immutable retry, partial successes, independent processes racing on one ledger, stale leases, private permissions, bounded socket behavior, native profile binding and short unload. The shared ID/timestamp oracle calls only `serializeEvent('hermes', event).body` in Node; it does not create a Node exporter, enqueue records or mock a network fetch. Provider fixtures separately cover OpenAI Chat, Codex Responses and Anthropic canonical normalizers; these are **adapter-shape tests, not live vendor accounting validation**.

For existing repository CI, add `python -B -m unittest discover -s plugins/hermes/tests -v` alongside the coding-agent plugin privacy tests after Python and Node setup. Native compatibility requires an explicitly supplied, pinned Hermes checkout and its environment; the default CI should not download or activate Hermes implicitly. A missing `HERMES_SOURCE` skips the three native tests; passing core/HTTP/multiprocess and serializer-oracle checks does not satisfy that gate. In the current shared Node verification, those Hermes native gates were skipped because the verified external checkout was unavailable; Hermes runtime was unchanged. Node syntax checks also do not substitute for an LSP check (none was configured) or native Hermes compatibility.

## Coverage limits

- Auxiliary capture is disabled by default to avoid nonstreaming main-MoA overlap. Opting in can double count that completion; standalone auxiliary calls remain captured when opted in.
- Streamed auxiliary hooks fire before the stream is consumed and carry no final usage. They are omitted, even if a future payload happens to contain counts while `streaming=True`.
- Auxiliary failures, absent usage, invalid identity/model/provider/timing, and turnless auxiliary calls with empty session/request IDs are omitted rather than fabricated or attributed to a synthetic session.
- Optional providers/runtimes that bypass these hooks, provider adapters that discard usage details, and internal calls without canonical usage remain outside coverage.
- No retrospective reconciliation, invoices, pricing, session hierarchy, or tool/edit/git spans. A separately authorized production deployment verified one real Codex session end to end and immutable replay accounting; see [DEPLOYMENT.md](DEPLOYMENT.md). This does not establish all-provider coverage or rendered dashboard filtering.
- Verified against the installed revision above; schema/hook changes require rerunning native tests. Unix/macOS/Linux permissions are assumed; Windows is not supported by this version.
