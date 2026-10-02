# Implementation verification / TDD record

Implementation used incremental test-first slices, running each missing-behavior test before implementing and rerunning the suite after each change. This record summarizes observed tool results; it is not a replacement for the execution logs.

| Slice | Observed RED | GREEN implementation |
|---|---|---|
| Metadata-only usage projection | `metadata projector missing` | Allowlisted canonical OTLP body |
| Reject malformed metadata / preserve unknown | Invalid labels exported; missing usage buckets raised KeyError | Type/range/identity/label/interval guards, optional counts absent |
| Canonical identity and retry distinction | Span ID differed from Node oracle | Same JSON/SHA-256 identity algorithm; source and retry count in event ID |
| Durable immutable outbox | `durable outbox missing` | Private SQLite insert-once ledger, transactional lease/ack and tombstones |
| Real HTTP retry / partial rejection | `exporter missing` | Local HTTP transport, immutable retries and partial-success handling |
| Profile settings / secret separation | `profile configuration missing` | Native settings, protected credentials file/environment and validation |
| Real native loader / worker | `no register() function` | Native registration, captured profile, background worker and short unload |
| Usage consistency and optional metadata | Inconsistent totals exported; response model absent | Validate canonical totals; hashed request ID and optional timing/model |
| Native manifest parsing | `native manifest missing` | Manifest v2; actual scratch candidate scanner/loader |
| Private outbox containment | Symlink directory accepted | Reject symlink directories and unsafe permissions/ownership/hardlinks |
| Main/aux callback-control injection | Ledger had 3 events rather than 2 | Separate control flags from hook kwargs |
| Huge timestamp validation | OverflowError converting huge integer to float | Range-check before `isfinite` |
| Fractional canonical timestamp encoding | `1700000000123456000 != 1700000000123456055` against Node | Exact port of the existing millisecond-to-nanosecond encoding |
| Malformed fractional partial success | `rejectedSpans: 0.5` was acknowledged | Accept only integer/string zero; retain malformed/positive rejections |

Additional verification exercised already implemented behavior without adding production behavior: four independent processes racing on one ledger; stale lease ownership; fixture OpenAI Chat/Codex Responses/Anthropic main and auxiliary normalizers; poisoned response objects; socket trickling; native manifest scan, observer dispatch, profile isolation and unload.

Final commands:

```sh
python3 -m unittest discover -s plugins/hermes/tests -v
HERMES_SOURCE=/path/to/verified/hermes-agent /path/to/hermes-agent/venv/bin/python \
  -m unittest discover -s plugins/hermes/tests -v
npm ci --prefix plugins --ignore-scripts --no-audit --no-fund
node --test tests/test_plugin_*.mjs
git diff --check
```

The native compatibility run uses the installed Hermes revision identified in README, but loads the candidate only into a temporary scratch profile and sends only fixtures to loopback. Live Hermes core remained clean. No production install/activation, collector transmission, commit or push was performed.

The first repository Node regression run lacked `jsonc-parser` and had two dependency errors in existing OpenCode installer tests. Installing locked development dependencies with scripts disabled resolved both; the rerun passed all 22 tests. This dependency installation is not installation/activation of the Hermes plugin.

CI wiring is intentionally left to the parent integration owner (this subtask owns `plugins/hermes/` only). Add the default unittest command to the existing coding-agent plugin check; the optional native suite must not silently clone/install Hermes in CI.

## P1 MoA overlap regression and approved default-profile hot update

- Source inspection found no physical-call correlation between the two completion hooks. `aux_task="moa_aggregator"` covers both main-facade aggregation and independent standalone synthesis. The safe no-core-change policy is boolean `capture_auxiliary: false` by default, not a synthesizer blacklist. Opt-in preserves standalone auxiliary events but can reintroduce overlap.
- RED: real installed auxiliary emitter → native PluginManager hooks → real SQLite/projector → repository `aggregate.norm_span()` / `aggregate.billable()` produced **3 billed spans, input 45, output 15**, against expected **1 / 15 / 5** (fixture includes duplicate main aggregator and independent standalone synthesis). GREEN: **1 / 15 / 5** with default main-only coverage. The standalone synthesis is intentionally outside default coverage, not claimed deduplicated.
- Boolean opt-in validation RED: seven non-bool inputs were accepted; GREEN: rejected. Native explicit opt-in fixture retains `aux_task="moa_aggregator"` and exports it to loopback.
- Paused-export RED: transport called three times despite pause; GREEN: zero calls with a durable pending fixture retained. Settings default/validation also exercised RED → GREEN. `export_paused` is needed to perform the requested reload without the new worker replaying the existing production queue.
- Full pinned native suite: **22 tests passed, no skips** using an explicitly supplied verified Hermes checkout and its environment (`HERMES_SOURCE=<verified-checkout> <checkout-venv-python> -m unittest discover -s plugins/hermes/tests -v`). Exact local paths are intentionally not recorded here.
- Approved update copied only `__init__.py` and `plugin.yaml` into the target profile's `plugins/datalake-usage` directory, and used native `hermes config set` for `capture_auxiliary=false`, `export_paused=true`. Endpoint and credential hashes remained unchanged.
- Supported `gateway.control_socket.reload_gateway_plugins()` returned `reloaded=true`, two adapters rewired, and datalake-usage activation registered only `post_api_request`. The gateway process identity and start time were unchanged: no gateway restart. Installed files matched candidate bytes.
- The previously pending real CLI event remained pending with small input/output counts (exact values retained only in private deployment evidence). The previously running worker made one further attempt before the paused replacement took over; no explicit production exporter/fixture send was invoked and the request was not acknowledged. Pausing the replacement cannot retroactively cancel old-worker activity.
- At this earlier checkpoint, no launchctl actions, process termination, tunnel changes, core edits, other-profile edits, Wiki edits, commit or push occurred; export was left paused pending separate authorization. **Superseded:** the later authorized resumption, real-event delivery, normal aggregation and replay invariant are recorded in [DEPLOYMENT.md](DEPLOYMENT.md).
