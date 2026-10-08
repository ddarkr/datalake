#!/usr/bin/env python3
"""Scheduled SQL aggregation: AI / vehicle / home summaries from raw tables.

Stdlib only. Reads GREPTIME_* connection env plus:
  AGG_INTERVAL_SECONDS (default 300): scheduler period. The process runs
    every section each pass and sleeps; SIGTERM/SIGINT stops between passes.
  AGG_LOOKBACK_HOURS (default 30): bound for vehicle trip/charge/log scans
    and the SQL-pushdownagg windows (vehicle 1m/1h, home 5m/1h/1d).
  AGG_MAX_ROWS (default 200000): per-section raw-row cap. A section over the
  cap fails closed (error, no partial writes) instead of OOMing.
  BATTERY_ANALYSIS_CONFIG (default /app/battery-analysis.json): plain JSON
  config for the battery section (explicit missing path is an error); plus
  BATTERY_LOOKBACK_HOURS/BATTERY_MAX_ROWS (empty = fall back to the AGG
  values) and BATTERY_BACKFILL_START/END (both-or-neither explicit older
  recompute range). Battery rides AGG_INTERVAL_SECONDS; no new schedule.

Retention policy (fail-closed; DELETE only drops obsolete same-session starts):
  AI summaries rebuild from the full retained raw range each pass, so there
  is no mid-window cutoff to partially overwrite. Writes are guarded by the
  explicit configured OTEL_TTL (empty = unbounded raw retention): a session,
  day, or tool window starting before the raw retention boundary is left
  alone, so raw TTL expiry can never turn a full aggregate into a partial
  one. A late earlier span moves a session's TIME INDEX start, which lands
  as a new row under the same (client, session_id) key: each session insert
  is followed by a DELETE scoped to that (client, session_id) removing
  other starts, so one logical session stays one row (repeat runs rewrite
  the same row). Same session_id under another client is a different row.
  Daily/tool windows keep stable day keys and overwrite in place; log scans
  start at floor_day so the first written day is fed by full-day data.
  Vehicle/home SQL pushdowns and trip/charge/log scans use bounded sealed
  windows only (window end <= sealed now); the open bucket is never written.
  Re-segmentation may leave superseded trip/charge rows (DELETE is never
  used there); dashboards order by started_at and take the latest per vehicle.

// ponytail: AI sections re-scan retained spans every pass and trip/charge
// re-scan AGG_LOOKBACK_HOURS of signals; fine for agent/vehicle scale here.
// If raw rows routinely hit AGG_MAX_ROWS, tighten lookback or move the sums
// into SQL pushdown instead of raising the cap.

Idempotent: summary rows share keys with existing rows, so re-running
overwrites the same (key, time) rows. Missing source tables are skipped
(exit 0 for that section); real SQL errors fail the section. Secrets are
never printed.
"""

import base64
import datetime as dt
import json
import os
import re
import signal
import sys
import time
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from bisect import bisect_right

from scripts.telemetry.activity import (ACTIVITY_COUNT_FIELDS, ACTIVITY_VALUE_FIELDS,
                      ACTIVITY_TRACE_ATTRIBUTES, summarize_activity)

MISSING_TABLE_HINTS = ("not found", "not exist", "does not exist", "unknown table")
MIXED = "mixed"
TTL_RE = re.compile(r"^[0-9]+[smhd]$")
TTL_UNIT_S = {"s": 1, "m": 60, "h": 3600, "d": 86400}
# --- Supplemental cost estimates (public list rates, never invoices) ---
#
# Stdlib-only load of LiteLLM's public model_prices_and_context_window.json
# (per-token USD fields input_cost_per_token, output_cost_per_token,
# cache_read_input_token_cost, cache_creation_input_token_cost,
# output_cost_per_reasoning_token; entry identity is the model key plus
# litellm_provider). Memory-only price cache; the operational-status volume
# does not persist prices, so a restart refetches. Refresh is at most daily
# per process; a failed refresh keeps the last-good table and waits PRICE_RETRY_S, so a
# down registry never retries every aggregate pass.
LITELLM_PRICE_URL = ("https://raw.githubusercontent.com/BerriAI/litellm/main/"
                     "model_prices_and_context_window.json")
PRICE_REFRESH_S = 86400
PRICE_RETRY_S = 3600
PRICE_TIMEOUT_S = 10
PRICE_MAX_BYTES = 8 * 1024 * 1024
# Clients whose token semantics are source-verified above (plugins/*):
# claude-code input excludes cache buckets (Anthropic API); the rest report
# input inclusive of cache. Unknown clients never receive estimates.
_EST_CLIENTS = frozenset({"codex", "oh-my-pi", "opencode", "claude-code"})
_CHAT_MODES = frozenset({"chat", "completion", "responses"})
_PRICES = {"table": None, "ok_at": 0.0, "attempt_at": 0.0}
def _price_num(value):
    """Non-negative finite USD rate, or None when absent/unparseable."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        num = float(value)
    elif isinstance(value, str):
        try:
            num = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    if num != num or num in (float("inf"), float("-inf")) or num < 0:
        return None
    return num


def _parse_price_table(data):
    """{model key: (in, out, cache_read, cache_write, reasoning, mode,
    provider)}. Entries without a litellm_provider string are skipped
    (sample_spec prose, fallback_generalizations regex rules); unknown
    fields ignored so new upstream fields never break parsing."""
    if not isinstance(data, dict):
        return None
    out = {}
    for key, entry in data.items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            continue
        provider = entry.get("litellm_provider")
        if not isinstance(provider, str) or not provider:
            continue
        # Long-context tariffs need request-specific tiers; do not silently
        # apply the base rate while this lightweight estimator lacks them.
        if ("tiered_pricing" in entry or "off_peak_pricing" in entry
                or any("_above_" in field and "cost" in field for field in entry)):
            continue
        out[key] = (
            _price_num(entry.get("input_cost_per_token")),
            _price_num(entry.get("output_cost_per_token")),
            _price_num(entry.get("cache_read_input_token_cost")),
            _price_num(entry.get("cache_creation_input_token_cost")),
            _price_num(entry.get("output_cost_per_reasoning_token")),
            entry.get("mode"), provider)
    return out


def _fetch_price_bytes(url=LITELLM_PRICE_URL, timeout=PRICE_TIMEOUT_S):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read(PRICE_MAX_BYTES + 1)
    if len(raw) > PRICE_MAX_BYTES:
        raise SqlError("price payload too large")
    return raw


def load_price_table(now=None, fetch=None):
    """Cached LiteLLM price table, or None when never fetched successfully.
    At most one fetch per PRICE_REFRESH_S; a failed refresh keeps the
    last-good table and waits PRICE_RETRY_S, so a down registry never
    retries every aggregate pass. Aggregation never fails for pricing:
    failure returns the last-good table (or None). Tests pass fetch=...
    to avoid network."""
    now = time.time() if now is None else now
    if _PRICES["table"] is not None and now - _PRICES["ok_at"] < PRICE_REFRESH_S:
        return _PRICES["table"]
    if now - _PRICES["attempt_at"] < PRICE_RETRY_S:
        return _PRICES["table"]
    _PRICES["attempt_at"] = now
    try:
        raw = fetch() if fetch is not None else _fetch_price_bytes()
        table = _parse_price_table(json.loads(raw.decode("utf-8")))
        if not table:
            raise SqlError("price payload has no usable entries")
    except Exception:
        return _PRICES["table"]
    _PRICES["table"] = table
    _PRICES["ok_at"] = now
    return table


def _price_entry(table, model, provider):
    """The one exact-matching entry billed by this call's vendor, in a
    token-priced mode. The provider-prefixed key wins when it matches the
    vendor, else the bare model key; anything else (unknown, vendor
    mismatch, image/embedding/audio mode) is None: fail closed."""
    if provider:
        prefixed = table.get(provider + "/" + model)
        if isinstance(prefixed, tuple) and prefixed[6] == provider and (
                prefixed[5] is None or prefixed[5] in _CHAT_MODES):
            return prefixed
    bare = table.get(model)
    if isinstance(bare, tuple) and bare[6] == provider and (
            bare[5] is None or bare[5] in _CHAT_MODES):
        return bare
    return None


def _tok(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def estimate_call(row, table):
    """Supplemental USD for one missing-cost LLM call, or None (fail closed).
    Callers only pass rows with no reported cost (including 0). Token
    semantics are per verified plugin mapping: claude-code input excludes
    cache buckets; the other supported clients include them. Codex and
    OpenCode output includes reasoning, so never charge that subset twice.
    OMP reasoning conventions are not inferred from a breakdown alone.
    Missing cache rates remain unknown; reasoning defaults to output rates."""
    if table is None or row.get("client") not in _EST_CLIENTS:
        return None
    model, provider = row.get("model"), row.get("provider")
    if not model or not provider:
        return None
    entry = _price_entry(table, model, provider)
    if entry is None:
        return None
    vals = [_tok(row.get(k)) for k in
            ("input", "output", "cache_read", "cache_write", "reasoning")]
    if any(v is None and row.get(k) is not None
           for v, k in zip(vals, ("input", "output", "cache_read",
                                  "cache_write", "reasoning"))):
        return None
    if vals[0] is None or vals[1] is None:
        return None
    inp, outp, cread, cwrite, reason = (v or 0 for v in vals)
    if row["client"] == "claude-code":
        text_in = inp
    else:
        text_in = inp - cread - cwrite
    if text_in < 0:
        return None
    if reason and row["client"] not in ("codex", "opencode"):
        return None
    if row["client"] in ("codex", "opencode"):
        text_out = outp - reason
    else:
        text_out = outp
    if text_out < 0:
        return None
    rin, rout, rread, rwrite, rreason = entry[:5]
    parts = [(text_in, rin),
             (cread, rread),
             (cwrite, rwrite),
             (text_out, rout),
             (reason, rreason if rreason is not None else rout)]
    total = 0.0
    for qty, rate in parts:
        if qty == 0:
            continue
        if rate is None:
            return None
        total += qty * rate
    return total


def _supplemental(bs, cost_ids, prices):
    """(estimated_sum_or_None, unpriced_count_or_None) over money-eligible
    missing-cost LLM calls. Rows already carrying cost (including 0) keep
    it and are ignored; turn-suppressed parts (outside cost_ids) never get
    fallback and never count as unpriced since the turn total covers them.
    prices None (never loaded) means unevaluated: (None, None)."""
    if prices is None:
        return None, None
    calls = {id(s) for s in llm_calls(bs)}
    est, unpriced = [], 0
    for s in bs:
        if id(s) not in cost_ids or id(s) not in calls:
            continue
        if resolve_cost(s) != (None, None):
            continue
        val = estimate_call(s, prices)
        if val is None:
            unpriced += 1
        else:
            est.append(val)
    return (sum(est) if est else None), unpriced


STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True


def env(name, default=""):
    return os.environ.get(name, default)


def utcnow():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def floor_step(value, minutes):
    value = value.replace(second=0, microsecond=0)
    if minutes > 1:
        value = value.replace(minute=(value.minute // minutes) * minutes)
    return value


def floor_minute(value):
    return floor_step(value, 1)


def floor_hour(value):
    return value.replace(minute=0, second=0, microsecond=0)


def floor_day(value):
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def ts_lit(value):
    if value is None:
        return "NULL"
    if isinstance(value, dt.datetime):
        return "'" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def str_lit(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def num_lit(value):
    if value is None:
        return "NULL"
    if isinstance(value, float) and value.is_integer() and -(1 << 63) <= value < (1 << 63):
        return str(int(value))  # Greptime Int64 rejects the literal "15.0".
    return repr(value)


class SqlError(Exception):
    pass


def request_sql(base_url, auth, db, stmt, timeout=60):
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db, safe="")
    body = urllib.parse.urlencode({"sql": stmt}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Authorization", "Basic " + auth)
    req.add_header("X-Greptime-Timezone", "UTC")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", "replace")
        except Exception:
            raw = ""
        raise SqlError("http " + str(e.code) + ": " + raw[:300])
    except Exception as e:
        raise SqlError("request failed: " + str(e)[:200])
    try:
        payload = json.loads(raw)
    except Exception:
        raise SqlError("non-JSON response: " + raw[:200])
    if not isinstance(payload, dict):
        raise SqlError("malformed response: top-level is not an object")
    if payload.get("error"):
        raise SqlError(str(payload["error"])[:400])
    output = payload.get("output")
    if not isinstance(output, list) or not output or not all(
            isinstance(item, dict) for item in output):
        raise SqlError("malformed response: output is not a list of objects")
    return payload


def _is_ts_type(data_type):
    return isinstance(data_type, str) and "timestamp" in data_type.lower()


def _from_us(total_us):
    return dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=total_us)


def _int_to_dt(value):
    """Exact integer epoch conversion (no float): ns/us/ms/s by magnitude."""
    mag = abs(value)
    if mag >= 10 ** 17:
        return _from_us(value // 1000)  # nanoseconds
    if mag >= 10 ** 14:
        return _from_us(value)  # microseconds
    if mag >= 10 ** 11:
        return _from_us(value * 1000)  # milliseconds
    if mag >= 10 ** 8:
        return _from_us(value * 1000000)  # seconds
    return None  # implausible epoch: refuse to guess


def _typed_ts(value, data_type):
    low = str(data_type or "").lower()
    if "nano" in low:
        return _from_us(value // 1000)
    if "micro" in low:
        return _from_us(value)
    if "milli" in low:
        return _from_us(value * 1000)
    if "second" in low:
        return _from_us(value * 1000000)
    return _int_to_dt(value)  # timestamp type, unknown unit: magnitude


def _normalize_ts_value(value, data_type):
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return _typed_ts(value, data_type)
    if isinstance(value, float):
        return None  # float epoch precision is lossy: never guessed
    if isinstance(value, str):
        return value  # strings parse in parse_ts
    return None


def fetch_rows(base_url, auth, db, stmt):
    """Returns (columns, rows); timestamp-typed ints normalized to datetime
    via column_schemas data_type (ns/ms/us/s, integer math, never float)."""
    payload = request_sql(base_url, auth, db, stmt)
    rec = payload["output"][0].get("records")
    if not isinstance(rec, dict):
        raise SqlError("malformed response: records is not an object")
    schema = (rec.get("schema") or {}).get("column_schemas") or []
    if not all(isinstance(c, dict) and isinstance(c.get("name"), str) for c in schema):
        raise SqlError("malformed response: column schema is not name objects")
    rows = rec.get("rows", [])
    if not isinstance(rows, list):
        raise SqlError("malformed response: rows is not a list")
    cols = [c.get("name") for c in schema]
    ts_cols = [(i, c.get("data_type")) for i, c in enumerate(schema)
               if _is_ts_type(c.get("data_type"))]
    if ts_cols:
        fixed = []
        for r in rows:
            r = list(r)
            for i, dtype in ts_cols:
                if i < len(r):
                    r[i] = _normalize_ts_value(r[i], dtype)
            fixed.append(r)
        rows = fixed
    return cols, rows


def is_missing_table(err):
    low = str(err).lower()
    return any(h in low for h in MISSING_TABLE_HINTS)


# --- AI normalization (per-call usage only; rollups excluded by signal) ---

TOKEN_IN = "span_attributes.gen_ai.usage.input_tokens"
TOKEN_OUT = "span_attributes.gen_ai.usage.output_tokens"
TOKEN_CACHE_READ = "span_attributes.gen_ai.usage.cache_read.input_tokens"
TOKEN_CACHE_CREATE = "span_attributes.gen_ai.usage.cache_creation.input_tokens"
TOKEN_CACHE_WRITE = "span_attributes.gen_ai.usage.cache_write.input_tokens"
TOKEN_REASON = "span_attributes.gen_ai.usage.reasoning.output_tokens"
COST_EST = "span_attributes.pi.gen_ai.cost.estimated_usd"
COST_SRC = "span_attributes.pi.gen_ai.cost.source"
COST_NATIVE = "span_attributes.cost_usd"
OP_NAME = "span_attributes.gen_ai.operation.name"
CONV_ID = "span_attributes.gen_ai.conversation.id"
SESSION = "span_attributes.coding_agent.session.id"
CLIENT = "span_attributes.coding_agent.client"
PROVIDER = "span_attributes.gen_ai.provider.name"
MODEL_REQ = "span_attributes.gen_ai.request.model"
MODEL_RESP = "span_attributes.gen_ai.response.model"
TOOL_NAME = "span_attributes.gen_ai.tool.name"
RESP_ID = "span_attributes.gen_ai.response.id"
TOOL_CALL_ID = "span_attributes.gen_ai.tool.call.id"

SPAN_COLS = ["timestamp", "timestamp_end", "duration_nano", "trace_id", "span_id",
             "parent_span_id", "span_name", "span_status_code", "service_name",
             TOKEN_IN, TOKEN_OUT, TOKEN_CACHE_READ, TOKEN_CACHE_CREATE,
             TOKEN_CACHE_WRITE, TOKEN_REASON, COST_EST, COST_SRC, COST_NATIVE,
             RESP_ID, TOOL_CALL_ID, OP_NAME, CONV_ID, SESSION, CLIENT, PROVIDER,
             MODEL_REQ, MODEL_RESP, TOOL_NAME]
SPAN_COLS = list(dict.fromkeys(SPAN_COLS + [
    "span_attributes." + key for key in ACTIVITY_TRACE_ATTRIBUTES
    if key not in ("service.name", "service_name")]))

# Operation names that denote a rollup/agent wrapper rather than one billed
# model call. Matched case-insensitively: exact names plus any op mentioning
# a session/workflow/rollup aggregate.
ROLLUP_OPS = {"invoke_agent", "execute_tool", "session", "workflow"}
ROLLUP_HINTS = ("session", "workflow", "rollup", "cumulative")


def is_rollup_op(op):
    if not op:
        return False
    low = str(op).lower()
    if low in ROLLUP_OPS:
        return True
    return any(h in low for h in ROLLUP_HINTS)


_NATIVE_SERVICE_CLIENTS = {
    "claude-code": "claude-code",
    "claude-code-desktop": "claude-code",
    "oh-my-pi": "oh-my-pi",
}
_CODEX_PREFIX = "codex."

def norm_span(row):
    """Normalize one deduped span dict. Missing-source fields stay None;
    unverified service names stay None (never guessed into identity)."""
    cache_write = row.get(TOKEN_CACHE_WRITE)
    if cache_write is None:
        cache_write = row.get(TOKEN_CACHE_CREATE)  # OMP native name
    model = row.get(MODEL_RESP) or row.get(MODEL_REQ)
    status = (row.get("span_status_code") or "")
    session = (row.get(SESSION) or row.get("span_attributes.session.id") or
               row.get("span_attributes.conversation.id") or row.get(CONV_ID))
    client = row.get(CLIENT) or row.get("span_attributes.client")
    if not client:
        # ponytail: codex.* namespace wins even over a known service.name
        # (Codex service.name is an overridable originator value). Otherwise
        # only source-verified service names map; anything else stays None.
        if str(row.get("span_attributes.event.name") or row.get("span_name") or "").startswith(_CODEX_PREFIX):
            client = "codex"
        else:
            service = row.get("service_name")
            if service in _NATIVE_SERVICE_CLIENTS:
                client = _NATIVE_SERVICE_CLIENTS[service]
            else:
                client = None
    return {
        "ts": row.get("timestamp"),
        "end": row.get("timestamp_end"),
        "trace": row.get("trace_id"),
        "span": row.get("span_id"),
        "parent": row.get("parent_span_id"),
        "op": row.get(OP_NAME),
        "session": session,
        "client": client,
        "provider": row.get(PROVIDER),
        "model": model,
        "tool": row.get(TOOL_NAME),
        "input": row.get(TOKEN_IN),
        "output": row.get(TOKEN_OUT),
        "cache_read": row.get(TOKEN_CACHE_READ),
        "cache_write": cache_write,
        "reasoning": row.get(TOKEN_REASON),
        "cost_est": row.get(COST_EST),
        "cost_native": row.get(COST_NATIVE),
        "cost_source": row.get(COST_SRC),
        "call_id": row.get(RESP_ID) or row.get(TOOL_CALL_ID),
        "duration_ms": (row.get("duration_nano") / 1e6
                        if row.get("duration_nano") is not None else None),
        "error": "ERROR" in status.upper(),
        "has_usage": row.get(TOKEN_IN) is not None or row.get(TOKEN_OUT) is not None,
        "billed_source": "span",
        "turn": row.get("span_attributes.turn.id") or row.get("span_attributes.prompt.id"),
    }


def sum_opt(values):
    vals = [v for v in values if v is not None]
    return sum(vals) if vals else None


def parse_ts(value):
    """Parse timestamps without float guessing. Ints use magnitude-based
    unit detection (fetch_rows already normalizes typed columns to
    datetime); floats and bools are refused (None), never guessed."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return _int_to_dt(value)
    if isinstance(value, float):
        return None
    try:
        s = str(value).replace("T", " ").replace("Z", "")
        return dt.datetime.fromisoformat(s.split(".")[0])
    except Exception:
        return None


def dedupe_spans(cols, rows):
    """Dedupe at-least-once redeliveries on (trace_id, span_id)."""
    idx = {c: i for i, c in enumerate(cols)}
    seen = {}
    for r in rows:
        d = {c: (r[idx[c]] if c in idx else None) for c in SPAN_COLS if c in idx}
        key = (d.get("trace_id"), d.get("span_id"))
        if key == (None, None):
            key = ("", "", len(seen))
        if key not in seen:
            seen[key] = d
    return [norm_span(d) for d in seen.values()]


def billable(spans):
    """Per-call spans only: must carry usage and must not be a rollup op,
    nor a parent whose usage exactly equals the sum of its billed children
    (cumulative parent re-emit, detected by real parent/usage signal).
    Native-authoritative session scope: when a (client, session) scope
    carries native-sourced usage, only native rows bill there; span usage
    rows stay as non-billed session/tool context (time range, tools, errors
    still read the full row set). Without shared call IDs the overlap cannot
    be paired per-request, so selection is once per scope, never guessed per
    row. Scopes without native usage bill spans exactly as before."""
    by_parent = {}
    for s in spans:
        if s["trace"] is not None and s["parent"]:
            by_parent.setdefault((s["trace"], s["parent"]), []).append(s)
    out = []
    for s in spans:
        if not s["has_usage"] or is_rollup_op(s["op"]):
            continue
        kids = by_parent.get((s["trace"], s["span"]), [])
        kids = [k for k in kids if k["has_usage"] and not is_rollup_op(k["op"])]
        if kids and all(k["input"] is not None and k["output"] is not None
                        for k in kids) and s["input"] is not None \
                and s["output"] is not None:
            if s["input"] == sum(k["input"] for k in kids) and \
                    s["output"] == sum(k["output"] for k in kids):
                continue  # cumulative parent: children already bill it
        out.append(s)
    # ponytail: native usage is authoritative per identified session. Mixed
    # native-only/span-only coverage needs an explicit producer overlap contract.
    native_scopes = {
        (s["client"], s["session"]) for s in out
        if s.get("client") and s.get("session")
        and s.get("billed_source") == "native"
        and (s.get("input") is not None or s.get("output") is not None)
    }
    return [s for s in out if s.get("billed_source") == "native"
            or (s.get("client"), s.get("session")) not in native_scopes]


# --- Native log usage (Codex sse_event + Claude api_request), JSON attrs ---

LOG_COLS = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes", "resource_attributes"]

NATIVE_CODEX_KIND = "response.completed"
NATIVE_CLAUDE_EVENT = "api_request"

# Terminal native tool records: one logical call each, joined to spans by
# stable tool id below. Permission/decision, hook plumbing, and lifecycle
# events never match this set, so they stay non-calls (norm_log drops them).
TOOL_RESULT_EVENTS = {"codex.tool_result", "tool_result",
                      "claude_code.tool_result"}


def to_int(value):
    """Trace numeric counts often arrive as strings: accept int-like
    strings/bools-free ints/floats with integral value, else None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        s = value.strip().replace(",", "").replace("_", "")
        if not s:
            return None
        try:
            if s.lower().startswith("0x"):
                return int(s, 16)
            if "." in s or "e" in s.lower():
                f = float(s)
                return int(f) if f.is_integer() else None
            return int(s)
        except Exception:
            return None
    return None


def to_float(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip().replace(",", "").replace("_", ""))
        except Exception:
            return None
    return None


def attrs_of(row, field="log_attributes"):
    """Greptime JSON attributes may be a string or an already-decoded map."""
    a = row.get(field)
    if isinstance(a, dict):
        return a
    if isinstance(a, str):
        try:
            import json as _json
            v = _json.loads(a)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}

def native_client(attrs, resource):
    """Client for native log records. Explicit coding_agent.client/client
    always wins verbatim. Codex is recognized by the codex.* event namespace
    (its service.name is runtime originator/override, never identity).
    Otherwise only source-verified native service names map; anything else
    is provenance, not identity (None -> "unknown" group downstream). Codex
    namespace wins even over a known service name (service.name is an
    overridable originator value)."""
    explicit = (attrs.get("coding_agent.client") or attrs.get("client") or
                resource.get("coding_agent.client") or resource.get("client"))
    if explicit:
        return explicit
    if str(attrs.get("event.name") or "").startswith(_CODEX_PREFIX):
        return "codex"
    service = resource.get("service.name")
    if service in _NATIVE_SERVICE_CLIENTS:
        return _NATIVE_SERVICE_CLIENTS[service]
    return None

def norm_log(row):
    """One native usage record or None (non-usage log). Never fabricates.
    Turn-cost receipts carry no call id (a turn total is not a call) and are
    marked turn_total so summaries replace, never sum, that turn's parts."""
    a = attrs_of(row)
    resource = attrs_of(row, "resource_attributes")
    client = native_client(a, resource)
    name = a.get("event.name")
    kind = a.get("event.kind")
    is_codex = name == "codex.sse_event" and kind == NATIVE_CODEX_KIND
    is_claude = name in ("api_request", "claude_code.api_request")
    is_turn_cost = name == "codex.turn_cost"
    is_tool = name in TOOL_RESULT_EVENTS
    if not (is_codex or is_claude or is_turn_cost or is_tool):
        return None
    if is_turn_cost:
        # Turn TOTAL: no token counts on this event; the sse_event
        # completions of the same turn carry usage. Money-only row: summaries
        # replace the same-turn per-call costs with this total (counts kept).
        cost = to_float(a.get("usage.estimated_usd"))
        if cost is None:
            return None
        return {
            "ts": row.get("timestamp"),
            "session": (a.get("coding_agent.session.id") or a.get("session.id")
                        or a.get("conversation.id")
                        or a.get("gen_ai.conversation.id")),
            "client": client,
            "provider": a.get("gen_ai.provider.name"),
            "model": a.get("model") or a.get("gen_ai.request.model"),
            "tool": None, "input": None, "output": None, "cache_read": None,
            "cache_write": None, "reasoning": None, "total": None,
            "cost": cost, "cost_source": "estimated",
            "call_id": None,
            "turn": a.get("turn.id") or a.get("prompt.id"),
            "turn_total": True,
            "conv": a.get("conversation.id"),
            "seq": to_int(a.get("event.sequence")),
            "error": (row.get("severity_text") or "") == "ERROR",
            "native": "codex_cost",
        }
    if is_tool:
        # Terminal tool record: one logical call, joined to spans by stable
        # tool id in the summaries. Carries no usage/cost, so it never bills
        # as an LLM call. Permission/hook events never reach this branch.
        return {
            "ts": row.get("timestamp"),
            "session": (a.get("coding_agent.session.id") or a.get("session.id")
                        or a.get("conversation.id")
                        or a.get("gen_ai.conversation.id")),
            "client": client,
            "provider": a.get("gen_ai.provider.name"),
            "model": a.get("model") or a.get("gen_ai.request.model"),
            "tool": (a.get("tool_name") or a.get("gen_ai.tool.name")
                     or a.get("tool")),
            "input": None, "output": None, "cache_read": None,
            "cache_write": None, "reasoning": None, "total": None,
            "cost": None, "cost_source": None,
            "call_id": (a.get("gen_ai.tool.call.id") or a.get("tool_use_id")
                        or a.get("call_id")),
            "turn": a.get("turn.id") or a.get("prompt.id"),
            "turn_total": False,
            "conv": a.get("conversation.id"),
            "seq": to_int(a.get("event.sequence")),
            "error": (row.get("severity_text") or "") == "ERROR",
            "duration_ms": to_float(a.get("duration_ms")),
            "native": "tool",
        }
    if is_codex:
        inp = to_int(a.get("input_token_count"))
        outp = to_int(a.get("output_token_count"))
        cache_read = to_int(a.get("cached_token_count"))
        cache_write = to_int(a.get("cache_write_token_count"))
        reasoning = to_int(a.get("reasoning_token_count"))
        total = to_int(a.get("tool_token_count"))
        cost = to_float(a.get("usage.estimated_usd"))
        cost_src = "estimated" if cost is not None else None
    else:
        inp = to_int(a.get("input_tokens"))
        outp = to_int(a.get("output_tokens"))
        cache_read = to_int(a.get("cache_read_tokens"))
        cache_write = to_int(a.get("cache_creation_tokens"))
        reasoning = None
        total = None
        cost = to_float(a.get("cost_usd"))
        micros = to_int(a.get("cost_usd_micros"))
        if cost is None and micros is not None:
            cost = micros / 1e6  # integer micros -> exact float division
        # Claude native cost_usd is an estimate: never claim billed cost.
        cost_src = "estimated" if cost is not None else None
    if inp is None and outp is None and cost is None:
        return None
    return {
        "ts": row.get("timestamp"),
        "session": (a.get("coding_agent.session.id") or a.get("session.id")
                    or a.get("conversation.id")
                    or a.get("gen_ai.conversation.id")),
        "client": client,
        "provider": a.get("gen_ai.provider.name"),
        "model": a.get("model") or a.get("gen_ai.request.model"),
        "tool": (a.get("tool_name") or a.get("gen_ai.tool.name")
                 or a.get("tool")),
        "input": inp, "output": outp, "cache_read": cache_read,
        "cache_write": cache_write, "reasoning": reasoning, "total": total,
        "cost": cost, "cost_source": cost_src,
        "call_id": (a.get("gen_ai.response.id") or a.get("gen_ai.tool.call.id")
                    or a.get("request_id") or a.get("client_request_id")
                    or a.get("call_id") or a.get("tool_use_id")
                    or a.get("message.uuid")),
        "turn": a.get("turn.id") or a.get("prompt.id"),
        "turn_total": False,
        "conv": a.get("conversation.id"),
        "seq": to_int(a.get("event.sequence")),
        "error": (row.get("severity_text") or "") == "ERROR",
        "native": "codex" if is_codex else "claude",
    }


def dedupe_logs(cols, rows):
    """Stable dedupe for at-least-once log redelivery: identity is
    (signal kind, client, session, request/tool/message id) when present,
    (signal kind, client, session, turn) for turn-total receipts, else
    (client, event.name, timestamp, session, sequence). The kind prefix keeps
    LLM usage, cost-only, and terminal tool ids in separate namespaces.
    Distinct ids, clients, sessions, or turns always stay separate."""
    idx = {c: i for i, c in enumerate(cols)}
    seen = {}
    for r in rows:
        d = {c: (r[idx[c]] if c in idx else None) for c in LOG_COLS if c in idx}
        rec = norm_log(d)
        if rec is None:
            continue
        a = attrs_of(d)
        name = a.get("event.name") or rec["native"]
        if rec.get("turn_total") and rec.get("turn") is not None:
            # One total per (client, session, turn): redeliveries and updated
            # receipts collapse here, never sum downstream.
            key = (rec["native"], rec["client"], rec["session"], rec["turn"])
        elif rec["call_id"]:
            key = (rec["native"], rec["client"], rec["session"], rec["call_id"])
        else:
            key = (rec["client"], name, rec["ts"], rec["session"], rec["seq"])
        if key not in seen:
            seen[key] = rec
    return list(seen.values())


def _native_row(log):
    """Appendable row for a native record with no span match. Tool records
    stay non-billed tool rows (no usage/cost); cost-only stays money-only.
    Rows carry billed_source so the billed set can select native-authoritative
    usage once per (client, session) without dropping span context."""
    if log.get("native") == "tool":
        op = "tool_result"
    elif str(log.get("native")).startswith("codex"):
        op = "codex.sse_event"
    else:
        op = "claude_code.api_request"
    return {
        "ts": log["ts"], "end": log["ts"], "trace": None, "span": None,
        "parent": None, "op": op,
        "session": log["session"], "client": log["client"],
        "provider": log["provider"], "model": log["model"],
        "tool": log["tool"], "input": log["input"], "output": log["output"],
        "cache_read": log["cache_read"], "cache_write": log["cache_write"],
        "reasoning": log["reasoning"], "cost_est": log["cost"],
        "cost_native": (log["cost"] if log["native"] == "claude" else None),
        "cost_source": log["cost_source"], "call_id": log["call_id"],
        "duration_ms": log.get("duration_ms"), "error": log["error"],
        "has_usage": (log["input"] is not None or log["output"] is not None
                      or log["cost"] is not None),
        "native_conv": log["conv"],
        "billed_source": ("tool" if log.get("native") == "tool" else "native"),
        "turn": log.get("turn"), "turn_total": bool(log.get("turn_total")),
    }


def _override_span(span, log):
    """Span copy with native usage/cost winning. Span trace identity,
    timestamps, duration, and tool/session enrichment survive; estimate and
    native copies never sum: one row bills the native value."""
    out = dict(span)
    for field in ("input", "output", "cache_read", "cache_write", "reasoning"):
        if log.get(field) is not None:
            out[field] = log[field]
    if log.get("cost") is not None:
        out["cost_est"] = log["cost"]
        out["cost_native"] = log["cost"] if log.get("native") == "claude" else None
        out["cost_source"] = log.get("cost_source")
    for field in ("tool", "session", "client", "provider", "model"):
        if out.get(field) is None:
            out[field] = log.get(field)
    if out.get("duration_ms") is None and log.get("duration_ms") is not None:
        out["duration_ms"] = log["duration_ms"]
    out["error"] = bool(span.get("error") or log.get("error"))
    out["has_usage"] = (out.get("input") is not None or out.get("output") is not None
                        or out.get("cost_est") is not None
                        or out.get("cost_native") is not None)
    if log.get("turn") is not None:
        out["turn"] = log.get("turn")
    elif "turn" not in out:
        out["turn"] = None
    if log.get("conv") is not None:
        out["native_conv"] = log.get("conv")
    elif "native_conv" not in out:
        out["native_conv"] = None
    out["billed_source"] = "native"
    return out


def _merge_usage(spans_billed, logs):
    """Usage fold only: stable (client, session, LLM id) override/append.
    Tool ids live in a separate namespace: a tool result whose id happens
    to equal an LLM request id never folds into that LLM row. Cost-only
    receipts never match an LLM row by turn id: matching a summary-level
    turn attribution onto a per-call id would silently bury money."""
    span_keys = set()
    for s in spans_billed:
        if s.get("call_id"):
            span_keys.add((s.get("client"), s.get("session"), s.get("call_id")))
    log_by_key, unmatched = {}, []
    for log in logs:
        if log.get("native") == "codex_cost" or not log.get("call_id"):
            unmatched.append(log)  # money-only: keep, never fold by turn id
            continue
        key = (log.get("client"), log.get("session"), log.get("call_id"))
        if key in span_keys and key not in log_by_key:
            log_by_key[key] = log
        elif key in span_keys:
            continue  # redelivered native copy of an already-folded call
        else:
            unmatched.append(log)
    merged, native_n, seen = [], 0, set()
    for s in spans_billed:
        if not s.get("call_id"):
            merged.append(s)
            continue
        key = (s.get("client"), s.get("session"), s.get("call_id"))
        log = log_by_key.get(key)
        if log is None:
            merged.append(s)
        elif key in seen:
            continue  # duplicate span copy of one call: native row kept once
        else:
            seen.add(key)
            merged.append(_override_span(s, log))
            native_n += 1
    for log in unmatched:
        merged.append(_native_row(log))
        native_n += 1
    return merged, native_n


def _merge_tools(spans, tools, native_n):
    """Append terminal tool rows as non-billed tool rows. No folding here:
    tool spans live outside the billed set, so cross-signal tool dedupe
    happens in the summaries (shared counting policy), never by dropping."""
    for log in tools:
        spans.append(_native_row(log))
        native_n += 1
    return spans, native_n


def merge_native(spans_billed, logs):
    """Fold native log usage into the billed set. Identity is the stable
    (client, session, request/tool/message id) triple: a native record
    matching a billed span overrides that span in place (native usage wins,
    span duration/tool/session enrichment survives, estimate plus native
    never sums). Token/cost values are never identity: identical counts on
    distinct ids, clients, or sessions stay separate rows. Cost-only turn
    receipts stay as money-only rows: summaries add their cost without
    counting an LLM call. Terminal tool records without usage stay as
    non-billed tool rows joined by stable tool id, so a tool span plus its
    terminal result bills once while genuinely distinct calls are kept.
    A native record with no stable id, or whose id matches nothing, is
    appended as its own row: without cross-signal identity there is
    nothing to join on, so the merge keeps both sides rather than
    guessing. Returns (merged, native_count) folding overrides and
    appends together."""
    if not logs:
        return list(spans_billed), 0
    usage = [l for l in logs if l.get("native") != "tool"]
    tools = [l for l in logs if l.get("native") == "tool"]
    merged, native_n = _merge_usage(spans_billed, usage)
    return _merge_tools(merged, tools, native_n)


def resolve_cost(s):
    """Estimated-first with explicit source; native only when no estimate.
    Both present -> mixed (never summed into one number silently)."""
    est, nat, src = s["cost_est"], s["cost_native"], s["cost_source"]
    if est is not None and nat is not None:
        return est, (src or MIXED)
    if est is not None:
        return est, (src or "estimated")
    if nat is not None:
        return nat, (src or "native")
    return None, None


def cost_label(spans):
    srcs = sorted({c for _, c in (resolve_cost(s) for s in spans) if c})
    if len(srcs) > 1:
        return MIXED  # estimated/native must never merge under one label
    if srcs:
        return srcs[0]
    return None


def llm_calls(rows):
    """Billed rows that are real LLM calls: carry token usage. Cost-only
    turn receipts (money, no tokens) add cost without counting a call."""
    return [s for s in rows
            if s.get("input") is not None or s.get("output") is not None]


def _cost_ids(rows):
    """Replace turn parts before provider/model/day grouping, without attribution guesses."""
    totals = {}
    for row in rows:
        key = (row.get("client"), row.get("session"), row.get("turn"))
        if row.get("turn_total") and all(key):
            totals.setdefault(key, row)
    return {id(row) for row in rows
            if (row.get("client"), row.get("session"), row.get("turn")) not in totals
            or totals[(row.get("client"), row.get("session"), row.get("turn"))] is row}


_SENTINEL = object()


def summarize_sessions(spans, prices=_SENTINEL):
    billed = billable(spans)
    bset = {id(s) for s in billed}
    cost_ids = _cost_ids(billed)
    # One shared tool identity: span + terminal result is one logical call
    # even when the halves carry different provider/model metadata.
    kept_tools = {id(s) for s in distinct_tools(
        [s for s in spans if s.get("tool")])}
    by = {}
    for s in spans:
        if not s["session"]:
            continue
        # Logical session identity is (client, session): the same session
        # id under different clients never merges (native merge joins on
        # the same triple, so summaries must count on the same key).
        # Missing metadata is the 'unknown' group, never a vendor: it
        # matches the existing daily/native fallback and keeps client PK
        # non-null.
        g = by.setdefault((s["client"] or "unknown", s["session"]),
                          {"all": [], "billed": []})
        g["all"].append(s)
        if id(s) in bset:
            g["billed"].append(s)
    out = []
    for (client, sid), g in by.items():
        ss, bs = g["all"], g["billed"]
        starts = [t for t in (parse_ts(s["ts"]) for s in ss) if t]
        ends = [t for t in (parse_ts(s["end"] or s["ts"]) for s in ss) if t]
        if not starts:
            continue
        start, end = min(starts), max(ends)
        models = sorted({s["model"] for s in ss if s["model"]})
        money = [s for s in bs if id(s) in cost_ids]
        cost_total = sum_opt([resolve_cost(s)[0] for s in money])
        cost_src = cost_label(money)
        if prices is _SENTINEL:
            cost_est, unpriced = None, None
        else:
            cost_est, unpriced = _supplemental(bs, cost_ids, prices)
        out.append({
            "session_start": start, "session_id": sid,
            "client": client,  # group key fallback, never NULL
            "provider": next((s["provider"] for s in ss if s["provider"]), None),
            "models": models or None,
            "model_count": len(models) or None,
            "session_end": end,
            "duration_s": (end - start).total_seconds(),
            "input": sum_opt([s["input"] for s in bs]),
            "output": sum_opt([s["output"] for s in bs]),
            "cache_read": sum_opt([s["cache_read"] for s in bs]),
            "cache_write": sum_opt([s["cache_write"] for s in bs]),
            "reasoning": sum_opt([s["reasoning"] for s in bs]),
            "total": sum_opt([((s["input"] or 0) + (s["output"] or 0))
                              if (s["input"] is not None or s["output"] is not None) else None
                              for s in bs]),
            "cost": cost_total,
            "cost_source": cost_src,
            "cost_estimated_usd": cost_est,
            "cost_unpriced_calls": unpriced,
            "llm_spans": len(llm_calls(bs)) or None,
            "tool_calls": len([s for s in ss if s["tool"] and id(s) in kept_tools]) or None,
            "errors": sum(1 for s in ss if s["error"] and (not s.get("tool") or id(s) in kept_tools)) or None,
        })
    return out

def summarize_daily(spans, prices=_SENTINEL):
    billed = billable(spans)
    bset = {id(s) for s in billed}
    cost_ids = _cost_ids(billed)
    kept_tools = {id(s) for s in distinct_tools(
        [s for s in spans if s.get("tool")])}
    by = {}
    for s in spans:
        t = parse_ts(s["ts"])
        if not t:
            continue
        day = floor_day(t)
        # Missing optional metadata is an explicit group, not a guessed provider.
        key = (day, s["client"] or "unknown", s["provider"] or "unknown",
               s["model"] or "unknown")
        g = by.setdefault(key, {"all": [], "billed": []})
        g["all"].append(s)
        if id(s) in bset:
            g["billed"].append(s)
    out = []
    for (day, client, provider, model), g in by.items():
        bs = g["billed"]
        money = [s for s in bs if id(s) in cost_ids]
        cost_total = sum_opt([resolve_cost(s)[0] for s in money])
        cost_src = cost_label(money)
        if prices is _SENTINEL:
            cost_est, unpriced = None, None
        else:
            cost_est, unpriced = _supplemental(bs, cost_ids, prices)
        out.append({
            "day": day, "client": client, "provider": provider, "model": model,
            "input": sum_opt([s["input"] for s in bs]),
            "output": sum_opt([s["output"] for s in bs]),
            "cache_read": sum_opt([s["cache_read"] for s in bs]),
            "cache_write": sum_opt([s["cache_write"] for s in bs]),
            "reasoning": sum_opt([s["reasoning"] for s in bs]),
            "total": sum_opt([((s["input"] or 0) + (s["output"] or 0))
                              if (s["input"] is not None or s["output"] is not None) else None
                              for s in bs]),
            "cost": cost_total,
            "cost_source": cost_src,
            "cost_estimated_usd": cost_est,
            "cost_unpriced_calls": unpriced,
            "llm_spans": len(llm_calls(bs)) or None,
            "tool_calls": len([s for s in g["all"] if s["tool"] and id(s) in kept_tools]) or None,
            "sessions": len({s["session"] for s in g["all"] if s["session"]}) or None,
            "errors": sum(1 for s in g["all"] if s["error"] and (not s.get("tool") or id(s) in kept_tools)) or None,
        })
    return out


def tool_key(s):
    """One logical tool counts once even when a tool span and its terminal
    result both carry the span/native half. Identity is only the reported
    stable tool id scoped by client/session: never amount, name, duration,
    or error. Rows without an id are distinct: dropping them would lose
    genuinely distinct calls."""
    if s.get("call_id"):
        return (s.get("client"), s.get("session"), s.get("call_id"))
    return ("", s.get("client"), s.get("session"), s.get("ts"),
            s.get("tool"), id(s))


def distinct_tools(rows):
    """First row wins per logical tool; duplicates fill gaps only."""
    seen = {}
    for s in rows:
        key = tool_key(s)
        cur = seen.get(key)
        if cur is None:
            seen[key] = s
            continue
        for field in ("tool", "session", "client", "provider", "model",
                      "duration_ms"):
            if cur.get(field) is None and s.get(field) is not None:
                cur[field] = s[field]
        cur["error"] = bool(cur.get("error") or s.get("error"))
    return list(seen.values())


def summarize_tools(spans):
    by = {}
    for s in distinct_tools([s for s in spans if s.get("tool")]):
        t = parse_ts(s["ts"])
        if not t:
            continue
        key = (floor_day(t), s["tool"], s["client"] or "unknown")
        g = by.setdefault(key, {"durs": [], "errors": 0, "calls": 0})
        g["calls"] += 1
        if s["duration_ms"] is not None:
            g["durs"].append(s["duration_ms"])
        g["errors"] += 1 if s["error"] else 0
    out = []
    for (day, tool, client), g in by.items():
        durs = g["durs"]
        out.append({"day": day, "tool": tool, "client": client,
                    "calls": g["calls"],
                    "errors": g["errors"] or None,
                    "avg_ms": (sum(durs) / len(durs)) if durs else None})
    return out


def summarize_logs(cols, rows):
    """Source-specific log counts by (day, scope, severity). Counts only."""
    idx = {c: i for i, c in enumerate(cols)}
    by = {}
    for r in rows:
        def col(name):
            return r[idx[name]] if name in idx else None
        t = parse_ts(col("timestamp"))
        if not t:
            continue
        sev = col("severity_text") or "UNSPECIFIED"
        scope = col("scope_name") or "unknown"
        num = col("severity_number")
        err = sev.upper() == "ERROR" or (isinstance(num, (int, float)) and num >= 17)
        g = by.setdefault((floor_day(t), scope, sev), {"n": 0, "e": 0})
        g["n"] += 1
        g["e"] += 1 if err else 0
    return [{"day": day, "scope": scope, "severity": sev,
             "count": g["n"], "errors": g["e"] or None}
            for (day, scope, sev), g in by.items()]


def insert_rows(base_url, auth, db, table, columns, rows, batch=500):
    for i in range(0, len(rows), batch):
        vals = ", ".join("(" + ", ".join(r) + ")" for r in rows[i:i + batch])
        request_sql(base_url, auth, db,
                    "INSERT INTO " + table + " (" + ", ".join(columns) + ") VALUES " + vals)


# --- SQL-built aggregations (sealed windows only; never the open bucket) ---

def vehicle_agg_sql(resolution, interval, cutoff, sealed):
    res = "'" + resolution.replace("'", "''") + "'"
    return ("INSERT INTO vehicle_agg (window_start, resolution, vehicle, path, source,"
            " decode_epoch, avg_value, min_value, max_value, sample_count, unit) SELECT"
            " date_bin('" + interval + "'::INTERVAL, event_time) AS window_start,"
            " " + res + " AS resolution, vehicle, path, source, decode_epoch,"
            " AVG(value_num), MIN(value_num), MAX(value_num), COUNT(*), MAX(unit)"
            " FROM vehicle_signal WHERE event_time >= '" + cutoff + "'"
            " AND event_time < '" + sealed + "'"
            " AND value_num IS NOT NULL"
            " GROUP BY date_bin('" + interval + "'::INTERVAL, event_time),"
            " vehicle, path, source, decode_epoch")


def home_agg_sql(table, time_col, entity_col, value_col, resolution, interval,
                 cutoff, sealed):
    ent = entity_col if entity_col else "'home'"
    return ("INSERT INTO home_agg (window_start, resolution, entity, source,"
            " avg_value, min_value, max_value, sample_count) SELECT"
            " date_bin('" + interval + "'::INTERVAL, " + time_col + ") AS window_start,"
            " '" + resolution + "' AS resolution, " + ent + ", " + str_lit(table + "." + value_col) + ","
            " AVG(" + value_col + "), MIN(" + value_col + "), MAX(" + value_col + "), COUNT(*)"
            " FROM " + table + " WHERE " + time_col + " >= '" + cutoff + "'"
            " AND " + time_col + " < '" + sealed + "'"
            " AND " + value_col + " IS NOT NULL"
            " GROUP BY date_bin('" + interval + "'::INTERVAL, " + time_col + "), " + ent)


# --- Vehicle signal helpers (known units only; unknown -> signal ignored) ---

SPEED_TO_KPH = {"km/h": 1.0, "kmh": 1.0, "m/s": 3.6, "mph": 1.60934}
POWER_TO_KW = {"kw": 1.0, "w": 0.001}
ENERGY_TO_KWH = {"kwh": 1.0, "wh": 0.001}

_VEHICLE_ATTRS = ("speed", "soc", "drive_energy", "charge_energy",
                  "power", "charging")

TRIP_CMP_FIELDS = ("duration_s", "distance_km", "energy_kwh",
                   "avg_speed_kph", "start_soc", "end_soc")
CHARGE_CMP_FIELDS = ("duration_s", "energy_added_kwh", "avg_power_kw",
                     "max_power_kw", "start_soc", "end_soc")
_CMP_BY_KIND = {"trip": TRIP_CMP_FIELDS, "charge": CHARGE_CMP_FIELDS}


def _num(value):
    """Finite numbers only; anything else becomes None, never TypeError."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        if value != value or value == float("inf") or value == float("-inf"):
            return None
        return value
    if isinstance(value, str):
        try:
            parsed = float(value.strip())
        except ValueError:
            return None
        if parsed != parsed or parsed == float("inf") or parsed == float("-inf"):
            return None
        return parsed
    return None


def to_kph(value, unit):
    num = _num(value)
    if num is None or unit is None:
        return None  # explicit known units only; never guess path-native
    factor = SPEED_TO_KPH.get(str(unit).strip().lower())
    return num * factor if factor is not None else None


def to_kw(value, unit):
    num = _num(value)
    if num is None or unit is None:
        return None
    factor = POWER_TO_KW.get(str(unit).strip().lower())
    return num * factor if factor is not None else None


def to_kwh(value, unit):
    num = _num(value)
    if num is None or unit is None:
        return None
    factor = ENERGY_TO_KWH.get(str(unit).strip().lower())
    return num * factor if factor is not None else None


def _norm_bool(value):
    if value is True or value is False:
        return value
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    low = str(value).strip().lower()
    if low in ("true", "1", "t", "yes", "y"):
        return True
    if low in ("false", "0", "f", "no", "n"):
        return False
    return None


def group_vehicle_rows(cols, rows, paths):
    """Group raw signal rows by (vehicle, source, decode_epoch); never merged
    across sources or epochs. Same (timestamp, path): last row wins. Drive
    and charge energy stay DISTINCT counters; only the six configured paths
    are read, anything else is ignored."""
    idx = {c: i for i, c in enumerate(cols)}
    want = {attr: paths.get(attr) for attr in _VEHICLE_ATTRS}
    known = {p for p in want.values() if p}
    groups = {}

    def empty():
        return {"speed": {}, "soc": {}, "drive_energy": {},
                "charge_energy": {}, "power": {}, "charging": {}}

    for r in rows:
        def col(name):
            return r[idx[name]] if name in idx else None
        p = col("path")
        if p not in known:
            continue
        ts = parse_ts(col("event_time"))
        if not ts:
            continue
        key = (col("vehicle"), col("source"), col("decode_epoch"))
        g = groups.get(key)
        if g is None:
            g = groups[key] = empty()
        if p == want["speed"]:
            g["speed"][ts] = (col("value_num"), col("unit"))
        elif p == want["soc"]:
            g["soc"][ts] = _num(col("value_num"))
        elif p == want["power"]:
            g["power"][ts] = (col("value_num"), col("unit"))
        elif p == want["charging"]:
            g["charging"][ts] = _norm_bool(col("value_bool"))
        elif p == want["drive_energy"]:
            g["drive_energy"][ts] = (col("value_num"), col("unit"))
        elif p == want["charge_energy"]:
            g["charge_energy"][ts] = (col("value_num"), col("unit"))
    return groups


def _converted(mapping, conv):
    """Sorted ([times], [converted values]) for one raw signal map."""
    items = sorted(mapping.items())
    return [t for t, _ in items], [conv(v) if isinstance(v, tuple) else v
                                   for _, v in items]


def _carry(times, vals, ts, max_age_s):
    """Latest observed value, including explicit unknowns, with bounded age."""
    i = bisect_right(times, ts) - 1
    if i < 0:
        return None
    if (ts - times[i]).total_seconds() > max_age_s:
        return None
    return vals[i]


def _trap_distance(pts):
    """Trapezoidal km; explicit unknown samples break the integration path."""
    dist, prev = 0.0, None
    for ts, speed in pts:
        if (prev is not None and ts is not None and prev[0] is not None
                and speed is not None and prev[1] is not None):
            dist += ((speed + prev[1]) / 2.0
                     * (ts - prev[0]).total_seconds() / 3600.0)
        prev = (ts, speed)
    return dist


def _counter_delta(pairs):
    """Delta over sorted [(ts, kwh)]; None when sparse, and None (never a
    guess) when any step decreases (meter reset/wrap)."""
    if len(pairs) < 2:
        return None
    prev = pairs[0][1]
    for _, v in pairs[1:]:
        if v < prev:
            return None
        prev = v
    delta = pairs[-1][1] - pairs[0][1]
    return delta if delta >= 0 else None


def _window_counter_delta(times, vals, start, end, bound_s):
    """A bounded baseline is required; missing samples and resets invalidate it."""
    left = bisect_right(times, start) - 1
    right = bisect_right(times, end)
    if (left < 0 or right - left < 2
            or (start - times[left]).total_seconds() > bound_s
            or (end - times[right - 1]).total_seconds() > bound_s
            or vals[left] is None):
        return None
    previous = vals[left]
    for i in range(left + 1, right):
        value = vals[i]
        if value is None or value < previous:
            return None
        previous = value
    return previous - vals[left]


def _trap_energy(powers):
    """Time-weighted kWh over sorted [(ts, kw)] via trapezoidal integration
    for irregular samples; None when fewer than two samples."""
    if len(powers) < 2:
        return None
    total = 0.0
    for (t0, p0), (t1, p1) in zip(powers, powers[1:]):
        total += (p0 + p1) / 2.0 * (t1 - t0).total_seconds() / 3600.0
    return total if total >= 0 else None


def segment_trips(points, gap_min=10, min_speed=1.0):
    """Integrate observed speed; a prolonged stop ends at its first observation."""
    segs, cur = [], None
    last_moving, first_stop = None, None
    gap_s = gap_min * 60
    for ts, speed, soc in points:
        moving = speed is not None and speed > min_speed
        if cur is not None and (ts - last_moving).total_seconds() > gap_s:
            cur["closed"] = True
            segs.append(cur)
            cur = None
        if moving:
            if cur is None:
                cur = {"start": ts, "pts": [], "start_soc": soc}
            cur["end"], cur["end_soc"] = ts, soc
            cur["pts"].append((ts, speed))
            last_moving, first_stop = ts, None
        elif cur is not None:
            cur["pts"].append((ts, speed))
            if speed is not None and first_stop is None:
                first_stop = ts
                cur["end"], cur["end_soc"] = ts, soc
    if cur is not None:
        segs.append(cur)
    out = []
    for t in segs:
        dist = _trap_distance(p for p in t["pts"] if p[0] <= t["end"])
        dur = (t["end"] - t["start"]).total_seconds()
        out.append({"start": t["start"], "end": t["end"], "duration_s": dur,
                    "distance_km": dist or None,
                    "avg_speed_kph": dist / (dur / 3600.0) if dur and dist else None,
                    "start_soc": t["start_soc"], "end_soc": t["end_soc"],
                    "open": not t.get("closed", False)})
    return out


def segment_charges(points, gap_min=30, min_speed=1.0, power_kw_min=0.1):
    """Meter/power/charging evidence opens a session; SOC alone never does."""
    segs, cur, previous_meter = [], None, None
    gap_s = gap_min * 60
    for ts, soc, power, charging, energy, speed in points:
        moving = speed is not None and speed > min_speed
        gaining = (energy is not None and previous_meter is not None
                   and 0 <= (ts - previous_meter[0]).total_seconds() <= gap_s
                   and energy > previous_meter[1])
        if cur is not None and (ts - cur["last_active"]).total_seconds() > gap_s:
            cur["closed"] = True
            segs.append(cur)
            cur = None
        if moving or charging is False:
            if cur is not None:
                # A real terminal reading belongs to the parked charging
                # session. Never include moving/regen power.
                if not moving and (power is not None or energy is not None):
                    cur["end"], cur["end_soc"] = ts, soc
                    if power is not None:
                        cur["powers"].append((ts, power))
                    if energy is not None:
                        cur["energies"].append((ts, energy))
                cur["closed"] = True
                segs.append(cur)
                cur = None
            previous_meter = None
            continue
        evidence = charging is True or (power is not None and power > power_kw_min) or gaining
        if cur is None and evidence:
            start, start_soc = (previous_meter[0], previous_meter[2]) if gaining else (ts, soc)
            cur = {"start": start, "end": ts, "start_soc": start_soc,
                   "end_soc": soc, "last_active": ts, "quiet_end": False,
                   "powers": [], "energies": []}
            if gaining:
                cur["energies"].append(previous_meter[:2])
        if cur is not None:
            if evidence:
                cur["end"], cur["end_soc"], cur["last_active"] = ts, soc, ts
                cur["quiet_end"] = False
            elif not cur["quiet_end"] and (power is not None or energy is not None):
                cur["end"], cur["end_soc"], cur["quiet_end"] = ts, soc, True
            if power is not None:
                cur["powers"].append((ts, power))
            if energy is not None:
                cur["energies"].append((ts, energy))
        if energy is not None:
            previous_meter = (ts, energy, soc)
    if cur is not None:
        segs.append(cur)
    out = []
    for c in segs:
        dur = (c["end"] - c["start"]).total_seconds()
        powers = [(t, p) for t, p in c["powers"] if t <= c["end"]]
        energies = [(t, e) for t, e in c["energies"] if t <= c["end"]]
        energy = _counter_delta(energies)
        power_e = _trap_energy(powers)
        if energy is None:
            energy = power_e
        avg = power_e / (dur / 3600.0) if power_e is not None and dur else None
        if avg is None and len(powers) == 1:
            avg = powers[0][1]
        out.append({"start": c["start"], "end": c["end"], "duration_s": dur,
                    "start_soc": c["start_soc"], "end_soc": c["end_soc"],
                    "avg_power_kw": avg,
                    "max_power_kw": max(p for _, p in powers) if powers else None,
                    "energy_added_kwh": energy,
                    "open": not c.get("closed", False)})
    return out


def _load_anchors(base_url, auth, db, table, id_col, extra,
                  cutoff_s, seal_s, vehicle, max_rows):
    """Existing summary rows overlapping [cutoff, seal), keyed by
    (vehicle, source, decode_epoch). Low volume: summaries only, never raw."""
    filt = (" AND vehicle = " + str_lit(vehicle)) if vehicle else ""
    try:
        cols, rows = guarded_fetch(
            base_url, auth, db,
            "SELECT started_at, " + id_col + ", vehicle, source,"
            " decode_epoch, ended_at" + extra + " FROM " + table +
            " WHERE started_at < '" + seal_s + "' AND (ended_at IS NULL"
            " OR ended_at >= '" + cutoff_s + "')" + filt, max_rows)
    except SqlError as e:
        if not is_missing_table(e):
            raise
        return {}
    idx = {c: i for i, c in enumerate(cols)}
    names = [c.strip() for c in extra.split(",") if c.strip()]
    out = {}
    for r in rows:
        def col(name):
            return r[idx[name]] if name in idx else None
        start = parse_ts(col("started_at"))
        if not start:
            continue
        rec = {"id": col(id_col), "start": start,
               "end": parse_ts(col("ended_at")) or start}
        for name in names:
            rec[name] = _num(col(name))
        out.setdefault((col("vehicle"), col("source"),
                        col("decode_epoch")), []).append(rec)
    return out


def _num_close(a, b, eps=1e-9):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(a - b) <= eps
    except TypeError:
        return a == b


def _rows_equal(kind, seg, anchor):
    if seg["start"] != anchor["start"] or seg["end"] != anchor["end"]:
        return False
    for field in _CMP_BY_KIND[kind]:
        if not _num_close(seg.get(field), anchor.get(field)):
            return False
    return True


def _segment_pk(key, start):
    return json.dumps([*key, start.isoformat()], separators=(",", ":"))


def _plan_group_writes(kind, key, segs, existing, window_start,
                       window_end, gap_s):
    """Upsert complete heads, retaining open anchors across lookback windows."""
    inserts, deletes, seen = [], [], set()
    for seg in segs:
        if not seg.get("start") or not seg.get("end"):
            continue
        in_window = seg["start"] < window_end and seg["end"] >= window_start
        ov = [e for e in existing
              if e["start"] <= seg["end"] and seg["start"] <= e["end"]]
        if not in_window and not ov:
            continue
        sealed = (not seg.get("open", False)
                  or (window_end - seg["end"]).total_seconds() >= gap_s)
        pk = _segment_pk(key, seg["start"])
        if not ov:
            out = dict(seg)
            out["_pk"] = pk
            out["open"] = not sealed
            inserts.append(out)
            continue
        if sealed and any(e["start"] <= seg["start"] and e["end"] >= seg["end"]
                          and _rows_equal(kind, seg, e) for e in ov):
            continue  # anchor already stores exactly this: keep it
        out = dict(seg)
        out["_pk"] = pk
        out["open"] = not sealed
        inserts.append(out)
        for e in ov:
            if e["id"] not in seen:
                seen.add(e["id"])
                deletes.append(e["id"])
    return inserts, deletes


def _apply_plans(base_url, auth, db, table, pk_col, columns, key,
                 inserts, deletes, row_fn):
    """Upsert every replacement before removing obsolete, group-scoped anchors."""
    v, src, epoch = key
    kept = set()
    for segment in inserts:
        insert_rows(base_url, auth, db, table, columns, [row_fn(segment)])
        kept.add(segment["_pk"])
    for old_id in set(deletes) - kept:
        request_sql(base_url, auth, db,
                    "DELETE FROM " + table + " WHERE " + pk_col + " = " +
                    str_lit(old_id) + " AND vehicle = " + str_lit(v) +
                    " AND source = " + str_lit(src) +
                    " AND decode_epoch = " + str_lit(epoch))
    return len(inserts)


def load_cfg():
    try:
        lookback_h = int(env("AGG_LOOKBACK_HOURS", "30"))
        interval_s = int(env("AGG_INTERVAL_SECONDS", "300"))
        max_rows = int(env("AGG_MAX_ROWS", "200000"))
    except ValueError:
        raise SqlError("AGG_LOOKBACK_HOURS/AGG_INTERVAL_SECONDS/AGG_MAX_ROWS must be integers")
    if min(lookback_h, interval_s, max_rows) <= 0:
        raise SqlError("aggregate intervals and row cap must be positive")
    home_raw_ttl = env("HOME_RAW_TTL", "90d").strip()
    if home_raw_ttl and not TTL_RE.fullmatch(home_raw_ttl):
        raise SqlError("HOME_RAW_TTL must look like 90d/12h (or empty for no TTL)")
    try:
        home = json.loads(env("HOME_AGG_SOURCES", "[]"))
    except (TypeError, ValueError) as error:
        raise SqlError("HOME_AGG_SOURCES must be a JSON list") from error
    if not isinstance(home, list):
        raise SqlError("HOME_AGG_SOURCES must be a JSON list")
    source_keys, table_times = set(), {}
    for source in home:
        if not isinstance(source, dict) or set(source) != {
                "table", "time_col", "entity_col", "value_col"}:
            raise SqlError("each Home source requires table/time_col/entity_col/value_col")
        for key, value in source.items():
            if key == "entity_col" and value == "":
                continue  # explicit whole-table aggregation
            if not isinstance(value, str) or not IDENT_RE.fullmatch(value):
                raise SqlError("invalid Home source identifier for " + key)
        identity = (source["table"], source["value_col"])
        if identity in source_keys:
            raise SqlError("duplicate Home table/value source")
        source_keys.add(identity)
        old_time = table_times.setdefault(source["table"], source["time_col"])
        if old_time != source["time_col"]:
            raise SqlError("Home sources sharing a table must share its time column")
    tuning = {}
    for key, variable, default in (
            ("trip_gap_min", "TRIP_GAP_MINUTES", "10"),
            ("trip_min_speed_kph", "TRIP_MIN_SPEED_KPH", "1"),
            ("charge_gap_min", "CHARGE_GAP_MINUTES", "30"),
            ("signal_max_age_s", "VEHICLE_SIGNAL_MAX_AGE_SECONDS", "300")):
        try:
            value = float(env(variable, default))
        except ValueError as error:
            raise SqlError(variable + " must be positive and finite") from error
        if not 0 < value < float("inf"):
            raise SqlError(variable + " must be positive and finite")
        tuning[key] = value
    otel_ttl = env("OTEL_TTL", "").strip()
    if otel_ttl and not TTL_RE.match(otel_ttl):
        raise SqlError("OTEL_TTL must look like 90d/12h (or empty for no TTL)")
    # Battery-only validation belongs to its isolated section, not startup.
    battery_lookback_h = env("BATTERY_LOOKBACK_HOURS", "").strip() or lookback_h
    battery_max_rows = env("BATTERY_MAX_ROWS", "").strip() or max_rows
    return {
        "base_url": env("GREPTIME_HTTP_URL", "http://greptimedb:4000"),
        "db": env("GREPTIME_DB", "datalake"),
        "user": env("GREPTIME_USER", "datalake"),
        "password": env("GREPTIME_PASSWORD", ""),
        "lookback_h": lookback_h,
        "interval_s": interval_s,
        "max_rows": max_rows,
        "otel_ttl": otel_ttl,
        "home_raw_ttl": home_raw_ttl,
        "battery_lookback_h": battery_lookback_h,
        "battery_max_rows": battery_max_rows,
        "battery_config": env("BATTERY_ANALYSIS_CONFIG", "/app/battery-analysis.json"),
        "battery_config_explicit": bool(env("BATTERY_ANALYSIS_CONFIG", "")),
        "battery_backfill_start": env("BATTERY_BACKFILL_START", ""),
        "battery_backfill_end": env("BATTERY_BACKFILL_END", ""),
        **tuning,
        "vehicle": env("VEHICLE_ID", ""),
        "paths": {
            "speed": env("VEHICLE_SPEED_PATH", "") or "Vehicle.Speed",
            "soc": env("VEHICLE_SOC_PATH", "") or "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
            "drive_energy": env("VEHICLE_DRIVE_ENERGY_PATH", ""),
            "charge_energy": env("VEHICLE_CHARGE_ENERGY_PATH", ""),
            "power": env("VEHICLE_CHARGING_POWER_PATH", ""),
            "charging": env("VEHICLE_CHARGING_PATH", "") or "",
        },
        "home": home,
    }


def coverage_min(base_url, auth, db, table, col, extra=""):
    """Oldest retained timestamp, or None when the table is empty."""
    cols, rows = fetch_rows(
        base_url, auth, db,
        "SELECT MIN(" + col + ") FROM " + table + extra)
    if not rows or not rows[0] or rows[0][0] is None:
        return None
    return parse_ts(rows[0][0])


def guarded_fetch(base_url, auth, db, stmt, max_rows):
    cols, rows = fetch_rows(base_url, auth, db, stmt.rstrip().rstrip(";") +
                            " LIMIT " + str(max_rows + 1))
    if len(rows) > max_rows:
        raise SqlError("row cap exceeded (%d > %d): refusing partial write"
                       % (len(rows), max_rows))
    return cols, rows


def ttl_boundary(ttl, now=None):
    """Oldest fully retained instant for a configured OTEL_TTL. Empty TTL
    means unbounded retention (None). Same Ns/Nm/Nh/Nd contract as
    db_init, so aggregate never guesses a window db_init did not set."""
    if not ttl:
        return None
    if not TTL_RE.match(ttl):
        raise SqlError("OTEL_TTL must look like 90d/12h (or empty for no TTL)")
    if int(ttl[:-1]) == 0:
        return None  # Greptime 0s means no expiry, not immediate expiry.
    now = now or utcnow()
    return now - dt.timedelta(seconds=int(ttl[:-1]) * TTL_UNIT_S[ttl[-1]])


def fully_retained(start, ttl, now=None, boundary=None):
    """Do not replace a persisted window from partly expired raw data."""
    bound = boundary if boundary is not None else ttl_boundary(ttl, now)
    if bound is None or start is None:
        return True if start is not None else False
    return start >= bound


def retention_boundary(ctx, source, ttl, now):
    """Increasing TTL cannot restore rows already expired under an older policy."""
    base_url, auth, db = ctx
    _, rows = fetch_rows(base_url, auth, db,
                        "SELECT MAX(deleted_before) FROM raw_retention_watermark"
                        " WHERE source = " + str_lit(source))
    previous = parse_ts(rows[0][0]) if rows and rows[0] else None
    current = ttl_boundary(ttl, now)
    if current is None or (previous is not None and previous >= current):
        return previous
    # One monotonic watermark per source/day. Record before deletion so a
    # crash cannot lose the evidence needed to protect long-term summaries.
    insert_rows(base_url, auth, db, "raw_retention_watermark",
                ["day_start", "source", "deleted_before"],
                [[ts_lit(floor_day(now)), str_lit(source), ts_lit(current)]])
    return current


ACTIVITY_INSTRUMENTS = frozenset({
    "claude_code.lines_of_code.count", "claude_code.code_edit_tool.decision",
    "claude_code.commit.count", "claude_code.pull_request.count",
    "claude_code.active_time.total", "coding_agent.lines_of_code.count",
    "coding_agent.code_edit_tool.decision", "coding_agent.commit.count",
    "coding_agent.pull_request.count",
})
ACTIVITY_FIELDS = ACTIVITY_COUNT_FIELDS + ACTIVITY_VALUE_FIELDS


def activity_metric_rows(ctx, cfg):
    """Discover declared OTLP names; Prometheus unit/suffix translation varies.
    Client mapping mirrors activity.py: explicit columns win verbatim,
    otherwise only source-verified service names map (arbitrary names stay
    provenance, never identity)."""
    base_url, auth, db = ctx
    _, catalog = fetch_rows(base_url, auth, db,
        "SELECT table_name, create_options FROM information_schema.tables"
        " WHERE table_schema = " + str_lit(db) +
        " AND create_options LIKE '%greptime.semantic.metric.original_name=%'")
    out = []
    for table, options in catalog:
        meta = dict(part.split("=", 1) for part in (options or "").split() if "=" in part)
        instrument = meta.get("greptime.semantic.metric.original_name")
        if instrument not in ACTIVITY_INSTRUMENTS:
            continue
        cols, rows = guarded_fetch(base_url, auth, db,
            'SELECT * FROM "' + table.replace('"', '""') + '"', cfg["max_rows"])
        for values in rows:
            row = dict(zip(cols, values))
            explicit = row.get("coding_agent_client") or row.get("client")
            service = row.get("service_name")
            if explicit:
                client = explicit
            elif instrument.startswith(_CODEX_PREFIX):
                # ponytail: codex.* instrument is the producer marker and wins
                # over any service.name (Codex service.name is overridable,
                # even to another known name).
                client = "codex"
            elif service in _NATIVE_SERVICE_CLIENTS:
                client = _NATIVE_SERVICE_CLIENTS[service]
            else:
                client = "unknown"
            value = row.get("greptime_value")
            if instrument == "claude_code.active_time.total":
                unit = meta.get("greptime.semantic.metric.unit", "")
                factor = {"s": 1, "ms": .001, "us": .000001, "ns": .000000001,
                          "min": 60, "h": 3600}.get(unit)
                # Missing/unsupported units cannot establish seconds.
                value = value * factor if value is not None and factor is not None else None
            out.append({
                "instrument": instrument, "client": client,
                "client_explicit": bool(explicit),
                "session_id": row.get("coding_agent_session_id") or row.get("session_id")
                    or row.get("conversation_id") or row.get("gen_ai_conversation_id"),
                "timestamp": parse_ts(row.get("greptime_timestamp")), "value": value,
                "temporality": to_int(row.get("datalake_temporality")),
                "start_ns": to_int(row.get("datalake_start_time_unix_nano")),
                "type": row.get("type"), "decision": row.get("decision"),
                "tool_name": row.get("tool_name") or row.get("gen_ai_tool_name"),
                "coding_agent.repository.id": row.get("coding_agent_repository_id"),
                "vcs.ref.head.name": row.get("vcs_ref_head_name"),
                "stream": tuple(sorted((k, v) for k, v in row.items() if k not in (
                    "greptime_timestamp", "greptime_value", "datalake_temporality",
                    "datalake_start_time_unix_nano"))),
            })
            if len(out) > cfg["max_rows"]:
                raise SqlError("activity metric row cap exceeded: refusing partial write")
    return out


def ai_section(ctx, cfg):
    base_url, auth, db = ctx
    ttl = cfg.get("otel_ttl", "")
    cols, rows = guarded_fetch(
        base_url, auth, db,
        "SELECT " + ", ".join('"' + c + '"' for c in SPAN_COLS) +
        " FROM opentelemetry_traces", cfg["max_rows"])
    spans = dedupe_spans(cols, rows)
    # Native log usage (Codex sse_event completed, Claude api_request) is
    # folded in here so dashboards count real billed calls, not traces-only
    # usage. Same stable (client, session, call) id on both signals bills
    # once with native usage winning; the merged billed set replaces the
    # trace-only one below so overrides (not just appends) reach summaries.
    try:
        lcols, lrows = guarded_fetch(
            base_url, auth, db,
            "SELECT " + ", ".join('"' + c + '"' for c in LOG_COLS) +
            " FROM opentelemetry_logs", cfg["max_rows"])
        logs = dedupe_logs(lcols, lrows)
        raw_logs = [dict(zip(lcols, row)) for row in lrows]
    except SqlError as e:
        if not is_missing_table(e):
            raise
        logs = []
        raw_logs = []
    billed = billable(spans)
    merged, _ = merge_native(billed, logs)
    bset = {id(s) for s in billed}
    spans = [s for s in spans if id(s) not in bset] + merged
    now = utcnow()
    bound = retention_boundary(ctx, "otel", ttl, now)
    total = 0
    # One registry fetch per pass at most (itself daily-cached in memory):
    # aggregation never fails for pricing; None keeps legacy NULLs.
    prices = load_price_table()
    activity_sessions, activity_days = summarize_activity(
        [dict(zip(cols, row)) for row in rows], raw_logs, activity_metric_rows(ctx, cfg))
    sessions = {(s["client"], s["session_id"]): s for s in summarize_sessions(spans, prices)}
    for key, activity in activity_sessions.items():
        s = sessions.setdefault(key, {"client": key[0], "session_id": key[1]})
        starts = [v for v in (s.get("session_start"), activity.get("start")) if v is not None]
        ends = [v for v in (s.get("session_end"), activity.get("end")) if v is not None]
        if starts:
            s["session_start"] = min(starts)
        if ends:
            s["session_end"] = max(ends)
        if starts and ends:
            s["duration_s"] = (max(ends) - min(starts)).total_seconds()
        s.update({field: activity.get(field) for field in ACTIVITY_FIELDS})
        s.update({field: activity.get(field) for field in (
            "repo", "branch", "outcome", "activity_sources")})
    _, old_rows = guarded_fetch(base_url, auth, db,
        "SELECT client, session_id, session_start FROM ai_session_summary", cfg["max_rows"])
    expired = {(client, sid) for client, sid, start in old_rows
               if not fully_retained(parse_ts(start), ttl, now, bound)}
    session_cols = ["session_start", "session_id", "client", "provider", "models",
                    "model_count", "session_end", "duration_s", "input_tokens",
                    "output_tokens", "cache_read_tokens", "cache_write_tokens",
                    "reasoning_tokens", "total_tokens", "cost_usd", "cost_source",
                    "cost_estimated_usd", "cost_unpriced_calls",
                    "llm_spans", "tool_calls", "error_count"] + list(ACTIVITY_FIELDS) + [
                    "repo", "branch", "outcome", "activity_sources"]
    pending = []
    for key, s in sessions.items():
        if key in expired or not fully_retained(s.get("session_start"), ttl, now, bound):
            continue  # Stored anchors also detect an already-expired raw head.
        pending.append((s, [ts_lit(s["session_start"]), str_lit(s["session_id"]),
                      str_lit(s["client"]), str_lit(s.get("provider")),
                      str_lit(json.dumps(s["models"])) if s.get("models") else "NULL",
                      num_lit(s.get("model_count")), ts_lit(s.get("session_end")),
                      num_lit(s.get("duration_s")), num_lit(s.get("input")),
                      num_lit(s.get("output")), num_lit(s.get("cache_read")),
                      num_lit(s.get("cache_write")), num_lit(s.get("reasoning")),
                      num_lit(s.get("total")), num_lit(s.get("cost")),
                      str_lit(s.get("cost_source")), num_lit(s.get("cost_estimated_usd")),
                      num_lit(s.get("cost_unpriced_calls")), num_lit(s.get("llm_spans")),
                      num_lit(s.get("tool_calls")), num_lit(s.get("errors"))] +
                     [num_lit(s.get(field)) for field in ACTIVITY_FIELDS] +
                     [str_lit(s.get(field)) for field in ("repo", "branch", "outcome")] +
                     [str_lit(json.dumps(s.get("activity_sources") or {}, sort_keys=True))]))
    if pending:
        # All eligible session batches land before any scoped stale-start
        # delete: a failed batch keeps old anchors (restart converges).
        insert_rows(base_url, auth, db, "ai_session_summary",
                    session_cols, [row for _, row in pending])
        for s, _ in pending:
            request_sql(base_url, auth, db,
                        "DELETE FROM ai_session_summary WHERE session_id = " +
                        str_lit(s["session_id"]) + " AND client = " +
                        str_lit(s["client"]) + " AND session_start != " + ts_lit(s["session_start"]))
        total += len(pending)
    activity_cols = ["day_start", "client"] + list(ACTIVITY_FIELDS) + ["activity_sources"]
    activity_rows = []
    for day in activity_days:
        if bound and day["day_start"] < bound:
            continue
        activity_rows.append([ts_lit(day["day_start"]), str_lit(day["client"])] +
                     [num_lit(day.get(field)) for field in ACTIVITY_FIELDS] +
                     [str_lit(json.dumps(day.get("activity_sources") or {}, sort_keys=True))])
    if activity_rows:
        insert_rows(base_url, auth, db, "ai_activity_daily",
                    activity_cols, activity_rows)
        total += len(activity_rows)
    daily_cols = ["day_start", "client", "provider", "model", "input_tokens",
                     "output_tokens", "cache_read_tokens", "cache_write_tokens",
                     "reasoning_tokens", "total_tokens", "cost_usd", "cost_source",
                     "cost_estimated_usd", "cost_unpriced_calls",
                     "llm_spans", "tool_calls", "active_sessions", "error_count"]
    daily_rows = []
    for d in summarize_daily(spans, prices):
        if bound and d["day"] < bound:
            continue  # partly expired day: keep the old row
        daily_rows.append([ts_lit(d["day"]), str_lit(d["client"]), str_lit(d["provider"]),
                      str_lit(d["model"]), num_lit(d["input"]), num_lit(d["output"]),
                      num_lit(d["cache_read"]), num_lit(d["cache_write"]),
                      num_lit(d["reasoning"]), num_lit(d["total"]), num_lit(d["cost"]),
                      str_lit(d["cost_source"]), num_lit(d.get("cost_estimated_usd")),
                      num_lit(d.get("cost_unpriced_calls")), num_lit(d["llm_spans"]),
                      num_lit(d["tool_calls"]), num_lit(d["sessions"]),
                      num_lit(d["errors"])])
    if daily_rows:
        insert_rows(base_url, auth, db, "ai_daily_summary",
                    daily_cols, daily_rows)
        total += len(daily_rows)
    tool_cols = ["day_start", "tool_name", "client", "calls", "errors", "avg_duration_ms"]
    tool_rows = []
    for t in summarize_tools(spans):
        if bound and t["day"] < bound:
            continue  # partly expired day: keep the old row
        tool_rows.append([ts_lit(t["day"]), str_lit(t["tool"]), str_lit(t["client"]),
                      num_lit(t["calls"]), num_lit(t["errors"]), num_lit(t["avg_ms"])])
    if tool_rows:
        insert_rows(base_url, auth, db, "ai_tool_daily",
                    tool_cols, tool_rows)
        total += len(tool_rows)
    return total


def log_section(ctx, cfg):
    base_url, auth, db = ctx
    cutoff = floor_day(utcnow() - dt.timedelta(hours=cfg["lookback_h"]))
    cols, rows = guarded_fetch(
        base_url, auth, db,
        "SELECT timestamp, severity_text, severity_number, scope_name"
        " FROM opentelemetry_logs WHERE timestamp >= '" +
        cutoff.strftime("%Y-%m-%d %H:%M:%S") + "'", cfg["max_rows"])
    total = 0
    bound = retention_boundary(ctx, "otel", cfg.get("otel_ttl", ""), utcnow())
    for g in summarize_logs(cols, rows):
        if not fully_retained(g["day"], cfg.get("otel_ttl", ""), boundary=bound):
            continue
        insert_rows(base_url, auth, db, "ai_log_daily",
                    ["day_start", "scope_name", "severity_text", "log_count",
                     "error_count"],
                    [[ts_lit(g["day"]), str_lit(g["scope"]), str_lit(g["severity"]),
                      num_lit(g["count"]), num_lit(g["errors"])]])
        total += 1
    return total


def vehicle_section(ctx, cfg):
    base_url, auth, db = ctx
    now = utcnow()
    cov = coverage_min(base_url, auth, db, "vehicle_signal", "event_time")
    aligned = floor_hour(now - dt.timedelta(hours=cfg["lookback_h"]))
    if cov:
        aligned = max(aligned, floor_hour(cov))
    cut = aligned.strftime("%Y-%m-%d %H:%M:%S")
    seal_m = floor_minute(now).strftime("%Y-%m-%d %H:%M:%S")
    seal_h = floor_hour(now).strftime("%Y-%m-%d %H:%M:%S")
    request_sql(base_url, auth, db, vehicle_agg_sql("1m", "60s", cut, seal_m), timeout=120)
    request_sql(base_url, auth, db, vehicle_agg_sql("1h", "1h", cut, seal_h), timeout=120)
    return 2  # two resolutions refreshed


def _extend_vehicle_head(ctx, key, segments, raw_from, cutoff, gap_s, lookback_h):
    """Recover a cut head in bounded steps; never relabel a tail as a new trip."""
    if (not segments or segments[0]["start"] >= raw_from + dt.timedelta(seconds=gap_s)
            or (segments[0]["end"] < cutoff and not segments[0].get("open"))):
        return None
    base_url, auth, db = ctx
    _, rows = fetch_rows(
        base_url, auth, db, "SELECT MIN(event_time) FROM vehicle_signal WHERE vehicle = "
        + str_lit(key[0]) + " AND source = " + str_lit(key[1])
        + " AND decode_epoch = " + str_lit(key[2]))
    first = parse_ts(rows[0][0]) if rows and rows[0] else None
    if first is None or first >= raw_from:
        return None  # The complete available source history is already loaded.
    return max(first, raw_from - dt.timedelta(hours=lookback_h))


def trip_section(ctx, cfg):
    """Release each query window before extending a cut head further back."""
    total, raw_floor = 0, None
    while True:
        written, raw_floor = _trip_window(ctx, cfg, raw_floor)
        total += written
        if raw_floor is None:
            return total


def _trip_window(ctx, cfg, raw_floor):
    """Source/epoch-isolated sessions with durable open anchors."""
    base_url, auth, db = ctx
    paths = cfg.get("paths", {})
    now = utcnow()
    cutoff = floor_hour(now - dt.timedelta(hours=cfg["lookback_h"]))
    seal = floor_minute(now)
    cut = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    seals = seal.strftime("%Y-%m-%d %H:%M:%S")
    filt_vehicle = cfg.get("vehicle")
    trip_gap = cfg.get("trip_gap_min", 10)
    min_speed = cfg.get("trip_min_speed_kph", 1.0)
    charge_gap = cfg.get("charge_gap_min", 30)
    max_age = cfg.get("signal_max_age_s", 300)
    context_s = max(trip_gap, charge_gap) * 60 + max_age
    trips_ex = _load_anchors(
        base_url, auth, db, "trip_summary", "trip_id",
        ", duration_s, distance_km, energy_kwh, avg_speed_kph,"
        " start_soc, end_soc",
        cut, seals, filt_vehicle, cfg["max_rows"])
    charges_ex = _load_anchors(
        base_url, auth, db, "charge_session", "session_id",
        ", duration_s, energy_added_kwh, start_soc, end_soc,"
        " avg_power_kw, max_power_kw",
        cut, seals, filt_vehicle, cfg["max_rows"])
    raw_from = cutoff - dt.timedelta(seconds=context_s)
    if raw_floor is not None:
        raw_from = min(raw_from, raw_floor)
    for by_group in (trips_ex, charges_ex):
        for anchors in by_group.values():
            for a in anchors:
                if a["start"] < cutoff:
                    cand = a["start"] - dt.timedelta(seconds=context_s)
                    if cand < raw_from:
                        raw_from = cand
    filt = (" AND vehicle = " + str_lit(filt_vehicle)) if filt_vehicle else ""
    wanted = [p for p in (paths.get(a) for a in _VEHICLE_ATTRS) if p]
    if not wanted:
        return 0, None  # no paths configured: never emit empty IN ()
    cols, rows = guarded_fetch(
        base_url, auth, db,
        "SELECT event_time, vehicle, path, source, decode_epoch, value_num,"
        " value_bool, unit FROM vehicle_signal"
        " WHERE event_time >= '" + raw_from.strftime("%Y-%m-%d %H:%M:%S")
        + "' AND event_time < '" + seals + "' AND path IN ("
        + ", ".join(str_lit(p) for p in wanted) + ")"
        + filt + " ORDER BY event_time", cfg["max_rows"])
    groups = group_vehicle_rows(cols, rows, paths)
    total = 0
    for (v, src, epoch), g in groups.items():
        key = (v, src, epoch)
        speed_t, speed_v = _converted(g["speed"], lambda p: to_kph(*p))
        soc_t, soc_v = _converted(g["soc"], lambda x: x)
        pow_t, pow_v = _converted(g["power"], lambda p: to_kw(*p))
        chg_t, chg_v = _converted(g["charge_energy"],
                                 lambda p: to_kwh(*p))
        cg_t, cg_v = _converted(g["charging"], lambda x: x)
        ct, cv = soc_t, soc_v
        if any(s is not None for s in speed_v):
            pts = [(t, speed, _carry(ct, cv, t, max_age))
                   for t, speed in zip(speed_t, speed_v)]
            drive_t, drive_v = _converted(g["drive_energy"],
                                         lambda p: to_kwh(*p))
            bound_s = trip_gap * 60 + max_age
            segs = segment_trips(pts, gap_min=trip_gap, min_speed=min_speed)
            earlier = _extend_vehicle_head(
                ctx, key, segs, raw_from, cutoff, trip_gap * 60, cfg["lookback_h"])
            if earlier is not None:
                return total, earlier
            for seg in segs:
                seg["energy_kwh"] = _window_counter_delta(
                    drive_t, drive_v, seg["start"], seg["end"], bound_s)

            def trip_row(s):
                return [ts_lit(s["start"]), str_lit(s["_pk"]),
                        str_lit(v), str_lit(src), str_lit(epoch),
                        ts_lit(None if s["open"] else s["end"]), num_lit(s["duration_s"]),
                        num_lit(s["distance_km"]), num_lit(s["energy_kwh"]),
                        num_lit(s["avg_speed_kph"]),
                        num_lit(s["start_soc"]), num_lit(s["end_soc"])]

            inserts, deletes = _plan_group_writes(
                "trip", key, segs, trips_ex.get(key, []), cutoff, seal, trip_gap * 60)
            total += _apply_plans(
                base_url, auth, db, "trip_summary", "trip_id",
                ["started_at", "trip_id", "vehicle", "source", "decode_epoch",
                 "ended_at", "duration_s", "distance_km", "energy_kwh",
                 "avg_speed_kph", "start_soc", "end_soc"],
                key, inserts, deletes, trip_row)
        if soc_t or pow_t or chg_t or cg_t:
            st, sv = speed_t, speed_v
            gt, gv = cg_t, cg_v
            pmap = dict(zip(pow_t, pow_v))
            emap = dict(zip(chg_t, chg_v))
            times = sorted(set(soc_t) | set(pow_t) | set(chg_t)
                           | set(cg_t) | set(speed_t))
            cpts = [(t, _carry(ct, cv, t, max_age), pmap.get(t),
                     _carry(gt, gv, t, max_age), emap.get(t),
                     _carry(st, sv, t, max_age)) for t in times]
            segs = segment_charges(cpts, gap_min=charge_gap,
                                   min_speed=min_speed)
            earlier = _extend_vehicle_head(
                ctx, key, segs, raw_from, cutoff, charge_gap * 60, cfg["lookback_h"])
            if earlier is not None:
                return total, earlier

            def charge_row(s):
                return [ts_lit(s["start"]), str_lit(s["_pk"]),
                        str_lit(v), str_lit(src), str_lit(epoch),
                        ts_lit(None if s["open"] else s["end"]), num_lit(s["duration_s"]),
                        num_lit(s["energy_added_kwh"]),
                        num_lit(s["start_soc"]), num_lit(s["end_soc"]),
                        num_lit(s["avg_power_kw"]),
                        num_lit(s["max_power_kw"])]

            inserts, deletes = _plan_group_writes(
                "charge", key, segs, charges_ex.get(key, []), cutoff, seal, charge_gap * 60)
            total += _apply_plans(
                base_url, auth, db, "charge_session", "session_id",
                ["started_at", "session_id", "vehicle", "source",
                 "decode_epoch", "ended_at", "duration_s",
                 "energy_added_kwh", "start_soc", "end_soc",
                 "avg_power_kw", "max_power_kw"],
                key, inserts, deletes, charge_row)
    return total, None


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def home_section(ctx, cfg):
    base_url, auth, db = ctx
    now = utcnow()
    expiry = ttl_boundary(cfg["home_raw_ttl"], now)
    epoch = dt.datetime(1970, 1, 1)
    total = 0
    tables = {}
    for source in cfg["home"]:
        table, tcol, ecol, vcol = (source[key] for key in
                                  ("table", "time_col", "entity_col", "value_col"))
        try:
            cov = coverage_min(base_url, auth, db, table, tcol)
        except SqlError as error:
            if is_missing_table(error):
                continue  # optional producer has not sent its first sample
            raise
        if cov is None:
            continue
        bound = retention_boundary(ctx, table, cfg["home_raw_ttl"], now)
        for resolution, interval, seconds in (("5m", "5m", 300),
                                             ("1h", "1h", 3600), ("1d", "1d", 86400)):
            step = dt.timedelta(seconds=seconds)
            start = max(cov, now - dt.timedelta(hours=cfg["lookback_h"]))
            cut = epoch + ((start - epoch) // step) * step
            if bound is not None:
                retained = epoch + ((bound - epoch) // step) * step
                if retained < bound:
                    retained += step
                cut = max(cut, retained)
            seal = epoch + ((now - epoch) // step) * step
            if cut >= seal:
                continue
            request_sql(base_url, auth, db,
                home_agg_sql(table, tcol, ecol, vcol, resolution, interval,
                             cut.strftime("%Y-%m-%d %H:%M:%S"),
                             seal.strftime("%Y-%m-%d %H:%M:%S")), timeout=120)
            total += 1
        tables[table] = tcol
    # Metric-engine logical tables cannot ALTER TTL in Greptime 1.2.
    # Apply raw retention only after every configured source has rolled up.
    if expiry is not None:
        for table, tcol in tables.items():
            request_sql(base_url, auth, db,
                "DELETE FROM " + table + " WHERE " + tcol + " < " + ts_lit(expiry), timeout=120)
    return total


def battery_section(ctx, cfg):
    """Run pure battery analyzers into vehicle_analysis. Import is lazy so
    unit tests importing aggregate never require sibling analyzer files;
    missing battery_runtime fails the section (loud), never silent zeros."""
    import importlib
    runtime = importlib.import_module("scripts.analytics.battery.battery_runtime")
    return runtime.run_battery(ctx, cfg)


STATUS_PATH = "/ops/aggregate-status.json"


def write_status(status, path=STATUS_PATH):
    """Atomic public operational state; never SQL, credentials, or row identities."""
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".aggregate-status-", dir=directory)
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(status, handle, allow_nan=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read_status(path=STATUS_PATH):
    try:
        with open(path) as handle:
            state = json.load(handle)
        return state if isinstance(state, dict) else {}
    except (OSError, ValueError):
        return {}


def startup_failure(path=STATUS_PATH):
    status = read_status(path)
    completed = time.time()
    status.update(timestamp_seconds=completed, running=0, success=0,
                  last_failure_timestamp_seconds=completed,
                  failed_sections=["startup"], section="")
    write_status(status, path)


def vehicle_window_lag(ctx):
    """Observed event-window gap, not execution age or receipt/ingest lag."""
    base_url, auth, db = ctx
    _, raw = fetch_rows(base_url, auth, db,
                        "SELECT source, MAX(event_time) FROM vehicle_signal"
                        " WHERE value_num IS NOT NULL GROUP BY source")
    _, summaries = fetch_rows(base_url, auth, db,
                              "SELECT source, MAX(window_start) FROM vehicle_agg"
                              " WHERE resolution='1m' GROUP BY source")
    ends = {source: parse_ts(ts) for source, ts in summaries}
    gaps = {}
    for source, ts in raw:
        latest, window = parse_ts(ts), ends.get(source)
        if source and latest is not None and window is not None:
            gaps[source] = max(0, (latest - window).total_seconds() - 60)
    return gaps


def run_pass(ctx, cfg, path=STATUS_PATH):
    previous = read_status(path)
    status = {key: previous[key] for key in (
        "last_success_timestamp_seconds", "last_failure_timestamp_seconds",
        "success") if key in previous}
    status.update(timestamp_seconds=time.time(), running=1,
                  last_start_timestamp_seconds=time.time(),
                  interval_seconds=cfg["interval_s"])
    write_status(status, path)

    def progress(section):
        status.update(timestamp_seconds=time.time(), section=section)
        write_status(status, path)

    try:
        counts, failed = run_all(ctx, cfg, progress)
        try:
            status["vehicle_window_lag_seconds"] = vehicle_window_lag(ctx)
            status["vehicle_window_observation_timestamp_seconds"] = time.time()
            status["vehicle_window_observation_success"] = 1
        except Exception:
            status["vehicle_window_observation_success"] = 0
        return counts, failed
    except Exception:
        failed = ["pass"]
        raise
    finally:
        completed = time.time()
        status.update(timestamp_seconds=completed, running=0,
                      success=int(not failed), failed_sections=failed, section="")
        status["last_failure_timestamp_seconds" if failed else
               "last_success_timestamp_seconds"] = completed
        write_status(status, path)


def run_all(ctx, cfg, progress=None):
    """One pass over every section. Returns (rows, failed)."""
    base_url, auth, db = ctx
    _ = (base_url, auth, db)
    failed = []
    counts = {}

    def run_section(name, fn):
        if progress is not None:
            progress(name)
        try:
            n = fn()
            counts[name] = n
            if name != "battery":  # battery reports ok/partial_errors/all_error itself
                sys.stdout.write("aggregate: " + name + ": ok (" + str(n) + " rows)\n")
        except SqlError as e:
            if is_missing_table(e):
                sys.stdout.write("aggregate: " + name + ": skipped (no source table yet)\n")
                counts[name] = 0
            else:
                sys.stderr.write("aggregate: error: " + name + ": " + str(e) + "\n")
                failed.append(name)
        except Exception as e:
            sys.stderr.write("aggregate: error: " + name + ": " + str(e)[:200] + "\n")
            failed.append(name)

    run_section("ai", lambda: ai_section(ctx, cfg))
    run_section("logs", lambda: log_section(ctx, cfg))
    run_section("vehicle", lambda: vehicle_section(ctx, cfg))
    run_section("trip_charge", lambda: trip_section(ctx, cfg))
    run_section("home", lambda: home_section(ctx, cfg))
    run_section("battery", lambda: battery_section(ctx, cfg))
    return counts, failed


def main():
    try:
        cfg = load_cfg()
    except SqlError as e:
        sys.stderr.write("aggregate: error: " + str(e) + "\n")
        startup_failure()
        return 1
    if not cfg["password"]:
        sys.stderr.write("aggregate: error: GREPTIME_PASSWORD is required\n")
        startup_failure()
        return 1
    auth = base64.b64encode((cfg["user"] + ":" + cfg["password"]).encode()).decode("ascii")
    ctx = (cfg["base_url"], auth, cfg["db"])

    # Wait briefly for SQL (never assume process-up == ready).
    deadline = time.monotonic() + 120
    while True:
        try:
            request_sql(cfg["base_url"], auth, cfg["db"], "SELECT 1", timeout=10)
            break
        except SqlError as e:
            if time.monotonic() >= deadline:
                sys.stderr.write("aggregate: error: db not ready: " + str(e) + "\n")
                startup_failure()
                return 2
            time.sleep(3)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    if env("AGG_RUN_ONCE", "") == "1":
        _, failed = run_pass(ctx, cfg)
        return 3 if failed else 0
    while not STOP:
        _, failed = run_pass(ctx, cfg)
        if failed:
            sys.stderr.write("aggregate: pass had failures: " + ",".join(failed) + "\n")
        for _ in range(cfg["interval_s"]):
            if STOP:
                break
            time.sleep(1)
    sys.stdout.write("aggregate: stopping\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
