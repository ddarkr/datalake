#!/usr/bin/env python3
"""Idempotent GreptimeDB schema bootstrap using only the Python standard library.

Reads GREPTIME_HTTP_URL / GREPTIME_DB / GREPTIME_USER / GREPTIME_PASSWORD from
the environment, waits until a real SQL query succeeds (never treats a bare
process-up probe as ready), then applies CREATE DATABASE / CREATE TABLE with
IF NOT EXISTS so repeated runs preserve data. Any SQL-level error fails the
process with a nonzero exit code. Secrets are never printed.
"""

import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from activity import ACTIVITY_COUNT_FIELDS, ACTIVITY_VALUE_FIELDS, ACTIVITY_TRACE_ATTRIBUTES

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TTL_RE = re.compile(r"^[0-9]+[smhd]$")
# Shared activity count/value fields and trace attribute keys. The raw
# repository URL is never stored; ingest hashes it into
# coding_agent.repository.id before the durable queue.
_ACTIVITY_INT_ATTRS = frozenset({
    "coding_agent.session.subagent_count",
    "coding_agent.session.lines.added",
    "coding_agent.session.lines.removed",
    "coding_agent.session.lines.accepted",
    "coding_agent.session.lines.rejected",
    "coding_agent.session.edit.accept_count",
    "coding_agent.session.edit.reject_count",
    "coding_agent.session.commit_count",
    "coding_agent.session.pr_count",
    "coding_agent.session.duration_ms",
    "coding_agent.subagent.duration_ms",
    "coding_agent.edit.lines.added",
    "coding_agent.edit.lines.removed",
    "duration_ms",
    "total_tool_uses",
    "event.sequence",
})
_ACTIVITY_BOOL_ATTRS = frozenset({"is_built_in", "is_async"})
_ACTIVITY_SKIP_TRACE_ATTRS = frozenset({"service.name", "service_name"})


def _activity_col_type(key):
    if key in _ACTIVITY_INT_ATTRS:
        return "Int64"
    if key in _ACTIVITY_BOOL_ATTRS:
        return "Boolean"
    return "STRING"


def trace_activity_columns():
    """Flat span_attributes.<key> DDL lines for the shared activity allowlist."""
    lines = []
    seen = set()
    for key in ACTIVITY_TRACE_ATTRIBUTES:
        if key in _ACTIVITY_SKIP_TRACE_ATTRS or key in seen:
            continue
        seen.add(key)
        lines.append('  "span_attributes.' + key + '" ' + _activity_col_type(key) + ' NULL,')
    return "\n".join(lines) + "\n"


def session_activity_columns():
    """Nullable v4 session summary DDL lines (counts, seconds, repo/branch/outcome/provenance)."""
    lines = []
    for field in ACTIVITY_COUNT_FIELDS:
        lines.append('  "' + field + '" Int64 NULL,')
    for field in ACTIVITY_VALUE_FIELDS:
        lines.append('  "' + field + '" Float64 NULL,')
    lines.extend((
        '  "repo" STRING NULL,',
        '  "branch" STRING NULL,',
        '  "outcome" STRING NULL,',
        '  "activity_sources" JSON NULL,',
    ))
    return "\n".join(lines) + "\n"


def daily_activity_ddl():
    """Vendor-neutral ai_activity_daily: TIME INDEX day_start, PK client."""
    cols = [
        '  "day_start" TIMESTAMP(9) NOT NULL TIME INDEX,',
        '  "client" STRING NOT NULL,',
    ]
    for field in ACTIVITY_COUNT_FIELDS:
        cols.append('  "' + field + '" Int64 NULL,')
    for field in ACTIVITY_VALUE_FIELDS:
        cols.append('  "' + field + '" Float64 NULL,')
    cols.append('  "activity_sources" JSON NULL,')
    return 'CREATE TABLE IF NOT EXISTS "ai_activity_daily" (\n' + "\n".join(cols) + '\n  PRIMARY KEY ("client")\n)'


class SqlError(Exception):
    pass


def fail(msg, code=1):
    sys.stderr.write("db_init: error: " + msg + "\n")
    raise SystemExit(code)


def qident(name):
    return '"' + name.replace('"', '""') + '"'


def env(name, default=""):
    return os.environ.get(name, default)


def sql_request(base_url, auth, db, stmt, timeout=15):
    """POST one SQL statement. Returns decoded JSON. Raises SqlError (no secrets)."""
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
        raise SqlError("http status " + str(e.code) + ": " + raw[:500])
    except Exception as e:
        raise SqlError("request failed: " + str(e)[:300])
    try:
        payload = json.loads(raw)
    except Exception:
        raise SqlError("non-JSON response: " + raw[:300])
    if isinstance(payload, dict):
        if "error" in payload and payload["error"]:
            raise SqlError(str(payload.get("error"))[:500])
        if "code" in payload and isinstance(payload["code"], int) and payload["code"] != 0:
            raise SqlError("code=" + str(payload["code"]) + " " + str(payload.get("error", ""))[:300])
        if "output" not in payload:
            raise SqlError("unexpected response shape: " + raw[:300])
    return payload


def wait_ready(base_url, auth, db, deadline_s):
    """Poll with a real query until SQL succeeds. Returns True, else False."""
    deadline = time.monotonic() + deadline_s
    attempt = 0
    while True:
        attempt += 1
        try:
            sql_request(base_url, auth, db, "SELECT 1", timeout=10)
            return True
        except SqlError as e:
            msg = str(e)
            # Auth/config errors never become ready by waiting.
            low = msg.lower()
            if "auth" in low or "password" in low or "unauthorized" in low or "forbidden" in low:
                raise
            if time.monotonic() >= deadline:
                sys.stderr.write("db_init: readiness timed out: " + msg + "\n")
                return False
            time.sleep(min(5.0, max(1.0, attempt * 0.5)))


def ddl_statements(otel_ttl):
    """Returns [(label, sql)]. All idempotent; no DROP/TRUNCATE/DELETE anywhere."""
    with_opts = " WITH (append_mode = 'true')"
    if otel_ttl:
        with_opts = " WITH (append_mode = 'true', ttl = '" + otel_ttl + "')"
    ddls = []
    # OTel traces: core greptime_trace_v1 columns plus the privacy-allowlisted
    # flattened attribute columns dashboards aggregate on. The pipeline adds any
    # further attributes by schema evolution; pre-creating the allowlist keeps
    # Grafana queries off missing-column errors before first telemetry lands.
    traces_head = """CREATE TABLE IF NOT EXISTS "opentelemetry_traces" (
  "timestamp" TIMESTAMP(9) NOT NULL TIME INDEX,
  "timestamp_end" TIMESTAMP(9) NULL,
  "duration_nano" UInt64 NULL,
  "parent_span_id" STRING NULL,
  "trace_id" STRING NULL,
  "span_id" STRING NULL,
  "span_kind" STRING NULL,
  "span_name" STRING NULL,
  "span_status_code" STRING NULL,
  "span_status_message" STRING NULL,
  "trace_state" STRING NULL,
  "scope_name" STRING NULL,
  "scope_version" STRING NULL,
  "service_name" STRING NULL,
  "span_attributes.gen_ai.operation.name" STRING NULL,
  "span_attributes.gen_ai.provider.name" STRING NULL,
  "span_attributes.gen_ai.request.model" STRING NULL,
  "span_attributes.gen_ai.response.model" STRING NULL,
  "span_attributes.gen_ai.response.id" STRING NULL,
  "span_attributes.gen_ai.usage.input_tokens" Int64 NULL,
  "span_attributes.gen_ai.usage.output_tokens" Int64 NULL,
  "span_attributes.gen_ai.usage.cache_read.input_tokens" Int64 NULL,
  "span_attributes.gen_ai.usage.cache_creation.input_tokens" Int64 NULL,
  "span_attributes.gen_ai.usage.cache_write.input_tokens" Int64 NULL,
  "span_attributes.gen_ai.usage.reasoning.output_tokens" Int64 NULL,
  "span_attributes.gen_ai.tool.name" STRING NULL,
  "span_attributes.gen_ai.tool.call.id" STRING NULL,
  "span_attributes.coding_agent.content_capture_mode" STRING NULL,
  "span_attributes.coding_agent.signal_source" STRING NULL,
"""
    traces_tail = """  "span_attributes.cost_usd" Float64 NULL,
  "span_attributes.pi.gen_ai.cost.estimated_usd" Float64 NULL,
  "span_attributes.pi.gen_ai.cost.source" STRING NULL,
  "span_attributes.error.type" STRING NULL,
  "resource_attributes.service.version" STRING NULL,
  "resource_attributes.host.name" STRING NULL,
  "span_events" JSON NULL,
  "span_links" JSON NULL,
  PRIMARY KEY ("service_name")
)"""
    ddls.append(("opentelemetry_traces", traces_head + trace_activity_columns() + traces_tail + with_opts))
    ddls.append(("opentelemetry_logs", """CREATE TABLE IF NOT EXISTS "opentelemetry_logs" (
  "timestamp" TIMESTAMP(9) NOT NULL TIME INDEX,
  "trace_id" STRING NULL,
  "span_id" STRING NULL,
  "severity_text" STRING NULL,
  "severity_number" Int32 NULL,
  "body" STRING NULL,
  "log_attributes" JSON NULL,
  "trace_flags" UInt32 NULL,
  "scope_name" STRING NULL,
  "scope_version" STRING NULL,
  "scope_attributes" JSON NULL,
  "scope_schema_url" STRING NULL,
  "resource_attributes" JSON NULL,
  "resource_schema_url" STRING NULL,
  PRIMARY KEY ("scope_name")
)""" + with_opts))
    # Canonical vehicle signals. Retry reuses the same event_id/event_time, so
    # at-least-once redelivery merges instead of duplicating. decode_epoch is
    # part of the key so a re-decode under a new epoch keeps history.
    ddls.append(("vehicle_signal", """CREATE TABLE IF NOT EXISTS "vehicle_signal" (
  "event_time" TIMESTAMP(9) NOT NULL TIME INDEX,
  "vehicle" STRING NOT NULL,
  "path" STRING NOT NULL,
  "source" STRING NOT NULL,
  "event_id" STRING NOT NULL,
  "decode_epoch" STRING NOT NULL,
  "value_num" Float64 NULL,
  "value_text" STRING NULL,
  "value_bool" Boolean NULL,
  "unit" STRING NULL,
  "vss_version" STRING NULL,
  "vehicle_firmware" STRING NULL,
  "dbc_primary_commit" STRING NULL,
  "dbc_supplemental_commit" STRING NULL,
  "dbc_override_version" STRING NULL,
  "dbc_override_commit" STRING NULL,
  "mapping_revision" STRING NULL,
  "collector_version" STRING NULL,
  "ingest_time" TIMESTAMP(9) NULL,
  "source_system" STRING NULL,
  "source_field" STRING NULL,
  "collector_id" STRING NULL,
  "source_is_resend" Boolean NULL,
  "quality" STRING NULL,
  "envelope_id" STRING NULL,
  "config_version" STRING NULL,
  "connectivity" STRING NULL,
  PRIMARY KEY ("vehicle", "path", "source", "event_id", "decode_epoch")
)"""))
    # Observed events (NOT episodes: each row is one observed event; a
    # derived episode links members via episode_id). event_id is
    # writer-supplied stable identity (decode_epoch included when re-decoded,
    # so a re-decode keeps history); body content is never stored, only
    # whether a body was observed (body_redacted). Quality is per-event
    # ('unknown_start' for missing-start warnings, 'invalid' or
    # 'range_rejected' tombstones, NULL otherwise). All dashboard fields are
    # flat typed columns; no JSON is needed to render them.
    ddls.append(("vehicle_event", """CREATE TABLE IF NOT EXISTS "vehicle_event" (
  "event_time" TIMESTAMP(9) NOT NULL TIME INDEX,
  "vehicle" STRING NOT NULL,
  "event_type" STRING NOT NULL,
  "name" STRING NOT NULL,
  "source" STRING NOT NULL,
  "event_id" STRING NOT NULL,
  "ingest_time" TIMESTAMP(9) NULL,
  "envelope_id" STRING NULL,
  "started_at" TIMESTAMP(9) NULL,
  "ended_at" TIMESTAMP(9) NULL,
  "duration_s" Float64 NULL,
  "audience" STRING NULL,
  "is_active" Boolean NULL,
  "body_redacted" Boolean NULL,
  "source_system" STRING NULL,
  "decode_epoch" STRING NULL,
  "collector_id" STRING NULL,
  "episode_id" STRING NULL,
  "quality" STRING NULL,
  "config_version" STRING NULL,
  "connectivity" STRING NULL,
  PRIMARY KEY ("vehicle", "event_type", "name", "source", "event_id")
)"""))
    # Battery analysis output. "metric" is quoted: unquoted it is rejected
    # by the Greptime parser as a reserved word. Default last_row merge
    # makes reruns idempotent: same key + window_start overwrites (a failed
    # run writes nothing, so stale values are never refreshed). A new
    # revision keeps history alongside the old one. Writers must supply
    # window_start (TIME INDEX is NOT NULL); unavailable rows carry the
    # evidence/run time with value NULL. value_text carries text-only
    # metrics (episodes/method descriptions); uncertainty_lower/upper bound
    # them when actually computed. computed_at selects the authoritative
    # latest revision downstream (Greptime PKs cannot express it).
    ddls.append(("vehicle_analysis", """CREATE TABLE IF NOT EXISTS "vehicle_analysis" (
  "window_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "vehicle" STRING NOT NULL,
  "metric" STRING NOT NULL,
  "source" STRING NOT NULL,
  "analysis_id" STRING NOT NULL,
  "revision" STRING NOT NULL,
  "value" Float64 NULL,
  "value_text" STRING NULL,
  "unit" STRING NULL,
  "status" STRING NULL,
  "reason" STRING NULL,
  "window_end" TIMESTAMP(9) NULL,
  "decode_epoch" STRING NULL,
  "evidence_count" Int64 NULL,
  "sample_count" Int64 NULL,
  "coverage_ratio" Float64 NULL,
  "algorithm_version" STRING NULL,
  "calibration_version" STRING NULL,
  "model_version" STRING NULL,
  "uncertainty" Float64 NULL,
  "uncertainty_lower" Float64 NULL,
  "uncertainty_upper" Float64 NULL,
  "computed_at" TIMESTAMP(9) NULL,
  "quality" STRING NULL,
  "config_version" STRING NULL,
  "connectivity" STRING NULL,
  "episode_id" STRING NULL,
  PRIMARY KEY ("vehicle", "metric", "source", "analysis_id", "revision")
)"""))
    # Derived AI summaries: no TTL so raw-data expiry never silently drops
    # long-term aggregates. Default last_row merge makes scheduled backfills
    # idempotent (same key + time overwrites).
    session_head = """CREATE TABLE IF NOT EXISTS "ai_session_summary" (
  "session_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "session_id" STRING NOT NULL,
  "client" STRING NOT NULL,
  "provider" STRING NULL,
  "models" JSON NULL,
  "model_count" Int64 NULL,
  "session_end" TIMESTAMP(9) NULL,
  "duration_s" Float64 NULL,
  "input_tokens" Int64 NULL,
  "output_tokens" Int64 NULL,
  "cache_read_tokens" Int64 NULL,
  "cache_write_tokens" Int64 NULL,
  "reasoning_tokens" Int64 NULL,
  "total_tokens" Int64 NULL,
  "cost_usd" Float64 NULL,
  "cost_source" STRING NULL,
  "cost_estimated_usd" Float64 NULL,
  "cost_unpriced_calls" Int64 NULL,
  "llm_spans" Int64 NULL,
  "tool_calls" Int64 NULL,
  "error_count" Int64 NULL,
"""
    session_tail = """  PRIMARY KEY ("session_id", "client")
)"""
    ddls.append(("ai_session_summary", session_head + session_activity_columns() + session_tail))
    ddls.append(("ai_activity_daily", daily_activity_ddl()))
    ddls.append(("ai_daily_summary", """CREATE TABLE IF NOT EXISTS "ai_daily_summary" (
  "day_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "client" STRING NOT NULL,
  "provider" STRING NOT NULL,
  "model" STRING NOT NULL,
  "input_tokens" Int64 NULL,
  "output_tokens" Int64 NULL,
  "cache_read_tokens" Int64 NULL,
  "cache_write_tokens" Int64 NULL,
  "reasoning_tokens" Int64 NULL,
  "total_tokens" Int64 NULL,
  "cost_usd" Float64 NULL,
  "cost_source" STRING NULL,
  "cost_estimated_usd" Float64 NULL,
  "cost_unpriced_calls" Int64 NULL,
  "llm_spans" Int64 NULL,
  "tool_calls" Int64 NULL,
  "active_sessions" Int64 NULL,
  "error_count" Int64 NULL,
  PRIMARY KEY ("client", "provider", "model")
)"""))
    ddls.append(("ai_tool_daily", """CREATE TABLE IF NOT EXISTS "ai_tool_daily" (
  "day_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "tool_name" STRING NOT NULL,
  "client" STRING NOT NULL,
  "calls" Int64 NULL,
  "errors" Int64 NULL,
  "avg_duration_ms" Float64 NULL,
  PRIMARY KEY ("tool_name", "client")
)"""))
    ddls.append(("ai_log_daily", """CREATE TABLE IF NOT EXISTS "ai_log_daily" (
  "day_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "scope_name" STRING NOT NULL,
  "severity_text" STRING NOT NULL,
  "log_count" Int64 NULL,
  "error_count" Int64 NULL,
  PRIMARY KEY ("scope_name", "severity_text")
)"""))
    ddls.append(("vehicle_agg", """CREATE TABLE IF NOT EXISTS "vehicle_agg" (
  "window_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "resolution" STRING NOT NULL,
  "vehicle" STRING NOT NULL,
  "path" STRING NOT NULL,
  "source" STRING NOT NULL,
  "decode_epoch" STRING NOT NULL,
  "avg_value" Float64 NULL,
  "min_value" Float64 NULL,
  "max_value" Float64 NULL,
  "sample_count" Int64 NULL,
  "unit" STRING NULL,
  PRIMARY KEY ("resolution", "vehicle", "path", "source", "decode_epoch")
)"""))
    # Trip = drive cycle on speed presence with a gap split; charge = energy
    # gain while parked/charging. Tables are filled by aggregate.py only when
    # the required VSS paths exist; otherwise they stay empty (never zero-
    # filled) and dashboards fall back to raw vehicle_signal queries.
    ddls.append(("trip_summary", """CREATE TABLE IF NOT EXISTS "trip_summary" (
  "started_at" TIMESTAMP(9) NOT NULL TIME INDEX,
  "trip_id" STRING NOT NULL,
  "vehicle" STRING NOT NULL,
  "source" STRING NOT NULL,
  "decode_epoch" STRING NOT NULL,
  "ended_at" TIMESTAMP(9) NULL,
  "duration_s" Float64 NULL,
  "distance_km" Float64 NULL,
  "energy_kwh" Float64 NULL,
  "avg_speed_kph" Float64 NULL,
  "start_soc" Float64 NULL,
  "end_soc" Float64 NULL,
  PRIMARY KEY ("trip_id")
)"""))
    ddls.append(("charge_session", """CREATE TABLE IF NOT EXISTS "charge_session" (
  "started_at" TIMESTAMP(9) NOT NULL TIME INDEX,
  "session_id" STRING NOT NULL,
  "vehicle" STRING NOT NULL,
  "source" STRING NOT NULL,
  "decode_epoch" STRING NOT NULL,
  "ended_at" TIMESTAMP(9) NULL,
  "duration_s" Float64 NULL,
  "energy_added_kwh" Float64 NULL,
  "start_soc" Float64 NULL,
  "end_soc" Float64 NULL,
  "avg_power_kw" Float64 NULL,
  "max_power_kw" Float64 NULL,
  PRIMARY KEY ("session_id")
)"""))
    ddls.append(("home_agg", """CREATE TABLE IF NOT EXISTS "home_agg" (
  "window_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "resolution" STRING NOT NULL,
  "entity" STRING NOT NULL,
  "source" STRING NOT NULL,
  "avg_value" Float64 NULL,
  "min_value" Float64 NULL,
  "max_value" Float64 NULL,
  "sample_count" Int64 NULL,
  PRIMARY KEY ("resolution", "entity", "source")
)"""))
    # Telegraf adds numeric payload columns on first arrival. The protocol
    # envelope exists from boot so an idle MQTT source stays query-safe.
    ddls.append(("mqtt_consumer", """CREATE TABLE IF NOT EXISTS "mqtt_consumer" (
  "greptime_timestamp" TIMESTAMP(9) NOT NULL TIME INDEX,
  "host" STRING NULL,
  "topic" STRING NULL,
  PRIMARY KEY ("host", "topic")
)"""))
    ddls.append(("raw_retention_watermark", """CREATE TABLE IF NOT EXISTS "raw_retention_watermark" (
  "day_start" TIMESTAMP(9) NOT NULL TIME INDEX,
  "source" STRING NOT NULL,
  "deleted_before" TIMESTAMP(9) NOT NULL,
  PRIMARY KEY ("source")
)"""))
    return ddls


def alter_statements():
    """Non-destructive ADD COLUMN repairs for pre-existing tables."""
    stmts = [
        'ALTER TABLE "opentelemetry_traces" ADD COLUMN IF NOT EXISTS'
        ' "span_attributes.cost_usd" Float64',
        'ALTER TABLE "opentelemetry_logs" ADD COLUMN IF NOT EXISTS'
        ' "trace_flags" UInt32',
        'ALTER TABLE "ai_daily_summary" ADD COLUMN IF NOT EXISTS'
        ' "cost_source" STRING',
        'ALTER TABLE "ai_daily_summary" ADD COLUMN IF NOT EXISTS'
        ' "cost_estimated_usd" Float64',
        'ALTER TABLE "ai_daily_summary" ADD COLUMN IF NOT EXISTS'
        ' "cost_unpriced_calls" Int64',
        'ALTER TABLE "ai_session_summary" ADD COLUMN IF NOT EXISTS'
        ' "cost_estimated_usd" Float64',
        'ALTER TABLE "ai_session_summary" ADD COLUMN IF NOT EXISTS'
        ' "cost_unpriced_calls" Int64',
        'ALTER TABLE "ai_daily_summary" ADD COLUMN IF NOT EXISTS'
        ' "error_count" Int64',
        'ALTER TABLE "vehicle_agg" ADD COLUMN IF NOT EXISTS'
        ' "decode_epoch" STRING',
        'ALTER TABLE "trip_summary" ADD COLUMN IF NOT EXISTS'
        ' "source" STRING',
        'ALTER TABLE "trip_summary" ADD COLUMN IF NOT EXISTS'
        ' "decode_epoch" STRING',
        'ALTER TABLE "charge_session" ADD COLUMN IF NOT EXISTS'
        ' "source" STRING',
        'ALTER TABLE "charge_session" ADD COLUMN IF NOT EXISTS'
        ' "decode_epoch" STRING',
    ]
    for name, coltype in (
        ("dbc_override_version", "STRING"),
        ("dbc_override_commit", "STRING"),
        ("source_system", "STRING"),
        ("source_field", "STRING"),
        ("collector_id", "STRING"),
        ("source_is_resend", "Boolean"),
        ("quality", "STRING"),
        ("envelope_id", "STRING"),
        ("config_version", "STRING"),
        ("connectivity", "STRING"),
    ):
        stmts.append('ALTER TABLE "vehicle_signal" ADD COLUMN IF NOT EXISTS'
                     ' "' + name + '" ' + coltype)
    for name, coltype in (
        ("episode_id", "STRING"),
        ("quality", "STRING"),
        ("config_version", "STRING"),
        ("connectivity", "STRING"),
    ):
        stmts.append('ALTER TABLE "vehicle_event" ADD COLUMN IF NOT EXISTS'
                     ' "' + name + '" ' + coltype)
    for name, coltype in (
        ("value_text", "STRING"),
        ("uncertainty_lower", "Float64"),
        ("uncertainty_upper", "Float64"),
        ("computed_at", "TIMESTAMP(9)"),
        ("quality", "STRING"),
        ("config_version", "STRING"),
        ("connectivity", "STRING"),
    ):
        stmts.append('ALTER TABLE "vehicle_analysis" ADD COLUMN IF NOT EXISTS'
                     ' "' + name + '" ' + coltype)
    # v4 activity repairs: mirror session_activity_columns() plus the flat
    # trace activity attributes for tables created before this change.
    for field in ACTIVITY_COUNT_FIELDS:
        stmts.append('ALTER TABLE "ai_session_summary" ADD COLUMN IF NOT EXISTS'
                     ' "' + field + '" Int64')
    for field in ACTIVITY_VALUE_FIELDS:
        stmts.append('ALTER TABLE "ai_session_summary" ADD COLUMN IF NOT EXISTS'
                     ' "' + field + '" Float64')
    for name, coltype in (("repo", "STRING"), ("branch", "STRING"),
                          ("outcome", "STRING"), ("activity_sources", "JSON"),
                          ("models", "JSON")):
        stmts.append('ALTER TABLE "ai_session_summary" ADD COLUMN IF NOT EXISTS'
                     ' "' + name + '" ' + coltype)
    seen = set()
    for key in ACTIVITY_TRACE_ATTRIBUTES:
        if key in _ACTIVITY_SKIP_TRACE_ATTRS or key in seen:
            continue
        seen.add(key)
        stmts.append('ALTER TABLE "opentelemetry_traces" ADD COLUMN IF NOT EXISTS'
                     ' "span_attributes.' + key + '" ' + _activity_col_type(key))
    if "coding_agent.repository.id" not in seen:
        stmts.append('ALTER TABLE "opentelemetry_traces" ADD COLUMN IF NOT EXISTS'
                     ' "span_attributes.coding_agent.repository.id" STRING')
    return stmts


def main():
    base_url = env("GREPTIME_HTTP_URL", "http://greptimedb:4000")
    db = env("GREPTIME_DB", "datalake")
    user = env("GREPTIME_USER", "datalake")
    password = env("GREPTIME_PASSWORD", "")
    try:
        wait_s = int(env("DB_INIT_WAIT_S", "300"))
    except ValueError:
        fail("DB_INIT_WAIT_S must be an integer")
    otel_ttl = env("OTEL_TTL", "").strip()
    if not password:
        fail("GREPTIME_PASSWORD is required", code=1)
    if not IDENT_RE.match(db):
        fail("invalid GREPTIME_DB identifier", code=1)
    if not IDENT_RE.match(user):
        fail("invalid GREPTIME_USER identifier", code=1)
    if otel_ttl and not TTL_RE.match(otel_ttl):
        fail("OTEL_TTL must look like 90d/12h (or empty for no TTL)", code=1)

    auth = base64.b64encode((user + ":" + password).encode("utf-8")).decode("ascii")
    if not wait_ready(base_url, auth, "public", wait_s):
        fail("greptimedb not SQL-ready in time", code=2)
    try:
        sql_request(base_url, auth, "public", "CREATE DATABASE IF NOT EXISTS " + qident(db))
    except SqlError as e:
        fail("create database failed: " + str(e), code=3)
    ddls = ddl_statements(otel_ttl)
    for label, stmt in ddls:
        try:
            sql_request(base_url, auth, db, stmt, timeout=30)
            # CREATE IF NOT EXISTS does not update retention. Explicit 0s
            # also prevents long-term tables inheriting a database-wide TTL.
            ttl = (otel_ttl or "0s") if label in (
                "opentelemetry_traces", "opentelemetry_logs") else "0s"
            sql_request(base_url, auth, db,
                        "ALTER TABLE " + qident(label) + " SET 'ttl'='" + ttl + "'",
                        timeout=30)
        except SqlError as e:
            fail("ddl failed for table " + label + ": " + str(e), code=3)
    # CREATE IF NOT EXISTS does not repair columns. Conditional ALTER handles
    # existing columns itself; all other errors must fail initialization.
    for stmt in alter_statements():
        try:
            sql_request(base_url, auth, db, stmt, timeout=30)
        except SqlError as e:
            fail("alter failed: " + str(e), code=3)
    # Verify every table answers a query (works on empty tables too).
    for label, _ in ddls:
        try:
            sql_request(base_url, auth, db, "SELECT 1 FROM " + qident(label) + " LIMIT 1")
        except SqlError as e:
            fail("verify failed for table " + label + ": " + str(e), code=3)
    sys.stdout.write("db_init: ok: database " + db + " tables=" + str(len(ddls)) + "\n")


if __name__ == "__main__":
    main()
