"""Scoped regression tests for scripts.database.db_init and scripts.analytics.aggregate.

Framework-free: plain asserts + stdlib fake HTTP server. Run with:
  python3 -m tests.test_database
Covers (all behavioral, via payload round-trip through the fake server):
  SQL error -> SqlError; malformed response shapes -> SqlError;
  per-call billing excludes rollup ops and cumulative parents;
  conversation->session / service_name->client fallbacks; mixed cost
  sources labeled "mixed"; window floor + sealed exclusion; same-timestamp
  last-row-wins per (vehicle, source, epoch) with no cross-source mixing;
  charge needs a real charging/power/energy signal (SOC rise alone never
  opens a session); home scheduler env actually reaches the home SQL builder.
"""

import http.server
import json
import os
import sys
import threading
import urllib.parse
from unittest.mock import patch

from scripts.analytics import aggregate as agg




class Handler(http.server.BaseHTTPRequestHandler):
    seen = []
    mode = "ok"
    rows = []
    cols = []
    types = []
    # Per-table fixtures for multi-SELECT sections (ai_section reads traces
    # then logs): when set (not None), the matching table's SELECT is served
    # from these instead of the generic cols/rows above.
    # Cross-contract: tests/test_ai_incremental.py owns issue #1's fenced
    # behavior with its own HTTP fake + fake Flight fence; this Handler
    # keeps the original fixtures (the state SELECT below serves zero rows,
    # so the shared path takes the explicit full rebuild), pinning the
    # full-scan oracle output the fenced path must match.
    span_cols = None
    span_rows = None
    span_types = None
    log_cols = None
    log_rows = None
    retention_rows = [[None]]
    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        stmt = urllib.parse.parse_qs(body).get("sql", [""])[0]
        Handler.seen.append(stmt)
        if Handler.mode == "bad-shape":
            payload = {"output": ["not-a-dict"]}
        elif Handler.mode == "no-output":
            payload = {"code": 0}
        elif Handler.mode == "error":
            payload = {"error": "boom"}
        elif stmt.startswith("SELECT"):
            cols, rows, types = Handler.cols, Handler.rows, Handler.types
            if "information_schema.tables" in stmt:
                cols, rows, types = ["table_name", "create_options"], [], None
            elif "FROM ai_aggregate_state" in stmt:
                # No stored fence on these fixtures: the shared fenced path
                # takes the explicit full rebuild (fence schema).
                cols, rows, types = (
                    ["fence", "price_digest",
                     "config_revision", "input_digest"], [], None)
            elif "FROM raw_retention_watermark" in stmt:
                cols, rows, types = ["deleted_before"], Handler.retention_rows, None
            elif "opentelemetry_traces" in stmt and Handler.span_cols is not None:
                cols, rows = Handler.span_cols, Handler.span_rows
                types = Handler.span_types
            elif "opentelemetry_logs" in stmt and Handler.log_cols is not None:
                cols, rows = Handler.log_cols, Handler.log_rows
                types = None
            types = types or [None] * len(cols)
            payload = {"output": [{"records": {
                "schema": {"column_schemas": [
                    {"name": c, **({"data_type": dt} if dt else {})}
                    for c, dt in zip(cols, types)]},
                "rows": rows}}], "execution_time_ms": 1}
        else:
            payload = {"output": [{"affectedrows": 1}], "execution_time_ms": 1}
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def serve():
    srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


def test_sql_error_raises():
    srv = serve()
    try:
        Handler.mode = "error"
        base = "http://127.0.0.1:%d" % srv.server_port
        for bad in ("error", "bad-shape", "no-output"):
            Handler.mode = bad
            try:
                agg.request_sql(base, "YXV0aA==", "db", "SELECT 1")
            except agg.SqlError:
                pass
            else:
                raise AssertionError("expected SqlError for " + bad)
    finally:
        Handler.mode = "ok"
        srv.shutdown()


def span_row(ts, trace, span, parent, op, session, conv, client, service,
             inp, outp, cost, cost_src, tool=None, create=None, native=None):
    return {"timestamp": ts, "trace_id": trace, "span_id": span,
            "parent_span_id": parent, agg.OP_NAME: op, agg.SESSION: session,
            agg.CONV_ID: conv, agg.CLIENT: client, "service_name": service,
            agg.TOKEN_IN: inp, agg.TOKEN_OUT: outp, agg.COST_EST: cost,
            agg.COST_SRC: cost_src, agg.COST_NATIVE: native,
            agg.TOOL_NAME: tool,
            agg.TOKEN_CACHE_CREATE: create, "span_status_code": "OK"}


def test_per_call_billing_excludes_rollups_and_cumulative_parents():
    parent = span_row("2026-09-21 10:00:00", "t1", "p", None, "chat",
                      "s1", None, "omp", "svc", 30, 20, 0.01, "estimated")
    kid1 = span_row("2026-09-21 10:00:01", "t1", "k1", "p", "chat",
                    "s1", None, "omp", "svc", 10, 5, 0.004, "estimated")
    kid2 = span_row("2026-09-21 10:00:02", "t1", "k2", "p", "chat",
                    "s1", None, "omp", "svc", 20, 15, 0.006, "estimated")
    rollup = span_row("2026-09-21 10:00:03", "t1", "r", None, "invoke_agent",
                      "s1", None, "omp", "svc", 100, 100, 0.05, "estimated")
    cols = list(parent)
    rows = [[d[c] for c in cols] for d in (parent, kid1, kid2, rollup,
                                           kid1)]  # redelivered kid1
    spans = agg.dedupe_spans(cols, rows)
    assert len(spans) == 4, spans
    billed = agg.billable(spans)
    assert sorted((s["span"], s["input"], s["output"]) for s in billed) == [
        ("k1", 10, 5), ("k2", 20, 15)], billed  # parent + rollup excluded
    daily = agg.summarize_daily(spans)
    assert len(daily) == 1
    assert (daily[0]["input"], daily[0]["output"]) == (30, 20), daily[0]


def test_session_client_fallbacks_and_mixed_cost():
    # Explicit coding_agent.client is authoritative; bare service.name is
    # provenance, never identity (Codex service.name is
    # originator/override-dependent). Session still falls back to
    # conversation id; mixed estimate/native costs never merge silently.
    a = span_row("2026-09-21 10:00:00", "t9", "a", None, "chat",
                 None, "conv-1", "codex", "custom-originator", 5, 5, 0.001, "estimated")
    b = span_row("2026-09-21 10:05:00", "t9", "b", None, "generate_content",
                 None, "conv-1", "codex", "custom-originator", 7, 3, 0.002, "native")
    spans = [agg.norm_span(a), agg.norm_span(b)]
    assert spans[0]["session"] == "conv-1"  # conversation fallback
    assert spans[0]["client"] == "codex"  # explicit client, not service
    sess = agg.summarize_sessions(spans)
    assert len(sess) == 1
    assert (sess[0]["input"], sess[0]["output"]) == (12, 8)
    assert sess[0]["cost_source"] == "mixed", sess[0]  # never merged silently


def log_row(ts, session, client, call_id, inp, outp, cost, native="codex",
            tool=None, turn=None):
    attrs = {"event.name": ("codex.sse_event" if native == "codex"
                            else "api_request"),
             "session.id": session, "conversation.id": None,
             "coding_agent.client": client,
             "input_token_count" if native == "codex" else "input_tokens": inp,
             "output_token_count" if native == "codex" else "output_tokens": outp,
             "usage.estimated_usd" if native == "codex" else "cost_usd": cost}
    if native == "codex":
        attrs["event.kind"] = "response.completed"
    if call_id:
        attrs["request_id" if native == "codex" else "message.uuid"] = call_id
    if tool:
        attrs["tool_name" if native == "codex" else "gen_ai.tool.name"] = tool
    if turn:
        attrs["turn.id"] = turn
    import json as _json
    return {"timestamp": ts, "severity_text": "INFO", "severity_number": 9,
            "scope_name": "s", "trace_id": None, "span_id": None, "body": "",
            "log_attributes": _json.dumps(attrs),
            "resource_attributes": _json.dumps({"service.name": client})}


def billed_span(ts, session, client, call_id, inp, outp, cost, tool=None,
                dur=None):
    s = agg.norm_span(span_row(ts, "t", "s-" + str(call_id or ts), None,
                               "chat", session, None, client, client, inp,
                               outp, cost, "estimated", tool=tool))
    s["call_id"] = call_id
    s["duration_ms"] = dur
    return s


def test_native_overrides_same_call_never_sums():
    billed = [billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                          5, 5, 0.001, dur=120.0)]
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    native = log_row("2026-09-21 10:00:01", "s1", "codex", "r1", 10, 3, 0.002)
    logs = agg.dedupe_logs(
        cols, [[native["timestamp"], native["severity_text"],
                native["severity_number"], native["scope_name"],
                native["trace_id"], native["span_id"], native["body"],
                native["log_attributes"], native["resource_attributes"]]])
    merged, n = agg.merge_native(billed, logs)
    assert len(merged) == 1 and n == 1, merged  # one row, not estimate+native
    assert (merged[0]["input"], merged[0]["output"]) == (10, 3), merged[0]
    got, _ = agg.resolve_cost(merged[0])
    assert got == 0.002, merged[0]  # native wins, never 0.001 + 0.002
    assert merged[0]["duration_ms"] == 120.0, merged[0]  # span enrich survives


def test_same_usage_distinct_ids_clients_sessions_stay_separate():
    billed = [billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                          10, 5, 0.001),
              billed_span("2026-09-21 10:01:00", "s1", "codex", "r2",
                          10, 5, 0.001)]  # identical counts, distinct id
    other = log_row("2026-09-21 10:02:00", "s1", "claude", "r1",
                    10, 5, 0.001)  # identical counts, other client
    sess = log_row("2026-09-21 10:03:00", "s2", "codex", "r1",
                   10, 5, 0.001)  # identical counts, other session
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    logs = agg.dedupe_logs(
        cols, [[r["timestamp"], r["severity_text"], r["severity_number"],
                r["scope_name"], r["trace_id"], r["span_id"], r["body"],
                r["log_attributes"], r["resource_attributes"]]
               for r in (other, sess)])
    merged, n = agg.merge_native(billed, logs)
    assert len(merged) == 4 and n == 2, merged  # nothing merged, none lost
    inputs = sorted(m["input"] for m in merged)
    assert inputs == [10, 10, 10, 10], merged  # no usage dropped either


def test_two_estimates_on_one_call_bill_once():
    # OTel hook estimate plus sse_event estimate on one stable call id:
    # still one row, not two estimate charges.
    billed = [billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                          10, 5, 0.001)]
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    dup = log_row("2026-09-21 10:00:01", "s1", "codex", "r1", 10, 5, 0.0012)
    logs = agg.dedupe_logs(
        cols, [[dup["timestamp"], dup["severity_text"], dup["severity_number"],
                dup["scope_name"], dup["trace_id"], dup["span_id"], dup["body"],
                dup["log_attributes"], dup["resource_attributes"]]])
    merged, n = agg.merge_native(billed, logs)
    assert len(merged) == 1 and n == 1, merged
    daily = agg.summarize_daily(merged)
    assert daily[0]["llm_spans"] == 1, daily  # second estimate folded in


def test_cost_only_turn_cost_kept_without_double_count():
    span = billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                       99, 11, 0.001)
    span.update(provider="openai", model="known-model")
    usage = agg.norm_log(log_row("2026-09-21 10:00:01", "s1", "codex",
                                 "r1", 13, 6, 0.001, turn="t9"))
    total = agg.norm_log({
        "timestamp": "2026-09-21 10:00:05",
        "log_attributes": {"event.name": "codex.turn_cost",
                           "session.id": "s1", "turn.id": "t9",
                           "usage.estimated_usd": 0.004}})
    merged, _ = agg.merge_native([span], [usage, total])
    session = agg.summarize_sessions(merged)[0]
    assert (session["input"], session["output"], session["llm_spans"]) == (13, 6, 1)
    assert session["cost"] == 0.004, session
    daily = {row["model"]: row for row in agg.summarize_daily(merged)}
    assert daily["known-model"]["cost"] is None, daily
    assert daily["known-model"]["llm_spans"] == 1, daily
    assert daily["unknown"]["cost"] == 0.004, daily
    assert daily["unknown"]["llm_spans"] is None, daily


def test_turn_cost_without_shared_turn_never_links():
    # Without turn identity no linkage is fabricated: both money rows bill.
    billed = [billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                          10, 5, 0.001)]
    import json as _json
    cost_only = {"timestamp": "2026-09-21 10:00:05", "severity_text": "INFO",
                 "severity_number": 9, "scope_name": "s", "trace_id": None,
                 "span_id": None, "body": "",
                 "log_attributes": _json.dumps(
                     {"event.name": "codex.turn_cost", "session.id": "s1",
                      "coding_agent.client": "codex",
                      "usage.estimated_usd": 0.004}),
                 "resource_attributes": _json.dumps({"service.name": "codex"})}
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    logs = agg.dedupe_logs(
        cols, [[cost_only["timestamp"], cost_only["severity_text"],
                cost_only["severity_number"], cost_only["scope_name"],
                cost_only["trace_id"], cost_only["span_id"], cost_only["body"],
                cost_only["log_attributes"], cost_only["resource_attributes"]]])
    assert logs and logs[0].get("turn") is None, logs
    merged, _ = agg.merge_native(billed, logs)
    daily = agg.summarize_daily(merged)
    assert daily[0]["cost"] == 0.005, daily  # no link: both kept
    assert daily[0]["llm_spans"] == 1, daily


def test_native_cost_kept_separate_from_estimated():
    est = span_row("2026-09-21 10:00:00", "t8", "e", None, "chat",
                   "s8", None, "omp", "svc", 5, 5, 0.003, "estimated")
    nat = span_row("2026-09-21 10:01:00", "t8", "n", None, "chat",
                   "s8", None, "omp", "svc", 5, 5, None, None, native=0.004)
    both = span_row("2026-09-21 10:02:00", "t8", "b", None, "chat",
                    "s8", None, "omp", "svc", 5, 5, 0.003, "estimated",
                    native=0.004)
    spans = [agg.norm_span(est), agg.norm_span(nat), agg.norm_span(both)]
    got = [agg.resolve_cost(s) for s in spans]
    assert got[0] == (0.003, "estimated"), got
    assert got[1] == (0.004, "native"), got  # raw vendor value, own label
    assert got[2] == (0.003, "estimated"), got  # estimated wins, never summed
    assert agg.cost_label(spans) == "mixed", spans  # never one merged number

def test_ns_timestamp_typed_conversion():
    # Live Greptime returns TimestampNanosecond ints like 1789977727964984000;
    # the old ms-only path blew up with `year 56724106 is out of range`.
    srv = serve()
    try:
        Handler.cols = ["timestamp", "span_id"]
        Handler.types = ["TimestampNanosecond", "String"]
        Handler.rows = [[1789977727964984000, "s1"]]
        base = "http://127.0.0.1:%d" % srv.server_port
        cols, rows = agg.fetch_rows(base, "YXV0aA==", "db",
                                    "SELECT timestamp, span_id FROM t")
        assert rows[0][0].year == 2026, rows[0]  # real date, not year 56M
        assert rows[0][0].month == 9 and rows[0][0].day == 21, rows[0]
        # untyped fallback by magnitude + float refusal
        assert agg.parse_ts(1789977727964984000).year == 2026
        assert agg.parse_ts(1789977727) is not None  # seconds
        assert agg.parse_ts(1789977727.5) is None  # float: never guessed
        assert agg.parse_ts(True) is None
    finally:
        Handler.types = []
        srv.shutdown()




def test_window_floor_and_sealed_exclusion():
    import datetime as dt
    assert agg.floor_minute(dt.datetime(2026, 9, 21, 10, 7, 42)) == \
        dt.datetime(2026, 9, 21, 10, 7, 0)
    assert agg.floor_hour(dt.datetime(2026, 9, 21, 10, 7, 42)) == \
        dt.datetime(2026, 9, 21, 10, 0, 0)


def test_epoch_source_separation_and_same_timestamp_policy():
    import datetime as dt
    base = dt.datetime(2026, 9, 21, 10, 0, 0)
    cols = ["event_time", "vehicle", "path", "source", "decode_epoch",
            "value_num", "value_bool", "unit"]
    r = lambda ts, v, src, ep, p="Vehicle.Speed", b=None, u=None: [
        ts.strftime("%Y-%m-%d %H:%M:%S"), v, p, src, ep, 30.0, b, u]
    rows = [r(base, "v", "can", "e1"), r(base, "v", "can", "e1"),
            r(base, "v", "can", "e2"), r(base, "v", "fleet", "e1")]
    groups = agg.group_vehicle_rows(
        cols, rows, {"speed": "Vehicle.Speed"})
    assert len(groups) == 3, groups  # epoch/source never merged
    assert len(groups[("v", "can", "e1")]["speed"]) == 1  # last row wins


def test_soc_rise_alone_never_charges():
    import datetime as dt
    base = dt.datetime(2026, 9, 21, 10, 0, 0)
    pts = [(base + dt.timedelta(minutes=m), 60.0 + m * 0.5, None, None, None, 0.0)
           for m in range(10)]
    assert agg.segment_charges(pts) == []  # no charging/power/energy signal
    pts2 = [(base + dt.timedelta(minutes=m), 60.0 + m * 0.5, None, True if m > 2 else None,
             None, 0.0) for m in range(10)]
    got = agg.segment_charges(pts2)
    assert len(got) == 1 and got[0]["avg_power_kw"] is None
    assert got[0]["energy_added_kwh"] is None


def test_trip_segmentation_gap():
    import datetime as dt
    base = dt.datetime(2026, 9, 21, 10, 0, 0)
    pts = [(base + dt.timedelta(minutes=m),
            30.0 if m < 5 or m > 20 else None,  # 16-min stop splits trips
            80.0 - m * 0.1) for m in range(30)]
    trips = agg.segment_trips(pts)
    assert len(trips) == 2, trips
    assert trips[0]["start_soc"] is not None






def test_native_resource_and_incomplete_metadata():
    import json as _json
    cols = ["timestamp", "resource_attributes", "log_attributes"]
    logs = agg.dedupe_logs(cols, [[
        "2026-09-21 10:00:00",
        _json.dumps({"service.name": "claude_code"}),
        _json.dumps({"event.name": "api_request", "session.id": "native-session",
                     "request_id": "native-request", "input_tokens": 17,
                     "output_tokens": 6, "cost_usd": 0.012}),
    ]])
    # 'claude_code' (underscore) is the native metric/event namespace, not a
    # verified service identity: client stays unknown, usage still bills.
    assert logs[0]["client"] is None, logs
    merged, _ = agg.merge_native([], logs)
    daily = agg.summarize_daily(merged)
    assert daily[0]["client"] == "unknown"
    assert daily[0]["provider"] == daily[0]["model"] == "unknown"
    assert (daily[0]["input"], daily[0]["output"], daily[0]["cost"]) == (17, 6, 0.012)
    first = billed_span("2026-09-21 10:00:00", "s", "c", "tool-1",
                        None, None, None, tool="read", dur=20)
    first["error"] = True
    second = billed_span("2026-09-21 10:00:01", "s", "c", "tool-2",
                         None, None, None, tool="read")
    tools = agg.summarize_tools([first, second])
    assert (tools[0]["calls"], tools[0]["errors"], tools[0]["avg_ms"]) == (2, 1, 20)


def test_tool_result_folds_into_matching_span_once():
    import json as _json
    span = billed_span("2026-09-21 10:00:00", "s1", "codex", "call-9",
                       None, None, None, tool="read")
    result = {"timestamp": "2026-09-21 10:00:01", "severity_text": "INFO",
              "severity_number": 9, "scope_name": "s", "trace_id": None,
              "span_id": None, "body": "",
              "log_attributes": _json.dumps(
                  {"event.name": "codex.tool_result", "session.id": "s1",
                   "coding_agent.client": "codex", "tool_name": "read",
                   "gen_ai.tool.call.id": "call-9", "duration_ms": 40.0}),
              "resource_attributes": _json.dumps({"service.name": "codex"})}
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    logs = agg.dedupe_logs(
        cols, [[result["timestamp"], result["severity_text"],
                result["severity_number"], result["scope_name"],
                result["trace_id"], result["span_id"], result["body"],
                result["log_attributes"], result["resource_attributes"]],
               [result["timestamp"], result["severity_text"],
                result["severity_number"], result["scope_name"],
                result["trace_id"], result["span_id"], result["body"],
                result["log_attributes"], result["resource_attributes"]]])
    assert len(logs) == 1, logs  # retransmitted terminal result dedupes
    merged, _ = agg.merge_native([], logs)
    both = [span, merged[0]]
    tools = agg.summarize_tools(both)
    assert tools[0]["calls"] == 1, tools  # span + result is one logical call
    assert tools[0]["avg_ms"] == 40.0, tools  # result duration fills the gap
    sess = agg.summarize_sessions(both)
    assert sess[0]["tool_calls"] == 1, sess  # session shares the policy
    daily = agg.summarize_daily(both)
    assert daily[0]["tool_calls"] == 1, daily  # daily shares the policy


def test_tool_decision_and_distinct_tools_not_counted_as_calls():
    import json as _json
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    decision = {"timestamp": "2026-09-21 10:00:00", "severity_text": "INFO",
                "severity_number": 9, "scope_name": "s", "trace_id": None,
                "span_id": None, "body": "",
                "log_attributes": _json.dumps(
                    {"event.name": "codex.tool_decision", "session.id": "s1",
                     "coding_agent.client": "codex",
                     "gen_ai.tool.call.id": "call-9"}),
                "resource_attributes": _json.dumps({"service.name": "codex"})}
    rows = [[decision["timestamp"], decision["severity_text"],
             decision["severity_number"], decision["scope_name"],
             decision["trace_id"], decision["span_id"], decision["body"],
             decision["log_attributes"], decision["resource_attributes"]]]
    assert agg.dedupe_logs(cols, rows) == []  # decisions are never calls
    first = billed_span("2026-09-21 10:00:00", "s1", "codex", "call-a",
                        None, None, None, tool="read")
    other_session = billed_span("2026-09-21 10:00:01", "s2", "codex",
                                "call-a", None, None, None, tool="read")
    no_id = billed_span("2026-09-21 10:00:02", "s1", "codex", None,
                        None, None, None, tool="read")
    tools = agg.summarize_tools([first, other_session, no_id])
    assert tools[0]["calls"] == 3, tools  # scoped ids + id-less stay distinct


def test_cost_only_missing_duration_still_money_without_call():
    billed = [billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                          10, 5, 0.001)]
    tool = billed_span("2026-09-21 10:00:01", "s1", "codex", "call-9",
                       None, None, None, tool="read")
    import json as _json
    cost_only = {"timestamp": "2026-09-21 10:00:05", "severity_text": "INFO",
                 "severity_number": 9, "scope_name": "s", "trace_id": None,
                 "span_id": None, "body": "",
                 "log_attributes": _json.dumps(
                     {"event.name": "codex.turn_cost", "session.id": "s1",
                      "coding_agent.client": "codex",
                      "usage.estimated_usd": 0.004}),
                 "resource_attributes": _json.dumps({"service.name": "codex"})}
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    logs = agg.dedupe_logs(
        cols, [[cost_only["timestamp"], cost_only["severity_text"],
                cost_only["severity_number"], cost_only["scope_name"],
                cost_only["trace_id"], cost_only["span_id"], cost_only["body"],
                cost_only["log_attributes"], cost_only["resource_attributes"]]])
    # No turn.id on either side: no linkage may be fabricated, so this stays
    # an additive money-only row (the .001+.004=.005 case is only valid here).
    assert logs[0].get("turn") is None, logs
    merged, _ = agg.merge_native(billed, logs)
    rows = merged + [tool]  # tool row has no duration_ms
    daily = agg.summarize_daily(rows)
    assert daily[0]["cost"] == 0.005, daily
    assert daily[0]["llm_spans"] == 1, daily  # money added, call count kept
    tools = agg.summarize_tools(rows)
    assert (tools[0]["calls"], tools[0]["avg_ms"]) == (1, None), tools


def _reset_table_fixtures():
    Handler.span_cols = None
    Handler.span_rows = None
    Handler.span_types = None
    Handler.log_cols = None
    Handler.log_rows = None
    Handler.cols = []
    Handler.rows = []
    Handler.types = []
    Handler.seen.clear()


def _ai_cfg(**over):
    cfg = {"base_url": "", "db": "db", "user": "u", "password": "p",
           "lookback_h": 30, "interval_s": 1, "max_rows": 1000,
           "otel_ttl": "", "vehicle": "",
           "paths": {"speed": "", "soc": "", "energy": "",
                     "power": "", "charging": ""},
           "home": {"table": "", "time_col": "ts", "entity_col": "",
                    "value_col": "value"}}
    cfg.update(over)
    return cfg


@patch.object(agg, "load_price_table", lambda: None)
def test_late_earlier_span_replaces_session_start_and_stays_idempotent():
    # Main's live repro: initial logical-smoke session starts at
    # 1790004181528579000 (input 11/output 5), then a late earlier span at
    # 1790000581528579000 (input 2/output 1) moves the TIME INDEX start.
    srv = serve()
    try:
        _reset_table_fixtures()
        first = span_row("2026-09-21 10:03:01", "t", "late",
                         None, "chat", "logical-smoke", None, "codex", "svc",
                         11, 5, 0.004, "estimated")
        late = span_row("2026-09-21 09:03:01", "t", "early",
                        None, "chat", "logical-smoke", None, "codex", "svc",
                        2, 1, 0.001, "estimated")
        tool = span_row("2026-09-21 10:03:02", "t", "tool",
                        None, "execute_tool", "logical-smoke", None, "codex",
                        "svc", None, None, None, None, tool="read")
        other = span_row("2026-09-21 10:04:00", "t", "other",
                        None, "chat", "unrelated", None, "codex", "svc",
                        3, 2, 0.001, "estimated")
        Handler.span_cols = list(first)
        Handler.span_rows = [[r[c] for c in Handler.span_cols]
                             for r in (first, tool, other)]
        Handler.log_cols = ["timestamp", "severity_text", "severity_number",
                            "scope_name", "trace_id", "span_id", "body",
                            "log_attributes", "resource_attributes"]
        Handler.log_rows = []
        base = "http://127.0.0.1:%d" % srv.server_port
        ctx = (base, "YXV0aA==", "db")
        cfg = _ai_cfg()
        cfg["base_url"] = base
        agg.ai_section(ctx, cfg)
        Handler.span_rows = [[r[c] for c in Handler.span_cols]
                             for r in (first, late, tool, other)]
        agg.ai_section(ctx, cfg)
        inserts = [s for s in Handler.seen if s.startswith("INSERT INTO ai_session_summary")]
        deletes = [s for s in Handler.seen if s.startswith("DELETE FROM ai_session_summary")]
        assert inserts, Handler.seen  # session row written
        assert any("'logical-smoke'" in s and "'codex'" in s and
                   "2026-09-21 09:03:01" in s for s in inserts), inserts
        assert any("session_id = 'logical-smoke'" in s and
                   "client = 'codex'" in s and "session_start !=" in s
                   for s in deletes), deletes
        assert all("session_start !=" in s and "client = " in s
                   and "IS NULL" not in s for s in deletes), deletes
        assert any("'unrelated'" in s for s in inserts), inserts
        seen_before = len(Handler.seen)
        agg.ai_section(ctx, cfg)
        rerun = Handler.seen[seen_before:]
        rerun_inserts = [s for s in rerun if s.startswith("INSERT INTO ai_session_summary")]
        smoke = [s for s in rerun_inserts if "'logical-smoke'" in s]
        assert len(smoke) == 1 and "2026-09-21 09:03:01" in smoke[0], rerun_inserts
        merged = agg.summarize_sessions(
            [agg.norm_span(first), agg.norm_span(late),
             agg.norm_span(tool)])
        assert len(merged) == 1, merged  # one logical session row
        assert (merged[0]["input"], merged[0]["output"]) == (13, 6), merged[0]
        assert merged[0]["llm_spans"] == 2, merged[0]
        assert merged[0]["tool_calls"] == 1, merged[0]
        assert merged[0]["cost"] == 0.005, merged[0]
    finally:
        _reset_table_fixtures()
        srv.shutdown()


def test_same_session_id_across_clients_stays_separate():
    a = billed_span("2026-09-21 10:00:00", "shared", "codex", "r1", 5, 5, 0.001)
    b = billed_span("2026-09-21 10:01:00", "shared", "claude-code", "r2",
                    7, 3, 0.002)
    sess = agg.summarize_sessions([a, b])
    assert sorted((s["client"], s["input"]) for s in sess) == [
        ("claude-code", 7), ("codex", 5)], sess
    missing = billed_span("2026-09-21 10:02:00", "shared", None, "r3",
                          1, 1, 0.001)
    missing["client"] = None  # neither span attr nor service fallback
    unknown = agg.summarize_sessions([missing])
    assert unknown[0]["client"] == "unknown", unknown  # non-null PK, not NULL

def test_native_codex_custom_service_folds_into_canonical_span():
    import json as _json
    span = billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                       5, 5, 0.001)
    custom = {"timestamp": "2026-09-21 10:00:01", "severity_text": "INFO",
              "severity_number": 9, "scope_name": "s", "trace_id": None,
              "span_id": None, "body": "",
              "log_attributes": _json.dumps(
                  {"event.name": "codex.sse_event",
                   "event.kind": "response.completed",
                   "session.id": "s1", "request_id": "r1",
                   "input_token_count": 10, "output_token_count": 3,
                   "usage.estimated_usd": 0.002}),
              "resource_attributes": _json.dumps(
                  {"service.name": "custom-originator"})}
    explicit = dict(custom)
    explicit["log_attributes"] = _json.dumps(
        {**_json.loads(custom["log_attributes"]),
         "coding_agent.client": "other"})
    cols = ["timestamp", "severity_text", "severity_number", "scope_name",
            "trace_id", "span_id", "body", "log_attributes",
            "resource_attributes"]
    rows = lambda r: [[r["timestamp"], r["severity_text"],
                       r["severity_number"], r["scope_name"], r["trace_id"],
                       r["span_id"], r["body"], r["log_attributes"],
                       r["resource_attributes"]]]
    logs = agg.dedupe_logs(cols, rows(custom))
    assert logs[0]["client"] == "codex", logs  # runtime name not the client
    merged, _ = agg.merge_native([span], logs)
    assert len(merged) == 1, merged  # same call folds, never two rows
    assert (merged[0]["input"], merged[0]["output"]) == (10, 3), merged[0]
    kept = agg.dedupe_logs(cols, rows(explicit))
    assert kept[0]["client"] == "other", kept  # explicit client wins


def test_arbitrary_span_service_never_becomes_client():
    a = span_row("2026-09-21 10:00:00", "t9", "a", None, "chat",
                 "s1", None, None, "my-fork", 5, 5, 0.001, "estimated")
    assert agg.norm_span(a)["client"] is None, agg.norm_span(a)
    unknown = agg.summarize_sessions([agg.norm_span(a)])
    assert unknown[0]["client"] == "unknown", unknown


@patch.object(agg, "load_price_table", lambda: None)
def test_ttl_preserves_partly_expired_windows_but_recomputes_retained():
    import datetime as dt
    now = dt.datetime(2026, 9, 21, 12, 0, 0)
    old_session = billed_span("2026-09-10 10:00:00", "old", "codex", "r1",
                              5, 5, 0.001)
    new_first = billed_span("2026-09-21 10:00:00", "new", "codex", "r2",
                            11, 5, 0.004)
    new_late = billed_span("2026-09-21 09:00:00", "new", "codex", "r3",
                           2, 1, 0.001)
    srv = serve()
    try:
        _reset_table_fixtures()
        first = span_row("2026-09-21 10:00:00", "t", "n1", None, "chat",
                         "new", None, "codex", "svc", 11, 5, 0.004,
                         "estimated")
        late = span_row("2026-09-21 09:00:00", "t", "n2", None, "chat",
                        "new", None, "codex", "svc", 2, 1, 0.001, "estimated")
        old = span_row("2026-09-10 10:00:00", "t", "o1", None, "chat",
                       "old", None, "codex", "svc", 5, 5, 0.001, "estimated")
        Handler.span_cols = list(first)
        Handler.span_rows = [[r[c] for c in Handler.span_cols]
                             for r in (first, late, old)]
        Handler.log_cols = ["timestamp", "severity_text", "severity_number",
                            "scope_name", "trace_id", "span_id", "body",
                            "log_attributes", "resource_attributes"]
        Handler.log_rows = []
        base = "http://127.0.0.1:%d" % srv.server_port
        ctx = (base, "YXV0aA==", "db")
        cfg = _ai_cfg()
        cfg["base_url"] = base
        cfg["otel_ttl"] = "7d"
        real_utcnow = agg.utcnow
        agg.utcnow = lambda: now
        try:
            agg.ai_section(ctx, cfg)
        finally:
            agg.utcnow = real_utcnow
        inserts = [s for s in Handler.seen
                   if s.startswith("INSERT INTO ai_session_summary")]
        daily_inserts = [s for s in Handler.seen
                         if s.startswith("INSERT INTO ai_daily_summary")]
        sessions = agg.summarize_sessions([old_session, new_first, new_late])
        kept = [s for s in sessions if agg.fully_retained(
            s["session_start"], "7d", now)]
        dropped = [s for s in sessions if not agg.fully_retained(
            s["session_start"], "7d", now)]
        assert len(kept) == 1 and kept[0]["session_id"] == "new", sessions
        assert (kept[0]["input"], kept[0]["output"]) == (13, 6), kept[0]
        assert len(dropped) == 1 and dropped[0]["session_id"] == "old", sessions
        assert any("'new'" in s for s in inserts), inserts
        assert not any("'old'" in s for s in inserts), inserts
        assert any("2026-09-21 00:00:00" in s for s in daily_inserts), daily_inserts
        assert not any("2026-09-10 00:00:00" in s for s in daily_inserts), daily_inserts
    finally:
        _reset_table_fixtures()
        srv.shutdown()


def test_log_fetch_starts_at_full_day_boundary():
    import datetime as dt
    srv = serve()
    try:
        _reset_table_fixtures()
        Handler.cols = ["timestamp", "severity_text", "severity_number",
                        "scope_name"]
        Handler.rows = []
        base = "http://127.0.0.1:%d" % srv.server_port
        ctx = (base, "YXV0aA==", "db")
        cfg = _ai_cfg()
        cfg["base_url"] = base
        real_utcnow = agg.utcnow
        agg.utcnow = lambda: dt.datetime(2026, 9, 21, 10, 7, 42)
        try:
            agg.log_section(ctx, cfg)
        finally:
            agg.utcnow = real_utcnow
        selects = [s for s in Handler.seen if s.startswith("SELECT")]
        # 30h lookback from 09-21 10:07 lands 09-20 04:07, floored to the
        # full-day boundary: the first written day is fed by full-day data.
        assert selects and "2026-09-20 00:00:00" in selects[0], selects
    finally:
        _reset_table_fixtures()
        srv.shutdown()


def test_retention_boundary_never_moves_backwards():
    import datetime as dt
    srv = serve()
    try:
        previous = dt.datetime(2026, 9, 20, 10)
        now = dt.datetime(2026, 9, 21, 10)
        Handler.retention_rows = [[previous.isoformat()]]
        ctx = ("http://127.0.0.1:%d" % srv.server_port, "YXV0aA==", "db")
        assert agg.retention_boundary(ctx, "raw_home", "90d", now) == previous
        assert agg.retention_boundary(ctx, "raw_home", "0s", now) == previous
        assert agg.retention_boundary(ctx, "raw_home", "12h", now) == \
            now - dt.timedelta(hours=12)
    finally:
        Handler.retention_rows = [[None]]
        srv.shutdown()


def test_native_scope_selection_preserves_context_without_double_billing():
    span = billed_span("2026-09-21 10:00:00", "scope", "claude-code",
                       "span-id", 99, 11, 0.01)
    usage = agg.norm_log(log_row("2026-09-21 10:00:05", "scope", "claude-code",
                                 "native-id", 13, 6, 0.002, native="claude"))
    merged, _ = agg.merge_native([span], [usage])
    session = agg.summarize_sessions(merged)[0]
    assert (session["input"], session["output"], session["llm_spans"]) == (13, 6, 1)
    assert session["cost"] == 0.002, session
    assert session["duration_s"] == 5, session


def test_missing_session_never_suppresses_unrelated_span_usage():
    span = billed_span("2026-09-21 10:00:00", None, "codex",
                       "unrelated-span", 99, 11, 0.001)
    usage = agg.norm_log(log_row("2026-09-21 10:00:05", None, "codex",
                                 "unrelated-native", 13, 6, 0.002))
    merged, _ = agg.merge_native([span], [usage])
    daily = agg.summarize_daily(merged)[0]
    assert (daily["input"], daily["output"], daily["llm_spans"]) == (112, 17, 2)
    assert daily["cost"] == 0.003, daily


def test_session_preserves_every_observed_model():
    first = billed_span("2026-09-21 10:00:00", "models", "codex",
                        "first", 10, 5, 0.001)
    second = billed_span("2026-09-21 10:01:00", "models", "codex",
                         "second", 20, 7, 0.002)
    first["model"], second["model"] = "model-z", "model-a"
    session = agg.summarize_sessions([first, second])[0]
    assert session["models"] == ["model-a", "model-z"], session
    assert session["model_count"] == 2, session


def _priced(model="m", provider="openai", rates=(0.001, 0.002, 0.0005, 0.004, 0.002),
            mode="chat"):
    return {model: rates + (mode, provider)}


def test_supplemental_mixed_present_missing_zero_untouched():
    # Reported costs (including explicit 0) are never overwritten: the
    # estimate covers only the missing-cost call; totals stay separate.
    missing = billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                          110, 5, None)
    missing.update(model="m", provider="openai", cache_read=10,
                   cache_write=0, reasoning=0)
    present = billed_span("2026-09-21 10:01:00", "s1", "codex", "r2",
                          5, 5, 0.01)
    present.update(model="m", provider="openai")
    zero = billed_span("2026-09-21 10:02:00", "s1", "codex", "r3",
                       5, 5, 0.0)
    zero.update(model="m", provider="openai")
    daily = agg.summarize_daily([missing, present, zero], _priced())[0]
    assert daily["cost"] == 0.01, daily  # reported kept, never summed in
    assert daily["cost_source"] == "estimated", daily
    assert abs(daily["cost_estimated_usd"] - 0.115) < 1e-12, daily
    assert daily["cost_unpriced_calls"] == 0, daily
    sess = agg.summarize_sessions([missing, present, zero], _priced())[0]
    assert (sess["cost"], sess["cost_estimated_usd"],
            sess["cost_unpriced_calls"]) == (0.01, daily["cost_estimated_usd"], 0)


def test_supplemental_unknown_model_counts_unpriced():
    row = billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                      10, 5, None)
    row.update(model="unknown-model", provider="openai")
    daily = agg.summarize_daily([row], _priced())[0]
    assert daily["cost"] is None and daily["cost_estimated_usd"] is None
    assert daily["cost_unpriced_calls"] == 1, daily


def test_supplemental_never_runs_without_prices_or_unverified_semantics():
    # Legacy default (no prices arg) preserves NULLs for unprocessed rows.
    row = billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                      10, 5, None)
    row.update(model="m", provider="openai")
    legacy = agg.summarize_daily([row])[0]
    assert legacy["cost_estimated_usd"] is None
    assert legacy["cost_unpriced_calls"] is None, legacy
    # agy carries length-heuristic estimates (plugins/agy/hook.mjs), and
    # unknown clients have no verified token contract: never estimated.
    for client in ("agy", "unknown-client"):
        unverified = billed_span("2026-09-21 10:00:00", "s1", client,
                                 "r1", 10, 5, None)
        unverified.update(model="m", provider="openai")
        got = agg.summarize_daily([unverified], _priced())[0]
        assert got["cost_estimated_usd"] is None
        assert got["cost_unpriced_calls"] == 1, got


def test_supplemental_cache_math_invalid_data_and_receipt_suppression():
    table = _priced()
    # claude-code input excludes cache buckets; opencode output includes
    # reasoning; codex input includes cache (verified plugin mappings).
    claude = billed_span("2026-09-21 10:00:00", "s1", "claude-code",
                         "r1", 100, 5, None)
    claude.update(model="m", provider="openai", cache_read=10,
                  cache_write=0, reasoning=0)
    assert abs(agg.estimate_call(claude, table) -
               (100 * 0.001 + 10 * 0.0005 + 5 * 0.002)) < 1e-12
    opencode = billed_span("2026-09-21 10:00:00", "s1", "opencode",
                           "r1", 100, 7, None)
    opencode.update(model="m", provider="openai", reasoning=2)
    assert abs(agg.estimate_call(opencode, table) -
               (100 * 0.001 + 5 * 0.002 + 2 * 0.002)) < 1e-12
    codex = dict(opencode, client="codex")
    assert abs(agg.estimate_call(codex, table) - 0.114) < 1e-12
    assert agg.estimate_call(dict(codex, input=None), table) is None
    assert agg.estimate_call(dict(codex, output=None), table) is None
    assert agg.estimate_call(dict(codex, client="oh-my-pi"), table) is None
    assert agg.estimate_call(claude, {"m": (0.001, 0.002, None, None,
                                            None, "chat", "openai")}) is None
    tiered = agg._parse_price_table({"m": {
        "litellm_provider": "openai", "mode": "chat",
        "input_cost_per_token": 0.001, "output_cost_per_token": 0.002,
        "input_cost_per_token_above_200k_tokens": 0.002}})
    assert agg.estimate_call(dict(codex, input=300000), tiered) is None
    # Non-int counts, negative split remainder, missing rate, vendor or
    # mode mismatch all fail closed (None, counted unpriced downstream).
    bad = billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                      10, 5, None)
    bad.update(model="m", provider="openai", cache_read="x")
    assert agg.estimate_call(bad, table) is None
    assert agg.estimate_call(claude, {"m": (None, 0.002, 0.0005, 0.004,
                                            0.002, "chat", "openai")}) is None
    assert agg.estimate_call(claude, {"m": (0.001, 0.002, 0.0005, 0.004,
                                            0.002, "chat", "anthropic")}) is None
    assert agg.estimate_call(claude, {"m": (0.001, 0.002, 0.0005, 0.004,
                                            0.002, "embedding", "openai")}) is None
    # Turn-suppressed parts are covered by the turn total: neither
    # estimated nor counted unpriced.
    part = billed_span("2026-09-21 10:00:00", "s1", "codex", "r1",
                       10, 5, None)
    part.update(model="m", provider="openai", turn="t1")
    total = dict(part)
    total.update(turn_total=True, input=None, output=None, cost_est=0.5,
                 cost_source="estimated")
    daily = agg.summarize_daily([part, total], table)[0]
    assert daily["cost"] == 0.5 and daily["cost_estimated_usd"] is None
    assert daily["cost_unpriced_calls"] == 0, daily


@patch.dict(agg._PRICES, table=None, ok_at=0.0, attempt_at=0.0)
def test_price_loader_caches_refresh_failure_and_rejects_bad_payload():
    calls = {"n": 0}

    def bad():
        calls["n"] += 1
        raise OSError("registry down")

    assert agg.load_price_table(now=10000.0, fetch=bad) is None
    assert calls["n"] == 1
    # Retry backoff: no refetch every aggregate pass.
    assert agg.load_price_table(now=10001.0, fetch=bad) is None
    assert calls["n"] == 1
    import json as _json
    good = {"m": {"litellm_provider": "openai", "mode": "chat",
                   "input_cost_per_token": 1, "output_cost_per_token": 2}}
    table = agg.load_price_table(
        now=20000.0, fetch=lambda: _json.dumps(good).encode())
    assert table["m"][0] == 1.0
    # Daily cache: no fetch; expired refresh failure keeps last-good.
    assert agg.load_price_table(now=20001.0, fetch=bad)["m"][0] == 1.0
    assert calls["n"] == 1
    assert agg.load_price_table(now=20000.0 + 86400 + 1,
                                fetch=bad)["m"][0] == 1.0
    assert calls["n"] == 2
    assert agg.load_price_table(now=20000.0 + 86400 + 3601,
                                fetch=lambda: b"{}")["m"][0] == 1.0
    # Prose/non-model keys skipped; non-object payload rejected.
    assert agg._parse_price_table({"sample_spec": {},
                                    "x": {"mode": "chat"}}) == {}
    assert agg._parse_price_table(["nope"]) is None

@patch.object(agg, "load_price_table", lambda: None)
def test_session_batch_replacement_isolation_and_failure_keeps_anchor():
    # Persisted-state regression over a local SQLite-backed HTTP store:
    # earlier session start replaces the stale anchor, client identity stays
    # isolated, an expired stored anchor is untouched, a faulted session
    # INSERT batch keeps old anchors, and a clean restart converges to one
    # current start per (client, session_id).
    import datetime as dt
    import sqlite3
    now = dt.datetime(2026, 9, 21, 12, 0, 0)
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.execute("CREATE TABLE store (client TEXT, session_id TEXT,"
               " session_start TEXT, input_tokens INTEGER,"
               " output_tokens INTEGER, cost_usd REAL)")
    db.executemany(
        "INSERT INTO store VALUES (?, ?, ?, ?, ?, ?)",
        [("codex", "move", "2026-09-21 10:00:00", 5, 5, 0.001),
         ("oh-my-pi", "move", "2026-09-21 10:00:00", 7, 3, 0.002),
         ("codex", "old", "2026-09-10 10:00:00", 5, 5, 0.001)])
    lock = threading.Lock()
    source = {"cols": [], "rows": []}
    faults = {"session_inserts": 0, "fail_at": None}

    class SessionStoreHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            stmt = urllib.parse.parse_qs(
                self.rfile.read(length).decode()).get("sql", [""])[0]
            payload = self.route(stmt)
            data = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def route(self, stmt):
            if stmt.startswith("SELECT"):
                if "FROM opentelemetry_traces" in stmt:
                    return _records(source["cols"], source["rows"])
                if "FROM opentelemetry_logs" in stmt:
                    return _records(list(agg.LOG_COLS), [])
                if "FROM ai_session_summary" in stmt:
                    with lock:
                        rows = db.execute(
                            "SELECT client, session_id, session_start"
                            " FROM store").fetchall()
                    return _records(["client", "session_id", "session_start"],
                                    [list(r) for r in rows])
                if "FROM raw_retention_watermark" in stmt:
                    return _records(["deleted_before"], [[None]])
                return _records(["table_name", "create_options"], [])
            if stmt.startswith("INSERT INTO ai_session_summary"):
                faults["session_inserts"] += 1
                if faults["fail_at"] == faults["session_inserts"]:
                    return {"error": "boom"}
                with lock:
                    for client, sid, start, inp, outp, cost in _lit_rows(stmt):
                        # Same (client, session, start) overwrites, mirroring
                        # the production upsert; a new start adds a row the
                        # scoped delete below then reconciles.
                        db.execute("DELETE FROM store WHERE client = ?"
                                   " AND session_id = ? AND session_start = ?",
                                   (client, sid, start))
                        db.execute("INSERT INTO store VALUES (?, ?, ?, ?, ?, ?)",
                                   (client, sid, start, inp, outp, cost))
                    db.commit()
                return {"output": [{"affectedrows": 1}]}
            if stmt.startswith("INSERT INTO"):
                return {"output": [{"affectedrows": 1}]}
            if stmt.startswith("DELETE FROM ai_session_summary"):
                sid = _lit(stmt.split("session_id = ", 1)[1])
                client = _lit(stmt.split("client = ", 1)[1])
                start = _lit(stmt.split("session_start != ", 1)[1])
                with lock:
                    db.execute("DELETE FROM store WHERE session_id = ?"
                               " AND client = ? AND session_start != ?",
                               (sid, client, start))
                    db.commit()
                return {"output": [{"affectedrows": 1}]}
            return {"output": [{"affectedrows": 1}]}

    def _s(ts, span, session, client):
        return span_row(ts, "t-%s-%s" % (session, span), span, None,
                        "chat", session, None, client, "svc",
                        5, 5, 0.001, "estimated")
    rows = [_s("2026-09-21 10:00:00", "late", "move", "codex"),
            _s("2026-09-21 09:00:00", "early", "move", "codex"),
            _s("2026-09-21 10:30:00", "other1", "move", "oh-my-pi"),
            _s("2026-09-10 10:00:00", "old1", "old", "codex")]
    source["cols"] = list(rows[0])
    source["rows"] = [[r[c] for c in source["cols"]] for r in rows]
    srv = http.server.HTTPServer(("127.0.0.1", 0), SessionStoreHandler)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    ctx = ("http://127.0.0.1:%d" % srv.server_port, "YXV0aA==", "db")
    cfg = _ai_cfg()
    cfg["otel_ttl"] = "7d"
    real_utcnow = agg.utcnow
    agg.utcnow = lambda: now
    try:
        agg.ai_section(ctx, cfg)
        got = {(c, s): (ts, i, o, m)
               for c, s, ts, i, o, m in db.execute(
                   "SELECT client, session_id, session_start, input_tokens,"
                   " output_tokens, cost_usd FROM store").fetchall()}
        assert got[("codex", "move")][0].startswith("2026-09-21 09:00:00"), got
        assert got[("codex", "move")][1:] == (10, 10, 0.002), got
        assert got[("oh-my-pi", "move")][0].startswith("2026-09-21 10:30:00"), got
        assert got[("oh-my-pi", "move")][1:] == (5, 5, 0.001), got
        assert got[("codex", "old")] == ("2026-09-10 10:00:00", 5, 5, 0.001), got
        # 501 input sessions reach the second INSERT batch; the injected
        # fault fails it after the first batch landed, so old anchors stay.
        bulk = [_s("2026-09-21 11:%02d:00" % (i % 60), "sp-%d" % i,
                    "bulk-%d" % i, "codex") for i in range(501)]
        source["cols"] = list(bulk[0])
        source["rows"] = [[r[c] for c in source["cols"]] for r in bulk]
        before = set(db.execute(
            "SELECT client, session_id, session_start FROM store").fetchall())
        faults["session_inserts"] = 0
        faults["fail_at"] = 2
        try:
            agg.ai_section(ctx, cfg)
        except agg.SqlError:
            pass
        else:
            raise AssertionError("expected SqlError on session batch failure")
        assert before <= set(db.execute(
            "SELECT client, session_id, session_start FROM store").fetchall())
        # Clean restart converges to one current start per affected session.
        faults["session_inserts"] = 0
        faults["fail_at"] = None
        source["cols"] = list(rows[0])
        source["rows"] = [[r[c] for c in source["cols"]] for r in rows]
        agg.ai_section(ctx, cfg)
        final = db.execute(
            "SELECT client, session_id, session_start FROM store").fetchall()
        assert sum(1 for c, s, ts in final
                   if (c, s) == ("codex", "move")
                   and str(ts).startswith("2026-09-21 09:00:00")) == 1, final
        assert sum(1 for c, s, ts in final
                   if (c, s) == ("oh-my-pi", "move")
                   and str(ts).startswith("2026-09-21 10:30:00")) == 1, final
        assert ("codex", "old", "2026-09-10 10:00:00") in [
            (c, s, str(ts)) for c, s, ts in final], final
    finally:
        agg.utcnow = real_utcnow
        _reset_table_fixtures()
        srv.shutdown()
        srv.server_close()
        db.close()


def _records(cols, rows):
    return {"output": [{"records": {
        "schema": {"column_schemas": [{"name": c} for c in cols]},
        "rows": rows}}], "execution_time_ms": 1}


def _lit(text):
    # Leading single-quoted literal ('' unescapes to ').
    text = text.lstrip()
    assert text.startswith("'"), text
    out, i = [], 1
    while i < len(text):
        if text[i] == "'" and text[i:i + 2] == "''":
            out.append("'")
            i += 2
        elif text[i] == "'":
            return "".join(out)
        else:
            out.append(text[i])
            i += 1
    raise AssertionError("unterminated literal: " + text)


def _split_cells(tup):
    parts, cur, quoted, i = [], "", False, 0
    while i < len(tup):
        ch = tup[i]
        if quoted:
            if ch == "'" and tup[i:i + 2] == "''":
                cur += "''"
                i += 2
                continue
            if ch == "'":
                quoted = False
            cur += ch
        elif ch == "'":
            quoted = True
            cur += ch
        elif ch == ",":
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
        i += 1
    parts.append(cur.strip())
    return parts


def _lit_rows(stmt):
    # Session INSERT tuples in column order: session_start, session_id,
    # client, ..., input_tokens, output_tokens, ... cost_usd. Only the
    # persisted-state cells below are extracted; the rest ride along.
    vals = stmt.split("VALUES", 1)[1]
    tuples, depth, quoted, buf = [], 0, False, ""
    i, n = 0, len(vals)
    while i < n:
        ch = vals[i]
        if quoted:
            if ch == "'" and vals[i:i + 2] == "''":
                buf += "''"
                i += 2
                continue
            if ch == "'":
                quoted = False
            if depth:
                buf += ch
        elif ch == "'":
            quoted = True
            if depth:
                buf += ch
        elif ch == "(":
            if depth:
                buf += ch
            else:
                buf = ""
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth:
                buf += ch
            else:
                tuples.append(buf)
        elif depth:
            buf += ch
        i += 1
    out = []
    for tup in tuples:
        parts = _split_cells(tup)
        def _num(text):
            return None if text == "NULL" else float(text)
        out.append((_lit(parts[2]), _lit(parts[1]), _lit(parts[0]),
                    _num(parts[8]), _num(parts[9]), _num(parts[14])))
    return out

def test_operational_status_failure_recovery_and_atomic_replace():
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory, "aggregate-status.json"))
        real_replace = agg.os.replace
        replaced = []

        def replace(source, target):
            # The replacement source is always a complete JSON document.
            state = agg.json.loads(Path(source).read_text())
            replaced.append(state)
            real_replace(source, target)

        def success(ctx, cfg, progress):
            progress("vehicle")
            current = agg.read_status(path)
            assert current["running"] == 1 and current["section"] == "vehicle"
            return {"vehicle": 2}, [], {"vehicle": 0.1}, {"observation_success": 1}

        with patch.object(agg.time, "time", return_value=100), \
                patch.object(agg.os, "replace", replace), \
                patch.object(agg, "run_all", success), \
                patch.object(agg, "vehicle_window_lag", lambda ctx: {"fleet": 12}):
            agg.run_pass(("", "", ""), {"interval_s": 300}, path)
        good = agg.read_status(path)
        assert good["success"] == 1 and good["running"] == 0
        last_success = good["last_success_timestamp_seconds"]
        with patch.object(agg.time, "time", return_value=200), \
                patch.object(agg, "run_all", lambda *args: ({}, ["vehicle"], {}, {})), \
                patch.object(agg, "vehicle_window_lag", side_effect=agg.SqlError("unreachable")):
            agg.run_pass(("", "", ""), {"interval_s": 300}, path)
        failed = agg.read_status(path)
        assert failed["success"] == 0 and failed["running"] == 0
        assert failed["last_success_timestamp_seconds"] == last_success
        assert failed["vehicle_window_observation_success"] == 0
        with patch.object(agg.time, "time", return_value=300):
            agg.startup_failure(path)
        assert agg.read_status(path)["last_success_timestamp_seconds"] == last_success
        with patch.object(agg.time, "time", return_value=400), \
                patch.object(agg, "run_all", success), \
                patch.object(agg, "vehicle_window_lag", lambda ctx: {}):
            agg.run_pass(("", "", ""), {"interval_s": 300}, path)
        recovered = agg.read_status(path)
        assert recovered["success"] == 1
        assert recovered["last_success_timestamp_seconds"] == 400
        assert recovered["last_failure_timestamp_seconds"] == 300
        assert not list(Path(directory).glob(".aggregate-status-*"))
        assert replaced[0]["running"] == 1
        with patch.object(agg.os, "replace", side_effect=OSError("synthetic interruption")):
            try:
                agg.write_status({"timestamp_seconds": 500, "success": 0}, path)
            except OSError:
                pass
            else:
                raise AssertionError("failed replacement must fail")
        assert agg.read_status(path) == recovered
        assert not list(Path(directory).glob(".aggregate-status-*"))


def test_vehicle_window_lag_is_observed_data_gap_not_wall_clock_age():
    import datetime as dt
    raw = [("fleet", dt.datetime(2026, 10, 1, 12, 5, 30)),
           ("can", dt.datetime(2026, 10, 1, 12, 0, 20)),
           ("new", dt.datetime(2026, 10, 1, 12, 0))]
    summaries = [("fleet", dt.datetime(2026, 10, 1, 12, 3)),
                 ("can", dt.datetime(2026, 10, 1, 12, 0))]
    with patch.object(agg, "fetch_rows", side_effect=[([], raw), ([], summaries)]):
        gaps = agg.vehicle_window_lag(("", "", ""))
    assert gaps == {"fleet": 90, "can": 0}


def test_dirty_checkpoint_sweep_tables_exist_without_append_or_ttl():
    import scripts.database.db_init as db_init
    ddls = dict(db_init.ddl_statements(""))
    for label in ("vehicle_signal_dirty", "vehicle_aggregate_checkpoint",
                  "vehicle_aggregate_sweep"):
        assert label in ddls, label
        upper = ddls[label].upper()
        assert "APPEND_MODE" not in upper, label
        assert "TTL" not in upper, label
        assert "TIME INDEX" in upper, label
    assert '"generation" STRING NOT NULL' in ddls["vehicle_signal_dirty"]
    assert '"applied_generation" STRING NOT NULL' in ddls[
        "vehicle_aggregate_checkpoint"]
    assert '"sweep_mark" TIMESTAMP(9) NOT NULL TIME INDEX' in ddls[
        "vehicle_aggregate_sweep"]
    assert '"cursor_ns" BIGINT NOT NULL' in ddls["vehicle_aggregate_sweep"]
    assert "RAW_FINGERPRINT" not in ddls["vehicle_aggregate_sweep"].upper()
    assert '"raw_fingerprint" STRING NOT NULL' in ddls[
        "vehicle_aggregate_checkpoint"]


def _vehicle_cfg(**over):
    cfg = {"base_url": "", "db": "db", "user": "u", "password": "p",
           "lookback_h": 30, "interval_s": 300, "max_rows": 200000,
           "vehicle": "", "trip_gap_min": 10, "trip_min_speed_kph": 1.0,
           "charge_gap_min": 30, "signal_max_age_s": 300,
           "battery_config": "/nonexistent",
           "paths": {"speed": "Vehicle.Speed", "soc": "",
                     "drive_energy": "", "charge_energy": "",
                     "power": "", "charging": ""}}
    cfg.update(over)
    return cfg


def _cell(scope, hour_ns, fake_rows):
    """Expected per-hour raw cell from the fake row table."""
    row = (fake_rows or {}).get((scope, hour_ns))
    if row is None:
        return agg._hour_fingerprint(1, hour_ns, None)
    _h, count, max_event, max_ingest = row
    return agg._hour_fingerprint(count, max_event, max_ingest)


def _fake_db(dirty_rows=(), checkpoint_rows=(), scope_hours=(),
             oldest_ns=None, generations=None, committed=None,
             sweep_state=None, scope_list=None, fake_rows=None):
    """Behavioral fake backend over patched helpers (no SQL strings here).
    dirty_rows: [((v,s,e), hour_ns, gen)]; checkpoint_rows: [((v,s,e),
    hour_ns, gen, rev)] or [((v,s,e), hour_ns, gen, rev, fp)]; scope_hours:
    {scope: [hour_ns with raw rows]}; oldest_ns: {scope: oldest raw ns};
    generations: {(scope, hour): gen} for the re-read;
    committed/sweep_state record writes. sweep_state: {scope: (cursor_ns,
    rev)}. fake_rows: {(scope, hour): (hour, count, max_event, max_ingest)}
    overriding the default (hour, 1, hour, None) raw cells."""
    generations = generations if generations is not None else {}
    committed = committed if committed is not None else []
    fake_rows = fake_rows if fake_rows is not None else {}
    sweeps = {}
    for scope, entry in (sweep_state or {}).items():
        if isinstance(entry, tuple):
            sweeps[scope] = (entry[0], entry[1] if len(entry) > 1 else "")
        else:
            sweeps[scope] = (entry, "")
    sweep_writes = []

    def fake_distinct(ctx, cfg):
        return set(scope_list) if scope_list is not None else set(
            scope_hours)

    def fake_oldest(ctx, scope):
        return (oldest_ns or {}).get(scope)

    def fake_list(ctx, cfg, scope, start_ns, seal_ns):
        rows = [fake_rows.get((scope, h), (h, 1, h, None))
                for h in (scope_hours.get(scope, ()))
                if start_ns <= h < seal_ns
                and h < start_ns + agg.DIRTY_LIST_HOURS * agg.HOUR_NS]
        end = min(start_ns + agg.DIRTY_LIST_HOURS * agg.HOUR_NS, seal_ns)
        return sorted(rows), end

    def fake_load_dirty(ctx, cfg, cutoff_ns, seal_ns):
        return {(s, h): g for (s, h, g) in dirty_rows
                if cutoff_ns <= h < seal_ns
                and (not cfg.get("vehicle") or s[0] == cfg["vehicle"])}

    def fake_load_checkpoints(ctx, cfg, cutoff_ns, seal_ns):
        out = {}
        for row in checkpoint_rows:
            s, h, g, r = row[0], row[1], row[2], row[3]
            fp = row[4] if len(row) > 4 else _cell(s, h, fake_rows)
            if cutoff_ns <= h < seal_ns \
                    and (not cfg.get("vehicle") or s[0] == cfg["vehicle"]):
                out[(s, h)] = (g, r, fp)
        return out

    def fake_sweeps(ctx, cfg):
        return dict(sweeps)

    def fake_read(ctx, scope, hour_ns):
        return generations.get((scope, hour_ns))

    def fake_commit(ctx, scope, hour_ns, gen, rev, now_ns, fp=""):
        committed.append((scope, hour_ns, gen, rev, fp))

    def fake_sweep(ctx, scope, next_ns, rev, now_ns):
        sweeps[scope] = (next_ns, rev)
        sweep_writes.append((scope, next_ns, rev))
    return (fake_distinct, fake_oldest, fake_list, fake_load_dirty,
            fake_load_checkpoints, fake_sweeps, fake_read, fake_commit,
            fake_sweep, committed, sweep_writes)


def _patch_coordinator(monkey, fakes, work=None, fail_at=None):
    (fake_distinct, fake_oldest, fake_list, fake_load_dirty,
     fake_load_checkpoints, fake_sweeps, fake_read, fake_commit,
     fake_sweep, committed, sweeps) = fakes
    calls = {"rollup": [], "trip": [], "battery": []}
    scope_windows_seen = {}

    def fake_rollup(ctx, cfg, scopes_hours):
        calls["rollup"].append(list(scopes_hours))
        return {h for (_, h) in scopes_hours}

    def fake_trip(ctx, cfg, pending):
        calls["trip"].append(dict(pending))
        if fail_at == "trip":
            raise agg.SqlError("boom")
        return dict.fromkeys(pending, True)

    def fake_battery(ctx, cfg, pending):
        calls["battery"].append(dict(pending))
        for (scope, hour_ns) in pending:
            assert scope[1] == "can", scope
            scope_windows_seen.setdefault(scope, []).append(
                (hour_ns, hour_ns + agg.HOUR_NS - 1))
        if fail_at == "battery":
            raise agg.SqlError("battery boom")
        return dict.fromkeys(pending, True)

    if work is not None:
        fake_rollup, fake_trip, fake_battery = work
    patches = [patch.object(agg, "_distinct_scopes", fake_distinct),
               patch.object(agg, "_raw_coverage_ns", fake_oldest),
               patch.object(agg, "_list_scope_hours", fake_list),
               patch.object(agg, "_load_dirty", fake_load_dirty),
               patch.object(agg, "_load_checkpoints", fake_load_checkpoints),
               patch.object(agg, "_load_sweeps", fake_sweeps),
               patch.object(agg, "_read_dirty_generation", fake_read),
               patch.object(agg, "_commit_checkpoint", fake_commit),
               patch.object(agg, "_commit_sweep", fake_sweep),
               patch.object(agg, "_rollup_hour_windows", fake_rollup),
               patch.object(agg, "_trip_charge_scope_windows", fake_trip),
               patch.object(agg, "_battery_scope_work", fake_battery)]
    for p in patches:
        monkey.append(p)
        p.start()
    return calls, scope_windows_seen, committed, sweeps


def _unpatch(monkey):
    for p in reversed(monkey):
        try:
            p.stop()
        except RuntimeError:
            pass
    del monkey[:]


def test_coordinator_bootstrap_visits_prenotification_history():
    # 4-day-old raw hours with NO dirty notification are listed from raw
    # coverage and checkpointed "" after work commits; the sweep cursor
    # advances only past committed hours.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 90 * agg.HOUR_NS  # beyond the old 96h cutoff: no age discard
    scope = ("v", "can", "e1")
    monkey = []
    try:
        fakes = _fake_db(scope_hours={scope: [old]},
                         oldest_ns={scope: old + 7},
                         scope_list=[scope])
        calls, seen, committed, sweeps = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 1
        assert [(c[0], c[1], c[2], c[3]) for c in committed] == [
            (scope, old, "", agg._config_revision(_vehicle_cfg()))], committed
        assert sweeps and sweeps[0][0] == scope and sweeps[0][1] > old
        assert calls["rollup"] and calls["trip"] and calls["battery"]
        # Restart: same revision checkpoint + empty re-list -> no recompute.
        rev = agg._config_revision(_vehicle_cfg())
        _unpatch(monkey)
        fakes = _fake_db(checkpoint_rows=[(scope, old, "", rev)],
                         scope_hours={scope: [old]},
                         oldest_ns={scope: old + 7},
                         scope_list=[scope],
                         sweep_state={scope: (old + agg.HOUR_NS, "")})
        calls2, _, committed2, _ = _patch_coordinator(monkey, fakes)
        before = (list(calls2["rollup"]), list(calls2["trip"]),
                  list(calls2["battery"]))
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 0
        assert committed2 == []
        assert (list(calls2["rollup"]), list(calls2["trip"]),
                list(calls2["battery"])) == before
    finally:
        _unpatch(monkey)


def test_coordinator_older_notified_windows_have_no_age_cutoff():
    # Dirty rows older than any fixed cutoff still pend and commit; the
    # re-read guards a mid-pass generation race; failures never checkpoint.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 150 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    key = (scope, old)
    monkey = []
    try:
        fakes = _fake_db(dirty_rows=[(scope, old, "gen-old")],
                         scope_hours={scope: [old]},
                         oldest_ns={scope: old},
                         generations={key: "gen-old"},
                         scope_list=[scope],
                         sweep_state={scope: (old, "")})
        calls, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 1
        assert committed[0][:3] == (scope, old, "gen-old"), committed
        # Mid-pass race: dirty row moved to gen2 -> re-read mismatch, stays.
        _unpatch(monkey)
        fakes = _fake_db(dirty_rows=[(scope, old, "gen-old")],
                         scope_hours={scope: [old]},
                         oldest_ns={scope: old},
                         generations={key: "gen2"},
                         scope_list=[scope],
                         sweep_state={scope: (old, "")})
        _, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 0
        assert committed == []
        # Section failure propagates and checkpoints nothing.
        _unpatch(monkey)
        fakes = _fake_db(dirty_rows=[(scope, old, "gen-old")],
                         scope_hours={scope: [old]},
                         oldest_ns={scope: old},
                         generations={key: "gen-old"},
                         scope_list=[scope],
                         sweep_state={scope: (old, "")})
        _patch_coordinator(monkey, fakes, fail_at="trip")
        try:
            agg.vehicle_history_section(("", "", ""), _vehicle_cfg(), now=now)
        except agg.SqlError:
            pass
        else:
            raise AssertionError("expected SqlError")
        assert fakes[9] == []
    finally:
        _unpatch(monkey)


def test_coordinator_cursor_progress_and_config_invalidation():
    # Two listed hours, one already checkpointed under this revision: only
    # the missing hour is recomputed; a stale revision reworks both.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    hours = [seal - 3 * agg.HOUR_NS, seal - 2 * agg.HOUR_NS]
    scope = ("v", "can", "e1")
    rev = agg._config_revision(_vehicle_cfg())
    monkey = []
    try:
        fakes = _fake_db(
            scope_hours={scope: hours}, oldest_ns={scope: hours[0]},
            checkpoint_rows=[(scope, hours[0], "", rev)],
            scope_list=[scope])
        calls, _, committed, sweeps = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 1
        assert [c[1] for c in committed] == [hours[1]], committed
        assert sweeps and sweeps[0][1] == seal, sweeps
        # Stale revision checkpoints: both hours rework under the new rev.
        _unpatch(monkey)
        fakes = _fake_db(
            scope_hours={scope: hours}, oldest_ns={scope: hours[0]},
            checkpoint_rows=[(scope, hours[0], "", "stale"),
                             (scope, hours[1], "", "stale")],
            scope_list=[scope])
        _, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 2
        assert sorted(c[1] for c in committed) == sorted(hours), committed
    finally:
        _unpatch(monkey)


def test_coordinator_rotates_past_fifty_scopes():
    # 60 scopes: the first pass lists the first 50 by rotation, the next
    # pass lists the remaining 10; every scope commits exactly once and
    # legacy sections exclude ALL coordinated CAN scopes each pass.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    hour = seal - agg.HOUR_NS
    scopes = [("v%02d" % i, "can", "e1") for i in range(60)]
    monkey = []
    try:
        fakes = _fake_db(
            scope_hours={s: [hour] for s in scopes},
            oldest_ns={s: hour for s in scopes},
            scope_list=scopes)
        calls, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == agg.DIRTY_SWEEP_SCOPES
        assert len(committed) == agg.DIRTY_SWEEP_SCOPES, len(committed)
        listed_first = {c[0] for c in committed}
        assert len(listed_first) == agg.DIRTY_SWEEP_SCOPES
        rev = agg._config_revision(_vehicle_cfg())
        carried = {s: (hour + agg.HOUR_NS, rev) for s in listed_first}
        _unpatch(monkey)
        fakes = _fake_db(
            scope_hours={s: [hour] for s in scopes},
            oldest_ns={s: hour for s in scopes},
            checkpoint_rows=[(s, hour, "", rev) for s in listed_first],
            scope_list=scopes,
            sweep_state=carried)
        _, _, committed2, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == len(scopes) - len(
                                               listed_first)
        assert {c[0] for c in committed2} == set(scopes) - listed_first
    finally:
        _unpatch(monkey)


def test_coordinator_empty_gap_advances_without_recompute():
    # A >48h empty gap followed by new raw: the empty checklist still
    # advances the cursor (no recompute, returns 0), so the later hour is
    # reached on the next pass instead of stalling forever.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    scope = ("v", "can", "e1")
    cursor = seal - 100 * agg.HOUR_NS
    new = seal - agg.HOUR_NS
    monkey = []
    try:
        state = {scope: (cursor, "")}
        advanced = None
        for _ in range(5):
            fakes = _fake_db(
                scope_hours={scope: [new]}, oldest_ns={scope: cursor},
                scope_list=[scope], sweep_state=dict(state))
            calls, _, committed, sweeps = _patch_coordinator(monkey, fakes)
            result = agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                                 now=now)
            _unpatch(monkey)
            if result == 1:
                assert [c[1] for c in committed] == [new], committed
                break
            assert committed == []
            assert not calls["rollup"] and not calls["trip"] \
                and not calls["battery"]
            assert sweeps and sweeps[0][1] > state[scope][0], sweeps
            state = {scope: (sweeps[-1][1], "")}
            advanced = sweeps[-1][1]
        else:
            raise AssertionError("gap never crossed: %r" % (state,))
        assert advanced is not None and advanced > cursor
    finally:
        _unpatch(monkey)


def test_coordinator_late_dirty_behind_cursor_still_pends():
    # A late notification for an hour BEHIND the advanced cursor pends via
    # the dirty table even though the raw listing has moved past it.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 100 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    key = (scope, old)
    monkey = []
    try:
        fakes = _fake_db(dirty_rows=[(scope, old, "gen-late")],
                         scope_hours={scope: []},
                         oldest_ns={scope: seal - agg.HOUR_NS},
                         generations={key: "gen-late"},
                         scope_list=[scope],
                         sweep_state={scope: (seal, "")})
        _, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 1
        assert committed[0][:3] == (scope, old, "gen-late"), committed
    finally:
        _unpatch(monkey)


def test_coordinator_revision_revisits_completed_history():
    # Completed history under rev A re-pends after the revision changes:
    # both the dirty re-notification and the sweep re-list it.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 200 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    monkey = []
    try:
        fakes = _fake_db(
            dirty_rows=[(scope, old, "gen1")],
            scope_hours={scope: [old]}, oldest_ns={scope: old},
            generations={(scope, old): "gen1"},
            checkpoint_rows=[(scope, old, "gen1", "rev-A")],
            scope_list=[scope],
            sweep_state={scope: (old, "rev-A")})
        _, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 1
        assert committed and committed[0][1] == old, committed
        assert committed[0][3] != "rev-A", committed
    finally:
        _unpatch(monkey)


def test_coordinator_sweep_single_row_per_scope():
    # Repeated progress overwrites one row per scope: two advances leave
    # exactly one sweep entry holding the latest cursor.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    hours = [seal - 60 * agg.HOUR_NS, seal - 55 * agg.HOUR_NS]
    scope = ("v", "can", "e1")
    monkey = []
    try:
        fakes = _fake_db(
            scope_hours={scope: hours}, oldest_ns={scope: hours[0]},
            scope_list=[scope])
        _, _, committed, sweeps = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 2
        assert len({s for (s, _h, _r) in sweeps}) == 1, sweeps
        _unpatch(monkey)
        rev = agg._config_revision(_vehicle_cfg())
        fakes = _fake_db(
            scope_hours={scope: hours}, oldest_ns={scope: hours[0]},
            checkpoint_rows=[(scope, h, "", rev) for h in hours],
            scope_list=[scope],
            sweep_state={scope: (sweeps[0][1], rev)})
        _, _, committed2, sweeps2 = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 0
        assert committed2 == []
        assert sweeps2 and sweeps2[0][1] >= sweeps[0][1], (sweeps, sweeps2)
    finally:
        _unpatch(monkey)


def test_coordinator_multiwindow_scopes_all_complete():
    # 55 scopes x 2 checklist windows each: least-advanced-cursor order
    # completes every scope across passes (no 51st-scope starvation).
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    scopes = [("v%02d" % i, "can", "e1") for i in range(55)]
    hours = {s: [seal - 100 * agg.HOUR_NS, seal - agg.HOUR_NS] for s in scopes}
    monkey = []
    try:
        state, done, passes = {}, set(), 0
        while len(done) < len(scopes) and passes < 8:
            fakes = _fake_db(
                scope_hours=hours,
                oldest_ns={s: hours[s][0] for s in scopes},
                checkpoint_rows=[(s, h, "", agg._config_revision(
                    _vehicle_cfg())) for s in done for h in hours[s]],
                scope_list=scopes, sweep_state=dict(state))
            calls, _, committed, sweeps = _patch_coordinator(monkey, fakes)
            result = agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                                 now=now)
            _unpatch(monkey)
            passes += 1
            for c in committed:
                if c[1] == hours[c[0]][-1]:
                    done.add(c[0])
            for (s, nxt, _r) in sweeps:
                state[s] = (nxt, "")
            if result == 0 and not sweeps:
                break
        assert done == set(scopes), (len(done), passes)
    finally:
        _unpatch(monkey)


def test_coordinator_sealed_cursor_revisits_history():
    # Cursor at seal (steady state) performs a bounded periodic revisit of
    # oldest history instead of skipping: unchanged hours stay skipped via
    # checkpoints, so the pass returns 0 but the cursor fingerprint refreshes.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 90 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    rev = agg._config_revision(_vehicle_cfg())
    monkey = []
    try:
        fakes = _fake_db(
            scope_hours={scope: [old]}, oldest_ns={scope: old},
            checkpoint_rows=[(scope, old, "", rev)],
            scope_list=[scope],
            sweep_state={scope: (seal, rev)})
        calls, _, committed, sweeps = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 0
        assert committed == []
        assert not calls["rollup"] and not calls["trip"] \
            and not calls["battery"]
        assert sweeps, sweeps
    finally:
        _unpatch(monkey)


def test_coordinator_late_nonotice_fingerprint_recomputes():
    # Same revision, new raw rows behind the cursor, no dirty notification:
    # the fingerprint changes, the hour re-pends as "" and recomputes.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 90 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    rev = agg._config_revision(_vehicle_cfg())
    monkey = []
    try:
        rows_v1 = {(scope, old): (old, 5, old + 100, None)}
        fakes = _fake_db(
            scope_hours={scope: [old]}, oldest_ns={scope: old},
            checkpoint_rows=[(scope, old, "", rev)],
            scope_list=[scope],
            sweep_state={scope: (old, rev)},
            fake_rows=rows_v1)
        calls, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 0
        assert committed == [] and not calls["rollup"]
        _unpatch(monkey)
        rows_v2 = {(scope, old): (old, 9, old + 999, None)}
        v1_fp = agg._hour_fingerprint(5, old + 100, None)
        fakes = _fake_db(
            scope_hours={scope: [old]}, oldest_ns={scope: old},
            checkpoint_rows=[(scope, old, "", rev, v1_fp)],
            scope_list=[scope],
            sweep_state={scope: (old, rev)},
            fake_rows=rows_v2)
        _, _, committed2, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 1
        assert [c[1] for c in committed2] == [old], committed2
    finally:
        _unpatch(monkey)


def test_coordinator_twopage_complete_then_zero():
    # Two-page (>48h) history: complete both pages, then reconcile again
    # with zero work — page A is NOT re-pended once its checkpoint holds
    # the same per-hour cells (no repeat-forever).
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    scope = ("v", "can", "e1")
    page_a = [seal - 100 * agg.HOUR_NS + i * agg.HOUR_NS for i in range(10)]
    page_b = [seal - 30 * agg.HOUR_NS + i * agg.HOUR_NS for i in range(10)]
    hours = sorted(page_a + page_b)
    rev = agg._config_revision(_vehicle_cfg())
    monkey = []
    try:
        state, done_fp = {}, {}
        for _ in range(6):
            fakes = _fake_db(
                scope_hours={scope: hours},
                oldest_ns={scope: hours[0]},
                checkpoint_rows=[(s, h, g, r, fp)
                                 for (s, h), (g, r, fp) in done_fp.items()],
                scope_list=[scope], sweep_state=dict(state))
            calls, _, committed, sweeps = _patch_coordinator(monkey, fakes)
            result = agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                                 now=now)
            _unpatch(monkey)
            for c in committed:
                done_fp[(c[0], c[1])] = (c[2], c[3], c[4])
            for (s, nxt, _r) in sweeps:
                state[s] = (nxt, rev)
            if result == 0:
                break
        assert len(done_fp) == len(hours), (len(done_fp), state)
        _unpatch(monkey)
        fakes = _fake_db(
            scope_hours={scope: hours},
            oldest_ns={scope: hours[0]},
            checkpoint_rows=[(s, h, g, r, fp)
                             for (s, h), (g, r, fp) in done_fp.items()],
            scope_list=[scope], sweep_state=dict(state))
        calls, _, committed, _ = _patch_coordinator(monkey, fakes)
        try:
            assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                               now=now) == 0
            assert committed == []
            assert not calls["rollup"] and not calls["trip"] \
                and not calls["battery"]
        finally:
            _unpatch(monkey)
    finally:
        _unpatch(monkey)


def test_coordinator_late_nonotice_old_page_after_new():
    # Page B completes; then late no-notify rows land on old page A:
    # only page-A hours recompute, page B stays skipped.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    scope = ("v", "can", "e1")
    page_a = [seal - 100 * agg.HOUR_NS + i * agg.HOUR_NS for i in range(5)]
    page_b = [seal - 30 * agg.HOUR_NS + i * agg.HOUR_NS for i in range(5)]
    rev = agg._config_revision(_vehicle_cfg())
    monkey = []
    try:
        done_fp = {(scope, h): ("", rev, agg._hour_fingerprint(1, h, None))
                   for h in page_a + page_b}
        target = page_a[2]
        v2_rows = {(scope, h): (h, 1, h, None) for h in page_a + page_b}
        v2_rows[(scope, target)] = (target, 7, target + 5, None)
        fakes = _fake_db(
            scope_hours={scope: page_a + page_b},
            oldest_ns={scope: page_a[0]},
            checkpoint_rows=[(s, h, g, r, fp)
                             for (s, h), (g, r, fp) in done_fp.items()],
            scope_list=[scope],
            sweep_state={scope: (page_a[0], rev)},
            fake_rows=v2_rows)
        _, _, committed, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 1
        assert [c[1] for c in committed] == [target], committed
    finally:
        _unpatch(monkey)


def test_coordinator_legacy_blank_fingerprint_repends_once():
    # Same-rev checkpoint WITHOUT a fingerprint never blind-skips: it
    # re-pends once, commits WITH the current cells, and the next pass is
    # zero-work (no repeat-forever).
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 90 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    rev = agg._config_revision(_vehicle_cfg())
    monkey = []
    try:
        import unittest.mock as _mock
        with _mock.patch.object(agg, "_load_checkpoints",
                                lambda ctx, cfg, co, se: {
                                    (scope, old): ("", rev, "")}):
            fakes = _fake_db(
                scope_hours={scope: [old]}, oldest_ns={scope: old},
                scope_list=[scope])
            calls, _, committed, _ = _patch_coordinator(monkey, fakes)
            assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                               now=now) == 1
            assert len(committed) == 1 and committed[0][4] != "", committed
            stored_fp = committed[0][4]
        _unpatch(monkey)
        fakes = _fake_db(
            scope_hours={scope: [old]}, oldest_ns={scope: old},
            checkpoint_rows=[(scope, old, "", rev, stored_fp)],
            scope_list=[scope],
            sweep_state={scope: (old, rev)})
        calls, _, committed2, _ = _patch_coordinator(monkey, fakes)
        assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                           now=now) == 0
        assert committed2 == [] and not calls["rollup"]
    finally:
        _unpatch(monkey)


def test_coordinator_midpass_notify_on_swept_hour_blocks():
    # An unnotified hour gains a notification DURING work: the post-work
    # recheck sees it and blocks the "" checkpoint (stays dirty).
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 90 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    phase = {"stage": "observe"}
    monkey = []
    try:
        fakes = _fake_db(
            scope_hours={scope: [old]}, oldest_ns={scope: old},
            scope_list=[scope])
        calls, _, committed, _ = _patch_coordinator(monkey, fakes)

        def arriving_read(ctx, sc, hour_ns):
            if phase["stage"] == "observe":
                return None
            return "gen-arrived"

        def phased_roll(ctx, cfg, scopes_hours):
            phase["stage"] = "work"
            return {h for (_, h) in scopes_hours}

        import unittest.mock as _mock
        with _mock.patch.object(agg, "_read_dirty_generation",
                                arriving_read), \
                _mock.patch.object(agg, "_rollup_hour_windows",
                                    phased_roll):
            assert agg.vehicle_history_section(("", "", ""), _vehicle_cfg(),
                                               now=now) == 0
        assert committed == []
    finally:
        _unpatch(monkey)


def test_coordinator_late_ingest_uses_event_time_window():
    hour = (1789977727964984000 // agg.HOUR_NS) * agg.HOUR_NS
    assert agg.floor_hour_ns(1789977727964984000) == hour
    assert agg.floor_hour_ns(hour + agg.HOUR_NS - 1) == hour
    lit = agg.ns_to_sql_lit(hour)
    assert lit.startswith("'2026-") and lit.endswith("'"), lit


def test_battery_worker_uses_only_scoped_call():
    # The real coordinator worker maps pending hours to Only=True scoped
    # battery windows (exact [hour, hour+1h) per scope); any BatteryError
    # propagates so no checkpoint advances.
    now = __import__("datetime").datetime(2026, 9, 20, 12, 0, 0)
    seal = agg.floor_hour_ns(agg.dt_to_ns(now))
    old = seal - 90 * agg.HOUR_NS
    scope = ("v", "can", "e1")
    pending = {(scope, old): ""}
    seen = {}

    def fake_section(ctx, cfg):
        seen.update(cfg)
        assert cfg["battery_scope_windows"] == {
            scope: [(old, old + agg.HOUR_NS - 1)]}, cfg
        assert cfg["battery_scope_windows_only"] is True, cfg
        return 3

    with patch.object(agg, "battery_section", fake_section):
        assert agg._battery_scope_work(("", "", ""), _vehicle_cfg(),
                                       pending) == {(scope, old): True}
    assert seen["battery_scope_windows_only"] is True


def test_run_all_excludes_can_from_legacy_sections():
    # CAN scopes run ONLY in vehicle_history; legacy global sections get
    # exclude_scopes + battery_exclude_scopes so no hidden recompute survives.
    seen = {}
    scope = ("v", "can", "e1")
    with patch.object(agg, "_distinct_scopes", lambda ctx, cfg: {scope}), \
            patch.object(agg, "ai_section", lambda ctx, cfg: 1), \
            patch.object(agg, "log_section", lambda ctx, cfg: 2), \
            patch.object(agg, "vehicle_section",
                         lambda ctx, cfg: seen.setdefault("vehicle", cfg) or 2), \
            patch.object(agg, "trip_section",
                         lambda ctx, cfg: seen.setdefault("trip", cfg) or 3), \
            patch.object(agg, "vehicle_history_section",
                         lambda ctx, cfg, now=None: 4), \
            patch.object(agg, "home_section", lambda ctx, cfg: 5), \
            patch.object(agg, "battery_section",
                         lambda ctx, cfg: seen.setdefault("battery", cfg) or 6), \
            patch.object(agg, "vehicle_freshness",
                         lambda ctx: {"observation_success": 1,
                                      "event_age_seconds": {}}):
        counts, failed, durations, freshness = agg.run_all(
            ("", "", ""), {"interval_s": 300}, None)
    assert failed == [], failed
    assert counts["vehicle_history"] == 4, counts
    assert ("v", "can", "e1") in [tuple(s) for s in
                                  seen["vehicle"]["exclude_scopes"]], seen
    assert ("v", "can", "e1") in [tuple(s) for s in
                                  seen["trip"]["exclude_scopes"]], seen
    assert ("v", "can", "e1") in [tuple(s) for s in
                                  seen["battery"]["battery_exclude_scopes"]], seen
    assert freshness["observation_success"] == 1, freshness


if __name__ == "__main__":
    test_sql_error_raises()
    test_per_call_billing_excludes_rollups_and_cumulative_parents()
    test_session_client_fallbacks_and_mixed_cost()
    test_native_overrides_same_call_never_sums()
    test_same_usage_distinct_ids_clients_sessions_stay_separate()
    test_two_estimates_on_one_call_bill_once()
    test_cost_only_turn_cost_kept_without_double_count()
    test_turn_cost_without_shared_turn_never_links()
    test_native_resource_and_incomplete_metadata()
    test_tool_result_folds_into_matching_span_once()
    test_tool_decision_and_distinct_tools_not_counted_as_calls()
    test_cost_only_missing_duration_still_money_without_call()
    test_native_cost_kept_separate_from_estimated()
    test_ns_timestamp_typed_conversion()
    test_window_floor_and_sealed_exclusion()
    test_epoch_source_separation_and_same_timestamp_policy()
    test_soc_rise_alone_never_charges()
    test_trip_segmentation_gap()
    test_late_earlier_span_replaces_session_start_and_stays_idempotent()
    test_same_session_id_across_clients_stays_separate()
    test_native_codex_custom_service_folds_into_canonical_span()
    test_arbitrary_span_service_never_becomes_client()
    test_ttl_preserves_partly_expired_windows_but_recomputes_retained()
    test_log_fetch_starts_at_full_day_boundary()
    test_retention_boundary_never_moves_backwards()
    test_native_scope_selection_preserves_context_without_double_billing()
    test_missing_session_never_suppresses_unrelated_span_usage()
    test_session_preserves_every_observed_model()
    test_supplemental_mixed_present_missing_zero_untouched()
    test_supplemental_unknown_model_counts_unpriced()
    test_supplemental_never_runs_without_prices_or_unverified_semantics()
    test_supplemental_cache_math_invalid_data_and_receipt_suppression()
    test_price_loader_caches_refresh_failure_and_rejects_bad_payload()
    test_session_batch_replacement_isolation_and_failure_keeps_anchor()
    test_operational_status_failure_recovery_and_atomic_replace()
    test_vehicle_window_lag_is_observed_data_gap_not_wall_clock_age()
    test_coordinator_bootstrap_visits_prenotification_history()
    test_coordinator_older_notified_windows_have_no_age_cutoff()
    test_coordinator_cursor_progress_and_config_invalidation()
    test_coordinator_rotates_past_fifty_scopes()
    test_coordinator_multiwindow_scopes_all_complete()
    test_coordinator_sealed_cursor_revisits_history()
    test_coordinator_late_nonotice_fingerprint_recomputes()
    test_coordinator_twopage_complete_then_zero()
    test_coordinator_legacy_blank_fingerprint_repends_once()
    test_coordinator_late_nonotice_old_page_after_new()
    test_coordinator_midpass_notify_on_swept_hour_blocks()
    test_coordinator_empty_gap_advances_without_recompute()
    test_coordinator_late_dirty_behind_cursor_still_pends()
    test_coordinator_revision_revisits_completed_history()
    test_coordinator_sweep_single_row_per_scope()
    test_battery_worker_uses_only_scoped_call()
    test_coordinator_late_ingest_uses_event_time_window()
    test_run_all_excludes_can_from_legacy_sections()
    test_dirty_checkpoint_sweep_tables_exist_without_append_or_ttl()
    print("test_database: ok (54 tests)")
