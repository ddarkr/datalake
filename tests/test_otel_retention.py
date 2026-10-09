"""Scoped regression tests for OTel retention policy (issue #7).

Framework-free: plain asserts + stdlib fake HTTP server. Run with:
  python3 -m tests.test_otel_retention

Owns TTL inspection/reinit semantics and TTL-boundary aggregate behavior.
Imports the aggregate public TTL contract read-only; tests/test_database.py
and aggregate.py stay owned by #1.
"""

import datetime as dt
import http.server
import json
import threading
import time
import urllib.parse

from scripts.analytics import aggregate as agg
from scripts.database import db_init


class Handler(http.server.BaseHTTPRequestHandler):
    seen = []
    span_cols = None
    span_rows = None
    log_cols = None
    log_rows = None
    summary_rows = []
    watermark_rows = [[None]]

    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        stmt = urllib.parse.parse_qs(body).get("sql", [""])[0]
        Handler.seen.append(stmt)
        if stmt.startswith("SELECT"):
            low = stmt.lower()
            if "max(ingest_time)" in low:
                # Ingest probe: tables carry server-stamped ingest_time.
                cols, rows = ["max"], [["2026-09-21 11:00:00"]]
            elif "is null" in low and "count(" in low:
                # Legacy-NULL fingerprint: no unstamped rows in the fixture.
                cols, rows = ["count", "max"], [[0, None]]
            elif "FROM raw_retention_watermark" in stmt:
                cols, rows = ["deleted_before"], Handler.watermark_rows
            elif "FROM ai_session_summary" in stmt:
                cols, rows = (["client", "session_id", "session_start"],
                              Handler.summary_rows)
            elif "FROM ai_aggregate_state" in stmt:
                # Zero rows: incremental path falls back to full rebuild.
                cols, rows = (["completed_through_ingest", "price_digest",
                               "config_revision", "input_digest"], [])
            elif "FROM opentelemetry_traces" in stmt:
                if Handler.span_cols is not None:
                    cols, rows = Handler.span_cols, Handler.span_rows
                else:
                    cols, rows = ["m"], []
            elif "FROM opentelemetry_logs" in stmt:
                if Handler.log_cols is not None:
                    cols, rows = Handler.log_cols, Handler.log_rows
                else:
                    cols, rows = ["m"], []
            elif "information_schema" in stmt:
                cols, rows = ["table_name", "create_options"], []
            else:
                cols, rows = ["n"], [[0]]
            payload = {"output": [{"records": {
                "schema": {"column_schemas": [{"name": c} for c in cols]},
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
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _reset(span=None, logs=None, summary=None):
    Handler.seen.clear()
    Handler.span_cols, Handler.span_rows = span if span else (None, None)
    Handler.log_cols, Handler.log_rows = logs if logs else (None, None)
    Handler.summary_rows = summary if summary is not None else []
    Handler.watermark_rows = [[None]]


def _span_dict(ts, trace, span, session, call, inp, outp, client="codex"):
    return {"timestamp": ts, "timestamp_end": ts, "duration_nano": None,
            "trace_id": trace, "span_id": span, "parent_span_id": None,
            "span_name": "chat", "span_status_code": "OK",
            "service_name": "svc",
            agg.TOKEN_IN: inp, agg.TOKEN_OUT: outp,
            agg.TOKEN_CACHE_READ: None, agg.TOKEN_CACHE_CREATE: None,
            agg.TOKEN_CACHE_WRITE: None, agg.TOKEN_REASON: None,
            agg.COST_EST: None, agg.COST_SRC: None, agg.COST_NATIVE: None,
            agg.RESP_ID: call, agg.TOOL_CALL_ID: None,
            agg.OP_NAME: "chat", agg.CONV_ID: None,
            agg.SESSION: session, agg.CLIENT: client,
            agg.PROVIDER: None, agg.MODEL_REQ: "m", agg.MODEL_RESP: "m",
            agg.TOOL_NAME: None}


def _span_payload(dicts):
    cols = list(agg.SPAN_COLS) + ["ingest_time"]
    return cols, [[d.get(c, "2026-09-21 11:00:00") for c in cols]
                  for d in dicts]


def _log_row(ts, session, call, inp, outp, client="codex", scope="codex"):
    attrs = json.dumps({"event.name": "codex.sse_event",
                        "event.kind": "response.completed",
                        "coding_agent.session.id": session,
                        "coding_agent.client": client,
                        "gen_ai.response.id": call,
                        "input_token_count": inp,
                        "output_token_count": outp})
    return [ts, "INFO", 9, scope, None, None, "", attrs, json.dumps({})]


def _log_payload(rows):
    cols = list(agg.LOG_COLS) + ["ingest_time"]
    return cols, [list(r[:len(agg.LOG_COLS)]) + ["2026-09-21 11:00:00"]
                  for r in rows]


def _no_network_prices():
    agg._PRICES["table"] = None
    agg._PRICES["ok_at"] = time.time()
    agg._PRICES["attempt_at"] = time.time()








def test_invalid_ttl_rejected():
    for bad in ("7", "d7", "7D", "7w", "-7d", "7 days", "0"):
        try:
            agg.ttl_boundary(bad, dt.datetime(2026, 9, 21, 12))
        except agg.SqlError:
            pass
        else:
            raise AssertionError("TTL accepted: " + bad)
    # "0s" is unlimited, never immediate expiry.
    assert agg.ttl_boundary("0s", dt.datetime(2026, 9, 21, 12)) is None
    assert agg.ttl_boundary("", dt.datetime(2026, 9, 21, 12)) is None


def test_ttl_boundary_exact_arithmetic():
    now = dt.datetime(2026, 9, 21, 12, 0, 0)
    assert agg.ttl_boundary("7d", now) == now - dt.timedelta(days=7)
    assert agg.ttl_boundary("90d", now) == now - dt.timedelta(days=90)
    assert agg.ttl_boundary("12h", now) == now - dt.timedelta(hours=12)
    assert agg.ttl_boundary("30m", now) == now - dt.timedelta(minutes=30)
    assert agg.ttl_boundary("60s", now) == now - dt.timedelta(seconds=60)
    bound = agg.ttl_boundary("7d", now)
    assert agg.fully_retained(bound, "7d", now) is True
    assert agg.fully_retained(bound - dt.timedelta(seconds=1), "7d", now) is False


def test_ttl_boundary_ai_section_keeps_expired_aggregates():
    # Long session "long" spans the 7d boundary (head 09-10, tail 09-21):
    # raw is partly expired, so the stored aggregate must survive and only
    # the fully retained session/day may be (re)written.
    now = dt.datetime(2026, 9, 21, 12, 0, 0)
    spans = _span_payload([
        _span_dict("2026-09-10 10:00:00", "t-old", "s-old", "long",
                   "call-old", 500, 500),
        _span_dict("2026-09-21 10:00:00", "t-tail", "s-tail", "long",
                   "call-tail", 11, 5),
        _span_dict("2026-09-21 10:05:00", "t-fresh", "s-fresh", "fresh",
                   "call-fresh", 11, 5),
    ])
    logs = _log_payload([
        _log_row("2026-09-10 09:00:00", "long", "resp-late-1", 7, 3),
    ])
    summary = [("codex", "long", "2026-09-10 09:00:00"),
               ("codex", "fresh", "2026-09-21 09:00:00")]
    srv = serve()
    orig_utcnow = agg.utcnow
    try:
        _reset(spans, logs, summary)
        _no_network_prices()
        agg.utcnow = lambda: now
        ctx = ("http://127.0.0.1:%d" % srv.server_port, "YXV0aA==", "db")
        total = agg.ai_section(ctx, {"otel_ttl": "7d", "max_rows": 1000})
        assert total >= 1
        writes = [s for s in Handler.seen
                  if s.startswith("INSERT") or s.startswith("DELETE")]
        # Expired long session untouched: no write may reference it.
        assert not [s for s in writes if "long" in s], writes
        # Fully retained session written.
        sess = [s for s in writes if "INTO ai_session_summary" in s]
        assert sess and any("fresh" in s for s in sess), writes
        # Partially expired day kept, retained day recomputed.
        daily = [s for s in writes if "INTO ai_daily_summary" in s]
        assert daily and any("2026-09-21" in s for s in daily), writes
        assert not [s for s in daily if "2026-09-10" in s], writes
    finally:
        agg.utcnow = orig_utcnow
        _reset()
        srv.shutdown()


def test_ttl_boundary_log_section_keeps_expired_day():
    now = dt.datetime(2026, 9, 21, 12, 0, 0)
    logs = _log_payload([
        _log_row("2026-09-10 09:00:00", "late-sess", "resp-late", 2, 1),
        _log_row("2026-09-21 09:00:00", "cur-sess", "resp-cur", 4, 2),
    ])
    srv = serve()
    orig_utcnow = agg.utcnow
    try:
        _reset(None, logs)
        _no_network_prices()
        agg.utcnow = lambda: now
        ctx = ("http://127.0.0.1:%d" % srv.server_port, "YXV0aA==", "db")
        total = agg.log_section(ctx, {"otel_ttl": "7d", "max_rows": 1000,
                                      "lookback_h": 24 * 400})
        assert total >= 1
        inserts = [s for s in Handler.seen if "INTO ai_log_daily" in s]
        assert inserts and any("2026-09-21" in s for s in inserts), Handler.seen
        assert not [s for s in inserts if "2026-09-10" in s], inserts
    finally:
        agg.utcnow = orig_utcnow
        _reset()
        srv.shutdown()


_TESTS = (
    test_invalid_ttl_rejected,
    test_ttl_boundary_exact_arithmetic,
    test_ttl_boundary_ai_section_keeps_expired_aggregates,
    test_ttl_boundary_log_section_keeps_expired_day,
)


if __name__ == "__main__":
    for fn in _TESTS:
        fn()
    print("test_otel_retention: ok (%d tests)" % len(_TESTS))
