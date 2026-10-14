#!/usr/bin/env python3
"""AI incremental benchmark: baseline full rebuild vs fenced incremental.

Isolated actual-Greptime runner. Production tables/data are never touched.

Pinned fixture: one sha256-pinned VALUES blob per matrix cell, shared by
both sides. Each side runs ai_section in an isolated subprocess importing
only its own tree (baseline: --baseline-root = immutable original HEAD,
whose ai_section is the timeless full scan; current: this tree, whose
ai_section defaults to the per-region sequence fence and takes
AGG_AI_FULL_REBUILD=1 only for the explicit-recovery cell).

Matrix: history {small, large} x new input {none, few, session}. Arrival
order is the commit order: history rows land first, new rows land later.
No ingest_time column is written by the fixture (fresh db_init carries no
ingest column for the fence path); discovery needs the committed sequence,
never a timestamp. An old-event-time row committed late is the delayed
case and rides the new-input leg.

Measurement is honest about history-linear cost: every HTTP SQL statement
is instrumented (result rows, server execution_time_ms, client wall/cpu,
request bytes) by category (raw/metric_discovery/state/derived_write/
watermark/other), every Flight fence read is instrumented (rows, upper
watermark regions, client wall/cpu), plus process wall/cpu/peak-RSS around
the aggregate call and the state fence content. The oracle compares ALL
related derived tables cell-by-cell (ai_session_summary, ai_activity_daily,
ai_daily_summary, ai_tool_daily), never row counts. History flush
(ADMIN FLUSH_TABLE) runs after fixture load because the fence
intentionally reads deltas, not SSTs; placement is stated, not hidden.

Run (shared native runtime owned by Main):
  /tmp/datalake-issues-20261009-venv/bin/python \\
    tools/benchmark_ai_incremental.py \\
    --base-url http://127.0.0.1:56955 \\
    --grpc-url grpc://127.0.0.1:56956 --db ai_incr_issue1_bench \\
    --user issue_synthetic --password-env ISSUE_SYNTH_PASS \\
    --baseline-root /tmp/datalake-issues-20261009-baseline \\
    --matrix --repeats 3 --history-sizes 200,2000
Single cell:
  ... tools/benchmark_ai_incremental.py --history 200 --new few ...
Without --baseline-root every side is labeled current-only (no baseline).
Credentials via env only, never argv. Synthetic values only.
"""

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.database import db_init  # noqa: E402

SCHEMA = "ai-incremental-benchmark/2"
FIXTURE_BASE = dt.datetime(2026, 9, 20, 10, 0, 0)
LATE_BASE = dt.datetime(2026, 9, 21, 10, 0, 0)
DB_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _fmt(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _q(value):
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def sql(base_url, auth, db, stmt, timeout=120):
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db, safe="")
    body = urllib.parse.urlencode({"sql": stmt}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    if auth:
        req.add_header("Authorization", "Basic " + auth)
    req.add_header("X-Greptime-Timezone", "UTC")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8", "replace"))
    if payload.get("error"):
        raise RuntimeError(str(payload["error"])[:300])
    return payload, len(body)


def rows_of(payload):
    rec = payload["output"][0].get("records") or {}
    return rec.get("rows", [])


def count_of(base_url, auth, db, stmt):
    payload, _ = sql(base_url, auth, db, stmt)
    rows = rows_of(payload)
    return rows[0][0] if rows else 0


# ---------------------------------------------------------------- fixture

def span_val(i, ts, session, client, inp, outp, call):
    return "(" + ", ".join(_q(v) for v in (
        _fmt(ts), "t%d" % i, "s%d" % i, "chat", call, session, client,
        inp, outp, "bench-m", "bench-p", "bench-m")) + ")"


SPAN_COLS_SQL = ('"timestamp", "trace_id", "span_id", '
                 '"span_attributes.gen_ai.operation.name", '
                 '"span_attributes.gen_ai.response.id", '
                 '"span_attributes.coding_agent.session.id", '
                 '"span_attributes.coding_agent.client", '
                 '"span_attributes.gen_ai.usage.input_tokens", '
                 '"span_attributes.gen_ai.usage.output_tokens", '
                 '"span_attributes.gen_ai.request.model", '
                 '"span_attributes.gen_ai.provider.name", '
                 '"span_attributes.gen_ai.response.model"')

LOG_COLS_SQL = ('"timestamp", "severity_text", "severity_number", '
                '"scope_name", "log_attributes", "resource_attributes"')


def log_val(ts, session, call, inp, outp):
    attrs = json.dumps({"event.name": "codex.sse_event",
                        "event.kind": "response.completed",
                        "session.id": session, "request_id": call,
                        "input_token_count": inp,
                        "output_token_count": outp})
    return "(" + ", ".join(_q(v) for v in (
        _fmt(ts), "INFO", 9, "bench", attrs,
        json.dumps({"service.name": "bench"}))) + ")"


def build_fixture(history, new_kind):
    """Pinned VALUES blobs; arrival order is the commit order.

    History timestamps stay inside one pinned OLD hour (modulo 3600s):
    H2000 would otherwise cross Sep-21 into the late cohort and change
    the affected day volume with H, so both sizes share the same old
    history count vs the new Aug-23/Sep-21 rows. Trace/span PKs stay
    unique via the row index.
    """
    spans, logs = [], []
    for i in range(history):
        ts = FIXTURE_BASE + dt.timedelta(seconds=(i * 37) % 3600)
        session = "hist-%d" % (i % max(1, history // 10))
        spans.append(span_val(i, ts, session, "codex", 100 + i % 50,
                              50 + i % 25, "r%d" % i))
        if i % 20 == 0:
            logs.append(log_val(ts + dt.timedelta(seconds=1), session,
                                "r%d" % i, 120 + i % 30, 60 + i % 15))
    n_new = {"none": 0, "few": 5, "session": 20}[new_kind]
    new_spans, new_logs = [], []
    for k in range(n_new):
        i = history + k
        # Old event time committed late: same fence delta as any new row.
        ts = (dt.datetime(2026, 8, 23, 10, 0, 0) if k == 0 and new_kind
              else LATE_BASE + dt.timedelta(seconds=k * 61))
        session = "late-shared" if new_kind == "session" else "late-%d" % k
        new_spans.append(span_val(i, ts, session, "codex", 200, 100,
                                  "rn%d" % k))
        new_logs.append(log_val(LATE_BASE + dt.timedelta(seconds=k * 61),
                                session, "rn%d" % k, 220, 110))
    digest = hashlib.sha256(
        ("\n".join(spans + new_spans) + "\n" + "\n".join(
            logs + new_logs)).encode()).hexdigest()
    return spans, logs, new_spans, new_logs, digest


# ---------------------------------------------------------------- sides

AGG_CHILD = r"""
import json, resource, sys, time
tree, db, action, payload = sys.argv[1], sys.argv[2], sys.argv[3], json.loads(sys.argv[4])
sys.path.insert(0, tree)
from scripts.analytics import aggregate as agg
cfg = {"max_rows": payload["max_rows"], "otel_ttl": "",
       "ai_full_rebuild": bool(payload.get("full_rebuild")),
       "grpc_url": payload.get("grpc_url") or "",
       "user": payload.get("user") or "", "password": payload.get("password") or ""}
ctx = (payload["base_url"], payload["auth"], db)
stmts, flights = [], []
real_request = agg.request_sql
import urllib.request as _urlopen_mod
real_urlopen = _urlopen_mod.urlopen
http_bytes = {"req": 0, "resp": 0}
def counting_urlopen(req, *a, **k):
    try:
        data = req.data if isinstance(getattr(req, "data", None), (bytes, bytearray)) else b""
        http_bytes["req"] += len(data)
    except Exception:
        pass
    resp = real_urlopen(req, *a, **k)
    orig_read = resp.read
    def counting_read(*ra, **rk):
        chunk = orig_read(*ra, **rk)
        try:
            http_bytes["resp"] += len(chunk)
        except Exception:
            pass
        return chunk
    try:
        resp.read = counting_read
    except Exception:
        pass
    return resp
_urlopen_mod.urlopen = counting_urlopen
def watching(base_url, auth, d, stmt, timeout=60):
    before_req, before_resp = http_bytes["req"], http_bytes["resp"]
    out = real_request(base_url, auth, d, stmt, timeout=timeout)
    try:
        rec = out["output"][0].get("records") or {}
        n = len(rec.get("rows", []))
    except Exception:
        n = None
    stmts.append({"sql": stmt, "result_rows": n,
                  "server_ms": out.get("execution_time_ms"),
                  "request_bytes": http_bytes["req"] - before_req,
                  "response_bytes": http_bytes["resp"] - before_resp})
    return out
agg.request_sql = watching
grpc_bytes = {"req": 0, "resp": 0}
try:
    import grpc as _grpc
    real_insecure, real_secure = _grpc.insecure_channel, getattr(_grpc, "secure_channel", None)
    def counting_channel(factory, *a, **k):
        ch = factory(*a, **k)
        orig_unary = ch.unary_stream
        def counting_unary(method, request_serializer=None, response_deserializer=None, **uk):
            orig_decode = response_deserializer
            def counting_ser(b):
                try:
                    grpc_bytes["req"] += len(b)
                except Exception:
                    pass
                return request_serializer(b) if request_serializer else b
            def counting_de(b):
                try:
                    grpc_bytes["resp"] += len(b)
                except Exception:
                    pass
                return orig_decode(b)
            return orig_unary(method, request_serializer=counting_ser, response_deserializer=counting_de, **uk)
        ch.unary_stream = counting_unary
        return ch
    _grpc.insecure_channel = lambda *a, **k: counting_channel(real_insecure, *a, **k)
    if real_secure is not None:
        _grpc.secure_channel = lambda *a, **k: counting_channel(real_secure, *a, **k)
except ImportError:
    pass
try:
    from scripts.database import greptime_flight as _flight
    real_flight = _flight.flight_query
    def flight_watching(sql_text, *, endpoint, db, user, password, lower=None, timeout=60, max_rows=200000):
        w0, c0 = time.monotonic(), time.process_time()
        before_req, before_resp = grpc_bytes["req"], grpc_bytes["resp"]
        cols, rows, upper = real_flight(sql_text, endpoint=endpoint, db=db, user=user, password=password, lower=lower, timeout=timeout, max_rows=max_rows)
        flights.append({"sql": sql_text[:160], "rows": len(rows),
                        "upper": {str(k): v for k, v in (upper or {}).items()},
                        "lower": json.dumps(lower, sort_keys=True, default=str),
                        "request_bytes": grpc_bytes["req"] - before_req,
                        "response_bytes": grpc_bytes["resp"] - before_resp,
                        "response_bytes_note": "actual serialized FlightData payload bytes through the gRPC proxy (not HTTP/2 framing or TLS overhead); ticket request bytes counted on the same proxy, no secret dump",
                        "client_s": round(time.monotonic() - w0, 4),
                        "client_cpu_s": round(time.process_time() - c0, 4)})
        return cols, rows, upper
    _flight.flight_query = flight_watching
except ImportError:
    pass
def derived():
    cells = {}
    for table in ["ai_session_summary", "ai_activity_daily", "ai_daily_summary", "ai_tool_daily"]:
        try:
            cols, rows = agg.fetch_rows(payload["base_url"], payload["auth"], db,
                                        "SELECT * FROM " + table)
            cells[table] = sorted([json.dumps(dict(zip(cols, r)), sort_keys=True, default=str) for r in rows])
        except Exception as e:
            cells[table] = "ERROR: " + str(e)[:120]
    return cells
if action == "aggregate":
    agg.load_price_table = lambda: None
    wall0, cpu0 = time.monotonic(), time.process_time()
    total = agg.ai_section(ctx, cfg)
    wall, cpu = time.monotonic() - wall0, time.process_time() - cpu0
    agg.request_sql = real_request
    import urllib.request as _restore_mod
    _restore_mod.urlopen = real_urlopen
    pass_stmts = stmts
    stmts = []
    try:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        peak = None
    try:
        state_rows = agg.fetch_rows(payload["base_url"], payload["auth"], db,
                                    "SELECT fence, price_digest, config_revision, input_digest FROM ai_aggregate_state WHERE scope = 'ai'")[1]
    except Exception:
        state_rows = "unavailable"
    print(json.dumps({"total": total, "wall_s": wall, "cpu_s": cpu, "peak_rss": peak,
                      "statements": pass_stmts, "flights": flights, "derived": derived(), "state": state_rows}, default=str))
"""

def agg_call(python, tree, db, payload):
    proc = subprocess.run(
        [python, "-c", AGG_CHILD, tree, db, "aggregate",
         json.dumps(payload)],
        capture_output=True, text=True, cwd=tree,
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
             "PYTHONPATH": tree, "PYTHONNOUSERSITE": "1",
             "VIRTUAL_ENV": str(Path(python).parents[1])},
        timeout=900)
    if proc.returncode != 0:
        raise RuntimeError("aggregate side failed: %s" % proc.stderr[-2000:])
    return json.loads(proc.stdout)


def categorize(stmt):
    low = stmt.lower()
    if "ai_aggregate_state" in low:
        return "state"
    if "information_schema" in low or "greptime_timestamp" in low:
        return "metric_discovery"
    if "opentelemetry_traces" in low or "opentelemetry_logs" in low:
        return "raw"
    if stmt.startswith("INSERT INTO ai_") or stmt.startswith("DELETE FROM ai_"):
        return "derived_write"
    if "raw_retention_watermark" in low:
        return "watermark"
    return "other"


def summarize_run(out):
    stmts = out.pop("statements")
    flights = out.pop("flights")
    by_cat = {}
    for s in stmts:
        cat = categorize(s["sql"])
        cell = by_cat.setdefault(cat, {"queries": 0, "result_rows": 0,
                                       "server_ms": 0, "request_bytes": 0,
                                       "response_bytes": 0})
        cell["queries"] += 1
        cell["result_rows"] += s["result_rows"] or 0
        cell["server_ms"] += s["server_ms"] or 0
        cell["request_bytes"] += s["request_bytes"] or 0
        cell["response_bytes"] += s.get("response_bytes") or 0
    out["summary"] = {
        "n_sql": len(stmts), "n_flight": len(flights),
        "raw_sql_rows": sum(s["result_rows"] or 0 for s in stmts
                            if categorize(s["sql"]) == "raw"),
        "flight_rows": sum(f["rows"] for f in flights),
        "flight_request_bytes": sum(f.get("request_bytes") or 0 for f in flights),
        "flight_response_bytes": sum(f.get("response_bytes") or 0 for f in flights),
        "flight_response_bytes_note": "actual serialized FlightData payload bytes via the gRPC channel proxy (not HTTP/2 framing or TLS overhead); per-read request ticket bytes counted the same way, no secret dump",
        "by_category": by_cat,
        "sql_server_ms_total": sum(s["server_ms"] or 0 for s in stmts
                                   if s["server_ms"] is not None),
        "sql_request_bytes_total": sum(s["request_bytes"] or 0
                                       for s in stmts),
        "sql_response_bytes_total": sum(s.get("response_bytes") or 0
                                        for s in stmts),
        "sql_bytes_note": "actual urllib request form bytes and response payload bytes via the urlopen proxy (headers excluded), not len(stmt)",
        "flight_client_s_total": round(sum(f["client_s"] for f in flights), 4),
        "flight_client_cpu_s_total": round(
            sum(f["client_cpu_s"] for f in flights), 4),
        "flights": flights}
    return out


def _insert_many(args, auth, db, cols_sql, table, blob, batch=200):
    for i in range(0, len(blob), batch):
        chunk = ", ".join(blob[i:i + batch])
        sql(args.base_url, auth, db,
            'INSERT INTO "%s" (%s) VALUES %s' % (table, cols_sql, chunk))


def measure_cell(args, auth, history, new_kind, repeats):
    base_db = "%s_h%d_%s" % (args.db, history, new_kind)
    if not DB_RE.match(base_db):
        raise RuntimeError("refusing unexpected db %r" % base_db)
    spans, logs, new_spans, new_logs, digest = build_fixture(history,
                                                             new_kind)
    report = {"history": history, "new": new_kind,
              "fixture_sha256": digest,
              "history_rows": len(spans) + len(logs),
              "new_rows": len(new_spans) + len(new_logs),
              "repeats": repeats, "sides": {}}
    trees = {"current": str(ROOT)}
    if args.baseline_root:
        trees["baseline"] = args.baseline_root
    for side, tree in trees.items():
        db = "%s_%s" % (base_db, side)
        if len(db) > 48:
            db = base_db[:44 - len(side)] + "_" + side
        if not DB_RE.match(db):
            raise RuntimeError("refusing unexpected db %r" % db)
        sql(args.base_url, auth, "public",
            'CREATE DATABASE IF NOT EXISTS "%s"' % db)
        for label, stmt in db_init.ddl_statements(""):
            sql(args.base_url, auth, db, stmt)
        for stmt in db_init.alter_statements():
            try:
                sql(args.base_url, auth, db, stmt)
            except RuntimeError:
                pass
        payload = {"base_url": args.base_url, "auth": auth,
                   "max_rows": args.max_rows, "full_rebuild": False,
                   "grpc_url": args.grpc_url, "user": args.user,
                   "password": os.environ.get(args.password_env, "")}
        _insert_many(args, auth, db, SPAN_COLS_SQL,
                     "opentelemetry_traces", spans)
        _insert_many(args, auth, db, LOG_COLS_SQL,
                     "opentelemetry_logs", logs)
        # History flush after fixture load: the fence reads deltas, not
        # SSTs, so a stale lower below this frontier must take the explicit
        # full recovery (covered by the regression suite).
        sql(args.base_url, auth, db,
            "ADMIN FLUSH_TABLE('opentelemetry_traces')")
        sql(args.base_url, auth, db,
            "ADMIN FLUSH_TABLE('opentelemetry_logs')")
        pre = count_of(args.base_url, auth, db,
                       'SELECT COUNT(*) FROM "opentelemetry_traces"')
        assert pre == len(spans), (side, pre, len(spans))
        runs = []
        for rep in range(repeats + 1):
            if rep == 1 and new_kind != "none":
                _insert_many(args, auth, db, SPAN_COLS_SQL,
                             "opentelemetry_traces", new_spans)
                _insert_many(args, auth, db, LOG_COLS_SQL,
                             "opentelemetry_logs", new_logs)
            runs.append(summarize_run(
                agg_call(sys.executable, tree, db, payload)))
        if side == "current":
            # Cursor actually persists and Flight is actually used: a
            # current side with zero fence reads is a both-full false
            # positive and fails here.
            states = [r["state"] for r in runs]
            assert all(s != "unavailable" and s for s in states), states
            flights = [r["summary"]["n_flight"] for r in runs]
            assert any(n > 0 for n in flights), flights
            if new_kind == "none":
                assert all(r["summary"]["flight_rows"] == 0
                           for r in runs[1:]), runs
        report["sides"][side] = {"db": db, "runs": runs, "tree": tree}
    cur, bl = report["sides"].get("current"), report["sides"].get("baseline")
    if cur and bl:
        match = all(
            cur["runs"][i]["derived"] == bl["runs"][i]["derived"]
            for i in range(len(cur["runs"])))
        report["oracle_match_all_runs"] = match
        if not match:
            raise RuntimeError(
                "oracle mismatch current vs baseline in %s/%s" % (
                    history, new_kind))
        last = cur["runs"][-1]["summary"]
        report["noop_proof"] = {
            "flight_rows": last["flight_rows"],
            "raw_sql_rows": last["raw_sql_rows"]}
    else:
        report["oracle_match_all_runs"] = None
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:56955")
    ap.add_argument("--grpc-url", default="grpc://127.0.0.1:56956")
    ap.add_argument("--db", default="ai_incr_issue1_bench")
    ap.add_argument("--user", default="issue_synthetic")
    ap.add_argument("--password-env", default="ISSUE_SYNTH_PASS")
    ap.add_argument("--baseline-root", default="")
    ap.add_argument("--history", type=int, default=200)
    ap.add_argument("--new", default="few",
                    choices=("none", "few", "session"))
    ap.add_argument("--history-sizes", default="200,2000")
    ap.add_argument("--new-kinds", default="none,few,session")
    ap.add_argument("--matrix", action="store_true")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--max-rows", type=int, default=200000)
    ap.add_argument("--no-cleanup", action="store_true")
    args = ap.parse_args(argv)
    password = os.environ.get(args.password_env, "")
    if not password:
        print("benchmark: error: %s is empty" % args.password_env,
              file=sys.stderr)
        return 1
    auth = base64.b64encode(
        (args.user + ":" + password).encode()).decode("ascii")
    if args.matrix:
        hist = [int(x) for x in args.history_sizes.split(",") if x.strip()]
        kinds = [k.strip() for k in args.new_kinds.split(",") if k.strip()]
        cells = [(h, k) for h in hist for k in kinds]
    else:
        cells = [(args.history, args.new)]
    report = {"schema": SCHEMA, "baseline_root": args.baseline_root or None,
              "grpc_url_note": "fenced current side reads deltas over "
                               "gRPC Flight; baseline has no Flight path "
                               "and always full-scans over HTTP SQL",
              "note": ("pinned fixture per cell shared by both sides; "
                       "arrival order is the commit order (an old event "
                       "time rides the new leg); ADMIN FLUSH_TABLE runs "
                       "after fixture load; repeats are independent "
                       "aggregate passes, not modeled latency; oracle "
                       "compares all derived tables cell-by-cell"),
              "cells": []}
    try:
        for history, kind in cells:
            report["cells"].append(measure_cell(args, auth, history, kind,
                                               args.repeats))
    finally:
        if not args.no_cleanup:
            for cell in report["cells"]:
                for side in cell["sides"].values():
                    try:
                        sql(args.base_url, auth, "public",
                            'DROP DATABASE IF EXISTS "%s"' % side["db"])
                    except Exception:
                        pass
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
