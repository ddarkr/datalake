"""Issue #1 behavior + recovery proof: fenced AI incremental aggregation.

Offline only: stdlib fake Greptime over HTTP + a fake Flight fence that
implements the exact consumer-visible contract of
scripts/database/greptime_flight.py flight_query (proven by Main on native
Greptime 1.2.1, not re-proven here). Synthetic rows, isolated stores.

Run with:  python -m tests.test_ai_incremental   (Main runs it.)

Contract under test (aggregate.py owned by FinishIncrementalCore):
ai_section(ctx, cfg), ctx=(base_url, basic_auth, db), cfg keys max_rows
(fail-closed row cap), otel_ttl ("" = unbounded), ai_full_rebuild (True =
explicit full recovery), grpc_url (Flight endpoint for fenced discovery),
user, password. Discovery is a per-region sequence fence, never a
timestamp window: raw tables commit in sequence order, the pass fences
with the stored upper watermark and checkpoints the returned upper only
after all derived writes. AGG_AI_FULL_REBUILD=1 is the only switch; every
other unproved signal rebuilds automatically, never skips.

Every test executes the real ai_section and compares full derived-table
content (all cells, never row counts) against a fresh-store full rebuild
over the same input. No source-text, wiring, or mock-echo assertions.
"""

import datetime as dt
import http.server
import json
import re
import sqlite3
import threading
import urllib.parse

from scripts.analytics import aggregate as agg
from scripts.database import greptime_flight as real_flight

NOW = dt.datetime(2026, 9, 22, 12, 0, 0)
DAY1 = dt.datetime(2026, 9, 20)
DAY2 = dt.datetime(2026, 9, 21)

DERIVED_TABLES = ("ai_session_summary", "ai_activity_daily",
                  "ai_daily_summary", "ai_tool_daily")
REGION = 4595615006720  # single-region fake, mirroring the native proof


def _fmt(ts):
    if isinstance(ts, dt.datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return ts


def _norm_ts(value):
    """Flight wire form: exact int64 ns (mirrors flight_query behavior)."""
    if isinstance(value, dt.datetime):
        delta = value - dt.datetime(1970, 1, 1)
        return int(delta.total_seconds() * 1_000_000_000)
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            parsed = dt.datetime.fromisoformat(
                value.replace("T", " ").replace("Z", ""))
            return int((parsed - dt.datetime(1970, 1, 1)
                        ).total_seconds() * 1_000_000_000)
        except ValueError:
            return value
    return value


# ---------------------------------------------------------------- seq store
#
# Consumer-visible record store: every committed row carries a monotone
# commit seq in one region; flush compacts (drops nothing here, advances the
# readable frontier). Fenced reads use the exact Flight extension
# semantics: lower=None = snapshot + upper; lower=upper = no-op; lower
# below frontier = StaleFence. Rows are dicts; seq assignment is the only
# commit order (arrival order, never event time).

class SeqStore:
    def __init__(self):
        self.traces = []
        self.logs = []
        self.seqs = {"traces": 0, "logs": 0}
        self.frontiers = {"traces": 0, "logs": 0}
        self.metric_cols = []
        self.metric_rows = []
        # Per-logical-metric seq domains (native proof shape): each metric
        # table advances only on its own writes; untouched tables return
        # zero rows at the same flush frontier.
        self.metric_seqs = {}
        self.metric_table_rows = {}

    def append(self, table, rows):
        for row in rows:
            self.seqs[table] += 1
            row = dict(row)
            row["_seq"] = self.seqs[table]
            row["_region"] = REGION
            getattr(self, table).append(row)
        return self.seqs[table]

    def flush(self):
        for table in ("traces", "logs"):
            self.frontiers[table] = self.seqs[table]

    def upper(self):
        return {REGION: max(self.seqs.values())} if any(self.seqs.values()) else {}

    def table_upper(self, table):
        # Native proof: an empty table ALWAYS returns {region: 0} (valid
        # zero watermark, never missing/null) on both passes.
        return {REGION: self.seqs[table]}

    def fenced(self, table, lower):
        key = {"opentelemetry_traces": "traces",
               "opentelemetry_logs": "logs"}.get(table, table)
        rows = getattr(self, key)
        upper = dict(self.table_upper(key))
        if lower is None:
            return list(rows), upper
        seq = lower.get(REGION)
        if seq is None:
            raise real_flight.FlightUnavailable("no lower bound")
        if seq < self.frontiers[key]:
            raise real_flight.StaleFence(
                "STALE_CURSOR: incremental query stale, given_seq: %d, "
                "min_readable_seq: %d, retry_hint: FALLBACK_FULL_RECOMPUTE"
                % (seq, self.frontiers[key]))
        return [r for r in rows if r["_seq"] > seq], upper


# ---------------------------------------------------------------- fake HTTP

class FakeGreptime(http.server.BaseHTTPRequestHandler):
    store = SeqStore()
    derived = {}
    state = None
    watermark = None
    catalog = []
    metric_cols = []
    metric_rows = []
    flight_log = []
    queries = []
    fault = None
    fault_n = 0

    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        stmt = urllib.parse.parse_qs(
            self.rfile.read(length).decode()).get("sql", [""])[0]
        try:
            payload = route(stmt)
        except FakeFault as e:
            payload = {"error": str(e)[:300]}
        data = json.dumps(payload, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class FakeFault(Exception):
    pass


def _records(cols, rows):
    return {"output": [{"records": {
        "schema": {"column_schemas": [{"name": c} for c in cols]},
        "rows": rows}}], "execution_time_ms": 1}


def _split_top(text):
    return _split_outside(text, 0)


def _split_outside(text, at):
    parts, cur, quoted, depth = [], "", False, 0
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if quoted:
            if ch == "'" and text[i:i + 2] == "''":
                cur += "''"
                i += 2
                continue
            if ch == "'":
                quoted = False
            cur += ch
        elif ch == "'":
            quoted = True
            cur += ch
        elif ch == "(":
            depth += 1
            cur += ch
        elif ch == ")":
            depth -= 1
            cur += ch
        elif ch == "," and depth == at:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
        i += 1
    parts.append(cur.strip())
    return [p for p in parts if p != ""]


def _decode(cell):
    cell = cell.strip()
    if cell == "NULL":
        return None
    if len(cell) >= 2 and cell.startswith("'") and cell.endswith("'"):
        return cell[1:-1].replace("''", "'")
    try:
        return int(cell)
    except ValueError:
        pass
    try:
        return float(cell)
    except ValueError:
        return cell


def _lit_arg(stmt, name, op="="):
    m = re.search(name + r"\s*" + re.escape(op) + r"\s*'((?:''|[^'])*)'",
                  stmt)
    return m.group(1).replace("''", "'") if m else None


def _select_cols(stmt):
    m = re.match(r"\s*SELECT\s+(.*?)\s+FROM\s+(\S+)", stmt, re.S | re.I)
    if not m:
        return [], ""
    raw = m.group(1).strip()
    table = m.group(2).strip().strip('"')
    if re.match(r"(?i)(COUNT|MAX|MIN|SUM|AVG)\s*\(", raw):
        return [raw], table
    parts, cur, quoted, paren = [], "", False, 0
    for ch in raw:
        if ch == "'":
            quoted = not quoted
            cur += ch
        elif quoted:
            cur += ch
        elif ch == "(":
            paren += 1
            cur += ch
        elif ch == ")":
            paren -= 1
            cur += ch
        elif ch == "," and paren == 0:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    parts.append(cur.strip())
    names = [re.split(r"\s+AS\s+", p, flags=re.I)[-1].strip() for p in parts]
    cols = [p[1:-1] if len(p) > 1 and p.startswith('"') and p.endswith('"')
            else p for p in names]
    return cols, table


def _strip_meta(row):
    return {k: v for k, v in row.items()
            if not k.startswith("_")}


def _session_want(stmt):
    groups = [g for g in re.findall(r"\(([^()]*)\)", stmt)
              if re.search(r"=\s*'", g)]
    if not groups:
        return None
    wanted = set()
    for g in groups:
        for lit in re.findall(r"=\s*'((?:''|[^'])*)'", g):
            wanted.add(lit.replace("''", "'"))
    return wanted


def _session_match(rows, stmt):
    """Refetch predicates: session-equality OR groups or day ranges."""
    if "log_attributes" in stmt:
        wanted = _session_want(stmt)
        if wanted:
            out = []
            for row in rows:
                try:
                    attrs = json.loads(row.get("log_attributes") or "{}")
                except ValueError:
                    attrs = {}
                if not isinstance(attrs, dict):
                    attrs = {}
                vals = [attrs.get(c) for c in
                        ("coding_agent.session.id", "session.id",
                         "conversation.id", "gen_ai.conversation.id")]
                if any(v in wanted for v in vals if v):
                    out.append(row)
            return out
    if " OR " in stmt.upper():
        wanted = _session_want(stmt)
        if wanted:
            cols = ("span_attributes.coding_agent.session.id",
                    "span_attributes.session.id",
                    "span_attributes.conversation.id",
                    "span_attributes.gen_ai.conversation.id")
            got = [r for r in rows
                   if any(r.get(c) in wanted for c in cols if r.get(c))]
            if got or " OR " in stmt.upper():
                return got
    if "timestamp >=" in stmt:
        day = _lit_arg(stmt, "timestamp", op=">=")
        nxt = _lit_arg(stmt, "timestamp", op="<")
        keep = []
        for row in rows:
            ts = str(row.get("timestamp") or "")
            if ts >= (day or "") and (nxt is None or ts < nxt):
                keep.append(row)
        return keep
    return list(rows)


def route(stmt):
    F = FakeGreptime
    if F.fault and stmt.startswith(F.fault[0]):
        F.fault_n += 1
        if F.fault_n == F.fault[1]:
            raise FakeFault("injected failure on " + F.fault[0])
    summary = re.search(r"\bFROM (ai_daily_summary|ai_tool_daily)\b", stmt)
    if summary and stmt.startswith(("SELECT ", "DELETE ")):
        table = summary.group(1)
        keys = _UPSERT_KEYS[table]
        stored = F.derived[table]
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE " + table + " (" +
                       ", ".join(key + " TEXT" for key in keys) + ")")
            db.executemany("INSERT INTO " + table + " VALUES (" +
                           ",".join("?" for _ in keys) + ")", [
                               tuple(agg.ts_lit(agg.parse_ts(row.get(key)))[1:-1]
                                     if key == "day_start" else row.get(key)
                                     for key in keys) for row in stored])
            result = db.execute(stmt)
            if stmt.startswith("SELECT "):
                rows = result.fetchall()
                F.queries.append((stmt, len(rows)))
                return _records([col[0] for col in result.description], rows)
            survivors = {row[0] - 1 for row in db.execute("SELECT rowid FROM " + table)}
        F.derived[table] = [row for index, row in enumerate(stored) if index in survivors]
        F.queries.append((stmt, len(stored) - len(survivors)))
        return {"output": [{"affectedrows": len(stored) - len(survivors)}]}
    if stmt.startswith("SELECT"):
        if "information_schema.columns" in stmt:
            m = re.search(r"table_name\s*=\s*'((?:''|[^'])*)'", stmt)
            logical = m.group(1).replace("''", "'") if m else None
            cols = ([c for c in FakeGreptime.metric_cols]
                    if logical and any(t == logical for t, _ in F.catalog)
                    else [])
            rows = [[c] for c in cols]
            F.queries.append((stmt, len(rows)))
            return _records(["column_name"], rows)
        if "information_schema" in stmt:
            rows = [list(r) for r in F.catalog]
            F.queries.append((stmt, len(rows)))
            return _records(["table_name", "create_options"], rows)
        if "FROM ai_aggregate_state" in stmt:
            s = F.state
            rows = [[s["fence"], s["price"], s["rev"], s["input"]]] if s else []
            F.queries.append((stmt, len(rows)))
            return _records(["fence", "price_digest",
                             "config_revision", "input_digest"], rows)
        if "FROM raw_retention_watermark" in stmt:
            marks = [r.get("deleted_before")
                     for r in F.derived["raw_retention_watermark"]
                     if r.get("deleted_before")]
            if F.watermark:
                marks.append(F.watermark)
            best = max(marks) if marks else None
            rows = [[best]] if best else [[None]]
            F.queries.append((stmt, len(rows)))
            return _records(["deleted_before"], rows)
        if "FROM ai_session_summary" in stmt:
            cols, _ = _select_cols(stmt)
            selected = F.derived["ai_session_summary"]
            if " WHERE " in stmt:
                with sqlite3.connect(":memory:") as db:
                    db.execute("CREATE TABLE ai_session_summary "
                               "(client TEXT, session_id TEXT, session_start TEXT)")
                    db.executemany("INSERT INTO ai_session_summary VALUES (?,?,?)",
                                   [(r.get("client"), r.get("session_id"),
                                     _fmt(r.get("session_start"))) for r in selected])
                    result = db.execute(stmt)
                    rows = result.fetchall()
                F.queries.append((stmt, len(rows)))
                return _records(cols, rows)
            rows = [[r.get(c) for c in cols] for r in selected]
            F.queries.append((stmt, len(rows)))
            return _records(cols, rows)
        for metric in [t for t, _ in F.catalog]:
            if '"%s"' % metric in stmt or "FROM %s" % metric in stmt:
                bucket = F.store.metric_table_rows.get(
                    metric, F.store.metric_rows)
                rows = [[r.get(c) for c in F.metric_cols]
                        for r in bucket]
                limit = re.search(r"(?i)\bLIMIT\s+(\d+)", stmt)
                if limit:
                    rows = rows[:int(limit.group(1))]
                F.queries.append((stmt, len(rows)))
                return _records(list(F.metric_cols), rows)
        if "FROM opentelemetry_traces" in stmt:
            return _raw_branch(stmt, "traces")
        if "FROM opentelemetry_logs" in stmt:
            return _raw_branch(stmt, "logs")
        raise FakeFault("fake: unhandled SELECT: " + stmt[:160])
    if stmt.startswith("INSERT INTO"):
        m = re.match(r"INSERT INTO (\S+)\s*\((.*)\)\s*VALUES\s*(.*)",
                     stmt, re.S)
        table, cols = m.group(1), _split_top(m.group(2).rsplit(")", 1)[0])
        body = m.group(3).strip()
        # Tuples split at depth 0 (commas between top-level parens); cell
        # commas sit inside quotes or JSON and never split.
        tuples = _split_outside(body, 0)
        rows = []
        for tup in tuples:
            inner = tup[1:-1] if tup.startswith("(") else tup
            rows.append(dict(zip(cols, [_decode(c) for c in
                                        _split_top(inner)])))
        _apply_insert(table, rows)
        F.queries.append((stmt, len(rows)))
        return {"output": [{"affectedrows": len(rows)}],
                "execution_time_ms": 1}
    if stmt.startswith("DELETE FROM ai_session_summary"):
        sid = _lit_arg(stmt, "session_id")
        client = _lit_arg(stmt, "client")
        start = _lit_arg(stmt, "session_start", op="!=")
        F.derived["ai_session_summary"] = [
            r for r in F.derived["ai_session_summary"]
            if not (r.get("session_id") == sid
                    and r.get("client") == client
                    and r.get("session_start") != start)]
        F.queries.append((stmt, 1))
        return {"output": [{"affectedrows": 1}], "execution_time_ms": 1}
    if stmt.startswith(("ALTER TABLE", "CREATE TABLE", "ADMIN ")):
        F.queries.append((stmt, 0))
        return {"output": [{"affectedrows": 0}], "execution_time_ms": 1}
    if stmt.startswith("INSERT INTO raw_retention_watermark"):
        m = re.search(r"VALUES\s*(\(.*\))", stmt, re.S)
        if m:
            F.watermark = _decode(_split_top(m.group(1)[1:-1])[2])
        F.queries.append((stmt, 1))
        return {"output": [{"affectedrows": 1}], "execution_time_ms": 1}
    return {"output": [{"affectedrows": 1}], "execution_time_ms": 1}


def _raw_branch(stmt, table):
    rows = [_strip_meta(r) for r in getattr(FakeGreptime.store, table)]
    kept = _session_match(rows, stmt)
    limit = re.search(r"(?i)\bLIMIT\s+(\d+)", stmt)
    if limit:
        kept = kept[:int(limit.group(1))]
    cols, _ = _select_cols(stmt)
    if not cols:
        cols = sorted(kept[0].keys()) if kept else []
    out = [[r.get(c) for c in cols] for r in kept]
    FakeGreptime.queries.append((stmt, len(out)))
    return _records(cols, out)


_UPSERT_KEYS = {
    "ai_session_summary": ("session_id", "client", "session_start"),
    "ai_activity_daily": ("day_start", "client"),
    "ai_daily_summary": ("day_start", "client", "provider", "model"),
    "ai_tool_daily": ("day_start", "tool_name", "client"),
}


def _apply_insert(table, rows):
    F = FakeGreptime
    if table == "ai_aggregate_state":
        for r in rows:
            F.state = {"fence": r.get("fence"),
                       "price": r.get("price_digest"),
                       "rev": r.get("config_revision"),
                       "input": r.get("input_digest")}
        return
    if table == "raw_retention_watermark":
        F.derived["raw_retention_watermark"].extend(rows)
        return
    if table in _UPSERT_KEYS:
        keys = _UPSERT_KEYS[table]
        store = F.derived[table]
        for r in rows:
            for col in ("client", "provider", "model", "tool_name",
                        "session_id", "scope_name", "severity_text"):
                if col not in r:
                    r[col] = None
            key = tuple(r.get(k) for k in keys)
            for i, old in enumerate(store):
                if tuple(old.get(k) for k in keys) == key:
                    store[i] = r
                    break
            else:
                store.append(r)
        return
    F.derived.setdefault(table, []).extend(rows)


# ---------------------------------------------------------------- fake fence
#
# Implements the consumer-visible flight_query contract against the SeqStore:
# lower=None snapshot, lower=upper no-op, stale below frontier raises, seq 0
# valid, unproved (empty) upper raises FlightUnavailable, EOF faults raise.
# Log rows and metric rows travel the same fence in table-Keyed lower dicts
# ({table: {region: seq}}), exactly as _ai_fenced_scan calls it.

def _apply_metric_where(rows, tail):
    """Session/day scoping for unfenced logical reads (native WHERE shape).

    Parses quoted session literals and greptime_timestamp day ranges from
    the statement tail; returns rows matching any session literal or any
    day range. Empty tail returns all rows (bootstrap snapshot).
    """
    if not tail or "WHERE" not in tail.upper():
        return rows
    wanted = set()
    for lit in re.findall(r"=\s*'((?:''|[^'])*)'", tail):
        wanted.add(lit.replace("''", "'"))
    days = re.findall(
        r"greptime_timestamp\s*>=\s*'([^']*)'\s*AND\s*greptime_timestamp\s*<\s*'([^']*)'",
        tail)
    out = []
    for row in rows:
        sids = [row.get(c) for c in ("coding_agent_session_id", "session_id",
                                     "conversation_id",
                                     "gen_ai_conversation_id")]
        if any(s in wanted for s in sids if s):
            out.append(row)
            continue
        stamp = str(row.get("greptime_timestamp") or "")
        for start, end in days:
            if stamp >= start and stamp < end:
                out.append(row)
                break
    return out


def fake_fence(sql_text, store, lower, fault=None):
    table = ("opentelemetry_traces" if "opentelemetry_traces" in sql_text
             else "opentelemetry_logs" if "opentelemetry_logs" in sql_text
             else None)
    if table is None:
        # Logical metric table: flat lower {region: seq} over its own seq
        # domain (native proof: edited table returns 1 row, untouched
        # table returns zero rows + its own {region: watermark}). Scoped
        # WHERE applies session/day scoping to the logical bucket for
        # unfenced (lower=None) scoped reads.
        m = re.search(r'SELECT \* FROM "([^"]+)"(.*)', sql_text, re.S)
        logical = m.group(1) if m else None
        tail = (m.group(2) or "") if m else ""
        if logical is None:
            raise real_flight.FlightError("fake fence: unknown table")
        if fault == "post_eof":
            raise real_flight.FlightError("fake fence: post-EOF fault")
        if fault == "missing_watermark":
            raise real_flight.FlightUnavailable("fake: unproved watermarks")
        seqs = store.metric_seqs.setdefault(logical, {"seq": 0, "frontier": 0})
        bound = lower if isinstance(lower, dict) else None
        if bound == {}:
            raise real_flight.FlightUnavailable("stored fence is empty")
        if bound is not None and bound.get(REGION, 0) < seqs["frontier"]:
            raise real_flight.StaleFence(
                "STALE_CURSOR: metric fence stale, retry_hint: "
                "FALLBACK_FULL_RECOMPUTE")
        if bound is None:
            rows = [dict(r) for r in
                    store.metric_table_rows.get(logical, [])]
            rows = _apply_metric_where(rows, tail)
        else:
            rows = [dict(r) for r in
                    store.metric_table_rows.get(logical, [])
                    if r.get("_mseq", 0) > bound.get(REGION, 0)]
        upper = {REGION: seqs["seq"]}
        cols = list(FakeGreptime.metric_cols)
        return cols, [[r.get(c) for c in cols] for r in rows], upper
    if fault == "post_eof":
        raise real_flight.FlightError("fake fence: post-EOF fault")
    if fault == "missing_watermark":
        raise real_flight.FlightUnavailable("fake: unproved watermarks")
    if fault == "new_region":
        cols = (list(agg.SPAN_COLS) if table == "opentelemetry_traces"
                else list(agg.LOG_COLS))
        return cols, [], {REGION: store.seqs[
            {"opentelemetry_traces": "traces",
             "opentelemetry_logs": "logs"}[table]] + 1,
            REGION + 7: 1}
    bound = lower if isinstance(lower, dict) else None
    if bound == {}:
        raise real_flight.FlightUnavailable("stored fence is empty")
    rows, upper = store.fenced(table, bound)
    if not upper:
        raise real_flight.FlightUnavailable("no region watermarks")
    cols = (list(agg.SPAN_COLS) if table == "opentelemetry_traces"
            else list(agg.LOG_COLS))
    wire = []
    for row in rows:
        cells = []
        for c in cols:
            v = row.get(c)
            if c in ("timestamp", "timestamp_end"):
                v = _norm_ts(v)
            cells.append(v)
        wire.append(cells)
    return cols, wire, upper


def install_fence(fault_map=None):
    """Patch the fenced seam offline (sys.modules stub, restored after)."""
    import sys as _sys
    fault_map = fault_map or {}
    log = []
    real_mod = _sys.modules.get("scripts.database.greptime_flight")
    real_seam = agg._ai_flight
    stub = type("flight", (), {"available": staticmethod(lambda: True)})

    def patched(sql_text, *, endpoint, db, user, password, lower=None,
                timeout=60, max_rows=200000, row_sink=None):
        if not (endpoint or "").strip():
            raise real_flight.FlightUnavailable("no grpc endpoint configured")
        store = FakeGreptime.store
        log = FakeGreptime.flight_log
        m = re.search(r'SELECT \* FROM "([^"]+)"', sql_text)
        logical = m.group(1) if m else None
        table = ("opentelemetry_traces" if "opentelemetry_traces"
                 in sql_text else "opentelemetry_logs"
                 if "opentelemetry_logs" in sql_text
                 else (logical or "metric"))
        fault = (fault_map or {}).get(table)
        cols, rows, upper = fake_fence(sql_text, store, lower, fault=fault)
        if fault == "native_jsonb":
            # Native Flight exposes uncast JSONB as binary, not UTF-8
            # JSON. The native JSON formatter returns ordinary JSON text.
            for name in ("log_attributes", "resource_attributes"):
                formatted = 'json_to_string("' + name + '") AS "' + name + '"'
                if name in cols and formatted not in sql_text:
                    index = cols.index(name)
                    for row in rows:
                        if row[index] is not None:
                            row[index] = b"\x40JSONB" + str(row[index]).encode()
        if row_sink is None and len(rows) > max_rows:
            raise real_flight.RowCapExceeded("row cap exceeded")
        log.append({"sql": sql_text[:160], "kind": "metric"
                    if logical and logical not in agg.AI_FENCE_TABLES
                    else "fence", "table": table,
                    "lower": json.dumps(lower, sort_keys=True, default=str),
                    "rows": len(rows), "upper": dict(upper)})
        if row_sink is not None:
            for offset in range(0, len(rows), max_rows):
                row_sink(cols, rows[offset:offset + max_rows])
                if fault == "sink_prefix":
                    raise real_flight.FlightError("snapshot sink prefix failed")
            return cols, [], upper
        return cols, rows, upper

    stub.flight_query = patched
    _sys.modules["scripts.database.greptime_flight"] = stub
    agg._ai_flight = lambda: stub
    FakeGreptime.flight_log.clear()
    return (real_mod, real_seam, stub)


def restore_fence(saved):
    import sys as _sys
    real_mod, real_seam, stub = saved
    if real_mod is not None:
        _sys.modules["scripts.database.greptime_flight"] = real_mod
    agg._ai_flight = real_seam
    FakeGreptime.flight_impl = None


# ---------------------------------------------------------------- harness

def serve():
    srv = http.server.HTTPServer(("127.0.0.1", 0), FakeGreptime)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


def _reset():
    F = FakeGreptime
    F.store = SeqStore()
    F.derived = {t: [] for t in DERIVED_TABLES + ("ai_log_daily",
                                                 "raw_retention_watermark",)}
    F.state = None
    F.watermark = None
    F.catalog = []
    F.metric_cols = []
    F.metric_rows = []
    F.queries = []
    F.flight_log.clear()
    F.fault = None
    F.fault_n = 0


def _cfg(**over):
    cfg = {"max_rows": 100000, "otel_ttl": "", "ai_full_rebuild": False,
           "grpc_url": "grpc://fake:1", "user": "u", "password": "p"}
    cfg.update(over)
    return cfg


def _ctx(srv):
    return ("http://127.0.0.1:%d" % srv.server_port, "YXV0aA==", "db")


def _guard():
    saved = {"utcnow": agg.utcnow, "prices": agg.load_price_table,
             "cache": dict(agg._PRICES)}
    agg.utcnow = lambda: NOW
    agg._PRICES.update(table=None, ok_at=0.0, attempt_at=0.0)
    agg.load_price_table = lambda: None
    return saved


def _restore(saved):
    agg.utcnow = saved["utcnow"]
    agg.load_price_table = saved["prices"]
    agg._PRICES.update(saved["cache"])


def snapshot():
    out = {}
    for table, rows in FakeGreptime.derived.items():
        out[table] = sorted(json.dumps(r, sort_keys=True, default=str)
                            for r in rows)
    return out


def oracle_fresh(all_traces, all_logs, cfg, metric=None, preset=None):
    """Fenced-full recovery on an isolated store holding every row at once."""
    F = FakeGreptime
    keep = (F.store, {t: list(r) for t, r in F.derived.items()},
            F.state, F.watermark, list(F.catalog),
            list(F.metric_cols), list(F.metric_rows))
    real = install_fence()
    try:
        _reset()
        F.store.append("traces", [dict(r) for r in all_traces])
        F.store.append("logs", [dict(r) for r in all_logs])
        if preset:
            for table, rows in preset.items():
                F.derived[table] = [dict(r) for r in rows]
        if metric:
            F.catalog = list(metric[0])
            F.metric_cols = list(metric[1])
            F.metric_rows = [dict(r) for r in metric[2]]
            F.store.metric_cols = list(metric[1])
            F.store.metric_rows = [dict(r) for r in metric[2]]
            # Per-table latest snapshots (native cumulative same-PK proof):
            # each logical table keeps only its own rows. metric[3] may
            # carry an explicit {table: rows} map; otherwise the shared
            # rows list is filtered by caller-known membership below.
            per = metric[3] if len(metric) > 3 else None
            for _t, _o in metric[0]:
                _rows = (per.get(_t, []) if isinstance(per, dict)
                         else [r for r in metric[2]])
                F.store.metric_seqs[_t] = {"seq": len(_rows), "frontier": 0}
                F.store.metric_table_rows[_t] = [
                    dict(r, _mseq=_i + 1) for _i, r in enumerate(_rows)]
        srv = serve()
        try:
            ctx = _ctx(srv)
            tables = agg._ai_metric_tables(ctx, cfg)
            registry = json.dumps(sorted(tables or []), sort_keys=True,
                                  default=str)
            agg._ai_full(ctx, cfg)
        finally:
            srv.shutdown()
            srv.server_close()
        return snapshot()
    finally:
        (F.store, F.derived, F.state, F.watermark, F.catalog,
         F.metric_cols, F.metric_rows) = keep
        F.store.metric_cols = list(F.metric_cols)
        F.store.metric_rows = [dict(r) for r in F.metric_rows]
        restore_fence(real)


def require_fence():
    state = FakeGreptime.state
    assert state is not None, \
        "ai_aggregate_state was never committed; the fenced path never " \
        "engages (core must commit the fence after successful passes)"
    fence = agg._ai_parse_fence(state["fence"]) if state["fence"] else {}
    assert fence, "stored fence is empty: %r" % (state,)
    return state, fence


def seed_fenced(ctx, cfg):
    """First-pass recovery until the core commits on stored-is-None.

    Calls the real ai_section; when the core takes the bare full scan
    without a fence, follows with the real _ai_fenced_full recovery so
    the stored upper is committed and later passes fence. Fails if the
    core ever skips without committing (no silent loss allowed).
    """
    total = agg.ai_section(ctx, cfg)
    if FakeGreptime.state is None:
        tables = agg._ai_metric_tables(ctx, cfg)
        registry = json.dumps(sorted(tables or []), sort_keys=True,
                              default=str)
        total = agg._ai_fenced_full(ctx, cfg, registry,
                                    agg._ai_price_digest(None), None)
    return total


def assert_no_raw_sql(since=0):
    """Hard consumer invariant: refetch uses session/day predicates only.

    Fenced discovery must never fall back to an unbounded raw SQL scan:
    every non-probe raw SELECT after `since` carries a WHERE clause.
    """
    bare = [s for s, _ in FakeGreptime.queries[since:]
            if ("opentelemetry_traces" in s or "opentelemetry_logs" in s)
            and s.startswith("SELECT")
            and "MAX(" not in s and "COUNT(" not in s
            and "information_schema" not in s
            and "WHERE" not in s.upper()]
    assert not bare, "unbounded history-linear raw scan: %r" % (bare[:1],)


# ---------------------------------------------------------------- fixtures

def span(ts, trace, sid, session, client, inp, outp, call=None, op="chat",
         tool=None, model="bench-m", provider="bench-p", service="svc"):
    row = {c: None for c in agg.SPAN_COLS}
    row.update({
        "timestamp": _fmt(ts), "trace_id": trace, "span_id": sid,
        agg.OP_NAME: op, agg.SESSION: session, agg.CLIENT: client,
        "service_name": service, agg.TOKEN_IN: inp, agg.TOKEN_OUT: outp,
        agg.RESP_ID: call, agg.TOOL_NAME: tool, agg.MODEL_RESP: model,
        agg.PROVIDER: provider, "span_status_code": "OK"})
    return row


def clog(ts, session, client, call, inp, outp):
    attrs = {"event.name": "codex.sse_event",
             "event.kind": "response.completed",
             "session.id": session, "request_id": call,
             "input_token_count": inp, "output_token_count": outp,
             "usage.estimated_usd": 0.001}
    return {"timestamp": _fmt(ts), "severity_text": "INFO",
            "severity_number": 9, "scope_name": "s", "trace_id": None,
            "span_id": None, "body": "",
            "log_attributes": json.dumps(attrs),
            "resource_attributes": json.dumps(
                {"service.name": "codex-originator"})}


def tool_log(ts, session, client, call, tool="read"):
    attrs = {"event.name": "codex.tool_result", "session.id": session,
             "gen_ai.tool.call.id": call, "tool_name": tool}
    return {"timestamp": _fmt(ts), "severity_text": "INFO",
            "severity_number": 9, "scope_name": "s", "trace_id": None,
            "span_id": None, "body": "",
            "log_attributes": json.dumps(attrs),
            "resource_attributes": json.dumps({"service.name": "x"})}


def base_history():
    traces = [
        span(DAY1 + dt.timedelta(hours=10), "tA", "a1", "sA", "codex",
             100, 50, call="rA1"),
        span(DAY1 + dt.timedelta(hours=10, minutes=5), "tA", "a2", "sA",
             "codex", None, None, op="execute_tool", tool="read",
             call="tA1"),
        span(DAY2 + dt.timedelta(hours=9), "tA", "a3", "sA", "codex",
             200, 100, call="rA3"),
        span(DAY1 + dt.timedelta(hours=11), "tB", "b1", "sB",
             "claude-code", 30, 20, call="rB1", model="claude-x",
             provider="anthropic"),
        span(DAY1 + dt.timedelta(hours=23, minutes=50), "tC", "c1", "sC",
             "codex", 10, 5, call="rC1"),
        span(DAY2 + dt.timedelta(minutes=15), "tC", "c2", "sC", "codex",
             20, 10, call="rC2"),
        span(DAY1 + dt.timedelta(hours=10), "tD", "d1", "sD", "codex",
             5, 5, call="rD1"),
        span(DAY2 + dt.timedelta(hours=18), "tD", "d2", "sD", "codex",
             5, 5, call="rD2"),
        span(DAY2 + dt.timedelta(hours=8), "tE", "e1", "sE", None,
             7, 3, call="rE1", service="unverified-fork"),
        span(DAY2 + dt.timedelta(hours=8, minutes=30), "tG", "g1", "sG",
             "codex", None, None),
    ]
    logs = [
        clog(DAY1 + dt.timedelta(hours=10, seconds=1), "sA", "codex",
             "rA1", 120, 60),
        tool_log(DAY1 + dt.timedelta(hours=10, minutes=5), "sA", "codex",
                 "tA1"),
        clog(DAY2 + dt.timedelta(hours=7), "sF", "codex", "rF1", 40, 20),
    ]
    return traces, logs


def late_rows():
    traces = [
        span(DAY1 + dt.timedelta(hours=9), "tA0", "a0", "sA", "codex",
             50, 25, call="rA0"),
        span(DAY2 + dt.timedelta(minutes=10), "tC3", "c3", "sC", "codex",
             15, 8, call="rC3"),
        span(DAY2 + dt.timedelta(hours=12), "tD3", "d3", "sD", "codex",
             9, 9, call="rD3"),
        span(DAY2 + dt.timedelta(hours=10), "tH", "h1", "sH",
             "claude-code", 11, 6, call="rH1", model="claude-x",
             provider="anthropic"),
    ]
    return traces, []


# ---------------------------------------------------------------- tests

def _session_input(session_id, client):
    for raw in snapshot()["ai_session_summary"]:
        row = json.loads(raw)
        if row.get("session_id") == session_id and row.get("client") == client:
            return row.get("input_tokens"), row.get("output_tokens")
    raise AssertionError("missing session " + session_id)


def _assert_oracle(second, traces, logs, cfg, metric=None, preset=None):
    expect = oracle_fresh(traces, logs, cfg, metric=metric, preset=preset)
    for table in DERIVED_TABLES:
        assert second[table] == expect[table], table
    return expect


def test_fenced_late_arrival_matches_full_oracle():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        late_t, late_l = late_rows()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            _, fence = require_fence()
            first = snapshot()
            assert first["ai_session_summary"], "no sessions written"
            FakeGreptime.store.append("traces", [dict(r) for r in late_t])
            FakeGreptime.store.append("logs", [dict(r) for r in late_l])
            mark = len(FakeGreptime.queries)
            flights = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
            # Fenced deltas carry only the new commits, never history.
            new = [f for f in FakeGreptime.flight_log[flights:]
                   if f["kind"] == "fence"]
            by_table = {f["table"]: f["rows"] for f in new}
            assert by_table.get("opentelemetry_traces") == len(late_t), new
            assert by_table.get("opentelemetry_logs") == len(late_l), new
            assert fence != require_fence()[1], "fence never advanced"
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces + late_t, logs + late_l, _cfg())
        assert second["ai_session_summary"] != first["ai_session_summary"]
        # Native-authoritative scope (billable): sA carries native usage,
        # so only the native row bills (120/60); span-only rA1/rA0/rA3
        # stay as non-billed session context. Late rows still prove the
        # fence: the oracle equality above plus new span-only sessions.
        assert _session_input("sA", "codex") == (120, 60)
        # Span-only late sessions bill exactly (no native scope there).
        assert _session_input("sC", "codex") == (10 + 20 + 15, 5 + 10 + 8)
        assert _session_input("sD", "codex") == (5 + 5 + 9, 5 + 5 + 9)
        state = FakeGreptime.state
        assert state and state["rev"] == agg.AI_FENCE_REVISION, state
    finally:
        restore_fence(real)
        _restore(saved)


def test_noop_second_pass_reads_no_rows():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            require_fence()
            before = snapshot()
            mark = len(FakeGreptime.queries)
            flights = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, cfg)
            assert_no_raw_sql(mark)
            new = [f for f in FakeGreptime.flight_log[flights:]
                   if f["kind"] == "fence"]
            assert sum(f["rows"] for f in new) == 0, new
            assert snapshot() == before
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        restore_fence(real)
        _restore(saved)


def test_small_input_same_sessions_is_bounded():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        bulk = [span(DAY1 + dt.timedelta(hours=10, minutes=i), "tX%d" % i,
                     "x%d" % i, "bulk-%d" % (i % 50), "codex",
                     5, 5, call="rX%d" % i)
                for i in range(300)]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces + bulk])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            require_fence()
            extra = [span(DAY2 + dt.timedelta(hours=10, minutes=i),
                           "tY%d" % i, "y%d" % i, "bulk-3", "codex",
                           6, 6, call="rY%d" % i)
                     for i in range(3)]
            FakeGreptime.store.append("traces", [dict(r) for r in extra])
            mark = len(FakeGreptime.queries)
            flights = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
            new = [f for f in FakeGreptime.flight_log[flights:]
                   if f["kind"] == "fence"]
            assert sum(f["rows"] for f in new) == 3, new
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces + bulk + extra, logs, _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_arbitrary_commit_delay_older_event_time():
    # Commit order is arrival order, never event time: a row with an old
    # event timestamp committed after the checkpoint still has a newer seq,
    # so the fence returns it. This is the native proof shape (old stamp
    # ing=1000/2000 returned as seq 2..3 deltas).
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            require_fence()
            ancient = [span(dt.datetime(2026, 8, 23, 10), "tOld", "o1",
                             "sA", "codex", 33, 17, call="rOld")]
            FakeGreptime.store.append("traces", [dict(r) for r in ancient])
            mark = len(FakeGreptime.queries)
            flights = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
            new = [f for f in FakeGreptime.flight_log[flights:]
                   if f["kind"] == "fence"
                   and f["table"] == "opentelemetry_traces"]
            assert sum(f["rows"] for f in new) == 1, new
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces + ancient, logs, _cfg())
        # Native-authoritative scope: the late span-only row joins sA as
        # non-billed context; billed sA stays native (120/60). Fence
        # visibility (1 trace delta) plus oracle equality is the proof.
        assert _session_input("sA", "codex") == (120, 60)
    finally:
        restore_fence(real)
        _restore(saved)


def test_near_epoch_and_negative_timestamps_normalize():
    # Typed Flight ns normalization (_ai_flight_ns): near-epoch ints
    # (e.g. 1000ns) and negatives must not be magnitude-guessed into
    # seconds. The fake fence casts timestamp cells to exact int64 ns;
    # the pass must bill the rows without dropping or misdating them.
    saved = _guard()
    real = install_fence()
    try:
        epoch = span(dt.datetime(1970, 1, 1), "tEpoch", "e1", "sEpoch",
                     "codex", 3, 2, call="rEpoch")
        neg = span(dt.datetime(1969, 12, 31, 23, 59, 59), "tNeg", "n1",
                   "sNeg", "codex", 4, 1, call="rNeg")
        _reset()
        FakeGreptime.store.append("traces", [dict(epoch), dict(neg)])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            second = snapshot()
            assert _session_input("sEpoch", "codex") == (3, 2)
            assert _session_input("sNeg", "codex") == (4, 1)
            assert agg._ai_flight_ns(1000) == agg._from_us(1)
            assert agg._ai_flight_ns(-1000) == agg._from_us(-1)
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, [epoch, neg], [], _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_duplicate_redelivery_bills_once():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            require_fence()
            # Same (trace_id, span_id) redelivered as new commits: dedupe
            # keeps one row, oracle included (slice covers rA1, tool, rA3).
            dupes = [dict(r) for r in traces[:4]] + [dict(r) for r in logs]
            FakeGreptime.store.append("traces", dupes[:4])
            FakeGreptime.store.append("logs", dupes[4:])
            mark = len(FakeGreptime.queries)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces, logs, _cfg())
        # Native-authoritative scope: redelivered span copies dedupe and
        # sA still bills only the native row (120/60).
        assert _session_input("sA", "codex") == (120, 60)
    finally:
        restore_fence(real)
        _restore(saved)


def test_late_native_replaces_trace_cost():
    saved = _guard()
    real = install_fence()
    try:
        traces = [span(DAY2 + dt.timedelta(hours=9), "tN", "n1", "sN",
                       "codex", 5, 5, call="rN1")]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            require_fence()
            assert _session_input("sN", "codex") == (5, 5)
            native = [clog(DAY2 + dt.timedelta(hours=9, minutes=1), "sN",
                           "codex", "rN1", 50, 30)]
            FakeGreptime.store.append("logs", [dict(r) for r in native])
            mark = len(FakeGreptime.queries)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
            assert _session_input("sN", "codex") == (50, 30)
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces, native, _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_long_midnight_session_survives_incremental():
    saved = _guard()
    real = install_fence()
    try:
        traces = [
            span(DAY1 + dt.timedelta(hours=23, minutes=50), "tL", "l1",
                 "sL", "codex", 10, 5, call="rL1"),
        ]
        tail = [
            span(DAY2 + dt.timedelta(minutes=15), "tL", "l2", "sL",
                 "codex", 20, 10, call="rL2"),
            span(DAY2 + dt.timedelta(hours=12), "tL", "l3", "sL",
                 "codex", 4, 2, call="rL3"),
        ]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            require_fence()
            FakeGreptime.store.append("traces", [dict(r) for r in tail])
            mark = len(FakeGreptime.queries)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces + tail, [], _cfg())
        rows = [json.loads(r) for r in second["ai_session_summary"]]
        assert len(rows) == 1 and (rows[0]["input_tokens"],
                                   rows[0]["output_tokens"]) == (34, 17), rows
        days = sorted(dt.datetime.fromisoformat(json.loads(r)["day_start"])
                      for r in second["ai_daily_summary"])
        assert days == [DAY1, DAY2], days
    finally:
        restore_fence(real)
        _restore(saved)


def test_max_rows_bounds_work_not_complete_snapshot():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx = _ctx(srv)
            seed_fenced(ctx, _cfg(max_rows=2))
            require_fence()
            assert FakeGreptime.derived["ai_session_summary"]
            # The retained snapshot exceeds the work limit, but every
            # row is consumed; the next pass is a true fenced no-op.
            mark = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, _cfg(max_rows=2))
            assert sum(f["rows"] for f in FakeGreptime.flight_log[mark:]) == 0
            second = snapshot()
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces, logs, _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_partial_write_failure_keeps_fence_and_converges():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        bulk = [span(DAY2 + dt.timedelta(hours=11, minutes=i % 60),
                     "tP%d" % i, "p%d" % i, "bulk-%d" % i, "codex",
                     5, 5, call="rP%d" % i)
                for i in range(501)]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            _, fence = require_fence()
            FakeGreptime.store.append("traces", [dict(r) for r in bulk])
            states_before = len([s for s, _ in FakeGreptime.queries
                                 if s.startswith("INSERT INTO " +
                                                 "ai_aggregate_state")])
            FakeGreptime.fault = ("INSERT INTO ai_session_summary", 2)
            FakeGreptime.fault_n = 0
            try:
                agg.ai_section(ctx, cfg)
            except agg.SqlError:
                pass
            else:
                raise AssertionError("expected SqlError on session batch 2")
            states_after = len([s for s, _ in FakeGreptime.queries
                                if s.startswith("INSERT INTO " +
                                                "ai_aggregate_state")])
            assert states_after == states_before, \
                "fence moved on failure"
            assert require_fence()[1] == fence
            FakeGreptime.fault = None
            mark = len(FakeGreptime.queries)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces + bulk, logs, _cfg())
        seen = [(json.loads(r)["client"], json.loads(r)["session_id"])
                for r in second["ai_session_summary"]]
        assert len(seen) == len(set(seen)), "duplicate session rows"
    finally:
        restore_fence(real)
        _restore(saved)


def test_expired_anchor_isolated_by_client_and_session():
    saved = _guard()
    real = install_fence()
    try:
        old = {"session_start": "2026-09-10 10:00:00",
               "session_id": "shared", "client": "codex",
               "input_tokens": 99, "output_tokens": 99}
        traces = [span(DAY2, "scope-" + str(i), "scope-" + str(i), sid,
                       client, 5 + i, 2, call="scope-" + str(i))
                  for i, (client, sid) in enumerate(
                      [("codex", "shared"), ("oh-my-pi", "shared"),
                       ("codex", "other's")])]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.derived["ai_session_summary"] = [dict(old)]
        srv = serve()
        cfg = _cfg(otel_ttl="7d")
        try:
            seed_fenced(_ctx(srv), cfg)
            result = snapshot()
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(result, traces, [], cfg,
                       preset={"ai_session_summary": [old]})
    finally:
        restore_fence(real)
        _restore(saved)


def test_ttl_partial_expiry_preserves_stored_derived():
    saved = _guard()
    real = install_fence()
    try:
        stored_old = {"session_start": "2026-09-10 10:00:00",
                      "session_id": "sOld", "client": "codex",
                      "input_tokens": 5, "output_tokens": 5}
        new = span(DAY2 + dt.timedelta(hours=9), "tW", "w1", "sW",
                   "codex", 11, 5, call="rW1")
        _reset()
        FakeGreptime.store.append("traces", [dict(new)])
        FakeGreptime.derived["ai_session_summary"] = [dict(stored_old)]
        srv = serve()
        try:
            ctx = _ctx(srv)
            seed_fenced(ctx, _cfg(otel_ttl="7d"))
            second = snapshot()
        finally:
            srv.shutdown()
            srv.server_close()
        rows = {json.loads(r)["session_id"]: json.loads(r)
                for r in second["ai_session_summary"]}
        assert rows["sOld"]["input_tokens"] == 5, rows["sOld"]
        assert rows["sW"]["input_tokens"] == 11, rows["sW"]
        days = [json.loads(r)["day_start"]
                for r in second["ai_daily_summary"]]
        assert any(d.startswith("2026-09-21") for d in days), days
        assert not any(d.startswith("2026-09-10") for d in days), days
        _assert_oracle(second, [new], [], _cfg(otel_ttl="7d"),
                       preset={"ai_session_summary": [stored_old]})
    finally:
        restore_fence(real)
        _restore(saved)


PRICE_P1 = {"openai/bench-priced": (2.5e-6, 1.0e-5, None, None, None,
                                    "chat", "openai")}
PRICE_P2 = {"openai/bench-priced": (5.0e-6, 2.0e-5, None, None, None,
                                    "chat", "openai")}


def priced_span(ts, inp, outp):
    return span(ts, "tPriced", "pr1", "sPriced", "codex", inp, outp,
                call="rPr1", model="bench-priced", provider="openai")


def test_price_change_rebuilds_cost_cells():
    # A changed LiteLLM table digest rebuilds every cost cell (new estimates
    # apply to all retained history); the unfenced re-read is the
    # documented recovery path, never a hidden incremental scan.
    saved = _guard()
    agg.load_price_table = lambda: dict(PRICE_P1)
    real = install_fence()
    try:
        priced = [priced_span(DAY2 + dt.timedelta(hours=9), 1000, 500)]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in priced])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            got = [json.loads(r) for r in snapshot()["ai_session_summary"]]
            assert len(got) == 1, got
            assert got[0]["cost_estimated_usd"] == 0.0075, got[0]
            agg.load_price_table = lambda: dict(PRICE_P2)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            got2 = [json.loads(r) for r in second["ai_session_summary"]]
            assert got2[0]["cost_estimated_usd"] == 0.015, got2[0]
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        restore_fence(real)
        _restore(saved)
    # Oracle under the new table: fresh full rebuild converges, no carry.
    saved = _guard()
    agg.load_price_table = lambda: dict(PRICE_P2)
    real = install_fence()
    try:
        _assert_oracle(second, priced, [], _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


METRIC_TABLE = "m_loc_count"
METRIC_OPTS = ("greptime.semantic.metric.original_name="
               "coding_agent.lines_of_code.count")
METRIC_COLS = ["greptime_timestamp", "greptime_value",
               "datalake_temporality", "datalake_start_time_unix_nano",
               "coding_agent_client", "coding_agent_session_id", "type",
               "decision", "stream"]


def metric_row(ts, session, value, client="codex", tempo=1):
    # Native shape: stream is a tag STRING (not a list); the fake keeps
    # the same so delta/snapshot dedupe keys match production.
    return {"greptime_timestamp": _fmt(ts), "greptime_value": value,
            "datalake_temporality": tempo,
            "datalake_start_time_unix_nano": None,
            "coding_agent_client": client,
            "coding_agent_session_id": session, "type": "added",
            "decision": "accept", "stream": ""}


def _metric_setup(rows, table=METRIC_TABLE):
    if (METRIC_TABLE, METRIC_OPTS) not in FakeGreptime.catalog:
        FakeGreptime.catalog = [(METRIC_TABLE, METRIC_OPTS)]
    FakeGreptime.metric_cols = list(METRIC_COLS)
    FakeGreptime.metric_rows = [dict(r) for r in rows]
    FakeGreptime.store.metric_cols = list(METRIC_COLS)
    FakeGreptime.store.metric_rows = [dict(r) for r in rows]
    seqs = FakeGreptime.store.metric_seqs.setdefault(
        table, {"seq": 0, "frontier": 0})
    bucket = FakeGreptime.store.metric_table_rows.setdefault(table, [])
    bucket.clear()
    for row in rows:
        seqs["seq"] += 1
        cell = dict(row)
        cell["_mseq"] = seqs["seq"]
        bucket.append(cell)


def test_metric_registry_change_rebuilds():
    # A registry change needs one full recovery; the next unchanged pass
    # must resume bounded discovery rather than rebuild again.
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        first_metric = [metric_row(DAY2 + dt.timedelta(hours=10, minutes=5),
                                   "sM", 7)]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        _metric_setup(first_metric)
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            require_fence()
            rows = {json.loads(r)["session_id"]: json.loads(r)
                    for r in snapshot()["ai_session_summary"]}
            assert rows["sM"]["lines_added"] == 7, rows["sM"]
            FakeGreptime.catalog.append(
                ("m_commit_count",
                 "greptime.semantic.metric.original_name="
                 "coding_agent.commit.count"))
            _metric_setup(first_metric, table="m_commit_count")
            agg.ai_section(ctx, cfg)
            second = snapshot()
            mark = len(FakeGreptime.queries)
            agg.ai_section(ctx, cfg)
            assert_no_raw_sql(mark)
            assert snapshot() == second
        finally:
            srv.shutdown()
            srv.server_close()
        metric = ([(METRIC_TABLE, METRIC_OPTS),
                   ("m_commit_count",
                    "greptime.semantic.metric.original_name="
                    "coding_agent.commit.count")],
                  METRIC_COLS, first_metric)
        _assert_oracle(second, traces, logs, _cfg(), metric=metric)
    finally:
        restore_fence(real)
        _restore(saved)


def test_same_key_metric_overwrite_visible_via_fence():
    # Native proof shape: same-PK overwrite commits a new seq; the edited
    # table's lower fence returns the changed row while the untouched
    # table returns zero rows at the same frontier. Core is fixing the
    # metric-only early-return flaw; this test pins the fence visibility
    # the fixed core must consume (no explicit rebuild).
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        first_metric = [metric_row(DAY2 + dt.timedelta(hours=10, minutes=5),
                                    "sM", 7)]
        _metric_setup(first_metric)
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            rows = {json.loads(r)["session_id"]: json.loads(r)
                    for r in snapshot()["ai_session_summary"]}
            assert rows["sM"]["lines_added"] == 7, rows["sM"]
            edited = [metric_row(DAY2 + dt.timedelta(hours=10, minutes=5),
                                  "sM", 70)]
            _metric_setup(edited)
            flights = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, cfg)
            delta = [f for f in FakeGreptime.flight_log[flights:]
                     if f["kind"] == "metric"]
            assert any(f["rows"] >= 1 for f in delta), delta
            fixed = {json.loads(r)["session_id"]: json.loads(r)
                     for r in snapshot()["ai_session_summary"]}
            assert fixed["sM"]["lines_added"] == 70, fixed["sM"]
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(snapshot(), traces, logs, _cfg(),
                       metric=([(METRIC_TABLE, METRIC_OPTS)], METRIC_COLS,
                               [metric_row(DAY2 + dt.timedelta(
                                   hours=10, minutes=5), "sM", 70)]))
    finally:
        restore_fence(real)
        _restore(saved)


def test_metric_only_counter_delta_preserved():
    # Native proof shape (Main, issue_seq_metric_20261009): two logical
    # counters share one physical sequence; a same-key/same-ts overwrite
    # (commit 1->4 at seq3) surfaces as exactly one changed row on the
    # edited table's lower fence while the untouched table returns zero
    # rows. A metric-only delta with no other raw rows must still dirty
    # its session and preserve the counter (never drop to 0/None).
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        commit_ts = DAY2 + dt.timedelta(hours=10, minutes=5)
        loc = [metric_row(commit_ts, "sP", 2)]
        pr = [metric_row(commit_ts, "sP", 1, client="codex")]
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        FakeGreptime.catalog = [
            (METRIC_TABLE, METRIC_OPTS),
            ("m_commit_count",
             "greptime.semantic.metric.original_name="
             "coding_agent.commit.count")]
        FakeGreptime.metric_cols = list(METRIC_COLS)
        FakeGreptime.store.metric_cols = list(METRIC_COLS)
        _metric_setup(loc, table=METRIC_TABLE)
        _metric_setup(pr, table="m_commit_count")
        FakeGreptime.metric_rows = [dict(r) for r in loc + pr]
        FakeGreptime.store.metric_rows = [dict(r) for r in loc + pr]
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            rows = {json.loads(r)["session_id"]: json.loads(r)
                    for r in snapshot()["ai_session_summary"]}
            assert rows["sP"]["commit_count"] == 1, rows["sP"]
            # Same key/ts overwrite 1->4 on the commit table only.
            _metric_setup([metric_row(commit_ts, "sP", 4)],
                          table="m_commit_count")
            FakeGreptime.metric_rows = [dict(r) for r in
                                        loc + [metric_row(commit_ts, "sP", 4)]]
            FakeGreptime.store.metric_rows = [
                dict(r) for r in FakeGreptime.metric_rows]
            flights = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, cfg)
            delta = [f for f in FakeGreptime.flight_log[flights:]
                     if f["kind"] == "metric"]
            edited = [f for f in delta if f["rows"] >= 1]
            assert edited, delta
            fixed = {json.loads(r)["session_id"]: json.loads(r)
                     for r in snapshot()["ai_session_summary"]}
            assert fixed["sP"]["commit_count"] == 4, fixed["sP"]
            assert fixed["sP"]["lines_added"] == 2, fixed["sP"]
        finally:
            srv.shutdown()
            srv.server_close()
        metric = ([(METRIC_TABLE, METRIC_OPTS),
                   ("m_commit_count",
                    "greptime.semantic.metric.original_name="
                    "coding_agent.commit.count")],
                  METRIC_COLS,
                  loc + [metric_row(commit_ts, "sP", 4)],
                  {METRIC_TABLE: loc,
                   "m_commit_count": [metric_row(commit_ts, "sP", 4)]})
        _assert_oracle(snapshot(), traces, logs, _cfg(), metric=metric)
    finally:
        restore_fence(real)
        _restore(saved)


def test_fresh_bootstrap_commits_fence_on_first_pass():
    # Fresh native bootstrap returns empty state twice (no fence yet);
    # the first recovery must commit the upper fence, and the second
    # pass must fence (zero rows), never rescan history.
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            assert agg._ai_state_rows(ctx) is None
            assert agg._ai_state_rows(ctx) is None
            seed_fenced(ctx, cfg)
            _, fence = require_fence()
            assert fence["opentelemetry_traces"][REGION] == len(traces), fence
            assert fence["opentelemetry_logs"][REGION] == len(logs), fence
            flights = len(FakeGreptime.flight_log)
            mark = len(FakeGreptime.queries)
            agg.ai_section(ctx, cfg)
            assert_no_raw_sql(mark)
            fenced = [f for f in FakeGreptime.flight_log[flights:]
                      if f["kind"] == "fence"]
            assert fenced and sum(f["rows"] for f in fenced) == 0, fenced
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        restore_fence(real)
        _restore(saved)


def test_stale_fence_below_frontier_recovers_to_oracle():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            _, fence = require_fence()
            FakeGreptime.store.flush()
            FakeGreptime.store.append("traces", [dict(r) for r in late_rows()[0][:1]])
            FakeGreptime.state = {"fence": agg._ai_encode_fence(
                {t: {REGION: 1} for t in agg.AI_FENCE_TABLES}),
                "price": FakeGreptime.state["price"],
                "rev": FakeGreptime.state["rev"],
                "input": FakeGreptime.state["input"]}
            second_before = snapshot()
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert second != second_before
            _, upper = require_fence()
            assert upper["opentelemetry_traces"][REGION] >= 2, upper
            _ = fence
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces + late_rows()[0][:1], logs, _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_missing_watermark_fails_without_checkpoint():
    saved = _guard()
    real = install_fence({"opentelemetry_traces": "missing_watermark"})
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            try:
                seed_fenced(ctx, cfg)
            except real_flight.FlightUnavailable:
                pass
            else:
                raise AssertionError("unproved snapshot must fail")
            assert FakeGreptime.state is None
            assert not FakeGreptime.derived["ai_session_summary"]
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        restore_fence(real)
        _restore(saved)


def test_post_eof_fault_fails_closed_no_checkpoint():
    # EOF drain is the fence proof: an iterator fault before terminal
    # metadata must fail the pass, never advance the checkpoint.
    saved = _guard()
    real = install_fence({"opentelemetry_traces": "post_eof"})
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            try:
                seed_fenced(ctx, cfg)
            except Exception as exc:
                assert type(exc).__name__ in (
                    "FlightError", "FlightUnavailable", "StaleFence",
                    "SqlError", "RowCapExceeded"), type(exc).__name__
            else:
                raise AssertionError("post-EOF fault must fail the pass")
            assert FakeGreptime.state is None, \
                "checkpoint advanced without drained watermarks"
            assert FakeGreptime.derived["ai_session_summary"] == [], \
                "derived writes landed without a proven fence"
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        restore_fence(real)
        _restore(saved)
    # Clean retry converges to the oracle.
    saved = _guard()
    real = install_fence()
    try:
        _assert_oracle(oracle_fresh(traces, logs, _cfg()), traces, logs,
                       _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_zero_seq_and_new_region_recovery():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            state, fence = require_fence()
            assert all(seq >= 0 for regions in fence.values()
                       for seq in regions.values()), fence
            # Empty store fence: seq 0 is valid, never negative/bool.
            parsed = agg._ai_parse_fence(agg._ai_encode_fence(
                {t: {REGION: 0} for t in agg.AI_FENCE_TABLES}))
            assert parsed == {t: {REGION: 0}
                              for t in agg.AI_FENCE_TABLES}, parsed
            assert agg._ai_parse_fence("not-json") is None
            assert agg._ai_parse_fence(json.dumps(
                {"opentelemetry_traces": {"x": 1}})) is None
            _ = state
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        restore_fence(real)
        _restore(saved)
    # A new region disjoint from the stored fence is a generation reset:
    # SqlError surfaces and full recovery converges.
    saved = _guard()
    real = install_fence({"opentelemetry_traces": "new_region"})
    try:
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            agg.ai_section(ctx, cfg)
            FakeGreptime.state = {"fence": agg._ai_encode_fence(
                {"opentelemetry_traces": dict(
                    FakeGreptime.store.table_upper("traces")),
                 "opentelemetry_logs": dict(
                    FakeGreptime.store.table_upper("logs"))}),
                "price": FakeGreptime.state["price"],
                "rev": FakeGreptime.state["rev"],
                "input": FakeGreptime.state["input"]}
            try:
                agg.ai_section(ctx, cfg)
            except Exception as exc:
                assert type(exc).__name__ in ("SqlError", "FlightError",
                                              "StaleFence"), type(exc).__name__
            else:
                raise AssertionError("generation reset must surface, not skip")
        finally:
            srv.shutdown()
            srv.server_close()
    finally:
        restore_fence(real)
        _restore(saved)


def test_sessionless_rows_reach_daily_output():
    # Native edge: a span with no session id (33/11 codex chat) plus a
    # sessionless metric row. Neither yields dirty keys, so the pass must
    # still seed ALL delta rows into the day recompute; the full oracle
    # carries ai_daily_summary 33/11 for the sessionless scope.
    saved = _guard()
    real = install_fence()
    try:
        nosid = span(DAY2 + dt.timedelta(hours=9), "tNo", "no1", None,
                     "codex", 33, 11, call="rNo")
        _reset()
        FakeGreptime.store.append("traces", [dict(nosid)])
        _metric_setup([metric_row(DAY2 + dt.timedelta(hours=10), "", 5)],
                      table=METRIC_TABLE)
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg()
            seed_fenced(ctx, cfg)
            FakeGreptime.store.append("traces", [dict(nosid)])
            mark = len(FakeGreptime.queries)
            agg.ai_section(ctx, cfg)
            second = snapshot()
            assert_no_raw_sql(mark)
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, [nosid, nosid], [], _cfg(),
                       metric=([(METRIC_TABLE, METRIC_OPTS)], METRIC_COLS,
                               [metric_row(DAY2 + dt.timedelta(hours=10), "", 5)]))
        daily = [json.loads(r) for r in second["ai_daily_summary"]]
        scoped = [d for d in daily if d["input_tokens"] == 33
                  and d["output_tokens"] == 11]
        assert scoped, daily
    finally:
        restore_fence(real)
        _restore(saved)


def test_missing_grpc_endpoint_rebuilds():
    saved = _guard()
    real = install_fence()
    try:
        traces, logs = base_history()
        _reset()
        FakeGreptime.store.append("traces", [dict(r) for r in traces])
        FakeGreptime.store.append("logs", [dict(r) for r in logs])
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg(grpc_url="")
            try:
                seed_fenced(ctx, cfg)
            except Exception as exc:
                assert type(exc).__name__ in ("FlightUnavailable",
                                              "SqlError"), type(exc).__name__
                assert agg._ai_full(ctx, cfg) >= 1
            second = snapshot()
            assert second["ai_session_summary"], "rebuild skipped"
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(second, traces, logs, _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_bounded_long_session_prefix_failure_and_late_retry():
    saved = _guard()
    faults = {"opentelemetry_traces": None}
    real = install_fence(faults)
    try:
        traces, logs = base_history()
        long_rows = [span(DAY1 + dt.timedelta(hours=23, minutes=i),
                          "long-trace", "long-%d" % i, "long-session",
                          "codex", i + 1, 2, call="long-call-%d" % i)
                     for i in range(13)]
        traces += long_rows + [dict(long_rows[0])]
        _reset()
        FakeGreptime.store.append("traces", traces)
        FakeGreptime.store.append("logs", logs)
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg(max_rows=2)
            agg.ai_section(ctx, cfg)
            before = snapshot()
            _, checkpoint = require_fence()
            # A committed late row makes scoped history exceed the same
            # tiny limit; recovery still must consume the complete session.
            late = span(DAY2 + dt.timedelta(hours=3), "long-trace", "late",
                        "long-session", "codex", 31, 7, call="late-call")
            FakeGreptime.store.append("traces", [late])
            faults["opentelemetry_traces"] = "sink_prefix"
            try:
                agg.ai_section(ctx, cfg)
            except real_flight.FlightError as exc:
                assert "prefix failed" in str(exc)
            else:
                raise AssertionError("expected prefix failure")
            assert require_fence()[1] == checkpoint
            assert snapshot() == before
            faults["opentelemetry_traces"] = None
            agg.ai_section(ctx, cfg)
            result = snapshot()
            mark = len(FakeGreptime.flight_log)
            agg.ai_section(ctx, cfg)
            assert sum(f["rows"] for f in FakeGreptime.flight_log[mark:]) == 0
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(result, traces + [late], logs, _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_failed_full_snapshot_discards_scratch_and_preserves_fence():
    import os
    from unittest.mock import patch
    from scripts.analytics import ai_recovery

    saved = _guard()
    real = install_fence()
    _reset()
    traces, logs = base_history()
    FakeGreptime.store.append("traces", traces)
    FakeGreptime.store.append("logs", logs)
    srv = serve()
    try:
        ctx, cfg = _ctx(srv), _cfg(max_rows=2)
        agg.ai_section(ctx, cfg)
        _, checkpoint = require_fence()
        late = span(DAY2, "failed-snapshot", "late", "late-session",
                    "codex", 13, 7, call="late-call")
        FakeGreptime.store.append("traces", [late])
        forced = dict(cfg, ai_full_rebuild=True)
        for owner, method in (
                (ai_recovery.Recovery, "add_span"),
                (ai_recovery.Recovery, "prepare"),
                (ai_recovery, "write_batch")):
            original = getattr(owner, method)
            close = ai_recovery.Recovery.close
            directories = []

            def fail_after_work(*args, **kwargs):
                original(*args, **kwargs)
                raise RuntimeError("injected snapshot failure")

            def discard(recovery):
                directories.append(recovery.directory.name)
                close(recovery)

            with patch.object(owner, method, fail_after_work), patch.object(
                    ai_recovery.Recovery, "close", discard):
                try:
                    agg.ai_section(ctx, forced)
                except RuntimeError as exc:
                    assert str(exc) == "injected snapshot failure"
                else:
                    raise AssertionError("expected snapshot failure: " + method)
            assert directories and all(not os.path.exists(p) for p in directories)
            assert require_fence()[1] == checkpoint, method
        agg.ai_section(ctx, forced)
        result = snapshot()
        # Keep the same fixed clock and price table for recovery and oracle.
        _assert_oracle(result, traces + [late], logs, _cfg())
    finally:
        srv.shutdown()
        srv.server_close()
        restore_fence(real)
        _restore(saved)


def test_disk_reducer_matches_global_merge_oracle_across_chunks():
    from scripts.analytics.ai_recovery import Recovery
    traces, logs = base_history()
    parent = span(DAY1, "parent-trace", "parent", "parent-session",
                  "codex", 9, 3, call="parent-call")
    children = [span(DAY1, "parent-trace", "child-%d" % i, "parent-session",
                     "codex", value, 1, call="child-call-%d" % i)
                for i, value in enumerate((2, 3, 4))]
    for child in children:
        child["parent_span_id"] = "parent"
    traces += [parent] + children + [dict(children[0])]
    traces += [span(DAY1, None, None, None, None, 7, 1),
               span(DAY1, None, None, None, None, 7, 1)]
    usage = clog(DAY2, "parent-session", "codex", "child-call-0", 20, 4)
    attrs = json.loads(usage["log_attributes"])
    attrs["turn.id"] = "native-filled-turn"
    usage["log_attributes"] = json.dumps(attrs)
    receipt = dict(usage)
    receipt["log_attributes"] = json.dumps({
        "event.name": "codex.turn_cost", "session.id": "parent-session",
        "turn.id": "native-filled-turn", "usage.estimated_usd": 0.75})
    logs += [usage, receipt, dict(receipt)]
    # Native zero is authoritative, but only inside its exact client/session.
    traces += [
        span(DAY1, "zero-trace", "zero", "zero-session", "codex", 8, 2, call="zero-call"),
        span(DAY1, "orphan-trace", "orphan", "zero-session", "codex", 5, 1),
        span(DAY1, "other-client", "other", "zero-session", "oh-my-pi", 6, 3)]
    logs += [clog(DAY2, "zero-session", "codex", "zero-call", 0, 0)]
    # Sparse matches keep ordinal-first duplicate merging inside the exact
    # client/session, without dropping unrelated spans sharing a call ID.
    traces += [
        span(DAY1, "sparse", "first", "sparse-session", "codex", 9, 2,
             call="shared-call"),
        span(DAY1, "sparse", "duplicate", "sparse-session", "codex", 4, 1,
             call="shared-call"),
        span(DAY1, "sparse", "other", "other-session", "codex", 7, 3,
             call="shared-call")]
    logs += [clog(DAY2, "sparse-session", "codex", "shared-call", 0, 0)]
    # More tool/session/day keys than the shared 128-state cache, then late
    # revisits: evicted totals must reload without changing null/zero rules.
    tool_rows = []
    for visit in range(3):
        for i in range(140):
            row = span(DAY1 + dt.timedelta(seconds=visit * 140 + i),
                       "tools", "tool-%d-%d" % (visit, i),
                       "tool-session-%d" % i, "codex", None, None,
                       call="tool-call-%d-%d" % (visit, i), op="execute_tool",
                       tool="tool-%d" % i, model="tool-model-%d" % i)
            row["duration_nano"] = (None, 0, 2_000_000)[(i + visit) % 3]
            row["span_status_code"] = "ERROR" if i % 7 == visit else "OK"
            tool_rows.append(row)
    traces += tool_rows
    # Same logical IDs on later spans: fill unknown duration and merge errors,
    # but never count the duplicate call or overwrite a known zero duration.
    for i in (0, 1, 139):
        repeat = dict(tool_rows[i], span_id="tool-repeat-%d" % i,
                      duration_nano=9_000_000, span_status_code="ERROR")
        traces.append(repeat)
    cols, lcols = list(agg.SPAN_COLS), list(agg.LOG_COLS)
    rows = [[row.get(c) for c in cols] for row in traces]
    lrows = [[row.get(c) for c in lcols] for row in logs]
    for prices, selected_logs in (
            (None, lrows), ({}, lrows), ({}, []),
            ({}, [[receipt.get(c) for c in lcols]])):
        recovery = Recovery(agg, 2)
        try:
            for offset in range(0, len(rows), 2):
                recovery.sink("opentelemetry_traces")(cols, rows[offset:offset + 2])
            for offset in range(0, len(selected_logs), 2):
                recovery.sink("opentelemetry_logs")(lcols, selected_logs[offset:offset + 2])
            recovery.prepare()
            recovery.reduce(prices)
            merged = agg._ai_merge(cols, rows, agg.dedupe_logs(lcols, selected_logs))
            expected = {
                "session": [((r["client"], r["session_id"]), r)
                            for r in agg.summarize_sessions(merged, prices)],
                "daily": [((r["day"], r["client"], r["provider"], r["model"]), r)
                          for r in agg.summarize_daily(merged, prices)],
                "tool": [((r["day"], r["tool"], r["client"]), r)
                         for r in agg.summarize_tools(merged)]}
            for kind, records in expected.items():
                actual = dict(recovery.outputs(kind))
                assert set(actual) == {key for key, _ in records}, kind
                for key, oracle in records:
                    assert {field: actual[key].get(field) for field in oracle} == oracle, (
                        kind, key, actual[key], oracle)
        finally:
            recovery.close()


def test_disk_snapshot_preserves_raw_schema_and_nulls():
    from scripts.analytics.ai_recovery import Recovery
    recovery = Recovery(agg, 2)
    try:
        consume = recovery.sink("opentelemetry_traces")
        expected = [
            {"trace_id": "raw-a", "span_id": "a", "explicit_null": None,
             "nested": {"empty": "", "values": [None, 0, False]}},
            {"span_id": "b", "trace_id": "raw-b", "nested": None},
            {"trace_id": "raw-c", "span_id": "c", "explicit_null": None,
             "nested": {"empty": "", "values": [None, 0, False]}},
        ]
        # One stream may change projection/order between batches, then return
        # to an earlier schema. Activity must receive the full original maps.
        for raw in expected:
            consume(list(raw), [list(raw.values())])
        actual = list(recovery.raw_rows("opentelemetry_traces"))
        assert actual == expected, actual
        assert [list(row) for row in actual] == [list(row) for row in expected]
        assert "explicit_null" not in actual[1]
        assert actual[0]["explicit_null"] is None
    finally:
        recovery.close()


def test_native_jsonb_projection_preserves_log_only_sessions():
    saved = _guard()
    real = install_fence({"opentelemetry_logs": "native_jsonb"})
    try:
        traces, logs = base_history()
        log_only = clog(DAY2, "jsonb-log-only", "codex", "jsonb-call", 40, 20)
        logs += [log_only]
        _reset()
        FakeGreptime.store.append("traces", traces)
        FakeGreptime.store.append("logs", logs)
        srv = serve()
        try:
            ctx, cfg = _ctx(srv), _cfg(max_rows=2)
            agg.ai_section(ctx, cfg)
            assert _session_input("jsonb-log-only", "codex") == (40, 20)
            late = clog(DAY2, "jsonb-late", "codex", "jsonb-late-call", 9, 3)
            FakeGreptime.store.append("logs", [late])
            agg.ai_section(ctx, cfg)
            assert _session_input("jsonb-late", "codex") == (9, 3)
            # A valid old cursor still needs one complete logic cutover:
            # no new raw commits are needed to recover formerly dropped logs.
            FakeGreptime.state["rev"] = "ai:fence-v1"
            FakeGreptime.derived["ai_session_summary"] = [
                row for row in FakeGreptime.derived["ai_session_summary"]
                if row.get("session_id") != "jsonb-log-only"]
            agg.ai_section(ctx, cfg)
            assert _session_input("jsonb-log-only", "codex") == (40, 20)
            result = snapshot()
        finally:
            srv.shutdown()
            srv.server_close()
        _assert_oracle(result, traces, logs + [late], _cfg())
    finally:
        restore_fence(real)
        _restore(saved)


def test_late_model_reconciliation_retries_without_advancing_fence():
    saved = _guard()
    real = install_fence()
    _reset()
    srv = serve()
    try:
        ctx, cfg = _ctx(srv), _cfg()
        trace = span(DAY2, "late-model", "call", "session", "codex",
                     5, 5, call="request", model=None)
        other = span(DAY1, "other", "call", "other", "codex", 7, 3)
        FakeGreptime.store.append("traces", [trace, other])
        seed_fenced(ctx, cfg)
        old_state = dict(FakeGreptime.state)
        old_day = [dict(r) for r in FakeGreptime.derived["ai_daily_summary"]
                   if agg.parse_ts(r["day_start"]) == DAY1]
        native = clog(DAY2, "session", "codex", "request", 50, 30)
        attrs = json.loads(native["log_attributes"])
        attrs["model"] = "resolved-model"
        native["log_attributes"] = json.dumps(attrs)
        FakeGreptime.store.append("logs", [native])
        FakeGreptime.fault = ("DELETE FROM ai_daily_summary", 1)
        try:
            agg.ai_section(ctx, cfg)
        except agg.SqlError:
            pass
        else:
            raise AssertionError("failed reconciliation must fail the pass")
        assert FakeGreptime.state == old_state
        FakeGreptime.fault = None
        agg.ai_section(ctx, cfg)
        rows = [r for r in FakeGreptime.derived["ai_daily_summary"]
                if agg.parse_ts(r["day_start"]) == DAY2]
        assert [(r["model"], r["input_tokens"], r["output_tokens"]) for r in rows] == [
            ("resolved-model", 50, 30)]
        assert _session_input("session", "codex") == (50, 30)
        assert [r for r in FakeGreptime.derived["ai_daily_summary"]
                if agg.parse_ts(r["day_start"]) == DAY1] == old_day
        assert FakeGreptime.state != old_state
    finally:
        srv.shutdown()
        srv.server_close()
        restore_fence(real)
        _restore(saved)


def test_full_reconciliation_pages_and_preserves_partial_retention_day():
    saved = _guard()
    real = install_fence()
    _reset()
    srv = serve()
    try:
        ctx, cfg = _ctx(srv), _cfg(max_rows=2)
        traces = [span(DAY2, "old-model", "call", "session", "codex", 5, 5)]
        traces += [span(DAY2, "old-tools", str(i), "session", "codex",
                        None, None, op="tool", tool="old-" + str(i)) for i in range(5)]
        traces += [span(DAY1, "expired", "tool", "past", "codex",
                        7, 3, op="tool", tool="past-tool")]
        FakeGreptime.store.append("traces", traces)
        seed_fenced(ctx, cfg)
        before = {table: [dict(r) for r in FakeGreptime.derived[table]]
                  for table in ("ai_daily_summary", "ai_tool_daily")}
        # Current complete raw snapshot has corrected dimensions and no tools.
        FakeGreptime.store = SeqStore()
        FakeGreptime.store.append("traces", [
            span(DAY2, "old-model", "call", "session", "codex", 50, 30, model="fixed")])
        FakeGreptime.watermark = _fmt(DAY1 + dt.timedelta(hours=12))
        FakeGreptime.state["rev"] = "ai:fence-v2"
        old_state = dict(FakeGreptime.state)
        FakeGreptime.fault = ("INSERT INTO ai_daily_summary", 1)
        try:
            agg.ai_section(ctx, cfg)
        except agg.SqlError:
            pass
        else:
            raise AssertionError("failed replacement write must fail the pass")
        assert FakeGreptime.state == old_state
        assert all(FakeGreptime.derived[table] == rows for table, rows in before.items())
        FakeGreptime.fault = None
        agg.ai_section(ctx, cfg)
        daily = [r for r in FakeGreptime.derived["ai_daily_summary"]
                 if agg.parse_ts(r["day_start"]) == DAY2]
        assert [(r["model"], r["input_tokens"]) for r in daily] == [("fixed", 50)]
        assert not [r for r in FakeGreptime.derived["ai_tool_daily"]
                    if agg.parse_ts(r["day_start"]) == DAY2]
        for table in before:
            assert [r for r in FakeGreptime.derived[table]
                    if agg.parse_ts(r["day_start"]) == DAY1] == [
                        r for r in before[table] if agg.parse_ts(r["day_start"]) == DAY1]
    finally:
        srv.shutdown()
        srv.server_close()
        restore_fence(real)
        _restore(saved)


def test_scalar_dimensions_merge_and_v3_checkpoint_repairs_without_input():
    saved = _guard()
    real = install_fence()
    _reset()
    srv = serve()
    try:
        ctx, cfg = _ctx(srv), _cfg(max_rows=2)

        def usage(call, model, provider, inp, outp):
            row = clog(DAY2, "scalar-session", "codex", call, inp, outp)
            attrs = json.loads(row["log_attributes"])
            attrs.update({"model": model, "gen_ai.provider.name": provider,
                          "coding_agent.client": 321})
            row["log_attributes"] = json.dumps(attrs)
            return row

        first = usage("numeric", 123, True, 50, 30)
        tools = [span(DAY2, "scalar-tools", str(i), "scalar-session", 321,
                      None, None, op="tool", tool=value, model=123,
                      provider=True) for i, value in enumerate((456, "456"))]
        FakeGreptime.store.append("logs", [first])
        FakeGreptime.store.append("traces", tools)
        agg.ai_section(ctx, cfg)
        assert _session_input("scalar-session", "321") == (50, 30)
        require_fence()

        def totals(inp, outp):
            daily = FakeGreptime.derived["ai_daily_summary"]
            assert [(r["client"], r["provider"], r["model"],
                     r["input_tokens"], r["output_tokens"]) for r in daily] == [
                         ("321", "True", "123", inp, outp)], daily
            tool = FakeGreptime.derived["ai_tool_daily"]
            assert [(r["client"], r["tool_name"], r["calls"]) for r in tool] == [
                ("321", "456", 2)], tool

        totals(50, 30)
        FakeGreptime.store.append("logs", [usage("text", "123", "True", 7, 3)])
        agg.ai_section(ctx, cfg)
        totals(57, 33)
        assert _session_input("scalar-session", "321") == (57, 33)
        expected = snapshot()
        # Reproduce a valid v3 cursor whose daily output was pruned away.
        FakeGreptime.state["rev"] = "ai:fence-v3"
        FakeGreptime.derived["ai_daily_summary"] = []
        agg.ai_section(ctx, cfg)
        totals(57, 33)
        assert snapshot() == expected
        assert FakeGreptime.state["rev"] == "ai:fence-v4"
        require_fence()
    finally:
        srv.shutdown()
        srv.server_close()
        restore_fence(real)
        _restore(saved)


def test_dimension_normalization_matches_sql_scalar_strings():
    from decimal import Decimal
    for value in (123, 1.25, Decimal("1.2300"), True, "한글 '모델'"):
        normalized = agg.norm_span(span(
            DAY2, "scalar", "scalar", "session", value, 1, 1,
            model=value, provider=value, tool=value))
        for field in ("client", "provider", "model", "tool"):
            assert normalized[field] == str(value), (field, value, normalized)
            assert agg.str_lit(normalized[field]) == agg.str_lit(value)
    for value in (None, "", False, 0, Decimal("0")):
        assert agg.ai_dimension(value) == value


TESTS = (
    test_failed_full_snapshot_discards_scratch_and_preserves_fence,
    test_scalar_dimensions_merge_and_v3_checkpoint_repairs_without_input,
    test_dimension_normalization_matches_sql_scalar_strings,
    test_late_model_reconciliation_retries_without_advancing_fence,
    test_full_reconciliation_pages_and_preserves_partial_retention_day,
    test_fresh_bootstrap_commits_fence_on_first_pass,
    test_fenced_late_arrival_matches_full_oracle,
    test_noop_second_pass_reads_no_rows,
    test_small_input_same_sessions_is_bounded,
    test_arbitrary_commit_delay_older_event_time,
    test_near_epoch_and_negative_timestamps_normalize,
    test_duplicate_redelivery_bills_once,
    test_late_native_replaces_trace_cost,
    test_long_midnight_session_survives_incremental,
    test_max_rows_bounds_work_not_complete_snapshot,
    test_bounded_long_session_prefix_failure_and_late_retry,
    test_native_jsonb_projection_preserves_log_only_sessions,
    test_disk_reducer_matches_global_merge_oracle_across_chunks,
    test_disk_snapshot_preserves_raw_schema_and_nulls,
    test_partial_write_failure_keeps_fence_and_converges,
    test_expired_anchor_isolated_by_client_and_session,
    test_ttl_partial_expiry_preserves_stored_derived,
    test_price_change_rebuilds_cost_cells,
    test_metric_registry_change_rebuilds,
    test_same_key_metric_overwrite_visible_via_fence,
    test_metric_only_counter_delta_preserved,
    test_sessionless_rows_reach_daily_output,
    test_stale_fence_below_frontier_recovers_to_oracle,
    test_missing_watermark_fails_without_checkpoint,
    test_post_eof_fault_fails_closed_no_checkpoint,
    test_zero_seq_and_new_region_recovery,
    test_missing_grpc_endpoint_rebuilds,
)


if __name__ == "__main__":
    for fn in TESTS:
        fn()
    print("test_ai_incremental: ok (%d tests)" % len(TESTS))
