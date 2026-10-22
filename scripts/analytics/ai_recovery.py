"""Disk-backed complete AI recovery; no retained-history Python collections."""
import io
import pickle
import sqlite3
import tempfile
from collections import OrderedDict


def packed(value):
    buffer = io.BytesIO()
    pickler = pickle.Pickler(buffer, protocol=5)
    # Value identities must not depend on object-alias memoization.
    pickler.fast = True
    pickler.dump(value)
    return buffer.getvalue()


class Recovery:
    def __init__(self, aggregate, limit):
        self.a = aggregate
        self.limit = limit
        self.directory = tempfile.TemporaryDirectory(prefix="ai-recovery-")
        self.db = sqlite3.connect(self.directory.name + "/snapshot.sqlite")
        self.db.execute("PRAGMA cache_size=-8192")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.executescript("""
            CREATE TABLE raw_schema (ordinal INTEGER PRIMARY KEY, columns BLOB UNIQUE);
            CREATE TABLE raw (kind TEXT, ordinal INTEGER PRIMARY KEY, schema INTEGER, payload BLOB);
            CREATE INDEX raw_kind ON raw(kind, ordinal);
            CREATE TABLE spans (ordinal INTEGER PRIMARY KEY, identity BLOB UNIQUE,
                payload BLOB, trace BLOB, span BLOB, parent BLOB, scope BLOB,
                call BLOB, turn BLOB, billed INTEGER, money INTEGER DEFAULT 1,
                tool INTEGER DEFAULT 0, phase INTEGER DEFAULT 0);
            CREATE INDEX span_parent ON spans(trace,parent);
            CREATE INDEX span_scope ON spans(scope);
            CREATE INDEX span_call ON spans(call);
            CREATE INDEX span_turn ON spans(turn);
            CREATE TABLE logs (ordinal INTEGER PRIMARY KEY, identity BLOB UNIQUE,
                payload BLOB, call BLOB);
            CREATE INDEX log_call ON logs(call);
            CREATE TABLE winners (kind TEXT, identity BLOB, ordinal INTEGER,
                PRIMARY KEY(kind,identity));
            CREATE TABLE output (kind TEXT, identity BLOB, payload BLOB,
                PRIMARY KEY(kind,identity));
        """)

    def close(self):
        self.db.close()
        self.directory.cleanup()

    def raw_rows(self, kind):
        schema, columns = None, None
        for schema_id, payload in self.db.execute(
                "SELECT schema,payload FROM raw WHERE kind=? ORDER BY ordinal", (kind,)):
            if schema_id != schema:
                columns = pickle.loads(self.db.execute(
                    "SELECT columns FROM raw_schema WHERE ordinal=?", (schema_id,)).fetchone()[0])
                schema = schema_id
            yield dict(zip(columns, pickle.loads(payload)))

    def sink(self, kind, instrument=None, meta=None):
        a = self.a
        schema, schema_id = None, None

        def append_raw(raw_kind, raw):
            nonlocal schema, schema_id
            columns = tuple(raw)
            if columns != schema:
                encoded = packed(columns)
                self.db.execute("INSERT OR IGNORE INTO raw_schema(columns) VALUES(?)", (encoded,))
                schema_id = self.db.execute(
                    "SELECT ordinal FROM raw_schema WHERE columns=?", (encoded,)).fetchone()[0]
                schema = columns
            # Store projected names once, without dropping explicit nulls or
            # conflating a missing key with a present key whose value is None.
            return self.db.execute("INSERT INTO raw(kind,schema,payload) VALUES(?,?,?)",
                                   (raw_kind, schema_id, packed(tuple(raw.values())))).lastrowid

        def consume(cols, rows):
            rows = a._ai_flight_ns_rows(kind, cols, rows)
            for values in rows:
                raw = dict(zip(cols, values))
                if instrument is not None:
                    raw = a._ai_norm_metric_row(raw, instrument, meta)
                    if raw is None:
                        continue
                    append_raw("metric", raw)
                else:
                    ordinal = append_raw(kind, raw)
                    if kind == "opentelemetry_traces":
                        identity = (raw.get("trace_id"), raw.get("span_id"))
                        if identity == (None, None):
                            identity = (None, None, ordinal)
                        self.add_span(a.norm_span(raw), packed(identity))
                    else:
                        rec = a.norm_log(raw)
                        if rec is None:
                            continue
                        attrs = a.attrs_of(raw)
                        if rec.get("turn_total") and rec.get("turn") is not None:
                            identity = (rec["native"], rec["client"], rec["session"], rec["turn"])
                        elif rec["call_id"]:
                            identity = (rec["native"], rec["client"], rec["session"], rec["call_id"])
                        else:
                            identity = (rec["client"], attrs.get("event.name") or rec["native"],
                                        rec["ts"], rec["session"], rec["seq"])
                        call = self.call(rec) if rec.get("native") not in ("tool", "codex_cost") else None
                        self.db.execute("INSERT OR IGNORE INTO logs(identity,payload,call) VALUES(?,?,?)",
                                        (packed(identity), packed(rec), call))
            self.db.commit()
        return consume

    @staticmethod
    def call(row):
        return packed((row.get("client"), row.get("session"), row["call_id"])) if row.get("call_id") else None

    def add_span(self, row, identity=None):
        self.db.execute("""INSERT OR IGNORE INTO spans
            (identity,payload,trace,span,parent,scope,call,turn,billed)
            VALUES(?,?,?,?,?,?,?,?,?)""", (
                identity, packed(row), packed(row.get("trace")), packed(row.get("span")),
                packed(row.get("parent")), packed((row.get("client"), row.get("session"))),
                self.call(row), packed((row.get("client"), row.get("session"), row.get("turn"))),
                int(bool(row.get("has_usage") and not self.a.is_rollup_op(row.get("op"))))))

    def rows(self, sql="SELECT ordinal,payload FROM spans ORDER BY phase,ordinal", args=()):
        for ordinal, payload in self.db.execute(sql, args):
            yield ordinal, pickle.loads(payload)

    def parent_filter(self):
        a = self.a
        # Parent decisions use all original children, not recursively filtered children.
        for ordinal, row in self.rows("SELECT ordinal,payload FROM spans WHERE billed=1 ORDER BY ordinal"):
            if row.get("trace") is None or row.get("input") is None or row.get("output") is None:
                continue
            count, inp, out, complete = 0, 0, 0, True
            for _, child in self.rows("SELECT ordinal,payload FROM spans WHERE trace=? AND parent=? ORDER BY ordinal",
                                      (packed(row["trace"]), packed(row["span"]))):
                if not child.get("parent") or not child["has_usage"] or a.is_rollup_op(child["op"]):
                    continue
                count += 1
                if child["input"] is None or child["output"] is None:
                    complete = False
                else:
                    inp += child["input"]
                    out += child["output"]
            if count and complete and row["input"] == inp and row["output"] == out:
                self.db.execute("UPDATE spans SET billed=0 WHERE ordinal=?", (ordinal,))

    def prepare(self):
        a = self.a
        self.parent_filter()
        # billable() before native merge also applies native-sourced span precedence.
        self.native_filter()
        self.db.execute("UPDATE spans SET phase=1 WHERE billed=1")
        for ordinal, row in self.rows("SELECT ordinal,payload FROM spans WHERE billed=1 ORDER BY ordinal"):
            call = self.call(row)
            if call is None:
                continue
            hit = self.db.execute("SELECT payload FROM logs WHERE call=? ORDER BY ordinal LIMIT 1", (call,)).fetchone()
            if hit is None:
                continue
            first = self.db.execute("INSERT OR IGNORE INTO winners VALUES('merge',?,?)", (call, ordinal)).rowcount
            if not first:
                self.db.execute("DELETE FROM spans WHERE ordinal=?", (ordinal,))
                continue
            merged = a._override_span(row, pickle.loads(hit[0]))
            self.db.execute("UPDATE spans SET payload=?,scope=?,call=?,turn=? WHERE ordinal=?",
                            (packed(merged), packed((merged.get("client"), merged.get("session"))),
                             self.call(merged), packed((merged.get("client"), merged.get("session"),
                                                      merged.get("turn"))), ordinal))
        for _, payload, call in self.db.execute("SELECT ordinal,payload,call FROM logs ORDER BY ordinal"):
            if call is not None and self.db.execute(
                    "SELECT 1 FROM winners WHERE kind='merge' AND identity=?", (call,)).fetchone():
                continue
            log = pickle.loads(payload)
            self.add_span(a._native_row(log))
            self.db.execute("UPDATE spans SET phase=? WHERE ordinal=last_insert_rowid()",
                            (3 if log.get("native") == "tool" else 2,))
        # Re-evaluate the post-merge billing rules exactly as the summary oracle does.
        self.db.execute("UPDATE spans SET billed=0")
        for ordinal, row in self.rows():
            if row.get("has_usage") and not a.is_rollup_op(row.get("op")):
                self.db.execute("UPDATE spans SET billed=1 WHERE ordinal=?", (ordinal,))
        self.parent_filter()
        self.native_filter()
        for ordinal, row in self.rows("SELECT ordinal,payload FROM spans WHERE billed=1 ORDER BY phase,ordinal"):
            if row.get("turn_total") and all((row.get("client"), row.get("session"), row.get("turn"))):
                self.db.execute("INSERT OR IGNORE INTO winners VALUES('turn',?,?)", (packed((row["client"], row["session"], row["turn"])), ordinal))
        self.db.execute("""UPDATE spans SET money=0 WHERE billed=1 AND EXISTS
            (SELECT 1 FROM winners w WHERE w.kind='turn' AND w.identity=spans.turn AND w.ordinal!=spans.ordinal)""")
        for ordinal, row in self.rows():
            if not row.get("tool"):
                continue
            identity = packed((row.get("client"), row.get("session"), row["call_id"])) if row.get("call_id") else packed(("anonymous", ordinal))
            hit = self.db.execute("SELECT ordinal FROM winners WHERE kind='tool' AND identity=?", (identity,)).fetchone()
            if hit is None:
                self.db.execute("INSERT INTO winners VALUES('tool',?,?)", (identity, ordinal))
                self.db.execute("UPDATE spans SET tool=1 WHERE ordinal=?", (ordinal,))
            else:
                first = self.db.execute("SELECT payload FROM spans WHERE ordinal=?", hit).fetchone()
                kept = pickle.loads(first[0])
                for field in ("tool", "session", "client", "provider", "model", "duration_ms"):
                    if kept.get(field) is None and row.get(field) is not None:
                        kept[field] = row[field]
                kept["error"] = bool(kept.get("error") or row.get("error"))
                self.db.execute("UPDATE spans SET payload=? WHERE ordinal=?", (packed(kept), hit[0]))
        self.db.commit()

    def native_filter(self):
        for _, row in self.rows("SELECT ordinal,payload FROM spans WHERE billed=1 ORDER BY ordinal"):
            if row.get("client") and row.get("session") and row.get("billed_source") == "native" and (row.get("input") is not None or row.get("output") is not None):
                self.db.execute("INSERT OR IGNORE INTO winners VALUES('native',?,0)", (packed((row["client"], row["session"])),))
        for ordinal, row in self.rows("SELECT ordinal,payload FROM spans WHERE billed=1 ORDER BY ordinal"):
            if row.get("billed_source") != "native" and self.db.execute(
                    "SELECT 1 FROM winners WHERE kind='native' AND identity=?", (packed((row.get("client"), row.get("session"))),)).fetchone():
                self.db.execute("UPDATE spans SET billed=0 WHERE ordinal=?", (ordinal,))
        self.db.execute("DELETE FROM winners WHERE kind='native'")

    def save(self, kind, key, value):
        self.db.execute("INSERT INTO output VALUES(?,?,?) ON CONFLICT(kind,identity) DO UPDATE SET payload=excluded.payload", (kind, packed(key), packed(value)))

    def outputs(self, kind):
        for identity, payload in self.db.execute("SELECT identity,payload FROM output WHERE kind=? ORDER BY rowid", (kind,)):
            yield pickle.loads(identity), pickle.loads(payload)

    def reduce(self, prices):
        a = self.a
        self.db.execute("CREATE TABLE distincts(kind TEXT, identity BLOB, value BLOB, PRIMARY KEY(kind,identity,value))")
        additive = ("input", "output", "cache_read", "cache_write", "reasoning", "total", "cost", "llm_spans", "tool_calls", "errors", "cost_estimated_usd", "cost_unpriced_calls")
        # Fixed-width accumulators only; distinct models/sessions stay in SQLite.
        pending = OrderedDict()

        def bump(kind, key, row, billed, money, tool):
            identity = packed(key)
            cache_key = (kind, identity)
            cached = pending.pop(cache_key, None)
            if cached is not None:
                _, state = cached
            else:
                hit = self.db.execute("SELECT payload FROM output WHERE kind=? AND identity=?", (kind, identity)).fetchone()
                if hit is None:
                    state = {field: None for field in additive}
                    if prices is not None:
                        state["cost_unpriced_calls"] = 0
                    state.update({"provider": None, "session_start": None, "session_end": None})
                    self.save(kind, key, state)
                else:
                    state = pickle.loads(hit[0])
            start = a.parse_ts(row["ts"])
            end = a.parse_ts(row["end"] or row["ts"])
            if start is not None:
                state["session_start"] = min(state["session_start"], start) if state["session_start"] is not None else start
            if end is not None:
                state["session_end"] = max(state["session_end"], end) if state["session_end"] is not None else end
            if state["provider"] is None and row["provider"]:
                state["provider"] = row["provider"]
            def add(field, value):
                if value is not None:
                    state[field] = (state[field] or 0) + value
            if billed:
                for field in ("input", "output", "cache_read", "cache_write", "reasoning"):
                    add(field, row[field])
                if row["input"] is not None or row["output"] is not None:
                    add("total", (row["input"] or 0) + (row["output"] or 0))
                    add("llm_spans", 1)
                if money:
                    cost, source = a.resolve_cost(row)
                    add("cost", cost)
                    if source:
                        self.db.execute("INSERT OR IGNORE INTO distincts VALUES(?,?,?)", (kind + "cost", identity, packed(source)))
                    est, unpriced = a._supplemental([row], {id(row)}, prices)
                    add("cost_estimated_usd", est)
                    add("cost_unpriced_calls", unpriced)
            if tool:
                add("tool_calls", 1)
            if row["error"] and (not row.get("tool") or tool):
                add("errors", 1)
            if row.get("model"):
                self.db.execute("INSERT OR IGNORE INTO distincts VALUES(?,?,?)", (kind + "model", identity, packed(row["model"])))
            if row.get("session"):
                self.db.execute("INSERT OR IGNORE INTO distincts VALUES(?,?,?)", (kind + "session", identity, packed(row["session"])))
            pending[cache_key] = (key, state)
            if len(pending) > 128:
                (old_kind, _), (old_key, old_state) = pending.popitem(last=False)
                self.save(old_kind, old_key, old_state)

        for ordinal, payload, billed, money, tool in self.db.execute(
                "SELECT ordinal,payload,billed,money,tool FROM spans ORDER BY phase,ordinal"):
            row = pickle.loads(payload)
            day = a._ai_day_of(row)
            if row.get("session"):
                bump("session", (row["client"] or "unknown", row["session"]), row, billed, money, tool)
            if day is not None:
                bump("daily", (day, row["client"] or "unknown", row["provider"] or "unknown", row["model"] or "unknown"), row, billed, money, tool)
                if tool:
                    key = (day, row["tool"], row["client"] or "unknown")
                    hit = self.db.execute("SELECT payload FROM output WHERE kind='tool' AND identity=?", (packed(key),)).fetchone()
                    state = pickle.loads(hit[0]) if hit else {"day": day, "tool": key[1], "client": key[2], "calls": 0, "errors": None, "duration": 0, "durations": 0}
                    state["calls"] += 1
                    if row["error"]:
                        state["errors"] = (state["errors"] or 0) + 1
                    if row["duration_ms"] is not None:
                        state["duration"] += row["duration_ms"]
                        state["durations"] += 1
                    self.save("tool", key, state)
        for (kind, _), (key, state) in pending.items():
            self.save(kind, key, state)
        for kind in ("session", "daily"):
            for key, state in self.outputs(kind):
                identity = packed(key)
                sources = [pickle.loads(r[0]) for r in self.db.execute("SELECT value FROM distincts WHERE kind=? AND identity=? LIMIT 2", (kind + "cost", identity))]
                state["cost_source"] = a.MIXED if len(sources) > 1 else sources[0] if sources else None
                if kind == "session":
                    state.update(client=key[0], session_id=key[1])
                    models = sorted(pickle.loads(r[0]) for r in self.db.execute("SELECT value FROM distincts WHERE kind=? AND identity=?", (kind + "model", identity)))
                    state["models"] = models or None
                    state["model_count"] = len(models) or None
                    if state["session_start"] is not None and state["session_end"] is not None:
                        state["duration_s"] = (state["session_end"] - state["session_start"]).total_seconds()
                else:
                    state.update(day=key[0], client=key[1], provider=key[2], model=key[3])
                    state["sessions"] = self.db.execute("SELECT COUNT(*) FROM distincts WHERE kind=? AND identity=?", (kind + "session", identity)).fetchone()[0] or None
                self.save(kind, key, state)
        for key, state in self.outputs("tool"):
            state["avg_ms"] = state["duration"] / state["durations"] if state["durations"] else None
            self.save("tool", key, state)
        self.db.commit()


def recover(a, ctx, cfg, metric_registry, price_digest, prices):
    from scripts.telemetry.activity import summarize_activity_spooled
    recovery = Recovery(a, cfg["max_rows"])
    try:
        flight = a._ai_flight()
        if flight is None or not (cfg.get("grpc_url") or "").strip():
            raise a._ai_no_flight()
        upper = {}
        metas = a._ai_metric_metas(ctx)
        sources = [(table, None) for table in a.AI_FENCE_TABLES] + list(a.json.loads(metric_registry))
        for table, instrument in sources:
            sql = a._ai_table_sql(table) if instrument is None else 'SELECT * FROM "' + table.replace('"', '""') + '"'
            _, _, marks = flight.flight_query(
                sql, endpoint=cfg["grpc_url"], db=ctx[2],
                user=cfg.get("user") or "", password=cfg.get("password") or "",
                lower=None, max_rows=cfg["max_rows"],
                row_sink=recovery.sink(table, instrument, (metas or {}).get(table)))
            upper[table] = dict(marks)
        recovery.prepare()
        recovery.reduce(prices)
        activity_sessions, activity_days = summarize_activity_spooled(
            recovery.raw_rows("opentelemetry_traces"),
            recovery.raw_rows("opentelemetry_logs"),
            recovery.raw_rows("metric"), recovery.db)
        for key, activity in activity_sessions:
            hit = recovery.db.execute("SELECT payload FROM output WHERE kind='session' AND identity=?", (packed(key),)).fetchone()
            session = pickle.loads(hit[0]) if hit else {"client": key[0], "session_id": key[1]}
            starts = [v for v in (session.get("session_start"), activity.get("start")) if v is not None]
            ends = [v for v in (session.get("session_end"), activity.get("end")) if v is not None]
            if starts:
                session["session_start"] = min(starts)
            if ends:
                session["session_end"] = max(ends)
            if starts and ends:
                session["duration_s"] = (max(ends) - min(starts)).total_seconds()
            session.update({field: activity.get(field) for field in a.ACTIVITY_FIELDS})
            session.update({field: activity.get(field) for field in ("repo", "branch", "outcome", "activity_sources")})
            recovery.save("session", key, session)
        for day in activity_days:
            recovery.save("activity", (day["day_start"], day["client"]), day)
        now = a.utcnow()
        bound = a.retention_boundary(ctx, "otel", cfg.get("otel_ttl", ""), now)
        total = 0
        batch_limit = min(500, cfg["max_rows"])
        for kind in ("session", "activity", "daily", "tool"):
            batch = []
            for key, value in recovery.outputs(kind):
                if kind == "session" and value.get("session_start") is None:
                    continue
                batch.append((key, value))
                if len(batch) == batch_limit:
                    total += write_batch(a, ctx, cfg, prices, bound, now, kind, batch)
                    batch = []
            if batch:
                total += write_batch(a, ctx, cfg, prices, bound, now, kind, batch)
        a._ai_commit_state(ctx, a._ai_encode_fence(upper), metric_registry, price_digest)
        return total
    finally:
        recovery.close()


def write_batch(a, ctx, cfg, prices, bound, now, kind, batch):
    values = [value for _, value in batch]
    return a._ai_write(ctx, cfg, dict(batch) if kind == "session" else {},
                       values if kind == "activity" else [], [], prices, bound, now,
                       computed=(values if kind == "daily" else [],
                                 values if kind == "tool" else []))
