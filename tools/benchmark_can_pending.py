#!/usr/bin/env python3
"""Pending-frontier benchmark: bounded heads vs full candidate cost.

Seeds one archive per matrix cell with a single bulk transaction (same rows,
event IDs, cursors, pending membership with durable head_id, and counters a
real accept/decode run would leave), then measures the single maintained
frontier query at limit 1/1000 plus the unbounded reference on identical data.
The legacy baseline side (optional --baseline-root) keeps its own original
full-scan candidate in an isolated subprocess importing only baseline code.

Matrix: completed {1000,10000,100000} x pending {1000,10000,100000}.
Detection, lock contention, and equivalence run on both sides with the same
protocol.

Run: python tools/benchmark_can_pending.py \
       --matrix --baseline-root /path/to/pristine-checkout --repeats 20
Single cell: ... tools/benchmark_can_pending.py --sessions 1000 --pending 1000
Without --baseline-root every side is labeled current-only (no honest baseline).
"""
import argparse
import json
import shutil
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.ingest.can.can_receiver import Archive, candidate_sql  # noqa: E402

SCHEMA = "can-pending-benchmark/3"
MATRIX_SESSIONS = (1000, 10000, 100000)
MATRIX_PENDING = (1000, 10000, 100000)
EPOCHS_PER_BODY = {"current": 2, "legacy": 1}
FRAME = b"t12320200\r"
META_BASE = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
             "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}
# Full legacy candidate exactly as shipped in the baseline tree's decode_once
FULL_BASELINE_CANDIDATE = ("WITH progress(session,next_seq) AS (VALUES (NULL,NULL)) "
    "SELECT c.session AS session, c.seq AS seq, c.offset_ns AS offset_ns, "
    "c.phase AS phase, c.data AS data, s.meta_json AS meta_json, d.state_json AS state_json, "
    "d.counts_json AS counts_json, d.rows AS rows FROM sessions s "
    "LEFT JOIN decode_states d ON d.session=s.id AND d.epoch=? "
    "LEFT JOIN progress p ON p.session=s.id "
    "CROSS JOIN raw_chunks c ON c.session=s.id AND c.seq=COALESCE(p.next_seq,d.next_seq,0) "
    "ORDER BY c.id LIMIT 1")


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _stats(walls):
    return {"n": len(walls), "mean_s": sum(walls) / len(walls),
            "min_s": min(walls), "max_s": max(walls)}


def _candidate_sql(side, limit=None):
    if side == "current":
        return candidate_sql(limit)
    return FULL_BASELINE_CANDIDATE


def _candidate_source(side):
    return "candidate_sql" if side == "current" else "inline-legacy-baseline"


def _candidate_params(side, epoch, limit=None):
    if side == "current":
        return (epoch,)
    return (epoch,) * EPOCHS_PER_BODY[side]


def build_archive(directory, sessions, pending):
    """Bulk-seed exactly what a real accept/decode/flush run would leave.

    One prototype decode_some call proves the state/counts/row shape; per-done
    session rows copy it with only identity fields substituted (no hash feeds
    on identity except event IDs, which live in flushed-out rows and are
    re-proven by the equivalence decode). Done sessions look decoded+ACKed
    (outbox empty, acked counter set): the candidate query never touches the
    outbox, so poll cost is identical with none of the 100k-commit cost.
    Everything lands in a single transaction: one fsync, not 100k.
    """
    from scripts.ingest.can.can_decoder import Decoder  # noqa: E402
    dbc = Path(directory) / "synthetic.dbc"
    defs = Path(directory) / "synthetic.json"
    dbc.write_text('VERSION "synthetic"\n\nNS_ :\n\nBS_:\n\nBU_: Fixture\n\n'
                   'BO_ 291 Sample: 2 Fixture\n SG_ Power : 0|16@1- (0.5,0) '
                   '[-16384|16383.5] "kW" Fixture\n', encoding="utf-8")
    defs.write_text(json.dumps({"revision": "synthetic-v1", "signals": [{
        "source": "synthetic", "id": "0x123", "signal": "Power",
        "source_signal": "Power", "kind": "data", "start_bit": 0,
        "bit_length": 16, "byte_order": "little_endian", "signed": True,
        "scale": 0.5, "offset": 0, "is_multiplexer": False,
        "multiplexer_signal": None, "multiplexer_ids": None,
        "actual_dbc_length": 2, "unit": "kW", "source_unit": "kW",
        "choices": {}, "evidence": "synthetic@0123456789abcdef"}]}), encoding="utf-8")
    decoder = Decoder(dbc, defs)
    archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
    archive.register_epoch(decoder)
    epoch = decoder.epoch
    proto_meta = dict(META_BASE, session_id="done-0")
    chunk = {"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}
    rows, state, counts, done = decoder.decode_some(proto_meta, chunk, None, 2000)
    assert done and rows, "synthetic fixture must decode to >=1 row in one batch"
    total = sessions + pending
    now = time.time_ns()
    with archive.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "INSERT INTO sessions(id,vehicle,collector_id,session_id,meta_json,next_seq,last_offset_ns)"
            " VALUES(?,?,?,?,?,?,?)",
            [(i + 1, META_BASE["vehicle"], META_BASE["collector_id"],
              ("done-%d" % i) if i < sessions else ("pending-%d" % (i - sessions)),
              _json(dict(META_BASE, session_id=("done-%d" % i) if i < sessions
                         else ("pending-%d" % (i - sessions)))), 1, 0)
             for i in range(total)])
        conn.executemany(
            "INSERT INTO raw_chunks(session,seq,offset_ns,phase,data) VALUES(?,?,?,?,?)",
            [(i + 1, 0, 0, "capture", FRAME) for i in range(total)])
        for i in range(sessions):
            own = dict(state)
            own.update(vehicle=META_BASE["vehicle"], collector_id=META_BASE["collector_id"],
                       session_id="done-%d" % i, started_ns=META_BASE["started_ns"])
            conn.execute("INSERT INTO decode_states VALUES(?,?,?,?,?,?)",
                         (i + 1, epoch, 1, _json(own), _json(counts), len(rows)))
        conn.executemany("INSERT INTO decode_pending(session,epoch,head_id) VALUES(?,?,?)",
                         [(sessions + 1 + j, epoch, sessions + 1 + j) for j in range(pending)])
        counters = {"raw_chunks": total, "raw_bytes": len(FRAME) * total, "outbox_rows": 0,
                    "decoded_rows_total": len(rows) * sessions,
                    "acked_rows_total": len(rows) * sessions, "pending_migrated": 1,
                    "pending_heads_v1": 1,
                    "last_receive_ns": now, "last_decode_ns": now, "last_full_ack_ns": now}
        conn.executemany("INSERT OR REPLACE INTO archive_meta(key,value) VALUES(?,?)",
                         list(counters.items()))
        conn.commit()
    with archive.connect() as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    return archive, decoder, epoch


def _open_ro(path):
    conn = sqlite3.connect("file:%s?mode=ro" % path, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def frontier_probe(path, epoch, limit):
    with _open_ro(path) as conn:
        rows = conn.execute(candidate_sql(limit), (epoch,)).fetchall()
        heads = len(rows)
        first = None
        if rows:
            name = conn.execute("SELECT session_id FROM sessions WHERE id=?",
                                (rows[0]["session"],)).fetchone()
            first = {"session_id": name["session_id"], "seq": rows[0]["seq"]}
    return {"heads_fetched": heads, "first": first}


def frontier_vm_steps(path, epoch, limit):
    steps = {"n": 0}

    def counter():
        steps["n"] += 1
        return False
    with _open_ro(path) as conn:
        conn.set_progress_handler(counter, 1)
        conn.execute(candidate_sql(limit), (epoch,)).fetchall()
    return steps["n"]


def candidate_probe(path, epoch, side):
    with _open_ro(path) as conn:
        row = conn.execute(_candidate_sql(side), _candidate_params(side, epoch)).fetchone()
        if row is None:
            return None
        name = conn.execute("SELECT session_id FROM sessions WHERE id=?", (row["session"],)).fetchone()
    return {"session_id": name["session_id"], "seq": row["seq"]}

def probe_timing(path, epoch, side, repeats):
    walls, cpus = [], []
    for _ in range(repeats):
        wall0, cpu0 = time.monotonic(), time.process_time()
        candidate_probe(path, epoch, side)
        walls.append(time.monotonic() - wall0)
        cpus.append(time.process_time() - cpu0)
    return walls, cpus


def candidate_vm_steps(path, epoch, side, limit=None):
    steps = {"n": 0}

    def counter():
        steps["n"] += 1
        return False
    with _open_ro(path) as conn:
        conn.set_progress_handler(counter, 1)
        conn.execute(_candidate_sql(side, limit), _candidate_params(side, epoch, limit)).fetchall()
    return steps["n"]


def batch_decode_details(archive, decoder, limit):
    """One bounded decode_once: chunks committed, wall/CPU, peak Python bytes."""
    import tracemalloc
    tracemalloc.start()
    wall0, cpu0 = time.monotonic(), time.process_time()
    progressed = archive.decode_once(decoder, limit=limit)
    wall, cpu = time.monotonic() - wall0, time.process_time() - cpu0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"limit": limit, "chunks_committed": progressed,
            "wall_s": wall, "cpu_s": cpu, "peak_python_bytes": peak}


def delayed_batch_details(archive, decoder, limit, delay_s):
    """Same batch with a fixed per-SQL-call delay: proves the read/CPU split."""
    import contextlib as _contextlib
    import sqlite3 as _sqlite3
    import unittest.mock as _mock

    class _DelayedConnection(_sqlite3.Connection):
        def execute(self, sql, *params):
            time.sleep(delay_s)
            return super().execute(sql, *params)

    def delayed_connect():
        conn = _sqlite3.connect(archive.path, timeout=5, factory=_DelayedConnection)
        try:
            conn.row_factory = _sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
        except _sqlite3.Error:
            conn.close()
            raise
        return _contextlib.closing(conn)

    with _mock.patch.object(archive, "connect", delayed_connect):
        return batch_decode_details(archive, decoder, limit)


def idle_timing(archive, decoder, repeats):
    walls, cpus = [], []
    for _ in range(repeats):
        wall0, cpu0 = time.monotonic(), time.process_time()
        progressed = archive.decode_once(decoder, limit=1)
        assert not progressed
        walls.append(time.monotonic() - wall0)
        cpus.append(time.process_time() - cpu0)
    return walls, cpus


def detection_rounds(archive, decoder, epoch, repeats, prefix):
    """Accept one fresh session per round, then time one decode round-trip.

    Oldest-first ordering means a non-empty tail is serviced before the new
    chunk, so new_chunk_first is only guaranteed at pending 0; the flag says
    what actually happened each round. Prefix namespaces writers per side so
    baseline/current never collide on session identity.
    """
    latencies, first = [], []
    for k in range(repeats):
        sid = "%s-%d" % (prefix, k)
        meta = dict(META_BASE, session_id=sid)
        chunk = {"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}
        t0 = time.monotonic()
        archive.accept(meta, [chunk])
        progressed = archive.decode_once(decoder, limit=1)
        latencies.append(time.monotonic() - t0)
        with archive.connect() as conn:
            cursor = conn.execute(
                "SELECT next_seq FROM decode_states WHERE session="
                "(SELECT id FROM sessions WHERE session_id=?) AND epoch=?", (sid, epoch)).fetchone()
        first.append(bool(progressed) and cursor is not None and cursor["next_seq"] == 1)
    return latencies, first


def contended_accept(archive, meta, hold_s):
    import threading
    ready, release = threading.Event(), threading.Event()
    holder = sqlite3.connect(archive.path, timeout=5, isolation_level=None,
                             check_same_thread=False)
    try:
        def hold():
            holder.execute("BEGIN IMMEDIATE")
            ready.set()
            time.sleep(hold_s)
            holder.execute("COMMIT")
        thread = threading.Thread(target=hold)
        thread.start()
        assert ready.wait(5)
        t0 = time.monotonic()
        archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
        wall = time.monotonic() - t0
        release.set()
        thread.join()
        return wall
    finally:
        holder.close()


def lock_probe(archive, repeats, hold_s, prefix):
    """Real writer contention: uncontended accept vs accept while another
    connection holds the writer lock (RESERVED via BEGIN IMMEDIATE)."""
    free, held = [], []
    for k in range(repeats):
        t0 = time.monotonic()
        archive.accept(dict(META_BASE, session_id="%s-u-%d" % (prefix, k)),
                       [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
        free.append(time.monotonic() - t0)
    for k in range(repeats):
        held.append(contended_accept(archive, dict(META_BASE, session_id="%s-c-%d" % (prefix, k)), hold_s))
    delay = sum(held) / len(held) - sum(free) / len(free)
    return {"uncontended_s": _stats(free), "contended_s": _stats(held),
            "contention_delay_s": delay}


def _snapshot(path):
    with _open_ro(path) as conn:
        meta = dict(conn.execute("SELECT key,value FROM archive_meta").fetchall())
        top = conn.execute("SELECT COALESCE(MAX(id),0) FROM outbox").fetchone()[0]
    return meta, top


def decode_one_details(archive, decoder, epoch):
    """Decode exactly one chunk on a scratch copy; return deterministic proof."""
    selected = candidate_probe(str(archive.path), epoch, "current")
    meta_before, top_before = _snapshot(str(archive.path))
    progressed = archive.decode_once(decoder, limit=1)
    assert progressed == 1, "equivalence copy must have a pending chunk"
    with archive.connect() as conn:
        events = [r[0] for r in conn.execute(
            "SELECT event_id FROM outbox WHERE id>? ORDER BY id", (top_before,))]
        row = conn.execute(
            "SELECT next_seq,state_json,counts_json,rows FROM decode_states WHERE session="
            "(SELECT id FROM sessions WHERE session_id=?) AND epoch=?",
            (selected["session_id"], epoch)).fetchone()
        meta_after = dict(conn.execute("SELECT key,value FROM archive_meta").fetchall())
    return {"selected": selected, "event_ids": events,
            "state_json": row["state_json"], "counts_json": row["counts_json"], "rows": row["rows"],
            "decoded_delta": meta_after.get("decoded_rows_total", 0) - meta_before.get("decoded_rows_total", 0),
            "outbox_delta": meta_after.get("outbox_rows", 0) - meta_before.get("outbox_rows", 0)}


_BASELINE_CHILD = r"""
import json, sqlite3, sys, time
db, epoch, action, payload = sys.argv[1], sys.argv[2], sys.argv[3], json.loads(sys.argv[4])
sys.path.insert(0, payload["baseline_root"])
FRAME = b"t12320200\r"
META = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
        "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}
def ro():
    conn = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    conn.row_factory = sqlite3.Row
    return conn
def selected(conn):
    row = conn.execute(payload["sql"], (epoch,) * payload["nepoch"]).fetchone()
    if row is None:
        return None
    name = conn.execute("SELECT session_id FROM sessions WHERE id=?", (row[0],)).fetchone()
    return {"session_id": name[0], "seq": row[1]}
if action == "probe":
    walls, cpus = [], []
    for _ in range(payload["repeats"]):
        w0, c0 = time.monotonic(), time.process_time()
        with ro() as conn:
            row = conn.execute(payload["sql"], (epoch,) * payload["nepoch"]).fetchone()
            if row is not None:
                conn.execute("SELECT session_id FROM sessions WHERE id=?", (row[0],)).fetchone()
        walls.append(time.monotonic() - w0)
        cpus.append(time.process_time() - c0)
    print(json.dumps({"walls": walls, "cpus": cpus}))
elif action == "selected":
    with ro() as conn:
        print(json.dumps({"selected": selected(conn)}))
elif action == "plan":
    with ro() as conn:
        print(json.dumps({"plan": [list(r) for r in conn.execute(
            "EXPLAIN QUERY PLAN " + payload["sql"], (epoch,) * payload["nepoch"])]}))
elif action == "steps":
    n = {"n": 0}
    def counter():
        n["n"] += 1
        return False
    with ro() as conn:
        conn.set_progress_handler(counter, 1)
        conn.execute(payload["sql"], (epoch,) * payload["nepoch"]).fetchall()
    print(json.dumps({"vm_steps": n["n"]}))
elif action in ("idle", "detect", "lock", "decode1"):
    from scripts.ingest.can.can_receiver import Archive
    from scripts.ingest.can.can_decoder import Decoder
    archive = Archive(db, disk_reserve_bytes=0)
    decoder = Decoder(payload["dbc"], payload["defs"])
    if action == "idle":
        walls, cpus = [], []
        for _ in range(payload["repeats"]):
            w0, c0 = time.monotonic(), time.process_time()
            assert not archive.decode_once(decoder, limit=1)
            walls.append(time.monotonic() - w0)
            cpus.append(time.process_time() - c0)
        print(json.dumps({"walls": walls, "cpus": cpus}))
    elif action == "detect":
        lat, first = [], []
        for k in range(payload["secondary"]):
            sid = "detect-%d" % k
            t0 = time.monotonic()
            archive.accept(dict(META, session_id=sid),
                           [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            progressed = archive.decode_once(decoder, limit=1)
            lat.append(time.monotonic() - t0)
            with archive.connect() as conn:
                cur = conn.execute("SELECT next_seq FROM decode_states WHERE session="
                                   "(SELECT id FROM sessions WHERE session_id=?) AND epoch=?",
                                   (sid, decoder.epoch)).fetchone()
            first.append(bool(progressed) and cur is not None and cur["next_seq"] == 1)
        print(json.dumps({"latencies": lat, "new_chunk_first": first}))
    elif action == "lock":
        import threading
        free, held = [], []
        for k in range(payload["secondary"]):
            t0 = time.monotonic()
            archive.accept(dict(META, session_id="lock-u-%d" % k),
                           [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            free.append(time.monotonic() - t0)
        for k in range(payload["secondary"]):
            ready, release = threading.Event(), threading.Event()
            holder = sqlite3.connect(db, timeout=5, isolation_level=None,
                                     check_same_thread=False)
            def hold():
                holder.execute("BEGIN IMMEDIATE")
                ready.set()
                time.sleep(payload["hold_s"])
                holder.execute("COMMIT")
            th = threading.Thread(target=hold)
            th.start()
            assert ready.wait(5)
            t0 = time.monotonic()
            archive.accept(dict(META, session_id="lock-c-%d" % k),
                           [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            held.append(time.monotonic() - t0)
            release.set()
            th.join()
            holder.close()
        print(json.dumps({"free": free, "held": held}))
    elif action == "decode1":
        with ro() as conn:
            sel = selected(conn)
            before = dict(conn.execute("SELECT key,value FROM archive_meta").fetchall())
            top = conn.execute("SELECT COALESCE(MAX(id),0) FROM outbox").fetchone()[0]
        assert archive.decode_once(decoder, limit=1) == 1
        with archive.connect() as conn:
            events = [r[0] for r in conn.execute(
                "SELECT event_id FROM outbox WHERE id>? ORDER BY id", (top,))]
            st = conn.execute("SELECT next_seq,state_json,counts_json,rows FROM decode_states WHERE session="
                              "(SELECT id FROM sessions WHERE session_id=?) AND epoch=?",
                              (sel["session_id"], epoch)).fetchone()
            after = dict(conn.execute("SELECT key,value FROM archive_meta").fetchall())
        print(json.dumps({"selected": sel, "event_ids": events, "state_json": st["state_json"],
                          "counts_json": st["counts_json"], "rows": st["rows"],
                          "decoded_delta": after.get("decoded_rows_total", 0) - before.get("decoded_rows_total", 0),
                          "outbox_delta": after.get("outbox_rows", 0) - before.get("outbox_rows", 0)}))
    elif action == "batch":
        import tracemalloc
        tracemalloc.start()
        w0, c0 = time.monotonic(), time.process_time()
        progressed = archive.decode_once(decoder, limit=payload["bl_limit"])
        wall, cpu = time.monotonic() - w0, time.process_time() - c0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(json.dumps({"limit": payload["bl_limit"], "chunks_committed": progressed,
                          "wall_s": wall, "cpu_s": cpu, "peak_python_bytes": peak}))
"""

def baseline_call(baseline_root, db, epoch, action, payload):
    proc = subprocess.run(
        [sys.executable, "-c", _BASELINE_CHILD, str(db), epoch, action, json.dumps(payload)],
        capture_output=True, text=True, cwd=baseline_root,
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "PYTHONPATH": baseline_root,
             "PYTHONNOUSERSITE": "1", "VIRTUAL_ENV": str(Path(sys.executable).parents[1])},
        timeout=600)
    if proc.returncode != 0:
        raise RuntimeError("baseline %s failed: %s" % (action, proc.stderr[-2000:]))
    return json.loads(proc.stdout)


def measure_cell(directory, sessions, pending, repeats, secondary, baseline_root, hold_s):
    archive, decoder, epoch = build_archive(directory, sessions, pending)
    db = str(archive.path)
    # Scratch copies before any mutation: equivalence pair plus a baseline
    # working copy so detect/lock writers never change the other side's scale.
    eq_current = str(Path(directory) / "eq_current.sqlite")
    eq_baseline = str(Path(directory) / "eq_baseline.sqlite")
    shutil.copy2(db, eq_current)
    if baseline_root:
        shutil.copy2(db, eq_baseline)
        bl_db = str(Path(directory) / "baseline_work.sqlite")
        shutil.copy2(db, bl_db)
    current, baseline = {"side": "current"}, {"side": "baseline" if baseline_root else "current-only"}
    current.update(candidate_query_source=_candidate_source("current"),
                   candidate_plan=candidate_plan(db, epoch, "current"),
                   candidate_vm_steps_full=candidate_vm_steps(db, epoch, "current"),
                   candidate_vm_steps_limit_1=candidate_vm_steps(db, epoch, "current", 1),
                   candidate_vm_steps_limit_1000=candidate_vm_steps(db, epoch, "current", 1000),
                   candidate_selected=candidate_probe(db, epoch, "current"),
                   frontier_limit_1=frontier_probe(db, epoch, 1),
                   frontier_limit_1_vm_steps=frontier_vm_steps(db, epoch, 1),
                   frontier_limit_1000=frontier_probe(db, epoch, 1000),
                   frontier_limit_1000_vm_steps=frontier_vm_steps(db, epoch, 1000))
    if pending == 0:
        walls, cpus = idle_timing(archive, decoder, repeats)
        current.update(poll_kind="idle_decode_once", poll_wall_s=_stats(walls), poll_cpu_s=_stats(cpus))
    else:
        walls, cpus = probe_timing(db, epoch, "current", repeats)
        current.update(poll_kind="candidate_probe_no_drain", poll_wall_s=_stats(walls),
                       poll_cpu_s=_stats(cpus))
        batch_copy = str(Path(directory) / "batch_current.sqlite")
        delayed_copy = str(Path(directory) / "batch_delayed.sqlite")
        shutil.copy2(db, batch_copy)
        shutil.copy2(db, delayed_copy)
        current.update(batch_limit_1=batch_decode_details(Archive(batch_copy, disk_reserve_bytes=0), decoder, 1),
                       batch_limit_default=batch_decode_details(Archive(batch_copy, disk_reserve_bytes=0), decoder, 1000),
                       batch_delayed_limit_default=delayed_batch_details(
                           Archive(delayed_copy, disk_reserve_bytes=0), decoder, 1000, 0.001))
    if baseline_root:
        sql, nepoch = FULL_BASELINE_CANDIDATE, EPOCHS_PER_BODY["legacy"]
        dbc = str(Path(directory) / "synthetic.dbc")
        defs = str(Path(directory) / "synthetic.json")
        base_payload = {"baseline_root": baseline_root, "sql": sql, "nepoch": nepoch,
                        "repeats": repeats, "secondary": secondary, "dbc": dbc,
                        "defs": defs, "hold_s": hold_s}
        baseline.update(candidate_query_source="inline-legacy-baseline",
                        candidate_plan=baseline_call(baseline_root, eq_baseline, epoch, "plan", base_payload)["plan"],
                        candidate_vm_steps=baseline_call(baseline_root, eq_baseline, epoch, "steps", base_payload)["vm_steps"],
                        candidate_selected=baseline_call(
                            baseline_root, eq_baseline, epoch, "selected", base_payload)["selected"])
        if pending == 0:
            out = baseline_call(baseline_root, bl_db, epoch, "idle", base_payload)
            baseline.update(poll_kind="idle_decode_once",
                            poll_wall_s=_stats(out["walls"]), poll_cpu_s=_stats(out["cpus"]))
        else:
            out = baseline_call(baseline_root, eq_baseline, epoch, "probe", base_payload)
            baseline.update(poll_kind="candidate_probe_no_drain",
                            poll_wall_s=_stats(out["walls"]), poll_cpu_s=_stats(out["cpus"]))
            for bl_limit, key in ((1, "batch_limit_1"), (1000, "batch_limit_default")):
                bl_batch = str(Path(directory) / ("batch_baseline_%d.sqlite" % bl_limit))
                shutil.copy2(db, bl_batch)
                baseline[key] = baseline_call(
                    baseline_root, bl_batch, epoch, "batch",
                    dict(base_payload, bl_limit=bl_limit))
    equivalence = None
    if pending:
        eq_archive = Archive(eq_current, disk_reserve_bytes=0)
        cur_details = decode_one_details(eq_archive, decoder, epoch)
        if baseline_root:
            bl_details = baseline_call(
                baseline_root, eq_baseline, epoch, "decode1",
                {"baseline_root": baseline_root, "sql": FULL_BASELINE_CANDIDATE,
                 "nepoch": EPOCHS_PER_BODY["legacy"], "repeats": 1, "secondary": 1,
                 "dbc": str(Path(directory) / "synthetic.dbc"),
                 "defs": str(Path(directory) / "synthetic.json"), "hold_s": hold_s})
            equivalence = {
                "match": all(cur_details[k] == bl_details[k] for k in
                             ("selected", "event_ids", "state_json", "counts_json", "rows",
                              "decoded_delta", "outbox_delta")),
                "current": cur_details, "baseline": bl_details}
        else:
            equivalence = {"match": None, "current": cur_details, "baseline": None}
    else:
        equivalence = {"match": current["candidate_selected"] == (baseline.get("candidate_selected")
                                                                  if baseline_root else None),
                       "current": {"selected": current["candidate_selected"]},
                       "baseline": {"selected": baseline.get("candidate_selected")}}
    return {"sessions": sessions, "pending": pending, "repeats": repeats,
            "secondary_repeats": secondary, "current": current, "baseline": baseline,
            "equivalence": equivalence}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=1000)
    parser.add_argument("--pending", type=int, default=5)
    parser.add_argument("--matrix", action="store_true",
                        help="run the full completed x pending matrix in one process")
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--secondary-repeats", type=int, default=5)
    parser.add_argument("--baseline-root", default=None,
                        help="immutable baseline tree; baseline sides run only its code")
    parser.add_argument("--lock-hold-s", type=float, default=0.2)
    args = parser.parse_args(argv)
    combos = ([(s, p) for s in MATRIX_SESSIONS for p in MATRIX_PENDING] if args.matrix
              else [(args.sessions, args.pending)])
    cells = []
    with tempfile.TemporaryDirectory() as workspace:
        for sessions, pending in combos:
            cell_dir = str(Path(workspace) / ("cell-%d-%d" % (sessions, pending)))
            os.makedirs(cell_dir, mode=0o700)
            cells.append(measure_cell(cell_dir, sessions, pending, args.repeats,
                                      args.secondary_repeats, args.baseline_root, args.lock_hold_s))
    print(json.dumps({"schema": SCHEMA, "baseline_root": args.baseline_root,
                      "seed": "single-bulk-transaction", "cells": cells}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
