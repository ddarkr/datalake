"""Regression tests for vehicle VSS setup + recorder helpers (stdlib only).

Run: python3 -m tests.test_vss
Covers: duplicate CAN-ID rejection, override replace semantics, mapping
signal validation, outbox value classification, INSERT escaping,
store column-order roundtrip, fail-closed Greptime ack (HTTP200 errors,
output errors, partial/missing affected rows keep rows; retry reuses the
same event_id/event_time), source-time exactness/fallback/int64 range,
restart and seen-prune bounds, batched snapshot/update commits (one commit
per received batch, late last-marking, commit-failure and pre-commit exit
rollback, full-content baseline parity), size/rate benchmark grid, uploader
catch-up tick, idle-subscribe unblock helper, env validation, manifest
top-level pins, and metrics reflecting outbox state.
"""

import calendar
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import urllib.request
from datetime import datetime, timezone
from unittest import mock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


from scripts.vehicle import vehicle_setup as setup
from scripts.vehicle import vss_recorder as rec

DBC_A = """VERSION ""

NS_ :
BS_ :
BU_ :

BO_ 100 MsgA: 8 Vector__XXX
 SG_ SigA : 0|8@1+ (1,0) [0|255] "" Vector__XXX
 SG_ Shared : 8|8@1+ (1,0) [0|255] "" Vector__XXX

BO_ 200 MsgB: 8 Vector__XXX
 SG_ SigB : 0|8@1+ (1,0) [0|255] "" Vector__XXX
"""

DBC_B = """VERSION ""

NS_ :
BS_ :
BU_ :

BO_ 200 MsgB2: 8 Vector__XXX
 SG_ SigB2 : 0|8@1+ (1,0) [0|255] "" Vector__XXX

BO_ 300 MsgC: 8 Vector__XXX
 SG_ SigC : 0|8@1+ (1,0) [0|255] "" Vector__XXX
"""



def vss_tree(signals):
    tree = {"Vehicle": {}}
    for path, sig, kind in signals:
        node = tree["Vehicle"]
        parts = path.split(".")[1:]
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        leaf = node.setdefault(parts[-1], {"datatype": "float", "type": "sensor"})
        leaf[kind] = {"signal": sig}
    return tree


class SetupTest(unittest.TestCase):
    def test_duplicate_can_id_detected(self):
        pa, _ = setup.parse_dbc(DBC_A)
        pb, _ = setup.parse_dbc(DBC_B)
        self.assertEqual(sorted(set(pa) & set(pb)), [200])

    def test_mapping_refs_found(self):
        tree = vss_tree([("Vehicle.Speed", "SigA", "dbc2vss"),
                         ("Vehicle.Body.Mirror", "SigB", "dbc2vss")])
        refs = setup.find_mappings(tree["Vehicle"], "Vehicle")
        got = {(p, s) for p, s, k in refs if k == "dbc2vss"}
        self.assertEqual(got, {("Vehicle.Speed", "SigA"),
                               ("Vehicle.Body.Mirror", "SigB")})

    def test_unknown_signal_detectable(self):
        _, sigs = setup.parse_dbc(DBC_A)
        self.assertNotIn("Nope", sigs)


class RecorderTest(unittest.TestCase):
    def test_classify(self):
        self.assertEqual(rec.classify(True), (None, None, 1))
        self.assertEqual(rec.classify(False), (None, None, 0))
        self.assertEqual(rec.classify(3), (3.0, None, None))
        self.assertEqual(rec.classify("P"), (None, "P", None))
        self.assertIsNone(rec.classify(None))
        with self.assertRaises(rec.UnsupportedValue):
            rec.classify([1, 2])  # arrays unsupported, never stored
        with self.assertRaises(rec.UnsupportedValue):
            rec.classify({"a": 1})

    def test_insert_escapes_quotes(self):
        row = {c: None for c in rec.COLUMNS}
        row.update(event_time=1, vehicle="v", path="Vehicle.X", source="can",
                   event_id="e1", decode_epoch="3", value_text="o'clock")
        sql = rec.render_insert("vehicle_signal", [row])
        self.assertIn("o''clock", sql)
        self.assertNotIn("o'clock", sql.replace("o''clock", ""))

    def test_insert_bool_literal(self):
        row = {c: None for c in rec.COLUMNS}
        row.update(event_time=1, vehicle="v", path="p", source="can",
                   event_id="e1", decode_epoch="1", value_bool=1)
        self.assertIn("TRUE", rec.render_insert("vehicle_signal", [row]))
        row["value_bool"] = 0
        self.assertIn("FALSE", rec.render_insert("vehicle_signal", [row]))

    def test_outbox_ack_only_delete(self):
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            base = {c: None for c in rec.COLUMNS}
            base.update(vehicle="v", path="Vehicle.Speed", source="can",
                        decode_epoch="1", value_num=10.0, ingest_time=5)
            r1 = dict(base, event_time=100, event_id="aaa")
            r2 = dict(base, event_time=101, event_id="bbb")
            rec.store(conn, r1)
            rec.store(conn, r2)
            # simulate ack of first row only -> delete exactly that event_id
            with conn:
                conn.executemany("DELETE FROM outbox WHERE event_id=?", [("aaa",)])
            left = conn.execute("SELECT event_id,event_time FROM outbox").fetchall()
            # retry keeps the same event_id/event_time: nothing minted, nothing lost
            self.assertEqual(left, [("bbb", 101)])
            conn.close()

    def test_open_outbox_creates_both_tables(self):
        # open_outbox once executed SEEN_DDL but dropped DDL: fresh
        # volumes had no outbox table and the first store crashed.
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertIn("outbox", tables)
            self.assertIn("seen_ids", tables)
            rec.store(conn, rec.make_row(
                "v", "Vehicle.Speed", 100, "fresh-1", _meta(), {}, 1.0,
                None, None, 1))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 1)
            conn.close()

    def test_store_column_order_roundtrip(self):
        # store() once wrote COLUMNS-order values under a different column
        # list, silently landing event_id in event_time etc. Chain check:
        # make_row -> store -> SELECT COLUMNS must return the same mapping,
        # and rendering the stored row must equal rendering the input row.
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            meta = _meta()
            row = rec.make_row("veh1", "Vehicle.Speed", 1234567890123456789,
                               "eid-chain-1", meta, {"Vehicle.Speed": "km/h"},
                               12.5, None, None, 999)
            rec.store(conn, row)
            got = conn.execute(
                "SELECT " + ",".join(rec.COLUMNS) + " FROM outbox").fetchone()
            back = dict(zip(rec.COLUMNS, got))
            self.assertEqual(back, row)
            self.assertEqual(rec.render_insert("vehicle_signal", [back]),
                             rec.render_insert("vehicle_signal", [row]))
            conn.close()

    def test_upload_full_ack_deletes_only_acked_batch(self):
        # limit boundary: 3 rows, limit=2, affected=2 -> newest row stays.
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            meta = _meta()
            for i, eid in enumerate(("e1", "e2", "e3")):
                rec.store(conn, rec.make_row(
                    "v", "Vehicle.Speed", 100 + i, eid, meta, {}, float(i),
                    None, None, 1))
            with mock.patch.object(rec, "greptime_insert", return_value=2):
                self.assertEqual(
                    rec.upload_once(conn, "vehicle_signal", "http://x",
                                    "db", "u", "p", limit=2), 2)
            left = conn.execute(
                "SELECT event_id FROM outbox").fetchall()
            self.assertEqual(left, [("e3",)])
            conn.close()

    def test_upload_top_code_error_keeps_rows(self):
        # HTTP 200 with Greptime code != 0 must not delete anything.
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            rec.store(conn, rec.make_row(
                "v", "Vehicle.Speed", 100, "keep-1", _meta(), {}, 1.0,
                None, None, 1))
            bad = _FakeHTTP(json.dumps({"code": 400, "error": "bad sql",
                                        "output": []}))
            with mock.patch.object(urllib.request, "urlopen",
                                   return_value=bad):
                with self.assertRaises(IOError):
                    rec.upload_once(conn, "vehicle_signal", "http://x",
                                    "db", "u", "p")
            left = conn.execute(
                "SELECT event_id,event_time FROM outbox").fetchall()
            self.assertEqual(left, [("keep-1", 100)])
            conn.close()

    def test_upload_output_error_keeps_rows(self):
        body = json.dumps({"code": 0,
                           "output": [{"affectedrows": 0,
                                       "error": "partial failure"}]})
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            rec.store(conn, rec.make_row(
                "v", "Vehicle.Speed", 100, "keep-2", _meta(), {}, 1.0,
                None, None, 1))
            with mock.patch.object(urllib.request, "urlopen",
                                   return_value=_FakeHTTP(body)):
                with self.assertRaises(IOError):
                    rec.upload_once(conn, "vehicle_signal", "http://x",
                                    "db", "u", "p")
            n = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
            self.assertEqual(n, 1)
            conn.close()

    def test_upload_partial_and_missing_affected_keep_rows(self):
        # affected != batch size, or no affected-rows field at all: keep all.
        for body in (
                json.dumps({"code": 0, "output": [{"affectedrows": 1}]}),
                json.dumps({"code": 0, "output": [{"rows": 2}]}),
                json.dumps({"code": 0})):
            with tempfile.TemporaryDirectory() as d:
                conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
                meta = _meta()
                rec.store(conn, rec.make_row(
                    "v", "Vehicle.Speed", 100, "p-a", meta, {}, 1.0,
                    None, None, 1))
                rec.store(conn, rec.make_row(
                    "v", "Vehicle.Speed", 101, "p-b", meta, {}, 2.0,
                    None, None, 1))
                with mock.patch.object(urllib.request, "urlopen",
                                       return_value=_FakeHTTP(body)):
                    with self.assertRaises(IOError):
                        rec.upload_once(conn, "vehicle_signal", "http://x",
                                        "db", "u", "p")
                left = conn.execute(
                    "SELECT event_id FROM outbox ORDER BY event_time"
                    ).fetchall()
                self.assertEqual(left, [("p-a",), ("p-b",)])
                conn.close()

    def test_upload_retry_reuses_same_event_id(self):
        # failed attempt keeps the row; the retry sends the identical
        # event_id/event_time (no duplicate minted server-side).
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            rec.store(conn, rec.make_row(
                "v", "Vehicle.Speed", 4242, "retry-1", _meta(), {}, 9.0,
                None, None, 1))
            seen = []

            def fake(base_url, db, user, password, sql, timeout=15):
                seen.append(sql)
                if len(seen) == 1:
                    raise IOError("timeout")
                return 1

            with mock.patch.object(rec, "greptime_insert", side_effect=fake):
                with self.assertRaises(IOError):
                    rec.upload_once(conn, "vehicle_signal", "http://x",
                                    "db", "u", "p")
                self.assertEqual(
                    rec.upload_once(conn, "vehicle_signal", "http://x",
                                    "db", "u", "p"), 1)
            self.assertIn("retry-1", seen[1])
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
            conn.close()

    def test_dt_to_ns_exactness_and_fallback(self):
        aware = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
        want = (calendar.timegm((2026, 1, 2, 3, 4, 5)) * 1_000_000_000
                + 123456 * 1000)
        self.assertEqual(rec.dt_to_ns(aware), want)
        # naive datetimes are UTC, not local time.
        self.assertEqual(rec.dt_to_ns(datetime(2026, 1, 2, 3, 4, 5, 123456)),
                         want)
        # epoch boundary is exactly zero, not float residue.
        self.assertEqual(rec.dt_to_ns(datetime(1970, 1, 1,
                                               tzinfo=timezone.utc)), 0)
        # microsecond precision survives (float ts*1e9 would smear this).
        one_us = datetime(2030, 6, 1, 12, 0, 0, 1, tzinfo=timezone.utc)
        self.assertEqual(rec.dt_to_ns(one_us) % 1000, 0)
        self.assertEqual(rec.dt_to_ns(one_us),
                         calendar.timegm((2030, 6, 1, 12, 0, 0))
                         * 1_000_000_000 + 1000)
        # fallback policy: None / non-datetime -> collector receive time.
        for bad in (None, "x", 123):
            before, got, after = rec.now_ns(), rec.dt_to_ns(bad), rec.now_ns()
            self.assertTrue(before <= got <= after)

    def test_classify_int_precision_and_nan_inf(self):
        self.assertEqual(rec.classify(2 ** 53), (float(2 ** 53), None, None))
        # beyond float64-exact range: preserved as exact text, never silently
        # rounded through float.
        num, text, boolean = rec.classify(2 ** 53 + 1)
        self.assertIsNone(num)
        self.assertEqual(text, str(2 ** 53 + 1))
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(rec.UnsupportedValue):
                rec.classify(bad)
        with self.assertRaises(rec.UnsupportedValue):
            rec.classify(b"raw-bytes")

    def test_deterministic_id_dedupe_and_same_value_new_time(self):
        # Same logical sample (same vehicle/path/time/epoch/value) maps to
        # one id: redelivery and post-ack restart never duplicate. Same
        # value at a different timestamp is a different sample and stores.
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            meta, units = _meta(), {}
            last = {}
            ets = 100
            eid = rec.deterministic_event_id("v", "Vehicle.Speed", ets,
                                             meta["decode_epoch"], 5.0,
                                             None, None)
            self.assertEqual(eid, rec.deterministic_event_id(
                "v", "Vehicle.Speed", ets, meta["decode_epoch"], 5.0,
                None, None))
            row = rec.make_row("v", "Vehicle.Speed", ets, eid, meta, units,
                               5.0, None, None, 1)
            self.assertTrue(rec.store_update(conn, last, "Vehicle.Speed",
                                             row))
            # redelivery of the identical sample: no duplicate row.
            redelivered = rec.make_row("v", "Vehicle.Speed", ets, eid,
                                       meta, units, 5.0, None, None, 2)
            self.assertFalse(rec.store_update(conn, {}, "Vehicle.Speed",
                                              redelivered))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 1)
            # ack-delete the row, then restart (fresh memory): the same
            # broker snapshot still dedupes via persistent seen_ids.
            with conn:
                conn.execute("DELETE FROM outbox WHERE event_id=?", (eid,))
            self.assertFalse(rec.store_update(conn, {}, "Vehicle.Speed",
                                              redelivered))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
            # same value, different source time -> distinct sample, stores.
            ets2 = 101
            eid2 = rec.deterministic_event_id("v", "Vehicle.Speed", ets2,
                                              meta["decode_epoch"], 5.0,
                                              None, None)
            self.assertNotEqual(eid, eid2)
            row2 = rec.make_row("v", "Vehicle.Speed", ets2, eid2, meta,
                                units, 5.0, None, None, 3)
            self.assertTrue(rec.store_update(conn, {}, "Vehicle.Speed",
                                             row2))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 1)
            conn.close()

    def test_seen_prune_is_time_bounded(self):
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            conn.execute("INSERT INTO seen_ids(event_id,seen_at)"
                         " VALUES(?,?)", ("old-id", 100))
            conn.execute("INSERT INTO seen_ids(event_id,seen_at)"
                         " VALUES(?,?)", ("new-id", 9999))
            conn.commit()
            self.assertEqual(rec.prune_seen(conn, 1000), 1)
            left = conn.execute(
                "SELECT event_id FROM seen_ids").fetchall()
            self.assertEqual(left, [("new-id",)])
            conn.close()

    def test_snapshot_batch_is_one_commit_with_late_cache(self):
        # One already-received snapshot batch commits once; last marks only
        # after commit, and rollback leaves last untouched for retry.
        # sqlite3.Connection.commit is read-only C: count COMMIT via the
        # supported trace callback, never by patching the method.
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            meta, last = _meta(), {}
            items = [(f"Vehicle.Speed.{i}", rec.make_row(
                "v", f"Vehicle.Speed.{i}", 100 + i, f"batch-{i}", meta, {},
                float(i), None, None, 1)) for i in range(4)]
            commits = [0]

            def trace(stmt):
                if stmt.strip().upper() == "COMMIT":
                    commits[0] += 1
            conn.set_trace_callback(trace)
            try:
                self.assertEqual(rec.store_updates(conn, last, items), 4)
            finally:
                conn.set_trace_callback(None)
            self.assertEqual(commits[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 4)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM seen_ids").fetchone()[0], 4)
            for i in range(4):
                self.assertEqual(last[f"Vehicle.Speed.{i}"], f"batch-{i}")
            # Redelivered snapshot stores nothing: full content identical,
            # last still marked, no new durable write.
            before = conn.execute(
                "SELECT " + ",".join(rec.COLUMNS) + " FROM outbox"
                " ORDER BY event_id").fetchall()
            conn.set_trace_callback(trace)
            try:
                self.assertEqual(rec.store_updates(conn, {}, items), 0)
            finally:
                conn.set_trace_callback(None)
            self.assertEqual(commits[0], 1)
            self.assertEqual(conn.execute(
                "SELECT " + ",".join(rec.COLUMNS) + " FROM outbox"
                " ORDER BY event_id").fetchall(), before)
            conn.close()

    def test_batch_rollback_leaves_cache_and_tables_clean(self):
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            meta, last = _meta(), {}
            items = [(f"Vehicle.Speed.{i}", rec.make_row(
                "v", f"Vehicle.Speed.{i}", 100 + i, f"rb-{i}", meta, {},
                float(i), None, None, 1)) for i in range(4)]
            real_insert = rec._insert_update
            calls = [0]

            def flaky(c, l, path, row):
                calls[0] += 1
                if calls[0] == 3:
                    raise sqlite3.OperationalError("synthetic stage failure")
                return real_insert(c, l, path, row)
            with mock.patch.object(rec, "_insert_update", side_effect=flaky):
                with self.assertRaises(sqlite3.OperationalError):
                    rec.store_updates(conn, last, items)
            self.assertEqual(last, {})
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM seen_ids").fetchone()[0], 0)
            self.assertEqual(rec.store_updates(conn, last, items), 4)
            self.assertEqual(len(last), 4)
            conn.close()

    def test_full_content_parity_with_baseline(self):
        # Same fixture through baseline (per-row commits) and batched
        # paths: all outbox/seen fields and the upload SQL must match,
        # not just row counts.
        base = _load_baseline_recorder(_baseline_root())
        if base is None:
            self.skipTest("baseline checkout absent")
        with tempfile.TemporaryDirectory() as d:
            conns, digests = [], []
            try:
                for mode, path in (("baseline", "base.sqlite"),
                                   ("batched", "batch.sqlite")):
                    mod = base if mode == "baseline" else rec
                    conn = mod.open_outbox(os.path.join(d, path))
                    conns.append(conn)
                    meta = _meta()
                    items = _bench_items(mod, meta, 60, "parity")
                    if mode == "baseline":
                        stored = sum(mod.store_update(conn, {}, p, r)
                                     for p, r in items)
                    else:
                        stored = mod.store_updates(conn, {}, items)
                    self.assertEqual(stored, 60)
                    digests.append((_content_digest(mod, conn),
                                    [dict(zip(mod.COLUMNS, t))
                                     for t in conn.execute(
                                        "SELECT " + ",".join(mod.COLUMNS) +
                                        " FROM outbox ORDER BY event_id")]))
                self.assertEqual(digests[0][0], digests[1][0])
                self.assertEqual(
                    base.render_insert("vehicle_signal", digests[0][1]),
                    rec.render_insert("vehicle_signal", digests[1][1]))
            finally:
                for conn in conns:
                    conn.close()

    def test_exit_before_commit_stores_nothing_on_restart(self):
        # Pre-commit boundary the old suite missed: a child killed before
        # commit must leave an empty outbox (no marks, no partial rows).
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "o.sqlite")
            child = (
                "import os, sys; sys.path.insert(0, %r);"
                "from scripts.vehicle import vss_recorder as rec;"
                "conn = rec.open_outbox(%r);"
                "meta = {'vehicle_firmware': 'fw1', 'decode_epoch': 'e7',"
                " 'vss_version': '4.1', 'dbc_primary_commit': 'c-primary',"
                " 'dbc_supplemental_commit': 'c-supp',"
                " 'mapping_revision': 'mrev',"
                " 'collector_version': 'vss-recorder-1'};"
                "items = [(f'Vehicle.Speed.{i}', rec.make_row("
                " 'v', f'Vehicle.Speed.{i}', 100 + i, f'pre-{i}',"
                " meta, {}, float(i), None, None, 1)) for i in range(3)];"
                "staged = [rec._insert_update(conn, {}, p, r)"
                " for p, r in items];"
                "assert all(s for s, _ in staged);"
                "os._exit(0)" % (REPO, db_path))
            import subprocess
            proc = subprocess.run([sys.executable, "-c", child], timeout=60)
            self.assertEqual(proc.returncode, 0)
            conn = rec.open_outbox(db_path)
            try:
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM seen_ids").fetchone()[0], 0)
            finally:
                conn.close()

    def test_commit_failure_rolls_back_cache_seen_and_outbox(self):
        # Real commit wiring: a fail-once Connection subclass through the
        # connect factory proves the batch leaves last/seen/outbox
        # untouched and retries cleanly.
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "o.sqlite")
            real_connect = rec.sqlite3.connect

            class FailOnceCommit(rec.sqlite3.Connection):
                armed = False
                fails_left = 1

                def commit(self):
                    if type(self).armed and type(self).fails_left:
                        type(self).fails_left -= 1
                        raise sqlite3.OperationalError(
                            "synthetic commit failure")
                    return super().commit()

            def factory(*args, **kwargs):
                kwargs.pop("factory", None)
                return real_connect(*args, factory=FailOnceCommit, **kwargs)
            meta, last = _meta(), {}
            items = [(f"Vehicle.Speed.{i}", rec.make_row(
                "v", f"Vehicle.Speed.{i}", 100 + i, f"cf-{i}", meta, {},
                float(i), None, None, 1)) for i in range(3)]
            with mock.patch.object(rec.sqlite3, "connect",
                                   side_effect=factory):
                conn = rec.open_outbox(db_path)
            FailOnceCommit.armed = True
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    rec.store_updates(conn, last, items)
                self.assertEqual(last, {})
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM seen_ids").fetchone()[0], 0)
                self.assertEqual(rec.store_updates(conn, last, items), 3)
                self.assertEqual(len(last), 3)
            finally:
                conn.close()


    def test_upload_tick_catches_up_full_batches(self):
        # 100 paths @ 1 Hz with 500 rows/10 s: a single tick must drain
        # multiple full batches, not one batch per 10 s.
        with tempfile.TemporaryDirectory() as d:
            conn = rec.open_outbox(os.path.join(d, "o.sqlite"))
            meta = _meta()
            for i in range(5):
                rec.store(conn, rec.make_row(
                    "v", "Vehicle.Speed", 100 + i, f"tick-{i}", meta, {},
                    float(i), None, None, 1))
            stop = _Stop()
            with mock.patch.object(rec, "greptime_insert", side_effect=[2, 2, 1]):
                # batch=2: 2 + 2 + 1 across one tick.
                self.assertEqual(rec.upload_tick(
                    conn, "vehicle_signal", "http://x", "db", "u", "p",
                    2, stop), 5)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
            # first failure ends the tick; rows stay for the next tick.
            for i in range(3):
                rec.store(conn, rec.make_row(
                    "v", "Vehicle.Speed", 200 + i, f"fail-{i}", meta, {},
                    float(i), None, None, 1))
            calls = []

            def flaky(base_url, db, user, password, sql, timeout=15):
                calls.append(sql)
                if len(calls) == 1:
                    return 2
                raise IOError("boom")

            with mock.patch.object(rec, "greptime_insert", side_effect=flaky):
                with self.assertRaises(IOError):
                    rec.upload_tick(conn, "vehicle_signal", "http://x",
                                    "db", "u", "p", 2, stop)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 1)
            # stop set mid-tick ends the catch-up loop without failure.
            stop_now = _StopOn(allowed=1)
            with mock.patch.object(rec, "greptime_insert", return_value=1):
                self.assertEqual(rec.upload_tick(
                    conn, "vehicle_signal", "http://x", "db", "u", "p",
                    1, stop_now), 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
            conn.close()

    def test_unblock_clients_disconnects_idle_subscribe(self):
        class FakeClient:
            def __init__(self, fail=False):
                self.calls = 0
                self.fail = fail

            def disconnect(self):
                self.calls += 1
                if self.fail:
                    raise RuntimeError("close failed")

        good, bad = FakeClient(), FakeClient(fail=True)
        self.assertEqual(rec.unblock_clients([good, bad]), 1)
        self.assertEqual((good.calls, bad.calls), (1, 1))

    def test_env_validation_rejects_non_positive(self):
        for name, fn, bad in (("VSS_BATCH_N", rec._positive_int, "0"),
                              ("VSS_BATCH_N", rec._positive_int, "-3"),
                              ("VSS_BATCH_N", rec._positive_int, "abc"),
                              ("VSS_FLUSH_SEC", rec._positive_float, "0"),
                              ("VSS_FLUSH_SEC", rec._positive_float, "nan"),
                              ("DATABROKER_PORT", rec._positive_int, "")):
            if name == "DATABROKER_PORT" and bad == "":
                continue  # empty means default, covered below
            with self.assertRaises(SystemExit):
                fn(name, bad, 1)
        self.assertEqual(rec._positive_int("VSS_BATCH_N", "", 500), 500)
        self.assertEqual(rec._positive_int("DATABROKER_PORT", "55555",
                                           55555), 55555)
        self.assertEqual(rec._positive_float("VSS_FLUSH_SEC", "2.5", 10.0),
                         2.5)

    def test_dt_out_of_int64_range_falls_back(self):
        # year 9999 (datetime max) is ~2.5e23 ns, far beyond int64:
        # fallback to receive time, never a corrupt overflowed stamp.
        far = datetime(9999, 1, 1, tzinfo=timezone.utc)
        before, got, after = rec.now_ns(), rec.dt_to_ns(far), rec.now_ns()
        self.assertTrue(before <= got <= after)
        self.assertLessEqual(got, rec._INT64_MAX)

    def test_manifest_reads_top_level_pins_only(self):
        # artifacts[].sha256 is content identity; it must never appear in a
        # commit column even when top-level pins are absent ("" then).
        with tempfile.TemporaryDirectory() as d:
            mp = os.path.join(d, "manifest.json")
            with open(mp, "w") as f:
                json.dump({
                    "decode_epoch": "e9", "vehicle_firmware": "fw",
                    "vss_version": "4.0", "dbc_primary_commit": "commit-A",
                    "dbc_supplemental_commit": "",
                    "mapping_revision": "maprev",
                    "artifacts": [
                        {"role": "primary", "sha256": "sha-P"},
                        {"role": "mapping", "sha256": "sha-M"}]}, f)
            got = rec.load_manifest(mp)
            self.assertEqual(got["dbc_primary_commit"], "commit-A")
            self.assertEqual(got["mapping_revision"], "maprev")
            for v in got.values():
                self.assertNotIn("sha-P", v)
                self.assertNotIn("sha-M", v)
            with open(mp, "w") as f:
                json.dump({"artifacts": [{"role": "primary",
                                          "sha256": "sha-P"}]}, f)
            bare = rec.load_manifest(mp)
            self.assertEqual(bare["dbc_primary_commit"], "")
            self.assertEqual(bare["dbc_supplemental_commit"], "")
            self.assertEqual(bare["mapping_revision"], "")

    def test_metrics_reflect_outbox_state(self):
        with tempfile.TemporaryDirectory() as d:
            op = os.path.join(d, "o.sqlite")
            conn = rec.open_outbox(op)
            body = rec.render_metrics({"stored": 0, "uploaded": 0,
                                       "upload_failures": 0, "gaps": 0,
                                       "unsupported": 0, "snap_deduped": 0},
                                      op)
            self.assertIn("vss_outbox_pending_rows 0", body)
            self.assertIn("vss_outbox_oldest_event_time_ns 0", body)
            meta = _meta()
            rec.store(conn, rec.make_row("v", "Vehicle.Speed", 100, "m-1",
                                         meta, {}, 1.0, None, None, 1))
            rec.store(conn, rec.make_row("v", "Vehicle.Speed", 200, "m-2",
                                         meta, {}, 2.0, None, None, 1))
            body = rec.render_metrics({"stored": 2, "uploaded": 0,
                                       "upload_failures": 1, "gaps": 1,
                                       "unsupported": 0, "snap_deduped": 0},
                                      op)
            self.assertIn("vss_outbox_pending_rows 2", body)
            self.assertIn("vss_outbox_oldest_event_time_ns 100", body)
            self.assertIn("vss_stream_gaps_total 1", body)
            conn.close()


def _meta(**over):
    m = {"vehicle_firmware": "fw1", "decode_epoch": "e7",
         "vss_version": "4.1", "dbc_primary_commit": "c-primary",
         "dbc_supplemental_commit": "c-supp", "mapping_revision": "mrev",
         "collector_version": "vss-recorder-1"}
    m.update(over)
    return m


class OverrideProvenanceTest(unittest.TestCase):
    def test_applied_override_nulls_supplemental_and_pins_version(self):
        _text, info = setup.resolve_supplemental(
            DBC_B, b'VERSION ""\n\nNS_ :\nBS_ :\nBU_ :\n\n'
            b'BO_ 300 MsgC: 8 Vector__XXX\n'
            b' SG_ SigC : 0|8@1+ (1,0) [0|255] "" Vector__XXX\n',
            "", "ov-7")
        self.assertTrue(info["applied"])
        self.assertEqual(info["version"], "ov-7")
        self.assertIsNone(info["commit"])  # version never invents a commit

    def test_live_row_carries_effective_override_provenance(self):
        with tempfile.TemporaryDirectory() as d:
            mp = os.path.join(d, "manifest.json")
            with open(mp, "w") as f:
                json.dump({
                    "decode_epoch": "e9", "vehicle_firmware": "fw",
                    "vss_version": "4.0", "dbc_primary_commit": "commit-A",
                    "dbc_supplemental_commit": None,
                    "dbc_override_version": "ov-7",
                    "dbc_override_commit": None,
                    "mapping_revision": "maprev"}, f)
            meta = rec.load_manifest(mp)
            row = rec.make_row("v", "Vehicle.Speed", 100, "eid-ov",
                               meta, {}, 1.0, None, None, 1)
            self.assertIsNone(row["dbc_supplemental_commit"])
            self.assertEqual(row["dbc_override_version"], "ov-7")
            self.assertIsNone(row["dbc_override_commit"])
            sql = rec.render_insert("vehicle_signal", [row])
            self.assertIn("ov-7", sql)
            self.assertNotIn("None", sql)  # NULL, never the string "None"

    def test_existing_outbox_survives_override_migration(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as d:
            op = os.path.join(d, "o.sqlite")
            old = _sqlite.connect(op, timeout=30)
            old.execute("CREATE TABLE outbox(event_id TEXT PRIMARY KEY,"
                        " event_time INTEGER NOT NULL, vehicle TEXT NOT NULL,"
                        " path TEXT NOT NULL, source TEXT NOT NULL,"
                        " decode_epoch TEXT NOT NULL, value_num REAL,"
                        " value_text TEXT, value_bool INTEGER, unit TEXT,"
                        " vss_version TEXT, vehicle_firmware TEXT,"
                        " dbc_primary_commit TEXT,"
                        " dbc_supplemental_commit TEXT,"
                        " mapping_revision TEXT, collector_version TEXT,"
                        " ingest_time INTEGER NOT NULL)")
            old.execute("CREATE TABLE seen_ids(event_id TEXT PRIMARY KEY,"
                        " seen_at INTEGER NOT NULL)")
            old.execute("INSERT INTO outbox VALUES(?,?,?,?,?,?,?,?,?,?,?,?,"
                        "?,?,?,?,?)",
                        ("queued-1", 100, "v", "Vehicle.Speed", "can", "e7",
                         1.0, None, None, None, "4.1", "fw1", "c-p", "c-s",
                         "mrev", "vss-recorder-1", 1))
            old.commit()
            old.close()
            conn = rec.open_outbox(op)
            try:
                queued = conn.execute(
                    "SELECT event_id,value_num FROM outbox").fetchall()
                self.assertEqual(queued, [("queued-1", 1.0)])
                meta = _meta(dbc_supplemental_commit=None,
                             dbc_override_version="ov-7",
                             dbc_override_commit=None)
                rec.store(conn, rec.make_row(
                    "v", "Vehicle.Speed", 101, "queued-2", meta, {}, 2.0,
                    None, None, 1))
                got = conn.execute(
                    "SELECT event_id,dbc_supplemental_commit,"
                    "dbc_override_version,dbc_override_commit FROM outbox"
                    " ORDER BY event_time").fetchall()
                self.assertEqual(got, [("queued-1", "c-s", None, None),
                                       ("queued-2", None, "ov-7", None)])
            finally:
                conn.close()


class _FakeHTTP:
    def __init__(self, body):
        self._body = body.encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self._body


class _Stop:
    def is_set(self):
        return False


class _StopOn:
    """is_set() False for `allowed` checks, True after: exercises the
    upload_tick stop boundary (stop set mid catch-up)."""

    def __init__(self, allowed):
        self.left = allowed

    def is_set(self):
        if self.left > 0:
            self.left -= 1
            return False
        return True


def _vss_percentile(sorted_ns, pct):
    """Nearest-rank percentile over the actual sample set."""
    if not sorted_ns:
        return 0
    idx = min(len(sorted_ns) - 1, int(pct / 100 * len(sorted_ns)))
    return sorted_ns[idx]

def _bench_items(mod, meta, size, tag):
    """Deterministic mixed-type fixture: num/text/bool rotation with units
    on every fifth path. Caller-supplied ids, so baseline and batched runs
    stage byte-identical inputs."""
    units = {f"Vehicle.Speed.{i}": "km/h"
             for i in range(size) if i % 5 == 0}
    items = []
    for i in range(size):
        path = f"Vehicle.Speed.{i}"
        if i % 3 == 0:
            num, text, boolean = float(i), None, None
        elif i % 3 == 1:
            num, text, boolean = None, "t-%d" % i, None
        else:
            num, text, boolean = None, None, i % 2
        items.append((path, mod.make_row(
            "v", path, 100 + i, "%s-%d" % (tag, i), meta, units,
            num, text, boolean, 1 + i)))
    return items

def _rss_kb():
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":  # macOS reports bytes, Linux kilobytes
        rss //= 1024
    return rss


def _load_baseline_recorder(root):
    """Load an untouched baseline vss_recorder.py for parity runs. Returns
    None when the baseline checkout is absent (benchmark then covers the
    batched path only)."""
    import importlib.util
    path = os.path.join(root, "scripts", "vehicle", "vss_recorder.py")
    if not os.path.isfile(path):
        return None
    spec = importlib.util.spec_from_file_location(
        "vss_baseline_recorder", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _baseline_root():
    return os.environ.get(
        "VSS_BASELINE_ROOT", "/tmp/datalake-issues-20261009-baseline")


def _content_digest(mod, conn):
    import hashlib
    outbox = conn.execute(
        "SELECT " + ",".join(mod.COLUMNS) + " FROM outbox"
        " ORDER BY event_id").fetchall()
    seen = conn.execute(
        "SELECT event_id FROM seen_ids ORDER BY event_id").fetchall()
    digest = hashlib.sha256(repr((outbox, seen)).encode()).hexdigest()[:16]
    rows = [dict(zip(mod.COLUMNS, t)) for t in outbox]
    sql = hashlib.sha256(mod.render_insert(
        "vehicle_signal", rows).encode()).hexdigest()[:16]
    return digest, sql


def run_outbox_benchmark(sizes=(200, 1000), rates=(50.0, 500.0), repeats=3,
                         baseline_root=None, units_per_repeat=25):
    """Same-fixture baseline-vs-batched grid over snapshot sizes and real
    received input rates. The received unit is one snapshot/update batch of
    `size` rows: units are paced at `rate` units/sec with one real
    monotonic receive timestamp per unit, so p50/p95/p99 run over
    units_per_repeat*repeats actual receive-to-durable samples per cell
    (never modeled per-row arrival). Each cell uses a fresh file-backed
    WAL/FULL outbox; the baseline stores each received unit one row (one
    commit) at a time, the batched path stores each received unit in one
    commit. Reports per-unit actual receive-to-durable latency, achieved
    receive rate, rows/s, commits/1000 rows, sleep-excluded CPU s, final
    backlog rows, platform-correct peak RSS KB, and full-content plus
    upload-SQL digests (counts alone prove nothing). Returns a list of row
    dicts, one per (mode, size, rate) cell."""
    import statistics
    import time
    meta = _meta()
    base = (_load_baseline_recorder(baseline_root)
            if baseline_root else None)
    modes = ("baseline", "batched") if base is not None else ("batched",)
    report = []
    for size in sizes:
        for rate in rates:
            gap = 1_000_000_000 / rate
            for mode in modes:
                mod = base if mode == "baseline" else rec
                lat, cpus, rss_peak = [], [], 0
                commits_per_1000, rates_out = [], []
                digest = sql_digest = None
                backlog = 0
                for rep in range(repeats):
                    with tempfile.TemporaryDirectory() as d:
                        conn = mod.open_outbox(
                            os.path.join(d, "bench.sqlite"))
                        last = {}
                        commits = [0]

                        def trace(stmt, _c=commits):
                            if stmt.strip().upper() == "COMMIT":
                                _c[0] += 1
                        conn.set_trace_callback(trace)
                        try:
                            units, stored, ends, recvs = 0, 0, [], []
                            c0 = time.process_time_ns()
                            w0 = time.perf_counter_ns()
                            for u in range(units_per_repeat):
                                items = _bench_items(
                                    mod, meta, size, "bench-%d-%d-%d"
                                    % (size, rep, u))
                                t_recv = time.perf_counter_ns()
                                if mode == "baseline":
                                    for p, r in items:
                                        stored += mod.store_update(
                                            conn, last, p, r)
                                else:
                                    stored += mod.store_updates(
                                        conn, last, items)
                                ends.append(time.perf_counter_ns())
                                recvs.append(t_recv)
                                units += 1
                                nxt = w0 + units * gap
                                while True:
                                    now = time.perf_counter_ns()
                                    if now >= nxt:
                                        break
                                    time.sleep(min(0.005, max(
                                        0.0, (nxt - now) / 1e9)))
                            t_end = time.perf_counter_ns()
                        finally:
                            conn.set_trace_callback(None)
                        cpu_s = (time.process_time_ns() - c0) / 1e9
                        wall_s = (t_end - w0) / 1e9
                        assert stored == units * size, (mode, stored, units)
                        lat.extend(e - r for r, e in zip(recvs, ends))
                        cpus.append(cpu_s)
                        rss_peak = max(rss_peak, _rss_kb())
                        commits_per_1000.append(
                            commits[0] * 1000 / stored)
                        rates_out.append(units / wall_s if wall_s > 0 else 0)
                        backlog = conn.execute(
                            "SELECT COUNT(*) FROM outbox").fetchone()[0]
                        assert backlog == stored
                        assert conn.execute(
                            "SELECT COUNT(*) FROM seen_ids").fetchone()[0] \
                            == stored
                        digest, sql_digest = _content_digest(mod, conn)
                        conn.close()
                lat_sorted = sorted(lat)
                report.append({
                    "mode": mode, "snapshot_rows": size,
                    "input_rate_per_sec": rate, "repeats": repeats,
                    "units_per_repeat": units_per_repeat,
                    "units_measured": units_per_repeat * repeats,
                    "achieved_units_per_sec": round(
                        statistics.mean(rates_out), 1),
                    "rows_per_sec": round(
                        statistics.mean(rates_out) * size, 1),
                    "commits_per_1000_rows": round(
                        statistics.mean(commits_per_1000), 3),
                    "unit_receive_to_durable_p50_ns": _vss_percentile(
                        lat_sorted, 50),
                    "unit_receive_to_durable_p95_ns": _vss_percentile(
                        lat_sorted, 95),
                    "unit_receive_to_durable_p99_ns": _vss_percentile(
                        lat_sorted, 99),
                    "cpu_s_mean": round(statistics.mean(cpus), 4),
                    "backlog_rows": backlog, "peak_rss_kb": rss_peak,
                    "content_digest": digest,
                    "upload_sql_digest": sql_digest,
                })
    return report


def main_benchmark(argv=None):
    """Entry: VSS_BENCH_SIZES=200,1000 VSS_BENCH_RATES=50,500
    VSS_BENCH_REPEATS=3 VSS_BENCH_UNITS=25 VSS_BASELINE_ROOT=<checkout>
    python3 -m tests.test_vss benchmark."""
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default=os.environ.get(
        "VSS_BENCH_SIZES", "200,1000"))
    ap.add_argument("--rates", default=os.environ.get(
        "VSS_BENCH_RATES", "50,500"))
    ap.add_argument("--repeats", type=int, default=int(os.environ.get(
        "VSS_BENCH_REPEATS", "3")))
    ap.add_argument("--units", type=int, default=int(os.environ.get(
        "VSS_BENCH_UNITS", "25")))
    ap.add_argument("--baseline-root", default=_baseline_root())
    args = ap.parse_args(argv)
    sizes = tuple(int(s) for s in args.sizes.split(",") if s.strip())
    rates = tuple(float(s) for s in args.rates.split(",") if s.strip())
    for row in run_outbox_benchmark(sizes=sizes, rates=rates,
                                    repeats=args.repeats,
                                    baseline_root=args.baseline_root,
                                    units_per_repeat=args.units):
        print(json.dumps(row, sort_keys=True))




if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "benchmark":
        main_benchmark(sys.argv[2:])
    else:
        unittest.main()
