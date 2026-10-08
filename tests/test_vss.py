"""Regression tests for vehicle VSS setup + recorder helpers (stdlib only).

Run: python3 -m tests.test_vss
Covers: duplicate CAN-ID rejection, override replace semantics, mapping
signal validation, outbox value classification, INSERT escaping,
store column-order roundtrip, fail-closed Greptime ack (HTTP200 errors,
output errors, partial/missing affected rows keep rows; retry reuses the
same event_id/event_time), source-time exactness/fallback/int64 range,
int precision boundaries, deterministic event-id dedupe incl. acked-row
restart and seen-prune bounds, uploader catch-up tick, idle-subscribe
unblock helper, env validation, manifest top-level pins, and metrics
reflecting outbox state.
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


if __name__ == "__main__":
    unittest.main()
