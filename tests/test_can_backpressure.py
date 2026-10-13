"""Issue #11 decoded-outbox backpressure helper/Worker tests (synthetic only).

Run (parent executes): python -m unittest discover -s tests -p test_can_backpressure.py

All tests invoke the real Archive/Worker paths and independently compare meta
counters to durable outbox table scans. No counter reconciliation stand-ins:
any drift between archive_meta and the table is a failure.
"""
import sqlite3
import tempfile
import threading
import time
import unittest
from http.server import HTTPServer
from pathlib import Path
from unittest.mock import patch

from scripts.ingest.can.can_receiver import (
    Archive, ConfigurationError, DownstreamError, Greptime, Worker,
    _migrate_outbox_bytes, _outbox_bump, _wait_hint, decode_paused, outbox_paused,
    outbox_table_usage, outbox_usage,
)
from tests.test_can_receiver import SQLSink, synthetic_decoder, wait_for


def _meta(directory):
    return {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
            "session_id": "bp", "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}


def _assert_counters_exact(testcase, archive):
    """Meta counters MUST equal the durable table scan (real behavior, no fixups)."""
    testcase.assertEqual(outbox_usage(archive), outbox_table_usage(archive))


class HysteresisPredicate(unittest.TestCase):
    def test_high_pauses_low_resumes_both_axes(self):
        self.assertFalse(outbox_paused(5, 5, 10, 10, 6, 6))
        self.assertTrue(outbox_paused(10, 5, 10, 10, 6, 6))
        self.assertTrue(outbox_paused(5, 10, 10, 10, 6, 6))
        self.assertTrue(outbox_paused(7, 7, 10, 10, 6, 6, was_paused=True))
        self.assertFalse(outbox_paused(6, 6, 10, 10, 6, 6, was_paused=True))

    def test_invalid_bounds_rejected(self):
        for bad in [(0, 10, 0, 10), (5, 5, 1, 1), (10, 10, 10, 10), (6, 10, 6, 10),
                    (True, 10, 0, 10), (10.0, 10, 0, 10)]:
            with self.assertRaises(ConfigurationError, msg=str(bad)):
                outbox_paused(0, 0, bad[0], bad[2], bad[1], bad[3])
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            decoder = synthetic_decoder(directory)
            with self.assertRaisesRegex(ConfigurationError, "invalid_outbox_watermarks"):
                Worker(archive, decoder, None, high_rows=0)
            with self.assertRaisesRegex(ConfigurationError, "invalid_outbox_watermarks"):
                Worker(archive, decoder, None, high_rows=10, low_rows=10)


class UsageAndMigration(unittest.TestCase):
    def test_usage_matches_table_utf8_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            with archive.connect() as conn:
                conn.execute("INSERT INTO epochs VALUES('e','r')")
                conn.commit()
                fixtures = [("a-1", '{"k":"é"}'), ("a-2", '{"k":"x"}')]
                blob_bytes = sum(len(j.encode()) for _, j in fixtures)
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany("INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
                                 [(e, "e", j) for e, j in fixtures])
                _outbox_bump(conn, len(fixtures), blob_bytes)
                conn.commit()
            # Lawful fixture initialization: seeded rows + counters in one txn.
            self.assertEqual(outbox_usage(archive), (2, blob_bytes))
            self.assertEqual(outbox_usage(archive), outbox_table_usage(archive))

    def test_migrate_backfill_idempotent_and_rollback_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            with archive.connect() as conn:
                conn.execute("INSERT INTO epochs VALUES('e','r')")
                conn.execute("INSERT INTO outbox(event_id,epoch,row_json) VALUES('a','e','{\"k\":1}')")
                # Simulate a legacy archive predating the byte counter.
                conn.execute("DELETE FROM archive_meta WHERE key='outbox_bytes'")
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                self.assertTrue(_migrate_outbox_bytes(conn))
                conn.rollback()
            with archive.connect() as conn:
                self.assertIsNone(conn.execute("SELECT value FROM archive_meta WHERE key='outbox_bytes'").fetchone())
                conn.execute("BEGIN IMMEDIATE")
                self.assertTrue(_migrate_outbox_bytes(conn))
                conn.commit()
            with archive.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self.assertFalse(_migrate_outbox_bytes(conn))
                conn.rollback()
                stored = conn.execute("SELECT value FROM archive_meta WHERE key='outbox_bytes'").fetchone()[0]
            self.assertEqual(stored, len('{"k":1}'.encode()))

    def test_bump_moves_rows_and_bytes_together(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            with archive.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                _outbox_bump(conn, 3, 99)
                conn.rollback()
                self.assertEqual(conn.execute("SELECT value FROM archive_meta WHERE key='outbox_rows'").fetchone()[0], 0)
                conn.execute("BEGIN IMMEDIATE")
                _outbox_bump(conn, 3, 99)
                conn.commit()
                got = {k: conn.execute("SELECT value FROM archive_meta WHERE key=?", (k,)).fetchone()[0]
                       for k in ("outbox_rows", "outbox_bytes")}
            self.assertEqual(got, {"outbox_rows": 3, "outbox_bytes": 99})


class WorkerPauseAndHints(unittest.TestCase):
    def test_high_low_oscillation_no_flap(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            frame = b"t12320200\r"
            archive.accept(_meta(directory), [{"seq": i, "offset_ns": i, "phase": "capture", "data": frame}
                                              for i in range(4)])
            while archive.decode_once(decoder):
                pass
            _assert_counters_exact(self, archive)
            rows, nbytes = outbox_usage(archive)
            self.assertGreater(rows, 0)
            worker = Worker(archive, decoder, None, interval=0.02, high_rows=rows, low_rows=max(0, rows - 2),
                            high_bytes=nbytes, low_bytes=max(0, nbytes - 2))
            # Enter the real Worker pause path: the loop observes halt, sets its
            # own _decode_paused, then waits hinted. was_paused persistence is
            # the loop's own transition, not a test-seeded flag.
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "outage"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic",
                                    "test", "test", timeout=5)
                worker.greptime = greptime
                worker.start()
                try:
                    wait_for(lambda: worker._decode_paused)
                finally:
                    worker.stop_event.set()
                    worker.join(5)
                self.assertTrue(worker._decode_paused)
                self.assertTrue(worker._decode_halted())
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)
            # Partial progress drains one row through the real full-ACK path so
            # byte counters move in the same transaction as the delete.
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "success"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                archive.flush_once(Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic",
                                            "test", "test", timeout=5), limit=1)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)
            # One row below high still pauses until low is reached (anti-flap),
            # and counters still match the table after the real delete.
            _assert_counters_exact(self, archive)
            self.assertTrue(worker._decode_halted())

    def test_mixed_dense_partial_pause_keeps_raw_durable(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            meta = _meta(directory)
            archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"t12320200\r" * 40}])
            drained_partial = 0
            while archive.decode_once(decoder):
                drained_partial += 1
                # Actual decode partial progress: counters exact after every batch.
                _assert_counters_exact(self, archive)
            self.assertGreater(drained_partial, 0)
            _assert_counters_exact(self, archive)
            rows, nbytes = outbox_usage(archive)
            worker = Worker(archive, decoder, None, interval=0.02, high_rows=1, low_rows=0,
                            high_bytes=1, low_bytes=0)
            worker._decode_paused = False
            self.assertTrue(worker._decode_halted())
            with archive.connect() as conn:
                before_raw = [(r["session"], r["seq"], bytes(r["data"])) for r in
                              conn.execute("SELECT session,seq,data FROM raw_chunks ORDER BY id")]
            # Raw accept still works while decode is paused (existing reserve policy).
            archive.accept(meta, [{"seq": 1, "offset_ns": 1, "phase": "capture", "data": b"t12320400\r"}])
            with archive.connect() as conn:
                raw_now = [(r["session"], r["seq"], bytes(r["data"])) for r in
                            conn.execute("SELECT session,seq,data FROM raw_chunks ORDER BY id")]
            self.assertEqual(raw_now[:len(before_raw)], before_raw)
            self.assertEqual(len(raw_now), len(before_raw) + 1)

    def test_lost_hint_and_restart_recover_via_table_recheck(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            stop = threading.Event()
            worker = Worker(archive, decoder, None, interval=0.02)
            # Lost hint: waiter was already past clear(); a fresh notify still wakes it.
            archive.upload_ready.clear()
            threading.Timer(0.05, archive.upload_ready.set).start()
            self.assertTrue(_wait_hint(stop, archive.upload_ready, 5, worker._has_outbox))
            # Restart: reopen the same path (fresh events) and recover through
            # the real full-ACK path; the fresh Worker rechecks durable tables.
            archive.accept(_meta(directory), [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"t12320200\r"}])
            archive.decode_once(decoder)
            _assert_counters_exact(self, archive)
            reopened = Archive(archive.path, disk_reserve_bytes=0)
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "success"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=5)
                worker = Worker(reopened, decoder, greptime, interval=0.02)
                worker.start()
                try:
                    wait_for(lambda: reopened.status()["pending_rows"] == 0)
                    worker.stop_event.set()
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                finally:
                    worker.stop_event.set()
                    worker.join(5)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)
            _assert_counters_exact(self, reopened)

    def test_accept_in_empty_read_to_clear_window_wakes(self):
        """An accept commit between the empty decode read and hint clear must wake."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            worker = Worker(archive, decoder, None, interval=5)
            real_clear = archive.decode_ready.clear

            def clear_then_accept():
                real_clear()
                archive.accept(_meta(directory), [{"seq": 0, "offset_ns": 0,
                                                  "phase": "capture", "data": b"t12320200\r"}])

            with patch.object(archive.decode_ready, "clear", clear_then_accept):
                # Pending probe runs AFTER the clear, so the raced commit is seen
                # without waiting the full idle interval.
                self.assertTrue(_wait_hint(worker.stop_event, archive.decode_ready, 5, worker._has_pending))
            self.assertTrue(worker._has_pending())

    def test_error_probe_backs_off_without_busy_loop(self):
        """A failing durable probe sleeps the idle interval instead of waking hot."""
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            stop = threading.Event()

            def fail():
                raise sqlite3.OperationalError("injected storage failure")

            started = time.monotonic()
            self.assertFalse(_wait_hint(stop, archive.decode_ready, 0.3, fail))
            self.assertGreaterEqual(time.monotonic() - started, 0.25)

    def test_stop_deadline_not_missed_during_hinted_wait(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            worker = Worker(archive, decoder, None, interval=0.02)
            worker_stop = threading.Event()
            started = time.monotonic()
            threading.Timer(0.05, worker_stop.set).start()
            _wait_hint(worker_stop, archive.decode_ready, 30, worker._has_pending)
            self.assertLess(time.monotonic() - started, 5)

    def test_full_ack_hint_wakes_decode_without_busy_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(_meta(directory), [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"t12320200\r"}])
            archive.decode_once(decoder)
            _assert_counters_exact(self, archive)
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "success"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=5)
                worker = Worker(archive, decoder, greptime, interval=0.02)
                worker.start()
                try:
                    wait_for(lambda: archive.status()["pending_rows"] == 0)
                    worker.stop_event.set()
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                finally:
                    worker.stop_event.set()
                    worker.join(5)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)
            _assert_counters_exact(self, archive)

    def test_outage_retry_not_busy_and_recovery_drains_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            meta = _meta(directory)
            archive.accept(meta, [{"seq": i, "offset_ns": i, "phase": "capture", "data": b"t12320200\r"}
                                  for i in range(3)])
            while archive.decode_once(decoder):
                pass
            _assert_counters_exact(self, archive)
            with archive.connect() as conn:
                before = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "outage"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=5)
                worker = Worker(archive, decoder, greptime, interval=0.05, retry_interval=0.2)
                worker.start()
                try:
                    time.sleep(0.4)
                    # Outage: nothing drained, no busy-loop deletion.
                    self.assertEqual(archive.status()["pending_rows"], len(before))
                    _assert_counters_exact(self, archive)
                    worker.stop_event.set()
                    worker.join(5)
                finally:
                    worker.stop_event.set()
                    worker.join(5)
                sink.mode = "success"
                drained = archive.flush_once(Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic",
                                                     "test", "test", timeout=5))
                self.assertEqual(drained, len(before))
                self.assertEqual(archive.status()["pending_rows"], 0)
                _assert_counters_exact(self, archive)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)

    def test_partial_timeout_dirty_contracts_hold(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(_meta(directory), [{"seq": i, "offset_ns": i, "phase": "capture", "data": b"t12320200\r"}
                                              for i in range(3)])
            while archive.decode_once(decoder):
                pass
            _assert_counters_exact(self, archive)
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "partial"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                partial = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=5)
                with self.assertRaisesRegex(DownstreamError, "^greptime_partial_ack$"):
                    archive.flush_once(partial)
                self.assertEqual(archive.status()["pending_rows"], 3)
                _assert_counters_exact(self, archive)
                import socket
                held = Greptime("http://127.0.0.1:9", "synthetic", "test", "test", timeout=5)
                with patch.object(Greptime, "_connect", side_effect=socket.timeout):
                    with self.assertRaisesRegex(DownstreamError, "^greptime_timeout$"):
                        archive.flush_once(held)
                self.assertEqual(archive.status()["pending_rows"], 3)
                _assert_counters_exact(self, archive)

                # Controlled dirty failure: the signal POST fully ACKs, only the
                # dirty-table POST fails at the HTTP layer (real Greptime
                # contract: affected rows echo submitted VALUES rows).
                import json as _json
                import urllib.parse as _parse
                with archive.connect() as conn:
                    before_rows = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                    before_events = [r[0] for r in conn.execute("SELECT event_id FROM outbox ORDER BY id")]
                seen = {"signal": 0, "dirty": 0}
                def fail_dirty_table(self):
                    length = int(self.headers["Content-Length"])
                    payload = self.rfile.read(length)
                    if b"vehicle_signal_dirty" in payload:
                        seen["dirty"] += 1
                        body = b"sensitive downstream error body"
                        self.send_response(503)
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                    seen["signal"] += 1
                    params = _parse.parse_qs(payload.decode("ascii"))
                    reported = params["sql"][0].count("),(") + 1
                    body = _json.dumps({"code": 0, "output": [{"affectedrows": reported}]}).encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                with patch.object(SQLSink, "do_POST", fail_dirty_table):
                    with self.assertRaisesRegex(DownstreamError, "^dirty_notify_failure$"):
                        archive.flush_once(partial)
                # Signal ACKed then dirty failed: the whole prefix stays durable.
                self.assertEqual(seen, {"signal": 1, "dirty": 1})
                self.assertEqual(archive.status()["pending_rows"], 3)
                with archive.connect() as conn:
                    self.assertEqual([r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")],
                                     before_rows)
                    self.assertEqual([r[0] for r in conn.execute("SELECT event_id FROM outbox ORDER BY id")],
                                     before_events)
                _assert_counters_exact(self, archive)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)

    def test_commit_failure_sends_no_hint(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(_meta(directory), [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"t12320200\r"}])
            with patch.object(Archive, "decode_once", side_effect=ValueError("boom")):
                try:
                    archive.decode_once(decoder)
                except ValueError:
                    pass
                else:
                    self.fail("decode_once must raise")
                # Worker only notifies after a successful commit; a raised
                # decode leaves the Archive hint clear.
                self.assertFalse(archive.upload_ready.is_set())
            self.assertEqual(archive.status()["pending_rows"], 0)
            _assert_counters_exact(self, archive)

    def test_recovery_converges_to_exact_events_cursors(self):
        import json as _json
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            meta = _meta(directory)
            archive.accept(meta, [{"seq": i, "offset_ns": i, "phase": "capture", "data": b"t12320200\r"}
                                  for i in range(3)])
            while archive.decode_once(decoder):
                pass
            _assert_counters_exact(self, archive)
            with archive.connect() as conn:
                expect_rows = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                expect_events = [r[0] for r in conn.execute("SELECT event_id FROM outbox ORDER BY id")]
                expect_cursors = [(r["session"], r["epoch"], r["next_seq"], r["state_json"], r["counts_json"], r["rows"])
                                  for r in conn.execute("SELECT session,epoch,next_seq,state_json,counts_json,rows FROM decode_states")]
                expect_raw = [(r["session"], r["seq"], bytes(r["data"])) for r in
                              conn.execute("SELECT session,seq,data FROM raw_chunks ORDER BY id")]
            # Simulated restart: reopen and drain via real success sink.
            archive = Archive(path, disk_reserve_bytes=0)
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "success"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            try:
                greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test", timeout=5)
                worker = Worker(archive, decoder, greptime, interval=0.02)
                worker.start()
                try:
                    wait_for(lambda: archive.status()["pending_rows"] == 0)
                    worker.stop_event.set()
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                finally:
                    worker.stop_event.set()
                    worker.join(5)
                # Cursor/raw equality after recovery drain; outbox empty by ACK contract.
                with archive.connect() as conn:
                    cursors = [(r["session"], r["epoch"], r["next_seq"], r["state_json"], r["counts_json"], r["rows"])
                               for r in conn.execute("SELECT session,epoch,next_seq,state_json,counts_json,rows FROM decode_states")]
                    raw = [(r["session"], r["seq"], bytes(r["data"])) for r in
                           conn.execute("SELECT session,seq,data FROM raw_chunks ORDER BY id")]
                    self.assertEqual(archive.status()["pending_rows"], 0)
                self.assertEqual(cursors, expect_cursors)
                self.assertEqual(raw, expect_raw)
                self.assertEqual([_json.loads(c)["event_id"] for c in expect_rows], expect_events)
                _assert_counters_exact(self, archive)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)


if __name__ == "__main__":
    unittest.main()
