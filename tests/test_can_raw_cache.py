"""Bounded raw-cache storage, ACK retention and restart behavior."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from http.server import HTTPServer

from scripts.ingest.can.can_receiver import Archive, ConfigurationError, DownstreamError, Greptime, Rejection
from tests.test_can_receiver import SQLSink, synthetic_decoder


class RawCache(unittest.TestCase):
    def test_ack_pruning_reclaims_disk_preserves_tail_and_retry_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.sqlite"
            cap = 8 * 1024 * 1024
            decoder = synthetic_decoder(directory)
            archive = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
            archive.register_epoch(decoder)
            meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                    "session_id": "bounded", "started_ns": 1800000000000000000,
                    "vehicle_firmware": "synthetic"}
            def chunk(seq, data):
                return {"seq": seq, "offset_ns": seq, "phase": "capture", "data": data}
            batches = [[chunk(i, b"t12320200\r" if i == 0 else b"V\r")
                        for i in range(start, start + 1000)] for start in range(0, 30000, 1000)]
            for batch in batches:
                archive.accept(meta, batch)
            archive.accept(meta, [chunk(30000, b"t1232")])
            while archive.decode_once(decoder):
                pass
            peak = path.stat().st_size
            self.assertEqual(archive.status()["pending_rows"], 1)
            self.assertEqual(archive.prune_once(), 0)
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "partial"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            greptime = Greptime("http://127.0.0.1:%d" % sink.server_port,
                                "synthetic", "test", "test", timeout=5)
            try:
                with self.assertRaisesRegex(DownstreamError, "greptime_partial_ack"):
                    archive.flush_once(greptime)
                self.assertEqual(archive.prune_once(), 0)
                self.assertEqual(archive.status()["retained_raw_chunks"], 30001)
                sink.mode = "success"
                self.assertEqual(archive.flush_once(greptime), 1)
                while archive.prune_once():
                    pass
                self.assertLess(path.stat().st_size, peak)
                self.assertLessEqual(path.stat().st_size, cap)
                self.assertFalse(Path(str(path) + "-wal").exists())
                state = archive.status()
                self.assertEqual((state["raw_chunks"], state["retained_raw_chunks"], state["pruned_chunks"]),
                                 (30001, 10000, 20001))
                self.assertEqual(state["backlog_chunks"], 0)
                self.assertEqual(archive.accept(meta, batches[0]), 0)
                with self.assertRaisesRegex(Rejection, "chunk_conflict"):
                    archive.accept(meta, [chunk(30000, b"conflict")])
                with self.assertRaisesRegex(ConfigurationError, "re-decode_requires_unpruned_raw"):
                    archive.register_epoch(synthetic_decoder(directory, revision="new"), explicit=True)
                reopened = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
                reopened.accept(meta, [chunk(30001, b"0200\r")])
                while reopened.decode_once(decoder):
                    pass
                with reopened.connect() as conn:
                    row = json.loads(conn.execute("SELECT row_json FROM outbox").fetchone()[0])
                self.assertEqual(row["value_num"], 1.0)
                self.assertEqual(row["event_time"], meta["started_ns"] + 30001)
                self.assertEqual(reopened.flush_once(greptime), 1)
                # Repeated completed batches reuse/shrink space rather than retaining history.
                for start in (30002, 50002, 70002):
                    for first in range(start, start + 20000, 1000):
                        reopened.accept(meta, [chunk(i, b"V\r") for i in range(first, first + 1000)])
                    while reopened.decode_once(decoder):
                        pass
                    while reopened.prune_once():
                        pass
                    self.assertEqual(reopened.status()["retained_raw_chunks"], 10000)
                    self.assertLessEqual(path.stat().st_size, cap)
            finally:
                greptime.close()
                sink.shutdown()
                sink.server_close()
                thread.join(3)

    def test_payload_budget_keeps_large_chunks_and_short_sessions_progressing(self):
        cap = 4 * 1024 * 1024
        payload = b"V\r" * 32768
        for mode in ("long", "short", "unbounded"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "raw.sqlite"
                decoder = synthetic_decoder(directory)
                archive = Archive(path, disk_reserve_bytes=0,
                                  max_database_bytes=None if mode == "unbounded" else cap)
                archive.register_epoch(decoder)
                meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                        "session_id": "budget", "started_ns": 1800000000000000000,
                        "vehicle_firmware": "synthetic"}
                for index in range(100):
                    current = dict(meta, session_id="budget-%d" % index) if mode == "short" else meta
                    seq = 0 if mode == "short" else index
                    chunk = {"seq": seq, "offset_ns": seq, "phase": "capture", "data": payload}
                    self.assertEqual(archive.accept(current, [chunk]), 1)
                    while archive.decode_once(decoder):
                        pass
                    while archive.prune_once(limit=3):
                        pass
                    if mode != "unbounded":
                        self.assertLessEqual(path.stat().st_size, cap)
                        self.assertLessEqual(archive.status()["raw_bytes"], cap // 4)
                state = archive.status()
                self.assertEqual((state["raw_chunks"], state["backlog_chunks"], state["pending_rows"]),
                                 (100, 0, 0))
                self.assertEqual(state["sessions"], 100 if mode == "short" else 1)
                self.assertEqual(state["pruned_chunks"], 0 if mode == "unbounded" else 84)
                self.assertEqual(state["retained_raw_chunks"], 100 if mode == "unbounded" else 16)
                first_meta = dict(meta, session_id="budget-0") if mode == "short" else meta
                self.assertEqual(archive.accept(first_meta, [dict(chunk, seq=0, offset_ns=0)]), 0)
                with archive.connect() as conn:
                    self.assertEqual(conn.execute("SELECT SUM(next_seq) FROM sessions").fetchone()[0], 100)
                    self.assertEqual(conn.execute("SELECT SUM(next_seq) FROM decode_states").fetchone()[0], 100)

    def test_pressure_pruning_waits_for_every_registered_epoch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.sqlite"
            cap = 4 * 1024 * 1024
            first = synthetic_decoder(directory)
            second = synthetic_decoder(directory, revision="second")
            archive = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
            archive.register_epoch(first)
            archive.register_epoch(second, explicit=True)
            meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                    "session_id": "epochs", "started_ns": 1800000000000000000,
                    "vehicle_firmware": "synthetic"}
            chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": b"V\r" * 32768}
                      for seq in range(20)]
            archive.accept(meta, chunks)
            self.assertEqual(archive.prune_once(), 0)
            while archive.decode_once(first):
                pass
            self.assertEqual(archive.prune_once(), 0)
            self.assertEqual(archive.status()["retained_raw_chunks"], 20)
            self.assertEqual(archive.decode_once(second, limit=1), 1)
            self.assertEqual(archive.prune_once(limit=1), 1)
            self.assertEqual(archive.prune_once(), 0)
            with archive.connect() as conn:
                self.assertEqual(conn.execute("SELECT MIN(seq) FROM raw_chunks").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT next_seq FROM raw_pruned").fetchone()[0], 1)
            while archive.decode_once(second):
                pass
            self.assertEqual(archive.prune_once(limit=2), 2)
            self.assertEqual(archive.prune_once(limit=2), 1)
            self.assertEqual(archive.prune_once(), 0)
            self.assertEqual(archive.status()["retained_raw_chunks"], 16)
            self.assertEqual(archive.status()["backlog_chunks"], 0)
            self.assertLessEqual(path.stat().st_size, cap)
            with archive.connect() as conn:
                self.assertEqual([r[0] for r in conn.execute("SELECT next_seq FROM decode_states")], [20, 20])
                self.assertEqual(conn.execute("SELECT MIN(seq) FROM raw_chunks").fetchone()[0], 4)

    def test_pressure_pruning_preserves_parser_and_partial_restart_after_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.sqlite"
            cap = 4 * 1024 * 1024
            decoder = synthetic_decoder(directory)
            archive = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
            archive.register_epoch(decoder)
            meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                    "session_id": "partial", "started_ns": 1800000000000000000,
                    "vehicle_firmware": "synthetic"}
            chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": b"V\r" * 32768}
                      for seq in range(17)]
            chunks[-1]["data"] = b"V\r" * 32765 + b"t1232"
            archive.accept(meta, chunks)
            while archive.decode_once(decoder):
                pass
            with archive.connect() as conn:
                parser = tuple(conn.execute("SELECT * FROM decode_states").fetchone())
            self.assertEqual(archive.prune_once(limit=1), 1)
            with archive.connect() as conn:
                self.assertEqual(tuple(conn.execute("SELECT * FROM decode_states").fetchone()), parser)
            archive = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
            continuation = {"seq": 17, "offset_ns": 17, "phase": "capture", "data": b"0200\rt12320200\r"}
            archive.accept(meta, [continuation])
            with patch("scripts.ingest.can.can_receiver.DECODE_ROW_BUDGET", 1):
                self.assertEqual(archive.decode_once(decoder), 1)
            with archive.connect() as conn:
                partial = tuple(conn.execute("SELECT * FROM decode_partial").fetchone())
                self.assertEqual(partial[2], 17)
            self.assertEqual(archive.status()["pending_rows"], 1)
            self.assertEqual(archive.prune_once(), 0)
            sink = HTTPServer(("127.0.0.1", 0), SQLSink)
            sink.mode = "partial"
            thread = threading.Thread(target=sink.serve_forever, daemon=True)
            thread.start()
            greptime = Greptime("http://127.0.0.1:%d" % sink.server_port,
                                "synthetic", "test", "test", timeout=5)
            try:
                with self.assertRaisesRegex(DownstreamError, "greptime_partial_ack"):
                    archive.flush_once(greptime)
                self.assertEqual(archive.prune_once(), 0)
                sink.mode = "success"
                self.assertEqual(archive.flush_once(greptime), 1)
                self.assertEqual(archive.prune_once(limit=1), 1)
                with archive.connect() as conn:
                    self.assertEqual(tuple(conn.execute("SELECT * FROM decode_partial").fetchone()), partial)
                    self.assertEqual(tuple(conn.execute("SELECT * FROM decode_states").fetchone()), parser)
                    self.assertEqual(conn.execute("SELECT data FROM raw_chunks WHERE seq=17").fetchone()[0],
                                     continuation["data"])
                    self.assertEqual(conn.execute("SELECT next_seq FROM sessions").fetchone()[0], 18)
                    self.assertEqual(conn.execute("SELECT next_seq FROM raw_pruned").fetchone()[0], 2)
                archive = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
                while archive.decode_once(decoder):
                    pass
                with archive.connect() as conn:
                    row = json.loads(conn.execute("SELECT row_json FROM outbox").fetchone()[0])
                    self.assertIsNone(conn.execute("SELECT 1 FROM decode_partial").fetchone())
                self.assertEqual(row["value_num"], 1.0)
                self.assertEqual(row["event_time"], meta["started_ns"] + 17)
                self.assertEqual(archive.flush_once(greptime), 1)
                self.assertEqual(archive.status()["backlog_chunks"], 0)
                self.assertEqual(archive.status()["pending_rows"], 0)
                self.assertLessEqual(path.stat().st_size, cap)
            finally:
                greptime.close()
                sink.shutdown()
                sink.server_close()
                thread.join(3)
    def test_admission_and_sqlite_cap_apply_after_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.sqlite"
            cap = 4 * 1024 * 1024
            archive = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
            meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                    "session_id": "full", "started_ns": 1800000000000000000,
                    "vehicle_firmware": "synthetic"}
            chunk = {"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"x" * cap}
            with self.assertRaisesRegex(Rejection, "raw_cache_full"):
                archive.accept(meta, [chunk])
            self.assertEqual(archive.status()["raw_chunks"], 0)
            reopened = Archive(path, disk_reserve_bytes=0, max_database_bytes=cap)
            # Exercise SQLite's actual allocation ceiling, independently of admission estimation.
            with reopened.connect() as conn:
                conn.execute("CREATE TABLE cap_probe(data BLOB)")
                conn.commit()
                import sqlite3
                with self.assertRaisesRegex(sqlite3.OperationalError, "full"):
                    conn.execute("INSERT INTO cap_probe VALUES(zeroblob(?))", (cap,))
                conn.rollback()
                conn.execute("DROP TABLE cap_probe")
                conn.commit()
            self.assertLessEqual(path.stat().st_size, cap)
            reopened.accept(meta, [dict(chunk, data=b"V\r")])
            self.assertEqual(reopened.status()["raw_chunks"], 1)


if __name__ == "__main__":
    unittest.main()
