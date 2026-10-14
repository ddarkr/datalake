"""Bounded raw-cache storage, ACK retention and restart behavior."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import HTTPServer

from scripts.ingest.can.can_receiver import Archive, ConfigurationError, DownstreamError, Greptime, Rejection
from test_can_receiver import SQLSink, synthetic_decoder


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
