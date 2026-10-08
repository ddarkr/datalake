"""Receiver raw-archive observability: capacity, errors, staged freshness."""
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.storage import storage_metrics as sm

ENV_KEYS = ("CAN_RECEIVER_ARCHIVE", "CAN_RECEIVER_DISK_RESERVE_BYTES")

SCHEMA = """
CREATE TABLE sessions(id INTEGER PRIMARY KEY, vehicle TEXT NOT NULL,
 collector_id TEXT NOT NULL, session_id TEXT NOT NULL, meta_json TEXT NOT NULL,
 next_seq INTEGER NOT NULL DEFAULT 0, last_offset_ns INTEGER NOT NULL DEFAULT -1);
CREATE TABLE raw_chunks(id INTEGER PRIMARY KEY, session INTEGER NOT NULL,
 seq INTEGER NOT NULL, offset_ns INTEGER NOT NULL, phase TEXT NOT NULL,
 data BLOB NOT NULL);
CREATE TABLE outbox(id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
 epoch TEXT NOT NULL, row_json TEXT NOT NULL);
CREATE TABLE worker_errors(kind TEXT PRIMARY KEY, count INTEGER NOT NULL,
 last_ns INTEGER NOT NULL);
"""


def make_archive(directory, rows=(), errors=(), meta=()):
    path = Path(directory) / "raw.sqlite3"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO sessions(vehicle,collector_id,session_id,meta_json) VALUES(?,?,?,?)",
            ("synthetic-vehicle", "synthetic-collector", "s1", "{}"))
        for event_id, event_ns, ingest_ns in rows:
            conn.execute(
                "INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
                (event_id, "synthetic-epoch", json.dumps(
                    {"event_time": event_ns, "ingest_time": ingest_ns,
                     "event_id": event_id, "vehicle": "synthetic-vehicle"})))
        for kind, count, last_ns in errors:
            conn.execute("INSERT INTO worker_errors VALUES(?,?,?)", (kind, count, last_ns))
        if meta:
            conn.execute("CREATE TABLE archive_meta(key TEXT PRIMARY KEY, value INTEGER)")
            for key, value in meta:
                conn.execute("INSERT INTO archive_meta VALUES(?,?)", (key, value))
        conn.commit()
    finally:
        conn.close()
    return str(path)


class ReceiverArchiveTest(unittest.TestCase):
    def setUp(self):
        self._old = {k: os.environ.get(k) for k in ENV_KEYS}
        for k in ENV_KEYS:
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_missing_archive_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(os.path.join(directory, "absent.sqlite3"))
            self.assertEqual(metrics, {"datalake_receiver_archive_known": 0,
                                       "datalake_receiver_archive_success": 0})

    def test_corrupt_archive_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.sqlite3"
            path.write_bytes(b"not a database")
            self.assertEqual(sm.receiver_metrics(str(path))["datalake_receiver_archive_known"], 0)

    def test_pending_oldest_and_errors_without_invented_raw_totals(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 1_700_000_000_000_000_000, 1_700_000_001_000_000_000),
                                                    ("e2", 1_700_000_002_000_000_000, 1_700_000_003_000_000_000)),
                                   errors=(("greptime_timeout", 3, 1_700_000_004_000_000_000),))
            metrics = sm.receiver_metrics(archive)
            self.assertEqual(metrics["datalake_receiver_archive_known"], 1)
            self.assertEqual(metrics["datalake_receiver_pending_rows"], 2)
            self.assertEqual(metrics["datalake_receiver_oldest_pending_id"], 1)
            self.assertAlmostEqual(metrics["datalake_receiver_oldest_pending_event_timestamp_seconds"], 1.7e9)
            self.assertAlmostEqual(metrics["datalake_receiver_oldest_pending_ingest_timestamp_seconds"], 1.700000001e9)
            self.assertEqual(metrics['datalake_receiver_errors_total{kind="greptime_timeout"}'], 3)
            self.assertAlmostEqual(metrics['datalake_receiver_error_last_timestamp_seconds{kind="greptime_timeout"}'], 1.700000004e9)
            self.assertEqual(metrics['datalake_receiver_errors_total{kind="decode_failure"}'], 0)
            # No raw scan here: totals stay unknown until receiver counters exist.
            self.assertNotIn("datalake_receiver_raw_chunks", metrics)
            self.assertNotIn("datalake_receiver_raw_bytes", metrics)
            self.assertNotIn("datalake_receiver_last_receive_timestamp_seconds", metrics)

    def test_empty_outbox_has_no_oldest_or_error_last(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(make_archive(directory))
            self.assertEqual(metrics["datalake_receiver_pending_rows"], 0)
            self.assertNotIn("datalake_receiver_oldest_pending_id", metrics)
            self.assertNotIn("datalake_receiver_oldest_pending_event_timestamp_seconds", metrics)
            self.assertNotIn("datalake_receiver_error_last_timestamp_seconds{kind=\"x\"}", metrics)

    def test_unknown_error_kind_never_leaks(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(
                make_archive(directory, errors=(("custom_secret_stuff", 9, 1_700_000_000_000_000_000),)))
            self.assertNotIn("custom_secret_stuff", " ".join(metrics))
            self.assertEqual(metrics['datalake_receiver_errors_total{kind="greptime_failure"}'], 0)

    def test_archive_meta_counters_and_staged_freshness(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, meta=(
                ("raw_chunks", 40), ("raw_bytes", 8000),
                ("decoded_rows_total", 30), ("acked_rows_total", 28),
                ("last_receive_ns", 1_700_000_000_000_000_000),
                ("last_decode_ns", 1_700_000_001_000_000_000),
                ("bogus_metric", 7)))
            metrics = sm.receiver_metrics(archive)
            self.assertEqual(metrics["datalake_receiver_raw_chunks"], 40)
            self.assertEqual(metrics["datalake_receiver_raw_bytes"], 8000)
            self.assertEqual(metrics["datalake_receiver_decoded_rows_total"], 30)
            self.assertEqual(metrics["datalake_receiver_acked_rows_total"], 28)
            # Receipt vs decode vs commit stays distinguishable.
            self.assertGreater(metrics["datalake_receiver_last_decode_timestamp_seconds"],
                               metrics["datalake_receiver_last_receive_timestamp_seconds"])
            # Absent full-ACK row is unknown, never a fake epoch.
            self.assertNotIn("datalake_receiver_last_full_ack_timestamp_seconds", metrics)
            self.assertNotIn("bogus_metric", " ".join(metrics))

    def test_malformed_archive_meta_is_unknown_not_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 5, 6),), meta=(
                ("raw_chunks", 40), ("last_receive_ns", "not-an-int")))
            self.assertEqual(sm.receiver_metrics(archive),
                             {"datalake_receiver_archive_known": 0,
                              "datalake_receiver_archive_success": 0})

    def test_negative_known_counter_is_unknown_not_omitted_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 5, 6),), meta=(
                ("raw_chunks", -1), ("raw_bytes", 8000)))
            self.assertEqual(sm.receiver_metrics(archive),
                             {"datalake_receiver_archive_known": 0,
                              "datalake_receiver_archive_success": 0})

    def test_invalid_known_error_count_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, errors=(
                ("greptime_timeout", -4, 1_700_000_000_000_000_000),))
            self.assertEqual(sm.receiver_metrics(archive),
                             {"datalake_receiver_archive_known": 0,
                              "datalake_receiver_archive_success": 0})

    def test_outbox_rows_counter_preferred_over_table_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 5, 6), ("e2", 7, 8)), meta=(
                ("outbox_rows", 2), ("raw_chunks", 9), ("raw_bytes", 100)))
            metrics = sm.receiver_metrics(archive)
            self.assertEqual(metrics["datalake_receiver_pending_rows"], 2)
            self.assertEqual(metrics["datalake_receiver_oldest_pending_id"], 1)

    def test_wal_metric_is_exact_wal_file_only(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory)
            live = sqlite3.connect(archive)
            try:
                live.execute("PRAGMA journal_mode=WAL")
                live.execute("INSERT INTO sessions(vehicle,collector_id,session_id,meta_json)"
                             " VALUES(?,?,?,?)", ("w", "c", "wal-snapshot", "{}"))
                live.commit()
                live.execute("INSERT INTO sessions(vehicle,collector_id,session_id,meta_json)"
                             " VALUES(?,?,?,?)", ("w", "c", "wal-uncheckpointed", "{}"))
                # No commit/checkpoint: the row sits in the real -wal while the
                # read-only snapshot below must still succeed.
                metrics = sm.receiver_metrics(archive)
                if Path(archive + "-wal").exists():
                    # Exact -wal bytes, never mixed with -shm.
                    self.assertEqual(metrics["datalake_receiver_wal_bytes"],
                                     Path(archive + "-wal").stat().st_size)
                else:
                    self.assertEqual(metrics["datalake_receiver_wal_bytes"], 0)
                if Path(archive + "-shm").exists():
                    self.assertEqual(metrics["datalake_receiver_shm_bytes"],
                                     Path(archive + "-shm").stat().st_size)
            finally:
                live.close()
        with tempfile.TemporaryDirectory() as directory:
            absent = sm.receiver_metrics(make_archive(directory))
            self.assertEqual(absent["datalake_receiver_wal_bytes"], 0)
            self.assertNotIn("datalake_receiver_shm_bytes", absent)

    def test_databases_without_counter_tables_publish_only_direct_facts(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(make_archive(directory, rows=(("e1", 5, 6),)))
            self.assertEqual(metrics["datalake_receiver_sessions"], 1)
            self.assertGreaterEqual(metrics["datalake_receiver_db_bytes"], 0)
            self.assertGreaterEqual(metrics["datalake_receiver_wal_bytes"], 0)
            self.assertGreater(metrics["datalake_receiver_disk_free_bytes"], 0)
            self.assertNotIn("datalake_receiver_disk_reserve_bytes", metrics)

    def test_reserve_from_env_and_explicit_argument(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory)
            os.environ["CAN_RECEIVER_DISK_RESERVE_BYTES"] = "67108864"
            self.assertEqual(sm.receiver_metrics(archive)["datalake_receiver_disk_reserve_bytes"], 67108864)
            self.assertEqual(sm.receiver_metrics(archive, reserve_bytes=7)["datalake_receiver_disk_reserve_bytes"], 7)
            os.environ["CAN_RECEIVER_DISK_RESERVE_BYTES"] = "not-a-number"
            self.assertNotIn("datalake_receiver_disk_reserve_bytes", sm.receiver_metrics(archive))


if __name__ == "__main__":
    raise SystemExit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)
