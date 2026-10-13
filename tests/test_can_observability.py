"""Issue #12 metrics core: cheap default status, exact durable counters, bounded stage stats.

Uses the real Archive/Decoder (synthetic definitions only) and compares the
trigger-maintained decode_epoch_state table against durable tables after
accept/decode/flush/restart/rollback/epoch partial progress. No fake
producer, no decode echoes.
"""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.ingest.can.can_receiver import (
    Archive,
    ERROR_KINDS,
    StageStats,
    _stage_quantiles,
    archive_status,
    classify_error,
)
from scripts.storage import storage_metrics as sm
from tests.test_can_receiver import synthetic_decoder


def _frame(byte=b"t12320200\r"):
    return byte


class EpochStatusTest(unittest.TestCase):
    def _archive(self, directory):
        decoder = synthetic_decoder(directory)
        archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
        archive.register_epoch(decoder)
        return archive, decoder

    def _durable_expectations(self, archive, epoch):
        with archive.connect() as conn:
            raw = conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0]
            processed = conn.execute(
                "SELECT COALESCE(SUM(next_seq),0) FROM decode_states WHERE epoch=?", (epoch,)).fetchone()[0]
            decoded = conn.execute(
                "SELECT COALESCE(SUM(rows),0) FROM decode_states WHERE epoch=?", (epoch,)).fetchone()[0]
            partial = conn.execute(
                "SELECT COALESCE(SUM(rows_emitted),0) FROM decode_partial WHERE epoch=?", (epoch,)).fetchone()[0]
            pending_sessions = conn.execute(
                "SELECT COUNT(*) FROM decode_pending WHERE epoch=?", (epoch,)).fetchone()[0]
            state = conn.execute(
                "SELECT processed_chunks,decoded_rows,partial_rows,pending_sessions"
                " FROM decode_epoch_state WHERE epoch=?", (epoch,)).fetchone()
        return raw, processed, decoded, partial, pending_sessions, tuple(state)

    def test_active_retained_backlog_and_partial_exactness(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "active-session", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        with tempfile.TemporaryDirectory() as directory:
            archive, decoder = self._archive(directory)
            chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": _frame()}
                      for seq in range(3)]
            archive.accept(meta, chunks)
            archive.decode_once(decoder, limit=1)
            raw, processed, decoded, partial, pending_sessions, state = self._durable_expectations(archive, decoder.epoch)
            self.assertEqual(state, (processed, decoded, partial, pending_sessions))
            facts = archive.status()
            entry = facts["epochs"][0]
            self.assertTrue(entry["active_epoch"])
            self.assertEqual(entry["active_backlog_chunks"], raw - processed)
            self.assertEqual(entry["retained_backlog_chunks"], 0)
            self.assertEqual(entry["decoded_rows_exact"], decoded + partial)
            # Real dense partial progress: a >budget chunk leaves a durable
            # decoder partial row (emitted rows count, cursor unmoved).
            while archive.decode_once(decoder):
                pass
            raw, processed, decoded, partial, pending_sessions, state = self._durable_expectations(archive, decoder.epoch)
            self.assertEqual(state, (processed, decoded, partial, pending_sessions))
            archive.accept(meta, [{"seq": 3, "offset_ns": 3, "phase": "capture", "data": _frame() * 2200}])
            archive.decode_once(decoder, limit=1)
            raw, processed, decoded, partial, pending_sessions, state = self._durable_expectations(archive, decoder.epoch)
            self.assertEqual(state, (processed, decoded, partial, pending_sessions))
            self.assertGreater(partial, 0)
            facts = archive.status()
            entry = facts["epochs"][0]
            self.assertEqual(entry["processed_chunks"], processed)
            self.assertEqual(entry["decoded_rows_exact"], decoded + partial)
            # Finishing the chunk deletes the partial row: no double count.
            while archive.decode_once(decoder):
                pass
            raw, processed, decoded, partial, pending_sessions, state = self._durable_expectations(archive, decoder.epoch)
            self.assertEqual(state, (processed, decoded, partial, pending_sessions))
            self.assertEqual(archive.status()["epochs"][0]["decoded_rows_exact"], decoded + partial)
            # New epoch retains the old one: active vs retained backlog splits.
            replay = synthetic_decoder(directory, revision="synthetic-v2")
            archive.register_epoch(replay, explicit=True)
            archive.accept(dict(meta, session_id="retained-session"),
                           [{"seq": 0, "offset_ns": 9, "phase": "capture", "data": _frame()}])
            facts = archive.status()
            by_epoch = {entry["epoch"]: entry for entry in facts["epochs"]}
            self.assertTrue(by_epoch[replay.epoch]["active_epoch"])
            self.assertFalse(by_epoch[decoder.epoch]["active_epoch"])
            self.assertEqual(by_epoch[replay.epoch]["retained_backlog_chunks"], 0)
            self.assertGreaterEqual(by_epoch[decoder.epoch]["retained_backlog_chunks"], 0)
            self.assertEqual(sum(entry["remaining_chunks"] for entry in facts["epochs"]),
                             facts["backlog_chunks"])
            # Reselecting the older decoder moves the explicit active label back.
            archive.register_epoch(decoder)
            reselected = archive.status()
            by_epoch = {entry["epoch"]: entry for entry in reselected["epochs"]}
            self.assertTrue(by_epoch[decoder.epoch]["active_epoch"])
            self.assertFalse(by_epoch[replay.epoch]["active_epoch"])
            # Restart keeps trigger state exact.
            reopened = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            self.assertEqual(reopened.status()["epochs"], reselected["epochs"])

    def test_migration_includes_partial_rows(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "legacy-session", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": _frame()}])
            archive.decode_once(decoder, limit=1)
            with archive.connect() as conn:
                conn.execute("INSERT INTO decode_partial(session,epoch,seq,state_json,counts_json,rows_emitted)"
                             " VALUES((SELECT id FROM sessions WHERE session_id='legacy-session'),?,?,?,?,?)",
                             (decoder.epoch, 0, "{}", "{}", 4))
                conn.execute("DELETE FROM decode_epoch_state")
                conn.execute("DELETE FROM archive_meta WHERE key='epoch_state_migrated'")
                conn.commit()
            reopened = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            with reopened.connect() as conn:
                decoded = conn.execute("SELECT COALESCE(SUM(rows),0) FROM decode_states WHERE epoch=?",
                                       (decoder.epoch,)).fetchone()[0]
                row = conn.execute("SELECT decoded_rows,partial_rows FROM decode_epoch_state WHERE epoch=?",
                                   (decoder.epoch,)).fetchone()
            self.assertEqual(tuple(row), (decoded, 4))

    def test_legacy_decoded_total_repaired_once_with_ack_history_kept(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "old-build-session", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": _frame()}])
            archive.decode_once(decoder, limit=1)
            with archive.connect() as conn:
                acked = conn.execute("SELECT value FROM archive_meta WHERE key='acked_rows_total'").fetchone()[0]
                states = conn.execute("SELECT COALESCE(SUM(rows),0) FROM decode_states").fetchone()[0]
                # Simulate the old build: counter seeded from states only while a
                # partial row held emitted rows, with the raw marker already set.
                conn.execute("INSERT INTO decode_partial(session,epoch,seq,state_json,counts_json,rows_emitted)"
                             " VALUES((SELECT id FROM sessions WHERE session_id='old-build-session'),?,?,?,?,?)",
                             (decoder.epoch, 0, "{}", "{}", 3))
                conn.execute("UPDATE archive_meta SET value=? WHERE key='decoded_rows_total'", (states,))
                conn.execute("DELETE FROM archive_meta WHERE key='decoded_total_v2'")
                conn.commit()
            reopened = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            with reopened.connect() as conn:
                total = conn.execute("SELECT value FROM archive_meta WHERE key='decoded_rows_total'").fetchone()[0]
                marker = conn.execute("SELECT value FROM archive_meta WHERE key='decoded_total_v2'").fetchone()[0]
                kept = conn.execute("SELECT value FROM archive_meta WHERE key='acked_rows_total'").fetchone()[0]
            self.assertEqual(total, states + 3)
            self.assertEqual(marker, 1)
            self.assertEqual(kept, acked)
            # Rollback leaves neither the repair nor a half marker.
            with reopened.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("UPDATE archive_meta SET value=0 WHERE key='decoded_rows_total'")
                conn.execute("DELETE FROM archive_meta WHERE key='decoded_total_v2'")
                conn.rollback()
            # Reopen is idempotent: second open is a marker probe, total unchanged.
            again = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            with again.connect() as conn:
                total2 = conn.execute("SELECT value FROM archive_meta WHERE key='decoded_rows_total'").fetchone()[0]
                marker2 = conn.execute("SELECT value FROM archive_meta WHERE key='decoded_total_v2'").fetchone()[0]
            self.assertEqual((total2, marker2), (total, 1))


class StageAndErrorTest(unittest.TestCase):
    def test_stage_quantiles_and_bounds(self):
        self.assertEqual(_stage_quantiles([]), {"count": 0})
        summary = _stage_quantiles([1.0, 2.0, 3.0, 4.0])
        self.assertEqual((summary["count"], summary["p50"], summary["p95"], summary["p99"]), (4, 2.0, 4.0, 4.0))
        stats = StageStats(enabled=True)
        for _ in range(600):
            stats.observe("decode_cpu", 0.001)
        summary = stats.summary()["decode_cpu"]
        self.assertEqual(summary["count"], 512)
        self.assertGreaterEqual(summary["dropped"], 88)
        self.assertEqual(set(summary) - {"count", "p50", "p95", "p99", "dropped"}, set())
        stats.reset()
        self.assertEqual(stats.summary(), {})
        disabled = StageStats(enabled=False)
        disabled.observe("decode_cpu", 1.0)
        self.assertEqual(disabled.summary(), {})

    def test_fixed_error_taxonomy_never_leaks_values(self):
        for kind in ERROR_KINDS:
            self.assertNotIn("vehicle", kind)
        self.assertIn("decode_failure", ERROR_KINDS)
        self.assertIn("sqlite_locked", ERROR_KINDS)
        self.assertIn("canonical_serialize_failure", ERROR_KINDS)
        self.assertEqual(classify_error(ValueError("vehicle=secret-session payload-bytes")), "decode_failure")
        self.assertEqual(classify_error(ValueError("decode cursor moved during decode")), "decode_state_conflict")
        self.assertEqual(classify_error(ValueError("decode partial moved during decode")), "decode_partial_conflict")
        from scripts.ingest.can.can_receiver import DownstreamError
        self.assertEqual(classify_error(DownstreamError("future_unknown_code")), "greptime_failure")
        self.assertEqual(classify_error(DownstreamError("greptime_timeout")), "greptime_timeout")

    def test_worker_unknown_downstream_survives_as_greptime_failure(self):
        from scripts.ingest.can.can_receiver import DownstreamError, Worker
        import threading
        import time
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                    "session_id": "boom-session", "started_ns": 1800000000000000000,
                    "vehicle_firmware": "synthetic"}
            archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"t12320200\r"}])
            archive.decode_once(decoder)
            worker = Worker(archive, decoder, None, interval=0.01, retry_interval=0.01)
            def boom(payload):
                raise DownstreamError("future_unknown_code")
            worker.greptime = type("BoomSink", (), {"send_body": staticmethod(boom),
                                                    "max_body_bytes": 1 << 20})()
            thread = threading.Thread(target=worker._upload, daemon=True)
            thread.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if "greptime_failure" in archive.status()["errors"]:
                    break
            worker.stop_event.set()
            thread.join(5)
            self.assertIn("greptime_failure", archive.status()["errors"])
            self.assertFalse(thread.is_alive())  # uploader survived the unknown code, then exited cleanly


class ExporterEpochTest(unittest.TestCase):
    def test_active_retained_labels_without_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                    "session_id": "export-session", "started_ns": 1800000000000000000,
                    "vehicle_firmware": "synthetic"}
            archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"t12320200\r"}])
            metrics = sm.receiver_metrics(str(archive.path))
            active = metrics['datalake_receiver_epoch_remaining_chunks{scope="active"}']
            self.assertEqual(metrics['datalake_receiver_epoch_retained_backlog_chunks{scope="active"}'], 0)
            self.assertEqual(metrics['datalake_receiver_epoch_active_backlog_chunks{scope="retained"}'], 0)
            self.assertEqual(active, 1)
            blob = json.dumps(metrics)
            self.assertNotIn("synthetic", blob)
            self.assertNotIn(decoder.epoch, blob)
            self.assertNotIn("export-session", blob)


if __name__ == "__main__":
    raise SystemExit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)
