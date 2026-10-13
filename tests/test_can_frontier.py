"""Issue #9: bounded pending frontier, durable head_id, CPU-only deadline.

No decoder/output changes: every case drains Archive.decode_once to
completion (or asserts bounded single-call progress) and compares ordered
outbox rows, cursors, partials, pending membership/head order and counters
against a sequential oracle or a restarted reopen of the same archive.
"""
import contextlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from scripts.ingest.can.can_receiver import Archive, candidate_sql
from tests.test_can_receiver import synthetic_decoder


META = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
        "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}
FRAME = b"t12320200\r"


def vm_steps(conn, sql, params):
    steps = {"n": 0}

    def counter():
        steps["n"] += 1
        return False

    conn.set_progress_handler(counter, 1)
    try:
        conn.execute(sql, params).fetchall()
    finally:
        conn.set_progress_handler(None, 0)
    return steps["n"]


def drain(archive, decoder):
    total = 0
    while True:
        progressed = archive.decode_once(decoder)
        if not progressed:
            break
        total += progressed
    return total


def snapshot(conn, epoch):
    return {
        "outbox": [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")],
        "cursors": sorted(tuple(r) for r in conn.execute(
            "SELECT session,next_seq,state_json,counts_json,rows FROM decode_states WHERE epoch=?",
            (epoch,)).fetchall()),
        "partials": sorted(tuple(r) for r in conn.execute(
            "SELECT session,seq,state_json,counts_json,rows_emitted FROM decode_partial WHERE epoch=?",
            (epoch,)).fetchall()),
        "pending": sorted(tuple(r) for r in conn.execute(
            "SELECT session,head_id FROM decode_pending WHERE epoch=?", (epoch,)).fetchall()),
        "meta": dict(conn.execute("SELECT key,value FROM archive_meta").fetchall()),
    }


class PendingFrontier(unittest.TestCase):
    def test_small_limit_avoids_full_pending_materialization(self):
        """limit=1 must read one frontier head: bounded Python + SQL work."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            for i in range(30):
                archive.accept(dict(META, session_id="s-%02d" % i),
                               [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            with archive.connect() as conn:
                heads = conn.execute(candidate_sql(1), (decoder.epoch,)).fetchall()
                self.assertEqual(len(heads), 1)
                full = len(conn.execute(candidate_sql(), (decoder.epoch,)).fetchall())
                self.assertEqual(full, 30)
                bounded = vm_steps(conn, candidate_sql(1), (decoder.epoch,))
                unbounded = vm_steps(conn, candidate_sql(), (decoder.epoch,))
                self.assertLess(bounded, unbounded)
                plan = [list(r) for r in conn.execute("EXPLAIN QUERY PLAN " + candidate_sql(1), (decoder.epoch,))]
                self.assertFalse(any("TEMP B-TREE" in " ".join(map(str, row)) for row in plan))
            # One call commits exactly the globally oldest chunk.
            self.assertEqual(archive.decode_once(decoder, limit=1), 1)
            with archive.connect() as conn:
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM decode_pending WHERE epoch=?", (decoder.epoch,)).fetchone()[0], 29)

    def test_delayed_read_still_commits_full_prefix(self):
        """CPU deadline starts after the read snapshot: slow read != 1-chunk commit."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            for i in range(4):
                archive.accept(dict(META, session_id="d-%d" % i),
                               [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])

            class _DelayedConnection(sqlite3.Connection):
                def execute(self, sql, *params):
                    import time as _time
                    _time.sleep(0.06)
                    return super().execute(sql, *params)

            def delayed_connect():
                conn = sqlite3.connect(archive.path, timeout=5, factory=_DelayedConnection)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("PRAGMA foreign_keys=ON")
                return contextlib.closing(conn)

            with patch.object(archive, "connect", delayed_connect):
                self.assertEqual(archive.decode_once(decoder, limit=4), 4)
            with archive.connect() as conn:
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM decode_pending WHERE epoch=?", (decoder.epoch,)).fetchone()[0], 0)

    def test_new_session_epoch_and_cas_rollback_keep_frontier(self):
        """New arrivals/epochs advance heads; CAS failure commits nothing."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            meta = dict(META, session_id="n-0")
            archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            archive.accept(meta, [{"seq": 1, "offset_ns": 1, "phase": "capture", "data": FRAME}])
            with archive.connect() as conn:
                row = conn.execute(
                    "SELECT head_id FROM decode_pending WHERE epoch=? AND session="
                    "(SELECT id FROM sessions WHERE session_id=?)", (decoder.epoch, "n-0")).fetchone()
                first = conn.execute(
                    "SELECT id FROM raw_chunks WHERE session="
                    "(SELECT id FROM sessions WHERE session_id=?) AND seq=0", ("n-0",)).fetchone()
                self.assertEqual(row["head_id"], first["id"])
            replay = synthetic_decoder(directory, revision="synthetic-v2")
            archive.register_epoch(replay, explicit=True)
            with archive.connect() as conn:
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM decode_pending WHERE epoch=?", (replay.epoch,)).fetchone()[0], 1)
            # Real race: a competitor commits a cursor advance after our
            # snapshot but before our commit (injected inside decode_some,
            # while the writer lock is released for CPU work). Our commit CAS
            # must reject; the competitor's state stays committed, ours rolls
            # back entirely.
            with archive.connect() as conn:
                racing = snapshot(conn, decoder.epoch)
                victim = conn.execute(
                    "SELECT session FROM decode_pending WHERE epoch=? LIMIT 1", (decoder.epoch,)).fetchone()
                victim_session = victim["session"] if victim else None
            real_decode_some = decoder.decode_some

            def racing_decode_some(meta_arg, chunk, state, budget):
                if victim_session is not None:
                    with archive.connect() as conn:
                        conn.execute("BEGIN IMMEDIATE")
                        conn.execute("INSERT INTO decode_states VALUES(?,?,?,?,?,?) "
                                     "ON CONFLICT(session,epoch) DO UPDATE SET next_seq=excluded.next_seq,"
                                     "state_json=excluded.state_json,counts_json=excluded.counts_json,"
                                     "rows=excluded.rows",
                                     (victim_session, decoder.epoch, 9999, "{}", "{}", 0))
                        conn.commit()
                return real_decode_some(meta_arg, chunk, state, budget)

            with patch.object(decoder, "decode_some", racing_decode_some):
                with self.assertRaisesRegex(ValueError, "decode cursor moved during decode"):
                    archive.decode_once(decoder, limit=1)
            with archive.connect() as conn:
                after = snapshot(conn, decoder.epoch)
                # Only the competitor's cursor row differs; outbox, partials,
                # pending membership and counters are untouched by our abort.
                self.assertEqual(after["outbox"], racing["outbox"])
                self.assertEqual(after["partials"], racing["partials"])
                self.assertEqual(after["pending"], racing["pending"])
                self.assertEqual(after["meta"], racing["meta"])
                raced = [r for r in after["cursors"] if r[0] == victim_session]
                self.assertEqual(len(raced), 1)
                self.assertEqual(raced[0][1], 9999)

    def test_restart_history_and_partial_preserve_order(self):
        """Reopen + retained completed history + partial resume keep exact rows/state."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            done = [dict(META, session_id="hist-%d" % i) for i in range(3)]
            for meta in done:
                archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            drain(archive, decoder)
            live_a, live_b = dict(META, session_id="live-a"), dict(META, session_id="live-b")
            arrival = ((live_a, {"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME[:6]}),
                       (live_b, {"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}),
                       (live_a, {"seq": 1, "offset_ns": 1, "phase": "capture", "data": FRAME[6:]}))
            # Frozen ingest clock so canonical rows (incl. ingest_time) match exactly.
            # Rows take ingest_time from the decoder's clock; the archive's own
            # clock covers ACK/commit markers. Freeze both for determinism.
            with patch("scripts.ingest.can.can_decoder.time.time_ns", return_value=1800000000000000001), \
                    patch("scripts.ingest.can.can_receiver.time.time_ns", return_value=1800000000000000001):
                for meta, chunk in arrival:
                    archive.accept(meta, [chunk])
                reopened = Archive(path, disk_reserve_bytes=0)
                drain(reopened, decoder)
                # Sequential oracle under the same frozen clock: ingest_time and
                # resume envelope identities match exactly; values/state keep
                # full equality in the comparisons below.
                states, totals, want = {}, {}, []
                for meta, chunk in arrival:
                    rows, state, counts = decoder.decode(meta, chunk, states.get(meta["session_id"]))
                    states[meta["session_id"]] = state
                    own = totals.setdefault(meta["session_id"], {})
                    for key, value in counts.items():
                        own[key] = value if key == "tail_bytes" else own.get(key, 0) + value
                    want.extend(rows)
            with reopened.connect() as conn:
                got = snapshot(conn, decoder.epoch)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_pending").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_partial").fetchone()[0], 0)
                self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")
                self.assertEqual(conn.execute("PRAGMA synchronous").fetchone()[0], 2)
            got_rows = [json.loads(r) for r in got["outbox"][-len(want):]]
            self.assertEqual(got_rows, want)
            self.assertEqual(len({r["event_id"] for r in got_rows}), len(got_rows))
            with reopened.connect() as conn:
                for row in conn.execute(
                        "SELECT s.session_id,d.state_json,d.counts_json,d.rows FROM decode_states d "
                        "JOIN sessions s ON s.id=d.session WHERE d.epoch=? AND s.session_id LIKE 'live-%'",
                        (decoder.epoch,)):
                    self.assertEqual(json.loads(row["state_json"]), states[row["session_id"]])
                    self.assertEqual(json.loads(row["counts_json"]), totals[row["session_id"]])
                    self.assertEqual(row["rows"], totals[row["session_id"]]["rows"])

    def test_legacy_migration_repairs_null_heads_and_rebuild(self):
        """Pre-head_id archives backfill the frontier; rebuild is idempotent."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            archive.accept(dict(META, session_id="leg-0"),
                           [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            with archive.connect() as conn:
                conn.execute("DROP INDEX IF EXISTS decode_pending_frontier")
                conn.execute("ALTER TABLE decode_pending DROP COLUMN head_id")
                conn.execute("DELETE FROM archive_meta WHERE key='pending_heads_v1'")
                conn.commit()
            repaired = Archive(path, disk_reserve_bytes=0)
            with repaired.connect() as conn:
                row = conn.execute("SELECT head_id FROM decode_pending WHERE epoch=?",
                                   (decoder.epoch,)).fetchone()
                self.assertIsNotNone(row["head_id"])
                self.assertEqual(repaired.decode_once(decoder, limit=1), 1)
                before = sorted(tuple(r) for r in conn.execute("SELECT session,epoch,head_id FROM decode_pending").fetchall())
            repaired.rebuild_pending()
            with repaired.connect() as conn:
                after = sorted(tuple(r) for r in conn.execute("SELECT session,epoch,head_id FROM decode_pending").fetchall())
                self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
