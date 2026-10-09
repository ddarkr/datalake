"""Dedicated #8 demand-driven prefetch regression; synthetic only.

Run: python -m unittest tests.test_can_prefetch -v
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.ingest.can.can_receiver import Archive, DECODE_PAGE_ROWS
from tests.test_can_receiver import synthetic_decoder

FRAME = b"t12320200\r"
BASE = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
        "started_ns": 1800000000000000000, "vehicle_firmware": "synthetic"}




def drain(archive, decoder, **kwargs):
    with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
        while archive.decode_once(decoder, **kwargs):
            pass


def full_snapshot(archive, decoder):
    with archive.connect() as conn:
        outbox = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
        states = [dict(r) for r in conn.execute(
            "SELECT s.session_id, d.next_seq, d.state_json, d.counts_json, d.rows"
            " FROM decode_states d JOIN sessions s ON s.id=d.session"
            " WHERE d.epoch=? ORDER BY s.session_id", (decoder.epoch,))]
        partial = [dict(r) for r in conn.execute(
            "SELECT s.session_id, d.seq, d.state_json, d.counts_json, d.rows_emitted"
            " FROM decode_partial d JOIN sessions s ON s.id=d.session"
            " WHERE d.epoch=? ORDER BY s.session_id", (decoder.epoch,))]
        counters = dict(conn.execute("SELECT key,value FROM archive_meta").fetchall())
        raw = conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0]
    return {"outbox": outbox, "states": states, "partial": partial,
            "counters": counters, "raw": raw}


class DemandPrefetch(unittest.TestCase):
    def test_roundrobin_fetches_demand_not_1000_for_16(self):
        """100x64 round-robin: bounded amplification, shrinking window."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            metas = {k: dict(BASE, session_id="rr-%d" % k) for k in range(100)}
            with patch("scripts.ingest.can.can_receiver.time.time_ns",
                        return_value=1800000000000000001):
                for seq in range(64):
                    for k in range(100):
                        archive.accept(metas[k], [{"seq": seq, "offset_ns": seq * 1000 + k,
                                                   "phase": "capture", "data": FRAME}])
            with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                first = archive.decode_once(decoder, limit=1000)
                stats = dict(archive._decode_prefetch_stats)
            # Demand sizing fetches fair-share pages capped at the 1000 window
            # (vs old 64/session speculative pages); everything fetched stages.
            self.assertEqual(stats["fetched_chunks"], 1000)
            self.assertEqual(first, stats["prefix_chunks"])
            self.assertEqual(first, stats["staged_chunks"])
            # Drain fully: overall amplification stays ~1x (old shape ~31x),
            # with a bounded call count instead of hundreds of mini-commits.
            total_fetched = stats["fetched_chunks"]
            total_staged = first
            calls = 1
            while True:
                with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                    n = archive.decode_once(decoder, limit=1000)
                if not n:
                    break
                total_fetched += archive._decode_prefetch_stats["fetched_chunks"]
                total_staged += n
                calls += 1
            self.assertEqual(total_staged, 6400)
            self.assertLessEqual(total_fetched / total_staged, 2.0)
            self.assertLessEqual(calls, 120)

    def test_many_session_fetched_cap_shrinks_window(self):
        """400-session round-robin: fetched-beyond-staged waste shrinks hint."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            metas = {k: dict(BASE, session_id="wide-%d" % k) for k in range(400)}
            with patch("scripts.ingest.can.can_receiver.time.time_ns",
                        return_value=1800000000000000001):
                for seq in range(10):
                    for k in range(400):
                        archive.accept(metas[k], [{"seq": seq, "offset_ns": seq * 1000 + k,
                                                   "phase": "capture", "data": FRAME}])
            with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                first = archive.decode_once(decoder, limit=1000)
                stats = dict(archive._decode_prefetch_stats)
            # ceil(1000/400)=3-row pages hit the fetched cap mid-prefix: more
            # fetched than staged, so the window must shrink below 1000.
            self.assertGreater(stats["fetched_chunks"], first)
            self.assertLess(archive._decode_prefetch_limit, 1000)

    def test_contiguous_single_session_keeps_full_pages(self):
        """A lone contiguous session still gets full 64-row pages."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            meta = dict(BASE, session_id="contig")
            with patch("scripts.ingest.can.can_receiver.time.time_ns",
                        return_value=1800000000000000001):
                archive.accept(meta, [{"seq": i, "offset_ns": i, "phase": "capture",
                                       "data": FRAME} for i in range(200)])
            with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                n = archive.decode_once(decoder, limit=200)
                stats = dict(archive._decode_prefetch_stats)
            self.assertEqual(n, 200)
            # ceil(200/64) = 4 page queries + 1 frontier + 2 cursor + refills.
            self.assertLessEqual(stats["read_queries"], 1 + 4 + 2 + 200)
            self.assertEqual(stats["fetched_chunks"], 200)

    def test_boundary_order_partial_cas_restart(self):
        """Page/byte boundaries, partial resume, new session, CAS, restart."""
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            path = Path(directory) / "raw.sqlite"
            archive = Archive(path, disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            metas = {name: dict(BASE, session_id=name) for name in ("a", "b", "dense")}
            a = [{"seq": i, "offset_ns": i, "phase": "capture", "data": FRAME}
                 for i in range(DECODE_PAGE_ROWS + 16)]
            arrival = [("a", c) for c in a] + [
                ("b", {"seq": 0, "offset_ns": 1000, "phase": "capture", "data": FRAME})]
            dense = [{"seq": i, "offset_ns": i, "phase": "capture", "data": payload}
                     for i, payload in enumerate((FRAME * 3, FRAME * 6000, FRAME))]
            arrival.extend(("dense", c) for c in dense)
            with patch("scripts.ingest.can.can_receiver.time.time_ns",
                        return_value=1800000000000000001):
                for name, c in arrival:
                    archive.accept(metas[name], [c])
                drain(archive, decoder)
                expected, states, totals = [], {}, {}
                for name, c in arrival:
                    rows, state, counts = decoder.decode(metas[name], c, states.get(name))
                    expected.extend(rows)
                    states[name] = state
                    own = totals.setdefault(name, {})
                    for key, value in counts.items():
                        own[key] = value if key == "tail_bytes" else own.get(key, 0) + value
            with archive.connect() as conn:
                actual = [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
                self.assertEqual([json.loads(r) for r in actual], expected)
                for row in conn.execute(
                        "SELECT s.session_id,d.state_json,d.counts_json,d.rows FROM decode_states d "
                        "JOIN sessions s ON s.id=d.session WHERE d.epoch=?", (decoder.epoch,)):
                    self.assertEqual(json.loads(row[1]), states[row[0]])
                    self.assertEqual(json.loads(row[2]), totals[row[0]])
                    self.assertEqual(row[3], totals[row[0]]["rows"])
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM decode_partial").fetchone()[0], 0)
            # New session arrival after drain decodes exactly once.
            before = full_snapshot(archive, decoder)
            with patch("scripts.ingest.can.can_receiver.time.time_ns",
                        return_value=1800000000000000002):
                archive.accept(dict(BASE, session_id="late"),
                               [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": FRAME}])
            with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                self.assertEqual(archive.decode_once(decoder), 1)
            after = full_snapshot(archive, decoder)
            self.assertEqual(after["raw"], before["raw"] + 1)
            self.assertEqual(len(after["outbox"]), len(before["outbox"]) + 1)
            # CAS: concurrent cursor move rolls the batch back atomically.
            archive.accept(dict(BASE, session_id="late"),
                           [{"seq": 1, "offset_ns": 1, "phase": "capture", "data": FRAME}])
            status_before = full_snapshot(archive, decoder)
            real = decoder.decode_some

            def move_cursor(meta, chunk, state, budget):
                rows, state, counts, done = real(meta, chunk, state, budget)
                with archive.connect() as conn:
                    conn.execute("UPDATE decode_states SET next_seq=99 WHERE session="
                                 "(SELECT id FROM sessions WHERE session_id='late') AND epoch=?",
                                 (decoder.epoch,))
                    conn.commit()
                return rows, state, counts, done
            with patch.object(decoder, "decode_some", move_cursor):
                with self.assertRaisesRegex(ValueError, "decode cursor moved during decode"):
                    archive.decode_once(decoder, limit=1)
            concurrent_state = {**status_before, "states": [
                {**row, "next_seq": 99} if row["session_id"] == "late" else row
                for row in status_before["states"]]}
            self.assertEqual(full_snapshot(archive, decoder), concurrent_state)
            # Undo only the fixture's committed cursor tamper before restart.
            with archive.connect() as conn:
                conn.execute("UPDATE decode_states SET next_seq=? WHERE session="
                             "(SELECT id FROM sessions WHERE session_id='late') AND epoch=?",
                             (next(row["next_seq"] for row in status_before["states"]
                                   if row["session_id"] == "late"), decoder.epoch))
                conn.commit()
            # Restart: reopened archive resumes and drains identically.
            reopened = Archive(path, disk_reserve_bytes=0)
            with patch("scripts.ingest.can.can_receiver.time.time_ns",
                       return_value=1800000000000000003):
                with patch("scripts.ingest.can.can_receiver.time.monotonic", return_value=0):
                    while reopened.decode_once(decoder):
                        pass
                prior_state = json.loads(next(row["state_json"] for row in
                                              status_before["states"] if row["session_id"] == "late"))
                rows, _, _ = decoder.decode(dict(BASE, session_id="late"),
                                             {"seq": 1, "offset_ns": 1,
                                              "phase": "capture", "data": FRAME}, prior_state)
            with reopened.connect() as conn:
                tail = [json.loads(r[0]) for r in
                        conn.execute("SELECT row_json FROM outbox ORDER BY id")]
            self.assertEqual(tail[-len(rows):], rows)



if __name__ == "__main__":
    unittest.main()
