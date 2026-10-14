"""Parent-runnable default-status scaling probe: raw/session AND epoch growth.

Builds real Archive/Decoder fixtures with synthetic definitions only, then
measures the ACTUAL read-only archive_status() path: a sqlite3.Connection
factory subclass installs a progress handler on every real connection the
status path opens (it uses sqlite3.connect directly, not Archive.connect),
recording observed VM steps; a separate uninstrumented pass records wall
latency. Fails (exit 2) when no step samples are observed — never a zero
fallback. A separate dense signal-partial cell asserts a live decode_partial
row exists. Matrix covers raw 1k/10k/100k x sessions 100/1k/10k x epochs
1/4/16 (feasible subset, raw >= sessions); counters via production
accept/decode txns.
Prints JSON: {"cells": [...]}.
"""
import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, ".")
import scripts.ingest.can.can_receiver as receiver
from scripts.ingest.can.can_receiver import Archive
from tests.test_can_receiver import synthetic_decoder

FRAME = b"t12320200\r"
CTRL_FRAME = b"z\r"
_real_connect = sqlite3.connect
_live_conns = []


class _CountingConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        steps = [0]

        def budget():
            steps[0] += 1
            return False

        self.set_progress_handler(budget, 1)
        self._probe_steps = steps
        _live_conns.append(self)


def _instrumented_connect(*args, **kwargs):
    kwargs["factory"] = _CountingConnection
    return _real_connect(*args, **kwargs)


def measure_status(path, repeats=3):
    latencies = []
    del _live_conns[:]
    orig = receiver.sqlite3.connect
    receiver.sqlite3.connect = _instrumented_connect
    try:
        for _ in range(repeats):
            facts = receiver.archive_status(str(path))
            assert facts, "status returned empty"
    finally:
        receiver.sqlite3.connect = orig
    steps_seen = [conn._probe_steps[0] for conn in _live_conns]
    del _live_conns[:]
    if not steps_seen or max(steps_seen) <= 0:
        print(json.dumps({"error": "no step samples observed"}))
        raise SystemExit(2)
    for _ in range(repeats):
        start = time.monotonic()
        receiver.archive_status(str(path))
        latencies.append(time.monotonic() - start)
    latencies.sort()
    return {"vm_steps": max(steps_seen), "latency_p50": latencies[len(latencies) // 2]}


def build(directory, sessions, chunks_per_session, epochs, frame=None, drain=True):
    decoder = synthetic_decoder(directory, revision="synthetic-scale")
    archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
    decoders = [decoder]
    archive.register_epoch(decoder)
    for index in range(epochs - 1):
        replay = synthetic_decoder(directory, revision="synthetic-scale-%d" % index)
        decoders.append(replay)
        archive.register_epoch(replay, explicit=True)
    archive.register_epoch(decoder)
    frame = CTRL_FRAME if frame is None else frame
    for session in range(sessions):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "scale-%04d" % session, "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        archive.accept(meta, [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": frame}
                              for seq in range(chunks_per_session)])
    if drain:
        # Completed decode_states + counts_json history for EVERY epoch
        # (retained epochs included), via the production Decoder path.
        for active in decoders:
            archive.register_epoch(active)
            while archive.decode_once(active, limit=1000):
                pass
        archive.register_epoch(decoder)
    else:
        archive.decode_once(decoder, limit=1)
    return archive, decoder


def main():
    cells = []
    # Feasible cartesian subset: raw 1k/10k/100k x sessions 100/1k/10k
    # x epochs 1/4/16 with raw >= sessions. Bulk-seeded via production
    # accept/decode (control frames: history without outbox rows).
    for sessions, chunks, epochs in ((100, 10, 1), (100, 10, 4), (100, 10, 16),
                                     (1000, 10, 1), (1000, 10, 4), (1000, 10, 16),
                                     (100, 1000, 1), (100, 1000, 4),
                                     (1000, 100, 1), (1000, 100, 4), (1000, 100, 16),
                                     (10000, 10, 1), (10000, 10, 4)):
        raw = sessions * chunks
        with tempfile.TemporaryDirectory() as directory:
            archive, _decoder = build(directory, sessions, chunks, epochs)
            with archive.connect() as conn:
                got_raw = conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0]
                got_sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
                got_epochs = conn.execute("SELECT COUNT(*) FROM epochs").fetchone()[0]
            assert (got_raw, got_sessions, got_epochs) == (raw, sessions, epochs), \
                (got_raw, got_sessions, got_epochs, raw, sessions, epochs)
            probe = measure_status(archive.path)
            cells.append({"sessions": sessions, "raw_chunks": raw, "epochs": epochs,
                          "frame": "control", **probe})
    # Dense partial cell: one 3000-frame signal chunk past the 2000-row budget
    # stages a live decode_partial row with emitted rows and outbox rows.
    with tempfile.TemporaryDirectory() as directory:
        archive, decoder = build(directory, 1, 0, 1, frame=FRAME, drain=False)
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "dense-partial", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        archive.accept(meta, [{"seq": 0, "offset_ns": 0, "phase": "capture",
                               "data": b"t1232FFFF\r" * 3000}])
        archive.decode_once(decoder, limit=1)
        with archive.connect() as conn:
            partial = conn.execute("SELECT COUNT(*),COALESCE(SUM(rows_emitted),0)"
                                   " FROM decode_partial").fetchone()
            outbox = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        assert partial[0] >= 1 and partial[1] > 0 and outbox > 0, (partial, outbox)
        probe = measure_status(archive.path)
        cells.append({"sessions": 1, "raw_chunks": 1, "epochs": 1,
                      "frame": "signal-partial", "partial_rows": partial[0],
                      "partial_emitted": partial[1], "outbox_rows": outbox, **probe})
    print(json.dumps({"cells": cells,
                      "note": "default status must stay O(epochs): steps flat across raw/session growth"}))


if __name__ == "__main__":
    main()
