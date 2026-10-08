"""Offline, versioned synthetic application workloads; never use production data.

Preparation (imports, fixtures, schema creation) is outside the operation. Each
prepared callable runs once. Operations include application work and correctness
checks. Fault mode is controlled close/reopen plus replay, NOT a SIGKILL,
power-loss, network ACK, or hardware durability test.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

FIXTURE_VERSION = "synthetic-bench-v1"
FIXTURE_SEED = 1729
WORKLOADS = ("can", "fleet", "vss", "ai")
BASE_NS = 1800000000000000000


def _check(condition, message):
    # Deliberately survives python -O: correctness is never an optional gate.
    if not condition:
        raise AssertionError(message)


def _equal(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: {actual!r} != {expected!r}")


def _durability(conn):
    _equal(conn.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal", "journal mode")
    _equal(conn.execute("PRAGMA synchronous").fetchone()[0], 2, "synchronous FULL")


def _jsonable(value):
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False,
                                     default=_jsonable).encode()).hexdigest()


def _result(size, golden, root=None):
    result = {"units": size, "golden": golden}
    if root is not None:
        # Diagnostic only: filesystem/SQLite versions may change these bytes.
        result["disk_pending_bytes"] = sum(p.stat().st_size for p in root.glob("*.sqlite3*") if p.is_file())
    return result


def prepare(name, size, directory, fault=False):
    """Return a single-use callable. ``size`` is unique input records/frames.

    All files are created in a fresh workload subdirectory; existing data is
    refused. The caller owns removal of directory after the callable finishes.
    """
    if name not in WORKLOADS:
        raise ValueError(f"unknown workload: {name}")
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ValueError("size must be a positive integer")
    root = Path(directory) / name
    root.mkdir(parents=True, mode=0o700, exist_ok=False)
    operation = globals()["_prepare_" + name](size, root, fault)
    used = False

    def once():
        nonlocal used
        if used:
            raise RuntimeError("prepared workloads are single-use")
        used = True
        return operation()
    return once


def run_workload(name, size, directory, fault=False):
    """Convenience API; use prepare when excluding setup from timing."""
    return prepare(name, size, directory, fault)()


def _prepare_can(size, root, fault):
    from scripts.ingest.can.can_decoder import Decoder
    from scripts.ingest.can.can_receiver import Archive

    dbc = root / "fixture.dbc"
    dbc.write_text('VERSION "synthetic"\n\nNS_ :\n\nBS_:\n\nBU_: Fixture\n\n'
                   'BO_ 291 Sample: 2 Fixture\n'
                   ' SG_ Power : 0|16@1- (0.5,0) [-16384|16383.5] "kW" Fixture\n',
                   encoding="utf-8")
    definitions = root / "fixture.json"
    definitions.write_text(json.dumps({"revision": FIXTURE_VERSION, "signals": [{
        "source": "synthetic", "id": "0x123", "signal": "Power",
        "source_signal": "Power", "kind": "data", "start_bit": 0,
        "bit_length": 16, "byte_order": "little_endian", "signed": True,
        "scale": 0.5, "offset": 0, "is_multiplexer": False,
        "multiplexer_signal": None, "multiplexer_ids": None,
        "actual_dbc_length": 2, "unit": "kW", "source_unit": "kW",
        "choices": {}, "evidence": "synthetic@0123456789abcdef"}]}), encoding="utf-8")
    decoder = Decoder(dbc, definitions)
    meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "bench",
            "session_id": FIXTURE_VERSION, "started_ns": BASE_NS,
            "vehicle_firmware": "synthetic"}
    values = [((i + FIXTURE_SEED) % 200) - 100 for i in range(size)]
    # Dense chunks exercise bounded parser cursors, not one SQLite commit/frame.
    chunks = []
    expected = []
    for start in range(0, size, 128):
        seq = len(chunks)
        data = b"".join(b"t1232" + v.to_bytes(2, "little", signed=True).hex().upper().encode() + b"\r"
                        for v in values[start:start + 128])
        chunks.append({"seq": seq, "offset_ns": seq, "phase": "capture", "data": data})
        expected.extend((BASE_NS + seq, v * 0.5, "Vehicle.CAN.x123.Power")
                        for v in values[start:start + 128])
    path = root / "archive.sqlite3"
    archive = Archive(path, disk_reserve_bytes=0)
    archive.register_epoch(decoder)
    with archive.connect() as conn:
        _durability(conn)

    def operation():
        current = archive
        _equal(current.accept(meta, chunks), len(chunks), "CAN accepted chunks")
        if fault:
            # Persist a bounded partial cursor and reopen before resuming it.
            _check(current.decode_once(decoder, limit=1) > 0, "CAN first decode")
            current = Archive(path, disk_reserve_bytes=0)
            current.register_epoch(decoder)
        _equal(current.accept(meta, chunks), 0, "CAN duplicate acceptance")
        for _ in range(size + len(chunks) + 1):
            if current.decode_once(decoder, limit=64) == 0:
                break
        else:
            raise AssertionError("CAN decode failed to make bounded progress")
        with current.connect() as conn:
            _durability(conn)
            rows = [json.loads(r[0]) for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]
            _equal(conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0], len(chunks), "raw chunk dedupe")
            _equal(conn.execute("SELECT COUNT(*) FROM decode_partial").fetchone()[0], 0, "finished partial cursor")
        _equal([(r["event_time"], r["value_num"], r["path"]) for r in rows], expected, "CAN golden values")
        _equal(len({r["event_id"] for r in rows}), size, "CAN unique identities")
        # Ingest wall-clock is intentionally excluded; all semantic fields stay.
        stable = [{k: v for k, v in row.items() if k != "ingest_time"} for row in rows]
        return _result(size, {"rows": len(rows), "raw_chunks": len(chunks), "sha256": _digest(stable)}, root)
    return operation


def _prepare_fleet(size, root, fault):
    from scripts.ingest import fleet_recorder as fr

    meta = dict(decode_epoch=FIXTURE_VERSION, vss_version="synthetic", vehicle_firmware="synthetic",
                mapping_revision=FIXTURE_VERSION, collector_version="bench-v1", collector_id="bench",
                target_vin="SYNTHETIC-BENCH", vehicle_id="synthetic", vehicle_salt="synthetic-fixture-salt")
    if size > 1_000_000_000:
        raise ValueError("Fleet fixture supports at most one billion records")
    payloads = [json.dumps({"vin": "SYNTHETIC-BENCH", "createdAt": f"2027-01-15T08:00:00.{i:09d}Z",
                           "data": [{"key": "Soc", "value": {"doubleValue": float((i + FIXTURE_SEED) % 101)}}]},
                          separators=(",", ":")).encode() for i in range(size)]
    path = root / "outbox.sqlite3"
    conn = fr.open_outbox(path)
    _durability(conn)

    def operation():
        current = conn
        stats = fr.default_stats()
        try:
            for payload in payloads:
                _equal(fr.process_frame(current, fr.TOPIC_V, payload, meta, stats, max_rows=size + 1), 1, "Fleet store")
            if fault:
                current.close()
                current = fr.open_outbox(path)
            _durability(current)
            for payload in payloads:
                _equal(fr.process_frame(current, fr.TOPIC_V, payload, meta, stats, max_rows=size + 1), 0, "Fleet duplicate")
            cols = list(fr.COLUMNS)
            rows = [dict(zip(cols, r)) for r in current.execute("SELECT " + ",".join(cols) + " FROM outbox ORDER BY event_time")]
            _equal([r["value_num"] for r in rows], [float((i + FIXTURE_SEED) % 101) for i in range(size)], "Fleet golden values")
            _equal([r["event_time"] for r in rows], [BASE_NS + i for i in range(size)], "Fleet nanoseconds")
            _equal(len({r["event_id"] for r in rows}), size, "Fleet unique identities")
            _equal(current.execute("SELECT COUNT(*) FROM seen_ids").fetchone()[0], size, "Fleet seen identities")
            pending_bytes = sum(p.stat().st_size for p in root.glob("*.sqlite3*") if p.is_file())
            if fault:
                # Controlled local ACK deletion tests retained dedupe tombstones.
                current.execute("DELETE FROM outbox")
                current.commit()
                current.close()
                current = fr.open_outbox(path)
                _durability(current)
                for payload in payloads:
                    _equal(fr.process_frame(current, fr.TOPIC_V, payload, meta, stats, max_rows=size + 1), 0, "Fleet ACK replay")
                _equal(fr.outbox_pending_total(current), 0, "Fleet ACK tombstone dedupe")
            stable = [{k: v for k, v in row.items() if k != "ingest_time"} for row in rows]
            result = _result(size, {"rows": len(rows), "sha256": _digest(stable)}, root)
            result["disk_pending_bytes"] = pending_bytes
            return result
        finally:
            current.close()
    return operation


def _prepare_vss(size, root, fault):
    from scripts.vehicle import vss_recorder as rec

    meta = dict(decode_epoch=FIXTURE_VERSION, vss_version="synthetic", vehicle_firmware="synthetic",
                dbc_primary_commit="synthetic", mapping_revision=FIXTURE_VERSION, collector_version="bench-v1")
    values = [0, False, "", "합성", -1.5]
    rows = []
    for i in range(size):
        num, text, boolean = rec.classify(values[(i + FIXTURE_SEED) % len(values)])
        path = "Vehicle.Synthetic.Value"
        event_id = rec.deterministic_event_id("synthetic", path, BASE_NS + i, FIXTURE_VERSION, num, text, boolean)
        rows.append(rec.make_row("synthetic", path, BASE_NS + i, event_id, meta, {}, num, text, boolean, BASE_NS))
    db = root / "outbox.sqlite3"
    conn = rec.open_outbox(db)
    _durability(conn)

    def operation():
        current = conn
        try:
            last = {}
            for row in rows:
                _equal(rec.store_update(current, last, row["path"], row), True, "VSS store")
            if fault:
                current.close()
                current = rec.open_outbox(db)
            _durability(current)
            last = {}
            for row in rows:
                _equal(rec.store_update(current, last, row["path"], row), False, "VSS duplicate")
            stored = [dict(zip(rec.COLUMNS, r)) for r in current.execute("SELECT " + ",".join(rec.COLUMNS) + " FROM outbox ORDER BY event_time")]
            _equal(stored, rows, "VSS full golden rows")
            golden_values = [(0.0, None, None), (None, None, 0), (None, "", None), (None, "합성", None), (-1.5, None, None)]
            _equal([(r["value_num"], r["value_text"], r["value_bool"]) for r in stored],
                   [golden_values[(i + FIXTURE_SEED) % 5] for i in range(size)], "VSS independent value classification")
            pending_bytes = sum(p.stat().st_size for p in root.glob("*.sqlite3*") if p.is_file())
            _equal(current.execute("SELECT COUNT(*) FROM seen_ids").fetchone()[0], size, "VSS seen identities")
            if fault:
                current.execute("DELETE FROM outbox")
                current.commit()
                current.close()
                current = rec.open_outbox(db)
                _durability(current)
                last = {}
                for row in rows:
                    _equal(rec.store_update(current, last, row["path"], row), False, "VSS ACK replay")
                _equal(current.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0, "VSS ACK tombstone dedupe")
            result = _result(size, {"rows": len(stored), "sha256": _digest(stored)}, root)
            result["disk_pending_bytes"] = pending_bytes
            return result
        finally:
            current.close()
    return operation


def _prepare_ai(size, root, fault):
    from scripts.analytics import aggregate as agg

    cols = ["timestamp", "timestamp_end", "trace_id", "span_id", agg.CLIENT,
            agg.SESSION, agg.OP_NAME, agg.TOKEN_IN, agg.TOKEN_OUT, agg.MODEL_REQ]
    rows = []
    expected = {}
    for i in range(size):
        mode = i % 3
        tokens = ((7, 3), (0, 0), (None, None))[mode]
        session = f"synthetic-{mode}"
        rows.append(["2027-01-15T08:00:00Z", "2027-01-15T08:00:01Z",
                     f"trace-{i:016x}", f"span-{i:016x}", "codex", session,
                     "chat", *tokens, "synthetic-unpriced"])
        group = expected.setdefault(session, {"count": 0, "input": tokens[0], "output": tokens[1]})
        if group["count"] and tokens[0] is not None:
            group["input"] += tokens[0]
            group["output"] += tokens[1]
        group["count"] += 1
    delivered = rows + rows  # Always exercise at-least-once duplicates.
    if fault:
        delivered += rows[::-1]  # Controlled reordered replay, not a process crash.

    def operation():
        spans = agg.dedupe_spans(cols, delivered)
        _equal(len(spans), size, "AI span dedupe")
        sessions = agg.summarize_sessions(spans, prices={})
        daily = agg.summarize_daily(spans, prices={})
        _equal(len(sessions), len(expected), "AI session identities")
        for session in sessions:
            want = expected[session["session_id"]]
            _equal((session["input"], session["output"]), (want["input"], want["output"]), "AI unknown/zero/reported tokens")
            _equal(session["cost"], None, "AI missing cost stays NULL")
            _equal(session["cost_estimated_usd"], None, "AI no invented prices")
        _equal(len(daily), 1, "AI daily groups")
        _equal(daily[0]["input"], sum(v["input"] or 0 for v in expected.values()), "AI daily input")
        _equal(daily[0]["output"], sum(v["output"] or 0 for v in expected.values()), "AI daily output")
        golden = json.loads(json.dumps({"spans": len(spans), "sessions": sessions, "daily": daily}, default=_jsonable))
        return _result(size, golden)
    return operation
