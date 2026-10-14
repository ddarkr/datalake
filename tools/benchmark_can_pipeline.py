#!/usr/bin/env python3
"""Sustained synthetic CAN pipeline harness: loopback receive to full ACK.

Parent-run, synthetic inputs only. Offers deterministic multisignal / enum /
mux input through a real Receiver HTTP loopback into a real durable Archive,
decodes with the real Decoder (manually and via the real Worker thread), and
full-ACKs through a real Greptime HTTP sink (loopback), including the dirty
notify. Complements tools/benchmark_can_decode.py (decode-only drain) and the
can-decode-perf gate, which stay the regression gate: this harness adds
sustained receive+decode+flush overlap, sink failure/recovery, retained-epoch
backlog, offered-vs-achieved rates, stage percentiles, and a process
SIGKILL/restart check (committed-prefix durability only, never a power-loss
claim).

Every scenario compares ALL canonical row bytes, event IDs, arrival order,
cursors, partial checkpoints, outbox content, and durable counters against a
sequential reference built from the same inputs with a frozen clock -- never
a length-only check. worker_errors are excluded from equality by design: the
outage/partial/dirty scenarios deliberately record fixed-category errors,
which are reported separately.

No production data, credentials, or absolute capacity claims. Temporary
archives and loopback sockets are removed on exit.

Runnable commands, e.g.::

    python tools/benchmark_can_pipeline.py --scenarios fast
    python tools/benchmark_can_pipeline.py \\
        --scenarios fast,slow,outage-recovery,partial-recovery,dirty-recovery
    python tools/benchmark_can_pipeline.py --scenarios fast \
        --compare-overhead --json-out /tmp/pipe.json
    python tools/benchmark_can_pipeline.py --scenarios kill-restart
    python tools/benchmark_can_pipeline.py --reference-only --scale 1
    python tools/benchmark_can_pipeline.py --scenarios fast --baseline-root \
        /path/to/base --scale 1 --dense-frames 40
    python tools/benchmark_can_pipeline.py --reference-only --source-root \
        /path/to/base --scale 1 --dense-frames 40
    python tools/benchmark_can_pipeline.py --scenarios fast \
        --high-rows 20000 --low-rows 15000
    python tools/benchmark_can_pipeline.py --scenarios fast,slow \
        --lowflow-samples 20 --worker-interval 1 --offered-cps 50
    python tools/benchmark_can_pipeline.py --scenarios fast \
        --require-production-stages --status-scale 200
"""
import argparse
import base64
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import math
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

CURRENT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CURRENT_ROOT))
_SOURCE_ROOT = None
for _i, _a in enumerate(sys.argv):
    if _a == "--source-root" and _i + 1 < len(sys.argv):
        _SOURCE_ROOT = sys.argv[_i + 1]
    elif _a.startswith("--source-root="):
        _SOURCE_ROOT = _a.split("=", 1)[1]
if _SOURCE_ROOT:
    sys.path.insert(0, _SOURCE_ROOT)

from scripts.ingest.can.can_decoder import Decoder  # noqa: E402
from scripts.ingest.can.can_otlp_wire import check_response, encode_batch  # noqa: E402
from scripts.ingest.can.can_receiver import (Archive, Greptime, Receiver,  # noqa: E402
                                             Worker)

FIXED_DEFAULT = 1820000000000000000
STARTED = 1800000000000000000
AUTH_USER, AUTH_PASS = "loopback-user", "loopback-pass"
SCENARIOS = ("fast", "slow", "outage-recovery", "partial-recovery",
             "dirty-recovery", "kill-restart")

DBC_TEXT = ('VERSION "synthetic"\n\nNS_ :\n\nBS_:\n\nBU_: Fixture\n\n'
            'BO_ 291 Sample: 2 Fixture\n'
            ' SG_ Power : 0|16@1- (0.5,0) [-16384|16383.5] "kW" Fixture\n'
            'BO_ 292 BigMsg: 8 Fixture\n'
            ' SG_ Big : 7|16@0+ (0.1,0) [0|6553.5] "V" Fixture\n'
            'BO_ 293 EnumMsg: 1 Fixture\n'
            ' SG_ Mode : 0|8@1+ (1,0) [0|255] "" Fixture\n'
            'VAL_ 293 Mode 0 "Off" 1 "On" 2 "INVALID VALUE" ;\n'
            'BO_ 512 MuxMsg: 8 Fixture\n'
            ' SG_ MuxSel M : 0|8@1+ (1,0) [0|255] "" Fixture\n'
            ' SG_ SigA m0 : 8|8@1+ (1,0) [0|255] "" Fixture\n'
            ' SG_ SigC m1 : 16|8@1+ (1,0) [0|255] "" Fixture\n'
            'BO_ 2147483904 MsgExt: 8 Fixture\n'
            ' SG_ ExtSig : 0|8@1+ (1,0) [0|10] "" Fixture\n')


def _entry(identifier, sig, start, length, order, signed, scale, mux,
           mux_sig, mux_ids, dlc, unit, choices, extended=False):
    item = {"source": "synthetic", "id": identifier, "signal": sig,
            "source_signal": sig, "kind": "data", "start_bit": start,
            "bit_length": length, "byte_order": order, "signed": signed,
            "scale": scale, "offset": 0, "is_multiplexer": mux,
            "multiplexer_signal": mux_sig, "multiplexer_ids": mux_ids,
            "actual_dbc_length": dlc, "unit": unit, "source_unit": unit,
            "choices": choices, "evidence": "synthetic@0123456789abcdef"}
    if extended:
        item["is_extended_frame"] = True
    return item


def write_fixture(directory, revision):
    directory = Path(directory)
    dbc = directory / "pipe.dbc"
    dbc.write_text(DBC_TEXT, encoding="utf-8")
    definitions = directory / ("pipe-%s.json" % revision)
    definitions.write_text(json.dumps({"revision": revision, "signals": [
        _entry("0x123", "Power", 0, 16, "little_endian", True, 0.5, False,
               None, None, 2, "kW", {}),
        _entry("0x124", "Big", 7, 16, "big_endian", False, 0.1, False,
               None, None, 8, "V", {}),
        _entry("0x125", "Mode", 0, 8, "little_endian", False, 1, False,
               None, None, 1, "",
               {"0": "Off", "1": "On", "2": "INVALID VALUE"}),
        _entry("0x200", "MuxSel", 0, 8, "little_endian", False, 1, True,
               None, None, 8, "", {}),
        _entry("0x200", "SigA", 8, 8, "little_endian", False, 1, False,
               "MuxSel", [0], 8, "", {}),
        _entry("0x200", "SigC", 16, 8, "little_endian", False, 1, False,
               "MuxSel", [1], 8, "", {}),
        _entry("0x100", "ExtSig", 0, 8, "little_endian", False, 1, False,
               None, None, 8, "", {}, extended=True),
    ]}), encoding="utf-8")
    return Decoder(dbc, definitions)


POWER = b"t1232FFFF\r"
BIG = b"t12481234000000000000\r"
ENUMS = (b"t125100\r", b"t125101\r", b"t125102\r", b"t125109\r")
MUX0 = b"t20080009000000000000\r"
MUX1 = b"t20080100080000000000\r"
BADMUX = b"t20080500000000000000\r"
EXT = b"T0000010080300000000000000\r"
UNK = b"t7FE20100\r"
CTRL = (b"Z\r", b"S6\r", b"\r")
MAL = (b"not-a-frame\r", b"t123100\r")


def _meta(session):
    return {"schema_version": 1, "vehicle": "synthetic",
            "collector_id": "fixture", "session_id": session,
            "started_ns": STARTED, "vehicle_firmware": "synthetic"}


def _chunk(seq, data):
    return {"seq": seq, "offset_ns": seq * 1000, "phase": "capture",
            "data": data}


def build_plan(scale=1, dense_frames=1200):
    """Deterministic multisignal/enum/mux plan with dense partial resume."""
    if scale < 1 or dense_frames < 1:
        raise ValueError("scale and dense_frames must be positive")
    stream = []
    for i in range(dense_frames):
        stream.append(MUX0 if i % 2 == 0 else MUX1)
        if i % 7 == 0:
            stream.append(ENUMS[(i // 7) % 4])
        if i % 50 == 0:
            stream.append(UNK)
    blob = b"".join(stream)
    cut = len(blob) - (len(POWER) // 2)  # split resume mid-frame
    dense = [("pipe-dense", [_chunk(0, blob[:cut] + POWER[:5]),
                             _chunk(1, POWER[5:] + blob[cut:] + BADMUX)])]
    mixed = []
    for s in range(3 + scale):
        name = "pipe-mixed-%d" % s
        body = (POWER + BIG + ENUMS[s % 4] + MUX0 + MUX1 + UNK + CTRL[s % 3]
                + MAL[s % 2] + EXT)
        if s == 1:  # split frame across a chunk boundary (tail resume)
            mixed.append((name, [_chunk(0, body + POWER[:9]),
                                _chunk(1, POWER[9:] + body)]))
        else:
            # Every session starts at seq 0: the archive rejects gaps per session.
            mixed.append((name, [_chunk(0, body),
                                _chunk(1, body)]))
    probe = [("pipe-probe", [_chunk(0, POWER)])]
    retained = [("pipe-retained", [_chunk(i, POWER) for i in range(3)])]
    batches = probe + dense + mixed
    total = sum(len(c) for _, c in batches) + 3
    total_bytes = sum(len(c["data"]) for _, c in batches for c in c)
    total_bytes += 3 * len(POWER)
    return {"probe": probe, "dense": dense, "mixed": mixed,
            "retained": retained, "batches": batches,
            "total_chunks": total, "total_bytes": total_bytes,
            "scale": scale, "dense_frames": dense_frames}


PRODUCTION_STAGES = ("accept", "decode_read", "decode_cpu", "decode_commit",
                       "sql_prepare", "json_prepare", "signal_ack",
                       "dirty_ack", "queue_wait", "flush_commit",
                       "sqlite_lock_wait")

_HARNESS_STAGES = ("accept", "decode", "flush", "raw_post", "status")


class StageRecorder:
    """Harness-local bounded stage timer; no labels, optional on/off.

    Coarse harness-local accept/decode/flush/raw_post/status timers only.
    Named production distributions (accept, decode_read, decode_cpu,
    decode_commit, sql_prepare, json_prepare, signal_ack, dirty_ack,
    queue_wait, flush_commit, sqlite_lock_wait) are collected separately
    via collect_production_stages(): read-only, getattr-guarded consumption
    of archive.read_stage_samples() (a {stage: [seconds]} mapping, subset
    allowed). Absent hooks yield an explicit
    production_stages_available=False instead of fabricated data.
    """

    def __init__(self, enabled=True, cap=200000):
        self.enabled = enabled
        self.cap = cap
        self.dropped = 0
        self.samples = {stage: [] for stage in _HARNESS_STAGES}

    def add(self, stage, duration):
        if not self.enabled:
            return
        target = self.samples[stage]
        if len(target) < self.cap:
            target.append(duration)
        else:
            self.dropped += 1

    def summary(self):
        out = {}
        for stage, values in self.samples.items():
            ordered = sorted(values)
            out[stage] = {"n": len(ordered), "dropped": self.dropped,
                          "p50": _pct(ordered, 50), "p95": _pct(ordered, 95),
                          "p99": _pct(ordered, 99)}
        return out


def _pct(ordered, quant):
    if not ordered:
        return None
    rank = int(math.ceil(quant / 100.0 * len(ordered))) - 1
    return ordered[max(0, min(rank, len(ordered) - 1))]


def collect_production_stages(archive):
    """Read-only production stage distributions; None when hooks absent.

    Prefers archive.read_stage_summary() ({stage: {count,p50,p95,p99,
    dropped}}); falls back to archive.read_stage_samples() ({stage:
    [seconds]}). Unknown extra stages pass through when non-empty;
    absent/empty stages stay omitted. None means hooks absent, never
    fabricated data.
    """
    summarizer = getattr(archive, "read_stage_summary", None)
    if callable(summarizer):
        try:
            summary = summarizer()
        except Exception:
            summary = None
        if isinstance(summary, dict) and summary:
            out = {}
            for stage, stats in summary.items():
                if not isinstance(stats, dict):
                    continue
                out[stage] = {key: stats.get(key) for key in
                              ("count", "p50", "p95", "p99", "dropped")}
            if out:
                return out
    reader = getattr(archive, "read_stage_samples", None)
    if not callable(reader):
        return None
    try:
        samples = reader()
    except Exception:
        return None
    if not isinstance(samples, dict):
        return None
    out = {}
    for stage, values in samples.items():
        if not values:
            continue
        ordered = sorted(values)
        out[stage] = {"count": len(ordered), "p50": _pct(ordered, 50),
                      "p95": _pct(ordered, 95), "p99": _pct(ordered, 99),
                      "dropped": None}
    return out or None


def reset_production_stages(archive):
    """Best-effort sample reset for clean per-run deltas; False when absent."""
    resetter = getattr(archive, "reset_stage_samples", None)
    if not callable(resetter):
        return False
    try:
        resetter()
    except Exception:
        return False
    return True


def set_production_instrumentation(archive, enabled):
    """Best-effort production instrumentation toggle; False when absent."""
    setter = getattr(archive, "set_stage_instrumentation", None)
    if not callable(setter):
        return False
    try:
        setter(bool(enabled))
    except Exception:
        return False
    return True


def _wrap_archive(archive, recorder):
    originals = (archive.accept, archive.decode_once, archive.flush_once)

    def accept(meta, chunks):
        start = time.monotonic()
        try:
            return originals[0](meta, chunks)
        finally:
            recorder.add("accept", time.monotonic() - start)

    def decode_once(decoder, limit=1000):
        start = time.monotonic()
        try:
            return originals[1](decoder, limit=limit)
        finally:
            recorder.add("decode", time.monotonic() - start)

    def flush_once(greptime, limit=20000, max_body_bytes=None):
        start = time.monotonic()
        try:
            if max_body_bytes is None:
                return originals[2](greptime, limit=limit)
            return originals[2](greptime, limit=limit,
                                max_body_bytes=max_body_bytes)
        finally:
            recorder.add("flush", time.monotonic() - start)

    archive.accept, archive.decode_once, archive.flush_once = (
        accept, decode_once, flush_once)

    def restore():
        archive.accept, archive.decode_once, archive.flush_once = originals

    return restore


def take_snapshot(archive, epochs):
    """Full ordered durable state: raw order, outbox, cursors, partials."""
    with archive.connect() as conn:
        raw = [(row[0], row[1], hashlib.sha256(row[2]).hexdigest(),
                len(row[2])) for row in conn.execute(
            "SELECT s.session_id,c.seq,c.data FROM raw_chunks c "
            "JOIN sessions s ON s.id=c.session ORDER BY c.id")]
        outbox = [(row[0], row[1]) for row in conn.execute(
            "SELECT id,row_json FROM outbox ORDER BY id")]
        # Independent durable ground truth (never the maintained counters):
        # exact table row count and SUM of UTF-8 row_json bytes.
        outbox_table = conn.execute("SELECT COUNT(*), "
                                    "COALESCE(SUM(LENGTH(row_json)),0) "
                                    "FROM outbox").fetchone()
        states = {}
        partial = {}
        for epoch in epochs:
            states[epoch] = [dict(row) for row in conn.execute(
                "SELECT s.session_id,d.next_seq,d.state_json,d.counts_json,"
                "d.rows FROM decode_states d JOIN sessions s ON s.id=d.session "
                "WHERE d.epoch=? ORDER BY s.session_id", (epoch,))]
            partial[epoch] = [dict(row) for row in conn.execute(
                "SELECT s.session_id,d.seq,d.state_json,d.counts_json,"
                "d.rows_emitted FROM decode_partial d "
                "JOIN sessions s ON s.id=d.session "
                "WHERE d.epoch=? ORDER BY s.session_id", (epoch,))]
        counters = dict(
            conn.execute("SELECT key,value FROM archive_meta").fetchall())
    derived = {"outbox_table_rows": outbox_table[0],
               "outbox_table_bytes": outbox_table[1],
               # Authoritative byte projection for cross-root comparison:
               # SUM over UTF-8 row_json bytes of the ordered outbox rows.
               # Deriving from content (not metadata) keeps legacy baseline
               # trees without outbox_bytes metadata comparable.
               "outbox_bytes": sum(len(text.encode("utf-8"))
                                   for _, text in outbox)}
    return {"raw": raw, "outbox": outbox, "states": states,
            "partial": partial, "counters": counters, "derived": derived}


def validate_counters(snapshot, detail_status=None):
    """Independently validate maintained counters; list of problems.

    outbox_rows == durable table count; outbox_bytes (when the tree
    maintains it) == SUM UTF-8 row_json bytes; decoded_rows_total ==
    states.rows + partial.rows_emitted summed over epochs. Optionally
    cross-checks epoch processed/rows against status(detail=True) cursors
    + partials when provided. Migration markers and time-sensitive
    instrumentation are never part of equality.
    """
    problems = []
    counters = snapshot.get("counters", {})
    derived = snapshot.get("derived", {})
    if counters.get("outbox_rows", 0) != derived.get("outbox_table_rows"):
        problems.append("outbox_rows %r != table count %r"
                        % (counters.get("outbox_rows"),
                           derived.get("outbox_table_rows")))
    maintained_bytes = counters.get("outbox_bytes")
    if maintained_bytes is not None and maintained_bytes != derived.get(
            "outbox_bytes"):
        problems.append("outbox_bytes %r != content SUM %r"
                        % (maintained_bytes, derived.get("outbox_bytes")))
    if maintained_bytes is None and derived.get("outbox_table_bytes") != \
            derived.get("outbox_bytes"):
        problems.append("outbox LENGTH() %r != UTF-8 SUM %r"
                        % (derived.get("outbox_table_bytes"),
                           derived.get("outbox_bytes")))
    decoded = counters.get("decoded_rows_total", 0)
    states_rows = sum(entry.get("rows", 0) or 0
                      for rows in snapshot.get("states", {}).values()
                      for entry in rows)
    partial_rows = sum(entry.get("rows_emitted", 0) or 0
                       for rows in snapshot.get("partial", {}).values()
                       for entry in rows)
    if decoded != states_rows + partial_rows:
        problems.append("decoded_rows_total %r != states %r + partial %r"
                        % (decoded, states_rows, partial_rows))
    if detail_status is not None:
        for entry in detail_status.get("epochs") or []:
            epoch = entry.get("epoch")
            want_rows = sum(e.get("rows", 0) or 0
                            for e in snapshot.get("states", {}).get(
                                epoch, []))
            if (entry.get("decoded_rows") is not None
                    and entry.get("decoded_rows") != want_rows):
                problems.append("epoch %s decoded_rows %r != cursor rows %r"
                                % (epoch, entry.get("decoded_rows"),
                                   want_rows))
    return problems


def scoped_view(snapshot):
    """Equality scope: everything durable except worker_errors (by design).

    Includes the content-derived outbox_bytes projection so legacy trees
    lacking the metadata stay comparable. Time-sensitive freshness and
    incidental migration markers are excluded from the hash.
    """
    counters = {key: snapshot["counters"].get(key, 0) for key in (
        "raw_chunks", "raw_bytes", "outbox_rows", "decoded_rows_total",
        "acked_rows_total")}
    derived = {"outbox_bytes": snapshot.get("derived", {}).get(
        "outbox_bytes", 0)}
    return {"raw": snapshot["raw"], "outbox": snapshot["outbox"],
            "states": snapshot["states"], "partial": snapshot["partial"],
            "counters": counters, "derived": derived}


def snapshot_sha(snapshot):
    raw = json.dumps(scoped_view(snapshot), sort_keys=True,
                     separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def compare_scoped(first, second):
    """None when equal, else a short first-difference description."""
    left, right = scoped_view(first), scoped_view(second)
    for key in ("raw", "outbox", "states", "partial", "counters"):
        if left[key] != right[key]:
            return "%s differs" % key
    return None


def _status_view(archive, active_epoch=None, detail=False):
    getter = getattr(archive, "status", None)
    try:
        status = getter(detail=True) if detail else getter()
    except TypeError:
        # Baseline trees without the detail flag: plain default status.
        status = getter()
    epochs = status.get("epochs") or []
    if active_epoch is None:
        remaining = sum((entry.get("remaining_chunks") or 0)
                        for entry in epochs)
    else:
        # Retained/replay epochs keep their own leftover by design; the
        # service backlog is the active epoch's remainder only.
        remaining = sum((entry.get("remaining_chunks") or 0)
                        for entry in epochs
                        if entry.get("epoch") == active_epoch)
    return status, remaining


class _SinkHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        payload = self.rfile.read(length)
        server = self.server
        with server.guard:
            mode = dict(server.mode)
            server.attempts += 1
            try:
                sql = urllib.parse.parse_qs(
                    payload.decode("ascii"))["sql"][0]
            except (ValueError, KeyError, IndexError):
                sql = ""
            dirty = "vehicle_signal_dirty" in sql
            rows = sql.count("),(") + 1 if sql else -1
            fail = (mode["name"] == "outage" and not dirty
                    and mode.get("fails", 0) > 0)
            if fail:
                mode["fails"] -= 1
                server.mode = mode
                self.send_response(503)
                body = b"synthetic outage"
            elif (mode["name"] == "partial" and not dirty
                    and mode.get("fails", 0) > 0):
                mode["fails"] -= 1
                server.mode = mode
                self.send_response(200)
                body = json.dumps(
                    {"code": 0, "output": [{"affectedrows": 0}]}).encode()
            elif (mode["name"] == "dirty-fail" and dirty
                    and mode.get("fails", 0) > 0):
                mode["fails"] -= 1
                server.mode = mode
                self.send_response(200)
                body = json.dumps(
                    {"code": 0, "output": [{"affectedrows": 0}]}).encode()
            elif rows < 0:
                self.send_response(503)
                body = b"synthetic bad payload"
            else:
                delay = mode.get("delay", 0)
                if dirty:
                    server.dirty_statements += rows
                else:
                    server.acked_batches.append(rows)
                    server.acked_rows += rows
                server.payloads.append(payload)
                self.send_response(200)
                body = json.dumps(
                    {"code": 0, "output": [{"affectedrows": rows}]}).encode()
        delay = server.mode.get("delay", 0)
        if delay:
            time.sleep(delay)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class Sink(HTTPServer):
    daemon_threads = True

    def __init__(self, address, mode):
        super().__init__(address, _SinkHandler)
        self.mode = dict(mode)
        self.guard = threading.Lock()
        self.attempts = 0
        self.payloads = []
        self.acked_batches = []
        self.acked_rows = 0
        self.dirty_statements = 0


def _serve(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def _greptime_for(sink, body_budget):
    return Greptime("http://127.0.0.1:%d" % sink.server_port, "synthetic",
                    "synthetic", "synthetic", timeout=10,
                    max_body_bytes=body_budget)


def _db_sizes(path):
    path = Path(path)
    wal = path.with_name(path.name + "-wal")
    return {"db_bytes": path.stat().st_size,
            "wal_bytes": wal.stat().st_size if wal.exists() else 0}


def _peak_rss():
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return peak if sys.platform == "darwin" else peak * 1024
    except Exception:
        return None


def _wait_for(predicate, timeout, recorder=None, archive=None, samples=None,
              active_epoch=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if archive is not None and samples is not None:
            start = time.monotonic()
            status, remaining = _status_view(archive, active_epoch,
                                                 detail=True)
            recorder.add("status", time.monotonic() - start)
            if len(samples) < 5000:
                samples.append(
                    {"t": time.monotonic(),
                     "pending_rows": status.get("pending_rows", 0),
                     "remaining": remaining,
                     "backlog": status.get("backlog_chunks")})
            if predicate(status, remaining):
                return status
        elif predicate(None, None):
            return None
        time.sleep(0.02)
    raise TimeoutError("drain deadline exceeded")


def seed_retained(archive, plan, old_decoder):
    archive.register_epoch(old_decoder)
    meta = _meta("pipe-retained")
    archive.accept(meta, plan["retained"][0][1])
    archive.decode_once(old_decoder, limit=1)  # leave retained backlog


def _decode_drain(archive, decoder):
    partial_seen = False
    calls = 0
    while True:
        staged = archive.decode_once(decoder, limit=1000)
        calls += 1
        if not staged:
            break
        with archive.connect() as conn:
            if conn.execute(
                    "SELECT 1 FROM decode_partial LIMIT 1").fetchone():
                partial_seen = True
    return calls, partial_seen


def _flush_drain(archive, greptime, outbox_limit, body_budget):
    total = 0
    while True:
        flushed = archive.flush_once(
            greptime, limit=outbox_limit, max_body_bytes=body_budget)
        if not flushed:
            break
        total += flushed
    return total


def run_reference(directory, plan, fixed_ns, instrument, outbox_limit,
                  body_budget):
    """Sequential accept/decode/flush reference with a frozen clock.

    Mirrors the sustained run's low-flow probe exactly (probe accept,
    decode, flush, then the sustained batches) so the pre-flush equality
    point and every payload byte match the loopback run.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    previous = time.time_ns
    time.time_ns = lambda: fixed_ns  # noqa: E731 deterministic ingest rows
    recorder = StageRecorder(enabled=instrument)
    try:
        decoder = write_fixture(directory, "synthetic-pipe")
        old = write_fixture(directory, "synthetic-old")
        archive = Archive(str(directory / "ref.sqlite"),
                          disk_reserve_bytes=0)
        restore = _wrap_archive(archive, recorder)
        try:
            seed_retained(archive, plan, old)
            archive.register_epoch(decoder, explicit=True)
            sizes_seed = _db_sizes(archive.path)
            wall0 = time.monotonic()
            for session, chunks in plan["probe"]:
                archive.accept(_meta(session), chunks)
            while archive.decode_once(decoder, limit=1000):
                pass
            sink = Sink(("127.0.0.1", 0), {"name": "success"})
            thread = _serve(sink)
            greptime = _greptime_for(sink, body_budget)
            try:
                _flush_drain(archive, greptime, outbox_limit, body_budget)
                with sink.guard:
                    sink.payloads.clear()
                    sink.acked_batches.clear()
                    sink.acked_rows = 0
                    sink.dirty_statements = 0
                for session, chunks in plan["dense"] + plan["mixed"]:
                    archive.accept(_meta(session), chunks)
                calls, partial_seen = _decode_drain(archive, decoder)
                mid = take_snapshot(archive, (decoder.epoch, old.epoch))
                sizes_decode = _db_sizes(archive.path)
                _flush_drain(archive, greptime, outbox_limit, body_budget)
            finally:
                sink.shutdown()
                sink.server_close()
                thread.join(3)
            wall = time.monotonic() - wall0
            final = take_snapshot(archive, (decoder.epoch, old.epoch))
            status, _ = _status_view(archive, decoder.epoch, detail=True)
            mid_problems = validate_counters(mid, status)
            final_problems = validate_counters(final, status)
            return {
                "counter_problems": {"mid": mid_problems,
                                     "final": final_problems},
                "mid": mid, "mid_sha": snapshot_sha(mid),
                "final": final, "final_sha": snapshot_sha(final),
                "payloads_sha": hashlib.sha256(
                    b"".join(sink.payloads)).hexdigest(),
                "acked_rows": sink.acked_rows,
                "dirty_statements": sink.dirty_statements,
                "decode_calls": calls, "partial_seen": partial_seen,
                "wall_s": wall, "stages": recorder.summary(),
                "production_stages": collect_production_stages(archive),
                "sizes_seed": sizes_seed, "sizes_decode": sizes_decode,
                "sizes_final": _db_sizes(archive.path),
                "peak_rss_bytes": _peak_rss(),
                "decode_epoch": decoder.epoch, "old_epoch": old.epoch,
                "errors": status.get("errors", {}),
            }
        finally:
            restore()
    finally:
        time.time_ns = previous


def _post_batch(endpoint, meta, chunks, recorder):
    payload = encode_batch(meta, chunks)
    request = urllib.request.Request(
        endpoint + "/v1/logs", data=gzip.compress(payload),
        headers={"Authorization": "Basic " + base64.b64encode(
            (AUTH_USER + ":" + AUTH_PASS).encode()).decode(),
                 "Content-Type": "application/x-protobuf",
                 "Content-Encoding": "gzip"})
    start = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read()
            check_response(body)
            return len(chunks), time.monotonic() - start
    except urllib.error.HTTPError as exc:
        raise RuntimeError("loopback receive failed: %d" % exc.code)


def _make_worker(archive, decoder, greptime, worker_interval, outbox_limit,
                 body_budget, backpressure=None):
    """Build the real Worker; never silently weaken requested options.

    Only kwargs verified present in Worker.__init__ via inspect.signature
    are passed. Any explicitly requested backpressure/wake option the
    running tree does not support is a hard error, never a silent fallback
    to a weaker Worker. Constructor TypeErrors always propagate so real
    bugs are never masked. Archive-owned readiness events (decode_ready /
    upload_ready) need no harness wiring. Returns (worker, applied_dict).
    """
    import inspect
    backpressure = backpressure or {}
    if backpressure.get("use_hub") or "hub" in backpressure:
        raise ValueError("--use-hub removed: readiness events are owned "
                         "by Archive, Worker takes no hub")
    requested = {}
    for key in ("high_rows", "low_rows", "high_bytes", "low_bytes",
                "retry_interval"):
        value = backpressure.get(key)
        if value is not None:
            requested[key] = value
    supported = set(inspect.signature(Worker.__init__).parameters)
    unsupported = [key for key in requested if key not in supported]
    if unsupported:
        raise ValueError("unsupported Worker options: %s"
                         % ",".join(sorted(unsupported)))
    worker = Worker(archive, decoder, greptime, interval=worker_interval,
                    outbox_limit=outbox_limit, max_body_bytes=body_budget,
                    **requested)
    return worker, dict(requested)


def _plain_backpressure(args):
    return {"high_rows": args.high_rows, "low_rows": args.low_rows,
            "high_bytes": args.high_bytes, "low_bytes": args.low_bytes,
            "retry_interval": args.downstream_retry_interval}


LOWFLOW_DEFAULT = 20


def run_lowflow_series(directory, plan, fixed_ns, outbox_limit, body_budget,
                       count=LOWFLOW_DEFAULT, worker_interval=0.02,
                       backpressure=None, drain_timeout=60):
    """N independent raw POST -> full signal+dirty ACK latency samples.

    Fresh archive per sample: one chunk through the real Receiver HTTP,
    then the real Worker (built by _make_worker at the passed
    worker_interval/backpressure -- the same wakeup/poll path as the
    sustained scenarios) decodes and full-ACKs through the real loopback
    Greptime sink including the dirty notify. Each sample waits for the
    authoritative drained state (pending_rows 0, probe chunks processed,
    sink signal rows + dirty statements observed) and fails on timeout;
    worker thread failures propagate via join. Frozen clock keeps rows
    deterministic; wall latency stays real. Returns {latencies_s, count,
    worker_interval, backpressure}.
    """
    from scripts.ingest.can.can_otlp_wire import encode_batch as _enc
    latencies = []
    applied = {}
    for _ in range(count):
        with tempfile.TemporaryDirectory(prefix="pipe-lowflow-") as tmp:
            sub = Path(tmp) / "lf.sqlite"
            decoder = write_fixture(tmp, "synthetic-pipe")
            archive = Archive(str(sub), disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            receiver = Receiver(("127.0.0.1", 0), archive, "synthetic",
                                "fixture", AUTH_USER, AUTH_PASS, timeout=10)
            thread = _serve(receiver)
            sink = Sink(("127.0.0.1", 0), {"name": "success"})
            sthread = _serve(sink)
            worker = None
            try:
                endpoint = "http://127.0.0.1:%d" % receiver.server_port
                greptime = _greptime_for(sink, body_budget)
                worker, applied = _make_worker(
                    archive, decoder, greptime, worker_interval,
                    outbox_limit, body_budget, backpressure)
                worker.start()
                payload = _enc(_meta("pipe-probe"), plan["probe"][0][1])
                req = urllib.request.Request(
                    endpoint + "/v1/logs",
                    data=gzip.compress(payload),
                    headers={"Authorization": "Basic " + base64.b64encode(
                        (AUTH_USER + ":" + AUTH_PASS).encode()).decode(),
                             "Content-Type": "application/x-protobuf",
                             "Content-Encoding": "gzip"})
                start = time.monotonic()
                with urllib.request.urlopen(req, timeout=10) as response:
                    check_response(response.read())

                def _lf_drained(status, remaining):
                    return (remaining == 0
                            and status.get("pending_rows", 0) == 0
                            and sink.acked_rows >= 1
                            and sink.dirty_statements >= 1)

                deadline = time.monotonic() + drain_timeout
                while True:
                    status, remaining = _status_view(
                        archive, decoder.epoch, detail=True)
                    if _lf_drained(status, remaining):
                        break
                    if time.monotonic() > deadline:
                        raise TimeoutError(
                            "lowflow sample drain deadline exceeded")
                    if not worker.is_alive():
                        raise RuntimeError(
                            "lowflow worker thread died mid-sample")
                    time.sleep(0.01)
                latencies.append(time.monotonic() - start)
            finally:
                if worker is not None:
                    worker.stop_event.set()
                    worker.join(10)
                    if worker.is_alive():
                        raise RuntimeError(
                            "lowflow worker thread failed to join")
                receiver.shutdown()
                receiver.server_close()
                thread.join(3)
                sink.shutdown()
                sink.server_close()
                sthread.join(3)
    ordered = sorted(latencies)
    return {"count": len(ordered), "latencies_s": ordered,
            "p50": _pct(ordered, 50), "p95": _pct(ordered, 95),
            "p99": _pct(ordered, 99),
            "worker_interval": worker_interval, "backpressure": applied}


def run_sustained(directory, plan, fixed_ns, sink_mode, offered_cps,
                  worker_interval, outbox_limit, body_budget, instrument,
                  drain_timeout, backpressure=None,
                  disable_production_instrumentation=False):
    """Loopback receive + Worker decode/flush against a loopback sink."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    previous = time.time_ns
    time.time_ns = lambda: fixed_ns  # noqa: E731 deterministic ingest rows
    recorder = StageRecorder(enabled=instrument)
    samples = []
    try:
        decoder = write_fixture(directory, "synthetic-pipe")
        old = write_fixture(directory, "synthetic-old")
        archive = Archive(str(directory / "sus.sqlite"),
                          disk_reserve_bytes=0)
        restore = _wrap_archive(archive, recorder)
        if disable_production_instrumentation:
            set_production_instrumentation(archive, False)
        receiver = None
        receiver_thread = None
        worker = None
        sink = Sink(("127.0.0.1", 0), {"name": "success"})
        sink_thread = _serve(sink)
        try:
            seed_retained(archive, plan, old)
            archive.register_epoch(decoder, explicit=True)
            receiver = Receiver(("127.0.0.1", 0), archive, "synthetic",
                                "fixture", AUTH_USER, AUTH_PASS, timeout=10)
            receiver_thread = _serve(receiver)
            endpoint = "http://127.0.0.1:%d" % receiver.server_port
            sizes_seed = _db_sizes(archive.path)
            # Low-flow probe: one chunk end to end through the live stack.
            probe_meta = _meta("pipe-probe")
            offered = 0
            wall0 = time.monotonic()
            offered += _post_batch(
                endpoint, probe_meta, plan["probe"][0][1], recorder)[0]
            greptime = _greptime_for(sink, body_budget)
            ack0 = time.monotonic()
            while archive.decode_once(decoder, limit=1000):
                pass
            _flush_drain(archive, greptime, outbox_limit, body_budget)
            lowflow_ack_s = time.monotonic() - ack0
            with sink.guard:
                sink.payloads.clear()
                sink.acked_batches.clear()
                sink.acked_rows = 0
                sink.dirty_statements = 0
            # Sustained phase: Worker decodes/flushes while offers arrive.
            # Hold the sink in outage so decode completes first (durable
            # outbox accumulates), giving a clean mid equality point.
            with sink.guard:
                sink.mode = {"name": "outage", "fails": 10 ** 9}
            worker, applied_bp = _make_worker(
                archive, decoder, greptime, worker_interval, outbox_limit,
                body_budget, backpressure)
            worker.start()
            offer_wall0 = time.monotonic()
            delay = 1.0 / offered_cps if offered_cps > 0 else 0
            for session, chunks in plan["dense"] + plan["mixed"]:
                for at in range(0, len(chunks), 40):
                    group = chunks[at:at + 40]
                    count, latency = _post_batch(
                        endpoint, _meta(session), group, recorder)
                    offered += count
                    recorder.add("raw_post", latency)
                    if delay:
                        time.sleep(delay)
            offer_wall = time.monotonic() - offer_wall0
            total_offered = offered  # already includes the low-flow probe chunk

            def decoded(status, remaining):
                return (status.get("raw_chunks", 0)
                        == plan["total_chunks"] and remaining == 0
                        and len(samples) > 0)

            _wait_for(decoded, drain_timeout, recorder, archive, samples,
                        decoder.epoch)
            mid = take_snapshot(archive, (decoder.epoch, old.epoch))
            sizes_decode = _db_sizes(archive.path)
            attempt_base = sink.attempts
            with sink.guard:
                if sink_mode == "fast":
                    sink.mode = {"name": "success"}
                elif sink_mode == "slow":
                    sink.mode = {"name": "success", "delay": 0.02}
                elif sink_mode == "outage-recovery":
                    sink.mode = {"name": "outage", "fails": 3}
                elif sink_mode == "partial-recovery":
                    sink.mode = {"name": "partial", "fails": 2}
                elif sink_mode == "dirty-recovery":
                    sink.mode = {"name": "dirty-fail", "fails": 2}
                else:
                    raise ValueError("unknown sink mode")
            drain0 = time.monotonic()

            def drained(status, remaining):
                return (status.get("pending_rows", 0) == 0
                        and remaining == 0)

            _wait_for(drained, drain_timeout, recorder, archive, samples,
                        decoder.epoch)
            drain_s = time.monotonic() - drain0
            wall = time.monotonic() - wall0
            final = take_snapshot(archive, (decoder.epoch, old.epoch))
            status, _ = _status_view(archive, decoder.epoch, detail=True)
            production_stages = collect_production_stages(archive)
            peak_pending = max((sample["pending_rows"] for sample in samples),
                               default=0)
            active = [entry for entry in (status.get("epochs") or [])
                      if entry.get("epoch") == decoder.epoch]
            retained = [entry for entry in (status.get("epochs") or [])
                        if entry.get("epoch") == old.epoch]
            epoch_flags = {
                "active_epoch": status.get("active_epoch"),
                "active_backlog_chunks": (active[0].get(
                    "active_backlog_chunks") if active else None),
                "retained_backlog_chunks": (retained[0].get(
                    "retained_backlog_chunks") if retained else None),
                "decoded_rows_exact": (active[0].get("decoded_rows_exact")
                                       if active else None)}
            total_rows = (final["counters"].get("decoded_rows_total", 0)
                          or 0)
            wall_floor = max(wall, 1e-9)
            counter_problems = {
                "mid": validate_counters(mid, status),
                "final": validate_counters(final, status)}
            result = {
                "counter_problems": counter_problems,
                "sink_mode": sink_mode,
                "mid": mid, "mid_sha": snapshot_sha(mid),
                "final": final, "final_sha": snapshot_sha(final),
                "payloads_sha": hashlib.sha256(
                    b"".join(sink.payloads)).hexdigest(),
                "payload_attempts": sink.attempts - attempt_base,
                "acked_rows": sink.acked_rows,
                "dirty_statements": sink.dirty_statements,
                "offered_chunks": total_offered,
                "offer_wall_s": offer_wall,
                "offered_per_s": total_offered / max(offer_wall, 1e-9),
                "achieved": {
                    "raw_per_s": plan["total_chunks"] / wall_floor,
                    "rows_per_s": total_rows / wall_floor,
                    "ack_per_s": (final["counters"].get(
                        "acked_rows_total", 0) or 0) / wall_floor,
                },
                "wall_s": wall, "drain_s": drain_s,
                "lowflow_ack_latency_s": lowflow_ack_s,
                "raw_post_p95_s": _pct(
                    sorted(recorder.samples["raw_post"]), 95),
                "stages": recorder.summary(),
                "production_stages": production_stages,
                "production_stages_available":
                production_stages is not None,
                "backlog_samples": len(samples),
                "peak_pending_rows": peak_pending,
                "epoch_flags": epoch_flags,
                "status_detail": True,
                "active_remaining": (active[0].get("remaining_chunks")
                                     if active else None),
                "retained_remaining": (retained[0].get("remaining_chunks")
                                       if retained else None),
                "sizes_seed": sizes_seed,
                "sizes_decode": sizes_decode,
                "sizes_final": _db_sizes(archive.path),
                "peak_rss_bytes": _peak_rss(),
                "decode_epoch": decoder.epoch, "old_epoch": old.epoch,
                "errors": status.get("errors", {}),
                "wake_config": {"worker_interval": worker_interval,
                                "outbox_limit": outbox_limit,
                                "max_body_bytes": body_budget,
                                "offered_cps": offered_cps,
                                "backpressure": applied_bp},
            }
            return result
        finally:
            if worker is not None:
                worker.stop_event.set()
                worker.join(10)
            if receiver is not None:
                receiver.shutdown()
                receiver.server_close()
                receiver_thread.join(3)
            sink.shutdown()
            sink.server_close()
            sink_thread.join(3)
            restore()
    finally:
        time.time_ns = previous


def run_kill_restart(directory, plan, fixed_ns, outbox_limit, body_budget):
    """SIGKILL a seeding child mid-drain, then reopen and prove equality.

    Uses the caller-supplied plan unchanged (real dense-frames): the child
    parks only after a durable decode_partial checkpoint exists, so the
    kill boundary always exercises dense partial resume.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pipe-ref-") as refdir:
        reference = run_reference(refdir, plan, fixed_ns, False,
                                  outbox_limit, body_budget)
    db_path = directory / "kill.sqlite"
    if db_path.exists():
        db_path.unlink()
    child_argv = [sys.executable, str(Path(__file__).resolve())]
    if _SOURCE_ROOT:
        child_argv += ["--source-root", _SOURCE_ROOT]
    child_argv += ["--child-seed",
                   "--db", str(db_path), "--workdir", str(directory),
                   "--fixed-ns", str(fixed_ns),
                   "--child-scale", str(plan["scale"]),
                   "--child-dense-frames", str(plan["dense_frames"])]
    child = subprocess.Popen(
        child_argv,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        cwd=str(CURRENT_ROOT))
    try:
        progress = 0
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            line = child.stdout.readline()
            if not line:
                break
            if "checkpoint" in line:
                progress = int(line.split()[1])
                break
        if not progress:
            child.kill()
            raise TimeoutError("seed child reached no partial checkpoint")
        child.send_signal(signal.SIGKILL)
        child.wait(timeout=30)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=30)
    archive = Archive(str(db_path), disk_reserve_bytes=0)
    decoder = write_fixture(directory, "synthetic-pipe")
    old = write_fixture(directory, "synthetic-old")
    archive.register_epoch(old)
    archive.register_epoch(decoder, explicit=True)
    with archive.connect() as conn:
        raw_chunks = conn.execute(
            "SELECT COUNT(*) FROM raw_chunks").fetchone()[0]
    while archive.decode_once(decoder, limit=1000):
        pass
    mid = take_snapshot(archive, (decoder.epoch, old.epoch))
    sink = Sink(("127.0.0.1", 0), {"name": "success"})
    thread = _serve(sink)
    try:
        greptime = _greptime_for(sink, body_budget)
        _flush_drain(archive, greptime, outbox_limit, body_budget)
    finally:
        sink.shutdown()
        sink.server_close()
        thread.join(3)
    final = take_snapshot(archive, (decoder.epoch, old.epoch))
    status, _ = _status_view(archive, decoder.epoch, detail=True)
    return {"killed_after_batches": progress,
            "counter_problems": {"mid": validate_counters(mid, status),
                                 "final": validate_counters(final, status)},
            "raw_chunks_preserved": raw_chunks,
            "expected_chunks": plan["total_chunks"],
            "plan_scale": plan["scale"],
            "plan_dense_frames": plan["dense_frames"],
            "mid_match": compare_scoped(reference["mid"], mid),
            "final_match": compare_scoped(reference["final"], final),
            "reference_mid_sha": reference["mid_sha"],
            "mid_sha": snapshot_sha(mid),
            "note": "process SIGKILL only: committed-prefix durability, "
                    "not a power-loss claim"}


def _child_seed(args):
    directory = Path(args.workdir)
    directory.mkdir(parents=True, exist_ok=True)
    fixed_ns = int(args.fixed_ns)
    previous = time.time_ns
    time.time_ns = lambda: fixed_ns  # noqa: E731
    try:
        plan = build_plan(scale=args.child_scale,
                          dense_frames=args.child_dense_frames)
        decoder = write_fixture(directory, "synthetic-pipe")
        old = write_fixture(directory, "synthetic-old")
        archive = Archive(args.db, disk_reserve_bytes=0)
        seed_retained(archive, plan, old)
        archive.register_epoch(decoder, explicit=True)
        for session, chunks in plan["batches"]:
            archive.accept(_meta(session), chunks)
        print("seeded %d" % plan["total_chunks"], flush=True)
        batches = 0
        checkpointed = False
        while archive.decode_once(decoder, limit=1):
            batches += 1
            with archive.connect() as conn:
                checkpointed = conn.execute(
                    "SELECT 1 FROM decode_partial LIMIT 1").fetchone() \
                    is not None
            print("progress %d%s" % (batches,
                                     " checkpoint" if checkpointed else ""),
                  flush=True)
            if checkpointed and batches >= 2:
                break
        if not checkpointed:
            print("no partial checkpoint reached", flush=True)
            return 1
        # Park mid-drain holding no locks; the parent SIGKILLs us here.
        time.sleep(300)
    finally:
        time.time_ns = previous


STATUS_SCALE_STAGES = ("status_default", "status_detail")


def measure_status_scale(sessions, fixed_ns):
    """Deterministic default/detail status latency vs retained history.

    Builds one archive with <sessions> fully-decoded retained sessions
    plus a leftover pending session, then times status() and
    status(detail=True) wall latency (getattr-safe; detail falls back
    when the flag is absent). Matches the core's revised meta+epoch
    complexity: default O(epochs) trigger-maintained counters, detail
    adds the counts_json pass. Returns {sessions, default_s, detail_s,
    detail_supported}.
    """
    previous = time.time_ns
    time.time_ns = lambda: fixed_ns  # noqa: E731 deterministic rows
    try:
        with tempfile.TemporaryDirectory(prefix="pipe-status-") as tmp:
            directory = Path(tmp)
            decoder = write_fixture(directory, "synthetic-pipe")
            archive = Archive(str(directory / "status.sqlite"),
                              disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            frame = b"t1232FFFF\r"
            for s in range(sessions):
                meta = _meta("pipe-status-%d" % s)
                archive.accept(meta, [{"seq": 0, "offset_ns": 0,
                                       "phase": "capture", "data": frame}])
                while archive.decode_once(decoder, limit=1000):
                    pass
            leftover = _meta("pipe-status-pending")
            archive.accept(leftover, [{"seq": 0, "offset_ns": 0,
                                       "phase": "capture", "data": frame}])
            start = time.monotonic()
            archive.status()
            default_s = time.monotonic() - start
            try:
                start = time.monotonic()
                archive.status(detail=True)
                detail_s = time.monotonic() - start
                supported = True
            except TypeError:
                detail_s = None
                supported = False
            return {"sessions": sessions, "retained_sessions": sessions,
                    "default_s": default_s, "detail_s": detail_s,
                    "detail_supported": supported,
                    "complexity": "default O(epochs) counters, "
                                  "detail +counts_json pass"}
    finally:
        time.time_ns = previous


def _reference_for_source(source_root, scale, dense_frames, fixed_ns,
                          outbox_limit, body_budget):
    """Run this candidate harness with --source-root against one tree.

    The harness file and test descriptor always come from the candidate
    checkout; --source-root prepends the tree under test to sys.path so its
    scripts.ingest.can sources load instead of the candidate's, with an
    isolated PYTHONPATH. No missing-tool fallback: baseline trees that
    predate the receiver API surface a hard error, never a skip.
    """
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--reference-only",
         "--source-root", str(source_root),
         "--scale", str(scale), "--dense-frames", str(dense_frames),
         "--fixed-ns", str(fixed_ns), "--outbox-limit", str(outbox_limit),
         "--max-body-bytes", str(body_budget)],
        capture_output=True, text=True, cwd=str(CURRENT_ROOT),
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
             "PYTHONPATH": str(source_root), "PYTHONNOUSERSITE": "1"},
        timeout=1200)
    if proc.returncode != 0:
        raise RuntimeError("reference for %s failed: %s"
                           % (source_root, proc.stderr[-2000:]))
    try:
        return json.loads(proc.stdout)
    except ValueError:
        raise RuntimeError("reference for %s printed no JSON" % source_root)


def _baseline_reference(baseline_root, scale, dense_frames, fixed_ns,
                        outbox_limit, body_budget, worker_interval,
                        offered_cps):
    baseline = _reference_for_source(baseline_root, scale, dense_frames,
                                     fixed_ns, outbox_limit, body_budget)
    baseline["baseline_available"] = True
    baseline["config"] = {"worker_interval": worker_interval,
                          "offered_cps": offered_cps}
    return baseline


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenarios", default="fast",
                        help="comma list of %s" % ",".join(SCENARIOS))
    parser.add_argument("--scale", type=int, default=1)
    parser.add_argument("--dense-frames", type=int, default=1200)
    parser.add_argument("--fixed-ns", type=int, default=FIXED_DEFAULT)
    parser.add_argument("--offered-cps", type=float, default=0)
    parser.add_argument("--worker-interval", type=float, default=0.02)
    parser.add_argument("--outbox-limit", type=int, default=20000)
    parser.add_argument("--max-body-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--drain-timeout", type=float, default=60)
    parser.add_argument("--lowflow-samples", type=int, default=LOWFLOW_DEFAULT)
    parser.add_argument("--require-production-stages", action="store_true")
    parser.add_argument("--status-scale", type=int, default=0)
    parser.add_argument("--high-rows", type=int, default=None)
    parser.add_argument("--low-rows", type=int, default=None)
    parser.add_argument("--high-bytes", type=int, default=None)
    parser.add_argument("--low-bytes", type=int, default=None)
    parser.add_argument("--downstream-retry-interval", type=float, default=None)
    parser.add_argument("--no-instrument", action="store_true")
    parser.add_argument("--compare-overhead", action="store_true")
    parser.add_argument("--baseline-root", default=None)
    parser.add_argument("--source-root", default=None,
                        help="test the receiver sources under this root "
                             "with the candidate harness file")
    parser.add_argument("--reference-only", action="store_true")
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--child-seed", action="store_true")
    parser.add_argument("--db", default=None)
    parser.add_argument("--workdir", default=None)
    parser.add_argument("--child-scale", type=int, default=1)
    parser.add_argument("--child-dense-frames", type=int,
                        default=1200)
    args = parser.parse_args(argv)
    if args.child_seed:
        if not args.db or not args.workdir or args.fixed_ns < 0:
            parser.error("--child-seed needs --db, --workdir, --fixed-ns")
        if args.child_scale < 1 or args.child_dense_frames < 1:
            parser.error("--child-scale/--child-dense-frames must be "
                         "positive")
        _child_seed(args)
        return 0
    if args.scale < 1 or args.dense_frames < 1 or args.fixed_ns < 0:
        parser.error("--scale/--dense-frames positive, --fixed-ns nonneg")
    if not math.isfinite(args.offered_cps) or args.offered_cps < 0:
        parser.error("--offered-cps must be finite and nonnegative")
    if not math.isfinite(args.worker_interval) or args.worker_interval <= 0:
        parser.error("--worker-interval must be finite and positive")
    if args.lowflow_samples < 0:
        parser.error("--lowflow-samples must be nonnegative")
    if args.status_scale < 0:
        parser.error("--status-scale must be nonnegative")
    if not 1 <= args.outbox_limit <= 20000 or args.max_body_bytes < 1:
        parser.error("--outbox-limit in 1..20000, --max-body-bytes >= 1")
    if args.reference_only:
        plan = build_plan(scale=args.scale, dense_frames=args.dense_frames)
        with tempfile.TemporaryDirectory(prefix="pipe-refonly-") as tmp:
            reference = run_reference(tmp, plan, args.fixed_ns, False,
                                      args.outbox_limit, args.max_body_bytes)
        view = scoped_view(reference["final"])
        print(json.dumps({"mid_sha": reference["mid_sha"],
                          "final_sha": reference["final_sha"],
                          "payloads_sha": reference["payloads_sha"],
                          "counters": view["counters"],
                          "derived": view["derived"],
                          "counter_problems": reference.get(
                              "counter_problems", {})}))
        return 0
    scenarios = [item.strip() for item in args.scenarios.split(",")
                 if item.strip()]
    unknown = [item for item in scenarios if item not in SCENARIOS]
    if not scenarios or unknown:
        parser.error("unknown scenarios: %s" % ",".join(unknown))
    plan = build_plan(scale=args.scale, dense_frames=args.dense_frames)
    if args.lowflow_samples:
        with tempfile.TemporaryDirectory(prefix="pipe-lowflow-") as lfdir:
            lowflow = run_lowflow_series(
                lfdir, plan, args.fixed_ns, args.outbox_limit,
                args.max_body_bytes, args.lowflow_samples,
                args.worker_interval, _plain_backpressure(args),
                args.drain_timeout)
    else:
        lowflow = {"count": 0, "latencies_s": [], "p50": None, "p95": None,
                   "p99": None}
    report = {"synthetic": True,
              "lowflow_ack": lowflow,
              "note": "synthetic fixture only; not production measurements",
              "plan": {"total_chunks": plan["total_chunks"],
                       "total_bytes": plan["total_bytes"]},
              "scenarios": {}, "mismatch": []}
    with tempfile.TemporaryDirectory(prefix="pipe-ref-") as refdir:
        reference = run_reference(refdir, plan, args.fixed_ns,
                                  not args.no_instrument, args.outbox_limit,
                                  args.max_body_bytes)
    if not reference["partial_seen"]:
        report["mismatch"].append(
            "reference never exercised the partial-resume path")
    for point, problems in reference.get("counter_problems", {}).items():
        for problem in problems:
            report["mismatch"].append("reference %s: %s" % (point, problem))
    report["reference_counters"] = {
        key: scoped_view(reference["final"])["counters"].get(key)
        for key in ("raw_chunks", "raw_bytes", "outbox_rows",
                    "decoded_rows_total", "acked_rows_total")}
    report["reference_derived"] = {
        "outbox_bytes": scoped_view(reference["final"])["derived"].get(
            "outbox_bytes")}
    report["reference"] = {
        "mid_sha": reference["mid_sha"],
        "final_sha": reference["final_sha"],
        "payloads_sha": reference["payloads_sha"],
        "acked_rows": reference["acked_rows"],
        "dirty_statements": reference["dirty_statements"],
        "decode_calls": reference["decode_calls"],
        "partial_resume_exercised": reference["partial_seen"],
        "wall_s": reference["wall_s"],
        "stages": reference["stages"]}
    for scenario in scenarios:
        with tempfile.TemporaryDirectory(prefix="pipe-sus-") as tmp:
            if scenario == "kill-restart":
                outcome = run_kill_restart(tmp, plan, args.fixed_ns,
                                           args.outbox_limit,
                                           args.max_body_bytes)
                for key in ("mid_match", "final_match"):
                    if outcome[key] is not None:
                        report["mismatch"].append(
                            "kill-restart %s: %s" % (key, outcome[key]))
                if (outcome["raw_chunks_preserved"]
                        != outcome["expected_chunks"]):
                    report["mismatch"].append("kill-restart lost raw")
                for point, problems in outcome.get(
                        "counter_problems", {}).items():
                    for problem in problems:
                        report["mismatch"].append(
                            "kill-restart %s: %s" % (point, problem))
                report["scenarios"][scenario] = outcome
                continue
            backpressure = {"high_rows": args.high_rows,
                                "low_rows": args.low_rows,
                                "high_bytes": args.high_bytes,
                                "low_bytes": args.low_bytes,
                                "retry_interval":
                                args.downstream_retry_interval}
            sustained = run_sustained(
                tmp, plan, args.fixed_ns, scenario, args.offered_cps,
                args.worker_interval, args.outbox_limit, args.max_body_bytes,
                not args.no_instrument, args.drain_timeout, backpressure)
            mid_diff = compare_scoped(reference["mid"], sustained["mid"])
            if mid_diff is not None:
                report["mismatch"].append(
                    "%s pre-flush: %s" % (scenario, mid_diff))
            final_diff = compare_scoped(reference["final"],
                                        sustained["final"])
            if final_diff is not None:
                report["mismatch"].append(
                    "%s final: %s" % (scenario, final_diff))
            for point, problems in sustained.get(
                    "counter_problems", {}).items():
                for problem in problems:
                    report["mismatch"].append(
                        "%s %s: %s" % (scenario, point, problem))
            if scenario in ("fast", "slow"):
                if (sustained["payloads_sha"]
                        != reference["payloads_sha"]):
                    report["mismatch"].append(
                        "%s Greptime payload bytes differ" % scenario)
            else:
                # Recovery retries resubmit the durable prefix: the sink
                # observes duplicates by design; the full snapshot comparison
                # above already pins durable correctness.
                pass
            if (args.require_production_stages
                    and not sustained.get("production_stages_available")):
                report["mismatch"].append(
                    "%s: production stages unavailable" % scenario)
            sustained["counter_summary"] = {
                key: scoped_view(sustained["final"])["counters"].get(key)
                for key in ("outbox_rows", "decoded_rows_total",
                            "acked_rows_total")}
            sustained["derived_summary"] = scoped_view(
                sustained["final"])["derived"]
            sustained.pop("mid")
            sustained.pop("final")
            report["scenarios"][scenario] = sustained
    if args.compare_overhead:
        with tempfile.TemporaryDirectory(prefix="pipe-off-") as tmp:
            plain = run_sustained(
                tmp, plan, args.fixed_ns, "fast", args.offered_cps,
                args.worker_interval, args.outbox_limit, args.max_body_bytes,
                False, args.drain_timeout,
                _plain_backpressure(args),
                disable_production_instrumentation=True)
        faster = report["scenarios"].get("fast")
        if faster is not None:
            report["instrument_overhead"] = {
                "harness_local_timer": {
                    "instrumented_wall_s": faster["wall_s"],
                    "plain_wall_s": plain["wall_s"],
                    "ratio": (faster["wall_s"] / plain["wall_s"]
                              if plain["wall_s"] > 0 else None)},
                "production_hooks": {
                    "instrumented_available":
                    faster.get("production_stages_available", False),
                    "plain_available":
                    plain.get("production_stages_available", False),
                    "note": "production on/off delta lives in "
                            "CanObservability; harness reports hook "
                            "availability and coarse timer ratio only"}}
    if args.baseline_root is not None:
        # Same offered rate, same outbox/body budget, same dense plan on
        # both sides. Caller passes the same --worker-interval for both
        # (parent uses the original interval 1): _baseline_reference
        # forwards worker_interval so polling can never differ silently.
        baseline = _baseline_reference(args.baseline_root, args.scale,
                                       args.dense_frames, args.fixed_ns,
                                       args.outbox_limit, args.max_body_bytes,
                                       args.worker_interval, args.offered_cps)
        candidate = _reference_for_source(
            args.source_root or CURRENT_ROOT, args.scale, args.dense_frames,
            args.fixed_ns, args.outbox_limit, args.max_body_bytes)
        candidate["config"] = {"worker_interval": args.worker_interval,
                               "offered_cps": args.offered_cps}
        report["baseline"] = baseline
        report["candidate_reference"] = candidate
        report["baseline_config"] = {
            "worker_interval": args.worker_interval,
            "offered_cps": args.offered_cps}
        if baseline.get("baseline_available"):
            for key in ("mid_sha", "final_sha"):
                if baseline.get(key) != candidate.get(key):
                    report["mismatch"].append("baseline %s differs" % key)
    if args.status_scale:
        report["status_scale"] = measure_status_scale(
            args.status_scale, args.fixed_ns)
    payload = json.dumps(report, indent=2, sort_keys=True)
    print(payload)
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(payload + "\n", encoding="utf-8")
    return 1 if report["mismatch"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
