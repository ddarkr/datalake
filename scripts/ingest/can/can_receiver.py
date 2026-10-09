"""Authenticated OTLP raw archive and resumable CAN-to-Greptime worker.

A successful /v1/logs response acknowledges SQLite raw durability, not Greptime.
Raw chunks are never deleted. Run from /app with ``python -m scripts.ingest.can.can_receiver serve|status|re-decode``.
"""
from __future__ import annotations

import argparse
import base64
from collections import deque
import contextlib
import datetime
import fcntl
import hashlib
import heapq
import hmac
import http.client
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import zlib

from scripts.ingest.can.can_decoder import Decoder
from scripts.ingest.can.can_otlp_wire import MAX_REQUEST_BYTES, decode_batch, success_response

COLUMNS = (
    "event_time", "vehicle", "path", "source", "event_id", "decode_epoch",
    "value_num", "value_text", "value_bool", "unit", "vss_version",
    "vehicle_firmware", "dbc_primary_commit", "dbc_supplemental_commit",
    "dbc_override_version", "dbc_override_commit", "mapping_revision",
    "collector_version", "ingest_time", "source_system", "source_field",
    "collector_id", "source_is_resend", "quality", "envelope_id",
    "config_version", "connectivity",
)
_COLUMN_SET = frozenset(COLUMNS)
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
INT64_MAX = 2**63 - 1
SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
 id INTEGER PRIMARY KEY, vehicle TEXT NOT NULL, collector_id TEXT NOT NULL,
 session_id TEXT NOT NULL, meta_json TEXT NOT NULL,
 next_seq INTEGER NOT NULL DEFAULT 0, last_offset_ns INTEGER NOT NULL DEFAULT -1,
 UNIQUE(vehicle,collector_id,session_id));
CREATE TABLE IF NOT EXISTS raw_chunks (
 id INTEGER PRIMARY KEY, session INTEGER NOT NULL REFERENCES sessions(id),
 seq INTEGER NOT NULL, offset_ns INTEGER NOT NULL, phase TEXT NOT NULL,
 data BLOB NOT NULL, UNIQUE(session,seq));
CREATE TABLE IF NOT EXISTS epochs (
 epoch TEXT PRIMARY KEY, mapping_revision TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decode_states (
 session INTEGER NOT NULL REFERENCES sessions(id),
 epoch TEXT NOT NULL REFERENCES epochs(epoch), next_seq INTEGER NOT NULL,
 state_json TEXT NOT NULL, counts_json TEXT NOT NULL, rows INTEGER NOT NULL,
 PRIMARY KEY(session,epoch));
CREATE TABLE IF NOT EXISTS decode_partial (
 session INTEGER NOT NULL REFERENCES sessions(id),
 epoch TEXT NOT NULL REFERENCES epochs(epoch), seq INTEGER NOT NULL,
 state_json TEXT NOT NULL, counts_json TEXT NOT NULL, rows_emitted INTEGER NOT NULL,
 PRIMARY KEY(session,epoch));
CREATE TABLE IF NOT EXISTS decode_pending (
 session INTEGER NOT NULL REFERENCES sessions(id),
 epoch TEXT NOT NULL REFERENCES epochs(epoch),
 PRIMARY KEY(session,epoch));
CREATE INDEX IF NOT EXISTS decode_pending_epoch ON decode_pending(epoch);
CREATE TABLE IF NOT EXISTS outbox (
 id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
 epoch TEXT NOT NULL REFERENCES epochs(epoch), row_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS worker_errors (
 kind TEXT PRIMARY KEY, count INTEGER NOT NULL, last_ns INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS archive_meta (
 key TEXT PRIMARY KEY, value INTEGER NOT NULL);
"""


class Rejection(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason
        super().__init__(reason)


class DownstreamError(Exception):
    """Only fixed non-sensitive error codes may enter persisted status."""

class ConfigurationError(ValueError):
    """Static, actionable configuration codes, never environment values."""



def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _identity(value):
    if not isinstance(value, str) or not IDENTITY.fullmatch(value):
        raise ConfigurationError("invalid_configured_identity")
    return value


def _credentials(prefix):
    user, password = os.environ.get(prefix + "_USER", ""), os.environ.get(prefix + "_PASSWORD", "")
    if not user or not password or ":" in user:
        raise ConfigurationError(prefix.lower() + "_credentials_required")
    return user, password


def _archive_path(path):
    path = Path(path).absolute()
    resolved = path.resolve()
    workspace = Path(__file__).resolve().parents[3]
    if resolved == workspace or workspace in resolved.parents:
        raise ConfigurationError("archive_must_be_outside_workspace")
    return path


@contextlib.contextmanager
def exclusive_archive(path):
    """Serve and explicit replay cannot race another process's checkpoint."""
    fd = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ConfigurationError("archive_already_in_use") from None
        yield
    finally:
        os.close(fd)

DECODE_ROW_BUDGET = 2000


def _meta_bump(conn, key, delta):
    conn.execute("INSERT INTO archive_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=value+?",
                 (key, delta, delta))


def _meta_set(conn, key, value):
    conn.execute("INSERT INTO archive_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (key, value))


def _migrate_counters(conn):
    """Bounded one-time migration: populate incremental counters from a legacy DB, then serve them without scans."""
    if conn.execute("SELECT value FROM archive_meta WHERE key='raw_chunks'").fetchone() is not None:
        return False
    raw, raw_bytes = conn.execute("SELECT COUNT(*),COALESCE(SUM(length(data)),0) FROM raw_chunks").fetchone()
    outbox = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    decoded = conn.execute("SELECT COALESCE(SUM(rows),0) FROM decode_states").fetchone()[0]
    for key, value in (("raw_chunks", raw), ("raw_bytes", raw_bytes), ("outbox_rows", outbox),
                       ("decoded_rows_total", decoded), ("acked_rows_total", 0)):
        conn.execute("INSERT OR IGNORE INTO archive_meta(key,value) VALUES(?,?)", (key, value))
    return True


def _refresh_pending_epoch(conn, epoch):
    """Reconcile one epoch's pending membership from cursors inside the caller's transaction.

    Pending holds exactly sessions with raw beyond the epoch cursor (or no
    cursor yet): completions delete during decode commit, new arrivals insert
    during accept. Crash/rollback safety comes free: both run in the same
    transaction as the cursor/raw write, so a lost commit leaves no phantom.
    """
    conn.execute(
        "DELETE FROM decode_pending WHERE epoch=? AND session NOT IN "
        "(SELECT s.id FROM sessions s LEFT JOIN decode_states d ON d.session=s.id AND d.epoch=? "
        "WHERE EXISTS(SELECT 1 FROM raw_chunks c WHERE c.session=s.id AND c.seq>=COALESCE(d.next_seq,0)))",
        (epoch, epoch),
    )
    conn.execute(
        "INSERT OR IGNORE INTO decode_pending(session,epoch) "
        "SELECT s.id,? FROM sessions s LEFT JOIN decode_states d ON d.session=s.id AND d.epoch=? "
        "WHERE EXISTS(SELECT 1 FROM raw_chunks c WHERE c.session=s.id AND c.seq>=COALESCE(d.next_seq,0))",
        (epoch, epoch),
    )


def candidate_sql():
    """Pending-head merge source shared by decode_once and runners.

    One row per pending session at its persisted next-seq cursor, in global
    raw arrival order: bind the epoch twice. decode_once pages each head
    forward with the indexed (session, seq) key, so retained completed
    history is never scanned and no staged-progress CTE is needed. BLOBs
    stay out: heads carry (id, session, seq) only; chunk bytes load per
    session page under a global byte cap.
    """
    return (
        "SELECT c.id AS id, c.session AS session, c.seq AS seq "
        "FROM decode_pending q "
        "CROSS JOIN sessions s ON s.id=q.session AND q.epoch=? "
        "LEFT JOIN decode_states d ON d.session=s.id AND d.epoch=? "
        "JOIN raw_chunks c ON c.session=s.id AND c.seq=COALESCE(d.next_seq,0) "
        "ORDER BY c.id"
    )


# Per-session page size for bounded prefetch: indexed (session, seq) pages of
# full rows behind each pending head, merged by global raw id in a heap.
DECODE_PAGE_ROWS = 64
# Global cap on prefetched raw chunk bytes across sessions: tiny serial
# frames decode far below this, so one batch normally stages one read pass.
DECODE_PREFETCH_BYTES = 1024 * 1024


def _reconcile_pending_session(conn, epoch, session_id):
    """Refresh one session's pending membership inside the caller's transaction.

    Cursor-first: one PK cursor read, then one indexed (session, seq>=bound)
    probe. Never joins or scans retained history, so commit cost stays
    bounded no matter how much completed prefix is retained.
    """
    row = conn.execute(
        "SELECT next_seq FROM decode_states WHERE session=? AND epoch=?",
        (session_id, epoch),
    ).fetchone()
    bound = row["next_seq"] if row else 0
    pending = conn.execute(
        "SELECT 1 FROM raw_chunks WHERE session=? AND seq>=? LIMIT 1",
        (session_id, bound),
    ).fetchone()
    if pending is None:
        conn.execute("DELETE FROM decode_pending WHERE epoch=? AND session=?", (epoch, session_id))
    else:
        conn.execute("INSERT OR IGNORE INTO decode_pending(session,epoch) VALUES(?,?)", (session_id, epoch))


def _migrate_pending(conn):
    """One-time build of decode_pending for pre-pending archives; later opens are a marker probe."""
    if conn.execute("SELECT value FROM archive_meta WHERE key='pending_migrated'").fetchone() is not None:
        return False
    for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
        _refresh_pending_epoch(conn, epoch)
    _meta_set(conn, "pending_migrated", 1)
    return True


HOUR_NS = 3600 * 10**9


def _ns_timestamp(value):
    """Greptime string-literal timestamp matching the Analytics ts_lit convention."""
    moment = datetime.datetime.fromtimestamp(value / 10**9, tz=datetime.timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + "%09d" % (value % 10**9)


def _hour_floor(value):
    return (value // HOUR_NS) * HOUR_NS


def _sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def _dirty_inserts(rows, received_at):
    """One INSERT row per affected hour bucket: original event_time floor, stable generation, single notify time.

    Generation is sha256 over the sorted ACKed canonical row_json bytes in the
    bucket (exact identities AND content): a same-PK value/quality/unit
    correction changes the hash, while an identical replay reproduces it.
    """
    buckets = {}
    for canonical, row in rows:
        key = (_hour_floor(row["event_time"]), row["vehicle"], row["source"], row["decode_epoch"])
        buckets.setdefault(key, []).append(canonical)
    statements = []
    for (window_ns, vehicle, source, epoch), canonicals in sorted(buckets.items()):
        digest = hashlib.sha256()
        for canonical in sorted(canonicals):
            digest.update(canonical.encode())
            digest.update(b"\0")
        statements.append("(" + ",".join((
            _sql_string(_ns_timestamp(window_ns)[:19]),
            _sql_string(vehicle), _sql_string(source), _sql_string(epoch),
            _sql_string(digest.hexdigest()), _sql_string(received_at))) + ")")
    return statements

class Archive:
    def __init__(self, path, disk_reserve_bytes=64 * 1024 * 1024):
        self.path = _archive_path(path)
        self.disk_reserve_bytes = disk_reserve_bytes
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        parent = self.path.parent.stat()
        if self.path.parent.is_symlink() or parent.st_uid != os.getuid() or parent.st_mode & 0o077:
            raise ConfigurationError("archive_directory_requires_private_0700_and_ownership")
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            if os.fstat(fd).st_uid != os.getuid():
                raise ConfigurationError("archive_requires_ownership")
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            _migrate_counters(conn)
            _migrate_pending(conn)
            conn.commit()



    def connect(self):
        conn = sqlite3.connect(self.path, timeout=5)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
        except sqlite3.Error:
            conn.close()
            raise
        return contextlib.closing(conn)

    def reserve(self, additional=0):
        if shutil.disk_usage(self.path.parent).free < self.disk_reserve_bytes + additional:
            raise Rejection(503, "archive_disk_reserve")

    def accept(self, meta, chunks):
        """Commit all new chunks, or none; matching overlap is legal on ACK loss."""
        encoded = _json(meta)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM sessions WHERE vehicle=? AND collector_id=? AND session_id=?",
                (meta["vehicle"], meta["collector_id"], meta["session_id"]),
            ).fetchone()
            if session is None:
                if chunks[0]["seq"] != 0:
                    raise Rejection(409, "sequence_gap")
                self.reserve(sum(len(c["data"]) for c in chunks))
                cursor = conn.execute(
                    "INSERT INTO sessions(vehicle,collector_id,session_id,meta_json) VALUES(?,?,?,?)",
                    (meta["vehicle"], meta["collector_id"], meta["session_id"], encoded),
                )
                session_id, next_seq, last_offset = cursor.lastrowid, 0, -1
            else:
                if session["meta_json"] != encoded:
                    raise Rejection(409, "metadata_conflict")
                session_id, next_seq, last_offset = session["id"], session["next_seq"], session["last_offset_ns"]
                if any(c["seq"] >= next_seq for c in chunks):
                    self.reserve(sum(len(c["data"]) for c in chunks if c["seq"] >= next_seq))
            added = 0
            for chunk in chunks:
                seq, offset, phase, data = (chunk[k] for k in ("seq", "offset_ns", "phase", "data"))
                if seq < next_seq:
                    old = conn.execute(
                        "SELECT offset_ns,phase,data FROM raw_chunks WHERE session=? AND seq=?",
                        (session_id, seq),
                    ).fetchone()
                    if old is None or (old["offset_ns"], old["phase"], old["data"]) != (offset, phase, data):
                        raise Rejection(409, "chunk_conflict")
                elif seq != next_seq or offset < last_offset or seq == INT64_MAX:
                    raise Rejection(409, "sequence_gap")
                else:
                    conn.execute(
                        "INSERT INTO raw_chunks(session,seq,offset_ns,phase,data) VALUES(?,?,?,?,?)",
                        (session_id, seq, offset, phase, data),
                    )
                    next_seq, last_offset, added = seq + 1, offset, added + 1
            conn.execute("UPDATE sessions SET next_seq=?,last_offset_ns=? WHERE id=?", (next_seq, last_offset, session_id))
            if added:
                new_bytes = sum(len(c["data"]) for c in chunks if c["seq"] >= next_seq - added)
                _meta_bump(conn, "raw_chunks", added)
                _meta_bump(conn, "raw_bytes", new_bytes)
                _meta_set(conn, "last_receive_ns", time.time_ns())
                for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
                    _reconcile_pending_session(conn, epoch, session_id)
            conn.commit()
            return added

    def register_epoch(self, decoder, explicit=False):
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT mapping_revision FROM epochs WHERE epoch=?", (decoder.epoch,)).fetchone()
            if existing:
                if existing[0] != decoder.mapping_revision:
                    raise ConfigurationError("decoder_epoch_mapping_conflict")
            else:
                if not explicit and conn.execute("SELECT 1 FROM epochs LIMIT 1").fetchone():
                    raise ConfigurationError("new_decoder_epoch_requires_re-decode")
                conn.execute("INSERT INTO epochs VALUES(?,?)", (decoder.epoch, decoder.mapping_revision))
                _refresh_pending_epoch(conn, decoder.epoch)
            conn.commit()

    def decode_once(self, decoder, limit=1000):
        """Decode an ordered multi-chunk batch without holding the writer lock during CPU work.

        Chunks and parser cursors are read briefly, decoded outside any SQLite
        transaction via Decoder.decode_some(meta,chunk,state,max_rows), then
        committed ONCE (cursor CAS + all rows + partial state + counters). A
        failure anywhere in the batch commits nothing: cursors, outbox rows,
        counters, and freshness stay exactly as before the call. A partial
        chunk still survives crashes via durable decode_partial state committed
        with its batch; rows stay under plain INSERT (re-emit fails loudly via
        UNIQUE instead of silently duplicating).
        """
        decode_some = decoder.decode_some
        # Release the writer lock between batches so raw ingestion can proceed.
        # Global row budget across the batch: a dense chunk that does not finish
        # within budget stages alone and commits its resume state; later chunks
        # wait for the next call so no remaining frames are skipped.
        deadline = time.monotonic() + 0.05
        if limit <= 0:
            return 0
        window = getattr(self, "_decode_prefetch_limit", 1000)
        prefetch_limit = min(limit, window)
        # Merge a bounded prefix while reading, then release the snapshot before
        # decoding. Unpaged heads remain in the heap: exhausting a page or the
        # byte budget must never let a later session bypass the earliest chunk.
        with self.connect() as conn:
            conn.execute("BEGIN")
            heap = [(head["id"], head["session"], head["seq"]) for head in
                    conn.execute(candidate_sql(), (decoder.epoch, decoder.epoch))]
            heapq.heapify(heap)
            pages, anchors, partials, metadata = {}, {}, {}, {}
            prefix = []
            prefetched_bytes = fetched_rows = 0
            while heap and len(prefix) < prefetch_limit:
                _, session, want = heapq.heappop(heap)
                run = pages.get(session)
                if not run:
                    if fetched_rows >= prefetch_limit or prefetched_bytes >= DECODE_PREFETCH_BYTES:
                        break
                    run = deque()
                    with contextlib.closing(conn.execute(
                        "SELECT c.id,c.session,c.seq,c.offset_ns,c.phase,c.data,s.meta_json "
                        "FROM raw_chunks c JOIN sessions s ON s.id=c.session "
                        "WHERE c.session=? AND c.seq>=? ORDER BY c.seq LIMIT ?",
                        (session, want, min(DECODE_PAGE_ROWS, prefetch_limit - fetched_rows)),
                    )) as cursor:
                        for row in cursor:
                            if row["seq"] != want + len(run):
                                break
                            if prefetched_bytes + len(row["data"]) > DECODE_PREFETCH_BYTES:
                                break
                            run.append(row)
                            prefetched_bytes += len(row["data"])
                            fetched_rows += 1
                    if not run:
                        break
                    pages[session] = run
                    if session not in anchors:
                        metadata[session] = json.loads(run[0]["meta_json"])
                        anchors[session] = conn.execute(
                            "SELECT next_seq,state_json,counts_json,rows FROM decode_states "
                            "WHERE session=? AND epoch=?", (session, decoder.epoch),
                        ).fetchone()
                        partials[session] = conn.execute(
                            "SELECT seq,state_json,counts_json,rows_emitted FROM decode_partial "
                            "WHERE session=? AND epoch=?", (session, decoder.epoch),
                        ).fetchone()
                raw = run.popleft()
                prefix.append(raw)
                if len(prefix) >= prefetch_limit:
                    break
                if run:
                    next_head = run[0]
                else:
                    next_head = conn.execute(
                        "SELECT id,seq FROM raw_chunks WHERE session=? AND seq=?",
                        (session, raw["seq"] + 1),
                    ).fetchone()
                if next_head is not None:
                    heapq.heappush(heap, (next_head["id"], session, next_head["seq"]))
        # Only the prefix is needed now; discard speculative page rows before
        # allocating decoded rows. Raw bytes are capped at 1MiB, fetched chunks
        # at `limit`, with at most one transient wire-capped row during reading.
        pages.clear()
        previous = {}
        staged = []
        emitted_rows = 0
        if not prefix:
            return 0
        self.reserve()
        for raw in prefix:
            session = raw["session"]
            meta = metadata[session]
            chunk = {key: raw[key] for key in ("seq", "offset_ns", "phase", "data")}
            anchor = anchors[session]
            partial = partials[session]
            if partial is not None and partial["seq"] == raw["seq"]:
                state = json.loads(partial["state_json"])
                carried, emitted = json.loads(partial["counts_json"]), partial["rows_emitted"]
                totals = json.loads(anchor["counts_json"]) if anchor and anchor["counts_json"] else {}
            elif session in previous:
                # Continue from the staged predecessor's output state: it has
                # not committed yet, so the persisted cursor still points at it.
                state, totals = previous[session][0], dict(previous[session][1])
                carried, emitted = {}, 0
            else:
                state = json.loads(anchor["state_json"]) if anchor and anchor["state_json"] else None
                carried, emitted = {}, 0
                totals = json.loads(anchor["counts_json"]) if anchor and anchor["counts_json"] else {}
            rows, state, counts, done = decode_some(meta, chunk, state, max(1, DECODE_ROW_BUDGET - emitted_rows))
            if type(done) is not bool:
                raise ValueError("invalid decode batch completion flag")
            for deltas in ([carried] if carried else []) + [counts]:
                for key, value in deltas.items():
                    if type(value) is not int or value < 0:
                        raise ValueError("invalid decoder count")
                    totals[key] = value if key == "tail_bytes" else totals.get(key, 0) + value
            for row in rows:
                validate_row(row)
                if row["vehicle"] != meta["vehicle"] or row["collector_id"] != meta["collector_id"] or row["decode_epoch"] != decoder.epoch:
                    raise ValueError("decoder row identity mismatch")
            if not done and not rows:
                raise ValueError("decoder partial batch must emit rows")
            staged.append((raw, chunk, {"state": state, "counts": counts, "done": done, "totals": totals,
                                        "carried": carried, "emitted": emitted, "rows": rows,
                                        "partial": partial, "anchor": anchor}))
            previous[session] = (state, totals)
            emitted_rows += len(rows)
            if not done or emitted_rows >= DECODE_ROW_BUDGET or time.monotonic() >= deadline:
                break
        if not staged:
            return 0
        for _, _, work in staged:
            work["canonicals"] = [_json(row) for row in work["rows"]]
        staged_bytes = sum(len(c.encode()) for _, _, work in staged for c in work["canonicals"])
        self.reserve(staged_bytes + len(staged) * 512 + staged_bytes // 4)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            # Validate each touched session ONCE before any writes, against
            # the state read in the prefetch pass (not an empty default):
            # staged same-session writes are ours, not an external race.
            order = {}
            for raw, chunk, work in staged:
                order.setdefault(raw["session"], []).append((raw, work))
            for session, entries in order.items():
                first_raw, first_work = entries[0]
                before = anchors[session]
                anchor = conn.execute(
                    "SELECT next_seq,state_json,counts_json,rows FROM decode_states WHERE session=? AND epoch=?",
                    (session, decoder.epoch),
                ).fetchone()
                if (anchor["next_seq"] if anchor else 0) != (before["next_seq"] if before else 0) or \
                        (anchor["state_json"] if anchor else None) != (before["state_json"] if before else None) or \
                        (anchor["counts_json"] if anchor else None) != (before["counts_json"] if before else None) or \
                        (anchor["rows"] if anchor else 0) != (before["rows"] if before else 0):
                    raise ValueError("decode cursor moved during decode")
                if first_work["partial"] is not None:
                    current = conn.execute(
                        "SELECT seq,rows_emitted FROM decode_partial WHERE session=? AND epoch=?",
                        (session, decoder.epoch),
                    ).fetchone()
                    if first_work["partial"]["seq"] == first_raw["seq"]:
                        if current is None or current["seq"] != first_raw["seq"] or current["rows_emitted"] != first_work["emitted"]:
                            raise ValueError("decode partial moved during decode")
                    elif current is not None and current["seq"] != first_raw["seq"]:
                        conn.execute("DELETE FROM decode_partial WHERE session=? AND epoch=?",
                                     (session, decoder.epoch))
            # Outbox stays in staged (global raw arrival) order; per-chunk
            # cursor writes collapse to the last done entry plus one trailing
            # partial entry per session. Only the batch-final entry can be
            # partial (it ends staging), so the last entry's own carried
            # counts are exactly the resume prefix, never an earlier chunk's.
            conn.executemany(
                "INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
                [(row["event_id"], decoder.epoch, canonical) for _, _, work in staged
                 for row, canonical in zip(work["rows"], work["canonicals"])],
            )
            total_rows = 0
            for session, entries in order.items():
                first_work = entries[0][1]
                base = (first_work["anchor"]["rows"] if first_work["anchor"] else 0) + first_work["emitted"]
                done_entries = [(raw, work) for raw, work in entries if work["done"]]
                if done_entries:
                    last_raw, last_work = done_entries[-1]
                    done_count = sum(len(work["rows"]) for _, work in done_entries)
                    conn.execute(
                        "INSERT INTO decode_states VALUES(?,?,?,?,?,?) ON CONFLICT(session,epoch) DO UPDATE SET "
                        "next_seq=excluded.next_seq,state_json=excluded.state_json,counts_json=excluded.counts_json,rows=excluded.rows",
                        (session, decoder.epoch, last_raw["seq"] + 1, _json(last_work["state"]), _json(last_work["totals"]), base + done_count),
                    )
                last_raw, last_work = entries[-1]
                if not last_work["done"]:
                    merged = dict(last_work["carried"])
                    for key, value in last_work["counts"].items():
                        merged[key] = value if key == "tail_bytes" else merged.get(key, 0) + value
                    conn.execute(
                        "INSERT INTO decode_partial VALUES(?,?,?,?,?,?) ON CONFLICT(session,epoch) DO UPDATE SET "
                        "seq=excluded.seq,state_json=excluded.state_json,counts_json=excluded.counts_json,rows_emitted=excluded.rows_emitted",
                        (session, decoder.epoch, last_raw["seq"], _json(last_work["state"]), _json(merged), last_work["emitted"] + len(last_work["rows"])),
                    )
                else:
                    conn.execute("DELETE FROM decode_partial WHERE session=? AND epoch=?",
                                 (session, decoder.epoch))
                total_rows += sum(len(work["rows"]) for _, work in entries)
            _meta_bump(conn, "outbox_rows", total_rows)
            _meta_bump(conn, "decoded_rows_total", total_rows)
            for session in order:
                _reconcile_pending_session(conn, decoder.epoch, session)
            _meta_set(conn, "last_decode_ns", time.time_ns())
            conn.commit()
        # A deadline/row-budget stop leaves speculative rows unused. Match the
        # next read to actual progress; grow again after consuming a full window.
        # This is an in-memory hint only: rollback/restart cannot move cursors.
        if limit >= window:
            if len(staged) < len(prefix):
                self._decode_prefetch_limit = max(1, len(staged))
            elif len(prefix) == prefetch_limit and staged[-1][2]["done"] and emitted_rows < DECODE_ROW_BUDGET:
                self._decode_prefetch_limit = min(limit, window * 2)
        return len(staged)

    def rebuild_pending(self):
        """Repair path for archives touched by outside writers: rescan all epochs in one transaction."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
                _refresh_pending_epoch(conn, epoch)
            _meta_set(conn, "pending_migrated", 1)
            conn.commit()

    def record_error(self, kind):
        if kind not in {"decode_failure", "greptime_failure", "greptime_partial_ack", "greptime_timeout", "greptime_row_too_large", "archive_disk_reserve", "dirty_notify_failure"}:
            raise ValueError("invalid error category")
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO worker_errors VALUES(?,1,?) ON CONFLICT(kind) DO UPDATE SET count=count+1,last_ns=excluded.last_ns",
                (kind, time.time_ns()),
            )
            conn.commit()

    def flush_once(self, greptime, limit=20000, max_body_bytes=None):
        """Send one fitting ordered prefix; dirty-notify after full ACK, delete only then."""
        budget = getattr(greptime, "max_body_bytes", DEFAULT_MAX_BODY_BYTES) if max_body_bytes is None else max_body_bytes
        prefix = "sql=" + urllib.parse.quote_plus(_SQL_PREFIX, safe="", encoding="utf-8", errors="strict")
        total = len(prefix.encode("ascii"))
        chosen, count, texts = [], 0, []
        with self.connect() as conn:
            for record in conn.execute("SELECT id,row_json FROM outbox ORDER BY id LIMIT ?", (limit,)):
                canonical = record["row_json"]
                row = json.loads(canonical)
                cell = _render_cell(row)
                addition = _urlencoded_len(cell) + (0 if count == 0 else _COMMA_ENCODED_LEN)
                if total + addition > budget:
                    break
                total += addition
                chosen.append((record["id"], canonical, row))
                texts.append(cell)
                count += 1
                if count >= limit:
                    break
        if not chosen:
            with self.connect() as conn:
                if conn.execute("SELECT 1 FROM outbox LIMIT 1").fetchone() is None:
                    return 0
            # The oversized head row stays durable; nothing is skipped or deleted.
            raise DownstreamError("greptime_row_too_large")
        rows = [(canonical, row) for _, canonical, row in chosen]
        payload = encode_body(_SQL_PREFIX + ",".join(texts))
        if greptime.send_body(payload) != count:
            raise DownstreamError("greptime_partial_ack")
        received_at = _ns_timestamp(time.time_ns())
        statements = _dirty_inserts(rows, received_at)
        dirty = encode_body('INSERT INTO "vehicle_signal_dirty" '
                            '("window_start","vehicle","source","decode_epoch","generation","received_at") VALUES '
                            + ",".join(statements))
        try:
            affected = greptime.send_body(dirty)
        except DownstreamError:
            raise DownstreamError("dirty_notify_failure") from None
        if affected != len(statements):
            raise DownstreamError("dirty_notify_failure")
        now = time.time_ns()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany("DELETE FROM outbox WHERE id=?", [(ident,) for ident, _, _ in chosen])
            _meta_bump(conn, "outbox_rows", -len(chosen))
            _meta_bump(conn, "acked_rows_total", len(chosen))
            _meta_set(conn, "last_full_ack_ns", now)
            conn.commit()
        return count

    def status(self):
        facts = archive_status(self.path)
        facts["disk_reserve_bytes"] = self.disk_reserve_bytes
        return facts


def archive_status(path):
    path = _archive_path(path)
    uri = path.as_uri() + "?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=5)) as conn:
        conn.execute("BEGIN")
        try:
            meta = {key: value for key, value in conn.execute("SELECT key,value FROM archive_meta")}
        except sqlite3.Error:
            meta = {}
        raw = meta.get("raw_chunks")
        if raw is None:
            raw, raw_bytes = conn.execute("SELECT COUNT(*),COALESCE(SUM(length(data)),0) FROM raw_chunks").fetchone()
        else:
            raw_bytes = meta.get("raw_bytes", 0)
        sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        oldest = conn.execute("SELECT MIN(id) FROM outbox").fetchone()[0]
        backlog = 0
        epochs = []
        for (epoch,) in conn.execute("SELECT epoch FROM epochs ORDER BY rowid"):
            processed, rows = conn.execute("SELECT COALESCE(SUM(next_seq),0),COALESCE(SUM(rows),0) FROM decode_states WHERE epoch=?", (epoch,)).fetchone()
            totals = {}
            for (encoded,) in conn.execute("SELECT counts_json FROM decode_states WHERE epoch=?", (epoch,)):
                for key, value in json.loads(encoded).items():
                    totals[key] = totals.get(key, 0) + value
            epochs.append({"epoch": epoch, "processed_chunks": processed, "remaining_chunks": raw - processed, "decoded_rows": rows, "counts": totals})
            backlog += raw - processed
        errors = {kind: {"count": count, "last_ns": last} for kind, count, last in conn.execute("SELECT kind,count,last_ns FROM worker_errors")}
        freshness = {key: meta.get(key) for key in ("last_receive_ns", "last_decode_ns", "last_full_ack_ns")}
        counters = {key: meta.get(key, 0) for key in ("decoded_rows_total", "acked_rows_total")}
    facts = {"sessions": sessions, "raw_chunks": raw, "raw_bytes": raw_bytes, "pending_rows": pending,
             "epochs": epochs, "errors": errors, "oldest_pending_id": oldest, "backlog_chunks": backlog,
             "freshness": freshness, "counters": counters, "disk_reserve_bytes": None,
             "db_bytes": None, "wal_bytes": None, "disk_free_bytes": None}
    try:
        facts["db_bytes"] = path.stat().st_size
        wal = path.with_name(path.name + "-wal")
        facts["wal_bytes"] = wal.stat().st_size if wal.exists() else 0
        facts["disk_free_bytes"] = shutil.disk_usage(path.parent).free
    except OSError:
        pass
    return facts


def validate_row(row):
    if not isinstance(row, dict) or row.keys() != _COLUMN_SET:
        raise ValueError("invalid canonical row fields")
    for field in COLUMNS:
        value = row[field]
        if field in {"event_time", "ingest_time"}:
            if type(value) is not int or not 0 <= value <= INT64_MAX:
                raise ValueError("invalid nanosecond timestamp")
        elif field in {"value_bool", "source_is_resend"}:
            if value is not None and type(value) is not bool:
                raise ValueError("invalid boolean field")
        elif field == "value_num":
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                raise ValueError("invalid numeric field")
        elif value is not None and not isinstance(value, str):
            raise ValueError("invalid string field")
    if sum(row[key] is not None for key in ("value_num", "value_text", "value_bool")) > 1:
        raise ValueError("canonical value types are mutually exclusive")
    if any(not row[key] for key in ("vehicle", "path", "source", "event_id", "decode_epoch")) or row["source"] != "can":
        raise ValueError("invalid canonical row identity")

DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024
_SQL_PREFIX = 'INSERT INTO "vehicle_signal" (' + ",".join('"' + c + '"' for c in COLUMNS) + ") VALUES "
_COMMA_ENCODED_LEN = len(urllib.parse.quote_plus(",", safe="", encoding="utf-8", errors="strict").encode("ascii"))


def _literal(field, value):
    if value is None:
        return "NULL"
    if field in {"event_time", "ingest_time"}:
        return str(value)
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    if type(value) in (int, float):
        return repr(value)
    return "'" + value.replace("'", "''") + "'"


def _render_cell(row):
    validate_row(row)
    return "(" + ",".join(_literal(c, row[c]) for c in COLUMNS) + ")"


def render_insert(rows):
    """Fixed table/columns, plain integer nanoseconds (literal-insert fast path), standard SQL literals."""
    cells = [_render_cell(row) for row in rows]
    if not cells:
        raise ValueError("cannot insert empty rows")
    return _SQL_PREFIX + ",".join(cells)


def _urlencoded_len(text):
    """Exact ASCII byte length of the urlencode (quote_plus) encoding of text."""
    return len(urllib.parse.quote_plus(text, safe="", encoding="utf-8", errors="strict").encode("ascii"))


def encode_body(sql):
    """Exact URL-encoded SQL request body bytes sent to Greptime."""
    return urllib.parse.urlencode({"sql": sql}).encode()

_REDIRECT_CODES = {301, 302, 303, 307, 308}


class Greptime:
    def __init__(self, base_url, database, user, password, timeout=60, max_body_bytes=DEFAULT_MAX_BODY_BYTES):
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
            raise ValueError("invalid Greptime base URL")
        if parsed.scheme == "http":
            try:
                private = ipaddress.ip_address(parsed.hostname).is_private
            except ValueError:
                private = parsed.hostname == "localhost" or "." not in parsed.hostname
            if not private:
                raise ValueError("remote Greptime requires HTTPS")
        if not database or not user or not password:
            raise ValueError("Greptime database and credentials are required")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Greptime timeout must be finite and positive")
        if type(max_body_bytes) is bool or not isinstance(max_body_bytes, int) or max_body_bytes < 1:
            raise ValueError("Greptime body budget must be a positive integer")
        self.url = base_url.rstrip("/") + "/v1/sql?" + urllib.parse.urlencode({"db": database})
        self.authorization = "Basic " + base64.b64encode((user + ":" + password).encode()).decode()
        self.timeout = timeout
        self.max_body_bytes = max_body_bytes
        self._parts = urllib.parse.urlsplit(base_url.rstrip("/"))
        self._lock = threading.RLock()
        self._connection = None


    def _connect(self):
        target = (self._parts.hostname, self._parts.port or (443 if self._parts.scheme == "https" else 80))
        if self._parts.scheme == "https":
            connection = http.client.HTTPSConnection(target[0], target[1], timeout=self.timeout)
        else:
            connection = http.client.HTTPConnection(target[0], target[1], timeout=self.timeout)
        connection.connect()
        self._connection = connection

    def close(self):
        with self._lock:
            connection, self._connection = self._connection, None
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass

    def _post(self, payload):
        """Single POST over a reused connection; any ambiguity closes it."""
        query = self.url.split("/v1/sql?", 1)[1]
        with self._lock:
            try:
                if self._connection is None:
                    self._connect()
                self._connection.request("POST", "/v1/sql?" + query, body=payload, headers={
                    "Authorization": self.authorization, "Content-Type": "application/x-www-form-urlencoded",
                    "Content-Length": str(len(payload)), "Host": self._parts.hostname,
                })
                response = self._connection.getresponse()
                body = response.read(65537)
                keep = response.getheader("Connection", "").lower() != "close" and response.status == 200
                status = response.status
                if status in _REDIRECT_CODES and response.getheader("Location") is not None:
                    status = 599  # Never follow redirects; auth must not leak to a new URL.
            except (TimeoutError, socket.timeout):
                self.close()
                raise DownstreamError("greptime_timeout") from None
            except (http.client.HTTPException, OSError, ValueError):
                self.close()
                raise DownstreamError("greptime_failure") from None
            if status != 200 or len(body) > 65536:
                self.close()
                raise DownstreamError("greptime_failure")
            if not keep:
                self.close()
            return body

    def send_body(self, payload):
        """POST an exact body; returns affected rows without weakening ACK semantics."""
        if type(payload) is not bytes or not 0 < len(payload) <= 64 * 1024 * 1024:
            raise ValueError("Greptime payload must be bounded bytes")
        body = self._post(payload)
        try:
            payload_json = json.loads(body)
            if not isinstance(payload_json, dict) or type(payload_json.get("code", 0)) is not int or payload_json.get("code", 0) != 0:
                raise DownstreamError("greptime_failure")
            output = payload_json.get("output")
            if not isinstance(output, list) or not output:
                raise DownstreamError("greptime_failure")
            affected = 0
            for item in output:
                if not isinstance(item, dict) or item.get("error"):
                    raise DownstreamError("greptime_failure")
                keys = [key for key in ("affectedrows", "affected_rows") if key in item]
                if len(keys) != 1 or type(item[keys[0]]) is not int or item[keys[0]] < 0:
                    raise DownstreamError("greptime_failure")
                affected += item[keys[0]]
            return affected
        except (ValueError, TypeError, AttributeError):
            raise DownstreamError("greptime_failure") from None



class Worker(threading.Thread):
    def __init__(self, archive, decoder, greptime, interval=1, outbox_limit=20000, max_body_bytes=DEFAULT_MAX_BODY_BYTES):
        super().__init__(name="can-archive-worker", daemon=True)
        self.archive, self.decoder, self.greptime = archive, decoder, greptime
        self.interval, self.outbox_limit, self.max_body_bytes = interval, outbox_limit, max_body_bytes
        self.stop_event = threading.Event()

    def _error(self, kind):
        try:
            self.archive.record_error(kind)
        except (sqlite3.Error, OSError):
            pass  # Storage failure must not kill the receiver or disclose raw data.

    def _upload(self):
        # One consumer owns ACK deletion; decoding can keep filling the durable outbox.
        while not self.stop_event.is_set():
            progressed = False
            try:
                progressed = bool(self.archive.flush_once(self.greptime, self.outbox_limit, self.max_body_bytes))
            except DownstreamError as exc:
                self._error(str(exc))
            except Exception:
                self._error("greptime_failure")
            if not progressed:
                self.stop_event.wait(self.interval)

    def run(self):
        uploader = threading.Thread(target=self._upload, name="can-archive-uploader", daemon=True)
        uploader.start()
        try:
            while not self.stop_event.is_set():
                progressed = False
                try:
                    progressed = bool(self.archive.decode_once(self.decoder))
                except Rejection:
                    self._error("archive_disk_reserve")
                except Exception:
                    self._error("decode_failure")
                if not progressed:
                    self.stop_event.wait(self.interval)
        finally:
            self.stop_event.set()
            # Do not release exclusive archive ownership while an HTTP ACK is pending.
            uploader.join()


SERVER_CONCURRENCY = 4


class Receiver(HTTPServer):
    request_queue_size = 16
    daemon_threads = True

    def __init__(self, address, archive, vehicle, collector_id, user, password, timeout=15, concurrency=SERVER_CONCURRENCY):
        if not user or not password or ":" in user:
            raise ConfigurationError("can_otlp_credentials_required")
        if type(concurrency) is bool or not isinstance(concurrency, int) or concurrency < 1:
            raise ConfigurationError("invalid_receiver_concurrency")
        self.archive = archive
        self.vehicle, self.collector_id = _identity(vehicle), _identity(collector_id)
        with archive.connect() as conn:
            if conn.execute("SELECT 1 FROM sessions WHERE vehicle<>? OR collector_id<>? LIMIT 1", (self.vehicle, self.collector_id)).fetchone():
                raise ConfigurationError("archive_identity_conflicts_with_configuration")
        self.expected_auth = (user + ":" + password).encode()
        self.read_timeout = timeout
        self._slots = threading.BoundedSemaphore(concurrency)
        self._shutdown = threading.Event()
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        if self._shutdown.is_set() or not self._slots.acquire(blocking=False):
            try:
                request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                request.close()
            except OSError:
                pass
            return
        worker = threading.Thread(target=self._serve_slot, args=(request, client_address), daemon=True)
        worker.start()

    def _serve_slot(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            try:
                self.shutdown_request(request)
            except OSError:
                pass
            self._slots.release()


    def server_close(self):
        self._shutdown.set()
        super().server_close()

    def handle_error(self, request, client_address):
        pass  # Never let stdlib traceback logging include client-controlled data.


class Handler(BaseHTTPRequestHandler):
    server_version = "CANArchive"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def setup(self):
        self._deadline_lock = threading.Lock()
        self._deadline_token = None
        self.request.settimeout(self.server.read_timeout)
        super().setup()

    def handle_one_request(self):
        # Absolute per-request deadline: socket timeout alone can be postponed
        # indefinitely by trickled bytes, so a timer bounds the whole request.
        # Token check and socket shutdown run atomically under the
        # handler-local lock so a preempted timer N cannot close request N+1;
        # cancel stays outside the lock so it never waits on a callback.
        token = object()
        expired = False
        with self._deadline_lock:
            self._deadline_token = token

        def expire():
            nonlocal expired
            with self._deadline_lock:
                if self._deadline_token is not token:
                    return
                expired = True
                # Expired request must not keep the connection (and its slot)
                # alive for buffered pipelined requests after the shutdown.
                self.close_connection = True
                try:
                    self.request.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        deadline = threading.Timer(self.server.read_timeout, expire)
        deadline.daemon = True
        deadline.start()
        try:
            super().handle_one_request()
        finally:
            with self._deadline_lock:
                if self._deadline_token is token:
                    self._deadline_token = None
                    if expired:
                        # A success reply racing the shutdown reopens
                        # keep-alive; re-assert the expiry close after it.
                        self.close_connection = True
            deadline.cancel()

    def log_message(self, *args):
        pass

    def send_error(self, code, message=None, explain=None):
        self.reply(code, b'{"error":"invalid_http_request"}')

    def reply(self, code, body, protobuf=False):
        # Success keeps the connection alive for client reuse; any error,
        # timeout, or framing ambiguity closes it so no state leaks across requests.
        self.close_connection = code != 200
        self.send_response(code)
        self.send_header("Content-Type", "application/x-protobuf" if protobuf else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "keep-alive" if code == 200 else "close")
        if code == 401:
            self.send_header("WWW-Authenticate", 'Basic realm="CANArchive"')
        self.end_headers()
        self.wfile.write(body)

    def authenticate(self):
        values = self.headers.get_all("Authorization", [])
        supplied = b""
        if len(values) == 1 and values[0][:6].lower() == "basic ":
            try:
                supplied = base64.b64decode(values[0][6:], validate=True)
            except ValueError:
                pass
        if not hmac.compare_digest(supplied, self.server.expected_auth):
            raise Rejection(401, "unauthorized")

    def do_GET(self):
        self._handle(status=True)

    def do_POST(self):
        self._handle(status=False)

    def _handle(self, status):
        try:
            self.authenticate()
            if self.path != ("/status" if status else "/v1/logs"):
                raise Rejection(404, "not_found")
            if status:
                self.reply(200, _json(self.server.archive.status()).encode())
                return
            lengths = self.headers.get_all("Content-Length", [])
            if self.headers.get_all("Transfer-Encoding") or len(lengths) != 1 or not re.fullmatch(r"[0-9]+", lengths[0]):
                raise Rejection(400, "invalid_content_length")
            if len(lengths[0]) > 10:
                raise Rejection(413, "request_too_large")
            length = int(lengths[0])
            if not 0 < length <= MAX_REQUEST_BYTES:
                raise Rejection(413, "request_too_large")
            types = self.headers.get_all("Content-Type", [])
            encodings = self.headers.get_all("Content-Encoding", [])
            if len(types) != 1 or types[0].split(";", 1)[0].strip().lower() != "application/x-protobuf":
                raise Rejection(415, "unsupported_content_type")
            if len(encodings) != 1 or encodings[0].strip().lower() != "gzip":
                raise Rejection(415, "unsupported_content_encoding")
            compressed = self.rfile.read(length)
            if len(compressed) != length:
                self._expected_remainder = length - len(compressed)
                raise Rejection(400, "truncated_request")
            self._expected_remainder = 0
            try:
                stream = zlib.decompressobj(16 + zlib.MAX_WBITS)
                payload = stream.decompress(compressed, MAX_REQUEST_BYTES + 1)
                if len(payload) > MAX_REQUEST_BYTES or stream.unconsumed_tail:
                    raise Rejection(413, "request_too_large")
                if not stream.eof or stream.unused_data:
                    raise Rejection(400, "invalid_gzip")
            except zlib.error:
                raise Rejection(400, "invalid_gzip") from None
            try:
                meta, chunks = decode_batch(payload)
            except ValueError:
                raise Rejection(400, "invalid_otlp") from None
            if meta["vehicle"] != self.server.vehicle or meta["collector_id"] != self.server.collector_id:
                raise Rejection(403, "identity_mismatch")
            self.server.archive.accept(meta, chunks)
            self.reply(200, success_response(), protobuf=True)
        except Rejection as exc:
            self.reply(exc.status, _json({"error": exc.reason}).encode())
        except (socket.timeout, TimeoutError):
            self.reply(408, b'{"error":"request_timeout"}')
        except (sqlite3.Error, OSError):
            self.reply(503, b'{"error":"archive_unavailable"}')
        except Exception:
            self.reply(500, b'{"error":"receiver_failure"}')
        finally:
            if self.request_version != "HTTP/1.0" and not self.close_connection:
                remaining = getattr(self, "_expected_remainder", 0)
                if remaining:
                    self.close_connection = True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="receive durable raw OTLP and run decode/SQL worker")
    replay = commands.add_parser("re-decode", help="explicitly decode retained raw into an independent epoch/outbox")
    status = commands.add_parser("status", help="print only local archive counts and non-sensitive error codes")
    for command in (serve, replay, status):
        command.add_argument("--database", required=True)
    for command in (serve, replay):
        command.add_argument("--dbc", required=True)
        command.add_argument("--definitions", required=True)
        command.add_argument("--disk-reserve-mib", type=int, default=64)
    for name in ("vehicle", "collector-id", "greptime-url", "greptime-db"):
        serve.add_argument("--" + name, required=True)
    serve.add_argument("--bind", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=4319)
    serve.add_argument("--worker-interval", type=float, default=1)
    serve.add_argument("--http-timeout", type=float, default=15)
    serve.add_argument("--greptime-timeout", type=float, default=60)
    serve.add_argument("--outbox-limit", type=int, default=20000)
    serve.add_argument("--max-body-bytes", type=int, default=4 * 1024 * 1024)
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            print(_json(archive_status(args.database)))
            return 0
        os.umask(0o077)
        if args.disk_reserve_mib < 0:
            raise ConfigurationError("disk_reserve_must_be_nonnegative")
        if args.command == "serve":
            if not 0 <= args.port <= 65535 or not math.isfinite(args.worker_interval) or args.worker_interval <= 0 or not math.isfinite(args.http_timeout) or args.http_timeout <= 0 or not math.isfinite(args.greptime_timeout) or args.greptime_timeout <= 0 or not 1 <= args.outbox_limit <= 20000 or args.max_body_bytes < 1:
                raise ConfigurationError("invalid_receiver_limits")
            otlp_user, otlp_password = _credentials("CAN_OTLP")
            db_user, db_password = _credentials("GREPTIME")
            greptime = Greptime(args.greptime_url, args.greptime_db, db_user, db_password, args.greptime_timeout, args.max_body_bytes)
        decoder = Decoder(args.dbc, args.definitions)
        archive = Archive(args.database, args.disk_reserve_mib * 1024 * 1024)
        with exclusive_archive(archive.path):
            archive.register_epoch(decoder, explicit=args.command == "re-decode")
            if args.command == "re-decode":
                while archive.decode_once(decoder):
                    pass
                print(_json(archive.status()))
                return 0
            server = Receiver((args.bind, args.port), archive, args.vehicle, args.collector_id, otlp_user, otlp_password, args.http_timeout)
            worker = Worker(archive, decoder, greptime, args.worker_interval, args.outbox_limit, args.max_body_bytes)
            worker.start()
            print(_json({"receiver": "ready", "port": server.server_port}), flush=True)

            def _stop(signum, frame):
                # shutdown() must not run in the serve_forever thread itself: it blocks
                # waiting for that same thread. Signal from a helper thread instead.
                worker.stop_event.set()
                threading.Thread(target=server.shutdown, daemon=True).start()
            previous_term = signal.getsignal(signal.SIGTERM)
            previous_int = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGTERM, _stop)
            signal.signal(signal.SIGINT, _stop)
            try:
                server.serve_forever()
            finally:
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)
                server.server_close()
                worker.stop_event.set()
                worker.join()
                greptime.close()
        return 0
    except ConfigurationError as exc:
        print(_json({"error": str(exc)}), flush=True)
        return 1
    except Exception:
        # Configuration/parser/DB exceptions can contain paths or values: do not echo them.
        print('{"error":"configuration_or_archive_failure"}', flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
