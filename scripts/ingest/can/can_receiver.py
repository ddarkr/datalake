"""Authenticated OTLP raw archive and resumable CAN-to-Greptime worker.

A successful /v1/logs response acknowledges SQLite raw durability, not Greptime.
CLI serve/work defaults to a bounded raw cache; programmatic archives may retain
raw for explicit replay. Run with ``python -m scripts.ingest.can.can_receiver serve|work|status|re-decode``.
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

from scripts.ingest.can.can_otlp_wire import MAX_REQUEST_BYTES, decode_batch, success_response
from scripts.ingest.can.can_files import (
    MAX_CAPTURE_BYTES, FileConfigurationError, FileImporter, FileRejection, FileStore, MAX_FILE_RANGE,
)

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
CREATE TABLE IF NOT EXISTS raw_pruned (
 session INTEGER PRIMARY KEY REFERENCES sessions(id), next_seq INTEGER NOT NULL);
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
 head_id INTEGER,
 PRIMARY KEY(session,epoch));
CREATE TABLE IF NOT EXISTS decode_epoch_state (
 epoch TEXT PRIMARY KEY REFERENCES epochs(epoch),
 processed_chunks INTEGER NOT NULL DEFAULT 0,
 decoded_rows INTEGER NOT NULL DEFAULT 0, partial_rows INTEGER NOT NULL DEFAULT 0,
 pending_sessions INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS outbox (
 id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
 epoch TEXT NOT NULL REFERENCES epochs(epoch), row_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS worker_errors (
 kind TEXT PRIMARY KEY, count INTEGER NOT NULL, last_ns INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS archive_meta (
 key TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS trg_epoch_state_init AFTER INSERT ON epochs BEGIN
 INSERT OR IGNORE INTO decode_epoch_state(epoch) VALUES(NEW.epoch);
END;
CREATE TRIGGER IF NOT EXISTS trg_states_insert AFTER INSERT ON decode_states BEGIN
 UPDATE decode_epoch_state SET processed_chunks=processed_chunks+NEW.next_seq,decoded_rows=decoded_rows+NEW.rows WHERE epoch=NEW.epoch;
END;
CREATE TRIGGER IF NOT EXISTS trg_states_update AFTER UPDATE ON decode_states WHEN NEW.next_seq!=OLD.next_seq OR NEW.rows!=OLD.rows BEGIN
 UPDATE decode_epoch_state SET processed_chunks=processed_chunks+(NEW.next_seq-OLD.next_seq),decoded_rows=decoded_rows+(NEW.rows-OLD.rows) WHERE epoch=NEW.epoch;
END;
CREATE TRIGGER IF NOT EXISTS trg_states_delete AFTER DELETE ON decode_states BEGIN
 UPDATE decode_epoch_state SET processed_chunks=processed_chunks-OLD.next_seq,decoded_rows=decoded_rows-OLD.rows WHERE epoch=OLD.epoch;
END;
CREATE TRIGGER IF NOT EXISTS trg_partial_insert AFTER INSERT ON decode_partial BEGIN
 UPDATE decode_epoch_state SET partial_rows=partial_rows+NEW.rows_emitted WHERE epoch=NEW.epoch;
END;
CREATE TRIGGER IF NOT EXISTS trg_partial_update AFTER UPDATE ON decode_partial WHEN NEW.rows_emitted!=OLD.rows_emitted BEGIN
 UPDATE decode_epoch_state SET partial_rows=partial_rows+(NEW.rows_emitted-OLD.rows_emitted) WHERE epoch=NEW.epoch;
END;
CREATE TRIGGER IF NOT EXISTS trg_partial_delete AFTER DELETE ON decode_partial BEGIN
 UPDATE decode_epoch_state SET partial_rows=partial_rows-OLD.rows_emitted WHERE epoch=OLD.epoch;
END;
CREATE TRIGGER IF NOT EXISTS trg_pending_insert AFTER INSERT ON decode_pending BEGIN
 UPDATE decode_epoch_state SET pending_sessions=pending_sessions+1 WHERE epoch=NEW.epoch;
END;
CREATE TRIGGER IF NOT EXISTS trg_pending_delete AFTER DELETE ON decode_pending BEGIN
 UPDATE decode_epoch_state SET pending_sessions=pending_sessions-1 WHERE epoch=OLD.epoch;
END;
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



def _prepare_archive_dir(path):
    """Create and validate the archive parent before any lock/DB file exists."""
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    parent = path.parent.stat()
    if path.parent.is_symlink() or parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise ConfigurationError("archive_directory_requires_private_0700_and_ownership")


@contextlib.contextmanager
def _locked(path, exclusive=True):
    """Hold a nonblocking flock for the whole context; SH allows serve+work coexistence."""
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ConfigurationError("archive_already_in_use") from None
        yield
    finally:
        os.close(fd)


INIT_LOCK_TIMEOUT = 120.0

@contextlib.contextmanager
def _init_guarded(path):
    """Serialize Archive schema/migration init across simultaneous role startups."""
    fd = os.open(str(path) + ".init.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
        deadline = time.monotonic() + INIT_LOCK_TIMEOUT
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ConfigurationError("archive_already_in_use") from None
                time.sleep(0.05)
        try:
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        os.close(fd)


@contextlib.contextmanager
def receiver_locks(path):
    """Raw-only role lifetime: SH legacy .lock plus EX receiver role lock."""
    with _locked(str(path) + ".lock", exclusive=False):
        with _locked(str(path) + ".receiver.lock", exclusive=True):
            yield


@contextlib.contextmanager
def worker_locks(path):
    """Decode/upload role lifetime (and re-decode): SH legacy .lock plus EX worker role lock."""
    with _locked(str(path) + ".lock", exclusive=False):
        with _locked(str(path) + ".worker.lock", exclusive=True):
            yield


DECODE_ROW_BUDGET = 2000
DECODE_PREFETCH_BYTES = 4 * 1024 * 1024
DECODE_PAGE_ROWS = 64

# Issue #11 decoded-outbox backpressure: row + canonical-UTF-8-byte high/low
# watermarks with hysteresis. High pauses decode only; low resumes. Raw accept
# keeps its existing reserve/auth/atomic policy. Conservative finite defaults:
# 20000/15000 rows and 32/24 MiB canonical bytes.
DEFAULT_OUTBOX_HIGH_ROWS = 20000
DEFAULT_OUTBOX_LOW_ROWS = 15000
DEFAULT_OUTBOX_HIGH_BYTES = 32 * 1024 * 1024
DEFAULT_OUTBOX_LOW_BYTES = 24 * 1024 * 1024


def _meta_bump(conn, key, delta):
    conn.execute("INSERT INTO archive_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=value+?",
                 (key, delta, delta))


def _meta_set(conn, key, value):
    conn.execute("INSERT INTO archive_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (key, value))


def _ensure_pending_frontier(conn):
    """Add the durable frontier column/index to legacy tables after SCHEMA.

    Fresh tables already carry head_id; the frontier index is created only
    here (not in SCHEMA) because a legacy table without the column would
    fail index creation before the ALTER below can run.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(decode_pending)")}
    if "head_id" not in cols:
        conn.execute("ALTER TABLE decode_pending ADD COLUMN head_id INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS decode_pending_epoch ON decode_pending(epoch)")
    conn.execute("CREATE INDEX IF NOT EXISTS decode_pending_frontier ON decode_pending(epoch,head_id)")


def _migrate_session_count(conn):
    """Dedicated sessions_total marker; legacy raw_chunks marker may already exist."""
    if conn.execute("SELECT value FROM archive_meta WHERE key='sessions_total'").fetchone() is not None:
        return False
    total = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    conn.execute("INSERT OR IGNORE INTO archive_meta(key,value) VALUES('sessions_total',?)", (total,))
    return True


def _migrate_active_epoch(conn):
    """Explicit selected epoch; never the latest-rowid heuristic on read."""
    if conn.execute("SELECT value FROM archive_meta WHERE key='active_epoch'").fetchone() is not None:
        return False
    row = conn.execute("SELECT epoch FROM epochs ORDER BY rowid DESC LIMIT 1").fetchone()
    if row is not None:
        _meta_set(conn, "active_epoch", row[0])
    return True

def _migrate_counters(conn):
    """Bounded one-time migration: populate incremental counters from a legacy DB, then serve them without scans."""
    if conn.execute("SELECT value FROM archive_meta WHERE key='raw_chunks'").fetchone() is not None:
        return False
    raw, raw_bytes = conn.execute("SELECT COUNT(*),COALESCE(SUM(length(data)),0) FROM raw_chunks").fetchone()
    outbox = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    decoded = conn.execute("SELECT COALESCE(SUM(rows),0) FROM decode_states").fetchone()[0]
    partial = conn.execute("SELECT COALESCE(SUM(rows_emitted),0) FROM decode_partial").fetchone()[0]
    for key, value in (("raw_chunks", raw), ("raw_bytes", raw_bytes), ("outbox_rows", outbox),
                       ("decoded_rows_total", decoded + partial), ("acked_rows_total", 0)):
        conn.execute("INSERT OR IGNORE INTO archive_meta(key,value) VALUES(?,?)", (key, value))
    return True

def _migrate_decoded_total(conn):
    """Versioned one-time repair: decoded_rows_total missed partial rows on old builds.

    Old _migrate_counters seeded decoded_rows_total from decode_states only
    while decode_partial held emitted rows, then set the raw_chunks marker so
    the fixed builder never reran. This reconciles states.rows +
    partial.rows_emitted exactly once via a dedicated decoded_total_v2
    marker, even when the legacy raw marker exists. ACK history untouched.
    One bounded scan at open; never on the hot path.
    """
    if conn.execute("SELECT value FROM archive_meta WHERE key='decoded_total_v2'").fetchone() is not None:
        return False
    if conn.execute("SELECT value FROM archive_meta WHERE key='decoded_rows_total'").fetchone() is None:
        return False  # fresh archive: _migrate_counters already seeds states+partial
    decoded = conn.execute("SELECT COALESCE(SUM(rows),0) FROM decode_states").fetchone()[0]
    partial = conn.execute("SELECT COALESCE(SUM(rows_emitted),0) FROM decode_partial").fetchone()[0]
    _meta_set(conn, "decoded_rows_total", decoded + partial)
    _meta_set(conn, "decoded_total_v2", 1)
    return True

def _migrate_epoch_state(conn):
    """Idempotent one-time build of decode_epoch_state; caller's transaction owns atomicity.

    Decoded count = completed rows plus durable partial emitted rows, never
    double counted: a finished chunk deletes its partial row, so summing both
    tables counts in-flight prefix rows exactly once. Dedicated
    epoch_state_migrated marker (legacy raw_chunks marker may already exist).
    """
    if conn.execute("SELECT value FROM archive_meta WHERE key='epoch_state_migrated'").fetchone() is not None:
        return False
    conn.execute("CREATE TABLE IF NOT EXISTS decode_epoch_state ("
                 "epoch TEXT PRIMARY KEY REFERENCES epochs(epoch), "
                 "processed_chunks INTEGER NOT NULL DEFAULT 0, "
                 "decoded_rows INTEGER NOT NULL DEFAULT 0, partial_rows INTEGER NOT NULL DEFAULT 0, "
                 "pending_sessions INTEGER NOT NULL DEFAULT 0)")
    epoch_cols = {row[1] for row in conn.execute("PRAGMA table_info(decode_epoch_state)")}
    if "remaining_chunks" in epoch_cols:
        raise sqlite3.Error("stale decode_epoch_state with remaining_chunks")
    if epoch_cols != {"epoch", "processed_chunks", "decoded_rows", "partial_rows", "pending_sessions"}:
        raise sqlite3.Error("malformed decode_epoch_state")
    for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
        processed, decoded, partial = conn.execute(
            "SELECT COALESCE((SELECT SUM(next_seq) FROM decode_states WHERE epoch=?),0),"
            "COALESCE((SELECT SUM(rows) FROM decode_states WHERE epoch=?),0),"
            "COALESCE((SELECT SUM(rows_emitted) FROM decode_partial WHERE epoch=?),0)",
            (epoch, epoch, epoch)).fetchone()
        pending = conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?", (epoch,)).fetchone()[0]
        conn.execute("INSERT OR REPLACE INTO decode_epoch_state(epoch,processed_chunks,decoded_rows,partial_rows,pending_sessions)"
                     " VALUES(?,?,?,?,?)", (epoch, processed, decoded, partial, pending))
    _meta_set(conn, "epoch_state_migrated", 1)
    return True

def _refresh_pending_epoch(conn, epoch):
    """Reconcile one epoch's pending membership from cursors inside the caller's transaction.

    Pending holds exactly sessions with raw beyond the epoch cursor (or no
    cursor yet): completions delete during decode commit, new arrivals insert
    during accept. Crash/rollback safety comes free: both run in the same
    transaction as the cursor/raw write, so a lost commit leaves no phantom.
    Each member also carries its durable next-raw head_id (raw_chunks id at
    the cursor), so bounded frontier reads order by the indexed head_id
    without re-sorting the full pending set.
    """
    conn.execute(
        "INSERT OR IGNORE INTO decode_pending(session,epoch) "
        "SELECT s.id,? FROM sessions s LEFT JOIN decode_states d ON d.session=s.id AND d.epoch=? "
        "WHERE EXISTS(SELECT 1 FROM raw_chunks c WHERE c.session=s.id AND c.seq>=COALESCE(d.next_seq,0))",
        (epoch, epoch),
    )
    conn.execute(
        "UPDATE decode_pending SET head_id=("
        "SELECT c.id FROM raw_chunks c "
        "LEFT JOIN decode_states d ON d.session=decode_pending.session AND d.epoch=decode_pending.epoch "
        "WHERE c.session=decode_pending.session AND c.seq>=COALESCE(d.next_seq,0) "
        "ORDER BY c.seq LIMIT 1) WHERE epoch=?",
        (epoch,),
    )
    conn.execute("DELETE FROM decode_pending WHERE epoch=? AND head_id IS NULL", (epoch,))


def candidate_sql(limit=None):
    """Bounded pending-head frontier in global raw arrival order.

    Single maintained query: at most `limit` members via the indexed
    (epoch, head_id) key (bind epoch, then limit; omit limit for the full
    ordered set used only by repair/diagnostics). The inner query sorts raw
    ids, not sessions, so SQL VM work scales with the frontier taken, not
    the pending population. seq resolves from the raw id inside the bounded
    set, never from a live cursor join. decode_once seeds its #8 merge with
    the prefetch_limit smallest heads; refill uses the indexed (session,
    seq) key with unpaged heads kept in the heap, so a P-chunk prefix never
    skips an unknown earlier head (every refill id exceeds its consumed
    head). A NULL head_id is corruption (migration/rebuild backfills
    atomically): callers must run rebuild_pending, never scan around it.
    """
    if limit is None:
        return (
            "SELECT q.head_id AS id, q.session AS session, c.seq AS seq FROM "
            "(SELECT session,head_id FROM decode_pending WHERE epoch=? AND head_id IS NOT NULL "
            "ORDER BY head_id) q JOIN raw_chunks c ON c.id=q.head_id"
        )
    if limit <= 0:
        raise ValueError("candidate limit must be positive")
    return (
        "SELECT q.head_id AS id, q.session AS session, c.seq AS seq FROM "
        "(SELECT session,head_id FROM decode_pending WHERE epoch=? AND head_id IS NOT NULL "
        "ORDER BY head_id LIMIT %d) q JOIN raw_chunks c ON c.id=q.head_id" % int(limit)
    )


def _reconcile_pending_session(conn, epoch, session_id):
    """Refresh one session's pending membership inside the caller's transaction.

    Cursor-first: one PK cursor read, then one indexed (session, seq>=bound)
    probe that also resolves the durable frontier head_id. Never joins or
    scans retained history, so commit cost stays bounded no matter how much
    completed prefix is retained.
    """
    row = conn.execute(
        "SELECT next_seq FROM decode_states WHERE session=? AND epoch=?",
        (session_id, epoch),
    ).fetchone()
    bound = row["next_seq"] if row else 0
    head = conn.execute(
        "SELECT id FROM raw_chunks WHERE session=? AND seq>=? ORDER BY seq LIMIT 1",
        (session_id, bound),
    ).fetchone()
    if head is None:
        conn.execute("DELETE FROM decode_pending WHERE epoch=? AND session=?", (epoch, session_id))
    else:
        conn.execute(
            "INSERT INTO decode_pending(session,epoch,head_id) VALUES(?,?,?) "
            "ON CONFLICT(session,epoch) DO UPDATE SET head_id=excluded.head_id",
            (session_id, epoch, head["id"]),
        )


def _migrate_pending(conn):
    """One-time build of decode_pending for pre-pending archives; later opens are a marker probe."""
    _ensure_pending_frontier(conn)
    if conn.execute("SELECT value FROM archive_meta WHERE key='pending_migrated'").fetchone() is not None:
        # Backfill durable head_id for archives migrated before the column
        # existed; NULL heads would otherwise hide pending sessions. Cheap
        # marker-gated second pass: skips once no NULL heads remain.
        if conn.execute("SELECT value FROM archive_meta WHERE key='pending_heads_v1'").fetchone() is not None:
            return False
        if conn.execute("SELECT 1 FROM decode_pending WHERE head_id IS NULL LIMIT 1").fetchone() is None:
            _meta_set(conn, "pending_heads_v1", 1)
            return False
        for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
            _refresh_pending_epoch(conn, epoch)
        _meta_set(conn, "pending_heads_v1", 1)
        return True
    for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
        _refresh_pending_epoch(conn, epoch)
    _meta_set(conn, "pending_migrated", 1)
    _meta_set(conn, "pending_heads_v1", 1)
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
    def __init__(self, path, disk_reserve_bytes=64 * 1024 * 1024, max_database_bytes=None):
        if max_database_bytes is not None and (type(max_database_bytes) is not int or max_database_bytes < 4 * 1024 * 1024):
            raise ConfigurationError("raw_cache_must_be_at_least_4_mib")
        self.path = _archive_path(path)
        self.disk_reserve_bytes = disk_reserve_bytes
        self.max_database_bytes = max_database_bytes
        # Secure parent init BEFORE lock creation; narrow init lock only here.
        _prepare_archive_dir(self.path)
        with _init_guarded(self.path):
            fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                if os.fstat(fd).st_uid != os.getuid():
                    raise ConfigurationError("archive_requires_ownership")
                os.fchmod(fd, 0o600)
            finally:
                os.close(fd)
            if self.max_database_bytes is not None:
                # The init guard serializes the one-time format conversion.
                with contextlib.closing(sqlite3.connect(self.path, timeout=5)) as conn:
                    conn.execute("PRAGMA journal_mode=DELETE")
                    if conn.execute("PRAGMA auto_vacuum").fetchone()[0] != 1:
                        conn.execute("PRAGMA auto_vacuum=FULL")
                        conn.execute("VACUUM")
            with self.connect() as conn:
                conn.executescript(SCHEMA)
                conn.execute("BEGIN IMMEDIATE")
                _migrate_counters(conn)
                _migrate_outbox_bytes(conn)
                _migrate_decoded_total(conn)
                _migrate_pending(conn)
                _migrate_session_count(conn)
                _migrate_epoch_state(conn)
                _migrate_active_epoch(conn)
                _meta_set(conn, "max_database_bytes", self.max_database_bytes or 0)
                conn.commit()
        # Wake hints live on the Archive so accept/decode/flush commits and the
        # Worker share one pair created once here (no racy lazy attach).
        self.decode_ready = threading.Event()
        self.upload_ready = threading.Event()
        self._stages = StageStats(enabled=False)

    def set_stage_instrumentation(self, enabled):
        """Optional bounded in-process stage timing; off by default, no labels."""
        self._stages.enabled = bool(enabled)

    def observe_stage(self, stage, seconds):
        self._stages.observe(stage, seconds)

    def read_stage_samples(self):
        """Fixed-stage raw seconds for the harness; empty/absent stage stays unknown."""
        return {stage: list(bucket) for stage, bucket in self._stages._samples.items() if bucket}

    def read_stage_summary(self):
        """Fixed-stage {count,p50,p95,p99,dropped}; no session/vehicle/raw labels."""
        return self._stages.summary()

    def reset_stage_samples(self):
        self._stages.reset()



    def connect(self):
        conn = sqlite3.connect(self.path, timeout=5)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=" + ("DELETE" if self.max_database_bytes is not None else "WAL"))
            if self.max_database_bytes is not None:
                page_size = conn.execute("PRAGMA page_size").fetchone()[0]
                limit = self.max_database_bytes // page_size
                if conn.execute("PRAGMA max_page_count=%d" % limit).fetchone()[0] > limit:
                    raise ConfigurationError("existing_database_exceeds_raw_cache_limit")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
        except (sqlite3.Error, ConfigurationError):
            conn.close()
            raise
        return contextlib.closing(conn)

    def _reserve_raw_cache(self, conn, chunks):
        if self.max_database_bytes is None or not chunks:
            return
        used = (conn.execute("PRAGMA page_count").fetchone()[0]
                - conn.execute("PRAGMA freelist_count").fetchone()[0]) * conn.execute("PRAGMA page_size").fetchone()[0]
        required = sum(len(c["data"]) + 128 for c in chunks) + 16384
        # ponytail: reserve 25% for decode/outbox writes; tune only if real batches outgrow it.
        if used + required > self.max_database_bytes * 3 // 4:
            raise Rejection(503, "raw_cache_full")

    def prune_once(self, limit=10000):
        """Drop a bounded decoded prefix only after the outbox drains; keep retry tail and parser state."""
        if self.max_database_bytes is None:
            return 0
        removed = 0
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("SELECT 1 FROM outbox LIMIT 1").fetchone() is not None:
                return 0
            epochs = conn.execute("SELECT COUNT(*) FROM epochs").fetchone()[0]
            if not epochs:
                return 0
            candidates = conn.execute(
                "SELECT s.id,MIN(MIN(d.next_seq),s.next_seq-10000) AS stop "
                "FROM sessions s JOIN decode_states d ON d.session=s.id "
                "GROUP BY s.id HAVING COUNT(*)=? AND stop>0", (epochs,)).fetchall()
            for session, stop in candidates:
                rows = conn.execute(
                    "SELECT id,seq,length(data) FROM raw_chunks WHERE session=? AND seq<? "
                    "ORDER BY seq LIMIT ?", (session, stop, limit - removed)).fetchall()
                if not rows:
                    continue
                conn.executemany("DELETE FROM raw_chunks WHERE id=?", [(r[0],) for r in rows])
                conn.execute(
                    "INSERT INTO raw_pruned VALUES(?,?) ON CONFLICT(session) DO UPDATE SET "
                    "next_seq=MAX(next_seq,excluded.next_seq)", (session, rows[-1][1] + 1))
                _meta_bump(conn, "pruned_chunks", len(rows))
                _meta_bump(conn, "raw_bytes", -sum(r[2] for r in rows))
                removed += len(rows)
                if removed == limit:
                    break
            conn.commit()
        return removed


    def reserve(self, additional=0):
        if shutil.disk_usage(self.path.parent).free < self.disk_reserve_bytes + additional:
            raise Rejection(503, "archive_disk_reserve")

    def accept(self, meta, chunks):
        """Commit all new chunks, or none; matching overlap is legal on ACK loss."""
        timing = self._stages.enabled
        start = time.monotonic() if timing else 0.0
        try:
            return self._accept_inner(meta, chunks)
        finally:
            if timing:
                self.observe_stage("accept", time.monotonic() - start)

    def _accept_inner(self, meta, chunks):
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
                self._reserve_raw_cache(conn, chunks)
                self.reserve(sum(len(c["data"]) for c in chunks))
                cursor = conn.execute(
                    "INSERT INTO sessions(vehicle,collector_id,session_id,meta_json) VALUES(?,?,?,?)",
                    (meta["vehicle"], meta["collector_id"], meta["session_id"], encoded),
                )
                session_id, next_seq, last_offset = cursor.lastrowid, 0, -1
                _meta_bump(conn, "sessions_total", 1)
            else:
                if session["meta_json"] != encoded:
                    raise Rejection(409, "metadata_conflict")
                session_id, next_seq, last_offset = session["id"], session["next_seq"], session["last_offset_ns"]
                if any(c["seq"] >= next_seq for c in chunks):
                    self.reserve(sum(len(c["data"]) for c in chunks if c["seq"] >= next_seq))
                    self._reserve_raw_cache(conn, [c for c in chunks if c["seq"] >= next_seq])
            added = 0
            pruned = conn.execute("SELECT next_seq FROM raw_pruned WHERE session=?", (session_id,)).fetchone()
            for chunk in chunks:
                seq, offset, phase, data = (chunk[k] for k in ("seq", "offset_ns", "phase", "data"))
                if seq < next_seq:
                    # Completed history is no longer available for byte-conflict comparison.
                    # Its authenticated retry is ACKed, never reinserted or decoded again.
                    if pruned is not None and seq < pruned[0]:
                        continue
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
            lock_start = time.monotonic() if self._stages.enabled else 0.0
            conn.commit()
            if self._stages.enabled:
                self.observe_stage("sqlite_lock_wait", time.monotonic() - lock_start)
            if added:
                # New raw may exist for decode: hint only, durable pending decides.
                self.decode_ready.set()
            return added

    def register_epoch(self, decoder, explicit=False):
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT mapping_revision FROM epochs WHERE epoch=?", (decoder.epoch,)).fetchone()
            if existing:
                if existing[0] != decoder.mapping_revision:
                    raise ConfigurationError("decoder_epoch_mapping_conflict")
            else:
                if conn.execute("SELECT 1 FROM raw_pruned LIMIT 1").fetchone() is not None:
                    raise ConfigurationError("re-decode_requires_unpruned_raw")
                if not explicit and conn.execute("SELECT 1 FROM epochs LIMIT 1").fetchone():
                    raise ConfigurationError("new_decoder_epoch_requires_re-decode")
                conn.execute("INSERT INTO epochs VALUES(?,?)", (decoder.epoch, decoder.mapping_revision))
                _refresh_pending_epoch(conn, decoder.epoch)
            _meta_set(conn, "active_epoch", decoder.epoch)
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
        # CPU deadline starts after the read snapshot closes (set below): a
        # delayed read must not eat decode budget and force 1-chunk commits.
        if limit <= 0:
            return 0
        window = getattr(self, "_decode_prefetch_limit", 1000)
        prefetch_limit = min(limit, window)
        # Merge a bounded prefix while reading, then release the snapshot before
        # decoding. Unpaged heads remain in the heap: exhausting a page or the
        # byte budget must never let a later session bypass the earliest chunk.
        # Cold pages are demand-sized: fair share of the remaining fetch
        # budget across frontier sessions lacking a buffered run (at most
        # DECODE_PAGE_ROWS), so wide round-robin fan-out fetches ~budget/cold
        # rows per session instead of over-fetching early heads and hitting
        # the fetched cap before every head enters the prefix; a lone
        # contiguous session still gets full 64-row pages. Buffered (hot)
        # runs are reused without new queries.
        stats = {"fetched_chunks": 0, "fetched_bytes": 0, "prefix_chunks": 0,
                 "staged_chunks": 0, "read_queries": 0}
        self._decode_prefetch_stats = stats
        has_more = False
        timing = self._stages.enabled
        read_start = time.monotonic() if timing else 0.0
        with self.connect() as conn:
            conn.execute("BEGIN")
            stats["read_queries"] += 1
            # Bounded frontier: P smallest durable heads via (epoch,head_id);
            # refill/fetch logic below unchanged (#8 contract preserved).
            heap = [(head["id"], head["session"], head["seq"]) for head in
                    conn.execute(candidate_sql(prefetch_limit), (decoder.epoch,))]
            heapq.heapify(heap)
            pages, anchors, partials, metadata = {}, {}, {}, {}
            prefix = []
            prefetched_bytes = fetched_rows = 0
            # Incremental count of heap heads without a buffered run: every
            # seed head starts cold (no pages yet); pop/push transitions keep
            # it exact in O(1), instead of rescanning the heap per fetch.
            cold = len(heap)
            while heap and len(prefix) < prefetch_limit:
                _, session, want = heapq.heappop(heap)
                run = pages.get(session)
                if not run:
                    cold -= 1
                    if prefetched_bytes >= DECODE_PREFETCH_BYTES:
                        has_more = True
                        cold += 1
                        break
                    if fetched_rows >= prefetch_limit:
                        cold += 1
                        break
                    run = deque()
                    fetch_rows = min(DECODE_PAGE_ROWS, max(1, (prefetch_limit - fetched_rows) // (cold + 1)))
                    stats["read_queries"] += 1
                    with contextlib.closing(conn.execute(
                        "SELECT c.id,c.session,c.seq,c.offset_ns,c.phase,c.data,s.meta_json "
                        "FROM raw_chunks c JOIN sessions s ON s.id=c.session "
                        "WHERE c.session=? AND c.seq>=? ORDER BY c.seq LIMIT ?",
                        (session, want, min(fetch_rows, prefetch_limit - fetched_rows)),
                    )) as cursor:
                        for row in cursor:
                            if row["seq"] != want + len(run):
                                break
                            if prefetched_bytes + len(row["data"]) > DECODE_PREFETCH_BYTES:
                                has_more = True
                                break
                            run.append(row)
                            prefetched_bytes += len(row["data"])
                            fetched_rows += 1
                    if not run:
                        has_more = True
                        cold += 1
                        break
                    pages[session] = run
                    if session not in anchors:
                        metadata[session] = json.loads(run[0]["meta_json"])
                        stats["read_queries"] += 2
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
                    stats["read_queries"] += 1
                    next_head = conn.execute(
                        "SELECT id,seq FROM raw_chunks WHERE session=? AND seq=?",
                        (session, raw["seq"] + 1),
                    ).fetchone()
                if next_head is not None:
                    heapq.heappush(heap, (next_head["id"], session, next_head["seq"]))
                    if not pages.get(session):
                        cold += 1
            # Read-side cap only: a full window consumed with heap/buffered rows
            # left is normal steady state (more pending work), not waste, so it
            # must still allow window growth below.
            stats["prefix_chunks"] = len(prefix)
            stats["fetched_chunks"] = fetched_rows
            stats["fetched_bytes"] = prefetched_bytes
            stats["staged_chunks"] = 0
        if timing:
            self.observe_stage("decode_read", time.monotonic() - read_start)
        # Snapshot closed: start the CPU-only deadline here so delayed reads
        # cannot shrink the decode budget.
        deadline = time.monotonic() + 0.05
        # Only the prefix is needed now; discard speculative page rows before
        # allocating decoded rows. Raw bytes are capped at 1MiB, fetched chunks
        pages.clear()
        previous = {}
        staged = []
        emitted_rows = 0
        if not prefix:
            stats["staged_chunks"] = 0
            if limit >= window and (has_more or fetched_rows):
                self._decode_prefetch_limit = max(1, len(staged))
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
            cpu_start = time.monotonic() if timing else 0.0
            rows, state, counts, done = decode_some(meta, chunk, state, max(1, DECODE_ROW_BUDGET - emitted_rows))
            if timing:
                self.observe_stage("decode_cpu", time.monotonic() - cpu_start)
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
            stats["staged_chunks"] = 0
            if limit >= window and (has_more or fetched_rows):
                self._decode_prefetch_limit = max(1, len(staged))
            return 0
        json_start = time.monotonic() if timing else 0.0
        for _, _, work in staged:
            work["canonicals"] = [_json(row) for row in work["rows"]]
        if timing:
            self.observe_stage("json_prepare", time.monotonic() - json_start)
        staged_bytes = sum(len(c.encode()) for _, _, work in staged for c in work["canonicals"])
        self.reserve(staged_bytes + len(staged) * 512 + staged_bytes // 4)
        commit_start = time.monotonic() if timing else 0.0
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
            _outbox_bump(conn, total_rows, staged_bytes)
            _meta_bump(conn, "decoded_rows_total", total_rows)
            for session in order:
                _reconcile_pending_session(conn, decoder.epoch, session)
            _meta_set(conn, "last_decode_ns", time.time_ns())
            lock_start = time.monotonic() if timing else 0.0
            conn.commit()
            if timing:
                lock_wait = time.monotonic() - lock_start
                self.observe_stage("sqlite_lock_wait", lock_wait)
                self.observe_stage("decode_commit", time.monotonic() - commit_start - lock_wait)
        # Match the next read to useful work, including pre-prefix speculative
        # waste: fetched rows beyond staged chunks (pages that never entered
        # the prefix, or a fetched-cap stop mid-prefix) shrink the window even
        # when the whole prefix stages. A full window consumed with nothing
        # left unread is steady state, not waste, and still allows growth.
        # This is an in-memory hint only: rollback/restart cannot move cursors.
        stats["staged_chunks"] = len(staged)
        if limit >= window:
            if len(staged) < len(prefix) or has_more or fetched_rows > len(staged):
                self._decode_prefetch_limit = max(1, len(staged))
            elif (staged[-1][2]["done"] and emitted_rows < DECODE_ROW_BUDGET):
                self._decode_prefetch_limit = min(limit, window * 2)
        return len(staged)

    def rebuild_pending(self):
        """Repair path for archives touched by outside writers: rescan all epochs in one transaction."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_pending_frontier(conn)
            for (epoch,) in conn.execute("SELECT epoch FROM epochs"):
                _refresh_pending_epoch(conn, epoch)
            _meta_set(conn, "pending_migrated", 1)
            _meta_set(conn, "pending_heads_v1", 1)
            conn.commit()

    def record_error(self, kind):
        if kind not in ERROR_KINDS:
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
        timing = self._stages.enabled
        total = len(_FORM_PREFIX_BYTES)
        chosen, count, encoded = [], 0, []
        prep_start = time.monotonic() if timing else 0.0
        with self.connect() as conn:
            for record in conn.execute("SELECT id,row_json FROM outbox ORDER BY id LIMIT ?", (limit,)):
                canonical = record["row_json"]
                row = json.loads(canonical)
                cell_enc = _encode_text(_render_cell(row))
                addition = len(cell_enc) + (0 if count == 0 else _COMMA_ENCODED_LEN)
                if total + addition > budget:
                    break
                total += addition
                chosen.append((record["id"], canonical, row))
                encoded.append(cell_enc)
                count += 1
                if count >= limit:
                    break
        if timing:
            self.observe_stage("sql_prepare", time.monotonic() - prep_start)
        if not chosen:
            with self.connect() as conn:
                if conn.execute("SELECT 1 FROM outbox LIMIT 1").fetchone() is None:
                    return 0
            # The oversized head row stays durable; nothing is skipped or deleted.
            raise DownstreamError("greptime_row_too_large")
        rows = [(canonical, row) for _, canonical, row in chosen]
        payload = _FORM_PREFIX_BYTES + _COMMA_ENCODED.join(encoded)
        signal_start = time.monotonic() if timing else 0.0
        if greptime.send_body(payload) != count:
            raise DownstreamError("greptime_partial_ack")
        if timing:
            self.observe_stage("signal_ack", time.monotonic() - signal_start)
        received_at = _ns_timestamp(time.time_ns())
        statements = _dirty_inserts(rows, received_at)
        dirty = encode_body('INSERT INTO "vehicle_signal_dirty" '
                            '("window_start","vehicle","source","decode_epoch","generation","received_at") VALUES '
                            + ",".join(statements))
        dirty_start = time.monotonic() if timing else 0.0
        try:
            affected = greptime.send_body(dirty)
        except DownstreamError:
            raise DownstreamError("dirty_notify_failure") from None
        if affected != len(statements):
            raise DownstreamError("dirty_notify_failure")
        if timing:
            self.observe_stage("dirty_ack", time.monotonic() - dirty_start)
        now = time.time_ns()
        freed_bytes = sum(len(canonical.encode()) for _, canonical, _ in chosen)
        flush_start = time.monotonic() if timing else 0.0
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany("DELETE FROM outbox WHERE id=?", [(ident,) for ident, _, _ in chosen])
            _outbox_bump(conn, -len(chosen), -freed_bytes)
            _meta_bump(conn, "acked_rows_total", len(chosen))
            _meta_set(conn, "last_full_ack_ns", now)
            lock_start = time.monotonic() if timing else 0.0
            conn.commit()
            if timing:
                lock_wait = time.monotonic() - lock_start
                self.observe_stage("sqlite_lock_wait", lock_wait)
                self.observe_stage("flush_commit", time.monotonic() - flush_start - lock_wait)
        # Full-ACK deletion freed room: hint decode, durable counters decide.
        self.decode_ready.set()
        return count

    def status(self, detail=False):
        facts = archive_status(self.path, detail=detail)
        facts["disk_reserve_bytes"] = self.disk_reserve_bytes
        facts["max_database_bytes"] = self.max_database_bytes
        return facts


def _stage_quantiles(samples):
    """Nearest-rank p50/p95/p99 over a bounded sample list; empty stays unknown."""
    ordered = sorted(samples)
    count = len(ordered)
    if not count:
        return {"count": 0}
    def _quantile(rank):
        position = min(count - 1, math.ceil(rank * count) - 1)
        return ordered[position]
    return {"count": count, "p50": _quantile(0.50), "p95": _quantile(0.95), "p99": _quantile(0.99)}


_STAGE_NAMES = ("accept", "decode_read", "decode_cpu", "decode_commit", "sql_prepare",
                "signal_ack", "dirty_ack", "queue_wait", "flush_commit",
                "sqlite_lock_wait", "json_prepare")


class StageStats:
    """Bounded optional in-process stage recorder; off by default, no labels.

    Fixed stage names only (integration contract for the sustained-load
    harness): accept, decode_read, decode_cpu, decode_commit, sql_prepare,
    signal_ack, dirty_ack, queue_wait, plus flush_commit, sqlite_lock_wait
    and json_prepare. SQLite lock wait stays separate from commit; canonical
    JSON preparation stays separate from coarse decode. Samples are seconds;
    per-stage cap with a drop counter. Getattr-consumed; absent means unknown.
    """
    _CAP = 512

    def __init__(self, enabled=False):
        self.enabled = bool(enabled)
        self._samples = {stage: [] for stage in _STAGE_NAMES}
        self._dropped = {stage: 0 for stage in _STAGE_NAMES}

    def observe(self, stage, seconds):
        if not self.enabled or stage not in self._samples:
            return
        if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds < 0:
            return
        bucket = self._samples[stage]
        if len(bucket) >= self._CAP:
            bucket.pop(0)
            self._dropped[stage] += 1
        bucket.append(float(seconds))

    def summary(self):
        facts = {}
        for stage, bucket in self._samples.items():
            if bucket:
                facts[stage] = dict(_stage_quantiles(bucket), dropped=self._dropped[stage])
        return facts

    def reset(self):
        for stage in _STAGE_NAMES:
            self._samples[stage] = []
            self._dropped[stage] = 0


ERROR_KINDS = ("decode_failure", "decode_state_conflict", "decode_partial_conflict",
               "greptime_failure", "greptime_partial_ack", "greptime_timeout", "greptime_row_too_large",
               "canonical_serialize_failure", "sqlite_locked", "sqlite_full", "sqlite_corrupt",
               "archive_disk_reserve", "dirty_notify_failure")


def classify_error(exc):
    """Fixed safe error taxonomy; never leaks values, payloads, or identities."""
    if isinstance(exc, Rejection):
        return "archive_disk_reserve"
    message = str(exc)
    if message in ("decode cursor moved during decode", "decode partial moved during decode"):
        return "decode_state_conflict" if "cursor" in message else "decode_partial_conflict"
    if "canonical" in message or "nanosecond" in message or "mutually exclusive" in message:
        return "canonical_serialize_failure"
    if isinstance(exc, DownstreamError):
        code = str(exc)
        return code if code in ERROR_KINDS else "greptime_failure"
    if isinstance(exc, sqlite3.OperationalError) and "locked" in message:
        return "sqlite_locked"
    if isinstance(exc, sqlite3.OperationalError) and "full" in message:
        return "sqlite_full"
    if isinstance(exc, sqlite3.DatabaseError):
        return "sqlite_corrupt"
    return "decode_failure"


def archive_status(path, detail=False):
    """Default status serves durable counters only: no decode/session/raw scans.

    Per-epoch rows carry the explicit selected active_epoch (register_epoch
    writes archive_meta.active_epoch; never the latest-rowid heuristic).
    remaining_chunks = durable global raw_chunks - epoch processed_chunks, so
    raw accept costs one meta bump per batch, never one write per epoch per
    chunk. decoded_rows_exact = completed rows + durable partial emitted
    rows. sessions_total is a transactional counter with a dedicated
    migration; pending uses outbox_rows with legacy COUNT fallback. Explicit
    detail=True adds one per-epoch counts_json pass. Complexity: O(E) default
    (E = retained epoch count), O(E + S*E) detailed; session/raw history
    growth never changes default cost.
    """
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
        sessions = meta.get("sessions_total")
        if sessions is None:
            sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        pending = meta.get("outbox_rows")
        if pending is None:
            pending = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        oldest = conn.execute("SELECT MIN(id) FROM outbox").fetchone()[0]
        epoch_rows = conn.execute("SELECT epoch FROM epochs ORDER BY rowid").fetchall()
        active = meta.get("active_epoch")
        if not isinstance(active, str) and epoch_rows:
            active = epoch_rows[-1][0]
        try:
            state = {row[0]: row[1:] for row in conn.execute(
                "SELECT epoch,processed_chunks,decoded_rows,partial_rows,pending_sessions FROM decode_epoch_state")}
        except sqlite3.Error:
            state = {}
        backlog = 0
        epochs = []
        for (epoch,) in epoch_rows:
            if epoch in state:
                processed, rows, partial, pending_sessions = state[epoch]
            else:  # legacy archive before migration: per-epoch cursor sums only, still no history scan
                processed = conn.execute("SELECT COALESCE(SUM(next_seq),0) FROM decode_states WHERE epoch=?", (epoch,)).fetchone()[0]
                rows = conn.execute("SELECT COALESCE(SUM(rows),0) FROM decode_states WHERE epoch=?", (epoch,)).fetchone()[0]
                partial = conn.execute("SELECT COALESCE(SUM(rows_emitted),0) FROM decode_partial WHERE epoch=?", (epoch,)).fetchone()[0]
                pending_sessions = conn.execute("SELECT COUNT(*) FROM decode_pending WHERE epoch=?", (epoch,)).fetchone()[0]
            remaining = raw - processed
            retained = epoch != active
            entry = {"epoch": epoch, "active_epoch": epoch == active, "processed_chunks": processed,
                     "remaining_chunks": remaining, "active_backlog_chunks": 0 if retained else remaining,
                     "retained_backlog_chunks": remaining if retained else 0,
                     "decoded_rows": rows, "partial_rows": partial,
                     "decoded_rows_exact": rows + partial, "pending_sessions": pending_sessions}
            if detail:
                totals = {}
                for (encoded,) in conn.execute("SELECT counts_json FROM decode_states WHERE epoch=?", (epoch,)):
                    for key, value in json.loads(encoded).items():
                        totals[key] = totals.get(key, 0) + value
                entry["counts"] = totals
            epochs.append(entry)
            backlog += remaining
        errors = {kind: {"count": count, "last_ns": last} for kind, count, last in conn.execute("SELECT kind,count,last_ns FROM worker_errors")}
        freshness = {key: meta.get(key) for key in ("last_receive_ns", "last_decode_ns", "last_full_ack_ns")}
        counters = {key: meta.get(key, 0) for key in ("decoded_rows_total", "acked_rows_total")}
    facts = {"sessions": sessions, "raw_chunks": raw, "raw_bytes": raw_bytes, "pending_rows": pending,
             "epochs": epochs, "errors": errors, "oldest_pending_id": oldest, "backlog_chunks": backlog,
             "freshness": freshness, "counters": counters, "disk_reserve_bytes": None,
             "db_bytes": None, "wal_bytes": None, "disk_free_bytes": None}
    facts["pruned_chunks"] = meta.get("pruned_chunks", 0)
    facts["retained_raw_chunks"] = raw - facts["pruned_chunks"]
    facts["max_database_bytes"] = meta.get("max_database_bytes", 0) or None
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
_COMMA_ENCODED = urllib.parse.quote_plus(",", safe="", encoding="utf-8", errors="strict").encode("ascii")
_COMMA_ENCODED_LEN = len(_COMMA_ENCODED)
_FORM_PREFIX_BYTES = b"sql=" + urllib.parse.quote_plus(_SQL_PREFIX, safe="", encoding="utf-8", errors="strict").encode("ascii")


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


def _encode_text(text):
    """Single quote_plus/UTF-8 encoding pass; reused for budget fit and final body."""
    return urllib.parse.quote_plus(text, safe="", encoding="utf-8", errors="strict").encode("ascii")


def render_insert(rows):
    """Fixed table/columns, plain integer nanoseconds (literal-insert fast path), standard SQL literals."""
    cells = [_render_cell(row) for row in rows]
    if not cells:
        raise ValueError("cannot insert empty rows")
    return _SQL_PREFIX + ",".join(cells)

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



def outbox_usage(archive):
    """Durable decoded-outbox usage as (rows, canonical UTF-8 bytes).

    Hot-path gate reads the transactionally maintained archive_meta counters
    (same-transaction _outbox_bump on insert/delete); a legacy archive without
    the byte key backfills once at open. CAST to BLOB makes LENGTH count UTF-8
    bytes, not characters.
    """
    with archive.connect() as conn:
        meta = {key: value for key, value in
                conn.execute("SELECT key,value FROM archive_meta WHERE key IN ('outbox_rows','outbox_bytes')")}
        if "outbox_bytes" not in meta:
            rows, nbytes = conn.execute(
                "SELECT COUNT(*),COALESCE(SUM(LENGTH(CAST(row_json AS BLOB))),0) FROM outbox").fetchone()
            return (rows, nbytes)
    return (meta.get("outbox_rows", 0), meta.get("outbox_bytes", 0))


def outbox_table_usage(archive):
    """Authoritative table scan for reconciliation/tests (hot path uses outbox_usage)."""
    with archive.connect() as conn:
        rows, nbytes = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(LENGTH(CAST(row_json AS BLOB))),0) FROM outbox").fetchone()
    return (rows, nbytes)


def _migrate_outbox_bytes(conn):
    """Idempotent one-time backfill of archive_meta.outbox_bytes in the caller's transaction.

    Rollback-safe: a failed commit leaves neither the key nor partial state.
    Returns True when it backfilled, False when the key already existed.
    """
    if conn.execute("SELECT value FROM archive_meta WHERE key='outbox_bytes'").fetchone() is not None:
        return False
    total = conn.execute("SELECT COALESCE(SUM(LENGTH(CAST(row_json AS BLOB))),0) FROM outbox").fetchone()[0]
    conn.execute("INSERT OR IGNORE INTO archive_meta(key,value) VALUES('outbox_bytes',?)", (total,))
    return True


def _outbox_bump(conn, rows_delta, bytes_delta):
    """Same-transaction outbox counter move; call inside the insert/delete commit."""
    _meta_bump(conn, "outbox_rows", rows_delta)
    _meta_bump(conn, "outbox_bytes", bytes_delta)


def outbox_paused(rows, nbytes, high_rows=DEFAULT_OUTBOX_HIGH_ROWS, high_bytes=DEFAULT_OUTBOX_HIGH_BYTES,
                  low_rows=DEFAULT_OUTBOX_LOW_ROWS, low_bytes=DEFAULT_OUTBOX_LOW_BYTES, was_paused=False):
    """Hysteresis predicate over durable outbox usage (pure function, no I/O).

    High pauses decode only; a paused decoder resumes only once BOTH rows and
    bytes fall to their low marks. Unpaused state needs either high mark to
    pause. Bounds require positive integer highs with 0 <= low < high.
    """
    for value in (high_rows, low_rows, high_bytes, low_bytes):
        if type(value) is bool or not isinstance(value, int):
            raise ConfigurationError("invalid_outbox_watermarks")
    if not (high_rows > 0 and high_bytes > 0 and 0 <= low_rows < high_rows
            and 0 <= low_bytes < high_bytes):
        raise ConfigurationError("invalid_outbox_watermarks")
    if was_paused:
        return rows > low_rows or nbytes > low_bytes
    return rows >= high_rows or nbytes >= high_bytes


def decode_paused(archive, high_rows=DEFAULT_OUTBOX_HIGH_ROWS, high_bytes=DEFAULT_OUTBOX_HIGH_BYTES,
                  low_rows=DEFAULT_OUTBOX_LOW_ROWS, low_bytes=DEFAULT_OUTBOX_LOW_BYTES, was_paused=False):
    """Durable-counter hysteresis read for the decode pause gate.

    Oscillation is damped by the high/low gap: near the boundary a bounded
    in-flight batch may overshoot high by at most one DECODE_ROW_BUDGET batch
    (documented max overshoot), while resume waits for both axes at/below low.
    """
    rows, nbytes = outbox_usage(archive)
    return outbox_paused(rows, nbytes, high_rows, high_bytes, low_rows, low_bytes, was_paused)


def _wait_hint(stop_event, hint, interval, has_work):
    """Clear-before-check hinted wait; True means recheck work now, False means idle interval passed.

    The hint is cleared BEFORE the ground-truth recheck, so a commit between
    the two still wakes the wait (no lost wakeup). Callers MUST pass a real
    durable-state probe (never a constant False): clearing without a recheck
    would drop a commit that landed in the empty-read to clear window. Waits
    in short slices so an external stop_event.set() never misses its deadline
    by more than ~50ms. A failing probe sleeps the full idle interval instead
    of waking immediately, so storage errors never busy-loop.
    """
    hint.clear()
    try:
        if has_work():
            return True
    except (sqlite3.Error, OSError):
        stop_event.wait(interval)
        return False
    deadline = time.monotonic() + interval
    while not stop_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if hint.wait(min(remaining, 0.05)):
            return True
    return False


class Worker(threading.Thread):
    """Decode/upload loops with decoded-outbox backpressure and wake hints (issue #11).

    High watermark pauses DECODE ONLY (raw accept keeps its existing
    reserve/auth/atomic policy); low resumes. A batch already staged past the
    gate may overshoot high by at most one DECODE_ROW_BUDGET batch. SQLite
    tables stay authoritative: hints only skip idle sleeps via clear-before-
    check, so lost hints and restarts fall back to periodic table rechecks.
    A failed downstream sleeps its own retry backoff and is never busy-
    retried by new-work hints. External stop_event.set() joins within ~50ms
    slices; shutdown join semantics are unchanged.
    """
    def __init__(self, archive, decoder, greptime, interval=1, outbox_limit=20000, max_body_bytes=DEFAULT_MAX_BODY_BYTES,
                 high_rows=DEFAULT_OUTBOX_HIGH_ROWS, low_rows=DEFAULT_OUTBOX_LOW_ROWS,
                 high_bytes=DEFAULT_OUTBOX_HIGH_BYTES, low_bytes=DEFAULT_OUTBOX_LOW_BYTES,
                 retry_interval=None, importer=None):
        super().__init__(name="can-archive-worker", daemon=True)
        for value in (high_rows, low_rows, high_bytes, low_bytes):
            if type(value) is bool or not isinstance(value, int):
                raise ConfigurationError("invalid_outbox_watermarks")
        if not (high_rows > 0 and high_bytes > 0 and 0 <= low_rows < high_rows
                and 0 <= low_bytes < high_bytes):
            raise ConfigurationError("invalid_outbox_watermarks")
        self.archive, self.decoder, self.greptime = archive, decoder, greptime
        self.interval, self.outbox_limit, self.max_body_bytes = interval, outbox_limit, max_body_bytes
        self.high_rows, self.low_rows = high_rows, low_rows
        self.high_bytes, self.low_bytes = high_bytes, low_bytes
        self.retry_interval = interval if retry_interval is None else retry_interval
        self._decode_paused = False
        self.importer = importer
        self.stop_event = threading.Event()

    def notify_decode(self):
        """Room freed or new raw may exist: wake the decode loop for one recheck."""
        self.archive.decode_ready.set()

    def notify_upload(self):
        """New decoded rows may exist: wake the upload loop for one recheck."""
        self.archive.upload_ready.set()

    def _error(self, kind):
        try:
            self.archive.record_error(kind)
        except (sqlite3.Error, OSError):
            pass  # Storage failure must not kill the receiver or disclose raw data.

    def _wait_queue(self, hint, has_work):
        timing = self.archive._stages.enabled
        start = time.monotonic() if timing else 0.0
        try:
            return _wait_hint(self.stop_event, hint, self.interval, has_work)
        finally:
            if timing:
                self.archive.observe_stage("queue_wait", time.monotonic() - start)

    def _has_outbox(self):
        with self.archive.connect() as conn:
            return conn.execute("SELECT 1 FROM outbox LIMIT 1").fetchone() is not None

    def _has_pending(self):
        """Indexed active-epoch pending probe for the empty-decode recheck."""
        with self.archive.connect() as conn:
            return conn.execute(
                "SELECT 1 FROM decode_pending WHERE epoch=? AND head_id IS NOT NULL LIMIT 1",
                (self.decoder.epoch,)).fetchone() is not None

    def _decode_halted(self):
        try:
            return decode_paused(self.archive, self.high_rows, self.high_bytes,
                                 self.low_rows, self.low_bytes, self._decode_paused)
        except (sqlite3.Error, OSError):
            return False  # Fall through to decode_once, which surfaces the real error.

    def _upload(self):
        # One consumer owns ACK deletion; decoding can keep filling the durable outbox.
        while not self.stop_event.is_set():
            try:
                done = self.archive.flush_once(self.greptime, self.outbox_limit, self.max_body_bytes)
                if self.archive.prune_once():
                    continue
            except DownstreamError as exc:
                self._error(classify_error(exc))
                # Own retry backoff only: new-work hints must not busy-retry a failed downstream.
                self.stop_event.wait(self.retry_interval)
                continue
            except Exception as exc:
                kind = classify_error(exc)
                self._error(kind if kind != "decode_failure" else "greptime_failure")
                self.stop_event.wait(self.retry_interval)
                continue
            if done:
                # Full-ACK deletion freed room: hint decode (durable recheck decides).
                self.notify_decode()
                continue  # Drain backlog without idle sleep.
            # Empty outbox: hinted idle wait, periodic table recheck as fallback.
            self._wait_queue(self.archive.upload_ready, self._has_outbox)

    def run(self):
        uploader = threading.Thread(target=self._upload, name="can-archive-uploader", daemon=True)
        uploader.start()
        try:
            while not self.stop_event.is_set():
                if self._decode_halted():
                    # Paused: decode only; raw accept continues, uploader drains.
                    # Resume needs BOTH rows and bytes at/below low (durable recheck).
                    self._decode_paused = True
                    self._wait_queue(self.archive.decode_ready, lambda: not self._decode_halted())
                    continue
                self._decode_paused = False
                if self.importer is not None:
                    try:
                        self.importer.import_once()
                    except Exception as exc:
                        self._error(classify_error(exc))
                        self.stop_event.wait(self.retry_interval)
                        continue
                try:
                    progressed = bool(self.archive.decode_once(self.decoder))
                except Rejection as exc:
                    # Reserve/SQLite/serialization failure: fixed retry backoff,
                    # never hint-driven, so errors cannot trigger a FULL commit storm.
                    self._error(classify_error(exc))
                    self.stop_event.wait(self.retry_interval)
                    continue
                except Exception as exc:
                    self._error(classify_error(exc))
                    self.stop_event.wait(self.retry_interval)
                    continue
                if progressed:
                    # Decode commit added rows: hint upload (durable recheck decides).
                    self.notify_upload()
                else:
                    # Idle: clear-before-check against the indexed pending
                    # frontier, so an accept commit landing between the empty
                    # read and the clear still wakes us (no lost wakeup).
                    self._wait_queue(self.archive.decode_ready, self._has_pending)
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
        self._concurrency = concurrency
        self._shutdown = threading.Event()
        self._files = None
        self._files_lock = threading.Lock()
        super().__init__(address, Handler)

    @property
    def files(self):
        # Guard the lazy race with a dedicated lock (never a slot
        # permit): concurrent first requests share one store whose
        # RLock coordinates PUT/status.
        with self._files_lock:
            store = self._files
            if store is None:
                store = FileStore(self.archive, self.vehicle, self.collector_id)
                self._files = store
            return store

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
        # Drain in-flight handler commits before the caller releases role
        # locks: slot threads still hold one permit each until their commit
        # lands, and Handler's per-request deadline bounds each wait. Listener
        # is already closed above, so no new slot can start during the drain.
        for _ in range(self._concurrency):
            self._slots.acquire()
        for _ in range(self._concurrency):
            self._slots.release()

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
        if self.server._shutdown.is_set():
            # Shutdown began between keep-alive requests: dispatch nothing
            # further so the slot releases and the server_close drain proceeds.
            # The in-flight request already replied (reply closes on shutdown).
            self.close_connection = True
            return
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
        # Shutdown also closes: the in-flight reply still commits, but no next
        # keep-alive request can sustain the slot and starve server_close drain.
        self.close_connection = code != 200 or self.server._shutdown.is_set()
        self.send_response(code)
        self.send_header("Content-Type", "application/x-protobuf" if protobuf else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "keep-alive" if not self.close_connection else "close")
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
        if self.path == "/v1/files" or self.path.startswith("/v1/files/"):
            self._handle_file(put=False)
        else:
            self._handle(status=True)

    def do_POST(self):
        self._handle(status=False)

    def do_PUT(self):
        self._handle_file(put=True)

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

    def _handle_file(self, put):
        try:
            self.authenticate()
            if not self.path.startswith("/v1/files/"):
                raise Rejection(404, "not_found")
            remainder = self.path[len("/v1/files/"):]
            if not remainder or remainder.endswith("/") or "%" in remainder \
                    or "?" in remainder or "#" in remainder:
                raise Rejection(404, "not_found")
            session_id, sep, filename = remainder.partition("/")
            if not sep or not session_id or not filename or "/" in filename:
                raise Rejection(404, "not_found")
            vehicles = self.headers.get_all("X-CAN-Vehicle", [])
            collectors = self.headers.get_all("X-CAN-Collector", [])
            if len(vehicles) != 1 or len(collectors) != 1:
                raise Rejection(403, "identity_mismatch")
            if vehicles[0] != self.server.vehicle or collectors[0] != self.server.collector_id:
                raise Rejection(403, "identity_mismatch")
            if self.headers.get_all("Transfer-Encoding") or self.headers.get_all("Content-Encoding"):
                raise Rejection(400, "invalid_content_encoding")
            try:
                ack = self.server.files.status(session_id, filename)
            except FileRejection as exc:
                raise Rejection(exc.status, exc.reason) from None
            except FileConfigurationError:
                raise Rejection(503, "archive_unavailable") from None
            if not put:
                lengths = self.headers.get_all("Content-Length", [])
                if len(lengths) > 1 or (lengths and lengths[0] != "0"):
                    raise Rejection(400, "invalid_content_length")
                self.reply(200, _json(ack).encode())
                return
            lengths = self.headers.get_all("Content-Length", [])
            if len(lengths) != 1 or not re.fullmatch(r"[0-9]+", lengths[0]):
                raise Rejection(400, "invalid_content_length")
            if len(lengths[0]) > 10:
                raise Rejection(413, "request_too_large")
            length = int(lengths[0])
            limit = MAX_CAPTURE_BYTES if filename == "capture.json" else MAX_FILE_RANGE
            if length > limit:
                raise Rejection(413, "request_too_large")
            types = self.headers.get_all("Content-Type", [])
            if len(types) != 1 or types[0].split(";", 1)[0].strip().lower() != "application/octet-stream":
                raise Rejection(415, "unsupported_content_type")
            offsets = self.headers.get_all("X-CAN-Offset", [])
            digests = self.headers.get_all("X-CAN-SHA256", [])
            if len(offsets) != 1 or not re.fullmatch(r"[0-9]{1,19}", offsets[0]):
                raise Rejection(400, "invalid_file_offset")
            if len(digests) != 1 or not re.fullmatch(r"[0-9a-f]{64}", digests[0]):
                raise Rejection(400, "invalid_file_hash")
            body = self.rfile.read(length) if length else b""
            if len(body) != length:
                self._expected_remainder = length - len(body)
                raise Rejection(400, "truncated_request")
            self._expected_remainder = 0
            try:
                ack = self.server.files.put(session_id, filename, int(offsets[0]), digests[0], body)
            except FileRejection as exc:
                raise Rejection(exc.status, exc.reason) from None
            except FileConfigurationError:
                raise Rejection(503, "archive_unavailable") from None
            self.reply(200, _json(ack).encode())
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

def _check_watermarks(high_rows, low_rows, high_bytes, low_bytes):
    for value in (high_rows, low_rows, high_bytes, low_bytes):
        if type(value) is bool or not isinstance(value, int):
            raise ConfigurationError("invalid_outbox_watermarks")
    if not (high_rows > 0 and high_bytes > 0
            and 0 <= low_rows < high_rows
            and 0 <= low_bytes < high_bytes):
        raise ConfigurationError("invalid_outbox_watermarks")

@contextlib.contextmanager
def _role_archive(args, role_locks):
    """Lifetime role locks around init and the whole role run.

    Directory is prepared/validated before lock files are created, so a first
    start on a nested non-existent parent works. Role + legacy SH locks are
    held for the whole run (released only after the graceful drain below);
    the EX init lock lives only inside Archive.__init__.
    """
    path = _archive_path(args.database)
    _prepare_archive_dir(path)
    with role_locks(path):
        yield Archive(path, args.disk_reserve_mib * 1024 * 1024,
                      max_database_bytes=args.raw_cache_mib * 1024 * 1024 if args.raw_cache_mib else None)


def _run_serve(args):
    if type(args.port) is bool or not 0 <= args.port <= 65535 or not math.isfinite(args.http_timeout) or args.http_timeout <= 0:
        raise ConfigurationError("invalid_receiver_limits")
    otlp_user, otlp_password = _credentials("CAN_OTLP")
    with _role_archive(args, receiver_locks) as archive:
        server = Receiver((args.bind, args.port), archive, args.vehicle, args.collector_id,
                          otlp_user, otlp_password, args.http_timeout)
        previous_term = signal.getsignal(signal.SIGTERM)
        previous_int = signal.getsignal(signal.SIGINT)

        def _stop(signum, frame):
            # shutdown() must not run in the serve_forever thread itself: it blocks
            # waiting for that same thread. Signal from a helper thread instead.
            threading.Thread(target=server.shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
        print(_json({"receiver": "ready", "port": server.server_port}), flush=True)
        try:
            server.serve_forever()
        finally:
            # Handlers stay installed through the drain, so a second SIGTERM
            # during server_close still routes to shutdown, not the default.
            server.server_close()
            signal.signal(signal.SIGTERM, previous_term)
            signal.signal(signal.SIGINT, previous_int)
    return 0


def _run_work(args):
    retry_interval = args.worker_interval if args.downstream_retry_interval is None else args.downstream_retry_interval
    if (not math.isfinite(args.worker_interval) or args.worker_interval <= 0
            or not math.isfinite(args.greptime_timeout) or args.greptime_timeout <= 0
            or not 1 <= args.outbox_limit <= 20000 or args.max_body_bytes < 1
            or not math.isfinite(retry_interval) or retry_interval <= 0):
        raise ConfigurationError("invalid_receiver_limits")
    _check_watermarks(args.outbox_high_rows, args.outbox_low_rows,
                      args.outbox_high_bytes, args.outbox_low_bytes)
    db_user, db_password = _credentials("GREPTIME")
    from scripts.ingest.can.can_decoder import Decoder
    decoder = Decoder(args.dbc, args.definitions)
    greptime = Greptime(args.greptime_url, args.greptime_db, db_user, db_password,
                        args.greptime_timeout, args.max_body_bytes)
    with _role_archive(args, worker_locks) as archive:
        archive.register_epoch(decoder)
        worker = Worker(archive, decoder, greptime, args.worker_interval, args.outbox_limit, args.max_body_bytes,
                        high_rows=args.outbox_high_rows, low_rows=args.outbox_low_rows,
                        high_bytes=args.outbox_high_bytes, low_bytes=args.outbox_low_bytes,
                        retry_interval=retry_interval, importer=FileImporter(archive))
        previous_term = signal.getsignal(signal.SIGTERM)
        previous_int = signal.getsignal(signal.SIGINT)

        def _stop(signum, frame):
            worker.stop_event.set()
        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
        worker.start()
        print(_json({"worker": "ready"}), flush=True)
        try:
            while worker.is_alive():
                worker.join(timeout=0.5)
        finally:
            worker.stop_event.set()
            worker.join()
            greptime.close()
            signal.signal(signal.SIGTERM, previous_term)
            signal.signal(signal.SIGINT, previous_int)
    return 0


def _run_redecode(args):
    from scripts.ingest.can.can_decoder import Decoder
    decoder = Decoder(args.dbc, args.definitions)
    with _role_archive(args, worker_locks) as archive:
        archive.register_epoch(decoder, explicit=True)
        while archive.decode_once(decoder):
            pass
        print(_json(archive.status()))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="receive durable raw OTLP only")
    work = commands.add_parser("work", help="decode stored raw and upload to Greptime")
    replay = commands.add_parser("re-decode", help="explicitly decode retained raw into an independent epoch/outbox")
    status = commands.add_parser("status", help="print only local archive counts and non-sensitive error codes")
    status.add_argument("--detail", action="store_true", help="include per-epoch signal counts (scans decode_states)")
    for command in (serve, work, replay, status):
        command.add_argument("--database", required=True)
    serve.add_argument("--vehicle", required=True)
    serve.add_argument("--collector-id", required=True)
    serve.add_argument("--bind", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=4319)
    serve.add_argument("--http-timeout", type=float, default=15)
    serve.add_argument("--disk-reserve-mib", type=int, default=64)
    serve.add_argument("--raw-cache-mib", type=int, default=256, help="SQLite cache cap; 0 retains raw for archival replay")
    for command in (work, replay):
        command.add_argument("--dbc", required=True)
        command.add_argument("--definitions", required=True)
        command.add_argument("--disk-reserve-mib", type=int, default=64)
        command.add_argument("--raw-cache-mib", type=int, default=256 if command is work else 0)
    work.add_argument("--greptime-url", required=True)
    work.add_argument("--greptime-db", required=True)
    work.add_argument("--worker-interval", type=float, default=1)
    work.add_argument("--greptime-timeout", type=float, default=60)
    work.add_argument("--outbox-limit", type=int, default=20000)
    work.add_argument("--max-body-bytes", type=int, default=4 * 1024 * 1024)
    work.add_argument("--outbox-high-rows", type=int, default=DEFAULT_OUTBOX_HIGH_ROWS)
    work.add_argument("--outbox-low-rows", type=int, default=DEFAULT_OUTBOX_LOW_ROWS)
    work.add_argument("--outbox-high-bytes", type=int, default=DEFAULT_OUTBOX_HIGH_BYTES)
    work.add_argument("--outbox-low-bytes", type=int, default=DEFAULT_OUTBOX_LOW_BYTES)
    work.add_argument("--downstream-retry-interval", type=float, default=None)
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            print(_json(archive_status(args.database, detail=args.detail)))
            return 0
        os.umask(0o077)
        if args.disk_reserve_mib < 0:
            raise ConfigurationError("disk_reserve_must_be_nonnegative")
        if args.raw_cache_mib < 0:
            raise ConfigurationError("raw_cache_must_be_nonnegative")
        if args.command == "serve":
            return _run_serve(args)
        if args.command == "work":
            return _run_work(args)
        return _run_redecode(args)
    except ConfigurationError as exc:
        print(_json({"error": str(exc)}), flush=True)
        return 1
    except Exception:
        # Configuration/parser/DB exceptions can contain paths or values: do not echo them.
        print('{"error":"configuration_or_archive_failure"}', flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
