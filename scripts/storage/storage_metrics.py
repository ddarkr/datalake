#!/usr/bin/env python3
"""S3 bucket usage inventory plus receiver raw-archive observability.

Reports what is really in the bucket (live SSTs, existing backups,
historical objects) via paginated ListObjectsV2. The raw MF4 bucket is
separate and never counted here.

Shared helper: raw_upload.py imports bucket_inventory() for its own Raw
bucket with its own limited key; keys never cross services.

Metrics (port 9105, internal only):
  datalake_s3_bucket_bytes{scope="greptime"}
  datalake_s3_bucket_objects{scope="greptime"}
  datalake_s3_inventory_timestamp_seconds  (last success only)
  datalake_s3_inventory_configured         (1 only: S3 mode + S3 settings)
  datalake_s3_inventory_success            (1 only: current values are fresh)
  datalake_receiver_archive_known/success  (1 only: raw archive read worked)
  datalake_receiver_raw_chunks/raw_bytes/sessions/pending_rows
                                           (raw totals from archive_meta)
  datalake_receiver_oldest_pending_id/event/ingest (only when outbox non-empty)
  datalake_receiver_db_bytes/wal_bytes     (archive file + exact -wal file)
  datalake_receiver_shm/journal_bytes     (aux sidecars, only when present)
  datalake_receiver_disk_size/free_bytes   (archive filesystem capacity)
  datalake_receiver_disk_reserve_bytes     (only when configured, else unknown)
  datalake_receiver_errors_total{kind}     (persisted worker error counters)
  datalake_receiver_last_receive/decode/full_ack_timestamp_seconds
                                           (only when known, never invented)
  datalake_receiver_decoded/acked_rows_total (receipt vs decode vs DB commit)
  datalake_storage_cost_*            (policy + bounded retained-run gauges;
                                      never billing-month/account coverage)
  datalake_receiver_epoch_*{scope="active"|"retained"} (trigger-kept epoch
                                           state; fixed active/retained labels,
                                           never epoch/session/vehicle values)

Only successful inventories publish values; a failure keeps the last good
values with success=0 (visibly stale, never a fake zero). The receiver
archive is read synchronously per scrape through a read-only URI with
query_only (never WAL-checkpointed, pruned, or vacuumed) plus bounded file
stats: cheap indexed sessions/outbox/errors queries plus one explicit
archive_meta key lookup. A missing, corrupt, malformed, or unmounted archive
reports known=0/success=0 with no value metrics; malformed required counter
data is unknown (never silently skipped while known=1). Legacy archives
without archive_meta omit raw totals/counters; unknown freshness and absent
reserve stay omitted, never zeroed.
File mode serves the disabled state without contacting S3 (boto3 is never
even imported). Errors never carry bucket/key/credential text: type name only.
"""

import os
import json
import math
import re

from scripts.storage.s3_cost import load_cost_reports
import shutil
import signal
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SCOPE = "greptime"

_S = {"bytes": None, "objects": None, "timestamp": 0,
      "configured": 0, "success": 0}
_SLOCK = threading.Lock()


def e(name, default=""):
    return os.environ.get(name, default)


def ei(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit("invalid integer %s=%r" % (name, raw))


def bucket_inventory(client, bucket):
    """Paginated (total_bytes, object_count); streams, never stores entries."""
    total = 0
    count = 0
    token = None
    while True:
        kw = {"Bucket": bucket}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        for obj in resp.get("Contents") or []:
            total += int(obj["Size"])
            count += 1
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                raise IOError("s3 listing truncated without continuation token")
        else:
            return total, count


def make_client():
    import boto3  # noqa: pinned dep, lazy so File mode never needs it
    from botocore.config import Config
    from scripts.database.greptime_preflight import derive_b2_region
    return boto3.client(
        "s3", endpoint_url=e("S3_ENDPOINT_URL") or None,
        region_name=e("S3_REGION") or derive_b2_region(e("S3_ENDPOINT_URL")) or None,
        aws_access_key_id=e("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=e("S3_SECRET_ACCESS_KEY"),
        config=Config(retries={"max_attempts": 1},  # own cadence below
                      connect_timeout=10, read_timeout=60))


def s3_configured():
    """True only in S3 mode with connection settings; else disabled, no S3."""
    return e("GREPTIME_STORAGE_TYPE", "S3") == "S3" and bool(
        e("S3_ENDPOINT_URL") and e("S3_BUCKET") and e("S3_ACCESS_KEY_ID")
        and e("S3_SECRET_ACCESS_KEY"))


def resolve_client():
    """Client, or None when disabled: the caller must not contact S3 then."""
    if not s3_configured():
        return None
    return make_client()


def refresh_once(client, bucket):
    """Success replaces values; failure keeps prior (stale, success=0)."""
    try:
        total, count = bucket_inventory(client, bucket)
    except Exception as ex:  # never leak bucket/key text: type name only
        print("inventory error: %s" % type(ex).__name__, file=sys.stderr)
        with _SLOCK:
            _S["success"] = 0
        return
    with _SLOCK:
        _S.update(bytes=total, objects=count,
                  timestamp=int(time.time()), success=1)

_RECEIVER_KINDS = ("decode_failure", "decode_state_conflict", "decode_partial_conflict",
                   "greptime_failure", "greptime_partial_ack",
                   "greptime_timeout", "greptime_row_too_large", "canonical_serialize_failure",
                   "sqlite_locked", "sqlite_full", "sqlite_corrupt",
                   "archive_disk_reserve", "dirty_notify_failure")
_RECEIVER_FRESHNESS = (("last_receive_ns", "datalake_receiver_last_receive_timestamp_seconds"),
                       ("last_decode_ns", "datalake_receiver_last_decode_timestamp_seconds"),
                       ("last_full_ack_ns", "datalake_receiver_last_full_ack_timestamp_seconds"))


def filesystem_metrics(path="/greptime-data"):
    """Read-only capacity and logical WAL bytes; no SST tree scan."""
    try:
        usage = shutil.disk_usage(path)
        pending = [os.path.join(path, "wal")]
        wal_bytes = 0
        while pending:
            try:
                with os.scandir(pending.pop()) as entries:
                    for entry in entries:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                pending.append(entry.path)
                            elif entry.is_file(follow_symlinks=False):
                                wal_bytes += entry.stat(follow_symlinks=False).st_size
                        except FileNotFoundError:
                            pass  # concurrent WAL purge
            except FileNotFoundError:
                pass  # empty/new volume or concurrent WAL directory purge
        return {"datalake_local_storage_success": 1,
                "datalake_disk_size_bytes": usage.total,
                "datalake_disk_used_bytes": usage.used,
                "datalake_disk_free_bytes": usage.free,
                "datalake_wal_bytes": wal_bytes}
    except OSError:
        return {"datalake_local_storage_success": 0}


def operational_metrics(path="/ops"):
    """Missing/corrupt markers are unknown; old timestamps remain visibly old."""
    values = {}
    for filename, prefix in (("aggregate-status.json", "datalake_aggregate"),
                             ("restore-verification.json", "datalake_restore_verification")):
        try:
            with open(os.path.join(path, filename)) as handle:
                state = json.load(handle)
            stamp = state["timestamp_seconds"]
            if (isinstance(stamp, bool) or not isinstance(stamp, (int, float))
                    or not math.isfinite(stamp) or stamp <= 0):
                raise ValueError("invalid timestamp")
            if not isinstance(state, dict):
                raise ValueError("invalid state")
        except (OSError, ValueError, TypeError, KeyError):
            values[prefix + ("_status_known" if filename.startswith("aggregate") else "_known")] = 0
            continue
        values[prefix + ("_status_known" if filename.startswith("aggregate") else "_known")] = 1
        if filename.startswith("restore"):
            values[prefix + "_timestamp_seconds"] = stamp
            continue
        values[prefix + "_status_timestamp_seconds"] = stamp
        for key in ("interval_seconds", "running", "success",
                    "last_start_timestamp_seconds", "last_success_timestamp_seconds",
                    "last_failure_timestamp_seconds"):
            value = state.get(key)
            if (not isinstance(value, bool) and isinstance(value, (int, float))
                    and math.isfinite(value) and value >= 0):
                values[prefix + "_" + key] = value
    return values


_META_KEYS = ("raw_chunks", "raw_bytes", "outbox_rows", "last_receive_ns",
              "last_decode_ns", "last_full_ack_ns", "decoded_rows_total",
              "acked_rows_total", "sessions_total", "active_epoch")
_META_COUNTS = {"raw_chunks": "datalake_receiver_raw_chunks",
                "raw_bytes": "datalake_receiver_raw_bytes",
                "decoded_rows_total": "datalake_receiver_decoded_rows_total",
                "acked_rows_total": "datalake_receiver_acked_rows_total"}
_EPOCH_GAUGES = {"processed_chunks": "datalake_receiver_epoch_processed_chunks",
                 "remaining_chunks": "datalake_receiver_epoch_remaining_chunks",
                 "active_backlog_chunks": "datalake_receiver_epoch_active_backlog_chunks",
                 "retained_backlog_chunks": "datalake_receiver_epoch_retained_backlog_chunks",
                 "decoded_rows": "datalake_receiver_epoch_decoded_rows",
                 "partial_rows": "datalake_receiver_epoch_partial_rows",
                 "decoded_rows_exact": "datalake_receiver_epoch_decoded_rows_exact",
                 "pending_sessions": "datalake_receiver_epoch_pending_sessions"}


def _valid_count(value):
    return (isinstance(value, int) and not isinstance(value, bool)
            and 0 <= value <= 2**63 - 1)


def _valid_ns(value):
    return (isinstance(value, int) and not isinstance(value, bool)
            and 0 < value <= 2**63 - 1)


def _receiver_meta(conn):
    """Exact ReceiverImprove contract: archive_meta(key TEXT PK, value INT).

    Returns (facts, malformed): facts maps the known keys to raw DB values
    (absent row = unknown); malformed is True when the table exists but has
    the wrong shape or a known key holds a non-integer value, in which case
    the caller reports archive unknown (never healthy with skipped facts).
    Unknown extra keys are ignored.
    """
    facts, malformed = {}, False
    try:
        shape = conn.execute("PRAGMA table_info(archive_meta)").fetchall()
    except sqlite3.Error:
        return facts, malformed  # legacy archive: counters simply unknown
    if not shape:
        return facts, malformed  # legacy archive: counters simply unknown
    if ([(row[1], row[2].upper()) for row in shape]
            != [("key", "TEXT"), ("value", "INTEGER")]):
        return facts, True
    try:
        rows = conn.execute(
            "SELECT key,value FROM archive_meta WHERE key IN (?,?,?,?,?,?,?,?,?,?)",
            _META_KEYS).fetchall()
    except sqlite3.Error:
        return facts, True
    for row in rows:
        if not isinstance(row, (tuple, list)) or len(row) != 2:
            return facts, True
        key, value = row
        if not isinstance(key, str):
            return facts, True
        if key in _META_KEYS and key not in facts:
            if key == "active_epoch":
                if not isinstance(value, str):
                    return facts, True
            elif not isinstance(value, int) or isinstance(value, bool):
                return facts, True
            facts[key] = value
    return facts, malformed



def receiver_metrics(archive=None, reserve_bytes=None):
    """Read-only raw-archive facts; unreadable stays known=0 with no values.

    Opens the SQLite archive through a read-only URI with query_only (never
    checkpointed, pruned, or vacuumed): cheap indexed sessions/outbox/errors
    queries plus one explicit archive_meta key lookup. Never COUNT/SUM over
    raw_chunks. Raw totals and receive/decode/full-ACK freshness come only
    from archive_meta (absent row/table = unknown, never zero); malformed
    archive_meta or error/row data reports archive unknown. Archive file
    mtime marks the last local DB write; per-stage freshness needs the
    receiver counters (receipt vs decode vs DB commit).
    """
    path = archive or e("CAN_RECEIVER_ARCHIVE") or "/can-raw/raw.sqlite3"
    if reserve_bytes is None:
        raw = os.environ.get("CAN_RECEIVER_DISK_RESERVE_BYTES", "")
        if re.fullmatch(r"[0-9]+", raw or ""):
            reserve_bytes = int(raw)
    try:
        db_bytes = os.path.getsize(path)
        mtime = os.path.getmtime(path)
    except OSError:
        return {"datalake_receiver_archive_known": 0,
                "datalake_receiver_archive_success": 0}
    try:
        uri = Path(path).absolute().as_uri() + "?mode=ro"
    except ValueError:
        return {"datalake_receiver_archive_known": 0,
                "datalake_receiver_archive_success": 0}
    values = {}
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")  # Counters and oldest row share one read-only snapshot.
            meta, meta_malformed = _receiver_meta(conn)
            if meta_malformed:
                raise sqlite3.Error("malformed archive_meta")
            if "sessions_total" in meta:
                sessions = meta["sessions_total"]
            else:  # legacy archive before migration: single indexed COUNT only
                sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            if not _valid_count(sessions):
                raise sqlite3.Error("invalid session count")
            for _key in _META_COUNTS:
                if _key in meta and not _valid_count(meta[_key]):
                    raise sqlite3.Error("invalid archive_meta count")
            if "outbox_rows" in meta:
                if not _valid_count(meta["outbox_rows"]):
                    raise sqlite3.Error("invalid outbox_rows counter")
                pending = meta["outbox_rows"]
                oldest = conn.execute("SELECT MIN(id) FROM outbox").fetchone()[0]
            else:  # legacy archive without counters: indexed COUNT/MIN only
                pending, oldest = conn.execute(
                    "SELECT COUNT(*),MIN(id) FROM outbox").fetchone()
                if not _valid_count(pending):
                    raise sqlite3.Error("invalid outbox count")
            if oldest is not None and not _valid_count(oldest):
                raise sqlite3.Error("invalid oldest outbox id")
            try:
                errors = conn.execute(
                    "SELECT kind,count,last_ns FROM worker_errors LIMIT 64").fetchall()
            except sqlite3.Error:
                errors = None  # errors table absent: error counts stay unknown
            seen = {}
            if errors is not None:
                for erow in errors:
                    if not isinstance(erow, (tuple, list)) or len(erow) != 3:
                        raise sqlite3.Error("invalid error row")
                    kind, count, last_ns = erow
                    if kind in _RECEIVER_KINDS:
                        if not _valid_count(count):
                            raise sqlite3.Error("invalid worker error count")
                        seen[kind] = (count, last_ns if _valid_ns(last_ns) else None)
            try:
                epoch_state = conn.execute(
                    "SELECT epoch,processed_chunks,decoded_rows,partial_rows,pending_sessions,"
                    "COALESCE(decoded_rows,0)+COALESCE(partial_rows,0) FROM decode_epoch_state LIMIT 64").fetchall()
            except sqlite3.Error:
                epoch_state = None  # legacy archive before migration: epoch gauges stay unknown
            epoch_rows = []
            if epoch_state is not None:
                order = [row[0] for row in conn.execute("SELECT epoch FROM epochs ORDER BY rowid").fetchall()]
                active = meta.get("active_epoch")
                if not isinstance(active, str):
                    active = order[-1] if order else None
                raw_total = meta.get("raw_chunks")
                if raw_total is None or not _valid_count(raw_total):
                    raise sqlite3.Error("invalid raw counter for epoch remaining")
                for _key in ("sessions_total",):
                    if _key in meta and not _valid_count(meta[_key]):
                        raise sqlite3.Error("invalid archive_meta count")
                by_epoch = {}
                for erow in epoch_state:
                    if (not isinstance(erow, (tuple, list)) or len(erow) != 6 or not isinstance(erow[0], str)
                            or any(not _valid_count(v) for v in erow[1:])):
                        raise sqlite3.Error("invalid epoch state row")
                    by_epoch[erow[0]] = erow[1:]
                for epoch in order:
                    if epoch not in by_epoch:
                        raise sqlite3.Error("missing epoch state row")
                    processed, decoded, partial, pending_sessions, exact = by_epoch[epoch]
                    remaining = raw_total - processed
                    if remaining < 0:
                        raise sqlite3.Error("invalid epoch remaining")
                    epoch_rows.append((epoch == active, processed, remaining, decoded, partial, pending_sessions, exact))
            oldest_event_ns = oldest_ingest_ns = None
            if oldest is not None:
                row = conn.execute(
                    "SELECT row_json FROM outbox WHERE id=?", (oldest,)).fetchone()
                if row is None:
                    raise sqlite3.Error("missing oldest outbox row")
                try:
                    body = json.loads(row[0])
                    if not isinstance(body, dict):
                        raise ValueError("invalid row body")
                    event_ns, ingest_ns = body.get("event_time"), body.get("ingest_time")
                    oldest_event_ns = event_ns if _valid_ns(event_ns) else None
                    oldest_ingest_ns = ingest_ns if _valid_ns(ingest_ns) else None
                except (ValueError, TypeError, AttributeError):
                    raise sqlite3.Error("invalid oldest outbox row")
        finally:
            conn.close()
    except sqlite3.Error:
        return {"datalake_receiver_archive_known": 0,
                "datalake_receiver_archive_success": 0}
    values["datalake_receiver_archive_known"] = 1
    values["datalake_receiver_archive_success"] = 1
    values["datalake_receiver_sessions"] = sessions
    values["datalake_receiver_pending_rows"] = pending
    if oldest is not None:
        values["datalake_receiver_oldest_pending_id"] = oldest
    if oldest_event_ns is not None:
        values["datalake_receiver_oldest_pending_event_timestamp_seconds"] = oldest_event_ns / 1e9
    if oldest_ingest_ns is not None:
        values["datalake_receiver_oldest_pending_ingest_timestamp_seconds"] = oldest_ingest_ns / 1e9
    for kind in _RECEIVER_KINDS:
        values['datalake_receiver_errors_total{kind="%s"}' % kind] = seen.get(kind, (0, None))[0]
        last_ns = seen.get(kind, (0, None))[1]
        if last_ns is not None:
            values['datalake_receiver_error_last_timestamp_seconds{kind="%s"}' % kind] = last_ns / 1e9
    for key, metric in _META_COUNTS.items():
        if key in meta:
            values[metric] = meta[key]  # validated inside the caught domain
    if epoch_rows:
        active_totals, retained_totals = [0] * 6, [0] * 6
        for is_active, processed, remaining, decoded, partial, pending_sessions, exact in epoch_rows:
            slot = active_totals if is_active else retained_totals
            for index, value in enumerate((processed, remaining, decoded, partial, pending_sessions, exact)):
                slot[index] += value
        names = ("processed_chunks", "remaining_chunks", "decoded_rows", "partial_rows",
                 "pending_sessions", "decoded_rows_exact")
        for role, totals in (("active", active_totals), ("retained", retained_totals)):
            for name, total in zip(names, totals):
                values['%s{scope="%s"}' % (_EPOCH_GAUGES[name], role)] = total
            values['%s{scope="%s"}' % (_EPOCH_GAUGES["active_backlog_chunks"], role)] = totals[1] if role == "active" else 0
            values['%s{scope="%s"}' % (_EPOCH_GAUGES["retained_backlog_chunks"], role)] = 0 if role == "active" else totals[1]
    for key, metric in _RECEIVER_FRESHNESS:
        if _valid_ns(meta.get(key)):
            values[metric] = meta[key] / 1e9
    values["datalake_receiver_db_bytes"] = db_bytes
    wal_bytes = 0
    wal_unknown = False
    try:
        wal_bytes = os.path.getsize(path + "-wal")
    except OSError as exc:
        if exc.errno != 2:  # absent WAL is legitimately zero, never unknown
            wal_unknown = True
    if wal_unknown:
        return {"datalake_receiver_archive_known": 0,
                "datalake_receiver_archive_success": 0}
    values["datalake_receiver_wal_bytes"] = wal_bytes
    for suffix, metric in (("-shm", "datalake_receiver_shm_bytes"),
                           ("-journal", "datalake_receiver_journal_bytes")):
        try:
            values[metric] = os.path.getsize(path + suffix)
        except OSError:
            pass  # aux sidecars are optional; WAL absence already means zero
    try:
        usage = shutil.disk_usage(os.path.dirname(os.path.abspath(path)) or "/")
        values["datalake_receiver_disk_size_bytes"] = usage.total
        values["datalake_receiver_disk_free_bytes"] = usage.free
    except OSError:
        pass
    if _valid_count(reserve_bytes):
        values["datalake_receiver_disk_reserve_bytes"] = reserve_bytes
    if isinstance(mtime, float) and math.isfinite(mtime) and mtime > 0:
        values["datalake_receiver_archive_mtime_timestamp_seconds"] = mtime
    return values


# Defaults apply ONLY when unset. Invalid configured policy is unknown,
# not silently replaced by a zero budget or a plausible provider default.
def cost_policy_metrics():
    values = {}
    known = True
    for env, metric, default, factor in (
        ("STORAGE_COST_FREE_EGRESS_MULTIPLIER", "free_egress_multiplier", 3.0, 1),
        ("STORAGE_COST_EGRESS_USD_PER_GB", "egress_usd_per_gb", 0.01, 1),
        ("STORAGE_COST_MAX_MONTHLY_EGRESS_GIB", "max_monthly_egress_bytes", 2400, 1024**3),
        ("STORAGE_COST_MAX_MONTHLY_OVERAGE_USD", "max_monthly_overage_usd", 0.0, 1),
    ):
        raw = os.environ.get(env)
        try:
            value = default if raw in (None, "") else float(raw)
            value *= factor
            if not math.isfinite(value) or value < 0:
                raise ValueError("invalid policy")
        except (ValueError, OverflowError):
            known = False
            continue
        values["datalake_storage_cost_" + metric] = value
    values["datalake_storage_cost_policy_known"] = int(known)
    return values


_COST_REPORT_DIRS = ("/ops", "/can-ops/can")
_REPORT_MAX_AGE_SECONDS = 48 * 3600


def cost_report_metrics(dirs=_COST_REPORT_DIRS, now=None):
    """Retained-run gauges, not cumulative counters or calendar-month totals.

    End-of-run reports cannot establish uninterrupted capture, account scope,
    or month coverage: complete stays 0 even with fresh valid reports. Invalid,
    truncated, missing or stale inputs also set known=0. Values that are present
    are lower bounds from retained reports and MUST NOT be used with increase().
    """
    now = time.time() if now is None else now
    reports = [load_cost_reports(path, now=now) for path in dict.fromkeys(dirs)]
    valid = sum(report["valid"] for report in reports)
    invalid = sum(report["invalid"] for report in reports)
    stamps = [report["newest_timestamp"] for report in reports if report["newest_timestamp"] is not None]
    oldest = [report["oldest_timestamp"] for report in reports if report["oldest_timestamp"] is not None]
    newest = max(stamps) if stamps else None
    truncated = any(report["truncated"] for report in reports)
    fresh = (bool(reports) and all(
        report["newest_timestamp"] is not None
        and 0 <= now - report["newest_timestamp"] <= _REPORT_MAX_AGE_SECONDS
        for report in reports))
    known = (valid > 0 and invalid == 0 and not truncated and fresh
             and all(report["configured"] for report in reports))
    out = {"datalake_storage_cost_report_known": int(known),
           "datalake_storage_cost_report_complete": 0,
           "datalake_storage_cost_report_truncated": int(truncated),
           "datalake_storage_cost_report_files": valid,
           "datalake_storage_cost_report_invalid_files": invalid}
    if newest is not None:
        out["datalake_storage_cost_report_timestamp_seconds"] = newest
        out["datalake_storage_cost_report_oldest_timestamp_seconds"] = min(oldest)
    totals = {}
    for report in reports:
        for comp, agg in report["totals"].items():
            current = totals.setdefault(comp, {"requests": {}, "attempts": {}, "retries": {}, "errors": {},
                        "read_bytes": 0, "write_bytes": 0, "copy_bytes": 0,
                        "unknown": {"get": 0, "put": 0, "copy": 0}, "http_observed": True})
            for section in ("requests", "attempts", "retries", "errors"):
                for op, count in agg[section].items():
                    current[section][op] = current[section].get(op, 0) + count
            for field in ("read_bytes", "write_bytes", "copy_bytes"):
                current[field] += agg[field]
            for kind, count in agg["unknown"].items():
                current["unknown"][kind] += count
            current["http_observed"] &= agg["http_observed"]
    for comp, agg in sorted(totals.items()):
        for section in ("requests", "attempts", "retries", "errors"):
            for op in sorted(agg["requests"]):
                out['datalake_storage_cost_report_retained_%s{component="%s",operation="%s"}'
                    % (section, comp, op)] = agg[section].get(op, 0)
        for field in ("read_bytes", "write_bytes", "copy_bytes"):
            out['datalake_storage_cost_report_retained_%s{component="%s"}' % (field, comp)] = agg[field]
        for kind, count in agg["unknown"].items():
            out['datalake_storage_cost_report_unknown_byte_operations{component="%s",kind="%s"}'
                % (comp, kind)] = count
        out['datalake_storage_cost_report_http_attempts_known{component="%s"}' % comp] = int(agg["http_observed"])
    return out


def render():
    with _SLOCK:
        snap = dict(_S)
    lines = ["datalake_s3_inventory_configured %d" % snap["configured"],
             "datalake_s3_inventory_success %d" % snap["success"]]
    if snap["bytes"] is not None:
        lines.append('datalake_s3_bucket_bytes{scope="%s"} %d'
                     % (SCOPE, snap["bytes"]))
        lines.append('datalake_s3_bucket_objects{scope="%s"} %d'
                     % (SCOPE, snap["objects"]))
        lines.append("datalake_s3_inventory_timestamp_seconds %d"
                     % snap["timestamp"])
    lines.extend("%s %d" % item for item in filesystem_metrics().items())
    lines.extend("%s %s" % item for item in operational_metrics().items())
    lines.extend("%s %s" % item for item in receiver_metrics().items())
    lines.extend("%s %s" % item for item in cost_policy_metrics().items())
    lines.extend("%s %s" % item for item in cost_report_metrics().items())
    return "".join(l + "\n" for l in lines)


class _H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        raw = render().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *a):
        pass


def main(argv):
    interval = max(ei("STORAGE_METRICS_INTERVAL_SEC", 3600), 1)
    # Listen inside the container; Compose exposes no host port (internal only).
    threading.Thread(target=lambda: ThreadingHTTPServer(("0.0.0.0", 9105), _H).serve_forever(),
                     daemon=True).start()
    stop = []
    signal.signal(signal.SIGTERM, lambda *a: stop.append(1))
    signal.signal(signal.SIGINT, lambda *a: stop.append(1))
    client = resolve_client()
    if client is None:  # File mode / S3 absent: disabled state, never touch S3
        while not stop:
            time.sleep(0.5)
        return 0
    with _SLOCK:
        _S["configured"] = 1
    bucket = e("S3_BUCKET")
    while not stop:
        refresh_once(client, bucket)
        for _ in range(interval * 2):
            if stop:
                break
            time.sleep(0.5)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
