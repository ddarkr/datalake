#!/usr/bin/env python3
"""Databroker -> SQLite outbox -> Greptime batch uploader (receive-only).

Pipeline: KUKSA Databroker subscribe_current_values -> one outbox row per
signal update (deterministic event_id over vehicle/path/source-time/epoch/
value, reused on retry) -> timer-driven batch INSERT to GreptimeDB over
HTTP SQL -> delete only acked rows in a single transaction.

Threads (stdlib only): one subscriber thread owns the broker loop and is
the single writer (own outbox connection); one uploader thread with its
own SQLite connection is the single uploader, timer-driven with in-tick
catch-up (keeps uploading while full batches ack) so quiet vehicles,
Databroker outages, and >50 rows/s fleets still drain; one health thread.
SIGTERM/SIGINT sets stop, the main thread then calls the public
VSSClient.disconnect() to unblock an idle blocking subscribe, joins both
threads, then does one bounded final drain (first failure or 30 s
deadline ends it).

Ack policy (fail-closed): any transport error, top-level code != 0,
per-output error, missing affected-rows, or affected != batch size keeps
every row; retry reuses the same event_id/event_time (event_id PRIMARY
KEY + INSERT OR IGNORE), so duplicates are impossible.

Timestamp policy: broker Datapoint.timestamp (datetime) converts with
integer timedelta math (no float loss), range-checked to signed int64 ns;
naive datetimes are UTC. None, non-datetime, or out-of-range stamps fall
back to collector receive time (now_ns). NaN/inf floats are rejected; ints
beyond float64-exact range are stored as exact text, never lossy float.

Provenance: dbc_*_commit and mapping_revision come only from the setup
manifest's top-level pins; an applied override nulls dbc_supplemental_commit
and pins dbc_override_* instead (a version string never lands in a commit
column). artifacts[].sha256 is content identity and is never read here,
so it can never leak into a commit column.

Stream-loss scope (explicit): Databroker is a current-state broker. On
reconnect we snapshot get_current_values and record the gap window
[last_recv, resnapshot]; intermediate changes inside the window are
unrecoverable by design. Counter vss_stream_gaps_total + log lines
expose every gap. Every sample carries a deterministic event_id over
(vehicle, path, source time, epoch, value): INSERT OR IGNORE dedupes
pending rows by primary key, and a persistent seen table (survives
ack-delete and restarts, bounded by time-prune) suppresses re-store of
already-acked samples. Same value at a different timestamp is a
different sample and keeps its own row; no raw frame ids exist here.

Value mapping: bool/int/float/str/datetime supported; None (no value) is
skipped; list/tuple/dict/set/bytes (VSS arrays etc.) are explicitly
unsupported -- counted, sampled in logs, never stored.

Health: GET /metrics on 0.0.0.0:9104 (fixed internal port; compose
publishes 127.0.0.1:9104:9104 for loopback-only host access)
exposes vss_outbox_pending_rows, vss_outbox_oldest_event_time_ns,
vss_stored/uploaded/upload_failures/stream_gaps/unsupported/snapshot_deduped.

Only stdlib at import time (kuksa_client is imported lazily in run() so unit
tests run without vehicle deps). Remote Greptime TLS is verified by default
(urllib default context); never disabled here.
"""

import base64
import hashlib
import http.server
import json
import math
import os
import re
import signal
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

_INT64_MIN = -(2 ** 63)
_INT64_MAX = 2 ** 63 - 1

TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SOURCE = "can"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_FLOAT_EXACT_INT = 2 ** 53  # ints beyond this lose precision as float64


class UnsupportedValue(Exception):
    pass


def e(name, default=""):
    return os.environ.get(name, default)


def now_ns():
    return time.time_ns()


def classify(value):
    """Return (num, text, boolean) triple; None means 'no value, skip row'."""
    if value is None:
        return None
    if isinstance(value, bool):
        return (None, None, int(value))
    if isinstance(value, int):
        # Silent float64 rounding of int64/uint64 is forbidden: exact ints
        # go numeric, oversized ones are preserved as exact text.
        if abs(value) <= _FLOAT_EXACT_INT:
            return (float(value), None, None)
        return (None, str(value), None)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise UnsupportedValue("nan/inf float")
        return (value, None, None)
    if isinstance(value, str):
        return (None, value, None)
    if isinstance(value, datetime):
        return (None, value.isoformat(), None)
    raise UnsupportedValue(f"{type(value).__name__}")


def dt_to_ns(dt):
    """Source datetime -> ns since epoch with integer timedelta math.

    Fallback policy (explicit): None, non-datetime, or out-of-range
    stamps carry no usable source time, so collector receive time
    (now_ns) is stored instead. Naive datetimes are assumed UTC.
    Out-of-range means unrepresentable as datetime arithmetic or outside
    signed int64 ns (Greptime TIMESTAMP(9) / outbox INTEGER range).
    """
    if isinstance(dt, datetime):
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = dt.astimezone(timezone.utc) - _EPOCH
            ns = ((delta.days * 86400 + delta.seconds) * 1_000_000_000
                  + delta.microseconds * 1000)
        except (OverflowError, ValueError):
            return now_ns()
        if ns < _INT64_MIN or ns > _INT64_MAX:
            return now_ns()
        return ns
    return now_ns()


def sql_escape(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def sql_bool(value):
    # outbox stores bools as 0/1 ints; Greptime BOOLEAN wants TRUE/FALSE.
    if value is None:
        return "NULL"
    return "TRUE" if value else "FALSE"


COLUMNS = ["event_time", "vehicle", "path", "source", "event_id",
           "decode_epoch", "value_num", "value_text", "value_bool", "unit",
           "vss_version", "vehicle_firmware", "dbc_primary_commit",
           "dbc_supplemental_commit", "dbc_override_version",
           "dbc_override_commit", "mapping_revision", "collector_version",
           "ingest_time"]


def render_insert(table, rows):
    """rows: list of dicts keyed by COLUMNS. Returns one INSERT statement."""
    if not TABLE_RE.fullmatch(table):
        raise ValueError(f"bad table name: {table}")
    cells = []
    for r in rows:
        cells.append(",".join(
            sql_bool(r.get(c)) if c == "value_bool" else sql_escape(r.get(c))
            for c in COLUMNS))
    return f"INSERT INTO {table} ({','.join(COLUMNS)}) VALUES {', '.join(f'({c})' for c in cells)}"

DDL = """CREATE TABLE IF NOT EXISTS outbox(
event_id TEXT PRIMARY KEY, event_time INTEGER NOT NULL,
vehicle TEXT NOT NULL, path TEXT NOT NULL, source TEXT NOT NULL,
decode_epoch TEXT NOT NULL, value_num REAL, value_text TEXT, value_bool INTEGER,
unit TEXT, vss_version TEXT, vehicle_firmware TEXT,
dbc_primary_commit TEXT, dbc_supplemental_commit TEXT,
dbc_override_version TEXT, dbc_override_commit TEXT,
mapping_revision TEXT, collector_version TEXT, ingest_time INTEGER NOT NULL)"""


SEEN_DDL = """CREATE TABLE IF NOT EXISTS seen_ids(
event_id TEXT PRIMARY KEY, seen_at INTEGER NOT NULL)"""


def open_outbox(path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute(DDL)
    conn.execute(SEEN_DDL)
    # Additive migration: existing outbox files keep queued rows; new
    # nullable provenance columns default to NULL on old rows.
    have = {r[1] for r in conn.execute("PRAGMA table_info(outbox)")}
    for col in ("dbc_override_version", "dbc_override_commit"):
        if col not in have:
            conn.execute(f"ALTER TABLE outbox ADD COLUMN {col} TEXT")
    # No secondary index: rowid ordering is implicit; CREATE INDEX ON
    # outbox(rowid) fails (rowid is not a column) and would crash boot.
    conn.commit()
    return conn


def store(conn, row):
    cols = ",".join(COLUMNS)
    conn.execute(
        f"INSERT OR IGNORE INTO outbox({cols})"
        f" VALUES({','.join('?' * len(COLUMNS))})",
        [row[c] for c in COLUMNS])
    conn.commit()


def deterministic_event_id(vehicle, path, event_time_ns, decode_epoch,
                           num, text, boolean):
    """Stable id for one logical sample: identical inputs (vehicle, path,
    source time, epoch, value) always yield the same id, so redeliveries,
    resnapshots, and restarts after acked rows can never duplicate.
    Same value at a different timestamp is a different sample by design
    and keeps its own row."""
    parts = [vehicle, path, SOURCE, str(event_time_ns), str(decode_epoch),
             repr(num), "" if text is None else str(text),
             "" if boolean is None else str(boolean)]
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def make_row(vehicle, path, event_time_ns, event_id, meta, units,
             num, text, boolean, ingest_time_ns):
    """Canonical outbox row. All identity/time inputs are caller-supplied
    so live (deterministic id from sample content) and offline re-encode
    share one constructor."""
    return {
        "event_time": event_time_ns, "vehicle": vehicle, "path": path,
        "source": SOURCE, "event_id": event_id,
        "decode_epoch": meta["decode_epoch"], "value_num": num,
        "value_text": text, "value_bool": boolean,
        "unit": units.get(path), "vss_version": meta["vss_version"],
        "vehicle_firmware": meta["vehicle_firmware"],
        "dbc_primary_commit": meta["dbc_primary_commit"],
        "dbc_supplemental_commit": meta.get("dbc_supplemental_commit"),
        "dbc_override_version": meta.get("dbc_override_version"),
        "dbc_override_commit": meta.get("dbc_override_commit"),
        "mapping_revision": meta["mapping_revision"],
        "collector_version": meta["collector_version"],
        "ingest_time": ingest_time_ns}


def store_update(conn, last, path, row):
    """Dedupe by deterministic event_id. Returns True when the row was
    new. event_id derives from (vehicle, path, source time, epoch,
    value), so redeliveries, resnapshots, and restarts after acked rows
    can never duplicate; the same value at a different timestamp is a
    different sample and keeps its own row. `last` is an in-memory
    same-tick shortcut only; seen_ids persists across ack-delete and
    restarts."""
    key = row["event_id"]
    if last.get(path) == key:
        return False
    seen = conn.execute("SELECT 1 FROM seen_ids WHERE event_id=?",
                        (key,)).fetchone()
    if seen is not None:
        last[path] = key
        return False
    cur = conn.execute(
        "INSERT OR IGNORE INTO outbox(" + ",".join(COLUMNS) + ")"
        " VALUES(" + ",".join("?" * len(COLUMNS)) + ")",
        [row[c] for c in COLUMNS])
    if cur.rowcount == 0:
        conn.execute("INSERT OR IGNORE INTO seen_ids(event_id,seen_at)"
                     " VALUES(?,?)", (key, now_ns()))
        conn.commit()
        last[path] = key
        return False
    conn.execute("INSERT OR IGNORE INTO seen_ids(event_id,seen_at)"
                 " VALUES(?,?)", (key, now_ns()))
    conn.commit()
    last[path] = key
    return True


def prune_seen(conn, older_than_ns, limit=5000):
    """Bound the persistent dedupe table; returns deleted rows. Time-based
    so only ids older than the retention window leave."""
    cur = conn.execute(
        "DELETE FROM seen_ids WHERE rowid IN (SELECT rowid FROM seen_ids"
        " WHERE seen_at < ? LIMIT ?)", (older_than_ns, limit))
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0

def load_manifest(path):
    with open(path) as f:
        m = json.load(f)
    def s(key):
        val = m.get(key, "")
        return val if isinstance(val, str) else str(val)
    def opt(key):
        # Absent key (legacy manifest) stays ""; explicit JSON null
        # (applied-override absence) stays NULL, never the string "None".
        if key not in m:
            return ""
        val = m[key]
        return None if val is None else (val if isinstance(val, str)
                                         else str(val))
    # Effective row metadata: setup nulls dbc_supplemental_commit and pins
    # dbc_override_* only when the override actually replaced supplemental.
    # Commit identity comes only from the top-level pins; artifacts[].sha256
    # is content identity and is never read here, so it can never leak into
    # a commit column (false provenance).
    return {
        "vehicle_firmware": s("vehicle_firmware"),
        "decode_epoch": s("decode_epoch"),
        "vss_version": s("vss_version"),
        "dbc_primary_commit": s("dbc_primary_commit"),
        "dbc_supplemental_commit": opt("dbc_supplemental_commit"),
        "dbc_override_version": opt("dbc_override_version"),
        "dbc_override_commit": opt("dbc_override_commit"),
        "mapping_revision": s("mapping_revision"),
        "collector_version": e("COLLECTOR_VERSION", "vss-recorder-1"),
    }


def mapped_paths(mapping_path):
    from vehicle_setup import find_mappings, iter_nodes
    with open(mapping_path) as f:
        mapping = json.load(f)
    refs = find_mappings(mapping.get("Vehicle", {}), "Vehicle")
    paths = list(dict.fromkeys(p for p, _s, k in refs if k == "dbc2vss"))
    path_set = set(paths)
    units = {}
    for path, node in iter_nodes(mapping.get("Vehicle", {}), "Vehicle"):
        if path in path_set and isinstance(node.get("unit"), str):
            units[path] = node["unit"]
    return paths, units


def greptime_insert(base_url, db, user, password, sql, timeout=15):
    """POST one batch; return the acked affected-row count.

    Fail-closed: transport errors, top-level code != 0, per-output
    errors, or a missing affected-rows field all raise -- the caller
    keeps every row for retry with the same event_id/event_time.
    """
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db)
    cred = base64.b64encode(f"{user}:{password}".encode()).decode()
    req = urllib.request.Request(
        url, data=urllib.parse.urlencode({"sql": sql}).encode(),
        headers={"Authorization": "Basic " + cred,
                 "Content-Type": "application/x-www-form-urlencoded"})
    # default context verifies remote TLS; never disable.
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode()
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise IOError(f"greptime bad response: {body[:200]}")
    if not isinstance(payload, dict) or payload.get("code", 0) != 0:
        raise IOError(f"greptime error: {body[:200]}")
    affected, seen = 0, False
    output = payload.get("output", [])
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            if item.get("error"):
                raise IOError(f"greptime output error: {str(item['error'])[:200]}")
            for key in ("affectedrows", "affected_rows"):
                if key in item:
                    affected += int(item[key])
                    seen = True
    if not seen:
        raise IOError(f"greptime response has no affected rows: {body[:200]}")
    return affected


def upload_once(conn, table, base_url, db, user, password, limit=500):
    cur = conn.execute(
        "SELECT rowid," + ",".join(COLUMNS) + " FROM outbox ORDER BY rowid LIMIT ?",
        (limit,))
    batch = cur.fetchall()
    if not batch:
        return 0
    rows = [dict(zip(COLUMNS, r[1:])) for r in batch]
    affected = greptime_insert(base_url, db, user, password,
                               render_insert(table, rows))
    if affected != len(rows):
        raise IOError(f"greptime partial ack: affected={affected} "
                      f"expected={len(rows)}; rows kept for retry")
    # ack: delete exactly the rows the server accepted, one transaction.
    with conn:
        conn.executemany("DELETE FROM outbox WHERE event_id=?",
                         [(r["event_id"],) for r in rows])
    return len(rows)


def render_metrics(stats, outbox_path):
    """Prometheus exposition for the outbox/uploader state."""
    try:
        conn = sqlite3.connect("file:" + urllib.parse.quote(os.path.abspath(outbox_path))
                               + "?mode=ro", uri=True, timeout=5)
        try:
            pending, oldest, received = conn.execute(
                "SELECT COUNT(*), MIN(event_time), MIN(ingest_time) FROM outbox").fetchone()
        finally:
            conn.close()
    except Exception:
        pending, oldest, received = -1, -1, None
    if oldest is None:
        oldest = 0

    def gauge(name, help_, value):
        return (f"# HELP {name} {help_}\n# TYPE {name} gauge\n"
                f"{name} {value}\n")

    def counter(name, help_, value):
        return (f"# HELP {name} {help_}\n# TYPE {name} counter\n"
                f"{name} {value}\n")

    ages = ""
    if pending >= 0:
        current = now_ns()
        ages = (gauge("vss_outbox_oldest_event_age_seconds",
                      "Original event age, not queue residence; seconds; 0 when empty.",
                      max(0, (current - oldest) / 1e9) if oldest else 0)
                + gauge("vss_outbox_oldest_enqueue_age_seconds",
                        "Age since local receipt/enqueue; seconds; 0 when empty.",
                        max(0, (current - received) / 1e9) if received else 0))
    if stats.get("last_receive_timestamp_seconds") is not None:
        ages += gauge("vss_last_receive_timestamp_seconds",
                      "Last locally received broker update/snapshot, including duplicates.",
                      stats["last_receive_timestamp_seconds"])

    return (
        gauge("vss_outbox_pending_rows",
              "Outbox rows buffered locally, not yet acked by Greptime.",
              pending)
        + gauge("vss_outbox_oldest_event_time_ns",
                "Oldest outbox event_time in ns; 0 when empty, -1 when unreadable.",
                oldest)
        + gauge("vss_outbox_metrics_success", "1 when outbox read succeeded, else 0.",
                int(pending >= 0))
        + ages
        + counter("vss_stored_rows_total", "Rows written to the outbox.",
                  stats.get("stored", 0))
        + counter("vss_uploaded_rows_total",
                  "Rows acked by Greptime and deleted.",
                  stats.get("uploaded", 0))
        + counter("vss_upload_failures_total",
                  "Upload attempts that failed; rows kept for retry.",
                  stats.get("upload_failures", 0))
        + counter("vss_stream_gaps_total",
                  "Databroker reconnects; values inside the gap window are lost by broker design.",
                  stats.get("gaps", 0))
        + counter("vss_unsupported_values_total",
                  "Signal updates dropped as unsupported types; never stored.",
                  stats.get("unsupported", 0))
        + counter("vss_snapshot_deduped_total",
                  "Redelivered samples deduped by deterministic event_id.",
                  stats.get("snap_deduped", 0))
    )


def make_health_handler(stats, lock, outbox_path):
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            with lock:
                snap = dict(stats)
            body = render_metrics(snap, outbox_path).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return HealthHandler


def upload_tick(conn, table, base_url, db, user, password, batch_n, stop):
    """One timer tick: keep uploading while full batches ack (catch-up),
    so arrival rates above batch_n/flush_sec drain instead of backlogging.
    First failure propagates; rows stay for the next tick with identical
    ids. Returns uploaded row count."""
    total = 0
    while not stop.is_set():
        n = upload_once(conn, table, base_url, db, user, password, batch_n)
        total += n
        if n < batch_n:
            break
    return total


def unblock_clients(clients):
    """Public-SDK unblock: closing the channel aborts an idle blocking
    subscribe iterator so shutdown/drain can proceed. Returns unblocked
    count; per-client errors never propagate."""
    n = 0
    for client in list(clients):
        try:
            client.disconnect()
            n += 1
        except Exception:
            pass
    return n


def uploader_loop(outbox_path, table, base_url, db, user, password,
                  batch_n, flush_sec, stats, lock, stop):
    """Single uploader on its own SQLite connection. Timer-driven, so
    quiet vehicles and Databroker outages still drain; each tick calls
    upload_tick (catch-up while full batches ack). First failure ends
    the tick; rows stay for the next tick with identical ids."""
    conn = open_outbox(outbox_path)
    try:
        while not stop.wait(flush_sec):
            try:
                total = upload_tick(conn, table, base_url, db, user,
                                    password, batch_n, stop)
                # 7-day retention on the persistent dedupe table, every
                # tick (also idle ones) so it cannot grow unbounded.
                prune_seen(conn, now_ns() - 7 * 86400 * 1_000_000_000)
            except Exception as ex:
                # network/timeout/partial failure: rows stay, same
                # event_id/event_time retried.
                with lock:
                    stats["upload_failures"] += 1
                print(f"vss_recorder: upload failed, will retry: {ex}")
            else:
                with lock:
                    stats["uploaded"] += total
    finally:
        conn.close()


def drain(conn, table, base_url, db, user, password, batch_n, stats, lock,
          deadline_s=30):
    """Bounded final flush on shutdown; first failure or deadline ends it."""
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        try:
            n = upload_once(conn, table, base_url, db, user, password,
                            batch_n)
        except Exception as ex:
            print(f"vss_recorder: final drain stopped: {ex}")
            break
        if n == 0:
            break
        with lock:
            stats["uploaded"] += n


def _positive_int(name, raw, default):
    try:
        val = int(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        print(f"vss_recorder: {name} must be a positive integer, got {raw!r}",
              file=sys.stderr)
        sys.exit(2)
    if val <= 0:
        print(f"vss_recorder: {name} must be > 0, got {val}",
              file=sys.stderr)
        sys.exit(2)
    return val


def _positive_float(name, raw, default):
    try:
        val = float(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        print(f"vss_recorder: {name} must be a positive number, got {raw!r}",
              file=sys.stderr)
        sys.exit(2)
    if not math.isfinite(val) or val <= 0:
        print(f"vss_recorder: {name} must be finite and > 0, got {val}",
              file=sys.stderr)
        sys.exit(2)
    return val


METRICS_PORT = 9104  # fixed internal port; compose publishes loopback only


def run():
    from kuksa_client.grpc import VSSClient  # real SDK, lazy so tests stay stdlib-only
    vehicle = e("VEHICLE_ID")
    if not vehicle:
        print("vss_recorder: VEHICLE_ID is required", file=sys.stderr)
        sys.exit(2)
    base_url, db, user, password = (e("GREPTIME_HTTP_URL"), e("GREPTIME_DB"),
                                    e("GREPTIME_USER"), e("GREPTIME_PASSWORD"))
    if not all([base_url, db, user, password]):
        print("vss_recorder: GREPTIME_HTTP_URL/DB/USER/PASSWORD required",
              file=sys.stderr)
        sys.exit(2)
    meta = load_manifest(e("MANIFEST_PATH", "/data/current/manifest.json"))
    paths, units = mapped_paths(e("MAPPING_PATH",
                                   "/data/current/mapping/vss_dbc.json"))
    if not paths:
        print("vss_recorder: no dbc2vss paths in mapping", file=sys.stderr)
        sys.exit(2)
    outbox_path = e("OUTBOX_PATH", "/data/outbox.sqlite")
    host = e("DATABROKER_HOST", "kuksa-databroker")
    port = _positive_int("DATABROKER_PORT", e("DATABROKER_PORT", "55555"),
                         55555)
    batch_n = _positive_int("VSS_BATCH_N", e("VSS_BATCH_N", "500"), 500)
    flush_sec = _positive_float("VSS_FLUSH_SEC", e("VSS_FLUSH_SEC", "10"),
                                10.0)
    stats = {"stored": 0, "uploaded": 0, "upload_failures": 0, "gaps": 0,
             "unsupported": 0, "snap_deduped": 0}
    lock = threading.Lock()
    # last event_id per path: same-tick shortcut for deterministic-id INSERT.
    last = {}
    stop = threading.Event()
    clients = []
    clients_lock = threading.Lock()

    def _stop(signum, frame):
        stop.set()
        with clients_lock:
            live = list(clients)
        # Public SDK API: closing the channel unblocks an idle blocking
        # subscribe iterator so shutdown/drain can proceed.
        unblock_clients(live)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    def handle(conn, path, dp, snapshot=False):
        received_ns = now_ns()
        with lock:
            stats["last_receive_timestamp_seconds"] = received_ns / 1e9
        if not snapshot:
            nonlocal_last_recv[0] = received_ns / 1e9
        try:
            got = classify(dp.value)
        except UnsupportedValue as ex:
            with lock:
                stats["unsupported"] += 1
                dropped = stats["unsupported"]
            if dropped <= 5:
                print(f"vss_recorder: unsupported value for {path}: {ex}")
            return
        if got is None:
            return
        num, text, boolean = got
        ets = dt_to_ns(dp.timestamp)
        row = make_row(vehicle, path, ets,
                       deterministic_event_id(
                           vehicle, path, ets, meta["decode_epoch"],
                           num, text, boolean),
                       meta, units, num, text, boolean, received_ns)
        if store_update(conn, last, path, row):
            with lock:
                stats["stored"] += 1
        else:
            with lock:
                stats["snap_deduped"] += 1

    nonlocal_last_recv = [0.0]

    def subscriber():
        conn = open_outbox(outbox_path)
        try:
            backoff = 5
            while not stop.is_set():
                gap_start = nonlocal_last_recv[0]
                client = VSSClient(host, port)
                with clients_lock:
                    clients.append(client)
                try:
                    client.connect()
                    snap = client.get_current_values(paths)
                    if stop.is_set():
                        return
                    if gap_start:
                        with lock:
                            stats["gaps"] += 1
                        print(f"vss_recorder: stream gap "
                              f"[{gap_start:.0f},{time.time():.0f}] resnapshotted "
                              f"(intermediate changes lost by broker design)")
                    for path, dp in snap.items():
                        if stop.is_set():
                            return
                        if dp is not None:
                            handle(conn, path, dp, snapshot=True)
                    backoff = 5
                    if stop.is_set():
                        return
                    for updates in client.subscribe_current_values(paths):
                        if stop.is_set():
                            return
                        for path, dp in updates.items():
                            if dp is not None:
                                handle(conn, path, dp)
                except Exception as ex:
                    if stop.is_set():
                        return
                    with lock:
                        snap_stats = dict(stats)
                    print(f"vss_recorder: databroker connection lost: {ex}; "
                          f"retry in {backoff}s (stored={snap_stats['stored']} "
                          f"uploaded={snap_stats['uploaded']} gaps={snap_stats['gaps']} "
                          f"unsupported={snap_stats['unsupported']})")
                    stop.wait(backoff)
                    backoff = min(backoff * 2, 60)
                finally:
                    with clients_lock:
                        if client in clients:
                            clients.remove(client)
                    try:
                        client.disconnect()
                    except Exception:
                        pass
        finally:
            conn.close()

    up = threading.Thread(
        target=uploader_loop,
        args=(outbox_path, "vehicle_signal", base_url, db, user, password,
              batch_n, flush_sec, stats, lock, stop),
        daemon=True)
    sub = threading.Thread(target=subscriber, daemon=True)
    up.start()
    sub.start()
    httpd = None
    ht = None
    try:
        httpd = http.server.ThreadingHTTPServer(
            ("0.0.0.0", METRICS_PORT),
            make_health_handler(stats, lock, outbox_path))
        ht = threading.Thread(target=httpd.serve_forever, daemon=True)
        ht.start()
        print(f"vss_recorder: subscribing to {len(paths)} paths as {vehicle} "
              f"(metrics :{METRICS_PORT}/metrics)")
        # Main thread only waits: SIGTERM/SIGINT sets stop and unblocks an
        # idle blocking subscribe via client.disconnect().
        while not stop.is_set():
            stop.wait(1.0)
    finally:
        # graceful SIGTERM even when idle: stop timer + unblock subscribe
        # first (single writer/uploader end), then one bounded drain on a
        # fresh connection, then close.
        stop.set()
        _stop(None, None)
        sub.join(timeout=30)
        up.join(timeout=flush_sec + 10)
        drain_conn = open_outbox(outbox_path)
        try:
            drain(drain_conn, "vehicle_signal", base_url, db, user,
                  password, batch_n, stats, lock)
        finally:
            drain_conn.close()
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        if ht is not None:
            ht.join(timeout=10)
        print(f"vss_recorder: stopped (stored={stats['stored']} "
              f"uploaded={stats['uploaded']} gaps={stats['gaps']} "
              f"unsupported={stats['unsupported']})")
if __name__ == "__main__":
    run()
