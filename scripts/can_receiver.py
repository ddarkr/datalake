"""Authenticated OTLP raw archive and resumable CAN-to-Greptime worker.

A successful /v1/logs response acknowledges SQLite raw durability, not Greptime.
Raw chunks are never deleted. Run with ``python /app/can_receiver.py serve|status|re-decode``.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import fcntl
import hmac
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.error
import urllib.parse
import urllib.request
import zlib

from can_decoder import Decoder
from can_otlp_wire import MAX_REQUEST_BYTES, decode_batch, success_response

COLUMNS = (
    "event_time", "vehicle", "path", "source", "event_id", "decode_epoch",
    "value_num", "value_text", "value_bool", "unit", "vss_version",
    "vehicle_firmware", "dbc_primary_commit", "dbc_supplemental_commit",
    "dbc_override_version", "dbc_override_commit", "mapping_revision",
    "collector_version", "ingest_time", "source_system", "source_field",
    "collector_id", "source_is_resend", "quality", "envelope_id",
    "config_version", "connectivity",
)
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
CREATE TABLE IF NOT EXISTS outbox (
 id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
 epoch TEXT NOT NULL REFERENCES epochs(epoch), row_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS worker_errors (
 kind TEXT PRIMARY KEY, count INTEGER NOT NULL, last_ns INTEGER NOT NULL);
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
    script_directory = Path(__file__).resolve().parent
    workspace = script_directory.parent if script_directory.name == "scripts" else script_directory
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
            conn.commit()

    def decode_once(self, decoder):
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            raw = conn.execute(
                "SELECT c.*,s.meta_json,d.state_json,d.counts_json,d.rows FROM raw_chunks c "
                "JOIN sessions s ON s.id=c.session LEFT JOIN decode_states d "
                "ON d.session=c.session AND d.epoch=? WHERE c.seq=COALESCE(d.next_seq,0) "
                "ORDER BY c.id LIMIT 1", (decoder.epoch,),
            ).fetchone()
            if raw is None:
                return False
            self.reserve()
            meta = json.loads(raw["meta_json"])
            chunk = {key: raw[key] for key in ("seq", "offset_ns", "phase", "data")}
            state = json.loads(raw["state_json"]) if raw["state_json"] else None
            rows, state, counts = decoder.decode(meta, chunk, state)
            totals = json.loads(raw["counts_json"]) if raw["counts_json"] else {}
            for key, value in counts.items():
                if type(value) is not int or value < 0:
                    raise ValueError("invalid decoder count")
                totals[key] = value if key == "tail_bytes" else totals.get(key, 0) + value
            for row in rows:
                validate_row(row)
                if row["vehicle"] != meta["vehicle"] or row["collector_id"] != meta["collector_id"] or row["decode_epoch"] != decoder.epoch:
                    raise ValueError("decoder row identity mismatch")
                conn.execute("INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)", (row["event_id"], decoder.epoch, _json(row)))
            conn.execute(
                "INSERT INTO decode_states VALUES(?,?,?,?,?,?) ON CONFLICT(session,epoch) DO UPDATE SET "
                "next_seq=excluded.next_seq,state_json=excluded.state_json,counts_json=excluded.counts_json,rows=excluded.rows",
                (raw["session"], decoder.epoch, raw["seq"] + 1, _json(state), _json(totals), (raw["rows"] or 0) + len(rows)),
            )
            conn.commit()
            return True

    def record_error(self, kind):
        if kind not in {"decode_failure", "greptime_failure", "greptime_partial_ack", "archive_disk_reserve"}:
            raise ValueError("invalid error category")
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO worker_errors VALUES(?,1,?) ON CONFLICT(kind) DO UPDATE SET count=count+1,last_ns=excluded.last_ns",
                (kind, time.time_ns()),
            )
            conn.commit()

    def flush_once(self, greptime, limit=500):
        with self.connect() as conn:
            batch = conn.execute("SELECT id,row_json FROM outbox ORDER BY id LIMIT ?", (limit,)).fetchall()
        if not batch:
            return 0
        rows = [json.loads(record["row_json"]) for record in batch]
        if greptime.insert(rows) != len(rows):
            raise DownstreamError("greptime_partial_ack")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany("DELETE FROM outbox WHERE id=?", [(r["id"],) for r in batch])
            conn.commit()
        return len(rows)

    def status(self):
        return archive_status(self.path)


def archive_status(path):
    path = _archive_path(path)
    uri = path.as_uri() + "?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=5)) as conn:
        conn.execute("BEGIN")
        raw, raw_bytes = conn.execute("SELECT COUNT(*),COALESCE(SUM(length(data)),0) FROM raw_chunks").fetchone()
        sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        epochs = []
        for (epoch,) in conn.execute("SELECT epoch FROM epochs ORDER BY rowid"):
            processed, rows = conn.execute("SELECT COALESCE(SUM(next_seq),0),COALESCE(SUM(rows),0) FROM decode_states WHERE epoch=?", (epoch,)).fetchone()
            totals = {}
            for (encoded,) in conn.execute("SELECT counts_json FROM decode_states WHERE epoch=?", (epoch,)):
                for key, value in json.loads(encoded).items():
                    totals[key] = totals.get(key, 0) + value
            epochs.append({"epoch": epoch, "processed_chunks": processed, "remaining_chunks": raw - processed, "decoded_rows": rows, "counts": totals})
        errors = {kind: {"count": count, "last_ns": last} for kind, count, last in conn.execute("SELECT kind,count,last_ns FROM worker_errors")}
        return {"sessions": sessions, "raw_chunks": raw, "raw_bytes": raw_bytes, "pending_rows": pending, "epochs": epochs, "errors": errors}


def validate_row(row):
    if not isinstance(row, dict) or set(row) != set(COLUMNS):
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


def render_insert(rows):
    """Fixed table/columns, exact integer nanoseconds, standard SQL literals."""
    def literal(field, value):
        if value is None:
            return "NULL"
        if field in {"event_time", "ingest_time"}:
            return f"CAST({value} AS TIMESTAMP(9))"
        if type(value) is bool:
            return "TRUE" if value else "FALSE"
        if type(value) in (int, float):
            return repr(value)
        return "'" + value.replace("'", "''") + "'"
    cells = []
    for row in rows:
        validate_row(row)
        cells.append("(" + ",".join(literal(c, row[c]) for c in COLUMNS) + ")")
    if not cells:
        raise ValueError("cannot insert empty rows")
    return 'INSERT INTO "vehicle_signal" (' + ",".join('"' + c + '"' for c in COLUMNS) + ") VALUES " + ",".join(cells)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Greptime:
    def __init__(self, base_url, database, user, password, timeout=15):
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
        self.url = base_url.rstrip("/") + "/v1/sql?" + urllib.parse.urlencode({"db": database})
        self.authorization = "Basic " + base64.b64encode((user + ":" + password).encode()).decode()
        self.timeout = timeout
        self.opener = urllib.request.build_opener(_NoRedirect())

    def insert(self, rows):
        request = urllib.request.Request(self.url, data=urllib.parse.urlencode({"sql": render_insert(rows)}).encode(), headers={
            "Authorization": self.authorization, "Content-Type": "application/x-www-form-urlencoded",
        })
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                body = response.read(65537)
                if response.status != 200 or len(body) > 65536:
                    raise DownstreamError("greptime_failure")
            payload = json.loads(body)
            if not isinstance(payload, dict) or type(payload.get("code", 0)) is not int or payload.get("code", 0) != 0:
                raise DownstreamError("greptime_failure")
            output = payload.get("output")
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
        except (urllib.error.URLError, OSError, ValueError, TypeError):
            raise DownstreamError("greptime_failure") from None


class Worker(threading.Thread):
    def __init__(self, archive, decoder, greptime, interval=1, outbox_limit=500):
        super().__init__(name="can-archive-worker", daemon=True)
        self.archive, self.decoder, self.greptime = archive, decoder, greptime
        self.interval, self.outbox_limit = interval, outbox_limit
        self.stop_event = threading.Event()

    def _error(self, kind):
        try:
            self.archive.record_error(kind)
        except (sqlite3.Error, OSError):
            pass  # Storage failure must not kill the receiver or disclose raw data.

    def run(self):
        while not self.stop_event.is_set():
            progressed = False
            failed = False
            try:
                # ponytail: one ordered worker; parallelize only if decode throughput needs it.
                for _ in range(16):
                    if self.stop_event.is_set() or not self.archive.decode_once(self.decoder):
                        break
                    progressed = True
            except Rejection:
                failed = True
                self._error("archive_disk_reserve")
            except Exception:
                failed = True
                self._error("decode_failure")
            try:
                progressed = bool(self.archive.flush_once(self.greptime, self.outbox_limit)) or progressed
            except DownstreamError as exc:
                failed = True
                self._error(str(exc))
            except Exception:
                failed = True
                self._error("greptime_failure")
            if failed or not progressed:
                self.stop_event.wait(self.interval)


class Receiver(HTTPServer):
    request_queue_size = 16

    def __init__(self, address, archive, vehicle, collector_id, user, password, timeout=15):
        if not user or not password or ":" in user:
            raise ConfigurationError("can_otlp_credentials_required")
        self.archive = archive
        self.vehicle, self.collector_id = _identity(vehicle), _identity(collector_id)
        with archive.connect() as conn:
            if conn.execute("SELECT 1 FROM sessions WHERE vehicle<>? OR collector_id<>? LIMIT 1", (self.vehicle, self.collector_id)).fetchone():
                raise ConfigurationError("archive_identity_conflicts_with_configuration")
        self.expected_auth = (user + ":" + password).encode()
        self.read_timeout = timeout
        super().__init__(address, Handler)

    def finish_request(self, request, client_address):
        # A whole-request deadline also bounds clients trickling headers/body.
        def expire():
            try:
                request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        deadline = threading.Timer(self.read_timeout, expire)
        deadline.daemon = True
        deadline.start()
        try:
            super().finish_request(request, client_address)
        finally:
            deadline.cancel()

    def handle_error(self, request, client_address):
        pass  # Never let stdlib traceback logging include client-controlled data.


class Handler(BaseHTTPRequestHandler):
    server_version = "CANArchive"
    sys_version = ""

    def setup(self):
        self.request.settimeout(self.server.read_timeout)
        super().setup()

    def log_message(self, *args):
        pass

    def send_error(self, code, message=None, explain=None):
        self.reply(code, b'{"error":"invalid_http_request"}')

    def reply(self, code, body, protobuf=False):
        self.close_connection = True
        self.send_response(code)
        self.send_header("Content-Type", "application/x-protobuf" if protobuf else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
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
                raise Rejection(400, "truncated_request")
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
    serve.add_argument("--outbox-limit", type=int, default=500)
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            print(_json(archive_status(args.database)))
            return 0
        os.umask(0o077)
        if args.disk_reserve_mib < 0:
            raise ConfigurationError("disk_reserve_must_be_nonnegative")
        if args.command == "serve":
            if not 0 <= args.port <= 65535 or not math.isfinite(args.worker_interval) or args.worker_interval <= 0 or not math.isfinite(args.http_timeout) or args.http_timeout <= 0 or not 1 <= args.outbox_limit <= 500:
                raise ConfigurationError("invalid_receiver_limits")
            otlp_user, otlp_password = _credentials("CAN_OTLP")
            db_user, db_password = _credentials("GREPTIME")
            greptime = Greptime(args.greptime_url, args.greptime_db, db_user, db_password, args.http_timeout)
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
            worker = Worker(archive, decoder, greptime, args.worker_interval, args.outbox_limit)
            worker.start()
            print(_json({"receiver": "ready", "port": server.server_port}), flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
                worker.stop_event.set()
                worker.join()
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
