"""Durable original-file receive store and worker-only file importer.

Serve role stores exact Pi original bytes (capture.json snapshot plus
serial_chunks.jsonl / events.jsonl append streams) with offset/hash ACKs.
The receive path never parses JSON, base64, protobuf, or gzip: it only
appends bytes at the committed offset and fsyncs payload, offset state, and
directories before acknowledging.

Worker-only FileImporter derives the same files root from the archive path,
parses complete serial JSONL lines with the old Go uploader's strictness
(exact decimals, contiguous seq, bounded base64), and feeds Archive.accept
with the same meta/session/seq identifiers so historical OTLP chunks dedup.
Importer cursors advance only after Archive.accept succeeds; replays are
harmless. Never construct FileImporter in the serve role.
"""

from __future__ import annotations

import base64
import calendar
import datetime
import errno
import hashlib
import json
import os
import re
import stat
import threading
import time
import unicodedata
from decimal import Decimal
from pathlib import Path

FILES = ("capture.json", "serial_chunks.jsonl", "events.jsonl")
CAPTURE_FILE = "capture.json"
SERIAL_FILE = "serial_chunks.jsonl"
MAX_FILE_RANGE = 4 * 1024 * 1024
MAX_CAPTURE_BYTES = 64 * 1024
TAIL_HASH_BYTES = 65536
MAX_LINE_BYTES = 128 * 1024
MAX_CHUNK_BYTES = 65536
MAX_ENCODED_BYTES = 4 * ((MAX_CHUNK_BYTES + 2) // 3)
INT64_MAX = 2 ** 63 - 1
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
MAX_OFFSET_DEC = Decimal("9223372036.854775807")
_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_UTC = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})"
    r"(?:\.(\d{1,9}))?(Z|\+00:00)\Z"
)

_FILE_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_import_state(
 vehicle TEXT NOT NULL, collector_id TEXT NOT NULL, session_id TEXT NOT NULL,
 meta_json TEXT NOT NULL, next_seq INTEGER NOT NULL, last_t TEXT NOT NULL,
 file_offset INTEGER NOT NULL, updated_ns INTEGER NOT NULL,
 PRIMARY KEY(vehicle,collector_id,session_id));
CREATE TABLE IF NOT EXISTS file_import_errors(
 vehicle TEXT NOT NULL, collector_id TEXT NOT NULL, session_id TEXT NOT NULL,
 file TEXT NOT NULL, error TEXT NOT NULL, count INTEGER NOT NULL,
 last_ns INTEGER NOT NULL, fail_offset INTEGER NOT NULL,
 PRIMARY KEY(vehicle,collector_id,session_id,file));
"""

# Deterministic failures: same cursor offset can never succeed, so later
# turns skip the session without re-reading it (bytes are retained).
# hash_mismatch is terminal for one metadata version (bytes never change
# under a committed offset); a corrected capture snapshot invalidates the
# verifier and clears the error via _clear_error on the ready parse.
# archive_truncated is transient (a resumed upload may still grow the file).
_DETERMINISTIC_ERRORS = frozenset({
    "invalid_capture", "invalid_chunk", "line_too_long",
    "identity_conflict", "archive_conflict", "hash_mismatch",
})


class FileRejection(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason
        super().__init__(reason)


class FileConfigurationError(ValueError):
    """Static config codes; the receiver entry point translates these."""


class _SourceInvalid(ValueError):
    """One sanitized importer failure code; bytes are always retained."""


def _no_dup(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("duplicate key")
        obj[key] = value
    return obj


def _no_const(value):
    raise ValueError("non-finite constant")


def _check_strings(value):
    """Reject lone surrogates in decoded strings (old Go uploader rule).

    Scans decoded str objects instead of raw backslash escapes, so an
    escaped literal backslash (for example the valid JSON "\\\\ud800")
    is never mistaken for a surrogate escape.
    """
    stack = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        if depth > 256:
            raise ValueError("excessive nesting")
        if type(item) is str:
            for char in item:
                if 0xD800 <= ord(char) <= 0xDFFF:
                    raise ValueError("lone surrogate")
        elif type(item) is dict:
            for key, entry in item.items():
                stack.append((key, depth + 1))
                stack.append((entry, depth + 1))
        elif type(item) is list:
            for entry in item:
                stack.append((entry, depth + 1))


def _strict_loads(data):
    """Strict JSON: UTF-8, duplicate keys, depth, exact int/Decimal numbers."""
    if type(data) is not bytes or not data:
        raise ValueError("empty document")
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("invalid encoding") from None
    try:
        parsed = json.loads(data, object_pairs_hook=_no_dup,
                            parse_int=int, parse_float=Decimal,
                            parse_constant=_no_const)
    except RecursionError:
        raise ValueError("excessive nesting") from None
    _check_strings(parsed)
    return parsed

def _decimal_value(value):
    if type(value) is bool:
        raise ValueError("non-numeric timestamp")
    if type(value) is int:
        return Decimal(value)
    if type(value) is Decimal:
        return value
    raise ValueError("non-numeric timestamp")


def _decimal_nanos(value):
    """Truncate an exact JSON decimal toward zero to integer nanoseconds.

    Digit slicing only: hostile exponents (1e-100000000) never allocate.
    Only the first 19 integer digits can affect the bounded result; a
    decimal position at or below zero (or a zero coefficient) is exactly
    zero nanoseconds, preserving the old Go negative-zero acceptance.
    """
    number = _decimal_value(value)
    if number.is_signed() and number != 0:
        raise ValueError("negative timestamp")
    if number.copy_abs() > MAX_OFFSET_DEC:
        raise ValueError("timestamp out of range")
    if type(value) is int:
        return value * 1_000_000_000
    tup = number.as_tuple()
    digits = "".join(map(str, tup.digits)).lstrip("0")
    if not digits:
        return 0
    position = len(digits) + tup.exponent + 9
    if position <= 0:
        return 0
    if position > 19:
        raise ValueError("timestamp out of range")
    head = digits[:position]
    return int(head + "0" * (position - len(head)))


def _parse_started_utc(value):
    """Exact RFC3339 nanoseconds; datetime alone loses digits 7-9."""
    if type(value) is not str:
        raise ValueError("invalid start timestamp")
    match = _UTC.fullmatch(value)
    if not match:
        raise ValueError("invalid start timestamp")
    year, mon, day, hour, minute, sec = (int(match.group(i)) for i in range(1, 7))
    try:
        datetime.datetime(year, mon, day, hour, minute, sec)
    except ValueError:
        raise ValueError("invalid start timestamp") from None
    seconds = calendar.timegm((year, mon, day, hour, minute, sec, 0, 0, 0))
    fraction = match.group(7) or ""
    nanos = seconds * 1_000_000_000 + int((fraction + "0" * 9)[:9])
    if seconds < 0 or seconds > INT64_MAX // 1_000_000_000 or nanos <= 0 or nanos > INT64_MAX:
        raise ValueError("invalid start timestamp")
    return nanos


def _firmware_ok(value):
    """Go metadata rule: printable runes, only ASCII space may separate."""
    if type(value) is not str or len(value) > 160:
        return False
    for char in value:
        if char == " ":
            continue
        if unicodedata.category(char).startswith("Z") or not char.isprintable():
            return False
    return True


def _parse_capture(data):
    """Split capture.json into importer meta; null/absent start is not-ready."""
    if type(data) is not bytes or len(data) > MAX_CAPTURE_BYTES:
        raise _SourceInvalid("invalid_capture")
    try:
        capture = _strict_loads(data)
    except ValueError:
        raise _SourceInvalid("invalid_capture") from None
    if type(capture) is not dict or type(capture.get("schema_version")) is not int \
            or capture["schema_version"] != 1:
        raise _SourceInvalid("invalid_capture")
    if "started_utc" not in capture or capture["started_utc"] is None:
        return {"ready": False}
    try:
        started_ns = _parse_started_utc(capture["started_utc"])
    except ValueError:
        raise _SourceInvalid("invalid_capture") from None
    if "vehicle_firmware" not in capture:
        firmware = ""
    elif not _firmware_ok(capture["vehicle_firmware"]):
        raise _SourceInvalid("invalid_capture")
    else:
        firmware = capture["vehicle_firmware"]
    raw_hash = capture.get("raw_file_sha256")
    if raw_hash is not None and (
            type(raw_hash) is not str or not re.fullmatch(r"[0-9a-f]{64}", raw_hash)):
        raise _SourceInvalid("invalid_capture")
    declared = capture.get("chunks")
    if declared is not None and (type(declared) is not int or declared < 0):
        raise _SourceInvalid("invalid_capture")
    return {"ready": True, "started_ns": started_ns,
            "firmware": firmware, "raw_hash": raw_hash,
            "chunks": declared, "complete": capture.get("complete")}


def _parse_chunk(line, seq, previous, started_ns):
    """Strict serial line matching the old Go uploader; exact decimal first."""
    if len(line) > MAX_LINE_BYTES:
        raise _SourceInvalid("line_too_long")
    try:
        record = _strict_loads(line)
    except ValueError:
        raise _SourceInvalid("invalid_chunk") from None
    if type(record) is not dict \
            or set(record) != {"seq", "t_s", "phase", "data_b64"}:
        raise _SourceInvalid("invalid_chunk")
    if type(record["seq"]) is not int or record["seq"] != seq \
            or seq < 0 or seq == INT64_MAX:
        raise _SourceInvalid("invalid_chunk")
    try:
        stamp = _decimal_value(record["t_s"])
        if previous is not None and stamp < previous:
            raise _SourceInvalid("invalid_chunk")
        offset_ns = _decimal_nanos(record["t_s"])
    except ValueError:
        raise _SourceInvalid("invalid_chunk") from None
    if started_ns > INT64_MAX - offset_ns:
        raise _SourceInvalid("invalid_chunk")
    if record["phase"] not in ("capture", "close_drain"):
        raise _SourceInvalid("invalid_chunk")
    encoded = record["data_b64"]
    if type(encoded) is not str or len(encoded) > MAX_ENCODED_BYTES \
            or "\r" in encoded or "\n" in encoded:
        raise _SourceInvalid("invalid_chunk")
    try:
        data = base64.b64decode(encoded.encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError):
        raise _SourceInvalid("invalid_chunk") from None
    if len(data) > MAX_CHUNK_BYTES:
        raise _SourceInvalid("invalid_chunk")
    return {"seq": seq, "offset_ns": offset_ns,
            "phase": record["phase"], "data": data}, stamp


def _check_names(session_id, filename):
    if type(session_id) is not str or not _IDENTITY.fullmatch(session_id):
        raise FileRejection(404, "not_found")
    if filename not in FILES:
        raise FileRejection(404, "not_found")


def _ack(session_id, filename, offset, tail_sha256):
    return {"version": 1, "session_id": session_id, "file": filename,
            "offset": offset, "tail_sha256": tail_sha256}


def _fsync_dir(path):
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        raise FileRejection(503, "archive_unavailable") from None
    try:
        os.fsync(fd)
    except OSError:
        raise FileRejection(503, "archive_unavailable") from None
    finally:
        os.close(fd)


def _private_dir(path, config_time):
    """Create private directories and durably publish their parent entries."""
    try:
        if path.is_symlink() or path.parent.is_symlink():
            raise FileRejection(404, "not_found")
        path.mkdir(mode=0o700, exist_ok=True)
        info = path.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise FileRejection(503, "archive_unavailable")
        _fsync_dir(path.parent)
    except (OSError, FileRejection) as exc:
        if config_time:
            raise FileConfigurationError("archive_directory_requires_private_0700_and_ownership") from None
        if isinstance(exc, FileRejection):
            raise
        raise FileRejection(503, "archive_unavailable") from None


def _open_nofollow(path, write=False, create=False):
    flags = (os.O_WRONLY if write else os.O_RDONLY) | os.O_NOFOLLOW | os.O_NONBLOCK
    if create:
        flags |= os.O_CREAT
    try:
        fd = os.open(str(path), flags, 0o600)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise FileRejection(404, "not_found")
        return fd
    except FileNotFoundError:
        raise FileRejection(503 if write else 404,
                            "archive_unavailable" if write else "not_found") from None
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise FileRejection(404, "not_found") from None
        raise FileRejection(503, "archive_unavailable") from None


class FileStore:
    """Receive-side exact-byte store; available as ``receiver.files``."""

    def __init__(self, archive, vehicle, collector_id):
        if (type(vehicle) is not str or not _IDENTITY.fullmatch(vehicle)
                or type(collector_id) is not str
                or not _IDENTITY.fullmatch(collector_id)):
            raise FileConfigurationError("invalid_configured_identity")
        self._archive = archive
        self.vehicle, self.collector_id = vehicle, collector_id
        self.root = Path(archive.path).parent / "files"
        # ponytail: one store-wide lock; per-file locks only if contended.
        self._lock = threading.RLock()
        _private_dir(self.root, True)
        _private_dir(self.root / vehicle, True)
        _private_dir(self.root / vehicle / collector_id, True)

    def _namespace(self, session_id, create):
        base = self.root / self.vehicle / self.collector_id / session_id
        for level in (self.root, base.parent.parent, base.parent, base):
            if level.is_symlink():
                raise FileRejection(404, "not_found")
        if create:
            _private_dir(base, False)
        elif not base.is_dir():
            return None
        return base

    def path(self, session_id, filename):
        """Exact on-disk path for integration/tests; validated, never created."""
        _check_names(session_id, filename)
        return self.root / self.vehicle / self.collector_id / session_id / filename

    def _read_exact(self, path, limit):
        """Bounded O_NOFOLLOW read; symlinks refuse as 404, never followed."""
        fd = _open_nofollow(path)
        try:
            parts, total = [], 0
            while True:
                piece = os.read(fd, min(65536, limit + 1 - total))
                if not piece:
                    break
                parts.append(piece)
                total += len(piece)
                if total > limit:
                    raise FileRejection(413, "request_too_large")
            return b"".join(parts)
        finally:
            os.close(fd)

    def _read_committed(self, sidecar):
        if sidecar.is_symlink():
            raise FileRejection(404, "not_found")
        try:
            raw = self._read_exact(sidecar, 32)
        except FileRejection as exc:
            if exc.status == 404:
                return 0
            raise
        if not re.fullmatch(rb"[0-9]{1,19}", raw):
            raise FileRejection(503, "archive_unavailable")
        return int(raw)

    def _recover(self, data, sidecar):
        """Crash recovery: truncate any uncommitted append tail."""
        if data.is_symlink():
            raise FileRejection(404, "not_found")
        committed = self._read_committed(sidecar)
        try:
            fd = _open_nofollow(data)
        except FileRejection as exc:
            if exc.status == 404 and committed == 0 and not data.exists():
                return 0
            raise
        try:
            size = os.fstat(fd).st_size
        finally:
            os.close(fd)
        if size < committed:
            raise FileRejection(503, "archive_unavailable")
        if size > committed:
            fd = _open_nofollow(data, write=True)
            try:
                os.ftruncate(fd, committed)
                os.fsync(fd)
            except OSError:
                raise FileRejection(503, "archive_unavailable") from None
            finally:
                os.close(fd)
        return committed

    def _tail_hex(self, data, committed):
        if committed == 0:
            return EMPTY_SHA256
        fd = _open_nofollow(data)
        try:
            start = max(0, committed - TAIL_HASH_BYTES)
            parts, offset = [], start
            while offset < committed:
                piece = os.pread(fd, min(65536, committed - offset), offset)
                if not piece:
                    raise FileRejection(503, "archive_unavailable")
                parts.append(piece)
                offset += len(piece)
        finally:
            os.close(fd)
        return hashlib.sha256(b"".join(parts)).hexdigest()

    def _reserve(self, additional):
        try:
            self._archive.reserve(additional)
        except Exception as exc:
            if getattr(exc, "status", None) == 503 \
                    and getattr(exc, "reason", None) == "archive_disk_reserve":
                raise FileRejection(507, "archive_disk_reserve") from None
            raise FileRejection(503, "archive_unavailable") from None

    def _status_capture(self, session_id, directory, data):
        # The renamed capture file is the sole commit: no offset sidecar,
        # so a crash can never pair a newer larger body with an older size.
        if data.is_symlink():
            raise FileRejection(404, "not_found")
        try:
            body = self._read_exact(data, MAX_CAPTURE_BYTES)
        except FileRejection as exc:
            if exc.status == 404:
                return _ack(session_id, CAPTURE_FILE, 0, EMPTY_SHA256)
            raise
        return _ack(session_id, CAPTURE_FILE, len(body),
                    hashlib.sha256(body).hexdigest())

    def status(self, session_id, filename):
        _check_names(session_id, filename)
        with self._lock:
            directory = self._namespace(session_id, False)
            if directory is None:
                return _ack(session_id, filename, 0, EMPTY_SHA256)
            data = directory / filename
            if filename == CAPTURE_FILE:
                return self._status_capture(session_id, directory, data)
            committed = self._recover(data, directory / (filename + ".committed"))
            return _ack(session_id, filename, committed,
                        self._tail_hex(data, committed))

    def put(self, session_id, filename, offset, digest, body):
        _check_names(session_id, filename)
        if type(offset) is not int or offset < 0:
            raise FileRejection(400, "invalid_file_offset")
        if type(digest) is not str or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise FileRejection(400, "invalid_file_hash")
        if type(body) is not bytes:
            raise FileRejection(400, "invalid_file_body")
        if len(body) > (MAX_CAPTURE_BYTES if filename == CAPTURE_FILE else MAX_FILE_RANGE):
            raise FileRejection(413, "request_too_large")
        if hashlib.sha256(body).hexdigest() != digest:
            raise FileRejection(409, "sha_mismatch")
        with self._lock:
            directory = self._namespace(session_id, True)
            data = directory / filename
            if filename == CAPTURE_FILE:
                return self._put_capture(session_id, directory, data, offset, body)
            sidecar = directory / (filename + ".committed")
            committed = self._recover(data, sidecar)
            if not body:
                if offset != committed:
                    raise FileRejection(409, "offset_conflict")
                if committed == 0:
                    self._reserve(0)
                    fd = _open_nofollow(data, write=True, create=True)
                    try:
                        os.fchmod(fd, 0o600)
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    self._write_atomic(directory, sidecar, b"0")
                return _ack(session_id, filename, committed,
                            self._tail_hex(data, committed))
            if offset != committed:
                if offset + len(body) > committed:
                    raise FileRejection(409, "offset_conflict")
                try:
                    fd = _open_nofollow(data)
                except FileRejection as exc:
                    if exc.status == 404:
                        raise FileRejection(409, "offset_conflict") from None
                    raise
                try:
                    retained = os.pread(fd, len(body), offset)
                finally:
                    os.close(fd)
                if retained != body:
                    raise FileRejection(409, "chunk_conflict")
                return _ack(session_id, filename, committed,
                            self._tail_hex(data, committed))
            self._reserve(len(body))
            fd = _open_nofollow(data, write=True, create=True)
            try:
                if os.fstat(fd).st_size != committed:
                    raise FileRejection(503, "archive_unavailable")
                os.fchmod(fd, 0o600)
                os.lseek(fd, offset, os.SEEK_SET)
                view = memoryview(body)
                while view:
                    written = os.write(fd, view)
                    view = view[written:]
                os.fsync(fd)
            except OSError:
                raise FileRejection(503, "archive_unavailable") from None
            finally:
                os.close(fd)
            committed += len(body)
            self._write_atomic(directory, sidecar, str(committed).encode("ascii"))
            return _ack(session_id, filename, committed,
                        self._tail_hex(data, committed))

    def _write_atomic(self, directory, target, body):
        """Stage + fsync + rename + dirsync; torn files are never committed."""
        if target.is_symlink():
            raise FileRejection(404, "not_found")
        tmp = target.with_name(target.name + ".tmp")
        try:
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC
                         | os.O_NOFOLLOW, 0o600)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise FileRejection(404, "not_found") from None
            raise FileRejection(503, "archive_unavailable") from None
        try:
            os.fchmod(fd, 0o600)
            view = memoryview(body)
            while view:
                written = os.write(fd, view)
                view = view[written:]
            os.fsync(fd)
        except OSError:
            raise FileRejection(503, "archive_unavailable") from None
        finally:
            os.close(fd)
        try:
            os.rename(str(tmp), str(target))
        except OSError:
            raise FileRejection(503, "archive_unavailable") from None
        _fsync_dir(target.parent)

    def _put_capture(self, session_id, directory, data, offset, body):
        if offset != 0:
            raise FileRejection(409, "offset_conflict")
        self._reserve(len(body))
        digest = hashlib.sha256(body).hexdigest()
        snapshots = directory / "snapshots"
        _private_dir(snapshots, False)
        snapshot = snapshots / (digest + ".json")
        if snapshot.is_symlink():
            raise FileRejection(404, "not_found")
        try:
            if not snapshot.is_file():
                self._write_atomic(snapshots, snapshot, body)
            elif self._read_exact(snapshot, MAX_CAPTURE_BYTES) != body:
                # Torn snapshot from a crashed direct create: repair from
                # the verified PUT body instead of trusting stale bytes.
                self._write_atomic(snapshots, snapshot, body)
        except FileRejection as exc:
            if exc.status == 404:
                raise
            raise FileRejection(503, "archive_unavailable") from None
        self._write_atomic(directory, data, body)
        return _ack(session_id, CAPTURE_FILE, len(body), digest)


class FileImporter:
    """Worker-only original-file importer; construct only in the work role."""

    def __init__(self, archive, max_lines=512, max_bytes=1048576):
        for value in (max_lines, max_bytes):
            if type(value) is bool or not isinstance(value, int) or value < 1:
                raise ValueError("invalid importer bounds")
        self._archive = archive
        self.root = Path(archive.path).parent / "files"
        self._max_lines, self._max_bytes = max_lines, max_bytes
        self._lock = threading.Lock()
        self._last = None
        self._verified = {}
        with archive.connect() as conn:
            conn.executescript(_FILE_SCHEMA)
            conn.commit()

    def _scan(self):
        found = []
        if not self.root.is_dir() or self.root.is_symlink():
            return found
        try:
            vehicles = sorted(self.root.iterdir())
        except OSError:
            return found
        for vehicle in vehicles:
            if vehicle.is_symlink() or not vehicle.is_dir() \
                    or not _IDENTITY.fullmatch(vehicle.name):
                continue
            try:
                collectors = sorted(vehicle.iterdir())
            except OSError:
                continue
            for collector in collectors:
                if collector.is_symlink() or not collector.is_dir() \
                        or not _IDENTITY.fullmatch(collector.name):
                    continue
                try:
                    sessions = sorted(collector.iterdir())
                except OSError:
                    continue
                for session in sessions:
                    if session.is_symlink() or not session.is_dir() \
                            or not _IDENTITY.fullmatch(session.name):
                        continue
                    found.append((vehicle.name, collector.name, session.name))
        return found

    def errors(self):
        with self._archive.connect() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT vehicle,collector_id,session_id,file,error,count,"
                "last_ns,fail_offset FROM file_import_errors ORDER BY "
                "vehicle,collector_id,session_id,file")]

    def import_once(self):
        """Import one bounded batch; True means archived bytes were consumed."""
        with self._lock:
            sessions = self._scan()
            if not sessions:
                return False
            start = sessions.index(self._last) + 1 if self._last in sessions else 0
            ordered = sessions[start:] + sessions[:start]
            for key in ordered:
                try:
                    progressed = self._import_session(*key)
                except _SourceInvalid:
                    progressed = False
                self._last = key
                if progressed:
                    return True
            return False

    def _connect(self):
        return self._archive.connect()

    def _fail(self, vehicle, collector, session, filename, code, fail_offset):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO file_import_errors VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(vehicle,collector_id,session_id,file) DO UPDATE SET "
                "error=excluded.error,count=count+1,last_ns=excluded.last_ns,"
                "fail_offset=excluded.fail_offset",
                (vehicle, collector, session, filename, code,
                 1, time.time_ns(), fail_offset))
            conn.commit()
        raise _SourceInvalid(code)

    def _cursor(self, vehicle, collector, session):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT meta_json,next_seq,last_t,file_offset FROM file_import_state "
                "WHERE vehicle=? AND collector_id=? AND session_id=?",
                (vehicle, collector, session)).fetchone()
        if row is None:
            return {"meta": None, "next_seq": 0, "last_t": None, "file_offset": 0}
        return {"meta": json.loads(row["meta_json"]), "next_seq": row["next_seq"],
                "last_t": Decimal(row["last_t"]) if row["last_t"] else None,
                "file_offset": row["file_offset"]}

    def _clear_error(self, vehicle, collector, session, filename):
        with self._connect() as conn:
            conn.execute("DELETE FROM file_import_errors WHERE vehicle=? "
                         "AND collector_id=? AND session_id=? AND file=?",
                         (vehicle, collector, session, filename))
            conn.commit()

    def _store_cursor(self, vehicle, collector, session, meta, next_seq, last_t, file_offset, filename=SERIAL_FILE):
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO file_import_state VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(vehicle,collector_id,session_id) DO UPDATE SET "
                "meta_json=excluded.meta_json,next_seq=excluded.next_seq,"
                "last_t=excluded.last_t,file_offset=excluded.file_offset,"
                "updated_ns=excluded.updated_ns",
                (vehicle, collector, session, json.dumps(meta, sort_keys=True),
                 next_seq, last_t, file_offset, time.time_ns()))
            conn.execute("DELETE FROM file_import_errors WHERE vehicle=? "
                         "AND collector_id=? AND session_id=? AND file=?",
                         (vehicle, collector, session, filename))
            conn.commit()

    def _read_at(self, path, offset, size):
        """Bounded nofollow pread; short reads end the window, never an error."""
        if size <= 0:
            return b""
        fd = _open_nofollow(path)
        try:
            parts, total = [], 0
            while total < size:
                try:
                    piece = os.pread(fd, min(65536, size - total), offset + total)
                except OSError:
                    raise _SourceInvalid("unreadable_offset")
                if not piece:
                    break
                parts.append(piece)
                total += len(piece)
            return b"".join(parts)
        finally:
            os.close(fd)

    def _import_session(self, vehicle, collector, session):
        try:
            return self._import_session_inner(vehicle, collector, session)
        except FileRejection:
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "unreadable_offset", 0)
        except OSError:
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "unreadable_offset", 0)

    def _import_session_inner(self, vehicle, collector, session):
        directory = self.root / vehicle / collector / session
        capture_path = directory / CAPTURE_FILE
        serial_path = directory / SERIAL_FILE
        if capture_path.is_symlink() or serial_path.is_symlink():
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "unexpected_symlink", 0)
        try:
            fd = _open_nofollow(capture_path)
        except FileRejection as exc:
            if exc.status == 404:
                return False
            raise
        try:
            parts, total = [], 0
            while True:
                piece = os.read(fd, 65536)
                if not piece:
                    break
                parts.append(piece)
                total += len(piece)
                if total > MAX_CAPTURE_BYTES:
                    raise _SourceInvalid("invalid_capture")
            capture_bytes = b"".join(parts)
        except OSError:
            raise _SourceInvalid("unreadable_offset")
        finally:
            os.close(fd)
        try:
            capture = _parse_capture(capture_bytes)
        except _SourceInvalid as exc:
            self._fail(vehicle, collector, session, CAPTURE_FILE,
                       str(exc), 0)
        if not capture["ready"]:
            return False
        self._clear_error(vehicle, collector, session, CAPTURE_FILE)
        state = self._verified.get((vehicle, collector, session))
        if state is not None and state[1] != capture.get("raw_hash"):
            del self._verified[(vehicle, collector, session)]
        meta = {"schema_version": 1, "vehicle": vehicle,
                "collector_id": collector, "session_id": session,
                "started_ns": capture["started_ns"],
                "vehicle_firmware": capture["firmware"]}
        cursor = self._cursor(vehicle, collector, session)
        prior = cursor["meta"]
        if prior is not None and (
                not isinstance(prior, dict)
                or prior.get("started_ns") != meta["started_ns"]
                or prior.get("vehicle_firmware") != meta["vehicle_firmware"]):
            self._fail(vehicle, collector, session, CAPTURE_FILE,
                       "identity_conflict", cursor["file_offset"])
        try:
            sidecar_raw = self._read_at(
                directory / (SERIAL_FILE + ".committed"), 0, 33)
        except _SourceInvalid:
            sidecar_raw = None
        if not sidecar_raw:
            committed = 0
        elif not re.fullmatch(rb"[0-9]{1,19}", sidecar_raw):
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "unreadable_offset", cursor["file_offset"])
            committed = 0
        else:
            committed = int(sidecar_raw)
        try:
            if serial_path.is_symlink():
                raise OSError("symlink")
            size = serial_path.stat().st_size
        except OSError:
            if committed == 0:
                size = 0
            else:
                self._fail(vehicle, collector, session, SERIAL_FILE,
                           "archive_truncated", cursor["file_offset"])
                size = 0
        if size < committed or cursor["file_offset"] > committed:
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "archive_truncated", cursor["file_offset"])
        with self._connect() as conn:
            known = conn.execute(
                "SELECT error,fail_offset FROM file_import_errors WHERE vehicle=? "
                "AND collector_id=? AND session_id=? AND file=?",
                (vehicle, collector, session, SERIAL_FILE)).fetchone()
        # hash_mismatch is metadata-versioned: a corrected capture snapshot
        # must re-enter verification instead of hitting the stale skip.
        if known is not None and known["error"] in _DETERMINISTIC_ERRORS \
                and known["fail_offset"] == cursor["file_offset"] \
                and not (known["error"] == "hash_mismatch"
                         and self._verified.get((vehicle, collector, session)) is None):
            return False
        # Declared final record count gates completion only: an absent
        # count never blocks prefix import, a wrong count never passes.
        declared = capture.get("chunks")
        final = declared is not None and capture.get("raw_hash") is not None
        end = min(committed, cursor["file_offset"] + self._max_bytes)
        try:
            window = self._read_at(serial_path, cursor["file_offset"],
                                   end - cursor["file_offset"])
        except _SourceInvalid:
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "unreadable_offset", cursor["file_offset"])
            window = b""
        if not window:
            # Nothing new, but late-arriving final metadata still needs
            # verification against the already-consumed prefix. Progress
            # is True when a verify slice advanced, so bounded rehash
            # turns keep cycling until the verdict lands.
            if final:
                before = self._verified.get((vehicle, collector, session))
                self._verify_final(vehicle, collector, session, serial_path,
                                   capture, committed, cursor)
                after = self._verified.get((vehicle, collector, session))
                return after is not None and after != before
            return False
        if window.endswith(b"\n"):
            complete, complete_end = window, end
        else:
            cut = window.rfind(b"\n")
            if cut < 0:
                if end < committed:
                    return False
                # EOF tail with no newline: wait while it can still be a
                # growing line; reject only a proven oversized line.
                if len(window) > MAX_LINE_BYTES:
                    self._fail(vehicle, collector, session, SERIAL_FILE,
                               "line_too_long", cursor["file_offset"])
                return False
            complete, complete_end = window[:cut + 1], cursor["file_offset"] + cut + 1
        lines = complete.split(b"\n")[:-1]
        if len(lines) > self._max_lines:
            lines = lines[:self._max_lines]
            complete_end = cursor["file_offset"] + sum(len(line) + 1 for line in lines)
        chunks, seq, previous = [], cursor["next_seq"], cursor["last_t"]
        last_stamp = previous
        try:
            for line in lines:
                chunk, last_stamp = _parse_chunk(
                    line, seq, previous, meta["started_ns"])
                chunks.append(chunk)
                seq, previous = seq + 1, last_stamp
        except _SourceInvalid as exc:
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       str(exc), cursor["file_offset"])
        if not chunks:
            return False
        try:
            self._archive.accept(meta, chunks)
        except Exception as exc:
            if getattr(exc, "status", None) == 409:
                self._fail(vehicle, collector, session, SERIAL_FILE,
                           "archive_conflict", cursor["file_offset"])
            raise
        last_t = (str(last_stamp) if type(last_stamp) is int
                  else format(last_stamp, "f"))
        self._store_cursor(vehicle, collector, session, meta, seq, last_t, complete_end)
        if final and complete_end == committed:
            self._verify_final(vehicle, collector, session, serial_path,
                               capture, committed,
                               {"meta": meta, "next_seq": seq,
                                "last_t": last_stamp, "file_offset": complete_end})
        return True

    def _verify_final(self, vehicle, collector, session, serial_path,
                      capture, committed, cursor):
        """Bounded incremental whole-file verification; one turn, one slice."""
        if not capture.get("raw_hash") or capture.get("chunks") is None:
            self._verified.pop((vehicle, collector, session), None)
            return
        if cursor["next_seq"] != capture["chunks"]:
            return
        key = (vehicle, collector, session)
        state = self._verified.get(key)
        if state is not None and state[:2] != (committed, capture["raw_hash"]):
            state = None
        if state is not None and state[2] == committed:
            return
        offset, digest = (0, hashlib.sha256()) if state is None else (state[2], state[3].copy())
        step = min(self._max_bytes, 1048576)
        try:
            piece = self._read_at(serial_path, offset, min(step, committed - offset))
        except _SourceInvalid:
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "archive_truncated", cursor["file_offset"])
            return
        if len(piece) != min(step, committed - offset):
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "archive_truncated", cursor["file_offset"])
            return
        digest.update(piece)
        offset += len(piece)
        if offset < committed:
            self._verified[key] = (committed, capture["raw_hash"], offset, digest)
            return
        self._verified[key] = (committed, capture["raw_hash"], offset, digest)
        if digest.hexdigest() != capture["raw_hash"]:
            # Suppressible: a corrected capture snapshot clears the error
            # via _clear_error on the next turn's ready parse.
            self._fail(vehicle, collector, session, SERIAL_FILE,
                       "hash_mismatch", committed)
            return
        self._clear_error(vehicle, collector, session, SERIAL_FILE)
