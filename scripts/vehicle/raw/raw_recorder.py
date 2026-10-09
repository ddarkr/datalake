#!/usr/bin/env python3
"""Receive-only SocketCAN raw recorder.

Pipeline: SocketCAN -> append-only JSONL ingress segment -> closed segment
-> finalized MF4 (.mf4) + ingress sidecar + manifest in the local spool.
A separate uploader moves the sealed triple to B2; this process never
deletes an acknowledged byte.

Durability: python-can MF4Writer only persists on stop(), so an active .mf4
is NOT crash-durable. The JSONL ingress segment (fsync every RAW_FSYNC_N
frames or RAW_FSYNC_SEC seconds) is the crash-safe truth; only closed
segments are finalized. The closed JSONL is KEPT locally until the remote
pair acks: finalize copies it to sealed/<stem>.ingress.jsonl.gz (exact
per-frame seq/twall_ns/tcan/bus/id/ext/rtr/err/fd/brs/esi/dlc/data) and the
uploader deletes closed + sealed locals only after the verified remote ack.
A closed JSONL is therefore never deleted by finalize.

MF4 layout is standard python-can bus logging (CAN_DataFrame /
CAN_ErrorFrame / CAN_RemoteFrame groups, CAN FD payloads up to 64 bytes in
DataBytes). Exact capture provenance (every frame, not first/last only)
lives in the sealed ingress sidecar; the manifest carries hashes + bounds
for fail-closed verification. Both are uploaded as a triple by the uploader.
Timestamps on MF4 messages are capture wall time (twall_ns); note the MF4
header start_time is finalize time, so absolute time = header start_time +
relative stamp, exactly as MF4Reader reconstructs it. vcan replay can never
recreate Databroker event_time (dbcfeeder drops observation time), so replay
is timing-faithful only.

Receive-only: recorder mode never calls bus.send(). Replay is a separate
argv-gated CLI that transmits only on vcan* interfaces with explicit opt-in.
"""

import errno
import hashlib
import json
import os
import queue
import shutil
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
try:
    from scripts.vehicle.raw import can_validation as _cv
except ImportError:
    _cv = None  # validation unavailable; Raw capture never depends on it

SCHEMA_V = 1

def _mem_frames_budget():
    return ei("RAW_FINALIZE_MEM_FRAMES", 20000)


def _tmp_max_bytes():
    return ei("RAW_TMP_MAX_BYTES", 2 << 30)


_LINE_CHUNK_BYTES = 64 << 10
_LINE_MAX_BYTES = 1 << 20

class CorruptSegment(ValueError):
    """Closed JSONL failed validation: quarantine, never finalize/delete."""


class IngressStream:
    """Bounded streaming parse of a closed JSONL segment.

    Yields decoded frames one at a time from fixed-size binary chunks;
    never holds the file bytes, text, or frame list, and never buffers
    more than one line head (capped at _LINE_MAX_BYTES). Policy matches
    the former whole-file reader exactly: only an UNTERMINATED trailing
    line (crash mid-write) may be salvaged by truncation (sets .torn);
    a newline-terminated malformed line, mid-file damage, an over-cap
    non-blank line, or undecodable UTF-8 anywhere (including an
    unterminated tail) is corruption (raises CorruptSegment, never
    sets .torn) so one bad byte never deletes good history.
    Over-cap whitespace-only runs are skipped like any blank line, so
    padded segments keep working. Chunk-split multibyte UTF-8 is
    handled by incremental text decoding, never by byte slicing.
    """

    def __init__(self, closed_path):
        self.path = closed_path
        with open(closed_path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            tail = b""
            if size:
                fh.seek(-1, os.SEEK_END)
                tail = fh.read(1)
        self.terminated = (tail == b"\n") if size else True
        self.torn = False

    def _decode_line(self, line, last):
        if not line.strip():
            return None
        try:
            obj = json.loads(line)
        except ValueError:
            pass
        else:
            try:
                return decode_frame(obj)
            except ValueError:
                pass
        if last and not self.terminated:
            self.torn = True  # torn tail only: salvage the rest
            return None
        raise CorruptSegment("corrupt JSONL line in %s" % self.path)

    def _iter_line_texts(self):
        """Yield (text, last) lines with bounded memory.

        Every newline-terminated line is non-last; only the final
        unterminated remainder (if any) is last. A line longer than
        _LINE_MAX_BYTES fails closed when non-blank, or is skipped
        when whitespace-only, without ever buffering the whole line.
        """
        import codecs
        decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
        pending = ""
        over_ws = False
        with open(self.path, "rb") as fh:
            while True:
                chunk = fh.read(_LINE_CHUNK_BYTES)
                final = not chunk
                try:
                    text = decoder.decode(chunk, final)
                except UnicodeDecodeError as ex:
                    raise CorruptSegment("segment not UTF-8: %s" % ex)
                if final:
                    if over_ws:
                        if text.strip():
                            raise CorruptSegment("oversize JSONL line in %s"
                                                 % self.path)
                        return
                    yield pending + text, True
                    return
                if over_ws:
                    cut = text.find("\n")
                    head = text if cut < 0 else text[:cut]
                    if head.strip():
                        raise CorruptSegment("oversize JSONL line in %s"
                                             % self.path)
                    if cut < 0:
                        continue
                    over_ws = False
                    text = text[cut + 1:]
                elif pending:
                    text = pending + text
                    pending = ""
                if "\n" not in text:
                    if len(text) > _LINE_MAX_BYTES:
                        if text.strip():
                            raise CorruptSegment("oversize JSONL line in %s"
                                                 % self.path)
                        over_ws = True
                    else:
                        pending = text
                    continue
                head, pending = text.rsplit("\n", 1)
                for line in head.split("\n"):
                    if len(line) > _LINE_MAX_BYTES:
                        if line.strip():
                            raise CorruptSegment("oversize JSONL line in %s"
                                                 % self.path)
                        continue  # over-cap whitespace run: blank line
                    yield line, False
                if len(pending) > _LINE_MAX_BYTES:
                    if pending.strip():
                        raise CorruptSegment("oversize JSONL line in %s"
                                             % self.path)
                    pending = ""
                    over_ws = True

    def __iter__(self):
        for text, last in self._iter_line_texts():
            frame = self._decode_line(text, last)
            if frame is not None:
                yield frame


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


def ef(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise SystemExit("invalid number %s=%r" % (name, raw))


_M = {"frames": 0, "dropped": 0, "closed": 0, "mf4": 0, "mf4_bytes": 0,
      "spool_files": 0, "spool_bytes": 0, "last_finalize": 0,
      "finalize_errors": 0, "quarantined": 0, "up": 1}
_MLOCK = threading.Lock()


def minc(k, n=1):
    with _MLOCK:
        _M[k] = _M.get(k, 0) + n


def mset(k, v):
    with _MLOCK:
        _M[k] = v


CAN_STATE = {}
_CAN_LOCK = threading.Lock()
_OBSERVED = set()  # stems validated this process; resume must not double-count


def _stem_vehicle(stem):
    parts = stem.split("_")
    if len(parts) >= 4:
        return "_".join(parts[:-3])
    return e("VEHICLE_ID", "")


def _observe_window(frames, stem):
    """Best-effort observational validation of one sealed window. Never
    raises: Raw stays authoritative regardless of validator failure.
    Accepts a frame list or a one-shot ordered generator (bounded path):
    the validator iterates its input exactly once, so counters/gauges
    match whole-window semantics either way."""
    vehicle = _stem_vehicle(stem)
    with _CAN_LOCK:
        if stem in _OBSERVED:
            return
        _OBSERVED.add(stem)
    if _cv is None:
        with _CAN_LOCK:
            CAN_STATE.update(seen=True, ok=False, vehicle=vehicle)
        return
    try:
        epoch, res, flags = _cv.validate_sealed_window(
            frames, vehicle, e("CAN_VALIDATION_DATA_DIR", "/data/current"))
    except Exception as ex:
        with _CAN_LOCK:
            _cv.apply_failure(CAN_STATE, vehicle)
        print("can validation unavailable: %s" % ex, file=sys.stderr)
        return
    with _CAN_LOCK:
        _cv.apply_success(CAN_STATE, vehicle, epoch, res, flags)


def metrics_text():
    with _MLOCK:
        snap = dict(_M)
    text = "".join(f"raw_recorder_{k} {v}\n" for k, v in sorted(snap.items()))
    if _cv is not None:
        with _CAN_LOCK:
            text += _cv.render(CAN_STATE)
    return text


class _H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = metrics_text().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def serve_metrics(host, port):
    try:
        ThreadingHTTPServer((host, port), _H).serve_forever()
    except OSError:
        pass  # metrics are best-effort; recording continues


def encode_frame(seq, twall_ns, tcan, bus, can_id, ext, rtr, err,
                 fd, brs, esi, dlc, data):
    return {"v": SCHEMA_V, "seq": seq, "twall_ns": twall_ns, "tcan": tcan,
            "bus": bus, "id": can_id, "ext": ext, "rtr": rtr, "err": err,
            "fd": fd, "brs": brs, "esi": esi, "dlc": dlc,
            "data": bytes(data).hex()}


def decode_frame(o):
    if not isinstance(o, dict) or o.get("v") != SCHEMA_V:
        raise ValueError("bad schema")
    for k in ("seq", "twall_ns", "bus", "id", "dlc", "data"):
        if k not in o:
            raise ValueError("missing " + k)
    data = bytes.fromhex(o["data"])
    if len(data) > 64:
        raise ValueError("payload too large")
    if o.get("rtr"):
        if len(data) != 0:  # RTR carries a requested length, no payload bytes
            raise ValueError("rtr with payload")
    elif len(data) != o["dlc"]:
        raise ValueError("dlc mismatch")
    d = dict(o)
    d["data"] = data
    return d


def utc_stamp(ns=None):
    if ns is None:
        ns = time.time_ns()
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def seg_dirs(spool):
    return {"active": os.path.join(spool, "active"),
            "closed": os.path.join(spool, "closed"),
            "sealed": os.path.join(spool, "sealed")}


def ensure_dirs(spool):
    for d in seg_dirs(spool).values():
        os.makedirs(d, exist_ok=True)


def read_ingress_frames(closed_path):
    """Read a closed JSONL segment. Only an UNTERMINATED trailing line
    (crash mid-write, no trailing newline) may be salvaged by truncation;
    a newline-terminated malformed line is corruption anywhere-else and
    quarantines the segment (returns None) so one bad byte never deletes
    good history.

    Compatibility wrapper over IngressStream: returns the full frame list.
    finalize_segment no longer uses this on its default path (it streams
    with a memory budget plus a disk-backed sort); callers that need random
    access (redecode verification) keep this exact contract.
    """
    try:
        stream = IngressStream(closed_path)
        frames = [f for f in stream]
    except CorruptSegment:
        return None, False
    return frames, stream.torn


def quarantine(spool, closed_path):
    """Move a corrupt closed segment aside; never finalize, never delete."""
    qd = os.path.join(spool, "quarantine")
    os.makedirs(qd, exist_ok=True)
    dst = os.path.join(qd, os.path.basename(closed_path))
    os.replace(closed_path, dst)
    minc("quarantined")
    return dst


def recover_spool(spool):
    """Crash recovery: active/*.jsonl are complete-but-unclosed, move to
    closed/ for finalization; *.tmp are torn writes, delete. Interrupted
    finalize temp dirs (sealed/<stem>.* holding frames/match sqlite
    files, journals, or legacy run-*.tmp) are swept too; the closed
    JSONL they came from is still on disk, so nothing is lost."""
    ensure_dirs(spool)
    dirs = seg_dirs(spool)
    recovered, swept = 0, 0
    for d in dirs.values():
        for name in sorted(os.listdir(d)):
            if name.endswith(".tmp"):
                try:
                    os.unlink(os.path.join(d, name))
                except OSError:
                    pass
                swept += 1
    sealed = dirs["sealed"]
    try:
        sealed_names = sorted(os.listdir(sealed))
    except OSError:
        sealed_names = []
    for name in sealed_names:
        path = os.path.join(sealed, name)
        if not os.path.isdir(path):
            continue
        try:
            entries = os.listdir(path)
        except OSError:
            continue
        if _is_stale_tmp_dir(entries):
            shutil.rmtree(path, ignore_errors=True)
            swept += 1
    for name in sorted(os.listdir(dirs["active"])):
        if name.endswith(".jsonl"):
            os.replace(os.path.join(dirs["active"], name),
                       os.path.join(dirs["closed"], name))
            recovered += 1
    return recovered, swept


class NoSpace(Exception):
    pass


def disk_ok(spool, min_free):
    try:
        return shutil.disk_usage(spool).free >= min_free
    except OSError:
        return False


def spool_stats(spool):
    nfiles, nbytes = 0, 0
    for d in seg_dirs(spool).values():
        try:
            names = os.listdir(d)
        except OSError:
            continue
        for name in names:
            p = os.path.join(d, name)
            try:
                nbytes += os.path.getsize(p)
                nfiles += 1
            except OSError:
                pass
    return nfiles, nbytes


class SegWriter:
    """Append-only JSONL ingress segment with explicit fsync cadence."""

    def __init__(self, spool, vehicle, chunk_sec, chunk_bytes,
                 fsync_n, fsync_sec):
        self.dirs = seg_dirs(spool)
        self.vehicle = vehicle
        self.chunk_sec = chunk_sec
        self.chunk_bytes = chunk_bytes
        self.fsync_n = fsync_n
        self.fsync_sec = fsync_sec
        self.fh = None
        self.stem = ""
        self.t0 = 0.0
        self.nbytes = 0
        self.nsince = 0
        self.tlast = 0.0
        self.nseg = 0
        # Unique per process: restarts in the same second can never reuse a
        # stem, so a sealed .mf4 is never overwritten by a later process.
        self.capture_id = uuid.uuid4().hex[:8]

    def _open(self):
        adir = self.dirs["active"]
        while True:  # same-second rotations/restarts must not share a stem
            self.nseg += 1
            self.stem = "%s_%s_%s_%03d" % (
                self.vehicle, utc_stamp(), self.capture_id, self.nseg)
            if not os.path.exists(os.path.join(adir, self.stem + ".jsonl")):
                break
        self.fh = open(os.path.join(adir, self.stem + ".jsonl"),
                       "a", encoding="utf-8", buffering=1)
        now = time.monotonic()
        self.t0 = now
        self.tlast = now
        self.nbytes = 0
        self.nsince = 0

    def _sync(self):
        try:
            self.fh.flush()
            os.fsync(self.fh.fileno())
        except OSError as ex:
            if ex.errno == errno.ENOSPC:
                raise NoSpace()
            raise

    def write(self, line):
        """Append one line; returns closed segment path when rotation fires."""
        if self.fh is None:
            self._open()
        try:
            self.fh.write(line + "\n")
        except OSError as ex:
            if ex.errno == errno.ENOSPC:
                raise NoSpace()
            raise
        self.nbytes += len(line) + 1
        self.nsince += 1
        now = time.monotonic()
        if self.nsince >= self.fsync_n or now - self.tlast >= self.fsync_sec:
            self._sync()
            self.nsince = 0
            self.tlast = now
        if now - self.t0 >= self.chunk_sec or self.nbytes >= self.chunk_bytes:
            return self.rotate()
        return None

    def idle_tick(self):
        """Advance fsync/rotation deadlines with no new frame (idle bus)."""
        if self.fh is None:
            return None
        now = time.monotonic()
        if self.nsince and now - self.tlast >= self.fsync_sec:
            self._sync()
            self.nsince = 0
            self.tlast = now
        if now - self.t0 >= self.chunk_sec:
            return self.rotate()
        return None

    def rotate(self):
        if self.fh is None:
            return None
        self._sync()
        self.fh.close()
        self.fh = None
        src = os.path.join(self.dirs["active"], self.stem + ".jsonl")
        if self.nbytes == 0:
            try:
                os.unlink(src)
            except OSError:
                pass
            return None
        dst = os.path.join(self.dirs["closed"], self.stem + ".jsonl")
        os.replace(src, dst)
        minc("closed")
        return dst


def _frame_to_message(f, bus):
    """Ingress frame -> python-can Message. Timestamp is capture wall time
    (twall_ns); bus names map to integer channels (can0->0). RTR carries no
    payload bytes, only the requested length in dlc."""
    import can
    channel = bus if bus is not None else f.get("bus", "")
    if channel is None:
        channel = ""
    return can.Message(
        timestamp=f["twall_ns"] / 1e9,
        arbitration_id=f["id"],
        data=bytes(f["data"]),
        channel=channel,
        is_extended_id=bool(f.get("ext", False)),
        is_remote_frame=bool(f.get("rtr", False)),
        is_error_frame=bool(f.get("err", False)),
        is_fd=bool(f.get("fd", False)),
        bitrate_switch=bool(f.get("brs", False)),
        error_state_indicator=bool(f.get("esi", False)),
        dlc=f["dlc"] if f.get("rtr") else len(f["data"]))


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _frame_json(f):
    return json.dumps(encode_frame(
        f["seq"], f["twall_ns"], f["tcan"], f["bus"], f["id"], f["ext"],
        f["rtr"], f["err"], f["fd"], f["brs"], f["esi"], f["dlc"],
        f["data"]), separators=(",", ":"))


def _frame_sort_key(f):
    return (f["twall_ns"], f["seq"])




def _enforce_tmp_budget(tmpdir, max_bytes):
    total = 0
    for name in os.listdir(tmpdir):
        try:
            total += os.path.getsize(os.path.join(tmpdir, name))
        except OSError:
            pass
    if total > max_bytes:
        raise NoSpace("finalize temp budget exceeded in %s" % tmpdir)
    return total


class SortedFrameStore:
    """Chronological frame order with a bounded memory budget.

    Small segments stream entirely in memory. Larger segments spill
    arrival batches into ONE temp SQLite file (`frames.sqlite3`,
    journal included) indexed by (twall_ns, seq); ordered reads page
    through that index with exactly one cursor and one decoded frame
    resident. No run-file list, no proportional heap, no multi-FD
    fan-in: memory stays bounded by the configured frame budget and
    disk stays bounded by tmp_max_bytes. The temp dir budget is
    enforced during every staging batch AND during index creation,
    including SQLite journal/WAL and match spillover; exceeding it
    raises NoSpace (closed JSONL stays, retry later) instead of
    filling the disk or silently discarding frames. Interrupted temp
    dirs (single `<stem>.*` sqlite dir) are swept on entry before
    reuse; `destroy()` closes the handle first, then removes the dir.
    """

    def __init__(self, sealed_dir, stem, budget_frames, max_bytes):
        self.prefix = stem + "."
        self.budget = int(budget_frames)
        if self.budget <= 0:
            raise ValueError("mem_frames budget must be > 0, got %r"
                             % (budget_frames,))
        self.max_bytes = int(max_bytes)
        if self.max_bytes <= 0:
            raise ValueError("tmp_max_bytes must be > 0, got %r"
                             % (max_bytes,))
        self.buf = []
        self.db = None
        self.dir = None
        try:
            self.dir = tempfile.mkdtemp(prefix=self.prefix, dir=sealed_dir)
        except OSError as ex:
            if ex.errno == errno.ENOSPC:
                raise NoSpace("finalize temp dir: %s" % ex)
            raise
        self.db_path = os.path.join(self.dir, "frames.sqlite3")
        self.n = 0
        self.spilled = False
        self.peak_resident_frames = 0
        self.peak_temp_bytes = 0
        try:
            self.db = sqlite3.connect(self.db_path)
            self.db.execute("CREATE TABLE frames (twall_ns INTEGER,"
                            " seq INTEGER, frame TEXT)")
            self.db.commit()
            self.peak_temp_bytes = _enforce_tmp_budget(self.dir, self.max_bytes)
        except (OSError, sqlite3.OperationalError) as ex:
            self.destroy()
            if getattr(ex, "errno", None) == errno.ENOSPC or \
                    "full" in str(ex).lower() or "space" in str(ex).lower():
                raise NoSpace("finalize temp db: %s" % ex) from ex
            raise
        except BaseException:
            self.destroy()
            raise

    def _note_temp(self):
        self.peak_temp_bytes = max(
            self.peak_temp_bytes,
            _enforce_tmp_budget(self.dir, self.max_bytes))

    def _flush_batch(self):
        try:
            self.db.executemany(
                "INSERT INTO frames VALUES (?,?,?)",
                [(f["twall_ns"], f["seq"], _frame_json(f))
                 for f in self.buf])
            self.db.commit()
        except sqlite3.OperationalError as ex:
            if "full" in str(ex).lower() or "space" in str(ex).lower():
                raise NoSpace("finalize temp db: %s" % ex)
            raise
        self.buf = []
        self.spilled = True
        self._note_temp()

    def add(self, frame):
        self.buf.append(frame)
        self.n += 1
        if len(self.buf) > self.peak_resident_frames:
            self.peak_resident_frames = len(self.buf)
        if len(self.buf) >= self.budget:
            self._flush_batch()

    def __len__(self):
        return self.n

    def finish_staging(self):
        """Flush leftovers and build the order index (budget-enforced)."""
        try:
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS frames_order"
                " ON frames (twall_ns, seq)")
            self.db.commit()
        except sqlite3.OperationalError as ex:
            if "full" in str(ex).lower() or "space" in str(ex).lower():
                raise NoSpace("finalize temp db: %s" % ex)
            raise
        self._note_temp()

    def _drain_buf_ordered(self):
        self.buf.sort(key=_frame_sort_key)
        for f in self.buf:
            yield f

    def ordered(self):
        if not self.spilled:
            for f in self._drain_buf_ordered():
                yield f
            return
        if self.buf:
            self._flush_batch()
        self.finish_staging()
        cur = self.db.execute(
            "SELECT frame FROM frames ORDER BY twall_ns, seq")
        try:
            for (blob,) in cur:
                yield decode_frame(json.loads(blob))
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def first_last(self):
        if not self.spilled:
            self.buf.sort(key=_frame_sort_key)
            if not self.buf:
                return 0, None, None, None, None, None, None
            first, last = self.buf[0], self.buf[-1]
            return (len(self.buf), first, last, first["seq"],
                    last["seq"], first["twall_ns"], last["twall_ns"])
        if self.buf:
            self._flush_batch()
        self.finish_staging()
        row = self.db.execute(
            "SELECT frame FROM frames ORDER BY twall_ns, seq LIMIT 1"
        ).fetchone()
        tail = self.db.execute(
            "SELECT frame FROM frames ORDER BY twall_ns DESC, seq DESC"
            " LIMIT 1").fetchone()
        if row is None:
            return 0, None, None, None, None, None, None
        first = decode_frame(json.loads(row[0]))
        last_f = decode_frame(json.loads(tail[0]))
        return (self.n, first, last_f, first["seq"], last_f["seq"],
                first["twall_ns"], last_f["twall_ns"])

    def destroy(self):
        try:
            if self.db is not None:
                self.db.close()
        except Exception:
            pass
        self.buf = []
        if self.dir is not None:
            shutil.rmtree(self.dir, ignore_errors=True)


def _write_manifest_bounds(path, nframes, bus, seq_first, seq_last,
                           tw_first, tw_last, stem, capture_id, torn,
                           mf4_digest, ingress_digest, sidecar_digest):
    manifest = {
        "v": 2,
        "stem": stem,
        "capture_id": capture_id,
        "bus": bus,
        "frames": nframes,
        "seq_first": seq_first,
        "seq_last": seq_last,
        "twall_ns_first": tw_first,
        "twall_ns_last": tw_last,
        "torn_tail_salvaged": bool(torn),
        "mf4_sha256": mf4_digest,
        "ingress_sha256": ingress_digest,
        "ingress_sidecar_sha256": sidecar_digest,
        "ingress_sidecar": stem + ".ingress.jsonl.gz",
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(manifest, separators=(",", ":")) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _is_stale_tmp_dir(entries):
    return any(
        (n.startswith("run-") and n.endswith(".tmp"))  # legacy run files
        or n in ("frames.sqlite3", "frames.sqlite3-journal",
                 "frames.sqlite3-wal", "match.sqlite3",
                 "match.sqlite3-journal", "match.sqlite3-wal")
        for n in entries)


def _sweep_stale_tmp(sealed_dir, stem):
    """Remove interrupted-finalize temp dirs for this stem only."""
    swept = 0
    try:
        names = os.listdir(sealed_dir)
    except OSError:
        return 0
    for name in names:
        if not name.startswith(stem + "."):
            continue
        path = os.path.join(sealed_dir, name)
        if name.endswith(".stage.tmp"):
            continue  # rebuilt/overwritten below, not a stale-run sweep
        if not os.path.isdir(path):
            continue
        try:
            entries = os.listdir(path)
        except OSError:
            continue
        if _is_stale_tmp_dir(entries):
            try:
                shutil.rmtree(path)
                swept += 1
            except OSError:
                pass
    return swept


def _message_key(message):
    from can.util import channel2int
    return (message.arbitration_id & 0x1FFFFFFF, bytes(message.data),
            message.dlc, channel2int(message.channel) or 0,
            message.is_extended_id, message.is_remote_frame,
            message.is_error_frame, message.is_fd,
            message.bitrate_switch, message.error_state_indicator)


def matched_mf4_frames(mf4_path, frames):
    """Match complete frame identities, not MF4 group iteration positions.

    Float-equal timestamps can reorder Data/Error/Remote groups. The
    ingress sidecar retains the exact nanoseconds for each matching frame.
    Identical frame copies consume their timestamps in chronological order.
    Accepts a frame list (kept for redecode callers) or a SortedFrameStore
    (bounded default path: identities spill to SQLite past the budget).
    """
    from can.io.mf4 import MF4Reader
    if isinstance(frames, SortedFrameStore):
        for message, frame in _matched_mf4_store(mf4_path, frames):
            yield message, frame
        return
    pending = _pending_index(_iter_ordered_frames(frames))
    reader = MF4Reader(mf4_path)
    try:
        for message, frame in _drain_reader(reader, pending):
            yield message, frame
        _require_consumed(pending)
    finally:
        reader.stop()


def _iter_ordered_frames(source):
    """Yield frames in chronological order from a list or a frame store."""
    if isinstance(source, SortedFrameStore):
        for f in source.ordered():
            yield f
        return
    for f in sorted(source, key=_frame_sort_key):
        yield f


def _pending_index(ordered):
    from collections import defaultdict
    pending = defaultdict(list)
    for frame in ordered:
        pending[_message_key(_frame_to_message(frame, None))].append(frame)
    for stack in pending.values():
        stack.reverse()  # chronological pop() from the end
    return pending


def _drain_reader(reader, pending):
    for message in reader:
        candidates = pending.get(_message_key(message))
        if not candidates:
            raise ValueError("MF4 contains an unmatched frame")
        frame = candidates.pop()
        if abs(message.timestamp - frame["twall_ns"] / 1e9) > 0.001:
            raise ValueError("timestamp mismatch seq=%s" % frame["seq"])
        yield message, frame


def _require_consumed(pending):
    if any(pending.values()):
        raise ValueError("MF4 is missing ingress frames")


def _matched_mf4_store(mf4_path, store):
    """Bounded identity matching: per-identity timestamp stacks live in a
    temp SQLite table (one row per frame) inside the store temp dir, so
    duplicate-heavy segments do not build one proportional in-memory
    list per identity. Rows stage in budget-derived batches (never the
    fixed 2000-row window regardless of a smaller budget, never more
    than the configured frame budget resident); the temp dir budget is
    enforced during every staging batch and after the match index is
    built, counting journal/WAL/index bytes. On NoSpace the caller keeps
    the closed JSONL, publishes nothing, and the temp dir is destroyed
    closed; the separate match table is unlinked before return.
    """
    import json as _json
    from can.io.mf4 import MF4Reader
    db_path = os.path.join(store.dir, "match.sqlite3")
    try:
        db = sqlite3.connect(db_path)
    except OSError as ex:
        if ex.errno == errno.ENOSPC:
            raise NoSpace("finalize match db: %s" % ex)
        raise
    try:
        db.execute("CREATE TABLE pending (ident BLOB, twall_ns INTEGER,"
                   " seq INTEGER, frame TEXT)")
        batch = max(1, min(2000, int(store.budget)))
        rows = []

        def _stage(rows):
            try:
                db.executemany("INSERT INTO pending VALUES (?,?,?,?)",
                               rows)
                db.commit()
            except sqlite3.OperationalError as ex:
                if "full" in str(ex).lower() \
                        or "space" in str(ex).lower():
                    raise NoSpace("finalize match db: %s" % ex)
                raise
            store._note_temp()
            if len(rows) > store.peak_resident_frames:
                store.peak_resident_frames = len(rows)

        for frame in store.ordered():
            ident = repr(_message_key(_frame_to_message(frame, None)))
            rows.append((ident, frame["twall_ns"], frame["seq"],
                         _frame_json(frame)))
            if len(rows) >= batch:
                _stage(rows)
                rows = []
        if rows:
            _stage(rows)
        try:
            db.execute("CREATE INDEX pending_ident ON pending (ident,"
                       " twall_ns, seq)")
            db.commit()
        except sqlite3.OperationalError as ex:
            if "full" in str(ex).lower() or "space" in str(ex).lower():
                raise NoSpace("finalize match db: %s" % ex)
            raise
        store._note_temp()
        reader = MF4Reader(mf4_path)
        try:
            for message in reader:
                ident = repr(_message_key(message))
                row = db.execute(
                    "SELECT rowid, frame FROM pending WHERE ident=?"
                    " ORDER BY twall_ns, seq LIMIT 1", (ident,)).fetchone()
                if row is None:
                    raise ValueError("MF4 contains an unmatched frame")
                frame = decode_frame(_json.loads(row[1]))
                if abs(message.timestamp - frame["twall_ns"] / 1e9) > 0.001:
                    raise ValueError("timestamp mismatch seq=%s"
                                     % frame["seq"])
                db.execute("DELETE FROM pending WHERE rowid=?", (row[0],))
                yield message, frame
            left = db.execute(
                "SELECT COUNT(*) FROM pending").fetchone()[0]
            if left:
                raise ValueError("MF4 is missing ingress frames")
        finally:
            reader.stop()
    finally:
        try:
            db.close()
        except Exception:
            pass
        for suffix in ("", "-journal", "-wal", "-shm"):
            try:
                os.unlink(db_path + suffix)
            except OSError:
                pass


def verify_sealed_mf4(mf4_path, frames):
    """Verify every payload, flag, identifier, channel and capture time."""
    for _message, _frame in matched_mf4_frames(mf4_path, frames):
        pass
    return True


def _write_ingress_sidecar(path, closed_path, _frames=None):
    """Copy the exact closed JSONL bytes to a gzipped sealed sidecar, so the
    full per-frame provenance (seq/twall_ns/tcan/DLC/payload) survives even
    though the standard MF4 groups cannot store it. Returns the sha256 of
    the COMPRESSED sidecar file, matching what the uploader re-hashes.
    Byte copy only (bounded chunks); no frame list is needed."""
    import gzip
    tmp = path + ".tmp"
    with open(closed_path, "rb") as src, open(tmp, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw,
                           mtime=0) as gz:
            for chunk in iter(lambda: src.read(1 << 20), b""):
                gz.write(chunk)
    with open(tmp, "rb") as fh:
        os.fsync(fh.fileno())
    digest = _sha256_file(tmp)
    os.replace(tmp, path)
    return digest


def _fsync_dir(path):
    try:
        dirfd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def _observe_ordered(store, stem):
    """Bounded observational validation: the validator folds one window per
    call and only iterates its input once, so passing the store's ordered
    generator keeps exact whole-window counters/gauges with bounded
    memory (validator state is bounded by signal/ID cardinality)."""
    _observe_window(store.ordered(), stem)


def finalize_segment(closed_path, sealed_dir, spool=None,
                     mem_frames=None, tmp_max_bytes=None, stats=None):
    """Finalize one closed JSONL segment to standard CAN bus logging MF4.
    Writes via python-can MF4Writer (CAN_DataFrame/Error/Remote groups, CAN
    FD payloads to 64 bytes), verifies the FULL payload round-trip with
    MF4Reader BEFORE the sealed rename (uploader never sees unverified MF4),
    then publishes the immutable triple (<stem>.mf4 + <stem>.ingress.jsonl.gz
    + <stem>.mf4.manifest.json) and RETURNS with the closed JSONL still on
    disk. The uploader deletes closed + sealed locals only after the verified
    remote ack, so finalize itself never deletes ingress history.
    Resumable: if a crash left a verified triple behind (MF4 + sidecar +
    manifest all present and hash-clean), the closed JSONL is re-hashed and
    the call returns the existing MF4; a half-written triple is rebuilt from
    the closed JSONL still on disk. Returns the MF4 path or None for empty.

    Bounded: the closed JSONL streams through IngressStream (fixed-size
    chunks, one line buffer capped at 1 MiB) with at most mem_frames
    (RAW_FINALIZE_MEM_FRAMES, default 20000) resident; larger segments
    spill insert batches to one temp SQLite file indexed by
    (twall_ns, seq) under sealed_dir, bounded by tmp_max_bytes
    (RAW_TMP_MAX_BYTES, default 2 GiB) enforced during sort AND match
    staging. Budgets <= 0 raise ValueError. Exceeding the temp budget
    raises NoSpace: the closed JSONL stays, nothing is published, retry
    later. Interrupted temp dirs (<stem>.* sqlite dirs) are swept on
    entry before reuse. Pass stats={} to collect peak_temp_bytes,
    peak_resident_frames, budget_frames, tmp_max_bytes.
    """
    import gzip
    from can.io.mf4 import MF4Writer

    stem = os.path.basename(closed_path)
    if not stem.endswith(".jsonl"):
        raise ValueError("not a segment: " + stem)
    stem = stem[:-len(".jsonl")]
    if mem_frames is None:
        mem_frames = _mem_frames_budget()
    if tmp_max_bytes is None:
        tmp_max_bytes = _tmp_max_bytes()
    if int(mem_frames) <= 0:
        raise ValueError("mem_frames budget must be > 0, got %r"
                         % (mem_frames,))
    if int(tmp_max_bytes) <= 0:
        raise ValueError("tmp_max_bytes must be > 0, got %r"
                         % (tmp_max_bytes,))
    if spool is None:
        spool = os.path.dirname(os.path.dirname(closed_path))
    _sweep_stale_tmp(sealed_dir, stem)
    out = os.path.join(sealed_dir, stem + ".mf4")
    sidecar = os.path.join(sealed_dir, stem + ".ingress.jsonl.gz")
    manifest_path = out + ".manifest.json"
    ingress_digest = _sha256_file(closed_path)
    capture_id = stem.split("_")[-2] if "_" in stem else ""
    # Crash-resume: a complete verified triple is immutable. If the manifest
    # verifies against the bytes on disk, reuse it; never rebuild or
    # overwrite a verified MF4.
    if os.path.exists(out) and os.path.exists(sidecar) and os.path.exists(
            manifest_path):
        try:
            with open(manifest_path, encoding="utf-8") as fh:
                man = json.load(fh)
            if (man.get("ingress_sha256") == ingress_digest
                    and man.get("mf4_sha256") == _sha256_file(out)
                    and man.get("ingress_sidecar_sha256")
                    == _sha256_file(sidecar)):
                try:
                    digest = hashlib.sha256()
                    with gzip.open(sidecar, "rb") as fz:
                        for chunk in iter(lambda: fz.read(8 << 20), b""):
                            digest.update(chunk)
                    if digest.hexdigest() != ingress_digest:
                        raise ValueError("sidecar ingress content drift")
                except OSError:
                    raise ValueError("sidecar unreadable")
                minc("mf4")
                _observe_window(IngressStream(closed_path), stem)
                return out
        except ValueError:
            pass
        # Complete files present but hash-dirty: torn triple, rebuild below.
    elif os.path.exists(out) and not (
            os.path.exists(sidecar) and os.path.exists(manifest_path)):
        # Orphan MF4 from a crash between MF4 rename and sidecar/manifest
        # write: the MF4 bytes predate verification, so they are NOT trusted.
        # Remove and rebuild from the closed JSONL still on disk.
        try:
            os.unlink(out)
        except OSError:
            pass
    try:
        store = SortedFrameStore(sealed_dir, stem, mem_frames, tmp_max_bytes)
    except NoSpace:
        minc("finalize_errors")
        raise
    try:
        try:
            stream = IngressStream(closed_path)
            for frame in stream:
                store.add(frame)
            torn = stream.torn
        except CorruptSegment:
            quarantine(spool, closed_path)
            minc("finalize_errors")
            raise ValueError("corrupt segment quarantined: " + stem)
        except NoSpace:
            if stats is not None:
                stats.update(peak_temp_bytes=store.peak_temp_bytes,
                             peak_resident_frames=store.peak_resident_frames,
                             budget_frames=store.budget,
                             tmp_max_bytes=store.max_bytes)
            minc("finalize_errors")
            raise
            if stats is not None:
                stats.update(peak_temp_bytes=store.peak_temp_bytes,
                             peak_resident_frames=store.peak_resident_frames,
                             budget_frames=store.budget,
                             tmp_max_bytes=store.max_bytes)
            store.destroy()
            os.unlink(closed_path)
            return None
        nframes, first, last, seq_first, seq_last, tw_first, tw_last = \
            store.first_last()
        bus = first["bus"]
        # MF4Writer has no append mode: one writer per segment, stop()
        # persists. Write to a staging name WITHOUT a .mf4 suffix: asammdf
        # save() appends the suffix itself when missing, so passing
        # stem.mf4.tmp would land on disk as stem.mf4.tmp.mf4.
        stage = os.path.join(sealed_dir, stem + ".stage.tmp")
        writer = MF4Writer(stage)
        try:
            for f in store.ordered():
                writer.on_message_received(_frame_to_message(f, f.get("bus")))
            writer.stop()
        except Exception:
            try:
                writer.stop()
            except Exception:
                pass
            try:
                os.unlink(stage)
            except OSError:
                pass
            minc("finalize_errors")
            raise
        try:
            staged_bytes = os.path.getsize(stage)
        except OSError:
            staged_bytes = 0
        combined_temp = store.peak_temp_bytes + staged_bytes
        if combined_temp > store.max_bytes:
            try:
                os.unlink(stage)
            except OSError:
                pass
            minc("finalize_errors")
            raise NoSpace("finalize temp budget exceeded by staged MF4"
                          " (%d + %d > %d)"
                          % (staged_bytes, store.peak_temp_bytes,
                             store.max_bytes))
        store.peak_temp_bytes = combined_temp
        try:
            verify_sealed_mf4(stage, store)
        except Exception:
            try:
                os.unlink(stage)
            except OSError:
                pass
            minc("finalize_errors")
            raise
        mf4_digest = _sha256_file(stage)
        # fsync the verified MF4 before any rename makes it visible.
        with open(stage, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(stage, out)
        sidecar_digest = _write_ingress_sidecar(sidecar, closed_path, store)
        _write_manifest_bounds(manifest_path, nframes, bus, seq_first,
                               seq_last, tw_first, tw_last, stem, capture_id,
                               torn, mf4_digest, ingress_digest,
                               sidecar_digest)
        _fsync_dir(sealed_dir)
        _observe_ordered(store, stem)
        peak_temp = store.peak_temp_bytes
        peak_resident = store.peak_resident_frames
        budget_frames = store.budget
        tmp_cap = store.max_bytes
    finally:
        store.destroy()
    if stats is not None:
        stats.update(peak_temp_bytes=peak_temp,
                     peak_resident_frames=peak_resident,
                     budget_frames=budget_frames,
                     tmp_max_bytes=tmp_cap)
    minc("mf4")
    minc("mf4_bytes", os.path.getsize(out))
    mset("last_finalize", int(time.time()))
    return out


def _final_worker(q, sealed_dir, spool):
    while True:
        p = q.get()
        if p is None:
            return
        try:
            try:
                finalize_segment(p, sealed_dir, spool=spool)
            except NoSpace as ex:
                time.sleep(60)
                q.put(p)  # budget/disk full: closed JSONL stays, retry
            except OSError as ex:
                if ex.errno == errno.ENOSPC:
                    time.sleep(60)
                    q.put(p)  # disk full: closed JSONL stays, retry later
                else:
                    minc("finalize_errors")
                    print("finalize error %s: %s" % (p, ex), file=sys.stderr)
        except Exception as ex:
            # Corrupt segments are already quarantined inside finalize; only
            # count+log the rest (verify failures keep the closed JSONL).
            if "quarantined" not in str(ex):
                minc("finalize_errors")
            print("finalize failed %s: %s" % (p, ex), file=sys.stderr)
        finally:
            q.task_done()


def replay_allowed(iface):
    if e("RAW_REPLAY_ALLOW", "0") != "1":
        return False, "replay not enabled (set RAW_REPLAY_ALLOW=1)"
    if not iface.startswith("vcan"):
        return False, "refusing TX on non-vcan interface %r" % (iface,)
    return True, ""


def run_replay(args):
    if not args:
        print("usage: python -m scripts.vehicle.raw.raw_recorder replay <file.mf4> [vcan_iface]",
              file=sys.stderr)
        return 2
    iface = args[1] if len(args) > 1 else e("CAN_INTERFACE", "can0")
    ok, reason = replay_allowed(iface)
    if not ok:
        print("replay refused: " + reason, file=sys.stderr)
        return 2
    import can
    from can.io.mf4 import MF4Reader
    reader = MF4Reader(args[0])
    try:
        bus = can.Bus(channel=iface, interface="socketcan", fd=True)
        try:
            prev = None
            for m in reader:
                # vcan replay is timing-faithful only: MF4Reader timestamps
                # are capture wall time; Databroker event_time cannot be
                # recreated (dbcfeeder drops observation time), so this path
                # never claims to reproduce VSS event_time.
                if m.is_error_frame:
                    continue  # error frames are not transmittable
                dt = (m.timestamp - prev) if prev is not None else 0.0
                if 0.0 < dt < 1.0:
                    time.sleep(float(dt))
                prev = m.timestamp
                bus.send(m)
        finally:
            bus.shutdown()
    finally:
        reader.stop()
    return 0


def run_recorder():
    vehicle = e("VEHICLE_ID", "")
    if not vehicle:
        print("VEHICLE_ID is required", file=sys.stderr)
        return 1
    spool = e("RAW_SPOOL_DIR", "/spool/raw")
    iface = e("CAN_INTERFACE", "can0")
    chunk_sec = ei("RAW_CHUNK_SECONDS", 600)
    chunk_bytes = ei("RAW_CHUNK_BYTES", 67108864)
    fsync_n = ei("RAW_FSYNC_N", 50)
    fsync_sec = ef("RAW_FSYNC_SEC", 1.0)
    min_free = ei("RAW_DISK_MIN_FREE_BYTES", 1073741824)
    ensure_dirs(spool)
    dirs = seg_dirs(spool)
    recovered, _ = recover_spool(spool)
    fq = queue.Queue()
    worker = threading.Thread(target=_final_worker,
                              args=(fq, dirs["sealed"], spool),
                              daemon=True)
    worker.start()
    # Resume: every closed JSONL still on disk is re-finalized. finalize is
    # resumable (verified triples return as-is, half-written triples rebuild
    # from the closed JSONL), so the MF4-rename/manifest crash orphan heals
    # here instead of refusing forever.
    for name in sorted(os.listdir(dirs["closed"])):
        if name.endswith(".jsonl"):
            fq.put(os.path.join(dirs["closed"], name))
    if recovered:
        print("recovered %d partial segment(s)" % recovered, file=sys.stderr)
    host = e("RAW_METRICS_HOST", "127.0.0.1")
    port = ei("RAW_METRICS_PORT", 9102)
    threading.Thread(target=serve_metrics, args=(host, port), daemon=True).start()
    import can  # noqa: hardware/socketcan backend, lazy so tests stay stdlib-only
    try:
        bus = can.Bus(channel=iface, interface="socketcan",
                      receive_own_messages=False, fd=True)
    except Exception as ex:
        print("cannot open %s: %s" % (iface, ex), file=sys.stderr)
        return 1
    stop = []
    signal.signal(signal.SIGTERM, lambda *a: stop.append(1))
    signal.signal(signal.SIGINT, lambda *a: stop.append(1))
    writer = SegWriter(spool, vehicle, chunk_sec, chunk_bytes, fsync_n, fsync_sec)
    seq = 0
    room = {"ok": True, "t": 0.0}
    last_stat = 0.0
    try:
        while not stop:
            try:
                msg = bus.recv(1.0)
            except Exception as ex:
                print("recv error: %s" % ex, file=sys.stderr)
                time.sleep(1.0)
                continue
            if msg is None:
                # Idle bus still owes fsync/rotation deadlines: force the
                # cadence check through the writer each second.
                closed = writer.idle_tick()
                if closed is not None:
                    fq.put(closed)
                continue
            now = time.monotonic()
            if now - room["t"] > 5.0:
                room["ok"] = disk_ok(spool, min_free)
                room["t"] = now
            if not room["ok"]:
                minc("dropped")  # disk full: frame lost, counted honestly
                continue
            seq += 1
            frame = encode_frame(seq, time.time_ns(), msg.timestamp, iface,
                                 msg.arbitration_id, msg.is_extended_id,
                                 msg.is_remote_frame, msg.is_error_frame,
                                 getattr(msg, "is_fd", False),
                                 getattr(msg, "bitrate_switch", False),
                                 getattr(msg, "error_state_indicator", False),
                                 msg.dlc, bytes(msg.data))
            try:
                closed = writer.write(json.dumps(frame, separators=(",", ":")))
            except NoSpace:
                room["ok"] = False
                room["t"] = now
                minc("dropped")
                continue
            minc("frames")
            if closed is not None:
                fq.put(closed)
            if now - last_stat > 5.0:
                nf, nb = spool_stats(spool)
                mset("spool_files", nf)
                mset("spool_bytes", nb)
                last_stat = now
    finally:
        try:
            closed = writer.rotate()
        except OSError:
            closed = None
        if closed is not None:
            fq.put(closed)
        bus.shutdown()
    return 0


def main(argv):
    if len(argv) > 1 and argv[1] == "replay":
        return run_replay(argv[2:])
    return run_recorder()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
