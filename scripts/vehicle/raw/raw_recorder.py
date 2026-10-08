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
import sys
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
    raises: Raw stays authoritative regardless of validator failure."""
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
    good history."""
    with open(closed_path, "rb") as fh:
        raw = fh.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, False
    terminated = raw.endswith(b"\n") if raw else True
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    frames = []
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            frames.append(decode_frame(json.loads(line)))
        except ValueError:
            if i == len(lines) - 1 and not terminated:
                return frames, True  # torn tail only: salvage the rest
            return None, False
    return frames, False


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
    closed/ for finalization; *.tmp are torn writes, delete."""
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


def _write_manifest(path, frames, stem, capture_id, torn, mf4_digest,
                    ingress_digest, sidecar_digest):
    manifest = {
        "v": 2,
        "stem": stem,
        "capture_id": capture_id,
        "bus": frames[0]["bus"],
        "frames": len(frames),
        "seq_first": frames[0]["seq"],
        "seq_last": frames[-1]["seq"],
        "twall_ns_first": frames[0]["twall_ns"],
        "twall_ns_last": frames[-1]["twall_ns"],
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
    """
    from collections import defaultdict
    from can.io.mf4 import MF4Reader
    pending = defaultdict(list)
    for frame in sorted(frames, key=lambda f: f["twall_ns"], reverse=True):
        pending[_message_key(_frame_to_message(frame, None))].append(frame)
    reader = MF4Reader(mf4_path)
    try:
        for message in reader:
            candidates = pending.get(_message_key(message))
            if not candidates:
                raise ValueError("MF4 contains an unmatched frame")
            frame = candidates.pop()
            if abs(message.timestamp - frame["twall_ns"] / 1e9) > 0.001:
                raise ValueError("timestamp mismatch seq=%s" % frame["seq"])
            yield message, frame
        if any(pending.values()):
            raise ValueError("MF4 is missing ingress frames")
    finally:
        reader.stop()


def verify_sealed_mf4(mf4_path, frames):
    """Verify every payload, flag, identifier, channel and capture time."""
    for _message, _frame in matched_mf4_frames(mf4_path, frames):
        pass
    return True


def _write_ingress_sidecar(path, closed_path, frames):
    """Copy the exact closed JSONL bytes to a gzipped sealed sidecar, so the
    full per-frame provenance (seq/twall_ns/tcan/DLC/payload) survives even
    though the standard MF4 groups cannot store it. Returns the sha256 of
    the COMPRESSED sidecar file, matching what the uploader re-hashes."""
    import gzip
    tmp = path + ".tmp"
    with open(closed_path, "rb") as src, open(tmp, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw,
                           mtime=0) as gz:
            for chunk in iter(lambda: src.read(8 << 20), b""):
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


def finalize_segment(closed_path, sealed_dir, spool=None):
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
    the closed JSONL still on disk. Returns the MF4 path or None for empty."""
    import gzip
    from can.io.mf4 import MF4Writer

    stem = os.path.basename(closed_path)
    if not stem.endswith(".jsonl"):
        raise ValueError("not a segment: " + stem)
    stem = stem[:-len(".jsonl")]
    frames, torn = read_ingress_frames(closed_path)
    if frames is None:
        if spool is None:
            spool = os.path.dirname(os.path.dirname(closed_path))
        quarantine(spool, closed_path)
        minc("finalize_errors")
        raise ValueError("corrupt segment quarantined: " + stem)
    if not frames:
        os.unlink(closed_path)
        return None
    frames.sort(key=lambda f: f["twall_ns"])  # stable; writer needs ordered t
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
                _observe_window(frames, stem)
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
    # MF4Writer has no append mode: one writer per segment, stop() persists.
    # Write to a staging name WITHOUT a .mf4 suffix: asammdf save() appends
    # the suffix itself when missing, so passing stem.mf4.tmp would land on
    # disk as stem.mf4.tmp.mf4.
    stage = os.path.join(sealed_dir, stem + ".stage.tmp")
    writer = MF4Writer(stage)
    try:
        for f in frames:
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
        verify_sealed_mf4(stage, frames)
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
    sidecar_digest = _write_ingress_sidecar(sidecar, closed_path, frames)
    _write_manifest(manifest_path, frames, stem, capture_id, torn,
                    mf4_digest, ingress_digest, sidecar_digest)
    _fsync_dir(sealed_dir)
    _observe_window(frames, stem)
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
