#!/usr/bin/env python3
"""S3 logical calls and observed stream consumption, without extra S3 I/O.

HTTP attempts count before-send events; retries count additional sends in the
same call, NOT needs-retry callbacks (which also run on successful responses).
PUT accounting wraps the prepared request after signing/checksum pre-reads.
Counts are bytes supplied to the HTTP transport, including retransmission and
failed sends, not proof of bytes delivered to the peer or socket framing.
Without botocore events, stream reads are logical consumption and wire coverage
is explicitly unknown. GET counts consumed response bytes, including delimiters
and bytes returned by the raw stream before SDK length/checksum validation fails.
COPY is server-side, not egress; missing observed HEAD sizes remain unknown.
Checksums and all other boto arguments/responses are left untouched.

Reports are private atomic per-run snapshots. Rewriting a run replaces it rather
than adding its totals again. A bounded report directory is a retained-run lower
bound, NEVER evidence of complete billing-month or account coverage. No bucket,
key, endpoint, credential, or arbitrary operation labels enter exported metrics.
"""

import io
import json
import math
import os
import re
import stat
import tempfile
import threading
import time

COMPONENTS = ("backup", "can-backup", "raw")
_OPS = ("GetObject", "HeadObject", "PutObject", "CopyObject",
        "ListObjectsV2", "DeleteObjects", "DeleteObject")
_METHOD_OP = {"get_object": "GetObject", "head_object": "HeadObject",
              "put_object": "PutObject", "copy_object": "CopyObject",
              "list_objects_v2": "ListObjectsV2", "delete_objects": "DeleteObjects",
              "delete_object": "DeleteObject"}
_BYTE_KINDS = ("get", "put", "copy")
_RUN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_HEAD_CACHE_CAP = 4096
_REPORT_MAX_FILES = 64
_REPORT_MAX_BYTES = 65536


def _is_control(key):
    if not isinstance(key, str) or not key:
        return False
    base = key.rsplit("/", 1)[-1]
    return (base in ("COMPLETE", "RETIRING", "manifest.json")
            or base.endswith(".sha256")
            or key.startswith("refs/") or "/refs/" in key
            or key.startswith("_locks/") or "/_locks/" in key
            or ((key.startswith("_sst/") or "/_sst/" in key)
                and base.endswith(".json")))


class _CountingStream:
    """Observe reads at one layer; seeking/resetting never changes counters."""

    def __init__(self, stream, add, failed):
        self._stream = stream
        self._add = add
        self._failed = failed

    def _read(self, method, *args, **kwargs):
        try:
            value = getattr(self._stream, method)(*args, **kwargs)
        except Exception as exc:
            partial = getattr(exc, "partial", None)
            if isinstance(partial, bytes):
                self._add(len(partial))
            self._failed()
            raise
        self._add(value if method == "readinto" else len(value))
        return value

    def read(self, amt=None):
        return self._read("read") if amt is None else self._read("read", amt)

    def readinto(self, buffer):
        if hasattr(self._stream, "readinto"):
            return self._read("readinto", buffer)
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def read1(self, size=-1):
        return self._read("read1", size)

    def readall(self):
        return self._read("readall")

    def readline(self, size=-1):
        return self._read("readline", size)

    def readlines(self, hint=-1):
        try:
            lines = self._stream.readlines(hint)
        except Exception:
            self._failed()
            raise
        self._add(sum(map(len, lines)))
        return lines

    def __iter__(self):
        for value in self._stream:
            self._add(len(value))
            yield value

    def __getattr__(self, name):
        return getattr(self._stream, name)


class _CountingBody:
    """Preserve SDK validation while counting raw consumption exactly once."""

    def __init__(self, body, add, failed):
        self._body = body
        self._failed = failed
        raw = getattr(body, "_raw_stream", None)
        if raw is not None:
            body._raw_stream = _CountingStream(raw, add, failed)
            # SDK read/readinto may raise AFTER returning bytes from raw.
            self._reader = body
        else:
            self._reader = _CountingStream(body, add, failed)

    def read(self, amt=None):
        try:
            return self._reader.read() if amt is None else self._reader.read(amt)
        except Exception:
            self._failed()
            raise

    def readinto(self, buffer):
        try:
            return self._reader.readinto(buffer)
        except Exception:
            self._failed()
            raise

    def iter_chunks(self, chunk_size=1024):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        while True:
            chunk = self.read(chunk_size)
            if not chunk:
                return
            yield chunk

    def iter_lines(self, chunk_size=1024, keepends=False):
        pending = b""
        for chunk in self.iter_chunks(chunk_size):
            lines = (pending + chunk).splitlines(True)
            for line in lines[:-1]:
                yield line.splitlines(keepends)[0]
            pending = lines[-1]
        if pending:
            yield pending.splitlines(keepends)[0]

    def __iter__(self):
        return self.iter_chunks()

    def __next__(self):
        chunk = self.read(1024)
        if not chunk:
            raise StopIteration
        return chunk

    next = __next__

    def readlines(self, hint=-1):
        # SDK readlines bypasses its read(), but the raw stream is counted.
        return self._reader.readlines(hint) if not hasattr(self._body, "_raw_stream") else self._body.readlines()

    def readline(self, size=-1):
        return self._reader.readline(size)

    def read1(self, size=-1):
        return self._reader.read1(size)

    def readall(self):
        return self.read()

    def close(self):
        return self._body.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return self.close()

    def __getattr__(self, name):
        return getattr(self._body, name)


def _copy_endpoints(kw):
    src = kw.get("CopySource")
    pair = None
    if isinstance(src, dict) and isinstance(src.get("Key"), str):
        pair = (src.get("Bucket"), src["Key"])
    elif isinstance(src, str) and "/" in src:
        bucket, _, key = src.partition("/")
        pair = (bucket or None, key or None)
    key = kw.get("Key")
    dst = (kw.get("Bucket"), key) if isinstance(key, str) and key else None
    if pair is not None and (not pair[0] or not pair[1]):
        pair = None
    return pair, dst


class CountingS3:
    """Delegated boto API; snapshots distinguish HTTP and logical observations."""

    def __init__(self, client, component):
        if component not in COMPONENTS:
            raise ValueError("unknown component")
        self._client = client
        self._component = component
        self._lock = threading.Lock()
        self._local = threading.local()
        self._requests = {}
        self._attempts = {}
        self._retries = {}
        self._errors = {}
        self._bytes = {kind: {"payload": 0, "control": 0} for kind in _BYTE_KINDS}
        self._unknown = {kind: 0 for kind in _BYTE_KINDS}
        self._head_sizes = {}
        self._pending_copy = {}
        self._http_events = self._hook_events(client)

    @property
    def component(self):
        return self._component

    def _hook_events(self, client):
        try:
            events = client.meta.events
        except AttributeError:
            return False
        for op in _OPS:
            events.register("before-send.s3.%s" % op, self._before_send)
        return True

    def _before_send(self, request, **kwargs):
        context = getattr(self._local, "call", None)
        if context is None:
            return
        op = context["op"]
        with self._lock:
            self._attempts[op] = self._attempts.get(op, 0) + 1
            if context["attempts"]:
                self._retries[op] = self._retries.get(op, 0) + 1
        context["attempts"] += 1
        if op != "PutObject":
            return
        body = request.body
        # The same prepared request is reused after botocore resets its stream.
        if isinstance(body, _CountingStream):
            body = body._stream
        if isinstance(body, (bytes, bytearray)):
            body = io.BytesIO(body)
        if body is None:
            return
        if not hasattr(body, "read"):
            context["unknown"]()
            return
        request.body = _CountingStream(body, context["add"], context["unknown"])

    def _callbacks(self, kind, key, op=None):
        slot = "control" if _is_control(key) else "payload"
        failed = [False]

        def add(count):
            with self._lock:
                self._bytes[kind][slot] += count

        def unknown():
            with self._lock:
                if not failed[0]:
                    failed[0] = True
                    self._unknown[kind] += 1
                    if op is not None:
                        self._errors[op] = self._errors.get(op, 0) + 1
        return add, unknown

    def __getattr__(self, name):
        target = getattr(self._client, name)
        if not callable(target) or name not in _METHOD_OP:
            return target

        def method(**kw):
            return self._call(_METHOD_OP[name], kw, target)
        return method

    def _call(self, op, kw, target):
        with self._lock:
            self._requests[op] = self._requests.get(op, 0) + 1
            self._retries.setdefault(op, 0)
            if not self._http_events:
                self._attempts[op] = self._attempts.get(op, 0) + 1
        add, unknown = self._callbacks("put", kw.get("Key"))
        context = {"op": op, "attempts": 0, "add": add, "unknown": unknown}
        previous = getattr(self._local, "call", None)
        self._local.call = context
        body = kw.get("Body")
        literal_size = None
        if op == "PutObject" and not self._http_events:
            # No transport events: these are logical consumed bytes, not wire.
            if hasattr(body, "read"):
                kw["Body"] = _CountingStream(body, add, unknown)
            elif isinstance(body, (bytes, bytearray)):
                literal_size = len(body)
            else:
                unknown()
        try:
            resp = target(**kw)
        except Exception:
            with self._lock:
                self._errors[op] = self._errors.get(op, 0) + 1
                if op == "GetObject":
                    self._unknown["get"] += 1
            if op == "PutObject" and (literal_size is not None or (self._http_events and not context["attempts"])):
                unknown()
            raise
        finally:
            self._local.call = previous
        if op == "GetObject" and isinstance(resp, dict):
            body = resp.get("Body")
            if body is not None:
                get_add, get_failed = self._callbacks("get", kw.get("Key"), op)
                resp["Body"] = _CountingBody(body, get_add, get_failed)
            else:
                with self._lock:
                    self._unknown["get"] += 1
        elif op == "PutObject":
            if literal_size is not None:
                add(literal_size)
            if self._http_events and not context["attempts"]:
                unknown()
        elif op == "CopyObject":
            self._count_copy(kw)
        elif op == "HeadObject":
            self._learn_head(kw, resp)
        return resp

    def _count_copy(self, kw):
        src, dst = _copy_endpoints(kw)
        slot = "control" if _is_control(kw.get("Key")) else "payload"
        with self._lock:
            if dst is not None:
                # Any overwrite invalidates an older unresolved destination.
                self._pending_copy.pop(dst, None)
            size = self._head_sizes.get(src) if src else None
            if size is not None:
                self._bytes["copy"][slot] += size
                return
            self._unknown["copy"] += 1
            if dst is not None:
                # ponytail: bounded pending HEAD map; dropped entries stay unknown.
                if len(self._pending_copy) >= _HEAD_CACHE_CAP:
                    self._pending_copy.clear()
                # A later HEAD cannot prove a previous overwritten copy's size.
                self._pending_copy[dst] = slot

    def _learn_head(self, kw, resp):
        pair = (kw.get("Bucket"), kw.get("Key"))
        size = resp.get("ContentLength") if isinstance(resp, dict) else None
        if not pair[0] or not isinstance(pair[1], str) or not _valid_uint(size):
            return
        with self._lock:
            if len(self._head_sizes) >= _HEAD_CACHE_CAP:
                self._head_sizes.clear()
            self._head_sizes[pair] = size
            slot = self._pending_copy.pop(pair, None)
            if slot is not None:
                self._bytes["copy"][slot] += size
                self._unknown["copy"] -= 1

    def snapshot(self):
        with self._lock:
            return {"component": self._component,
                    "requests": dict(self._requests), "attempts": dict(self._attempts),
                    "retries": dict(self._retries), "errors": dict(self._errors),
                    "bytes": {kind: dict(leg) for kind, leg in self._bytes.items()},
                    "unknown": dict(self._unknown),
                    "measurement": {"attempts": "http" if self._http_events else "logical",
                                    "put": "request-stream" if self._http_events else "logical-consumed"}}

    def mark(self):
        return self.snapshot()

    def io_since(self, mark):
        cur = self.snapshot()
        if not isinstance(mark, dict):
            mark = {}
        out = {"component": self._component, "measurement": cur["measurement"]}
        for key in ("requests", "attempts", "retries", "errors", "unknown"):
            base = mark.get(key, {})
            if not isinstance(base, dict):
                base = {}
            out[key] = {name: max(0, count - base.get(name, 0))
                        for name, count in cur[key].items()}
        out["bytes"] = {kind: {slot: max(0, count - mark.get("bytes", {}).get(kind, {}).get(slot, 0))
                               for slot, count in leg.items()}
                        for kind, leg in cur["bytes"].items()}
        return out

    def render_metrics(self):
        snap = self.snapshot()
        comp = snap["component"]
        lines = []
        for section in ("requests", "attempts", "retries", "errors"):
            for op in sorted(snap["requests"]):
                lines.append('datalake_s3_client_%s_total{component="%s",operation="%s"} %d'
                             % (section, comp, op, snap[section].get(op, 0)))
        for kind, name in (("get", "read"), ("put", "write"), ("copy", "copy")):
            lines.append('datalake_s3_client_%s_bytes_total{component="%s"} %d'
                         % (name, comp, sum(snap["bytes"][kind].values())))
            lines.append('datalake_s3_client_unknown_byte_operations{component="%s",kind="%s"} %d'
                         % (comp, kind, snap["unknown"][kind]))
        lines.append('datalake_s3_client_http_attempts_known{component="%s"} %d'
                     % (comp, self._http_events))
        lines.append('datalake_s3_client_write_transport_observed{component="%s"} %d'
                     % (comp, self._http_events))
        return "".join(line + "\n" for line in lines)


def _valid_uint(value):
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 2**63 - 1


def _valid_timestamp(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value > 0)


def valid_snapshot(obj):
    if not isinstance(obj, dict) or obj.get("component") not in COMPONENTS:
        return None
    out = {"component": obj["component"]}
    for key in ("requests", "attempts", "retries", "errors"):
        section = obj.get(key)
        if not isinstance(section, dict) or any(op not in _OPS or not _valid_uint(count) for op, count in section.items()):
            return None
        out[key] = dict(section)
    section = obj.get("bytes")
    if not isinstance(section, dict):
        return None
    out["bytes"] = {}
    for kind in _BYTE_KINDS:
        leg = section.get(kind)
        if not isinstance(leg, dict) or any(not _valid_uint(leg.get(slot)) for slot in ("payload", "control")):
            return None
        out["bytes"][kind] = {slot: leg[slot] for slot in ("payload", "control")}
    unknown = obj.get("unknown")
    measurement = obj.get("measurement")
    if (not isinstance(unknown, dict) or any(not _valid_uint(unknown.get(kind)) for kind in _BYTE_KINDS)
            or not isinstance(measurement, dict)
            or measurement.get("attempts") not in ("http", "logical")
            or measurement.get("put") not in ("request-stream", "logical-consumed")):
        return None
    out["unknown"] = {kind: unknown[kind] for kind in _BYTE_KINDS}
    out["measurement"] = {key: measurement[key] for key in ("attempts", "put")}
    return out


def _report_name(component, run_id):
    return "s3-cost-%s-%s.json" % (component, run_id)


def persist_cost(ops_dir, component, run_id, snapshot):
    """Atomically replace one run's snapshot; never add an existing run twice."""
    if component not in COMPONENTS or not isinstance(run_id, str) or not _RUN_RE.fullmatch(run_id):
        raise ValueError("invalid report identity")
    clean = valid_snapshot(snapshot)
    if clean is None or clean["component"] != component:
        raise ValueError("invalid snapshot or component mismatch")
    os.makedirs(ops_dir, exist_ok=True)
    path = os.path.join(ops_dir, _report_name(component, run_id))
    payload = json.dumps({"version": 1, "component": component, "run_id": run_id,
                          "captured_at": time.time(), "snapshot": clean}, sort_keys=True).encode("utf-8")
    fd, tmp = tempfile.mkstemp(dir=ops_dir, prefix=".s3-cost-tmp-")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)  # mkstemp is already 0600; replaces, never follows a symlink.
        directory_fd = os.open(ops_dir, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
    return path


def load_cost_reports(ops_dir, max_files=_REPORT_MAX_FILES, max_bytes=_REPORT_MAX_BYTES, now=None):
    """Bounded retained-run totals and explicit incomplete coverage metadata.

    Missing dirs, invalid/oversized files and truncation are never full coverage.
    Validated report timestamps (not mutable mtimes) drive freshness. Historical
    reports lacking measurement provenance are invalid rather than fake wire I/O.
    """
    if not _valid_uint(max_files) or max_files == 0 or not _valid_uint(max_bytes) or max_bytes == 0:
        raise ValueError("positive report limits required")
    now = time.time() if now is None else now
    result = {"totals": {}, "valid": 0, "invalid": 0, "truncated": False,
              "configured": False, "newest_timestamp": None,
              "oldest_timestamp": None, "complete": False}
    try:
        names = sorted(name for name in os.listdir(ops_dir)
                       if name.startswith("s3-cost-") and name.endswith(".json"))
    except FileNotFoundError:
        return result
    except OSError:
        result["invalid"] = 1
        return result
    result["configured"] = True
    result["truncated"] = len(names) > max_files
    result["invalid"] = max(0, len(names) - max_files)
    for name in names[:max_files]:
        try:
            fd = os.open(os.path.join(ops_dir, name), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > max_bytes:
                    raise ValueError("invalid report file")
                data = handle.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError("oversize report")
            report = json.loads(data)
            if not isinstance(report, dict):
                raise ValueError("invalid report shape")
            clean = valid_snapshot(report.get("snapshot"))
            stamp = report.get("captured_at")
            run_id = report.get("run_id")
            if (report.get("version") != 1 or clean is None
                    or report.get("component") != clean["component"]
                    or not isinstance(run_id, str) or not _RUN_RE.fullmatch(run_id)
                    or _report_name(clean["component"], run_id) != name
                    or not _valid_timestamp(stamp) or stamp > now):
                raise ValueError("invalid report identity/provenance")
        except (OSError, ValueError, UnicodeError, TypeError):
            result["invalid"] += 1
            continue
        agg = result["totals"].setdefault(clean["component"],
              {"requests": {}, "attempts": {}, "retries": {}, "errors": {},
               "read_bytes": 0, "write_bytes": 0, "copy_bytes": 0,
               "unknown": {kind: 0 for kind in _BYTE_KINDS}, "http_observed": True})
        for section in ("requests", "attempts", "retries", "errors"):
            for op, count in clean[section].items():
                agg[section][op] = agg[section].get(op, 0) + count
        for kind, field in (("get", "read_bytes"), ("put", "write_bytes"), ("copy", "copy_bytes")):
            agg[field] += sum(clean["bytes"][kind].values())
            agg["unknown"][kind] += clean["unknown"][kind]
        agg["http_observed"] &= clean["measurement"]["attempts"] == "http"
        result["valid"] += 1
        result["newest_timestamp"] = max(stamp, result["newest_timestamp"] or stamp)
        result["oldest_timestamp"] = min(stamp, result["oldest_timestamp"] or stamp)
    return result
