#!/usr/bin/env python3
"""Loopback S3 HTTP boundary meter (stdlib only, no dependencies).

Reverse-proxies Greptime's S3 traffic to the real object store while
preserving the SigV4-signed Host header, path and query string exactly.
MinIO accepts path-style requests regardless of Host, so forwarding the
original Host keeps the client signature valid end to end.

Bounded streaming: request and response bodies are relayed in fixed-size
chunks and never buffered whole, so multi-MiB SSTs stay bounded. Framing
is Content-Length when declared, chunked otherwise; HEAD never carries a
body. Truncated or failed transfers record the exact bytes actually moved
and close the connection to stay frame-safe.

Attempts come from the real `amz-sdk-request: attempt=N` header. Each wire
request counts 1 in op_requests; a request with attempt>1 counts one
observed retry. An absent/garbage header means attempts=0 (unknown),
tallied in unknown_attempts, never invented.

COPY bytes are server-side, not egress: the request body length is never
used. copy_bytes is folded in only when the explicit copy_size_lookup
hook returns a known source size; otherwise the entry keeps
copy_bytes=None and copy_unknown+=1.

4xx responses are attempted client errors (client_errors +
client_error_bytes) and 5xx/upstream/truncated/unsupported are errors
(errors + error_bytes): both are an additional breakdown of the same
moved bytes, which always stay in read/write/copy/payload/control totals.

Optional manifest/_last_checkpoint GET 404s require a complete, bounded
S3 Error document saying NoSuchKey. Bodyless HEAD 404s are expected only
for the source-proven region/.dropping existence probe. Both still count
in client_errors, client_error_bytes and all cost totals. Error bodies and
object paths are never retained; malformed/other misses stay unexpected.

Cold-cache evidence is narrower: sst_get_requests/sst_read_bytes count
only completed successful 2xx GETs with a positive body for paths ending
in .parquet (native Greptime SST) or .sst (synthetic fixtures). The entry
keeps only object_kind, never the signed path, query or object name.

AWS-chunked / streaming-SigV4 / trailer body modes are NOT silently
rewritten (that would break the signature): they fail safe with 502 and
an `unsupported` marker, still counted.

Only used against isolated loopback MinIO, or as an in-Compose sidecar
with an explicit --allow-non-loopback bind. Never pointed at production.

Snapshots wait for all active proxy requests to finish accounting before
returning a coherent counter/request view. The bounded wait fails explicitly
with TimeoutError (HTTP 503 on the status port), never an incomplete zero.

Sidecar deployment (lifecycle runner): mount this file read-only into a
pinned python container and run

  python3 s3_meter.py --target http://minio:9000 --bind 0.0.0.0 \\
      --port 8080 --status-port 8081 --allow-non-loopback

then poll GET http://<host>:<mapped-status-port>/snapshot (same shape as
S3Meter.snapshot()) and GET /health. Guaranteed helpers for sidecars:
classify_op, parse_attempts, S3Meter.snapshot counter keys.
"""

import argparse
import http.client
import http.server
import json
import re
import socket
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET

_CHUNK = 65536
_ERROR_BODY_LIMIT = 65536
_LOOPBACKS = {"127.0.0.1", "::1", "localhost"}

# AWS SDKs send `amz-sdk-request: attempt=1; max=5` on every try, including
# retries, so attempts are observed, not inferred. Absent/garbage = 0
# (unknown), never invented as 1.
_ATTEMPT_RE = re.compile(r"attempt=(\d+)")


def classify_op(method, query, headers):
    """Map HTTP method + query string + headers to an S3 operation class."""
    if method == "PUT" and "x-amz-copy-source" in {k.lower() for k in headers}:
        return "COPY"
    qs = urllib.parse.parse_qs(query, keep_blank_values=True)
    if method == "GET" and ("list-type" in qs or "listType" in qs):
        return "LIST"
    if method == "GET" and "uploads" in qs:
        return "LIST_UPLOADS"
    if method == "GET" and "uploadId" in qs:
        return "LIST_PARTS"
    if method == "POST" and "uploads" in qs:
        return "CREATE_MP"
    if method == "PUT" and "uploadId" in qs and "partNumber" in qs:
        return "UPLOAD_PART"
    if method == "POST" and "uploadId" in qs:
        return "COMPLETE_MP"
    if method == "DELETE" and "uploadId" in qs:
        return "ABORT_MP"
    return {"GET": "GET", "PUT": "PUT", "HEAD": "HEAD",
            "DELETE": "DELETE", "POST": "POST"}.get(method, method)


def classify_object(path):
    """Retain only a non-sensitive object class, never an object selector."""
    if path.endswith((".parquet", ".sst")):
        return "sst"
    parts = path.split("/")
    if (path.startswith("/") and len(parts) >= 4
            and all(p and p not in (".", "..") for p in parts[1:])):
        if parts[-2:] == ["manifest", "_last_checkpoint"]:
            return "manifest_checkpoint_discovery"
        if parts[-1] == ".dropping":
            return "drop_marker"
    return "control"


def _is_missing_key(body):
    """Fail closed on malformed/oversized XML; never persist the error body."""
    if not body or len(body) > _ERROR_BODY_LIMIT:
        return False
    try:
        text = body.decode("utf-8")
        if "\x00" in text or "<!DOCTYPE" in text or "<!ENTITY" in text:
            return False
        error = ET.fromstring(text)
    except (ValueError, ET.ParseError):
        return False
    codes = error.findall("Code")
    return (error.tag == "Error" and len(codes) == 1
            and len(codes[0]) == 0 and codes[0].text == "NoSuchKey")


_PAYLOAD_OPS = ("GET", "PUT", "UPLOAD_PART", "COPY")

_HOP_REQ = {"connection", "keep-alive", "proxy-connection", "transfer-encoding",
            "upgrade", "proxy-authenticate", "te", "trailer"}
_HOP_RESP = {"connection", "keep-alive", "proxy-connection", "transfer-encoding",
             "upgrade", "trailer"}


def parse_attempts(headers):
    """Observed SDK attempt number; 0 when the header is absent (unknown)."""
    m = _ATTEMPT_RE.search(headers.get("amz-sdk-request", ""))
    if not m:
        m = _ATTEMPT_RE.search(headers.get("Amz-Sdk-Request", ""))
    if not m:
        return 0
    try:
        return max(1, int(m.group(1)))
    except ValueError:
        return 0


def _unsupported_body(lowered):
    """Reason string when the body mode must fail safe, else None.

    AWS-chunked / streaming-SigV4 / trailer modes sign the wire framing
    itself, so re-framing would break SigV4. Never silently mutate them.
    """
    ce = lowered.get("content-encoding", "").lower()
    if "aws-chunk" in ce:
        return "aws-chunked content-encoding"
    te = lowered.get("transfer-encoding", "").lower()
    if "aws-chunk" in te:
        return "aws-chunked transfer-encoding"
    if lowered.get("x-amz-content-sha256", "").startswith("STREAMING-"):
        return "streaming sigv4 payload"
    if "x-amz-decoded-content-length" in lowered:
        return "streaming decoded length"
    if "x-amz-trailer" in lowered or "trailer" in lowered:
        return "trailer checksum"
    return None


def _relay_sized(rfile, send, want):
    """Stream exactly `want` bytes; (sent, truncated), never whole-buffered."""
    sent = 0
    while sent < want:
        data = rfile.read(min(_CHUNK, want - sent))
        if not data:
            return sent, True
        try:
            send(data)
        except OSError:
            return sent, True
        sent += len(data)
    return sent, False

def _relay_chunked(rfile, send):
    """Re-chunk a plain chunked body; (bytes, truncated), bounded memory."""
    total = 0
    while True:
        line = rfile.readline(8192)
        if not line:
            return total, True
        line = line.strip()
        if not line:
            return total, True
        try:
            size = int(line.split(b";")[0], 16)
        except ValueError:
            return total, True
        if size == 0:
            while True:
                tail = rfile.readline(8192)
                if not tail or tail in (b"\r\n", b"\n"):
                    break
            try:
                send(b"0\r\n\r\n")
            except OSError:
                return total, True
            return total, False
        try:
            send(b"%X\r\n" % size)
        except OSError:
            return total, True
        remaining = size
        while remaining > 0:
            data = rfile.read(min(_CHUNK, remaining))
            if not data:
                return total, True
            try:
                send(data)
            except OSError:
                return total, True
            total += len(data)
            remaining -= len(data)
        if rfile.read(2) != b"\r\n":
            return total, True
        try:
            send(b"\r\n")
        except OSError:
            return total, True

class S3Meter:
    """Threaded loopback proxy metering S3 boundary traffic."""

    def __init__(self, target_endpoint, bind="127.0.0.1", port=0,
                 status_port=None, status_bind=None, copy_size_lookup=None,
                 allow_non_loopback=False):
        parts = urllib.parse.urlsplit(target_endpoint)
        if parts.scheme not in ("http", "https"):
            raise ValueError("target must be http(s)")
        if not parts.hostname:
            raise ValueError("target needs a host")
        if parts.username or parts.password or parts.query or parts.fragment \
                or parts.path not in ("", "/"):
            raise ValueError("target must be a credential-free origin")
        if parts.hostname not in _LOOPBACKS and not (
                allow_non_loopback and parts.hostname == "minio"):
            raise ValueError("target must be loopback or the owned MinIO sidecar")
        if bind not in _LOOPBACKS and not allow_non_loopback:
            raise ValueError(
                "refusing non-loopback bind %r without allow_non_loopback"
                % (bind,))
        self.target_host = parts.hostname
        self.target_port = parts.port or (443 if parts.scheme == "https" else 80)
        self.target_ssl = parts.scheme == "https"
        self.bind = bind
        self.port = port
        self.status_port = status_port
        self.status_bind = status_bind or bind
        if status_port is not None and self.status_bind not in _LOOPBACKS \
                and not allow_non_loopback:
            raise ValueError(
                "refusing non-loopback status bind %r without "
                "allow_non_loopback" % (self.status_bind,))
        # copy_size_lookup(copy_source_value) -> int|None: explicit known
        # source size for COPY (server-side, never the request body).
        self.copy_size_lookup = copy_size_lookup
        self._lock = threading.Lock()
        self._idle = threading.Condition(self._lock)
        self._active_requests = 0
        self.requests = []
        self.dropped_requests = 0
        self._client_error_breakdown = {}
        self.counters = {"requests": 0, "retries": 0, "unknown_attempts": 0,
                         "errors": 0, "client_errors": 0,
                         "expected_not_found_requests": 0,
                         "expected_not_found_bytes": 0,
                         "read_bytes": 0, "write_bytes": 0, "copy_bytes": 0,
                         "sst_get_requests": 0, "sst_read_bytes": 0,
                         "copy_unknown": 0,
                         "payload_bytes": 0, "control_bytes": 0,
                         "error_bytes": 0, "client_error_bytes": 0,
                         "truncated": 0, "unsupported": 0}
        self._server = None
        self._thread = None
        self._status_server = None
        self._status_thread = None

    @property
    def endpoint(self):
        return "http://%s:%d" % (self.bind, self.port)

    @property
    def status_endpoint(self):
        if self.status_port is None:
            return None
        return "http://%s:%d" % (self.status_bind, self.status_port)

    def _record(self, entry):
        with self._lock:
            status = entry.get("status")
            entry["success"] = (status is not None and 200 <= status < 300
                                and not entry.get("truncated")
                                and not entry.get("unsupported"))
            sst_get = (entry["op"] == "GET"
                       and entry.get("object_kind") == "sst"
                       and entry["success"] and entry["resp_bytes"] > 0)
            entry["sst_get_requests"] = int(sst_get)
            entry["sst_read_bytes"] = entry["resp_bytes"] if sst_get else 0
            optional_miss = (
                (entry["op"] == "GET"
                 and entry.get("object_kind") == "manifest_checkpoint_discovery"
                 and entry.get("expected_not_found") is True)
                or (entry["op"] == "HEAD"
                    and entry.get("object_kind") == "drop_marker"
                    and entry["req_bytes"] == 0 and entry["resp_bytes"] == 0))
            entry["expected_not_found"] = (
                status == 404 and optional_miss
                and not entry.get("truncated") and not entry.get("unsupported"))
            self.requests.append(entry)
            if len(self.requests) > 100000:
                # Bounded memory on 10M-row runs; totals already folded in.
                del self.requests[:10000]
                self.dropped_requests += 10000
            c = self.counters
            c["sst_get_requests"] += entry["sst_get_requests"]
            c["sst_read_bytes"] += entry["sst_read_bytes"]
            c["requests"] += 1
            if entry["attempts"] > 1:
                # Each observed attempt>1 is one retry; attempt=1/2/3 over
                # the wire is 2 retries, not 0+1+2=3.
                c["retries"] += 1
            elif entry["attempts"] == 0:
                c["unknown_attempts"] += 1
            moved = entry["req_bytes"] + entry["resp_bytes"]
            # Totals count actual streamed bytes for every attempt,
            # whatever the outcome: a GET failing at the last byte still
            # spent egress, and each retry moved bytes again.
            if entry["op"] == "GET":
                c["read_bytes"] += entry["resp_bytes"]
            elif entry["op"] in ("PUT", "UPLOAD_PART", "POST",
                                 "COMPLETE_MP", "CREATE_MP"):
                c["write_bytes"] += entry["req_bytes"]
            elif entry["op"] == "COPY":
                # Attempted server-side size, even on failure; unknown
                # sizes stay visible as a count, never a fake zero.
                if entry.get("copy_known"):
                    c["copy_bytes"] += entry["copy_bytes"] or 0
                else:
                    c["copy_unknown"] += 1
            if entry["op"] in _PAYLOAD_OPS:
                c["payload_bytes"] += moved
            else:
                c["control_bytes"] += moved
            # Error tallies are an additional breakdown of the same bytes,
            # never an exclusive alternative bucket.
            if status is None or status >= 500 \
                    or entry.get("truncated") or entry.get("unsupported"):
                c["errors"] += 1
                c["error_bytes"] += moved
            elif status >= 400:
                c["client_errors"] += 1
                c["client_error_bytes"] += moved
                expected = entry["expected_not_found"]
                if expected:
                    c["expected_not_found_requests"] += 1
                    c["expected_not_found_bytes"] += moved
                key = (status, entry["op"], entry["object_kind"], expected)
                group = self._client_error_breakdown.setdefault(key, {
                    "status": status, "op": entry["op"],
                    "object_kind": entry["object_kind"],
                    "expected_not_found": expected, "requests": 0, "bytes": 0})
                group["requests"] += 1
                group["bytes"] += moved
            if entry.get("truncated"):
                c["truncated"] += 1
            if entry.get("unsupported"):
                c["unsupported"] += 1

    def _connect(self):
        if self.target_ssl:
            import ssl
            ctx = ssl.create_default_context()
            return http.client.HTTPSConnection(self.target_host, self.target_port,
                                               context=ctx, timeout=60)
        return http.client.HTTPConnection(self.target_host, self.target_port,
                                          timeout=60)

    def start(self):
        meter = self

        class Handler(http.server.BaseHTTPRequestHandler):
            server_version = "S3Meter/1"
            protocol_version = "HTTP/1.1"

            def _fail(self, entry, code, msg):
                meter._record(entry)
                self.close_connection = True
                self.send_error(code, msg)

            def _proxy(self):
                with meter._idle:
                    meter._active_requests += 1
                try:
                    self._relay()
                finally:
                    with meter._idle:
                        meter._active_requests -= 1
                        meter._idle.notify_all()

            def _relay(self):
                method = self.command
                selector = self.path  # path + query preserved verbatim
                parsed = urllib.parse.urlsplit(selector)
                in_headers = {k: v for k, v in self.headers.items()}
                lowered = {k.lower(): v for k, v in in_headers.items()}
                op = classify_op(method, parsed.query, in_headers)
                attempts = parse_attempts(in_headers)
                started = time.time()
                base = {"ts": started, "method": method,
                        "op": op,
                        "object_kind": classify_object(parsed.path),
                        "expected_not_found": False,
                        "req_bytes": 0, "resp_bytes": 0, "status": None,
                        "range": in_headers.get("Range"),
                        "attempts": attempts,
                        "retries": 1 if attempts > 1 else 0,
                        "truncated": False, "unsupported": None,
                        "copy_bytes": None, "copy_known": False}
                if op == "COPY" and meter.copy_size_lookup is not None:
                    try:
                        size = meter.copy_size_lookup(
                            lowered.get("x-amz-copy-source", ""))
                    except Exception:
                        size = None
                    if isinstance(size, int) and size >= 0 and not isinstance(
                            size, bool):
                        base["copy_bytes"] = size
                        base["copy_known"] = True
                if method != "HEAD":
                    unsupported = _unsupported_body(lowered)
                    if unsupported is not None:
                        base["unsupported"] = unsupported
                        base["status"] = 502
                        self._fail(base, 502,
                                   "unsupported body mode: %s" % unsupported)
                        return
                fwd = [(k, v) for k, v in in_headers.items()
                       if k.lower() not in _HOP_REQ]
                if "host" not in {k.lower() for k, _ in fwd}:
                    # Host header preserved exactly: client-signed value
                    # passes through untouched so the store validates SigV4.
                    fwd.append(("Host", self.headers.get("Host", "")))
                # Request framing: HEAD never has a body; Content-Length
                # streams sized; plain chunked re-chunks; else bodiless.
                content_length = None
                chunked_req = False
                if method != "HEAD":
                    if "content-length" in lowered:
                        try:
                            content_length = int(
                                lowered["content-length"])
                        except ValueError:
                            content_length = None
                        if content_length is not None and content_length < 0:
                            content_length = None
                    elif "chunked" in lowered.get("transfer-encoding", "") \
                            .lower():
                        chunked_req = True
                status = None
                req_bytes = 0
                truncated = False
                resp_bytes = 0
                resp_headers = []
                error_body = None
                # Answer 100-continue ourselves (Expect stripped from the
                # upstream forward): SDKs wait for it before sending the
                # body, and the meter must not deadlock them.
                fwd = [(k, v) for k, v in fwd if k.lower() != "expect"]
                if lowered.get("expect", "").lower() == "100-continue" \
                        and method != "HEAD":
                    try:
                        self.wfile.write(b"HTTP/1.1 100 Continue\r\n\r\n")
                        self.wfile.flush()
                    except OSError:
                        base.update(status=None)
                        meter._record(base)
                        self.close_connection = True
                        return
                try:
                    conn = meter._connect()
                    try:
                        conn.putrequest(method, selector, skip_host=True)
                        for k, v in fwd:
                            kl = k.lower()
                            if method == "HEAD" and kl in (
                                    "content-length", "transfer-encoding"):
                                continue
                            if chunked_req and kl == "content-length":
                                continue
                            conn.putheader(k, v)
                        if chunked_req:
                            conn.putheader("Transfer-Encoding", "chunked")
                        conn.endheaders()
                        if method != "HEAD":
                            if content_length:
                                req_bytes, truncated = _relay_sized(
                                    self.rfile, conn.send, content_length)
                            elif chunked_req:
                                req_bytes, truncated = _relay_chunked(
                                    self.rfile, conn.send)
                        if truncated:
                            base.update(req_bytes=req_bytes, status=None,
                                        truncated=True)
                            self._fail(base, 502, "truncated request body")
                            return
                        try:
                            resp = conn.getresponse()
                        except Exception:
                            resp = None
                        if resp is None:
                            base.update(req_bytes=req_bytes, status=None)
                            self._fail(base, 502, "meter upstream failure")
                            return
                        status = resp.status
                        if (status == 404 and op == "GET" and base["object_kind"]
                                == "manifest_checkpoint_discovery"):
                            error_body = bytearray()
                        resp_headers = [(k, v) for k, v in resp.getheaders()
                                        if k.lower() not in _HOP_RESP]
                        origin_clen = resp.getheader("Content-Length")
                        try:
                            want = int(origin_clen) \
                                if origin_clen is not None else None
                        except ValueError:
                            want = None
                        if want is not None and want < 0:
                            want = None
                        no_body = method == "HEAD" or status in (204, 304)
                        self.send_response(status)
                        for k, v in resp_headers:
                            if k.lower() not in ("content-length",
                                                 "transfer-encoding"):
                                self.send_header(k, v)
                        if no_body:
                            if want is not None:
                                self.send_header("Content-Length", str(want))
                            self.end_headers()
                            resp_bytes = 0
                        elif want is not None:
                            self.send_header("Content-Length", str(want))
                            self.end_headers()
                            got = 0
                            while got < want:
                                try:
                                    data = resp.read(
                                        min(_CHUNK, want - got))
                                except Exception:
                                    truncated = True
                                    break
                                if not data:
                                    truncated = True
                                    break
                                try:
                                    self.wfile.write(data)
                                except OSError:
                                    truncated = True
                                    break
                                got += len(data)
                                if error_body is not None:
                                    if len(error_body) + len(data) > _ERROR_BODY_LIMIT:
                                        error_body = None
                                    else:
                                        error_body.extend(data)
                            resp_bytes = got
                            if truncated:
                                # Declared more than arrived: stay frame-safe.
                                self.close_connection = True
                        else:
                            self.send_header("Transfer-Encoding", "chunked")
                            self.end_headers()
                            while True:
                                try:
                                    data = resp.read(_CHUNK)
                                except Exception:
                                    truncated = True
                                    break
                                if not data:
                                    break
                                try:
                                    self.wfile.write(b"%X\r\n" % len(data))
                                    self.wfile.write(data)
                                    self.wfile.write(b"\r\n")
                                except OSError:
                                    truncated = True
                                    break
                                resp_bytes += len(data)
                                if error_body is not None:
                                    if len(error_body) + len(data) > _ERROR_BODY_LIMIT:
                                        error_body = None
                                    else:
                                        error_body.extend(data)
                            if truncated:
                                # Never emit a clean terminator after a
                                # truncated stream: just hang up frame-safe.
                                self.close_connection = True
                            else:
                                try:
                                    self.wfile.write(b"0\r\n\r\n")
                                except OSError:
                                    truncated = True
                    finally:
                        conn.close()
                except Exception:
                    status = None
                base.update(req_bytes=req_bytes, resp_bytes=resp_bytes,
                            status=status, truncated=truncated)
                if status == 404 and not truncated and error_body is not None:
                    base["expected_not_found"] = _is_missing_key(error_body)
                meter._record(base)
                if status is None:
                    self.close_connection = True
                    self.send_error(502, "meter upstream failure")

            do_GET = _proxy
            do_PUT = _proxy
            do_HEAD = _proxy
            do_POST = _proxy
            do_DELETE = _proxy

            def log_message(self, *a):
                pass

        # Allow immediate rebinding across current/baseline phases.
        class Server(http.server.ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._server = Server((self.bind, self.port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        kwargs={"poll_interval": 0.05},
                                        daemon=True)
        self._thread.start()
        if self.status_port is not None:
            status_meter = self

            class StatusHandler(http.server.BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"

                def do_GET(self):
                    sel = urllib.parse.urlsplit(self.path).path
                    if sel == "/snapshot":
                        try:
                            body = json.dumps(
                                status_meter.snapshot()).encode()
                        except TimeoutError as exc:
                            self.send_error(503, str(exc))
                            return
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                    elif sel == "/health":
                        body = b'{"status":"ok"}'
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                    else:
                        self.send_error(404, "unknown status path")

                def log_message(self, *a):
                    pass

            self._status_server = Server((self.status_bind, self.status_port),
                                         StatusHandler)
            self.status_port = self._status_server.server_address[1]
            self._status_thread = threading.Thread(
                target=self._status_server.serve_forever,
                kwargs={"poll_interval": 0.05}, daemon=True)
            self._status_thread.start()
        # Wait until the loopback port accepts connections.
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with socket.create_connection((self.bind, self.port), timeout=1):
                    return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("s3 meter failed to bind %s:%d" % (self.bind, self.port))

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=10)
            self._server = None
        if self._status_server is not None:
            self._status_server.shutdown()
            self._status_server.server_close()
            self._status_thread.join(timeout=10)
            self._status_server = None

    def op_counters(self):
        """Request counts by operation class (each wire attempt counts 1)."""
        with self._lock:
            agg = {}
            for r in self.requests:
                agg[r["op"]] = agg.get(r["op"], 0) + 1
            return agg

    def snapshot(self, timeout=60):
        """Return a quiescent view, or fail explicitly if transfers stay active."""
        deadline = time.monotonic() + timeout
        with self._idle:
            while self._active_requests:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "S3 meter snapshot incomplete: %d active requests"
                        % self._active_requests)
                self._idle.wait(remaining)
            reqs = [dict(r) for r in self.requests]
            counters = dict(self.counters)
            dropped = self.dropped_requests
            breakdown = [dict(group) for group in
                         self._client_error_breakdown.values()]
            ops = {}
            for r in reqs:
                ops[r["op"]] = ops.get(r["op"], 0) + 1
        return {"counters": counters, "op_requests": ops,
                "client_error_breakdown": breakdown,
                "requests": reqs[-5000:],
                "truncated_requests": max(0, len(reqs) - 5000) + dropped}


def main(argv=None):
    ap = argparse.ArgumentParser(description="isolated S3 boundary meter")
    ap.add_argument("--target", required=True,
                    help="origin to forward to, e.g. http://minio:9000")
    ap.add_argument("--bind", default="127.0.0.1",
                    help="proxy bind address (loopback unless "
                         "--allow-non-loopback)")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--status-bind", default=None,
                    help="snapshot endpoint bind (defaults to --bind)")
    ap.add_argument("--status-port", type=int, default=8081,
                    help="snapshot endpoint port")
    ap.add_argument("--allow-non-loopback", action="store_true",
                    help="required for any non-loopback bind (sidecar use)")
    args = ap.parse_args(argv)
    meter = S3Meter(args.target, bind=args.bind, port=args.port,
                    status_port=args.status_port,
                    status_bind=args.status_bind or args.bind,
                    allow_non_loopback=args.allow_non_loopback)
    meter.start()
    print("s3-meter ready proxy=%s status=%s origin=%s:%d"
          % (meter.endpoint, meter.status_endpoint, meter.target_host,
             meter.target_port), flush=True)
    try:
        meter._thread.join()
    except KeyboardInterrupt:
        pass
    finally:
        meter.stop()


if __name__ == "__main__":
    main()
