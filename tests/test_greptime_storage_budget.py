"""Greptime storage bench behavioral tests (no Docker, no copies, no wiring).

Covers observable behavior only: the S3 meter's operation classification,
Host/query/range/attempt accounting, bounded streaming byte counts
(Content-Length/chunked/HEAD), retry sequences, truncated bodies, large
streams, COPY server-side accounting, 4xx/5xx separation, SST-only cold
evidence, diagnostic privacy, sidecar status
endpoint, non-loopback guard, and cleanup; the bench's oracle math, query
count, scenario schema keys, relative regression gate (incl. budget
uncertainty + baseline-zero payload rule) and budget gate mapping. Heavy
lifecycle functions are never executed here; Main owns the native proof.
"""
import http.client
import json
import os
import socket
import sys
import tempfile
import threading
import tracemalloc
import unittest
from unittest.mock import patch

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

import tools.benchmark_greptime_storage as bench
from tools.s3_meter import S3Meter, classify_op, parse_attempts


class MeterClassifyTest(unittest.TestCase):
    def test_op_classes(self):
        self.assertEqual(classify_op("GET", "list-type=2", {}), "LIST")
        self.assertEqual(classify_op("GET", "", {}), "GET")
        self.assertEqual(classify_op("PUT", "", {"x-amz-copy-source": "b/k"}), "COPY")
        self.assertEqual(classify_op("PUT", "partNumber=1&uploadId=x", {}), "UPLOAD_PART")
        self.assertEqual(classify_op("POST", "uploadId=x", {}), "COMPLETE_MP")
        self.assertEqual(classify_op("HEAD", "", {}), "HEAD")

    def test_attempts_from_sdk_header(self):
        # Absent/garbage header is unknown (0), never invented as 1.
        self.assertEqual(parse_attempts({}), 0)
        self.assertEqual(
            parse_attempts({"amz-sdk-request": "attempt=3; max=5"}), 3)
        self.assertEqual(parse_attempts({"amz-sdk-request": "garbage"}), 0)


class MeterBehaviorTest(unittest.TestCase):
    """Live loopback proxy against a stub origin: proves Host/path/query
    preservation, range forwarding, bounded streaming bytes, statuses,
    attempts, truncation, COPY and error separation."""

    def setUp(self):
        self.seen = []
        seen = self.seen

        import http.server

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _read_req(self):
                # Stream without whole-body buffering; keep small bodies
                # for assertions, count large ones (bounded test peak).
                cap = 1 << 20
                kept = bytearray()
                total = 0

                def _feed(piece):
                    nonlocal total
                    total += len(piece)
                    if total <= cap:
                        kept.extend(piece)

                te = self.headers.get("Transfer-Encoding", "")
                if "chunked" in te.lower():
                    while True:
                        line = self.rfile.readline().strip()
                        size = int(line.split(b";")[0] or b"0", 16)
                        if size == 0:
                            self.rfile.readline()
                            break
                        left = size
                        while left > 0:
                            piece = self.rfile.read(min(65536, left))
                            if not piece:
                                break
                            _feed(piece)
                            left -= len(piece)
                        self.rfile.readline()
                else:
                    length = int(self.headers.get("Content-Length", "0") or 0)
                    left = length
                    while left > 0:
                        piece = self.rfile.read(min(65536, left))
                        if not piece:
                            break
                        _feed(piece)
                        left -= len(piece)
                if total > cap:
                    return b"<large:%d>" % total
                return bytes(kept)

            def _serve(self, head_only=False):
                body = self._read_req()
                seen.append({"method": self.command, "path": self.path,
                             "host": self.headers.get("Host"),
                             "range": self.headers.get("Range"),
                             "copy": self.headers.get("x-amz-copy-source"),
                             "body": body})
                if self.path.startswith("/probe/"):
                    code = "NoSuchKey"
                    status = 404
                    if "case=bucket" in self.path:
                        code = "NoSuchBucket"
                    elif "case=auth" in self.path:
                        code, status = "AccessDenied", 403
                    payload = ("<Error><Code>%s</Code><Key>private_xml_key</Key>"
                               "<RequestId>private_xml_request</RequestId></Error>"
                               % code).encode()
                    if "case=malformed" in self.path:
                        payload = b"<Error><Code>NoSuchKey"
                    elif "case=duplicate" in self.path:
                        payload = b"<Error><Code>NoSuchKey</Code><Code>AccessDenied</Code></Error>"
                    elif "case=oversized" in self.path:
                        payload = b"<Error><Code>NoSuchKey</Code><Padding>" + b"x" * 65536 + b"</Padding></Error>"
                    self.send_response(status)
                    if "case=truncated" in self.path:
                        self.send_header("Content-Length", str(len(payload) + 1))
                        self.end_headers()
                        if not head_only:
                            self.wfile.write(payload)
                        self.close_connection = True
                        return
                    if "case=chunked" in self.path:
                        self.send_header("Transfer-Encoding", "chunked")
                        self.end_headers()
                        if not head_only:
                            self.wfile.write(b"%X\r\n" % len(payload))
                            self.wfile.write(payload + b"\r\n0\r\n\r\n")
                        return
                elif self.path.startswith("/boom"):
                    payload = b"err"
                    self.send_response(500)
                elif self.path.startswith("/missing"):
                    payload = b"nope"
                    self.send_response(404)
                elif self.path.startswith("/short"):
                    # Lie about length then hang up: truncated origin body.
                    self.send_response(200)
                    self.send_header("Content-Length", "100")
                    self.end_headers()
                    if not head_only:
                        self.wfile.write(b"0123456789")
                    self.close_connection = True
                    return
                elif self.path.startswith("/chunked-resp"):
                    self.send_response(200)
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    if not head_only:
                        for piece in (b"hello-", b"range-", b"body"):
                            self.wfile.write(b"%X\r\n" % len(piece))
                            self.wfile.write(piece)
                            self.wfile.write(b"\r\n")
                        self.wfile.write(b"0\r\n\r\n")
                    return
                elif self.path.startswith("/range"):
                    payload = b"hello-range-body"[2:6]
                    self.send_response(206)
                    self.send_header("Content-Range", "bytes 2-5/16")
                elif self.path.startswith("/empty"):
                    payload = b""
                    self.send_response(200)
                else:
                    payload = b"hello-range-body"
                    self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if not head_only:
                    self.wfile.write(payload)

            def do_GET(self):
                self._serve()

            def do_PUT(self):
                self._serve()

            def do_HEAD(self):
                self._serve(head_only=True)

            def do_POST(self):
                self._serve()

            def log_message(self, *a):
                pass

        self.origin = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.origin.daemon_threads = True
        threading.Thread(target=self.origin.serve_forever,
                         kwargs={"poll_interval": 0.05}, daemon=True).start()
        port = self.origin.server_address[1]
        self.meter = S3Meter("http://127.0.0.1:%d" % port).start()

    def tearDown(self):
        self.meter.stop()
        self.origin.shutdown()
        self.origin.server_close()

    def _req(self, method, sel, body=b"", headers=None, chunked=False):
        c = http.client.HTTPConnection("127.0.0.1", self.meter.port, timeout=30)
        c.putrequest(method, sel, skip_host=True)
        c.putheader("Host", "signed-host.example")
        for k, v in (headers or {}).items():
            c.putheader(k, v)
        if chunked:
            c.putheader("Transfer-Encoding", "chunked")
            c.endheaders()
            for i in range(0, len(body), 16384):
                piece = body[i:i + 16384]
                c.send(b"%X\r\n" % len(piece))
                c.send(piece)
                c.send(b"\r\n")
            c.send(b"0\r\n\r\n")
        else:
            c.putheader("Content-Length", str(len(body)))
            c.endheaders(body if body else None)
        r = c.getresponse()
        status = r.status  # status line precedes any truncated body
        try:
            payload = r.read()
        except http.client.IncompleteRead as e:
            payload = e.partial
        except http.client.RemoteDisconnected:
            payload = b""
        c.close()
        return status, payload

    def test_signed_selector_is_forwarded_but_not_retained_in_diagnostics(self):
        selector = "/private_object_marker.parquet?X-Amz-Signature=private_signature_marker"
        status, _ = self._req(
            "GET", selector,
            headers={"Authorization": "private_authorization_marker",
                     "X-Amz-Security-Token": "private_token_marker"})
        self.assertEqual(status, 200)
        self.assertEqual(self.seen[-1]["path"], selector)
        diagnostics = json.dumps(self.meter.snapshot())
        self.assertNotIn("private_object_marker", diagnostics)
        self.assertNotIn("private_signature_marker", diagnostics)
        self.assertNotIn("private_authorization_marker", diagnostics)
        self.assertNotIn("private_token_marker", diagnostics)
        self.assertEqual(self.meter.snapshot()["counters"]["sst_get_requests"], 1)
        self.assertGreater(self.meter.snapshot()["counters"]["sst_read_bytes"], 0)

    def test_sst_range_has_successful_payload_evidence(self):
        before = self.meter.snapshot()["counters"]
        status, payload = self._req("GET", "/range.parquet",
                                    headers={"Range": "bytes=2-5"})
        self.assertEqual(status, 206)
        self.assertEqual(payload, b"llo-")
        self.assertEqual(self.seen[-1]["range"], "bytes=2-5")
        snap = self.meter.snapshot()
        entry = snap["requests"][-1]
        self.assertGreater(entry["ts"], 0)
        self.assertEqual(entry["status"], 206)
        self.assertEqual(entry["object_kind"], "sst")
        self.assertTrue(entry["success"])
        self.assertEqual(entry["sst_get_requests"], 1)
        self.assertEqual(entry["sst_read_bytes"], 4)
        self.assertEqual(snap["counters"]["sst_get_requests"] -
                         before["sst_get_requests"], 1)
        self.assertEqual(snap["counters"]["sst_read_bytes"] -
                         before["sst_read_bytes"], 4)
        self.assertEqual(snap["counters"]["read_bytes"], 4)
        # Synthetic SST fixtures use the same successful-body contract.
        self._req("GET", "/range.sst", headers={"Range": "bytes=2-5"})
        after = self.meter.snapshot()["counters"]
        self.assertEqual(after["sst_get_requests"], 2)
        self.assertEqual(after["sst_read_bytes"], 8)

    def test_metadata_errors_and_empty_bodies_cannot_prove_cold(self):
        cases = (
            ("GET", "/bucket/manifest.json", 200, "unknown", True),
            ("GET", "/bucket/manifest.json?key=object.parquet", 200, "unknown", True),
            ("GET", "/missing.parquet", 404, "sst", False),
            ("GET", "/boom.parquet", 500, "sst", False),
            ("HEAD", "/bucket/object.parquet", 200, "sst", True),
            ("PUT", "/bucket/object.parquet", 200, "sst", True),
            ("GET", "/empty.parquet", 200, "sst", True),
            ("GET", "/bucket/object.parquet?list-type=2", 200, "control", True),
        )
        for method, selector, expected_status, kind, success in cases:
            with self.subTest(method=method, selector=selector):
                status, _ = self._req(method, selector)
                self.assertEqual(status, expected_status)
                snap = self.meter.snapshot()
                entry = snap["requests"][-1]
                self.assertEqual(entry["object_kind"], kind)
                self.assertEqual(entry["success"], success)
                self.assertEqual(entry["sst_get_requests"], 0)
                self.assertEqual(entry["sst_read_bytes"], 0)
                self.assertEqual(snap["counters"]["sst_get_requests"], 0)
                self.assertEqual(snap["counters"]["sst_read_bytes"], 0)
        self.assertEqual(snap["counters"]["read_bytes"], sum(
            r["resp_bytes"] for r in snap["requests"] if r["op"] == "GET"))

    def test_complete_attribution_survives_request_log_rotation(self):
        cases = (
            ("/bucket/greptime-backup-20261015T010203.123456789Z-abcdef/"
             "greptime-backup-20261015T010203.123456789Z-abcdef.tar.gz",
             "backup_payload", "payload_read_bytes"),
            ("/bucket/greptime-backup-20261015T010203.123456789Z-abcdef/COMPLETE",
             "metadata", "metadata_read_bytes"),
            ("/bucket/greptime-backup-20261015T010203.123456789Z-abcdef/manifest.json",
             "metadata", "metadata_read_bytes"),
            ("/private_region/data.parquet", "sst", "payload_read_bytes"),
            ("/private_region/index.puffin", "index", "payload_read_bytes"),
            ("/bucket/_sst/" + "a" * 64, "backup_payload", "payload_read_bytes"),
            ("/bucket/_sst/not-a-digest", "unknown", "unknown_read_bytes"),
            ("/private_region/manifest/00000000000000000001.json",
             "metadata", "metadata_read_bytes"),
            ("/private_region/manifest/00000000000000000001.checkpoint.gz",
             "metadata", "metadata_read_bytes"),
            ("/private_region/manifest/_last_checkpoint",
             "manifest_checkpoint_discovery", "metadata_read_bytes"),
            ("/private_region/arbitrary.json", "unknown", "unknown_read_bytes"),
            ("/private_region/manifest/arbitrary.json", "unknown", "unknown_read_bytes"),
            ("/bucket?list-type=2&X-Amz-Signature=private_signature",
             "control", None),
        )
        expected_classes = {}
        expected_reads = dict.fromkeys(
            ("payload_read_bytes", "metadata_read_bytes", "unknown_read_bytes"), 0)
        with patch("tools.s3_meter._REQUEST_LOG_LIMIT", 3), patch(
                "tools.s3_meter._REQUEST_LOG_TRIM", 2):
            for selector, kind, read_counter in cases:
                _, body = self._req("GET", selector)
                snap = self.meter.snapshot()
                self.assertEqual(snap["requests"][-1]["object_kind"], kind)
                expected_classes[kind] = expected_classes.get(kind, 0) + len(body)
                if read_counter:
                    expected_reads[read_counter] += len(body)
        counters = snap["counters"]
        self.assertEqual(snap["response_bytes_by_object"], expected_classes)
        self.assertEqual(counters["response_bytes"], sum(expected_classes.values()))
        for name, size in expected_reads.items():
            self.assertEqual(counters[name], size)
        self.assertEqual(counters["read_bytes"], sum(expected_reads.values()))
        self.assertEqual(snap["op_requests"], {"GET": len(cases) - 1, "LIST": 1})
        self.assertEqual(self.meter.op_counters(), snap["op_requests"])
        self.assertGreater(snap["truncated_requests"], 0)
        self.assertLess(len(snap["requests"]), len(cases))
        encoded = json.dumps(snap)
        for secret in ("private_region", "private_signature", "arbitrary.json", "a" * 64):
            self.assertNotIn(secret, encoded)

    def test_payload_and_unknown_errors_partial_and_retries_are_cumulative(self):
        cases = (
            ("/short.puffin", "index", "payload_read_bytes", 200, 10),
            ("/boom.puffin", "index", "payload_read_bytes", 500, 3),
            ("/missing/_sst/" + "b" * 64,
             "backup_payload", "payload_read_bytes", 404, 4),
            ("/short-unknown", "unknown", "unknown_read_bytes", 200, 10),
            ("/boom/arbitrary.json", "unknown", "unknown_read_bytes", 500, 3),
        )
        expected = {"payload_read_bytes": 0, "unknown_read_bytes": 0}
        total = 0
        for selector, kind, counter, status, size in cases:
            for attempt in (1, 2):
                actual, body = self._req("GET", selector, headers={
                    "amz-sdk-request": "attempt=%d; max=2" % attempt})
                self.assertEqual((actual, len(body)), (status, size))
                entry = self.meter.snapshot()["requests"][-1]
                self.assertEqual(entry["object_kind"], kind)
                self.assertFalse(entry["success"])
                expected[counter] += size
                total += size
        snap = self.meter.snapshot()
        for counter, size in expected.items():
            self.assertEqual(snap["counters"][counter], size)
        self.assertEqual(snap["counters"]["response_bytes"], total)
        self.assertEqual(snap["counters"]["read_bytes"], total)
        self.assertEqual(sum(snap["response_bytes_by_object"].values()), total)
        self.assertEqual(snap["counters"]["metadata_read_bytes"], 0)
        self.assertEqual(snap["counters"]["retries"], len(cases))
        self.assertEqual(snap["counters"]["sst_read_bytes"], 0)

    def test_optional_checkpoint_nosuchkey_is_counted_and_private(self):
        selector = "/probe/private_region/manifest/_last_checkpoint"
        bodies = []
        for query in ("?X-Amz-Signature=private_probe_signature", "?case=chunked"):
            status, body = self._req(
                "GET", selector + query,
                headers={"Authorization": "private_probe_authorization"})
            self.assertEqual(status, 404)
            self.assertIn(b"<Code>NoSuchKey</Code>", body)
            bodies.append(body)
        snap = self.meter.snapshot()
        c = snap["counters"]
        moved = sum(map(len, bodies))
        self.assertEqual(c["expected_not_found_requests"], 2)
        self.assertEqual(c["expected_not_found_bytes"], moved)
        self.assertEqual(c["client_errors"], 2)
        self.assertEqual(c["client_error_bytes"], moved)
        self.assertEqual(c["read_bytes"], moved)
        self.assertEqual(c["payload_bytes"], moved)
        self.assertEqual(c["sst_get_requests"], 0)
        self.assertEqual(c["sst_read_bytes"], 0)
        self.assertEqual(snap["client_error_breakdown"], [{
            "status": 404, "op": "GET",
            "object_kind": "manifest_checkpoint_discovery",
            "expected_not_found": True, "requests": 2, "bytes": moved}])
        diagnostics = json.dumps(snap)
        for private in ("private_region", "private_probe_signature",
                        "private_probe_authorization", "private_xml_key",
                        "private_xml_request"):
            self.assertNotIn(private, diagnostics)
        # Cumulative diagnostics survive request-history retirement.
        with self.meter._lock:
            self.meter.requests.clear()
        after = self.meter.snapshot()
        self.assertEqual(after["requests"], [])
        self.assertEqual(after["client_error_breakdown"], snap["client_error_breakdown"])
        self.assertEqual(after["counters"], c)

    def test_drop_marker_head_miss_is_counted_without_body(self):
        status, body = self._req(
            "HEAD", "/probe/private_region/.dropping?X-Amz-Signature=private_drop_signature")
        self.assertEqual(status, 404)
        self.assertEqual(body, b"")
        snap = self.meter.snapshot()
        c = snap["counters"]
        self.assertEqual(c["expected_not_found_requests"], 1)
        self.assertEqual(c["expected_not_found_bytes"], 0)
        self.assertEqual(c["client_errors"], 1)
        self.assertEqual(c["client_error_bytes"], 0)
        self.assertEqual(c["read_bytes"], 0)
        self.assertEqual(c["sst_get_requests"], 0)
        self.assertEqual(snap["client_error_breakdown"], [{
            "status": 404, "op": "HEAD", "object_kind": "drop_marker",
            "expected_not_found": True, "requests": 1, "bytes": 0}])
        self.assertNotIn("private_region", json.dumps(snap))
        self.assertNotIn("private_drop_signature", json.dumps(snap))

    def test_unproven_checkpoint_and_other_4xx_stay_unexpected(self):
        checkpoint = "/probe/region/manifest/_last_checkpoint"
        cases = (
            ("GET", checkpoint + "?case=bucket", 404),
            ("GET", checkpoint + "?case=auth", 403),
            ("GET", checkpoint + "?case=malformed", 404),
            ("GET", checkpoint + "?case=duplicate", 404),
            ("GET", checkpoint + "?case=oversized", 404),
            ("GET", "/probe/region/data.parquet", 404),
            ("GET", "/probe/region/manifest/metadata.json", 404),
            ("GET", "/probe/region/manifest/nested/_last_checkpoint", 404),
            ("GET", "/probe/region//manifest/_last_checkpoint", 404),
            ("HEAD", checkpoint, 404),
            ("PUT", checkpoint, 404),
            ("GET", checkpoint + "?list-type=2", 404),
            ("HEAD", "/probe/region/data.parquet", 404),
            ("HEAD", "/probe/region/metadata.json", 404),
            ("GET", "/probe/region/.dropping", 404),
            ("HEAD", "/probe/region/.dropping?case=auth", 403),
            ("HEAD", "/probe/region//.dropping", 404),
        )
        for method, selector, expected_status in cases:
            with self.subTest(method=method, selector=selector):
                status, _ = self._req(method, selector)
                self.assertEqual(status, expected_status)
                snap = self.meter.snapshot()
                self.assertFalse(snap["requests"][-1]["expected_not_found"])
                self.assertEqual(snap["counters"]["expected_not_found_requests"], 0)
                self.assertEqual(snap["counters"]["expected_not_found_bytes"], 0)
        self.assertEqual(snap["counters"]["client_errors"], len(cases))
        self.assertEqual(sum(g["requests"] for g in snap["client_error_breakdown"]), len(cases))
        self.assertEqual(sum(g["bytes"] for g in snap["client_error_breakdown"]),
                         snap["counters"]["client_error_bytes"])
        status, _ = self._req("GET", checkpoint + "?case=truncated")
        self.assertEqual(status, 404)
        snap = self.meter.snapshot()
        self.assertEqual(snap["counters"]["errors"], 1)
        self.assertEqual(snap["counters"]["truncated"], 1)
        self.assertEqual(snap["counters"]["expected_not_found_requests"], 0)
        self.assertFalse(snap["requests"][-1]["expected_not_found"])

    def test_production_and_credential_bearing_origins_are_refused(self):
        for origin in ("https://storage.example.com", "http://minio.example.com",
                       "http://user:private_password@127.0.0.1:9000",
                       "http://127.0.0.1:9000?token=private_token"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                S3Meter(origin, allow_non_loopback=True)

    def test_host_path_query_range_streaming(self):
        status, payload = self._req("GET", "/bucket/key?list-type=2&x=1",
                                    headers={"Range": "bytes=2-5"})
        self.assertEqual(status, 200)
        # Origin saw the exact signed Host, path, query and Range.
        self.assertEqual(self.seen[-1]["host"], "signed-host.example")
        self.assertEqual(self.seen[-1]["path"], "/bucket/key?list-type=2&x=1")
        self.assertEqual(self.seen[-1]["range"], "bytes=2-5")
        snap = self.meter.snapshot()
        self.assertEqual(snap["counters"]["requests"], 1)
        self.assertEqual(snap["op_requests"].get("LIST"), 1)
        self.assertEqual(snap["requests"][0]["resp_bytes"], len(payload))
        self.assertEqual(snap["requests"][0]["status"], 200)
        self.assertEqual(snap["requests"][0]["attempts"], 0)
        self.assertEqual(snap["counters"]["unknown_attempts"], 1)
        self.assertEqual(snap["counters"]["retries"], 0)

    def test_retry_sequence_counts_two_not_three(self):
        for n in (1, 2, 3):
            status, _ = self._req(
                "PUT", "/bucket/key", body=b"v%d" % n,
                headers={"amz-sdk-request": "attempt=%d; max=5" % n})
            self.assertEqual(status, 200)
        snap = self.meter.snapshot()
        self.assertEqual(snap["counters"]["requests"], 3)
        # Each wire attempt counts 1.
        self.assertEqual(snap["op_requests"].get("PUT"), 3)
        # attempt=1/2/3 is 2 observed retries, not 0+1+2=3.
        self.assertEqual([r["attempts"] for r in snap["requests"]], [1, 2, 3])
        self.assertEqual(snap["counters"]["retries"], 2)
        self.assertEqual([r["retries"] for r in snap["requests"]],
                         [0, 1, 1])
        # Each attempt's streamed bytes count once: 3 x 2-byte PUTs.
        self.assertEqual(snap["counters"]["write_bytes"], 6)
        self.assertEqual(snap["counters"]["payload_bytes"],
                         sum(r["req_bytes"] + r["resp_bytes"]
                             for r in snap["requests"][-3:]))

    def test_chunked_request_relayed_bounded(self):
        body = b"c" * 200000
        status, _ = self._req("PUT", "/bucket/chunked", body=body,
                              chunked=True)
        self.assertEqual(status, 200)
        self.assertEqual(self.seen[-1]["body"], body)
        snap = self.meter.snapshot()
        put = [r for r in snap["requests"] if r["op"] == "PUT"][-1]
        self.assertEqual(put["req_bytes"], len(body))
        self.assertFalse(put["truncated"])
        self.assertEqual(snap["counters"]["write_bytes"], len(body))

    def test_snapshot_waits_for_completed_response_accounting(self):
        held = threading.Event()
        release = threading.Event()
        waiting = threading.Event()
        done = threading.Event()
        record = self.meter._record
        wait = self.meter._idle.wait
        result = {}
        worker = None

        def blocked_record(entry):
            held.set()
            if not release.wait(10):
                raise RuntimeError("test did not release completed response")
            record(entry)

        def observed_wait(timeout=None):
            waiting.set()
            return wait(timeout)

        def snapshot():
            try:
                result["snapshot"] = self.meter.snapshot(timeout=10)
            except Exception as exc:
                result["error"] = exc
            finally:
                done.set()

        self.meter._record = blocked_record
        self.meter._idle.wait = observed_wait
        try:
            # The final chunk has reached the client, but accounting is held.
            status, payload = self._req("GET", "/chunked-resp.parquet")
            self.assertEqual(status, 200)
            self.assertEqual(payload, b"hello-range-body")
            self.assertTrue(held.wait(10))
            with self.meter._lock:
                self.assertEqual(self.meter.counters["sst_get_requests"], 0)
            with self.assertRaisesRegex(TimeoutError, "snapshot incomplete"):
                self.meter.snapshot(timeout=0)
            worker = threading.Thread(target=snapshot, daemon=True)
            worker.start()
            self.assertTrue(waiting.wait(10))
            self.assertFalse(done.is_set())
        finally:
            release.set()
            if worker is not None:
                worker.join(timeout=10)
            self.meter._record = record
            self.meter._idle.wait = wait
        self.assertTrue(done.is_set())
        self.assertNotIn("error", result)
        snap = result["snapshot"]
        self.assertEqual(snap["counters"]["requests"], 1)
        self.assertEqual(snap["op_requests"], {"GET": 1})
        self.assertEqual(snap["counters"]["read_bytes"], len(payload))
        self.assertEqual(snap["counters"]["sst_get_requests"], 1)
        self.assertEqual(snap["counters"]["sst_read_bytes"], len(payload))
        self.assertEqual(snap["requests"][-1]["resp_bytes"], len(payload))
        self.assertTrue(snap["requests"][-1]["success"])

    def test_chunked_response_streamed(self):
        status, payload = self._req("GET", "/chunked-resp")
        self.assertEqual(status, 200)
        self.assertEqual(payload, b"hello-range-body")
        snap = self.meter.snapshot()
        self.assertEqual(snap["requests"][-1]["resp_bytes"], len(payload))
        self.assertEqual(snap["counters"]["read_bytes"], len(payload))

    def test_head_records_no_body(self):
        c = http.client.HTTPConnection("127.0.0.1", self.meter.port, timeout=10)
        c.putrequest("HEAD", "/bucket/key", skip_host=True)
        c.putheader("Host", "signed-host.example")
        c.putheader("Content-Length", "5")
        c.endheaders()
        r = c.getresponse()
        r.read()
        self.assertEqual(r.status, 200)
        c.close()
        snap = self.meter.snapshot()
        head = [x for x in snap["requests"] if x["op"] == "HEAD"][-1]
        # HEAD never waits for a body and records none.
        self.assertEqual(head["req_bytes"], 0)
        self.assertEqual(head["resp_bytes"], 0)
        self.assertFalse(head["truncated"])

    def test_4xx_bytes_stay_in_totals(self):
        status, _ = self._req("GET", "/missing")
        self.assertEqual(status, 404)
        snap = self.meter.snapshot()
        last = snap["requests"][-1]
        self.assertEqual(snap["counters"]["client_errors"], 1)
        self.assertEqual(snap["counters"]["errors"], 0)
        # Attempted bytes count in totals; client_error is a breakdown.
        self.assertEqual(snap["counters"]["read_bytes"], last["resp_bytes"])
        self.assertGreater(last["resp_bytes"], 0)
        self.assertEqual(snap["counters"]["client_error_bytes"],
                         last["req_bytes"] + last["resp_bytes"])
        self.assertEqual(snap["counters"]["payload_bytes"],
                         last["req_bytes"] + last["resp_bytes"])

    def test_5xx_bytes_stay_in_totals(self):
        status, _ = self._req("GET", "/boom?list-type=2")
        self.assertEqual(status, 500)
        snap = self.meter.snapshot()
        last = snap["requests"][-1]
        self.assertEqual(snap["counters"]["errors"], 1)
        self.assertEqual(snap["counters"]["client_errors"], 0)
        # GET error XML is control/response-status bytes but its body
        # egress still counts; LIST is a control op.
        self.assertEqual(snap["counters"]["read_bytes"], 0)
        self.assertGreater(last["resp_bytes"], 0)
        self.assertEqual(snap["counters"]["error_bytes"],
                         last["req_bytes"] + last["resp_bytes"])
        self.assertEqual(snap["counters"]["control_bytes"],
                         last["req_bytes"] + last["resp_bytes"])

    def test_failed_get_partial_bytes_stay_in_totals(self):
        status, payload = self._req("GET", "/short.parquet")
        snap = self.meter.snapshot()
        last = snap["requests"][-1]
        self.assertTrue(last["truncated"])
        self.assertEqual(last["object_kind"], "sst")
        self.assertFalse(last["success"])
        self.assertEqual(snap["counters"]["sst_get_requests"], 0)
        self.assertEqual(snap["counters"]["sst_read_bytes"], 0)
        self.assertEqual(last["resp_bytes"], 10)
        self.assertEqual(snap["counters"]["errors"], 1)
        # Fail-at-end cost cannot disappear: partial bytes in both
        # totals and the error breakdown. Retries count each attempt.
        self.assertEqual(snap["counters"]["read_bytes"], 10)
        self.assertEqual(snap["counters"]["error_bytes"],
                         last["req_bytes"] + last["resp_bytes"])
        self.assertEqual(snap["counters"]["payload_bytes"],
                         last["req_bytes"] + last["resp_bytes"])
        self.assertEqual(status, 200)
        self.assertEqual(payload, b"0123456789")


    def test_truncated_request_body_fails_safe(self):
        sock = socket.create_connection(("127.0.0.1", self.meter.port),
                                        timeout=10)
        try:
            sock.sendall(b"PUT /bucket/cut HTTP/1.1\r\n"
                         b"Host: signed-host.example\r\n"
                         b"Content-Length: 100\r\n"
                         b"Connection: close\r\n\r\n" + b"z" * 10)
            sock.shutdown(socket.SHUT_WR)
            raw = b""
            while True:
                piece = sock.recv(65536)
                if not piece:
                    break
                raw += piece
        finally:
            sock.close()
        self.assertIn(b"502", raw.split(b"\r\n", 1)[0])
        snap = self.meter.snapshot()
        last = snap["requests"][-1]
        self.assertTrue(last["truncated"])
        self.assertEqual(last["req_bytes"], 10)
        self.assertEqual(snap["counters"]["errors"], 1)
        # Partial upload bytes still count; error is a breakdown.
        self.assertEqual(snap["counters"]["write_bytes"], 10)
        self.assertEqual(snap["counters"]["error_bytes"], 10)

    def test_large_stream_stays_bounded(self):
        body = b"s" * (20 << 20)
        tracemalloc.start()
        try:
            before = tracemalloc.get_traced_memory()[0]
            status, _ = self._req("PUT", "/bucket/big", body=body)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(status, 200)
        snap = self.meter.snapshot()
        put = [r for r in snap["requests"] if r["op"] == "PUT"][-1]
        # Stub origin is bounded too: large bodies keep a size marker only.
        self.assertEqual(self.seen[-1]["body"], b"<large:%d>" % len(body))
        # Whole-SST buffering would peak near the body size; 64KiB
        # streaming stays far below it.
        self.assertLess(peak - before, len(body) // 2)

    def test_copy_uses_known_source_size_not_body(self):
        meter = self.meter
        meter.copy_size_lookup = lambda src: 12345 if src == "b/src" else None
        try:
            status, _ = self._req("PUT", "/bucket/dst", body=b"",
                                  headers={"x-amz-copy-source": "b/src"})
            self.assertEqual(status, 200)
            status, _ = self._req("PUT", "/bucket/dst2", body=b"junk",
                                  headers={"x-amz-copy-source": "b/other"})
            self.assertEqual(status, 200)
        finally:
            meter.copy_size_lookup = None
        snap = meter.snapshot()
        copies = [r for r in snap["requests"] if r["op"] == "COPY"]
        self.assertEqual(len(copies), 2)
        self.assertTrue(copies[0]["copy_known"])
        self.assertEqual(copies[0]["copy_bytes"], 12345)
        self.assertFalse(copies[1]["copy_known"])
        # COPY body bytes are not egress: request bytes stay body-sized
        # while copy_bytes comes from the explicit lookup.
        self.assertEqual(copies[0]["req_bytes"], 0)
        self.assertEqual(snap["counters"]["copy_bytes"], 12345)
        self.assertEqual(snap["counters"]["copy_unknown"], 1)
        self.assertEqual(snap["counters"]["write_bytes"], 0)

    def test_aws_chunked_body_fails_safe_unmutated(self):
        status, _ = self._req("PUT", "/bucket/stream", body=b"data",
                              headers={"Content-Encoding": "aws-chunked",
                                       "x-amz-content-sha256":
                                       "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"})
        self.assertEqual(status, 502)
        snap = self.meter.snapshot()
        last = snap["requests"][-1]
        self.assertIsNotNone(last["unsupported"])
        self.assertEqual(snap["counters"]["unsupported"], 1)
        self.assertEqual(snap["counters"]["errors"], 1)
        self.assertEqual(snap["counters"]["write_bytes"], 0)

    def test_non_loopback_bind_requires_explicit_opt_in(self):
        with self.assertRaises(ValueError):
            S3Meter("http://127.0.0.1:9", bind="0.0.0.0")
        # Explicit opt-in is the sidecar path; it binds and rebinds clean.
        m = S3Meter("http://127.0.0.1:9", bind="0.0.0.0",
                    allow_non_loopback=True).start()
        port = m.port
        m.stop()
        S3Meter("http://127.0.0.1:9", bind="0.0.0.0", port=port,
                allow_non_loopback=True).start().stop()

    def test_status_snapshot_endpoint(self):
        m = S3Meter("http://127.0.0.1:%d" % self.origin.server_address[1],
                    status_port=0).start()
        try:
            c = http.client.HTTPConnection("127.0.0.1", m.status_port,
                                           timeout=10)
            c.request("GET", "/snapshot")
            r = c.getresponse()
            doc = json.loads(r.read())
            self.assertEqual(r.status, 200)
            self.assertIn("counters", doc)
            self.assertIn("op_requests", doc)
            c.request("GET", "/health")
            self.assertEqual(c.getresponse().status, 200)
            c.close()
        finally:
            m.stop()

    def test_cleanup_stops_listener(self):
        port = self.meter.port
        self.meter.stop()
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", port), timeout=2)
        # Rebind allowed immediately (current/baseline phases reuse ports).
        S3Meter("http://127.0.0.1:9", port=port).start().stop()


def _oracle_stats(n):
    """Mirror the bench's deterministic rows + incremental fold (10M-safe)."""
    st = {"n": 0, "sum": 0.0, "min": None, "max": None,
          "by_vehicle": {}, "by_path": {}, "by_seg": {},
          "sum_v0": 0.0, "over": 0, "groups": set()}
    for i in range(n):
        seg = (i * bench.SEGMENTS) // max(n, 1)
        v = i % len(bench.VEHICLES)
        p = (i // len(bench.VEHICLES)) % len(bench.PATHS)
        val = ((i * 7919) % 10000) / 100.0 + v * 10.0
        st["n"] += 1
        st["sum"] += val
        st["min"] = val if st["min"] is None else min(st["min"], val)
        st["max"] = val if st["max"] is None else max(st["max"], val)
        vb = st["by_vehicle"].setdefault(bench.VEHICLES[v], [0, 0.0])
        vb[0] += 1
        vb[1] += val
        pb = st["by_path"].setdefault(bench.PATHS[p], [0, 0.0])
        pb[0] += 1
        pb[1] += val
        sb = st["by_seg"].setdefault(seg, [0, 0.0])
        sb[0] += 1
        sb[1] += val
        if bench.VEHICLES[v] == "v0":
            st["sum_v0"] += val
        if val > 50.0:
            st["over"] += 1
        st["groups"].add((bench.VEHICLES[v], bench.PATHS[p]))
    return st


class OracleTest(unittest.TestCase):
    def test_twenty_queries_and_oracles(self):
        st = _oracle_stats(400)
        queries = bench.build_queries(st)
        self.assertEqual(len(queries), 20)
        names = [q[0] for q in queries]
        self.assertEqual(len(set(names)), 20)
        # Deterministic: same input rows give identical expectations.
        st2 = _oracle_stats(400)
        self.assertEqual([q[2] for q in bench.build_queries(st2)],
                         [q[2] for q in queries])

    def test_num_coercion(self):
        self.assertEqual(bench._num("12.5"), 12.5)
        self.assertIsNone(bench._num(None))
        self.assertIsNone(bench._num("nan-x"))


class GateTest(unittest.TestCase):
    def _scenario(self, **over):
        s = {"ingest_rows": 100, "flushed_sst_bytes": 10, "flushed_sst_objects": 2,
             "compacted_bytes": 100, "cold_get_bytes": 50, "cold_get_requests": 5,
             "warm_get_bytes": 10, "warm_get_requests": 2,
             "query_oracle_pass": True, "query_count": 20,
             "timing_spread_ms": 1.0, "requests_by_op": {"GET": 5},
             "read_bytes": 50, "write_bytes": 100, "copy_bytes": 0,
             "errors": 0, "retries": 0, "payload_bytes": 150,
             "control_bytes": 10, "total_requests": 20, "oracle_detail": []}
        s.update(over)
        return s

    def test_relative_growth_fails(self):
        base = self._scenario()
        cur = self._scenario(read_bytes=base["read_bytes"] * 2,
                             write_bytes=base["write_bytes"] * 2,
                             compacted_bytes=base["compacted_bytes"] * 3,
                             total_requests=base["total_requests"] * 2)
        fails, _ = bench.check_relative(cur, base, {})
        self.assertTrue(fails)

    def test_relative_within_tolerance_passes(self):
        base = self._scenario()
        cur = self._scenario(read_bytes=base["read_bytes"] * 1.19)
        fails, _ = bench.check_relative(cur, base, {})
        self.assertFalse(fails)

    def test_uncertainty_credited_not_ignored(self):
        base = self._scenario(read_bytes=1000)
        cur = self._scenario(read_bytes=1150)
        fails, _ = bench.check_relative(cur, base, {"relative_growth_max": 0.10,
                                                    "uncertainty_bytes": 100})
        self.assertFalse(fails)  # 1150 vs 1100 credited = 4.5%
        fails2, _ = bench.check_relative(cur, base, {"relative_growth_max": 0.10})
        self.assertTrue(fails2)

    def test_baseline_zero_payload_growth_fails(self):
        base = self._scenario(read_bytes=0, write_bytes=0)
        cur = self._scenario(read_bytes=2 << 20, write_bytes=2 << 20)
        fails, _ = bench.check_relative(cur, base, {})
        self.assertTrue(fails)

    def test_budget_keymap(self):
        s = self._scenario(compacted_bytes=10 ** 9, total_requests=10 ** 6,
                           cold_get_bytes=10 ** 8, warm_get_bytes=10 ** 7)
        with open(os.path.join(REPO, "config", "cost-budget.json"), encoding="utf-8") as f:
            budget = json.load(f)
        budget["greptime"].update(max_sst_bytes=1, max_requests=1,
                                  max_cold_get_bytes=1, max_warm_get_bytes=1)
        with tempfile.NamedTemporaryFile("w", suffix=".json",
                                         delete=False) as f:
            json.dump(budget, f)
            path = f.name
        try:
            res, params = bench.check_budget(s, path)
            self.assertEqual(res["status"], "fail")
            self.assertEqual(len(res["failures"]), 4)
            self.assertEqual(params["rows_nightly"], 10000000)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
