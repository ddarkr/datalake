"""HTTP per-request deadline regression (real loopback; synthetic auth only).

Run: python -m unittest discover -s tests -p test_can_http_deadline.py
"""
import base64
import gzip
import http.client
import socket
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

from scripts.ingest.can.can_otlp_wire import check_response, encode_batch
from scripts.ingest.can.can_receiver import Archive, Receiver
from tests.test_can_receiver import synthetic_decoder


def _serve(archive, timeout=1, **kwargs):
    server = Receiver(("127.0.0.1", 0), archive, "synthetic", "fixture",
                      "fixture-user", "fixture-password", timeout=timeout, **kwargs)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _stop(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(3)


class HttpDeadline(unittest.TestCase):
    def test_keepalive_survives_beyond_timeout_across_fast_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            server, thread = _serve(archive)
            try:
                auth = "Basic " + base64.b64encode(b"fixture-user:fixture-password").decode()
                keep = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    first_sock = None
                    latencies = []
                    for _ in range(4):
                        start = time.monotonic()
                        keep.request("GET", "/status", headers={"Authorization": auth})
                        response = keep.getresponse()
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.getheader("Connection", ""), "keep-alive")
                        response.read()
                        latencies.append(time.monotonic() - start)
                        if first_sock is None:
                            first_sock = keep.sock
                        else:
                            self.assertIs(keep.sock, first_sock)  # Zero reconnects.
                        time.sleep(0.4)  # Total connection life (~1.6s) exceeds timeout=1.
                    self.assertLess(max(latencies), 1)
                finally:
                    keep.close()
            finally:
                _stop(server, thread)

    def test_stale_timer_cannot_close_next_request(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            server, thread = _serve(archive, timeout=5)
            try:
                created = []
                real_timer = threading.Timer

                def spy(interval, function, args=None, kwargs=None):
                    timer = real_timer(interval, function, args, kwargs)
                    created.append(timer)
                    return timer

                auth = "Basic " + base64.b64encode(b"fixture-user:fixture-password").decode()
                calls = {"n": 0}
                entered = threading.Event()
                proceed = threading.Event()
                real_status = archive.status

                def gated_status():
                    calls["n"] += 1
                    if calls["n"] == 2:
                        entered.set()
                        proceed.wait(10)
                    return real_status()

                outcome = {}

                def second_request(keep):
                    try:
                        keep.request("GET", "/status", headers={"Authorization": auth})
                        response = keep.getresponse()
                        outcome["status"] = response.status
                        response.read()
                    except Exception as exc:
                        outcome["error"] = exc

                with patch("threading.Timer", spy), patch.object(archive, "status", gated_status):
                    keep = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=10)
                    try:
                        keep.request("GET", "/status", headers={"Authorization": auth})
                        first = keep.getresponse()
                        self.assertEqual(first.status, 200)
                        first.read()
                        first_sock = keep.sock
                        self.assertTrue(created)
                        stale = created[0]
                        stale.cancel()
                        worker = threading.Thread(target=second_request, args=(keep,), daemon=True)
                        worker.start()
                        # Request N+1 is blocked inside the handler holding
                        # token N+1 when the stale timer N fires.
                        self.assertTrue(entered.wait(10))
                        stale.function(*stale.args or (), **stale.kwargs or {})
                        proceed.set()
                        worker.join(10)
                        self.assertFalse(worker.is_alive())
                        self.assertNotIn("error", outcome)
                        self.assertEqual(outcome.get("status"), 200)
                        self.assertIs(keep.sock, first_sock)
                    finally:
                        keep.close()
            finally:
                _stop(server, thread)

    def test_slow_header_closed_within_bounded_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            server, thread = _serve(archive)
            try:
                sock = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                try:
                    # Header bytes keep arriving faster than the socket idle
                    # timeout, so only the absolute whole-request deadline
                    # can close this connection.
                    sock.sendall(b"GET /status HTTP/1.1\r\nHost: x\r\n")
                    start = time.monotonic()
                    closed = False
                    while time.monotonic() - start < 5:
                        try:
                            sock.sendall(b"X-Pad: %d\r\n" % int((time.monotonic() - start) * 10))
                        except OSError:
                            closed = True
                            break
                        try:
                            sock.settimeout(0.2)
                            if sock.recv(64) == b"":
                                closed = True
                                break
                        except socket.timeout:
                            pass
                        except ConnectionResetError:
                            closed = True
                            break
                    self.assertTrue(closed)
                    self.assertLess(time.monotonic() - start, 5)
                    sock.settimeout(3)
                    try:
                        self.assertEqual(sock.recv(64), b"")
                    except ConnectionResetError:
                        pass
                finally:
                    sock.close()
            finally:
                _stop(server, thread)

    def test_slow_body_trickle_closed_despite_activity(self):
        # Bytes arrive faster than the idle socket timeout, so only the
        # absolute per-request deadline can close this.
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            server, thread = _serve(archive)
            try:
                auth = base64.b64encode(b"fixture-user:fixture-password").decode()
                sock = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                try:
                    sock.sendall(
                        b"POST /v1/logs HTTP/1.1\r\nHost: x\r\n"
                        b"Authorization: Basic " + auth.encode() + b"\r\n"
                        b"Content-Type: application/x-protobuf\r\n"
                        b"Content-Encoding: gzip\r\n"
                        b"Content-Length: 64\r\n\r\nX")
                    start = time.monotonic()
                    closed = False
                    while time.monotonic() - start < 5:
                        try:
                            sock.sendall(b"Y")
                        except OSError:
                            closed = True
                            break
                        try:
                            sock.settimeout(0.2)
                            if sock.recv(64) == b"":
                                closed = True
                                break
                        except socket.timeout:
                            pass
                        except ConnectionResetError:
                            closed = True
                            break
                    self.assertTrue(closed)
                    self.assertLess(time.monotonic() - start, 5)
                finally:
                    sock.close()
            finally:
                _stop(server, thread)

    def test_error_paths_keep_existing_close_semantics(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            server, thread = _serve(archive)
            try:
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    conn.request("GET", "/status",
                                   headers={"Authorization": "Basic Zm9vOmJhcg=="})
                    denied = conn.getresponse()
                    self.assertEqual(denied.status, 401)
                    self.assertEqual(denied.getheader("Connection", ""), "close")
                    denied.read()
                finally:
                    conn.close()
                raw = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                try:
                    # Four request-line tokens is rejected by parse_request
                    # (a three-token line like "GET ??? HTTP/1.1" is valid
                    # and would route to 401, not 400). The valid version
                    # must come last so the 400 reply keeps HTTP/1.1 framing.
                    raw.sendall(b"GET /status EXTRA HTTP/1.1\r\nHost: x\r\n\r\n")
                    raw.settimeout(3)
                    data = b""
                    try:
                        while b"\r\n\r\n" not in data:
                            chunk = raw.recv(4096)
                            if chunk == b"":
                                break
                            data += chunk
                        self.assertIn(b"400", data)
                        # Drain the framed body before expecting EOF.
                        length = 0
                        for line in data.split(b"\r\n"):
                            if line[:15].lower() == b"content-length:":
                                length = int(line[15:].strip())
                        body = data.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in data else b""
                        while len(body) < length:
                            chunk = raw.recv(4096)
                            if chunk == b"":
                                break
                            body += chunk
                    except ConnectionResetError:
                        pass
                    raw.settimeout(3)
                    try:
                        self.assertEqual(raw.recv(64), b"")
                    except ConnectionResetError:
                        pass
                finally:
                    raw.close()
            finally:
                _stop(server, thread)

    def test_slot_freed_and_close_rejects_new_input(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            server, thread = _serve(archive, timeout=5, concurrency=1)
            closed = False
            try:
                auth = "Basic " + base64.b64encode(b"fixture-user:fixture-password").decode()
                first = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    first.request("GET", "/status", headers={"Authorization": auth})
                    response = first.getresponse()
                    self.assertEqual(response.status, 200)
                    response.read()
                    # Slot still held by the idle keep-alive connection:
                    # a second connection is refused promptly.
                    probe = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                    try:
                        probe.settimeout(3)
                        try:
                            self.assertEqual(probe.recv(64), b"")
                        except ConnectionResetError:
                            pass
                    finally:
                        probe.close()
                finally:
                    first.close()
                # Closing the holder frees the slot; a new socket is served.
                deadline = time.monotonic() + 5
                while True:
                    keep = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                    try:
                        try:
                            keep.request("GET", "/status", headers={"Authorization": auth})
                            retry = keep.getresponse()
                        except (ConnectionResetError, BrokenPipeError):
                            if time.monotonic() >= deadline:
                                raise
                            time.sleep(0.05)
                            continue
                        self.assertEqual(retry.status, 200)
                        retry.read()
                        break
                    finally:
                        keep.close()
                # server_close is the real shutdown boundary: no idle
                # keep-alive holder left, so shutdown() wakes serve_forever,
                # the slot drains, and server_close() refuses new input.
                server.shutdown()
                thread.join(3)
                server.server_close()
                closed = True
                try:
                    refused = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                except (ConnectionRefusedError, OSError):
                    pass  # Listen socket closed: new input refused at TCP level.
                else:
                    try:
                        refused.settimeout(3)
                        try:
                            refused.sendall(b"GET /status HTTP/1.0\r\nHost: x\r\n\r\n")
                        except OSError:
                            pass
                        try:
                            self.assertEqual(refused.recv(64), b"")
                        except (ConnectionResetError, ConnectionRefusedError, OSError):
                            pass
                    finally:
                        refused.close()
            finally:
                if not closed:
                    server.shutdown()
                    server.server_close()
                    thread.join(3)

    def test_commit_then_retransmit_and_partial_body(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "http-durable", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": b"t12320200\r"}
                  for seq in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            server, thread = _serve(archive, timeout=5)
            try:
                compressed = gzip.compress(encode_batch(meta, chunks))
                endpoint = "http://127.0.0.1:%d/v1/logs" % server.server_port
                auth = "Basic " + base64.b64encode(b"fixture-user:fixture-password").decode()

                def post(body):
                    request = urllib.request.Request(
                        endpoint, data=body,
                        headers={"Authorization": auth,
                                 "Content-Type": "application/x-protobuf",
                                 "Content-Encoding": "gzip"}, method="POST")
                    with urllib.request.urlopen(request, timeout=10) as response:
                        assert response.status == 200
                        check_response(response.read())

                post(compressed)
                self.assertEqual(archive.status()["raw_chunks"], len(chunks))
                # ACK loss retransmit: overlap-legal, commits nothing new.
                post(compressed)
                self.assertEqual(archive.status()["raw_chunks"], len(chunks))
                # Partial body commits nothing; full retransmit commits once.
                sock = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                try:
                    sock.sendall(
                        b"POST /v1/logs HTTP/1.1\r\nHost: x\r\n"
                        b"Authorization: " + auth.encode() + b"\r\n"
                        b"Content-Type: application/x-protobuf\r\n"
                        b"Content-Encoding: gzip\r\n"
                        b"Content-Length: " + str(len(compressed)).encode() + b"\r\n\r\n"
                        + compressed[:len(compressed) // 2])
                    sock.shutdown(socket.SHUT_WR)
                    sock.settimeout(5)
                    data = b""
                    try:
                        while b"\r\n\r\n" not in data:
                            chunk = sock.recv(4096)
                            if chunk == b"":
                                break
                            data += chunk
                    except ConnectionResetError:
                        pass
                    self.assertIn(b"400", data)
                finally:
                    sock.close()
                self.assertEqual(archive.status()["raw_chunks"], len(chunks))
            finally:
                _stop(server, thread)


    def test_commit_near_deadline_ack_loss_then_retry(self):
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "http-ack-loss", "started_ns": 1800000000000000000,
                "vehicle_firmware": "synthetic"}
        chunks = [{"seq": seq, "offset_ns": seq, "phase": "capture", "data": b"t12320200\r"}
                  for seq in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            decoder = synthetic_decoder(directory)
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(decoder)
            server, thread = _serve(archive, timeout=1)
            try:
                compressed = gzip.compress(encode_batch(meta, chunks))
                endpoint = "http://127.0.0.1:%d/v1/logs" % server.server_port
                auth = "Basic " + base64.b64encode(b"fixture-user:fixture-password").decode()
                real_accept = server.archive.accept

                def slow_accept(meta_arg, chunks_arg):
                    added = real_accept(meta_arg, chunks_arg)
                    # Raw is already committed; the whole-request timer fires
                    # before the reply is written, so the client loses the ACK.
                    time.sleep(1.6)
                    return added

                def post(body):
                    request = urllib.request.Request(
                        endpoint, data=body,
                        headers={"Authorization": auth,
                                 "Content-Type": "application/x-protobuf",
                                 "Content-Encoding": "gzip"}, method="POST")
                    with urllib.request.urlopen(request, timeout=10) as response:
                        assert response.status == 200
                        return check_response(response.read())

                with patch.object(server.archive, "accept", slow_accept):
                    try:
                        post(compressed)
                        acked = True
                    except Exception:
                        acked = False
                # Client lost the response, but the commit is durable.
                self.assertFalse(acked)
                self.assertEqual(archive.status()["raw_chunks"], len(chunks))
                # Exact retry after ACK loss commits nothing new.
                post(compressed)
                self.assertEqual(archive.status()["raw_chunks"], len(chunks))
                # Conflicting bytes for an ACKed seq are rejected, not stored.
                conflict = [dict(chunks[0], data=b"t12320500\r")]
                try:
                    post(gzip.compress(encode_batch(meta, conflict)))
                    self.fail("conflicting retransmit must not ACK")
                except urllib.error.HTTPError as error:
                    self.assertEqual(error.code, 409)
                self.assertEqual(archive.status()["raw_chunks"], len(chunks))
            finally:
                _stop(server, thread)


if __name__ == "__main__":
    unittest.main()
