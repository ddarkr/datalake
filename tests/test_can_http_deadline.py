"""HTTP per-request deadline regression (real loopback; synthetic auth only).

Run: python -m unittest discover -s tests -p test_can_http_deadline.py
"""
import base64
import http.client
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from scripts.ingest.can.can_receiver import Archive, Receiver


def _serve(archive, timeout=1):
    server = Receiver(("127.0.0.1", 0), archive, "synthetic", "fixture",
                      "fixture-user", "fixture-password", timeout=timeout)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


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
                    for _ in range(4):
                        keep.request("GET", "/status", headers={"Authorization": auth})
                        response = keep.getresponse()
                        self.assertEqual(response.status, 200)
                        response.read()
                        if first_sock is None:
                            first_sock = keep.sock
                        else:
                            self.assertIs(keep.sock, first_sock)
                        time.sleep(0.4)  # Total connection life (~1.6s) exceeds timeout=1.
                finally:
                    keep.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(3)

    def test_slow_header_closed_within_bounded_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            server, thread = _serve(archive)
            try:
                sock = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                try:
                    sock.sendall(b"GET /status HTTP/1.1\r\nHost: x\r\n")
                    time.sleep(1.6)  # Trickle slower than timeout=1.
                    sock.settimeout(3)
                    try:
                        self.assertEqual(sock.recv(64), b"")
                    except ConnectionResetError:
                        pass
                finally:
                    sock.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(3)

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
                server.shutdown()
                server.server_close()
                thread.join(3)

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
                server.shutdown()
                server.server_close()
                thread.join(3)


if __name__ == "__main__":
    unittest.main()
