"""Behavioral direct-file receive + worker-only import tests (Main runs).

Covers exact bytes/hash, interrupted PUT recovery, lost-ACK replay, mismatch
no-advance, path/symlink/auth/disk bounds, growing partial JSONL, snapshot
replacement, legacy OTLP overlap dedup with exact nanoseconds,
invalid-source isolation, and crash replay. No live data, no services.
"""

import base64
import hashlib
import json
import os
import tempfile
import threading
import unittest
from http.server import HTTPServer
from unittest.mock import patch
from decimal import Decimal
from pathlib import Path

from scripts.ingest.can import can_files
from scripts.ingest.can.can_files import (
    EMPTY_SHA256, FileImporter, FileStore, _parse_capture,
    _parse_chunk,
)
from scripts.ingest.can.can_receiver import Archive, Greptime, Receiver, Worker
from tests.test_can_receiver import SQLSink, synthetic_decoder

AUTH = "fixture-user:fixture-password"
AUTH_HEADER = "Basic " + base64.b64encode(AUTH.encode()).decode()
IDENT = {"X-CAN-Vehicle": "synthetic", "X-CAN-Collector": "fixture"}
STARTED_UTC = "2026-10-04T07:24:48.015510123Z"
STARTED_NS = 1791098688015510123


def _capture(started_utc=STARTED_UTC, firmware="synthetic", raw_hash=None, extra=None):
    doc = {"schema_version": 1, "started_utc": started_utc,
           "vehicle_firmware": firmware}
    if raw_hash is not None:
        doc["raw_file_sha256"] = raw_hash
    doc.update(extra or {})
    return json.dumps(doc).encode()


def _line(seq, stamp, phase="capture", payload=b"t12320200\r"):
    return (b'{"seq":%d,"t_s":%s,"phase":"%s","data_b64":"%s"}\n'
            % (seq, stamp, phase.encode(),
               base64.b64encode(payload)))


class _Fixture:
    def __init__(self, test):
        self.test = test
        self.tmp = tempfile.TemporaryDirectory()
        test.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.archive = Archive(root / "raw.sqlite", disk_reserve_bytes=0)
        decoder = synthetic_decoder(self.tmp.name)
        self.archive.register_epoch(decoder)
        self.decoder = decoder
        self.store = FileStore(self.archive, "synthetic", "fixture")

    def put(self, filename, body, offset=0, session="s1"):
        digest = hashlib.sha256(body).hexdigest()
        return self.store.put(session, filename, offset, digest, body)


SERIAL_FILE = "serial_chunks.jsonl"


def _drain(importer):
    for _ in range(100):
        if not importer.import_once():
            return
    raise AssertionError("bounded fixture did not reach importer idle")


class StrictParsing(unittest.TestCase):
    def test_exact_decimal_ns_and_order(self):
        chunk, stamp = _parse_chunk(
            _line(0, b"0.000000007")[:-1], 0, None, STARTED_NS)
        self.assertEqual((chunk["offset_ns"], stamp), (7, Decimal("0.000000007")))
        chunk, stamp = _parse_chunk(
            _line(1, b"0.000000007")[:-1], 1, stamp, STARTED_NS)
        self.assertEqual(chunk["offset_ns"], 7)
        with self.assertRaises(Exception):
            _parse_chunk(_line(1, b"0.000000006")[:-1], 1, stamp, STARTED_NS)
        with self.assertRaises(Exception):
            _parse_chunk(_line(1, b"1e400")[:-1], 1, stamp, STARTED_NS)

    def test_sub_nanosecond_digits_truncate(self):
        chunk, _ = _parse_chunk(
            _line(0, b"0.0000000079")[:-1], 0, None, STARTED_NS)
        self.assertEqual(chunk["offset_ns"], 7)

    def test_extreme_exponents_stay_bounded(self):
        for stamp in (b"1e-100000000", b"0e100000000", b"1.5e-100000000",
                      b"0.00000000000000000000000001"):
            chunk, _ = _parse_chunk(
                _line(0, stamp)[:-1], 0, None, STARTED_NS)
            self.assertEqual(chunk["offset_ns"], 0)
        chunk, _ = _parse_chunk(
            _line(0, b"9223372036.854775806")[:-1], 0, None, 1)
        self.assertEqual(chunk["offset_ns"], 9223372036854775806)
        with self.assertRaises(Exception):
            _parse_chunk(_line(0, b"9223372036.854775807")[:-1], 0, None, 1)

    def test_start_ns_boundary_rejects_overflow_fraction(self):
        with self.assertRaises(Exception):
            _parse_capture(b'{"schema_version":1,'
                           b'"started_utc":"2262-04-11T23:47:16.854775808Z"}')
        capture = _parse_capture(b'{"schema_version":1,'
                                 b'"started_utc":"2262-04-11T23:47:16.854775807Z"}')
        self.assertEqual(capture["started_ns"], 2 ** 63 - 1)

    def test_escaped_backslash_not_surrogate(self):
        # A literal backslash before uXXXX is valid JSON text, not an
        # escape; probe through _parse_capture which accepts extra keys.
        with self.assertRaises(Exception):
            _parse_capture(b'{"schema_version":1,"started_utc":"2026-10-04T07:24:48Z",'
                           b'"vehicle_firmware":"bad\\ud800"}')
        ok = _parse_capture(b'{"schema_version":1,"started_utc":"2026-10-04T07:24:48Z",'
                            b'"vehicle_firmware":"a\\\\ud800b"}')
        self.assertTrue(ok["ready"])

class FileStoreBehavior(unittest.TestCase):
    def test_exact_bytes_hash_and_status(self):
        fix = _Fixture(self)
        self.assertEqual(fix.store.status("new", "serial_chunks.jsonl"),
                         {"version": 1, "session_id": "new",
                          "file": "serial_chunks.jsonl", "offset": 0,
                          "tail_sha256": EMPTY_SHA256})
        body = _line(0, b"0.000000007")
        ack = fix.put("serial_chunks.jsonl", body)
        self.assertEqual(ack["offset"], len(body))
        self.assertEqual(ack["tail_sha256"],
                         hashlib.sha256(body).hexdigest())
        self.assertEqual(fix.store.path("s1", "serial_chunks.jsonl").read_bytes(), body)

    def test_preflight_reads_tail_and_status_keeps_real_hash(self):
        fix = _Fixture(self)
        body = b"x" * 70000
        fix.put(SERIAL_FILE, body)
        self.assertIsNone(fix.store.preflight("s1", SERIAL_FILE))
        self.assertEqual(fix.store.status("s1", SERIAL_FILE)["tail_sha256"],
                         hashlib.sha256(body[-65536:]).hexdigest())
        with patch.object(can_files.os, "pread", return_value=b""):
            with self.assertRaises(can_files.FileRejection) as ctx:
                fix.store.preflight("s1", SERIAL_FILE)
        self.assertEqual((ctx.exception.status, ctx.exception.reason),
                         (503, "archive_unavailable"))

    def test_interrupted_put_recovers_to_committed(self):
        fix = _Fixture(self)
        body = _line(0, b"0.000000007")
        fix.put("serial_chunks.jsonl", body)
        data = fix.store.path("s1", "serial_chunks.jsonl")
        with open(data, "ab") as handle:
            handle.write(b"uncommitted-tail")
        ack = fix.store.status("s1", "serial_chunks.jsonl")
        self.assertEqual((ack["offset"], data.read_bytes()), (len(body), body))

    def test_lost_ack_replay_and_mismatch(self):
        fix = _Fixture(self)
        body = _line(0, b"0.000000007")
        first = fix.put("serial_chunks.jsonl", body)
        replay = fix.put("serial_chunks.jsonl", body, offset=0)
        self.assertEqual(replay, first)
        with self.assertRaises(Exception) as ctx:
            fix.put("serial_chunks.jsonl", body + b"x", offset=0)
        self.assertEqual(ctx.exception.status, 409)
        with self.assertRaises(Exception) as ctx:
            fix.put("serial_chunks.jsonl", _line(9, b"9"), offset=1)
        self.assertEqual(ctx.exception.status, 409)

    def test_digest_mismatch_no_advance(self):
        fix = _Fixture(self)
        body = _line(0, b"0.000000007")
        with self.assertRaises(Exception) as ctx:
            fix.store.put("s1", "serial_chunks.jsonl", 0,
                          "0" * 64, body)
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(fix.store.status("s1", "serial_chunks.jsonl")["offset"], 0)

    def test_bounds_paths_symlinks_and_auth_surface(self):
        fix = _Fixture(self)
        for session, name in [("../x", "serial_chunks.jsonl"),
                              ("s1", "nope.json"), ("s1", "x/../y")]:
            with self.assertRaises(Exception) as ctx:
                fix.store.status(session, name)
            self.assertEqual(ctx.exception.status, 404)
        with self.assertRaises(Exception) as ctx:
            fix.put("serial_chunks.jsonl", b"x" * (4 * 1024 * 1024 + 1))
        self.assertEqual(ctx.exception.status, 413)
        directory = fix.store.root / "synthetic" / "fixture" / "evil"
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        (directory / "serial_chunks.jsonl").symlink_to("/etc/hostname")
        with self.assertRaises(Exception) as ctx:
            fix.store.status("evil", "serial_chunks.jsonl")
        self.assertEqual(ctx.exception.status, 404)
        original = _line(0, b"1")
        fix.put("serial_chunks.jsonl", original, session="safe")
        saved = fix.store.path("safe", "serial_chunks.jsonl")
        committed = saved.with_name(saved.name + ".committed")
        committed.unlink()
        zero = saved.parent / "zero"
        zero.write_bytes(b"0")
        committed.symlink_to(zero)
        with self.assertRaises(Exception) as ctx:
            fix.store.status("safe", "serial_chunks.jsonl")
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(saved.read_bytes(), original)

    def test_disk_reserve_maps_to_507(self):
        fix = _Fixture(self)
        fix.archive.disk_reserve_bytes = 1 << 62
        with self.assertRaises(Exception) as ctx:
            fix.put("serial_chunks.jsonl", _line(0, b"1"))
        self.assertEqual(ctx.exception.status, 507)

    def test_capture_replace_and_snapshot_history(self):
        fix = _Fixture(self)
        first = _capture()
        ack = fix.put("capture.json", first)
        self.assertEqual(ack["offset"], len(first))
        second = _capture(firmware="v2")
        ack = fix.put("capture.json", second)
        self.assertEqual((ack["offset"], ack["tail_sha256"]),
                         (len(second), hashlib.sha256(second).hexdigest()))
        self.assertEqual(fix.store.path("s1", "capture.json").read_bytes(), second)
        snapshots = list((fix.store.root / "synthetic" / "fixture"
                          / "s1" / "snapshots").glob("*.json"))
        self.assertEqual(len(snapshots), 2)
        with self.assertRaises(Exception) as ctx:
            fix.store.put("s1", "capture.json", 3,
                          hashlib.sha256(second).hexdigest(), second)
        self.assertEqual(ctx.exception.status, 409)


class LiveFileHttp(unittest.TestCase):
    def test_get_put_roundtrip_over_http(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory) / "raw.sqlite", disk_reserve_bytes=0)
            archive.register_epoch(synthetic_decoder(directory))
            server = Receiver(("127.0.0.1", 0), archive, "synthetic", "fixture",
                              "fixture-user", "fixture-password", timeout=5)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                import http.client
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)

                def call(method, target, body=None, extra=None):
                    headers = {"Authorization": AUTH_HEADER, **IDENT}
                    if extra:
                        headers.update(extra)
                    conn.request(method, target, body=body, headers=headers)
                    response = conn.getresponse()
                    payload = response.read()
                    return response.status, payload

                status, payload = call(
                    "GET", "/v1/files/s1/serial_chunks.jsonl")
                self.assertEqual((status, json.loads(payload)["offset"]), (200, 0))
                status, _ = call("GET", "/v1/files/s1/nope.json")
                self.assertEqual(status, 404)
                status, _ = call("GET", "/v1/files/s1/serial_chunks.jsonl",
                                 extra={"X-CAN-Vehicle": "other"})
                self.assertEqual(status, 403)
                # Route regression: exactly one separator; queries,
                # traversal, and nesting refuse without touching bytes.
                for bad in ("/v1/files/s1", "/v1/files/s1/",
                            "/v1/files/a/b/c", "/v1/files/s1/x?y=1",
                            "/v1/files/../s1/serial_chunks.jsonl"):
                    status, _ = call("GET", bad)
                    self.assertEqual(status, 404, bad)
                # Capture Content-Length caps at 64KiB before any read.
                import socket
                raw = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
                try:
                    raw.sendall(
                        b"PUT /v1/files/s1/capture.json HTTP/1.1\r\nHost: x\r\n"
                        b"Authorization: " + AUTH_HEADER.encode() + b"\r\n"
                        b"Content-Type: application/octet-stream\r\n"
                        b"Content-Length: 70000\r\n"
                        b"X-CAN-Vehicle: synthetic\r\nX-CAN-Collector: fixture\r\n"
                        b"X-CAN-Offset: 0\r\nX-CAN-SHA256: " + hashlib.sha256(b"x").hexdigest().encode()
                        + b"\r\nConnection: close\r\n\r\n")
                    reply = b""
                    while b"\r\n\r\n" not in reply:
                        chunk = raw.recv(4096)
                        if not chunk:
                            break
                        reply += chunk
                finally:
                    raw.close()
                self.assertIn(b" 413 ", reply.split(b"\r\n", 1)[0])
                status, _ = call("PUT", "/v1/files/s1/serial_chunks.jsonl", body=b"x",
                                 extra={"Content-Type": "application/octet-stream",
                                        "X-CAN-Offset": "0",
                                        "X-CAN-SHA256": hashlib.sha256(b"x").hexdigest()})
                self.assertEqual(status, 200)
                status, payload = call("GET", "/v1/files/s1/serial_chunks.jsonl")
                ack = json.loads(payload)
                self.assertEqual((status, ack["offset"], ack["tail_sha256"]),
                                 (200, 1, hashlib.sha256(b"x").hexdigest()))
            finally:
                conn.close()
                server.shutdown()
                server.server_close()
                thread.join(3)


class FileImporterBehavior(unittest.TestCase):
    def _session(self, fix, session, lines, capture=None):
        directory = fix.store.root / "synthetic" / "fixture" / session
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        body = b"".join(lines)
        default = _capture(raw_hash=hashlib.sha256(body).hexdigest(),
                           extra={"chunks": len(lines), "complete": True})
        (directory / "capture.json").write_bytes(
            capture if capture is not None else default)
        (directory / "serial_chunks.jsonl").write_bytes(body)
        (directory / "serial_chunks.jsonl.committed").write_bytes(str(len(body)).encode())
        return body

    def test_absent_and_unready_capture_do_not_open_import_connection(self):
        fix = _Fixture(self)
        importer = FileImporter(fix.archive)
        directory = fix.store.root / "synthetic" / "fixture" / "s1"
        directory.mkdir()
        with patch.object(importer, "_connect", side_effect=AssertionError("unexpected connection")):
            self.assertFalse(importer._import_session("synthetic", "fixture", "s1"))
            (directory / "capture.json").write_bytes(b'{"schema_version":1}')
            self.assertFalse(importer._import_session("synthetic", "fixture", "s1"))
        self._session(fix, "s1", [_line(0, b"0")], capture=_capture())
        self.assertTrue(importer._import_session("synthetic", "fixture", "s1"))

    def test_idle_preflight_and_capture_error_recovery(self):
        fix = _Fixture(self)
        first = _line(0, b"0")
        self._session(fix, "s1", [first], capture=_capture())
        importer = FileImporter(fix.archive)
        self.assertTrue(importer.import_once())
        self.assertFalse(importer.import_once())

        directory = fix.store.root / "synthetic" / "fixture" / "s1"
        (directory / "capture.json").write_bytes(b"invalid")
        self.assertFalse(importer.import_once())
        self.assertEqual(importer.errors()[0]["file"], "capture.json")
        second = _line(1, b"1")
        (directory / SERIAL_FILE).write_bytes(first + second)
        (directory / (SERIAL_FILE + ".committed")).write_bytes(
            str(len(first + second)).encode())
        (directory / "capture.json").write_bytes(_capture())
        self.assertTrue(importer.import_once())
        self.assertEqual(importer.errors(), [])
        with fix.archive.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0], 2)

    def test_bounded_reads_discovery_fairness_and_restart(self):
        fix = _Fixture(self)
        lines = [_line(i, b"0") for i in range(1200)]
        self._session(fix, "a", lines, capture=_capture())
        path = fix.store.root / "synthetic" / "fixture" / "a" / SERIAL_FILE
        serial_inode = path.stat().st_ino
        pread = os.pread
        serial_reads = []

        def counted(fd, size, offset):
            data = pread(fd, size, offset)
            if os.fstat(fd).st_ino == serial_inode:
                serial_reads.append(len(data))
            return data

        importer = FileImporter(fix.archive, max_lines=32)
        with patch.object(can_files.os, "pread", side_effect=counted):
            self.assertTrue(importer.import_once())
        self.assertLessEqual(sum(serial_reads), 4096)
        self._session(fix, "b", [_line(0, b"0")], capture=_capture())
        for _ in range(17):
            self.assertTrue(importer.import_once())
        with fix.archive.connect() as conn:
            rows = conn.execute(
                "SELECT session_id,next_seq FROM file_import_state ORDER BY session_id"
            ).fetchall()
        self.assertEqual(rows[1]["session_id"], "b")
        self.assertEqual(rows[1]["next_seq"], 1)
        restarted = FileImporter(fix.archive, max_lines=32)
        _drain(restarted)
        with fix.archive.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0], 1201)

    def test_cached_session_revalidates_replaced_directory(self):
        fix = _Fixture(self)
        self._session(fix, "a", [_line(0, b"0"), _line(1, b"0")], capture=_capture())
        importer = FileImporter(fix.archive, max_lines=1)
        self.assertTrue(importer.import_once())
        directory = fix.store.root / "synthetic" / "fixture" / "a"
        moved = Path(fix.tmp.name) / "relocated"
        directory.rename(moved)
        directory.symlink_to(moved, target_is_directory=True)
        self.assertFalse(importer.import_once())
        with fix.archive.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0], 1)

    def test_growing_partial_prefix_and_bounded_drain(self):
        fix = _Fixture(self)
        lines = [_line(0, b"0.000000007"), _line(1, b"0.000000008")]
        body = self._session(fix, "s1", lines)
        importer = FileImporter(fix.archive, max_lines=1)
        self.assertTrue(importer.import_once())
        self.assertTrue(importer.import_once())
        self.assertFalse(importer.import_once())
        with fix.archive.connect() as conn:
            rows = conn.execute("SELECT seq,offset_ns FROM raw_chunks ORDER BY seq").fetchall()
        self.assertEqual([(r[0], r[1]) for r in rows], [(0, 7), (1, 8)])
        partial = body[:len(lines[0]) + 3]
        directory = fix.store.root / "synthetic" / "fixture" / "s2"
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        (directory / "capture.json").write_bytes(_capture())
        (directory / "serial_chunks.jsonl").write_bytes(partial)
        (directory / "serial_chunks.jsonl.committed").write_bytes(str(len(partial)).encode())
        importer = FileImporter(fix.archive)
        for _ in range(10):
            if not importer.import_once():
                break
        self.assertEqual(fix.archive.status()["raw_chunks"], 3)
        self.assertEqual(importer.errors(), [])
        full = b"".join(lines)
        (directory / "serial_chunks.jsonl").write_bytes(full)
        (directory / "serial_chunks.jsonl.committed").write_bytes(str(len(full)).encode())
        for _ in range(10):
            if not importer.import_once():
                break
        self.assertEqual(fix.archive.status()["raw_chunks"], 4)
        self.assertEqual(importer.errors(), [])

    def test_import_cursor_failure_rolls_back_raw_and_pending(self):
        fix = _Fixture(self)
        self._session(fix, "s1", [_line(0, b"0")])
        importer = FileImporter(fix.archive)
        store_cursor = importer._store_cursor

        def fail_after_cursor(*args):
            store_cursor(*args)
            raise RuntimeError("injected failure")

        with patch.object(importer, "_store_cursor", side_effect=fail_after_cursor):
            with self.assertRaisesRegex(RuntimeError, "injected failure"):
                importer.import_once()
        with fix.archive.connect() as conn:
            for table in ("sessions", "raw_chunks", "file_import_state", "decode_pending"):
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0], 0)
        restarted = FileImporter(fix.archive)
        self.assertTrue(restarted.import_once())
        with fix.archive.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_chunks").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT next_seq FROM file_import_state").fetchone()[0], 1)

    def test_legacy_otlp_overlap_dedups_exact_ns(self):
        fix = _Fixture(self)
        lines = [_line(0, b"0.000000007"), _line(1, b"0.000000009")]
        self._session(fix, "s1", lines)
        meta = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
                "session_id": "s1", "started_ns": STARTED_NS, "vehicle_firmware": "synthetic"}
        fix.archive.accept(meta, [{"seq": 0, "offset_ns": 7, "phase": "capture",
                                   "data": b"t12320200\r"}])
        importer = FileImporter(fix.archive)
        _drain(importer)
        self.assertEqual(fix.archive.status()["raw_chunks"], 2)

    def test_invalid_source_isolated_and_replay_harmless(self):
        fix = _Fixture(self)
        bad = b'{"seq":0,"t_s":"nope","phase":"capture","data_b64":"eA=="}\n'
        good = [_line(0, b"0.000000007")]
        self._session(fix, "bad", [bad])
        self._session(fix, "good", good)
        importer = FileImporter(fix.archive)
        _drain(importer)
        self.assertEqual(fix.archive.status()["raw_chunks"], 1)
        errors = {(e["session_id"], e["file"]): e["error"] for e in importer.errors()}
        self.assertEqual(errors[("bad", "serial_chunks.jsonl")], "invalid_chunk")
        self.assertFalse(importer.import_once())

    def test_restart_resumes_cursor_and_final_hash(self):
        fix = _Fixture(self)
        lines = [_line(0, b"0.000000007"), _line(1, b"0.000000008")]
        body = self._session(fix, "s1", lines)
        first = FileImporter(fix.archive, max_lines=1)
        self.assertTrue(first.import_once())
        second = FileImporter(fix.archive)
        self.assertTrue(second.import_once())
        self.assertFalse(second.import_once())
        self.assertEqual(fix.archive.status()["raw_chunks"], 2)
        self.assertEqual(hashlib.sha256(body).hexdigest(),
                         json.loads((fix.store.root / "synthetic" / "fixture"
                                     / "s1" / "capture.json").read_bytes())["raw_file_sha256"])

    def test_metadata_crash_boundaries(self):
        fix = _Fixture(self)
        first = _capture(extra={"note": "v1"})
        fix.put("capture.json", first)
        data = fix.store.path("s1", "capture.json")
        second = _capture(extra={"note": "v2-longer-body"})
        data.write_bytes(second)
        (data.parent / "capture.json.tmp").write_bytes(b"stale-tmp")
        ack = fix.store.status("s1", "capture.json")
        self.assertEqual((ack["offset"], data.read_bytes()), (len(second), second))
        stale = fix.store.root / "synthetic" / "fixture" / "s1" / "snapshots" \
            / (hashlib.sha256(second).hexdigest() + ".json")
        stale.write_bytes(b"torn")
        third = _capture(extra={"note": "v3"})
        fix.put("capture.json", third)
        self.assertEqual(stale.read_bytes(), b"torn")
        ok = fix.store.root / "synthetic" / "fixture" / "s1" / "snapshots" \
            / (hashlib.sha256(third).hexdigest() + ".json")
        self.assertEqual(ok.read_bytes(), third)
        # Same-digest PUT repairs a torn snapshot body before committing.
        fix.put("capture.json", second)
        self.assertEqual(stale.read_bytes(), second)

    def test_partial_tail_waits_and_oversized_rejects(self):
        fix = _Fixture(self)
        tiny = _line(0, b"0.000000007")
        partial = tiny[:len(tiny) - 2]
        directory = fix.store.root / "synthetic" / "fixture" / "wait"
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        (directory / "capture.json").write_bytes(_capture())
        (directory / "serial_chunks.jsonl").write_bytes(partial)
        (directory / "serial_chunks.jsonl.committed").write_bytes(str(len(partial)).encode())
        importer = FileImporter(fix.archive)
        self.assertFalse(importer.import_once())
        self.assertEqual(importer.errors(), [])
        huge = b'{"seq":0,"t_s":0,"phase":"capture","data_b64":"' + b"A" * 200000
        (directory / "serial_chunks.jsonl").write_bytes(huge)
        (directory / "serial_chunks.jsonl.committed").write_bytes(str(len(huge)).encode())
        self.assertFalse(importer.import_once())
        self.assertEqual(importer.errors()[0]["error"], "line_too_long")

    def test_final_gating_metadata_first_and_late_metadata(self):
        fix = _Fixture(self)
        lines = [_line(i, b"%d" % (7 + i)) for i in range(4)]
        body = b"".join(lines)
        early = _capture(extra={"chunks": 4, "complete": True,
                                "raw_file_sha256": hashlib.sha256(body).hexdigest()})
        directory = fix.store.root / "synthetic" / "fixture" / "big"
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        (directory / "capture.json").write_bytes(early)
        (directory / "serial_chunks.jsonl").write_bytes(body[:len(lines[0])])
        (directory / "serial_chunks.jsonl.committed").write_bytes(
            str(len(lines[0])).encode())
        importer = FileImporter(fix.archive, max_lines=1, max_bytes=10 ** 9)
        self.assertTrue(importer.import_once())
        # Only the first prefix consumed: no premature final verdict.
        self.assertEqual(importer.errors(), [])
        (directory / "serial_chunks.jsonl").write_bytes(body)
        (directory / "serial_chunks.jsonl.committed").write_bytes(str(len(body)).encode())
        _drain(importer)
        self.assertEqual(fix.archive.status()["raw_chunks"], 4)
        # Late final metadata after EOF still verifies the consumed prefix.
        late_dir = fix.store.root / "synthetic" / "fixture" / "late"
        late_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        (late_dir / "capture.json").write_bytes(_capture())
        (late_dir / "serial_chunks.jsonl").write_bytes(body)
        (late_dir / "serial_chunks.jsonl.committed").write_bytes(str(len(body)).encode())
        late = FileImporter(fix.archive)
        _drain(late)
        (late_dir / "capture.json").write_bytes(
            _capture(raw_hash=hashlib.sha256(body).hexdigest(),
                     extra={"chunks": 4, "complete": True}))
        _drain(late)
        self.assertEqual(late.errors(), [])
        (late_dir / "capture.json").write_bytes(
            _capture(raw_hash="0" * 64, extra={"chunks": 4, "complete": True}))
        _drain(late)
        self.assertEqual(late.errors()[0]["error"], "hash_mismatch")
        (late_dir / "capture.json").write_bytes(
            _capture(raw_hash=hashlib.sha256(body).hexdigest(),
                     extra={"chunks": 4, "complete": False}))
        _drain(late)
        self.assertEqual(late.errors(), [])

    def test_bounded_verifier_resumes_after_restart(self):
        fix = _Fixture(self)
        lines = [_line(i, b"%d" % (7 + i)) for i in range(6)]
        body = self._session(fix, "s1", lines)
        first = FileImporter(fix.archive, max_bytes=10 ** 9)
        _drain(first)
        key = ("synthetic", "fixture", "s1")
        self.assertIn(key, first._verified)
        done_offset = first._verified[key][2]
        self.assertEqual(done_offset, len(body))
        # A fresh importer restarts the bounded rehash from zero and
        # re-verifies the same committed bytes one slice at a time.
        second = FileImporter(fix.archive, max_bytes=1)
        for _ in range(len(body) + 2):
            second.import_once()
        state = second._verified.get(key)
        self.assertIsNotNone(state)
        self.assertEqual((state[0], state[2]), (len(body), len(body)))
        self.assertFalse(second.import_once())

class FilePreservationBehavior(unittest.TestCase):
    def test_worker_import_ack_and_restart_preserve_originals(self):
        fix = _Fixture(self)
        lines = [_line(i, str(i).encode()).rstrip(b"\n") + b" " * 100000 + b"\n"
                 for i in range(4)]
        body = b"".join(lines)
        capture = _capture(raw_hash=hashlib.sha256(body).hexdigest(),
                           extra={"chunks": 4, "complete": True})
        events = b'{"event":"synthetic"}\n'
        for name, data in (("capture.json", capture), (SERIAL_FILE, body),
                           ("events.jsonl", events)):
            fix.put(name, data)
        sink = HTTPServer(("127.0.0.1", 0), SQLSink)
        sink.mode = "ok"
        thread = threading.Thread(target=sink.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 3)
        self.addCleanup(sink.server_close)
        self.addCleanup(sink.shutdown)
        greptime = Greptime(f"http://127.0.0.1:{sink.server_port}", "synthetic", "test", "test")

        # Observe multiple idle upload cycles after the actual full ACK, including
        # the old periodic-reclamation interval, rather than relying on a sleep.
        idle_after_ack = threading.Event()
        for _ in range(2):
            worker = Worker(fix.archive, fix.decoder, greptime, interval=0.02,
                            importer=FileImporter(fix.archive))
            original_wait = worker._wait_queue
            idle_count = [0]

            def counted_wait(hint, has_work):
                if (threading.current_thread().name == "can-archive-uploader"
                        and fix.archive.status()["counters"]["acked_rows_total"] > 0):
                    idle_count[0] += 1
                    if idle_count[0] >= 3:
                        idle_after_ack.set()
                return original_wait(hint, has_work)

            worker._wait_queue = counted_wait
            idle_after_ack.clear()
            worker.start()
            try:
                self.assertTrue(idle_after_ack.wait(10))
                status = fix.archive.status()
                self.assertEqual(status["raw_chunks"], 4)
                self.assertGreater(status["counters"]["acked_rows_total"], 0)
                self.assertEqual(status["pending_rows"], 0)
                for name, data in (("capture.json", capture), (SERIAL_FILE, body),
                                   ("events.jsonl", events)):
                    self.assertEqual(fix.store.path("s1", name).read_bytes(), data)
                self.assertEqual(fix.put(SERIAL_FILE, body)["offset"], len(body))
            finally:
                worker.stop_event.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())

    def test_historical_intent_blocks_replay_and_whole_file_verification(self):
        fix = _Fixture(self)
        body = _line(0, b"0")
        fix.put("capture.json", _capture(raw_hash=hashlib.sha256(body).hexdigest(),
                                         extra={"chunks": 1, "complete": True}))
        fix.put(SERIAL_FILE, body)
        with fix.archive.connect() as conn:
            conn.execute("INSERT INTO file_reclaim_state "
                         "(vehicle,collector_id,session_id,reclaimed,intent_end) "
                         "VALUES(?,?,?,?,?)", ("synthetic", "fixture", "s1", 0, len(body)))
            conn.commit()
        importer = FileImporter(fix.archive)
        _drain(importer)
        self.assertNotIn(("synthetic", "fixture", "s1"), importer._verified)
        with self.assertRaises(can_files.FileRejection) as conflict:
            fix.put(SERIAL_FILE, body)
        self.assertEqual((conflict.exception.status, conflict.exception.reason), (409, "offset_conflict"))
        self.assertEqual(fix.store.path("s1", SERIAL_FILE).read_bytes(), body)

if __name__ == "__main__":
    unittest.main()
