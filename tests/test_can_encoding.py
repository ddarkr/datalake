"""Synthetic single-encode flush regression for issue #10 (no production data).

Compares the working-tree flush_once request bytes against an independent
old-construction oracle (local literal rendering + stdlib form encoding,
never the module's helpers), and exercises the budget boundaries and
failure paths. No gates executed here; run:

  python -m tests.test_can_encoding
"""
import json
import socket
import tempfile
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch

from scripts.ingest.can.can_receiver import (
    COLUMNS,
    Archive,
    DownstreamError,
    Greptime,
)
from tests.test_can_receiver import synthetic_decoder


def _old_literal(field, value):
    """Independent replica of the pre-change SQL literal contract."""
    if value is None:
        return "NULL"
    if field in {"event_time", "ingest_time"}:
        return str(value)
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    if type(value) in (int, float):
        return repr(value)
    return "'" + value.replace("'", "''") + "'"


def _old_body(rows):
    """Independent replica of the old two-pass construction (render + encode)."""
    prefix = 'INSERT INTO "vehicle_signal" (' + ",".join('"' + c + '"' for c in COLUMNS) + ") VALUES "
    cells = ["(" + ",".join(_old_literal(c, row[c]) for c in COLUMNS) + ")" for row in rows]
    return urllib.parse.urlencode({"sql": prefix + ",".join(cells)}).encode()


class RecordingSink(BaseHTTPRequestHandler):
    """Per-server request recorder; bodies live on the server, not globals."""

    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        payload = self.rfile.read(length)
        self.server.requests.append(payload)
        if self.server.mode == "outage":
            body = b"sensitive downstream error body"
            self.send_response(503)
        else:
            try:
                params = urllib.parse.parse_qs(payload.decode("ascii"))
                submitted = params["sql"][0].count("),(") + 1
            except (ValueError, KeyError, IndexError):
                submitted = -1
            if submitted < 0:
                body = b"sensitive downstream error body"
                self.send_response(503)
            else:
                reported = 0 if self.server.mode == "partial" else submitted
                body = json.dumps({"code": 0, "output": [{"affectedrows": reported}]}).encode()
                self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve(mode="ok"):
    sink = HTTPServer(("127.0.0.1", 0), RecordingSink)
    sink.mode = mode
    sink.requests = []
    thread = Thread(target=sink.serve_forever, daemon=True)
    thread.start()
    return sink, thread


def _stop(sink, thread):
    sink.shutdown()
    sink.server_close()
    thread.join(3)


def _seed(directory, name, rows):
    decoder = synthetic_decoder(directory)
    archive = Archive(Path(directory) / name, disk_reserve_bytes=0)
    archive.register_epoch(decoder)
    with archive.connect() as conn:
        conn.executemany(
            "INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
            [(r["event_id"], decoder.epoch, json.dumps(r)) for r in rows],
        )
        conn.commit()
    return archive


def _greptime(sink, **kwargs):
    return Greptime("http://127.0.0.1:%d" % sink.server_port,
                    "synthetic", "test", "test", timeout=60, **kwargs)


def _sql_of(body):
    return urllib.parse.parse_qs(body.decode("ascii"))["sql"][0]


def base_row(**over):
    row = {c: None for c in COLUMNS}
    row.update({
        "event_time": 1800000000000000000,
        "vehicle": "synthetic",
        "path": "Vehicle.CAN.x123.Power",
        "source": "can",
        "event_id": "base",
        "decode_epoch": "synthetic-v1",
        "ingest_time": 1800000000000000001,
        "collector_id": "fixture",
        "quality": "reported_unverified",
        "mapping_revision": "synthetic-v1",
        "vehicle_firmware": "synthetic",
    })
    row.update(over)
    return row


def outbox_jsons(archive):
    with archive.connect() as conn:
        return [r[0] for r in conn.execute("SELECT row_json FROM outbox ORDER BY id")]


class EncodingEquivalence(unittest.TestCase):
    def specified_rows(self):
        return [
            base_row(event_id="ascii", value_text="plain power 12.5 kW"),
            base_row(event_id="korean", value_text="출력 12.5 킬로와트"),
            base_row(event_id="quote", value_text="o'clock"),
            base_row(event_id="spaces", value_text="a b\tc"),
            base_row(event_id="specials", value_text="100% & = + ? # / \\"),
            base_row(event_id="nulls"),
            base_row(event_id="bool-t", value_bool=True),
            base_row(event_id="bool-f", value_bool=False),
            base_row(event_id="int", value_num=42),
            base_row(event_id="float", value_num=12.5),
            base_row(event_id="neg", value_num=-0.5),
            base_row(event_id="emoji", value_text="🔋⚡한글 mixed %&=+'\""),
            base_row(event_id="plus", value_text="a+b", path="A B/C+%"),
        ]

    def assert_signal_then_dirty(self, requests, rows):
        """Signal body first, dirty notify second; signal bytes match the oracle."""
        self.assertEqual(len(requests), 2)
        self.assertIn('"vehicle_signal" (', _sql_of(requests[0]))
        self.assertIn('"vehicle_signal_dirty"', _sql_of(requests[1]))
        self.assertEqual(requests[0], _old_body(rows))

    def test_specified_values_byte_identical(self):
        rows = self.specified_rows()
        with tempfile.TemporaryDirectory() as directory:
            archive = _seed(directory, "raw.sqlite", rows)
            sink, thread = _serve()
            try:
                self.assertEqual(archive.flush_once(_greptime(sink)), len(rows))
                self.assert_signal_then_dirty(sink.requests, rows)
            finally:
                _stop(sink, thread)

    def test_flush_bytes_match_old_construction(self):
        rows = []
        for i in range(25):
            if i % 3 == 0:
                rows.append(base_row(event_id="e%d" % i, value_text="출력-%d 100%% &=" % i))
            elif i % 3 == 1:
                rows.append(base_row(event_id="e%d" % i, value_num=float(i) + 0.5))
            else:
                rows.append(base_row(event_id="e%d" % i, value_bool=bool(i % 2)))
        with tempfile.TemporaryDirectory() as directory:
            archive = _seed(directory, "raw.sqlite", rows)
            sink, thread = _serve()
            try:
                self.assertEqual(archive.flush_once(_greptime(sink)), len(rows))
                self.assert_signal_then_dirty(sink.requests, rows)
            finally:
                _stop(sink, thread)

    def test_budget_boundaries_and_limit(self):
        rows = [base_row(event_id="e%d" % i, value_text="text-%d" % i) for i in range(3)]
        two, three = len(_old_body(rows[:2])), len(_old_body(rows))
        self.assertLess(two, three)
        with tempfile.TemporaryDirectory() as directory:
            for delta, want in ((0, 2), (-1, 1), (1, 2)):
                archive = _seed(directory, "b%d.sqlite" % delta, rows)
                sink, thread = _serve()
                try:
                    self.assertEqual(
                        archive.flush_once(_greptime(sink, max_body_bytes=two + delta)), want)
                    # Recorded signal body is exactly the oracle's fitting prefix.
                    self.assertEqual(len(sink.requests), 2)
                    self.assertEqual(sink.requests[0], _old_body(rows[:want]))
                finally:
                    _stop(sink, thread)
            # Count limit selects the same ordered prefix as the old construction.
            archive = _seed(directory, "limit.sqlite", rows)
            sink, thread = _serve()
            try:
                self.assertEqual(archive.flush_once(_greptime(sink), limit=2), 2)
                self.assertEqual(len(sink.requests), 2)
                self.assertEqual(sink.requests[0], _old_body(rows[:2]))
            finally:
                _stop(sink, thread)

    def test_oversize_head_preserved(self):
        big = base_row(event_id="oversized-head", value_text="x" * 4000)
        small = base_row(event_id="after", value_text="kept")
        with tempfile.TemporaryDirectory() as directory:
            archive = _seed(directory, "raw.sqlite", [big, small])
            before = outbox_jsons(archive)
            stuck = Greptime("http://127.0.0.1:9", "synthetic", "test", "test",
                             max_body_bytes=len(_old_body([big])) - 1)
            with self.assertRaisesRegex(DownstreamError, "^greptime_row_too_large$"):
                archive.flush_once(stuck)
            self.assertEqual(outbox_jsons(archive), before)

    def test_partial_timeout_dirty_keep_outbox(self):
        rows = [base_row(event_id="e%d" % i, value_text="출력-%d" % i) for i in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            archive = _seed(directory, "raw.sqlite", rows)
            before = outbox_jsons(archive)
            sink, thread = _serve(mode="partial")
            try:
                with self.assertRaisesRegex(DownstreamError, "^greptime_partial_ack$"):
                    archive.flush_once(_greptime(sink))
                # Only the signal attempt went out; no dirty notify, nothing deleted.
                self.assertEqual(len(sink.requests), 1)
                self.assertEqual(sink.requests[0], _old_body(rows))
                self.assertEqual(outbox_jsons(archive), before)
            finally:
                _stop(sink, thread)
            held = Greptime("http://127.0.0.1:9", "synthetic", "test", "test", timeout=60)
            with patch.object(Greptime, "_connect", side_effect=socket.timeout):
                with self.assertRaisesRegex(DownstreamError, "^greptime_timeout$"):
                    archive.flush_once(held)
            self.assertEqual(outbox_jsons(archive), before)
            # Dirty-notify failure also leaves the full signal batch durable.
            sink, thread = _serve()
            try:
                with patch.object(Greptime, "send_body", side_effect=[len(rows), 0]):
                    with self.assertRaisesRegex(DownstreamError, "^dirty_notify_failure$"):
                        archive.flush_once(_greptime(sink))
                self.assertEqual(outbox_jsons(archive), before)
            finally:
                _stop(sink, thread)


if __name__ == "__main__":
    unittest.main()
