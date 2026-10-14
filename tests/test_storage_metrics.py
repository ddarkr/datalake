"""Receiver observability and S3 accounting/retained-report behavior."""
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.storage import storage_metrics as sm

ENV_KEYS = ("CAN_RECEIVER_ARCHIVE", "CAN_RECEIVER_DISK_RESERVE_BYTES",
          "STORAGE_COST_FREE_EGRESS_MULTIPLIER",
          "STORAGE_COST_EGRESS_USD_PER_GB",
          "STORAGE_COST_MAX_MONTHLY_EGRESS_GIB",
          "STORAGE_COST_MAX_MONTHLY_OVERAGE_USD")

SCHEMA = """
CREATE TABLE sessions(id INTEGER PRIMARY KEY, vehicle TEXT NOT NULL,
 collector_id TEXT NOT NULL, session_id TEXT NOT NULL, meta_json TEXT NOT NULL,
 next_seq INTEGER NOT NULL DEFAULT 0, last_offset_ns INTEGER NOT NULL DEFAULT -1);
CREATE TABLE raw_chunks(id INTEGER PRIMARY KEY, session INTEGER NOT NULL,
 seq INTEGER NOT NULL, offset_ns INTEGER NOT NULL, phase TEXT NOT NULL,
 data BLOB NOT NULL);
CREATE TABLE outbox(id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
 epoch TEXT NOT NULL, row_json TEXT NOT NULL);
CREATE TABLE worker_errors(kind TEXT PRIMARY KEY, count INTEGER NOT NULL,
 last_ns INTEGER NOT NULL);
"""


def make_archive(directory, rows=(), errors=(), meta=()):
    path = Path(directory) / "raw.sqlite3"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO sessions(vehicle,collector_id,session_id,meta_json) VALUES(?,?,?,?)",
            ("synthetic-vehicle", "synthetic-collector", "s1", "{}"))
        for event_id, event_ns, ingest_ns in rows:
            conn.execute(
                "INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
                (event_id, "synthetic-epoch", json.dumps(
                    {"event_time": event_ns, "ingest_time": ingest_ns,
                     "event_id": event_id, "vehicle": "synthetic-vehicle"})))
        for kind, count, last_ns in errors:
            conn.execute("INSERT INTO worker_errors VALUES(?,?,?)", (kind, count, last_ns))
        if meta:
            conn.execute("CREATE TABLE archive_meta(key TEXT PRIMARY KEY, value INTEGER)")
            for key, value in meta:
                conn.execute("INSERT INTO archive_meta VALUES(?,?)", (key, value))
        conn.commit()
    finally:
        conn.close()
    return str(path)


class ReceiverArchiveTest(unittest.TestCase):
    def setUp(self):
        self._old = {k: os.environ.get(k) for k in ENV_KEYS}
        for k in ENV_KEYS:
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_missing_archive_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(os.path.join(directory, "absent.sqlite3"))
            self.assertEqual(metrics, {"datalake_receiver_archive_known": 0,
                                       "datalake_receiver_archive_success": 0})

    def test_corrupt_archive_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.sqlite3"
            path.write_bytes(b"not a database")
            self.assertEqual(sm.receiver_metrics(str(path))["datalake_receiver_archive_known"], 0)

    def test_pending_oldest_and_errors_without_invented_raw_totals(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 1_700_000_000_000_000_000, 1_700_000_001_000_000_000),
                                                    ("e2", 1_700_000_002_000_000_000, 1_700_000_003_000_000_000)),
                                   errors=(("greptime_timeout", 3, 1_700_000_004_000_000_000),))
            metrics = sm.receiver_metrics(archive)
            self.assertEqual(metrics["datalake_receiver_archive_known"], 1)
            self.assertEqual(metrics["datalake_receiver_pending_rows"], 2)
            self.assertEqual(metrics["datalake_receiver_oldest_pending_id"], 1)
            self.assertAlmostEqual(metrics["datalake_receiver_oldest_pending_event_timestamp_seconds"], 1.7e9)
            self.assertAlmostEqual(metrics["datalake_receiver_oldest_pending_ingest_timestamp_seconds"], 1.700000001e9)
            self.assertEqual(metrics['datalake_receiver_errors_total{kind="greptime_timeout"}'], 3)
            self.assertAlmostEqual(metrics['datalake_receiver_error_last_timestamp_seconds{kind="greptime_timeout"}'], 1.700000004e9)
            self.assertEqual(metrics['datalake_receiver_errors_total{kind="decode_failure"}'], 0)
            # No raw scan here: totals stay unknown until receiver counters exist.
            self.assertNotIn("datalake_receiver_raw_chunks", metrics)
            self.assertNotIn("datalake_receiver_raw_bytes", metrics)
            self.assertNotIn("datalake_receiver_last_receive_timestamp_seconds", metrics)

    def test_empty_outbox_has_no_oldest_or_error_last(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(make_archive(directory))
            self.assertEqual(metrics["datalake_receiver_pending_rows"], 0)
            self.assertNotIn("datalake_receiver_oldest_pending_id", metrics)
            self.assertNotIn("datalake_receiver_oldest_pending_event_timestamp_seconds", metrics)
            self.assertNotIn("datalake_receiver_error_last_timestamp_seconds{kind=\"x\"}", metrics)

    def test_unknown_error_kind_never_leaks(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(
                make_archive(directory, errors=(("custom_secret_stuff", 9, 1_700_000_000_000_000_000),)))
            self.assertNotIn("custom_secret_stuff", " ".join(metrics))
            self.assertEqual(metrics['datalake_receiver_errors_total{kind="greptime_failure"}'], 0)

    def test_archive_meta_counters_and_staged_freshness(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, meta=(
                ("raw_chunks", 40), ("raw_bytes", 8000),
                ("decoded_rows_total", 30), ("acked_rows_total", 28),
                ("last_receive_ns", 1_700_000_000_000_000_000),
                ("last_decode_ns", 1_700_000_001_000_000_000),
                ("bogus_metric", 7)))
            metrics = sm.receiver_metrics(archive)
            self.assertEqual(metrics["datalake_receiver_raw_chunks"], 40)
            self.assertEqual(metrics["datalake_receiver_raw_bytes"], 8000)
            self.assertEqual(metrics["datalake_receiver_decoded_rows_total"], 30)
            self.assertEqual(metrics["datalake_receiver_acked_rows_total"], 28)
            # Receipt vs decode vs commit stays distinguishable.
            self.assertGreater(metrics["datalake_receiver_last_decode_timestamp_seconds"],
                               metrics["datalake_receiver_last_receive_timestamp_seconds"])
            # Absent full-ACK row is unknown, never a fake epoch.
            self.assertNotIn("datalake_receiver_last_full_ack_timestamp_seconds", metrics)
            self.assertNotIn("bogus_metric", " ".join(metrics))

    def test_malformed_archive_meta_is_unknown_not_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 5, 6),), meta=(
                ("raw_chunks", 40), ("last_receive_ns", "not-an-int")))
            self.assertEqual(sm.receiver_metrics(archive),
                             {"datalake_receiver_archive_known": 0,
                              "datalake_receiver_archive_success": 0})

    def test_negative_known_counter_is_unknown_not_omitted_healthy(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 5, 6),), meta=(
                ("raw_chunks", -1), ("raw_bytes", 8000)))
            self.assertEqual(sm.receiver_metrics(archive),
                             {"datalake_receiver_archive_known": 0,
                              "datalake_receiver_archive_success": 0})

    def test_invalid_known_error_count_is_unknown_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, errors=(
                ("greptime_timeout", -4, 1_700_000_000_000_000_000),))
            self.assertEqual(sm.receiver_metrics(archive),
                             {"datalake_receiver_archive_known": 0,
                              "datalake_receiver_archive_success": 0})

    def test_outbox_rows_counter_preferred_over_table_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory, rows=(("e1", 5, 6), ("e2", 7, 8)), meta=(
                ("outbox_rows", 2), ("raw_chunks", 9), ("raw_bytes", 100)))
            metrics = sm.receiver_metrics(archive)
            self.assertEqual(metrics["datalake_receiver_pending_rows"], 2)
            self.assertEqual(metrics["datalake_receiver_oldest_pending_id"], 1)

    def test_wal_metric_is_exact_wal_file_only(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory)
            live = sqlite3.connect(archive)
            try:
                live.execute("PRAGMA journal_mode=WAL")
                live.execute("INSERT INTO sessions(vehicle,collector_id,session_id,meta_json)"
                             " VALUES(?,?,?,?)", ("w", "c", "wal-snapshot", "{}"))
                live.commit()
                live.execute("INSERT INTO sessions(vehicle,collector_id,session_id,meta_json)"
                             " VALUES(?,?,?,?)", ("w", "c", "wal-uncheckpointed", "{}"))
                # No commit/checkpoint: the row sits in the real -wal while the
                # read-only snapshot below must still succeed.
                metrics = sm.receiver_metrics(archive)
                if Path(archive + "-wal").exists():
                    # Exact -wal bytes, never mixed with -shm.
                    self.assertEqual(metrics["datalake_receiver_wal_bytes"],
                                     Path(archive + "-wal").stat().st_size)
                else:
                    self.assertEqual(metrics["datalake_receiver_wal_bytes"], 0)
                if Path(archive + "-shm").exists():
                    self.assertEqual(metrics["datalake_receiver_shm_bytes"],
                                     Path(archive + "-shm").stat().st_size)
            finally:
                live.close()
        with tempfile.TemporaryDirectory() as directory:
            absent = sm.receiver_metrics(make_archive(directory))
            self.assertEqual(absent["datalake_receiver_wal_bytes"], 0)
            self.assertNotIn("datalake_receiver_shm_bytes", absent)

    def test_databases_without_counter_tables_publish_only_direct_facts(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(make_archive(directory, rows=(("e1", 5, 6),)))
            self.assertEqual(metrics["datalake_receiver_sessions"], 1)
            self.assertGreaterEqual(metrics["datalake_receiver_db_bytes"], 0)
            self.assertGreaterEqual(metrics["datalake_receiver_wal_bytes"], 0)
            self.assertGreater(metrics["datalake_receiver_disk_free_bytes"], 0)
            self.assertNotIn("datalake_receiver_disk_reserve_bytes", metrics)

    def test_reserve_from_env_and_explicit_argument(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = make_archive(directory)
            os.environ["CAN_RECEIVER_DISK_RESERVE_BYTES"] = "67108864"
            self.assertEqual(sm.receiver_metrics(archive)["datalake_receiver_disk_reserve_bytes"], 67108864)
            self.assertEqual(sm.receiver_metrics(archive, reserve_bytes=7)["datalake_receiver_disk_reserve_bytes"], 7)
            os.environ["CAN_RECEIVER_DISK_RESERVE_BYTES"] = "not-a-number"
            self.assertNotIn("datalake_receiver_disk_reserve_bytes", sm.receiver_metrics(archive))

    def test_epoch_state_fixed_labels_without_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.sqlite3"
            conn = sqlite3.connect(path)
            try:
                conn.execute("CREATE TABLE epochs(epoch TEXT PRIMARY KEY, mapping_revision TEXT NOT NULL)")
                conn.execute("CREATE TABLE decode_epoch_state(epoch TEXT PRIMARY KEY,"
                             "processed_chunks INTEGER NOT NULL,"
                             "decoded_rows INTEGER NOT NULL, partial_rows INTEGER NOT NULL,"
                             "pending_sessions INTEGER NOT NULL)")
                conn.execute("INSERT INTO epochs VALUES('new-epoch','v1')")
                conn.execute("INSERT INTO epochs VALUES('old-epoch','v1')")
                conn.execute("INSERT INTO decode_epoch_state VALUES('new-epoch',5,50,2,1)")
                conn.execute("INSERT INTO decode_epoch_state VALUES('old-epoch',3,30,0,0)")
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT INTO sessions(vehicle,collector_id,session_id,meta_json) VALUES(?,?,?,?)",
                    ("synthetic-vehicle", "synthetic-collector", "s1", "{}"))
                conn.execute("CREATE TABLE archive_meta(key TEXT PRIMARY KEY, value INTEGER)")
                conn.execute("INSERT INTO archive_meta VALUES('raw_chunks',8)")
                conn.execute("INSERT INTO archive_meta VALUES('sessions_total',1)")
                conn.execute("INSERT INTO archive_meta VALUES('active_epoch','old-epoch')")
                conn.commit()
            finally:
                conn.close()
            metrics = sm.receiver_metrics(str(path))
            # Active = explicit archive_meta selection; labels fixed, never epoch/session/vehicle values.
            self.assertEqual(metrics['datalake_receiver_epoch_processed_chunks{scope="active"}'], 3)
            self.assertEqual(metrics['datalake_receiver_epoch_processed_chunks{scope="retained"}'], 5)
            self.assertEqual(metrics['datalake_receiver_epoch_remaining_chunks{scope="active"}'], 5)
            self.assertEqual(metrics['datalake_receiver_epoch_remaining_chunks{scope="retained"}'], 3)
            self.assertEqual(metrics['datalake_receiver_epoch_decoded_rows_exact{scope="retained"}'], 52)
            self.assertEqual(metrics['datalake_receiver_epoch_active_backlog_chunks{scope="active"}'], 5)
            self.assertEqual(metrics['datalake_receiver_epoch_retained_backlog_chunks{scope="retained"}'], 3)
            self.assertEqual(metrics['datalake_receiver_epoch_retained_backlog_chunks{scope="active"}'], 0)
            self.assertNotIn("new-epoch", json.dumps(metrics))
            self.assertNotIn("old-epoch", json.dumps(metrics))

    def test_legacy_archive_without_epoch_state_omits_epoch_gauges(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = sm.receiver_metrics(make_archive(directory))
            self.assertNotIn("datalake_receiver_epoch_processed_chunks", " ".join(metrics))


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.fail_once = set()

    def head_object(self, Bucket, Key):
        if Key in self.fail_once:
            self.fail_once.discard(Key)
            raise IOError("synthetic throttling")
        return {"ContentLength": len(self.objects[Key]), "ETag": '"synthetic"'}

    def get_object(self, Bucket, Key):
        import io
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        data = Body.read() if hasattr(Body, "read") else Body
        if Key in self.fail_once:
            self.fail_once.discard(Key)
            raise IOError("synthetic denied")
        self.objects[Key] = bytes(data)
        return {}

    def copy_object(self, Bucket, CopySource, Key, **kw):
        self.objects[Key] = self.objects[CopySource["Key"]]
        return {}

    def list_objects_v2(self, **kw):
        return {"Contents": []}


class CountingS3Test(unittest.TestCase):
    def test_get_counts_partial_reads_lines_readinto_and_iteration_once(self):
        from scripts.storage.s3_cost import CountingS3
        data = b"a\nbb\r\nccc\n"
        fake = FakeS3()
        fake.objects["live/a.sst"] = data
        for mode in ("lines", "readinto", "iterator", "readlines", "next", "context"):
            with self.subTest(mode=mode):
                wrapped = CountingS3(fake, "backup")
                body = wrapped.get_object(Bucket="private-bucket", Key="live/a.sst")["Body"]
                self.assertEqual(wrapped.snapshot()["bytes"]["get"]["payload"], 0)
                if mode == "lines":
                    self.assertEqual(list(body.iter_lines(chunk_size=2)), [b"a", b"bb", b"ccc"])
                elif mode == "readinto":
                    buf = bytearray(3)
                    self.assertEqual(body.readinto(buf), 3)
                    self.assertEqual(bytes(buf), data[:3])
                    self.assertEqual(body.read(), data[3:])
                elif mode == "iterator":
                    self.assertEqual(b"".join(body), data)
                elif mode == "readlines":
                    self.assertEqual(b"".join(body.readlines()), data)
                elif mode == "next":
                    self.assertEqual(next(body), data)
                    with self.assertRaises(StopIteration):
                        next(body)
                else:
                    with body as stream:
                        self.assertIs(stream, body)
                        self.assertEqual(stream.read(), data)
                self.assertEqual(wrapped.snapshot()["bytes"]["get"]["payload"], len(data))
                body.close()
                self.assertTrue(body.closed)
        wrapped = CountingS3(fake, "backup")
        body = wrapped.get_object(Bucket="b", Key="live/a.sst")["Body"]
        self.assertEqual(body.read(2), data[:2])
        body.close()
        self.assertEqual(wrapped.snapshot()["bytes"]["get"]["payload"], 2)

    def test_get_sdk_incomplete_body_preserves_consumed_bytes_and_validation(self):
        import io
        from botocore.response import StreamingBody
        from botocore.exceptions import IncompleteReadError
        from scripts.storage.s3_cost import CountingS3
        fake = FakeS3()
        fake.get_object = lambda **kw: {"Body": StreamingBody(io.BytesIO(b"partial"), 20)}
        wrapped = CountingS3(fake, "raw")
        body = wrapped.get_object(Bucket="b", Key="k")["Body"]
        with self.assertRaises(IncompleteReadError):
            body.read()
        snap = wrapped.snapshot()
        self.assertEqual(snap["bytes"]["get"]["payload"], 7)
        self.assertEqual(snap["errors"]["GetObject"], 1)
        self.assertEqual(snap["unknown"]["get"], 1)
        body.close()

    def test_disk_put_counts_consumption_before_eof_and_on_failure(self):
        from scripts.storage.s3_cost import CountingS3
        fake = FakeS3()
        wrapped = CountingS3(fake, "backup")
        with tempfile.TemporaryFile("w+b") as body:
            body.write(b"p" * 50)
            body.seek(0)
            wrapped.put_object(Bucket="b", Key="live/a.sst", Body=body, ContentLength=50)
            self.assertEqual(body.tell(), 50)
        wrapped.put_object(Bucket="b", Key="bid/COMPLETE", Body=b"c" * 8, ContentLength=8)
        wrapped.head_object(Bucket="b", Key="live/a.sst")
        wrapped.copy_object(Bucket="b", CopySource={"Bucket": "b", "Key": "live/a.sst"}, Key="bid/a.sst")
        fake.fail_once.add("failed-file")
        with tempfile.TemporaryFile("w+b") as body:
            body.write(b"z" * 9)
            body.seek(0)
            with self.assertRaises(IOError):
                wrapped.put_object(Bucket="b", Key="failed-file", Body=body)
        snap = wrapped.snapshot()
        self.assertEqual(snap["bytes"]["put"], {"payload": 59, "control": 8})
        self.assertEqual(snap["bytes"]["copy"], {"payload": 50, "control": 0})
        self.assertEqual(snap["errors"]["PutObject"], 1)
        self.assertEqual(snap["measurement"], {"attempts": "logical", "put": "logical-consumed"})
        text = wrapped.render_metrics()
        self.assertIn('datalake_s3_client_write_bytes_total{component="backup"} 67', text)
        self.assertIn('datalake_s3_client_http_attempts_known{component="backup"} 0', text)
        self.assertNotIn("failed-file", text)

    def test_unknown_copy_sizes_resolve_only_latest_destination_without_extra_request(self):
        from scripts.storage.s3_cost import CountingS3
        fake = FakeS3()
        fake.objects["src"] = b"copy-data"
        wrapped = CountingS3(fake, "backup")
        for _ in range(2):
            wrapped.copy_object(Bucket="b", CopySource={"Bucket": "b", "Key": "src"}, Key="dst")
        self.assertEqual(wrapped.snapshot()["unknown"]["copy"], 2)
        self.assertEqual(wrapped.snapshot()["bytes"]["copy"]["payload"], 0)
        self.assertNotIn("HeadObject", wrapped.snapshot()["requests"])
        wrapped.head_object(Bucket="b", Key="dst")
        # A HEAD cannot prove the size of a previous overwritten destination.
        self.assertEqual(wrapped.snapshot()["unknown"]["copy"], 1)
        self.assertEqual(wrapped.snapshot()["bytes"]["copy"]["payload"], len(b"copy-data"))

    def test_known_copy_overwrite_never_resolves_earlier_unknown_copy(self):
        from scripts.storage.s3_cost import CountingS3
        fake = FakeS3()
        fake.objects.update(first=b"short", second=b"longer-copy")
        wrapped = CountingS3(fake, "backup")
        wrapped.copy_object(Bucket="b", CopySource={"Bucket": "b", "Key": "first"}, Key="dst")
        wrapped.head_object(Bucket="b", Key="second")
        wrapped.copy_object(Bucket="b", CopySource={"Bucket": "b", "Key": "second"}, Key="dst")
        wrapped.head_object(Bucket="b", Key="dst")
        self.assertEqual(wrapped.snapshot()["unknown"]["copy"], 1)
        self.assertEqual(wrapped.snapshot()["bytes"]["copy"]["payload"], len(b"longer-copy"))

    def test_actual_send_attempts_exclude_prehash_and_retain_failed_prefix(self):
        import io
        from types import SimpleNamespace
        from botocore.hooks import HierarchicalEmitter
        from scripts.storage.s3_cost import CountingS3

        class EventClient:
            def __init__(self):
                self.meta = SimpleNamespace(events=HierarchicalEmitter())
                self.fail = False
                self.retry = False

            def put_object(self, **kw):
                # SDK checksum/signing reads occur BEFORE before-send.
                source = kw["Body"]
                data = source.read()
                source.seek(0)
                request = SimpleNamespace(body=source)
                self.meta.events.emit("before-send.s3.PutObject", request=request)
                if self.fail or self.retry:
                    request.body.read(3)
                    self.meta.events.emit("needs-retry.s3.PutObject", attempts=1)
                    if self.fail:
                        raise IOError("failed after prefix consumption")
                    request.body.seek(0)
                    self.meta.events.emit("before-send.s3.PutObject", request=request)
                self.asserted = request.body.read()
                self.meta.events.emit("needs-retry.s3.PutObject", attempts=2 if self.retry else 1)
                return {"ChecksumSHA256": "unchanged", "data": data}

        client = EventClient()
        wrapped = CountingS3(client, "raw")
        self.assertEqual(wrapped.put_object(Bucket="b", Key="k", Body=io.BytesIO(b"123456"))["ChecksumSHA256"], "unchanged")
        snap = wrapped.snapshot()
        self.assertEqual(snap["bytes"]["put"]["payload"], 6)
        self.assertEqual(snap["attempts"]["PutObject"], 1)
        self.assertEqual(snap["retries"].get("PutObject", 0), 0)
        client.retry = True
        wrapped.put_object(Bucket="b", Key="k", Body=io.BytesIO(b"123456"))
        self.assertEqual(wrapped.snapshot()["bytes"]["put"]["payload"], 15)
        self.assertEqual(wrapped.snapshot()["attempts"]["PutObject"], 3)
        self.assertEqual(wrapped.snapshot()["retries"]["PutObject"], 1)
        client.fail = True
        with self.assertRaises(IOError):
            wrapped.put_object(Bucket="b", Key="k", Body=io.BytesIO(b"123456"))
        self.assertEqual(wrapped.snapshot()["bytes"]["put"]["payload"], 18)
        self.assertEqual(wrapped.snapshot()["errors"]["PutObject"], 1)

    def test_real_boto_file_put_head_get_and_head_retry(self):
        import base64
        import hashlib
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import boto3
        from botocore.config import Config
        from scripts.storage.s3_cost import CountingS3
        objects = {}
        head_retry = [True]

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def reply(self, status, data=b"", length=None):
                self.send_response(status)
                self.send_header("Content-Length", str(len(data) if length is None else length))
                self.send_header("ETag", '"synthetic"')
                self.end_headers()
                if data:
                    self.wfile.write(data)

            def do_PUT(self):
                data = self.rfile.read(int(self.headers["Content-Length"]))
                expected = base64.b64encode(hashlib.md5(data).digest()).decode()
                if self.headers.get("Content-MD5") != expected:
                    self.reply(400, b"<Error><Code>BadDigest</Code></Error>")
                    return
                objects[self.path] = data
                self.reply(200)

            def do_HEAD(self):
                if self.path.endswith("/retry") and head_retry[0]:
                    head_retry[0] = False
                    self.reply(503)
                    return
                self.reply(200, length=len(objects.get(self.path, b"")))

            def do_GET(self):
                self.reply(200, objects[self.path])

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = boto3.client("s3", endpoint_url="http://127.0.0.1:%d" % server.server_port,
                    region_name="us-east-1", aws_access_key_id="synthetic", aws_secret_access_key="synthetic",
                    config=Config(retries={"mode": "standard", "max_attempts": 1},
                         request_checksum_calculation="when_required", response_checksum_validation="when_required",
                         s3={"addressing_style": "path", "payload_signing_enabled": True}))
        try:
            wrapped = CountingS3(client, "raw")
            data = b"native-a\nnative-b\r\nlast\n"
            with tempfile.TemporaryFile("w+b") as body:
                body.write(data)
                body.seek(0)
                digest = base64.b64encode(hashlib.md5(data).digest()).decode()
                wrapped.put_object(Bucket="synthetic", Key="payload", Body=body, ContentMD5=digest)
                self.assertEqual(body.tell(), len(data))
            wrapped.head_object(Bucket="synthetic", Key="payload")
            snap = wrapped.snapshot()
            self.assertEqual(snap["requests"]["HeadObject"], 1)
            self.assertEqual(snap["attempts"]["HeadObject"], 1)
            self.assertEqual(snap["retries"].get("HeadObject", 0), 0)
            self.assertEqual(snap["bytes"]["put"]["payload"], len(data))
            self.assertEqual(objects["/synthetic/payload"], data)
            body = wrapped.get_object(Bucket="synthetic", Key="payload")["Body"]
            self.assertEqual(wrapped.snapshot()["bytes"]["get"]["payload"], 0)
            self.assertEqual(list(body.iter_lines(chunk_size=4)), [b"native-a", b"native-b", b"last"])
            body.close()
            self.assertEqual(wrapped.snapshot()["bytes"]["get"]["payload"], len(data))
            body = wrapped.get_object(Bucket="synthetic", Key="payload")["Body"]
            buf = bytearray(5)
            self.assertEqual(body.readinto(buf), 5)
            self.assertEqual(bytes(buf) + b"".join(body), data)
            body.close()
            self.assertEqual(wrapped.snapshot()["bytes"]["get"]["payload"], 2 * len(data))
            wrapped.head_object(Bucket="synthetic", Key="retry")
            snap = wrapped.snapshot()
            self.assertEqual(snap["requests"]["HeadObject"], 2)
            self.assertEqual(snap["attempts"]["HeadObject"], 3)
            self.assertEqual(snap["retries"]["HeadObject"], 1)
            self.assertEqual(snap["measurement"]["put"], "request-stream")
        finally:
            client.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_persist_overwrites_one_run_atomically_and_privately(self):
        import stat
        from unittest.mock import patch
        from scripts.storage.s3_cost import CountingS3, persist_cost, load_cost_reports
        with tempfile.TemporaryDirectory() as ops:
            wrapped = CountingS3(FakeS3(), "can-backup")
            wrapped.put_object(Bucket="private-bucket", Key="private-key", Body=b"ab")
            path = persist_cost(ops, "can-backup", "run-1", wrapped.snapshot())
            self.assertEqual(stat.S_IMODE(Path(path).stat().st_mode), 0o600)
            original = Path(path).read_bytes()
            wrapped.put_object(Bucket="private-bucket", Key="private-key2", Body=b"abcd")
            with patch("scripts.storage.s3_cost.os.replace", side_effect=OSError("synthetic replace failure")):
                with self.assertRaises(OSError):
                    persist_cost(ops, "can-backup", "run-1", wrapped.snapshot())
            self.assertEqual(Path(path).read_bytes(), original)
            self.assertFalse(any(name.startswith(".s3-cost-tmp-") for name in os.listdir(ops)))
            persist_cost(ops, "can-backup", "run-1", wrapped.snapshot())
            reports = load_cost_reports(ops)
            self.assertEqual((reports["valid"], reports["invalid"]), (1, 0))
            self.assertEqual(reports["totals"]["can-backup"]["write_bytes"], 6)
            self.assertFalse(reports["complete"])
            self.assertNotIn("private-bucket", Path(path).read_text())
            self.assertNotIn("private-key", Path(path).read_text())
            with self.assertRaises(ValueError):
                persist_cost(ops, "backup", "run-2", wrapped.snapshot())
            with self.assertRaises(ValueError):
                persist_cost(ops, "can-backup", "../escape", wrapped.snapshot())

    def test_report_cap_invalid_stale_missing_never_claim_complete_month(self):
        import time
        from unittest.mock import patch
        from scripts.storage.s3_cost import CountingS3, persist_cost, load_cost_reports
        stamp = time.time() - 1
        with tempfile.TemporaryDirectory() as ops:
            wrapped = CountingS3(FakeS3(), "backup")
            wrapped.put_object(Bucket="b", Key="k", Body=b"x")
            with patch("scripts.storage.s3_cost.time.time", return_value=stamp):
                for index in range(65):
                    persist_cost(ops, "backup", "run-%03d" % index, wrapped.snapshot())
            reports = load_cost_reports(ops)
            self.assertEqual((reports["valid"], reports["invalid"]), (64, 1))
            self.assertTrue(reports["truncated"])
            self.assertFalse(reports["complete"])
            metrics = sm.cost_report_metrics(dirs=[ops])
            self.assertEqual(metrics["datalake_storage_cost_report_known"], 0)
            self.assertEqual(metrics["datalake_storage_cost_report_complete"], 0)
            self.assertEqual(metrics["datalake_storage_cost_report_truncated"], 1)
            self.assertEqual(metrics['datalake_storage_cost_report_retained_write_bytes{component="backup"}'], 64)
            Path(ops, "s3-cost-backup-run-064.json").unlink()
            self.assertEqual(sm.cost_report_metrics(dirs=[ops])["datalake_storage_cost_report_known"], 1)
            self.assertEqual(sm.cost_report_metrics(dirs=[ops])["datalake_storage_cost_report_complete"], 0)
            stale = sm.cost_report_metrics(dirs=[ops], now=stamp + 49 * 3600)
            self.assertEqual(stale["datalake_storage_cost_report_known"], 0)
            path = Path(ops, "s3-cost-backup-run-000.json")
            os.utime(path, (stamp + 49 * 3600, stamp + 49 * 3600))
            self.assertEqual(sm.cost_report_metrics(dirs=[ops], now=stamp + 49 * 3600)["datalake_storage_cost_report_timestamp_seconds"], stamp)
            missing = sm.cost_report_metrics(dirs=[ops, str(Path(ops, "missing"))])
            self.assertEqual(missing["datalake_storage_cost_report_known"], 0)
            path.write_text("[]")
            self.assertEqual(sm.cost_report_metrics(dirs=[ops])["datalake_storage_cost_report_known"], 0)
            self.assertEqual(load_cost_reports(ops)["invalid"], 1)

    def test_untrusted_reports_never_leak_operation_labels_or_follow_symlinks(self):
        from scripts.storage.s3_cost import CountingS3, persist_cost, load_cost_reports
        with tempfile.TemporaryDirectory() as ops:
            snapshot = CountingS3(FakeS3(), "backup").snapshot()
            snapshot["requests"]["secret-bucket/key"] = 1
            with self.assertRaises(ValueError):
                persist_cost(ops, "backup", "run-1", snapshot)
            wrapped = CountingS3(FakeS3(), "backup")
            path = Path(persist_cost(ops, "backup", "run-1", wrapped.snapshot()))
            data = json.loads(path.read_text())
            data["snapshot"]["requests"]["secret-bucket/key"] = 2
            path.write_text(json.dumps(data))
            Path(ops, "s3-cost-backup-run-2.json").symlink_to(path)
            Path(ops, "s3-cost-backup-run-3.json").write_bytes(b"x" * 65537)
            metrics = sm.cost_report_metrics(dirs=[ops])
            self.assertEqual(load_cost_reports(ops)["invalid"], 3)
            self.assertEqual(metrics["datalake_storage_cost_report_known"], 0)
            self.assertNotIn("secret-bucket", json.dumps(metrics))
            self.assertFalse(any("retained_read_bytes" in name for name in metrics))

    def test_fresh_directory_does_not_hide_stale_or_empty_other_scope(self):
        import time
        from unittest.mock import patch
        from scripts.storage.s3_cost import CountingS3, persist_cost
        now = time.time()
        with tempfile.TemporaryDirectory() as fresh, tempfile.TemporaryDirectory() as other:
            snapshot = CountingS3(FakeS3(), "backup").snapshot()
            with patch("scripts.storage.s3_cost.time.time", return_value=now - 1):
                persist_cost(fresh, "backup", "fresh", snapshot)
            self.assertEqual(sm.cost_report_metrics(dirs=[fresh, other], now=now)["datalake_storage_cost_report_known"], 0)
            with patch("scripts.storage.s3_cost.time.time", return_value=now - 49 * 3600):
                persist_cost(other, "backup", "stale", snapshot)
            metrics = sm.cost_report_metrics(dirs=[fresh, other], now=now)
            self.assertEqual(metrics["datalake_storage_cost_report_known"], 0)
            self.assertEqual(metrics["datalake_storage_cost_report_complete"], 0)

    def test_literal_failure_unknown_is_exported(self):
        from scripts.storage.s3_cost import CountingS3
        fake = FakeS3()
        fake.fail_once.add("denied")
        wrapped = CountingS3(fake, "raw")
        with self.assertRaises(IOError):
            wrapped.put_object(Bucket="b", Key="denied", Body=b"unobservable")
        self.assertEqual(wrapped.snapshot()["unknown"]["put"], 1)
        self.assertIn('datalake_s3_client_unknown_byte_operations{component="raw",kind="put"} 1', wrapped.render_metrics())

    def test_get_error_response_consumption_is_explicitly_unknown(self):
        from scripts.storage.s3_cost import CountingS3
        fake = FakeS3()
        def denied(**kwargs):
            raise IOError("synthetic HTTP error response")
        fake.get_object = denied
        wrapped = CountingS3(fake, "raw")
        with self.assertRaises(IOError):
            wrapped.get_object(Bucket="b", Key="denied")
        self.assertEqual(wrapped.snapshot()["unknown"]["get"], 1)
        self.assertEqual(wrapped.snapshot()["errors"]["GetObject"], 1)

    def test_policy_defaults_only_when_unset_and_invalid_values_remain_unknown(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {}, clear=True):
            defaults = sm.cost_policy_metrics()
            self.assertEqual(defaults["datalake_storage_cost_free_egress_multiplier"], 3)
            self.assertEqual(defaults["datalake_storage_cost_max_monthly_overage_usd"], 0)
            self.assertEqual(defaults["datalake_storage_cost_policy_known"], 1)
            os.environ["STORAGE_COST_MAX_MONTHLY_OVERAGE_USD"] = "bogus"
            os.environ["STORAGE_COST_EGRESS_USD_PER_GB"] = "nan"
            os.environ["STORAGE_COST_MAX_MONTHLY_EGRESS_GIB"] = "-5"
            metrics = sm.cost_policy_metrics()
            self.assertEqual(metrics["datalake_storage_cost_policy_known"], 0)
            self.assertNotIn("datalake_storage_cost_max_monthly_overage_usd", metrics)
            self.assertNotIn("datalake_storage_cost_egress_usd_per_gb", metrics)
            self.assertNotIn("datalake_storage_cost_max_monthly_egress_bytes", metrics)


if __name__ == "__main__":
    raise SystemExit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)
