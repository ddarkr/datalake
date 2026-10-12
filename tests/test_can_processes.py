"""Split-process regressions: independent serve + work CLI processes.

Synthetic loopback only. Real subprocesses, real HTTP, real SQLite durability.
Run: python -m unittest discover -s tests -p test_can_processes.py
- serve: --database --vehicle --collector-id --bind --port --http-timeout
  --disk-reserve-mib, CAN_OTLP_* only, ready {"receiver":"ready","port":N}.
- work: --database --dbc --definitions --greptime-url --greptime-db plus
  worker options (--worker-interval --greptime-timeout --outbox-limit
  --max-body-bytes), GREPTIME_* only, ready {"worker":"ready"}.
- status unchanged from the published base (sessions/raw_chunks/pending_rows/
  epochs[{epoch,processed_chunks,remaining_chunks,decoded_rows,counts}]);
  re-decode takes the worker role lock.
- serve holds SH legacy .lock + EX .receiver.lock; work holds SH legacy
  .lock + EX .worker.lock; init serialized via bounded .init.lock wait.
"""
import base64
import gzip
import json
import os
import fcntl
import queue
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

from scripts.storage.can_backup import _lock_held
from tests.test_can_receiver import encode_batch, synthetic_decoder

ROOT = Path(__file__).resolve().parents[1]
FRAME = b"t12320200\r"


def _port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _meta(session, start=1800000000000000000):
    return {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
            "session_id": session, "started_ns": start, "vehicle_firmware": "synthetic"}


def _chunks(count, start_seq=0):
    return [{"seq": start_seq + i, "offset_ns": (start_seq + i) * 1000,
             "phase": "capture", "data": FRAME} for i in range(count)]


class _Proc:
    """A live CLI role process with bounded ready-line detection.

    Readiness reads run on a daemon thread feeding a bounded queue, so a
    silent/hung child can never block the deadline: on failure the child is
    killed, reaped, and pipes closed.
    """

    def __init__(self, argv, env):
        self.proc = subprocess.Popen(
            argv, cwd=str(ROOT), env=env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True)
        self._lines = queue.Queue()
        self._reader = threading.Thread(target=self._drain, daemon=True)
        self._reader.start()
        self.ready = None

    def _drain(self):
        try:
            for line in self.proc.stdout:
                self._lines.put(line)
        except (OSError, ValueError):
            pass

    def wait_ready(self, key, seconds=60):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                line = self._lines.get(timeout=max(0.0, deadline - time.monotonic()))
            except queue.Empty:
                break
            try:
                payload = json.loads(line.strip())
            except ValueError:
                continue
            if key in payload:
                self.ready = payload
                return payload
        self.stop()
        raise AssertionError("role did not print ready %s" % key)

    def stop(self, timeout=20):
        try:
            if self.proc.poll() is None:
                self.proc.send_signal(signal.SIGTERM)
                self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=timeout)
        finally:
            try:
                self.proc.stdout.close()
            except (OSError, ValueError):
                pass
            self._reader.join(timeout=5)
        return self.proc.returncode


def _serve_argv(db, port):
    return [sys.executable, "-m", "scripts.ingest.can.can_receiver", "serve",
            "--database", str(db), "--vehicle", "synthetic",
            "--collector-id", "fixture", "--bind", "127.0.0.1",
            "--port", str(port), "--http-timeout", "10",
            "--disk-reserve-mib", "0"]


def _serve_env(extra=None):
    env = dict(os.environ)
    env.pop("GREPTIME_USER", None)
    env.pop("GREPTIME_PASSWORD", None)
    env["CAN_OTLP_USER"] = "proc-user"
    env["CAN_OTLP_PASSWORD"] = "proc-pass-1234"
    if extra:
        env.update(extra)
    return env


def _work_argv(directory, db, greptime_url, extra_args=()):
    dbc = Path(str(directory)) / "synthetic.dbc"
    definitions = Path(str(directory)) / "synthetic.json"
    return [sys.executable, "-m", "scripts.ingest.can.can_receiver", "work",
            "--database", str(db), "--dbc", str(dbc),
            "--definitions", str(definitions),
            "--greptime-url", greptime_url, "--greptime-db", "synthetic",
            "--worker-interval", "0.05", "--disk-reserve-mib", "0",
            *extra_args]


def _work_env(extra=None):
    env = dict(os.environ)
    env.pop("CAN_OTLP_USER", None)
    env.pop("CAN_OTLP_PASSWORD", None)
    env["GREPTIME_USER"] = "u"
    env["GREPTIME_PASSWORD"] = "p"
    if extra:
        env.update(extra)
    return env


def _serve(directory, db, port, extra_env=None):
    proc = _Proc(_serve_argv(db, port), _serve_env(extra_env))
    proc.wait_ready("receiver")
    return proc


def _work(directory, db, greptime_url, extra_args=(), extra_env=None):
    synthetic_decoder(str(directory))
    proc = _Proc(_work_argv(directory, db, greptime_url, extra_args), _work_env(extra_env))
    proc.wait_ready("worker")
    return proc


def _status(db):
    out = subprocess.run(
        [sys.executable, "-m", "scripts.ingest.can.can_receiver",
         "status", "--database", str(db)],
        cwd=str(ROOT), capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stdout + out.stderr
    return json.loads(out.stdout)


def _wait_status(db, predicate, seconds=60, label="durable state"):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = _status(db)
        if predicate(state):
            return state
        time.sleep(0.1)
    raise AssertionError("split processes did not reach " + label)


def _read_only_rows(db):
    uri = Path(db).as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        return [json.loads(row[0]) for row in
                conn.execute("SELECT row_json FROM outbox ORDER BY id")]
    finally:
        conn.close()


def _partial_rows(db):
    uri = Path(db).as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        return conn.execute("SELECT COALESCE(SUM(rows_emitted),0) FROM decode_partial").fetchone()[0]
    finally:
        conn.close()


def _outbox_rows(db):
    uri = Path(db).as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        return conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    finally:
        conn.close()

def _post(port, meta, chunks, timeout=30):
    auth = "Basic " + base64.b64encode(b"proc-user:proc-pass-1234").decode()
    request = urllib.request.Request(
        "http://127.0.0.1:%d/v1/logs" % port,
        data=gzip.compress(encode_batch(meta, chunks)),
        headers={"Authorization": auth, "Content-Type": "application/x-protobuf",
                 "Content-Encoding": "gzip"}, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        assert response.status == 200
        return response.read()


class SplitProcesses(unittest.TestCase):
    def test_serve_without_dbc_greptime_ack_is_durable(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            port = _port()
            serve = _serve(directory, db, port)
            try:
                # No DBC files needed by serve; no GREPTIME_* in env.
                self.assertFalse((Path(directory) / "synthetic.dbc").exists())
                meta = _meta("raw-only")
                _post(serve.ready["port"], meta, _chunks(3))
                state = _wait_status(db, lambda s: s["raw_chunks"] == 3)
                self.assertEqual((state["sessions"], state["raw_chunks"]), (1, 3))
            finally:
                serve.stop()

    def test_retry_idempotence_overlaps_commit_nothing_new(self):
        # Same batch posted twice (as an at-least-once redelivery would):
        # the overlap commits nothing new. A dropped-socket ACK proof lives in
        # the existing in-process receiver regression, not duplicated here.
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            serve = _serve(directory, db, _port())
            try:
                meta = _meta("retry")
                batch = _chunks(2)
                _post(serve.ready["port"], meta, batch)
                _post(serve.ready["port"], meta, batch)  # Redelivery.
                state = _wait_status(db, lambda s: s["raw_chunks"] == 2)
                self.assertEqual(state["raw_chunks"], 2)
                _post(serve.ready["port"], meta, _chunks(1, start_seq=2))
                state = _wait_status(db, lambda s: s["raw_chunks"] == 3)
                self.assertEqual(state["sessions"], 1)
            finally:
                serve.stop()

    def test_work_starts_after_serve_discovers_raw(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            serve = _serve(directory, db, _port())
            try:
                _post(serve.ready["port"], _meta("late-work"), _chunks(4))
                _wait_status(db, lambda s: s["raw_chunks"] == 4)
                work = _work(directory, db, "http://127.0.0.1:9")
                try:
                    # Epoch persisted by the work role: visible to a fresh status read.
                    state = _wait_status(db, lambda s: len(s["epochs"]) >= 1, label="epoch")
                    self.assertEqual(len(state["epochs"]), 1)
                    drained = _wait_status(db, lambda s: s["pending_rows"] == 4, label="decode")
                    self.assertEqual(drained["raw_chunks"], 4)
                finally:
                    work.stop()
            finally:
                serve.stop()

    def test_serve_starts_after_work_discovers_raw(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            work = _work(directory, db, "http://127.0.0.1:9")
            try:
                _wait_status(db, lambda s: len(s["epochs"]) >= 1, label="epoch")
                serve = _serve(directory, db, _port())
                try:
                    _post(serve.ready["port"], _meta("early-work"), _chunks(4))
                    drained = _wait_status(db, lambda s: s["pending_rows"] == 4, label="decode")
                    self.assertEqual(drained["raw_chunks"], 4)
                finally:
                    serve.stop()
            finally:
                work.stop()

    def test_simultaneous_fresh_startup_nested_path(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "nested" / "deep" / "raw.sqlite"
            synthetic_decoder(str(directory))
            port = _port()
            serve = _Proc(_serve_argv(db, port), _serve_env())
            work = _Proc(_work_argv(directory, db, "http://127.0.0.1:9"), _work_env())
            try:
                # Both spawned before either ready is awaited: init contends for real.
                serve.wait_ready("receiver")
                work.wait_ready("worker")
                _post(serve.ready["port"], _meta("race"), _chunks(4))
                drained = _wait_status(db, lambda s: s["pending_rows"] == 4, label="decode")
                self.assertEqual((drained["sessions"], drained["raw_chunks"]), (1, 4))
            finally:
                work.stop()
                serve.stop()

    def test_worker_restart_resumes_partial_outbox_exact(self):
        # A dense chunk exceeding DECODE_ROW_BUDGET forces a real decode_partial
        # row. Seed it deterministically via Archive.decode_once (in-process,
        # no timing tricks), then prove real serve+work roles resume the same
        # rows exactly once. Partial state is read via direct SQLite, since
        # the published status contract carries no partial_rows field.
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            serve = _serve(directory, db, _port())
            try:
                meta = _meta("resume")
                _post(serve.ready["port"], meta, _chunks(2))
                _post(serve.ready["port"], meta, [{"seq": 2, "offset_ns": 2000,
                                                   "phase": "capture", "data": FRAME * 2500}])
            finally:
                serve.stop()
            decoder = synthetic_decoder(str(directory))
            sys.path.insert(0, str(ROOT))
            try:
                from scripts.ingest.can.can_receiver import Archive
                archive = Archive(db, 0)
                archive.register_epoch(decoder)
                partial = 0
                while True:
                    progressed = archive.decode_once(decoder)
                    partial = _partial_rows(db)
                    if partial or not progressed:
                        break
                self.assertGreater(partial, 0)
                self.assertGreaterEqual(_outbox_rows(db), 2000)
            finally:
                sys.path.remove(str(ROOT))
            self.assertGreater(_partial_rows(db), 0)
            serve = _serve(directory, db, _port())
            try:
                work = _work(directory, db, "http://127.0.0.1:9")
                try:
                    after = _wait_status(db, lambda s: s["pending_rows"] == 2502
                                         and _partial_rows(db) == 0, label="resume")
                    self.assertEqual(after["raw_chunks"], 3)
                    got = _read_only_rows(db)
                    self.assertEqual(len(got), 2502)
                    self.assertEqual(len({row["event_id"] for row in got}), 2502)
                finally:
                    work.stop()
            finally:
                serve.stop()

    def test_duplicate_roles_and_redecode_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            serve = _serve(directory, db, _port())
            work = _work(directory, db, "http://127.0.0.1:9")
            try:
                dup = subprocess.run(
                    _serve_argv(db, _port()),
                    cwd=str(ROOT), capture_output=True, text=True, timeout=60,
                    env=_serve_env())
                self.assertNotEqual(dup.returncode, 0)
                self.assertIn("archive_already_in_use", dup.stdout)
                dup_work = subprocess.run(
                    _work_argv(directory, db, "http://127.0.0.1:9"),
                    cwd=str(ROOT), capture_output=True, text=True, timeout=60,
                    env=_work_env())
                self.assertNotEqual(dup_work.returncode, 0)
                self.assertIn("archive_already_in_use", dup_work.stdout)
                replay = subprocess.run(
                    [sys.executable, "-m", "scripts.ingest.can.can_receiver", "re-decode",
                     "--database", str(db), "--dbc", str(Path(directory) / "synthetic.dbc"),
                     "--definitions", str(Path(directory) / "synthetic.json"),
                     "--disk-reserve-mib", "0"],
                    cwd=str(ROOT), capture_output=True, text=True, timeout=120,
                    env=_work_env())
                self.assertNotEqual(replay.returncode, 0)
                self.assertIn("archive_already_in_use", replay.stdout)
            finally:
                work.stop()
                serve.stop()

    def test_legacy_ex_owner_blocks_both_roles(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            synthetic_decoder(str(directory))
            db.touch()
            lock = str(db) + ".lock"
            fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                serve = subprocess.run(
                    _serve_argv(db, _port()),
                    cwd=str(ROOT), capture_output=True, text=True, timeout=60,
                    env=_serve_env())
                self.assertNotEqual(serve.returncode, 0)
                self.assertIn("archive_already_in_use", serve.stdout)
                work = subprocess.run(
                    _work_argv(directory, db, "http://127.0.0.1:9"),
                    cwd=str(ROOT), capture_output=True, text=True, timeout=60,
                    env=_work_env())
                self.assertNotEqual(work.returncode, 0)
                self.assertIn("archive_already_in_use", work.stdout)
            finally:
                os.close(fd)

    def test_restore_guard_blocked_by_either_role_alone(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            lock = str(db) + ".lock"
            self.assertFalse(_lock_held(lock))
            serve = _serve(directory, db, _port())
            try:
                self.assertTrue(_lock_held(lock))
            finally:
                serve.stop()
            self.assertFalse(_lock_held(lock))
            work = _work(directory, db, "http://127.0.0.1:9")
            try:
                self.assertTrue(_lock_held(lock))
            finally:
                work.stop()
            self.assertFalse(_lock_held(lock))

    def test_split_matches_sequential_canonical_equivalence(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            decoder = synthetic_decoder(str(directory))
            meta = _meta("equiv")
            chunks_list = _chunks(8)
            expected, state = [], None
            for chunk in chunks_list:
                rows, state, _ = decoder.decode(meta, chunk, state)
                expected.extend(rows)
            self.assertGreater(len(expected), 0)

            def canonical(row):
                return {key: value for key, value in row.items() if key != "ingest_time"}

            want = sorted(json.dumps(canonical(row), sort_keys=True) for row in expected)
            serve = _serve(directory, db, _port())
            try:
                _post(serve.ready["port"], meta, chunks_list)
                work = _work(directory, db, "http://127.0.0.1:9")
                try:
                    drained = _wait_status(db, lambda s: s["pending_rows"] == len(expected),
                                           label="equivalence decode")
                    self.assertEqual(drained["counters"]["decoded_rows_total"], len(expected))
                    got = sorted(json.dumps(canonical(row), sort_keys=True)
                                 for row in _read_only_rows(db))
                    self.assertEqual(got, want)
                finally:
                    work.stop()
            finally:
                serve.stop()

    def test_blocking_accept_completes_before_sigterm_releases_locks(self):
        # Expect: 100-continue proves a live handler slot admitted this request
        # (interim 100 comes from the handler itself) before SIGTERM; the
        # trickled remainder then proves the in-flight ACK commits during the
        # drain, and role locks release only after.
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            lock = str(db) + ".lock"
            proc = _Proc(_serve_argv(db, _port()), _serve_env())
            try:
                proc.wait_ready("receiver")
                port = proc.ready["port"]
                meta = _meta("drain")
                payload = gzip.compress(encode_batch(meta, _chunks(2)))
                held = socket.socket()
                held.connect(("127.0.0.1", port))
                try:
                    auth = base64.b64encode(b"proc-user:proc-pass-1234").decode()
                    held.sendall(("POST /v1/logs HTTP/1.1\r\nHost: x\r\n"
                                  "Authorization: Basic %s\r\n"
                                  "Content-Type: application/x-protobuf\r\n"
                                  "Content-Encoding: gzip\r\n"
                                  "Expect: 100-continue\r\n"
                                  "Content-Length: %d\r\n\r\n" % (auth, len(payload))).encode())
                    held.settimeout(10)
                    interim = b""
                    while b"100" not in interim:
                        part = held.recv(4096)
                        if not part:
                            break
                        interim += part
                    # Handler admitted the request: interim 100-continue observed.
                    self.assertIn(b"100", interim)
                    self.assertTrue(_lock_held(lock))
                    held.sendall(payload[:-1])
                    proc.proc.send_signal(signal.SIGTERM)
                    held.sendall(payload[-1:])
                    held.settimeout(10)
                    reply = b""
                    while b"200" not in reply:
                        part = held.recv(4096)
                        if not part:
                            break
                        reply += part
                    self.assertIn(b"200", reply)
                finally:
                    held.close()
                proc.proc.wait(timeout=20)
                self.assertEqual(_status(db)["raw_chunks"], 2)
                self.assertFalse(_lock_held(lock))
            finally:
                proc.stop()

    def test_keepalive_during_term_exits_bounded_and_releases_roles(self):
        # One persistent keepalive connection issuing repeated /status during
        # SIGTERM must not pin the receiver: it exits bounded, the accepted
        # batch stays durable, and a new role can acquire afterwards. A fresh
        # connection per request would not exercise the persistent-slot bug.
        import http.client
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "raw.sqlite"
            lock = str(db) + ".lock"
            serve = _serve(directory, db, _port())
            try:
                auth = "Basic " + base64.b64encode(b"proc-user:proc-pass-1234").decode()
                stop = threading.Event()
                served = []

                def hammer():
                    conn = http.client.HTTPConnection(
                        "127.0.0.1", serve.ready["port"], timeout=5)
                    try:
                        while not stop.is_set():
                            try:
                                conn.request("GET", "/status",
                                             headers={"Authorization": auth})
                                response = conn.getresponse()
                                self.assertEqual(response.status, 200)
                                response.read()
                                served.append(1)
                            except (OSError, http.client.HTTPException):
                                break
                    finally:
                        try:
                            conn.close()
                        except (OSError, http.client.HTTPException):
                            pass

                hammer_thread = threading.Thread(target=hammer, daemon=True)
                hammer_thread.start()
                try:
                    deadline = time.monotonic() + 10
                    while not served and time.monotonic() < deadline:
                        time.sleep(0.05)
                    # Persistent connection served at least once before TERM.
                    self.assertTrue(served)
                    _post(serve.ready["port"], _meta("term-keepalive"), _chunks(2))
                    serve.proc.send_signal(signal.SIGTERM)
                    serve.proc.wait(timeout=30)
                finally:
                    stop.set()
                    hammer_thread.join(timeout=10)
                self.assertEqual(_status(db)["raw_chunks"], 2)
                self.assertFalse(_lock_held(lock))
                # A new role acquires immediately after the bounded exit.
                successor = _serve(directory, db, _port())
                try:
                    _post(successor.ready["port"], _meta("term-keepalive"), _chunks(1, start_seq=2))
                    self.assertEqual(_status(db)["raw_chunks"], 3)
                finally:
                    successor.stop()
            finally:
                serve.stop()


if __name__ == "__main__":
    unittest.main()
