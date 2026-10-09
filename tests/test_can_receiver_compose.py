"""Exercise the packaged read-only receiver against isolated GreptimeDB v1.2.1.

Synthetic DBC/OTLP only. Brings up a minimal File-storage Greptime plus the
source-generated packaged receiver under a unique compose project, then proves
offline outbox durability, substantial multi-session upload with a dense chunk,
exact Greptime rows/dirty ACK, restart/replay dedup, and outage retention plus
drain. Cleans up all test resources in a finally block.

Run: python -m tests.test_can_receiver_compose
"""
import base64
import gzip
import http.client
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from scripts.database.db_init import ddl_statements
from scripts.ingest.can.can_otlp_wire import check_response
from tests.test_can_receiver import encode_batch, synthetic_decoder

ROOT = Path(__file__).resolve().parents[1]
GREPTIME_IMAGE = "greptime/greptimedb:v1.2.1"
RECEIVER_IMAGE = "python:3.12.8-slim-bookworm"
REQUIRED_SOURCES = (
    "scripts/ingest/can/can_receiver.py",
    "scripts/ingest/can/can_decoder.py",
    "scripts/ingest/can/can_otlp_wire.py",
    "scripts/database/db_init.py",
    "scripts/database/greptime_preflight.py",
    "compose/can-receiver.yaml",
)

START_A = 1800000000000000000
START_B = 1800000001000000000
START_C = 1800000002000000000
TINY_A = 6000
TINY_B = 4000
DENSE_ROWS = 2500
BATCH = 500
OUTAGE_CHUNKS = 200


def _tiny(session_chunks, count, step_ns=1000):
    base = len(session_chunks)
    for seq in range(base, base + count):
        session_chunks.append({"seq": seq, "offset_ns": seq * step_ns,
                               "phase": "capture", "data": b"t12320200\r"})


def _post(endpoint, user, password, meta, chunks):
    payload = encode_batch(meta, chunks)
    request = urllib.request.Request(
        endpoint, data=gzip.compress(payload),
        headers={"Authorization": "Basic " + base64.b64encode(
            (user + ":" + password).encode()).decode(),
                 "Content-Type": "application/x-protobuf",
                 "Content-Encoding": "gzip"},
        method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == 200
        check_response(response.read())


def _sql(base_url, auth, db, stmt, timeout=30):
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db, safe="")
    body = urllib.parse.urlencode({"sql": stmt}).encode()
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    request.add_header("Authorization", "Basic " + auth)
    request.add_header("X-Greptime-Timezone", "UTC")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise AssertionError("greptime sql http status " + str(error.code))
    if (not isinstance(payload, dict) or payload.get("code", 0) != 0
            or not isinstance(payload.get("output"), list) or not payload["output"]):
        raise AssertionError("greptime sql failed")
    return payload


def _rows(payload):
    records = payload["output"][0].get("records")
    assert isinstance(records, dict) and isinstance(records.get("rows"), list)
    columns = [c["name"] for c in records["schema"]["column_schemas"]]
    return columns, records["rows"]


def _count(base_url, auth, db, table, where=""):
    payload = _sql(base_url, auth, db,
                   "SELECT COUNT(*) FROM " + table + (" WHERE " + where if where else ""))
    return int(_rows(payload)[1][0][0])


def main():
    project = "can-greptime-" + secrets.token_hex(6)
    otlp_user, otlp_password = "fixture", secrets.token_hex(16)
    db_user, db_password = "smokeuser", secrets.token_hex(16)
    db_name = "smoke"
    auth = base64.b64encode((db_user + ":" + db_password).encode()).decode()
    environment = {k: v for k, v in os.environ.items()
                   if k in {"PATH", "HOME", "TMPDIR", "LANG"} or k.startswith("DOCKER_")}
    with tempfile.TemporaryDirectory(prefix=project) as directory:
        path = Path(directory)
        path.chmod(0o755)
        decoder = synthetic_decoder(path)
        epoch = decoder.epoch
        etc_dir = path / "greptime-etc"
        etc_dir.mkdir(mode=0o755)
        preflight_env = dict(environment, GREPTIME_DB=db_name, GREPTIME_USER=db_user,
                             GREPTIME_PASSWORD=db_password, GF_ADMIN_PASSWORD=secrets.token_hex(16),
                             OTLP_USER="smokeotlp", OTLP_PASSWORD=secrets.token_hex(16),
                             GREPTIME_STORAGE_TYPE="File", OUT_DIR=str(etc_dir))
        subprocess.check_output([sys.executable, "-m", "scripts.database.greptime_preflight"],
                                cwd=str(ROOT), env=preflight_env, text=True)
        override = path / "override.yaml"

        def write_override(greptime_url):
            receiver = {
                "environment": {"CAN_OTLP_USER": otlp_user, "CAN_OTLP_PASSWORD": otlp_password,
                                "GREPTIME_USER": db_user, "GREPTIME_PASSWORD": db_password},
                "command": ["/opt/venv/bin/python", "-m", "scripts.ingest.can.can_receiver", "serve",
                            "--database", "/data/raw.sqlite3", "--dbc", "/definitions/synthetic.dbc",
                            "--definitions", "/definitions/synthetic.json", "--vehicle", "fixture",
                            "--collector-id", "fixture", "--greptime-url", greptime_url,
                            "--greptime-db", db_name, "--bind", "0.0.0.0", "--port", "4319",
                            "--worker-interval", "0.2"],
                "volumes": [str(path) + ":/definitions:ro"],
            }
            override.write_text(
                "services:\n  can-receiver:\n" + "".join(
                    "    " + key + ": " + json.dumps(value) + "\n" for key, value in receiver.items())
                + "    ports: !override ['127.0.0.1::4319']\n"
                + "  smoke-greptime:\n"
                + "    image: " + json.dumps(GREPTIME_IMAGE) + "\n"
                + '    profiles: ["can-receiver"]\n'
                + '    command: ["standalone", "start", "--config-file", "/etc/greptime/greptimedb.toml"]\n'
                + "    volumes: " + json.dumps(["smoke-greptime-data:/data",
                                                str(etc_dir) + ":/etc/greptime:ro"]) + "\n"
                + "    ports: !override ['127.0.0.1::4000']\n"
                + "volumes:\n  can-receiver-raw: !override {}\n"
                + "  can-receiver-definitions: !override {}\n"
                + "  smoke-greptime-data: {}\n")

        write_override("http://127.0.0.1:9")
        command = ["docker", "compose", "-p", project, "--env-file", str(ROOT / ".env.example"),
                   "-f", str(ROOT / "compose.yaml"), "-f", str(override), "--profile", "can-receiver"]

        def compose(*args):
            return subprocess.check_output(command + list(args), env=environment, text=True)

        def status():
            return json.loads(compose("exec", "-T", "can-receiver", "/opt/venv/bin/python",
                                      "-m", "scripts.ingest.can.can_receiver", "status",
                                      "--database", "/data/raw.sqlite3"))

        def wait_for(probe, seconds, label):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                try:
                    if probe():
                        return
                except (OSError, subprocess.CalledProcessError, AssertionError):
                    pass
                time.sleep(0.5)
            raise AssertionError("can smoke timed out: " + label)

        chunks_a, chunks_b = [], []
        _tiny(chunks_a, TINY_A)
        _tiny(chunks_b, TINY_B)
        chunks_a.append({"seq": TINY_A, "offset_ns": TINY_A * 1000, "phase": "capture",
                         "data": b"t12320200\r" * DENSE_ROWS})
        total_chunks = len(chunks_a) + len(chunks_b)
        meta_a = {"schema_version": 1, "vehicle": "fixture", "collector_id": "fixture",
                  "session_id": "fixture-a", "started_ns": START_A, "vehicle_firmware": "synthetic"}
        meta_b = {"schema_version": 1, "vehicle": "fixture", "collector_id": "fixture",
                  "session_id": "fixture-b", "started_ns": START_B, "vehicle_firmware": "synthetic"}

        def upload_all(endpoint):
            pending_a = [chunks_a[i:i + BATCH] for i in range(0, len(chunks_a) - 1, BATCH)]
            pending_b = [chunks_b[i:i + BATCH] for i in range(0, len(chunks_b), BATCH)]
            for index in range(max(len(pending_a), len(pending_b))):
                if index < len(pending_a):
                    _post(endpoint, otlp_user, otlp_password, meta_a, pending_a[index])
                if index < len(pending_b):
                    _post(endpoint, otlp_user, otlp_password, meta_b, pending_b[index])
            _post(endpoint, otlp_user, otlp_password, meta_a, [chunks_a[-1]])

        def expect_rows(metas_chunks):
            expected = {}
            for meta, chunks in metas_chunks:
                state = None
                for chunk in chunks:
                    rows, state, _ = decoder.decode(meta, chunk, state)
                    for row in rows:
                        assert row["decode_epoch"] == epoch and row["value_num"] == 1.0
                        expected[row["event_id"]] = (row["event_time"], row["value_num"],
                                                     row["path"], row["vehicle"], row["source"])
            return expected

        try:
            compose("up", "-d", "smoke-greptime")
            greptime_host = "http://" + compose("port", "smoke-greptime", "4000").strip()
            wait_for(lambda: _sql(greptime_host, auth, "public", "SELECT 1"), 180, "greptime ready")
            _sql(greptime_host, auth, "public", 'CREATE DATABASE IF NOT EXISTS "' + db_name + '"')
            ddls = dict(ddl_statements(""))
            for label in ("vehicle_signal", "vehicle_signal_dirty"):
                _sql(greptime_host, auth, db_name, ddls[label], timeout=60)
                _sql(greptime_host, auth, db_name, "SELECT 1 FROM " + label + " LIMIT 1")

            compose("up", "-d", "can-receiver-deps")
            deps = compose("ps", "-aq", "can-receiver-deps").strip()
            assert subprocess.check_output(["docker", "wait", deps], text=True).strip() == "0"
            compose("run", "--rm", "--no-deps", "--user", "0:0", "--cap-add", "CHOWN",
                    "--entrypoint", "/bin/sh", "can-receiver", "-ec",
                    "chmod 0700 /data; chown 10001:10001 /data")
            compose("up", "-d", "can-receiver")
            cid = compose("ps", "-q", "can-receiver").strip()
            container = json.loads(subprocess.check_output(["docker", "inspect", cid], text=True))[0]
            assert container["HostConfig"]["ReadonlyRootfs"] and container["Config"]["User"] == "10001:10001"
            assert not next(m for m in container["Mounts"] if m["Destination"] == "/app")["RW"]
            endpoint = "http://" + compose("port", "can-receiver", "4319").strip() + "/v1/logs"

            def unauthorized():
                try:
                    urllib.request.urlopen(urllib.request.Request(endpoint, data=b""), timeout=5)
                except urllib.error.HTTPError as error:
                    assert error.code == 401
                    return True
                return False

            wait_for(unauthorized, 60, "receiver auth")
            # Offline durability: backend unreachable, raw ACKs and decodes locally only.
            # HTTP per-request deadline: the packaged socket stays reusable
            # beyond one whole-request timeout worth of fast /status calls.
            status_port = compose("port", "can-receiver", "4319").strip().rsplit(":", 1)[1]
            status_auth = "Basic " + base64.b64encode((otlp_user + ":" + otlp_password).encode()).decode()
            keep = http.client.HTTPConnection("127.0.0.1", int(status_port), timeout=30)
            first_sock = None
            for _ in range(8):
                keep.request("GET", "/status", headers={"Authorization": status_auth})
                kept = keep.getresponse()
                assert kept.status == 200
                kept.read()
                if first_sock is None:
                    first_sock = keep.sock
                else:
                    assert keep.sock is first_sock  # Zero reconnects.
                time.sleep(2)  # ~16s connection life exceeds the default 15s whole-request bound.
            keep.close()

            upload_all(endpoint)
            expected = expect_rows([(meta_a, chunks_a), (meta_b, chunks_b)])
            assert len(expected) == TINY_A + TINY_B + DENSE_ROWS
            wait_for(lambda: status()["pending_rows"] == len(expected)
                     and status()["raw_chunks"] == total_chunks, 300, "offline decode")
            saved = status()
            assert saved["sessions"] == 2 and saved["epochs"][0]["decoded_rows"] == len(expected)
            # Restart with the backend still down: durable outbox survives, replay dedups.
            compose("restart", "can-receiver")
            endpoint = "http://" + compose("port", "can-receiver", "4319").strip() + "/v1/logs"
            wait_for(unauthorized, 60, "receiver auth after restart")
            upload_all(endpoint)
            saved = status()
            assert (saved["raw_chunks"], saved["sessions"], saved["pending_rows"]) == \
                (total_chunks, 2, len(expected))

            # Point at the real isolated backend (test service only) and drain.
            write_override("http://smoke-greptime:4000")
            compose("up", "-d", "can-receiver")
            endpoint = "http://" + compose("port", "can-receiver", "4319").strip() + "/v1/logs"
            wait_for(unauthorized, 60, "receiver auth after backend switch")
            wait_for(lambda: status()["pending_rows"] == 0, 300, "drain to greptime")
            assert _count(greptime_host, auth, db_name, "vehicle_signal") == len(expected)
            assert _count(greptime_host, auth, db_name, "vehicle_signal_dirty") >= 1
            fetched, offset = {}, 0
            while True:
                columns, batch_rows = _rows(_sql(
                    greptime_host, auth, db_name,
                    "SELECT event_id,value_num FROM vehicle_signal ORDER BY event_id"
                    " LIMIT 5000 OFFSET " + str(offset), timeout=60))
                if not batch_rows:
                    break
                for event_id, value_num in batch_rows:
                    fetched[event_id] = value_num
                offset += len(batch_rows)
            assert set(fetched) == set(expected) and all(
                fetched[k] == expected[k][1] for k in expected)
            for event_id in list(expected)[:5]:
                event_ns, value_num, epath, ev, esrc = expected[event_id]
                assert _count(greptime_host, auth, db_name, "vehicle_signal",
                              "event_id='" + event_id + "' AND event_time=" + str(event_ns)
                              + " AND value_num=" + repr(value_num)) == 1
            columns, sample = _rows(_sql(greptime_host, auth, db_name,
                                         "SELECT path,vehicle,source,decode_epoch FROM vehicle_signal"
                                         " WHERE event_id='" + next(iter(expected)) + "'"))
            assert tuple(sample[0]) == (expected[next(iter(expected))][2],
                                        expected[next(iter(expected))][3],
                                        expected[next(iter(expected))][4], epoch)
            columns, dirty = _rows(_sql(greptime_host, auth, db_name,
                                        "SELECT vehicle,source,decode_epoch,generation"
                                        " FROM vehicle_signal_dirty LIMIT 100"))
            assert dirty and all(r[0] == "fixture" and r[1] == "can" and r[2] == epoch
                                 and len(r[3]) == 64 for r in dirty)

            # Restart with the backend up, then replay: no duplicates.
            compose("restart", "can-receiver")
            endpoint = "http://" + compose("port", "can-receiver", "4319").strip() + "/v1/logs"
            wait_for(unauthorized, 60, "receiver auth after online restart")
            upload_all(endpoint)
            time.sleep(10)
            assert status()["pending_rows"] == 0
            assert _count(greptime_host, auth, db_name, "vehicle_signal") == len(expected)

            # Outage retention: new raw ACKs while the backend is down, drain after.
            compose("stop", "smoke-greptime")
            chunks_c = []
            _tiny(chunks_c, OUTAGE_CHUNKS)
            meta_c = {"schema_version": 1, "vehicle": "fixture", "collector_id": "fixture",
                      "session_id": "fixture-c", "started_ns": START_C,
                      "vehicle_firmware": "synthetic"}
            for i in range(0, len(chunks_c), BATCH):
                _post(endpoint, otlp_user, otlp_password, meta_c, chunks_c[i:i + BATCH])
            wait_for(lambda: status()["pending_rows"] == OUTAGE_CHUNKS, 120, "outage retention")
            try:
                _sql(greptime_host, auth, db_name, "SELECT 1", timeout=5)
            except (urllib.error.URLError, TimeoutError):
                pass
            else:
                raise AssertionError("greptime unexpectedly reachable during outage")
            compose("start", "smoke-greptime")
            greptime_host = "http://" + compose("port", "smoke-greptime", "4000").strip()
            wait_for(lambda: _sql(greptime_host, auth, "public", "SELECT 1"), 180, "greptime back")
            expected.update(expect_rows([(meta_c, chunks_c)]))
            wait_for(lambda: status()["pending_rows"] == 0, 300, "post-outage drain")
            assert _count(greptime_host, auth, db_name, "vehicle_signal") == len(expected)

            print("read-only receiver: offline durability, %d chunks/%d rows, dense resume,"
                  " real greptime rows/dirty ACK, restart dedup, outage retention+drain passed"
                  % (total_chunks + OUTAGE_CHUNKS, len(expected)))
            print("sources: " + ",".join(REQUIRED_SOURCES))
            print("images: " + GREPTIME_IMAGE + "," + RECEIVER_IMAGE)
            print("command: python -m tests.test_can_receiver_compose")
        finally:
            compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
