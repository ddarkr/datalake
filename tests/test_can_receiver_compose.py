"""Exercise the packaged read-only receiver with synthetic CAN and an offline DB."""
import base64
import gzip
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from tests.test_can_receiver import encode_batch, synthetic_decoder

ROOT = Path(__file__).resolve().parents[1]


def main():
    project = "can-startup-" + secrets.token_hex(6)
    environment = {k: v for k, v in os.environ.items()
                   if k in {"PATH", "HOME", "TMPDIR", "LANG"} or k.startswith("DOCKER_")}
    with tempfile.TemporaryDirectory(prefix=project) as directory:
        path = Path(directory)
        path.chmod(0o755)
        synthetic_decoder(path)
        override = path / "override.yaml"
        service = {
            "environment": {"CAN_OTLP_USER": "fixture", "CAN_OTLP_PASSWORD": "fixture",
                            "GREPTIME_USER": "fixture", "GREPTIME_PASSWORD": "fixture"},
            "command": ["/opt/venv/bin/python", "-m", "scripts.ingest.can.can_receiver", "serve",
                        "--database", "/data/raw.sqlite3", "--dbc", "/definitions/synthetic.dbc",
                        "--definitions", "/definitions/synthetic.json", "--vehicle", "fixture",
                        "--collector-id", "fixture", "--greptime-url", "http://127.0.0.1:9",
                        "--greptime-db", "fixture", "--bind", "0.0.0.0", "--port", "4319"],
            "volumes": [str(path) + ":/definitions:ro"],
        }
        override.write_text("services:\n  can-receiver:\n" + "".join(
            "    " + key + ": " + json.dumps(value) + "\n" for key, value in service.items())
            + "    ports: !override ['127.0.0.1::4319']\n"
            + "volumes:\n  can-receiver-raw: !override {}\n"
            + "  can-receiver-definitions: !override {}\n")
        command = ["docker", "compose", "-p", project, "--env-file", str(ROOT / ".env.example"),
                   "-f", str(ROOT / "compose.yaml"), "-f", str(override), "--profile", "can-receiver"]

        def compose(*args):
            return subprocess.check_output(command + list(args), env=environment, text=True)

        def status():
            return json.loads(compose("exec", "-T", "can-receiver", "/opt/venv/bin/python",
                                      "-m", "scripts.ingest.can.can_receiver", "status", "--database", "/data/raw.sqlite3"))

        def wait_for(probe):
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    if probe():
                        return
                except (OSError, subprocess.CalledProcessError):
                    pass
                time.sleep(0.2)
            raise AssertionError("packaged receiver did not reach expected state")

        try:
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
                    urllib.request.urlopen(urllib.request.Request(endpoint, data=b""), timeout=2)
                except urllib.error.HTTPError as error:
                    assert error.code == 401
                    return True
                return False

            wait_for(unauthorized)
            payload = encode_batch(
                {"schema_version": 1, "vehicle": "fixture", "collector_id": "fixture",
                 "session_id": "fixture-session", "started_ns": 1800000000000000000,
                 "vehicle_firmware": "synthetic"},
                [{"seq": 0, "offset_ns": 0, "phase": "capture", "data": b"t12320200\r"}])
            request = urllib.request.Request(endpoint, data=gzip.compress(payload), headers={
                "Authorization": "Basic " + base64.b64encode(b"fixture:fixture").decode(),
                "Content-Type": "application/x-protobuf", "Content-Encoding": "gzip"})
            with urllib.request.urlopen(request, timeout=5) as response:
                assert response.status == 200
            wait_for(lambda: status()["pending_rows"] == 1)
            compose("restart", "can-receiver")
            endpoint = "http://" + compose("port", "can-receiver", "4319").strip() + "/v1/logs"
            request.full_url = endpoint
            wait_for(unauthorized)
            with urllib.request.urlopen(request, timeout=5) as response:
                assert response.status == 200
            saved = status()
            assert saved["raw_chunks"] == 1 and saved["sessions"] == 1 and saved["pending_rows"] == 1
            assert saved["epochs"][0]["processed_chunks"] == 1 and saved["epochs"][0]["decoded_rows"] == 1
            print("read-only receiver: authenticated durable ACK, restart and replay dedup passed")
        finally:
            compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
