#!/usr/bin/env python3
"""Persistent guard regression; --docker exercises core.yaml's native gate."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

import yaml
from scripts.storage import egress_guard as g

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/storage/egress_guard.py"


def main():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        unsafe = path / "unsafe"
        unsafe.mkdir(mode=0o755)
        try:
            with g.locked(unsafe):
                raise AssertionError("shared state directory accepted")
        except PermissionError:
            assert not (unsafe / "lock").exists()
            assert unsafe.stat().st_mode & 0o777 == 0o755
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as listener:
            address = str(path / "notify")
            listener.bind(address)
            listener.settimeout(1)
            with patch.dict(os.environ, NOTIFY_SOCKET=address):
                g.notify()
            assert listener.recv(128) == b"READY=1\nWATCHDOG=1"
        state = dict(version=1, project="guard-regression", budget_bytes=100,
                     used_bytes=0, last=None, blocked=False)
        first = {"Id": "one", "State": {"StartedAt": "first"}}
        g.bind_instance(state, first)
        assert not g.observe(state, {"s3": 40})
        g.save(path, state)
        state = g.load(path)
        assert not g.observe(state, {"s3": 60})
        assert state["used_bytes"] == 60
        try:
            # A new process with a higher counter is still a discontinuity.
            g.bind_instance(state, {"Id": "one", "State": {"StartedAt": "second"}})
            g.observe(state, {"s3": 90})
            raise AssertionError("higher-counter rollover accepted")
        except ValueError:
            assert state["used_bytes"] == 60
        try:
            g.observe(state, {"s3": 1})
            raise AssertionError("reset accepted")
        except ValueError:
            g.latch(path, state, "reset")
        assert g.load(path)["blocked"] and (path / "lease").read_text() == "0\n"
        result = subprocess.run([sys.executable, str(SCRIPT), "--state-dir", tmp, "arm",
                                 "--project", "guard-regression", "--budget-bytes", "200"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert result.returncode != 0
        assert g.load(path)["used_bytes"] == 60 and g.load(path)["blocked"]
        subprocess.run([sys.executable, str(SCRIPT), "--state-dir", tmp, "unblock",
                        "--budget-bytes", "200", "--startup-seconds", "1"],
                       stdout=subprocess.DEVNULL, check=True)
        assert g.load(path)["used_bytes"] == 60 and g.load(path)["instance"] is None
        if "--docker" in sys.argv:
            project = "egress-guard-smoke-" + str(int(time.time()))
            state = g.load(path)
            state.update(project=project, last={"s3": 0}, sample_at=time.time(), startup_until=0)
            g.save(path, state)
            name = project + "-db"
            core = yaml.safe_load((ROOT / "compose/core.yaml").read_text())["services"]["greptimedb"]
            assert "GREPTIME_EGRESS_GUARD_ENABLED" in core["environment"]
            entrypoint = [v.replace("$$", "$") for v in core["entrypoint"]]
            image = os.environ.get("GREPTIME_IMAGE", "greptime/greptimedb:v1.2.1")
            def launch(mode, suffix, *command):
                return subprocess.run(["docker", "run", "-d", "--name", name + suffix,
                                       "--label", "com.docker.compose.project=" + project,
                                       "--label", "com.docker.compose.service=greptimedb",
                                       "-e", "GREPTIME_EGRESS_GUARD_ENABLED=" + mode,
                                       "-p", "127.0.0.1::4000", "--entrypoint", entrypoint[0],
                                       "-v", tmp + ":/egress-guard:ro", image,
                                       *entrypoint[1:], *command], check=True,
                                      stdout=subprocess.DEVNULL)
            g.atomic(path / "lease", str(int(time.time() + 30)) + "\n")
            worker = None
            try:
                launch("typo", "-invalid", "--version")
                assert g.docker("wait", name + "-invalid") == "78"
                launch("true", "", "standalone", "start")
                container = json.loads(g.docker("inspect", name))[0]
                port = container["NetworkSettings"]["Ports"]["4000/tcp"][0]["HostPort"]
                url = "http://127.0.0.1:" + port + "/metrics"
                assert g.database([container], url)["Id"] == container["Id"]
                try:
                    g.database([container], "http://127.0.0.1:1/metrics")
                    raise AssertionError("unrelated metrics endpoint accepted")
                except ValueError:
                    pass
                g.bind_instance(state, container)
                g.save(path, state)
                # Fresh File-backed DB has no S3 metric: absence must latch,
                # not accept a plausible zero counter.
                worker = subprocess.Popen([sys.executable, str(SCRIPT), "--state-dir", tmp,
                                           "run", "--metrics-url", url,
                                           "--interval", "0.1", "--timeout", "1", "--grace", "1"])
                deadline = time.time() + 20
                while time.time() < deadline:
                    if g.load(path)["blocked"] and not json.loads(g.docker("inspect", name))[0]["State"]["Running"]:
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError("metrics failure did not stop project")
                assert g.load(path)["used_bytes"] == 60
                g.docker("start", name)
                assert g.docker("wait", name) == "78"
                assert g.load(path)["blocked"]
                worker.terminate()
                worker.wait(timeout=5)
                worker = None
                g.atomic(path / "lease", str(int(time.time() + 30)) + "\n")
                g.docker("start", name)
                (path / "state.json").write_text("corrupt-state\n")
                trip = subprocess.run([sys.executable, str(SCRIPT), "--state-dir", tmp,
                                       "trip", "--project", project, "--grace", "1"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                assert trip.returncode != 0  # reports need for manual state repair
                assert not json.loads(g.docker("inspect", name))[0]["State"]["Running"]
                assert (path / "lease").read_text() == "0\n"
                assert (path / "state.json").read_text() == "corrupt-state\n"
            finally:
                if worker is not None:
                    worker.terminate()
                    worker.wait(timeout=5)
                g.docker("rm", "-f", name, name + "-invalid")
    print("egress guard: persistence/reset/higher-counter rollover/metrics failure/core gate passed")


if __name__ == "__main__":
    main()
