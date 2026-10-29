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


def daily_limits(path):
    midnight = g.datetime(2026, 10, 18, tzinfo=g.KST).timestamp()
    state = dict(version=2, project="daily-regression", used_bytes=0, last=None,
                 blocked=False, day=g.day_at(midnight - 1), day_used_bytes=0, alerted=[])
    with patch.object(g.time, "time", return_value=midnight - 1):
        assert not g.observe(state, {"s3": g.SOFT_BYTES - 1})
        g.save(path, state)
        with patch.object(g, "telegram") as sent:
            g.alert(path, path / "telegram.json", False)
            assert sent.call_count == 0
            assert not g.observe(state, {"s3": g.SOFT_BYTES})
            g.save(path, state)
            g.alert(path, path / "telegram.json", False)
            g.alert(path, path / "telegram.json", False)
            assert sent.call_count == 1
            assert not g.load(path)["blocked"]
            state = g.load(path)
            assert g.observe(state, {"s3": g.HARD_BYTES})
            g.latch(path, state, "daily hard download limit reached")
            g.alert(path, path / "telegram.json", True)
            assert sent.call_count == 2  # escalation bypasses soft retry delay
            assert "정지 확인" in sent.call_args.args[1]
            g.alert(path, path / "telegram.json", True)
            assert sent.call_count == 2
        state = g.load(path)
        try:
            g.main(["--state-dir", str(path), "unblock"])
            raise AssertionError("same-day hard limit bypassed")
        except SystemExit as exc:
            assert exc.code == 2
    with patch.object(g.time, "time", return_value=midnight):
        # Only explicit unblock advances a stopped guard into the new day.
        assert state["blocked"] and state["day"] == "2026-10-17"
        g.main(["--state-dir", str(path), "unblock"])
        state = g.load(path)
        assert not state["blocked"] and state["day_used_bytes"] == 0
        assert state["used_bytes"] == g.HARD_BYTES
        assert state["alerted"] == []
        assert (path / "lease").read_text() == "0\n"
    state.update(last={"s3": g.HARD_BYTES}, day="2026-10-17",
                 day_used_bytes=g.HARD_BYTES)
    with patch.object(g.time, "time", return_value=midnight):
        assert not g.observe(state, {"s3": g.HARD_BYTES + 123})
        assert state["day_used_bytes"] == 123 and state["used_bytes"] == g.HARD_BYTES + 123
        state["day_used_bytes"] = g.SOFT_BYTES
        g.save(path, state)
        with patch.object(g, "telegram", side_effect=OSError("delivery failed")) as failed:
            g.alert(path, path / "telegram.json", False)
            assert g.load(path)["alerted"] == []
            g.alert(path, path / "telegram.json", False)
            assert failed.call_count == 1
    with patch.object(g.time, "time", return_value=midnight + 60):
        with patch.object(g, "telegram") as sent:
            g.alert(path, path / "telegram.json", False)
            assert sent.call_count == 1
            assert g.load(path)["alerted"] == ["soft"]
    try:
        with patch.object(g.time, "time", return_value=midnight - 1):
            g.observe(state, {"s3": g.HARD_BYTES + 124})
        raise AssertionError("backward calendar accepted")
    except ValueError:
        pass


def monthly_limits(path):
    start = g.datetime(2026, 10, 1, 12, tzinfo=g.KST).timestamp()
    state = dict(version=2, project="monthly-regression", used_bytes=0,
                 last=None, blocked=False, day=g.day_at(start),
                 day_used_bytes=0, alerted=[])
    # Individually safe days must still exhaust the month.
    with patch.object(g.time, "time", return_value=start):
        assert not g.observe(state, {"s3": 199_000_000_000})
    for day in range(1, 12):
        with patch.object(g.time, "time", return_value=start + day * 86400):
            assert not g.observe(state, {"s3": (day + 1) * 199_000_000_000})
    with patch.object(g.time, "time", return_value=start + 12 * 86400):
        assert g.observe(state, {"s3": 13 * 199_000_000_000})
        assert state["day_used_bytes"] < g.HARD_BYTES
        g.latch(path, state, "monthly hard download limit reached")
        with patch.object(g, "telegram") as sent:
            g.alert(path, path / "telegram.json", True)
            assert "월간 다운로드 한도 초과" in sent.call_args.args[1]
        try:
            g.main(["--state-dir", str(path), "unblock"])
            raise AssertionError("monthly limit bypassed")
        except SystemExit as exc:
            assert exc.code == 2
    with patch.object(g.time, "time", return_value=g.datetime(2026, 11, 1, tzinfo=g.KST).timestamp()):
        g.main(["--state-dir", str(path), "unblock"])
        restored = g.load(path)
        assert restored["month"] == "2026-11"
        assert restored["month_used_bytes"] == restored["day_used_bytes"] == 0
        assert restored["used_bytes"] == 13 * 199_000_000_000
        # The first new sample is charged conservatively after a rebind.
        assert not g.observe(restored, {"s3": 123})
        assert restored["month_used_bytes"] == 123
    # Legacy adoption charges known lifetime usage rather than claiming zero.
    legacy = dict(version=2, project="legacy-month", used_bytes=1234,
                  last={"s3": 1234}, blocked=False, day="2026-10-01",
                  day_used_bytes=12, alerted=[])
    with patch.object(g.time, "time", return_value=start):
        assert not g.observe(legacy, {"s3": 1235})
    assert legacy["month_used_bytes"] == 1235
    g.save(path, legacy)
    assert g.load(path)["month_used_bytes"] == 1235
    legacy["month_used_bytes"] = -1
    g.save(path, legacy)
    try:
        g.load(path)
        raise AssertionError("invalid monthly ledger accepted")
    except ValueError:
        pass


def storage_limits(path):
    state = dict(storage_used_bytes=800, storage_limit_bytes=1000)
    assert not g.observe_storage(state, {"write": 100})
    assert state["storage_used_bytes"] == 900
    assert not g.observe_storage(state, {"write": 100})
    assert g.observe_storage(state, {"write": 200})
    assert state["storage_used_bytes"] == 1000
    for bad in ({}, {"write": 199}):
        try:
            g.observe_storage(state, bad)
            raise AssertionError("lost storage accounting accepted")
        except ValueError:
            pass
        assert state["storage_used_bytes"] == 1000
    try:
        g.observe_storage({}, {"write": 1})
        raise AssertionError("unknown inventory accepted")
    except ValueError:
        pass
    g.disk_room({"Mounts": [{"Destination": "/data", "Source": str(path)}]}, 0)
    try:
        g.disk_room({"Mounts": [{"Destination": "/data", "Source": str(path)}]}, 2**63)
        raise AssertionError("free-space floor ignored")
    except ValueError:
        pass
    # Legacy states may be inspected, but never receive a new lease without
    # an explicit measured baseline.
    legacy = dict(version=2, project="storage-regression", used_bytes=0,
                  last=None, blocked=False, day=g.day_at(time.time()),
                  day_used_bytes=0, alerted=[])
    g.save(path, legacy)
    with patch.object(g, "stop") as stopped, patch.object(g, "telegram"), patch.object(g, "notify"), patch.object(g.time, "sleep", side_effect=LookupError):
        try:
            g.main(["--state-dir", str(path), "run", "--metrics-url", "http://127.0.0.1:4000/metrics",
                    "--telegram-config", str(path / "telegram.json")])
        except LookupError:
            pass
        stopped.assert_called_once()
    assert g.load(path)["blocked"] and (path / "lease").read_text() == "0\n"
    g.main(["--state-dir", str(path), "unblock", "--storage-used-bytes", "800",
            "--storage-limit-bytes", "1000"])
    assert g.load(path)["storage_used_bytes"] == 800
    # Known disk exhaustion must stop immediately even inside startup grace.
    instance = {"Id": "synthetic", "State": {"StartedAt": "first"}}
    with patch.object(g, "docker", return_value=""), patch.object(g, "database", return_value=instance), patch.object(g, "disk_room", side_effect=ValueError("disk full")), patch.object(g, "metrics") as fetched, patch.object(g, "stop") as stopped, patch.object(g, "telegram"), patch.object(g, "notify"), patch.object(g.time, "sleep", side_effect=LookupError):
        try:
            g.main(["--state-dir", str(path), "run", "--metrics-url", "http://127.0.0.1:4000/metrics",
                    "--telegram-config", str(path / "telegram.json")])
        except LookupError:
            pass
        stopped.assert_called_once()
        fetched.assert_not_called()
    assert g.load(path)["blocked"] and (path / "lease").read_text() == "0\n"


def blocked_alerts(path):
    state = dict(version=2, project="blocked-regression", used_bytes=1000,
                 last=None, blocked=False, day=g.day_at(1000),
                 day_used_bytes=1000, alerted=[])
    g.save(path, state)
    config = path / "telegram.json"
    with patch.object(g.time, "time", return_value=1000), patch.object(g, "telegram") as sent:
        g.latch(path, state, "native metrics unavailable: https://user:secret@example.invalid")
        g.alert(path, config, False)
        text = sent.call_args.args[1]
        assert "긴급" in text and "정지 미확인" in text
        assert "secret" not in text and "계측 또는 보호 상태 이상" in text
        assert "계속 가동" not in text
        g.alert(path, config, False)
        assert sent.call_count == 1
        g.alert(path, config, True)
        assert sent.call_count == 2 and "정지 확인" in sent.call_args.args[1]
        g.alert(path, config, True)
        assert sent.call_count == 2
        g.main(["--state-dir", str(path), "unblock"])
        state = g.load(path)
        state.update(used_bytes=g.SOFT_BYTES, day_used_bytes=g.SOFT_BYTES)
        g.save(path, state)
        g.alert(path, config, False)
        assert "계속 가동" in sent.call_args.args[1]
        state = g.load(path)
        g.latch(path, state, "unaccounted S3 helper running")
        g.alert(path, config, False)
        assert sent.call_count == 4 and "계측 외 S3 작업" in sent.call_args.args[1]
    # Delivery failure persists across calls/reloads; exact 60s retries.
    state = g.load(path)
    state.pop("block_alerted")
    state.pop("block_alert_attempt_level")
    g.save(path, state)
    with patch.object(g.time, "time", return_value=2000), patch.object(g, "telegram", side_effect=OSError("secret")) as sent:
        g.alert(path, config, True)
        g.alert(path, config, True)
        assert sent.call_count == 1
    with patch.object(g.time, "time", return_value=2059), patch.object(g, "telegram") as sent:
        g.alert(path, config, True)
        assert sent.call_count == 0
    with patch.object(g.time, "time", return_value=2060), patch.object(g, "telegram") as sent:
        g.alert(path, config, True)
        assert sent.call_count == 1
    # Exercise explicit trip's real stop-result consumer, including failure.
    for failure in (True, False):
        g.main(["--state-dir", str(path), "unblock"])
        with patch.object(g, "stop", side_effect=RuntimeError("Docker unavailable") if failure else None), patch.object(g, "telegram") as sent:
            try:
                g.main(["--state-dir", str(path), "trip", "--project", state["project"],
                        "--telegram-config", str(config)])
                assert not failure
            except RuntimeError:
                assert failure
            text = sent.call_args.args[1]
            assert ("정지 미확인" if failure else "정지 확인") in text
            assert "명시적 차단 / 모니터 종료" in text
    # The running monitor must report the actual result, not just the latch.
    for failure in (True, False):
        state = g.load(path)
        for key in ("block_alerted", "block_alert_attempt_level"):
            state.pop(key, None)
        g.save(path, state)
        with patch.object(g, "stop", side_effect=RuntimeError("Docker unavailable") if failure else None), patch.object(g, "telegram") as sent, patch.object(g, "notify"), patch.object(g.time, "sleep", side_effect=LookupError("end loop")):
            try:
                g.main(["--state-dir", str(path), "run", "--metrics-url", "http://127.0.0.1:4000/metrics",
                        "--telegram-config", str(config)])
                raise AssertionError("monitor did not loop")
            except LookupError:
                pass
            assert ("정지 미확인" if failure else "정지 확인") in sent.call_args.args[1]


def blocked_episode_race(path):
    # Legacy v2 gains an identity without changing accounting or prior delivery.
    state = dict(version=2, project="episode-regression", used_bytes=1234,
                 last=None, blocked=True, day=g.day_at(1000),
                 day_used_bytes=1000, alerted=["soft"], block_alerted="stopped",
                 block_alert_attempt_level="stopped", block_alert_attempt_at=999)
    g.save(path, state)
    with patch.object(g.time, "time", return_value=1000):
        migrated = g.load(path)
        episode = migrated["block_episode"]
        assert episode and g.load(path) == migrated
        assert {k: migrated[k] for k in state} == state
        g.latch(path, migrated, "unaccounted S3 helper running")
        assert g.load(path)["block_episode"] == episode
        with patch.object(g, "telegram") as sent:
            g.alert(path, path / "telegram.json", True)
            assert sent.call_count == 0
        g.main(["--state-dir", str(path), "unblock"])
        g.latch(path, g.load(path), "unaccounted S3 helper running")
        old_episode = g.load(path)["block_episode"]
        assert old_episode != episode

        def unblock_and_trip(config, text):
            assert "계측 외 S3 작업 감지" in text
            g.main(["--state-dir", str(path), "unblock"])
            with patch.object(g, "stop") as stopped:
                g.main(["--state-dir", str(path), "trip", "--project", state["project"]])
                stopped.assert_called_once_with(state["project"], 10)

        with patch.object(g, "telegram", side_effect=unblock_and_trip) as sent:
            g.alert(path, path / "telegram.json", True)
            assert sent.call_count == 1
        current = g.load(path)
        assert current["block_episode"] != old_episode
        assert current["blocked"] and "block_alerted" not in current
        assert current["used_bytes"] == 1234 and current["day_used_bytes"] == 1000
        with patch.object(g, "telegram") as sent:
            g.alert(path, path / "telegram.json", True)
            g.alert(path, path / "telegram.json", True)
            assert sent.call_count == 1
            assert "명시적 차단 / 모니터 종료" in sent.call_args.args[1]
        g.latch(path, g.load(path), "explicit trip / monitor stopped")
        assert g.load(path)["block_episode"] == current["block_episode"]
        assert (g.SOFT_BYTES, g.HARD_BYTES) == (50_000_000_000, 200_000_000_000)


def planned_stop_alerts(path):
    config = path / "telegram.json"
    for prepared, result, matching, protected, stop_failed in (
            (True, "success", True, False, False),
            (False, "success", True, False, False),
            (False, "signal", True, False, False),
            (True, "watchdog", True, False, False),
            (True, "timeout", True, False, False),
            (True, "", True, False, False),
            (True, "success", False, False, False),
            (True, "success", True, True, False),
            (True, "success", True, False, True)):
        state = dict(version=2, project="planned-regression", used_bytes=1234,
                     last=None, blocked=False, day=g.day_at(1000),
                     day_used_bytes=1000, alerted=[])
        g.save(path, state)
        if protected:
            g.latch(path, state, "daily hard download limit reached")
        with patch.dict(os.environ, INVOCATION_ID="current", SERVICE_RESULT=result,
                        MAINPID=str(os.getpid()) if prepared else ""):
            g.main(["--state-dir", str(path), "prepare-stop"])
            if not matching:
                os.environ["INVOCATION_ID"] = "new-invocation"
            with patch.object(g, "stop", side_effect=RuntimeError("stop failed") if stop_failed else None) as stopped, patch.object(g, "telegram") as sent:
                try:
                    g.main(["--state-dir", str(path), "trip", "--systemd-stop",
                            "--project", state["project"], "--telegram-config", str(config)])
                    assert not stop_failed
                except RuntimeError:
                    assert stop_failed
                stopped.assert_called_once_with(state["project"], 10)
                quiet = prepared and result == "success" and matching and not protected and not stop_failed
                assert sent.call_count == int(not quiet)
                current = g.load(path)
                assert current["blocked"] and (path / "lease").read_text() == "0\n"
                assert (current["used_bytes"], current["day_used_bytes"]) == (1234, 1000)
                assert not (path / "planned-stop").exists()
                if quiet:
                    g.alert(path, config, True)
                    assert sent.call_count == 0
                    # No fresh marker: even a clean unexpected exit must alert.
                    g.main(["--state-dir", str(path), "trip", "--systemd-stop",
                            "--project", state["project"], "--telegram-config", str(config)])
                    assert sent.call_count == 1
                if protected:
                    assert current["reason"] == "daily hard download limit reached"
                if stop_failed:
                    g.alert(path, config, True)
                    assert sent.call_count == 2


def condition_alerts(path):
    import re
    config = path / "telegram.json"
    state = dict(version=2, project="condition-regression", used_bytes=4_000_000_000,
                 day_used_bytes=2_981_714_767, day=g.day_at(1000), alerted=[],
                 blocked=False, sample_at=939, last={}, startup_until=0,
                 storage_used_bytes=0, storage_limit_bytes=1000)
    g.save(path, state)
    readings = {json.dumps({"operation": op}): 0 for op in ("read", "write")}
    with patch.object(g.time, "time", return_value=1000), patch.object(g, "docker", return_value=""), patch.object(g, "database", return_value={"Id": "one", "State": {"StartedAt": "start"}}), patch.object(g, "disk_room", create=True), patch.object(g, "metrics", return_value=readings), patch.object(g, "stop"), patch.object(g, "telegram") as sent, patch.object(g, "notify"), patch.object(g.time, "sleep", side_effect=LookupError):
        try:
            g.main(["--state-dir", str(path), "run", "--metrics-url", "http://127.0.0.1:4000/metrics",
                    "--interval", "2", "--timeout", "10", "--telegram-config", str(config)])
        except LookupError:
            pass
        current = g.load(path)
        assert current["observation_gap_seconds"] == 61
        assert current["monitor_limits"]["gap_seconds"] == 60
        assert current["day_used_bytes"] == state["day_used_bytes"] < g.SOFT_BYTES
        text = sent.call_args.args[1]
        assert "감시 관측 지연" in text and "중단 요건:" in text
        assert [float(v) for v in re.findall(r"([\d.]+)초", text)] == [61, 60]
        headers = {text.splitlines()[0]}
    for error in ("TimeoutError: timed out",
                  "HTTPError: HTTP Error 503: https://user:secret@example.invalid",
                  "URLError: <urlopen error https://user:secret@example.invalid>",
                  "ValueError: native counter reset",
                  "ValueError: private https://user:secret@example.invalid"):
        g.save(path, dict(state, monitor_limits={"metrics_timeout_seconds": 10}))
        g.latch(path, g.load(path), "native metrics unavailable, stale, or reset: " + error)
        with patch.object(g, "telegram") as sent:
            g.alert(path, config, True)
            text = sent.call_args.args[1]
            assert "secret" not in text and "example.invalid" not in text
            assert "중단 요건:" in text
            headers.add(text.splitlines()[0])
            if error.startswith("HTTPError"):
                assert "503" in text
            if error.startswith("TimeoutError"):
                assert [float(v) for v in re.findall(r"([\d.]+)초", text)] == [10]
    assert len(headers) == 6


def transient_metrics(path):
    read = json.dumps({"operation": "read"})
    write = json.dumps({"operation": "write"})
    base = dict(version=2, project="retry-regression", used_bytes=10,
                day_used_bytes=10, month_used_bytes=10, month=g.day_at(1000)[:7],
                day=g.day_at(1000), alerted=[], blocked=False, sample_at=990,
                last={read: 10}, last_write={write: 0}, startup_until=0,
                storage_used_bytes=0, storage_limit_bytes=1000)

    def run(samples, times, initial=None):
        g.save(path, dict(base if initial is None else initial))
        g.atomic(path / "lease", "995\n")
        clock = [times[0]]
        index = [0]

        def sleep(_):
            index[0] += 1
            if index[0] == len(times):
                raise LookupError
            clock[0] = times[index[0]]

        with patch.object(g.time, "time", side_effect=lambda: clock[0]), patch.object(g, "docker", return_value=""), patch.object(g, "database", return_value={"Id": "one", "State": {"StartedAt": "start"}}), patch.object(g, "disk_room"), patch.object(g, "metrics", side_effect=samples), patch.object(g, "stop"), patch.object(g, "telegram"), patch.object(g, "notify"), patch.object(g.time, "sleep", side_effect=sleep):
            try:
                g.main(["--state-dir", str(path), "run", "--metrics-url", "http://127.0.0.1:4000/metrics",
                        "--timeout", "10", "--telegram-config", str(path / "telegram.json")])
            except LookupError:
                pass
        return g.load(path)

    current = run([TimeoutError()], [1000])
    assert not current["blocked"] and current["metrics_failure_count"] == 1
    assert current["last"] == base["last"] and current["used_bytes"] == 10
    assert (path / "lease").read_text() == "995\n"  # expired, never renewed by failure
    current = run([TimeoutError(), ConnectionError(), {read: 30, write: 0}], [1000, 1002, 1004])
    assert not current["blocked"] and current["used_bytes"] == 30
    assert "metrics_failure_count" not in current
    current = run([TimeoutError(), {read: 25, write: 0}], [1000, 1002])
    assert not current["blocked"] and current["used_bytes"] == 25
    assert "metrics_failure_count" not in current and int((path / "lease").read_text()) > 1002
    current = run([TimeoutError()] * 3, [1000, 1002, 1004])
    assert current["blocked"] and current["metrics_failure_count"] == 3
    assert current["metrics_failure_at"] == 1004 and current["metrics_failure_age_seconds"] == 14
    current = run([TimeoutError()], [1051])
    assert current["blocked"] and current["observation_gap_seconds"] == 61
    current = run([{read: 25, write: 0}], [1051])
    assert current["blocked"] and current["used_bytes"] == 25  # charge late successful observation
    current = run([{read: 25, write: 0}], [1050])
    assert not current["blocked"] and current["used_bytes"] == 25  # exactly 60 seconds is allowed
    current = run([TimeoutError(), {read: g.HARD_BYTES, write: 0}], [1000, 1002])
    assert current["blocked"] and current["day_used_bytes"] == g.HARD_BYTES
    for sample in (ValueError("cached metrics"), ValueError("native S3 read metrics missing"),
                   {read: 9, write: 0},
                   g.urllib.error.HTTPError("private", 403, "forbidden", {}, None)):
        current = run([sample], [1000])
        assert current["blocked"] and "metrics_failure_count" not in current
    initial = dict(base, last=None, startup_until=999)
    assert run([TimeoutError()], [1000], initial)["blocked"]
    for code in (429, 500, 503):
        current = run([g.urllib.error.HTTPError("private", code, "retry", {}, None)], [1000])
        assert not current["blocked"] and current["metrics_failure_count"] == 1


def daily_docker(path):
    """Real monitor/HTTP/Docker stop, without sending synthetic alerts to users."""
    path.mkdir(mode=0o700)
    project = "daily-guard-smoke-" + str(time.time_ns())
    counter = path / "counter"
    counter.write_text(str(g.SOFT_BYTES - 1))
    server = """
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = ('opendal_operation_bytes_sum{scheme="s3",operation="read"} '
                + Path('/probe/counter').read_text()
                + '\\nopendal_operation_bytes_sum{scheme="s3",operation="write"} 0\\n').encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
HTTPServer(('0.0.0.0', 4000), Handler).serve_forever()
"""
    worker = None
    try:
        g.docker("run", "-d", "--name", project,
                 "--label", "com.docker.compose.project=" + project,
                 "--label", "com.docker.compose.service=greptimedb",
                 "-p", "127.0.0.1::4000", "-v", str(path) + ":/probe:ro",
                 "-v", str(path) + ":/data:ro",
                 "python:3.12.8-slim-bookworm", "python", "-c", server)
        port = json.loads(g.docker("inspect", project))[0]["NetworkSettings"]["Ports"]["4000/tcp"][0]["HostPort"]
        url = "http://127.0.0.1:" + port + "/metrics"
        deadline = time.time() + 20
        while True:
            try:
                g.metrics(url, 1)
                break
            except OSError:
                if time.time() >= deadline:
                    raise
                time.sleep(0.1)
        g.main(["--state-dir", str(path), "arm", "--project", project,
                "--storage-used-bytes", "0"])
        worker = subprocess.Popen([sys.executable, str(SCRIPT), "--state-dir", str(path),
                                   "run", "--metrics-url", url, "--interval", "0.1",
                                   "--timeout", "1", "--grace", "1",
                                   "--telegram-config", str(path / "intentionally-absent.json")])
        def reached(predicate):
            deadline = time.time() + 20
            while time.time() < deadline:
                state = g.load(path)
                if predicate(state):
                    return state
                if worker.poll() is not None:
                    raise AssertionError("guard worker exited")
                time.sleep(0.1)
            raise AssertionError("daily guard did not reach expected state")
        reached(lambda s: s["day_used_bytes"] == g.SOFT_BYTES - 1)
        g.atomic(counter, str(g.SOFT_BYTES))
        soft = reached(lambda s: s["day_used_bytes"] == g.SOFT_BYTES)
        assert not soft["blocked"] and soft["alerted"] == []
        assert json.loads(g.docker("inspect", project))[0]["State"]["Running"]
        g.atomic(counter, str(g.HARD_BYTES))
        hard = reached(lambda s: s["blocked"] and
                       not json.loads(g.docker("inspect", project))[0]["State"]["Running"])
        assert hard["blocked"] and hard["day_used_bytes"] == g.HARD_BYTES
        assert not json.loads(g.docker("inspect", project))[0]["State"]["Running"]
        assert (path / "lease").read_text() == "0\n"
    finally:
        if worker is not None:
            worker.terminate()
            worker.wait(timeout=5)
        g.docker("rm", "-f", project)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        unsafe = path / "unsafe"
        daily_path = path / "daily"
        daily_path.mkdir(mode=0o700)
        daily_limits(daily_path)
        monthly_path = path / "monthly"
        monthly_path.mkdir(mode=0o700)
        monthly_limits(monthly_path)
        storage_path = path / "storage"
        storage_path.mkdir(mode=0o700)
        storage_limits(storage_path)
        blocked_path = path / "blocked"
        blocked_path.mkdir(mode=0o700)
        blocked_alerts(blocked_path)
        episode_path = path / "episode"
        episode_path.mkdir(mode=0o700)
        blocked_episode_race(episode_path)
        planned_path = path / "planned"
        planned_path.mkdir(mode=0o700)
        planned_stop_alerts(planned_path)
        condition_path = path / "condition"
        condition_path.mkdir(mode=0o700)
        condition_alerts(condition_path)
        transient_path = path / "transient"
        transient_path.mkdir(mode=0o700)
        transient_metrics(transient_path)
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
        state = dict(version=2, project="guard-regression", used_bytes=0, last=None,
                     blocked=False, day=g.day_at(time.time()), day_used_bytes=0, alerted=[])
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
                                 "--project", "guard-regression"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert result.returncode != 0
        assert g.load(path)["used_bytes"] == 60 and g.load(path)["blocked"]
        subprocess.run([sys.executable, str(SCRIPT), "--state-dir", tmp, "unblock",
                        "--startup-seconds", "1", "--storage-used-bytes", "0"],
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
                                       "-v", tmp + ":/egress-guard:ro",
                                       "-v", tmp + ":/data:ro", image,
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
                                           "--interval", "0.1", "--timeout", "1", "--grace", "1",
                                           "--telegram-config", str(path / "telegram.json")])
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
            daily_docker(path / "daily-docker")
    print("egress guard: daily thresholds/rollover/alerts/persistence/reset/metrics failure/core gate passed")


if __name__ == "__main__":
    main()
