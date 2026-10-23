#!/usr/bin/env python3
"""Host-only, fail-closed Greptime S3 daily and monthly read guard.

Native counters measure observed OpenDAL reads, not provider billing. Helpers'
s3_cost retained reports are incomplete lower bounds and are NOT included.
Polling plus Docker stop has unbounded byte overshoot (not a network quota).
Run outside Compose under systemd; ExecStopPost must invoke `trip` so a dead
worker revokes its lease and stops this exact project. Never mount Docker's
socket in application containers. State and lease directory must persist.
"""
import argparse
import contextlib
from datetime import datetime, timedelta, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import tempfile
import time
import urllib.request
import urllib.parse
import uuid

COVERAGE = "native Greptime OpenDAL S3 read bytes only; helper/account egress NOT covered"
SOFT_BYTES = 50_000_000_000
HARD_BYTES = 200_000_000_000
MONTH_BYTES = 2400 * 1024**3
STORAGE_BYTES = 1200 * 1024**3
MIN_FREE_BYTES = 1024**3
KST = timezone(timedelta(hours=9))


def day_at(timestamp):
    return datetime.fromtimestamp(timestamp, KST).date().isoformat()


def daily(state, now):
    # Older v2 states have no monthly ledger. Charge lifetime usage rather than
    # inventing an unobserved zero balance during migration.
    state.setdefault("month", state["day"][:7])
    state.setdefault("month_used_bytes", state["used_bytes"])
    day = day_at(now)
    if day < state["day"]:
        raise ValueError("guard clock moved to an earlier day")
    if day != state["day"]:
        state.update(day=day, day_used_bytes=0, alerted=[], alert_attempt_at=0)
    month = day[:7]
    if month < state["month"]:
        raise ValueError("guard clock moved to an earlier month")
    if month != state["month"]:
        state.update(month=month, month_used_bytes=0)

def notify():
    """Notify only after a completed loop; a stalled worker misses watchdog."""
    address = os.environ.get("NOTIFY_SOCKET")
    if address:
        if address.startswith("@"):
            address = "\0" + address[1:]  # Linux abstract Unix socket
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as channel:
            channel.settimeout(1)
            channel.connect(address)
            channel.sendall(b"READY=1\nWATCHDOG=1")



def atomic(path, text):
    fd, name = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as out:
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextlib.contextmanager
def locked(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = directory.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise PermissionError("guard state directory must be owned by current user with mode 0700")
    with (directory / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def save(directory, state):
    atomic(directory / "state.json", json.dumps(state, sort_keys=True) + "\n")


def load(directory):
    state = json.loads((directory / "state.json").read_text())
    if (state.get("version") != 2 or type(state.get("blocked")) is not bool
            or any(type(state.get(k)) is not int or state[k] < 0
                   for k in ("used_bytes", "day_used_bytes"))
            or state["day_used_bytes"] > state["used_bytes"]
            or not isinstance(state.get("alerted"), list)
            or any(v not in ("soft", "hard") for v in state["alerted"])):
        raise ValueError("invalid persistent guard state")
    datetime.strptime(state["day"], "%Y-%m-%d")
    if ("month" in state) != ("month_used_bytes" in state):
        raise ValueError("incomplete persistent monthly ledger")
    if "month" in state:
        datetime.strptime(state["month"], "%Y-%m")
        if (type(state["month_used_bytes"]) is not int
                or not 0 <= state["month_used_bytes"] <= state["used_bytes"]
                or state["month"] != state["day"][:7]):
            raise ValueError("invalid persistent monthly ledger")
    storage_keys = ("storage_used_bytes", "storage_limit_bytes")
    if any(k in state for k in storage_keys):
        if any(type(state.get(k)) is not int or state[k] < 0 for k in storage_keys):
            raise ValueError("invalid persistent storage budget")
        if not state["storage_limit_bytes"]:
            raise ValueError("storage budget must be positive")
    if state["blocked"] and "block_episode" not in state:
        state["block_episode"] = uuid.uuid4().hex
        save(directory, state)
    return state


def revoke(directory):
    atomic(directory / "lease", "0\n")


def latch(directory, state, reason):
    # Revoke before state write: crash between the two still denies new starts.
    revoke(directory)
    if not state["blocked"]:
        state["block_episode"] = uuid.uuid4().hex
        for key in ("block_alerted", "block_alert_attempt_at", "block_alert_attempt_level"):
            state.pop(key, None)
    elif "block_episode" not in state:
        state["block_episode"] = uuid.uuid4().hex
    state.update(blocked=True, reason=reason)
    save(directory, state)


def observe(state, values):
    """Keep each series baseline; missing series or reset loses accounting."""
    before = state.get("last")
    if before is not None:
        if not before.keys() <= values.keys():
            raise ValueError("native metric series disappeared")
        if any(values[k] < v for k, v in before.items()):
            raise ValueError("native counter reset")
    now = time.time()
    daily(state, now)
    delta = sum(v - (before or {}).get(k, 0) for k, v in values.items())
    state["used_bytes"] += delta
    # Charge a sample straddling midnight to the new day, never discard bytes.
    state["day_used_bytes"] += delta
    state["month_used_bytes"] += delta
    state["last"] = values
    state["sample_at"] = now
    return state["day_used_bytes"] >= HARD_BYTES or state["month_used_bytes"] >= MONTH_BYTES


def observe_storage(state, values):
    """Conservative storage ceiling: baseline plus writes, never deletion credit."""
    if "storage_used_bytes" not in state:
        raise ValueError("storage inventory unknown; provide --storage-used-bytes on arm/unblock")
    before = state.get("last_write")
    if not values or (before is not None and (
            not before.keys() <= values.keys()
            or any(values[k] < v for k, v in before.items()))):
        raise ValueError("native S3 write metrics missing or reset")
    state["storage_used_bytes"] += sum(v - (before or {}).get(k, 0)
                                       for k, v in values.items())
    state["last_write"] = values
    return state["storage_used_bytes"] >= state["storage_limit_bytes"]


def disk_room(container, minimum):
    mounts = [m for m in container.get("Mounts", ()) if m["Destination"] == "/data"]
    if len(mounts) != 1:
        raise ValueError("Greptime data volume unknown")
    usage = os.statvfs(mounts[0]["Source"])
    if usage.f_bavail * usage.f_frsize < minimum:
        raise ValueError("Greptime data volume free-space floor reached")


def metrics(url, timeout, *, include_writes=False):
    request = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.headers.get("Age") not in (None, "0"):
            raise ValueError("cached metrics")
        text = response.read(16 * 1024 * 1024 + 1)
    if len(text) > 16 * 1024 * 1024:
        raise ValueError("metrics too large")
    result = {}
    for line in text.decode().splitlines():
        match = re.fullmatch(r'(opendal_operation_bytes_sum\{([^}]*)\})\s+(\S+)(?:\s+(\d+))?', line)
        if not match:
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', match[2]))
        if (labels.get("operation") not in (("read", "write") if include_writes else ("read",))
                or labels.get("scheme") != "s3"):
            continue
        value = float(match[3])
        if not math.isfinite(value) or value < 0 or not value.is_integer():
            raise ValueError("invalid native bytes")
        if match[4] and abs(time.time() - int(match[4]) / 1000) > timeout:
            raise ValueError("stale native metrics")
        key = json.dumps(labels, sort_keys=True)
        if key in result:
            raise ValueError("duplicate native series")
        result[key] = int(value)
    if not result:
        raise ValueError("native S3 read metric missing")
    return result


def docker(*args, timeout=30):
    return subprocess.run(["docker", *args], check=True, capture_output=True,
                          text=True, timeout=timeout).stdout.strip()


def bind_instance(state, container):
    identity = [container["Id"], container["State"]["StartedAt"]]
    if state.get("instance") is not None and state["instance"] != identity:
        raise ValueError("Greptime instance changed")
    state["instance"] = identity


def database(containers, url):
    matches = [c for c in containers if
               c["Config"]["Labels"].get("com.docker.compose.service") == "greptimedb"]
    if len(matches) != 1:
        raise ValueError("exactly one project Greptime instance required")
    container = matches[0]
    endpoint = urllib.parse.urlsplit(url)
    bindings = container["NetworkSettings"]["Ports"].get("4000/tcp") or []
    if (endpoint.scheme != "http" or endpoint.hostname not in ("127.0.0.1", "::1", "localhost")
            or endpoint.path != "/metrics" or endpoint.query or endpoint.username
            or not any(int(p["HostPort"]) == (endpoint.port or 80) and
                       p["HostIp"] in ("0.0.0.0", "::", endpoint.hostname)
                       for p in bindings)):
        raise ValueError("metrics URL is not this project's published Greptime port")
    return container


def stop(project, grace):
    # Labels select only this exact project, including one-off/helper containers.
    ids = docker("ps", "-aq", "--filter", "label=com.docker.compose.project=" + project).split()
    if ids:
        try:
            docker("stop", "--time", str(grace), *ids, timeout=grace + 30)
        except (subprocess.SubprocessError, OSError):
            docker("kill", *ids)
        states = json.loads(docker("inspect", *ids))
        active = [v["Id"] for v in states if v["State"]["Running"] or v["State"].get("Restarting")]
        if active:
            docker("kill", *active)
    # Catch containers recreated while stop was in progress, rather than report
    # the original IDs as proof that the entire project is stopped.
    running = docker("ps", "-q", "--filter", "label=com.docker.compose.project=" + project)
    if running:
        raise RuntimeError("project still has running containers")



def telegram(config, text):
    info = config.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
        raise PermissionError("Telegram config must be owned by current user with mode 0600")
    credentials = json.loads(config.read_text())
    token, chat = credentials["token"], credentials["chat_id"]
    if not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", token) or type(chat) is not int:
        raise ValueError("invalid Telegram credentials")
    request = urllib.request.Request(
        "https://api.telegram.org/bot" + token + "/sendMessage",
        data=json.dumps({"chat_id": chat, "text": text}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=5) as response:
        result = json.load(response)
    if not result.get("ok"):
        raise RuntimeError("Telegram rejected notification")


def alert(directory, config, stopped):
    # Persist acknowledgement only after delivery. A crash in between can
    # duplicate one alert; losing a hard-limit notification is worse.
    with locked(directory):
        state = load(directory)
        blocked = state["blocked"]
        episode = state.get("block_episode")
        level = (("stopped" if stopped else "unconfirmed") if blocked else
                 "hard" if state["day_used_bytes"] >= HARD_BYTES else
                 "soft" if state["day_used_bytes"] >= SOFT_BYTES else None)
        acknowledged = (state.get("block_alerted") == level if blocked else
                        level in state["alerted"])
        if not level or acknowledged:
            return
        now = time.time()
        prefix = "block_alert" if blocked else "alert"
        if state.get(prefix + "_attempt_level") == level and now - state.get(prefix + "_attempt_at", 0) < 60:
            return
        state[prefix + "_attempt_at"] = now
        state[prefix + "_attempt_level"] = level
        save(directory, state)
        day = state["day"]
        action = ("프로젝트 정지 확인. 수동 확인 후 재개하세요." if stopped else
                  "정지 미확인: 호스트/Docker 상태를 즉시 확인하세요.") if blocked or level == "hard" else "서비스는 계속 가동합니다."
        # Diagnostics may contain URLs/credentials. Only fixed cause labels
        # leave the host; retain the original reason in the private state.
        reason = state.get("reason", "")
        cause = {
            "daily hard download limit reached": "일일 다운로드 한도 초과",
            "monthly hard download limit reached": "월간 다운로드 한도 초과",
            "storage hard limit reached": "저장 용량 예산 초과",
            "Greptime instance changed": "Greptime 인스턴스 변경",
            "unaccounted S3 helper running": "계측 외 S3 작업 감지",
            "explicit trip / monitor stopped": "명시적 차단 / 모니터 종료",
        }.get(reason, "계측 또는 보호 상태 이상")
        if blocked:
            action = f"보호 차단: {cause}\n{action}"
        text = (f"[Datalake {'긴급' if blocked or level == 'hard' else '주의'}]\n"
                f"{day} KST S3 다운로드 {state['day_used_bytes'] / 1e9:.3f} GB\n"
                f"소프트 50 GB / 하드 200 GB (일일)\n"
                f"월간 {state.get('month_used_bytes', state['used_bytes']) / 1024**3:.3f}"
                f" / {MONTH_BYTES / 1024**3:.0f} GiB\n{action}\n"
                "Greptime 계측 기준이며 공급자 청구량과 다를 수 있습니다.")
    try:
        telegram(config, text)
    except Exception:
        # Exceptions can contain the token-bearing URL. Never log them.
        print("Telegram notification failed; retry in 60 seconds", flush=True)
        return
    with locked(directory):
        state = load(directory)
        if blocked:
            if state["blocked"] and state.get("block_episode") == episode:
                state["block_alerted"] = level
                save(directory, state)
        elif state["day"] == day:
            state["alerted"] = ["soft", "hard"] if level == "hard" else ["soft"]
            save(directory, state)

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("arm", "unblock"):
        p = sub.add_parser(name)
        p.add_argument("--startup-seconds", type=int, default=30)
        p.add_argument("--storage-used-bytes", type=int,
                       help="fresh measured bucket inventory; required before the first run")
        p.add_argument("--storage-limit-bytes", type=int, default=STORAGE_BYTES)
        if name == "arm":
            p.add_argument("--project", required=True)
    sub.add_parser("status")
    trip = sub.add_parser("trip")
    trip.add_argument("--grace", type=int, default=10)
    trip.add_argument("--telegram-config", type=Path,
                      help="send stop outcome when state is readable; no retry worker")
    trip.add_argument("--project", required=True,
                      help="trusted exact project to stop even if persistent state is unreadable")
    run = sub.add_parser("run")
    run.add_argument("--metrics-url", required=True)
    run.add_argument("--interval", type=float, default=2)
    run.add_argument("--timeout", type=float, default=3)
    run.add_argument("--grace", type=int, default=10)
    run.add_argument("--telegram-config", required=True, type=Path)
    run.add_argument("--min-free-bytes", type=int, default=MIN_FREE_BYTES)
    args = parser.parse_args(argv)
    directory = args.state_dir.resolve()
    if hasattr(args, "grace") and not 0 <= args.grace <= 60:
        parser.error("grace must be 0..60 seconds")
    if args.command == "trip":
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", args.project):
            parser.error("invalid exact Compose project")
        try:
            with locked(directory):
                revoke(directory)
                state = load(directory)
                if state["project"] != args.project:
                    raise ValueError("trip project differs from persistent state; selected project stopped")
                latch(directory, state, "explicit trip / monitor stopped")
        finally:
            # Storage failure must never bypass the host's explicit stop target.
            # Do not recreate malformed state or discard its usage evidence.
            stopped = False
            try:
                stop(args.project, args.grace)
                stopped = True
            finally:
                if args.telegram_config:
                    try:
                        alert(directory, args.telegram_config, stopped)
                    except Exception:
                        print("Trip notification unavailable; inspect private guard state", flush=True)
        return
    with locked(directory):
        if args.command == "arm":
            if (directory / "state.json").exists():
                parser.error("already armed; status/unblock never resets usage")
            if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", args.project):
                parser.error("invalid exact Compose project")
            state = dict(version=2, project=args.project, used_bytes=0, last=None,
                         blocked=True, day=day_at(time.time()), day_used_bytes=0, alerted=[])
        else:
            state = load(directory)
        if args.command in ("arm", "unblock"):
            daily(state, time.time())
            if args.storage_used_bytes is not None:
                if (args.storage_used_bytes < 0 or args.storage_limit_bytes <= 0
                        or args.storage_used_bytes >= args.storage_limit_bytes):
                    parser.error("storage inventory must be nonnegative and below its positive budget")
                if args.command == "unblock" and not state["blocked"]:
                    parser.error("storage baseline may only change while blocked")
                state.update(storage_used_bytes=args.storage_used_bytes,
                             storage_limit_bytes=args.storage_limit_bytes, last_write=None)
            if ("storage_used_bytes" in state
                    and state["storage_used_bytes"] >= state["storage_limit_bytes"]):
                parser.error("storage hard limit reached; fresh inventory required")
            if (state["day_used_bytes"] >= HARD_BYTES
                    or state["month_used_bytes"] >= MONTH_BYTES
                    or not 1 <= args.startup_seconds <= 120):
                parser.error("daily/monthly hard limit reached or startup seconds outside 1..120")
            if args.command == "unblock" and not state["blocked"]:
                parser.error("not blocked")
            # Explicit authorization after reset starts a new baseline, charging
            # its entire first counter. Persisted usage is never subtracted.
            revoke(directory)
            state.update(blocked=False, reason="authorized",
                         last=None, instance=None, startup_until=time.time() + args.startup_seconds)
            state["last_write"] = None
            for key in ("block_episode", "block_alerted", "block_alert_attempt_at", "block_alert_attempt_level"):
                state.pop(key, None)
            save(directory, state)
            # run must start before DB: arm alone does not grant a lease.
        if args.command != "run":
            print(json.dumps(dict(state, coverage=COVERAGE), sort_keys=True))
    if args.command != "run":
        return
    if (args.interval <= 0 or args.timeout <= 0 or args.interval + args.timeout > 30
            or args.min_free_bytes < MIN_FREE_BYTES):
        parser.error("positive polling/timeout, combined maximum 30 seconds; minimum free space 1 GiB")
    # Separate process lock prevents two monitor instances without blocking trip.
    with (directory / "monitor.lock").open("a") as worker:
        fcntl.flock(worker, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            with locked(directory):
                state = load(directory)
                containers = []
                if not state["blocked"] and "storage_used_bytes" not in state:
                    latch(directory, state, "storage inventory unknown")
                if not state["blocked"]:
                    ids = docker("ps", "-q", "--filter",
                                 "label=com.docker.compose.project=" + state["project"]).split()
                    if ids:
                        containers = json.loads(docker("inspect", *ids))
                        # backup/can-backup also implement restore; redecode
                        # can fetch raw archives outside DB-native accounting.
                        if any(c["Config"]["Labels"].get("com.docker.compose.service") in
                               {"backup", "can-backup", "raw-uploader", "redecode"}
                               for c in containers):
                            latch(directory, state, "unaccounted S3 helper running")
                if not state["blocked"]:
                    try:
                        container = database(containers, args.metrics_url)
                        try:
                            bind_instance(state, container)
                        except ValueError:
                            latch(directory, state, "Greptime instance changed")
                            raise
                        try:
                            disk_room(container, args.min_free_bytes)
                        except (OSError, ValueError):
                            # Startup grace permits missing metrics, never an
                            # already-known full or uninspectable data volume.
                            latch(directory, state, "local storage unavailable or full")
                            raise
                        all_values = metrics(args.metrics_url, args.timeout, include_writes=True)
                        values = {k: v for k, v in all_values.items()
                                  if json.loads(k)["operation"] == "read"}
                        writes = {k: v for k, v in all_values.items()
                                  if json.loads(k)["operation"] == "write"}
                        if not values:
                            raise ValueError("native S3 read metrics missing")
                        if state.get("sample_at") and state.get("last") is not None and time.time() - state["sample_at"] > args.interval + args.timeout + 5:
                            raise ValueError("monitor observation gap")
                        storage_full = observe_storage(state, writes)
                        download_full = observe(state, values)
                        if storage_full or download_full:
                            reason = ("storage hard limit reached" if storage_full else
                                      "monthly hard download limit reached"
                                      if state["month_used_bytes"] >= MONTH_BYTES
                                      else "daily hard download limit reached")
                            latch(directory, state, reason)
                        else:
                            save(directory, state)
                    except Exception as exc:
                        if state["blocked"] or state.get("last") is not None or time.time() >= state["startup_until"]:
                            latch(directory, state,
                                  f"native metrics unavailable, stale, or reset: {type(exc).__name__}: {exc}")
                    if not state["blocked"]:
                        atomic(directory / "lease", str(int(time.time() + args.interval + args.timeout + 5)) + "\n")
                blocked = state["blocked"]
            stopped = False
            if blocked:
                try:
                    stop(state["project"], args.grace)
                    stopped = True
                except Exception:
                    print("Project stop failed; retrying next loop", flush=True)
            alert(directory, args.telegram_config, stopped)
            notify()
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
