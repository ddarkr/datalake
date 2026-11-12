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
import urllib.error
import uuid

COVERAGE = "native Greptime OpenDAL S3 read bytes only; helper/account egress NOT covered"
SOFT_BYTES = 50_000_000_000
HARD_BYTES = 200_000_000_000
MONTH_BYTES = 2400 * 1024**3
STORAGE_BYTES = 1200 * 1024**3
MIN_FREE_BYTES = 1024**3
MAX_METRICS_FAILURES = 3
MAX_OBSERVATION_GAP = 60
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


def clear_metrics_failures(state):
    for key in ("metrics_failure_count", "metrics_failure_at", "metrics_failure_age_seconds"):
        state.pop(key, None)


def retry_metrics(state, exc):
    """Tolerate transport failures only after an established valid baseline."""
    transient = (exc.code == 429 or 500 <= exc.code <= 599) if isinstance(exc, urllib.error.HTTPError) else isinstance(
        exc, (TimeoutError, ConnectionError, urllib.error.URLError))
    if not transient:
        clear_metrics_failures(state)
        return False
    if state.get("last") is None:
        return False
    now = time.time()
    count = state.get("metrics_failure_count", 0) + 1
    age = now - state["sample_at"]
    state.update(metrics_failure_count=count, metrics_failure_at=now,
                 metrics_failure_age_seconds=age)
    print(f"Metrics transient failure count={count}/{MAX_METRICS_FAILURES} age_seconds={age:.3f}", flush=True)
    if age > MAX_OBSERVATION_GAP:
        state["observation_gap_seconds"] = age
        return False
    return count < MAX_METRICS_FAILURES


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
    # Active project containers include paused/restarting and one-off helpers.
    ids = docker("ps", "-q", "--filter", "label=com.docker.compose.project=" + project).split()
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



def stop_condition(state):
    """Public cause/criterion only; never forward raw exception text."""
    limits = state.get("monitor_limits", {})
    reason = state.get("reason", "")

    def seconds(value):
        return f"{value:.3f}초" if type(value) in (int, float) and math.isfinite(value) and value >= 0 else "미기록"

    causes = {
        "daily hard download limit reached": ("일일 다운로드 한도 초과", f"일일 다운로드 ≥ {HARD_BYTES / 1e9:g} GB"),
        "monthly hard download limit reached": ("월간 다운로드 한도 초과", "월간 다운로드 ≥ 설정된 월간 하드 한도"),
        "storage hard limit reached": ("저장 용량 예산 초과", "보존 저장량 ≥ 설정된 저장 용량 예산"),
        "storage inventory unknown": ("저장량 기준값 누락", "저장량 ledger 없이 감시 실행"),
        "local storage unavailable or full": ("로컬 저장소 보호 차단", "데이터 볼륨 조회 실패 또는 최소 여유 공간 미달"),
        "Greptime instance changed": ("Greptime 인스턴스 변경", "컨테이너 ID 또는 시작 시각이 승인된 기준과 다름"),
        "unaccounted S3 helper running": ("계측 외 S3 작업 감지", "backup·can-backup·raw-uploader·redecode 실행 감지"),
        "planned monitor stop": ("계획 정지", "계획정지 후 프로젝트 정지 확인 실패"),
    }
    if reason in causes:
        return causes[reason]
    if reason == "explicit trip / monitor stopped":
        info = state.get("monitor_exit", {})
        result = info.get("SERVICE_RESULT")
        if result in ("success", "exit-code", "signal", "core-dump", "watchdog", "timeout", "oom-kill", "resources", "protocol"):
            return "모니터 비정상 종료", f"계획정지 확인 없이 감시 종료; systemd 결과={result}"
        return "명시적 차단 / 모니터 종료", "명시적인 trip 명령 또는 감시기 종료"
    prefix = "native metrics unavailable, stale, or reset: "
    if reason.startswith(prefix):
        kind, _, detail = reason[len(prefix):].partition(": ")
        if kind == "ValueError":
            if detail == "monitor observation gap":
                return "감시 관측 지연", (
                    f"실제 관측 간격 {seconds(state.get('observation_gap_seconds'))}"
                    f" > 허용 {seconds(limits.get('gap_seconds'))}")
            errors = {
                "native counter reset": ("계측 카운터 감소", "동일 인스턴스의 누적 다운로드 카운터가 이전 값보다 작음"),
                "native metric series disappeared": ("계측 계열 소실", "이전 관측에 있던 S3 read 계열이 사라짐"),
                "native S3 read metric missing": ("S3 read 계측 누락", "유효한 S3 read 계열이 응답에 없음"),
                "native S3 read metrics missing": ("S3 read 계측 누락", "유효한 S3 read 계열이 응답에 없음"),
                "native S3 write metrics missing or reset": ("S3 write 계측 이상", "S3 write 계열 누락 또는 카운터 감소"),
                "stale native metrics": ("오래된 계측 표본", "계측 표본 시각이 허용 응답 시간보다 오래됨"),
                "cached metrics": ("캐시된 계측 응답", "HTTP Age가 0이 아닌 응답"),
                "metrics too large": ("계측 응답 크기 초과", "응답 크기 > 16 MiB"),
                "invalid native bytes": ("잘못된 계측 값", "음수·비정수·유한하지 않은 다운로드 카운터"),
                "duplicate native series": ("중복 계측 계열", "같은 label 조합의 S3 계열이 중복됨"),
                "Greptime instance changed": causes["Greptime instance changed"],
                "Greptime data volume unknown": ("데이터 볼륨 식별 실패", "/data mount가 정확히 하나가 아님"),
                "Greptime data volume free-space floor reached": causes["local storage unavailable or full"],
                "guard clock moved to an earlier day": ("감시 날짜 역행", "현재 KST 날짜가 저장된 날짜보다 과거임"),
                "guard clock moved to an earlier month": ("감시 월 역행", "현재 KST 월이 저장된 월보다 과거임"),
            }
            if detail in errors:
                return errors[detail]
        if kind == "TimeoutError" or (kind == "URLError" and detail == "<urlopen error timed out>"):
            return "계측 응답 시간 초과", f"응답 제한시간 {seconds(limits.get('metrics_timeout_seconds'))} 내 요청 미완료"
        if kind == "HTTPError":
            status = re.match(r"HTTP Error (\d{3}):", detail)
            return "계측 HTTP 오류", f"계측 endpoint가 HTTP {status[1] if status else '오류'} 응답"
        if kind in ("URLError", "ConnectionError", "ConnectionRefusedError",
                    "ConnectionResetError", "ConnectionAbortedError", "BrokenPipeError"):
            return "계측 접속 실패", "계측 endpoint에 연결하지 못함"
    return "계측 또는 보호 상태 이상", "미분류 검증 실패; 호스트 state.json에서 원문 확인"


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
        if (blocked and state.get("reason") == "planned monitor stop" and stopped
                and not state.get("block_alert_attempt_level")):
            return
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
        cause, criterion = stop_condition(state)
        if state.get("metrics_failure_count"):
            criterion += (
                f"; 연속 실패 {state['metrics_failure_count']}/{MAX_METRICS_FAILURES}"
                f"; 마지막 유효 관측 후 {state['metrics_failure_age_seconds']:.3f}초"
                f" / 허용 {MAX_OBSERVATION_GAP}초")
        if blocked:
            action = f"보호 차단: {cause}\n중단 요건: {criterion}\n{action}"
        title = f"긴급 — {cause}" if blocked else ("긴급" if level == "hard" else "주의")
        text = (f"[Datalake {title}]\n"
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
    sub.add_parser("prepare-stop", help="systemd ExecStop: record this live invocation's planned stop")
    trip = sub.add_parser("trip")
    trip.add_argument("--grace", type=int, default=10)
    trip.add_argument("--systemd-stop", action="store_true",
                      help="systemd ExecStopPost: classify this invocation's shutdown")
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
    if args.command == "prepare-stop":
        invocation = os.environ.get("INVOCATION_ID")
        pid = os.environ.get("MAINPID", "")
        if invocation and pid.isdecimal() and int(pid) > 1:
            try:
                os.kill(int(pid), 0)
            except ProcessLookupError:
                return
            with locked(directory):
                atomic(directory / "planned-stop", invocation)
        return
    if args.command == "trip":
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", args.project):
            parser.error("invalid exact Compose project")
        try:
            with locked(directory):
                revoke(directory)
                planned = False
                if args.systemd_stop:
                    marker = directory / "planned-stop"
                    try:
                        planned = (bool(os.environ.get("INVOCATION_ID"))
                                   and marker.read_text() == os.environ["INVOCATION_ID"]
                                   and os.environ.get("SERVICE_RESULT") == "success")
                    except FileNotFoundError:
                        pass
                    finally:
                        marker.unlink(missing_ok=True)
                state = load(directory)
                if state["project"] != args.project:
                    raise ValueError("trip project differs from persistent state; selected project stopped")
                if args.systemd_stop:
                    state["monitor_exit"] = {"SERVICE_RESULT": os.environ.get("SERVICE_RESULT", "")}
                reason = state.get("reason") if state["blocked"] else None
                if reason == "planned monitor stop":
                    reason = None
                latch(directory, state, reason or (
                    "planned monitor stop" if planned else "explicit trip / monitor stopped"))
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
            clear_metrics_failures(state)
            for key in ("block_episode", "block_alerted", "block_alert_attempt_at", "block_alert_attempt_level",
                        "observation_gap_seconds", "monitor_exit"):
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
    monitor_limits = {"gap_seconds": MAX_OBSERVATION_GAP,
                      "metrics_timeout_seconds": args.timeout}
    # Separate process lock prevents two monitor instances without blocking trip.
    with (directory / "monitor.lock").open("a") as worker:
        fcntl.flock(worker, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            with locked(directory):
                state = load(directory)
                state["monitor_limits"] = monitor_limits
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
                    fresh = False
                    startup_pending = False
                    fetching_metrics = False
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
                        fetching_metrics = True
                        all_values = metrics(args.metrics_url, args.timeout, include_writes=True)
                        fetching_metrics = False
                        values = {k: v for k, v in all_values.items()
                                  if json.loads(k)["operation"] == "read"}
                        writes = {k: v for k, v in all_values.items()
                                  if json.loads(k)["operation"] == "write"}
                        if not values:
                            raise ValueError("native S3 read metrics missing")
                        gap = time.time() - state["sample_at"] if state.get("sample_at") and state.get("last") is not None else 0
                        try:
                            download_full = observe(state, values)
                        except ValueError:
                            # Preserve storage accounting when a read counter resets.
                            observe_storage(state, writes)
                            raise
                        if (not writes and state.get("last_write") is None
                                and (time.time() < state["startup_until"] or download_full)):
                            startup_pending = True
                            storage_full = False
                        else:
                            storage_full = observe_storage(state, writes)
                        fresh = not startup_pending
                        clear_metrics_failures(state)
                        if gap > monitor_limits["gap_seconds"]:
                            state["observation_gap_seconds"] = gap
                            raise ValueError("monitor observation gap")
                        if storage_full or download_full:
                            reason = ("storage hard limit reached" if storage_full else
                                      "monthly hard download limit reached"
                                      if state["month_used_bytes"] >= MONTH_BYTES
                                      else "daily hard download limit reached")
                            latch(directory, state, reason)
                        else:
                            save(directory, state)
                    except Exception as exc:
                        if not fetching_metrics:
                            clear_metrics_failures(state)
                        retry = (fetching_metrics and retry_metrics(state, exc)
                                 and (state.get("last_write") is not None
                                      or time.time() < state["startup_until"]))
                        if retry and not state["blocked"]:
                            save(directory, state)
                        elif state["blocked"] or state.get("last") is not None or time.time() >= state["startup_until"]:
                            latch(directory, state,
                                  f"native metrics unavailable, stale, or reset: {type(exc).__name__}: {exc}")
                    if not state["blocked"] and (fresh or startup_pending or (
                            state.get("last") is None and time.time() < state["startup_until"])):
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
