#!/usr/bin/env python3
"""Synthetic unlimited/finite TTL comparison runner (issue #7).

Isolated actual-Greptime runner. Production tables/data are never touched.

Fixture: one sha256-pinned VALUES blob per run, shared by both scenarios.
Each kind (traces/logs) mixes expired historic rows (older than now minus
finite-TTL minus a day) with retained recent rows (now minus two minutes),
same row shape, so post-compaction counts prove real expiry instead of
showing only precleanup numbers. Default anchors (no --greptime-binary clock
read needed by unit tests) are fixed; the runner always anchors to UTC now.

Expiration phase: after INSERT, each bench table is measured (COUNT(*)),
then ADMIN FLUSH_TABLE + ADMIN COMPACT_TABLE force the server to enforce
TTL, then COUNT(*) again. Both before (precleanup) and after counts plus
per-query result rows / server ms / client s / request bytes are reported.

Shared-runtime mode (default): bench tables inside one database of an
already-running Greptime. Store bytes are unavailable over the 1.2.1 SQL
dialect, so they stay null (never estimated).

Owned-store mode (--greptime-binary): one owned standalone per scenario
under --data-home, exact stopped data-home bytes, offline tar.gz snapshot
(bytes + sha256), and relaunch-from-snapshot restore proof. Owned processes
bind loopback only and never touch the shared runtime.

  shared:
    SYNTH_PASS=... /tmp/datalake-issues-20261009-venv/bin/python \\
      tools/benchmark_otel_retention.py \\
      --base-url http://127.0.0.1:56955 --db otel_ret7_bench \\
      --user issue_synthetic --password-env SYNTH_PASS \\
      --traces 2000 --logs 2000 --finite-ttl 7d
  owned:
    /tmp/datalake-issues-20261009-venv/bin/python \\
      tools/benchmark_otel_retention.py \\
      --greptime-binary /path/to/greptime --data-home /tmp/ret7-store \\
      --traces 2000 --logs 2000 --finite-ttl 7d

Count mismatches (unlimited != total, finite != expected recent, restore !=
retained) fail closed with a nonzero exit, not a silent matches:false.
Dependencies: stdlib only. Credentials via env, never argv.
"""

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request

TABLES = ("ret7_traces_unlimited", "ret7_traces_finite",
          "ret7_logs_unlimited", "ret7_logs_finite")
FIXTURE_BASE = dt.datetime(2026, 9, 21, 10, 0, 0)
FIXTURE_RECENT_OFFSET_DAYS = 18  # test-only default anchor, never the runner
RECENT_SKEW_S = 120  # recent rows sit this far behind now: safely retained
HISTORIC_MARGIN_S = 86400  # historic rows sit TTL + this far back: safely expired
TABLE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
TTL_RE = re.compile(r"^[0-9]+[smhd]$")
TTL_UNIT_S = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def ttl_seconds(ttl):
    if not TTL_RE.match(ttl or ""):
        raise ValueError("TTL must look like 7d/12h (got %r)" % (ttl,))
    return int(ttl[:-1]) * TTL_UNIT_S[ttl[-1]]


def fixture_split(n):
    hist = n // 2
    return hist, n - hist


def build_fixture(n_traces, n_logs, now=None, ttl_s=7 * 86400):
    if now is None:
        hist_anchor = FIXTURE_BASE
        rec_anchor = FIXTURE_BASE + dt.timedelta(
            days=FIXTURE_RECENT_OFFSET_DAYS)
    else:
        hist_anchor = now - dt.timedelta(seconds=ttl_s + HISTORIC_MARGIN_S)
        rec_anchor = now - dt.timedelta(seconds=RECENT_SKEW_S)
    th, tr = fixture_split(n_traces)
    lh, lr = fixture_split(n_logs)
    trace_hist = ["('%s', 't%d', 's%d')" % (
        (hist_anchor + dt.timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S"),
        i, i) for i in range(th)]
    trace_rec = ["('%s', 't%d', 's%d')" % (
        (rec_anchor + dt.timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S"),
        th + i, th + i) for i in range(tr)]
    log_hist = ["('%s', 'scope', 'INFO')" % (
        (hist_anchor + dt.timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S"),)
        for i in range(lh)]
    log_rec = ["('%s', 'scope', 'INFO')" % (
        (rec_anchor + dt.timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S"),)
        for i in range(lr)]
    trace_vals = ", ".join(trace_hist + trace_rec)
    log_vals = ", ".join(log_hist + log_rec)
    digest = hashlib.sha256(
        (trace_vals + "\n" + log_vals).encode("utf-8")).hexdigest()
    return trace_vals, log_vals, digest


def bench_ddl(table, kind, ttl):
    if kind == "traces":
        return ("CREATE TABLE IF NOT EXISTS \"%s\" (\"timestamp\" TIMESTAMP(9)"
                " NOT NULL TIME INDEX, \"trace_id\" STRING NULL,"
                " \"span_id\" STRING NULL) WITH (append_mode = 'true', ttl = '%s')"
                % (table, ttl))
    return ("CREATE TABLE IF NOT EXISTS \"%s\" (\"timestamp\" TIMESTAMP(9)"
            " NOT NULL TIME INDEX, \"severity_text\" STRING NULL,"
            " \"scope_name\" STRING NULL) WITH (append_mode = 'true', ttl = '%s')"
            % (table, ttl))


def sql(base_url, auth, db, stmt, timeout=60):
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db, safe="")
    body = urllib.parse.urlencode({"sql": stmt}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    if auth:
        req.add_header("Authorization", "Basic " + auth)
    req.add_header("X-Greptime-Timezone", "UTC")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8", "replace"))
    if payload.get("error"):
        raise RuntimeError(str(payload["error"])[:300])
    return payload, len(body)


def rows_of(payload):
    rec = payload["output"][0].get("records") or {}
    return rec.get("rows", [])


def server_ms_of(payload):
    try:
        return payload.get("execution_time_ms")
    except AttributeError:
        return None


def query_stats(base_url, auth, db, stmt):
    """Measured query facts only: result rows, server ms, client s, req bytes."""
    start = time.monotonic()
    payload, size = sql(base_url, auth, db, stmt)
    elapsed = time.monotonic() - start
    return {"result_rows": len(rows_of(payload)),
            "server_ms": server_ms_of(payload),
            "client_s": round(elapsed, 4), "request_bytes": size}


def count_value(base_url, auth, db, stmt):
    """One COUNT query: preserved value plus its measured query stats."""
    start = time.monotonic()
    payload, size = sql(base_url, auth, db, stmt)
    elapsed = time.monotonic() - start
    rows = rows_of(payload)
    return (rows[0][0] if rows else 0,
            {"result_rows": len(rows), "server_ms": server_ms_of(payload),
             "client_s": round(elapsed, 4), "request_bytes": size})


def expire_table(base_url, auth, db, table, req_log):
    """Precleanup COUNT, ADMIN flush+compact (forces TTL), post COUNT."""
    if not TABLE_RE.match(table):
        raise RuntimeError("refusing ADMIN on unexpected table %r" % (table,))
    pre, pre_q = count_value(base_url, auth, db,
                             "SELECT COUNT(*) FROM \"%s\"" % table)
    req_log.append(pre_q["request_bytes"])
    ops = {}
    for op in ("FLUSH_TABLE", "COMPACT_TABLE"):
        start = time.monotonic()
        payload, size = sql(base_url, auth, db,
                            "ADMIN %s('%s')" % (op, table))
        elapsed = time.monotonic() - start
        req_log.append(size)
        ops[op.lower()] = {"server_ms": server_ms_of(payload),
                           "client_s": round(elapsed, 4),
                           "request_bytes": size}
    post, post_q = count_value(base_url, auth, db,
                               "SELECT COUNT(*) FROM \"%s\"" % table)
    req_log.append(post_q["request_bytes"])
    return {"table": table, "pre_flush_rows": pre, "pre_flush_query": pre_q,
            "flush": ops["flush_table"], "compact": ops["compact_table"],
            "preserved_rows": post, "post_flush_query": post_q}


def dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path, followlinks=False):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total


def wait_sql_ready(base_url, db, timeout_s):
    deadline = time.monotonic() + timeout_s
    last = "no attempt"
    while time.monotonic() < deadline:
        try:
            sql(base_url, "", "public", "SELECT 1", timeout=10)
            return True
        except Exception as e:  # noqa: BLE001 -- readiness polling only
            last = str(e)[:120]
            time.sleep(1.0)
    raise RuntimeError("owned greptime not SQL-ready in %ds: %s"
                       % (timeout_s, last))


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def discover_flags(binary):
    try:
        proc = subprocess.run(
            [binary, "standalone", "start", "--help"],
            capture_output=True, text=True, timeout=30)
        text = (proc.stdout or "") + (proc.stderr or "")
    except Exception:
        return set()
    return set(re.findall(r"--([a-z0-9][a-z0-9-]*)", text))


def owned_argv(binary, flags, home, port, extra_ports):
    argv = [binary, "standalone", "start"]
    data_flag = next((f for f in ("data-home", "data-dir") if f in flags),
                     None)
    http_flag = next((f for f in ("http-addr", "http-bind-addr",
                                  "http-listen-addr") if f in flags), None)
    if data_flag is None or http_flag is None:
        raise RuntimeError("owned mode needs --data-home/--http-addr style "
                           "flags; found: " + ",".join(sorted(flags)))
    argv += ["--" + data_flag, home, "--" + http_flag,
             "127.0.0.1:%d" % port]
    for flag, value in extra_ports:
        if flag in flags:
            argv += ["--" + flag, value]
    return argv


def extra_listener_ports():
    return (("grpc-bind-addr", "127.0.0.1:%d" % free_port()),
            ("mysql-addr", "127.0.0.1:%d" % free_port()),
            ("postgres-addr", "127.0.0.1:%d" % free_port()))


def stop_proc(proc, timeout_s=60):
    try:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=timeout_s)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)


def snapshot_home(home, dest_path):
    with tarfile.open(dest_path, "w:gz") as tar:
        tar.add(home, arcname=os.path.basename(home.rstrip("/")))
    digest = hashlib.sha256()
    with open(dest_path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"bytes": os.path.getsize(dest_path),
            "sha256": digest.hexdigest(), "path": dest_path}


def extract_snapshot(snap_path, fresh_dir):
    with tarfile.open(snap_path, "r:gz") as tar:
        try:
            tar.extractall(fresh_dir, filter="data")
        except TypeError:  # pre-3.12 tarfile without filter= support
            tar.extractall(fresh_dir)


def fresh_dir(path, reuse):
    """Fail closed on an existing store path; never clear a user path."""
    if os.path.exists(path) and not reuse:
        raise RuntimeError(
            "refusing to use existing %s without --reuse-data-home "
            "(stale rows would corrupt the expiration proof)" % path)


def load_scenario(base_url, auth, db, ttl, trace_vals, log_vals, req_log,
                  tables):
    def run(stmt):
        payload, size = sql(base_url, auth, db, stmt)
        req_log.append(size)
        return payload

    _, size = sql(base_url, auth, "public",
                  "CREATE DATABASE IF NOT EXISTS \"%s\"" % db)
    req_log.append(size)
    for table, kind in tables:
        run(bench_ddl(table, kind, ttl))
    ttable = tables[0][0]
    ltable = tables[1][0]
    run("INSERT INTO \"%s\" (\"timestamp\", \"trace_id\", \"span_id\")"
        " VALUES %s" % (ttable, trace_vals))
    run("INSERT INTO \"%s\" (\"timestamp\", \"severity_text\","
        " \"scope_name\") VALUES %s" % (ltable, log_vals))
    out = {}
    for table, kind in tables:
        out[kind] = expire_table(base_url, auth, db, table, req_log)
    out["retained_rows"] = sum(out[k]["preserved_rows"] for k in ("traces", "logs"))
    out["precleanup_rows"] = sum(out[k]["pre_flush_rows"] for k in ("traces", "logs"))
    return out


def check_counts(name, scenario, total, expected):
    retained = scenario["retained_rows"]
    if retained != expected:
        raise RuntimeError(
            "%s expiration proof failed: retained %d != expected %d "
            "(precleanup %d of %d input rows; tables=%s)"
            % (name, retained, expected, scenario["precleanup_rows"], total,
               [(k, scenario[k]["pre_flush_rows"],
                 scenario[k]["preserved_rows"])
                for k in ("traces", "logs")]))


def run_owned(args, ttl_s, trace_vals, log_vals, fixture_sha, fixture_meta):
    flags = discover_flags(args.greptime_binary)
    created_temp = not args.data_home
    root = args.data_home or tempfile.mkdtemp(prefix="ret7-store-")
    os.makedirs(root, exist_ok=True)
    total = args.traces + args.logs
    th, tr = fixture_split(args.traces)
    lh, lr = fixture_split(args.logs)
    recent_total = tr + lr
    expected_finite = total if ttl_s == 0 else recent_total
    report = {"mode": "owned-store", "binary": args.greptime_binary,
              "fixture": dict(fixture_meta, traces=args.traces,
                              logs=args.logs, fixture_sha256=fixture_sha,
                              historic_rows=th + lh,
                              recent_rows=recent_total),
              "scenarios": {}}
    scenarios = {"unlimited": "0s", "finite": args.finite_ttl}
    try:
        for name, ttl in scenarios.items():
            home = os.path.join(root, "ret7_" + name)
            fresh_dir(home, args.reuse_data_home)
            os.makedirs(home, exist_ok=True)
            port = free_port()
            proc = subprocess.Popen(
                owned_argv(args.greptime_binary, flags, home, port,
                           extra_listener_ports()),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            scenario = {"ttl": ttl, "http_port": port, "data_home": home}
            req_log = []
            try:
                base_url = "http://127.0.0.1:%d" % port
                wait_sql_ready(base_url, args.db, args.startup_timeout)
                scenario.update(load_scenario(
                    base_url, "", args.db, ttl, trace_vals, log_vals,
                    req_log, [("ret7_traces", "traces"), ("ret7_logs", "logs")]))
            finally:
                stop_proc(proc, timeout_s=args.shutdown_timeout)
                scenario["exit_code"] = proc.poll()
            check_counts("owned " + name, scenario, total,
                         total if name == "unlimited" else expected_finite)
            scenario["store_bytes"] = dir_bytes(home)
            snap_path = os.path.join(root, "ret7_%s.tar.gz" % name)
            fresh_dir(snap_path, args.reuse_data_home)
            scenario["offline_snapshot"] = snapshot_home(home, snap_path)
            scenario["request_bytes"] = sum(req_log)
            if not args.no_restore_proof:
                fresh = os.path.join(root, "ret7_%s_restore" % name)
                fresh_dir(fresh, args.reuse_data_home)
                os.makedirs(fresh, exist_ok=True)
                extract_snapshot(snap_path, fresh)
                relaunched = os.path.join(
                    fresh, os.path.basename(home.rstrip("/")))
                port2 = free_port()
                proc2 = subprocess.Popen(
                    owned_argv(args.greptime_binary, flags, relaunched,
                               port2, extra_listener_ports()),
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    base2 = "http://127.0.0.1:%d" % port2
                    wait_sql_ready(base2, args.db, args.startup_timeout)
                    traces, _ = count_value(
                        base2, "", args.db,
                        "SELECT COUNT(*) FROM \"ret7_traces\"")
                    logs, _ = count_value(
                        base2, "", args.db,
                        "SELECT COUNT(*) FROM \"ret7_logs\"")
                    restored = traces + logs
                    scenario["restore_proof"] = {
                        "restored_rows": restored,
                        "matches": restored == scenario["retained_rows"]}
                    if restored != scenario["retained_rows"]:
                        raise RuntimeError(
                            "owned %s restore proof failed: restored %d != "
                            "retained %d" % (name, restored,
                                             scenario["retained_rows"]))
                finally:
                    stop_proc(proc2, timeout_s=args.shutdown_timeout)
            report["scenarios"][name] = scenario
        report["note"] = ("same pinned fixture per scenario (historic rows "
                          "older than now-TTL expire, recent rows stay); "
                          "pre_flush_rows is the precleanup read, "
                          "preserved_rows the post-flush+compact proof; "
                          "store bytes are exact stopped data-home bytes")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        if created_temp and not args.keep_data_home:
            shutil.rmtree(root, ignore_errors=True)


def run_shared(args, auth, ttl_s, trace_vals, log_vals, fixture_sha,
               fixture_meta):
    req_bytes = 0

    def run(stmt):
        nonlocal req_bytes
        payload, size = sql(args.base_url, auth, args.db, stmt)
        req_bytes += size
        return payload

    total = args.traces + args.logs
    th, tr = fixture_split(args.traces)
    lh, lr = fixture_split(args.logs)
    recent_total = tr + lr
    expected_finite = total if ttl_s == 0 else recent_total
    scenarios = {"unlimited": "0s", "finite": args.finite_ttl}
    report = {"mode": "shared-runtime",
              "fixture": dict(fixture_meta, traces=args.traces,
                              logs=args.logs, fixture_sha256=fixture_sha,
                              historic_rows=th + lh,
                              recent_rows=recent_total),
              "scenarios": {}}
    try:
        run("CREATE DATABASE IF NOT EXISTS \"%s\"" % args.db)
        for name, ttl in scenarios.items():
            tables = [("ret7_traces_%s" % name, "traces"),
                      ("ret7_logs_%s" % name, "logs")]
            for table, kind in tables:
                run(bench_ddl(table, kind, ttl))
            run("INSERT INTO \"ret7_traces_%s\" (\"timestamp\", \"trace_id\","
                " \"span_id\") VALUES %s" % (name, trace_vals))
            run("INSERT INTO \"ret7_logs_%s\" (\"timestamp\","
                " \"severity_text\", \"scope_name\") VALUES %s"
                % (name, log_vals))
            entry = {"ttl": ttl}
            for table, kind in tables:
                table_req = []
                stats = expire_table(args.base_url, auth, args.db, table,
                                     table_req)
                req_bytes += sum(table_req)
                entry[kind] = stats
            entry["retained_rows"] = sum(entry[k]["preserved_rows"]
                                        for k in ("traces", "logs"))
            entry["precleanup_rows"] = sum(entry[k]["pre_flush_rows"]
                                           for k in ("traces", "logs"))
            check_counts("shared " + name, entry, total,
                         total if name == "unlimited" else expected_finite)
            # No per-table byte counter in the 1.2.1 SQL dialect: null,
            # never estimated. Owned --greptime-binary mode measures it.
            entry["store_bytes"] = None
            entry["store_bytes_note"] = (
                "unavailable over shared SQL; rerun owned mode for "
                "physical bytes")
            entry["offline_snapshot"] = None
            report["scenarios"][name] = entry
        report["request_bytes"] = req_bytes
        report["note"] = ("same pinned fixture per scenario; pre_flush_rows "
                          "is the precleanup read, preserved_rows the "
                          "post-flush+compact proof")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        if not args.no_cleanup:
            for table in TABLES:
                try:
                    sql(args.base_url, auth, args.db,
                        "DROP TABLE IF EXISTS \"%s\"" % table)
                except Exception:
                    pass


def main():
    ap = argparse.ArgumentParser(description="synthetic TTL comparison")
    ap.add_argument("--base-url", default="http://127.0.0.1:4000")
    ap.add_argument("--db", default="otel_ret7_bench")
    ap.add_argument("--user", default="")
    ap.add_argument("--password-env", default="GREPTIME_PASSWORD")
    ap.add_argument("--traces", type=int, default=2000)
    ap.add_argument("--logs", type=int, default=2000)
    ap.add_argument("--finite-ttl", default="7d")
    ap.add_argument("--no-cleanup", action="store_true")
    ap.add_argument("--greptime-binary", default="",
                    help="owned-store mode: path to a greptime binary; "
                    "launches one owned standalone per scenario")
    ap.add_argument("--data-home", default="",
                    help="owned-store mode: parent dir for scenario stores "
                    "(default: fresh tempdir, removed unless --keep)")
    ap.add_argument("--startup-timeout", type=int, default=120)
    ap.add_argument("--shutdown-timeout", type=int, default=60)
    ap.add_argument("--reuse-data-home", action="store_true")
    ap.add_argument("--keep-data-home", action="store_true")
    ap.add_argument("--no-restore-proof", action="store_true")
    args = ap.parse_args()
    try:
        ttl_s = ttl_seconds(args.finite_ttl)
    except ValueError as e:
        print("benchmark: error: --finite-ttl: " + str(e), file=sys.stderr)
        return 2
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    trace_vals, log_vals, fixture_sha = build_fixture(
        args.traces, args.logs, now=now, ttl_s=ttl_s)
    fixture_meta = {
        "historic_anchor": (now - dt.timedelta(
            seconds=ttl_s + HISTORIC_MARGIN_S)).strftime("%Y-%m-%d %H:%M:%S"),
        "recent_anchor": (now - dt.timedelta(
            seconds=RECENT_SKEW_S)).strftime("%Y-%m-%d %H:%M:%S"),
        "finite_ttl_s": ttl_s}
    try:
        if args.greptime_binary:
            return run_owned(args, ttl_s, trace_vals, log_vals, fixture_sha,
                             fixture_meta)
    except RuntimeError as e:
        print("benchmark: error: " + str(e)[:300], file=sys.stderr)
        return 2
    password = os.environ.get(args.password_env, "")
    if not password:
        print("benchmark: error: %s is empty" % args.password_env,
              file=sys.stderr)
        return 1
    if not args.user:
        print("benchmark: error: --user is required in shared mode",
              file=sys.stderr)
        return 1
    auth = base64.b64encode(
        (args.user + ":" + password).encode("utf-8")).decode("ascii")
    try:
        return run_shared(args, auth, ttl_s, trace_vals, log_vals,
                          fixture_sha, fixture_meta)
    except RuntimeError as e:
        print("benchmark: error: " + str(e)[:300], file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
