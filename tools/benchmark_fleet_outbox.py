#!/usr/bin/env python3
"""Fleet outbox baseline-vs-batched comparison runner (issue #2).

Loads the pristine baseline recorder from --baseline-root and the
working-tree recorder, feeds both the same fixed synthetic inputs through
file-backed SQLite WAL/FULL outboxes, and compares full durable content +
final upload output: every outbox/outbox_events column (event_time order),
full sorted seen_ids sets, and identical render_insert upload SQL drained
in the same batch order.

Benchmark shape: each repeat schedules >=20 distinct received V envelopes
at --rates received envelopes/s (0 = back-to-back, real pacing only, never
backdated t_recv); each envelope carries --sizes rows and commits on its
own (no cross-message accumulation). Reports achieved schedule rate,
backlog, rows/s, commits/1000 rows (COMMIT via set_trace_callback),
per-envelope receive-to-durable p50/p95/p99, CPU s, peak RSS KB (platform
correct), and raw capacity ORDER correctness.

Synthetic isolated temp DBs only; never production.

  python3 tools/benchmark_fleet_outbox.py \
      --baseline-root /tmp/datalake-issues-20261009-baseline \
      --sizes 10,50 --rates 50,500 --repeats 3 --units 20
"""

import argparse
import hashlib
import importlib.util
import json
import os
import statistics
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

META = {
    "target_vin": "V1", "vehicle_id": "c1", "vehicle_salt": "",
    "decode_epoch": "fleet-v1", "vss_version": "",
    "vehicle_firmware": "", "mapping_revision": "fleet-v1",
    "collector_version": "fleet-1", "collector_id": "c1",
    "config_version": "",
}


def load_recorder(root):
    path = os.path.join(root, "scripts", "ingest", "fleet_recorder.py")
    name = "fleet_baseline_" + hashlib.sha1(
        os.path.abspath(root).encode()).hexdigest()[:8]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load baseline recorder: %s" % path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def unit_payload(unit, size):
    # VehicleSpeed range is 0..350 km/h AFTER mph->km/h scaling: keep raw
    # mph small (1..50) so every row stores; raw values stay distinct per
    # unit so event_ids never collide across units.
    return json.dumps({
        "vin": "V1",
        "createdAt": "2026-09-26T13:%02d:%02dZ" % ((unit // 60) % 60, unit % 60),
        "data": [{"key": "VehicleSpeed",
                  "value": {"floatValue": float(1 + (unit + i) % 50)}}
                 for i in range(size)],
    }).encode()

def rss_kb():
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":  # macOS bytes vs Linux KB
        rss //= 1024
    return rss


def fetch_all(conn, table, columns, order):
    return conn.execute(
        "SELECT " + ",".join(columns) + " FROM " + table
        + " ORDER BY " + order).fetchall()


def snapshot(fr, db_path, size, rate, units):
    """Schedule `units` distinct received envelopes at `rate`/s (0 = paced
    only by real execution); each envelope commits on its own. now_ns is
    pinned so ingest_time/seen_at match across impls for exact comparison.
    Latency is real monotonic receive-to-commit ns per envelope."""
    from unittest.mock import patch
    conn = fr.open_outbox(db_path)
    stats = fr.default_stats()
    commits = [0]

    def tracer(stmt):
        if stmt.lstrip().upper().startswith("COMMIT"):
            commits[0] += 1

    lat = []
    conn.set_trace_callback(tracer)
    t0 = time.perf_counter_ns()
    c0 = time.process_time_ns()
    try:
        with patch.object(fr, "now_ns", return_value=1780427600123456789):
            for u in range(units):
                if rate:
                    target = t0 + int(u * 1_000_000_000 / rate)
                    now = time.perf_counter_ns()
                    if target > now:
                        time.sleep((target - now) / 1e9)
                t_recv = time.perf_counter_ns()
                stored = fr.process_frame(conn, fr.TOPIC_V,
                                          unit_payload(u, size),
                                          dict(META), stats,
                                          max_rows=10 * size * units)
                lat.append(time.perf_counter_ns() - t_recv)
                assert stored == size
    finally:
        conn.set_trace_callback(None)
    wall_s = (time.perf_counter_ns() - t0) / 1e9
    cpu_s = (time.process_time_ns() - c0) / 1e9
    outbox = fetch_all(conn, "outbox", fr.COLUMNS, "event_time, event_id")
    events = fetch_all(conn, "outbox_events", fr.EVENT_COLUMNS,
                       "event_time, event_id")
    seen = sorted(r[0] for r in conn.execute(
        "SELECT event_id FROM seen_ids ORDER BY event_id").fetchall())
    ordered = all(outbox[i][0] <= outbox[i + 1][0]
                  for i in range(len(outbox) - 1))
    upload_sql = [fr.render_insert("vehicle_signal",
                                   [dict(zip(fr.COLUMNS, r)) for r in outbox],
                                   columns=fr.COLUMNS)] if outbox else []
    if events:
        upload_sql.append(fr.render_insert(
            "vehicle_event",
            [dict(zip(fr.EVENT_COLUMNS, r)) for r in events],
            columns=fr.EVENT_COLUMNS))
    conn.close()
    digest = hashlib.sha256(repr((outbox, events, seen)).encode()).hexdigest()
    return {"rows": outbox, "events": events, "seen": seen,
            "upload_sql": upload_sql, "digest": digest,
            "lat": lat, "cpu_s": cpu_s, "wall_s": wall_s,
            "commits": commits[0], "ordered": ordered, "stats": stats}


def percentile(sorted_ns, pct):
    if not sorted_ns:
        return 0
    return sorted_ns[min(len(sorted_ns) - 1, int(pct / 100 * len(sorted_ns)))]


def main(argv=None):
    ap = argparse.ArgumentParser(description="fleet outbox baseline compare")
    ap.add_argument("--baseline-root", required=True)
    ap.add_argument("--sizes", default="10,50")
    ap.add_argument("--rates", default="50,500")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--units", type=int, default=20)
    args = ap.parse_args(argv)
    sizes = [int(s) for s in args.sizes.split(",") if s.strip()]
    rates = [float(r) for r in args.rates.split(",") if r.strip()]
    base = load_recorder(args.baseline_root)
    from scripts.ingest import fleet_recorder as new

    mismatch = 0
    for size in sizes:
        for rate in rates:
            rows = {}
            for label, fr in (("baseline", base), ("batched", new)):
                lat_all, cpu_all, rate_all, c1000 = [], [], [], []
                rss = 0
                for _ in range(args.repeats):
                    with tempfile.TemporaryDirectory() as d:
                        snap = snapshot(
                            fr, os.path.join(d, "outbox.sqlite"), size,
                            rate, args.units)
                    lat_all.extend(snap["lat"])
                    cpu_all.append(snap["cpu_s"])
                    n = len(snap["rows"]) + len(snap["events"])
                    rate_all.append(args.units / snap["wall_s"]
                                    if snap["wall_s"] else 0)
                    c1000.append(snap["commits"] * 1000 / max(n, 1))
                    rss = max(rss, rss_kb())
                    rows[label] = snap  # last repeat = content comparison
                lat_sorted = sorted(lat_all)
                print(json.dumps({
                    "impl": label, "envelope_rows": size, "repeats": args.repeats,
                    "units_per_repeat": args.units,
                    "scheduled_rate_per_s": rate,
                    "achieved_rate_per_s": round(statistics.mean(rate_all), 1),
                    "rows_per_sec": round(statistics.mean(rate_all) * size, 1),
                    "commits_per_1000_rows": round(statistics.mean(c1000), 3),
                    "durable_p50_ns": percentile(lat_sorted, 50),
                    "durable_p95_ns": percentile(lat_sorted, 95),
                    "durable_p99_ns": percentile(lat_sorted, 99),
                    "cpu_s_mean": round(statistics.mean(cpu_all), 4),
                    "backlog_rows": len(rows[label]["rows"]) + len(
                        rows[label]["events"]),
                    "peak_rss_kb": rss,
                    "content_digest": rows[label]["digest"][:16],
                    "raw_capacity_order_ok": rows[label]["ordered"],
                }, sort_keys=True))
            b, n = rows["baseline"], rows["batched"]
            equal = (b["rows"] == n["rows"] and b["events"] == n["events"]
                     and b["seen"] == n["seen"]
                     and b["upload_sql"] == n["upload_sql"])
            if not equal:
                mismatch += 1
            print(json.dumps({
                "comparison": "baseline-vs-batched", "envelope_rows": size,
                "scheduled_rate_per_s": rate,
                "units_per_repeat": args.units,
                "equal_full_content": equal,
                "equal_upload_output": b["upload_sql"] == n["upload_sql"],
                "baseline_seen": len(b["seen"]), "batched_seen": len(n["seen"]),
                "baseline_rows": len(b["rows"]), "batched_rows": len(n["rows"]),
            }, sort_keys=True))
    if mismatch:
        print("MISMATCH: %d (size,rate) combos differ" % mismatch,
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
