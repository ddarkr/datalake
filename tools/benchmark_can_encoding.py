#!/usr/bin/env python3
"""Synthetic CAN flush-serialization comparison: baseline vs current tree.

Seeds the same deterministic synthetic outbox rows (small/large text
shapes) into a temp file-backed Archive in each tree, then times the
ACTUAL Archive.flush_once selection/body-prep path (SELECT in id order,
json.loads, per-cell SQL render, per-cell URL-encoding budget fit, final
body assembly) with a deterministic in-process ACK sink. Reports
CPU/wall/peak RSS with alternating run order. Verifies the captured
signal request bytes are identical (SHA256) per case; reports only
measured values, never fabricated ones.

One runnable command, e.g.::

    python tools/benchmark_can_encoding.py \\
        --baseline-root /tmp/datalake-issues-baseline \\
        --rows 1,2000,20000 --repeats 3 --json-out /tmp/can-encoding.json

Synthetic fixture only; not production measurements.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

CURRENT_ROOT = Path(__file__).resolve().parents[1]

SHAPES = ("small", "large")

_CHILD = r"""
import hashlib, json, resource, sys, tempfile, time, urllib.parse
from pathlib import Path
side_root, shape, n_rows = sys.argv[1], sys.argv[2], int(sys.argv[3])
sys.path.insert(0, side_root)
from scripts.ingest.can import can_receiver as rec

text = ("x" * 8) if shape == "small" else ("출력-값 %&=+'" * 64)
rows = []
for i in range(n_rows):
    row = {c: None for c in rec.COLUMNS}
    row.update({
        "event_time": 1800000000000000000 + i,
        "vehicle": "synthetic",
        "path": "Vehicle.CAN.x123.Power",
        "source": "can",
        "event_id": "bench-%d" % i,
        "decode_epoch": "synthetic-v1",
        "ingest_time": 1800000000000000001,
        "collector_id": "fixture",
        "quality": "reported_unverified",
        "mapping_revision": "synthetic-v1",
        "vehicle_firmware": "synthetic",
        "value_text": "%s-%d" % (text, i),
    })
    rows.append(row)

captured = {}

class FakeGreptime:
    # Deterministic in-process ACK sink: full signal ACK, then dirty ACK.
    # Large enough that every case (incl. 20000 large rows) fits one
    # prefix: timing covers the full selection/body-prep path on both trees.
    max_body_bytes = 512 * 1024 * 1024
    calls = 0

    def send_body(self, payload):
        self.calls += 1
        if self.calls == 1:
            captured["signal"] = bytes(payload)
            return len(rows)
        captured["dirty"] = bytes(payload)
        sql = urllib.parse.parse_qs(payload.decode("ascii"))["sql"][0]
        return sql.count("),(") + 1

with tempfile.TemporaryDirectory(prefix="can-enc-seed-") as work:
    archive = rec.Archive(str(Path(work) / "bench.sqlite"), disk_reserve_bytes=0)
    with archive.connect() as conn:
        conn.execute("INSERT INTO epochs VALUES(?,?)", ("synthetic-v1", "synthetic-v1"))
        conn.executemany(
            "INSERT INTO outbox(event_id,epoch,row_json) VALUES(?,?,?)",
            [(r["event_id"], "synthetic-v1", json.dumps(r)) for r in rows])
        conn.commit()
    fake = FakeGreptime()
    t0, c0 = time.monotonic(), time.process_time()
    n = archive.flush_once(fake)
    wall, cpu = time.monotonic() - t0, time.process_time() - c0
    assert n == len(rows), (n, len(rows))
    assert fake.calls == 2, fake.calls
    signal = captured["signal"]
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
if sys.platform != "darwin":
    peak *= 1024
print(json.dumps({"wall_s": wall, "cpu_s": cpu, "peak_rss_bytes": peak,
                  "body_bytes": len(signal),
                  "body_sha256": hashlib.sha256(signal).hexdigest()}))
"""


def _run_side(side_root, shape, rows):
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, str(side_root), shape, str(rows)],
        capture_output=True, text=True, cwd=str(side_root),
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
             "PYTHONPATH": str(side_root), "PYTHONNOUSERSITE": "1"},
        timeout=1200)
    if proc.returncode != 0:
        raise RuntimeError("%s %s/%d failed: %s" % (
            side_root, shape, rows, proc.stderr[-3000:]))
    return json.loads(proc.stdout)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", required=True)
    parser.add_argument("--rows", default="1,2000,20000")
    parser.add_argument("--shapes", default=",".join(SHAPES))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args(argv)
    shapes = [s.strip() for s in args.shapes.split(",") if s.strip()]
    row_counts = [int(r) for r in args.rows.split(",") if r.strip()]
    if not shapes or not row_counts or args.repeats < 1:
        parser.error("--shapes/--rows must be non-empty; --repeats positive")
    unknown = [s for s in shapes if s not in SHAPES]
    if unknown:
        raise SystemExit("unknown shapes: %s" % ",".join(unknown))
    report = {"synthetic": True,
              "note": "synthetic fixture only; not production measurements",
              "cases": {}, "mismatch": []}
    for shape in shapes:
        for n in row_counts:
            key = "%s/%d" % (shape, n)
            base_runs, cur_runs = [], []
            for rep in range(args.repeats):
                order = (CURRENT_ROOT, args.baseline_root) if rep % 2 else (
                    args.baseline_root, CURRENT_ROOT)
                outs = {}
                for side in order:
                    outs[str(side)] = _run_side(side, shape, n)
                base_runs.append(outs[str(args.baseline_root)])
                cur_runs.append(outs[str(CURRENT_ROOT)])
            if {r["body_sha256"] for r in base_runs + cur_runs} != {base_runs[0]["body_sha256"]}:
                report["mismatch"].append({"case": key})
            report["cases"][key] = {
                "equal": not any(m["case"] == key for m in report["mismatch"]),
                "body_bytes": base_runs[0]["body_bytes"],
                "body_sha256": base_runs[0]["body_sha256"],
                "baseline": {"wall_s": [r["wall_s"] for r in base_runs],
                             "cpu_s": [r["cpu_s"] for r in base_runs],
                             "peak_rss_bytes": [r["peak_rss_bytes"] for r in base_runs]},
                "current": {"wall_s": [r["wall_s"] for r in cur_runs],
                            "cpu_s": [r["cpu_s"] for r in cur_runs],
                            "peak_rss_bytes": [r["peak_rss_bytes"] for r in cur_runs]},
            }
    payload = json.dumps(report, indent=2, sort_keys=True)
    print(payload)
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(payload + "\n", encoding="utf-8")
    return 1 if report["mismatch"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
