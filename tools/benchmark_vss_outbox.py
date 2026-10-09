#!/usr/bin/env python3
"""Executable VSS outbox batch benchmark (stdlib only).

Compares the untouched baseline (per-row commits) against the batched
store_updates path on the same deterministic fixture: same file-backed
WAL/FULL outbox, same rows, same upload rendering. Grid over snapshot
rows-per-unit and real received units/sec: each received unit is one
snapshot/update batch, paced with one real monotonic receive timestamp
per unit, so reported latency is actual receive-to-durable per unit
(never modeled per-row arrival). Content equality covers every
outbox/seen field plus the upload SQL, never counts alone.

  /tmp/datalake-issues-20261009-venv/bin/python tools/benchmark_vss_outbox.py \
      --baseline-root /tmp/datalake-issues-20261009-baseline \
      --sizes 200,1000 --rates 50,500 --repeats 3 --units 25

One JSON object per (mode, size, rate) cell on stdout. Baseline checkout
absent -> batched mode only (baseline cells skipped with a stderr note).
"""

import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from tests.test_vss import run_outbox_benchmark  # noqa: E402


def main(argv=None):
    import argparse
    import json
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline-root", default=os.environ.get(
        "VSS_BASELINE_ROOT", "/tmp/datalake-issues-20261009-baseline"))
    ap.add_argument("--sizes", default=os.environ.get(
        "VSS_BENCH_SIZES", "200,1000"))
    ap.add_argument("--rates", default=os.environ.get(
        "VSS_BENCH_RATES", "50,500"))
    ap.add_argument("--repeats", type=int, default=int(os.environ.get(
        "VSS_BENCH_REPEATS", "3")))
    ap.add_argument("--units", type=int, default=int(os.environ.get(
        "VSS_BENCH_UNITS", "25")))
    args = ap.parse_args(argv)
    sizes = tuple(int(s) for s in args.sizes.split(",") if s.strip())
    rates = tuple(float(s) for s in args.rates.split(",") if s.strip())
    rows = run_outbox_benchmark(sizes=sizes, rates=rates,
                                repeats=args.repeats,
                                baseline_root=args.baseline_root,
                                units_per_repeat=args.units)
    if rows and {r["mode"] for r in rows} == {"batched"}:
        print("benchmark_vss_outbox: baseline checkout absent;"
              " batched-only run", file=sys.stderr)
    for row in rows:
        print(json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    main()
