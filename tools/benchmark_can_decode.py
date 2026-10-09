#!/usr/bin/env python3
"""Synthetic CAN decode throughput benchmark: baseline vs current tree.

End-to-end Archive.accept -> decode_once drain using the real Decoder in
each tree (no mocks). Each side runs in a separate subprocess loading its
own root (--baseline-root vs this checkout), seeded with an identical
deterministic synthetic fixture; only the unprofiled decode drain is timed
and time.time_ns is frozen during decoding so ingest timestamps match
exactly.

Cases: tiny (1-5 byte fragments),
single (one frame/chunk), sparse (control/unknown heavy, low signal
yield), interleaved (4 sessions, split frames, round-robin arrival),
dense (3000-frame chunks forcing decode_some partial resume), large
(~59KB chunks near the decoder's 65536-byte datum cap; a single datum
above that is rejected by contract so >128KiB arrives as chunk streams,
not one datum), many_sessions (sessions x 5 chunks, candidate-query
scaling), xsession_contig/roundrobin/uneven (1/4/100/400-session
long-backlog matrix derived from --chunks: contiguous, round-robin, and
skewed-depth arrival; the round-robin corner reproduces the #8
1000-read/16-commit shape). An un-timed second pass per side wraps the
DB-API connection (seed excluded): every sqlite3.Row pulled from a
raw_chunks SELECT counts as one observed materialized fetch with its data
bytes, identically for both trees (no _decode_prefetch_stats trust, no
staged-as-fetched fallback); other SELECTs count as read queries and
commit() calls as commits. The report compares observed
fetched/staged amplification, read queries, fetched bytes and commits
alongside wall/CPU time. Fails on any outbox / decode_states /
decode_partial / counter content mismatch (full compare, never
length-only), or when a case's current median wall time exceeds its
baseline median by more than --max-slowdown (default 0.25). Repeats
alternate baseline-first / current-first order to reduce runner drift.
--json-out writes the same report to a file even on failure for artifact
publishing.

One runnable command, e.g.::

    python tools/benchmark_can_decode.py --baseline-root /path/to/baseline \\
        --chunks 12000 --repeats 1

CI regression gate, e.g.::

    python tools/benchmark_can_decode.py --baseline-root /tmp/base \\
        --chunks 2000 --repeats 3 --no-instrument \\
        --max-slowdown 0.25 --json-out /tmp/can-decode-perf.json

All identities/frames are synthetic; measurements are synthetic, not
production. Temporary archives are removed on exit.
"""
import argparse
import hashlib
import json
import math
import subprocess
import statistics
import sys
import tempfile
from pathlib import Path

CURRENT_ROOT = Path(__file__).resolve().parents[1]


CASES = ("tiny", "single", "sparse", "interleaved", "dense", "large",
         "many_sessions", "xsession_contig", "xsession_roundrobin",
         "xsession_uneven")

_CHILD = r"""
import json, sqlite3, sys, tempfile, time
side_root, workdir, case, n_chunks, fixed_ns, instrument = (
    sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]),
    int(sys.argv[5]), int(sys.argv[6]))
sys.path.insert(0, side_root)
from pathlib import Path
work = Path(workdir)
work.mkdir(parents=True, exist_ok=True)

DBC_TEXT = ('VERSION "synthetic"\n\nNS_ :\n\nBS_:\n\nBU_: Fixture\n\n'
            'BO_ 291 Sample: 2 Fixture\n'
            ' SG_ Power : 0|16@1- (0.5,0) [-16384|16383.5] "kW" Fixture\n')
DEFS = {"revision": "synthetic-bench", "signals": [{
    "source": "synthetic", "id": "0x123", "signal": "Power",
    "source_signal": "Power", "kind": "data", "start_bit": 0,
    "bit_length": 16, "byte_order": "little_endian", "signed": True,
    "scale": 0.5, "offset": 0, "is_multiplexer": False,
    "multiplexer_signal": None, "multiplexer_ids": None,
    "actual_dbc_length": 2, "unit": "kW", "source_unit": "kW",
    "choices": {}, "evidence": "synthetic@0123456789abcdef"}]}
(work / "bench.dbc").write_text(DBC_TEXT, encoding="utf-8")
(work / "bench.json").write_text(json.dumps(DEFS), encoding="utf-8")

from scripts.ingest.can.can_receiver import Archive
from scripts.ingest.can.can_decoder import Decoder

GOOD = b"t1232FFFF\r"     # valid single-signal frame (10 bytes)
UNKNOWN = b"t7FE20100\r"  # well-formed, no DBC message
CTRL = [b"z\r", b"S6\r", b"\r"]
STARTED = 1800000000000000000

def meta(session):
    return {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
            "session_id": session, "started_ns": STARTED, "vehicle_firmware": "synthetic"}

def chunk(seq, offset, data):
    return dict(seq=seq, offset_ns=offset, phase="capture", data=data)

def _xshape(default_sessions):
    # Cross-session backlog matrix for 1/4/100/400 sessions: derive the
    # session count from --chunks so small CI runs stay small and large runs
    # reproduce the deep-backlog shape. Thresholds chosen so --chunks 2000
    # (CI) yields the default session count per case, --chunks 12000+ scales
    # to the 400-session corner.
    total = max(default_sessions, n_chunks // 5)
    if total >= 2000:
        sessions = 400
    elif total >= 500:
        sessions = 100
    elif total >= 20:
        sessions = 4
    else:
        sessions = 1
    return sessions, max(1, total // sessions)

def plan():
    # Accept batches in arrival order: [(session, [chunk, ...])].
    if case == "tiny":
        # Small serial fragments, offset_ns=i.
        pieces = (b"t", b"123", b"2", b"0200", b"\r")
        return [("tiny", [chunk(i, i, pieces[i % len(pieces)])
                          for i in range(n_chunks)])]
    if case == "single":
        return [("syn-single", [chunk(s, s * 1000, GOOD)
                                for s in range(n_chunks)])]
    if case == "sparse":
        records = [CTRL[0], CTRL[1], UNKNOWN, CTRL[2], b"not-a-frame\r", GOOD]
        return [("syn-sparse", [chunk(
            s, s * 1000, records[s % len(records)] + records[(s + 3) % len(records)])
            for s in range(n_chunks)])]
    if case == "interleaved":
        sessions, per = 4, max(1, n_chunks // 4)
        cut = [5, 8, 3, 11]
        order = []
        for s in range(per):
            for k in range(sessions):
                stream = (GOOD * 3 + UNKNOWN + GOOD * 2) * ((s // 8) + 1)
                pos = (s * cut[k]) % len(stream)
                order.append(("syn-int-%d" % k, [chunk(s, s * 1000 + k,
                    stream[pos:pos + cut[k]] or GOOD[:cut[k]])]))
        return order
    if case == "dense":
        # 3000 rows/chunk exceeds the 2000-row budget: partial resume path.
        per_chunk = min(n_chunks, 16)
        return [("syn-dense", [chunk(s, s * 1000, GOOD * 3000)
                               for s in range(per_chunk)])]
    if case == "large":
        # Decoder rejects a single datum >65536 bytes, so the memory case is
        # near-cap ~59KB chunks (5900 frames) streamed: 3+ chunks >128KiB total.
        per_chunk = min(max(3, n_chunks // 5000), 8)
        return [("syn-large", [chunk(s, s * 1000, GOOD * 5900)
                               for s in range(per_chunk)])]
    if case == "many_sessions":
        sessions = max(1, n_chunks // 5)
        order = []
        for s in range(5):
            for k in range(sessions):
                order.append(("syn-many-%d" % k, [chunk(s, s * 1000 + k, GOOD)]))
        return order
    if case == "xsession_contig":
        # N-session long-backlog matrix, contiguous per-session arrival:
        # sessions derived from n_chunks so --chunks scales total work.
        sessions, per = _xshape(1)
        return [("syn-xs-%d" % k, [chunk(s, s * 1000 + k, GOOD)
                                   for s in range(per)]) for k in range(sessions)]
    if case == "xsession_roundrobin":
        # Same totals, seq-outer round-robin arrival: 100x64-style interleave
        # that triggered the #8 1000-read/16-commit waste.
        sessions, per = _xshape(100)
        order = []
        for s in range(per):
            for k in range(sessions):
                order.append(("syn-xs-%d" % k, [chunk(s, s * 1000 + k, GOOD)]))
        return order
    if case == "xsession_uneven":
        # Same totals, skewed per-session depths (half get 1 chunk, rest share
        # the remainder) in round-robin arrival order.
        sessions, per = _xshape(4)
        depths = [1 if k < sessions // 2 else 1 + (per * sessions - sessions // 2) // ((sessions + 1) // 2)
                  for k in range(sessions)]
        depths[-1] += per * sessions - sum(depths)
        order = []
        for s in range(max(depths)):
            for k in range(sessions):
                if s < depths[k]:
                    order.append(("syn-xs-%d" % k, [chunk(s, s * 1000 + k, GOOD)]))
        return order
    raise ValueError("unknown case")

def seed(db):
    archive = Archive(db, disk_reserve_bytes=0)
    decoder = Decoder(str(work / "bench.dbc"), str(work / "bench.json"))
    archive.register_epoch(decoder)
    batches = plan()
    for session, chunks in batches:
        archive.accept(meta(session), chunks)
    total_chunks = sum(len(c) for _, c in batches)
    total_bytes = sum(len(c["data"]) for _, c in batches for c in c)
    return archive, decoder, total_chunks, total_bytes

def snapshot(archive, decoder):
    with archive.connect() as conn:
        outbox = [{"id": r[0], "row": r[1]} for r in conn.execute(
            "SELECT id, row_json FROM outbox ORDER BY id")]
        states = [dict(r) for r in conn.execute(
            "SELECT s.session_id, d.next_seq, d.state_json, d.counts_json, d.rows"
            " FROM decode_states d JOIN sessions s ON s.id=d.session"
            " WHERE d.epoch=? ORDER BY s.session_id", (decoder.epoch,))]
        partial = [dict(r) for r in conn.execute(
            "SELECT s.session_id, d.seq, d.state_json, d.counts_json, d.rows_emitted"
            " FROM decode_partial d JOIN sessions s ON s.id=d.session"
            " WHERE d.epoch=? ORDER BY s.session_id", (decoder.epoch,))]
        counters = dict(conn.execute("SELECT key,value FROM archive_meta").fetchall())
    return {"epoch": decoder.epoch, "outbox": outbox, "states": states,
            "partial": partial, "counters": counters}

# Frozen before seeding: every wall-derived value (last_receive_ns,
# ingest_time, last_decode_ns) is deterministic, so equality covers the
# full snapshot with no excluded fields.
time.time_ns = lambda: fixed_ns  # noqa: E731 - deterministic wall clock
archive, decoder, total_chunks, total_bytes = seed(str(work / "seed.db"))
w0, c0 = time.monotonic(), time.process_time()
calls, staged = 0, 0
while True:
    n = archive.decode_once(decoder, limit=1000)
    if not n:
        break
    calls += 1
    staged += n
wall, cpu = time.monotonic() - w0, time.process_time() - c0
snap = snapshot(archive, decoder)
try:
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        peak *= 1024
except Exception:
    peak = None
# Separate un-timed pass, seed excluded: count what the tree REALLY fetches
# by wrapping the cursor, never by trusting _decode_prefetch_stats (absent
# on the baseline tree) and never by falling back to staged/committed.
# Materialized raw SELECT rows and their data bytes are counted per fetched
# cursor row, including a byte-cap overflow row the receiver discards after
# the length check (labeled overflow, not retained). Read queries, refill
# lookups and write commits are counted from execute() text; the snapshot()
# SELECTs above run before wrapping, so they never pollute the counts.
conns = None
io = None
if instrument:
    with tempfile.TemporaryDirectory() as tmp:
        archive2, decoder2, _, _ = seed(str(Path(tmp) / "i.db"))
        counts = {"conns": 0, "fetched_rows": 0, "fetched_bytes": 0,
                  "read_queries": 0, "commits": 0}
        real = archive2.connect
        # Wrap rows at the cursor level: every sqlite3.Row the receiver pulls
        # from a raw_chunks SELECT is one materialized fetch with its data
        # bytes. A page row read but discarded by the 1MiB byte-cap check is
        # still fetched off the cursor (not retained); the receiver-side
        # _decode_prefetch_stats on the current tree labels retained vs
        # discarded, while this connection-level counter reports the observed
        # cursor total identically for both trees. Non-raw SELECTs (pending
        # heads, cursor/partial probes, refill id lookups) count as read
        # queries without row bytes. Commits counted via commit().
        class RowIter:
            # Cursor facade: iteration, fetchone/fetchall/fetchmany, close
            # and contextlib.closing() all route here, so every raw row the
            # receiver materializes is counted exactly once however it reads.
            def __init__(self, cursor, counts, raw):
                self._cursor, self._counts, self._raw = cursor, counts, raw
            def _count(self, row):
                if self._raw and row is not None:
                    try:
                        data = row["data"]
                    except Exception:
                        data = None
                    if data is not None:
                        self._counts["fetched_rows"] += 1
                        self._counts["fetched_bytes"] += len(data)
                return row
            def __iter__(self):
                return self
            def __next__(self):
                return self._count(next(self._cursor))
            next = __next__
            def fetchone(self):
                return self._count(self._cursor.fetchone())
            def fetchall(self):
                rows = self._cursor.fetchall()
                for row in rows:
                    self._count(row)
                return rows
            def fetchmany(self, size=None):
                rows = self._cursor.fetchmany(size) if size is not None else self._cursor.fetchmany()
                for row in rows:
                    self._count(row)
                return rows
            def close(self):
                return self._cursor.close()
            def __enter__(self):
                self._cursor.__enter__()
                return self
            def __exit__(self, *exc):
                return self._cursor.__exit__(*exc)
            def __getattr__(self, name):
                return getattr(self._cursor, name)
        class ConnWrap:
            # Connection facade: `with self.connect()` needs __enter__/__exit__
            # on the type (special-method lookup bypasses __getattr__), so
            # delegate explicitly; __enter__ returns the wrapper to keep
            # execute() counting inside the block.
            def __init__(self, conn, counts):
                self._conn, self._counts = conn, counts
            def execute(self, sql, *args):
                text = sql if isinstance(sql, str) else ""
                upper = text.upper()
                if upper.lstrip().startswith("SELECT") and "FROM RAW_CHUNKS" in upper:
                    self._counts["read_queries"] += 1
                    return RowIter(self._conn.execute(sql, *args), self._counts, True)
                if upper.lstrip().startswith("SELECT"):
                    self._counts["read_queries"] += 1
                return self._conn.execute(sql, *args)
            def __enter__(self):
                self._conn.__enter__()
                return self
            def __exit__(self, *exc):
                return self._conn.__exit__(*exc)
            def __getattr__(self, name):
                if name == "commit":
                    counts = self._counts
                    real_commit = self._conn.commit
                    def commit():
                        counts["commits"] += 1
                        return real_commit()
                    return commit
                return getattr(self._conn, name)
        def counting():
            counts["conns"] += 1
            return ConnWrap(real(), counts)
        archive2.connect = counting
        while archive2.decode_once(decoder2, limit=1000):
            pass
        conns = counts["conns"]
        io = {"fetched_rows": counts["fetched_rows"],
              "fetched_bytes": counts["fetched_bytes"],
              "read_queries": counts["read_queries"],
              "commit_statements": counts["commits"]}
print(json.dumps({"wall_s": wall, "cpu_s": cpu, "calls": calls,
                  "staged_chunks": staged, "total_chunks": total_chunks,
                  "total_bytes": total_bytes, "outbox_rows": len(snap["outbox"]),
                  "read_io": io,
                  "peak_rss_bytes": peak, "connections": conns, "snapshot": snap}))
"""

def _run_side(side_root, case, chunks, fixed_ns, instrument):
    with tempfile.TemporaryDirectory(prefix="can-decode-%s-" % case) as tmp:
        proc = subprocess.run(
            [sys.executable, "-c", _CHILD, str(side_root), tmp, case,
             str(chunks), str(fixed_ns), "1" if instrument else "0"],
            capture_output=True, text=True, cwd=str(side_root),
            env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                 "PYTHONPATH": str(side_root), "PYTHONNOUSERSITE": "1"},
            timeout=1200)
    if proc.returncode != 0:
        raise RuntimeError("%s %s failed: %s" % (side_root, case, proc.stderr[-3000:]))
    return json.loads(proc.stdout)


def _digest(snapshot):
    raw = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    return snapshot, hashlib.sha256(raw).hexdigest()


def _rates(side):
    wall = max(side["wall_s"], 1e-9)
    io = side.get("read_io") or {}
    fetched = io.get("fetched_rows")
    staged = side["staged_chunks"]
    return {"wall_s": side["wall_s"], "cpu_s": side["cpu_s"],
            "chunks_per_s": side["total_chunks"] / wall,
            "bytes_per_s": side["total_bytes"] / wall,
            "rows_per_s": side["outbox_rows"] / wall,
            "decode_calls": side["calls"],
            "staged_chunks": staged,
            "fetched_chunks": fetched,
            "read_queries": io.get("read_queries"),
            "fetched_bytes": io.get("fetched_bytes"),
            "commit_statements": io.get("commit_statements"),
            "connections": side["connections"],
            "peak_rss_bytes": side["peak_rss_bytes"],
            "total_chunks": side["total_chunks"],
            "total_bytes": side["total_bytes"],
            "outbox_rows": side["outbox_rows"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", required=True)
    parser.add_argument("--chunks", type=int, default=12000)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--cases", default=",".join(CASES))
    parser.add_argument("--fixed-ns", type=int, default=1810000000000000000)
    parser.add_argument("--no-instrument", action="store_true")
    parser.add_argument("--max-slowdown", type=float, default=0.25,
                        help="fail when a case's current median wall time exceeds "
                        "the baseline median by more than this fraction")
    parser.add_argument("--json-out", type=Path, default=None,
                        help="write the JSON report here even on failure (CI artifact)")
    args = parser.parse_args(argv)
    if args.chunks < 1 or args.repeats < 1 or args.fixed_ns < 0:
        parser.error("--chunks and --repeats must be positive; --fixed-ns must be nonnegative")
    if not math.isfinite(args.max_slowdown) or args.max_slowdown < 0:
        parser.error("--max-slowdown must be finite and nonnegative")
    cases = [c.strip() for c in args.cases.split(",") if c.strip()]
    if not cases:
        parser.error("--cases must select at least one case")
    unknown = [c for c in cases if c not in CASES]
    if unknown:
        raise SystemExit("unknown cases: %s" % ",".join(unknown))
    report = {"synthetic": True,
              "note": "synthetic fixture only; not production measurements",
              "baseline_available": True, "max_slowdown": args.max_slowdown,
              "cases": {}, "mismatch": [], "regressions": []}
    for case in cases:
        base_walls, cur_walls, base_cpus, cur_cpus = [], [], [], []
        base = cur = None
        for rep in range(args.repeats):
            if rep % 2:
                cur = _run_side(CURRENT_ROOT, case, args.chunks,
                                args.fixed_ns, not args.no_instrument)
                base = _run_side(args.baseline_root, case, args.chunks,
                                 args.fixed_ns, not args.no_instrument)
            else:
                base = _run_side(args.baseline_root, case, args.chunks,
                                 args.fixed_ns, not args.no_instrument)
                cur = _run_side(CURRENT_ROOT, case, args.chunks,
                                args.fixed_ns, not args.no_instrument)
            base_view, base_hash = _digest(base["snapshot"])
            cur_view, cur_hash = _digest(cur["snapshot"])
            if base_view != cur_view:
                report["mismatch"].append(
                    {"case": case, "baseline_sha256": base_hash,
                     "current_sha256": cur_hash})
                break
            base_walls.append(base["wall_s"])
            cur_walls.append(cur["wall_s"])
            base_cpus.append(base["cpu_s"])
            cur_cpus.append(cur["cpu_s"])
        base_median = statistics.median(base_walls) if base_walls else base["wall_s"]
        cur_median = statistics.median(cur_walls) if cur_walls else cur["wall_s"]
        equal = not any(m["case"] == case for m in report["mismatch"])
        if base_median > 0:
            slowdown = (cur_median - base_median) / base_median
        else:
            slowdown = None
        regressed = bool(equal and base_walls and slowdown is not None
                         and slowdown > args.max_slowdown)
        if regressed:
            report["regressions"].append(
                {"case": case, "baseline_wall_s": base_median,
                 "current_wall_s": cur_median, "slowdown_wall": slowdown,
                 "max_slowdown": args.max_slowdown})
        report["cases"][case] = {
            "equal": equal,
            "equality_sha256": base_hash if base_view == cur_view else None,
            "baseline": _rates(dict(base, wall_s=base_median,
                                    cpu_s=statistics.median(base_cpus) if base_cpus else base["cpu_s"])),
            "current": _rates(dict(cur, wall_s=cur_median,
                                   cpu_s=statistics.median(cur_cpus) if cur_cpus else cur["cpu_s"])),
            "speedup_wall": base_median / cur_median if cur_median > 0 else None,
            "slowdown_wall": slowdown,
            "regressed": regressed,
            "max_slowdown": args.max_slowdown,
            "baseline_walls_s": base_walls, "current_walls_s": cur_walls}
    payload = json.dumps(report, indent=2, sort_keys=True)
    print(payload)
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(payload + "\n", encoding="utf-8")
    return 1 if (report["mismatch"] or report["regressions"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
