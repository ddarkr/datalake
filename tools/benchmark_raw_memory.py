#!/usr/bin/env python3
"""Stepped RAW resource-budget benchmark: finalize + upload preflight.

Builds one synthetic closed JSONL fixture per step in the PARENT (input
creation memory is never counted in child peaks), then runs
finalize_segment and the uploader gunzip_compare preflight each in a
spawned child and reports per-step wall/CPU/peak-RSS plus the ACTUAL
peak temp-disk bytes (sort runs, match database and staged MF4; verified
sealed triple never counts as temporary) and max resident frames.

The stepped sizes deliberately exceed the chosen tiny frame budget so
the spill path is exercised, e.g. --memory-frames 50 with
--steps 5000,20000,100000. Budgets are passed explicitly into
finalize_segment (not via ambient env). With --baseline-root, the same
fixture bytes finalize under the pristine baseline recorder and the
manifest semantics are compared for equivalence.

Isolated temp dirs only; never production paths.

Usage:
  python3 tools/benchmark_raw_memory.py \
      --steps 5000,20000,100000 --memory-frames 50 \
      [--tmp-max-bytes 2147483648] [--baseline-root /tmp/...baseline]
"""

import argparse
import hashlib
import importlib.util
import inspect
import json
import multiprocessing as _mp
import os
import sys
import tempfile
import threading

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

SEMANTIC_KEYS = ("frames", "seq_first", "seq_last", "twall_ns_first",
                 "twall_ns_last", "bus", "torn_tail_salvaged",
                 "ingress_sha256")


def need_mf4():
    try:
        import can.io.mf4  # noqa
    except ImportError as ex:
        print("pinned decode dep missing: %s" % ex, file=sys.stderr)
        return False
    return True


def build_segment(closed_path, n):
    from scripts.vehicle.raw import raw_recorder as rec
    with open(closed_path, "w", encoding="utf-8") as fh:
        for i in range(n):
            fh.write(json.dumps(rec.encode_frame(
                i + 1, 1_700_000_000_000_000_000 + i * 1_000_000,
                1700000000.0 + i * 0.001, "can0", 0x100 + (i % 7),
                bool(i % 2), False, False, False, False, False, 8,
                bytes([(i + b) % 256 for b in range(8)])),
                separators=(",", ":")) + "\n")


def rss_kb():
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":  # macOS bytes vs Linux KB
        rss //= 1024
    return rss


def load_recorder(root):
    path = os.path.join(root, "scripts", "vehicle", "raw",
                        "raw_recorder.py")
    name = "raw_baseline_" + hashlib.sha1(
        os.path.abspath(root).encode()).hexdigest()[:8]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load baseline recorder: %s" % path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sampler_peak(stop, sealed_dir, stem, out):
    # Sample all per-segment temporary files: sort/match directories and
    # the staged MF4 outside them. Verified sealed triple files are excluded.
    peak = 0
    prefix = stem + "."
    while not stop.is_set():
        total = 0
        try:
            for name in os.listdir(sealed_dir):
                if not name.startswith(prefix):
                    continue
                d = os.path.join(sealed_dir, name)
                if not os.path.isdir(d):
                    if name.endswith(".tmp"):
                        try:
                            total += os.path.getsize(d)
                        except OSError:
                            pass
                    continue
                for root, _ds, fns in os.walk(d):
                    for fn in fns:
                        try:
                            total += os.path.getsize(os.path.join(root, fn))
                        except OSError:
                            pass
        except OSError:
            pass
        peak = max(peak, total)
    out.append(peak)


def _finalize_call(rec, cp, sealed, tmp, mem_frames, tmp_max):
    stats = {}
    params = inspect.signature(rec.finalize_segment).parameters
    kw = {key: value for key, value in {
        "spool": tmp, "mem_frames": mem_frames,
        "tmp_max_bytes": tmp_max, "stats": stats,
    }.items() if key in params}
    return rec.finalize_segment(cp, sealed, **kw), stats


def _child_finalize(q, tmp, stem, cp, sealed, mem_frames, tmp_max, n):
    import time
    from scripts.vehicle.raw import raw_recorder as rec
    try:
        stop = threading.Event()
        sampled = []
        t = threading.Thread(target=_sampler_peak,
                             args=(stop, sealed, stem, sampled))
        c0 = time.process_time()
        t.start()
        try:
            mf4, stats = _finalize_call(rec, cp, sealed, tmp, mem_frames,
                                        tmp_max)
        finally:
            stop.set()
            t.join()
        cpu = time.process_time() - c0
        with open(mf4 + ".manifest.json", encoding="utf-8") as fh:
            man = json.load(fh)
        peak_disk = max(sampled[0] if sampled else 0,
                        int(stats.get("peak_temp_bytes", 0)))
        if "peak_resident_frames" in stats:
            resident, source = int(stats["peak_resident_frames"]), "hook"
        else:
            resident, source = min(n, mem_frames), "derived"
        q.put({"cpu": cpu, "rss": rss_kb(), "want": man["ingress_sha256"],
               "peak_temp_bytes": peak_disk,
               "peak_resident_frames": resident,
               "resident_source": source,
               "manifest": {k: man.get(k) for k in SEMANTIC_KEYS}})
    except BaseException as ex:
        q.put({"error": "%r" % ex})


def _child_baseline(q, tmp, stem, cp, sealed, mem_frames, tmp_max, n,
                    baseline_root):
    import time
    try:
        rec = load_recorder(baseline_root)
        c0 = time.process_time()
        stop = threading.Event()
        sampled = []
        t = threading.Thread(target=_sampler_peak,
                             args=(stop, sealed, stem, sampled))
        t.start()
        try:
            mf4, stats = _finalize_call(rec, cp, sealed, tmp, mem_frames,
                                        tmp_max)
        finally:
            stop.set()
            t.join()
        with open(mf4 + ".manifest.json", encoding="utf-8") as fh:
            man = json.load(fh)
        peak_disk = max(sampled[0] if sampled else 0,
                        int(stats.get("peak_temp_bytes", 0)))
        q.put({"cpu": time.process_time() - c0, "rss": rss_kb(),
               "want": man["ingress_sha256"],
               "peak_temp_bytes": peak_disk,
               "manifest": {k: man.get(k) for k in SEMANTIC_KEYS}})
    except BaseException as ex:
        q.put({"error": "%r" % ex})


def _child_preflight(q, tmp, stem, want, cp):
    import time
    from scripts.vehicle.raw import raw_upload as upl
    try:
        sidecar = os.path.join(tmp, "sealed", stem + ".ingress.jsonl.gz")
        c0 = time.process_time()
        res = upl.gunzip_compare(sidecar, want, cp)
        cpu = time.process_time() - c0
        q.put({"cpu": cpu, "rss": rss_kb(), "res": list(res)})
    except BaseException as ex:
        q.put({"error": "%r" % ex})


def spawn_measure(target, args):
    import time
    ctx = _mp.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=target, args=(q,) + tuple(args))
    t0 = time.time()
    p.start()
    payload = q.get()
    p.join()
    wall = time.time() - t0
    if p.exitcode != 0 or "error" in payload:
        raise RuntimeError("bench child failed: %r exit=%r"
                           % (payload, p.exitcode))
    return wall, payload


def run_step(n, mem_frames, tmp_max, baseline_root):
    import shutil
    from scripts.vehicle.raw import raw_recorder as rec
    # One fixture built in the parent; identical bytes for both recorders.
    fx = tempfile.mkdtemp(prefix="raw-bench-fixture-%d-" % n)
    stem = "m3_bench_%d" % n
    src = os.path.join(fx, stem + ".jsonl")
    build_segment(src, n)
    closed_bytes = os.path.getsize(src)
    tmp = btmp = None
    try:
        tmp = tempfile.mkdtemp(prefix="raw-bench-%d-" % n)
        rec.ensure_dirs(tmp)
        dirs = rec.seg_dirs(tmp)
        cp = os.path.join(dirs["closed"], stem + ".jsonl")
        shutil.copyfile(src, cp)
        wall, fres = spawn_measure(
            _child_finalize,
            (tmp, stem, cp, dirs["sealed"], mem_frames, tmp_max, n))
        sealed_total = sum(
            os.path.getsize(os.path.join(dirs["sealed"], x))
            for x in os.listdir(dirs["sealed"]))
        pwall, pres = spawn_measure(_child_preflight,
                                    (tmp, stem, fres["want"], cp))
        ok, reason = pres["res"]
        out = {
            "frames": n, "closed_bytes": closed_bytes,
            "memory_frames": mem_frames, "tmp_max_bytes": tmp_max,
            "finalize": {
                "wall_s": round(wall, 3), "cpu_s": round(fres["cpu"], 3),
                "peak_rss_kb": fres["rss"],
                "peak_temp_bytes": fres["peak_temp_bytes"],
                "peak_resident_frames": fres["peak_resident_frames"],
                "resident_source": fres["resident_source"],
                "sealed_bytes": sealed_total},
            "preflight": {
                "wall_s": round(pwall, 3),
                "cpu_s": round(pres["cpu"], 3),
                "peak_rss_kb": pres["rss"], "ok": bool(ok),
                "reason": "" if ok else reason},
            "manifest": fres["manifest"],
            "baseline": None, "equivalent": bool(ok),
            "units": {"wall": "s", "cpu": "s", "peak_rss": "KiB",
                      "peak_temp": "bytes", "resident": "frames",
                      "sealed": "bytes"},
        }
        if baseline_root:
            btmp = tempfile.mkdtemp(prefix="raw-bench-base-%d-" % n)
            brec = load_recorder(baseline_root)
            brec.ensure_dirs(btmp)
            bdirs = brec.seg_dirs(btmp)
            bcp = os.path.join(bdirs["closed"], stem + ".jsonl")
            shutil.copyfile(src, bcp)
            bwall, bres = spawn_measure(
                _child_baseline,
                (btmp, stem, bcp, bdirs["sealed"], mem_frames, tmp_max,
                 n, os.path.abspath(baseline_root)))
            out["baseline"] = {
                "wall_s": round(bwall, 3), "cpu_s": round(bres["cpu"], 3),
                "peak_rss_kb": bres["rss"],
                "peak_temp_bytes": bres["peak_temp_bytes"],
                "manifest": bres["manifest"]}
            out["equivalent"] = bool(
                ok and bres["manifest"] == fres["manifest"]
                and bres["want"] == fres["want"])
        print(json.dumps(out, sort_keys=True))
    finally:
        for directory in (fx, tmp, btmp):
            if directory is not None:
                shutil.rmtree(directory, ignore_errors=True)


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default=os.environ.get(
        "RAW_BENCH_STEPS", "5000,20000,100000"))
    ap.add_argument("--memory-frames", type=int, default=int(
        os.environ.get("RAW_FINALIZE_MEM_FRAMES", "50")))
    ap.add_argument("--tmp-max-bytes", type=int, default=int(
        os.environ.get("RAW_TMP_MAX_BYTES", str(2 << 30))))
    ap.add_argument("--baseline-root", default="")
    ns = ap.parse_args(argv[1:])
    if not need_mf4():
        return 2
    for tok in ns.steps.split(","):
        run_step(int(tok.strip()), ns.memory_frames, ns.tmp_max_bytes,
                 ns.baseline_root or "")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
