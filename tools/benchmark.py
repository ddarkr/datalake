#!/usr/bin/env python3
"""Offline paired performance harness; never connects to production services."""
import argparse
import csv
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
SCHEMA = 1
PROFILES = {"quick": 100, "large": 3000, "soak": 1000, "fault": 100}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def disk_bytes(directory):
    return sum(p.stat().st_size for p in Path(directory).rglob("*") if p.is_file())


def worker(args):
    # The harness/fixtures are shared, application imports come ONLY from this revision.
    from bench_workloads import prepare
    repo = Path(args.repo).resolve()
    if not (repo / "scripts").is_dir():
        raise ValueError("target checkout has no application scripts")
    sys.path.insert(0, str(repo))
    with tempfile.TemporaryDirectory(prefix="datalake-bench-") as directory:
        operation = prepare(args.workload, args.size, directory, fault=args.fault)
        for name, module in sys.modules.items():
            source = getattr(module, "__file__", None)
            if name.startswith("scripts.") and source and not Path(source).resolve().is_relative_to(repo):
                raise ValueError(f"application import escaped target revision: {name}")
        before = disk_bytes(directory)
        rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        cpu = time.process_time()
        start = time.perf_counter()
        result = operation()
        wall = time.perf_counter() - start
        cpu = time.process_time() - cpu
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        factor = 1 if sys.platform == "darwin" else 1024
        result.update(wall_seconds=wall, cpu_seconds=cpu,
                      cpu_percent=100 * cpu / wall, units_per_second=result["units"] / wall,
                      peak_rss_bytes=rss * factor,
                      setup_peak_rss_bytes=rss_before * factor,
                      disk_before_bytes=before, disk_after_bytes=disk_bytes(directory))
        result["disk_growth_bytes"] = result["disk_after_bytes"] - before
        result.setdefault("disk_pending_bytes", 0)
        print(json.dumps(result, sort_keys=True))


def runtime():
    return {"python": platform.python_version(), "platform": platform.platform(),
            "machine": platform.machine(), "cpu_count": os.cpu_count(),
            "dependencies": {name: importlib.metadata.version(name) for name in
                             ("cantools", "opentelemetry-proto", "PyYAML", "python-can", "protobuf",
                              "bitstruct", "textparser", "diskcache", "argparse-addons", "crccheck",
                              "packaging", "typing_extensions", "wrapt")}}


def run(args):
    from bench_workloads import WORKLOADS
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result = {"schema_version": SCHEMA, "revision": args.revision,
              "profile": args.profile, "duration_seconds": args.duration,
              "fixture_sha256": hashlib.sha256((HERE / "bench_workloads.py").read_bytes()).hexdigest(),
              "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "runtime": runtime(), "workloads": {}, "status": "running"}
    output.write_text(json.dumps(result, indent=2) + "\n")
    try:
        for name in WORKLOADS:
            command = [sys.executable, str(Path(__file__).resolve()), "worker", "--repo", args.repo,
                       "--workload", name, "--size", str(PROFILES[args.profile])]
            if args.profile == "fault":
                command.append("--fault")
            samples = []
            golden = None
            deadline = time.monotonic() + args.duration / len(WORKLOADS)
            # The first fresh-process run warms OS/file caches, but is not reported.
            warmup = True
            while warmup or len(samples) < 3 or (args.profile == "soak" and time.monotonic() < deadline):
                completed = subprocess.run(command, capture_output=True, text=True, timeout=120, check=True)
                sample = json.loads(completed.stdout)
                current = digest(sample["golden"])
                if golden is not None and current != golden:
                    raise ValueError(f"non-deterministic golden output: {name}")
                golden = current
                if warmup:
                    warmup = False
                    continue
                samples.append(sample)
            metrics = ["wall_seconds", "cpu_seconds", "cpu_percent", "units_per_second", "peak_rss_bytes",
                       "setup_peak_rss_bytes", "disk_before_bytes", "disk_after_bytes", "disk_growth_bytes", "disk_pending_bytes"]
            result["workloads"][name] = {"units": samples[0]["units"], "golden_sha256": golden,
                "golden": samples[0]["golden"], "samples": samples,
                "median": {metric: statistics.median(s[metric] for s in samples) for metric in metrics}}
            output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        result["status"] = "passed"
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        if isinstance(exc, subprocess.CalledProcessError):
            result["worker_stderr"] = exc.stderr[-12000:]
        raise
    finally:
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


def compare(args):
    base = json.loads(Path(args.base).read_text())
    candidate = json.loads(Path(args.candidate).read_text())
    for key in ("schema_version", "profile", "duration_seconds", "fixture_sha256", "harness_sha256", "runtime"):
        if base[key] != candidate[key]:
            raise ValueError(f"incompatible comparison: {key}")
    if base["status"] != "passed" or candidate["status"] != "passed":
        raise ValueError("correctness/run failure; performance comparison refused")
    if base["workloads"].keys() != candidate["workloads"].keys():
        raise ValueError("workload sets differ")
    rows = []
    for name, b in base["workloads"].items():
        c = candidate["workloads"][name]
        if b["golden_sha256"] != c["golden_sha256"] or b["units"] != c["units"]:
            raise ValueError(f"golden correctness mismatch: {name}")
        for metric, old in b["median"].items():
            new = c["median"][metric]
            rows.append({"workload": name, "metric": metric, "base": old, "candidate": new,
                         "change_percent": 100 * (new - old) / old if old else None})
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    (out / "comparison.json").write_text(json.dumps({"advisory": True, "base": base["revision"],
        "candidate": candidate["revision"], "rows": rows}, indent=2) + "\n")
    with (out / "results.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = ["## Synthetic performance comparison", "", "Correctness passed. Performance is advisory, not a merge gate.",
             f"Base: `{base['revision']}` · Candidate: `{candidate['revision']}`", "",
             "| Workload | Metric | Base | Candidate | Change |", "|---|---|---:|---:|---:|"]
    for row in rows:
        change = "n/a" if row["change_percent"] is None else f"{row['change_percent']:+.1f}%"
        lines.append(f"| {row['workload']} | {row['metric']} | {row['base']:.4g} | {row['candidate']:.4g} | {change} |")
    lines += ["", "Fresh-process samples; installation, fixture preparation and warm-up excluded from timing. "
              "Peak RSS includes imports/setup; setup RSS is a high-water mark, not idle memory. Disk bytes are logical file sizes including SQLite WAL. "
              "No network/Greptime/container throughput or container image size is measured. See docs/performance.md."]
    (out / "summary.md").write_text("\n".join(lines) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run")
    run_parser.add_argument("--repo", required=True)
    run_parser.add_argument("--output", required=True)
    run_parser.add_argument("--revision", required=True)
    run_parser.add_argument("--profile", choices=PROFILES, default="quick")
    run_parser.add_argument("--duration", type=int, default=60)
    worker_parser = commands.add_parser("worker")
    worker_parser.add_argument("--repo", required=True)
    worker_parser.add_argument("--workload", required=True)
    worker_parser.add_argument("--size", type=int, required=True)
    worker_parser.add_argument("--fault", action="store_true")
    compare_parser = commands.add_parser("compare")
    compare_parser.add_argument("--base", required=True)
    compare_parser.add_argument("--candidate", required=True)
    compare_parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if args.command == "run" and not 1 <= args.duration <= 600:
        parser.error("duration must be between 1 and 600 seconds")
    {"worker": worker, "run": run, "compare": compare}[args.command](args)


if __name__ == "__main__":
    main()
