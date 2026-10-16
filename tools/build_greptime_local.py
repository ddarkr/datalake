#!/usr/bin/env python3
"""Export the task-owned native patch, or build its pinned Docker image.

  python3 tools/build_greptime_local.py export --source /tmp/datalake-greptime-local-only-src
  python3 tools/build_greptime_local.py build --jobs 2

BuildKit caches are retained; neither command changes the source Git index.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

UPSTREAM = "179ff8e5099d35e217a5ffeb6e4096405824dd6c"
ROOT = Path(__file__).resolve().parent
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))
from tools.demo import clean_env, require_local_docker
PATCH = ROOT / "greptime_local.patch"
# Explicit native ownership, including the planned typed local-query transport.
SOURCE_ROOTS = (
    "src/mito2/src/", "src/cmd/src/", "src/session/src/", "src/query/src/",
    "src/catalog/src/", "src/table/src/", "src/store-api/src/", "src/servers/src/",
    "src/frontend/src/", "src/metric-engine/src/",
)
SOURCE_FILES = {"src/mito2/Cargo.toml", "Cargo.lock", "src/common/meta/src/node_manager.rs"}


def git(source: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(source), *args])


def owned(path: str) -> bool:
    return path in SOURCE_FILES or (
        path.startswith(SOURCE_ROOTS) and path.endswith(".rs")
    )


def export(source: Path, destination: Path) -> None:
    if git(source, "rev-parse", "HEAD").decode().strip() != UPSTREAM:
        raise ValueError(f"source HEAD must be exactly {UPSTREAM}")
    tracked = git(source, "diff", "--name-only", "-z", "HEAD").decode().split("\0")
    new = git(source, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
    paths = sorted(p for p in tracked + new if p)
    rejected = [p for p in paths if not owned(p)]
    if rejected:
        raise ValueError(f"refusing unrelated source changes: {rejected}")
    for path in paths:
        full = source / path
        if full.is_symlink() or (full.exists() and not full.is_file()):
            raise ValueError(f"refusing non-regular source: {path}")
    patch = git(source, "diff", "--no-ext-diff", "--no-textconv", "--binary", "HEAD", "--", *[p for p in tracked if p])
    for path in sorted(p for p in new if p):
        result = subprocess.run(
            ["git", "diff", "--no-index", "--no-ext-diff", "--no-textconv", "--binary", "--", "/dev/null", path],
            cwd=source, stdout=subprocess.PIPE, check=False,
        )
        if result.returncode != 1:
            raise ValueError(f"cannot export new source {path}: exit {result.returncode}")
        patch += result.stdout
    if not patch:
        raise ValueError("refusing empty native patch")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
        temporary = Path(output.name)
        output.write(patch)
    temporary.replace(destination)
    print(f"{destination}: sha256={hashlib.sha256(patch).hexdigest()}")


def build(patch: Path, jobs: int, platform: str | None, native_tests: bool) -> None:
    require_local_docker()
    payload = patch.read_bytes()
    if not payload or (b"--require-local-sst-mirror-v1" not in payload and b"require_local_sst_mirror_v1" not in payload):
        raise ValueError("patch must include the native local-mirror startup guard")
    digest = hashlib.sha256(payload).hexdigest()
    tag = f"datalake/greptime-local:1.2.1-{UPSTREAM[:12]}-{digest[:16]}"
    if native_tests:
        tag += "-tests"
    with tempfile.TemporaryDirectory(prefix="greptime-local-build-") as directory:
        context = Path(directory)
        shutil.copyfile(ROOT / "greptime_local" / "Dockerfile", context / "Dockerfile")
        (context / "greptime_local.patch").write_bytes(payload)
        command = ["docker", "build", "--tag", tag, "--build-arg", f"PATCH_SHA256={digest}",
                   "--build-arg", f"BUILD_JOBS={jobs}"]
        if platform:
            command += ["--platform", platform]
        if native_tests:
            command += ["--target", "native-tests"]
        subprocess.run(command + [str(context)], check=True, env={**clean_env(), "DOCKER_BUILDKIT": "1"})
    print(tag)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    exporter = commands.add_parser("export", help="export all owned changes without staging")
    exporter.add_argument("--source", type=Path, required=True)
    exporter.add_argument("--patch", type=Path, default=PATCH)
    builder = commands.add_parser("build", help="build clean pinned source plus exported patch in Docker")
    builder.add_argument("--patch", type=Path, default=PATCH)
    builder.add_argument("--jobs", type=int, choices=range(1, 5), default=2)
    builder.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    builder.add_argument("--native-tests", action="store_true",
                         help="run focused Rust regressions using the cached native build")
    args = parser.parse_args()
    try:
        if args.command == "export":
            export(args.source.resolve(), args.patch.resolve())
        else:
            build(args.patch.resolve(), args.jobs, args.platform, args.native_tests)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"error: {error}\n")


if __name__ == "__main__":
    main()
