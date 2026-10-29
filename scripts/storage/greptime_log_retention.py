#!/usr/bin/env python3
"""Remove expired, closed Greptime file logs from a running Docker container."""
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


RETENTION_SECONDS = 3 * 24 * 60 * 60
LOG_NAME = re.compile(r"greptimedb(?:-err|-slow-queries)?\.[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}\Z")


def container_log_directory(container):
    template = '{{.State.Pid}}\n{{range .Mounts}}{{if eq .Destination "/data"}}{{json .Source}}{{end}}{{end}}'
    result = subprocess.run(
        ["docker", "inspect", "--format", template, container],
        check=True, capture_output=True, text=True, timeout=15,
    )
    pid_text, source_json = result.stdout.strip().split("\n", 1)
    pid = int(pid_text)
    source = json.loads(source_json)
    if pid <= 0 or not isinstance(source, str) or not os.path.isabs(source):
        raise ValueError("running Greptime container and absolute /data mount required")
    return pid, Path(source) / "logs"


def open_files(pid):
    """Collect open inodes before deleting; fail if the process is inaccessible."""
    fd_dir = Path("/proc") / str(pid) / "fd"
    opened = set()
    with os.scandir(fd_dir) as descriptors:
        for descriptor in descriptors:
            try:
                info = descriptor.stat()
            except FileNotFoundError:  # A descriptor closed during the scan.
                continue
            if stat.S_ISREG(info.st_mode):
                opened.add((info.st_dev, info.st_ino))
    fd_dir.stat()  # Do not delete if the process exited during the scan.
    return opened


def prune_logs(directory, opened, now):
    removed = allocated = skipped_open = 0
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with os.scandir(directory_fd) as entries:
            for entry in entries:
                if not LOG_NAME.fullmatch(entry.name):
                    continue
                try:
                    info = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(info.st_mode) or info.st_mtime > now - RETENTION_SECONDS:
                    continue
                if (info.st_dev, info.st_ino) in opened:
                    skipped_open += 1
                    continue
                try:
                    os.unlink(entry.name, dir_fd=directory_fd)
                except FileNotFoundError:
                    continue
                removed += 1
                allocated += info.st_blocks * 512
    finally:
        os.close(directory_fd)
    return {"removed": removed, "allocated_bytes": allocated, "skipped_open": skipped_open}


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: greptime_log_retention.py GREPTIME_CONTAINER")
    try:
        pid, directory = container_log_directory(sys.argv[1])
        print(json.dumps(prune_logs(directory, open_files(pid), time.time())))
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"Greptime log retention failed: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
