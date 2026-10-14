#!/usr/bin/env python3
"""Delete only the two Greptime 1.2.1 object caches in an offline owned volume.

The benchmark validates Docker ownership and stops the sole volume consumer
before invoking this helper. No Docker socket, network, credentials, or S3
volume is mounted here. A full filesystem preflight precedes any deletion.
"""
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tomllib

CACHE_DIRS = ("cache/object/write", "cache/object/manifest")


def cache_config(text):
    config = tomllib.loads(text)
    storage = config.get("storage", {})
    engines = config.get("region_engine", [])
    if (storage.get("type") != "S3"
            or storage.get("data_home") != "/data"
            or storage.get("bucket") != "bench-data"
            or storage.get("root") != "greptime"
            or config.get("wal", {}).get("provider") != "raft_engine"
            or config.get("wal", {}).get("dir") != "/data/wal"
            or len(engines) != 1
            or "mito" not in engines[0]):
        raise RuntimeError("unknown benchmark storage/cache layout")
    mito = engines[0]["mito"]
    path = mito.get("write_cache_path", "")
    if not isinstance(path, str) or (path.strip() and path != "/data"):
        raise RuntimeError("unknown benchmark write_cache_path")
    return {"data_home": "/data", "s3_root": "greptime",
            "configured_write_cache_path": path,
            "effective_write_cache_path": "/data",
            "configured_enable_write_cache": mito.get("enable_write_cache", False),
            "runtime_enable_write_cache": True,
            "runtime_reason": "Greptime 1.2.1 forces object-storage write cache on",
            "cache_dirs": list(CACHE_DIRS)}


def _directory(path):
    mode = path.lstat().st_mode
    if not stat.S_ISDIR(mode):
        raise RuntimeError("cache reset refuses non-directory or symlink")


def _walk_error(error):
    raise RuntimeError("cache reset cannot completely inspect cache tree") from error


def reset_cache(root, text):
    """The CLI fixes root=/data; tests use a real temporary volume directory."""
    cache_config(text)
    root = Path(root)
    if not root.is_absolute() or root == Path("/") or ".." in root.parts:
        raise RuntimeError("cache reset refuses unsafe volume root")
    # Never resolve a symlink to a different volume, including an ancestor.
    for path in reversed((root, *root.parents)):
        _directory(path)
    targets, records = [], []
    for relative in CACHE_DIRS:
        target = root / relative
        parent = root
        missing = False
        for part in Path(relative).parts:
            parent /= part
            try:
                _directory(parent)
            except FileNotFoundError:
                missing = True
                break
        files = size = 0
        if not missing:
            for current, dirs, names in os.walk(target, followlinks=False, onerror=_walk_error):
                for name in dirs:
                    _directory(Path(current) / name)
                for name in names:
                    info = (Path(current) / name).lstat()
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise RuntimeError("cache reset refuses symlink/special/linked file")
                    files += 1
                    size += info.st_size
            targets.append(target)
        records.append({"cache_dir": relative, "deleted_files": files,
                        "deleted_bytes": size})
    # Both trees are fully validated before either is removed. Greptime is
    # stopped and the helper is the only volume consumer, so no writer races.
    if not shutil.rmtree.avoids_symlink_attacks:
        raise RuntimeError("cache reset needs symlink-safe rmtree")
    for target in targets:
        shutil.rmtree(target)
    return {"cache_dirs": records,
            "deleted_files": sum(r["deleted_files"] for r in records),
            "deleted_bytes": sum(r["deleted_bytes"] for r in records),
            "preserved_primary": ["wal", "data", "metadata", "auth", "s3_objects"]}


def main():
    volumes = {}
    for role in ("greptime", "minio"):
        value = os.environ.get("CACHE_%s_VOLUME_ID" % role.upper(), "")
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise RuntimeError("missing opaque owned volume identity")
        volumes[role] = value
    with open("/etc/greptime/greptimedb.toml", encoding="utf-8") as source:
        text = source.read()
    result = reset_cache("/data", text)
    result["preserved_volume_ids"] = volumes
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Filesystem/parse exceptions can contain paths or config text. The
        # caller receives a failure, never credentials or arbitrary paths.
        print(json.dumps({"status": "failure", "error": "cache reset refused"}))
        raise SystemExit(1)
