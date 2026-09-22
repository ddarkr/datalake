#!/usr/bin/env python3
"""S3 bucket usage inventory: actual stored objects, not transfer counters.

Reports what is really in the bucket (live SSTs, existing backups,
historical objects) via paginated ListObjectsV2. The raw MF4 bucket is
separate and never counted here.

Shared helper: raw_upload.py imports bucket_inventory() for its own Raw
bucket with its own limited key; keys never cross services.

Metrics (port 9105, internal only):
  datalake_s3_bucket_bytes{scope="greptime"}
  datalake_s3_bucket_objects{scope="greptime"}
  datalake_s3_inventory_timestamp_seconds  (last success only)
  datalake_s3_inventory_configured         (1 only: S3 mode + S3 settings)
  datalake_s3_inventory_success            (1 only: current values are fresh)

Only successful inventories publish values; a failure keeps the last good
values with success=0 (visibly stale, never a fake zero). File mode serves
the disabled state without contacting S3 (boto3 is never even imported).
Errors never carry bucket/key/credential text: type name only.
"""

import os
import signal
import shutil
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCOPE = "greptime"

_S = {"bytes": None, "objects": None, "timestamp": 0,
      "configured": 0, "success": 0}
_SLOCK = threading.Lock()


def e(name, default=""):
    return os.environ.get(name, default)


def ei(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit("invalid integer %s=%r" % (name, raw))


def bucket_inventory(client, bucket):
    """Paginated (total_bytes, object_count); streams, never stores entries."""
    total = 0
    count = 0
    token = None
    while True:
        kw = {"Bucket": bucket}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        for obj in resp.get("Contents") or []:
            total += int(obj["Size"])
            count += 1
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                raise IOError("s3 listing truncated without continuation token")
        else:
            return total, count


def make_client():
    import boto3  # noqa: pinned dep, lazy so File mode never needs it
    from botocore.config import Config
    from greptime_preflight import derive_b2_region
    return boto3.client(
        "s3", endpoint_url=e("S3_ENDPOINT_URL") or None,
        region_name=e("S3_REGION") or derive_b2_region(e("S3_ENDPOINT_URL")) or None,
        aws_access_key_id=e("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=e("S3_SECRET_ACCESS_KEY"),
        config=Config(retries={"max_attempts": 1},  # own cadence below
                      connect_timeout=10, read_timeout=60))


def s3_configured():
    """True only in S3 mode with connection settings; else disabled, no S3."""
    return e("GREPTIME_STORAGE_TYPE", "S3") == "S3" and bool(
        e("S3_ENDPOINT_URL") and e("S3_BUCKET") and e("S3_ACCESS_KEY_ID")
        and e("S3_SECRET_ACCESS_KEY"))


def resolve_client():
    """Client, or None when disabled: the caller must not contact S3 then."""
    if not s3_configured():
        return None
    return make_client()


def refresh_once(client, bucket):
    """Success replaces values; failure keeps prior (stale, success=0)."""
    try:
        total, count = bucket_inventory(client, bucket)
    except Exception as ex:  # never leak bucket/key text: type name only
        print("inventory error: %s" % type(ex).__name__, file=sys.stderr)
        with _SLOCK:
            _S["success"] = 0
        return
    with _SLOCK:
        _S.update(bytes=total, objects=count,
                  timestamp=int(time.time()), success=1)

def filesystem_metrics(path="/greptime-data"):
    """Read-only capacity and logical WAL bytes; no SST tree scan."""
    try:
        usage = shutil.disk_usage(path)
        pending = [os.path.join(path, "wal")]
        wal_bytes = 0
        while pending:
            try:
                with os.scandir(pending.pop()) as entries:
                    for entry in entries:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                pending.append(entry.path)
                            elif entry.is_file(follow_symlinks=False):
                                wal_bytes += entry.stat(follow_symlinks=False).st_size
                        except FileNotFoundError:
                            pass  # concurrent WAL purge
            except FileNotFoundError:
                pass  # empty/new volume or concurrent WAL directory purge
        return {"datalake_local_storage_success": 1,
                "datalake_disk_size_bytes": usage.total,
                "datalake_disk_used_bytes": usage.used,
                "datalake_disk_free_bytes": usage.free,
                "datalake_wal_bytes": wal_bytes}
    except OSError:
        return {"datalake_local_storage_success": 0}



def render():
    with _SLOCK:
        snap = dict(_S)
    lines = ["datalake_s3_inventory_configured %d" % snap["configured"],
             "datalake_s3_inventory_success %d" % snap["success"]]
    if snap["bytes"] is not None:
        lines.append('datalake_s3_bucket_bytes{scope="%s"} %d'
                     % (SCOPE, snap["bytes"]))
        lines.append('datalake_s3_bucket_objects{scope="%s"} %d'
                     % (SCOPE, snap["objects"]))
        lines.append("datalake_s3_inventory_timestamp_seconds %d"
                     % snap["timestamp"])
    lines.extend("%s %d" % item for item in filesystem_metrics().items())
    return "".join(l + "\n" for l in lines)


class _H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        raw = render().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *a):
        pass


def main(argv):
    interval = max(ei("STORAGE_METRICS_INTERVAL_SEC", 3600), 1)
    # Listen inside the container; Compose exposes no host port (internal only).
    threading.Thread(target=lambda: ThreadingHTTPServer(("0.0.0.0", 9105), _H).serve_forever(),
                     daemon=True).start()
    stop = []
    signal.signal(signal.SIGTERM, lambda *a: stop.append(1))
    signal.signal(signal.SIGINT, lambda *a: stop.append(1))
    client = resolve_client()
    if client is None:  # File mode / S3 absent: disabled state, never touch S3
        while not stop:
            time.sleep(0.5)
        return 0
    with _SLOCK:
        _S["configured"] = 1
    bucket = e("S3_BUCKET")
    while not stop:
        refresh_once(client, bucket)
        for _ in range(interval * 2):
            if stop:
                break
            time.sleep(0.5)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
