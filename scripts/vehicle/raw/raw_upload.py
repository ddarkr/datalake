#!/usr/bin/env python3
"""Sealed triple (MF4 + ingress sidecar + manifest) -> S3 uploader.

Ack rule: local closed JSONL + sealed .mf4 + .ingress.jsonl.gz + .mf4.manifest
are deleted only after ALL THREE remote objects verify (PUT then re-GET,
stream-hash, compare). Remote user-metadata hash is recorded but never
trusted alone. Any failure on any leg keeps ALL locals. Before uploading,
the sealed MF4 bytes must equal the manifest mf4_sha256 AND the sidecar
bytes must equal ingress_sidecar_sha256 AND the sidecar must gunzip to the
exact closed JSONL bytes (ingress_sha256); any drift quarantines the segment
and uploads nothing (fail-closed: post-seal corruption is never backed up as
truth, originals are never deleted). Uploads are idempotent: remote keys are
content-addressed by SHA256(manifest bytes) -- which already pins MF4 +
sidecar -- so a crash between upload and delete re-uploads to the same key
on restart. Only complete triples are pending; staging *.tmp and partial
sets are invisible to the uploader.
"""

import hashlib
import os
import signal
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from scripts.storage.storage_metrics import bucket_inventory as _bucket_inventory
except ImportError:  # validation helper absent: uploads continue regardless
    _bucket_inventory = None

_M = {"pending_files": 0, "pending_bytes": 0, "uploaded_files": 0,
      "uploaded_bytes": 0, "failed": 0, "dropped_verify": 0,
      "last_upload": 0, "bucket_bytes": None, "bucket_objects": None,
      "inventory_timestamp_seconds": None, "inventory_success": 0, "up": 1}
_MLOCK = threading.Lock()


def minc(k, n=1):
    with _MLOCK:
        _M[k] = _M.get(k, 0) + n


def mset(k, v):
    with _MLOCK:
        _M[k] = v


class _H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        with _MLOCK:
            snap = {k: v for k, v in dict(_M).items() if v is not None}
        body = "".join("raw_uploader_%s %s\n" % kv for kv in sorted(snap.items()))
        raw = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *a):
        pass


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


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def remote_key(path, vehicle, manifest_digest):
    """Content-addressed key: raw/vehicle/<id>/can/YYYY/MM/DD/<md>/<stem>.mf4.

    manifest_digest is SHA256(manifest bytes), which pins MF4 + sidecar, so
    same filename with changed bytes lands under a different key instead of
    overwriting live archives. Stem filename is unchanged so redecode
    sidecar-name validation keeps working.
    """
    import re as _re
    stem = os.path.basename(path)
    if stem.endswith(".mf4"):
        stem = stem[:-4]
    dt = None
    m = _re.search(r"(\d{8}T\d{6}Z)", stem)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc)
        except ValueError:
            dt = None
    if dt is None:
        dt = datetime.now(timezone.utc)
    return "raw/vehicle/%s/can/%s/%s/%s" % (
        vehicle, dt.strftime("%Y/%m/%d"), manifest_digest, stem + ".mf4")


def require_https_endpoint(value, name="RAW_S3_ENDPOINT_URL"):
    """HTTPS-only endpoint with valid host before boto3 sees credentials."""
    parsed = urllib.parse.urlparse((value or "").strip())
    if parsed.scheme != "https" or not parsed.hostname:
        raise SystemExit(name + " must be an https:// URL with a host")
    return value.strip()

def make_client():
    endpoint = require_https_endpoint(e("RAW_S3_ENDPOINT_URL"))
    import boto3  # noqa: pinned dep, lazy so import errors stay local
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=endpoint,
        region_name=e("RAW_S3_REGION", "us-west-004"),
        aws_access_key_id=e("RAW_S3_ACCESS_KEY_ID"),
        aws_secret_access_key=e("RAW_S3_SECRET_ACCESS_KEY"),
        config=Config(retries={"max_attempts": 1},  # own backoff below
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required",
                      connect_timeout=10, read_timeout=60))


def _err_code(ex):
    resp = getattr(ex, "response", None) or {}
    return ((resp.get("Error", {}) or {}).get("Code", ""),
            (resp.get("ResponseMetadata", {}) or {}).get("HTTPStatusCode"))


def _is_missing(ex):
    """Genuine missing-key only: recognized S3 codes (never bare status)."""
    code, _ = _err_code(ex)
    if code == "NoSuchBucket":
        return False
    return code in ("NoSuchKey", "NotFound")


def _remote_matches(resp, size, digest):
    """True only when remote length (when reported) and SHA256 both match."""
    body = resp["Body"]
    try:
        length = resp.get("ContentLength")
        if length is not None and int(length) != size:
            return False
        h = hashlib.sha256()
        for chunk in body.iter_chunks(chunk_size=8 << 20):
            h.update(chunk)
        return h.hexdigest() == digest
    finally:
        try:
            body.close()
        except Exception:
            pass


def upload_verified(client, bucket, key, path, digest, max_retries):
    """Verified write to a content-addressed key: never overwrites truth.

    Pre-check GET first: identical size+SHA256 reuses the live object;
    different bytes are a conflict (kept, never overwritten); any other GET
    error is unreadable (retried, never mistaken for missing). Only genuine
    missing-key permits PUT, then post-PUT GET-and-hash must match. True
    only on verified bytes; any False keeps all locals (run_once deletes
    only on full-triple ack).
    # ponytail: key safety rests on content addressing, not PUT atomicity --
    concurrent compliant writers sharing a key hold identical triple bytes
    (same manifest digest), so a plain S3 PUT is safe; noncompliant external
    writers must not share the archive prefix.
    """
    size = os.path.getsize(path)
    delay = 2.0
    for attempt in range(max_retries + 1):
        try:
            try:
                existing = client.get_object(Bucket=bucket, Key=key)
            except Exception as gex:
                if not _is_missing(gex):
                    raise
                existing = None
            if existing is not None:
                if _remote_matches(existing, size, digest):
                    return True
                minc("dropped_verify")
                print("immutable conflict %s: live object differs, kept"
                      % (key,), file=sys.stderr)
                return False
            with open(path, "rb") as fh:
                client.put_object(Bucket=bucket, Key=key, Body=fh,
                                  ContentLength=size,
                                  Metadata={"sha256": digest})
            if _remote_matches(client.get_object(Bucket=bucket, Key=key),
                               size, digest):
                return True
            minc("dropped_verify")  # checksum mismatch: keep local, retry
            print("verify mismatch %s (attempt %d)" % (key, attempt),
                  file=sys.stderr)
        except Exception as ex:
            print("upload %s attempt %d: %s" % (key, attempt, ex),
                  file=sys.stderr)
        if attempt < max_retries:
            time.sleep(delay)
            delay = min(delay * 2, 120.0)
    return False

def refresh_inventory(client, bucket):
    """Own bucket total via shared helper; failure keeps prior, success=0."""
    if _bucket_inventory is None:
        mset("inventory_success", 0)  # helper absent: uploads unaffected
        return
    try:
        total, count = _bucket_inventory(client, bucket)
    except Exception as ex:  # never leak bucket/key text: type name only
        print("raw inventory error: %s" % type(ex).__name__, file=sys.stderr)
        mset("inventory_success", 0)
        return
    with _MLOCK:
        _M.update(bucket_bytes=total, bucket_objects=count,
                  inventory_timestamp_seconds=int(time.time()), inventory_success=1)


def inventory_loop(client, bucket, interval, stop):
    """Own cadence, separate from upload sweeps: long listings never stall
    durable upload."""
    while not stop:
        refresh_inventory(client, bucket)
        for _ in range(max(interval, 1) * 2):
            if stop:
                break
            time.sleep(0.5)


def _load_manifest(manifest_path):
    import json as _json
    with open(manifest_path, encoding="utf-8") as fh:
        return _json.load(fh)


def pending(spool):
    """Sealed triples ready for upload. Anything less than MF4 + sidecar +
    manifest (or any *.tmp staging fragment) is NOT pending: it is either
    still being finalized or a torn write, and the uploader must not see it.
    Returns (mf4, sidecar, manifest, closed_or_None) tuples; closed is
    resolved from the local closed/ dir by stem."""
    sealed = os.path.join(spool, "sealed")
    closed_dir = os.path.join(spool, "closed")
    try:
        names = sorted(os.listdir(sealed))
    except OSError:
        return []
    have = set(names)
    out = []
    for n in names:
        if not n.endswith(".mf4"):
            continue
        if n.endswith(".tmp.mf4") or n.endswith(".stage.tmp"):
            continue
        if n + ".stage.tmp" in have:
            continue
        stem = n[:-len(".mf4")]
        sidecar_n = stem + ".ingress.jsonl.gz"
        manifest_n = n + ".manifest.json"
        if sidecar_n not in have or manifest_n not in have:
            continue
        mf4_path = os.path.join(sealed, n)
        sidecar_path = os.path.join(sealed, sidecar_n)
        manifest_path = os.path.join(sealed, manifest_n)
        closed_path = os.path.join(closed_dir, stem + ".jsonl")
        if not os.path.exists(closed_path):
            closed_path = None  # already GC'd edge: triple ack decides
        out.append((mf4_path, sidecar_path, manifest_path, closed_path))
    return out


def manifest_key(mf4_key):
    return mf4_key + ".manifest.json"


def sidecar_key(mf4_key):
    return mf4_key[:-len(".mf4")] + ".ingress.jsonl.gz"


def fail_closed_check(mf4_path, sidecar_path, manifest_path, closed_path):
    """Verify sealed bytes against the manifest pin BEFORE any upload.
    Returns (ok, reason): ok only when MF4 == mf4_sha256, sidecar ==
    ingress_sidecar_sha256, and sidecar gunzips to bytes == ingress_sha256
    (and to the closed JSONL on disk when still present)."""
    import gzip
    try:
        man = _load_manifest(manifest_path)
    except (OSError, ValueError) as ex:
        return False, "manifest unreadable: %s" % (ex,)
    for field in ("mf4_sha256", "ingress_sha256", "ingress_sidecar_sha256"):
        if not man.get(field):
            return False, "manifest missing " + field
    if sha256_file(mf4_path) != man["mf4_sha256"]:
        return False, "sealed MF4 drifted from manifest pin"
    if sha256_file(sidecar_path) != man["ingress_sidecar_sha256"]:
        return False, "sealed sidecar drifted from manifest pin"
    try:
        with open(sidecar_path, "rb") as fh:
            raw = fh.read()
        body = gzip.decompress(raw)
    except (OSError, EOFError) as ex:
        return False, "sidecar gunzip failed: %s" % (ex,)
    h = hashlib.sha256()
    h.update(body)
    if h.hexdigest() != man["ingress_sha256"]:
        return False, "sidecar content drifted from ingress pin"
    if closed_path is not None:
        try:
            with open(closed_path, "rb") as fh:
                closed_bytes = fh.read()
        except OSError as ex:
            return False, "closed unreadable: %s" % (ex,)
        if closed_bytes != body:
            return False, "closed JSONL drifted from sidecar"
    return True, ""


def quarantine(spool, paths):
    """Move drifted/corrupt sealed locals aside; never upload, never delete."""
    qd = os.path.join(spool, "quarantine")
    os.makedirs(qd, exist_ok=True)
    for p in paths:
        if not p:
            continue
        try:
            os.replace(p, os.path.join(qd, os.path.basename(p)))
        except OSError:
            pass
    minc("dropped_verify")


def run_once(client, bucket, spool, vehicle, max_retries):
    triples = pending(spool)
    nbytes = 0
    for mf4_path, _, _, _ in triples:
        try:
            nbytes += os.path.getsize(mf4_path)
        except OSError:
            pass
    mset("pending_files", len(triples))
    mset("pending_bytes", nbytes)
    for mf4_path, sidecar_path, manifest_path, closed_path in triples:
        ok, reason = fail_closed_check(mf4_path, sidecar_path, manifest_path,
                                       closed_path)
        if not ok:
            print("fail-closed %s: %s (quarantined, originals kept)"
                  % (mf4_path, reason), file=sys.stderr)
            quarantine(spool, [mf4_path, sidecar_path, manifest_path,
                               closed_path])
            minc("failed")
            continue
        manifest_digest = sha256_file(manifest_path)  # pins MF4 + sidecar
        key = remote_key(mf4_path, vehicle, manifest_digest)
        legs = [(key, mf4_path), (sidecar_key(key), sidecar_path),
                (manifest_key(key), manifest_path)]
        digests = [sha256_file(p) for _, p in legs]
        acked = True
        for (k, p), digest in zip(legs, digests):
            if not upload_verified(client, bucket, k, p, digest, max_retries):
                minc("failed")  # any leg fails: ALL locals stay
                acked = False
                break
        if not acked:
            continue
        size = sum(os.path.getsize(p) for _, p in legs)
        for p in [mf4_path, sidecar_path, manifest_path, closed_path]:
            if p is None:
                continue
            try:
                os.unlink(p)
            except OSError:
                pass
        minc("uploaded_files")
        minc("uploaded_bytes", size)
        mset("last_upload", int(time.time()))
    return len(triples)


def main(argv):
    vehicle = e("VEHICLE_ID", "")
    bucket = e("RAW_BUCKET", "")
    if not vehicle or not bucket:
        print("VEHICLE_ID and RAW_BUCKET are required", file=sys.stderr)
        return 1
    if not e("RAW_S3_ACCESS_KEY_ID") or not e("RAW_S3_SECRET_ACCESS_KEY"):
        print("RAW_S3_ENDPOINT_URL/RAW_S3_ACCESS_KEY_ID/RAW_S3_SECRET_ACCESS_KEY required",
              file=sys.stderr)
        return 1
    try:
        require_https_endpoint(e("RAW_S3_ENDPOINT_URL"))
    except SystemExit as ex:
        print(str(ex), file=sys.stderr)
        return 1
    spool = e("RAW_SPOOL_DIR", "/spool/raw")
    os.makedirs(os.path.join(spool, "sealed"), exist_ok=True)
    interval = ei("RAW_UPLOAD_INTERVAL_SEC", 60)
    retries = ei("RAW_UPLOAD_RETRIES", 8)
    # Listen inside the container; Compose restricts the host-side interface.
    threading.Thread(target=lambda: ThreadingHTTPServer(("0.0.0.0", 9103), _H).serve_forever(),
                     daemon=True).start()
    stop = []
    signal.signal(signal.SIGTERM, lambda *a: stop.append(1))
    client = make_client()
    threading.Thread(target=lambda: inventory_loop(
        client, bucket, ei("RAW_INVENTORY_INTERVAL_SEC", 3600), stop),
        daemon=True).start()
    # Restart recovery is implicit: any sealed triple left behind (crash between
    # upload and delete) is re-uploaded to its deterministic key, verified,
    # then deleted. Staging *.tmp / manifest-less .mf4 are never touched here.
    while not stop:
        try:
            run_once(client, bucket, spool, vehicle, retries)
        except Exception as ex:
            print("sweep error: %s" % ex, file=sys.stderr)
        for _ in range(interval * 2):
            if stop:
                break
            time.sleep(0.5)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
