#!/usr/bin/env python3
"""Sealed triple (MF4 + ingress sidecar + manifest) -> S3 uploader.

Acknowledgment rule: locals (closed JSONL + sealed .mf4 + .ingress.jsonl.gz
+ .mf4.manifest) are deleted only after ALL THREE remote objects prove
server-validated integrity. Each PUT sends the local SHA256 as S3
ChecksumSHA256 and the PUT response must echo the same value back; the
post-PUT HEAD must then report that ChecksumSHA256 with ContentLength equal
to the local size and user-metadata sha256 equal to the local digest.
Metadata alone never verifies bytes: reusing a live object requires its
HEAD ChecksumSHA256 to match too (legacy/checksum-less objects fail closed:
kept, never trusted). A missing or unsupported server checksum is an
explicit failure -- locals kept, no GET body readback fallback, ever.
Before uploading, the sealed MF4 bytes must equal the manifest mf4_sha256
AND the sidecar bytes must equal ingress_sidecar_sha256 AND the sidecar must
gunzip to the exact closed JSONL bytes (ingress_sha256); any drift
found before upload quarantines the segment without uploading. The exact
manifest bytes, validated digests and sizes remain pinned for every PUT and
replay HEAD. The same guard rechecks those pins before local deletion;
post-preflight drift keeps or quarantines all locals, never acknowledges it.
Uploads are idempotent: remote keys are content-addressed by
SHA256(manifest bytes) -- which already pins MF4 + sidecar -- so a crash
between upload and delete re-uploads to the same key on restart. Only
complete triples are pending; staging *.tmp and partial sets are invisible
to the uploader.
"""

import base64
import hashlib
import os
import signal
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from scripts.storage.s3_cost import CountingS3

try:
    from scripts.storage.storage_metrics import bucket_inventory as _bucket_inventory
except ImportError:  # inventory helper absent: uploads unaffected
    _bucket_inventory = None

_S3_WRAPPED = None  # registered CountingS3 renderer; None = not yet wrapped
_S3_SOURCE = None  # underlying client already wrapped; reuse avoids double hooks

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
            wrapped = _S3_WRAPPED
        body = "".join("raw_uploader_%s %s\n" % kv for kv in sorted(snap.items()))
        if wrapped is not None:
            body += wrapped.render_metrics()
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
    return code in ("404", "NoSuchKey", "NotFound")


def sha256_b64_of_hex(digest):
    """Base64 ChecksumSHA256 for an S3 PUT from a hex sha256 digest."""
    return base64.b64encode(bytes.fromhex(digest)).decode("ascii")


def _checksum_ok(resp, want_b64):
    """True only when the server reports the expected full-object checksum."""
    return bool(want_b64) and (resp or {}).get("ChecksumSHA256") == want_b64


def _head(client, bucket, key):
    """HEAD with ChecksumMode ENABLED (MinIO-proven: else no checksum).

    A client lacking the kwarg raises TypeError -> IOError so the leg
    fails closed explicitly (locals kept, never metadata-only trust).
    """
    try:
        return client.head_object(Bucket=bucket, Key=key,
                                  ChecksumMode="ENABLED")
    except TypeError:
        raise IOError("HEAD ChecksumMode unsupported for %s" % key)


def _remote_matches(resp, size, digest):
    """True only when HEAD carries size, metadata digest AND server checksum.

    Metadata sha256 alone never verifies bytes: reuse demands the server's
    own ChecksumSHA256 for the same local digest. Legacy/checksum-less
    objects fail closed (kept, never trusted).
    """
    try:
        if int(resp.get("ContentLength")) != size:
            return False
    except (TypeError, ValueError):
        return False
    meta = resp.get("Metadata") or {}
    # No digest recorded (legacy object): fail closed, never trust.
    if meta.get("sha256") != digest:
        return False
    return _checksum_ok(resp, sha256_b64_of_hex(digest))


def _put_checked(client, bucket, key, path, size, digest):
    """PUT with ChecksumSHA256; response echo required, no GET readback."""
    want = sha256_b64_of_hex(digest)
    with open(path, "rb") as fh:
        resp = client.put_object(Bucket=bucket, Key=key, Body=fh,
                                 ContentLength=size,
                                 Metadata={"sha256": digest},
                                 ChecksumSHA256=want) or {}
    if not _checksum_ok(resp, want):
        raise IOError("missing server checksum on PUT %s" % key)


def wrap_client(client):
    """Always wrap in CountingS3; reuse the same wrapper across run/restart.

    s3_cost is a required packaged dependency (compose mounts it): import
    failures fail startup honestly instead of acking unmetered uploads.
    """
    global _S3_WRAPPED, _S3_SOURCE
    if isinstance(client, CountingS3):
        _S3_WRAPPED, _S3_SOURCE = client, getattr(client, "_client", client)
        return client
    if _S3_WRAPPED is not None and _S3_SOURCE is client:
        return _S3_WRAPPED
    _S3_WRAPPED = CountingS3(client, "raw")
    _S3_SOURCE = client
    return _S3_WRAPPED


def upload_verified(client, bucket, key, path, digest, max_retries, *, size=None):
    """Verified write to a content-addressed key: never overwrites truth.

    Triple uploads pass the preflight size; standalone callers may omit it.
    Pre-check HEAD first: size + Metadata sha256 + server ChecksumSHA256
    reuses the live object; any mismatch (including missing checksum or
    missing digest) is a conflict (kept, never overwritten); any other
    HEAD error is unreadable (retried, never mistaken for missing). Only
    genuine missing-key permits PUT with ChecksumSHA256; the PUT response
    must echo the checksum and the post-PUT HEAD must report it. Missing
    or unsupported server checksums fail closed explicitly -- locals kept,
    never a GET body readback fallback. True only on verified HEAD; any
    False keeps all locals (run_once deletes only on full-triple ack).
    # ponytail: key safety rests on content addressing, not PUT atomicity --
    concurrent compliant writers sharing a key hold identical triple bytes
    (same manifest digest), so a plain S3 PUT is safe; noncompliant external
    writers must not share the archive prefix.
    """
    if size is None:
        size = os.path.getsize(path)
    delay = 2.0
    for attempt in range(max_retries + 1):
        try:
            try:
                existing = _head(client, bucket, key)
            except Exception as hexc:
                if not _is_missing(hexc):
                    raise
                existing = None
            if existing is not None:
                if _remote_matches(existing, size, digest):
                    return True
                minc("dropped_verify")
                print("immutable conflict: live object differs, kept",
                      file=sys.stderr)
                return False
            _put_checked(client, bucket, key, path, size, digest)
            if _remote_matches(_head(client, bucket, key), size, digest):
                return True
            minc("dropped_verify")  # HEAD mismatch: keep local, retry
            print("verify mismatch (attempt %d)" % attempt, file=sys.stderr)
        except Exception as ex:
            print("upload attempt %d: %s" % (attempt, type(ex).__name__),
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
    with open(manifest_path, "rb") as fh:
        blob = fh.read()
    return _json.loads(blob), blob


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


def _preflight_chunk_bytes():
    raw = os.environ.get("RAW_PREFLIGHT_CHUNK_BYTES", "")
    try:
        n = int(raw) if raw else (1 << 20)
    except ValueError:
        raise SystemExit("invalid integer RAW_PREFLIGHT_CHUNK_BYTES=%r"
                         % (raw,))
    return max(4096, n)


def gunzip_compare(sidecar_path, want_digest, closed_path=None,
                   chunk_bytes=None):
    """Streamed sidecar verification with bounded chunks: incremental
    gunzip + SHA-256, never a proportional bytes object. Multi-member
    gzip verifies every member (GzipFile streams members in order);
    truncated members raise and fail closed. When closed_path is present,
    the gunzipped stream must equal the closed JSONL byte-for-byte,
    compared chunk-by-chunk. Returns (ok, reason); ok only on full-stream
    digest equality."""
    import gzip as _gzip
    if chunk_bytes is None:
        chunk_bytes = _preflight_chunk_bytes()
    h = hashlib.sha256()
    closed_fh = None
    if closed_path is not None:
        try:
            closed_fh = open(closed_path, "rb")
        except OSError as ex:
            return False, "closed unreadable: %s" % (ex,)
    closed_len = 0
    try:
        with open(sidecar_path, "rb") as raw:
            try:
                with _gzip.GzipFile(fileobj=raw, mode="rb") as gz:
                    while True:
                        plain = gz.read(chunk_bytes)
                        if not plain:
                            break
                        h.update(plain)
                        if closed_fh is not None:
                            pos = 0
                            while pos < len(plain):
                                have = closed_fh.read(len(plain) - pos)
                                if not have:
                                    return False, ("closed JSONL drifted"
                                                   " from sidecar")
                                if plain[pos:pos + len(have)] != have:
                                    return False, ("closed JSONL drifted"
                                                   " from sidecar")
                                pos += len(have)
                                closed_len += len(have)
            except (OSError, EOFError) as ex:
                return False, "sidecar gunzip failed: %s" % (ex,)
    finally:
        if closed_fh is not None:
            try:
                closed_fh.close()
            except OSError:
                pass
    if h.hexdigest() != want_digest:
        return False, "sidecar content drifted from ingress pin"
    if closed_path is not None:
        try:
            with open(closed_path, "rb") as fh:
                fh.seek(closed_len)
                if fh.read(1):
                    return False, "closed JSONL drifted from sidecar"
        except OSError as ex:
            return False, "closed unreadable: %s" % (ex,)
    return True, ""


def fail_closed_check(mf4_path, sidecar_path, manifest_path, closed_path,
                      snap=None):
    """Return (ok, reason, snapshot) for the complete sealed triple.

    Parse the exact manifest bytes once, pin its address and the validated
    MF4/sidecar digests and sizes. Passing that snapshot rechecks the same
    pins without parsing a replacement manifest or adopting changed bytes.
    gunzip_compare also preserves the closed-ingress byte-for-byte guard.
    Only the small manifest is buffered; payload checks remain streamed.
    """
    try:
        if snap is None:
            man, blob = _load_manifest(manifest_path)
            if not isinstance(man, dict):
                return False, "manifest is not an object", None
            for field in ("mf4_sha256", "ingress_sha256",
                          "ingress_sidecar_sha256"):
                if not man.get(field):
                    return False, "manifest missing " + field, None
            snap = {
                "manifest_bytes": blob,
                "manifest_digest": hashlib.sha256(blob).hexdigest(),
                "mf4_digest": man["mf4_sha256"],
                "sidecar_digest": man["ingress_sidecar_sha256"],
                "ingress_digest": man["ingress_sha256"],
                "sizes": (os.path.getsize(mf4_path),
                          os.path.getsize(sidecar_path), len(blob)),
            }
        else:
            with open(manifest_path, "rb") as fh:
                if fh.read() != snap["manifest_bytes"]:
                    return False, "manifest drifted after preflight", None
        for path, size, digest, label in (
                (mf4_path, snap["sizes"][0], snap["mf4_digest"], "MF4"),
                (sidecar_path, snap["sizes"][1], snap["sidecar_digest"],
                 "sidecar")):
            if os.path.getsize(path) != size or sha256_file(path) != digest:
                return False, "sealed %s drifted from manifest pin" % label, None
        ok, reason = gunzip_compare(sidecar_path, snap["ingress_digest"],
                                    closed_path)
    except (OSError, ValueError) as ex:
        return False, "sealed triple unreadable: %s" % (ex,), None
    if not ok:
        return False, reason, None
    return True, "", snap


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
        ok, reason, snap = fail_closed_check(mf4_path, sidecar_path,
                                            manifest_path, closed_path)
        if not ok:
            print("preflight rejected: %s (quarantined, originals kept)"
                  % reason.split(":", 1)[0], file=sys.stderr)
            quarantine(spool, [mf4_path, sidecar_path, manifest_path,
                               closed_path])
            minc("failed")
            continue
        # Fixed preflight pins: the key and every PUT checksum reuse the
        # validated digests, never a drifted re-hash (a correct server
        # rejects a drifted body under the pinned ChecksumSHA256).
        key = remote_key(mf4_path, vehicle, snap["manifest_digest"])
        legs = [(key, mf4_path, snap["mf4_digest"], snap["sizes"][0]),
                (sidecar_key(key), sidecar_path, snap["sidecar_digest"],
                 snap["sizes"][1]),
                (manifest_key(key), manifest_path, snap["manifest_digest"],
                 snap["sizes"][2])]
        acked = True
        for k, p, digest, size in legs:
            if not upload_verified(client, bucket, k, p, digest, max_retries,
                                   size=size):
                minc("failed")  # any leg fails: ALL locals stay
                acked = False
                break
        if not acked:
            continue
        # Final revalidation (same check, no second convention): drift
        # after preflight -- including replay reuse with no PUT -- must
        # never delete changed locals or adopt a replacement manifest.
        ok, reason, _ = fail_closed_check(mf4_path, sidecar_path,
                                         manifest_path, closed_path,
                                         snap=snap)
        if not ok:
            print("final guard rejected: %s (quarantined, originals kept)"
                  % reason.split(":", 1)[0], file=sys.stderr)
            quarantine(spool, [mf4_path, sidecar_path, manifest_path,
                               closed_path])
            minc("failed")
            continue
        for p in [mf4_path, sidecar_path, manifest_path, closed_path]:
            if p is None:
                continue
            try:
                os.unlink(p)
            except OSError:
                pass
        minc("uploaded_files")
        minc("uploaded_bytes", sum(snap["sizes"]))
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
    client = wrap_client(make_client())
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
            print("sweep error: %s" % type(ex).__name__, file=sys.stderr)
        for _ in range(interval * 2):
            if stop:
                break
            time.sleep(0.5)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
