#!/usr/bin/env python3
"""Offline backup / restore for GreptimeDB (local tar + S3 SST snapshot).

Commands (operator runs only these; `.env` holds all settings):
  backup    offline snapshot -> local tar -> verified S3 upload -> prune
  restore   fetch archive (local or S3) -> verify -> restore SST + volumes
  list      local archives (+ remote COMPLETE backup ids when S3 configured)
  retention evaluate remote generations (dry-run default; explicit approve to delete)

Procedure:
  stop:    docker compose --profile server stop
  backup:  BACKUP_OFFLINE_CONFIRMED=1 docker compose --profile backup run --rm backup backup
  restore: BACKUP_OFFLINE_CONFIRMED=1 docker compose --profile backup run --rm backup restore
  up:      docker compose --profile server up -d
Disaster recovery on the same host uses a fresh project name
(COMPOSE_PROJECT_NAME=<fresh>) so server volumes start empty by construction;
never `docker volume rm` live data. In S3 mode restore auto-downloads the
archive from S3, so a fresh project needs only `.env` + compose files.
File-mode same-host recovery sets BACKUP_VOLUME_NAME to the existing backup
volume and BACKUP_VOLUME_EXTERNAL=true so recovery cannot delete that volume.

Offline precondition (enforced, no docker socket): BACKUP_OFFLINE_CONFIRMED=1
must be set AND GREPTIME_HTTP_URL must be unreachable. A reachable DB fails
backup AND restore with exit 2. There is no live-backup mode: a tar of a
running standalone's volumes is torn WAL, not crash-consistent.

Consistency truth: with GREPTIME_STORAGE_TYPE=S3 the objects under
s3://<S3_BUCKET>/<S3_ROOT>/ are live state. A local tar alone is NOT a
point-in-time snapshot: after the DB resumes, compaction/GC may delete SSTs
the local files referenced. A restorable S3 backup is all of one backup id:
the local tar (WAL/metadata, embeds manifest.json incl. the SST snapshot
record) + the immutable SST snapshot prefix
s3://<bucket>/<S3_BACKUP_PREFIX>/<backup-id>/sst/ + the verified off-host tar
copy + a COMPLETE marker carrying tar_sha256/sidecar_sha256/manifest_sha256
and the full SST digest list. Every restore path (downloaded AND local tars)
is refused unless the tar, sidecar, embedded manifest, snapshot objects and
COMPLETE record all agree. Retention of the backup prefix is a dependency of
restore: never set lifecycle expiry on it; besides its own failed-run writes
(rollback), this tool deletes remote objects only via explicit retention
enforce+approval (refs/RETIRING-guarded generation retirement). The raw MF4
bucket is separate and never touched. With GREPTIME_STORAGE_TYPE=File the local
tar is the whole backup (same offline/tar/restore contract, no S3).

Content verification (never ETag): object copies and uploads are verified by
streaming SHA256 GETs in 1 MiB chunks (multipart CopyObject ETags differ
legitimately, so ETags are never compared). Large objects are never read
wholly into memory: the tar downloads stream to a .tmp file with incremental
hashing and only atomically replace into place after the COMPLETE digest
matches. Ownership/rollback: the backup id namespace is proven empty before
the first write, every destination key is registered BEFORE its write, and
any failure deletes the whole owned set including a half-written COMPLETE
marker (no dangling COMPLETE can point at rolled-back objects).
Backup ids are time-sortable with a random suffix; an occupied remote
namespace is never written into. Restores require the SAME root —
cross-root migration is not claimed.

S3 backup order: offline check -> claim fresh id (local + remote namespace
empty) -> copy each live object to the snapshot prefix (per-object streaming
SHA verify; failure deletes the owned set) -> write local tar (0600) +
.sha256 sidecar -> upload tar + sidecar + manifest copy (GET-and-hash
verified) -> write COMPLETE marker (tracked, verified) -> only then prune
old LOCAL generations (never the file just written; remote objects are never
pruned). Overlapping live/backup prefixes fail closed. Missing S3 settings
fail closed: a local-only S3 tar is unrestorable, so it is not written.

S3 restore order: offline check -> resolve archive locally, else download the
selected (BACKUP_FILE id/name) or newest COMPLETE remote backup (exact
top-level basename match, stream to .tmp, COMPLETE-digest verify, required
remote sidecar + manifest cross-check, atomic replace) -> local S3 tars must
also carry a sidecar and match their remote COMPLETE record (a local tar
from a failed upload is NOT restorable) -> schema/identity/boundary checks
(no force flag, legacy archives without an SST record refused, duplicate SST
keys refused, snapshot prefix must equal backup-prefix/backup-id/sst
exactly) -> refuse when local targets are non-empty; live SST keys already
matching the manifest size+SHA256 are reused, missing snapshot keys are
copied back and streaming-verified against the manifest digest, conflicting
live contents refuse without overwriting (failure deletes only own writes)
-> staged tar populate; any failure also removes the SST keys just written
and rolls targets back to empty.
Prefixes without COMPLETE are incomplete and ignored (safe to delete by hand).

Secrets: greptime-etc holds credentials (auth/users, S3 secret in
greptimedb.toml), so archives/sidecars are 0600 under umask 077. Restored
credentials are STALE: the next db-init/preflight run regenerates
greptimedb.toml + auth/users from the current `.env`. S3 keys are read from
the standard S3_* env and never printed.

Capacity (both modes, separate filesystems): backup preflights 2x the live
source bytes + BACKUP_RESERVE_BYTES (default 64MiB) free on BACKUP_DIR before
any write (a tar of incompressible SSTs peaks near source size). Restore
preflights tar + uncompressed members + reserve free on BACKUP_DIR for staging
and members + reserve free on every restore target filesystem. Shortfalls
refuse BEFORE any write (exit 2); unreadable disks fail closed (exit 3).
Failed runs delete only their own writes; the source is never mutated.

Exit codes: 0 ok, 1 usage/env error, 2 precondition failure, 3 IO error.
"""

import calendar
import glob
from contextlib import closing
import hashlib
import io
import json
import math
import os
import re
import secrets
import shutil
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from scripts.database.greptime_preflight import derive_b2_region

BACKUP_PREFIX = "greptime-backup-"
BACKUP_SUFFIX = ".tar.gz"
SIDECAR_SUFFIX = ".sha256"
MANIFEST_NAME = "manifest.json"
COMPLETE_NAME = "COMPLETE"
RETIRING_NAME = "RETIRING"
REFS_DIR = "refs/"
LABELS = ("greptime-data", "greptime-etc")
S3ENV = ("S3_ENDPOINT_URL", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
CHUNK = 1 << 20
# ponytail: bound JSON inventory memory; use a streamed inventory format if 64 MiB is insufficient.
CONTROL_CAP = 64 << 20
SIDECAR_CAP = 1 << 20
ID_TRIES = 5


def env(name, default=""):
    return os.environ.get(name, default)


def fail(msg, code=1):
    sys.stderr.write("backup: error: " + msg + "\n")
    return code


def backup_dir():
    d = env("BACKUP_DIR", "/backup")
    if not os.path.isdir(d):
        return None
    return d


def sources():
    raw = env("BACKUP_SOURCE_DIRS", "/source/greptime-data:/source/greptime-etc")
    return [s for s in raw.split(":") if s]


def restore_base():
    return env("BACKUP_RESTORE_BASE", "/source")


def targets():
    base = restore_base()
    return [(os.path.join(base, label), label) for label in LABELS]


def storage_type():
    return env("GREPTIME_STORAGE_TYPE", "S3")


def current_identity():
    return {
        "storage_type": storage_type(),
        "bucket": env("S3_BUCKET", ""),
        "root": env("S3_ROOT", "greptime"),
        "backup_prefix": env("S3_BACKUP_PREFIX", "greptime-backups"),
    }


def norm_prefix(s):
    return s.strip().strip("/") + "/"


def prefixes_overlap(a, b):
    return a.startswith(b) or b.startswith(a)


def db_reachable(url, timeout=3):
    """True if anything answers HTTP (any status counts as running)."""
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout):
            return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


def offline_error(op):
    if env("BACKUP_OFFLINE_CONFIRMED", "") != "1":
        return fail(op + " refused: set BACKUP_OFFLINE_CONFIRMED=1 only after"
                   " stopping the DB (e.g. docker compose --profile server stop)",
                   code=2)
    url = env("GREPTIME_HTTP_URL", "http://greptimedb:4000")
    if db_reachable(url):
        return fail(op + " refused: DB still reachable at " + url
                    + "; stop the stack first"
                    " (docker compose --profile server stop)", code=2)
    return None


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def find_symlink(srcs):
    for s in srcs:
        for root, dirs, files in os.walk(s, followlinks=False):
            for name in dirs + files:
                if os.path.islink(os.path.join(root, name)):
                    return os.path.join(root, name)
    return None


def require_https_endpoint(value, name):
    """HTTPS-only endpoint with valid host before boto3 sees credentials."""
    parsed = urllib.parse.urlparse((value or "").strip())
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError(name + " must be an https:// URL with a host")
    return value.strip()

def make_s3_client():
    endpoint = require_https_endpoint(env("S3_ENDPOINT_URL"), "S3_ENDPOINT_URL")
    import boto3  # noqa: pinned dep, lazy so import errors stay local
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=endpoint,
        region_name=env("S3_REGION") or derive_b2_region(env("S3_ENDPOINT_URL")) or None,
        aws_access_key_id=env("S3_ACCESS_KEY_ID") or None,
        aws_secret_access_key=env("S3_SECRET_ACCESS_KEY") or None,
        config=Config(retries={"max_attempts": 1},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required",
                      connect_timeout=10, read_timeout=60))


def s3_missing():
    return [n for n in S3ENV if not env(n)
            and not (n == "S3_REGION" and derive_b2_region(env("S3_ENDPOINT_URL")))]


def s3_list_all(client, bucket, prefix):
    """All objects under prefix, following pagination (truncation loses SSTs)."""
    out = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        out.extend(resp.get("Contents") or [])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
            if not token:
                raise IOError("s3 listing truncated without continuation token")
        else:
            return out


def s3_stream_hash(client, bucket, key):
    """Streaming SHA256 of a remote object; never holds it wholly in memory."""
    h = hashlib.sha256()
    size = 0
    with closing(client.get_object(Bucket=bucket, Key=key)["Body"]) as body:
        while True:
            chunk = body.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def read_control(body, cap, label):
    """Bound each read and the total retained bytes, even without ContentLength."""
    with io.BytesIO() as buffer:
        while True:
            chunk = body.read(min(CHUNK, cap + 1 - buffer.tell()))
            if not chunk:
                return buffer.getvalue()
            buffer.write(chunk)
            if buffer.tell() > cap:
                raise IOError("control object too large (limit %d bytes): %s" % (cap, label))


def s3_get_control(client, bucket, key, cap=None):
    """Manifest/COMPLETE share the writer's bound; sidecars have a smaller bound."""
    if cap is None:
        cap = CONTROL_CAP
    response = client.get_object(Bucket=bucket, Key=key)
    with closing(response["Body"]) as body:
        if response.get("ContentLength", 0) > cap:
            raise IOError("control object too large (limit %d bytes): %s" % (cap, key))
        return read_control(body, cap, key)


def s3_delete_keys(client, bucket, keys):
    """Delete keys; per-key partial failures raise IOError naming survivors."""
    errors = []
    for i in range(0, len(keys), 1000):
        chunk = [{"Key": k} for k in keys[i:i + 1000]]
        resp = client.delete_objects(Bucket=bucket, Delete={"Objects": chunk})
        for err in (resp or {}).get("Errors") or []:
            errors.append("%s: %s" % (err.get("Key"), err.get("Code", "delete failed")))
    if errors:
        raise IOError("partial delete failed (%d): %s%s"
                      % (len(errors), "; ".join(errors[:5]),
                         "..." if len(errors) > 5 else ""))


def rollback_owned(client, bucket, owned):
    try:
        s3_delete_keys(client, bucket, owned)
    except Exception:
        pass


def s3_copy_content_verified(client, bucket, src, dst):
    """Copy then stream-verify both sides by content hash (never ETag:
    multipart CopyObject ETags legitimately differ from source ETags)."""
    src_sha, src_size = s3_stream_hash(client, bucket, src)
    client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src}, Key=dst)
    dst_sha, dst_size = s3_stream_hash(client, bucket, dst)
    if dst_sha != src_sha or dst_size != src_size:
        raise IOError("copy verify mismatch: %s -> %s" % (src, dst))
    return src_sha, src_size


def s3_put_bytes_verified(client, bucket, key, blob):
    client.put_object(Bucket=bucket, Key=key, Body=blob, ContentLength=len(blob))
    got_sha, got_size = s3_stream_hash(client, bucket, key)
    want = hashlib.sha256(blob).hexdigest()
    if got_sha != want or got_size != len(blob):
        raise IOError("upload verify mismatch: " + key)


def s3_put_file_verified(client, bucket, key, path, digest, size):
    with open(path, "rb") as f:
        client.put_object(Bucket=bucket, Key=key, Body=f,
                          ContentLength=size,
                          Metadata={"sha256": digest})
    got_sha, got_size = s3_stream_hash(client, bucket, key)
    if got_sha != digest or got_size != size:
        raise IOError("upload verify mismatch: " + key)


def snapshot_prefix(backup_prefix, backup_id):
    return norm_prefix(backup_prefix) + backup_id + "/sst/"


def remote_base(backup_prefix, backup_id):
    return norm_prefix(backup_prefix) + backup_id + "/"


def cmd_list():
    d = backup_dir()
    if d is None:
        return fail("BACKUP_DIR does not exist: " + env("BACKUP_DIR", "/backup"), code=2)
    files = sorted(glob.glob(os.path.join(d, BACKUP_PREFIX + "*" + BACKUP_SUFFIX)))
    for f in files:
        sys.stdout.write(os.path.basename(f) + "\n")
    if storage_type() == "S3" and not s3_missing():
        try:
            client = make_s3_client()
            bp = norm_prefix(env("S3_BACKUP_PREFIX", "greptime-backups"))
            ids = remote_complete_ids(client, env("S3_BUCKET"), bp)
            for bid in ids:
                sys.stdout.write("remote: " + bid + "\n")
        except Exception as e:
            return fail("remote list failed: " + str(e)[:200], code=3)
    return 0


def write_local_tar(d, name, srcs, manifest):
    raw_manifest = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
    if len(raw_manifest) > CONTROL_CAP:
        raise IOError("manifest exceeds control size limit (%d bytes)" % CONTROL_CAP)
    path = os.path.join(d, name)
    tmp = path + ".tmp"
    try:
        with tarfile.open(tmp, "w:gz", format=tarfile.PAX_FORMAT) as tar:
            for s in srcs:
                tar.add(s, arcname=os.path.basename(s.rstrip("/")), recursive=True)
            info = tarfile.TarInfo(MANIFEST_NAME)
            info.size = len(raw_manifest)
            info.mode = 0o600
            info.mtime = int(time.time())
            tar.addfile(info, io.BytesIO(raw_manifest))
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
        os.chmod(path, 0o600)
        digest = sha256_file(path)
        with open(path + SIDECAR_SUFFIX, "w", encoding="utf-8") as f:
            f.write(digest + "  " + name + "\n")
        os.chmod(path + SIDECAR_SUFFIX, 0o600)
    except Exception as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise IOError("backup write failed: " + str(e)[:200])
    return path, digest, raw_manifest


def prune_local(d, path, keep, prefix=BACKUP_PREFIX, suffix=BACKUP_SUFFIX,
                sidecar_suffix=SIDECAR_SUFFIX):
    """Delete generations beyond keep, never the file just written."""
    files = sorted(glob.glob(os.path.join(d, prefix + "*" + suffix)))
    for old in files[:-keep]:
        if old != path:
            for victim in (old, old + sidecar_suffix):
                try:
                    os.unlink(victim)
                except FileNotFoundError:
                    pass
                except OSError as e:
                    sys.stderr.write("backup: warning: prune failed: " + victim + ": "
                                     + str(e)[:100] + "\n")


def claim_backup_id(d, s3, bucket, bp, prefix=BACKUP_PREFIX,
                    suffix=BACKUP_SUFFIX, sidecar_suffix=SIDECAR_SUFFIX):
    """Time-sortable id with random suffix, proven fresh locally AND remotely.
    An occupied remote namespace is never written into (fail closed)."""
    for _ in range(ID_TRIES):
        now_ns = time.time_ns()
        stamp = (time.strftime("%Y%m%dT%H%M%S", time.gmtime(now_ns // 1_000_000_000))
                 + ".%09dZ" % (now_ns % 1_000_000_000))
        bid = prefix + stamp + "-" + secrets.token_hex(3)
        if (os.path.exists(os.path.join(d, bid + suffix))
                or os.path.exists(os.path.join(d, bid + suffix + sidecar_suffix))):
            continue
        if s3 is not None:
            if s3_list_all(s3, bucket, remote_base(bp, bid)):
                continue
        return bid, stamp
    raise IOError("could not claim a fresh backup namespace (collision)")


def read_reserve(var="BACKUP_RESERVE_BYTES"):
    """Returns (reserve, errmsg): <var> floor for staging-space preflights."""
    try:
        reserve = int(env(var, str(64 << 20)))
    except ValueError:
        return None, var + " must be an integer"
    if reserve < 0:
        return None, var + " must be >= 0"
    return reserve, None


def tree_bytes(srcs):
    """Summed regular-file bytes under srcs (symlinks already refused)."""
    total = 0
    for s in srcs:
        for root, _dirs, files in os.walk(s, followlinks=False):
            for name in files:
                p = os.path.join(root, name)
                if os.path.islink(p):
                    raise IOError("source symlink appeared mid-backup (refusing): " + p)
                total += os.path.getsize(p)
    return total


def cmd_backup():
    os.umask(0o077)
    err = offline_error("backup")
    if err is not None:
        return err
    d = backup_dir()
    if d is None:
        return fail("BACKUP_DIR does not exist: " + env("BACKUP_DIR", "/backup"), code=2)
    srcs = sources()
    missing = [s for s in srcs if not os.path.isdir(s)]
    if missing:
        return fail("source dir missing: " + ", ".join(missing), code=2)
    link = find_symlink(srcs)
    if link is not None:
        return fail("source contains symlink (unrestorable, refusing): " + link, code=2)
    try:
        keep = int(env("BACKUP_KEEP", "7"))
    except ValueError:
        return fail("BACKUP_KEEP must be an integer", code=1)
    if keep < 1:
        keep = 1
    reserve, rerr = read_reserve()
    if rerr is not None:
        return fail(rerr, code=1)
    try:
        source_bytes = tree_bytes(srcs)
        need = 2 * source_bytes + reserve
        if shutil.disk_usage(d).free < need:
            return fail("backup refused: insufficient space in %s (need %d bytes)"
                        % (d, need), code=2)
    except OSError as e:
        return fail("backup preflight failed: " + str(e)[:200], code=3)
    ident = current_identity()
    s3 = None
    bucket = ""
    bp = ""
    root_pfx = ""
    if ident["storage_type"] == "S3":
        miss = s3_missing()
        if miss:
            return fail("S3 mode needs S3 settings (fail closed: a local-only S3 tar"
                         " is unrestorable): missing " + ", ".join(miss), code=2)
        bucket = env("S3_BUCKET")
        if not env("S3_ROOT", "").strip() or not ident["backup_prefix"].strip():
            return fail("S3 mode needs S3_ROOT and S3_BACKUP_PREFIX set", code=2)
        root_pfx = norm_prefix(env("S3_ROOT", "greptime"))
        bp = norm_prefix(ident["backup_prefix"])
        if prefixes_overlap(root_pfx, bp):
            return fail("backup prefix overlaps live root (would snapshot snapshots): "
                         + bp + " vs " + root_pfx, code=2)
        try:
            s3 = make_s3_client()
        except Exception as e:
            return fail("S3 client failed: " + str(e)[:200], code=3)
    try:
        bid, stamp = claim_backup_id(d, s3, bucket, bp)
    except IOError as e:
        return fail(str(e)[:200], code=3)
    name = bid + BACKUP_SUFFIX
    manifest = {
        "backup_id": bid,
        "created_utc": stamp,
        "storage_type": ident["storage_type"],
        "bucket": ident["bucket"],
        "root": ident["root"],
        "backup_prefix": ident["backup_prefix"],
        "labels": list(LABELS),
        "tool": "backup.py v4",
    }
    owned = []
    snap = None
    snap_objects = []
    if s3 is not None:
        try:
            snap = snapshot_prefix(ident["backup_prefix"], bid)
            seen = set()
            for obj in s3_list_all(s3, bucket, root_pfx):
                rel = obj["Key"][len(root_pfx):]
                if not rel:
                    continue
                if rel in seen:
                    raise IOError("duplicate key in live listing: " + rel)
                seen.add(rel)
                dst = snap + rel
                owned.append(dst)  # registered BEFORE the write
                sha, size = s3_copy_content_verified(s3, bucket, obj["Key"], dst)
                snap_objects.append({"key": rel, "size": size, "sha256": sha})
            manifest["sst_snapshot"] = {
                "bucket": bucket,
                "src_prefix": root_pfx,
                "snap_prefix": snap,
                "objects": snap_objects,
            }
        except Exception as e:
            rollback_owned(s3, bucket, owned)
            return fail("snapshot failed (own writes rolled back): "
                         + str(e)[:200], code=3)
    try:
        path, digest, raw_manifest = write_local_tar(d, name, srcs, manifest)
    except IOError as e:
        if s3 is not None:
            rollback_owned(s3, bucket, owned)
        return fail(str(e)[:300], code=3)
    if s3 is not None:
        rbase = remote_base(ident["backup_prefix"], bid)
        tar_size = os.path.getsize(path)
        try:
            with open(path + SIDECAR_SUFFIX, "rb") as f:
                sidecar = f.read()
            manifest_sha = hashlib.sha256(raw_manifest).hexdigest()
            sidecar_sha = hashlib.sha256(sidecar).hexdigest()
            complete = json.dumps({
                "backup_id": bid, "created_utc": stamp,
                "tar_sha256": digest, "tar_size": tar_size,
                "sidecar_sha256": sidecar_sha,
                "manifest_sha256": manifest_sha,
                "sst_count": len(snap_objects),
                "sst": [{"key": o["key"], "size": o["size"], "sha256": o["sha256"]}
                        for o in snap_objects],
            }, sort_keys=True).encode("utf-8")
            if len(complete) > CONTROL_CAP:
                raise IOError("COMPLETE exceeds control size limit (%d bytes)" % CONTROL_CAP)
            owned.append(rbase + name)  # BEFORE each write
            s3_put_file_verified(s3, bucket, rbase + name, path, digest, tar_size)
            owned.append(rbase + name + SIDECAR_SUFFIX)
            s3_put_bytes_verified(s3, bucket, rbase + name + SIDECAR_SUFFIX, sidecar)
            owned.append(rbase + MANIFEST_NAME)
            s3_put_bytes_verified(s3, bucket, rbase + MANIFEST_NAME, raw_manifest)
            owned.append(rbase + COMPLETE_NAME)  # tracked BEFORE the marker write
            s3_put_bytes_verified(s3, bucket, rbase + COMPLETE_NAME, complete)
        except Exception as e:
            rollback_owned(s3, bucket, owned)
            return fail("off-host upload failed (local tar kept, remote rolled back"
                         " including COMPLETE): " + str(e)[:200], code=3)
        prune_local(d, path, keep)
        try:
            cmd_retention(current_bid=bid)  # off/dry-run/enforce; warnings never fail backup
        except Exception as e:
            sys.stderr.write("backup: warning: retention skipped: " + str(e)[:150] + "\n")
        sys.stdout.write("backup: ok: %s (sst %d, off-host verified)\n"
                         % (name, len(snap_objects)))
    else:
        prune_local(d, path, keep)
        sys.stdout.write("backup: ok: " + name + " (File mode, local only)\n")
    return 0


def is_empty_dir(path):
    try:
        with os.scandir(path) as it:
            for _ in it:
                return False
    except FileNotFoundError:
        return True
    except NotADirectoryError:
        return False
    return True


def validate_members(members):
    seen = set()
    tops = set()
    for m in members:
        name = m.name
        if not name or name in (".", "./"):
            return "empty member name"
        if name.startswith("/") or name.startswith("\\"):
            return "absolute member refused: " + name
        if ".." in name.split("/"):
            return "parent traversal refused: " + name
        if m.issym() or m.islnk():
            return "link member refused: " + name
        if m.isdev() or m.isfifo() or m.ischr() or m.isblk():
            return "special member refused: " + name
        if not (m.isfile() or m.isdir()):
            return "unsupported member type refused: " + name
        if name in seen:
            return "duplicate member refused: " + name
        seen.add(name)
        tops.add(name.split("/")[0])
    allowed = set(LABELS) | {MANIFEST_NAME}
    missing = [label for label in LABELS if label not in tops]
    if missing:
        return ("partial archive refused: missing top level(s) "
                + ",".join(missing) + " (has: " + ",".join(sorted(tops)) + ")")
    extra = sorted(tops - allowed)
    if extra:
        return "unexpected top-level entries refused: " + ",".join(extra)
    if MANIFEST_NAME not in seen:
        return "archive manifest missing (partial/legacy archive refused)"
    return None


def validate_sst_record(obj):
    if not isinstance(obj, dict):
        return "sst record not an object"
    if not isinstance(obj.get("key"), str) or not obj["key"]:
        return "sst record missing key"
    if obj["key"].startswith("/") or ".." in obj["key"].split("/"):
        return "sst record unsafe key: " + obj["key"]
    if not isinstance(obj.get("size"), int) or obj["size"] < 0:
        return "sst record bad size: " + obj["key"]
    if not isinstance(obj.get("sha256"), str) or not SHA_RE.match(obj["sha256"]):
        return "sst record bad sha256: " + obj["key"]
    return None


def validate_manifest(manifest):
    """Schema + identity + snapshot-boundary checks. Fail closed."""
    if not isinstance(manifest, dict):
        return "manifest not an object"
    required = ("backup_id", "created_utc", "storage_type", "root", "backup_prefix", "labels")
    if manifest.get("storage_type") == "S3":
        required += ("bucket",)
    for field in required:
        if field not in manifest or manifest[field] in ("", None, []):
            return "manifest missing field: " + field
    if not isinstance(manifest.get("bucket"), str):
        return "manifest bad bucket"
    if not isinstance(manifest["backup_id"], str):
        return "manifest bad backup_id"
    if list(manifest.get("labels") or []) != list(LABELS):
        return "manifest bad labels (partial/legacy archive refused)"
    ident = current_identity()
    for key in ("storage_type", "bucket", "root", "backup_prefix"):
        if str(manifest.get(key, "")) != ident[key]:
            return ("identity mismatch: archive " + key + "="
                    + repr(manifest.get(key, ""))
                    + " vs current " + repr(ident[key])
                    + " (same-root restore only: refusing to mix generations)")
    if ident["storage_type"] == "S3":
        snap = manifest.get("sst_snapshot")
        if not isinstance(snap, dict):
            return ("legacy archive without SST snapshot record"
                    " (unrestorable in S3 mode: mix of generations refused)")
        if snap.get("bucket") != manifest["bucket"]:
            return "snapshot bucket differs from archive bucket (refusing)"
        if snap.get("src_prefix") != norm_prefix(str(manifest["root"])):
            return ("snapshot src_prefix is not the live root"
                    " (refusing: cross-root migration unproven)")
        want_snap = (norm_prefix(str(manifest["backup_prefix"]))
                     + manifest["backup_id"] + "/sst/")
        if snap.get("snap_prefix") != want_snap:
            return ("snapshot prefix outside this backup namespace"
                    " (refusing): " + repr(snap.get("snap_prefix")))
        objs = snap.get("objects")
        if not isinstance(objs, list):
            return "snapshot objects not a list"
        seen = set()
        for obj in objs:
            problem = validate_sst_record(obj)
            if problem is not None:
                return problem
            if obj["key"] in seen:
                return "duplicate SST key in manifest: " + obj["key"]
            seen.add(obj["key"])
    return None


def validate_complete(complete, bid):
    if not isinstance(complete, dict):
        return "COMPLETE not an object"
    if complete.get("backup_id") != bid:
        return "COMPLETE backup_id mismatch (refusing)"
    for field in ("tar_sha256", "sidecar_sha256", "manifest_sha256"):
        if not isinstance(complete.get(field), str) or not SHA_RE.match(complete[field]):
            return "COMPLETE bad field: " + field
    if not isinstance(complete.get("tar_size"), int) or complete["tar_size"] <= 0:
        return "COMPLETE bad tar_size"
    sst = complete.get("sst")
    if not isinstance(sst, list):
        return "COMPLETE sst not a list"
    seen = set()
    for obj in sst:
        problem = validate_sst_record(obj)
        if problem is not None:
            return "COMPLETE " + problem
        if obj["key"] in seen:
            return "COMPLETE duplicate SST key: " + obj["key"]
        seen.add(obj["key"])
    if complete.get("sst_count") != len(sst):
        return "COMPLETE sst_count mismatch"
    return None


def clear_dir(path):
    with os.scandir(path) as it:
        for entry in it:
            p = os.path.join(path, entry.name)
            try:
                if entry.is_symlink() or entry.is_file(follow_symlinks=False):
                    os.unlink(p)
                elif entry.is_dir(follow_symlinks=False):
                    shutil.rmtree(p)
            except FileNotFoundError:
                pass


def copy_contents(src, dst):
    with os.scandir(src) as it:
        for entry in it:
            dest = os.path.join(dst, entry.name)
            if entry.is_symlink():
                raise IOError("symlink in staging (should have been refused): " + entry.name)
            if entry.is_dir(follow_symlinks=False):
                shutil.copytree(entry.path, dest, symlinks=False)
            elif entry.is_file(follow_symlinks=False):
                shutil.copy2(entry.path, dest)
            else:
                raise IOError("unsupported entry in staging: " + entry.name)


def normalize_want(choice, sidecar_suffix=SIDECAR_SUFFIX, suffix=BACKUP_SUFFIX):
    base = os.path.basename(choice)
    if base.endswith(sidecar_suffix):
        base = base[:-len(sidecar_suffix)]
    if base.endswith(suffix):
        base = base[:-len(suffix)]
    return base


def resolve_local_archive(d, file_var="BACKUP_FILE", prefix=BACKUP_PREFIX,
                          suffix=BACKUP_SUFFIX):
    """Returns (path, want): local tar to use, or remote backup id to fetch."""
    choice = env(file_var, "")
    if choice:
        if os.path.isabs(choice):
            if os.path.isfile(choice):
                return choice, None
            return None, normalize_want(choice)
        direct = os.path.join(d, choice)
        if os.path.isfile(direct):
            return direct, None
        if not choice.endswith(suffix):
            suffixed = direct + suffix
            if os.path.isfile(suffixed):
                return suffixed, None
        return None, normalize_want(choice)
    files = sorted(glob.glob(os.path.join(d, prefix + "*" + suffix)))
    if files:
        return files[-1], None
    return None, None


def read_sidecar_digest(path):
    """Require an archive-bound SHA256 sidecar in every storage mode."""
    sidecar = path + SIDECAR_SUFFIX
    if not os.path.isfile(sidecar):
        return None, "backup sidecar required but missing: " + sidecar
    try:
        with open(sidecar, "rb") as f:
            parts = read_control(f, SIDECAR_CAP, sidecar).decode("utf-8").split()
        if len(parts) != 2 or not SHA_RE.match(parts[0]):
            return None, "backup sidecar malformed: " + sidecar
        if parts[1] != os.path.basename(path):
            return None, "backup sidecar names another archive: " + sidecar
        return parts[0], None
    except (OSError, UnicodeError):
        return None, "backup sidecar unreadable or oversized: " + sidecar


def load_manifest(path):
    """Returns (manifest, raw_bytes, members, errmsg, errcode): torn archives
    are code 3, policy refusals are code 2."""
    try:
        with tarfile.open(path, "r:gz") as tar:
            members = tar.getmembers()
    except Exception as e:
        return None, None, None, "backup unreadable (torn archive?): " + str(e)[:200], 3
    problem = validate_members(members)
    if problem is not None:
        return None, None, None, problem, 2
    try:
        with tarfile.open(path, "r:gz") as tar:
            info = tar.getmember(MANIFEST_NAME)
            if info.size > CONTROL_CAP:
                return None, None, None, "manifest exceeds control size limit (refusing)", 2
            with tar.extractfile(MANIFEST_NAME) as stream:
                raw = read_control(stream, CONTROL_CAP, MANIFEST_NAME)
            if len(raw) != info.size:
                return None, None, None, "manifest short read (torn archive?)", 3
        manifest = json.loads(raw.decode("utf-8"))
    except Exception as e:
        return None, None, None, "backup manifest unreadable: " + str(e)[:150], 2
    problem = validate_manifest(manifest)
    if problem is not None:
        return None, None, None, problem, 2
    return manifest, raw, members, None, 0


def ensure_targets_empty():
    for target, _label in targets():
        parent = os.path.dirname(target)
        if parent and not os.path.isdir(parent):
            return "mount missing: " + target
        os.makedirs(target, exist_ok=True)
        if not is_empty_dir(target):
            return ("refusing to overwrite non-empty " + target
                    + " (no force flag: restore into fresh volumes first)")
    return None


def staged_populate(d, path):
    staging = tempfile.mkdtemp(dir=d, prefix=".restore-")
    try:
        with tarfile.open(path, "r:gz") as tar:
            try:
                tar.extractall(path=staging, filter="data")
            except TypeError:
                tar.extractall(path=staging)
        for _target, label in targets():
            if not os.path.isdir(os.path.join(staging, label)):
                raise IOError("staging missing top level: " + label)
        for target, label in targets():
            copy_contents(os.path.join(staging, label), target)
    except Exception as e:
        for target, _label in targets():
            try:
                clear_dir(target)
            except OSError:
                pass
        raise IOError("restore failed, rolled back to empty: " + str(e)[:200])
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def remote_complete_ids(client, bucket, bp):
    objs = s3_list_all(client, bucket, bp)
    return sorted({o["Key"][len(bp):].split("/")[0]
                   for o in objs if o["Key"].endswith("/" + COMPLETE_NAME)})


def fetch_complete(client, bucket, bp, bid):
    """Read + schema-check the COMPLETE record for one backup id."""
    rbase = remote_base(bp, bid)
    try:
        raw = s3_get_control(client, bucket, rbase + COMPLETE_NAME)
    except Exception as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if isinstance(e, KeyError) or code in ("NoSuchKey", "404", "NotFound"):
            return None, ("no COMPLETE record for %s (incomplete backup or failed"
                          " upload: not restorable)" % bid)
        return None, "COMPLETE record unreadable for %s (refusing): %s" % (bid, str(e)[:200])
    try:
        complete = json.loads(raw.decode("utf-8"))
    except Exception:
        return None, "COMPLETE record corrupt for %s (refusing)" % bid
    problem = validate_complete(complete, bid)
    if problem is not None:
        return None, problem
    return complete, None


def check_against_complete(path, side_digest, raw_manifest, sst_objs, complete):
    """Tar + sidecar + embedded manifest + SST list must all match COMPLETE."""
    if sha256_file(path) != complete["tar_sha256"]:
        return "tar hash differs from COMPLETE record (refusing)"
    if os.path.getsize(path) != complete["tar_size"]:
        return "tar size differs from COMPLETE record (refusing)"
    if side_digest != complete["tar_sha256"]:
        return "sidecar digest differs from COMPLETE record (refusing)"
    with open(path + SIDECAR_SUFFIX, "rb") as stream:
        side_raw = read_control(stream, SIDECAR_CAP, path + SIDECAR_SUFFIX)
    if hashlib.sha256(side_raw).hexdigest() != complete["sidecar_sha256"]:
        return "sidecar bytes differ from COMPLETE record (refusing)"
    if hashlib.sha256(raw_manifest).hexdigest() != complete["manifest_sha256"]:
        return "embedded manifest differs from COMPLETE record (refusing)"
    want = sorted((o["key"], o["size"], o["sha256"]) for o in sst_objs)
    got = sorted((o["key"], o["size"], o["sha256"]) for o in complete["sst"])
    if want != got:
        return "SST list differs from COMPLETE record (refusing)"
    return None


def download_remote_backup(d, client, bucket, bp, want):
    """Fetch a COMPLETE backup id. Exact top-level basename match only;
    registers a refs/ reader lease first (conditional: COMPLETE present,
    no RETIRING), streams to .tmp, verifies against COMPLETE, then
    atomically replaces. Returns (path, complete, ref_key_or_errmsg):
    ref_key on success (caller releases after verify), errmsg when path
    is None. The caller releases the ref on every path after use."""
    ids = remote_complete_ids(client, bucket, bp)
    if not ids:
        return None, None, "no COMPLETE backups under s3://%s/%s" % (bucket, bp)
    bid = want if want in ids else (ids[-1] if not want else None)
    if bid is None:
        return None, None, "remote backup not found: %s (have: %s)" % (want, ",".join(ids))
    try:
        ref_key = acquire_generation_ref(client, bucket, bp, bid)
    except IOError as e:
        return None, None, str(e)[:250]
    complete, cerr = fetch_complete(client, bucket, bp, bid)
    if cerr is not None:
        release_generation_ref(client, bucket, ref_key)
        return None, None, cerr
    rbase = remote_base(bp, bid)
    expected_tar = bid + BACKUP_SUFFIX
    top = set()
    for o in s3_list_all(client, bucket, rbase):
        rel = o["Key"][len(rbase):]
        if not rel or "/" in rel:
            continue  # nested inventory (e.g. sst/ tree) never an archive
        top.add(rel)
    if expected_tar not in top:
        release_generation_ref(client, bucket, ref_key)
        return None, None, ("remote backup %s has no exact top-level archive %s"
                             % (bid, expected_tar))
    if expected_tar + SIDECAR_SUFFIX not in top:
        release_generation_ref(client, bucket, ref_key)
        return None, None, ("remote backup %s missing sidecar (refusing)" % bid)
    if MANIFEST_NAME not in top:
        release_generation_ref(client, bucket, ref_key)
        return None, None, ("remote backup %s missing manifest copy (refusing)" % bid)
    path = os.path.join(d, expected_tar)
    tmp = path + ".tmp"
    try:
        h = hashlib.sha256()
        size = 0
        with closing(client.get_object(Bucket=bucket, Key=rbase + expected_tar)["Body"]) as body:
            with open(tmp, "wb") as f:
                while True:
                    chunk = body.read(CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
        if h.hexdigest() != complete["tar_sha256"] or size != complete["tar_size"]:
            raise IOError("downloaded tar differs from COMPLETE record (refusing)")
        side_raw = s3_get_control(client, bucket, rbase + expected_tar + SIDECAR_SUFFIX,
                                  SIDECAR_CAP)
        parts = side_raw.decode("utf-8").split()
        if len(parts) != 2 or parts[0] != complete["tar_sha256"]:
            raise IOError("remote sidecar differs from COMPLETE record (refusing)")
        if hashlib.sha256(side_raw).hexdigest() != complete["sidecar_sha256"]:
            raise IOError("remote sidecar bytes differ from COMPLETE record (refusing)")
        remote_manifest = s3_get_control(client, bucket, rbase + MANIFEST_NAME)
        if hashlib.sha256(remote_manifest).hexdigest() != complete["manifest_sha256"]:
            raise IOError("remote manifest differs from COMPLETE record (refusing)")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
        os.chmod(path, 0o600)
        with open(path + SIDECAR_SUFFIX, "wb") as f:
            f.write(side_raw)
        os.chmod(path + SIDECAR_SUFFIX, 0o600)
    except IOError as e:
        release_generation_ref(client, bucket, ref_key)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None, None, str(e)[:250]
    except Exception as e:
        release_generation_ref(client, bucket, ref_key)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None, None, "remote download failed: " + str(e)[:200]
    return path, complete, ref_key


def s3_restore_snapshot(s3, bucket, root_pfx, snap, objs):
    """Restore snapshot objects under the live root, reusing pre-existing
    keys whose size+SHA256 already match the manifest. Only missing keys
    are copied (each verified against the manifest digest); a pre-existing
    key with different contents is a conflict and refuses without
    overwriting anything. Each destination is registered BEFORE its write;
    failure deletes only keys this run actually wrote, never pre-existing
    or unrelated live objects."""
    planned = [root_pfx + o["key"] for o in objs]
    if len(set(planned)) != len(planned):
        raise IOError("duplicate SST destination (refusing)")
    snap_pfx = snap["snap_prefix"]
    written = []
    try:
        live = {o["Key"] for o in s3_list_all(s3, bucket, root_pfx)}
        for obj in objs:
            dst = root_pfx + obj["key"]
            src = snap_pfx + obj["key"]
            if dst in live:
                got_sha, got_size = s3_stream_hash(s3, bucket, dst)
                if got_sha == obj["sha256"] and got_size == obj["size"]:
                    continue  # intact SST already live: reuse, write nothing
                raise IOError("live object conflicts with snapshot (refusing,"
                              " not overwriting): " + obj["key"])
            written.append(dst)  # registered BEFORE the write
            s3.copy_object(Bucket=bucket,
                           CopySource={"Bucket": bucket, "Key": src},
                           Key=dst)
            got_sha, got_size = s3_stream_hash(s3, bucket, dst)
            if got_sha != obj["sha256"] or got_size != obj["size"]:
                raise IOError("restored object differs from manifest digest: "
                              + obj["key"])
    except Exception:
        rollback_owned(s3, bucket, written)
        raise
    return written


def record_restore_verification(d, bid):
    """Last successful offline content verification, not a DB restart check."""
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=d,
                                         prefix=".restore-verification-", suffix=".tmp",
                                         delete=False) as stream:
            tmp = stream.name
            json.dump({"timestamp_seconds": time.time(), "backup_id": bid}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, os.path.join(d, "restore-verification.json"))
    finally:
        if tmp is not None and os.path.exists(tmp):
            os.unlink(tmp)


def cmd_restore():
    os.umask(0o077)
    err = offline_error("restore")
    if err is not None:
        return err
    d = backup_dir()
    if d is None:
        return fail("BACKUP_DIR does not exist: " + env("BACKUP_DIR", "/backup"), code=2)
    path, want = resolve_local_archive(d)
    s3 = None
    complete = None
    ref_key = None
    ident = current_identity()
    if path is None:
        if ident["storage_type"] != "S3":
            if env("BACKUP_FILE", ""):
                return fail("backup file not found: " + env("BACKUP_FILE", ""), code=2)
            return fail("no backup files in " + d, code=2)
        miss = s3_missing()
        if miss:
            return fail("no local archive and S3 download needs: " + ", ".join(miss), code=2)
        try:
            s3 = make_s3_client()
            got = download_remote_backup(
                d, s3, env("S3_BUCKET"), norm_prefix(ident["backup_prefix"]), want)
            path, complete, derr_or_ref = got[0], got[1], got[2]
            if path is None:
                return fail(derr_or_ref, code=2)
            ref_key = derr_or_ref
        except Exception as e:
            return fail("remote download failed: " + str(e)[:200], code=3)
    def _fail_ref(msg, code=1):
        if ref_key is not None and s3 is not None:
            release_generation_ref(s3, env("S3_BUCKET"), ref_key)
        return fail(msg, code=code)
    if not os.path.isfile(path):
        return _fail_ref("backup file not found: " + path, code=2)
    side_digest, serr = read_sidecar_digest(path)
    if serr is not None:
        return _fail_ref(serr, code=2)
    if ident["storage_type"] == "File" and sha256_file(path) != side_digest:
        return _fail_ref("backup hash mismatch (torn/corrupt download?): " + path, code=2)
    manifest, raw_manifest, members, merr, mcode = load_manifest(path)
    if merr is not None:
        return _fail_ref(merr, code=mcode)
    problem = ensure_targets_empty()
    if problem is not None:
        return _fail_ref(problem, code=2)
    reserve, rerr = read_reserve()
    if rerr is not None:
        return _fail_ref(rerr, code=1)
    try:
        members_total = sum(m.size for m in members if m.isfile())
        tar_size = os.path.getsize(path)
        if shutil.disk_usage(d).free < tar_size + members_total + reserve:
            return _fail_ref("restore refused: insufficient staging space in " + d,
                             code=2)
        for target, _label in targets():
            if shutil.disk_usage(target).free < members_total + reserve:
                return _fail_ref("restore refused: insufficient space for restore targets",
                                 code=2)
    except OSError as e:
        return _fail_ref("restore preflight failed: " + str(e)[:200], code=3)
    written_sst = []
    if ident["storage_type"] == "S3":
        miss = s3_missing()
        if miss:
            return _fail_ref("S3 restore needs S3 settings: " + ", ".join(miss), code=2)
        bucket = env("S3_BUCKET")
        root_pfx = norm_prefix(env("S3_ROOT", "greptime"))
        try:
            if s3 is None:
                s3 = make_s3_client()
            if complete is None:
                try:
                    ref_key = acquire_generation_ref(
                        s3, bucket, norm_prefix(ident["backup_prefix"]),
                        manifest["backup_id"])
                except IOError as e:
                    return _fail_ref(str(e)[:250], code=2)
                complete, cerr = fetch_complete(
                    s3, bucket, norm_prefix(ident["backup_prefix"]),
                    manifest["backup_id"])
                if cerr is not None:
                    return _fail_ref(cerr + " (a local tar from a failed upload is"
                                      " not restorable)", code=2)
            problem = check_against_complete(
                path, side_digest, raw_manifest,
                manifest["sst_snapshot"]["objects"], complete)
            if problem is not None:
                return _fail_ref(problem, code=2)
            try:
                written_sst = s3_restore_snapshot(
                    s3, bucket, root_pfx,
                    manifest["sst_snapshot"], manifest["sst_snapshot"]["objects"])
            except IOError as e:
                if "conflicts with snapshot" in str(e):
                    return _fail_ref(str(e)[:250], code=2)
                return _fail_ref("SST restore failed (own writes rolled back): "
                                 + str(e)[:250], code=3)
        except IOError as e:
            return _fail_ref(str(e)[:300], code=3)
        except Exception as e:
            return _fail_ref("SST restore failed (own writes rolled back): "
                             + str(e)[:200], code=3)
    try:
        staged_populate(d, path)
        record_restore_verification(d, manifest["backup_id"])
    except IOError as e:
        for target, _label in targets():
            try:
                clear_dir(target)
            except OSError:
                pass
        if s3 is not None and written_sst:
            rollback_owned(s3, env("S3_BUCKET"), written_sst)
        return _fail_ref(str(e)[:300], code=3)
    if ref_key is not None and s3 is not None:
        release_generation_ref(s3, env("S3_BUCKET"), ref_key)
    sys.stdout.write("backup: restore ok: " + os.path.basename(path) + "\n"
                     + "backup: note: restored greptime-etc credentials are stale;"
                     " next preflight regenerates them from the current .env\n")
    return 0


def retention_settings(current_bid=None):
    """Parse remote-retention env. Returns (settings, errmsg). Off/dry-run/enforce."""
    mode = env("BACKUP_REMOTE_RETENTION_MODE", "off").strip().lower() or "off"
    if mode not in ("off", "dry-run", "enforce"):
        return None, "BACKUP_REMOTE_RETENTION_MODE must be off|dry-run|enforce"
    try:
        count = int(env("BACKUP_REMOTE_RETAIN_COUNT", "7"))
        minimum = int(env("BACKUP_REMOTE_MIN_RECOVERY", "1"))
        grace = int(env("BACKUP_REMOTE_INCOMPLETE_GRACE_SEC", "86400"))
        days = float(env("BACKUP_REMOTE_RETAIN_DAYS", "0"))
    except ValueError:
        return None, "retention count/min/grace/days must be numbers"
    if not math.isfinite(days):
        return None, "retention days must be a finite number"
    if count < 1 or minimum < 1 or grace < 0 or days < 0:
        return None, "retention count/minimum >= 1, grace/days >= 0"
    pinned = {p.strip() for p in env("BACKUP_REMOTE_PINNED", "").split(",") if p.strip()}
    for pin in sorted(pinned):
        if "/" in pin or pin in (".", "..") or pin != os.path.basename(pin):
            return None, "pinned id escapes backup scope (refusing): " + pin
    return ({"mode": mode, "count": count, "days": days, "minimum": minimum,
             "grace": grace, "pinned": pinned, "current": current_bid}, None)


def complete_created_ns(complete, bid, now_ns=None):
    """COMPLETE created_utc -> epoch ns. Unparseable or future-dated means
    corrupt (fail closed): clock-skewed markers must not drive age policy."""
    try:
        stamp = complete["created_utc"]
        dt, frac = stamp.split(".")
        secs = calendar.timegm(time.strptime(dt, "%Y%m%dT%H%M%S"))
        created = secs * 1_000_000_000 + int(frac.rstrip("Z"))
    except Exception:
        raise IOError("COMPLETE corrupt created_utc for %s (refusing)" % bid)
    if now_ns is not None and created > now_ns:
        raise IOError("COMPLETE created_utc in the future for %s (refusing)" % bid)
    return created


def remote_generations(client, bucket, bp, now_ns=None):
    """Group objects under bp into verified recovery generations. Fail closed:
    corrupt COMPLETE -> generation marked corrupt (never a deletion target,
    never counted as recovery); missing sidecar/manifest/SST bytes -> incomplete.
    Returns (generations, problems) sorted oldest-first by COMPLETE created_utc."""
    if not bp or not bp.strip("/"):
        raise IOError("backup prefix missing or empty (refusing to scope deletions)")
    objs = s3_list_all(client, bucket, bp)
    by_id = {}
    for o in objs:
        rel = o["Key"][len(bp):]
        bid = rel.split("/")[0]
        if bid:
            by_id.setdefault(bid, []).append(o)
    if now_ns is None:
        now_ns = time.time_ns()
    gens = []
    for bid in sorted(by_id):
        keys = {o["Key"] for o in by_id[bid]}
        rbase = remote_base(bp, bid)
        refs = sorted(k for k in keys if k.startswith(rbase + REFS_DIR))
        retiring = rbase + RETIRING_NAME in keys
        complete, cerr = fetch_complete(client, bucket, bp, bid)
        base = {"backup_id": bid,
                "keys": sorted(keys), "bytes": sum(o.get("Size", 0) for o in by_id[bid]),
                "refs": refs, "retiring": retiring}
        if cerr is not None:
            gens.append(dict(base, status="incomplete", reason=cerr,
                            complete=None, created_ns=None))
            continue
        try:
            created = complete_created_ns(complete, bid, now_ns)
        except IOError as e:
            gens.append(dict(base, status="corrupt", reason=str(e),
                            complete=complete, created_ns=None))
            continue
        want_top = {bid + BACKUP_SUFFIX, bid + BACKUP_SUFFIX + SIDECAR_SUFFIX,
                    MANIFEST_NAME, COMPLETE_NAME}
        top = {k[len(rbase):] for k in keys if k.startswith(rbase) and "/" not in k[len(rbase):]}
        missing_top = want_top - top
        sizes = {o["Key"]: o.get("Size") for o in by_id[bid]}
        want_sst = {rbase + "sst/" + o["key"] for o in complete["sst"]}
        missing_sst = sorted(want_sst - keys)
        size_mismatch = sorted(o["key"] for o in complete["sst"]
                               if rbase + "sst/" + o["key"] in keys
                               and sizes.get(rbase + "sst/" + o["key"]) != o["size"])
        if missing_top or missing_sst or size_mismatch:
            gens.append(dict(base, status="incomplete",
                            reason="missing " + ",".join(sorted(missing_top | set(missing_sst[:3])))
                            + ("; size mismatch " + ",".join(size_mismatch[:3]) if size_mismatch else ""),
                            complete=complete, created_ns=created))
            continue
        gens.append(dict(base, status="complete", reason="",
                         complete=complete, created_ns=created))
    gens.sort(key=lambda g: (g["created_ns"] is None, g["created_ns"] or 0, g["backup_id"]))
    return gens, None


def retention_plan(gens, settings, now_ns=None):
    """Select deletion candidates among COMPLETE generations only. Never deletes:
    live root (not listed here), outside-prefix keys (never listed), pinned,
    the just-written id, newest `count`, younger-than-days, newest `minimum`,
    referenced (active restore/backup reader) or retiring generations.
    A retiring generation is an interrupted prior delete: it is resumed by
    name (not re-planned) so the planner never double-selects it.
    Returns (keep_ids, delete_gens, skipped)."""
    if now_ns is None:
        now_ns = time.time_ns()
    complete = [g for g in gens if g["status"] == "complete"]
    keep = set()
    for g in complete[-settings["count"]:]:
        keep.add(g["backup_id"])
    if settings["days"] > 0:
        cutoff = now_ns - int(settings["days"] * 86400 * 1_000_000_000)
        for g in complete:
            if g["created_ns"] is not None and g["created_ns"] >= cutoff:
                keep.add(g["backup_id"])
    for g in complete[-settings["minimum"]:]:
        keep.add(g["backup_id"])
    keep |= (settings["pinned"] & {g["backup_id"] for g in complete})
    if settings["current"]:
        keep.add(settings["current"])
    deletes, skipped = [], []
    for g in complete:
        if g.get("retiring"):
            skipped.append((g["backup_id"], "retiring: resume by retry (not re-planned)"))
        elif g.get("refs"):
            skipped.append((g["backup_id"], "referenced by %d active reader(s): refusing" % len(g["refs"])))
        elif g["backup_id"] in keep:
            skipped.append((g["backup_id"], "retained"))
        else:
            deletes.append(g)
    for g in gens:
        if g["status"] != "complete":
            skipped.append((g["backup_id"], g["status"] + ": " + g["reason"][:120]))
    return sorted(keep), deletes, skipped

def generation_ref_key(bp, bid, token):
    return remote_base(bp, bid) + REFS_DIR + token


def acquire_generation_ref(client, bucket, bp, bid, token=None):
    """Register an active reader (restore/backup verify) of one generation.

    Two-phase handshake over strongly consistent S3 reads: precheck
    (COMPLETE present, no RETIRING), PUT a unique ref, then recheck
    marker + COMPLETE before ANY consumption. A failed recheck releases
    the just-written ref and refuses, so a writer that won the race can
    proceed and the reader never consumes a retiring/retired generation.
    Returns the ref key."""
    rbase = remote_base(bp, bid)
    live = {o["Key"] for o in s3_list_all(client, bucket, rbase)}
    if rbase + COMPLETE_NAME not in live:
        raise IOError("generation %s has no COMPLETE (retired/incomplete: refusing)" % bid)
    if rbase + RETIRING_NAME in live:
        raise IOError("generation %s is retiring (refusing new readers)" % bid)
    token = token or (("%016x" % time.time_ns()) + secrets.token_hex(4))
    if "/" in token or token in (".", ".."):
        raise IOError("ref token escapes generation scope (refusing)")
    key = generation_ref_key(bp, bid, token)
    client.put_object(Bucket=bucket, Key=key, Body=b"ref", ContentLength=3)
    recheck = {o["Key"] for o in s3_list_all(client, bucket, rbase)}
    if rbase + RETIRING_NAME in recheck or rbase + COMPLETE_NAME not in recheck:
        try:
            s3_delete_keys(client, bucket, [key])
        except Exception:
            pass
        raise IOError("generation %s retired during ref acquire (refusing)" % bid)
    return key


def release_generation_ref(client, bucket, key):
    """Release one reader ref. Best effort; missing keys are already gone."""
    try:
        s3_delete_keys(client, bucket, [key])
    except Exception:
        pass


def resume_retiring(client, bucket, bp, bid):
    """Finish an interrupted retirement: re-list live keys (durable identity
    is the REAL remaining objects under the prefix), refuse active refs, then
    delete COMPLETE alone, confirm its absence, and only then delete payload
    before the RETIRING marker. Idempotent."""
    rbase = remote_base(bp, bid)
    live = [o["Key"] for o in s3_list_all(client, bucket, rbase)]
    if not live:
        return 0
    refs = sorted(k for k in live if k.startswith(rbase + REFS_DIR))
    if refs:
        raise IOError("generation %s referenced by %d active reader(s): refusing"
                      % (bid, len(refs)))
    if rbase + COMPLETE_NAME in set(live):
        s3_delete_keys(client, bucket, [rbase + COMPLETE_NAME])
        if rbase + COMPLETE_NAME in {o["Key"] for o in s3_list_all(client, bucket, rbase)}:
            raise IOError("COMPLETE still present for %s (refusing payload delete)" % bid)
    live = [o["Key"] for o in s3_list_all(client, bucket, rbase)]
    payload = sorted(k for k in live if k != rbase + RETIRING_NAME)
    if payload:
        s3_delete_keys(client, bucket, payload)
        live = [o["Key"] for o in s3_list_all(client, bucket, rbase)]
        payload = sorted(k for k in live if k != rbase + RETIRING_NAME)
        if payload:
            raise IOError("retirement incomplete for %s (%d keys remain)" % (bid, len(payload)))
    if rbase + RETIRING_NAME in {o["Key"] for o in s3_list_all(client, bucket, rbase)}:
        s3_delete_keys(client, bucket, [rbase + RETIRING_NAME])
    return len(payload)


def retire_generation(client, bucket, bp, gen):
    """Retire one planned generation with a durable identity. Steps:
    1. re-list live keys (never trust the stale plan listing);
    2. refuse when active refs/RETIRING exist (precheck);
    3. write RETIRING marker (resume identity), then recheck refs after the
    marker before touching COMPLETE: late readers that passed the reader
    precheck see the marker on postcheck, release, and refuse, so the
    writer must abort here, leaving marker+COMPLETE (a safe resume state);
    4. delete COMPLETE alone, confirm its absence;
    5. delete payload, confirm empty, remove RETIRING last.
    Interrupted runs resume via resume_retiring(); reruns are idempotent."""
    bid = gen["backup_id"] if isinstance(gen, dict) else gen
    rbase = remote_base(bp, bid)
    live = {o["Key"] for o in s3_list_all(client, bucket, rbase)}
    if not live:
        return 0  # fully retired already: idempotent rerun
    if rbase + RETIRING_NAME in live:
        return resume_retiring(client, bucket, bp, bid)
    refs = sorted(k for k in live if k.startswith(rbase + REFS_DIR))
    if refs:
        raise IOError("generation %s referenced by %d active reader(s): refusing"
                      % (bid, len(refs)))
    if rbase + COMPLETE_NAME not in live:
        raise IOError("generation %s has no COMPLETE (not a planned delete: refusing)" % bid)
    marker = ("%s\n" % bid).encode("utf-8")
    client.put_object(Bucket=bucket, Key=rbase + RETIRING_NAME, Body=marker,
                      ContentLength=len(marker))
    post = {o["Key"] for o in s3_list_all(client, bucket, rbase)}
    post_refs = sorted(k for k in post if k.startswith(rbase + REFS_DIR))
    if post_refs:
        raise IOError("generation %s referenced by %d active reader(s): aborting"
                      " (marker+COMPLETE preserved for resume)" % (bid, len(post_refs)))
    s3_delete_keys(client, bucket, [rbase + COMPLETE_NAME])
    if rbase + COMPLETE_NAME in {o["Key"] for o in s3_list_all(client, bucket, rbase)}:
        raise IOError("COMPLETE still present for %s (refusing payload delete)" % bid)
    return resume_retiring(client, bucket, bp, bid) + 1


def cmd_retention(current_bid=None):
    """Evaluate remote generations; delete only in enforce+approved mode.
    Prints candidates + expected bytes; dry-run output matches enforce selection."""
    ident = current_identity()
    if ident["storage_type"] != "S3":
        return fail("retention needs GREPTIME_STORAGE_TYPE=S3", code=2)
    miss = s3_missing()
    if miss:
        return fail("retention needs S3 settings: " + ", ".join(miss), code=2)
    if not env("S3_ROOT", "").strip() or not ident["backup_prefix"].strip():
        return fail("S3 mode needs S3_ROOT and S3_BACKUP_PREFIX set", code=2)
    bp = norm_prefix(ident["backup_prefix"])
    if prefixes_overlap(norm_prefix(env("S3_ROOT", "greptime")), bp):
        return fail("backup prefix overlaps live root (refusing)", code=2)
    settings, serr = retention_settings(current_bid)
    if serr is not None:
        return fail(serr, code=1)
    if settings["mode"] == "off":
        sys.stdout.write("retention: off (no evaluation performed)\n")
        return 0
    try:
        client = make_s3_client()
        gens, _ = remote_generations(client, env("S3_BUCKET"), bp)
        # Resume interrupted retirements first: a RETIRING marker is the
        # durable identity cmd_retention omitted before. Refuse refs: a
        # restore that started before retirement must finish first.
        # Unaged/fresh incomplete prefixes (concurrent backup/restore with
        # no COMPLETE yet) block enforce; aged ones are reported, never
        # deleted. Set grace 0 to override explicitly after manual review.
        retiring = [g for g in gens if g.get("retiring")]
        now_ns = time.time_ns()
        blocking = [g for g in gens
                    if g["status"] == "incomplete" and (
                        g["created_ns"] is None
                        or now_ns - g["created_ns"] < settings["grace"] * 1_000_000_000)]
        keep, deletes, skipped = retention_plan(gens, settings, now_ns)
        expect = sum(g["bytes"] for g in deletes)
        for g in deletes:
            sys.stdout.write("retention: candidate %s bytes=%d objects=%d\n"
                             % (g["backup_id"], g["bytes"], len(g["keys"])))
        sys.stdout.write("retention: mode=%s keep=%d delete=%d bytes=%d\n"
                         % (settings["mode"], len(keep), len(deletes), expect))
        for bid, why in skipped:
            sys.stdout.write("retention: skip %s (%s)\n" % (bid, why))
        if settings["mode"] != "enforce":
            return 0
        if env("BACKUP_REMOTE_RETENTION_APPROVE", "") != "1":
            return fail("retention enforce needs BACKUP_REMOTE_RETENTION_APPROVE=1"
                        " after reviewing the dry-run list", code=2)
        if settings["grace"] > 0 and blocking:
            return fail("retention refused: incomplete generation %s"
                        " unaged or within grace (concurrent run? clean up"
                        " by hand or wait)" % blocking[0]["backup_id"], code=2)
        for g in retiring:
            resume_retiring(client, env("S3_BUCKET"), bp, g["backup_id"])
        for g in deletes:
            retire_generation(client, env("S3_BUCKET"), bp, g)
        sys.stdout.write("retention: deleted %d generations (resumed %d)\n"
                         % (len(deletes), len(retiring)))
        return 0
    except IOError as e:
        return fail(str(e)[:300], code=3)
    except Exception as e:
        return fail("retention failed: " + str(e)[:200], code=3)


def main(argv):
    if len(argv) != 2 or argv[1] not in ("backup", "restore", "list", "retention"):
        sys.stderr.write("usage: python -m scripts.storage.backup [backup|restore|list|retention]\n")
        return 1
    if argv[1] == "backup":
        return cmd_backup()
    if argv[1] == "restore":
        return cmd_restore()
    if argv[1] == "retention":
        return cmd_retention()
    return cmd_list()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
