#!/usr/bin/env python3
"""Integrated CAN archive backup / restore (local tar + S3 off-host copy).

Commands (operator runs only these; `.env` holds all settings):
  backup    pinned SQLite snapshot + exact definition bytes -> local tar -> verified S3 upload
  restore   fetch archive (local or S3) -> verify -> populate FRESH restore volumes
  list      local archives (+ remote COMPLETE backup ids when S3 configured)

Procedure (backup profile; both nondisruptive to the live receiver):
  docker compose --profile backup run --rm can-backup backup
  BACKUP_OFFLINE_CONFIRMED=1 docker compose --profile backup run --rm can-backup restore

Consistency: the live archive is a SQLite WAL database (sessions, raw_chunks,
epochs, decode_states parser cursors, outbox, worker_errors). An unpinned
paged copy starves under a continuous WAL writer (proven), so the snapshot is
taken through ONE pinned read transaction: the source is opened read-only,
BEGIN holds the WAL snapshot, and a single Connection.backup() pass copies it.
Writers keep appending outside the snapshot; the copy always terminates and is
internally consistent (contiguous chunk prefix, matching sessions cursors).
Definition bytes (observed.dbc/observed.json or whatever regular files the
definitions volume holds) are copied to staging first and hashed there, so the
tar bytes are exactly the hashed bytes. The tar holds the snapshot
raw.sqlite3 + definitions/ + manifest.json; tar member modes are normalized
(0600/0700) and the manifest carries the authoritative mode/uid/gid.

Off-host (S3 mode): tar + sidecar + manifest are uploaded and GET-and-hash
verified (streaming SHA256, never ETag: multipart ETags legitimately differ),
then a COMPLETE marker binds tar/sidecar/manifest/archive/definitions
digests. Restore refuses anything the COMPLETE record does not bind,
including local tars from failed uploads. boto checksum settings and all S3
helpers are reused from scripts.storage.backup (same pinned boto3 venv).

Restore: separate fresh volumes only (CAN_RESTORE_RAW_DIR /
CAN_RESTORE_DEFS_DIR, never the live source paths: self-overwrite refuses).
Non-empty targets refuse with no force flag. Restored files AND their parent
dirs regain the exact recorded uid/gid/modes (no widening: root traverses
its own 0700 without other-execute, matching the receiver's 0700+ownership
rule); chown/chmod failure fails closed (restore needs privilege).
Post-restore re-hashes every byte, recounts every table against the manifest,
and runs integrity_check. Manifest uids/modes are untrusted input:
world-writable and setuid/setgid/sticky modes refuse,
and staging extraction is traversal/symlink/special-file free.

Restore gate: BACKUP_OFFLINE_CONFIRMED=1 AND the *restore-target* receiver
lock (CAN_RESTORE_RAW_DIR/raw.sqlite3.lock) is free. The live source needs
no gate: restore populates only the separate fresh volumes (self-overwrite
refuses), reads the source read-only, and never disturbs the running
receiver or its WAL writer. Gates only inspect (read-only flock); they never
write the source mounts, which stay read-only in Compose. Cutover/restart
replacement is a separate step: stop the original, keep a single archive
owner for one path, and start the receiver on the verified restore.

Capacity: backup preflights 2x source estimate + reserve on the backup
volume; restore preflights tar + uncompressed members + reserve on the backup
volume and members + WAL overhead + reserve on the restore volume. Shortfalls
refuse BEFORE any write. Failures delete only this run's staging/owned
writes, never the source, prior backups, or unrelated live objects.

Environment: CAN_ARCHIVE_PATH, CAN_DEFINITIONS_DIR, CAN_BACKUP_DIR,
CAN_RESTORE_RAW_DIR, CAN_RESTORE_DEFS_DIR, CAN_BACKUP_FILE (exact name or id),
CAN_BACKUP_KEEP, CAN_BACKUP_RESERVE_BYTES (default 64MiB),
CAN_STORAGE_TYPE (File|S3), CAN_S3_BACKUP_PREFIX, BACKUP_OFFLINE_CONFIRMED,
standard S3_* (S3 keys live ONLY on this service, never printed).

Exit codes: 0 ok, 1 usage/env error, 2 precondition failure, 3 IO error.
"""

import contextlib
import fcntl
import hashlib
import io
import json
import os
import shutil
import sqlite3
import stat
import sys
import tarfile
import tempfile
import time
from pathlib import Path

from scripts.storage import backup as bk

TOOL = "can_backup.py v1"
CURRENT_DIR_VERSION = 1
CAN_PREFIX = "can-archive-backup-"
CAN_SUFFIX = ".tar.gz"
CAN_SIDECAR = ".sha256"
CAN_MANIFEST = "manifest.json"
CAN_COMPLETE = "COMPLETE"
CAN_ARCHIVE = "raw.sqlite3"
CAN_DEFS = "definitions"


def archive_path():
    return bk.env("CAN_ARCHIVE_PATH", "/source/can-raw/raw.sqlite3")


def defs_dir():
    return bk.env("CAN_DEFINITIONS_DIR", "/source/can-definitions")


def ensure_backup_dir():
    """Create CAN_BACKUP_DIR itself (e.g. /backup/can on first run)."""
    raw = bk.env("CAN_BACKUP_DIR", "/backup")
    try:
        os.makedirs(raw, mode=0o700, exist_ok=True)
    except OSError as e:
        return None, "CAN backup dir unavailable: " + str(e)[:150]
    if not os.path.isdir(raw):
        return None, "CAN_BACKUP_DIR does not exist: " + raw
    return raw, None


def restore_raw_dir():
    return bk.env("CAN_RESTORE_RAW_DIR", "/restore/can-raw")


def restore_defs_dir():
    return bk.env("CAN_RESTORE_DEFS_DIR", "/restore/can-definitions")


def current_identity():
    return {
        "storage_type": bk.env("CAN_STORAGE_TYPE", "File"),
        "bucket": bk.env("S3_BUCKET", ""),
        "backup_prefix": bk.env("CAN_S3_BACKUP_PREFIX", "can-archive-backups"),
    }


def _free(path):
    return shutil.disk_usage(path).free


def _lock_held(lock_path):
    """True if another process holds the receiver lock, False if free/absent.

    None when the lock cannot even be inspected (fail closed). Read-only:
    never creates or mutates the lock file."""
    try:
        fd = os.open(lock_path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return False
    except OSError:
        return None
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        except OSError:
            return None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            return None
        return False
    finally:
        os.close(fd)


def offline_error(op):
    if op == "backup":
        return None
    if bk.env("BACKUP_OFFLINE_CONFIRMED", "") != "1":
        return bk.fail(op + " refused: set BACKUP_OFFLINE_CONFIRMED=1 only"
                       " after confirming the fresh restore targets", code=2)
    lock = os.path.join(restore_raw_dir(), CAN_ARCHIVE + ".lock")
    held = _lock_held(lock)
    if held is None:
        return bk.fail(op + " refused: cannot inspect restore-target lock: "
                       + lock, code=2)
    if held:
        return bk.fail(op + " refused: restore target in use (lock held): "
                       + lock, code=2)
    return None


def archive_counts(conn):
    """Consistent counts; MUST run inside the pinned read transaction."""
    raw, raw_bytes = conn.execute(
        "SELECT COUNT(*),COALESCE(SUM(length(data)),0) FROM raw_chunks").fetchone()
    sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    states = conn.execute("SELECT COUNT(*) FROM decode_states").fetchone()[0]
    epochs = [row[0] for row in
              conn.execute("SELECT epoch FROM epochs ORDER BY rowid")]
    return {"sessions": sessions, "raw_chunks": raw, "raw_bytes": raw_bytes,
            "outbox_rows": pending, "decode_state_rows": states,
            "epochs": sorted(epochs)}


def pinned_backup(src_path, dst_path):
    """Copy the live WAL archive under one pinned read transaction.

    BEGIN pins the WAL snapshot on this read-only connection; concurrent
    writers keep appending outside it, so a single backup() pass always
    terminates with an internally consistent snapshot. No paged sleep loop:
    that starves under a continuous writer. The source is never written."""
    uri = Path(src_path).as_uri() + "?mode=ro"
    src = sqlite3.connect(uri, uri=True, timeout=30)
    try:
        src.execute("BEGIN")
        try:
            counts = archive_counts(src)
            dst = sqlite3.connect(dst_path, timeout=30)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            try:
                src.execute("ROLLBACK")
            except sqlite3.Error:
                pass
    finally:
        src.close()
    fix = sqlite3.connect(dst_path, timeout=30)
    try:
        fix.execute("PRAGMA journal_mode=WAL")
        fix.execute("PRAGMA synchronous=FULL")
        fix.commit()
    finally:
        fix.close()
    os.chmod(dst_path, 0o600)
    return counts


def snapshot_defs(src_defs):
    """Copy definition files to own temp files AND hash the copies.

    Returns (staged_paths, meta): staged (name, path) pairs plus manifest
    metadata (source modes/ownership). Symlinks, subdirs, and an empty dir
    fail closed: the mapping is unrestorable without exact definition bytes."""
    try:
        names = sorted(os.listdir(src_defs))
    except OSError as e:
        raise IOError("definitions unreadable: " + str(e)[:150])
    if not names:
        raise IOError("definitions dir empty (refusing: no exact bytes to pin)")
    staged = []
    meta = []
    for name in names:
        if "/" in name or name in (".", ".."):
            raise IOError("definitions unsafe name (refusing): " + name)
        src = os.path.join(src_defs, name)
        if os.path.islink(src):
            raise IOError("definitions symlink refused (unrestorable): " + name)
        if os.path.isdir(src):
            raise IOError("definitions subdirectory refused: " + name)
        try:
            st = os.stat(src)
        except OSError as e:
            raise IOError("definitions stat failed: " + name + ": "
                          + str(e)[:100])
        if not stat.S_ISREG(st.st_mode):
            raise IOError("definitions not a regular file: " + name)
        staged.append((name, src, st))
    out = []
    try:
        for name, src, st in staged:
            fd, tmp = tempfile.mkstemp(prefix=".can-defs-")
            try:
                h = hashlib.sha256()
                size = 0
                with os.fdopen(fd, "wb") as f:
                    with open(src, "rb") as s:
                        while True:
                            chunk = s.read(bk.CHUNK)
                            if not chunk:
                                break
                            f.write(chunk)
                            h.update(chunk)
                            size += len(chunk)
                os.chmod(tmp, 0o600)
            except OSError as e:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise IOError("definitions copy failed: " + name + ": "
                              + str(e)[:100])
            out.append((name, tmp))
            meta.append({"name": name, "sha256": h.hexdigest(), "size": size,
                         "mode": stat.S_IMODE(st.st_mode),
                         "uid": st.st_uid, "gid": st.st_gid})
    except Exception:
        for _name, tmp in out:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        raise
    return out, meta


def cleanup_staged(staged_defs):
    for _name, tmp in staged_defs:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def check_file_mode(mode, label):
    """Private-permission trust boundary for untrusted manifest modes."""
    if not isinstance(mode, int):
        return label + " bad mode"
    if mode & 0o7000:
        return label + " special mode bits refused"
    if mode & 0o002:
        return label + " world-writable mode refused"
    if not mode & 0o400:
        return label + " owner-unreadable mode refused"
    return None


def check_uid_gid(value, label):
    if not isinstance(value, int) or isinstance(value, bool):
        return label + " bad id"
    if not 0 <= value < 2 ** 32:
        return label + " id out of range"
    return None


def validate_def_record(obj):
    if not isinstance(obj, dict):
        return "definition record not an object"
    name = obj.get("name")
    if not isinstance(name, str) or not name or "/" in name or name in (".", ".."):
        return "definition record unsafe name: %r" % (name,)
    if not isinstance(obj.get("size"), int) or obj["size"] < 0:
        return "definition record bad size: " + name
    if not isinstance(obj.get("sha256"), str) or not bk.SHA_RE.match(obj["sha256"]):
        return "definition record bad sha256: " + name
    for field in ("mode", "uid", "gid"):
        if field not in obj:
            return "definition record missing %s: %s" % (field, name)
    problem = check_file_mode(obj["mode"], "definition " + name)
    if problem is not None:
        return problem
    for field in ("uid", "gid"):
        problem = check_uid_gid(obj[field], "definition " + name + " " + field)
        if problem is not None:
            return problem
    return None


def validate_manifest(manifest):
    if not isinstance(manifest, dict):
        return "manifest not an object"
    required = ("backup_id", "created_utc", "storage_type", "backup_prefix",
                "archive", "definitions")
    if manifest.get("storage_type") == "S3":
        required += ("bucket",)
    for field in required:
        if field not in manifest or manifest[field] in ("", None, []):
            return "manifest missing field: " + field
    bid = manifest.get("backup_id")
    if not isinstance(bid, str) or not bid.startswith(CAN_PREFIX):
        return "manifest bad backup_id"
    ident = current_identity()
    for key in ("storage_type", "bucket", "backup_prefix"):
        if str(manifest.get(key, "")) != ident[key]:
            return ("identity mismatch: archive " + key + "="
                    + repr(manifest.get(key, "")) + " vs current "
                    + repr(ident[key]) + " (same-root restore only)")
    arch = manifest.get("archive")
    if not isinstance(arch, dict):
        return "manifest archive not an object"
    if arch.get("name") != CAN_ARCHIVE:
        return "manifest archive bad name"
    if not isinstance(arch.get("sha256"), str) or not bk.SHA_RE.match(arch["sha256"]):
        return "manifest archive bad sha256"
    if not isinstance(arch.get("size"), int) or arch["size"] <= 0:
        return "manifest archive bad size"
    for field in ("mode", "uid", "gid"):
        if field not in arch:
            return "manifest archive missing " + field
    problem = check_file_mode(arch["mode"], "archive")
    if problem is not None:
        return problem
    for field in ("uid", "gid"):
        problem = check_uid_gid(arch[field], "archive " + field)
        if problem is not None:
            return problem
    counts = arch.get("counts")
    if not isinstance(counts, dict):
        return "manifest archive counts not an object"
    for field in ("sessions", "raw_chunks", "raw_bytes", "outbox_rows",
                  "decode_state_rows"):
        if not isinstance(counts.get(field), int) or counts[field] < 0:
            return "manifest archive bad count: " + field
    if not isinstance(counts.get("epochs"), list) or \
            any(not isinstance(e, str) for e in counts["epochs"]):
        return "manifest archive bad epochs"
    dirs = manifest.get("dirs")
    if not isinstance(dirs, dict) or not isinstance(dirs.get("version"), int) \
            or dirs["version"] != CURRENT_DIR_VERSION:
        return "manifest dirs missing or unsupported version"
    for key in ("raw_mode", "defs_mode"):
        problem = check_file_mode(dirs.get(key), "manifest dirs " + key)
        if problem is not None:
            return problem
    for key in ("raw_uid", "raw_gid", "defs_uid", "defs_gid"):
        problem = check_uid_gid(dirs.get(key), "manifest dirs " + key)
        if problem is not None:
            return problem
    defs = manifest.get("definitions")
    seen = set()
    for obj in defs["files"]:
        problem = validate_def_record(obj)
        if problem is not None:
            return problem
        if obj["name"] in seen:
            return "duplicate definition file: " + obj["name"]
        seen.add(obj["name"])
    return None


def validate_members_structural(members):
    """Trust-free shape check: no traversal, links, specials, or extras."""
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
        tops.add(name.rstrip("/").split("/")[0])
    allowed = {CAN_ARCHIVE, CAN_DEFS, CAN_MANIFEST}
    if CAN_ARCHIVE not in tops or CAN_MANIFEST not in tops:
        return ("partial archive refused (has: " + ",".join(sorted(tops)) + ")")
    extra = sorted(tops - allowed)
    if extra:
        return "unexpected top-level entries refused: " + ",".join(extra)
    return None


def check_member_set(members, manifest):
    want = {CAN_ARCHIVE, CAN_MANIFEST}
    want |= {CAN_DEFS + "/" + f["name"] for f in
             manifest["definitions"]["files"]}
    got = set()
    for m in members:
        if m.isdir():
            if m.name.rstrip("/") != CAN_DEFS:
                return "unexpected directory: " + m.name
            continue
        got.add(m.name)
    if got != want:
        missing = sorted(want - got)
        extra = sorted(got - want)
        return ("archive member mismatch (missing %s, extra %s)"
                % (missing, extra))
    return None


def load_can_archive(path):
    """Returns (manifest, raw_bytes, members, errmsg, errcode)."""
    try:
        with tarfile.open(path, "r:gz") as tar:
            members = tar.getmembers()
    except Exception as e:
        return None, None, None, "backup unreadable (torn archive?): " \
            + str(e)[:200], 3
    problem = validate_members_structural(members)
    if problem is not None:
        return None, None, None, problem, 2
    try:
        with tarfile.open(path, "r:gz") as tar:
            info = tar.getmember(CAN_MANIFEST)
            if info.size > bk.CONTROL_CAP:
                return None, None, None, \
                    "manifest exceeds control size limit (refusing)", 2
            with tar.extractfile(CAN_MANIFEST) as stream:
                raw = bk.read_control(stream, bk.CONTROL_CAP, CAN_MANIFEST)
            if len(raw) != info.size:
                return None, None, None, "manifest short read (torn archive?)", 3
        manifest = json.loads(raw.decode("utf-8"))
    except Exception as e:
        return None, None, None, "backup manifest unreadable: " \
            + str(e)[:150], 2
    problem = validate_manifest(manifest)
    if problem is not None:
        return None, None, None, problem, 2
    problem = check_member_set(members, manifest)
    if problem is not None:
        return None, None, None, problem, 2
    return manifest, raw, members, None, 0


def _source_estimate(src, ddefs):
    total = 0
    for p in (src, src + "-wal", src + "-shm"):
        try:
            total += os.path.getsize(p)
        except OSError:
            pass
    try:
        names = os.listdir(ddefs)
    except OSError:
        names = []
    for name in names:
        p = os.path.join(ddefs, name)
        try:
            if os.path.isfile(p) and not os.path.islink(p):
                total += os.path.getsize(p)
        except OSError:
            pass
    return total


def claim_backup_id(d, s3, bucket, bp):
    return bk.claim_backup_id(d, s3, bucket, bp, prefix=CAN_PREFIX,
                              suffix=CAN_SUFFIX, sidecar_suffix=CAN_SIDECAR)


def remote_base(backup_prefix, backup_id):
    return bk.remote_base(backup_prefix, backup_id)

def write_can_tar(d, name, db_copy, staged_defs, manifest):
    raw_manifest = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
    if len(raw_manifest) > bk.CONTROL_CAP:
        raise IOError("manifest exceeds control size limit (%d bytes)"
                      % bk.CONTROL_CAP)
    path = os.path.join(d, name)
    tmp = path + ".tmp"
    try:
        with tarfile.open(tmp, "w:gz", format=tarfile.PAX_FORMAT) as tar:
            info = tarfile.TarInfo(CAN_ARCHIVE)
            info.size = os.path.getsize(db_copy)
            info.mode = 0o600
            info.mtime = int(time.time())
            with open(db_copy, "rb") as f:
                tar.addfile(info, f)
            dinfo = tarfile.TarInfo(CAN_DEFS)
            dinfo.type = tarfile.DIRTYPE
            dinfo.mode = 0o700
            dinfo.mtime = int(time.time())
            tar.addfile(dinfo)
            for fname, fpath in staged_defs:
                fi = tarfile.TarInfo(CAN_DEFS + "/" + fname)
                fi.size = os.path.getsize(fpath)
                fi.mode = 0o600
                fi.mtime = int(time.time())
                with open(fpath, "rb") as f:
                    tar.addfile(fi, f)
            mi = tarfile.TarInfo(CAN_MANIFEST)
            mi.size = len(raw_manifest)
            mi.mode = 0o600
            mi.mtime = int(time.time())
            tar.addfile(mi, io.BytesIO(raw_manifest))
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
        os.chmod(path, 0o600)
        digest = bk.sha256_file(path)
        with open(path + CAN_SIDECAR, "w", encoding="utf-8") as f:
            f.write(digest + "  " + name + "\n")
        os.chmod(path + CAN_SIDECAR, 0o600)
    except Exception as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise IOError("backup write failed: " + str(e)[:200])
    return path, digest, raw_manifest


def prune_local(d, path, keep):
    return bk.prune_local(d, path, keep, prefix=CAN_PREFIX,
                          suffix=CAN_SUFFIX, sidecar_suffix=CAN_SIDECAR)


def cmd_backup():
    os.umask(0o077)
    d, derr = ensure_backup_dir()
    if derr is not None:
        return bk.fail(derr, code=2)
    src = archive_path()
    ddefs = defs_dir()
    if os.path.islink(src):
        return bk.fail("CAN archive is a symlink (refusing): " + src, code=2)
    if not os.path.isfile(src):
        return bk.fail("CAN archive not found: " + src, code=2)
    if not os.path.isdir(ddefs):
        return bk.fail("CAN definitions dir missing: " + ddefs, code=2)
    try:
        keep = int(bk.env("CAN_BACKUP_KEEP", "7"))
    except ValueError:
        return bk.fail("CAN_BACKUP_KEEP must be an integer", code=1)
    if keep < 1:
        keep = 1
    reserve, rerr = bk.read_reserve(var="CAN_BACKUP_RESERVE_BYTES")
    if rerr is not None:
        return bk.fail(rerr, code=1)
    ident = current_identity()
    if ident["storage_type"] not in ("File", "S3"):
        return bk.fail("CAN_STORAGE_TYPE must be File or S3", code=1)
    s3 = None
    bucket = ""
    bp = ""
    if ident["storage_type"] == "S3":
        miss = bk.s3_missing()
        if miss:
            return bk.fail("S3 mode needs S3 settings (fail closed): missing "
                           + ", ".join(miss), code=2)
        if not bk.env("S3_BUCKET", "").strip() \
                or not ident["backup_prefix"].strip():
            return bk.fail("S3 mode needs S3_BUCKET and CAN_S3_BACKUP_PREFIX set",
                           code=2)
        bucket = bk.env("S3_BUCKET")
        bp = bk.norm_prefix(ident["backup_prefix"])
        try:
            s3 = bk.make_s3_client()
        except Exception as e:
            return bk.fail("S3 client failed: " + str(e)[:200], code=3)
    try:
        est = _source_estimate(src, ddefs)
        need = 2 * est + reserve
        if _free(d) < need:
            return bk.fail("backup refused: insufficient space in %s (need %d)"
                           % (d, need), code=2)
    except OSError as e:
        return bk.fail("backup preflight failed: " + str(e)[:200], code=3)
    try:
        bid, stamp = claim_backup_id(d, s3, bucket, bp)
    except IOError as e:
        return bk.fail(str(e)[:200], code=3)
    staging = tempfile.mkdtemp(dir=d, prefix=".can-backup-")
    staged_defs = []
    try:
        db_copy = os.path.join(staging, CAN_ARCHIVE)
        try:
            counts = pinned_backup(src, db_copy)
        except (sqlite3.Error, OSError) as e:
            return bk.fail("archive snapshot failed: " + str(e)[:200], code=3)
        try:
            staged_defs, defs_meta = snapshot_defs(ddefs)
        except IOError as e:
            return bk.fail(str(e)[:250], code=2)
        try:
            lst = os.lstat(src)
            if not stat.S_ISREG(lst.st_mode):
                return bk.fail("CAN archive not a regular file (refusing): "
                               + src, code=2)
            src_mode, src_uid, src_gid = (stat.S_IMODE(lst.st_mode),
                                          lst.st_uid, lst.st_gid)
        except OSError as e:
            return bk.fail("CAN archive stat failed: " + str(e)[:150], code=3)
        problem = check_file_mode(src_mode, "live archive")
        if problem is not None:
            return bk.fail("live archive permissions unsafe (" + problem
                           + "): refusing", code=2)
        try:
            raw_st = os.stat(os.path.dirname(os.path.abspath(src)))
            defs_st = os.stat(ddefs)
        except OSError as e:
            return bk.fail("source dir stat failed: " + str(e)[:150], code=3)
        raw_mode, defs_mode = stat.S_IMODE(raw_st.st_mode), stat.S_IMODE(defs_st.st_mode)
        for label, mode in (("source raw dir", raw_mode),
                            ("source definitions dir", defs_mode)):
            problem = check_file_mode(mode, label)
            if problem is not None:
                return bk.fail(label + " permissions unsafe (" + problem
                               + "): refusing", code=2)
        arch_sha = bk.sha256_file(db_copy)
        manifest = {
            "tool": TOOL,
            "backup_id": bid,
            "created_utc": stamp,
            "storage_type": ident["storage_type"],
            "bucket": ident["bucket"],
            "backup_prefix": ident["backup_prefix"],
            "dirs": {"version": CURRENT_DIR_VERSION,
                     "raw_uid": raw_st.st_uid, "raw_gid": raw_st.st_gid,
                     "raw_mode": raw_mode,
                     "defs_uid": defs_st.st_uid, "defs_gid": defs_st.st_gid,
                     "defs_mode": defs_mode},
            "archive": {"name": CAN_ARCHIVE, "sha256": arch_sha,
                        "size": os.path.getsize(db_copy),
                        "mode": src_mode, "uid": src_uid, "gid": src_gid,
                        "counts": counts},
            "definitions": {"files": defs_meta},
        }
        name = bid + CAN_SUFFIX
        try:
            path, digest, raw_manifest = write_can_tar(
                d, name, db_copy, staged_defs, manifest)
        except IOError as e:
            if s3 is not None:
                bk.rollback_owned(s3, bucket, [])
            return bk.fail(str(e)[:300], code=3)
    finally:
        cleanup_staged(staged_defs)
        shutil.rmtree(staging, ignore_errors=True)
    if s3 is not None:
        rbase = remote_base(ident["backup_prefix"], bid)
        tar_size = os.path.getsize(path)
        owned = []
        try:
            with open(path + CAN_SIDECAR, "rb") as f:
                sidecar = f.read()
            complete = json.dumps({
                "backup_id": bid, "created_utc": stamp,
                "tar_sha256": digest, "tar_size": tar_size,
                "sidecar_sha256": hashlib.sha256(sidecar).hexdigest(),
                "manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
                "archive": {"sha256": manifest["archive"]["sha256"],
                            "size": manifest["archive"]["size"]},
                "definitions": [{"name": f["name"], "size": f["size"],
                                 "sha256": f["sha256"]}
                                for f in defs_meta],
            }, sort_keys=True).encode("utf-8")
            if len(complete) > bk.CONTROL_CAP:
                raise IOError("COMPLETE exceeds control size limit (%d bytes)"
                              % bk.CONTROL_CAP)
            owned.append(rbase + name)
            bk.s3_put_file_verified(s3, bucket, rbase + name, path, digest,
                                    tar_size)
            owned.append(rbase + name + CAN_SIDECAR)
            bk.s3_put_bytes_verified(s3, bucket, rbase + name + CAN_SIDECAR,
                                     sidecar)
            owned.append(rbase + CAN_MANIFEST)
            bk.s3_put_bytes_verified(s3, bucket, rbase + CAN_MANIFEST,
                                     raw_manifest)
            owned.append(rbase + CAN_COMPLETE)
            bk.s3_put_bytes_verified(s3, bucket, rbase + CAN_COMPLETE, complete)
        except Exception as e:
            bk.rollback_owned(s3, bucket, owned)
            return bk.fail("off-host upload failed (local tar kept, remote rolled"
                           " back including COMPLETE): " + str(e)[:200], code=3)
        prune_local(d, path, keep)
        sys.stdout.write("can-backup: ok: %s (off-host verified)\n" % name)
    else:
        prune_local(d, path, keep)
        sys.stdout.write("can-backup: ok: " + name + " (File mode, local only)\n")
    return 0


def cmd_list():
    import glob as _glob
    d, derr = ensure_backup_dir()
    if derr is not None:
        return bk.fail(derr, code=2)
    files = sorted(_glob.glob(os.path.join(d, CAN_PREFIX + "*" + CAN_SUFFIX)))
    for f in files:
        sys.stdout.write(os.path.basename(f) + "\n")
    if current_identity()["storage_type"] == "S3" and not bk.s3_missing():
        try:
            client = bk.make_s3_client()
            bp = bk.norm_prefix(bk.env("CAN_S3_BACKUP_PREFIX",
                                       "can-archive-backups"))
            objs = bk.s3_list_all(client, bk.env("S3_BUCKET"), bp)
            ids = sorted({o["Key"][len(bp):].split("/")[0]
                          for o in objs
                          if o["Key"].endswith("/" + CAN_COMPLETE)})
            for bid in ids:
                sys.stdout.write("remote: " + bid + "\n")
        except Exception as e:
            return bk.fail("remote list failed: " + str(e)[:200], code=3)
    return 0


def normalize_want(choice):
    return bk.normalize_want(choice, sidecar_suffix=CAN_SIDECAR,
                             suffix=CAN_SUFFIX)


def resolve_local_archive(d):
    return bk.resolve_local_archive(d, file_var="CAN_BACKUP_FILE",
                                    prefix=CAN_PREFIX, suffix=CAN_SUFFIX)


def validate_complete(complete, bid):
    if not isinstance(complete, dict):
        return "COMPLETE not an object"
    if complete.get("backup_id") != bid:
        return "COMPLETE backup_id mismatch (refusing)"
    for field in ("tar_sha256", "sidecar_sha256", "manifest_sha256"):
        if not isinstance(complete.get(field), str) \
                or not bk.SHA_RE.match(complete[field]):
            return "COMPLETE bad field: " + field
    if not isinstance(complete.get("tar_size"), int) \
            or complete["tar_size"] <= 0:
        return "COMPLETE bad tar_size"
    arch = complete.get("archive")
    if not isinstance(arch, dict) or not isinstance(arch.get("sha256"), str) \
            or not bk.SHA_RE.match(arch["sha256"]) \
            or not isinstance(arch.get("size"), int) or arch["size"] <= 0:
        return "COMPLETE bad archive record"
    defs = complete.get("definitions")
    if not isinstance(defs, list) or not defs:
        return "COMPLETE definitions not a non-empty list"
    seen = set()
    for obj in defs:
        if not isinstance(obj, dict) or not isinstance(obj.get("name"), str) \
                or not obj["name"] or "/" in obj["name"]:
            return "COMPLETE bad definition name"
        if not isinstance(obj.get("size"), int) or obj["size"] < 0:
            return "COMPLETE bad definition size: " + obj["name"]
        if not isinstance(obj.get("sha256"), str) \
                or not bk.SHA_RE.match(obj["sha256"]):
            return "COMPLETE bad definition sha256: " + obj["name"]
        if obj["name"] in seen:
            return "COMPLETE duplicate definition: " + obj["name"]
        seen.add(obj["name"])
    return None


def fetch_complete(client, bucket, bp, bid):
    rbase = remote_base(bp, bid)
    try:
        raw = bk.s3_get_control(client, bucket, rbase + CAN_COMPLETE)
    except Exception as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if isinstance(e, KeyError) or code in ("NoSuchKey", "404", "NotFound"):
            return None, ("no COMPLETE record for %s (incomplete backup or failed"
                          " upload: not restorable)" % bid)
        return None, "COMPLETE record unreadable for %s (refusing): %s" \
            % (bid, str(e)[:200])
    try:
        complete = json.loads(raw.decode("utf-8"))
    except Exception:
        return None, "COMPLETE record corrupt for %s (refusing)" % bid
    problem = validate_complete(complete, bid)
    if problem is not None:
        return None, problem
    return complete, None


def check_against_complete(path, side_digest, raw_manifest, manifest, complete):
    if bk.sha256_file(path) != complete["tar_sha256"]:
        return "tar hash differs from COMPLETE record (refusing)"
    if os.path.getsize(path) != complete["tar_size"]:
        return "tar size differs from COMPLETE record (refusing)"
    if side_digest != complete["tar_sha256"]:
        return "sidecar digest differs from COMPLETE record (refusing)"
    with open(path + CAN_SIDECAR, "rb") as stream:
        side_raw = bk.read_control(stream, bk.SIDECAR_CAP,
                                   path + CAN_SIDECAR)
    if hashlib.sha256(side_raw).hexdigest() != complete["sidecar_sha256"]:
        return "sidecar bytes differ from COMPLETE record (refusing)"
    if hashlib.sha256(raw_manifest).hexdigest() != complete["manifest_sha256"]:
        return "embedded manifest differs from COMPLETE record (refusing)"
    arch = manifest["archive"]
    if (arch["sha256"], arch["size"]) != (complete["archive"]["sha256"],
                                          complete["archive"]["size"]):
        return "archive record differs from COMPLETE record (refusing)"
    want = sorted((f["name"], f["size"], f["sha256"])
                  for f in manifest["definitions"]["files"])
    got = sorted((f["name"], f["size"], f["sha256"])
                 for f in complete["definitions"])
    if want != got:
        return "definitions list differs from COMPLETE record (refusing)"
    return None


def download_remote_backup(d, client, bucket, bp, want):
    objs = bk.s3_list_all(client, bucket, bp)
    ids = sorted({o["Key"][len(bp):].split("/")[0]
                  for o in objs if o["Key"].endswith("/" + CAN_COMPLETE)})
    if not ids:
        return None, None, "no COMPLETE backups under s3://%s/%s" % (bucket, bp)
    bid = want if want in ids else (ids[-1] if not want else None)
    if bid is None:
        return None, None, "remote backup not found: %s (have: %s)" \
            % (want, ",".join(ids))
    complete, cerr = fetch_complete(client, bucket, bp, bid)
    if cerr is not None:
        return None, None, cerr
    rbase = remote_base(bp, bid)
    expected_tar = bid + CAN_SUFFIX
    top = set()
    for o in bk.s3_list_all(client, bucket, rbase):
        rel = o["Key"][len(rbase):]
        if not rel or "/" in rel:
            continue
        top.add(rel)
    if expected_tar not in top:
        return None, None, ("remote backup %s has no exact top-level archive %s"
                             % (bid, expected_tar))
    if expected_tar + CAN_SIDECAR not in top:
        return None, None, ("remote backup %s missing sidecar (refusing)" % bid)
    if CAN_MANIFEST not in top:
        return None, None, ("remote backup %s missing manifest copy (refusing)"
                             % bid)
    path = os.path.join(d, expected_tar)
    tmp = path + ".tmp"
    try:
        h = hashlib.sha256()
        size = 0
        with contextlib.closing(client.get_object(
                Bucket=bucket, Key=rbase + expected_tar)["Body"]) as body:
            with open(tmp, "wb") as f:
                while True:
                    chunk = body.read(bk.CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
        if h.hexdigest() != complete["tar_sha256"] \
                or size != complete["tar_size"]:
            raise IOError("downloaded tar differs from COMPLETE record (refusing)")
        side_raw = bk.s3_get_control(
            client, bucket, rbase + expected_tar + CAN_SIDECAR, bk.SIDECAR_CAP)
        parts = side_raw.decode("utf-8").split()
        if len(parts) != 2 or parts[0] != complete["tar_sha256"]:
            raise IOError("remote sidecar differs from COMPLETE record (refusing)")
        if hashlib.sha256(side_raw).hexdigest() != complete["sidecar_sha256"]:
            raise IOError("remote sidecar bytes differ from COMPLETE record"
                          " (refusing)")
        remote_manifest = bk.s3_get_control(client, bucket,
                                            rbase + CAN_MANIFEST)
        if hashlib.sha256(remote_manifest).hexdigest() \
                != complete["manifest_sha256"]:
            raise IOError("remote manifest differs from COMPLETE record (refusing)")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
        os.chmod(path, 0o600)
        with open(path + CAN_SIDECAR, "wb") as f:
            f.write(side_raw)
        os.chmod(path + CAN_SIDECAR, 0o600)
    except IOError as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None, None, str(e)[:250]
    except Exception as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None, None, "remote download failed: " + str(e)[:200]
    return path, complete, None


def ensure_restore_fresh(raw_dir, defs_dir_out):
    for target in (raw_dir, defs_dir_out):
        if os.path.lexists(target) and not os.path.isdir(target):
            return "restore target not a directory: " + target
        os.makedirs(target, mode=0o700, exist_ok=True)
        if not bk.is_empty_dir(target):
            return ("refusing to overwrite non-empty " + target
                    + " (no force flag: restore into fresh volumes first)")
    src_db = os.path.realpath(archive_path())
    dst_db = os.path.realpath(os.path.join(raw_dir, CAN_ARCHIVE))
    if dst_db == src_db:
        return "restore raw target equals the live source (refusing self-overwrite)"
    if os.path.realpath(defs_dir_out) == os.path.realpath(defs_dir()):
        return "restore definitions target equals the live source (refusing)"
    return None


def apply_identity(path, record, label):
    """Restore the exact recorded ownership/mode, nothing wider.

    Root traverses its own 0700 dirs without any other-execute bit, so no
    mode widening is ever needed here. chmod lands before chown so a failed
    ownership handoff cannot strand a path with relaxed modes. Failures
    fail closed."""
    mode = record["mode"]
    try:
        os.chmod(path, mode)
    except OSError as e:
        raise IOError(label + " chmod failed: " + str(e)[:120])
    try:
        if record["gid"] != os.getgid():
            os.chown(path, -1, record["gid"])
    except OSError as e:
        raise IOError(label + " needs group privilege for %d: %s"
                      % (record["gid"], str(e)[:120]))
    try:
        if record["uid"] != os.getuid():
            os.chown(path, record["uid"], -1)
    except OSError as e:
        raise IOError(label + " needs ownership privilege for %d:%d: %s"
                      % (record["uid"], record["gid"], str(e)[:120]))


def recount_archive(db_path):
    uri = Path(db_path).as_uri() + "?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True,
                                            timeout=30)) as conn:
        conn.execute("BEGIN")
        try:
            counts = archive_counts(conn)
            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        finally:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
    if integrity != "ok":
        raise IOError("restored archive integrity_check failed: "
                      + str(integrity)[:120])
    return counts


def record_restore_verification(d, bid):
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=d,
                                         prefix=".can-restore-verification-",
                                         suffix=".tmp", delete=False) as stream:
            tmp = stream.name
            json.dump({"timestamp_seconds": time.time(), "backup_id": bid},
                      stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, os.path.join(d, "can-restore-verification.json"))
    finally:
        if tmp is not None and os.path.exists(tmp):
            os.unlink(tmp)


def cmd_restore():
    os.umask(0o077)
    err = offline_error("restore")
    if err is not None:
        return err
    d, derr0 = ensure_backup_dir()
    if derr0 is not None:
        return bk.fail(derr0, code=2)
    reserve, rerr = bk.read_reserve(var="CAN_BACKUP_RESERVE_BYTES")
    if rerr is not None:
        return bk.fail(rerr, code=1)
    path, want = resolve_local_archive(d)
    s3 = None
    complete = None
    ident = current_identity()
    if ident["storage_type"] not in ("File", "S3"):
        return bk.fail("CAN_STORAGE_TYPE must be File or S3", code=1)
    if path is None:
        if ident["storage_type"] != "S3":
            if bk.env("CAN_BACKUP_FILE", ""):
                return bk.fail("backup file not found: "
                               + bk.env("CAN_BACKUP_FILE", ""), code=2)
            return bk.fail("no backup files in " + d, code=2)
        miss = bk.s3_missing()
        if miss:
            return bk.fail("no local archive and S3 download needs: "
                           + ", ".join(miss), code=2)
        try:
            s3 = bk.make_s3_client()
            path, complete, derr = download_remote_backup(
                d, s3, bk.env("S3_BUCKET"),
                bk.norm_prefix(ident["backup_prefix"]), want)
        except Exception as e:
            return bk.fail("remote download failed: " + str(e)[:200], code=3)
        if derr is not None:
            return bk.fail(derr, code=2)
    if not os.path.isfile(path):
        return bk.fail("backup file not found: " + path, code=2)
    from scripts.storage.backup import read_sidecar_digest as _sidecar
    side_digest, serr = _sidecar(path)
    if serr is not None:
        return bk.fail(serr, code=2)
    if ident["storage_type"] == "File" and bk.sha256_file(path) != side_digest:
        return bk.fail("backup hash mismatch (torn/corrupt download?): " + path,
                       code=2)
    manifest, raw_manifest, members, merr, mcode = load_can_archive(path)
    if merr is not None:
        return bk.fail(merr, code=mcode)
    raw_dir = restore_raw_dir()
    defs_dir_out = restore_defs_dir()
    problem = ensure_restore_fresh(raw_dir, defs_dir_out)
    if problem is not None:
        return bk.fail(problem, code=2)
    if ident["storage_type"] == "S3":
        miss = bk.s3_missing()
        if miss:
            return bk.fail("S3 restore needs S3 settings: " + ", ".join(miss),
                           code=2)
        bucket = bk.env("S3_BUCKET")
        try:
            if s3 is None:
                s3 = bk.make_s3_client()
            if complete is None:
                complete, cerr = fetch_complete(
                    s3, bucket, bk.norm_prefix(ident["backup_prefix"]),
                    manifest["backup_id"])
                if cerr is not None:
                    return bk.fail(cerr + " (a local tar from a failed upload is"
                                  " not restorable)", code=2)
            problem = check_against_complete(path, side_digest, raw_manifest,
                                             manifest, complete)
            if problem is not None:
                return bk.fail(problem, code=2)
        except IOError as e:
            return bk.fail(str(e)[:300], code=3)
        except Exception as e:
            return bk.fail("COMPLETE verification failed: " + str(e)[:200],
                           code=3)
    try:
        members_total = sum(m.size for m in members if m.isfile())
        tar_size = os.path.getsize(path)
        if _free(d) < tar_size + members_total + reserve:
            return bk.fail("restore refused: insufficient staging space in %s"
                           % d, code=2)
        if _free(raw_dir) < members_total + members_total // 4 + reserve:
            return bk.fail("restore refused: insufficient space in " + raw_dir,
                           code=2)
        if _free(defs_dir_out) < members_total + reserve:
            return bk.fail("restore refused: insufficient space in "
                           + defs_dir_out, code=2)
    except OSError as e:
        return bk.fail("restore preflight failed: " + str(e)[:200], code=3)
    staging = tempfile.mkdtemp(dir=d, prefix=".can-restore-")
    written = []
    try:
        try:
            with tarfile.open(path, "r:gz") as tar:
                try:
                    tar.extractall(path=staging, filter="data")
                except TypeError:
                    tar.extractall(path=staging)
        except Exception as e:
            raise IOError("staging failed: " + str(e)[:200])
        staged_db = os.path.join(staging, CAN_ARCHIVE)
        if bk.sha256_file(staged_db) != manifest["archive"]["sha256"]:
            raise IOError("staged archive differs from manifest (refusing)")
        staged_defs = []
        for f in manifest["definitions"]["files"]:
            p = os.path.join(staging, CAN_DEFS, f["name"])
            if not os.path.isfile(p) or os.path.islink(p):
                raise IOError("staged definition missing: " + f["name"])
            if bk.sha256_file(p) != f["sha256"] \
                    or os.path.getsize(p) != f["size"]:
                raise IOError("staged definition differs from manifest: "
                              + f["name"])
            staged_defs.append((f, p))
        dst_db = os.path.join(raw_dir, CAN_ARCHIVE)
        shutil.copyfile(staged_db, dst_db)
        written.append(dst_db)
        dirs = manifest["dirs"]
        apply_identity(raw_dir, {"uid": dirs["raw_uid"], "gid": dirs["raw_gid"],
                                 "mode": dirs["raw_mode"]}, "restored raw dir")
        apply_identity(dst_db, manifest["archive"], "restored archive")
        apply_identity(defs_dir_out, {"uid": dirs["defs_uid"],
                                      "gid": dirs["defs_gid"],
                                      "mode": dirs["defs_mode"]},
                       "restored definitions dir")
        for f, p in staged_defs:
            dst = os.path.join(defs_dir_out, f["name"])
            shutil.copyfile(p, dst)
            written.append(dst)
            apply_identity(dst, f, "restored definition " + f["name"])
        if bk.sha256_file(dst_db) != manifest["archive"]["sha256"]:
            raise IOError("restored archive differs after copy (refusing)")
        for f in manifest["definitions"]["files"]:
            dst = os.path.join(defs_dir_out, f["name"])
            if bk.sha256_file(dst) != f["sha256"]:
                raise IOError("restored definition differs after copy: "
                              + f["name"])
        counts = recount_archive(dst_db)
        if counts != manifest["archive"]["counts"]:
            raise IOError("restored archive state differs from manifest counts"
                          " (refusing)")
        record_restore_verification(d, manifest["backup_id"])
    except IOError as e:
        for victim in written:
            try:
                os.unlink(victim)
            except OSError:
                pass
        return bk.fail(str(e)[:300], code=3)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    sys.stdout.write("can-backup: restore ok: " + os.path.basename(path) + "\n"
                     + "can-backup: note: restored files carry their recorded"
                     " uid/gid/modes for the receiver user\n")
    return 0


def main(argv):
    if len(argv) != 2 or argv[1] not in ("backup", "restore", "list"):
        sys.stderr.write("usage: python -m scripts.storage.can_backup"
                         " [backup|restore|list]\n")
        return 1
    if argv[1] == "backup":
        return cmd_backup()
    if argv[1] == "restore":
        return cmd_restore()
    return cmd_list()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
