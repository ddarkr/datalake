#!/usr/bin/env python3
"""Measure real backup/retention/restore, RAW and CAN code on identical fixtures.

The default S3 model measures calls and consumed bytes; --endpoint uses an owned
real bucket prefix. Restore oracle GETs are outside backup costs. Missing source
trees/APIs and failed cleanup produce storage-cost-measurements/1 failure JSON.
"""
import argparse
import base64
import contextlib
import hashlib
import importlib
import importlib.util
import io
import json
import os
import secrets
import shutil
import sqlite3
import sys
import tempfile
import time
import types
from unittest.mock import patch

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_METER_SPEC = importlib.util.spec_from_file_location(
    "_benchmark_s3_cost", os.path.join(REPO, "scripts", "storage", "s3_cost.py"))
_S3_METER = importlib.util.module_from_spec(_METER_SPEC)
_METER_SPEC.loader.exec_module(_S3_METER)
SCHEMA = "storage-cost-measurements/1"
CASE_IDS = ("repeated-identical", "repeated-identical-2", "small-change", "large-change")
AUX_IDS = ("raw_fresh", "raw_replay", "can_small", "can_big")
OPS = ("lists", "heads", "gets", "puts", "copies", "deletes")
RETENTION_API = ("retention_settings", "retention_plan", "cmd_retention", "remote_generations")
_FROZEN_NS = 1783987200000000000


def det(seed, n):
    return hashlib.shake_256(seed.encode()).digest(n)


@contextlib.contextmanager
def _frozen_time():
    """Reset per variant, while advancing every clock/token read within it."""
    clock, tokens = [0], [0]
    strftime = time.strftime

    def now_ns():
        clock[0] += 1000
        return _FROZEN_NS + clock[0]

    def now():
        return now_ns() / 1_000_000_000

    def format_time(fmt, tup=None):
        return strftime(fmt, time.gmtime(now()) if tup is None else tup)

    def token_hex(n=32):
        tokens[0] += 1
        return hashlib.shake_256(str(tokens[0]).encode()).hexdigest(n)

    umask = os.umask(0o077)
    try:
        with patch.object(time, "time", now), patch.object(time, "time_ns", now_ns), \
                patch.object(time, "strftime", format_time), \
                patch.object(secrets, "token_hex", token_hex):
            yield
    finally:
        os.umask(umask)


@contextlib.contextmanager
def implementation(root):
    """Isolate namespace imports AND lazy helper calls to the selected tree."""
    root = os.path.abspath(root)
    saved = {k: v for k, v in sys.modules.items()
             if k == "scripts" or k.startswith("scripts.")}
    for name in saved:
        del sys.modules[name]
    old_path = sys.path[:]
    sys.path.insert(0, root)
    try:
        # Namespace packages otherwise merge baseline/current search paths,
        # silently borrowing current helpers when a baseline file is absent.
        for name in ("scripts", "scripts.storage", "scripts.database",
                     "scripts.vehicle", "scripts.vehicle.raw"):
            directory = os.path.join(root, *name.split("."))
            if not os.path.isdir(directory):
                raise FileNotFoundError("required implementation package: " + name)
            package = types.ModuleType(name)
            package.__path__ = [directory]
            sys.modules[name] = package
            if "." in name:
                parent, attr = name.rsplit(".", 1)
                setattr(sys.modules[parent], attr, package)
        modules = tuple(importlib.import_module(name) for name in
                        ("scripts.storage.backup", "scripts.storage.can_backup",
                         "scripts.vehicle.raw.raw_upload"))
        bk, can, raw = modules
        for module, names in ((bk, ("LABELS", "cmd_backup", "cmd_restore", "make_s3_client",
                                   "remote_complete_ids", "fetch_complete")),
                              (can, ("cmd_backup", "bk")),
                              (raw, ("run_once", "pending", "_M", "_MLOCK"))):
            for name in names:
                if not hasattr(module, name):
                    raise AttributeError("required implementation API: "
                                         + module.__name__ + "." + name)
        if can.bk is not bk:
            raise RuntimeError("CAN loaded another tree's backup helpers")
        yield modules
    finally:
        for name in list(sys.modules):
            if name == "scripts" or name.startswith("scripts."):
                del sys.modules[name]
        sys.modules.update(saved)
        sys.path[:] = old_path


@contextlib.contextmanager
def with_env(env):
    saved = dict(os.environ)
    for name in list(os.environ):
        if name.startswith(("BACKUP_", "CAN_", "RAW_", "S3_", "GREPTIME_")):
            del os.environ[name]
    os.environ.update({k: v for k, v in env.items() if not k.startswith("_")})
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


class _S3Error(Exception):
    def __init__(self, code, status=404):
        super().__init__(code)
        self.response = {"Error": {"Code": code},
                         "ResponseMetadata": {"HTTPStatusCode": status, "RetryAttempts": 0}}


def _b64_sha(blob):
    return base64.b64encode(hashlib.sha256(blob).digest()).decode("ascii")


def _is_control_key(key):
    name = key.rsplit("/", 1)[-1]
    return (name in ("COMPLETE", "RETIRING", "manifest.json")
            or name.endswith((".sha256", ".manifest.json"))
            or "/refs/" in key or "/_locks/" in key
            or ("/_sst/" in key and name.endswith(".json")))


def _blank_io():
    return {"requests": dict.fromkeys(OPS, 0), "errors": dict.fromkeys(OPS, 0),
            "retries": 0, "bytes": {"get": 0, "put": 0, "copy": 0},
            "payload": {"requests": 0, "get_bytes": 0, "put_bytes": 0, "copy_bytes": 0},
            "control": {"requests": 0, "get_bytes": 0, "put_bytes": 0, "copy_bytes": 0}}


class _Meter:
    def reset(self):
        self._io = _blank_io()
        self._log = []

    def mark(self):
        return json.loads(json.dumps(self._io)), len(self._log)

    def io_since(self, before):
        old, start = before
        result = _blank_io()
        for section in ("requests", "errors", "bytes", "payload", "control"):
            for key in result[section]:
                result[section][key] = self._io[section][key] - old[section][key]
        result["retries"] = self._io["retries"] - old["retries"]
        # Missing HEAD -> PUT -> HEAD is not a retry. Count only another
        # same-operation attempt with no intervening operation on that key.
        failed = {}
        for op, key, ok in self._log[start:]:
            if failed.pop(key, None) == op:
                result["retries"] += 1
            if not ok:
                failed[key] = op
        return result

    def _note(self, op, key, ok, response=None, attempts=1):
        self._io["requests"][op] += attempts
        self._io["errors"][op] += int(not ok)
        side = "control" if _is_control_key(key) or op in ("lists", "deletes") else "payload"
        self._io[side]["requests"] += attempts
        self._io["retries"] += int((response or {}).get("ResponseMetadata", {}).get("RetryAttempts", 0))
        self._log.append((op, key, ok))

    def _consume(self, kind, key, size):
        self._io["bytes"][kind] += size
        side = "control" if _is_control_key(key) else "payload"
        self._io[side][kind + "_bytes"] += size


class _CountedBody:
    """Count partial/streamed reads and SDK-consumed PUT bodies, including errors."""
    def __init__(self, body, client, key, kind="get"):
        self._body, self._client, self._key, self._kind = body, client, key, kind

    def read(self, n=-1):
        try:
            value = self._body.read() if n is None or n < 0 else self._body.read(n)
        except Exception:
            # A successful GET response can still fail while consuming its
            # body. Keep the request count fixed and mark that attempt failed.
            if self._kind == "get" and not getattr(self, "_read_failed", False):
                self._client._io["errors"]["gets"] += 1
                self._client._log.append(("gets", self._key, False))
                self._read_failed = True
            raise
        self._client._consume(self._kind, self._key, len(value))
        return value

    def readinto(self, buffer):
        value = self.read(len(buffer))
        buffer[:len(value)] = value
        return len(value)

    def iter_chunks(self, chunk_size=1024):
        while True:
            value = self.read(chunk_size)
            if not value:
                return
            yield value

    def iter_lines(self, chunk_size=1024, keepends=False):
        pending = b""
        for chunk in self.iter_chunks(chunk_size):
            lines = (pending + chunk).splitlines(True)
            pending = b""
            if lines and not lines[-1].endswith((b"\n", b"\r")):
                pending = lines.pop()
            for line in lines:
                yield line if keepends else line.rstrip(b"\r\n")
        if pending:
            yield pending

    def __iter__(self):
        return self.iter_chunks()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._body.close()

    def __getattr__(self, name):
        return getattr(self._body, name)


class CountingStore(_Meter):
    """S3 model: independent stored checksums, conditional copies, shaped errors."""
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.metadata = {}
        self.reset()

    def list_objects_v2(self, Bucket, Prefix, **kw):
        self._note("lists", Prefix, True)
        return {"Contents": [{"Key": k, "Size": len(v)} for k, v in
                             sorted(self.objects.items()) if k.startswith(Prefix)],
                "IsTruncated": False}

    def head_object(self, Bucket, Key, **kw):
        if Key not in self.objects:
            self._note("heads", Key, False)
            raise _S3Error("NoSuchKey")
        blob = self.objects[Key]
        self._note("heads", Key, True)
        return {"ETag": '"' + hashlib.md5(blob).hexdigest() + '"',
                "ContentLength": len(blob), "Metadata": self.metadata.get(Key, {}),
                "ChecksumSHA256": _b64_sha(blob), "ChecksumType": "FULL_OBJECT"}

    def get_object(self, Bucket, Key, **kw):
        if Key not in self.objects:
            self._note("gets", Key, False)
            raise _S3Error("NoSuchKey")
        self._note("gets", Key, True)
        blob = self.objects[Key]
        return {"Body": _CountedBody(io.BytesIO(blob), self, Key), "ContentLength": len(blob)}

    def put_object(self, Bucket, Key, Body, Metadata=None, ChecksumSHA256=None,
                   ContentLength=None, ContentMD5=None, **kw):
        try:
            if hasattr(Body, "read"):
                stream = _CountedBody(Body, self, Key, "put")
                blob = b"".join(stream.iter_chunks(65536))
            else:
                blob = bytes(Body)
                self._consume("put", Key, len(blob))
        except Exception:
            self._note("puts", Key, False)
            raise
        if ((ChecksumSHA256 is not None and ChecksumSHA256 != _b64_sha(blob))
                or (ContentMD5 is not None and ContentMD5 !=
                    base64.b64encode(hashlib.md5(blob).digest()).decode())):
            self._note("puts", Key, False)
            raise _S3Error("BadDigest", 400)
        self._note("puts", Key, True)
        self.objects[Key] = blob
        self.metadata[Key] = dict(Metadata or {})
        return {"ChecksumSHA256": _b64_sha(blob)}

    def copy_object(self, Bucket, CopySource, Key, CopySourceIfMatch=None, **kw):
        source = CopySource["Key"]
        if source not in self.objects:
            self._note("copies", Key, False)
            raise _S3Error("NoSuchKey")
        blob = self.objects[source]
        etag = '"' + hashlib.md5(blob).hexdigest() + '"'
        if CopySourceIfMatch is not None and CopySourceIfMatch != etag:
            self._note("copies", Key, False)
            raise _S3Error("PreconditionFailed", 412)
        self._note("copies", Key, True)
        self._consume("copy", Key, len(blob))
        self.objects[Key] = blob
        self.metadata[Key] = dict(self.metadata.get(source, {}))
        return {"CopyObjectResult": {"ETag": etag, "ChecksumSHA256": _b64_sha(blob)}}

    def delete_objects(self, Bucket, Delete, **kw):
        self._note("deletes", "", True)
        for item in Delete["Objects"]:
            self.objects.pop(item["Key"], None)
            self.metadata.pop(item["Key"], None)
        return {}


class InstrumentedClient(_Meter):
    """Measure actual boto calls and body consumption, not declared lengths."""
    def __init__(self, real):
        self.real = real
        self._sizes = {}
        self._put_meter = None
        try:
            real.meta.events
        except AttributeError:
            pass
        else:
            # Reuse the shared before-send meter, after SDK hashing/signing reads.
            self._put_meter = _S3_METER.CountingS3(real, "backup")
        self.reset()

    def _call(self, op, method, kw):
        key = kw.get("Key", kw.get("Prefix", ""))
        try:
            response = getattr(self.real, method)(**kw)
        except Exception as error:
            self._note(op, key, False, getattr(error, "response", None))
            raise
        ok = not (method == "delete_objects" and response.get("Errors"))
        self._note(op, key, ok, response)
        if not ok:
            raise IOError("S3 returned delete errors")
        return response

    def list_objects_v2(self, **kw):
        response = self._call("lists", "list_objects_v2", kw)
        for obj in response.get("Contents", []):
            self._sizes[(kw["Bucket"], obj["Key"])] = obj["Size"]
        return response

    def head_object(self, **kw):
        response = self._call("heads", "head_object", kw)
        self._sizes[(kw["Bucket"], kw["Key"])] = response["ContentLength"]
        return response

    def get_object(self, **kw):
        response = self._call("gets", "get_object", kw)
        self._sizes[(kw["Bucket"], kw["Key"])] = response["ContentLength"]
        failed = [False]
        def body_failed():
            if not failed[0]:
                failed[0] = True
                self._io["errors"]["gets"] += 1
                self._log.append(("gets", kw["Key"], False))
        response["Body"] = _S3_METER._CountingBody(
            response["Body"], lambda size: self._consume("get", kw["Key"], size),
            body_failed)
        return response

    def _native_put(self, kw):
        before = self._put_meter.snapshot()
        ok = False
        try:
            response = self._put_meter.put_object(**kw)
            ok = True
            return response
        finally:
            after = self._put_meter.snapshot()
            attempts = after["attempts"].get("PutObject", 0) - before["attempts"].get("PutObject", 0)
            retries = after["retries"].get("PutObject", 0) - before["retries"].get("PutObject", 0)
            consumed = sum(after["bytes"]["put"].values()) - sum(before["bytes"]["put"].values())
            self._consume("put", kw["Key"], consumed)
            self._note("puts", kw["Key"], ok,
                       {"ResponseMetadata": {"RetryAttempts": retries}}, attempts=attempts)

    def put_object(self, **kw):
        body = kw["Body"]
        if self._put_meter is not None:
            response = self._native_put(kw)
        else:
            stream = body if hasattr(body, "read") else io.BytesIO(bytes(body))
            kw["Body"] = _CountedBody(stream, self, kw["Key"], "put")
            response = self._call("puts", "put_object", kw)
        if kw.get("ContentLength") is not None:
            self._sizes[(kw["Bucket"], kw["Key"])] = kw["ContentLength"]
        elif not hasattr(body, "read"):
            self._sizes[(kw["Bucket"], kw["Key"])] = len(body)
        return response

    def copy_object(self, **kw):
        source = kw["CopySource"]
        response = self._call("copies", "copy_object", kw)
        size = self._sizes[(source["Bucket"], source["Key"])]
        self._consume("copy", kw["Key"], size)
        self._sizes[(kw["Bucket"], kw["Key"])] = size
        return response

    def delete_objects(self, **kw):
        response = self._call("deletes", "delete_objects", kw)
        for obj in kw["Delete"]["Objects"]:
            self._sizes.pop((kw["Bucket"], obj["Key"]), None)
        return response


def listed_sizes(client, bucket, prefix):
    sizes, token = {}, None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        response = client.list_objects_v2(**kw)
        for obj in response.get("Contents", []):
            sizes[obj["Key"]] = int(obj["Size"])
        if not response.get("IsTruncated"):
            return sizes
        token = response.get("NextContinuationToken")
        if not token:
            raise IOError("listing truncated without continuation token")


def new_bytes_since(before, after, payload_only=False):
    return sum(size for key, size in after.items() if key not in before
               and (not payload_only or not _is_control_key(key)))


def measured_case(mio, label, before, after, input_bytes, input_objects):
    return {"case": label, "io": mio, **mio["requests"], "errors": mio["errors"],
            "retries": mio["retries"], "get_bytes": mio["bytes"]["get"],
            "put_bytes": mio["bytes"]["put"], "copy_bytes": mio["bytes"]["copy"],
            "verify_bytes": mio["bytes"]["get"], "payload": mio["payload"], "control": mio["control"],
            "input_bytes": input_bytes, "input_objects": input_objects,
            "new_remote_bytes": new_bytes_since(before, after),
            "new_payload_bytes": new_bytes_since(before, after, True),
            "store_bytes": sum(after.values()),
            "net_remote_bytes": sum(after.values()) - sum(before.values()),
            "objects": len(after) - len(before), "gen_bytes": new_bytes_since(before, after)}


def hash_tree(root):
    result = {}
    for base, _, names in os.walk(root):
        for name in names:
            path = os.path.join(base, name)
            with open(path, "rb") as source:
                result[os.path.relpath(path, root)] = hashlib.file_digest(source, "sha256").hexdigest()
    return result


def freeze_mtimes(root):
    for base, dirs, names in os.walk(root, topdown=False):
        for name in dirs + names:
            os.utime(os.path.join(base, name), ns=(_FROZEN_NS, _FROZEN_NS))
        os.utime(base, ns=(_FROZEN_NS, _FROZEN_NS))


def make_source(root, small_extra=b"", large_count=0, seed="src"):
    data, etc = os.path.join(root, "greptime-data"), os.path.join(root, "greptime-etc")
    os.makedirs(os.path.join(data, "wal"), exist_ok=True)
    os.makedirs(os.path.join(etc, "auth"), exist_ok=True)
    blobs = {"greptime-data/wal/seg001": det(seed + "/wal", 4096) + small_extra,
             "greptime-data/meta.json": b'{"seq":42}',
             "greptime-etc/greptimedb.toml": b'[storage]\ntype = "S3"\n',
             "greptime-etc/auth/users": b"synthetic-user=synthetic-password\n"}
    for i in range(large_count):
        blobs["greptime-data/wal/big-%04d" % i] = det("%s/big-%d" % (seed, i), 4096)
    for rel, blob in blobs.items():
        with open(os.path.join(root, rel), "wb") as out:
            out.write(blob)
    freeze_mtimes(root)


def resolve_complete_snap(bkmod, complete, backup_pfx, bid):
    """Snapshot handle for old per-generation and new shared-pool layouts."""
    if complete.get("sst_layout") == getattr(bkmod, "SHARED_LAYOUT", "shared-v1"):
        return {"layout": complete.get("sst_layout"), "snap_prefix": backup_pfx + "_sst/"}
    if hasattr(bkmod, "snapshot_prefix"):
        return {"snap_prefix": bkmod.snapshot_prefix(backup_pfx, bid)}
    return {"snap_prefix": backup_pfx + bid + "/sst/"}


def sst_snapshot_hashes(bkmod, client, bucket, backup_pfx, bid):
    """Explicit benchmark-only byte verification, outside measured backup I/O."""
    complete, err = bkmod.fetch_complete(client, bucket, backup_pfx, bid)
    assert err is None, bid + ": " + str(err)[:120]
    snap = resolve_complete_snap(bkmod, complete, backup_pfx, bid)
    out = {}
    for o in complete["sst"]:
        key = bkmod.snapshot_object_key(snap, o) if hasattr(bkmod, "snapshot_object_key") else snap["snap_prefix"] + o["key"]
        out[o["key"]] = bkmod.s3_stream_hash(client, bucket, key)[0]
    return out


def live_fixture(small_n=0, large_count=0):
    # Several MiB plus many small objects: include ALL archive/control growth
    # in the gross identical-storage <=.02 gate rather than excluding it.
    blobs = {"sst/a.sst": det("live/a", 4 << 20),
             "sst/b.sst": det("live/b-small" if small_n else "live/b", (1 << 20) + small_n)}
    for i in range(48):
        blobs["sst/tiny-%03d.sst" % i] = det("live/tiny-%d" % i, 1024)
    for i in range(large_count):
        blobs["sst/big-%04d.sst" % i] = det("live/big-%d" % i, 65536)
    return blobs


def seed_live(client, bucket, prefix, blobs):
    old = listed_sizes(client, bucket, prefix)
    obsolete = [key for key in old if key[len(prefix):] not in blobs]
    if obsolete:
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in obsolete]})
    for rel, blob in blobs.items():
        client.put_object(Bucket=bucket, Key=prefix + rel, Body=blob,
                          ContentLength=len(blob), ChecksumSHA256=_b64_sha(blob))


def complete_gens(bk, client, bucket, prefix, env):
    result = []
    with with_env(env):
        for bid in bk.remote_complete_ids(client, bucket, prefix):
            complete, error = bk.fetch_complete(client, bucket, prefix, bid)
            if error is not None:
                raise RuntimeError("unreadable COMPLETE: " + str(error))
            result.append((bid, complete))
    return sorted(result)


def run_backup(bk, client, env, label, bucket, live_prefix, backup_prefix):
    tmp = tempfile.mkdtemp(prefix="bench-backup-")
    source, target, backup_dir = (os.path.join(tmp, name) for name in ("src", "target", "backup"))
    for directory in (source, target, backup_dir):
        os.makedirs(directory)
    try:
        make_source(source, env["_small_extra"], env["_large_count"], label.removesuffix("-2"))
        want = hash_tree(source)
        e = dict(env, BACKUP_DIR=backup_dir, BACKUP_RESTORE_BASE=target,
                 BACKUP_SOURCE_DIRS=":".join(os.path.join(source, name) for name in bk.LABELS))
        before, live = listed_sizes(client, bucket, backup_prefix), listed_sizes(client, bucket, live_prefix)
        mark, start = client.mark(), time.monotonic()
        with with_env(e), patch.object(bk, "make_s3_client", lambda *a, **kw: client):
            if bk.cmd_backup() != 0:
                raise RuntimeError(label + ": backup failed")
        measured, wall = client.io_since(mark), time.monotonic() - start
        # Inventories/COMPLETE reads below are scaffolding, not backup cost.
        after = listed_sizes(client, bucket, backup_prefix)
        generations = complete_gens(bk, client, bucket, backup_prefix, e)
        if not generations:
            raise RuntimeError(label + ": no complete backup")
        bid = generations[-1][0]
        out = measured_case(measured, label, before, after, sum(live.values()), len(live))
        out.update(backup_id=bid, wall_sec=round(wall, 3), generations=len(generations),
                   live_bytes=sum(live.values()), live_objects=len(live),
                   live_keys=sorted(k[len(live_prefix):] for k in live))
        return out, bid, want, tmp, e
    except BaseException:
        shutil.rmtree(tmp)
        raise


def restore_one(bk, client, env, bid, want_hash, role, live_prefix, want_sst):
    """Remote fresh recovery checked against ORIGINAL local and SST fixture SHA."""
    with tempfile.TemporaryDirectory(prefix="bench-restore-") as root:
        backup_dir, target = os.path.join(root, "backup"), os.path.join(root, "target")
        os.makedirs(backup_dir)
        os.makedirs(target)
        e = dict(env, BACKUP_FILE=bid, BACKUP_DIR=backup_dir, BACKUP_RESTORE_BASE=target)
        live_keys = list(listed_sizes(client, e["S3_BUCKET"], live_prefix))
        if live_keys:
            client.delete_objects(Bucket=e["S3_BUCKET"], Delete={"Objects": [{"Key": k} for k in live_keys]})
        mark = client.mark()
        with with_env(e), patch.object(bk, "make_s3_client", lambda *a, **kw: client):
            if bk.cmd_restore() != 0:
                raise RuntimeError(role + ": restore failed")
            got = hash_tree(target)
            if got != want_hash:
                raise AssertionError(role + ": original local fixture hash mismatch")
            restored = {}
            for key in listed_sizes(client, e["S3_BUCKET"], live_prefix):
                body = client.get_object(Bucket=e["S3_BUCKET"], Key=key)["Body"]
                try:
                    digest = hashlib.sha256()
                    for chunk in iter(lambda: body.read(65536), b""):
                        digest.update(chunk)
                    restored[key[len(live_prefix):]] = digest.hexdigest()
                finally:
                    body.close()
            if restored != want_sst:
                raise AssertionError(role + ": original SST fixture hash mismatch")
        oracle = client.io_since(mark)
        return {"role": role, "files": len(got), "file_hashes_match": True,
                "file_hashes": got, "sst_hashes_match": True, "sst_hashes": restored,
                "oracle_io": oracle, "oracle_get_bytes": oracle["bytes"]["get"],
                "oracle_gets": oracle["requests"]["gets"]}


def retention_eval(bk, client, env, pinned, mode):
    e = dict(env, BACKUP_REMOTE_RETENTION_MODE=mode, BACKUP_REMOTE_RETAIN_COUNT="3",
             BACKUP_REMOTE_MIN_RECOVERY="1", BACKUP_REMOTE_INCOMPLETE_GRACE_SEC="0",
             BACKUP_REMOTE_PINNED=pinned, BACKUP_REMOTE_RETENTION_APPROVE="1" if mode == "enforce" else "")
    prefix, bucket = bk.norm_prefix(e["S3_BACKUP_PREFIX"]), e["S3_BUCKET"]
    before = listed_sizes(client, bucket, prefix)
    mark = client.mark()
    with with_env(e), patch.object(bk, "make_s3_client", lambda *a, **kw: client):
        if mode == "enforce":
            if bk.cmd_retention() != 0:
                raise RuntimeError("retention enforcement failed")
            measured = client.io_since(mark)
            generations, error = bk.remote_generations(client, bucket, prefix)
            if error is not None:
                raise RuntimeError(str(error))
            details = {"remaining": [g["backup_id"] for g in generations if g["status"] == "complete"]}
        else:
            guard = bk.shared_pool_lock(client, bucket, prefix) if hasattr(bk, "shared_pool_lock") else contextlib.nullcontext()
            with guard:
                settings, error = bk.retention_settings()
                if error is not None:
                    raise RuntimeError(str(error))
                generations, error = bk.remote_generations(client, bucket, prefix)
                if error is not None:
                    raise RuntimeError(str(error))
                keep, victims, skipped = bk.retention_plan(generations, settings)
                garbage, gc_reason = [], None
                if hasattr(bk, "pool_gc_plan"):
                    removed = [g["backup_id"] for g in victims]
                    removed.extend(g["backup_id"] for g in generations if g.get("retiring"))
                    garbage, gc_reason = bk.pool_gc_plan(client, bucket, prefix, generations, removed)
                gc_bytes = sum(obj.get("Size", 0) for obj in garbage)
                details = {"keep": sorted(keep), "delete_ids": [g["backup_id"] for g in victims],
                           "delete_bytes": sum(before.get(k, 0) for g in victims for k in g["keys"]) + gc_bytes,
                           "generation_bytes": sum(before.get(k, 0) for g in victims for k in g["keys"]),
                           "pool_gc_objects": len(garbage), "pool_gc_bytes": gc_bytes,
                           "pool_gc_blocked": gc_reason,
                           "skipped": [[bid, reason] for bid, reason in skipped]}
            measured = client.io_since(mark)
    after = listed_sizes(client, bucket, prefix)
    details.update(io=measured, **measured["requests"], errors=measured["errors"],
                   retries=measured["retries"], get_bytes=measured["bytes"]["get"],
                   put_bytes=measured["bytes"]["put"], copy_bytes=measured["bytes"]["copy"],
                   payload=measured["payload"], control=measured["control"],
                   remote_bytes_before=sum(before.values()), remote_bytes_after=sum(after.values()),
                   remote_bytes_removed=sum(before.values()) - sum(after.values()))
    return details


def retention_roles(section, roles):
    out = dict(section)
    for name in ("keep", "delete_ids", "remaining"):
        if name in out:
            out[name] = sorted(roles[bid] for bid in out[name])
    if "skipped" in out:
        out["skipped"] = [[roles[bid], reason] for bid, reason in out["skipped"]]
    return out


CAN_DDL = """
CREATE TABLE sessions (
 id INTEGER PRIMARY KEY, vehicle TEXT NOT NULL, collector_id TEXT NOT NULL,
 session_id TEXT NOT NULL, meta_json TEXT NOT NULL,
 next_seq INTEGER NOT NULL DEFAULT 0, last_offset_ns INTEGER NOT NULL DEFAULT -1,
 UNIQUE(vehicle,collector_id,session_id));
CREATE TABLE raw_chunks (
 id INTEGER PRIMARY KEY, session INTEGER NOT NULL REFERENCES sessions(id),
 seq INTEGER NOT NULL, offset_ns INTEGER NOT NULL, phase TEXT NOT NULL,
 data BLOB NOT NULL, UNIQUE(session,seq));
CREATE TABLE epochs (
 epoch TEXT PRIMARY KEY, mapping_revision TEXT NOT NULL);
CREATE TABLE decode_states (
 session INTEGER NOT NULL REFERENCES sessions(id),
 epoch TEXT NOT NULL REFERENCES epochs(epoch), next_seq INTEGER NOT NULL,
 state_json TEXT NOT NULL, counts_json TEXT NOT NULL, rows INTEGER NOT NULL,
 PRIMARY KEY(session,epoch));
CREATE TABLE outbox (
 id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
 epoch TEXT NOT NULL REFERENCES epochs(epoch), row_json TEXT NOT NULL);
CREATE TABLE worker_errors (
 kind TEXT PRIMARY KEY, count INTEGER NOT NULL, last_ns INTEGER NOT NULL);
"""


def make_can_archive(path, seed, n_chunks=3, chunk_size=512):
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.executescript(CAN_DDL)
        conn.execute("INSERT INTO sessions VALUES(1,'synth','fix','s1','{}',?,20)", (n_chunks,))
        conn.execute("INSERT INTO epochs VALUES('epoch-a','rev-1')")
        for seq in range(n_chunks):
            conn.execute("INSERT INTO raw_chunks(session,seq,offset_ns,phase,data) VALUES(1,?,?,?,?)",
                         (seq, seq * 10, "capture", det("%s/chunk-%d" % (seed, seq), chunk_size)))
        conn.execute("INSERT INTO decode_states VALUES(1,'epoch-a',?,'{}','{}',5)", (n_chunks,))
        conn.execute("INSERT INTO outbox VALUES(1,'ev-1','epoch-a','{\"a\":1}')")
        conn.execute("INSERT INTO worker_errors VALUES('decode_failure',2,7)")
        conn.commit()
    finally:
        conn.close()
    os.chmod(path, 0o600)


def run_can_backup(can, bk, client, env, label, bucket, prefix, seed, n_chunks, chunk_size):
    with tempfile.TemporaryDirectory(prefix="bench-can-") as root:
        source, backup_dir = os.path.join(root, "source"), os.path.join(root, "backup")
        defs = os.path.join(source, "defs")
        os.makedirs(defs, mode=0o700)
        os.makedirs(backup_dir)
        database = os.path.join(source, "raw.sqlite3")
        make_can_archive(database, seed, n_chunks, chunk_size)
        for name, blob in {"observed.dbc": b'VERSION "synthetic"\n',
                           "observed.json": json.dumps({"revision": seed}, sort_keys=True).encode()}.items():
            with open(os.path.join(defs, name), "wb") as out:
                out.write(blob)
            os.chmod(os.path.join(defs, name), 0o600)
        freeze_mtimes(source)
        sizes = [os.path.getsize(database)] + [os.path.getsize(os.path.join(defs, n)) for n in os.listdir(defs)]
        e = dict(env, CAN_ARCHIVE_PATH=database, CAN_DEFINITIONS_DIR=defs,
                 CAN_BACKUP_DIR=backup_dir, CAN_STORAGE_TYPE="S3", CAN_S3_BACKUP_PREFIX=prefix)
        before = listed_sizes(client, bucket, prefix)
        mark = client.mark()
        with with_env(e), patch.object(bk, "make_s3_client", lambda *a, **kw: client):
            if can.cmd_backup() != 0:
                raise RuntimeError(label + ": CAN backup failed")
        measured = client.io_since(mark)
        after = listed_sizes(client, bucket, prefix)
        return measured_case(measured, label, before, after, sum(sizes), len(sizes))


def make_raw_triple(spool, stem, mf4_size=2 << 20, ingress_lines=4):
    import gzip
    os.makedirs(os.path.join(spool, "sealed"))
    os.makedirs(os.path.join(spool, "closed"))
    mf4 = det("raw/" + stem, mf4_size)
    lines = b"".join(b'{"seq":%d,"v":%d}\n' % (i, i) for i in range(ingress_lines))
    sidecar = gzip.compress(lines, mtime=0)
    blobs = {"sealed/" + stem + ".mf4": mf4,
             "sealed/" + stem + ".ingress.jsonl.gz": sidecar,
             "closed/" + stem + ".jsonl": lines}
    manifest = {"mf4_sha256": hashlib.sha256(mf4).hexdigest(),
                "ingress_sha256": hashlib.sha256(lines).hexdigest(),
                "ingress_sidecar_sha256": hashlib.sha256(sidecar).hexdigest()}
    manifest_rel = "sealed/" + stem + ".mf4.manifest.json"
    blobs[manifest_rel] = json.dumps(manifest, sort_keys=True).encode()
    for rel, blob in blobs.items():
        with open(os.path.join(spool, rel), "wb") as out:
            out.write(blob)
    freeze_mtimes(spool)
    return hashlib.sha256(blobs[manifest_rel]).hexdigest()


def run_raw_once(raw, client, bucket, spool, vehicle, label, fresh=True):
    triples = raw.pending(spool)
    if len(triples) != 1:
        raise AssertionError("fixture must have exactly one pending RAW triple")
    paths = triples[0][:3]
    input_bytes = sum(os.path.getsize(path) for path in paths)
    original = hash_tree(spool)
    prefix = "raw/vehicle/" + vehicle + "/"
    before = listed_sizes(client, bucket, prefix)
    mark = client.mark()
    with raw._MLOCK:
        ack_before, failed_before = raw._M["uploaded_files"], raw._M["failed"]
    unlink = os.unlink

    def keep_fixture(path, *args, **kw):
        # Fixture-only GC disable: real ACK and final local validation still
        # run, but the exact sealed triple + closed JSONL survive crash replay.
        if os.path.commonpath((os.path.abspath(path), spool)) != spool:
            return unlink(path, *args, **kw)

    with patch.object(raw.os, "unlink", keep_fixture), patch.object(time, "sleep", lambda _: None):
        processed = raw.run_once(client, bucket, spool, vehicle, 1)
    measured = client.io_since(mark)
    with raw._MLOCK:
        acknowledged = raw._M["uploaded_files"] - ack_before
        failures = raw._M["failed"] - failed_before
    if processed != 1 or acknowledged != 1 or failures != 0:
        raise AssertionError(label + ": RAW triple was not acknowledged")
    if hash_tree(spool) != original:
        raise AssertionError(label + ": fixture GC/corruption changed local files")
    after = listed_sizes(client, bucket, prefix)
    out = measured_case(measured, label, before, after, input_bytes, len(paths))
    out.update(triples_seen=processed, acknowledged=acknowledged, replay=not fresh, locals_validated=True)
    archive_put_keys = [key for op, key, ok in client._log[mark[1]:]
                        if op == "puts" and key.startswith(prefix)]
    archive_put_bytes = sum(after.get(key, 0) for key in archive_put_keys)
    out.update(archive_puts=len(archive_put_keys), archive_put_bytes=archive_put_bytes)
    if fresh:
        if out["archive_puts"] != 3 or len(after) != 3 or out["new_remote_bytes"] != input_bytes:
            raise AssertionError("fresh RAW must upload exactly the immutable triple")
    elif out["archive_puts"] != 0 or archive_put_bytes != 0 or out["new_remote_bytes"] != 0:
        raise AssertionError("RAW replay must add no archive PUTs or retained remote bytes")
    return out


def raw_vehicle(case_prefix):
    return "bench-" + hashlib.sha256(case_prefix.encode()).hexdigest()[:32]


def bench_raw_can(bk, can, raw, client, bucket, case_prefix, env):
    with tempfile.TemporaryDirectory(prefix="bench-raw-") as spool:
        make_raw_triple(spool, "20261014T000000Z-seg")
        vehicle = raw_vehicle(case_prefix)
        fresh = run_raw_once(raw, client, bucket, spool, vehicle, "raw_fresh", True)
        replay = run_raw_once(raw, client, bucket, spool, vehicle, "raw_replay", False)
        if (fresh["input_bytes"], fresh["input_objects"]) != (replay["input_bytes"], replay["input_objects"]):
            raise AssertionError("RAW replay input changed")
    prefix = case_prefix + "can-backups/"
    small = run_can_backup(can, bk, client, env, "can_small", bucket, prefix, "small", 2, 256)
    big = run_can_backup(can, bk, client, env, "can_big", bucket, prefix, "big", 64, 65536)
    if big["input_bytes"] <= small["input_bytes"]:
        raise AssertionError("CAN big fixture must contain more actual source bytes")
    return dict(raw_fresh=fresh, raw_replay=replay, can_small=small, can_big=big)


def bench_impl(bk, can, raw, client, bucket, case_prefix, env, small_n, large_count, tag):
    live_prefix, backup_prefix = case_prefix + "live/", case_prefix + "backups/"
    cases, backups, roots = [], {}, []
    scenarios = ((CASE_IDS[0], b"", 0), (CASE_IDS[1], b"", 0),
                 (CASE_IDS[2], det("small", small_n), 0), (CASE_IDS[3], b"", large_count))
    with _frozen_time():
        try:
            for label, extra, large in scenarios:
                blobs = live_fixture(len(extra), large)
                original_sst = {k: hashlib.sha256(v).hexdigest() for k, v in blobs.items()}
                seed_live(client, bucket, live_prefix, blobs)
                e = dict(env, S3_ROOT=live_prefix, S3_BACKUP_PREFIX=backup_prefix,
                         _small_extra=extra, _large_count=large)
                out, bid, original_local, root, backup_env = run_backup(
                    bk, client, e, label, bucket, live_prefix, backup_prefix)
                roots.append(root)
                if label == CASE_IDS[1] and original_local != backups[cases[0]["backup_id"]][0]:
                    raise AssertionError("identical local fixture changed")
                cases.append(out)
                backups[bid] = (original_local, backup_env, original_sst)
            bids = [case["backup_id"] for case in cases]
            if len(set(bids)) != len(CASE_IDS):
                raise AssertionError("backup IDs collided")
            roles = dict(zip(bids, ("victim", "oldest-retained", "pinned", "newest")))
            if all(hasattr(bk, name) for name in RETENTION_API):
                dry = retention_eval(bk, client, backups[bids[-1]][1], bids[2], "dry-run")
                if dry["delete_ids"] != [bids[0]] or set(dry["keep"]) != set(bids[1:]):
                    raise AssertionError("retention selected wrong generation roles")
                enforced = retention_eval(bk, client, backups[bids[-1]][1], bids[2], "enforce")
                if set(enforced["remaining"]) != set(bids[1:]):
                    raise AssertionError("retention removed a survivor")
                if enforced["remote_bytes_removed"] != dry["delete_bytes"]:
                    raise AssertionError("planned and actual retention deletion bytes differ")
                dry, enforced = retention_roles(dry, roles), retention_roles(enforced, roles)
            else:
                if tag == "current":
                    raise AttributeError("current retention API missing")
                dry = enforced = {"retention": "unavailable-in-baseline"}
            restores = [restore_one(bk, client, backups[bid][1], bid, backups[bid][0],
                                    roles[bid], live_prefix, backups[bid][2]) for bid in bids[1:]]
            oracle = {"fresh_restore_hash": True, "restores": restores,
                      "get_bytes": sum(r["oracle_get_bytes"] for r in restores),
                      "gets": sum(r["oracle_gets"] for r in restores)}
            auxiliary = bench_raw_can(bk, can, raw, client, bucket, case_prefix, env)
            return {"cases": cases, "retention_dry_run": dry, "retention_enforced": enforced,
                    "oracle": oracle, "auxiliary": auxiliary}
        finally:
            for root in roots:
                shutil.rmtree(root)


def compare_implementations(before, current):
    def by_case(variant):
        cases = variant["cases"]
        if len(cases) != len(CASE_IDS) or {c["case"] for c in cases} != set(CASE_IDS):
            raise ValueError("missing/duplicate required backup cases")
        if set(variant["auxiliary"]) != set(AUX_IDS):
            raise ValueError("missing required auxiliary cases")
        return {c["case"]: c for c in cases}

    base, candidate = by_case(before), by_case(current)
    metrics = ("lists", "gets", "heads", "copies", "puts", "deletes", "get_bytes", "put_bytes",
               "copy_bytes", "new_remote_bytes", "new_payload_bytes", "objects", "gen_bytes", "store_bytes")
    scenarios = []
    for name in CASE_IDS:
        b, c = base[name], candidate[name]
        if (b["live_bytes"], b["live_objects"], b["live_keys"]) != (c["live_bytes"], c["live_objects"], c["live_keys"]):
            raise AssertionError("baseline/current fixture denominators differ")
        scenarios.append({"case": name, **{key: c[key] - b[key] for key in metrics},
                          "wall_sec_delta": round(c["wall_sec"] - b["wall_sec"], 3)})
    auxiliary = {}
    for name in AUX_IDS:
        b, c = before["auxiliary"][name], current["auxiliary"][name]
        if (b["input_bytes"], b["input_objects"]) != (c["input_bytes"], c["input_objects"]):
            raise AssertionError("auxiliary baseline/current inputs differ")
        auxiliary[name] = {key: c[key] - b[key] for key in metrics}
    base_retention = before["retention_dry_run"]
    retention = ("current-only (baseline has no retention API)" if "retention" in base_retention
                 else base_retention["delete_ids"] == current["retention_dry_run"]["delete_ids"]
                 and base_retention["keep"] == current["retention_dry_run"]["keep"])

    def oracle_hashes(variant):
        return {r["role"]: (r["file_hashes"], r["sst_hashes"]) for r in variant["oracle"]["restores"]}

    hashes = oracle_hashes(before) == oracle_hashes(current)
    if not hashes:
        raise AssertionError("baseline/current original-fixture restore roles differ")
    return {"scenarios": scenarios, "auxiliary": auxiliary, "retention": retention, "role_hashes_match": hashes}


def cleanup_owned(client, bucket, prefixes):
    """Remove and verify only exact owned backup and RAW vehicle prefixes."""
    for prefix in prefixes:
        keys = sorted(listed_sizes(client, bucket, prefix))
        for start in range(0, len(keys), 1000):
            response = client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[start:start + 1000]]})
            if response.get("Errors"):
                raise IOError("owned prefix cleanup returned delete errors")
        if listed_sizes(client, bucket, prefix):
            raise IOError("owned prefix cleanup left objects")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--endpoint", default="", help="owned S3 URL; empty selects the S3 model")
    parser.add_argument("--bucket", default="bench-isolated")
    parser.add_argument("--user", default="")
    parser.add_argument("--password-env", default="BENCH_S3_SECRET")
    parser.add_argument("--baseline-root")
    parser.add_argument("--no-baseline", action="store_true")
    parser.add_argument("--json-out")
    parser.add_argument("--prefix", default="bench-backup-retention")
    parser.add_argument("--small-kb", type=int, default=1)
    parser.add_argument("--large-count", type=int, default=50)
    parser.add_argument("--region", default="us-east-1")
    return parser.parse_args(argv)


def failure_document(store="counting-fake"):
    return {"schema": SCHEMA, "benchmark": "backup-retention-before-current", "store": store,
            "status": "fail", "failures": [], "baseline_available": False}


def collect(args):
    doc = failure_document("real-s3" if args.endpoint else "counting-fake")
    owned = []
    phase = "configuration"
    try:
        if not args.baseline_root and not args.no_baseline:
            raise ValueError("missing --baseline-root (or pass --no-baseline)")
        if args.baseline_root and args.no_baseline:
            raise ValueError("--baseline-root and --no-baseline are mutually exclusive")
        if args.small_kb < 1 or args.large_count < 1:
            raise ValueError("change fixtures must be nonempty")
        if args.endpoint:
            if not args.user or not args.bucket or not os.environ.get(args.password_env):
                raise ValueError("real mode needs bucket, user and password-env credentials")
            if not args.prefix.strip("/"):
                raise ValueError("real mode requires a nonempty owned prefix")
            import boto3
            from botocore.config import Config
            real = boto3.client("s3", endpoint_url=args.endpoint, region_name=args.region,
                                aws_access_key_id=args.user, aws_secret_access_key=os.environ[args.password_env],
                                config=Config(retries={"max_attempts": 0},
                                              request_checksum_calculation="when_required",
                                              response_checksum_validation="when_required"))
            # Never create or assume ownership of somebody else's bucket.
            real.head_bucket(Bucket=args.bucket)
            clients = [InstrumentedClient(real), InstrumentedClient(real)]
        else:
            clients = [CountingStore(), CountingStore()]
        run_id = secrets.token_hex(16)
        root_prefix = args.prefix.strip("/") + "/" + run_id + "/"
        env = {"BACKUP_OFFLINE_CONFIRMED": "1", "GREPTIME_HTTP_URL": "http://127.0.0.1:9",
               "BACKUP_KEEP": "30", "GREPTIME_STORAGE_TYPE": "S3",
               "S3_ENDPOINT_URL": args.endpoint or "https://bench.invalid",
               "S3_BUCKET": args.bucket, "S3_REGION": args.region,
               "S3_ACCESS_KEY_ID": args.user or "bench",
               "S3_SECRET_ACCESS_KEY": os.environ[args.password_env] if args.endpoint else "bench",
               "BACKUP_REMOTE_RETENTION_MODE": "off"}
        for index, tag, root in ((0, "before", args.baseline_root), (1, "current", REPO)):
            if root is None:
                doc["baseline"] = "absent (--no-baseline)"
                continue
            phase = tag + " measurement"
            prefix = root_prefix + tag + "/"
            raw_prefix = "raw/vehicle/" + raw_vehicle(prefix) + "/"
            owned.append((clients[index], [prefix, raw_prefix]))
            with implementation(root) as (bk, can, raw):
                doc[tag] = bench_impl(bk, can, raw, clients[index], args.bucket, prefix,
                                      env, args.small_kb * 1024, args.large_count, tag)
        doc["baseline_available"] = "before" in doc
        if doc["baseline_available"]:
            phase = "comparison"
            doc["comparison"] = compare_implementations(doc["before"], doc["current"])
        doc["status"] = "ok"
    except BaseException as error:
        # Configuration errors are controlled text. Implementation exceptions
        # may contain endpoints/keys, so publish only their phase and type.
        detail = str(error) if phase == "configuration" and isinstance(error, ValueError) else type(error).__name__
        doc["failures"].append(phase + " failed: " + detail)
    finally:
        for client, prefixes in owned:
            try:
                cleanup_owned(client, args.bucket, prefixes)
            except BaseException as error:
                doc["failures"].append("owned cleanup failed: " + type(error).__name__)
        if doc["failures"]:
            doc["status"] = "fail"
    return doc


def emit(doc, path):
    text = json.dumps(doc, indent=2, sort_keys=True) + "\n"
    if path:
        try:
            with open(path, "w", encoding="utf-8") as out:
                out.write(text)
        except OSError as error:
            doc["status"] = "fail"
            doc["failures"].append("JSON output failed: " + type(error).__name__)
            sys.stdout.write(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    else:
        sys.stdout.write(text)
    return 0 if doc["status"] == "ok" else 1


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = parse_args(argv)
    except SystemExit as error:
        if error.code == 0:
            return 0
        doc = failure_document()
        doc["failures"].append("argument parsing failed")
        path = None
        for index, value in enumerate(argv):
            if value.startswith("--json-out="):
                path = value.split("=", 1)[1]
            elif value == "--json-out" and index + 1 < len(argv):
                path = argv[index + 1]
        return emit(doc, path)
    # Implementation output never contaminates machine-readable stdout.
    with contextlib.redirect_stdout(sys.stderr):
        doc = collect(args)
    return emit(doc, args.json_out)


if __name__ == "__main__":
    sys.exit(main())
