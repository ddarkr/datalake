#!/usr/bin/env python3
"""Same-fixture before/current backup+retention benchmark (isolated only).

Runs an identical deterministic fixture against the baseline backup.py
(--baseline-root) and the current scripts/storage/backup.py, then compares
actual LIST/GET/COPY/PUT counts, verification bytes, wall time, cumulative
store bytes, retention dry-run selection, and full-hash restores.

Fake (default, stdlib only, full metrics + hash equality):
  python3 tools/benchmark_backup_retention.py

Real owned MinIO (isolated bucket/prefix only; Main provides loopback):
  BENCH_S3_SECRET=<secret> python3 tools/benchmark_backup_retention.py \
    --endpoint http://127.0.0.1:PORT --bucket <isolated> --user <user> \
    --password-env BENCH_S3_SECRET \
    --baseline-root /tmp/datalake-issues-20261009-baseline

Safety protocol (matches scripts/storage/backup.py retention_settings):
  retention runs dry-run first; enforce only with
  BACKUP_REMOTE_RETENTION_MODE=enforce + BACKUP_REMOTE_RETENTION_APPROVE=1
  on the isolated bench prefix. Live and backup prefixes must not overlap.
  This runner never reads production BACKUP_*/S3_*/GREPTIME_* env: every
  value comes from CLI flags (secret from the named password env var) or
  fixed isolated defaults. No production TTL/deletion is ever performed.

Printed JSON: {"benchmark", "store", "before", "current", "comparison"}.
before/current each hold per-backup metrics, retention dry-run + enforce
results with op counts, remote bytes before/after with planned-vs-removed
reconciliation, and post-GC restores of oldest-retained/pinned/newest with
full file + SST snapshot hash matches. comparison holds the
current-minus-before metric deltas per scenario plus retention equality.
"""

import argparse
import contextlib
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from scripts.storage import backup as cur  # noqa: E402


def det(seed, n):
    """Deterministic fixture bytes (identical for before and current runs)."""
    out = bytearray()
    c = 0
    while len(out) < n:
        out.extend(hashlib.sha256(("%s:%d" % (seed, c)).encode()).digest())
        c += 1
    return bytes(out[:n])


class CountingStore:
    """Fake S3 surface with request/byte counters (stdlib only)."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.reset()

    def reset(self):
        self.lists = self.gets = self.copies = self.puts = self.deletes = 0
        self.verify_bytes = 0

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        self.lists += 1
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k, "Size": len(self.objects[k])} for k in keys],
                "IsTruncated": False}

    def head_object(self, Bucket, Key):
        # Match S3: missing keys fail instead of returning a fake size.
        if Key not in self.objects:
            raise KeyError(Key)
        return {"ETag": '"bench"', "ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket, Key):
        # Missing keys raise KeyError, which backup.fetch_complete maps to
        # "no COMPLETE record" just like S3 NoSuchKey (stdlib only).
        if Key not in self.objects:
            raise KeyError(Key)
        self.gets += 1
        blob = self.objects[Key]
        # Count bytes actually read, matching the real InstrumentedClient.
        return {"Body": _CountedBody(io.BytesIO(blob), self),
                "ContentLength": len(blob)}

    def put_object(self, Bucket, Key, Body, ContentLength=None, Metadata=None):
        self.puts += 1
        if hasattr(Body, "read"):
            Body = Body.read()
        self.objects[Key] = bytes(Body)
        return {}

    def copy_object(self, Bucket, CopySource, Key):
        self.copies += 1
        self.objects[Key] = self.objects[CopySource["Key"]]
        return {}

    def delete_objects(self, Bucket, Delete):
        self.deletes += 1
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
        return {}


class _CountedBody:
    """Wrap a real GET body so verification bytes are actually measured."""

    def __init__(self, body, client):
        self._body = body
        self._client = client

    def read(self, n=-1):
        # botocore StreamingBody.read() takes amt=None for a full read;
        # passing -1 through is not a full read there (BytesIO accepts it).
        chunk = self._body.read() if n is None or n < 0 else self._body.read(n)
        self._client.verify_bytes += len(chunk)
        return chunk

    def close(self):
        return self._body.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __getattr__(self, name):
        return getattr(self._body, name)


class InstrumentedClient:
    """Counting wrapper around a real boto3 client (real actual metrics)."""

    def __init__(self, real):
        self.real = real
        self.reset()

    def reset(self):
        self.lists = self.gets = self.copies = self.puts = self.deletes = 0
        self.verify_bytes = 0

    def list_objects_v2(self, **kw):
        self.lists += 1
        return self.real.list_objects_v2(**kw)

    def head_object(self, **kw):
        return self.real.head_object(**kw)

    def get_object(self, **kw):
        self.gets += 1
        resp = self.real.get_object(**kw)
        resp["Body"] = _CountedBody(resp["Body"], self)
        return resp

    def put_object(self, **kw):
        self.puts += 1
        return self.real.put_object(**kw)

    def copy_object(self, **kw):
        self.copies += 1
        return self.real.copy_object(**kw)

    def delete_objects(self, **kw):
        self.deletes += 1
        return self.real.delete_objects(**kw)


def make_source(root, small_extra=b"", large_count=0, seed="src"):
    data = os.path.join(root, "greptime-data")
    etc = os.path.join(root, "greptime-etc")
    os.makedirs(os.path.join(data, "wal"), exist_ok=True)
    os.makedirs(os.path.join(etc, "auth"), exist_ok=True)
    with open(os.path.join(data, "wal", "seg001"), "wb") as f:
        f.write(det(seed + "/wal", 4096) + small_extra)
    for i in range(large_count):
        with open(os.path.join(data, "wal", "big-%04d" % i), "wb") as f:
            f.write(det("%s/big-%d" % (seed, i), 4096))
    with open(os.path.join(data, "meta.json"), "wb") as f:
        f.write(b'{"seq": 42}')
    with open(os.path.join(etc, "greptimedb.toml"), "wb") as f:
        f.write(b'[storage]\ntype = "S3"\n')
    with open(os.path.join(etc, "auth", "users"), "wb") as f:
        f.write(b"user=pw\n")


def complete_gens(bkmod, client, bucket, bp):
    """Complete generations newest-last; baseline shim via COMPLETE records."""
    if hasattr(bkmod, "remote_generations"):
        gens, _ = bkmod.remote_generations(client, bucket, bp)
        return [g for g in gens if g["status"] == "complete"]
    out = []
    listed = {o["Key"]: o.get("Size", 0)
              for o in bkmod.s3_list_all(client, bucket, bp)}
    for bid in bkmod.remote_complete_ids(client, bucket, bp):
        complete, err = bkmod.fetch_complete(client, bucket, bp, bid)
        assert err is None, bid + ": " + str(err)[:120]
        keys = sorted(k for k in listed if k.startswith(bkmod.remote_base(bp, bid)))
        out.append({"backup_id": bid, "status": "complete", "keys": keys,
                    "bytes": sum(listed[k] for k in keys)})
    return out


def hash_tree(base):
    out = {}
    for root, _dirs, files in os.walk(base):
        for name in files:
            path = os.path.join(root, name)
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            out[os.path.relpath(path, base)] = h.hexdigest()
    return out


def live_snapshot_hashes(bkmod, client, bucket, live_pfx):
    """SHA256 of every live SST object, hashed remotely (streamed)."""
    out = {}
    for o in bkmod.s3_list_all(client, bucket, live_pfx):
        rel = o["Key"][len(live_pfx):]
        out[rel] = bkmod.s3_stream_hash(client, bucket, o["Key"])[0]
    return out


def sst_snapshot_hashes(bkmod, client, bucket, backup_pfx, bid):
    """COMPLETE SST digests for one generation (manifest ground truth)."""
    complete, err = bkmod.fetch_complete(client, bucket, backup_pfx, bid)
    assert err is None, bid + ": " + str(err)[:120]
    return {o["key"]: o["sha256"] for o in complete["sst"]}
    return out


def load_baseline(root):
    """Load baseline backup.py isolated (current scripts.storage.backup kept)."""
    import importlib.util
    path = os.path.join(root, "scripts", "storage", "backup.py")
    if not os.path.isfile(path):
        raise SystemExit("baseline backup.py missing: " + path)
    spec = importlib.util.spec_from_file_location("bench_baseline_backup", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def live_fixture(small_n=0, large_count=0):
    live = {"sst/a.sst": det("live/a", 2048), "sst/b.sst": det("live/b", 512)}
    if small_n:
        live["sst/b.sst"] = det("live/b-small", 512 + small_n)
    for i in range(large_count):
        live["sst/big-%04d.sst" % i] = det("live/big-%d" % i, 2048)
    return live


def with_env(env):
    saved = dict(os.environ)
    for k in list(os.environ):
        if k.startswith(("BACKUP_", "S3_", "GREPTIME_")):
            del os.environ[k]
    os.environ.update(env)
    return saved


def restore_env(saved):
    os.environ.clear()
    os.environ.update(saved)



def run_backup(bkmod, client, env, label):
    """One measured backup; returns (metrics, backup_id, src_hash, tmp, env)."""
    tmp = tempfile.mkdtemp(prefix="bench-retention-")
    src = os.path.join(tmp, "src")
    tgt = os.path.join(tmp, "tgt")
    bdir = os.path.join(tmp, "bdir")
    os.makedirs(src)
    os.makedirs(tgt)
    os.makedirs(bdir)
    e = dict(env, BACKUP_DIR=bdir,
             BACKUP_SOURCE_DIRS=os.path.join(src, "greptime-data") + ":"
             + os.path.join(src, "greptime-etc"),
             BACKUP_RESTORE_BASE=tgt)
    make_source(src, env["_small_extra"], env["_large_count"],
                seed=label.removesuffix("-2"))
    src_hash = hash_tree(src)
    case_pfx = env["_BENCH_CASE"]
    saved = with_env({k: v for k, v in e.items() if not k.startswith("_")})
    old_client = bkmod.make_s3_client
    before = (client.lists, client.gets, client.copies, client.puts, client.verify_bytes)
    try:
        bkmod.make_s3_client = lambda: client
        start = time.time()
        assert bkmod.cmd_backup() == 0, label + ": backup failed"
        wall = time.time() - start
        m = {"lists": client.lists - before[0], "gets": client.gets - before[1],
             "copies": client.copies - before[2], "puts": client.puts - before[3],
             "verify_bytes": client.verify_bytes - before[4]}
        bp = bkmod.norm_prefix(os.environ["S3_BACKUP_PREFIX"])
        complete = complete_gens(bkmod, client, os.environ["S3_BUCKET"], bp)
        assert complete, label + ": no complete generations"
        bid = complete[-1]["backup_id"]
        blobs = bkmod.s3_list_all(client, os.environ["S3_BUCKET"],
                                  bkmod.norm_prefix(case_pfx))
        out = {"case": label, "backup_id": bid,
               "objects": len(complete[-1]["keys"]),
               "gen_bytes": complete[-1]["bytes"],
               "wall_sec": round(wall, 3),
               "store_bytes": sum(o.get("Size", 0) for o in blobs),
               "generations": len(complete)}
        out.update(m)
        return out, bid, src_hash, tmp, e
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    finally:
        restore_env(saved)
        bkmod.make_s3_client = old_client


def restore_one(bkmod, client, env, bid, want_hash, label, live_pfx):
    """Restore into an empty isolated root; compare all file + SST hashes.

    Compares every restored file hash plus the live SST snapshot hashes after
    restore (not just the tar): live snapshot objects under the isolated live
    prefix are hashed remotely and matched against the COMPLETE SST digests.
    Returns the role name with hashes and match flags.
    """
    e = dict(env, BACKUP_FILE=bid)
    e = {k: v for k, v in e.items() if not k.startswith("_")}
    saved = with_env(e)
    old_client = bkmod.make_s3_client
    try:
        bkmod.make_s3_client = lambda: client
        # Snapshot restore refuses conflicting live keys: clear the isolated
        # live prefix first so each restore starts from an empty live root.
        live_keys = [o["Key"] for o in bkmod.s3_list_all(
            client, e["S3_BUCKET"], bkmod.norm_prefix(live_pfx))]
        if live_keys:
            bkmod.s3_delete_keys(client, e["S3_BUCKET"], live_keys)
        for lbl in bkmod.LABELS:
            os.makedirs(os.path.join(e["BACKUP_RESTORE_BASE"], lbl), exist_ok=True)
            bkmod.clear_dir(os.path.join(e["BACKUP_RESTORE_BASE"], lbl))
        assert bkmod.cmd_restore() == 0, label + ": restore failed"
        got = hash_tree(os.environ["BACKUP_RESTORE_BASE"])
        assert got == want_hash, label + ": hash mismatch"
        snap_hashes = live_snapshot_hashes(
            bkmod, client, e["S3_BUCKET"], bkmod.norm_prefix(live_pfx))
        want_sst = sst_snapshot_hashes(bkmod, client, e["S3_BUCKET"],
                                       bkmod.norm_prefix(e["S3_BACKUP_PREFIX"]),
                                       bid)
        assert snap_hashes == want_sst, label + ": SST snapshot hash mismatch"
        for lbl in bkmod.LABELS:
            bkmod.clear_dir(os.path.join(e["BACKUP_RESTORE_BASE"], lbl))
        return {"role": label, "restored": bid, "files": len(want_hash),
                "file_hashes_match": got == want_hash,
                "file_hashes": got,
                "sst_hashes_match": snap_hashes == want_sst,
                "sst_hashes": snap_hashes}
    finally:
        restore_env(saved)
        bkmod.make_s3_client = old_client


def has_retention(bkmod):
    return all(hasattr(bkmod, n) for n in
               ("retention_settings", "retention_plan", "cmd_retention"))


def retention_eval(bkmod, client, env, pinned, mode):
    """Same code path as cmd_retention minus prints; enforce needs approval.

    Returns per-phase measured LIST/GET/COPY/PUT/delete counts so dry-run and
    enforce I/O are both visible. Enforce additionally reports measured
    removed and remaining remote bytes.
    """
    e = {k: v for k, v in env.items() if not k.startswith("_")}
    e.update({"BACKUP_REMOTE_RETENTION_MODE": mode,
              "BACKUP_REMOTE_RETAIN_COUNT": "3",
              "BACKUP_REMOTE_MIN_RECOVERY": "1",
              "BACKUP_REMOTE_PINNED": pinned})
    if mode == "enforce":
        e["BACKUP_REMOTE_RETENTION_APPROVE"] = "1"
    saved = with_env(e)
    old_client = bkmod.make_s3_client
    before = (client.lists, client.gets, client.copies, client.puts,
              client.deletes, client.verify_bytes)
    try:
        bkmod.make_s3_client = lambda: client
        if mode == "enforce":
            bp = bkmod.norm_prefix(e["S3_BACKUP_PREFIX"])
            blobs = bkmod.s3_list_all(client, e["S3_BUCKET"], bp)
            bytes_before = sum(o.get("Size", 0) for o in blobs)
            with contextlib.redirect_stdout(io.StringIO()):
                assert bkmod.cmd_retention() == 0, "enforce failed"
        else:
            settings, err = bkmod.retention_settings()
            assert err is None, err
            bp = bkmod.norm_prefix(os.environ["S3_BACKUP_PREFIX"])
            gens, _ = bkmod.remote_generations(client, os.environ["S3_BUCKET"], bp)
            keep, deletes, skipped = bkmod.retention_plan(gens, settings)
            m = {"lists": client.lists - before[0], "gets": client.gets - before[1],
                 "copies": client.copies - before[2], "puts": client.puts - before[3],
                 "deletes": client.deletes - before[4],
                 "verify_bytes": client.verify_bytes - before[5]}
            out = {"keep": keep,
                   "delete_ids": [g["backup_id"] for g in deletes],
                   "delete_bytes": sum(g["bytes"] for g in deletes),
                   "skipped": [[b, w] for b, w in skipped]}
            out.update(m)
            return out
        bp = bkmod.norm_prefix(os.environ["S3_BACKUP_PREFIX"])
        gens, _ = bkmod.remote_generations(client, os.environ["S3_BUCKET"], bp)
        complete = [g["backup_id"] for g in gens if g["status"] == "complete"]
        blobs = bkmod.s3_list_all(client, os.environ["S3_BUCKET"], bp)
        bytes_after = sum(o.get("Size", 0) for o in blobs)
        m = {"lists": client.lists - before[0], "gets": client.gets - before[1],
             "copies": client.copies - before[2], "puts": client.puts - before[3],
             "deletes": client.deletes - before[4],
             "verify_bytes": client.verify_bytes - before[5]}
        out = {"remaining": complete, "remote_bytes_before": bytes_before,
               "remote_bytes_after": bytes_after,
               "remote_bytes_removed": bytes_before - bytes_after}
        out.update(m)
        return out
    finally:
        restore_env(saved)
        bkmod.make_s3_client = old_client


def seed_live(client, bucket, live_pfx, blobs):
    for rel, blob in blobs.items():
        client.put_object(Bucket=bucket, Key=live_pfx + rel, Body=blob,
                          ContentLength=len(blob))
    client.reset()


def bench_impl(bkmod, client, bucket, case_pfx, base_env, small_n, large_count, tag):
    """Same fixture for one implementation: 2x identical + small + large."""
    live_pfx = case_pfx + "live/"
    backup_pfx = case_pfx + "backups/"
    scenarios = [("repeated-identical", b"", 0), ("repeated-identical-2", b"", 0),
                 ("small-change", det("small", small_n), 0),
                 ("large-change", b"", large_count)]
    cases, by_bid, tmps = [], {}, []
    for label, extra, large in scenarios:
        seed_live(client, bucket, live_pfx,
                  {k: v for k, v in live_fixture(len(extra), large).items()})
        env = dict(base_env, S3_ROOT=live_pfx, S3_BACKUP_PREFIX=backup_pfx,
                   _BENCH_CASE=case_pfx,
                   _small_extra=extra, _large_count=large)
        out, bid, src_hash, tmp, e = run_backup(bkmod, client, env, label)
        if label == "repeated-identical-2":
            assert src_hash == by_bid[cases[0]["backup_id"]][0]
        cases.append(out)
        by_bid[bid] = (src_hash, tmp, e)
        tmps.append(tmp)
    try:
        bp = bkmod.norm_prefix(backup_pfx)
        e0 = {k: v for k, v in by_bid[cases[0]["backup_id"]][2].items()
              if not k.startswith("_")}
        saved = with_env(dict(e0, S3_ROOT=live_pfx, S3_BACKUP_PREFIX=backup_pfx))
        try:
            complete = complete_gens(bkmod, client, bucket, bp)
        finally:
            restore_env(saved)
        assert len(complete) == 4, "expected 4 complete generations"
        # retain=3 keeps ids1/2/3; pin mid ids2 so oldest-retained, pinned, and
        # newest are three distinct survivors and ids0 is the only candidate.
        victim, oldest_retained, pinned_bid, newest = (
            complete[0]["backup_id"], complete[1]["backup_id"],
            complete[2]["backup_id"], complete[-1]["backup_id"])
        assert len({victim, oldest_retained, pinned_bid, newest}) == 4
        if not has_retention(bkmod):
            restores = [restore_one(bkmod, client, by_bid[bid][2], bid,
                                    by_bid[bid][0], tag + "-pregc-" + role,
                                    live_pfx)
                        for bid, role in ((oldest_retained, "oldest-retained"),
                                          (pinned_bid, "pinned"),
                                          (newest, "newest"))]
            seed_live(client, bucket, live_pfx, live_fixture(0, 0))
            return {"cases": cases, "retention_dry_run": {"retention": "unsupported"},
                    "retention_enforced": {"retention": "unsupported"},
                    "restores": restores}
        dry = retention_eval(bkmod, client, by_bid[newest][2], pinned_bid, "dry-run")
        assert pinned_bid in dry["keep"], "pinned must be retained"
        assert oldest_retained in dry["keep"], "oldest-retained must be kept"
        assert newest in dry["keep"], "newest must be retained"
        assert dry["delete_ids"] == [victim], "only ids0 deletable, got %r" % dry
        enforced = retention_eval(bkmod, client, by_bid[newest][2], pinned_bid, "enforce")
        assert victim not in enforced["remaining"], "ids0 must be deleted"
        for bid in (oldest_retained, pinned_bid, newest):
            assert bid in enforced["remaining"], bid + " must survive enforce"
        # Planned-vs-actual: dry-run bytes must equal measured removed bytes.
        assert enforced["remote_bytes_removed"] == dry["delete_bytes"], (
            "planned %d != removed %d" % (dry["delete_bytes"],
                                          enforced["remote_bytes_removed"]))
        # AFTER enforcement: restore each survivor from remote into fresh empty
        # roots; every file hash plus every live SST snapshot hash must match.
        restores = [restore_one(bkmod, client, by_bid[bid][2], bid, by_bid[bid][0],
                                role, live_pfx)
                    for bid, role in ((oldest_retained, "oldest-retained"),
                                      (pinned_bid, "pinned"),
                                      (newest, "newest"))]
        # Reseed the isolated live prefix so restores leave no owned keys behind.
        seed_live(client, bucket, live_pfx, live_fixture(0, 0))
        return {"cases": cases, "retention_dry_run": dry,
                "retention_enforced": enforced, "restores": restores}
    finally:
        for tmp in tmps:
            shutil.rmtree(tmp, ignore_errors=True)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--endpoint", default="",
                   help="owned MinIO http(s) URL; empty selects the local mock")
    p.add_argument("--bucket", default="bench-isolated")
    p.add_argument("--user", default="")
    p.add_argument("--password-env", default="BENCH_S3_SECRET",
                   help="env var NAME holding the secret (value never printed)")
    p.add_argument("--baseline-root", default="/tmp/datalake-issues-20261009-baseline")
    p.add_argument("--prefix", default="bench-backup-retention")
    p.add_argument("--small-kb", type=int, default=1)
    p.add_argument("--large-count", type=int, default=50)
    p.add_argument("--region", default="us-east-1")
    return p.parse_args()


def main():
    args = parse_args()
    real = bool(args.endpoint)
    if real and (not args.bucket or not args.user or not os.environ.get(args.password_env)):
        raise SystemExit("real mode needs --bucket, --user, and $%s set" % args.password_env)
    baseline = load_baseline(args.baseline_root)
    endpoint = args.endpoint or "https://bench.invalid"
    secret = os.environ.get(args.password_env, "bench") if real else "bench"
    key = args.user or "bench"
    base_env = {"BACKUP_DIR": "", "BACKUP_SOURCE_DIRS": "",
                "BACKUP_RESTORE_BASE": "", "BACKUP_OFFLINE_CONFIRMED": "1",
                "GREPTIME_HTTP_URL": "http://127.0.0.1:9", "BACKUP_KEEP": "30",
                "GREPTIME_STORAGE_TYPE": "S3", "S3_ENDPOINT_URL": endpoint,
                "S3_BUCKET": args.bucket, "S3_REGION": args.region,
                "S3_ACCESS_KEY_ID": key, "S3_SECRET_ACCESS_KEY": secret,
                "BACKUP_REMOTE_RETENTION_MODE": "off"}
    stamp = "%d-" % int(time.time())
    if real:
        import boto3  # noqa: pinned dep, only for the real-store run
        from botocore.config import Config
        raw = boto3.client("s3", endpoint_url=args.endpoint, region_name=args.region,
                           aws_access_key_id=args.user,
                           aws_secret_access_key=os.environ[args.password_env],
                           config=Config(retries={"max_attempts": 1},
                                         request_checksum_calculation="when_required",
                                         response_checksum_validation="when_required"))
        from botocore.exceptions import ClientError
        try:
            raw.create_bucket(Bucket=args.bucket)
        except ClientError as e:
            # Idempotent only: an already-owned bucket is fine; auth, network,
            # or permission failures must fail loudly, never pass silently.
            if e.response.get("Error", {}).get("Code") not in (
                    "BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise
        before_client = InstrumentedClient(raw)
        current_client = InstrumentedClient(raw)
    else:
        # Default counting-fake branch: isolated stores per implementation so
        # the same fixture runs against identical but independent state.
        before_client = CountingStore()
        current_client = CountingStore()
    def case_pfx(tag):
        if real:
            return "%s/%s%s/" % (args.prefix.strip("/"), stamp, tag)
        return "bench/%s%s/" % (stamp, tag)

    def cleanup():
        if not real:
            return
        # Best effort: remove only this run's owned stamp prefix. A cleanup
        # failure must not mask the benchmark result, so warn and continue.
        try:
            token = None
            while True:
                kw = {"Bucket": args.bucket,
                      "Prefix": "%s/%s" % (args.prefix.strip("/"), stamp)}
                if token:
                    kw["ContinuationToken"] = token
                resp = raw.list_objects_v2(**kw)
                keys = [o["Key"] for o in resp.get("Contents") or []]
                for i in range(0, len(keys), 1000):
                    raw.delete_objects(
                        Bucket=args.bucket,
                        Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                    if not token:
                        raise IOError("cleanup listing truncated without token")
                else:
                    return
        except Exception as e:
            sys.stderr.write("bench: warning: cleanup failed: %s\n" % str(e)[:150])
    try:
        before = bench_impl(baseline, before_client, args.bucket, case_pfx("before"),
                            base_env, args.small_kb * 1024, args.large_count, "before")
        current = bench_impl(cur, current_client, args.bucket, case_pfx("current"),
                             base_env, args.small_kb * 1024, args.large_count, "current")
    finally:
        cleanup()
    comparison = {
        "scenarios": [{k: c[k] - b[k] for k in
                       ("lists", "gets", "copies", "puts", "verify_bytes",
                        "objects", "gen_bytes", "store_bytes")}
                      | {"wall_sec_delta": round(c["wall_sec"] - b["wall_sec"], 3),
                         "case": b["case"]}
                      for b, c in zip(before["cases"], current["cases"])],
        "retention": ("current-only (baseline has no retention API)"
                      if before["retention_dry_run"].get("retention") == "unsupported"
                      else before["retention_dry_run"]["delete_ids"]
                      == current["retention_dry_run"]["delete_ids"]),
    }
    sys.stdout.write(json.dumps({"benchmark": "backup-retention-before-current",
                                 "store": "real-s3" if real else "counting-fake",
                                 "before": before, "current": current,
                                 "comparison": comparison},
                                indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
