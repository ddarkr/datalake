"""Behavioral regression tests for scripts/backup.py.

Framework-free: plain asserts + in-process S3 fake (no network, no real B2).
Run with: python3 tests/test_backup.py
(Not run here by siblings: project-wide validation is the parent's job.)

Behavioral checks (each fails against the named defect, passes fixed):
  (1) COMPLETE-gated restore: tampered COMPLETE digest, missing remote
      sidecar, and local S3 tars from failed uploads (no COMPLETE) are all
      refused; a missing sidecar is refused, never silently accepted.
  (2) Rollback ownership: corrupt snapshot copy / corrupt upload / corrupt
      COMPLETE write leave zero owned keys behind (incl. the COMPLETE
      marker itself — no dangling COMPLETE).
  (3) Content (streaming SHA256), never ETag: multipart-style ETags do not
      break backup/restore; bit-rotted snapshot copies and uploads are
      caught by hash; short-read truncations fail rather than verify.
  (4) Namespace claim: a second backup into an occupied remote id fails
      closed and deletes nothing of the existing backup.
  (5) Exact download: nested prefix inventory never resolves as the
      archive; downloads stream to .tmp and only atomically appear after
      COMPLETE verification (a failed download leaves no tar behind).
  Plus the standing contract: offline refusal (live probe), File-mode
  byte roundtrip, matching-live reuse (no out-of-band deletion of intact
  SSTs; missing-only copy; conflict refusal without overwrite; rollback of
  only own writes), restore copy-failure own-write rollback, HTTPS-only
  object endpoints, legacy-record refusal, nonempty-target preservation,
  dead force flag, subset/traversal/symlink/torn(sidecar mismatch/identity/
  prune rules, overlap and missing-S3 fail-closed, incomplete prefixes
  ignored.

Real B2 is NOT verified here (no live credentials in this environment);
the fake streams chunks like the S3 API and the parent gates real runs.
"""

import hashlib
import http.server
import io
import json
import os
import shutil
import stat
import sys
import tarfile
import tempfile
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import backup as bk

CLOSED_URL = "http://127.0.0.1:9"
B2ENV = {
    "S3_ENDPOINT_URL": "https://s3.test.backblazeb2.com",
    "S3_BUCKET": "test-bucket",
    "S3_REGION": "test-region",
    "S3_ACCESS_KEY_ID": "test-key",
    "S3_SECRET_ACCESS_KEY": "test-secret",
    "S3_ROOT": "test-root",
    "S3_BACKUP_PREFIX": "test-backups",
}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


def serve():
    srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


class FakeStream:
    """Chunked reader: backup.py must stream, never full-read large objects."""

    def __init__(self, blob, limit=None):
        self._blob = blob
        self._pos = 0
        self._limit = len(blob) if limit is None else limit
        self.closed = False

    def read(self, n=-1):
        assert not self.closed, "read after close"
        assert n is not None and n >= 0, "unbounded remote read"
        if self._pos >= self._limit:
            return b""
        chunk = self._blob[self._pos:self._pos + n]
        self._pos += len(chunk)
        return chunk

    def close(self):
        self.closed = True


class FakeS3:
    """Same surface backup.py drives: list/head/get/put/copy/delete.

    get_object returns a bounded-read-only stream tracked for close assertions.
    corrupt dict maps Key -> bytes actually stored on GET (simulating
    bit-rot the stream hash must catch). put_store maps Key -> bytes the
    fake keeps regardless of what was sent (simulating a lying transport).
    """

    def __init__(self, objects=None, fail_copy_at=None, fail_put_key=None,
                 corrupt=None, put_store=None, drop_tail=None):
        self.objects = dict(objects or {})
        self.fail_copy_at = fail_copy_at
        self.fail_put_key = fail_put_key
        self.corrupt = dict(corrupt or {})
        self.put_store = dict(put_store or {})
        self.drop_tail = dict(drop_tail or {})
        self.copies = 0
        self.deleted = []
        self.streams = []

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        if ContinuationToken is not None:
            keys = [k for k in keys if k >= ContinuationToken]
            return {"Contents": [{"Key": k, "Size": len(self.objects[k])}
                                 for k in keys], "IsTruncated": False}
        page = keys[:2]
        rest = keys[2:]
        out = {"Contents": [{"Key": k, "Size": len(self.objects[k])} for k in page]}
        if rest:
            out["IsTruncated"] = True
            out["NextContinuationToken"] = rest[0]
        else:
            out["IsTruncated"] = False
        return out

    def head_object(self, Bucket, Key):
        blob = self.objects[Key]
        import hashlib as _h
        fake_etag = '"deadbeef-multipart-style"'
        return {"ETag": fake_etag, "ContentLength": len(blob)}

    def get_object(self, Bucket, Key):
        blob = self.corrupt.get(Key, self.objects[Key])
        if Key in self.drop_tail:
            blob = blob[:self.drop_tail[Key]]
        stream = FakeStream(blob)
        self.streams.append(stream)
        return {"Body": stream, "ContentLength": len(blob)}

    def put_object(self, Bucket, Key, Body, ContentLength=None, Metadata=None):
        if self.fail_put_key is not None and Key == self.fail_put_key:
            raise IOError("injected put failure")
        if hasattr(Body, "read"):
            chunks = []
            while True:
                c = Body.read(1 << 20)
                if not c:
                    break
                chunks.append(c)
            Body = b"".join(chunks)
        self.objects[Key] = self.put_store.get(Key, bytes(Body))
        return {}

    def copy_object(self, Bucket, CopySource, Key):
        if self.fail_copy_at is not None and self.copies >= self.fail_copy_at:
            raise IOError("injected copy failure")
        self.copies += 1
        self.objects[Key] = self.put_store.get(
            Key, self.objects[CopySource["Key"]])
        return {}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.deleted.append(o["Key"])
            self.objects.pop(o["Key"], None)
        return {}


_SAVED = dict(os.environ)
_ORIG_MAKE_S3 = bk.make_s3_client


def setenv(mapping):
    for k in list(os.environ):
        if (k.startswith("BACKUP_") or k in ("GREPTIME_HTTP_URL",
                                             "GREPTIME_STORAGE_TYPE",
                                             "S3_ENDPOINT_URL", "S3_BUCKET", "S3_REGION",
                                             "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY",
                                             "S3_ROOT", "S3_BACKUP_PREFIX")):
            del os.environ[k]
    os.environ.update(mapping)


def restore_env():
    os.environ.clear()
    os.environ.update(_SAVED)
    bk.make_s3_client = _ORIG_MAKE_S3


def use_fake(fake):
    if fake is None:
        bk.make_s3_client = _ORIG_MAKE_S3
    else:
        bk.make_s3_client = lambda: fake


def make_tree(base):
    data = os.path.join(base, "greptime-data")
    etc = os.path.join(base, "greptime-etc")
    os.makedirs(os.path.join(data, "wal", "nested"))
    os.makedirs(os.path.join(etc, "auth"))
    payloads = {
        os.path.join(data, "wal", "seg001"): os.urandom(4096),
        os.path.join(data, "wal", "nested", "seg002"): os.urandom(1024),
        os.path.join(data, "meta.json"): b'{"seq": 42, "regions": ["a", "b"]}',
        os.path.join(etc, "greptimedb.toml"): b'[storage]\ntype = "S3"\n',
        os.path.join(etc, "auth", "users"): b"user=pw\n",
    }
    for path, blob in payloads.items():
        with open(path, "wb") as f:
            f.write(blob)
    return payloads


def hash_tree(base):
    out = {}
    for root, _dirs, files in os.walk(base):
        for name in files:
            path = os.path.join(root, name)
            if os.path.islink(path):
                out[os.path.relpath(path, base)] = "symlink"
                continue
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            out[os.path.relpath(path, base)] = h.hexdigest()
    return out


def sst_fixture():
    return {
        "test-root/sst/part-0001.sst": os.urandom(2048),
        "test-root/sst/part-0002.sst": os.urandom(512),
        "test-root/manifest.json": b'{"seq": 7}',
    }


def fresh_layout(mode="S3", fake=None):
    tmp = tempfile.mkdtemp(prefix="backup-test-")
    src = os.path.join(tmp, "src")
    tgt = os.path.join(tmp, "tgt")
    bdir = os.path.join(tmp, "bdir")
    os.makedirs(src)
    os.makedirs(tgt)
    os.makedirs(bdir)
    make_tree(src)
    base_env = {
        "BACKUP_DIR": bdir,
        "BACKUP_SOURCE_DIRS": os.path.join(src, "greptime-data") + ":"
        + os.path.join(src, "greptime-etc"),
        "BACKUP_RESTORE_BASE": tgt,
        "BACKUP_OFFLINE_CONFIRMED": "1",
        "GREPTIME_HTTP_URL": CLOSED_URL,
        "BACKUP_KEEP": "7",
        "GREPTIME_STORAGE_TYPE": mode,
    }
    base_env.update(B2ENV)
    use_fake(fake)
    return tmp, src, tgt, bdir, base_env


def newest_tar(bdir):
    files = sorted(f for f in os.listdir(bdir) if f.endswith(".tar.gz"))
    assert files, "expected a backup tar in " + bdir
    return os.path.join(bdir, files[-1])


def read_manifest(path):
    with tarfile.open(path, "r:gz") as tar:
        return json.loads(tar.extractfile("manifest.json").read().decode("utf-8"))


def test_backup_refuses_running_db():
    srv = serve()
    tmp, _src, _tgt, bdir, env = fresh_layout("File")
    try:
        env["GREPTIME_HTTP_URL"] = "http://127.0.0.1:%d/" % srv.server_address[1]
        setenv(env)
        assert bk.cmd_backup() == 2
        assert bk.cmd_restore() == 2
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
    finally:
        srv.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_backup_requires_offline_confirmation():
    tmp, _src, _tgt, bdir, env = fresh_layout("File")
    try:
        del env["BACKUP_OFFLINE_CONFIRMED"]
        setenv(env)
        assert bk.cmd_backup() == 2
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_file_mode_roundtrip_hash_match():
    tmp, src, tgt, bdir, env = fresh_layout("File")
    env["S3_BUCKET"] = ""  # File mode must not need any S3 account.
    try:
        setenv(env)
        before = hash_tree(src)
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        assert stat.S_IMODE(os.stat(tar).st_mode) == 0o600
        assert stat.S_IMODE(os.stat(tar + ".sha256").st_mode) == 0o600
        assert bk.cmd_restore() == 0
        assert hash_tree(tgt) == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_backup_writes_snapshot_and_complete():
    fake = FakeS3(sst_fixture())
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        manifest = read_manifest(tar)
        snap = manifest["sst_snapshot"]
        assert snap["bucket"] == "test-bucket"
        assert snap["src_prefix"] == "test-root/"
        assert len(snap["objects"]) == 3, snap
        bid = manifest["backup_id"]
        assert snap["snap_prefix"] == "test-backups/%s/sst/" % bid
        for obj in snap["objects"]:
            assert fake.objects[snap["snap_prefix"] + obj["key"]] is not None
        rbase = "test-backups/%s/" % bid
        assert rbase + os.path.basename(tar) in fake.objects
        assert rbase + os.path.basename(tar) + ".sha256" in fake.objects
        assert rbase + "manifest.json" in fake.objects
        complete = json.loads(fake.objects[rbase + "COMPLETE"].decode("utf-8"))
        with open(tar, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        assert complete["tar_sha256"] == digest
        assert complete["sst_count"] == 3
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_backup_copy_failure_rolls_back():
    fake = FakeS3(sst_fixture(), fail_copy_at=1)
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 3
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
        assert [k for k in fake.objects if k.startswith("test-backups/")] == []
        assert sorted(fake.objects) == sorted(sst_fixture())
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_backup_upload_failure_rolls_back_remote():
    fake = FakeS3(sst_fixture())
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    orig_put = fake.put_object

    def flaky(Bucket, Key, Body, ContentLength=None, Metadata=None):
        if Key.endswith(".tar.gz"):
            raise IOError("injected upload failure")
        return orig_put(Bucket, Key, Body, ContentLength, Metadata)
    fake.put_object = flaky
    try:
        setenv(env)
        assert bk.cmd_backup() == 3
        assert len([f for f in os.listdir(bdir) if f.endswith(".tar.gz")]) == 1
        assert [k for k in fake.objects if k.startswith("test-backups/")] == []
        assert sorted(fake.objects) == sorted(sst_fixture())
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_restore_downloads_and_verifies():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        before_src = hash_tree(os.path.join(tmp, "src"))
        assert bk.cmd_backup() == 0
        # Fresh host: empty local dir AND empty volumes/prefix.
        shutil.rmtree(os.path.join(tmp, "src"))
        for f in os.listdir(bdir):
            os.unlink(os.path.join(bdir, f))
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        assert bk.cmd_restore() == 0
        assert hash_tree(tgt) == before_src
        live = {k[len("test-root/"):]: v for k, v in fake.objects.items()
                if k.startswith("test-root/")}
        snap = {k.split("/sst/", 1)[1]: v for k, v in fake.objects.items()
                if k.startswith("test-backups/") and "/sst/" in k}
        assert len(live) == 3 and len(snap) == 3, (sorted(live), sorted(snap))
        assert live == snap
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_restore_reuses_matching_live_prefix():
    # Host/metadata loss with intact SSTs: matching live objects are reused
    # (no out-of-band deletion), missing keys are copied, unrelated live
    # objects are left untouched.
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        before_src = hash_tree(os.path.join(tmp, "src"))
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        manifest = read_manifest(tar)
        snap = manifest["sst_snapshot"]
        live_before = {k: v for k, v in fake.objects.items()
                       if k.startswith("test-root/")}
        assert live_before, "backup fixture must leave live SSTs in place"
        extra_key, extra_blob = "test-root/unrelated-note.txt", b"unrelated"
        fake.objects[extra_key] = extra_blob
        copies_before = fake.copies
        assert bk.cmd_restore() == 0
        assert hash_tree(tgt) == before_src
        live_after = {k: v for k, v in fake.objects.items()
                      if k.startswith("test-root/")}
        snap_live = {("test-root/" + o["key"]): fake.objects[snap["snap_prefix"] + o["key"]]
                     for o in snap["objects"]}
        for key, blob in snap_live.items():
            assert live_after[key] == blob
        assert live_after[extra_key] == extra_blob  # unrelated untouched
        assert fake.copies == copies_before  # all matched: nothing copied
        assert not [k for k in fake.deleted if k.startswith("test-root/")]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_restore_uploads_only_missing():
    # One snapshot key deleted after backup: restore copies exactly it and
    # repopulates the live prefix without touching the survivors.
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        manifest = read_manifest(tar)
        snap = manifest["sst_snapshot"]
        missing = "test-root/" + snap["objects"][0]["key"]
        survivor = "test-root/" + snap["objects"][1]["key"]
        survivor_blob = fake.objects[survivor]
        del fake.objects[missing]
        copies_before = fake.copies
        assert bk.cmd_restore() == 0
        assert fake.objects[missing] == fake.objects[snap["snap_prefix"] + snap["objects"][0]["key"]]
        assert fake.objects[survivor] == survivor_blob
        assert fake.copies == copies_before + 1  # exactly one missing key
        assert hash_tree(tgt) == hash_tree(os.path.join(tmp, "src"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_restore_conflict_refuses_without_overwrite():
    # Same live key, different bytes: refuse, overwrite nothing, keep targets
    # empty, and delete nothing of the live prefix.
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        manifest = read_manifest(tar)
        snap = manifest["sst_snapshot"]
        victim = "test-root/" + snap["objects"][0]["key"]
        conflict_blob = b"conflicting-bytes-not-snapshot"
        assert fake.objects[snap["snap_prefix"] + snap["objects"][0]["key"]] != conflict_blob
        fake.objects[victim] = conflict_blob
        before = hash_tree(tgt)
        assert bk.cmd_restore() == 2
        assert fake.objects[victim] == conflict_blob  # never overwritten
        assert hash_tree(tgt) == before  # local volumes untouched
        assert [k for k in fake.objects if k.startswith("test-root/")]
        assert not [k for k in fake.deleted if k.startswith("test-root/")]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()



def test_s3_client_refuses_plaintext_or_hostless_endpoint():
    # No profile may hand object credentials to boto3 over plaintext or a
    # hostless endpoint; the gate fires before any client is constructed.
    tmp, _src, _tgt, _bdir, env = fresh_layout("S3", FakeS3(sst_fixture()))
    try:
        setenv(env)
        orig = bk.make_s3_client
        bk.make_s3_client = _ORIG_MAKE_S3  # real gate, fake boto not needed
        for bad in ("http://s3.test.backblazeb2.com", "https://",
                    "s3.test.backblazeb2.com", "", "http://127.0.0.1:9000"):
            os.environ["S3_ENDPOINT_URL"] = bad
            try:
                _ORIG_MAKE_S3()
            except ValueError:
                pass
            except Exception as ex:
                raise AssertionError("wrong error for %r: %r" % (bad, ex))
            else:
                raise AssertionError("plaintext accepted: %r" % bad)
        bk.make_s3_client = orig
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_restore_partial_copy_failure_keeps_matching_live():
    # Missing key whose copy fails: only that key is rolled back; the
    # pre-existing matching SSTs are never in the rollback set.
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        manifest = read_manifest(newest_tar(bdir))
        snap = manifest["sst_snapshot"]
        missing = "test-root/" + snap["objects"][0]["key"]
        survivor = "test-root/" + snap["objects"][1]["key"]
        survivor_blob = fake.objects[survivor]
        del fake.objects[missing]
        fake.fail_copy_at = 0  # the single missing-key copy fails
        assert bk.cmd_restore() == 3
        assert missing not in fake.objects  # own write rolled back
        assert fake.objects[survivor] == survivor_blob  # reused key kept
        assert not [k for k in fake.deleted if k == survivor]
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_unreadable_live_sst_is_never_overwritten_or_deleted():
    import hashlib

    class UnreadableLive(FakeS3):
        def get_object(self, Bucket, Key):
            if Key == "live/data.sst":
                raise PermissionError("existing object is unreadable")
            return super().get_object(Bucket, Key)

    original = {"live/data.sst": b"existing data", "snapshot/data.sst": b"snapshot"}
    fake = UnreadableLive(original)
    objects = [{"key": "data.sst", "size": 8,
                "sha256": hashlib.sha256(b"snapshot").hexdigest()}]
    try:
        bk.s3_restore_snapshot(fake, "bucket", "live/",
                               {"snap_prefix": "snapshot/"}, objects)
    except PermissionError:
        pass
    else:
        raise AssertionError("unverifiable live object was accepted")
    assert fake.objects == original


def test_s3_restore_copy_failure_deletes_own_writes():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        fake.fail_copy_at = 1  # second object copy fails on restore
        assert bk.cmd_restore() == 3
        assert [k for k in fake.objects if k.startswith("test-root/")] == []
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_restore_refuses_legacy_archive():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        # Legacy tar: both labels + manifest but no sst_snapshot record.
        import io as _io
        legacy_manifest = json.dumps({
            "backup_id": "greptime-backup-legacy", "storage_type": "S3",
            "created_utc": "2000-01-01T00:00:00Z",
            "bucket": "test-bucket", "root": "test-root",
            "backup_prefix": "test-backups", "labels": ["greptime-data", "greptime-etc"],
        }).encode("utf-8")
        path = os.path.join(bdir, "greptime-backup-20000101T000000Z.tar.gz")
        with tarfile.open(path, "w:gz", format=tarfile.PAX_FORMAT) as tar:
            for label in ("greptime-data", "greptime-etc"):
                d = os.path.join(tmp, "leg", label)
                os.makedirs(d)
                with open(os.path.join(d, "f"), "wb") as f:
                    f.write(b"x")
                tar.add(d, arcname=label)
            info = tarfile.TarInfo("manifest.json")
            info.size = len(legacy_manifest)
            tar.addfile(info, _io.BytesIO(legacy_manifest))
        write_sidecar(path)
        assert bk.validate_manifest(json.loads(legacy_manifest)) is not None
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        env2 = dict(env, BACKUP_FILE=os.path.basename(path))
        setenv(env2)
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
        assert [k for k in fake.objects if k.startswith("test-root/")] == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_restore_ignores_incomplete_prefix():
    fake = FakeS3(sst_fixture())
    fake.objects["test-backups/partial-1/sst/test-root/sst/a"] = b"x"
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_restore() == 2  # no COMPLETE ids, refuses (nothing to fetch)
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_requires_b2_settings():
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", FakeS3(sst_fixture()))
    try:
        for k in ("S3_ENDPOINT_URL", "S3_BUCKET", "S3_ACCESS_KEY_ID"):
            env.pop(k, None)
        setenv(env)
        assert bk.cmd_backup() == 2
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_overlapping_prefixes_fail_closed():
    fake = FakeS3(sst_fixture())
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    try:
        env["S3_BACKUP_PREFIX"] = "test-root/backups"
        setenv(env)
        assert bk.cmd_backup() == 2
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
        env["S3_ROOT"] = "test-root/sub"
        env["S3_BACKUP_PREFIX"] = "test-root"
        setenv(env)
        assert bk.cmd_backup() == 2
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_refuses_nonempty_and_keeps_content():
    tmp, _src, tgt, bdir, env = fresh_layout("File")
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        assert bk.cmd_restore() == 0
        sentinel = os.path.join(tgt, "greptime-data", "sentinel.txt")
        with open(sentinel, "w", encoding="utf-8") as f:
            f.write("operator data - do not touch")
        before = hash_tree(tgt)
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_force_flag_is_dead():
    tmp, _src, tgt, bdir, env = fresh_layout("File")
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        assert bk.cmd_restore() == 0
        env["BACKUP_RESTORE_FORCE"] = "1"
        setenv(env)
        assert bk.cmd_restore() == 2
        assert os.path.isfile(os.path.join(tgt, "greptime-data", "meta.json"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def write_sidecar(path):
    with open(path, "rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    with open(path + ".sha256", "w", encoding="utf-8") as stream:
        stream.write(digest + "  " + os.path.basename(path) + "\n")


def write_raw_tar(bdir, name, add):
    path = os.path.join(bdir, name)
    with tarfile.open(path, "w:gz", format=tarfile.PAX_FORMAT) as tar:
        manifest = dict(bk.current_identity(), backup_id=name[:-7],
                        created_utc="2026-09-21T00:00:00Z",
                        labels=["greptime-data", "greptime-etc"])
        raw = json.dumps(manifest).encode()
        info = tarfile.TarInfo("manifest.json")
        info.size = len(raw)
        tar.addfile(info, io.BytesIO(raw))
        add(tar)
    write_sidecar(path)
    return path


def file_env(tmp, src, tgt, bdir):
    env = {
        "BACKUP_DIR": bdir,
        "BACKUP_SOURCE_DIRS": os.path.join(src, "greptime-data") + ":"
        + os.path.join(src, "greptime-etc"),
        "BACKUP_RESTORE_BASE": tgt,
        "BACKUP_OFFLINE_CONFIRMED": "1",
        "GREPTIME_HTTP_URL": CLOSED_URL,
        "BACKUP_KEEP": "7",
        "GREPTIME_STORAGE_TYPE": "File",
    }
    return env


def test_restore_rejects_subset_archive():
    tmp = tempfile.mkdtemp(prefix="backup-test-")
    src = os.path.join(tmp, "src")
    tgt = os.path.join(tmp, "tgt")
    bdir = os.path.join(tmp, "bdir")
    os.makedirs(src)
    os.makedirs(tgt)
    os.makedirs(bdir)
    make_tree(src)
    env = file_env(tmp, src, tgt, bdir)
    try:
        setenv(env)

        def add(tar):
            d = os.path.join(tmp, "only-data")
            os.makedirs(d)
            with open(os.path.join(d, "f"), "wb") as f:
                f.write(b"x")
            tar.add(d, arcname="greptime-data")

        write_raw_tar(bdir, "greptime-backup-subset.tar.gz", add)
        env2 = dict(env, BACKUP_FILE="greptime-backup-subset.tar.gz")
        setenv(env2)
        assert bk.cmd_restore() == 2
        assert os.listdir(tgt) == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_rejects_traversal_and_symlink():
    for tag in ("traversal", "symlink"):
        tmp = tempfile.mkdtemp(prefix="backup-test-")
        src = os.path.join(tmp, "src")
        tgt = os.path.join(tmp, "tgt")
        bdir = os.path.join(tmp, "bdir")
        os.makedirs(src)
        os.makedirs(tgt)
        os.makedirs(bdir)
        make_tree(src)
        env = file_env(tmp, src, tgt, bdir)
        try:
            setenv(env)

            def add(tar, tag=tag):
                for label in ("greptime-data", "greptime-etc"):
                    d = os.path.join(tmp, "evil-" + tag, label)
                    os.makedirs(d)
                    with open(os.path.join(d, "ok"), "wb") as f:
                        f.write(b"ok")
                    tar.add(d, arcname=label)
                if tag == "traversal":
                    info = tarfile.TarInfo("../evil")
                    blob = b"evil"
                    info.size = len(blob)
                    import io
                    tar.addfile(info, io.BytesIO(blob))
                else:
                    info = tarfile.TarInfo("greptime-data/link")
                    info.type = tarfile.SYMTYPE
                    info.linkname = "/etc/passwd"
                    tar.addfile(info)

            name = "greptime-backup-%s.tar.gz" % tag
            write_raw_tar(bdir, name, add)
            env2 = dict(env, BACKUP_FILE=name)
            setenv(env2)
            assert bk.cmd_restore() == 2, tag
            assert hash_tree(tgt) == {}, tag
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            restore_env()


def test_restore_rejects_torn_archive():
    tmp, _src, tgt, bdir, env = fresh_layout("File")
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        good = newest_tar(bdir)
        with open(good, "rb") as f:
            blob = f.read()
        torn = os.path.join(bdir, "greptime-backup-torn.tar.gz")
        with open(torn, "wb") as f:
            f.write(blob[:len(blob) // 3])
        write_sidecar(torn)
        env2 = dict(env, BACKUP_FILE=os.path.basename(torn))
        setenv(env2)
        assert bk.cmd_restore() == 3
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_rejects_sidecar_mismatch():
    tmp, _src, tgt, bdir, env = fresh_layout("File")
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        with open(tar + ".sha256", "w", encoding="utf-8") as f:
            f.write("0" * 64 + "  " + os.path.basename(tar) + "\n")
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_rejects_identity_change():
    tmp, _src, tgt, bdir, env = fresh_layout("File")
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        env2 = dict(env, GREPTIME_STORAGE_TYPE="S3")
        env2.update(B2ENV)
        setenv(env2)
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_prune_never_deletes_just_written():
    tmp, _src, _tgt, bdir, env = fresh_layout("File")
    try:
        env["BACKUP_KEEP"] = "2"
        setenv(env)
        for old in ("greptime-backup-20000101T000000Z.tar.gz",
                    "greptime-backup-20000102T000000Z.tar.gz"):
            with open(os.path.join(bdir, old), "wb") as f:
                f.write(b"old")
        assert bk.cmd_backup() == 0
        tars = sorted(f for f in os.listdir(bdir) if f.endswith(".tar.gz"))
        assert len(tars) == 2, tars
        assert not os.path.isfile(os.path.join(bdir, "greptime-backup-20000101T000000Z.tar.gz"))
        fresh = [f for f in tars if f not in (
            "greptime-backup-20000101T000000Z.tar.gz",
            "greptime-backup-20000102T000000Z.tar.gz")]
        assert len(fresh) == 1, tars
        assert os.path.isfile(os.path.join(bdir, fresh[0] + ".sha256"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def _remote_bid(bdir):
    tar = os.path.basename(newest_tar(bdir))
    assert tar.endswith(".tar.gz")
    return tar[:-len(".tar.gz")]


def test_complete_digest_tamper_refused():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        bid = _remote_bid(bdir)
        rbase = "test-backups/%s/" % bid
        raw = fake.objects[rbase + "COMPLETE"]
        doc = json.loads(raw.decode("utf-8"))
        doc["tar_sha256"] = "0" * 64
        fake.objects[rbase + "COMPLETE"] = json.dumps(doc, sort_keys=True).encode()
        for f in os.listdir(bdir):
            os.unlink(os.path.join(bdir, f))
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
        assert [k for k in fake.objects if k.startswith("test-root/")] == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_missing_sidecar_refused():
    # Both local File backups and cached S3 archives require integrity proof.
    for mode in ("File", "S3"):
        fake = FakeS3(sst_fixture())
        tmp, _src, tgt, bdir, env = fresh_layout(mode, fake)
        try:
            setenv(env)
            assert bk.cmd_backup() == 0
            tar = newest_tar(bdir)
            os.unlink(tar + ".sha256")
            fake.objects = {k: v for k, v in fake.objects.items()
                            if not k.startswith("test-root/")}
            assert bk.cmd_restore() == 2
            assert hash_tree(tgt) == {}
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            restore_env()


def test_missing_remote_sidecar_refused():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        bid = _remote_bid(bdir)
        rbase = "test-backups/%s/" % bid
        fake.objects.pop(rbase + os.path.basename(newest_tar(bdir)) + ".sha256")
        for f in os.listdir(bdir):
            os.unlink(os.path.join(bdir, f))
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        assert bk.cmd_restore() == 2
        assert os.listdir(bdir) == []
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_local_tar_from_failed_upload_refused():
    # Local tar whose off-host upload failed has no COMPLETE: not restorable.
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    orig_put = fake.put_object

    def flaky(Bucket, Key, Body, ContentLength=None, Metadata=None):
        if Key.endswith(".tar.gz"):
            raise IOError("injected upload failure")
        return orig_put(Bucket, Key, Body, ContentLength, Metadata)
    fake.put_object = flaky
    try:
        setenv(env)
        assert bk.cmd_backup() == 3
        fake.put_object = orig_put
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        # Local tar exists but no COMPLETE anywhere: must refuse.
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_corrupt_snapshot_copy_rolls_back():
    # Transport stores different bytes than copied: hash must catch it,
    # and the corrupt object must be in the rollback set.
    fake = FakeS3(sst_fixture())
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        fake.put_store.update({k: b"corrupt-bytes" for k in list(fake.objects)})
        # copy_object path: pre-seed corrupt destination for first SST key.
        orig_copy = fake.copy_object

        def lying_copy(Bucket, CopySource, Key):
            r = orig_copy(Bucket, CopySource, Key)
            fake.objects[Key] = b"corrupt-bytes"
            return r
        fake.copy_object = lying_copy
        assert bk.cmd_backup() == 3
        assert [k for k in fake.objects if k.startswith("test-backups/")] == []
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_corrupt_upload_rolls_back_including_complete():
    # COMPLETE write itself stores corrupt bytes: the marker must be
    # rolled back too (no dangling COMPLETE).
    fake = FakeS3(sst_fixture())
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        orig_put = fake.put_object

        def flaky_complete(Bucket, Key, Body, ContentLength=None, Metadata=None):
            r = orig_put(Bucket, Key, Body, ContentLength, Metadata)
            if Key.endswith("/COMPLETE"):
                fake.objects[Key] = b"corrupt-marker"
            return r
        fake.put_object = flaky_complete
        assert bk.cmd_backup() == 3
        assert [k for k in fake.objects if k.startswith("test-backups/")] == []
        assert len([f for f in os.listdir(bdir) if f.endswith(".tar.gz")]) == 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_multipart_etag_style_does_not_break():
    # Fake head_object always returns a bogus multipart-style ETag; the
    # workflow must still pass because it never compares ETags.
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        for f in os.listdir(bdir):
            os.unlink(os.path.join(bdir, f))
        assert bk.cmd_restore() == 0
        assert len(hash_tree(tgt)) == 5
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_bitrotted_snapshot_copy_detected():
    # GET returns different bytes than stored (bit-rot on the wire):
    # streaming hash must fail the backup, not bless it.
    fake = FakeS3(sst_fixture())
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        first = sorted(sst_fixture())[0]
        fake.corrupt[first] = b"bit-rot"
        assert bk.cmd_backup() == 3
        assert [k for k in fake.objects if k.startswith("test-backups/")] == []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_occupied_remote_namespace_fails_closed():
    # Force the id claim onto an occupied remote namespace (fixed random
    # suffix + pre-created objects there): backup must refuse and delete
    # nothing of the existing backup.
    fake = FakeS3(sst_fixture())
    tmp, _src, _tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        import backup as _bk
        orig_hex = _bk.secrets.token_hex
        orig_ns = _bk.time.time_ns
        fixed_ns = orig_ns()
        _bk.time.time_ns = lambda: fixed_ns
        _bk.secrets.token_hex = lambda n: "ab" * n
        try:
            assert bk.cmd_backup() == 0
            bid = _remote_bid(bdir)
            assert bid.endswith("-" + "ab" * 3), bid
            before = dict(fake.objects)
            # Second run claims the same id: remote namespace occupied.
            assert bk.cmd_backup() == 3
        finally:
            _bk.secrets.token_hex = orig_hex
            _bk.time.time_ns = orig_ns
        assert fake.objects == before
        rbase = "test-backups/%s/" % bid
        assert rbase + "COMPLETE" in fake.objects
        assert rbase + bid + ".tar.gz" in fake.objects
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_nested_inventory_never_resolves_as_archive():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        bid = _remote_bid(bdir)
        rbase = "test-backups/%s/" % bid
        # Attacker/accidental nested tar inside the sst/ tree.
        fake.objects[rbase + "sst/nested-" + bid + ".tar.gz"] = b"junk"
        for f in os.listdir(bdir):
            os.unlink(os.path.join(bdir, f))
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        assert bk.cmd_restore() == 0  # exact top-level match, nested ignored
        assert len(hash_tree(tgt)) == 5
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_failed_download_leaves_no_tar():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        bid = _remote_bid(bdir)
        rbase = "test-backups/%s/" % bid
        tar_key = rbase + bid + ".tar.gz"
        fake.drop_tail[tar_key] = len(fake.objects[tar_key]) // 2
        for f in os.listdir(bdir):
            os.unlink(os.path.join(bdir, f))
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        assert bk.cmd_restore() == 2
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
        assert [f for f in os.listdir(bdir) if f.endswith(".tmp")] == []
        assert hash_tree(tgt) == {}
        assert all(stream.closed for stream in fake.streams)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_duplicate_sst_keys_refused():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        import io as _io
        manifest = read_manifest(tar)
        manifest["sst_snapshot"]["objects"] = (
            manifest["sst_snapshot"]["objects"]
            + [manifest["sst_snapshot"]["objects"][0]])
        raw = json.dumps(manifest, indent=2, sort_keys=True).encode()
        with tarfile.open(tar, "r:gz") as src:
            members = [m for m in src.getmembers() if m.name != "manifest.json"]
            blobs = {m.name: src.extractfile(m).read()
                     for m in members if m.isfile()}
        with tarfile.open(tar, "w:gz", format=tarfile.PAX_FORMAT) as out:
            for m in members:
                if m.isfile():
                    info = tarfile.TarInfo(m.name)
                    info.size = len(blobs[m.name])
                    info.mode = m.mode
                    out.addfile(info, _io.BytesIO(blobs[m.name]))
                else:
                    out.addfile(m)
            info = tarfile.TarInfo("manifest.json")
            info.size = len(raw)
            info.mode = 0o600
            out.addfile(info, _io.BytesIO(raw))
        with open(tar, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        with open(tar + ".sha256", "w") as f:
            f.write(digest + "  " + os.path.basename(tar) + "\n")
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_wrong_snapshot_bucket_refused():
    fake = FakeS3(sst_fixture())
    tmp, _src, tgt, bdir, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        tar = newest_tar(bdir)
        import io as _io
        manifest = read_manifest(tar)
        manifest["sst_snapshot"]["bucket"] = "other-bucket"
        raw = json.dumps(manifest, indent=2, sort_keys=True).encode()
        with tarfile.open(tar, "r:gz") as src:
            members = [m for m in src.getmembers() if m.name != "manifest.json"]
            blobs = {m.name: src.extractfile(m).read()
                     for m in members if m.isfile()}
        with tarfile.open(tar, "w:gz", format=tarfile.PAX_FORMAT) as out:
            for m in members:
                if m.isfile():
                    info = tarfile.TarInfo(m.name)
                    info.size = len(blobs[m.name])
                    info.mode = m.mode
                    out.addfile(info, _io.BytesIO(blobs[m.name]))
                else:
                    out.addfile(m)
            info = tarfile.TarInfo("manifest.json")
            info.size = len(raw)
            info.mode = 0o600
            out.addfile(info, _io.BytesIO(raw))
        with open(tar, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        with open(tar + ".sha256", "w") as f:
            f.write(digest + "  " + os.path.basename(tar) + "\n")
        fake.objects = {k: v for k, v in fake.objects.items()
                        if not k.startswith("test-root/")}
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_latest_restore_uses_creation_order_within_one_second():
    tmp, src, tgt, _bdir, env = fresh_layout("File")
    orig_ns, orig_gmtime, orig_hex = bk.time.time_ns, bk.time.gmtime, bk.secrets.token_hex
    ticks = iter((1_789_999_999_000_000_001, 1_789_999_999_000_000_002))
    suffixes = iter(("ffffff", "000000"))
    fixed_time = orig_gmtime(1_789_999_999)
    try:
        setenv(env)
        bk.time.time_ns = lambda: next(ticks)
        bk.time.gmtime = lambda *args: fixed_time
        bk.secrets.token_hex = lambda n: next(suffixes)
        assert bk.cmd_backup() == 0
        with open(os.path.join(src, "greptime-data", "meta.json"), "wb") as f:
            f.write(b'{"seq": 43}')
        expected = hash_tree(src)
        assert bk.cmd_backup() == 0
        assert bk.cmd_restore() == 0
        assert hash_tree(tgt) == expected
    finally:
        bk.time.time_ns, bk.time.gmtime, bk.secrets.token_hex = orig_ns, orig_gmtime, orig_hex
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_large_sst_inventory_roundtrips_local_and_remote():
    inventory = {"test-root/data/" + str(i).zfill(5) + "-" + "a" * 64 + ".sst":
                 ("synthetic-sst-%d" % i).encode() for i in range(8001)}
    for remote in (False, True):
        fake = FakeS3(inventory)
        tmp, src, tgt, bdir, env = fresh_layout("S3", fake)
        try:
            setenv(env)
            expected = hash_tree(src)
            assert bk.cmd_backup() == 0
            bid = _remote_bid(bdir)
            rbase = "test-backups/%s/" % bid
            assert len(fake.objects[rbase + "manifest.json"]) > 1 << 20
            assert len(fake.objects[rbase + "COMPLETE"]) > 1 << 20
            if remote:
                for name in os.listdir(bdir):
                    os.unlink(os.path.join(bdir, name))
            fake.objects = {k: v for k, v in fake.objects.items()
                            if not k.startswith("test-root/")}
            assert bk.cmd_restore() == 0
            assert hash_tree(tgt) == expected
            assert {k: v for k, v in fake.objects.items()
                    if k.startswith("test-root/")} == inventory
            with open(os.path.join(bdir, "restore-verification.json")) as stream:
                marker = json.load(stream)
            assert marker["backup_id"] == bid
            assert marker["timestamp_seconds"] > 0
            assert all(stream.closed for stream in fake.streams)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            restore_env()


def test_inventory_limit_is_shared_by_backup_and_restore():
    fake = FakeS3(sst_fixture())
    tmp, src, tgt, bdir, env = fresh_layout("S3", fake)
    old_cap = bk.CONTROL_CAP
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        bid = _remote_bid(bdir)
        raw = fake.objects["test-backups/%s/manifest.json" % bid]
        bk.CONTROL_CAP = len(raw)
        assert bk.cmd_restore() == 0
        assert hash_tree(tgt) == hash_tree(src)
        before = dict(fake.objects)
        local_before = set(os.listdir(bdir))
        bk.CONTROL_CAP -= 1
        assert bk.cmd_backup() == 3
        assert fake.objects == before
        assert set(os.listdir(bdir)) == local_before
        for label in bk.LABELS:
            bk.clear_dir(os.path.join(tgt, label))
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
        assert all(stream.closed for stream in fake.streams)
    finally:
        bk.CONTROL_CAP = old_cap
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_oversized_complete_is_not_missing_and_closes_body():
    fake = FakeS3({"backups/id/COMPLETE": b"x" * 17})
    old_cap = bk.CONTROL_CAP
    try:
        bk.CONTROL_CAP = 16
        complete, error = bk.fetch_complete(fake, "bucket", "backups", "id")
        assert complete is None
        assert "too large" in error and "no COMPLETE" not in error
        assert fake.streams[-1].closed
        assert fake.streams[-1]._pos == 0  # ContentLength refuses before allocating.
        # Missing/lying length still enforces the bound during reads.
        get = fake.get_object
        fake.get_object = lambda **kw: {"Body": get(**kw)["Body"]}
        complete, error = bk.fetch_complete(fake, "bucket", "backups", "id")
        assert complete is None and "too large" in error
        assert fake.streams[-1].closed and fake.streams[-1]._pos == 17
    finally:
        bk.CONTROL_CAP = old_cap


def test_stream_bodies_close_on_read_failure():
    class BrokenStream(FakeStream):
        def read(self, n=-1):
            raise IOError("injected read failure")

    body = BrokenStream(b"")
    fake = FakeS3()
    fake.get_object = lambda **kw: {"Body": body}
    try:
        bk.s3_stream_hash(fake, "bucket", "key")
        assert False, "read failure must propagate"
    except IOError:
        assert body.closed
    body = BrokenStream(b"")
    complete, error = bk.fetch_complete(fake, "bucket", "backups", "id")
    assert complete is None and "injected read failure" in error and body.closed


def test_failed_restore_preserves_last_success_marker():
    tmp, _src, tgt, bdir, env = fresh_layout("File")
    original_replace = bk.os.replace
    try:
        setenv(env)
        assert bk.cmd_backup() == 0
        assert bk.cmd_restore() == 0
        marker_path = os.path.join(bdir, "restore-verification.json")
        with open(marker_path, "rb") as stream:
            before = stream.read()
        for label in bk.LABELS:
            bk.clear_dir(os.path.join(tgt, label))
        with open(newest_tar(bdir) + ".sha256", "w") as stream:
            stream.write("0" * 64 + "  " + os.path.basename(newest_tar(bdir)) + "\n")
        assert bk.cmd_restore() == 2
        assert hash_tree(tgt) == {}
        with open(marker_path, "rb") as stream:
            assert stream.read() == before
        # Marker publication failure must not turn this restore into a success
        # or replace the last successful timestamp.
        write_sidecar(newest_tar(bdir))

        def fail_marker(source, destination):
            if destination == marker_path:
                raise PermissionError("injected marker publication failure")
            return original_replace(source, destination)

        bk.os.replace = fail_marker
        assert bk.cmd_restore() == 3
        assert hash_tree(tgt) == {}
        with open(marker_path, "rb") as stream:
            assert stream.read() == before
        assert not [name for name in os.listdir(bdir) if name.endswith(".tmp")]
    finally:
        bk.os.replace = original_replace
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


if __name__ == "__main__":
    test_backup_refuses_running_db()
    test_backup_requires_offline_confirmation()
    test_file_mode_roundtrip_hash_match()
    test_s3_backup_writes_snapshot_and_complete()
    test_s3_backup_copy_failure_rolls_back()
    test_s3_backup_upload_failure_rolls_back_remote()
    test_s3_restore_downloads_and_verifies()
    test_s3_restore_reuses_matching_live_prefix()
    test_s3_restore_uploads_only_missing()
    test_s3_restore_conflict_refuses_without_overwrite()
    test_s3_restore_partial_copy_failure_keeps_matching_live()
    test_s3_client_refuses_plaintext_or_hostless_endpoint()
    test_s3_restore_copy_failure_deletes_own_writes()
    test_s3_restore_refuses_legacy_archive()
    test_s3_restore_ignores_incomplete_prefix()
    test_s3_requires_b2_settings()
    test_overlapping_prefixes_fail_closed()
    test_complete_digest_tamper_refused()
    test_missing_sidecar_refused()
    test_missing_remote_sidecar_refused()
    test_local_tar_from_failed_upload_refused()
    test_corrupt_snapshot_copy_rolls_back()
    test_corrupt_upload_rolls_back_including_complete()
    test_multipart_etag_style_does_not_break()
    test_bitrotted_snapshot_copy_detected()
    test_occupied_remote_namespace_fails_closed()
    test_latest_restore_uses_creation_order_within_one_second()
    test_nested_inventory_never_resolves_as_archive()
    test_failed_download_leaves_no_tar()
    test_duplicate_sst_keys_refused()
    test_wrong_snapshot_bucket_refused()
    test_restore_refuses_nonempty_and_keeps_content()
    test_restore_force_flag_is_dead()
    test_restore_rejects_subset_archive()
    test_restore_rejects_traversal_and_symlink()
    test_restore_rejects_torn_archive()
    test_restore_rejects_sidecar_mismatch()
    test_restore_rejects_identity_change()
    test_prune_never_deletes_just_written()
    test_unreadable_live_sst_is_never_overwritten_or_deleted()
    test_large_sst_inventory_roundtrips_local_and_remote()
    test_inventory_limit_is_shared_by_backup_and_restore()
    test_oversized_complete_is_not_missing_and_closes_body()
    test_stream_bodies_close_on_read_failure()
    test_failed_restore_preserves_last_success_marker()
    print("test_backup: ok (45 tests)")
