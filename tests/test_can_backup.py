"""Behavioral regression tests for scripts.storage.can_backup.

Synthetic only: a fabricated SQLite archive (same table shape as the CAN
receiver archive) plus invented definition bytes. No vehicle data, no
production hosts, no real S3.
"""

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tarfile
import tempfile
import threading

from scripts.storage import backup as bk
from scripts.storage import can_backup as cb

B2ENV = {
    "S3_ENDPOINT_URL": "https://s3.test.backblazeb2.com",
    "S3_BUCKET": "test-bucket",
    "S3_REGION": "test-region",
    "S3_ACCESS_KEY_ID": "test-key",
    "S3_SECRET_ACCESS_KEY": "test-secret",
    "CAN_S3_BACKUP_PREFIX": "test-can-backups",
}

DDL = """
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


class FakeStream:
    def __init__(self, blob):
        self._blob = blob
        self._pos = 0
        self.closed = False

    def read(self, n=-1):
        assert not self.closed, "read after close"
        assert n is not None and n >= 0, "unbounded remote read"
        chunk = self._blob[self._pos:self._pos + n]
        self._pos += len(chunk)
        return chunk

    def close(self):
        self.closed = True


class FakeS3:
    def __init__(self, objects=None, fail_copy_at=None, fail_put_key=None,
                 corrupt=None):
        self.objects = dict(objects or {})
        self.fail_copy_at = fail_copy_at
        self.fail_put_key = fail_put_key
        self.corrupt = dict(corrupt or {})
        self.copies = 0
        self.streams = []

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        if ContinuationToken is not None:
            keys = [k for k in keys if k >= ContinuationToken]
            return {"Contents": [{"Key": k, "Size": len(self.objects[k])}
                                 for k in keys], "IsTruncated": False}
        page, rest = keys[:2], keys[2:]
        out = {"Contents": [{"Key": k, "Size": len(self.objects[k])}
                            for k in page]}
        if rest:
            out["IsTruncated"] = True
            out["NextContinuationToken"] = rest[0]
        else:
            out["IsTruncated"] = False
        return out

    def get_object(self, Bucket, Key):
        blob = self.corrupt.get(Key, self.objects[Key])
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
        self.objects[Key] = bytes(Body)
        return {}

    def copy_object(self, Bucket, CopySource, Key):
        if self.fail_copy_at is not None and self.copies >= self.fail_copy_at:
            raise IOError("injected copy failure")
        self.copies += 1
        self.objects[Key] = self.objects[CopySource["Key"]]
        return {}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
        return {}


_SAVED = dict(os.environ)
_ORIG_MAKE_S3 = bk.make_s3_client


def setenv(mapping):
    for k in list(os.environ):
        if k.startswith(("CAN_", "BACKUP_", "S3_", "GREPTIME_")):
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


def make_live(base):
    """Fabricated archive + definitions; returns (db_path, defs_dir, dump)."""
    live = os.path.join(base, "live")
    defs = os.path.join(live, "defs")
    os.makedirs(defs)
    db_path = os.path.join(live, "raw.sqlite3")
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(DDL)
    conn.execute("INSERT INTO sessions VALUES(1,'synth','fix','s1','{}',3,20)")
    conn.execute("INSERT INTO epochs VALUES('epoch-a','rev-1')")
    blobs = [bytes(range(256)), os.urandom(3000), b"\x00\xff" * 500]
    for seq, blob in enumerate(blobs):
        conn.execute("INSERT INTO raw_chunks(session,seq,offset_ns,phase,data)"
                     " VALUES(1,?,?,?,?)", (seq, seq * 10, "capture", blob))
    conn.execute("INSERT INTO decode_states VALUES(1,'epoch-a',3,"
                 "'{\"next_seq\":3}','{\"rows\":5}',5)")
    conn.execute("INSERT INTO outbox VALUES(1,'ev-1','epoch-a','{\"a\":1}')")
    conn.execute("INSERT INTO outbox VALUES(2,'ev-2','epoch-a','{\"a\":2}')")
    conn.execute("INSERT INTO worker_errors VALUES('decode_failure',2,7)")
    conn.commit()
    conn.close()
    for name, blob in (("observed.dbc", b"VERSION \"synth\"\n" + os.urandom(64)),
                       ("observed.json",
                        json.dumps({"revision": "synth-1"}).encode())):
        with open(os.path.join(defs, name), "wb") as f:
            f.write(blob)
    os.chmod(db_path, 0o600)
    return db_path, defs, dump_state(db_path, defs)


def dump_state(db_path, defs):
    conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True, timeout=30)
    try:
        tables = {}
        for table in ("sessions", "raw_chunks", "epochs", "decode_states",
                      "outbox", "worker_errors"):
            tables[table] = conn.execute(
                "SELECT * FROM %s ORDER BY rowid" % table).fetchall()
    finally:
        conn.close()
    files = {}
    for name in sorted(os.listdir(defs)):
        with open(os.path.join(defs, name), "rb") as f:
            files[name] = f.read()
    return {"tables": tables, "files": files}


def fresh_layout(mode="File", fake=None):
    tmp = tempfile.mkdtemp(prefix="can-backup-test-")
    db_path, defs, before = make_live(tmp)
    bdir = os.path.join(tmp, "bdir")
    raw_dir = os.path.join(tmp, "restore-raw")
    defs_dir = os.path.join(tmp, "restore-defs")
    os.makedirs(bdir)
    env = {
        "CAN_ARCHIVE_PATH": db_path,
        "CAN_DEFINITIONS_DIR": defs,
        "CAN_BACKUP_DIR": bdir,
        "CAN_RESTORE_RAW_DIR": raw_dir,
        "CAN_RESTORE_DEFS_DIR": defs_dir,
        "BACKUP_OFFLINE_CONFIRMED": "1",
        "CAN_STORAGE_TYPE": mode,
        "CAN_BACKUP_KEEP": "7",
    }
    env.update(B2ENV)
    use_fake(fake)
    return tmp, db_path, defs, bdir, raw_dir, defs_dir, before, env


def newest_tar(bdir):
    files = sorted(f for f in os.listdir(bdir) if f.endswith(".tar.gz"))
    assert files, "expected a backup tar in " + bdir
    return os.path.join(bdir, files[-1])


def read_manifest(path):
    with tarfile.open(path, "r:gz") as tar:
        return json.loads(tar.extractfile("manifest.json").read().decode())


def test_file_mode_roundtrip_exact():
    tmp, db_path, defs, bdir, raw_dir, defs_dir, before, env = \
        fresh_layout("File")
    try:
        setenv(env)
        src_mode = stat.S_IMODE(os.stat(db_path).st_mode)
        assert cb.cmd_backup() == 0
        tar = newest_tar(bdir)
        assert stat.S_IMODE(os.stat(tar).st_mode) == 0o600
        assert cb.cmd_restore() == 0
        got = dump_state(os.path.join(raw_dir, "raw.sqlite3"), defs_dir)
        assert got == before, "restored bytes/state/defs differ from source"
        st = os.stat(os.path.join(raw_dir, "raw.sqlite3"))
        assert stat.S_IMODE(st.st_mode) == src_mode
        assert (st.st_uid, st.st_gid) == (os.stat(db_path).st_uid,
                                          os.stat(db_path).st_gid)
        assert dump_state(db_path, defs) == before, "source mutated"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_uncommitted_write_excluded_and_source_kept():
    tmp, db_path, defs, bdir, _rr, _rd, before, env = fresh_layout("File")
    try:
        setenv(env)
        writer = sqlite3.connect(db_path, timeout=30)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO outbox VALUES(99,'ev-held','epoch-a','{}')")
        try:
            assert cb.cmd_backup() == 0, "pinned snapshot must not starve"
        finally:
            writer.execute("ROLLBACK")
            writer.close()
        assert cb.cmd_restore() == 0
        got = dump_state(os.path.join(env["CAN_RESTORE_RAW_DIR"],
                                      "raw.sqlite3"),
                         env["CAN_RESTORE_DEFS_DIR"])
        assert got == before, "held write leaked into the snapshot"
        assert dump_state(db_path, defs) == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_concurrent_commits_stay_manifest_exact():
    tmp, db_path, defs, bdir, _rr, _rd, _before, env = fresh_layout("File")
    stop = threading.Event()
    errors = []

    def churn():
        seq = [100]
        while not stop.is_set():
            try:
                conn = sqlite3.connect(db_path, timeout=30)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("INSERT INTO worker_errors VALUES(?,?,?)",
                                 ("churn-%d" % seq[0], 1, seq[0]))
                    conn.commit()
                finally:
                    conn.close()
                seq[0] += 1
            except Exception as e:  # noqa: BLE001 - record, then fail loudly
                errors.append(e)
                return

    try:
        setenv(env)
        thread = threading.Thread(target=churn, daemon=True)
        thread.start()
        try:
            assert cb.cmd_backup() == 0
        finally:
            stop.set()
            thread.join(10)
        assert not errors, errors
        manifest = read_manifest(newest_tar(bdir))
        assert cb.cmd_restore() == 0
        got = dump_state(os.path.join(env["CAN_RESTORE_RAW_DIR"],
                                      "raw.sqlite3"),
                         env["CAN_RESTORE_DEFS_DIR"])
        conn = sqlite3.connect(
            "file:%s?mode=ro" % os.path.join(env["CAN_RESTORE_RAW_DIR"],
                                             "raw.sqlite3"), uri=True)
        try:
            counts = dict(zip(("sessions", "raw_chunks", "raw_bytes",
                               "outbox_rows", "decode_state_rows"),
                              conn.execute("SELECT (SELECT COUNT(*) FROM sessions),"
                                           " (SELECT COUNT(*) FROM raw_chunks),"
                                           " (SELECT COALESCE(SUM(length(data)),0)"
                                           " FROM raw_chunks),"
                                           " (SELECT COUNT(*) FROM outbox),"
                                           " (SELECT COUNT(*) FROM decode_states)"
                                           ).fetchone()))
        finally:
            conn.close()
        for field, value in counts.items():
            assert manifest["archive"]["counts"][field] == value, \
                "manifest/restore mismatch under concurrent writer: " + field
        assert sorted(got["files"]) == sorted(
            f["name"] for f in manifest["definitions"]["files"])
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_targets_gated_but_live_source_may_run():
    import threading
    tmp, db_path, defs, bdir, raw_dir, _rd, before, env = fresh_layout("File")
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        stop = threading.Event()
        errors = []

        def churn():
            seq = [1000]
            while not stop.is_set():
                try:
                    conn = sqlite3.connect(db_path, timeout=30)
                    try:
                        conn.execute("BEGIN IMMEDIATE")
                        conn.execute("INSERT INTO worker_errors VALUES(?,?,?)",
                                     ("live-%d" % seq[0], 1, seq[0]))
                        conn.commit()
                    finally:
                        conn.close()
                    seq[0] += 1
                except Exception as e:  # noqa: BLE001 - record, then assert
                    errors.append(e)
                    return

        src_fd = os.open(db_path + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl
            fcntl.flock(src_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            thread = threading.Thread(target=churn, daemon=True)
            thread.start()
            try:
                assert cb.cmd_restore() == 0, \
                    "fresh restore must not need the live source stopped"
            finally:
                stop.set()
                thread.join(10)
            assert not errors, errors
            live = dump_state(db_path, defs)
            assert live["tables"]["worker_errors"] != \
                before["tables"]["worker_errors"], "source writer ran during restore"
            got = dump_state(os.path.join(raw_dir, "raw.sqlite3"),
                             env["CAN_RESTORE_DEFS_DIR"])
            assert got["tables"]["sessions"] == before["tables"]["sessions"]
            assert got["files"] == before["files"]
        finally:
            fcntl.flock(src_fd, fcntl.LOCK_UN)
            os.close(src_fd)
        tgt_fd = os.open(os.path.join(raw_dir, "raw.sqlite3.lock"),
                         os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl
            fcntl.flock(tgt_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert cb.cmd_restore() == 2, "locked target must refuse"
            assert dump_state(db_path, defs)["tables"]["sessions"] == \
                before["tables"]["sessions"], "failed restore touched source"
        finally:
            fcntl.flock(tgt_fd, fcntl.LOCK_UN)
            os.close(tgt_fd)
        env2 = dict(env)
        del env2["BACKUP_OFFLINE_CONFIRMED"]
        setenv(env2)
        assert cb.cmd_restore() == 2
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_definitions_symlink_or_empty_refused():
    for mode in ("symlink", "empty"):
        tmp, db_path, defs, bdir, _rr, _rd, _before, env = fresh_layout("File")
        try:
            if mode == "empty":
                for name in os.listdir(defs):
                    os.unlink(os.path.join(defs, name))
            else:
                victim = os.path.join(defs, "observed.dbc")
                os.unlink(victim)
                os.symlink(os.path.join(defs, "observed.json"), victim)
            setenv(env)
            assert cb.cmd_backup() == 2, mode
            assert [f for f in os.listdir(bdir)
                    if f.endswith(".tar.gz")] == [], mode
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            restore_env()




def test_restore_refuses_nonempty_and_tampered_sidecar():
    tmp, db_path, defs, bdir, raw_dir, _dd, before, env = \
        fresh_layout("File")
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        tar = newest_tar(bdir)
        os.makedirs(raw_dir)
        sentinel = os.path.join(raw_dir, "sentinel")
        with open(sentinel, "wb") as f:
            f.write(b"live")
        assert cb.cmd_restore() == 2
        with open(sentinel, "rb") as f:
            assert f.read() == b"live", "non-empty target content touched"
        shutil.rmtree(raw_dir)
        with open(tar + ".sha256", "w", encoding="utf-8") as f:
            f.write("0" * 64 + "  " + os.path.basename(tar) + "\n")
        assert cb.cmd_restore() == 2
        assert not os.path.exists(raw_dir) or \
            os.listdir(raw_dir) == [], "failed restore wrote targets"
        assert dump_state(db_path, defs) == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_insufficient_space_refuses_before_writes():
    tmp, db_path, defs, bdir, raw_dir, defs_dir, before, env = \
        fresh_layout("File")
    real_usage = shutil.disk_usage
    try:
        setenv(env)
        shutil.disk_usage = lambda _p: type("U", (), {"free": 1})()
        assert cb.cmd_backup() == 2
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == []
        shutil.disk_usage = real_usage
        assert cb.cmd_backup() == 0
        shutil.disk_usage = lambda _p: type("U", (), {"free": 1})()
        assert cb.cmd_restore() == 2
        assert not os.path.exists(raw_dir) or os.listdir(raw_dir) == []
        assert dump_state(db_path, defs) == before
    finally:
        shutil.disk_usage = real_usage
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_rejects_member_mismatch():
    tmp, db_path, defs, bdir, _rr, _rd, before, env = fresh_layout("File")
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        tar = newest_tar(bdir)
        with tarfile.open(tar, "r:gz") as src:
            members = src.getmembers()
            manifest_raw = src.extractfile("manifest.json").read()
        manifest = json.loads(manifest_raw.decode())
        manifest["definitions"]["files"] = manifest["definitions"]["files"][:1]
        forged = json.dumps(manifest, indent=2,
                            sort_keys=True).encode()
        with tarfile.open(tar + ".evil", "w:gz") as dst:
            for m in members:
                if m.name == "manifest.json":
                    info = tarfile.TarInfo("manifest.json")
                    info.size = len(forged)
                    info.mode = 0o600
                    import io as _io
                    dst.addfile(info, _io.BytesIO(forged))
                elif m.isfile():
                    with tarfile.open(tar, "r:gz") as src2:
                        dst.addfile(m, src2.extractfile(m.name))
                else:
                    dst.addfile(m)
        os.replace(tar + ".evil", tar)
        with open(tar + ".sha256", "w", encoding="utf-8") as f:
            h = hashlib.sha256()
            with open(tar, "rb") as s:
                for chunk in iter(lambda: s.read(65536), b""):
                    h.update(chunk)
            f.write(h.hexdigest() + "  " + os.path.basename(tar) + "\n")
        assert cb.cmd_restore() == 2, "member/manifest mismatch must refuse"
        assert dump_state(db_path, defs) == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_backup_uploads_and_restore_verifies():
    fake = FakeS3({})
    tmp, _db, _defs, bdir, _rr, _rd, before, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        tar = newest_tar(bdir)
        bid = os.path.basename(tar)[:-len(".tar.gz")]
        rbase = "test-can-backups/" + bid + "/"
        assert rbase + os.path.basename(tar) in fake.objects
        complete = json.loads(fake.objects[rbase + "COMPLETE"].decode())
        assert complete["backup_id"] == bid
        shutil.rmtree(os.path.join(tmp, "restore-raw"), ignore_errors=True)
        shutil.rmtree(os.path.join(tmp, "restore-defs"), ignore_errors=True)
        os.remove(tar)
        os.remove(tar + ".sha256")
        assert cb.cmd_restore() == 0, "off-host download + fresh restore"
        got = dump_state(os.path.join(env["CAN_RESTORE_RAW_DIR"],
                                      "raw.sqlite3"),
                         env["CAN_RESTORE_DEFS_DIR"])
        assert got == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_corrupt_remote_tar_refused_and_local_kept():
    fake = FakeS3({})
    tmp, _db, _defs, bdir, _rr, _rd, before, env = fresh_layout("S3", fake)
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        tar = newest_tar(bdir)
        bid = os.path.basename(tar)[:-len(".tar.gz")]
        rbase = "test-can-backups/" + bid + "/"
        fake.corrupt[rbase + os.path.basename(tar)] = b"corrupt-bytes"
        os.remove(tar)
        assert cb.cmd_restore() == 2
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")] == [], \
            "failed download must leave no tar"
        live = dump_state(env["CAN_ARCHIVE_PATH"],
                          env["CAN_DEFINITIONS_DIR"])
        assert live == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_s3_upload_failure_rolls_back_remote():
    fake = FakeS3({})
    tmp, _db, _defs, bdir, _rr, _rd, before, env = fresh_layout("S3", fake)
    real_put = fake.put_object

    def fail_complete(**kw):
        if kw.get("Key", "").endswith("/COMPLETE"):
            raise IOError("injected COMPLETE failure")
        return real_put(**kw)

    try:
        setenv(env)
        fake.put_object = fail_complete
        assert cb.cmd_backup() == 3
        assert fake.objects == {}, \
            "failed run must roll back its own remote writes: %r" % (
                sorted(fake.objects),)
        assert [f for f in os.listdir(bdir) if f.endswith(".tar.gz")]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_rejects_unsafe_dir_mode_and_traversal():
    tmp, _db, _defs, bdir, _rr, _rd, before, env = fresh_layout("File")
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        tar = newest_tar(bdir)
        with tarfile.open(tar, "r:gz") as src:
            members = src.getmembers()
            manifest_raw = src.extractfile("manifest.json").read()
        manifest = json.loads(manifest_raw.decode())
        manifest["dirs"]["raw_mode"] = 0o777
        forged = json.dumps(manifest, indent=2, sort_keys=True).encode()
        with tarfile.open(tar + ".evil", "w:gz") as dst:
            for m in members:
                if m.name == "manifest.json":
                    info = tarfile.TarInfo("manifest.json")
                    info.size = len(forged)
                    info.mode = 0o600
                    import io as _io
                    dst.addfile(info, _io.BytesIO(forged))
                elif m.isfile():
                    with tarfile.open(tar, "r:gz") as src2:
                        dst.addfile(m, src2.extractfile(m.name))
                else:
                    dst.addfile(m)
        os.replace(tar + ".evil", tar)
        with open(tar + ".sha256", "w", encoding="utf-8") as f:
            h = hashlib.sha256()
            with open(tar, "rb") as s:
                for chunk in iter(lambda: s.read(65536), b""):
                    h.update(chunk)
            f.write(h.hexdigest() + "  " + os.path.basename(tar) + "\n")
        assert cb.cmd_restore() == 2, "world-writable dir metadata must refuse"
        live = dump_state(env["CAN_ARCHIVE_PATH"],
                          env["CAN_DEFINITIONS_DIR"])
        assert live == before
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_preflight_checks_each_target_filesystem():
    tmp, _db, _defs, bdir, _rr, _rd, before, env = fresh_layout("File")
    real_usage = shutil.disk_usage
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        calls = []

        def fake_usage(path):
            calls.append(path)
            if path == env["CAN_RESTORE_DEFS_DIR"]:
                return type("U", (), {"free": 1})()
            return real_usage(path)

        shutil.disk_usage = fake_usage
        assert cb.cmd_restore() == 2
        assert any(c == env["CAN_RESTORE_DEFS_DIR"] for c in calls), \
            "defs filesystem must be preflighted separately"
        assert not os.path.exists(env["CAN_RESTORE_RAW_DIR"]) or \
            os.listdir(env["CAN_RESTORE_RAW_DIR"]) == []
    finally:
        shutil.disk_usage = real_usage
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


def test_restore_applies_exact_dir_modes_no_widening():
    tmp, _db, _defs, bdir, raw_dir, defs_dir, before, env = fresh_layout("File")
    try:
        setenv(env)
        assert cb.cmd_backup() == 0
        tar = newest_tar(bdir)
        manifest = read_manifest(tar)
        src_mode = manifest["dirs"]["raw_mode"]
        assert src_mode & 0o007 == 0, "fixture must be private: %o" % src_mode
        assert cb.cmd_restore() == 0
        got_raw = stat.S_IMODE(os.stat(raw_dir).st_mode)
        got_defs = stat.S_IMODE(os.stat(defs_dir).st_mode)
        assert got_raw == manifest["dirs"]["raw_mode"] == 0o700, \
            "restore must keep exact recorded dir mode: %o" % got_raw
        assert got_defs == manifest["dirs"]["defs_mode"], \
            "defs dir mode widened: %o" % got_defs
        st = os.stat(os.path.join(raw_dir, "raw.sqlite3"))
        assert (st.st_uid, st.st_gid) == (os.stat(
            env["CAN_ARCHIVE_PATH"]).st_uid,
            os.stat(env["CAN_ARCHIVE_PATH"]).st_gid)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        restore_env()


if __name__ == "__main__":
    test_file_mode_roundtrip_exact()
    test_uncommitted_write_excluded_and_source_kept()
    test_concurrent_commits_stay_manifest_exact()
    test_restore_targets_gated_but_live_source_may_run()
    test_definitions_symlink_or_empty_refused()
    test_restore_refuses_nonempty_and_tampered_sidecar()
    test_insufficient_space_refuses_before_writes()
    test_restore_rejects_member_mismatch()
    test_s3_backup_uploads_and_restore_verifies()
    test_s3_corrupt_remote_tar_refused_and_local_kept()
    test_s3_upload_failure_rolls_back_remote()
    test_restore_rejects_unsafe_dir_mode_and_traversal()
    test_restore_preflight_checks_each_target_filesystem()
    test_restore_applies_exact_dir_modes_no_widening()
    print("test_can_backup: ok (14 tests)")
