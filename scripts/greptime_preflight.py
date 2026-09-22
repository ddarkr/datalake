#!/usr/bin/env python3
"""GreptimeDB preflight: validate env, write standalone TOML + auth file.

Reads configuration from the environment only (errors name inputs, never
values) and writes ``greptimedb.toml``, ``auth/users`` and
``storage-identity`` under ``OUT_DIR`` (the ``greptime-etc`` volume, mounted
at /etc/greptime in greptimedb). Idempotent: outputs are rewritten on every
run; the data volume is never touched.

Key layout follows the exact pinned image's standalone config
(greptime/greptimedb:v1.2.1 config/standalone.example.toml): cache/flush
keys live under [region_engine.mito], not [storage]. A changed storage
backend identity fails closed — restore into a fresh volume via the
backup/restore path instead of mixing metadata across backends.
"""

import os
import re
import sys
import urllib.parse

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
# Greptime file credential lines split on '=' (user=password), so these
# cannot appear in the password.
FORBIDDEN_PW = ("\n", "\r", "\x00", "=")
SIZE_RE = re.compile(r"^[0-9]+(B|KB|KiB|MB|MiB|GB|GiB|TB|TiB|PB|PiB)$")
DURATION_RE = re.compile(r"^[0-9]+(ns|us|ms|s|m|h|d)$")


def fail(msg):
    print(f"greptime-preflight: error: {msg}", file=sys.stderr)
    raise SystemExit(1)


def toml_str(s):
    parts = ['"']
    for ch in s:
        if ch == '"':
            parts.append('\\"')
        elif ch == "\\":
            parts.append("\\\\")
        elif ch == "\n":
            parts.append("\\n")
        elif ch == "\t":
            parts.append("\\t")
        elif ch == "\r":
            parts.append("\\r")
        elif ord(ch) < 0x20:
            parts.append("\\u%04X" % ord(ch))
        else:
            parts.append(ch)
    parts.append('"')
    return "".join(parts)


def derive_b2_region(endpoint):
    m = re.search(r"s3\.([A-Za-z0-9-]+)\.backblaze", endpoint)
    return m.group(1) if m else ""


def atomic_write(path, data, mode=0o600):
    """Write data atomically (temp + fsync + replace + dir fsync)."""
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    os.chmod(tmp, mode)
    os.replace(tmp, path)
    dfd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def main():
    os.umask(0o077)  # TOML embeds the S3 secret: never create it world-readable
    db = os.environ.get("GREPTIME_DB", "datalake")
    user = os.environ.get("GREPTIME_USER", "datalake")
    password = os.environ.get("GREPTIME_PASSWORD", "")
    gf_password = os.environ.get("GF_ADMIN_PASSWORD", "")
    otlp_user = os.environ.get("OTLP_USER", "")
    otlp_password = os.environ.get("OTLP_PASSWORD", "")
    storage = os.environ.get("GREPTIME_STORAGE_TYPE", "S3").strip()
    page_cache = os.environ.get("GREPTIME_PAGE_CACHE_SIZE", "512MB")
    write_buf = os.environ.get("GREPTIME_WRITE_BUFFER_SIZE", "128MB")
    flush_interval = os.environ.get("GREPTIME_AUTO_FLUSH_INTERVAL", "10m")
    out_dir = os.environ.get("OUT_DIR", "/out")

    for name, value in (("GREPTIME_DB", db), ("GREPTIME_USER", user)):
        if not IDENT.match(value):
            fail(f"{name} must match [A-Za-z_][A-Za-z0-9_]*")
    if not password:
        fail("missing required env: GREPTIME_PASSWORD")
    for ch in FORBIDDEN_PW:
        if ch in password:
            fail("GREPTIME_PASSWORD must not contain newline or '=' "
                 "(Greptime credential-line format limitation)")
    if not gf_password:
        fail("missing required env: GF_ADMIN_PASSWORD "
             "(empty/default admin boot is forbidden)")
    missing_auth = [n for n, v in (("OTLP_USER", otlp_user),
                                   ("OTLP_PASSWORD", otlp_password)) if not v]
    if missing_auth:
        fail("missing required env: " + ", ".join(missing_auth) +
             " (anonymous server ingest is forbidden)")
    if storage not in ("S3", "File"):
        fail("GREPTIME_STORAGE_TYPE must be S3 or File")
    if not SIZE_RE.match(page_cache):
        fail("GREPTIME_PAGE_CACHE_SIZE must be a size like 512MB")
    if not SIZE_RE.match(write_buf):
        fail("GREPTIME_WRITE_BUFFER_SIZE must be a size like 128MB")
    if not DURATION_RE.match(flush_interval):
        fail("GREPTIME_AUTO_FLUSH_INTERVAL must be a duration like 10m")

    s3 = {}
    if storage == "S3":
        endpoint = os.environ.get("S3_ENDPOINT_URL", "")
        bucket = os.environ.get("S3_BUCKET", "")
        key_id = os.environ.get("S3_ACCESS_KEY_ID", "")
        secret = os.environ.get("S3_SECRET_ACCESS_KEY", "")
        region = os.environ.get("S3_REGION", "") or derive_b2_region(endpoint)
        root = os.environ.get("S3_ROOT", "greptime")
        missing = [n for n, v in (
            ("S3_ENDPOINT_URL", endpoint), ("S3_BUCKET", bucket),
            ("S3_ACCESS_KEY_ID", key_id), ("S3_SECRET_ACCESS_KEY", secret),
            ("S3_REGION", region)) if not v]
        if missing:
            fail("missing required env for STORAGE_TYPE=S3: " + ", ".join(missing) +
                 " (S3_REGION auto-derives from s3.<region>.backblaze... endpoints)")
        try:
            parsed = urllib.parse.urlsplit(endpoint)
            valid_endpoint = (parsed.scheme == "https" and bool(parsed.hostname)
                              and parsed.username is None and parsed.password is None)
        except ValueError:
            valid_endpoint = False
        if not valid_endpoint:
            fail("S3_ENDPOINT_URL must be an https:// URL with a host and no embedded credentials")
        if not re.match(r"^[A-Za-z0-9][A-Za-z0-9.-]{1,61}[A-Za-z0-9]$", bucket):
            fail("S3_BUCKET looks invalid")
        s3 = {"bucket": bucket, "root": root, "access_key_id": key_id,
              "secret_access_key": secret, "endpoint": endpoint, "region": region}

    auth_path = "/etc/greptime/auth/users"  # runtime path inside greptimedb
    lines = [
        'default_timezone = "UTC"',
        f"user_provider = {toml_str('static_user_provider:file:' + auth_path)}",
        "",
        "init_regions_in_background = false",
        "init_regions_parallelism = 16",
        "",
        "[http]",
        'addr = "0.0.0.0:4000"',
        'timeout = "0s"',
        'body_limit = "64MB"',
        "",
        "[grpc]",
        'bind_addr = "0.0.0.0:4001"',
        "runtime_size = 8",
        "",
        "[mysql]",
        "enable = true",
        'addr = "0.0.0.0:4002"',
        "runtime_size = 2",
        "",
        # Unused protocol servers stay off (defaults are on upstream).
        "[postgres]",
        "enable = false",
        "",
        "[opentsdb]",
        "enable = false",
        "",
        "[influxdb]",
        "enable = true",
        "",
        "[otlp]",
        "enable = true",
        "",
        "[prom_store]",
        "enable = true",
        "with_metric_engine = true",
        "",
        "[wal]",
        'provider = "raft_engine"',
        'dir = "/data/wal"',
        'file_size = "128MB"',
        'purge_threshold = "1GB"',
        'purge_interval = "1m"',
        # Ack durability: fsync every WAL write before acknowledging.
        "sync_write = true",
        "",
        "[storage]",
        'data_home = "/data"',
        f"type = {toml_str(storage)}",
    ]
    if storage == "S3":
        lines += [
            f"bucket = {toml_str(s3['bucket'])}",
            f"root = {toml_str(s3['root'])}",
            f"access_key_id = {toml_str(s3['access_key_id'])}",
            f"secret_access_key = {toml_str(s3['secret_access_key'])}",
            f"endpoint = {toml_str(s3['endpoint'])}",
            f"region = {toml_str(s3['region'])}",
        ]
    lines += [
        "",
        # Engine options: one [[region_engine]] + [region_engine.mito]
        # (v1.2.1 standalone config; these keys are NOT [storage] keys).
        "[[region_engine]]",
        "",
        "[region_engine.mito]",
        f"page_cache_size = {toml_str(page_cache)}",
        f"global_write_buffer_size = {toml_str(write_buf)}",
        f"auto_flush_interval = {toml_str(flush_interval)}",
        f"enable_write_cache = {'true' if storage == 'S3' else 'false'}",
        "",
        "[logging]",
        'dir = "/data/logs"',
        'level = "info"',
        "",
    ]

    auth_dir = os.path.join(out_dir, "auth")
    os.makedirs(auth_dir, mode=0o700, exist_ok=True)
    os.chmod(auth_dir, 0o700)

    # Fail closed on backend switches: existing data/metadata belongs to the
    # recorded backend; changing it needs the backup/restore path, not a
    # silent re-point (restore is the only dedicated path).
    identity_items = [f"storage={storage}"]
    if storage == "S3":
        identity_items += [f"bucket={s3['bucket']}", f"root={s3['root']}",
                           f"endpoint={s3['endpoint']}", f"region={s3['region']}"]
    identity = "\n".join(identity_items) + "\n"
    identity_path = os.path.join(out_dir, "storage-identity")
    if os.path.exists(identity_path):
        with open(identity_path, encoding="utf-8") as f:
            recorded = f.read()
        if recorded != identity:
            fail("storage backend identity changed (storage/bucket/root/"
                 "endpoint/region differ from the recorded identity); refusing "
                 "to mix metadata across backends — restore into a fresh "
                 "volume via the backup/restore path instead")

    toml_path = os.path.join(out_dir, "greptimedb.toml")
    atomic_write(toml_path, "\n".join(lines))
    users_path = os.path.join(out_dir, "auth", "users")
    atomic_write(users_path, f"{user}={password}\n")
    atomic_write(identity_path, identity)
    print(f"greptime-preflight: wrote {toml_path} "
          f"(db={db} user={user} storage={storage})")


if __name__ == "__main__":
    main()
