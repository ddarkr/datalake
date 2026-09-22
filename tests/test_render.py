"""Core regression: renderer isolation + preflight fail-fast (parent runs).

Behavioral only: byte roundtrip of inlined scripts, fail-closed
duplicates, nonzero exit on missing secrets, exact-config keys from
the pinned image. No source-text asserts.
"""

import os
import stat
import subprocess
import sys
import tomllib
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import render  # noqa: E402


def base_env(tmp_path):
    return dict(os.environ, GREPTIME_PASSWORD="t3st-pw without equals",
                GF_ADMIN_PASSWORD="grafana-admin-pw",
                OTLP_USER="otlp", OTLP_PASSWORD="otlp-pw",
                GREPTIME_STORAGE_TYPE="File", OUT_DIR=str(tmp_path))


def test_xsource_roundtrip():
    # Compose folds $$ back to $ on deploy; unescaped content must equal
    # the repo script byte-for-byte or the deployed preflight is corrupt.
    frag = render.load_fragment(str(ROOT / "compose" / "core.yaml"))
    content = frag["configs"]["greptime_preflight"]["content"]
    assert content.replace("$$", "$") == (
        ROOT / "scripts" / "greptime_preflight.py"
    ).read_text(encoding="utf-8")


def test_duplicate_service_fails(tmp_path):
    a = tmp_path / "a.yaml"
    b = tmp_path / "b.yaml"
    a.write_text("services:\n  dup:\n    image: img:1\n", encoding="utf-8")
    b.write_text("services:\n  dup:\n    image: img:2\n", encoding="utf-8")
    try:
        render.merge_docs(
            [render.load_fragment(str(a)), render.load_fragment(str(b))])
    except render.RenderError:
        return
    raise AssertionError("duplicate service merged silently")


def test_xsource_escape_fails(tmp_path):
    frag = tmp_path / "evil.yaml"
    frag.write_text("configs:\n  evil:\n    x-source: ../outside.py\n",
                    encoding="utf-8")
    try:
        render.load_fragment(str(frag))
    except render.RenderError:
        return
    raise AssertionError("repo-root escape inlined silently")


def test_final_forbids_remote_mounts():
    doc = {"services": {"s": {"image": "img:1",
                              "volumes": ["./host:/data"]}},
           "configs": {}, "volumes": {}}
    try:
        render.check_final(doc)
    except render.RenderError:
        return
    raise AssertionError("host bind mount passed silently")




def test_preflight_fails_fast_without_password(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "GREPTIME_PASSWORD"}
    env.update(GREPTIME_STORAGE_TYPE="File", OUT_DIR=str(tmp_path))
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert "GREPTIME_PASSWORD" in r.stderr


def test_preflight_fails_fast_without_admin(tmp_path):
    env = dict(base_env(tmp_path))
    del env["GF_ADMIN_PASSWORD"]
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert "GF_ADMIN_PASSWORD" in r.stderr


def test_preflight_fails_fast_without_otlp(tmp_path):
    env = dict(base_env(tmp_path))
    del env["OTLP_USER"]
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert "OTLP_USER" in r.stderr


def test_preflight_rejects_bad_size(tmp_path):
    env = dict(base_env(tmp_path), GREPTIME_PAGE_CACHE_SIZE="huge")
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert "GREPTIME_PAGE_CACHE_SIZE" in r.stderr


def test_preflight_rejects_hostless_endpoint_without_echo(tmp_path):
    env = dict(base_env(tmp_path), GREPTIME_STORAGE_TYPE="S3",
               S3_ENDPOINT_URL="https://?private=endpoint-canary",
               S3_REGION="test-region", S3_BUCKET="test-bucket",
               S3_ACCESS_KEY_ID="fixture-key", S3_SECRET_ACCESS_KEY="fixture-secret")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=env)
    assert result.returncode != 0
    assert "S3_ENDPOINT_URL" in result.stderr
    assert "endpoint-canary" not in result.stdout + result.stderr
    assert not list(tmp_path.iterdir())


def test_preflight_file_mode_writes_exact_config(tmp_path):
    env = dict(base_env(tmp_path), GREPTIME_PAGE_CACHE_SIZE="256MB",
               GREPTIME_WRITE_BUFFER_SIZE="64MB",
               GREPTIME_AUTO_FLUSH_INTERVAL="20m")
    pw = env["GREPTIME_PASSWORD"]
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    assert pw not in r.stdout + r.stderr  # secrets never echoed
    assert "otlp-pw" not in r.stdout + r.stderr
    with open(tmp_path / "greptimedb.toml", "rb") as f:
        doc = tomllib.load(f)
    assert doc["storage"]["type"] == "File"
    # Exact v1.2.1 standalone layout: engine keys under region_engine.mito.
    assert "page_cache_size" not in doc["storage"]
    assert "global_write_buffer_size" not in doc["storage"]
    mito = doc["region_engine"][0]["mito"] \
        if isinstance(doc.get("region_engine"), list) \
        else doc["region_engine"]["mito"]
    assert mito["page_cache_size"] == "256MB"
    assert mito["global_write_buffer_size"] == "64MB"
    assert mito["auto_flush_interval"] == "20m"
    assert doc["wal"]["sync_write"] is True
    assert doc["postgres"]["enable"] is False
    assert doc["opentsdb"]["enable"] is False
    assert doc["user_provider"].startswith("static_user_provider:file:")
    users = (tmp_path / "auth" / "users").read_text(encoding="utf-8")
    assert users == f"datalake={pw}\n"
    mode = stat.S_IMODE(os.stat(tmp_path / "auth" / "users").st_mode)
    assert mode == 0o600
    toml_mode = stat.S_IMODE(os.stat(tmp_path / "greptimedb.toml").st_mode)
    assert toml_mode == 0o600
    assert (tmp_path / "storage-identity").exists()


def test_preflight_identity_change_fails_closed(tmp_path):
    env = dict(base_env(tmp_path), GREPTIME_STORAGE_TYPE="File")
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    s3env = dict(env, GREPTIME_STORAGE_TYPE="S3",
                 S3_ENDPOINT_URL="https://s3.us-west-004.backblazeb2.com",
                 S3_BUCKET="bkt", S3_ACCESS_KEY_ID="k", S3_SECRET_ACCESS_KEY="s")
    r2 = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "greptime_preflight.py")],
        capture_output=True, text=True, env=s3env)
    assert r2.returncode != 0
    assert "storage backend identity changed" in r2.stderr


if __name__ == "__main__":
    test_xsource_roundtrip()
    test_final_forbids_remote_mounts()
    for check in (
        test_duplicate_service_fails, test_xsource_escape_fails,
        test_preflight_fails_fast_without_password,
        test_preflight_fails_fast_without_admin,
        test_preflight_fails_fast_without_otlp,
        test_preflight_rejects_bad_size,
        test_preflight_rejects_hostless_endpoint_without_echo,
        test_preflight_file_mode_writes_exact_config,
        test_preflight_identity_change_fails_closed,
    ):
        with tempfile.TemporaryDirectory() as directory:
            check(Path(directory))
    print("test_render: ok (11 checks)")
