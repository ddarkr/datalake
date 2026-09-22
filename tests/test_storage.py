"""Storage usage inventory regressions: real bucket totals, no fake zeros."""
import importlib.util
import os
import sys
import unittest
import tempfile
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(os.path.dirname(HERE), "scripts")


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(SCRIPTS, name + ".py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sm = load("storage_metrics")
upl = load("raw_upload")

ENV_KEYS = ("GREPTIME_STORAGE_TYPE", "S3_ENDPOINT_URL", "S3_BUCKET",
            "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY")


class FakeList:
    """Paginated ListObjectsV2; records tokens to prove pagination."""

    def __init__(self, pages=None, fail=None):
        self.pages = pages or []
        self.fail = fail
        self.tokens = []
        self.calls = 0

    def list_objects_v2(self, Bucket, ContinuationToken=None):
        if self.fail is not None:
            raise self.fail
        self.tokens.append(ContinuationToken)
        page = self.pages[self.calls]
        self.calls += 1
        return page


class InventoryTest(unittest.TestCase):
    def setUp(self):
        self._old_env = {k: os.environ.get(k) for k in ENV_KEYS}
        self._old_s = dict(sm._S)
        self._old_m = dict(upl._M)
        self._old_helper = upl._bucket_inventory

    def tearDown(self):
        for k, v in self._old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        sm._S.clear()
        sm._S.update(self._old_s)
        upl._M.clear()
        upl._M.update(self._old_m)
        upl._bucket_inventory = self._old_helper

    def _s3env(self):
        os.environ.update(GREPTIME_STORAGE_TYPE="S3", S3_ENDPOINT_URL="https://x",
                          S3_BUCKET="b", S3_ACCESS_KEY_ID="k", S3_SECRET_ACCESS_KEY="s")

    def test_paginated_totals(self):
        cli = FakeList([{"Contents": [{"Size": 5}, {"Size": 7}],
                         "IsTruncated": True, "NextContinuationToken": "t"},
                        {"Contents": [{"Size": 3}], "IsTruncated": False}])
        self.assertEqual(sm.bucket_inventory(cli, "b"), (15, 3))

    def test_empty_bucket_is_real_zero(self):
        cli = FakeList([{"IsTruncated": False}])
        self.assertEqual(sm.bucket_inventory(cli, "b"), (0, 0))

    def test_failure_keeps_prior_never_zero(self):
        upl._bucket_inventory = lambda c, b: (100, 4)  # noqa: E731
        upl.refresh_inventory(object(), "b")
        self.assertEqual(upl._M["inventory_success"], 1)

        def _boom(client, bucket):
            raise ConnectionError("down")
        upl._bucket_inventory = _boom
        upl.refresh_inventory(object(), "b")
        # Prior totals survive; failure is explicit, never a fake zero.
        self.assertEqual((upl._M["bucket_bytes"], upl._M["bucket_objects"]),
                         (100, 4))
        self.assertEqual(upl._M["inventory_success"], 0)

    def test_file_mode_never_touches_s3(self):
        os.environ.update(GREPTIME_STORAGE_TYPE="File", S3_ENDPOINT_URL="https://x",
                          S3_BUCKET="b", S3_ACCESS_KEY_ID="k", S3_SECRET_ACCESS_KEY="s")
        self.assertFalse(sm.s3_configured())
        before = set(sys.modules)
        self.assertIsNone(sm.resolve_client())
        self.assertNotIn("boto3", set(sys.modules) - before)

    def test_s3_without_keys_is_disabled(self):
        os.environ.update(GREPTIME_STORAGE_TYPE="S3", S3_ENDPOINT_URL="",
                          S3_BUCKET="", S3_ACCESS_KEY_ID="", S3_SECRET_ACCESS_KEY="")
        self.assertFalse(sm.s3_configured())
        self.assertIsNone(sm.resolve_client())

    def test_render_absent_until_success(self):
        self._s3env()
        self.assertNotIn("datalake_s3_bucket_bytes", sm.render())  # no fake zero
        sm.refresh_once(FakeList([{"Contents": [{"Size": 9}],
                                   "IsTruncated": False}]), "b")
        body = sm.render()
        self.assertIn('datalake_s3_bucket_bytes{scope="greptime"} 9', body)
        self.assertIn('datalake_s3_bucket_objects{scope="greptime"} 1', body)
        sm.refresh_once(FakeList(fail=TimeoutError("stale")), "b")
        stale = sm.render()
        self.assertIn('datalake_s3_bucket_bytes{scope="greptime"} 9', stale)
        self.assertIn("datalake_s3_inventory_success 0", stale)

    def test_wal_usage_excludes_sst_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "wal" / "nested").mkdir(parents=True)
            (root / "wal" / "first").write_bytes(b"123")
            (root / "wal" / "nested" / "second").write_bytes(b"4567")
            (root / "sst").write_bytes(b"x" * 100)
            (root / "wal" / "link").symlink_to(root / "sst")
            metrics = sm.filesystem_metrics(directory)
            self.assertEqual(metrics["datalake_wal_bytes"], 7)
            self.assertEqual(metrics["datalake_local_storage_success"], 1)
        self.assertEqual(sm.filesystem_metrics(directory),
                         {"datalake_local_storage_success": 0})

    def test_malformed_inventory_keeps_last_good_measurement(self):
        sm.refresh_once(FakeList([{"Contents": [{"Size": 9}]}]), "b")
        sm.refresh_once(FakeList([{"Contents": [{"Size": "invalid"}]}]), "b")
        self.assertIn('datalake_s3_bucket_bytes{scope="greptime"} 9', sm.render())
        self.assertIn("datalake_s3_inventory_success 0", sm.render())


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)
