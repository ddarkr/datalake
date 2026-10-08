"""Storage usage inventory regressions: real bucket totals, no fake zeros."""
import json
import os
import sys
import unittest
import tempfile
from pathlib import Path

from scripts.storage import storage_metrics as sm
from scripts.vehicle.raw import raw_upload as upl

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


class OperationalMarkersTest(unittest.TestCase):
    def test_unknown_fresh_stale_and_failed_preserve_success(self):
        with tempfile.TemporaryDirectory() as directory:
            def metrics():
                return sm.operational_metrics(directory)
            self.assertEqual(metrics()["datalake_aggregate_status_known"], 0)
            self.assertEqual(metrics()["datalake_restore_verification_known"], 0)
            path = Path(directory, "aggregate-status.json")
            path.write_text(json.dumps({"timestamp_seconds": 100, "running": 0,
                                       "success": 1, "last_success_timestamp_seconds": 100,
                                       "interval_seconds": 300}))
            good = metrics()
            self.assertEqual(good["datalake_aggregate_last_success_timestamp_seconds"], 100)
            # Age belongs to the consumer: scraping must not renew persisted success.
            self.assertEqual(metrics()["datalake_aggregate_status_timestamp_seconds"], 100)
            path.write_text(json.dumps({"timestamp_seconds": 500, "running": 0,
                                       "success": 0, "last_success_timestamp_seconds": 100,
                                       "last_failure_timestamp_seconds": 500,
                                       "vehicle_window_observation_success": 0}))
            failed = metrics()
            self.assertEqual(failed["datalake_aggregate_success"], 0)
            self.assertEqual(failed["datalake_aggregate_last_success_timestamp_seconds"], 100)
            self.assertFalse(any("lag_seconds" in key for key in failed))
            path.write_text("{broken")
            self.assertEqual(metrics()["datalake_aggregate_status_known"], 0)
            self.assertNotIn("datalake_aggregate_success", metrics())
            Path(directory, "restore-verification.json").write_text(json.dumps(
                {"timestamp_seconds": 90, "backup_id": "synthetic"}))
            self.assertEqual(metrics()["datalake_restore_verification_timestamp_seconds"], 90)
            self.assertEqual(metrics()["datalake_restore_verification_known"], 1)


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)
