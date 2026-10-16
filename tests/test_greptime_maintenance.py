"""Pure checks for the native maintenance evidence oracles (no engine mocks)."""

import unittest

from tools.greptime_maintenance_cases import assert_rows, file_ids, is_scan_limit_error, mixed_sst_files


class MaintenanceOracles(unittest.TestCase):
    def test_exact_values_null_and_replay_cardinality(self):
        rows = [{"series": "a", "value": None, "version": 2}]
        self.assertEqual(assert_rows(rows, [("a", None, 2)])["rows"], 1)
        with self.assertRaises(AssertionError):
            assert_rows(rows, [("a", 0, 2)])
        with self.assertRaises(AssertionError):
            assert_rows(rows + rows, [("a", None, 2)])
        with self.assertRaises(AssertionError):
            assert_rows(rows, [("a", None, 1)])

    def test_manifest_retirement_does_not_require_replacement(self):
        before = file_ids([{"file_id": "expired"}])
        after = file_ids([])
        self.assertEqual(before - after, {"expired"})
        self.assertEqual(after - before, set())
        self.assertEqual(file_ids([{"file_path": "fixture.parquet"}]), {"fixture.parquet"})
        with self.assertRaises(AssertionError):
            file_ids([{"file_size": 42}])

    def test_mixed_expiry_requires_one_physical_sst_crossing_cutoff(self):
        split = [{"min_ts": 10, "max_ts": 10, "num_rows": 1},
                 {"min_ts": 30, "max_ts": 30, "num_rows": 1}]
        with self.assertRaises(AssertionError):
            mixed_sst_files(split, 20)
        spanning = {"min_ts": 10, "max_ts": 30, "num_rows": 2}
        self.assertEqual(mixed_sst_files([spanning], 20), [spanning])
        for cutoff in (10, 30, 40):
            with self.assertRaises(AssertionError):
                mixed_sst_files([spanning], cutoff)

    def test_only_native_scan_limit_error_is_expected(self):
        self.assertTrue(is_scan_limit_error(RuntimeError("TooManyFilesToRead { num_files: 12 }")))
        self.assertTrue(is_scan_limit_error(RuntimeError("Too many files to read concurrently: 12, max allowed: 8")))
        for message in ("too many open files", "S3 timeout", "TooManyFilesToReadOther",
                        "Too many files to read, max: 8", "HTTP 500"):
            self.assertFalse(is_scan_limit_error(RuntimeError(message)))


if __name__ == "__main__":
    unittest.main()
