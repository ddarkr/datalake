"""Synthetic benchmark contract tests; no services, vehicle data, or network."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tools import bench_workloads as bench


class BenchmarkWorkloads(unittest.TestCase):
    def test_versioned_golden_and_replay_parity(self):
        expected = {
            "can": "2146cd31b078ad44752c198a446371fc61c6597087aee57754e2d79c190a6702",
            "fleet": "5dcedd46b0df692f3224adc90781a27993928133fa9adfe97a7682f8df27c3cf",
            "vss": "d67f547ea6d71513e03f743777a5366b88f53ba664f8e1f21380ad3bb9585a4e",
        }
        self.assertEqual(bench.FIXTURE_VERSION, "synthetic-bench-v1")
        self.assertEqual(bench.FIXTURE_SEED, 1729)
        # A network attempt is a test failure, even if the app catches its error.
        with patch("socket.create_connection", side_effect=AssertionError("offline only")) as connect, \
             patch("urllib.request.urlopen", side_effect=AssertionError("offline only")) as urlopen:
            for name in bench.WORKLOADS:
                results = []
                for fault in (False, True):
                    with self.subTest(name=name, fault=fault), tempfile.TemporaryDirectory() as directory:
                        result = bench.run_workload(name, 5, directory, fault=fault)
                        self.assertEqual(result["units"], 5)
                        json.dumps(result, allow_nan=False)
                        results.append(result["golden"])
                        if name in expected:
                            self.assertEqual(result["golden"]["sha256"], expected[name])
                            self.assertGreater(result["disk_pending_bytes"], 0)
                self.assertEqual(results[0], results[1])
            connect.assert_not_called()
            urlopen.assert_not_called()

    def test_can_dense_chunk_partial_resume(self):
        results = []
        for fault in (False, True):
            with tempfile.TemporaryDirectory() as directory:
                result = bench.run_workload("can", 129, directory, fault=fault)
                self.assertEqual(result["golden"]["raw_chunks"], 2)
                self.assertEqual(result["golden"]["rows"], 129)
                results.append(result["golden"])
        self.assertEqual(results[0], results[1])

    def test_ai_null_zero_and_known_totals(self):
        with tempfile.TemporaryDirectory() as directory:
            golden = bench.run_workload("ai", 5, directory)["golden"]
        sessions = {s["session_id"]: s for s in golden["sessions"]}
        self.assertEqual(sessions["synthetic-0"]["total"], 20)
        self.assertEqual(sessions["synthetic-1"]["total"], 0)
        self.assertIsNone(sessions["synthetic-2"]["total"])
        self.assertEqual(bench._digest(golden),
                         "73228570887ac6e92b2d55998a1254c6141389117ead590506e895f6c98f117c")
        self.assertEqual(golden["daily"][0]["total"], 20)
        self.assertEqual(golden["daily"][0]["cost_unpriced_calls"], 4)
        self.assertIsNone(golden["daily"][0]["cost_estimated_usd"])

    def test_fault_ack_replay_leaves_tombstones_not_pending_rows(self):
        for name in ("fleet", "vss"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                bench.run_workload(name, 5, directory, fault=True)
                conn = sqlite3.connect(Path(directory) / name / "outbox.sqlite3")
                try:
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM seen_ids").fetchone()[0], 5)
                finally:
                    conn.close()

    def test_prepared_call_is_single_use_and_refuses_existing_data(self):
        with tempfile.TemporaryDirectory() as directory:
            operation = bench.prepare("ai", 1, directory)
            self.assertEqual(operation()["units"], 1)
            with self.assertRaisesRegex(RuntimeError, "single-use"):
                operation()
            with self.assertRaises(FileExistsError):
                bench.prepare("ai", 1, directory)

    def test_invalid_sizes_and_unknown_workload_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            for size in (0, -1, True, 1.5, "5"):
                with self.subTest(size=size), self.assertRaises(ValueError):
                    bench.prepare("ai", size, directory)
            with self.assertRaises(ValueError):
                bench.prepare("unknown", 1, directory)

    def test_corrupt_aggregation_is_a_hard_failure(self):
        from scripts.analytics import aggregate
        original = aggregate.summarize_sessions

        def corrupt(*args, **kwargs):
            rows = original(*args, **kwargs)
            rows[0]["input"] = -999
            return rows

        with tempfile.TemporaryDirectory() as directory:
            operation = bench.prepare("ai", 5, directory)
            with patch.object(aggregate, "summarize_sessions", side_effect=corrupt), \
                 self.assertRaisesRegex(AssertionError, "AI unknown/zero/reported"):
                operation()

    def test_durability_rejects_weaker_sqlite_modes(self):
        conn = sqlite3.connect(":memory:")
        try:
            with self.assertRaisesRegex(AssertionError, "journal mode"):
                bench._durability(conn)
        finally:
            conn.close()
        with tempfile.TemporaryDirectory() as directory:
            conn = sqlite3.connect(Path(directory) / "test.sqlite3")
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                with self.assertRaisesRegex(AssertionError, "synchronous FULL"):
                    bench._durability(conn)
            finally:
                conn.close()

    def test_hard_checks_do_not_use_optional_assert_statements(self):
        with self.assertRaises(AssertionError):
            bench._check(False, "hard failure")


if __name__ == "__main__":
    unittest.main()
