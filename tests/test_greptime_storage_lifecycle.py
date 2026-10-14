"""Greptime storage lifecycle behavioral tests (no Docker, no copies).

Covers observable behavior only: the urlencoded SQL form + error/code
handling against a stub /v1/sql server, the demo-shaped record reader,
oracle math over all 20 Grafana-shaped queries (a wrong GROUP value
fails, not just a wrong length), bucket-millis normalization, and the
cold-vs-warm equality rule. Heavy lifecycle functions are never executed
here; Main owns the native Docker proof.
"""
import json
import math
import os
import sys
import threading
from pathlib import Path
import unittest
import tempfile
from unittest.mock import patch
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

import tools.benchmark_greptime_storage as bench
from tools.greptime_cache_reset import CACHE_DIRS, cache_config, reset_cache


def _oracle_stats(n):
    """Mirror the bench's deterministic rows + incremental fold (10M-safe)."""
    st = bench.new_stats()
    for i in range(n):
        seg, vehicle, path, ts_ms, val = bench.fixture_row(i, n)
        bench._fold(st, vehicle, path, val, seg, ts_ms)
    return st


class SqlFormTest(unittest.TestCase):
    """Actual HTTP form/result/error behavior of the SQL client."""

    def setUp(self):
        self.seen = {}

        def rows_for(sql):
            if "FAIL_CODE" in sql:
                return {"code": 40001, "error": "boom", "output": []}
            if "ERR_FIELD" in sql:
                return {"code": 0, "error": "stale", "output": []}
            rec = {"schema": {"column_schemas": [{"name": "a"}, {"name": "b"}]},
                   "rows": [["12.5", "x"]]}
            if "NO_CODE" in sql:
                return {"output": [{"records": rec}], "execution_time_ms": 6}
            return {"code": 0, "output": [{"records": rec}]}

        seen = self.seen

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or 0)
                body = self.rfile.read(length) if length else b""
                seen["ctype"] = self.headers.get("Content-Type")
                seen["auth"] = self.headers.get("Authorization")
                seen["form"] = urllib.parse.parse_qs(body.decode())
                doc = rows_for(seen["form"].get("sql", [""])[0])
                payload = json.dumps(doc).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever,
                         kwargs={"poll_interval": 0.05}, daemon=True).start()
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_form_is_urlencoded_sql(self):
        rows = bench.grecords(self.base, "SELECT 1")
        self.assertEqual(self.seen["ctype"],
                         "application/x-www-form-urlencoded")
        self.assertEqual(self.seen["form"]["sql"], ["SELECT 1"])
        self.assertEqual(rows, [{"a": "12.5", "b": "x"}])

    def test_native_success_without_code(self):
        self.assertEqual(bench.grecords(self.base, "NO_CODE SELECT 1"), [{"a": "12.5", "b": "x"}])

    def test_sql_auth_is_forwarded(self):
        bench.grecords(self.base, "SELECT 1", auth=("bench", "synthetic"))
        self.assertEqual(self.seen["auth"], "Basic YmVuY2g6c3ludGhldGlj")

    def test_nonzero_code_fails(self):
        with self.assertRaises(RuntimeError):
            bench.grecords(self.base, "FAIL_CODE SELECT")

    def test_error_field_fails(self):
        with self.assertRaises(RuntimeError):
            bench.grecords(self.base, "ERR_FIELD SELECT")


class OracleValueTest(unittest.TestCase):
    def test_twenty_dashboard_shaped_queries(self):
        st = _oracle_stats(400)
        queries = bench.build_queries(st)
        self.assertEqual(len(queries), 20)
        names = [q[0] for q in queries]
        self.assertEqual(len(set(names)), 20)
        self.assertIn("datebin_hours", names)
        self.assertIn("longrange_rollup_avg", names)
        self.assertIn("rollup_spot", names)
        st2 = _oracle_stats(400)
        self.assertEqual([q[2] for q in bench.build_queries(st2)],
                         [q[2] for q in queries])

    def test_wrong_group_value_fails_not_just_length(self):
        st = _oracle_stats(400)
        queries = bench.build_queries(st)
        gq = next(q for q in queries if q[0] == "groups_full")
        name, sql, expected, kind = gq
        self.assertEqual(kind, "rows")
        corrupted = [list(r) for r in expected]
        corrupted[0][2] += 1.0  # right row count, wrong count value
        ok = (len(corrupted) == len(expected)
              and all(all(bench._cell_equal(ce, cg)
                          for ce, cg in zip(e, g))
                      for e, g in zip(expected, corrupted)))
        self.assertFalse(ok)

    def test_wrong_bin_value_fails(self):
        st = _oracle_stats(400)
        queries = bench.build_queries(st)
        bq = next(q for q in queries if q[0] == "datebin_hours")
        corrupted = [list(r) for r in bq[2]]
        corrupted[-1][1] += 5.0
        self.assertFalse(all(
            all(bench._cell_equal(ce, cg) for ce, cg in zip(e, g))
            for e, g in zip(bq[2], corrupted)))

    def test_bucket_millis_normalization(self):
        self.assertEqual(bench._to_ms("2026-01-01 01:00:00"),
                         bench.BASE_MS + 3600 * 1000)
        self.assertIsNone(bench._to_ms("not-a-time"))
        self.assertTrue(bench._cell_equal(bench.BASE_MS,
                                          "2026-01-01 00:00:00"))

    def test_cold_warm_equality_rule(self):
        self.assertTrue(bench._got_equal([1.0, [2.0]], [1.0, [2.0]]))
        self.assertFalse(bench._got_equal([1.0], [2.0]))
        self.assertFalse(bench._got_equal([[1.0, 2.0]], [[1.0, 2.5]]))


class LifecycleEvidenceTest(unittest.TestCase):
    def test_ten_million_key_period_and_old_collision(self):
        rows = 10_000_000
        period = 6 * bench.HOUR_MS
        self.assertEqual(math.gcd(7, period), 1)
        self.assertLess((rows + 3) // 4, period)
        self.assertNotEqual(bench.fixture_row(0, rows)[1:4],
                            bench.fixture_row(540000, rows)[1:4])
        for seg in range(4):
            for i in (seg * (rows // 4), (seg + 1) * (rows // 4) - 1):
                got_seg, _, _, stamp, _ = bench.fixture_row(i, rows)
                self.assertEqual(got_seg, seg)
                self.assertGreaterEqual(stamp, bench.BASE_MS + seg * period)
                self.assertLess(stamp, bench.BASE_MS + (seg + 1) * period)

    def test_auth_mount_and_current_meter_do_not_require_baseline_harness(self):
        with tempfile.TemporaryDirectory() as tmp:
            bench.write_project(tmp, "/baseline-without-tools", "greptime/greptimedb:v1.2.1",
                                "python:3.12.8-slim-bookworm", "synthetic", "synthetic-password")
            with open(os.path.join(tmp, "docker-compose.yaml"), encoding="utf-8") as f:
                compose = f.read()
            self.assertIn("./preflight/auth:/etc/greptime/auth:ro", compose)
            self.assertIn(os.path.join(bench.REPO, "tools", "s3_meter.py"), compose)
            self.assertNotIn("/baseline-without-tools/tools", compose)

    def test_scheduled_or_partial_compaction_is_not_completed(self):
        before = {"completed": 0, "failures": 0, "inflight": 0}
        files = {"input.parquet"}
        changed = {"input.parquet", "output.parquet"}
        self.assertFalse(bench.compaction_completed(before, before, files, changed))
        after = {"completed": 1, "failures": 0, "inflight": 1}
        self.assertFalse(bench.compaction_completed(before, after, files, changed))
        after["inflight"] = 0
        self.assertTrue(bench.compaction_completed(before, after, files, changed))
        self.assertFalse(bench.compaction_completed(before, after, files, files))
        after["failures"] = 1
        self.assertFalse(bench.compaction_completed(before, after, files, changed))

    def test_cleanup_failure_retains_files_and_records_inspection_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "docker-compose.yaml"), "w") as f:
                f.write("services: {}\n")
            env = {"MINIO_PORT": "12345", "GREPTIME_PORT": "12346", "SNAP_PORT": "12347"}
            evidence = {}
            with patch.object(bench, "compose_run", return_value=(1, "", "down failed")) as compose, \
                    patch.object(bench, "sh", return_value=(1, "", "inspect failed")):
                bench.cleanup_project(tmp, "owned", env, evidence)
            self.assertEqual(compose.call_args.args[2], env)
            self.assertFalse(evidence["cleanup"]["success"])
            self.assertEqual(evidence["cleanup"]["recovery_dir"], tmp)
            self.assertTrue(os.path.isdir(tmp))
            self.assertEqual(evidence["cleanup"]["residual_networks"], ["inspection-failed"])

    def test_cleanup_before_compose_exists_still_checks_resources(self):
        with tempfile.TemporaryDirectory() as tmp:
            evidence = {}
            with patch.object(bench, "sh", return_value=(0, "", "")), \
                    patch.object(bench, "compose_run") as compose:
                bench.cleanup_project(tmp, "owned", {}, evidence)
            compose.assert_not_called()
            self.assertTrue(evidence["cleanup"]["success"])
            self.assertFalse(os.path.exists(tmp))

    def test_cleanup_removes_stopped_owned_containers_and_networks(self):
        with tempfile.TemporaryDirectory() as tmp:
            evidence, commands = {}, []
            seen = set()
            def execute(command):
                commands.append(command)
                if "--filter" in command:
                    key = tuple(command[:3])
                    if key not in seen:
                        seen.add(key)
                        if command[1] == "ps":
                            return 0, "stopped-container", ""
                        if command[1] == "network":
                            return 0, "owned-network", ""
                    return 0, "", ""
                return 0, "", ""
            with patch.object(bench, "sh", side_effect=execute):
                bench.cleanup_project(tmp, "owned", {}, evidence)
            self.assertIn(["docker", "rm", "-f", "stopped-container"], commands)
            self.assertIn(["docker", "network", "rm", "owned-network"], commands)
            self.assertTrue(evidence["cleanup"]["success"])

    def test_uncertainty_zero_and_exact_twenty_percent_boundary(self):
        base = dict(read_bytes=100, write_bytes=100, compacted_bytes=100,
                    cold_get_bytes=100, warm_get_bytes=100, total_requests=100)
        cur = dict(base, read_bytes=119)
        self.assertFalse(bench.check_relative(cur, base, {"uncertainty_bytes": 0})[0])
        cur["read_bytes"] = 120
        self.assertTrue(bench.check_relative(cur, base, {"uncertainty_bytes": 0})[0])
        cur["read_bytes"] = 100
        self.assertFalse(bench.check_relative(cur, base, {"relative_growth_max": 0})[0])
        cur["read_bytes"] = 101
        self.assertTrue(bench.check_relative(cur, base, {"relative_growth_max": 0})[0])

    def test_uncertainty_cannot_credit_positive_payload_from_zero(self):
        base = dict(read_bytes=0, write_bytes=0, compacted_bytes=0,
                    cold_get_bytes=0, warm_get_bytes=0, total_requests=0)
        self.assertTrue(bench.check_relative(dict(base, read_bytes=1), base,
                                            {"uncertainty_bytes": 100})[0])


class CacheResetSafetyTest(unittest.TestCase):
    CONFIG = '''
[wal]
provider = "raft_engine"
dir = "/data/wal"
[storage]
type = "S3"
data_home = "/data"
bucket = "bench-data"
root = "greptime"
[[region_engine]]
[region_engine.mito]
enable_write_cache = false
'''

    def seed(self, root):
        protected = {"wal/0001.log": b"durable-wal", "data/primary.parquet": b"primary",
                     "metadata/catalog": b"catalog", "auth/users": b"synthetic-auth",
                     "storage-state.json": b"s3-identity"}
        for relative, data in protected.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        for relative, data in ((CACHE_DIRS[0] + "/nested/a.parquet", b"cached-sst"),
                               (CACHE_DIRS[1] + "/checkpoint", b"cached-manifest")):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return protected

    def test_only_cache_bytes_deleted_primary_and_volume_identity_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            protected = self.seed(root)
            inode = root.stat().st_ino
            result = reset_cache(root, self.CONFIG)
            self.assertEqual(result["deleted_files"], 2)
            self.assertEqual(result["deleted_bytes"], len(b"cached-sstcached-manifest"))
            self.assertEqual(root.stat().st_ino, inode)
            for relative, data in protected.items():
                self.assertEqual((root / relative).read_bytes(), data)
            self.assertTrue(all(not (root / relative).exists() for relative in CACHE_DIRS))
            self.assertEqual(reset_cache(root, self.CONFIG)["deleted_bytes"], 0)

    def test_symlink_in_second_cache_rejects_before_any_deletion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            protected = self.seed(root)
            (root / CACHE_DIRS[1] / "wal-link").symlink_to(root / "wal", target_is_directory=True)
            with self.assertRaises(RuntimeError):
                reset_cache(root, self.CONFIG)
            self.assertEqual((root / CACHE_DIRS[0] / "nested/a.parquet").read_bytes(), b"cached-sst")
            for relative, data in protected.items():
                self.assertEqual((root / relative).read_bytes(), data)

    def test_cache_ancestor_symlink_and_volume_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp).resolve()
            root = parent / "volume"
            root.mkdir()
            (root / "wal").mkdir()
            (root / "wal/primary").write_bytes(b"wal")
            (root / "cache").symlink_to(root / "wal", target_is_directory=True)
            with self.assertRaises(RuntimeError):
                reset_cache(root, self.CONFIG)
            alias = parent / "alias"
            alias.symlink_to(root, target_is_directory=True)
            with self.assertRaises(RuntimeError):
                reset_cache(alias, self.CONFIG)
            self.assertEqual((root / "wal/primary").read_bytes(), b"wal")

    def test_unknown_layout_cannot_delete_existing_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            self.seed(root)
            for text in (self.CONFIG.replace('data_home = "/data"', 'data_home = "/"'),
                         self.CONFIG.replace('type = "S3"', 'type = "File"'),
                         self.CONFIG + 'write_cache_path = "/data/wal"\n'):
                with self.assertRaises(RuntimeError):
                    reset_cache(root, text)
            self.assertTrue((root / CACHE_DIRS[0] / "nested/a.parquet").exists())
            layout = cache_config(self.CONFIG)
            self.assertFalse(layout["configured_enable_write_cache"])
            self.assertTrue(layout["runtime_enable_write_cache"])

    def test_metadata_404_and_startup_sst_gets_do_not_satisfy_cold_window(self):
        def snapshot(requests, read, sst_requests, sst_read):
            return {"counters": {"requests": requests, "read_bytes": read,
                                 "sst_get_requests": sst_requests, "sst_read_bytes": sst_read}}
        before = snapshot(10, 1000, 2, 900)
        for after in (snapshot(11, 1100, 2, 900), snapshot(11, 1400, 2, 900)):
            with self.assertRaises(RuntimeError):
                bench.sst_query_delta(before, after, require_cold=True)
        after = snapshot(12, 1500, 3, 1100)
        self.assertEqual(bench.sst_query_delta(before, after, require_cold=True),
                         {"sst_get_requests": 1, "sst_read_bytes": 200})
        self.assertEqual(bench.sst_query_delta(after, after)["sst_read_bytes"], 0)
        with self.assertRaises(RuntimeError):
            bench.sst_query_delta(after, before)

class MeterDiscoveryGateTest(unittest.TestCase):
    def snapshot(self, **changes):
        counters = {"client_errors": 1, "client_error_bytes": 300,
                    "expected_not_found_requests": 1, "expected_not_found_bytes": 300,
                    "errors": 0, "copy_unknown": 0, "truncated": 0, "unsupported": 0,
                    "requests": 7, "read_bytes": 1500}
        counters.update(changes)
        return {"counters": counters, "truncated_requests": 0, "requests": []}

    def test_classified_miss_preserves_all_original_cost_fields(self):
        snapshot = self.snapshot()
        original = dict(snapshot["counters"])
        gate = bench.validate_meter_errors(snapshot)
        self.assertEqual(gate["unexpected_client_errors"], 0)
        self.assertEqual(snapshot["counters"], original)
        scenario = bench.scenario_of({"meter_total": snapshot["counters"]})
        self.assertEqual(scenario["client_errors"], 1)
        self.assertEqual(scenario["client_error_bytes"], 300)
        self.assertEqual(scenario["total_requests"], 7)
        self.assertEqual(scenario["read_bytes"], 1500)

    def test_unclassified_sst_auth_and_metadata_client_errors_still_fail(self):
        snapshot = self.snapshot(expected_not_found_requests=0, expected_not_found_bytes=0)
        for op, kind, status in (("GET", "sst", 404), ("GET", "control", 403),
                                 ("HEAD", "control", 404), ("GET", "control", 404),
                                 ("HEAD", "drop_marker", 403)):
            snapshot["requests"] = [{"op": op, "object_kind": kind, "status": status,
                                     "req_bytes": 0, "resp_bytes": 300}]
            with self.assertRaises(RuntimeError):
                bench.validate_meter_errors(snapshot)

    def test_missing_inconsistent_and_transport_error_counters_fail_closed(self):
        bad = (self.snapshot(expected_not_found_requests=2),
               self.snapshot(expected_not_found_bytes=301),
               self.snapshot(expected_not_found_bytes=299),
               self.snapshot(errors=1), self.snapshot(truncated=1),
               self.snapshot(unsupported=1), self.snapshot(copy_unknown=1),
               self.snapshot(expected_not_found_requests=True))
        for snapshot in bad:
            with self.assertRaises(RuntimeError):
                bench.validate_meter_errors(snapshot)
        snapshot = self.snapshot()
        del snapshot["counters"]["expected_not_found_requests"]
        with self.assertRaises(RuntimeError):
            bench.validate_meter_errors(snapshot)

    def test_failure_diagnostics_cannot_persist_paths_queries_headers_or_bodies(self):
        snapshot = self.snapshot()
        request = {"op": "GET", "object_kind": "control", "status": 404,
                   "req_bytes": 0, "resp_bytes": 300,
                   "path": "private-object-secret", "query": "signed-secret",
                   "headers": {"Authorization": "credential-secret"},
                   "body": "error-body-secret"}
        snapshot["requests"] = [request]
        snapshot["client_error_breakdown"] = [dict(request, requests=1, bytes=300)]
        diagnosis = bench.meter_error_diagnostics(snapshot)
        rendered = json.dumps(diagnosis)
        self.assertNotIn("secret", rendered)
        row = {"status": 404, "op": "GET", "object_kind": "control",
               "expected_not_found": False, "requests": 1, "bytes": 300}
        self.assertEqual(diagnosis["sample_client_error_breakdown"], [row])
        self.assertEqual(diagnosis["cumulative_client_error_breakdown"], [row])
        snapshot["truncated_requests"] = 1
        self.assertFalse(bench.meter_error_diagnostics(snapshot)["sample_complete"])

if __name__ == "__main__":
    unittest.main()
