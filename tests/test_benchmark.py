"""Harness compatibility and fail-closed reporting without services."""
import json
import shutil
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from tools import benchmark as bench


class ComparisonTests(unittest.TestCase):
    def result(self):
        return dict(schema_version=1, profile="quick", duration_seconds=60,
                    fixture_sha256="fixture", harness_sha256="harness", runtime={},
                    revision="synthetic", status="passed", workloads={"synthetic": {
                        "golden_sha256": "golden", "units": 5,
                        "median": {"wall_seconds": 1.0, "disk_growth_bytes": 0}}})

    def compare(self, left, right, directory):
        root = Path(directory)
        (root / "base.json").write_text(json.dumps(left))
        (root / "head.json").write_text(json.dumps(right))
        bench.compare(SimpleNamespace(base=root / "base.json", candidate=root / "head.json", output=root / "report"))

    def test_advisory_regression_and_outputs(self):
        left, right = self.result(), self.result()
        right["workloads"]["synthetic"]["median"]["wall_seconds"] = 100
        with tempfile.TemporaryDirectory() as directory:
            self.compare(left, right, directory)
            report = Path(directory) / "report"
            self.assertTrue(json.loads((report / "comparison.json").read_text())["advisory"])
            self.assertIn("+9900.0%", (report / "summary.md").read_text())
            self.assertIn("n/a", (report / "summary.md").read_text())
            self.assertTrue((report / "results.csv").exists())

    def test_incompatible_fixtures_fail(self):
        for key in ("schema_version", "profile", "duration_seconds", "fixture_sha256", "harness_sha256", "runtime"):
            left, right = self.result(), self.result()
            right[key] = "changed"
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                with self.assertRaisesRegex(ValueError, "incompatible"):
                    self.compare(left, right, directory)

    def test_correctness_failure_is_not_advisory(self):
        for kind in ("status", "golden", "units", "workloads"):
            left, right = self.result(), self.result()
            if kind == "status":
                right["status"] = "failed"
            elif kind == "workloads":
                right["workloads"] = {}
            else:
                right["workloads"]["synthetic"]["golden_sha256" if kind == "golden" else "units"] = "changed"
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    self.compare(left, right, directory)

    def test_target_cannot_shadow_shared_harness(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            shutil.copytree(repo / "scripts", target / "scripts", ignore=shutil.ignore_patterns("__pycache__"))
            (target / "bench_workloads.py").write_text('raise RuntimeError("wrong harness")')
            result = subprocess.run([sys.executable, str(repo / "tools/benchmark.py"), "worker",
                                     "--repo", str(target), "--workload", "ai", "--size", "5"],
                                    check=True, capture_output=True, text=True, timeout=30)
            self.assertEqual(json.loads(result.stdout)["units"], 5)

    def test_duration_bounds(self):
        for duration in ("0", "601", "-5"):
            with self.subTest(duration=duration), self.assertRaises(SystemExit):
                bench.main(["run", "--repo", ".", "--output", "unused", "--revision", "test", "--duration", duration])


if __name__ == "__main__":
    unittest.main()
