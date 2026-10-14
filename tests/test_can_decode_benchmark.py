"""Regression-gate behavior with measured runner-drift samples."""

import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from tools import benchmark_can_decode as benchmark


class DecodeBenchmark(unittest.TestCase):
    def run_gate(self, baseline, current):
        roots = {Path("/fixture/baseline"): iter(baseline),
                 benchmark.CURRENT_ROOT: iter(current)}

        def measured(root, *args):
            wall = next(roots[Path(root)])
            return {"wall_s": wall, "cpu_s": wall, "total_chunks": 1,
                    "total_bytes": 10, "outbox_rows": 0, "calls": 1,
                    "staged_chunks": 1, "peak_rss_bytes": 0,
                    "connections": None,
                    "snapshot": {"epoch": "fixture", "outbox": [],
                                 "states": [], "partial": [], "counters": {}}}

        output = io.StringIO()
        with patch.object(benchmark, "_run_side", measured), redirect_stdout(output):
            status = benchmark.main(["--baseline-root", "/fixture/baseline",
                                     "--cases", "large", "--repeats", "3"])
        return status, json.loads(output.getvalue())

    def test_adjacent_pairs_distinguish_runner_drift_from_sustained_slowdown(self):
        # CI samples: independent medians falsely report a 30.8% regression.
        status, report = self.run_gate(
            [1.501056265, 1.603101215, 2.486601041],
            [3.005614113, 1.259433806, 2.096893607])
        self.assertEqual(status, 0)
        self.assertEqual(report["regressions"], [])
        self.assertAlmostEqual(report["cases"]["large"]["slowdown_wall"],
                               -0.1567229433, places=8)

        status, report = self.run_gate([1, 4, 10], [1.3, 5.2, 13])
        self.assertEqual(status, 1)
        self.assertEqual(report["regressions"][0]["case"], "large")
        self.assertAlmostEqual(report["cases"]["large"]["slowdown_wall"], 0.3)


if __name__ == "__main__":
    unittest.main()
