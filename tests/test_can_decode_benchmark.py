"""Regression-gate behavior with measured runner-drift samples."""

import io
import json
import os
import subprocess
import tempfile
import textwrap
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

    def test_ci_scope_runs_real_git_decision_without_benchmark_dependencies(self):
        workflow = (benchmark.CURRENT_ROOT / ".github/workflows/compose.yml").read_text()
        job = workflow.split("  can-decode-perf:\n", 1)[1].split(
            "  storage-cost-budget:\n", 1)[0]
        # Execute the actual scope shell, not assertions about YAML spelling.
        scope = textwrap.dedent(job.split("        run: |\n", 1)[1].split(
            "      - name:", 1)[0])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo, runner = root / "repo", root / "runner"
            repo.mkdir()
            runner.mkdir()

            def git(*args):
                return subprocess.check_output(["git", *args], cwd=repo,
                                               text=True).strip()

            git("init", "-q")
            git("config", "user.email", "fixture@example.invalid")
            git("config", "user.name", "Fixture")
            git("commit", "--allow-empty", "-qm", "base")
            base = git("rev-parse", "HEAD")

            def decide(before, head, event="push"):
                output = runner / "output"
                output.write_text("")
                env = dict(os.environ, RUNNER_TEMP=str(runner),
                           GITHUB_OUTPUT=str(output), GITHUB_EVENT_NAME=event,
                           PUSH_BEFORE_SHA=before, PR_BASE_SHA=before,
                           GITHUB_SHA=head, PR_HEAD_SHA=head)
                result = subprocess.run(["bash", "-c", scope], cwd=repo,
                                        env=env, capture_output=True, text=True)
                return result.returncode, output.read_text()

            paths = [
                ("docs/operations.md", False), ("compose/grafana.yaml", False),
                ("scripts/ingest/can/can_receiver.py", True),
                ("scripts/ingest/can/can_decoder.py", True),
                ("scripts/ingest/can/can_files.py", True),
                ("scripts/ingest/can/can_otlp_wire.py", True),
                ("scripts/ingest/can/한글\ncapture.py", True),
                ("scripts/ingest/__init__.py", True),
                ("tools/benchmark_can_decode.py", True),
                ("tools/benchmark-requirements.txt", True),
                ("tests/test_can_decode_benchmark.py", True),
                (".github/workflows/compose.yml", True),
            ]
            for path, relevant in paths:
                with self.subTest(path=path):
                    target = repo / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text("fixture\n")
                    git("add", path)
                    git("commit", "-qm", path)
                    head = git("rev-parse", "HEAD")
                    code, output = decide(base, head)
                    self.assertEqual(code, 0)
                    self.assertEqual(output.splitlines()[-1],
                                     f"available={str(relevant).lower()}")
                    if not relevant:
                        report = json.loads((runner / "can-decode-perf.json").read_text())
                        self.assertEqual(report["status"], "not-applicable")
                    code, output = decide(base, head, "pull_request")
                    self.assertEqual(code, 0)
                    self.assertEqual(output.splitlines()[-1],
                                     f"available={str(relevant).lower()}")
                    base = head
            for before in ("missing", "1" * 40):
                self.assertNotEqual(decide(before, head)[0], 0)
                self.assertFalse(json.loads(
                    (runner / "can-decode-perf.json").read_text())["baseline_available"])
            self.assertEqual(decide("0" * 40, head)[0], 0)
            self.assertNotEqual(decide("0" * 40, head, "pull_request")[0], 0)
            self.assertEqual(decide(base, head, "pull_request")[0], 0)


if __name__ == "__main__":
    unittest.main()
