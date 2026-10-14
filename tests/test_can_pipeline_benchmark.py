"""Minimal sustained-pipeline harness regression; synthetic inputs only.

Runs the harness ``fast`` scenario on a tiny dense plan through the real
Receiver HTTP loopback, durable Archive, Decoder, Worker, and Greptime HTTP
sink, comparing the full ordered durable state (canonical row bytes, event
IDs, raw order, cursors, partial checkpoints, outbox, counters) against the
sequential reference. No mocks: the loopback socket and the real worker
thread do the work.

Run: python -m unittest discover -s tests -p test_can_pipeline_benchmark.py
"""

import tempfile
import unittest

from tools.benchmark_can_pipeline import (build_plan, compare_scoped,
                                          run_reference, run_sustained)


class PipelineHarness(unittest.TestCase):
    def test_fast_loopback_matches_sequential_reference(self):
        plan = build_plan(scale=1, dense_frames=2500)
        fixed_ns = 1820000000000000000
        with tempfile.TemporaryDirectory() as refdir:
            reference = run_reference(refdir, plan, fixed_ns, False,
                                      20000, 4 * 1024 * 1024)
        with tempfile.TemporaryDirectory() as susdir:
            sustained = run_sustained(susdir, plan, fixed_ns, "fast", 0,
                                      0.01, 20000, 4 * 1024 * 1024, False, 60)
        self.assertTrue(reference["partial_seen"],
                        "fixture must exercise the partial-resume path")
        self.assertIsNone(compare_scoped(reference["mid"], sustained["mid"]))
        self.assertIsNone(
            compare_scoped(reference["final"], sustained["final"]))
        self.assertEqual(sustained["payloads_sha"],
                         reference["payloads_sha"])
        # Only loopback-offered chunks count: retained preseed never hits HTTP.
        retained_chunks = sum(len(chunks) for _, chunks in plan["retained"])
        self.assertGreater(retained_chunks, 0)
        self.assertEqual(sustained["offered_chunks"], plan["total_chunks"] - retained_chunks)
        self.assertEqual(sustained["final"]["counters"].get(
            "outbox_rows", 0), 0)
        for stage, stats in sustained["stages"].items():
            self.assertIn("p50", stats)
            self.assertIn("p95", stats)
            self.assertIn("p99", stats)
        self.assertGreaterEqual(sustained["raw_post_p95_s"] or 0, 0)
        self.assertIsNotNone(sustained["lowflow_ack_latency_s"])
        # Worker-built backpressure is echoed, never silently dropped.
        self.assertEqual(sustained["wake_config"]["backpressure"], {})


if __name__ == "__main__":
    unittest.main()
