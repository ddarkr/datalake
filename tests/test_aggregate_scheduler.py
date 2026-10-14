"""Scheduler cadence regressions for scripts/analytics/aggregate.py.

Run with: python -m tests.test_aggregate_scheduler
"""

import contextlib
import tempfile
import unittest
from unittest.mock import patch

from scripts.analytics import aggregate as agg

CTX = ("", "", "")
CFG = {"interval_s": 300}


class PassFailureContainmentTest(unittest.TestCase):
    def test_discovery_failure_isolates_dependents_and_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/aggregate-status.json"
            agg.write_status({"last_success_timestamp_seconds": 50.0}, path)
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch.object(agg.time, "time", return_value=100.0))
                stack.enter_context(patch.object(
                    agg, "_distinct_scopes", side_effect=agg.SqlError("timeout")))
                independent = [stack.enter_context(patch.object(agg, name, return_value=1))
                               for name in ("ai_section", "log_section", "home_section")]
                dependent = [stack.enter_context(patch.object(agg, name))
                             for name in ("vehicle_section", "trip_section",
                                          "vehicle_history_section", "battery_section",
                                          "_legacy_cfgs", "vehicle_freshness",
                                          "vehicle_window_lag")]
                counts, failed = agg.run_pass(CTX, CFG, path)
                self.assertEqual(counts, {"ai": 1, "logs": 1, "home": 1})
                for fn in independent:
                    fn.assert_called_once()
                for fn in dependent:
                    fn.assert_not_called()
            self.assertIn("vehicle_discovery", failed)
            self.assertTrue(set(("vehicle", "trip_charge", "vehicle_history", "battery"))
                            <= set(failed))
            bad = agg.read_status(path)
            self.assertEqual(bad["success"], 0)
            self.assertEqual(bad["running"], 0)
            self.assertEqual(bad["last_success_timestamp_seconds"], 50.0)
            self.assertEqual(bad["vehicle_window_observation_success"], 0)
            with patch.object(agg.time, "time", return_value=200.0), \
                    patch.object(agg, "run_all", return_value=(
                        {"vehicle": 2}, [], {"vehicle": 0.1}, {})), \
                    patch.object(agg, "vehicle_window_lag", return_value={}):
                _, failed = agg.run_pass(CTX, CFG, path)
            self.assertEqual(failed, [])
            good = agg.read_status(path)
            self.assertEqual(good["last_success_timestamp_seconds"], 200.0)
            self.assertEqual(good["last_failure_timestamp_seconds"], 100.0)


class TerminationTest(unittest.TestCase):
    def test_keyboard_interrupt_propagates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/aggregate-status.json"
            with patch.object(agg, "run_all", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    agg.run_pass(CTX, CFG, path)
            self.assertEqual(agg.read_status(path)["running"], 0)

    def test_system_exit_propagates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/aggregate-status.json"
            with patch.object(agg, "run_all", side_effect=SystemExit(1)):
                with self.assertRaises(SystemExit):
                    agg.run_pass(CTX, CFG, path)


class CadenceTest(unittest.TestCase):
    def _stack(self, stack, cfg, path, clock, distinct):
        stack.enter_context(patch.object(agg, "load_cfg",
                                         return_value=cfg))
        stack.enter_context(patch.object(agg, "request_sql",
                                         return_value=None))
        stack.enter_context(patch.object(agg, "_distinct_scopes",
                                         side_effect=distinct))
        for name in ("ai_section", "log_section", "vehicle_section",
                     "trip_section", "vehicle_history_section",
                     "home_section", "battery_section"):
            stack.enter_context(patch.object(agg, name, return_value=1))
        stack.enter_context(patch.object(agg, "vehicle_freshness",
                                         return_value={
                                             "observation_success": 1}))
        stack.enter_context(patch.object(agg, "vehicle_window_lag",
                                         return_value={}))
        stack.enter_context(patch.object(agg.run_pass, "__defaults__",
                                         (path,)))
        stack.enter_context(patch.object(agg.startup_failure,
                                         "__defaults__", (path,)))
        stack.enter_context(patch.object(agg.time, "monotonic",
                                         side_effect=lambda: clock[0]))
        stack.enter_context(patch.object(agg.time, "time",
                                         side_effect=lambda: clock[0]))
        stack.enter_context(patch.object(
            agg.time, "sleep",
            side_effect=lambda s: clock.__setitem__(0, clock[0] + s)))
        stack.enter_context(patch.dict(agg.os.environ, {"AGG_RUN_ONCE": ""}))

    def test_failed_then_recovery_keeps_start_to_start_cadence(self):
        cfg = {"base_url": "http://x", "user": "u", "password": "p",
               "db": "d", "interval_s": 100}
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/aggregate-status.json"
            clock = [1000.0]
            starts = []
            calls = [0]

            def distinct(ctx, cfg_arg):
                starts.append(clock[0])
                calls[0] += 1
                if calls[0] == 1:
                    raise agg.SqlError("boom")
                agg.STOP = True
                return []

            agg.STOP = False
            try:
                with contextlib.ExitStack() as stack:
                    self._stack(stack, cfg, path, clock, distinct)
                    self.assertEqual(agg.main(), 0)
            finally:
                agg.STOP = False
            # Responsive STOP: no extra sleep after the recovery pass.
            self.assertEqual(clock[0], 1100.0)
            status = agg.read_status(path)
            self.assertEqual(status["success"], 1)
            self.assertEqual(status["failed_sections"], [])
            self.assertEqual(status["last_failure_timestamp_seconds"], 1000.0)
            self.assertEqual(status["last_success_timestamp_seconds"], 1100.0)

    def test_hung_pass_restarts_cadence_without_sprint(self):
        cfg = {"base_url": "http://x", "user": "u", "password": "p",
               "db": "d", "interval_s": 100}
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/aggregate-status.json"
            clock = [1000.0]
            starts = []
            calls = [0]

            def distinct(ctx, cfg_arg):
                starts.append(clock[0])
                calls[0] += 1
                if calls[0] >= 2:
                    agg.STOP = True
                return []

            slowed = [False]

            def slow_ai(ctx, cfg_arg):
                if not slowed[0]:
                    slowed[0] = True
                    clock[0] += 250.0
                return 1

            agg.STOP = False
            try:
                with contextlib.ExitStack() as stack:
                    self._stack(stack, cfg, path, clock, distinct)
                    stack.enter_context(patch.object(agg, "ai_section",
                                                     side_effect=slow_ai))
                    self.assertEqual(agg.main(), 0)
            finally:
                agg.STOP = False
            # Slow first pass pushes the next start; no catch-up sprint.
            self.assertEqual(starts, [1250.0, 1250.0])
            self.assertEqual(clock[0], 1250.0)


class RunOnceTest(unittest.TestCase):
    def test_failed_presection_exits_nonzero_and_reports_failed(self):
        cfg = {"base_url": "http://x", "user": "u", "password": "p",
               "db": "d", "interval_s": 300}
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/aggregate-status.json"
            with patch.object(agg, "load_cfg", return_value=cfg), \
                    patch.object(agg, "request_sql", return_value=None), \
                    patch.object(agg, "_distinct_scopes",
                                 side_effect=agg.SqlError("timeout")), \
                    patch.object(agg, "ai_section", return_value=1), \
                    patch.object(agg, "log_section", return_value=1), \
                    patch.object(agg, "home_section", return_value=1), \
                    patch.object(agg, "vehicle_window_lag", return_value={}), \
                    patch.object(agg.run_pass, "__defaults__", (path,)), \
                    patch.object(agg.startup_failure, "__defaults__",
                                 (path,)), \
                    patch.object(agg.time, "time", return_value=500.0), \
                    patch.dict(agg.os.environ, {"AGG_RUN_ONCE": "1"}):
                self.assertEqual(agg.main(), 3)
            status = agg.read_status(path)
            self.assertEqual(status["success"], 0)
            self.assertIn("vehicle_discovery", status["failed_sections"])
            self.assertEqual(status["section_counts"], {"ai": 1, "logs": 1, "home": 1})
            self.assertEqual(status["last_failure_timestamp_seconds"], 500.0)

    def test_successful_pass_exits_zero(self):
        cfg = {"base_url": "http://x", "user": "u", "password": "p",
               "db": "d", "interval_s": 300}
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/aggregate-status.json"
            with patch.object(agg, "load_cfg", return_value=cfg), \
                    patch.object(agg, "request_sql", return_value=None), \
                    patch.object(agg, "_distinct_scopes", return_value=[]), \
                    patch.object(agg, "ai_section", return_value=1), \
                    patch.object(agg, "log_section", return_value=1), \
                    patch.object(agg, "vehicle_section", return_value=1), \
                    patch.object(agg, "trip_section", return_value=1), \
                    patch.object(agg, "vehicle_history_section",
                                 return_value=1), \
                    patch.object(agg, "home_section", return_value=1), \
                    patch.object(agg, "battery_section", return_value=1), \
                    patch.object(agg, "vehicle_freshness",
                                 return_value={"observation_success": 1}), \
                    patch.object(agg, "vehicle_window_lag", return_value={}), \
                    patch.object(agg.run_pass, "__defaults__", (path,)), \
                    patch.object(agg.startup_failure, "__defaults__",
                                 (path,)), \
                    patch.object(agg.time, "time", return_value=600.0), \
                    patch.dict(agg.os.environ, {"AGG_RUN_ONCE": "1"}):
                self.assertEqual(agg.main(), 0)
            status = agg.read_status(path)
            self.assertEqual(status["success"], 1)
            self.assertEqual(status["failed_sections"], [])


if __name__ == "__main__":
    unittest.main()
