"""Budget regressions use actual deterministic collector JSON, not a mock schema."""

import copy
import functools
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(ROOT, "tools", "benchmark_backup_retention.py")
CHECK = os.path.join(ROOT, "tools", "check_cost_budget.py")
BUDGET = os.path.join(ROOT, "config", "cost-budget.json")
SPEC = importlib.util.spec_from_file_location("cost_budget_checker", CHECK)
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


@functools.lru_cache(maxsize=1)
def collected():
    with tempfile.TemporaryDirectory(prefix="budget-test-") as root:
        path = os.path.join(root, "measurements.json")
        result = subprocess.run([sys.executable, BENCH, "--baseline-root", ROOT,
                                 "--json-out", path, "--small-kb", "1", "--large-count", "4"],
                                capture_output=True, text=True, cwd=ROOT, timeout=600)
        assert result.returncode == 0, result.stderr[-2000:]
        with open(path, encoding="utf-8") as source:
            return json.load(source)


def measurements():
    return copy.deepcopy(collected())


def policy():
    return checker.load_policy(BUDGET)


def set_bytes(case, operation, payload, control=0):
    case["io"]["payload"][operation + "_bytes"] = payload
    case["io"]["control"][operation + "_bytes"] = control
    case["io"]["bytes"][operation] = payload + control
    case[operation + "_bytes"] = payload + control
    case["payload"] = copy.deepcopy(case["io"]["payload"])
    case["control"] = copy.deepcopy(case["io"]["control"])
    if operation == "get":
        case["verify_bytes"] = payload + control


def minimum_floor_scenario():
    """Bounded measured archive growth must not disappear under a $100 floor."""
    meas, base = measurements(), policy()
    base["monthly"]["backup_retention_months"] = 36
    for case in meas["current"]["cases"]:
        for key in ("new_remote_bytes", "store_bytes", "net_remote_bytes", "gen_bytes"):
            case[key] += 60000
        set_bytes(case, "put", case["io"]["payload"]["put_bytes"],
                  case["io"]["control"]["put_bytes"] + 60000)
    storage_gb = checker.monthly_cost(meas, base, "before")["storage_gb"]
    base["monthly"]["storage_usd_per_gb_month"] = 40 / storage_gb
    candidate = copy.deepcopy(base)
    candidate["monthly"]["storage_usd_per_gb_month"] *= 1.01
    candidate["monthly"]["minimum_monthly_storage_usd"] = 100
    return meas, base, candidate


class StorageCostBudgetTest(unittest.TestCase):
    def test_actual_collector_self_comparison_passes(self):
        meas, budget = measurements(), policy()
        result = checker.check_measurements(meas, budget, budget)
        assert result["status"] == "pass", result
        assert meas["baseline_available"] is True
        assert set(meas["current"]["auxiliary"]) == set(checker.AUXILIARY)
        assert result["monthly"]["coverage"].startswith("scoped estimate")
        assert result["monthly"]["request_cost_usd"] == 0
        assert sum(result["monthly"]["requests"].values()) > 0


    def test_backup_two_full_gets_project_9600_gib(self):
        meas, budget = measurements(), policy()
        for case in meas["current"]["cases"]:
            set_bytes(case, "get", case["live_bytes"] * 2)
        result = checker.check_measurements(meas, budget)
        assert result["monthly"]["egress_gib"] == 9600
        assert result["monthly"]["backup_get_bytes"] == 9600 * checker.GIB
        assert result["status"] == "fail"
        assert any("backup read ratio" in failure for failure in result["failures"])
        assert any("monthly egress" in failure for failure in result["failures"])
        assert any("monthly overage" in failure for failure in result["failures"])


    def test_backup_two_point_zero_one_gets_project_9648_gib(self):
        meas = measurements()
        for case in meas["current"]["cases"]:
            set_bytes(case, "get", case["live_bytes"] * 2.01)
        result = checker.check_measurements(meas, policy())
        assert math.isclose(result["monthly"]["egress_gib"], 9648, rel_tol=1e-12)
        assert result["status"] == "fail"


    def test_raw_full_get_projects_200_gib_from_unique_upload_bytes(self):
        meas = measurements()
        raw = meas["current"]["auxiliary"]["raw_fresh"]
        set_bytes(raw, "get", raw["input_bytes"])
        result = checker.check_measurements(meas, policy())
        assert result["monthly"]["raw_get_bytes"] == 200 * checker.GIB
        assert result["status"] == "fail"
        assert any("raw_fresh read ratio" in failure for failure in result["failures"])


    def test_can_bulk_get_is_checked_against_can_not_backup_input(self):
        for name in ("can_small", "can_big"):
            meas = measurements()
            case = meas["current"]["auxiliary"][name]
            set_bytes(case, "get", case["input_bytes"])
            result = checker.check_measurements(meas, policy())
            assert result["status"] == "fail"
            assert any(name + " read ratio" in failure for failure in result["failures"])


    def test_twenty_percent_below_absolute_recomputed_not_trusted(self):
        meas = measurements()
        old, new = meas["before"]["cases"][1], meas["current"]["cases"][1]
        set_bytes(old, "get", 100000)
        set_bytes(new, "get", 120000)
        assert new["get_bytes"] / new["live_bytes"] < 0.05
        # False comparison claims must not hide measured 20% growth.
        for scenario in meas["comparison"]["scenarios"]:
            scenario["get_bytes"] = -999999999
        result = checker.check_measurements(meas, policy())
        assert result["status"] == "fail"
        assert any("relative growth" in failure for failure in result["failures"])
        assert not any("read ratio exceeds" in failure for failure in result["failures"])


    def test_raw_replay_archive_put_cannot_hide_in_control_claims(self):
        meas = measurements()
        replay = meas["current"]["auxiliary"]["raw_replay"]
        replay["archive_puts"] = 1
        replay["archive_put_bytes"] = 1
        result = checker.check_measurements(meas, policy())
        assert result["status"] == "fail"
        assert any("RAW replay" in failure for failure in result["failures"])

    def test_negative_savings_are_valid_even_with_negative_comparison_deltas(self):
        meas = measurements()
        old = meas["before"]["cases"][1]
        set_bytes(old, "get", 100000)
        for scenario in meas["comparison"]["scenarios"]:
            scenario["get_bytes"] = -100000
        result = checker.check_measurements(meas, policy())
        assert result["status"] == "pass", result


    def test_total_cost_delta_at_least_one_dollar_fails_below_other_gates(self):
        meas, budget = measurements(), policy()
        # Bounded control/archive growth is below relative/noise gates, but
        # projected byte-month cost increases by more than one dollar.
        case = meas["current"]["cases"][2]
        for key in ("new_remote_bytes", "store_bytes", "net_remote_bytes", "gen_bytes"):
            case[key] += 10000
        set_bytes(case, "put", case["io"]["payload"]["put_bytes"], case["io"]["control"]["put_bytes"] + 10000)
        budget["monthly"]["storage_usd_per_gb_month"] = 1
        budget["monthly"]["max_monthly_total_usd"] = 100000
        result = checker.check_measurements(meas, budget, budget)
        assert result["monthly_cost_delta_usd"] >= 1
        assert result["monthly"]["cost_usd"] < budget["monthly"]["max_monthly_total_usd"]
        assert any("monthly cost increase" in failure for failure in result["failures"])
        assert not any("relative growth" in failure for failure in result["failures"])


    def test_identical_gate_counts_archive_and_control_not_only_sst(self):
        meas = measurements()
        case = meas["current"]["cases"][1]
        case["new_remote_bytes"] = case["live_bytes"] * 0.021
        case["new_payload_bytes"] = 0
        result = checker.check_measurements(meas, policy())
        assert any("identical gross retained ratio" in failure for failure in result["failures"])


    def test_first_seed_is_one_copy_plus_explicit_overhead_not_three_copies(self):
        meas = measurements()
        first = meas["current"]["cases"][0]
        first["new_remote_bytes"] = first["live_bytes"] * 2
        result = checker.check_measurements(meas, policy())
        assert any("first backup requires one full copy" in failure for failure in result["failures"])


    def test_bulk_baseline_zero_stays_zero_control_noise_is_separate(self):
        meas = measurements()
        case = meas["current"]["cases"][1]
        set_bytes(case, "get", 1, case["io"]["control"]["get_bytes"])
        result = checker.check_measurements(meas, policy())
        assert any("baseline-zero" in failure and "payload.get_bytes" in failure for failure in result["failures"])
        meas = measurements()
        case = meas["current"]["cases"][1]
        set_bytes(case, "get", 0, case["io"]["control"]["get_bytes"] + 1)
        result = checker.check_measurements(meas, policy())
        assert result["status"] == "pass", result


    def test_request_operation_migration_uses_total_per_input_object(self):
        meas = measurements()
        case = meas["current"]["cases"][1]
        count = case["io"]["requests"]["lists"]
        case["io"]["requests"]["lists"] = 0
        case["io"]["requests"]["heads"] += count
        case["lists"] = 0
        case["heads"] += count
        result = checker.check_measurements(meas, policy())
        assert result["status"] == "pass", result
        case["io"]["requests"]["heads"] *= 2
        case["heads"] = case["io"]["requests"]["heads"]
        extra = sum(case["io"]["requests"].values()) - case["io"]["payload"]["requests"] - case["io"]["control"]["requests"]
        case["io"]["control"]["requests"] += extra
        case["control"] = copy.deepcopy(case["io"]["control"])
        result = checker.check_measurements(meas, policy())
        assert any("requests/input_objects" in failure for failure in result["failures"])


    def test_storage_requests_and_fixed_allowance_use_exact_gb_conversion(self):
        meas, budget = measurements(), policy()
        monthly = checker.monthly_cost(meas, budget)
        assert monthly["allowance_gb"] == 1200 * checker.GIB * 3 / checker.GB
        assert monthly["storage_cost_usd"] == monthly["storage_byte_months"] / checker.GB * 0.006
        assert monthly["egress_gb"] == monthly["egress_bytes"] / checker.GB
        before_allowance = monthly["allowance_gb"]
        for case in meas["current"]["cases"]:
            case["new_remote_bytes"] *= 2
        after = checker.monthly_cost(meas, budget)
        assert after["allowance_gb"] == before_allowance
        assert after["storage_byte_months"] > monthly["storage_byte_months"]
        budget["monthly"]["storage_usd_per_gb_month"] = 0
        budget["monthly"]["minimum_monthly_storage_usd"] = 0
        budget["monthly"]["request_usd_per_1000"] = dict.fromkeys(checker.REQ_OPS, 0.001)
        after = checker.monthly_cost(meas, budget)
        assert after["storage_cost_usd"] == 0
        assert math.isclose(after["request_cost_usd"], sum(after["requests"].values()) / 1000 * 0.001)


    def test_missing_cases_raw_can_oracles_baseline_and_zero_inputs_fail(self):
        changes = [lambda m: m["current"]["cases"].pop(),
                   lambda m: m["before"]["cases"].pop(),
                   lambda m: m["current"]["auxiliary"].pop("raw_fresh"),
                   lambda m: m["before"]["auxiliary"].pop("can_big"),
                   lambda m: m["current"].pop("oracle"),
                   lambda m: m.pop("before"),
                   lambda m: m.pop("baseline_available"),
                   lambda m: m["comparison"].pop("role_hashes_match"),
                   lambda m: m["comparison"].pop("scenarios"),
                   lambda m: m["current"]["retention_enforced"].update(remaining=["newest"]),
                   lambda m: m["current"]["auxiliary"]["raw_fresh"].update(input_bytes=0),
                   lambda m: m["current"]["cases"][0].update(live_objects=0)]
        for change in changes:
            meas = measurements()
            change(meas)
            result = checker.check_measurements(meas, policy())
            assert result["status"] == "fail", result


    def test_nan_boolean_and_nested_io_inconsistency_fail(self):
        for value in (float("nan"), float("inf"), True, -1):
            meas = measurements()
            meas["current"]["auxiliary"]["raw_fresh"]["get_bytes"] = value
            assert checker.check_measurements(meas, policy())["status"] == "fail"
            budget = policy()
            budget["monthly"]["egress_usd_per_gb"] = value
            assert checker.validate_policy(budget) is not None
        meas = measurements()
        meas["current"]["cases"][1]["io"]["bytes"]["get"] += 1
        assert checker.check_measurements(meas, policy())["status"] == "fail"


    def test_missing_measurement_baseline_is_not_policy_bootstrap(self):
        meas = measurements()
        meas.pop("before")
        assert checker.check_measurements(meas, policy())["status"] == "fail"
        meas["baseline_available"] = False
        meas["baseline"] = "absent (--no-baseline)"
        meas.pop("comparison")
        result = checker.check_measurements(meas, policy())
        assert result["status"] == "pass", result
        assert "unavailable (not zero)" in result["baseline"]


    def test_policy_removal_relaxation_workload_allowance_prices_and_noise_fail(self):
        base = policy()
        assert checker.validate_policy(base) is None
        assert checker.compare_policy(base, base) == []
        for section, key, value in (("absolute", "backup_read_ratio_max", 0.9),
                                    ("baseline", "control_noise_allowance_bytes", 9999999),
                                    ("monthly", "storage_gib", 1),
                                    ("monthly", "raw_gib_per_month", 1),
                                    ("monthly", "free_egress_multiplier", 4),
                                    ("monthly", "allowance_storage_gib", 2400),
                                    ("monthly", "egress_usd_per_gb", 0),
                                    ("monthly", "storage_usd_per_gb_month", 0),
                                    ("monthly", "minimum_monthly_storage_usd", 0),
                                    ("greptime", "uncertainty_requests", 999999)):
            candidate = copy.deepcopy(base)
            candidate[section][key] = value
            assert checker.compare_policy(candidate, base)
        for section, keys in checker.RULES.items():
            for key in keys:
                candidate = copy.deepcopy(base)
                del candidate[section][key]
                assert checker.validate_policy(candidate) is not None
                assert checker.compare_policy(candidate, base)
        candidate = copy.deepcopy(base)
        candidate["monthly"]["request_usd_per_1000"]["new_operation"] = 0
        assert checker.validate_policy(candidate) is not None
        candidate = copy.deepcopy(base)
        candidate["monthly"]["backup_input_objects"] = 0
        assert checker.validate_policy(candidate) is not None
        priced_base = copy.deepcopy(base)
        priced_base["monthly"]["request_usd_per_1000"]["heads"] = 0.001
        assert checker.compare_policy(base, priced_base)
        candidate = copy.deepcopy(base)
        candidate["monthly"]["backup_case_runs"]["repeated-identical-2"] = 4
        candidate["monthly"]["backup_case_runs"]["small-change"] = 0
        candidate["monthly"]["backup_case_runs"]["large-change"] = 0
        assert checker.validate_policy(candidate) is None
        assert checker.compare_policy(candidate, base)


    def test_minimum_floor_cannot_change_in_either_direction(self):
        meas, base, candidate = minimum_floor_scenario()
        expected = ("policy relaxation monthly.minimum_monthly_storage_usd"
                    " must remain unchanged from base policy")
        assert checker.validate_policy(base) is None
        assert checker.validate_policy(candidate) is None
        assert checker.compare_policy(base, base) == []
        assert checker.check_measurements(measurements(), base, base)["status"] == "pass"
        priced = copy.deepcopy(candidate)
        priced["monthly"]["minimum_monthly_storage_usd"] = base["monthly"]["minimum_monthly_storage_usd"]
        assert checker.compare_policy(priced, base) == []
        assert math.isclose(checker.monthly_cost(meas, base, "before")["cost_usd"], 40)
        visible = checker.check_measurements(meas, priced, base)
        assert visible["monthly_cost_delta_usd"] >= 1
        assert visible["failures"] == ["monthly cost increase >= configured USD threshold"]
        for floor in (100, 0):
            with self.subTest(floor=floor):
                candidate["monthly"]["minimum_monthly_storage_usd"] = floor
                assert checker.compare_policy(candidate, base) == [expected]
                result = checker.check_measurements(meas, candidate, base)
                assert result["status"] == "fail", result
                assert expected in result["failures"]
                if floor == 100:
                    assert result["failures"] == [expected]
                    assert result["monthly"]["cost_usd"] == 100
                    assert result["baseline_monthly"]["cost_usd"] == 100
                    assert result["monthly_cost_delta_usd"] == 0
                    assert result["monthly"]["cost_usd"] == base["monthly"]["max_monthly_total_usd"]


    def test_cli_rejects_floor_masking_a_more_expensive_storage_price(self):
        meas, base, candidate = minimum_floor_scenario()
        with tempfile.TemporaryDirectory(prefix="budget-floor-cli-") as root:
            paths = {name: Path(root) / (name + ".json")
                     for name in ("measurements", "candidate", "base")}
            for name, value in (("measurements", meas), ("candidate", candidate), ("base", base)):
                paths[name].write_text(json.dumps(value))
            summary = Path(root) / "summary.md"
            result = subprocess.run([sys.executable, CHECK, "--measurements", str(paths["measurements"]),
                                     "--budget", str(paths["candidate"]),
                                     "--baseline-budget", str(paths["base"]), "--summary-out", str(summary)],
                                    capture_output=True, text=True, timeout=120)
            assert result.returncode == 1, result.stdout + result.stderr
            report = json.loads(result.stdout)
            assert report["status"] == "fail", report
            assert any("minimum_monthly_storage_usd must remain unchanged" in failure
                       for failure in report["failures"])
            assert report["monthly"]["cost_usd"] == 100
            assert report["baseline_monthly"]["cost_usd"] == 100
            assert report["monthly_cost_delta_usd"] == 0
            assert "Storage cost budget: FAIL" in summary.read_text()
            assert "minimum_monthly_storage_usd must remain unchanged" in summary.read_text()


    def test_unchanged_floor_preserves_negative_monthly_cost_savings(self):
        meas, base, _ = minimum_floor_scenario()
        meas["current"], meas["before"] = meas["before"], meas["current"]
        result = checker.check_measurements(meas, base, base)
        assert result["status"] == "pass", result
        assert result["monthly_cost_delta_usd"] < 0
        assert result["baseline_monthly"]["cost_usd"] > result["monthly"]["cost_usd"]


    def test_cli_emits_json_and_markdown_on_invalid_policy(self):
        with tempfile.TemporaryDirectory(prefix="budget-cli-test-") as root:
            tmp_path = Path(root)
            meas_path, budget_path, summary = (tmp_path / name for name in ("m.json", "b.json", "summary.md"))
            meas_path.write_text(json.dumps(measurements()))
            budget = policy()
            del budget["monthly"]["max_monthly_egress_gib"]
            budget_path.write_text(json.dumps(budget))
            result = subprocess.run([sys.executable, CHECK, "--measurements", str(meas_path),
                                     "--budget", str(budget_path), "--summary-out", str(summary)],
                                    capture_output=True, text=True, timeout=120)
            assert result.returncode == 1
            assert json.loads(result.stdout)["status"] == "fail"
            assert "Storage cost budget: FAIL" in summary.read_text()


if __name__ == "__main__":
    unittest.main()
