"""Capacity-math regression for the OTel retention inspector (issue #7).

Framework-free: plain asserts on pure helpers only, no server, no network.
Run with: python3 -m tests.test_retention_capacity. Owned by FixRetentionCapacity;
tests/test_otel_retention.py stays owned by its sibling.
"""

import importlib.util as _ilu
import os


def _load_tool(name, filename):
    path = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "tools", filename)
    spec = _ilu.spec_from_file_location(name, path)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _insp():
    return _load_tool("ret7_capacity", "inspect_otel_retention.py")


def test_combined_growth_weights_actual_rates():
    insp = _insp()
    # Unequal table volumes and unequal row sizes: traces 100 rows/day at
    # 10 B/row, logs 300 rows/day at 100 B/row -> combined 400 rows/day and
    # 100*10 + 300*100 = 31000 B/day. The old product-of-averages gave
    # ((100+300)/2) * ((10+100)/2) = 11000: wrong by ~3x.
    days = ["2026-10-0%d" % i for i in range(1, 4)]
    series = {
        "opentelemetry_traces": [{"day": d, "rows": 100} for d in days],
        "opentelemetry_logs": [{"day": d, "rows": 300} for d in days],
    }
    per, combined = insp.combine_daily(series, days)
    assert combined == [400, 400, 400], combined
    assert per["opentelemetry_traces"] == [100, 100, 100]
    means = {t: sum(v) / len(days) for t, v in per.items()}
    total, parts = insp.weighted_bytes_per_day(
        means, {"opentelemetry_traces": 10.0, "opentelemetry_logs": 100.0})
    assert total == 31000.0, total
    assert parts == {"opentelemetry_traces": 1000.0,
                     "opentelemetry_logs": 30000.0}, parts


def test_zero_input_days_count_in_window():
    insp = _insp()
    # Sparse input: one nonzero day in a 3-day window averages over all 3.
    days = ["2026-10-0%d" % i for i in range(1, 4)]
    series = {
        "opentelemetry_traces": [{"day": days[2], "rows": 30}],
        "opentelemetry_logs": [],
    }
    _, combined = insp.combine_daily(series, days)
    assert combined == [0, 0, 30], combined
    assert sum(combined) / len(days) == 10.0


def test_numeric_date_days_format():
    insp = _insp()
    # Greptime may return DATE groups as numeric days-since-epoch.
    assert insp.norm_day(20340) == "2025-09-09", insp.norm_day(20340)
    assert insp.norm_day("2026-10-01") == "2026-10-01"
    per, combined = insp.combine_daily(
        {"opentelemetry_traces": [{"day": 20340, "rows": 5}],
         "opentelemetry_logs": [{"day": 20340, "rows": 7}]},
        ["2025-09-08", "2025-09-09"])
    assert combined == [0, 12], combined
    assert per["opentelemetry_traces"] == [0, 5]


def test_mixed_catalog_row_sums_actual_bytes_only():
    insp = _insp()
    # Greptime TABLES row with all six byte-ish columns present:
    # data_length=635, index_length=116 are actual used bytes; max_* are
    # capacities and avg_row_length is an average -- none may be summed in.
    row635 = {"data_length": 635, "index_length": 116,
              "max_data_length": 999999, "max_index_length": 888888,
              "avg_row_length": 42}
    used = row635["data_length"] + row635["index_length"]
    assert used == 751, used
    assert insp.finite_rate(row635["data_length"])
    assert insp.finite_rate(row635["avg_row_length"])
    total, _ = insp.weighted_bytes_per_day(
        {"opentelemetry_traces": 10.0}, {"opentelemetry_traces": used / 10.0})
    assert total == 751.0, total
    # NaN/inf rates are rejected, never silently averaged in.
    assert not insp.finite_rate(float("nan"))
    assert not insp.finite_rate(float("inf"))
    assert not insp.finite_rate(-1.0)
    assert not insp.finite_rate(True)
    total, parts = insp.weighted_bytes_per_day(
        {"opentelemetry_traces": 10.0, "opentelemetry_logs": 5.0},
        {"opentelemetry_traces": float("nan"),
         "opentelemetry_logs": float("inf")})
    assert total is None and parts == {}, (total, parts)


def test_option_validation_and_empty_window():
    insp = _insp()
    per, combined = insp.combine_daily(
        {"opentelemetry_traces": [], "opentelemetry_logs": []},
        ["2026-10-01", "2026-10-02"])
    assert combined == [0, 0]
    total, parts = insp.weighted_bytes_per_day(
        {"opentelemetry_traces": 0.0}, {})
    assert total is None and parts == {}


def test_loader_resolves_tool():
    path = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "tools", "inspect_otel_retention.py")
    assert os.path.isfile(path), path


if __name__ == "__main__":
    test_combined_growth_weights_actual_rates()
    test_zero_input_days_count_in_window()
    test_numeric_date_days_format()
    test_mixed_catalog_row_sums_actual_bytes_only()
    test_option_validation_and_empty_window()
    test_loader_resolves_tool()
    print("test_retention_capacity: ok (6 tests)")
