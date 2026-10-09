#!/usr/bin/env python3
"""Focused stdlib tests for scripts.analytics.battery.battery_conditions. Plain asserts,
stdlib only; doubles as __main__ runner. All numbers are synthetic with
analytic expected answers, never Tesla health cutoffs. Known-hour
fixtures explicitly declare max_gap_ns (production gap limit is kept);
hour-spaced legs never rely on incidental defaults."""

import os
import sys

from scripts.analytics.battery import battery_common as bc
from scripts.analytics.battery import battery_conditions as co

HOUR = 3600000000000
T0 = 1700000000000000000
SCOPE = {"vehicle": "*", "source": "*", "decode_epoch": "*"}
# Production gap limit kept: hour fixtures declare 2h explicitly and
# never repin the incidental 10-minute default.
GAP2H = {"max_gap_ns": 2 * HOUR}


def sig(ts, field="Soc", num=1.0, unit="%", quality=None, vehicle="v",
        source="fleet", epoch="e1", ingest=None):
    return {"event_time_ns": ts, "ingest_time_ns": ingest, "vehicle": vehicle,
            "source": source, "decode_epoch": epoch, "path": "P",
            "source_field": field, "value_num": num, "value_text": None,
            "value_bool": None, "unit": unit, "quality": quality,
            "envelope_id": None, "config_version": None,
            "connectivity": None}


def raw(ts, field, num, vehicle="v"):
    return sig(ts, field=field, num=num, unit=None,
               quality="unit_unverified", vehicle=vehicle)


def cond(ts, field, num, unit, vehicle="v"):
    return sig(ts, field=field, num=num, unit=unit, quality=None,
               vehicle=vehicle)


def cfg(**kw):
    return {"conditions": dict(kw)}


def by(rows, metric):
    got = [r for r in rows if r["metric"] == metric]
    assert len(got) == 1, (metric, len(got))
    return got[0]


def pack_current(t, amps, vehicle="v"):
    return raw(t, "PackCurrent", amps, vehicle)


def batt_max(t, raw_temp, vehicle="v"):
    return raw(t, "ModuleTempMax", raw_temp, vehicle)


def batt_min(t, raw_temp, vehicle="v"):
    return raw(t, "ModuleTempMin", raw_temp, vehicle)


def brick_pair(t, mx, mn, max_id=7, min_id=3, soc=50.0, batt_c=None,
               curr_amps=None, vehicle="v"):
    rows = [raw(t, "BrickVoltageMax", mx, vehicle),
            raw(t, "BrickVoltageMin", mn, vehicle),
            raw(t, "NumBrickVoltageMax", max_id, vehicle),
            raw(t, "NumBrickVoltageMin", min_id, vehicle)]
    if soc is not None:
        rows.append(cond(t, "Soc", soc, "%", vehicle))
    if batt_c is not None:
        # Battery temp arrives raw (unit NULL); scale-1 tests pass
        # calibrated C straight through.
        rows.append(batt_max(t, batt_c, vehicle))
        rows.append(batt_min(t, batt_c, vehicle))
    if curr_amps is not None:
        rows.append(pack_current(t, curr_amps, vehicle))
    return rows


def therm_pair(t, mx, mn, max_id=7, min_id=3, vehicle="v"):
    return [raw(t, "ModuleTempMax", mx, vehicle),
            raw(t, "ModuleTempMin", mn, vehicle),
            raw(t, "NumModuleTempMax", max_id, vehicle),
            raw(t, "NumModuleTempMin", min_id, vehicle)]


def cal(unit, scale, offset=0.0, domain="synthetic", sign=None):
    out = {"version": "synth-1", "domain": domain, "scope": dict(SCOPE),
           "unit": unit, "scale": scale, "offset": offset}
    if sign is not None:
        out["current_sign"] = sign
    return out


def mod_cal(scale=1.0, offset=0.0, domain="synthetic"):
    return cal("celsius", scale, offset, domain)


def pack_cal(scale=1.0, sign=1, domain="synthetic"):
    return cal("A", scale, 0.0, domain, sign)


def test_median_mad_known_answer():
    median, mad = co.median_mad([0.10, 0.12, 0.11, 0.13, 0.09])
    assert abs(median - 0.11) < 1e-12
    assert abs(mad - 0.01) < 1e-12
    assert co.median_mad([]) == (None, None)
    assert co.median_mad([1.0, float("nan")]) == (None, None)
    median, mad = co.median_mad([1.0, 3.0])
    assert abs(median - 2.0) < 1e-12 and abs(mad - 1.0) < 1e-12


def test_calibrated_zero_spreads_remain_observed_zero():
    rows = brick_pair(T0, 4.0, 4.0) + therm_pair(T0, 25.0, 25.0)
    out = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 1.0),
        module_temp_calibration=mod_cal()))
    for metric in ("battery.conditions.brick_spread_v",
                   "battery.conditions.thermal_spread_c"):
        result = by(out, metric)
        assert result["status"] == "derived"
        assert result["value"] == 0.0


def test_spread_sync_known_answer_raw_and_calibrated():
    rows = brick_pair(T0, 4.2, 4.0)
    brick = cal("V", 2.0)
    cfgd = cfg(brick_voltage_calibration=brick,
               module_temp_calibration=mod_cal(),
               pack_current_calibration=pack_cal())
    out = co.analyze(rows, [], cfgd)
    assert abs(by(out, "battery.conditions.brick_spread_raw")["value"]
               - 0.2) < 1e-9
    assert abs(by(out, "battery.conditions.brick_spread_v")["value"]
               - 0.4) < 1e-9
    assert by(out, "battery.conditions.brick_spread_v")["unit"] == "V"
    assert by(out, "battery.conditions.brick_spread_v")[
        "calibration_version"] == "synth-1"
    # Spreads are differences: offset must not shift them.
    out2 = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 2.0, offset=100.0),
        module_temp_calibration=mod_cal(),
        pack_current_calibration=pack_cal()))
    assert abs(by(out2, "battery.conditions.brick_spread_v")["value"]
               - 0.4) < 1e-9


def test_sync_spread_series_skew_boundary():
    got = co.sync_spread_series([(100, 4.2)], [(50, 4.0)], 50)
    assert len(got) == 1 and abs(got[0][1] - 0.2) < 1e-12
    assert co.sync_spread_series([(100, 4.2)], [(49, 4.0)], 50) == []
    try:
        co.sync_spread_series([(100, 4.2)], [(90, 4.0)], -1)
    except co.ConditionsError:
        pass
    else:
        raise AssertionError("negative skew must raise")


def test_thermal_slope_same_id_known_answer():
    rows = []
    for i, spread in enumerate((2.0, 3.0, 4.0)):
        rows += therm_pair(T0 + i * HOUR, 20.0 + spread, 20.0)
    out = co.analyze(rows, [], cfg(module_temp_calibration=mod_cal(),
                                   **GAP2H))
    slope = by(out, "battery.conditions.thermal_slope_raw_per_h")
    assert slope["status"] == "derived"
    assert abs(slope["value"] - 1.0) < 1e-12
    assert slope["unit"] is None
    # Calibrated slope scales without offset.
    out2 = co.analyze(rows, [], cfg(
        module_temp_calibration=cal("celsius", 2.0, offset=50.0), **GAP2H))
    cal_slope = by(out2, "battery.conditions.thermal_slope_c_per_h")
    assert abs(cal_slope["value"] - 2.0) < 1e-12
    assert cal_slope["unit"] == "celsius/h"


def test_slope_gap_boundary_inclusive_then_exceeded():
    rows = therm_pair(T0, 22.0, 20.0) + therm_pair(T0 + HOUR, 23.0, 20.0)
    out = co.analyze(rows, [], cfg(max_gap_ns=HOUR,
                                   module_temp_calibration=mod_cal()))
    assert out and by(out, "battery.conditions.thermal_slope_raw_per_h")[
        "status"] == "derived"
    out2 = co.analyze(rows, [], cfg(max_gap_ns=HOUR - 1,
                                    module_temp_calibration=mod_cal()))
    bad = by(out2, "battery.conditions.thermal_slope_raw_per_h")
    assert bad["status"] == "unavailable" and "gap_exceeded" in bad["reason"]


def test_slope_stops_on_id_switch():
    rows = therm_pair(T0, 22.0, 20.0, max_id=7, min_id=3)
    rows += therm_pair(T0 + HOUR, 23.0, 20.0, max_id=7, min_id=3)
    rows += therm_pair(T0 + 2 * HOUR, 24.0, 20.0, max_id=8, min_id=3)
    out = co.analyze(rows, [], cfg(module_temp_calibration=mod_cal(),
                                   **GAP2H))
    bad = by(out, "battery.conditions.thermal_slope_raw_per_h")
    assert bad["status"] == "unavailable" and "id_changed" in bad["reason"]


def test_slope_barrier_invalid_timestamp_stops_interpolation():
    rows = therm_pair(T0, 22.0, 20.0) + therm_pair(T0 + 2 * HOUR, 24.0, 20.0)
    rows.append(sig(T0 + HOUR, field="ModuleTempMax", num=None,
                    unit=None, quality="invalid"))
    out = co.analyze(rows, [], cfg(module_temp_calibration=mod_cal(),
                                   max_gap_ns=3 * HOUR))
    bad = by(out, "battery.conditions.thermal_slope_raw_per_h")
    assert bad["status"] == "unavailable" and "gap_exceeded" in bad["reason"]


def test_short_same_id_run_is_not_extrapolated_to_hourly_slope():
    # Production case: 0.5 spread change over 30 s must not become 60/h.
    rows = therm_pair(T0, 35.0, 30.0) + therm_pair(T0 + 5 * 10**9, 35.0, 30.0)
    rows += therm_pair(T0 + 30 * 10**9, 34.5, 30.0)
    out = co.analyze(rows, [], cfg(module_temp_calibration=mod_cal()))
    bad = by(out, "battery.conditions.thermal_slope_raw_per_h")
    assert bad["status"] == "unavailable" and bad["value"] is None
    assert "min_span" in bad["reason"]
    assert by(out, "battery.conditions.thermal_slope_c_per_h")["value"] is None
    # The span boundary is inclusive; an isolation run obeys the same rule.
    span = 10 * 60 * 10**9
    ok = therm_pair(T0, 22.0, 20.0) + therm_pair(T0 + span, 23.0, 20.0)
    got = by(co.analyze(ok, [], cfg(min_slope_span_ns=span)),
             "battery.conditions.thermal_slope_raw_per_h")
    assert got["status"] == "derived" and abs(got["value"] - 6.0) < 1e-9
    iso = [raw(T0, "IsolationResistance", 1000.0),
           raw(T0 + 60 * 10**9, "IsolationResistance", 900.0)]
    trend = by(co.analyze(iso, [], cfg(isolation_calibration=cal("ohm", 1.0))),
               "battery.conditions.isolation_trend_ohm_per_h")
    assert trend["value"] is None and "min_span" in trend["reason"]


def test_conditioned_baseline_known_answer():
    spreads = [(0.10, 50.0, 20.0, 5.0), (0.12, 50.0, 20.0, 5.0),
               (0.11, 50.0, 20.0, 5.0), (0.20, 50.0, 20.0, 5.0)]
    res = co.conditioned_baseline(spreads, 3, 5.0, 5.0, 10.0, 3)
    assert res["reason"] is None
    assert abs(res["baseline"] - 0.11) < 1e-12
    assert abs(res["mad"] - 0.01) < 1e-12
    assert abs(res["residual"] - 0.09) < 1e-12
    assert res["peers"] == 3


def test_conditioned_baseline_end_to_end_battery_temp_and_pack_current():
    rows = []
    for i, mx in enumerate((4.10, 4.12, 4.11, 4.20)):
        rows += brick_pair(T0 + i * HOUR, mx, 4.00, batt_c=20.0,
                           curr_amps=5.0)
    out = co.analyze(rows, [], cfg(
        min_peers=3, module_temp_calibration=mod_cal(),
        pack_current_calibration=pack_cal(), **GAP2H))
    base = by(out, "battery.conditions.spread_baseline_raw")
    resid = by(out, "battery.conditions.spread_residual_raw")
    assert base["status"] == "estimated"
    assert abs(base["value"] - 0.11) < 1e-9
    assert abs(base["uncertainty"] - 0.01) < 1e-9
    assert abs(resid["value"] - 0.09) < 1e-9


def test_baseline_needs_calibrated_battery_conditions():
    # Without pack/module calibrations the brick spreads have no
    # temp/current join, so the baseline cannot condition.
    rows = []
    for i, mx in enumerate((4.10, 4.12, 4.11, 4.20)):
        rows += brick_pair(T0 + i * HOUR, mx, 4.00, batt_c=20.0,
                           curr_amps=5.0)
    out = co.analyze(rows, [], cfg(min_peers=3, **GAP2H))
    bad = by(out, "battery.conditions.spread_baseline_raw")
    assert bad["status"] == "unavailable"
    assert "condition_unavailable" in bad["reason"]


def test_condition_mismatch_insufficient_peers():
    spreads = [(0.10, 10.0, 20.0, 5.0), (0.12, 11.0, 20.0, 5.0),
               (0.11, 12.0, 20.0, 5.0), (0.20, 90.0, 20.0, 5.0)]
    res = co.conditioned_baseline(spreads, 3, 5.0, 5.0, 10.0, 3)
    assert res["reason"] == "insufficient_peers" and res["peers"] == 0
    rows = []
    for i, soc in enumerate((10.0, 11.0, 12.0, 90.0)):
        rows += brick_pair(T0 + i * HOUR, 4.10 + i * 0.01, 4.00, batt_c=20.0,
                           curr_amps=5.0, soc=soc)
    out = co.analyze(rows, [], cfg(
        min_peers=3, module_temp_calibration=mod_cal(),
        pack_current_calibration=pack_cal(), **GAP2H))
    bad = by(out, "battery.conditions.spread_baseline_raw")
    assert bad["status"] == "unavailable"
    assert "insufficient_peers" in bad["reason"]


def test_zero_mad_is_unavailable_not_zero():
    spreads = [(0.10, 50.0, 20.0, 5.0), (0.10, 50.0, 20.0, 5.0),
               (0.10, 50.0, 20.0, 5.0), (0.20, 50.0, 20.0, 5.0)]
    res = co.conditioned_baseline(spreads, 3, 5.0, 5.0, 10.0, 3)
    assert res["reason"] == "zero_mad" and res["mad"] == 0.0
    assert res["baseline"] is None and res["residual"] is None


def test_condition_unavailable_when_latest_lacks_soc():
    spreads = [(0.10, 50.0, 20.0, 5.0), (0.12, 50.0, 20.0, 5.0),
               (0.11, 50.0, 20.0, 5.0), (0.20, None, 20.0, 5.0)]
    res = co.conditioned_baseline(spreads, 3, 5.0, 5.0, 10.0, 3)
    assert res["reason"] == "condition_unavailable"


def test_exposure_known_answer_both_endpoints():
    res = co.exposure_s([(T0, 90.0), (T0 + HOUR, 90.0),
                         (T0 + 2 * HOUR, 90.0)], lambda v: v >= 80.0,
                        2 * HOUR)
    assert abs(res["exposure_s"] - 7200.0) < 1e-9
    assert abs(res["valid_covered_s"] - 7200.0) < 1e-9
    assert abs(res["coverage_ratio"] - 1.0) < 1e-12
    # One endpoint below threshold: leg is covered but not exposure.
    # Observation fractions would still claim 2/3; valid time says 0.
    res2 = co.exposure_s([(T0, 90.0), (T0 + HOUR, 70.0),
                          (T0 + 2 * HOUR, 90.0)], lambda v: v >= 80.0,
                         2 * HOUR)
    assert res2["exposure_s"] == 0.0
    assert abs(res2["valid_covered_s"] - 7200.0) < 1e-9


def test_exposure_high_soc_end_to_end():
    rows = [cond(T0, "Soc", 90.0, "%"),
            cond(T0 + 1800000000000, "Soc", 92.0, "%"),
            cond(T0 + 3600000000000, "Soc", 91.0, "%")]
    out = co.analyze(rows, [], cfg(soc_high_pct=80.0, max_gap_ns=HOUR))
    got = by(out, "battery.conditions.exposure_high_soc_s")
    assert got["status"] == "derived" and got["unit"] == "s"
    assert abs(got["value"] - 3600.0) < 1e-9
    assert abs(got["coverage_ratio"] - 1.0) < 1e-12
    low = by(out, "battery.conditions.exposure_low_soc_s")
    assert low["status"] == "unavailable"
    assert "missing_threshold" in low["reason"]


def test_exposure_gap_rejected_never_interpolated():
    rows = [cond(T0, "Soc", 90.0, "%"),
            cond(T0 + HOUR, "Soc", 90.0, "%"),
            cond(T0 + 5 * HOUR, "Soc", 90.0, "%")]
    out = co.analyze(rows, [], cfg(soc_high_pct=80.0, max_gap_ns=HOUR))
    got = by(out, "battery.conditions.exposure_high_soc_s")
    assert abs(got["value"] - 3600.0) < 1e-9
    assert got["coverage_ratio"] is not None
    assert got["coverage_ratio"] < 1.0


def test_invalid_sample_is_barrier_not_skip():
    rows = [cond(T0, "Soc", 90.0, "%"),
            sig(T0 + HOUR, field="Soc", num=None, unit="%",
                quality="invalid"),
            cond(T0 + 2 * HOUR, "Soc", 90.0, "%")]
    out = co.analyze(rows, [], cfg(soc_high_pct=80.0, **GAP2H))
    got = by(out, "battery.conditions.exposure_high_soc_s")
    assert got["status"] == "unavailable"
    assert "gap_exceeded" in got["reason"]


def test_ambiguous_timestamp_no_spread():
    rows = [raw(T0, "BrickVoltageMax", 4.2), raw(T0, "BrickVoltageMax", 4.3),
            raw(T0, "BrickVoltageMin", 4.0)]
    out = co.analyze(rows, [], cfg())
    bad = by(out, "battery.conditions.brick_spread_raw")
    assert bad["status"] == "unavailable"
    assert bad["value"] is None
    rawmax = by(out, "battery.conditions.brick_max_raw")
    assert rawmax["status"] == "unavailable"


def test_id_stats_switch_recurrence_persistence():
    stats = co.id_stats([(T0, 1.0), (T0 + HOUR, 2.0),
                         (T0 + 2 * HOUR, 1.0)], 2 * HOUR)
    assert stats["switches"] == 2
    assert stats["recurrence"] == 1
    assert stats["persistence_s"] == 0.0
    assert stats["last_id"] == 1.0
    stats2 = co.id_stats([(T0, 5.0), (T0 + HOUR, 5.0),
                          (T0 + 2 * HOUR, 5.0)], 2 * HOUR)
    assert stats2["switches"] == 0 and stats2["recurrence"] == 0
    assert abs(stats2["persistence_s"] - 7200.0) < 1e-9
    # A barrier inside the trailing run stops persistence there.
    stats3 = co.id_stats([(T0, 5.0), (T0 + HOUR, 5.0),
                          (T0 + 2 * HOUR, 5.0)], 3 * HOUR,
                         barriers=[T0 + HOUR // 2])
    assert abs(stats3["persistence_s"] - 3600.0) < 1e-9


def test_id_switch_recurrence_end_to_end():
    rows = []
    for i, ident in enumerate((1, 2, 1)):
        rows += brick_pair(T0 + i * HOUR, 4.10, 4.00, max_id=ident)
    out = co.analyze(rows, [], cfg(**GAP2H))
    assert by(out, "battery.conditions.brick_max_id_switches")["value"] == 2.0
    assert by(out, "battery.conditions.brick_max_id_recurrence")[
        "value"] == 1.0
    assert by(out, "battery.conditions.brick_max_id_persistence_s")[
        "value"] == 0.0


def test_scope_isolation_no_cross_contamination():
    rows = brick_pair(T0, 4.20, 4.00, vehicle="v1")
    rows += brick_pair(T0, 4.50, 4.00, vehicle="v2")
    out = co.analyze(rows, [], cfg())
    vals = {}
    for row in out:
        if row["metric"] == "battery.conditions.brick_spread_raw":
            vals[row["vehicle"]] = row["value"]
    assert abs(vals["v1"] - 0.2) < 1e-9
    assert abs(vals["v2"] - 0.5) < 1e-9


def test_unknown_units_raw_preserved_physical_unavailable():
    rows = brick_pair(T0, 4.2, 4.0)
    out = co.analyze(rows, [], cfg())
    rawmax = by(out, "battery.conditions.brick_max_raw")
    assert rawmax["status"] == "reported" and rawmax["unit"] is None
    assert abs(rawmax["value"] - 4.2) < 1e-12
    assert by(out, "battery.conditions.brick_spread_v")[
        "status"] == "unavailable"
    assert "missing_calibration" in by(
        out, "battery.conditions.brick_spread_v")["reason"]


def test_batterylevel_never_drives_soc_exposure():
    rows = [cond(T0, "BatteryLevel", 95.0, "%"),
            cond(T0 + HOUR, "BatteryLevel", 96.0, "%")]
    out = co.analyze(rows, [], cfg(soc_high_pct=80.0, **GAP2H))
    got = by(out, "battery.conditions.exposure_high_soc_s")
    assert got["status"] == "unavailable"


def test_pack_current_drives_exposure_with_calibration():
    # Official Fleet load path: uncalibrated PackCurrent stays
    # unavailable, never silently substituted.
    rows = [pack_current(T0, 500.0), pack_current(T0 + HOUR, 500.0)]
    out = co.analyze(rows, [], cfg(current_high_a=100.0, **GAP2H))
    got = by(out, "battery.conditions.exposure_high_current_s")
    assert got["status"] == "unavailable"
    assert "missing_calibration" in got["reason"]
    # With an explicit scope-bound pack calibration the same rows map
    # to 500 A and the full hour is exposure.
    rows2 = [pack_current(T0, 0.5), pack_current(T0 + HOUR, 0.5)]
    out2 = co.analyze(rows2, [], cfg(
        current_high_a=100.0,
        pack_current_calibration=cal("A", 1000.0, sign=1), **GAP2H))
    got2 = by(out2, "battery.conditions.exposure_high_current_s")
    assert got2["status"] == "derived" and got2["unit"] == "s"
    assert abs(got2["value"] - 3600.0) < 1e-9
    assert "source=pack" in got2["reason"]


def test_pack_current_sign_is_explicit():
    bad = cal("A", 1000.0)  # no current_sign
    out = co.analyze([pack_current(T0, 0.5)], [], cfg(
        current_high_a=100.0, pack_current_calibration=bad, **GAP2H))
    err = by(out, "battery.conditions.exposure_high_current_s")
    assert err["status"] == "error" and "current_sign" in err["reason"]
    # Negative sign flips polarity: same raw maps to discharge.
    rows = [pack_current(T0, -0.5), pack_current(T0 + HOUR, -0.5)]
    out2 = co.analyze(rows, [], cfg(
        current_high_a=100.0,
        pack_current_calibration=cal("A", 1000.0, sign=-1), **GAP2H))
    assert abs(by(out2, "battery.conditions.exposure_high_current_s")[
        "value"] - 3600.0) < 1e-9


def test_explicit_current_fields_mapping_only():
    # Non-Fleet alternatives need an explicit existing mapping entry;
    # the default current_fields is PackCurrent-only, so a bare
    # BatteryCurrent row never drives current exposure silently.
    rows = [cond(T0, "BatteryCurrent", 500.0, "A"),
            cond(T0 + HOUR, "BatteryCurrent", 500.0, "A")]
    out = co.analyze(rows, [], cfg(current_high_a=100.0, **GAP2H))
    got = by(out, "battery.conditions.exposure_high_current_s")
    assert got["status"] == "unavailable"
    out2 = co.analyze(rows, [], cfg(current_high_a=100.0,
                                    current_fields=["BatteryCurrent"],
                                    **GAP2H))
    got2 = by(out2, "battery.conditions.exposure_high_current_s")
    assert got2["status"] == "derived"
    assert "source=BatteryCurrent" in got2["reason"]


def test_outside_temp_never_drives_battery_thermal():
    # OutsideTemp is ambient context only: it must not drive battery
    # temp exposure even when within range. Within-limit timestamps
    # with an explicit gap keep the failure reason honest.
    rows = [cond(T0, "OutsideTemp", 60.0, "celsius"),
            cond(T0 + HOUR, "OutsideTemp", 60.0, "celsius")]
    out = co.analyze(rows, [], cfg(temp_high_c=40.0,
                                   module_temp_calibration=mod_cal(),
                                   **GAP2H))
    got = by(out, "battery.conditions.exposure_high_temp_s")
    assert got["status"] == "unavailable"
    assert "sparse" in got["reason"]
    # Calibrated battery Max drives the metric: raw 40 -> 40 C over a
    # 35 C threshold for the full hour.
    rows2 = [batt_max(T0, 40.0), batt_max(T0 + HOUR, 40.0)]
    out2 = co.analyze(rows2, [], cfg(temp_high_c=35.0,
                                     module_temp_calibration=mod_cal(),
                                     **GAP2H))
    got2 = by(out2, "battery.conditions.exposure_high_temp_s")
    assert got2["status"] == "derived"
    assert abs(got2["value"] - 3600.0) < 1e-9


def test_low_temp_uses_module_min():
    rows = [batt_min(T0, 0.0), batt_min(T0 + HOUR, 0.0),
            batt_max(T0, 40.0), batt_max(T0 + HOUR, 40.0)]
    out = co.analyze(rows, [], cfg(temp_low_c=5.0,
                                   module_temp_calibration=mod_cal(),
                                   **GAP2H))
    assert abs(by(out, "battery.conditions.exposure_low_temp_s")[
        "value"] - 3600.0) < 1e-9


def test_wrong_unit_condition_is_barrier():
    rows = [sig(T0, field="Soc", num=90.0, unit="V", quality=None),
            sig(T0 + HOUR, field="Soc", num=90.0, unit="V", quality=None)]
    out = co.analyze(rows, [], cfg(soc_high_pct=80.0, **GAP2H))
    got = by(out, "battery.conditions.exposure_high_soc_s")
    assert got["status"] == "unavailable"


def test_malformed_calibration_is_error_raw_survives():
    rows = brick_pair(T0, 4.2, 4.0)
    bad_cal = {"version": "v", "domain": "d",
               "scope": dict(SCOPE), "unit": "V", "scale": 0.0}
    out = co.analyze(rows, [], cfg(brick_voltage_calibration=bad_cal))
    err = by(out, "battery.conditions.brick_spread_v")
    assert err["status"] == "error" and "scale" in err["reason"]
    assert by(out, "battery.conditions.brick_spread_raw")[
        "status"] == "derived"


def test_wrong_unit_calibration_is_error():
    rows = brick_pair(T0, 4.2, 4.0)
    bad = cal("kWh", 1.0)  # brick voltage must be V
    out = co.analyze(rows, [], cfg(brick_voltage_calibration=bad))
    err = by(out, "battery.conditions.brick_spread_v")
    assert err["status"] == "error" and "unit" in err["reason"]


def test_scope_mismatch_rejects_calibration_loudly():
    rows = brick_pair(T0, 4.2, 4.0)
    scoped = cal("V", 1.0)
    scoped["scope"] = {"vehicle": "other", "source": "*",
                       "decode_epoch": "*"}
    out = co.analyze(rows, [], cfg(brick_voltage_calibration=scoped))
    got = by(out, "battery.conditions.brick_spread_v")
    assert got["status"] == "unavailable"
    assert "scope_mismatch" in got["reason"]
    assert by(out, "battery.conditions.brick_spread_raw")[
        "status"] == "derived"


def test_domain_mismatch_rejects_calibration_loudly():
    rows = brick_pair(T0, 4.2, 4.0)
    out = co.analyze(rows, [], cfg(
        domain="pack-A",
        brick_voltage_calibration=cal("V", 1.0, domain="pack-B")))
    got = by(out, "battery.conditions.brick_spread_v")
    assert got["status"] == "unavailable"
    assert "domain_mismatch" in got["reason"]


def test_malformed_threshold_and_params_are_error():
    rows = brick_pair(T0, 4.2, 4.0)
    out = co.analyze(rows, [], cfg(soc_high_pct=120.0))
    err = by(out, "battery.conditions.exposure_high_soc_s")
    assert err["status"] == "error"
    out2 = co.analyze(rows, [], cfg(max_gap_ns="hour"))
    assert all(r["status"] == "error" for r in out2)


def test_malformed_current_fields_is_error():
    rows = [pack_current(T0, 0.5), pack_current(T0 + HOUR, 0.5)]
    out = co.analyze(rows, [], cfg(current_high_a=100.0,
                                   current_fields=[],
                                   pack_current_calibration=pack_cal(),
                                   **GAP2H))
    assert by(out, "battery.conditions.exposure_high_current_s")[
        "status"] == "error"


def test_decision_time_excludes_future_ingest():
    rows = [cond(T0, "Soc", 90.0, "%"),
            cond(T0 + HOUR, "Soc", 90.0, "%")]
    out = co.analyze(rows, [], {"conditions": {"soc_high_pct": 80.0,
                                               **GAP2H},
                                "decision_time_ns": T0})
    got = by(out, "battery.conditions.exposure_high_soc_s")
    assert got["status"] == "unavailable"


def test_reorder_and_dedup_stable():
    rows = brick_pair(T0, 4.2, 4.0) + brick_pair(T0 + HOUR, 4.3, 4.0)
    fwd = co.analyze(list(rows), [], cfg(**GAP2H))
    back = co.analyze(list(reversed(rows)), [], cfg(**GAP2H))
    assert by(fwd, "battery.conditions.brick_spread_raw")["value"] == \
        by(back, "battery.conditions.brick_spread_raw")["value"]
    dup = co.analyze(rows + list(rows), [], cfg(**GAP2H))
    assert by(dup, "battery.conditions.brick_spread_raw")["value"] == \
        by(fwd, "battery.conditions.brick_spread_raw")["value"]


def test_diagnostics_reports_quality_counts():
    rows = brick_pair(T0, 4.2, 4.0)
    rows.append(sig(T0 + 1, field="Soc", num=None, unit="%",
                    quality="invalid"))
    rows.append(raw(T0, "BrickVoltageMax", 4.2))
    out = co.analyze(rows, [], cfg(**GAP2H))
    diag = by(out, "battery.conditions.diagnostics")
    assert diag["status"] == "derived"
    assert diag["value"] is None
    assert "valid=" in diag["value_text"] and "deduped=" in diag["value_text"]


def test_empty_input_scopeless_unavailable():
    out = co.analyze([], [], cfg())
    assert len(out) == len(co.SUPPORTED_METRICS)
    assert all(r["status"] == "unavailable" for r in out)
    assert all(r["vehicle"] is None for r in out)


def test_isolation_raw_vs_calibrated_trend():
    t1, t2 = T0, T0 + HOUR
    rows = [raw(t1, "IsolationResistance", 1000.0),
            raw(t2, "IsolationResistance", 1100.0)]
    iso = cal("ohm", 2.0, offset=100.0)
    out = co.analyze(rows, [], cfg())
    assert by(out, "battery.conditions.isolation_raw")["value"] == 1100.0
    assert by(out, "battery.conditions.isolation_raw")["unit"] is None
    assert by(out, "battery.conditions.isolation_ohm")[
        "status"] == "unavailable"
    out2 = co.analyze(rows, [], cfg(isolation_calibration=iso, **GAP2H))
    assert by(out2, "battery.conditions.isolation_ohm")["value"] == 2300.0
    trend = by(out2, "battery.conditions.isolation_trend_ohm_per_h")
    assert abs(trend["value"] - 200.0) < 1e-9
    assert trend["unit"] == "ohm/h"


def test_run_analyses_contract():
    rows = brick_pair(T0, 4.2, 4.0)
    out = bc.run_analyses(rows, [], cfg(), [co.analyze])
    assert out and all(isinstance(r, dict) for r in out)
    assert all(r.get("analysis_id") == "battery_conditions" for r in out)


def test_terminal_tombstone_no_resurrection():
    # Valid BrickVoltageMin at 6:00 then invalid at 6:00:01 with an
    # extended window: latest raw/ID/spread AND calibrated spread must
    # report unavailable, never the older 6:00 values as current.
    t0, t1 = T0, T0 + 1000000000
    rows = [raw(t0, "BrickVoltageMax", 3.72),
            raw(t0, "BrickVoltageMin", 3.6999),
            raw(t0, "NumBrickVoltageMax", 7),
            raw(t0, "NumBrickVoltageMin", 2),
            raw(t1, "BrickVoltageMax", 3.72),
            sig(t1, field="BrickVoltageMin", num=None, unit=None,
                quality="invalid"),
            raw(t1, "NumBrickVoltageMax", 7),
            raw(t1, "NumBrickVoltageMin", 2)]
    out = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 1.0), **GAP2H))
    assert "terminal_invalid" in by(
        out, "battery.conditions.brick_min_raw")["reason"]
    assert by(out, "battery.conditions.brick_min_raw")["value"] is None
    assert "terminal_invalid" in by(
        out, "battery.conditions.brick_spread_raw")["reason"]
    assert "terminal_invalid" in by(
        out, "battery.conditions.brick_spread_v")["reason"]
    assert by(out, "battery.conditions.brick_spread_v")["value"] is None
    assert "terminal_invalid" in by(
        out, "battery.conditions.spread_baseline_raw")["reason"]
    # Historical trend legs still use bounded valid segments: the
    # pre-tombstone valid point keeps evidence but is not current.
    assert by(out, "battery.conditions.brick_max_raw")["value"] == 3.72


def test_terminal_min_tombstone_no_resurrection():
    # Mirror: valid BrickVoltageMax at t0 then invalid at t1. Min-side
    # terminal state must also block spreads (not just max-side).
    t0, t1 = T0, T0 + 1000000000
    rows = [raw(t0, "BrickVoltageMax", 3.72),
            raw(t0, "BrickVoltageMin", 3.6999),
            sig(t1, field="BrickVoltageMax", num=None, unit=None,
                quality="invalid"),
            raw(t1, "BrickVoltageMin", 3.6999)]
    out = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 1.0), **GAP2H))
    assert "terminal_invalid" in by(
        out, "battery.conditions.brick_max_raw")["reason"]
    assert "terminal_invalid" in by(
        out, "battery.conditions.brick_spread_raw")["reason"]
    assert "terminal_invalid" in by(
        out, "battery.conditions.brick_spread_v")["reason"]


def test_terminal_conflict_no_resurrection():
    # Same-time conflicting BrickVoltageMin after a valid sample: the
    # latest group disagrees, so latest raw + spreads go unavailable
    # instead of either conflicting value or the older valid one.
    rows = [raw(T0, "BrickVoltageMin", 3.6999),
            raw(T0, "BrickVoltageMax", 3.72),
            raw(T0 + 1000000000, "BrickVoltageMin", 3.70),
            raw(T0 + 1000000000, "BrickVoltageMin", 3.71),
            raw(T0 + 1000000000, "BrickVoltageMax", 3.72)]
    out = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 1.0), **GAP2H))
    assert "terminal_conflict" in by(
        out, "battery.conditions.brick_min_raw")["reason"]
    assert "terminal_conflict" in by(
        out, "battery.conditions.brick_spread_raw")["reason"]
    assert "terminal_conflict" in by(
        out, "battery.conditions.brick_spread_v")["reason"]


def test_terminal_isolation_tombstone():
    t0, t1 = T0, T0 + HOUR
    rows = [raw(t0, "IsolationResistance", 1000.0),
            sig(t1, field="IsolationResistance", num=None, unit=None,
                quality="invalid")]
    out = co.analyze(rows, [], cfg(
        isolation_calibration=cal("ohm", 1.0), **GAP2H))
    got = by(out, "battery.conditions.isolation_ohm")
    assert got["status"] == "unavailable"
    assert "terminal_invalid" in got["reason"]


def test_malformed_ids_are_barriers_not_attribution():
    # Fractional module IDs are barriers; an integer ID change also
    # breaks attribution. Raw rows preserve the reported values.
    rows = therm_pair(T0, 22.0, 20.0, max_id=7, min_id=3)
    rows += therm_pair(T0 + HOUR, 23.0, 20.0, max_id=7.5, min_id=3)
    rows += therm_pair(T0 + 2 * HOUR, 24.0, 20.0, max_id=7, min_id=0)
    out = co.analyze(rows, [], cfg(module_temp_calibration=mod_cal(),
                                   **GAP2H))
    bad = by(out, "battery.conditions.thermal_slope_raw_per_h")
    assert bad["status"] == "unavailable"
    # Raw ID rows still preserve the reported (malformed) values.
    assert by(out, "battery.conditions.module_temp_max_id")["value"] == 7.0
    assert by(out, "battery.conditions.module_temp_min_id")["value"] == 0.0


def test_zero_module_id_remains_valid_without_assumed_index_base():
    rows = therm_pair(T0, 22.0, 20.0, max_id=7, min_id=0)
    rows += therm_pair(T0 + HOUR, 23.0, 20.0, max_id=7, min_id=0)
    out = co.analyze(rows, [], cfg(module_temp_calibration=mod_cal(),
                                   max_gap_ns=HOUR))
    assert by(out, "battery.conditions.thermal_slope_raw_per_h")[
        "status"] == "derived"


def test_zero_brick_id_is_not_used_for_persistence():
    rows = brick_pair(T0, 3.72, 3.70, min_id=0)
    rows += brick_pair(T0 + HOUR, 3.73, 3.71, min_id=0)
    out = co.analyze(rows, [], cfg(max_gap_ns=HOUR))
    assert by(out, "battery.conditions.brick_min_id")["value"] == 0.0
    persistence = by(out, "battery.conditions.brick_min_id_persistence_s")
    assert persistence["status"] == "unavailable"
    assert persistence["value"] is None


def test_prepared_and_raw_analyses_agree():
    rows = brick_pair(T0, 4.2, 4.0) + brick_pair(T0 + HOUR, 4.3, 4.0)
    expected = co.analyze(list(rows), [], cfg(**GAP2H))
    assert co.analyze(bc.prepare_signals(rows), [], cfg(**GAP2H)) == expected
    assert co.analyze(
        bc.prepare_signals(list(reversed(rows))), [], cfg(**GAP2H)) == expected


def test_raw_malformed_inputs_dropped_in_both_paths():
    good = brick_pair(T0, 4.2, 4.0)
    bad = [None, "x", 42, {}, dict(good[0], event_time_ns="bad"),
           dict(good[0], vehicle="")]
    rows = good + bad
    expected = co.analyze(list(good), [], cfg())
    assert co.analyze(list(rows), [], cfg()) == expected
    assert co.analyze(bc.prepare_signals(rows), [], cfg()) == expected


def test_invalid_and_conflicting_duplicates_stay_barriers_across_paths():
    rows = [raw(T0, "BrickVoltageMax", 4.2), raw(T0, "BrickVoltageMax", 4.3),
            raw(T0, "BrickVoltageMin", 4.0),
            raw(T0, "NumBrickVoltageMax", 7),
            raw(T0, "NumBrickVoltageMin", 3)]
    expected = co.analyze(list(rows), [], cfg())
    assert co.analyze(bc.prepare_signals(rows), [], cfg()) == expected
    bad = by(expected, "battery.conditions.brick_spread_raw")
    assert bad["status"] == "unavailable" and bad["value"] is None
    inv = [sig(T0, field="BrickVoltageMax", num=None, unit=None,
               quality="invalid"),
           raw(T0, "BrickVoltageMin", 4.0)]
    out = co.analyze(list(inv), [], cfg())
    assert co.analyze(bc.prepare_signals(inv), [], cfg()) == out
    assert by(out, "battery.conditions.brick_spread_raw")["status"] == \
        "unavailable"


def test_late_ingest_decision_preserved_across_paths():
    rows = [sig(T0, field="Soc", num=90.0, unit="%", ingest=T0),
            sig(T0 + HOUR, field="Soc", num=90.0, unit="%",
                ingest=T0 + 2 * HOUR)]
    config = {"conditions": {"soc_high_pct": 80.0, **GAP2H},
              "decision_time_ns": T0 + HOUR}
    expected = co.analyze(list(rows), [], config)
    assert co.analyze(bc.prepare_signals(rows), [], config) == expected
    got = by(expected, "battery.conditions.exposure_high_soc_s")
    assert got["status"] == "unavailable"


def test_terminal_equivalence_across_paths():
    t0, t1 = T0, T0 + 1000000000
    rows = [raw(t0, "BrickVoltageMax", 3.72),
            raw(t0, "BrickVoltageMin", 3.6999),
            raw(t0, "NumBrickVoltageMax", 7),
            raw(t0, "NumBrickVoltageMin", 2),
            raw(t1, "BrickVoltageMax", 3.72),
            sig(t1, field="BrickVoltageMin", num=None, unit=None,
                quality="invalid"),
            raw(t1, "NumBrickVoltageMax", 7),
            raw(t1, "NumBrickVoltageMin", 2)]
    config = cfg(brick_voltage_calibration=cal("V", 1.0), **GAP2H)
    expected = co.analyze(list(rows), [], config)
    assert co.analyze(bc.prepare_signals(rows), [], config) == expected
    assert "terminal_invalid" in by(
        expected, "battery.conditions.brick_spread_raw")["reason"]


def test_skew_and_wrong_unit_barriers_across_paths():
    skew_rows = [raw(T0 + HOUR, "BrickVoltageMax", 4.2),
                 raw(T0, "BrickVoltageMin", 4.0),
                 raw(T0 + HOUR, "NumBrickVoltageMax", 7),
                 raw(T0, "NumBrickVoltageMin", 3)]
    out = co.analyze(list(skew_rows), [], cfg())
    assert co.analyze(bc.prepare_signals(skew_rows), [], cfg()) == out
    assert "unsynchronized" in by(
        out, "battery.conditions.brick_spread_raw")["reason"]
    rows = [dict(r, unit="V") if r.get("source_field") == "Soc" else r
            for r in brick_pair(T0, 4.2, 4.0)]
    out = co.analyze(list(rows), [], cfg())
    assert co.analyze(bc.prepare_signals(rows), [], cfg()) == out
    assert by(out, "battery.conditions.brick_spread_raw")["status"] == \
        "derived"

def test_calibrated_terminal_extrema_offset_scale():
    rows = ([raw(T0, "BrickVoltageMax", 4.0),
             raw(T0, "BrickVoltageMin", 3.9),
             batt_max(T0, 20.0), batt_min(T0, 18.0)]
            + [raw(T0 + HOUR, "BrickVoltageMax", 4.1),
               raw(T0 + HOUR, "BrickVoltageMin", 4.0),
               batt_max(T0 + HOUR, 21.0),
               batt_min(T0 + HOUR, 19.0)])
    out = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 2.0, offset=1.0),
        module_temp_calibration=mod_cal(scale=3.0, offset=-2.0),
        **GAP2H))
    got_max = by(out, "battery.conditions.brick_max_v")
    assert got_max["status"] == "derived" and got_max["unit"] == "V"
    assert abs(got_max["value"] - 9.2) < 1e-9
    assert got_max["calibration_version"] == "synth-1"
    assert "asof_ns=%d" % (T0 + HOUR) in got_max["reason"]
    assert got_max["value_text"] == "2023-11-14 23:13:20.000000000"
    got_min = by(out, "battery.conditions.brick_min_v")
    assert abs(got_min["value"] - 9.0) < 1e-9
    assert by(out, "battery.conditions.module_temp_max_c")["value"] == 61.0
    assert by(out, "battery.conditions.module_temp_min_c")["value"] == 55.0
    assert by(out, "battery.conditions.module_temp_max_c")["unit"] == \
        "celsius"


def test_calibrated_extrema_missing_and_wrong_scope_fail_closed():
    rows = brick_pair(T0, 4.2, 4.0) + therm_pair(T0, 25.0, 24.0)
    out = co.analyze(rows, [], cfg())
    for metric in ("battery.conditions.brick_max_v",
                   "battery.conditions.brick_min_v",
                   "battery.conditions.module_temp_max_c",
                   "battery.conditions.module_temp_min_c"):
        got = by(out, metric)
        assert got["status"] == "unavailable" and got["value"] is None
        assert "missing_calibration" in got["reason"]
    scoped = cal("V", 1.0)
    scoped["scope"] = {"vehicle": "other", "source": "*",
                       "decode_epoch": "*"}
    out2 = co.analyze(rows, [], cfg(brick_voltage_calibration=scoped,
                                    module_temp_calibration=mod_cal()))
    for metric in ("battery.conditions.brick_max_v",
                   "battery.conditions.brick_min_v"):
        got = by(out2, metric)
        assert got["status"] == "unavailable"
        assert "scope_mismatch" in got["reason"]
    assert by(out2, "battery.conditions.module_temp_max_c")[
        "status"] == "derived"
    out3 = co.analyze(rows, [], cfg(
        domain="pack-A",
        brick_voltage_calibration=cal("V", 1.0, domain="pack-B"),
        module_temp_calibration=mod_cal()))
    assert "domain_mismatch" in by(
        out3, "battery.conditions.brick_max_v")["reason"]
    bad = cal("kWh", 1.0)
    out4 = co.analyze(rows, [], cfg(brick_voltage_calibration=bad,
                                    module_temp_calibration=mod_cal()))
    err = by(out4, "battery.conditions.brick_max_v")
    assert err["status"] == "error" and "unit" in err["reason"]
    assert by(out4, "battery.conditions.brick_max_raw")[
        "status"] == "reported"


def test_calibrated_extrema_terminal_tombstone_conflict_no_fallback():
    t0, t1 = T0, T0 + 1000000000
    rows = [raw(t0, "BrickVoltageMax", 4.2),
            raw(t0, "BrickVoltageMin", 4.0),
            raw(t0, "ModuleTempMax", 25.0),
            raw(t0, "ModuleTempMin", 24.0),
            raw(t1, "BrickVoltageMax", 4.3),
            sig(t1, field="BrickVoltageMin", num=None, unit=None,
                quality="invalid"),
            raw(t1, "ModuleTempMax", 26.0),
            raw(t1, "ModuleTempMax", 27.0),
            raw(t1, "ModuleTempMin", 24.0)]
    out = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 2.0, offset=1.0),
        module_temp_calibration=mod_cal(), **GAP2H))
    # Terminal-invalid min stays NULL: no older 4.0*2+1 fallback.
    assert by(out, "battery.conditions.brick_min_v")["value"] is None
    assert "terminal_invalid" in by(
        out, "battery.conditions.brick_min_v")["reason"]
    assert abs(by(out, "battery.conditions.brick_max_v")["value"]
               - 9.6) < 1e-9
    # Terminal-conflicting max stays NULL even with an older valid 25.0.
    assert by(out, "battery.conditions.module_temp_max_c")["value"] is None
    assert "terminal_conflict" in by(
        out, "battery.conditions.module_temp_max_c")["reason"]
    assert by(out, "battery.conditions.module_temp_min_c")["value"] == 24.0


def test_calibrated_extrema_wrong_unit_terminal_fails_closed():
    rows = [dict(r, unit="V")
            if r.get("source_field") == "BrickVoltageMax" else r
            for r in brick_pair(T0, 4.2, 4.0)]
    out = co.analyze(rows, [], cfg(
        brick_voltage_calibration=cal("V", 2.0, offset=1.0)))
    got = by(out, "battery.conditions.brick_max_v")
    assert got["status"] == "unavailable" and got["value"] is None
    assert "wrong_unit" in got["reason"]
    assert by(out, "battery.conditions.brick_min_v")["value"] == 9.0

if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_conditions: ok (%d tests)" % len(names))
