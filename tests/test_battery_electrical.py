"""Focused stdlib tests for scripts/battery_electrical.py. Plain asserts,
stdlib only; doubles as __main__ runner. All circuits/curves are synthetic
(supplied validated calibration), never Tesla pack truth.
"""

import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import battery_electrical as be

HOUR = 3600000000000
T0 = 1700000000000000000
ANCHOR = 1790474400000000000


def sig(ts, field="PackCurrent", num=1.0, unit=None, quality="unit_unverified",
        vehicle="v", source="fleet", epoch="e1", ingest=None):
    return {"event_time_ns": ts, "ingest_time_ns": ingest, "vehicle": vehicle,
            "source": source, "decode_epoch": epoch, "path": "P",
            "source_field": field, "value_num": num, "value_text": None,
            "value_bool": None, "unit": unit, "quality": quality,
            "envelope_id": None, "config_version": None,
            "connectivity": None}


def pack_sig(ts, num, **kw):
    kw.setdefault("field", "PackCurrent")
    kw.setdefault("unit", None)
    kw.setdefault("quality", "unit_unverified")
    return sig(ts, num=num, **kw)


def pack_v(ts, num, **kw):
    kw.setdefault("field", "PackVoltage")
    kw.setdefault("unit", None)
    kw.setdefault("quality", "unit_unverified")
    return sig(ts, num=num, **kw)


def cal(**over):
    base = {"calibration_version": "synth-1", "domain": "synthetic-circuit",
            "calibration_scope": {"vehicle": "v", "source": "fleet",
                                  "decode_epoch": "e1"},
            "capacity_ah": 100.0, "current_sign": 1,
            "initial_soc_pct": 50.0, "initial_soc_time_ns": T0,
            "max_gap_ns": 2 * HOUR,
            "field_units": {"PackCurrent": "A", "PackVoltage": "V"}}
    base.update(over)
    return {"electrical": base}


OCV = [[0.0, 3.0], [0.5, 3.7], [1.0, 4.2]]
OCV_STEEP = [[0.0, 3.0], [0.25, 3.5], [0.5, 3.8], [0.75, 4.0], [1.0, 4.2]]
CIRCUIT_OCV = [[0.0, 350.0], [1.0, 450.0]]


def circuit_rows(scope=None, temp_unit="celsius", soc_field="Soc",
                 with_barrier=None):
    """Main's 242-timestamp charge/rest/discharge smoke shape, official
    Pack fields, raw NULL/unit_unverified units, 60 s spacing.

    Profile: 2 h charge at +10 A, 1 h rest, then 1 h discharge at -20 A,
    with zero-current boundary samples. Capacity 100 Ah from 50% gives
    final 49.91666666666661%. V = 350 + Soc01*100 + 0.05*PackCurrent.
    ModuleTempMin 24 / Max 26 (configurable unit); OutsideTemp 10 must
    never condition DCR. with_barrier=(field, idx) makes one row invalid.
    """
    scope = scope or {}
    vehicle = scope.get("vehicle", "battery-circuit")
    source = scope.get("source", "fleet")
    epoch = scope.get("decode_epoch", "circuit-v1")
    rows = []
    soc = 0.5
    previous_current = 0.0
    for minute in range(-1, 241):
        tstamp = ANCHOR + minute * 60000000000
        amps = 10.0 if 0 <= minute < 120 else \
            -20.0 if 180 <= minute < 240 else 0.0
        if minute > 0:
            soc += (previous_current + amps) * 0.5 / 60.0 / 100.0
        previous_current = amps
        volts = 350.0 + soc * 100.0 + 0.05 * amps
        rows.append(pack_sig(tstamp, amps, vehicle=vehicle, source=source,
                             epoch=epoch))
        rows.append(pack_v(tstamp, volts, vehicle=vehicle, source=source,
                           epoch=epoch))
        rows.append(sig(tstamp, field="ModuleTempMin", num=24.0,
                        unit=temp_unit, quality=None, vehicle=vehicle,
                        source=source, epoch=epoch))
        rows.append(sig(tstamp, field="ModuleTempMax", num=26.0,
                        unit=temp_unit, quality=None, vehicle=vehicle,
                        source=source, epoch=epoch))
        rows.append(sig(tstamp, field="OutsideTemp", num=10.0,
                        unit="celsius", quality=None, vehicle=vehicle,
                        source=source, epoch=epoch))
        rows.append(sig(tstamp, field=soc_field, num=soc * 100.0, unit="%",
                        quality=None, vehicle=vehicle, source=source,
                        epoch=epoch))
    if with_barrier is not None:
        field, bidx = with_barrier
        for pos, row in enumerate(rows):
            if row["source_field"] == field and \
                    row["event_time_ns"] == ANCHOR + bidx * 60000000000:
                rows[pos] = dict(row, value_num=None, quality="invalid")
                break
    return rows


def circuit_cfg(**over):
    base = {"calibration_version": "circuit-1", "domain": "circuit-smoke",
            "calibration_scope": {"vehicle": "battery-circuit",
                                  "source": "fleet",
                                  "decode_epoch": "circuit-v1"},
            "capacity_ah": 100.0, "current_sign": 1,
            "initial_soc_pct": 50.0, "initial_soc_time_ns": ANCHOR,
            "max_gap_ns": 600000000000,
            "field_units": {"PackCurrent": "A", "PackVoltage": "V"},
            "ocv_curve": CIRCUIT_OCV, "ocv_version": "ocv-circuit",
            "rest_seconds": 600.0, "rest_current_threshold_a": 2.0,
            "ekf": {"r0_ohm": 0.05, "r1_ohm": 0.01, "c1_f": 1000.0,
                    "capacity_ah": 100.0, "q_soc_per_s": 1e-9,
                    "q_v1_per_s": 1e-12, "r_v": 1e-4, "init_soc_pct": 50.0,
                    "init_time_ns": ANCHOR, "init_v1_v": 0.0,
                    "init_p_soc": 1e-4, "init_p_v1": 1e-6},
            "ekf_version": "ekf-circuit"}
    bounds = {key: over.pop(key) for key in
              ("window_start_ns", "window_end_ns", "decision_time_ns")
              if key in over}
    base.update(over)
    return {"electrical": base, **bounds}


def test_coulomb_known_answer_charge_then_discharge():
    # 10 A charge for 1 h into 100 Ah from 50% -> 60%; then a distinct
    # discharge step: leg into T0+2h averages (10 + -10)/2 = 0, then
    # -10 A for 0.5 h gives 60% - 5% = 55%.
    charge = [(T0, 10.0), (T0 + HOUR, 10.0)]
    res = be.coulomb_soc_series(charge, 100.0, 0.5, 1, max_gap_ns=HOUR)
    assert abs(res["final_soc01"] - 0.6) < 1e-12
    assert res["legs_used"] == 1 and res["gaps"] == 0
    assert res["ambiguous"] == 0
    both = [(T0, 10.0), (T0 + HOUR, 10.0), (T0 + 2 * HOUR, -10.0),
            (T0 + 2 * HOUR + HOUR // 2, -10.0)]
    res = be.coulomb_soc_series(both, 100.0, 0.5, 1, max_gap_ns=HOUR)
    assert abs(res["final_soc01"] - 0.55) < 1e-12
    assert res["ambiguous"] == 0 and res["legs_used"] == 3


def test_coulomb_ambiguous_same_time_refuses():
    # Contradictory values at one timestamp are ambiguous: continuity
    # breaks there, never a deterministic-looking SOC from one branch.
    res = be.coulomb_soc_series(
        [(T0, 10.0), (T0 + HOUR, 10.0), (T0 + HOUR, -10.0)], 100.0,
        0.5, 1, max_gap_ns=HOUR)
    assert res["ambiguous"] == 1
    assert res["soc"][1][1] is None and res["soc"][2][1] is None
    assert res["final_soc01"] == 0.5  # only the pre-ambiguity anchor
    # Identical duplicates are one sample, not ambiguity.
    res = be.coulomb_soc_series(
        [(T0, 10.0), (T0 + HOUR, 10.0), (T0 + HOUR, 10.0)], 100.0,
        0.5, 1, max_gap_ns=HOUR)
    assert res["ambiguous"] == 0
    assert abs(res["final_soc01"] - 0.6) < 1e-12


def test_coulomb_sign_and_efficiency():
    # sign -1: positive measured current discharges; charging leg at half
    # efficiency banks only half the amp-hours.
    res = be.coulomb_soc_series([(T0, 10.0), (T0 + HOUR, 10.0)], 100.0,
                                0.5, -1, max_gap_ns=HOUR)
    assert abs(res["final_soc01"] - 0.4) < 1e-12
    res = be.coulomb_soc_series([(T0, 10.0), (T0 + HOUR, 10.0)], 100.0,
                                0.5, 1, efficiency=0.5, max_gap_ns=HOUR)
    assert abs(res["final_soc01"] - 0.55) < 1e-12
    res = be.coulomb_soc_series([(T0, -10.0), (T0 + HOUR, -10.0)], 100.0,
                                0.5, 1, efficiency=0.5, max_gap_ns=HOUR)
    assert abs(res["final_soc01"] - 0.4) < 1e-12  # discharge ignores eff
    try:
        be.coulomb_soc_series([(T0, 1.0)], 0.0, 0.5, 1)
    except be.ElectricalError:
        pass
    else:
        raise AssertionError("zero capacity must raise")
    for bad in (0, 2, True):
        try:
            be.coulomb_soc_series([(T0, 1.0)], 100.0, 0.5, bad)
        except be.ElectricalError:
            pass
        else:
            raise AssertionError("sign must be exactly 1/-1")


def test_coulomb_gap_invalidates_and_clamps():
    res = be.coulomb_soc_series(
        [(T0, 10.0), (T0 + HOUR, 10.0), (T0 + 3 * HOUR, 10.0)], 100.0,
        0.5, 1, max_gap_ns=HOUR)
    assert res["gaps"] == 1 and res["soc"][-1][1] is None
    assert abs(res["final_soc01"] - 0.6) < 1e-12
    res = be.coulomb_soc_series([(T0, 1000.0), (T0 + 10 * HOUR, 1000.0)],
                                100.0, 0.9, 1, max_gap_ns=20 * HOUR)
    assert res["final_soc01"] == 1.0 and res["saturated"] is True
    res = be.coulomb_soc_series([(T0, 1.0), (T0 + 10, 1.0)], 100.0, 0.5, 1,
                                current_std_a=1.0, initial_std01=0.01,
                                max_gap_ns=HOUR)
    assert res["std01"] is not None and res["std01"] >= 0.01
    # One 1 h leg at 10 A, 2 A noise, 100 Ah: endpoint weights 0.005 and
    # 0.005, so var = 2 * 0.005^2 * 4.0 = 2e-4, std = sqrt(2e-4).
    res = be.coulomb_soc_series([(T0, 10.0), (T0 + HOUR, 10.0)], 100.0,
                                0.5, 1, current_std_a=2.0,
                                initial_std01=0.0, max_gap_ns=HOUR)
    assert abs(res["std01"] - math.sqrt(2e-4)) < 1e-12
    # Two equal 1 h legs: shared middle sample weight 0.01, ends 0.005,
    # so var = (2 * 0.005^2 + 0.01^2) * 4.0 = 6e-4 (legs correlate).
    res = be.coulomb_soc_series(
        [(T0, 10.0), (T0 + HOUR, 10.0), (T0 + 2 * HOUR, 10.0)], 100.0,
        0.5, 1, current_std_a=2.0, initial_std01=0.0, max_gap_ns=HOUR)
    assert abs(res["final_soc01"] - 0.7) < 1e-12
    assert abs(res["std01"] - math.sqrt(6e-4)) < 1e-12
    res = be.coulomb_soc_series([(T0, 1.0), (T0 + 10, 1.0)], 100.0, 0.5, 1,
                                max_gap_ns=HOUR)
    assert res["std01"] is None  # never fabricated


def test_ocv_lookup_inverse_and_flat_rejection():
    curve = be.validate_ocv_curve(OCV)
    assert abs(be.ocv_from_soc(0.25, curve) - 3.35) < 1e-12
    assert be.ocv_from_soc(-0.1, curve) is None
    assert be.ocv_from_soc(1.5, curve) is None
    assert abs(be.soc_from_ocv(3.35, curve) - 0.25) < 1e-9
    assert be.soc_from_ocv(2.0, curve) is None  # out of domain
    flat = be.validate_ocv_curve([[0.0, 3.0], [0.5, 3.0001], [1.0, 4.2]])
    assert be.soc_from_ocv(3.00005, flat, min_dv_dsoc_v=0.05) is None
    for bad in ([[0.0, 3.7]], [[0.0, 3.7], [0.5, 3.6]],
                [[0.0, 3.7], [0.5, 3.7]], [[1.5, 3.7], [2.0, 4.0]]):
        try:
            be.validate_ocv_curve(bad)
        except be.ElectricalError:
            pass
        else:
            raise AssertionError("non-monotonic curve must raise: %r" % bad)


def test_resting_eligibility():
    chg = [(T0 + i * 600000000000, 0.5) for i in range(5)]  # 40 min @0.5A
    assert be.is_resting_at(chg, T0 + 4 * 600000000000, 1800, 2.0, HOUR)
    hot = [(T0 + i * 600000000000, 5.0 if i == 4 else 0.5)
           for i in range(5)]
    assert not be.is_resting_at(hot, T0 + 4 * 600000000000, 1800, 2.0, HOUR)
    assert not be.is_resting_at(chg[:1], T0, 1800, 2.0, HOUR)  # no coverage


def test_ekf_corrects_toward_voltage():
    # Synthetic 1RC circuit: true SOC 0.5, R0 voltage drop 0.05 V at 10 A.
    # EKF starts low at 0.3 with tight voltage noise; after one 60 s step
    # the posterior must move up toward truth (analytic check: corrected
    # SOC in (0.3, 0.6), gain dominated by steep OCV slope 1.0 V/SOC).
    curve = [[0.0, 3.0], [1.0, 4.0]]
    true_soc = 0.5
    volts = 3.0 + true_soc + 10.0 * 0.005
    steps = [(T0, 10.0, volts - 0.02), (T0 + 60000000000, 10.0, volts)]
    res = be.ekf_1rc_soc(steps, curve, 0.005, 0.01, 1000.0, 100.0, 1,
                         q_soc_per_s=1e-9, q_v1_per_s=1e-12, r_v=1e-6,
                         init_soc01=0.3, init_v1_v=0.0, init_p_soc=1e-2,
                         init_p_v1=1e-6)
    assert res["updates"] == 1
    assert 0.3 < res["final_soc01"] < 0.6
    assert res["flat_updates"] == 0
    # Flat OCV flags degraded correction instead of a confident number.
    flat_curve = [[0.0, 3.7], [1.0, 3.7001]]
    res = be.ekf_1rc_soc(steps, flat_curve, 0.005, 0.01, 1000.0, 100.0, 1,
                         q_soc_per_s=1e-9, q_v1_per_s=1e-12, r_v=1e-6,
                         init_soc01=0.3, init_v1_v=0.0, init_p_soc=1e-2,
                         init_p_v1=1e-6)
    assert res["flat_updates"] == 1
    # Contradictory same-time steps refuse instead of silently picking.
    try:
        be.ekf_1rc_soc([(T0, 10.0, 3.5), (T0, 11.0, 3.6),
                        (T0 + 60000000000, 10.0, 3.55)], curve, 0.005,
                       0.01, 1000.0, 100.0, 1)
    except be.ElectricalError as exc:
        assert "ambiguous" in str(exc)
    else:
        raise AssertionError("contradictory steps must raise")
    # Identical same-time duplicates are one sample, not ambiguity.
    res = be.ekf_1rc_soc([(T0, 10.0, volts - 0.02),
                          (T0, 10.0, volts - 0.02),
                          (T0 + 60000000000, 10.0, volts)], curve, 0.005,
                         0.01, 1000.0, 100.0, 1,
                         q_soc_per_s=1e-9, q_v1_per_s=1e-12, r_v=1e-6,
                         init_soc01=0.3, init_v1_v=0.0, init_p_soc=1e-2,
                         init_p_v1=1e-6)
    assert res["updates"] == 1
    for bad_kw in ({"r0_ohm": 0.0}, {"r0_ohm": -1.0}):
        try:
            be.ekf_1rc_soc(steps, curve, bad_kw.get("r0_ohm", 0.005), 0.01,
                           1000.0, 100.0, 1)
        except be.ElectricalError:
            pass
        else:
            raise AssertionError("non-positive R0 must raise")


def test_apparent_resistance_known_answer():
    # Charge convention: +10 A step raises terminal voltage 0.05 V over a
    # 0.005 ohm apparent resistance.
    res, why = be.apparent_resistance_ohm(3.9, 3.95, 0.0, 10.0)
    assert why is None and abs(res - 0.005) < 1e-12
    res, why = be.apparent_resistance_ohm(3.9, 3.95, 0.0, 1.0)
    assert res is None and why == "low_delta"
    res, why = be.apparent_resistance_ohm(3.95, 3.9, 0.0, 10.0)
    assert res is None and why == "non_physical"  # never negative
    ok, why = be.check_step_timing(100, 101, 200, 201, 10, 50, 1000)
    assert ok and why is None
    ok, why = be.check_step_timing(100, 500, 200, 201, 10, 50, 1000)
    assert not ok and why == "pre_skew"
    ok, why = be.check_step_timing(100, 101, 105, 106, 10, 50, 1000)
    assert not ok and why == "step_too_short"
    ok, why, checked = be.check_step_comparability(
        0.5, 0.505, 25.0, 26.0, 0.02, 5.0)
    assert ok and why is None and checked is True
    ok, why, _ = be.check_step_comparability(0.5, 0.6, None, None, 0.02,
                                             5.0)
    assert not ok and why == "soc_shift"


def test_ica_dva_known_answer():
    # Synthetic charge: Q 0..2 Ah, V = 3.5 + 0.25 Q + 0.5 Q^2 / 2 peak slope
    # near the end; analytic dQ/dV at Q=1 is 1/(0.25+0.5) = 1.333 Ah/V.
    samples = [(T0 + i * 60000000000, i * 0.25,
                3.5 + 0.25 * (i * 0.25) + 0.25 * (i * 0.25) ** 2)
               for i in range(9)]
    res = be.ica_dva_curve(samples, "charge")
    assert res["reason"] is None
    assert res["features"]["n_points"] == 9
    mid = res["curve"][4]
    assert abs(mid[2] - 1.333333) < 0.05  # central-difference ICA
    assert abs(mid[3] - 0.75) < 0.05  # DVA reciprocal
    # dV/dQ grows with Q, so |dQ/dV| peaks at the low-Q end (endpoint
    # one-sided diff ~3.2) and |dV/dQ| at the high-Q end.
    assert abs(res["features"]["ica_peak_dqdv_ah_per_v"] - 3.2) < 0.05
    assert res["features"]["ica_peak_q_ah"] <= 0.5
    assert res["features"]["dva_peak_q_ah"] >= 1.5
    assert res["features"]["dva_peak_dvdq_v_per_ah"] > 1.0
    # Smoothed helper reproduces the same analytic midpoint.
    res3 = be.ica_dva_curve(samples, "charge", smooth_window=3)
    assert abs(res3["curve"][4][2] - 1.333333) < 0.15
    # Discharge direction accepts falling voltage with rising throughput.
    down = [(t, q, 4.2 - (v - 3.5)) for t, q, v in samples]
    res_d = be.ica_dva_curve(down, "discharge")
    assert res_d["reason"] is None and res_d["features"]["n_points"] == 9
    assert be.ica_dva_curve(samples, "discharge")["reason"] == \
        "non_monotonic_v"
    short = samples[:2]
    assert be.ica_dva_curve(short, "charge")["reason"] == "sparse"
    gapped = list(samples)
    gapped[4] = (gapped[4][0] + 2 * HOUR, gapped[4][1], gapped[4][2])
    assert be.ica_dva_curve(gapped, "charge")["reason"] == "gap"
    flat = [(T0 + i * 60000000000, i * 0.1, 3.7) for i in range(5)]
    assert be.ica_dva_curve(flat, "charge")["reason"] in (
        "non_monotonic_v", "insufficient_span")


def test_circuit_coulomb_final_and_anchor_outside():
    rows = circuit_rows()
    out = be.analyze(rows, [], circuit_cfg())
    got = {r["metric"]: r for r in out}[
        "battery.electrical.soc_coulomb_pct"]
    assert got["status"] == "estimated"
    assert abs(got["value"] - 49.91666666666661) < 1e-9
    assert ";observation_time_ns=%d" % (ANCHOR + 4 * HOUR) \
        in got["reason"]
    # Anchor after all history: unavailable, never a reset estimate.
    out = be.analyze(rows, [], circuit_cfg(
        initial_soc_time_ns=ANCHOR + 300 * 60000000000))
    got = {r["metric"]: r for r in out}[
        "battery.electrical.soc_coulomb_pct"]
    assert got["status"] == "unavailable"
    assert "anchor_outside_history" in got["reason"]


def test_circuit_two_adjacent_windows_continuous():
    rows = circuit_rows()
    mid = ANCHOR + 121 * 60000000000
    cfg1 = circuit_cfg(window_start_ns=ANCHOR, window_end_ns=mid)
    cfg2 = circuit_cfg(window_start_ns=mid + 1, window_end_ns=None)
    got1 = {r["metric"]: r for r in be.analyze(rows, [], cfg1)}[
        "battery.electrical.soc_coulomb_pct"]
    got2 = {r["metric"]: r for r in be.analyze(rows, [], cfg2)}[
        "battery.electrical.soc_coulomb_pct"]
    assert got1["status"] == "estimated" and got2["status"] == "estimated"
    # Window 2 continues the anchor run (no reset to 50% at its start):
    # same final value as the unwindowed run.
    full = {r["metric"]: r for r in be.analyze(rows, [],
                                               circuit_cfg())}[
        "battery.electrical.soc_coulomb_pct"]
    assert abs(got2["value"] - full["value"]) < 1e-9
    assert got2["value"] < got1["value"]  # discharge after the split


def test_circuit_scope_mismatch_is_unavailable():
    rows = circuit_rows()
    cfg = circuit_cfg(calibration_scope={"vehicle": "other-vehicle",
                                         "source": "fleet",
                                         "decode_epoch": "circuit-v1"})
    out = be.analyze(rows, [], cfg)
    assert len(out) == 8
    assert all(r["status"] == "unavailable" for r in out)
    assert all("scope_mismatch" in r["reason"] for r in out)


def test_circuit_ocv_ekf_dcr_paths():
    rows = circuit_rows()
    out = be.analyze(rows, [], circuit_cfg())
    by_metric = {r["metric"]: r for r in out}
    ocv = by_metric["battery.electrical.soc_ocv_pct"]
    assert ocv["status"] == "estimated"
    assert 0.0 < ocv["value"] < 100.0
    assert "observation_time_ns" in ocv["reason"]
    ekf = by_metric["battery.electrical.soc_ekf_pct"]
    assert ekf["status"] == "estimated"
    assert ekf["uncertainty"] is not None
    assert "observation_time_ns" in ekf["reason"]
    dcr = by_metric["battery.electrical.resistance_apparent_ohm"]
    assert dcr["status"] == "derived"
    assert 0.03 < dcr["value"] < 0.07  # apparent value includes OCV drift


def test_circuit_invalid_between_close_rows_blocks_paths():
    # One invalid PackVoltage row mid-discharge: OCV must skip only the
    # bridged stamp (still estimates from another rest window), while an
    # invalid PackCurrent row mid-charge breaks every current-bridging
    # path that must cross it.
    rows_v = circuit_rows(with_barrier=("PackVoltage", 200))
    out = be.analyze(rows_v, [], circuit_cfg())
    by_metric = {r["metric"]: r for r in out}
    assert by_metric["battery.electrical.soc_ocv_pct"]["status"] == \
        "estimated"
    rows_i = circuit_rows(with_barrier=("PackCurrent", 30))
    out = be.analyze(rows_i, [], circuit_cfg())
    by_metric = {r["metric"]: r for r in out}
    # Coulomb run ends at the barrier: last integrated stamp precedes it.
    coul = by_metric["battery.electrical.soc_coulomb_pct"]
    assert coul["status"] == "estimated"
    assert "observation_time_ns=%d" % (ANCHOR + 29 * 60000000000) \
        in coul["reason"]
    # EKF/DCR/ICA cannot bridge the invalid current row.
    assert "truncated" in coul["reason"] or "gap" in coul["reason"] \
        or "ambiguity" in coul["reason"] or "observation_time" in coul[
            "reason"]
    ica = by_metric["battery.electrical.ica_peak_voltage_v"]
    assert ica["status"] == "unavailable"
    assert "gap" in ica["reason"] or "skew" in ica["reason"] or \
        "invalid" in ica["reason"] or "segment" in ica["reason"]


def test_circuit_missing_battery_temp_is_explicit():
    rows = [r for r in circuit_rows()
            if r["source_field"] not in ("ModuleTempMin", "ModuleTempMax")]
    out = be.analyze(rows, [], circuit_cfg())
    dcr = {r["metric"]: r for r in out}[
        "battery.electrical.resistance_apparent_ohm"]
    assert dcr["status"] == "unavailable"
    assert "missing_battery_temperature" in dcr["reason"]
    # Ambient OutsideTemp=10 present throughout is never a substitute.


def test_circuit_missing_calibration_and_bms_passthrough():
    out = be.analyze(circuit_rows(), [], {})
    assert len(out) == 8
    by_metric = {r["metric"]: r for r in out}
    assert by_metric["battery.electrical.soc_coulomb_pct"]["status"] == \
        "unavailable"
    assert "missing_calibration" in \
        by_metric["battery.electrical.soc_coulomb_pct"]["reason"]
    bms = [sig(ANCHOR, field="Soc", num=62.0, unit="%", quality=None),
           sig(ANCHOR + 60 * 10 ** 9, field="Soc", num=62.0, unit="%",
               quality=None)]
    out = be.analyze(bms, [], cal())
    got = {r["metric"]: r for r in out}[
        "battery.electrical.soc_coulomb_pct"]
    assert got["status"] == "unavailable" and "no_current" in got["reason"]


def test_circuit_coulomb_gap_and_uncalibrated():
    rows = [pack_sig(T0, 10.0), pack_sig(T0 + HOUR, 10.0),
            pack_sig(T0 + 5 * HOUR, 10.0)]
    out = be.analyze(rows, [], cal())
    got = {r["metric"]: r for r in out}[
        "battery.electrical.soc_coulomb_pct"]
    assert got["status"] == "estimated" and abs(got["value"] - 60.0) < 1e-9
    assert "truncated_at_gap" in got["reason"]
    rows = [pack_sig(T0, 10.0),
            pack_sig(T0 + HOUR, 10.0),
            dict(pack_sig(T0 + HOUR, -10.0))]
    out = be.analyze(rows, [], cal())
    got = {r["metric"]: r for r in out}[
        "battery.electrical.soc_coulomb_pct"]
    assert got["status"] == "unavailable"
    assert "ambiguous" in got["reason"]
    raw = [sig(T0, field="PackCurrent", num=10.0, unit=None,
               quality="unit_unverified"),
           sig(T0 + HOUR, field="PackCurrent", num=10.0, unit=None,
               quality="unit_unverified")]
    out = be.analyze(raw, [], {"electrical": {
        "calibration_version": "synth-1", "domain": "synthetic-circuit",
        "calibration_scope": {"vehicle": "v", "source": "fleet",
                              "decode_epoch": "e1"},
        "capacity_ah": 100.0, "current_sign": 1,
        "initial_soc_pct": 50.0, "initial_soc_time_ns": T0,
        "max_gap_ns": 2 * HOUR}})
    got = {r["metric"]: r for r in out}[
        "battery.electrical.soc_coulomb_pct"]
    assert got["status"] == "unavailable"
    assert "uncalibrated_current" in got["reason"]


def test_circuit_dcr_and_ica_known_steps():
    pulse = []
    for offset, current in ((0, 0.0), (60000000000, 10.0)):
        stamp = T0 + offset
        pulse.extend([pack_sig(stamp, current),
                      pack_v(stamp, 400.0 + 0.05 * current),
                      sig(stamp, field="Soc", num=50.0, unit="%", quality=None),
                      sig(stamp, field="ModuleTempMin", num=24.0,
                          unit="celsius", quality=None),
                      sig(stamp, field="ModuleTempMax", num=26.0,
                          unit="celsius", quality=None)])
    dcr = {r["metric"]: r for r in be.analyze(pulse, [], cal())}[
        "battery.electrical.resistance_apparent_ohm"]
    assert dcr["status"] == "derived" and abs(dcr["value"] - 0.05) < 1e-12
    # Low delta: 1 A step on the same voltages is rejected.
    rows = [pack_sig(T0, 0.0),
            pack_sig(T0 + 60 * 10 ** 9, 1.0),
            pack_v(T0, 350.5),
            pack_v(T0 + 60 * 10 ** 9, 350.55)]
    cfg_small = cal(initial_soc_time_ns=T0,
                    field_units={"PackCurrent": "A", "PackVoltage": "V"})
    out = be.analyze(rows, [], cfg_small)
    dcr = {r["metric"]: r for r in out}[
        "battery.electrical.resistance_apparent_ohm"]
    assert dcr["status"] == "unavailable" and "low_delta" in dcr["reason"]
    # ICA on a clean constant-current charge leg.
    cc_cfg = cal(initial_soc_time_ns=T0,
                 field_units={"PackCurrent": "A", "PackVoltage": "V"})
    rows = []
    for idx in range(9):
        rows.append(pack_sig(T0 + idx * 60000000000, 10.0))
        rows.append(pack_v(T0 + idx * 60000000000, 350.0 + 0.05 * idx))
    out = be.analyze(rows, [], cc_cfg)
    by_metric = {r["metric"]: r for r in out}
    assert by_metric["battery.electrical.ica_peak_voltage_v"][
        "status"] == "derived"
    assert abs(by_metric["battery.electrical.ica_peak_dqdv_ah_per_v"][
        "value"] - (10.0 / 60.0) / 0.05) < 1e-9
    assert abs(by_metric["battery.electrical.dva_peak_dvdq_v_per_ah"][
        "value"] - 0.05 / (10.0 / 60.0)) < 1e-6
    rows = []
    for idx in range(9):
        rows.append(pack_sig(T0 + idx * 60000000000,
                             10.0 if idx < 5 else 0.0))
        rows.append(pack_v(T0 + idx * 60000000000, 350.0 + 0.05 * idx))
    out = be.analyze(rows, [], cc_cfg)
    assert "non_cc" in {r["metric"]: r for r in out}[
        "battery.electrical.ica_peak_voltage_v"]["reason"]


def test_circuit_scope_isolation_order_and_quality():
    cfg = cal(initial_soc_time_ns=T0,
              field_units={"PackCurrent": "A", "PackVoltage": "V"})
    a = [pack_sig(T0 + i * HOUR, 10.0, vehicle="a") for i in range(2)]
    b = [pack_sig(T0 + i * HOUR, 20.0, vehicle="b") for i in range(2)]
    scoped_cfg = cal(calibration_scope={"vehicle": "a", "source": "fleet",
                                        "decode_epoch": "e1"})
    out = be.analyze(list(reversed(a + b)), [], scoped_cfg)
    got = [r for r in out if r["metric"].endswith("soc_coulomb_pct")]
    assert len(got) == 2
    vals = {r["vehicle"]: r["value"] for r in got}
    assert abs(vals["a"] - 60.0) < 1e-9 and vals["b"] is None
    rows = [pack_sig(T0, 10.0),
            pack_sig(T0 + HOUR, None, quality="invalid"),
            pack_sig(T0 + 2 * HOUR, 10.0)]
    early = dict(rows[0], ingest_time_ns=5)
    late_dup = dict(rows[0], ingest_time_ns=50)
    out = be.analyze([late_dup, early] + rows[1:], [], cfg)
    got = [r for r in out if r["metric"].endswith("soc_coulomb_pct")][0]
    assert got["status"] == "unavailable"  # single-sample segment only
    assert "single_sample" in got["reason"]
    rows = [pack_sig(T0, 10.0), pack_sig(T0 + HOUR, 10.0)]
    fwd = be.analyze(rows, [], cfg)
    rev = be.analyze(list(reversed(rows)), [], cfg)
    assert fwd[0]["value"] == rev[0]["value"]
    assert fwd[0]["revision"] == rev[0]["revision"]
    # Online decision time excludes unknown-ingest rows entirely.
    rows = [pack_sig(T0, 10.0, ingest=T0),
            pack_sig(T0 + HOUR, 10.0, ingest=T0 + 10 * HOUR)]
    cfg_dt = dict(cfg)
    cfg_dt["decision_time_ns"] = T0 + 2 * HOUR
    out = be.analyze(rows, [], cfg_dt)
    got = [r for r in out if r["metric"].endswith("soc_coulomb_pct")][0]
    assert got["status"] == "unavailable"


def test_analyze_malformed_config_is_error():
    rows = [pack_sig(T0, 10.0), pack_sig(T0 + HOUR, 10.0)]
    out = be.analyze(rows, [], {"electrical": "nope"})
    assert all(r["status"] == "error" for r in out)
    out = be.analyze(rows, [], cal(capacity_ah=-5.0))
    assert all(r["status"] == "error" for r in out)
    out = be.analyze(rows, [], {"window_start_ns": "x"})
    assert all(r["status"] == "error" for r in out)
    out = be.analyze(rows, [], {"electrical": {"current_sign": 1}})
    assert all(r["status"] == "error" for r in out)


def test_circuit_flat_ocv_is_unobservable():
    rows = circuit_rows()
    flat_cfg = circuit_cfg(
        ocv_curve=[[0.0, 419.91], [1.0, 419.93]],
        ocv_version="ocv-flat")
    out = be.analyze(rows, [], flat_cfg)
    ocv = {r["metric"]: r for r in out}[
        "battery.electrical.soc_ocv_pct"]
    assert ocv["status"] == "unavailable" and "flat_ocv" in ocv["reason"]
    out = be.analyze(rows, [], circuit_cfg(
        ocv_curve=[[0.0, 3.7], [0.5, 3.6]], ocv_version="ocv-bad"))
    assert {r["metric"]: r for r in out}[
        "battery.electrical.soc_ocv_pct"]["status"] == "error"


def test_anchor_and_gap_cannot_supply_later_window():
    rows = circuit_rows(with_barrier=("PackCurrent", 30))
    output = {r["metric"]: r for r in be.analyze(
        rows, [], circuit_cfg(window_start_ns=ANCHOR + HOUR))}
    for metric in ("soc_coulomb_pct", "soc_ekf_pct"):
        assert output["battery.electrical." + metric]["value"] is None
    shifted = circuit_cfg(initial_soc_time_ns=ANCHOR + 1)
    output = {r["metric"]: r for r in be.analyze(circuit_rows(), [], shifted)}
    assert output["battery.electrical.soc_coulomb_pct"]["value"] is None
    rest_tail = [(T0 + 540 * 10**9, 0.0), (T0 + 600 * 10**9, 0.0)]
    assert not be.is_resting_at(rest_tail, T0 + 600 * 10**9, 600, 2, HOUR)


def test_ica_uses_requested_charge_window():
    output = {r["metric"]: r for r in be.analyze(circuit_rows(), [],
        circuit_cfg(window_start_ns=ANCHOR,
                    window_end_ns=ANCHOR + HOUR - 1))}
    result = output["battery.electrical.ica_peak_dqdv_ah_per_v"]
    assert result["status"] == "derived"
    assert abs(result["value"] - 1.0) < 1e-8


if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_electrical: ok (%d tests)" % len(names))
