#!/usr/bin/env python3
"""Focused stdlib tests for scripts/battery_energy.py. Plain asserts,
stdlib only; doubles as __main__ runner. All circuits/meters are
synthetic (supplied scoped PackCurrent/PackVoltage calibration), never
Tesla pack truth.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import battery_common as bc
import battery_energy as en

HOUR = 3600000000000
T0 = 1700000000000000000
SCOPE = ("v", "fleet", "e1")


def sig(ts, field="PackCurrent", num=1.0, unit=None, quality="unit_unverified",
        vehicle="v", source="fleet", epoch="e1", ingest=None, text=None):
    return {"event_time_ns": ts, "ingest_time_ns": ingest, "vehicle": vehicle,
            "source": source, "decode_epoch": epoch, "path": "P",
            "source_field": field, "value_num": num, "value_text": text,
            "value_bool": None, "unit": unit, "quality": quality,
            "envelope_id": None, "config_version": None,
            "connectivity": None}


def pack_pair(ts, amps, volts=400.0, **kw):
    return [sig(ts, field="PackCurrent", num=amps, unit=None,
                quality="unit_unverified", **kw),
            sig(ts, field="PackVoltage", num=volts, unit=None,
                quality="unit_unverified", **kw)]


def cal(**over):
    base = {"current_sign": "positive_charge",
            "field_calibration": {
                "PackCurrent": {
                    "vehicle": "v", "source": "fleet",
                    "decode_epoch": "e1", "declared_domain": "synthetic",
                    "version": "pack-i-1", "unit": "A", "unit_scale": 1.0,
                    "unit_offset": 0.0},
                "PackVoltage": {
                    "vehicle": "v", "source": "fleet",
                    "decode_epoch": "e1", "declared_domain": "synthetic",
                    "version": "pack-v-1", "unit": "V", "unit_scale": 1.0,
                    "unit_offset": 0.0}},
            "reference": {"version": "ref-1", "domain": "synthetic",
                          "energy_kwh": 100.0, "charge_ah": 100.0},
            "domain": "synthetic"}
    base.update(over)
    return {"energy": base}


def by(rows, metric):
    got = [r for r in rows if r["metric"] == metric]
    assert got, (metric, rows)
    return got


def one(rows, metric):
    got = by(rows, metric)
    assert len(got) == 1, (metric, len(got))
    return got[0]


def test_vi_known_answer_charge_then_discharge():
    # 400 V * 10 A for 1 h -> 4 kWh charge; 400 V * -10 A for 0.5 h
    # -> 2 kWh discharge magnitude.
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    rows += pack_pair(T0 + 2 * HOUR, -10.0)
    rows += pack_pair(T0 + 2 * HOUR + HOUR // 2, -10.0)
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    chg = one(out, "battery.energy.vi_charge_energy_kwh")
    dis = one(out, "battery.energy.vi_discharge_energy_kwh")
    assert chg["status"] == "derived" and abs(chg["value"] - 4.0) < 1e-9
    assert dis["status"] == "derived" and abs(dis["value"] - 2.0) < 1e-9
    assert chg["unit"] == "kWh" and chg["coverage_ratio"] == 1.0


def test_ah_known_answer_and_kwh_independent():
    # 10 A for 1 h -> 10 Ah; voltage faults must not move Ah.
    rows = pack_pair(T0, 10.0, volts=400.0)
    rows += pack_pair(T0 + HOUR, 10.0, volts=400.0)
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    chg = one(out, "battery.energy.charge_throughput_ah")
    assert chg["status"] == "derived" and abs(chg["value"] - 10.0) < 1e-9
    assert chg["unit"] == "Ah"
    rows = [sig(T0, field="PackCurrent", num=10.0),
            sig(T0 + HOUR, field="PackCurrent", num=10.0)]
    rows[0]["unit"] = None
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    chg = one(out, "battery.energy.charge_throughput_ah")
    assert chg["status"] == "derived" and abs(chg["value"] - 10.0) < 1e-9
    vi = one(out, "battery.energy.vi_charge_energy_kwh")
    assert vi["status"] == "unavailable"  # no voltage: Ah stands alone


def test_no_batterycurrent_substitute():
    # BatteryCurrent rows are a different field; Pack-calibrated config
    # must not silently read them as PackCurrent.
    rows = [sig(T0, field="BatteryCurrent", num=10.0, unit="A",
                quality=None),
            sig(T0 + HOUR, field="BatteryCurrent", num=10.0, unit="A",
                quality=None)]
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    assert one(out, "battery.energy.vi_charge_energy_kwh")[
        "status"] == "unavailable"
    assert one(out, "battery.energy.charge_throughput_ah")[
        "status"] == "unavailable"


def test_scope_mismatch_refuses_cross_scope():
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    cfg = cal(max_gap_ns=HOUR)
    cfg["energy"]["field_calibration"]["PackCurrent"]["vehicle"] = "other"
    out = en.analyze(rows, [], cfg)
    got = one(out, "battery.energy.charge_throughput_ah")
    assert got["status"] == "unavailable" and "scope_mismatch" in got["reason"]
    got = one(out, "battery.energy.vi_charge_energy_kwh")
    assert got["status"] == "unavailable" and "scope_mismatch" in got["reason"]


def test_malformed_calibration_is_error():
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    cfg = cal(max_gap_ns=HOUR)
    cfg["energy"]["field_calibration"]["PackCurrent"] = {"unit": "A"}
    out = en.analyze(rows, [], cfg)
    for metric in ("battery.energy.vi_charge_energy_kwh",
                   "battery.energy.charge_throughput_ah"):
        assert one(out, metric)["status"] == "error"


def test_counter_known_answer_and_reset_refusal():
    base = T0
    dc = [sig(base, field="DCChargingEnergyIn", num=100.0, unit="kWh",
              quality=None),
          sig(base + HOUR, field="DCChargingEnergyIn", num=108.0,
              unit="kWh", quality=None)]
    ac = [sig(base, field="ACChargingEnergyIn", num=50.0, unit="kWh",
              quality=None),
          sig(base + HOUR, field="ACChargingEnergyIn", num=58.8,
              unit="kWh", quality=None)]
    dis = [sig(base, field="LifetimeEnergyUsed", num=200.0, unit="kWh",
               quality=None),
           sig(base + HOUR, field="LifetimeEnergyUsed", num=208.2,
               unit="kWh", quality=None)]
    out = en.analyze(dc + ac + dis, [], {"energy": {}})
    assert abs(one(out, "battery.energy.dc_charging_energy_in_kwh")["value"]
               - 8.0) < 1e-9
    assert abs(one(out, "battery.energy.ac_charging_energy_in_kwh")["value"]
               - 8.8) < 1e-9
    assert abs(one(out, "battery.energy.discharge_energy_kwh")["value"]
               - 8.2) < 1e-9
    # DC and AC stay distinct: never summed into one window total.
    vals = {r["metric"]: r["value"] for r in out
            if r["metric"] in ("battery.energy.dc_charging_energy_in_kwh",
                               "battery.energy.ac_charging_energy_in_kwh")}
    assert vals["battery.energy.dc_charging_energy_in_kwh"] != \
        vals["battery.energy.ac_charging_energy_in_kwh"]
    # Reset refuses a false delta.
    dc_bad = [sig(base, field="DCChargingEnergyIn", num=108.0, unit="kWh",
                  quality=None),
              sig(base + HOUR, field="DCChargingEnergyIn", num=100.0,
                  unit="kWh", quality=None)]
    out = en.analyze(dc_bad, [], {"energy": {}})
    bad = one(out, "battery.energy.dc_charging_energy_in_kwh")
    assert bad["status"] == "unavailable" and bad["reason"].startswith(
        "reset:")
    # Ambiguous same-stamp conflict refuses.
    dc_amb = [sig(base, field="DCChargingEnergyIn", num=100.0, unit="kWh",
                  quality=None),
              sig(base, field="DCChargingEnergyIn", num=101.0, unit="kWh",
                  quality=None),
              sig(base + HOUR, field="DCChargingEnergyIn", num=108.0,
                  unit="kWh", quality=None)]
    out = en.analyze(dc_amb, [], {"energy": {}})
    bad = one(out, "battery.energy.dc_charging_energy_in_kwh")
    assert bad["status"] == "unavailable"
    assert bad["reason"].startswith("ambiguous:")


def test_session_meter_gain_known_answer():
    t1, t2 = T0, T0 + HOUR
    rows = [sig(t1, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(t2, field="ChargeEnergyAdded", num=13.5, unit="kWh",
                quality=None)]
    out = en.analyze(rows, [], {"energy": {}})
    sess = by(out, "battery.energy.charge_session_energy_kwh")
    assert len(sess) == 1
    assert sess[0]["status"] == "derived"
    assert abs(sess[0]["value"] - 3.5) < 1e-9
    assert sess[0]["episode_id"] is not None
    assert sess[0]["analysis_id"].startswith("battery_energy:session:")
    assert sess[0]["analysis_id"].endswith(sess[0]["episode_id"])
    assert "start_ns=" in (sess[0]["value_text"] or "")


def test_session_per_session_ids_no_collision():
    t1, t2, t3, t4 = T0, T0 + HOUR, T0 + 4 * HOUR, T0 + 5 * HOUR
    rows = [sig(t1, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(t2, field="ChargeEnergyAdded", num=13.0, unit="kWh",
                quality=None),
            sig(t3, field="ChargeEnergyAdded", num=20.0, unit="kWh",
                quality=None),
            sig(t4, field="ChargeEnergyAdded", num=24.0, unit="kWh",
                quality=None)]
    rows.append(sig(t2, field="DetailedChargeState", num=None, unit=None,
                    quality=None, text="DetailedChargeStateComplete"))
    rows.append(sig(t3, field="DetailedChargeState", num=None, unit=None,
                    quality=None, text="DetailedChargeStateCharging"))
    out = en.analyze(rows, [], {"energy": {"max_gap_ns": 2 * HOUR}})
    sess = by(out, "battery.energy.charge_session_energy_kwh")
    assert len(sess) == 2
    aids = {r["analysis_id"] for r in sess}
    assert len(aids) == 2 and all(a.startswith("battery_energy:session:")
                                  for a in aids)
    vals = sorted(r["value"] for r in sess)
    assert abs(vals[0] - 3.0) < 1e-9 and abs(vals[1] - 4.0) < 1e-9


def test_session_starts_before_window_preserved():
    ws, we = T0 + 2 * HOUR, T0 + 3 * HOUR
    rows = [sig(T0, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(T0 + HOUR, field="ChargeEnergyAdded", num=12.0,
                unit="kWh", quality=None),
            sig(T0 + 2 * HOUR + 1, field="ChargeEnergyAdded", num=12.5,
                unit="kWh", quality=None),
            sig(T0 + 3 * HOUR, field="ChargeEnergyAdded", num=13.0,
                unit="kWh", quality=None)]
    cfg = {"energy": {}, "window_start_ns": ws, "window_end_ns": we}
    out = en.analyze(rows, [], cfg)
    sess = by(out, "battery.energy.charge_session_energy_kwh")
    assert len(sess) == 1
    assert sess[0]["status"] == "unavailable"
    assert "incomplete_boundary" in sess[0]["reason"]
    assert sess[0]["window_start_ns"] == ws
    # Counter with fewer than two in-window samples refuses a delta
    # even though retained context exists outside the window.
    counters = [sig(T0, field="DCChargingEnergyIn", num=100.0, unit="kWh",
                    quality=None),
                sig(we, field="DCChargingEnergyIn", num=104.0, unit="kWh",
                    quality=None)]
    out = en.analyze(counters, [], cfg)
    dc = one(out, "battery.energy.dc_charging_energy_in_kwh")
    assert dc["status"] == "unavailable" and "sparse" in dc["reason"]


def test_session_text_change_and_reset_boundaries():
    t1, t2, t3 = T0, T0 + HOUR, T0 + 2 * HOUR
    rows = [sig(t1, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(t2, field="ChargeEnergyAdded", num=12.0, unit="kWh",
                quality=None),
            sig(t3, field="ChargeEnergyAdded", num=11.0, unit="kWh",
                quality=None)]
    out = en.analyze(rows, [], {"energy": {}})
    sess = by(out, "battery.energy.charge_session_energy_kwh")
    assert sess and all(s["status"] == "unavailable" for s in sess)
    assert any(s["reason"].startswith("reset:") for s in sess)


def test_vi_gap_and_invalid_reject_partial_totals():
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    rows += pack_pair(T0 + 5 * HOUR, 10.0)
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    got = one(out, "battery.energy.vi_charge_energy_kwh")
    assert got["status"] == "unavailable"
    assert "incomplete_window" in got["reason"]
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    rows.append(sig(T0 + HOUR // 2, field="PackVoltage", num=None,
                    unit=None, quality="invalid"))
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    got = one(out, "battery.energy.vi_charge_energy_kwh")
    assert got["status"] == "unavailable"
    # Order/duplicates do not change the reduction.
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    fwd = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    rev = en.analyze(list(reversed(rows)), [], cal(max_gap_ns=HOUR))
    assert one(fwd, "battery.energy.vi_charge_energy_kwh")["value"] == \
        one(rev, "battery.energy.vi_charge_energy_kwh")["value"]
    dup = rows + [dict(rows[0])]
    out = en.analyze(dup, [], cal(max_gap_ns=HOUR))
    assert one(out, "battery.energy.vi_charge_energy_kwh")["value"] == \
        one(fwd, "battery.energy.vi_charge_energy_kwh")["value"]


def test_efc_fixed_reference_math_and_single_domain():
    assert en.efc_cycles(3.0, 8.0, 100.0) == {"oneway": 0.08,
                                             "bidirectional": 0.055}
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    rows += pack_pair(T0 + 2 * HOUR, -10.0)
    rows += pack_pair(T0 + 2 * HOUR + HOUR // 2, -10.0)
    out = en.analyze(rows, [], cal(max_gap_ns=HOUR))
    oneway = one(out, "battery.energy.efc_oneway_cycles")
    both = one(out, "battery.energy.efc_bidirectional_cycles")
    # Ah domain wins when both Ah and counters exist: 10 Ah charge,
    # 5 Ah discharge over a 100 Ah reference.
    assert oneway["status"] == "derived"
    assert abs(oneway["value"] - 0.05) < 1e-9
    assert abs(both["value"] - 0.075) < 1e-9
    assert "ah_throughput" in oneway["reason"]
    # Counters alone (no V/A): DC delta 8, discharge delta 8 over
    # 100 kWh reference gives 0.08 one-way, 0.08 bidirectional.
    dc = [sig(T0, field="DCChargingEnergyIn", num=100.0, unit="kWh",
              quality=None),
          sig(T0 + HOUR, field="DCChargingEnergyIn", num=108.0,
              unit="kWh", quality=None)]
    dis = [sig(T0, field="LifetimeEnergyUsed", num=200.0, unit="kWh",
               quality=None),
           sig(T0 + HOUR, field="LifetimeEnergyUsed", num=208.0,
               unit="kWh", quality=None)]
    cfg = {"energy": {"reference": {"version": "ref-1",
                                    "domain": "synthetic",
                                    "energy_kwh": 100.0},
                      "domain": "synthetic"}}
    out = en.analyze(dc + dis, [], cfg)
    assert abs(one(out, "battery.energy.efc_oneway_cycles")["value"]
               - 0.08) < 1e-9
    assert abs(one(out, "battery.energy.efc_bidirectional_cycles")[
        "value"] - 0.08) < 1e-9
    # Reference absent: unavailable, never zero.
    out = en.analyze(dc + dis, [], {"energy": {}})
    assert one(out, "battery.energy.efc_oneway_cycles")[
        "status"] == "unavailable"
    assert "reference_absent" in one(out, "battery.energy.efc_oneway_cycles")[
        "reason"]


def test_interval_capacity_known_answer_and_span_gates():
    # Session delta 5 kWh over SOC 40 -> 60% gives 25 kWh equivalent.
    t1, t2 = T0, T0 + HOUR
    rows = [sig(t1, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(t2, field="ChargeEnergyAdded", num=15.0, unit="kWh",
                quality=None),
            sig(t1, field="Soc", num=40.0, unit="%", quality=None),
            sig(t2, field="Soc", num=60.0, unit="%", quality=None)]
    out = en.analyze(rows, [], {"energy": {}})
    cap = one(out, "battery.energy.interval_capacity_kwh")
    assert cap["status"] == "derived" and abs(cap["value"] - 25.0) < 1e-9
    assert cap["uncertainty"] is None  # no supplied uncertainty
    # With supplied stds the uncertainty is propagated mathematically.
    cfg = {"energy": {"soc_uncertainty_pct": 0.5,
                      "energy_uncertainty_kwh": 0.1}}
    out = en.analyze(rows, [], cfg)
    cap = one(out, "battery.energy.interval_capacity_kwh")
    assert cap["uncertainty"] is not None and cap["uncertainty"] > 0.0
    assert cap["uncertainty_lower"] is not None
    # Zero span refuses.
    rows = [sig(t1, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(t2, field="ChargeEnergyAdded", num=15.0, unit="kWh",
                quality=None),
            sig(t1, field="Soc", num=50.0, unit="%", quality=None),
            sig(t2, field="Soc", num=50.0, unit="%", quality=None)]
    out = en.analyze(rows, [], {"energy": {}})
    cap = one(out, "battery.energy.interval_capacity_kwh")
    assert cap["status"] == "unavailable" and "zero" in cap["reason"]
    # Small span below the minimum refuses.
    rows = [sig(t1, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(t2, field="ChargeEnergyAdded", num=11.0, unit="kWh",
                quality=None),
            sig(t1, field="Soc", num=50.0, unit="%", quality=None),
            sig(t2, field="Soc", num=55.0, unit="%", quality=None)]
    out = en.analyze(rows, [], {"energy": {}})
    cap = one(out, "battery.energy.interval_capacity_kwh")
    assert cap["status"] == "unavailable"
    assert "soc_span_too_small" in cap["reason"]


def test_circular_labeled_not_independent():
    t1, t2 = T0, T0 + HOUR
    rows = [sig(t1, field="EnergyRemaining", num=20.0, unit="kWh",
                quality=None),
            sig(t2, field="EnergyRemaining", num=22.0, unit="kWh",
                quality=None),
            sig(t1, field="Soc", num=40.0, unit="%", quality=None),
            sig(t2, field="Soc", num=60.0, unit="%", quality=None)]
    out = en.analyze(rows, [], {"energy": {}})
    circ = one(out, "battery.energy.bms_circular_capacity_kwh")
    assert circ["status"] == "derived" and abs(circ["value"] - 10.0) < 1e-9
    assert "circular" in circ["reason"]


def test_trend_soh_like_for_like_only():
    t1, t2 = T0, T0 + HOUR
    rows = [sig(t1, field="ChargeEnergyAdded", num=10.0, unit="kWh",
                quality=None),
            sig(t2, field="ChargeEnergyAdded", num=15.0, unit="kWh",
                quality=None),
            sig(t1, field="Soc", num=40.0, unit="%", quality=None),
            sig(t2, field="Soc", num=60.0, unit="%", quality=None)]
    cfg = cal()
    out = en.analyze(rows, [], cfg)
    # Interval 25 kWh vs 100 kWh reference: trend -75, SOH 25.
    trend = one(out, "battery.energy.capacity_trend_kwh")
    soh = one(out, "battery.energy.soh_pct")
    assert trend["status"] == "derived" and abs(trend["value"] + 75.0) < 1e-9
    assert soh["status"] == "derived" and abs(soh["value"] - 25.0) < 1e-9
    assert soh["unit"] == "%"
    # Domain mismatch refuses the like-for-like claim.
    cfg2 = cal(domain="other")
    out = en.analyze(rows, [], cfg2)
    assert "incomparable_domain" in one(
        out, "battery.energy.soh_pct")["reason"]
    # Conditions mismatch refuses when the reference states conditions.
    cfg3 = cal(conditions="cold")
    cfg3["energy"]["reference"]["conditions"] = "warm"
    out = en.analyze(rows, [], cfg3)
    assert "incomparable_conditions" in one(
        out, "battery.energy.soh_pct")["reason"]
    # No reference: unavailable, never an initial-100% trick.
    out = en.analyze(rows, [], {"energy": {}})
    assert one(out, "battery.energy.soh_pct")["status"] == "unavailable"


def test_empty_config_yields_unavailable_not_empty():
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    out = en.analyze(rows, [], {})
    assert len(out) == len(en.SUPPORTED_METRICS)
    physical = [r for r in out if r["metric"] in (
        "battery.energy.vi_charge_energy_kwh",
        "battery.energy.charge_throughput_ah",
        "battery.energy.efc_oneway_cycles",
        "battery.energy.interval_capacity_kwh",
        "battery.energy.soh_pct")]
    assert physical and all(r["status"] == "unavailable" for r in physical)
    assert all(r["value"] is None for r in out)


def test_scope_isolation_and_malformed_windows():
    rows = pack_pair(T0, 10.0, vehicle="a") + pack_pair(T0 + HOUR, 10.0,
                                                       vehicle="a")
    rows += pack_pair(T0, 20.0, vehicle="b") + pack_pair(
        T0 + HOUR, 20.0, vehicle="b")
    cfg = {"energy": {
        "current_sign": "positive_charge",
        "field_calibration": {
            "PackCurrent": [
                {"vehicle": "a", "source": "fleet", "decode_epoch": "e1",
                 "declared_domain": "synthetic", "version": "i-a",
                 "unit": "A", "unit_scale": 1.0, "unit_offset": 0.0},
                {"vehicle": "b", "source": "fleet", "decode_epoch": "e1",
                 "declared_domain": "synthetic", "version": "i-b",
                 "unit": "A", "unit_scale": 1.0, "unit_offset": 0.0}],
            "PackVoltage": [
                {"vehicle": "a", "source": "fleet", "decode_epoch": "e1",
                 "declared_domain": "synthetic", "version": "v-a",
                 "unit": "V", "unit_scale": 1.0, "unit_offset": 0.0},
                {"vehicle": "b", "source": "fleet", "decode_epoch": "e1",
                 "declared_domain": "synthetic", "version": "v-b",
                 "unit": "V", "unit_scale": 1.0, "unit_offset": 0.0}]},
        "max_gap_ns": HOUR}}
    out = en.analyze(rows, [], cfg)
    vals = {(r["vehicle"], r["metric"]): r["value"] for r in out
            if r["status"] == "derived"
            and r["metric"] == "battery.energy.charge_throughput_ah"}
    assert abs(vals[("a", "battery.energy.charge_throughput_ah")]
               - 10.0) < 1e-9
    assert abs(vals[("b", "battery.energy.charge_throughput_ah")]
               - 20.0) < 1e-9
    out = en.analyze(rows, [], {"window_start_ns": "x"})
    assert all(r["status"] == "error" for r in out)
    out = en.analyze(rows, [], {"window_start_ns": 5,
                                "window_end_ns": 4})
    assert all(r["status"] == "error" for r in out)


def test_decision_time_and_run_contract():
    rows = pack_pair(T0, 10.0) + pack_pair(T0 + HOUR, 10.0)
    cfg = cal(max_gap_ns=HOUR)
    cfg["decision_time_ns"] = T0 + HOUR // 2
    out = en.analyze(rows, [], cfg)
    assert one(out, "battery.energy.charge_throughput_ah")[
        "status"] == "unavailable"
    out = bc.run_analyses(pack_pair(T0, 10.0)
                          + pack_pair(T0 + HOUR, 10.0), [],
                          cal(max_gap_ns=HOUR), [en.analyze])
    assert out and all(isinstance(r, dict) for r in out)
    aids = {r["analysis_id"] for r in out
            if r["metric"] == "battery.energy.charge_session_energy_kwh"}
    assert aids and all(a.startswith("battery_energy:session:")
                        for a in aids)


if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_energy: ok (%d tests)" % len(names))
