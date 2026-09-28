#!/usr/bin/env python3
"""Calibrated electrical analyses: Coulomb SOC, OCV/EKF SOC, apparent DC
resistance, ICA/DVA. Standard library only.

Config contract (config is a plain dict):
  config["window_start_ns"] / ["window_end_ns"] / ["decision_time_ns"]:
    optional integer ns; PRESENT invalid values yield per-metric error
    rows. Online (decision_time_ns set): only rows with real
    decision_time are admitted; unknown-ingest rows are excluded (counts
    appended to reasons). Offline (absent): all retained rows admitted.
  config["electrical"]: plain dict, all physical calibration lives here:
    "calibration_version": non-empty string, required whenever any
      physical calibration below is supplied (capacity, sign, initial,
      ocv_curve, ekf, field_units).
    "domain": non-empty string describing the calibration scope
      (for example "synthetic-circuit" or a pack/chemistry identifier).
      Required together with calibration_version. Never globally infer
      pack chemistry, capacity, or current sign.
    "calibration_scope": dict with optional vehicle/source/decode_epoch
      strings binding the calibration to one scope (absent keys are
      wildcards). Required whenever physical calibration is supplied:
      a missing binding, or a scope that does not match, yields
      unavailable rows (never silently applied cross-scope). Pure
      offline math helpers take calibration as arguments and are
      unaffected by scope.
    Coulomb counting:
      "capacity_ah": finite > 0 (calibrated amp-hour capacity).
      "current_sign": exactly 1 or -1 (int, never bool). +1 means a
        positive calibrated current sample means charging (SOC rises);
        -1 means positive means discharging (SOC falls). Explicit always.
      "initial_soc_pct": finite 0..100, anchored at "initial_soc_time_ns"
        (integer ns, required with Coulomb calibration). The anchor binds
        the state to a timestamp: analyze integrates retained contiguous
        history from the first usable current sample at/after the anchor
        (within max_gap_ns, no invalid barrier between) through the
        requested window end. The anchor is never re-applied at a later
        window start; a missing anchor, an anchor outside available
        history, or a barrier/gap before the first sample yields
        unavailable, never a reset estimate. Estimates carry
        ";observation_time_ns=<last integrated sample>" in reason.
      "coulombic_efficiency": finite in (0, 1], default 1.0. Applied only
        to charging legs (charge current > 0); discharging legs use 1.0.
      "current_std_a": finite >= 0, optional. When supplied (with optional
        "initial_soc_uncertainty_pct" / "capacity_ah_uncertainty" for
        documentation only), SOC uncertainty is propagated; otherwise no
        uncertainty is emitted (never fabricated).
      "initial_soc_uncertainty_pct": finite >= 0, optional.
      "max_gap_ns": non-negative int, default 600000000000 (10 min).
        Any leg wider than this invalidates continuity: samples after the
        gap report no SOC (never forward-filled). Contradictory values
        at one timestamp are ambiguous and likewise truncate continuity.
    Current/voltage field selection (official Fleet source_field names,
    never VSS path guesses): "current_fields" default ["PackCurrent"],
    "voltage_fields" default ["PackVoltage"]. Non-Fleet alternatives only
    via explicit mapping here. Raw Pack rows carry unit NULL / quality
    unit_unverified and require "field_units" plus top-level
    calibration_version/domain to become physical; without it they are
    barriers (uncalibrated), never silent zeros. A sample is usable as
    physical only when its unit is calibrated: unit == "A" for current,
    "V" for voltage. "field_units": for example {"PackCurrent": "A",
    "PackVoltage": "V"}.
    OCV / resting:
      "ocv_curve": list of [soc01, volts] pairs (or dicts with soc/soc01
        and v/voltage_v/ocv_v). soc01 in [0, 1] strictly increasing,
        volts finite > 0 strictly increasing (monotonic validation).
      "ocv_version": non-empty string, required with ocv_curve.
      "ocv_min_dv_dsoc_v": finite > 0, default 0.05. Local slope below
        this yields no inverse (flat, unobservable) rather than a number.
      "rest_seconds": finite > 0, default 1800.0.
      "rest_current_threshold_a": finite >= 0, default 2.0.
    1RC EKF:
      "ekf": dict with "r0_ohm" (>0), "r1_ohm" (>0), "c1_f" (>0),
        "q_soc_per_s" (>0, SOC^2 per second), "q_v1_per_s" (>0, V^2/s),
        "r_v" (>0, V^2 measurement variance), "init_soc_pct" (0..100)
        anchored at "init_time_ns" (integer ns, required with ekf),
        "init_v1_v" (finite), "init_p_soc" (>=0), "init_p_v1" (>=0).
        Like Coulomb: retained contiguous history from the first usable
        synchronized step at/after init_time_ns through the window end;
        never re-applied at a later window start; missing/outside-history
        anchor or barrier/gap yields unavailable. Enforces the Coulomb
        max_gap_ns leg bound on steps (sorted order alone is not enough).
        Estimates carry ";observation_time_ns=<last step>" in reason.
      "ekf_version": non-empty string, required with "ekf".
      "ekf_min_dv_dsoc_v": finite > 0, default 0.05 (flat flag).
    DCR comparability: "dcr_soc_field" (default "Soc", one explicit SOC
      field, never a merged Soc/BatteryLevel series); "dcr_temp_fields"
      (default ["ModuleTempMin", "ModuleTempMax"], calibrated battery
      temperature only via "dcr_temp_units", never InsideTemp/
      OutsideTemp cabin/ambient fallback). Missing battery temperature or
      SOC yields an explicit unavailable comparability reason (or a
      distinct unconditioned apparent-resistance diagnostic), never a
      claim of matched conditions.
    Apparent DC resistance (labelled apparent everywhere; never EIS or
    true cell resistance):
      "dcr": optional dict with "min_delta_a" (default 5.0, >0),
        "max_skew_ns" (default 30000000000), "min_step_ns" (default
        10000000000), "max_step_ns" (default 60000000000),
        "max_soc_change" (SOC 0..1 fraction, default 0.02),
        "max_temp_change_c" (default 5.0).
    ICA/DVA:
      "ica": optional dict with "min_span_v" (default 0.05, >0),
        "max_gap_ns" (default 600000000000), "smooth_window" (int >= 1,
        default 1 = none), "cc_tolerance_a" (default 2.0, >=0),
        "min_mean_current_a" (default 1.0, >=0).

Units: helpers assume calibrated A, V, Ah, s/ns as documented; analyze()
gates on units before calling helpers. Current sign is always explicit.

Metrics (namespace battery.electrical.*):
  soc_coulomb_pct, soc_ocv_pct, soc_ekf_pct (unit "%", status estimated
    when a value is produced, else unavailable/error),
  resistance_apparent_ohm (unit "ohm", derived; reason always notes
    apparent, not EIS/true cell),
  ica_peak_voltage_v ("V"), ica_peak_dqdv_ah_per_v ("Ah/V"),
  dva_peak_capacity_ah ("Ah"), dva_peak_dvdq_v_per_ah ("V/Ah")
  (derived ICA/DVA scalar features; full curves come from the plain
  helper ica_dva_curve, not from Grafana rows).
"""

import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import battery_common as bc

ALGORITHM_VERSION = "1.1.0"
ANALYSIS_ID = "battery_electrical"

SUPPORTED_METRICS = (
    "battery.electrical.soc_coulomb_pct",
    "battery.electrical.soc_ocv_pct",
    "battery.electrical.soc_ekf_pct",
    "battery.electrical.resistance_apparent_ohm",
    "battery.electrical.ica_peak_voltage_v",
    "battery.electrical.ica_peak_dqdv_ah_per_v",
    "battery.electrical.dva_peak_capacity_ah",
    "battery.electrical.dva_peak_dvdq_v_per_ah",
)

DEFAULT_MAX_GAP_NS = 600000000000
DEFAULT_DCR = {"min_delta_a": 5.0, "max_skew_ns": 30000000000,
               "min_step_ns": 10000000000, "max_step_ns": 60000000000,
               "max_soc_change": 0.02, "max_temp_change_c": 5.0}
DEFAULT_ICA = {"min_span_v": 0.05, "max_gap_ns": 600000000000,
               "smooth_window": 1, "cc_tolerance_a": 2.0,
               "min_mean_current_a": 1.0}


class ElectricalError(ValueError):
    pass


def _finite(value):
    return bc.safe_float(value)


def _check_sign(sign):
    if isinstance(sign, bool) or sign not in (1, -1):
        raise ElectricalError("charge_sign must be exactly 1 or -1")
    return sign


def _require_version_domain(ecfg, what):
    ver = ecfg.get("calibration_version")
    dom = ecfg.get("domain")
    if not isinstance(ver, str) or not ver:
        raise ElectricalError("malformed: %s needs calibration_version" % what)
    if not isinstance(dom, str) or not dom:
        raise ElectricalError("malformed: %s needs domain" % what)
    return ver, dom


def coulomb_soc_series(pairs, capacity_ah, initial_soc01, charge_sign,
                       efficiency=1.0, max_gap_ns=DEFAULT_MAX_GAP_NS,
                       current_std_a=None, initial_std01=None):
    """Integrate calibrated current into SOC (fraction 0..1).

    pairs: sorted-or-unsorted [(event_time_ns, amps_calibrated)].
    charge_sign: +1 positive means charging, -1 positive means discharge.
    efficiency in (0, 1] applies to charging legs only.
    Legs wider than max_gap_ns invalidate continuity: that leg and every
    later sample yield None (never filled). Identical duplicate timestamps
    share one anchor/step SOC row each; contradictory same-time values
    invalidate continuity as ambiguous (never order-dependent).
    SOC clamps to [0, 1] with a saturated flag.
    Uncertainty propagates only from supplied current_std_a/initial_std01
    (trapezoidal weights with shared-sample covariance across adjacent
    legs); otherwise std is None.
    Returns dict with soc list [(ns, soc01|None)], gaps, ambiguous,
    legs_used, legs_rejected, saturated, std01, final_soc01.
    """
    cap = _finite(capacity_ah)
    if cap is None or cap <= 0.0:
        raise ElectricalError("capacity_ah must be finite > 0")
    if _finite(initial_soc01) is None or not 0.0 <= initial_soc01 <= 1.0:
        raise ElectricalError("initial_soc01 must be finite in [0, 1]")
    _check_sign(charge_sign)
    eff = _finite(efficiency)
    if eff is None or not 0.0 < eff <= 1.0:
        raise ElectricalError("efficiency must be finite in (0, 1]")
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise ElectricalError("max_gap_ns must be a non-negative int")
    var_i = None
    if current_std_a is not None:
        conv = _finite(current_std_a)
        if conv is None or conv < 0.0:
            raise ElectricalError("current_std_a must be finite >= 0")
        var_i = conv * conv
    var = None
    if initial_std01 is not None:
        conv = _finite(initial_std01)
        if conv is None or conv < 0.0:
            raise ElectricalError("initial_std01 must be finite >= 0")
        var = conv * conv
    elif var_i is not None:
        var = 0.0
    clean = []
    for item in pairs or []:
        try:
            t_ns, amps = item
        except (TypeError, ValueError):
            raise ElectricalError("pairs must hold (ns, amps)")
        if bc.to_ns(t_ns) is None:
            raise ElectricalError("pairs hold bad timestamp")
        aval = _finite(amps)
        if aval is None:
            raise ElectricalError("pairs hold non-finite amps")
        clean.append((t_ns, aval))
    clean.sort(key=lambda p: p[0])
    if not clean:
        return {"soc": [], "gaps": 0, "ambiguous": 0, "legs_used": 0,
                "legs_rejected": 0, "saturated": False,
                "std01": None if var is None else math.sqrt(var),
                "final_soc01": None}
    # Group by timestamp: one output row per input sample; identical
    # duplicates share the anchor/step SOC, contradictory values are
    # ambiguous and invalidate continuity (never order-dependent).
    groups = []
    for t_ns, aval in clean:
        if groups and groups[-1][0] == t_ns:
            groups[-1][1].append(aval)
        else:
            groups.append((t_ns, [aval]))
    soc = float(initial_soc01)
    saturated = False
    out = []
    gaps = 0
    ambiguous = 0
    used = 0
    rejected = 0
    broken = False
    coeffs = []
    prev = None
    for t_ns, values in groups:
        if broken:
            out.extend((t_ns, None) for _ in values)
            continue
        if len(set(values)) > 1:
            ambiguous += 1
            rejected += 1
            broken = True
            out.extend((t_ns, None) for _ in values)
            continue
        aval = values[0]
        if prev is None:
            out.extend((t_ns, soc) for _ in values)
            coeffs.append(0.0)
            prev = (t_ns, aval)
            continue
        t0, i0 = prev
        dt_ns = t_ns - t0
        if dt_ns > max_gap_ns:
            gaps += 1
            rejected += 1
            broken = True
            out.extend((t_ns, None) for _ in values)
            continue
        c0 = charge_sign * i0
        c1 = charge_sign * aval
        e0 = eff if c0 > 0.0 else 1.0
        e1 = eff if c1 > 0.0 else 1.0
        dt_h = dt_ns / 3.6e12
        step = (e0 * c0 + e1 * c1) / 2.0 * dt_h / cap
        if not bc.is_finite_number(step):
            rejected += 1
            broken = True
            out.extend((t_ns, None) for _ in values)
            continue
        soc += step
        if soc < 0.0:
            soc = 0.0
            saturated = True
        elif soc > 1.0:
            soc = 1.0
            saturated = True
        # Independent-sample variance: each sample's total coefficient
        # across its adjacent legs (shared middle samples correlate legs).
        if var_i is not None:
            coeffs[-1] += e0 * charge_sign * dt_h / (2.0 * cap)
            coeffs.append(e1 * charge_sign * dt_h / (2.0 * cap))
        else:
            coeffs.append(0.0)
        used += 1
        out.extend((t_ns, soc) for _ in values)
        prev = (t_ns, aval)
    if var is not None and var_i is not None:
        for weight in coeffs:
            var += weight * weight * var_i
    final = None
    for _, val in reversed(out):
        if val is not None:
            final = val
            break
    return {"soc": out, "gaps": gaps, "ambiguous": ambiguous,
            "legs_used": used, "legs_rejected": rejected,
            "saturated": saturated,
            "std01": None if var is None else math.sqrt(var),
            "final_soc01": final}


def validate_ocv_curve(points):
    """Validate a monotonic OCV curve; return cleaned [(soc01, volts)].

    Accepts (soc01, volts) pairs or dicts with soc/soc01 and
    v/voltage_v/ocv_v. Requires >= 2 points, soc01 strictly increasing in
    [0, 1], volts strictly increasing > 0. Raises ElectricalError when
    malformed (including flat/non-monotonic curves).
    """
    if not isinstance(points, (list, tuple)) or len(points) < 2:
        raise ElectricalError("malformed: ocv_curve needs >= 2 points")
    clean = []
    for item in points:
        if isinstance(item, dict):
            soc = item.get("soc", item.get("soc01"))
            vol = item.get("v", item.get("voltage_v", item.get("ocv_v")))
        else:
            try:
                soc, vol = item
            except (TypeError, ValueError):
                raise ElectricalError("malformed: ocv_curve point")
        soc = _finite(soc)
        # Accept percent SOC explicitly only via pct key; bare numbers are
        # fractions. Values in (1, 100] are rejected rather than guessed.
        vol = _finite(vol)
        if soc is None or not 0.0 <= soc <= 1.0:
            raise ElectricalError("malformed: ocv soc01 in [0, 1]")
        if vol is None or vol <= 0.0:
            raise ElectricalError("malformed: ocv volts finite > 0")
        clean.append((soc, vol))
    clean.sort(key=lambda p: p[0])
    for (s0, v0), (s1, v1) in zip(clean, clean[1:]):
        if not s1 > s0 or not v1 > v0:
            raise ElectricalError(
                "malformed: ocv_curve must be strictly increasing")
    return clean


def ocv_from_soc(soc01, curve):
    """Piecewise-linear OCV lookup; None outside the calibrated SOC domain."""
    soc = _finite(soc01)
    if soc is None or not 0.0 <= soc <= 1.0:
        return None
    if soc < curve[0][0] or soc > curve[-1][0]:
        return None
    for (s0, v0), (s1, v1) in zip(curve, curve[1:]):
        if s0 <= soc <= s1:
            if s1 == s0:
                return None
            frac = (soc - s0) / (s1 - s0)
            val = v0 + frac * (v1 - v0)
            return val if bc.is_finite_number(val) else None
    return None


def _ocv_slope_at(soc01, curve):
    for (s0, v0), (s1, v1) in zip(curve, curve[1:]):
        if s0 <= soc01 <= s1:
            span = s1 - s0
            if span <= 0.0:
                return None
            slope = (v1 - v0) / span
            return slope if bc.is_finite_number(slope) else None
    return None


def soc_from_ocv(voltage_v, curve, min_dv_dsoc_v=0.05):
    """Inverse OCV lookup; None outside the voltage domain or on flat
    segments whose local slope is below min_dv_dsoc_v (unobservable)."""
    vol = _finite(voltage_v)
    slope_min = _finite(min_dv_dsoc_v)
    if vol is None or vol <= 0.0:
        return None
    if slope_min is None or slope_min <= 0.0:
        raise ElectricalError("min_dv_dsoc_v must be finite > 0")
    if vol < curve[0][1] or vol > curve[-1][1]:
        return None
    for (s0, v0), (s1, v1) in zip(curve, curve[1:]):
        low, high = (v0, v1) if v1 >= v0 else (v1, v0)
        if low <= vol <= high:
            span_v = v1 - v0
            span_s = s1 - s0
            if span_v <= 0.0 or span_s <= 0.0:
                return None
            slope = span_v / span_s
            if slope < slope_min:
                return None
            soc = s0 + (vol - v0) / slope
            if not 0.0 <= soc <= 1.0 or not bc.is_finite_number(soc):
                return None
            return soc
    return None


def is_resting_at(currents_sorted, end_ns, rest_seconds,
                  threshold_a, max_gap_ns):
    """True when every calibrated current sample in the trailing
    [end_ns - rest_seconds, end_ns] window is within threshold_a and no leg
    exceeds max_gap_ns. currents_sorted: ascending [(ns, amps_charge)]."""
    if bc.to_ns(end_ns) is None:
        return False
    rest = _finite(rest_seconds)
    thr = _finite(threshold_a)
    if rest is None or rest <= 0.0 or thr is None or thr < 0.0:
        raise ElectricalError("rest_seconds > 0 and threshold >= 0")
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise ElectricalError("max_gap_ns must be a non-negative int")
    start = end_ns - int(rest * 1e9)
    prior = [p for p in currents_sorted if p[0] <= start]
    if not prior or start - prior[-1][0] > max_gap_ns:
        return False
    window = [prior[-1]] + [
        p for p in currents_sorted if start < p[0] <= end_ns]
    if len(window) < 2:
        return False
    if end_ns - window[-1][0] > max_gap_ns:
        return False
    for _, amps in window:
        aval = _finite(amps)
        if aval is None or abs(aval) > thr:
            return False
    for (t0, _), (t1, _) in zip(window, window[1:]):
        if t1 - t0 > max_gap_ns:
            return False
    return True


def ekf_1rc_soc(steps, ocv_curve, r0_ohm, r1_ohm, c1_f, capacity_ah,
                charge_sign, efficiency=1.0, q_soc_per_s=1e-9,
                q_v1_per_s=1e-8, r_v=1e-4, init_soc01=0.5, init_v1_v=0.0,
                init_p_soc=1e-2, init_p_v1=1e-2, min_dv_dsoc_v=0.05):
    """Actual 1RC equivalent-circuit EKF over [(ns, amps, volts)].

    Model (charge convention i = charge_sign * measured):
      soc += eff_dir * i * dt / (Q * 3600); v1 += R1 (1 - a) i with
      a = exp(-dt / (R1 C1)); V = OCV(soc) + i R0 + v1.
    H = [dOCV/dsoc, 1]; Q step scales with dt; R is the scalar voltage
    variance. Flat local OCV (slope < min) still updates (SOC gain ~ 0)
    but flags flat; out-of-domain OCV predicts only. SOC clamps to [0, 1]
    with a saturated flag. Returns final state, trace, and counters.
    """
    curve = validate_ocv_curve(ocv_curve)
    for name, val in (("r0_ohm", r0_ohm), ("r1_ohm", r1_ohm),
                      ("c1_f", c1_f), ("capacity_ah", capacity_ah)):
        conv = _finite(val)
        if conv is None or conv <= 0.0:
            raise ElectricalError("%s must be finite > 0" % name)
    r0 = float(r0_ohm)
    r1 = float(r1_ohm)
    c1 = float(c1_f)
    cap = float(capacity_ah)
    _check_sign(charge_sign)
    eff = _finite(efficiency)
    if eff is None or not 0.0 < eff <= 1.0:
        raise ElectricalError("efficiency must be finite in (0, 1]")
    for name, val in (("q_soc_per_s", q_soc_per_s),
                      ("q_v1_per_s", q_v1_per_s), ("r_v", r_v)):
        conv = _finite(val)
        if conv is None or conv <= 0.0:
            raise ElectricalError("%s must be finite > 0" % name)
    if _finite(init_soc01) is None or not 0.0 <= init_soc01 <= 1.0:
        raise ElectricalError("init_soc01 must be finite in [0, 1]")
    if _finite(init_v1_v) is None or _finite(init_p_soc) is None \
            or _finite(init_p_v1) is None or init_p_soc < 0.0 \
            or init_p_v1 < 0.0:
        raise ElectricalError("malformed EKF initial state/covariance")
    slope_min = _finite(min_dv_dsoc_v)
    if slope_min is None or slope_min <= 0.0:
        raise ElectricalError("min_dv_dsoc_v must be finite > 0")
    clean = []
    for item in steps or []:
        try:
            t_ns, amps, volts = item
        except (TypeError, ValueError):
            raise ElectricalError("steps must hold (ns, amps, volts)")
        if bc.to_ns(t_ns) is None:
            raise ElectricalError("steps hold bad timestamp")
        aval, vval = _finite(amps), _finite(volts)
        if aval is None or vval is None or vval <= 0.0:
            raise ElectricalError("steps hold non-finite amps/volts")
        clean.append((t_ns, aval, vval))
    clean.sort(key=lambda p: p[0])
    seen = {}
    for t_ns, aval, vval in clean:
        key = (t_ns, repr(aval), repr(vval))
        if t_ns in seen and seen[t_ns] != key:
            raise ElectricalError("ambiguous same-time steps")
        seen[t_ns] = key
    if len(clean) < 2:
        raise ElectricalError("steps need >= 2 samples")
    tau = r1 * c1
    if not bc.is_finite_number(tau) or tau <= 0.0:
        raise ElectricalError("R1*C1 must be finite > 0")
    soc = float(init_soc01)
    v1 = float(init_v1_v)
    p00, p01, p11 = float(init_p_soc), 0.0, float(init_p_v1)
    saturated = False
    updates = 0
    flat = 0
    out_of_domain = 0
    skipped = 0
    trace = [(clean[0][0], soc, v1, "init")]
    for (t0, _, _), (t1, i_meas, v_meas) in zip(clean, clean[1:]):
        dt_ns = t1 - t0
        if dt_ns <= 0:
            skipped += 1
            trace.append((t1, soc, v1, "unordered_or_duplicate"))
            continue
        dt = dt_ns / 1e9
        ichg = charge_sign * i_meas
        if not bc.is_finite_number(ichg):
            skipped += 1
            trace.append((t1, soc, v1, "non_finite_current"))
            continue
        alpha = math.exp(-dt / tau) if dt / tau < 700.0 else 0.0
        edir = eff if ichg > 0.0 else 1.0
        soc_pred = soc + edir * ichg * dt / (cap * 3600.0)
        v1_pred = alpha * v1 + r1 * (1.0 - alpha) * ichg
        if not bc.is_finite_number(soc_pred) \
                or not bc.is_finite_number(v1_pred):
            skipped += 1
            trace.append((t1, soc, v1, "non_finite_predict"))
            continue
        if soc_pred < 0.0:
            soc_pred = 0.0
            saturated = True
        elif soc_pred > 1.0:
            soc_pred = 1.0
            saturated = True
        f11 = alpha
        q00 = float(q_soc_per_s) * dt
        q11 = float(q_v1_per_s) * dt
        # F = [[1, 0], [0, a]] diagonal: p00 += q00, cross scales by a.
        p00_pred = p00 + q00
        p01_pred = p01 * f11
        p11_pred = f11 * f11 * p11 + q11
        ocv = ocv_from_soc(soc_pred, curve)
        if ocv is None:
            soc, v1, p00, p01, p11 = soc_pred, v1_pred, p00_pred, \
                p01_pred, p11_pred
            out_of_domain += 1
            trace.append((t1, soc, v1, "out_of_domain_predict_only"))
            continue
        slope = _ocv_slope_at(soc_pred, curve)
        if slope is None:
            soc, v1, p00, p01, p11 = soc_pred, v1_pred, p00_pred, \
                p01_pred, p11_pred
            skipped += 1
            trace.append((t1, soc, v1, "slope_unavailable"))
            continue
        is_flat = slope < slope_min
        v_pred = ocv + ichg * r0 + v1_pred
        if not bc.is_finite_number(v_pred):
            soc, v1, p00, p01, p11 = soc_pred, v1_pred, p00_pred, \
                p01_pred, p11_pred
            skipped += 1
            trace.append((t1, soc, v1, "non_finite_v_pred"))
            continue
        innov = v_meas - v_pred
        denom = slope * (p00_pred * slope + p01_pred) + \
            (p01_pred * slope + p11_pred) + float(r_v)
        if not bc.is_finite_number(denom) or denom <= 0.0:
            soc, v1, p00, p01, p11 = soc_pred, v1_pred, p00_pred, \
                p01_pred, p11_pred
            skipped += 1
            trace.append((t1, soc, v1, "singular_innovation"))
            continue
        k0 = (p00_pred * slope + p01_pred) / denom
        k1 = (p01_pred * slope + p11_pred) / denom
        soc = soc_pred + k0 * innov
        v1 = v1_pred + k1 * innov
        if soc < 0.0:
            soc = 0.0
            saturated = True
        elif soc > 1.0:
            soc = 1.0
            saturated = True
        p00 = p00_pred - k0 * (p00_pred * slope + p01_pred)
        p01 = p01_pred - k0 * (p01_pred * slope + p11_pred)
        p11 = p11_pred - k1 * (p01_pred * slope + p11_pred)
        if p00 < 0.0:
            p00 = 0.0
        if p11 < 0.0:
            p11 = 0.0
        updates += 1
        if is_flat:
            flat += 1
            trace.append((t1, soc, v1, "update_flat_ocv"))
        else:
            trace.append((t1, soc, v1, "update"))
    return {"final_soc01": soc, "final_v1_v": v1, "p_soc": p00,
            "p_v1": p11, "saturated": saturated, "updates": updates,
            "flat_updates": flat, "out_of_domain": out_of_domain,
            "skipped": skipped, "trace": trace}


def apparent_resistance_ohm(pre_v_v, post_v_v, pre_i_charge_a,
                            post_i_charge_a, min_delta_a=5.0,
                            max_ohm=None):
    """Apparent DC resistance dV/dI in charge convention (equals -dV/dI in
    discharge-positive convention). Inputs must already be charge-convention
    amperes. Returns (ohms|None, reason|None); None with reason low_delta
    when |dI| < min_delta_a, or non_physical when the ratio is negative,
    non-finite, or above max_ohm when supplied. Never EIS/true cell."""
    pre_v, post_v = _finite(pre_v_v), _finite(post_v_v)
    pre_i, post_i = _finite(pre_i_charge_a), _finite(post_i_charge_a)
    need = _finite(min_delta_a)
    if pre_v is None or post_v is None or pre_i is None or post_i is None:
        return None, "non_finite_inputs"
    if need is None or need <= 0.0:
        raise ElectricalError("min_delta_a must be finite > 0")
    if max_ohm is not None:
        bound = _finite(max_ohm)
        if bound is None or bound <= 0.0:
            raise ElectricalError("max_ohm must be finite > 0")
    else:
        bound = None
    delta_i = post_i - pre_i
    delta_v = post_v - pre_v
    if not bc.is_finite_number(delta_i) \
            or not bc.is_finite_number(delta_v):
        return None, "non_finite_inputs"
    if abs(delta_i) < need:
        return None, "low_delta"
    if delta_i == 0.0:
        return None, "low_delta"
    res = delta_v / delta_i
    if not bc.is_finite_number(res) or res < 0.0:
        return None, "non_physical"
    if bound is not None and res > bound:
        return None, "non_physical"
    return res, None


def check_step_timing(pre_v_ns, pre_i_ns, post_v_ns, post_i_ns,
                      max_skew_ns, min_step_ns, max_step_ns):
    """Synchronized bounded load-step timing check -> (ok, reason)."""
    for val in (pre_v_ns, pre_i_ns, post_v_ns, post_i_ns):
        if bc.to_ns(val) is None:
            return False, "bad_timestamp"
    for name, val in (("max_skew_ns", max_skew_ns),
                      ("min_step_ns", min_step_ns),
                      ("max_step_ns", max_step_ns)):
        if isinstance(val, bool) or not isinstance(val, int) or val < 0:
            raise ElectricalError("%s must be a non-negative int" % name)
    if abs(pre_v_ns - pre_i_ns) > max_skew_ns:
        return False, "pre_skew"
    if abs(post_v_ns - post_i_ns) > max_skew_ns:
        return False, "post_skew"
    step = min(post_v_ns, post_i_ns) - max(pre_v_ns, pre_i_ns)
    if step < min_step_ns:
        return False, "step_too_short"
    if step > max_step_ns:
        return False, "step_too_long"
    return True, None


def check_step_comparability(pre_soc01, post_soc01, pre_temp_c,
                             post_temp_c, max_dsoc01, max_dtemp_c):
    """SOC/temperature comparability -> (ok, reason, temp_checked).

    Either pair may be None (unavailable): that side is skipped, never
    filled. temp_checked reports whether temperature actually gated."""
    dsoc = _finite(max_dsoc01)
    dtemp = _finite(max_dtemp_c)
    if dsoc is None or dsoc < 0.0 or dtemp is None or dtemp < 0.0:
        raise ElectricalError("comparability bounds finite >= 0")
    if pre_soc01 is not None and post_soc01 is not None:
        pre_s, post_s = _finite(pre_soc01), _finite(post_soc01)
        if pre_s is None or post_s is None:
            return False, "non_finite_soc", False
        if abs(post_s - pre_s) > dsoc:
            return False, "soc_shift", False
    if pre_temp_c is not None and post_temp_c is not None:
        pre_t, post_t = _finite(pre_temp_c), _finite(post_temp_c)
        if pre_t is None or post_t is None:
            return False, "non_finite_temp", False
        if abs(post_t - pre_t) > dtemp:
            return False, "temp_shift", True
        return True, None, True
    return True, None, False


def _boxcar(values, window):
    if window <= 1:
        return list(values)
    half = window // 2
    out = []
    for idx in range(len(values)):
        lo = max(0, idx - half)
        hi = min(len(values), idx + half + 1)
        seg = values[lo:hi]
        out.append(sum(seg) / len(seg))
    return out


def ica_dva_curve(samples, direction, min_span_v=0.05,
                  max_gap_ns=DEFAULT_MAX_GAP_NS, smooth_window=1):
    """ICA dQ/dV (Ah/V) and DVA dV/dQ (V/Ah) over [(ns, q_ah, v_v)].

    direction: "charge" needs strictly increasing V, "discharge" strictly
    decreasing V; Q throughput strictly increasing in both. Any leg wider
    than max_gap_ns, span below min_span_v, unordered/duplicate stamps, or
    non-monotonic V/Q rejects the whole curve (no partial features).
    smooth_window is a centered boxcar over Q/V before central differences
    (1 = none); documented here. Returns dict with curve
    [(q_ah, v_v, ica_ah_per_v|None, dva_v_per_ah|None)], features
    (peak by largest |dQ/dV| and |dV/dQ| with locations), and reason.
    Full curve data comes from this plain helper; analyze() emits only the
    compact scalar features.
    """
    if direction not in ("charge", "discharge"):
        raise ElectricalError('direction must be "charge"/"discharge"')
    span_min = _finite(min_span_v)
    if span_min is None or span_min <= 0.0:
        raise ElectricalError("min_span_v must be finite > 0")
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise ElectricalError("max_gap_ns must be a non-negative int")
    if isinstance(smooth_window, bool) or not isinstance(smooth_window, int) \
            or smooth_window < 1:
        raise ElectricalError("smooth_window must be int >= 1")
    clean = []
    for item in samples or []:
        try:
            t_ns, q_ah, v_v = item
        except (TypeError, ValueError):
            return {"curve": None, "features": None,
                    "reason": "malformed_samples"}
        if bc.to_ns(t_ns) is None:
            return {"curve": None, "features": None,
                    "reason": "bad_timestamp"}
        qval, vval = _finite(q_ah), _finite(v_v)
        if qval is None or vval is None or vval <= 0.0:
            return {"curve": None, "features": None,
                    "reason": "non_finite_qv"}
        clean.append((t_ns, qval, vval))
    if len(clean) < 3:
        return {"curve": None, "features": None, "reason": "sparse"}
    clean.sort(key=lambda p: p[0])
    for (t0, _, _), (t1, _, _) in zip(clean, clean[1:]):
        if t1 <= t0:
            return {"curve": None, "features": None,
                    "reason": "unordered_or_duplicate"}
        if t1 - t0 > max_gap_ns:
            return {"curve": None, "features": None, "reason": "gap"}
    for (_, q0, _), (_, q1, _) in zip(clean, clean[1:]):
        if not q1 > q0:
            return {"curve": None, "features": None,
                    "reason": "non_monotonic_q"}
    for (_, _, v0), (_, _, v1) in zip(clean, clean[1:]):
        if direction == "charge" and not v1 > v0:
            return {"curve": None, "features": None,
                    "reason": "non_monotonic_v"}
        if direction == "discharge" and not v1 < v0:
            return {"curve": None, "features": None,
                    "reason": "non_monotonic_v"}
    volts = [v for _, _, v in clean]
    if max(volts) - min(volts) < span_min:
        return {"curve": None, "features": None,
                "reason": "insufficient_span"}
    qs_raw = [q for _, q, _ in clean]
    vs_raw = list(volts)
    qs = _boxcar(qs_raw, smooth_window)
    vs = _boxcar(vs_raw, smooth_window)
    curve = []
    for idx in range(len(clean)):
        if idx == 0:
            dq = qs[1] - qs[0]
            dv = vs[1] - vs[0]
        elif idx == len(clean) - 1:
            dq = qs[-1] - qs[-2]
            dv = vs[-1] - vs[-2]
        else:
            dq = (qs[idx + 1] - qs[idx - 1]) / 2.0
            dv = (vs[idx + 1] - vs[idx - 1]) / 2.0
        ica = dva = None
        if dv != 0.0 and bc.is_finite_number(dq / dv):
            ica = dq / dv
        if dq != 0.0 and bc.is_finite_number(dv / dq):
            dva = dv / dq
        if ica is not None and not bc.is_finite_number(ica):
            ica = None
        if dva is not None and not bc.is_finite_number(dva):
            dva = None
        curve.append((qs_raw[idx], vs_raw[idx], ica, dva))
    peak = None
    dpeak = None
    for q, v, ica, dva in curve:
        if ica is not None and (peak is None
                                or abs(ica) > abs(peak[2])):
            peak = (q, v, ica)
        if dva is not None and (dpeak is None
                                or abs(dva) > abs(dpeak[2])):
            dpeak = (q, v, dva)
    if peak is None or dpeak is None:
        return {"curve": None, "features": None, "reason": "flat_curve"}
    features = {"ica_peak_q_ah": peak[0], "ica_peak_v_v": peak[1],
                "ica_peak_dqdv_ah_per_v": peak[2],
                "dva_peak_q_ah": dpeak[0], "dva_peak_v_v": dpeak[1],
                "dva_peak_dvdq_v_per_ah": dpeak[2],
                "n_points": len(curve), "direction": direction,
                "smooth_window": smooth_window}
    return {"curve": curve, "features": features, "reason": None}


def _parse_field_list(value, name, default):
    if value is None:
        return list(default)
    if not isinstance(value, (list, tuple)) or not value:
        raise ElectricalError("malformed: %s non-empty list" % name)
    out = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise ElectricalError("malformed: %s strings" % name)
        out.append(item)
    return out


def _parse_calibration_scope(raw):
    """Structured scope binding {vehicle,source,decode_epoch}; absent keys
    are wildcards. None (missing key) means unbound: no calibrated
    result may run. Returns (vehicle|None, source|None, epoch|None)."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ElectricalError("malformed: calibration_scope dict")
    out = []
    for key in ("vehicle", "source", "decode_epoch"):
        val = raw.get(key)
        if val is None:
            out.append(None)
        elif isinstance(val, str) and val:
            out.append(val)
        else:
            raise ElectricalError("malformed: calibration_scope strings")
    return tuple(out)


def _scope_matches(cal_scope, scope):
    for bound, actual in zip(cal_scope, scope):
        if bound is not None and bound != actual:
            return False
    return True


def _parse_coulomb(ecfg):
    # Coulomb needs capacity + anchor; a bare current_sign alone only
    # orients OCV/DCR/ICA (see analysis_sign in analyze) and leaves
    # Coulomb unavailable rather than erroring the whole run.
    present = any(k in ecfg for k in ("capacity_ah", "initial_soc_pct",
                                      "initial_soc_time_ns"))
    if not present:
        return None
    _require_version_domain(ecfg, "coulomb")
    cap = _finite(ecfg.get("capacity_ah"))
    if cap is None or cap <= 0.0:
        raise ElectricalError("malformed: capacity_ah finite > 0")
    sign = ecfg.get("current_sign")
    _check_sign(sign)
    init = _finite(ecfg.get("initial_soc_pct"))
    if init is None or not 0.0 <= init <= 100.0:
        raise ElectricalError("malformed: initial_soc_pct in [0, 100]")
    anchor = bc.to_ns(ecfg.get("initial_soc_time_ns"))
    if anchor is None:
        raise ElectricalError("malformed: initial_soc_time_ns integer ns")
    eff = _finite(ecfg.get("coulombic_efficiency", 1.0))
    if eff is None or not 0.0 < eff <= 1.0:
        raise ElectricalError("malformed: coulombic_efficiency (0, 1]")
    gap = ecfg.get("max_gap_ns", DEFAULT_MAX_GAP_NS)
    if isinstance(gap, bool) or not isinstance(gap, int) or gap < 0:
        raise ElectricalError("malformed: max_gap_ns non-negative int")
    std = ecfg.get("current_std_a")
    if std is not None:
        conv = _finite(std)
        if conv is None or conv < 0.0:
            raise ElectricalError("malformed: current_std_a >= 0")
        std = conv
    init_std = ecfg.get("initial_soc_uncertainty_pct")
    if init_std is not None:
        conv = _finite(init_std)
        if conv is None or conv < 0.0:
            raise ElectricalError("malformed: initial_soc_uncertainty >= 0")
        init_std = conv / 100.0
    cap_std = ecfg.get("capacity_ah_uncertainty")
    if cap_std is not None and (_finite(cap_std) is None
                                or cap_std < 0.0):
        raise ElectricalError("malformed: capacity_ah_uncertainty >= 0")
    return {"capacity_ah": cap, "current_sign": sign,
            "initial_soc01": init / 100.0, "anchor_ns": anchor,
            "efficiency": eff, "max_gap_ns": gap, "current_std_a": std,
            "initial_std01": init_std}


def _parse_ocv(ecfg):
    if "ocv_curve" not in ecfg:
        return None
    _require_version_domain(ecfg, "ocv")
    ver = ecfg.get("ocv_version")
    if not isinstance(ver, str) or not ver:
        raise ElectricalError("malformed: ocv needs ocv_version")
    try:
        curve = validate_ocv_curve(ecfg.get("ocv_curve"))
    except ElectricalError as exc:
        raise ElectricalError("malformed: ocv_curve %s" % exc)
    slope = _finite(ecfg.get("ocv_min_dv_dsoc_v", 0.05))
    if slope is None or slope <= 0.0:
        raise ElectricalError("malformed: ocv_min_dv_dsoc_v > 0")
    rest_s = _finite(ecfg.get("rest_seconds", 1800.0))
    rest_thr = _finite(ecfg.get("rest_current_threshold_a", 2.0))
    if rest_s is None or rest_s <= 0.0 or rest_thr is None \
            or rest_thr < 0.0:
        raise ElectricalError("malformed: rest_seconds/threshold")
    return {"curve": curve, "version": ver, "min_slope": slope,
            "rest_seconds": rest_s, "rest_threshold_a": rest_thr}


def _parse_ekf(ecfg):
    if "ekf" not in ecfg:
        return None
    _require_version_domain(ecfg, "ekf")
    ver = ecfg.get("ekf_version")
    if not isinstance(ver, str) or not ver:
        raise ElectricalError("malformed: ekf needs ekf_version")
    raw = ecfg.get("ekf")
    if not isinstance(raw, dict):
        raise ElectricalError("malformed: ekf dict")
    try:
        init_pct = _finite(raw.get("init_soc_pct"))
        if init_pct is None or not 0.0 <= init_pct <= 100.0:
            raise ElectricalError("bad init_soc_pct")
        init_ns = bc.to_ns(raw.get("init_time_ns",
                                   ecfg.get("ekf_init_time_ns")))
        if init_ns is None:
            raise ElectricalError("malformed: ekf init_time_ns integer ns")
        out = {"r0_ohm": float(raw.get("r0_ohm")),
               "r1_ohm": float(raw.get("r1_ohm")),
               "c1_f": float(raw.get("c1_f")),
               "capacity_ah": float(raw.get("capacity_ah")),
               "q_soc_per_s": float(raw.get("q_soc_per_s")),
               "q_v1_per_s": float(raw.get("q_v1_per_s")),
               "r_v": float(raw.get("r_v")),
               "init_soc01": init_pct / 100.0, "init_time_ns": init_ns,
               "init_v1_v": float(raw.get("init_v1_v", 0.0)),
               "init_p_soc": float(raw.get("init_p_soc")),
               "init_p_v1": float(raw.get("init_p_v1")),
               "version": ver}
    except (TypeError, ValueError):
        raise ElectricalError("malformed: ekf numeric fields")
    for key in ("r0_ohm", "r1_ohm", "c1_f", "capacity_ah",
                "q_soc_per_s", "q_v1_per_s", "r_v"):
        if not bc.is_finite_number(out[key]) or out[key] <= 0.0:
            raise ElectricalError("malformed: ekf %s > 0" % key)
    for key in ("init_v1_v",):
        if not bc.is_finite_number(out[key]):
            raise ElectricalError("malformed: ekf %s finite" % key)
    for key in ("init_p_soc", "init_p_v1"):
        if not bc.is_finite_number(out[key]) or out[key] < 0.0:
            raise ElectricalError("malformed: ekf %s >= 0" % key)
    slope = _finite(ecfg.get("ekf_min_dv_dsoc_v", 0.05))
    if slope is None or slope <= 0.0:
        raise ElectricalError("malformed: ekf_min_dv_dsoc_v > 0")
    out["min_slope"] = slope
    # EKF reuses the Coulomb current sign + efficiency + OCV curve when
    # present; otherwise it carries its own sign under the same top-level
    # calibration. Sign stays explicit: no Tesla-wide assumption.
    sign = ecfg.get("current_sign", raw.get("current_sign"))
    if sign is None:
        raise ElectricalError("malformed: ekf needs current_sign")
    _check_sign(sign)
    out["current_sign"] = sign
    eff = raw.get("efficiency", ecfg.get("coulombic_efficiency", 1.0))
    eff = _finite(eff)
    if eff is None or not 0.0 < eff <= 1.0:
        raise ElectricalError("malformed: ekf efficiency (0, 1]")
    out["efficiency"] = eff
    return out


def _parse_dcr(ecfg):
    raw = ecfg.get("dcr", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ElectricalError("malformed: dcr dict")
    merged = dict(DEFAULT_DCR)
    merged.update(raw)
    delta = _finite(merged.get("min_delta_a"))
    if delta is None or delta <= 0.0:
        raise ElectricalError("malformed: dcr min_delta_a > 0")
    for key in ("max_skew_ns", "min_step_ns", "max_step_ns"):
        val = merged.get(key)
        if isinstance(val, bool) or not isinstance(val, int) or val < 0:
            raise ElectricalError("malformed: dcr %s non-negative int" % key)
    for key in ("max_soc_change", "max_temp_change_c"):
        val = _finite(merged.get(key))
        if val is None or val < 0.0:
            raise ElectricalError("malformed: dcr %s >= 0" % key)
    if merged["min_step_ns"] > merged["max_step_ns"]:
        raise ElectricalError("malformed: dcr step window")
    return merged


def _parse_ica(ecfg):
    raw = ecfg.get("ica", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ElectricalError("malformed: ica dict")
    merged = dict(DEFAULT_ICA)
    merged.update(raw)
    span = _finite(merged.get("min_span_v"))
    if span is None or span <= 0.0:
        raise ElectricalError("malformed: ica min_span_v > 0")
    gap = merged.get("max_gap_ns")
    if isinstance(gap, bool) or not isinstance(gap, int) or gap < 0:
        raise ElectricalError("malformed: ica max_gap_ns non-negative int")
    win = merged.get("smooth_window")
    if isinstance(win, bool) or not isinstance(win, int) or win < 1:
        raise ElectricalError("malformed: ica smooth_window int >= 1")
    for key in ("cc_tolerance_a", "min_mean_current_a"):
        val = _finite(merged.get(key))
        if val is None or val < 0.0:
            raise ElectricalError("malformed: ica %s >= 0" % key)
    return merged


def _field_timeline(rows, field, expected_unit, field_units,
                    has_top_calibration):
    """Barrier-preserving timeline for one source_field: (ns, value|None,
    usable, ambiguous). Invalid/uncalibrated stamps and contradictory
    same-time values stay visible so alignment cannot bridge them."""
    series = sorted(
        (r for r in rows if r.get("source_field") == field),
        key=lambda r: r["event_time_ns"])
    stamped = {}
    for row in series:
        usable = _row_usable(row, field, expected_unit, field_units,
                             has_top_calibration)
        stamped.setdefault(row["event_time_ns"], []).append(
            (row.get("value_num"), usable))
    timeline = []
    for tstamp in sorted(stamped):
        entries = stamped[tstamp]
        if len({repr(v) for v, _ in entries}) > 1:
            timeline.append((tstamp, None, False, True))
        elif not all(ok for _, ok in entries):
            timeline.append((tstamp, entries[0][0], False, False))
        else:
            timeline.append((tstamp, entries[0][0], True, False))
    return timeline


def _usable_pairs(timeline):
    return [(t, v) for t, v, ok, _ in timeline if ok]


def _first_contiguous_from_anchor(timeline, anchor_ns, max_gap_ns,
                                  window_end_ns):
    """Integrate only from an observed anchor; never shift the initial SOC."""
    if not timeline:
        return [], None, "sparse"
    idx = 0
    while idx < len(timeline) and timeline[idx][0] < anchor_ns:
        idx += 1
    same = [e for e in timeline if e[0] == anchor_ns]
    if same and not (len(same) == 1 and same[0][2]):
        return [], None, "anchor_barrier"
    if idx >= len(timeline):
        return [], None, "anchor_outside_history"
    first_ns, _, ok, _ = timeline[idx]
    if not ok:
        return [], None, "anchor_barrier"
    if window_end_ns is not None and first_ns > window_end_ns:
        return [], None, "anchor_outside_history"
    if first_ns != anchor_ns:
        return [], None, "anchor_outside_history"
    pairs = [(first_ns, timeline[idx][1])]
    prev_ns = first_ns
    for tstamp, val, ok, ambiguous in timeline[idx + 1:]:
        if window_end_ns is not None and tstamp > window_end_ns:
            break
        if not ok or tstamp - prev_ns > max_gap_ns:
            if len(pairs) < 2:
                return pairs, first_ns, (
                    "ambiguous_current" if ambiguous else "single_sample")
            break
        pairs.append((tstamp, val))
        prev_ns = tstamp
    if len(pairs) < 2:
        return pairs, first_ns, "single_sample"
    return pairs, first_ns, None


def _synchronized_steps(cur_timeline, vol_timeline, max_skew_ns,
                        max_gap_ns, anchor_ns, window_end_ns):
    """Barrier-aware V/I alignment from the EKF anchor: walks usable
    voltage stamps at/after the anchor, joins each to the latest usable
    current at t<=v within skew, and requires every joined step to be
    unbroken (barrier/ambiguity/gap ends the run, never bridged)."""
    cur_by_t = {t: v for t, v in _usable_pairs(cur_timeline)}
    cur_usable = sorted(cur_by_t)
    steps = []
    prev_t = None
    started = False
    for tstamp, val, ok, _ in vol_timeline:
        if tstamp < anchor_ns:
            continue
        if window_end_ns is not None and tstamp > window_end_ns:
            break
        if not ok:
            if started:
                break
            continue
        match = None
        for cstamp in reversed(cur_usable):
            if cstamp > tstamp:
                continue
            if tstamp - cstamp > max_skew_ns:
                break
            match = (cstamp, cur_by_t[cstamp])
            break
        if match is None:
            if started:
                break
            continue
        cstamp, cval = match
        if not started:
            if tstamp != anchor_ns:
                return [], "anchor_outside_history"
            started = True
        elif tstamp - prev_t > max_gap_ns:
            break
        # A barrier/ambiguous stamp strictly between the joined current
        # time and the voltage time invalidates the step.
        gap_ok = True
        for b_t, _, b_ok, _ in cur_timeline:
            lower = min(cstamp, prev_t) if prev_t is not None else cstamp
            if lower < b_t <= tstamp and not b_ok:
                gap_ok = False
                break
        if not gap_ok:
            break
        for b_t, _, b_ok, _ in vol_timeline:
            if prev_t is not None and prev_t < b_t < tstamp and not b_ok:
                gap_ok = False
                break
        if not gap_ok:
            break
        steps.append((tstamp, cval, val))
        prev_t = tstamp
    if not started:
        return [], "anchor_outside_history"
    return steps, None


def _rested_voltages(cur_timeline, vol_timeline, sign, rest_seconds,
                     threshold_a, max_gap_ns, window_end_ns):
    """Barrier-aware resting voltages: every sample in the trailing rest
    window must be usable and within threshold, and no barrier/ambiguity
    may sit strictly inside the window or between its last sample and the
    voltage stamp. Never bridges an invalid row."""
    chg = sorted((t, sign * v) for t, v in _usable_pairs(cur_timeline))
    cur_barriers = [t for t, _, ok, _ in cur_timeline if not ok]
    vol_barriers = [t for t, _, ok, _ in vol_timeline if not ok]
    out = []
    for tstamp, val, ok, _ in vol_timeline:
        if not ok:
            continue
        if window_end_ns is not None and tstamp > window_end_ns:
            break
        start = tstamp - int(rest_seconds * 1e9)
        if not is_resting_at(chg, tstamp, rest_seconds, threshold_a,
                             max_gap_ns):
            continue
        prior = [t for t, _ in chg if t <= start]
        lower = prior[-1]
        if any(lower <= t <= tstamp for t in cur_barriers) \
                or any(lower <= t <= tstamp for t in vol_barriers):
            continue
        out.append((tstamp, val))
    return out


def _battery_temp_timeline(rows, field, temp_units, has_top_calibration):
    """Calibrated battery temperature timeline: explicit celsius/C mapping
    in dcr_temp_units, else native celsius/C rows. No ambient fallback."""
    unit = temp_units.get(field) if isinstance(temp_units, dict) else None
    if unit is not None:
        return _field_timeline(rows, field, unit, temp_units,
                               has_top_calibration)
    native_rows = (
        dict(row, unit="celsius") if row.get("unit") == "C" else row
        for row in rows if row.get("source_field") == field)
    return _field_timeline(native_rows, field, "celsius", {}, False)


def _scope_rows(signals):
    normed = []
    for raw in signals or []:
        sig = bc.normalize_signal(raw)
        if sig is not None:
            normed.append(sig)
    return normed


def _row_usable(row, field, expected_unit, field_units, has_top_calibration):
    """Physical usability: measurable value/quality AND a calibrated unit.

    Raw unit-NULL rows (PackVoltage/PackCurrent without field_units) are
    barriers, never silently skipped inside a continuous segment.
    """
    if row.get("value_num") is None \
            or not bc.is_valid_quality(row.get("quality")):
        return False
    if row.get("unit") == expected_unit:
        return True
    return has_top_calibration and isinstance(field_units, dict) \
        and field_units.get(field) == expected_unit


def _first_field_timeline(rows, fields, expected_unit, field_units,
                          has_top_calibration):
    """First configured field with any rows: (field, timeline)."""
    for field in fields:
        if any(r.get("source_field") == field for r in rows):
            return field, _field_timeline(rows, field, expected_unit,
                                          field_units, has_top_calibration)
    return None, []


def _scope_mismatch_rows(scope, window, cal_ver, cal_scope):
    rows = []
    for metric in SUPPORTED_METRICS:
        rows.append(bc.make_result(
            metric=metric, value=None,
            unit="%" if "soc" in metric else None,
            status="unavailable",
            reason="missing_calibration:scope_mismatch",
            window_start_ns=window[0], window_end_ns=window[1],
            vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
            evidence_count=0, algorithm_version=ALGORITHM_VERSION,
            calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(metric, scope, "scope_mismatch",
                                    cal_scope, ALGORITHM_VERSION)))
    return rows

def analyze(signals, events, config):
    """analyze(signals, events, config) -> list[dict] (stable contract)."""
    cfg_all = config if isinstance(config, dict) else {}
    ecfg = cfg_all.get("electrical", {})
    if ecfg is None:
        ecfg = {}
    if not isinstance(ecfg, dict):
        return [bc.make_result(
            metric=m, value=None, unit=None, status="error",
            reason="malformed: electrical dict",
            analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "electrical", ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    raw_ws = cfg_all.get("window_start_ns")
    raw_we = cfg_all.get("window_end_ns")
    for label, raw in (("window_start_ns", raw_ws),
                       ("window_end_ns", raw_we),
                       ("decision_time_ns",
                        cfg_all.get("decision_time_ns"))):
        if raw is not None and bc.to_ns(raw) is None:
            rows = []
            for metric in SUPPORTED_METRICS:
                rows.append(bc.make_result(
                    metric=metric, value=None, unit=None, status="error",
                    reason="malformed:%s" % label,
                    analysis_id=ANALYSIS_ID,
                    revision=bc.revision_id(metric, "window",
                                            raw, ALGORITHM_VERSION)))
            return rows
    window = (bc.to_ns(raw_ws), bc.to_ns(raw_we))
    if window[0] is not None and window[1] is not None \
            and window[1] < window[0]:
        rows = []
        for metric in SUPPORTED_METRICS:
            rows.append(bc.make_result(
                metric=metric, value=None, unit=None, status="error",
                reason="malformed:window_order", analysis_id=ANALYSIS_ID,
                revision=bc.revision_id(metric, "window",
                                        window, ALGORITHM_VERSION)))
        return rows
    decision = bc.to_ns(cfg_all.get("decision_time_ns"))
    rows_all = _scope_rows(signals)
    excluded_unknown = excluded_late = 0
    if decision is not None:
        kept = []
        for row in rows_all:
            if row["event_time_ns"] > decision:
                excluded_late += 1
                continue
            ingest = row.get("ingest_time_ns")
            if ingest is None:
                excluded_unknown += 1
                continue  # online: unknown ingest cannot prove availability
            if ingest > decision:
                excluded_late += 1
                continue
            kept.append(row)
        rows_all = kept
    exclusion_note = ""
    if decision is not None and (excluded_unknown or excluded_late):
        exclusion_note = "; excluded %d unknown-ingest (availability " \
            "unprovable at decision_time) + %d late-ingest observations" \
            % (excluded_unknown, excluded_late)
    # Retained history before window_start stays: anchor continuity
    # integrates from the anchor timestamp, never resets at window_start.
    # Only the upper bound clips evidence; unknown-ingest exclusion above
    # applies on the online path only.
    history_all = list(rows_all)
    rows_all = [r for r in history_all
                if window[1] is None or r["event_time_ns"] <= window[1]]
    if not rows_all:
        out = []
        for metric in SUPPORTED_METRICS:
            out.append(bc.make_result(
                metric=metric, value=None,
                unit="%" if "soc" in metric else None,
                status="unavailable", reason="no_signals" + exclusion_note,
                window_start_ns=window[0], window_end_ns=window[1],
                analysis_id=ANALYSIS_ID,
                revision=bc.revision_id(metric, "empty", ecfg,
                                        ALGORITHM_VERSION,
                                        exclusion_note)))
        return out
    try:
        coulomb_cal = _parse_coulomb(ecfg)
    except ElectricalError as exc:
        return [bc.make_result(
            metric=m, value=None, unit="%" if "soc" in m else None,
            status="error", reason=str(exc), window_start_ns=window[0],
            window_end_ns=window[1], analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "coulomb", str(exc),
                                    ALGORITHM_VERSION)) for m in SUPPORTED_METRICS]
    try:
        ocv_cal = _parse_ocv(ecfg)
    except ElectricalError as exc:
        return [bc.make_result(
            metric=m, value=None, unit="%" if "soc" in m else None,
            status="error", reason=str(exc), window_start_ns=window[0],
            window_end_ns=window[1], analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "ocv", str(exc),
                                    ALGORITHM_VERSION)) for m in SUPPORTED_METRICS]
    try:
        ekf_cal = _parse_ekf(ecfg)
    except ElectricalError as exc:
        return [bc.make_result(
            metric=m, value=None, unit="%" if "soc" in m else None,
            status="error", reason=str(exc), window_start_ns=window[0],
            window_end_ns=window[1], analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "ekf", str(exc),
                                    ALGORITHM_VERSION)) for m in SUPPORTED_METRICS]
    try:
        dcr_cal = _parse_dcr(ecfg)
        ica_cal = _parse_ica(ecfg)
        current_fields = _parse_field_list(ecfg.get("current_fields"),
                                           "current_fields",
                                           ["PackCurrent"])
        voltage_fields = _parse_field_list(ecfg.get("voltage_fields"),
                                           "voltage_fields",
                                           ["PackVoltage"])
        dcr_soc_field = ecfg.get("dcr_soc_field", "Soc")
        if not isinstance(dcr_soc_field, str) or not dcr_soc_field:
            raise ElectricalError("malformed: dcr_soc_field string")
        dcr_temp_fields = ecfg.get("dcr_temp_fields",
                                   ["ModuleTempMin", "ModuleTempMax"])
        if not isinstance(dcr_temp_fields, (list, tuple)) \
                or not dcr_temp_fields:
            raise ElectricalError("malformed: dcr_temp_fields non-empty")
        for item in dcr_temp_fields:
            if not isinstance(item, str) or not item:
                raise ElectricalError("malformed: dcr_temp_fields strings")
        dcr_temp_fields = list(dcr_temp_fields)
        dcr_temp_units = ecfg.get("dcr_temp_units", {})
        if dcr_temp_units is None:
            dcr_temp_units = {}
        if not isinstance(dcr_temp_units, dict):
            raise ElectricalError("malformed: dcr_temp_units dict")
        for key, val in dcr_temp_units.items():
            if not isinstance(key, str) or not key or val not in (
                    "celsius", "C"):
                raise ElectricalError(
                    "malformed: dcr_temp_units celsius/C map")
        cal_scope = _parse_calibration_scope(ecfg.get("calibration_scope"))
    except ElectricalError as exc:
        return [bc.make_result(
            metric=m, value=None, unit=None, status="error",
            reason=str(exc), window_start_ns=window[0],
            window_end_ns=window[1], analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "params", str(exc),
                                    ALGORITHM_VERSION)) for m in SUPPORTED_METRICS]
    field_units = ecfg.get("field_units", {})
    if field_units is None:
        field_units = {}
    if not isinstance(field_units, dict):
        return [bc.make_result(
            metric=m, value=None, unit=None, status="error",
            reason="malformed: field_units dict",
            window_start_ns=window[0], window_end_ns=window[1],
            analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "units", ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    has_top_cal = isinstance(ecfg.get("calibration_version"), str) \
        and bool(ecfg.get("calibration_version")) \
        and isinstance(ecfg.get("domain"), str) \
        and bool(ecfg.get("domain"))
    if field_units and not has_top_cal:
        return [bc.make_result(
            metric=m, value=None, unit=None, status="error",
            reason="malformed: field_units needs calibration_version/domain",
            window_start_ns=window[0], window_end_ns=window[1],
            analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "units", ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    cal_ver = ecfg.get("calibration_version") if has_top_cal else None
    analysis_sign = None
    if coulomb_cal is not None:
        analysis_sign = coulomb_cal["current_sign"]
    elif ekf_cal is not None:
        analysis_sign = ekf_cal["current_sign"]
    elif ecfg.get("current_sign") in (1, -1) \
            and not isinstance(ecfg.get("current_sign"), bool):
        if not has_top_cal:
            return [bc.make_result(
                metric=m, value=None, unit=None, status="error",
                reason="malformed: current_sign needs "
                "calibration_version/domain",
                window_start_ns=window[0], window_end_ns=window[1],
                analysis_id=ANALYSIS_ID,
                revision=bc.revision_id(m, "sign", ALGORITHM_VERSION))
                for m in SUPPORTED_METRICS]
        analysis_sign = ecfg.get("current_sign")
    out = []
    needs_scope = (coulomb_cal is not None or ocv_cal is not None
                   or ekf_cal is not None or bool(field_units)
                   or analysis_sign is not None)
    for scope, srows in bc.group_by_scope(rows_all).items():
        ordered = bc.sort_dedup(srows)
        scope_ok = cal_scope is not None and _scope_matches(cal_scope,
                                                            scope)
        if needs_scope and (not has_top_cal or not scope_ok):
            out.extend(_scope_mismatch_rows(scope, window, cal_ver,
                                            cal_scope))
            continue
        _, cur_timeline = _first_field_timeline(
            ordered, current_fields, "A", field_units, has_top_cal)
        _, vol_timeline = _first_field_timeline(
            ordered, voltage_fields, "V", field_units, has_top_cal)
        cur_pairs = _usable_pairs(cur_timeline)
        vol_pairs = _usable_pairs(vol_timeline)
        cur_raw = sum(len([r for r in ordered
                           if r.get("source_field") == f])
                      for f in current_fields)
        vol_raw = sum(len([r for r in ordered
                           if r.get("source_field") == f])
                      for f in voltage_fields)
        # --- Coulomb SOC ---
        if coulomb_cal is None:
            out.append(bc.make_result(
                metric="battery.electrical.soc_coulomb_pct", value=None,
                unit="%", status="unavailable",
                reason="missing_calibration:coulomb",
                window_start_ns=window[0], window_end_ns=window[1],
                vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                evidence_count=len(cur_pairs),
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
                revision=bc.revision_id("soc_coulomb", scope, cur_pairs,
                                        ecfg, ALGORITHM_VERSION)))
        elif not cur_pairs:
            out.append(bc.make_result(
                metric="battery.electrical.soc_coulomb_pct", value=None,
                unit="%", status="unavailable",
                reason="sparse:uncalibrated_current" if cur_raw
                else "sparse:no_current",
                window_start_ns=window[0], window_end_ns=window[1],
                vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                evidence_count=0, algorithm_version=ALGORITHM_VERSION,
                calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
                revision=bc.revision_id("soc_coulomb", scope, cur_raw,
                                        ecfg, ALGORITHM_VERSION)))
        else:
            # Anchor-bound continuity: integrate the first contiguous run
            # at/after initial_soc_time_ns; barriers/gaps end the run and
            # later segments are never silently re-anchored.
            seg_pairs, _first_ns, anchor_reason = \
                _first_contiguous_from_anchor(
                    cur_timeline, coulomb_cal["anchor_ns"],
                    coulomb_cal["max_gap_ns"], window[1])
            if seg_pairs and window[0] is not None \
                    and seg_pairs[-1][0] < window[0]:
                anchor_reason = "anchor_outside_history"
            if anchor_reason is not None or len(seg_pairs) < 2:
                out.append(bc.make_result(
                    metric="battery.electrical.soc_coulomb_pct", value=None,
                    unit="%", status="unavailable",
                    reason="anchor_outside_history"
                    if anchor_reason == "anchor_outside_history"
                    else ("anchor_barrier:invalid_before_first_sample"
                          if anchor_reason in ("anchor_barrier",
                                               "anchor_gap")
                          else ("ambiguous:same_time_current"
                                if anchor_reason == "ambiguous_current"
                                else "sparse:single_sample_segment")),
                    window_start_ns=window[0], window_end_ns=window[1],
                    vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                    evidence_count=len(cur_pairs),
                    algorithm_version=ALGORITHM_VERSION,
                    calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
                    revision=bc.revision_id("soc_coulomb", scope, cur_pairs,
                                            coulomb_cal["anchor_ns"],
                                            ALGORITHM_VERSION)))
            else:
                res = coulomb_soc_series(
                    seg_pairs, coulomb_cal["capacity_ah"],
                    coulomb_cal["initial_soc01"],
                    coulomb_cal["current_sign"],
                    coulomb_cal["efficiency"], coulomb_cal["max_gap_ns"],
                    coulomb_cal["current_std_a"],
                    coulomb_cal["initial_std01"])
                last_ns = seg_pairs[-1][0]
                truncated = cur_timeline[-1][0] > last_ns \
                    or res["gaps"] > 0 or res["ambiguous"] > 0
                no_value = res["final_soc01"] is None or (
                    res["ambiguous"] > 0 and res["legs_used"] == 0)
                if no_value:
                    reason = "ambiguous:same_time_current" \
                        if res["ambiguous"] > 0 else "gap:discontinuity"
                    reason += exclusion_note
                    out.append(bc.make_result(
                        metric="battery.electrical.soc_coulomb_pct",
                        value=None, unit="%", status="unavailable",
                        reason=reason,
                        window_start_ns=window[0], window_end_ns=window[1],
                        vehicle=scope[0], source=scope[1],
                        decode_epoch=scope[2],
                        evidence_count=len(seg_pairs),
                        sample_count=res["legs_used"],
                        algorithm_version=ALGORITHM_VERSION,
                        calibration_version=cal_ver,
                        analysis_id=ANALYSIS_ID,
                        revision=bc.revision_id("soc_coulomb", scope,
                                                seg_pairs, ecfg,
                                                ALGORITHM_VERSION)))
                else:
                    pct = res["final_soc01"] * 100.0
                    std_pct = None
                    lo = hi = None
                    if res["std01"] is not None:
                        std_pct = res["std01"] * 100.0
                        lo = max(0.0, pct - std_pct)
                        hi = min(100.0, pct + std_pct)
                    reason = "coulomb_integration"
                    if res["ambiguous"] > 0:
                        reason += ";truncated_at_ambiguity"
                    elif truncated:
                        reason += ";truncated_at_gap"
                    if res["saturated"]:
                        reason += ";saturated_0_100"
                    reason += ";observation_time_ns=%d" % last_ns
                    reason += exclusion_note
                    out.append(bc.make_result(
                        metric="battery.electrical.soc_coulomb_pct",
                        value=pct, unit="%", status="estimated",
                        reason=reason, window_start_ns=window[0],
                        window_end_ns=window[1], vehicle=scope[0],
                        source=scope[1], decode_epoch=scope[2],
                        evidence_count=len(seg_pairs),
                        sample_count=res["legs_used"],
                        coverage_ratio=max(0, last_ns - max(
                            coulomb_cal["anchor_ns"],
                            window[0] or coulomb_cal["anchor_ns"])) / max(
                                1, (window[1] or cur_timeline[-1][0]) - max(
                                    coulomb_cal["anchor_ns"],
                                    window[0] or coulomb_cal["anchor_ns"])),
                        algorithm_version=ALGORITHM_VERSION,
                        calibration_version=cal_ver, uncertainty=std_pct,
                        uncertainty_lower=lo, uncertainty_upper=hi,
                        analysis_id=ANALYSIS_ID,
                        revision=bc.revision_id("soc_coulomb", scope,
                                                seg_pairs, ecfg,
                                                ALGORITHM_VERSION)))
        # --- OCV SOC (resting only, never BMS passthrough) ---
        if ocv_cal is None:
            out.append(bc.make_result(
                metric="battery.electrical.soc_ocv_pct", value=None,
                unit="%", status="unavailable",
                reason="missing_calibration:ocv",
                window_start_ns=window[0], window_end_ns=window[1],
                vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                evidence_count=len(vol_pairs),
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
                revision=bc.revision_id("soc_ocv", scope, vol_pairs,
                                        ecfg, ALGORITHM_VERSION)))
        elif not vol_pairs or not cur_pairs:
            out.append(bc.make_result(
                metric="battery.electrical.soc_ocv_pct", value=None,
                unit="%", status="unavailable",
                reason="sparse:ocv_needs_resting_voltage_and_current",
                window_start_ns=window[0], window_end_ns=window[1],
                vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                evidence_count=len(vol_pairs) + len(cur_pairs),
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=ocv_cal["version"],
                analysis_id=ANALYSIS_ID,
                revision=bc.revision_id("soc_ocv", scope, vol_pairs,
                                        cur_pairs, ALGORITHM_VERSION)))
        else:
            sign = analysis_sign
            gap = DEFAULT_MAX_GAP_NS
            if coulomb_cal is not None:
                gap = coulomb_cal["max_gap_ns"]
            if sign is None:
                out.append(bc.make_result(
                    metric="battery.electrical.soc_ocv_pct", value=None,
                    unit="%", status="unavailable",
                    reason="missing_calibration:current_sign",
                    window_start_ns=window[0], window_end_ns=window[1],
                    vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                    evidence_count=len(vol_pairs),
                    algorithm_version=ALGORITHM_VERSION,
                    calibration_version=ocv_cal["version"],
                    analysis_id=ANALYSIS_ID,
                    revision=bc.revision_id("soc_ocv", scope, vol_pairs,
                                            ALGORITHM_VERSION)))
            else:
                rested = _rested_voltages(cur_timeline, vol_timeline, sign,
                                          ocv_cal["rest_seconds"],
                                          ocv_cal["rest_threshold_a"], gap,
                                          window[1])
                rested = [p for p in rested
                          if window[0] is None or p[0] >= window[0]]
                if not rested:
                    out.append(bc.make_result(
                        metric="battery.electrical.soc_ocv_pct", value=None,
                        unit="%", status="unavailable",
                        reason="no_rest:current_above_threshold_or_gap"
                        + exclusion_note,
                        window_start_ns=window[0], window_end_ns=window[1],
                        vehicle=scope[0], source=scope[1],
                        decode_epoch=scope[2],
                        evidence_count=len(vol_pairs) + len(cur_pairs),
                        algorithm_version=ALGORITHM_VERSION,
                        calibration_version=ocv_cal["version"],
                        analysis_id=ANALYSIS_ID,
                        revision=bc.revision_id("soc_ocv", scope, vol_pairs,
                                                cur_timeline, ALGORITHM_VERSION)))
                else:
                    t_pick, v_pick = rested[-1]
                    soc = soc_from_ocv(v_pick, ocv_cal["curve"],
                                       ocv_cal["min_slope"])
                    if soc is None:
                        flat = v_pick < ocv_cal["curve"][0][1] \
                            or v_pick > ocv_cal["curve"][-1][1]
                        out.append(bc.make_result(
                            metric="battery.electrical.soc_ocv_pct",
                            value=None, unit="%", status="unavailable",
                            reason=("out_of_domain" if flat
                                    else "flat_ocv:unobservable")
                            + exclusion_note,
                            window_start_ns=window[0],
                            window_end_ns=window[1], vehicle=scope[0],
                            source=scope[1], decode_epoch=scope[2],
                            evidence_count=len(rested),
                            sample_count=len(vol_pairs),
                            algorithm_version=ALGORITHM_VERSION,
                            calibration_version=ocv_cal["version"],
                            analysis_id=ANALYSIS_ID,
                            revision=bc.revision_id(
                                "soc_ocv", scope, rested, ecfg,
                                ALGORITHM_VERSION)))
                    else:
                        out.append(bc.make_result(
                            metric="battery.electrical.soc_ocv_pct",
                            value=soc * 100.0, unit="%", status="estimated",
                            reason="ocv_inverse_at_rest"
                            ";observation_time_ns=%d" % t_pick
                            + exclusion_note,
                            window_start_ns=window[0],
                            window_end_ns=window[1], vehicle=scope[0],
                            source=scope[1], decode_epoch=scope[2],
                            evidence_count=len(rested),
                            sample_count=len(vol_pairs),
                            algorithm_version=ALGORITHM_VERSION,
                            calibration_version=ocv_cal["version"],
                            analysis_id=ANALYSIS_ID,
                            revision=bc.revision_id(
                                "soc_ocv", scope, rested, ecfg,
                                ALGORITHM_VERSION)))
        # --- EKF SOC ---
        if ekf_cal is None or ocv_cal is None:
            out.append(bc.make_result(
                metric="battery.electrical.soc_ekf_pct", value=None,
                unit="%", status="unavailable",
                reason="missing_calibration:ekf_ocv",
                window_start_ns=window[0], window_end_ns=window[1],
                vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                evidence_count=len(cur_pairs) + len(vol_pairs),
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
                revision=bc.revision_id("soc_ekf", scope, cur_pairs,
                                        vol_pairs, ALGORITHM_VERSION)))
        elif not cur_pairs or not vol_pairs:
            out.append(bc.make_result(
                metric="battery.electrical.soc_ekf_pct", value=None,
                unit="%", status="unavailable",
                reason="sparse:ekf_needs_current_and_voltage",
                window_start_ns=window[0], window_end_ns=window[1],
                vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                evidence_count=len(cur_pairs) + len(vol_pairs),
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=ekf_cal["version"],
                analysis_id=ANALYSIS_ID,
                revision=bc.revision_id("soc_ekf", scope, ALGORITHM_VERSION)))
        else:
            max_gap = coulomb_cal["max_gap_ns"] if coulomb_cal is not None \
                else DEFAULT_MAX_GAP_NS
            steps, step_reason = _synchronized_steps(
                cur_timeline, vol_timeline, dcr_cal["max_skew_ns"], max_gap,
                ekf_cal["init_time_ns"], window[1])
            if steps and window[0] is not None and steps[-1][0] < window[0]:
                step_reason = "anchor_outside_history"
            if step_reason is not None or len(steps) < 2:
                out.append(bc.make_result(
                    metric="battery.electrical.soc_ekf_pct", value=None,
                    unit="%", status="unavailable",
                    reason="anchor_outside_history"
                    if step_reason == "anchor_outside_history"
                    else ("anchor_barrier:invalid_before_first_step"
                          if step_reason == "anchor_gap"
                          else ("sparse:single_synchronized_step"
                                if len(steps) == 1
                                else "unsynchronized:"
                                "voltage_current_skew")) + exclusion_note,
                    window_start_ns=window[0], window_end_ns=window[1],
                    vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
                    evidence_count=len(steps),
                    sample_count=len(vol_pairs),
                    algorithm_version=ALGORITHM_VERSION,
                    calibration_version=ekf_cal["version"],
                    analysis_id=ANALYSIS_ID,
                    revision=bc.revision_id("soc_ekf", scope,
                                            ekf_cal["init_time_ns"],
                                            ALGORITHM_VERSION)))
            else:
                try:
                    res = ekf_1rc_soc(
                        steps, ocv_cal["curve"], ekf_cal["r0_ohm"],
                        ekf_cal["r1_ohm"], ekf_cal["c1_f"],
                        ekf_cal["capacity_ah"], ekf_cal["current_sign"],
                        ekf_cal["efficiency"], ekf_cal["q_soc_per_s"],
                        ekf_cal["q_v1_per_s"], ekf_cal["r_v"],
                        ekf_cal["init_soc01"], ekf_cal["init_v1_v"],
                        ekf_cal["init_p_soc"], ekf_cal["init_p_v1"],
                        ekf_cal["min_slope"])
                except ElectricalError as exc:
                    msg = str(exc)
                    if msg.startswith("ambiguous"):
                        reason = "ambiguous:same_time_steps"
                        status = "unavailable"
                    else:
                        reason = msg
                        status = "error"
                    out.append(bc.make_result(
                        metric="battery.electrical.soc_ekf_pct", value=None,
                        unit="%", status=status,
                        reason=reason + exclusion_note,
                        window_start_ns=window[0], window_end_ns=window[1],
                        vehicle=scope[0], source=scope[1],
                        decode_epoch=scope[2],
                        evidence_count=len(steps),
                        algorithm_version=ALGORITHM_VERSION,
                        calibration_version=ekf_cal["version"],
                        analysis_id=ANALYSIS_ID,
                        revision=bc.revision_id("soc_ekf", scope, steps,
                                                msg, ALGORITHM_VERSION)))
                else:
                    reason = "ekf_1rc"
                    if res["flat_updates"]:
                        reason += ";flat_ocv_degraded"
                    if res["out_of_domain"]:
                        reason += ";out_of_domain_predict_only"
                    if res["saturated"]:
                        reason += ";saturated_0_100"
                    reason += ";observation_time_ns=%d" % steps[-1][0]
                    reason += exclusion_note
                    std = None
                    lo = hi = None
                    if bc.is_finite_number(res["p_soc"]) \
                            and res["p_soc"] >= 0.0:
                        std = math.sqrt(res["p_soc"]) * 100.0
                        pct = res["final_soc01"] * 100.0
                        lo = max(0.0, pct - std)
                        hi = min(100.0, pct + std)
                    out.append(bc.make_result(
                        metric="battery.electrical.soc_ekf_pct",
                        value=res["final_soc01"] * 100.0, unit="%",
                        status="estimated", reason=reason,
                        window_start_ns=window[0], window_end_ns=window[1],
                        vehicle=scope[0], source=scope[1],
                        decode_epoch=scope[2], evidence_count=len(steps),
                        sample_count=res["updates"],
                        algorithm_version=ALGORITHM_VERSION,
                        calibration_version=ekf_cal["version"],
                        uncertainty=std, uncertainty_lower=lo,
                        uncertainty_upper=hi, analysis_id=ANALYSIS_ID,
                        revision=bc.revision_id("soc_ekf", scope, steps,
                                                ekf_cal, ALGORITHM_VERSION)))
        # --- Apparent DC resistance ---
        soc_timelines = {dcr_soc_field: _field_timeline(
            ordered, dcr_soc_field, "%", {}, False)}
        temp_timelines = {}
        for field in dcr_temp_fields:
            temp_timelines[field] = _battery_temp_timeline(
                ordered, field, dcr_temp_units, has_top_cal)
        out.extend(_analyze_dcr(scope, window, cur_timeline, vol_timeline,
                                soc_timelines, temp_timelines, analysis_sign,
                                dcr_cal, cal_ver, dcr_soc_field,
                                dcr_temp_fields, exclusion_note))
        # --- ICA/DVA ---
        out.extend(_analyze_ica(scope, window, cur_timeline, vol_timeline,
                                analysis_sign, ica_cal, dcr_cal, cal_ver,
                                exclusion_note))
    return out


def _step_sequence(cur_timeline, vol_timeline, sign, max_skew_ns,
                   window_end_ns):
    """Barrier-aware V/I join: latest usable current at t<=v within skew;
    any barrier/ambiguous stamp strictly inside (current, voltage] or
    between consecutive steps ends that region (never bridged)."""
    cur_by_t = {t: v for t, v in _usable_pairs(cur_timeline)}
    cur_usable = sorted(cur_by_t)
    seq = []
    prev_t = None
    for tstamp, val, ok, _ in vol_timeline:
        if window_end_ns is not None and tstamp > window_end_ns:
            break
        if not ok:
            continue
        match = None
        for cstamp in reversed(cur_usable):
            if cstamp > tstamp:
                continue
            if tstamp - cstamp > max_skew_ns:
                break
            match = (cstamp, cur_by_t[cstamp])
            break
        if match is None:
            continue
        cstamp, cval = match
        bridged = False
        for b_t, _, b_ok, _ in cur_timeline:
            if cstamp < b_t <= tstamp and not b_ok:
                bridged = True
                break
        if not bridged and prev_t is not None:
            for b_t, _, b_ok, _ in vol_timeline:
                if prev_t < b_t < tstamp and not b_ok:
                    bridged = True
                    break
        if bridged:
            continue
        seq.append((tstamp, val, sign * cval, cstamp))
        prev_t = tstamp
    return seq, None


def _barrier_asof(pairs, barriers, t_ns, max_skew_ns):
    """Latest usable value at t<=t_ns within skew; None when a barrier
    stamp sits strictly inside (matched, t_ns] (never bridged)."""
    best = None
    for tstamp, val in pairs:
        if tstamp <= t_ns and t_ns - tstamp <= max_skew_ns:
            best = (tstamp, val)
        elif tstamp > t_ns:
            break
    if best is None:
        return None
    for b_t in barriers:
        if best[0] < b_t <= t_ns:
            return None
    return best[1]


def _analyze_dcr(scope, window, cur_timeline, vol_timeline, soc_timelines,
                 temp_timelines, sign, dcr_cal, cal_ver, soc_field,
                 temp_fields, exclusion_note=""):
    metric = "battery.electrical.resistance_apparent_ohm"
    if sign is None:
        return [bc.make_result(
            metric=metric, value=None, unit="ohm", status="unavailable",
            reason="missing_calibration:current_sign",
            window_start_ns=window[0], window_end_ns=window[1],
            vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
            evidence_count=len(_usable_pairs(cur_timeline))
            + len(_usable_pairs(vol_timeline)),
            algorithm_version=ALGORITHM_VERSION,
            calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
            revision=bc.revision_id("dcr", scope, "nosign",
                                    ALGORITHM_VERSION))]
    cur_pairs = _usable_pairs(cur_timeline)
    vol_pairs = _usable_pairs(vol_timeline)
    if not cur_pairs or not vol_pairs:
        return [bc.make_result(
            metric=metric, value=None, unit="ohm", status="unavailable",
            reason="sparse:dcr_needs_current_and_voltage",
            window_start_ns=window[0], window_end_ns=window[1],
            vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
            evidence_count=len(cur_pairs) + len(vol_pairs),
            algorithm_version=ALGORITHM_VERSION,
            calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
            revision=bc.revision_id("dcr", scope, "sparse",
                                    ALGORITHM_VERSION))]
    seq, seq_reason = _step_sequence(cur_timeline, vol_timeline, sign,
                                     dcr_cal["max_skew_ns"], window[1])
    if seq_reason is not None or len(seq) < 2:
        return [bc.make_result(
            metric=metric, value=None, unit="ohm", status="unavailable",
            reason="unsynchronized:voltage_current_skew",
            window_start_ns=window[0], window_end_ns=window[1],
            vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
            evidence_count=len(seq), sample_count=len(vol_pairs),
            algorithm_version=ALGORITHM_VERSION,
            calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
            revision=bc.revision_id("dcr", scope, seq, ALGORITHM_VERSION))]
    soc_pairs = _usable_pairs(soc_timelines.get(soc_field, []))
    temperatures = {
        field: (_usable_pairs(temp_timelines.get(field, [])),
                [t for t, _, ok, _ in temp_timelines.get(field, []) if not ok])
        for field in temp_fields}
    electrical_barriers = [
        t for timeline in (cur_timeline, vol_timeline)
        for t, _, ok, _ in timeline if not ok]
    soc_barriers = [t for t, _, ok, _ in soc_timelines.get(soc_field, [])
                    if not ok]
    valid_rs = []
    dominance = {"low_delta": 0, "non_physical": 0, "skew": 0,
                 "shift": 0}
    shift_kind = None
    for idx_a in range(len(seq)):
        for idx_b in range(idx_a + 1, len(seq)):
            t_a, v_a, i_a, ti_a = seq[idx_a]
            t_b, v_b, i_b, ti_b = seq[idx_b]
            if t_b - t_a > dcr_cal["max_step_ns"]:
                break
            if window[0] is not None and t_b < window[0]:
                continue
            if any(t_a <= t <= t_b for t in electrical_barriers):
                dominance["skew"] += 1
                continue
            ok, why = check_step_timing(
                t_a, ti_a, t_b, ti_b, dcr_cal["max_skew_ns"],
                dcr_cal["min_step_ns"], dcr_cal["max_step_ns"])
            if not ok:
                dominance["skew"] += 1
                continue
            res, why_r = apparent_resistance_ohm(
                v_a, v_b, i_a, i_b, dcr_cal["min_delta_a"])
            if res is None:
                dominance[why_r if why_r in dominance else
                          "non_physical"] += 1
                continue
            pre_soc = _barrier_asof(soc_pairs, soc_barriers, t_a,
                                    dcr_cal["max_skew_ns"])
            post_soc = _barrier_asof(soc_pairs, soc_barriers, t_b,
                                     dcr_cal["max_skew_ns"])
            pre_soc01 = pre_soc / 100.0 if pre_soc is not None else None
            post_soc01 = post_soc / 100.0 if post_soc is not None else None
            temperature_pairs = [
                (_barrier_asof(pairs, barriers, t_a, dcr_cal["max_skew_ns"]),
                 _barrier_asof(pairs, barriers, t_b, dcr_cal["max_skew_ns"]))
                for pairs, barriers in temperatures.values()]
            if any(a is None or b is None for a, b in temperature_pairs):
                dominance["shift"] += 1
                shift_kind = "temp_missing"
                continue
            if pre_soc is None or post_soc is None:
                dominance["shift"] += 1
                shift_kind = "soc_missing"
                continue
            comparisons = [
                check_step_comparability(
                    pre_soc01, post_soc01, a, b,
                    dcr_cal["max_soc_change"], dcr_cal["max_temp_change_c"])
                for a, b in temperature_pairs]
            if not all(result[0] for result in comparisons):
                dominance["shift"] += 1
                shift_kind = next(result[1] for result in comparisons
                                  if not result[0])
                continue
            valid_rs.append((res, t_a, t_b))
    if not valid_rs:
        if dominance["low_delta"] >= max(dominance.values()) and not dominance["shift"]:
            reason = "low_delta:load_step_below_minimum"
        elif dominance["shift"]:
            if shift_kind == "temp_missing":
                reason = "unavailable_comparability:" \
                    "missing_battery_temperature"
            elif shift_kind == "soc_missing":
                reason = "unavailable_comparability:missing_soc"
            else:
                reason = "soc_or_temp_shift:steps_not_comparable"
        elif dominance["skew"]:
            reason = "unsynchronized:step_timing_out_of_bounds"
        else:
            reason = "non_physical:no_stable_load_step"
        return [bc.make_result(
            metric=metric, value=None, unit="ohm", status="unavailable",
            reason=reason + exclusion_note, window_start_ns=window[0],
            window_end_ns=window[1], vehicle=scope[0], source=scope[1],
            decode_epoch=scope[2], evidence_count=len(seq),
            sample_count=len(vol_pairs),
            algorithm_version=ALGORITHM_VERSION,
            calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
            revision=bc.revision_id("dcr", scope, seq, dcr_cal,
                                    ALGORITHM_VERSION))]
    ordered = sorted(r for r, _, _ in valid_rs)
    median = ordered[len(ordered) // 2]
    note = ("apparent DC resistance -dV/dI discharge convention "
            "(dV/dI charge); not EIS or true cell resistance"
            ";conditioned_on_%s_%s" % (soc_field, "+".join(temp_fields)))
    note += exclusion_note
    return [bc.make_result(
        metric=metric, value=median, unit="ohm", status="derived",
        reason=note, window_start_ns=window[0], window_end_ns=window[1],
        vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
        evidence_count=len(valid_rs), sample_count=len(seq),
        algorithm_version=ALGORITHM_VERSION,
        calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
        revision=bc.revision_id("dcr", scope, ordered, dcr_cal,
                                ALGORITHM_VERSION))]


def _analyze_ica(scope, window, cur_timeline, vol_timeline, sign, ica_cal,
                 dcr_cal, cal_ver, exclusion_note=""):
    cols = [("battery.electrical.ica_peak_voltage_v", "V"),
            ("battery.electrical.ica_peak_dqdv_ah_per_v", "Ah/V"),
            ("battery.electrical.dva_peak_capacity_ah", "Ah"),
            ("battery.electrical.dva_peak_dvdq_v_per_ah", "V/Ah")]

    def _unavailable(reason, evidence):
        return [bc.make_result(
            metric=m, value=None, unit=u, status="unavailable",
            reason=reason + exclusion_note, window_start_ns=window[0],
            window_end_ns=window[1], vehicle=scope[0], source=scope[1],
            decode_epoch=scope[2], evidence_count=evidence,
            algorithm_version=ALGORITHM_VERSION,
            calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, scope, reason, ica_cal,
                                    ALGORITHM_VERSION)) for m, u in cols]

    if window[0] is not None:
        cur_timeline = [p for p in cur_timeline if p[0] >= window[0]]
        vol_timeline = [p for p in vol_timeline if p[0] >= window[0]]
    if any(not p[2] for timeline in (cur_timeline, vol_timeline)
           for p in timeline):
        return _unavailable("invalid_ica_segment:quality_barrier", 0)
    cur_pairs = _usable_pairs(cur_timeline)
    vol_pairs = _usable_pairs(vol_timeline)
    if sign is None:
        return _unavailable("missing_calibration:current_sign",
                            len(cur_pairs) + len(vol_pairs))
    if len(cur_pairs) < 3 or len(vol_pairs) < 3:
        return _unavailable("sparse:ica_needs_cc_charge_curve",
                            len(cur_pairs) + len(vol_pairs))
    chg = [(t, sign * i) for t, i in sorted(cur_pairs)]
    mean_i = sum(i for _, i in chg) / len(chg)
    spread = max(i for _, i in chg) - min(i for _, i in chg)
    if abs(mean_i) < ica_cal["min_mean_current_a"] \
            or spread > ica_cal["cc_tolerance_a"]:
        return _unavailable("non_cc:current_variation_or_low_current",
                            len(chg))
    direction = "charge" if mean_i > 0.0 else "discharge"
    dir_sign = 1.0 if direction == "charge" else -1.0
    cur_barriers = set(t for t, _, ok, _ in cur_timeline if not ok)
    # Throughput in the analysis direction; a barrier/gap between current
    # stamps ends the run (never bridged), so an invalid row between two
    # otherwise close valid rows cannot be skipped over.
    q_series = []
    charge = 0.0
    prev_t, prev_i = chg[0]
    if prev_t in cur_barriers:
        return _unavailable("gap:ica_curve_discontinuity", len(chg))
    q_series.append((prev_t, 0.0))
    broken = False
    for tstamp, ival in chg[1:]:
        dt_h = (tstamp - prev_t) / 3.6e12
        if tstamp - prev_t > ica_cal["max_gap_ns"]:
            broken = True
            break
        if any(prev_t < b <= tstamp for b in cur_barriers):
            broken = True
            break
        avg = (prev_i + ival) / 2.0
        charge += dir_sign * avg * dt_h
        q_series.append((tstamp, charge))
        prev_t, prev_i = tstamp, ival
    if broken:
        return _unavailable("gap:ica_curve_discontinuity", len(chg))
    vol_barriers = set(t for t, _, ok, _ in vol_timeline if not ok)
    samples = []
    for tstamp, qval in q_series:
        if window[1] is not None and tstamp > window[1]:
            break
        best = None
        for v_t, v_v in vol_pairs:
            if v_t <= tstamp and tstamp - v_t <= dcr_cal["max_skew_ns"]:
                best = (v_t, v_v)
            elif v_t > tstamp:
                break
        if best is None:
            continue
        v_t, vval = best
        if any(v_t < b <= tstamp for b in vol_barriers):
            continue
        if any(v_t < b <= tstamp for b in cur_barriers):
            continue
        samples.append((tstamp, qval, vval))
    if len(samples) < 3:
        return _unavailable("unsynchronized:ica_voltage_skew",
                            len(samples))
    res = ica_dva_curve(samples, direction, ica_cal["min_span_v"],
                        ica_cal["max_gap_ns"], ica_cal["smooth_window"])
    if res["curve"] is None:
        return _unavailable("invalid_ica_segment:" + res["reason"],
                            len(samples))
    feat = res["features"]
    vals = {"battery.electrical.ica_peak_voltage_v":
            feat["ica_peak_v_v"],
            "battery.electrical.ica_peak_dqdv_ah_per_v":
            feat["ica_peak_dqdv_ah_per_v"],
            "battery.electrical.dva_peak_capacity_ah": feat["dva_peak_q_ah"],
            "battery.electrical.dva_peak_dvdq_v_per_ah":
            feat["dva_peak_dvdq_v_per_ah"]}
    out = []
    for metric, unit in cols:
        out.append(bc.make_result(
            metric=metric, value=vals[metric], unit=unit, status="derived",
            reason="cc_%s_ica_dva_window%d_smooth%d%s" % (
                direction, len(samples), ica_cal["smooth_window"],
                exclusion_note),
            window_start_ns=window[0], window_end_ns=window[1],
            vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
            evidence_count=len(samples), sample_count=len(samples),
            algorithm_version=ALGORITHM_VERSION,
            calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(metric, scope, samples, ica_cal,
                                    direction, ALGORITHM_VERSION)))
    return out
