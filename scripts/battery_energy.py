#!/usr/bin/env python3
"""Battery energy, charge/discharge sessions and conditional capacity. Stdlib only.

analyze(signals, events, config) retains pre-window session context. Window
integrals use only in-window samples; cumulative-meter deltas additionally
anchor on the last valid pre-window reading within max_gap_ns, so adjacent
windows partition the meter increase instead of dropping boundary legs.
Optional integer-ns
window_start_ns/window_end_ns bound inclusive windows. decision_time_ns admits
only observations with known event and ingest times at or before the decision.

config["energy"]:
  max_skew_ns=300000000000; max_gap_ns=600000000000; min_soc_span_pct=10.
  max_offline_gap_ns=86400000000000: longest reporting gap whose
  LifetimeEnergyUsed increase is credited as parked_discharge_kwh.
  Exact source_field overrides use FIELD_DEFAULTS below, never path aliases.
  current_sign: positive_charge/+1 or positive_discharge/-1.
  field_calibration: {field: entry or [entries]} for PackCurrent/PackVoltage.
    Each entry requires exact vehicle/source/decode_epoch, declared_domain,
    version, unit A/V, unit_scale (finite nonzero, default1), unit_offset
    (finite, default0). Conflicting scopes error. Native A/V is already usable.
  reference: version, domain, energy_kwh and/or charge_ah (>0), optional
    conditions. domain/conditions identify the comparable measurement regime.
    reference_source: "configured" (default) or "bms_first". bms_first uses
    the latest valid NominalFullPackEnergyKwh reading (BMS-reported, version
    bms_nominal_full_pack) when the vehicle sends one and falls back to the
    configured reference otherwise; with no reference configured the BMS
    reading is used whenever present.
  soh_min_soc_span_pct=30: SOC span a complete session needs before its
    interval capacity yields soh_estimated_pct (partial-window estimate
    with uncertainty; soh_pct still needs a full 100->0 discharge).
  soc_uncertainty_pct/energy_uncertainty_kwh: optional nonnegative independent
    single-reading standard deviations; absent means unknown, not zero.

Invalid, unknown-unit and same-time contradictory values are barriers.
V*I/Ah reject a window containing a rejected leg; linear endpoint power/current
is split at zero crossings instead of cancelling gross charge and discharge.
Coverage describes observed time, not proof that a whole hour was sampled.
DCChargingEnergyIn is battery-side AC+DC; ACChargingEnergyIn is charger-side AC.
They are never added. LifetimeEnergyUsed is the independent discharge counter.
EFC selects exactly one domain (Ah, counters, V*I), never adds redundant sources.

Signed current, charger power or counter changes bound directional sessions.
Unknown state, text changes, gaps and invalid values cannot prove completion.
Observed idle on both sides plus exact, gap-bounded meter endpoints is required
for complete energy. Retained cross-hour sessions are credited only on closure.
episode_id is folded into analysis_id for persistence without PK collisions.

Partial delta-energy/delta-SOC is interval-equivalent capacity, not absolute
SOH. Full usable capacity needs a complete monotonic 100->0 SOC discharge with
independent LifetimeEnergyUsed endpoints. Latest eligible retained session is
labelled with its asof_ns; trend/SOH additionally require comparable reference.
EnergyRemaining/SOC is separately labelled BMS-circular, never independent.
Supplied endpoint uncertainty propagates against a fixed reference only.
"""

import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import battery_common as bc

CODE_VERSION = "1.0.0"
ALGORITHM_VERSION = "1.2.0"
ANALYSIS_ID = "battery_energy"

SUPPORTED_METRICS = (
    "battery.energy.charge_session_energy_kwh",
    "battery.energy.discharge_session_energy_kwh",
    "battery.energy.full_usable_capacity_kwh",
    "battery.energy.dc_charging_energy_in_kwh",
    "battery.energy.ac_charging_energy_in_kwh",
    "battery.energy.discharge_energy_kwh",
    "battery.energy.parked_discharge_kwh",
    "battery.energy.vi_charge_energy_kwh",
    "battery.energy.vi_discharge_energy_kwh",
    "battery.energy.charge_throughput_ah",
    "battery.energy.discharge_throughput_ah",
    "battery.energy.efc_oneway_cycles",
    "battery.energy.efc_bidirectional_cycles",
    "battery.energy.interval_capacity_kwh",
    "battery.energy.bms_circular_capacity_kwh",
    "battery.energy.capacity_trend_kwh",
    "battery.energy.soh_pct",
    "battery.energy.soh_estimated_pct",
)

METRIC_UNITS = {
    "battery.energy.charge_session_energy_kwh": "kWh",
    "battery.energy.discharge_session_energy_kwh": "kWh",
    "battery.energy.full_usable_capacity_kwh": "kWh",
    "battery.energy.dc_charging_energy_in_kwh": "kWh",
    "battery.energy.ac_charging_energy_in_kwh": "kWh",
    "battery.energy.discharge_energy_kwh": "kWh",
    "battery.energy.parked_discharge_kwh": "kWh",
    "battery.energy.vi_charge_energy_kwh": "kWh",
    "battery.energy.vi_discharge_energy_kwh": "kWh",
    "battery.energy.charge_throughput_ah": "Ah",
    "battery.energy.discharge_throughput_ah": "Ah",
    "battery.energy.efc_oneway_cycles": "cycles",
    "battery.energy.efc_bidirectional_cycles": "cycles",
    "battery.energy.interval_capacity_kwh": "kWh",
    "battery.energy.bms_circular_capacity_kwh": "kWh",
    "battery.energy.capacity_trend_kwh": "kWh",
    "battery.energy.soh_pct": "%",
    "battery.energy.soh_estimated_pct": "%",
}

DEFAULT_MAX_SKEW_NS = 300000000000
DEFAULT_MAX_GAP_NS = 600000000000
DEFAULT_MIN_SOC_SPAN_PCT = 10.0
DEFAULT_MAX_OFFLINE_GAP_NS = 86400000000000
DEFAULT_SOH_MIN_SOC_SPAN_PCT = 30.0
# Estimate-only reading noise when soc/energy uncertainty is not configured:
# Fleet Soc and kWh counters report ~0.01 resolution; 0.5 %p and 0.1 kWh per
# endpoint also cover sample-time skew at session boundaries.
DEFAULT_ESTIMATE_SOC_STD_PCT = 0.5
DEFAULT_ESTIMATE_ENERGY_STD_KWH = 0.1

FIELD_DEFAULTS = {
    "voltage_field": "PackVoltage",
    "current_field": "PackCurrent",
    "soc_field": "Soc",
    "energy_remaining_field": "EnergyRemaining",
    "dc_counter_field": "DCChargingEnergyIn",
    "ac_counter_field": "ACChargingEnergyIn",
    "discharge_counter_field": "LifetimeEnergyUsed",
    "session_energy_field": "DCChargingEnergyIn",
    "charging_state_field": "ChargingState",
    "detailed_charge_field": "DetailedChargeState",
    "bms_state_field": "BMSState",
    "power_field": "ChargerPower",
    "nominal_pack_field": "NominalFullPackEnergyKwh",
}

SIGN_METRICS = (
    "battery.energy.vi_charge_energy_kwh",
    "battery.energy.vi_discharge_energy_kwh",
    "battery.energy.charge_throughput_ah",
    "battery.energy.discharge_throughput_ah",
    "battery.energy.efc_oneway_cycles",
    "battery.energy.efc_bidirectional_cycles",
)

CAL_METRICS = (
    "battery.energy.vi_charge_energy_kwh",
    "battery.energy.vi_discharge_energy_kwh",
    "battery.energy.charge_throughput_ah",
    "battery.energy.discharge_throughput_ah",
)


class EnergyError(ValueError):
    pass


def _finite(value):
    return bc.safe_float(value)


def _check_ns(name, value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EnergyError("malformed: %s non-negative int" % name)
    return value


def _parse_sign(raw):
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise EnergyError("malformed: current_sign string or 1/-1")
    if raw == "positive_charge" or raw == 1:
        return 1
    if raw == "positive_discharge" or raw == -1:
        return -1
    raise EnergyError("malformed: current_sign string or 1/-1")


def _parse_scoped_calibration(raw, field, expected_unit):
    """Validate one scoped field-calibration entry.

    Required: vehicle/source/decode_epoch (exact scope identity),
    declared_domain (calibration domain; never a global chemistry
    guess), version, finite nonzero unit_scale, finite unit_offset
    (default 0.0), unit matching the expected physical unit.
    """
    if not isinstance(raw, dict):
        raise EnergyError("malformed: field_calibration[%s] dict" % field)
    for key in ("vehicle", "source", "decode_epoch", "declared_domain",
                "version"):
        val = raw.get(key)
        if not isinstance(val, str) or not val:
            raise EnergyError(
                "malformed: field_calibration[%s] needs %s" % (field, key))
    if raw.get("unit") != expected_unit:
        raise EnergyError(
            "malformed: field_calibration[%s] unit must be %s"
            % (field, expected_unit))
    scale = _finite(raw.get("unit_scale", 1.0))
    if scale is None or scale == 0.0:
        raise EnergyError(
            "malformed: field_calibration[%s] unit_scale finite nonzero"
            % field)
    offset = _finite(raw.get("unit_offset", 0.0))
    if offset is None:
        raise EnergyError(
            "malformed: field_calibration[%s] unit_offset finite" % field)
    return {"vehicle": raw["vehicle"], "source": raw["source"],
            "decode_epoch": raw["decode_epoch"],
            "declared_domain": raw["declared_domain"],
            "version": raw["version"], "unit": expected_unit,
            "unit_scale": scale, "unit_offset": offset}


def _parse_field_calibrations(ecfg, fields):
    """Build {source_field: [scoped entries]} for V/A math (canonical)."""
    out = {}
    raw = ecfg.get("field_calibration")
    if raw is not None:
        if not isinstance(raw, dict):
            raise EnergyError("malformed: field_calibration dict")
        for field, entry in raw.items():
            if not isinstance(field, str) or not field:
                raise EnergyError("malformed: field_calibration field names")
            cands = entry if isinstance(entry, list) else [entry]
            for cand in cands:
                if field == fields["voltage_field"]:
                    parsed = _parse_scoped_calibration(cand, field, "V")
                elif field == fields["current_field"]:
                    parsed = _parse_scoped_calibration(cand, field, "A")
                else:
                    raise EnergyError(
                        "malformed: field_calibration[%s] not a V/A field"
                        % field)
                if any((c["vehicle"], c["source"], c["decode_epoch"]) ==
                       (parsed["vehicle"], parsed["source"], parsed["decode_epoch"])
                       for c in out.get(field, [])):
                    raise EnergyError("malformed: overlapping field_calibration scope")
                out.setdefault(field, []).append(parsed)
    for key in ("voltage_calibration", "current_calibration"):
        if ecfg.get(key) is not None:
            raise EnergyError(
                "malformed: %s removed; use field_calibration" % key)
    return out


def _calibration_for(field_cals, field, scope):
    """Entries whose exact scope matches (canonical; no open scopes)."""
    cands = field_cals.get(field, [])
    return [c for c in cands
            if (c.get("vehicle"), c.get("source"),
                c.get("decode_epoch")) == scope]


def _cal_version(*cals):
    versions = []
    for cand in cals:
        if cand is None:
            continue
        if isinstance(cand, list):
            versions.extend(c["version"] for c in cand
                            if isinstance(c, dict) and c.get("version"))
        elif isinstance(cand, dict) and cand.get("version"):
            versions.append(cand["version"])
    return "+".join(versions) if versions else None


def _parse_reference(raw):
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise EnergyError("malformed: reference dict")
    version = raw.get("version")
    domain = raw.get("domain")
    if not isinstance(version, str) or not version:
        raise EnergyError("malformed: reference needs version")
    if not isinstance(domain, str) or not domain:
        raise EnergyError("malformed: reference needs domain")
    energy = _finite(raw.get("energy_kwh")) \
        if raw.get("energy_kwh") is not None else None
    charge_ah = _finite(raw.get("charge_ah")) \
        if raw.get("charge_ah") is not None else None
    if raw.get("energy_kwh") is not None and (energy is None or energy <= 0.0):
        raise EnergyError("malformed: reference energy_kwh finite > 0")
    if raw.get("charge_ah") is not None and (charge_ah is None or charge_ah <= 0.0):
        raise EnergyError("malformed: reference charge_ah finite > 0")
    if energy is None and charge_ah is None:
        raise EnergyError("malformed: reference needs energy_kwh/charge_ah")
    conditions = raw.get("conditions")
    if conditions is not None and (not isinstance(conditions, str)
                                   or not conditions):
        raise EnergyError("malformed: reference conditions string")
    return {"version": version, "domain": domain, "energy_kwh": energy,
            "charge_ah": charge_ah, "conditions": conditions}


def _parse_config(ecfg):
    if not isinstance(ecfg, dict):
        ecfg = {}
    skew = ecfg.get("max_skew_ns", DEFAULT_MAX_SKEW_NS)
    gap = ecfg.get("max_gap_ns", DEFAULT_MAX_GAP_NS)
    _check_ns("max_skew_ns", skew)
    _check_ns("max_gap_ns", gap)
    offline = ecfg.get("max_offline_gap_ns", DEFAULT_MAX_OFFLINE_GAP_NS)
    _check_ns("max_offline_gap_ns", offline)
    span = _finite(ecfg.get("min_soc_span_pct", DEFAULT_MIN_SOC_SPAN_PCT))
    if span is None or span <= 0.0:
        raise EnergyError("malformed: min_soc_span_pct finite > 0")
    soh_span = _finite(ecfg.get("soh_min_soc_span_pct",
                                DEFAULT_SOH_MIN_SOC_SPAN_PCT))
    if soh_span is None or not span <= soh_span <= 100.0:
        raise EnergyError(
            "malformed: soh_min_soc_span_pct finite in [min_soc_span_pct, 100]")
    fields = {}
    for key, default in FIELD_DEFAULTS.items():
        raw = ecfg.get(key, default)
        if not isinstance(raw, str) or not raw:
            raise EnergyError("malformed: %s non-empty string" % key)
        fields[key] = raw
    domain = ecfg.get("domain")
    if domain is not None and (not isinstance(domain, str) or not domain):
        raise EnergyError("malformed: domain string")
    conditions = ecfg.get("conditions")
    if conditions is not None and (not isinstance(conditions, str)
                                   or not conditions):
        raise EnergyError("malformed: conditions string")
    source = ecfg.get("reference_source", "configured")
    if source not in ("configured", "bms_first"):
        raise EnergyError("malformed: reference_source configured|bms_first")
    return {"max_skew_ns": skew, "max_gap_ns": gap,
            "max_offline_gap_ns": offline,
            "min_soc_span_pct": span, "soh_min_soc_span_pct": soh_span,
            "fields": fields, "domain": domain, "conditions": conditions,
            "reference_source": source}


def _parse_uncertainties(ecfg):
    out = {}
    for key in ("soc_uncertainty_pct", "energy_uncertainty_kwh"):
        raw = ecfg.get(key)
        if raw is None:
            out[key] = None
            continue
        conv = _finite(raw)
        if conv is None or conv < 0.0:
            raise EnergyError("malformed: %s finite >= 0" % key)
        out[key] = conv
    return out


def meter_delta(pairs):
    """Cumulative-meter delta over [(ns, value)] with reset detection.

    Values must be finite (callers pre-filter validity). Returns
    (delta|None, info) with info in ok/reset/sparse/ambiguous:
    sparse below two samples, reset on any decrease, ambiguous on
    conflicting values at one timestamp. Order-independent (sorted here).
    """
    if len(pairs) < 2:
        return None, "sparse"
    by_ts = {}
    for tst, val in pairs:
        if bc.to_ns(tst) is None or _finite(val) is None:
            return None, "sparse"
        by_ts.setdefault(tst, set()).add(float(val))
    for vals in by_ts.values():
        if len(vals) > 1:
            return None, "ambiguous"
    ordered = sorted((tst, next(iter(vals))) for tst, vals in by_ts.items())
    prev = ordered[0][1]
    for _, val in ordered[1:]:
        if val < prev:
            return None, "reset"
        prev = val
    delta = ordered[-1][1] - ordered[0][1]
    if not bc.is_finite_number(delta):
        return None, "sparse"
    return delta, "ok"


def split_power_legs(steps, max_gap_ns):
    """Split calibrated V*I steps into charge/discharge kWh.

    steps: [(ns, volts, amps_charge)] with charge-convention current.
    Legs wider than max_gap_ns, touching non-positive/non-finite volts,
    or non-finite current are rejected (never filled). Legs are classed
    by mean charge current; bucket totals floor at zero. Coverage is
    integrated seconds over spanned seconds.
    """
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise EnergyError("max_gap_ns must be a non-negative int")
    clean = sorted(steps, key=lambda p: p[0])
    charge = dis = 0.0
    used = rejected = 0
    used_dt = 0
    stamps = [t for t, _, _ in clean if bc.to_ns(t) is not None]
    span = (max(stamps) - min(stamps)) if len(stamps) >= 2 else None
    for (t0, v0, i0), (t1, v1, i1) in zip(clean, clean[1:]):
        c0, c1 = _finite(i0), _finite(i1)
        w0, w1 = _finite(v0), _finite(v1)
        if bc.to_ns(t0) is None or bc.to_ns(t1) is None or t1 < t0:
            rejected += 1
            continue
        if t1 - t0 > max_gap_ns:
            rejected += 1
            continue
        if c0 is None or c1 is None or w0 is None or w1 is None \
                or w0 <= 0.0 or w1 <= 0.0:
            rejected += 1
            continue
        # Integrate linear endpoint power, splitting its zero crossing.
        p0, p1 = w0 * c0, w1 * c1
        duration = (t1 - t0) / 3.6e15
        if p0 * p1 < 0:
            fraction = abs(p0) / (abs(p0) + abs(p1))
            charge += (max(p0, 0) * fraction
                       + max(p1, 0) * (1 - fraction)) * duration / 2
            dis += (max(-p0, 0) * fraction
                    + max(-p1, 0) * (1 - fraction)) * duration / 2
        else:
            energy = (p0 + p1) * duration / 2
            charge += max(energy, 0)
            dis += max(-energy, 0)
        used += 1
        used_dt += t1 - t0
    if used and not bc.is_finite_number(charge + dis):
        return {"charge_kwh": None, "discharge_kwh": None,
                "legs_used": used, "legs_rejected": rejected,
                "span_ns": span, "coverage_ratio": None}
    if not used:
        return {"charge_kwh": None, "discharge_kwh": None,
                "legs_used": 0, "legs_rejected": rejected,
                "span_ns": span, "coverage_ratio": None}
    coverage = (used_dt / span) if span else 1.0
    return {"charge_kwh": max(0.0, charge), "discharge_kwh": max(0.0, dis),
            "legs_used": used, "legs_rejected": rejected,
            "span_ns": span, "coverage_ratio": coverage}


def split_current_legs(pairs, max_gap_ns):
    """Split calibrated charge-convention current into Ah throughput.

    pairs: [(ns, amps_charge)]. Same gap/reject/coverage rules as
    split_power_legs but voltage-free, so Ah never depends on volts.
    discharge_ah is reported as a non-negative magnitude.
    """
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise EnergyError("max_gap_ns must be a non-negative int")
    clean = sorted(pairs, key=lambda p: p[0])
    charge = dis = 0.0
    used = rejected = 0
    used_dt = 0
    stamps = [t for t, _ in clean if bc.to_ns(t) is not None]
    span = (max(stamps) - min(stamps)) if len(stamps) >= 2 else None
    for (t0, i0), (t1, i1) in zip(clean, clean[1:]):
        c0, c1 = _finite(i0), _finite(i1)
        if bc.to_ns(t0) is None or bc.to_ns(t1) is None or t1 < t0:
            rejected += 1
            continue
        if t1 - t0 > max_gap_ns:
            rejected += 1
            continue
        if c0 is None or c1 is None:
            rejected += 1
            continue
        duration = (t1 - t0) / 3.6e12
        if c0 * c1 < 0:
            fraction = abs(c0) / (abs(c0) + abs(c1))
            charge += (max(c0, 0) * fraction
                       + max(c1, 0) * (1 - fraction)) * duration / 2
            dis += (max(-c0, 0) * fraction
                    + max(-c1, 0) * (1 - fraction)) * duration / 2
        else:
            ahs = (c0 + c1) * duration / 2
            charge += max(ahs, 0)
            dis += max(-ahs, 0)
        used += 1
        used_dt += t1 - t0
    if not used:
        return {"charge_ah": None, "discharge_ah": None,
                "legs_used": 0, "legs_rejected": rejected,
                "span_ns": span, "coverage_ratio": None}
    coverage = (used_dt / span) if span else 1.0
    return {"charge_ah": charge, "discharge_ah": dis,
            "legs_used": used, "legs_rejected": rejected,
            "span_ns": span, "coverage_ratio": coverage}


def efc_cycles(charge, discharge, ref):
    """Equivalent full cycles: oneway = discharge/ref (one direction),
    bidirectional = (charge + discharge) / (2 * ref) (absolute total).
    All inputs finite, throughputs >= 0, ref > 0; else EnergyError."""
    amount_c = _finite(charge)
    amount_d = _finite(discharge)
    ref_v = _finite(ref)
    if amount_c is None or amount_c < 0.0 or amount_d is None \
            or amount_d < 0.0 or ref_v is None or ref_v <= 0.0:
        raise EnergyError("efc needs finite throughputs >= 0 and ref > 0")
    oneway = amount_d / ref_v
    both = (amount_c + amount_d) / (2.0 * ref_v)
    if not bc.is_finite_number(oneway) or not bc.is_finite_number(both):
        raise EnergyError("efc result non-finite")
    return {"oneway": oneway, "bidirectional": both}


def interval_capacity_kwh(delta_e_kwh, delta_soc01):
    """Equivalent capacity = delta-E / delta-SOC. None when either side is
    non-finite or the SOC span is not positive (callers label zero vs
    small vs inconsistent)."""
    energy = _finite(delta_e_kwh)
    span = _finite(delta_soc01)
    if energy is None or span is None or span <= 0.0:
        return None
    cap = energy / span
    return cap if bc.is_finite_number(cap) else None


def capacity_uncertainty_kwh(cap_kwh, delta_e_kwh, energy_std_kwh,
                             delta_soc01, soc_std01):
    """Propagate supplied single-reading stds only (independent
    start/end): sigma = C * sqrt((sqrt2*sE/dE)^2 + (sqrt2*sS/dS)^2).
    None unless every input is usable and both deltas positive."""
    cap = _finite(cap_kwh)
    energy = _finite(delta_e_kwh)
    span = _finite(delta_soc01)
    serr = _finite(energy_std_kwh)
    sserr = _finite(soc_std01)
    if cap is None or energy is None or span is None \
            or serr is None or sserr is None:
        return None
    if energy <= 0.0 or span <= 0.0 or serr < 0.0 or sserr < 0.0:
        return None
    rel = math.sqrt(((math.sqrt(2.0) * serr / energy) ** 2)
                    + ((math.sqrt(2.0) * sserr / span) ** 2))
    sigma = abs(cap) * rel
    return sigma if bc.is_finite_number(sigma) and sigma >= 0.0 else None


def segment_sessions(points, max_gap_ns):
    """Reduce observed signed directions; gaps/barriers never close a full session."""
    _check_ns("max_gap_ns", max_gap_ns)
    sessions, current, previous = [], None, None

    def close(end, reason, complete):
        sessions.append(dict(current, end_ns=end, close_reason=reason,
                             incomplete_end=not complete, end_inclusive=True))

    for point in sorted(points, key=lambda p: p["t"]):
        stamp, direction = point["t"], point.get("direction")
        gap = previous is not None and stamp - previous["t"] > max_gap_ns
        if gap or direction is None:
            if current:
                close(previous["t"], "gap" if gap else "quality_barrier", False)
                current = None
            previous = None
        if direction is None:
            continue
        if current and direction != current["direction"]:
            close(stamp if direction == 0 else previous["t"],
                  "observed_idle" if direction == 0 else "direction_change",
                  direction == 0)
            current = None
        if direction and current is None:
            observed_start = previous is not None and previous["direction"] == 0
            current = {"start_ns": previous["t"] if observed_start else stamp,
                       "direction": direction, "incomplete_start": not observed_start,
                       "evidence": ["signed_current_or_counter"]}
        previous = point
    if current:
        close(previous["t"], "open_at_end", False)
    return sessions


def _session_analysis_id(episode_id):
    """Stable per-session persisted identity (episode_id not in table PK).

    Sharing one analysis_id across sessions would silently overwrite
    them in vehicle_analysis; the runtime folds episode_id into
    analysis_id generically, but the stable per-session id is emitted
    here so offline callers and tests see the persisted identity.
    """
    return "battery_energy:session:%s" % (episode_id,)


def _scope_rows(signals):
    normed = []
    for raw in signals or []:
        sig = bc.normalize_signal(raw)
        if sig is not None:
            normed.append(sig)
    return normed


def _num_pairs(rows, field, unit):
    """Retain invalid/unit/conflict barriers instead of joining across them."""
    grouped = {}
    for row in rows:
        if row.get("source_field") != field:
            continue
        value = _finite(row.get("value_num"))
        if not bc.is_valid_quality(row.get("quality")) or row.get("unit") != unit:
            value = None
        grouped.setdefault(row["event_time_ns"], set()).add(value)
    return [(t, next(iter(values)) if len(values) == 1 else None)
            for t, values in sorted(grouped.items())]


def _text_series(rows, field):
    grouped = {}
    for row in rows:
        if row.get("source_field") != field:
            continue
        value = row.get("value_text")
        if not bc.is_valid_quality(row.get("quality")) or not isinstance(value, str):
            value = None
        grouped.setdefault(row["event_time_ns"], set()).add(value)
    return [(t, next(iter(values)) if len(values) == 1 else None)
            for t, values in sorted(grouped.items())]


def _calibrated_values(rows, field, entries, unit):
    grouped, raw = {}, 0
    for row in rows:
        if row.get("source_field") != field:
            continue
        value = _finite(row.get("value_num"))
        if not bc.is_valid_quality(row.get("quality")):
            value = None
        elif row.get("unit") != unit:
            matches = [c for c in entries if
                       (c["vehicle"], c["source"], c["decode_epoch"]) ==
                       (row["vehicle"], row["source"], row["decode_epoch"])]
            if value is not None and row.get("unit") is None and len(matches) == 1:
                value = _finite(value * matches[0]["unit_scale"]
                                + matches[0]["unit_offset"])
            else:
                value = None
                raw += 1
        grouped.setdefault(row["event_time_ns"], []).append((row, value))
    result = []
    for stamp, samples in sorted(grouped.items()):
        values = {value for _, value in samples}
        value = next(iter(values)) if len(values) == 1 else None
        result.append(dict(samples[0][0], value_num=value, unit=unit,
                           quality="valid" if value is not None else "invalid"))
    return result, raw


def _calibrated_current(rows, field, entries):
    converted, raw = _calibrated_values(rows, field, entries, "A")
    return [(r["event_time_ns"], r["value_num"]) for r in converted], raw


def _calibrated_voltage_rows(rows, field, entries):
    return _calibrated_values(rows, field, entries, "V")


def _calibration_scope_note(scope, field_cals, field):
    cands = field_cals.get(field, [])
    if not cands:
        return "no_field_calibration"
    scopes = sorted(set((c["vehicle"], c["source"], c["decode_epoch"])
                        for c in cands))
    if scope in scopes:
        return "scope_match"
    return "scope_mismatch:calibrated_for_%r" % (scopes,)


def _asof(pairs, tst, skew_ns):
    best = None
    for ptime, val in pairs:
        if ptime <= tst and tst - ptime <= skew_ns:
            best = (ptime, val)
        elif ptime > tst:
            break
    return best[1] if best is not None else None


def analyze(signals, events, config):
    """analyze(signals, events, config) -> list[dict] (stable contract)."""
    cfg_all = config if isinstance(config, dict) else {}
    raw_ws = cfg_all.get("window_start_ns")
    raw_we = cfg_all.get("window_end_ns")
    for label, raw in (("window_start_ns", raw_ws),
                       ("window_end_ns", raw_we),
                       ("decision_time_ns",
                        cfg_all.get("decision_time_ns"))):
        if raw is not None and bc.to_ns(raw) is None:
            return [bc.make_result(
                metric=m, value=None, unit=METRIC_UNITS[m],
                status="error", reason="malformed:%s" % label,
                analysis_id=ANALYSIS_ID,
                revision=bc.revision_id(m, "window", raw,
                                        ALGORITHM_VERSION))
                for m in SUPPORTED_METRICS]
    window = (bc.to_ns(raw_ws), bc.to_ns(raw_we))
    if window[0] is not None and window[1] is not None \
            and window[1] < window[0]:
        return [bc.make_result(
            metric=m, value=None, unit=METRIC_UNITS[m], status="error",
            reason="malformed:window_order", analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "window", window,
                                    ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    decision = bc.to_ns(cfg_all.get("decision_time_ns"))
    raw_ecfg = cfg_all.get("energy", {})
    ecfg = raw_ecfg if isinstance(raw_ecfg, dict) else {}
    try:
        parsed = _parse_config(ecfg)
    except EnergyError as exc:
        return [bc.make_result(
            metric=m, value=None, unit=METRIC_UNITS[m], status="error",
            reason=str(exc), window_start_ns=window[0],
            window_end_ns=window[1], analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "config", str(exc),
                                    ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    try:
        sign = _parse_sign(ecfg.get("current_sign"))
        sign_error = None
    except EnergyError as exc:
        sign = None
        sign_error = str(exc)
    fields = parsed["fields"]
    try:
        field_cals = _parse_field_calibrations(ecfg, fields)
        cal_error = None
    except EnergyError as exc:
        field_cals = {}
        cal_error = str(exc)
    try:
        reference = _parse_reference(ecfg.get("reference"))
        ref_error = None
    except EnergyError as exc:
        reference = None
        ref_error = str(exc)
    try:
        uncertainties = _parse_uncertainties(ecfg)
        unc_error = None
    except EnergyError as exc:
        uncertainties = {"soc_uncertainty_pct": None,
                         "energy_uncertainty_kwh": None}
        unc_error = str(exc)
    rows_all = _scope_rows(signals)
    if decision is not None:
        rows_all = [r for r in rows_all
                    if r["event_time_ns"] <= decision
                    and r.get("ingest_time_ns") is not None
                    and r["ingest_time_ns"] <= decision]
    # Retained context: keep pre-window history for session continuity,
    # drop only samples past window_end. Window-scoped metrics below
    # filter to the in-window span explicitly.
    context_all = [r for r in rows_all
                   if window[1] is None or r["event_time_ns"] <= window[1]]
    if not context_all:
        return [bc.make_result(
            metric=m, value=None, unit=METRIC_UNITS[m],
            status="unavailable", reason="no_signals",
            window_start_ns=window[0], window_end_ns=window[1],
            analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "empty", ecfg, ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    out = []
    for scope in sorted(bc.group_by_scope(context_all).items(), key=lambda item: repr(item[0])):
        key, srows = scope
        ordered = bc.sort_dedup(srows)
        out.extend(_analyze_scope(
            key, ordered, window, parsed, sign, sign_error, field_cals,
            cal_error, reference, ref_error, uncertainties,
            unc_error, ecfg))
    return out


def _unavailable(scope, window, metric, reason, evidence, ecfg,
                 cal_ver=None, samples=None):
    return bc.make_result(
        metric=metric, value=None, unit=METRIC_UNITS[metric],
        status="unavailable", reason=reason,
        window_start_ns=window[0], window_end_ns=window[1],
        vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
        evidence_count=evidence, sample_count=samples,
        algorithm_version=ALGORITHM_VERSION,
        calibration_version=cal_ver, analysis_id=ANALYSIS_ID,
        revision=bc.revision_id(metric, scope, reason, evidence, ecfg,
                                ALGORITHM_VERSION))


def _derived(scope, window, metric, value, unit, reason, evidence,
             ecfg, cal_ver=None, samples=None, coverage=None,
             uncertainty=None, uncertainty_lower=None,
             uncertainty_upper=None, value_text=None, episode_id=None,
             analysis_id=None):
    if not bc.is_finite_number(value):
        return _unavailable(scope, window, metric, "non_finite_result",
                            evidence, ecfg, cal_ver, samples)
    return bc.make_result(
        metric=metric, value=value, unit=unit, status="derived",
        reason=reason, window_start_ns=window[0],
        window_end_ns=window[1], vehicle=scope[0], source=scope[1],
        decode_epoch=scope[2], evidence_count=evidence,
        sample_count=samples, coverage_ratio=coverage,
        algorithm_version=ALGORITHM_VERSION,
        calibration_version=cal_ver, uncertainty=uncertainty,
        uncertainty_lower=uncertainty_lower,
        uncertainty_upper=uncertainty_upper, value_text=value_text,
        episode_id=episode_id,
        analysis_id=analysis_id or ANALYSIS_ID,
        revision=bc.revision_id(metric, scope, value, reason, evidence,
                                ecfg, ALGORITHM_VERSION))


def _error_row(scope, window, metric, reason, ecfg):
    return bc.make_result(
        metric=metric, value=None, unit=METRIC_UNITS[metric],
        status="error", reason=reason, window_start_ns=window[0],
        window_end_ns=window[1], vehicle=scope[0], source=scope[1],
        decode_epoch=scope[2], analysis_id=ANALYSIS_ID,
        revision=bc.revision_id(metric, scope, "error", reason, ecfg,
                                ALGORITHM_VERSION))


def _in_window(tst, window):
    return (window[0] is None or tst >= window[0]) \
        and (window[1] is None or tst <= window[1])


def _analyze_scope(scope, ordered, window, parsed, sign, sign_error,
                   field_cals, cal_error, reference, ref_error,
                   uncertainties, unc_error, ecfg):
    fields = parsed["fields"]
    skew = parsed["max_skew_ns"]
    gap = parsed["max_gap_ns"]
    out = []
    if ref_error is None and (reference is None
                              or parsed["reference_source"] == "bms_first"):
        reference = _bms_reference(ordered, fields["nominal_pack_field"],
                                   parsed) or reference
    # Window-scoped pairs for counters/SOC/energy: unmatched boundaries
    # refuse deltas even when retained context exists elsewhere.
    win_rows = [r for r in ordered if _in_window(r["event_time_ns"],
                                                 window)]
    soc_pairs = _num_pairs(win_rows, fields["soc_field"], "%")
    remain_pairs = _num_pairs(win_rows, fields["energy_remaining_field"],
                              "kWh")
    dc_pairs = _num_pairs(win_rows, fields["dc_counter_field"], "kWh")
    ac_pairs = _num_pairs(win_rows, fields["ac_counter_field"], "kWh")
    dis_pairs = _num_pairs(win_rows, fields["discharge_counter_field"],
                           "kWh")
    cur_entries = field_cals.get(fields["current_field"], [])
    vol_entries = field_cals.get(fields["voltage_field"], [])
    cur_pairs_all, cur_raw_all = _calibrated_current(
        ordered, fields["current_field"], cur_entries)
    volt_rows_all, volt_raw_all = _calibrated_voltage_rows(
        ordered, fields["voltage_field"], vol_entries)
    cur_pairs = [(t, v) for t, v in cur_pairs_all if _in_window(t, window)]
    cur_raw_win = sum(1 for r in win_rows
                      if r.get("source_field") == fields["current_field"]
                      and r.get("value_num") is not None
                      and bc.is_valid_quality(r.get("quality"))
                      and r.get("unit") != "A"
                      and not (r.get("unit") is None and cur_entries))
    volt_rows = [r for r in volt_rows_all
                 if _in_window(r["event_time_ns"], window)]
    volt_raw_win = sum(1 for r in win_rows
                       if r.get("source_field") == fields["voltage_field"]
                       and r.get("value_num") is not None
                       and bc.is_valid_quality(r.get("quality"))
                       and r.get("unit") != "V"
                       and not (r.get("unit") is None and vol_entries))
    # Retained context bounds both directions; window integrals stay separate.
    raw_current = [(t, sign * v if v is not None else None)
                   for t, v in cur_pairs_all] if sign is not None else []
    sessions, session_rows = _session_results(
        scope, window, ordered, raw_current, fields, gap, ecfg)
    out.extend(session_rows)
    counter_rows = _counter_results(scope, window, ordered, dc_pairs,
                                    ac_pairs, dis_pairs, fields, gap, ecfg)
    out.extend(counter_rows)
    out.append(_parked_discharge(scope, window, ordered, dis_pairs,
                                 fields["discharge_counter_field"], gap,
                                 parsed["max_offline_gap_ns"], ecfg))
    counter_deltas = _counter_deltas_for_efc(counter_rows)
    cal_ver_vi = _cal_version(vol_entries, cur_entries)
    cal_ver_i = _cal_version(cur_entries)
    signed_current = [(tst, sign * val if val is not None else None) for tst, val in cur_pairs] \
        if sign is not None else []
    vi_steps = []
    vi_note = None
    if sign_error is not None or sign is None or cal_error is not None \
            or not any(r["value_num"] is not None for r in volt_rows) \
            or not any(v is not None for _, v in cur_pairs):
        if sign_error is not None:
            vi_note = sign_error
        elif sign is None:
            vi_note = "missing_calibration:current_sign"
        elif cal_error is not None:
            vi_note = cal_error
        elif cur_entries and not _calibration_for(
                field_cals, fields["current_field"], scope):
            vi_note = "scope_mismatch:field_calibration_%s" % (
                _calibration_scope_note(
                    scope, field_cals, fields["current_field"]),)
        elif vol_entries and not _calibration_for(
                field_cals, fields["voltage_field"], scope):
            vi_note = "scope_mismatch:field_calibration_%s" % (
                _calibration_scope_note(
                    scope, field_cals, fields["voltage_field"]),)
        elif not any(r["value_num"] is not None for r in volt_rows) and volt_raw_win:
            vi_note = "missing_calibration:voltage_units"
        elif not any(v is not None for _, v in cur_pairs) and cur_raw_win:
            vi_note = "missing_calibration:current_units"
        else:
            vi_note = "sparse:no_voltage_current"
    if vi_note is None:
        cur_exact = {}
        for tst, val in cur_pairs:
            cur_exact.setdefault(tst, set()).add(val)
        for tst, vals in cur_exact.items():
            if len(vals) > 1:
                vi_note = "ambiguous:current_samples_conflict"
                break
    if vi_note is None:
        vol_exact = {}
        for r in volt_rows:
            vol_exact.setdefault(r["event_time_ns"], set()).add(
                r["value_num"])
        for tst, vals in vol_exact.items():
            if len(vals) > 1:
                vi_note = "ambiguous:voltage_samples_conflict"
                break
    if vi_note is None:
        irows = []
        for tst in sorted(cur_exact):
            irows.append({"event_time_ns": tst, "vehicle": scope[0],
                          "source": scope[1], "decode_epoch": scope[2],
                          "value_num": next(iter(cur_exact[tst])),
                          "quality": None})
        joined = bc.join_asof(
            sorted(volt_rows, key=lambda r: r["event_time_ns"]),
            irows, skew)
        for vrow, irow in joined:
            vi_steps.append((vrow["event_time_ns"],
                             vrow["value_num"],
                             sign * irow["value_num"] if irow is not None else None))
        # Current-only barriers must also interrupt the voltage sampling grid.
        vi_steps.extend((t, None, None) for t, v in cur_pairs if v is None)
        vi_steps.sort(key=lambda p: p[0])
    if vi_note is not None:
        for metric in ("battery.energy.vi_charge_energy_kwh",
                       "battery.energy.vi_discharge_energy_kwh"):
            out.append(_unavailable(scope, window, metric, vi_note,
                                    len(vi_steps) + volt_raw_win
                                    + cur_raw_win,
                                    ecfg, cal_ver_vi))
        vi_res = None
    elif len(vi_steps) < 2:
        for metric in ("battery.energy.vi_charge_energy_kwh",
                       "battery.energy.vi_discharge_energy_kwh"):
            out.append(_unavailable(
                scope, window, metric,
                "unsynchronized:voltage_current_skew", len(vi_steps),
                ecfg, cal_ver_vi))
        vi_res = None
    else:
        vi_res = split_power_legs(vi_steps, gap)
        if vi_res["legs_rejected"]:
            for metric in ("battery.energy.vi_charge_energy_kwh",
                           "battery.energy.vi_discharge_energy_kwh"):
                out.append(_unavailable(
                    scope, window, metric,
                    "incomplete_window:legs_rejected_%d_coverage_unproven"
                    % vi_res["legs_rejected"], len(vi_steps), ecfg,
                    cal_ver_vi, vi_res["legs_used"]))
            vi_res = None
        else:
            cov = vi_res["coverage_ratio"]
            out.append(_derived(
                scope, window, "battery.energy.vi_charge_energy_kwh",
                vi_res["charge_kwh"], "kWh",
                "vi_integration charge_legs=%d coverage=%r"
                % (vi_res["legs_used"], cov), len(vi_steps), ecfg,
                cal_ver_vi, vi_res["legs_used"], cov))
            out.append(_derived(
                scope, window, "battery.energy.vi_discharge_energy_kwh",
                vi_res["discharge_kwh"], "kWh",
                "vi_integration discharge_legs=%d coverage=%r"
                % (vi_res["legs_used"], cov), len(vi_steps), ecfg,
                cal_ver_vi, vi_res["legs_used"], cov))
    if sign_error is not None or sign is None or cal_error is not None \
            or not any(v is not None for _, v in cur_pairs):
        if sign_error is not None:
            ah_note = sign_error
        elif sign is None:
            ah_note = "missing_calibration:current_sign"
        elif cal_error is not None:
            ah_note = cal_error
        elif cur_entries and not _calibration_for(
                field_cals, fields["current_field"], scope):
            ah_note = "scope_mismatch:field_calibration_%s" % (
                _calibration_scope_note(
                    scope, field_cals, fields["current_field"]),)
        elif cur_raw_win:
            ah_note = "missing_calibration:current_units"
        else:
            ah_note = "sparse:no_current"
        for metric in ("battery.energy.charge_throughput_ah",
                       "battery.energy.discharge_throughput_ah"):
            out.append(_unavailable(scope, window, metric, ah_note,
                                    len(cur_pairs) + cur_raw_win, ecfg,
                                    cal_ver_i))
        ah_res = None
    elif len(signed_current) < 2:
        for metric in ("battery.energy.charge_throughput_ah",
                       "battery.energy.discharge_throughput_ah"):
            out.append(_unavailable(scope, window, metric,
                                    "sparse:single_current_sample",
                                    len(signed_current), ecfg, cal_ver_i))
        ah_res = None
    else:
        cur_conflict = {}
        for tst, val in cur_pairs:
            cur_conflict.setdefault(tst, set()).add(val)
        if any(len(v) > 1 for v in cur_conflict.values()):
            for metric in ("battery.energy.charge_throughput_ah",
                           "battery.energy.discharge_throughput_ah"):
                out.append(_unavailable(
                    scope, window, metric,
                    "ambiguous:current_samples_conflict",
                    len(signed_current), ecfg, cal_ver_i))
            ah_res = None
        else:
            ah_res = split_current_legs(signed_current, gap)
            if ah_res["legs_rejected"]:
                for metric in ("battery.energy.charge_throughput_ah",
                               "battery.energy.discharge_throughput_ah"):
                    out.append(_unavailable(
                        scope, window, metric,
                        "incomplete_window:legs_rejected_%d_coverage_unproven"
                        % ah_res["legs_rejected"], len(signed_current),
                        ecfg, cal_ver_i, ah_res["legs_used"]))
                ah_res = None
            else:
                cov = ah_res["coverage_ratio"]
                out.append(_derived(
                    scope, window, "battery.energy.charge_throughput_ah",
                    ah_res["charge_ah"], "Ah",
                    "current_integration charge_legs=%d coverage=%r"
                    % (ah_res["legs_used"], cov), len(signed_current),
                    ecfg, cal_ver_i, ah_res["legs_used"], cov))
                out.append(_derived(
                    scope, window, "battery.energy.discharge_throughput_ah",
                    ah_res["discharge_ah"], "Ah",
                    "current_integration discharge_legs=%d coverage=%r"
                    % (ah_res["legs_used"], cov), len(signed_current),
                    ecfg, cal_ver_i, ah_res["legs_used"], cov))
    out.extend(_efc_results(scope, window, ah_res, vi_res, counter_deltas,
                            ecfg, reference, ref_error, cal_ver_vi))
    interval = _interval_result(scope, window, sessions, soc_pairs,
                                parsed, uncertainties, unc_error, ecfg)
    out.append(interval["row"])
    full_capacity = _interval_result(scope, window, sessions, soc_pairs,
                                    parsed, uncertainties, unc_error, ecfg, full=True)
    out.append(full_capacity["row"])
    out.extend(_circular_results(scope, window, remain_pairs, soc_pairs,
                                 parsed, uncertainties, unc_error,
                                 fields, ecfg))
    out.extend(_trend_soh_results(scope, window, full_capacity, reference,
                                  ref_error, parsed, ecfg))
    out.append(_estimated_soh(scope, window, interval, reference, ref_error,
                              parsed, uncertainties, unc_error, ecfg))
    if sign_error is not None:
        out = [_promote_sign_error(r, sign_error) for r in out]
    if cal_error is not None:
        out = [_promote_cal_error(r, cal_error) for r in out]
    return out


def _bms_reference(ordered, field, parsed):
    """Latest valid BMS nominal full-pack energy as a fixed reference.

    Reported by the vehicle, so SOH against it is relative to the BMS's
    own nominal, not an independent new-pack measurement. The reference
    domain is the analysis domain (same pack), with no conditions.
    """
    pairs = [(t, v) for t, v in _num_pairs(ordered, field, "kWh")
             if v is not None and v > 0.0]
    if not pairs:
        return None
    return {"version": "bms_nominal_full_pack", "domain": parsed.get("domain"),
            "energy_kwh": pairs[-1][1], "charge_ah": None, "conditions": None,
            "asof_ns": pairs[-1][0]}


def _estimated_soh(scope, window, interval, reference, ref_error, parsed,
                   uncertainties, unc_error, ecfg):
    """Partial-window SOH estimate: interval capacity / reference energy.

    Needs a complete monotonic session spanning soh_min_soc_span_pct. The
    interval uncertainty uses configured stds, else the documented
    estimate defaults; the result is always labelled an estimate.
    """
    metric = "battery.energy.soh_estimated_pct"
    if ref_error is not None:
        return _error_row(scope, window, metric, ref_error, ecfg)
    if unc_error is not None:
        return _error_row(scope, window, metric, unc_error, ecfg)
    if reference is None or reference.get("energy_kwh") is None:
        return _unavailable(scope, window, metric,
                            "reference_absent:soh_needs_reference_energy", 0,
                            ecfg)
    cap, energy, span = (interval.get("capacity_kwh"),
                         interval.get("energy_kwh"), interval.get("soc_span01"))
    if cap is None or span is None:
        return _unavailable(scope, window, metric,
                            "sparse:no_complete_session_with_soc_span", 0,
                            ecfg, reference["version"])
    if span < parsed["soh_min_soc_span_pct"] / 100.0:
        return _unavailable(scope, window, metric,
                            "soc_span_too_small:min_%r_pct"
                            % parsed["soh_min_soc_span_pct"], 1, ecfg,
                            reference["version"])
    soc_std = uncertainties.get("soc_uncertainty_pct")
    energy_std = uncertainties.get("energy_uncertainty_kwh")
    source = "configured"
    if soc_std is None or energy_std is None:
        soc_std, energy_std = (DEFAULT_ESTIMATE_SOC_STD_PCT,
                               DEFAULT_ESTIMATE_ENERGY_STD_KWH)
        source = "default"
    sigma = capacity_uncertainty_kwh(cap, energy, energy_std, span,
                                     soc_std / 100.0)
    ref = reference["energy_kwh"]
    soh = cap / ref * 100.0
    soh_sigma = sigma / ref * 100.0 if sigma is not None else None
    return _derived(
        scope, window, metric, soh, "%",
        "estimated_partial_window ref=%s:%r uncertainty=%s;%s" % (
            reference["version"], ref, source, interval["row"]["reason"]),
        1, ecfg, reference["version"], uncertainty=soh_sigma,
        uncertainty_lower=soh - soh_sigma if soh_sigma is not None else None,
        uncertainty_upper=soh + soh_sigma if soh_sigma is not None else None)


def _promote_sign_error(row, sign_error):
    if row["metric"] in SIGN_METRICS and row["status"] == "unavailable":
        fixed = dict(row)
        fixed["status"] = "error"
        fixed["reason"] = sign_error
        fixed["value"] = None
        return fixed
    return row


def _promote_cal_error(row, cal_error):
    if row["metric"] in CAL_METRICS and row["status"] == "unavailable":
        fixed = dict(row)
        fixed["status"] = "error"
        fixed["reason"] = cal_error
        fixed["value"] = None
        return fixed
    return row


def _bounded_meter(pairs, start, end, gap):
    span = [(t, value) for t, value in pairs if start <= t <= end]
    if len(span) < 2 or span[0][0] != start or span[-1][0] != end:
        return None, "unmatched_boundary", span
    if any(b[0] - a[0] > gap for a, b in zip(span, span[1:])):
        return None, "gap", span
    value, reason = meter_delta(span)
    return value, reason, span


def _session_results(scope, window, ordered, signed_current, fields, gap, ecfg):
    charge = _num_pairs(ordered, fields["session_energy_field"], "kWh")
    discharge = _num_pairs(ordered, fields["discharge_counter_field"], "kWh")
    soc = _num_pairs(ordered, fields["soc_field"], "%")
    current = dict(signed_current)
    power = dict(_num_pairs(ordered, fields["power_field"], "kW"))
    texts = [dict(_text_series(ordered, fields[key])) for key in
             ("charging_state_field", "detailed_charge_field", "bms_state_field")]
    stamps = sorted(set(current) | set(power) | {t for t, _ in charge + discharge}
                    | {t for series in texts for t in series})
    previous_text = [None] * len(texts)
    points, previous = [], None
    previous_direction = None
    skew = ecfg.get("max_skew_ns", DEFAULT_MAX_SKEW_NS)
    for stamp in stamps:
        direction = None
        c, d = _asof(charge, stamp, skew), _asof(discharge, stamp, skew)
        if stamp in current:
            value = current[stamp]
            if value is not None:
                direction = 1 if value > 0 else -1 if value < 0 else 0
        elif stamp in power:
            value = power[stamp]
            if value is not None and value >= 0:
                direction = 1 if value > 0 else 0
        elif previous is not None and stamp - previous[0] <= gap:
            dc = c - previous[1] if c is not None and previous[1] is not None else None
            dd = d - previous[2] if d is not None and previous[2] is not None else None
            if (dc is None or dc >= 0) and (dd is None or dd >= 0):
                if dc is not None and dc > 0 and (dd is None or dd == 0):
                    direction = 1
                elif dd is not None and dd > 0 and (dc is None or dc == 0):
                    direction = -1
                elif dc == 0 and dd == 0:
                    direction = 0
        for index, series in enumerate(texts):
            if stamp not in series:
                continue
            value = series[stamp]
            if value is None or (previous_text[index] is not None
                                 and value != previous_text[index] and direction != 0
                                 and previous_direction not in (None, 0)):
                direction = None
            previous_text[index] = value
        points.append({"t": stamp, "direction": direction})
        previous = (stamp, c, d)
        previous_direction = direction
    sessions = segment_sessions(points, gap)
    rows, enriched = [], []
    for session in sessions:
        start, end = session["start_ns"], session["end_ns"]
        direction = "charge" if session["direction"] > 0 else "discharge"
        metric = "battery.energy.%s_session_energy_kwh" % direction
        meter = charge if session["direction"] > 0 else discharge
        value, reason, span = _bounded_meter(meter, start, end, gap)
        if session["incomplete_start"] or session["incomplete_end"]:
            value, reason = None, "incomplete_boundary:" + session["close_reason"]
        soc_span = [(t, v) for t, v in soc if start <= t <= end]
        soc0, soc1 = _asof(soc, start, skew), _asof(soc, end, skew)
        if any(v is None or not 0 <= v <= 100 for _, v in soc_span) or any(
                b[0] - a[0] > gap for a, b in zip(soc_span, soc_span[1:])):
            soc0 = soc1 = None
        enriched.append({"session": session, "meter_pairs": span,
                         "energy_kwh": value, "soc0_pct": soc0, "soc1_pct": soc1,
                         "soc_pairs": soc_span})
        # Complete sessions are credited once, in their closure window.
        if not _in_window(end, window):
            continue
        episode = bc.episode_key(scope[0], "energy", direction + "_session", start, scope[2])
        text = "%s start_ns=%d end_ns=%d close=%s" % (
            direction, start, end, session["close_reason"])
        if value is None:
            row = _unavailable(scope, window, metric, reason, len(span), ecfg)
        else:
            row = _derived(scope, window, metric, value, "kWh",
                           "completed_session:independent_counter", len(span), ecfg)
        row.update(value_text=text, episode_id=episode,
                   analysis_id=_session_analysis_id(episode))
        row["revision"] = bc.revision_id(metric, scope, session, span, ecfg, ALGORITHM_VERSION)
        rows.append(row)
    for direction in ("charge", "discharge"):
        metric = "battery.energy.%s_session_energy_kwh" % direction
        if not any(row["metric"] == metric for row in rows):
            rows.append(_unavailable(scope, window, metric, "no_sessions", len(points), ecfg))
    return enriched, rows


def _with_prior_baseline(ordered, field, window, pairs, gap):
    """Prepend the last pre-window reading as the delta anchor.

    Only a valid, unambiguous reading within gap of the first in-window
    sample anchors; an invalid/conflicting latest prior reading is a
    barrier, so the window falls back to its own span.
    """
    if window[0] is None or not pairs:
        return pairs, False
    prior = _num_pairs([r for r in ordered
                        if r["event_time_ns"] < window[0]], field, "kWh")
    if not prior:
        return pairs, False
    tst, value = prior[-1]
    if value is None or pairs[0][0] - tst > gap:
        return pairs, False
    return [(tst, value)] + pairs, True


def _counter_results(scope, window, ordered, dc_pairs, ac_pairs, dis_pairs,
                     fields, gap, ecfg):
    out = []
    for metric, win_pairs, field, note in (
            ("battery.energy.dc_charging_energy_in_kwh", dc_pairs,
             fields["dc_counter_field"],
             "counter_delta battery_side_ac_and_dc_meter"),
            ("battery.energy.ac_charging_energy_in_kwh", ac_pairs,
             fields["ac_counter_field"],
             "counter_delta charger_side_ac_only_meter"),
            ("battery.energy.discharge_energy_kwh", dis_pairs,
             fields["discharge_counter_field"],
             "counter_delta discharging_meter")):
        pairs, anchored = _with_prior_baseline(ordered, field, window,
                                               win_pairs, gap)
        if anchored:
            note += " anchor=prior_window"
        delta, info = meter_delta(pairs)
        if any(b[0] - a[0] > gap for a, b in zip(pairs, pairs[1:])):
            delta, info = None, "gap"
        if info == "ok":
            out.append(_derived(scope, window, metric, delta, "kWh",
                                note, len(pairs), ecfg))
        elif info == "reset":
            out.append(_unavailable(scope, window, metric,
                                    "reset:%s" % field, len(pairs),
                                    ecfg))
        elif info == "ambiguous":
            out.append(_unavailable(scope, window, metric,
                                    "ambiguous:%s" % field, len(pairs),
                                    ecfg))
        else:
            out.append(_unavailable(scope, window, metric,
                                    "sparse:no_%s_span" % field,
                                    len(pairs), ecfg))
    return out


def _parked_discharge(scope, window, ordered, win_pairs, field, gap,
                      max_offline, ecfg):
    """Discharge-meter increase across a reporting gap (vehicle offline).

    Hourly counter deltas stop at gaps wider than max_gap_ns, so energy used
    while parked/asleep lands in no window. This credits that single leg
    (last valid reading before the gap -> first in-window reading after it)
    to the window where reporting resumes, so window deltas plus parked legs
    partition the meter. Invalid/conflicting endpoints, decreases and gaps
    beyond max_offline stay unavailable.
    """
    metric = "battery.energy.parked_discharge_kwh"
    if window[0] is None or not win_pairs:
        return _unavailable(scope, window, metric, "sparse:no_resume_reading",
                            len(win_pairs), ecfg)
    t1, v1 = win_pairs[0]
    prior = _num_pairs([r for r in ordered if r["event_time_ns"] < t1],
                       field, "kWh")
    if not prior or v1 is None or prior[-1][1] is None:
        return _unavailable(scope, window, metric,
                            "sparse:no_valid_gap_endpoints", len(win_pairs),
                            ecfg)
    t0, v0 = prior[-1]
    if t1 - t0 <= gap:
        return _unavailable(scope, window, metric, "no_offline_gap",
                            len(win_pairs), ecfg)
    if t1 - t0 > max_offline:
        return _unavailable(scope, window, metric,
                            "gap_exceeded:max_offline_gap_ns", 2, ecfg)
    delta, info = meter_delta([(t0, v0), (t1, v1)])
    if info != "ok":
        return _unavailable(scope, window, metric, "%s:%s" % (info, field),
                            2, ecfg)
    return _derived(scope, window, metric, delta, "kWh",
                    "counter_delta offline_gap_s=%d" % ((t1 - t0) // 10**9),
                    2, ecfg)


def _counter_deltas_for_efc(counter_rows):
    deltas = {}
    for row in counter_rows:
        if row.get("status") != "derived":
            continue
        if row["metric"] == "battery.energy.dc_charging_energy_in_kwh":
            deltas["charge"] = row["value"]
        elif row["metric"] == "battery.energy.discharge_energy_kwh":
            deltas["discharge"] = row["value"]
    return deltas


def _efc_results(scope, window, ah_res, vi_res, counter_deltas, ecfg,
                 reference, ref_error, cal_ver):
    oneway_m = "battery.energy.efc_oneway_cycles"
    bi_m = "battery.energy.efc_bidirectional_cycles"
    if ref_error is not None:
        return [_error_row(scope, window, oneway_m, ref_error, ecfg),
                _error_row(scope, window, bi_m, ref_error, ecfg)]
    if reference is None:
        return [_unavailable(scope, window, oneway_m,
                             "reference_absent:efc_needs_fixed_reference",
                             0, ecfg),
                _unavailable(scope, window, bi_m,
                             "reference_absent:efc_needs_fixed_reference",
                             0, ecfg)]
    # Exactly one throughput domain per run: Ah, else window counters,
    # else V*I. Counter and integral are never both credited.
    charge = discharge = ref = None
    domain = None
    version = reference["version"]
    if ah_res is not None and reference.get("charge_ah") is not None:
        charge, discharge = ah_res["charge_ah"], ah_res["discharge_ah"]
        ref = reference["charge_ah"]
        domain = "ah_throughput"
        version = "%s+%s" % (reference["version"], cal_ver or "current")
    elif counter_deltas.get("charge") is not None \
            and counter_deltas.get("discharge") is not None \
            and reference.get("energy_kwh") is not None:
        charge = counter_deltas["charge"]
        discharge = counter_deltas["discharge"]
        ref = reference["energy_kwh"]
        domain = "window_counters"
    elif vi_res is not None and reference.get("energy_kwh") is not None:
        charge, discharge = vi_res["charge_kwh"], vi_res["discharge_kwh"]
        ref = reference["energy_kwh"]
        domain = "vi_integration"
        version = "%s+%s" % (reference["version"], cal_ver or "vi")
    else:
        return [_unavailable(scope, window, oneway_m,
                             "sparse:efc_needs_one_throughput_domain",
                             0, ecfg, reference["version"]),
                _unavailable(scope, window, bi_m,
                             "sparse:efc_needs_one_throughput_domain",
                             0, ecfg, reference["version"])]
    try:
        res = efc_cycles(charge, discharge, ref)
    except EnergyError as exc:
        return [_error_row(scope, window, oneway_m, str(exc), ecfg),
                _error_row(scope, window, bi_m, str(exc), ecfg)]
    reason = ("efc domain=%s charge=%r discharge=%r ref=%r" % (
        domain, charge, discharge, ref))
    return [
        _derived(scope, window, oneway_m, res["oneway"], "cycles",
                 reason + " one_direction_discharge_over_ref", 2, ecfg,
                 version),
        _derived(scope, window, bi_m, res["bidirectional"], "cycles",
                 reason + " absolute_total_over_twice_ref", 2, ecfg,
                 version)]


def _interval_result(scope, window, sessions, soc_pairs, parsed,
                     uncertainties, unc_error, ecfg, full=False):
    metric = "battery.energy.%s_capacity_kwh" % ("full_usable" if full else "interval")
    empty = {"capacity_kwh": None, "energy_kwh": None,
             "soc_span01": None, "session": None}
    if unc_error is not None:
        return dict(empty, row=_error_row(scope, window, metric, unc_error, ecfg))
    for entry in reversed(sessions):
        energy = entry["energy_kwh"]
        soc0, soc1 = entry["soc0_pct"], entry["soc1_pct"]
        session = entry["session"]
        if energy is None or energy <= 0 or soc0 is None or soc1 is None:
            continue
        direction = session["direction"]
        span = direction * (soc1 - soc0) / 100
        samples = entry["soc_pairs"]
        if span < parsed["min_soc_span_pct"] / 100 or not samples:
            continue
        if any(direction * (b[1] - a[1]) < 0 for a, b in zip(samples, samples[1:])):
            continue
        if full and (direction != -1 or soc0 != 100 or soc1 != 0
                     or parsed["fields"]["discharge_counter_field"] == "EnergyRemaining"
                     or samples[0][0] != session["start_ns"]
                     or samples[-1][0] != session["end_ns"]):
            continue
        cap = energy if full else interval_capacity_kwh(energy, span)
        sigma = capacity_uncertainty_kwh(
            cap, energy, uncertainties.get("energy_uncertainty_kwh"), span,
            uncertainties["soc_uncertainty_pct"] / 100
            if uncertainties.get("soc_uncertainty_pct") is not None else None)
        reason = "%s asof_ns=%d start_ns=%d span_soc=%r" % (
            "full_usable:independent_discharge" if full else "interval_equivalent:not_absolute_soh",
            session["end_ns"], session["start_ns"], span)
        return {"row": _derived(
            scope, window, metric, cap, "kWh", reason, len(samples), ecfg,
            uncertainty=sigma, uncertainty_lower=cap-sigma if sigma is not None else None,
            uncertainty_upper=cap+sigma if sigma is not None else None),
            "capacity_kwh": cap, "energy_kwh": energy, "soc_span01": span, "session": session}
    return dict(empty, row=_unavailable(
        scope, window, metric, "sparse:no_complete_full_discharge" if full
        else "sparse:no_complete_session_with_soc_span", len(sessions), ecfg))


def _circular_results(scope, window, remain_pairs, soc_pairs, parsed,
                      uncertainties, unc_error, fields, ecfg):
    metric = "battery.energy.bms_circular_capacity_kwh"
    if unc_error is not None:
        return [_error_row(scope, window, metric, unc_error, ecfg)]
    if len(remain_pairs) < 2 or len(soc_pairs) < 2:
        return [_unavailable(scope, window, metric,
                             "sparse:bms_needs_energy_and_soc_span",
                             len(remain_pairs) + len(soc_pairs), ecfg)]
    if any(v is None for _, v in remain_pairs + soc_pairs) or any(
            b[0] - a[0] > parsed["max_gap_ns"]
            for pairs in (remain_pairs, soc_pairs) for a, b in zip(pairs, pairs[1:])):
        return [_unavailable(scope, window, metric, "quality_or_gap_barrier",
                             len(remain_pairs) + len(soc_pairs), ecfg)]
    (t0, e0), (t1, e1) = remain_pairs[0], remain_pairs[-1]
    soc0 = _asof(soc_pairs, t0, parsed["max_skew_ns"])
    soc1 = _asof(soc_pairs, t1, parsed["max_skew_ns"])
    if soc0 is None or soc1 is None:
        return [_unavailable(scope, window, metric,
                             "unsynchronized:bms_soc_skew",
                             len(remain_pairs) + len(soc_pairs), ecfg)]
    d_e = e1 - e0
    d_s = (soc1 - soc0) / 100.0
    if d_s == 0.0:
        return [_unavailable(scope, window, metric, "zero:soc_span_zero",
                             2, ecfg)]
    if d_s < 0.0:
        return [_unavailable(scope, window, metric,
                             "inconsistent:soc_span_non_positive", 2,
                             ecfg)]
    if d_s < parsed["min_soc_span_pct"] / 100.0:
        return [_unavailable(
            scope, window, metric,
            "soc_span_too_small:min_%r_pct" % parsed["min_soc_span_pct"],
            2, ecfg)]
    cap = interval_capacity_kwh(d_e, d_s)
    if cap is None or cap <= 0.0:
        return [_unavailable(scope, window, metric,
                             "inconsistent:bms_energy_direction", 2,
                             ecfg)]
    sigma = None
    lo = hi = None
    soc_std = uncertainties.get("soc_uncertainty_pct")
    energy_std = uncertainties.get("energy_uncertainty_kwh")
    if soc_std is not None and energy_std is not None:
        sigma = capacity_uncertainty_kwh(
            cap, abs(d_e), energy_std, d_s, soc_std / 100.0)
        if sigma is not None:
            lo, hi = cap - sigma, cap + sigma
    return [_derived(
        scope, window, metric, cap, "kWh",
        "bms_circular EnergyRemaining_over_Soc not_independent_capacity",
        2, ecfg, None, 2, None, sigma, lo, hi)]


def _trend_soh_results(scope, window, interval, reference, ref_error,
                       parsed, ecfg):
    trend_m = "battery.energy.capacity_trend_kwh"
    soh_m = "battery.energy.soh_pct"
    if ref_error is not None:
        return [_error_row(scope, window, trend_m, ref_error, ecfg),
                _error_row(scope, window, soh_m, ref_error, ecfg)]
    if reference is None or reference.get("energy_kwh") is None:
        return [_unavailable(scope, window, trend_m,
                             "reference_absent:trend_needs_fixed_reference",
                             0, ecfg),
                _unavailable(scope, window, soh_m,
                             "reference_absent:soh_needs_fixed_reference",
                             0, ecfg)]
    if parsed.get("domain") != reference.get("domain"):
        return [_unavailable(scope, window, trend_m,
                             "incomparable_domain:reference_%r_window_%r"
                             % (reference.get("domain"),
                                parsed.get("domain")), 0, ecfg,
                             reference["version"]),
                _unavailable(scope, window, soh_m,
                             "incomparable_domain:reference_%r_window_%r"
                             % (reference.get("domain"),
                                parsed.get("domain")), 0, ecfg,
                             reference["version"])]
    if reference.get("conditions") is not None \
            and parsed.get("conditions") != reference.get("conditions"):
        return [_unavailable(scope, window, trend_m,
                             "incomparable_conditions:reference_%r_window_%r"
                             % (reference.get("conditions"),
                                parsed.get("conditions")), 0, ecfg,
                             reference["version"]),
                _unavailable(scope, window, soh_m,
                             "incomparable_conditions:reference_%r_window_%r"
                             % (reference.get("conditions"),
                                parsed.get("conditions")), 0, ecfg,
                             reference["version"])]
    if interval.get("capacity_kwh") is None:
        return [_unavailable(scope, window, trend_m,
                             "sparse:no_comparable_interval_capacity",
                             0, ecfg, reference["version"]),
                _unavailable(scope, window, soh_m,
                             "sparse:no_comparable_interval_capacity",
                             0, ecfg, reference["version"])]
    cap = interval["capacity_kwh"]
    trend = cap - reference["energy_kwh"]
    soh = cap / reference["energy_kwh"] * 100.0
    if not bc.is_finite_number(trend) or not bc.is_finite_number(soh):
        return [_unavailable(scope, window, trend_m, "non_finite_result",
                             1, ecfg, reference["version"]),
                _unavailable(scope, window, soh_m, "non_finite_result",
                             1, ecfg, reference["version"])]
    sigma = interval["row"]["uncertainty"]
    soh_sigma = sigma / reference["energy_kwh"] * 100 if sigma is not None else None
    provenance = interval["row"]["reason"]
    return [_derived(scope, window, trend_m, trend, "kWh",
                     "capacity_trend like_for_like_vs_fixed_reference;" + provenance, 1, ecfg,
                     reference["version"], uncertainty=sigma,
                     uncertainty_lower=trend-sigma if sigma is not None else None,
                     uncertainty_upper=trend+sigma if sigma is not None else None),
            _derived(scope, window, soh_m, soh, "%",
                     "absolute_soh like_for_like_vs_fixed_reference;" + provenance, 1, ecfg,
                     reference["version"], uncertainty=soh_sigma,
                     uncertainty_lower=soh-soh_sigma if soh_sigma is not None else None,
                     uncertainty_upper=soh+soh_sigma if soh_sigma is not None else None)]
