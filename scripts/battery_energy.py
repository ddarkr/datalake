#!/usr/bin/env python3
"""Charge/discharge energy, throughput, equivalent cycles and capacity. Stdlib only.

Config contract (config is a plain dict):
  config["window_start_ns"] / ["window_end_ns"] / ["decision_time_ns"]:
    optional integer ns echoed on rows. A present-but-malformed bound, or
    end < start, yields per-metric error rows. Signals with event_time
    before window_start are RETAINED as session context (sessions may
    start before the hourly window); only event_time > window_end is
    dropped. decision_time_ns admits only rows with event_time and
    ingest_time <= decision (online filtering; unknown ingest admitted
    when decision is None).
  config["energy"]: plain dict, all optional. Absent/not-a-dict runs
    uncalibrated (counters and sessions still work; physical integrals,
    EFC, trend and SOH report unavailable, never guessed numbers).
    "max_skew_ns": non-negative int, default 300000000000 (5 min).
      Bounded alignment for joins and session/SOC boundary matching.
    "max_gap_ns": non-negative int, default 600000000000 (10 min).
      Every integration leg and every session continuity step wider than
      this is rejected, never filled.
    "min_soc_span_pct": finite > 0, default 10.0. Minimum SOC span for
      any delta-E / delta-SOC capacity.
    Field selection (exact Fleet source_field names, never VSS path
      guesses). Defaults read the official PackCurrent/PackVoltage raw
      fields (unit NULL, quality unit_unverified) plus the aligned
      official BMSState/DetailedChargeState text fields:
      "voltage_field" default "PackVoltage",
      "current_field" default "PackCurrent",
      "soc_field" default "Soc" (BatteryLevel is displayed SOC and is
      never equated; only the configured soc field is read),
      "energy_remaining_field" default "EnergyRemaining",
      "dc_counter_field" default "DCChargingEnergyIn",
      "ac_counter_field" default "ACChargingEnergyIn",
      "discharge_counter_field" default "LifetimeEnergyUsed",
      "session_energy_field" default "ChargeEnergyAdded",
      "charging_state_field" default "ChargingState",
      "detailed_charge_field" default "DetailedChargeState",
      "bms_state_field" default "BMSState",
      "power_field" default "ChargerPower".
    PackCurrent/PackVoltage math requires explicit scoped field
    calibration (see below); without it they are evidence only.
    "current_sign": "positive_charge" (+1: positive calibrated current
      means charging) or "positive_discharge" (-1), ints 1/-1 accepted.
      Required for every V*I and Ah split. Missing leaves those metrics
      unavailable; present-but-wrong errors the sign-dependent metrics.
    "field_calibration": optional dict keyed by source_field, each entry
      {"vehicle": str non-empty, "source": str non-empty,
       "decode_epoch": str non-empty, "declared_domain": str non-empty,
       "version": str non-empty, "unit_scale": finite nonzero,
       "unit_offset": finite (default 0.0), "unit": "V" for the
       configured voltage field, "A" for the configured current field}.
      A PackCurrent/PackVoltage sample joins physical V*I/Ah math only
      when its exact (vehicle, source, decode_epoch) scope matches a
      calibration entry, the entry unit matches the expected physical
      unit, and current_sign is present. Calibrated value =
      raw * unit_scale + unit_offset. Entries never apply across scopes;
      scope mismatch reports scope_mismatch (never a cross-scope value).
      A present-but-malformed entry errors the V*I/Ah metrics
      (malformed:*); malformed entries never silently fall back.
      Unknown keys "voltage_calibration"/"current_calibration" are
      rejected as malformed field selection (canonical path only).
    "soc_uncertainty_pct" / "energy_uncertainty_kwh": optional finite
      >= 0 single-reading standard deviations. Interval/circular capacity
      uncertainty is propagated from these only; otherwise no uncertainty
      is emitted (never fabricated). Malformed values error only the two
      capacity metrics.
    "reference": optional dict {version (non-empty str), domain
      (non-empty str), energy_kwh (finite > 0) and/or charge_ah
      (finite > 0, at least one required), conditions (optional str)}.
      A fixed like-for-like new-pack reference supplied by the operator.
      Present-but-malformed errors the reference-dependent metrics
      (EFC pair, trend, SOH). Absent leaves them unavailable.
    "domain" / "conditions": optional strings describing the current
      window. Trend and SOH additionally require domain equality with
      the reference, and conditions equality when the reference states
      conditions; otherwise incomparable_* (never a like-for-like claim).

Sessions: charge sessions reduce meter/power/current evidence plus
  charging/BMS text *changes* (any change is a boundary; text values are
  never interpreted). A session opens on meter gain, observed power > 0,
  or charge-direction current; current evidence is sign-independent
  (any nonzero calibrated current opens: charge direction extends,
  discharge direction closes). Sessions close on gaps, text changes,
  meter resets, observed power returning to 0, or discharge-direction
  current. Sessions may start before the hourly window: runtime passes
  retained context, so boundary sessions keep their rows with
  incomplete_boundary (never silently dropped, never credited energy).
  Discharge segmentation stays in aggregate trip logic; this module
  reports the discharge window delta only. Each logical session gets a
  stable persisted identity: analysis_id
  "battery_energy:session:<episode_id>" where episode_id =
  bc.episode_key(vehicle, "energy", "charge_session", start_ns, epoch),
  so hourly runtime windows never collide on one constant PK.

Semantics:
  DCChargingEnergyIn is the battery-side meter (AC+DC into the pack);
  ACChargingEnergyIn is the charger-side meter (AC only). They are
  reported as distinct window deltas, never summed. LifetimeEnergyUsed
  is the discharging counter. Counter deltas reject resets and
  ambiguous same-stamp conflicts; sparse counters stay unavailable.
  Counter deltas are computed over the retained window span only;
  unmatched boundaries (fewer than two in-window counter samples)
  refuse false deltas even when retained context exists.
  V*I and Ah integrals need explicit scoped calibration plus sign,
  bounded skew/gap, and valid quality; any rejected leg makes the
  window metric unavailable (partial integrals are never presented as
  window energy). Integration covers only in-window samples; retained
  pre-window context bounds sessions, never extends the energy totals.
  Ah and kWh are separate metrics, never converted into each other.
  EFC uses exactly one throughput domain per run (Ah, then counters,
  then V*I) so the counter and the integral are never counted twice:
  oneway = discharge / ref, bidirectional = (charge + discharge) /
  (2 * ref). Interval capacity is session-meter energy over SOC span;
  EnergyRemaining/SOC is reported separately as BMS-circular, never as
  independent capacity. No initial-baseline 100% trick: SOH and trend
  exist only against the supplied reference.

Metrics (namespace battery.energy.*), all status derived when valued:
  charge_session_energy_kwh (kWh, per session, episode_id + value_text),
  dc_charging_energy_in_kwh, ac_charging_energy_in_kwh,
  discharge_energy_kwh (kWh window deltas),
  vi_charge_energy_kwh, vi_discharge_energy_kwh (kWh),
  charge_throughput_ah, discharge_throughput_ah (Ah),
  efc_oneway_cycles, efc_bidirectional_cycles (cycles),
  interval_capacity_kwh, bms_circular_capacity_kwh, capacity_trend_kwh
  (kWh), soh_pct (%).
"""

import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import battery_common as bc

CODE_VERSION = "1.0.0"
ALGORITHM_VERSION = "1.0.0"
ANALYSIS_ID = "battery_energy"

SUPPORTED_METRICS = (
    "battery.energy.charge_session_energy_kwh",
    "battery.energy.dc_charging_energy_in_kwh",
    "battery.energy.ac_charging_energy_in_kwh",
    "battery.energy.discharge_energy_kwh",
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
)

METRIC_UNITS = {
    "battery.energy.charge_session_energy_kwh": "kWh",
    "battery.energy.dc_charging_energy_in_kwh": "kWh",
    "battery.energy.ac_charging_energy_in_kwh": "kWh",
    "battery.energy.discharge_energy_kwh": "kWh",
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
}

DEFAULT_MAX_SKEW_NS = 300000000000
DEFAULT_MAX_GAP_NS = 600000000000
DEFAULT_MIN_SOC_SPAN_PCT = 10.0

FIELD_DEFAULTS = {
    "voltage_field": "PackVoltage",
    "current_field": "PackCurrent",
    "soc_field": "Soc",
    "energy_remaining_field": "EnergyRemaining",
    "dc_counter_field": "DCChargingEnergyIn",
    "ac_counter_field": "ACChargingEnergyIn",
    "discharge_counter_field": "LifetimeEnergyUsed",
    "session_energy_field": "ChargeEnergyAdded",
    "charging_state_field": "ChargingState",
    "detailed_charge_field": "DetailedChargeState",
    "bms_state_field": "BMSState",
    "power_field": "ChargerPower",
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
    span = _finite(ecfg.get("min_soc_span_pct", DEFAULT_MIN_SOC_SPAN_PCT))
    if span is None or span <= 0.0:
        raise EnergyError("malformed: min_soc_span_pct finite > 0")
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
    return {"max_skew_ns": skew, "max_gap_ns": gap,
            "min_soc_span_pct": span, "fields": fields,
            "domain": domain, "conditions": conditions}


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
        energy = (w0 * c0 + w1 * c1) / 2.0 * (t1 - t0) / 3.6e15
        if not bc.is_finite_number(energy):
            rejected += 1
            continue
        avg = (c0 + c1) / 2.0
        if avg > 0.0:
            charge += energy
        elif avg < 0.0:
            dis -= energy
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
        ahs = (c0 + c1) / 2.0 * (t1 - t0) / 3.6e12
        if not bc.is_finite_number(ahs):
            rejected += 1
            continue
        if ahs > 0.0:
            charge += ahs
        elif ahs < 0.0:
            dis -= ahs
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
    """Reduce a merged charge-evidence timeline into charge sessions.

    points: list of dicts {t (ns), powers [...], meters [...],
      icharges [...] (raw calibrated amps, sign-independent),
      ctext/dtext/btext (deterministic joined strings or None)}.
    Same-timestamp conflicts stay visible: straddling meter values mark
    the point ambiguous, joined text changes mark boundaries. Current
    opens on any nonzero sample; discharge direction closes.
    Returns session dicts {start_ns, end_ns, close_reason,
      evidence (sorted kinds), incomplete_start, incomplete_end,
      end_inclusive}. Evidence kinds: meter_gain, power, current.
    """
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise EnergyError("max_gap_ns must be a non-negative int")
    merged = {}
    for point in points or []:
        if not isinstance(point, dict):
            continue
        tst = bc.to_ns(point.get("t"))
        if tst is None:
            continue
        slot = merged.setdefault(tst, {"t": tst, "powers": [],
                                       "meters": [], "icharges": [],
                                       "ctext": [], "dtext": [],
                                       "btext": []})
        for key in ("powers", "meters", "icharges"):
            for val in point.get(key) or []:
                conv = _finite(val)
                if conv is not None:
                    slot[key].append(conv)
        for key in ("ctext", "dtext", "btext"):
            val = point.get(key)
            if isinstance(val, str) and val:
                slot[key].append(val)
    ordered = [merged[tst] for tst in sorted(merged)]
    if not ordered:
        return []
    first_t = ordered[0]["t"]
    last_t = ordered[-1]["t"]
    out = []
    cur = None
    prev_t = None
    prev_meter = None
    prev_meter_t = None
    last_text = {"ctext": None, "dtext": None, "btext": None}

    def _close(reason, end_ns, inclusive):
        sess = dict(cur)
        sess["close_reason"] = reason
        sess["end_ns"] = end_ns
        sess["end_inclusive"] = inclusive
        if reason == "open_at_end":
            sess["incomplete_end"] = True
        elif reason == "gap":
            sess["incomplete_end"] = end_ns >= last_t
        else:
            sess["incomplete_end"] = False
        out.append(sess)

    for point in ordered:
        tst = point["t"]
        powers = point["powers"]
        meters = point["meters"]
        currents = point["icharges"]
        gain = False
        reset = False
        ambiguous = False
        if meters and prev_meter is not None:
            ups = [v for v in meters if v > prev_meter]
            downs = [v for v in meters if v < prev_meter]
            if ups and downs:
                ambiguous = True
            elif ups:
                gain = True
            elif downs:
                reset = True
        changed = False
        for key in ("ctext", "dtext", "btext"):
            joined = "\x1f".join(sorted(set(point[key]))) \
                if point[key] else None
            if joined is not None and last_text[key] is not None \
                    and joined != last_text[key]:
                changed = True
            point[key] = joined
        power_ev = any(v > 0.0 for v in powers)
        power_stop = bool(powers) and all(v == 0.0 for v in powers)
        nonzero = [v for v in currents if v != 0.0]
        charge_ev = bool(nonzero)
        dis_ev = any(v < 0.0 for v in currents)
        if cur is not None and prev_t is not None \
                and tst - prev_t > max_gap_ns:
            _close("gap", cur["last_evidence_ns"], True)
            cur = None
        if cur is not None and changed:
            _close("text_boundary", cur["last_evidence_ns"], True)
            cur = None
        if reset and cur is not None:
            _close("meter_reset", cur["last_evidence_ns"], True)
            cur = None
        if cur is not None and power_stop:
            _close("power_stop", tst, True)
            cur = None
            skip_open = True
        elif cur is not None and dis_ev:
            _close("discharge_current", cur["last_evidence_ns"], True)
            cur = None
            skip_open = False
        else:
            skip_open = ambiguous
            if ambiguous and cur is not None:
                _close("ambiguous_meter", cur["last_evidence_ns"], True)
                cur = None
        evidence = set()
        if gain:
            evidence.add("meter_gain")
        if power_ev:
            evidence.add("power")
        if charge_ev:
            evidence.add("current")
        if cur is None:
            if evidence and not skip_open:
                # Meter gain credits the observed baseline: the session
                # spans [prev_meter_t, tst] when that step itself is
                # gap-bounded, never across a rejected gap.
                start_ns = tst
                if "meter_gain" in evidence \
                        and prev_meter_t is not None \
                        and prev_t is not None \
                        and tst - prev_t <= max_gap_ns:
                    start_ns = prev_meter_t
                cur = {"start_ns": start_ns, "last_evidence_ns": tst,
                       "evidence": set(evidence),
                       "incomplete_start": start_ns == first_t}
        elif evidence:
            # Sessions never bridge a rejected gap: a far-apart gain
            # point starts a new session instead of extending this one.
            if "meter_gain" in evidence and prev_t is not None \
                    and tst - prev_t > max_gap_ns:
                _close("gap", cur["last_evidence_ns"], True)
                cur = {"start_ns": tst, "last_evidence_ns": tst,
                       "evidence": set(evidence),
                       "incomplete_start": tst == first_t}
            else:
                cur["last_evidence_ns"] = tst
                cur["evidence"] |= evidence
        prev_t = tst
        if meters:
            prev_meter = max(meters)
            prev_meter_t = tst
        for key in ("ctext", "dtext", "btext"):
            if point[key] is not None:
                last_text[key] = point[key]
    if cur is not None:
        _close("open_at_end", cur["last_evidence_ns"], True)
    sessions = []
    for sess in out:
        sessions.append({
            "start_ns": sess["start_ns"], "end_ns": sess["end_ns"],
            "close_reason": sess["close_reason"],
            "evidence": sorted(sess["evidence"]),
            "incomplete_start": sess["incomplete_start"],
            "incomplete_end": sess["incomplete_end"],
            "end_inclusive": sess["end_inclusive"]})
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
    return sorted((r["event_time_ns"], r["value_num"]) for r in rows
                  if r.get("source_field") == field
                  and r.get("value_num") is not None
                  and bc.is_valid_quality(r.get("quality"))
                  and r.get("unit") == unit)


def _text_series(rows, field):
    by_ts = {}
    for r in rows:
        if r.get("source_field") != field:
            continue
        if r.get("value_num") is None \
                and r.get("value_text") is None \
                and r.get("value_bool") is None:
            continue
        if not bc.is_valid_quality(r.get("quality")):
            continue
        text = r.get("value_text")
        if isinstance(text, bool) or text is None:
            text = "" if r.get("value_bool") is None \
                else ("true" if r.get("value_bool") else "false")
        if not isinstance(text, str):
            continue
        by_ts.setdefault(r["event_time_ns"], {})[repr(
            (r.get("value_num"), text, r.get("value_bool")))] = text
    out = []
    for tst in sorted(by_ts):
        vals = sorted(set(by_ts[tst].values()))
        out.append((tst, "\x1f".join(v for v in vals if v) or "present"))
    return out


def _calibrated_current(rows, field, entries):
    """Calibrated [(ns, amps)] for the configured current field.

    Unit "A" rows pass through; unit-NULL rows scale only under an
    exactly scope-matched field_calibration entry
    (raw * unit_scale + unit_offset). Anything else counts as
    uncalibrated evidence (never silently integrated).
    """
    pairs = []
    raw = 0
    for r in rows:
        if r.get("source_field") != field:
            continue
        if r.get("value_num") is None \
                or not bc.is_valid_quality(r.get("quality")):
            continue
        if r.get("unit") == "A":
            pairs.append((r["event_time_ns"], r["value_num"]))
        elif r.get("unit") is None and entries:
            scope = (r["vehicle"], r["source"], r["decode_epoch"])
            match = [c for c in entries
                     if (c["vehicle"], c["source"],
                         c["decode_epoch"]) == scope]
            if not match:
                raw += 1
                continue
            cal = match[0]
            val = _finite(r["value_num"] * cal["unit_scale"]
                          + cal["unit_offset"])
            if val is None:
                raw += 1
                continue
            pairs.append((r["event_time_ns"], val))
        else:
            raw += 1
    return sorted(pairs), raw


def _calibrated_voltage_rows(rows, field, entries):
    """Calibrated voltage rows; same scope/scale rules as current."""
    usable = []
    raw = 0
    for r in rows:
        if r.get("source_field") != field:
            continue
        if r.get("value_num") is None \
                or not bc.is_valid_quality(r.get("quality")):
            continue
        if r.get("unit") == "V":
            usable.append(r)
        elif r.get("unit") is None and entries:
            scope = (r["vehicle"], r["source"], r["decode_epoch"])
            match = [c for c in entries
                     if (c["vehicle"], c["source"],
                         c["decode_epoch"]) == scope]
            if not match:
                raw += 1
                continue
            scaled = dict(r)
            val = _finite(r["value_num"] * match[0]["unit_scale"]
                          + match[0]["unit_offset"])
            if val is None:
                raw += 1
                continue
            scaled["value_num"] = val
            usable.append(scaled)
        else:
            raw += 1
    return usable, raw


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
                    and (r.get("ingest_time_ns") is None
                         or r["ingest_time_ns"] <= decision)]
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
    for scope in sorted(bc.group_by_scope(context_all).items()):
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
    # Window-scoped pairs for counters/SOC/energy: unmatched boundaries
    # refuse deltas even when retained context exists elsewhere.
    win_rows = [r for r in ordered if _in_window(r["event_time_ns"],
                                                 window)]
    meter_field = fields["session_energy_field"]
    meter_pairs = _num_pairs(win_rows, meter_field, "kWh")
    power_pairs = _num_pairs(win_rows, fields["power_field"], "kW")
    soc_pairs = _num_pairs(win_rows, fields["soc_field"], "%")
    remain_pairs = _num_pairs(win_rows, fields["energy_remaining_field"],
                              "kWh")
    dc_pairs = _num_pairs(win_rows, fields["dc_counter_field"], "kWh")
    ac_pairs = _num_pairs(win_rows, fields["ac_counter_field"], "kWh")
    dis_pairs = _num_pairs(win_rows, fields["discharge_counter_field"],
                           "kWh")
    # Session evidence spans retained context (pre-window history may
    # open the session); text joins read the full ordered scope. Power
    # pairs are window-scoped (hourly totals); current evidence spans
    # retained context so pre-window sessions stay visible.
    ctext = _text_series(ordered, fields["charging_state_field"])
    dtext = _text_series(ordered, fields["detailed_charge_field"])
    btext = _text_series(ordered, fields["bms_state_field"])
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
    # Sessions are sign-independent: any nonzero calibrated current is
    # evidence (retained context included); the sign only orients V*I/Ah
    # splits below. Meter/power/SOC anchors stay window-scoped so the
    # hourly totals never leak pre-window energy.
    raw_current = [(t, v) for t, v in cur_pairs_all]
    sessions, session_rows = _session_results(
        scope, window, ordered, meter_pairs, power_pairs,
        raw_current, ctext, dtext, btext, soc_pairs, meter_field,
        gap, ecfg)
    out.extend(session_rows)
    counter_rows = _counter_results(scope, window, dc_pairs, ac_pairs,
                                    dis_pairs, fields, ecfg)
    out.extend(counter_rows)
    counter_deltas = _counter_deltas_for_efc(counter_rows)
    cal_ver_vi = _cal_version(vol_entries, cur_entries)
    cal_ver_i = _cal_version(cur_entries)
    signed_current = [(tst, sign * val) for tst, val in cur_pairs] \
        if sign is not None else []
    vi_steps = []
    vi_note = None
    if sign_error is not None or sign is None or cal_error is not None \
            or not volt_rows or not cur_pairs:
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
        elif not volt_rows and volt_raw_win:
            vi_note = "missing_calibration:voltage_units"
        elif not cur_pairs and cur_raw_win:
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
            if irow is None:
                continue
            vi_steps.append((vrow["event_time_ns"],
                             vrow["value_num"],
                             sign * irow["value_num"]))
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
            or not cur_pairs:
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
    out.extend(_circular_results(scope, window, remain_pairs, soc_pairs,
                                 parsed, uncertainties, unc_error,
                                 fields, ecfg))
    out.extend(_trend_soh_results(scope, window, interval, reference,
                                  ref_error, parsed, ecfg))
    if sign_error is not None:
        out = [_promote_sign_error(r, sign_error) for r in out]
    if cal_error is not None:
        out = [_promote_cal_error(r, cal_error) for r in out]
    return out


def _promote_sign_error(row, sign_error):
    if row["metric"] in SIGN_METRICS and row["status"] == "unavailable" \
            and row.get("reason") != sign_error:
        fixed = dict(row)
        fixed["status"] = "error"
        fixed["reason"] = sign_error
        fixed["value"] = None
        return fixed
    return row


def _promote_cal_error(row, cal_error):
    if row["metric"] in CAL_METRICS and row["status"] == "unavailable" \
            and row.get("reason") != cal_error:
        fixed = dict(row)
        fixed["status"] = "error"
        fixed["reason"] = cal_error
        fixed["value"] = None
        return fixed
    return row


def _session_results(scope, window, ordered, meter_pairs, power_pairs,
                     raw_current, ctext, dtext, btext, soc_pairs,
                     meter_field, gap, ecfg):
    skew = ecfg.get("max_skew_ns", DEFAULT_MAX_SKEW_NS)
    grouped = {}

    def _bucket(pairs, key):
        for tst, val in pairs:
            grouped.setdefault(tst, {"t": tst}).setdefault(key, []).append(
                val)

    _bucket(power_pairs, "powers")
    _bucket(meter_pairs, "meters")
    _bucket(raw_current, "icharges")
    for pairs, key in ((ctext, "ctext"), (dtext, "dtext"),
                       (btext, "btext")):
        for tst, val in pairs:
            grouped.setdefault(tst, {"t": tst})[key] = val
    # Retained pre-window meter anchor bounds the first in-window point:
    # sessions may start before the hourly window. The latest pre-window
    # meter sample joins the timeline as a real meter point (never an
    # energy span by itself).
    pre_meter = [(t, v) for t, v in _session_meter_context(
        ordered, meter_field) if not _in_window(t, window)]
    if pre_meter:
        anchor_t, anchor_v = pre_meter[-1]
        grouped.setdefault(anchor_t, {"t": anchor_t}).setdefault(
            "meters", []).append(anchor_v)
    points = sorted(grouped.values(), key=lambda p: p["t"])
    sessions = segment_sessions(points, gap)
    rows = []
    enriched = []
    metric = "battery.energy.charge_session_energy_kwh"
    if not sessions:
        rows.append(_unavailable(scope, window, metric, "no_sessions",
                                 len(points), ecfg))
        return [], rows
    for sess in sessions:
        start, end = sess["start_ns"], sess["end_ns"]
        if sess["end_inclusive"]:
            span_pairs = [(tst, val) for tst, val in meter_pairs
                          if start <= tst <= end]
        else:
            span_pairs = [(tst, val) for tst, val in meter_pairs
                          if start <= tst < end]
        edge = sess["incomplete_start"] or sess["incomplete_end"]
        # Retained-context sessions keep incomplete_boundary rows but
        # never credit energy: only fully in-window closed sessions
        # value a delta.
        at_edge = edge or not _in_window(start, window) \
            or not _in_window(end, window)
        delta, info = meter_delta(span_pairs)
        first_ts = min((tst for tst, _ in span_pairs), default=None)
        last_ts = max((tst for tst, _ in span_pairs), default=None)
        if info == "reset":
            status = ("unavailable", "reset:%s" % meter_field)
            value = None
        elif info == "ambiguous":
            status = ("unavailable", "ambiguous:%s" % meter_field)
            value = None
        elif info == "sparse":
            if edge or at_edge:
                status = ("unavailable", "incomplete_boundary:"
                          "session_open_at_window_edge")
            else:
                status = ("unavailable", "unmatched_boundary:"
                          "session_meter_single_or_missing")
            value = None
        elif edge or at_edge:
            status = ("unavailable", "incomplete_boundary:"
                      "session_open_at_window_edge")
            value = None
        elif first_ts - start > skew or end - last_ts > skew:
            status = ("unavailable", "unmatched_boundary:"
                      "session_meter_unaligned")
            value = None
        else:
            status = ("derived", None)
            value = delta
        soc0 = _asof(soc_pairs, start, skew)
        soc1 = _asof(soc_pairs, end, skew)
        enriched.append({"session": sess, "meter_pairs": span_pairs,
                         "energy_kwh": value, "soc0_pct": soc0,
                         "soc1_pct": soc1})
        episode = bc.episode_key(scope[0], "energy", "charge_session",
                                 start, scope[2])
        text = "charge start_ns=%d end_ns=%d evidence=%s close=%s" % (
            start, end, "+".join(sess["evidence"]) or "none",
            sess["close_reason"])
        aid = _session_analysis_id(episode)
        if status[0] == "derived":
            rows.append(_derived(
                scope, window, metric, value, "kWh",
                "charge_session meter_delta_kwh evidence=%s close=%s"
                % ("+".join(sess["evidence"]) or "none",
                   sess["close_reason"]), len(span_pairs), ecfg,
                None, None, None, None, None, None, text, episode, aid))
        else:
            row = _unavailable(scope, window, metric, status[1],
                               len(span_pairs), ecfg)
            row["value_text"] = text
            row["episode_id"] = episode
            row["analysis_id"] = aid
            row["revision"] = bc.revision_id(
                metric, scope, status[1], span_pairs, sess, ecfg,
                ALGORITHM_VERSION)
            rows.append(row)
    # Sessions fully outside the window are context only: keep rows for
    kept_rows, kept_enriched = [], []
    for row, entry in zip(rows, enriched):
        start, end = entry["session"]["start_ns"], entry["session"]["end_ns"]
        before = window[0] is not None and end < window[0]
        after = window[1] is not None and start > window[1]
        if before or after:
            continue
        kept_rows.append(row)
        kept_enriched.append(entry)
    if not kept_rows:
        return [], [_unavailable(scope, window, metric,
                                 "no_sessions:no_session_overlaps_window",
                                 len(points), ecfg)]
    return kept_enriched, kept_rows


def _session_meter_context(ordered, meter_field):
    vals = []
    for r in ordered:
        if r.get("source_field") != meter_field:
            continue
        if r.get("value_num") is None \
                or not bc.is_valid_quality(r.get("quality")):
            continue
        if r.get("unit") != "kWh":
            continue
        vals.append((r["event_time_ns"], r["value_num"]))
    return sorted(vals)


def _counter_results(scope, window, dc_pairs, ac_pairs, dis_pairs, fields,
                     ecfg):
    out = []
    for metric, pairs, field, note in (
            ("battery.energy.dc_charging_energy_in_kwh", dc_pairs,
             fields["dc_counter_field"],
             "counter_delta battery_side_ac_and_dc_meter"),
            ("battery.energy.ac_charging_energy_in_kwh", ac_pairs,
             fields["ac_counter_field"],
             "counter_delta charger_side_ac_only_meter"),
            ("battery.energy.discharge_energy_kwh", dis_pairs,
             fields["discharge_counter_field"],
             "counter_delta discharging_meter")):
        delta, info = meter_delta(pairs)
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
                     uncertainties, unc_error, ecfg):
    metric = "battery.energy.interval_capacity_kwh"
    if unc_error is not None:
        return {"row": _error_row(scope, window, metric, unc_error, ecfg),
                "capacity_kwh": None, "energy_kwh": None,
                "soc_span01": None, "session": None}
    min_span = parsed["min_soc_span_pct"] / 100.0
    for entry in sessions:
        energy = entry.get("energy_kwh")
        soc0, soc1 = entry.get("soc0_pct"), entry.get("soc1_pct")
        if energy is None or soc0 is None or soc1 is None:
            continue
        span = (soc1 - soc0) / 100.0
        cap = interval_capacity_kwh(energy, span)
        if cap is None:
            continue
        if span < min_span:
            return {"row": _unavailable(
                        scope, window, metric,
                        "soc_span_too_small:min_%r_pct"
                        % parsed["min_soc_span_pct"], 2, ecfg),
                    "capacity_kwh": None, "energy_kwh": None,
                    "soc_span01": None, "session": None}
        sigma = None
        lo = hi = None
        soc_std = uncertainties.get("soc_uncertainty_pct")
        energy_std = uncertainties.get("energy_uncertainty_kwh")
        if soc_std is not None and energy_std is not None:
            sigma = capacity_uncertainty_kwh(
                cap, energy, energy_std, span, soc_std / 100.0)
            if sigma is not None:
                lo, hi = cap - sigma, cap + sigma
        reason = ("interval_capacity session start_ns=%d span_soc=%r "
                  "energy=%r" % (entry["session"]["start_ns"], span,
                                 energy))
        return {"row": _derived(
                    scope, window, metric, cap, "kWh", reason, 2, ecfg,
                    None, 2, None, sigma, lo, hi),
                "capacity_kwh": cap, "energy_kwh": energy,
                "soc_span01": span, "session": entry["session"]}
    reason = "sparse:no_valued_session_with_soc_span"
    if not sessions:
        reason = "sparse:no_sessions_for_capacity"
    else:
        spans = []
        for entry in sessions:
            soc0, soc1 = entry.get("soc0_pct"), entry.get("soc1_pct")
            if soc0 is None or soc1 is None:
                continue
            spans.append((soc1 - soc0) / 100.0)
        if any(s == 0.0 for s in spans):
            reason = "zero:soc_span_zero"
        elif spans and all(s < 0.0 for s in spans):
            reason = "inconsistent:soc_span_non_positive"
        elif spans and any(s < 0.0 for s in spans):
            reason = "inconsistent:soc_span_non_positive"
        elif spans and max(spans) < min_span:
            reason = ("soc_span_too_small:min_%r_pct"
                      % parsed["min_soc_span_pct"])
    return {"row": _unavailable(scope, window, metric, reason,
                                len(sessions), ecfg),
            "capacity_kwh": None, "energy_kwh": None,
            "soc_span01": None, "session": None}


def _circular_results(scope, window, remain_pairs, soc_pairs, parsed,
                      uncertainties, unc_error, fields, ecfg):
    metric = "battery.energy.bms_circular_capacity_kwh"
    if unc_error is not None:
        return [_error_row(scope, window, metric, unc_error, ecfg)]
    if len(remain_pairs) < 2 or len(soc_pairs) < 2:
        return [_unavailable(scope, window, metric,
                             "sparse:bms_needs_energy_and_soc_span",
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
    return [_derived(scope, window, trend_m, trend, "kWh",
                     "capacity_trend like_for_like_vs_reference", 1, ecfg,
                     reference["version"]),
            _derived(scope, window, soh_m, soh, "%",
                     "absolute_soh like_for_like_vs_reference", 1, ecfg,
                     reference["version"])]
