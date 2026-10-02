#!/usr/bin/env python3
"""Brick/thermal/isolation conditions and stress exposures. Stdlib only.

Config contract (config is a plain dict):
  Top level: ``window_start_ns`` / ``window_end_ns`` / ``decision_time_ns``
  (optional integer ns; malformed values yield per-metric error rows).
  Under ``"conditions"`` (dict, all optional unless noted):
    ``max_skew_ns``: non-negative int, default 30000000000 (30 s).
      Sync window for max/min values, extreme IDs, and SOC / battery-temp
      / current joins (latest secondary at t <= primary t, never filled).
    ``max_gap_ns``: non-negative int, default 600000000000 (10 min).
      Any slope/exposure leg wider than this is rejected, never
      interpolated. Invalid or ambiguous timestamps are barriers even
      when the leg is otherwise within the gap.
    ``min_slope_span_ns``: non-negative int, default 600000000000 (10 min).
      Thermal/isolation slopes need a same-ID run spanning at least this
      long; shorter runs report unavailable instead of extrapolating a
      few seconds of change to a per-hour rate.
    ``min_peers``: int >= 3, default 5. Minimum conditioned peers for
      the robust baseline (median needs >= 3 to mean anything).
    ``domain``: non-empty string or None (default None). Declared target
      domain for this analysis (for example a pack/chemistry identifier
      or "synthetic-circuit"). When set, every supplied calibration whose
      ``domain`` differs is rejected for every scope (domain mismatch,
      never silently applied).
    ``soc_window_pct``: finite > 0, default 5.0. Soc match window (%).
    ``temp_window_c``: finite > 0, default 5.0. Calibrated battery-temp
      (ModuleTempMax, C) match window.
    ``current_window_a``: finite > 0, default 10.0. Calibrated current
      (A) match window.
    Stress thresholds (no defaults, no universal Tesla cutoffs; missing
    means the exposure metric reports unavailable, never a guessed
    number):
      ``soc_high_pct`` / ``soc_low_pct``: finite in [0, 100] (uses the
        ``Soc`` field in ``%`` only, never ``BatteryLevel``).
      ``temp_high_c`` / ``temp_low_c``: finite calibrated-battery-temp
        thresholds in C (high compares ``ModuleTempMax``, low compares
        ``ModuleTempMin``). ``OutsideTemp`` (ambient) may arrive as raw
        context but never drives battery thermal metrics.
      ``current_high_a``: finite > 0 magnitude (exposure when
        abs(calibrated current in A) >= threshold).
    Load / thermal sources (official configured Fleet fields):
      Current conditioning and exposure use ``PackCurrent`` raw mapped to
      A by an explicit scope-bound ``pack_current_calibration`` (with an
      explicit ``current_sign``). Uncalibrated ``PackCurrent`` stays
      unavailable, never zero-filled. Non-Fleet alternatives (such as
      ``BatteryCurrent``) are used only via the explicit existing
      ``current_fields`` mapping, never a silent fallback: the default
      ``current_fields`` is ``["PackCurrent"]``, so a non-default source
      requires an explicit config entry. Battery temperature uses raw
      ``ModuleTempMax`` / ``ModuleTempMin`` mapped to celsius by
      ``module_temp_calibration`` (high exposure and conditioning join
      use Max, low exposure uses Min). Latest-value outputs are terminal:
      when the newest timestamp for a field is invalid/conflicting (a
      terminal tombstone), latest raw/ID rows report unavailable with an
      explicit reason and no older value is resurrected as current;
      historical trend/exposure legs use only bounded valid segments
      with coverage/asof stated.
    Physical calibrations (inline dicts, never global chemistry
    inference; missing means physical metrics report unavailable):
      ``brick_voltage_calibration`` (expected unit ``V``),
      ``module_temp_calibration`` (expected unit ``celsius``),
      ``isolation_calibration`` (expected unit ``ohm``),
      ``pack_current_calibration`` (expected unit ``A`` plus an explicit
      ``current_sign``). Each is ``{version, scale, offset=0.0, unit,
      domain, scope[, current_sign]}`` where version/domain are
      non-empty strings, scope is ``{vehicle, source, decode_epoch}``
      with exact strings or ``"*"`` wildcards, unit must equal the
      expected physical unit above (never an arbitrary string), scale is
      finite > 0 (positive monotonic; current direction comes only from
      ``current_sign``, exactly 1 or -1, never bool), and offset is
      finite. Point values use ``raw * scale + offset`` (current:
      ``current_sign * raw * scale + offset``); spreads and slopes are
      differences so they scale without offset. A well-formed calibration
      whose scope binding does not match the analysis scope, or whose
      domain differs from declared ``domain``, is rejected for that scope
      (scope/domain mismatch, dependent physical metrics unavailable,
      raw preserved). Malformed calibrations are per-metric errors.

Public numerical helpers (pure, stdlib):
  ``median_mad(values)`` -> (median|None, mad|None).
  ``apply_calibration(raw_value, calibration)`` -> float|None
    (``raw * scale + offset`` point mapping).
  ``calibrate_current(raw_value, calibration)`` -> float|None
    (``current_sign * raw * scale + offset``).
  ``parse_calibration(raw, name, expected_unit, require_sign=False)``
    -> dict|None (raises ConditionsError when malformed).
  ``conditioned_baseline(spreads, latest_index, soc_w, temp_w, curr_w,
  min_peers)`` robust median/MAD baseline over Soc / battery-temp /
  current peers.
  ``slope_per_h(pairs)`` -> float|None over sorted [(ns, value)].
  ``exposure_s(pairs, predicate, max_gap_ns, barriers=())`` ->
    dict with exposure/valid-covered seconds, legs/span-ns/coverage.
  ``id_stats(id_pairs, max_gap_ns, barriers=())`` -> dict with switches,
    persistence_s, recurrence, n, last_id.
  ``sync_spread_series(max_pairs, min_pairs, max_skew_ns)`` pure
    numeric synchronizer for tests.
  ``analyze(signals, events, config)`` -> list[dict] via make_result.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import battery_common as bc

ALGORITHM_VERSION = "1.2.0"
ANALYSIS_ID = "battery_conditions"

BRICK_MAX_FIELD = "BrickVoltageMax"
BRICK_MIN_FIELD = "BrickVoltageMin"
BRICK_MAX_ID_FIELD = "NumBrickVoltageMax"
BRICK_MIN_ID_FIELD = "NumBrickVoltageMin"
MOD_MAX_FIELD = "ModuleTempMax"
MOD_MIN_FIELD = "ModuleTempMin"
MOD_MAX_ID_FIELD = "NumModuleTempMax"
MOD_MIN_ID_FIELD = "NumModuleTempMin"
ISO_FIELD = "IsolationResistance"
SOC_FIELD = "Soc"
PACK_CURR_FIELD = "PackCurrent"
SOC_UNIT = "%"
CURR_UNIT = "A"

DEFAULT_MAX_SKEW_NS = 30000000000
DEFAULT_MAX_GAP_NS = 600000000000
DEFAULT_MIN_SLOPE_SPAN_NS = 600000000000
DEFAULT_MIN_PEERS = 5
DEFAULT_SOC_WINDOW = 5.0
DEFAULT_TEMP_WINDOW = 5.0
DEFAULT_CURR_WINDOW = 10.0

SUPPORTED_METRICS = (
    "battery.conditions.brick_max_raw",
    "battery.conditions.brick_min_raw",
    "battery.conditions.module_temp_max_raw",
    "battery.conditions.module_temp_min_raw",
    "battery.conditions.isolation_raw",
    "battery.conditions.brick_max_id",
    "battery.conditions.brick_min_id",
    "battery.conditions.module_temp_max_id",
    "battery.conditions.module_temp_min_id",
    "battery.conditions.brick_spread_raw",
    "battery.conditions.brick_spread_v",
    "battery.conditions.thermal_spread_raw",
    "battery.conditions.thermal_spread_c",
    "battery.conditions.spread_baseline_raw",
    "battery.conditions.spread_residual_raw",
    "battery.conditions.thermal_slope_raw_per_h",
    "battery.conditions.thermal_slope_c_per_h",
    "battery.conditions.isolation_ohm",
    "battery.conditions.isolation_trend_ohm_per_h",
    "battery.conditions.brick_max_id_switches",
    "battery.conditions.brick_min_id_switches",
    "battery.conditions.brick_max_id_persistence_s",
    "battery.conditions.brick_min_id_persistence_s",
    "battery.conditions.brick_max_id_recurrence",
    "battery.conditions.brick_min_id_recurrence",
    "battery.conditions.thermal_max_id_switches",
    "battery.conditions.thermal_min_id_switches",
    "battery.conditions.exposure_high_soc_s",
    "battery.conditions.exposure_low_soc_s",
    "battery.conditions.exposure_high_temp_s",
    "battery.conditions.exposure_low_temp_s",
    "battery.conditions.exposure_high_current_s",
    "battery.conditions.diagnostics",
)

CAL_SPECS = {
    "brick_voltage_calibration": ("V", False,
                                  ("battery.conditions.brick_spread_v",)),
    "module_temp_calibration": ("celsius", False,
                                ("battery.conditions.thermal_spread_c",
                                 "battery.conditions.thermal_slope_c_per_h")),
    "isolation_calibration": ("ohm", False,
                              ("battery.conditions.isolation_ohm",
                               "battery.conditions.isolation_trend_ohm_per_h")),
    "pack_current_calibration": ("A", True,
                                 ("battery.conditions."
                                  "exposure_high_current_s",)),
}


class ConditionsError(ValueError):
    pass


def _finite(value):
    return bc.safe_float(value)


def median_mad(values):
    """Robust median and MAD over finite numbers; (None, None) when empty
    or any member is non-finite. Even-n median averages the two middles."""
    if not isinstance(values, (list, tuple)) or not values:
        return (None, None)
    clean = []
    for val in values:
        conv = _finite(val)
        if conv is None:
            return (None, None)
        clean.append(conv)
    if not clean:
        return (None, None)
    ordered = sorted(clean)
    count = len(ordered)
    mid = count // 2
    if count % 2:
        median = ordered[mid]
    else:
        median = (ordered[mid - 1] + ordered[mid]) / 2.0
    if not bc.is_finite_number(median):
        return (None, None)
    deviations = sorted(abs(val - median) for val in ordered)
    mad_mid = len(deviations) // 2
    if len(deviations) % 2:
        mad = deviations[mad_mid]
    else:
        mad = (deviations[mad_mid - 1] + deviations[mad_mid]) / 2.0
    if not bc.is_finite_number(mad):
        return (None, None)
    return (median, mad)


def parse_calibration(raw, name, expected_unit, require_sign=False):
    """Validate an inline calibration dict; None when absent.

    Requires non-empty version/domain strings, a scope dict
    ``{vehicle, source, decode_epoch}`` with exact strings or ``"*"``
    wildcards, unit exactly ``expected_unit``, finite scale > 0
    (positive monotonic), finite offset (default 0.0), and — when
    ``require_sign`` — ``current_sign`` exactly 1 or -1 (never bool).
    Raises ConditionsError when present but malformed (never guessed).
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ConditionsError("malformed: %s calibration dict" % name)
    version = raw.get("version")
    domain = raw.get("domain")
    for label, val in (("version", version), ("domain", domain)):
        if not isinstance(val, str) or not val:
            raise ConditionsError(
                "malformed: %s calibration %s non-empty string" % (name, label))
    scope = raw.get("scope")
    if not isinstance(scope, dict):
        raise ConditionsError(
            "malformed: %s calibration scope dict "
            "{vehicle, source, decode_epoch}" % name)
    bound = {}
    for comp in ("vehicle", "source", "decode_epoch"):
        val = scope.get(comp)
        if not isinstance(val, str) or not val:
            raise ConditionsError(
                "malformed: %s calibration scope.%s exact string or "
                "\"*\"" % (name, comp))
        bound[comp] = val
    unit = raw.get("unit")
    if not isinstance(unit, str) or unit != expected_unit:
        raise ConditionsError(
            "malformed: %s calibration unit must be %r" % (name,
                                                           expected_unit))
    scale = _finite(raw.get("scale"))
    if scale is None or scale <= 0.0:
        raise ConditionsError(
            "malformed: %s calibration scale finite > 0" % name)
    offset = raw.get("offset", 0.0)
    offset = _finite(offset)
    if offset is None:
        raise ConditionsError(
            "malformed: %s calibration offset finite" % name)
    out = {"version": version, "domain": domain, "scope": bound,
           "unit": unit, "scale": scale, "offset": offset}
    if require_sign:
        sign = raw.get("current_sign")
        if isinstance(sign, bool) or sign not in (1, -1):
            raise ConditionsError(
                "malformed: %s calibration current_sign exactly 1 or -1"
                % name)
        out["current_sign"] = sign
    return out


def apply_calibration(raw_value, calibration):
    """Point mapping ``raw * scale + offset``; None when non-finite."""
    if calibration is None:
        return None
    conv = _finite(raw_value)
    if conv is None:
        return None
    try:
        out = conv * calibration["scale"] + calibration["offset"]
    except (OverflowError, KeyError, TypeError):
        return None
    return out if bc.is_finite_number(out) else None


def calibrate_current(raw_value, calibration):
    """Current mapping ``current_sign * raw * scale + offset``; None when
    non-finite or the calibration carries no explicit sign."""
    if calibration is None:
        return None
    conv = _finite(raw_value)
    if conv is None:
        return None
    sign = calibration.get("current_sign")
    if isinstance(sign, bool) or sign not in (1, -1):
        return None
    try:
        out = sign * conv * calibration["scale"] + calibration["offset"]
    except (OverflowError, KeyError, TypeError):
        return None
    return out if bc.is_finite_number(out) else None


def _scope_text(bound):
    return "%s/%s/%s" % (bound["vehicle"], bound["source"],
                         bound["decode_epoch"])


def _check_binding(cal, key, scope, declared_domain):
    """Scope/domain authorization for one calibration on one scope.

    Returns (True, None) when the scope binding matches (exact or "*")
    and the calibration domain agrees with the declared domain (when
    one is declared); otherwise (False, reason) and the caller must
    reject the calibration for this scope, never silently apply it.
    """
    bind = cal["scope"]
    for comp, actual in (("vehicle", scope[0]), ("source", scope[1]),
                         ("decode_epoch", scope[2])):
        want = bind[comp]
        if want != "*" and (actual is None or want != actual):
            return (False, "calibration_scope_mismatch:%s bound %s" %
                    (key, _scope_text(bind)))
    if declared_domain is not None and cal["domain"] != declared_domain:
        return (False, "calibration_domain_mismatch:%s declared %r" %
                (key, declared_domain))
    return (True, None)


def _resolve_cals(cals, cal_errors, scope, declared_domain):
    """Split parsed calibrations into scope-authorized and rejected.

    Returns (active, blocks): active maps key -> cal dict or None;
    blocks maps key -> mismatch reason for well-formed calibrations that
    do not authorize this scope. Malformed keys stay in cal_errors
    (per-metric error path) and map to None here.
    """
    active, blocks = {}, {}
    for key in CAL_SPECS:
        cal = cals.get(key)
        if key in cal_errors or cal is None:
            active[key] = None
            continue
        ok, why = _check_binding(cal, key, scope, declared_domain)
        if ok:
            active[key] = cal
        else:
            active[key] = None
            blocks[key] = why
    return active, blocks


def slope_per_h(pairs):
    """End-to-end slope per hour over sorted [(ns, value)]; None when
    fewer than 2 points, non-positive span, or non-finite math."""
    if not isinstance(pairs, (list, tuple)) or len(pairs) < 2:
        return None
    try:
        first_t, first_v = pairs[0]
        last_t, last_v = pairs[-1]
    except (TypeError, ValueError):
        return None
    if bc.to_ns(first_t) is None or bc.to_ns(last_t) is None:
        return None
    first = _finite(first_v)
    last = _finite(last_v)
    if first is None or last is None:
        return None
    span_h = (last_t - first_t) / 3.6e12
    if not bc.is_finite_number(span_h) or span_h <= 0.0:
        return None
    slope = (last - first) / span_h
    return slope if bc.is_finite_number(slope) else None


def sync_spread_series(max_pairs, min_pairs, max_skew_ns):
    """Pure numeric max-primary synchronizer for tests.

    Both inputs are [(ns, value)] (any order); returns [(t, spread)]
    with one entry per max sample whose latest min at t <= max t is
    within max_skew_ns. Invalid/ambiguous handling lives in analyze();
    this helper Syncs clean numbers only.
    """
    if isinstance(max_skew_ns, bool) or not isinstance(max_skew_ns, int) \
            or max_skew_ns < 0:
        raise ConditionsError("malformed: max_skew_ns non-negative int")
    clean_max, clean_min = [], []
    for item in max_pairs or []:
        try:
            tstamp, val = item
        except (TypeError, ValueError):
            raise ConditionsError("pairs must hold (ns, value)")
        if bc.to_ns(tstamp) is None or _finite(val) is None:
            raise ConditionsError("pairs hold bad timestamp/value")
        clean_max.append((tstamp, float(val)))
    for item in min_pairs or []:
        try:
            tstamp, val = item
        except (TypeError, ValueError):
            raise ConditionsError("pairs must hold (ns, value)")
        if bc.to_ns(tstamp) is None or _finite(val) is None:
            raise ConditionsError("pairs hold bad timestamp/value")
        clean_min.append((tstamp, float(val)))
    clean_max.sort(key=lambda p: p[0])
    clean_min.sort(key=lambda p: p[0])
    mins = [t for t, _ in clean_min]
    from bisect import bisect_right
    out = []
    for tmax, vmax in clean_max:
        idx = bisect_right(mins, tmax) - 1
        if idx < 0:
            continue
        tmin, vmin = clean_min[idx]
        if tmax - tmin > max_skew_ns:
            continue
        spread = vmax - vmin
        if bc.is_finite_number(spread):
            out.append((tmax, spread))
    return out


def exposure_s(pairs, predicate, max_gap_ns, barriers=()):
    """Bounded stress-time coverage over sorted [(ns, value)].

    A leg counts toward valid time only when dt is in [0, max_gap_ns]
    with no barrier timestamp strictly inside; it counts toward
    exposure only when BOTH endpoints satisfy predicate. Never fills
    across gaps, invalid, or ambiguous samples. Returns dict with
    exposure_s (seconds), valid_covered_s (seconds), legs_used,
    legs_rejected, span_ns (ns), coverage_ratio (covered ns over span
    ns, None when no usable leg).
    """
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise ConditionsError("malformed: max_gap_ns non-negative int")
    clean = []
    for item in pairs or []:
        try:
            tstamp, val = item
        except (TypeError, ValueError):
            raise ConditionsError("pairs must hold (ns, value)")
        if bc.to_ns(tstamp) is None or _finite(val) is None:
            raise ConditionsError("pairs hold bad timestamp/value")
        clean.append((tstamp, float(val)))
    clean.sort(key=lambda p: p[0])
    blockers = sorted(b for b in (barriers or []) if bc.to_ns(b) is not None)
    exposure, covered, used, rejected = 0.0, 0, 0, 0
    span = None
    if len(clean) >= 2:
        span = clean[-1][0] - clean[0][0]
        if bc.to_ns(span) is None and span != 0:
            span = None
        if not isinstance(span, int) or span < 0:
            span = None
    for (t0, v0), (t1, v1) in zip(clean, clean[1:]):
        dt = t1 - t0
        if dt < 0 or dt > max_gap_ns:
            rejected += 1
            continue
        blocked = False
        for barrier in blockers:
            if t0 < barrier < t1:
                blocked = True
                break
            if barrier >= t1:
                break
        if blocked:
            rejected += 1
            continue
        try:
            ok0 = bool(predicate(v0))
            ok1 = bool(predicate(v1))
        except Exception:
            raise ConditionsError("predicate must return bool")
        covered += dt
        used += 1
        if ok0 and ok1:
            exposure += dt
    # ponytail: ns ints throughout; seconds only at this return
    # boundary (ns / 1e9). Keeps gap/coverage arithmetic exact.
    if not used:
        return {"exposure_s": None, "valid_covered_s": 0.0, "legs_used": 0,
                "legs_rejected": rejected, "span_ns": span,
                "coverage_ratio": None}
    coverage = None
    if isinstance(span, int) and span > 0:
        coverage = covered / span
        if not bc.is_finite_number(coverage) or not 0.0 <= coverage <= 1.0:
            coverage = None
    return {"exposure_s": exposure / 1e9, "valid_covered_s": covered / 1e9,
            "legs_used": used, "legs_rejected": rejected,
            "span_ns": span, "coverage_ratio": coverage}


def id_stats(id_pairs, max_gap_ns, barriers=()):
    """Switch/persistence/recurrence over sorted [(ns, id_value)].

    Switches count consecutive value changes; recurrence counts returns
    to a previously seen ID after leaving it; persistence_s is the
    gap-bounded valid time over the trailing same-ID run (0.0 on a
    single sample). Any barrier timestamp strictly inside a trailing
    leg stops persistence there. Returns None when empty.
    """
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise ConditionsError("malformed: max_gap_ns non-negative int")
    clean = []
    for item in id_pairs or []:
        try:
            tstamp, val = item
        except (TypeError, ValueError):
            raise ConditionsError("pairs must hold (ns, id)")
        if bc.to_ns(tstamp) is None or _finite(val) is None:
            raise ConditionsError("pairs hold bad timestamp/id")
        clean.append((tstamp, float(val)))
    clean.sort(key=lambda p: p[0])
    if not clean:
        return None
    switches = 0
    recurrence = 0
    seen = {clean[0][1]}
    for idx in range(1, len(clean)):
        prev = clean[idx - 1][1]
        cur = clean[idx][1]
        if cur != prev:
            switches += 1
            if cur in seen:
                recurrence += 1
        seen.add(cur)
    run_start = len(clean) - 1
    while run_start > 0 and clean[run_start][1] == clean[run_start - 1][1]:
        run_start -= 1
    blockers = sorted(b for b in (barriers or []) if bc.to_ns(b) is not None)
    persistence = 0
    for idx in range(len(clean) - 1, run_start, -1):
        t0, _ = clean[idx - 1]
        t1, _ = clean[idx]
        dt = t1 - t0
        if dt < 0 or dt > max_gap_ns:
            break
        blocked = False
        for barrier in blockers:
            if t0 < barrier < t1:
                blocked = True
                break
            if barrier >= t1:
                break
        if blocked:
            break
        persistence += dt
    return {"switches": switches, "persistence_s": persistence / 1e9,
            "recurrence": recurrence, "n": len(clean),
            "last_id": clean[-1][1]}


def conditioned_baseline(spreads, latest_index, soc_w, temp_w, curr_w,
                         min_peers):
    """Robust baseline for one spread from Soc / battery-temp / current
    peers.

    spreads: list of (spread, soc|None, batt_temp_c|None, curr_a|None)
    with finite spreads. latest_index selects the target whose
    soc/temp/curr must all be present; peers are all other entries
    (excluding the target itself) with all three conditions present and
    each within its window. Returns dict with baseline/mad/peers/
    residual or a reason: condition_unavailable, insufficient_peers,
    zero_mad, sparse.
    """
    for label, val in (("soc_w", soc_w), ("temp_w", temp_w),
                       ("curr_w", curr_w)):
        if _finite(val) is None or val <= 0.0:
            raise ConditionsError("malformed: %s finite > 0" % label)
    if isinstance(min_peers, bool) or not isinstance(min_peers, int) \
            or min_peers < 3:
        raise ConditionsError("malformed: min_peers int >= 3")
    if not spreads:
        return {"baseline": None, "mad": None, "peers": 0,
                "residual": None, "reason": "sparse:no_spreads"}
    if not 0 <= latest_index < len(spreads):
        raise ConditionsError("malformed: latest_index out of range")
    target = spreads[latest_index]
    try:
        target_spread, t_soc, t_temp, t_curr = target
    except (TypeError, ValueError):
        raise ConditionsError("spreads must hold (spread, soc, temp, curr)")
    if _finite(target_spread) is None:
        return {"baseline": None, "mad": None, "peers": 0,
                "residual": None, "reason": "sparse:no_latest_spread"}
    if _finite(t_soc) is None or _finite(t_temp) is None \
            or _finite(t_curr) is None:
        return {"baseline": None, "mad": None, "peers": 0,
                "residual": None, "reason": "condition_unavailable"}
    peers = []
    for idx, entry in enumerate(spreads):
        if idx == latest_index:
            continue
        try:
            spread, soc, temp, curr = entry
        except (TypeError, ValueError):
            raise ConditionsError(
                "spreads must hold (spread, soc, temp, curr)")
        if _finite(spread) is None or _finite(soc) is None \
                or _finite(temp) is None or _finite(curr) is None:
            continue
        if abs(soc - t_soc) <= soc_w and abs(temp - t_temp) <= temp_w \
                and abs(curr - t_curr) <= curr_w:
            peers.append(float(spread))
    if len(peers) < min_peers:
        return {"baseline": None, "mad": None, "peers": len(peers),
                "residual": None, "reason": "insufficient_peers"}
    median, mad = median_mad(peers)
    if median is None or mad is None:
        return {"baseline": None, "mad": None, "peers": len(peers),
                "residual": None, "reason": "insufficient_peers"}
    if mad == 0.0:
        return {"baseline": None, "mad": mad, "peers": len(peers),
                "residual": None, "reason": "zero_mad"}
    residual = float(target_spread) - median
    if not bc.is_finite_number(residual):
        return {"baseline": None, "mad": mad, "peers": len(peers),
                "residual": None, "reason": "insufficient_peers"}
    return {"baseline": median, "mad": mad, "peers": len(peers),
            "residual": residual, "reason": None}


def _parse_params(ccfg):
    skew = ccfg.get("max_skew_ns", DEFAULT_MAX_SKEW_NS)
    if isinstance(skew, bool) or not isinstance(skew, int) or skew < 0:
        raise ConditionsError("malformed: max_skew_ns non-negative int")
    gap = ccfg.get("max_gap_ns", DEFAULT_MAX_GAP_NS)
    if isinstance(gap, bool) or not isinstance(gap, int) or gap < 0:
        raise ConditionsError("malformed: max_gap_ns non-negative int")
    min_span = ccfg.get("min_slope_span_ns", DEFAULT_MIN_SLOPE_SPAN_NS)
    if isinstance(min_span, bool) or not isinstance(min_span, int) \
            or min_span < 0:
        raise ConditionsError("malformed: min_slope_span_ns non-negative int")
    peers = ccfg.get("min_peers", DEFAULT_MIN_PEERS)
    if isinstance(peers, bool) or not isinstance(peers, int) or peers < 3:
        raise ConditionsError("malformed: min_peers int >= 3")
    dom = ccfg.get("domain")
    if dom is not None and (not isinstance(dom, str) or not dom):
        raise ConditionsError("malformed: domain non-empty string")
    soc_w = _finite(ccfg.get("soc_window_pct", DEFAULT_SOC_WINDOW))
    if soc_w is None or soc_w <= 0.0:
        raise ConditionsError("malformed: soc_window_pct finite > 0")
    temp_w = _finite(ccfg.get("temp_window_c", DEFAULT_TEMP_WINDOW))
    if temp_w is None or temp_w <= 0.0:
        raise ConditionsError("malformed: temp_window_c finite > 0")
    curr_w = _finite(ccfg.get("current_window_a", DEFAULT_CURR_WINDOW))
    if curr_w is None or curr_w <= 0.0:
        raise ConditionsError("malformed: current_window_a finite > 0")
    fields = _parse_current_fields(ccfg)
    return {"max_skew_ns": skew, "max_gap_ns": gap,
            "min_slope_span_ns": min_span, "min_peers": peers,
            "domain": dom, "soc_window_pct": soc_w, "temp_window_c": temp_w,
            "current_window_a": curr_w, "current_fields": fields}


def _parse_thresholds(ccfg):
    values = {"soc_high": None, "soc_low": None, "temp_high": None,
              "temp_low": None, "curr_high": None}
    errors = {}

    def _soc(key, metric):
        raw = ccfg.get(key)
        if raw is None:
            return
        conv = _finite(raw)
        if conv is None or not 0.0 <= conv <= 100.0:
            errors[metric] = "malformed: %s in [0, 100]" % key
            return
        values["soc_high" if key == "soc_high_pct" else "soc_low"] = conv

    _soc("soc_high_pct", "battery.conditions.exposure_high_soc_s")
    _soc("soc_low_pct", "battery.conditions.exposure_low_soc_s")
    for key, slot, metric in (
            ("temp_high_c", "temp_high",
             "battery.conditions.exposure_high_temp_s"),
            ("temp_low_c", "temp_low",
             "battery.conditions.exposure_low_temp_s")):
        raw = ccfg.get(key)
        if raw is None:
            continue
        conv = _finite(raw)
        if conv is None:
            errors[metric] = "malformed: %s finite" % key
            continue
        values[slot] = conv
    raw = ccfg.get("current_high_a")
    if raw is not None:
        conv = _finite(raw)
        if conv is None or conv <= 0.0:
            errors["battery.conditions.exposure_high_current_s"] = \
                "malformed: current_high_a finite > 0"
        else:
            values["curr_high"] = conv

    def _ordered(high_slot, low_slot, high_metric, low_metric,
                 high_key, low_key):
        if values[high_slot] is not None and values[low_slot] is not None \
                and values[high_slot] <= values[low_slot] \
                and high_metric not in errors and low_metric not in errors:
            errors[high_metric] = \
                "malformed: %s must exceed %s" % (high_key, low_key)
            errors[low_metric] = \
                "malformed: %s must exceed %s" % (high_key, low_key)

    _ordered("soc_high", "soc_low",
             "battery.conditions.exposure_high_soc_s",
             "battery.conditions.exposure_low_soc_s",
             "soc_high_pct", "soc_low_pct")
    _ordered("temp_high", "temp_low",
             "battery.conditions.exposure_high_temp_s",
             "battery.conditions.exposure_low_temp_s",
             "temp_high_c", "temp_low_c")
    return values, errors


def _parse_calibrations(ccfg):
    cals, errors = {}, {}
    for key, (unit, need_sign, metrics) in CAL_SPECS.items():
        raw = ccfg.get(key)
        if raw is None:
            cals[key] = None
            continue
        try:
            cals[key] = parse_calibration(raw, key, unit, need_sign)
        except ConditionsError as exc:
            cals[key] = None
            for metric in metrics:
                errors[metric] = str(exc)
    return cals, errors


def _payload(row):
    return (repr(row.get("value_num")), row.get("value_text"),
            row.get("value_bool"))


def _field_rows(ordered, field):
    return [r for r in ordered if r.get("source_field") == field]


def _barrier_times(ordered, fields, id_fields=()):
    """Timestamps that stop interpolation: any invalid/unmeasurable row
    or any timestamp with conflicting payloads for the field. ID fields
    additionally reject non-integers and non-positive NumBrick IDs;
    NumModule index bounds are not documented."""
    barriers = set()
    for field in fields:
        groups = {}
        for row in ordered:
            if row.get("source_field") == field:
                groups.setdefault(row["event_time_ns"], []).append(row)
        for tstamp, members in groups.items():
            if len({_payload(r) for r in members}) != 1:
                barriers.add(tstamp)
                continue
            rep = members[0]
            if rep.get("value_num") is None \
                    or not bc.is_valid_quality(rep.get("quality")) \
                    or _finite(rep.get("value_num")) is None:
                barriers.add(tstamp)
                continue
            if field in id_fields \
                    and _valid_id(rep.get("value_num"), field) is None:
                barriers.add(tstamp)
    return barriers


def _latest_state(ordered, field):
    """Terminal state of one field: ("ok", rep) | ("tombstone", None) |
    ("conflict", None) | ("empty", None).

    The newest timestamp decides: a terminal invalid/unmeasurable group
    is a tombstone and a terminal conflicting group is a conflict, even
    when older valid samples exist (no resurrection of stale values as
    current). Identity use is stricter than raw preservation: raw rows
    keep reported IDs, but ID matching/slopes/persistence use
    ``_valid_id`` (positive integers; NumBrick IDs are official
    1-indexed) and treat fractional/zero/negative IDs as barriers.
    """
    groups = {}
    for row in ordered:
        if row.get("source_field") == field:
            groups.setdefault(row["event_time_ns"], []).append(row)
    if not groups:
        return ("empty", None)
    latest = max(groups)
    members = groups[latest]
    if len({_payload(r) for r in members}) != 1:
        return ("conflict", None)
    rep = members[0]
    if rep.get("value_num") is None \
            or not bc.is_valid_quality(rep.get("quality")) \
            or _finite(rep.get("value_num")) is None:
        return ("tombstone", None)
    return ("ok", rep)


def _valid_id(value, field):
    """Integer ID, with the documented 1-based bound only for NumBrick."""
    conv = _finite(value)
    if conv is None or not float(conv).is_integer() \
            or (field.startswith("NumBrick") and conv < 1.0):
        return None
    return conv


def _id_points(ordered, field):
    """Identity-validated [(ns, id)] sorted; rejected times remain barriers."""
    groups = {}
    for row in ordered:
        if row.get("source_field") == field:
            groups.setdefault(row["event_time_ns"], []).append(row)
    points = []
    for tstamp in sorted(groups):
        members = groups[tstamp]
        if len({_payload(r) for r in members}) != 1:
            continue
        rep = members[0]
        if rep.get("value_num") is None \
                or not bc.is_valid_quality(rep.get("quality")):
            continue
        conv = _valid_id(rep.get("value_num"), field)
        if conv is None:
            continue
        points.append((tstamp, conv))
    return points


def _condition_points(ordered, field, unit):
    """Valid [(ns, value)] with calibrated unit; wrong-unit rows are
    barriers (excluded and reported via barriers)."""
    groups = {}
    for row in ordered:
        if row.get("source_field") == field:
            groups.setdefault(row["event_time_ns"], []).append(row)
    points, barriers = [], set()
    for tstamp in sorted(groups):
        members = groups[tstamp]
        if len({_payload(r) for r in members}) != 1:
            barriers.add(tstamp)
            continue
        rep = members[0]
        if rep.get("value_num") is None \
                or not bc.is_valid_quality(rep.get("quality")) \
                or rep.get("unit") != unit:
            barriers.add(tstamp)
            continue
        conv = _finite(rep.get("value_num"))
        if conv is None:
            barriers.add(tstamp)
            continue
        points.append((tstamp, conv))
    return points, barriers


def _calibrated_points(ordered, field, apply_fn):
    """Valid [(ns, calibrated)] for a raw unit-NULL field; ambiguous,
    invalid, wrong-unit, or non-finite-calibrated timestamps are
    barriers (never interpolated across)."""
    groups = {}
    for row in ordered:
        if row.get("source_field") == field:
            groups.setdefault(row["event_time_ns"], []).append(row)
    points, barriers = [], set()
    for tstamp in sorted(groups):
        members = groups[tstamp]
        if len({_payload(r) for r in members}) != 1:
            barriers.add(tstamp)
            continue
        rep = members[0]
        if rep.get("value_num") is None \
                or not bc.is_valid_quality(rep.get("quality")) \
                or rep.get("unit") is not None:
            barriers.add(tstamp)
            continue
        conv = _finite(rep.get("value_num"))
        if conv is None:
            barriers.add(tstamp)
            continue
        try:
            val = apply_fn(conv)
        except Exception:
            raise ConditionsError("calibration apply must return a number")
        if _finite(val) is None:
            barriers.add(tstamp)
            continue
        points.append((tstamp, float(val)))
    return points, barriers


def _asof_points(points, tstamp, skew_ns):
    """Latest calibrated point value at t <= tstamp within skew_ns;
    None when missing or stale (never filled forward)."""
    best = None
    for tprev, vprev in points:
        if tprev > tstamp:
            break
        if tstamp - tprev <= skew_ns:
            best = vprev
        # ponytail: points are short change-gated series; linear scan
        # keeps the join boring and order-independent via sorted input.
    return best


def _parse_current_fields(ccfg):
    raw = ccfg.get("current_fields", ["PackCurrent"])
    if raw is None:
        raw = ["PackCurrent"]
    if not isinstance(raw, (list, tuple)) or not raw:
        raise ConditionsError("malformed: current_fields non-empty list")
    out = []
    for item in raw:
        if not isinstance(item, str) or not item:
            raise ConditionsError("malformed: current_fields strings")
        out.append(item)
    return out


def _current_series(ordered, pack_cal, current_fields):
    """Calibrated current series [(ns, A)] with barriers.

    Only explicitly mapped sources are used, never a silent fallback:
    ``PackCurrent`` (the default) requires the scope-bound
    ``pack_current_calibration``; any other listed field (for example a
    recorder-calibrated ``BatteryCurrent`` in A) is used only when
    explicitly present in ``current_fields``. Returns (points, barriers,
    source): source names the first listed field yielding a usable
    point, or "none". ``pack_configured`` says whether any
    ``PackCurrent`` sample exists, so callers report missing pack
    calibration instead of silently substituting.
    """
    pack_configured = bool(_field_rows(ordered, PACK_CURR_FIELD))
    for field in current_fields:
        if field == PACK_CURR_FIELD:
            if pack_cal is None:
                continue
            pts, bar = _calibrated_points(
                ordered, PACK_CURR_FIELD,
                lambda raw: calibrate_current(raw, pack_cal))
            if pts:
                return pts, bar, "pack", pack_configured
            continue
        pts, bar = _condition_points(ordered, field, CURR_UNIT)
        if pts:
            return pts, bar, field, pack_configured
    return [], set(), "none", pack_configured


def _sync_pair(ordered, max_field, min_field, max_id_field, min_id_field,
               skew_ns, temp_points, curr_points):
    """Synchronize max-primary spreads with min values, IDs, and Soc /
    battery-temp / current conditions. Returns (points, barriers,
    terminal, unsync, ambiguous) where each point holds t/spread/max/min
    plus IDs (identity-validated positive integers; malformed IDs are
    barriers, never attribution), soc (Soc %), temp (calibrated battery
    C), curr (calibrated A), each None when no unambiguous in-skew
    match. ``terminal`` is None normally, else "terminal_invalid" when
    the newest max or min timestamp is a tombstone or
    "terminal_conflict" when either disagrees: the sync then emits no
    spread as current and callers report unavailable instead of an
    older matched point.
    """
    max_all = _field_rows(ordered, max_field)
    min_all = _field_rows(ordered, min_field)
    maxid_all = _field_rows(ordered, max_id_field)
    minid_all = _field_rows(ordered, min_id_field)
    soc_all = _field_rows(ordered, SOC_FIELD)
    barriers = _barrier_times(
        ordered, (max_field, min_field, max_id_field, min_id_field),
        id_fields=(max_id_field, min_id_field))
    terminal = None
    max_state, _ = _latest_state(ordered, max_field)
    min_state, _ = _latest_state(ordered, min_field)
    if max_state == "tombstone" or min_state == "tombstone":
        terminal = "terminal_invalid"
    elif max_state == "conflict" or min_state == "conflict":
        terminal = "terminal_conflict"
    groups = {}
    for row in max_all:
        groups.setdefault(row["event_time_ns"], []).append(row)
    cands = []
    ambiguous = 0
    for tstamp in sorted(groups):
        members = groups[tstamp]
        if len({_payload(r) for r in members}) != 1:
            ambiguous += 1
            continue
        rep = members[0]
        if rep.get("value_num") is None \
                or not bc.is_valid_quality(rep.get("quality")) \
                or _finite(rep.get("value_num")) is None:
            continue  # invalid barrier, already in barriers
        cands.append((tstamp, rep))
    reps = [rep for _, rep in cands]
    if reps:
        min_hit = [m for _, m in bc.join_asof(reps, min_all, skew_ns)]
        maxid_hit = [m for _, m in bc.join_asof(reps, maxid_all, skew_ns)]
        minid_hit = [m for _, m in bc.join_asof(reps, minid_all, skew_ns)]
        soc_hit = [m for _, m in bc.join_asof(reps, soc_all, skew_ns)]
    else:
        min_hit = maxid_hit = minid_hit = soc_hit = []

    def _row_value(match, unit=None):
        if match is None or match.get("value_num") is None \
                or not bc.is_valid_quality(match.get("quality")):
            return None
        if unit is not None and match.get("unit") != unit:
            return None
        return _finite(match.get("value_num"))

    points, unsync = [], 0
    for (tstamp, rep), got, maxid_raw, minid_raw, soc_raw in \
            zip(cands, min_hit, maxid_hit, minid_hit, soc_hit):
        if got is None or got.get("value_num") is None \
                or not bc.is_valid_quality(got.get("quality")) \
                or _finite(got.get("value_num")) is None:
            unsync += 1
            continue
        spread = float(rep["value_num"]) - float(got["value_num"])
        if not bc.is_finite_number(spread):
            unsync += 1
            continue
        max_id = _valid_id(_row_value(maxid_raw), max_id_field)
        min_id = _valid_id(_row_value(minid_raw), min_id_field)
        if max_id is None or min_id is None:
            barriers = set(barriers) | {tstamp}
        points.append({
            "t": tstamp,
            "spread": spread,
            "max_v": float(rep["value_num"]),
            "min_v": float(got["value_num"]),
            "max_id": max_id,
            "min_id": min_id,
            "soc": _row_value(soc_raw, SOC_UNIT),
            "temp": _asof_points(temp_points, tstamp, skew_ns),
            "curr": _asof_points(curr_points, tstamp, skew_ns),
        })
    return points, barriers, terminal, unsync, ambiguous


def _id_text(value):
    if float(value).is_integer():
        return "id %d" % int(value)
    return "id %r" % (value,)


def _scope_sort(scope):
    return tuple(v or "" for v in scope)


def _error_rows(reason, window, revision_note):
    return [bc.make_result(
        metric=m, value=None, unit=None, status="error", reason=reason,
        window_start_ns=window[0], window_end_ns=window[1],
        analysis_id=ANALYSIS_ID,
        revision=bc.revision_id(m, "error", revision_note,
                                ALGORITHM_VERSION)) for m in SUPPORTED_METRICS]


def _unavailable_rows(scopes, window, ccfg, reason="no_signals"):
    rows = []
    targets = sorted(scopes, key=_scope_sort) if scopes else [(None, None, None)]
    revision = bc.revision_id("battery_conditions", targets, window, ccfg,
                              ALGORITHM_VERSION)
    for scope in targets:
        vehicle, source, epoch = scope
        for metric in SUPPORTED_METRICS:
            unit = None
            if metric == "battery.conditions.brick_spread_v":
                unit = "V"
            elif metric == "battery.conditions.thermal_spread_c":
                unit = "celsius"
            elif metric == "battery.conditions.thermal_slope_c_per_h":
                unit = "celsius/h"
            elif metric == "battery.conditions.isolation_ohm":
                unit = "ohm"
            elif metric == "battery.conditions.isolation_trend_ohm_per_h":
                unit = "ohm/h"
            elif metric.endswith(("_switches", "_recurrence")):
                unit = "count"
            elif metric.endswith(("_persistence_s", "_s")) \
                    and metric != "battery.conditions.diagnostics":
                unit = "s"
            text = None
            if metric == "battery.conditions.diagnostics":
                text = "valid=0 total=0 invalid=0 ambiguous_ts=0 " \
                    "deduped=0 after_decision=0 gaps_rejected=0"
            rows.append(bc.make_result(
                metric=metric, value=None, unit=unit, status="unavailable",
                reason=reason, window_start_ns=window[0],
                window_end_ns=window[1], vehicle=vehicle, source=source,
                decode_epoch=epoch, analysis_id=ANALYSIS_ID,
                value_text=text, revision=revision))
        if not scopes:
            break
    return rows


def analyze(signals, events, config):
    """analyze(signals, events, config) -> list[dict] (stable contract)."""
    cfg_all = config if isinstance(config, dict) else {}
    ccfg = cfg_all.get("conditions")
    if not isinstance(ccfg, dict):
        ccfg = {}
    raw_ws = cfg_all.get("window_start_ns")
    raw_we = cfg_all.get("window_end_ns")
    raw_dt = cfg_all.get("decision_time_ns")
    for label, raw in (("window_start_ns", raw_ws),
                       ("window_end_ns", raw_we),
                       ("decision_time_ns", raw_dt)):
        if raw is not None and bc.to_ns(raw) is None:
            return _error_rows("malformed:%s" % label, (None, None), raw)
    window = (bc.to_ns(raw_ws), bc.to_ns(raw_we))
    if window[0] is not None and window[1] is not None \
            and window[1] < window[0]:
        return _error_rows("malformed:window_order", window, window)
    decision = bc.to_ns(raw_dt)
    try:
        params = _parse_params(ccfg)
    except ConditionsError as exc:
        return _error_rows(str(exc), window, str(exc))
    thresholds, thresh_errors = _parse_thresholds(ccfg)
    cals, cal_errors = _parse_calibrations(ccfg)

    rows_all = []
    after_decision = 0
    for sig in bc.normalize_signals(signals if isinstance(signals, list) else []):
        if decision is not None:
            if sig["event_time_ns"] > decision:
                after_decision += 1
                continue
            ingest = sig.get("ingest_time_ns")
            if ingest is not None and ingest > decision:
                after_decision += 1
                continue
        if window[0] is not None and sig["event_time_ns"] < window[0]:
            continue
        if window[1] is not None and sig["event_time_ns"] > window[1]:
            continue
        rows_all.append(sig)
    if not rows_all:
        # No surviving signal: scope-less set, runtime scopes it when the
        # vehicle is known. Never invent a vehicle/source/epoch here.
        return _unavailable_rows([], window, ccfg)

    out = []
    grouped = bc.group_by_scope(rows_all)
    for scope in sorted(grouped, key=_scope_sort):
        ordered = bc.sort_dedup(grouped[scope])
        out.extend(_analyze_scope(scope, ordered, window, params,
                                  thresholds, thresh_errors, cals,
                                  cal_errors, ccfg, len(grouped[scope]),
                                  after_decision))
    return out


def _row(metric, value, unit, status, reason, scope, window, evidence=None,
         sample=None, coverage=None, calibration_version=None,
         uncertainty=None, value_text=None, revision=None):
    return bc.make_result(
        metric=metric, value=value, unit=unit, status=status, reason=reason,
        window_start_ns=window[0], window_end_ns=window[1],
        vehicle=scope[0], source=scope[1], decode_epoch=scope[2],
        evidence_count=evidence, sample_count=sample,
        coverage_ratio=coverage, algorithm_version=ALGORITHM_VERSION,
        calibration_version=calibration_version, uncertainty=uncertainty,
        value_text=value_text, analysis_id=ANALYSIS_ID, revision=revision)


def _analyze_scope(scope, ordered, window, params, thresholds, thresh_errors,
                   cals, cal_errors, ccfg, pre_dedup_count, after_decision):
    skew = params["max_skew_ns"]
    gap = params["max_gap_ns"]
    min_span = params["min_slope_span_ns"]
    rows = []
    rev = lambda *parts: bc.revision_id(
        scope, params, ccfg, ALGORITHM_VERSION, *parts)

    active, blocks = _resolve_cals(cals, cal_errors, scope,
                                   params.get("domain"))
    brick_cal = active["brick_voltage_calibration"]
    mod_cal = active["module_temp_calibration"]
    iso_cal = active["isolation_calibration"]
    pack_cal = active["pack_current_calibration"]

    total = pre_dedup_count
    deduped = total - len(ordered)
    valid = sum(1 for r in ordered
                if r.get("value_num") is not None
                and bc.is_valid_quality(r.get("quality")))
    invalid = len(ordered) - valid
    ambiguous = 0
    for field in tuple([BRICK_MAX_FIELD, BRICK_MIN_FIELD, MOD_MAX_FIELD,
                  MOD_MIN_FIELD, ISO_FIELD, BRICK_MAX_ID_FIELD,
                  BRICK_MIN_ID_FIELD, MOD_MAX_ID_FIELD, MOD_MIN_ID_FIELD,
                  SOC_FIELD, PACK_CURR_FIELD]
                 + [f for f in params["current_fields"]
                    if f != PACK_CURR_FIELD]):
        groups = {}
        for row in ordered:
            if row.get("source_field") == field:
                groups.setdefault(row["event_time_ns"], []).append(row)
        for members in groups.values():
            if len({_payload(r) for r in members}) != 1:
                ambiguous += 1
    gaps_rejected = [0]

    def _terminal_row(field, metric, id_row=False):
        state, rep = _latest_state(ordered, field)
        nrows = len(_field_rows(ordered, field))
        if state == "ok":
            val = float(rep["value_num"])
            rows.append(_row(
                metric, val, None,
                "reported",
                "reported_latest_id unit_unverified_preserved"
                if id_row else
                "reported_latest_raw unit_unverified_preserved",
                scope, window, evidence=1, sample=nrows,
                value_text=_id_text(val) if id_row else None,
                revision=rev(metric, rep["event_time_ns"],
                             rep["value_num"])))
            return
        if state == "empty":
            reason = "sparse:no_unambiguous_valid_sample"
        elif state == "tombstone":
            reason = "terminal_invalid:latest_sample_unmeasurable"
        else:
            reason = "terminal_conflict:latest_samples_disagree"
        rows.append(_row(
            metric, None, None, "unavailable", reason,
            scope, window, evidence=0, sample=nrows,
            revision=rev(metric, reason)))

    # --- Raw extrema and IDs (reported, unit NULL, raw preserved) ---
    for field, metric in (
            (BRICK_MAX_FIELD, "battery.conditions.brick_max_raw"),
            (BRICK_MIN_FIELD, "battery.conditions.brick_min_raw"),
            (MOD_MAX_FIELD, "battery.conditions.module_temp_max_raw"),
            (MOD_MIN_FIELD, "battery.conditions.module_temp_min_raw"),
            (ISO_FIELD, "battery.conditions.isolation_raw")):
        _terminal_row(field, metric)
    for field, metric in (
            (BRICK_MAX_ID_FIELD, "battery.conditions.brick_max_id"),
            (BRICK_MIN_ID_FIELD, "battery.conditions.brick_min_id"),
            (MOD_MAX_ID_FIELD, "battery.conditions.module_temp_max_id"),
            (MOD_MIN_ID_FIELD, "battery.conditions.module_temp_min_id")):
        _terminal_row(field, metric, id_row=True)

    # --- Calibrated battery-temp / current series for this scope ---
    if mod_cal is not None:
        temp_max_pts, temp_max_bar = _calibrated_points(
            ordered, MOD_MAX_FIELD,
            lambda raw: apply_calibration(raw, mod_cal))
        temp_min_pts, temp_min_bar = _calibrated_points(
            ordered, MOD_MIN_FIELD,
            lambda raw: apply_calibration(raw, mod_cal))
    else:
        temp_max_pts, temp_max_bar = [], set()
        temp_min_pts, temp_min_bar = [], set()
    curr_pts, curr_bar, curr_kind, pack_configured = _current_series(
        ordered, pack_cal, params["current_fields"])

    # --- Synchronized spreads (conditioned on battery temp + current) ---
    brick_pts, brick_bar, brick_term, brick_unsync, brick_amb = _sync_pair(
        ordered, BRICK_MAX_FIELD, BRICK_MIN_FIELD, BRICK_MAX_ID_FIELD,
        BRICK_MIN_ID_FIELD, skew, temp_max_pts, curr_pts)
    therm_pts, therm_bar, therm_term, therm_unsync, therm_amb = _sync_pair(
        ordered, MOD_MAX_FIELD, MOD_MIN_FIELD, MOD_MAX_ID_FIELD,
        MOD_MIN_ID_FIELD, skew, temp_max_pts, curr_pts)

    def _phys_cal(cal_key, cal_metric):
        """Resolve one physical calibration for this scope.

        Returns (cal|None, status|None, reason|None): a scope/domain
        mismatch yields ("unavailable", reason) so the mismatch is
        rejected loudly but raw metrics survive; a malformed entry
        yields ("error", reason).
        """
        if blocks.get(cal_key) is not None:
            return None, "unavailable", blocks[cal_key]
        if cal_metric in cal_errors:
            return None, "error", cal_errors[cal_metric]
        return {"brick_voltage_calibration": brick_cal,
                "module_temp_calibration": mod_cal,
                "isolation_calibration": iso_cal,
                "pack_current_calibration": pack_cal}[cal_key], None, None

    def _spread_rows(points, terminal, unsync, amb, raw_metric, cal_key,
                     cal_metric, cal_unit_hint):
        if terminal is not None:
            reason = ("terminal_invalid:latest_extreme_unmeasurable"
                      if terminal == "terminal_invalid" else
                      "terminal_conflict:latest_extreme_samples_disagree")
            rows.append(_row(raw_metric, None, None, "unavailable", reason,
                             scope, window, evidence=len(points),
                             sample=len(points),
                             revision=rev(raw_metric, reason)))
        elif points:
            latest = points[-1]
            rows.append(_row(
                raw_metric, latest["spread"], None, "derived",
                "synchronized_max_min skew_ns=%d n=%d" % (skew, len(points)),
                scope, window, evidence=len(points), sample=len(points),
                revision=rev(raw_metric, [(p["t"], p["spread"])
                                          for p in points])))
        else:
            if amb:
                reason = "ambiguous_timestamp:no_unambiguous_max_sample"
            elif unsync:
                reason = "unsynchronized:max_min_skew_exceeded"
            else:
                reason = "sparse:needs_max_and_min"
            rows.append(_row(raw_metric, None, None, "unavailable", reason,
                             scope, window, evidence=0, sample=0,
                             revision=rev(raw_metric, reason)))
        scal, err_status, err_reason = _phys_cal(cal_key, cal_metric)
        if err_status is not None:
            rows.append(_row(cal_metric, None,
                             None if err_status == "error" else cal_unit_hint,
                             err_status, err_reason, scope, window,
                             revision=rev(cal_metric, "cal_error")))
        elif scal is None:
            rows.append(_row(
                cal_metric, None, cal_unit_hint, "unavailable",
                "missing_calibration:%s" % cal_key, scope, window,
                evidence=len(points), revision=rev(cal_metric, "no_cal")))
        elif terminal is not None:
            reason = ("terminal_invalid:latest_extreme_unmeasurable"
                      if terminal == "terminal_invalid" else
                      "terminal_conflict:latest_extreme_samples_disagree")
            rows.append(_row(cal_metric, None, cal_unit_hint, "unavailable",
                             reason, scope, window, evidence=len(points),
                             revision=rev(cal_metric, reason)))
        elif not points:
            rows.append(_row(cal_metric, None, cal_unit_hint, "unavailable",
                             "sparse:no_synchronized_spread", scope, window,
                             evidence=0, revision=rev(cal_metric, "sparse")))
        else:
            spread = points[-1]["spread"]
            val = _finite(spread * scal["scale"])
            if val is None:
                rows.append(_row(cal_metric, None, scal["unit"], "unavailable",
                                 "non_finite:calibrated_spread", scope,
                                 window, evidence=len(points),
                                 revision=rev(cal_metric, "non_finite")))
            else:
                rows.append(_row(
                    cal_metric, val, scal["unit"], "derived",
                    "calibrated domain=%s scope=%s" % (
                        scal["domain"], _scope_text(scal["scope"])),
                    scope, window, evidence=len(points),
                    sample=len(points), calibration_version=scal["version"],
                    revision=rev(cal_metric, points[-1], scal["version"])))

    _spread_rows(brick_pts, brick_term, brick_unsync, brick_amb,
                 "battery.conditions.brick_spread_raw",
                 "brick_voltage_calibration",
                 "battery.conditions.brick_spread_v", "V")
    _spread_rows(therm_pts, therm_term, therm_unsync, therm_amb,
                 "battery.conditions.thermal_spread_raw",
                 "module_temp_calibration",
                 "battery.conditions.thermal_spread_c", "celsius")

    # --- Conditioned robust baseline over brick spreads ---
    if brick_term is None and brick_pts:
        tuples = [(p["spread"], p["soc"], p["temp"], p["curr"])
                  for p in brick_pts]
        res = conditioned_baseline(
            tuples, len(tuples) - 1, params["soc_window_pct"],
            params["temp_window_c"], params["current_window_a"],
            params["min_peers"])
        if res["reason"] is None:
            condition_version = ";".join(sorted({
                cal["version"] for cal in
                (mod_cal, pack_cal if curr_kind == "pack" else None)
                if cal is not None
            })) or None
            rows.append(_row(
                "battery.conditions.spread_baseline_raw", res["baseline"],
                None, "estimated",
                "conditioned_median peers=%d soc_w=%r temp_w=%r curr_w=%r"
                % (res["peers"], params["soc_window_pct"],
                   params["temp_window_c"], params["current_window_a"]),
                scope, window, evidence=res["peers"],
                sample=len(tuples), uncertainty=res["mad"],
                calibration_version=condition_version,
                revision=rev("baseline", tuples, params["min_peers"])))
            rows.append(_row(
                "battery.conditions.spread_residual_raw", res["residual"],
                None, "derived",
                "latest_minus_baseline peers=%d" % res["peers"], scope,
                window, evidence=res["peers"] + 1, sample=len(tuples),
                calibration_version=condition_version,
                revision=rev("residual", tuples, res["baseline"])))
        else:
            rows.append(_row(
                "battery.conditions.spread_baseline_raw", None, None,
                "unavailable", res["reason"] + ":peers=%d/%d"
                % (res["peers"], params["min_peers"]), scope, window,
                evidence=res["peers"], sample=len(tuples),
                revision=rev("baseline", tuples, res["reason"])))
            rows.append(_row(
                "battery.conditions.spread_residual_raw", None, None,
                "unavailable", res["reason"], scope, window,
                evidence=res["peers"], sample=len(tuples),
                revision=rev("residual", tuples, res["reason"])))
    elif brick_term is not None:
        reason = ("terminal_invalid:latest_extreme_unmeasurable"
                  if brick_term == "terminal_invalid" else
                  "terminal_conflict:latest_extreme_samples_disagree")
        rows.append(_row("battery.conditions.spread_baseline_raw", None,
                         None, "unavailable", reason, scope, window,
                         evidence=len(brick_pts), sample=len(brick_pts),
                         revision=rev("baseline", reason)))
        rows.append(_row("battery.conditions.spread_residual_raw", None,
                         None, "unavailable", reason, scope, window,
                         evidence=len(brick_pts), sample=len(brick_pts),
                         revision=rev("residual", reason)))
    else:
        rows.append(_row("battery.conditions.spread_baseline_raw", None,
                         None, "unavailable", "sparse:no_spreads", scope,
                         window, evidence=0, revision=rev("baseline")))
        rows.append(_row("battery.conditions.spread_residual_raw", None,
                         None, "unavailable", "sparse:no_spreads", scope,
                         window, evidence=0, revision=rev("residual")))

    def _thermal_slope(raw_metric, cal_metric):
        # Raw slope never needs calibration; only the calibrated leg
        # consults the scoped calibration, so a malformed/rejected
        # calibration errors just the calibrated metric while raw still
        # computes. A terminal extreme tombstone/conflict blocks both
        # legs: no older spread is presented as the current slope.
        if therm_term is not None or not therm_pts:
            if therm_term == "terminal_invalid":
                reason = "terminal_invalid:latest_extreme_unmeasurable"
            elif therm_term == "terminal_conflict":
                reason = "terminal_conflict:latest_extreme_samples_disagree"
            else:
                reason = "sparse:no_synchronized_module_temperatures"
            rows.append(_row(raw_metric, None, None, "unavailable", reason,
                             scope, window, evidence=len(therm_pts),
                             revision=rev(raw_metric, reason)))
            scal, err_status, err_reason = _phys_cal(
                "module_temp_calibration", cal_metric)
            if err_status is not None:
                rows.append(_row(cal_metric, None,
                                 None if err_status == "error"
                                 else "celsius/h", err_status, err_reason,
                                 scope, window,
                                 revision=rev(cal_metric, "cal_error")))
            elif scal is None:
                rows.append(_row(cal_metric, None, "celsius/h",
                                 "unavailable",
                                 "missing_calibration:"
                                 "module_temp_calibration", scope, window,
                                 evidence=len(therm_pts),
                                 revision=rev(cal_metric, "no_cal")))
            else:
                rows.append(_row(cal_metric, None, "celsius/h",
                                 "unavailable", reason, scope, window,
                                 evidence=len(therm_pts),
                                 revision=rev(cal_metric, reason)))
            return
        latest = therm_pts[-1]
        if latest["max_id"] is None or latest["min_id"] is None:
            rows.append(_row(raw_metric, None, None, "unavailable",
                             "id_changed:missing_extreme_id_at_latest",
                             scope, window, evidence=len(therm_pts),
                             revision=rev(raw_metric, "missing_id")))
            gaps_rejected[0] += 1
        else:
            run = [latest]
            stop = None
            for idx in range(len(therm_pts) - 1, 0, -1):
                cur = therm_pts[idx]
                before = therm_pts[idx - 1]
                if before["max_id"] is None or before["min_id"] is None \
                        or before["max_id"] != latest["max_id"] \
                        or before["min_id"] != latest["min_id"]:
                    stop = "id_changed"
                    break
                dt = cur["t"] - before["t"]
                if dt < 0 or dt > gap:
                    gaps_rejected[0] += 1
                    stop = "gap_exceeded"
                    break
                if any(before["t"] < b < cur["t"] for b in therm_bar):
                    gaps_rejected[0] += 1
                    stop = "gap_exceeded:barrier"
                    break
                run.append(before)
            run = sorted(run, key=lambda p: p["t"])
            if len(run) < 2 or run[-1]["t"] - run[0]["t"] < min_span:
                if len(run) >= 2:
                    reason = "sparse:same_id_run_shorter_than_min_span"
                elif stop == "id_changed":
                    reason = "id_changed:same_module_run_too_short"
                else:
                    reason = "gap_exceeded:trailing_run_too_short"
                rows.append(_row(raw_metric, None, None, "unavailable",
                                 reason, scope, window,
                                 evidence=len(therm_pts),
                                 revision=rev(raw_metric, reason)))
            else:
                slope = slope_per_h([(p["t"], p["spread"]) for p in run])
                if slope is None:
                    rows.append(_row(raw_metric, None, None, "unavailable",
                                     "non_finite:slope", scope, window,
                                     evidence=len(run),
                                     revision=rev(raw_metric, "non_finite")))
                else:
                    rows.append(_row(
                        raw_metric, slope, None, "derived",
                        "same_extreme_ids max=%s min=%s n=%d" % (
                            _id_text(latest["max_id"]),
                            _id_text(latest["min_id"]), len(run)),
                        scope, window, evidence=len(run),
                        sample=len(therm_pts),
                        revision=rev(raw_metric, [(p["t"], p["spread"])
                                                  for p in run])))
                    scal, err_status, err_reason = _phys_cal(
                        "module_temp_calibration", cal_metric)
                    if err_status is not None:
                        rows.append(_row(
                            cal_metric, None,
                            None if err_status == "error" else "celsius/h",
                            err_status, err_reason, scope, window,
                            revision=rev(cal_metric, "cal_error")))
                        return
                    if scal is None:
                        rows.append(_row(
                            cal_metric, None, "celsius/h", "unavailable",
                            "missing_calibration:module_temp_calibration",
                            scope, window, evidence=len(run),
                            revision=rev(cal_metric, "no_cal")))
                    else:
                        c_slope = slope * scal["scale"]
                        if not bc.is_finite_number(c_slope):
                            rows.append(_row(
                                cal_metric, None, scal["unit"] + "/h",
                                "unavailable", "non_finite:slope", scope,
                                window, evidence=len(run),
                                revision=rev(cal_metric, "non_finite")))
                        else:
                            rows.append(_row(
                                cal_metric, c_slope, scal["unit"] + "/h",
                                "derived",
                                "calibrated domain=%s scope=%s "
                                "same_extreme_ids" % (
                                    scal["domain"],
                                    _scope_text(scal["scope"])),
                                scope, window, evidence=len(run),
                                sample=len(therm_pts),
                                calibration_version=scal["version"],
                                revision=rev(cal_metric, slope,
                                             scal["version"])))
                    return
        scal, err_status, err_reason = _phys_cal(
            "module_temp_calibration", cal_metric)
        if err_status is not None:
            rows.append(_row(cal_metric, None,
                             None if err_status == "error" else "celsius/h",
                             err_status, err_reason, scope, window,
                             revision=rev(cal_metric, "cal_error")))
        elif scal is None:
            rows.append(_row(cal_metric, None, "celsius/h", "unavailable",
                             "missing_calibration:module_temp_calibration",
                             scope, window, evidence=len(therm_pts),
                             revision=rev(cal_metric, "no_cal")))
        else:
            rows.append(_row(cal_metric, None, "celsius/h", "unavailable",
                             "sparse:no_same_id_slope", scope, window,
                             evidence=len(therm_pts),
                             revision=rev(cal_metric, "sparse")))

    _thermal_slope("battery.conditions.thermal_slope_raw_per_h",
                   "battery.conditions.thermal_slope_c_per_h")

    # --- Isolation value and trend (calibrated only) ---
    iso_state, iso_rep = _latest_state(ordered, ISO_FIELD)
    # Raw isolation accepts any unit: no unit gating, invalid stops runs.
    iso_groups = {}
    for row in ordered:
        if row.get("source_field") == ISO_FIELD:
            iso_groups.setdefault(row["event_time_ns"], []).append(row)
    iso_pts = []
    iso_bar = set()
    for tstamp in sorted(iso_groups):
        members = iso_groups[tstamp]
        if len({_payload(r) for r in members}) != 1:
            iso_bar.add(tstamp)
            continue
        rep = members[0]
        if rep.get("value_num") is None \
                or not bc.is_valid_quality(rep.get("quality")) \
                or _finite(rep.get("value_num")) is None:
            iso_bar.add(tstamp)
            continue
        iso_pts.append((tstamp, float(rep["value_num"])))
    scal, err_status, err_reason = _phys_cal(
        "isolation_calibration", "battery.conditions.isolation_ohm")
    if err_status is not None:
        rows.append(_row("battery.conditions.isolation_ohm", None,
                         None if err_status == "error" else "ohm",
                         err_status, err_reason, scope, window,
                         revision=rev("iso", "cal_error")))
    elif scal is None:
        rows.append(_row("battery.conditions.isolation_ohm", None, "ohm",
                         "unavailable",
                         "missing_calibration:isolation_calibration",
                         scope, window, evidence=len(iso_pts),
                         revision=rev("iso", "no_cal")))
    elif iso_state != "ok":
        if iso_state == "empty":
            reason = "sparse:no_unambiguous_valid_sample"
        elif iso_state == "tombstone":
            reason = "terminal_invalid:latest_sample_unmeasurable"
        else:
            reason = "terminal_conflict:latest_samples_disagree"
        rows.append(_row("battery.conditions.isolation_ohm", None, "ohm",
                         "unavailable", reason, scope, window,
                         evidence=len(iso_pts),
                         revision=rev("iso", reason)))
    else:
        val = apply_calibration(float(iso_rep["value_num"]), scal)
        if val is None:
            rows.append(_row("battery.conditions.isolation_ohm", None,
                             scal["unit"], "unavailable",
                             "non_finite:calibrated_isolation", scope,
                             window, evidence=len(iso_pts),
                             revision=rev("iso", "non_finite")))
        else:
            rows.append(_row(
                "battery.conditions.isolation_ohm", val, scal["unit"],
                "derived", "calibrated domain=%s scope=%s" % (
                    scal["domain"], _scope_text(scal["scope"])),
                scope, window, evidence=len(iso_pts), sample=len(iso_pts),
                calibration_version=scal["version"],
                revision=rev("iso", iso_rep["value_num"], scal["version"])))
    tscal, terr_status, terr_reason = _phys_cal(
        "isolation_calibration",
        "battery.conditions.isolation_trend_ohm_per_h")
    if terr_status is not None:
        rows.append(_row("battery.conditions.isolation_trend_ohm_per_h",
                         None, None if terr_status == "error" else "ohm/h",
                         terr_status, terr_reason, scope, window,
                         revision=rev("iso_trend", "cal_error")))
    elif tscal is None:
        rows.append(_row("battery.conditions.isolation_trend_ohm_per_h",
                         None, "ohm/h", "unavailable",
                         "missing_calibration:isolation_calibration",
                         scope, window, evidence=len(iso_pts),
                         revision=rev("iso_trend", "no_cal")))
    elif len(iso_pts) < 2:
        # Trend uses only bounded valid legs with coverage stated; it
        # never presents a stale value as current (the ohm row above is
        # terminal), but a trailing valid run still describes history.
        rows.append(_row("battery.conditions.isolation_trend_ohm_per_h",
                         None, "ohm/h", "unavailable",
                         "sparse:needs_two_isolation_samples", scope, window,
                         evidence=len(iso_pts),
                         revision=rev("iso_trend", "sparse")))
    else:
        run = [iso_pts[-1]]
        for idx in range(len(iso_pts) - 1, 0, -1):
            t0, _ = iso_pts[idx - 1]
            t1, _ = iso_pts[idx]
            dt = t1 - t0
            if dt < 0 or dt > gap:
                gaps_rejected[0] += 1
                break
            if any(t0 < b < t1 for b in iso_bar):
                gaps_rejected[0] += 1
                break
            run.append(iso_pts[idx - 1])
        run = sorted(run, key=lambda p: p[0])
        if len(run) < 2:
            gaps_rejected[0] += 1
            rows.append(_row("battery.conditions.isolation_trend_ohm_per_h",
                             None, tscal["unit"] + "/h", "unavailable",
                             "gap_exceeded:trailing_run_too_short", scope,
                             window, evidence=len(iso_pts),
                             revision=rev("iso_trend", "gap")))
        elif run[-1][0] - run[0][0] < min_span:
            rows.append(_row("battery.conditions.isolation_trend_ohm_per_h",
                             None, tscal["unit"] + "/h", "unavailable",
                             "sparse:run_shorter_than_min_span", scope,
                             window, evidence=len(iso_pts),
                             revision=rev("iso_trend", "min_span")))
        else:
            raw_slope = slope_per_h(run)
            if raw_slope is None:
                rows.append(_row(
                    "battery.conditions.isolation_trend_ohm_per_h", None,
                    tscal["unit"] + "/h", "unavailable", "non_finite:slope",
                    scope, window, evidence=len(run),
                    revision=rev("iso_trend", "non_finite")))
            else:
                trend = raw_slope * tscal["scale"]
                if not bc.is_finite_number(trend):
                    rows.append(_row(
                        "battery.conditions.isolation_trend_ohm_per_h", None,
                        tscal["unit"] + "/h", "unavailable",
                        "non_finite:slope", scope, window, evidence=len(run),
                        revision=rev("iso_trend", "non_finite")))
                else:
                    rows.append(_row(
                        "battery.conditions.isolation_trend_ohm_per_h", trend,
                        tscal["unit"] + "/h", "derived",
                        "calibrated domain=%s scope=%s n=%d as_of_ns=%d" % (
                            tscal["domain"], _scope_text(tscal["scope"]),
                            len(run), run[-1][0]),
                        scope, window, evidence=len(run),
                        sample=len(iso_pts),
                        coverage=((run[-1][0] - run[0][0])
                                  / (window[1] - window[0])
                                  if window[0] is not None
                                  and window[1] is not None
                                  and window[1] > window[0] else None),
                        calibration_version=tscal["version"],
                        revision=rev("iso_trend", run, tscal["version"])))

    # --- Extreme-ID dynamics ---
    id_series = {
        "battery.conditions.brick_max_id_switches": _id_points(
            ordered, BRICK_MAX_ID_FIELD),
        "battery.conditions.brick_min_id_switches": _id_points(
            ordered, BRICK_MIN_ID_FIELD),
        "battery.conditions.thermal_max_id_switches": _id_points(
            ordered, MOD_MAX_ID_FIELD),
        "battery.conditions.thermal_min_id_switches": _id_points(
            ordered, MOD_MIN_ID_FIELD),
    }
    id_barriers = {
        BRICK_MAX_ID_FIELD: _barrier_times(
            ordered, (BRICK_MAX_ID_FIELD,),
            id_fields=(BRICK_MAX_ID_FIELD,)),
        BRICK_MIN_ID_FIELD: _barrier_times(
            ordered, (BRICK_MIN_ID_FIELD,),
            id_fields=(BRICK_MIN_ID_FIELD,)),
        MOD_MAX_ID_FIELD: _barrier_times(
            ordered, (MOD_MAX_ID_FIELD,), id_fields=(MOD_MAX_ID_FIELD,)),
        MOD_MIN_ID_FIELD: _barrier_times(
            ordered, (MOD_MIN_ID_FIELD,), id_fields=(MOD_MIN_ID_FIELD,)),
    }
    id_field = {
        "battery.conditions.brick_max_id_switches": BRICK_MAX_ID_FIELD,
        "battery.conditions.brick_min_id_switches": BRICK_MIN_ID_FIELD,
        "battery.conditions.thermal_max_id_switches": MOD_MAX_ID_FIELD,
        "battery.conditions.thermal_min_id_switches": MOD_MIN_ID_FIELD,
    }
    brick_id_stats = {}
    for metric, pairs in id_series.items():
        if not pairs:
            rows.append(_row(metric, None, "count", "unavailable",
                             "sparse:no_id_samples", scope, window,
                             evidence=0, revision=rev(metric, "sparse")))
            continue
        try:
            stats = id_stats(pairs, gap, id_barriers[id_field[metric]])
        except ConditionsError as exc:
            rows.append(_row(metric, None, "count", "error", str(exc),
                             scope, window, revision=rev(metric, "error")))
            continue
        rows.append(_row(metric, float(stats["switches"]), "count",
                         "derived",
                         "extreme_id_switches n=%d last=%s" % (
                             stats["n"], _id_text(stats["last_id"])),
                         scope, window, evidence=stats["n"],
                         sample=len(pairs),
                         revision=rev(metric, pairs)))
        if metric in ("battery.conditions.brick_max_id_switches",
                      "battery.conditions.brick_min_id_switches"):
            brick_id_stats[metric] = (pairs, stats)
    for switches_metric, persist_metric, recur_metric in (
            ("battery.conditions.brick_max_id_switches",
             "battery.conditions.brick_max_id_persistence_s",
             "battery.conditions.brick_max_id_recurrence"),
            ("battery.conditions.brick_min_id_switches",
             "battery.conditions.brick_min_id_persistence_s",
             "battery.conditions.brick_min_id_recurrence")):
        entry = brick_id_stats.get(switches_metric)
        if entry is None:
            rows.append(_row(persist_metric, None, "s", "unavailable",
                             "sparse:no_id_samples", scope, window,
                             evidence=0,
                             revision=rev(persist_metric, "sparse")))
            rows.append(_row(recur_metric, None, "count", "unavailable",
                             "sparse:no_id_samples", scope, window,
                             evidence=0,
                             revision=rev(recur_metric, "sparse")))
            continue
        pairs, stats = entry
        rows.append(_row(
            persist_metric, stats["persistence_s"], "s", "derived",
            "valid_covered_s_since_last_switch n=%d" % stats["n"], scope,
            window, evidence=stats["n"], sample=len(pairs),
            coverage=None, revision=rev(persist_metric, pairs)))
        rows.append(_row(
            recur_metric, float(stats["recurrence"]), "count", "derived",
            "reentries_of_previously_seen_ids n=%d" % stats["n"], scope,
            window, evidence=stats["n"], sample=len(pairs),
            revision=rev(recur_metric, pairs)))

    # --- Stress exposures (bounded valid time, never fractions) ---
    soc_pts, soc_bar = _condition_points(ordered, SOC_FIELD, SOC_UNIT)
    # Calibration-gated exposure sources: a scope/domain mismatch or a
    # malformed entry rejects the metric (never silently applied);
    # a missing calibration is reported, never guessed.
    exp_gate = {}
    mod_gate_cal, mod_gate_status, mod_gate_reason = _phys_cal(
        "module_temp_calibration", "battery.conditions.thermal_spread_c")
    if mod_gate_status is not None:
        exp_gate["battery.conditions.exposure_high_temp_s"] = (
            mod_gate_status, mod_gate_reason)
        exp_gate["battery.conditions.exposure_low_temp_s"] = (
            mod_gate_status, mod_gate_reason)
    elif mod_gate_cal is None:
        exp_gate["battery.conditions.exposure_high_temp_s"] = (
            "unavailable", "missing_calibration:module_temp_calibration")
        exp_gate["battery.conditions.exposure_low_temp_s"] = (
            "unavailable", "missing_calibration:module_temp_calibration")
    pack_gate_cal, pack_gate_status, pack_gate_reason = _phys_cal(
        "pack_current_calibration",
        "battery.conditions.exposure_high_current_s")
    if pack_gate_status is not None:
        exp_gate["battery.conditions.exposure_high_current_s"] = (
            pack_gate_status, pack_gate_reason)
    elif pack_gate_cal is None and curr_kind == "none":
        if pack_configured:
            exp_gate["battery.conditions.exposure_high_current_s"] = (
                "unavailable",
                "missing_calibration:pack_current_calibration")
    exposures = [
        ("battery.conditions.exposure_high_soc_s", soc_pts, soc_bar,
         thresholds["soc_high"], "soc_high_pct",
         lambda v, t=thresholds["soc_high"]: v >= t if t is not None else False,
         "soc>=%r%%"),
        ("battery.conditions.exposure_low_soc_s", soc_pts, soc_bar,
         thresholds["soc_low"], "soc_low_pct",
         lambda v, t=thresholds["soc_low"]: v <= t if t is not None else False,
         "soc<=%r%%"),
        ("battery.conditions.exposure_high_temp_s", temp_max_pts,
         temp_max_bar, thresholds["temp_high"], "temp_high_c",
         lambda v, t=thresholds["temp_high"]: v >= t
         if t is not None else False, "batt_max>=%rC"),
        ("battery.conditions.exposure_low_temp_s", temp_min_pts,
         temp_min_bar, thresholds["temp_low"], "temp_low_c",
         lambda v, t=thresholds["temp_low"]: v <= t
         if t is not None else False, "batt_min<=%rC"),
        ("battery.conditions.exposure_high_current_s", curr_pts, curr_bar,
         thresholds["curr_high"], "current_high_a",
         lambda v, t=thresholds["curr_high"]: abs(v) >= t
         if t is not None else False, "abs_current>=%rA"),
    ]
    for metric, pairs, barriers, thresh, key, pred, fmt in exposures:
        if metric in thresh_errors:
            rows.append(_row(metric, None, None, "error",
                             thresh_errors[metric], scope, window,
                             revision=rev(metric, "threshold_error")))
            continue
        if thresh is None:
            rows.append(_row(
                metric, None, "s", "unavailable",
                "missing_threshold:%s" % key, scope, window,
                evidence=len(pairs), sample=len(pairs),
                revision=rev(metric, "missing_threshold")))
            continue
        if metric in exp_gate:
            status, reason = exp_gate[metric]
            rows.append(_row(metric, None,
                             None if status == "error" else "s", status,
                             reason, scope, window,
                             evidence=len(pairs), sample=len(pairs),
                             revision=rev(metric, "cal_gate")))
            continue
        if not pairs:
            rows.append(_row(metric, None, "s", "unavailable",
                             "sparse:no_condition_samples", scope, window,
                             evidence=0, revision=rev(metric, "sparse")))
            continue
        try:
            res = exposure_s(pairs, pred, gap, barriers)
        except ConditionsError as exc:
            rows.append(_row(metric, None, "s", "error", str(exc), scope,
                             window, revision=rev(metric, "error")))
            continue
        gaps_rejected[0] += res["legs_rejected"]
        if res["exposure_s"] is None:
            reason = "sparse:single_condition_sample" if len(pairs) < 2 \
                else "gap_exceeded:no_usable_leg"
            rows.append(_row(metric, None, "s", "unavailable", reason, scope,
                             window, evidence=len(pairs),
                             revision=rev(metric, "sparse")))
            continue
        detail = ""
        if metric == "battery.conditions.exposure_high_current_s":
            detail = " source=%s" % curr_kind
        rows.append(_row(
            metric, res["exposure_s"], "s", "derived",
            ("threshold " + fmt + " valid_covered=%.1fs legs=%d/%d%s")
            % (thresh, res["valid_covered_s"],
               res["legs_used"], res["legs_used"] + res["legs_rejected"],
               detail),
            scope, window, evidence=res["legs_used"], sample=len(pairs),
            coverage=res["coverage_ratio"],
            calibration_version=(
                mod_cal["version"] if key in ("temp_high_c", "temp_low_c")
                else pack_cal["version"]
                if key == "current_high_a" and curr_kind == "pack"
                else None),
            revision=rev(metric, pairs, thresh, gap)))

    # --- Diagnostics ---
    if curr_kind == "none" and pack_configured and pack_cal is None:
        curr_source = "none+pack_uncalibrated"
    else:
        curr_source = curr_kind
    text = "valid=%d total=%d invalid=%d ambiguous_ts=%d deduped=%d " \
        "after_decision=%d gaps_rejected=%d current_source=%s" % (
            valid, len(ordered), invalid, ambiguous, deduped,
            after_decision, gaps_rejected[0], curr_source)
    rows.append(_row("battery.conditions.diagnostics", None, None,
                     "derived", "scope_quality_gaps_reorder_dedup",
                     scope, window, evidence=valid, sample=len(ordered),
                     value_text=text, revision=rev("diagnostics", text)))
    return rows
