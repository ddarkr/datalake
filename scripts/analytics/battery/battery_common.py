#!/usr/bin/env python3
"""Shared plain-Python helpers for battery analytics. Standard library only.

Normalized signal input (dict):
  event_time_ns (int, required), ingest_time_ns (int|None),
  vehicle / source / decode_epoch (str, required),
  path / source_field (str|None),
  value_num (finite float|None), value_text (str|None),
  value_bool (bool|None), unit (str|None),
  quality (str|None: None/'valid'/'ok'/'unit_unverified' = measurable;
    anything else, e.g. 'invalid'/'range_rejected', stops segments/joins),
  envelope_id / config_version / connectivity (str|None).
Old signals lack provenance: quality/envelope/config stay None, never invented.
Identity fields must be non-empty strings; huge ints convert to None, never
raise.

Normalized event input adds per-event quality and retains config_version,
event_id, body_redacted, connectivity (body content never stored).

Stable analysis contract: every analysis module exposes
  analyze(signals, events, config) -> list[dict]
where each dict is a make_result() row. Dispatch through run_analyses(),
which isolates per-module execution failure. No universal thresholds and
no default confidence live here; modules report status explicitly.
"""

import hashlib
from bisect import bisect_right
from itertools import groupby

SCHEMA_VERSION = "1"
CODE_VERSION = "1.0.1"

STATUSES = frozenset({"reported", "derived", "estimated", "unavailable", "error"})

# Qualities that still count as measurable. 'unit_unverified' keeps the raw
# numeric (unit NULL, never zero-filled); analyzers gate on known units.
# Any other non-None quality ('invalid', 'range_rejected', 'unknown_start',
# ...) is preserved on the row but stops segments and joins.
VALID_QUALITIES = frozenset({"valid", "ok", "unit_unverified"})

INT64_MAX = 2 ** 63 - 1

ANALYZE_CONTRACT = "analyze(signals, events, config) -> list[dict]"


def is_finite_number(value):
    """Strict finite check: int/float only (never bool, never strings)."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return (isinstance(value, float) and value == value
            and value not in (float("inf"), float("-inf")))


def is_valid_quality(quality):
    """None (old rows) or an explicitly measurable literal; everything else
    stops carry while staying preserved on the row."""
    return quality is None or quality in VALID_QUALITIES


def safe_float(value):
    """float(value) or None. Huge ints yield None, never OverflowError."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        try:
            result = float(value)
        except (OverflowError, ValueError):
            return None
        return result if is_finite_number(result) else None
    if isinstance(value, float):
        return value if is_finite_number(value) else None
    return None


def _canonical(obj):
    """Deterministic serialization: dicts sort by key, so equal observations
    hash equal regardless of insertion order. Scalars match repr()."""
    if isinstance(obj, dict):
        items = sorted((_canonical(k), _canonical(v)) for k, v in obj.items())
        return "{" + ",".join(k + ":" + v for k, v in items) + "}"
    if isinstance(obj, (list, tuple)):
        return "[" + ",".join(_canonical(v) for v in obj) + "]"
    if isinstance(obj, (set, frozenset)):
        return "[" + ",".join(sorted(_canonical(v) for v in obj)) + "]"
    return repr(obj)


def _ingest_rank(sig):
    """Sort key: earliest real ingest wins; unknown (None) sorts last."""
    ingest = sig.get("ingest_time_ns")
    if isinstance(ingest, bool) or not isinstance(ingest, int):
        return (1, 0)
    return (0, ingest)


def to_ns(value):
    """Strict nanosecond epoch: int (not bool) in (0, INT64_MAX]; else None.

    Floats are rejected even when integral: ns precision does not survive
    float64, so accepting them would silently corrupt event identity.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 < value <= INT64_MAX else None


def _opt_str(raw, key):
    value = raw.get(key)
    return value if isinstance(value, str) and value else None


def normalize_signal(raw):
    """Canonical signal dict, or None when time/identity is missing.

    Non-finite numerics become None (unmeasurable, never zero); identity
    fields must be non-empty strings (never ints/bools); huge ints become
    None instead of raising. Provenance absent on old rows stays None,
    never invented.
    """
    if not isinstance(raw, dict):
        return None
    ts = to_ns(raw.get("event_time_ns", raw.get("event_time")))
    vehicle = _opt_str(raw, "vehicle")
    source = _opt_str(raw, "source")
    epoch = _opt_str(raw, "decode_epoch")
    if ts is None or vehicle is None or source is None or epoch is None:
        return None
    flag = raw.get("value_bool")
    if isinstance(flag, bool):
        boolean = flag
    elif isinstance(flag, int) and flag in (0, 1):
        boolean = bool(flag)
    else:
        boolean = None
    text = raw.get("value_text")
    text = text if isinstance(text, str) and text else None
    return {
        "event_time_ns": ts,
        "ingest_time_ns": to_ns(raw.get("ingest_time_ns", raw.get("ingest_time"))),
        "vehicle": vehicle,
        "source": source,
        "decode_epoch": epoch,
        "path": _opt_str(raw, "path"),
        "source_field": _opt_str(raw, "source_field"),
        "value_num": safe_float(raw.get("value_num")),
        "value_text": text,
        "value_bool": boolean,
        "unit": _opt_str(raw, "unit"),
        "quality": _opt_str(raw, "quality"),
        "envelope_id": _opt_str(raw, "envelope_id"),
        "config_version": _opt_str(raw, "config_version"),
        "connectivity": _opt_str(raw, "connectivity"),
    }


class _PreparedSignals(list):
    """Private marker for once-canonicalized signal collections."""
    pass


def prepare_signals(signals):
    """Validate/canonicalize raw rows once; order, duplicates, barriers kept."""
    return _PreparedSignals(normalize_signals(signals))


def prepare_by_scope(signals):
    """Single-pass normalize + group by (vehicle, source, decode_epoch).

    One scan of the raw input; each scope gets its own _PreparedSignals
    so per-scope prepared rows keep mutation isolation (normalize_signals
    on a _PreparedSignals still detaches via copies) and cross-scope
    quality never merges. Rows missing time/identity drop, never guessed.
    """
    grouped = {}
    for raw in signals or []:
        sig = normalize_signal(raw)
        if sig is None:
            continue
        key = (sig["vehicle"], sig["source"], sig["decode_epoch"])
        grouped.setdefault(key, []).append(sig)
    return {key: _PreparedSignals(rows) for key, rows in grouped.items()}


def normalize_signals(signals):
    """Fresh normalized rows; prepared input skips revalidation with copies."""
    if isinstance(signals, _PreparedSignals):
        return [dict(row) for row in signals]
    normed = []
    for raw in signals or []:
        sig = normalize_signal(raw)
        if sig is not None:
            normed.append(sig)
    return normed


def normalize_event(raw):
    """Canonical event dict, or None when envelope time/identity is missing.

    A missing start is retained with duration None (warning kept, duration
    and recurrence excluded downstream). Conflicting ends (end < start) keep
    both stamps but yield no duration. Per-event quality, config_version,
    event_id, connectivity and body_redacted are retained; body content is
    never stored.
    """
    if not isinstance(raw, dict):
        return None
    ts = to_ns(raw.get("event_time_ns", raw.get("event_time")))
    event_type = _opt_str(raw, "event_type")
    name = _opt_str(raw, "name")
    vehicle = _opt_str(raw, "vehicle")
    source = _opt_str(raw, "source")
    if ts is None or event_type is None or name is None \
            or vehicle is None or source is None:
        return None
    started = to_ns(raw.get("started_ns", raw.get("started_at")))
    ended = to_ns(raw.get("ended_ns", raw.get("ended_at")))
    duration = None
    if started is not None and ended is not None and ended >= started:
        duration = (ended - started) / 1e9
    active = raw.get("is_active")
    active = active if isinstance(active, bool) else None
    body = raw.get("body")
    return {
        "event_time_ns": ts,
        "ingest_time_ns": to_ns(raw.get("ingest_time_ns", raw.get("ingest_time"))),
        "event_id": _opt_str(raw, "event_id"),
        "vehicle": vehicle,
        "event_type": event_type,
        "name": name,
        "source": source,
        "envelope_id": _opt_str(raw, "envelope_id"),
        "started_ns": started,
        "ended_ns": ended,
        "duration_s": duration,
        "audience": _opt_str(raw, "audience"),
        "is_active": active,
        "body_redacted": (True if body not in (None, "") and "body" in raw
                          else raw.get("body_redacted")
                          if isinstance(raw.get("body_redacted"), bool) else None),
        "source_system": _opt_str(raw, "source_system"),
        "decode_epoch": _opt_str(raw, "decode_epoch"),
        "quality": _opt_str(raw, "quality"),
        "config_version": _opt_str(raw, "config_version"),
        "connectivity": _opt_str(raw, "connectivity"),
    }


def episode_key(vehicle, event_type, name, started_ns, decode_epoch=None):
    """Pragmatic episode identity; None without a valid start (no fabrication).

    Byte-matches the recorder episode_id formula:
    sha256(vehicle|event_type|name|started_ns[|decode_epoch]).
    """
    if (not isinstance(vehicle, str) or not vehicle
            or not isinstance(event_type, str) or not event_type
            or not isinstance(name, str) or not name
            or to_ns(started_ns) is None):
        return None
    parts = [vehicle, event_type, name, str(started_ns)]
    if isinstance(decode_epoch, str) and decode_epoch:
        parts.append(decode_epoch)
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def group_by_scope(signals):
    """Isolate by (vehicle, source, decode_epoch); scopes are never merged."""
    groups = {}
    for sig in signals:
        groups.setdefault(
            (sig["vehicle"], sig["source"], sig["decode_epoch"]), []).append(sig)
    return groups


def _sig_key(sig):
    return (sig.get("vehicle"), sig.get("source"), sig.get("decode_epoch"),
            sig.get("path"), sig.get("source_field"), sig.get("event_time_ns"),
            sig.get("value_num"), sig.get("value_text"), sig.get("value_bool"),
            sig.get("unit"), sig.get("quality"), sig.get("envelope_id"),
            sig.get("config_version"), sig.get("connectivity"))


def _sort_prefix(sig):
    return (sig["event_time_ns"],
            sig.get("source_field") or sig.get("path") or "",
            repr((sig.get("value_num"), sig.get("value_text"),
                  sig.get("value_bool"))))


def sort_dedup(signals):
    """Sort by (time, field, payload, canonical row); collapse exact key
    matches only.

    The same timestamp with a different payload, quality, unit, or envelope
    is NOT the same sample and is kept, so invalid-vs-valid semantics and
    provenance survive. An identical retransmit keeps the earliest actual
    ingest regardless of arrival order (unknown ingest loses to any real
    stamp). The same observation set always yields the same rows.
    """
    cheap = sorted(signals, key=_sort_prefix)
    ordered = []
    for _, tied in groupby(cheap, key=_sort_prefix):
        tied = list(tied)
        if len(tied) > 1:
            tied.sort(key=_canonical)
        ordered.extend(tied)
    out, index = [], {}
    for sig in ordered:
        key = _sig_key(sig)
        pos = index.get(key)
        if pos is None:
            index[key] = len(out)
            out.append(sig)
            continue
        if _ingest_rank(sig) < _ingest_rank(out[pos]):
            out[pos] = sig
    return out


def split_on_invalid(rows):
    """Split sorted signals wherever a sample is unmeasurable (numeric None
    or non-valid quality). Invalid stops the segment; nothing is filled."""
    segments, cur = [], []
    for row in rows:
        if row.get("value_num") is None \
                or not is_valid_quality(row.get("quality")):
            if cur:
                segments.append(cur)
                cur = []
            continue
        cur.append(row)
    if cur:
        segments.append(cur)
    return segments


def join_asof(primary, secondary, max_skew_ns):
    """For each primary row, the latest in-scope secondary at t <= primary t
    within max_skew_ns. Secondary is sorted internally, so any input order
    gives the same matches.

    Missing, stale, cross-scope, non-valid-quality, or unmeasurable
    secondary yields None: invalid stops the join (never skip back to an
    older valid one), never fills. Contradictory same-time secondaries
    (different payload, or any invalid/unmeasurable member) are ambiguous
    and yield None. A non-valid primary quality never matches.
    """
    if isinstance(max_skew_ns, bool) or not isinstance(max_skew_ns, int) \
            or max_skew_ns < 0:
        raise ValueError("max_skew_ns must be a non-negative int")
    ordered = sorted(secondary, key=lambda s: s["event_time_ns"])
    times = [s["event_time_ns"] for s in ordered]
    out = []
    for prob in primary:
        match = None
        if is_valid_quality(prob.get("quality")):
            scope = (prob.get("vehicle"), prob.get("source"),
                     prob.get("decode_epoch"))
            ptime = prob["event_time_ns"]
            same, cand_time = [], None
            idx = bisect_right(times, ptime) - 1
            # ponytail: linear scope scan per probe; pre-group by scope
            # only if profiles show it matters.
            while idx >= 0:
                cand = ordered[idx]
                ctime = cand["event_time_ns"]
                if cand_time is not None and ctime < cand_time:
                    break
                if (cand.get("vehicle"), cand.get("source"),
                        cand.get("decode_epoch")) != scope:
                    idx -= 1
                    continue
                if cand_time is None:
                    if ptime - ctime > max_skew_ns:
                        break  # stale; older rows are only staler
                    cand_time = ctime
                same.append(cand)
                idx -= 1
            if cand_time is not None:
                payloads = set()
                for cand in same:
                    if not is_valid_quality(cand.get("quality")) \
                            or safe_float(cand.get("value_num")) is None:
                        payloads = set()
                        break
                    payloads.add((repr(cand.get("value_num")),
                                  cand.get("value_text"),
                                  cand.get("value_bool")))
                if len(payloads) == 1:
                    match = min(same, key=_ingest_rank)
        out.append((prob, match))
    return out


def counter_delta_ns(pairs):
    """Delta over sorted [(ns, value)]; None when sparse, any member None or
    non-finite, any step decreasing (reset/wrap), unordered, or unrepresentable
    (huge ints). Never filled."""
    if len(pairs) < 2:
        return None
    prev_t, prev = pairs[0]
    if to_ns(prev_t) is None or not is_finite_number(prev):
        return None
    for tst, val in pairs[1:]:
        if to_ns(tst) is None or tst < prev_t:
            return None
        if not is_finite_number(val) or val < prev:
            return None
        prev_t, prev = tst, val
    try:
        return float(pairs[-1][1]) - float(pairs[0][1])
    except OverflowError:
        return None


def trap_integral_ns(pairs, max_gap_ns):
    """Trapezoidal integral over sorted [(ns, value)] in value*hours.

    max_gap_ns (non-negative int, required) caps every leg: wider legs are
    rejected, never integrated as if fully observed. Returns stats so callers
    report coverage instead of silently integrating partial paths:
      {"value", "legs_used", "legs_rejected", "span_ns", "coverage_ratio"}
    coverage_ratio is integrated seconds over span seconds (1.0 on zero span
    with a used leg; None with no usable leg). Legs touching bad stamps,
    negative dt, non-finite values, or unrepresentable ints are rejected,
    never filled.
    """
    if isinstance(max_gap_ns, bool) or not isinstance(max_gap_ns, int) \
            or max_gap_ns < 0:
        raise ValueError("max_gap_ns must be a non-negative int")
    total, used, rejected, used_dt = 0.0, 0, 0, 0
    stamps = [t for t, _ in pairs if to_ns(t) is not None]
    span = (max(stamps) - min(stamps)) if len(stamps) >= 2 else None
    for (t0, v0), (t1, v1) in zip(pairs, pairs[1:]):
        if (to_ns(t0) is None or to_ns(t1) is None or t1 < t0
                or safe_float(v0) is None or safe_float(v1) is None
                or t1 - t0 > max_gap_ns):
            rejected += 1
            continue
        try:
            total += (v0 + v1) / 2.0 * (t1 - t0) / 3.6e12
        except OverflowError:
            rejected += 1
            continue
        used += 1
        used_dt += t1 - t0
    if not used or not is_finite_number(total):
        return {"value": None, "legs_used": used, "legs_rejected": rejected,
                "span_ns": span, "coverage_ratio": None}
    if span:
        coverage = used_dt / span
    else:
        coverage = 1.0
    return {"value": total, "legs_used": used, "legs_rejected": rejected,
            "span_ns": span, "coverage_ratio": coverage}


def revision_id(*parts):
    """Deterministic revision: same inputs + versions -> same id, so a rerun
    replaces the same row instead of duplicating it. Dicts serialize with
    sorted keys, so observation order never perturbs the id."""
    return hashlib.sha256(
        "\x1f".join(_canonical(p) for p in parts).encode("utf-8")).hexdigest()


def make_result(metric, value, unit, status, reason=None, window_start_ns=None,
                window_end_ns=None, vehicle=None, source=None, decode_epoch=None,
                evidence_count=None, sample_count=None, coverage_ratio=None,
                algorithm_version=None, calibration_version=None,
                model_version=None, uncertainty=None, uncertainty_lower=None,
                uncertainty_upper=None, value_text=None, computed_at_ns=None,
                quality=None, config_version=None, connectivity=None,
                episode_id=None, analysis_id=None, revision=None):
    """Flat dashboard-safe row (no JSON needed for mandatory fields).

    'unavailable'/'error' force value NULL. A 'derived' row needs a numeric
    value or a defined value_text (text-only episode/method metric).
    Uncertainty appears only when actually computed, never defaulted (no
    fake confidence): negative/non-finite spreads and unordered bounds
    raise, as do invalid windows (bad bound, or end before start).
    """
    if status not in STATUSES:
        raise ValueError("unknown status: %r" % (status,))
    if not isinstance(metric, str) or not metric:
        raise ValueError("metric is required")
    start = end = computed = None
    if window_start_ns is not None:
        start = to_ns(window_start_ns)
        if start is None:
            raise ValueError("invalid window_start_ns")
    if window_end_ns is not None:
        end = to_ns(window_end_ns)
        if end is None:
            raise ValueError("invalid window_end_ns")
    if start is not None and end is not None and end < start:
        raise ValueError("window_end_ns precedes window_start_ns")
    if computed_at_ns is not None:
        computed = to_ns(computed_at_ns)
        if computed is None:
            raise ValueError("invalid computed_at_ns")
    text = value_text if isinstance(value_text, str) and value_text else None
    if status in ("unavailable", "error"):
        value = None
    elif value is not None:
        value = safe_float(value)
        if value is None:
            raise ValueError("non-finite value requires status 'unavailable'")
    elif status == "derived" and text is None:
        raise ValueError("derived result needs a value or value_text")

    def _count(val):
        return val if isinstance(val, int) and not isinstance(val, bool) \
            and val >= 0 else None

    def _spread(val):
        if val is None:
            return None
        conv = safe_float(val)
        if conv is None or conv < 0:
            raise ValueError("uncertainty must be a non-negative finite number")
        return conv

    def _bound(val, name):
        if val is None:
            return None
        conv = safe_float(val)
        if conv is None:
            raise ValueError(name + " must be a finite number")
        return conv

    lower = _bound(uncertainty_lower, "uncertainty_lower")
    upper = _bound(uncertainty_upper, "uncertainty_upper")
    if lower is not None and upper is not None and lower > upper:
        raise ValueError("uncertainty_lower exceeds uncertainty_upper")

    cov = None
    if is_finite_number(coverage_ratio) and 0 <= coverage_ratio <= 1:
        cov = float(coverage_ratio)
    return {
        "metric": metric,
        "value": value,
        "value_text": text,
        "unit": unit,
        "status": status,
        "reason": reason if isinstance(reason, str) and reason else None,
        "window_start_ns": start,
        "window_end_ns": end,
        "vehicle": vehicle,
        "source": source,
        "decode_epoch": decode_epoch,
        "evidence_count": _count(evidence_count),
        "sample_count": _count(sample_count),
        "coverage_ratio": cov,
        "algorithm_version": algorithm_version or None,
        "calibration_version": calibration_version or None,
        "model_version": model_version or None,
        "uncertainty": _spread(uncertainty),
        "uncertainty_lower": lower,
        "uncertainty_upper": upper,
        "computed_at_ns": computed,
        "quality": quality if isinstance(quality, str) and quality else None,
        "config_version": config_version if isinstance(config_version, str)
        and config_version else None,
        "connectivity": connectivity if isinstance(connectivity, str)
        and connectivity else None,
        "episode_id": episode_id or None,
        "analysis_id": analysis_id or None,
        "revision": revision or None,
    }


def run_analyses(signals, events, config, analyzers):
    """Dispatch each analyze(signals, events, config) -> list[dict].

    A module raising becomes one 'error' row with a safe reason
    ('execution_error:<Type>', never the message); a non-list return becomes
    'contract_violation:non_list_return'. Each error row carries the module
    key in metric and analysis_id, disambiguated so same-named functions
    never collapse to one name. Every other module's rows are preserved.
    Only returned rows are upserted downstream, so a failed run never
    refreshes stale values.
    """
    config = config if isinstance(config, dict) else {}
    analyzers = list(analyzers)
    bases = [getattr(fn, "__name__", None) or "analyzer" for fn in analyzers]
    out = []
    for pos, fn in enumerate(analyzers):
        name = bases[pos]
        if bases.count(name) > 1:
            name = "%s#%d" % (name, pos)
        try:
            rows = fn(signals, events, config)
        except Exception as exc:  # execution failure, distinct from no data
            out.append(make_result(metric=name, value=None, unit=None,
                                   status="error",
                                   reason="execution_error:"
                                   + type(exc).__name__,
                                   analysis_id=name))
            continue
        if not isinstance(rows, list):
            out.append(make_result(metric=name, value=None, unit=None,
                                   status="error",
                                   reason="contract_violation:non_list_return",
                                   analysis_id=name))
            continue
        out.extend(rows)
    return out
