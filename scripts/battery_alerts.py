#!/usr/bin/env python3
"""OEM alert lifecycle reducer. Standard library only.

Order-independent reduction of immutable observed ``vehicle_event`` rows
(``event_type == "alerts"``) into pragmatic episodes keyed by
vehicle + exact name + valid ``started_ns`` (plus decode epoch in scope).
The key is pragmatic linkage, never claimed as a guaranteed Tesla UID.

Semantics (no invention, no causal claims):
  - Lifecycle is built from retained history, not just in-window envelopes:
    pre-window observations are kept so overlapping active episodes are not
    lost. ``starts``/``ends`` count actual event boundaries inside the
    requested window; overlapping episode snapshots are still emitted even
    when the start lies outside the window.
  - Observation dedup (scoped ``event_id`` or canonical row) is separate
    from episode identity (vehicle/name/valid start). Re-observing the same
    episode never inflates recurrence: recurrence counts distinct episodes.
  - Explicit distinct ``ended_ns`` candidates conflict: no authoritative
    duration. A late active observation with envelope ``event_time <= end``
    is normal (late arrival); active with ``event_time > end`` conflicts.
  - ``active`` requires actual valid active evidence (``is_active`` True on
    a non-error observation). A malformed present ``endedAt`` (quality
    ``error``, ``is_active`` None) never implies active. Observation quality
    is exposed per episode: ``error`` (all error), ``conflicting`` (mixed),
    else None. An explicit inactive observation with no end contradicts.
  - Absence and connectivity/disconnect never close an episode. Errors and
    connectivity are distinct event types, never battery alerts.
  - Missing/unparseable start retains the warning (per-observation row,
    ``episode_id`` None) but is excluded from duration and recurrence.
  - Explicit valid start + single valid end reports the OEM duration even
    when no opening active observation was seen (status ``reported``).
  - Counts and rates derive only from matched windows: ``starts``/``ends``
    by boundary in window, recurrence repeats with start in window (beyond
    the first known episode of that name in retained history). The rate
    numerator and denominator share that window.
  - Online prediction (``decision_time_ns`` set) admits only observations
    with real ``ingest_time_ns <= decision_time``. Unknown ingest cannot
    prove availability and is excluded (counted in reasons); late ingest is
    excluded. A PRESENT but malformed time bound raises ValueError (never
    a silent offline/unbounded fallback); only absent/None means offline.
    Invalid event timestamps are dropped by normalization.
  - Warning dictionary is config-provided exact-name only, with source and
    version provenance and an unknown fallback. ``audience`` is never mapped
    to severity and no diagnosis is invented.
  - Surrounding signal context is bounded per episode ([start-skew,
    end+skew]) and reported as association, never causation: a count row
    plus per-field before/during/after condition summaries (mean/min/max,
    units, first/last timestamps). Invalid/unmeasurable signals are
    excluded; unknown-unit values stay raw (unit NULL, no physical claim).
    Revision hashes include the influencing signal observations.
  - Recurrence rate is an observed calendar rate over matched window days
    with unknown coverage (change-gated telemetry never implies guaranteed
    coverage), unless explicit bounded coverage exposure
    (``coverage_start_ns``/``coverage_end_ns``) is supplied, in which case
    the rate is exposure-adjusted over that caller-asserted span. Any
    PRESENT malformed/reversed/half-supplied bound raises ValueError.

Config shape (plain dict):
  Top level: ``window_start_ns`` (int|None), ``window_end_ns`` (int|None),
  ``decision_time_ns`` (int|None).
  Under ``"alerts"`` (dict, all optional):
    ``warning_dictionary``: {exact name: description str}
    ``dictionary_source``: str provenance (e.g. owner-manual URL)
    ``dictionary_version``: str provenance (e.g. "2026-09-01")
    ``context_skew_ns``: non-negative int, default 600_000_000_000 (10 min)
    ``context_source_fields``: list of exact ``source_field`` strs or None
    ``coverage_start_ns``/``coverage_end_ns``: explicit bounded coverage
      exposure for the exposure-adjusted recurrence rate (both required,
      end > start); otherwise the calendar rate is reported.

Public helpers:
  ``dedup_events(events)`` order-independent scoped observation dedup.
  ``reduce_episode(observations)`` single-episode reducer; output retains
  ``end_candidates`` (distinct explicit ends, conflict sources) plus
  ``quality``/``quality_counts``/``active_evidence``.
  ``analyze(signals, events, config)`` -> list[dict] via make_result.

Persisted identity (vehicle_analysis PK has no episode_id column, so
revision alone cannot keep episodes apart): per-episode episode/context
rows use ``battery_alerts:episode:<episode_id>`` /
``battery_alerts:context:<episode_id>``, per-field per-phase condition
rows use ``battery_alerts:condition:<episode_id>:<field>:<phase>``, and
each unknown-start warning uses a stable
``battery_alerts:unknown:<hash16>`` derived from its observation identity.
Scope scalars keep ``battery_alerts``. ``episode_id`` still rides every
row; runtime should also include it in persisted logical identity.
"""

import battery_common as bc

ALGORITHM_VERSION = "1.2.1"
ANALYSIS_ID = "battery_alerts"
DEFAULT_CONTEXT_SKEW_NS = 600_000_000_000

METRIC_EPISODE = "battery.alerts.episode"
METRIC_STARTS = "battery.alerts.starts"
METRIC_ENDS = "battery.alerts.ends"
METRIC_ACTIVE = "battery.alerts.active"
METRIC_CONFLICTED = "battery.alerts.conflicted"
METRIC_RECURRENCE = "battery.alerts.recurrence"
METRIC_RECURRENCE_RATE = "battery.alerts.recurrence_rate"
METRIC_CONTEXT = "battery.alerts.context"
METRIC_CONDITION = "battery.alerts.condition"

SUPPORTED_SCALARS = (
    METRIC_STARTS,
    METRIC_ENDS,
    METRIC_ACTIVE,
    METRIC_CONFLICTED,
    METRIC_RECURRENCE,
    METRIC_RECURRENCE_RATE,
)

PHASES = ("before", "during", "after")


def _episode_analysis_id(episode_id):
    """Stable per-episode persisted identity (episode_id not in table PK).

    Sharing one analysis_id across episodes would silently overwrite them;
    episode_id is never None here (valid-start episodes only).
    """
    return "battery_alerts:episode:%s" % (episode_id,)


def _context_analysis_id(episode_id):
    """Stable per-episode context persisted identity."""
    return "battery_alerts:context:%s" % (episode_id,)


def _condition_analysis_id(episode_id, field, phase):
    """Stable per-field per-phase condition persisted identity."""
    safe = "".join(c if (c.isalnum() or c in ("_", "-", ".")) else "_"
                  for c in str(field))
    return "battery_alerts:condition:%s:%s:%s" % (
        episode_id, safe or "unknown_field", phase)

def _unknown_analysis_id(e):
    """Stable per-observation PK identity for unknown-start warnings."""
    seed = e.get("event_id") or _canon(e)
    hex16 = bc.revision_id("battery_alerts:unknown", e.get("vehicle"),
                           e.get("source"), e.get("decode_epoch"),
                           e.get("name"), e.get("event_time_ns"), seed)[:16]
    return "battery_alerts:unknown:%s" % (hex16,)


def _canon(obj):
    """Deterministic serialization local to this module (dicts sort by key)."""
    if isinstance(obj, dict):
        items = sorted((_canon(k), _canon(v)) for k, v in obj.items())
        return "{" + ",".join(k + ":" + v for k, v in items) + "}"
    if isinstance(obj, (list, tuple)):
        return "[" + ",".join(_canon(v) for v in obj) + "]"
    if isinstance(obj, (set, frozenset)):
        return "[" + ",".join(sorted(_canon(v) for v in obj)) + "]"
    return repr(obj)


def _ingest_rank(row):
    """Sort key: earliest real ingest wins; unknown (None) sorts last."""
    ingest = row.get("ingest_time_ns")
    if isinstance(ingest, bool) or not isinstance(ingest, int):
        return (1, 0)
    return (0, ingest)


def _scope_sort(scope):
    """Sort key for (vehicle, source, epoch) scopes tolerating None epoch."""
    v, s, e = scope
    return (v or "", s or "", e or "")


def _alert_cfg(config):
    if isinstance(config, dict):
        sub = config.get("alerts")
        if isinstance(sub, dict):
            return sub
    return {}


def _window(config):
    """Requested window; absent/None means unbounded (offline-style).

    A PRESENT but malformed bound raises ValueError (trust boundary: never
    silently widen to unbounded). A reversed window (end < start) raises.
    """
    raw_ws = config.get("window_start_ns") if isinstance(config, dict) else None
    raw_we = config.get("window_end_ns") if isinstance(config, dict) else None
    ws = None if raw_ws is None else bc.to_ns(raw_ws)
    we = None if raw_we is None else bc.to_ns(raw_we)
    if raw_ws is not None and ws is None:
        raise ValueError("invalid window_start_ns")
    if raw_we is not None and we is None:
        raise ValueError("invalid window_end_ns")
    if ws is not None and we is not None and we < ws:
        raise ValueError("window_end_ns precedes window_start_ns")
    return ws, we


def _decision_time(config):
    """Online decision time; absent/None means offline (admit all).

    A PRESENT but malformed value raises ValueError (trust boundary: never
    fall back to offline and admit future evidence).
    """
    raw = config.get("decision_time_ns") if isinstance(config, dict) else None
    if raw is None:
        return None
    decision = bc.to_ns(raw)
    if decision is None:
        raise ValueError("invalid decision_time_ns")
    return decision


def _coverage(alert_cfg):
    """Explicit caller-asserted coverage exposure, or (None, None).

    Absent (both missing/None) means no asserted exposure (calendar rate
    fallback). Any PRESENT bound must be valid, both bounds are required,
    and end must exceed start; otherwise ValueError (never silently drop
    to calendar rate).
    """
    raw_s = alert_cfg.get("coverage_start_ns")
    raw_e = alert_cfg.get("coverage_end_ns")
    if raw_s is None and raw_e is None:
        return None, None
    start = None if raw_s is None else bc.to_ns(raw_s)
    end = None if raw_e is None else bc.to_ns(raw_e)
    if start is None:
        raise ValueError("invalid coverage_start_ns")
    if end is None:
        raise ValueError("invalid coverage_end_ns")
    if end <= start:
        raise ValueError("coverage_end_ns must exceed coverage_start_ns")
    return start, end


def _dictionary(alert_cfg):
    raw = alert_cfg.get("warning_dictionary")
    table = {}
    if isinstance(raw, dict):
        for key, val in raw.items():
            if isinstance(key, str) and key and isinstance(val, str) and val:
                table[key] = val
    source = alert_cfg.get("dictionary_source")
    source = source if isinstance(source, str) and source else None
    version = alert_cfg.get("dictionary_version")
    version = version if isinstance(version, str) and version else None
    return table, source, version


def _skew(alert_cfg):
    raw = alert_cfg.get("context_skew_ns", DEFAULT_CONTEXT_SKEW_NS)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        return DEFAULT_CONTEXT_SKEW_NS
    return raw


def _context_fields(alert_cfg):
    raw = alert_cfg.get("context_source_fields")
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        fields = [f for f in raw if isinstance(f, str) and f]
        return fields if fields else []
    return None


def _ingest_status(row, decision_time):
    """Availability vs decision time: admitted | unknown | late.

    Offline (no decision time) admits everything. Online, only a real
    ingest stamp at or before decision time proves availability; unknown
    ingest is excluded (unprovable), late ingest is excluded (future).
    """
    if decision_time is None:
        return "admitted"
    ingest = row.get("ingest_time_ns")
    if ingest is None:
        return "unknown"
    return "admitted" if ingest <= decision_time else "late"


def _in_window(event_time, ws, we):
    if ws is not None and event_time < ws:
        return False
    if we is not None and event_time > we:
        return False
    return True


def _event_dedup_key(ev):
    """Scoped observation identity: same event_id in another vehicle, source
    or decode epoch is a different observation and never merges."""
    eid = ev.get("event_id")
    if isinstance(eid, str) and eid:
        return ("id", ev.get("vehicle"), ev.get("source"),
                ev.get("decode_epoch"), eid)
    return ("row", _canon(ev))


def dedup_events(events):
    """Collapse retransmits order-independently; earliest real ingest wins.

    Dedup key is scoped ``event_id`` when present, else the canonical row.
    The surviving row keeps the earliest ingest stamp; unknown ingest loses
    to any real stamp. Output is deterministically sorted.
    """
    best = {}
    for ev in events:
        if not isinstance(ev, dict):
            continue
        key = _event_dedup_key(ev)
        prev = best.get(key)
        if prev is None:
            best[key] = ev
            continue
        if _ingest_rank(ev) < _ingest_rank(prev):
            best[key] = ev
    return sorted(best.values(), key=lambda e: (
        e.get("event_time_ns") or 0,
        e.get("vehicle") or "",
        e.get("source") or "",
        e.get("decode_epoch") or "",
        e.get("name") or "",
        e.get("started_ns") or 0,
        e.get("ended_ns") or 0,
        e.get("event_id") or "",
        _canon(e),
    ))


def reduce_episode(observations):
    """Reduce one pragmatic episode to lifecycle facts.

    Input: normalized deduped ``alerts`` observations sharing one
    (vehicle, name, valid started_ns) identity. Output dict retains
    ``end_candidates`` (sorted distinct explicit ``ended_ns``) so callers
    never lose conflict sources, plus ``quality`` (``error`` when every
    observation is a parse failure, ``conflicting`` when mixed, else None),
    ``quality_counts`` and ``active_evidence``. Active requires actual valid
    active evidence; a malformed present end never implies active. No
    thresholds, no severity, no diagnosis.
    """
    obs = [o for o in observations if isinstance(o, dict)]
    obs = sorted(obs, key=lambda o: (
        o.get("event_time_ns") or 0,
        o.get("ended_ns") or 0,
        o.get("event_id") or "",
        _canon(o),
    ))
    if not obs:
        return {"observation_count": 0, "end_candidates": [],
                "duration_s": None, "conflicted": False, "active": False,
                "episode_id": None, "started_ns": None, "name": None,
                "vehicle": None, "source": None, "decode_epoch": None,
                "event_type": "alerts", "quality": None, "quality_counts": {},
                "active_evidence": False,
                "has_active_observation": False,
                "observation_event_times": [],
                "authoritative_end_ns": None}
    first = obs[0]
    vehicle = first.get("vehicle")
    name = first.get("name")
    source = first.get("source")
    epoch = first.get("decode_epoch")
    started = first.get("started_ns")
    ends = sorted({o.get("ended_ns") for o in obs
                   if bc.to_ns(o.get("ended_ns")) is not None})
    episode_id = bc.episode_key(vehicle, "alerts", name, started, epoch) \
        if bc.to_ns(started) is not None else None
    counts = {}
    for o in obs:
        q = o.get("quality")
        counts[q if isinstance(q, str) and q else "none"] = \
            counts.get(q if isinstance(q, str) and q else "none", 0) + 1
    if all(o.get("quality") == "error" for o in obs):
        quality = "error"
    elif any(o.get("quality") == "error" for o in obs):
        quality = "conflicting"
    else:
        quality = None
    active_evidence = any(o.get("is_active") is True
                          and o.get("quality") != "error" for o in obs)
    conflicted = False
    duration = None
    active = False
    if bc.to_ns(started) is None:
        conflicted = False
        duration = None
    elif len(ends) == 0:
        if any(o.get("is_active") is False for o in obs):
            conflicted = True  # explicit inactive with no end contradicts
        else:
            active = active_evidence
    elif len(ends) >= 2:
        conflicted = True
    else:
        end = ends[0]
        if end < started:
            conflicted = True
        else:
            late_conflict = any(
                o.get("is_active") is True and o.get("quality") != "error"
                and o.get("event_time_ns") is not None
                and o["event_time_ns"] > end for o in obs)
            if late_conflict:
                conflicted = True
            else:
                duration = (end - started) / 1e9
    return {
        "vehicle": vehicle,
        "name": name,
        "source": source,
        "decode_epoch": epoch,
        "event_type": "alerts",
        "started_ns": started,
        "end_candidates": ends,
        "authoritative_end_ns": None if conflicted or duration is None else ends[0],
        "duration_s": duration,
        "conflicted": conflicted,
        "active": active,
        "observation_count": len(obs),
        "observation_event_times": sorted(o.get("event_time_ns") for o in obs),
        "has_active_observation": any(o.get("is_active") is True for o in obs),
        "active_evidence": active_evidence,
        "quality": quality,
        "quality_counts": counts,
        "episode_id": episode_id,
    }


def _dict_reason(name, table, source, version):
    if name in table:
        prov = "/".join(p for p in (source, version) if p) or "config"
        return "oem_alert %s: %s (dictionary %s known)" % (name, table[name], prov)
    prov = "/".join(p for p in (source, version) if p) or "missing"
    return "oem_alert %s: unknown_name (dictionary %s fallback)" % (name, prov)


def _overlaps(ep, ws, we):
    """Lifecycle overlap with the requested window (unbounded when unset)."""
    if ws is None and we is None:
        return True
    if we is not None and ep["started_ns"] > we:
        return False
    if ep["end_candidates"]:
        if ws is not None and max(ep["end_candidates"]) < ws:
            return False
    return True


def _boundary_in(boundary, ws, we):
    if ws is None and we is None:
        return True
    if boundary is None:
        return False
    if ws is not None and boundary < ws:
        return False
    if we is not None and boundary > we:
        return False
    return True


def _field_label(sig):
    field = sig.get("source_field")
    if isinstance(field, str) and field:
        return field
    path = sig.get("path")
    if isinstance(path, str) and path:
        return path
    return "unknown_field"


def _condition_rows(ep, inbounds, ws, we, vehicle, source, epoch,
                    dict_version, revision):
    """Per-field before/during/after summaries over bounded context signals.

    Only valid-quality finite numerics; invalid/unmeasurable excluded.
    Unknown-unit values stay raw (unit NULL, no physical claim). Timestamps
    and provenance ride the reason; every influencing signal canonical is
    returned for the revision hash.
    """
    start = ep["started_ns"]
    if ep["authoritative_end_ns"] is not None:
        endref = ep["authoritative_end_ns"]
    elif ep["end_candidates"]:
        endref = max(ep["end_candidates"])
    else:
        endref = max(ep["observation_event_times"])
    by_field = {}
    for s in inbounds:
        if not bc.is_valid_quality(s.get("quality")):
            continue
        num = bc.safe_float(s.get("value_num"))
        if num is None:
            continue
        by_field.setdefault(_field_label(s), []).append(s)
    rows = []
    used = []
    for field in sorted(by_field):
        samples = sorted(by_field[field], key=lambda s: s["event_time_ns"])
        buckets = {"before": [], "during": [], "after": []}
        for s in samples:
            t = s["event_time_ns"]
            if t < start:
                buckets["before"].append(s)
            elif t <= endref:
                buckets["during"].append(s)
            else:
                buckets["after"].append(s)
        for phase in PHASES:
            group = buckets[phase]
            if not group:
                continue
            nums = [float(s["value_num"]) for s in group]
            mean = sum(nums) / len(nums)
            if not bc.is_finite_number(mean):
                continue
            units = {s.get("unit") for s in group}
            unit = next(iter(units)) if len(units) == 1 else None
            times = [s["event_time_ns"] for s in group]
            raw_note = ("unit=unknown (values raw, unit unverified; "
                        "no physical claim)") if unit is None \
                else ("unit=%s" % unit)
            unverified = any(s.get("quality") == "unit_unverified"
                             for s in group)
            reason = ("field=%s phase=%s n=%d mean=%r min=%r max=%r "
                      "first_ns=%d last_ns=%d %s%s; invalid/unmeasurable "
                      "excluded; association not causation"
                      % (field, phase, len(group), mean, min(nums),
                         max(nums), min(times), max(times), raw_note,
                         "; some samples unit_unverified" if unverified else ""))
            rows.append(bc.make_result(
                METRIC_CONDITION, mean, unit, "derived", reason=reason,
                window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=len(group), sample_count=len(group),
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                value_text=ep["name"], episode_id=ep["episode_id"],
                analysis_id=_condition_analysis_id(
                    ep["episode_id"], field, phase),
                revision=revision))
            used.extend(_canon(s) for s in group)
    return rows, used


def analyze(signals, events, config):
    """Reduce OEM alert observations to lifecycle + recurrence rows."""
    config = config if isinstance(config, dict) else {}
    alert_cfg = _alert_cfg(config)
    table, dict_source, dict_version = _dictionary(alert_cfg)
    skew = _skew(alert_cfg)
    fields = _context_fields(alert_cfg)
    cov_start, cov_end = _coverage(alert_cfg)  # raises on malformed bounds
    ws, we = _window(config)  # raises on malformed/reversed bounds
    decision_time = _decision_time(config)  # raises on malformed bound

    adm_sig, exc_sig = [], {}
    for raw in signals if isinstance(signals, list) else []:
        s = bc.normalize_signal(raw)
        if s is None:
            continue
        status = _ingest_status(s, decision_time)
        if status == "admitted":
            adm_sig.append(s)
        else:
            key = (s["vehicle"], s["source"], s["decode_epoch"])
            slot = exc_sig.setdefault(key, [0, 0])
            slot[0 if status == "unknown" else 1] += 1
    adm_ev, exc_ev = [], {}
    for raw in events if isinstance(events, list) else []:
        e = bc.normalize_event(raw)
        if e is None:
            continue
        if e.get("event_type") != "alerts":
            continue  # errors/connectivity stay distinct, never close alerts
        status = _ingest_status(e, decision_time)
        if status == "admitted":
            adm_ev.append(e)
        else:
            key = (e["vehicle"], e["source"], e.get("decode_epoch"))
            slot = exc_ev.setdefault(key, [0, 0])
            slot[0 if status == "unknown" else 1] += 1
    # Lifecycle is built from retained history; window filtering applies to
    # boundaries/overlap below, never by dropping history first.
    history = dedup_events(adm_ev)

    by_scope = {}
    for e in history:
        by_scope.setdefault(
            (e["vehicle"], e["source"], e.get("decode_epoch")), []).append(e)
    sig_by_scope = {}
    for s in adm_sig:
        sig_by_scope.setdefault(
            (s["vehicle"], s["source"], s["decode_epoch"]), []).append(s)
    scope_hints = set(by_scope) | set(sig_by_scope) | set(exc_ev) | set(exc_sig)
    for raw in events if isinstance(events, list) else []:
        if isinstance(raw, dict):
            v = raw.get("vehicle")
            s = raw.get("source")
            e = raw.get("decode_epoch")
            if isinstance(v, str) and v and isinstance(s, str) and s:
                scope_hints.add((v, s, e if isinstance(e, str) and e else None))

    window_days = None
    if ws is not None and we is not None and we > ws:
        window_days = (we - ws) / 86400e9
    coverage_days = None
    if cov_start is not None:
        coverage_days = (cov_end - cov_start) / 86400e9

    rows = []
    for scope in sorted(scope_hints, key=_scope_sort):
        vehicle, source, epoch = scope
        obs = by_scope.get(scope, [])
        valid_groups, unknown = {}, []
        # Unknown-start warnings have no lifecycle span: only in-window
        # envelopes are shown (nothing pre-window to retain). Valid-start
        # history is always retained for boundary/overlap decisions.
        for e in obs:
            if bc.to_ns(e.get("started_ns")) is None:
                if _in_window(e["event_time_ns"], ws, we):
                    unknown.append(e)
            else:
                valid_groups.setdefault(
                    (e.get("name"), e.get("started_ns")), []).append(e)
        episodes = [reduce_episode(g) for g in valid_groups.values()]
        episodes.sort(key=lambda d: (d["name"] or "", d["started_ns"] or 0))
        overlapping = [ep for ep in episodes if _overlaps(ep, ws, we)]
        if not overlapping and not unknown:
            if obs:
                reason = "no_overlapping_alert_lifecycle_in_window"
            else:
                reason = "no_matched_alert_observations"
            reason += _exclusion_note(scope, exc_ev, exc_sig, decision_time)
            revision = bc.revision_id(
                "battery_alerts", ALGORITHM_VERSION, scope, ws, we,
                decision_time, cov_start, cov_end, alert_cfg,
                sorted(_canon(e) for e in obs),
                _exc_key(scope, exc_ev, exc_sig))
            for metric in SUPPORTED_SCALARS:
                unit = "1/d" if metric == METRIC_RECURRENCE_RATE else "count"
                rows.append(bc.make_result(
                    metric, None, unit, "unavailable", reason=reason,
                    window_start_ns=ws, window_end_ns=we,
                    vehicle=vehicle, source=source, decode_epoch=epoch,
                    evidence_count=len(obs), sample_count=0,
                    algorithm_version=ALGORITHM_VERSION,
                    calibration_version=dict_version,
                    analysis_id=ANALYSIS_ID, revision=revision))
            continue
        starts = sum(1 for ep in episodes
                     if _boundary_in(ep["started_ns"], ws, we))
        ends = sum(1 for ep in episodes
                   if ep["duration_s"] is not None
                   and _boundary_in(ep["authoritative_end_ns"], ws, we))
        active = sum(1 for ep in overlapping if ep["active"])
        conflicted = sum(1 for ep in overlapping if ep["conflicted"])
        by_name = {}
        for ep in episodes:
            by_name.setdefault(ep["name"], []).append(ep)
        recurrence = 0
        for group in by_name.values():
            ordered = sorted(group, key=lambda d: d["started_ns"] or 0)
            for ep in ordered[1:]:  # repeats beyond first known episode
                if _boundary_in(ep["started_ns"], ws, we):
                    recurrence += 1
        scope_signals = sorted(
            sig_by_scope.get(scope, []), key=lambda s: s["event_time_ns"])
        hist_canon = sorted(_canon(e) for e in obs)
        # Revision is per scope so influencing signals hash with their scope;
        # episodes are emitted first with a provisional revision, then the
        # used-signal canonicals are folded in via a second pass below.
        revision = bc.revision_id(
            "battery_alerts", ALGORITHM_VERSION, scope, ws, we,
            decision_time, cov_start, cov_end, alert_cfg, hist_canon,
            _exc_key(scope, exc_ev, exc_sig))
        used_canons = []
        ep_rows = []
        for ep in overlapping:
            if ep["conflicted"]:
                reason = ("conflicting_ends %s; no authoritative duration; "
                          "absence/disconnect never closes; "
                          % (ep["end_candidates"],))
            elif ep["duration_s"] is not None:
                reason = ("oem_reported start/end without requiring an observed "
                          "opening; late active envelope<=end is normal; ")
            elif ep["active"]:
                reason = ("active with valid active evidence; "
                          "absence/disconnect never closes; ")
            else:
                reason = ("open without valid active evidence "
                          "(missing/parse-failed observations); "
                          "absence/disconnect never closes; ")
            reason += _dict_reason(ep["name"], table, dict_source, dict_version)
            if ep["quality"] is not None:
                reason += "; observation_quality=%s%s" % (
                    ep["quality"], ep["quality_counts"])
            reason += _exclusion_note(scope, exc_ev, exc_sig, decision_time)
            reason += "; association only, no causal claim"
            ep_rows.append(bc.make_result(
                METRIC_EPISODE, ep["duration_s"],
                "s" if ep["duration_s"] is not None else None, "reported",
                reason=reason, window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=ep["observation_count"],
                sample_count=ep["observation_count"],
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                value_text=ep["name"], quality=ep["quality"],
                episode_id=ep["episode_id"],
                analysis_id=_episode_analysis_id(ep["episode_id"]),
                revision=revision))
            # Bounded surrounding signal context (association, not causation).
            lo = ep["started_ns"] - skew
            if ep["authoritative_end_ns"] is not None:
                endref = ep["authoritative_end_ns"]
            elif ep["end_candidates"]:
                endref = max(ep["end_candidates"])
            else:
                endref = max(ep["observation_event_times"])
            hi = endref + skew
            inbounds = [s for s in scope_signals
                        if lo <= s["event_time_ns"] <= hi
                        and (fields is None
                             or s.get("source_field") in fields)]
            used_canons.extend(_canon(s) for s in inbounds)
            ep_rows.append(bc.make_result(
                METRIC_CONTEXT, float(len(inbounds)), "count", "derived",
                reason=("associated %d surrounding signals within bounded "
                        "skew; association not causation" % len(inbounds)),
                window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=len(inbounds), sample_count=len(inbounds),
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                value_text=ep["name"], episode_id=ep["episode_id"],
                analysis_id=_context_analysis_id(ep["episode_id"]),
                revision=revision))
        revision = bc.revision_id(
            "battery_alerts", ALGORITHM_VERSION, scope, ws, we,
            decision_time, cov_start, cov_end, alert_cfg, hist_canon,
            sorted(used_canons), _exc_key(scope, exc_ev, exc_sig))
        for row in ep_rows:
            row["revision"] = revision
        rows.extend(ep_rows)
        for ep in overlapping:
            lo = ep["started_ns"] - skew
            if ep["authoritative_end_ns"] is not None:
                endref = ep["authoritative_end_ns"]
            elif ep["end_candidates"]:
                endref = max(ep["end_candidates"])
            else:
                endref = max(ep["observation_event_times"])
            hi = endref + skew
            inbounds = [s for s in scope_signals
                        if lo <= s["event_time_ns"] <= hi
                        and (fields is None
                             or s.get("source_field") in fields)]
            cond, _ = _condition_rows(
                ep, inbounds, ws, we, vehicle, source, epoch,
                dict_version, revision)
            rows.extend(cond)
        for e in sorted(unknown, key=lambda x: (x.get("event_time_ns") or 0,
                                                x.get("name") or "",
                                                x.get("event_id") or "")):
            reason = ("unknown_start: warning preserved without duration or "
                      "recurrence; ")
            reason += _dict_reason(e.get("name"), table,
                                   dict_source, dict_version)
            reason += _exclusion_note(scope, exc_ev, exc_sig, decision_time)
            rows.append(bc.make_result(
                METRIC_EPISODE, None, None, "reported", reason=reason,
                window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=1, sample_count=1,
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                value_text=e.get("name"), quality=e.get("quality"),
                config_version=e.get("config_version"),
                connectivity=e.get("connectivity"),
                episode_id=None, analysis_id=_unknown_analysis_id(e),
                revision=revision))
        scalars = (
            (METRIC_STARTS, float(starts),
             "distinct valid-start episodes with start boundary in window"),
            (METRIC_ENDS, float(ends),
             "episodes with authoritative OEM end boundary in window"),
            (METRIC_ACTIVE, float(active),
             "overlapping open episodes with valid active evidence"),
            (METRIC_CONFLICTED, float(conflicted),
             "overlapping episodes with conflicting ends"),
            (METRIC_RECURRENCE, float(recurrence),
             "repeat episodes with start in window beyond first known "
             "episode per name; re-observation excluded"),
        )
        for metric, val, why in scalars:
            why += _exclusion_note(scope, exc_ev, exc_sig, decision_time)
            rows.append(bc.make_result(
                metric, val, "count", "derived", reason=why,
                window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=len(obs), sample_count=starts,
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                analysis_id=ANALYSIS_ID, revision=revision))
        if coverage_days is not None:
            rate = recurrence / coverage_days
            rows.append(bc.make_result(
                METRIC_RECURRENCE_RATE, rate, "1/d",
                reason=("exposure-adjusted recurrence over caller-supplied "
                        "bounded coverage exposure; observed signal coverage "
                        "still unknown (change-gated telemetry, never "
                        "guaranteed)"),
                status="derived", window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=len(obs), sample_count=starts,
                coverage_ratio=None,
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                analysis_id=ANALYSIS_ID, revision=revision))
        elif window_days:
            rate = recurrence / window_days
            rows.append(bc.make_result(
                METRIC_RECURRENCE_RATE, rate, "1/d",
                reason=("observed calendar rate over matched window days; "
                        "coverage unknown (change-gated telemetry, no "
                        "guaranteed coverage)"),
                status="derived", window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=len(obs), sample_count=starts,
                coverage_ratio=None,
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                analysis_id=ANALYSIS_ID, revision=revision))
        else:
            reason = ("recurrence rate needs a positive window duration or "
                      "explicit bounded coverage exposure")
            rows.append(bc.make_result(
                METRIC_RECURRENCE_RATE, None, None, "unavailable",
                reason=reason,
                window_start_ns=ws, window_end_ns=we,
                vehicle=vehicle, source=source, decode_epoch=epoch,
                evidence_count=len(obs), sample_count=starts,
                algorithm_version=ALGORITHM_VERSION,
                calibration_version=dict_version,
                analysis_id=ANALYSIS_ID, revision=revision))
    return rows


def _exclusion_note(scope, exc_ev, exc_sig, decision_time):
    if decision_time is None:
        return ""
    ev_u, ev_l = exc_ev.get(scope, (0, 0))
    sg_u, sg_l = exc_sig.get(scope, (0, 0))
    total_u, total_l = ev_u + sg_u, ev_l + sg_l
    if not total_u and not total_l:
        return ""
    return ("; excluded %d unknown-ingest (availability unprovable at "
            "decision_time) + %d late-ingest observations"
            % (total_u, total_l))


def _exc_key(scope, exc_ev, exc_sig):
    return [exc_ev.get(scope, (0, 0)), exc_sig.get(scope, (0, 0))]
