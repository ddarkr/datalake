#!/usr/bin/env python3
"""Tests for scripts.analytics.battery.battery_alerts. Plain asserts, stdlib only; __main__ runner."""

import os
import sys

from scripts.analytics.battery import battery_alerts as al
from scripts.analytics.battery import battery_common as bc

MANUAL = "https://www.tesla.com/ownersmanual/model3/en_us/Owners_Manual.pdf"


def ev(t, name="bms_a035", started=1_000_000_000, ended=None, active=None,
       vehicle="v", source="fleet", epoch="fleet-v1", eid=None, ingest=None,
       etype="alerts", audience=None, quality=None):
    row = {"event_time_ns": t, "vehicle": vehicle, "event_type": etype,
           "name": name, "source": source, "decode_epoch": epoch,
           "started_ns": started, "ended_ns": ended, "is_active": active,
           "audience": audience}
    if eid is not None:
        row["event_id"] = eid
    if ingest is not None:
        row["ingest_time_ns"] = ingest
    if quality is not None:
        row["quality"] = quality
    return {k: v for k, v in row.items() if v is not None}


def sig(t, field="Soc", num=55.0, vehicle="v", source="fleet",
        epoch="fleet-v1", ingest=None, unit="%", quality=None, path="P"):
    row = {"event_time_ns": t, "vehicle": vehicle, "source": source,
           "decode_epoch": epoch, "path": path, "source_field": field,
           "value_num": num, "unit": unit}
    if ingest is not None:
        row["ingest_time_ns"] = ingest
    if quality is not None:
        row["quality"] = quality
    return {k: v for k, v in row.items() if v is not None}


def cfg(**kw):
    base = {"alerts": {"warning_dictionary": {"bms_a035": "caller-supplied text"},
                       "dictionary_source": MANUAL,
                       "dictionary_version": "2026-09-28"}}
    base["alerts"].update(kw.pop("alerts", {}))
    base.update(kw)
    return base


def rows_by(rows, metric):
    return [r for r in rows if r["metric"] == metric]


def test_shuffle_replay_same_result():
    obs = [ev(100, ended=None, active=True, eid="a1"),
           ev(200, ended=2_000_000_000, active=False, eid="a2"),
           ev(150, ended=None, active=True, eid="a3")]
    fwd = al.analyze([], obs, cfg())
    rev = al.analyze([], list(reversed(obs)), cfg())
    assert fwd == rev  # order-independent
    ep = [r for r in fwd if r["metric"] == "battery.alerts.episode"][0]
    assert ep["value"] == (2_000_000_000 - 1_000_000_000) / 1e9
    assert ep["status"] == "reported"
    assert ep["value_text"] == "bms_a035"
    assert ep["episode_id"] == bc.episode_key("v", "alerts", "bms_a035",
                                              1_000_000_000, "fleet-v1")
    assert ep["unit"] == "s"


def test_malformed_end_never_implies_active():
    got = al.reduce_episode([{"event_time_ns": 200, "vehicle": "v",
                              "event_type": "alerts", "name": "bms_a035",
                              "source": "fleet", "decode_epoch": "fleet-v1",
                              "started_ns": 100, "ended_ns": None,
                              "is_active": None, "quality": "error",
                              "event_id": "m1"}])
    assert got["active"] is False
    assert got["active_evidence"] is False
    assert got["quality"] == "error"
    assert got["duration_s"] is None
    rows = al.analyze([], [ev(200, started=100, ended=None, active=None,
                              eid="m1", quality="error")], cfg())
    assert rows_by(rows, "battery.alerts.active")[0]["value"] == 0.0
    ep = rows_by(rows, "battery.alerts.episode")[0]
    assert ep["quality"] == "error"
    assert "without valid active evidence" in (ep["reason"] or "")


def test_mixed_quality_exposed_as_conflicting():
    rows = al.analyze([], [ev(100, started=100, eid="q1", quality="error"),
                           ev(150, started=100, active=True, eid="q2")],
                      cfg())
    ep = rows_by(rows, "battery.alerts.episode")[0]
    assert ep["quality"] == "conflicting"
    assert rows_by(rows, "battery.alerts.active")[0]["value"] == 1.0


def test_conflicting_ends_no_authoritative_duration():
    obs = [ev(100, ended=2_000_000_000, active=False, eid="e1"),
           ev(150, ended=3_000_000_000, active=False, eid="e2")]
    norm = [bc.normalize_event(o) for o in obs]
    red = al.reduce_episode(al.dedup_events(norm))
    assert red["conflicted"] is True
    assert red["duration_s"] is None
    assert red["end_candidates"] == [2_000_000_000, 3_000_000_000]
    rows = al.analyze([], obs, cfg())
    ep = rows_by(rows, "battery.alerts.episode")[0]
    assert ep["value"] is None and ep["unit"] is None
    assert "conflicting_ends" in (ep["reason"] or "")
    assert rows_by(rows, "battery.alerts.conflicted")[0]["value"] == 1.0
    assert rows_by(rows, "battery.alerts.ends")[0]["value"] == 0.0


def test_window_counts_boundaries_not_envelopes():
    # Envelope at 200 (in 150..250) but the start boundary (100) is outside.
    rows = al.analyze([], [ev(200, started=100, eid="w1")],
                      cfg(window_start_ns=150, window_end_ns=250))
    assert rows_by(rows, "battery.alerts.starts")[0]["value"] == 0.0
    # The open episode still overlaps, so a snapshot is retained.
    eps = rows_by(rows, "battery.alerts.episode")
    assert len(eps) == 1 and eps[0]["value"] is None
    # Pre-window envelopes are retained for an overlapping lifecycle: an
    # end observation seen pre-window still closes inside the window.
    rows2 = al.analyze(
        [], [ev(100, started=100, ended=200, active=False, eid="h2")],
        cfg(window_start_ns=150, window_end_ns=250))
    assert rows_by(rows2, "battery.alerts.starts")[0]["value"] == 0.0
    assert rows_by(rows2, "battery.alerts.ends")[0]["value"] == 1.0
    eps2 = rows_by(rows2, "battery.alerts.episode")
    assert len(eps2) == 1
    assert eps2[0]["value"] == (200 - 100) / 1e9
    # Fully pre-window closed episode: no overlap, no snapshot.
    rows3 = al.analyze(
        [], [ev(50, started=100, ended=120, active=False, eid="h3")],
        cfg(window_start_ns=150, window_end_ns=250))
    assert rows3[0]["status"] == "unavailable"


def test_historic_active_normal_later_active_conflict():
    # Late active envelope (1500) <= end (2000): normal, duration kept.
    obs = [ev(2_500, started=1_000, ended=2_000, active=False, eid="c1"),
           ev(1_500, started=1_000, ended=None, active=True, eid="c2")]
    red = al.reduce_episode(al.dedup_events([bc.normalize_event(o) for o in obs]))
    assert red["conflicted"] is False
    assert red["duration_s"] == (2_000 - 1_000) / 1e9
    # Later active envelope (2500) > end (2000): conflict.
    obs2 = [ev(900, started=1_000, ended=2_000, active=False, eid="d1"),
            ev(2_500, started=1_000, ended=None, active=True, eid="d2")]
    red2 = al.reduce_episode(al.dedup_events([bc.normalize_event(o) for o in obs2]))
    assert red2["conflicted"] is True
    assert red2["duration_s"] is None
    assert red2["end_candidates"] == [2_000]


def test_unknown_start_preserved_excluded():
    obs = [{"event_time_ns": 100, "vehicle": "v", "event_type": "alerts",
            "name": "bms_a035", "source": "fleet", "decode_epoch": "fleet-v1",
            "event_id": "u1"}]  # no started_ns
    rows = al.analyze([], obs, cfg())
    eps = rows_by(rows, "battery.alerts.episode")
    assert len(eps) == 1
    assert eps[0]["episode_id"] is None and eps[0]["value"] is None
    assert "unknown_start" in (eps[0]["reason"] or "")
    assert eps[0]["analysis_id"].startswith("battery_alerts:unknown:")
    assert rows_by(rows, "battery.alerts.starts")[0]["value"] == 0.0
    assert rows_by(rows, "battery.alerts.recurrence")[0]["value"] == 0.0


def test_unknown_start_pk_identity_distinct():
    obs = [{"event_time_ns": 100, "vehicle": "v", "event_type": "alerts",
            "name": "bms_a035", "source": "fleet", "decode_epoch": "fleet-v1",
            "event_id": "u1"},
           {"event_time_ns": 120, "vehicle": "v", "event_type": "alerts",
            "name": "bms_a035", "source": "fleet", "decode_epoch": "fleet-v1",
            "event_id": "u2"}]
    rows = al.analyze([], obs, cfg())
    eps = rows_by(rows, "battery.alerts.episode")
    assert len(eps) == 2
    ids = [r["analysis_id"] for r in eps]
    assert len(set(ids)) == 2  # no PK overwrite
    assert all(i.startswith("battery_alerts:unknown:") for i in ids)


def test_episode_pk_identity_distinct_same_name():
    obs = [ev(100, started=1_000, ended=2_000, active=False, eid="p1"),
           ev(150, started=5_000, ended=6_000, active=False, eid="p2")]
    rows = al.analyze([], obs, cfg())
    eps = sorted(rows_by(rows, "battery.alerts.episode"),
                 key=lambda r: r["analysis_id"])
    assert len(eps) == 2
    assert eps[0]["analysis_id"] != eps[1]["analysis_id"]
    assert all(r["analysis_id"].startswith("battery_alerts:episode:")
               for r in eps)
    ctx = rows_by(rows, "battery.alerts.context")
    assert len({r["analysis_id"] for r in ctx}) == 2


def test_disconnect_and_errors_never_close():
    obs = [ev(100, ended=None, active=True, eid="o1"),
           {"event_time_ns": 150, "vehicle": "v", "event_type": "connectivity",
            "name": "connectivity", "source": "fleet", "decode_epoch": "fleet-v1",
            "connectivity": "DISCONNECTED", "is_active": False,
            "event_id": "conn1"},
           {"event_time_ns": 160, "vehicle": "v", "event_type": "errors",
            "name": "PCS_a019", "source": "fleet", "decode_epoch": "fleet-v1",
            "event_id": "err1"}]
    rows = al.analyze([], obs, cfg())
    assert rows_by(rows, "battery.alerts.active")[0]["value"] == 1.0
    assert rows_by(rows, "battery.alerts.ends")[0]["value"] == 0.0
    assert all(r["metric"].startswith("battery.alerts.") for r in rows)
    assert not [r for r in rows if (r["value_text"] or "") in ("PCS_a019", "connectivity")
                and r["metric"] == "battery.alerts.episode"]


def test_cross_scope_never_merges():
    obs = [ev(100, started=1_000, eid="s1", vehicle="v1"),
           ev(100, started=1_000, eid="s2", vehicle="v2"),
           ev(100, started=1_000, eid="s3", vehicle="v1", epoch="other-epoch")]
    rows = al.analyze([], obs, cfg())
    assert len(rows_by(rows, "battery.alerts.episode")) == 3
    assert len({r["episode_id"] for r in rows_by(rows, "battery.alerts.episode")}) == 3
    assert len(rows_by(rows, "battery.alerts.starts")) == 3


def test_scoped_dedup_never_merges_cross_scope():
    a = bc.normalize_event(ev(100, started=1_000, eid="same"))
    b = bc.normalize_event(ev(100, started=1_000, eid="same", vehicle="other"))
    assert len(al.dedup_events([a, b])) == 2
    c = bc.normalize_event(ev(100, started=1_000, eid="same", source="can"))
    assert len(al.dedup_events([a, c])) == 2


def test_recurrence_denominator_same_window_and_no_reobservation_inflation():
    ws, we = 0 + 1, 10 * 86400_000_000_000  # 10 days in ns
    in1 = ev(100, started=1_000, ended=2_000, active=False, eid="r1")
    in2 = ev(200, started=5_000, ended=6_000, active=False, eid="r2")
    dup = ev(200, started=5_000, ended=6_000, active=False, eid="r2")  # retransmit
    outside = ev(we + 1, started=we + 5, ended=we + 10, active=False,
                 eid="r3")
    rows = al.analyze([], [in1, in2, dup, outside],
                      cfg(window_start_ns=ws, window_end_ns=we))
    assert rows_by(rows, "battery.alerts.starts")[0]["value"] == 2.0
    assert rows_by(rows, "battery.alerts.recurrence")[0]["value"] == 1.0
    rate = rows_by(rows, "battery.alerts.recurrence_rate")[0]
    assert rate["status"] == "derived"
    assert abs(rate["value"] - 0.1) < 1e-12  # 1 repeat / 10 days, same window
    assert rate["unit"] == "1/d"
    assert "coverage unknown" in (rate["reason"] or "")


def test_recurrence_excludes_starts_outside_rate_window():
    ws, we = 1_000, 1_000 + 10 * 86400_000_000_000
    old = ev(100, started=100, ended=200, active=False, eid="old1")
    new = ev(we - 10, started=ws + 5, ended=ws + 500, active=False, eid="new1")
    rows = al.analyze([], [old, new],
                      cfg(window_start_ns=ws, window_end_ns=we))
    assert rows_by(rows, "battery.alerts.starts")[0]["value"] == 1.0
    # Only the in-window repeat counts, and the old episode anchors history.
    assert rows_by(rows, "battery.alerts.recurrence")[0]["value"] == 1.0
    rate = rows_by(rows, "battery.alerts.recurrence_rate")[0]
    assert abs(rate["value"] - 0.1) < 1e-12


def test_recurrence_rate_explicit_coverage_exposure():
    ws, we = 0 + 1, 10 * 86400_000_000_000
    cs, ce = 0 + 1, 0 + 1 + 5 * 86400_000_000_000  # 5-day asserted exposure
    obs = [ev(100, started=1_000, ended=2_000, active=False, eid="g1"),
           ev(200, started=5_000, ended=6_000, active=False, eid="g2")]
    rows = al.analyze([], obs, cfg(window_start_ns=ws, window_end_ns=we,
                                   alerts={"coverage_start_ns": cs,
                                           "coverage_end_ns": ce}))
    rate = rows_by(rows, "battery.alerts.recurrence_rate")[0]
    assert abs(rate["value"] - 0.2) < 1e-12  # 1 repeat / 5 exposure days
    assert "exposure-adjusted" in (rate["reason"] or "")


def test_dictionary_unknown_fallback_no_severity():
    obs = [ev(100, name="bms_zzz_unknown", started=1_000, eid="k1")]
    rows = al.analyze([], obs, cfg())
    ep = rows_by(rows, "battery.alerts.episode")[0]
    assert "unknown_name" in (ep["reason"] or "")
    assert "severity" not in (ep["reason"] or "").lower()
    assert "diagnos" not in (ep["reason"] or "").lower()
    known = al.analyze([], [ev(100, started=1_000, eid="k2")], cfg())
    assert "caller-supplied text" in (rows_by(known, "battery.alerts.episode")[0]["reason"] or "")
    assert "severity" not in (rows_by(known, "battery.alerts.episode")[0]["reason"] or "").lower()


def test_context_future_ingest_excluded_and_conditions():
    ep_obs = [ev(1_500, started=1_000, ended=2_000, active=False, eid="x1",
                 ingest=500)]
    near = sig(1_500, ingest=500)
    future = sig(1_600, ingest=9_999)
    rows = al.analyze([near, future], ep_obs,
                      cfg(decision_time_ns=1_000,
                          alerts={"context_skew_ns": 10_000}))
    ctx = rows_by(rows, "battery.alerts.context")[0]
    assert ctx["value"] == 1.0
    assert "association not causation" in (ctx["reason"] or "")
    conds = rows_by(rows, "battery.alerts.condition")
    assert len(conds) == 1
    cond = conds[0]
    assert cond["value"] == 55.0 and cond["unit"] == "%"
    assert "phase=during" in (cond["reason"] or "")
    assert "first_ns=1500" in (cond["reason"] or "")
    assert cond["analysis_id"].startswith("battery_alerts:condition:")
    # Revision folds influencing signals in: dropping the signal changes it.
    rev_with = ctx["revision"]
    rev_without = rows_by(al.analyze([], ep_obs,
                                     cfg(decision_time_ns=1_000,
                                         alerts={"context_skew_ns": 10_000})),
                          "battery.alerts.context")[0]["revision"]
    assert rev_with != rev_without
    # Late-ingest observation itself excluded from the episode.
    late_obs = [ev(1_500, started=1_000, ended=2_000, active=False,
                   eid="x2", ingest=9_999)]
    rows2 = al.analyze([], late_obs, cfg(decision_time_ns=1_000))
    assert rows2[0]["status"] == "unavailable"
    assert "late-ingest" in (rows2[0]["reason"] or "")


def test_context_unknown_ingest_excluded_online_not_offline():
    ep_obs = [ev(1_500, started=1_000, ended=2_000, active=False, eid="y1")]
    rows = al.analyze([], ep_obs, cfg())  # offline: admitted
    assert rows_by(rows, "battery.alerts.starts")[0]["value"] == 1.0
    rows2 = al.analyze([], ep_obs, cfg(decision_time_ns=2_000))
    assert rows2[0]["status"] == "unavailable"
    assert "unknown-ingest" in (rows2[0]["reason"] or "")


def test_condition_phases_units_invalid_excluded():
    cfg10 = cfg(alerts={"context_skew_ns": 10_000})
    ep_obs = [ev(5_000, started=5_000, ended=6_000, active=False, eid="z1")]
    signals = [sig(1_000, num=10.0),  # before
               sig(5_500, num=20.0), sig(5_800, num=30.0),  # during
               sig(9_000, num=40.0),  # after
               dict(sig(5_600, num=999.0), quality="invalid"),  # excluded
               sig(5_700, field="PackVoltage", num=350.0, unit=None)]
    rows = al.analyze(signals, ep_obs, cfg10)
    conds = sorted(rows_by(rows, "battery.alerts.condition"),
                   key=lambda r: (r["value_text"] or "",
                                  r["analysis_id"]))
    soc = [c for c in conds if "field=Soc" in (c["reason"] or "")]
    assert [c["value"] for c in soc
            if "phase=before" in (c["reason"] or "")] == [10.0]
    assert [c["value"] for c in soc
            if "phase=during" in (c["reason"] or "")] == [25.0]
    assert [c["value"] for c in soc
            if "phase=after" in (c["reason"] or "")] == [40.0]
    raw = [c for c in conds if "field=PackVoltage" in (c["reason"] or "")]
    assert len(raw) == 1 and raw[0]["unit"] is None
    assert "no physical claim" in (raw[0]["reason"] or "")
    assert 999.0 not in [c["value"] for c in conds]


def test_explicit_start_end_without_opening_reports_oem_duration():
    obs = [ev(5_000, started=1_000, ended=4_000, active=False, eid="w1")]
    red = al.reduce_episode(al.dedup_events([bc.normalize_event(o) for o in obs]))
    assert red["duration_s"] == 3_000 / 1e9
    assert red["has_active_observation"] is False
    rows = al.analyze([], obs, cfg())
    ep = rows_by(rows, "battery.alerts.episode")[0]
    assert ep["status"] == "reported" and ep["value"] == 3_000 / 1e9


def test_episode_text_scalar_rows_dashboard_safe():
    rows = al.analyze([], [ev(100, started=1_000, ended=2_000,
                              active=False, eid="v1")], cfg())
    for r in rows:
        assert r["metric"].startswith("battery.alerts.")
        assert r["analysis_id"].startswith("battery_alerts")
        assert r["revision"]
    eps = rows_by(rows, "battery.alerts.episode")
    assert eps[0]["value_text"] == "bms_a035" and eps[0]["episode_id"]
    ctx = rows_by(rows, "battery.alerts.context")[0]
    assert ctx["value_text"] == "bms_a035" and ctx["episode_id"] == eps[0]["episode_id"]


def test_malformed_time_bounds_raise():
    # Trust boundary: a PRESENT malformed time bound must raise (surfaced
    # by run_analyses as an isolated error row), never silently fall back
    # to offline/unbounded/calendar calculations that admit future evidence.
    for bad in (cfg(decision_time_ns="bad"),
                cfg(window_start_ns="bad"),
                cfg(window_end_ns="bad"),
                cfg(window_start_ns=250, window_end_ns=150),
                cfg(alerts={"coverage_start_ns": 200}),
                cfg(alerts={"coverage_start_ns": "bad",
                            "coverage_end_ns": 300}),
                cfg(alerts={"coverage_start_ns": 300,
                            "coverage_end_ns": 200})):
        try:
            al.analyze([], [ev(100, started=100, eid="f1")], bad)
        except ValueError:
            continue
        raise AssertionError("malformed time bound must raise: %r" % (bad,))
    # Absent/None still means offline/unbounded (not an error).
    rows = al.analyze([], [ev(100, started=100, eid="f1")], cfg())
    assert rows_by(rows, "battery.alerts.starts")[0]["value"] == 1.0


def test_invalid_event_timestamp_dropped():
    bad = dict(ev(100, started=100, eid="g1"), event_time_ns="x")
    rows = al.analyze([], [bad], cfg())
    assert rows and rows[0]["status"] == "unavailable"


if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_alerts: ok (%d tests)" % len(names))
