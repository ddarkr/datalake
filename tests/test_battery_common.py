"""Regression tests for scripts.analytics.battery.battery_common and the battery schema
in scripts.database.db_init. Plain asserts, stdlib only; doubles as __main__ runner.
"""

import os
import sys

from scripts.analytics.battery import battery_common as bc
from scripts.database import db_init


def sig(ts, vehicle="v", source="fleet", epoch="e1", path="P",
        field="F", num=1.0, quality=None, unit="km/h", envelope=None,
        ingest=None, config=None, connectivity=None):
    return {"event_time_ns": ts, "ingest_time_ns": ingest, "vehicle": vehicle,
            "source": source, "decode_epoch": epoch, "path": path,
            "source_field": field, "value_num": num, "value_text": None,
            "value_bool": None, "unit": unit, "quality": quality,
            "envelope_id": envelope, "config_version": config,
            "connectivity": connectivity}


def test_ns_rejects_float_bool_zero_overflow():
    assert bc.to_ns(1_700_000_000_000_000_000) == 1_700_000_000_000_000_000
    assert bc.to_ns(1_700_000_000_000_000_000.0) is None  # float loses ns
    assert bc.to_ns(True) is None
    assert bc.to_ns(0) is None
    assert bc.to_ns(-5) is None
    assert bc.to_ns(bc.INT64_MAX) == bc.INT64_MAX
    assert bc.to_ns(bc.INT64_MAX + 1) is None
    assert bc.to_ns("123") is None


def test_numeric_strict_finite():
    assert bc.is_finite_number(3) and bc.is_finite_number(3.5)
    assert not bc.is_finite_number(True)
    assert not bc.is_finite_number(float("nan"))
    assert not bc.is_finite_number(float("inf"))
    assert not bc.is_finite_number("3.5")  # never coerce strings
    s = bc.normalize_signal(sig(100, num=float("nan")))
    assert s["value_num"] is None  # unmeasurable, never zero
    assert bc.normalize_signal(sig(0)) is None  # bad time, no row at all
    assert bc.normalize_signal(sig(100, num="x"))["value_num"] is None


def test_huge_int_converts_to_none_without_crash():
    assert bc.safe_float(10 ** 1000) is None  # OverflowError, never raised
    assert bc.safe_float(True) is None
    assert bc.safe_float("3.5") is None
    assert bc.safe_float(2.5) == 2.5
    s = bc.normalize_signal(sig(100, num=10 ** 1000))
    assert s["value_num"] is None
    # counter endpoints that overflow float are rejected, not raised
    assert bc.counter_delta_ns([(1, 10), (2, 10 ** 1000)]) is None


def test_identity_fields_must_be_nonempty_strings():
    base = {"event_time_ns": 100, "vehicle": "v", "source": "fleet",
            "decode_epoch": "e1", "value_num": 1.0}
    assert bc.normalize_signal(dict(base, vehicle=123)) is None
    assert bc.normalize_signal(dict(base, source=True)) is None
    assert bc.normalize_signal(dict(base, decode_epoch="")) is None
    ev = {"event_time_ns": 100, "vehicle": "v", "event_type": "alerts",
          "name": "n", "source": "s"}
    assert bc.normalize_event(dict(ev, vehicle=7)) is None
    assert bc.normalize_event(dict(ev, name=["n"])) is None


def test_old_signals_keep_missing_provenance():
    s = bc.normalize_signal({"event_time_ns": 100, "vehicle": "v",
                             "source": "fleet", "decode_epoch": "e1",
                             "value_num": 2.0})
    assert s["quality"] is None and s["envelope_id"] is None
    assert s["config_version"] is None and s["connectivity"] is None
    assert s["value_num"] == 2.0


def test_unit_unverified_keeps_raw_numeric():
    # undocumented unit: raw numeric persists (unit NULL, never zero-filled)
    # and carries through segments; analyzers gate on known units, not here
    s = bc.normalize_signal({"event_time_ns": 100, "vehicle": "v",
                             "source": "fleet", "decode_epoch": "e1",
                             "value_num": 3.25, "quality": "unit_unverified"})
    assert s["value_num"] == 3.25 and s["quality"] == "unit_unverified"
    segs = bc.split_on_invalid([s])
    assert len(segs) == 1  # measurable, does not stop carry


def test_range_rejected_stops_segment_keeps_provenance():
    rows = [sig(1, num=1.0), sig(2, num=None, quality="range_rejected"),
            sig(3, num=4.0)]
    segs = bc.split_on_invalid(rows)
    assert [[r["event_time_ns"] for r in s] for s in segs] == [[1], [3]]
    assert rows[1]["quality"] == "range_rejected"  # semantics preserved


def test_dedup_keeps_same_time_different_payload():
    a = sig(100, num=1.0)
    b = sig(100, num=2.0)  # same ts, different payload: distinct samples
    c = dict(a)
    assert len(bc.sort_dedup([b, a, c])) == 2
    assert [r["value_num"] for r in bc.sort_dedup([b, a])] == [1.0, 2.0]


def test_dedup_key_preserves_quality_unit_envelope():
    a = sig(100, num=1.0, quality=None)
    b = sig(100, num=1.0, quality="invalid")  # tombstone is not the sample
    c = sig(100, num=1.0, unit="mph")
    d = sig(100, num=1.0, envelope="env-9")
    assert len(bc.sort_dedup([a, b, c, d])) == 4


def test_dedup_identical_retransmit_keeps_earliest_ingest():
    early = sig(100, num=1.0, envelope="e", ingest=50)
    late = sig(100, num=1.0, envelope="e", ingest=90)
    for order in ([early, late], [late, early]):
        got = bc.sort_dedup(order)
        assert len(got) == 1
        assert got[0]["ingest_time_ns"] == 50  # order-independent


def test_dedup_cheap_collision_keeps_provenance_picks_earliest_ingest():
    early = sig(100, num=1.0, envelope="e", ingest=50)
    late = dict(early, ingest_time_ns=90)
    unknown = dict(early, ingest_time_ns=None)
    tombstone = dict(early, quality="invalid", ingest_time_ns=10)
    other_unit = dict(early, unit="mph", ingest_time_ns=20)
    other_env = dict(early, ingest_time_ns=30)
    other_env["envelope_id"] = "env-9"
    rows = [unknown, other_env, late, tombstone, other_unit, early]
    got = bc.sort_dedup(rows)
    assert len(got) == 4  # quality/unit/envelope variants stay distinct
    kept = [r for r in got if r["quality"] is None and r["unit"] == "km/h"
            and r["envelope_id"] == "e"]
    assert len(kept) == 1 and kept[0]["ingest_time_ns"] == 50
    assert got == bc.sort_dedup(list(reversed(rows)))  # order-independent


def test_dedup_stable_observation_set_stable_result():
    rows = [sig(100, num=1.0), sig(100, num=2.0), sig(90, num=0.5)]
    assert bc.sort_dedup(rows) == bc.sort_dedup(list(reversed(rows)))


def test_scopes_never_merge():
    rows = [sig(100, source="fleet"), sig(100, source="can"),
            sig(100, source="fleet", epoch="e2")]
    assert len(bc.group_by_scope(rows)) == 3


def test_invalid_stops_join_without_skip_back():
    old = sig(0, num=9.0)
    bad = sig(90, num=50.0, quality="invalid")
    prob = sig(100, num=1.0)
    got = bc.join_asof([prob], [old, bad], 1000)
    assert got[0][1] is None  # invalid at 90 stops; never falls back to t=0
    good = sig(95, num=7.0)
    got = bc.join_asof([prob], [old, good], 1000)
    assert got[0][1]["value_num"] == 7.0
    stale = sig(10, num=7.0)
    assert bc.join_asof([prob], [stale], 50)[0][1] is None  # too old
    try:
        bc.join_asof([prob], [good], -1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative skew must raise")


def test_join_rejects_cross_scope():
    prob = sig(100, num=1.0)
    other_source = sig(95, num=7.0, source="can")
    other_epoch = sig(95, num=7.0, epoch="e2")
    assert bc.join_asof([prob], [other_source], 1000)[0][1] is None
    assert bc.join_asof([prob], [other_epoch], 1000)[0][1] is None


def test_join_ambiguous_same_time_yields_none():
    prob = sig(100, num=1.0)
    rival_a = sig(95, num=7.0)
    rival_b = sig(95, num=8.0)  # contradictory same-time values
    assert bc.join_asof([prob], [rival_a, rival_b], 1000)[0][1] is None
    tomb = sig(95, num=None, quality="invalid")
    assert bc.join_asof([prob], [rival_a, tomb], 1000)[0][1] is None


def test_join_invalid_primary_never_matches():
    prob = sig(100, num=None, quality="invalid")
    good = sig(95, num=7.0)
    assert bc.join_asof([prob], [good], 1000)[0][1] is None


def test_split_on_invalid_breaks_segments():
    rows = bc.sort_dedup([sig(1, num=1.0), sig(2, num=float("nan")),
                          sig(3, num=3.0), sig(4, num=4.0)])
    # NaN normalized to None, which splits; the two valid runs stay separate
    normed = [bc.normalize_signal(r) for r in rows]
    segs = bc.split_on_invalid(normed)
    assert [[r["event_time_ns"] for r in s] for s in segs] == [[1], [3, 4]]


def test_counter_delta_rejects_reset_and_gaps():
    assert bc.counter_delta_ns([(1, 10.0), (2, 12.5)]) == 2.5
    assert bc.counter_delta_ns([(1, 10.0)]) is None  # sparse, never zero
    assert bc.counter_delta_ns([(1, 10.0), (2, 9.0)]) is None  # reset/wrap
    assert bc.counter_delta_ns([(1, 10.0), (2, None)]) is None  # gap unfilled
    assert bc.counter_delta_ns([(2, 10.0), (1, 12.0)]) is None  # unordered


def test_trap_integral_requires_gap_and_reports_coverage():
    # 1.0 kW for one hour -> 1 kWh; invalid middle sample breaks both legs
    t0 = 1_700_000_000_000_000_000
    hour = 3_600_000_000_000
    full = bc.trap_integral_ns([(t0, 1.0), (t0 + hour, 1.0)], hour)
    assert full["value"] == 1.0
    assert (full["legs_used"], full["legs_rejected"]) == (1, 0)
    assert full["coverage_ratio"] == 1.0 and full["span_ns"] == hour
    try:
        bc.trap_integral_ns([(t0, 1.0), (t0 + hour, 1.0)])  # gap required
    except TypeError:
        pass
    else:
        raise AssertionError("missing max_gap_ns must raise")
    try:
        bc.trap_integral_ns([(t0, 1.0), (t0 + hour, 1.0)], -1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative max_gap_ns must raise")
    # 20-minute change-gated gap is NOT integrated as a full leg
    wide = bc.trap_integral_ns([(t0, 1.0), (t0 + hour, 1.0)], hour - 1)
    assert wide["value"] is None and wide["legs_rejected"] == 1
    assert wide["coverage_ratio"] is None
    partial = bc.trap_integral_ns(
        [(t0, 1.0), (t0 + hour, 1.0), (t0 + 2 * hour, 1.0),
         (t0 + 4 * hour, 1.0)], hour)
    assert partial["legs_used"] == 2 and partial["legs_rejected"] == 1
    assert partial["value"] == 2.0
    assert partial["span_ns"] == 4 * hour
    assert partial["coverage_ratio"] == 0.5  # 2 observed hours of 4 spanned
    assert bc.trap_integral_ns(
        [(t0, 1.0), (t0 + 1, None), (t0 + 2, 1.0)], hour)["value"] is None
    assert bc.trap_integral_ns([(t0, 1.0)], hour)["value"] is None
    zero = bc.trap_integral_ns([(t0, 1.0), (t0, 2.0)], 0)
    assert zero["legs_used"] == 1 and zero["coverage_ratio"] == 1.0


def test_result_forces_null_and_rejects_fake_confidence():
    r = bc.make_result("soh", 5.0, "pct", "unavailable", reason="sparse")
    assert r["value"] is None and r["status"] == "unavailable"
    r = bc.make_result("soh", 92.0, "pct", "derived")
    assert r["uncertainty"] is None and r["coverage_ratio"] is None
    r = bc.make_result("soh", 92.0, "pct", "derived",
                       uncertainty=1.5, coverage_ratio=0.8)
    assert (r["uncertainty"], r["coverage_ratio"]) == (1.5, 0.8)
    assert bc.make_result("x", 1.0, None, "derived",
                          coverage_ratio=1.7)["coverage_ratio"] is None
    assert bc.make_result("x", 1.0, None, "derived",
                          computed_at_ns=1700000000000000000,
                          config_version="c1",
                          connectivity="online")["computed_at_ns"] == \
        1700000000000000000
    try:
        bc.make_result("x", 1.0, None, "bogus")
    except ValueError:
        pass
    else:
        raise AssertionError("unknown status must raise")
    try:
        bc.make_result("x", float("nan"), None, "derived")
    except ValueError:
        pass
    else:
        raise AssertionError("non-finite value must raise")


def test_result_rejects_bad_windows_and_spreads():
    for kwargs in ({"window_start_ns": "x"},
                   {"window_start_ns": 200, "window_end_ns": 100},
                   {"computed_at_ns": -3},
                   {"uncertainty": -1.0},
                   {"uncertainty": float("nan")},
                   {"uncertainty_lower": 2.0, "uncertainty_upper": 1.0},
                   {"uncertainty_lower": float("inf")}):
        try:
            bc.make_result("x", 1.0, None, "derived", **kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError("bad window/spread must raise: %r" % kwargs)
    try:
        bc.make_result("x", None, None, "derived")  # no value, no text
    except ValueError:
        pass
    else:
        raise AssertionError("derived needs a value or value_text")
    band = bc.make_result("r", 5.0, "ohm", "derived",
                          uncertainty_lower=4.0, uncertainty_upper=6.0)
    assert (band["uncertainty_lower"], band["uncertainty_upper"]) == (4.0, 6.0)
    text_only = bc.make_result("episode", None, None, "derived",
                               value_text="charge 12:00-13:00")
    assert text_only["value"] is None
    assert text_only["value_text"] == "charge 12:00-13:00"


def test_event_conflicting_ends_keep_stamps_without_duration():
    e = bc.normalize_event({"event_time_ns": 100, "vehicle": "v",
                            "event_type": "oem", "name": "n", "source": "s",
                            "started_ns": 90, "ended_ns": 80,
                            "body": "secret text"})
    assert e["duration_s"] is None  # no authoritative duration
    assert (e["started_ns"], e["ended_ns"]) == (90, 80)  # stamps retained
    assert e["body_redacted"] is True and "secret text" not in repr(e.values())
    late = bc.normalize_event({"event_time_ns": 100, "vehicle": "v",
                               "event_type": "oem", "name": "n", "source": "s"})
    assert late["started_ns"] is None and late["duration_s"] is None
    assert bc.normalize_event({"event_time_ns": 100}) is None


def test_event_retains_provenance_per_event():
    e = bc.normalize_event({"event_time_ns": 100, "vehicle": "v",
                            "event_type": "alerts", "name": "n", "source": "s",
                            "event_id": "ev-1", "quality": "unknown_start",
                            "config_version": "cfg-7",
                            "connectivity": "offline", "body_redacted": True})
    assert e["event_id"] == "ev-1"
    assert e["quality"] == "unknown_start"
    assert e["config_version"] == "cfg-7"
    assert e["connectivity"] == "offline"
    assert e["body_redacted"] is True
    assert bc.normalize_event(dict(e, body_redacted=False))["body_redacted"] is False


def test_episode_key_needs_valid_start():
    assert bc.episode_key("v", "oem", "n", 90) is not None
    assert bc.episode_key("v", "oem", "n", None) is None
    assert bc.episode_key("v", "oem", "n", 90) == \
        bc.episode_key("v", "oem", "n", 90)  # deterministic
    assert bc.episode_key("v", "oem", 7, 90) is None  # strict identity types
    import hashlib
    # byte-matches the recorder episode_id formula (decode_epoch optional)
    assert bc.episode_key("v", "oem", "n", 90) == hashlib.sha256(
        b"v|oem|n|90").hexdigest()
    assert bc.episode_key("v", "oem", "n", 90, "e2") == hashlib.sha256(
        b"v|oem|n|90|e2").hexdigest()


def test_run_analyses_isolates_module_failure():
    def good(signals, events, config):
        return [bc.make_result("m", 1.0, None, "derived")]

    def bad(signals, events, config):
        raise RuntimeError("super secret token abc123")

    def wrong(signals, events, config):
        return {"not": "a list"}

    rows = bc.run_analyses([], [], {}, [good, bad, wrong])
    assert rows[0]["status"] == "derived" and rows[0]["value"] == 1.0
    assert rows[1]["status"] == "error"
    assert rows[1]["reason"] == "execution_error:RuntimeError"
    assert "secret" not in rows[1]["reason"]  # no str(exc) leak
    assert rows[1]["analysis_id"] == "bad"  # caller scope preserved
    assert rows[2]["reason"] == "contract_violation:non_list_return"


def test_run_analyses_disambiguates_same_name():
    def dup(signals, events, config):
        raise RuntimeError("x")

    rows = bc.run_analyses([], [], {}, [dup, dup])
    assert [r["metric"] for r in rows] == ["dup#0", "dup#1"]
    assert [r["analysis_id"] for r in rows] == ["dup#0", "dup#1"]


def test_revision_deterministic():
    assert bc.revision_id("a", 1) == bc.revision_id("a", 1)
    assert bc.revision_id("a", 1) != bc.revision_id("a", 2)


def test_revision_canonical_over_dict_order():
    assert bc.revision_id({"b": 1, "a": 2}) == bc.revision_id({"a": 2, "b": 1})
    assert bc.revision_id({"a": 1}) != bc.revision_id({"a": 2})


def test_normalize_signals_detaches_prepared_rows():
    rows = bc.prepare_signals([sig(100, num=1.0), sig(200, num=2.0)])
    got = bc.normalize_signals(rows)
    assert got == list(rows) and all(g is not r for g, r in zip(got, rows))
    got[0]["value_num"] = 9.0
    assert rows[0]["value_num"] == 1.0
    assert bc.normalize_signals(rows)[0]["value_num"] == 1.0


def test_normalize_signals_raw_preserves_dup_and_invalid_order():
    bad_time = dict(sig(100, num=1.0), event_time_ns="bad")
    rows = [bad_time, sig(100, num=1.0), sig(100, num=2.0),
            sig(200, num=None, quality="invalid")]
    got = bc.normalize_signals(rows)
    assert [r["value_num"] for r in got] == [1.0, 2.0, None]
    assert bc.normalize_signals(None) == [] and bc.normalize_signals([]) == []


def test_join_batched_matches_sequential_probes():
    primaries = [sig(t, num=1.0) for t in (100, 200, 300)]
    secondaries = [sig(90, num=5.0), sig(190, num=6.0),
                   sig(290, num=7.0)]
    batched = bc.join_asof(primaries, secondaries, 50)
    sequential = [bc.join_asof([p], secondaries, 50)[0][1]
                  for p in primaries]
    assert [m for _, m in batched] == sequential
    assert [m["value_num"] for m in sequential] == [5.0, 6.0, 7.0]

def _ddls():
    return dict(db_init.ddl_statements(""))
def test_signal_schema_extends_safely():
    ddl = _ddls()["vehicle_signal"]
    for col in ('"quality" STRING NULL', '"envelope_id" STRING NULL',
                '"config_version" STRING NULL', '"connectivity" STRING NULL'):
        assert col in ddl, col
    for col in ('"event_time" TIMESTAMP(9) NOT NULL TIME INDEX',
                '"value_num" Float64 NULL', '"value_bool" Boolean NULL'):
        assert col in ddl, col


def test_event_analysis_tables_greptime_compatible():
    ddls = _ddls()
    event = ddls["vehicle_event"]
    assert '"event_time" TIMESTAMP(9) NOT NULL TIME INDEX' in event
    for col in ('"event_type" STRING NOT NULL', '"started_at" TIMESTAMP(9) NULL',
                '"ended_at" TIMESTAMP(9) NULL', '"duration_s" Float64 NULL',
                '"audience" STRING NULL', '"is_active" Boolean NULL',
                '"body_redacted" Boolean NULL', '"episode_id" STRING NULL',
                '"quality" STRING NULL', '"config_version" STRING NULL',
                '"connectivity" STRING NULL'):
        assert col in event, col
    assert "body" not in event.replace("body_redacted", "")
    analysis = ddls["vehicle_analysis"]
    assert '"window_start" TIMESTAMP(9) NOT NULL TIME INDEX' in analysis
    assert '"metric" STRING NOT NULL' in analysis  # quoted: reserved word
    for col in ('"value" Float64 NULL', '"value_text" STRING NULL',
                '"status" STRING NULL', '"reason" STRING NULL',
                '"evidence_count" Int64 NULL',
                '"coverage_ratio" Float64 NULL',
                '"algorithm_version" STRING NULL',
                '"calibration_version" STRING NULL',
                '"model_version" STRING NULL', '"uncertainty" Float64 NULL',
                '"uncertainty_lower" Float64 NULL',
                '"uncertainty_upper" Float64 NULL',
                '"computed_at" TIMESTAMP(9) NULL',
                '"quality" STRING NULL', '"config_version" STRING NULL',
                '"connectivity" STRING NULL',
                '"episode_id" STRING NULL', '"revision" STRING NOT NULL'):
        assert col in analysis, col
    assert "JSON" not in analysis  # mandatory fields never need JSON queries


def test_schema_statements_nondestructive():
    labels = [label for label, _ in db_init.ddl_statements("")]
    assert "vehicle_event" in labels and "vehicle_analysis" in labels
    banned = ("DROP TABLE", "TRUNCATE", "DELETE FROM")
    for label, stmt in db_init.ddl_statements(""):
        upper = stmt.upper()
        assert not any(b in upper for b in banned), label
    alters = " ".join(db_init.alter_statements())
    for col in ('"quality"', '"envelope_id"', '"config_version"',
                '"connectivity"', '"episode_id"', '"value_text"',
                '"uncertainty_lower"', '"uncertainty_upper"',
                '"computed_at"'):
        assert col in alters, col
    for tbl in ('"vehicle_signal"', '"vehicle_event"', '"vehicle_analysis"'):
        assert tbl in alters, tbl
    assert "IF NOT EXISTS" in alters


if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_common: ok (%d tests)" % len(names))
