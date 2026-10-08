"""Behavior tests for scripts.analytics.battery.battery_runtime.

Plain asserts, stdlib only; doubles as __main__ runner. All inputs are
tiny in-memory dicts and fake fetch/insert seams -- never a live DB, never
mock echoes. Covers: exact ns cell conversion (no float), strict bound
parsing (offsets, garbage rejection), per-module error isolation with
scope/window/module identity, failure invalidation of previous successes
(same-identity NULL error revisions; epochs/modules preserved),
recovery clearing of stale error indicators (non-health, history kept),
nullable-epoch invalidation, episode analysis_id folding without touching
revision, duplicate-PK refusal, config boundary errors (incl.
explicit-missing distinction), bounded-query refusal, backfill windows,
late/older-data warnings, and SQL quoting/identifier safety.
"""
import os
import sys
import tempfile
import sqlite3

from scripts.analytics.battery import battery_common as bc
from scripts.analytics.battery import battery_runtime as br


def _sig(vehicle, source, epoch, ns, field="BatteryCurrent", value=10.0,
         unit="A", quality="valid", ingest=None):
    row = {"event_time_ns": ns, "ingest_time_ns": ingest,
           "vehicle": vehicle, "source": source, "decode_epoch": epoch,
           "path": "Vehicle.Battery." + field, "source_field": field,
           "value_num": value, "value_text": None, "value_bool": None,
           "unit": unit, "quality": quality, "envelope_id": None,
           "config_version": None, "connectivity": None}
    assert bc.normalize_signal(row) is not None
    return row


def _row(metric="battery.rul.cycles_remaining", analysis_id="battery_rul",
         episode=None, vehicle="v", source="fleet", epoch="e1",
         ws=3_600_000_000_000, we=7_199_999_999_999, computed=5,
         revision="r1", value=None, status="unavailable",
         reason="missing_model"):
    return bc.make_result(
        metric=metric, value=value, unit=None, status=status, reason=reason,
        window_start_ns=ws, window_end_ns=we, vehicle=vehicle,
        source=source, decode_epoch=epoch, computed_at_ns=computed,
        analysis_id=analysis_id, episode_id=episode, revision=revision)


def test_ns_cell_exact_no_float():
    # 9-digit ns ints survive exactly; floats/bools refuse.
    assert br.to_ns_cell(1790493066062145838, "TimestampNanosecond") == \
        1790493066062145838
    assert br.to_ns_cell(1790493066062145,
                         "TimestampMicrosecond") == 1790493066062145000
    assert br.to_ns_cell(1790493066062, "TimestampMillisecond") == \
        1790493066062000000
    assert br.to_ns_cell(1790493066, "TimestampSecond") == 1790493066000000000
    assert br.to_ns_cell(1790493066.5, "TimestampNanosecond") is None
    assert br.to_ns_cell(True, "TimestampNanosecond") is None
    assert br.to_ns_cell(-5, "TimestampNanosecond") is None
    ts = br.ns_to_sql_ts(1790493066062145838)
    assert ts.endswith(".062145838"), ts
    back = br.parse_time_bound("2026-09-27 07:11:06.062145838",
                               allow_empty=False)
    assert back == 1790493066062145838, back
    # Timezone offsets convert to the same UTC instant (all spellings).
    assert br.parse_time_bound("2026-09-27T16:11:06.062145838+09:00",
                               allow_empty=False) == back
    assert br.parse_time_bound("2026-09-27 16:11:06.062145838+0900",
                               allow_empty=False) == back
    assert br.parse_time_bound("2026-09-27T07:11:06.062145838Z",
                               allow_empty=False) == back
    assert br.parse_time_bound("2026-09-27", allow_empty=False) == \
        1790467200000000000
    assert br.parse_time_bound("", allow_empty=True) is None
    for bad in ("not-a-time", "2026-13-99 99:99:99",
                "2026-09-27 07:11:06.1234567891", "1969-01-01 00:00:00",
                "2026-09-27 07:11:06.062145838+25:00",
                "2026-09-27 07:11:06.062145838+09:99",
                "2026-09-27 07:11:06.06214583x9",
                "2026-09-27 07:11:06.abc",
                "2026-09-27 07:11:06.062145838junk",
                "2026-09-27T16:11:06+09",
                0, -1, True, 1.5):
        try:
            br.parse_time_bound(bad, allow_empty=True)
        except br.BatteryConfigError:
            pass
        else:
            raise AssertionError("must refuse %r" % (bad,))


def test_module_error_isolated_with_identity():
    def boom(signals, events, config):
        raise RuntimeError("secret-conn-string-must-not-leak")

    def fine(signals, events, config):
        return [bc.make_result(
            metric="battery.fine.m", value=None, unit=None,
            status="unavailable", reason="no_signals",
            window_start_ns=config.get("window_start_ns"),
            window_end_ns=config.get("window_end_ns"),
            analysis_id="fine", revision="fixed-rev")]
    boom.__name__ = "battery_boom"
    fine.__name__ = "battery_fine"
    rows = bc.run_analyses([], [], {"window_start_ns": 7}, [boom, fine])
    assert len(rows) == 2, rows
    err = [r for r in rows if r["analysis_id"] == "battery_boom"][0]
    assert err["status"] == "error", err
    assert err["reason"] == "execution_error:RuntimeError", err
    assert "secret" not in err["reason"], err  # never the message
    assert err["value"] is None, err
    # Non-list returns are contract violations, same isolation.
    def bad(signals, events, config):
        return {"not": "a list"}
    bad.__name__ = "battery_bad"
    rows = bc.run_analyses([], [], {}, [bad, fine])
    assert [r for r in rows
            if r["reason"] == "contract_violation:non_list_return"], rows
    # Runtime enriches the synthetic error with scope/window identity.
    filled = br.fill_missing_identity(
        [dict(err)], ("vv", "fleet", "e9"), (100, 200), 999)
    assert filled[0]["vehicle"] == "vv", filled[0]
    assert filled[0]["source"] == "fleet", filled[0]
    assert filled[0]["decode_epoch"] == "e9", filled[0]
    assert filled[0]["window_start_ns"] == 100, filled[0]
    assert filled[0]["window_end_ns"] == 200, filled[0]
    assert filled[0]["computed_at_ns"] == 999, filled[0]
    assert filled[0]["revision"], filled[0]  # deterministic, never clock


def test_module_failure_invalidates_previous_success():
    # Root bug: prior metric=battery.conditions.probe value=1 (estimated)
    # plus a new raising-module pseudo-name error left TWO latest rows and
    # the old 1 authoritative. Failure must write a NULL error revision
    # over the SAME logical identity; the dashboard RANKED query (same
    # identity, computed_at DESC, revision DESC) then resolves to the
    # tombstone. Other modules/epochs stay untouched.
    ws = 8 * br.HOUR_NS
    scope = ("vv", "fleet", "e1")
    old = _row(metric="battery.conditions.probe",
               analysis_id="battery_conditions", vehicle="vv",
               source="fleet", epoch="e1", ws=ws,
               we=ws + br.HOUR_NS - 1, computed=10, revision="old-rev",
               value=1.0, status="estimated", reason="fitted")
    previous = {("battery.conditions.probe", "battery_conditions", ws,
                 "e1")}
    failed = {"battery_conditions"}
    tombstones = br.invalidate_previous(previous, scope,
                                        (ws, ws + br.HOUR_NS - 1), failed,
                                        20)
    def raising_conditions(signals, events, config):
        raise RuntimeError("synthetic execution failure")
    raising_conditions.__name__ = "battery_conditions"
    indicator = br.fill_missing_identity(
        bc.run_analyses([], [], {}, [raising_conditions]),
        scope, (ws, ws + br.HOUR_NS - 1), 20)[0]
    assert len(tombstones) == 1, tombstones
    tomb = tombstones[0]
    assert tomb["metric"] == "battery.conditions.probe", tomb
    assert tomb["analysis_id"] == "battery_conditions", tomb
    assert tomb["window_start_ns"] == ws and tomb["decode_epoch"] == "e1"
    assert tomb["status"] == "error" and tomb["value"] is None, tomb
    assert tomb["revision"] != "old-rev"  # new revision, history kept
    # Same logical identity, newer computed_at: the RANKED latest row is
    # the tombstone, so the old value=1 is no longer authoritative.
    assert (tomb["vehicle"], tomb["metric"], tomb["source"],
            tomb["decode_epoch"], tomb["analysis_id"],
            tomb["window_start_ns"]) == (
                old["vehicle"], old["metric"], old["source"],
                old["decode_epoch"], old["analysis_id"],
                old["window_start_ns"])
    assert (tomb["computed_at_ns"], str(tomb["revision"])) > (
        old["computed_at_ns"], str(old["revision"]))
    # Indicator is a separate identity (never collides with health rows).
    assert (indicator["metric"], indicator["analysis_id"]) == (
        "battery.conditions.error", "battery_conditions")
    recovered = br.recover_indicators(
        {(indicator["metric"], indicator["analysis_id"], ws, "e1")},
        scope, (ws, ws + br.HOUR_NS - 1), failed, 30)
    assert len(recovered) == 1 and recovered[0]["status"] == "reported"
    assert recovered[0]["value"] is None
    # Other epochs are never erased by this scope's failure.
    assert br.invalidate_previous(
        previous, ("vv", "fleet", "e2"), (ws, ws + br.HOUR_NS - 1),
        failed, 20) == []
    # Other modules' successes get no tombstone.
    rul_ok = _row(metric="battery.rul.cycles_remaining",
                  analysis_id="battery_rul", vehicle="vv", source="fleet",
                  epoch="e1", ws=ws, we=ws + br.HOUR_NS - 1,
                  computed=5, revision="rul-rev",
                  value=3.0, status="estimated", reason="fitted")
    assert rul_ok["metric"] not in {
        r["metric"] for r in br.invalidate_previous(
            previous | {(rul_ok["metric"], rul_ok["analysis_id"], ws,
                         "e1")}, scope, (ws, ws + br.HOUR_NS - 1),
            failed, 20)}
    # Unrelated previous identities get no tombstone; reruns are stable.
    assert br.invalidate_previous(
        {("battery.rul.cycles_remaining", "battery_rul", ws, "e1")},
        scope, (ws, ws + br.HOUR_NS - 1), failed, 20) == []
    again = br.invalidate_previous(previous, scope,
                                   (ws, ws + br.HOUR_NS - 1), failed, 20)
    assert [r["revision"] for r in again] == [tomb["revision"]]


def test_recovered_module_clears_stale_indicator():
    # Error indicators must not stay latest forever after recovery: a
    # recovered module writes an explicit non-health recovery state over
    # the same indicator identity (history preserved, never a healthy
    # value). Health metrics are untouched by recovery rows.
    ws = 8 * br.HOUR_NS
    scope = ("vv", "fleet", "e1")
    previous = {("battery.conditions.error", "battery_conditions", ws,
                 "e1"),
                ("battery.conditions.probe", "battery_conditions", ws,
                 "e1")}
    recovered = {"battery_conditions"}
    rows = br.recover_indicators(previous, scope,
                                 (ws, ws + br.HOUR_NS - 1), recovered,
                                 30)
    assert len(rows) == 1, rows
    rec = rows[0]
    assert (rec["metric"], rec["analysis_id"]) == (
        "battery.conditions.error", "battery_conditions"), rec
    assert rec["status"] == "reported" and rec["value"] is None, rec
    assert rec["value_text"] == "execution_recovered", rec
    assert rec["reason"] == "recovered:battery.conditions.error", rec
    assert rec["window_start_ns"] == ws and rec["decode_epoch"] == "e1"
    assert rec["computed_at_ns"] == 30, rec
    # Refreshed (same-pass rewritten) indicators are not duplicated.
    assert br.recover_indicators(
        previous, scope, (ws, ws + br.HOUR_NS - 1), recovered, 30,
        refreshed={("battery.conditions.error", "battery_conditions",
                    ws, "e1")}) == []
    # Health identities are never cleared by recovery; other modules and
    # still-failing modules get no recovery row.
    assert all(r["metric"].endswith(".error") for r in rows)
    assert br.recover_indicators(previous, scope,
                                 (ws, ws + br.HOUR_NS - 1),
                                 {"battery_rul"}, 30) == []
    assert br.recover_indicators(set(), scope,
                                 (ws, ws + br.HOUR_NS - 1), recovered,
                                 30) == []
    again = br.recover_indicators(previous, scope,
                                  (ws, ws + br.HOUR_NS - 1), recovered,
                                  30)
    assert [r["revision"] for r in again] == [rec["revision"]]


def test_nullable_epoch_invalidation():
    # Scopes with decode_epoch None persist decode_epoch NULL; the
    # previous-identity fetch must use IS NULL and invalidation must
    # match the NULL epoch instead of silently matching nothing.
    ws = 8 * br.HOUR_NS
    scope = ("vv", "fleet", None)
    previous = {("battery.rul.cycles_remaining", "battery_rul", ws,
                 None)}
    rows = br.invalidate_previous(previous, scope,
                                  (ws, ws + br.HOUR_NS - 1),
                                  {"battery_rul"}, 20)
    assert len(rows) == 1, rows
    assert rows[0]["decode_epoch"] is None, rows[0]
    assert rows[0]["window_start_ns"] == ws, rows[0]
    assert rows[0]["status"] == "error" and rows[0]["value"] is None
    # A NULL-epoch tombstone never touches a named epoch's identity.
    assert br.invalidate_previous(
        {("battery.rul.cycles_remaining", "battery_rul", ws, "e1")},
        scope, (ws, ws + br.HOUR_NS - 1), {"battery_rul"}, 20) == []
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE vehicle_analysis (metric TEXT, analysis_id TEXT,"
               " window_start TEXT, vehicle TEXT, source TEXT, decode_epoch TEXT)")
    scopes = [("vv", "fleet", None), ("vv", None, None), ("vv", None, "")]
    for vehicle, source, epoch in scopes:
        db.execute("INSERT INTO vehicle_analysis VALUES (?, ?, ?, ?, ?, ?)",
                   ("battery.rul.cycles_remaining", "battery_rul",
                    br.ns_to_sql_ts(ws), vehicle, source, epoch))

    def sqlite_fetch(base_url, auth, database, statement, max_rows):
        cursor = db.execute(statement)
        columns = [column[0] for column in cursor.description]
        return columns, cursor.fetchall(), [
            {"name": column, "data_type": "String"} for column in columns]

    original_fetch = br.fetch_raw
    try:
        br.fetch_raw = sqlite_fetch
        for scope in scopes:
            assert br.fetch_previous_identities(
                "", "", "", scope, [(ws, ws + br.HOUR_NS - 1)], 100) == {
                    ("battery.rul.cycles_remaining", "battery_rul", ws,
                     scope[2])}
    finally:
        br.fetch_raw = original_fetch
        db.close()


def test_episode_identity_fold_keeps_revision():
    ep1 = _row(metric="battery.alerts.episode",
               analysis_id="battery_alerts:episode:H1", episode="H1",
               revision="same-rev")
    ep2 = _row(metric="battery.alerts.episode",
               analysis_id="battery_alerts:episode:H2", episode="H2",
               revision="same-rev")
    br.disambiguate_rows([ep1, ep2])
    assert ep1["analysis_id"] == "battery_alerts:episode:H1", ep1
    assert ep2["analysis_id"] == "battery_alerts:episode:H2", ep2
    assert ep1["revision"] == ep2["revision"] == "same-rev"  # untouched
    br.validate_no_duplicate_pks([ep1, ep2])  # distinct PKs coexist
    generic = _row(metric="battery.alerts.episode",
                   analysis_id="battery_alerts", episode="H9",
                   revision="r9")
    br.disambiguate_rows([generic])
    assert generic["analysis_id"] == "battery_alerts:H9", generic
    assert generic["revision"] == "r9"
    unknown = _row(metric="battery.alerts.episode",
                   analysis_id="battery_alerts:unknown:abcd1234",
                   episode=None, revision="ru")
    br.disambiguate_rows([unknown])
    assert unknown["analysis_id"] == "battery_alerts:unknown:abcd1234"
    # Same PK with differing payloads refuses instead of last-wins.
    clash = _row(metric="battery.alerts.episode",
                 analysis_id="battery_alerts:episode:H1", episode="H1",
                 revision="same-rev", reason="different-reason")
    try:
        br.validate_no_duplicate_pks([ep1, clash])
    except br.BatteryDuplicateError:
        pass
    else:
        raise AssertionError("conflicting PK must raise")
    exact_copy = dict(ep1)
    assert br.validate_no_duplicate_pks([ep1, exact_copy]) == [ep1]


def test_bad_battery_limit_does_not_abort_aggregate_startup():
    from scripts.analytics import aggregate
    original_env = dict(os.environ)
    try:
        os.environ.clear()
        os.environ["BATTERY_LOOKBACK_HOURS"] = "not-an-integer"
        config = aggregate.load_cfg()
        try:
            aggregate.battery_section((None, None, None), config)
        except br.BatteryConfigError:
            pass
        else:
            raise AssertionError("battery section must reject its invalid limit")
    finally:
        os.environ.clear()
        os.environ.update(original_env)


def test_config_boundaries():
    # Only intentionally absent config (empty or default path) is
    # uncalibrated; a missing EXPLICITLY configured path is a visible
    # config_missing error, never silent calibration erasure.
    assert br.load_analysis_config("") == ({}, None, None)
    assert br.load_analysis_config("/nonexistent/path.json",
                                   explicit=False) == ({}, None, None)
    cfg, version, err = br.load_analysis_config(
        "/nonexistent/explicit.json", explicit=True)
    assert cfg == {} and version is None and err and \
        err.startswith("config_missing:"), (cfg, version, err)
    assert "explicit.json" in err and "/" not in err.split(":", 1)[1]
    with tempfile.TemporaryDirectory(prefix="batt-cfg-") as tmp:
        default_path = br.CONFIG_PATH_DEFAULT
        try:
            br.CONFIG_PATH_DEFAULT = os.path.join(tmp, "absent-default.json")
            assert br.load_analysis_config(br.CONFIG_PATH_DEFAULT) == \
                ({}, None, None)
            assert br.load_analysis_config(
                br.CONFIG_PATH_DEFAULT, explicit=True)[2] == \
                "config_missing:absent-default.json"
        finally:
            br.CONFIG_PATH_DEFAULT = default_path
        non_utf8 = os.path.join(tmp, "non-utf8.json")
        with open(non_utf8, "wb") as handle:
            handle.write(b"\xff")
        assert br.load_analysis_config(non_utf8)[2] == \
            "malformed:analysis_config_encoding"
        good = os.path.join(tmp, "good.json")
        with open(good, "w", encoding="utf-8") as handle:
            handle.write('{"rul": {}, "alerts": {}}')
        cfg, version, err = br.load_analysis_config(good)
        assert err is None and cfg == {"rul": {}, "alerts": {}}, (cfg, err)
        assert version and len(version) == 16, version
        same = os.path.join(tmp, "same.json")
        with open(same, "w", encoding="utf-8") as handle:
            handle.write('{"alerts": {}, "rul": {}}')
        assert br.load_analysis_config(same)[1] == version  # key order
        for name, body in (
                ("bad.json", "{not json"),
                ("list.json", "[1, 2]"),
                ("unknown.json", '{"hacker": {}}'),
                ("section.json", '{"rul": []}')):
            path = os.path.join(tmp, name)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(body)
            cfg, version, err = br.load_analysis_config(path)
            assert cfg == {} and version is None and err and \
                err.startswith("malformed:"), (name, err)
        big = os.path.join(tmp, "big.json")
        with open(big, "w", encoding="utf-8") as handle:
            handle.write('{"rul": "' + "x" * (5 * 1024 * 1024) + '"}')
        assert br.load_analysis_config(big)[2] == \
            "malformed:analysis_config_oversize"


def test_bounded_query_refusal():
    schema = [{"name": "a", "data_type": "String"}]
    seen = {}

    def fake_request(base_url, auth, db, stmt, timeout=60):
        seen["stmt"] = stmt
        rows = [[str(i)] for i in range(7)]
        return {"output": [{"records": {"schema": {"column_schemas":
                                                   schema}, "rows": rows}}]}
    real = br.request_sql
    br.request_sql = fake_request
    try:
        try:
            br.fetch_raw("http://x", "a", "db", "SELECT a FROM t", 5)
        except br.BatteryCapError as exc:
            assert "row cap exceeded (7 > 5)" in str(exc), exc
            assert "LIMIT 6" in seen["stmt"], seen["stmt"]  # cap+1 probe
        else:
            raise AssertionError("over-cap fetch must raise")
        _, rows_out, _ = br.fetch_raw("http://x", "a", "db",
                                      "SELECT a FROM t;", 7)
        assert len(rows_out) == 7  # at-cap passes
    finally:
        br.request_sql = real


def test_late_backfill_windows_and_warnings():
    hour = br.HOUR_NS
    seal = 10 * hour
    cutoff = 8 * hour
    normal = br.build_windows(cutoff, seal)
    assert normal == [(8 * hour, 9 * hour - 1), (9 * hour, 10 * hour - 1)], \
        normal  # sealed only; open hour never written
    backfilled = br.build_windows(cutoff, seal, 5 * hour, 6 * hour + 1)
    starts = [ws for ws, _ in backfilled]
    assert 5 * hour in starts and 8 * hour in starts, starts
    assert backfilled == sorted(set(backfilled))  # deduped, ordered
    for bad in ((cutoff, seal, 1 * hour, None),
                (cutoff, seal, 2 * hour, 1 * hour)):
        try:
            br.build_windows(*bad)
        except br.BatteryConfigError:
            pass
        else:
            raise AssertionError("bad backfill must raise: %r" % (bad,))
    # Late rows inside the fetch range are included by inclusive bounds;
    # rows older than fetch_start trigger the explicit warning path.
    assert br.floor_hour_ns(8 * hour + 1) == 8 * hour
    assert br.ceil_hour_ns(8 * hour) == 8 * hour
    assert br.ceil_hour_ns(8 * hour + 1) == 9 * hour


def test_sql_values_preserve_identity_and_nanoseconds():
    ns = 1790493066062145838
    vehicle = "v'); DROP TABLE vehicle_analysis; --"
    sql = br.row_to_sql(_row(ws=ns, we=ns + 1, computed=ns + 2,
                             vehicle=vehicle))
    with sqlite3.connect(":memory:") as conn:
        decoded = conn.execute("SELECT " + sql[1:-1]).fetchone()
    assert vehicle in decoded
    assert "2026-09-27 07:11:06.062145838" in decoded
    assert "2026-09-27 07:11:06.062145839" in decoded
    assert "2026-09-27 07:11:06.062145840" in decoded


def test_sparse_runtime_only_persists_unavailable_to_invalidate_prior_result():
    from contextlib import ExitStack
    from unittest.mock import patch

    hour = br.HOUR_NS
    signals = []
    with sqlite3.connect(":memory:") as db, ExitStack() as stack:
        db.execute("CREATE TABLE vehicle_analysis ("
                   + ", ".join(br.ANALYSIS_COLS) + ")")

        def sql_request(base_url, auth, database, statement, timeout=60):
            cursor = db.execute(statement)
            if cursor.description is None:
                return {}
            return {"output": [{"records": {
                "schema": {"column_schemas": [
                    {"name": column[0], "data_type": "String"}
                    for column in cursor.description]},
                "rows": cursor.fetchall()}}]}

        stack.enter_context(patch.object(br, "request_sql", sql_request))
        stack.enter_context(patch.object(
            br, "fetch_signals", lambda *args: list(signals)))
        stack.enter_context(patch.object(br, "fetch_events", lambda *args: []))
        stack.enter_context(patch.object(br, "coverage_min", lambda *args: None))
        cfg = {"vehicle": "v", "battery_config": "", "battery_lookback_h": 2}
        assert br.run_battery(("", "", ""), cfg, now_ns=10 * hour) == 0
        assert db.execute("SELECT COUNT(*) FROM vehicle_analysis").fetchone() == (0,)

        signals.append(_sig("v", "fleet", "e1", 8 * hour + 1,
                            field="BrickVoltageMax", value=4.1, unit=None,
                            quality="unit_unverified"))
        br.run_battery(("", "", ""), cfg, now_ns=10 * hour + 1)
        metric = "battery.conditions.brick_max_raw"
        assert db.execute(
            'SELECT "value", status FROM vehicle_analysis WHERE metric=?',
            (metric,)).fetchall() == [(4.1, "reported")]
        assert db.execute(
            "SELECT COUNT(*) FROM vehicle_analysis WHERE status='unavailable'"
        ).fetchone() == (0,)

        # A late invalid sample must clear the old value, but cannot create
        # empty results for a different epoch or the adjacent empty hour.
        for epoch in ("e1", "e2"):
            signals.append(_sig("v", "fleet", epoch, 8 * hour + 2,
                                field="BrickVoltageMax", value=None,
                                unit=None, quality="invalid"))
        br.run_battery(("", "", ""), cfg, now_ns=10 * hour + 2)
        assert db.execute(
            'SELECT "value", status FROM vehicle_analysis WHERE metric=? '
            "AND decode_epoch='e1' ORDER BY computed_at DESC LIMIT 1",
            (metric,)).fetchone() == (None, "unavailable")
        assert db.execute(
            "SELECT metric, decode_epoch FROM vehicle_analysis "
            "WHERE status='unavailable'").fetchall() == [(metric, "e1")]
        assert db.execute(
            "SELECT COUNT(*) FROM vehicle_analysis WHERE window_start=?",
            (br.ns_to_sql_ts(9 * hour),)).fetchone() == (0,)

def test_prepared_scope_rows_do_not_mutate_across_windows():
    hour = br.HOUR_NS
    first = _sig("v", "fleet", "e1", 8 * hour + 1, field="Soc", value=60.0,
                 unit="%", quality="valid")
    second = _sig("v", "fleet", "e1", 9 * hour + 1, field="Soc", value=61.0,
                  unit="%", quality="valid")
    prepared = bc.prepare_signals([first, second])
    snapshot = [dict(r) for r in prepared]
    from scripts.analytics.battery import battery_conditions as co
    one = {"window_start_ns": 8 * hour, "window_end_ns": 9 * hour - 1}
    two = {"window_start_ns": 9 * hour, "window_end_ns": 10 * hour - 1}
    first_out = co.analyze(prepared, [], one)
    assert prepared == snapshot
    second_out = co.analyze(prepared, [], two)
    assert prepared == snapshot
    fresh = co.analyze([first, second], [], two)
    assert second_out == fresh
    assert first_out != second_out


if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_runtime: ok (%d tests)" % len(names))
