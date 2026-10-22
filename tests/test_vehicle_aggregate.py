"""Vehicle summary boundary and reconciliation regressions."""
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

from scripts.analytics import aggregate as agg


def dt(h=0, m=0, s=0):
    return datetime(2026, 9, 20, h, m, s)


class TripDistanceTest(unittest.TestCase):
    def test_zero_speed_ramps_down_no_bridge(self):
        pts = [(dt(10, 0), 36.0, None), (dt(10, 5), 0.0, None),
               (dt(10, 10), 36.0, None)]
        segs = agg.segment_trips(pts, gap_min=30, min_speed=1.0)
        self.assertEqual(len(segs), 1)
        # (36+0)/2*5/60 + (0+36)/2*5/60 = 3.0; bridging the stop gives 6.0
        self.assertAlmostEqual(segs[0]["distance_km"], 3.0, places=6)

    def test_continuous_zero_closes_trip(self):
        pts = [(dt(10, 0), 36.0, None), (dt(10, 1), 36.0, None)]
        pts += [(dt(10, minute), 0.0, None) for minute in range(2, 16)]
        pts.append((dt(10, 20), 36.0, None))
        segs = agg.segment_trips(pts, gap_min=10, min_speed=1.0)
        closed = [s for s in segs if not s["open"]]
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0]["end"], dt(10, 2))

    def test_unknown_speed_breaks_path_without_closing(self):
        pts = [(dt(10, 0), 36.0, None), (dt(10, 2), None, None),
               (dt(10, 4), 36.0, None)]
        segs = agg.segment_trips(pts, gap_min=30, min_speed=1.0)
        self.assertEqual(len(segs), 1)
        dist = segs[0]["distance_km"]
        self.assertTrue(dist is None or abs(dist) < 1e-9)

    def test_open_flag_when_never_closed(self):
        pts = [(dt(10, 0), 36.0, None), (dt(10, 1), 36.0, None)]
        segs = agg.segment_trips(pts, gap_min=10, min_speed=1.0)
        self.assertTrue(segs[0]["open"])


class ChargeRulesTest(unittest.TestCase):
    def test_soc_alone_never_opens(self):
        pts = [(dt(10, i), 40.0 + i, None, None, None, None)
               for i in range(5)]
        self.assertEqual(agg.segment_charges(pts), [])

    def test_explicit_false_closes_and_keeps_zero_evidence(self):
        t0, t1, t2, t3 = dt(10, 0), dt(10, 5), dt(10, 10), dt(10, 11)
        pts = [(t0, 40.0, 6.0, True, None, 0.0),
               (t1, 41.0, 6.0, True, None, 0.0),
               (t2, 41.0, None, False, None, 0.0),
               (t3, 41.0, 6.0, True, None, 0.0)]
        segs = agg.segment_charges(pts)
        self.assertEqual(len(segs), 2)
        # First session: 6kW over 5min = 0.5kWh, closed at/before t2.
        self.assertLessEqual(segs[0]["end"], t2)
        self.assertAlmostEqual(segs[0]["energy_added_kwh"], 0.5, places=6)

    def test_movement_closes_session(self):
        pts = [(dt(10, 0), 40.0, 7.0, True, None, 0.0),
               (dt(10, 5), 41.0, 7.0, True, None, 60.0),
               (dt(10, 6), 41.0, 7.0, True, None, 0.0)]
        segs = agg.segment_charges(pts)
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0]["end"], dt(10, 0))

    def test_unknown_speed_counts_as_parked(self):
        pts = [(dt(10, i), 40.0 + i, 7.0, None, None, None)
               for i in range(3)]
        segs = agg.segment_charges(pts)
        self.assertEqual(len(segs), 1)

    def test_time_weighted_power_irregular(self):
        t0, t1, t2 = dt(10, 0), dt(10, 30), dt(10, 36)
        pts = [(t0, 40.0, 6.0, None, None, 0.0),
               (t1, 45.0, 6.0, None, None, 0.0),
               (t2, 46.0, 12.0, None, None, 0.0)]
        # mean-of-samples would give 8kW*0.6h=4.8; trapezoidal gives
        # 6*0.5 + 9*0.1 = 3.9
        segs = agg.segment_charges(pts)
        self.assertAlmostEqual(segs[0]["energy_added_kwh"], 3.9, places=6)

    def test_counter_seed_keeps_first_increment(self):
        t0, t1 = dt(10, 0), dt(10, 5)
        pts = [(t0, 40.0, None, None, 10.0, 0.0),
               (t1, 41.0, None, None, 11.0, 0.0)]
        segs = agg.segment_charges(pts, gap_min=30)
        self.assertEqual(len(segs), 1)
        self.assertAlmostEqual(segs[0]["energy_added_kwh"], 1.0, places=9)
        self.assertEqual(segs[0]["start"], t0)
        self.assertEqual(segs[0]["duration_s"], 300.0)

    def test_reset_counter_yields_null_not_negative(self):
        pts = [(dt(10, 0), 40.0, None, True, 10.0, 0.0),
               (dt(10, 5), 41.0, None, True, 11.0, 0.0),
               (dt(10, 10), 42.0, None, True, 2.0, 0.0)]
        segs = agg.segment_charges(pts)
        self.assertIsNone(segs[0]["energy_added_kwh"])

    def test_no_power_no_energy_is_null(self):
        pts = [(dt(10, i), 40.0, None, True, None, 0.0) for i in range(3)]
        segs = agg.segment_charges(pts)
        self.assertIsNone(segs[0]["energy_added_kwh"])
        self.assertIsNone(segs[0]["avg_power_kw"])

    def test_terminal_zero_power_is_integrated(self):
        pts = [(dt(10, 0), 40., 6., True, None, 0.),
               (dt(10, 5), 41., 6., True, None, 0.),
               (dt(10, 10), 42., 0., False, None, 0.)]
        session = agg.segment_charges(pts)[0]
        self.assertEqual(session["end"], dt(10, 10))
        self.assertAlmostEqual(session["energy_added_kwh"], .75)

    def test_quiet_samples_do_not_extend_charging_forever(self):
        pts = [(dt(10, 0), 40., 6., None, None, 0.),
               (dt(10, 5), 41., 6., None, None, 0.)]
        pts += [(dt(10, minute), 41., 0., None, None, 0.)
                for minute in range(6, 41)]
        session = agg.segment_charges(pts)[0]
        self.assertFalse(session["open"])
        self.assertEqual(session["end"], dt(10, 6))
        self.assertAlmostEqual(session["energy_added_kwh"], .55)


class ConversionTest(unittest.TestCase):
    def test_explicit_units_only(self):
        self.assertIsNone(agg.to_kph(10.0, "furlongs"))
        self.assertIsNone(agg.to_kph(10.0, None))
        self.assertAlmostEqual(agg.to_kph(10.0, "m/s"), 36.0)
        self.assertIsNone(agg.to_kw(10.0, "hp"))
        self.assertIsNone(agg.to_kwh(10.0, "btu"))

    def test_non_numeric_nan_inf_never_raise(self):
        self.assertIsNone(agg.to_kwh("abc", "kwh"))
        self.assertIsNone(agg.to_kph({"v": 1}, "km/h"))
        self.assertIsNone(agg.to_kw([1], "kw"))
        self.assertIsNone(agg.to_kwh(float("nan"), "kwh"))
        self.assertIsNone(agg.to_kw(float("inf"), "kw"))
        self.assertIsNone(agg.to_kph("nan", "km/h"))


class GroupingTest(unittest.TestCase):
    def test_epoch_isolation(self):
        cols = ["event_time", "vehicle", "path", "source", "decode_epoch",
                "value_num", "value_bool", "unit"]
        rows = [
            [dt(10, 0), "v", "P.speed", "s", "e1", 10.0, None, "m/s"],
            [dt(10, 0), "v", "P.speed", "s", "e2", 99.0, None, "m/s"],
        ]
        paths = {"speed": "P.speed", "soc": "", "drive_energy": "",
                 "charge_energy": "", "power": "", "charging": ""}
        g = agg.group_vehicle_rows(cols, rows, paths)
        self.assertEqual(len(g), 2)
        self.assertNotEqual(g[("v", "s", "e1")]["speed"],
                            g[("v", "s", "e2")]["speed"])

    def test_split_energy_paths(self):
        cols = ["event_time", "vehicle", "path", "source", "decode_epoch",
                "value_num", "value_bool", "unit"]
        rows = [[dt(10, 0), "v", "P.drive", "s", "e", 5.0, None, "kwh"],
                [dt(10, 0), "v", "P.charge", "s", "e", 7.0, None, "kwh"]]
        paths = {"speed": "", "soc": "", "drive_energy": "P.drive",
                 "charge_energy": "P.charge", "power": "", "charging": ""}
        g = agg.group_vehicle_rows(cols, rows, paths)
        grp = g[("v", "s", "e")]
        key = agg.parse_ts_ns(dt(10, 0))
        self.assertEqual(grp["drive_energy"][key], (5.0, "kwh"))
        self.assertEqual(grp["charge_energy"][key], (7.0, "kwh"))

    def test_carry_bounded_by_age(self):
        t0, t1 = dt(10, 0), dt(10, 1)
        self.assertEqual(agg._carry([t0], [50.0], t1, 300), 50.0)
        self.assertIsNone(agg._carry([t0], [50.0], dt(12, 0), 300))
        self.assertIsNone(agg._carry([t0, t1], [50.0, None], dt(10, 2), 300))

    def test_drive_counter_requires_unbroken_bounded_baseline(self):
        self.assertIsNone(agg._window_counter_delta(
            [dt(10, 1), dt(10, 2)], [10., 11.], dt(10, 0), dt(10, 3), 300))
        self.assertIsNone(agg._window_counter_delta(
            [dt(10, 0), dt(10, 1), dt(10, 2)], [10., None, 11.],
            dt(10, 0), dt(10, 3), 300))
        self.assertEqual(agg._window_counter_delta(
            [dt(10, 0), dt(10, 2)], [10., 11.], dt(10, 0), dt(10, 3), 300), 1.)


class ReconcilePlanTest(unittest.TestCase):
    def test_covered_identical_skips_but_changed_refreshes(self):
        key = ("v", "s", "e")
        seg = {"start": dt(10, 0), "end": dt(10, 30), "duration_s": 1800.0,
               "distance_km": 5.0, "energy_kwh": 1.0, "avg_speed_kph": 10.0,
               "start_soc": 40.0, "end_soc": 45.0}
        anchor = dict(seg, id="v/s/e/old")
        ins, dels = agg._plan_group_writes(
            "trip", key, [seg], [anchor], dt(9, 0), dt(12, 0), 600)
        self.assertEqual((ins, dels), ([], []))
        changed = dict(seg, distance_km=9.0)
        ins, dels = agg._plan_group_writes(
            "trip", key, [changed], [anchor], dt(9, 0), dt(12, 0), 600)
        self.assertEqual(len(ins), 1)
        self.assertEqual(dels, ["v/s/e/old"])

    def test_open_trailing_keeps_anchor_until_sealed(self):
        key = ("v", "s", "e")
        seg = {"start": dt(11, 50), "end": dt(11, 59), "duration_s": 540.0,
               "distance_km": 1.0, "energy_kwh": None,
               "avg_speed_kph": 6.0, "start_soc": None, "end_soc": None,
               "open": True}
        ins, dels = agg._plan_group_writes(
            "trip", key, [seg], [], dt(9, 0), dt(12, 0), 600)
        self.assertEqual(len(ins), 1)
        self.assertTrue(ins[0]["open"])
        ins, dels = agg._plan_group_writes(
            "trip", key, [seg], [], dt(9, 0), dt(12, 10), 600)
        self.assertFalse(ins[0]["open"])


class ExactNsTest(unittest.TestCase):
    NS0 = 1789977727964984000  # int-ns cell, exact

    def test_int_ns_cells_keep_sub_microsecond_distinct(self):
        cols = ["event_time", "vehicle", "path", "source", "decode_epoch",
                "value_num", "value_bool", "unit"]
        rows = [
            [self.NS0, "v", "P.speed", "s", "e", 10.0, None, "m/s"],
            [self.NS0 + 250, "v", "P.speed", "s", "e", 10.0, None, "m/s"],
        ]
        paths = {"speed": "P.speed", "soc": "", "drive_energy": "",
                 "charge_energy": "", "power": "", "charging": ""}
        g = agg.group_vehicle_rows(cols, rows, paths)
        self.assertEqual(len(g[("v", "s", "e")]["speed"]), 2)

    def test_iso_string_keeps_nine_digits(self):
        self.assertEqual(
            agg.parse_ts_ns("2026-09-20 10:00:00.123456789"),
            agg.parse_ts_ns("2026-09-20 10:00:00.123456789"))
        self.assertNotEqual(
            agg.parse_ts_ns("2026-09-20 10:00:00.123456789"),
            agg.parse_ts_ns("2026-09-20 10:00:00.123456788"))
        self.assertIsNone(agg.parse_ts_ns(12.5))
        self.assertIsNone(agg.parse_ts_ns(True))

    def test_ns_datetime_mixed_segments_agree(self):
        t0, t1 = self.NS0, self.NS0 + 300_000_000_000
        by_ns = agg.segment_trips(
            [(t0, 36.0, None), (t1, 36.0, None)], gap_min=10)
        by_dt = agg.segment_trips(
            [(dt(10, 0), 36.0, None), (dt(10, 5), 36.0, None)], gap_min=10)
        self.assertAlmostEqual(
            by_ns[0]["duration_s"], by_dt[0]["duration_s"], places=6)

    def test_ns_stable_pk_and_serialization(self):
        pk = agg._segment_pk(("v", "s", "e"), self.NS0)
        self.assertIn(str(self.NS0), pk)
        lit = agg.ns_to_sql_lit(self.NS0)
        self.assertIn(".964984000", lit)
        self.assertEqual(agg.ns_to_sql_lit(None), "NULL")
        self.assertEqual(agg.ns_to_sql_lit(0), "'1970-01-01 00:00:00.000000000'")
        self.assertEqual(agg.ns_to_sql_lit(-1), "NULL")

    def test_integer_segments_accept_datetime_window_bounds(self):
        lower = datetime.fromtimestamp(self.NS0 // 1_000_000_000, timezone.utc)
        upper = lower + timedelta(seconds=1)
        for kind in ("trip", "charge"):
            segments = [{"start": self.NS0 + offset,
                         "end": self.NS0 + offset + 1, "open": False}
                        for offset in (100, 101)]
            rows, deletes = agg._plan_group_writes(
                kind, ("v", "can", "e"), segments, [], lower, upper, 60)
            self.assertEqual([row["start"] for row in rows],
                             [self.NS0 + 100, self.NS0 + 101])
            self.assertNotEqual(rows[0]["_pk"], rows[1]["_pk"])
            self.assertEqual(deletes, [])

    def test_events_across_windows_stay_separate(self):
        hour = self.NS0 - (self.NS0 % agg.HOUR_NS)
        late = hour - 1  # previous window's last ns
        self.assertEqual(agg.floor_hour_ns(late), hour - agg.HOUR_NS)
        self.assertEqual(agg.floor_hour_ns(hour), hour)


class CoordinatorLogicTest(unittest.TestCase):
    def test_pending_dirty_skips_same_generation_across_revisions(self):
        key = (("v", "can", "e"), 100)
        dirty = {key: "gen1"}
        self.assertEqual(agg._pending_dirty({}, {}), {})
        self.assertEqual(agg._pending_dirty(dirty, {key: ("gen1", "old")}), {})
        self.assertEqual(agg._pending_dirty(dirty, {key: ("gen0", "rev")}), dirty)
        self.assertEqual(agg._pending_dirty(dirty, {}), dirty)

    def test_scope_predicate_and_tuple_are_exact(self):
        pred = agg._dirty_scope_predicate([("v", "can", "e")])
        self.assertIn("vehicle = 'v'", pred)
        self.assertIn("decode_epoch = 'e'", pred)
        self.assertEqual(agg._scope_tuple(("v", "can", "e")), ("v", "can", "e"))
        self.assertIsNone(agg._scope_tuple(("v", "can")))
        self.assertIsNone(agg._scope_tuple(("", "can", "e")))
        self.assertTrue(agg._is_can_scope(("v", "can", "e")))
        self.assertFalse(agg._is_can_scope(("v", "fleet", "e")))

    def test_config_revision_covers_tuning_and_battery_code(self):
        base = {"paths": {"speed": "A"}, "trip_gap_min": 10,
                "trip_min_speed_kph": 1.0, "charge_gap_min": 30,
                "signal_max_age_s": 300, "battery_config": "/nonexistent"}
        changed = dict(base, trip_gap_min=11)
        self.assertNotEqual(agg._config_revision(base),
                            agg._config_revision(changed))

    def test_legacy_cfgs_exclude_only_can_scopes(self):
        cfg = {"exclude_scopes": [], "battery_exclude_scopes": []}
        coordinated = [("v", "can", "e"), ("w", "fleet", "f")]
        legacy, trip, bat = agg._legacy_cfgs(cfg, coordinated)
        self.assertEqual(legacy["exclude_scopes"], [("v", "can", "e")])
        self.assertEqual(trip["exclude_scopes"], [("v", "can", "e")])
        self.assertEqual(bat["battery_exclude_scopes"], [("v", "can", "e")])
        self.assertEqual(bat["battery_exclude_sources"], ["can"])
        plain, _, _ = agg._legacy_cfgs(cfg, [("w", "fleet", "f")])
        self.assertEqual(plain["exclude_scopes"], [])
        self.assertEqual(agg._legacy_cfgs(cfg, [])[2]["battery_exclude_sources"], ["can"])


class DirtyBatchTest(unittest.TestCase):
    def test_exact_holes_and_scopes_are_preserved(self):
        import sqlite3
        from unittest.mock import patch
        with sqlite3.connect(":memory:") as db:
            db.create_function("date_bin", 2, lambda interval, stamp:
                               stamp[:13] + ":00:00.000000000")
            db.execute("""CREATE TABLE vehicle_signal (
                event_time TEXT, vehicle TEXT, path TEXT, source TEXT,
                decode_epoch TEXT, source_field TEXT, quality TEXT,
                unit TEXT, value_num REAL)""")
            db.execute("""CREATE TABLE vehicle_agg (
                window_start TEXT, resolution TEXT, vehicle TEXT, path TEXT,
                source TEXT, decode_epoch TEXT, source_field TEXT, quality TEXT,
                unit_key TEXT, avg_value REAL, min_value REAL, max_value REAL,
                sample_count INTEGER, unit TEXT, rollup_version INTEGER)""")
            db.executemany("INSERT INTO vehicle_signal VALUES "
                           "(?, ?, 'speed', 'can', 'e', 'speed', 'ok', 'km/h', ?)",
                           [("2026-09-20 10:00:00.000000000", "v", 10),
                            ("2026-09-20 11:00:00.000000000", "v", 999),
                            ("2026-09-20 12:00:00.000000000", "v", 999),
                            ("2026-09-20 10:00:00.000000000", "window_start", 999),
                            ("2026-09-20 12:00:00.000000000", "window_start", 30)])
            hour = agg.parse_ts_ns("2026-09-20 10:00:00")
            keys = [(("v", "can", "e"), hour),
                    (("window_start", "can", "e"), hour + 2 * agg.HOUR_NS)]
            def execute(_url, _auth, _database, statement, **_kwargs):
                db.execute(statement.replace("::INTERVAL", ""))
            with patch.object(agg, "request_sql", execute), \
                    patch.object(agg, "guarded_fetch", return_value=([], [])):
                agg._rollup_hour_windows(("", "", ""), {"max_rows": 200000}, keys)
            self.assertEqual(db.execute(
                "SELECT vehicle, window_start, avg_value, sample_count "
                "FROM vehicle_agg WHERE resolution='1h' ORDER BY vehicle"
            ).fetchall(), [
                ("v", "2026-09-20 10:00:00.000000000", 10, 1),
                ("window_start", "2026-09-20 12:00:00.000000000", 30, 1)])

    def test_generation_changed_during_batch_is_not_checkpointed(self):
        from unittest.mock import patch
        key = (("v", "can", "e"), agg.parse_ts_ns("2026-09-20 10:00:00"))
        with patch.object(agg, "_load_dirty", return_value={key: "g"}), \
                patch.object(agg, "_load_checkpoints", return_value={}), \
                patch.object(agg, "_read_dirty_generation", side_effect=["g", "new"]), \
                patch.object(agg, "_config_revision", return_value="rev"), \
                patch.object(agg, "_rollup_hour_windows", return_value={key[1]}), \
                patch.object(agg, "_trip_charge_scope_windows") as trip, \
                patch.object(agg, "_commit_checkpoint") as commit:
            self.assertEqual(agg.vehicle_history_section(
                ("", "", ""), {}, datetime(2026, 9, 20, 14)), 0)
        trip.assert_not_called()
        commit.assert_not_called()


class RollupV2SqlTest(unittest.TestCase):
    def test_rollup_preserves_quality_nulls_and_corrected_scope(self):
        import sqlite3
        from unittest.mock import patch

        with sqlite3.connect(":memory:") as db:
            def date_bin(interval, timestamp):
                stamp = datetime.fromisoformat(timestamp)
                if interval == "1h":
                    stamp = stamp.replace(minute=0)
                return stamp.replace(second=0, microsecond=0).strftime(
                    "%Y-%m-%d %H:%M:%S.000000000")

            db.create_function("date_bin", 2, date_bin)
            db.execute("""CREATE TABLE vehicle_signal (
                event_time TEXT, vehicle TEXT, path TEXT, source TEXT,
                decode_epoch TEXT, source_field TEXT, quality TEXT,
                unit TEXT, value_num REAL)""")
            db.execute("""CREATE TABLE vehicle_agg (
                window_start TEXT, resolution TEXT, vehicle TEXT, path TEXT,
                source TEXT, decode_epoch TEXT, source_field TEXT, quality TEXT,
                unit_key TEXT, avg_value REAL, min_value REAL, max_value REAL,
                sample_count INTEGER, unit TEXT, rollup_version INTEGER)""")
            db.executemany(
                "INSERT INTO vehicle_signal VALUES (?, ?, 'speed', 'can', 'e',"
                " 'speed', ?, ?, ?)",
                [("2026-09-20 10:00:10.000000000", "v", "ok", "km/h", 10),
                 ("2026-09-20 10:00:20.000000000", "v", "ok", "km/h", 20),
                 ("2026-09-20 10:00:30.000000000", "v", "invalid", "km/h", 999),
                 ("2026-09-20 10:00:40.000000000", "v", "ok", "m/s", 5),
                 ("2026-09-20 10:01:00.000000000", "v", "ok", "km/h", None),
                 ("2026-09-20 10:00:10.000000000", "other", "ok", "km/h", 75)])

            def execute(_url, _auth, _database, statement, **_kwargs):
                db.execute(statement.replace("::INTERVAL", ""))

            hour = agg.parse_ts_ns("2026-09-20 10:00:00")
            scopes = [(("v", "can", "e"), hour),
                      (("other", "can", "e"), hour)]
            with patch.object(agg, "request_sql", execute):
                agg._rollup_hour_windows(("", "", ""), {}, scopes)
                rows = db.execute("""SELECT quality, unit, avg_value, min_value,
                    max_value, sample_count, rollup_version FROM vehicle_agg
                    WHERE vehicle='v' AND resolution='1m'
                    ORDER BY window_start, quality, unit""").fetchall()
                self.assertEqual(rows, [
                    ("invalid", "km/h", 999, 999, 999, 1, 2),
                    ("ok", "km/h", 15, 10, 20, 2, 2),
                    ("ok", "m/s", 5, 5, 5, 1, 2),
                    ("ok", "km/h", None, None, None, 0, 2)])
                db.execute("UPDATE vehicle_signal SET quality='ok'"
                           " WHERE vehicle='v' AND quality='invalid'")
                agg._rollup_hour_windows(("", "", ""), {}, scopes[:1])
                self.assertEqual(db.execute(
                    "SELECT COUNT(*) FROM vehicle_agg WHERE quality='invalid'"
                ).fetchone(), (0,))
                self.assertEqual(db.execute(
                    "SELECT avg_value, sample_count FROM vehicle_agg"
                    " WHERE vehicle='other' ORDER BY resolution"
                ).fetchall(), [(75, 1), (75, 1)])
                db.execute("""INSERT INTO vehicle_agg
                    (window_start,resolution,vehicle,path,source,decode_epoch,
                     avg_value,sample_count)
                    VALUES ('2026-09-20 10:00:00.000000000','1h',
                            'v','speed','can','e',999,10)""")

                def interrupted(url, auth, database, statement, **kwargs):
                    if statement.startswith("INSERT") and "date_bin('1h'" in statement:
                        raise agg.SqlError("injected second-resolution failure")
                    return execute(url, auth, database, statement, **kwargs)

                with patch.object(agg, "request_sql", interrupted):
                    with self.assertRaises(agg.SqlError):
                        agg._rollup_hour_windows(("", "", ""), {}, scopes[:1])
                self.assertEqual(db.execute(
                    "SELECT avg_value FROM vehicle_agg WHERE rollup_version IS NULL"
                ).fetchall(), [(999,)])
                agg._rollup_hour_windows(("", "", ""), {}, scopes[:1])
                self.assertEqual(db.execute(
                    "SELECT COUNT(*) FROM vehicle_agg WHERE rollup_version IS NULL"
                ).fetchone(), (0,))
                # The coordinator must invoke real SQL rollups for a notification.
                key = scopes[0]
                committed = []
                with patch.object(agg, "_load_dirty", return_value={key: "new"}), \
                        patch.object(agg, "_load_checkpoints", return_value={}), \
                        patch.object(agg, "_read_dirty_generation", return_value="new"), \
                        patch.object(agg, "_config_revision", return_value="rev"), \
                        patch.object(agg, "_trip_charge_scope_windows", return_value={}), \
                        patch.object(agg, "battery_section", side_effect=AssertionError("automatic CAN battery")), \
                        patch.object(agg, "_commit_checkpoint",
                                     side_effect=lambda *args: committed.append(args)):
                    db.execute("DELETE FROM vehicle_agg WHERE vehicle='v'")
                    self.assertEqual(agg.vehicle_history_section(
                        ("", "", ""), {}, datetime(2026, 9, 20, 12)), 1)
                self.assertEqual(len(committed), 1)
                self.assertEqual(db.execute(
                    "SELECT SUM(sample_count) FROM vehicle_agg"
                    " WHERE vehicle='v' AND resolution='1h'"
                ).fetchone(), (4,))
                # Legacy pass processes fleet but leaves unregistered CAN untouched.
                db.execute("""INSERT INTO vehicle_signal VALUES
                    ('2026-09-20 10:00:10.000000000','fleet-v','speed','fleet',
                     NULL,'speed','ok','km/h',50)""")
                with patch.object(agg, "utcnow", return_value=datetime(2026, 9, 20, 12)), \
                        patch.object(agg, "_flushed_floor_ns", return_value=hour), \
                        patch.object(agg, "_migrate_legacy_hours", return_value=0):
                    agg._vehicle_rollup(("", "", ""), {"lookback_h": 30})
                self.assertEqual(db.execute(
                    "SELECT SUM(sample_count) FROM vehicle_agg"
                    " WHERE vehicle='v' AND resolution='1h'"
                ).fetchone(), (4,))
                self.assertEqual(db.execute(
                    "SELECT avg_value FROM vehicle_agg"
                    " WHERE vehicle='fleet-v' ORDER BY resolution"
                ).fetchall(), [(50, ), (50, )])


if __name__ == "__main__":
    unittest.main()
