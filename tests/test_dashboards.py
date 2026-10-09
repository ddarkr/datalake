"""Execute dashboard coverage SQL: missing input must not look healthy."""
import json
from pathlib import Path
import sqlite3

import yaml

ROOT = Path(__file__).resolve().parent.parent


def test_coverage_distinguishes_absence_from_observed_zero():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    queries = []
    for name in ("grafana-dash-ai-usage", "grafana-dash-ai-sessions"):
        dashboard = json.loads(configs[name]["content"])
        panel = next(p for p in dashboard["panels"] if p["id"] == 3)
        query = panel["targets"][0]["rawSql"].replace("$$", "$")
        queries.append(query.replace("$__timeFilter(timestamp)", "timestamp BETWEEN 1000 AND 2000")
                       .replace("$__unixEpochTo()", "2000"))
    with sqlite3.connect(":memory:") as db:
        db.create_function("FROM_UNIXTIME", 1, lambda value: value)
        db.execute('''CREATE TABLE opentelemetry_traces (
            timestamp INTEGER, trace_id TEXT, span_id TEXT,
            "span_attributes.gen_ai.usage.input_tokens" INTEGER,
            "span_attributes.gen_ai.usage.output_tokens" INTEGER,
            "span_attributes.coding_agent.session.id" TEXT,
            "span_attributes.session.id" TEXT,
            "span_attributes.conversation.id" TEXT,
            "span_attributes.gen_ai.conversation.id" TEXT
        )''')
        assert [db.execute(q).fetchone()[0] for q in queries] == [None, None]
        # A measured zero token count is present data, not a missing attribute.
        db.execute("INSERT INTO opentelemetry_traces VALUES (1500, 'trace', 'good', 0, 0, 'session', NULL, NULL, NULL)")
        assert [db.execute(q).fetchone()[0] for q in queries] == [0, 0]
        # Retransmission must not multiply missing spans; out-of-range rows do not count.
        db.executemany("INSERT INTO opentelemetry_traces VALUES (?, 'trace', 'missing', NULL, NULL, NULL, NULL, NULL, NULL)",
                       [(1500,), (1500,), (3000,)])
        assert [db.execute(q).fetchone()[0] for q in queries] == [1, 1]

def test_known_cost_total_adds_supplemental_without_zero_filling_unknown():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-ai-usage"]["content"])
    panel = next(p for p in dashboard["panels"] if p["id"] == 12)
    query = panel["targets"][0]["rawSql"].replace("$$", "$")
    query = query.replace("$__timeFilter(day_start)", "day_start BETWEEN 1000 AND 2000")
    with sqlite3.connect(":memory:") as db:
        db.execute("""CREATE TABLE ai_daily_summary (
            day_start INTEGER, cost_usd REAL,
            cost_estimated_usd REAL, cost_unpriced_calls INTEGER)""")
        # Legacy/unprocessed row stays fully unknown.
        db.execute("INSERT INTO ai_daily_summary VALUES (1500, NULL, NULL, NULL)")
        assert db.execute(query).fetchone() == (None, None, None, None)
        # Mixed reported + supplemental, explicit zero kept, unresolved counted.
        db.execute("INSERT INTO ai_daily_summary VALUES (1500, 0.0, NULL, 0)")
        db.execute("INSERT INTO ai_daily_summary VALUES (1500, 0.02, 0.015, 2)")
        total, reported, supplemental, unpriced = db.execute(query).fetchone()
        assert reported == 0.02 and supplemental == 0.015 and unpriced is None
        assert abs(total - 0.035) < 1e-9
        # Unknown legacy coverage must not disappear beside evaluated rows.
        db.execute("DELETE FROM ai_daily_summary WHERE cost_unpriced_calls IS NULL")
        assert db.execute(query).fetchone()[3] == 2
        # Supplemental-only row: total equals the estimate, reported stays NULL.
        with sqlite3.connect(":memory:") as only:
            only.execute("""CREATE TABLE ai_daily_summary (
                day_start INTEGER, cost_usd REAL,
                cost_estimated_usd REAL, cost_unpriced_calls INTEGER)""")
            only.execute("INSERT INTO ai_daily_summary VALUES (1500, NULL, 0.015, 0)")
            assert only.execute(query).fetchone() == (0.015, None, 0.015, 0)

def _all_battery_panels(panels):
    for panel in panels:
        yield panel
        yield from _all_battery_panels(panel.get("panels") or [])


def _battery_query(configs, panel_id):
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    panel = next(p for p in _all_battery_panels(dashboard["panels"]) if p["id"] == panel_id)
    query = panel["targets"][0]["rawSql"].replace("$$", "$")
    return (query.replace("$__timeFilter(window_start)", "window_start BETWEEN 1000 AND 2000")
            .replace("$__timeFilter(event_time)", "event_time BETWEEN 1000 AND 2000")
            .replace("$__unixEpochTo()", "2000").replace("$__unixEpochFrom()", "1000")
            .replace("${vehicle:sqlstring}", "''").replace("${source:sqlstring}", "''")
            .replace("${vehicle_ids:sqlstring}", "''")
            .replace("${epoch:sqlstring}", "''").replace("${status:sqlstring}", "''")
            .replace("${metric:sqlstring}", "'battery.electrical.soc_ekf_pct'"))


def _battery_db():
    db = sqlite3.connect(":memory:")
    db.create_function("FROM_UNIXTIME", 1, lambda value: value)
    db.execute('''CREATE TABLE vehicle_analysis (
        window_start TIMESTAMP, window_end TIMESTAMP, vehicle TEXT,
        "metric" TEXT, source TEXT, analysis_id TEXT, revision TEXT,
        "value" REAL, value_text TEXT, unit TEXT, status TEXT, reason TEXT,
        decode_epoch TEXT, evidence_count INTEGER, sample_count INTEGER,
        coverage_ratio REAL, algorithm_version TEXT, calibration_version TEXT,
        model_version TEXT, uncertainty REAL, uncertainty_lower REAL,
        uncertainty_upper REAL, computed_at TIMESTAMP, quality TEXT,
        config_version TEXT, connectivity TEXT, episode_id TEXT)''')
    db.execute('''CREATE TABLE vehicle_event (
        event_time TIMESTAMP, vehicle TEXT, event_type TEXT, name TEXT,
        source TEXT, event_id TEXT, ingest_time TIMESTAMP, envelope_id TEXT,
        started_at TIMESTAMP, ended_at TIMESTAMP, duration_s REAL,
        audience TEXT, is_active INTEGER, body_redacted INTEGER,
        source_system TEXT, decode_epoch TEXT, collector_id TEXT,
        episode_id TEXT, quality TEXT, config_version TEXT,
        connectivity TEXT)''')
    db.execute('''CREATE TABLE vehicle_signal (
        event_time TIMESTAMP, vehicle TEXT, source TEXT,
        decode_epoch TEXT, source_field TEXT, value_num REAL,
        unit TEXT, quality TEXT, ingest_time TIMESTAMP,
        envelope_id TEXT, path TEXT, value_text TEXT, value_bool BOOLEAN)''')
    db.execute('''CREATE TABLE vehicle_identity (
        mapped_at TIMESTAMP, vehicle TEXT, canonical_vehicle TEXT)''')
    return db


def test_battery_dashboard_latest_revision_wins_before_status_filter():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _battery_query(configs, 6)
    with _battery_db() as db:
        # Empty analyses show unknown, never a healthy zero.
        assert db.execute(query).fetchall() == []
        assert db.execute(_battery_query(configs, 3)).fetchone() == (None,)
        assert db.execute(_battery_query(configs, 5)).fetchone() == (None, None, None, None)
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1500, 1600, 'v', 'battery.energy.x', 'can', 'battery_energy',
             'r1', 42.0, NULL, 'kWh', 'derived', 'ok', 'e1',
             5, 5, 0.9, 'a', 'c', 'm', NULL, NULL, NULL, 10,
             NULL, NULL, NULL, NULL)""")
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1500, 1600, 'v', 'battery.energy.x', 'can', 'battery_energy',
             'r2', NULL, NULL, NULL, 'unavailable', 'sparse', 'e1',
             0, 5, 0.1, 'a', NULL, NULL, NULL, NULL, NULL, 20,
             NULL, NULL, NULL, NULL)""")
        # A fresh unavailable revision replaces the stale good value: the
        # authoritative row is NULL/unavailable, and the degraded counter sees it.
        rows = db.execute(query).fetchall()
        assert len(rows) == 1
        assert rows[0][5] is None and rows[0][8] == "unavailable"
        assert db.execute(_battery_query(configs, 3)).fetchone() == (1,)
        # Same metric on another epoch stays a separate current row.
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1500, 1600, 'v', 'battery.energy.x', 'can', 'battery_energy',
             'r3', 7.0, NULL, 'kWh', 'derived', 'ok', 'e2',
             5, 5, 0.9, 'a', 'c', 'm', NULL, NULL, NULL, 30,
             NULL, NULL, NULL, NULL)""")
        assert len(db.execute(query).fetchall()) == 2


def test_battery_dashboard_warning_overlap_keeps_started_before_range():
    """Alerts-only lifecycle: open needs valid active evidence."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    ledger = _battery_query(configs, 18)
    state = _battery_query(configs, 19)
    counts = _battery_query(configs, 5)
    with _battery_db() as db:
        assert db.execute(ledger).fetchall() == []
        assert db.execute(state).fetchall() == []
        assert db.execute(counts).fetchone() == (None, None, None, None)
        db.execute("""INSERT INTO vehicle_event VALUES
            (1100, 'v', 'alerts', 'w', 'can', 'e-a', 1100, 'env',
             1100, NULL, NULL, 'owner', 1, 0, 's', 'e1', 'c', 'ep-active',
             NULL, NULL, NULL)""")
        db.execute("""INSERT INTO vehicle_event VALUES
            (1200, 'v', 'alerts', 'w', 'can', 'e-b', 1200, 'env',
             1200, 1300, 100.0, 'owner', 0, 0, 's', 'e1', 'c', 'ep-closed',
             NULL, NULL, NULL)""")
        db.execute("""INSERT INTO vehicle_event VALUES
            (1400, 'v', 'alerts', 'w', 'can', 'e-c', 1400, 'env',
             1400, NULL, NULL, 'owner', NULL, 0, 's', 'e1', 'c', 'ep-badonly',
             'error', NULL, NULL)""")
        db.execute("""INSERT INTO vehicle_event VALUES
            (1500, 'v', 'alerts', 'w', 'can', 'e-d', 1500, 'env',
             1500, 1400, NULL, 'owner', 0, 0, 's', 'e1', 'c', 'ep-reversed',
             NULL, NULL, NULL)""")
        db.execute("""INSERT INTO vehicle_event VALUES
            (900, 'v', 'alerts', 'w', 'can', 'e-e', 900, 'env',
             500, NULL, NULL, 'owner', NULL, 0, 's', 'e1', 'c', 'ep-stale',
             NULL, NULL, NULL)""")
        db.execute("""INSERT INTO vehicle_event VALUES
            (1500, 'v', 'errors', 'boom', 'can', 'x1', 1500, 'env', NULL, NULL,
             NULL, 'owner', 0, 0, 's', 'e1', 'c', NULL, NULL, NULL, NULL)""")
        assert db.execute(counts).fetchone() == (4, 4, 1, None)
        rows = {r[0]: r for r in db.execute(state).fetchall()}
        assert rows["ep-active"][10] == "open"
        assert rows["ep-closed"][10] == "closed" and rows["ep-closed"][12] == 1300.0
        assert rows["ep-reversed"][10] == "conflict_time" and rows["ep-reversed"][12] is None
        assert rows["ep-stale"][10] == "unknown_activity"
        db.execute("""INSERT INTO vehicle_event VALUES
            (2600, 'v', 'alerts', 'w', 'can', 'e-late', 2600, 'env',
             1100, NULL, NULL, 'owner', 1, 0, 's', 'e1', 'c', 'ep-closed',
             NULL, NULL, NULL)""")
        rows = {r[0]: r for r in db.execute(state).fetchall()}
        assert rows["ep-closed"][10] == "conflict_active"
        assert rows["ep-closed"][12] is None and rows["ep-closed"][13] is None


def test_battery_cards_raw_display_latest_valid_only():
    """Panel 25 shows the newest BatteryLevel report; a bad newest stays NULL."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _battery_query(configs, 25)
    with _battery_db() as db:
        # No reports: no rows (unknown), never a zero fill.
        assert db.execute(query).fetchall() == []
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1500, 'v', 'fleet', 'e1', 'BatteryLevel', 62.5, '%', NULL, 1500, 'env-a')""")
        row = db.execute(query).fetchone()
        assert row[:3] == ('v', 'fleet', 'e1')
        assert row[3] == 1500 and row[4] == 62.5
        # A newer invalid tombstone blocks the stale good value: row stays, reading NULL.
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1600, 'v', 'fleet', 'e1', 'BatteryLevel', NULL, '%', 'invalid', 1600, 'env-b')""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 1 and rows[0][4] is None
        # A newer wrong-unit report is not a percent either: still NULL.
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1700, 'v', 'fleet', 'e1', 'BatteryLevel', 62.5, 'count', NULL, 1700, 'env-c')""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 1 and rows[0][4] is None
        # Other scopes stay separate; the BMS Soc field never leaks into this card.
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1700, 'v', 'fleet', 'e2', 'BatteryLevel', 70.0, '%', 'ok', 1700, 'env-d')""")
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1700, 'v', 'fleet', 'e1', 'Soc', 68.0, '%', NULL, 1700, 'env-e')""")
        rows = {r[2]: r for r in db.execute(query).fetchall()}
        assert set(rows) == {'e1', 'e2'}
        assert rows['e1'][4] is None and rows['e2'][4] == 70.0


def test_battery_cards_latest_window_energy_not_lifetime():
    """Panel 30 shows the newest window's energy; a bad newest never resurrects older goods."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _battery_query(configs, 30)
    metric = 'battery.energy.discharge_energy_kwh'
    with _battery_db() as db:
        assert db.execute(query).fetchall() == []
        db.execute(f"""INSERT INTO vehicle_analysis VALUES
            (1200, 1300, 'v', '{metric}', 'fleet', 'battery_energy',
             'r1', 3.0, NULL, 'kWh', 'derived', 'ok', 'e1',
             5, 5, 0.9, 'a', 'c', 'm', NULL, NULL, NULL, 10,
             NULL, NULL, NULL, NULL)""")
        db.execute(f"""INSERT INTO vehicle_analysis VALUES
            (1500, 1600, 'v', '{metric}', 'fleet', 'battery_energy',
             'r2', 5.0, NULL, 'kWh', 'derived', 'ok', 'e1',
             5, 5, 0.9, 'a', 'c', 'm', NULL, NULL, NULL, 20,
             NULL, NULL, NULL, NULL)""")
        # Latest window wins: its value and window, not a lifetime sum.
        rows = db.execute(query).fetchall()
        assert len(rows) == 1
        assert rows[0][3] == 1500 and rows[0][4] == 5.0
        # A newer unavailable revision on the latest window blocks it; older goods stay buried.
        db.execute(f"""INSERT INTO vehicle_analysis VALUES
            (1500, 1600, 'v', '{metric}', 'fleet', 'battery_energy',
             'r3', NULL, NULL, NULL, 'unavailable', 'sparse', 'e1',
             0, 5, 0.1, 'a', NULL, NULL, NULL, NULL, NULL, 30,
             NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 1 and rows[0][4] is None
        # Another epoch stays a separate current row.
        db.execute(f"""INSERT INTO vehicle_analysis VALUES
            (1500, 1600, 'v', '{metric}', 'fleet', 'battery_energy',
             'r4', 7.0, NULL, 'kWh', 'derived', 'ok', 'e2',
             5, 5, 0.9, 'a', 'c', 'm', NULL, NULL, NULL, 40,
             NULL, NULL, NULL, NULL)""")
        rows = {r[2]: r for r in db.execute(query).fetchall()}
        assert set(rows) == {'e1', 'e2'}
        assert rows['e1'][4] is None and rows['e2'][4] == 7.0


def test_battery_soh_card_prefers_absolute_then_estimate_with_error():
    """Panel 33: full-discharge SOH wins; else the estimate carries its ±range."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = (_battery_query(configs, 33)
             .replace("CAST(ROUND(uncertainty, 1) AS STRING)", "ROUND(uncertainty, 1)")
             .replace("CAST(window_start AS TIMESTAMP(6))", "window_start"))
    with _battery_db() as db:
        db.create_function("CONCAT", 3, lambda a, b, c: f"{a}{b}{c}")
        assert db.execute(query).fetchall() == []
        est = 'battery.energy.soh_estimated_pct'
        db.execute(f"""INSERT INTO vehicle_analysis VALUES
            (1500, 1600, 'v', '{est}', 'fleet', 'battery_energy', 'r1', 91.5,
             NULL, '%', 'derived', 'ok', 'e1', 1, 1, NULL, 'a', 'c', NULL,
             1.24, 90.26, 92.74, 10, NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 1 and rows[0][5] == 91.5 and '±1.2%p' in rows[0][4]
        # An older absolute measurement still outranks a newer estimate.
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1200, 1300, 'v', 'battery.energy.soh_pct', 'fleet', 'battery_energy',
             'r2', 88.0, NULL, '%', 'derived', 'ok', 'e1', 1, 1, NULL, 'a', 'c',
             NULL, NULL, NULL, NULL, 20, NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 1 and rows[0][5] == 88.0 and rows[0][4] == '완전 방전 기준'
        # A newer unavailable absolute revision drops back to the estimate.
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1200, 1300, 'v', 'battery.energy.soh_pct', 'fleet', 'battery_energy',
             'r3', NULL, NULL, NULL, 'unavailable', 'sparse', 'e1', 0, 0, NULL,
             'a', NULL, NULL, NULL, NULL, NULL, 30, NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 1 and rows[0][5] == 91.5


def test_battery_period_totals_sum_latest_revision_per_window():
    """Panels 37-40: period totals add each window's newest derived kWh once."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    for panel, metrics in ((37, ("battery.energy.discharge_energy_kwh",
                                 "battery.energy.parked_discharge_kwh")),
                           (39, ("battery.energy.dc_charging_energy_in_kwh",))):
        query = _battery_query(configs, panel)
        bound = query.split("window_start >= ", 1)[1].split(" AND ", 1)[0]
        query = query.replace(bound, "1000")
        with _battery_db() as db:
            assert db.execute(query).fetchall() == []
            for ts, metric, rev, value, computed in (
                    (1200, metrics[0], "r1", 2.0, 10),
                    (1200, metrics[0], "r2", 3.0, 20),   # newer revision replaces r1
                    (1500, metrics[-1], "r3", 0.5, 10),
                    (500, metrics[0], "r4", 9.0, 10),    # before the period
                    (1800, "battery.energy.soh_pct", "r5", 99.0, 10)):
                db.execute(f"""INSERT INTO vehicle_analysis VALUES
                    ({ts}, {ts + 100}, 'v', '{metric}', 'fleet', 'battery_energy',
                     '{rev}', {value}, NULL, 'kWh', 'derived', 'ok', 'e1', 1, 1,
                     NULL, 'a', NULL, NULL, NULL, NULL, NULL, {computed}, NULL,
                     NULL, NULL, NULL)""")
            rows = db.execute(query).fetchall()
            # Newest r2 (3.0) replaces r1; r3 (0.5) is a different window.
            assert len(rows) == 1 and abs(rows[0][4] - 3.5) < 1e-9, (panel, rows)
            assert rows[0][3] == 2


def _physical_query(configs, dashboard_name, panel_id, start=1000, end=2000):
    dashboard = json.loads(configs[dashboard_name]["content"])
    panel = next(p for p in _all_battery_panels(dashboard["panels"])
                 if p["id"] == panel_id)
    return (panel["targets"][0]["rawSql"].replace("$$", "$")
            .replace("$__timeFilter(window_start)", f"window_start BETWEEN {start} AND {end}")
            .replace("$__timeFilter(s.event_time)", f"s.event_time BETWEEN {start} AND {end}")
            .replace("$__timeFilter(event_time)", f"event_time BETWEEN {start} AND {end}")
            .replace("$__unixEpochTo()", str(end))
            .replace("${vehicle:sqlstring}", "''")
            .replace("${vehicle_ids:sqlstring}", "''")
            .replace("${source:sqlstring}", "''")
            .replace("${epoch:sqlstring}", "''"))


def test_physical_cards_latest_analysis_and_new_raw_barriers():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    cards = (("grafana-dash-vehicle-overview", 6, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-drives", 7, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-charging", 6, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-battery", 42, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-battery", 46, "latest_pack_voltage_v", "V", 410.0))
    for dashboard, panel, suffix, unit, value in cards:
        query = _physical_query(configs, dashboard, panel, start=0, end=7800)
        with _battery_db() as db:
            db.create_function("FROM_UNIXTIME", 1, lambda epoch: epoch)
            assert db.execute(query).fetchall() == []
            def insert(ws, revision, reading, status, computed):
                db.execute("""INSERT INTO vehicle_analysis
                    (window_start, window_end, vehicle, metric, source,
                     analysis_id, revision, value, value_text, unit, status,
                     decode_epoch, computed_at) VALUES
                    (?, ?, 'v', ?, 'fleet', 'battery_energy', ?, ?, '7140',
                     ?, ?, 'fleet-v1', ?)""",
                           (ws, ws+3599, "battery.energy."+suffix, revision,
                            reading, unit, status, computed))
            insert(0, "older-window", value+1, "derived", 10000)
            # Unavailable identities in the newest hour may be omitted by runtime.
            # An older stored physical number must not fill that absence.
            assert db.execute(query).fetchone()[4] is None
            insert(3600, "good", value, "derived", 10001)
            assert db.execute(query).fetchone()[4] == value
            insert(3600, "uncalibrated", None, "unavailable", 10002)
            assert db.execute(query).fetchone()[4] is None
            insert(3600, "recalibrated", value, "derived", 10003)
            # A newer bad raw leg blocks old analyzed numbers before next job.
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (7500, 'v', 'fleet', 'fleet-v1', 'PackVoltage',
                 NULL, NULL, 'invalid', 9000, 'new-invalid')""")
            assert db.execute(query).fetchone()[4] is None
            # Configured analyzer field overrides must share the freshness barrier.
            db.execute("UPDATE vehicle_signal SET source_field = 'CustomVoltage'")
            assert db.execute(query).fetchone()[4] is None
            db.execute("UPDATE vehicle_signal SET event_time = 8500")
            assert db.execute(query).fetchone()[4] is None
            db.execute("DELETE FROM vehicle_signal")
            # Late-ingested same-time invalid observations also need reanalysis.
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (7140, 'v', 'fleet', 'fleet-v1', 'PackVoltage',
                 NULL, NULL, 'invalid', 11000, 'late-invalid')""")
            assert db.execute(query).fetchone()[4] is None
            db.execute("UPDATE vehicle_signal SET decode_epoch = 'other'")
            assert db.execute(query).fetchone()[4] == value


def test_physical_graphs_keep_signed_samples_and_latest_invalid_revision():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    for dashboard, panel in (("grafana-dash-vehicle-overview", 12),
                             ("grafana-dash-charging", 10),
                             ("grafana-dash-battery", 56)):
        query = _physical_query(configs, dashboard, panel)
        with _battery_db() as db:
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            for revision, value, status, computed in (
                    ("r1", -18.45, "derived", 2100),
                    ("r2", None, "unavailable", 2101)):
                db.execute("""INSERT INTO vehicle_analysis
                    (window_start, window_end, vehicle, metric, source,
                     analysis_id, revision, value, value_text, unit, status,
                     decode_epoch, computed_at) VALUES
                    (1500, 1600, 'v', 'battery.energy.latest_power_kw', 'fleet',
                     'battery_energy', ?, ?, '1550', 'kW', ?, 'fleet-v1', ?)""",
                           (revision, value, status, computed))
                row = db.execute(query).fetchone()
                assert row[0] == 1550 and row[2] == value


def test_raw_physical_cards_never_treat_fleet_scope_as_unit_calibration():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    for panel, field, unit in ((44, "BrickVoltageMax", "V"),
                               (45, "BrickVoltageMin", "V"),
                               (51, "ModuleTempMax", "celsius"),
                               (52, "ModuleTempMin", "celsius")):
        query = _physical_query(configs, "grafana-dash-battery", panel)
        with _battery_db() as db:
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            db.create_function("FROM_UNIXTIME", 1, lambda value: value)
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (1500, 'v', 'fleet', 'fleet-v1', ?, 4, NULL,
                 'unit_unverified', 1500, 'raw')""", (field,))
            row = db.execute(query).fetchone()
            index = 4 if panel == 52 else 3
            assert row[index] is None
            db.execute("UPDATE vehicle_signal SET unit = ?, quality = 'ok'", (unit,))
            assert db.execute(query).fetchone()[index] == 4
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (1600, 'v', 'fleet', 'fleet-v1', ?, NULL, ?,
                 'invalid', 1600, 'invalid')""", (field, unit))
            assert db.execute(query).fetchone()[index] is None


def _native_ts_query(configs, dashboard, panel, start=1000, end=2000):
    """_physical_query plus the native TIMESTAMP(9) transform.

    Greptime compares native timestamps; sqlite CAST is a no-op, so the
    harness translates CAST(x AS TIMESTAMP(9)) to NS_TIMESTAMP(x) backed by
    the real backend time parser (same transform, not a repinned result).
    """
    from scripts.analytics.battery import battery_runtime as battery_runtime

    query = _physical_query(configs, dashboard, panel, start=start, end=end)
    for alias, column in (("f", "raw_event_time"), ("c", "value_text"),
                          ("fmax", "raw_event_time"), ("cm", "value_text"),
                          ("fmin", "raw_event_time"), ("cn", "value_text")):
        query = query.replace(
            "CAST(%s.%s AS TIMESTAMP(9))" % (alias, column),
            "NS_TIMESTAMP(%s.%s)" % (alias, column))
    assert "TIMESTAMP(9)" not in query

    def ns_timestamp(value):
        if value is None or isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        return battery_runtime.parse_time_bound(value)

    return query, ns_timestamp


def test_calibrated_extrema_fallback_and_frontier():
    """Panels 44/45/51/52: explicit-unit raw wins; unit-NULL raw falls back
    to the scoped calibrated conditions metric only while the newest raw of
    that field stays calibratable and already analyzed; stale analysis
    behind newer raw, wrong units, bad revisions, same-timestamp tombstones,
    and unknown ingest stay NULL (no resurrection)."""
    from scripts.analytics.battery import battery_runtime as battery_runtime

    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    cases = ((44, "BrickVoltageMax", "battery.conditions.brick_max_v", "V", 3, 4.02),
             (45, "BrickVoltageMin", "battery.conditions.brick_min_v", "V", 3, 3.92),
             (51, "ModuleTempMax", "battery.conditions.module_temp_max_c", "celsius", 3, 25.5),
             (52, "ModuleTempMin", "battery.conditions.module_temp_min_c", "celsius", 4, 20.5))
    for panel, field, metric, unit, index, calibrated in cases:
        query, ns_timestamp = _native_ts_query(
            configs, "grafana-dash-battery", panel)
        observed = battery_runtime.ns_to_sql_ts(1500)
        with _battery_db() as db:
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            db.create_function("NS_TIMESTAMP", 1, ns_timestamp)
            assert db.execute(query).fetchall() == []
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (1500, 'v', 'fleet', 'fleet-v1', ?, 4, NULL,
                 'unit_unverified', 1500, 'raw')""", (field,))
            # Unit-unverified raw alone never renders a physical value.
            assert db.execute(query).fetchone()[index] is None
            db.execute("""INSERT INTO vehicle_analysis
                (window_start, window_end, vehicle, metric, source,
                 analysis_id, revision, value, value_text, unit, status,
                 reason, decode_epoch, computed_at) VALUES
                (1500, 1500, 'v', ?, 'fleet', 'battery_conditions', 'r1',
                 ?, ?, ?, 'derived',
                 'latest_calibrated_sample domain=d scope=v/fleet/fleet-v1;asof_ns=1500',
                 'fleet-v1', 1600)""", (metric, calibrated, observed, unit))
            row = db.execute(query).fetchone()
            assert row[index] == calibrated
            assert row[6] == observed
            # Explicit-unit raw wins over calibration.
            db.execute("UPDATE vehicle_signal SET unit = ?, quality = 'ok'", (unit,))
            row = db.execute(query).fetchone()
            assert row[index] == 4 and row[6] == "raw"
            db.execute("UPDATE vehicle_signal SET unit = NULL, quality = 'unit_unverified'")
            # 1ns-newer raw of the same field blocks stale calibration until reanalysis.
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (1501, 'v', 'fleet', 'fleet-v1', ?, 5, NULL,
                 'unit_unverified', 1501, 'newer')""", (field,))
            assert db.execute(query).fetchone()[index] is None
            db.execute("DELETE FROM vehicle_signal WHERE envelope_id = 'newer'")
            # A wrong-unit newest raw never revives calibration.
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (1600, 'v', 'fleet', 'fleet-v1', ?, 4100, 'mV',
                 'ok', 1600, 'wrong-unit')""", (field,))
            assert db.execute(query).fetchone()[index] is None
            db.execute("DELETE FROM vehicle_signal WHERE envelope_id = 'wrong-unit'")
            # A same-timestamp late tombstone blocks calibration until reanalysis.
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (1500, 'v', 'fleet', 'fleet-v1', ?, NULL, ?,
                 'invalid', 1700, 'tombstone')""", (field, unit))
            assert db.execute(query).fetchone()[index] is None
            db.execute("DELETE FROM vehicle_signal WHERE envelope_id = 'tombstone'")
            # Unknown ingest is fail-closed: analysis may not have seen this raw.
            db.execute("UPDATE vehicle_signal SET ingest_time = NULL")
            assert db.execute(query).fetchone()[index] is None
            db.execute("UPDATE vehicle_signal SET ingest_time = 1500")
            assert db.execute(query).fetchone()[index] == calibrated
            # Raw beyond the selected range end never blocks the historical view.
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (2500, 'v', 'fleet', 'fleet-v1', ?, 9, NULL,
                 'unit_unverified', 2500, 'future')""", (field,))
            assert db.execute(query).fetchone()[index] == calibrated
            db.execute("DELETE FROM vehicle_signal WHERE envelope_id = 'future'")
            # A bad newest revision (invalid/conflict/mismatch) never resurrects older goods.
            for revision, status, value, rev_unit in (
                    ("r2", "unavailable", None, unit),
                    ("r3", "error", None, None),
                    ("r4", "derived", 9.9, "wrong")):
                db.execute("""INSERT INTO vehicle_analysis
                    (window_start, window_end, vehicle, metric, source,
                     analysis_id, revision, value, value_text, unit, status,
                     reason, decode_epoch, computed_at) VALUES
                    (1500, 1500, 'v', ?, 'fleet', 'battery_conditions', ?,
                     ?, ?, ?, ?,
                     'terminal_invalid:latest_extreme_unmeasurable',
                     'fleet-v1', 1700)""",
                           (metric, revision, value, observed, rev_unit, status))
                assert db.execute(query).fetchone()[index] is None


def test_can_reports_keep_invalid_latest_and_scope_boundaries():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    cases = (
        (1, "x292.BMS_socUI", "%", 62, None, 4, 62),
        (2, "x352.BMS_nominalEnergyRemaining", "kWh", 38, None, 3, 38),
        (7, "x118.DI_gear", None, 4, "DI_GEAR_D", 3, "DI_GEAR_D"),
        (8, "x13D.CP_hvChargeStatus", None, 5, "CP_CHARGE_ENABLED", 3, "CP_CHARGE_ENABLED"),
        (9, "x20C.VCRIGHT_tempAmbientRaw", "°C", 18, None, 4, 18),
    )
    for panel_id, signal, unit, number, text, index, expected in cases:
        query = _physical_query(configs, "grafana-dash-vehicle-overview", panel_id)
        with _battery_db() as db:
            def insert(event, quality="reported_unverified", epoch="can-v1", path=None):
                db.execute("""INSERT INTO vehicle_signal
                    (event_time, vehicle, source, decode_epoch, source_field,
                     value_num, value_text, unit, quality, ingest_time, envelope_id, path)
                    VALUES (?, 'can-car', 'can', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                           (event, epoch, signal.split(".", 1)[1], number, text,
                            unit, quality, event, str(event), path or "Vehicle.CAN." + signal))
            insert(1500)
            assert db.execute(query).fetchone()[index] == expected
            # A same-named signal at a different CAN path is not an alias.
            insert(1550, path="Vehicle.CAN.other." + signal.split(".", 1)[1])
            assert db.execute(query).fetchone()[index] == expected
            insert(1600, "invalid")
            assert db.execute(query).fetchone()[index] is None
            insert(1650, epoch="can-v2")
            rows = {row[2]: row[index] for row in db.execute(query)}
            assert rows == {"can-v1": None, "can-v2": expected}
            if unit is not None:
                db.execute("UPDATE vehicle_signal SET unit = 'wrong'")
                assert all(row[index] is None for row in db.execute(query))
            else:
                db.execute("UPDATE vehicle_signal SET value_text = 'UNKNOWN(99)'")
                assert all(row[index] is None for row in db.execute(query))
            db.execute("UPDATE vehicle_signal SET source = 'fleet'")
            assert db.execute(query).fetchall() == []


def test_vehicle_coverage_separates_observation_from_receipt():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _physical_query(configs, "grafana-dash-vehicle-overview", 92)
    query = query.replace("$__timeFilter(ingest_time)", "ingest_time BETWEEN 1000 AND 2000")
    with _battery_db() as db:
        assert db.execute(query).fetchall() == []
        db.executemany("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, ingest_time) VALUES (?, ?, ?, ?, ?)""",
                       [(500, "can-car", "can", "old", 1500),
                        (1500, "can-car", "can", "new", 2500),
                        (1500, "fleet-car", "fleet", "fleet-v1", 1500)])
        rows = {(row[0], row[2]): row for row in db.execute(query)}
        assert rows["can-car", "old"][6:] == (0, 1)
        assert rows["can-car", "new"][6:] == (1, 0)
        assert rows["fleet-car", "fleet-v1"][6:] == (1, 1)


def test_can_soc_graph_rejects_wrong_units_and_keeps_null_gaps():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _physical_query(configs, "grafana-dash-vehicle-overview", 11)
    query = query.replace("date_bin(INTERVAL $__interval_ms MILLISECOND, event_time)", "event_time")
    with _battery_db() as db:
        db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
        db.executemany("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (?, 'can-car', 'can', 'can-v1', 'BMS_socUI',
             'Vehicle.CAN.x292.BMS_socUI', ?, ?, ?)""",
                       [(1100, 62, "%", "reported_unverified"),
                        (1200, 99, "wrong", "reported_unverified"),
                        (1300, 99, "%", "invalid")])
        assert [(row[0], row[2]) for row in db.execute(query)] == [(1100, 62), (1200, None), (1300, None)]


def test_vehicle_identity_canonical_selection_and_scope_split():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    variables = {v["name"]: v for v in dashboard["templating"]["list"]}
    panels = {p["id"]: p for p in _all_battery_panels(dashboard["panels"])}
    raw_ids_sql = "'demo-car'"

    def query(sql, source=""):
        return (sql.replace("$$", "$")
                .replace("${vehicle:sqlstring}", "'demo-car'")
                .replace("${vehicle_ids:sqlstring}", raw_ids_sql)
                .replace("${source:sqlstring}", repr(source))
                .replace("${epoch:sqlstring}", "''")
                .replace("$__timeFilter(window_start)", "window_start BETWEEN 1000 AND 2000")
                .replace("$__timeFilter(event_time)", "event_time BETWEEN 1000 AND 2000"))

    with _battery_db() as db:
        db.executemany("INSERT INTO vehicle_identity VALUES ('1970-01-01 00:00:00', ?, ?)",
                       [("demo-can", "demo-car"), ("demo-fleet", "demo-car")])
        scopes = [("demo-can", "can", "can-v1"),
                  ("demo-fleet", "fleet", "fleet-v1"),
                  ("demo-other", "can", "can-v1")]
        for vehicle, source, epoch in scopes:
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, ingest_time, path, value_num)
                VALUES (1500, ?, ?, ?, 1500, 'Vehicle.Speed', ?)""",
                       (vehicle, source, epoch, 42 if source == "can" else 44))
            db.execute("""INSERT INTO vehicle_analysis
                (window_start, window_end, vehicle, metric, source, analysis_id,
                 revision, value, unit, status, decode_epoch, computed_at)
                VALUES (1500, 1600, ?, 'battery.energy.discharge_energy_kwh',
                        ?, 'battery_energy', 'r1', 1.0, 'kWh', 'derived', ?, 10)""",
                       (vehicle, source, epoch))
        assert [r[0] for r in db.execute(variables["vehicle"]["query"])] == ["demo-car", "demo-other"]
        raw_ids = [r[0] for r in db.execute(query(variables["vehicle_ids"]["query"]))]
        assert set(raw_ids) == {"demo-car", "demo-can", "demo-fleet"}
        raw_ids_sql = ", ".join(repr(v) for v in raw_ids)
        unmapped = variables["vehicle_ids"]["query"].replace("$$", "$").replace(
            "${vehicle:sqlstring}", "'demo-other'")
        assert [r[0] for r in db.execute(unmapped)] == ["demo-other"]
        db.execute("INSERT INTO vehicle_identity VALUES (0, 'demo-new', 'demo-car')")
        refreshed = [r[0] for r in db.execute(query(variables["vehicle_ids"]["query"]))]
        assert set(refreshed) == {"demo-car", "demo-can", "demo-fleet", "demo-new"}
        assert {r[0] for r in db.execute(query(variables["source"]["query"]))} == {"", "can", "fleet"}
        hourly = panels[58]["targets"][0]["rawSql"]
        rows = db.execute(query(hourly)).fetchall()
        assert {(r[2], r[3], r[4], r[5]) for r in rows} == {
            ("demo-can", "can", "can-v1", -1.0),
            ("demo-fleet", "fleet", "fleet-v1", -1.0)}
        assert [(r[2], r[3], r[5]) for r in db.execute(query(hourly, "fleet"))] == [
            ("demo-fleet", "fleet", -1.0)]
        db.create_function("date_trunc", 2, lambda unit, value: value - value % 60)
        diagnostics = json.loads(configs["grafana-dash-dbc-health"]["content"])
        divergence = next(p for p in diagnostics["panels"] if p["id"] == 9)["targets"][0]["rawSql"]
        assert [(r[1], r[2], r[3], r[9]) for r in db.execute(query(divergence))] == [
            ("demo-car", "demo-can", "demo-fleet", -2.0)]
        # A later window under an existing raw ID needs no new mapping.
        db.execute("""INSERT INTO vehicle_analysis
            (window_start, window_end, vehicle, metric, source, analysis_id,
             revision, value, unit, status, decode_epoch, computed_at)
            VALUES (1700, 1800, 'demo-can', 'battery.energy.discharge_energy_kwh',
                    'can', 'battery_energy', 'r2', 2.0, 'kWh', 'derived', 'can-v1', 20)""")
        assert sorted((r[0], r[2], r[5]) for r in db.execute(query(hourly))) == [
            (1500, "demo-can", -1.0), (1500, "demo-fleet", -1.0),
            (1700, "demo-can", -2.0)]


def test_coverage_frontier_lists_scopes_without_range_rows():
    """Panel 92: whole-history frontier lists scopes even with zero range rows."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _physical_query(configs, "grafana-dash-vehicle-overview", 92)
    query = query.replace("$__timeFilter(ingest_time)", "ingest_time BETWEEN 1000 AND 2000")
    with _battery_db() as db:
        db.executemany("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, ingest_time) VALUES (?, ?, ?, ?, ?)""",
                       [(100, "stale-car", "can", "old", 100),
                        (1500, "can-car", "can", "new", 2500)])
        rows = {(row[0], row[2]): row for row in db.execute(query)}
        assert rows["stale-car", "old"][6:] == (0, 0)
        assert rows["can-car", "new"][6:] == (1, 0)


def test_transition_timeline_starts_from_boundary_sample():
    """Gear timeline: latest pre-range row anchors the series; tombstones stay NULL."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _physical_query(configs, "grafana-dash-vehicle-overview", 14)
    query = (query.replace("$__timeFilter(event_time)", "event_time BETWEEN 1000 AND 2000")
             .replace("$__unixEpochFrom()", "1000"))
    with _battery_db() as db:
        db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
        db.create_function("FROM_UNIXTIME", 1, lambda value: value)
        for event, text, quality in (
                (100, "DI_GEAR_N", "reported_unverified"),
                (500, "DI_GEAR_P", "reported_unverified"),
                (1100, "DI_GEAR_P", "reported_unverified"),
                (1300, "UNKNOWN(9)", "reported_unverified"),
                (1400, "DI_GEAR_D", "invalid"),
                (1500, "DI_GEAR_D", "reported_unverified")):
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, value_text, unit, quality, ingest_time, envelope_id, path)
                VALUES (?, 'v', 'can', 'e1', 'DI_gear', NULL, ?, NULL, ?, ?, ?, 'Vehicle.CAN.x118.DI_gear')""",
                       (event, text, quality, event, str(event)))
        rows = [(row[0], row[2]) for row in db.execute(query)]
        assert rows == [(500, "DI_GEAR_P"), (1100, "DI_GEAR_P"),
                        (1300, None), (1400, None), (1500, "DI_GEAR_D")]


def test_cell_frequency_respects_selected_source_and_epoch():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    with _battery_db() as db:
        db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
        db.create_function("floor", 1, lambda value: int(value))
        for panel_id, field in ((48, "NumBrickVoltageMin"), (50, "NumBrickVoltageMax")):
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field, value_num, quality)
                VALUES (1500, 'fleet-car', 'fleet', 'fleet-v1', ?, 3, 'ok')""", (field,))
            raw = next(p for p in dashboard["panels"] if p["id"] == panel_id)["targets"][0]["rawSql"]

            def query(source, epoch):
                return (raw.replace("$$", "$")
                        .replace("$__timeFilter(event_time)", "event_time BETWEEN 1000 AND 2000")
                        .replace("${vehicle:sqlstring}", "''")
                        .replace("${vehicle_ids:sqlstring}", "''")
                        .replace("${source:sqlstring}", repr(source))
                        .replace("${epoch:sqlstring}", repr(epoch)))

            assert db.execute(query("can", "")).fetchall() == []
            assert db.execute(query("fleet", "other-epoch")).fetchall() == []
            rows = db.execute(query("fleet", "fleet-v1")).fetchall()
            assert rows[0][1] == 1


def test_can_latest_cards_keep_invalid_reports_and_separate_epochs():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-can-battery"]["content"])
    raw = next(p for p in dashboard["panels"] if p["id"] == 20)["targets"][0]["rawSql"]
    query = (raw.replace("$$", "$")
             .replace("$__timeFilter(event_time)", "event_time BETWEEN 1000 AND 2000")
             .replace("${vehicle:sqlstring}", "'car'")
             .replace("${vehicle_ids:sqlstring}", "'car'")
             .replace("${epoch:sqlstring}", "''"))
    with _battery_db() as db:
        db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
        db.executemany("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (?, ?, ?, ?, 'BMS_socUI', 'Vehicle.CAN.x292.BMS_socUI', ?, ?, ?, ?, 'env')""",
                       [(1100, "car", "can", "e1", 60, "%", "reported_unverified", 1100),
                        (1200, "car", "can", "e1", 70, "%", "invalid", 1200),
                        (1300, "car", "can", "e2", 55, "%", "reported_unverified", 1300),
                        (1400, "other", "can", "e3", 99, "%", "reported_unverified", 1400),
                        (1500, "car", "fleet", "e4", 88, "%", "reported_unverified", 1500)])
        rows = db.execute(query).fetchall()
        assert len(rows) == 2
        assert {row[0].split()[-1]: row[1] for row in rows} == {"e1": None, "e2": 55}
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1600, 'car', 'can', 'e2', 'BMS_socUI', 'Vehicle.CAN.x292.BMS_socUI',
             99, 'wrong', 'reported_unverified', 1600, 'new')""")
        assert [row[1] for row in db.execute(query)] == [None, None]


if __name__ == "__main__":
    test_coverage_distinguishes_absence_from_observed_zero()
    test_known_cost_total_adds_supplemental_without_zero_filling_unknown()
    test_battery_dashboard_latest_revision_wins_before_status_filter()
    test_battery_dashboard_warning_overlap_keeps_started_before_range()
    test_battery_cards_raw_display_latest_valid_only()
    test_battery_cards_latest_window_energy_not_lifetime()
    test_battery_period_totals_sum_latest_revision_per_window()
    test_physical_cards_latest_analysis_and_new_raw_barriers()
    test_physical_graphs_keep_signed_samples_and_latest_invalid_revision()
    test_raw_physical_cards_never_treat_fleet_scope_as_unit_calibration()
    test_calibrated_extrema_fallback_and_frontier()
    test_can_reports_keep_invalid_latest_and_scope_boundaries()
    test_vehicle_coverage_separates_observation_from_receipt()
    test_can_soc_graph_rejects_wrong_units_and_keeps_null_gaps()
    test_vehicle_identity_canonical_selection_and_scope_split()
    test_coverage_frontier_lists_scopes_without_range_rows()
    test_transition_timeline_starts_from_boundary_sample()
    test_cell_frequency_respects_selected_source_and_epoch()
    test_can_latest_cards_keep_invalid_reports_and_separate_epochs()
    print("test_dashboards: ok (absence, observed zero, retransmission, time range, known cost, battery latest-wins, warning overlap, raw display card, latest-window energy)")
