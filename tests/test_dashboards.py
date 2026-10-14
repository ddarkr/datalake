"""Execute dashboard coverage SQL: missing input must not look healthy."""
import json
from pathlib import Path
import sqlite3

import yaml

ROOT = Path(__file__).resolve().parent.parent



def test_dashboard_and_folder_uids_are_disjoint():
    from scripts.database.grafana_folders import FOLDERS

    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    providers = yaml.safe_load(configs["grafana-dashboards"]["content"])["providers"]
    folder_uids = {uid for uid, _, _ in FOLDERS}
    folder_uids.update(p["folderUid"] for p in providers if p.get("folderUid"))
    dashboards = [json.loads(config["content"]) for name, config in configs.items()
                  if name.startswith("grafana-dash-")]
    dashboard_uids = [dashboard["uid"] for dashboard in dashboards]
    assert len(dashboard_uids) == len(set(dashboard_uids)), "Duplicate dashboard UID"
    # Grafana's browse tree indexes both resource types by UID.
    assert not folder_uids.intersection(dashboard_uids), "Folder/dashboard UID collision"

def test_coverage_distinguishes_absence_from_observed_zero():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    queries = []
    for name in ("grafana-dash-ai-usage", "grafana-dash-ai-sessions"):
        dashboard = json.loads(configs[name]["content"])
        panel = next(p for p in dashboard["panels"] if p["id"] == 3)
        query = panel["targets"][0]["rawSql"].replace("$$", "$")
        queries.append(query.replace("$__timeFilter(timestamp)", "timestamp BETWEEN 1000 AND 2000")
                       .replace("$__unixEpochTo()", "2000")
                       .replace("${client:sqlstring}", "''"))
    with sqlite3.connect(":memory:") as db:
        db.create_function("FROM_UNIXTIME", 1, lambda value: value)
        db.execute('''CREATE TABLE opentelemetry_traces (
            timestamp INTEGER, trace_id TEXT, span_id TEXT,
            "span_attributes.gen_ai.usage.input_tokens" INTEGER,
            "span_attributes.gen_ai.usage.output_tokens" INTEGER,
            "span_attributes.coding_agent.session.id" TEXT,
            "span_attributes.session.id" TEXT,
            "span_attributes.conversation.id" TEXT,
            "span_attributes.gen_ai.conversation.id" TEXT,
            "span_attributes.coding_agent.client" TEXT,
            "span_attributes.client" TEXT,
            "span_attributes.event.name" TEXT,
            span_name TEXT, service_name TEXT)''')
        assert [db.execute(q).fetchone()[0] for q in queries] == [None, None]
        # A measured zero token count is present data, not a missing attribute.
        db.execute("INSERT INTO opentelemetry_traces VALUES (1500, 'trace', 'good', 0, 0, 'session', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        assert [db.execute(q).fetchone()[0] for q in queries] == [0, 0]
        # Retransmission must not multiply missing spans; out-of-range rows do not count.
        db.executemany("INSERT INTO opentelemetry_traces VALUES (?, 'trace', 'missing', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
                       [(1500,), (1500,), (3000,)])
        assert [db.execute(q).fetchone()[0] for q in queries] == [1, 1]

def test_known_cost_total_adds_supplemental_without_zero_filling_unknown():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-ai-usage"]["content"])
    panel = next(p for p in dashboard["panels"] if p["id"] == 12)
    query = panel["targets"][0]["rawSql"].replace("$$", "$")
    query = query.replace("$__timeFilter(day_start)", "day_start BETWEEN 1000 AND 2000")
    query = (query.replace("${client:sqlstring}", "''")
             .replace("${provider:sqlstring}", "''")
             .replace("${model:sqlstring}", "''"))
    with sqlite3.connect(":memory:") as db:
        db.execute("""CREATE TABLE ai_daily_summary (
            day_start INTEGER, cost_usd REAL,
            cost_estimated_usd REAL, cost_unpriced_calls INTEGER,
            client TEXT, provider TEXT, model TEXT)""")
        # Legacy/unprocessed row stays fully unknown.
        db.execute("INSERT INTO ai_daily_summary VALUES (1500, NULL, NULL, NULL, NULL, NULL, NULL)")
        assert db.execute(query).fetchone()[:4] == (None, None, None, None)
        # Mixed reported + supplemental, explicit zero kept, unresolved counted.
        db.execute("INSERT INTO ai_daily_summary VALUES (1500, 0.0, NULL, 0, NULL, NULL, NULL)")
        db.execute("INSERT INTO ai_daily_summary VALUES (1500, 0.02, 0.015, 2, NULL, NULL, NULL)")
        total, reported, supplemental, unpriced = db.execute(query).fetchone()[:4]
        assert reported == 0.02 and supplemental == 0.015 and unpriced is None
        assert abs(total - 0.035) < 1e-9
        # Unknown legacy coverage must not disappear beside evaluated rows.
        db.execute("DELETE FROM ai_daily_summary WHERE cost_unpriced_calls IS NULL")
        assert db.execute(query).fetchone()[3] == 2
        # Supplemental-only row: total equals the estimate, reported stays NULL.
        with sqlite3.connect(":memory:") as only:
            only.execute("""CREATE TABLE ai_daily_summary (
                day_start INTEGER, cost_usd REAL,
                cost_estimated_usd REAL, cost_unpriced_calls INTEGER,
                client TEXT, provider TEXT, model TEXT)""")
            only.execute("INSERT INTO ai_daily_summary VALUES (1500, NULL, 0.015, 0, NULL, NULL, NULL)")
            assert only.execute(query).fetchone()[:4] == (0.015, None, 0.015, 0)

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


class _LastValue:
    """SQLite adapter for Greptime's NULL-preserving ordered aggregate."""
    def __init__(self):
        self.value = None

    def step(self, value):
        self.value = value

    def finalize(self):
        return self.value


def _battery_db():
    db = sqlite3.connect(":memory:")
    db.create_function("FROM_UNIXTIME", 1, lambda value: value)
    db.create_function("NS_TIMESTAMP", 1, _physical_ns_timestamp)
    db.create_aggregate("LAST_VALUE", 1, _LastValue)
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
        db.row_factory = sqlite3.Row
        rows = {r["episode"]: r for r in db.execute(state).fetchall()}
        assert rows["ep-active"]["state"] == "open"
        assert rows["ep-closed"]["state"] == "closed"
        assert rows["ep-closed"]["authoritative_end"] == 1300.0
        assert rows["ep-closed"]["reported_duration_s"] == 100.0
        assert rows["ep-reversed"]["state"] == "conflict_time"
        assert rows["ep-reversed"]["authoritative_end"] is None
        assert rows["ep-stale"]["state"] == "unknown_activity"
        db.execute("""INSERT INTO vehicle_event VALUES
            (2600, 'v', 'alerts', 'w', 'can', 'e-late', 2600, 'env',
             1100, NULL, NULL, 'owner', 1, 0, 's', 'e1', 'c', 'ep-closed',
             NULL, NULL, NULL)""")
        rows = {r["episode"]: r for r in db.execute(state).fetchall()}
        assert rows["ep-closed"]["state"] == "conflict_active"
        assert rows["ep-closed"]["authoritative_end"] is None
        assert rows["ep-closed"]["reported_duration_s"] is None


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


def _physical_ns_timestamp(value):
    """Backend time parser for _physical_query TIMESTAMP(9) guards (same as
    _native_ts_query's ns_timestamp; test-only adapter, never source SQL)."""
    from scripts.analytics.battery import battery_runtime as _br
    if value is None or isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return _br.parse_time_bound(value)



def _physical_query(configs, dashboard_name, panel_id, start=1000, end=2000):
    dashboard = json.loads(configs[dashboard_name]["content"])
    panel = next(p for p in _all_battery_panels(dashboard["panels"])
                 if p["id"] == panel_id)
    import re as _re
    query = (panel["targets"][0]["rawSql"].replace("$$", "$")
            .replace("$__timeFilter(window_start)", f"window_start BETWEEN {start} AND {end}")
            .replace("$__timeFilter(s.event_time)", f"s.event_time BETWEEN {start} AND {end}")
            .replace("$__timeFilter(event_time)", f"event_time BETWEEN {start} AND {end}")
            .replace("$__unixEpochTo()", str(end))
            .replace("${vehicle:sqlstring}", "''")
            .replace("${vehicle_ids:sqlstring}", "''")
            .replace("${source:sqlstring}", "''")
            .replace("${epoch:sqlstring}", "''"))
    # Landed 110/111 guard compares native timestamps
    # (CAST(raw.raw_event_time AS TIMESTAMP(9)) <= CAST(latest.value_text AS
    # TIMESTAMP(9))); sqlite CAST on UTC text is NUMERIC-affinity lossy, so
    # translate to the backend parser (callers register NS_TIMESTAMP via
    # _physical_ns_timestamp; test-only adapter name, never source SQL).
    query = _re.sub(r"CAST\(\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+)\s+AS\s+TIMESTAMP\(9\)\s*\)",
                    r"NS_TIMESTAMP(\1)", query)
    query = _re.sub(r"CAST\(\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+TIMESTAMP\(9\)\s*\)",
                    r"NS_TIMESTAMP(\1)", query)
    return query

def test_physical_cards_latest_analysis_and_new_raw_barriers():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    cards = (("grafana-dash-vehicle-overview", 6, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-drives", 7, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-charging", 6, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-battery", 110, "latest_power_kw", "kW", -18.45),
             ("grafana-dash-battery", 111, "latest_pack_voltage_v", "V", 410.0))
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
            # First-time unavailable identities are stored (value NULL); an
            # older stored physical number must not fill that absence.
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
            # Current battery cards fence observations at the selected end;
            # future reports cannot invalidate the completed historical window.
            if dashboard == "grafana-dash-battery":
                assert db.execute(query).fetchone()[4] == value
            else:
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
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 1500, 'v', 'fleet', 'fleet-v1', ?, 4, NULL,
                 'unit_unverified', 1500, 'raw')""", (field,))
            row = db.execute(query).fetchone()
            index = 4 if panel == 52 else 3
            assert row[index] is None
            db.execute("UPDATE vehicle_signal SET unit = ?, quality = 'ok'", (unit,))
            assert db.execute(query).fetchone()[index] == 4
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 1600, 'v', 'fleet', 'fleet-v1', ?, NULL, ?,
                 'invalid', 1600, 'invalid')""", (field, unit))
            assert db.execute(query).fetchone()[index] is None
            if panel in (51, 52):
                db.execute("DELETE FROM vehicle_signal")
                db.executemany("""INSERT INTO vehicle_signal
                    (event_time, vehicle, source, decode_epoch, source_field,
                     value_num, unit, quality, ingest_time, envelope_id, path)
                    VALUES (1700, 'v', ?, 'fleet-v1', ?, 23, ?, 'ok', 1700, 'outside', ?)""",
                    (("can", field, unit, f"Vehicle.Powertrain.TractionBattery.{field}"),
                     ("fleet", field, unit, f"Vehicle.Custom.{field}")))
                assert db.execute(query).fetchall() == []


def _native_ts_query(configs, dashboard, panel, start=1000, end=2000):
    """_physical_query plus the native TIMESTAMP(9)/TIMESTAMP(6) transforms.

    Greptime compares native timestamps; sqlite CAST is a no-op for the
    TIMESTAMP(9) safety comparisons but applies NUMERIC affinity to the
    TIMESTAMP(6) display projections, mangling UTC strings ('1970-...' ->
    1970) while native returns a real timestamp. The harness translates
    both to backend-time functions: NS_TIMESTAMP for the (9) comparisons
    and TS_DISPLAY for the (6) projections, both backed by the real
    backend time parser (same transforms, not repinned results).
    """
    from scripts.analytics.battery import battery_runtime as battery_runtime

    query = _physical_query(configs, dashboard, panel, start=start, end=end)
    import re as _re
    # Native TIMESTAMP(9) safety comparisons: the landed source also uses
    # unqualified simple identifiers (e.g. the 44 raw_frontier lower bound
    # MIN(... CAST(value_text AS TIMESTAMP(9)) ...)). Translate every
    # dotted alias.column form first, then any remaining bare identifier
    # form; both use the same backend time parser (no result repinning).
    # NS_TIMESTAMP is a test-only adapter name, never source SQL.
    for alias, column in (("f", "raw_event_time"), ("c", "value_text"),
                          ("fmax", "raw_event_time"), ("cm", "value_text"),
                          ("fmin", "raw_event_time"), ("cn", "value_text"),
                          ("fvmax", "raw_event_time"), ("cvmax", "value_text"),
                          ("fvmin", "raw_event_time"), ("cvmin", "value_text"),
                          ("ftmax", "raw_event_time"), ("ctmax", "value_text"),
                          ("ftmin", "raw_event_time"), ("ctmin", "value_text"),
                          ("latest", "value_text"), ("raw", "raw_event_time")):
        query = query.replace(
            "CAST(%s.%s AS TIMESTAMP(9))" % (alias, column),
            "NS_TIMESTAMP(%s.%s)" % (alias, column))
    query = _re.sub(r"CAST\(\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+TIMESTAMP\(9\)\s*\)",
                    r"NS_TIMESTAMP(\1)", query)
    assert "TIMESTAMP(9)" not in query
    # TIMESTAMP(6) display projections: translate every qualified form the
    # landed source uses in _native_ts_query panels (44/45/51/52/94), plus
    # any other qualified or bare-identifier form, to the same backend µs
    # parser (no result repinning).
    for alias, column in (("r", "raw_time"), ("c", "value_text"),
                          ("r", "raw_max_time"), ("cm", "value_text"),
                          ("r", "raw_min_time"), ("cn", "value_text"),
                          ("r", "max_time"), ("r", "min_time"),
                          ("r", "tmax_time"), ("r", "tmin_time"),
                          ("r", "raw_max_time"), ("r", "raw_min_time"),
                          ("cvmax", "value_text"), ("cvmin", "value_text"),
                          ("ctmax", "value_text"), ("ctmin", "value_text")):
        query = query.replace(
            "CAST(%s.%s AS TIMESTAMP(6))" % (alias, column),
            "TS_DISPLAY(%s.%s)" % (alias, column))
    query = _re.sub(r"CAST\(\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+)\s+AS\s+TIMESTAMP\(6\)\s*\)",
                    r"TS_DISPLAY(\1)", query)
    query = _re.sub(r"CAST\(\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+TIMESTAMP\(6\)\s*\)",
                    r"TS_DISPLAY(\1)", query)
    assert "TIMESTAMP(6)" not in query

    def ns_timestamp(value):
        if value is None or isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        return battery_runtime.parse_time_bound(value)

    def ts_display(value):
        """Native TIMESTAMP(6) projection: typed time at µs precision."""
        if value is None:
            return None
        if isinstance(value, int):
            return value // 1000 * 1000
        if isinstance(value, float):
            return int(value) // 1000 * 1000
        return battery_runtime.parse_time_bound(value) // 1000 * 1000

    return query, ns_timestamp, ts_display



def _graph_cal_query(configs, panel_id, target=1, start=1000, end=2000, epoch="''"):
    """Panel 47/49/54 calibrated target: latest revision per window first."""
    import json as _json
    dashboard = _json.loads(configs["grafana-dash-battery"]["content"])
    panel = next(p for p in _all_battery_panels(dashboard["panels"]) if p["id"] == panel_id)
    assert panel["targets"][0]["format"] == "time_series"
    target_def = panel["targets"][target]
    assert target_def["format"] == "time_series"
    assert "$$__interval" not in target_def["rawSql"]
    query = target_def["rawSql"].replace("$$", "$")
    # sqlite CAST(x AS TIMESTAMP(6)) is lossy (leading-integer cast); the
    # native backend parses these. Select the source column directly here.
    query = query.replace("CAST(value_text AS TIMESTAMP(6))", "value_text")
    query = query.replace("CAST(window_start AS TIMESTAMP(6))", "window_start")
    return (query.replace("$__timeFilter(window_start)", f"window_start BETWEEN {start} AND {end}")
            .replace("${vehicle:sqlstring}", "''")
            .replace("${vehicle_ids:sqlstring}", "''")
            .replace("${source:sqlstring}", "''")
            .replace("${epoch:sqlstring}", epoch))




def test_calibrated_graphs_render_hourly_samples_beside_verified_raw():
    """Panels 47/49/54: verified raw series stay; hourly calibrated samples
    render when raw units are NULL. Extrema use the actual observation
    timestamp (value_text); the spread uses the completed window_start
    bucket scaled V->mV. Bad latest revisions stay NULL (no resurrection);
    scope and time filters are honored."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    panels = {p["id"]: p for p in _all_battery_panels(dashboard["panels"])}
    assert panels[47]["targets"][1]["refId"] == "B"
    assert panels[49]["targets"][1]["refId"] == "B"
    assert panels[54]["targets"][1]["refId"] == "B"
    assert panels[54]["targets"][2]["refId"] == "C"
    cases = ((47, 1, ("battery.conditions.brick_max_v", "battery.conditions.brick_min_v"),
              "V", 1.0, "1970-01-01 00:00:01.000000500"),
             (54, 2, ("battery.conditions.module_temp_max_c", "battery.conditions.module_temp_min_c"),
              "celsius", 1.0, "1970-01-01 00:00:01.000000500"))
    for panel, target, metrics, unit, scale, observed in cases:
        cal = _graph_cal_query(configs, panel, target=target)
        with _battery_db() as db:
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            assert db.execute(cal).fetchall() == []
            for metric in metrics:
                db.execute("""INSERT INTO vehicle_analysis
                    (window_start, window_end, vehicle, metric, source,
                     analysis_id, revision, value, value_text, unit, status,
                     reason, decode_epoch, computed_at) VALUES
                    (1500, 1500, 'v', ?, 'fleet', 'battery_conditions', 'r1',
                     ?, ?, ?, 'derived',
                     'latest_calibrated_sample domain=d scope=v/fleet/fleet-v1;asof_ns=1500000000500',
                     'fleet-v1', 1600)""", (metric, scale, observed, unit))
            rows = db.execute(cal).fetchall()
            assert len(rows) == 2, (panel, rows)
            assert {row[0] for row in rows} == {observed}
            assert sorted(row[2] for row in rows) == [scale, scale]
            # Time filter honored: rows outside the window disappear.
            assert db.execute(cal.replace(
                "window_start BETWEEN 1000 AND 2000",
                "window_start BETWEEN 1000 AND 1400")).fetchall() == []
            # Scope filter honored: another epoch never leaks in.
            scoped = _graph_cal_query(configs, panel, target=target, epoch="'other'")
            assert db.execute(scoped).fetchall() == []
            # A bad newest revision never resurrects the older good sample.
            db.execute("""INSERT INTO vehicle_analysis
                (window_start, window_end, vehicle, metric, source,
                 analysis_id, revision, value, value_text, unit, status,
                 reason, decode_epoch, computed_at) VALUES
                (1500, 1500, 'v', ?, 'fleet', 'battery_conditions', 'r2',
                 NULL, ?, ?, 'unavailable',
                 'terminal_invalid:latest_sample_unmeasurable',
                 'fleet-v1', 1700)""", (metrics[0], observed, unit))
            rows = db.execute(cal).fetchall()
            assert len(rows) == 2
            assert [row[2] is None for row in rows] == [True, False]
    # Panel 49: spread renders mV at the completed window_start bucket.
    cal = _graph_cal_query(configs, 49, target=1)
    assert "window_start" in cal and "* 1000" in cal
    with _battery_db() as db:
        db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
        assert db.execute(cal).fetchall() == []
        db.execute("""INSERT INTO vehicle_analysis
            (window_start, window_end, vehicle, metric, source,
             analysis_id, revision, value, value_text, unit, status,
             reason, decode_epoch, computed_at) VALUES
            (1500, 1500, 'v', 'battery.conditions.brick_spread_v', 'fleet',
             'battery_conditions', 'r1', 0.02, NULL, 'V', 'derived',
             'calibrated domain=d scope=v/fleet/fleet-v1',
             'fleet-v1', 1600)""")
        rows = db.execute(cal).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1500 and abs(rows[0][2] - 20.0) < 1e-9
        # Wrong-unit newest revision never revives the older good spread.
        db.execute("""INSERT INTO vehicle_analysis
            (window_start, window_end, vehicle, metric, source,
             analysis_id, revision, value, value_text, unit, status,
             reason, decode_epoch, computed_at) VALUES
            (1500, 1500, 'v', 'battery.conditions.brick_spread_v', 'fleet',
             'battery_conditions', 'r2', 9.9, NULL, 'wrong', 'derived',
             'calibrated domain=d scope=v/fleet/fleet-v1',
             'fleet-v1', 1700)""")
        assert db.execute(cal).fetchone()[2] is None


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
        query, ns_timestamp, ts_display = _native_ts_query(
            configs, "grafana-dash-battery", panel)
        observed = battery_runtime.ns_to_sql_ts(1500)
        with _battery_db() as db:
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            db.create_function("NS_TIMESTAMP", 1, ns_timestamp)
            db.create_function("TS_DISPLAY", 1, ts_display)
            assert db.execute(query).fetchall() == []
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 1500, 'v', 'fleet', 'fleet-v1', ?, 4, NULL,
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
            assert row[5] == ts_display(observed)
            assert row[6] == observed
            # Explicit-unit raw wins over calibration.
            db.execute("UPDATE vehicle_signal SET unit = ?, quality = 'ok'", (unit,))
            row = db.execute(query).fetchone()
            assert row[index] == 4 and row[6] == "raw"
            db.execute("UPDATE vehicle_signal SET unit = NULL, quality = 'unit_unverified'")
            # 1ns-newer raw of the same field blocks stale calibration until reanalysis.
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 1501, 'v', 'fleet', 'fleet-v1', ?, 5, NULL,
                 'unit_unverified', 1501, 'newer')""", (field,))
            assert db.execute(query).fetchone()[index] is None
            db.execute("DELETE FROM vehicle_signal WHERE envelope_id = 'newer'")
            # A wrong-unit newest raw never revives calibration.
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 1600, 'v', 'fleet', 'fleet-v1', ?, 4100, 'mV',
                 'ok', 1600, 'wrong-unit')""", (field,))
            assert db.execute(query).fetchone()[index] is None
            db.execute("DELETE FROM vehicle_signal WHERE envelope_id = 'wrong-unit'")
            # A same-timestamp late tombstone blocks calibration until reanalysis.
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 1500, 'v', 'fleet', 'fleet-v1', ?, NULL, ?,
                 'invalid', 1700, 'tombstone')""", (field, unit))
            assert db.execute(query).fetchone()[index] is None
            db.execute("DELETE FROM vehicle_signal WHERE envelope_id = 'tombstone'")
            # Unknown ingest is fail-closed: analysis may not have seen this raw.
            db.execute("UPDATE vehicle_signal SET ingest_time = NULL")
            assert db.execute(query).fetchone()[index] is None
            db.execute("UPDATE vehicle_signal SET ingest_time = 1500")
            assert db.execute(query).fetchone()[index] == calibrated
            # Raw beyond the selected range end never blocks the historical view.
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 2500, 'v', 'fleet', 'fleet-v1', ?, 9, NULL,
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


def test_calibrated_extrema_retained_without_raw_join():
    """Panels 44/45/51/52: calibration-only scopes (no raw_latest row, so
    r.* is NULL) still render the retained value; a same-scope newer raw
    frontier outside the initial selection then blocks the stale value."""
    from scripts.analytics.battery import battery_runtime as battery_runtime

    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    cases = ((44, "BrickVoltageMax", "battery.conditions.brick_max_v", "V", 3, 4.02),
             (45, "BrickVoltageMin", "battery.conditions.brick_min_v", "V", 3, 3.92),
             (51, "ModuleTempMax", "battery.conditions.module_temp_max_c", "celsius", 3, 25.5),
             (52, "ModuleTempMin", "battery.conditions.module_temp_min_c", "celsius", 4, 20.5))
    for panel, field, metric, unit, index, calibrated in cases:
        query, ns_timestamp, ts_display = _native_ts_query(
            configs, "grafana-dash-battery", panel)
        narrow_query, _, _ = _native_ts_query(
            configs, "grafana-dash-battery", panel, start=1000, end=1400)
        observed = battery_runtime.ns_to_sql_ts(1200)
        with _battery_db() as db:
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            db.create_function("NS_TIMESTAMP", 1, ns_timestamp)
            db.create_function("TS_DISPLAY", 1, ts_display)
            # Retained-only: valid calibration, no raw rows at all.
            db.execute("""INSERT INTO vehicle_analysis
                (window_start, window_end, vehicle, metric, source,
                 analysis_id, revision, value, value_text, unit, status,
                 reason, decode_epoch, computed_at) VALUES
                (1200, 1200, 'v', ?, 'fleet', 'battery_conditions', 'r1',
                 ?, ?, ?, 'derived',
                 'latest_calibrated_sample domain=d scope=v/fleet/fleet-v1;asof_ns=1200',
                 'fleet-v1', 1300)""", (metric, calibrated, observed, unit))
            row = db.execute(query).fetchone()
            assert row[index] == calibrated
            assert row[5] == ts_display(observed)
            assert row[6] == observed
            if panel in (51, 52):
                db.execute("UPDATE vehicle_analysis SET source = 'can'")
                assert db.execute(query).fetchall() == []
                db.execute("UPDATE vehicle_analysis SET source = 'fleet'")
            # A same-scope newer raw frontier outside the initial narrow
            # selection blocks the stale value when the range widens.
            db.execute(f"""INSERT INTO vehicle_signal
                (path, event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                ('Vehicle.Powertrain.TractionBattery.{field}', 1500, 'v', 'fleet', 'fleet-v1', ?, 5, NULL,
                 'unit_unverified', 1500, 'newer')""", (field,))
            assert db.execute(narrow_query).fetchone()[index] == calibrated
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
        if panel_id == 9:
            query = "SELECT * FROM (" + query + ") WHERE scope_label LIKE '%외기%'"
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


def test_temperature_cards_keep_both_cabin_and_outside_readings():
    """Battery 53 / overview 9 UNION long form: one row per scope+measure with
    the actual reading (not just scope labels); both cabin (실내) and outside
    (외기) legs survive with their own values, invalid-newest NULLs only its
    own leg, and fleet-relabeled CAN rows stay excluded."""
    for dashboard, panel in (("grafana-dash-battery", 53),
                             ("grafana-dash-vehicle-overview", 9)):
        configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
        query = _physical_query(configs, dashboard, panel)
        with _battery_db() as db:
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            assert db.execute(query).fetchall() == []
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id, path) VALUES
                (1500, 'v', 'fleet', 'e1', 'InsideTemp', 20.0, 'celsius', 'ok', 1500, 'a', 'p/inside'),
                (1500, 'v', 'fleet', 'e1', 'OutsideTemp', 15.0, 'celsius', 'ok', 1500, 'b', 'p/outside')""")
            rows = {row[3]: row[4] for row in db.execute(query)}
            assert len(rows) == 2, rows
            cabin = next(v for k, v in rows.items() if "실내" in k)
            outside = next(v for k, v in rows.items() if "외기" in k)
            assert cabin == 20.0 and outside == 15.0, rows
            # Newest-invalid cabin tombstone NULLs only the cabin leg.
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id, path) VALUES
                (1600, 'v', 'fleet', 'e1', 'InsideTemp', NULL, 'celsius', 'invalid', 1600, 'c', 'p/inside')""")
            rows = {row[3]: row[4] for row in db.execute(query)}
            cabin = next(v for k, v in rows.items() if "실내" in k)
            outside = next(v for k, v in rows.items() if "외기" in k)
            assert cabin is None and outside == 15.0, rows

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
        soc_rows = {
            "demo-can": ("Vehicle.CAN.x292.BMS_socUI", "BMS_socUI", 42.0),
            "demo-fleet": ("Vehicle.Powertrain.TractionBattery.StateOfCharge.Displayed",
                           "BatteryLevel", 44.0)}
        for vehicle, source, epoch in scopes:
            if vehicle in soc_rows:
                path, field, value = soc_rows[vehicle]
                db.execute("""INSERT INTO vehicle_signal
                    (event_time, vehicle, source, decode_epoch, ingest_time, path,
                     source_field, value_num, unit, quality)
                    VALUES (1500, ?, ?, ?, 1500, ?, ?, ?, '%', 'reported_unverified')""",
                           (vehicle, source, epoch, path, field, value))
            else:
                db.execute("""INSERT INTO vehicle_signal
                    (event_time, vehicle, source, decode_epoch, ingest_time, path, value_num)
                    VALUES (1500, ?, ?, ?, 1500, 'Vehicle.Speed', ?)""",
                           (vehicle, source, epoch, 42))
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


def test_provenance_table_matches_card_eligibility():
    """Panel 94: one row per scope/measurement with the card value, actual
    observed_at (never window_start), reported-vs-calibrated basis and
    calibration_version; direct and calibrated rows carry different
    timestamps/bases, latest-invalid stays NULL, no-raw retained stays visible."""
    from scripts.analytics.battery import battery_runtime as battery_runtime

    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query, ns_timestamp, ts_display = _native_ts_query(
        configs, "grafana-dash-battery", 94, start=1000, end=1700000000)
    observed = battery_runtime.ns_to_sql_ts(1500000000)
    observed_late = battery_runtime.ns_to_sql_ts(1550000000)
    with _battery_db() as db:
        db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
        db.create_function("NS_TIMESTAMP", 1, ns_timestamp)
        db.create_function("TS_DISPLAY", 1, ts_display)
        assert db.execute(query).fetchall() == []
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1500000000, 'v', 'fleet', 'fleet-v1', 'BrickVoltageMax', 4.1,
             'V', 'ok', 1500000000, 'raw-max')""")
        db.execute("""INSERT INTO vehicle_analysis
            (window_start, window_end, vehicle, metric, source,
             analysis_id, revision, value, value_text, unit, status,
             reason, decode_epoch, computed_at, calibration_version) VALUES
            (1550000000, 1550000000, 'v', 'battery.conditions.module_temp_min_c', 'fleet',
             'battery_conditions', 'r1', 20.5, ?, 'celsius', 'derived',
             'latest_calibrated_sample domain=d scope=v/fleet/fleet-v1;asof_ns=1550000000',
             'fleet-v1', 1600000000, 'cal-t1')""", (observed_late,))
        rows = {row[3]: row for row in db.execute(query)}
        assert set(rows) == {'최고 셀 전압', '최저 셀 전압', '배터리 최고 온도', '배터리 최저 온도'}
        assert rows['최고 셀 전압'][5] == 4.1
        assert rows['최고 셀 전압'][6] == ts_display(1500000000) and rows['최고 셀 전압'][7] == 'raw'
        assert rows['최고 셀 전압'][8] is None
        assert rows['배터리 최저 온도'][5] == 20.5
        assert rows['배터리 최저 온도'][6] == ts_display(observed_late)
        assert rows['배터리 최저 온도'][7] == 'derived'
        assert rows['배터리 최저 온도'][8] == 'cal-t1'
        assert (rows['최고 셀 전압'][6], rows['최고 셀 전압'][7]) != (rows['배터리 최저 온도'][6], rows['배터리 최저 온도'][7])
        card, _, _ = _native_ts_query(configs, "grafana-dash-battery", 44)
        with _battery_db() as card_db:
            card_db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            card_db.create_function("NS_TIMESTAMP", 1, ns_timestamp)
            card_db.create_function("TS_DISPLAY", 1, ts_display)
            card_db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, unit, quality, ingest_time, envelope_id) VALUES
                (1500, 'v', 'fleet', 'fleet-v1', 'BrickVoltageMax', 4.1,
                 'V', 'ok', 1500, 'raw-max')""")
            assert card_db.execute(card).fetchone()[3] == rows['최고 셀 전압'][5]
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field,
             value_num, unit, quality, ingest_time, envelope_id) VALUES
            (1600000000, 'v', 'fleet', 'fleet-v1', 'BrickVoltageMax', NULL,
             'V', 'invalid', 1600000000, 'tombstone')""")
        rows = {row[3]: row for row in db.execute(query)}
        assert rows['최고 셀 전압'][5] is None
        assert rows['최고 셀 전압'][7] is None
        db.execute("DELETE FROM vehicle_signal WHERE envelope_id IN ('raw-max', 'tombstone')")
        db.execute("""INSERT INTO vehicle_analysis
            (window_start, window_end, vehicle, metric, source,
             analysis_id, revision, value, value_text, unit, status,
             reason, decode_epoch, computed_at, calibration_version) VALUES
            (1500000000, 1500000000, 'v', 'battery.conditions.brick_max_v', 'fleet',
             'battery_conditions', 'r1', 4.02, ?, 'V', 'derived',
             'latest_calibrated_sample domain=d scope=v/fleet/fleet-v1;asof_ns=1500000000',
             'fleet-v1', 1600000000, 'cal-v1')""", (observed,))
        rows = {row[3]: row for row in db.execute(query)}
        assert rows['최고 셀 전압'][5] == 4.02
        assert rows['최고 셀 전압'][6] == ts_display(observed)
        assert rows['최고 셀 전압'][8] == 'cal-v1'


def _fleet_timeline_query(configs, dashboard_name, panel_id, start=1000, end=2000):
    """State timeline with boundary anchor (panels 13/14 pattern)."""
    dashboard = json.loads(configs[dashboard_name]["content"])
    panel = next(p for p in _all_battery_panels(dashboard["panels"])
                 if p["id"] == panel_id)
    return (panel["targets"][0]["rawSql"].replace("$$", "$")
            .replace("$__timeFilter(event_time)", f"event_time BETWEEN {start} AND {end}")
            .replace("$__unixEpochFrom()", str(start))
            .replace("$__unixEpochTo()", str(end))
            .replace("${vehicle:sqlstring}", "''")
            .replace("${vehicle_ids:sqlstring}", "''")
            .replace("${source:sqlstring}", "''")
            .replace("${epoch:sqlstring}", "''"))


def test_fleet_gear_and_charge_timelines_include_latest_invalid():
    """Gear/charge timelines (overview 13/14, drives 9, charging 11, battery 57)
    carry both Fleet and CAN branches: a newer invalid report wins and renders
    NULL instead of resurrecting the older good value."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    cases = (("grafana-dash-vehicle-overview", 14, "Gear",
              "Vehicle.Powertrain.Transmission.CurrentGear", "D",
              "DI_gear", "Vehicle.CAN.x118.DI_gear", "DI_GEAR_D"),
             ("grafana-dash-vehicle-overview", 13, "DetailedChargeState",
              "Vehicle.Powertrain.TractionBattery.Charging.DetailedState",
              "DetailedChargeStateCharging",
              "CP_hvChargeStatus", "Vehicle.CAN.x13D.CP_hvChargeStatus",
              "CP_CHARGE_ENABLED"),
             ("grafana-dash-drives", 9, "Gear",
              "Vehicle.Powertrain.Transmission.CurrentGear", "D",
              "DI_gear", "Vehicle.CAN.x118.DI_gear", "DI_GEAR_D"),
             ("grafana-dash-charging", 11, "DetailedChargeState",
              "Vehicle.Powertrain.TractionBattery.Charging.DetailedState",
              "DetailedChargeStateCharging",
              "CP_hvChargeStatus", "Vehicle.CAN.x13D.CP_hvChargeStatus",
              "CP_CHARGE_ENABLED"),
             ("grafana-dash-battery", 57, "DetailedChargeState",
              "Vehicle.Powertrain.TractionBattery.Charging.DetailedState",
              "DetailedChargeStateCharging",
              "CP_hvChargeStatus", "Vehicle.CAN.x13D.CP_hvChargeStatus",
              "CP_CHARGE_ENABLED"))
    for dashboard, panel, fleet_field, fleet_path, fleet_text, can_field, can_path, can_text in cases:
        query = _fleet_timeline_query(configs, dashboard, panel)
        with _battery_db() as db:
            db.row_factory = sqlite3.Row
            db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
            db.create_function("FROM_UNIXTIME", 1, lambda value: value)
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, value_text, unit, quality, ingest_time, envelope_id, path)
                VALUES (900, 'v', 'fleet', 'fleet-v1', ?, NULL, ?, NULL,
                        NULL, 900, 'fleet-base', ?)""",
                       (fleet_field, fleet_text, fleet_path))
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, value_text, unit, quality, ingest_time, envelope_id, path)
                VALUES (1500, 'v', 'fleet', 'fleet-v1', ?, NULL, ?, NULL,
                        'invalid', 1500, 'fleet-bad', ?)""",
                       (fleet_field, fleet_text, fleet_path))
            db.execute("""INSERT INTO vehicle_signal
                (event_time, vehicle, source, decode_epoch, source_field,
                 value_num, value_text, unit, quality, ingest_time, envelope_id, path)
                VALUES (1500, 'v', 'can', 'e1', ?, NULL, ?, NULL,
                        'reported_unverified', 1500, 'can-good', ?)""",
                       (can_field, can_text, can_path))
            rows = db.execute(query).fetchall()
            assert rows, (dashboard, panel)
            value_key = [key for key in rows[0].keys() if key not in ("time", "series")][0]
            by_series = {}
            for row in rows:
                by_series.setdefault(row["series"], []).append(row)
            assert any("fleet" in series for series in by_series), (dashboard, panel, list(by_series))
            assert any("can" in series for series in by_series), (dashboard, panel, list(by_series))
            fleet_in_range = [row for series, series_rows in by_series.items()
                              if "fleet" in series for row in series_rows
                              if row["time"] >= 1000]
            # Latest Fleet report is invalid: in-range Fleet buckets render NULL,
            # never the older good value.
            assert fleet_in_range and all(row[value_key] is None for row in fleet_in_range), (dashboard, panel, [dict(row) for row in fleet_in_range])
            can_rows = [row for series, series_rows in by_series.items()
                        if "can" in series for row in series_rows]
            assert can_rows[-1][value_key] == can_text, (dashboard, panel, [dict(row) for row in can_rows])


def _scoped_graph_query(configs, dashboard_name, panel_id):
    """Numeric graph with expected-unit CASE and invalid-only NULL buckets."""
    query = _physical_query(configs, dashboard_name, panel_id)
    return query.replace("date_bin(INTERVAL $__interval_ms MILLISECOND, event_time)",
                         "event_time")


def test_speed_soc_scope_separation_units_and_invalid_buckets():
    """Speed/SOC graphs keep raw vehicle/source/epoch scopes separate, gate on
    expected units (km/h, %) and leave invalid-only buckets NULL, never 0."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    with _battery_db() as db:
        db.row_factory = sqlite3.Row
        db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
        # Drives speed: scopes stay separate; a wrong-unit-only scope keeps its
        # bucket but reads NULL, never 0.
        speed = _scoped_graph_query(configs, "grafana-dash-drives", 1)
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1100, 'can-car', 'can', 'can-v1', 'DI_vehicleSpeed',
             'Vehicle.CAN.x257.DI_vehicleSpeed', 50, 'km/h', 'reported_unverified'),
            (1100, 'other-car', 'can', 'can-v1', 'DI_vehicleSpeed',
             'Vehicle.CAN.x257.DI_vehicleSpeed', 30, 'km/h', 'reported_unverified')""")
        rows = db.execute(speed).fetchall()
        assert {row["value"] for row in rows} == {50.0, 30.0}, [dict(row) for row in rows]
        db.execute("UPDATE vehicle_signal SET unit = 'mph' WHERE vehicle = 'can-car'")
        rows = db.execute(speed).fetchall()
        mine = [row for row in rows if "can-car" in row["metric"]]
        assert len(mine) == 1 and mine[0]["value"] is None, [dict(row) for row in rows]
        # Overview SOC: incompatible-unit CAN leg yields a NULL bucket, not 0;
        # the invalid Fleet row never moves the valid mean.
        soc = _scoped_graph_query(configs, "grafana-dash-vehicle-overview", 11)
        db.execute("DELETE FROM vehicle_signal")
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1100, 'can-car', 'can', 'can-v1', 'BMS_socUI',
             'Vehicle.CAN.x292.BMS_socUI', 62, 'fraction', 'reported_unverified')""")
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1200, 'fleet-car', 'fleet', 'fleet-v1', 'Soc',
             'Vehicle.Powertrain.TractionBattery.StateOfCharge.Current',
             70, '%', NULL),
            (1200, 'fleet-car', 'fleet', 'fleet-v1', 'Soc',
             'Vehicle.Powertrain.TractionBattery.StateOfCharge.Current',
             80, '%', 'invalid')""")
        pairs = {(row["time"], row["value"]) for row in db.execute(soc)}
        assert (1100, None) in pairs, pairs
        assert (1200, 70.0) in pairs, pairs


def test_ledger_overlap_window_and_nested_raw_scopes():
    """Drive/charge ledgers return sessions overlapping the range (not just
    started inside it); every ledger and nested raw sibling carries
    source/epoch filters."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    with _battery_db() as db:
        db.row_factory = sqlite3.Row
        db.create_function("FROM_UNIXTIME", 1, lambda value: value)
        db.execute("""CREATE TABLE trip_summary (
            trip_id TEXT, vehicle TEXT, source TEXT, decode_epoch TEXT,
            started_at TIMESTAMP, ended_at TIMESTAMP, duration_s REAL,
            distance_km REAL, energy_kwh REAL, avg_speed_kph REAL,
            start_soc REAL, end_soc REAL)""")
        db.execute("""CREATE TABLE charge_session (
            session_id TEXT, vehicle TEXT, source TEXT, decode_epoch TEXT,
            started_at TIMESTAMP, ended_at TIMESTAMP, duration_s REAL,
            energy_added_kwh REAL, start_soc REAL, end_soc REAL,
            avg_power_kw REAL, max_power_kw REAL)""")
        for dashboard, panel, table in (
                ("grafana-dash-drives", 4, "trip_summary"),
                ("grafana-dash-charging", 4, "charge_session")):
            raw = next(p for p in _all_battery_panels(
                json.loads(configs[dashboard]["content"])["panels"])
                if p["id"] == panel)["targets"][0]["rawSql"].replace("$$", "$")
            query = (raw.replace("$__timeFilter(started_at)",
                                "started_at BETWEEN 1000 AND 2000")
                     .replace("$__unixEpochFrom()", "1000")
                     .replace("$__unixEpochTo()", "2000")
                     .replace("${vehicle:sqlstring}", "''")
                     .replace("${vehicle_ids:sqlstring}", "''")
                     .replace("${source:sqlstring}", "''")
                     .replace("${epoch:sqlstring}", "''"))
            # Intended contract: overlap window plus source/epoch on the ledger.
            assert "started_at <=" in query and "ended_at" in query, (dashboard, panel)
            assert "source" in query and "decode_epoch" in query, (dashboard, panel)
            db.execute(f"""INSERT INTO {table} VALUES
                ('inside', 'v', 'can', 'e1', 1500, 1600, 100, 10, 1, 40, 20, 30)""")
            # A session started before the window but still open inside it
            # overlaps the range and must be listed.
            db.execute(f"""INSERT INTO {table} VALUES
                ('started-before', 'v', 'can', 'e1', 900, NULL, NULL, 10, 1, 40, 20, 30)""")
            # A session fully ended before the window does not overlap.
            db.execute(f"""INSERT INTO {table} VALUES
                ('ended-before', 'v', 'can', 'e1', 800, 950, 100, 10, 1, 40, 20, 30)""")
            rows = {row[0] for row in db.execute(query)}
            assert rows == {"inside", "started-before"}, (dashboard, panel, rows)
            db.execute(f"DELETE FROM {table}")
        # Nested raw children share fixtures but keep distinct scopes, each with
        # source/epoch filters.
        children = []
        row = next(p for p in _all_battery_panels(
            json.loads(configs["grafana-dash-charging"]["content"])["panels"])
            if p["id"] == 20)
        for panel_id in (1, 2, 3):
            raw = next(p for p in row["panels"]
                       if p["id"] == panel_id)["targets"][0]["rawSql"].replace("$$", "$")
            child = (raw.replace("$__timeFilter(event_time)",
                                "event_time BETWEEN 1000 AND 2000")
                     .replace("${vehicle:sqlstring}", "''")
                     .replace("${vehicle_ids:sqlstring}", "''")
                     .replace("${source:sqlstring}", "''")
                     .replace("${epoch:sqlstring}", "''"))
            assert "source" in child and "decode_epoch" in child, panel_id
            children.append(child)
        assert len({len(query) for query in children}) == 3
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1500, 'v', 'can', 'e1', 'BMS_socUI',
             'Vehicle.CAN.x292.BMS_socUI', 62, '%', 'reported_unverified')""")
        assert db.execute(children[0]).fetchall() != []
        assert db.execute(children[1]).fetchall() != []
        assert db.execute(children[2]).fetchall() != []


def test_dbc_divergence_exact_soc_aliases_with_unit_gate():
    """DBC divergence (panel 9) pairs exact supported SOC aliases (CAN x292
    BMS_socUI/Avg vs Fleet Soc/BatteryLevel canonical paths) under the %
    reported gate; a missing counterpart yields NULL diff, never a
    fabricated zero."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _physical_query(configs, "grafana-dash-dbc-health", 9)
    with _battery_db() as db:
        db.row_factory = sqlite3.Row
        db.create_function("date_trunc", 2, lambda unit, value: value - value % 60)
        db.executemany("INSERT INTO vehicle_identity VALUES ('1970-01-01 00:00:00', ?, ?)",
                       [("demo-can", "demo-car"), ("demo-fleet", "demo-car")])
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1100, 'demo-can', 'can', 'can-v1', 'BMS_socUI',
             'Vehicle.CAN.x292.BMS_socUI', 60, '%', 'reported_unverified')""")
        # CAN-only bucket: no Fleet leg joins, so diff stays NULL, never 0.
        rows = db.execute(query).fetchall()
        assert len(rows) == 1, [dict(row) for row in rows]
        assert rows[0]["can_value"] == 60.0, dict(rows[0])
        assert rows[0]["fleet_value"] is None and rows[0]["diff"] is None, dict(rows[0])
        # Distinct raw identities sharing one canonical vehicle pair up.
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1100, 'demo-fleet', 'fleet', 'fleet-v1', 'Soc',
             'Vehicle.Powertrain.TractionBattery.StateOfCharge.Current',
             62, '%', 'reported_unverified')""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 1, [dict(row) for row in rows]
        assert rows[0]["can_value"] == 60.0 and rows[0]["fleet_value"] == 62.0, dict(rows[0])
        assert rows[0]["diff"] == -2.0, dict(rows[0])
        # A wrong-unit CAN leg is unit-ineligible: no fabricated pair row.
        db.execute("DELETE FROM vehicle_signal")
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1100, 'demo-can', 'can', 'can-v1', 'BMS_socUI',
             'Vehicle.CAN.x292.BMS_socUI', 60, 'fraction', 'reported_unverified')""")
        assert db.execute(query).fetchall() == []


def test_battery_lifecycle_counts_only_valid_active_evidence():
    """Warning lifecycle (panel 19): an invalid-only active episode surfaces
    as unknown_activity with zero valid-active observations for diagnosis —
    never counted as an open episode."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    state = _battery_query(configs, 19)
    with _battery_db() as db:
        db.row_factory = sqlite3.Row
        db.execute("""INSERT INTO vehicle_event VALUES
            (1500, 'v', 'alerts', 'w', 'can', 'e-bad', 1500, 'env',
             1500, NULL, NULL, 'owner', 1, 0, 's', 'e1', 'c', 'ep-badonly',
             'error', NULL, NULL)""")
        db.execute("""INSERT INTO vehicle_event VALUES
            (1500, 'v', 'alerts', 'w', 'can', 'e-good', 1500, 'env',
             1500, NULL, NULL, 'owner', 1, 0, 's', 'e1', 'c', 'ep-good',
             NULL, NULL, NULL)""")
        rows = {row["episode"]: row for row in db.execute(state).fetchall()}
        assert rows["ep-badonly"]["state"] == "unknown_activity", dict(rows["ep-badonly"])
        assert rows["ep-badonly"]["valid_active_obs"] == 0, dict(rows["ep-badonly"])
        assert rows["ep-good"]["state"] == "open", dict(rows["ep-good"])
        assert rows["ep-good"]["valid_active_obs"] == 1, dict(rows["ep-good"])


def test_cell_latest_raw_scope_across_epochs_with_cap():
    """Cell latest table (panel 91) projects raw scope identifiers, keeps all
    96 bricks visible across 3 epochs, and documents its row cap; the
    per-brick coverage sibling (panel 93) accounts every brick/epoch."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _physical_query(configs, "grafana-dash-can-cells", 91)
    with _battery_db() as db:
        db.row_factory = sqlite3.Row
        for epoch in ("e1", "e2", "e3"):
            for brick in range(96):
                db.execute("""INSERT INTO vehicle_signal
                    (event_time, vehicle, source, decode_epoch, source_field, path,
                     value_num, unit, quality, ingest_time, envelope_id) VALUES
                    (1500, 'v', 'can', ?, ?, ?, 4.1, 'V',
                     'reported_unverified', 1500, ?)""",
                           (epoch, f"BMS_brick{brick}",
                            f"Vehicle.CAN.x401.BMS_brick{brick}",
                            f"env-{epoch}-{brick}"))
        rows = db.execute(query).fetchall()
        assert len(rows) == 288, len(rows)
        assert {row["해석 버전"] for row in rows} == {"e1", "e2", "e3"}
        assert {row["수집원"] for row in rows} == {"can"}
        assert {row["원본 신호"] for row in rows} == {f"BMS_brick{brick}" for brick in range(96)}
        coverage = _physical_query(configs, "grafana-dash-can-cells", 93)
        assert len(db.execute(coverage).fetchall()) == 288


def test_ai_runtime_dedup_ttft_and_subagent_samples():
    """AI runtime aggregates (panels 11/13) dedupe on (trace_id, span_id):
    duplicate ingested rows never double-count samples or subagent spans, and
    NULL/negative TTFT never becomes a latency sample."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    runtime = json.loads(configs["grafana-dash-ai-runtime"]["content"])
    panels = {p["id"]: p for p in _all_battery_panels(runtime["panels"])}

    def query(panel_id, *percentiles):
        sql = (panels[panel_id]["targets"][0]["rawSql"].replace("$$", "$")
               .replace("$__timeFilter(timestamp)", "timestamp BETWEEN 1000 AND 2000")
               .replace("FROM_UNIXTIME($__unixEpochTo() - 172800)", "0")
               .replace("${client:sqlstring}", "''")
               .replace("${provider:sqlstring}", "''")
               .replace("${model:sqlstring}", "''"))
        for percentile in percentiles:
            inner = percentile[len("approx_percentile_cont("):-1].split(",")[0].strip()
            sql = sql.replace(percentile, "AVG(" + inner + ")")
        return sql

    with sqlite3.connect(":memory:") as db:
        db.row_factory = sqlite3.Row
        db.execute("""CREATE TABLE opentelemetry_traces (
            timestamp TIMESTAMP, trace_id TEXT, span_id TEXT,
            service_name TEXT, span_name TEXT, duration_nano INTEGER,
            "span_attributes.coding_agent.client" TEXT,
            "span_attributes.client" TEXT,
            "span_attributes.event.name" TEXT,
            "span_attributes.gen_ai.provider.name" TEXT,
            "span_attributes.gen_ai.request.model" TEXT,
            "span_attributes.gen_ai.response.model" TEXT,
            "span_attributes.ttft_ms" REAL,
            "span_attributes.coding_agent.subagent.type" TEXT,
            "span_attributes.coding_agent.subagent.status" TEXT,
            "span_attributes.coding_agent.subagent.duration_ms" INTEGER)""")
        for _ in range(2):
            db.execute("""INSERT INTO opentelemetry_traces VALUES
                (1500, 't1', 's1', 'oh-my-pi', 'op', 1000000, 'oh-my-pi', NULL, NULL,
                 'prov', 'm', NULL, 120, 'task', 'completed', 50)""")
        db.execute("""INSERT INTO opentelemetry_traces VALUES
            (1500, 't1', 's2', 'oh-my-pi', 'op', 2000000, 'oh-my-pi', NULL, NULL,
             'prov', 'm', NULL, 250, 'scout', 'failed', NULL)""")
        db.execute("""INSERT INTO opentelemetry_traces VALUES
            (1500, 't2', 's3', 'oh-my-pi', 'op', 3000000, 'oh-my-pi', NULL, NULL,
             'prov', 'm', NULL, -5, NULL, NULL, NULL)""")
        db.execute("""INSERT INTO opentelemetry_traces VALUES
            (1500, 't3', 's4', 'oh-my-pi', 'op', 4000000, 'oh-my-pi', NULL, NULL,
             'prov', 'm', NULL, NULL, NULL, NULL, NULL)""")
        rows = db.execute(query(11, "approx_percentile_cont(ttft_ms, 0.5)",
                                "approx_percentile_cont(ttft_ms, 0.95)")).fetchall()
        assert [(row["client"], row["samples"]) for row in rows] == [("oh-my-pi", 2)], [dict(row) for row in rows]
        assert db.execute(query(10)).fetchone()["ttft_samples"] == 2
        rows = {(row["subagent_type"], row["subagent_status"]): row for row in db.execute(
            query(13, "approx_percentile_cont(duration_ms, 0.5)",
                  "approx_percentile_cont(duration_ms, 0.95)"))}
        assert rows[("task", "completed")]["spans"] == 1, {key: dict(row) for key, row in rows.items()}
        assert rows[("scout", "failed")]["spans"] == 1, {key: dict(row) for key, row in rows.items()}
        assert rows[("scout", "failed")]["duration_samples"] == 0, {key: dict(row) for key, row in rows.items()}
        db.execute('DELETE FROM opentelemetry_traces WHERE "span_attributes.ttft_ms" >= 0')
        assert db.execute(query(10)).fetchone()["ttft_samples"] is None

def test_last_reported_snapshots_keep_history_without_resurrecting_invalid():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    for dashboard, panel in (("grafana-dash-battery", 25),
                             ("grafana-dash-vehicle-overview", 1)):
        query = _physical_query(configs, dashboard, panel).replace(
            "CAST(event_time AS TIMESTAMP(6))", "event_time")
        with _battery_db() as db:
            assert db.execute(query).fetchall() == []
            db.execute("""INSERT INTO vehicle_signal
                (event_time,vehicle,source,decode_epoch,source_field,
                 value_num,unit,quality,path)
                VALUES (900,'v','fleet','e1','BatteryLevel',62,'%','ok','soc')""")
            cursor = db.execute(query)
            row = dict(zip((c[0] for c in cursor.description), cursor.fetchone()))
            assert row["reading"] == 62
            if dashboard == "grafana-dash-battery":
                assert row["observed_at"] == 900
            # A future report cannot leak into a historical snapshot.
            db.execute("""INSERT INTO vehicle_signal
                (event_time,vehicle,source,decode_epoch,source_field,
                 value_num,unit,quality,path)
                VALUES (2001,'v','fleet','e1','BatteryLevel',99,'%','ok','soc')""")
            cursor = db.execute(query)
            assert dict(zip((c[0] for c in cursor.description),
                            cursor.fetchone()))["reading"] == 62
            # The newest retained invalid report wins, even before range start.
            db.execute("""INSERT INTO vehicle_signal
                (event_time,vehicle,source,decode_epoch,source_field,
                 value_num,unit,quality,path)
                VALUES (950,'v','fleet','e1','BatteryLevel',NULL,'%','invalid','soc')""")
            cursor = db.execute(query)
            assert dict(zip((c[0] for c in cursor.description),
                            cursor.fetchone()))["reading"] is None

    query, ns_timestamp, ts_display = _native_ts_query(
        configs, "grafana-dash-battery", 44)
    provenance, _, _ = _native_ts_query(configs, "grafana-dash-battery", 94)
    with _battery_db() as db:
        db.create_function("NS_TIMESTAMP", 1, ns_timestamp)
        db.create_function("TS_DISPLAY", 1, ts_display)
        db.execute("""INSERT INTO vehicle_analysis
            (window_start,window_end,vehicle,metric,source,analysis_id,
             revision,value,value_text,unit,status,reason,decode_epoch,computed_at)
            VALUES (800,900,'v','battery.conditions.brick_max_v','fleet',
                    'battery_conditions','r1',4.02,'1970-01-01 00:00:00.0000009',
                    'V','derived','latest_calibrated_sample','e1',1000)""")
        assert db.execute(query).fetchone()[3] == 4.02
        evidence = {row[3]: row for row in db.execute(provenance)}
        assert evidence["최고 셀 전압"][5] == 4.02
        # An unfinished window cannot replace a completed older snapshot.
        db.execute("""INSERT INTO vehicle_analysis
            (window_start,window_end,vehicle,metric,source,analysis_id,
             revision,value,value_text,unit,status,reason,decode_epoch,computed_at)
            VALUES (1900,2100,'v','battery.conditions.brick_max_v','fleet',
                    'battery_conditions','future',9,'1970-01-01 00:00:00.0000019',
                    'V','derived','latest_calibrated_sample','e1',2100)""")
        assert db.execute(query).fetchone()[3] == 4.02
        db.execute("""INSERT INTO vehicle_analysis
            (window_start,window_end,vehicle,metric,source,analysis_id,
             revision,value,value_text,unit,status,reason,decode_epoch,computed_at)
            VALUES (800,900,'v','battery.conditions.brick_max_v','fleet',
                    'battery_conditions','r2',NULL,NULL,'V','unavailable',
                    'terminal_invalid','e1',1100)""")
        assert db.execute(query).fetchone()[3] is None
        evidence = {row[3]: row for row in db.execute(provenance)}
        assert evidence["최고 셀 전압"][5] is None


def test_analysis_ah_observed_completed_windows_only():
    """Panel 130: only observed completed windows; sparse coverage is shown, never a daily total."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _battery_query(configs, 130)
    with _battery_db() as db:
        assert db.execute(query).fetchall() == []
        for ts, metric, rev, value, computed, status in (
                (1200, "battery.energy.charge_throughput_ah", "r1", 1.5, 10, "derived"),
                (1200, "battery.energy.charge_throughput_ah", "r2", None, 20, "unavailable"),
                (1500, "battery.energy.discharge_throughput_ah", "r3", 0.4, 10, "derived"),
                (1700, "battery.energy.charge_throughput_ah", "r4", 9.0, 10, "derived"),
                (1200, "battery.energy.soh_pct", "r5", 99.0, 10, "derived")):
            db.execute("""INSERT INTO vehicle_analysis
                (window_start, window_end, vehicle, "metric", source, analysis_id,
                 revision, "value", value_text, unit, status, reason, decode_epoch,
                 evidence_count, sample_count, coverage_ratio, algorithm_version,
                 calibration_version, model_version, uncertainty, uncertainty_lower,
                 uncertainty_upper, computed_at, quality, config_version,
                 connectivity, episode_id)
                VALUES (?, ?, 'v', ?, 'fleet', 'battery_energy',
                 ?, ?, NULL, ?, ?, 'ok', 'e1', 1, 2, 0.05,
                 'a', NULL, NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL, NULL)""",
                (ts, None if ts == 1700 else ts + 100, metric, rev,
                 value, "Ah" if "ah" in metric else "%", status, computed))
        rows = db.execute(query).fetchall()
        by_metric = {row[6]: row for row in rows}
        # Window 1200: newest revision is unavailable -> NULL, never resurrected r1.
        assert by_metric["battery.energy.charge_throughput_ah"][7] is None
        assert by_metric["battery.energy.charge_throughput_ah"][10] == "unavailable"
        # Window 1500 discharge survives with its sparse coverage intact.
        assert by_metric["battery.energy.discharge_throughput_ah"][7] == 0.4
        assert by_metric["battery.energy.discharge_throughput_ah"][9] == 0.05
        # Window 1700 has no window_end (incomplete) and soh_pct is out of scope.
        assert sorted(by_metric) == ["battery.energy.charge_throughput_ah",
                                     "battery.energy.discharge_throughput_ah"]
        # A foreign analysis_id writing the same metric/window never merges in.
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1200, 1300, 'v', 'battery.energy.charge_throughput_ah', 'fleet',
             'battery_energy:session:ep1', 'r9', 99.0, NULL, 'Ah', 'derived',
             'ok', 'e1', 1, 2, 0.05, 'a', NULL, NULL, NULL, NULL, NULL, 50,
             NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        assert len(rows) == 2 and {r[3] for r in rows} == {"battery_energy"}
        assert rows[0][7] != 99.0


def test_analysis_ah_summary_selected_vs_observed_gap():
    """Panel 130 target B: per-direction (metric) rows, so one observed hour with both
    charge+discharge counts ONE window per direction (distinct=1), never 2; a missing
    direction stays derived 0, not fully covered."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    panel = next(p for p in _all_battery_panels(dashboard["panels"]) if p["id"] == 130)
    query = (panel["targets"][1]["rawSql"].replace("$$", "$")
            .replace("$__timeFilter(window_start)", "window_start BETWEEN 1000 AND 2000")
            .replace("$__unixEpochTo()", "2000").replace("$__unixEpochFrom()", "1000")
            .replace("${vehicle:sqlstring}", "''").replace("${source:sqlstring}", "''")
            .replace("${vehicle_ids:sqlstring}", "''").replace("${epoch:sqlstring}", "''"))
    with _battery_db() as db:
        assert db.execute(query).fetchall() == []
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1200, 1300, 'v', 'battery.energy.charge_throughput_ah', 'fleet',
             'battery_energy', 'r1', 1.5, NULL, 'Ah', 'derived', 'ok', 'e1',
             1, 2, 0.05, 'a', NULL, NULL, NULL, NULL, NULL, 10,
             NULL, NULL, NULL, NULL),
            (1200, 1300, 'v', 'battery.energy.discharge_throughput_ah', 'fleet',
             'battery_energy', 'r1', 0.4, NULL, 'Ah', 'derived', 'ok', 'e1',
             1, 2, 0.05, 'a', NULL, NULL, NULL, NULL, NULL, 10,
             NULL, NULL, NULL, NULL),
            (1200, 1300, 'v', 'battery.energy.charge_throughput_ah', 'fleet',
             'rogue_writer', 'r9', 99.0, NULL, 'Ah', 'derived', 'ok', 'e1',
             1, 2, 0.05, 'a', NULL, NULL, NULL, NULL, NULL, 50,
             NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        # Two rows: one per direction; each counts ONE window, never 2.
        assert len(rows) == 2 and {r[4] for r in rows} == {
            "battery.energy.charge_throughput_ah",
            "battery.energy.discharge_throughput_ah"}
        assert all(r[3] == "battery_energy" and r[5] == 1 and r[6] == 1 and r[9] == 1
                   for r in rows)
        assert rows[0][10] == 1000 / 3600.0  # selected_hours from the range macros
        # Same hour with discharge unavailable: charge derived 1, discharge derived 0.
        db.execute("""UPDATE vehicle_analysis SET "value" = NULL, status = 'unavailable',
            reason = 'sparse', revision = 'r2', computed_at = 20
            WHERE "metric" = 'battery.energy.discharge_throughput_ah'
            AND analysis_id = 'battery_energy'""")
        by_metric = {r[4]: r for r in db.execute(query).fetchall()}
        assert by_metric["battery.energy.charge_throughput_ah"][6] == 1
        assert by_metric["battery.energy.discharge_throughput_ah"][6] == 0
        assert by_metric["battery.energy.discharge_throughput_ah"][5] == 1


def test_analysis_counter_vs_vi_same_window_no_addition():
    """Panel 131: counter and VI legs join only on the exact same window/scope; AC+DC never added."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _battery_query(configs, 131)
    with _battery_db() as db:
        assert db.execute(query).fetchall() == []
        for ts, metric, rev, value, computed, status in (
                (1200, "battery.energy.dc_charging_energy_in_kwh", "r1", 2.0, 10, "derived"),
                (1200, "battery.energy.vi_charge_energy_kwh", "r1", 1.8, 10, "derived"),
                (1200, "battery.energy.vi_charge_energy_kwh", "r2", None, 20, "unavailable"),
                (1200, "battery.energy.dc_charging_energy_in_kwh", "r3", 5.0, 10, "derived"),
                (1500, "battery.energy.discharge_energy_kwh", "r4", 1.0, 10, "derived"),
                (1500, "battery.energy.vi_discharge_energy_kwh", "r4", 0.9, 10, "derived")):
            epoch = "e2" if rev == "r3" else "e1"
            db.execute(f"""INSERT INTO vehicle_analysis VALUES
                ({ts}, {ts + 100}, 'v', '{metric}', 'fleet', 'battery_energy',
                 '{rev}', {value if value is not None else 'NULL'}, NULL, 'kWh',
                 '{status}', 'ok', '{epoch}', 1, 1, NULL, 'a', NULL, NULL,
                 NULL, NULL, NULL, {computed}, NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        e1 = [r for r in rows if r[2] == "e1"]
        e2 = [r for r in rows if r[2] == "e2"]
        # Same scope+window 1200/e1: counter survives, newest VI is unavailable -> NULL.
        assert len(e1) == 2 and {r[3] for r in e1} == {"battery_energy"}
        w1200 = [r for r in e1 if r[4] == 1200][0]
        assert w1200[6] == 2.0 and w1200[8] is None and w1200[9] == "unavailable"
        # Same window on another epoch never merges into e1 (no cross-scope join).
        assert len(e2) == 1 and e2[0][6] == 5.0 and e2[0][8] is None
        # Discharge pair on window 1500 stays in separate columns, never summed.
        w1500 = [r for r in e1 if r[4] == 1500][0]
        assert w1500[10] == 1.0 and w1500[12] == 0.9
        # A foreign analysis_id writing the same metric/window never merges in.
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1200, 1300, 'v', 'battery.energy.vi_charge_energy_kwh', 'fleet',
             'rogue_writer', 'r9', 42.0, NULL, 'kWh', 'derived', 'ok', 'e1',
             1, 1, NULL, 'a', NULL, NULL, NULL, NULL, NULL, 99,
             NULL, NULL, NULL, NULL)""")
        rows = db.execute(query).fetchall()
        w1200 = [r for r in rows if r[2] == "e1" and r[4] == 1200][0]
        assert w1200[3] == "battery_energy" and w1200[8] is None and w1200[6] == 2.0


def test_analysis_brick_id_latest_invalid_never_resurrected():
    """Panel 132: newest unavailable ID row wins; max/min stay separate (diagnostic only)."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    query = _battery_query(configs, 132)
    with _battery_db() as db:
        assert db.execute(query).fetchall() == []
        for metric, rev, value, computed, status in (
                ("battery.conditions.brick_max_id_persistence_s", "r1", 3600.0, 10, "derived"),
                ("battery.conditions.brick_max_id_persistence_s", "r2", None, 20, "unavailable"),
                ("battery.conditions.brick_min_id_recurrence", "r3", 2.0, 10, "derived"),
                ("battery.conditions.brick_max_id", "r4", 7.0, 10, "reported"),
                ("battery.conditions.brick_min_id", "r5", 3.0, 10, "reported"),
                ("battery.conditions.brick_min_id", "r6", None, 20, "unavailable")):
            db.execute(f"""INSERT INTO vehicle_analysis VALUES
                (1200, 1300, 'v', '{metric}', 'fleet', 'battery_conditions',
                 '{rev}', {value if value is not None else 'NULL'}, NULL,
                 's', '{status}', 'ok', 'e1', 4, 4, NULL, 'a', NULL, NULL,
                 NULL, NULL, NULL, {computed}, NULL, NULL, NULL, NULL)""")
        rows = {r[4]: (r[5], r[8]) for r in db.execute(query).fetchall()}
        assert rows["battery.conditions.brick_max_id_persistence_s"] == (None, "unavailable")
        assert rows["battery.conditions.brick_min_id_recurrence"] == (2.0, "derived")
        assert rows["battery.conditions.brick_max_id"] == (7.0, "reported")
        assert rows["battery.conditions.brick_min_id"] == (None, "unavailable")
        # A foreign analysis_id writing the same metric/window never merges in.
        db.execute("""INSERT INTO vehicle_analysis VALUES
            (1200, 1300, 'v', 'battery.conditions.brick_max_id_persistence_s', 'fleet',
             'rogue_writer', 'r9', 9999.0, NULL, 's', 'derived', 'ok', 'e1',
             4, 4, NULL, 'a', NULL, NULL, NULL, NULL, NULL, 99,
             NULL, NULL, NULL, NULL)""")
        rows = {r[4]: (r[3], r[5], r[8]) for r in db.execute(query).fetchall()}
        assert rows["battery.conditions.brick_max_id_persistence_s"] == ("battery_conditions", None, "unavailable")


def test_analysis_level_minus_soc_exact_timestamp_only():
    """Panel 133: BatteryLevel-Soc only at identical event_time+scope; async/invalid/conflict excluded."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    db = _battery_db()
    db.create_function("CONCAT", -1, lambda *parts: "".join(map(str, parts)))
    query = _battery_query(configs, 133)
    with db:
        assert db.execute(query).fetchall() == []
        db.execute("""INSERT INTO vehicle_signal
            (event_time, vehicle, source, decode_epoch, source_field, path,
             value_num, unit, quality) VALUES
            (1100, 'v', 'fleet', 'fleet-v1', 'BatteryLevel', 'p/level', 70, '%', NULL),
            (1100, 'v', 'fleet', 'fleet-v1', 'Soc', 'p/soc', 65, '%', NULL),
            (1200, 'v', 'fleet', 'fleet-v1', 'BatteryLevel', 'p/level', 71, '%', NULL),
            (1250, 'v', 'fleet', 'fleet-v1', 'Soc', 'p/soc', 66, '%', NULL),
            (1300, 'v', 'fleet', 'fleet-v1', 'BatteryLevel', 'p/level', 72, '%', 'invalid'),
            (1300, 'v', 'fleet', 'fleet-v1', 'Soc', 'p/soc', 67, '%', NULL),
            (1400, 'v', 'fleet', 'fleet-v1', 'BatteryLevel', 'p/level', 73, '%', NULL),
            (1400, 'v', 'fleet', 'fleet-v1', 'Soc', 'p/soc', 68, '%', NULL),
            (1400, 'v', 'fleet', 'fleet-v1', 'Soc', 'p/soc', 69, '%', NULL),
            (1500, 'v', 'fleet', 'fleet-v1', 'BatteryLevel', 'p/level', 74, 'fraction', NULL),
            (1500, 'v', 'fleet', 'fleet-v1', 'Soc', 'p/soc', 70, '%', NULL),
            (1600, 'v', 'fleet', 'fleet-v1', 'BatteryLevel', 'p/level', 62, '%', 'ok'),
            (1600, 'v', 'fleet', 'fleet-v1', 'BatteryLevel', 'p/level', NULL, '%', 'ok'),
            (1600, 'v', 'fleet', 'fleet-v1', 'Soc', 'p/soc', 60, '%', NULL)""")
        pairs = {(row[0], row[2]) for row in db.execute(query).fetchall()}
        assert (1100, 5.0) in pairs, pairs               # exact same-timestamp pair
        assert not any(t in (1200, 1250) for t, _ in pairs), pairs  # async never paired
        assert not any(t in (1300, 1400, 1500, 1600) for t, _ in pairs), pairs  # invalid/conflict/unit/NULL-sibling


def _uistatus_query(configs, panel_id, start=1000, end=2000):
    import json, math
    from scripts.analytics.battery import battery_runtime as br
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    def flat(panels):
        for p in panels:
            yield p
            yield from flat(p.get("panels") or [])
    panel = next(p for p in flat(dashboard["panels"]) if p["id"] == panel_id)
    q = panel["targets"][0]["rawSql"].replace("$$", "$")
    q = q.replace("$__timeFilter(window_start)", f"window_start BETWEEN {start} AND {end}")
    q = q.replace("$__timeFilter(event_time)", f"event_time BETWEEN {start} AND {end}")
    q = q.replace("$__unixEpochTo()", str(end)).replace("$__unixEpochFrom()", str(start))
    for v in ("vehicle", "source", "vehicle_ids", "epoch", "status", "metric"):
        q = q.replace("${%s:sqlstring}" % v, "''")
    import re
    time_expr = r"([\w.]+|(?:MAX|MIN)\([\w.]+\))"
    q = re.sub(r"CAST\(\s*" + time_expr + r"\s+AS\s+TIMESTAMP\(9\)\s*\)", r"NS_TIMESTAMP(\1)", q)
    q = re.sub(r"CAST\(\s*" + time_expr + r"\s+AS\s+TIMESTAMP\(6\)\s*\)", r"TS_DISPLAY(\1)", q)
    assert "TIMESTAMP(9)" not in q and "TIMESTAMP(6)" not in q
    assert "$__" not in q and "${" not in q
    def ns_timestamp(value):
        if value is None or isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        return br.parse_time_bound(value)
    def ts_display(value):
        if value is None:
            return None
        if isinstance(value, int):
            return value // 1000 * 1000
        if isinstance(value, float):
            return int(value) // 1000 * 1000
        return br.parse_time_bound(value) // 1000 * 1000
    return q, ns_timestamp, ts_display, math.floor

def _uistatus_db():
    import sqlite3
    from tests.test_dashboards import _battery_db
    return _battery_db()

_ANALYSIS_COLS = "(window_start,window_end,vehicle,metric,source,analysis_id,revision,value,value_text,unit,status,reason,decode_epoch,computed_at)"
_SIGNAL_COLS = "(event_time,vehicle,source,decode_epoch,source_field,value_num,unit,quality,ingest_time,path)"

def test_uistatus_current_power_needs_fresh_completed_window():
    """Panel 110 CURRENT: stale completed window stays NULL; fresh completed window shows value. No resurrection."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 110, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        assert db.execute(q).fetchall() == []
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (1000,2000,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',5.0,'1970-01-01 00:00:00.000002000','kW','derived','latest_calibrated_sample;asof_ns=2000','e1',2000)")
        rows = db.execute(q).fetchall()
        assert len(rows) == 1 and rows[0][4] is None, dict(rows)
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','latest_calibrated_sample;asof_ns=7200','e1',7200)")
        rows = db.execute(q).fetchall()
        assert len(rows) == 1 and rows[0][4] == 7.5, dict(rows)

def test_uistatus_future_raw_never_suppresses_current():
    """Panels 110/111: raw beyond selectedEnd is excluded early; historical current state stays visible."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    for pid, metric, aid, val, unit in ((110, "battery.energy.latest_power_kw", "battery_energy", 7.5, "kW"), (111, "battery.energy.latest_pack_voltage_v", "battery_voltage", 400.0, "V")):
        q, ns, disp, fl = _uistatus_query(configs, pid, start=0, end=7200)
        with _uistatus_db() as db:
            import math
            db.create_function("FROM_UNIXTIME", 1, lambda v: v)
            db.create_function("FLOOR", 1, math.floor)
            db.create_function("NS_TIMESTAMP", 1, ns)
            db.create_function("TS_DISPLAY", 1, disp)
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','{metric}','fleet','{aid}','r1',{val},'1970-01-01 00:00:00.000007200','{unit}','derived','latest_calibrated_sample','e1',7200)")
            assert db.execute(q).fetchall()[0][4] == val
            db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (9000,'v','fleet','e1','PackCurrent',10.0,'A','ok',9001,'p')")
            rows = db.execute(q).fetchall()
            assert len(rows) == 1 and rows[0][4] == val, dict(rows)

def test_uistatus_inrange_raw_keeps_conservative_guard():
    """Panels 110/111: in-range raw newer than the analyzed observation suppresses the card; older raw does not."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    for pid, metric, aid, val, unit in ((110, "battery.energy.latest_power_kw", "battery_energy", 7.5, "kW"), (111, "battery.energy.latest_pack_voltage_v", "battery_voltage", 400.0, "V")):
        q, ns, disp, fl = _uistatus_query(configs, pid, start=0, end=7200)
        with _uistatus_db() as db:
            import math
            db.create_function("FROM_UNIXTIME", 1, lambda v: v)
            db.create_function("FLOOR", 1, math.floor)
            db.create_function("NS_TIMESTAMP", 1, ns)
            db.create_function("TS_DISPLAY", 1, disp)
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','{metric}','fleet','{aid}','r1',{val},'1970-01-01 00:00:00.000007000','{unit}','derived','latest_calibrated_sample','e1',7200)")
            db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (7100,'v','fleet','e1','PackVoltage',400.0,'V','ok',7101,'p')")
            rows = db.execute(q).fetchall()
            assert len(rows) == 1 and rows[0][4] is None, rows
            db.execute("DELETE FROM vehicle_signal")
            db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (6000,'v','fleet','e1','PackVoltage',400.0,'V','ok',6001,'p')")
            rows = db.execute(q).fetchall()
            assert len(rows) == 1 and rows[0][4] == val, dict(rows)
            # A late receipt can invalidate a result even when its observation is older.
            db.execute("UPDATE vehicle_signal SET ingest_time = 7300")
            assert db.execute(q).fetchone()[4] is None

def test_uistatus_last_completed_is_timestamped_nonlive():
    """Panel 112 LAST: newest completed window shows value with observed_at/window/computed_at; explicitly history, not live."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 112, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        for ws, we, val, vt, comp in ((1000, 2000, 5.0, "1970-01-01 00:00:00.000002000", 2000), (7000, 7200, 7.5, "1970-01-01 00:00:00.000007200", 7200)):
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES ({ws},{we},'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',{val},'{vt}','kW','derived','latest_calibrated_sample','e1',{comp})")
        rows = db.execute(q).fetchall()
        assert len(rows) == 1
        d = dict(zip([c[0] for c in db.execute(q).description], rows[0]))
        assert d["value"] == 7.5 and d["observed_at"] is not None and d["window_end"] is not None and d["computed_at"] is not None

def test_uistatus_evidence_keeps_analysis_identities_separate():
    """Panel 113: same scope/metric with two analysis_ids yields two rows; identities never merge."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 113, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        assert db.execute(q).fetchall() == []
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','A1','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','x','e1',7200)")
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','A2','r1',9.5,'1970-01-01 00:00:00.000007200','kW','derived','x','e1',7201)")
        rows = db.execute(q).fetchall()
        d = [dict(zip([c[0] for c in db.execute(q).description], r)) for r in rows]
        assert sorted((x["analysis_id"], x["value"]) for x in d) == [("A1", 7.5), ("A2", 9.5)], d

def test_uistatus_evidence_raw_snapshot_basis():
    """Panel 113: raw BatteryLevel snapshot keeps before-range latest, excludes future, shows invalid; CAN alias is reported_unverified."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 113, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','x','e1',7200)")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (100,'v','fleet','e1','BatteryLevel',60.0,'%','ok',101,'p')")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','fleet','e1','BatteryLevel',62.5,'%','ok',5001,'p')")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (9000,'v','fleet','e1','BatteryLevel',99.0,'%','ok',9001,'p')")
        d = dict(zip([c[0] for c in db.execute(q).description], db.execute(q).fetchone()))
        assert d["raw_battery_level"] == 62.5 and d["raw_battery_level_at"] == 5000 and d["raw_battery_level_basis"] == "보고값", d
        db.execute("DELETE FROM vehicle_signal")
        db.execute("INSERT INTO vehicle_signal (event_time,vehicle,source,decode_epoch,source_field,unit,quality,ingest_time,path) VALUES (5000,'v','fleet','e1','BatteryLevel','%','invalid',5001,'p')")
        d = dict(zip([c[0] for c in db.execute(q).description], db.execute(q).fetchone()))
        assert d["raw_battery_level"] is None and d["raw_battery_level_at"] == 5000 and d["raw_battery_level_basis"] == "invalid", d
        db.execute("DELETE FROM vehicle_signal")
        db.execute("INSERT INTO vehicle_signal (event_time,vehicle,source,decode_epoch,source_field,value_num,unit,quality,ingest_time,path) VALUES (5000,'v','can','e1','BMS_socUI',70.0,'%','reported_unverified',5001,'Vehicle.CAN.x292.BMS_socUI')")
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','can','battery_energy','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','x','e1',7202)")
        rows = {r[2]: dict(zip([c[0] for c in db.execute(q).description], r)) for r in db.execute(q)}
        assert rows["can"]["raw_battery_level"] == 70.0 and rows["can"]["raw_battery_level_basis"] == "보고값(can·미검증)", rows["can"]

def test_uistatus_evidence_raw_energy_cabin():
    """Panel 113: raw EnergyRemaining/cabin snapshots follow the same selectedEnd-bounded latest-valid rule."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 113, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','x','e1',7200)")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','fleet','e1','EnergyRemaining',50.0,'kWh','ok',5001,'p')")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','fleet','e1','InsideTemp',20.0,'celsius','ok',5001,'p')")
        db.execute("INSERT INTO vehicle_signal (event_time,vehicle,source,decode_epoch,source_field,value_num,unit,quality,ingest_time,path) VALUES (5000,'v','can','e1','VCRIGHT_tempAmbientRaw',15.0,'°C','reported_unverified',5001,'Vehicle.CAN.x20C.VCRIGHT_tempAmbientRaw')")
        rows = {r[2]: dict(zip([c[0] for c in db.execute(q).description], r)) for r in db.execute(q)}
        assert rows["fleet"]["raw_energy_kwh"] == 50.0 and rows["fleet"]["raw_cabin_c"] == 20.0, rows["fleet"]

def test_uistatus_newest_invalid_blocks_older_valid():
    """Panels 110/112: newest unavailable revision NULLs the value; older derived never resurrects. Scope-isolated."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    for pid in (110, 112):
        q, ns, disp, fl = _uistatus_query(configs, pid, start=0, end=7200)
        with _uistatus_db() as db:
            import math
            db.create_function("FROM_UNIXTIME", 1, lambda v: v)
            db.create_function("FLOOR", 1, math.floor)
            db.create_function("NS_TIMESTAMP", 1, ns)
            db.create_function("TS_DISPLAY", 1, disp)
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','latest_calibrated_sample','e1',7200)")
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r2',NULL,NULL,NULL,'unavailable','terminal_invalid:latest_sample_unmeasurable;asof_ns=7200','e1',7300)")
            rows = db.execute(q).fetchall()
            assert len(rows) == 1
            d = dict(zip([c[0] for c in db.execute(q).description], rows[0]))
            val = d.get("reading", d.get("value"))
            assert val is None, d
            assert d.get("reason") is not None and d.get("status") == "unavailable", d
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',9.0,'1970-01-01 00:00:00.000007200','kW','derived','latest_calibrated_sample','e2',7200)")
            assert len(db.execute(q).fetchall()) == 2

def test_uistatus_latest_rank_keeps_canonical_unavailable_over_rogue_value():
    """Panels 112/114: final latest_rank drops analysis_id, so a rogue writer's
    numeric row can outrank the canonical writer's newer unavailable row.
    The canonical newest-unavailable must stay visible (NULL, never the rogue
    number); DashboardRecovery owns the source fix (per-scope canonical
    constraint or analysis_id-aware rank). This test pins the coexistence
    shape that the fix must resolve toward the canonical NULL."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    for pid in (112, 114):
        q, ns, disp, fl = _uistatus_query(configs, pid, start=0, end=7200)
        with _uistatus_db() as db:
            import math
            db.create_function("FROM_UNIXTIME", 1, lambda v: v)
            db.create_function("FLOOR", 1, math.floor)
            db.create_function("NS_TIMESTAMP", 1, ns)
            db.create_function("TS_DISPLAY", 1, disp)
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','latest_calibrated_sample','e1',7200)")
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r2',NULL,NULL,NULL,'unavailable','terminal_invalid:latest_sample_unmeasurable;asof_ns=7200','e1',7300)")
            db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','rogue_writer','r9',99.0,'1970-01-01 00:00:00.000007200','kW','derived','x','e1',7400)")
            rows = db.execute(q).fetchall()
            d = [dict(zip([c[0] for c in db.execute(q).description], r)) for r in rows]
            canon = [x for x in d if x["analysis_id"] == "battery_energy"]
            assert len(canon) == 1, d
            val = canon[0].get("reading", canon[0].get("value"))
            assert val is None and canon[0]["status"] == "unavailable", d

def test_uistatus_window_end_fence_blocks_unfinished():
    """Panels 110/112/113/114: window_end beyond selected end never displays."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 112, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',7.5,'1970-01-01 00:00:00.000007200','kW','derived','latest_calibrated_sample','e1',7200)")
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7100,8000,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',99.0,'1970-01-01 00:00:00.000008000','kW','derived','latest_calibrated_sample','e1',8000)")
        rows = db.execute(q).fetchall()
        assert len(rows) == 1 and rows[0][4] == 7.5, rows

def test_uistatus_pending_states_follow_scoped_evidence():
    """Panel 115: scoped bounded observation evidence only. Observed raw with no
    analysis remains pending; a completed window with later in-range raw needs
    reanalysis; covered/latest input produces no pending row."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    qp, ns, disp, fl = _uistatus_query(configs, 115, start=1000, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        assert db.execute(qp).fetchall() == []
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (500,'v','fleet','e1','PackCurrent',10.0,'A','ok',501,'p')")
        assert db.execute(qp).fetchall() == []
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','fleet','e1','PackCurrent',10.0,'A','ok',5001,'p')")
        rows = db.execute(qp).fetchall()
        assert len(rows) == 1, rows
        d = dict(zip([c[0] for c in db.execute(qp).description], rows[0]))
        assert d["completed_window_end"] is None and d["last_observation"] == disp(5000), d
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (100,200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',5.0,'1970-01-01 00:00:00.000000200','kW','derived','x','e1',200)")
        rows = db.execute(qp).fetchall()
        assert len(rows) == 1 and rows[0][5] is None, rows
        db.execute("DELETE FROM vehicle_analysis")
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (1000,2000,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',5.0,'1970-01-01 00:00:00.000002000','kW','derived','x','e1',2000)")
        db.execute("DELETE FROM vehicle_signal")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (1500,'v','fleet','e1','PackCurrent',10.0,'A','ok',1501,'p')")
        assert db.execute(qp).fetchall() == []
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','fleet','e1','PackCurrent',10.0,'A','ok',5001,'p')")
        rows = db.execute(qp).fetchall()
        assert len(rows) == 1, rows
        d = dict(zip([c[0] for c in db.execute(qp).description], rows[0]))
        assert d["state"] == "재분석 필요" and d["completed_window_end"] == 2000, d
        db.execute("DELETE FROM vehicle_signal")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (9000,'v','fleet','e1','PackCurrent',10.0,'A','ok',9001,'p')")
        assert db.execute(qp).fetchall() == []


def test_uistatus_pending_ignores_superseded_windows_and_future_analysis():
    """Panel 115: two completed window_ends (09/10) with raw 09:30 covered by the
    newest window must not pend; raw without analysis remains pending;
    future analysis rows never confirm."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    qp, ns, disp, fl = _uistatus_query(configs, 115, start=1000, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        # Superseded window covered by the newest: raw 09:30 is older than the
        # newest completed window_end 10 -> covered, no pending row.
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (1000,2000,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',5.0,'1970-01-01 00:00:00.000002000','kW','derived','x','e1',2000)")
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (800,900,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',4.0,'1970-01-01 00:00:00.000000900','kW','derived','x','e1',900)")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (950,'v','fleet','e1','PackCurrent',10.0,'A','ok',951,'p')")
        assert db.execute(qp).fetchall() == []
        # Observed raw with no analysis anywhere in range: explicit
        # '분석 기록 없음 · 확인 필요', never a healthy zero or silent absence.
        db.execute("DELETE FROM vehicle_analysis")
        db.execute("DELETE FROM vehicle_signal")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','fleet','e1','PackCurrent',10.0,'A','ok',5001,'p')")
        rows = db.execute(qp).fetchall()
        assert len(rows) == 1, rows
        d = dict(zip([c[0] for c in db.execute(qp).description], rows[0]))
        assert d["completed_window_end"] is None and d["last_observation"] == disp(5000), d
        # Future analysis never confirms: window_end beyond selected end plus
        # in-range raw still yields no covered row (pending only, end NULL).
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7100,8000,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',99.0,'1970-01-01 00:00:00.000008000','kW','derived','x','e1',8000)")
        rows = db.execute(qp).fetchall()
        assert len(rows) == 1, rows
        d = dict(zip([c[0] for c in db.execute(qp).description], rows[0]))
        assert d["completed_window_end"] is None, d

def test_uistatus_evidence_times_match_card_logic():
    """Panel 113 observed_at/window/computed_at equal the per-identity latest analysis row; raw times are selectedEnd-bounded."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 113, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (1000,2000,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',5.0,'1970-01-01 00:00:00.000002000','kW','derived','x','e1',2000)")
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7000,7200,'v','battery.energy.latest_power_kw','fleet','battery_energy','r2',7.5,'1970-01-01 00:00:00.000007200','kW','derived','x','e1',7200)")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (9000,'v','fleet','e1','BatteryLevel',99.0,'%','ok',9001,'p')")
        d = dict(zip([c[0] for c in db.execute(q).description], db.execute(q).fetchone()))
        assert d["value"] == 7.5 and d["observed_at"] == disp(7200) and d["window_start"] == 7000 and d["computed_at"] == disp(7200), d
        assert d["raw_battery_level"] is None and d["raw_battery_level_at"] is None, d

def test_uistatus_pending_fences_selected_end():
    """Panel 115: completed window beyond selectedEnd never confirms; future raw never lists."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    qp, ns, disp, fl = _uistatus_query(configs, 115, start=1000, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        db.execute(f"INSERT INTO vehicle_analysis {_ANALYSIS_COLS} VALUES (7100,8000,'v','battery.energy.latest_power_kw','fleet','battery_energy','r1',99.0,'1970-01-01 00:00:00.000008000','kW','derived','x','e1',8000)")
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','fleet','e1','PackCurrent',10.0,'A','ok',5001,'p')")
        rows = db.execute(qp).fetchall()
        assert len(rows) == 1, rows
        d = dict(zip([c[0] for c in db.execute(qp).description], rows[0]))
        assert d["state"] == "분석 기록 없음 · 확인 필요" and d["completed_window_end"] is None, d

def test_uistatus_observation_vs_receipt_distinct():
    """Panel 116 keeps event_time and ingest_time as separate columns; receipt never poses as freshness."""
    import yaml
    import tests.test_dashboards as _T
    configs = yaml.safe_load((_T.ROOT / "compose/grafana.yaml").read_text())["configs"]
    q, ns, disp, fl = _uistatus_query(configs, 116, start=0, end=7200)
    with _uistatus_db() as db:
        import math
        db.create_function("FROM_UNIXTIME", 1, lambda v: v)
        db.create_function("FLOOR", 1, math.floor)
        db.create_function("NS_TIMESTAMP", 1, ns)
        db.create_function("TS_DISPLAY", 1, disp)
        db.execute(f"INSERT INTO vehicle_signal {_SIGNAL_COLS} VALUES (5000,'v','can','e1','BMS_socUI',62,'%','reported_unverified',7000,'Vehicle.CAN.x292.BMS_socUI')")
        row = db.execute(q).fetchone()
        d = dict(zip([c[0] for c in db.execute(q).description], row))
        assert d["last_observation"] != d["last_receipt"], d
        assert d["range_rows"] == 1, d

def test_uistatus_row118_holds_flat_panels_only():
    """Row 118 contract: flat regular-panel move children only (Grafana one-level rows); observed energy 30 stays visible."""
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    row = next(p for p in _all_battery_panels(dashboard["panels"]) if p["id"] == 118)
    assert row["type"] == "row" and row.get("collapsed") is True
    child_ids = [c["id"] for c in row.get("panels", [])]
    assert 30 not in child_ids, child_ids
    assert set(child_ids) == {27, 31, 32, 33, 11, 12, 14, 15, 16}, child_ids
    for c in row.get("panels", []):
        assert c.get("type") != "row" and "panels" not in c, c
    tops = {p["id"] for p in dashboard["panels"]}
    assert 30 in tops, tops

if __name__ == "__main__":
    test_last_reported_snapshots_keep_history_without_resurrecting_invalid()
    test_dashboard_and_folder_uids_are_disjoint()
    test_coverage_distinguishes_absence_from_observed_zero()
    test_known_cost_total_adds_supplemental_without_zero_filling_unknown()
    test_battery_dashboard_latest_revision_wins_before_status_filter()
    test_battery_dashboard_warning_overlap_keeps_started_before_range()
    test_battery_cards_raw_display_latest_valid_only()
    test_battery_cards_latest_window_energy_not_lifetime()
    test_battery_soh_card_prefers_absolute_then_estimate_with_error()
    test_physical_cards_latest_analysis_and_new_raw_barriers()
    test_physical_graphs_keep_signed_samples_and_latest_invalid_revision()
    test_raw_physical_cards_never_treat_fleet_scope_as_unit_calibration()
    test_calibrated_extrema_fallback_and_frontier()
    test_calibrated_extrema_retained_without_raw_join()
    test_calibrated_graphs_render_hourly_samples_beside_verified_raw()
    test_can_reports_keep_invalid_latest_and_scope_boundaries()
    test_temperature_cards_keep_both_cabin_and_outside_readings()
    test_vehicle_coverage_separates_observation_from_receipt()
    test_can_soc_graph_rejects_wrong_units_and_keeps_null_gaps()
    test_vehicle_identity_canonical_selection_and_scope_split()
    test_coverage_frontier_lists_scopes_without_range_rows()
    test_transition_timeline_starts_from_boundary_sample()
    test_cell_frequency_respects_selected_source_and_epoch()
    test_provenance_table_matches_card_eligibility()
    test_can_latest_cards_keep_invalid_reports_and_separate_epochs()
    test_fleet_gear_and_charge_timelines_include_latest_invalid()
    test_speed_soc_scope_separation_units_and_invalid_buckets()
    test_ledger_overlap_window_and_nested_raw_scopes()
    test_dbc_divergence_exact_soc_aliases_with_unit_gate()
    test_battery_lifecycle_counts_only_valid_active_evidence()
    test_cell_latest_raw_scope_across_epochs_with_cap()
    test_ai_runtime_dedup_ttft_and_subagent_samples()
    test_temperature_cards_keep_both_cabin_and_outside_readings()
    test_analysis_ah_observed_completed_windows_only()
    test_analysis_ah_summary_selected_vs_observed_gap()
    test_analysis_counter_vs_vi_same_window_no_addition()
    test_analysis_brick_id_latest_invalid_never_resurrected()
    test_analysis_level_minus_soc_exact_timestamp_only()
    test_uistatus_row118_holds_flat_panels_only()
    test_uistatus_observation_vs_receipt_distinct()
    test_uistatus_pending_fences_selected_end()
    test_uistatus_evidence_times_match_card_logic()
    test_uistatus_pending_states_follow_scoped_evidence()
    test_uistatus_window_end_fence_blocks_unfinished()
    test_uistatus_latest_rank_keeps_canonical_unavailable_over_rogue_value()
    test_uistatus_pending_ignores_superseded_windows_and_future_analysis()
    test_uistatus_evidence_raw_energy_cabin()
    test_uistatus_evidence_raw_snapshot_basis()
    test_uistatus_evidence_keeps_analysis_identities_separate()
    test_uistatus_last_completed_is_timestamped_nonlive()
    test_uistatus_inrange_raw_keeps_conservative_guard()
    test_uistatus_future_raw_never_suppresses_current()
    test_uistatus_current_power_needs_fresh_completed_window()

