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

def _battery_query(configs, panel_id):
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    panel = next(p for p in dashboard["panels"] if p["id"] == panel_id)
    query = panel["targets"][0]["rawSql"].replace("$$", "$")
    return (query.replace("$__timeFilter(window_start)", "window_start BETWEEN 1000 AND 2000")
            .replace("$__timeFilter(event_time)", "event_time BETWEEN 1000 AND 2000")
            .replace("$__unixEpochTo()", "2000").replace("$__unixEpochFrom()", "1000")
            .replace("${vehicle:sqlstring}", "''").replace("${source:sqlstring}", "''")
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
    return db


def test_battery_dashboard_latest_revision_wins_before_status_filter():
    configs = yaml.safe_load((ROOT / "compose/grafana.yaml").read_text())["configs"]
    assert "grafana-dash-battery" in configs
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    assert dashboard["uid"] == "datalake-vehicle-battery"
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
    dashboard = json.loads(configs["grafana-dash-battery"]["content"])
    assert dashboard["uid"] == "datalake-vehicle-battery"
    panels = {p["id"]: p for p in dashboard["panels"]}
    assert panels[5]["title"].startswith("Warning")
    # Warning panels read alerts only; errors/connectivity never leak in.
    for pid in (5, 18, 19):
        assert "event_type = 'alerts'" in panels[pid]["targets"][0]["rawSql"]
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


if __name__ == "__main__":
    test_coverage_distinguishes_absence_from_observed_zero()
    test_known_cost_total_adds_supplemental_without_zero_filling_unknown()
    test_battery_dashboard_latest_revision_wins_before_status_filter()
    test_battery_dashboard_warning_overlap_keeps_started_before_range()
    print("test_dashboards: ok (absence, observed zero, retransmission, time range, known cost, battery latest-wins, warning overlap)")
