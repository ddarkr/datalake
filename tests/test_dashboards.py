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

if __name__ == "__main__":
    test_coverage_distinguishes_absence_from_observed_zero()
    test_known_cost_total_adds_supplemental_without_zero_filling_unknown()
    print("test_dashboards: ok (absence, observed zero, retransmission, time range, known cost)")
