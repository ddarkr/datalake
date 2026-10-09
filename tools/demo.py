#!/usr/bin/env python3
"""Synthetic, owned Compose demo. Host CLI uses Python stdlib only."""
import argparse
import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = Path(tempfile.gettempdir()) / f"doda-datalake-demo-{os.getuid()}"
MARKER = "private_content_smoke_do_not_store"
TABLES = ("opentelemetry_traces", "opentelemetry_logs", "vehicle_signal",
          "ai_session_summary", "ai_daily_summary", "ai_activity_daily", "vehicle_agg",
          "vehicle_analysis", "trip_summary", "charge_session")
SUMMARY_SQL = ("SELECT session_id, input_tokens, output_tokens, cost_usd, "
               "cost_estimated_usd, llm_spans FROM ai_session_summary "
               "WHERE session_id LIKE 'demo-%' ORDER BY session_id")
EXPECTED = {"demo-known": (7, 3), "demo-unknown": (None, None), "demo-zero": (0, 0),
            "demo-queued": (19, 5)}


def private_write(path, content):
    with open(path, "w", encoding="utf-8", opener=lambda p, f: os.open(p, f, 0o600)) as out:
        out.write(content)
    os.chmod(path, 0o600)


def clean_env():
    # Compose must not inherit operator credentials, profiles, endpoints or ports.
    return {key: value for key, value in os.environ.items()
            if key in {"PATH", "HOME", "TMPDIR", "SYSTEMROOT", "LANG"}
            or key.startswith("DOCKER_")}


def require_local_docker():
    environment = clean_env()
    host = environment.get("DOCKER_HOST")
    if environment.get("DOCKER_CONTEXT") or not host:
        host = subprocess.run(["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
                              env=environment, text=True, stdout=subprocess.PIPE, check=True).stdout.strip()
    if not host.startswith(("unix://", "npipe://")):
        raise RuntimeError("demo requires a local Docker socket; remote Docker endpoints are refused")


def compose(state, *args, restore=False, capture=False):
    project = state["project"] + ("-restore" if restore else "")
    directory = Path(state["directory"])
    envfile = directory / ("restore.env" if restore else "demo.env")
    if args and args[0] == "down":
        config = json.loads(compose(state, "config", "--format", "json", restore=restore, capture=True))
        for volume in config.get("volumes", {}).values():
            name = volume.get("name", "")
            shared_backup = restore and volume.get("external") and name == state["project"] + "_backup-data"
            if not name.startswith(project + "_") and not shared_backup:
                raise ValueError("refusing cleanup of a volume outside the owned demo project")
    result = subprocess.run(["docker", "compose", "--project-directory", str(directory),
                             "--env-file", str(envfile), "-p", project,
                             "-f", str(ROOT / "compose.yaml"), "-f", str(directory / "override.yaml"),
                             "--profile", "server", *args],
                            env=clean_env(), check=True, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else None


def load_state(directory):
    state = json.loads((directory / "state.json").read_text())
    if (not re.fullmatch(r"datalake-demo-[0-9a-f]{16}", state["project"])
            or Path(state["directory"]).resolve() != directory.resolve()):
        raise ValueError("not an owned demo state directory")
    return state


def save_state(state):
    private_write(Path(state["directory"]) / "state.json", json.dumps(state, indent=2) + "\n")


def request(url, data=None, user=None, password=None, content_type="application/json"):
    headers = {"Content-Type": content_type}
    if user is not None:
        headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=30) as response:
        body = response.read()
    return json.loads(body) if body else {}


def sql(state, statement, restore=False):
    result = request(state["restore_db_url"] if restore else state["db_url"],
                     urllib.parse.urlencode({"sql": statement}).encode(),
                     state["db_user"], state["db_password"], "application/x-www-form-urlencoded")
    if result.get("code", 0) != 0 or result.get("error"):
        raise RuntimeError(result)
    rows = []
    for output in result.get("output", []):
        records = output.get("records")
        if records:
            columns = [column["name"] for column in records["schema"]["column_schemas"]]
            rows.extend(dict(zip(columns, row)) for row in records["rows"])
    return rows


def wait_for(probe, description, timeout=300):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = probe()
            if value:
                return value
        except (urllib.error.URLError, OSError, RuntimeError, AssertionError) as error:
            last = str(error)
        time.sleep(2)
    raise RuntimeError(f"timed out waiting for {description}: {last}")


def discover(state, restore=False):
    def port(service, internal):
        return compose(state, "port", service, str(internal), restore=restore, capture=True).strip()
    base = "http://" + port("greptimedb", 4000)
    state["restore_db_url" if restore else "db_url"] = base + "/v1/sql?db=datalake"
    if not restore:
        state["otlp_url"] = "http://" + port("alloy", 4318)
        state["grafana_url"] = "http://" + port("grafana", 3000)
        state["wire_url"] = "http://" + port("demo-wire", 18144)
    save_state(state)


def attributes(values):
    return [{"key": key, "value": {"intValue": str(value)} if isinstance(value, int)
             else {"stringValue": value}} for key, value in values.items()]


def send_span(state, session):
    index = list(EXPECTED).index(session) + 1
    timestamp = state["timestamp_ns"] + index * 1_000_000_000
    identity = hashlib.sha256((state["project"] + session).encode()).hexdigest()
    attrs = {"coding_agent.client": "omp", "coding_agent.session.id": session,
             "gen_ai.conversation.id": session, "gen_ai.provider.name": "openai",
             "gen_ai.request.model": "synthetic-unpriced-model", "gen_ai.operation.name": "chat",
             "gen_ai.prompt": MARKER, "error.type": MARKER}
    incoming, outgoing = EXPECTED[session]
    if incoming is not None:
        attrs.update({"gen_ai.usage.input_tokens": incoming, "gen_ai.usage.output_tokens": outgoing})
    span = {"traceId": identity[:32], "spanId": identity[32:48], "name": "coding_agent.llm.turn",
            "kind": 3, "startTimeUnixNano": str(timestamp),
            "endTimeUnixNano": str(timestamp + 10_000_000), "attributes": attributes(attrs),
            "status": {"code": 1, "message": MARKER}}
    payload = {"resourceSpans": [{"resource": {"attributes": attributes({"service.name": "omp"})},
                                 "scopeSpans": [{"scope": {"name": "synthetic-demo"}, "spans": [span]}]}]}
    response = request(state["otlp_url"] + "/v1/traces", json.dumps(payload).encode(),
                       state["otlp_user"], state["otlp_password"])
    if response.get("partialSuccess", {}).get("rejectedSpans", 0):
        raise RuntimeError("OTLP rejected synthetic spans")


def summaries_ready(state, queued=False):
    rows = sql(state, SUMMARY_SQL)
    expected = EXPECTED if queued else {key: value for key, value in EXPECTED.items() if key != "demo-queued"}
    if {row["session_id"] for row in rows} != set(expected):
        return False
    for row in rows:
        assert (row["input_tokens"], row["output_tokens"]) == expected[row["session_id"]], row
        # No observed usage means an unknown call count, including retransmissions.
        assert row["llm_spans"] == (None if expected[row["session_id"]][0] is None else 1), row
        assert row["cost_usd"] is None and row["cost_estimated_usd"] is None, row
    raw = sql(state, 'SELECT session_id, COUNT(*) AS spans FROM (SELECT DISTINCT '
                    '"span_attributes.coding_agent.session.id" AS session_id, trace_id, span_id '
                    'FROM opentelemetry_traces WHERE "span_attributes.coding_agent.session.id" LIKE \'demo-%\') '
                    'AS identities GROUP BY session_id')
    assert {row["session_id"]: row["spans"] for row in raw} == {session: 1 for session in expected}, raw
    return rows


def start(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    state = {"project": "datalake-demo-" + secrets.token_hex(8), "directory": str(directory.resolve()),
             "timestamp_ns": time.time_ns() - 60_000_000_000, "db_user": "demo",
             "db_password": secrets.token_hex(24), "otlp_user": "demo",
             "otlp_password": secrets.token_hex(24), "grafana_user": "demo",
             "grafana_password": secrets.token_hex(24)}
    save_state(state)  # failed start remains stoppable
    calibration = {field: {"vehicle": "demo-car", "source": "fleet", "decode_epoch": "fleet-v1",
                           "declared_domain": "synthetic-demo", "version": "demo-1", "unit": unit,
                           "unit_scale": 1, "unit_offset": 0}
                   for field, unit in (("PackVoltage", "V"), ("PackCurrent", "A"))}
    env = {"GREPTIME_STORAGE_TYPE": "File", "GREPTIME_DB": "datalake", "GREPTIME_USER": state["db_user"],
           "GREPTIME_PASSWORD": state["db_password"], "OTLP_USER": state["otlp_user"],
           "OTLP_PASSWORD": state["otlp_password"], "GF_ADMIN_USER": state["grafana_user"],
           "GF_ADMIN_PASSWORD": state["grafana_password"], "AGG_INTERVAL_SECONDS": "5",
           "GREPTIME_PAGE_CACHE_SIZE": "128MB", "GREPTIME_WRITE_BUFFER_SIZE": "64MB",
           "GREPTIME_AUTO_FLUSH_INTERVAL": "30s", "OTLP_BATCH_MIN_SIZE": "1",
           "OTLP_BATCH_MAX_SIZE": "10", "OTLP_BATCH_FLUSH_TIMEOUT": "1s",
           "STORAGE_METRICS_INTERVAL_SEC": "10", "INFRA_SCRAPE_INTERVAL": "15s",
           "BATTERY_ANALYSIS_CONFIG_JSON": json.dumps({"energy": {"current_sign": "positive_charge",
                                                                   "field_calibration": calibration}})}
    private_write(directory / "demo.env", "".join(f"{key}='{value}'\n" for key, value in env.items()))
    # Docker allocates free ports atomically; no reserve/release socket race.
    services = {"greptimedb": [4000, 4002], "grafana": [3000]}
    override = "services:\n"
    for service, ports in services.items():
        override += f"  {service}:\n    ports: !override\n" + "".join(f'      - "127.0.0.1::{port}"\n' for port in ports)
    override += ("  alloy:\n    ports: !override\n      - '127.0.0.1::4317'\n      - '127.0.0.1::4318'\n"
                 "    command: [run, --stability.level=public-preview, --server.http.listen-addr=0.0.0.0:12345, "
                 "--storage.path=/var/lib/alloy/data, /etc/alloy/config.alloy]\n"
                 "    environment:\n      GREPTIME_HTTP_URL: http://demo-wire:18144\n")
    wire = {"image": "python:3.12.8-slim-bookworm", "profiles": ["server"],
            "command": ["sh", "-ec", "pip install --disable-pip-version-check --no-cache-dir opentelemetry-proto==1.39.1 && python /demo/demo.py _wire"],
            "ports": ["127.0.0.1::18144"], "environment": env | {"GREPTIME_HTTP_URL": "http://greptimedb:4000", "OTLP_HTTP_URL": "http://alloy:4318"},
            "volumes": [f"{ROOT / 'tools/demo.py'}:/demo/demo.py:ro", f"{ROOT / 'tests/test_privacy.py'}:/demo/test_privacy.py:ro",
                        f"{ROOT / 'scripts/ingest/fleet_recorder.py'}:/demo/scripts/ingest/fleet_recorder.py:ro", "demo-fixtures:/fixtures"],
            "restart": "unless-stopped"}
    # JSON mappings are YAML mappings; only native !override port tags need YAML text.
    override += "  demo-wire: " + json.dumps(wire) + "\nvolumes:\n  demo-fixtures: {}\n"
    # Storage observation must never mount an operator's external CAN archive.
    override += ("  can-receiver-raw:\n"
                 "    name: ${COMPOSE_PROJECT_NAME}_can-receiver-raw\n"
                 "    external: false\n")
    # Override even private bundles that inline their operator calibration.
    override += "configs:\n  battery_analysis_json:\n    content: " + json.dumps(env["BATTERY_ANALYSIS_CONFIG_JSON"]) + "\n"
    private_write(directory / "override.yaml", override)
    compose(state, "up", "-d")
    discover(state)
    wait_for(lambda: sql(state, "SELECT 1"), "database schema")
    wait_for(lambda: request(state["wire_url"] + "/evidence"), "wire audit proxy")
    wait_for(lambda: request(state["grafana_url"] + "/api/health"), "Grafana")
    assert sql(state, "SELECT NULLIF(COUNT(*),0) AS sessions FROM ai_session_summary") == [{"sessions": None}]
    for session in ("demo-known", "demo-unknown", "demo-zero"):
        send_span(state, session)
        send_span(state, session)
    compose(state, "exec", "-T", "demo-wire", "python", "/demo/demo.py", "_fleet")
    wait_for(lambda: summaries_ready(state), "deduplicated usage summaries")
    return state


def grafana_query(state, query, from_ms, to_ms, fmt="table"):
    result = request(state["grafana_url"] + "/api/ds/query",
                     json.dumps({"from": str(from_ms), "to": str(to_ms),
                                 "queries": [{"refId": "A", "datasource": {"type": "mysql", "uid": "greptime-mysql"},
                                              "format": fmt, "rawQuery": True, "rawSql": query,
                                              "queryText": query}]}).encode(),
                     state["grafana_user"], state["grafana_password"])
    output = result.get("results", {}).get("A", {})
    assert not output.get("error"), result
    observed = []
    for frame in output.get("frames", []):
        columns = [field["name"] for field in frame["schema"]["fields"]]
        observed.extend(dict(zip(columns, row)) for row in zip(*frame["data"]["values"]))
    return observed


def grafana_check(state, rows):
    now_ms = int(time.time() * 1000)
    observed = grafana_query(state, SUMMARY_SQL, now_ms - 86_400_000, now_ms)
    assert observed == rows, (observed, rows)
    dashboard = request(state["grafana_url"] + "/api/dashboards/uid/datalake-ai-usage",
                        user=state["grafana_user"], password=state["grafana_password"])
    assert dashboard["dashboard"]["uid"] == "datalake-ai-usage"
    return observed


def battery_dashboard_check(state):
    window = sql(state, "SELECT window_start, window_end FROM vehicle_analysis WHERE vehicle = 'demo-car' "
                 "AND metric = 'battery.energy.latest_power_kw' ORDER BY window_start DESC, computed_at DESC LIMIT 1")[0]
    from_ms = window["window_start"] // 1_000_000 - 3_600_000
    to_ms = (window["window_end"] + 1) // 1_000_000 + 60_000
    checked = []
    for uid in ("datalake-vehicle-overview", "datalake-vehicle-drives",
                "datalake-vehicle-charging", "datalake-vehicle-battery"):
        dashboard = request(state["grafana_url"] + "/api/dashboards/uid/" + uid,
                            user=state["grafana_user"], password=state["grafana_password"])["dashboard"]
        pending = list(dashboard["panels"])
        while pending:
            panel = pending.pop()
            pending.extend(panel.get("panels", []))
            for target in panel.get("targets", []):
                query = target.get("rawSql", "")
                if "AND `metric` = 'battery.energy.latest_power_kw'" in query:
                    expected = -40
                elif "AND `metric` = 'battery.energy.latest_pack_voltage_v'" in query:
                    expected = 400
                else:
                    continue
                for variable, value in (("vehicle", "'demo-car'"), ("vehicle_ids", "'demo-car'"), ("source", "''"), ("epoch", "''")):
                    query = query.replace("${" + variable + ":sqlstring}", value)
                rows = grafana_query(state, query, from_ms, to_ms, target.get("format", "table"))
                values = ([row["reading"] for row in rows] if panel["type"] == "stat" else
                          [value for row in rows for field, value in row.items() if field.lower() != "time"])
                assert values == [expected], (uid, panel["id"], rows)
                if panel["type"] == "stat":
                    stale = grafana_query(state, query, from_ms, to_ms + 3_600_000)
                    assert [row["reading"] for row in stale] == [None], (uid, panel["id"], stale)
                checked.append({"dashboard": uid, "panel": panel["id"], "value": expected})
    assert len(checked) == 8, checked
    return {"from_ms": from_ms, "to_ms": to_ms, "panels": checked}


def snapshot(state, restore=False):
    tables = TABLES + tuple(value for row in sql(state, "SHOW TABLES", restore=restore)
                           for value in row.values() if isinstance(value, str) and "token" in value)
    return {table: sorted(sql(state, f"SELECT * FROM `{table}`", restore=restore),
                          key=lambda row: json.dumps(row, sort_keys=True)) for table in tables}


def restore_check(state):
    compose(state, "stop", "aggregate")
    expected = snapshot(state)
    private_write(Path(state["directory"]) / "expected.json", json.dumps(expected, sort_keys=True))
    compose(state, "stop")
    compose(state, "run", "--rm", "-e", "BACKUP_OFFLINE_CONFIRMED=1", "backup", "backup")
    directory = Path(state["directory"])
    private_write(directory / "restore.env", (directory / "demo.env").read_text() +
                  f"BACKUP_VOLUME_NAME='{state['project']}_backup-data'\nBACKUP_VOLUME_EXTERNAL='true'\n")
    try:
        compose(state, "run", "--rm", "-e", "BACKUP_OFFLINE_CONFIRMED=1", "backup", "restore", restore=True)
        compose(state, "up", "-d", "greptimedb", restore=True)
        discover(state, restore=True)
        wait_for(lambda: sql(state, "SELECT 1", restore=True), "fresh restored database")
        assert snapshot(state, restore=True) == expected, "restored content differs from frozen source"
    finally:
        compose(state, "down", "--volumes", "--remove-orphans", restore=True)
        compose(state, "up", "-d")
        discover(state)
        wait_for(lambda: sql(state, "SELECT 1"), "original demo restart")
    return {table: len(rows) for table, rows in expected.items()}


def calibrated_battery(state):
    rows = sql(state, "SELECT metric, value, unit FROM vehicle_analysis WHERE vehicle = 'demo-car' "
               "AND source = 'fleet' AND metric IN "
               "('battery.energy.latest_power_kw', 'battery.energy.latest_pack_voltage_v')")
    expected = {"battery.energy.latest_power_kw": (-40, "kW"),
                "battery.energy.latest_pack_voltage_v": (400, "V")}
    observed = {row["metric"]: (row["value"], row["unit"]) for row in rows}
    return rows if observed == expected else False


def check(state):
    discover(state)
    if not state.get("queued_verified"):
        compose(state, "stop", "greptimedb")
        try:
            # More than the two exporter consumers: observe backlog, not just in-flight retries.
            for _ in range(16):
                send_span(state, "demo-queued")
            # Observe the actual persistent exporter queue before killing Alloy.
            compose(state, "exec", "-T", "demo-wire", "python", "/demo/demo.py", "_queue")
            compose(state, "kill", "-s", "SIGKILL", "alloy")
            compose(state, "up", "-d", "greptimedb", "alloy")
        finally:
            compose(state, "start", "greptimedb", "alloy")
            # Dynamic host ports belong to a running container, not its saved state.
            discover(state)
        wait_for(lambda: summaries_ready(state, queued=True), "durable queued delivery", timeout=360)
        state["queued_verified"] = True
        save_state(state)
    compose(state, "exec", "-T", "demo-wire", "python", "/demo/demo.py", "_privacy")
    rows = wait_for(lambda: summaries_ready(state, queued=True), "usage summary")
    fleet_rows = sql(state, "SELECT source_field, value_num, unit, quality FROM vehicle_signal WHERE vehicle = 'demo-car'")
    assert len(fleet_rows) == 180, fleet_rows
    values = {"Soc": 59, "PackVoltage": 400, "PackCurrent": -100}
    assert all(row["value_num"] == values[row["source_field"]] for row in fleet_rows), fleet_rows
    assert all(row["unit"] is None and row["quality"] == "unit_unverified"
               for row in fleet_rows if row["source_field"] in ("PackVoltage", "PackCurrent"))
    battery = wait_for(lambda: calibrated_battery(state), "scoped calibrated sealed-hour V/I")
    grafana_rows = grafana_check(state, rows)
    battery_panels = battery_dashboard_check(state)
    raw = sql(state, "SELECT * FROM opentelemetry_traces")
    assert MARKER not in json.dumps(raw), "privacy marker in DB"
    evidence = request(state["wire_url"] + "/evidence")
    for signal, bodies in evidence.items():
        assert all(MARKER.encode() not in base64.b64decode(body) for body in bodies), signal
    restored = restore_check(state)
    state["last_check"] = {"dedup": rows, "grafana_query": grafana_rows, "restore_exact_rows": restored,
                           "calibrated_battery": battery, "battery_panels": battery_panels,
                           "privacy_wire_and_db": True, "queued_after_sigkill": True,
                           "checked_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    save_state(state)
    return state


def status(state):
    raw = compose(state, "ps", "--all", "--format", "json", capture=True).strip()
    containers = json.loads(raw) if raw.startswith("[") else [json.loads(line) for line in raw.splitlines()]
    print(json.dumps({key: state[key] for key in ("project", "directory", "grafana_url", "last_check") if key in state}
                     | {"ui_path": "/d/datalake-ai-usage", "credentials_file": str(Path(state["directory"]) / "demo.env"),
                        "services": [{key: item.get(key) for key in ("Service", "State", "Health", "ExitCode")}
                                     for item in containers]}, indent=2))


def container_mode(mode):
    sys.path.insert(0, "/demo")
    if mode == "_wire":
        import test_privacy as privacy
        class AuditProxy(privacy.WireProxy):
            def do_GET(self):
                if self.path != "/evidence":
                    self.send_error(404)
                    return
                body = json.dumps({signal: [base64.b64encode(message.SerializeToString()).decode() for message in messages]
                                   for signal, messages in self.server.received.items()}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        with ThreadingHTTPServer(("0.0.0.0", 18144), AuditProxy) as proxy:
            proxy.backend = "http://greptimedb:4000"
            proxy.received = {"traces": [], "logs": [], "metrics": []}
            proxy.serve_forever()
    elif mode == "_privacy":
        import test_privacy as privacy
        types = {"traces": privacy.ExportTraceServiceRequest, "logs": privacy.ExportLogsServiceRequest,
                 "metrics": privacy.ExportMetricsServiceRequest}
        class Received:
            def __getitem__(self, signal):
                return [types[signal].FromString(base64.b64decode(body))
                        for body in request("http://demo-wire:18144/evidence")[signal]]
        privacy.test_live_privacy_pipeline(Received())
    elif mode == "_queue":
        def queued():
            with urllib.request.urlopen("http://alloy:12345/metrics", timeout=10) as response:
                metrics = response.read().decode()
            return any(line.startswith("otelcol_exporter_queue_size{") and 'greptime_traces' in line
                       and float(line.rsplit(" ", 1)[-1]) > 0 for line in metrics.splitlines())
        wait_for(queued, "persistent Alloy trace queue", timeout=90)
    elif mode == "_fleet":
        from scripts.ingest import fleet_recorder as fleet
        meta = dict.fromkeys(("target_vin", "vehicle_id", "vehicle_salt", "decode_epoch", "vss_version",
                              "vehicle_firmware", "mapping_revision", "collector_version", "collector_id", "config_version"), "")
        meta.update(target_vin="SYNTHETIC-DEMO-NOT-A-VEHICLE", vehicle_id="demo-car",
                    decode_epoch="fleet-v1", collector_id="demo", collector_version="demo-1")
        stats = fleet.default_stats()
        conn = fleet.open_outbox("/fixtures/fleet.sqlite")
        # Sealed previous hour, with paired V/I up to its final minute. No real vehicle identity.
        end = int(time.time() // 3600) * 3600
        count = 0
        for minute in range(60):
            timestamp = dt.datetime.fromtimestamp(end - 3600 + minute * 60, dt.timezone.utc).isoformat()
            payload = {"vin": "SYNTHETIC-DEMO-NOT-A-VEHICLE", "createdAt": timestamp, "isResend": False,
                       "data": [{"key": key, "value": {"doubleValue": value}} for key, value in
                                (("Soc", 59), ("PackVoltage", 400), ("PackCurrent", -100))]}
            topic, body = fleet.parse_zmq_frames([fleet.TOPIC_V.encode(), json.dumps(payload).encode()])
            count += fleet.process_frame(conn, topic, body, meta, stats)
        assert count == 180, stats
        uploaded = 0
        while True:
            batch = fleet.upload_tick(conn, os.environ["GREPTIME_HTTP_URL"], os.environ["GREPTIME_DB"],
                                      os.environ["GREPTIME_USER"], os.environ["GREPTIME_PASSWORD"], stats=stats)
            uploaded += batch
            if not batch:
                break
        assert uploaded == 180 and conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 0
        conn.close()
        print("Fleet synthetic recorder/outbox/Greptime: 180 rows acknowledged")


def regressions():
    """Mandatory synthetic CAN/RAW checks: any skip is a CI failure."""
    import unittest
    sys.path.insert(0, str(ROOT))
    from tests import test_redecode
    with tempfile.TemporaryDirectory(prefix="datalake-kuksa-") as work:
        os.environ["KUKSA_SRC_DIR"] = test_redecode.ensure_kuksa_src(work)
        suite = unittest.TestSuite()
        for name in ("test_raw.py", "test_redecode.py", "test_can_validation.py"):
            suite.addTests(unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern=name, top_level_dir=str(ROOT)))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        if not result.wasSuccessful() or result.skipped:
            raise SystemExit(f"mandatory CAN/RAW regression failed or skipped: {result.skipped}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("start", "status", "check", "stop", "regressions", "_wire", "_privacy", "_queue", "_fleet"))
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    args = parser.parse_args()
    if args.command == "regressions":
        regressions()
        return
    if args.command.startswith("_"):
        container_mode(args.command)
        return
    directory = args.state.expanduser().resolve()
    require_local_docker()
    if args.command == "start":
        state = start(directory)
    else:
        state = load_state(directory)
        if args.command == "check":
            check(state)
        elif args.command == "stop":
            if (directory / "restore.env").exists():
                compose(state, "down", "--volumes", "--remove-orphans", restore=True)
            compose(state, "down", "--volumes", "--remove-orphans")
            for name in ("demo.env", "restore.env", "override.yaml", "expected.json", "state.json"):
                (directory / name).unlink(missing_ok=True)
            try:
                directory.rmdir()
            except OSError:
                print("Unrecognized files retained in demo state directory", file=sys.stderr)
            print(json.dumps({"project": state["project"], "stopped": True, "owned_volumes_removed": True}))
            return
    status(state)


if __name__ == "__main__":
    main()
