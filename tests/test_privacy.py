"""Live privacy integration check for the ingest pipeline.

Sends one protobuf OTLP trace/log/metric batch (with a sensitive marker in
every content-bearing field, plus a same-identity duplicate trace) through
the real Alloy endpoint, then polls GreptimeDB SQL until the records land
(or a bounded deadline expires) and asserts:

- the marker is absent from actual exported protobufs AND stored records,
  including exemplar attributes which GreptimeDB can silently discard,
- allowlisted token counts (7 in / 3 out) and session/model/tool ids survive,
- same-identity retries preserve exact per-record usage (summary dedupe is separate),
- bad OTLP credentials and anonymous SQL requests are rejected.

Inputs (all required, no defaults that fake a pass): OTLP_HTTP_URL,
GREPTIME_HTTP_URL, GREPTIME_DB, GREPTIME_USER, GREPTIME_PASSWORD,
OTLP_USER, OTLP_PASSWORD. Run: python tests/test_privacy.py.
The test starts an auditing proxy on OTLP_WIRE_HOST (default 127.0.0.1),
OTLP_WIRE_PORT (default 18144). Point Alloy's GREPTIME_HTTP_URL at that
proxy, but set this test's GREPTIME_HTTP_URL to the real database.
For a container VM, set OTLP_WIRE_HOST=0.0.0.0 and use its host gateway
in Alloy's URL. Use synthetic data only. Requires opentelemetry-proto==1.39.1.
"""

import base64
import gzip
import json
import os
import time
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

MARKER = "private_content_smoke_do_not_store"
IN_TOKENS, OUT_TOKENS = 7, 3
DEADLINE_S = 120


class WireProxy(BaseHTTPRequestHandler):
    """Observe exported protobufs before the database can discard fields."""

    def log_message(self, *_args):
        pass

    def do_POST(self):
        types = {
            "traces": ExportTraceServiceRequest,
            "logs": ExportLogsServiceRequest,
            "metrics": ExportMetricsServiceRequest,
        }
        path = urllib.parse.urlsplit(self.path).path
        signal = path.rsplit("/", 1)[-1]
        if path not in {"/v1/otlp/v1/" + key for key in types} | {"/v1/prometheus/write"}:
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if signal in types:
            decoded = gzip.decompress(body) if self.headers.get("Content-Encoding") == "gzip" else body
            self.server.received[signal].append(types[signal].FromString(decoded))
        headers = {
            key: self.headers[key] for key in (
                "Content-Type", "Content-Encoding", "Authorization",
                "X-Greptime-DB-Name", "X-Greptime-Pipeline-Name",
            ) if key in self.headers
        }
        request = urllib.request.Request(self.server.backend + self.path, data=body, headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            result = response.read()
            self.send_response(response.status)
            self.send_header("Content-Length", str(len(result)))
            self.end_headers()
            self.wfile.write(result)

def env(name):
    value = os.environ.get(name, "")
    assert value, f"missing required env {name}"
    return value


def attr(container, key, value):
    entry = container.add(key=key)
    if isinstance(value, bool):
        entry.value.bool_value = value
    elif isinstance(value, int):
        entry.value.int_value = value
    elif isinstance(value, float):
        entry.value.double_value = value
    else:
        entry.value.string_value = value


def build_traces(now_ns, trace_hex, span_hex):
    req = ExportTraceServiceRequest()
    rs = req.resource_spans.add()
    attr(rs.resource.attributes, "service.name", "omp")
    attr(rs.resource.attributes, "process.command_line", MARKER)
    scope = rs.scope_spans.add()
    scope.scope.name = "datalake-smoke"
    attr(scope.scope.attributes, "private.scope", MARKER)
    span = scope.spans.add(
        name="coding_agent." + MARKER,
        trace_id=bytes.fromhex(trace_hex),
        span_id=bytes.fromhex(span_hex),
        start_time_unix_nano=now_ns,
        end_time_unix_nano=now_ns + 10_000_000,
        trace_state="vendor=" + MARKER,
        kind=3,
    )
    for key, value in {
        "gen_ai.operation.name": "chat",
        "gen_ai.provider.name": "openai",
        "gen_ai.request.model": "smoke-model",
        "gen_ai.response.model": "smoke-model",
        "gen_ai.conversation.id": "smoke-session",
        "coding_agent.session.id": "smoke-session",
        "coding_agent.client": "omp",
        "gen_ai.usage.input_tokens": IN_TOKENS,
        "gen_ai.usage.output_tokens": OUT_TOKENS,
        "gen_ai.prompt": MARKER,
        "gen_ai.tool.call.arguments": MARKER,
        "gen_ai.tool.call.result": MARKER,
        "bash_argv0": MARKER,
        "bash_command_class": MARKER,
        "private.raw": MARKER,
    }.items():
        attr(span.attributes, key, value)
    span.status.code = 1
    span.status.message = MARKER
    event = span.events.add(name=MARKER, time_unix_nano=now_ns)
    attr(event.attributes, "content", MARKER)
    attr(event.attributes, "gen_ai.tool.call.result", MARKER)
    link = span.links.add(trace_id=b"3" * 16, span_id=b"4" * 8)
    attr(link.attributes, "private.link", MARKER)
    return req


def build_logs(now_ns):
    req = ExportLogsServiceRequest()
    native = [
        ("codex", "codex." + MARKER, {
            "event.name": "codex.sse_event", "event.kind": "response.completed",
            "conversation.id": f"smoke-codex-{now_ns}", "model": "smoke-model",
            "input_token_count": "11", "output_token_count": "5",
            "cached_token_count": 2, "cache_write_token_count": 1,
            "reasoning_token_count": 1, "tool_token_count": "16",
        }),
        ("claude_code", "claude_code.api_request", {
            "event.name": "api_request", "session.id": f"smoke-claude-{now_ns}",
            "request_id": f"smoke-request-{now_ns}", "model": "smoke-claude",
            "input_tokens": 17, "output_tokens": 6,
            "cache_read_tokens": 3, "cache_creation_tokens": 2,
            "cost_usd": 0.012, "cost_usd_micros": 12000,
        }),
    ]
    for index, (service, event_name, attributes) in enumerate(native):
        rl = req.resource_logs.add()
        attr(rl.resource.attributes, "service.name", service)
        attr(rl.resource.attributes, "private.raw", MARKER)
        sl = rl.scope_logs.add()
        sl.scope.name = "datalake-smoke"
        attr(sl.scope.attributes, "private.scope", MARKER)
        rec = sl.log_records.add(
            time_unix_nano=now_ns + index,
            observed_time_unix_nano=now_ns + index,
            severity_number=9, severity_text="INFO", event_name=event_name,
        )
        rec.body.string_value = MARKER
        for key, value in {
            **attributes, "private.raw": MARKER,
            "gen_ai.tool.call.arguments": MARKER,
            "bash_argv0": MARKER,
            "bash_command_class": MARKER,
        }.items():
            attr(rec.attributes, key, value)
    return req


def build_metrics(now_ns):
    req = ExportMetricsServiceRequest()
    rm = req.resource_metrics.add()
    attr(rm.resource.attributes, "service.name", "datalake-smoke")
    attr(rm.resource.attributes, "private.raw", MARKER)
    sm = rm.scope_metrics.add()
    sm.scope.name = "datalake-smoke"
    attr(sm.scope.attributes, "private.scope", MARKER)
    metric = sm.metrics.add(name="gen_ai.client.token.usage", description=MARKER, unit="{token}")
    dp = metric.sum.data_points.add(
        start_time_unix_nano=now_ns - 10_000_000, time_unix_nano=now_ns, as_int=13
    )
    metric.sum.aggregation_temporality = 2
    metric.sum.is_monotonic = True
    for key, value in {
        "gen_ai.provider.name": "openai",
        "gen_ai.request.model": f"smoke-model-{now_ns}",
        "private.raw": MARKER,
    }.items():
        attr(dp.attributes, key, value)
    ex = dp.exemplars.add(span_id=b"5" * 8, time_unix_nano=now_ns, as_double=13.0)
    attr(ex.filtered_attributes, "private.exemplar", MARKER)
    return req


def post_otlp(base, user, password, signal, message, expect_ok=True):
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = urllib.request.Request(
        f"{base}/v1/{signal}",
        data=message.SerializeToString(),
        headers={"Content-Type": "application/x-protobuf", "Authorization": "Basic " + auth},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            assert expect_ok, f"{signal} unexpectedly accepted bad credentials"
            return response.status
    except urllib.error.HTTPError as error:
        assert not expect_ok and error.code in (401, 403), f"{signal}: {error.code}"
        return error.code


def sql(base, db, user, password, statement, authorized=True):
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if authorized:
        headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
    request = urllib.request.Request(
        f"{base}/v1/sql?db={urllib.parse.quote(db)}",
        data=urllib.parse.urlencode({"sql": statement}).encode(),
        headers=headers,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.load(response)
    assert result.get("code", 0) == 0 and not result.get("error"), result
    rows = []
    for item in result.get("output", []):
        records = item.get("records")
        if records:
            columns = [column["name"] for column in records["schema"]["column_schemas"]]
            rows.extend(dict(zip(columns, values)) for values in records["rows"])
    return rows




def poll_table(query_fn, presence_token, deadline):
    # Bounded wait until OUR rows land (presence token), then return output.
    while time.monotonic() < deadline:
        output = query_fn()
        if presence_token in json.dumps(output, ensure_ascii=False):
            return output
        time.sleep(3)
    raise AssertionError(f"timed out waiting for {presence_token} to land")


def test_live_privacy_pipeline(received):
    otlp = env("OTLP_HTTP_URL")
    greptime = env("GREPTIME_HTTP_URL")
    db = env("GREPTIME_DB")
    db_user = env("GREPTIME_USER")
    db_password = env("GREPTIME_PASSWORD")
    otlp_user = env("OTLP_USER")
    otlp_password = env("OTLP_PASSWORD")

    now_ns = time.time_ns()
    trace_hex = f"{now_ns:032x}"
    span_hex = f"{(now_ns & (1 << 64) - 1):016x}"

    # Unauthenticated requests must be rejected on both surfaces.
    traces = build_traces(now_ns, trace_hex, span_hex)
    post_otlp(otlp, "wrong", "wrong", "traces", traces, expect_ok=False)
    try:
        sql(greptime, db, db_user, db_password, "SELECT 1", authorized=False)
    except urllib.error.HTTPError as error:
        assert error.code == 401, error.code
    else:
        raise AssertionError("anonymous SQL unexpectedly accepted")

    post_otlp(otlp, otlp_user, otlp_password, "traces", traces)
    post_otlp(otlp, otlp_user, otlp_password, "traces", traces)  # same identity retry
    post_otlp(otlp, otlp_user, otlp_password, "logs", build_logs(now_ns))
    post_otlp(otlp, otlp_user, otlp_password, "metrics", build_metrics(now_ns))
    deadline = time.monotonic() + DEADLINE_S
    run = lambda stmt: sql(greptime, db, db_user, db_password, stmt)

    trace_out = poll_table(
        lambda: run(f"SELECT * FROM opentelemetry_traces WHERE trace_id = '{trace_hex}'"),
        "smoke-session", deadline,
    )
    blob = json.dumps(trace_out, ensure_ascii=False)
    assert MARKER not in blob, f"sensitive marker stored in trace record: {blob[:2000]}"
    # Every stored row keeps exact token counts: retry reuse of identity must
    # not corrupt or amplify per-record usage (aggregate dedupe, if needed,
    # belongs to the summary layer, not the record itself).
    assert trace_out, "trace record missing"
    assert all(row["span_attributes.gen_ai.usage.input_tokens"] == IN_TOKENS for row in trace_out)
    assert all(row["span_attributes.gen_ai.usage.output_tokens"] == OUT_TOKENS for row in trace_out)

    log_out = poll_table(
        lambda: run("SELECT * FROM opentelemetry_logs ORDER BY timestamp DESC LIMIT 50"),
        f"smoke-codex-{now_ns}", deadline,
    )
    own_logs = [row for row in log_out if f"-{now_ns}" in json.dumps(row)]
    assert MARKER not in json.dumps(own_logs, ensure_ascii=False), "marker stored in log record"
    native_attrs = [row["log_attributes"] for row in own_logs]
    native_attrs = [json.loads(value) if isinstance(value, str) else value for value in native_attrs]
    codex = next(value for value in native_attrs if value.get("event.name") == "codex.sse_event")
    claude = next(value for value in native_attrs if value.get("event.name") == "api_request")
    assert codex["input_token_count"] == "11" and codex["output_token_count"] == "5"
    assert codex["conversation.id"] == f"smoke-codex-{now_ns}"
    assert claude["input_tokens"] == 17 and claude["output_tokens"] == 6
    assert claude["cost_usd"] == 0.012 and claude["cost_usd_micros"] == 12000
    assert claude["session.id"] == f"smoke-claude-{now_ns}"

    tables_out = poll_table(lambda: run("SHOW TABLES"), "token", deadline)
    metric_tables = {
        value for row in tables_out for value in row.values()
        if isinstance(value, str) and "token" in value
    }
    model = f"smoke-model-{now_ns}"
    metric_rows = poll_table(
        lambda: [row for table in metric_tables
                 for row in run(f"SELECT * FROM `{table}` ORDER BY greptime_timestamp DESC LIMIT 50")],
        model, deadline,
    )
    own_metrics = [row for row in metric_rows if model in json.dumps(row)]
    assert MARKER not in json.dumps(own_metrics, ensure_ascii=False), "marker stored in metric"

    wire_tokens = {
        "traces": bytes.fromhex(trace_hex),
        "logs": f"smoke-codex-{now_ns}".encode(),
        "metrics": model.encode(),
    }
    for signal, token in wire_tokens.items():
        own_exports = [
            message.SerializeToString() for message in received[signal]
            if token in message.SerializeToString()
        ]
        assert own_exports, f"no {signal} wire evidence: point Alloy at the test proxy"
        assert all(MARKER.encode() not in body for body in own_exports), f"marker exported in {signal}"
    print(f"privacy: PASS trace={trace_hex}, {len(trace_out)} trace rows, logs+metrics, wire+DB redaction, auth rejected")


if __name__ == "__main__":
    address = (os.environ.get("OTLP_WIRE_HOST", "127.0.0.1"), int(os.environ.get("OTLP_WIRE_PORT", "18144")))
    with ThreadingHTTPServer(address, WireProxy) as proxy:
        proxy.backend = env("GREPTIME_HTTP_URL").rstrip("/")
        proxy.received = {"traces": [], "logs": [], "metrics": []}
        worker = threading.Thread(target=proxy.serve_forever, daemon=True)
        worker.start()
        try:
            test_live_privacy_pipeline(proxy.received)
        finally:
            proxy.shutdown()
            worker.join()
