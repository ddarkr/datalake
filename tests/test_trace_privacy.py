"""Trace-hop privacy and receipt regressions using isolated loopback servers."""
import json
from pathlib import Path
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import urllib.error
import urllib.request
from unittest.mock import patch

from scripts.telemetry.trace_privacy import redact
from scripts.telemetry import trace_privacy


def test_private_arrays_removed_without_losing_usage():
    marker = "synthetic-private-event-content"
    spans = []
    resources = []
    for index, tokens in enumerate((7, 11), start=1):
        span = {
            "traceId": str(index) * 32,
            "spanId": str(index) * 16,
            "startTimeUnixNano": "1700000000000000001",
            "attributes": [{"key": "gen_ai.usage.input_tokens", "value": {"intValue": str(tokens)}}],
            "events": [{"name": marker}],
            "links": [{"traceId": "3" * 32, "spanId": "3" * 16,
                       "attributes": [{"key": "private", "value": {"stringValue": marker}}]}],
        }
        spans.append(span)
        resources.append({"scopeSpans": [{"spans": [span]}]})
    result = redact({"resourceSpans": resources})
    assert marker.encode() not in result
    parsed = json.loads(result)
    for index, expected in enumerate(spans):
        actual = parsed["resourceSpans"][index]["scopeSpans"][0]["spans"][0]
        assert "events" not in actual and "links" not in actual
        assert actual["traceId"] == expected["traceId"]
        assert actual["spanId"] == expected["spanId"]
        assert actual["startTimeUnixNano"] == "1700000000000000001"
        usage = {entry["key"]: entry["value"] for entry in actual["attributes"]}
        assert int(usage["gen_ai.usage.input_tokens"]["intValue"]) == (7, 11)[index]


def test_invalid_json_is_rejected():
    for payload in (b"{", b'{"resourceSpans":[],"invalid":NaN}'):
        try:
            redact(json.loads(payload))
        except ValueError:
            pass
        else:
            raise AssertionError("malformed/nonfinite JSON must not pass the privacy hop")


def test_receipt_requires_ack_and_never_labels_private_clients():
    class Backend(BaseHTTPRequestHandler):
        status, response = 200, b"{}"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(self.status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(self.response)

    backend = HTTPServer(("127.0.0.1", 0), Backend)
    front = HTTPServer(("127.0.0.1", 0), trace_privacy.Handler)
    threads = [threading.Thread(target=server.serve_forever, daemon=True)
               for server in (backend, front)]
    for thread in threads:
        thread.start()
    url = "http://127.0.0.1:%d" % front.server_port
    previous = dict(trace_privacy.LAST_RECEIVED)
    trace_privacy.LAST_RECEIVED.clear()
    payload = {"resourceSpans": [{"scopeSpans": [{"spans": [{
        "attributes": [{"key": "coding_agent.client",
                        "value": {"stringValue": "omp"}}]}]}]}]}

    def post(auth=True):
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Basic synthetic"
        request = urllib.request.Request(url + "/v1/traces",
                                         json.dumps(payload).encode(), headers)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status
        except urllib.error.HTTPError as error:
            with error:
                return error.code

    try:
        with patch.object(trace_privacy, "UPSTREAM",
                          "http://127.0.0.1:%d/v1/traces" % backend.server_port):
            assert post(False) == 401
            assert trace_privacy.receipt_metrics() == b""
            with patch("scripts.telemetry.trace_privacy.time.time", return_value=100):
                assert post() == 200
            assert trace_privacy.LAST_RECEIVED == {"omp": 100}
            Backend.status = 503
            assert post() == 503
            assert trace_privacy.LAST_RECEIVED == {"omp": 100}
            Backend.status, Backend.response = 200, b'{"partialSuccess":{"rejectedSpans":"1"}}'
            assert post() == 200
            assert trace_privacy.LAST_RECEIVED == {"omp": 100}
            Backend.response = b"{}"
            private = "synthetic/private-token-sentinel"
            payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"][0]["value"]["stringValue"] = private
            with patch("scripts.telemetry.trace_privacy.time.time", return_value=200):
                assert post() == 200
            with urllib.request.urlopen(url + "/metrics", timeout=5) as response:
                metrics = response.read()
            assert private.encode() not in metrics
            assert trace_privacy.LAST_RECEIVED == {"omp": 100, "other": 200}
            payload["resourceSpans"] = []
            assert post() == 200
            assert trace_privacy.LAST_RECEIVED == {"omp": 100, "other": 200}
    finally:
        trace_privacy.LAST_RECEIVED.clear()
        trace_privacy.LAST_RECEIVED.update(previous)
        for server in (front, backend):
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()


def check_native_performance_projection(project):
    """Exercise real Alloy via project(signal, resource, name, attrs) -> (name, attrs).

    The runtime owner supplies the isolated OTLP sender/export capture, not an
    imitation of OTTL. Use flat typed attributes on both sides of this boundary.
    An optional fifth argument supplies OTLP span start/end times in nanoseconds.
    """
    private = "synthetic-private-request-secret"
    for signal in ("traces", "logs"):
        fixtures = (
            ("codex_exec", "codex.sse_event", {
                "event.name": "codex.sse_event", "event.kind": "response.completed",
                "ttft_ms": 0, "service_tier": "priority",
                "model_reasoning_effort": "high", "output_token_count": 7,
                "duration_ms": 999, "coding_agent.request.duration_ms": 999,
                "coding_agent.duration.kind": "provider_call",
            }, {
                "ttft_ms": 0, "coding_agent.ttft.kind": "first_output_item",
                "coding_agent.request.service_tier": "priority",
                "coding_agent.request.reasoning.effort": "high",
                "coding_agent.request.settings_source": "native",
                "coding_agent.usage.source": "reported",
            }),
            ("claude_code", "claude_code.llm_request", {
                "duration_ms": 100, "ttft_ms": 20, "first_content_ms": 30,
                "speed": "fast", "effort": "max", "output_tokens": 7,
            }, {
                "coding_agent.request.duration_ms": 100,
                "coding_agent.duration.kind": "logical_call",
                "ttft_ms": 20, "coding_agent.ttft.kind": "provider_reported",
                "first_content_ms": 30,
                "coding_agent.request.service_tier": "fast",
                "coding_agent.request.reasoning.effort": "max",
                "coding_agent.usage.source": "reported",
            }),
            ("oh-my-pi", "chat model" if signal == "traces" else
             "gen_ai.client.inference.operation.details", {
                "gen_ai.response.time_to_first_chunk": 0.025,
                "gen_ai.provider.name": "openai-codex",
                "omp.gen_ai.request.reasoning.effort": "xhigh",
                "openai.request.service_tier": "priority",
                "gen_ai.usage.output_tokens": 7,
            }, {
                "ttft_ms": 25, "coding_agent.ttft.kind": "first_output_item",
                "coding_agent.request.reasoning.effort": "xhigh",
                "coding_agent.request.service_tier": "priority",
                "coding_agent.usage.source": "reported",
            }),
        )
        for service, name, attrs, expected in fixtures:
            attrs = dict(attrs, **{"gen_ai.input.messages": private,
                                  "gen_ai.output.messages": private,
                                  "coding_agent.model.variant": private + "!"})
            _, actual = project(signal, {"service.name": service,
                                        "service.version": "1.2.3"}, name, attrs)
            for key, value in expected.items():
                assert actual.get(key) == value, (signal, service, key, actual)
            assert actual["coding_agent.signal_source"] == "native"
            assert actual["coding_agent.harness.version"] == "1.2.3"
            assert "coding_agent.response.service_tier" not in actual
            assert "coding_agent.response.first_text_ms" not in actual
            assert private not in json.dumps(actual)
            if service == "codex_exec":
                assert "coding_agent.request.duration_ms" not in actual
                assert "coding_agent.duration.kind" not in actual

        _, actual = project(signal, {"service.name": "codex_exec"},
                            "codex.sse_event", {
            "event.name": "codex.sse_event", "event.kind": "response.completed",
        })
        for key in ("coding_agent.usage.source", "ttft_ms",
                    "coding_agent.request.service_tier",
                    "coding_agent.request.reasoning.effort",
                    "coding_agent.request.duration_ms"):
            assert key not in actual, (signal, key, actual)

        for native_ttft in ("0", "12.5"):
            _, actual = project(signal, {"service.name": "codex_exec"},
                                "codex.sse_event", {
                "event.name": "codex.sse_event", "event.kind": "response.completed",
                "ttft_ms": native_ttft, "input_token_count": "11",
                "output_token_count": "0", "tool_token_count": "16",
            })
            assert actual["ttft_ms"] == float(native_ttft)
            assert actual["coding_agent.ttft.kind"] == "first_output_item"
            assert actual["input_token_count"] == (11 if signal == "traces" else "11")
            assert actual["output_token_count"] == (0 if signal == "traces" else "0")
            assert actual["tool_token_count"] == (16 if signal == "traces" else "16")
            assert actual["event.kind"] == "response.completed"
            if signal == "traces":
                assert actual["gen_ai.usage.input_tokens"] == 11
                assert actual["gen_ai.usage.output_tokens"] == 0
            assert actual["coding_agent.usage.source"] == "reported"
            assert "coding_agent.request.duration_ms" not in actual

        for malformed in ("-1", "NaN", "1e3", private, "9" * 40):
            _, actual = project(signal, {"service.name": "codex_exec"},
                                "codex.sse_event", {
                "event.name": "codex.sse_event", "event.kind": "response.completed",
                "ttft_ms": malformed, "input_token_count": malformed,
                "output_token_count": malformed,
            })
            for key in ("ttft_ms", "input_token_count", "output_token_count",
                        "coding_agent.ttft.kind", "coding_agent.usage.source"):
                assert key not in actual, (signal, malformed, key, actual)
            assert private not in json.dumps(actual)

        _, actual = project(signal, {"service.name": "codex_exec"},
                            "codex.sse_event", {
            "event.name": "codex.sse_event", "event.kind": "response.completed",
            "output_token_count": "1.5",
        })
        assert "output_token_count" not in actual
        assert "coding_agent.usage.source" not in actual

        for malformed in (True, -1, "secret", {"secret": private}, [private]):
            _, actual = project(signal, {"service.name": "claude_code"},
                                "claude_code.llm_request", {
                "duration_ms": malformed, "ttft_ms": malformed,
                "first_content_ms": malformed, "speed": malformed,
                "input_tokens": malformed, "output_tokens": malformed,
                "openai.request.service_tier": malformed,
                "effort": malformed, "coding_agent.model.variant": private + "!",
                "coding_agent.request.reasoning.budget_tokens": malformed,
            })
            for key in ("coding_agent.request.duration_ms", "ttft_ms",
                        "first_content_ms", "coding_agent.request.service_tier",
                        "coding_agent.request.reasoning.effort",
                        "coding_agent.model.variant",
                        "coding_agent.request.reasoning.budget_tokens"):
                assert key not in actual, (signal, malformed, key, actual)
            assert private not in json.dumps(actual)
            for key in ("duration_ms", "input_tokens", "output_tokens", "speed",
                        "openai.request.service_tier"):
                assert key not in actual, (signal, malformed, key, actual)

        _, actual = project(signal, {"service.name": "oh-my-pi"},
                            "chat model" if signal == "traces" else
                            "gen_ai.client.inference.operation.details", {
            "gen_ai.response.time_to_first_chunk": 0.5,
            "gen_ai.provider.name": "openai",
            "coding_agent.request.reasoning.budget_tokens": 1.5,
            "openai.request.service_tier": "unsupported-secret",
        })
        assert actual["ttft_ms"] == 500
        assert actual["coding_agent.ttft.kind"] == "provider_reported"
        assert "coding_agent.request.service_tier" not in actual
        assert "coding_agent.response.service_tier" not in actual
        assert "coding_agent.harness.version" not in actual
        assert "coding_agent.request.reasoning.budget_tokens" not in actual

        _, actual = project(signal, {"service.name": "unrelated"},
                            "claude_code.llm_request", {
            "duration_ms": 100, "speed": "fast", "effort": "high",
        })
        assert "coding_agent.request.duration_ms" not in actual
        assert "coding_agent.request.service_tier" not in actual
        assert "coding_agent.signal_source" not in actual

        for malformed_count in (True, 1.5, 1e100, {"secret": private}):
            _, actual = project(signal, {"service.name": "oh-my-pi"},
                                "chat" if signal == "traces" else
                                "gen_ai.client.inference.operation.details", {
                "gen_ai.usage.output_tokens": malformed_count,
            })
            assert "gen_ai.usage.output_tokens" not in actual
            assert "coding_agent.usage.source" not in actual
            assert private not in json.dumps(actual)

    for duration_ns in (250_000_000, 0):
        _, actual = project("traces", {"service.name": "oh-my-pi"},
                            "chat model", {"gen_ai.usage.output_tokens": 7}, {
            "startTimeUnixNano": "1700000000000000000",
            "endTimeUnixNano": str(1700000000000000000 + duration_ns),
        })
        if duration_ns:
            assert actual["coding_agent.request.duration_ms"] == 250
            assert actual["coding_agent.duration.kind"] == "logical_call"
            assert actual["coding_agent.usage.source"] == "reported"
        else:
            assert "coding_agent.request.duration_ms" not in actual
            assert "coding_agent.duration.kind" not in actual

    _, actual = project("logs", {"service.name": "oh-my-pi"},
                        "gen_ai.client.inference.operation.details", {
        "duration_ms": 250, "gen_ai.usage.output_tokens": 7,
    })
    assert "coding_agent.request.duration_ms" not in actual
    assert "coding_agent.duration.kind" not in actual

    name, attrs = project("traces", {}, "coding_agent.interaction", {
        "coding_agent.interaction.first_response_ms": 0,
        "coding_agent.interaction.latency.kind": "hook_submit_to_display_ready",
    })
    assert name == "coding_agent.interaction"
    assert attrs["coding_agent.interaction.first_response_ms"] == 0
    assert attrs["coding_agent.interaction.latency.kind"] == "hook_submit_to_display_ready"


if __name__ == "__main__":
    test_private_arrays_removed_without_losing_usage()
    test_invalid_json_is_rejected()
    test_receipt_requires_ack_and_never_labels_private_clients()
    print("test_trace_privacy: ok (private arrays removed; usage preserved; invalid JSON rejected)")
