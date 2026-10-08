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


if __name__ == "__main__":
    test_private_arrays_removed_without_losing_usage()
    test_invalid_json_is_rejected()
    test_receipt_requires_ack_and_never_labels_private_clients()
    print("test_trace_privacy: ok (private arrays removed; usage preserved; invalid JSON rejected)")
