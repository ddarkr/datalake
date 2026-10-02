"""Offline regression for the trace hop; no server, credentials or network."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from trace_privacy import redact


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
    result = redact(json.dumps({"resourceSpans": resources}).encode())
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
            redact(payload)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed/nonfinite JSON must not pass the privacy hop")


if __name__ == "__main__":
    test_private_arrays_removed_without_losing_usage()
    test_invalid_json_is_rejected()
    print("test_trace_privacy: ok (private arrays removed; usage preserved; invalid JSON rejected)")
