"""Synthetic wire behavioral regression; no vehicle data or observations.

Covers exact deterministic round-trips, strict metadata/chunk validation,
record-count and byte ceilings, early incremental over-budget preflight,
unknown-field / wrong-wiretype / duplicate-attribute rejection, malformed
and oversize payloads, negative and multibyte values, int64 ns boundaries,
and complete-vs-partial acknowledgement semantics.

Run: python -m unittest discover -s tests -p test_can_otlp_wire.py
"""
import unittest

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

from scripts.ingest.can.can_otlp_wire import (
    MAX_CHUNK_BYTES,
    MAX_RECORDS,
    MAX_REQUEST_BYTES,
    PartialSuccess,
    WireError,
    check_response,
    decode_batch,
    encode_batch,
    encoded_size_estimate,
    success_response,
    validate_meta,
)

META = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
        "session_id": "wire", "started_ns": 1800000000000000000,
        "vehicle_firmware": "synthetic"}


def chunk(seq, data=b"x", offset=None, phase="capture"):
    return {"seq": seq, "offset_ns": seq if offset is None else offset,
            "phase": phase, "data": data}


class WireBehavior(unittest.TestCase):
    def test_round_trip_and_determinism(self):
        chunks = [chunk(0, b"\x00\xff\r"), chunk(1, "雪".encode("utf-8"), offset=11)]
        first, second = encode_batch(META, chunks), encode_batch(META, chunks)
        self.assertEqual(first, second)
        self.assertEqual(decode_batch(first), (META, chunks))

    def test_record_and_byte_boundaries(self):
        exact = [chunk(seq) for seq in range(MAX_RECORDS)]
        payload = encode_batch(META, exact)
        self.assertEqual(decode_batch(payload), (META, exact))
        self.assertLessEqual(len(payload), MAX_REQUEST_BYTES)
        with self.assertRaisesRegex(WireError, "record count"):
            encode_batch(META, exact + [chunk(MAX_RECORDS, offset=MAX_RECORDS)])
        with self.assertRaisesRegex(WireError, "request size"):
            decode_batch(b"x" * (MAX_REQUEST_BYTES + 1))
        with self.assertRaisesRegex(WireError, "request size"):
            decode_batch(b"")

    def test_early_preflight_rejects_overbudget(self):
        over = [chunk(seq, b"x" * MAX_CHUNK_BYTES) for seq in range(32)]
        self.assertGreater(encoded_size_estimate(META, over), MAX_REQUEST_BYTES)
        with self.assertRaisesRegex(WireError, "byte limit"):
            encode_batch(META, over)
        # The estimate never undercounts the real protobuf framing.
        exact = [chunk(seq) for seq in range(MAX_RECORDS)]
        estimate, payload = encoded_size_estimate(META, exact), encode_batch(META, exact)
        self.assertLessEqual(estimate, MAX_REQUEST_BYTES)
        self.assertGreaterEqual(estimate, len(payload))
        tiny = [chunk(0)]
        self.assertGreaterEqual(encoded_size_estimate(META, tiny), len(encode_batch(META, tiny)))

    def test_strict_metadata(self):
        for bad in (dict(META, schema_version=2),
                    dict(META, vehicle="has space"),
                    dict(META, started_ns=0),
                    dict(META, vehicle_firmware="x" * 161)):
            with self.assertRaises(WireError):
                validate_meta(bad)
            with self.assertRaises(WireError):
                encode_batch(bad, [chunk(0)])
        validate_meta(META)

    def test_strict_chunks(self):
        bad_batches = ([],  # empty batch
                       [chunk(0), chunk(0)],  # duplicate seq, not monotonic
                       [dict(chunk(0), phase="control")],
                       [dict(chunk(0), data="not-bytes")],
                       [dict(chunk(0), data=b"x" * (MAX_CHUNK_BYTES + 1))],
                       [dict(chunk(0), offset_ns=-1)],
                       [dict(chunk(0), seq=-1)])
        for chunks in bad_batches:
            with self.assertRaises(WireError):
                encode_batch(META, chunks)

    def test_ns_boundaries(self):
        edge = dict(META, started_ns=2 ** 63 - 1)
        payload = encode_batch(edge, [chunk(0, offset=0)])
        self.assertEqual(decode_batch(payload), (edge, [chunk(0, offset=0)]))
        with self.assertRaisesRegex(WireError, "sequence/timestamp"):
            encode_batch(dict(META, started_ns=2 ** 63 - 10),
                         [chunk(0, offset=11)])

    def test_envelope_strictness(self):
        payload = encode_batch(META, [chunk(0, b"a"), chunk(1, b"b", offset=5)])
        request = ExportLogsServiceRequest.FromString(payload)
        # Unknown record field trips the fixed-schema envelope gate.
        request.resource_logs[0].scope_logs[0].log_records[0].observed_time_unix_nano = 1
        with self.assertRaisesRegex(WireError, "log record envelope"):
            decode_batch(request.SerializeToString())
        # Wrong attribute wire type: seq as string.
        request = ExportLogsServiceRequest.FromString(payload)
        for attribute in request.resource_logs[0].scope_logs[0].log_records[0].attributes:
            if attribute.key == "can.seq":
                attribute.value.string_value = "0"
        with self.assertRaisesRegex(WireError, "attribute type"):
            decode_batch(request.SerializeToString())
        # Duplicate attribute.
        request = ExportLogsServiceRequest.FromString(payload)
        record = request.resource_logs[0].scope_logs[0].log_records[0]
        record.attributes.add().CopyFrom(record.attributes[0])
        with self.assertRaisesRegex(WireError, "duplicate"):
            decode_batch(request.SerializeToString())
        # Tampered timestamp.
        request = ExportLogsServiceRequest.FromString(payload)
        request.resource_logs[0].scope_logs[0].log_records[1].time_unix_nano += 1
        with self.assertRaisesRegex(WireError, "[Ii]nconsistent"):
            decode_batch(request.SerializeToString())
        # Truncated bytes.
        with self.assertRaises(WireError):
            decode_batch(payload[:len(payload) // 2])
        with self.assertRaises(WireError):
            decode_batch("not-bytes")

    def test_acknowledgement_semantics(self):
        check_response(success_response())
        with self.assertRaises(WireError):
            check_response(b"not-a-response")
        with self.assertRaises(WireError):
            check_response(b"x" * 65537)
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
            ExportLogsServiceResponse,
        )
        partial = ExportLogsServiceResponse()
        partial.partial_success.rejected_log_records = 1
        with self.assertRaises(PartialSuccess):
            check_response(partial.SerializeToString(deterministic=True))
        warned = ExportLogsServiceResponse()
        warned.partial_success.error_message = "warning"
        with self.assertRaises(PartialSuccess):
            check_response(warned.SerializeToString(deterministic=True))
        self.assertIsNone(check_response(success_response()))


if __name__ == "__main__":
    unittest.main()
