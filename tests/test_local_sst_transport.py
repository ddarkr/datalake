"""Consumer boundary checks; run: python -m unittest tests.test_local_sst_transport."""
import io
import json
import os
import sys
import types
import unittest
import urllib.error
from unittest.mock import patch

from scripts.analytics import aggregate as agg
from scripts.analytics.battery import battery_runtime as battery
from scripts.database import greptime_flight as flight


class LocalSstTransportTests(unittest.TestCase):
    def test_missing_endpoint_never_becomes_missing_table(self):
        for module in (agg, battery):
            error = urllib.error.HTTPError("http://localhost/v1/local-sql", 404,
                                           "not found", {}, io.BytesIO(b"not found"))
            with patch.dict(os.environ, AGG_LOCAL_SST_ONLY="1"), patch.object(
                    module.urllib.request, "urlopen", side_effect=error) as request:
                with self.assertRaises(module.LocalSstTransportError) as caught:
                    module.request_sql("http://localhost", "", "test", "SELECT * FROM raw")
                self.assertFalse(module.is_missing_table(caught.exception))
                self.assertEqual(request.call_count, 1)

    def test_invalid_configuration_fails_before_network(self):
        for value in ("", "true", "false", "2", " 1", "01"):
            with patch.dict(os.environ, AGG_LOCAL_SST_ONLY=value):
                with self.assertRaisesRegex(agg.SqlError, "AGG_LOCAL_SST_ONLY"):
                    agg.load_cfg()
                for module, error in ((agg, agg.SqlError), (battery, battery.BatteryError)):
                    with patch.object(module.urllib.request, "urlopen") as request:
                        with self.assertRaisesRegex(error, "AGG_LOCAL_SST_ONLY"):
                            module.request_sql("http://localhost", "", "test", "SELECT 1")
                        request.assert_not_called()

    def test_strict_action_failure_after_terminal_fence_is_not_success(self):
        calls, decoded = [], []

        class RpcError(Exception):
            def details(self):
                return "unknown action"

        class Data:
            @staticmethod
            def FromString(raw):
                decoded.append(raw)
                return types.SimpleNamespace(data_header=b"", data_body=b"", app_metadata=b"proof")

        class Result:
            @staticmethod
            def FromString(raw):
                self.assertEqual(raw, b"result-envelope")
                return types.SimpleNamespace(body=b"original-flight-data")

        class Channel:
            def unary_stream(self, method, request_serializer, response_deserializer):
                def call(request, **kwargs):
                    calls.append((method, request_serializer(request), kwargs))

                    def frames():
                        yield response_deserializer(b"result-envelope")
                        raise RpcError()
                    return frames()
                return call

            def close(self):
                pass

        grpc = types.SimpleNamespace(RpcError=RpcError, insecure_channel=lambda *args: Channel())

        def messages():
            flight._descriptors["result"] = Result
            return Data, None

        with patch.dict(os.environ, AGG_LOCAL_SST_ONLY="1"), patch.dict(
                sys.modules, {"grpc": grpc, "pyarrow": types.SimpleNamespace()}), patch.object(
                flight, "_messages", side_effect=messages), patch.object(
                flight, "_parse_upper", return_value={7: 12}), patch.dict(flight._descriptors):
            with self.assertRaisesRegex(flight.FlightError, "unknown action"):
                flight.flight_query("SELECT * FROM raw", endpoint="localhost:4001", db="test",
                                    user="user", password="secret", lower={7: 10})
        self.assertEqual(decoded, [b"original-flight-data"])
        self.assertEqual(len(calls), 1)
        method, request, options = calls[0]
        self.assertEqual(method, flight.FLIGHT_DO_ACTION)
        extensions = json.loads(dict(options["metadata"])[flight.FLOW_EXTENSIONS_HEADER])
        self.assertIn(["flow.incremental_mode", "memtable_only"], extensions)
        self.assertIn(["flow.incremental_after_seqs", '{"7": 10}'], extensions)

    def _snapshot_query(self, *, sink=None, fault=None):
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("pyarrow is required for the actual IPC decoder")
        buffer = pa.BufferOutputStream()
        schema = pa.schema([("timestamp", pa.timestamp("ns")), ("value", pa.int64())])
        with pa.ipc.new_stream(buffer, schema) as writer:
            writer.write_batch(pa.record_batch(
                [pa.array([1001, 1002, 1003, 1004, 1005], type=pa.timestamp("ns")),
                 pa.array([1, 2, 3, 4, 5])], schema=schema))
        messages = list(pa.ipc.MessageReader.open_stream(buffer.getvalue()))
        frames = [types.SimpleNamespace(
            data_header=message.metadata.to_pybytes(),
            data_body=message.body.to_pybytes() if message.body else b"",
            app_metadata=b"") for message in messages]
        proof = types.SimpleNamespace(data_header=b"", data_body=b"", app_metadata=b"proof")
        closed = []

        class RpcError(Exception):
            def details(self):
                return "incomplete snapshot"

        class Channel:
            def unary_stream(self, *args, **kwargs):
                def call(*args, **kwargs):
                    yield from frames
                    if fault == "missing":
                        return
                    yield proof
                    if fault == "after":
                        raise RpcError()
                return call

            def close(self):
                closed.append(True)

        grpc = types.SimpleNamespace(RpcError=RpcError, insecure_channel=lambda *args: Channel())
        with patch.dict(sys.modules, {"grpc": grpc}), patch.object(
                flight, "_messages", return_value=(types.SimpleNamespace(FromString=lambda raw: raw), None)), patch.object(
                flight, "_parse_upper", return_value={7: 12}):
            try:
                return flight.flight_query(
                    "SELECT * FROM raw", endpoint="localhost:4001", db="test",
                    user="user", password="secret", max_rows=2,
                    local_sst_only=False, row_sink=sink)
            finally:
                self.assertEqual(closed, [True])

    def test_snapshot_sink_bounds_chunks_without_truncating(self):
        chunks = []
        cols, rows, upper = self._snapshot_query(
            sink=lambda cols, rows: chunks.append((list(cols), rows)))
        self.assertEqual(cols, ["timestamp", "value"])
        self.assertEqual(rows, [])
        self.assertEqual(upper, {7: 12})
        self.assertEqual([len(rows) for _, rows in chunks], [2, 2, 1])
        self.assertEqual([row for _, rows in chunks for row in rows],
                         [[1001, 1], [1002, 2], [1003, 3], [1004, 4], [1005, 5]])
        with self.assertRaises(flight.RowCapExceeded):
            self._snapshot_query()

    def test_snapshot_sink_never_sees_unproved_network_prefix(self):
        for fault in ("missing", "after"):
            chunks = []
            with self.assertRaises(flight.FlightError):
                self._snapshot_query(sink=lambda *args: chunks.append(args), fault=fault)
            self.assertEqual(chunks, [])

    def test_snapshot_sink_failure_propagates_and_retry_is_complete(self):
        chunks = []

        def fail(cols, rows):
            chunks.extend(rows)
            raise RuntimeError("disk full")

        with self.assertRaisesRegex(RuntimeError, "disk full"):
            self._snapshot_query(sink=fail)
        self.assertEqual(len(chunks), 2)
        retry = []
        self._snapshot_query(sink=lambda cols, rows: retry.extend(rows))
        self.assertEqual(len(retry), 5)


if __name__ == "__main__":
    unittest.main()
