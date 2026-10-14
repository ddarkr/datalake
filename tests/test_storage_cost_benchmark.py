"""Behavioral coverage for the real storage-cost collector and its S3 boundary."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tools" / "benchmark_backup_retention.py"
spec = importlib.util.spec_from_file_location("storage_cost_collector", RUNNER)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class CollectorNativePutRegressionTest(unittest.TestCase):
    def test_native_put_counts_transport_consumption_not_sdk_hashing_reads(self):
        from botocore.hooks import HierarchicalEmitter
        from types import SimpleNamespace

        payload = b"abcdef"
        for attempts in (1, 2):
            for failed in (False, True):
                with self.subTest(attempts=attempts, failed=failed):
                    class Boundary:
                        def __init__(boundary):
                            boundary.meta = SimpleNamespace(events=HierarchicalEmitter())

                        def put_object(boundary, **kw):
                            # SDK checksum/signature preparation reads are not network sends.
                            for _ in range(2):
                                self.assertEqual(kw["Body"].read(), payload)
                                kw["Body"].seek(0)
                            request = SimpleNamespace(body=kw["Body"])
                            for attempt in range(attempts):
                                if attempt:
                                    request.body.seek(0)
                                boundary.meta.events.emit("before-send.s3.PutObject", request=request)
                                if failed and attempt == attempts - 1:
                                    self.assertEqual(request.body.read(3), b"abc")
                                    error = bench._S3Error("RequestTimeout", 408)
                                    error.response["ResponseMetadata"]["RetryAttempts"] = attempts - 1
                                    raise error
                                self.assertEqual(request.body.read(), payload)
                            return {"ResponseMetadata": {"RetryAttempts": attempts - 1}}

                    wrapper = bench.InstrumentedClient(Boundary())
                    mark = wrapper.mark()
                    kwargs = {"Bucket": "b", "Key": "payload", "Body": io.BytesIO(payload),
                              "ContentLength": len(payload)}
                    if failed:
                        with self.assertRaises(bench._S3Error):
                            wrapper.put_object(**kwargs)
                    else:
                        wrapper.put_object(**kwargs)
                    metric = wrapper.io_since(mark)
                    expected_bytes = len(payload) * attempts - (3 if failed else 0)
                    self.assertEqual(metric["requests"], dict.fromkeys(bench.OPS, 0) | {"puts": attempts})
                    self.assertEqual(metric["errors"], dict.fromkeys(bench.OPS, 0) | {"puts": int(failed)})
                    self.assertEqual(metric["bytes"], {"get": 0, "put": expected_bytes, "copy": 0})
                    self.assertEqual(metric["payload"]["put_bytes"], expected_bytes)
                    self.assertEqual(metric["payload"]["requests"], attempts)
                    self.assertEqual(metric["control"]["requests"], 0)
                    self.assertEqual(metric["retries"], attempts - 1)


class CollectorMeterRegressionTest(unittest.TestCase):
    def test_conditional_copy_preserves_bytes_without_inventing_head_calls(self):
        blob = b"actual payload"
        store = bench.CountingStore({"source": blob})
        store.metadata["source"] = {"sha256": "spoofed-user-metadata"}
        wrapper = bench.InstrumentedClient(store)
        head = wrapper.head_object(Bucket="b", Key="source")
        self.assertEqual(head["ChecksumSHA256"], bench._b64_sha(blob))
        self.assertEqual(head["Metadata"]["sha256"], "spoofed-user-metadata")
        marks = [(meter, meter.mark()) for meter in (store, wrapper)]
        response = wrapper.copy_object(Bucket="b", Key="copied",
                                       CopySource={"Bucket": "b", "Key": "source"},
                                       CopySourceIfMatch=head["ETag"])
        self.assertEqual(response["CopyObjectResult"]["ChecksumSHA256"], bench._b64_sha(blob))
        self.assertEqual(store.objects["copied"], blob)
        self.assertEqual(store.metadata["copied"], store.metadata["source"])
        for meter, mark in marks:
            with self.subTest(meter=type(meter).__name__):
                metric = meter.io_since(mark)
                self.assertEqual(metric["requests"], dict.fromkeys(bench.OPS, 0) | {"copies": 1})
                self.assertEqual(metric["errors"], dict.fromkeys(bench.OPS, 0))
                self.assertEqual(metric["bytes"], {"get": 0, "put": 0, "copy": len(blob)})

    def test_failed_conditional_copy_counts_one_attempt_without_head_or_copy_bytes(self):
        for wrapped in (False, True):
            for source, etag, code in (("source", '"stale"', "PreconditionFailed"),
                                       ("missing", None, "NoSuchKey")):
                with self.subTest(wrapped=wrapped, source=source):
                    store = bench.CountingStore({"source": b"payload"})
                    client = bench.InstrumentedClient(store) if wrapped else store
                    if wrapped and source == "source":
                        client.head_object(Bucket="b", Key="source")
                    meters = (store, client) if wrapped else (store,)
                    marks = [(meter, meter.mark()) for meter in meters]
                    with self.assertRaises(bench._S3Error) as caught:
                        client.copy_object(Bucket="b", Key="copied",
                                           CopySource={"Bucket": "b", "Key": source},
                                           CopySourceIfMatch=etag)
                    self.assertEqual(caught.exception.response["Error"]["Code"], code)
                    self.assertNotIn("copied", store.objects)
                    for meter, mark in marks:
                        metric = meter.io_since(mark)
                        self.assertEqual(metric["requests"], dict.fromkeys(bench.OPS, 0) | {"copies": 1})
                        self.assertEqual(metric["errors"], dict.fromkeys(bench.OPS, 0) | {"copies": 1})
                        self.assertEqual(metric["bytes"], {"get": 0, "put": 0, "copy": 0})

    def test_fake_failed_put_counts_consumed_bytes_and_does_not_store_partial_object(self):
        class BrokenBody:
            def __init__(self):
                self.reads = 0

            def read(self, n=-1):
                self.reads += 1
                if self.reads == 1:
                    return b"abc"
                raise OSError("upload source failed")

        store = bench.CountingStore()
        mark = store.mark()
        with self.assertRaisesRegex(OSError, "upload source failed"):
            store.put_object(Bucket="b", Key="payload", Body=BrokenBody(), ContentLength=6000)
        metric = store.io_since(mark)
        self.assertEqual(metric["requests"], dict.fromkeys(bench.OPS, 0) | {"puts": 1})
        self.assertEqual(metric["errors"], dict.fromkeys(bench.OPS, 0) | {"puts": 1})
        self.assertEqual(metric["bytes"], {"get": 0, "put": 3, "copy": 0})
        self.assertNotIn("payload", store.objects)

    def test_sdk_incomplete_get_counts_raw_consumption_and_only_one_failed_request(self):
        from botocore.exceptions import IncompleteReadError
        from botocore.response import StreamingBody

        for partial_read in (False, True):
            with self.subTest(partial_read=partial_read):
                class Boundary:
                    def get_object(self, **kw):
                        return {"ContentLength": 6, "Body": StreamingBody(io.BytesIO(b"abc"), 6)}

                wrapper = bench.InstrumentedClient(Boundary())
                mark = wrapper.mark()
                body = wrapper.get_object(Bucket="b", Key="payload")["Body"]
                if partial_read:
                    self.assertEqual(body.read(2), b"ab")
                with self.assertRaises(IncompleteReadError):
                    body.read()
                with self.assertRaises(IncompleteReadError):
                    body.read()
                metric = wrapper.io_since(mark)
                self.assertEqual(metric["requests"], dict.fromkeys(bench.OPS, 0) | {"gets": 1})
                self.assertEqual(metric["errors"], dict.fromkeys(bench.OPS, 0) | {"gets": 1})
                self.assertEqual(metric["bytes"], {"get": 3, "put": 0, "copy": 0})
                self.assertEqual(metric["retries"], 0)
                body.close()

    def test_runtime_and_fake_copy_meters_each_count_only_the_callers_copy(self):
        with bench.implementation(str(ROOT)) as (bk, _, _):
            store = bench.CountingStore({"source": b"payload"})
            client = bk.CountingS3(store, "backup")
            head = client.head_object(Bucket="b", Key="source")
            fake_mark, runtime_mark = store.mark(), client.mark()
            client.copy_object(Bucket="b", Key="copied",
                               CopySource={"Bucket": "b", "Key": "source"},
                               CopySourceIfMatch=head["ETag"])
            fake, runtime = store.io_since(fake_mark), client.io_since(runtime_mark)
        self.assertEqual(fake["requests"], dict.fromkeys(bench.OPS, 0) | {"copies": 1})
        self.assertEqual(runtime["requests"].get("CopyObject"), 1)
        self.assertEqual(runtime["requests"].get("HeadObject", 0), 0)
        self.assertEqual(runtime["attempts"].get("CopyObject"), 1)
        self.assertEqual(runtime["attempts"].get("HeadObject", 0), 0)
        self.assertEqual(fake["bytes"]["copy"], len(b"payload"))
        self.assertEqual(runtime["bytes"]["copy"]["payload"], len(b"payload"))
        self.assertEqual(runtime["errors"].get("CopyObject", 0), 0)

    def test_missing_historical_api_produces_failure_json_without_borrowing_current(self):
        with tempfile.TemporaryDirectory() as directory:
            baseline = Path(directory) / "baseline"
            shutil.copytree(ROOT / "scripts", baseline / "scripts")
            backup = baseline / "scripts" / "storage" / "backup.py"
            with backup.open("a") as out:
                out.write("\ndel cmd_backup\n")
            with self.assertRaisesRegex(AttributeError, "required implementation API"):
                with bench.implementation(str(baseline)):
                    self.fail("missing baseline API was silently borrowed from current")
            output = Path(directory) / "missing-api.json"
            result = subprocess.run(
                [sys.executable, str(RUNNER), "--baseline-root", str(baseline), "--json-out", str(output)],
                cwd=ROOT, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 1)
            doc = json.loads(output.read_text())
        self.assertEqual(doc["schema"], bench.SCHEMA)
        self.assertEqual(doc["status"], "fail")
        self.assertIn("before measurement failed: AttributeError", doc["failures"])


def backup_env():
    return {"BACKUP_OFFLINE_CONFIRMED": "1", "GREPTIME_HTTP_URL": "http://127.0.0.1:9",
            "BACKUP_KEEP": "30", "GREPTIME_STORAGE_TYPE": "S3", "S3_BUCKET": "fixture",
            "S3_ENDPOINT_URL": "https://bench.invalid", "S3_REGION": "us-east-1",
            "S3_ACCESS_KEY_ID": "fixture", "S3_SECRET_ACCESS_KEY": "fixture",
            "S3_ROOT": "owned/live/", "S3_BACKUP_PREFIX": "owned/backups/",
            "BACKUP_REMOTE_RETENTION_MODE": "off", "_small_extra": b"", "_large_count": 0}


class RealCollectorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory(prefix="storage-cost-") as directory:
            output = Path(directory) / "measurements.json"
            result = subprocess.run(
                [sys.executable, str(RUNNER), "--baseline-root", str(ROOT),
                 "--large-count", "4", "--json-out", str(output)],
                cwd=ROOT, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                raise AssertionError(result.stderr[-4000:])
            cls.measurements = json.loads(output.read_text())

    def test_real_collector_json_and_gross_identical_storage(self):
        doc = self.measurements
        self.assertEqual(doc["schema"], "storage-cost-measurements/1")
        self.assertEqual(doc["status"], "ok")
        self.assertEqual(doc["failures"], [])
        self.assertIs(doc["baseline_available"], True)
        for variant in (doc["before"], doc["current"]):
            self.assertEqual([case["case"] for case in variant["cases"]], list(bench.CASE_IDS))
            self.assertEqual(set(variant["auxiliary"]), set(bench.AUX_IDS))
            first, identical = variant["cases"][:2]
            self.assertGreaterEqual(first["live_bytes"], 5 << 20)
            self.assertGreaterEqual(first["live_objects"], 50)
            self.assertEqual(identical["live_bytes"], first["live_bytes"])
            # Gross storage includes tar, manifest, COMPLETE and pool controls.
            self.assertLessEqual(identical["new_remote_bytes"] / identical["live_bytes"], .02)
            self.assertEqual(identical["copy_bytes"], 0)
            self.assertGreater(identical["new_remote_bytes"], identical["new_payload_bytes"])
            for case in variant["cases"] + list(variant["auxiliary"].values()):
                self.assertIsInstance(case["io"], dict)
                for operation in ("get", "put", "copy"):
                    self.assertEqual(case["io"]["bytes"][operation], case[operation + "_bytes"])
                    self.assertEqual(
                        sum(case["io"][side][operation + "_bytes"] for side in ("payload", "control")),
                        case["io"]["bytes"][operation])
                for op in bench.OPS:
                    self.assertEqual(case["io"]["requests"][op], case[op])
                self.assertEqual(
                    sum(case["io"][side]["requests"] for side in ("payload", "control")),
                    sum(case["io"]["requests"].values()))

    def test_self_comparison_uses_roles_and_original_hashes(self):
        measurements = self.measurements
        self.assertIs(measurements["comparison"]["retention"], True)
        self.assertIs(measurements["comparison"]["role_hashes_match"], True)
        for variant in (measurements["before"], measurements["current"]):
            self.assertEqual(variant["retention_dry_run"]["delete_ids"], ["victim"])
            self.assertEqual(set(variant["retention_dry_run"]["keep"]),
                             {"oldest-retained", "pinned", "newest"})
            self.assertEqual(set(variant["retention_enforced"]["remaining"]),
                             {"oldest-retained", "pinned", "newest"})
            self.assertIs(variant["oracle"]["fresh_restore_hash"], True)
            self.assertEqual({r["role"] for r in variant["oracle"]["restores"]},
                             {"oldest-retained", "pinned", "newest"})
            self.assertGreater(variant["oracle"]["get_bytes"], variant["cases"][0]["live_bytes"])
        # Random raw IDs must not participate in retention or restore comparisons.
        base, current = copy.deepcopy(measurements["before"]), copy.deepcopy(measurements["current"])
        for case in current["cases"]:
            case["backup_id"] = "another-run-" + case["case"]
        self.assertIs(bench.compare_implementations(base, current)["retention"], True)
        base["cases"].reverse()
        self.assertEqual(len(bench.compare_implementations(base, current)["scenarios"]), 4)

    def test_comparison_rejects_every_missing_case(self):
        for missing in bench.CASE_IDS:
            with self.subTest(missing=missing):
                current = copy.deepcopy(self.measurements["current"])
                current["cases"] = [c for c in current["cases"] if c["case"] != missing]
                with self.assertRaisesRegex(ValueError, "required backup cases"):
                    bench.compare_implementations(self.measurements["before"], current)

    def test_comparison_rejects_missing_auxiliary(self):
        current = copy.deepcopy(self.measurements["current"])
        del current["auxiliary"]["can_big"]
        with self.assertRaisesRegex(ValueError, "auxiliary"):
            bench.compare_implementations(self.measurements["before"], current)

    def test_real_raw_ack_replay_and_can_input_denominators(self):
        for variant in (self.measurements["before"], self.measurements["current"]):
            auxiliary = variant["auxiliary"]
            fresh, replay = auxiliary["raw_fresh"], auxiliary["raw_replay"]
            for case in (fresh, replay):
                self.assertEqual(case["triples_seen"], 1)
                self.assertEqual(case["acknowledged"], 1)
                self.assertIs(case["locals_validated"], True)
                self.assertEqual(case["input_objects"], 3)
            self.assertEqual(fresh["input_bytes"], replay["input_bytes"])
            self.assertEqual(fresh["puts"], 3)
            self.assertEqual(fresh["new_remote_bytes"], fresh["input_bytes"])
            for metric in ("puts", "put_bytes", "get_bytes", "new_remote_bytes"):
                self.assertEqual(replay[metric], 0)
            small, big = auxiliary["can_small"], auxiliary["can_big"]
            self.assertEqual(small["input_objects"], 3)
            self.assertEqual(big["input_objects"], 3)
            self.assertGreater(small["input_bytes"], 2 * 256)
            self.assertGreater(big["input_bytes"], 64 * 65536)
            self.assertGreater(big["input_bytes"], small["input_bytes"])
            self.assertEqual(small["get_bytes"], 0)
            self.assertEqual(big["get_bytes"], 0)

    def test_cleanup_failure_turns_success_into_failure_json(self):
        def fail_cleanup(*args):
            raise OSError("cannot delete owned prefix")

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(bench, "bench_impl", side_effect=lambda *a, **kw: copy.deepcopy(self.measurements["current"])), \
                patch.object(bench, "cleanup_owned", side_effect=fail_cleanup):
            output = Path(directory) / "cleanup.json"
            self.assertEqual(bench.main(["--no-baseline", "--json-out", str(output)]), 1)
            doc = json.loads(output.read_text())
        self.assertTrue(doc["current"])
        self.assertEqual(doc["status"], "fail")
        self.assertTrue(any("owned cleanup failed" in error for error in doc["failures"]))

    def test_explicit_no_baseline_keeps_actual_current_measurements(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(bench, "bench_impl", side_effect=lambda *a, **kw: copy.deepcopy(self.measurements["current"])):
            output = Path(directory) / "current-only.json"
            self.assertEqual(bench.main(["--no-baseline", "--json-out", str(output)]), 0)
            doc = json.loads(output.read_text())
        self.assertIs(doc["baseline_available"], False)
        self.assertEqual(doc["baseline"], "absent (--no-baseline)")
        self.assertNotIn("before", doc)
        self.assertNotIn("comparison", doc)
        self.assertEqual(set(doc["current"]["auxiliary"]), set(bench.AUX_IDS))


class CollectorBoundaryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.tmp_path = Path(directory.name)

    def test_restore_rejects_wrong_original_even_when_snapshot_and_copy_agree(self):
        store = bench.CountingStore()
        original = {"sst/a.sst": b"original fixture bytes"}
        env = backup_env()
        with bench.implementation(str(ROOT)) as (bk, _, _), bench._frozen_time():
            bench.seed_live(store, "fixture", "owned/live/", original)
            _, bid, local_hashes, root, e = bench.run_backup(
                bk, store, env, "repeated-identical", "fixture", "owned/live/", "owned/backups/")
            try:
                # A matching snapshot and restored copy cannot replace the original oracle.
                wrong_original = {"sst/a.sst": hashlib.sha256(b"different ORIGINAL").hexdigest()}
                with self.assertRaisesRegex(AssertionError, "original SST fixture hash mismatch"):
                    bench.restore_one(bk, store, e, bid, local_hashes, "newest", "owned/live/", wrong_original)
            finally:
                shutil.rmtree(root)

    def test_raw_processed_return_is_not_an_ack(self):
        class FailedPut(bench.CountingStore):
            def put_object(self, **kw):
                raise bench._S3Error("AccessDenied", 403)

        bench.make_raw_triple(str(self.tmp_path), "20261014T000000Z-seg", 1024)
        original = bench.hash_tree(str(self.tmp_path))
        with bench.implementation(str(ROOT)) as (_, _, raw):
            with self.assertRaisesRegex(AssertionError, "not acknowledged"):
                bench.run_raw_once(raw, FailedPut(), "fixture", str(self.tmp_path), "fixture-vehicle", "raw_fresh")
        self.assertEqual(bench.hash_tree(str(self.tmp_path)), original)

    def test_deterministic_clock_tokens_and_fixture_mtimes_reset(self):
        runs = []
        for index in range(2):
            root = self.tmp_path / str(index)
            with bench._frozen_time():
                first, second = time.time_ns(), time.time_ns()
                token = secrets.token_hex(4)
                bench.make_source(str(root))
                runs.append((first, second, token, bench.hash_tree(str(root))))
            self.assertLess(first, second)
            self.assertTrue(all(path.stat().st_mtime_ns == bench._FROZEN_NS for path in root.rglob("*")))
        self.assertEqual(runs[0], runs[1])

    def test_actual_partial_get_put_errors_retry_and_control_counts(self):
        store = bench.CountingStore({"data": b"abcdef", "x/manifest.json": b"{}"})
        mark = store.mark()
        body = store.get_object(Bucket="b", Key="data")["Body"]
        self.assertEqual(body.read(2), b"ab")
        body.close()
        control = store.get_object(Bucket="b", Key="x/manifest.json")["Body"]
        self.assertEqual(b"".join(control.iter_chunks(1)), b"{}")
        control.close()
        for _ in range(2):
            with self.assertRaises(bench._S3Error):
                store.head_object(Bucket="b", Key="missing")
        store.put_object(Bucket="b", Key="missing", Body=b"new")
        store.head_object(Bucket="b", Key="missing")
        with self.assertRaises(bench._S3Error):
            store.put_object(Bucket="b", Key="bad", Body=b"bad", ChecksumSHA256="invalid")
        metric = store.io_since(mark)
        self.assertEqual(metric["bytes"], {"get": 4, "put": 6, "copy": 0})
        self.assertEqual(metric["payload"]["get_bytes"], 2)
        self.assertEqual(metric["control"]["get_bytes"], 2)
        self.assertEqual(metric["errors"]["heads"], 2)
        self.assertEqual(metric["errors"]["puts"], 1)
        self.assertEqual(metric["retries"], 1)  # two missing HEAD attempts, not post-PUT HEAD
        self.assertEqual(metric["payload"]["requests"], 6)

    def test_real_wrapper_counts_consumed_failed_put_and_sdk_retries(self):
        class Boundary:
            def put_object(boundary, **kw):
                self.assertEqual(kw["Body"].read(3), b"abc")
                error = bench._S3Error("RequestTimeout", 408)
                error.response["ResponseMetadata"]["RetryAttempts"] = 2
                raise error

            def delete_objects(boundary, **kw):
                return {"Errors": [{"Code": "AccessDenied"}]}

        for body in (b"abcdef", io.BytesIO(b"abcdef")):
            with self.subTest(body_type=type(body).__name__):
                wrapper = bench.InstrumentedClient(Boundary())
                mark = wrapper.mark()
                with self.assertRaises(bench._S3Error):
                    wrapper.put_object(Bucket="b", Key="payload", Body=body, ContentLength=6000)
                with self.assertRaisesRegex(OSError, "delete errors"):
                    wrapper.delete_objects(Bucket="b", Delete={"Objects": [{"Key": "payload"}]})
                metric = wrapper.io_since(mark)
                self.assertEqual(metric["bytes"]["put"], 3)  # actual consumption, even on failure
                self.assertEqual(metric["errors"]["puts"], 1)
                self.assertEqual(metric["errors"]["deletes"], 1)
                self.assertEqual(metric["retries"], 2)

    def test_owned_cleanup_removes_backup_and_raw_without_touching_other_runs(self):
        first, other = "benchmark/run-a/current/", "benchmark/run-b/current/"
        raw_first, raw_other = ("raw/vehicle/" + bench.raw_vehicle(prefix) + "/" for prefix in (first, other))
        store = bench.CountingStore({first + "backups/id/COMPLETE": b"{}", first + "live/a.sst": b"sst",
                                     raw_first + "can/a.mf4": b"raw", other + "backups/id/COMPLETE": b"{}",
                                     raw_other + "can/a.mf4": b"other"})
        bench.cleanup_owned(store, "fixture", [first, raw_first])
        self.assertEqual(set(store.objects), {other + "backups/id/COMPLETE", raw_other + "can/a.mf4"})

    def test_owned_cleanup_rejects_delete_errors_and_silent_leftovers(self):
        class DeleteErrors(bench.CountingStore):
            def delete_objects(self, **kw):
                return {"Errors": [{"Code": "AccessDenied"}]}

        class LeavesObjects(bench.CountingStore):
            def delete_objects(self, **kw):
                return {}

        for store in (DeleteErrors({"owned/a": b"data"}), LeavesObjects({"owned/a": b"data"})):
            with self.subTest(store_type=type(store).__name__), self.assertRaises(OSError):
                bench.cleanup_owned(store, "fixture", ["owned/"])

    def test_failure_json_for_missing_baseline_import_and_argument_errors(self):
        for args in ([], ["--baseline-root", "nonexistent-source-tree"],
                     ["--no-baseline", "--small-kb", "not-a-number"]):
            with self.subTest(args=args):
                output = self.tmp_path / "failure.json"
                # Each invocation must write its own JSON, not reuse the previous result.
                output.unlink(missing_ok=True)
                result = subprocess.run([sys.executable, str(RUNNER), "--json-out", str(output)] + args,
                                        cwd=ROOT, capture_output=True, text=True, timeout=60)
                self.assertEqual(result.returncode, 1)
                doc = json.loads(output.read_text())
                self.assertEqual(doc["schema"], bench.SCHEMA)
                self.assertEqual(doc["status"], "fail")
                self.assertTrue(doc["failures"])

    def test_missing_historical_helper_cannot_import_current_tree(self):
        baseline = self.tmp_path / "baseline"
        shutil.copytree(ROOT / "scripts", baseline / "scripts")
        (baseline / "scripts" / "database" / "greptime_preflight.py").unlink()
        with self.assertRaises(ModuleNotFoundError):
            with bench.implementation(str(baseline)):
                self.fail("missing baseline helper was silently borrowed from current")

    def test_every_measurement_exception_is_reported_as_json(self):
        @bench.contextlib.contextmanager
        def broken_implementation(root):
            raise RuntimeError("private endpoint must not reach public JSON")
            yield

        output = self.tmp_path / "exception.json"
        with patch.object(bench, "implementation", broken_implementation):
            self.assertEqual(bench.main(["--no-baseline", "--json-out", str(output)]), 1)
        doc = json.loads(output.read_text())
        self.assertEqual(doc["status"], "fail")
        self.assertTrue(doc["failures"])
        self.assertNotIn("private endpoint", output.read_text())

    def test_partial_get_body_failure_counts_consumed_bytes_and_error(self):
        class BrokenBody:
            def __init__(self):
                self.reads = 0

            def read(self, n=-1):
                self.reads += 1
                if self.reads == 1:
                    return b"ab"
                raise OSError("connection dropped mid-body")

        class Boundary:
            def get_object(self, **kw):
                return {"ContentLength": 6, "Body": BrokenBody()}

        wrapper = bench.InstrumentedClient(Boundary())
        mark = wrapper.mark()
        body = wrapper.get_object(Bucket="b", Key="payload")["Body"]
        self.assertEqual(body.read(2), b"ab")
        with self.assertRaises(OSError):
            body.read(2)
        metric = wrapper.io_since(mark)
        self.assertEqual(metric["requests"]["gets"], 1)
        self.assertEqual(metric["errors"]["gets"], 1)
        self.assertEqual(metric["bytes"]["get"], 2)


if __name__ == "__main__":
    unittest.main()
