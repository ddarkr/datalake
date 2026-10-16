#!/usr/bin/env python3
"""Local DB + S3 download proof: resident cache versus File-primary backup.

All reads, including analytics and remote-only recovery, are metered.
No production resources are used; S3 cache guarantees are working-set bounded.

Experimental fork proof (owned local Docker/MinIO only):
  python tools/benchmark_greptime_egress.py --local-input-only \\
    --greptime-image datalake-greptime:local-sst-mirror-v1 --rows 20000 \\
    --json-out /tmp/greptime-local-input-proof.json
Requires the native --require-local-sst-mirror-v1 startup capability. The
normal restart/cache-loss/replay lifecycle uses real persistent named volumes.
Only the separate filesystem-full case uses a bounded, mount-verified tmpfs.
Expected refusals must preserve source hashes/manifests and then repair to
completed maintenance; absent GC/index/format/restore proof fails acceptance.
"""
import argparse
import datetime
import hashlib
import json
import os
import random
import re
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)
from tools import benchmark_greptime_storage as b


def meter_delta(before, after):
    """Cumulative counters, not the bounded request log, define every interval."""
    counters = {key: value - before["counters"].get(key, 0)
                for key, value in after["counters"].items()}
    classes = {key: value - before.get("response_bytes_by_object", {}).get(key, 0)
               for key, value in after["response_bytes_by_object"].items()}
    if any(value < 0 for value in (*counters.values(), *classes.values())):
        raise RuntimeError("meter counters moved backwards")
    if sum(classes.values()) != counters["response_bytes"]:
        raise RuntimeError("response class totals do not reconcile")
    if (counters["payload_read_bytes"] + counters["metadata_read_bytes"]
            + counters["unknown_read_bytes"] != counters["read_bytes"]):
        raise RuntimeError("GET classifications do not reconcile")
    return dict(counters, response_bytes_by_object=classes)


def combine_costs(costs, excluded=()):
    """Merge independent meters; exclusions are disjoint subsets, never totals."""
    total = {}
    classes = {}
    for sign, intervals in ((1, costs), (-1, excluded)):
        for interval in intervals:
            for key, value in interval.items():
                if key == "response_bytes_by_object":
                    for kind, count in value.items():
                        classes[kind] = classes.get(kind, 0) + sign * count
                else:
                    total[key] = total.get(key, 0) + sign * value
    if any(value < 0 for value in (*total.values(), *classes.values())):
        raise RuntimeError("overlapping or invalid meter exclusions")
    if sum(classes.values()) != total["response_bytes"]:
        raise RuntimeError("merged response class totals do not reconcile")
    return dict(total, response_bytes_by_object=classes)


def proven_zero_payload(cost):
    return cost["payload_read_bytes"] == 0 and cost["unknown_read_bytes"] == 0



def prove_repository_paths(c):
    import base64
    import datetime as dt
    import math
    from pathlib import Path
    import subprocess
    from unittest.mock import patch
    from scripts.analytics import aggregate as agg
    from scripts.database import db_init
    from scripts.storage import backup
    from tools.s3_meter import S3Meter
    bench = b
    ctx = (c['base'], base64.b64encode(('%s:%s' % c['auth']).encode()).decode(), 'public')
    phases = {}

    def measure(name, action, kind="analytics_nonquery"):
        before = bench.snap_get(c['snap_port'])
        result = action()
        after = bench.snap_get(c['snap_port'])
        delta = meter_delta(before, after)
        kind = "user_query" if "user_query" in name else kind
        phases[name] = dict(delta, result=result, kind=kind)
        if kind == "user_query":
            c["query_costs"].append(delta)
        bench.validate_meter_errors(after)
        return result

    sql = lambda statement: bench.gsql(c['base'], statement, auth=c['auth'])
    keep = {'vehicle_signal', 'vehicle_agg', 'trip_summary', 'charge_session',
            'vehicle_signal_dirty', 'vehicle_aggregate_checkpoint'}
    for name, ddl in db_init.ddl_statements(''):
        if name in keep:
            sql(ddl)
    origin = dt.datetime(2026, 1, 2)
    paths = {'speed': 'Vehicle.Speed', 'soc': '', 'drive_energy': '',
             'charge_energy': '', 'power': '', 'charging': ''}
    cfg = {'vehicle': 'proof-car', 'lookback_h': 3, 'max_rows': 200000,
           'paths': paths, 'trip_gap_min': 10, 'charge_gap_min': 30,
           'trip_min_speed_kph': 1.0, 'signal_max_age_s': 300,
           'battery_config': '', 'interval_s': 300}

    def ingest():
        for hour in range(2):
            values = []
            for minute in range(60):
                at = origin + dt.timedelta(hours=hour, minutes=minute)
                speed = 36 if 1 <= minute <= 10 else 0
                values.append("('%s','proof-car','Vehicle.Speed','can','proof-%s-%s',"
                              "'proof-epoch',%s,'km/h','Speed','valid')" %
                              (at.strftime('%Y-%m-%d %H:%M:%S'), hour, minute, speed))
            sql('INSERT INTO vehicle_signal(event_time,vehicle,path,source,event_id,'
                'decode_epoch,value_num,unit,source_field,quality) VALUES ' + ','.join(values))
            at = origin + dt.timedelta(hours=hour)
            sql("INSERT INTO vehicle_signal_dirty VALUES ('%s','proof-car','can',"
                "'proof-epoch','generation-1','%s')" % (at, at))
            sql("ADMIN flush_table('vehicle_signal')")
        return {'raw_rows': 120, 'dirty_hours': 2}

    measure('repository_ingest_flush', ingest, kind="nonquery")
    now = origin + dt.timedelta(hours=3)
    completed = measure('repository_dirty_rollup_trip',
                        lambda: agg.vehicle_history_section(ctx, cfg, now=now))
    if completed != 2:
        raise AssertionError('actual CAN coordinator did not checkpoint both hours')
    repeated = measure('repository_unchanged_dirty',
                       lambda: agg.vehicle_history_section(ctx, cfg, now=now))
    if repeated != 0:
        raise AssertionError('unchanged notification replayed raw work')

    def oracle():
        raw = bench.grecords(c['base'], 'SELECT COUNT(*) AS n, SUM(value_num) AS total FROM vehicle_signal', auth=c['auth'])
        assert int(raw[0]['n']) == 120 and float(raw[0]['total']) == 720
        hour = bench.grecords(c['base'], "SELECT avg_value,min_value,max_value,sample_count FROM vehicle_agg WHERE resolution='1h' AND vehicle='proof-car' ORDER BY window_start", auth=c['auth'])
        assert len(hour) == 2
        assert all(math.isclose(float(r['avg_value']), 6) and float(r['min_value']) == 0 and float(r['max_value']) == 36 and int(r['sample_count']) == 60 for r in hour)
        trips = bench.grecords(c['base'], "SELECT duration_s,distance_km FROM trip_summary WHERE vehicle='proof-car' ORDER BY started_at", auth=c['auth'])
        assert len(trips) == 2
        assert all(math.isclose(float(r['duration_s']), 600) and math.isclose(float(r['distance_km']), 5.7) for r in trips)
        return {'raw_rows': 120, 'hourly_values_exact': True, 'trip_count': 2, 'trip_duration_seconds': 600, 'trip_distance_km': 5.7}

    measure('repository_user_query_oracle', oracle)
    rc, _, err = bench.compose_run(c['tmp'], c['project'], c['env'], 'stop', '--timeout', '60', 'greptimedb')
    if rc:
        raise RuntimeError('owned DB stop failed: ' + bench._redact(err))
    rc, container, err = bench.compose_run(c['tmp'], c['project'], c['env'], 'ps', '--all', '--quiet', 'greptimedb')
    if rc or not container:
        raise RuntimeError('owned DB identity missing')
    source_root = Path(c['tmp'], 'offline-backup-proof')
    data = source_root / 'greptime-data'
    etc = source_root / 'greptime-etc'
    backup_dir = source_root / 'backup'
    for path in (data, etc, backup_dir):
        path.mkdir(parents=True, mode=0o700, exist_ok=False)
    proc = subprocess.run(['docker', 'cp', container + ':/data/.', str(data)], capture_output=True, text=True, env=bench.clean_env(), timeout=120)
    if proc.returncode:
        raise RuntimeError('copy owned offline data failed')
    import shutil
    shutil.copy2(Path(c['tmp'], 'greptimedb.toml'), etc / 'greptimedb.toml')
    shutil.copytree(Path(c['tmp'], 'preflight', 'auth'), etc / 'auth')
    meter = S3Meter(c['minio_target']).start()
    backup_start = meter.snapshot()
    try:
        settings = {
            'BACKUP_DIR': str(backup_dir), 'BACKUP_SOURCE_DIRS': str(data) + ':' + str(etc),
            'BACKUP_OFFLINE_CONFIRMED': '1', 'GREPTIME_HTTP_URL': c['base'],
            'GREPTIME_STORAGE_TYPE': c['storage_type'], 'S3_ENDPOINT_URL': meter.endpoint,
            'S3_BUCKET': bench.BUCKET, 'S3_ROOT': 'greptime', 'S3_BACKUP_PREFIX': 'backup-proof',
            'S3_REGION': bench.REGION, 'S3_ACCESS_KEY_ID': c['access'],
            'S3_SECRET_ACCESS_KEY': c['secret'], 'BACKUP_RETENTION_MODE': 'off',
            'BACKUP_KEEP': '3', 'BACKUP_RESERVE_BYTES': '1048576',
        }
        endpoint_check = backup.require_https_endpoint
        def local_endpoint(value, name):
            # Sole test-only transport override: owned loopback MinIO, no TLS.
            if value == meter.endpoint:
                return value
            return endpoint_check(value, name)
        with patch.dict(os.environ, settings), patch.object(backup, 'require_https_endpoint', local_endpoint):
            for label in ('first', 'unchanged'):
                before = meter.snapshot()
                prior_reports = set(backup_dir.glob('s3-cost-backup-cmd_backup-*.json'))
                prior_archives = set(backup_dir.glob('greptime-backup-*.tar.gz'))
                result = backup.main(['backup', 'backup'])
                assert result == 0, 'real backup CLI failed'
                if c['storage_type'] == 'File':
                    archive, = set(backup_dir.glob('greptime-backup-*.tar.gz')) - prior_archives
                    client = backup.make_s3_client()
                    remote_key = 'backup-proof/file/' + archive.name
                    backup.s3_put_file_verified(client, bench.BUCKET, remote_key, str(archive),
                                               backup.sha256_file(str(archive)), archive.stat().st_size)
                    backup.s3_put_bytes_verified(client, bench.BUCKET, remote_key + '.sha256',
                                                Path(str(archive) + '.sha256').read_bytes())
                    sdk = client.snapshot()
                after = meter.snapshot()
                delta = meter_delta(before, after)
                delta['kind'] = 'backup_nonquery'
                delta['requests_by_operation'] = {k: v - before['op_requests'].get(k, 0) for k, v in after['op_requests'].items()}
                delta['server_copy_bytes_known'] = False
                delta['test_transport_override'] = 'HTTPS-only endpoint check accepts owned loopback HTTP; checksums/HEAD/COPY unchanged'
                if c['storage_type'] == 'S3':
                    new_reports = set(backup_dir.glob('s3-cost-backup-cmd_backup-*.json')) - prior_reports
                    assert len(new_reports) == 1, 'backup SDK accounting missing'
                    sdk = json.loads(new_reports.pop().read_text())['snapshot']
                delta['sdk_bytes'] = sdk['bytes']
                delta['sdk_unknown'] = sdk['unknown']
                delta['zero_payload_proven'] = proven_zero_payload(delta) and (
                    sdk['bytes']['get']['payload'] == 0 and not any(sdk['unknown'].values()))
                phases['repository_backup_' + label] = delta
                # Unknown or payload bytes invalidate zero, not evidence capture.
            assert phases['repository_backup_unchanged']['requests_by_operation'].get('COPY', 0) == 0
            if c['storage_type'] == 'File':
                before = meter.snapshot()
                downloaded = source_root / 'downloaded'
                restored = source_root / 'restored'
                downloaded.mkdir(mode=0o700)
                for name in backup.LABELS:
                    (restored / name).mkdir(parents=True, mode=0o700)
                with client.get_object(Bucket=bench.BUCKET, Key=remote_key)['Body'] as body:
                    with (downloaded / archive.name).open('wb') as destination:
                        shutil.copyfileobj(body, destination, 65536)
                sidecar = backup.s3_get_control(client, bench.BUCKET, remote_key + '.sha256',
                                               cap=backup.SIDECAR_CAP)
                (downloaded / (archive.name + '.sha256')).write_bytes(sidecar)
                with patch.dict(os.environ, {'BACKUP_DIR': str(downloaded),
                                             'BACKUP_RESTORE_BASE': str(restored),
                                             'BACKUP_FILE': archive.name}):
                    assert backup.main(['backup', 'restore']) == 0, 'real File restore CLI failed'
                after = meter.snapshot()
                restore_port = bench.free_port()
                restored_base = 'http://127.0.0.1:%d' % restore_port
                restore_container = c['project'] + '-recovery'
                command = ['docker', 'run', '-d', '--rm', '--name', restore_container,
                           '--label', 'com.docker.compose.project=' + c['project'],
                           '--network', c['project'] + '_default', '--user', '0:0',
                           '-p', '127.0.0.1:%d:4000' % restore_port,
                           '-v', str(restored / 'greptime-data') + ':/data',
                           '-v', str(restored / 'greptime-etc') + ':/etc/greptime:ro',
                           bench.GREPTIME_IMAGE_DEFAULT, 'standalone', 'start',
                           '--config-file', '/etc/greptime/greptimedb.toml']
                rc, _, err = bench.sh(command)
                if rc:
                    raise RuntimeError('owned recovery DB start failed: ' + bench._redact(err))
                original_base = c['base']
                try:
                    bench.gwait(restored_base, auth=c['auth'])
                    c['base'] = restored_base
                    recovered_vehicle = oracle()
                    observed = bench.grecords(restored_base, 'SELECT COUNT(*) AS n,SUM(value_num) AS total FROM ' + bench.TABLE, auth=c['auth'])[0]
                    expected_sum = sum(bench.fixture_row(i, c['rows'])[4] for i in range(c['rows']))
                    assert int(observed['n']) == c['rows'] and math.isclose(float(observed['total']), expected_sum)
                    restore_cost = meter_delta(before, after)
                    c["restore_costs"].append(restore_cost)
                    phases['repository_disaster_recovery_boundary'] = dict(
                        restore_cost, kind='restore_exception',
                        archive_sha256_verified=True, native_recovered_rows=int(observed['n']),
                        vehicle_oracle=recovered_vehicle,
                        note='Remote-only disaster recovery retrieval remains in overall totals.')
                finally:
                    c['base'] = original_base
                    rc, _, err = bench.sh(['docker', 'stop', '--time', '60', restore_container])
                    if rc:
                        raise RuntimeError('owned recovery DB stop failed: ' + bench._redact(err))
    finally:
        backup_end = meter.snapshot()
        c["external_meters"].append({
            "source": "repository_backup_sdk", "meter_before": backup_start,
            "meter_after": backup_end, "cost": meter_delta(backup_start, backup_end)})
        meter.stop()
    before = bench.snap_get(c['snap_port'])
    rc, _, err = bench.compose_run(c['tmp'], c['project'], c['env'], 'start', 'greptimedb')
    if rc:
        raise RuntimeError('owned DB restart failed: ' + bench._redact(err))
    bench.gwait(c['base'], auth=c['auth'])
    after = bench.snap_get(c['snap_port'])
    phases['repository_restart_after_backup'] = dict(
        meter_delta(before, after), kind="nonquery")
    measure('repository_post_backup_user_query', oracle)
    return phases
def size_bytes(value):
    m = re.fullmatch(r"([1-9][0-9]*)(B|KiB|MiB|GiB|KB|MB|GB)", value)
    if not m:
        raise argparse.ArgumentTypeError("use an integer size such as 64KiB or 1GiB")
    return int(m[1]) * {"B": 1, "KiB": 1024, "MiB": 1024**2,
                      "GiB": 1024**3, "KB": 1000, "MB": 1000**2,
                      "GB": 1000**3}[m[2]]


def maintenance_physical_inventory(objects, active_rows, known_rows):
    """Actual LIST sizes, not live-manifest sizes, describe stored objects."""
    from tools.s3_meter import classify_object
    classes = {}
    stored_ssts = {}
    for key, size in objects:
        kind = classify_object("/" + b.BUCKET + "/" + key)
        group = classes.setdefault(kind, {"objects": 0, "bytes": 0})
        group["objects"] += 1
        group["bytes"] += int(size)
        if kind == "sst":
            stored_ssts[key.rsplit("/", 1)[-1].rsplit(".", 1)[0]] = int(size)
    active_ids = {str(row["file_id"]) for row in active_rows}
    known_retired = {str(row["file_id"]) for row in known_rows} - active_ids
    present_retired = known_retired.intersection(stored_ssts)
    nonactive = set(stored_ssts) - active_ids
    return {
        "stored_object_count": sum(v["objects"] for v in classes.values()),
        "stored_object_bytes": sum(v["bytes"] for v in classes.values()),
        "stored_by_class": classes,
        "stored_sst_files": len(stored_ssts),
        "stored_sst_bytes": sum(stored_ssts.values()),
        "active_manifest_files": len(active_rows),
        "active_manifest_bytes": sum(int(row["file_size"]) for row in active_rows),
        "active_manifest_index_bytes": sum(int(row.get("index_file_size") or 0) for row in active_rows),
        "stored_nonactive_sst_files": len(nonactive),
        "stored_nonactive_sst_bytes": sum(stored_ssts[key] for key in nonactive),
        "known_retired_sst_still_stored": len(present_retired),
        "known_retired_sst_absent": len(known_retired - set(stored_ssts)),
        "gc_sst_state": "pending_retired_sst_objects_present" if present_retired else "no_known_retired_sst_objects_present",
        "index_and_orphan_gc": "unverified",
        "unreferenced_objects_are_not_proven_orphans": True,
        "physical_storage_bound_proven": False,
    }


def maintenance_manifest_state(rows):
    """SQL manifest scans are unordered; row permutations are not SST changes."""
    return tuple(sorted(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows))


def maintenance_range_inputs(rows, start=None, end=None):
    """Count direct SST overlaps only; native dependency expansion may add more."""
    if start is None and end is None:
        return len(rows)
    lower = b._to_ms(start) if start is not None else None
    upper = b._to_ms(end) if end is not None else None
    if (start is not None and lower is None) or (end is not None and upper is None):
        raise ValueError("invalid maintenance range")
    count = 0
    for row in rows:
        if row.get("min_ts") is None or row.get("max_ts") is None:
            return None
        # information_schema.ssts_manifest stores bounds as epoch nanoseconds.
        minimum, maximum = int(row["min_ts"]), int(row["max_ts"])
        if (lower is None or maximum >= lower * 1000000) and (
                upper is None or minimum < upper * 1000000):
            count += 1
    return count


def maintenance_cache_budgets(total):
    """Pinned 1.2.1 file_cache.rs split_cache_capacities, default index20%."""
    minimum = min(512 * 1024**2, total // 2)
    index = min(max(total * 20 // 100, minimum), total - minimum)
    return {"parquet_bytes": total - index, "index_bytes": index,
            "source": "Greptime1.2.1 file_cache.rs split_cache_capacities; not a residency guarantee"}


def maintenance_writes(cycle, rows):
    """Distinct hourly keys plus exact replays and late corrections/NULL."""
    writes = [(b.BASE_MS + cycle * b.HOUR_MS + i, "v%d" % (i % 4), "p%d" % (i % 5),
               float((i + cycle) % 1000),
               hashlib.shake_256(("%d:%d" % (cycle, i)).encode()).hexdigest(128))
              for i in range(rows)]
    writes.extend(writes[:min(8, rows)])
    if cycle:
        writes.extend((b.BASE_MS + (cycle - 1) * b.HOUR_MS + i,
                       "v%d" % (i % 4), "p%d" % (i % 5),
                       None if i % 2 == 0 else float(-cycle - i), "late-%d-%d" % (cycle, i))
                      for i in range(min(8, rows)))
    return writes


def maintenance_verify(records, oracle):
    got = records("SELECT event_time,vehicle,path,value_num,value_text FROM %s "
                  "ORDER BY event_time,vehicle,path" % b.TABLE)
    actual = {(b._to_ms(row["event_time"]), row["vehicle"], row["path"]):
              (b._num(row["value_num"]), row["value_text"]) for row in got}
    if len(got) != len(oracle) or actual != oracle:
        raise RuntimeError("maintenance exact raw last-row/NULL oracle mismatch")
    expected = {}
    for (stamp, vehicle, path), (value, _) in oracle.items():
        key = (stamp // b.HOUR_MS * b.HOUR_MS, vehicle, path)
        group = expected.setdefault(key, [0, 0, 0.0, None, None])
        group[0] += 1
        if value is not None:
            group[1] += 1
            group[2] += value
            group[3] = value if group[3] is None else min(group[3], value)
            group[4] = value if group[4] is None else max(group[4], value)
    for group in expected.values():
        if group[1] == 0:
            group[2] = None
    grouped = records("SELECT %s AS bucket,vehicle,path,COUNT(*) AS n,COUNT(value_num) AS nn,"
                      "SUM(value_num) AS s,MIN(value_num) AS lo,MAX(value_num) AS hi "
                      "FROM %s GROUP BY %s,vehicle,path" % (b.BIN1H, b.TABLE, b.BIN1H))
    observed = {(b._to_ms(row["bucket"]), row["vehicle"], row["path"]):
                [b._num(row[name]) for name in ("n", "nn", "s", "lo", "hi")] for row in grouped}
    if observed != expected:
        raise RuntimeError("maintenance native hourly summary oracle mismatch")
    return {"exact_rows": len(actual), "hour_groups": len(expected), "null_correction_late_pass": True}


def run_variant(label, rows, cache, small_bytes, large_bytes, result, storage_type="S3", background_policy=None,
                maintenance_policy=None, greptime_image=None, local_input_only=False, scenario=None):
    tmp = tempfile.mkdtemp(prefix="gegress-")
    project = "gbench-gbench%d%d" % (os.getpid(), random.getrandbits(32))
    creds = {k: "bench" + os.urandom(16).hex() for k in
             ("db_password", "admin_password", "otlp_password", "s3_password")}
    creds.update(db_user="bench", otlp_user="bench", s3_user="bench" + os.urandom(4).hex())
    access, secret = creds["s3_user"], creds["s3_password"]
    auth = (creds["db_user"], creds["db_password"])
    env = {k: str(b.free_port()) for k in ("MINIO_PORT", "GREPTIME_PORT", "SNAP_PORT")}
    if local_input_only:
        env["GREPTIME_GRPC_PORT"] = str(b.free_port())
    base = "http://127.0.0.1:" + env["GREPTIME_PORT"]
    target = "http://127.0.0.1:" + env["MINIO_PORT"]
    snap_port = int(env["SNAP_PORT"])
    evidence = {}
    lifecycle_start = None
    external_meters, query_costs, restore_costs = [], [], []
    comparison_costs = []
    maintenance_inventory = None
    meter_proxy_port = b.free_port() if maintenance_policy is not None else None
    result.update(label=label, project=project, phases=[], evidence=evidence,
                  settings={"storage_type": storage_type, "write_cache_size": cache,
                            "page_cache_size": "1MiB", "greptime_image": greptime_image or b.GREPTIME_IMAGE_DEFAULT,
                            "minio_pin": b.MINIO_PIN, "rows": rows})
    result["settings"]["background_policy"] = background_policy

    def command(*args):
        rc, out, err = b.compose_run(tmp, project, env, *args)
        if rc:
            raise RuntimeError("owned compose command failed: " + b._redact(err))
        return out

    def sql(text):
        return b.gsql(base, text, timeout=300, auth=auth)

    def records(text):
        return b.grecords(base, text, timeout=300, auth=auth)

    def inventory():
        if storage_type == "File":
            return [(r["file_path"], int(r["file_size"])) for r in records(
                "SELECT file_path,file_size FROM information_schema.ssts_manifest"
                " WHERE visible=true AND table_id IN (SELECT table_id FROM information_schema.tables"
                " WHERE table_schema='public' AND table_name='%s')" % b.TABLE)]
        return [(k, n) for k, n in b.s3_list_all(target, access, secret)
                if k.endswith(".parquet")]

    def phase(name, kind, fn):
        before = b.snap_get(snap_port)
        entry = {"name": name, "kind": kind, "status": "running"}
        result["phases"].append(entry)
        start = time.monotonic()
        try:
            entry["result"] = fn()
            entry["status"] = "success"
        except Exception as exc:
            entry.update(status="failure", error=b._redact(str(exc)))
            raise
        finally:
            after = b.snap_get(snap_port)
            entry["seconds"] = time.monotonic() - start
            entry["meter_before"] = before
            entry["meter_after"] = after
            entry["counters_delta"] = {k: after["counters"].get(k, 0) - v
                                       for k, v in before["counters"].items()}
            entry["cost"] = meter_delta(before, after)
            if kind in ("user_query", "verification_query"):
                query_costs.append(entry["cost"])
            if kind == "remote_restore":
                restore_costs.append(entry["cost"])
            entry["op_requests_delta"] = {k: v - before["op_requests"].get(k, 0)
                                          for k, v in after["op_requests"].items()}
            entry["sst_payload_delta"] = b.sst_query_delta(before, after)
            entry["error_gate"] = b.validate_meter_errors(after)
            if name == "user_cold_query" and storage_type == "S3":
                b.sst_query_delta(before, after, require_cold=True)
        if maintenance_inventory is not None and kind == "maintenance":
            phase(name + "_physical_inventory", "measurement_nonquery", maintenance_inventory)
        return entry["result"]

    def ingest(enforce_bracket=False):
        for seg in range(8):
            lo, hi = rows * seg // 8, rows * (seg + 1) // 8
            for start in range(lo, hi, b.BATCH):
                values = []
                for i in range(start, min(start + b.BATCH, hi)):
                    _, vehicle, path, stamp, value = b.fixture_row(i, rows)
                    ts = datetime.datetime.fromtimestamp(stamp / 1000, datetime.timezone.utc)
                    entropy = hashlib.shake_256(("fixture:%d" % i).encode()).hexdigest(128)
                    values.append("('%s','%s','%s','bench',%r,'%s')" %
                                  (ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], vehicle, path, value, entropy))
                sql("INSERT INTO %s(event_time,vehicle,path,source,value_num,value_text) VALUES %s" %
                    (b.TABLE, ",".join(values)))
            sql("ADMIN flush_table('%s')" % b.TABLE)
        files = inventory()
        n = sum(size for _, size in files)
        if enforce_bracket and not small_bytes < n < large_bytes:
            raise RuntimeError("actual flushed SST bytes %d must exceed small %d and fit large %d; adjust --rows/cache sizes" %
                               (n, small_bytes, large_bytes))
        if len(files) < 2:
            raise RuntimeError("multiple flushed SST files required")
        return {"rows": rows, "sst_bytes": n, "sst_files": len(files),
                "capacity_bracket_pass": True if enforce_bracket else None}

    def compact():
        before = b.compaction_metrics(base, auth)
        old = {k for k, _ in inventory()}
        sql("ADMIN compact_table('%s', 'swcs', 'window=86400')" % b.TABLE)
        deadline, stable, last = time.monotonic() + 300, 0, None
        while time.monotonic() < deadline:
            files = inventory()
            state = tuple(sorted(files))
            stable = stable + 1 if state == last else 0
            last = state
            after = b.compaction_metrics(base, auth)
            if after["failures"] > before["failures"]:
                raise RuntimeError("native compaction failure")
            if stable >= 3 and b.compaction_completed(before, after, old, {k for k, _ in files}):
                return {"before_metrics": before, "after_metrics": after,
                        "new_sst_files": len({k for k, _ in files} - old),
                        "sst_bytes": sum(n for _, n in files), "completed_proven": True}
            time.sleep(2)
        raise RuntimeError("completed compaction with new SST files not observed")

    def aggregate():
        # Reads are analytics, NOT user queries; reruns replace identical keys.
        for dest, source, interval, count, total, minimum, maximum in (
                ("bench_minute", b.TABLE, "1 minute", "COUNT(*)", "SUM(value_num)", "MIN(value_num)", "MAX(value_num)"),
                ("bench_hour", "bench_minute", "1 hour", "SUM(cnt)", "SUM(sum_v)", "MIN(min_v)", "MAX(max_v)")):
            timestamp = "event_time" if source == b.TABLE else "bucket_start"
            bucket = "date_bin(INTERVAL '%s', %s, TIMESTAMP '%s')" % (interval, timestamp, b.ORIGIN_TS)
            grouped = records("SELECT %s AS bucket_start,vehicle,path,%s AS cnt,%s AS sum_v,%s AS min_v,%s AS max_v FROM %s GROUP BY %s,vehicle,path ORDER BY bucket_start,vehicle,path" %
                              (bucket, count, total, minimum, maximum, source, bucket))
            for start in range(0, len(grouped), b.BATCH):
                values = []
                for row in grouped[start:start + b.BATCH]:
                    timestamp, vehicle, path, count_v, sum_v, min_v, max_v = list(row.values())
                    ts = datetime.datetime.fromtimestamp(b._to_ms(timestamp) / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    values.append("('%s','%s','%s',%d,%r,%r,%r)" %
                                  (ts, vehicle, path, int(b._num(count_v)), b._num(sum_v), b._num(min_v), b._num(max_v)))
                sql("INSERT INTO %s VALUES %s" % (dest, ",".join(values)))
            sql("ADMIN flush_table('%s')" % dest)
        return {"minute_to_hour": "count/sum/min/max preserved; weighted average=sum/count"}

    def verify():
        oracle = b.new_stats()
        for i in range(rows):
            seg, vehicle, path, stamp, value = b.fixture_row(i, rows)
            b._fold(oracle, vehicle, path, value, seg, stamp)
        got = records("SELECT COUNT(*) AS n,SUM(value_num) AS s,MIN(value_num) AS lo,MAX(value_num) AS hi FROM " + b.TABLE)[0]
        for expected, actual in zip((oracle["n"], oracle["sum"], oracle["min"], oracle["max"]), got.values()):
            if not b._cell_equal(expected, b._num(actual)):
                raise RuntimeError("raw deterministic oracle mismatch")
        raw = records("SELECT %s AS bucket_start,vehicle,path,COUNT(*) AS cnt,SUM(value_num) AS sum_v,MIN(value_num) AS min_v,MAX(value_num) AS max_v FROM %s GROUP BY %s,vehicle,path ORDER BY bucket_start,vehicle,path" % (b.BIN1H, b.TABLE, b.BIN1H))
        hour = records("SELECT bucket_start,vehicle,path,cnt,sum_v,min_v,max_v FROM bench_hour ORDER BY bucket_start,vehicle,path")
        if len(raw) != len(hour):
            raise RuntimeError("hour group count mismatch")
        for a, z in zip(raw, hour):
            av, zv = list(a.values()), list(z.values())
            if (b._to_ms(av[0]) != b._to_ms(zv[0]) or av[1:3] != zv[1:3]
                    or not all(b._cell_equal(b._num(x), b._num(y)) for x, y in zip(av[3:], zv[3:]))):
                raise RuntimeError("raw/minute/hour statistics diverged")
        return {"raw_count": rows, "raw_sum": oracle["sum"], "hour_groups": len(hour), "accuracy_pass": True}

    def background_scenario():
        from unittest.mock import patch
        from scripts.database import db_init

        def initialize():
            with patch.dict(os.environ, {
                    "GREPTIME_HTTP_URL": base, "GREPTIME_DB": "public",
                    "GREPTIME_USER": auth[0], "GREPTIME_PASSWORD": auth[1],
                    "OTEL_TTL": ""}):
                db_init.main()

        phase("schema_initialization", "nonquery", initialize)
        # Same key as the first real CAN fixture row; on-demand replay replaces it.
        sql("INSERT INTO vehicle_signal(event_time,vehicle,path,source,event_id,"
            "decode_epoch,value_num,unit,source_field,quality) VALUES "
            "('2026-01-02 00:00:00','proof-car','Vehicle.Speed','can',"
            "'proof-0-0','proof-epoch',1,'km/h','Speed','valid')")
        sql("ADMIN flush_table('vehicle_signal')")
        native_before = b.compaction_metrics(base, auth)
        for cycle in range(4):
            phase("ingest_flush_%d" % cycle, "nonquery",
                  lambda: ingest(enforce_bracket=False))
            if background_policy == "legacy":
                phase("scheduled_analytics_%d" % cycle, "analytics_nonquery", aggregate)
            phase("idle_%d" % cycle, "nonquery", lambda: time.sleep(5))
        native_before_restart = b.compaction_metrics(base, auth)
        phase("same_volume_restart", "nonquery",
              lambda: (command("restart", "greptimedb"), b.gwait(base, auth=auth),
                       time.sleep(5)))
        native_after_restart = b.compaction_metrics(base, auth)
        phase("cache_loss_restart", "nonquery",
              lambda: (b.reset_owned_cache(tmp, project, env, evidence),
                       b.gwait(base, auth=auth), time.sleep(5)))
        if background_policy == "deferred":
            phase("cold_schema_reinitialization", "nonquery", initialize)
        native_after = b.compaction_metrics(base, auth)
        if any(m["failures"] for m in (
                native_before, native_before_restart, native_after_restart, native_after)):
            raise RuntimeError("background compaction failed")
        result["background_compactions"] = {
            "before": native_before, "before_restart": native_before_restart,
            "after_restart": native_after_restart, "after_cache_loss_restart": native_after,
            "completed": (native_before_restart["completed"] - native_before["completed"]
                          + native_after_restart["completed"] + native_after["completed"])}
        result["background_sst_files"] = records(
            "SELECT COUNT(*) AS n,SUM(file_size) AS bytes "
            "FROM information_schema.ssts_manifest WHERE visible=true "
            "AND table_id=(SELECT table_id FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='%s')" % b.TABLE)
        routine = [p for p in result["phases"]
                   if p["kind"] in ("nonquery", "analytics_nonquery")]
        # Includes startup, initial DDL, seed writes and asynchronous phase gaps.
        routine_cost = meter_delta(lifecycle_start, routine[-1]["meter_after"])
        result["background_cost"] = routine_cost
        result["background_read_bytes"] = routine_cost["read_bytes"]
        result["background_sst_read_bytes"] = routine_cost["sst_read_bytes"]
        # Foreground analytics remain available but are never hidden as routine zero.
        phase("on_demand_analytics", "explicit_analytics", aggregate)
        phase("on_demand_accuracy", "verification_query", verify)
        _, vehicle, path, stamp, _ = b.fixture_row(0, rows)
        at = datetime.datetime.fromtimestamp(stamp / 1000, datetime.timezone.utc)
        key = "event_time='%s' AND vehicle='%s' AND \"path\"='%s'" % (
            at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], vehicle, path)

        def corrections():
            for value in ("123", "NULL"):
                sql("INSERT INTO %s(event_time,vehicle,\"path\",source,value_num) "
                    "VALUES ('%s','%s','%s','bench',%s)" %
                    (b.TABLE, at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                     vehicle, path, value))
                sql("ADMIN flush_table('%s')" % b.TABLE)

        def last_null():
            values = records("SELECT value_num FROM %s WHERE %s" % (b.TABLE, key))
            if len(values) != 1 or values[0]["value_num"] is not None:
                raise RuntimeError("last_row/latest NULL semantics changed")
            return {"latest_null_preserved": True, "visible_rows_for_key": 1}

        phase("explicit_corrections", "explicit_write", corrections)
        phase("correction_restart", "explicit_write",
              lambda: (command("restart", "greptimedb"), b.gwait(base, auth=auth)))
        phase("latest_null_after_restart", "verification_query", last_null)
        phase("on_demand_compaction", "explicit_compaction", compact)
        phase("latest_null_after_compaction", "verification_query", last_null)
        result["repository_paths"] = prove_repository_paths(
            dict(tmp=tmp, project=project, env=env, base=base, auth=auth,
                 minio_target=target, access=access, secret=secret,
                 external_meters=external_meters, query_costs=query_costs,
                 restore_costs=restore_costs,
                 snap_port=snap_port, creds=creds, evidence=evidence, phase=phase,
                 storage_type="S3", rows=rows))
        result["repository_paths_kind"] = "explicit_analytics_and_offline_backup"
        result["meter_final"] = b.snap_get(snap_port)
        result["error_gate"] = b.validate_meter_errors(result["meter_final"])
        result["status"] = "success"

    def maintenance_scenario():
        from tools.greptime_maintenance_cases import run_boundaries
        nonlocal maintenance_inventory

        def physical_inventory():
            # LIST goes through the same meter; no SST GET warms compaction.
            objects = b.s3_list_all("http://127.0.0.1:%d" % meter_proxy_port, access, secret)
            active = records("SELECT file_id,file_size,index_file_size FROM "
                             "information_schema.ssts_manifest WHERE visible=true")
            known = [row for observation in result.get("maintenance_observations", [])
                     for row in observation["before_files"]]
            summary = maintenance_physical_inventory(objects, active, known)
            disk_code = (
                "import os,json\n"
                "logical=allocated=files=vanished=0\n"
                "def error(exc): raise exc\n"
                "for root,dirs,names in os.walk('/data',onerror=error):\n"
                " allocated+=os.stat(root,follow_symlinks=False).st_blocks*512\n"
                " for name in names:\n"
                "  try: st=os.stat(os.path.join(root,name),follow_symlinks=False)\n"
                "  except FileNotFoundError: vanished+=1; continue\n"
                "  logical+=st.st_size; allocated+=st.st_blocks*512; files+=1\n"
                "print(json.dumps(dict(files=files,logical_bytes=logical,allocated_bytes=allocated,"
                "vanished_during_scan=vanished,atomic_snapshot=False)))\n")
            summary["minio_disk"] = json.loads(command("exec", "-T", "minio", "python3", "-c", disk_code))
            b.owned_cache_scope(tmp, project, env)
            summary["greptime_disk"] = json.loads(command(
                "run", "--rm", "--no-deps", "--entrypoint", "python3", "cache-reset", "-c", disk_code))
            summary["local_disk_scope"] = "owned /data volumes only; allocated bytes include cache/WAL/metadata and MinIO internals"
            result.setdefault("physical_storage_progression", []).append(summary)
            return summary

        maintenance_inventory = physical_inventory

        def identity():
            container = command("ps", "--all", "--quiet", "greptimedb")
            rc, text, _ = b.sh(["docker", "inspect", container])
            if rc:
                raise RuntimeError("owned DB identity inspection failed")
            obj, = json.loads(text)
            if obj["Config"]["Labels"]["com.docker.compose.project"] != project:
                raise RuntimeError("owned DB project mismatch")
            return obj["Id"], sorted(m["Name"] for m in obj["Mounts"] if m["Type"] == "volume")

        def restart():
            before = identity()
            command("stop", "--timeout", "60", "greptimedb")
            command("start", "greptimedb")
            b.gwait(base, auth=auth)
            if before != identity():
                raise RuntimeError("maintenance restart changed owned data identity")
            return {"same_container_and_volumes": True}

        def configure(values):
            path = os.path.join(tmp, "greptimedb.toml")
            with open(path) as source:
                text = source.read()
            previous = {}
            for key, value in values.items():
                if key not in ("max_concurrent_scan_files", "sst_mirror_output_reserve", "min_compaction_interval", "index.build_mode"):
                    raise ValueError("unsupported owned maintenance override")
                pattern = r"(?m)^" + re.escape(key) + r" = (.*)$"
                match = re.search(pattern, text)
                if key == "sst_mirror_output_reserve":
                    if not match or not re.fullmatch(r"[1-9][0-9]*(B|MiB|GiB)", str(value)):
                        raise ValueError("invalid native output reserve")
                    previous[key] = json.loads(match[1])
                    replacement = "%s = %s" % (key, json.dumps(value))
                elif key == "min_compaction_interval":
                    if not match or value not in ("0m", "36500d"):
                        raise ValueError("invalid owned automatic compaction interval")
                    previous[key] = json.loads(match[1])
                    replacement = "%s = %s" % (key, json.dumps(value))
                elif key == "index.build_mode":
                    if value not in ("sync", "async"):
                        raise ValueError("invalid owned index build mode")
                    previous[key] = json.loads(match[1]) if match else "sync"
                    replacement = "%s = %s" % (key, json.dumps(value))
                else:
                    previous[key] = int(match[1]) if match else 384
                    replacement = "%s = %d" % (key, int(value))
                text = re.sub(pattern, replacement, text) if match else text.replace(
                    "[region_engine.mito]", "[region_engine.mito]\n" + replacement)
            b.cache_config(text)
            # Each mounted configuration is immutable, including its host path.
            # Verify the actual container bytes before starting the owned DB.
            before = identity()
            command("stop", "--timeout", "60", "greptimedb")
            command("rm", "-f", "greptimedb")
            with open(path + ".next", "w") as output:
                output.write(text)
            os.chmod(path + ".next", 0o600)
            os.replace(path + ".next", path)
            with tempfile.NamedTemporaryFile(mode="w", dir=tmp, prefix="greptimedb-config-",
                                             suffix=".toml", delete=False) as output:
                output.write(text)
                output.flush()
                os.fsync(output.fileno())
                mounted_name = os.path.basename(output.name)
            compose_path = os.path.join(tmp, "docker-compose.yaml")
            with open(compose_path) as source:
                compose = source.read()
            mount_pattern = r"\./[^\"/]+\.toml:/etc/greptime/greptimedb\.toml:ro"
            if len(re.findall(mount_pattern, compose)) != 1:
                raise RuntimeError("owned config mount anchor changed")
            compose = re.sub(mount_pattern, "./" + mounted_name +
                             ":/etc/greptime/greptimedb.toml:ro", compose)
            with open(compose_path, "w") as output:
                output.write(compose)
            digest = command("run", "--rm", "--no-deps", "--entrypoint", "sha256sum",
                             "greptimedb", "/etc/greptime/greptimedb.toml").split()[0]
            if digest != hashlib.sha256(text.encode()).hexdigest():
                raise RuntimeError("owned mounted configuration bytes differ from generated TOML")
            command("up", "-d", "--no-deps", "greptimedb")
            b.gwait(base, auth=auth)
            if identity()[1] != before[1]:
                raise RuntimeError("owned configuration replacement changed primary volumes")
            return previous

        def files(table):
            if not re.fullmatch(r"[a-z][a-z0-9_]*", table):
                raise ValueError("invalid fixture table")
            return records("SELECT * FROM information_schema.ssts_manifest WHERE visible=true "
                           "AND table_id IN (SELECT table_id FROM information_schema.tables "
                           "WHERE table_schema='public' AND table_name='%s')" % table)

        def compact_regular(table, start=None, end=None):
            old = files(table)
            before = b.compaction_metrics(base, auth)
            old_state = maintenance_manifest_state(old)
            direct_inputs = maintenance_range_inputs(old, start, end)
            observation = {"table": table, "start": start, "end": end,
                           "before_metrics": before, "before_files": old}
            result.setdefault("maintenance_observations", []).append(observation)
            options = []
            for key, value in (("start_time", start), ("end_time", end)):
                if value is not None:
                    if not re.fullmatch(r"[0-9T :.+Z-]+", str(value)):
                        raise ValueError("invalid bounded compaction timestamp")
                    normalized = str(value).replace(" ", "T")
                    options.append("%s=%s" % (key, normalized if normalized.endswith("Z") else normalized + "Z"))
            args = ", 'regular'" + (", '%s'" % ",".join(options) if options else "")
            sql("ADMIN compact_table('%s'%s)" % (table, args))
            deadline = time.monotonic() + 300
            stable, last = 0, None
            while time.monotonic() < deadline:
                current = files(table)
                state = maintenance_manifest_state(current)
                stable = stable + 1 if state == last else 0
                last = state
                after = b.compaction_metrics(base, auth)
                observation.update(after_metrics=after, after_files=current, stable_polls=stable)
                if after["failures"] > before["failures"]:
                    raise RuntimeError("native regular compaction failed")
                changed = state != old_state
                if stable >= 3 and after["inflight"] == 0 and (
                        after["completed"] > before["completed"] or changed or not old):
                    return {"before_metrics": before, "after_metrics": after,
                            "before_files": len(old), "after_files": len(current),
                            "manifest_changed": changed,
                            "completed_proven": after["completed"] > before["completed"],
                            "empty_input": not old}
                # Pinned regular ADMIN awaits worker resolution (including NoPlan);
                # its affected_rows=0 does not distinguish a merge from no plan.
                # Require unchanged manifests AND no completed-counter advance.
                if (stable >= 5 and not changed and direct_inputs is not None
                        and direct_inputs <= 1 and after["inflight"] == 0
                        and after["completed"] == before["completed"]):
                    return {"before_metrics": before, "after_metrics": after,
                            "before_files": len(old), "after_files": len(current),
                            "completed_proven": False, "unchanged_manifest_observed": True,
                            "direct_range_inputs": direct_inputs,
                            "no_op_reason": "regular ADMIN resolved; stable unchanged manifest and no completed-counter advance",
                            "regular_admin_waiters_resolved": True,
                            "dependency_expansion_not_assumed": True}
                time.sleep(1)
            raise RuntimeError("native regular completion/retirement not observed")

        def reset():
            b.reset_owned_cache(tmp, project, env, evidence)
            b.gwait(base, auth=auth)
            return {"owned_offline_cache_removed": True}

        def mirror_fault(ids, restore):
            # Offline move preserves exact original bytes/inodes; no S3 repair.
            b.owned_cache_scope(tmp, project, env)
            command("stop", "--timeout", "60", "greptimedb")
            code = (
                "import os,json,pathlib\n"
                "root=pathlib.Path('/data/sst-mirror-v1/objects')\n"
                "ids=json.loads(%r)\n"
                "restore=%r\n"
                "changed=[]\n"
                "for p in root.rglob('*'):\n"
                " if not p.is_file() or p.is_symlink(): continue\n"
                " if not any(identity in p.name for identity in ids): continue\n"
                " if restore:\n"
                "  if not p.name.endswith('.proof-held'): continue\n"
                "  dest=p.with_name(p.name[:-11])\n"
                " else:\n"
                "  if not p.name.endswith('.parquet'): continue\n"
                "  dest=p.with_name(p.name+'.proof-held')\n"
                " os.rename(p,dest); changed.append(str(dest))\n"
                "if not changed: raise RuntimeError('no owned mirror input changed')\n"
                "print(json.dumps(changed))\n" % (json.dumps(ids), restore))
            try:
                changed = json.loads(command("run", "--rm", "--no-deps", "--entrypoint",
                                             "python3", "cache-reset", "-c", code))
            finally:
                command("start", "greptimedb")
                b.gwait(base, auth=auth)
            return changed

        ctx = dict(records=records, sql=sql, phase=phase, files=files,
                   compact=compact_regular, restart=restart, reset_cache=reset,
                   configure=configure, metrics=lambda: b.compaction_metrics(base, auth), rows=rows,
                   index_failure_logs=result.setdefault("index_failure_logs", []),
                   logs=lambda: b._redact(command("logs", "--no-color", "--tail", "250", "greptimedb")),
                   phase_cost=lambda name: next(p["cost"] for p in reversed(result["phases"]) if p["name"] == name))
        def source_snapshot(source_rows):
            from tools.greptime_local_cases import mirror_snapshot_code
            return json.loads(command("run", "--rm", "--no-deps", "--entrypoint",
                                      "python3", "cache-reset", "-c", mirror_snapshot_code(source_rows)))
        ctx["snapshot"] = source_snapshot
        if local_input_only:
            ctx.update(command=command, physical_inventory=physical_inventory)
            result["local_input_cases"] = {}
        oracle = {}
        progression = []
        new_bytes = 0
        for cycle in range(4):
            logical_cycle = cycle % 2 if local_input_only else cycle
            writes = maintenance_writes(logical_cycle, rows)
            new_inputs = [row for row in writes[:rows] if tuple(row[:3]) not in oracle]
            # Expected last-row state is established before any native write.
            for stamp, vehicle, path, value, entropy in writes:
                oracle[(stamp, vehicle, path)] = (value, entropy)
            new_bytes += sum(len(json.dumps(row).encode()) for row in new_inputs)

            def ingest_cycle():
                flushes = 1 if maintenance_policy == "large_batch" else 4
                for part in range(flushes):
                    chunk = writes[len(writes) * part // flushes:len(writes) * (part + 1) // flushes]
                    for offset in range(0, len(chunk), b.BATCH):
                        values = []
                        for stamp, vehicle, path, value, entropy in chunk[offset:offset + b.BATCH]:
                            timestamp = datetime.datetime.fromtimestamp(stamp / 1000, datetime.timezone.utc)
                            values.append("('%s','%s','%s','bench',%s,'%s')" % (
                                timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], vehicle,
                                path, "NULL" if value is None else repr(value), entropy))
                        sql("INSERT INTO %s(event_time,vehicle,path,source,value_num,value_text) VALUES %s" %
                            (b.TABLE, ",".join(values)))
                    sql("ADMIN flush_table('%s')" % b.TABLE)
                return {"writes": len(writes), "flushes": flushes, "new_logical_keys": len(new_inputs)}

            phase("window_%d_ingest" % cycle, "nonquery", ingest_cycle)
            phase("window_%d_preclose_physical_inventory" % cycle, "measurement_nonquery", physical_inventory)
            if maintenance_policy != "deferred":
                start = datetime.datetime.fromtimestamp((b.BASE_MS + logical_cycle * b.HOUR_MS) / 1000,
                                                        datetime.timezone.utc)
                end = start + datetime.timedelta(hours=1)
                phase("window_%d_close" % cycle, "maintenance",
                      lambda: compact_regular(b.TABLE, start.strftime("%Y-%m-%d %H:%M:%S"),
                                              end.strftime("%Y-%m-%d %H:%M:%S")))
            snapshot = phase("window_%d_manifest" % cycle, "nonquery", lambda: files(b.TABLE))
            physical = sum(int(row["file_size"]) for row in snapshot)
            logical = sum(len(json.dumps((key, value)).encode()) for key, value in oracle.items())
            progression.append({"cycle": cycle, "active_files": len(snapshot), "active_manifest_bytes": physical,
                                "retained_rows": len(oracle), "retained_logical_bytes": logical,
                                "active_manifest_to_logical": physical / logical})
            progression[-1]["flush_file_upper_bound"] = (cycle + 1) * (
                1 if maintenance_policy == "large_batch" else 4)
            if local_input_only:
                progression[-1]["flush_file_upper_bound"] = 16
                progression[-1]["bound_scope"] = "finite two-window replay fixture, at most 16 active SSTs"
            progression[-1]["within_flush_file_bound"] = (
                len(snapshot) <= progression[-1]["flush_file_upper_bound"])
            if local_input_only and not progression[-1]["within_flush_file_bound"]:
                raise RuntimeError("bounded retained-window fixture exceeded active file ceiling")
            rc, usage, _ = b.sh(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}",
                                identity()[0]], timeout=30)
            progression[-1]["owned_db_memory_usage"] = usage if rc == 0 else None
            phase("window_%d_idle" % cycle, "nonquery", lambda: time.sleep(5))
        phase("maintenance_same_volume_restart", "nonquery", restart)
        if local_input_only:
            result["local_input_cases"]["restart"] = {"proven": True}
        phase("recovery_idle", "nonquery", lambda: time.sleep(10))
        # Add unmerged local input before deleting cache: no verification query warms it.
        phase("cold_input_flush", "nonquery", ingest_cycle)
        phase("maintenance_cache_loss", "nonquery", reset)
        phase("cold_regular_compaction", "maintenance", lambda: compact_regular(b.TABLE))
        if local_input_only:
            result["local_input_cases"]["cache_loss"] = {"proven": True}
            result["local_input_cases"]["warm_windows"] = {
                "proven": all(p["result"].get("completed_proven", False)
                              for p in result["phases"]
                              if re.fullmatch(r"window_[0-9]+_close", p["name"]))}
            from tools.greptime_local_cases import run_missing_input
            result["local_input_cases"]["missing_input_repair"] = run_missing_input(
                dict(ctx, table=b.TABLE, seed=ingest_cycle, mirror_fault=mirror_fault,
                     logs=lambda: b._redact(command("logs", "--no-color", "--tail", "250", "greptimedb"))))
            from tools.greptime_local_cases import run_quota
            result["local_input_cases"]["quota_repair"] = run_quota(
                dict(ctx, table=b.TABLE, seed=ingest_cycle,
                     logs=lambda: b._redact(command("logs", "--no-color", "--tail", "250", "greptimedb"))))
            from tools.greptime_local_cases import run_eviction_recovery_race
            result["local_input_cases"]["eviction_recovery_race"] = run_eviction_recovery_race(
                dict(ctx, table=b.TABLE, seed=ingest_cycle))
            from tools.greptime_local_cases import run_filesystem_full, tmpfs_fault_code
            def full_variant():
                nonlocal maintenance_inventory
                from tools.greptime_local_cases import bounded_tmpfs_compose
                compose_path = os.path.join(tmp, "docker-compose.yaml")
                config_path = os.path.join(tmp, "greptimedb.toml")
                with open(compose_path) as source:
                    original_compose = source.read()
                with open(config_path) as source:
                    original_config = source.read()
                roots = re.findall(r'(?m)^root = "(.*)"$', original_config)
                if len(roots) != 1:
                    raise RuntimeError("isolated fault S3 root anchor changed")
                fault_config = re.sub(r'(?m)^root = ".*"$',
                                      'root = "p3-filesystem-full"', original_config)
                fault_compose = original_compose.replace("greptime-data:/data", "greptime-fault-data:/data")
                config_mounts = re.findall(r"\./[^\"/]+\.toml:/etc/greptime/greptimedb\.toml:ro", fault_compose)
                if len(config_mounts) != 1:
                    raise RuntimeError("isolated fault config mount anchor changed")
                fault_compose = fault_compose.replace(
                    config_mounts[0], "./greptimedb-fault.toml:/etc/greptime/greptimedb.toml:ro")
                fault_compose += "\n  greptime-fault-data:\n"
                fault_compose = bounded_tmpfs_compose(fault_compose)
                saved_inventory = maintenance_inventory
                maintenance_inventory = None
                keeper = None
                command("stop", "--timeout", "60", "greptimedb")
                command("rm", "-f", "greptimedb")
                try:
                    with open(compose_path, "w") as output:
                        output.write(fault_compose)
                    with open(os.path.join(tmp, "greptimedb-fault.toml"), "w") as output:
                        output.write(fault_config)
                    keeper = command("run", "-d", "--no-deps", "--entrypoint", "python3",
                                     "cache-reset", "-c", "import time; time.sleep(86400)")
                    command("up", "-d", "greptimedb")
                    b.gwait(base, auth=auth)
                    sql(b.DDL)
                    sql("ALTER TABLE %s ADD COLUMN value_text STRING" % b.TABLE)
                    def tmpfs_fault(repair):
                        return json.loads(command("run", "--rm", "--no-deps", "--entrypoint",
                                                  "python3", "cache-reset", "-c", tmpfs_fault_code(repair)))
                    proof = run_filesystem_full(
                        dict(ctx, table=b.TABLE, seed=ingest_cycle, tmpfs_fault=tmpfs_fault,
                             logs=lambda: b._redact(command("logs", "--no-color", "--tail", "250", "greptimedb"))))
                    proof["isolation"] = "separate bounded tmpfs /data and separate S3 root; normal lifecycle uses persistent volume"
                    return proof
                except Exception:
                    evidence["filesystem_fault_failure_logs"] = b._redact(
                        command("logs", "--no-color", "--tail", "250", "greptimedb"))
                    raise
                finally:
                    command("stop", "--timeout", "60", "greptimedb")
                    command("rm", "-f", "greptimedb")
                    if keeper:
                        rc, _, err = b.sh(["docker", "rm", "-f", keeper])
                        if rc:
                            raise RuntimeError("owned tmpfs keeper cleanup failed: " + err)
                    with open(compose_path, "w") as output:
                        # Keep the fault-volume declaration so project cleanup owns it.
                        output.write(original_compose + "\n  greptime-fault-data:\n"
                                     "    driver_opts:\n      type: tmpfs\n      device: tmpfs\n"
                                     '      o: "size=1073741824"\n')
                    command("up", "-d", "greptimedb")
                    b.gwait(base, auth=auth)
                    maintenance_inventory = saved_inventory
            result["local_input_cases"]["filesystem_full_repair"] = full_variant()
        phase("maintenance_exact_oracle", "verification_query", lambda: maintenance_verify(records, oracle))
        result["boundaries"] = run_boundaries(ctx)
        if local_input_only:
            result["local_input_cases"].update(
                whole_ttl={"proven": True}, mixed_ttl={"proven": True})
            from tools.greptime_local_cases import run_formats_and_indexes
            result["local_input_cases"].update(run_formats_and_indexes(ctx))
            phase("local_user_cache_loss", "nonquery", reset)
            phase("user_cold_query", "user_query", lambda: maintenance_verify(records, oracle))
            cold_cost = ctx["phase_cost"]("user_cold_query")
            if cold_cost["payload_read_bytes"] <= 0:
                raise RuntimeError("ordinary cold query did not exercise canonical S3 payload reads")
            result["local_input_cases"]["cold_user_query"] = {"proven": True, "cost": cold_cost}

            def unclean_restart():
                sql("CREATE TABLE gbench_local_wal(ts TIMESTAMP(9) TIME INDEX, "
                    "k STRING, val DOUBLE, PRIMARY KEY(k))")
                for values in ("('2026-01-01T00:00:00Z','ack-null',1)",
                               "('2026-01-01T00:00:00Z','ack-null',NULL),('2026-01-01T00:00:00Z','ack-value',7)"):
                    sql("INSERT INTO gbench_local_wal VALUES " + values)
                if files("gbench_local_wal"):
                    raise RuntimeError("WAL replay proof requires acknowledged unflushed rows")
                before = identity()
                command("kill", "--signal", "SIGKILL", "greptimedb")
                command("start", "greptimedb")
                b.gwait(base, auth=auth)
                if identity() != before:
                    raise RuntimeError("unclean restart changed owned volume/container identity")
                return {"unflushed_acknowledged_rows": True, "same_container_and_volumes": True}
            crash = phase("local_unclean_wal_restart", "nonquery", unclean_restart)
            def wal_oracle():
                got = records("SELECT k,val FROM gbench_local_wal ORDER BY k")
                if got != [{"k": "ack-null", "val": None}, {"k": "ack-value", "val": 7.0}]:
                    raise RuntimeError("unclean restart lost acknowledged last-row/NULL data: " + repr(got))
                return got
            crash["rows"] = phase("local_wal_replay_oracle", "verification_query", wal_oracle)
            phase("local_replayed_wal_flush", "nonquery", lambda: sql("ADMIN flush_table('gbench_local_wal')"))
            result["local_input_cases"]["unclean_wal_replay"] = dict(crash, proven=True)
            from tools.greptime_local_cases import run_automatic_compaction
            result["local_input_cases"]["automatic_compaction"] = run_automatic_compaction(ctx)

            def observe_gc():
                known = {str(row["file_id"])
                         for observation in result.get("maintenance_observations", [])
                         for row in observation["before_files"]}
                deadline = time.monotonic() + 180
                polls = []
                while True:
                    active = {str(row["file_id"]) for row in records(
                        "SELECT file_id FROM information_schema.ssts_manifest WHERE visible=true")}
                    retired = known - active
                    if not retired:
                        raise RuntimeError("no retired native SSTs available for GC proof")
                    objects = b.s3_list_all("http://127.0.0.1:%d" % meter_proxy_port, access, secret)
                    from tools.greptime_local_cases import gc_retired_ids
                    remaining = gc_retired_ids(objects, retired)
                    polls.append({"at": time.time(), "retired": sorted(retired),
                                  "remaining": remaining, "objects": len(objects)})
                    if not remaining:
                        return {"proven": True, "polls": polls, "teardown_used": False}
                    if time.monotonic() >= deadline:
                        raise RuntimeError("native GC did not retire physical SST objects: " +
                                           json.dumps(polls))
                    time.sleep(2)

            result["local_input_cases"]["engine_gc"] = phase(
                "local_engine_gc", "measurement_nonquery", observe_gc)

            def physical_bound():
                from tools.greptime_local_cases import payload_bound
                with open(os.path.join(tmp, "greptimedb.toml")) as source:
                    roots = re.findall(r'(?m)^root = "(.*)"$', source.read())
                if len(roots) != 1:
                    raise RuntimeError("physical-bound S3 root anchor changed")
                code = (
                    "import pathlib,json\n"
                    "root=pathlib.Path('/data/sst-mirror-v1/objects')\n"
                    "rows=[]\n"
                    "for p in root.rglob('*'):\n"
                    " if p.is_symlink(): raise RuntimeError('mirror contains symlink')\n"
                    " if p.is_file(): rows.append((str(p.relative_to(root)),p.stat().st_size))\n"
                    "print(json.dumps(rows))\n")
                deadline = time.monotonic() + 180
                polls = result.setdefault("physical_bound_polls", [])
                while True:
                    active = records("SELECT * FROM information_schema.ssts_manifest WHERE visible=true")
                    remote = b.s3_list_all("http://127.0.0.1:%d" % meter_proxy_port, access, secret)
                    b.owned_cache_scope(tmp, project, env)
                    local = json.loads(command("run", "--rm", "--no-deps", "--entrypoint",
                                               "python3", "cache-reset", "-c", code))
                    bound = payload_bound(active, remote, local, roots[0])
                    polls.append(dict(at=time.time(), **bound))
                    if bound["proven"]:
                        return bound
                    if time.monotonic() >= deadline:
                        raise RuntimeError("native physical payload/index GC bound not reached: " +
                                           json.dumps(bound))
                    time.sleep(2)


            from pathlib import Path
            import shutil
            import subprocess
            from unittest.mock import patch
            from scripts.storage import backup

            recovery = Path(tmp, "native-remote-recovery")
            source_data = recovery / "source" / "greptime-data"
            source_etc = recovery / "source" / "greptime-etc"
            archives = recovery / "archives"
            downloaded = recovery / "downloaded"
            restored = recovery / "restored"
            for directory in (source_data, source_etc, archives, downloaded):
                directory.mkdir(parents=True, mode=0o700)
            for label in backup.LABELS:
                (restored / label).mkdir(parents=True, mode=0o700)
            endpoint = "http://127.0.0.1:%d" % meter_proxy_port
            settings = {
                "BACKUP_DIR": str(archives),
                "BACKUP_SOURCE_DIRS": str(source_data) + ":" + str(source_etc),
                "BACKUP_OFFLINE_CONFIRMED": "1", "GREPTIME_HTTP_URL": base,
                "GREPTIME_STORAGE_TYPE": "S3", "S3_ENDPOINT_URL": endpoint,
                "S3_BUCKET": b.BUCKET, "S3_ROOT": "greptime",
                "S3_BACKUP_PREFIX": "native-recovery", "S3_REGION": b.REGION,
                "S3_ACCESS_KEY_ID": access, "S3_SECRET_ACCESS_KEY": secret,
                "BACKUP_REMOTE_RETENTION_MODE": "off", "BACKUP_KEEP": "3",
                "BACKUP_RESERVE_BYTES": "1048576", "BACKUP_FILE": "",
            }
            endpoint_check = backup.require_https_endpoint

            def owned_endpoint(value, name):
                # Test-only transport exception; checksum and restore validation stay intact.
                return value if value == endpoint else endpoint_check(value, name)

            def copy_owned(source, destination):
                copied = subprocess.run(["docker", "cp", str(source), str(destination)],
                                        env=b.clean_env(), capture_output=True, text=True, timeout=120)
                if copied.returncode:
                    raise RuntimeError("owned offline recovery copy failed")

            def offsite_backup():
                command("stop", "--timeout", "60", "greptimedb")
                copy_sizes.update({b.BUCKET + "/" + key: size for key, size in
                                   b.s3_list_all("http://127.0.0.1:%d" % meter_proxy_port,
                                                 access, secret)})
                container = command("ps", "--all", "--quiet", "greptimedb").strip()
                if not container:
                    raise RuntimeError("owned offline backup container missing")
                copy_owned(container + ":/data/.", source_data)
                shutil.copy2(Path(tmp, "greptimedb.toml"), source_etc / "greptimedb.toml")
                shutil.copytree(Path(tmp, "preflight", "auth"), source_etc / "auth")
                with patch.dict(os.environ, settings), patch.object(
                        backup, "require_https_endpoint", owned_endpoint):
                    if backup.main(["backup", "backup"]) != 0:
                        raise RuntimeError("native offsite backup failed")
                archive, = archives.glob("greptime-backup-*.tar.gz")
                return {"archive": archive.name, "archive_bytes": archive.stat().st_size,
                        "includes_wal_catalog_and_mirror": True}

            def remote_only_restore():
                # Empty download/restore directories: no original local archive or volume reuse.
                before_restore = backup_meter.snapshot()
                try:
                    with patch.dict(os.environ, dict(settings, BACKUP_DIR=str(downloaded),
                            BACKUP_RESTORE_BASE=str(restored), BACKUP_FILE=archived["archive"])), patch.object(
                            backup, "require_https_endpoint", owned_endpoint):
                        if backup.main(["backup", "restore"]) != 0:
                            raise RuntimeError("native remote-only archive restore failed")
                finally:
                    restore_costs.append(meter_delta(before_restore, backup_meter.snapshot()))
                command("rm", "-f", "greptimedb")
                compose_path = os.path.join(tmp, "docker-compose.yaml")
                with open(compose_path) as source:
                    compose = source.read()
                original_compose = compose
                anchor = '"greptime-data:/data", "./greptimedb'
                if compose.count(anchor) != 1:
                    raise RuntimeError("owned recovery volume anchor changed")
                compose = compose.replace(anchor, '"greptime-restore-data:/data", "./greptimedb')
                compose += "\n  greptime-restore-data:\n"
                with open(compose_path, "w") as output:
                    output.write(compose)
                try:
                    command("up", "--no-start", "--no-deps", "greptimedb")
                    container = command("ps", "--all", "--quiet", "greptimedb").strip()
                    if not container:
                        raise RuntimeError("owned fresh recovery container missing")
                    copy_owned(str(restored / "greptime-data") + "/.", container + ":/data/")
                    command("start", "greptimedb")
                    b.gwait(base, auth=auth)
                    checked = maintenance_verify(records, oracle)
                    return {"proven": True, "fresh_volume": True, "local_copy_used": False,
                            "remote_archive": archived, "canonical_s3_oracle": checked,
                            "catalog_wal_and_mirror_recovered_from_s3": True}
                finally:
                    command("stop", "--timeout", "60", "greptimedb")
                    command("rm", "-f", "greptimedb")
                    with open(compose_path, "w") as output:
                        output.write(original_compose + "\n  greptime-restore-data:\n")
                    command("up", "-d", "greptimedb")
                    b.gwait(base, auth=auth)

            from urllib.parse import unquote
            from tools.s3_meter import S3Meter
            copy_sizes = {}
            backup_meter = S3Meter(target, copy_size_lookup=lambda source:
                                  copy_sizes.get(unquote(source).lstrip("/"))).start()
            endpoint = backup_meter.endpoint
            settings["S3_ENDPOINT_URL"] = endpoint
            backup_start = backup_meter.snapshot()
            try:
                archived = phase("local_offsite_backup", "backup_nonquery", offsite_backup)
                result["native_backup_cost"] = meter_delta(backup_start, backup_meter.snapshot())
                result["local_input_cases"]["remote_only_restore"] = phase(
                    "local_remote_only_restore", "remote_restore", remote_only_restore)
            finally:
                backup_end = backup_meter.snapshot()
                external_meters.append(dict(source="native_offsite_backup_restore",
                                            meter_before=backup_start, meter_after=backup_end,
                                            cost=meter_delta(backup_start, backup_end)))
                backup_meter.stop()
                b.validate_meter_errors(backup_end)
            from tools.greptime_summary_cases import prove_summary_cases
            result["summary_cases"] = {}
            prove_summary_cases(
                dict(base=base, auth=auth, snap_port=snap_port, query_costs=query_costs,
                     comparison_costs=comparison_costs,
                     tmp=tmp, project=project, summary_report=result["summary_cases"],
                     mirror_fault=mirror_fault),
                restart=reset, grpc_url="grpc://127.0.0.1:" + env["GREPTIME_GRPC_PORT"])
            result["local_input_cases"]["all_summary_families"] = {"proven": True}
            result["local_input_cases"]["physical_bounds"] = phase(
                "local_physical_bound", "measurement_nonquery", physical_bound)
        phase("post_boundary_gc_observation", "measurement_nonquery", physical_inventory)
        result["physical_storage_bound_proven"] = (
            result.get("local_input_cases", {}).get("physical_bounds", {}).get("proven", False))
        result["physical_storage_bound_scope"] = (
            "Local-input mode requires exact physical S3 and persistent-mirror payload/index "
            "sets equal active manifest paths after native GC; finite retained-data replay only. "
            "Other modes report observations, not a physical bound. Teardown is never GC proof.")
        result["file_progression"] = progression
        result["new_input_bytes"] = new_bytes
        result["normalization_scope"] = {
            "input_bytes": "Python JSON serialization of newly appended core fixture rows; replays/corrections excluded",
            "cost_numerator": "entire measured lifecycle, including separate boundary fixtures and shutdown",
            "not_monthly_or_1tb_projection": True}
        result["cache_budgets"] = maintenance_cache_budgets(size_bytes(cache))
        result["cache_budgets"]["twice_peak_manifest_bytes"] = 2 * max(p["active_manifest_bytes"] for p in progression)
        result["cache_budgets"]["parquet_input_output_headroom"] = (
            result["cache_budgets"]["parquet_bytes"] >= result["cache_budgets"]["twice_peak_manifest_bytes"])
        result["amplification"] = {
            "observed_cycles": len(progression),
            "max_active_manifest_to_logical": max(p["active_manifest_to_logical"] for p in progression),
            "last_to_first_active_manifest_ratio": progression[-1]["active_manifest_to_logical"] / progression[0]["active_manifest_to_logical"],
            "finite_fixture_only": True}
        result["window_close_observations"] = [
            dict(name=p["name"], result=p["result"]) for p in result["phases"]
            if re.fullmatch(r"window_[0-9]+_close", p["name"])]
        result["all_window_closes_completed"] = (
            all(p["result"].get("completed_proven", False) for p in result["window_close_observations"])
            if result["window_close_observations"] else None)
        result["finite_file_bound_pass"] = all(p["within_flush_file_bound"] for p in progression)
        result["status"] = "success"
        if local_input_only:
            from tools.greptime_local_cases import require_complete
            result["local_input_acceptance"] = require_complete(result["local_input_cases"])

    try:
        python_image = b.python_image_for(REPO)
        if storage_type == "S3":
            toml, _ = b.run_preflight(REPO, tmp, creds, python_image, project, evidence)
        else:
            from scripts.database import greptime_preflight
            from unittest.mock import patch
            settings = dict(b.clean_env(), OUT_DIR=os.path.join(tmp, "preflight"),
                            GREPTIME_STORAGE_TYPE="File", GREPTIME_DB="public",
                            GREPTIME_USER=creds["db_user"], GREPTIME_PASSWORD=creds["db_password"],
                            GF_ADMIN_PASSWORD=creds["admin_password"], OTLP_USER=creds["otlp_user"],
                            OTLP_PASSWORD=creds["otlp_password"])
            with patch.dict(os.environ, settings, clear=True):
                greptime_preflight.main()
            with open(os.path.join(tmp, "preflight", "greptimedb.toml")) as source:
                toml = source.read()
            evidence["preflight"] = {"script": "scripts/database/greptime_preflight.py",
                                     "storage_type": "File", "executed": True}
        overrides = []
        for key, value in (("write_cache_size", cache), ("page_cache_size", "1MiB")):
            pattern = r'(?m)^' + key + r' = .*$'
            old = re.findall(pattern, toml)
            if len(old) != 1:
                raise RuntimeError("unknown generated cache setting")
            replacement = '%s = "%s"' % (key, value)
            toml = re.sub(pattern, replacement, toml)
            overrides.append({"key": key, "before": old[0], "after": replacement, "scope": "owned test only"})
        if background_policy is not None or maintenance_policy is not None:
            legacy = background_policy == "legacy"
            policy_overrides = [
                    ("preload_index_cache", str(legacy).lower()),
                    ("enable_refill_cache_on_read", str(legacy).lower()),
                    ("schedule_compaction_after_edit", str(legacy).lower())]
            # Historical manual-maintenance comparisons must not inherit new defaults.
            deferred = background_policy == "deferred" or maintenance_policy is not None
            policy_overrides.append(("min_compaction_interval", '"36500d"' if deferred else '"0m"'))
            for key, value in policy_overrides:
                pattern = r"(?m)^" + key + r" = .*$"
                prior = re.findall(pattern, toml)
                if len(prior) != 1:
                    raise RuntimeError("unknown generated background setting: " + key)
                replacement = key + " = " + value
                toml = re.sub(pattern, replacement, toml)
                overrides.append({"key": key, "before": prior[0], "after": replacement,
                                  "scope": "owned background policy comparison"})
        evidence["cache_overrides"] = overrides
        if local_input_only:
            from tools.greptime_local_cases import mirror_toml
            toml = mirror_toml(toml)
            evidence["local_input_contract"] = {
                "mirror_path": "/data/sst-mirror-v1", "capacity": "256MiB",
                "output_reserve": "32MiB", "canonical_storage": "S3",
                "startup_capability": "--require-local-sst-mirror-v1"}
        with open(os.path.join(tmp, "greptimedb.toml"), "w", opener=lambda p, f: os.open(p, f, 0o600)) as f:
            f.write(toml)
        b.fetch_minio_binary(tmp)
        b.write_project(tmp, REPO, greptime_image or b.GREPTIME_IMAGE_DEFAULT, python_image, access, secret)
        if local_input_only:
            from tools.greptime_local_cases import strict_compose
            compose_path = os.path.join(tmp, "docker-compose.yaml")
            with open(compose_path) as source:
                owned_compose = strict_compose(source.read())
            anchor = 'ports: ["127.0.0.1:${GREPTIME_PORT}:4000"]'
            if owned_compose.count(anchor) != 1:
                raise RuntimeError("owned native Flight port anchor changed")
            owned_compose = owned_compose.replace(
                anchor, 'ports: ["127.0.0.1:${GREPTIME_PORT}:4000", '
                '"127.0.0.1:${GREPTIME_GRPC_PORT}:4001"]')
            with open(compose_path, "w") as output:
                output.write(owned_compose)
        if maintenance_policy is not None:
            compose_path = os.path.join(tmp, "docker-compose.yaml")
            with open(compose_path) as source:
                owned_compose = source.read()
            anchor = 'ports: ["127.0.0.1:${SNAP_PORT}:8080"]'
            if owned_compose.count(anchor) != 1:
                raise RuntimeError("owned meter proxy publication anchor changed")
            owned_compose = owned_compose.replace(
                anchor, 'ports: ["127.0.0.1:${SNAP_PORT}:8080", "127.0.0.1:%d:9000"]' % meter_proxy_port)
            with open(compose_path, "w") as output:
                output.write(owned_compose)
        command("up", "-d", "--wait", "minio", "s3meter")
        b.wait_minio(target, access, secret)
        status, _ = b.s3_request(target, "PUT", "/" + b.BUCKET, access, secret)
        if status != 200:
            raise RuntimeError("fresh bucket creation failed")
        lifecycle_start = b.snap_get(snap_port)
        phase("startup", "nonquery", lambda: (command("up", "-d", "greptimedb"), b.gwait(base, auth=auth)))
        sql(b.DDL)
        sql("ALTER TABLE %s ADD COLUMN value_text STRING" % b.TABLE)
        for table in ("bench_minute", "bench_hour"):
            sql('CREATE TABLE %s(bucket_start TIMESTAMP(3) NOT NULL TIME INDEX,vehicle STRING,"path" STRING,cnt BIGINT,sum_v DOUBLE,min_v DOUBLE,max_v DOUBLE,PRIMARY KEY(vehicle,"path"))' % table)
        if scenario is not None:
            scenario(dict(sql=sql, records=records, phase=phase, command=command,
                          tmp=tmp, project=project, env=env, evidence=evidence,
                          base=base, auth=auth, target=target, access=access,
                          secret=secret, result=result, rows=rows, cache=cache))
            result["status"] = "success"
            return
        if maintenance_policy is not None:
            maintenance_scenario()
            return
        if background_policy is not None:
            background_scenario()
            return
        phase("ingest_flush", "nonquery", lambda: ingest(enforce_bracket=True))
        phase("warm_compaction", "nonquery", compact)
        phase("raw_minute_hour", "analytics_nonquery", aggregate)
        phase("accuracy", "verification_query", verify)
        result["repository_paths"] = prove_repository_paths(
            dict(tmp=tmp, project=project, env=env, base=base, auth=auth,
                 minio_target=target, access=access, secret=secret,
                 external_meters=external_meters, query_costs=query_costs,
                 restore_costs=restore_costs,
                 snap_port=snap_port, creds=creds, evidence=evidence, phase=phase,
                 storage_type=storage_type, rows=rows))
        def identity():
            container = command("ps", "--all", "--quiet", "greptimedb")
            rc, text, _ = b.sh(["docker", "inspect", container])
            if rc:
                raise RuntimeError("owned DB identity inspection failed")
            obj, = json.loads(text)
            if obj["Config"]["Labels"]["com.docker.compose.project"] != project:
                raise RuntimeError("owned DB project mismatch")
            return {"container": obj["Id"],
                    "data_volumes": [m["Name"] for m in obj["Mounts"] if m["Type"] == "volume"]}
        before = identity()
        def restart():
            command("stop", "--timeout", "60", "greptimedb")
            command("start", "greptimedb")
            b.gwait(base, auth=auth)
            after = identity()
            if before != after:
                raise RuntimeError("same-volume restart identity changed")
            return {"same_containers_and_volumes": True, "cache_preserved": True}
        phase("same_volume_restart", "nonquery", restart)
        phase("analytics_rerun", "analytics_nonquery", aggregate)
        phase("background_idle", "nonquery", lambda: time.sleep(30))
        if storage_type == "S3":
            for cycle in range(1, 4):
                phase("resident_reingest_%d" % cycle, "nonquery", ingest)
                phase("resident_compaction_%d" % cycle, "nonquery", compact)
                phase("resident_analytics_%d" % cycle, "analytics_nonquery", aggregate)
            phase("resident_accuracy", "verification_query", verify)
        if storage_type == "S3":
            phase("offline_cache_loss", "nonquery_boundary", lambda: (b.reset_owned_cache(tmp, project, env, evidence), b.gwait(base, auth=auth)))
        else:
            phase("cold_process_restart", "nonquery", restart)
        phase("user_cold_query", "user_query", verify)
        phase("user_warm_query", "user_query", verify)
        # Cache loss is a real S3-primary boundary; File mode retains primary data.
        phase("boundary_reingest", "nonquery_boundary", ingest)
        if storage_type == "S3":
            phase("boundary_cache_loss", "nonquery_boundary", lambda: (b.reset_owned_cache(tmp, project, env, evidence), b.gwait(base, auth=auth)))
        else:
            phase("boundary_process_restart", "nonquery", restart)
        phase("cache_loss_compaction" if storage_type == "S3" else "post_restart_compaction",
              "nonquery_boundary" if storage_type == "S3" else "nonquery", compact)
        phase("boundary_accuracy", "verification_query", verify)
        final = b.snap_get(snap_port)
        result["meter_final"] = final
        result["error_gate"] = b.validate_meter_errors(final)
        result["nonquery_zero_sst_downloads"] = all(
            p["sst_payload_delta"]["sst_read_bytes"] == 0
            for p in result["phases"]
            if p["kind"] in ("nonquery", "analytics_nonquery")) and all(
                p["sst_read_bytes"] == 0
                for name, p in result["repository_paths"].items()
                if "user_query" not in name and p.get("kind") != "restore_exception")
        result["status"] = "success"
    except (Exception, SystemExit) as exc:
        result.update(status="failure", error=b._redact(str(exc)))
        if os.path.isfile(os.path.join(tmp, "docker-compose.yaml")):
            b._startup_diag(tmp, project, env, evidence, "egress proof failure")
    finally:
        if lifecycle_start is not None:
            try:
                # Stop DB while its proxy is still alive: shutdown I/O counts.
                command("stop", "--timeout", "60", "greptimedb")
                final = b.snap_get(snap_port)
                db_cost = meter_delta(lifecycle_start, final)
                result["meter_final"] = final
                result["meters"] = [dict(source="database", meter_before=lifecycle_start,
                                         meter_after=final, cost=db_cost)] + external_meters
                costs = [item["cost"] for item in result["meters"]]
                result["lifecycle_cost"] = combine_costs(costs)
                result["nonquery_cost"] = combine_costs(costs, query_costs + restore_costs + comparison_costs)
                result["excluded_query_costs"] = query_costs
                result["restore_costs"] = restore_costs
                result["baseline_analytics_comparison_costs"] = comparison_costs
                result["nonquery_zero_payload_downloads"] = (
                    proven_zero_payload(result["nonquery_cost"]) and all(
                        item["zero_payload_proven"]
                        for name, item in result.get("repository_paths", {}).items()
                        if name.startswith("repository_backup_")))
                result["accounting_scope"] = (
                    "Database before startup through shutdown plus independent backup SDK meter; "
                    "all asynchronous gaps included. Nonquery excludes measured user/verification queries, "
                    "explicitly labelled ordinary-summary comparison oracles, and remote restore; "
                    "strict analytics and all manual compaction remain included. "
                    "Phase costs are subsets, never added to lifecycle totals.")
                result["error_gate"] = b.validate_meter_errors(final)
                if maintenance_policy is not None and result.get("new_input_bytes"):
                    idle = [p for p in result["phases"] if "idle" in p["name"]]
                    idle_seconds = sum(p["seconds"] for p in idle)
                    idle_bytes = sum(p["cost"]["response_bytes"] for p in idle)
                    result["normalization"] = {
                        "response_bytes_per_new_input_byte": result["nonquery_cost"]["response_bytes"] / result["new_input_bytes"],
                        "payload_bytes_per_new_input_byte": result["nonquery_cost"]["payload_read_bytes"] / result["new_input_bytes"],
                        "idle_observed_seconds": idle_seconds,
                        "idle_response_bytes_per_hour": idle_bytes * 3600 / idle_seconds if idle_seconds else None,
                        "restart_costs": [dict(name=p["name"], cost=p["cost"]) for p in result["phases"]
                                          if "restart" in p["name"] or "cache_loss" in p["name"]],
                        "verification_query_seconds": sum(p["seconds"] for p in result["phases"]
                                                          if p["kind"] == "verification_query")}
            except Exception as exc:
                result.update(status="failure", accounting_error=b._redact(str(exc)))
        if os.path.isfile(os.path.join(tmp, "docker-compose.yaml")):
            try:
                volumes, containers = b.cleanup_project(tmp, project, env, evidence)
                if volumes or containers or not evidence["cleanup"]["success"]:
                    raise RuntimeError("owned resource cleanup incomplete")
            except Exception as exc:
                result.update(status="failure", cleanup_error=b._redact(str(exc)))
        result["recovery_directory"] = tmp if os.path.exists(tmp) else None


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=200000)
    ap.add_argument("--json-out", required=True)
    ap.add_argument("--small-cache", default="64KiB")
    ap.add_argument("--large-cache", default="1GiB")
    ap.add_argument("--s3-only", action="store_true",
                    help="prove S3-primary cache behavior without the File-primary comparison")
    ap.add_argument("--background-only", action="store_true",
                    help="compare old automatic work, on-demand analytics, and deferred compaction with the same small cache")
    ap.add_argument("--maintenance-only", action="store_true",
                    help="compare native bounded hot-window maintenance policies on local S3")
    ap.add_argument("--greptime-image", help="explicit experimental image; pinned default otherwise")
    ap.add_argument("--local-input-only", action="store_true",
                    help="experimental native local-mirror proof; requires an explicit fork image; incomplete cases fail closed")
    args = ap.parse_args(argv)
    if args.maintenance_only and args.background_only:
        ap.error("--maintenance-only and --background-only are mutually exclusive")
    if args.local_input_only and (not args.greptime_image or args.background_only):
        ap.error("--local-input-only requires --greptime-image and excludes --background-only")
    if args.local_input_only:
        args.maintenance_only = True
    small, large = size_bytes(args.small_cache), size_bytes(args.large_cache)
    if args.rows < 1000 or small >= large:
        ap.error("rows must be >=1000 and small cache < large cache")
    doc = {"benchmark": "greptime_egress", "status": "running", "variants": [],
           "limits": ["Finite local fixture, not a production zero-egress guarantee.",
                      "Analytics scans are non-user reads, never labelled user queries.",
                      "Offline cache loss and small-cache eviction are explicit boundaries.",
                      "Actual repository CAN coordinator and offline backup execute against the same owned MinIO.",
                      "All response body bytes (including LIST/errors) exclude HTTP headers; cumulative class totals reconcile."]}
    doc["limits"].append("File mode uses explicit verified off-host archive upload; the production File backup CLI remains local-only.")
    doc["limits"].append("Remote-only disaster recovery necessarily downloads the archive and is measured separately.")
    try:
        b.require_local_docker()
        if args.local_input_only:
            strategies = [("local_input_only", args.small_cache, "S3")]
        elif args.maintenance_only:
            strategies = [("deferred", args.small_cache, "S3"),
                          ("pressure", args.small_cache, "S3"),
                          ("resident", args.large_cache, "S3"),
                          ("large_batch", args.large_cache, "S3")]
        elif args.background_only:
            strategies = [(label, args.small_cache, "S3") for label in
                          ("legacy", "on_demand", "deferred")]
        else:
            strategies = [("small", args.small_cache, "S3"),
                          ("large", args.large_cache, "S3")]
            if not args.s3_only:
                strategies.append(("local_primary", args.small_cache, "File"))
        for label, cache, mode in strategies:
            result = {}
            doc["variants"].append(result)
            run_variant(label, args.rows, cache, small, large, result, storage_type=mode,
                        background_policy=label if args.background_only else None,
                        maintenance_policy=label if args.maintenance_only else None,
                        greptime_image=args.greptime_image, local_input_only=args.local_input_only)
            if result["status"] != "success":
                raise RuntimeError(label + " variant failed")
            if args.local_input_only and not result["nonquery_zero_payload_downloads"]:
                raise RuntimeError("strict local-input lifecycle downloaded payload or unknown S3 data")
        doc["comparison"] = {}
        for variant in doc["variants"]:
            doc["comparison"][variant["label"]] = {
                p["name"]: p["sst_payload_delta"] for p in variant["phases"]}
        variants = {v["label"]: v for v in doc["variants"]}
        doc["whole_lifecycle"] = {
            label: {"all_responses": variant["lifecycle_cost"],
                    "nonquery": variant["nonquery_cost"],
                    "nonquery_zero_payload_proven": variant["nonquery_zero_payload_downloads"]}
            for label, variant in variants.items()}
        if args.maintenance_only:
            doc["acceptance"] = {
                "policy_validation_completed": True,
                "native_zero_contract_met": all(v["nonquery_zero_payload_downloads"] for v in variants.values()),
                "finite_file_bounds": {label: v["finite_file_bound_pass"] for label, v in variants.items()},
                "physical_storage_bounds_proven": {
                    label: v["physical_storage_bound_proven"] for label, v in variants.items()},
                "all_window_closes_completed": {
                    label: v["all_window_closes_completed"] for label, v in variants.items()},
                "cold_input_remote_get_recorded": {
                    label: next(p["cost"] for p in variant["phases"]
                                if p["name"] == "cold_regular_compaction")
                    for label, variant in variants.items()},
                "production_recommendation": False,
                "p3_required_if_payload_or_unknown_reads": True}
            doc["status"] = "success"
            return 0
        if args.background_only:
            old = variants["legacy"]["background_sst_read_bytes"]
            best = variants["deferred"]["background_sst_read_bytes"]
            if old <= 0 or best >= old:
                raise RuntimeError("deferred background policy did not reduce native SST reads")
            doc["acceptance"] = {
                "reduced_background_sst_reads": True,
                "routine_sst_get_zero": best == 0,
                "same_small_cache_all_policies": True,
                "manual_analytics_and_compaction_preserved": True,
                "latest_null_preserved_after_restart_and_compaction": True}
            doc["status"] = "success"
            return 0
        if not variants["large"]["nonquery_zero_sst_downloads"]:
            raise RuntimeError("resident S3 cache did not eliminate routine SST downloads")
        if "local_primary" in variants:
            file_variant = variants["local_primary"]
            if not file_variant["nonquery_zero_sst_downloads"]:
                raise RuntimeError("local-primary routine path downloaded SST payload")
            if any(p["counters_delta"]["read_bytes"] for p in file_variant["phases"]):
                raise RuntimeError("File engine unexpectedly used S3 GET")
            if any(p["read_bytes"] for p in file_variant["repository_paths"].values()
                   if p.get("kind") != "restore_exception"):
                raise RuntimeError("local-primary routine backup or analytics downloaded S3 bodies")
        for label, phase_name in (("small", "warm_compaction"), ("large", "cache_loss_compaction")):
            if not next(p for p in variants[label]["phases"] if p["name"] == phase_name)["sst_payload_delta"]["sst_read_bytes"]:
                raise RuntimeError("cache pressure/loss download boundary was not reproduced")
        doc["acceptance"] = {"resident_cache_routine_sst_get_zero": True,
                             "resident_reingest_compaction_cycles": 3,
                             "cold_cache_compaction_boundary_reproduced": True}
        if "local_primary" in variants:
            doc["acceptance"].update(local_primary_routine_all_get_zero=True,
                                     native_restore_from_remote_archive=True)
        doc["status"] = "success"
    except Exception as exc:
        doc.update(status="failure", error=b._redact(str(exc)))
    finally:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
            f.write("\n")
        print(json.dumps({"status": doc["status"], "json_out": args.json_out}))
    return 0 if doc["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
