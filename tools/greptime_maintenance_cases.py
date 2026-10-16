"""Small native cases run inside the existing owned MinIO/Greptime harness."""

import datetime as dt
import re
import time


def file_ids(files):
    """Compare active manifest identities, never infer completion from new output."""
    result = set()
    for row in files:
        identity = row.get("file_id", row.get("file_path"))
        if identity is None:
            raise AssertionError("active SST metadata has no file identity")
        result.add(str(identity))
    return result


def assert_rows(actual, expected):
    """Keep NULL distinct from zero; duplicate/replayed keys must not add rows."""
    observed = [(row["series"], row["value"], int(row["version"])) for row in actual]
    assert observed == expected, {"actual": observed, "expected": expected}
    return {"rows": len(observed), "exact_values_and_nulls": True}


def is_scan_limit_error(exc):
    # Match the engine error variant/display, not arbitrary resource failures.
    return bool(re.search(r"TooManyFilesToRead\b|Too many files to read concurrently: \d+, max allowed: \d+\b", str(exc)))


def mixed_sst_files(files, cutoff_ns):
    """Require a physical SST spanning TTL, not two whole-file expiry cases."""
    mixed = [row for row in files
             if int(row["min_ts"]) < cutoff_ns < int(row["max_ts"])
             and int(row["num_rows"]) >= 2]
    assert mixed, "fixture has no physical SST containing both expired and live rows"
    return mixed


def run_boundaries(ctx):
    sql, records, phase = ctx["sql"], ctx["records"], ctx["phase"]
    evidence = {}
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)

    def stamp(delta):
        return (now + dt.timedelta(seconds=delta)).strftime("%Y-%m-%d %H:%M:%S")

    def operation(name, kind, callback):
        return phase("boundary_" + name, kind, callback)

    def create(table, ttl=None, partition=False):
        suffix = " PARTITION ON COLUMNS (\"series\") (\"series\" < 'm', \"series\" >= 'm')" if partition else ""
        options = " WITH (ttl='%s')" % ttl if ttl else ""
        sql("CREATE TABLE \"%s\"(\"ts\" TIMESTAMP(3) NOT NULL TIME INDEX,\"series\" STRING,"
            "\"value\" DOUBLE,\"version\" BIGINT,PRIMARY KEY(\"series\"))%s%s" % (table, suffix, options))

    def put(table, rows):
        values = ["('%s','%s',%s,%d)" % (when, series, "NULL" if value is None else repr(value), version)
                  for when, series, value, version in rows]
        sql("INSERT INTO \"%s\"(\"ts\",\"series\",\"value\",\"version\") VALUES %s" % (table, ",".join(values)))
        sql("ADMIN flush_table('%s')" % table)

    def manifest(table):
        return ctx["files"](table)

    def check(table, expected, where=""):
        return assert_rows(records("SELECT \"series\",\"value\",\"version\" FROM \"%s\" %s ORDER BY \"ts\",\"series\"" %
                                   (table, where)), expected)

    # Old event timestamps are genuinely beyond the engine's wall-clock TTL.
    whole = "p2b_expiry_whole"
    def seed_whole():
        create(whole, "1h")
        put(whole, [(stamp(-172800), "a", 1.0, 1)])
        files = manifest(whole)
        assert files, "whole-expiry case needs an actual flushed SST"
        return files
    before = operation("expiry_whole_seed", "nonquery", seed_whole)
    def retire_whole():
        completion = ctx["compact"](whole)
        after = manifest(whole)
        assert not file_ids(before).intersection(file_ids(after)), "expired SST remains active"
        assert not after, "whole-expired fixture must retire without replacement SST"
        return {"before_files": before, "after_files": after, "completion": completion}
    evidence["whole_expiry"] = operation("expiry_whole_retirement", "maintenance", retire_whole)
    cost = ctx["phase_cost"]("boundary_expiry_whole_retirement")
    assert cost["payload_read_bytes"] == 0 and cost["unknown_read_bytes"] == 0, cost
    evidence["whole_expiry"]["zero_payload_and_unknown"] = True
    operation("expiry_whole_oracle", "verification_query", lambda: check(whole, []))

    mixed = "p2b_expiry_mixed"
    # Both rows share the previous native hour window, with the TTL cutoff
    # between them and fifteen minutes of live-row margin.
    mixed_now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    previous_hour = mixed_now.replace(minute=0, second=0) - dt.timedelta(hours=1)
    cutoff = previous_hour + dt.timedelta(minutes=30)
    ttl_seconds = int((mixed_now - cutoff).total_seconds())
    old_stamp = (previous_hour + dt.timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
    live_stamp = (previous_hour + dt.timedelta(minutes=45)).strftime("%Y-%m-%d %H:%M:%S")
    def seed_mixed():
        create(mixed, "%ds" % ttl_seconds)
        for version in (1, 2):
            put(mixed, [(old_stamp, "old", float(version), version),
                        (live_stamp, "live", float(version), version)])
        files = manifest(mixed)
        mixed_sst_files(files, time.time_ns() - ttl_seconds * 1_000_000_000)
        return files
    before = operation("expiry_mixed_seed", "nonquery", seed_mixed)
    def compact_mixed():
        cutoff_ns = time.time_ns() - ttl_seconds * 1_000_000_000
        spanning = mixed_sst_files(manifest(mixed), cutoff_ns)
        return {"completion": ctx["compact"](mixed), "ttl_seconds": ttl_seconds,
                "wall_clock_ttl_cutoff_ns": cutoff_ns, "spanning_input_files": spanning}
    completion = operation("expiry_mixed_compact", "maintenance", compact_mixed)
    def mixed_oracle():
        rows = records("SELECT \"series\",\"value\",\"version\" FROM \"%s\" ORDER BY \"ts\",\"series\"" % mixed)
        live = [row for row in rows if row["series"] == "live"]
        assert_rows(live, [("live", 2.0, 2)])
        old = [row for row in rows if row["series"] == "old"]
        assert_rows(old, [("old", 2.0, 2)] if old else [])
        return {"live_preserved": True, "expired_rows_visible": len(old),
                "ttl_semantics": "expired rows remain query-visible" if old else "expired rows removed",
                "row_level_ttl_guaranteed": False, "before_files": before,
                "after_files": manifest(mixed), "completion": completion}
    evidence["mixed_expiry"] = operation("expiry_mixed_oracle", "verification_query", mixed_oracle)

    table = "p2b_scan_limit"
    def seed_limit():
        create(table)
        for version in range(12):
            put(table, [(stamp(-120), "a", float(version), version),
                        (stamp(-60), "a", float(version), version)])
        files = manifest(table)
        assert len(files) > 8, "scan boundary requires more than eight overlapping SSTs"
        return files
    before = operation("scan_limit_seed", "nonquery", seed_limit)
    previous = operation("scan_limit_configure", "nonquery",
                         lambda: ctx["configure"]({"max_concurrent_scan_files": 8}))
    try:
        def expect_limit():
            try:
                records("SELECT \"series\",\"value\",\"version\" FROM \"%s\" ORDER BY \"ts\",\"series\"" % table)
            except Exception as exc:
                if not is_scan_limit_error(exc):
                    raise
                return {"native_error": "TooManyFilesToRead", "exact_boundary_observed": True}
            raise AssertionError("native TooManyFilesToRead was not raised")
        error = operation("scan_limit_error", "verification_query", expect_limit)
        def recover_limit():
            files = manifest(table)
            passes = []
            # Regular TWCS may merge only a subset per ADMIN invocation.
            # Require actual manifest progress; never treat a no-op as recovery.
            while len(files) > 8:
                old_count = len(files)
                completion = ctx["compact"](table)
                files = manifest(table)
                assert len(files) < old_count, "regular compaction made no scan-boundary progress"
                passes.append({"completion": completion, "after_files": files})
            return {"passes": passes, "after_files": files, "limit_unchanged": 8}
        completion = operation("scan_limit_warm_compact", "maintenance", recover_limit)
        oracle = operation("scan_limit_recovery", "verification_query",
                           lambda: check(table, [("a", 11.0, 11), ("a", 11.0, 11)]))
        evidence["scan_limit"] = {"configured_limit": 8, "before_files": before,
                                  "error": error, "completion": completion, "oracle": oracle}
    finally:
        operation("scan_limit_restore", "nonquery", lambda: ctx["configure"](previous))

    table = "p2b_range_crossing"
    def seed_range():
        create(table)
        # Pin the candidate bucket: an inferred window may exclude the range
        # even though each physical file spans it. This is not a row filter.
        sql("ALTER TABLE %s SET 'compaction.twcs.time_window'='24h'" % table)
        for version in range(4):
            put(table, [("2026-01-02 10:00:00", "a", float(version), version),
                        ("2026-01-02 12:00:00", "a", float(version), version)])
        files = manifest(table)
        assert len(files) >= 2, "range crossing requires multiple real SSTs"
        return files
    before = operation("range_seed", "nonquery", seed_range)
    completion = operation("range_compact", "maintenance",
                           lambda: ctx["compact"](table, "2026-01-02 10:30:00", "2026-01-02 11:30:00"))
    def range_oracle():
        oracle = check(table, [("a", 3.0, 3), ("a", 3.0, 3)])
        after = manifest(table)
        return {"before_files": before, "after_files": after,
                "removed_file_ids": sorted(file_ids(before) - file_ids(after)),
                "added_file_ids": sorted(file_ids(after) - file_ids(before)),
                "input_file_spans_requested_range": True,
                "strict_file_filter_assumed": False, "completion": completion, "oracle": oracle}
    evidence["range_crossing"] = operation("range_oracle", "verification_query", range_oracle)

    table = "p2b_durability"
    def seed_durability():
        create(table, partition=True)
        put(table, [(stamp(-10800), "a", 1.0, 1), (stamp(-7200), "z", 2.0, 1),
                    (stamp(-3600), "a", 3.0, 1)])
        # Correction plus a latest NULL; replay the exact acknowledged write.
        correction = [(stamp(-10800), "a", 9.0, 2), (stamp(-3600), "a", None, 2)]
        put(table, correction)
        put(table, correction)
        # Late previous-window arrival is a new key, not a correction.
        put(table, [(stamp(-9000), "z", 4.0, 1)])
        files = manifest(table)
        regions = {str(row["region_id"]) for row in files if "region_id" in row}
        assert len(regions) >= 2, "partition fixture must have active SSTs in multiple regions"
        return {"files": files, "regions": sorted(regions)}
    seeded = operation("durability_seed", "nonquery", seed_durability)
    expected = [("a", 9.0, 2), ("z", 4.0, 1), ("z", 2.0, 1), ("a", None, 2)]
    operation("durability_restart", "nonquery", ctx["restart"])
    restart_oracle = operation("durability_restart_oracle", "verification_query", lambda: check(table, expected))
    operation("durability_cache_loss", "nonquery", ctx["reset_cache"])
    cold_oracle = operation("durability_cold_oracle", "verification_query", lambda: check(table, expected))
    evidence["durability"] = {"seed": seeded, "restart": restart_oracle, "cache_loss": cold_oracle,
                              "latest_null_correction_replay_late_windows_preserved": True}
    return evidence
