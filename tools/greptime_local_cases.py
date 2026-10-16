"""Fail-closed native local-SST proof helpers used by the existing egress harness."""
import json
import re
import time


REQUIRED_CASES = frozenset({
    "warm_windows", "restart", "cache_loss", "missing_input_repair",
    "eviction_recovery_race", "quota_repair", "filesystem_full_repair",
    "parquet_format", "primary_key_format", "puffin_rebuild", "async_index_rebuild",
    "whole_ttl", "mixed_ttl", "engine_gc", "physical_bounds", "cold_user_query", "remote_only_restore",
    "all_summary_families", "unclean_wal_replay", "automatic_compaction",
})


def mirror_toml(text, capacity="256MiB", reserve="32MiB"):
    section = "[region_engine.mito]"
    if text.count(section) != 1:
        raise ValueError("expected one native Mito configuration")
    if re.search(r"(?m)^sst_mirror_", text):
        raise ValueError("mirror settings already present")
    settings = ('sst_mirror_path = "/data/sst-mirror-v1"\n'
                'sst_mirror_capacity = "%s"\n'
                'sst_mirror_output_reserve = "%s"\n' % (capacity, reserve))
    return text.replace(section, section + "\n" + settings)


def strict_compose(text):
    anchor = '"--config-file", "/etc/greptime/greptimedb.toml"]'
    if text.count(anchor) != 1:
        raise ValueError("native startup capability anchor changed")
    return text.replace(anchor, '"--config-file", "/etc/greptime/greptimedb.toml", '
                        '"--require-local-sst-mirror-v1"]')


def manifest_identity(rows):
    return tuple(sorted(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows))


def expect_preserved_failure(action, manifest, marker, logs, source_snapshot=None):
    """A refusal is not successful maintenance; retain the exact error and logs."""
    before = manifest_identity(manifest())
    source_before = source_snapshot() if source_snapshot else None
    started = time.monotonic()
    try:
        action()
    except Exception as exc:
        evidence = {"error": str(exc), "logs": logs(), "seconds": time.monotonic() - started,
                    "completed_proven": False}
        if marker not in evidence["error"] and marker not in evidence["logs"]:
            raise AssertionError({"unexpected_failure": evidence, "required_marker": marker}) from exc
        if manifest_identity(manifest()) != before:
            raise AssertionError({"manifest_changed_after_refusal": evidence}) from exc
        evidence["manifest_preserved"] = True
        if source_snapshot:
            source_after = source_snapshot()
            if source_before != source_after:
                raise AssertionError({"source_payload_changed_after_refusal": evidence}) from exc
            evidence["source_payload_preserved"] = source_before
        return evidence
    raise AssertionError("expected native refusal, maintenance unexpectedly succeeded")


def gc_retired_ids(objects, retired_ids):
    """Inspect real object names, not a teardown or manifest-only disappearance."""
    return sorted(identity for identity in retired_ids
                  if any(identity in key.rsplit("/", 1)[-1] for key, _ in objects))


def require_complete(cases):
    missing = sorted(REQUIRED_CASES - {name for name, value in cases.items()
                                    if value.get("proven") is True})
    if missing:
        raise RuntimeError("local-input-only proof incomplete: " + ", ".join(missing))
    return {"all_required_cases_proven": True, "required_cases": sorted(REQUIRED_CASES)}


def run_missing_input(ctx):
    """Remove only owned mirrored Parquet inputs, then restore those same inodes."""
    table = ctx["table"]
    ctx["phase"]("local_missing_seed", "nonquery", ctx["seed"])
    manifest = lambda: ctx["files"](table)
    before = manifest()
    if len(before) < 2:
        raise AssertionError("missing-input fixture needs multiple compactable SSTs")
    ids = [str(row["file_id"]) for row in before]
    ctx["mirror_fault"](ids, False)
    try:
        refusal = ctx["phase"](
            "local_missing_input_refusal", "maintenance",
            lambda: expect_preserved_failure(lambda: ctx["compact"](table),
                                             manifest, "SST_MIRROR_MISS", ctx["logs"],
                                             lambda: ctx["snapshot"](manifest())))
    finally:
        ctx["mirror_fault"](ids, True)
    repaired = ctx["phase"]("local_missing_input_repaired", "maintenance",
                            lambda: ctx["compact"](table))
    if not repaired.get("completed_proven"):
        raise AssertionError("restored input did not complete real maintenance")
    return {"proven": True, "refusal": refusal, "repair": repaired,
            "source_file_ids": ids}


def run_formats_and_indexes(ctx):
    cases = {}
    for format_name in ("flat", "primary_key"):
        table = "p3_format_" + format_name
        def seed():
            ctx["sql"]("CREATE TABLE %s(ts TIMESTAMP(3) TIME INDEX, series STRING, "
                       '"value" DOUBLE, PRIMARY KEY(series)) WITH (\'sst_format\'=\'%s\')' % (table, format_name))
            for version in range(4):
                value = "NULL" if version == 3 else str(version)
                ctx["sql"]("INSERT INTO %s VALUES ('2026-01-01 00:00:00','a',%s),"
                           "('2026-01-01 00:00:01','b',2)" % (table, value))
                ctx["sql"]("ADMIN FLUSH_TABLE('%s')" % table)
        ctx["phase"]("local_%s_seed" % format_name, "nonquery", seed)
        completion = ctx["phase"]("local_%s_compact" % format_name, "maintenance",
                                  lambda: ctx["compact"](table))
        if not completion.get("completed_proven"):
            raise AssertionError("format fixture did not complete compaction")
        def oracle():
            rows = ctx["records"]('SELECT series,"value" FROM %s ORDER BY series' % table)
            if rows != [{"series": "a", "value": None}, {"series": "b", "value": 2.0}]:
                raise AssertionError(rows)
            return rows
        ctx["phase"]("local_%s_oracle" % format_name, "verification_query", oracle)
        cases["parquet_format" if format_name == "flat" else "primary_key_format"] = {
            "proven": True, "native_format": format_name, "completion": completion}

        def build(async_only):
            previous = ctx["configure"]({"index.build_mode": "async"}) if async_only else None
            try:
                before = ctx["files"](table)
                ctx["sql"]('ALTER TABLE %s MODIFY COLUMN "value" SET INVERTED INDEX' % table)
                if not async_only:
                    ctx["sql"]("ADMIN BUILD_INDEX('%s')" % table)
                deadline = time.monotonic() + 120
                while True:
                    after = ctx["files"](table)
                    old_indexes = {row.get("index_file_path") for row in before if row.get("index_file_path")}
                    new_indexes = {row.get("index_file_path") for row in after if row.get("index_file_path")}
                    if new_indexes and new_indexes != old_indexes and all(
                            int(row.get("index_file_size") or 0) > 0 for row in after):
                        return {"proven": True, "before": before, "after": after,
                                "async_schema_rebuild": async_only}
                    if time.monotonic() >= deadline:
                        raise AssertionError({"index_publication_not_observed": after, "before": before})
                    time.sleep(1)
            except Exception:
                ctx["index_failure_logs"].append(ctx["logs"]())
                raise
            finally:
                if previous is not None:
                    ctx["configure"](previous)
        name = "async_index_rebuild" if format_name == "flat" else "puffin_rebuild"
        cases[name] = ctx["phase"]("local_" + name, "maintenance",
                                   lambda: build(format_name == "flat"))
        ctx["phase"]("local_%s_postindex_oracle" % format_name, "verification_query", oracle)
    return cases


def run_quota(ctx):
    table = ctx["table"]
    ctx["phase"]("local_quota_seed", "nonquery", ctx["seed"])
    previous = ctx["phase"]("local_quota_limit", "nonquery",
                            lambda: ctx["configure"]({"sst_mirror_output_reserve": "1B"}))
    try:
        refusal = ctx["phase"](
            "local_quota_refusal", "maintenance",
            lambda: expect_preserved_failure(lambda: ctx["compact"](table),
                                             lambda: ctx["files"](table),
                                             "SST_MIRROR_CAPACITY", ctx["logs"],
                                             lambda: ctx["snapshot"](ctx["files"](table))))
    finally:
        ctx["phase"]("local_quota_restore", "nonquery", lambda: ctx["configure"](previous))
    completion = ctx["phase"]("local_quota_repaired", "maintenance",
                              lambda: ctx["compact"](table))
    if not completion.get("completed_proven"):
        raise AssertionError("quota repair did not complete real maintenance")
    return {"proven": True, "refusal": refusal, "repair": completion}


def run_eviction_recovery_race(ctx):
    from concurrent.futures import ThreadPoolExecutor
    ctx["phase"]("local_race_seed", "nonquery", ctx["seed"])
    # No warm-up query or recovery-idle sleep: submit during cache recovery.
    ctx["phase"]("local_race_restart", "nonquery", ctx["restart"])
    def race():
        with ThreadPoolExecutor(max_workers=2) as workers:
            compact = workers.submit(ctx["compact"], ctx["table"])
            ingest = workers.submit(ctx["seed"])
            written = ingest.result()
            completed = compact.result()
        if not completed.get("completed_proven"):
            raise AssertionError("concurrent compaction did not complete")
        return {"proven": True, "completion": completed, "concurrent_flush": written,
                "query_warming": False}
    return ctx["phase"]("local_eviction_recovery_race", "maintenance", race)


def bounded_tmpfs_compose(text):
    anchor = "  greptime-fault-data:\n"
    if text.count(anchor) != 1:
        raise ValueError("owned tmpfs volume anchor changed")
    return text.replace(anchor, anchor + '    driver_opts:\n      type: tmpfs\n'
                        '      device: tmpfs\n      o: "size=1073741824"\n')


def tmpfs_fault_code(repair):
    # Kernel mount identity is checked inside the container before ANY writes.
    return (
        "import os,json,errno\n"
        "mounts=open('/proc/self/mountinfo').read().splitlines()\n"
        "owned=[m for m in mounts if m.split()[4]=='/data']\n"
        "if len(owned)!=1 or ' - tmpfs ' not in owned[0]: raise RuntimeError('refuse non-tmpfs /data')\n"
        "st=os.statvfs('/data')\n"
        "if st.f_blocks*st.f_frsize>1073741824: raise RuntimeError('refuse unbounded tmpfs')\n"
        "path='/data/.p3-owned-full-fault'\n"
        "if %r:\n"
        " os.unlink(path)\n"
        " print(json.dumps(dict(repaired=True)))\n"
        "else:\n"
        " written=0; failure=None\n"
        " with open(path,'xb',buffering=0) as output:\n"
        "  try:\n"
        "   while True: written+=output.write(bytes(1048576))\n"
        "  except OSError as exc:\n"
        "   if exc.errno!=errno.ENOSPC: raise\n"
        "   failure=dict(errno=exc.errno,error=str(exc))\n"
        " if failure is None: raise RuntimeError('actual ENOSPC not reached')\n"
        " print(json.dumps(dict(bytes_written=written,actual_enospc=failure,mount=owned[0])))\n"
    ) % repair


def run_filesystem_full(ctx):
    ctx["phase"]("local_full_seed", "nonquery", ctx["seed"])
    fault = ctx["phase"]("local_actual_tmpfs_full", "nonquery",
                         lambda: ctx["tmpfs_fault"](False))
    try:
        refusal = ctx["phase"](
            "local_full_refusal", "maintenance",
            lambda: expect_preserved_failure(lambda: ctx["compact"](ctx["table"]),
                                             lambda: ctx["files"](ctx["table"]),
                                             "SST_MIRROR_CAPACITY", ctx["logs"],
                                             lambda: ctx["snapshot"](ctx["files"](ctx["table"]))))
    finally:
        ctx["phase"]("local_full_space_repair", "nonquery", lambda: ctx["tmpfs_fault"](True))
    completion = ctx["phase"]("local_full_repaired", "maintenance",
                              lambda: ctx["compact"](ctx["table"]))
    if not completion.get("completed_proven"):
        raise AssertionError("filesystem repair did not complete maintenance")
    return {"proven": True, "fault": fault, "refusal": refusal, "repair": completion}


def active_payload_paths(rows):
    return {row[key].lstrip("/") for row in rows for key in ("file_path", "index_file_path")
            if row.get(key)}


def payload_bound(active_rows, remote_objects, local_objects, root):
    """Exact payload namespaces, including versioned Puffin; no billing extrapolation."""
    active = active_payload_paths(active_rows)
    remote = {key[len(root) + 1:]: size for key, size in remote_objects
              if key.startswith(root.rstrip("/") + "/")
              and not key.startswith(root.rstrip("/") + "/p3-filesystem-full/")
              and key.endswith((".parquet", ".sst", ".puffin"))}
    local = dict(local_objects)
    missing_remote, missing_local = active - remote.keys(), active - local.keys()
    stale_remote, stale_local = remote.keys() - active, local.keys() - active
    return {"proven": bool(active) and not (missing_remote or missing_local or stale_remote or stale_local),
            "active_paths": sorted(active), "missing_remote": sorted(missing_remote),
            "missing_local": sorted(missing_local), "stale_remote": sorted(stale_remote),
            "stale_local": sorted(stale_local), "remote_payload_bytes": sum(remote.values()),
            "local_payload_bytes": sum(local.values()), "active_files": len(active),
            "scope": "exact retained payload plus indexes; control/WAL/cache disk reported separately; finite bounded replay fixture"}


def mirror_snapshot_code(rows):
    paths = sorted(active_payload_paths(rows))
    if not paths or any(".." in path.split("/") or path.startswith("/") for path in paths):
        raise ValueError("invalid source mirror paths")
    return (
        "import pathlib,json,hashlib\n"
        "root=pathlib.Path('/data/sst-mirror-v1/objects')\n"
        "result={}\n"
        "for key in json.loads(%r):\n"
        " p=root/key\n"
        " if not p.exists(): p=p.with_name(p.name+'.proof-held')\n"
        " if p.is_symlink(): raise RuntimeError('source mirror symlink')\n"
        " digest=hashlib.sha256()\n"
        " with p.open('rb') as source:\n"
        "  for chunk in iter(lambda:source.read(1048576),b''): digest.update(chunk)\n"
        " result[key]=dict(size=p.stat().st_size,sha256=digest.hexdigest())\n"
        "print(json.dumps(result,sort_keys=True))\n"
    ) % json.dumps(paths)


def run_automatic_compaction(ctx):
    table = "p3_automatic"
    previous = ctx["phase"]("local_automatic_enable", "nonquery",
                           lambda: ctx["configure"]({"min_compaction_interval": "0m"}))
    try:
        def run():
            ctx["sql"]("CREATE TABLE %s(ts TIMESTAMP(9) TIME INDEX, k STRING, val DOUBLE, "
                       "PRIMARY KEY(k)) WITH ('compaction.type'='twcs', "
                       "'compaction.twcs.trigger_file_num'='2')" % table)
            before = ctx["metrics"]()
            seen = set()
            for value in range(8):
                ctx["sql"]("INSERT INTO %s VALUES ('2026-01-01T00:00:00Z','v',%d),"
                           "('2026-01-01T00:00:00Z','n',%s)" % (table, value, "NULL" if value == 7 else value))
                ctx["sql"]("ADMIN flush_table('%s')" % table)
                seen.update(row["file_id"] for row in ctx["files"](table))
            deadline = time.monotonic() + 180
            while True:
                current = ctx["files"](table)
                after = ctx["metrics"]()
                retired = seen - {row["file_id"] for row in current}
                if after["failures"] > before["failures"]:
                    raise AssertionError("automatic local-input compaction failed")
                if (retired and len(current) < 8 and after["inflight"] == 0
                        and after["completed"] > before["completed"]):
                    return {"proven": True, "retired_inputs": len(retired),
                            "active_files": len(current), "before_metrics": before,
                            "after_metrics": after, "manual_compaction_used": False}
                if time.monotonic() >= deadline:
                    raise AssertionError("automatic compaction did not retire its own flushed inputs")
                time.sleep(1)
        result = ctx["phase"]("local_automatic_merge", "maintenance", run)
        def oracle():
            rows = ctx["records"]("SELECT k,val FROM %s ORDER BY k" % table)
            if rows != [{"k": "n", "val": None}, {"k": "v", "val": 7.0}]:
                raise AssertionError(("automatic last-row/NULL", rows))
            return rows
        ctx["phase"]("local_automatic_oracle", "verification_query", oracle)
        return result
    finally:
        ctx["phase"]("local_automatic_restore_policy", "nonquery", lambda: ctx["configure"](previous))
