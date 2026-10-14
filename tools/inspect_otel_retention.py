#!/usr/bin/env python3
"""Read-only OTel retention inspection: effective TTL, volume, growth, budget.

Stdlib only. Never writes, alters, or deletes; every statement is a SELECT
against information_schema or raw tables, verified against the GreptimeDB
1.2.1 SQL dialect (information_schema.tables with table_schema/create_options
filter, COUNT/MIN/MAX, GROUP BY CAST(timestamp AS DATE)). Run from the repo
root:
  python tools/inspect_otel_retention.py --base-url URL --db DB --user U
      --password-env GREPTIME_PASSWORD [--daily-days 30]
      [--sample-rows 20] [--budget-bytes N]
      [--disk-bytes-per-row-traces F] [--disk-bytes-per-row-logs F]
      [--disk-measurement TEXT] [--disk-budget-bytes N]

Prints per-table effective TTL (parsed from the actual create_options
string), measured row counts, oldest/newest event times, the measured daily
row series over the requested current-time window (UTC, zero-input days
count), measured wire bytes per sample row (from a real SELECT LIMIT
payload -- transfer size, never claimed as SST/disk bytes), measured query
stats (result rows, server execution_time_ms, client seconds, request bytes),
a wire-transfer estimate (labeled as such, never physical capacity), and a
disk-based capacity estimate grounded either in a real read-only store
metric exposed over SQL or in explicit operator-measured per-table bytes
with recorded provenance. Store/SST bytes are exactly measurable with an
owned store snapshot; see tools/benchmark_otel_retention.py
--greptime-binary.
"""

import argparse
import base64
import datetime as dt
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request

OTEL_TABLES = ("opentelemetry_traces", "opentelemetry_logs")


def request_sql(base_url, auth, db, stmt, timeout=30):
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db, safe="")
    body = urllib.parse.urlencode({"sql": stmt}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Authorization", "Basic " + auth)
    req.add_header("X-Greptime-Timezone", "UTC")
    start = time.monotonic()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8", "replace"))
    elapsed = time.monotonic() - start
    if payload.get("error"):
        raise RuntimeError(str(payload["error"])[:300])
    return payload, elapsed, len(body)


def fetch_rows(base_url, auth, db, stmt):
    payload, elapsed, size = request_sql(base_url, auth, db, stmt)
    rec = payload["output"][0].get("records") or {}
    schema = rec.get("schema", {}).get("column_schemas", [])
    return ([c.get("name") for c in schema], rec.get("rows", []),
            {"result_rows": len(rec.get("rows", [])),
             "server_ms": payload.get("execution_time_ms"),
             "client_s": round(elapsed, 4), "request_bytes": size})


def parse_ttl(create_options):
    for part in (create_options or "").split():
        if part.startswith("ttl="):
            return part.split("=", 1)[1].strip("'\"")
    return "unknown"


def norm_day(value):
    """Format a DATE-grouped day value as YYYY-MM-DD.

    Greptime may return DATE groups as numeric days; str() alone would
    report e.g. "20340" instead of a calendar day.
    """
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)) and float(value).is_integer():
        return (dt.date(1970, 1, 1)
                + dt.timedelta(days=int(value))).isoformat()
    return str(value)


def window_days_utc(n):
    """The requested current-time window: last n UTC calendar days."""
    today = dt.datetime.now(dt.timezone.utc).date()
    return [(today - dt.timedelta(days=n - 1 - i)).isoformat()
            for i in range(n)]


def combine_daily(series_by_table, days):
    """Combine per-table daily rows on common calendar days.

    Returns (per_table, combined) where per_table maps each table to a
    zero-filled row list aligned to `days`, and combined is the day-wise
    traces+logs sum. Zero-input days count: missing days contribute 0.
    """
    per_table = {}
    for table, series in series_by_table.items():
        by_day = {}
        if isinstance(series, list):
            for entry in series:
                try:
                    by_day[norm_day(entry.get("day"))] = \
                        int(entry.get("rows") or 0)
                except (TypeError, ValueError, AttributeError):
                    continue
        per_table[table] = [by_day.get(day, 0) for day in days]
    combined = [sum(per_table[t][i] for t in series_by_table)
                for i in range(len(days))]
    return per_table, combined


def finite_rate(value):
    """Accept only finite non-negative numeric rates; reject NaN/inf."""
    return (isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def weighted_bytes_per_day(per_table_daily_mean, bytes_per_row):
    """Sum per-table byte rates weighted by actual row rates.

    Never the product of averages: sum over tables of
    (table mean daily rows * that table's bytes/row). Returns
    (total_or_None, per_table_parts); None means no rate was measurable.
    """
    parts = {}
    for table, mean in per_table_daily_mean.items():
        rate = bytes_per_row.get(table)
        if finite_rate(mean) and finite_rate(rate):
            parts[table] = mean * rate
    if not parts:
        return None, {}
    return sum(parts.values()), parts


def main():
    ap = argparse.ArgumentParser(description="read-only OTel retention inspection")
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password-env", default="GREPTIME_PASSWORD")
    ap.add_argument("--daily-days", type=int, default=30)
    ap.add_argument("--sample-rows", type=int, default=20)
    ap.add_argument("--budget-bytes", type=int, default=0,
                    help="wire-transfer budget for the transfer estimate; 0 = "
                    "report measured inputs and the formula only")
    ap.add_argument("--disk-bytes-per-row-traces", type=float, default=None,
                    help="operator-measured disk bytes/row for traces")
    ap.add_argument("--disk-bytes-per-row-logs", type=float, default=None,
                    help="operator-measured disk bytes/row for logs")
    ap.add_argument("--disk-measurement", default="",
                    help="provenance for operator-measured disk bytes/row "
                    "(how/when measured)")
    ap.add_argument("--disk-budget-bytes", type=int, default=0,
                    help="disk-byte budget for the physical capacity "
                    "estimate; 0 = report inputs only")
    args = ap.parse_args()
    if args.daily_days < 1 or args.sample_rows < 1:
        print("inspect: error: --daily-days and --sample-rows must be >= 1",
              file=sys.stderr)
        return 2
    if args.budget_bytes < 0 or args.disk_budget_bytes < 0:
        print("inspect: error: budgets must be >= 0", file=sys.stderr)
        return 2
    for label, value in (
            ("traces", args.disk_bytes_per_row_traces),
            ("logs", args.disk_bytes_per_row_logs)):
        if value is not None and not finite_rate(value):
            print("inspect: error: --disk-bytes-per-row-%s must be a finite number >= 0"
                  % label, file=sys.stderr)
            return 2
    password = os.environ.get(args.password_env, "")
    if not password:
        print("inspect: error: password env %s is empty" % args.password_env,
              file=sys.stderr)
        return 1
    auth = base64.b64encode(
        (args.user + ":" + password).encode("utf-8")).decode("ascii")
    out = {"tables": {}, "daily_days": args.daily_days,
           "queries": {}, "estimates": {}}
    db_esc = args.db.replace("'", "''")
    _, catalog, catalog_stats = fetch_rows(
        args.base_url, auth, args.db,
        "SELECT table_name, create_options FROM information_schema.tables"
        " WHERE table_schema = '%s'" % db_esc)
    out["queries"]["catalog"] = catalog_stats
    options = {name: (opts or "") for name, opts in catalog}
    for table in OTEL_TABLES:
        _, counts, vol_stats = fetch_rows(
            args.base_url, auth, args.db,
            "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM " + table)
        count, first, last = ((counts[0] + [None, None, None])[:3]
                              if counts else (0, None, None))
        _, sample, sample_stats = fetch_rows(
            args.base_url, auth, args.db,
            "SELECT * FROM " + table + " LIMIT %d" % args.sample_rows)
        wire = len(json.dumps(sample).encode("utf-8")) if sample else 0
        per_row = (wire / len(sample)) if sample else None
        out["tables"][table] = {
            "effective_ttl": parse_ttl(options.get(table, "")),
            "rows": count, "oldest": str(first), "newest": str(last),
            # Measured transfer bytes of a real sample payload; not disk.
            "sample_rows": len(sample),
            "sample_wire_bytes": wire,
            "wire_bytes_per_row": per_row,
            "volume_query": vol_stats, "sample_query": sample_stats}
    # Real read-only store metric per Greptime TABLES semantics
    # (https://docs.greptime.com/reference/sql/information-schema/tables):
    # data_length = SST bytes (approx), index_length = index file bytes
    # (approx). Actual used bytes = data_length + index_length only:
    # max_*_length are capacities and avg_row_length is an average, so they
    # MUST NOT be summed into used bytes. avg_row_length is reported
    # separately, labeled, and never used for capacity math.
    store_bytes, store_avg_row, store_meta = {}, {}, {}
    try:
        cols, cat_rows, _ = fetch_rows(
            args.base_url, auth, args.db,
            "SELECT table_name, data_length, index_length, avg_row_length"
            " FROM information_schema.tables"
            " WHERE table_schema = '%s'" % db_esc)
        name_i, data_i, idx_i, avg_i = (cols.index("table_name"),
                                       cols.index("data_length"),
                                       cols.index("index_length"),
                                       cols.index("avg_row_length"))
        for row in cat_rows:
            if row[name_i] in OTEL_TABLES:
                if finite_rate(row[data_i]) and finite_rate(row[idx_i]):
                    store_bytes[row[name_i]] = row[data_i] + row[idx_i]
                store_meta[row[name_i]] = {
                    "data_length": row[data_i], "index_length": row[idx_i]}
                if finite_rate(row[avg_i]):
                    store_avg_row[row[name_i]] = row[avg_i]
    except Exception:  # noqa: BLE001 -- no byte counter exposed; operator path
        store_bytes, store_avg_row, store_meta = {}, {}, {}
    out["store_bytes_probe"] = {
        "columns": ["data_length", "index_length"],
        "per_table": store_bytes or None,
        "per_table_metadata": store_meta or None,
        # Labeled only; never summed into used bytes or capacity math.
        "avg_row_length": store_avg_row or None,
        "note": ("data_length (SST bytes, approx) + index_length (index "
                 "file bytes, approx) = actual used bytes; max_data_length/"
                 "max_index_length are capacities and avg_row_length is an "
                 "average, excluded; null when absent; never estimated"),
    }
    days = window_days_utc(args.daily_days)
    cutoff = days[0] + " 00:00:00"
    out["window_days"] = days
    out["window_start"] = cutoff
    out["daily_series"] = {}
    for table in OTEL_TABLES:
        try:
            _, series, series_stats = fetch_rows(
                args.base_url, auth, args.db,
                "SELECT CAST(timestamp AS DATE) AS day, COUNT(*) FROM " + table
                + " WHERE timestamp >= '" + cutoff + "'"
                + " GROUP BY CAST(timestamp AS DATE)"
                " ORDER BY CAST(timestamp AS DATE) DESC LIMIT %d" % args.daily_days)
            out["daily_series"][table] = [
                {"day": norm_day(day), "rows": n} for day, n in series]
            out["queries"]["daily_" + table] = series_stats
        except Exception as e:  # noqa: BLE001 -- dialect fallback, totals still stand
            out["daily_series"][table] = {"unavailable": str(e)[:120]}
    avail = {t: isinstance(out["daily_series"].get(t), list)
             for t in OTEL_TABLES}
    growth_known = all(avail.values())
    per_table, combined = combine_daily(
        {t: (out["daily_series"][t] if avail[t] else []) for t in OTEL_TABLES},
        days)
    if not growth_known:
        combined = None
    per_mean = ({t: sum(v) / args.daily_days for t, v in per_table.items()}
                if growth_known else {})
    mean_daily = (sum(combined) / args.daily_days
                  if combined is not None else None)
    wire_row = {t: out["tables"][t].get("wire_bytes_per_row")
                for t in OTEL_TABLES}
    wire_row = {t: v for t, v in wire_row.items() if finite_rate(v)}
    wire_day, wire_parts = weighted_bytes_per_day(per_mean, wire_row)
    unknown = []
    op_disk = {"opentelemetry_traces": args.disk_bytes_per_row_traces,
               "opentelemetry_logs": args.disk_bytes_per_row_logs}
    disk_row, disk_prov = {}, {}
    for table in OTEL_TABLES:
        if op_disk[table] is not None:
            if not finite_rate(op_disk[table]):
                unknown.append("disk rate invalid (non-finite --disk-bytes-per-row-%s)"
                               % ("traces" if "traces" in table else "logs"))
                continue
            disk_row[table] = op_disk[table]
            disk_prov[table] = ("operator-measured" + (
                ": " + args.disk_measurement if args.disk_measurement
                else " (no provenance given)"))
        elif (table in store_bytes
                and isinstance(out["tables"][table].get("rows"), int)
                and out["tables"][table]["rows"]):
            disk_row[table] = (store_bytes[table]
                               / out["tables"][table]["rows"])
            disk_prov[table] = "information_schema:data_length+index_length"
    disk_day, disk_parts = weighted_bytes_per_day(per_mean, disk_row)
    if not growth_known:
        unknown.append("daily growth unavailable for: "
                       + ",".join(t for t in OTEL_TABLES if not avail[t]))
    if growth_known and wire_day is None:
        unknown.append("wire rate unavailable (no sample rows)")
    if growth_known and disk_day is None:
        unknown.append("disk rate unavailable "
                       "(no store metric or --disk-bytes-per-row-*)")
    empty = combined is not None and sum(combined) == 0
    totals = [t["rows"] or 0 for t in out["tables"].values()
              if isinstance(t.get("rows"), int)]
    out["estimates"] = {
        "total_rows": sum(totals) if totals else 0,
        # Combined traces+logs growth over the requested current window;
        # zero-input days count in the denominator (no stale history).
        "combined_daily_rows": combined,
        "per_table_daily_rows": per_table if growth_known else None,
        "mean_daily_rows": mean_daily,
        # Wire transfer only; never physical capacity.
        "wire_bytes_per_row": wire_row or None,
        "wire_bytes_per_day": wire_day,
        "wire_bytes_per_day_by_table": wire_parts,
        "budget_wire_bytes": args.budget_bytes or None,
        "retention_days_at_budget": None,
        # Honest physical capacity: disk-based rate and budget only.
        "disk_bytes_per_row": disk_row or None,
        "disk_provenance": disk_prov or None,
        "disk_bytes_per_day": disk_day,
        "disk_bytes_per_day_by_table": disk_parts,
        "disk_budget_bytes": args.disk_budget_bytes or None,
        "retention_days_at_disk_budget": None,
        "empty_window": empty,
        "unknown": unknown or None,
        "capacity_note": ("retention_days ~= budget_bytes / bytes_per_day "
                          "within the measured window above; wire bytes are "
                          "transfer size, never SST/disk bytes; disk days "
                          "need a real store metric or operator-measured "
                          "bytes/row with provenance; empty/unknown are "
                          "reported separately, never as stale history; "
                          "never generalize synthetic numbers to production"),
    }
    if args.budget_bytes and wire_day:
        out["estimates"]["retention_days_at_budget"] = round(
            args.budget_bytes / wire_day, 2)
    if args.disk_budget_bytes and disk_day:
        out["estimates"]["retention_days_at_disk_budget"] = round(
            args.disk_budget_bytes / disk_day, 2)
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
