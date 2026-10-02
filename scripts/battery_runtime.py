#!/usr/bin/env python3
"""Battery analytics runtime: pure analyzers inside the aggregate job.

Stdlib only. No new service, no broker, no live calls: reads
vehicle_signal/vehicle_event over Greptime HTTP SQL and writes flat rows to
vehicle_analysis. Other aggregate sections are untouched; this module is
called from aggregate.py battery_section and from its own one-shot CLI.

Env (exact names, coordinated with BatteryIntegrationPackaging):
  BATTERY_ANALYSIS_CONFIG default /app/battery-analysis.json (read-only
    native Compose config; private JSON or uncalibrated {}). Only an
    intentionally absent config (empty value or missing default path)
    yields uncalibrated ({}); a missing EXPLICITLY configured path is a
    config_missing error (visible error rows, never silent uncalibrated).
  BATTERY_LOOKBACK_HOURS default 30, fallback to AGG_LOOKBACK_HOURS when
    empty. Positive int.
  BATTERY_MAX_ROWS default 200000, fallback to AGG_MAX_ROWS when empty.
    Per-table raw cap; over cap fails closed with no partial writes.
  BATTERY_BACKFILL_START / BATTERY_BACKFILL_END default empty. Explicit
    recompute range for windows older than the normal lookback. Each is
    either integer ns or UTC "YYYY-MM-DD HH:MM:SS[.fffffffff]" (T/Z
    accepted). Both or neither; end must exceed start.
  VEHICLE_ID (shared, no BATTERY_VEHICLE): optional vehicle filter.
  GREPTIME_HTTP_URL / GREPTIME_DB / GREPTIME_USER / GREPTIME_PASSWORD
    (shared connection).
  AGG_INTERVAL_SECONDS is the only schedule; battery rides it.

Config JSON shape (plain JSON object, no secrets):
  {"conditions": {}, "energy": {}, "electrical": {}, "rul": {},
   "alerts": {}} -- only these five keys, each value a dict passed
  opaquely to that analyzer under its suffix. Absent/empty dict runs
  uncalibrated (never guessed numbers). New unavailable identities are
  omitted from storage; only prior results get NULL invalidations. Unknown
  top-level keys, non-dict top level, or non-dict suffix values are
  malformed and yield per-scope/window error rows (status error), never
  silent zeros. Per-analyzer thresholds/calibration/model content are
  owned by those modules (electrical/rul/alerts document theirs); this
  runtime never invents calibration. No enable flags: present dicts run,
  missing modules yield missing_module error rows with scope identity.

Execution:
  Hourly sealed windows: seal=floor_hour(now), cutoff=seal-lookback.
  Windows are [ws, ws+3600e9) with inclusive end ws+3600e9-1 so analyzer
  inclusive filters never double-count a boundary sample. The open hour
  [seal, seal+1h) is never written. Backfill adds hourly windows covering
  [backfill_start, backfill_end], deduped against normal windows.
  Stateless: every pass re-runs all windows in scope; no watermark, no
  new broker/engine. Same inputs+config+versions -> same revision
  (overwrites same PK); changed inputs/config -> new revision alongside
  the old; latest revision per window is selected downstream by
  computed_at DESC, revision DESC (never lexical revision alone), keyed
  by (vehicle, metric, source, decode_epoch, analysis_id, window_start)
  exactly as the dashboard RANKED query partitions.
  decision_time_ns is omitted (offline reduction admits all retained
  rows, including unknown-ingest old rows); per-module online filtering
  stays available for future real-time callers.
  Per scope (vehicle,source,decode_epoch) x window, modules run isolated
  via battery_common.run_analyses; a raising module becomes an error row
  enriched with that scope/window/module identity (run_analyses alone
  lacks it). A whole-dispatch raise becomes one battery.dispatch.error
  row (never empty-success). A failed module additionally invalidates
  its previously stored same-(metric,analysis_id,window,epoch) rows by
  writing NULL error revisions over those exact identities (fetched per
  scope before the pass; history preserved, other modules/epochs
  untouched), so the latest view never presents a stale success as
  current. Error indicators (metric battery.<suffix>.error /
  battery.dispatch.error) behave the same way in reverse: a recovered
  module writes an explicit non-health recovery state
  (status=reported, value NULL, value_text=execution_recovered)
  over the same indicator identity, so a stale error never stays
  latest forever after recovery while the historical error row is
  preserved. A failed run never refreshes a value (value NULL on
  unavailable/error). Failure indicator rows use metric
  battery.<suffix>.error for dashboard visibility.

NS precision:
  aggregate.fetch_rows normalizes timestamp ints to datetime (microsecond
  loss) and MUST NOT be used here. This module reads raw SQL responses
  and converts timestamp cells to integer ns exactly: int cells scale by
  column data_type (nano/micro/milli/second), ISO strings parse up to 9
  fraction digits, floats/bools refuse (None). SQL filters and identities
  use 9-digit literals; Grafana MySQL projections still CAST to
  TIMESTAMP(6) for display only.

Late/duplicate/reorder/mapping/config:
  Duplicates collapse via sort_dedup (exact key only; same time with
  different payload/quality/unit/envelope stays). Reordered inputs hash
  equal (sorted canonical). Late rows with event_time inside the fetched
  range are included (new revision, latest wins). Rows older than the
  fetched range are NOT silently ignored: MIN(event_time) is compared to
  the fetch start and a warning names the oldest uncovered instant with
  the exact backfill range needed. Changed decode_epoch is a new scope
  (history preserved); changed config is a new revision (history kept).
  Limitations: episodes starting before fetch_start cannot be rebuilt
  from truncated history; extend the backfill start to include episode
  starts. Unknown-ingest rows are admitted offline (online decision_time
  callers would exclude them).

Persisted identity (coordinated with BatteryDashboardRecovery):
  Table vehicle_analysis PK is
  (vehicle, metric, source, analysis_id, revision) + window_start (TIME
  INDEX). "metric" is quoted (reserved word). episode_id is NOT in the
  PK, so persisted analysis_id incorporates the logical episode_id
  whenever present (analysis_id "battery_alerts:episode:<episode_id>",
  ":context:", ":condition:<field>:<phase>" preserved as provided;
  unknown-start rows keep their unique "battery_alerts:unknown:<hash16>"
  IDs) without changing revision. Generic rule: if a row carries
  episode_id not already in analysis_id, analysis_id becomes
  "<analysis_id>:<episode_id>". Duplicate PKs with differing payloads in
  one run raise (refuse silent overwrite); exact duplicates dedup.

Writes quote every identifier ("window_start","vehicle","metric",...)
  and use 9-digit timestamp literals for window_start/window_end/
  computed_at. Missing vehicle/metric/source/analysis_id/revision or
  window_start rows cannot satisfy NOT NULL and are skipped with a
  warning (counted, never written as NULL).
"""

import base64
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import battery_common as bc

RUNTIME_VERSION = "1.0.3"
ANALYSIS_TABLE = "vehicle_analysis"
SIGNAL_TABLE = "vehicle_signal"
EVENT_TABLE = "vehicle_event"

CONFIG_PATH_DEFAULT = "/app/battery-analysis.json"
CONFIG_SUFFIXES = ("conditions", "energy", "electrical", "rul", "alerts")

MODULE_FILES = (
    ("battery_conditions", "conditions"),
    ("battery_energy", "energy"),
    ("battery_electrical", "electrical"),
    ("battery_rul", "rul"),
    ("battery_alerts", "alerts"),
)

MISSING_TABLE_HINTS = ("not found", "not exist", "does not exist",
                       "unknown table")
HOUR_NS = 3_600_000_000_000
SIGNAL_CONTEXT_NS = 24 * HOUR_NS
EPOCH = dt.datetime(1970, 1, 1)


class BatteryError(Exception):
    pass


class BatteryConfigError(BatteryError):
    pass


class BatteryDuplicateError(BatteryError):
    pass


class BatteryCapError(BatteryError):
    pass


def env(name, default=""):
    return os.environ.get(name, default)


def _sql_str(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _sql_num(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "NULL"
    if isinstance(value, float) and value.is_integer() \
            and -(1 << 63) <= value < (1 << 63):
        return str(int(value))
    return repr(value)


def ns_to_sql_ts(timestamp_ns):
    """Integer ns -> UTC 'YYYY-MM-DD HH:MM:SS.fffffffff' (no quotes)."""
    secs = timestamp_ns // 1_000_000_000
    rem = timestamp_ns % 1_000_000_000
    base = EPOCH + dt.timedelta(seconds=secs)
    return base.strftime("%Y-%m-%d %H:%M:%S") + ".%09d" % rem

def _sql_ts(timestamp_ns):
    if timestamp_ns is None:
        return "NULL"
    return "'" + ns_to_sql_ts(timestamp_ns) + "'"


def request_sql(base_url, auth, db, stmt, timeout=60):
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db,
                                                                   safe="")
    body = urllib.parse.urlencode({"sql": stmt}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Authorization", "Basic " + auth)
    req.add_header("X-Greptime-Timezone", "UTC")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        try:
            raw = exc.read().decode("utf-8", "replace")
        except Exception:
            raw = ""
        raise BatteryError("http " + str(exc.code) + ": " + raw[:300])
    except Exception as exc:
        raise BatteryError("request failed: " + str(exc)[:200])
    try:
        payload = json.loads(raw)
    except Exception:
        raise BatteryError("non-JSON response: " + raw[:200])
    if not isinstance(payload, dict):
        raise BatteryError("malformed response: top-level is not an object")
    if payload.get("error"):
        raise BatteryError(str(payload["error"])[:400])
    output = payload.get("output")
    if not isinstance(output, list) or not output or not all(
            isinstance(item, dict) for item in output):
        raise BatteryError("malformed response: output is not a list")
    return payload


def is_missing_table(err):
    return any(h in str(err).lower() for h in MISSING_TABLE_HINTS)


def fetch_raw(base_url, auth, db, stmt, max_rows):
    """Raw (columns, rows, schema) with NO timestamp conversion."""
    payload = request_sql(base_url, auth, db,
                          stmt.rstrip().rstrip(";") + " LIMIT "
                          + str(max_rows + 1))
    rec = payload["output"][0].get("records")
    if not isinstance(rec, dict):
        raise BatteryError("malformed response: records is not an object")
    schema = (rec.get("schema") or {}).get("column_schemas") or []
    if not all(isinstance(c, dict) and isinstance(c.get("name"), str)
               for c in schema):
        raise BatteryError("malformed response: column schema names")
    rows = rec.get("rows", [])
    if not isinstance(rows, list):
        raise BatteryError("malformed response: rows is not a list")
    if len(rows) > max_rows:
        raise BatteryCapError("row cap exceeded (%d > %d): refusing partial"
                              % (len(rows), max_rows))
    cols = [c.get("name") for c in schema]
    return cols, [list(r) for r in rows], schema


def to_ns_cell(value, data_type=None):
    """Exact ns from a raw Greptime cell; None when unrepresentable.

    Ints scale by data_type unit (nano/micro/milli/second, integer math,
    never float). ISO strings parse up to 9 fraction digits (padded).
    Floats/bools refuse (float64 cannot hold ns exactly).
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        low = str(data_type or "").lower()
        if "nano" in low:
            factor = 1
        elif "micro" in low:
            factor = 1_000
        elif "milli" in low:
            factor = 1_000_000
        elif "second" in low:
            factor = 1_000_000_000
        else:
            factor = 1  # TIMESTAMP(9) columns report nanosecond ints
        try:
            result = value * factor
        except Exception:
            return None
        return result if bc.to_ns(result) is not None else None
    if isinstance(value, dt.datetime):
        base = value.replace(tzinfo=None) - EPOCH
        try:
            total_us = (base.days * 86400 + base.seconds) * 1_000_000 \
                + base.microseconds
        except Exception:
            return None
        return total_us * 1_000
    if isinstance(value, str):
        return parse_time_bound(value, allow_empty=False)
    return None


_BOUND_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:[T ](?P<time>\d{2}:\d{2}:\d{2})(?:\.(?P<frac>\d{1,9}))?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2}(?::?\d{2})?)?)?$")


def _tz_offset_seconds(spec):
    """Offset string to seconds east of UTC; None when invalid."""
    if spec == "Z":
        return 0
    if not isinstance(spec, str) or len(spec) < 5 \
            or spec[0] not in "+-":
        return None
    sign = 1 if spec[0] == "+" else -1
    digits = spec[1:].replace(":", "")
    if len(digits) not in (4, 6) or not digits.isdigit():
        return None
    hours = int(digits[:2])
    minutes = int(digits[2:4]) if len(digits) >= 4 else 0
    seconds = int(digits[4:6]) if len(digits) == 6 else 0
    if hours > 14 or minutes > 59 or seconds > 59:
        return None
    if hours == 14 and (minutes or seconds):
        return None  # no zone exceeds +14:00
    return sign * (hours * 3600 + minutes * 60 + seconds)


def parse_time_bound(text, allow_empty=True):
    """UTC bound to integer ns. Empty -> None when allowed.

    Accepts integer ns strings or
    YYYY-MM-DD[ T]HH:MM:SS[.fffffffff][Z|offset]. The 9-digit fraction is
    preserved with integer math (never float); a strict regex rejects
    garbage such as junk fraction text or invalid offsets. Raises
    BatteryConfigError on malformed input (never guesses, never secrets).
    """
    if text is None or (isinstance(text, str) and not text.strip()):
        if allow_empty:
            return None
        raise BatteryConfigError("empty time bound")
    if isinstance(text, int) and not isinstance(text, bool):
        result = bc.to_ns(text)
        if result is None:
            raise BatteryConfigError("invalid ns bound")
        return result
    if not isinstance(text, str):
        raise BatteryConfigError("invalid time bound type")
    stripped = text.strip()
    if stripped.lstrip("+-").isdigit():
        try:
            num = int(stripped)
        except ValueError:
            raise BatteryConfigError("invalid ns bound")
        result = bc.to_ns(num)
        if result is None:
            raise BatteryConfigError("invalid ns bound")
        return result
    normalized = stripped.replace("T", " ")
    if normalized.endswith("z"):
        normalized = normalized[:-1] + "Z"
    match = _BOUND_RE.match(normalized)
    if match is None:
        raise BatteryConfigError("invalid time bound: " + stripped[:64])
    date_part = match.group("date")
    time_part = match.group("time")
    frac = match.group("frac") or ""
    tz_spec = match.group("tz")
    tz_offset = 0
    if tz_spec:
        tz_offset = _tz_offset_seconds(tz_spec)
        if tz_offset is None:
            raise BatteryConfigError("invalid time bound: " + stripped[:64])
    try:
        if time_part is None:
            base = dt.datetime.strptime(date_part, "%Y-%m-%d")
        else:
            base = dt.datetime.strptime(date_part + " " + time_part,
                                        "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise BatteryConfigError("invalid time bound: " + stripped[:64])
    frac_ns = int((frac + "0" * 9)[:9]) if frac else 0
    delta = base - EPOCH
    try:
        secs = delta.days * 86400 + delta.seconds - tz_offset
    except Exception:
        raise BatteryConfigError("invalid time bound")
    if secs < 0:
        raise BatteryConfigError("pre-epoch time bound refused")
    try:
        total = secs * 1_000_000_000 + frac_ns
    except Exception:
        raise BatteryConfigError("invalid time bound")
    result = bc.to_ns(total)
    if result is None:
        raise BatteryConfigError("invalid time bound")
    return result


def floor_hour_ns(timestamp_ns):
    return timestamp_ns - (timestamp_ns % HOUR_NS)


def ceil_hour_ns(timestamp_ns):
    remainder = timestamp_ns % HOUR_NS
    return timestamp_ns if remainder == 0 else timestamp_ns + (
        HOUR_NS - remainder)


def load_analysis_config(path, explicit=False):
    """(config_dict, config_version, error_reason).

    Only an intentionally absent config (empty path or the default path
    missing because the operator never mounted one) yields ({}, None,
    None) uncalibrated. A missing EXPLICITLY configured path (env/CLI
    pointing at a file that is not there) returns
    "config_missing:<basename>" so the misconfiguration stays visible
    instead of silently erasing calibration. Malformed file or boundary
    violation -> ({}, None, reason) so callers emit error rows with
    scope identity instead of crashing or zero-filling.
    """
    if not path:
        return {}, None, None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = handle.read(4 * 1024 * 1024 + 1)
    except FileNotFoundError:
        if explicit:
            return {}, None, "config_missing:" + os.path.basename(path)
        return {}, None, None
    except UnicodeError:
        return {}, None, "malformed:analysis_config_encoding"
    except OSError as exc:
        return {}, None, "config_unreadable:" + type(exc).__name__
    if len(raw) > 4 * 1024 * 1024:
        return {}, None, "malformed:analysis_config_oversize"
    try:
        parsed = json.loads(raw)
    except ValueError:
        return {}, None, "malformed:analysis_config_json"
    if not isinstance(parsed, dict):
        return {}, None, "malformed:analysis_config_not_object"
    for key in parsed:
        if key not in CONFIG_SUFFIXES:
            return {}, None, "malformed:analysis_config_unknown_key"
    for key in CONFIG_SUFFIXES:
        if key in parsed and not isinstance(parsed[key], dict):
            return {}, None, "malformed:analysis_config_section_not_object"
    kept = {k: parsed[k] for k in CONFIG_SUFFIXES if k in parsed}
    canon = json.dumps(kept, sort_keys=True, separators=(",", ":"))
    version = hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]
    return kept, version, None


def build_windows(cutoff_ns, seal_ns, backfill_start_ns=None,
                  backfill_end_ns=None):
    """Hourly sealed windows, sorted and deduped. Pure (testable)."""
    if bc.to_ns(cutoff_ns) is None or bc.to_ns(seal_ns) is None \
            or seal_ns < cutoff_ns:
        raise BatteryConfigError("invalid window range")
    windows = []
    cursor = floor_hour_ns(cutoff_ns)
    # ponytail: hourly buckets; finer windows only if a module needs them.
    while cursor + HOUR_NS <= seal_ns + 1:
        windows.append((cursor, cursor + HOUR_NS - 1))
        cursor += HOUR_NS
        if len(windows) > 24 * 366:
            raise BatteryConfigError("window range exceeds one year")
    if (backfill_start_ns is not None) != (backfill_end_ns is not None):
        raise BatteryConfigError("backfill needs both START and END")
    end = None
    if backfill_start_ns is not None:
        if bc.to_ns(backfill_start_ns) is None \
                or bc.to_ns(backfill_end_ns) is None \
                or backfill_end_ns <= backfill_start_ns:
            raise BatteryConfigError("invalid backfill range")
        cursor = floor_hour_ns(backfill_start_ns)
        end = ceil_hour_ns(backfill_end_ns)
        while cursor < end:
            windows.append((cursor, cursor + HOUR_NS - 1))
            cursor += HOUR_NS
            if len(windows) > 24 * 366 * 2:
                raise BatteryConfigError("window range exceeds one year")
    ordered = sorted(set(windows))
    backfilling = backfill_start_ns is not None
    backfill_floor = floor_hour_ns(backfill_start_ns) if backfilling else None
    kept = []
    for ws, we in ordered:
        if ws >= seal_ns:
            continue
        if we >= cutoff_ns:
            kept.append((ws, we))
        elif backfilling and backfill_floor <= ws < end:
            kept.append((ws, we))
    return kept


def _named(key, fn):
    def _inner(signals, events, config):
        return fn(signals, events, config)
    _inner.__name__ = key
    return _inner


def get_analyzers():
    """[(key, suffix, analyze_fn|None)]. Missing files stay None (isolated)."""
    out = []
    for module_name, suffix in MODULE_FILES:
        try:
            module = __import__(module_name)
        except Exception:
            out.append((module_name, suffix, None))
            continue
        fn = getattr(module, "analyze", None)
        out.append((module_name, suffix,
                    fn if callable(fn) else None))
    return out


def disambiguate_rows(rows):
    """Fold episode_id into persisted analysis_id (revision untouched).

    Unknown-start rows already carry unique analysis_ids
    (battery_alerts:unknown:<hash16>) with episode_id None and pass
    through unchanged. Missing analysis_id falls back to metric.
    """
    for row in rows:
        if not isinstance(row, dict):
            continue
        aid = row.get("analysis_id")
        eid = row.get("episode_id")
        if isinstance(eid, str) and eid:
            if not isinstance(aid, str) or not aid:
                row["analysis_id"] = eid
            elif eid not in aid:
                row["analysis_id"] = aid + ":" + eid
        if not isinstance(row.get("analysis_id"), str) \
                or not row.get("analysis_id"):
            metric = row.get("metric")
            if isinstance(metric, str) and metric:
                row["analysis_id"] = metric
            else:
                row["analysis_id"] = "battery_unknown"
    return rows


def fill_missing_identity(rows, scope, window, computed_at_ns,
                          config_version=None):
    """Give run_analyses synthetic error rows scope/window identity.

    scope (vehicle, source, epoch), window (ws, we). Existing scoped rows
    keep their identity; scope-less unavailable rows adopt the requesting
    scope when known. Missing revision becomes a deterministic hash of
    (module, scope, window, reason); missing computed_at becomes the run
    timestamp. Never wall-clock revision.
    """
    vehicle, source, epoch = scope
    ws, we = window
    module_keys = {key for key, _ in MODULE_FILES}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("status") == "error" \
                and row.get("analysis_id") in module_keys \
                and row.get("metric") == row.get("analysis_id"):
            row["metric"] = error_metric_for(row["analysis_id"])
        if row.get("vehicle") is None and vehicle is not None:
            row["vehicle"] = vehicle
        if row.get("source") is None and source is not None:
            row["source"] = source
        if row.get("decode_epoch") is None and epoch is not None:
            row["decode_epoch"] = epoch
        if row.get("window_start_ns") is None and ws is not None:
            row["window_start_ns"] = ws
        if row.get("window_end_ns") is None and we is not None:
            row["window_end_ns"] = we
        if row.get("computed_at_ns") is None:
            row["computed_at_ns"] = computed_at_ns
        if config_version and not row.get("config_version"):
            row["config_version"] = config_version
        if not row.get("revision"):
            row["revision"] = bc.revision_id(
                row.get("analysis_id"), scope, window,
                row.get("reason"), RUNTIME_VERSION)
    return rows


def validate_no_duplicate_pks(rows):
    """Dedup exact copies; raise on conflicting rows sharing one PK.

    PK is (window_start_ns, vehicle, metric, source, analysis_id,
    revision): sharing it with differing payloads would let the last
    episode silently survive, so refuse instead.
    """
    seen = {}
    out = []
    for row in rows:
        key = (row.get("window_start_ns"), row.get("vehicle"),
               row.get("metric"), row.get("source"),
               row.get("analysis_id"), row.get("revision"))
        prev = seen.get(key)
        if prev is None:
            seen[key] = row
            out.append(row)
            continue
        if prev == row:
            continue
        raise BatteryDuplicateError(
            "duplicate analysis PK with differing payloads: metric=%s "
            "analysis_id=%s window_start=%s" % (
                row.get("metric"), row.get("analysis_id"),
                row.get("window_start_ns")))
    return out


def module_base(analysis_id):
    """Persisted analysis_id base: strip episode folding suffixes."""
    if isinstance(analysis_id, str) and ":" in analysis_id:
        return analysis_id.split(":")[0]
    return analysis_id


def error_metric_for(module_key):
    """Dashboard-visible failure indicator in the module namespace."""
    if isinstance(module_key, str) and module_key.startswith("battery_"):
        return "battery." + module_key[len("battery_"):] + ".error"
    return module_key if isinstance(module_key, str) and module_key \
        else "battery.dispatch.error"


def invalidate_previous(previous_identities, scope, window, failed_modules,
                        computed_at_ns, config_version=None, refreshed=None):
    """NULL error revisions over previous same-identity rows on failure.

    previous_identities: {(metric, analysis_id, window_start_ns,
    decode_epoch)} already stored (fetch_previous_identities). For each
    entry in this window whose module base is in failed_modules, emit one
    status=error value=NULL row with the SAME (metric, analysis_id,
    window_start, vehicle, source, decode_epoch) but a NEW deterministic
    revision, so the latest view (computed_at DESC) invalidates the stale
    success while history is preserved. Revision NEVER includes wall
    clock (reruns overwrite the same PK). Identities refreshed by this
    pass (fresh success written alongside) are excluded, so a same-pass
    tie on computed_at can never arbitrarily bury a fresh success.
    Other modules' successes and other epochs are untouched. A new
    metric= battery.<suffix>.error row is emitted alongside by the
    caller for dashboard visibility.
    """
    vehicle, source, epoch = scope
    ws, we = window
    refreshed = refreshed or set()
    out = []
    for metric, analysis_id, prev_ws, prev_epoch in sorted(
            previous_identities):
        if prev_ws != ws or prev_epoch != epoch:
            continue
        if module_base(analysis_id) not in failed_modules:
            continue
        if is_error_indicator(metric, analysis_id):
            continue  # indicators are refreshed by the pass itself;
            # only recover_indicators clears them after real recovery.
        if (metric, analysis_id, ws, epoch) in refreshed:
            continue
        reason = "invalidated:previous_success_stale_after_module_failure"
        out.append(bc.make_result(
            metric=metric, value=None, unit=None, status="error",
            reason=reason, window_start_ns=ws, window_end_ns=we,
            vehicle=vehicle, source=source, decode_epoch=epoch,
            computed_at_ns=computed_at_ns, config_version=config_version,
            analysis_id=analysis_id,
            revision=bc.revision_id(analysis_id, metric, scope, (ws, we),
                                    reason, config_version,
                                    RUNTIME_VERSION)))
    return out

def is_error_indicator(metric, analysis_id):
    """True for execution-indicator identities (never health data)."""
    if metric == "battery.dispatch.error":
        return True
    if not isinstance(metric, str) or not metric.endswith(".error"):
        return False
    if not isinstance(analysis_id, str) or not analysis_id:
        return False
    return metric == error_metric_for(module_base(analysis_id))


def recover_indicators(previous_identities, scope, window,
                       recovered_modules, computed_at_ns,
                       config_version=None, refreshed=None):
    """Clear stale execution indicators after a module recovers.

    For each previously stored error-indicator identity in this window
    whose module base recovered (i.e. it has a non-error row in this
    pass), write one explicit non-health recovery state
    (status=reported, value NULL, value_text=execution_recovered,
    reason=recovered:<prior indicator metric>) over the SAME identity
    with a NEW deterministic revision. The historical error row is
    preserved; only the latest view moves off the stale error. Health
    metrics are never touched here (only invalidate_previous covers
    those on failure).
    """
    vehicle, source, epoch = scope
    ws, we = window
    refreshed = refreshed or set()
    out = []
    for metric, analysis_id, prev_ws, prev_epoch in sorted(
            previous_identities):
        if prev_ws != ws or prev_epoch != epoch:
            continue
        if not is_error_indicator(metric, analysis_id):
            continue
        if module_base(analysis_id) not in recovered_modules:
            continue
        if (metric, analysis_id, ws, epoch) in refreshed:
            continue
        reason = "recovered:" + metric
        out.append(bc.make_result(
            metric=metric, value=None, unit=None, status="reported",
            value_text="execution_recovered",
            reason=reason, window_start_ns=ws, window_end_ns=we,
            vehicle=vehicle, source=source, decode_epoch=epoch,
            computed_at_ns=computed_at_ns, config_version=config_version,
            analysis_id=analysis_id,
            revision=bc.revision_id(analysis_id, metric, scope, (ws, we),
                                    reason, config_version,
                                    RUNTIME_VERSION)))
    return out


def fetch_previous_identities(base_url, auth, db, scope, windows,
                              max_rows):
    """Previous (metric, analysis_id) identities per scope/window/epoch.

    Returns {(metric, analysis_id, window_start_ns, decode_epoch)} for
    rows already stored for this scope in the requested windows. Missing
    table -> empty set (nothing to invalidate). Any other SQL error
    propagates (loud, never silent success).
    """
    if not windows:
        return set()
    starts = sorted({ws for ws, _ in windows})
    filt = " AND ".join(
        name + " IS NULL" if value is None
        else name + " = " + _sql_str(value)
        for name, value in zip(("vehicle", "source", "decode_epoch"), scope))
    literals = ", ".join("'" + ns_to_sql_ts(ws) + "'" for ws in starts)
    stmt = ('SELECT DISTINCT "metric", "analysis_id", "window_start", '
            '"decode_epoch" FROM "' + ANALYSIS_TABLE + '" WHERE ' + filt
            + " AND \"window_start\" IN (" + literals + ")")
    try:
        cols, rows, schema = fetch_raw(base_url, auth, db, stmt, max_rows)
    except BatteryError as exc:
        if is_missing_table(exc):
            return set()
        raise
    dtypes = {c.get("name"): c.get("data_type") for c in schema}
    idx = {c: i for i, c in enumerate(cols)}
    out = set()
    for record in rows:
        metric = record[idx["metric"]] if "metric" in idx else None
        analysis_id = record[idx["analysis_id"]] \
            if "analysis_id" in idx else None
        epoch_val = record[idx["decode_epoch"]] \
            if "decode_epoch" in idx else None
        ws = to_ns_cell(record[idx["window_start"]]
                        if "window_start" in idx else None,
                        dtypes.get("window_start"))
        if not isinstance(metric, str) or not metric \
                or not isinstance(analysis_id, str) or not analysis_id \
                or ws is None:
            continue
        out.add((metric, analysis_id, ws,
                 epoch_val if isinstance(epoch_val, str) else None))
    return out


SIGNAL_COLS = ["event_time", "vehicle", "path", "source", "decode_epoch",
               "value_num", "value_text", "value_bool", "unit", "quality",
               "envelope_id", "config_version", "connectivity",
               "ingest_time", "source_field"]
EVENT_COLS = ["event_time", "vehicle", "event_type", "name", "source",
              "event_id", "ingest_time", "envelope_id", "started_at",
              "ended_at", "audience", "is_active", "body_redacted",
              "source_system", "decode_epoch", "quality", "config_version",
              "connectivity"]


def _vehicle_filter(vehicle):
    if isinstance(vehicle, str) and vehicle:
        return " AND vehicle = " + _sql_str(vehicle)
    return ""


def fetch_signals(base_url, auth, db, start_ns, end_ns, vehicle, max_rows):
    filt = _vehicle_filter(vehicle)
    stmt = ("SELECT " + ", ".join(SIGNAL_COLS) + " FROM " + SIGNAL_TABLE
            + " WHERE event_time >= '" + ns_to_sql_ts(start_ns) + "'"
            + " AND event_time <= '" + ns_to_sql_ts(end_ns) + "'" + filt
            + " ORDER BY event_time")
    try:
        cols, rows, schema = fetch_raw(base_url, auth, db, stmt, max_rows)
    except BatteryError as exc:
        if is_missing_table(exc):
            return []
        raise
    dtypes = {c.get("name"): c.get("data_type") for c in schema}
    idx = {c: i for i, c in enumerate(cols)}
    out = []
    for record in rows:
        def _col(name):
            pos = idx.get(name)
            return record[pos] if pos is not None and pos < len(record) \
                else None
        event_ns = to_ns_cell(_col("event_time"),
                              dtypes.get("event_time"))
        if event_ns is None:
            continue
        vehicle_val = _col("vehicle")
        source_val = _col("source")
        epoch_val = _col("decode_epoch")
        if not all(isinstance(v, str) and v for v in
                   (vehicle_val, source_val, epoch_val)):
            continue
        boolean = _col("value_bool")
        boolean = boolean if isinstance(boolean, bool) else None
        text = _col("value_text")
        text = text if isinstance(text, str) and text else None
        out.append({
            "event_time_ns": event_ns,
            "ingest_time_ns": to_ns_cell(_col("ingest_time"),
                                         dtypes.get("ingest_time")),
            "vehicle": vehicle_val,
            "source": source_val,
            "decode_epoch": epoch_val,
            "path": _col("path") if isinstance(_col("path"), str)
            and _col("path") else None,
            "source_field": _col("source_field")
            if isinstance(_col("source_field"), str)
            and _col("source_field") else None,
            "value_num": bc.safe_float(_col("value_num")),
            "value_text": text,
            "value_bool": boolean,
            "unit": _col("unit") if isinstance(_col("unit"), str)
            and _col("unit") else None,
            "quality": _col("quality") if isinstance(_col("quality"), str)
            and _col("quality") else None,
            "envelope_id": _col("envelope_id")
            if isinstance(_col("envelope_id"), str)
            and _col("envelope_id") else None,
            "config_version": _col("config_version")
            if isinstance(_col("config_version"), str)
            and _col("config_version") else None,
            "connectivity": _col("connectivity")
            if isinstance(_col("connectivity"), str)
            and _col("connectivity") else None,
        })
    return out


def fetch_events(base_url, auth, db, start_ns, end_ns, vehicle, max_rows):
    filt = _vehicle_filter(vehicle)
    stmt = ("SELECT " + ", ".join(EVENT_COLS) + " FROM " + EVENT_TABLE
            + " WHERE event_time >= '" + ns_to_sql_ts(start_ns) + "'"
            + " AND event_time <= '" + ns_to_sql_ts(end_ns) + "'" + filt
            + " ORDER BY event_time")
    try:
        cols, rows, schema = fetch_raw(base_url, auth, db, stmt, max_rows)
    except BatteryError as exc:
        if is_missing_table(exc):
            return []
        raise
    dtypes = {c.get("name"): c.get("data_type") for c in schema}
    idx = {c: i for i, c in enumerate(cols)}
    out = []
    for record in rows:
        def _col(name):
            pos = idx.get(name)
            return record[pos] if pos is not None and pos < len(record) \
                else None
        event_ns = to_ns_cell(_col("event_time"),
                              dtypes.get("event_time"))
        if event_ns is None:
            continue
        vehicle_val = _col("vehicle")
        event_type = _col("event_type")
        name = _col("name")
        source_val = _col("source")
        if not all(isinstance(v, str) and v for v in
                   (vehicle_val, event_type, name, source_val)):
            continue
        active = _col("is_active")
        active = active if isinstance(active, bool) else None
        redacted = _col("body_redacted")
        redacted = redacted if isinstance(redacted, bool) else None
        epoch = _col("decode_epoch")
        epoch = epoch if isinstance(epoch, str) and epoch else None
        out.append({
            "event_time_ns": event_ns,
            "ingest_time_ns": to_ns_cell(_col("ingest_time"),
                                         dtypes.get("ingest_time")),
            "event_id": _col("event_id")
            if isinstance(_col("event_id"), str)
            and _col("event_id") else None,
            "vehicle": vehicle_val,
            "event_type": event_type,
            "name": name,
            "source": source_val,
            "envelope_id": _col("envelope_id")
            if isinstance(_col("envelope_id"), str)
            and _col("envelope_id") else None,
            "started_ns": to_ns_cell(_col("started_at"),
                                     dtypes.get("started_at")),
            "ended_ns": to_ns_cell(_col("ended_at"),
                                   dtypes.get("ended_at")),
            "audience": _col("audience")
            if isinstance(_col("audience"), str)
            and _col("audience") else None,
            "is_active": active,
            "body_redacted": redacted,
            "source_system": _col("source_system")
            if isinstance(_col("source_system"), str)
            and _col("source_system") else None,
            "decode_epoch": epoch,
            "quality": _col("quality") if isinstance(_col("quality"), str)
            and _col("quality") else None,
            "config_version": _col("config_version")
            if isinstance(_col("config_version"), str)
            and _col("config_version") else None,
            "connectivity": _col("connectivity")
            if isinstance(_col("connectivity"), str)
            and _col("connectivity") else None,
        })
    return out


def coverage_min(base_url, auth, db, table, vehicle, max_rows=5):
    filt = _vehicle_filter(vehicle)
    stmt = ("SELECT MIN(event_time) FROM " + table + " WHERE 1=1" + filt)
    try:
        cols, rows, schema = fetch_raw(base_url, auth, db, stmt, max_rows)
    except BatteryError as exc:
        if is_missing_table(exc):
            return None
        raise
    if not rows or not rows[0] or rows[0][0] is None:
        return None
    dtype = (schema[0].get("data_type") if schema else None)
    return to_ns_cell(rows[0][0], dtype)


ANALYSIS_COLS = ['"window_start"', '"vehicle"', '"metric"', '"source"',
                 '"analysis_id"', '"revision"', '"value"',
                 '"value_text"', '"unit"', '"status"', '"reason"',
                 '"window_end"', '"decode_epoch"', '"evidence_count"',
                 '"sample_count"', '"coverage_ratio"',
                 '"algorithm_version"', '"calibration_version"',
                 '"model_version"', '"uncertainty"',
                 '"uncertainty_lower"', '"uncertainty_upper"',
                 '"computed_at"', '"quality"', '"config_version"',
                 '"connectivity"', '"episode_id"']


def row_to_sql(row):
    return "(" + ", ".join([
        _sql_ts(row.get("window_start_ns")),
        _sql_str(row.get("vehicle")),
        _sql_str(row.get("metric")),
        _sql_str(row.get("source")),
        _sql_str(row.get("analysis_id")),
        _sql_str(row.get("revision")),
        _sql_num(row.get("value")),
        _sql_str(row.get("value_text")),
        _sql_str(row.get("unit")),
        _sql_str(row.get("status")),
        _sql_str(row.get("reason")),
        _sql_ts(row.get("window_end_ns")),
        _sql_str(row.get("decode_epoch")),
        _sql_num(row.get("evidence_count")),
        _sql_num(row.get("sample_count")),
        _sql_num(row.get("coverage_ratio")),
        _sql_str(row.get("algorithm_version")),
        _sql_str(row.get("calibration_version")),
        _sql_str(row.get("model_version")),
        _sql_num(row.get("uncertainty")),
        _sql_num(row.get("uncertainty_lower")),
        _sql_num(row.get("uncertainty_upper")),
        _sql_ts(row.get("computed_at_ns")),
        _sql_str(row.get("quality")),
        _sql_str(row.get("config_version")),
        _sql_str(row.get("connectivity")),
        _sql_str(row.get("episode_id")),
    ]) + ")"


def insert_analysis_rows(base_url, auth, db, rows, batch=500):
    writable = [r for r in rows
                if bc.to_ns(r.get("window_start_ns")) is not None
                and all(isinstance(r.get(k), str) and r.get(k) for k in
                        ("vehicle", "metric", "source", "analysis_id",
                         "revision"))]
    skipped = len(rows) - len(writable)
    deduped = validate_no_duplicate_pks(writable)
    for i in range(0, len(deduped), batch):
        chunk = deduped[i:i + batch]
        stmt = ('INSERT INTO "' + ANALYSIS_TABLE + '" ('
                + ", ".join(ANALYSIS_COLS) + ") VALUES "
                + ", ".join(row_to_sql(r) for r in chunk))
        request_sql(base_url, auth, db, stmt, timeout=120)
    return len(deduped), skipped


def run_battery(ctx, cfg, now_ns=None):
    """One battery pass. Returns rows written. Raises on cap/duplicate."""
    base_url, auth, db = ctx
    vehicle = cfg.get("vehicle") or ""
    try:
        lookback_h = int(cfg.get("battery_lookback_h",
                                 cfg.get("lookback_h", 30)))
        max_rows = int(cfg.get("battery_max_rows",
                               cfg.get("max_rows", 200000)))
    except (TypeError, ValueError):
        raise BatteryConfigError(
            "BATTERY_LOOKBACK_HOURS/BATTERY_MAX_ROWS must be integers")
    if min(lookback_h, max_rows) <= 0:
        raise BatteryConfigError("battery intervals and row cap positive")
    config_path = cfg.get("battery_config",
                          env("BATTERY_ANALYSIS_CONFIG",
                              CONFIG_PATH_DEFAULT))
    config_explicit = bool(cfg.get("battery_config_explicit",
                                   cfg.get("battery_config")
                                   or env("BATTERY_ANALYSIS_CONFIG", "")))
    analysis_cfg, config_version, config_error = load_analysis_config(
        config_path, explicit=config_explicit)
    backfill_start_raw = cfg.get("battery_backfill_start",
                                 env("BATTERY_BACKFILL_START", ""))
    backfill_end_raw = cfg.get("battery_backfill_end",
                               env("BATTERY_BACKFILL_END", ""))
    try:
        backfill_start = parse_time_bound(backfill_start_raw)
        backfill_end = parse_time_bound(backfill_end_raw)
    except BatteryConfigError as exc:
        raise BatteryConfigError("BATTERY_BACKFILL_START/END: " + str(exc))
    if (backfill_start is None) != (backfill_end is None):
        raise BatteryConfigError("BATTERY_BACKFILL_START/END need both")
    if backfill_start is not None and backfill_end <= backfill_start:
        raise BatteryConfigError("BATTERY_BACKFILL_END must exceed START")
    now = now_ns if bc.to_ns(now_ns) is not None else time.time_ns()
    computed_at = now
    seal = floor_hour_ns(now)
    cutoff = seal - lookback_h * HOUR_NS
    windows = build_windows(cutoff, seal, backfill_start, backfill_end)
    if not windows:
        return 0
    fetch_start = min(ws for ws, _ in windows)
    fetch_end = max(we for _, we in windows)
    # Pre-window signal context covering the energy analyzer's longest
    # credited offline gap (max_offline_gap_ns, 24 h) lets the earliest
    # lookback window anchor meter deltas and parked legs exactly like later
    # windows, so a sliding lookback never flips its first window's value.
    signals = fetch_signals(base_url, auth, db,
                            fetch_start - SIGNAL_CONTEXT_NS,
                            fetch_end, vehicle, max_rows)
    events = fetch_events(base_url, auth, db, fetch_start, fetch_end,
                          vehicle, max_rows)
    scopes = set()
    for raw in signals:
        scopes.add((raw["vehicle"], raw["source"], raw["decode_epoch"]))
    for raw in events:
        scopes.add((raw["vehicle"], raw["source"], raw.get("decode_epoch")))
    if not scopes and isinstance(vehicle, str) and vehicle:
        scopes.add((vehicle, "fleet", "unknown"))
    analyzers = get_analyzers()
    runnable = []
    missing = []
    for key, _suffix, fn in analyzers:
        if fn is None:
            missing.append(key)
        else:
            runnable.append((key, _named(key, fn)))
    previous = {}
    for scope in sorted(scopes, key=repr):
        previous[scope] = fetch_previous_identities(
            base_url, auth, db, scope, windows, max_rows)
    all_rows = []
    for scope in sorted(scopes, key=repr):
        sig_scope = bc.prepare_signals(
            [s for s in signals
             if (s["vehicle"], s["source"], s["decode_epoch"])
             == scope])
        ev_scope = [e for e in events
                    if (e["vehicle"], e["source"], e.get("decode_epoch"))
                    == scope]
        for ws, we in windows:
            window_cfg = dict(analysis_cfg)
            window_cfg["window_start_ns"] = ws
            window_cfg["window_end_ns"] = we
            failed_modules = set()
            recovered_modules = set()
            refreshed = set()
            if config_error is not None:
                # Malformed global config: every module failed, so every
                # module's previous identities for this window are stale.
                failed_modules = {key for key, _s, _f in analyzers}
                for key in sorted(failed_modules):
                    all_rows.append(bc.make_result(
                        metric=error_metric_for(key),
                        value=None, unit=None, status="error",
                        reason=config_error,
                        window_start_ns=ws, window_end_ns=we,
                        vehicle=scope[0], source=scope[1],
                        decode_epoch=scope[2],
                        computed_at_ns=computed_at,
                        config_version=config_version,
                        analysis_id=key,
                        revision=bc.revision_id(
                            key, scope, (ws, we), config_error,
                            RUNTIME_VERSION)))
                refreshed = {(r.get("metric"), r.get("analysis_id"), ws,
                              r.get("decode_epoch"))
                             for r in all_rows[-len(failed_modules):]
                             if isinstance(r, dict)}
            else:
                produced = []
                try:
                    produced = bc.run_analyses(
                        sig_scope, ev_scope, window_cfg,
                        [fn for _, fn in runnable])
                    recovered_modules.add("battery_dispatch")
                except Exception as exc:
                    # Whole-dispatch failure (never empty-success): every
                    # runnable module is affected, so all its previous
                    # identities for this window are stale.
                    failed_modules = {key for key, _ in runnable}
                    produced = [bc.make_result(
                        metric="battery.dispatch.error", value=None,
                        unit=None, status="error",
                        reason="execution_error:" + type(exc).__name__,
                        window_start_ns=ws, window_end_ns=we,
                        vehicle=scope[0], source=scope[1],
                        decode_epoch=scope[2],
                        computed_at_ns=computed_at,
                        config_version=config_version,
                        analysis_id="battery_dispatch",
                        revision=bc.revision_id(
                            "battery_dispatch", scope, (ws, we),
                            type(exc).__name__, RUNTIME_VERSION))]
                fill_missing_identity(produced, scope, (ws, we),
                                      computed_at, config_version)
                for key in missing:
                    failed_modules.add(key)
                    produced.append(bc.make_result(
                        metric=error_metric_for(key),
                        value=None, unit=None, status="error",
                        reason="missing_module:" + key,
                        window_start_ns=ws, window_end_ns=we,
                        vehicle=scope[0], source=scope[1],
                        decode_epoch=scope[2],
                        computed_at_ns=computed_at,
                        config_version=config_version,
                        analysis_id=key,
                        revision=bc.revision_id(key, scope, (ws, we),
                                                "missing_module",
                                                RUNTIME_VERSION)))
                per_module_failed = set()
                for row in produced:
                    if not isinstance(row, dict):
                        continue
                    if row.get("status") == "error":
                        aid = module_base(row.get("analysis_id"))
                        if isinstance(aid, str) and aid:
                            per_module_failed.add(aid)
                failed_modules |= per_module_failed
                produced_ok_modules = set()
                for row in produced:
                    if not isinstance(row, dict):
                        continue
                    if row.get("status") != "error":
                        aid = module_base(row.get("analysis_id"))
                        if isinstance(aid, str) and aid:
                            produced_ok_modules.add(aid)
                recovered_modules.update(
                    key for key, _s, _ in analyzers
                    if module_base(key) in produced_ok_modules
                    and module_base(key) not in failed_modules)
                disambiguate_rows(produced)
                # Re-fill after disambiguation in case episode folding
                # changed the persisted identity but revision stayed
                # untouched.
                for row in produced:
                    if row.get("computed_at_ns") is None:
                        row["computed_at_ns"] = computed_at
                    if config_version and not row.get("config_version"):
                        row["config_version"] = config_version
                all_rows.extend(produced)
                refreshed = {(r.get("metric"), r.get("analysis_id"), ws,
                              r.get("decode_epoch"))
                             for r in produced if isinstance(r, dict)}
            if failed_modules:
                all_rows.extend(invalidate_previous(
                    previous.get(scope, set()), scope, (ws, we),
                    failed_modules, computed_at, config_version,
                    refreshed=refreshed))
            if recovered_modules:
                all_rows.extend(recover_indicators(
                    previous.get(scope, set()), scope, (ws, we),
                    recovered_modules, computed_at, config_version,
                    refreshed=refreshed))
    if config_error is not None and not all_rows:
        raise BatteryConfigError(config_error)
    disambiguate_rows(all_rows)
    writable = []
    omitted = {}
    for row in all_rows:
        scope = (row.get("vehicle"), row.get("source"), row.get("decode_epoch"))
        identity = (row.get("metric"), row.get("analysis_id"),
                    row.get("window_start_ns"), row.get("decode_epoch"))
        # Absence is not a measurement. Keep NULL revisions only where
        # dropping one would leave a previously stored result authoritative.
        if (row.get("status") == "unavailable"
                and identity not in previous.get(scope, ())):
            reason = (row.get("reason") or "unspecified").split(":", 1)[0]
            omitted[reason] = omitted.get(reason, 0) + 1
        else:
            writable.append(row)
    checked = validate_no_duplicate_pks(writable)
    written, skipped = insert_analysis_rows(base_url, auth, db, checked)
    oldest_signal = coverage_min(base_url, auth, db, SIGNAL_TABLE,
                                 vehicle)
    oldest_event = coverage_min(base_url, auth, db, EVENT_TABLE, vehicle)
    oldest = None
    for cand in (oldest_signal, oldest_event):
        if cand is not None and (oldest is None or cand < oldest):
            oldest = cand
    if oldest is not None and oldest < fetch_start:
        need = "'%s' to '%s'" % (ns_to_sql_ts(oldest),
                                 ns_to_sql_ts(fetch_start - 1))
        sys.stdout.write(
            "battery: older data before fetch_start exists (oldest %s); "
            "supply BATTERY_BACKFILL_START/END covering %s to recompute; "
            "normal lookback left it untouched\n"
            % (ns_to_sql_ts(oldest), need))
    if skipped:
        sys.stdout.write("battery: skipped %d rows missing NOT NULL "
                         "identity (no vehicle/metric/revision)\n" % skipped)
    if omitted:
        sys.stdout.write("battery: unavailable_not_stored (%d rows; %s)\n"
                         % (sum(omitted.values()),
                            json.dumps(omitted, sort_keys=True)))
    error_rows = sum(1 for r in checked if r.get("status") == "error")
    if error_rows and error_rows >= len(checked):
        sys.stdout.write("battery: all_error (%d rows, %d scopes x %d "
                         "windows; latest view invalidated, no healthy "
                         "values)\n" % (written, len(scopes),
                                        len(windows)))
    elif error_rows:
        sys.stdout.write("battery: partial_errors (%d rows, %d error, "
                         "%d scopes x %d windows)\n"
                         % (written, error_rows, len(scopes),
                            len(windows)))
    else:
        sys.stdout.write("battery: ok (%d rows, %d scopes x %d windows)\n"
                         % (written, len(scopes), len(windows)))
    return written


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    overrides = {}
    battery_only = False
    i = 0
    while i < len(args):
        token = args[i]
        if token == "--battery-only":
            battery_only = True
        elif token.startswith("--config="):
            overrides["battery_config"] = token.split("=", 1)[1]
        elif token.startswith("--backfill-start="):
            overrides["battery_backfill_start"] = token.split("=", 1)[1]
        elif token.startswith("--backfill-end="):
            overrides["battery_backfill_end"] = token.split("=", 1)[1]
        elif token.startswith("--lookback-hours="):
            overrides["battery_lookback_h"] = token.split("=", 1)[1]
        elif token.startswith("--max-rows="):
            overrides["battery_max_rows"] = token.split("=", 1)[1]
        elif token.startswith("--vehicle="):
            overrides["vehicle"] = token.split("=", 1)[1]
        elif token.startswith("--base-url="):
            overrides["base_url"] = token.split("=", 1)[1]
        elif token.startswith("--db="):
            overrides["db"] = token.split("=", 1)[1]
        elif token in ("-h", "--help"):
            sys.stdout.write(
                "usage: battery_runtime.py [--battery-only] "
                "[--config=PATH] [--backfill-start=TS] [--backfill-end=TS] "
                "[--lookback-hours=N] [--max-rows=N] [--vehicle=ID] "
                "[--base-url=URL] [--db=NAME]\n"
                "TS is integer ns or UTC 'YYYY-MM-DD HH:MM:SS[.fffffffff]'.\n"
                "Reads GREPTIME_* env; writes vehicle_analysis only.\n")
            return 0
        else:
            sys.stderr.write("battery_runtime: unknown arg: " + token
                             + "\n")
            return 1
        i += 1
    _ = battery_only  # standalone CLI always runs battery only
    base_url = overrides.get("base_url",
                             env("GREPTIME_HTTP_URL",
                                 "http://greptimedb:4000"))
    db = overrides.get("db", env("GREPTIME_DB", "datalake"))
    user = env("GREPTIME_USER", "datalake")
    password = env("GREPTIME_PASSWORD", "")
    if not password:
        sys.stderr.write("battery_runtime: error: GREPTIME_PASSWORD "
                         "is required\n")
        return 1
    try:
        lookback_h = int(overrides.get("battery_lookback_h",
                                       env("BATTERY_LOOKBACK_HOURS", "")
                                       or env("AGG_LOOKBACK_HOURS", "30")))
        max_rows = int(overrides.get("battery_max_rows",
                                     env("BATTERY_MAX_ROWS", "")
                                     or env("AGG_MAX_ROWS", "200000")))
    except ValueError:
        sys.stderr.write("battery_runtime: error: lookback/max-rows "
                         "must be integers\n")
        return 1
    cfg = {
        "vehicle": overrides.get("vehicle", env("VEHICLE_ID", "")),
        "lookback_h": lookback_h,
        "max_rows": max_rows,
        "battery_lookback_h": lookback_h,
        "battery_max_rows": max_rows,
        "battery_config": overrides.get("battery_config",
                                        env("BATTERY_ANALYSIS_CONFIG",
                                            CONFIG_PATH_DEFAULT)),
        "battery_config_explicit": ("battery_config" in overrides
                                    or bool(env("BATTERY_ANALYSIS_CONFIG",
                                                ""))),
        "battery_backfill_start": overrides.get(
            "battery_backfill_start",
            env("BATTERY_BACKFILL_START", "")),
        "battery_backfill_end": overrides.get(
            "battery_backfill_end", env("BATTERY_BACKFILL_END", "")),
    }
    auth = base64.b64encode(
        (user + ":" + password).encode()).decode("ascii")
    ctx = (base_url, auth, db)
    deadline = time.monotonic() + 60
    while True:
        try:
            request_sql(base_url, auth, db, "SELECT 1", timeout=10)
            break
        except BatteryError as exc:
            if time.monotonic() >= deadline:
                sys.stderr.write("battery_runtime: error: db not ready: "
                                 + str(exc) + "\n")
                return 2
            time.sleep(3)
    try:
        run_battery(ctx, cfg)
    except (BatteryConfigError, BatteryCapError,
            BatteryDuplicateError) as exc:
        sys.stderr.write("battery_runtime: error: " + str(exc) + "\n")
        return 1
    except BatteryError as exc:
        sys.stderr.write("battery_runtime: error: " + str(exc) + "\n")
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
