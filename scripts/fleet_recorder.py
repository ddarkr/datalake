#!/usr/bin/env python3
"""Tesla Fleet Telemetry ZMQ subscriber -> SQLite outbox -> Greptime HTTP SQL uploader.

Pipeline:
  1. ZMQ SUB (5555) receives Tesla Fleet Telemetry JSON payloads (receive-only).
  2. Normalize payload, parse original createdAt with nanosecond precision,
     extract original isResend flag, resolve pseudonym vehicle ID.
  3. Validate fields against strict allowlist & unit ranges.
     Unknown timestamps or invalid fields fail closed.
     Sparse unreceived fields are never padded with 0/false.
  4. Generate deterministic event_id over (vehicle, path, source="fleet",
     event_time, decode_epoch, num, text, boolean). isResend is excluded
     so redeliveries produce the identical event_id and deduplicate.
  5. Store to bounded SQLite outbox (PRAGMA WAL) guarded by MAX_OUTBOX_ROWS.
  6. Timer-driven batch INSERT to GreptimeDB via HTTP SQL.
     Fail-closed ack policy: deletes only acked rows.
     DB down / restart / redelivery preserves queue and resumes cleanly.
  7. HTTP GET /metrics on 0.0.0.0:9105 for Prometheus scraping.

Tesla API / vehicle command transmission is strictly prohibited (receive-only).
"""

import base64
import hashlib
import http.server
import json
import math
import os
import re
import signal
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

_INT64_MIN = -(2 ** 63)
_INT64_MAX = 2 ** 63 - 1
_FLOAT_EXACT_INT = 2 ** 53

TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

SOURCE = "fleet"
SOURCE_SYSTEM = "tesla_fleet_telemetry"

COLUMNS = [
    "event_time", "vehicle", "path", "source", "event_id",
    "decode_epoch", "value_num", "value_text", "value_bool", "unit",
    "vss_version", "vehicle_firmware", "dbc_primary_commit",
    "dbc_supplemental_commit", "dbc_override_version",
    "dbc_override_commit", "mapping_revision", "collector_version",
    "ingest_time", "source_system", "source_field", "collector_id",
    "source_is_resend"
]

DDL = """CREATE TABLE IF NOT EXISTS outbox(
  event_id TEXT PRIMARY KEY,
  event_time INTEGER NOT NULL,
  vehicle TEXT NOT NULL,
  path TEXT NOT NULL,
  source TEXT NOT NULL,
  decode_epoch TEXT NOT NULL,
  value_num REAL,
  value_text TEXT,
  value_bool INTEGER,
  unit TEXT,
  vss_version TEXT,
  vehicle_firmware TEXT,
  dbc_primary_commit TEXT,
  dbc_supplemental_commit TEXT,
  dbc_override_version TEXT,
  dbc_override_commit TEXT,
  mapping_revision TEXT,
  collector_version TEXT,
  ingest_time INTEGER NOT NULL,
  source_system TEXT,
  source_field TEXT,
  collector_id TEXT,
  source_is_resend INTEGER
)"""

SEEN_DDL = """CREATE TABLE IF NOT EXISTS seen_ids(
  event_id TEXT PRIMARY KEY,
  seen_at INTEGER NOT NULL
)"""

# Field Allowlist with expected units and physical range limits
FIELD_ALLOWLIST = {
    # Speed & Motion
    "VehicleSpeed": {
        "path": "Vehicle.Speed",
        "unit": "km/h",
        "type": "num",
        "min": 0.0,
        "max": 350.0,
    },
    "speed": {
        "path": "Vehicle.Speed",
        "unit": "km/h",
        "type": "num",
        "min": 0.0,
        "max": 350.0,
    },
    # Battery & Powertrain
    "Soc": {
        "path": "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
        "unit": "%",
        "type": "num",
        "min": 0.0,
        "max": 100.0,
    },
    "BatteryLevel": {
        "path": "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
        "unit": "%",
        "type": "num",
        "min": 0.0,
        "max": 100.0,
    },
    "battery_level": {
        "path": "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
        "unit": "%",
        "type": "num",
        "min": 0.0,
        "max": 100.0,
    },
    "Odometer": {
        "path": "Vehicle.TraveledDistance",
        "unit": "km",
        "type": "num",
        "min": 0.0,
        "max": 2000000.0,
    },
    "odometer": {
        "path": "Vehicle.TraveledDistance",
        "unit": "km",
        "type": "num",
        "min": 0.0,
        "max": 2000000.0,
    },
    "EstRange": {
        "path": "Vehicle.Powertrain.TractionBattery.Range",
        "unit": "km",
        "type": "num",
        "min": 0.0,
        "max": 2000.0,
    },
    "IdealBatteryRange": {
        "path": "Vehicle.Powertrain.TractionBattery.Range",
        "unit": "km",
        "type": "num",
        "min": 0.0,
        "max": 2000.0,
    },
    "BatteryCurrent": {
        "path": "Vehicle.Powertrain.TractionBattery.Current",
        "unit": "A",
        "type": "num",
        "min": -1000.0,
        "max": 2500.0,
    },
    "PackCurrent": {
        "path": "Vehicle.Powertrain.TractionBattery.Current",
        "unit": "A",
        "type": "num",
        "min": -1000.0,
        "max": 2500.0,
    },
    "BatteryVoltage": {
        "path": "Vehicle.Powertrain.TractionBattery.Voltage",
        "unit": "V",
        "type": "num",
        "min": 0.0,
        "max": 1000.0,
    },
    "PackVoltage": {
        "path": "Vehicle.Powertrain.TractionBattery.Voltage",
        "unit": "V",
        "type": "num",
        "min": 0.0,
        "max": 1000.0,
    },
    "Gear": {
        "path": "Vehicle.Powertrain.Transmission.CurrentGear",
        "unit": None,
        "type": "text",
    },
    "gear": {
        "path": "Vehicle.Powertrain.Transmission.CurrentGear",
        "unit": None,
        "type": "text",
    },
    # Temperatures
    "OutsideTemp": {
        "path": "Vehicle.Exterior.AirTemperature",
        "unit": "celsius",
        "type": "num",
        "min": -60.0,
        "max": 80.0,
    },
    "outside_temp": {
        "path": "Vehicle.Exterior.AirTemperature",
        "unit": "celsius",
        "type": "num",
        "min": -60.0,
        "max": 80.0,
    },
    "InsideTemp": {
        "path": "Vehicle.Cabin.AirTemperature",
        "unit": "celsius",
        "type": "num",
        "min": -60.0,
        "max": 80.0,
    },
    "inside_temp": {
        "path": "Vehicle.Cabin.AirTemperature",
        "unit": "celsius",
        "type": "num",
        "min": -60.0,
        "max": 80.0,
    },
    # Charging
    "ChargerPower": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ChargePower",
        "unit": "kW",
        "type": "num",
        "min": 0.0,
        "max": 500.0,
    },
    "charger_power": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ChargePower",
        "unit": "kW",
        "type": "num",
        "min": 0.0,
        "max": 500.0,
    },
    "ChargeAmps": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ChargeCurrent",
        "unit": "A",
        "type": "num",
        "min": 0.0,
        "max": 1000.0,
    },
    "charge_amps": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ChargeCurrent",
        "unit": "A",
        "type": "num",
        "min": 0.0,
        "max": 1000.0,
    },
    "ChargeVoltage": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ChargeVoltage",
        "unit": "V",
        "type": "num",
        "min": 0.0,
        "max": 1000.0,
    },
    "charge_voltage": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ChargeVoltage",
        "unit": "V",
        "type": "num",
        "min": 0.0,
        "max": 1000.0,
    },
    "ChargeEnergyAdded": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.AccumulatedEnergy",
        "unit": "kWh",
        "type": "num",
        "min": 0.0,
        "max": 500.0,
    },
    "charge_energy_added": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.AccumulatedEnergy",
        "unit": "kWh",
        "type": "num",
        "min": 0.0,
        "max": 500.0,
    },
    "ChargingState": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.Status",
        "unit": None,
        "type": "text",
    },
    "charging_state": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.Status",
        "unit": None,
        "type": "text",
    },
    "FastChargerPresent": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.IsFastCharging",
        "unit": None,
        "type": "bool",
    },
    "fast_charger_present": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.IsFastCharging",
        "unit": None,
        "type": "bool",
    },
    # Location
    "Latitude": {
        "path": "Vehicle.CurrentLocation.Latitude",
        "unit": "degrees",
        "type": "num",
        "min": -90.0,
        "max": 90.0,
    },
    "latitude": {
        "path": "Vehicle.CurrentLocation.Latitude",
        "unit": "degrees",
        "type": "num",
        "min": -90.0,
        "max": 90.0,
    },
    "Longitude": {
        "path": "Vehicle.CurrentLocation.Longitude",
        "unit": "degrees",
        "type": "num",
        "min": -180.0,
        "max": 180.0,
    },
    "longitude": {
        "path": "Vehicle.CurrentLocation.Longitude",
        "unit": "degrees",
        "type": "num",
        "min": -180.0,
        "max": 180.0,
    },
    "Heading": {
        "path": "Vehicle.CurrentLocation.Heading",
        "unit": "degrees",
        "type": "num",
        "min": 0.0,
        "max": 360.0,
    },
    "heading": {
        "path": "Vehicle.CurrentLocation.Heading",
        "unit": "degrees",
        "type": "num",
        "min": 0.0,
        "max": 360.0,
    },
    # Pedals
    "BrakePedal": {
        "path": "Vehicle.Chassis.Brake.PedalPosition",
        "unit": "%",
        "type": "num",
        "min": 0.0,
        "max": 100.0,
    },
    "AcceleratorPedal": {
        "path": "Vehicle.Chassis.Accelerator.PedalPosition",
        "unit": "%",
        "type": "num",
        "min": 0.0,
        "max": 100.0,
    },
    # Doors
    "DoorOpen": {
        "path": "Vehicle.Cabin.Door.IsOpen",
        "unit": None,
        "type": "bool",
    },
    # Tires
    "TirePressureFL": {
        "path": "Vehicle.Chassis.Axle.Row1.Wheel.Left.Tire.Pressure",
        "unit": "bar",
        "type": "num",
        "min": 0.0,
        "max": 10.0,
    },
    "TirePressureFR": {
        "path": "Vehicle.Chassis.Axle.Row1.Wheel.Right.Tire.Pressure",
        "unit": "bar",
        "type": "num",
        "min": 0.0,
        "max": 10.0,
    },
    "TirePressureRL": {
        "path": "Vehicle.Chassis.Axle.Row2.Wheel.Left.Tire.Pressure",
        "unit": "bar",
        "type": "num",
        "min": 0.0,
        "max": 10.0,
    },
    "TirePressureRR": {
        "path": "Vehicle.Chassis.Axle.Row2.Wheel.Right.Tire.Pressure",
        "unit": "bar",
        "type": "num",
        "min": 0.0,
        "max": 10.0,
    },
}


def env(name, default=""):
    return os.environ.get(name, default)


def now_ns():
    return time.time_ns()


class SqlError(Exception):
    pass


def parse_created_at(val):
    """Parse original createdAt into nanoseconds since Unix epoch.

    Fail-closed policy: None, empty string, non-numeric unparseable strings,
    non-positive values, or values outside signed int64 range raise ValueError.
    Supports ISO-8601 strings and epoch timestamps (s, ms, us, ns).
    """
    if val is None or val == "":
        raise ValueError("missing timestamp")

    if isinstance(val, (int, float)):
        if val <= 0:
            raise ValueError(f"invalid non-positive timestamp: {val}")
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            raise ValueError(f"invalid float timestamp: {val}")
        if val < 1e11:  # seconds
            ns = int(val * 1_000_000_000)
        elif val < 1e14:  # milliseconds
            ns = int(val * 1_000_000)
        elif val < 1e17:  # microseconds
            ns = int(val * 1_000)
        else:  # nanoseconds
            ns = int(val)
        if ns < _INT64_MIN or ns > _INT64_MAX:
            raise ValueError(f"timestamp out of int64 range: {val}")
        return ns

    if isinstance(val, str):
        val = val.strip()
        if not val:
            raise ValueError("empty timestamp string")
        # Direct digit check for epoch timestamps formatted as string
        if val.isdigit():
            return parse_created_at(int(val))
        try:
            if "." in val:
                num = float(val)
                return parse_created_at(num)
        except ValueError:
            pass

        # Parse ISO-8601
        iso_str = val
        if iso_str.endswith("Z") or iso_str.endswith("z"):
            iso_str = iso_str[:-1] + "+00:00"
        try:
            dt_obj = datetime.fromisoformat(iso_str)
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            delta = dt_obj.astimezone(timezone.utc) - _EPOCH
            ns = ((delta.days * 86400 + delta.seconds) * 1_000_000_000
                  + delta.microseconds * 1000)
            if ns < _INT64_MIN or ns > _INT64_MAX:
                raise ValueError(f"timestamp out of range: {val}")
            return ns
        except Exception as ex:
            raise ValueError(f"unparseable ISO timestamp: {val}") from ex

    raise ValueError(f"unsupported timestamp type: {type(val).__name__}")


def resolve_pseudonym(configured_id, vin, salt=""):
    """Resolve pseudonym vehicle ID.

    If configured_id (e.g. VEHICLE_ID env) is present, use it.
    Else hash vin with salt to produce a stable pseudo-ID.
    """
    if configured_id:
        return configured_id
    if vin:
        h = hashlib.sha256((salt + str(vin)).encode("utf-8")).hexdigest()[:16]
        return f"v-{h}"
    return "unknown-vehicle"


def validate_field_value(spec, raw_value):
    """Validate raw value against field specification.

    Returns (num, text, boolean) tuple, or None if validation fails.
    Never pads missing or invalid values with 0/false.
    """
    if raw_value is None:
        return None

    ftype = spec["type"]
    if ftype == "num":
        try:
            val = float(raw_value)
        except (ValueError, TypeError):
            return None
        if math.isnan(val) or math.isinf(val):
            return None
        min_v = spec.get("min")
        max_v = spec.get("max")
        if min_v is not None and val < min_v:
            return None
        if max_v is not None and val > max_v:
            return None
        return (val, None, None)

    if ftype == "bool":
        if isinstance(raw_value, bool):
            return (None, None, 1 if raw_value else 0)
        if isinstance(raw_value, (int, float)):
            if raw_value in (0, 1):
                return (None, None, int(raw_value))
            return None
        if isinstance(raw_value, str):
            s = raw_value.strip().lower()
            if s in ("true", "1", "yes", "on"):
                return (None, None, 1)
            if s in ("false", "0", "no", "off"):
                return (None, None, 0)
            return None
        return None

    if ftype == "text":
        if isinstance(raw_value, (dict, list, set, tuple)):
            return None
        return (None, str(raw_value), None)

    return None


def deterministic_event_id(vehicle, path, event_time_ns, decode_epoch,
                           num, text, boolean):
    """Stable id for one logical sample.

    isResend is intentionally EXCLUDED so redeliveries of the same sample
    yield identical event_id, deduplicating both in outbox and GreptimeDB.
    """
    parts = [
        vehicle, path, SOURCE, str(event_time_ns), str(decode_epoch),
        repr(num), "" if text is None else str(text),
        "" if boolean is None else str(boolean)
    ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def extract_signals_and_metadata(payload_bytes, default_vehicle="", salt=""):
    """Parse raw incoming ZMQ message into (metadata, signals_list).

    metadata: {
        'event_time_ns': int,
        'vehicle': str,
        'is_resend': bool,
        'vin': str
    }
    signals_list: list of (source_field, raw_value)

    Fails closed on malformed JSON or unparseable timestamp.
    """
    try:
        data = json.loads(payload_bytes.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"malformed JSON: {e}") from e

    if not isinstance(data, dict):
        raise ValueError("top-level payload must be a JSON object")

    # Timestamp extraction (createdAt / timestamp / time)
    created_at_raw = (data.get("createdAt") or data.get("created_at") or
                      data.get("timestamp") or data.get("time"))
    event_time_ns = parse_created_at(created_at_raw)

    # Resend flag
    is_resend_raw = data.get("isResend", data.get("is_resend", False))
    is_resend = bool(is_resend_raw)

    # VIN / Vehicle ID
    vin = str(data.get("vin", data.get("vehicle_id", "")))
    vehicle = resolve_pseudonym(default_vehicle, vin, salt)

    # Collect signal candidates (sparse by design)
    raw_signals = []
    # 1. Check for nested list: data/signals/items
    nested = data.get("data") or data.get("signals") or data.get("items")
    if isinstance(nested, list):
        for item in nested:
            if isinstance(item, dict):
                # {key: "VehicleSpeed", value: 60} or {name: ..., value: ...}
                k = item.get("key") or item.get("name") or item.get("field")
                v = item.get("value")
                if k is not None and v is not None:
                    raw_signals.append((str(k), v))
    elif isinstance(nested, dict):
        for k, v in nested.items():
            if isinstance(v, dict) and "value" in v:
                raw_signals.append((str(k), v["value"]))
            elif not isinstance(v, (dict, list)):
                raw_signals.append((str(k), v))

    # 2. Check top-level keys
    for k, v in data.items():
        if k in ("createdAt", "created_at", "timestamp", "time", "isResend",
                 "is_resend", "vin", "vehicle_id", "data", "signals", "items", "txid", "txId"):
            continue
        if isinstance(v, dict) and "value" in v:
            raw_signals.append((str(k), v["value"]))
        elif not isinstance(v, (dict, list)):
            raw_signals.append((str(k), v))

    metadata = {
        "event_time_ns": event_time_ns,
        "vehicle": vehicle,
        "is_resend": is_resend,
        "vin": vin
    }
    return metadata, raw_signals


def sql_escape(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def sql_bool(value):
    if value is None:
        return "NULL"
    return "TRUE" if value else "FALSE"


def render_insert(table, rows):
    """Render batch INSERT statement for GreptimeDB HTTP SQL."""
    if not TABLE_RE.fullmatch(table):
        raise ValueError(f"bad table name: {table}")
    cells = []
    for r in rows:
        cells.append(",".join(
            sql_bool(r.get(c)) if c in ("value_bool", "source_is_resend")
            else sql_escape(r.get(c))
            for c in COLUMNS))
    return f"INSERT INTO {table} ({','.join(COLUMNS)}) VALUES {', '.join(f'({c})' for c in cells)}"


def open_outbox(path):
    """Open and initialize SQLite outbox database."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute(DDL)
    conn.execute(SEEN_DDL)
    conn.commit()
    return conn


def store_signal_update(conn, row, max_rows=50000):
    """Insert one row into outbox with deduplication and bounding.

    Returns:
      1 if a new row was inserted into outbox
      0 if deduped (already seen)
      -1 if dropped due to max_rows overflow (disk protection)
    """
    key = row["event_id"]
    seen = conn.execute("SELECT 1 FROM seen_ids WHERE event_id=?", (key,)).fetchone()
    if seen is not None:
        return 0

    # Check bounded queue capacity
    cur = conn.execute("SELECT COUNT(*) FROM outbox")
    count = cur.fetchone()[0]
    if count >= max_rows:
        # Bounded outbox overflow protection: drop oldest unacked row (FIFO)
        conn.execute("DELETE FROM outbox WHERE rowid IN (SELECT rowid FROM outbox ORDER BY event_time ASC LIMIT 1)")

    cur = conn.execute(
        "INSERT OR IGNORE INTO outbox(" + ",".join(COLUMNS) + ")"
        " VALUES(" + ",".join("?" * len(COLUMNS)) + ")",
        [row[c] for c in COLUMNS])

    inserted = cur.rowcount > 0
    conn.execute("INSERT OR IGNORE INTO seen_ids(event_id, seen_at) VALUES(?,?)",
                 (key, now_ns()))
    conn.commit()
    return 1 if inserted else 0


def prune_seen(conn, older_than_ns, limit=5000):
    """Prune seen_ids table to prevent unbounded disk growth."""
    cur = conn.execute(
        "DELETE FROM seen_ids WHERE rowid IN (SELECT rowid FROM seen_ids"
        " WHERE seen_at < ? LIMIT ?)", (older_than_ns, limit))
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def greptime_insert(base_url, db, user, password, sql, timeout=15):
    """Execute SQL statement against GreptimeDB HTTP SQL endpoint.

    Fail-closed: requires status 200, code == 0, and non-null affectedrows.
    """
    url = base_url.rstrip("/") + "/v1/sql?db=" + urllib.parse.quote(db, safe="")
    body = urllib.parse.urlencode({"sql": sql}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    auth = base64.b64encode((user + ":" + password).encode("utf-8")).decode("ascii")
    req.add_header("Authorization", "Basic " + auth)
    req.add_header("X-Greptime-Timezone", "UTC")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", "replace")
        except Exception:
            raw = ""
        raise SqlError(f"http status {e.code}: {raw[:300]}")
    except Exception as e:
        raise SqlError(f"request failed: {e}")

    try:
        payload = json.loads(raw)
    except Exception:
        raise SqlError(f"non-JSON response: {raw[:300]}")

    if not isinstance(payload, dict):
        raise SqlError(f"unexpected response type: {raw[:300]}")
    if payload.get("code", 0) != 0:
        raise SqlError(f"Greptime error code {payload.get('code')}: {payload.get('error', '')}")

    output = payload.get("output", [])
    if not output or not isinstance(output, list):
        raise SqlError("missing or invalid output list")
    first = output[0]
    if not isinstance(first, dict) or "affectedrows" not in first:
        raise SqlError(f"missing affectedrows in output: {raw[:300]}")
    return first["affectedrows"]


def process_message(conn, payload_bytes, meta_env, stats, max_rows=50000):
    """Parse payload and store allowlisted signals into outbox."""
    try:
        metadata, raw_signals = extract_signals_and_metadata(
            payload_bytes,
            default_vehicle=meta_env["vehicle"],
            salt=meta_env["vehicle_salt"]
        )
    except Exception as ex:
        stats["invalid_messages"] += 1
        return 0

    stats["messages_received"] += 1
    stored_count = 0
    ingest_time_ns = now_ns()

    for source_field, raw_val in raw_signals:
        spec = FIELD_ALLOWLIST.get(source_field)
        if not spec:
            continue  # Not allowlisted: ignore

        classified = validate_field_value(spec, raw_val)
        if classified is None:
            stats["invalid_fields"] += 1
            continue  # Value invalid: fail-closed for this signal

        num, text, boolean = classified
        path = spec["path"]
        unit = spec.get("unit")
        event_time_ns = metadata["event_time_ns"]
        vehicle = metadata["vehicle"]

        event_id = deterministic_event_id(
            vehicle=vehicle,
            path=path,
            event_time_ns=event_time_ns,
            decode_epoch=meta_env["decode_epoch"],
            num=num,
            text=text,
            boolean=boolean
        )

        row = {
            "event_time": event_time_ns,
            "vehicle": vehicle,
            "path": path,
            "source": SOURCE,
            "event_id": event_id,
            "decode_epoch": meta_env["decode_epoch"],
            "value_num": num,
            "value_text": text,
            "value_bool": boolean,
            "unit": unit,
            "vss_version": meta_env["vss_version"],
            "vehicle_firmware": meta_env["vehicle_firmware"],
            "dbc_primary_commit": None,
            "dbc_supplemental_commit": None,
            "dbc_override_version": None,
            "dbc_override_commit": None,
            "mapping_revision": meta_env["mapping_revision"],
            "collector_version": meta_env["collector_version"],
            "ingest_time": ingest_time_ns,
            "source_system": SOURCE_SYSTEM,
            "source_field": source_field,
            "collector_id": meta_env["collector_id"],
            "source_is_resend": 1 if metadata["is_resend"] else 0
        }

        res = store_signal_update(conn, row, max_rows=max_rows)
        if res == 1:
            stored_count += 1
            stats["signals_stored"] += 1
        elif res == 0:
            stats["deduped"] += 1
        elif res == -1:
            stats["dropped_signals"] += 1

    return stored_count


def upload_tick(conn, base_url, db, user, password, batch_size=500):
    """Execute one batch upload tick from outbox to GreptimeDB."""
    cur = conn.execute(
        "SELECT " + ",".join(COLUMNS) + " FROM outbox ORDER BY event_time ASC LIMIT ?",
        (batch_size,))
    rows = [dict(zip(COLUMNS, r)) for r in cur.fetchall()]
    if not rows:
        return 0

    sql = render_insert("vehicle_signal", rows)
    affected = greptime_insert(base_url, db, user, password, sql, timeout=20)
    if affected != len(rows):
        raise SqlError(f"partial affected rows: expected {len(rows)}, got {affected}")

    # Single-transaction delete of acked rows
    event_ids = [r["event_id"] for r in rows]
    conn.execute(
        "DELETE FROM outbox WHERE event_id IN (" + ",".join("?" * len(event_ids)) + ")",
        event_ids)
    conn.commit()
    return len(rows)


class MetricsHandler(http.server.BaseHTTPRequestHandler):
    stats = {}
    db_path = ""

    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        pending = 0
        oldest_ns = 0
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            row = conn.execute("SELECT COUNT(*), MIN(event_time) FROM outbox").fetchone()
            if row:
                pending = row[0] or 0
                oldest_ns = row[1] or 0
            conn.close()
        except Exception:
            pass

        lines = [
            f"fleet_outbox_pending_rows {pending}",
            f"fleet_outbox_oldest_event_time_ns {oldest_ns}",
            f"fleet_messages_received_total {self.stats.get('messages_received', 0)}",
            f"fleet_signals_stored_total {self.stats.get('signals_stored', 0)}",
            f"fleet_uploaded_total {self.stats.get('uploaded', 0)}",
            f"fleet_upload_failures_total {self.stats.get('upload_failures', 0)}",
            f"fleet_deduped_total {self.stats.get('deduped', 0)}",
            f"fleet_invalid_messages_total {self.stats.get('invalid_messages', 0)}",
            f"fleet_invalid_fields_total {self.stats.get('invalid_fields', 0)}",
            f"fleet_dropped_signals_total {self.stats.get('dropped_signals', 0)}",
        ]
        body = "\n".join(lines).encode("utf-8") + b"\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def run_subscriber(endpoint, topic, on_msg, stop_event):
    """ZMQ subscriber loop (lazy import zmq for stdlib testing isolation)."""
    import zmq
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt_string(zmq.SUBSCRIBE, topic)
    sock.RCVTIMEO = 1000  # 1 second timeout

    while not stop_event.is_set():
        try:
            parts = sock.recv_multipart()
            if not parts:
                continue
            payload = parts[-1]
            on_msg(payload)
        except zmq.Again:
            continue
        except Exception as e:
            if stop_event.is_set():
                break
            sys.stderr.write(f"subscriber error: {e}\n")
            time.sleep(0.5)

    sock.close(linger=0)
    ctx.term()


def run():
    outbox_path = env("OUTBOX_PATH", "/data/outbox.sqlite")
    max_outbox_rows = int(env("MAX_OUTBOX_ROWS", "50000"))
    batch_n = int(env("FLEET_BATCH_N", "500"))
    flush_sec = float(env("FLEET_FLUSH_SEC", "5"))
    endpoint = env("FLEET_ZMQ_ENDPOINT", "tcp://tesla-helper:5555")
    topic = env("FLEET_ZMQ_TOPIC", "")
    metrics_port = int(env("FLEET_METRICS_PORT", "9105"))

    greptime_url = env("GREPTIME_HTTP_URL", "http://greptimedb:4000")
    greptime_db = env("GREPTIME_DB", "datalake")
    greptime_user = env("GREPTIME_USER", "datalake")
    greptime_pw = env("GREPTIME_PASSWORD", "")

    meta_env = {
        "vehicle": env("VEHICLE_ID", ""),
        "vehicle_salt": env("VEHICLE_ID_SALT", ""),
        "decode_epoch": env("DECODE_EPOCH", "fleet-v1"),
        "vss_version": env("VSS_VERSION", "4.0"),
        "vehicle_firmware": env("VEHICLE_FIRMWARE", ""),
        "mapping_revision": env("MAPPING_REVISION", "fleet-v1"),
        "collector_version": env("COLLECTOR_VERSION", "tesla-fleet-recorder-1"),
        "collector_id": env("COLLECTOR_ID", "fleet-collector-1"),
    }

    stats = {
        "messages_received": 0,
        "signals_stored": 0,
        "uploaded": 0,
        "upload_failures": 0,
        "deduped": 0,
        "invalid_messages": 0,
        "invalid_fields": 0,
        "dropped_signals": 0,
    }

    stop_event = threading.Event()

    def sig_handler(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    # Initialize main outbox
    main_conn = open_outbox(outbox_path)
    main_conn.close()

    # Subscriber worker thread
    def sub_worker():
        conn = open_outbox(outbox_path)
        def on_msg(payload):
            process_message(conn, payload, meta_env, stats, max_rows=max_outbox_rows)
        try:
            run_subscriber(endpoint, topic, on_msg, stop_event)
        finally:
            conn.close()

    sub_thread = threading.Thread(target=sub_worker, name="sub_worker", daemon=True)
    sub_thread.start()

    # Uploader worker thread
    def upload_worker():
        conn = open_outbox(outbox_path)
        last_prune = time.monotonic()
        while not stop_event.is_set():
            try:
                # Catch-up drain loop
                while not stop_event.is_set():
                    n = upload_tick(conn, greptime_url, greptime_db, greptime_user, greptime_pw, batch_size=batch_n)
                    if n > 0:
                        stats["uploaded"] += n
                    if n < batch_n:
                        break
            except Exception as e:
                stats["upload_failures"] += 1
                sys.stderr.write(f"uploader tick error: {e}\n")

            # Periodic prune seen_ids every 10 minutes (keep 24 hours)
            now_mono = time.monotonic()
            if now_mono - last_prune > 600:
                retention_ns = now_ns() - 86400 * 1_000_000_000
                prune_seen(conn, retention_ns)
                last_prune = now_mono

            # Wait flush interval
            stop_event.wait(flush_sec)

        conn.close()

    up_thread = threading.Thread(target=upload_worker, name="upload_worker", daemon=True)
    up_thread.start()

    # Metrics server thread
    MetricsHandler.stats = stats
    MetricsHandler.db_path = outbox_path
    try:
        metrics_srv = http.server.HTTPServer(("0.0.0.0", metrics_port), MetricsHandler)
        metrics_thread = threading.Thread(target=metrics_srv.serve_forever, daemon=True)
        metrics_thread.start()
    except Exception as e:
        sys.stderr.write(f"metrics server failed on {metrics_port}: {e}\n")

    sys.stdout.write(f"tesla-fleet-recorder started: zmq={endpoint} greptime={greptime_url}\n")
    sys.stdout.flush()

    # Main thread blocks until signal
    while not stop_event.is_set():
        time.sleep(1)

    sys.stdout.write("tesla-fleet-recorder stopping, joining threads...\n")
    sub_thread.join(timeout=3)
    up_thread.join(timeout=3)

    # Final bounded drain
    sys.stdout.write("performing bounded final drain...\n")
    try:
        final_conn = open_outbox(outbox_path)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            n = upload_tick(final_conn, greptime_url, greptime_db, greptime_user, greptime_pw, batch_size=batch_n)
            if n > 0:
                stats["uploaded"] += n
            if n < batch_n:
                break
        final_conn.close()
    except Exception as e:
        sys.stderr.write(f"final drain error: {e}\n")

    sys.stdout.write("tesla-fleet-recorder stopped cleanly.\n")


if __name__ == "__main__":
    run()
