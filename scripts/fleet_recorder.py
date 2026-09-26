#!/usr/bin/env python3
"""Tesla Fleet Telemetry ZMQ subscriber -> SQLite outbox -> Greptime HTTP SQL uploader.

Official Contract Compliance (crates/tesla-api/src/telemetry.rs):
  1. Two-frame ZMQ messages: frame[0] == topic (b"tesla_V"), frame[1] == protojson payload.
     Any other framing or arbitrary flat fallback payloads are strictly rejected.
  2. Protojson data[{key, value: {oneof}}] unwrapping:
     - floatValue, doubleValue, intValue, longValue (quoted integer), booleanValue,
       stringValue, shiftStateValue (e.g. "ShiftStateP" -> "P"),
       hvacAutoMode / hvacAutoModeValue ("HvacAutoModeStateOn" -> true).
     - kind == "invalid" is dropped.
  3. Original createdAt preserved to exact nanosecond precision (9 digits).
     Timestamps without an explicit timezone offset (Z or +/-HH:MM) or boolean types are rejected.
  4. Strict VIN & pseudonymization policy:
     - Non-empty VIN is required on every record (no unknown-vehicle fallback).
     - When TARGET_VIN is set, non-matching VINs are dropped to prevent multi-vehicle mixing.
     - When TARGET_VIN is not set, VEHICLE_ID_SALT is mandatory for deterministic SHA-256 hashing.
  5. Official unit handling & conversion:
     - VehicleSpeed (raw mph) converted to VSS km/h (* 1.609344).
     - Odometer / EstRange (raw miles) converted to VSS km (* 1.609344).
     - InsideTemp / OutsideTemp (already Celsius) verified and preserved.
     - Soc / BatteryLevel (%) verified and preserved.
     - Sparse unreceived fields are NEVER padded with 0 or false.
  6. Deterministic event_id (isResend excluded) for redelivery deduplication.
  7. Bounded SQLite outbox (PRAGMA WAL) guarded by MAX_OUTBOX_ROWS.
     Existing unacked rows are NEVER deleted on overflow; new arrivals are rejected
     with explicit counter increments to ensure remaining disk (2.5 GiB) safety.
  8. Seen table bounded by retention time and hard maximum row limit (MAX_SEEN_IDS).
  9. Greptime HTTP SQL batch INSERT with fail-closed ack verification (affectedrows == batch size).

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

# Strict ISO-8601 with mandatory timezone
ISO_STRICT_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})$"
)

SOURCE = "fleet"
SOURCE_SYSTEM = "tesla_fleet_telemetry"
DEFAULT_TOPIC = "tesla_V"
DEFAULT_ENDPOINT = "tcp://fleet-telemetry:5555"
MAX_SEEN_IDS = 50000
MPH_TO_KPH = 1.609344
MILES_TO_KM = 1.609344

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

# Field Allowlist with official Tesla telemetry field names, target VSS paths,
# conversion multipliers, expected units, and physical range validation limits.
FIELD_ALLOWLIST = {
    # Speed (Tesla sends raw mph; VSS requires km/h)
    "VehicleSpeed": {
        "path": "Vehicle.Speed",
        "unit": "km/h",
        "type": "num",
        "scale": MPH_TO_KPH,
        "min": 0.0,
        "max": 350.0,
    },
    # Odometer (Tesla sends raw miles; VSS requires km)
    "Odometer": {
        "path": "Vehicle.TraveledDistance",
        "unit": "km",
        "type": "num",
        "scale": MILES_TO_KM,
        "min": 0.0,
        "max": 2000000.0,
    },
    # Range (Tesla sends raw miles; VSS requires km)
    "EstRange": {
        "path": "Vehicle.Powertrain.TractionBattery.Range",
        "unit": "km",
        "type": "num",
        "scale": MILES_TO_KM,
        "min": 0.0,
        "max": 2000.0,
    },
    "IdealBatteryRange": {
        "path": "Vehicle.Powertrain.TractionBattery.Range",
        "unit": "km",
        "type": "num",
        "scale": MILES_TO_KM,
        "min": 0.0,
        "max": 2000.0,
    },
    # Battery & Powertrain (percentage / electrical units)
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
    # Temperatures (Tesla sends Celsius directly)
    "InsideTemp": {
        "path": "Vehicle.Cabin.AirTemperature",
        "unit": "celsius",
        "type": "num",
        "min": -60.0,
        "max": 80.0,
    },
    "OutsideTemp": {
        "path": "Vehicle.Exterior.AirTemperature",
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
    "ChargeAmps": {
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
    "ChargeEnergyAdded": {
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
    "FastChargerPresent": {
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
    "Longitude": {
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
    # Chassis / Controls
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
    "DoorOpen": {
        "path": "Vehicle.Cabin.Door.IsOpen",
        "unit": None,
        "type": "bool",
    },
    "HvacFanStatus": {
        "path": "Vehicle.Cabin.HVAC.FanSpeed",
        "unit": None,
        "type": "num",
        "min": 0.0,
        "max": 15.0,
    },
    "HvacACEnabled": {
        "path": "Vehicle.Cabin.HVAC.IsAirConditioningActive",
        "unit": None,
        "type": "bool",
    },
    "HvacAutoMode": {
        "path": "Vehicle.Cabin.HVAC.IsAutoActive",
        "unit": None,
        "type": "bool",
    },
    # Tire Pressures (bar)
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
    """Parse original createdAt with exact 9-digit nanosecond precision.

    Fail-closed requirements:
      - None, boolean, or empty string are rejected.
      - Timestamps without explicit timezone offset (Z or +/-HH:MM) are rejected.
      - Floating point NaN, Inf, non-positive numbers are rejected.
    """
    if val is None or isinstance(val, bool):
        raise ValueError("missing or boolean timestamp rejected")

    if isinstance(val, (int, float)):
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            raise ValueError(f"invalid float timestamp: {val}")
        if val <= 0:
            raise ValueError(f"invalid non-positive timestamp: {val}")
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

        m = ISO_STRICT_RE.match(val)
        if not m:
            raise ValueError(f"invalid ISO timestamp (strict timezone required): {val}")

        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        hour = int(m.group(4))
        minute = int(m.group(5))
        second = int(m.group(6))
        frac_str = m.group(7) or ""
        tz_str = m.group(8)

        # Exact 9-digit nanosecond fraction preservation
        frac_ns = int(frac_str.ljust(9, "0")[:9]) if frac_str else 0

        # Timezone offset resolution
        if tz_str in ("Z", "z"):
            offset_sec = 0
        else:
            tz_clean = tz_str.replace(":", "")
            sign = -1 if tz_clean[0] == "-" else 1
            tz_h = int(tz_clean[1:3])
            tz_m = int(tz_clean[3:5])
            offset_sec = sign * (tz_h * 3600 + tz_m * 60)

        dt_utc = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        epoch_seconds = int((dt_utc - _EPOCH).total_seconds()) - offset_sec
        total_ns = epoch_seconds * 1_000_000_000 + frac_ns
        if total_ns < _INT64_MIN or total_ns > _INT64_MAX:
            raise ValueError(f"timestamp out of range: {val}")
        return total_ns

    raise ValueError(f"unsupported timestamp type: {type(val).__name__}")


def resolve_vehicle_identity(vin, target_vin="", configured_id="", salt=""):
    """Resolve pseudonym vehicle ID adhering to strict anti-mixing contract.

    - VIN must be non-empty string.
    - If target_vin is configured: record must match target_vin, else returns None (drop).
    - If target_vin is not configured: salt is mandatory; creates stable hash pseudonym.
    - No unknown-vehicle or unsalted fallbacks.
    """
    if not vin or not isinstance(vin, str) or not vin.strip():
        raise ValueError("telemetry record missing or empty VIN")
    vin = vin.strip()

    if target_vin:
        if vin != target_vin:
            return None  # Multi-vehicle isolation: ignore other vehicles
        return configured_id if configured_id else f"v-{vin[:8]}"

    if not salt or not salt.strip():
        raise ValueError("VEHICLE_ID_SALT is required when TARGET_VIN is not set to prevent vehicle mixing")

    h = hashlib.sha256((salt.strip() + vin).encode("utf-8")).hexdigest()[:16]
    return f"v-{h}"


def unwrap_protojson_value(field, value_obj):
    """Unwrap protojson oneof field mapping per official telemetry contract.

    Matches crates/tesla-api/src/telemetry.rs:45-97:
      - floatValue, doubleValue -> float
      - intValue -> int
      - longValue -> int (protojson serializes 64-bit int as string)
      - booleanValue -> bool
      - stringValue -> str
      - shiftStateValue -> strip 'ShiftState' prefix (e.g. 'ShiftStateP' -> 'P')
      - hvacAutoMode / hvacAutoModeValue -> 'HvacAutoModeStateOn' -> true
      - kind == 'invalid' -> None (dropped)
    """
    if not isinstance(value_obj, dict) or not value_obj:
        return None

    kind, raw_val = next(iter(value_obj.items()))
    if kind == "invalid":
        return None

    # Normalization per telemetry.rs contract
    if field == "Gear" and kind == "shiftStateValue":
        if isinstance(raw_val, str):
            return (raw_val.removeprefix("ShiftState"), kind)
        return (str(raw_val), kind)

    if field == "HvacAutoMode" and kind in ("hvacAutoModeValue", "hvacAutoMode"):
        return (raw_val == "HvacAutoModeStateOn", kind)

    if kind in ("floatValue", "doubleValue"):
        try:
            return (float(raw_val), kind)
        except (ValueError, TypeError):
            return None

    if kind in ("intValue", "sintValue", "uintValue", "fixed32Value", "sfixed32Value"):
        try:
            return (int(raw_val), kind)
        except (ValueError, TypeError):
            return None

    if kind in ("longValue", "sint64Value", "uint64Value", "fixed64Value", "sfixed64Value"):
        # protojson represents 64-bit integers as strings or numbers
        try:
            return (int(raw_val), kind)
        except (ValueError, TypeError):
            return None

    if kind == "booleanValue":
        if isinstance(raw_val, bool):
            return (raw_val, kind)
        if isinstance(raw_val, (int, str)):
            s = str(raw_val).lower()
            if s in ("true", "1"):
                return (True, kind)
            if s in ("false", "0"):
                return (False, kind)
        return None

    if kind == "stringValue":
        return (str(raw_val), kind)

    return None


def validate_field_value(spec, unwrapped_value):
    """Validate and convert unwrapped value according to spec (scale + range).

    Returns (num, text, boolean) tuple, or None if invalid.
    """
    if unwrapped_value is None:
        return None

    ftype = spec["type"]
    if ftype == "num":
        try:
            val = float(unwrapped_value)
        except (ValueError, TypeError):
            return None
        if math.isnan(val) or math.isinf(val):
            return None

        # Unit scaling (e.g. mph -> km/h, miles -> km)
        scale = spec.get("scale")
        if scale:
            val = val * scale

        min_v = spec.get("min")
        max_v = spec.get("max")
        if min_v is not None and val < min_v:
            return None
        if max_v is not None and val > max_v:
            return None
        return (val, None, None)

    if ftype == "bool":
        if isinstance(unwrapped_value, bool):
            return (None, None, 1 if unwrapped_value else 0)
        if isinstance(unwrapped_value, (int, float)):
            if unwrapped_value in (0, 1):
                return (None, None, int(unwrapped_value))
            return None
        if isinstance(unwrapped_value, str):
            s = unwrapped_value.strip().lower()
            if s in ("true", "1"):
                return (None, None, 1)
            if s in ("false", "0"):
                return (None, None, 0)
        return None

    if ftype == "text":
        if isinstance(unwrapped_value, (dict, list, set, tuple)):
            return None
        return (None, str(unwrapped_value), None)

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


def parse_zmq_frames(frames, expected_topic=DEFAULT_TOPIC):
    """Validate 2-frame ZMQ message and return payload bytes.

    Strict contract: frame[0] must equal expected_topic (e.g. b"tesla_V").
    Frame[1] must contain the protojson payload.
    """
    if len(frames) != 2:
        raise ValueError(f"expected exactly 2 frames, got {len(frames)}")
    topic_bytes = frames[0]
    if topic_bytes != expected_topic.encode("utf-8"):
        raise ValueError(f"unexpected topic frame: {topic_bytes!r}, expected {expected_topic!r}")
    return frames[1]


def extract_protojson_records(payload_bytes, target_vin="", configured_vehicle_id="", salt=""):
    """Parse strict protojson payload into (metadata, signals_list).

    Strict official contract only:
      - vin: non-empty string
      - createdAt: strict ISO timestamp with timezone
      - isResend: boolean
      - data: list of {key: ..., value: {kind: ...}} objects
    Flat or fallback payloads are rejected.
    """
    try:
        data = json.loads(payload_bytes.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"malformed JSON: {e}") from e

    if not isinstance(data, dict):
        raise ValueError("top-level payload must be a JSON object")

    vin_raw = data.get("vin")
    vehicle = resolve_vehicle_identity(vin_raw, target_vin, configured_vehicle_id, salt)
    if vehicle is None:
        # Non-matching VIN under target_vin filter: skip silently
        return None, []

    created_at_raw = data.get("createdAt")
    event_time_ns = parse_created_at(created_at_raw)

    is_resend = bool(data.get("isResend", False))

    raw_data_list = data.get("data")
    if not isinstance(raw_data_list, list):
        raise ValueError("telemetry payload missing 'data' array")

    unwrapped_signals = []
    for datum in raw_data_list:
        if not isinstance(datum, dict):
            continue
        key = datum.get("key")
        val_obj = datum.get("value")
        if not key or not isinstance(key, str) or not isinstance(val_obj, dict):
            continue
        unwrapped = unwrap_protojson_value(key, val_obj)
        if unwrapped is not None:
            unwrapped_signals.append((key, unwrapped[0]))

    metadata = {
        "event_time_ns": event_time_ns,
        "vehicle": vehicle,
        "is_resend": is_resend,
        "vin": str(vin_raw).strip()
    }
    return metadata, unwrapped_signals


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
    """Insert one row into outbox with deduplication and strict disk bounding.

    Overflow policy (disk safety):
      - When count >= max_rows, NEW arrivals are rejected (return -1).
      - Existing unacked rows are NEVER deleted.
      - Rejected rows are NOT recorded in seen_ids, allowing retry upon drain.
    """
    key = row["event_id"]
    seen = conn.execute("SELECT 1 FROM seen_ids WHERE event_id=?", (key,)).fetchone()
    if seen is not None:
        return 0

    # Guard queue capacity
    cur = conn.execute("SELECT COUNT(*) FROM outbox")
    count = cur.fetchone()[0]
    if count >= max_rows:
        return -1  # Capacity overflow: reject new arrival to prevent disk filling

    cur = conn.execute(
        "INSERT OR IGNORE INTO outbox(" + ",".join(COLUMNS) + ")"
        " VALUES(" + ",".join("?" * len(COLUMNS)) + ")",
        [row[c] for c in COLUMNS])

    inserted = cur.rowcount > 0
    conn.execute("INSERT OR IGNORE INTO seen_ids(event_id, seen_at) VALUES(?,?)",
                 (key, now_ns()))
    conn.commit()
    return 1 if inserted else 0


def prune_seen(conn, older_than_ns, max_seen_limit=MAX_SEEN_IDS):
    """Bound seen_ids table by age and absolute row limit to prevent unbounded disk growth."""
    # 1. Prune entries older than retention window
    conn.execute("DELETE FROM seen_ids WHERE seen_at < ?", (older_than_ns,))
    # 2. Hard count bounding: retain at most max_seen_limit newest entries
    conn.execute(
        "DELETE FROM seen_ids WHERE rowid NOT IN ("
        " SELECT rowid FROM seen_ids ORDER BY seen_at DESC LIMIT ?"
        ")", (max_seen_limit,))
    conn.commit()


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
        metadata, raw_signals = extract_protojson_records(
            payload_bytes,
            target_vin=meta_env["target_vin"],
            configured_vehicle_id=meta_env["vehicle_id"],
            salt=meta_env["vehicle_salt"]
        )
    except Exception as ex:
        stats["invalid_messages"] += 1
        return 0

    if metadata is None:
        # Non-matching VIN filtered out
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
            continue  # Value invalid or out of range: fail-closed for this signal

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
            stats["outbox_overflow_drops"] += 1

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
            f"fleet_outbox_overflow_drops_total {self.stats.get('outbox_overflow_drops', 0)}",
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
    """ZMQ subscriber loop strictly enforcing 2-frame topic protocol."""
    import zmq
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt_string(zmq.SUBSCRIBE, topic)
    sock.RCVTIMEO = 1000  # 1 second poll timeout

    while not stop_event.is_set():
        try:
            parts = sock.recv_multipart()
            payload = parse_zmq_frames(parts, expected_topic=topic)
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
    endpoint = env("FLEET_ZMQ_ENDPOINT", DEFAULT_ENDPOINT)
    topic = env("FLEET_ZMQ_TOPIC", DEFAULT_TOPIC)
    metrics_port = int(env("FLEET_METRICS_PORT", "9105"))

    greptime_url = env("GREPTIME_HTTP_URL", "http://greptimedb:4000")
    greptime_db = env("GREPTIME_DB", "datalake")
    greptime_user = env("GREPTIME_USER", "datalake")
    greptime_pw = env("GREPTIME_PASSWORD", "")

    meta_env = {
        "target_vin": env("TARGET_VIN", ""),
        "vehicle_id": env("VEHICLE_ID", ""),
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
        "outbox_overflow_drops": 0,
    }

    stop_event = threading.Event()

    def sig_handler(sig, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    # Initialize main outbox schema
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

            # Periodic prune seen_ids every 5 minutes (keep 24h, max MAX_SEEN_IDS rows)
            now_mono = time.monotonic()
            if now_mono - last_prune > 300:
                retention_ns = now_ns() - 86400 * 1_000_000_000
                prune_seen(conn, retention_ns, max_seen_limit=MAX_SEEN_IDS)
                last_prune = now_mono

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

    sys.stdout.write(f"tesla-fleet-recorder started: zmq={endpoint} topic={topic} greptime={greptime_url}\n")
    sys.stdout.flush()

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
