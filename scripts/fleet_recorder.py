#!/usr/bin/env python3
"""Tesla Fleet Telemetry ZMQ subscriber -> SQLite outbox -> Greptime HTTP SQL uploader.

Official dispatch across four namespaced topics (FLEET_ZMQ_TOPICS, default all four):
  tesla_V -> vehicle_signal rows from protojson data[{key, value:{oneof}}].
  tesla_alerts -> vehicle_event rows (protos/vehicle_alert.proto envelope
    {vin, createdAt, alerts:[{name, audiences?, startedAt?, endedAt?}]}).
  tesla_errors -> vehicle_event rows (protos/vehicle_error.proto envelope
    {vin, createdAt, errors:[{createdAt?, name, tags?, body?}]}).
  tesla_connectivity -> vehicle_event rows (protos/vehicle_connectivity.proto
    {vin, createdAt, status, connection_id?, network_interface?}).
Any other framing or non-allowlisted topic is strictly rejected.

Signal contract (crates/tesla-api/src/telemetry.rs oneof mapping):
  floatValue/doubleValue/int*/long*/booleanValue/stringValue unwrapped;
  shiftStateValue prefix stripped; hvacAutoMode* On -> true.
  Undecodable, explicit-invalid, or non-finite samples are stored as tombstones
  (values NULL, quality 'invalid'), never zero-filled. Type-ok but out-of-range
  samples are tombstones with quality 'range_rejected'. Undocumented-unit
  battery fields are stored raw with quality 'unit_unverified', never rescaled.
  Sparse unreceived fields are NEVER padded. Original createdAt keeps exact
  nanosecond precision; timezone-less or boolean timestamps are rejected.

Event contract:
  Active alert iff endedAt absent; invalid endedAt yields quality 'error' with
  ended NULL and is_active NULL (parse failure never implies active). Missing
  or unparseable startedAt retains the warning with quality 'unknown_start'
  (no duration/episode). Conflicting ends (end < start) keep both stamps with
  no duration. Connectivity DISCONNECTED/CONNECTED rows stay separate and never
  close alert episodes. Body/tags/connection_id/network_interface are never
  stored raw (body presence recorded as body_redacted only; connection_id only
  as a hash in connectivity episode_id); raw VIN never stored.

Identity and provenance:
  Signal event_id hashes (vehicle, path, source, source_system, source_field,
  event_time_ns, decode_epoch, value); isResend/envelope excluded so redelivery
  dedups. Event event_id hashes (vehicle, event_type, name, source,
  source_system, started, ended, event_time, decode_epoch, audience, is_active).
  episode_id = sha256(vehicle|event_type|name|started_ns|decode_epoch), NULL
  without a valid start. Connectivity episode_id =
  sha256(vehicle|connectivity|connection_id|decode_epoch): one per vehicle
  socket, so concurrent wifi/cellular sockets are tracked independently.
  envelope_id = sha256(topic + payload) rides every row.
  CONFIG_VERSION provenance is operator-supplied only, never invented.

Durability:
  Bounded SQLite outbox (PRAGMA WAL); MAX_OUTBOX_ROWS bounds TOTAL pending
  signal + event rows. Overflow rejects new arrivals (never deletes unacked
  rows); rejected rows skip seen_ids so redelivery can land after drain.
  Seen table bounded by 24h prune and MAX_SEEN_IDS limit. Upload acks only on
  affectedrows == batch size, then deletes exactly the acked batch.

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
TOPIC_V = "tesla_V"
TOPIC_ALERTS = "tesla_alerts"
TOPIC_ERRORS = "tesla_errors"
TOPIC_CONNECTIVITY = "tesla_connectivity"
DEFAULT_TOPICS = (TOPIC_V, TOPIC_ALERTS, TOPIC_ERRORS, TOPIC_CONNECTIVITY)
EVENT_TOPICS = frozenset({TOPIC_ALERTS, TOPIC_ERRORS, TOPIC_CONNECTIVITY})
DEFAULT_ENDPOINT = "tcp://fleet-telemetry:5555"
MAX_SEEN_IDS = 50000
MPH_TO_KPH = 1.609344
MILES_TO_KM = 1.609344
BAR_TO_KPA = 100.0

COLUMNS = [
    "event_time", "vehicle", "path", "source", "event_id",
    "decode_epoch", "value_num", "value_text", "value_bool", "unit",
    "vss_version", "vehicle_firmware", "dbc_primary_commit",
    "dbc_supplemental_commit", "dbc_override_version",
    "dbc_override_commit", "mapping_revision", "collector_version",
    "ingest_time", "source_system", "source_field", "collector_id",
    "source_is_resend", "quality", "envelope_id", "config_version",
    "connectivity"
]

EVENT_COLUMNS = [
    "event_time", "vehicle", "event_type", "name", "source", "event_id",
    "ingest_time", "envelope_id", "started_at", "ended_at", "duration_s",
    "audience", "is_active", "body_redacted", "source_system",
    "decode_epoch", "collector_id", "episode_id", "quality",
    "config_version", "connectivity"
]

BOOL_COLUMNS = frozenset({"value_bool", "source_is_resend", "is_active",
                          "body_redacted"})

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
  source_is_resend INTEGER,
  quality TEXT,
  envelope_id TEXT,
  config_version TEXT,
  connectivity TEXT
)"""

EVENT_DDL = """CREATE TABLE IF NOT EXISTS outbox_events(
  event_id TEXT PRIMARY KEY,
  event_time INTEGER NOT NULL,
  vehicle TEXT NOT NULL,
  event_type TEXT NOT NULL,
  name TEXT NOT NULL,
  source TEXT NOT NULL,
  ingest_time INTEGER NOT NULL,
  envelope_id TEXT,
  started_at INTEGER,
  ended_at INTEGER,
  duration_s REAL,
  audience TEXT,
  is_active INTEGER,
  body_redacted INTEGER,
  source_system TEXT,
  decode_epoch TEXT,
  collector_id TEXT,
  episode_id TEXT,
  quality TEXT,
  config_version TEXT,
  connectivity TEXT
)"""

SEEN_DDL = """CREATE TABLE IF NOT EXISTS seen_ids(
  event_id TEXT PRIMARY KEY,
  seen_at INTEGER NOT NULL
)"""

OUTBOX_MIGRATIONS = (
    "ALTER TABLE outbox ADD COLUMN quality TEXT",
    "ALTER TABLE outbox ADD COLUMN envelope_id TEXT",
    "ALTER TABLE outbox ADD COLUMN config_version TEXT",
    "ALTER TABLE outbox ADD COLUMN connectivity TEXT",
)

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
    # Range fields: distinct canonical paths prevent collision/deduplication loss
    "EstRange": {
        "path": "Vehicle.Powertrain.TractionBattery.Range",
        "unit": "km",
        "type": "num",
        "scale": MILES_TO_KM,
        "min": 0.0,
        "max": 2000.0,
    },
    "IdealBatteryRange": {
        "path": "Vehicle.Powertrain.TractionBattery.IdealRange",
        "unit": "km",
        "type": "num",
        "scale": MILES_TO_KM,
        "min": 0.0,
        "max": 2000.0,
    },
    # Battery & Powertrain (distinct paths: physical Soc vs displayed BatteryLevel)
    "Soc": {
        "path": "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
        "unit": "%",
        "type": "num",
        "min": 0.0,
        "max": 100.0,
    },
    "BatteryLevel": {
        "path": "Vehicle.Powertrain.TractionBattery.StateOfCharge.Displayed",
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
        "unit": None,
        "type": "num",
        "unverified": True,
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
        "unit": None,
        "type": "num",
        "unverified": True,
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
    # DriverSeatOccupied: vendor-specific path to avoid guessing left/right seat VSS mapping
    "DriverSeatOccupied": {
        "path": "Vehicle.Tesla.DriverSeatOccupied",
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
    # Tire Pressures (Tesla sends bar; VSS requires kPa, 1 bar = 100 kPa)
    "TpmsPressureFl": {
        "path": "Vehicle.Chassis.Axle.Row1.Wheel.Left.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    "TirePressureFL": {
        "path": "Vehicle.Chassis.Axle.Row1.Wheel.Left.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    "TpmsPressureFr": {
        "path": "Vehicle.Chassis.Axle.Row1.Wheel.Right.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    "TirePressureFR": {
        "path": "Vehicle.Chassis.Axle.Row1.Wheel.Right.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    "TpmsPressureRl": {
        "path": "Vehicle.Chassis.Axle.Row2.Wheel.Left.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    "TirePressureRL": {
        "path": "Vehicle.Chassis.Axle.Row2.Wheel.Left.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    "TpmsPressureRr": {
        "path": "Vehicle.Chassis.Axle.Row2.Wheel.Right.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    "TirePressureRR": {
        "path": "Vehicle.Chassis.Axle.Row2.Wheel.Right.Tire.Pressure",
        "unit": "kPa",
        "type": "num",
        "scale": BAR_TO_KPA,
        "min": 0.0,
        "max": 500.0,
    },
    # Battery energy counters (authoritative kWh per available-data docs:
    # DCChargingEnergyIn = battery meter AC+DC, ACChargingEnergyIn = charger
    # meter AC only, EnergyRemaining = nominal pack kWh, LifetimeEnergyUsed =
    # discharge-lost kWh). No circular readings: stored verbatim for analysis.
    "DCChargingEnergyIn": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.DCEnergyIn",
        "unit": "kWh",
        "type": "num",
        "min": 0.0,
        "max": 500.0,
    },
    "ACChargingEnergyIn": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ACEnergyIn",
        "unit": "kWh",
        "type": "num",
        "min": 0.0,
        "max": 500.0,
    },
    "EnergyRemaining": {
        "path": "Vehicle.Powertrain.TractionBattery.EnergyRemaining",
        "unit": "kWh",
        "type": "num",
        "min": 0.0,
        "max": 500.0,
    },
    "LifetimeEnergyUsed": {
        "path": "Vehicle.Powertrain.TractionBattery.LifetimeEnergyUsed",
        "unit": "kWh",
        "type": "num",
        "min": 0.0,
        "max": 1000000.0,
    },
    # Brick voltages / module temps / isolation: docs give no authoritative
    # unit or scale, so stored raw (unit None) with quality 'unit_unverified'
    # for downstream calibration; never rescaled or range-clamped here.
    "BrickVoltageMin": {
        "path": "Vehicle.Powertrain.TractionBattery.BrickVoltageMin",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "BrickVoltageMax": {
        "path": "Vehicle.Powertrain.TractionBattery.BrickVoltageMax",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "NumBrickVoltageMin": {
        "path": "Vehicle.Powertrain.TractionBattery.NumBrickVoltageMin",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "NumBrickVoltageMax": {
        "path": "Vehicle.Powertrain.TractionBattery.NumBrickVoltageMax",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "ModuleTempMin": {
        "path": "Vehicle.Powertrain.TractionBattery.ModuleTempMin",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "ModuleTempMax": {
        "path": "Vehicle.Powertrain.TractionBattery.ModuleTempMax",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "NumModuleTempMin": {
        "path": "Vehicle.Powertrain.TractionBattery.NumModuleTempMin",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "NumModuleTempMax": {
        "path": "Vehicle.Powertrain.TractionBattery.NumModuleTempMax",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    "IsolationResistance": {
        "path": "Vehicle.Powertrain.TractionBattery.IsolationResistance",
        "unit": None,
        "type": "num",
        "unverified": True,
    },
    # BMS / charge state enums: stored as opaque text, never reinterpreted.
    "BMSState": {
        "path": "Vehicle.Powertrain.TractionBattery.BMSState",
        "unit": None,
        "type": "text",
    },
    "DetailedChargeState": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.DetailedState",
        "unit": None,
        "type": "text",
    },
    "BatteryHeaterOn": {
        "path": "Vehicle.Powertrain.TractionBattery.BatteryHeaterOn",
        "unit": None,
        "type": "bool",
    },
    # Official charge-limit SOC (% of capacity at which charging terminates).
    "ChargeLimitSoc": {
        "path": "Vehicle.Powertrain.TractionBattery.Charging.ChargeLimitSoc",
        "unit": "%",
        "type": "num",
        "min": 0.0,
        "max": 100.0,
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
    - If target_vin matches: returns configured_id if set, otherwise salted hash if salt is set.
      If neither configured_id nor salt is provided, fails closed (raises ValueError).
    - If target_vin is not configured: salt is mandatory; creates stable hash pseudonym.
    - No raw VIN exposure, no unsalted fallbacks, no unknown-vehicle fallbacks.
    """
    if not vin or not isinstance(vin, str) or not vin.strip():
        raise ValueError("telemetry record missing or empty VIN")
    vin = vin.strip()
    target_vin = target_vin.strip() if target_vin else ""
    configured_id = configured_id.strip() if configured_id else ""
    salt = salt.strip() if salt else ""

    if target_vin:
        if vin != target_vin:
            return None  # Multi-vehicle isolation: ignore other vehicles
        if configured_id:
            return configured_id
        if salt:
            h = hashlib.sha256((salt + vin).encode("utf-8")).hexdigest()[:16]
            return f"v-{h}"
        raise ValueError("Either VEHICLE_ID or VEHICLE_ID_SALT is required when TARGET_VIN is set")

    if not salt:
        raise ValueError("VEHICLE_ID_SALT is required when TARGET_VIN is not set to prevent vehicle mixing")

    h = hashlib.sha256((salt + vin).encode("utf-8")).hexdigest()[:16]
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
      - kind == 'invalid' -> None (tombstone downstream, never zero-filled)
      - other scalar kinds (enum states such as BMSStateValue) -> preserved
        verbatim so text/bool specs can store them without reinterpretation.
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

    if isinstance(raw_val, (str, bool, int, float)):
        return (raw_val, kind)

    return None


def validate_field_value(spec, unwrapped_value):
    """Validate and convert unwrapped value according to spec.

    Returns (num, text, boolean, quality): quality None means verified-valid;
    'invalid' means undecodable/non-finite (tombstone, values NULL);
    'range_rejected' means type-ok but outside min/max (tombstone);
    'unit_unverified' means spec has unverified=True (raw numeric kept, no
    scale or range applied). Enum-typed text/bool carry quality None.
    """
    ftype = spec["type"]
    if ftype == "num":
        val = None
        try:
            if isinstance(unwrapped_value, bool):
                raise ValueError("bool is not numeric")
            val = float(unwrapped_value)
        except (ValueError, TypeError):
            return (None, None, None, "invalid")
        if math.isnan(val) or math.isinf(val):
            return (None, None, None, "invalid")
        if spec.get("unverified"):
            # No scale, no clamp: raw value persists for downstream calibration.
            return (val, None, None, "unit_unverified")

        # Unit scaling (e.g. mph -> km/h, miles -> km, bar -> kPa)
        scale = spec.get("scale")
        if scale:
            val = val * scale

        min_v = spec.get("min")
        max_v = spec.get("max")
        if min_v is not None and val < min_v:
            return (None, None, None, "range_rejected")
        if max_v is not None and val > max_v:
            return (None, None, None, "range_rejected")
        return (val, None, None, None)

    if ftype == "bool":
        if isinstance(unwrapped_value, bool):
            return (None, None, 1 if unwrapped_value else 0, None)
        if isinstance(unwrapped_value, (int, float)):
            if unwrapped_value in (0, 1):
                return (None, None, int(unwrapped_value), None)
            return (None, None, None, "range_rejected")
        if isinstance(unwrapped_value, str):
            s = unwrapped_value.strip().lower()
            if s in ("true", "1"):
                return (None, None, 1, None)
            if s in ("false", "0"):
                return (None, None, 0, None)
        return (None, None, None, "invalid")

    if ftype == "text":
        if isinstance(unwrapped_value, (dict, list, set, tuple)):
            return (None, None, None, "invalid")
        if unwrapped_value is None or (isinstance(unwrapped_value, bool)):
            return (None, None, None, "invalid")
        return (None, str(unwrapped_value), None, None)

    return (None, None, None, "invalid")


def deterministic_event_id(vehicle, path, source_system, source_field,
                           event_time_ns, decode_epoch, num, text, boolean):
    """Stable id for one logical sample.

    Includes source_system and source_field so distinct fields arriving at
    the same timestamp/value retain independent identity without collision.
    isResend is intentionally EXCLUDED so redeliveries of the same field
    yield identical event_id, deduplicating cleanly.
    """
    parts = [
        vehicle, path, SOURCE, source_system, source_field,
        str(event_time_ns), str(decode_epoch),
        repr(num), "" if text is None else str(text),
        "" if boolean is None else str(boolean)
    ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def deterministic_event_row_id(vehicle, event_type, name, event_time_ns,
                               decode_epoch, started_ns, ended_ns,
                               audience, is_active):
    """Stable id for one observed alert/error/connectivity episode sample.

    Includes decode_epoch (re-decode keeps history); excludes isResend,
    envelope, body, connection, and config (metadata never moves identity).
    """
    parts = [
        vehicle, event_type, name, SOURCE, SOURCE_SYSTEM,
        "" if started_ns is None else str(started_ns),
        "" if ended_ns is None else str(ended_ns),
        str(event_time_ns), str(decode_epoch),
        "" if audience is None else audience,
        "" if is_active is None else ("1" if is_active else "0"),
    ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def episode_id(vehicle, event_type, name, started_ns, decode_epoch):
    """Pragmatic episode linkage; None without a valid start (no invention)."""
    if not vehicle or not event_type or not name or started_ns is None:
        return None
    try:
        started = int(started_ns)
    except (ValueError, TypeError):
        return None
    if started <= 0 or started > _INT64_MAX:
        return None
    tail = "|" + str(decode_epoch) if decode_epoch else ""
    return hashlib.sha256(
        f"{vehicle}|{event_type}|{name}|{started}{tail}".encode("utf-8")).hexdigest()


def connection_episode_id(vehicle, connection, decode_epoch):
    """Hash of one vehicle socket; None without an id (no invention)."""
    if not vehicle or not connection:
        return None
    tail = "|" + str(decode_epoch) if decode_epoch else ""
    return hashlib.sha256(
        f"{vehicle}|connectivity|{connection}{tail}".encode("utf-8")).hexdigest()


def _opt_nonempty_str(value):
    return value if isinstance(value, str) and value else None


def _parse_optional_time(raw):
    """Optional envelope/element time: (ns|None, status).

    Status True only when the raw value is absent (None/"") or a valid
    timestamp. A present-but-unparseable value returns False so callers can
    retain the warning with quality 'unknown_start'/'error' instead of
    treating it as valid, and never infer activity from the failure.
    """
    if raw is None or raw == "":
        return (None, True)
    try:
        return (parse_created_at(raw), True)
    except (ValueError, TypeError, OverflowError):
        return (None, False)


def _resolve_envelope_identity(data, target_vin, configured_vehicle_id, salt):
    vin_raw = data.get("vin")
    vehicle = resolve_vehicle_identity(vin_raw, target_vin, configured_vehicle_id, salt)
    if vehicle is None:
        return None, None
    return vehicle, str(vin_raw).strip()


def extract_alert_records(data, vehicle, event_time_ns):
    """Official VehicleAlerts envelope -> per-alert dicts (no invention).

    createdAt is the envelope event time. Each alerts[] element needs name;
    audiences join to one string or None; startedAt/endedAt parse optionally.
    No top-level name/status fallback and no events[] probing: non-envelope
    shapes raise ValueError.
    """
    alerts = data.get("alerts")
    if not isinstance(alerts, list):
        raise ValueError("alerts envelope missing 'alerts' array")
    out = []
    for elem in alerts:
        if not isinstance(elem, dict):
            continue
        name = elem.get("name")
        if not name or not isinstance(name, str):
            continue
        audiences = elem.get("audiences")
        audience = None
        if isinstance(audiences, list):
            parts = [a for a in audiences if isinstance(a, str) and a]
            audience = ",".join(parts) if parts else None
        elif isinstance(audiences, str) and audiences:
            audience = audiences
        started_ns, start_ok = _parse_optional_time(elem.get("startedAt"))
        ended_ns, end_ok = _parse_optional_time(elem.get("endedAt"))
        has_start_raw = elem.get("startedAt") not in (None, "")
        has_end_raw = elem.get("endedAt") not in (None, "")
        if ended_ns is not None and started_ns is not None and ended_ns >= started_ns:
            duration_s = (ended_ns - started_ns) / 1e9
        else:
            duration_s = None
        if ended_ns is not None:
            is_active = False
        elif has_end_raw and not end_ok:
            # Invalid endedAt: parse failure never implies active.
            is_active = None
        elif not has_start_raw or not start_ok:
            # Missing/unparseable start retains the warning without activity.
            is_active = None
        else:
            is_active = True
        if not has_start_raw or not start_ok:
            quality = "unknown_start"
        elif has_end_raw and not end_ok:
            quality = "error"
        else:
            quality = None
        out.append({
            "name": name, "audience": audience, "started_ns": started_ns,
            "ended_ns": ended_ns if end_ok else None, "duration_s": duration_s,
            "is_active": is_active, "quality": quality,
            "body_redacted": None, "connectivity": None,
            "event_time_ns": event_time_ns, "vehicle": vehicle,
        })
    return out


def extract_error_records(data, vehicle, event_time_ns):
    """Official VehicleErrors envelope -> per-error dicts (no invention).

    Each errors[] element needs name; per-element createdAt overrides the
    envelope time when valid; tags/body are never stored (body presence only).
    """
    errors = data.get("errors")
    if not isinstance(errors, list):
        raise ValueError("errors envelope missing 'errors' array")
    out = []
    for elem in errors:
        if not isinstance(elem, dict):
            continue
        name = elem.get("name")
        if not name or not isinstance(name, str):
            continue
        elem_time, elem_ok = _parse_optional_time(elem.get("createdAt"))
        row_time = elem_time if elem_ok and elem_time is not None else event_time_ns
        body = elem.get("body")
        out.append({
            "name": name, "audience": None, "started_ns": None,
            "ended_ns": None, "duration_s": None, "is_active": None,
            "quality": None if elem_ok else "error",
            "body_redacted": True if body not in (None, "") and "body" in elem else None,
            "connectivity": None, "event_time_ns": row_time,
            "vehicle": vehicle,
        })
    return out


def extract_connectivity_record(data, vehicle, event_time_ns):
    """Official VehicleConnectivity envelope -> single state row.

    status maps CONNECTED/DISCONNECTED/UNKNOWN (case-insensitive); anything
    else keeps the raw string as connectivity with quality 'error'. Rows stay
    separate and never close alert episodes downstream. The raw connection
    id is never stored; its hash becomes episode_id so a DISCONNECTED row
    closes only its own connection (a vehicle may hold wifi and cellular
    sockets at once).
    """
    raw_status = data.get("status")
    status = raw_status.strip().upper() if isinstance(raw_status, str) and raw_status.strip() else None
    if status in ("CONNECTED", "DISCONNECTED", "UNKNOWN"):
        connectivity, quality = status, None
    elif status is None:
        connectivity, quality = None, "error"
    else:
        connectivity, quality = raw_status, "error"
    connection = None
    for key in ("connectionId", "connection_id", "ConnectionID"):
        connection = _opt_nonempty_str(data.get(key))
        if connection:
            break
    return {
        "name": "connectivity", "audience": None, "started_ns": None,
        "ended_ns": None, "duration_s": None,
        "is_active": True if status == "CONNECTED" else (False if status == "DISCONNECTED" else None),
        "quality": quality, "body_redacted": None, "connectivity": connectivity,
        "event_time_ns": event_time_ns, "vehicle": vehicle,
        "connection": connection,
    }


def extract_event_envelope(topic, payload_bytes, target_vin="",
                           configured_vehicle_id="", salt=""):
    """Parse one alerts/errors/connectivity envelope.

    Returns (metadata, event_dicts) on event topics, else (None, None).
    VIN/target filter, exact-ns createdAt, and camelCase protojson field
    names follow the official protos; unknown shapes fail closed.
    """
    if topic not in EVENT_TOPICS:
        return None, None
    try:
        data = json.loads(payload_bytes.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"malformed JSON: {e}") from e
    if not isinstance(data, dict):
        raise ValueError("top-level payload must be a JSON object")
    vehicle, _vin = _resolve_envelope_identity(data, target_vin, configured_vehicle_id, salt)
    if vehicle is None:
        return None, []
    event_time_ns = parse_created_at(data.get("createdAt"))
    if topic == TOPIC_ALERTS:
        events = extract_alert_records(data, vehicle, event_time_ns)
        event_type = "alerts"
    elif topic == TOPIC_ERRORS:
        events = extract_error_records(data, vehicle, event_time_ns)
        event_type = "errors"
    else:
        events = [extract_connectivity_record(data, vehicle, event_time_ns)]
        event_type = "connectivity"
    metadata = {"event_time_ns": event_time_ns, "vehicle": vehicle, "event_type": event_type}
    return metadata, events


def resolve_topics(raw=""):
    """Parse FLEET_ZMQ_TOPICS allowlist; empty -> default full four.

    Unknown names are dropped; the clean cutover reads only FLEET_ZMQ_TOPICS.
    """
    names = [t.strip() for t in str(raw or "").split(",") if t.strip()]
    if not names:
        return list(DEFAULT_TOPICS)
    known = [t for t in names if t in DEFAULT_TOPICS]
    return known or list(DEFAULT_TOPICS)


def envelope_id(topic, payload_bytes):
    """Stable envelope over the exact received bytes (topic + payload)."""
    return hashlib.sha256(topic.encode("utf-8") + b"\x1f" + bytes(payload_bytes)).hexdigest()


def parse_zmq_frames(frames, topics=DEFAULT_TOPICS):
    """Validate 2-frame ZMQ message and return (topic, payload bytes).

    topics: allowlist (tuple/list/set); frame[0] must match one entry.
    A single string topic is accepted as a one-entry allowlist.
    """
    if isinstance(topics, str):
        topics = (topics,)
    allowed = set(topics) if topics else set(DEFAULT_TOPICS)
    if len(frames) != 2:
        raise ValueError(f"expected exactly 2 frames, got {len(frames)}")
    raw0 = frames[0]
    topic = raw0.decode("utf-8") if isinstance(raw0, (bytes, bytearray)) else raw0
    if topic not in allowed:
        raise ValueError(f"unexpected topic frame: {raw0!r}")
    return topic, frames[1]


def parse_zmq_payload(frames, topics=DEFAULT_TOPICS):
    """Legacy helper: return payload bytes of a 2-frame message."""
    _, payload = parse_zmq_frames(frames, topics)
    return payload


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
        if key not in FIELD_ALLOWLIST:
            continue  # Not allowlisted: ignore silently
        # Every allowlisted datum keeps a slot: undecodable/invalid -> raw
        # None tombstone (classified downstream as quality 'invalid').
        unwrapped = unwrap_protojson_value(key, val_obj)
        raw = unwrapped[0] if unwrapped is not None else None
        unwrapped_signals.append((key, raw))

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


def render_insert(table, rows, columns=None):
    """Render batch INSERT statement for GreptimeDB HTTP SQL."""
    if not TABLE_RE.fullmatch(table):
        raise ValueError(f"bad table name: {table}")
    cols = list(columns) if columns is not None else list(COLUMNS)
    for col in cols:
        if not TABLE_RE.fullmatch(col):
            raise ValueError(f"bad column name: {col}")
    cells = []
    for r in rows:
        cells.append(",".join(
            sql_bool(r.get(c)) if c in BOOL_COLUMNS
            else sql_escape(r.get(c))
            for c in cols))
    return f"INSERT INTO {table} ({','.join(cols)}) VALUES {', '.join(f'({c})' for c in cells)}"


def _existing_columns(conn, table):
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def outbox_pending_total(conn):
    """TOTAL pending rows across signal + event queues (single bound)."""
    total = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    try:
        total += conn.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0]
    except Exception:
        pass
    return total


def _store_row(conn, table, columns, row, max_rows):
    key = row["event_id"]
    seen = conn.execute("SELECT 1 FROM seen_ids WHERE event_id=?", (key,)).fetchone()
    if seen is not None:
        return 0
    if outbox_pending_total(conn) >= max_rows:
        return -1  # Capacity overflow: reject new arrival, keep unacked rows
    existing = _existing_columns(conn, table)
    write_cols = [c for c in columns if c in existing] if existing else list(columns)
    cur = conn.execute(
        "INSERT OR IGNORE INTO " + table + "(" + ",".join(write_cols) + ")"
        " VALUES(" + ",".join("?" * len(write_cols)) + ")",
        [row[c] for c in write_cols])
    inserted = cur.rowcount > 0
    conn.execute("INSERT OR IGNORE INTO seen_ids(event_id, seen_at) VALUES(?,?)",
                 (key, now_ns()))
    conn.commit()
    return 1 if inserted else 0


def open_outbox(path):
    """Open and initialize SQLite outbox database (migrates old rows)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute(DDL)
    conn.execute(EVENT_DDL)
    conn.execute(SEEN_DDL)
    existing = _existing_columns(conn, "outbox")
    for stmt in OUTBOX_MIGRATIONS:
        col = stmt.split()[-2]
        if col not in existing:
            conn.execute(stmt)
    conn.commit()
    return conn


def store_signal_update(conn, row, max_rows=50000):
    """Insert one signal row; overflow rejects new, keeps unacked, skips seen."""
    return _store_row(conn, "outbox", COLUMNS, row, max_rows)


def store_event_update(conn, row, max_rows=50000):
    """Insert one event row; same non-destructive TOTAL-bound semantics."""
    return _store_row(conn, "outbox_events", EVENT_COLUMNS, row, max_rows)


def prune_seen(conn, older_than_ns, max_seen_limit=MAX_SEEN_IDS):
    """Bound seen_ids table by age and absolute row limit to prevent unbounded disk growth."""
    conn.execute("DELETE FROM seen_ids WHERE seen_at < ?", (older_than_ns,))
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


def _stats_bump(stats, key, delta=1):
    try:
        stats[key] = stats.get(key, 0) + delta
    except Exception:
        pass


def _store_signal_row(conn, meta_env, stats, source_field, raw_val, metadata,
                      ingest_time_ns, envelope, max_rows):
    spec = FIELD_ALLOWLIST.get(source_field)
    if not spec:
        return 0  # Unreachable: extractor already filters non-allowlisted.
    num, text, boolean, quality = validate_field_value(spec, raw_val)
    if quality in ("invalid", "range_rejected"):
        _stats_bump(stats, "invalid_fields")
    path = spec["path"]
    event_id = deterministic_event_id(
        vehicle=metadata["vehicle"], path=path, source_system=SOURCE_SYSTEM,
        source_field=source_field, event_time_ns=metadata["event_time_ns"],
        decode_epoch=meta_env["decode_epoch"], num=num, text=text,
        boolean=boolean)
    row = {
        "event_time": metadata["event_time_ns"],
        "vehicle": metadata["vehicle"],
        "path": path,
        "source": SOURCE,
        "event_id": event_id,
        "decode_epoch": meta_env["decode_epoch"],
        "value_num": num,
        "value_text": text,
        "value_bool": boolean,
        "unit": spec.get("unit"),
        "vss_version": meta_env["vss_version"] or None,
        "vehicle_firmware": meta_env["vehicle_firmware"] or None,
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
        "source_is_resend": 1 if metadata.get("is_resend") else 0,
        "quality": quality,
        "envelope_id": envelope,
        "config_version": meta_env.get("config_version") or None,
        "connectivity": None,
    }
    res = store_signal_update(conn, row, max_rows=max_rows)
    if res == 1:
        _stats_bump(stats, "signals_stored")
        return 1
    if res == 0:
        _stats_bump(stats, "deduped")
    elif res == -1:
        _stats_bump(stats, "dropped_signals")
        _stats_bump(stats, "outbox_overflow_drops")
    return 0


def _store_event_row(conn, meta_env, stats, topic, event_type, item,
                     metadata, ingest_time_ns, envelope, max_rows):
    audience = _opt_nonempty_str(item.get("audience"))
    is_active = item.get("is_active")
    is_active = is_active if isinstance(is_active, bool) else None
    event_id = deterministic_event_row_id(
        vehicle=item["vehicle"], event_type=event_type, name=item["name"],
        event_time_ns=item["event_time_ns"],
        decode_epoch=meta_env["decode_epoch"],
        started_ns=item.get("started_ns"), ended_ns=item.get("ended_ns"),
        audience=audience, is_active=is_active)
    row = {
        "event_time": item["event_time_ns"],
        "vehicle": item["vehicle"],
        "event_type": event_type,
        "name": item["name"],
        "source": SOURCE,
        "event_id": event_id,
        "ingest_time": ingest_time_ns,
        "envelope_id": envelope,
        "started_at": item.get("started_ns"),
        "ended_at": item.get("ended_ns"),
        "duration_s": item.get("duration_s"),
        "audience": audience,
        "is_active": 1 if is_active is True else (0 if is_active is False else None),
        "body_redacted": 1 if item.get("body_redacted") is True else None,
        "source_system": SOURCE_SYSTEM,
        "decode_epoch": meta_env["decode_epoch"],
        "collector_id": meta_env["collector_id"],
        "episode_id": connection_episode_id(item["vehicle"], item.get("connection"),
                                            meta_env["decode_epoch"])
        if event_type == "connectivity" else
        episode_id(item["vehicle"], event_type, item["name"],
                   item.get("started_ns"), meta_env["decode_epoch"]),
        "quality": item.get("quality"),
        "config_version": meta_env.get("config_version") or None,
        "connectivity": _opt_nonempty_str(item.get("connectivity")),
    }
    res = store_event_update(conn, row, max_rows=max_rows)
    if res == 1:
        _stats_bump(stats, "events_stored")
        return 1
    if res == 0:
        _stats_bump(stats, "deduped")
    elif res == -1:
        _stats_bump(stats, "dropped_events")
        _stats_bump(stats, "event_overflow_drops")
        _stats_bump(stats, "dropped_signals")
        _stats_bump(stats, "outbox_overflow_drops")
    return 0


def process_message(conn, payload_bytes, meta_env, stats, max_rows=50000,
                    topic=TOPIC_V):
    """Parse one V payload and store allowlisted signals (with tombstones)."""
    try:
        metadata, raw_signals = extract_protojson_records(
            payload_bytes,
            target_vin=meta_env["target_vin"],
            configured_vehicle_id=meta_env["vehicle_id"],
            salt=meta_env["vehicle_salt"]
        )
    except Exception:
        _stats_bump(stats, "invalid_messages")
        return 0
    if metadata is None:
        return 0  # Non-matching VIN filtered out
    _stats_bump(stats, "messages_received")
    ingest_time_ns = now_ns()
    envelope = envelope_id(topic, payload_bytes)
    stored = 0
    for source_field, raw_val in raw_signals:
        stored += _store_signal_row(conn, meta_env, stats, source_field,
                                    raw_val, metadata, ingest_time_ns,
                                    envelope, max_rows)
    return stored


def process_envelope(conn, topic, payload_bytes, meta_env, stats,
                     max_rows=50000):
    """Parse one alerts/errors/connectivity envelope into event rows."""
    try:
        metadata, items = extract_event_envelope(
            topic, payload_bytes,
            target_vin=meta_env["target_vin"],
            configured_vehicle_id=meta_env["vehicle_id"],
            salt=meta_env["vehicle_salt"])
    except Exception:
        _stats_bump(stats, "invalid_events")
        return 0
    if metadata is None:
        return 0  # Non-matching VIN filtered out
    _stats_bump(stats, "messages_received")
    ingest_time_ns = now_ns()
    envelope = envelope_id(topic, payload_bytes)
    stored = 0
    for item in items:
        stored += _store_event_row(conn, meta_env, stats, topic,
                                   metadata["event_type"], item, metadata,
                                   ingest_time_ns, envelope, max_rows)
    return stored


def process_frame(conn, topic, payload_bytes, meta_env, stats,
                  max_rows=50000):
    """Dispatch one (topic, payload) pair to the signal or event path."""
    if topic in EVENT_TOPICS:
        return process_envelope(conn, topic, payload_bytes, meta_env, stats,
                                max_rows=max_rows)
    if topic == TOPIC_V:
        return process_message(conn, payload_bytes, meta_env, stats,
                               max_rows=max_rows, topic=topic)
    _stats_bump(stats, "invalid_messages")
    return 0


def _upload_table(conn, table, columns, dest_table, base_url, db, user,
                  password, batch_size):
    existing = _existing_columns(conn, table)
    if existing and table == "outbox":
        missing = [c for c in COLUMNS if c not in existing]
        if missing:
            return 0  # Old DB mid-migration: next open_outbox migrates.
    cur = conn.execute(
        "SELECT " + ",".join(columns) + " FROM " + table
        + " ORDER BY event_time ASC LIMIT ?",
        (batch_size,))
    rows = [dict(zip(columns, r)) for r in cur.fetchall()]
    if not rows:
        return 0
    sql = render_insert(dest_table, rows, columns=columns)
    affected = greptime_insert(base_url, db, user, password, sql, timeout=20)
    if affected != len(rows):
        raise SqlError(f"partial affected rows: expected {len(rows)}, got {affected}")
    event_ids = [r["event_id"] for r in rows]
    conn.execute(
        "DELETE FROM " + table + " WHERE event_id IN ("
        + ",".join("?" * len(event_ids)) + ")",
        event_ids)
    conn.commit()
    return len(rows)


def upload_tick(conn, base_url, db, user, password, batch_size=500, stats=None):
    """Upload one tick: signal batch then event batch (fail-closed each).

    Returns total rows; with stats, bumps uploaded (all rows) and
    events_uploaded (vehicle_event rows) as each table batch lands.
    """
    total = _upload_table(conn, "outbox", COLUMNS, "vehicle_signal",
                          base_url, db, user, password, batch_size)
    if stats is not None and total:
        _stats_bump(stats, "uploaded", total)
    if total < batch_size:
        events = _upload_table(conn, "outbox_events", EVENT_COLUMNS,
                               "vehicle_event", base_url, db, user,
                               password, batch_size - total)
        if stats is not None and events:
            _stats_bump(stats, "uploaded", events)
            _stats_bump(stats, "events_uploaded", events)
        total += events
    return total


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
        ev_pending = 0
        ev_oldest = 0
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            try:
                row = conn.execute("SELECT COUNT(*), MIN(event_time) FROM outbox").fetchone()
            except Exception:
                row = None
            if row:
                pending = row[0] or 0
                oldest_ns = row[1] or 0
            try:
                erow = conn.execute("SELECT COUNT(*), MIN(event_time) FROM outbox_events").fetchone()
            except Exception:
                erow = None
            if erow:
                ev_pending = erow[0] or 0
                ev_oldest = erow[1] or 0
            conn.close()
        except Exception:
            pass

        lines = [
            f"fleet_outbox_pending_rows {pending}",
            f"fleet_outbox_oldest_event_time_ns {oldest_ns}",
            f"fleet_outbox_events_pending_rows {ev_pending}",
            f"fleet_outbox_events_oldest_event_time_ns {ev_oldest}",
            f"fleet_messages_received_total {self.stats.get('messages_received', 0)}",
            f"fleet_signals_stored_total {self.stats.get('signals_stored', 0)}",
            f"fleet_events_stored_total {self.stats.get('events_stored', 0)}",
            f"fleet_uploaded_total {self.stats.get('uploaded', 0)}",
            f"fleet_events_uploaded_total {self.stats.get('events_uploaded', 0)}",
            f"fleet_upload_failures_total {self.stats.get('upload_failures', 0)}",
            f"fleet_deduped_total {self.stats.get('deduped', 0)}",
            f"fleet_invalid_messages_total {self.stats.get('invalid_messages', 0)}",
            f"fleet_invalid_fields_total {self.stats.get('invalid_fields', 0)}",
            f"fleet_invalid_events_total {self.stats.get('invalid_events', 0)}",
            f"fleet_dropped_signals_total {self.stats.get('dropped_signals', 0)}",
            f"fleet_dropped_events_total {self.stats.get('dropped_events', 0)}",
            f"fleet_outbox_overflow_drops_total {self.stats.get('outbox_overflow_drops', 0)}",
            f"fleet_event_overflow_drops_total {self.stats.get('event_overflow_drops', 0)}",
        ]
        body = "\n".join(lines).encode("utf-8") + b"\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def run_subscriber(endpoint, topics, on_frame, stop_event):
    """ZMQ subscriber loop over the allowlisted topics (strict 2-frame)."""
    import zmq
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(endpoint)
    for topic in topics:
        sock.setsockopt_string(zmq.SUBSCRIBE, topic)
    sock.RCVTIMEO = 1000  # 1 second poll timeout

    while not stop_event.is_set():
        try:
            parts = sock.recv_multipart()
            topic, payload = parse_zmq_frames(parts, topics)
            on_frame(topic, payload)
        except zmq.Again:
            continue
        except Exception as e:
            if stop_event.is_set():
                break
            sys.stderr.write(f"subscriber error: {e}\n")
            time.sleep(0.5)

    sock.close(linger=0)
    ctx.term()


def default_stats():
    return {
        "messages_received": 0,
        "signals_stored": 0,
        "events_stored": 0,
        "uploaded": 0,
        "events_uploaded": 0,
        "upload_failures": 0,
        "deduped": 0,
        "invalid_messages": 0,
        "invalid_fields": 0,
        "invalid_events": 0,
        "dropped_signals": 0,
        "dropped_events": 0,
        "outbox_overflow_drops": 0,
        "event_overflow_drops": 0,
    }


def run():
    outbox_path = env("OUTBOX_PATH", "/data/outbox.sqlite")
    max_outbox_rows = int(env("MAX_OUTBOX_ROWS", "50000"))
    batch_n = int(env("FLEET_BATCH_N", "500"))
    flush_sec = float(env("FLEET_FLUSH_SEC", "5"))
    endpoint = env("FLEET_ZMQ_ENDPOINT", DEFAULT_ENDPOINT)
    topics = resolve_topics(env("FLEET_ZMQ_TOPICS", ""))
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
        "vss_version": env("VSS_VERSION", ""),
        "vehicle_firmware": env("VEHICLE_FIRMWARE", ""),
        "mapping_revision": env("MAPPING_REVISION", "fleet-v1"),
        "collector_version": env("COLLECTOR_VERSION", "tesla-fleet-recorder-1"),
        "collector_id": env("COLLECTOR_ID", "fleet-collector-1"),
        "config_version": env("CONFIG_VERSION", ""),
    }

    stats = default_stats()

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
        def on_frame(topic, payload):
            process_frame(conn, topic, payload, meta_env, stats, max_rows=max_outbox_rows)
        try:
            run_subscriber(endpoint, topics, on_frame, stop_event)
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
                    n = upload_tick(conn, greptime_url, greptime_db, greptime_user, greptime_pw,
                                    batch_size=batch_n, stats=stats)
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

    sys.stdout.write(f"tesla-fleet-recorder started: zmq={endpoint} topics={','.join(topics)} greptime={greptime_url}\n")
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
            n = upload_tick(final_conn, greptime_url, greptime_db, greptime_user, greptime_pw,
                            batch_size=batch_n, stats=stats)
            if n < batch_n:
                break
        final_conn.close()
    except Exception as e:
        sys.stderr.write(f"final drain error: {e}\n")

    sys.stdout.write("tesla-fleet-recorder stopped cleanly.\n")


if __name__ == "__main__":
    run()
