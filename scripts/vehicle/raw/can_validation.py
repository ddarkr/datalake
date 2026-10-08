#!/usr/bin/env python3
"""Observational CAN validation on sealed Raw windows (stdlib + cantools).

Reads the current immutable vehicle-data generation (manifest-pinned DBCs)
read-only, and counts known/unknown IDs, decode failures, DBC-range
violations, configured checksum failures and cross-signal divergence for one
sealed window. Observation only: never writes derived data, never transmits,
never blocks sealing. Any problem (missing/drifted DBCs, bad config, missing
cantools) raises; the recorder catches it, keeps capturing, and exposes
can_validation_configured 0 instead of healthy-looking zeros.

Config schema (all JSON in env, defaults empty = check absent, not zero):

CAN_VALIDATION_CHECKSUMS, object keyed by CAN ID (decimal or 0x-hex string):
  {"256": {"algorithm": "sum8|xor8|crc8", "checksum_byte": 7,
           "start": 0, "end": 7, "id_bytes": 0,
           "polynomial": 63, "init": 255, "xorout": 255}}
  IDs > 0x7FF are 29-bit: use an "ext:" prefix ("ext:0x1FFFFFFF").
  start/end cover data[start:end] (end exclusive, default whole frame); the

CAN_VALIDATION_RANGES, object keyed by DBC signal name (must exist in DBCs):
  {"PackVoltage": {"min": 200.0, "max": 450.0}}  (min or max may be omitted)

CAN_VALIDATION_EXPECTED_SIGNALS, list of DBC signal names expected every
  window: ["PackVoltage", "VehicleSpeed"]

CAN_VALIDATION_RELATIONS, object keyed by rule name (or list of rules each
  with a "name" field). Each rule names DBC signals that must exist:
  {"pack_power": {"result": "PackPower", "left": "PackVoltage",
                  "right": "PackCurrent", "op": "product", "factor": 1.0,
                  "abs_tolerance": 500.0, "rel_tolerance": 0.02,
                  "max_age_seconds": 1.0}}
  equal:   result == left * factor (right must be absent)
  product: result == left * right * factor (pack_power = voltage * current)
  sum:     result == (left + right) * factor (drive = front + rear)
  pass: |result - ref| <= abs_tolerance + rel_tolerance * max(|.|). Samples
  older than max_age_seconds apart are skipped (async buses), never failed.
"""

import hashlib
import json
import math
import os
import time

MASK29 = 0x1FFFFFFF


class ConfigError(ValueError):
    pass


class Unavailable(Exception):
    """DBCs/manifest/cantools missing or drifted: not-configured, not zero."""
    pass


def _json_env(env, name, default):
    raw = env.get(name, "")
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raw = default
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except ValueError as ex:
        raise ConfigError("%s is not valid JSON: %s" % (name, ex))


def _num(x, what):
    if isinstance(x, bool) or not isinstance(x, (int, float)) \
            or not math.isfinite(x):
        raise ConfigError("%s must be a finite number, got %r" % (what, x))
    return x


_CK_KEYS = {"algorithm", "checksum_byte", "start", "end", "id_bytes",
            "polynomial", "init", "xorout"}


def _parse_can_id(k):
    ext = False
    cid = k
    if isinstance(k, str):
        s = k.strip()
        if s[:4].lower() == "ext:":
            cid, ext = s[4:], True
        elif s[:4].lower() == "std:":
            cid, ext = s[4:], False
    try:
        cid = int(str(cid).strip(), 0)
    except (ValueError, TypeError):
        raise ConfigError("checksum key %r is not a CAN ID" % (k,))
    if not 0 <= cid <= 0x1FFFFFFF:
        raise ConfigError("checksum CAN ID %r out of range" % (k,))
    if not ext and cid > 0x7FF:
        raise ConfigError(
            "checksum CAN ID %r needs 29-bit range: use ext: prefix" % (k,))
    return cid, ext


def parse_checksums(raw):
    """Raw JSON value -> {(can_id, extended): rule}. Rejects ambiguity."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("checksums must be an object keyed by CAN ID")
    out = {}
    for k, v in raw.items():
        key = _parse_can_id(k)
        if key in out:
            raise ConfigError("duplicate checksum CAN ID %r" % (k,))
        if not isinstance(v, dict):
            raise ConfigError("checksum rule for ID %r must be an object" % (k,))
        extra = set(v) - _CK_KEYS
        if extra:
            raise ConfigError("unknown checksum option(s) %s for ID %r"
                              % (sorted(extra), k))
        algo = v.get("algorithm")
        if algo not in ("sum8", "xor8", "crc8"):
            raise ConfigError(
                "unsupported checksum algorithm %r for ID %r "
                "(want one of sum8/xor8/crc8)" % (algo, k))
        cb = v.get("checksum_byte")
        if isinstance(cb, bool) or not isinstance(cb, int) \
                or not 0 <= cb < 64:
            raise ConfigError(
                "checksum_byte 0..63 is required for ID %r" % (k,))
        start = v.get("start", 0)
        end = v.get("end", None)
        if isinstance(start, bool) or not isinstance(start, int) or start < 0:
            raise ConfigError("start must be an int >= 0 for ID %r" % (k,))
        if end is not None and (isinstance(end, bool)
                                or not isinstance(end, int) or end <= start):
            raise ConfigError("end must be an int > start for ID %r" % (k,))
        idb = v.get("id_bytes", 0)
        if idb not in (0, 2, 4):
            raise ConfigError("id_bytes must be 0, 2 or 4 for ID %r" % (k,))
        rule = {"algorithm": algo, "checksum_byte": cb, "start": start,
                "end": end, "id_bytes": idb}
        if algo == "crc8":
            for p in ("polynomial", "init", "xorout"):
                w = v.get(p)
                if isinstance(w, bool) or not isinstance(w, int) \
                        or not 0 <= w <= 255:
                    raise ConfigError(
                        "crc8 needs explicit %s 0..255 for ID %r" % (p, k))
                rule[p] = w
        out[key] = rule
    return out


def parse_ranges(raw, signals=None):
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("ranges must be an object keyed by signal name")
    out = {}
    for name, v in raw.items():
        if not isinstance(name, str) or not name:
            raise ConfigError("range key %r is not a signal name" % (name,))
        if signals is not None and name not in signals:
            raise ConfigError("range signal %r not in current DBCs" % (name,))
        if not isinstance(v, dict):
            raise ConfigError("range for %r must be an object" % (name,))
        extra = set(v) - {"min", "max"}
        if extra:
            raise ConfigError("unknown range option(s) %s for %r"
                              % (sorted(extra), name))
        lo = _num(v["min"], "range min for %r" % name) if "min" in v else None
        hi = _num(v["max"], "range max for %r" % name) if "max" in v else None
        if lo is None and hi is None:
            raise ConfigError("range for %r needs min and/or max" % (name,))
        if lo is not None and hi is not None and lo > hi:
            raise ConfigError("range min > max for %r" % (name,))
        out[name] = (lo, hi)
    return out


def parse_expected(raw, signals=None):
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ConfigError("expected signals must be a list")
    out = []
    for s in raw:
        if not isinstance(s, str) or not s:
            raise ConfigError("expected signal %r is not a name" % (s,))
        if signals is not None and s not in signals:
            raise ConfigError("expected signal %r not in current DBCs" % (s,))
        if s not in out:
            out.append(s)
    return out


_REL_KEYS = {"name", "result", "left", "right", "op", "factor",
             "abs_tolerance", "rel_tolerance", "max_age_seconds"}


def _one_relation(name, v, signals):
    if not isinstance(name, str) or not name:
        raise ConfigError("relation name %r is not a name" % (name,))
    if not isinstance(v, dict):
        raise ConfigError("relation %r must be an object" % (name,))
    extra = set(v) - _REL_KEYS
    if extra:
        raise ConfigError("unknown relation option(s) %s for %r"
                          % (sorted(extra), name))
    op = v.get("op")
    if op not in ("equal", "product", "sum"):
        raise ConfigError("relation %r needs op equal/product/sum" % (name,))
    res, left = v.get("result"), v.get("left")
    for field, val in (("result", res), ("left", left)):
        if not isinstance(val, str) or not val:
            raise ConfigError("relation %r needs %s signal" % (name, field))
    right = v.get("right", None)
    if op == "equal":
        if right is not None:
            raise ConfigError("relation %r: equal takes result+left only" % name)
    elif not isinstance(right, str) or not right:
        raise ConfigError("relation %r: op %s needs right signal" % (name, op))
    if signals is not None:
        for field, val in (("result", res), ("left", left),
                           ("right", right)):
            if val is not None and val not in signals:
                raise ConfigError("relation %r: %s signal %r not in DBCs"
                                  % (name, field, val))
    return {"result": res, "left": left, "right": right, "op": op,
            "factor": _num(v.get("factor", 1.0), "factor for %r" % name),
            "abs_tolerance": _num(v.get("abs_tolerance", 0.0),
                                  "abs_tolerance for %r" % name),
            "rel_tolerance": _num(v.get("rel_tolerance", 0.0),
                                  "rel_tolerance for %r" % name),
            "max_age_seconds": _num(v.get("max_age_seconds", 1.0),
                                    "max_age_seconds for %r" % name)}


def parse_relations(raw, signals=None):
    if raw is None:
        return {}
    if isinstance(raw, list):
        items = []
        for v in raw:
            if not isinstance(v, dict) or not v.get("name"):
                raise ConfigError("relation list entries need a name")
            items.append((v["name"], v))
    elif isinstance(raw, dict):
        items = list(raw.items())
    else:
        raise ConfigError("relations must be an object or a list")
    out = {}
    for name, v in items:
        if name in out:
            raise ConfigError("duplicate relation %r" % (name,))
        rule = _one_relation(name, v, signals)
        if rule["abs_tolerance"] < 0 or rule["rel_tolerance"] < 0:
            raise ConfigError("relation %r tolerances must be >= 0" % (name,))
        if rule["max_age_seconds"] <= 0:
            raise ConfigError("relation %r max_age_seconds must be > 0" % name)
        out[name] = rule
    return out


def load_db(data_dir):
    """Verify DBC bytes against the current manifest pin, then load both
    DBCs. Returns ((msgs_by_identity, signal_names), decode_epoch).
    Message identity is (can_id, is_extended_frame); a supplemental message
    whose identity already exists in primary is ignored (primary wins, no
    per-signal merge). Raises Unavailable on anything missing or drifted
    (never half-pinned)."""
    try:
        import cantools
    except ImportError:
        raise Unavailable("cantools not installed")
    real = os.path.realpath(data_dir)
    try:
        with open(os.path.join(real, "manifest.json"),
                  encoding="utf-8") as fh:
            man = json.load(fh)
    except (OSError, ValueError) as ex:
        raise Unavailable("no readable manifest in %s: %s" % (data_dir, ex))
    arts = man.get("artifacts")
    by_role = {}
    if isinstance(arts, list):
        for a in arts:
            if isinstance(a, dict) and a.get("role") and a.get("sha256"):
                by_role.setdefault(a["role"], a)
    epoch = man.get("decode_epoch") \
        or (man.get("inputs") or {}).get("decode_epoch")
    if not isinstance(epoch, str) or not epoch:
        raise Unavailable("manifest has no decode_epoch")
    want_primary = (by_role.get("primary") or {}).get("sha256")
    if not want_primary:
        raise Unavailable("manifest missing primary artifact hash")
    over = man.get("override") or {}
    over_sha = (by_role.get("override") or {}).get("sha256")
    want_second = over_sha if (over.get("applied") and over_sha) \
        else (by_role.get("supplemental") or {}).get("sha256")
    if not want_second:
        raise Unavailable("manifest missing supplemental/override hash")
    paths = {"primary": os.path.join(real, "dbc/primary.dbc"),
             "supplemental": os.path.join(real, "dbc/supplemental.dbc")}
    blobs = {}
    for label, want in (("primary", want_primary),
                        ("supplemental", want_second)):
        try:
            with open(paths[label], "rb") as fh:
                blobs[label] = fh.read()
        except OSError as ex:
            raise Unavailable("cannot read %s DBC: %s" % (label, ex))
        if hashlib.sha256(blobs[label]).hexdigest().lower() \
                != str(want).lower():
            raise Unavailable("%s DBC drifted from manifest pin" % label)
    try:
        # Parse the exact bytes just hashed (cantools' default DBC
        # encoding is cp1252, errors replaced, like load_file).
        dbs = [cantools.database.load_string(
            blobs[label].decode("cp1252", errors="replace"),
            database_format="dbc") for label in ("primary", "supplemental")]
    except Exception as ex:
        raise Unavailable("DBC load failed: %s" % ex)
    msgs = {}
    for db in dbs:
        for m in db.messages:
            key = (int(m.frame_id) & MASK29, bool(m.is_extended_frame))
            if key not in msgs:
                msgs[key] = m
    if not msgs:
        raise Unavailable("DBCs define no messages")
    sigs = set()
    for m in msgs.values():
        for s in m.signals:
            sigs.add(s.name)
    return (msgs, sigs), epoch



def _crc8(data, poly, init, xorout):
    crc = init & 0xFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = ((crc << 1) ^ poly) & 0xFF if crc & 0x80 \
                else (crc << 1) & 0xFF
    return (crc ^ xorout) & 0xFF


def _id_bytes(can_id, n):
    if n == 2:
        return bytes([(can_id >> 8) & 0xFF, can_id & 0xFF])
    if n == 4:
        return bytes([(can_id >> 24) & 0xFF, (can_id >> 16) & 0xFF,
                      (can_id >> 8) & 0xFF, can_id & 0xFF])
    return b""


def _checksum_ok(rule, can_id, data):
    n = len(data)
    cb = rule["checksum_byte"]
    if cb >= n:
        return False
    end = n if rule["end"] is None else min(rule["end"], n)
    start = min(rule["start"], n)
    covered = bytes(data[i] for i in range(start, end) if i != cb)
    blob = _id_bytes(can_id & MASK29, rule["id_bytes"]) + covered
    if rule["algorithm"] == "sum8":
        return (sum(blob) & 0xFF) == data[cb]
    if rule["algorithm"] == "xor8":
        x = 0
        for b in blob:
            x ^= b
        return x == data[cb]
    return _crc8(blob, rule["polynomial"], rule["init"],
                 rule["xorout"]) == data[cb]


def _rel_fail(rule, pts):
    r, l = pts[rule["result"]], pts[rule["left"]]
    vals = [r, l]
    if rule["right"] is not None:
        vals.append(pts[rule["right"]])
    if any(not math.isfinite(v) for v in vals):
        return False  # no reading, no verdict
    if rule["op"] == "equal":
        ref = l * rule["factor"]
    elif rule["op"] == "product":
        ref = l * pts[rule["right"]] * rule["factor"]
    else:
        ref = (l + pts[rule["right"]]) * rule["factor"]
    tol = rule["abs_tolerance"] + rule["rel_tolerance"] * max(abs(r), abs(ref))
    return abs(r - ref) > tol


def validate_window(frames, db, checksums, ranges, expected, relations):
    """Pure window validation. Error/remote frames count separately and never
    touch decode or checksum state (link errors are not app successes).
    Identity is (can_id, is_extended_frame). DBC bounds come from the
    decoded message's own signal metadata, never a global name lookup.
    Latest per-signal value uses integer twall_ns; only the age comparison
    converts to seconds."""
    msgs, _sigs = db
    known = unknown = error = remote = decode_errors = 0
    invalid_values = checksum_failures = 0
    known_ids, unknown_ids = set(), set()
    latest = {}  # name -> (twall_ns, value); finite values only
    for f in frames:
        if f.get("err"):
            error += 1
            continue
        if f.get("rtr"):
            remote += 1
            continue
        cid = int(f["id"]) & MASK29
        ext = bool(f.get("ext", False))
        data = bytes(f["data"])
        msg = msgs.get((cid, ext))
        if msg is None:
            unknown += 1
            unknown_ids.add((cid, ext))
            continue
        known += 1
        known_ids.add((cid, ext))
        try:
            vals = msg.decode(data, decode_choices=False, scaling=True)
        except Exception:
            decode_errors += 1
            continue
        by_name = {}
        for s in msg.signals:
            by_name.setdefault(s.name, s)
        rule = checksums.get((cid, ext))
        if rule is not None and not _checksum_ok(rule, cid, data):
            checksum_failures += 1
        t = f["twall_ns"]
        for name, val in vals.items():
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                continue
            fv = float(val)
            if not math.isfinite(fv):
                invalid_values += 1
                continue  # NaN/Inf is not a usable sample
            bad = False
            sig = by_name.get(name)
            if sig is not None:
                lo, hi = sig.minimum, sig.maximum
                if (lo is not None and fv < lo) \
                        or (hi is not None and fv > hi):
                    bad = True
            if name in ranges:
                lo, hi = ranges[name]
                if (lo is not None and fv < lo) \
                        or (hi is not None and fv > hi):
                    bad = True
            if bad:
                invalid_values += 1
            prev = latest.get(name)
            if prev is None or t >= prev[0]:
                latest[name] = (t, fv)
    distinct = known_ids | unknown_ids
    coverage = len(known_ids) / len(distinct) if distinct else None
    missing = len([s for s in expected if s not in latest]) \
        if expected else None
    cross = {}
    for rname, rule in relations.items():
        need = [rule["result"], rule["left"]] + (
            [rule["right"]] if rule["right"] is not None else [])
        if any(s not in latest for s in need):
            continue
        pts = {s: latest[s] for s in need}
        span_ns = max(p[0] for p in pts.values()) \
            - min(p[0] for p in pts.values())
        if span_ns / 1e9 > rule["max_age_seconds"]:
            continue  # async pair too stale to judge
        only = {s: pts[s][1] for s in need}
        if _rel_fail(rule, only):
            cross[rname] = 1
    return {"frames": {"known": known, "unknown": unknown, "error": error,
                       "remote": remote},
            "unknown_ids": len(unknown_ids), "coverage": coverage,
            "decode_errors": decode_errors, "invalid_values": invalid_values,
            "checksum_failures": checksum_failures, "missing": missing,
            "cross": cross}


def validate_sealed_window(frames, vehicle, data_dir, env=None):
    """Configs + pinned DBCs + window. Returns (epoch, result, flags).
    Raises ConfigError (bad calibration) or Unavailable (no DBC truth)."""
    env = os.environ if env is None else env
    checksums = parse_checksums(
        _json_env(env, "CAN_VALIDATION_CHECKSUMS", "{}"))
    ranges_raw = _json_env(env, "CAN_VALIDATION_RANGES", "{}")
    expected_raw = _json_env(env, "CAN_VALIDATION_EXPECTED_SIGNALS", "[]")
    relations_raw = _json_env(env, "CAN_VALIDATION_RELATIONS", "{}")
    (msgs, sigs), epoch = load_db(data_dir)
    ranges = parse_ranges(ranges_raw, sigs)
    expected = parse_expected(expected_raw, sigs)
    relations = parse_relations(relations_raw, sigs)
    res = validate_window(frames, (msgs, sigs), checksums, ranges, expected,
                          relations)
    res["success_ts"] = int(time.time())
    flags = {"has_checksums": bool(checksums),
             "has_expected": bool(expected), "rules": sorted(relations)}
    return epoch, res, flags


def _init(state):
    state.setdefault("seen", False)
    state.setdefault("ok", False)
    state.setdefault("vehicle", "")
    state.setdefault("epoch", "none")
    state.setdefault("successes", 0)
    state.setdefault("c", {"known": 0, "unknown": 0, "error": 0, "remote": 0,
                           "decode_errors": 0, "invalid_values": 0,
                           "checksum_failures": 0})
    state.setdefault("g", {"unknown_ids": 0, "coverage": None,
                           "missing": None, "success_ts": None})
    state.setdefault("cross", {})
    state.setdefault("has_checksums", False)
    state.setdefault("has_expected", False)
    state.setdefault("rules", [])


def apply_success(state, vehicle, epoch, res, flags):
    """Fold one validated window in: counters accumulate, gauges track the
    last sealed window. A vehicle/epoch change resets all counters first,
    so old-epoch counts never move under a new label."""
    _init(state)
    if state.get("successes") and (state.get("vehicle") != vehicle
                                   or state.get("epoch") != epoch):
        state["successes"] = 0
        state["c"] = {"known": 0, "unknown": 0, "error": 0, "remote": 0,
                      "decode_errors": 0, "invalid_values": 0,
                      "checksum_failures": 0}
        state["g"] = {"unknown_ids": 0, "coverage": None, "missing": None,
                      "success_ts": None}
        state["cross"] = {}
    state.update(seen=True, ok=True, vehicle=vehicle, epoch=epoch,
                 has_checksums=flags["has_checksums"],
                 has_expected=flags["has_expected"], rules=flags["rules"])
    state["successes"] += 1
    c = state["c"]
    for k, v in res["frames"].items():
        c[k] += v
    c["decode_errors"] += res["decode_errors"]
    c["invalid_values"] += res["invalid_values"]
    c["checksum_failures"] += res["checksum_failures"]
    state["g"].update(unknown_ids=res["unknown_ids"],
                      coverage=res["coverage"], missing=res["missing"],
                      success_ts=res["success_ts"])
    for r in flags["rules"]:
        state["cross"][r] = state["cross"].get(r, 0) + res["cross"].get(r, 0)


def apply_failure(state, vehicle):
    """No DBC truth or bad calibration: not-configured, never healthy zero."""
    _init(state)
    state.update(seen=True, ok=False, vehicle=vehicle)


def _esc(s):
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


def render(state):
    """Prometheus exposition for the can_validation_ series. Counters appear
    only after the first successful window; unconfigured checks stay absent."""
    if not state.get("seen"):
        return ""
    lbl = 'vehicle="%s",decode_epoch="%s"' % (_esc(state.get("vehicle", "")),
                                              _esc(state.get("epoch", "none")))
    lines = ["can_validation_configured{%s} %d"
             % (lbl, 1 if state.get("ok") else 0)]
    if not state.get("successes"):
        return "".join(l + "\n" for l in lines)
    c, g = state["c"], state["g"]
    for cls in ("known", "unknown", "error", "remote"):
        lines.append('can_validation_frames_total{%s,classification="%s"} %d'
                     % (lbl, cls, c[cls]))
    lines.append("can_validation_decode_errors_total{%s} %d"
                 % (lbl, c["decode_errors"]))
    lines.append("can_validation_invalid_values_total{%s} %d"
                 % (lbl, c["invalid_values"]))
    if state.get("has_checksums"):
        lines.append("can_validation_checksum_failures_total{%s} %d"
                     % (lbl, c["checksum_failures"]))
    lines.append("can_validation_unknown_ids{%s} %d" % (lbl, g["unknown_ids"]))
    if g.get("coverage") is not None:
        lines.append("can_validation_known_id_coverage_ratio{%s} %g"
                     % (lbl, g["coverage"]))
    if state.get("has_expected") and g.get("missing") is not None:
        lines.append("can_validation_missing_expected_signals{%s} %d"
                     % (lbl, g["missing"]))
    for r in state.get("rules", []):
        lines.append(
            'can_validation_cross_signal_failures_total{%s,rule="%s"} %d'
            % (lbl, _esc(r), state["cross"].get(r, 0)))
    if g.get("success_ts") is not None:
        lines.append("can_validation_last_success_timestamp_seconds{%s} %d"
                     % (lbl, g["success_ts"]))
    return "".join(l + "\n" for l in lines)
