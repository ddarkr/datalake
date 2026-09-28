#!/usr/bin/env python3
"""Offline NASA PCoE battery reference ingestion (one-shot, stdlib runtime).

Converts public NASA Prognostics Center of Excellence lithium-ion aging
cycles (.mat, read through an optional scipy loader) into bounded JSON
records (full V/A/C/s curves) or a scalar CSV summary for later offline
model work (SOC/DCIR/ICA/RUL). No RUL fitting happens here; this module
only builds honestly-labelled offline targets. Fitting/evaluation is a
separate future agent.

Record shape (one per discharge operation, JSON "records" list):
  battery_id: str            e.g. "B0005"
  operation_index: int       0-based position in the full .mat cycle list
                             (charge + discharge + impedance)
  discharge_cycle: int       0-based count among discharge ops only
  capacity_ah: float         measured discharge capacity (actualCapacityAh)
  n_points: int              curve length (2..max_points)
  duration_s: float          t_s[-1] - t_s[0]
  t_s: [float]               seconds within the discharge, non-decreasing
  v_v: [float]               Voltage_measured, volts
  i_a: [float]               Current_measured, amps
  temp_c: [float]            Temperature_measured, Celsius
  rul_cycles_target: int|None  offline label: eol_discharge_cycle minus
                             discharge_cycle (negative past EOL), or None
                             when the battery never crosses eol_ah
                             (censored). Future knowledge: NEVER a model
                             input feature, training may only use rows
                             with discharge_cycle <= eol_discharge_cycle.
  is_eol_crossing: bool      discharge_cycle == eol_discharge_cycle
  censored: bool             True when capacity never reaches eol_ah
  eol_discharge_cycle: int|None  first discharge_cycle with
                             capacity_ah <= eol_ah, None when censored

Envelope shape (JSON output):
  metadata: {source_url, source_page, protocol, domain, dataset_domain,
             reference, eol_ah, loader, rul_note}
  batteries: {battery_id: {battery_id, n_operations, n_discharge,
                           first_capacity_ah, last_capacity_ah,
                           eol_discharge_cycle, censored}}
  records: [record, ...] ordered by (battery_id, discharge_cycle)

Inputs are official NASA .mat files parsed with scipy.io.loadmat only.
No pickle/marshal/exec, no vendored datasets. .mat input needs scipy
(Main-verified 1.16.3/numpy 2.5.3); everything else is stdlib.
"""

import argparse
import csv
import json
import math
import os
import sys
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import battery_common as bc

LOADER_VERSION = "1.0.0"
NASA_SOURCE_URL = ("https://phm-datasets.s3.amazonaws.com/"
                   "NASA/5.+Battery+Data+Set.zip")
NASA_PAGE_URL = ("https://www.nasa.gov/intelligent-systems-division/"
                 "discovery-and-systems-health/pcoe/pcoe-data-set-repository/")
NASA_REFERENCE = ("Saha, B. and Goebel, K. (2007). Battery Data Set, "
                  "NASA Prognostics Data Repository. Charge 1.5A CC to "
                  "4.2V then CV to 20mA; discharge 2A to cell-specific "
                  "cutoff; EIS 0.1Hz-5kHz.")
# NASA 30%-fade lab criterion for 2Ah rated cells. Lab-only: NEVER a Tesla EOL.
DEFAULT_EOL_AH = 1.4
DEFAULT_DATASET_DOMAIN = "nasa-pcoe-lab"
DEFAULT_MAX_POINTS = 1000000
CROSS_DOMAIN_TOKENS = ("tesla", "fleet", "vehicle_signal", "vss")

CURVE_FIELDS = (("t_s", "Time"), ("v_v", "Voltage_measured"),
                ("i_a", "Current_measured"),
                ("temp_c", "Temperature_measured"))

RUL_NOTE = ("rul_cycles_target is an offline label from the first observed "
            "capacity_ah <= eol_ah crossing (uses future knowledge); never "
            "use it as a model input feature.")


class ReferenceError(ValueError):
    pass


def _fail(msg):
    sys.stderr.write(f"battery_reference: error: {msg}\n")
    return 2


def _positive_int(name, value):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReferenceError(f"{name} must be a positive int")
    return value


def metadata_for(source_url, page, reference, dataset_domain, eol_ah,
                 allow_cross_domain=False):
    """Explicit dataset metadata. Fails closed on Tesla/Fleet relabelling."""
    if not isinstance(source_url, str) or not source_url:
        raise ReferenceError("source_url must be a non-empty string")
    if not isinstance(dataset_domain, str) or not dataset_domain:
        raise ReferenceError("dataset_domain must be a non-empty string")
    if not bc.is_finite_number(eol_ah) or float(eol_ah) <= 0:
        raise ReferenceError("eol_ah must be a finite number > 0")
    parts = urllib.parse.urlsplit(source_url)
    if parts.scheme not in ("https", "http") or not parts.hostname:
        raise ReferenceError(f"source_url must be http(s) with a host: "
                             f"{source_url!r}")
    lowered = dataset_domain.lower()
    if (not allow_cross_domain
            and any(tok in lowered for tok in CROSS_DOMAIN_TOKENS)):
        raise ReferenceError(
            f"dataset_domain {dataset_domain!r} looks like live-vehicle "
            f"scope; this NASA lab artifact must stay lab-labelled "
            f"(pass --allow-cross-domain to override)")
    return {"source_url": source_url, "source_page": page,
            "protocol": parts.scheme, "domain": parts.hostname,
            "dataset_domain": dataset_domain, "reference": reference,
            "eol_ah": float(eol_ah), "loader": f"battery_reference/"
            f"{LOADER_VERSION}", "rul_note": RUL_NOTE}


def _curve(battery_id, operation_index, name, values, max_points):
    if values is None:
        raise ReferenceError(f"{battery_id} op {operation_index}: "
                             f"missing {name}")
    if not isinstance(values, (list, tuple)) or not values:
        raise ReferenceError(f"{battery_id} op {operation_index} {name}: "
                             f"need a non-empty sequence")
    if len(values) > max_points:
        raise ReferenceError(f"{battery_id} op {operation_index} {name}: "
                             f"{len(values)} points exceeds "
                             f"max_points={max_points}")
    out = []
    for value in values:
        if not bc.is_finite_number(value):
            raise ReferenceError(f"{battery_id} op {operation_index} "
                                 f"{name}: all samples must be finite "
                                 f"numbers")
        out.append(float(value))
    return out


def discharge_record(battery_id, operation_index, discharge_cycle, data,
                     max_points=DEFAULT_MAX_POINTS):
    """Validate one discharge op mapping into a record (no RUL target yet)."""
    if not isinstance(battery_id, str) or not battery_id:
        raise ReferenceError("battery_id must be a non-empty string")
    for label, value in (("operation_index", operation_index),
                         ("discharge_cycle", discharge_cycle)):
        if (isinstance(value, bool) or not isinstance(value, int)
                or value < 0):
            raise ReferenceError(f"{label} must be a non-negative int")
    _positive_int("max_points", max_points)
    if not isinstance(data, dict):
        raise ReferenceError(f"{battery_id} op {operation_index}: "
                             f"data must be a mapping")
    capacity = data.get("Capacity")
    if (isinstance(capacity, (list, tuple))
            or not bc.is_finite_number(capacity)
            or float(capacity) <= 0):
        raise ReferenceError(f"{battery_id} op {operation_index}: "
                             f"Capacity must be a finite number > 0")
    curves = {}
    for record_key, data_key in CURVE_FIELDS:
        curves[record_key] = _curve(battery_id, operation_index, data_key,
                                    data.get(data_key), max_points)
    lengths = {len(v) for v in curves.values()}
    if len(lengths) != 1:
        raise ReferenceError(f"{battery_id} op {operation_index}: "
                             f"curve length mismatch {sorted(lengths)}")
    if len(curves["t_s"]) < 2:
        raise ReferenceError(f"{battery_id} op {operation_index}: "
                             f"need >= 2 samples, got {len(curves['t_s'])}")
    times = curves["t_s"]
    for prev, cur in zip(times, times[1:]):
        if cur < prev:
            raise ReferenceError(f"{battery_id} op {operation_index}: "
                                 f"Time must be non-decreasing")
    return {"battery_id": battery_id, "operation_index": operation_index,
            "discharge_cycle": discharge_cycle,
            "capacity_ah": float(capacity),
            "n_points": len(times),
            "duration_s": times[-1] - times[0], "t_s": times,
            "v_v": curves["v_v"], "i_a": curves["i_a"],
            "temp_c": curves["temp_c"]}


def apply_eol(records, eol_ah, n_operations):
    """Attach offline RUL targets. First real crossing defines EOL; a battery
    that never crosses stays censored with no artificial label."""
    if not bc.is_finite_number(eol_ah) or float(eol_ah) <= 0:
        raise ReferenceError("eol_ah must be a finite number > 0")
    ordered = sorted(records, key=lambda r: r["discharge_cycle"])
    cycles = [r["discharge_cycle"] for r in ordered]
    if cycles != list(range(len(ordered))):
        raise ReferenceError("discharge_cycle must run 0..n-1 without gaps")
    threshold = float(eol_ah)
    eol_cycle = None
    for record in ordered:
        if record["capacity_ah"] <= threshold and eol_cycle is None:
            eol_cycle = record["discharge_cycle"]
    censored = eol_cycle is None
    labeled = []
    for record in ordered:
        row = dict(record)
        row["censored"] = censored
        row["eol_discharge_cycle"] = eol_cycle
        if censored:
            row["rul_cycles_target"] = None
            row["is_eol_crossing"] = False
        else:
            row["rul_cycles_target"] = eol_cycle - record["discharge_cycle"]
            row["is_eol_crossing"] = (record["discharge_cycle"] == eol_cycle)
        labeled.append(row)
    summary = {"battery_id": ordered[0]["battery_id"],
               "n_operations": n_operations,
               "n_discharge": len(ordered),
               "first_capacity_ah": ordered[0]["capacity_ah"],
               "last_capacity_ah": ordered[-1]["capacity_ah"],
               "eol_discharge_cycle": eol_cycle, "censored": censored}
    return labeled, summary


def convert_operations(battery_id, operations, eol_ah,
                       max_points=DEFAULT_MAX_POINTS):
    """Convert plain op mappings [{type, data}] to labeled records+summary.
    This is the stdlib-only seam: the scipy .mat loader produces these
    plain mappings, and tests build them by hand. Only type == "discharge"
    (case-insensitive) ops become records; operation_index preserves the
    position in the full sequence."""
    if not isinstance(operations, (list, tuple)) or not operations:
        raise ReferenceError(f"{battery_id}: need a non-empty operation list")
    records = []
    for operation_index, op in enumerate(operations):
        if not isinstance(op, dict):
            raise ReferenceError(f"{battery_id} op {operation_index}: "
                                 f"operation must be a mapping")
        kind = op.get("type")
        if not isinstance(kind, str):
            raise ReferenceError(f"{battery_id} op {operation_index}: "
                                 f"type must be a string")
        if kind.strip().lower() != "discharge":
            continue
        records.append(discharge_record(
            battery_id, operation_index, len(records),
            op.get("data"), max_points))
    if not records:
        raise ReferenceError(f"{battery_id}: no discharge operations found")
    return apply_eol(records, eol_ah, len(operations))


def _collect_text(node, parts):
    if isinstance(node, bytes):
        parts.append(node.decode("utf-8", "replace"))
    elif isinstance(node, str):
        parts.append(node)
    elif isinstance(node, (list, tuple)):
        for sub in node:
            _collect_text(sub, parts)
    elif hasattr(node, "tolist"):
        try:
            _collect_text(node.tolist(), parts)
        except (ValueError, TypeError):
            raise ReferenceError(f"expected text, got {type(node)!r}")
    else:
        raise ReferenceError(f"expected text, got {type(node)!r}")


def _to_text(value):
    """MATLAB char data arrives as str/bytes, a scalar array (.item()), or a
    squeezed char vector (sequence of 1-char strings); accept all three."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, str):
        return value
    if hasattr(value, "item"):
        try:
            item = value.item()
        except (ValueError, IndexError, TypeError, AttributeError):
            pass
        else:
            if isinstance(item, bytes):
                return item.decode("utf-8", "replace")
            if isinstance(item, str):
                return item
    parts = []
    _collect_text(value, parts)
    text = "".join(parts)
    if not text:
        raise ReferenceError(f"expected a scalar string, got {type(value)!r}")
    return text


def load_nasa_mat(path):
    """Read one official NASA .mat into (battery_id, operations).

    scipy.io.loadmat only (never pickle). Raises ReferenceError with the
    tested scipy/numpy versions when scipy is missing."""
    try:
        import numpy as np
        import scipy.io as sio
    except ImportError:
        raise ReferenceError(
            "reading .mat needs scipy (Main-verified scipy 1.16.3 / "
            "numpy 2.5.3); record/CSV handling stays stdlib-only")
    try:
        mat = sio.loadmat(path, struct_as_record=False, squeeze_me=True)
    except Exception as ex:
        raise ReferenceError(f"cannot read .mat {path}: {ex}")
    stem = os.path.splitext(os.path.basename(path))[0]
    keys = [k for k in mat if not k.startswith("__")]
    if stem in keys:
        key = stem
    elif len(keys) == 1:
        key = keys[0]
    else:
        raise ReferenceError(f"{path}: ambiguous variables {keys}, "
                             f"expected {stem!r}")
    try:
        cycles = mat[key].cycle
    except AttributeError:
        raise ReferenceError(f"{path}: variable {key!r} has no cycle field")
    if not isinstance(cycles, (list, tuple)) and getattr(
            cycles, "ndim", 1) == 0:
        cycles = [cycles]
    operations = []
    for position, cycle in enumerate(list(cycles)):
        try:
            kind = _to_text(cycle.type)
        except (AttributeError, ValueError, ReferenceError) as ex:
            raise ReferenceError(f"{path} cycle {position}: bad type: {ex}")
        data = {}
        raw = getattr(cycle, "data", None)
        if kind.strip().lower() == "discharge":
            for name in ("Voltage_measured", "Current_measured",
                         "Temperature_measured", "Time"):
                try:
                    arr = np.asarray(getattr(raw, name),
                                     dtype=float).ravel().tolist()
                except (AttributeError, TypeError, ValueError) as ex:
                    raise ReferenceError(
                        f"{path} cycle {position}: bad {name}: {ex}")
                data[name] = arr
            try:
                data["Capacity"] = float(
                    np.asarray(raw.Capacity, dtype=float).ravel()[0])
            except (AttributeError, TypeError, ValueError,
                    IndexError) as ex:
                raise ReferenceError(
                    f"{path} cycle {position}: bad Capacity: {ex}")
        operations.append({"type": kind, "data": data})
    return key, operations


def find_mat_files(input_path):
    if os.path.isfile(input_path):
        if not input_path.lower().endswith(".mat"):
            raise ReferenceError(f"input file is not a .mat: {input_path}")
        return [input_path]
    if os.path.isdir(input_path):
        found = sorted(os.path.join(input_path, name)
                       for name in os.listdir(input_path)
                       if name.lower().endswith(".mat")
                       and os.path.isfile(os.path.join(input_path, name)))
        if not found:
            raise ReferenceError(f"no .mat files in {input_path}")
        return found
    raise ReferenceError(f"input not found: {input_path}")


def convert_files(pairs, metadata):
    """pairs: [(battery_id, operations)]. Returns the JSON envelope dict."""
    records, batteries = [], {}
    for battery_id, operations in pairs:
        labeled, summary = convert_operations(
            battery_id, operations, metadata["eol_ah"])
        records.extend(labeled)
        if battery_id in batteries:
            raise ReferenceError(f"duplicate battery_id {battery_id!r}")
        batteries[battery_id] = summary
    records.sort(key=lambda r: (r["battery_id"], r["discharge_cycle"]))
    return {"metadata": metadata, "batteries": batteries,
            "records": records}


CSV_COLUMNS = ("battery_id", "operation_index", "discharge_cycle",
               "capacity_ah", "n_points", "duration_s",
               "rul_cycles_target", "censored", "is_eol_crossing",
               "eol_discharge_cycle")


def _csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, str):
        return value
    return repr(value)


def write_csv(envelope, output_path):
    try:
        handle = open(output_path, "w", newline="", encoding="utf-8")
    except OSError as ex:
        raise ReferenceError(f"cannot open {output_path}: {ex}")
    with handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_COLUMNS)
        for record in envelope["records"]:
            writer.writerow([_csv_cell(record[col]) for col in CSV_COLUMNS])


def write_json(envelope, output_path, pretty=False):
    try:
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(envelope, handle, indent=2 if pretty else None,
                      sort_keys=False)
            handle.write("\n")
    except (OSError, TypeError, ValueError) as ex:
        raise ReferenceError(f"cannot write {output_path}: {ex}")


def load_reference_json(path):
    """Consumer entry point for the BatteryRUL agent: read back an envelope."""
    try:
        with open(path, encoding="utf-8") as handle:
            envelope = json.load(handle)
    except (OSError, ValueError) as ex:
        raise ReferenceError(f"cannot load {path}: {ex}")
    if not isinstance(envelope, dict) or not isinstance(
            envelope.get("metadata"), dict) or not isinstance(
            envelope.get("records"), list):
        raise ReferenceError(f"{path}: not a battery reference envelope")
    return envelope


def group_by_battery(records):
    """Map battery_id -> discharge_cycle-sorted record list."""
    groups = {}
    for record in records:
        groups.setdefault(record["battery_id"], []).append(record)
    for group in groups.values():
        group.sort(key=lambda r: r["discharge_cycle"])
    return groups


def split_by_battery(records, test_battery_ids):
    """Strict group split: no battery appears on both sides."""
    wanted = list(test_battery_ids)
    known = {r["battery_id"] for r in records}
    for battery_id in wanted:
        if battery_id not in known:
            raise ReferenceError(f"unknown test battery {battery_id!r}")
    test_set = set(wanted)
    test = [r for r in records if r["battery_id"] in test_set]
    train = [r for r in records if r["battery_id"] not in test_set]
    return train, test


def _eol_cell(value):
    return "censored" if value is None else str(value)


def summary_lines(envelope):
    lines = []
    for battery_id in sorted(envelope["batteries"]):
        info = envelope["batteries"][battery_id]
        lines.append(
            f"{battery_id} ops={info['n_operations']} "
            f"discharges={info['n_discharge']} "
            f"eol_discharge_cycle={_eol_cell(info['eol_discharge_cycle'])} "
            f"censored={info['censored']} "
            f"first_ah={info['first_capacity_ah']:.4f} "
            f"last_ah={info['last_capacity_ah']:.4f}")
    lines.append(f"wrote {len(envelope['records'])} records "
                 f"({len(envelope['batteries'])} batteries)")
    return lines


def build_parser():
    parser = argparse.ArgumentParser(
        description="Ingest official NASA PCoE battery .mat cycles into "
                    "bounded JSON records (full V/A/C/s curves) or a scalar "
                    "CSV summary. RUL targets are offline labels only; no "
                    "fitting happens here. .mat input needs scipy "
                    "(Main-verified scipy 1.16.3 / numpy 2.5.3); record, "
                    "CSV, and JSON handling are stdlib-only.")
    parser.add_argument("input", help=".mat file or directory of .mat files")
    parser.add_argument("output", help="output JSON (default) or CSV path")
    parser.add_argument("--format", choices=("json", "csv"), default="json",
                        help="json keeps full curves; csv keeps scalars only")
    parser.add_argument("--eol-ah", type=float, default=DEFAULT_EOL_AH,
                        help="lab EOL capacity in Ah (default %(default)s; "
                             "NASA 30%%-fade lab criterion, never a Tesla "
                             "EOL)")
    parser.add_argument("--dataset-domain", default=DEFAULT_DATASET_DOMAIN,
                        help="domain label recorded in metadata "
                             "(default %(default)s)")
    parser.add_argument("--source-url", default=NASA_SOURCE_URL,
                        help="dataset source URL recorded in metadata")
    parser.add_argument("--source-page", default=NASA_PAGE_URL,
                        help="dataset landing page recorded in metadata")
    parser.add_argument("--reference", default=NASA_REFERENCE,
                        help="attribution string recorded in metadata")
    parser.add_argument("--max-points", type=int,
                        default=DEFAULT_MAX_POINTS,
                        help="per-curve sample bound (default %(default)s)")
    parser.add_argument("--allow-cross-domain", action="store_true",
                        help="permit tesla/fleet dataset-domain labels")
    parser.add_argument("--pretty", action="store_true",
                        help="indent JSON output")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if not math.isfinite(args.eol_ah) or args.eol_ah <= 0:
            raise ReferenceError("--eol-ah must be a finite number > 0")
        _positive_int("--max-points", args.max_points)
        metadata = metadata_for(args.source_url, args.source_page,
                                args.reference, args.dataset_domain,
                                args.eol_ah, args.allow_cross_domain)
        pairs = []
        for path in find_mat_files(args.input):
            battery_id, operations = load_nasa_mat(path)
            if not operations:
                raise ReferenceError(f"{path}: no cycles found")
            pairs.append((battery_id, operations))
        envelope = convert_files(pairs, metadata)
        if args.format == "csv":
            write_csv(envelope, args.output)
        else:
            write_json(envelope, args.output, args.pretty)
    except ReferenceError as ex:
        return _fail(ex)
    for line in summary_lines(envelope):
        print(f"battery_reference: {line}")
    print(f"battery_reference: output={args.output} "
          f"domain={metadata['dataset_domain']} eol_ah={metadata['eol_ah']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
