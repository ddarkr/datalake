"""Behavior tests for scripts/battery_reference.py.

Plain asserts, stdlib only; doubles as __main__ runner. All inputs are
tiny in-memory operation mappings (the stdlib seam convert_operations
consumes) -- never a full dataset copy, never mock echoes.
"""
import csv
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import battery_reference as br


def dis(cap, n=4, start=0.0, step=10.0, v0=4.1, dv=0.3, i=-2.0, c=24.0):
    return {"type": "discharge",
            "data": {"Capacity": cap,
                     "Time": [start + k * step for k in range(n)],
                     "Voltage_measured": [v0 - dv * k for k in range(n)],
                     "Current_measured": [i] * n,
                     "Temperature_measured": [c] * n}}


def chg():
    return {"type": "charge", "data": {}}


def test_eol_first_crossing_labels_past_negative():
    ops = [chg(), dis(1.9), dis(1.5), dis(1.39), dis(1.2)]
    records, summary = br.convert_operations("B0005", ops, 1.4)
    assert [r["discharge_cycle"] for r in records] == [0, 1, 2, 3]
    assert [r["operation_index"] for r in records] == [1, 2, 3, 4]
    assert [r["rul_cycles_target"] for r in records] == [2, 1, 0, -1]
    assert [r["is_eol_crossing"] for r in records] == [False, False, True,
                                                       False]
    assert all(r["censored"] is False for r in records)
    assert all(r["eol_discharge_cycle"] == 2 for r in records)
    assert summary["eol_discharge_cycle"] == 2
    assert summary["censored"] is False
    assert summary["n_operations"] == 5 and summary["n_discharge"] == 4
    assert (summary["first_capacity_ah"],
            summary["last_capacity_ah"]) == (1.9, 1.2)


def test_censored_battery_gains_no_label():
    ops = [dis(1.89), dis(1.60), dis(1.43)]  # B0007-like: never hits 1.4
    records, summary = br.convert_operations("B0007", ops, 1.4)
    assert all(r["rul_cycles_target"] is None for r in records)
    assert all(r["censored"] is True for r in records)
    assert all(r["eol_discharge_cycle"] is None for r in records)
    assert all(r["is_eol_crossing"] is False for r in records)
    assert summary["censored"] is True
    assert summary["eol_discharge_cycle"] is None


def test_recovery_after_crossing_keeps_first_eol():
    ops = [dis(1.9), dis(1.39), dis(1.45), dis(1.3)]
    records, _ = br.convert_operations("B0006", ops, 1.4)
    assert [r["rul_cycles_target"] for r in records] == [1, 0, -1, -2]
    assert [r["is_eol_crossing"] for r in records] == [False, True, False,
                                                       False]


def test_configurable_eol_no_universal_default_applied():
    ops = [dis(1.9), dis(1.5)]
    records, _ = br.convert_operations("B", ops, 1.6)
    assert [r["rul_cycles_target"] for r in records] == [1, 0]
    records, _ = br.convert_operations("B", ops, 1.4)
    assert all(r["censored"] for r in records)


def test_curve_validation_rejects_nonfinite_ragged_backward():
    base = dis(1.8)["data"]
    bad_cap = dict(base, Capacity=float("nan"))
    try:
        br.convert_operations("B", [dis(1.8), {"type": "discharge",
                                               "data": bad_cap}], 1.4)
    except br.ReferenceError:
        pass
    else:
        raise AssertionError("NaN capacity must raise")
    ragged = dict(base, Time=[0.0, 10.0])
    try:
        br.convert_operations("B", [{"type": "discharge", "data": ragged}],
                              1.4)
    except br.ReferenceError:
        pass
    else:
        raise AssertionError("ragged curves must raise")
    backward = dict(base, Time=[20.0, 10.0, 0.0, -10.0])
    try:
        br.convert_operations("B", [{"type": "discharge",
                                     "data": backward}], 1.4)
    except br.ReferenceError:
        pass
    else:
        raise AssertionError("decreasing Time must raise")
    try:
        br.convert_operations("B", [chg()], 1.4)
    except br.ReferenceError:
        pass
    else:
        raise AssertionError("no-discharge battery must raise")


def test_envelope_records_order_and_json_csv_roundtrip():
    meta = br.metadata_for(br.NASA_SOURCE_URL, br.NASA_PAGE_URL,
                           br.NASA_REFERENCE, "nasa-pcoe-lab", 1.4)
    assert meta["protocol"] == "https"
    assert meta["domain"] == "phm-datasets.s3.amazonaws.com"
    env = br.convert_files([("B0006", [dis(2.03), dis(1.18)]),
                            ("B0005", [dis(1.85), dis(1.32)])], meta)
    assert [r["battery_id"] for r in env["records"]] == ["B0005", "B0005",
                                                         "B0006", "B0006"]
    assert set(env["batteries"]) == {"B0005", "B0006"}
    tmp = tempfile.mkdtemp(prefix="battery-ref-")
    jpath, cpath = os.path.join(tmp, "ref.json"), os.path.join(tmp,
                                                                "ref.csv")
    br.write_json(env, jpath)
    back = br.load_reference_json(jpath)
    assert back["records"] == env["records"]
    assert back["metadata"]["eol_ah"] == 1.4
    br.write_csv(env, cpath)
    with open(cpath, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 4
    assert rows[0]["battery_id"] == "B0005"
    assert float(rows[0]["capacity_ah"]) == 1.85
    assert rows[2]["censored"] == "0"  # B0006 crossed 1.4
    # full curves live in JSON only; CSV keeps scalar summary columns
    assert set(rows[0]) == set(br.CSV_COLUMNS)


def test_tesla_domain_rejected_without_explicit_override():
    try:
        br.metadata_for(br.NASA_SOURCE_URL, br.NASA_PAGE_URL,
                        br.NASA_REFERENCE, "tesla-fleet", 1.4)
    except br.ReferenceError:
        pass
    else:
        raise AssertionError("tesla domain must be rejected by default")
    meta = br.metadata_for(br.NASA_SOURCE_URL, br.NASA_PAGE_URL,
                           br.NASA_REFERENCE, "tesla-fleet", 1.4,
                           allow_cross_domain=True)
    assert meta["dataset_domain"] == "tesla-fleet"
    try:
        br.metadata_for("ftp://example.com/x.zip", br.NASA_PAGE_URL,
                        br.NASA_REFERENCE, "nasa-pcoe-lab", 1.4)
    except br.ReferenceError:
        pass
    else:
        raise AssertionError("non-http source URL must raise")


def test_group_split_strict_and_grouping():
    recs = []
    for k in range(2):
        recs.append({"battery_id": "B0005", "operation_index": k,
                     "discharge_cycle": k, "capacity_ah": 1.8,
                     "censored": False, "eol_discharge_cycle": 1,
                     "rul_cycles_target": 1 - k, "is_eol_crossing": (k == 1)})
    for k in range(2):
        recs.append({"battery_id": "B0007", "operation_index": k,
                     "discharge_cycle": k, "capacity_ah": 1.9,
                     "censored": True, "eol_discharge_cycle": None,
                     "rul_cycles_target": None, "is_eol_crossing": False})
    groups = br.group_by_battery(recs)
    assert sorted(groups) == ["B0005", "B0007"]
    train, test = br.split_by_battery(recs, ["B0007"])
    assert {r["battery_id"] for r in train} == {"B0005"}
    assert {r["battery_id"] for r in test} == {"B0007"}
    try:
        br.split_by_battery(recs, ["B0099"])
    except br.ReferenceError:
        pass
    else:
        raise AssertionError("unknown test battery must raise")


def test_summary_lines_report_verified_shape():
    meta = br.metadata_for(br.NASA_SOURCE_URL, br.NASA_PAGE_URL,
                           br.NASA_REFERENCE, "nasa-pcoe-lab", 1.4)
    env = br.convert_files([("B0007", [dis(1.89), dis(1.43)])], meta)
    lines = br.summary_lines(env)
    assert "B0007" in lines[0] and "censored=True" in lines[0]
    assert "eol_discharge_cycle=censored" in lines[0]
    assert lines[-1].startswith("wrote 2 records (1 batteries)")


if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_reference: ok (%d tests)" % len(names))
