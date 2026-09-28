"""Behavior tests for scripts/battery_rul.py.

Plain asserts, stdlib only; doubles as __main__ runner. All inputs are
tiny synthetic lab-cell operation mappings through the real
battery_reference loader (same schema as the official 636-record JSON),
labelled synthetic. Nothing here touches vehicle data.
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import battery_reference as br
import battery_rul as rul


def dis(cap, times=(0.0, 1800.0, 3600.0)):
    n = len(times)
    return {"type": "discharge",
            "data": {"Capacity": cap, "Time": list(times),
                     "Voltage_measured": [4.1 - 0.1 * k for k in range(n)],
                     "Current_measured": [-2.0] * n,
                     "Temperature_measured": [24.0] * n}}


def lin_battery(bid, c0, rate, n, eol=1.4):
    ops = [dis(c0 - rate * k) for k in range(n)]
    return br.convert_operations(bid, ops, eol)


def lab_envelope():
    """Three same-rate, different-offset synthetic batteries (exact linear
    RUL = (capacity - eol) / rate). Plus one censored battery."""
    recs = []
    for bid, c0, n in (("B1", 2.0, 65), ("B2", 1.9, 55), ("B3", 1.8, 45)):
        records, _ = lin_battery(bid, c0, 0.01, n)
        recs.extend(records)
    censored, _ = lin_battery("B7", 1.9, 0.001, 10)  # never reaches 1.4
    assert all(r["censored"] for r in censored)
    recs.extend(censored)
    meta = br.metadata_for(br.NASA_SOURCE_URL, br.NASA_PAGE_URL,
                           br.NASA_REFERENCE, "nasa-pcoe-lab", 1.4)
    return recs, meta


def history_of(records, bid, upto):
    rows = sorted((r for r in records if r["battery_id"] == bid),
                  key=lambda r: r["discharge_cycle"])[:upto]
    return [{"discharge_cycle": r["discharge_cycle"],
             "capacity_ah": r["capacity_ah"],
             "duration_s": r["duration_s"]} for r in rows]


def test_known_linear_relation_learned_and_beats_baseline():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    test = model["test"]
    assert test["eligible_batteries"] == ["B3"]
    assert test["n_samples"] == 40  # eol cycle 40, min_history 2
    assert test["mae"] < 0.5  # near-exact linear recovery, not a guess
    assert test["mae"] < test["baseline_mae"] / 10
    assert model["train"]["mae"] < 0.5
    assert model["baseline_rul_cycles"] > 0


def test_battery_level_split_isolation():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    assert set(model["train"]["batteries"]) == {"B1", "B2", "B7"}
    assert set(model["train"]["eligible_batteries"]) == {"B1", "B2"}
    assert "B3" not in model["train"]["eligible_batteries"]
    train, test = br.split_by_battery(recs, ["B3"])
    assert {r["battery_id"] for r in train} == {"B1", "B2", "B7"}
    assert {r["battery_id"] for r in test} == {"B3"}
    try:
        rul.train_model(recs, meta, test_battery_ids=["B0099"])
    except rul.RULerror:
        pass
    else:
        raise AssertionError("unknown test battery must raise")


def test_features_use_history_only_no_future_leakage():
    recs, _ = lab_envelope()
    full = rul.samples_from_records(
        [r for r in recs if r["battery_id"] == "B1"], 2, 5)
    prefix_recs = [r for r in recs if r["battery_id"] == "B1"
                   and r["discharge_cycle"] < 10]
    prefix = rul.samples_from_records(prefix_recs, 2, 5)
    assert len(prefix) == 9
    for a, b in zip(prefix, full[:9]):
        assert a["features"] == b["features"]  # future never leaks back
        assert a["target"] == b["target"]
    assert list(rul.FEATURE_NAMES) == ["capacity_ah",
                                       "slope_ah_per_cycle",
                                       "discharge_cycle", "duration_s"]
    assert not any("rul" in name or "eol" in name
                   for name in rul.FEATURE_NAMES)
    # slope over the trailing window ending at the current cycle only
    clean = rul._clean_history(history_of(recs, "B1", 8))
    feats = rul.history_features(clean, 5)
    assert abs(feats[1] - (-0.01)) < 1e-12
    assert feats[0] == clean[-1][1] and feats[2] == float(clean[-1][0])


def test_censored_battery_contributes_no_samples():
    recs, _ = lab_envelope()
    censored = [r for r in recs if r["battery_id"] == "B7"]
    assert censored and all(r["rul_cycles_target"] is None
                            for r in censored)
    assert rul.samples_from_records(censored, 2, 5) == []
    report = rul.evaluate_model(
        rul.train_model([r for r in recs if r["battery_id"] != "B7"],
                        br.metadata_for(br.NASA_SOURCE_URL, br.NASA_PAGE_URL,
                                        br.NASA_REFERENCE,
                                        "nasa-pcoe-lab", 1.4),
                        test_battery_ids=["B1"]),
        censored, test_battery_ids=["B7"])
    assert report["n_samples"] == 0 and report["mae"] is None


def test_post_eol_negatives_excluded():
    ops = [dis(1.9), dis(1.39), dis(1.45), dis(1.3)]
    records, _ = br.convert_operations("B", ops, 1.4)
    assert [r["rul_cycles_target"] for r in records] == [1, 0, -1, -2]
    samples = rul.samples_from_records(records, 2, 5)
    assert [s["target"] for s in samples] == [0.0]  # negatives not trained


def test_train_serialization_deterministic():
    recs, meta = lab_envelope()
    first = rul.train_model(recs, meta, test_battery_ids=["B3"])
    second = rul.train_model(recs, meta, test_battery_ids=["B3"])
    assert first == second  # same inputs -> same artifact, no wall clock
    tmp = tempfile.mkdtemp(prefix="battery-rul-")
    path = os.path.join(tmp, "model.json")
    rul.save_model(first, path)
    back = rul.load_model(path)
    assert back == first
    with open(path, encoding="utf-8") as handle:
        assert json.load(handle) == first  # plain JSON, never pickle
    feats = rul.samples_from_records(
        [r for r in recs if r["battery_id"] == "B3"], 2, 5)[0]["features"]
    assert (rul.predict_features(feats, back)
            == rul.predict_features(feats, first))
    try:
        rul.load_model(os.path.join(tmp, "missing.json"))
    except rul.RULerror:
        pass
    else:
        raise AssertionError("missing artifact must raise")


def test_domain_mismatch_rejected():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    history = history_of(recs, "B1", 8)
    rows = rul.analyze([], [], {"rul": {"model": model,
                                        "domain": "tesla-fleet",
                                        "history": history}})
    assert {r["metric"] for r in rows} == set(rul.SUPPORTED_METRICS)
    assert all(r["status"] == "unavailable" for r in rows)
    assert all("domain_mismatch" in (r["reason"] or "") for r in rows)
    assert all(r["value"] is None for r in rows)  # never a Tesla estimate
    try:
        rul._check_domain("tesla-fleet", model)
    except rul.RULerror:
        pass
    else:
        raise AssertionError("cross-domain inference must raise")


def test_analyze_happy_path_estimated_rows():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    rows = rul.analyze([], [], {"rul": {"model": model,
                                        "domain": "nasa-pcoe-lab",
                                        "history": history_of(recs, "B1",
                                                              8)}})
    by_metric = {r["metric"]: r for r in rows}
    assert sorted(by_metric) == sorted(rul.SUPPORTED_METRICS)
    pred = by_metric["battery.rul.cycles_remaining"]
    base = by_metric["battery.rul.baseline_cycles_remaining"]
    assert pred["status"] == "estimated" and base["status"] == "estimated"
    assert pred["unit"] == "cycles" and base["unit"] == "cycles"
    assert abs(base["value"] - model["baseline_rul_cycles"]) < 1e-12
    assert pred["value"] is not None and pred["value"] > 0
    assert pred["model_version"] == model["model_version"]
    assert model["model_version"].startswith("sha256:")
    assert pred["evidence_count"] == 8
    assert pred["sample_count"] == model["train"]["n_samples"]
    assert "unvalidated" in (pred["reason"] or "")
    assert pred["analysis_id"] == "battery_rul"
    assert pred["uncertainty"] is None  # never fabricated confidence


def test_analyze_missing_model_history_insufficient():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    rows = rul.analyze([], [], {})
    assert all(r["status"] == "unavailable" for r in rows)
    assert all("missing_model" in (r["reason"] or "") for r in rows)
    rows = rul.analyze([], [], {"rul": {"model": model,
                                        "domain": "nasa-pcoe-lab"}})
    assert all("missing_history" in (r["reason"] or "") for r in rows)
    one = history_of(recs, "B1", 1)
    rows = rul.analyze([], [], {"rul": {"model": model,
                                        "domain": "nasa-pcoe-lab",
                                        "history": one}})
    assert all(r["status"] == "unavailable" for r in rows)
    assert all("insufficient_history" in (r["reason"] or "") for r in rows)
    rows = rul.analyze([], [], {"rul": {"model": {"format": "nope"},
                                        "domain": "nasa-pcoe-lab",
                                        "history": history_of(recs, "B1",
                                                              8)}})
    assert all(r["status"] == "error" for r in rows)


def test_analyze_malformed_window_and_config():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    cfg = {"window_start_ns": 2, "window_end_ns": 1,
           "rul": {"model": model, "domain": "nasa-pcoe-lab",
                   "history": history_of(recs, "B1", 8)}}
    rows = rul.analyze([], [], cfg)
    assert all(r["status"] == "error" for r in rows)
    assert all("window_order" in (r["reason"] or "") for r in rows)
    rows = rul.analyze([], [], {"window_start_ns": "x", "rul": {}})
    assert all(r["status"] == "error" for r in rows)
    rows = rul.analyze([], [], {"decision_time_ns": "x", "rul": {}})
    assert all(r["status"] == "error" for r in rows)
    assert all("decision_time_ns" in (r["reason"] or "") for r in rows)
    rows = rul.analyze([], [], {"rul": ["not", "a", "dict"]})
    assert all(r["status"] == "error" for r in rows)
    # two scopes, one history: ambiguous, never split the difference
    sig = {"event_time_ns": 100, "vehicle": "v", "source": "fleet",
           "decode_epoch": "e1"}
    other = dict(sig, source="can")
    rows = rul.analyze([sig, other], [], {"rul": {"model": model,
                                                  "domain": "nasa-pcoe-lab",
                                                  "history": history_of(
                                                      recs, "B1", 8)}})
    assert all("ambiguous_scope" in (r["reason"] or "") for r in rows)


def test_model_metadata_honest():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    assert model["format"] == "battery-rul-model/1"
    assert model["model_version"].startswith("sha256:")
    assert model["dataset_domain"] == "nasa-pcoe-lab"
    assert model["eol_ah"] == 1.4
    assert model["features"] == list(rul.FEATURE_NAMES)
    assert model["chemistry"] is None and model["protocol"] is None
    assert "never inferred" in model["chemistry_note"]
    assert "unvalidated" in model["generalization_note"].lower() \
        or "out of domain" in model["generalization_note"]
    assert set(model["input_ranges"]) == set(rul.FEATURE_NAMES)
    assert model["test"]["baseline_mae"] is not None


def test_model_version_identity_distinct_and_deterministic():
    recs, meta = lab_envelope()
    first = rul.train_model(recs, meta, test_battery_ids=["B3"])
    again = rul.train_model(recs, meta, test_battery_ids=["B3"])
    other = rul.train_model(recs, meta, test_battery_ids=["B2"])
    assert first["model_version"] == again["model_version"]
    assert first["model_version"] != other["model_version"]
    assert first["format"] == other["format"] == "battery-rul-model/1"


def test_scoped_run_needs_binding_and_rejects_live_target():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    history = history_of(recs, "B1", 8)
    lab_scope = {"vehicle": "labcell-B1", "source": "labdaq",
                 "decode_epoch": "lab-e1"}
    lab_sig = {"event_time_ns": 100, "vehicle": lab_scope["vehicle"],
               "source": lab_scope["source"],
               "decode_epoch": lab_scope["decode_epoch"]}
    rows = rul.analyze([lab_sig], [], {"rul": {"model": model,
                                               "domain": "nasa-pcoe-lab",
                                               "history": history}})
    assert all(r["status"] == "unavailable" for r in rows)
    assert all("missing_history_scope" in (r["reason"] or "") for r in rows)
    rows = rul.analyze([lab_sig], [],
                       {"rul": {"model": model, "domain": "nasa-pcoe-lab",
                                "history": history,
                                "history_scope": dict(lab_scope)}})
    assert all(r["status"] == "estimated" for r in rows)
    assert rows[0]["vehicle"] == lab_scope["vehicle"]
    wrong = dict(lab_scope, vehicle="labcell-B9")
    rows = rul.analyze([lab_sig], [],
                       {"rul": {"model": model, "domain": "nasa-pcoe-lab",
                                "history": history,
                                "history_scope": wrong}})
    assert all("history_scope_mismatch" in (r["reason"] or "")
               for r in rows)
    fleet_sig = {"event_time_ns": 100, "vehicle": "tesla-pack",
                 "source": "fleet", "decode_epoch": "e1"}
    fleet_scope = {"vehicle": "tesla-pack", "source": "fleet",
                   "decode_epoch": "e1"}
    rows = rul.analyze([fleet_sig], [],
                       {"rul": {"model": model, "domain": "nasa-pcoe-lab",
                                "history": history,
                                "history_scope": fleet_scope}})
    assert all(r["status"] == "unavailable" for r in rows)
    assert all("domain_mismatch" in (r["reason"] or "") for r in rows)
    assert all(r["value"] is None for r in rows)
    rows = rul.analyze([], [],
                       {"rul": {"model": model, "domain": "nasa-pcoe-lab",
                                "history": history,
                                "history_scope": fleet_scope}})
    assert all(r["status"] == "unavailable" for r in rows)
    assert all("domain_mismatch" in (r["reason"] or "") for r in rows)

def test_decision_time_needs_observed_evidence():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    history = history_of(recs, "B1", 8)
    stamped = [dict(row, observed_ns=1000 + row["discharge_cycle"])
               for row in history]
    early = [dict(row, observed_ns=100 + row["discharge_cycle"])
             for row in history]
    rows = rul.analyze([], [], {"decision_time_ns": 50,
                                "rul": {"model": model,
                                        "domain": "nasa-pcoe-lab",
                                        "history": history}})
    assert all("missing_observed_ns" in (r["reason"] or "") for r in rows)
    rows = rul.analyze([], [], {"decision_time_ns": 1003,
                                "rul": {"model": model,
                                        "domain": "nasa-pcoe-lab",
                                        "history": stamped}})
    assert all("post_decision_history" in (r["reason"] or "") for r in rows)
    rows = rul.analyze([], [], {"decision_time_ns": 10 ** 12,
                                "rul": {"model": model,
                                        "domain": "nasa-pcoe-lab",
                                        "history": early}})
    assert all(r["status"] == "estimated" for r in rows)
    assert rows[0]["evidence_count"] == 8


def test_out_of_range_history_stays_unavailable_in_aggregate():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    history = history_of(recs, "B1", 8)
    wild = [dict(history[k]) for k in range(8)]
    wild[-1] = dict(wild[-1], capacity_ah=9.9)
    feats = rul.history_features(rul._clean_history(wild),
                                 model["slope_window"])
    assert "capacity_ah" in rul.range_flags(feats, model)
    rows = rul.analyze([], [], {"rul": {"model": model,
                                        "domain": "nasa-pcoe-lab",
                                        "history": wild}})
    assert all(r["status"] == "unavailable" for r in rows)
    assert all("out_of_input_range" in (r["reason"] or "") for r in rows)
    assert all(r["value"] is None and r["unit"] is None for r in rows)


def test_evaluate_rejects_overlap_and_reference_mismatch():
    recs, meta = lab_envelope()
    model = rul.train_model(recs, meta, test_battery_ids=["B3"])
    try:
        rul.evaluate_model(model, recs, test_battery_ids=["B1", "B3"],
                           metadata=meta)
    except rul.RULerror as exc:
        assert "train_test_overlap" in str(exc)
    else:
        raise AssertionError("train/test overlap must raise")
    try:
        rul.evaluate_model(model, recs, test_battery_ids=["B1"],
                           metadata=dict(meta, dataset_domain="other-lab"))
    except rul.RULerror as exc:
        assert "domain_mismatch" in str(exc)
    else:
        raise AssertionError("domain mismatch must raise")
    try:
        rul.evaluate_model(model, recs, test_battery_ids=["B1"],
                           metadata=dict(meta, eol_ah=1.2))
    except rul.RULerror as exc:
        assert "eol_mismatch" in str(exc)
    else:
        raise AssertionError("EOL mismatch must raise")
    report = rul.evaluate_model(model, recs, test_battery_ids=["B3"],
                                metadata=meta)
    assert report["n_samples"] > 0 and report["mae"] is not None


def test_cli_parser_contract():
    parser = rul.build_parser()
    args = parser.parse_args(["train", "ref.json", "model.json",
                              "--test-battery", "B0018",
                              "--test-battery", "B0005"])
    assert args.command == "train" and args.reference == "ref.json"
    assert args.test_battery == ["B0018", "B0005"]
    assert args.min_history == 2 and args.slope_window == 5
    args = parser.parse_args(["evaluate", "ref.json", "model.json"])
    assert args.command == "evaluate" and args.test_battery == []
    args = parser.parse_args(["predict", "--model", "m.json",
                              "--history", "h.json"])
    assert args.command == "predict" and args.history == "h.json"


if __name__ == "__main__":
    names = sorted(n for n in list(globals()) if n.startswith("test_"))
    for name in names:
        globals()[name]()
    print("test_battery_rul: ok (%d tests)" % len(names))
