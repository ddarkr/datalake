#!/usr/bin/env python3
"""Offline remaining-useful-life (RUL) training, evaluation and inference.

Lab-cell method verification only. Standard library runtime (plus the
stdlib-only battery_reference loader for input envelopes).

What this is: a fitted supervised ridge-regression model that predicts
discharge-cycle RUL from history available at prediction time, trained on
official NASA PCoE lab-cell discharge records ingested by
scripts/analytics/battery/battery_reference.py. Plus one honest train-mean baseline for
comparison. No fabricated weights: every number in the artifact is fitted
or counted from the supplied reference JSON.

What this is not: never a Tesla pack lifetime estimate. The NASA
lab-cell artifact must never be applied to Fleet/Tesla data. Inference
fails closed on domain mismatch, insufficient history and absent models,
and aggregate analyze() reports unavailable with a precise reason rather
than a linear extrapolation branded as validated learned RUL.

Label discipline (offline labels use future knowledge, features never do):
  - rul_cycles_target comes from the first observed capacity_ah <= eol_ah
    crossing (see battery_reference.apply_eol). It is the training label.
  - Features for discharge_cycle k use records 0..k of the same battery
    only: current capacity_ah, least-squares capacity slope over the last
    slope_window points, discharge_cycle itself, and the current
    duration_s curve summary. EOL index, future capacities and the offline
    target are never features.
  - Right-censored batteries (never cross eol_ah) get no fake target and
    contribute zero training/evaluation samples.
  - Post-EOL rows (negative targets) are excluded from normal
    training/evaluation samples.

Split discipline: battery-level split only (never row split). The feature
scaler (mean/std) and the baseline are fit on train batteries only.

Config contract (config is a plain dict):
  config["window_start_ns"] / ["window_end_ns"]: optional integer ns
    echoed on rows; invalid values yield per-metric error rows.
  config["decision_time_ns"]: optional integer ns. Offline (None): the
    full supplied history is used. Online (set): every history entry
    needs a valid observed_ns, and only entries with
    observed_ns <= decision_time_ns count as as-of evidence; missing
    stamps or too few surviving points yield unavailable, never an
    as-of claim without proof.
  config["rul"]: plain dict, all optional:
    "model": model artifact dict exactly as written by the train CLI
      (required for any estimated value; no file path is read at runtime,
      the caller pastes the artifact content).
    "history": list of {"discharge_cycle": int >= 0, "capacity_ah": float
      > 0, "duration_s": float >= 0, "observed_ns": optional int ns},
      strictly increasing cycles, the last entry is the current cycle
      being predicted.
    "domain": non-empty string naming the history domain, e.g.
      "nasa-pcoe-lab". Must equal the model dataset_domain exactly or no
      estimate is produced.
    "history_scope": {"vehicle": str, "source": str, "decode_epoch": str}.
      Required whenever signals supply a concrete scope: it must equal
      that scope exactly, binding the config history to the target.
      Unscoped runs (no signals) are lab-offline only and still reject
      a live-vehicle history_scope. A NASA lab-cell estimate is never
      attached to a live-vehicle scope, even when the domain string
      matches.
  No thresholds, no enable flags, no calibration refs. Empty/absent "rul"
  runs uncalibrated: per-metric unavailable rows, never guessed numbers.
CLI (all stdlib, no training inside the reference loader):
  python3 -m scripts.analytics.battery.battery_rul train REFERENCE_JSON MODEL_JSON
      [--test-battery ID ...] [--min-history N] [--slope-window N]
      [--ridge FLOAT]
  python3 -m scripts.analytics.battery.battery_rul evaluate REFERENCE_JSON MODEL_JSON
      [--test-battery ID ...]
  python3 -m scripts.analytics.battery.battery_rul predict --model MODEL_JSON
      --history HISTORY_JSON
  HISTORY_JSON: {"domain": str, "history": [{...entries as above...}]}.
  Offline predict accepts entries with or without observed_ns and keeps
  the diagnostic out-of-range flags; aggregate analyze() is stricter.

Metrics (namespace battery.rul.*):
  battery.rul.cycles_remaining (unit "cycles"): fitted-model prediction,
    status estimated when produced, else unavailable/error.
  battery.rul.baseline_cycles_remaining (unit "cycles"): train-mean
    baseline prediction for the same history, labelled as baseline.
Held-out MAE/RMSE live in the model artifact and CLI output, not as
rows. No value_text, no episode_id, no uncertainty (never fabricated);
model_version is the deterministic artifact identity (format stays
battery-rul-model/1); calibration_version stays None (learned model,
not a calibration). Out-of-train-range history stays unavailable in
aggregate (never an estimate with a flag); the offline predict CLI may
report it as a labelled diagnostic.
"""

import argparse
import json
import math
import sys

from scripts.analytics.battery import battery_common as bc
from scripts.analytics.battery import battery_reference as br

CODE_VERSION = "1.0.0"
ALGORITHM_VERSION = "1.0.0"
ANALYSIS_ID = "battery_rul"
MODEL_FORMAT = "battery-rul-model/1"

SUPPORTED_METRICS = (
    "battery.rul.cycles_remaining",
    "battery.rul.baseline_cycles_remaining",
)

FEATURE_NAMES = ("capacity_ah", "slope_ah_per_cycle", "discharge_cycle",
                 "duration_s")
DEFAULT_MIN_HISTORY = 2
DEFAULT_SLOPE_WINDOW = 5
DEFAULT_RIDGE = 1e-6
# Source substrings that mark a live-vehicle target scope. A NASA
# lab-cell estimate is never attached to such a scope, even when the
# config domain string was copied to match the model domain.
LIVE_SCOPE_TOKENS = ("fleet", "tesla", "vehicle_signal", "vss")

LAB_WARNING = ("lab-cell model only; Tesla-pack accuracy unvalidated "
               "(out of domain)")


def _model_version_for(payload):
    """Deterministic artifact identity (not the format tag).

    sha256 over the canonical fitted content (domain, EOL, algorithm,
    features, scaler, weights, bias, baseline, ridge, windows, train
    battery/sample counts, test split), so two different fits always
    differ while re-fitting identical inputs reproduces the same
    version. Format stays MODEL_FORMAT; this is the trained-artifact
    """
    return "sha256:" + bc.revision_id(payload)


class RULerror(ValueError):
    pass


def _fail(msg):
    sys.stderr.write("battery_rul: error: %s\n" % (msg,))
    return 2


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _finite(value):
    return bc.safe_float(value)


def _clean_history(entries):
    """Validate explicit history into [(cycle, capacity_ah, duration_s,
    observed_ns|None)].

    Strictly increasing cycles; finite capacity > 0; finite duration >= 0.
    observed_ns, when present, must be a valid ns int (never bool/float);
    when absent it stays None (offline history). Anything else is a
    config authoring bug (RULerror, never guessing).
    """
    if not isinstance(entries, (list, tuple)) or not entries:
        raise RULerror("malformed: history must be a non-empty list")
    out = []
    prev_cycle = None
    for pos, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise RULerror("malformed: history[%d] must be a mapping" % pos)
        cycle = entry.get("discharge_cycle")
        if not _is_int(cycle) or cycle < 0:
            raise RULerror("malformed: history[%d] discharge_cycle must be "
                           "a non-negative int" % pos)
        cap = _finite(entry.get("capacity_ah"))
        if cap is None or cap <= 0.0:
            raise RULerror("malformed: history[%d] capacity_ah must be a "
                           "finite number > 0" % pos)
        dur = _finite(entry.get("duration_s"))
        if dur is None or dur < 0.0:
            raise RULerror("malformed: history[%d] duration_s must be a "
                           "finite number >= 0" % pos)
        observed = entry.get("observed_ns")
        if observed is not None and bc.to_ns(observed) is None:
            raise RULerror("malformed: history[%d] observed_ns must be a "
                           "valid ns int" % pos)
        if prev_cycle is not None and cycle <= prev_cycle:
            raise RULerror("malformed: history cycles must strictly increase")
        prev_cycle = cycle
        out.append((cycle, cap, dur, observed))
    return out


def _asof_history(clean, decision_time_ns):
    """Apply decision_time evidence: offline (None) keeps everything.

    Online (set) drops every entry whose observed_ns is missing or
    later than decision_time_ns; the survivor order is preserved, so
    slopes stay causal. Returns (kept, dropped).
    """
    if decision_time_ns is None:
        return list(clean), []
    kept = [row for row in clean
            if row[3] is not None and row[3] <= decision_time_ns]
    dropped = [row for row in clean if row not in kept]
    return kept, dropped


def _slope(xs, ys):
    """Least-squares slope of y over x (needs >= 2 points)."""
    n = len(xs)
    s_x = sum(xs)
    s_y = sum(ys)
    s_xx = sum(x * x for x in xs)
    s_xy = sum(x * y for x, y in zip(xs, ys))
    denom = n * s_xx - s_x * s_x
    if denom == 0.0:
        raise RULerror("internal: zero slope denominator")
    return (n * s_xy - s_x * s_y) / denom


def history_features(clean, slope_window):
    """Feature vector for the last (current) point of validated history.

    Uses the last slope_window points ending at the current cycle only:
    no future capacity, no EOL index, no offline target anywhere.
    """
    if len(clean) < 2:
        raise RULerror("sparse: need >= 2 history points for a slope")
    if not _is_int(slope_window) or slope_window < 2:
        raise RULerror("slope_window must be an int >= 2")
    window = clean[-slope_window:]
    cycle, cap, dur = clean[-1][:3]
    slope = _slope([c for c, _, _, *_ in window],
                   [v for _, v, _, *_ in window])
    return [cap, slope, float(cycle), dur]


def _check_records(records):
    if not isinstance(records, (list, tuple)):
        raise RULerror("records must be a list")
    for pos, rec in enumerate(records):
        if not isinstance(rec, dict) \
                or not isinstance(rec.get("battery_id"), str):
            raise RULerror("malformed: records[%d] needs a battery_id "
                           "string" % pos)
    return None


def samples_from_records(records, min_history, slope_window):
    """Eligible supervised samples from reference records.

    Skips censored batteries entirely (no fake target) and post-EOL rows
    (negative targets are not normal training/evaluation). Reads only
    discharge_cycle/capacity_ah/duration_s plus the censored flag and the
    offline label; eol_discharge_cycle is never touched.
    """
    if not _is_int(min_history) or min_history < 2:
        raise RULerror("min_history must be an int >= 2")
    if not _is_int(slope_window) or slope_window < 2:
        raise RULerror("slope_window must be an int >= 2")
    _check_records(records)
    samples = []
    battery_ids = sorted({r["battery_id"] for r in records})
    try:
        ordered = {}
        for battery_id in battery_ids:
            ordered[battery_id] = sorted(
                (r for r in records
                 if r.get("battery_id") == battery_id),
                key=lambda r: r.get("discharge_cycle", 0))
    except TypeError as exc:
        raise RULerror("malformed: discharge_cycle must be comparable: %s"
                       % (exc,))
    for battery_id in battery_ids:
        rows = ordered[battery_id]
        if not rows or any(r.get("censored") for r in rows):
            continue  # censored battery: never invent a terminal label
        entries = [{"discharge_cycle": r.get("discharge_cycle"),
                    "capacity_ah": r.get("capacity_ah"),
                    "duration_s": r.get("duration_s")} for r in rows]
        try:
            clean = _clean_history(entries)
        except RULerror as exc:
            raise RULerror("%s: %s" % (battery_id, exc))
        for pos, rec in enumerate(rows):
            target = rec.get("rul_cycles_target")
            if target is None or isinstance(target, bool):
                continue
            if _finite(target) is None or float(target) < 0:
                continue  # post-EOL negatives excluded; censored is None
            if pos + 1 < min_history:
                continue  # insufficient history at prediction time
            feats = history_features(clean[:pos + 1], slope_window)
            samples.append({"battery_id": battery_id,
                            "discharge_cycle": clean[pos][0],
                            "features": feats, "target": float(target)})
    return samples


def fit_scaler(rows):
    """Per-feature mean/std over train rows only. Zero spread -> std 1.0."""
    n_feat = len(FEATURE_NAMES)
    mean = [sum(r[i] for r in rows) / len(rows) for i in range(n_feat)]
    std = []
    for i in range(n_feat):
        var = sum((r[i] - mean[i]) ** 2 for r in rows) / len(rows)
        std.append(math.sqrt(var) if var > 0.0 else 1.0)
    return {"mean": mean, "std": std}


def apply_scaler(rows, scaler):
    mean, std = scaler["mean"], scaler["std"]
    return [[(r[i] - mean[i]) / std[i] for i in range(len(FEATURE_NAMES))]
            for r in rows]


def _solve_linear(matrix, vec):
    """Gaussian elimination with partial pivoting (tiny, deterministic)."""
    n = len(matrix)
    aug = [list(matrix[i]) + [vec[i]] for i in range(n)]
    for col in range(n):
        piv = max(range(col, n), key=lambda r: abs(aug[r][col]))
        if aug[piv][col] == 0.0:
            raise RULerror("internal: singular normal equations")
        aug[col], aug[piv] = aug[piv], aug[col]
        pivot = aug[col][col]
        for r in range(col + 1, n):
            factor = aug[r][col] / pivot
            if factor != 0.0:
                for c in range(col, n + 1):
                    aug[r][c] -= factor * aug[col][c]
    out = [0.0] * n
    for r in range(n - 1, -1, -1):
        if aug[r][r] == 0.0:
            raise RULerror("internal: singular normal equations")
        out[r] = (aug[r][n] - sum(aug[r][c] * out[c]
                                  for c in range(r + 1, n))) / aug[r][r]
    return out


def fit_ridge(rows_z, targets, ridge):
    """Ridge regression on standardized features; bias unpenalized."""
    lam = _finite(ridge)
    if lam is None or lam < 0.0:
        raise RULerror("ridge must be a finite number >= 0")
    p = len(FEATURE_NAMES)
    mat = [[0.0] * (p + 1) for _ in range(p + 1)]
    vec = [0.0] * (p + 1)
    for z, y in zip(rows_z, targets):
        row = [1.0] + list(z)
        for i in range(p + 1):
            vec[i] += row[i] * y
            for j in range(p + 1):
                mat[i][j] += row[i] * row[j]
    for i in range(1, p + 1):
        mat[i][i] += lam
    sol = _solve_linear(mat, vec)
    return {"bias": sol[0], "weights": sol[1:]}


def predict_row(feats_z, coef):
    return coef["bias"] + sum(w * z for w, z in
                              zip(coef["weights"], feats_z))


def mae_rmse(targets, preds):
    n = len(targets)
    errs = [p - t for p, t in zip(preds, targets)]
    mae = sum(abs(e) for e in errs) / n
    rmse = math.sqrt(sum(e * e for e in errs) / n)
    return mae, rmse


def _metadata(records_envelope_meta):
    meta = records_envelope_meta
    if not isinstance(meta, dict):
        raise RULerror("envelope metadata must be a mapping")
    domain = meta.get("dataset_domain")
    if not isinstance(domain, str) or not domain:
        raise RULerror("envelope metadata needs a dataset_domain string")
    eol = _finite(meta.get("eol_ah"))
    if eol is None or eol <= 0.0:
        raise RULerror("envelope metadata needs eol_ah > 0")
    return domain, eol, meta


def train_model(records, metadata, test_battery_ids=None, min_history=None,
                slope_window=None, ridge=None):
    """Fit scaler + ridge model + baseline; battery-level split enforced."""
    if min_history is None:
        min_history = DEFAULT_MIN_HISTORY
    if slope_window is None:
        slope_window = DEFAULT_SLOPE_WINDOW
    if ridge is None:
        ridge = DEFAULT_RIDGE
    domain, eol_ah, meta = _metadata(metadata)
    _check_records(records)
    known = sorted({r["battery_id"] for r in records})
    if len(known) < 2:
        raise RULerror("need >= 2 batteries for a battery-level split")
    if test_battery_ids is None:
        test_battery_ids = [known[-1]]
    else:
        test_battery_ids = list(test_battery_ids)
        for bid in test_battery_ids:
            if bid not in known:
                raise RULerror("unknown test battery %r" % (bid,))
    try:
        train_recs, test_recs = br.split_by_battery(records,
                                                    test_battery_ids)
    except br.ReferenceError as exc:
        raise RULerror(str(exc))
    train_ids = sorted({r.get("battery_id") for r in train_recs
                        if isinstance(r, dict)
                        and isinstance(r.get("battery_id"), str)})
    train_samples = samples_from_records(train_recs, min_history,
                                         slope_window)
    test_samples = samples_from_records(test_recs, min_history,
                                        slope_window)
    need = len(FEATURE_NAMES) + 1
    if len(train_samples) < need:
        raise RULerror("sparse: need >= %d train samples, have %d" %
                       (need, len(train_samples)))
    x_train = [s["features"] for s in train_samples]
    y_train = [s["target"] for s in train_samples]
    scaler = fit_scaler(x_train)
    coef = fit_ridge(apply_scaler(x_train, scaler), y_train, ridge)
    baseline = sum(y_train) / len(y_train)
    train_preds = [predict_row(z, coef) for z in
                   apply_scaler(x_train, scaler)]
    train_mae, train_rmse = mae_rmse(y_train, train_preds)
    base_train_mae, base_train_rmse = mae_rmse(
        y_train, [baseline] * len(y_train))
    if test_samples:
        x_test = [s["features"] for s in test_samples]
        y_test = [s["target"] for s in test_samples]
        test_preds = [predict_row(z, coef) for z in
                      apply_scaler(x_test, scaler)]
        test_mae, test_rmse = mae_rmse(y_test, test_preds)
        base_mae, base_rmse = mae_rmse(y_test,
                                       [baseline] * len(y_test))
    else:
        test_mae = test_rmse = base_mae = base_rmse = None
    ranges = {}
    for i, name in enumerate(FEATURE_NAMES):
        col = [s["features"][i] for s in train_samples]
        ranges[name] = [min(col), max(col)]
    eligible_train = sorted({s["battery_id"] for s in train_samples})
    eligible_test = sorted({s["battery_id"] for s in test_samples})
    identity = {"format": MODEL_FORMAT,
                "algorithm": "ridge_linear_regression_stdlib",
                "code": "battery_rul/" + CODE_VERSION,
                "dataset_domain": domain, "eol_ah": eol_ah,
                "reference": meta.get("reference"),
                "source_url": meta.get("source_url"),
                "features": list(FEATURE_NAMES),
                "min_history_cycles": min_history,
                "slope_window": slope_window, "ridge": float(ridge),
                "scaler": scaler, "bias": coef["bias"],
                "weights": coef["weights"],
                "baseline_rul_cycles": baseline,
                "train_batteries": train_ids,
                "train_n": len(train_samples),
                "test_batteries": sorted(test_battery_ids)}
    version = _model_version_for(identity)
    return {
        "format": MODEL_FORMAT,
        "model_version": version,
        "algorithm": "ridge_linear_regression_stdlib",
        "code": "battery_rul/" + CODE_VERSION,
        "dataset_domain": domain,
        "eol_ah": eol_ah,
        "reference": meta.get("reference"),
        "source_url": meta.get("source_url"),
        "chemistry": None,
        "chemistry_note": ("loader does not specify cell chemistry; "
                           "never inferred"),
        "protocol": None,
        "protocol_note": ("beyond the reference attribution string, no "
                          "machine-readable protocol is recorded"),
        "features": list(FEATURE_NAMES),
        "min_history_cycles": min_history,
        "slope_window": slope_window,
        "ridge": float(ridge),
        "scaler": scaler,
        "bias": coef["bias"],
        "weights": coef["weights"],
        "baseline_rul_cycles": baseline,
        "train": {"batteries": train_ids,
                  "eligible_batteries": eligible_train,
                  "n_samples": len(train_samples),
                  "mae": train_mae, "rmse": train_rmse,
                  "baseline_mae": base_train_mae,
                  "baseline_rmse": base_train_rmse},
        "test": {"batteries": sorted(test_battery_ids),
                 "eligible_batteries": eligible_test,
                 "n_samples": len(test_samples),
                 "mae": test_mae, "rmse": test_rmse,
                 "baseline_mae": base_mae, "baseline_rmse": base_rmse},
        "input_ranges": ranges,
        "generalization_note": ("held-out lab batteries only; Tesla-pack "
                                "accuracy unvalidated (out of domain)"),
    }


def check_model(model):
    """Validate a pasted artifact dict; returns it unchanged when honest."""
    if not isinstance(model, dict):
        raise RULerror("malformed: model must be a mapping")
    if model.get("format") != MODEL_FORMAT:
        raise RULerror("malformed: model format must be %r" % MODEL_FORMAT)
    if list(model.get("features", [])) != list(FEATURE_NAMES):
        raise RULerror("malformed: model features %r do not match %r" %
                       (model.get("features"), list(FEATURE_NAMES)))
    domain = model.get("dataset_domain")
    if not isinstance(domain, str) or not domain:
        raise RULerror("malformed: model needs a dataset_domain string")
    scaler = model.get("scaler")
    weights = model.get("weights")
    bias = _finite(model.get("bias"))
    base = _finite(model.get("baseline_rul_cycles"))
    if not isinstance(scaler, dict):
        raise RULerror("malformed: model needs a scaler mapping")
    mean, std = scaler.get("mean"), scaler.get("std")
    if (not isinstance(mean, list) or not isinstance(weights, list)
            or len(mean) != len(FEATURE_NAMES)
            or len(weights) != len(FEATURE_NAMES)):
        raise RULerror("malformed: model scaler/weights must each have %d "
                       "entries" % len(FEATURE_NAMES))
    if not isinstance(std, list) or len(std) != len(FEATURE_NAMES):
        raise RULerror("malformed: model scaler needs %d std entries" %
                       len(FEATURE_NAMES))
    for seq in (mean, std, weights):
        for v in seq:
            if _finite(v) is None:
                raise RULerror("malformed: model numbers must be finite")
    for s in std:
        if float(s) == 0.0:
            raise RULerror("malformed: model scaler std must be non-zero")
    if bias is None or base is None:
        raise RULerror("malformed: model needs finite bias and baseline")
    history_need = model.get("min_history_cycles")
    if not _is_int(history_need) or history_need < 2:
        raise RULerror("malformed: model min_history_cycles must be >= 2")
    win = model.get("slope_window")
    if not _is_int(win) or win < 2:
        raise RULerror("malformed: model slope_window must be >= 2")
    version = model.get("model_version")
    if not isinstance(version, str) or not version:
        raise RULerror("malformed: model needs a model_version string")
    train = model.get("train", {})
    train_n = train.get("n_samples") if isinstance(train, dict) else None
    if not _is_int(train_n) or train_n < 0:
        raise RULerror("malformed: model train.n_samples must be >= 0")
    eol = _finite(model.get("eol_ah"))
    if eol is None or eol <= 0.0:
        raise RULerror("malformed: model needs eol_ah > 0")
    return model


def predict_features(feats, model):
    """Deterministic inference: standardize with the train-only scaler."""
    check_model(model)
    if len(feats) != len(FEATURE_NAMES):
        raise RULerror("need %d features, have %d" % (len(FEATURE_NAMES),
                                                      len(feats)))
    conv = [_finite(v) for v in feats]
    if any(v is None for v in conv):
        raise RULerror("features must be finite numbers")
    scaler = model["scaler"]
    z = [(v - m) / s for v, m, s in
         zip(conv, scaler["mean"], scaler["std"])]
    pred = model["bias"] + sum(w * v for w, v in zip(model["weights"], z))
    if _finite(pred) is None:
        raise RULerror("internal: non-finite prediction")
    return pred


def range_flags(feats, model):
    """Feature names outside the recorded train input range."""
    ranges = model.get("input_ranges", {})
    out = []
    for i, name in enumerate(FEATURE_NAMES):
        span = ranges.get(name)
        if (isinstance(span, list) and len(span) == 2
                and _finite(span[0]) is not None
                and _finite(span[1]) is not None):
            if feats[i] < span[0] or feats[i] > span[1]:
                out.append(name)
    return out


def evaluate_model(model, records, test_battery_ids=None, metadata=None):
    """Held-out evaluation with the frozen train-only scaler/weights.

    Rejects any train/test battery overlap (held-out means disjoint).
    When the reference envelope metadata is supplied, its dataset_domain
    and eol_ah must match the model exactly; a different EOL definition
    or domain would silently re-label the targets, so evaluation
    refuses instead of printing a misleading MAE.
    """
    check_model(model)
    _check_records(records)
    if metadata is not None:
        domain, eol_ah, _ = _metadata(metadata)
        if domain != model["dataset_domain"]:
            raise RULerror("domain_mismatch: reference domain %r != model "
                           "domain %r" % (domain, model["dataset_domain"]))
        if eol_ah != float(model["eol_ah"]):
            raise RULerror("eol_mismatch: reference eol_ah %r != model "
                           "eol_ah %r" % (eol_ah, model["eol_ah"]))
    if test_battery_ids is None:
        test = model.get("test", {})
        test_battery_ids = list(test.get("batteries", [])) \
            if isinstance(test, dict) else []
    else:
        test_battery_ids = list(test_battery_ids)
    known = {r.get("battery_id") for r in records
             if isinstance(r, dict)
             and isinstance(r.get("battery_id"), str)}
    for bid in test_battery_ids:
        if bid not in known:
            raise RULerror("unknown test battery %r" % (bid,))
    trained = model.get("train", {})
    trained_ids = set(trained.get("batteries", [])) \
        if isinstance(trained, dict) else set()
    overlap = sorted(set(test_battery_ids) & set(trained_ids))
    if overlap:
        raise RULerror("train_test_overlap: %r was used to fit the model; "
                       "held-out evaluation needs disjoint batteries"
                       % (overlap,))
    subset = [r for r in records if isinstance(r, dict)
              and r.get("battery_id") in set(test_battery_ids)]
    samples = samples_from_records(subset, model["min_history_cycles"],
                                   model["slope_window"])
    eligible = sorted({s["battery_id"] for s in samples})
    report = {"model_version": model["model_version"],
              "dataset_domain": model["dataset_domain"],
              "batteries": sorted(test_battery_ids),
              "eligible_batteries": eligible,
              "n_samples": len(samples),
              "mae": None, "rmse": None,
              "baseline_mae": None, "baseline_rmse": None}
    if not samples:
        return report
    y = [s["target"] for s in samples]
    preds = [predict_features(s["features"], model) for s in samples]
    report["mae"], report["rmse"] = mae_rmse(y, preds)
    report["baseline_mae"], report["baseline_rmse"] = mae_rmse(
        y, [model["baseline_rul_cycles"]] * len(y))
    return report


def save_model(model, path):
    check_model(model)
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(model, handle, indent=2, sort_keys=True)
            handle.write("\n")
    except (OSError, TypeError, ValueError) as exc:
        raise RULerror("cannot write %s: %s" % (path, exc))


def load_model(path):
    try:
        with open(path, encoding="utf-8") as handle:
            model = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RULerror("cannot load %s: %s" % (path, exc))
    return check_model(model)


def load_history_file(path):
    try:
        with open(path, encoding="utf-8") as handle:
            doc = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RULerror("cannot load %s: %s" % (path, exc))
    if not isinstance(doc, dict):
        raise RULerror("%s: history file must hold a mapping" % path)
    domain = doc.get("domain")
    if not isinstance(domain, str) or not domain:
        raise RULerror("%s: history file needs a domain string" % path)
    try:
        clean = _clean_history(doc.get("history"))
    except RULerror as exc:
        raise RULerror("%s: %s" % (path, exc))
    return domain, clean


def _check_domain(history_domain, model):
    if history_domain != model["dataset_domain"]:
        raise RULerror(
            "domain_mismatch: history domain %r != model domain %r; "
            "a NASA lab-cell model never applies to Tesla packs" %
            (history_domain, model["dataset_domain"]))


def _is_live_scope(scope):
    """True when a concrete aggregate scope smells like a live vehicle.

    A bound history scope is required for scoped inference, but binding
    alone is not enough: copying 'nasa-pcoe-lab' into the config must
    never attach a lab-cell estimate to a Fleet/Tesla target.
    """
    return any(isinstance(part, str)
               and any(tok in part.lower() for tok in LIVE_SCOPE_TOKENS)
               for part in scope)


def _history_scope(raw):
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise RULerror("malformed: rul.history_scope must be a mapping")
    scope = (raw.get("vehicle"), raw.get("source"),
             raw.get("decode_epoch"))
    if not all(isinstance(part, str) and part for part in scope):
        raise RULerror("malformed: rul.history_scope needs non-empty "
                       "vehicle/source/decode_epoch strings")
    return scope


def _window(config):
    raw_ws = config.get("window_start_ns")
    raw_we = config.get("window_end_ns")
    labels = []
    for label, raw in (("window_start_ns", raw_ws),
                       ("window_end_ns", raw_we),
                       ("decision_time_ns", config.get("decision_time_ns"))):
        if raw is None:
            continue
        if bc.to_ns(raw) is None:
            labels.append(label)
    if labels:
        return None, labels
    return (bc.to_ns(raw_ws), bc.to_ns(raw_we)), []


def analyze(signals, events, config):
    """analyze(signals, events, config) -> list[dict] (stable contract)."""
    cfg_all = config if isinstance(config, dict) else {}
    window, bad_labels = _window(cfg_all)
    if bad_labels:
        return [bc.make_result(
            metric=m, value=None, unit=None, status="error",
            reason="malformed:%s" % bad_labels[0], analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "window", cfg_all.get(
                bad_labels[0]), ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    if window[0] is not None and window[1] is not None \
            and window[1] < window[0]:
        return [bc.make_result(
            metric=m, value=None, unit=None, status="error",
            reason="malformed:window_order", analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "window", window,
                                    ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    rcfg = cfg_all.get("rul", {})
    if not isinstance(rcfg, dict):
        return [bc.make_result(
            metric=m, value=None, unit=None, status="error",
            reason="malformed:rul_config", analysis_id=ANALYSIS_ID,
            revision=bc.revision_id(m, "config", ALGORITHM_VERSION))
            for m in SUPPORTED_METRICS]
    _ = events
    scoped = set()
    for sig in bc.normalize_signals(
            signals if isinstance(signals, list) else []):
        scoped.add((sig["vehicle"], sig["source"],
                    sig["decode_epoch"]))
    scopes = sorted(scoped, key=repr) if scoped else [(None, None, None)]
    single = len(scopes) == 1

    def _rows(status, reason, values=None, model_version=None,
              evidence=None, train_n=None, err_rev=None):
        out = []
        for pos, metric in enumerate(SUPPORTED_METRICS):
            value = None if values is None else values[pos]
            unit = "cycles" if status == "estimated" else None
            if status in ("unavailable", "error"):
                value = None
                unit = None
            rev = err_rev if err_rev is not None else bc.revision_id(
                metric, scopes if single else [], rcfg if single else {},
                ALGORITHM_VERSION)
            out.append(bc.make_result(
                metric=metric, value=value, unit=unit, status=status,
                reason=reason, window_start_ns=window[0],
                window_end_ns=window[1],
                vehicle=scopes[0][0] if single else None,
                source=scopes[0][1] if single else None,
                decode_epoch=scopes[0][2] if single else None,
                evidence_count=evidence, sample_count=train_n,
                algorithm_version=ALGORITHM_VERSION,
                model_version=model_version, analysis_id=ANALYSIS_ID,
                revision=rev))
        return out

    if not single:
        return _rows("unavailable",
                     "ambiguous_scope:%d_scopes_for_single_history"
                     % len(scopes))
    scope = scopes[0]
    scoped_run = scope != (None, None, None)
    raw_model = rcfg.get("model")
    if raw_model is None:
        return _rows("unavailable",
                     "missing_model:configure rul.model from a "
                     "battery_rul train artifact")
    try:
        model = check_model(raw_model)
    except RULerror as exc:
        return _rows("error", str(exc))
    domain = rcfg.get("domain")
    if not isinstance(domain, str) or not domain:
        return _rows("unavailable",
                     "missing_domain:history domain must equal model "
                     "domain %r" % (model["dataset_domain"],),
                     model_version=model["model_version"])
    if domain != model["dataset_domain"]:
        return _rows(
            "unavailable",
            "domain_mismatch:target %r != model %r; NASA lab-cell "
            "estimates never apply to Tesla packs"
            % (domain, model["dataset_domain"]),
            model_version=model["model_version"])
    try:
        bound = _history_scope(rcfg.get("history_scope"))
    except RULerror as exc:
        return _rows("error", str(exc),
                     model_version=model["model_version"])
    if bound is not None and _is_live_scope(bound):
        return _rows(
            "unavailable",
            "domain_mismatch:live-vehicle history_scope %r is out of "
            "domain for NASA lab-cell model %r"
            % (bound, model["dataset_domain"]),
            model_version=model["model_version"])
    if scoped_run and _is_live_scope(scope):
        return _rows(
            "unavailable",
            "domain_mismatch:live-vehicle scope %r is out of domain "
            "for NASA lab-cell model %r"
            % (scope, model["dataset_domain"]),
            model_version=model["model_version"])
    if scoped_run and bound is None:
        return _rows("unavailable",
                     "missing_history_scope:bind rul.history_scope to "
                     "the target scope before attaching lab history",
                     model_version=model["model_version"])
    if scoped_run and bound is not None and bound != scope:
        return _rows("unavailable",
                     "history_scope_mismatch:history bound to %r, "
                     "target scope is %r" % (bound, scope),
                     model_version=model["model_version"])
    raw_history = rcfg.get("history")
    if raw_history is None or raw_history == []:
        return _rows("unavailable",
                     "missing_history:configure rul.history with "
                     "discharge_cycle/capacity_ah/duration_s rows",
                     model_version=model["model_version"])
    try:
        clean = _clean_history(raw_history)
    except RULerror as exc:
        return _rows("error", str(exc))
    decision = bc.to_ns(cfg_all.get("decision_time_ns"))
    kept, dropped = _asof_history(clean, decision)
    if decision is not None:
        if any(row[3] is None for row in clean):
            return _rows(
                "unavailable",
                "missing_observed_ns:decision_time_ns needs a valid "
                "observed_ns on every history entry",
                model_version=model["model_version"], evidence=len(clean),
                train_n=model["train"]["n_samples"])
        if dropped:
            return _rows(
                "unavailable",
                "post_decision_history:%d_of_%d_entries_observed_after_"
                "decision_time" % (len(dropped), len(clean)),
                model_version=model["model_version"], evidence=len(kept),
                train_n=model["train"]["n_samples"])
    if len(kept) < model["min_history_cycles"]:
        if decision is not None:
            return _rows(
                "unavailable",
                "sparse:insufficient_asof_history_have_%d_need_%d"
                % (len(kept), model["min_history_cycles"]),
                model_version=model["model_version"], evidence=len(kept),
                train_n=model["train"]["n_samples"])
        return _rows(
            "unavailable",
            "sparse:insufficient_history_have_%d_need_%d"
            % (len(kept), model["min_history_cycles"]),
            model_version=model["model_version"], evidence=len(kept),
            train_n=model["train"]["n_samples"])
    try:
        feats = history_features(kept, model["slope_window"])
        pred = predict_features(feats, model)
    except RULerror as exc:
        return _rows("error", str(exc))
    base = model["baseline_rul_cycles"]
    if _finite(pred) is None or _finite(base) is None:
        return _rows("error", "internal:non_finite_estimate")
    flags = range_flags(feats, model)
    if flags:
        return _rows(
            "unavailable",
            "out_of_input_range:" + ",".join(flags) + ";no_unsupported_"
            "estimate",
            model_version=model["model_version"], evidence=len(kept),
            train_n=model["train"]["n_samples"])
    rows = _rows("estimated",
                 "fitted_ridge_regression_on_lab_cell_history;"
                 "unvalidated_for_tesla_packs",
                 values=[pred, base],
                 model_version=model["model_version"], evidence=len(kept),
                 train_n=model["train"]["n_samples"])
    rows[1] = bc.make_result(
        metric=rows[1]["metric"], value=base, unit="cycles",
        status="estimated",
        reason="train_mean_baseline;unvalidated_for_tesla_packs",
        window_start_ns=window[0], window_end_ns=window[1],
        vehicle=rows[1]["vehicle"], source=rows[1]["source"],
        decode_epoch=rows[1]["decode_epoch"],
        evidence_count=len(kept),
        sample_count=model["train"]["n_samples"],
        algorithm_version=ALGORITHM_VERSION,
        model_version=model["model_version"], analysis_id=ANALYSIS_ID,
        revision=rows[1]["revision"])
    return rows


def _pos_int_flag(name, value):
    if not _is_int(value) or value < 2:
        raise RULerror("--%s must be an int >= 2" % name)
    return value


def cmd_train(args):
    if not _finite(args.ridge) or args.ridge < 0:
        return _fail("--ridge must be a finite number >= 0")
    try:
        envelope = br.load_reference_json(args.reference)
    except br.ReferenceError as exc:
        return _fail(exc)
    try:
        model = train_model(
            envelope.get("records"), envelope.get("metadata"),
            test_battery_ids=args.test_battery or None,
            min_history=_pos_int_flag("min-history", args.min_history),
            slope_window=_pos_int_flag("slope-window", args.slope_window),
            ridge=args.ridge)
        save_model(model, args.model)
    except (RULerror, br.ReferenceError) as exc:
        return _fail(exc)
    test = model["test"]
    print("battery_rul: train batteries=%s eligible=%s samples=%d "
          "mae=%.4f rmse=%.4f" % (model["train"]["batteries"],
                                  model["train"]["eligible_batteries"],
                                  model["train"]["n_samples"],
                                  model["train"]["mae"],
                                  model["train"]["rmse"]))
    if test["n_samples"]:
        print("battery_rul: test batteries=%s eligible=%s samples=%d "
              "mae=%.4f rmse=%.4f baseline_mae=%.4f baseline_rmse=%.4f"
              % (test["batteries"], test["eligible_batteries"],
                 test["n_samples"], test["mae"], test["rmse"],
                 test["baseline_mae"], test["baseline_rmse"]))
    else:
        print("battery_rul: test batteries=%s eligible=[] samples=0 "
              "(censored or insufficient history; no targets invented)"
              % (test["batteries"],))
    print("battery_rul: wrote %s domain=%s eol_ah=%s model_version=%s" %
          (args.model, model["dataset_domain"], model["eol_ah"],
           model["model_version"]))
    print("battery_rul: warning=%s" % LAB_WARNING)
    return 0


def cmd_evaluate(args):
    try:
        envelope = br.load_reference_json(args.reference)
        model = load_model(args.model)
        report = evaluate_model(model, envelope.get("records"),
                                test_battery_ids=args.test_battery or None,
                                metadata=envelope.get("metadata"))
    except (RULerror, br.ReferenceError) as exc:
        return _fail(exc)
    print("battery_rul: evaluate model_version=%s domain=%s" %
          (report["model_version"], report["dataset_domain"]))
    if report["n_samples"]:
        print("battery_rul: test batteries=%s eligible=%s samples=%d" %
              (report["batteries"], report["eligible_batteries"],
               report["n_samples"]))
        print("battery_rul: model mae=%.4f rmse=%.4f" %
              (report["mae"], report["rmse"]))
        print("battery_rul: baseline mae=%.4f rmse=%.4f" %
              (report["baseline_mae"], report["baseline_rmse"]))
    else:
        print("battery_rul: test batteries=%s eligible=[] samples=0 "
              "(censored or insufficient history; no targets invented)" %
              (report["batteries"],))
    print("battery_rul: warning=%s" % LAB_WARNING)
    return 0


def cmd_predict(args):
    try:
        model = load_model(args.model)
        history_domain, clean = load_history_file(args.history)
        _check_domain(history_domain, model)
        if len(clean) < model["min_history_cycles"]:
            raise RULerror("sparse: history has %d points, model needs "
                           ">= %d" % (len(clean),
                                       model["min_history_cycles"]))
        feats = history_features(clean, model["slope_window"])
        pred = predict_features(feats, model)
    except (RULerror, br.ReferenceError) as exc:
        return _fail(exc)
    doc = {"rul_cycles": pred,
           "baseline_rul_cycles": model["baseline_rul_cycles"],
           "model_version": model["model_version"],
           "dataset_domain": model["dataset_domain"],
           "extrapolated_out_of_input_range": range_flags(feats, model),
           "warning": LAB_WARNING}
    print(json.dumps(doc, sort_keys=True))
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        description="Train, evaluate and apply a lab-cell RUL regression "
                    "model on NASA PCoE reference JSON (see "
                    "battery_reference.py). Battery-level splits only; "
                    "censored batteries get no invented targets; NASA "
                    "lab-cell estimates never apply to Tesla packs.")
    sub = parser.add_subparsers(dest="command", required=True)
    train = sub.add_parser("train", help="fit scaler + ridge model + "
                                         "baseline from reference JSON")
    train.add_argument("reference", help="reference JSON from "
                                         "battery_reference.py")
    train.add_argument("model", help="output model JSON path (no pickle)")
    train.add_argument("--test-battery", action="append", default=[],
                       help="held-out battery id (repeatable; default: "
                            "last id sorted)")
    train.add_argument("--min-history", type=int,
                       default=DEFAULT_MIN_HISTORY,
                       help="history points required (default %(default)s)")
    train.add_argument("--slope-window", type=int,
                       default=DEFAULT_SLOPE_WINDOW,
                       help="points per capacity slope (default "
                            "%(default)s)")
    train.add_argument("--ridge", type=float, default=DEFAULT_RIDGE,
                       help="ridge penalty (default %(default)s)")
    train.set_defaults(func=cmd_train)
    evaluate = sub.add_parser("evaluate", help="held-out MAE/RMSE with "
                                               "frozen train-only "
                                               "preprocessing; rejects "
                                               "train/test overlap and "
                                               "domain/EOL mismatch")
    evaluate.add_argument("reference", help="reference JSON")
    evaluate.add_argument("model", help="model JSON from train")
    evaluate.add_argument("--test-battery", action="append", default=[],
                          help="override held-out battery ids (default: "
                               "the ones recorded in the model)")
    evaluate.set_defaults(func=cmd_evaluate)
    predict = sub.add_parser("predict", help="one RUL estimate from an "
                                             "explicit history file")
    predict.add_argument("--model", required=True,
                         help="model JSON from train")
    predict.add_argument("--history", required=True,
                         help='history JSON {"domain": str, "history": '
                              '[{"discharge_cycle": int, "capacity_ah": '
                              'float, "duration_s": float}]}')
    predict.set_defaults(func=cmd_predict)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command == "train" and (
            not _finite(args.ridge) or args.ridge < 0):
        return _fail("--ridge must be a finite number >= 0")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
