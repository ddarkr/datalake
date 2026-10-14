#!/usr/bin/env python3
"""Check measured backup/RAW/CAN costs against synthetic storage budgets.

GET is client egress; server-side COPY and retained storage are not egress.
GB prices use 10**9 bytes; GiB workloads use 2**30. Only display is rounded.
Oracle traffic is separate from explicitly configured restore assumptions.
"""

import argparse
import json
import math
import sys

SCHEMA = "storage-cost-measurements/1"
GIB, GB = 2 ** 30, 10 ** 9
REQ_OPS = ("lists", "heads", "gets", "puts", "copies", "deletes")
CASES = ("repeated-identical", "repeated-identical-2", "small-change", "large-change")
AUXILIARY = ("raw_fresh", "raw_replay", "can_small", "can_big")
ROLES = ("oldest-retained", "pinned", "newest")
# Unknown policy fields cannot introduce unreviewed relaxation semantics.
RULES = {
    "absolute": {
        "backup_read_ratio_max": "max", "raw_readback_ratio_max": "max",
        "identical_retained_ratio_max": "max", "first_backup_allowance_min": "fixed",
        "first_control_overhead_bytes_max": "max", "first_local_archive_ratio_max": "max",
    },
    "baseline": {
        "relative_growth_max": "max", "control_noise_allowance_bytes": "max",
        "control_noise_allowance_requests": "max", "cost_increase_usd_max": "max",
    },
    "monthly": {
        "storage_gib": "min", "backup_runs_per_month": "fixed",
        "backup_case_runs": "schedule", "backup_retention_months": "min",
        "backup_input_objects": "min", "retention_runs_per_month": "min",
        "raw_gib_per_month": "min", "raw_input_objects_per_month": "min",
        "raw_retention_months": "min", "raw_replays_per_upload": "min",
        "query_egress_gib": "min", "restore_egress_gib": "min", "other_egress_gib": "min",
        "storage_usd_per_gb_month": "min", "request_usd_per_1000": "prices",
        # Raising a shared billing floor can hide the measured cost delta.
        "minimum_monthly_storage_usd": "fixed",
        "egress_usd_per_gb": "min", "free_egress_multiplier": "max",
        "allowance_storage_gib": "max", "max_monthly_egress_gib": "max",
        "max_monthly_overage_usd": "max", "max_monthly_total_usd": "max",
    },
    "greptime": {
        "max_sst_bytes": "max", "max_requests": "max", "max_cold_get_bytes": "max",
        "max_warm_get_bytes": "max", "relative_growth_max": "max",
        "uncertainty_bytes": "max", "uncertainty_requests": "max",
        "rows_pr": "min", "rows_nightly": "min",
    },
}
TEXT_FIELDS = ("price_source", "price_checked", "workload_note")


def num(value):
    try:
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) and value >= 0)
    except OverflowError:
        return False


def validate_policy(policy):
    """Return None or a reason; every cost-bearing field is required."""
    if not isinstance(policy, dict) or policy.get("$schema") != "storage-cost-budget/1":
        return "policy schema must be storage-cost-budget/1"
    if set(policy) != set(RULES) | set(TEXT_FIELDS) | {"$schema"}:
        return "policy missing or unknown top-level fields"
    for key in TEXT_FIELDS:
        if not isinstance(policy[key], str) or not policy[key].strip():
            return "policy.%s must be a nonempty string" % key
    for section, rules in RULES.items():
        values = policy[section]
        if not isinstance(values, dict) or set(values) != set(rules):
            return "policy.%s missing or unknown fields" % section
        for key, rule in rules.items():
            value = values[key]
            if rule in ("schedule", "prices"):
                keys = CASES if rule == "schedule" else REQ_OPS
                if not isinstance(value, dict) or set(value) != set(keys):
                    return "policy.%s.%s missing or unknown fields" % (section, key)
                numbers = value.values()
            else:
                numbers = (value,)
            if not all(num(v) for v in numbers):
                return "policy.%s.%s must be finite nonnegative numbers, not booleans" % (section, key)
            if (key.endswith("objects") or key.endswith("objects_per_month")
                    or key.startswith("rows_") or key == "backup_runs_per_month"):
                if value <= 0 or int(value) != value:
                    return "policy.%s.%s must be a positive integer" % (section, key)
    a, b, m, g = (policy[s] for s in RULES)
    if a["first_backup_allowance_min"] != 1:
        return "first backup allowance must be exactly one full live copy"
    for section in (a, b, g):
        for key, value in section.items():
            if "ratio" in key or key == "relative_growth_max":
                if value > 1 or (key == "relative_growth_max" and value == 0):
                    return "policy ratio outside bounds: %s" % key
    if b["cost_increase_usd_max"] <= 0:
        return "cost increase threshold must be positive"
    for key in ("storage_gib", "backup_retention_months", "raw_gib_per_month", "raw_retention_months"):
        if m[key] <= 0:
            return "policy.monthly.%s must be positive" % key
    if sum(m["backup_case_runs"].values()) != m["backup_runs_per_month"]:
        return "backup case schedule must sum to backup_runs_per_month"
    return None


def load_policy(path):
    """Load a fully validated policy; shared with both CI budget jobs."""
    with open(path, encoding="utf-8") as handle:
        policy = json.load(handle)
    problem = validate_policy(policy)
    if problem is not None:
        raise ValueError("policy %s: %s" % (path, problem))
    return policy


def compare_policy(candidate, base):
    """Fail closed on missing policy and candidate self-relaxation."""
    problems = []
    for label, policy in (("candidate", candidate), ("base", base)):
        problem = validate_policy(policy)
        if problem:
            problems.append("policy relaxation/invalid %s: %s" % (label, problem))
    if problems:
        return problems
    for section, rules in RULES.items():
        for key, rule in rules.items():
            current, previous = candidate[section][key], base[section][key]
            if rule == "prices":
                relaxed = any(current[op] < previous[op] for op in REQ_OPS)
            elif rule in ("fixed", "schedule"):
                relaxed = current != previous
            else:
                relaxed = current > previous if rule == "max" else current < previous
            if relaxed:
                detail = " must remain unchanged from base policy" if rule == "fixed" else ""
                problems.append("policy relaxation %s.%s%s" % (section, key, detail))
    return problems


def require_numbers(obj, keys, where):
    if not isinstance(obj, dict):
        raise ValueError("missing %s object" % where)
    for key in keys:
        if key not in obj or not num(obj[key]):
            raise ValueError("missing/bad number %s.%s" % (where, key))


def validate_io(io, where):
    if not isinstance(io, dict):
        raise ValueError("missing %s.io" % where)
    for group in ("requests", "errors"):
        require_numbers(io.get(group), REQ_OPS, where + ".io." + group)
        if any(int(io[group][op]) != io[group][op] for op in REQ_OPS):
            raise ValueError("request/error counts must be integers at %s" % where)
    require_numbers(io, ("retries",), where + ".io")
    require_numbers(io.get("bytes"), ("get", "put", "copy"), where + ".io.bytes")
    for side in ("payload", "control"):
        require_numbers(io.get(side), ("requests", "get_bytes", "put_bytes", "copy_bytes"), where + ".io." + side)
    if sum(io[side]["requests"] for side in ("payload", "control")) != sum(io["requests"].values()):
        raise ValueError("request partition mismatch at %s" % where)
    for op in ("get", "put", "copy"):
        if sum(io[side][op + "_bytes"] for side in ("payload", "control")) != io["bytes"][op]:
            raise ValueError("byte partition mismatch at %s" % where)


def validate_case(case, where, backup=False):
    require_numbers(case, ("get_bytes", "put_bytes", "copy_bytes", "new_remote_bytes",
                          "new_payload_bytes", "store_bytes", "input_bytes", "input_objects",
                          "objects", "gen_bytes", "verify_bytes"), where)
    if not isinstance(case.get("case"), str):
        raise ValueError("missing %s.case" % where)
    denominators = ("live_bytes", "live_objects") if backup else ("input_bytes", "input_objects")
    require_numbers(case, denominators, where)
    if any(case[key] <= 0 for key in denominators):
        raise ValueError("zero input denominator at %s" % where)
    if int(case[denominators[1]]) != case[denominators[1]]:
        raise ValueError("input objects must be an integer at %s" % where)
    if backup:
        if case["copy_bytes"] > case["new_remote_bytes"]:
            raise ValueError("backup COPY exceeds gross new storage at %s" % where)
        if (case["input_bytes"], case["input_objects"]) != (case["live_bytes"], case["live_objects"]):
            raise ValueError("backup input aliases disagree at %s" % where)
        if not isinstance(case.get("live_keys"), list) or len(set(case["live_keys"])) != case["live_objects"]:
            raise ValueError("backup input keys disagree at %s" % where)
    validate_io(case.get("io"), where)
    for op in ("get", "put", "copy"):
        if case[op + "_bytes"] != case["io"]["bytes"][op]:
            raise ValueError("top-level IO mismatch at %s.%s" % (where, op))
    require_numbers(case, REQ_OPS + ("retries",), where)
    if (any(case[op] != case["io"]["requests"][op] for op in REQ_OPS)
            or any(case.get(key) != case["io"][key] for key in ("errors", "retries", "payload", "control"))):
        raise ValueError("top-level counters disagree at %s" % where)
    if case["new_payload_bytes"] > case["new_remote_bytes"]:
        raise ValueError("payload exceeds gross storage at %s" % where)
    net = case.get("net_remote_bytes")
    if not isinstance(net, (int, float)) or isinstance(net, bool) or not math.isfinite(net):
        raise ValueError("missing/bad signed net_remote_bytes at %s" % where)


def validate_run(run, where):
    if not isinstance(run, dict) or not isinstance(run.get("cases"), list):
        raise ValueError("missing %s.cases" % where)
    cases = run["cases"]
    if len(cases) != 4 or [c.get("case") if isinstance(c, dict) else None for c in cases] != list(CASES):
        raise ValueError("%s.cases must contain all four ordered backup cases" % where)
    for case in cases:
        validate_case(case, where + "." + case["case"], True)
    aux = run.get("auxiliary")
    if not isinstance(aux, dict) or set(aux) != set(AUXILIARY):
        raise ValueError("missing/unknown %s.auxiliary RAW/CAN cases" % where)
    for name, case in aux.items():
        validate_case(case, where + ".auxiliary." + name)
        if case["case"] != name:
            raise ValueError("auxiliary case identity mismatch at %s" % where)
    for key in ("input_bytes", "input_objects"):
        if aux["raw_fresh"][key] != aux["raw_replay"][key]:
            raise ValueError("RAW fresh/replay input mismatch")
    for name in ("raw_fresh", "raw_replay"):
        raw = aux[name]
        require_numbers(raw, ("acknowledged", "triples_seen"), where + ".auxiliary." + name)
        if raw.get("acknowledged") != 1 or raw.get("triples_seen") != 1 or raw.get("locals_validated") is not True:
            raise ValueError("missing/failed RAW local integrity oracle at %s" % where)
    if aux["can_big"]["input_bytes"] <= aux["can_small"]["input_bytes"]:
        raise ValueError("CAN big/small input ordering invalid")
    oracle = run.get("oracle")
    if not isinstance(oracle, dict) or oracle.get("fresh_restore_hash") is not True:
        raise ValueError("missing/failed %s fresh-restore oracle" % where)
    require_numbers(oracle, ("get_bytes", "gets"), where + ".oracle")
    restores = oracle.get("restores")
    if (not isinstance(restores, list) or len(restores) != 3
            or any(not isinstance(r, dict) for r in restores)
            or {r.get("role") for r in restores} != set(ROLES)):
        raise ValueError("missing/disagreeing %s oracle restore roles" % where)
    for restore in restores:
        if restore.get("file_hashes_match") is not True or restore.get("sst_hashes_match") is not True:
            raise ValueError("%s oracle restore hash failed" % where)
        for key in ("file_hashes", "sst_hashes"):
            if not isinstance(restore.get(key), dict) or not restore[key]:
                raise ValueError("missing %s original hashes" % where)
        validate_io(restore.get("oracle_io"), where + ".oracle." + restore["role"])
        require_numbers(restore, ("oracle_get_bytes", "oracle_gets"), where + ".oracle")
        if (restore["oracle_get_bytes"] != restore["oracle_io"]["bytes"]["get"]
                or restore["oracle_gets"] != restore["oracle_io"]["requests"]["gets"]):
            raise ValueError("oracle IO mismatch")
    if (sum(r["oracle_get_bytes"] for r in restores) != oracle["get_bytes"]
            or sum(r["oracle_gets"] for r in restores) != oracle["gets"]):
        raise ValueError("oracle totals disagree")
    dry, enforced = run.get("retention_dry_run"), run.get("retention_enforced")
    if not isinstance(dry, dict) or not isinstance(enforced, dict):
        raise ValueError("missing %s retention result" % where)
    if where == "before" and dry == enforced == {"retention": "unavailable-in-baseline"}:
        return
    if (set(dry.get("keep", [])) != set(ROLES) or dry.get("delete_ids") != ["victim"]
            or set(enforced.get("remaining", [])) != set(ROLES)):
        raise ValueError("%s retention/oracle roles disagree" % where)
    require_numbers(dry, ("delete_bytes",), where + ".retention_dry_run")
    require_numbers(enforced, ("remote_bytes_before", "remote_bytes_after", "remote_bytes_removed"), where + ".retention_enforced")
    if (enforced["remote_bytes_before"] - enforced["remote_bytes_after"] != enforced["remote_bytes_removed"]
            or dry["delete_bytes"] != enforced["remote_bytes_removed"]):
        raise ValueError("%s retention removal reconciliation failed" % where)
    for name, phase in (("dry", dry), ("enforced", enforced)):
        validate_io(phase.get("io"), where + ".retention." + name)


def monthly_cost(meas, budget, side="current"):
    """Steady-state byte-month model: one seed pool plus scheduled changes.

    SST COPY is retained once per new identity. Non-COPY gross bytes include
    archives/control and remain for each generation's retention duration.
    Free egress uses fixed allowance storage, not measured storage growth.
    """
    run, m = meas[side], budget["monthly"]
    cases = dict(zip(CASES, run["cases"]))
    dataset, raw_volume = m["storage_gib"] * GIB, m["raw_gib_per_month"] * GIB
    schedule = m["backup_case_runs"]
    backup_get = sum(cases[name]["get_bytes"] / cases[name]["live_bytes"] * dataset * count
                     for name, count in schedule.items())
    first = cases[CASES[0]]
    pool = first["copy_bytes"] / first["live_bytes"] * dataset
    changes = sum(cases[name]["copy_bytes"] / cases[name]["live_bytes"] * dataset * count
                  for name, count in schedule.items())
    overhead = sum((cases[name]["new_remote_bytes"] - cases[name]["copy_bytes"])
                   / cases[name]["live_bytes"] * dataset * count for name, count in schedule.items())
    retained = pool + (changes + overhead) * m["backup_retention_months"]
    aux = run["auxiliary"]
    fresh, replay = aux["raw_fresh"], aux["raw_replay"]
    raw_get = (fresh["get_bytes"] + replay["get_bytes"] * m["raw_replays_per_upload"]) / fresh["input_bytes"] * raw_volume
    raw_retained = fresh["new_remote_bytes"] / fresh["input_bytes"] * raw_volume * m["raw_retention_months"]
    requests = {}
    retention_known = "io" in run["retention_enforced"]
    for op in REQ_OPS:
        requests[op] = sum(cases[name]["io"]["requests"][op] / cases[name]["live_objects"]
                           * m["backup_input_objects"] * count for name, count in schedule.items())
        raw_requests = (fresh["io"]["requests"][op] + replay["io"]["requests"][op] * m["raw_replays_per_upload"])
        requests[op] += raw_requests / fresh["input_objects"] * m["raw_input_objects_per_month"]
        if retention_known:
            requests[op] += sum(run[phase]["io"]["requests"][op] for phase in ("retention_dry_run", "retention_enforced")) / first["live_objects"] * m["backup_input_objects"] * m["retention_runs_per_month"]
    assumptions = {name: m[name + "_egress_gib"] * GIB for name in ("query", "restore", "other")}
    egress = backup_get + raw_get + sum(assumptions.values())
    allowance = m["allowance_storage_gib"] * GIB * m["free_egress_multiplier"]
    overage = max(0, egress - allowance)
    storage = dataset + retained + raw_retained
    storage_cost = max(m["minimum_monthly_storage_usd"], storage / GB * m["storage_usd_per_gb_month"])
    request_cost = sum(requests[op] / 1000 * m["request_usd_per_1000"][op] for op in REQ_OPS)
    egress_cost = overage / GB * m["egress_usd_per_gb"]
    return {"backup_get_bytes": backup_get, "raw_get_bytes": raw_get,
            "assumed_egress_bytes": assumptions, "storage_byte_months": storage,
            "retained_byte_months": retained + raw_retained, "storage_gb": storage / GB,
            "egress_bytes": egress, "egress_gib": egress / GIB, "egress_gb": egress / GB,
            "allowance_gb": allowance / GB, "overage_gb": overage / GB,
            "requests": requests, "storage_cost_usd": storage_cost,
            "request_cost_usd": request_cost, "overage_cost_usd": egress_cost,
            "cost_usd": storage_cost + request_cost + egress_cost,
            "retention_request_coverage": "measured" if retention_known else "unavailable in baseline; excluded, NOT measured zero",
            "coverage": "scoped estimate: synthetic backup+RAW workload; CAN checked separately; "
                        "query/restore/other are explicit assumptions, not whole-account telemetry"}


def regression(current, before, budget, where, failures, backup=False):
    """Compare measurements, not authored deltas or individual op swaps."""
    denominators = ("live_bytes", "live_objects", "live_keys") if backup else ("input_bytes", "input_objects")
    if any(current[key] != before[key] for key in denominators):
        raise ValueError("baseline input denominators disagree at %s" % where)
    object_key = denominators[1]
    policy = budget["baseline"]
    def compare(new, old, metric, noise=0, strict_zero=False):
        delta = new - old
        if old == 0:
            if new > (0 if strict_zero else noise):
                failures.append("baseline-zero %s.%s must stay within %s" % (where, metric, 0 if strict_zero else noise))
        elif delta > noise and delta >= old * policy["relative_growth_max"]:
            failures.append("relative growth %s.%s >= %.0f%%" % (where, metric, policy["relative_growth_max"] * 100))
    for side in ("payload", "control"):
        noise = 0 if side == "payload" else policy["control_noise_allowance_bytes"]
        for op in ("get", "put", "copy"):
            key = op + "_bytes"
            compare(current["io"][side][key], before["io"][side][key], side + "." + key,
                    noise, strict_zero=side == "payload" and op == "get")
    old_requests = sum(before["io"]["requests"].values())
    if old_requests == 0:
        compare(current["io"]["payload"]["requests"], 0, "payload.requests", strict_zero=True)
    compare(sum(current["io"]["requests"].values()) / current[object_key],
            old_requests / before[object_key], "requests/input_objects",
            policy["control_noise_allowance_requests"] / current[object_key] if old_requests == 0 else 0)
    for key in ("new_remote_bytes", "new_payload_bytes", "store_bytes"):
        compare(current[key], before[key], key, policy["control_noise_allowance_bytes"] if key != "new_payload_bytes" else 0)
    compare(current["io"]["retries"], before["io"]["retries"], "retries")


def check_measurements(meas, budget, base=None):
    failures = []
    result = {"schema": "storage-cost-check/1", "status": "fail", "failures": failures,
              "monthly": {}, "policy": "bootstrap (new policy; no base policy supplied)" if base is None else "constrained"}
    try:
        problem = validate_policy(budget)
        if problem:
            raise ValueError(problem)
        if base is not None:
            failures.extend(compare_policy(budget, base))
        if not isinstance(meas, dict) or meas.get("schema") != SCHEMA:
            raise ValueError("measurements schema must be %s" % SCHEMA)
        if meas.get("status") != "ok" or meas.get("failures") != []:
            raise ValueError("collector did not report successful complete measurements")
        if type(meas.get("baseline_available")) is not bool:
            raise ValueError("missing explicit baseline_available boolean")
        validate_run(meas.get("current"), "current")
        current, a = meas["current"], budget["absolute"]
        for case in current["cases"]:
            name = case["case"]
            if case["get_bytes"] / case["live_bytes"] > a["backup_read_ratio_max"]:
                failures.append("%s backup read ratio exceeds gate" % name)
            if name == CASES[1] and case["new_remote_bytes"] / case["live_bytes"] > a["identical_retained_ratio_max"]:
                failures.append("identical gross retained ratio exceeds gate")
        first = current["cases"][0]
        allowance = first["live_bytes"] * (1 + a["first_local_archive_ratio_max"]) + a["first_control_overhead_bytes_max"]
        if first["new_remote_bytes"] > allowance or first["copy_bytes"] != first["live_bytes"]:
            failures.append("first backup requires one full copy plus bounded explicit overhead")
        for name, case in current["auxiliary"].items():
            gate = a["raw_readback_ratio_max"] if name.startswith("raw_") else a["backup_read_ratio_max"]
            if case["get_bytes"] / case["input_bytes"] > gate:
                failures.append("%s read ratio exceeds gate" % name)
        replay = current["auxiliary"]["raw_replay"]
        if replay["put_bytes"] != 0 or replay["new_remote_bytes"] != 0:
            failures.append("RAW replay must PUT and retain zero bytes")
        monthly = monthly_cost(meas, budget)
        if not all(num(v) for v in monthly.values() if isinstance(v, (int, float))):
            raise ValueError("monthly projection must remain finite")
        result["monthly"] = monthly
        m = budget["monthly"]
        if monthly["egress_bytes"] > m["max_monthly_egress_gib"] * GIB:
            failures.append("monthly egress exceeds absolute GiB budget")
        if monthly["overage_cost_usd"] > m["max_monthly_overage_usd"]:
            failures.append("monthly overage exceeds absolute USD budget")
        if monthly["cost_usd"] > m["max_monthly_total_usd"]:
            failures.append("monthly total cost exceeds absolute USD budget")
        if meas["baseline_available"]:
            validate_run(meas.get("before"), "before")
            comparison = meas.get("comparison")
            if not isinstance(comparison, dict) or comparison.get("role_hashes_match") is not True:
                raise ValueError("missing/failed baseline role hash comparison")
            if (not isinstance(comparison.get("scenarios"), list)
                    or [d.get("case") if isinstance(d, dict) else None for d in comparison["scenarios"]] != list(CASES)
                    or not isinstance(comparison.get("auxiliary"), dict) or set(comparison["auxiliary"]) != set(AUXILIARY)):
                raise ValueError("missing baseline comparison cases")
            delta_keys = REQ_OPS + ("get_bytes", "put_bytes", "copy_bytes", "new_remote_bytes",
                                   "new_payload_bytes", "objects", "gen_bytes", "store_bytes")
            for delta_case in comparison["scenarios"] + list(comparison["auxiliary"].values()):
                for key in delta_keys:
                    value = delta_case.get(key) if isinstance(delta_case, dict) else None
                    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
                        raise ValueError("missing/nonfinite signed comparison number")
            for old, new in zip(meas["before"]["cases"], current["cases"]):
                regression(new, old, budget, new["case"], failures, True)
            for name in AUXILIARY:
                regression(current["auxiliary"][name], meas["before"]["auxiliary"][name], budget, name, failures)
            def hashes(run):
                return {r["role"]: (r["file_hashes"], r["sst_hashes"]) for r in run["oracle"]["restores"]}
            if hashes(meas["before"]) != hashes(current):
                raise ValueError("baseline/current oracle role hashes disagree")
            if "io" in meas["before"]["retention_enforced"]:
                if comparison.get("retention") is not True:
                    raise ValueError("baseline/current retention roles disagree")
            elif comparison.get("retention") != "current-only (baseline has no retention API)":
                raise ValueError("missing explicit historical retention coverage")
            baseline_monthly = monthly_cost(meas, budget, "before")
            if not all(num(v) for v in baseline_monthly.values() if isinstance(v, (int, float))):
                raise ValueError("baseline monthly projection must remain finite")
            result["baseline_monthly"] = baseline_monthly
            delta = monthly["cost_usd"] - baseline_monthly["cost_usd"]
            result["monthly_cost_delta_usd"] = delta
            if not math.isfinite(delta):
                raise ValueError("nonfinite monthly cost delta")
            if delta >= budget["baseline"]["cost_increase_usd_max"]:
                failures.append("monthly cost increase >= configured USD threshold")
            result["baseline"] = "available; deltas recomputed from measurements"
        else:
            if meas.get("before") is not None or meas.get("baseline") != "absent (--no-baseline)":
                raise ValueError("baseline absence requires explicit --no-baseline semantics")
            result["baseline"] = "explicitly absent; relative comparison unavailable (not zero)"
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        failures.append(str(error))
    result["status"] = "fail" if failures else "pass"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--measurements", required=True)
    parser.add_argument("--budget", required=True)
    parser.add_argument("--baseline-budget")
    parser.add_argument("--summary-out")
    args = parser.parse_args()
    try:
        with open(args.measurements, encoding="utf-8") as handle:
            measurements = json.load(handle)
        budget = load_policy(args.budget)
        base = load_policy(args.baseline_budget) if args.baseline_budget else None
        result = check_measurements(measurements, budget, base)
    except (OSError, ValueError, TypeError) as error:
        result = {"schema": "storage-cost-check/1", "status": "fail", "failures": [str(error)], "monthly": {}}
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")
    monthly = result["monthly"]
    lines = ["# Storage cost budget: " + result["status"].upper(), "",
             "policy: " + result.get("policy", "invalid"),
             "baseline: " + result.get("baseline", "unavailable"),
             "monthly: %.3f GiB egress; storage $%.4f + requests $%.4f + overage $%.4f = $%.4f" % (
                 monthly.get("egress_gib", 0), monthly.get("storage_cost_usd", 0),
                 monthly.get("request_cost_usd", 0), monthly.get("overage_cost_usd", 0), monthly.get("cost_usd", 0)),
             monthly.get("coverage", "invalid measurements; no estimate"), ""]
    lines.extend("- " + failure for failure in result["failures"])
    summary = "\n".join(lines) + "\n"
    if args.summary_out:
        with open(args.summary_out, "w", encoding="utf-8") as handle:
            handle.write(summary)
    else:
        sys.stderr.write(summary)
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
