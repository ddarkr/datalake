#!/usr/bin/env python3
"""AI coding-activity normalization: pure functions, stdlib only.

Accepts already-parsed naive UTC datetimes, raw trace/log fields and
normalized metric rows; all SQL/OTLP I/O lives elsewhere.

Key semantics:
- Native Claude signals outrank OpenLIT per (client, session, field); never
  summed. Native modified LOC never implies accepted LOC (accepted/rejected
  are added-lines-only, from per-edit decision+LOC or session totals).
- Delta metric points are increments (stream + timestamp deduped); cumulative
  points are differenced in time order per (stream, start_ns), scoped by
  session. Missing start/temporality is unknown, never guessed. Session
  totals are latest cumulative snapshots; a first value or interval that
  straddles UTC days is session-truthful but not day-allocatable, so the
  straddled days stay NULL instead of reporting a partial daily total.
- Session fields prefer latest OpenLIT session totals; daily fields prefer
  per-event/counter increments. A lone session total is allocated to a day
  only when the whole session fits in one UTC day.
- session_wall_seconds is the union of reported session/root intervals
  (coding_agent.session, invoke_agent roots) clipped per UTC day; native
  user/cli active counters stay separate; missing stays NULL.
- Repo is a validated coding_agent.repository.id SHA256 only (hashed before
  the exporter queue; raw URLs never reach here); branch/outcome only when
  the producer explicitly emitted them; generic Codex tool decisions are not
  edits.

Source semantic limits: OpenCode/OMP emit none of the requested native
activity fields; native Codex emits no LOC/commit/PR/outcome counters;
native Claude emits no accepted/rejected LOC, no branch, no session outcome;
OpenLIT merged/committed outcomes are heuristics without forge context, and
its accepted/rejected LOC covers added lines only.
"""
import json
from datetime import datetime, timedelta, timezone

ACTIVITY_COUNT_FIELDS = (
    "subagents",
    "lines_added",
    "lines_removed",
    "lines_accepted",
    "lines_rejected",
    "edit_accept_count",
    "edit_reject_count",
    "commit_count",
    "pr_count",
)

ACTIVITY_VALUE_FIELDS = (
    "active_user_seconds",
    "active_cli_seconds",
    "session_wall_seconds",
)
ACTIVITY_TRACE_ATTRIBUTES = (
    "coding_agent.session.id",
    "coding_agent.session.subagent_count",
    "coding_agent.session.lines.added",
    "coding_agent.session.lines.removed",
    "coding_agent.session.lines.accepted",
    "coding_agent.session.lines.rejected",
    "coding_agent.session.edit.accept_count",
    "coding_agent.session.edit.reject_count",
    "coding_agent.session.commit_count",
    "coding_agent.session.pr_count",
    "coding_agent.session.duration_ms",
    "coding_agent.session.outcome",
    "coding_agent.agent.id",
    "coding_agent.agent.parent_id",
    "coding_agent.subagent.type",
    "coding_agent.subagent.status",
    "coding_agent.subagent.duration_ms",
    "coding_agent.edit.decision",
    "coding_agent.edit.lines.added",
    "coding_agent.edit.lines.removed",
    "coding_agent.edit.tool.name",
    "coding_agent.edit.language",
    "coding_agent.client",
    "coding_agent.repository.id",
    "service.name",
    "service_name",
    "session.id",
    "conversation.id",
    "gen_ai.conversation.id",
    "turn.id",
    "prompt.id",
    "event.name",
    "event.timestamp",
    "event.sequence",
    "agent_type",
    "agent.source",
    "is_built_in",
    "is_async",
    "total_tool_uses",
    "duration_ms",
    "model",
    "type",
    "tool_name",
    "tool_namespace",
    "call_id",
    "tool_use_id",
    "decision",
    "source",
    "language",
    "query_source",
    "client",
    "vcs.ref.head.name",
    "vcs.ref.head.revision",
)

_SESSION_KEYS = ("coding_agent.session.id", "session.id", "conversation.id",
                 "gen_ai.conversation.id")
_CLIENT_KEYS = ("coding_agent.client", "client")
_CODEX_PREFIX = "codex."
_CLAUDE_CLIENTS = frozenset({"claude-code", "claude-code-desktop"})
_OMP_CLIENTS = frozenset({"oh-my-pi"})
# ponytail: explicit coding_agent.client/client wins verbatim. Fallback maps
# only verified names (Claude pair, oh-my-pi, codex.* namespace); any other
# service.name stays provenance, never identity (else "unknown").

_SESSION_ATTR_FIELDS = {
    "coding_agent.session.subagent_count": "subagents",
    "coding_agent.session.lines.added": "lines_added",
    "coding_agent.session.lines.removed": "lines_removed",
    "coding_agent.session.lines.accepted": "lines_accepted",
    "coding_agent.session.lines.rejected": "lines_rejected",
    "coding_agent.session.edit.accept_count": "edit_accept_count",
    "coding_agent.session.edit.reject_count": "edit_reject_count",
    "coding_agent.session.commit_count": "commit_count",
    "coding_agent.session.pr_count": "pr_count",
}

_EDIT_TOOLS = frozenset({"Edit", "Write", "NotebookEdit"})
_ACCEPT = frozenset({"accept", "auto_accepted"})

_NATIVE_LOC = "native:claude_code.lines_of_code.count"
_NATIVE_DEC = "native:claude_code.code_edit_tool.decision"
_NATIVE_DEC_EVENT = "native:claude_code.tool_decision"
_NATIVE_SUB = "native:claude_code.subagent_completed"
_NATIVE_ACTIVE = "native:claude_code.active_time.total"
_NATIVE_COMMIT = "native:claude_code.commit.count"
_NATIVE_PR = "native:claude_code.pull_request.count"
_OPENLIT_TOTAL = "openlit:coding_agent.session (latest total)"
_OPENLIT_TOTAL_DAY = "openlit:coding_agent.session total (single-day session)"
_OPENLIT_LOC = "openlit:coding_agent.lines_of_code.count"
_OPENLIT_DEC = "openlit:coding_agent.code_edit_tool.decision"
_OPENLIT_EDIT_SPAN = "openlit:coding_agent.edit.decision spans"
_OPENLIT_SUB_SPAN = "openlit:coding_agent.subagent spans"
_OPENLIT_COMMIT = "openlit:coding_agent.commit.count"
_OPENLIT_PR = "openlit:coding_agent.pull_request.count"
_OPENLIT_COMMIT_SPAN = "openlit:coding_agent.git.commit spans"
_OPENLIT_PR_SPAN = "openlit:coding_agent.git.pull_request spans"
_WALL_SRC = "session/root interval union"
_WALL_NAMES = frozenset({"coding_agent.session", "invoke_agent"})


def _as_dt(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _as_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _as_scalar(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (str, int, float)):
        return value
    return None


def _as_count(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            value = float(text)
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        # ponytail: int(float) crashes on NaN/Inf; explicit zero stays 0.
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            return None
        value = int(value)
        return value if value >= 0 else None
    return None


def _metric_value(value):
    # ponytail: metric values stay float-safe; explicit zero is a real 0.
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            value = float(text)
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            return None
        return value
    return None

def _start_ns(value):
    # ponytail: missing/zero start_ns stays unknown, never guessed.
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed > 0 else None
    return None


def _freeze(value):
    try:
        hash(value)
        return value
    except TypeError:
        pass
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(v) for v in value)
    return repr(value)


def _dim(stream, name):
    if isinstance(stream, dict):
        return stream.get(name)
    if isinstance(stream, (list, tuple, set, frozenset)):
        for item in stream:
            if isinstance(item, (list, tuple)) and len(item) == 2 and item[0] == name:
                return item[1]
    return None


def _span_attrs(row):
    out = {}
    nested = row.get("span_attributes")
    if isinstance(nested, dict):
        out.update(nested)
    prefix = "span_attributes."
    for key, value in row.items():
        if key.startswith(prefix):
            out[key[len(prefix):]] = value
    for key in ACTIVITY_TRACE_ATTRIBUTES:
        if key not in out and key in row:
            out[key] = row[key]
    return out


def _fallback_client(row, attrs, hint=None):
    # ponytail: codex.* namespace wins even over a known service.name:
    # Codex service.name is an overridable originator value, so a Codex
    # row may carry another product's name. Only exact verified names map.
    if (hint or "")[:6] == _CODEX_PREFIX:
        return "codex"
    service = _as_scalar(attrs.get("service.name"))
    if service is None:
        service = _as_scalar(attrs.get("service_name"))
    if service is None and isinstance(row, dict):
        service = _as_scalar(row.get("service.name"))
    if service is None and isinstance(row, dict):
        service = _as_scalar(row.get("service_name"))
    if service is not None and str(service) != "":
        text = str(service)
        if text in _CLAUDE_CLIENTS:
            return "claude-code"
        if text in _OMP_CLIENTS:
            return text
    return "unknown"


def _identity(attrs, row=None, hint=None):
    for key in _SESSION_KEYS:
        value = _as_scalar(attrs.get(key))
        if value is not None and str(value) != "":
            session_id = str(value)
            break
    else:
        return None, None
    for key in _CLIENT_KEYS:
        value = _as_scalar(attrs.get(key))
        if value is not None and str(value) != "":
            return str(value), session_id
    return _fallback_client(row or {}, attrs, hint), session_id


def _repo_id(value):
    # ponytail: hashed-before-queue coding_agent.repository.id only; Main
    # validates the 64-hex SHA256 shape, anything else is unavailable.
    value = _as_scalar(value)
    if value is None:
        return None
    text = str(value).strip().lower()
    if len(text) != 64:
        return None
    for ch in text:
        if ch not in "0123456789abcdef":
            return None
    return text


def _day_start(ts):
    return datetime(ts.year, ts.month, ts.day)


def _merge_intervals(intervals):
    ordered = sorted(intervals)
    merged = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1][1] = end
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def _clip_union_by_day(intervals):
    per_day = {}
    for start, end in _merge_intervals(intervals):
        day = _day_start(start)
        cursor = start
        while True:
            nxt = day + timedelta(days=1)
            if end <= nxt:
                per_day[day] = per_day.get(day, 0.0) + (end - cursor).total_seconds()
                break
            per_day[day] = per_day.get(day, 0.0) + (nxt - cursor).total_seconds()
            cursor = nxt
            day = nxt
    return per_day


def _new_session(client, session_id):
    return {
        "client": client,
        "session_id": session_id,
        "start": None,
        "end": None,
        "inc": {"n_metric": {}, "n_event": {}, "o_metric": {}, "o_span": {}},
        "total": {},
        # ponytail: cumulative streams keep session-scoped snapshot totals
        # apart from day-allocatable increments: a baseline or interval that
        # straddles UTC days still counts session-side, but the straddled
        # days are poisoned so daily totals stay NULL instead of partial.
        "csum": {"n_metric": {}, "o_metric": {}},
        "csum_active": {},
        "cpoison": {},
        "active": {"active_user_seconds": {}, "active_cli_seconds": {}},
        "intervals": [],
        "repo": None,
        "branch": None,
        "outcome": None,
    }


def _touch(session, ts, end=None):
    if ts is None:
        return
    if session["start"] is None or ts < session["start"]:
        session["start"] = ts
    last = end if end is not None and end > ts else ts
    if session["end"] is None or last > session["end"]:
        session["end"] = last


def _bump(session, bucket, field, day, amount, prov):
    entry = session["inc"][bucket].setdefault(field, [{}, prov])
    entry[0][day] = entry[0].get(day, 0) + amount
    entry[1] = prov


def _day_range(start_day, end_day):
    # ponytail: intersected UTC days between two day-starts, inclusive, so
    # a cross-midnight interval poisons intervening days too, not endpoints.
    days = []
    cursor = start_day
    while cursor <= end_day:
        days.append(cursor)
        cursor = cursor + timedelta(days=1)
    return days


def _csum_bump(session, bucket, field, delta_total, day, amount, prov, days):
    entry = session["csum"][bucket].setdefault(field, [0, prov])
    entry[0] += delta_total
    entry[1] = prov
    if not days:
        _bump(session, bucket, field, day, amount, prov)
    else:
        poisoned = session["cpoison"].setdefault((bucket, field), set())
        for poisoned_day in days:
            poisoned.add(poisoned_day)


def _csum_active_bump(session, field, delta_total, day, amount, days):
    session["csum_active"][field] = session["csum_active"].get(field, 0) + delta_total
    if not days:
        slot = session["active"][field]
        slot[day] = slot.get(day, 0) + amount
    else:
        poisoned = session["cpoison"].setdefault(("active", field), set())
        for poisoned_day in days:
            poisoned.add(poisoned_day)


def _metric_targets(instrument, typ, decision):
    if instrument == "claude_code.lines_of_code.count":
        if typ == "added":
            return [("lines_added", "n_metric", _NATIVE_LOC)]
        if typ == "removed":
            return [("lines_removed", "n_metric", _NATIVE_LOC)]
    elif instrument == "coding_agent.lines_of_code.count":
        out = []
        if typ == "added":
            out.append(("lines_added", "o_metric", _OPENLIT_LOC))
            if decision in _ACCEPT:
                out.append(("lines_accepted", "o_metric", _OPENLIT_LOC))
            elif decision == "reject":
                out.append(("lines_rejected", "o_metric", _OPENLIT_LOC))
        elif typ == "removed":
            out.append(("lines_removed", "o_metric", _OPENLIT_LOC))
        return out
    elif instrument == "claude_code.code_edit_tool.decision":
        if decision == "accept":
            return [("edit_accept_count", "n_metric", _NATIVE_DEC)]
        if decision == "reject":
            return [("edit_reject_count", "n_metric", _NATIVE_DEC)]
    elif instrument == "coding_agent.code_edit_tool.decision":
        if decision in _ACCEPT:
            return [("edit_accept_count", "o_metric", _OPENLIT_DEC)]
        if decision == "reject":
            return [("edit_reject_count", "o_metric", _OPENLIT_DEC)]
    elif instrument == "claude_code.commit.count":
        return [("commit_count", "n_metric", _NATIVE_COMMIT)]
    elif instrument == "claude_code.pull_request.count":
        return [("pr_count", "n_metric", _NATIVE_PR)]
    elif instrument == "coding_agent.commit.count":
        return [("commit_count", "o_metric", _OPENLIT_COMMIT)]
    elif instrument == "coding_agent.pull_request.count":
        return [("pr_count", "o_metric", _OPENLIT_PR)]
    return []


def summarize_activity(trace_rows, log_rows, metric_rows):
    """Normalize activity rows into (sessions, daily).

    sessions: dict keyed by (client, session_id) with start/end, every
      count/value field (None when unavailable), repo (validated
      coding_agent.repository.id SHA256), branch,
      outcome and per-field activity_sources provenance.
    daily: list of dicts with day_start (naive UTC midnight), client, every
      count/value field and activity_sources.
    """
    sessions = {}
    wall_pool = {}

    def session_for(client, session_id):
        key = (client, session_id)
        session = sessions.get(key)
        if session is None:
            session = _new_session(client, session_id)
            sessions[key] = session
        return session

    # Traces: dedupe retransmitted/updated spans by identity, latest end wins.
    # Rows without trace/span IDs carry no stable identity, so they are
    # skipped rather than counted under an invented row-index identity.
    deduped = {}
    for row in trace_rows or []:
        if not isinstance(row, dict):
            continue
        trace_id = row.get("trace_id")
        span_id = row.get("span_id")
        if trace_id is None or span_id is None:
            continue
        ts = _as_dt(row.get("timestamp"))
        end = _as_dt(row.get("timestamp_end"))
        order = (end or ts or datetime.min, ts or datetime.min)
        key = (trace_id, span_id)
        if key not in deduped or order >= deduped[key][0]:
            deduped[key] = (order, row)
    ordered_traces = [row for _, row in sorted(deduped.values(), key=lambda kv: kv[0])]

    subagent_spans = set()
    commit_spans = set()
    pr_spans = set()
    for index, row in enumerate(ordered_traces):
        attrs = _span_attrs(row)
        client, session_id = _identity(attrs, row=row, hint=row.get("span_name"))
        if session_id is None:
            continue
        session = session_for(client, session_id)
        ts = _as_dt(row.get("timestamp"))
        end = _as_dt(row.get("timestamp_end"))
        _touch(session, ts, end)
        name = row.get("span_name")
        order = (end or ts or datetime.min, ts or datetime.min, index)
        if name in _WALL_NAMES and ts is not None and end is not None and end > ts:
            session["intervals"].append((ts, end))
        if name == "coding_agent.session":
            for attr, field in _SESSION_ATTR_FIELDS.items():
                value = _as_count(attrs.get(attr))
                if value is not None:
                    prev = session["total"].get(field)
                    if prev is None or order >= prev[0]:
                        session["total"][field] = (order, value)
            outcome = _as_scalar(attrs.get("coding_agent.session.outcome"))
            if outcome is not None and str(outcome) != "":
                if session["outcome"] is None or order >= session["outcome"][0]:
                    session["outcome"] = (order, str(outcome))
        elif name == "coding_agent.subagent":
            agent = _as_scalar(attrs.get("coding_agent.agent.id"))
            if agent is not None:
                ident = (client, session_id, "agent", str(agent))
            else:
                ident = (client, session_id, "span", row.get("trace_id"), row.get("span_id"))
            if ts is not None and ident not in subagent_spans:
                subagent_spans.add(ident)
                _bump(session, "o_span", "subagents", _day_start(ts), 1, _OPENLIT_SUB_SPAN)
        elif name == "coding_agent.edit.decision":
            if ts is None:
                continue
            day = _day_start(ts)
            decision = attrs.get("coding_agent.edit.decision")
            added = _as_count(attrs.get("coding_agent.edit.lines.added"))
            removed = _as_count(attrs.get("coding_agent.edit.lines.removed"))
            # ponytail: explicit zero LOC is a real 0 (bump preserves it);
            # missing LOC attributes stay NULL (no bump).
            if added is not None:
                _bump(session, "o_span", "lines_added", day, added, _OPENLIT_EDIT_SPAN)
                if decision in _ACCEPT:
                    _bump(session, "o_span", "lines_accepted", day, added, _OPENLIT_EDIT_SPAN)
                elif decision == "reject":
                    _bump(session, "o_span", "lines_rejected", day, added, _OPENLIT_EDIT_SPAN)
            if removed is not None:
                _bump(session, "o_span", "lines_removed", day, removed, _OPENLIT_EDIT_SPAN)
            if decision in _ACCEPT:
                _bump(session, "o_span", "edit_accept_count", day, 1, _OPENLIT_EDIT_SPAN)
            elif decision == "reject":
                _bump(session, "o_span", "edit_reject_count", day, 1, _OPENLIT_EDIT_SPAN)
        elif name == "coding_agent.git.commit":
            if ts is None:
                continue
            sha = _as_scalar(attrs.get("vcs.ref.head.revision"))
            if sha is not None and str(sha) != "":
                ident = (client, session_id, "sha", str(sha))
            else:
                ident = (client, session_id, "span", row.get("trace_id"), row.get("span_id"))
            if ident not in commit_spans:
                commit_spans.add(ident)
                _bump(session, "o_span", "commit_count", _day_start(ts), 1, _OPENLIT_COMMIT_SPAN)
        elif name == "coding_agent.git.pull_request":
            if ts is None:
                continue
            ident = (client, session_id, "span", row.get("trace_id"), row.get("span_id"))
            if ident not in pr_spans:
                pr_spans.add(ident)
                _bump(session, "o_span", "pr_count", _day_start(ts), 1, _OPENLIT_PR_SPAN)
        repo = _repo_id(attrs.get("coding_agent.repository.id"))
        if repo is not None:
            if session["repo"] is None or order >= session["repo"][0]:
                session["repo"] = (order, repo)
        branch = _as_scalar(attrs.get("vcs.ref.head.name"))
        if branch is not None and str(branch) != "":
            if session["branch"] is None or order >= session["branch"][0]:
                session["branch"] = (order, str(branch))

    # Logs: subagent completions and Claude edit decisions with stable dedupe.
    subagent_events = set()
    decision_events = set()
    for row in log_rows or []:
        if not isinstance(row, dict):
            continue
        attrs = dict(_as_dict(row.get("resource_attributes")))
        attrs.update(_as_dict(row.get("log_attributes")))
        for key in ACTIVITY_TRACE_ATTRIBUTES:
            if key not in attrs and key in row:
                attrs[key] = row[key]
        client, session_id = _identity(attrs, row=row, hint=attrs.get("event.name"))
        if session_id is None:
            continue
        session = session_for(client, session_id)
        ts = _as_dt(row.get("timestamp"))
        if ts is None:
            ts = _as_dt(attrs.get("event.timestamp"))
        _touch(session, ts)
        event = attrs.get("event.name")
        if event in ("subagent_completed", "claude_code.subagent_completed") and ts is not None:
            ident = (client, session_id, ts, attrs.get("event.sequence"),
                     row.get("trace_id"), row.get("span_id"))
            if ident not in subagent_events:
                subagent_events.add(ident)
                _bump(session, "n_event", "subagents", _day_start(ts), 1, _NATIVE_SUB)
            continue
        if "tool_use_id" in attrs and attrs.get("tool_use_id") is not None:
            if attrs.get("tool_namespace") is not None or attrs.get("call_id") is not None:
                continue  # generic Codex permission decision, not an edit
            tool = attrs.get("tool_name")
            if tool is not None and str(tool) not in _EDIT_TOOLS:
                continue
            decision = attrs.get("decision")
            if decision not in ("accept", "reject"):
                continue
            if ts is None:
                continue
            ident = (client, session_id, str(attrs.get("tool_use_id")), decision)
            if ident in decision_events:
                continue
            decision_events.add(ident)
            field = "edit_accept_count" if decision == "accept" else "edit_reject_count"
            _bump(session, "n_event", field, _day_start(ts), 1, _NATIVE_DEC_EVENT)
            continue
        repo = _repo_id(attrs.get("coding_agent.repository.id"))
        if repo is not None and ts is not None:
            order = (ts, ts, 0)
            if session["repo"] is None or order >= session["repo"][0]:
                session["repo"] = (order, repo)
        branch = _as_scalar(attrs.get("vcs.ref.head.name"))
        if branch is not None and str(branch) != "" and ts is not None:
            order = (ts, ts, 0)
            if session["branch"] is None or order >= session["branch"][0]:
                session["branch"] = (order, str(branch))

    # Metrics: delta points are increments; cumulative points are differenced
    # per (stream, start_ns) in time order. Unknown temporality/start is
    # skipped, never guessed.
    delta_points = {}
    cumulative = {}
    for seq, row in enumerate(metric_rows or []):
        if not isinstance(row, dict):
            continue
        instrument = row.get("instrument")
        if not instrument:
            continue
        session_id = row.get("session_id")
        if session_id is None or str(session_id) == "":
            continue
        raw_client = row.get("client")
        explicit = row.get("client_explicit")
        if raw_client is not None and str(raw_client) != "" and explicit:
            client = str(raw_client)
        elif str(instrument)[:6] == _CODEX_PREFIX and not explicit:
            # ponytail: codex.* instrument is the producer marker and wins
            # over any service.name (Codex service.name is overridable,
            # even to another known name).
            client = "codex"
        elif raw_client is not None and str(raw_client) in _CLAUDE_CLIENTS:
            client = "claude-code"
        elif raw_client is not None and str(raw_client) in _OMP_CLIENTS:
            client = str(raw_client)
        else:
            client = "unknown"
        session_id = str(session_id)
        ts = _as_dt(row.get("timestamp"))
        if ts is None:
            continue
        session = session_for(client, session_id)
        _touch(session, ts)
        # ponytail: metric-only sessions carry their git context on the row
        # (collector copies the hashed id/branch to datapoint attrs); same
        # explicit handling as trace/log, never raw URLs, never inferred.
        repo = _repo_id(row.get("coding_agent.repository.id"))
        if repo is None:
            repo = _repo_id(_dim(row.get("stream"), "coding_agent.repository.id"))
        if repo is not None:
            order = (ts, ts, seq)
            if session["repo"] is None or order >= session["repo"][0]:
                session["repo"] = (order, repo)
        branch = _as_scalar(row.get("vcs.ref.head.name"))
        if branch is None:
            branch = _as_scalar(_dim(row.get("stream"), "vcs.ref.head.name"))
        if branch is not None and str(branch) != "":
            order = (ts, ts, seq)
            if session["branch"] is None or order >= session["branch"][0]:
                session["branch"] = (order, str(branch))
        value = _metric_value(row.get("value"))
        if value is None:
            continue
        temporality = row.get("temporality")
        if isinstance(temporality, bool):
            continue
        stream = row.get("stream")
        frozen = _freeze(stream)
        typ = row.get("type")
        if typ is None:
            typ = _dim(stream, "type")
        decision = row.get("decision")
        if decision is None:
            decision = _dim(stream, "decision")
        if instrument == "claude_code.active_time.total":
            if temporality not in (1, 2) or typ not in ("user", "cli"):
                continue
            field = "active_user_seconds" if typ == "user" else "active_cli_seconds"
            targets = [(field, "active", _NATIVE_ACTIVE)]
        else:
            targets = _metric_targets(instrument, typ, decision)
            if not targets:
                continue
            if temporality not in (1, 2):
                continue
            value = _as_count(value)
            if value is None:
                continue
        if temporality == 2:
            start_ns = _start_ns(row.get("start_ns"))
            if start_ns is None:
                continue
        if temporality == 1:
            # ponytail: same (stream, timestamp) retransmission is one
            # increment; arrival order wins deterministically.
            key = (client, session_id, instrument, frozen, ts)
            delta_points[key] = (targets, value, session, ts)
        else:
            key = (client, session_id, instrument, frozen, start_ns)
            cumulative.setdefault(key, []).append((ts, value, seq))
            cumulative[key + ("targets",)] = targets
            cumulative[key + ("session",)] = session

    for (client, session_id, instrument, frozen, ts), (targets, value, session, ts2) in delta_points.items():
        day = _day_start(ts2)
        for field, bucket, prov in targets:
            if bucket == "active":
                slot = session["active"][field]
                slot[day] = slot.get(day, 0) + value
            else:
                _bump(session, bucket, field, day, value, prov)

    for key, points in list(cumulative.items()):
        if not isinstance(key, tuple) or len(key) != 5:
            continue
        client, session_id, instrument, frozen, start_ns = key
        targets = cumulative.get(key + ("targets",))
        session = cumulative.get(key + ("session",))
        if not targets or session is None:
            continue
        latest = {}
        for ts, value, seq in points:
            if ts not in latest or seq >= latest[ts][0]:
                latest[ts] = (seq, value)
        ordered = sorted(latest.items())
        try:
            start_day = datetime.fromtimestamp(
                start_ns // 1_000_000_000, timezone.utc).replace(tzinfo=None).date()
        except (OverflowError, OSError, ValueError):
            continue
        prev = None
        prev_ts = None
        start_ds = datetime(start_day.year, start_day.month, start_day.day)
        for position, (ts, (_, value)) in enumerate(ordered):
            day = _day_start(ts)
            if position == 0:
                if start_day == ts.date():
                    increment = value
                    poison_days = []
                else:
                    # ponytail: counter scoped by session, so the baseline
                    # value counts session-side even though its UTC-day split
                    # is unknown; the point day and the counter-start day are
                    # poisoned so daily stays NULL instead of partial.
                    prev = value
                    prev_ts = ts
                    poison_days = [day, start_ds]
                    for field, bucket, prov in targets:
                        if bucket == "active":
                            _csum_active_bump(session, field, value, day, 0, poison_days)
                        else:
                            _csum_bump(session, bucket, field, value, day, 0, prov, poison_days)
                    continue
            else:
                increment = value - prev if value >= prev else value
                prev_day = _day_start(prev_ts)
                if increment == 0:
                    # ponytail: a zero delta crossing midnight is an
                    # unambiguous 0 (normal overnight idle exporter); it must
                    # not poison an otherwise known day.
                    poison_days = []
                elif prev_day == day:
                    poison_days = []
                else:
                    poison_days = _day_range(prev_day, day)
            prev = value
            prev_ts = ts
            for field, bucket, prov in targets:
                if bucket == "active":
                    _csum_active_bump(session, field, increment, day, increment, poison_days)
                else:
                    _csum_bump(session, bucket, field, increment, day, increment, prov, poison_days)

    out_sessions = {}
    daily_cells = {}
    global_poison = {}
    for session in sessions.values():
        for poison_key, days in session["cpoison"].items():
            bucket, field = poison_key
            for day in days:
                global_poison.setdefault((session["client"], day, bucket, field), True)
    for key, session in sessions.items():
        client, session_id = key
        if session["intervals"]:
            wall_pool.setdefault(client, []).extend(session["intervals"])
        record = {
            "client": client,
            "session_id": session_id,
            "start": session["start"],
            "end": session["end"],
        }
        prov = {}
        single_day = None
        if session["start"] is not None and session["end"] is not None:
            if _day_start(session["start"]) == _day_start(session["end"]):
                single_day = _day_start(session["start"])

        def poisoned_day(bucket, field, day):
            return (client, day, bucket, field) in global_poison

        for field in ACTIVITY_COUNT_FIELDS:
            csum_entry = None
            for bucket in ("n_metric", "o_metric"):
                if field in session["csum"][bucket]:
                    csum_entry = (bucket, session["csum"][bucket][field])
                    break
            if csum_entry is not None:
                bucket, (total, cprov) = csum_entry
                record[field] = total
                prov[field] = cprov
                inc_entry = session["inc"][bucket].get(field)
                if inc_entry:
                    for day, amount in inc_entry[0].items():
                        if poisoned_day(bucket, field, day):
                            daily_cells.setdefault((client, day), {})
                            continue
                        cell = daily_cells.setdefault((client, day), {})
                        entry = cell.setdefault(field, [0, set()])
                        entry[0] += amount
                        entry[1].add(inc_entry[1])
                for (c, day, b, f) in global_poison:
                    if c == client and b == bucket and f == field:
                        daily_cells.setdefault((client, day), {})
                continue
            native = session["inc"]["n_metric"].get(field) or session["inc"]["n_event"].get(field)
            if native and native[0]:
                record[field] = sum(native[0].values())
                prov[field] = native[1]
                for day, amount in native[0].items():
                    cell = daily_cells.setdefault((client, day), {})
                    entry = cell.setdefault(field, [0, set()])
                    entry[0] += amount
                    entry[1].add(native[1])
                continue
            total = session["total"].get(field)
            opened = session["inc"]["o_metric"].get(field) or session["inc"]["o_span"].get(field)
            if total is not None:
                record[field] = total[1]
                prov[field] = _OPENLIT_TOTAL
            elif opened and opened[0]:
                record[field] = sum(opened[0].values())
                prov[field] = opened[1]
            else:
                record[field] = None
                continue
            if opened and opened[0]:
                for day, amount in opened[0].items():
                    cell = daily_cells.setdefault((client, day), {})
                    entry = cell.setdefault(field, [0, set()])
                    entry[0] += amount
                    entry[1].add(opened[1])
            elif total is not None and single_day is not None:
                cell = daily_cells.setdefault((client, single_day), {})
                entry = cell.setdefault(field, [0, set()])
                entry[0] += total[1]
                entry[1].add(_OPENLIT_TOTAL_DAY)
                prov[field] = _OPENLIT_TOTAL_DAY
        for field in ("active_user_seconds", "active_cli_seconds"):
            csum_total = session["csum_active"].get(field)
            if csum_total is not None:
                record[field] = csum_total
                prov[field] = _NATIVE_ACTIVE
                slot = session["active"][field]
                for day, amount in slot.items():
                    if poisoned_day("active", field, day):
                        daily_cells.setdefault((client, day), {})
                        continue
                    cell = daily_cells.setdefault((client, day), {})
                    entry = cell.setdefault(field, [0, set()])
                    entry[0] += amount
                    entry[1].add(_NATIVE_ACTIVE)
                for (c, day, b, f) in global_poison:
                    if c == client and b == "active" and f == field:
                        daily_cells.setdefault((client, day), {})
                continue
            slot = session["active"][field]
            if slot:
                record[field] = sum(slot.values())
                prov[field] = _NATIVE_ACTIVE
                for day, amount in slot.items():
                    cell = daily_cells.setdefault((client, day), {})
                    entry = cell.setdefault(field, [0, set()])
                    entry[0] += amount
                    entry[1].add(_NATIVE_ACTIVE)
            else:
                record[field] = None
        if session["intervals"]:
            record["session_wall_seconds"] = sum(
                _clip_union_by_day(session["intervals"]).values())
            prov["session_wall_seconds"] = _WALL_SRC
        else:
            record["session_wall_seconds"] = None
        record["repo"] = session["repo"][1] if session["repo"] else None
        if record["repo"] is not None:
            prov["repo"] = "coding_agent.repository.id"
        record["branch"] = session["branch"][1] if session["branch"] else None
        if record["branch"] is not None:
            prov["branch"] = "vcs.ref.head.name"
        record["outcome"] = session["outcome"][1] if session["outcome"] else None
        if record["outcome"] is not None:
            prov["outcome"] = "openlit:coding_agent.session.outcome"
        record["activity_sources"] = prov
        out_sessions[key] = record

    for client, intervals in wall_pool.items():
        for day, seconds in _clip_union_by_day(intervals).items():
            cell = daily_cells.setdefault((client, day), {})
            entry = cell.setdefault("session_wall_seconds", [0.0, set()])
            entry[0] = seconds
            entry[1].add(_WALL_SRC)

    daily = []
    for (client, day), cell in sorted(daily_cells.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        record = {"day_start": day, "client": client}
        sources = {}
        for field in ACTIVITY_COUNT_FIELDS:
            poisoned = False
            for bucket in ("n_metric", "o_metric"):
                if (client, day, bucket, field) in global_poison:
                    poisoned = True
                    break
            if poisoned:
                record[field] = None
                continue
            if field in cell:
                record[field] = cell[field][0]
                sources[field] = " + ".join(sorted(cell[field][1]))
            else:
                record[field] = None
        for field in ACTIVITY_VALUE_FIELDS:
            if field in ("active_user_seconds", "active_cli_seconds"):
                if (client, day, "active", field) in global_poison:
                    record[field] = None
                    continue
            if field in cell:
                record[field] = cell[field][0]
                sources[field] = " + ".join(sorted(cell[field][1]))
            else:
                record[field] = None
        record["activity_sources"] = sources
        daily.append(record)
    return out_sessions, daily
