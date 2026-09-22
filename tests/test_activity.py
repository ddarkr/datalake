"""Activity normalization boundary regressions: pure function, stdlib only."""
import importlib.util
import os
import unittest
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(os.path.dirname(HERE), "scripts")


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(SCRIPTS, name + ".py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


act = load("activity")


def dt(y, mo, d, h=0, mi=0, s=0):
    return datetime(y, mo, d, h, mi, s)


def ns(d):
    # ponytail: naive datetimes are UTC wall-clock; calendar.timegm keeps
    # start_ns stable regardless of the machine's local timezone.
    import calendar
    return int(calendar.timegm(d.timetuple()) * 1e9)


REPO = "a" * 64


class PrecedenceTest(unittest.TestCase):
    def test_native_loc_beats_openlit_no_sum(self):
        m = [
            {"instrument": "claude_code.lines_of_code.count", "client": "claude-code",
             "session_id": "s1", "timestamp": dt(2026, 9, 20, 1), "value": 10,
             "temporality": 1, "type": "added", "stream": (("type", "added"),)},
            {"instrument": "coding_agent.lines_of_code.count", "client": "claude-code",
             "session_id": "s1", "timestamp": dt(2026, 9, 20, 2), "value": 100,
             "temporality": 1, "type": "added", "stream": (("type", "added"),)},
        ]
        sessions, _ = act.summarize_activity([], [], m)
        self.assertEqual(sessions[("claude-code", "s1")]["lines_added"], 10)

    def test_native_modified_never_implies_accepted(self):
        m = [{"instrument": "claude_code.lines_of_code.count", "client": "claude-code",
              "session_id": "s1", "timestamp": dt(2026, 9, 20, 1), "value": 10,
              "temporality": 1, "type": "added", "stream": (("type", "added"),)}]
        sessions, _ = act.summarize_activity([], [], m)
        rec = sessions[("claude-code", "s1")]
        self.assertEqual(rec["lines_added"], 10)
        self.assertIsNone(rec["lines_accepted"])
        self.assertIsNone(rec["lines_rejected"])

    def test_openlit_edit_span_accepts_added_only(self):
        t = [{"trace_id": "t", "span_id": "e1", "span_name": "coding_agent.edit.decision",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 1, 0, 1),
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.client": "claude-code",
                                  "coding_agent.edit.decision": "accept",
                                  "coding_agent.edit.lines.added": 5,
                                  "coding_agent.edit.lines.removed": 3}}]
        sessions, _ = act.summarize_activity(t, [], [])
        rec = sessions[("claude-code", "s1")]
        self.assertEqual(rec["lines_accepted"], 5)
        self.assertIsNone(rec["lines_rejected"])
        self.assertEqual(rec["lines_removed"], 3)

    def test_per_edit_explicit_zero_loc_stays_zero(self):
        t = [{"trace_id": "t", "span_id": "e1", "span_name": "coding_agent.edit.decision",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 1, 0, 1),
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.client": "c",
                                  "coding_agent.edit.decision": "accept",
                                  "coding_agent.edit.lines.added": 0,
                                  "coding_agent.edit.lines.removed": 0}}]
        sessions, _ = act.summarize_activity(t, [], [])
        rec = sessions[("c", "s1")]
        self.assertEqual(rec["edit_accept_count"], 1)
        self.assertEqual(rec["lines_added"], 0)
        self.assertEqual(rec["lines_accepted"], 0)

    def test_numeric_string_counts_parsed(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.client": "c",
                                  "coding_agent.session.commit_count": "3"}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertEqual(sessions[("c", "s1")]["commit_count"], 3)

    def test_nan_count_ignored(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.client": "c",
                                  "coding_agent.session.commit_count": float("nan")}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertIsNone(sessions[("c", "s1")]["commit_count"])

    def test_desktop_service_maps_to_canonical_claude(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "service.name": "claude-code-desktop",
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.session.lines.added": 4}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertIn(("claude-code", "s1"), sessions)

    def test_explicit_client_wins_over_service(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "service.name": "claude-code",
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.client": "custom-fork",
                                  "coding_agent.session.lines.added": 4}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertIn(("custom-fork", "s1"), sessions)

    def test_codex_namespace_fallback_without_service(self):
        rows = [{"trace_id": "t", "span_id": "s", "span_name": "codex.turn",
                 "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
                 "span_attributes": {"session.id": "s9"}}]
        sessions, _ = act.summarize_activity(rows, [], [])
        self.assertIn(("codex", "s9"), sessions)

    def test_codex_namespace_wins_over_known_service_name(self):
        # Codex service.name is an overridable originator value: a Codex row
        # may carry another product's verified name. Namespace wins.
        rows = [{"trace_id": "t", "span_id": "s", "span_name": "codex.turn",
                 "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
                 "service.name": "claude-code",
                 "span_attributes": {"session.id": "s9"}}]
        sessions, _ = act.summarize_activity(rows, [], [])
        self.assertIn(("codex", "s9"), sessions)

    def test_arbitrary_service_name_stays_unknown(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "service.name": "my-fork",
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.session.lines.added": 4}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertIn(("unknown", "s1"), sessions)

    def test_conversation_id_fallback_matches_usage_normalizer(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "span_attributes": {"gen_ai.conversation.id": "conv1",
                                  "coding_agent.client": "c",
                                  "coding_agent.session.commit_count": 2}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertIn(("c", "conv1"), sessions)

    def test_raw_url_never_becomes_repo(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.client": "c",
                                  "vcs.repository.url.full": "https://github.com/o/r"}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertIsNone(sessions[("c", "s1")]["repo"])

    def test_hashed_repository_id_accepted(self):
        t = [{"trace_id": "t", "span_id": "s", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "span_attributes": {"coding_agent.session.id": "s1",
                                  "coding_agent.client": "c",
                                  "coding_agent.repository.id": REPO,
                                  "vcs.ref.head.name": "feature/activity"}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertEqual(sessions[("c", "s1")]["repo"], REPO)
        self.assertEqual(sessions[("c", "s1")]["branch"], "feature/activity")

    def test_metric_row_git_context_feeds_session(self):
        m = [{"instrument": "coding_agent.commit.count", "client": "c",
              "client_explicit": True,
              "session_id": "s", "timestamp": dt(2026, 9, 20, 1),
              "value": 1, "temporality": 1, "stream": (("x", "1"),),
              "coding_agent.repository.id": REPO, "vcs.ref.head.name": "main"}]
        sessions, _ = act.summarize_activity([], [], m)
        rec = sessions[("c", "s")]
        self.assertEqual(rec["repo"], REPO)
        self.assertEqual(rec["branch"], "main")
        self.assertIn("repo", rec["activity_sources"])
        self.assertIn("branch", rec["activity_sources"])

    def test_metric_row_raw_url_never_becomes_repo(self):
        m = [{"instrument": "coding_agent.commit.count", "client": "c",
              "client_explicit": True,
              "session_id": "s", "timestamp": dt(2026, 9, 20, 1),
              "value": 1, "temporality": 1, "stream": (("x", "1"),),
              "vcs.repository.url.full": "https://github.com/o/r"}]
        sessions, _ = act.summarize_activity([], [], m)
        self.assertIsNone(sessions[("c", "s")]["repo"])


class CumulativeTest(unittest.TestCase):
    def _cum(self, values, start, day=20, instrument="coding_agent.commit.count",
             stream=(("x", "1"),)):
        return [{"instrument": instrument, "client": "claude-code", "session_id": "s1",
                 "timestamp": dt(2026, 9, day, h), "value": v, "temporality": 2,
                 "start_ns": ns(start), "stream": stream}
                for h, v in enumerate(values, start=1)]

    def test_reset_treated_as_new_base(self):
        start = dt(2026, 9, 20)
        sessions, _ = act.summarize_activity(
            [], [], self._cum([5, 8, 3, 6], start))
        # ponytail: start_ns is in-day so the first reading counts: 5 + 3 + 3 (reset base) + 3.
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 14)

    def test_retransmit_same_timestamp_deduped(self):
        start = dt(2026, 9, 20)
        rows = self._cum([5, 8], start) + self._cum([8], start)[1:]
        rows[-1]["timestamp"] = dt(2026, 9, 20, 2)
        sessions, _ = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 8)

    def test_first_point_without_same_day_start_unknown(self):
        rows = self._cum([50, 55], dt(2026, 9, 19))
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 55)
        by_day = {d["day_start"]: d for d in daily}
        self.assertIsNone(by_day[dt(2026, 9, 19)]["commit_count"])
        self.assertIsNone(by_day[dt(2026, 9, 20)]["commit_count"])

    def test_cross_midnight_baseline_session_total_daily_unknown(self):
        start = datetime(2026, 9, 20, 23, 59)
        rows = [{"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": datetime(2026, 9, 21, 0, 1),
                 "value": 5, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)},
                {"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": datetime(2026, 9, 21, 0, 2),
                 "value": 8, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)}]
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 8)
        by_day = {d["day_start"]: d for d in daily}
        self.assertIsNone(by_day[dt(2026, 9, 20)]["commit_count"])
        self.assertIsNone(by_day[dt(2026, 9, 21)]["commit_count"])

    def test_cross_midnight_interval_daily_unknown(self):
        start = dt(2026, 9, 20)
        rows = [{"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": dt(2026, 9, 20, 23),
                 "value": 5, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)},
                {"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": dt(2026, 9, 21, 1),
                 "value": 9, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)}]
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 9)
        by_day = {d["day_start"]: d for d in daily}
        self.assertIsNone(by_day[dt(2026, 9, 20)]["commit_count"])
        self.assertIsNone(by_day[dt(2026, 9, 21)]["commit_count"])

    def test_cross_midnight_active_session_total_daily_unknown(self):
        start = datetime(2026, 9, 20, 23, 59)
        mk = lambda ts, v: {"instrument": "claude_code.active_time.total",
                            "client": "claude-code", "session_id": "s1",
                            "timestamp": ts, "value": v, "temporality": 2,
                            "start_ns": ns(start), "type": "user",
                            "stream": (("type", "user"),)}
        rows = [mk(datetime(2026, 9, 21, 0, 1), 5), mk(datetime(2026, 9, 21, 0, 2), 8)]
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["active_user_seconds"], 8)
        by_day = {d["day_start"]: d for d in daily}
        self.assertIsNone(by_day[dt(2026, 9, 20)]["active_user_seconds"])
        self.assertIsNone(by_day[dt(2026, 9, 21)]["active_user_seconds"])

    def test_multi_session_unknown_poisons_known_daily(self):
        start = datetime(2026, 9, 20, 23, 59)
        known = {"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "known", "timestamp": dt(2026, 9, 21, 5),
                 "value": 4, "temporality": 1, "stream": (("x", "2"),)}
        mk = lambda sid, ts, v: {"instrument": "coding_agent.commit.count",
                                 "client": "claude-code", "session_id": sid,
                                 "timestamp": ts, "value": v, "temporality": 2,
                                 "start_ns": ns(start), "stream": (("x", "1"),)}
        rows = [known,
                mk("cross", datetime(2026, 9, 21, 0, 1), 5),
                mk("cross", datetime(2026, 9, 21, 0, 2), 8)]
        sessions, daily = act.summarize_activity([], [], rows)
        by_day = {d["day_start"]: d for d in daily}
        self.assertIsNone(by_day[dt(2026, 9, 21)]["commit_count"])
        self.assertEqual(sessions[("claude-code", "known")]["commit_count"], 4)
        self.assertEqual(sessions[("claude-code", "cross")]["commit_count"], 8)

    def test_zero_delta_crossing_midnight_not_poison(self):
        start = dt(2026, 9, 20)
        rows = [{"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": dt(2026, 9, 20, 23),
                 "value": 5, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)},
                {"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": dt(2026, 9, 21, 1),
                 "value": 5, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)}]
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 5)
        by_day = {d["day_start"]: d for d in daily}
        self.assertEqual(by_day[dt(2026, 9, 20)]["commit_count"], 5)

    def test_intervening_days_poisoned(self):
        start = dt(2026, 9, 20)
        rows = [{"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": dt(2026, 9, 20, 12),
                 "value": 5, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)},
                {"instrument": "coding_agent.commit.count", "client": "claude-code",
                 "session_id": "s1", "timestamp": dt(2026, 9, 22, 12),
                 "value": 9, "temporality": 2, "start_ns": ns(start),
                 "stream": (("x", "1"),)}]
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 9)
        by_day = {d["day_start"]: d for d in daily}
        self.assertIsNone(by_day[dt(2026, 9, 20)]["commit_count"])
        self.assertIsNone(by_day[dt(2026, 9, 21)]["commit_count"])
        self.assertIsNone(by_day[dt(2026, 9, 22)]["commit_count"])

    def test_exact_midnight_start_allocates(self):
        start = dt(2026, 9, 21)
        rows = self._cum([5, 8], start, day=21)
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 8)
        by_day = {d["day_start"]: d for d in daily}
        self.assertEqual(by_day[dt(2026, 9, 21)]["commit_count"], 8)

    def test_zero_start_ns_stays_unknown(self):
        rows = [{"instrument": "coding_agent.commit.count", "client": "c",
                 "client_explicit": True,
                 "session_id": "s", "timestamp": dt(2026, 9, 20, 1),
                 "value": 5, "temporality": 2, "start_ns": 0,
                 "stream": (("x", "1"),)}]
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertIsNone(sessions[("c", "s")]["commit_count"])
        self.assertEqual(daily, [])

    def test_same_day_cumulative_still_allocates(self):
        start = dt(2026, 9, 20)
        sessions, daily = act.summarize_activity(
            [], [], self._cum([5, 8], start))
        self.assertEqual(sessions[("claude-code", "s1")]["commit_count"], 8)
        self.assertEqual(daily[0]["commit_count"], 8)

    def test_delta_same_stream_timestamp_last_wins(self):
        mk = lambda v: {"instrument": "coding_agent.commit.count", "client": "c",
                        "client_explicit": True,
                        "session_id": "s", "timestamp": dt(2026, 9, 20, 1),
                        "value": v, "temporality": 1, "stream": (("x", "1"),)}
        sessions, _ = act.summarize_activity([], [], [mk(5), mk(7)])
        self.assertEqual(sessions[("c", "s")]["commit_count"], 7)

    def test_explicit_desktop_client_preserved_verbatim(self):
        rows = [{"instrument": "coding_agent.commit.count", "client": "claude-code-desktop",
                 "client_explicit": True, "session_id": "s",
                 "timestamp": dt(2026, 9, 20, 1), "value": 2,
                 "temporality": 1, "stream": (("x", "1"),)}]
        sessions, _ = act.summarize_activity([], [], rows)
        self.assertIn(("claude-code-desktop", "s"), sessions)

    def test_missing_temporality_never_guessed(self):
        rows = [{"instrument": "coding_agent.commit.count", "client": "c",
                 "client_explicit": True,
                 "session_id": "s", "timestamp": dt(2026, 9, 20, 1),
                 "value": 9, "stream": (("x", "1"),)}]
        sessions, daily = act.summarize_activity([], [], rows)
        self.assertIsNone(sessions[("c", "s")]["commit_count"])
        self.assertEqual(daily, [])

    def test_string_metric_values_parsed(self):
        rows = [{"instrument": "coding_agent.commit.count", "client": "c",
                 "client_explicit": True,
                 "session_id": "s", "timestamp": dt(2026, 9, 20, 1),
                 "value": "4", "temporality": 1, "stream": (("x", "1"),)}]
        sessions, _ = act.summarize_activity([], [], rows)
        self.assertEqual(sessions[("c", "s")]["commit_count"], 4)

    def test_explicit_zero_preserved(self):
        rows = [{"instrument": "coding_agent.commit.count", "client": "c",
                 "client_explicit": True,
                 "session_id": "s", "timestamp": dt(2026, 9, 20, 1),
                 "value": 0, "temporality": 1, "stream": (("x", "1"),)}]
        sessions, daily = act.summarize_activity([], [], rows)
        rec = sessions[("c", "s")]
        self.assertEqual(rec["commit_count"], 0)
        self.assertIn("commit_count", rec["activity_sources"])
        self.assertEqual(daily[0]["commit_count"], 0)


class SpanEventTest(unittest.TestCase):
    def test_root_retransmit_latest_wins(self):
        mk = lambda v, end: {
            "trace_id": "t", "span_id": "root", "span_name": "coding_agent.session",
            "timestamp": dt(2026, 9, 20, 1), "timestamp_end": end,
            "span_attributes": {"coding_agent.session.id": "s1",
                                "coding_agent.client": "c",
                                "coding_agent.session.commit_count": v}}
        sessions, _ = act.summarize_activity(
            [mk(2, dt(2026, 9, 20, 2)), mk(5, dt(2026, 9, 20, 3))], [], [])
        self.assertEqual(sessions[("c", "s1")]["commit_count"], 5)

    def test_midnight_union_session_roots_only(self):
        t = [{"trace_id": "t", "span_id": "a", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 23), "timestamp_end": dt(2026, 9, 21, 1),
              "span_attributes": {"coding_agent.session.id": "s1", "client": "c"}},
             {"trace_id": "t", "span_id": "b", "span_name": "invoke_agent",
              "timestamp": dt(2026, 9, 20, 23, 30),
              "timestamp_end": dt(2026, 9, 21, 0, 30),
              "span_attributes": {"coding_agent.session.id": "s1", "client": "c"}}]
        sessions, daily = act.summarize_activity(t, [], [])
        rec = sessions[("c", "s1")]
        self.assertEqual(rec["session_wall_seconds"], 7200)
        by_day = {d["day_start"]: d for d in daily}
        self.assertEqual(by_day[dt(2026, 9, 20)]["session_wall_seconds"], 3600)
        self.assertEqual(by_day[dt(2026, 9, 21)]["session_wall_seconds"], 3600)

    def test_tool_spans_excluded_from_wall(self):
        t = [{"trace_id": "t", "span_id": "a", "span_name": "execute_tool",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 5),
              "span_attributes": {"coding_agent.session.id": "s1", "client": "c"}}]
        sessions, _ = act.summarize_activity(t, [], [])
        self.assertIsNone(sessions[("c", "s1")]["session_wall_seconds"])

    def test_subagent_sequence_reset_both_count(self):
        mk = lambda ts, seq: {"timestamp": ts, "log_attributes": {
            "event.name": "claude_code.subagent_completed", "session.id": "s1",
            "client": "claude-code", "event.sequence": seq,
            "event.timestamp": ts}}
        sessions, _ = act.summarize_activity(
            [], [mk(dt(2026, 9, 20, 1), 3), mk(dt(2026, 9, 20, 5), 3)], [])
        self.assertEqual(sessions[("claude-code", "s1")]["subagents"], 2)

    def test_subagent_agent_id_scoped_per_session(self):
        mk = lambda sid, client: {"trace_id": "t-" + sid, "span_id": "g",
                                  "span_name": "coding_agent.subagent",
                                  "timestamp": dt(2026, 9, 20, 1),
                                  "timestamp_end": dt(2026, 9, 20, 1, 0, 1),
                                  "span_attributes": {
                                      "coding_agent.session.id": sid,
                                      "coding_agent.client": client,
                                      "coding_agent.agent.id": "same-agent"}}
        rows = [mk("s1", "c"), mk("s2", "c")]
        sessions, _ = act.summarize_activity(rows, [], [])
        self.assertEqual(sessions[("c", "s1")]["subagents"], 1)
        self.assertEqual(sessions[("c", "s2")]["subagents"], 1)

    def test_commit_sha_dedupes_spans(self):
        mk = lambda sid: {"trace_id": "t", "span_id": sid,
                          "span_name": "coding_agent.git.commit",
                          "timestamp": dt(2026, 9, 20, 1),
                          "timestamp_end": dt(2026, 9, 20, 1, 0, 1),
                          "span_attributes": {"coding_agent.session.id": "s1",
                                              "client": "c",
                                              "vcs.ref.head.revision": "abc"}}
        sessions, _ = act.summarize_activity([mk("a"), mk("b")], [], [])
        self.assertEqual(sessions[("c", "s1")]["commit_count"], 1)

    def test_commit_sha_scoped_per_session(self):
        mk = lambda sid: {"trace_id": "t-" + sid, "span_id": "g",
                          "span_name": "coding_agent.git.commit",
                          "timestamp": dt(2026, 9, 20, 1),
                          "timestamp_end": dt(2026, 9, 20, 1, 0, 1),
                          "span_attributes": {"coding_agent.session.id": sid,
                                              "client": "c",
                                              "vcs.ref.head.revision": "abc"}}
        sessions, _ = act.summarize_activity([mk("s1"), mk("s2")], [], [])
        self.assertEqual(sessions[("c", "s1")]["commit_count"], 1)
        self.assertEqual(sessions[("c", "s2")]["commit_count"], 1)

    def test_generic_codex_decision_not_an_edit(self):
        rows = [{"timestamp": dt(2026, 9, 20, 1), "log_attributes": {
            "session.id": "s1", "tool_use_id": "u1", "tool_name": "shell",
            "tool_namespace": "codex", "call_id": "c1", "decision": "accept"}}]
        sessions, _ = act.summarize_activity([], rows, [])
        rec = sessions[("unknown", "s1")]
        self.assertIsNone(rec["edit_accept_count"])
        self.assertIsNone(rec["edit_reject_count"])

    def test_identities_without_span_ids_skipped(self):
        rows = [{"span_name": "coding_agent.git.commit",
                 "timestamp": dt(2026, 9, 20, 1),
                 "timestamp_end": dt(2026, 9, 20, 1, 0, 1),
                 "span_attributes": {"coding_agent.session.id": "s1", "client": "c"}}]
        sessions, daily = act.summarize_activity(rows, [], [])
        self.assertEqual((sessions, daily), ({}, []))

    def test_unsupported_fields_stay_null(self):
        sessions, daily = act.summarize_activity([], [], [])
        self.assertEqual((sessions, daily), ({}, []))
        t = [{"trace_id": "t", "span_id": "a", "span_name": "coding_agent.session",
              "timestamp": dt(2026, 9, 20, 1), "timestamp_end": dt(2026, 9, 20, 2),
              "span_attributes": {"coding_agent.session.id": "s1", "client": "c"}}]
        sessions, _ = act.summarize_activity(t, [], [])
        rec = sessions[("c", "s1")]
        for field in act.ACTIVITY_COUNT_FIELDS + act.ACTIVITY_VALUE_FIELDS:
            if field != "session_wall_seconds":
                self.assertIsNone(rec[field], field)


if __name__ == "__main__":
    unittest.main()
