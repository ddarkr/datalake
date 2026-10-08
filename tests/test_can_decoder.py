"""Synthetic decoder behavioral regression; no vehicle DBC or observations.

Covers sign/endianness/scaling, named/invalid/unknown enums, simple mux
variants plus unsupported selectors, extended frames, FD-length serial
rejection, control/unknown/malformed records, fragmented tails, oversize
discard, exact ordinals/timestamps/identities across splits and restarts,
ns-boundary integers, and bounded decode_some resume parity.

Run: python -m unittest discover -s tests -p test_can_decoder.py
"""
import json
from pathlib import Path
import tempfile
import unittest

from scripts.ingest.can.can_decoder import Decoder

DBC_TEXT = ('VERSION "synthetic"\n\nNS_ :\n\nBS_:\n\nBU_: Fixture\n\n'
            'BO_ 291 Sample: 2 Fixture\n'
            ' SG_ Power : 0|16@1- (0.5,0) [-16384|16383.5] "kW" Fixture\n'
            'BO_ 292 BigMsg: 8 Fixture\n'
            ' SG_ Big : 7|16@0+ (0.1,0) [0|6553.5] "V" Fixture\n'
            'BO_ 293 EnumMsg: 1 Fixture\n'
            ' SG_ Mode : 0|8@1+ (1,0) [0|255] "" Fixture\n'
            'VAL_ 293 Mode 0 "Off" 1 "On" 2 "INVALID VALUE" ;\n'
            'BO_ 512 MuxMsg: 8 Fixture\n'
            ' SG_ MuxSel M : 0|8@1+ (1,0) [0|255] "" Fixture\n'
            ' SG_ SigA m0 : 8|8@1+ (1,0) [0|255] "" Fixture\n'
            ' SG_ SigC m1 : 16|8@1+ (1,0) [0|255] "" Fixture\n'
            'BO_ 2147483904 MsgExt: 8 Fixture\n'
            ' SG_ ExtSig : 0|8@1+ (1,0) [0|10] "" Fixture\n'
            'BO_ 300 FdMsg: 12 Fixture\n'
            ' SG_ Fast : 0|8@1+ (1,0) [0|255] "" Fixture\n')


def _entry(identifier, signal, start, length, order, signed, scale, mux,
           mux_signal, mux_ids, dlc, unit, choices, extended=False):
    item = {"source": "synthetic", "id": identifier, "signal": signal,
            "source_signal": signal, "kind": "data", "start_bit": start,
            "bit_length": length, "byte_order": order, "signed": signed,
            "scale": scale, "offset": 0, "is_multiplexer": mux,
            "multiplexer_signal": mux_signal, "multiplexer_ids": mux_ids,
            "actual_dbc_length": dlc, "unit": unit, "source_unit": unit,
            "choices": choices, "evidence": "synthetic@0123456789abcdef"}
    if extended:
        item["is_extended_frame"] = True
    return item


def wide_decoder(directory):
    dbc = Path(directory) / "wide.dbc"
    dbc.write_text(DBC_TEXT, encoding="utf-8")
    definitions = Path(directory) / "wide.json"
    definitions.write_text(json.dumps({"revision": "synthetic-wide", "signals": [
        _entry("0x123", "Power", 0, 16, "little_endian", True, 0.5, False, None, None, 2, "kW", {}),
        _entry("0x124", "Big", 7, 16, "big_endian", False, 0.1, False, None, None, 8, "V", {}),
        _entry("0x125", "Mode", 0, 8, "little_endian", False, 1, False, None, None, 1, "",
               {"0": "Off", "1": "On", "2": "INVALID VALUE"}),
        _entry("0x200", "MuxSel", 0, 8, "little_endian", False, 1, True, None, None, 8, "", {}),
        _entry("0x200", "SigA", 8, 8, "little_endian", False, 1, False, "MuxSel", [0], 8, "", {}),
        _entry("0x200", "SigC", 16, 8, "little_endian", False, 1, False, "MuxSel", [1], 8, "", {}),
        _entry("0x100", "ExtSig", 0, 8, "little_endian", False, 1, False, None, None, 8, "", {},
               extended=True),
        _entry("0x12C", "Fast", 0, 8, "little_endian", False, 1, False, None, None, 12, "", {}),
    ]}), encoding="utf-8")
    return Decoder(dbc, definitions)


META = {"schema_version": 1, "vehicle": "synthetic", "collector_id": "fixture",
        "session_id": "behavior", "started_ns": 1800000000000000000,
        "vehicle_firmware": "synthetic"}

FRAMES = [b"t1232FFFF\r",  # Signed Power = -0.5 kW, negative multibyte value.
          b"t12481234000000000000\r",  # Big-endian scaled voltage: 4660 * 0.1.
          b"t125100\r",  # Named enum.
          b"t125102\r",  # Invalid enum label.
          b"t125109\r",  # Unknown enum value.
          b"t20080009000000000000\r",  # Simple mux variant 0.
          b"t20080100080000000000\r",  # Simple mux variant 1.
          b"t20080500000000000000\r",  # Unsupported mux selector.
          b"T0000010080300000000000000\r",  # Extended frame.
          b"t12C120700000000000000000000000\r",  # FD-length DLC: malformed serial.
          b"t123100\r",  # DLC mismatch for Power.
          b"t12020000\r",  # Unknown ID with a defined DLC shape.
          b"r1230\r",  # Remote frame still consumes an ordinal.
          b"Z\r",  # Control record.
          b"not-a-frame\r"]  # Malformed record.
TAIL_FRAME = b"t12320400\r"  # Power = 2.0, split across chunk boundaries.


class DecoderBehavior(unittest.TestCase):
    def test_sign_endian_enum_mux_extended_control_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = wide_decoder(directory)
            chunk0 = {"seq": 0, "offset_ns": 0, "phase": "capture",
                      "data": b"".join(FRAMES) + TAIL_FRAME[:5]}
            chunk1 = {"seq": 1, "offset_ns": 11, "phase": "capture", "data": TAIL_FRAME[5:]}
            rows0, state0, counts0 = decoder.decode(META, chunk0, None)
            rows1, state1, counts1 = decoder.decode(META, chunk1, state0)
            totals = dict(counts0)
            for key, value in counts1.items():
                totals[key] = value if key == "tail_bytes" else totals.get(key, 0) + value
            self.assertEqual((totals["frames"], totals["decoded_frames"], totals["rows"]),
                             (13, 9, 11))
            self.assertEqual((totals["unknown_frames"], totals["remote_frames"],
                              totals["malformed_records"], totals["dlc_mismatch"],
                              totals["unsupported_mux"], totals["unknown_enum_signals"],
                              totals["control_records"], totals["tail_bytes"]),
                             (1, 1, 2, 1, 1, 1, 1, 0))
            by_path = {}
            for row in rows0 + rows1:
                by_path.setdefault(row["path"], []).append(row)
            self.assertEqual(by_path["Vehicle.CAN.x123.Power"][0]["value_num"], -0.5)
            self.assertEqual(by_path["Vehicle.CAN.x123.Power"][-1]["value_num"], 2.0)
            self.assertEqual(by_path["Vehicle.CAN.x124.Big"][0]["value_num"], 466.0)
            self.assertEqual([row["value_text"] for row in by_path["Vehicle.CAN.x125.Mode"]],
                             ["Off", "INVALID VALUE", "UNKNOWN(9)"])
            self.assertEqual([row["quality"] for row in by_path["Vehicle.CAN.x125.Mode"]],
                             ["reported_unverified", "invalid", "unknown_enum"])
            self.assertEqual(by_path["Vehicle.CAN.x200.SigA"][0]["value_num"], 9.0)
            self.assertEqual(by_path["Vehicle.CAN.x200.SigC"][0]["value_num"], 8.0)
            self.assertEqual([row["value_num"] for row in by_path["Vehicle.CAN.x200.MuxSel"]],
                             [0.0, 1.0])
            self.assertEqual(by_path["Vehicle.CAN.x100.ExtSig"][0]["value_num"], 3.0)
            self.assertEqual([row["event_time"] for row in rows0],
                             [META["started_ns"]] * len(rows0))
            self.assertEqual([row["event_time"] for row in rows1],
                             [META["started_ns"] + 11] * len(rows1))
            # Duplicate ACK replays reproduce identical identities, never new rows.
            again0, _, _ = decoder.decode(META, chunk0, None)
            self.assertEqual([row["event_id"] for row in again0],
                             [row["event_id"] for row in rows0])
            replay1, _, _ = decoder.decode(META, chunk1, decoder.decode(META, chunk0, None)[1])
            self.assertEqual([row["event_id"] for row in replay1],
                             [row["event_id"] for row in rows1])

    def test_bounded_resume_reproduces_exact_rows_state_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = wide_decoder(directory)
            chunk0 = {"seq": 0, "offset_ns": 0, "phase": "capture",
                      "data": b"".join(FRAMES) + TAIL_FRAME[:5]}
            chunk1 = {"seq": 1, "offset_ns": 11, "phase": "capture", "data": TAIL_FRAME[5:]}
            rows0, state0, _ = decoder.decode(META, chunk0, None)
            rows1, state1, counts1 = decoder.decode(META, chunk1, state0)
            reference = rows0 + rows1
            for budget in (1, 2, 2000):
                walked, walking, summed, batches = [], None, {}, 0
                for chunk in (chunk0, chunk1):
                    walking = None if chunk["seq"] == 0 else walking
                    while True:
                        batch, walking, deltas, done = decoder.decode_some(
                            META, chunk, walking, budget)
                        if not done:
                            self.assertTrue(batch)  # Non-final batches never emit empty.
                        walked.extend(batch)
                        batches += 1
                        for key, value in deltas.items():
                            summed[key] = (value if key == "tail_bytes"
                                           else summed.get(key, 0) + value)
                        if done:
                            break
                self.assertEqual([row["event_id"] for row in walked],
                                 [row["event_id"] for row in reference])
                self.assertEqual(walking, state1)
                self.assertEqual(summed.pop("tail_bytes"), counts1["tail_bytes"])
                check = dict(counts1)
                for key in ("tail_bytes", "bytes", "chunks"):
                    # bytes/chunks ride the first batch of each chunk only.
                    check.pop(key, None)
                    summed.pop(key, None)
                self.assertEqual(summed["rows"], len(reference))
                self.assertLessEqual(batches, len(reference) + 2)
            with self.assertRaisesRegex(ValueError, "positive integer"):
                decoder.decode_some(META, chunk0, None, 0)

    def test_oversize_discard_and_ns_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            decoder = wide_decoder(directory)
            huge = {"seq": 0, "offset_ns": 0, "phase": "capture",
                    "data": b"x" * 4097 + b"\r" + b"t12320200\r"}
            rows, final, counts = decoder.decode(META, huge, None)
            self.assertEqual([(row["value_num"], row["event_time"]) for row in rows],
                             [(1.0, META["started_ns"])])
            self.assertEqual((counts["oversize_records"], counts["malformed_records"],
                              counts["tail_bytes"], final["next_frame"]), (1, 1, 0, 1))
            edge = dict(META, started_ns=2 ** 63 - 12)
            edge_rows, _, _ = decoder.decode(
                edge, {"seq": 0, "offset_ns": 11, "phase": "capture", "data": b"t12320200\r"},
                None)
            self.assertEqual(edge_rows[0]["event_time"], 2 ** 63 - 1)
            self.assertIs(type(edge_rows[0]["event_time"]), int)

    def test_replay_identity_golden_unicode_escaped_meta(self):
        """Golden consumer-replay identities: unicode/escaped metadata bytes plus
        adjacent-ns and adjacent-path rows must hash to distinct exact IDs."""
        import hashlib
        import json as _json
        from scripts.ingest.can.can_decoder import _hash
        with tempfile.TemporaryDirectory() as directory:
            decoder = wide_decoder(directory)
            tricky = dict(META, vehicle_firmware="synthetic 雪 'quoted' \\ label",
                          session_id="sess dupa\\u00e9")
            frame = b"t12320200\r"
            first, _, _ = decoder.decode(
                tricky, {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}, None)
            row = first[0]
            ordinal = 0
            expected = hashlib.sha256(_json.dumps(
                [tricky["vehicle"], tricky["collector_id"], tricky["session_id"],
                 ordinal, row["path"], decoder.epoch, row["event_time"]],
                sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                allow_nan=False).encode()).hexdigest()
            self.assertEqual(row["event_id"], expected)
            self.assertEqual(row["event_id"], _hash(
                [tricky["vehicle"], tricky["collector_id"], tricky["session_id"],
                 ordinal, row["path"], decoder.epoch, row["event_time"]]))
            # Replay of the same chunk reproduces the identical ID (idempotent).
            replay, _, _ = decoder.decode(
                tricky, {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}, None)
            self.assertEqual(replay[0]["event_id"], row["event_id"])
            # Adjacent ns and adjacent paths/ordinals never collide.
            later, _, _ = decoder.decode(
                tricky, {"seq": 0, "offset_ns": 1, "phase": "capture", "data": frame}, None)
            self.assertNotEqual(later[0]["event_id"], row["event_id"])
            multi, _, _ = decoder.decode(
                tricky, {"seq": 0, "offset_ns": 0, "phase": "capture",
                         "data": b"t12320200\r" + b"t12481234000000000000\r"}, None)
            ids = [entry["event_id"] for entry in multi]
            self.assertEqual(len(set(ids)), len(ids))
            # Same ordinal under a different session is a different identity.
            other = dict(tricky, session_id="other-session")
            foreign, _, _ = decoder.decode(
                other, {"seq": 0, "offset_ns": 0, "phase": "capture", "data": frame}, None)
            self.assertNotEqual(foreign[0]["event_id"], row["event_id"])


if __name__ == "__main__":
    unittest.main()
