# Outbox batching: durability boundary

Fleet `process_message`/`process_envelope` and VSS `store_updates`
(+ single-row `store_update`) commit once per already-received unit:
one Fleet envelope, one VSS snapshot fetch, or one subscribe update.
No cross-message micro-batch or timer: at low input rates nothing waits
for a commit, and shutdown/error recovery needs no extra flush.

Durable boundary: a row counts as stored only after the batch `commit`
returns. Success counters (`signals_stored`, `events_stored`, `stored`)
and dedupe state (`last`, `seen_ids`) land only on commit; a failed
stage or commit rolls back with zero counter/cache side effects, so
retry redelivers cleanly. `seen_ids` + outbox rows share the same
transaction, and rollback never leaves a `seen_ids` entry that would
block retry.

What commit-then-exit proves: an executable subprocess test in each
suite commits one batch, then `os._exit(0)` without cleanup; the parent
reopens the file-backed WAL/FULL outbox and finds every row. This is a
process-termination proof only. It is NOT a power-failure or OS-crash
guarantee: SQLite `synchronous=FULL` + WAL narrows the loss window, but
only real power-fault testing could bound it, and none was run here.

Invariants preserved: FULL/WAL everywhere; Fleet TOTAL signal+event cap
from one base COUNT plus staged inserts (no per-row COUNT), order kept
(insertion order within the envelope); overflow rejects new rows without
deleting unacked rows and skips `seen_ids` for rejected rows so
post-drain redelivery lands; Greptime full-ACK-only exact-`event_id`
delete; timeout/partial ACK keeps the outbox; same-value-different-time
keeps its own row; restart/seen dedupe unchanged.

## Synthetic measurements

File-backed SQLite WAL/FULL, three repeats, received units scheduled at
500/s. Each unit is already received before storage begins; no rows are
held across units. Full outbox/seen content and generated upload SQL
matched the original per-row implementation.

| Recorder | Rows/unit | Commits/1000 before | Commits/1000 after | Durable p95 before (ms) | Durable p95 after (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fleet | 50 | 1000 | 20 | 7.713 | 1.635 |
| VSS | 1000 | 1000 | 1 | 140.940 | 11.844 |

The offered rate is not a throughput claim: VSS achieved 11.0 units/s
before and 107.5 units/s after in this fixture. These local synthetic
measurements do not establish production throughput or power-loss safety.

```bash
python tools/benchmark_fleet_outbox.py --baseline-root /path/to/baseline \
  --sizes 10,50 --rates 50,500 --repeats 3 --units 20
python tools/benchmark_vss_outbox.py --baseline-root /path/to/baseline \
  --sizes 200,1000 --rates 50,500 --repeats 3 --units 25
```

Related docs: [operations](operations.md) (recorder/uploader runtime),
[development](development.md) (test layout), [tesla_fleet](tesla_fleet.md)
(Fleet protocol), plus module docstrings in
`scripts/ingest/fleet_recorder.py` and `scripts/vehicle/vss_recorder.py`.
