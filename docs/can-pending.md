# CAN pending index

`decode_once` candidate selection reads only the durable `decode_pending`
membership table instead of scanning every completed session.

See also: [operations § can-receiver](operations.md#별도-can-uploader의-서버-receiver).

## Design

- `decode_pending(session, epoch)` holds exactly sessions with raw chunks at or
  beyond the epoch cursor (or no cursor yet). Index on `(epoch)`.
- Same-transaction maintenance, so crash/rollback cannot skew membership:
  - `accept()` refreshes the session for every registered epoch via the
    cursor-first `_reconcile_pending_session` (one PK cursor read plus one
    indexed `(session, seq>=cursor)` probe): completed sessions reawaken,
    pure overlap resends reconcile to no change, retained history is never
    joined or scanned.
  - `decode_once()` commit refreshes each staged session the same way;
    partial-chunk sessions stay pending, full completions drop out.
  - `register_epoch(..., explicit=True)` populates the new epoch from all raw
    regardless of other epochs' cursors, so re-decode replays every original.
- Ordering invariant unchanged: `candidate_sql()` keeps `ORDER BY c.id
  LIMIT 1` over the same per-session next-seq join and staged-progress CTE,
  with `CROSS JOIN` pinning `decode_pending` first so the planner cannot
  choose a raw-history scan. Cursor CAS, partial resume, row budget,
  outbox/counter atomicity untouched.
- `candidate_sql(n_progress)` is the single shipped source for the candidate
  SELECT (bind progress pairs, then the epoch twice); runners import it so
  measured plans cannot drift from the shipped query.
- No raw deletion, no flush budget change.

## Crash / restart

Pending writes commit atomically with the cursor/raw writes they describe, so
an interrupted decode or receive leaves membership equal to the persisted
cursors. Restart needs no replay beyond opening the archive.

## Migration / rebuild

- `Archive.__init__` creates the table/index and runs one-time
  `_migrate_pending` (marker `pending_migrated` in `archive_meta`): rebuilds
  membership per existing epoch. Later opens are a single marker probe.
- `Archive.rebuild_pending()` rescans all epochs in one transaction for
  archives modified by outside writers. Safe to rerun (idempotent).

## Evidence

- Regression: `python -m unittest tests.test_can_receiver.ReceiverBehavior.test_pending_index_reawakens_epochs_resume_and_rebuild`
  covers pending-driven selection (hiding a member diverts the shipped pick),
  actual candidate VM steps instead of plan text, completion drop-out,
  reawaken on new raw, resend overlap/conflict fixtures, new-epoch full
  replay, crash-before-commit status equality, migration exact-set equality,
  and rebuild idempotence.
- Bounded history: `python -m unittest tests.test_can_receiver.ReceiverBehavior.test_decode_resumes_in_arrival_order_without_rescanning_history`
  passes unchanged; the decode-commit reconcile no longer joins history.
- Benchmark: `python tools/benchmark_can_pending.py --matrix
  --baseline-root /path/to/pristine-checkout --repeats 20`. It uses
  synthetic SQLite archives only, measures the shipped candidate query,
  and runs the original implementation in separate subprocesses.

The full matrix used 1,000/10,000/100,000 completed sessions and
0/5/1,000 pending sessions. All nine cells matched the original selected
chunk, event IDs, parser state, counters and row deltas.

| Completed sessions | Idle VM steps before | Idle VM steps after |
| --- | ---: | ---: |
| 1,000 | 31,172 | 177 |
| 10,000 | 310,172 | 177 |
| 100,000 | 3,100,172 | 177 |

At five pending sessions the new query used 445 VM steps at every
history size; at 1,000 pending it used 48,205. Writer contention was
measured with a real competing `BEGIN IMMEDIATE` transaction; it is not
the same metric as an uncontended `accept()` duration. These synthetic
results do not establish production throughput.
