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
- Ordering invariant unchanged: pending-driven selection resolves staged
  chunks from `decode_pending` membership plus per-session next-seq via the
  existing `(session, seq)` index, keeping global `raw_chunks.id` arrival
  order across sessions. Raw chunk boundaries are never coalesced, no
  per-session `LIMIT` skips another session's earlier arrival, and retained
  completed history is never scanned. Each bounded turn does two indexed
  read phases on one read connection (autocommit SELECTs, closed before CPU
  decode): `pending_heads_sql()` binds the epoch twice and returns one row
  per pending session with id/session/seq only (never blob data), ordered by
  arrival id; one indexed per-head prefix lookup (`session, seq>=head`
  `ORDER BY seq`) is heap-merged by arrival id and re-sorted by raw id, and
  only the chosen streams fetch their bounded chunk rows by `(session, seq)`.
  Prefetch stops at 1,000 chunks or 4MiB raw (plus at most one bounded row),
  whichever first, and keeps chunk dict shape with state/meta/totals staged
  in an incremental per-session map. The 50ms soft budget is set after the
  bounded read closes and applies only to decode work, so a slow read cannot
  systematically consume the whole decode time budget; the CPU batch still
  closes at 1,000 chunks, 2,000 output rows, or the budget, and row
  budget/partial-chunk resume/deadline can leave later chunks for subsequent
  turns with only committed staged rows advancing cursors. Per-session
  anchors/partials are read once in the read phase for commit-time CAS. The
  single final `BEGIN IMMEDIATE` commit validates cursor/partial CAS once
  per session, inserts all staged rows with one staged-order executemany,
  stores only each session's last completed cursor/state and trailing
  partial, applies one batch-sum bump per counter key, and reconciles each
  staged session's pending membership. `decode_once` and any runners share
  `pending_heads_sql()` as their single source so measured plans cannot
  drift from the shipped query; `candidate_sql(n)` is removed. No profile,
  env var, or CLI change. Synthetic profile measurements are not operating
  guarantees.
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
  covers pending-driven selection (hiding a member diverts the shipped
  heads), actual heads-query VM steps instead of plan text, completion
  drop-out, reawaken on new raw, resend overlap/conflict fixtures, new-epoch
  full replay, crash-before-commit status equality, migration exact-set
  equality, and rebuild idempotence.
- Bounded history: `python -m unittest tests.test_can_receiver.ReceiverBehavior.test_decode_resumes_in_arrival_order_without_rescanning_history`
  passes unchanged; the decode-commit reconcile no longer joins history.
- Benchmark: `python tools/benchmark_can_pending.py --matrix
  --baseline-root /path/to/pristine-checkout --repeats 20`. It uses
  synthetic SQLite archives only, measures the shipped `pending_heads_sql()`
  query, and runs the original implementation in separate subprocesses.

The full matrix used 1,000/10,000/100,000 completed sessions and
0/5/1,000 pending sessions. All nine cells matched the original selected
chunk, event IDs, parser state, counters and row deltas.

The step counts below were recorded for the earlier single-candidate query
and are kept as historical reference; the shipped query is now the
`pending_heads_sql()` heads query plus bounded per-head prefix lookups, and
`python tools/benchmark_can_pending.py --matrix` re-measures the current
shape on every run.

| Completed sessions | Idle VM steps before | Idle VM steps after (historical) |
| --- | ---: | ---: |
| 1,000 | 31,172 | 177 |
| 10,000 | 310,172 | 177 |
| 100,000 | 3,100,172 | 177 |

At five pending sessions the earlier query used 445 VM steps at every
history size; at 1,000 pending it used 48,205. Writer contention was
measured with a real competing `BEGIN IMMEDIATE` transaction; it is not
the same metric as an uncontended `accept()` duration. These synthetic
results do not establish production throughput.
