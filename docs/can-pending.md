# CAN pending index and bounded decoding

`decode_once` reads pending session heads without scanning completed history,
prefetches an ordered raw prefix, and commits the decoded batch atomically.

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
- `candidate_sql()` returns `(id, session, seq)` for every pending session's
  persisted next chunk, ordered by raw ID. It binds the epoch twice and uses
  `CROSS JOIN` to keep `decode_pending` first; heads contain no raw BLOBs.
- A heap merges demand-sized indexed `(session, seq)` pages: each cold fetch
  takes the fair share of the remaining prefix need across frontier sessions
  without a buffered run (at most 64 chunks), so interleaved round-robin
  reads ~need/frontier rows per session instead of 64 each, while a lone
  contiguous session still gets full 64-row pages. Hot runs are reused
  without new queries. Exhausted pages refill before advancing to later
  heads. Unpaged heads remain in the merge, so page, byte and chunk limits
  cannot reorder sessions.
- One read snapshot prefetches at most 1 MiB of raw bytes and `limit` chunks
  across all sessions; the iterator may hold one additional wire-capped
  64 KiB row while checking the byte limit. Lightweight head memory scales
  with the number of pending sessions, not retained raw history.
- Prefetch starts at 1,000 chunks and adapts to successfully committed work
  including pre-prefix speculative waste: a staged shortfall, a byte-cap stop,
  or fetched rows beyond staged chunks shrink the next window to useful work.
  Fully useful completed work allows growth even when more work remains.
  Explicit smaller caller limits do not train the hint. The hint is nondurable;
  cursors remain authoritative. Per-call `archive._decode_prefetch_stats`
  reports fetched/prefix/staged chunks, fetched bytes and read queries.
- The read connection closes before decoding. Session metadata and parser
  state are reused within the batch; raw chunks are not concatenated, so
  completion timestamps, envelope IDs and frame ordinals remain unchanged.
- Canonical session identity bytes and signal paths are cached; envelope and
  event-time atoms are built only when a chunk emits rows. Counter defaults
  are copied instead of rebuilding a zero-filled `Counter` per chunk. These
  changes preserve canonical hashes, enum handling and timestamp precision.
- One `BEGIN IMMEDIATE` commit checks each touched session's cursor, inserts
  outbox rows in global arrival order, writes the final completed cursor and
  optional trailing partial per session, and bumps counters once per batch.
  Finishing a partial removes its checkpoint in the same transaction.
- The 2,000-row budget and 50 ms staging deadline are unchanged. The deadline
  is checked between decoded chunks, not a hard bound on prefetch, one chunk
  or commit duration. Cursor CAS, disk reserve and FULL/WAL durability remain.
- Benchmarks import the shipped `candidate_sql()` rather than carrying a
  second current-query implementation.
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
- Page/byte boundaries and partial completion:
  `python -m unittest tests.test_can_receiver.ReceiverBehavior.test_prefetch_page_and_byte_boundaries_preserve_order_and_partial_completion`
  compares every ordered output field, parser state and counter with a
  sequential decoder across 64-row pages, 64 KiB chunks and dense resumes.
- Demand-driven prefetch: `python -m unittest tests.test_can_prefetch -v`
  covers the 100x64 round-robin waste shape (no 1000-read/16-commit
  repetition), contiguous single-session full pages, byte-cap adaptive
  shrink, and boundary/order/partial/new-session/CAS/restart in one module.
- Split-frame merge and cursor-race rollback:
  `python -m unittest tests.test_can_receiver.ReceiverBehavior.test_prefetch_merge_keeps_arrival_order_split_resume_and_conflict`.
- Throughput: `python tools/benchmark_can_decode.py --baseline-root
  /path/to/pre-batch-checkout --chunks 10000 --repeats 3`. Each side runs its
  own receiver/decoder in an isolated subprocess. Seeding is outside timed
  decoding, wall-clock nanoseconds are fixed, and full ordered rows (including
  ingest timestamps), cursors, partials and persisted counters must match.
  Reports use median wall/CPU time; a separate instrumented pass observes
  cursor-fetched raw rows/bytes, read queries, connections and commits in both
  source trees. Retained-prefetch statistics are not baseline I/O evidence.
  Fixtures cover small fragments, whole frames, sparse/control traffic,
  interleaved and many sessions, dense/near-maximum-size chunks, and the
  1/4/100/400-session contiguous/round-robin/uneven long-backlog matrix
  (`xsession_contig`, `xsession_roundrobin`, `xsession_uneven`) with
  fetched/staged amplification, read-query counts and fetched bytes
  alongside wall/CPU time.
- A separate native CLI smoke uploaded synthetic OTLP over HTTP, verified
  unauthorized 401 and replay deduplication, killed the receiver during work,
  then restarted it to drain 121,001 chunks into 30,200 rows. Original raw
  hashes, committed row bytes/ingest timestamps, final parser states and
  counters were preserved; graceful SIGTERM exited 0. The downstream was
  deliberately unavailable, so this proves durable decode/outbox recovery,
  not successful Greptime ingestion.
- Pending-history scaling: `python tools/benchmark_can_pending.py --matrix
  --repeats 3 --secondary-repeats 1 --lock-hold-s 0.02`. The current-only run
  measures the shipped heads query and real competing-writer contention.
  An optional `--baseline-root /path/to/pristine-checkout` runs that tree in
  a separate subprocess; without it no baseline speedup is claimed.

### Decode throughput

Python 3.12, cantools 40.7.1, local macOS SQLite, three repetitions per fixture.
The baseline is a pre-batch archive; baseline/current execution order alternates.
Every fixture matched all output fields, ordered outbox IDs, parser checkpoints
and counters in all three comparisons. Times are medians of decode draining;
seeding, comparison serialization and connection instrumentation are not timed.

| Fixture | Before (s) | After (s) | Speedup |
| --- | ---: | ---: | ---: |
| 2,000 tiny serial fragments | 0.8784 | 0.0250 | 35.14x |
| 2,000 single-frame chunks | 0.9303 | 0.0567 | 16.40x |
| 2,000 sparse/control chunks | 0.8812 | 0.0323 | 27.25x |
| 2,000 interleaved chunks, 4 sessions | 0.9211 | 0.0415 | 22.21x |
| 2,000 chunks, 400 sessions | 1.9842 | 0.1346 | 14.74x |
| 16 dense chunks, 48,000 rows | 1.3775 | 1.4118 | 0.98x |
| 3 near-cap chunks, 17,700 rows | 0.4142 | 0.3996 | 1.04x |

Dense/large chunks remain decoder/row-work dominated; upload concurrency does
not address that cost. Current child peak RSS was approximately 103–190 MiB,
including the complete comparison snapshot, not a receiver-only memory bound.

### Public performance gate

`can-decode-perf` in `.github/workflows/compose.yml` compares the PR base or
push-before archive with the checkout using the seven synthetic fixtures above,
three alternating-order repetitions and `--max-slowdown 0.25`. Any final-output
mismatch or fixture median over 25% slower fails the job. JSON timing/equality
evidence is uploaded even on a regression. An initial repository push explicitly
records that no baseline exists; invalid or unavailable nonzero baselines fail.
There are no production inputs, credentials or absolute production-rate claims.

### Pending-history scaling

All nine current-only cells used the same query source. Query work stayed
constant as completed history grew; it scales with pending session count.
Newly appended data remains behind older pending chunks, preserving arrival
order rather than giving new sessions priority.

| Completed sessions | Idle VM steps | 5 pending VM steps | 1,000 pending VM steps |
| --- | ---: | ---: | ---: |
| 1,000 | 171 | 360 | 38,170 |
| 10,000 | 171 | 360 | 38,170 |
| 100,000 | 171 | 360 | 38,170 |

The packaged test `python -m tests.test_can_receiver_compose` passed against
isolated GreptimeDB 1.2.1 with File storage: 10,201 synthetic chunks produced
12,700 rows. It checked read-only code/rootfs and uid 10001, raw full ACK and
401, durable offline outbox, dense resume, every event ID and numeric value,
nanosecond timestamp predicates, dirty-table ACK, restart/replay deduplication,
and retention/drain across a real backend stop/start. Only test containers and
volumes were created and removed. All nine Compose profiles also validated.

These synthetic results and process-recovery checks do not establish sustained
production receive/decode/Greptime throughput or a declining operating backlog.
