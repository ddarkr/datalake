# RAW recorder/uploader bounded memory

Finalize (`finalize_segment`) and upload preflight (`fail_closed_check`
via `gunzip_compare`) never build proportional bytes/frame objects.

## Budgets

- `RAW_FINALIZE_MEM_FRAMES` (default 20000, must be > 0): max resident
  decoded frames across sort AND identity match staging (match batches
  are `min(2000, budget)`). Larger segments spill arrival batches into
  ONE temp SQLite file (`frames.sqlite3`) indexed by `(twall_ns, seq)`;
  ordered reads page one cursor/one frame, never a run-file list, heap,
  or multi-FD fan-in. Identity matching stages into `match.sqlite3` in
  the same temp dir with the same batch ceiling. Budgets <= 0 raise
  `ValueError` (fail fast, never silent `max(1, ...)`).
  This bounds application frame staging, not total process RSS: SQLite,
  Python and MF4 libraries have their own buffers. Measure RSS separately.
- `RAW_TMP_MAX_BYTES` (default 2 GiB, must be > 0): ONE aggregate over the
  simultaneously live sort dir (`frames.sqlite3` + journal/WAL/shm/index),
  match DB/files (`match.sqlite3*`), and the staged MF4
  (`<stem>.stage.tmp`) living beside the dir, enforced via
  `store._note_live` after staging and on every match batch/index, and
  recording the true combined `peak_temp_bytes`. Partial sums can each fit
  while the joint total exceeds the cap; that trips `NoSpace` too.
  This is not an OS filesystem quota; an individual write can cross the
  threshold before the check. Exceeding it raises `NoSpace`: the closed
  JSONL stays, nothing is published, and temp handles/directories are
  cleaned before retry. No segment size cap drops frames silently.
- `RAW_PREFLIGHT_CHUNK_BYTES` (default 1 MiB, min 4096): gunzip/compare
  chunk for `gunzip_compare`. Multi-member gzip verifies every member;
  truncated members fail closed.
- Ingress lines: `_LINE_MAX_BYTES` (1 MiB) caps one line buffer read in
  fixed 64 KiB binary chunks. Over-cap whitespace-only runs skip like
  blank lines; over-cap content, mid-file damage, newline-terminated
  corruption, or UTF-8 damage anywhere (including an unterminated tail)
  quarantines via `CorruptSegment` (never salvaged, `.torn` stays False).
  Only an unterminated decodable tail sets `.torn`.

- Temp dirs are `sealed/<stem>.*` holding `frames.sqlite3` (+ journal/WAL/shm)
  and `match.sqlite3` during verify, plus the staged MF4
  (`<stem>.stage.tmp`) living beside the dir until the sealed rename.
  `finalize_segment` sweeps stale ones for its stem on entry (legacy
  `run-*.tmp` dirs and a stale `<stem>.stage.tmp` too);
  `recover_spool` sweeps all of them on restart. The closed JSONL they
  came from is still on disk, so sweeping never loses data.
- `store.destroy()` closes the SQLite handle first, then removes the
  temp dir, on success, empty-segment return, corruption quarantine,
  NoSpace, and verify-failure paths. `finalize_segment(..., stats={})`
  reports `peak_temp_bytes` (true combined max: live sort dir + match
  files + staged MF4), `peak_resident_frames` (max resident incl. match
  batches), `budget_frames`, `tmp_max_bytes` for the benchmark hook;
  sealed outputs never count as temporary.

## Semantics preserved

- Torn-tail salvage (unterminated last line only), mid-file and
  newline-terminated corruption quarantine, UTF-8 failure quarantine.
  Zero decodable frames (empty file, whitespace/newlines only, or a lone
  torn tail such as `b'{"v":'` with nothing salvageable) is the prior
  empty policy: `finalize_segment` returns `None`, unlinks the empty
  closed JSONL, and publishes no triple with no temp leftovers.
- Chronological MF4 order by `(twall_ns, seq)`; float-equal timestamps,
  Data/Error/Remote reorder, duplicate identities consume timestamps
  chronologically; full payload/DLC/channel/flags/identifier/capture-time
  verification before the sealed rename.
- Manifest `v:2` fields, hash pins, sidecar name, and fail-closed triple
  checks unchanged. Corruption quarantines (never uploads, never deletes
  originals); partial sets stay invisible; remote ACK still deletes.
- `read_ingress_frames` / `matched_mf4_frames(mf4, list)` /
  `verify_sealed_mf4(mf4, list)` contracts unchanged for redecode
  callers; the bounded default path takes a `SortedFrameStore` instead.
- Observational CAN validation folds one window with exact counters;
  the per-stem once-guard is claimed after a successful fold only.

## Measuring

```bash
python -m tools.benchmark_raw_memory --steps=5000,20000,100000 \
  --memory-frames=50 --baseline-root=/path/to/pristine-checkout
```

Per step it prints finalize and preflight wall/CPU seconds, spawned-child
peak RSS KiB, peak temp bytes (sort + match spill + journals + staged
MF4; sealed outputs never count), peak resident frames with source
(`hook` when the finalize `stats={}` contract reports it, else derived),
and sealed bytes. Numbers are environment-specific; record actuals after
running, never from estimates.

## Measured

Measured with real pinned MF4 libraries and synthetic frames in separate
spawned processes; all three manifest/provenance comparisons and upload
preflight checks matched the original implementation.

| Frames | Finalize RSS before/after (KiB) | Finalize wall before/after (s) | Temp peak (bytes) | Preflight RSS (KiB) |
| --- | ---: | ---: | ---: | ---: |
| 5,000 | 101,472 / 100,224 | 0.742 / 1.317 | 5,136,976 | 30,992 |
| 20,000 | 151,120 / 114,736 | 1.879 / 3.115 | 20,488,920 | 33,312 |
| 100,000 | 300,368 / 128,800 | 8.286 / 14.308 | 102,941,360 | 33,248 |

Application frame staging stayed at 50 in every case. Lower memory costs
additional SQLite IO and CPU; these results do not claim a speedup or a
hard whole-process RSS limit. Peak temporary storage includes the match
database, not just sort spills.

Related: [vehicle operations](operations.md) for spool layout and ACK rules.
