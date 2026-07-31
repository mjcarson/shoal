# 1–3. Acknowledged writes were not durable

Three defects in the intent log, filed separately and fixed together because each one alone
was enough to lose an acknowledged write. They are kept as one page because the fix was a
single rewrite of the write path.

## Symptom

A client received "inserted", and the row was not there after a power loss — or, in the worst
of the three, after a clean restart.

## Cause

| # | Defect |
| --- | --- |
| 1 | `commit` returned a hardcoded position rather than the offset of the record it had just written, so every response was parked against a meaningless watermark |
| 2 | The flush watermark reported a buffer's *start* offset, marking records durable that had not been written yet |
| 3 | No `fdatasync`. O_DIRECT skips the page cache but not the drive's volatile write cache |

Fixing those exposed two more: writes to the log were not block aligned, which O_DIRECT
requires, and rotation could race the writes it was supposed to be sealing.

## The fix

The write path was rewritten around `StreamWriter`:

- `commit` returns the log offset one past the record it wrote (`.../fs.rs:351`), and the
  response is parked against that position (`.../storage.rs:66-69`).
- Completions land in `FlushState`, shared between the writer and its detached IO tasks, which
  advances a **contiguous** watermark — a low-water mark, not a maximum. With `write_behind`
  defaulting to 128, io_uring completions routinely arrive out of order, so taking the highest
  completed offset would advance the watermark past data still in flight.
- The gate is `synced_pos`, not `written_pos`: an `fdatasync` group commits behind the
  watermark, and `PendingResponse::get` releases only what is below it
  (`.../storage.rs:102-130`).
- Every write to the log is block aligned, with padding regions marked by a sentinel the reader
  skips rather than mistaking for end of log.
- Rotation fsyncs before and after the rename.

`durability: Async` is the documented way to opt out, gating on `written_pos` and skipping the
fsync. It is honest about what it gives up.

## Invariants to uphold

- **A response may only be released once its record's offset is below the synced watermark.**
  Not the written watermark, and not the highest completed offset.
- **The watermark only advances over writes that have completed contiguously from the front of
  the queue.** Out-of-order completions are normal, not exceptional.
- **Every write to the log is block aligned**, including the padding that makes it so. The
  reader distinguishes padding from a record by sentinel, so a change to one is a change to
  both.
- **Rotation fsyncs on both sides of the rename**, or a crash can leave a sealed log that the
  filesystem has not committed to its new name.

## Tests

`.../fs/stream_tests.rs` covers padding and the flush watermark; `.../fs/tests.rs` covers the
reader's handling of truncated records, bad checksums, and pad regions.
`ack_survives_sigkill` (`shoal/tests/persistent_sorted_table.rs`) is the end-to-end proof: it
re-execs the test binary as a child, waits for the child to report an acknowledged write,
`SIGKILL`s it, and restarts against the same directory.

## Related

- [The Intent Log](../../storage/intent-log.md) — the design that replaced this
- [Storage Overview](../../storage/overview.md#durability-model)
- [Recovery](../../storage/recovery.md)
