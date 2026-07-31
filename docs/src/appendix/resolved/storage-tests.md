# 20. The storage tests were entirely commented out

The fixed half of item 20. The other half — `cursor.rs` and `response.rs` being orphaned,
uncompiled source files — is still open and stays in
[Known Issues](../known-issues.md#20-orphaned-source-files).

## Symptom

`.../fs/tests.rs` was 429 lines of commented-out tests. The storage engine's corruption
handling — truncated records, bad checksums, pad regions, inactive log discovery — had no
coverage at all, on the one path where being wrong is unrecoverable.

## Cause

The tests had been written against an earlier API and commented out rather than updated. They
built their fixtures through the storage engine itself, so every API change broke them, and
they described the layout under test only indirectly.

## The fix

They are live again, rewritten so that fixtures are built with plain `std::fs` and hand-framed
bytes:

```rust
fn entry(data: &[u8]) -> Vec<u8> {
    let mut hasher = GxHasher::default();
    hasher.write(data);
    let checksum = hasher.finish();
    let mut framed = Vec::with_capacity(16 + data.len());
    framed.extend_from_slice(&data.len().to_le_bytes());
    framed.extend_from_slice(&checksum.to_le_bytes());
    framed.extend_from_slice(data);
    framed
}
```

`.../fs/tests.rs`

That is the whole point of the rewrite: these tests are about how a reader reacts to a
malformed layout, so the layout has to be written out explicitly rather than produced by the
code under test. A fixture built by the writer can only ever contain what the writer believes.

They are joined by `.../fs/stream_tests.rs`, covering padding and the flush watermark, and by
`remove_intent_drops_an_entry` in `.../fs/map.rs`, which builds a map snapshot and an intent
log by hand and loads them back through the real loader.

## Invariants to uphold

- **Storage fixtures are written by hand, not by the writer under test.** Anything else tests
  the writer against itself.
- **Tests that touch DMA must run on a real filesystem.** `TempDir::new` uses `/tmp`, which is
  usually tmpfs, and glommio silently disables `O_DIRECT` there — so the test would exercise a
  buffered path where alignment is not enforced and `fdatasync` means nothing. Both test
  helpers build their temp dirs under `target/` for this reason
  (`.../fs/tests.rs`, `shoal/tests/utils.rs`).

## Related

- [The Intent Log](../../storage/intent-log.md)
- [Recovery](../../storage/recovery.md#truncation-and-corruption)
