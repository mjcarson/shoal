# Known Issues

A severity-ranked index of defects found by reading the code on the `ZeroCopyResponses`
branch. Each entry names the symptom, the cause, and a `file:line`.

**How these were established.** Everything here comes from reading the source, except item 5,
which was reproduced against a running server (see below). Nothing here has been fixed —
this book is documentation, not a patch. Line numbers drift.

**Baseline as of writing:** `cargo check --workspace --all-targets` passes with warnings;
`cargo test --workspace` passes — 13 integration tests, 0 unit tests.

---

## Critical — durability

These three compound. Together they mean **a write acknowledged by Shoal today is not
guaranteed to be on disk.** All three trace to the intent-log writer rewrite this branch is in
the middle of.

### 1. `commit` does not report a log position

`shoal-core/src/server/tables/storage/fs.rs:347`

```rust
self.intent_log2.consume(total_size).await;
// TODO this should use channels to mark how much was consumed
Ok(0)
```

`commit` is supposed to return the intent log offset at which the write will have landed.
It returns `0`.

That position is what a response is parked against:

```rust
self.pending.add(meta, pos, action);
```

`.../persistent/sorted.rs:369`

and what releases it:

```rust
Some((pending_pos, _, _)) => flushed_pos >= *pending_pos,
```

`.../server/tables/storage.rs:78`

With `pending_pos == 0`, that test is unconditionally true. Every write is acknowledged on the
next `handle_flushed` regardless of whether any IO has completed. The entire
`PendingResponse` mechanism is inert.

**Fix direction:** return `self.intent_log2.get_unflushed_pos()` after `consume`, i.e. the
offset one past this record.

### 2. Flush watermark is the buffer's start offset

`.../fs/stream.rs:108-115`

```rust
file.write_at(buff, pos).await.unwrap();
let msg = ServerMsg::DataFlushed { table, flushed: pos };
```

`pos` is where the buffer was written *from*, not where it ends. The watermark therefore lags
by one full buffer: records in the buffer that just completed are not covered by the position
that completion reports. Once item 1 is fixed, this would leave every write waiting for the
*next* buffer to complete before being acknowledged.

**Fix direction:** report `pos + buff.len()`.

### 3. No `fdatasync` on the steady-state write path

`.../fs/stream.rs:255-264`

```rust
pub async fn sync(&mut self) -> Result<(), ServerError> {
    if self.buff_pos > 0 { self.write(self.default_buffer_size).await.unwrap(); }
    Ok(())
}
```

`StreamWriter::sync` — reached from the shard's idle flush (`shard.rs:656`) — issues another
background write and returns. It does not wait and does not `fdatasync`.

`sync_blocking` does fsync (`.../fs/stream.rs:267-277`), but is only reached from `refresh`
during log rotation. So between rotations, Shoal never forces data to stable storage.
"Flushed" means "submitted to the kernel".

The `pending_sync` field appears to be scaffolding for a background fsync task that was never
written — it is only ever `take()`n (`.../fs/stream.rs:272`, `:323`).

---

## High — data loss and silent failure

### 4. Unsorted updates and deletes never consult disk

`.../persistent/unsorted.rs:540` (delete), `:591` (update)

```rust
match self.partitions.remove(&key) {
    Some(old) => { /* write intent, ack true */ }
    None => { /* respond Delete(false) */ }
}
```

Neither calls `storage.load_partition`, unlike `get` (`:409`) and `exists` (`:497`) in the same
file, and unlike every sorted equivalent (`.../persistent/sorted.rs:698-720`, `:857-876`).

So an update or delete against an unsorted partition that is on disk but not resident — after
eviction, or after a restart before it has been faulted in — silently reports `false` and does
nothing. The caller is told the row does not exist when it does.

### 5. Pruned partitions leak a stale archive map entry

`.../fs/compactor.rs:198-203`

```rust
if let ShouldPrune::Yes = T::apply_intents(&mut self.loaded, partition, intents) {
    self.loaded.remove(&partition);
    // TODO: does anything else need to be done to remove this partition
    // from archive maps?
}
```

The in-code TODO is right to worry. When compaction empties a partition it is dropped from
`loaded` and never rewritten — but its old `ArchiveEntry` stays in `to_archive`, still
pointing at the pre-delete copy in the old archive. **Deleted data is resurrected.**

**This one is confirmed, not inferred.** Reproduced with a temporary integration test against
a real server:

| Session | Action | `exists` |
| --- | --- | --- |
| 1 | insert row, shut down | — |
| 2 | restart (insert compacts into an archive), delete row | `false` ✅ |
| 3 | restart — delete intent replayed from the log | `false` ✅ |
| 4 | restart — delete intent has been compacted away | **`true` ❌** |

By session 4 the delete intent has been compacted and its log deleted, the tombstone exists
nowhere, and the map still points at the original archive extent. The row returns.

The existing test named `delete_survives_restart`
(`shoal/tests/persistent_sorted_table.rs:232`) does not catch this: despite its name it
deletes and checks `exists` in the *same* session and never restarts afterwards.

**Fix direction:** on `ShouldPrune::Yes`, remove the key from `to_archive` and log a map
intent recording the removal. `MapIntent` has no variant for this today — it would need one.

### 6. Negative `isize` cast collapses memory accounting

`.../persistent/sorted.rs:262-263`

```rust
let new_mem_usage = self.memory_usage.borrow().saturating_sub(diff as usize);
```

Reached only when `diff` is negative (the `else` branch of `if diff.is_positive()`). Casting a
negative `isize` to `usize` wraps to a value near `usize::MAX`, so the saturating subtraction
floors shard memory usage at **0**.

The shard then believes it is using no memory and stops evicting until the counter climbs back
above the limit — with the true resident set already past it.

**Fix direction:** `saturating_sub(diff.unsigned_abs())`.

### 7. `limit` is ignored by persistent sorted tables

`.../persistent/sorted.rs:436-448`

The get loop is inlined in the table and has no limit check. `SortedPartition::get` implements
it correctly (`.../tables/partitions.rs:278-284`) but is not the method that runs.

So `limit` is parsed by SHQL, type-checked, serialized, sent, and discarded. A query with
`LIMIT 10` against a million-row partition returns a million rows.

### 8. Sort keys are accepted and ignored

`SortedGet::sort_keys` (`shared/queries/sorted.rs:78`) and `SortedExists::sort_keys` (`:106`)
are populated by clients, carried through `to_blocked` (`:91-98`), and never read by the
server. A get always scans every live row in the partition.

Consequences: no point lookup by sort key, no range scans, and SHQL `WHERE sort_key = 'x'`
returns the whole partition. Deletes and updates *do* use the sort key, so the field is only
inert on read paths.

### 9. Recovery and compaction panic on orphaned update intents

`.../persistent/unsorted.rs:867-868`

```rust
// TODO handling a partition missing
None => panic!("Missing partition?"),
```

and

`.../persistent/unsorted.rs:896-901`

```rust
UnsortedIntents::Update(update) => match &mut maybe_partition {
    Some(partition) => partition.update(&update),
    None => panic!("Applying update to no partition?"),
},
```

The first fires during startup replay when an update's base partition is not resident and
`scan`'s `load_partition_direct` found nothing. The second fires during compaction when a log
contains an update whose insert was compacted in an earlier generation — `apply_intents` for
unsorted tables starts from `None` rather than from the partition's current archive copy.

Both are crashes on the startup path, which is the worst place for them: a shard that cannot
start cannot be recovered without deleting data.

---

## Medium — robustness

### 10. `end` flag computation is wrong for streams

`shoal-core/src/server/shard.rs:422-435`

```rust
let end_index = queries.queries.len() - 1;
for (mut index, kind) in queries.queries.into_iter().enumerate() {
    ...
    index += queries.base_index;
    let end = index == end_index;
```

Two defects. `len() - 1` underflows and panics on an empty bundle. And `index` is adjusted by
`base_index` while `end_index` is not, so for any streamed bundle with `base_index > 0` the
comparison is meaningless — usually never true, so `end` is never set.

Masked today because streaming clients set `unbounded_queries: true` and ignore the server's
`end` entirely, terminating on a locally generated `ClientMsg::End`
(`client.rs:1308-1313`).

### 11. Ring lookup panics on an empty ring

`shoal-core/src/server/ring.rs:60-65`

```rust
None => self.ring.range((Included(&0), Excluded(&partition))).next().unwrap(),
```

The wrap-around fallback `.unwrap()`s. If a query is routed before any `ServerMsg::Join` has
been processed — a client connecting during the startup window — the shard panics. Each shard
builds its ring from broadcasts, so the window is real if small.

### 12. Vnodes provide no load smoothing

`shoal-core/src/server/ring.rs:26-44`

Every shard lays down 1000 vnodes at the same fixed stride `RING_JUMP = u64::MAX / 1000`, with
only the starting offset varying by name hash. Because all combs share one period, the ring
repeats every `RING_JUMP` with exactly N vnodes per period, so **each shard's share is
identical to what it would get from a single vnode.**

The variance reduction that vnodes normally provide is absent; shard shares are the gaps
between N uniformly random points on a circle. For 16 shards the busiest should be expected to
own roughly 3× the mean rather than ~1.1×. The 1000 entries cost a `BTreeMap` of 1000 × N per
shard and buy nothing.

**Fix direction:** hash `(name, i)` per vnode so positions are independent.

Full reasoning in [Partitioning](../architecture/partitioning.md#the-vnodes-do-not-do-what-vnodes-usually-do).

### 13. Eviction logging can underflow

`.../persistent/sorted.rs:1010`, `.../persistent/unsorted.rs:685`

```rust
event!(Level::INFO, pre, post, diff = pre - post, ...);
```

A plain `usize` subtraction inside a log statement. Any accounting drift leaving `post > pre`
panics the shard — from the logging, not the logic. Given item 6 and the accounting
inconsistencies in item 22, this is reachable.

### 14. Empty rotated intent logs are never deleted

`.../fs/compactor.rs:298-309`

```rust
self.sort_intent_log(&path).await?;
if !self.changes.is_empty() {
    ...
    glommio::io::remove(path).await?;
}
```

The removal is inside the `if`. A rotated log that produced no changes — an empty log, or one
whose records all failed to parse — is left on disk forever and replayed on every startup.
Since startup always forces a rotation (`.../persistent/sorted.rs:210`), a table that is never
written accumulates one orphan file per restart.

### 15. No backpressure anywhere

Every channel is `kanal::unbounded_async`: the shard mesh (`comms.rs:33`), per-client response
channels (`shard.rs:131`), compaction jobs (`fs.rs:276`), loader requests (`sorted.rs:175`).

Sends never block, so no shard can deadlock on another — but nothing throttles a client
either. A shard that falls behind grows its queue until the process is killed. `blocked` and
`pending_data` are likewise unbounded, as is `PendingResponse`.

The one exception is `StreamWriter`'s `max_write_behind` (`.../fs/stream.rs:157-164`), which
bounds in-flight writes only.

### 16. Panics on the hot path

239 `.unwrap()` calls and 32 `panic!`s outside `target/`. The ones on live request paths:

| Site | Trigger |
| --- | --- |
| `shard.rs:61` | Any non-EOF socket error from a client |
| `shard.rs:68` | Failed read of a request body |
| `shard.rs:104`, `:106` | Zero-length or failed socket write |
| `shard.rs:493` | Reply for a client with no channel |
| `shard.rs:623` | Client UUID collision |
| `comms.rs:53`, `:72` | Unknown shard contact |
| `ring.rs:65` | Empty ring (item 11) |
| `.../fs/stream.rs:108`, `:115` | WAL write or notification failure |
| `.../fs/loader.rs:138`, `:158` | Any loader task error |
| `shared/traits.rs:54` | rkyv serialization failure |
| `.../persistent/sorted.rs:245`, `:355`, `:453`, `:599`, `:734`, `:890` | Corrupt archive data |

There is also a live `todo!()`:

```rust
if let Err(_) = self.spawn_task(table_name, partition_id).await {
    todo!("Add back onto loader channel");
}
```

`.../fs/loader.rs:126-129`

A partition load that fails to spawn panics the loader. Any query blocked on that partition
then waits forever, since there is no timeout ([item 15](#15-no-backpressure-anywhere)).

---

## Low — hygiene and documentation drift

### 17. Leftover debug `println!`s

| Location | Content |
| --- | --- |
| `.../persistent/sorted.rs:546`, `:553`, `:593`, `:594`, `:613`, `:639` | Six lines in `exists`, one of which `{:#?}`-prints an entire partition |
| `.../fs.rs:366-370` | "Compacting ->" on every log rotation |
| `.../server/conf.rs:111` | "listening on ..." from inside `Networking::to_addr` |

All bypass the tracing level filter. Visible in any test run — see the sample output in
[Observability](../operations/observability.md#debug-output-that-is-not-tracing).

### 18. `exluded_cores` is silently ignored

The checked-in `shoal.yml` sets `exluded_cores`; the struct field is `exclude_cores`
(`shoal-core/src/server/conf.rs:21`). The `config` crate ignores unknown keys, so core
exclusion never takes effect. `CLAUDE.md` reproduces the typo.

### 19. `memory` has no serde default

`shoal-core/src/server/conf.rs:22-24` — the only field in `Resources` without a default. A
`resources:` block omitting `memory` fails to deserialize. Omitting the whole block yields
`memory: 0`, so eviction runs continuously.

### 20. Orphaned source files

`shoal-core/src/server/cursor.rs` and `shoal-core/src/server/response.rs` are not declared in
`shoal-core/src/server.rs:14-22` and are not compiled. They reference APIs that no longer
exist (`crate::ShoalRow`, `rkyv::AlignedVec`). Dead.

Also: `shoal-core/src/server/tables/storage/fs/tests.rs` is **429 lines of entirely
commented-out tests** — covering exactly the intent-log truncation, checksum, and map
corruption behaviour documented in [Recovery](../storage/recovery.md). The module is declared
`#[cfg(test)] mod tests;` (`.../fs.rs:31-32`) and compiles to nothing. `shoal-core` has zero
active unit tests.

### 21. Constant and comment mismatches

| Constant | Comment says | Value is |
| --- | --- | --- |
| `default_intent_log_size` (`.../fs/conf.rs:28-31`) | 100 MiB | `10 << 20` = 10 MiB |
| `MIN_ARCHIVE_COMPACTABLE` (`.../fs/compactor.rs:31-33`) | 100 MiB | `10 << 20` = 10 MiB |

Also `RemoteTracing::Grpc` exports over HTTP (`trace.rs:36-40`), and
`FileSystemThroughputWriterConf::write_behind` — a count — is deserialized with
`deserialize_byte_size` (`.../fs/conf.rs:116-118`).

### 22. Size accounting inconsistencies

- `UnsortedPartition::new` sets `size = row.deep_size_of() + 17`
  (`.../tables/partitions.rs:81`); `UnsortedPartition::update` sets
  `size = self.deep_size_of()` (`:112`). Different bases for the same field.
- `SortedPartition` tombstones subtract the row's bytes but the tombstone still occupies a
  `BTreeMap` slot, so delete-heavy partitions under-report.
- Sorted partition sizes are maintained by delta and never recomputed, so they drift.

### 23. Client stream and pool rough edges

- `ShoalResultStream::skip(0)` panics: `skip -= 1` precedes the zero check
  (`client.rs:979-986`).
- `is_valid` / `has_broken` use `peer_addr()`, which does not probe the peer
  (`client.rs:76-90`, marked TODO).
- `Shoal::send` archives the bundle before `track_response` may regenerate its id
  (`client.rs:211-215`).
- Two large commented-out blocks remain (`client.rs:544-598`, `:1025-1091`).
- `suceeded` / `QuerySuceededOpts` are misspelled in the public API.

### 24. shoalctl warnings

```rust
panic!("{:#?}", self.error);
return;
```

`shoalctl/src/components/tab.rs:144-145` — an unconditional panic where an error should be
rendered, plus an unreachable `return`. Because a panic bypasses `ratatui::restore()`, it can
leave the terminal in raw mode. `TabState::next` / `prev` are dead code (`:430`, `:440`).

### 25. CLAUDE.md drift

| Claim | Reality |
| --- | --- |
| `Conf::new("shoal.yml")` | The method is `Conf::from_file` (`conf.rs:269`) |
| "LRU eviction at 60%" | Eviction triggers when usage exceeds the configured limit exactly; 40% is then freed (`shard.rs:661`, `:557`) |
| `exluded_cores` | Should be `exclude_cores` |
| Lists `EphemeralTable` as a usable table type | It does not satisfy the interface `#[db]` generates calls against |

---

## Unsafe `Send` invariant

Not a bug, but the most dangerous thing in the codebase to change without knowing.

```rust
/// # Safety
///
/// The Partition variant should not be sent across threads ever.
unsafe impl<D: ShoalDatabase> Send for ServerMsg<D> where ... {}
```

`shoal-core/src/server/messages.rs:131-140`, and similarly for `Comms`
(`shoal-core/src/server/comms.rs:16-23`).

`ServerMsg::Partition` carries a glommio `ReadResult`, which is not `Send`. The whole enum
asserts `Send` anyway so it can travel kanal channels. The invariant — a `Partition` message
may only ever be sent on its own shard's channel — is upheld solely by `FsLoader` being
constructed with a clone of its own shard's sender and no other
(`.../fs/loader.rs:70-85`, `:48`).

Give a loader, compactor, or any task a sender belonging to a different shard, and `Partition`
messages become cross-thread: undefined behaviour, no compiler error, and probably no test
failure. Any change touching loader construction should be read against this.

---

## Suggested triage order

1. **Items 1–3** — durability. Nothing else about persistence can be assessed until
   acknowledgement means something. They are one coherent piece of work: finish the writer
   migration.
2. **Item 5** — confirmed data resurrection, and the fix is well understood.
3. **Item 4** — silent no-op mutations on unsorted tables.
4. **Item 6** — one-line fix that restores memory accounting.
5. **Items 7 and 8** — features that appear to work and do not; either implement or reject at
   the API boundary.
6. **Items 9, 11, 16** — startup and hot-path panics.
7. **Item 20** — re-enable the commented-out storage tests, which already cover several of
   the behaviours above.
