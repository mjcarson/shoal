# Known Issues

A severity-ranked index of open defects on the `ZeroCopyResponses` branch. Each entry names
the symptom, the cause, and a `file:line`.

**How these were established.** Everything here comes from reading the source unless an entry
says otherwise. Line numbers drift.

Performance findings are catalogued separately in [Optimizations](optimizations.md), and what the
test suite does and does not reach is in [Test Coverage](test-coverage.md).

Defects that have been fixed move to [Resolved Issues](resolved-issues.md), one page each,
carrying the reasoning and the invariants the fix depends on. Item numbers are shared between
the two pages and never reused, so a number appears on exactly one of them — which is why this
list starts at 9 and skips 10, 26, and 39. The exceptions are items 9, 20, and 24, which were
only partly fixed: the open remainder is here and the rest is there.

**Baseline as of writing:** `cargo check --workspace --all-targets` passes with warnings;
`cargo test --workspace` passes — 132 integration tests (one ignored), 159 `shoal-core` unit
tests, 10 doctests. That is up from 14 and 32 with the addition of SHQL coverage
([SHQL](../api/shql.md#testing)), the restart and eviction tests added with items 4 and 5, the
limit and cross-shard coverage added with item 7, the row-order and `IN`/`OR` coverage added
with items 26 and 39, the sort-key selection coverage added with item 8, and the range coverage
added with [F1](../features/sort-key-ranges.md). The counts before F1 were 115, 129, and 8, and
before item 8 were 105 and 116; they were re-run and confirmed unchanged when items 31–38 were
added.

---

## High — data loss and silent failure

### 9. Orphaned update intents are dropped silently

The panics this item was filed for are
[fixed](resolved/orphaned-update-intents.md). What is left is the observability half: an intent
whose base row is genuinely gone is dropped with only a `warn!`
(`.../persistent/unsorted.rs:1014`, `:1056`), and nothing counts it. There is no metric, no
error surfaced to an operator, and no way to answer "did this restart lose anything?" other
than grepping logs.

That is the same gap as the mid-log corruption case in
[Recovery](../storage/recovery.md#truncation-and-corruption), and both want the same thing: a
counter of intents discarded during recovery, reported once at the end of startup.

### 31. Multi-log recovery discards already-replayed intents

`FileSystem::read_intents` (`.../storage/fs.rs:449-463`) replays every inactive intent log into
one shared `partitions` map, in generation order, before replaying the active log. Each
`replay_intent_log` (`.../storage/fs.rs:191-213`) runs its whole **scan** pass over the log
before its **replay** pass, and `scan` inserts unconditionally:

```rust
// wrap this partition as being accessible
let wrapped = MaybeLoaded::Accessible(partition_read);
// load this partition
partitions.insert(partition_key, wrapped);
```

`.../persistent/sorted.rs:1177-1179`, and the same shape in `.../persistent/unsorted.rs`.

A partition that an earlier generation's log already replayed into as `MaybeLoaded::Loaded` is
overwritten by the copy read back from the archive, and every intent replayed into it is lost.
The archive predates those intents by definition — that is why they were still in a log.

The memory accounting drifts with it: `sorted.rs:1175` adds the archive read's length to
`memory_usage` without subtracting whatever it displaced.

Reaching this needs two or more inactive logs — that is, compactions that were interrupted, which
is exactly the crash case recovery exists for — the same partition touched in both, an `Update`
intent in the later one, and the partition present in an archive. Single-log recovery is
unaffected, which is why `update_intent_replay` does not catch it.

**Fix direction:** `scan` should leave an existing entry alone rather than overwrite it. The
distinction it needs is the one `load_partition` (`sorted.rs:256-295`) already draws between a
`Vacant` and an `Occupied` entry — and note that the `Occupied` arm there has its own gap,
[item 30](#30-a-sorted-partition-load-can-be-silently-thrown-away). Both are the same underlying
question: what should happen when a copy read from disk meets a copy already in memory.

---

## Medium — robustness

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

`.../persistent/sorted.rs:1106`, `.../persistent/unsorted.rs:811`

```rust
event!(Level::INFO, pre, post, diff = pre - post, ...);
```

A plain `usize` subtraction inside a log statement. Any accounting drift leaving `post > pre`
panics the shard — from the logging, not the logic. Given the accounting inconsistencies in item
22, this is reachable.

### 14. Empty rotated intent logs are never deleted

`.../fs/compactor.rs:316-338`

```rust
let partitions = if self.changes.is_empty() {
    Vec::default()
} else {
    ...
    glommio::io::remove(path).await?;
    partitions
};
```

The removal is inside the branch that had something to compact. A rotated log that produced no
changes — an empty log, or one whose records all failed to parse — is left on disk forever and
replayed on every startup. Since startup always forces a rotation
(`.../persistent/sorted.rs:218`), a table that is never written accumulates one orphan file per
restart.

The generation is now reported either way (`.../fs/compactor.rs:334-336`), so an empty log no
longer pins everything tagged with its generation — but the file itself still accumulates.

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

### 27. SHQL cannot express a string containing a single quote

`string_literal` is `delimited("'", take_till(0.., '\''), "'")`
(`shoal-core/src/shared/queries/parser.rs:207-211`). There is no escape syntax — not doubling
(`''`), not backslash.

This is worse than a missing convenience. Any row whose **partition key** is a string
containing an apostrophe is unreachable from SHQL entirely, because the partition key is the one
condition every query must supply. `WHERE title = 'it''s'` does not parse, and there is no
spelling that works.

**Fix direction:** doubling is the SQL-standard form and the smaller change — replace the
`take_till` with a loop that accumulates until an unescaped quote, treating `''` as a literal
quote. Backslash escapes would also work but diverge from SQL. Either way the byte offsets
recorded in `WhereClause` must continue to span the *raw* literal including its quotes, since
that is what error rendering slices; the decoded value and the source span will no longer be the
same length.

### 32. A disconnected client is never cleaned up anywhere

Nothing removes an entry from `client_map` (`shard.rs:254`), and `ServerMsg` has no variant for a
client going away. `client_rx_relay` breaks its loop on EOF (`shard.rs:57-61`) and tells nobody.

Because `client_acceptor` broadcasts `NewClient` to every shard (`shard.rs:138-140`), every shard
holds a clone of that client's `client_tx` for as long as the process runs. So the channel never
closes, `client_tx_relay`'s `recv()` never returns `Err`, and the task never exits
(`shard.rs:86-91`).

Per connection that has already gone away, permanently:

| Leaked | Where |
| --- | --- |
| One `client_map` entry | Every shard |
| One `kanal` channel | Every shard holds the sender |
| One detached glommio task | The accepting shard |

A response that arrives for a dead client is not an error either — it is sent into an unbounded
channel ([item 15](#15-no-backpressure-anywhere)) that nothing will ever read.

This is the cost of a *disconnect*, not of a failure: an ordinary client that opens a pool, does
its work, and exits leaves all of it behind. The client's own pool is 50 connections
(`client.rs:140-148`).

**Fix direction:** a `ServerMsg::ClientGone` broadcast from `client_rx_relay` when its loop ends.
Dropping the sender from every `client_map` is what closes the channel, which is what lets
`client_tx_relay` return on its own.

### 33. Collected split-query state has no expiry

A `Gather` is inserted when a query is split across shards (`shard.rs:470-480`) and removed only
when `outstanding` reaches zero (`shard.rs:643-651`). Nothing else ever removes one.

A shard that never sends its share leaves the entry resident forever and the client waiting
forever. That is not hypothetical: a partition load that fails to spawn hits the `todo!()` in
`.../fs/loader.rs:126-129` and panics the loader, stranding every query blocked on it
([item 16](#16-panics-on-the-hot-path)), and there is no timeout anywhere to break the wait
([TODOs](todos.md#timeouts)).

Client disconnect does not clear them either, so this compounds with
[item 32](#32-a-disconnected-client-is-never-cleaned-up-anywhere).

### 34. The request length prefix is unvalidated

```rust
// parse the upcoming messages size
let len = u64::from_le_bytes(len_bytes) as usize;
// allocate a buffer that is exactly the right size
let mut data = BytesMut::zeroed(len);
```

`shard.rs:66-68`

The length is taken from the wire and used as an allocation size directly, before a single byte
of the body has been read. There is no maximum message size in the protocol
([Wire Protocol](../architecture/wire-protocol.md)), so a corrupt or hostile prefix asks for up to
`usize::MAX` bytes. The relay also `panic!`s on the read that follows
([item 16](#16-panics-on-the-hot-path)), so a truncated message takes the shard down rather than
the connection.

`zeroed` is also pure waste — `read_exact` overwrites every byte of it on the next line.

### 36. A partial intent log buffer is only written when the shard's channel drains

```rust
// if we have no more messages then flush our current queries to disk
if self.shard_local_rx.is_empty() {
    self.tables.flush().await?;
}
```

`shard.rs:787-789`

`StreamWriter::prep` and `consume` (`.../fs/stream.rs:519-546`) write only when the staging buffer
fills, so this `is_empty()` check is the only other path by which staged data reaches disk. Writes
are acknowledged only once durable ([Resolved Issues #1-3](resolved/durability.md)), so whether a
client hears back depends on the shard's channel happening to run dry.

Under sustained load it does not. Worse, the writer's own `DataFlushed` wakeups
(`.../fs/stream.rs:373`, sent per completed write) are themselves messages on that channel, so
write traffic helps keep the condition false. The escape is an intent log rotation, which syncs
unconditionally (`.../fs.rs:383`) — meaning a trailing write can wait for up to
`intent_log_size`, 10 MiB by default, of *other* traffic before its client is answered.

Not a durability bug: nothing is acknowledged that is not durable. It is an unbounded
acknowledgement delay for the last writes before a lull.

### 38. Integration test binaries all bind the same ports

`shoal/tests/utils.rs:48-53` hands out ports from a counter:

```rust
static PORT_COUNTER: AtomicU16 = AtomicU16::new(13000);
fn get_unique_port() -> u16 { PORT_COUNTER.fetch_add(1, Ordering::SeqCst) }
```

The counter is per test *binary*. Cargo runs binaries in parallel, so every binary starts handing
out 13000, 13001, 13002 at the same time. Measured by capturing the `listening on` line
(`conf.rs:111`) from each binary in turn: `persistent_sorted_table` binds 13000-13034,
`persistent_unsorted_table` binds 13000-13021, and **all 22 of the unsorted binary's ports are
also bound by the sorted one**.

The bind does not fail, which is what makes this worth an entry. Glommio sets `SO_REUSEPORT` on
listening sockets (`glommio/src/net/tcp_socket.rs:135`), so the second bind succeeds silently and
the kernel load balances incoming connections between the two servers. A client in one test can
therefore have its connection handed to a server owned by another test — a different schema, a
different temp dir — with no error anywhere to say so.

`persistent_sorted_table.rs:626` additionally hardcodes `let port = 13900`, so two concurrent runs
of that one binary collide with each other regardless of the counter.

**This has not been observed to fail.** `cargo test --workspace` was run four times while
investigating and passed every time; the two binaries are simply never alive on the same port at
the same moment. Nothing enforces that — it is timing, and it is the safety net every other item
on this page is checked against.

**Fix direction:** bind port 0 and read back the assigned port, which removes the shared namespace
entirely. Failing that, give each binary a distinct base offset — but that only moves the
collision to the next binary someone adds.

---

## Low — hygiene and documentation drift

### 17. Leftover debug `println!`s

| Location | Content |
| --- | --- |
| `.../persistent/sorted.rs:577`, `:584`, `:629`, `:630`, `:649`, `:681` | Six lines in `exists`, one of which `{:#?}`-prints an entire partition |
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

The other half of this item — `.../fs/tests.rs` being 429 lines of commented-out tests — is
[fixed](resolved/storage-tests.md).

### 21. Constant and comment mismatches

| Constant | Comment says | Value is |
| --- | --- | --- |
| `MIN_ARCHIVE_COMPACTABLE` (`.../fs/compactor.rs:31-33`) | 100 MiB | `10 << 20` = 10 MiB |

(`default_intent_log_size` had the same mismatch and has been corrected to say 10 MiB.)

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

`TabState::next` / `prev` are dead code (`shoalctl/src/components/tab.rs:527`, `:537`), and
`submit_query` `tokio::spawn`s the query only to `.await` the join handle immediately
(`:254-258`), so the UI blocks for the round trip anyway.

The panic that could leave a terminal in raw mode is [fixed](resolved/shoalctl-panic.md).

### 25. CLAUDE.md drift

| Claim | Reality |
| --- | --- |
| `Conf::new("shoal.yml")` | The method is `Conf::from_file` (`conf.rs:269`) |
| "LRU eviction at 60%" | Eviction triggers when usage exceeds the configured limit exactly; 40% is then freed (`shard.rs:661`, `:557`) |
| `exluded_cores` | Should be `exclude_cores` |
| Lists `EphemeralTable` as a usable table type | It does not satisfy the interface `#[db]` generates calls against |

### 28. SHQL "Unknown field" names the field it is listing as valid

A field declared on a table but marked neither `partition`, `sort`, nor `filter` — an
`#[shoal(update)]`-only field, for instance — has no `FieldRole`, so using it in a `WHERE`
clause fails. The message it fails with is confusing:

```
Unknown field 'data'. Valid fields are: ["id", "title", "data"]
```

`data` appears on both sides. The cause is that `field_names()` is generated from *all* fields
while the `get_field_role` match arms are generated only for fields that have a role
(`shoal-derive/src/traits/table_schema.rs`), so the two lists disagree by construction. The
error is raised at `structs/client.rs:127-137` (unsorted) and `:198-208` (sorted).

**Fix direction:** two different errors are being conflated. Distinguish them — if
`field_names()` contains the name, say `Field 'data' cannot be used in a WHERE clause because it
is not a partition key, sort key, or filter`; only say `Unknown field` when it is genuinely
absent. Better still, generate a `queryable_field_names()` alongside `field_names()` so the
"valid fields" list is the set that can actually appear in a `WHERE` clause.

### 29. SHQL rejects exponent notation with a misleading error

`float_number` is `(opt(sign), digit1, ".", digit1)`
(`shoal-core/src/shared/queries/parser.rs:261`), so digits are required on both sides of the
point and there is no exponent form. `.5` and `5.` fail with a reasonable
`Expected a value for field 'x'`, but `1e9` does something worse:

```
SELECT * FROM Movie WHERE id = 1e9
  =>  Unexpected trailing input: 'e9'
```

The integer parser matches the leading `1`, the condition completes, and the leftover `e9` is
reported by the end-of-input check as though the problem were somewhere else entirely. Confirmed
by running it.

**Fix direction:** low priority, since spelling the number out always works. If it is fixed, add
an optional exponent to `float_number` rather than special-casing the error — the error is only
misleading because the grammar accepts a prefix of what the user meant. Note that
`serde_json::Number::from_f64` already rejects the infinities a large exponent can produce, so
the overflow path is covered.

### 40. `UnsortedExists` still names a single partition

`UnsortedGet` now carries `partition_keys: Vec<u64>` and splits across shards like its sorted
counterpart ([26, 39](resolved/partition-order.md)), but `UnsortedExists`
(`shared/queries/unsorted.rs`) was left with a scalar `partition_key`. `SortedExists` has taken a
`Vec` all along, so the two table kinds now disagree about what an exists can ask.

Nothing is wrong today: SHQL emits no exists queries, so the only way to reach one is the typed
API, where the single-partition shape is what the generated `*Exists` offers anyway. It is a
consistency gap that will bite whoever adds `EXISTS` to the query language, since the sorted
spelling will accept an `IN` list and the unsorted one will not.

**Fix direction:** the same change `UnsortedGet` took — a `Vec<u64>`, `for_partitions`, and
`group_by_shard` in `split_by_shard` — plus a `PendingGet`-shaped wait in the table so an exists
spanning partitions can park on more than one read.

### 41. SHQL cannot express a composite partition key

A table with several `#[shoal(partition)]` fields gets a tuple `PartitionKey`
(`shoal-derive/src/traits/partition_key.rs`), and the generated parse arm deserializes one
literal straight into that type. No SHQL literal is a tuple, so every query against such a table
fails with `Failed to deserialize partition key` however it is written.

The parser now guarantees one clause per field, which is what makes the fix tractable: the arm
could collect the conditions naming each partition field and build the tuple from them in
declaration order, so `WHERE a = 1 AND b = 2` would name one partition. Note that this is the one
place where `AND` across two fields is a conjunction *within* a key rather than a filter on top
of one, and that `IN` over a composite key would need each field's values crossed with the
others.

### 42. SHQL cannot express a composite sort key

The same defect as [41](#41-shql-cannot-express-a-composite-partition-key), one key over. Several
`#[shoal(sort)]` fields make a tuple `Sort` (`shoal-derive/src/structs/get.rs`), and the sorted
parse arm deserializes one literal straight into it, so `WHERE partition = 'x' AND a = 1 AND b = 2`
fails with `Failed to deserialize sort key` rather than naming a row.

Unlike the partition-key case the query still runs when the sort condition is simply left out — it
just returns the whole partition — so this is a missing capability rather than a table that cannot
be reached at all. It only started mattering with [item 8](resolved/sort-keys.md): while sort keys
were ignored there was nothing to express.

**Fix direction:** the same shape as 41, and worth doing in the same change. Collect the conditions
naming each sort field, build the tuple in declaration order, and reject a partial one. A prefix of
a composite sort key is a *range*, not a point, and although ranges exist now
([F1](../features/sort-key-ranges.md)) a range over a *prefix* still does not — lowering one needs
synthesized minimum and maximum values for the fields the prefix leaves out, which `Sort` does not
name ([TODOs](todos.md#sort-key-range-predicates--built)).

### 30. A sorted partition load can be silently thrown away

`.../persistent/sorted.rs:249-291` — `load_partition` merges a freshly read archive extent into
whatever is resident, and its `Occupied` arm only matches `MaybeLoaded::Loaded`:

```rust
hash_map::Entry::Occupied(mut entry) => {
    if let MaybeLoaded::Loaded { partition, .. } = entry.get_mut() {
        /* merge */
    }
}
```

If the resident entry is `Accessible` — another copy of the same extent — the `if let` does not
match, the data that was just read from disk is dropped on the floor, and memory usage is not
adjusted. Any query blocked on that load is still released, so it answers from the copy that
was already there.

Harmless today, because the two copies hold the same bytes. It is listed because it is a silent
no-op at the end of an IO path: if the two ever diverge, nothing here would say so.

**Fix direction:** handle the arm explicitly, even if the body is `// the resident copy is the
same extent, so keep it and drop what we read`. An `else` that says why is worth more than a
pattern that quietly does not match.

### 35. A `RefCell` borrow is held across three awaits in the compactor

```rust
if let Some(entry) = self.map.to_archive.borrow().get(partition) {
    let handle = self.map.get_archive(&entry.archive).await?;
    let read = handle.read_at(entry.offset, entry.size).await?;
    let archived = <T as RkyvSupport>::access(&read)?;
    handle.close().await?;
```

`.../fs/compactor.rs:180-193`

`shoal-core` is edition 2021, where the temporary `Ref` produced by `.borrow()` lives to the end
of the `if let` statement. So the shared borrow on `to_archive` is held across all three awaits,
during which other tasks on the same executor run.

It does not panic today, because the only `borrow_mut` callers — `set_partition` and
`remove_partition` (`.../fs/map.rs:436-452`) — are the compactor itself, which is not running
while it is parked here. It is listed because that is a property of who happens to call what, not
of anything enforced, and because the fix is already sitting next to it:
`ArchiveMap::find_partition` (`.../fs/map.rs:478-484`) exists to copy the entry out, `ArchiveEntry`
is `Copy`, and the loader path already uses it (`.../fs.rs:505`, `:533`).

Worth reading against the edition too: this becomes correct for free under edition 2024's
temporary scoping, which means an edition bump would silently change the failure mode rather than
the code.

### 37. `Ring::add` is not idempotent

`shoal-core/src/server/ring.rs:26-44` appends to `self.shards` and lays down 1000 vnodes every
time it is called, with no check for a shard name it already knows:

```rust
self.shards.push(shard);
let shard_id = self.shards.len();
```

A `Join` broadcast that arrives twice for the same shard counts that shard twice in `shards` and
rewrites its 1000 vnodes to point at the new index, leaving the old index in `shards` with nothing
on the ring pointing at it. The ring's shape therefore depends on `Join` being delivered exactly
once, which nothing guarantees — `join_cluster` (`shard.rs:397-403`) is only called from `init`
today, so the invariant holds by call site alone.

Compounds with [item 12](#12-vnodes-provide-no-load-smoothing): both are reasons the ring's
distribution is not what the vnode count suggests.

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

1. **Item 31** — the only item here that loses committed data. It needs a crash to reach, which is
   the case recovery exists for, and the fix is small.
2. **Item 38** — not a production defect, but the test suite is what every other fix on this page
   is judged by, and right now two of its binaries can silently serve each other's traffic. Worth
   doing before the fixes below rather than after them.
3. **Items 11 and 16** — hot-path panics, and the empty-ring window a client can hit during
   startup.
4. **Item 14** — empty rotated logs accumulating on disk and being replayed every startup.
5. **Items 27 and 42** — data that SHQL cannot reach at all: a partition key containing a quote,
   and a composite sort key. Item 42 is the sharper of the two now that
   [item 8](resolved/sort-keys.md) is fixed, since a sort key is a thing you can query with.
6. **Item 9's remaining half and item 13** — two sides of the same absence: nothing counts what
   recovery discards, and nothing can observe memory accounting except a log line that panics
   when it is wrong. Item 13 is the sharper of the two now that
   [item 6](resolved/memory-accounting.md) is fixed: the accounting it reports on is closer to
   right, so the log line that panics on it is the remaining hazard.
7. **Items 32 and 33** — two leaks with one shape: state keyed by something that goes away and is
   never told. They are cheap together, since a `ClientGone` broadcast is what both want.

Everything that has been fixed, and why it was fixed the way it was, is in
[Resolved Issues](resolved-issues.md). The SHQL parser has gained test coverage at both stages
([SHQL](../api/shql.md#testing)) — items 26–29 were found while writing it.
