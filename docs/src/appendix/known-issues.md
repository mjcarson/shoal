# Known Issues

A severity-ranked index of open defects on the `ZeroCopyResponses` branch. Each entry names
the symptom, the cause, and a `file:line`.

**How these were established.** Everything currently here comes from reading the source. Entries
that were later confirmed by reproduction say so on their resolved page, and item 14 was the last
one on this page to carry that note before it moved
([Resolved #14](resolved/empty-rotated-logs.md#evidence)). Line numbers drift.

Reading is enough to find a defect and not always enough to characterise it. Item 13 was filed
from a reading that called its panic reachable; trying to reproduce it showed the panic was
latent and that the line's live cost was a different one
([Resolved #13](resolved/eviction-log-underflow.md#evidence)). An entry here is a claim about the
source, not yet a claim about a running server.

Performance findings are catalogued separately in [Optimizations](optimizations.md), and what the
test suite does and does not reach is in [Test Coverage](test-coverage.md).

Defects that have been fixed move to [Resolved Issues](resolved-issues.md), one page each,
carrying the reasoning and the invariants the fix depends on. Item numbers are shared between
the two pages and never reused, so a number appears on exactly one of them — which is why this
list starts at 15 and skips 26, 31, 39, 44, 45, and 48, and why item 57 is the newest. The exceptions are items 16, 20, 24 and 51, which were only
partly fixed: the open remainder is here and the rest is there. Item 9 was a fifth exception
until its second half was fixed, and is now on the resolved page alone.

**Baseline as of writing:** `cargo check --workspace --all-targets` passes with warnings;
`cargo test --workspace` passes — 454 integration tests (one ignored), 229 `shoal-core` unit
tests, 21 doctests, plus 8 more behind `--features stage-profile` that a default run does not
reach ([Test Coverage](test-coverage.md)). That is up from 452, 225 and 21 with
[Resolved #16, 51](resolved/partition-load-failure.md) — two integration tests over a partition
read that fails and four unit tests over how a read failure is classified. Before that it was up
from 410, 219 and 21 with
[F9](../features/ephemeral-tables.md), and before that from 359, 219 and 16 with
[F8](../features/purpose-built-workloads.md), whose count moved in **both** directions — it
deleted a comparison engine along with its tests and moved others between crates, which that page
accounts for line by line. Before that it was up from 172, 215 and 11 with the stamp and
offset tests added by [F6](../features/stage-breakdown.md). Before that it was up from 168, 194 and 11 with the config and cpu selection tests
added by [items 18 and 50](resolved/excluded-cores-typo.md) and the baseline versioning and
throughput tests added by [F3](../features/performance-harness.md). Before those it was up
from 14 and 32 with the addition of SHQL coverage
([SHQL](../api/shql.md#testing)), the restart and eviction tests added with items 4 and 5, the
limit and cross-shard coverage added with item 7, the row-order and `IN`/`OR` coverage added
with items 26 and 39, the sort-key selection coverage added with item 8, the range coverage
added with [F1](../features/sort-key-ranges.md), the multi-log recovery and recovery
counting coverage added with items 31 and 9, the tablet map and storage marker coverage
added with items 11 and 12, the eviction accounting coverage added with item 13, the empty
rotated log coverage added with item 14, the compaction tail loss and marker format coverage
added with items 44 and 45, and the query error display coverage added with
[item 48](resolved/query-error-display.md). The counts before item 48 were 157, 194 and 11;
before F2 were 136, 183 and 11; before items 44 and 45
were 136, 178 and 11; before
item 14 were 135, 178 and 11;
before item 13 were 135, 177 and 11; before items 11 and 12 were 133, 168 and 10; before
items 9 and 31 were 132, 159 and 10; before F1 they were 115, 129, and 8, and before item 8
were 105 and 116.

---

## High — data loss and silent failure

*Nothing is currently filed at this severity.* Items 9 and 31 were the last two and are both
[resolved](resolved-issues.md).

One thread they shared is still worth pulling. Both were about what happens when a copy read
from disk meets a copy already in memory: item 31's `scan` overwrote the in-memory copy, and
[item 30](#30-a-sorted-partition-load-can-be-silently-thrown-away) is the `Occupied` arm of
`load_partition` (`sorted.rs:256-295`) throwing the disk copy away instead. Item 31 answered the
question for recovery by removing the collision entirely — nothing loads after a replay — which
leaves item 30 as the remaining place the question is answered badly.

---

## Medium — robustness

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
| `.../fs/stream.rs:108`, `:115` | WAL write or notification failure |
| `shared/traits.rs:54` | rkyv serialization failure |
| `.../persistent/sorted.rs:245`, `:355`, `:453`, `:599`, `:734`, `:890` | Corrupt archive data |

**The loader's three are gone.** A `todo!()` on the partition read path and the two `panic!`s
that fired on any loader task error have been replaced by a failure the shard is told about:
[Resolved #16, 51](resolved/partition-load-failure.md). The rest of this item is open.

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
forever, and there is no timeout anywhere to break the wait ([TODOs](todos.md#timeouts)).

~~That is not hypothetical: a partition load that fails to spawn hits the `todo!()` in the
loader and panics it, stranding every query blocked on it.~~ That route is closed — a partition
read that fails now releases the queries parked on it
([Resolved #16, 51](resolved/partition-load-failure.md)). What remains is the general defect:
nothing bounds how long a `Gather` waits for a share, so any *other* way a shard can fail to
send one leaks it just as permanently.

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
(`conf.rs:111`) from each binary in turn: `persistent_sorted_table` binds 13000-13085,
`persistent_unsorted_table` binds 13000-13024, and **every one of the unsorted binary's ports is
also bound by the sorted one**. Both ranges grow with every test that restarts a server — they
were 13034 and 13021 when this was filed — so re-measure rather than trusting the numbers.

The bind does not fail, which is what makes this worth an entry. Glommio sets `SO_REUSEPORT` on
listening sockets (`glommio/src/net/tcp_socket.rs:135`), so the second bind succeeds silently and
the kernel load balances incoming connections between the two servers. A client in one test can
therefore have its connection handed to a server owned by another test — a different schema, a
different temp dir — with no error anywhere to say so.

`persistent_sorted_table.rs:812` additionally hardcodes `let port = 13900`, so two concurrent runs
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
- Recovery adds a partition to the counter in *archive bytes* and eviction takes it off in
  *deep size*. `FileSystem::load_scanned` (`.../storage/fs.rs`) does `*memory_usage.borrow_mut()
  += partition_read.len()`, which matches `MaybeLoaded::size()` while the entry is
  `Accessible`. Replay then converts it to `MaybeLoaded::Loaded` (`.../persistent/sorted.rs`,
  `.../persistent/unsorted.rs`) adding only the update's `diff`, so `size()` starts answering
  `partition.size()` — a `deep_size_of` — against an amount that was the archive extent's length.
  `evict` subtracts the new base. The residual per partition is `read.len() - partition.size()`,
  either sign. This is the most concrete candidate for the `drift` that
  [item 13](resolved/eviction-log-underflow.md) now reports, because it is the one place two
  different size *bases* meet on the same counter rather than two different arithmetic paths.
  Note [item 31](resolved/multi-log-recovery.md) shrank this considerably without meaning to —
  the old `scan` re-added `read.len()` once per update intent, so a partition with *N* updates
  was counted *N* times.

The fix for the first bullet is the one filed as
[O4](optimizations.md#o4-deep_size_of-is-a-recursive-walk-called-on-every-mutation), ranked **B2**
there. The mismatched bases exist *because* the size is re-derived at each call site instead of
being owned by one, so carrying a row's measured size alongside it settles this item and removes a
recursive walk from every mutation with the same edit. That is why O4 outranks the other write-path
entries despite the profile saying the write path is waiting on the device — it is bought for a
correctness fix, and the cost removal is change left over.

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
| ~~`exluded_cores`~~ | ~~Should be `exclude_cores`~~ — [fixed](resolved/excluded-cores-typo.md), in both files |
| ~~Lists `EphemeralTable` as a usable table type~~ | ~~It does not satisfy the interface `#[db]` generates calls against~~ — [fixed](../features/ephemeral-tables.md) by making it usable rather than by changing the claim; the type is now `EphemeralSortedTable` / `EphemeralUnsortedTable` and both files say so |

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

### 49. The coarsest parse errors report a span covering the whole query

Three of the errors a user hits most often blame the entire query rather than the part of it that
is wrong:

| Query | Reported span |
| --- | --- |
| `SELECT * FROM Nope WHERE id = 1` | `0..31` — all of it, for a table name in bytes 14 to 18 |
| `SELECT * FROM Movie WHERE title = 'a'` | `0..37` — all of it, for a missing partition key |
| `PICK * FROM Movie WHERE id = 1` | `0..30` — all of it, for a keyword in bytes 0 to 4 |

The first comes from the unknown-table arm the derive generates
(`shoal-derive/src/structs/client.rs`), which passes `0, query.len()`; the second from the
missing-partition-key arm beside it, which does the same; the third from `ParsedSelect::new`
(`shoal-core/src/shared/queries/parser.rs`), which reports `at_position(..., 0, query)` because
the winnow parser it wraps returns a `ContextError`, and a `ContextError` carries no input slice
to recover an offset from.

Everywhere else the parser tracks real offsets — `field_start`/`field_end` on a `WhereClause`,
`start`/`end` on a `WhereValue` and a `ParsedProjection` — and computes them from
`original.len() - input.len()` on the surrounding `&str`. These three sites can do the same; the
`ContextError` is not the obstacle it looks like, because the offset comes from the input the
parser was handed rather than from the error it returned.

The cost is paid in shoalctl, which underlines the span an error names
([item 48](resolved/query-error-display.md)). A span covering the whole query is refused as
misleading, so exactly the errors a beginner hits first are the ones drawn without a mark. The
message names the position instead, which is correct and less useful.

**Fix direction:** for the unknown table, `ParsedSelect` already has the table name and could
carry its offsets alongside it the way `ParsedProjection` does. For the missing partition key,
the honest span is the whole `WHERE` clause rather than the whole query, since that is the clause
that has to change. For the failed `SELECT`, `query.len() - parsable.len()` at the point winnow
gave up is the offset, and it is already in scope.

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

### 51. A partition load that fails *inside* `load_partition` still never releases its queries

**The larger half of this is [fixed](resolved/partition-load-failure.md).** A read that fails
before it reaches the table — the archive could not be opened, or the partition was pruned out
from under it — now reports itself, and the queries parked on it are released and replayed. What
follows is the remainder.

`.../persistent/sorted.rs` and `.../persistent/unsorted.rs` — `load_partition` builds the loaded
partition and then, at the end, drains `self.blocked` for the queries that were parked on it. Every
early exit between those two points still leaves those queries parked forever: the client waits on a
response that no longer has anything to produce it, and the entry in `blocked` is never collected.

There is one such exit today, `ValidatedArchive::new` on a corrupt archive. It returns `Err`, which
propagates up through the shard message loop and ends the shard, so the *symptom* of that particular
one is hidden behind a bigger failure. The defect is that the function has early exits at all and the
next one added to it may not be fatal.

Not introduced by [F4](../features/validated-archives.md), and not fixed by it — but F4 is what made
it worth filing, because it added the first failure that returns rather than panics.

The same shape is on the recovery path in `FileSystem::load_scanned`, which is less interesting
because nothing is blocked yet during startup.

**Fix direction:** `fail_partition` is already there and already does the releasing, so this is now
just a matter of routing `load_partition`'s own errors into it rather than out of the function.
Answering those queries with something better than "found nothing" needs a query error response that
can carry a storage failure ([item 56](#56-a-response-cannot-say-that-a-read-failed)).

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

### 43. The storage marker only guards the default storage root

`shoal-core/src/server.rs` claims `storage.default.filesystem.latency_sensitive.path`, and that
one path alone, with the shard count that wrote it
([items 11, 12](resolved/tablet-ring.md)).

A table with its own `storage.tables` entry pointing somewhere else is not covered. So a
configuration that overrides one table's path keeps the guard for every other table and loses it
for that one: reopening with a changed `cores` refuses to start only if the default root was
also written, and if it was not, the overridden table's data is stranded exactly as silently as
before.

Found while building the marker rather than by reading the storage config, which is why it is
recorded here instead of being fixed there — covering it properly means claiming every distinct
root a config names, and deciding what a marker means when two tables disagree.

**Fix direction:** collect the distinct roots across `storage.default` and every `storage.tables`
entry, and claim each one. The shard count is the same for all of them, so the file's contents do
not change — only how many are written.

### 46. An unmarked storage directory is claimed rather than refused

`StorageMeta::claim` (`server/meta.rs`) treats a missing `shoal-meta.json` as a directory nothing
has written to, creates one, and starts:

```rust
// this directory has never been written to, so claim it for this shard count
Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
    std::fs::create_dir_all(root)?;
    std::fs::write(&path, serde_json::to_vec_pretty(&StorageMeta::new(shards))?)?;
```

"Has no marker" and "has never been written to" were the same statement for exactly as long as
the marker has existed, which is one commit. Every directory written before
[items 11 and 12](resolved/tablet-ring.md) has no marker and plenty of data, and that change also
replaced the vnode ring with a tablet map — so ownership moved from a hash of the shard's *name*
to `top-12-bits-of-key % shard_count`, and effectively every partition now belongs to a different
shard than the one whose archive map holds it. A shard's data is stored under its own name, so
each shard reads its own archives, finds none of the partitions it is now asked for, and the
server comes up empty.

This is the exact failure `StorageMeta` was built to prevent. It is missed because the marker is
newer than the data it guards, and a guard that only fires when it recognises the directory
cannot fire on the one case where it does not.

The severity is bounded by who has such a directory: this is a pre-1.0 branch and the only known
instances are disposable dev data, which is why this is filed rather than fixed. It is recorded
because the reasoning generalises — the next on-disk marker will have the same blind spot on the
day it ships.

**Fix direction:** claiming is only safe for a directory that is genuinely empty. Before writing a
marker, check the root for archives and `*-active` intent logs; if any exist, refuse with a
distinct error saying the directory predates the marker and no migration exists. An empty
directory is still claimed, which keeps first start working. Note this cannot be a `format`
check — [item 45](resolved/storage-marker-format.md) covers a marker that is *wrong*, and this is
one that is *absent*.

### 47. A torn tail on the active log is counted as data loss

`FileSystem::read_intents` (`.../storage/fs.rs`) counts a `truncated` reader against
`RecoveryStats::truncated_logs`, and it does so for the active log on the same terms as for an
inactive one:

```rust
// a reader that stopped on a bad tail dropped everything after it
if reader.truncated {
    stats.truncated_logs += 1;
}
```

`RecoveryStats::is_clean` treats any non-zero `truncated_logs` as loss
([Recovery](../storage/recovery.md#what-recovery-discards)), so `Shard::report_recovery` emits
`WARN Recovery discarded data` — after an ordinary crash, where nothing was lost.

A torn tail on the active log is what a crash *looks like*. Writes are acknowledged only after
they are durable — that is [items 1-3](resolved/durability.md), and `ack_survives_sigkill`
(`shoal/tests/persistent_sorted_table.rs`) is the end-to-end proof — so the half-written entry at
the end of the log belongs to a write no client was ever told about. Dropping it is the design
working.

This matters by the recovery page's own argument. `updates_after_delete` is kept out of
`is_clean` because "an update that lands on a row a delete already tombstoned is correctly
dropped, and counting it as loss would make the numbers that do mean loss useless"
([storage.rs](../storage/recovery.md#what-recovery-discards)). Counting benign torn tails is the
same mistake in the opposite direction: every unclean shutdown produces a `WARN` that claims
data was discarded, so the warning that means real corruption is buried in warnings that mean a
process was killed. [Item 44](resolved/compaction-tail-loss.md) draws this same distinction
correctly on the compaction side.

**Fix direction:** the reader already knows where it stopped. A tail whose remainder is padding
or zeros is the benign shape; damage with non-zero bytes after it is a corrupt record with data
behind it. `IntentLogReader` can scan the remainder once on the way out and set two different
flags. Failing that, the cheaper split is positional — count damage in the *active* log
separately from damage in an inactive one, since an inactive log has been fully written and
rotated, so damage in it is never benign. Either way `is_clean` should ignore the benign counter,
the way it already ignores `updates_after_delete`.

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

1. **Item 38** — not a production defect, but the test suite is what every other fix on this page
   is judged by, and right now two of its binaries can silently serve each other's traffic. Worth
   doing before the fixes below rather than after them.
2. **Item 16** — the hot-path panics. This entry used to read "items 11 and 16"; item 11, the
   empty-ring window a client could hit during startup, is [resolved](resolved/tablet-ring.md) and
   was made unbuildable rather than checked.
3. **Item 22** — the size accounting the whole memory limit rests on, now with *four* different
   bases for the same field. It moved up this list because the two things that used to sit in
   front of it are done: [item 6](resolved/memory-accounting.md) fixed the path that destroyed the
   counter outright, and [item 13](resolved/eviction-log-underflow.md) turned the eviction log into
   something that reports drift instead of breaking on it. That `drift` field is the instrument for
   this one, and it now has a first hypothesis to test rather than a whole page to reason about:
   recovery adds a partition in archive bytes and eviction takes it off in deep size, which
   predicts drift proportional to how many partitions a shard's recovery loaded and none on a
   shard that started clean.
4. **Item 47** — cheap, and it is making the recovery counters harder to trust the longer it sits.
   Every unclean shutdown currently reports discarded data, so the warning that means real
   corruption is buried under warnings that mean a process was killed.
5. **Items 27 and 42** — data that SHQL cannot reach at all: a partition key containing a quote,
   and a composite sort key. Item 42 is the sharper of the two now that
   [item 8](resolved/sort-keys.md) is fixed, since a sort key is a thing you can query with.
6. **Items 32 and 33** — two leaks with one shape: state keyed by something that goes away and is
   never told. They are cheap together, since a `ClientGone` broadcast is what both want.
7. **Items 43 and 46** — the two remaining holes in the storage marker. Worth doing together,
   since both are changes to what `StorageMeta::claim` looks at before it writes.

### 52. A resident hit in `exists` answers a query a blocked clone will answer again

`PersistentSortedTable::exists` (`.../persistent/sorted.rs`) walks the partition keys an exists
named. A key whose partition has to be read from disk pushes a clone of the whole query into
`self.blocked` and moves on:

```rust
if will_load {
    // add this query to the list of ones waiting on this partition
    let entry = self.blocked.entry(*partition_key).or_default();
    let blocked_exists = exists_query.to_blocked(*partition_key);
    entry.push((meta.clone(), SortedQuery::Exists(blocked_exists)));
    blocked.push(*partition_key);
    continue;
}
```

A *later* key in the same loop that is resident and does hold a matching row returns straight
away:

```rust
if partition.exists(exists_query, &mut seek) {
    return Some((meta.client, meta.id, meta.stamps, response));
}
```

That early return never reaches the `self.pending_exists.insert` at the bottom of the loop, and
it does not remove the clone from `self.blocked`. When the partition finishes loading the clone
is replayed and answers the same `(id, index)` a second time. The client is owed exactly one
response per query, and an unordered stream will surface both.

Only reachable when one exists names partitions on the same shard where at least one is
evicted and a *later* one is resident and matching — the order matters, since a resident hit
before the blocked key would have returned before the clone was ever parked.
`PersistentUnsortedTable::exists` has no such path: it names one partition and has one exit.

**Established by reading the source**, while building [F6](../features/stage-breakdown.md).
The stage report counts these as `join.duplicates` rather than folding them into a bucket, so
a run that hits this says so — but nothing yet reproduces it.


### 53. `hotpath`'s `percent_total` is meaningless for a concurrent scope

`docs/perf/runs/*.hotpath.json`, every capture

`hotpath` reports a `percent_total` per scope, and it is not a percentage of anything a reader
would take it for. It is not normalised across scopes that ran at the same time on different
shards, so twelve shards each spending most of a run inside a scope sum to far more than the run
did. The committed `B1-performance.hotpath.json` reports:

```
shoal_core::server::tables::storage::fs::stream::write_helper   percent_total: 1253041
shoal_core::server::shard::handle_query                         percent_total: 6245
```

Those are 12,530% and 62%, of a run that was 100% of itself.

The field is harmless as long as nothing reads it, and the trap is that it looks exactly like the
number anybody would reach for first. `total` — nanoseconds summed across every shard that entered
the scope — is the field to rank by, and it needs saying that it is a sum across shards rather
than a share of the wall clock, which is why the chart's axis on
[Benchmark Results](../operations/benchmark-results.md) says so.

**Established by reading the committed artifacts**, while building
[F7](../features/bench-runner.md). `shoal-bench` never plots or tabulates the field, and
`render::chart::hotpath_scopes::tests::the_unnormalised_percentage_is_never_drawn` pins that.
Fixing it properly is upstream in `hotpath`, or means dividing by the shard count that actually
touched each scope — which the profile does not record.

### 54. `#[shoal::db]` needs three crates the caller has never heard of

`shoal-derive/src/lib.rs`, every generated `#[shoal::db]` and `#[derive(Shoal*Table)]`

The generated code names `glommio`, `uuid` and `deepsize2` by path. A crate that writes a schema
therefore has to declare all three as its own dependencies, even though it mentions none of them
and has no reason to know they exist. The failure is at least loud — `cannot find module or crate
glommio in this scope`, pointing at the `#[shoal::db]` attribute — but it points at the macro
rather than at the manifest, and nothing in [Derive Macros](../api/derive-macros.md) says a word
about it.

Nothing had noticed because nothing had ever tried. Every schema in this repository lived inside
`shoal` — the `tmdb` example, the integration tests — and `shoal` already depends on all three, so
the requirement was invisible from the only place it was ever exercised. Writing the
[F8](../features/purpose-built-workloads.md) workload schema in `shoal-bench` was the first time a
schema was defined outside that crate, and it failed on all three in turn.

**Established by reproducing it**, while building F8. The fix is for the macros to emit
`::shoal::...` paths through re-exports the facade already controls, which is a `shoal-derive`
change and would make the requirement disappear rather than need documenting. Until then
`shoal-bench/Cargo.toml` carries the three with a comment pointing here.

### 55. A get that found nothing is reported as a query that failed

`shoal-core/src/client.rs`, `Shoal::send_one`

`send_one` calls `suceeded` on the response and turns a get that matched no rows into
`Err(QueryDidNotSucceed)`. "The row is not there" and "the query did not work" are different
answers, and a caller that wants the first has no way to ask for it through `send_one` — it has to
drop to `send` and drain the stream itself, or use `exists`, which only answers a yes-or-no.

This is a usability defect rather than a correctness one, and it bites in a specific way: any code
that probes with a get treats an empty table as a broken server. The F8 readiness probe did exactly
that and timed out for thirty seconds against a server that was answering every query correctly,
until it was changed to use `exists`.

**Established by reproducing it**, while building F8. `QuerySuceededOpts` already exists as the
knob that decides what counts as success, so the fix is plausibly to let `send_one` take one rather
than always using the default.

### 56. A response cannot say that a read failed

`ResponseAction` (`shoal-core/src/shared/responses.rs:28-39`) has five variants and none of them
carries an error. A get answers `Get(None)`, and that one answer has to stand for both "this
partition holds no such row" and "the copy on disk could not be read".

That gap is what decides the shape of every storage failure that reaches a query. A read that
gives up now releases the queries parked on it and they answer from what is resident
([Resolved #16, 51](resolved/partition-load-failure.md)) — which is the right thing to do with
them and still reports a short answer as a complete one. The alternative, ending the shard, is
worse: it turns one unreadable archive into an outage.

The cost is bounded by how visible the failure is elsewhere: the loader logs every give-up at
`ERROR` naming the table, the partition and the errno. So the server knows. The client does not.

**Fix direction:** a `ResponseAction::Error` variant, which is a wire format change and reaches
the gather/merge path (`responses.rs:51-70`), the client, and every site that builds a response.
This is also what [item 51](#51-a-partition-load-that-fails-inside-load_partition-still-never-releases-its-queries)
needs to answer its parked queries with something truthful, and what
[item 55](#55-a-get-that-found-nothing-is-reported-as-a-query-that-failed) needs to stop
conflating an empty result with a failure — the three want the same variant.

### 57. A missing archive is created empty rather than reported

```rust
let file = OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .dma_open(&path)
```

`.../fs/map.rs:461-470`, `ArchiveMap::get_archive`

A read whose archive is not on disk does not fail. `create(true)` makes an empty one, `read_at`
against it comes back short, and the failure surfaces later as a validation error on bytes that
were never written — which ends the shard (`.../persistent/sorted.rs:349-352`) rather than
naming the missing file. A stray zero-byte archive is left behind each time.

This is reachable: the compactor deletes archives it has rewritten (`.../fs/compactor.rs:604`)
after re-pointing their entries, so a read holding an entry from before that re-point looks for
a file that is gone.

**Fix direction:** `get_archive` has two callers with opposite needs — the writer wants the file
created, a read wants to know it is missing. Split them, and let the read path return an error
naming the archive. That error then classifies as `Fatal` and takes the same release path the
loader already has, so the queries waiting on it are answered instead of the shard dying.

Everything that has been fixed, and why it was fixed the way it was, is in
[Resolved Issues](resolved-issues.md). The SHQL parser has gained test coverage at both stages
([SHQL](../api/shql.md#testing)) — items 26–29 were found while writing it.
