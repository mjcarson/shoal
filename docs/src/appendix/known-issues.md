# Known Issues

A severity-ranked index of open defects on the `ZeroCopyResponses` branch. Each entry names
the symptom, the cause, and a `file:line`.

**How these were established.** Everything currently here comes from reading the source. Entries
that were later confirmed by reproduction say so on their resolved page, and item 14 was the last
one on this page to carry that note before it moved
([Resolved #14](resolved/empty-rotated-logs.md#evidence)).

**Line numbers drift, and they had.** Every citation on this page was re-resolved against the tree
in August 2026 ([Review](review-2026-08.md)) and most of them had moved — item 16's whole table by
a hundred lines or more. They are correct as of that sweep and will rot again; the symbol name
beside each one is what a reader should grep for, and the line is a shortcut. That review also
found that two entries had been quietly overtaken by the code — item 17 is now mostly fixed, and
item 25 is fully [resolved](resolved/claude-md-drift.md).

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
list starts at 15 and skips 25, 26, 31, 34, 39, 44, 45, 48, 51, 56, 57, 61, 67 and 68, and why
item 70 is the newest. The exceptions are items 16, 17, 20, 24 and 54, which were only
partly fixed: the open remainder is here and the rest is there. Items 9 and 51 were each one such
exception until their second half was fixed, and are now on the resolved page alone; item 25 was one
in the other direction — it had one row left open, that row was fixed, and the whole item
[moved](resolved/claude-md-drift.md).

**Baseline as of writing:** `cargo check --workspace --all-targets` passes with warnings;
`cargo test --workspace` passes — 986 tests, one ignored, plus 8 more behind
`--features stage-profile` that a default run does not reach ([Test Coverage](test-coverage.md)).
That is up from 967 with [F19](../features/chart-legends.md), which added 16 `shoal-bench` unit
tests over the shared chart legend, the data-derived axis ticks and the encryption charts in
nanoseconds, 2 integration tests over the legend's geometry, and 1 doctest. That is up from 896
with [F17](../features/workload-grid.md) and [F18](../features/results-pages.md), which added 71.
That is up from 876 with [F16](../features/client-builder.md), which added 12 `shoal-client` unit
tests over the builder and the endpoint order, a new `pool.rs` integration binary carrying 6, and 2
doctests. That is up from 868 with [F15](../features/client-server-split.md), which also
re-attributed 177
of them: `shoal-core` went from 323 unit tests to 146 as the protocol and the client became crates
of their own, and none were lost. That is up from 490, 301 and 25 with
[F14](../features/encryption-in-transit.md) — one new integration binary (`tls.rs`) carrying 8
tests, 17 unit tests over the TLS configuration and the kernel key material, 5 over the config
section, and 4 new doctests. The 8 in `tls.rs` and one of the 17 need the `tls` kernel module and
skip loudly without it, the same way the `stage-profile` tests sit outside a default run. No new
defect was filed while building it; item 64 was filed just before it, while correcting
[D4](../direction/encryption.md). Before that it was 482, 301 and 24 with
[F13](../features/transport-workloads.md) — 8 unit tests in `shoal-bench` over the eight transport
workloads and one doctest over `seed_batch`, with the `shoal-core` count unmoved because no engine
code changed. No new defect was filed while writing it. Item 64 was filed later, while correcting
[D4](../direction/encryption.md), and changes no count — it is a documentation defect and no test
was added or could be. Before that it was 473, 272 and 21 with
[F12](../features/authentication.md) — one new integration binary (`auth.rs`) carrying 9 tests, 17
unit tests over the mechanism and its credential store, 8 over the auth frame codec and the
handshake's new fields, 4 over the config section, and 3 new doctests. Item 63 was filed while
writing it, from reading the source. Before that it was 470, 258 and 21 with
[F11](../features/error-channel.md) — one new integration binary (`errors.rs`) carrying 3 tests,
7 unit tests over the error codec, 3 over the response payload, and 4 over the client's read path
and its dead connection sweep. Four existing integration tests changed what they assert rather than
being added to, which is the signal that the behaviour they pinned is the behaviour that moved.
Item 62 was filed while writing it, from reproducing it, and its reproduction is not committed —
the test that found it asserted behaviour F11 does not provide.
Before that it was 456, 231 and 21 with
[F10](../features/framing-and-protocol-evolution.md) — three new integration binaries
(`framing.rs`, `handshake.rs`, `fingerprint.rs`) carrying 14 tests between them, 23 unit tests over
the frame codec and the fingerprint, 2 over the client's read path, and 2 over the frame bound's
config default.
Before that it was re-run and re-counted binary by binary in August 2026, unchanged — items 59 and
60 are filed from reading and neither added a test.
That is up from 454, 229 and 21 with
[Resolved #57](resolved/missing-archive.md) — one integration test per persistent table over a
read whose archive is not on disk, and two unit tests over `get_archive` and how the failure it
now reports is classified. Before that it was up from 452, 225 and 21 with
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
`load_partition` (`.../persistent/sorted.rs:314-352`) throwing the disk copy away instead. Item 31 answered the
question for recovery by removing the collision entirely — nothing loads after a replay — which
leaves item 30 as the remaining place the question is answered badly.

---

## Medium — robustness

### 15. No backpressure anywhere

Every channel is `kanal::unbounded_async`: the shard mesh (`comms.rs:33`), per-client response
channels (`shard.rs:146`), compaction jobs (`.../storage/fs.rs:317`), loader requests
(`.../persistent/sorted.rs:234`, `.../persistent/unsorted.rs:180`).

Sends never block, so no shard can deadlock on another — but nothing throttles a client
either. A shard that falls behind grows its queue until the process is killed. `blocked` and
`pending_data` are likewise unbounded, as is `PendingResponse`.

The one exception is `StreamWriter`'s `max_write_behind` (`.../fs/stream.rs:553`), which
bounds in-flight writes only.

**Fix direction:** [D6](../direction/connection-pool.md#bounded-channels), ~~sequenced behind
[D2](../direction/framing.md#the-error-channel)~~ — the prerequisite is met.
[F11](../features/error-channel.md) made *shedding* sayable: `ErrorCode::Shedding` is defined, and
a query the server declined can now be reported as declined rather than as an empty result. What
remains is the bound itself and the policy that decides when it is hit.

### 16. Panics on the hot path

233 `.unwrap()` calls and 59 `panic!`s outside `target/` and `old/`. The ones on live request
paths:

| Site | Trigger |
| --- | --- |
| `shard.rs:923` | Reply for a client with no channel |
| `shard.rs:979` | A split query whose `gather` contact is missing — an `.expect` |
| `shard.rs:1163` | Client UUID collision |
| `comms.rs:53`, `:72` | Unknown shard contact |
| `messages.rs:223` | A `Gathered` message asked to be cloned, which only a broadcast does |
| `.../tables/persistent.rs:185` | A parked get resumed with a different projection |
| `.../fs/stream.rs:455`, `:506` | A `DataFlushed` wakeup that could not be sent to the shard |
| `.../fs/stream.rs:662`, `:680`, `:752` | A WAL write that failed |
| `shared/traits.rs:54` | rkyv serialization failure |
| `.../persistent/sorted.rs:511`, `:795`, `:854`, `:952`, `:1007`; `.../persistent/unsorted.rs:481`, `:722`, `:822` | **An intent-log commit that failed** |
| `.../persistent/sorted.rs:439`; `.../persistent/unsorted.rs:386` | A `load_partition` that could not ask for a read |
| `.../persistent/sorted.rs:547`, `:845`, `:999`, `:1297`, `:1343`, `:1383` | Corrupt archive data |

**Three things about this table are worth more than the sites in it.**

*The counting method undercounts.* It counts `.unwrap()` and `panic!` and not `.expect()`, of
which `shoal-core` alone has 78 — one of them, `shard.rs:732`, is on the split-query path and is
now listed above. `unimplemented!()` in `shoal-derive` is a third spelling, though that one fires
at expansion time rather than at runtime.

*The write path is a class of its own.* Eight of the sites above are
`self.storage.commit(&intent).await.unwrap()`. This entry used to file all of the table sites under
*corrupt archive data*, which is wrong for these: a full disk, an `EIO`, or a closed intent log
panics the shard on an ordinary insert. They are the ones with a plausible non-adversarial trigger,
and the ones an error channel ~~would~~ **can** actually answer:
[F11](../features/error-channel.md) built it, and these eight sites are the largest thing that has
somewhere to put an error now and does not use it. They are what is left of this item that a client
would ever see.

*The loader's three are gone.* A `todo!()` on the partition read path and the two `panic!`s
that fired on any loader task error have been replaced by a failure the shard is told about:
[Resolved #16, 51](resolved/partition-load-failure.md). The rest of this item is open.

*Two of the corrupt archive sites are gone.* `sorted.rs:326` and `:328` — the `access` and
`deserialize` on the merge path inside `load_partition` — became a failure that releases the queries
parked on that read and answers them with `ErrorCode::CorruptArchive`
([Resolved #56, 61](resolved/response-error-channel.md)). The line numbers above have also been
corrected: this table had been carrying `shard.rs:676`, `:732` and `:916` since before
[F10](../features/framing-and-protocol-evolution.md) moved them.

*The two relays' five are gone.* `client_rx_relay` panicked on any non-EOF socket error, on a
failed read of a request body, and on a failed forward into the shard; `client_tx_relay` panicked
on both a short write and a write error. All five became a logged `break` with
[F10](../features/framing-and-protocol-evolution.md), so a frame nobody can read ends one
connection instead of the shard and every other client it was serving
([Resolved #34](resolved/unvalidated-length-prefix.md)). These are the sites that were reachable
by anything a peer could put on a socket, which is what made them the worst ones in the table.

### 27. SHQL cannot express a string containing a single quote

`string_literal` is `delimited("'", take_till(0.., |c| c == '\''), "'")`
(`shoal-core/src/shared/queries/parser.rs:470-472`). There is no escape syntax — not doubling
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

Nothing removes an entry from `client_map` (`shard.rs:279`, inserted at `:914`), and `ServerMsg`
has no variant for a client going away. `client_rx_relay` breaks its loop on EOF
(`shard.rs:58-65`) and tells nobody.

Because `client_acceptor` broadcasts `NewClient` to every shard, every shard holds a clone of
that client's `client_tx` for as long as the process runs. So the channel never closes and
`client_tx_relay`'s `recv()` never returns `Err`.

Per connection that has already gone away, permanently:

| Leaked | Where |
| --- | --- |
| One `client_map` entry | Every shard |
| One `kanal` channel | Every shard holds the sender |

**The socket and the tasks are no longer among them.**
[F10](../features/framing-and-protocol-evolution.md) put both relays under one per-connection task
that owns the write task's handle and cancels it when the read relay ends, so a disconnect now
drops both halves of the split stream and closes the socket. This was found by a test that hung:
the two halves of a split stream keep the stream alive between them, so a read relay that ended on
its own left the write relay parked on an empty channel holding a socket nobody would ever read
from again. What is still leaked is the bookkeeping every *other* shard holds, which is what
`ClientGone` below is for.

A response that arrives for a dead client is not an error either — it is sent into an unbounded
channel ([item 15](#15-no-backpressure-anywhere)) that nothing will ever read.

This is the cost of a *disconnect*, not of a failure: an ordinary client that opens a pool, does
its work, and exits leaves all of it behind. The client's own pool is 50 connections
(`shoal-core/src/client.rs:140-148`).

**Fix direction:** a `ServerMsg::ClientGone` broadcast from `client_rx_relay` when its loop ends.
Dropping the sender from every `client_map` is what closes the channel, which is what lets
`client_tx_relay` return on its own.

That handles the socket dying. The *clean* case — a client shutting down deliberately — is better
served by [D2](../direction/framing.md#message-types)'s `GoAway`, which lets the server drain
before the socket closes rather than discovering the disconnect afterwards. The two are
complementary: `ClientGone` is what the server tells itself, `GoAway` is what the peers tell each
other, and [D6](../direction/connection-pool.md#connection-death) needs the second to fail the
right streams on the client side.

### 33. Collected split-query state has no expiry

A `Gather` is inserted when a query is split across shards (`shard.rs:557-571`) and removed only
when `outstanding` reaches zero (`shard.rs:789-797`). Nothing else ever removes one.

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

### 36. A partial intent log buffer is only written when the shard's channel drains

```rust
// if we have no more messages then flush our current queries to disk
if self.shard_local_rx.is_empty() {
    self.tables.flush().await?;
}
```

`shard.rs:970-973`

`StreamWriter::prep` and `consume` (`.../fs/stream.rs:655-682`) write only when the staging buffer
fills, so this `is_empty()` check is the only other path by which staged data reaches disk. Writes
are acknowledged only once durable ([Resolved Issues #1-3](resolved/durability.md)), so whether a
client hears back depends on the shard's channel happening to run dry.

Under sustained load it does not. Worse, the writer's own `DataFlushed` wakeups
(`.../fs/stream.rs:455`, `:506`, sent per completed write) are themselves messages on that channel,
so write traffic helps keep the condition false. The escape is an intent log rotation, which
refreshes the writer and reports its flushed position unconditionally (`.../fs.rs:442-450`) —
meaning a trailing write can wait for up to `intent_log_size`, 10 MiB by default, of *other*
traffic before its client is answered.

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
(`conf.rs:166`) from each binary in turn:

| Binary | Ports bound |
| --- | --- |
| `persistent_sorted_table` | 13000-13102, plus 13900 and 13901 |
| `persistent_unsorted_table` | 13000-13033 |
| `ephemeral_sorted_table` | 13000-13015 |
| `ephemeral_unsorted_table` | 13000-13014 |
| `storage_meta` | 13000-13002 |

**Every port bound by any of the other four is also bound by the sorted one.** The ranges grow with
every test that restarts a server — the sorted binary was 13034 when this was filed, 13085 at the
last measurement and is 13102 now — so re-measure rather than trusting the numbers.

**It has got worse since it was filed, and not by growing.** This entry described *two* colliding
binaries; there are now **five**, because [F9](../features/ephemeral-tables.md) added two more that
start their own servers and `storage_meta` was never counted. Every binary someone adds that starts
a server joins the collision by default, which is the argument for fixing the mechanism rather than
the ranges.

The bind does not fail, which is what makes this worth an entry. Glommio sets `SO_REUSEPORT` on
listening sockets (`glommio/src/net/tcp_socket.rs:135`), so the second bind succeeds silently and
the kernel load balances incoming connections between the two servers. A client in one test can
therefore have its connection handed to a server owned by another test — a different schema, a
different temp dir — with no error anywhere to say so.

`persistent_sorted_table.rs:829` additionally hardcodes `let port = 13900`, so two concurrent runs
of that one binary collide with each other regardless of the counter.

**This has not been observed to fail.** `cargo test --workspace` was run four times while
investigating, and twice more during the review that re-measured the table above; it passed every
time. The binaries are simply never alive on the same port at the same moment. Nothing enforces
that — it is timing, and it is the safety net every other item on this page is checked against.

**Fix direction:** bind port 0 and read back the assigned port, which removes the shared namespace
entirely. Failing that, give each binary a distinct base offset — but that only moves the
collision to the next binary someone adds.

---

## Low — hygiene and documentation drift

### 17. Leftover debug `println!`s

**Mostly fixed.** The six lines in `PersistentSortedTable::exists` — one of which `{:#?}`-printed
an entire partition — are gone. `shoal-core/src/` now has exactly two `println!`s left, and they
are a different thing from what this item was filed about:

| Location | Content | Why it is still here |
| --- | --- |  --- |
| `.../server/conf.rs:166` | "listening on ..." from inside `Networking::to_addr` | It is the only way a test learns which port a server bound, which is what [item 38](#38-integration-test-binaries-all-bind-the-same-ports) is measured with |
| `.../server/trace.rs:61` | "Sending traces for … to gRPC trace sink at …" | It runs *while* the subscriber is being installed, so there is no subscriber yet to emit it through |

Both still bypass the tracing level filter, and the first is still visible in any test run — see
the sample output in
[Observability](../operations/observability.md#debug-output-that-is-not-tracing). Neither is a
debug leftover in the way the six removed ones were, which is why what remains of this item is
"these two want a reason to exist or a `tracing` event" rather than "delete them".

### 19. `memory` has no serde default

`shoal-core/src/server/conf.rs:30-32` — the only field in `Resources` without a default. A
`resources:` block omitting `memory` fails to deserialize. Omitting the whole block yields
`memory: 0`, so eviction runs continuously.

### 20. Orphaned source files

`shoal-core/src/server/cursor.rs` and `shoal-core/src/server/response.rs` are not declared in
`shoal-core/src/server.rs:14-24` and are not compiled. They reference APIs that no longer
exist (`crate::ShoalRow`, `rkyv::AlignedVec`). Dead.

The other half of this item — `.../fs/tests.rs` being 429 lines of commented-out tests — is
[fixed](resolved/storage-tests.md).

### 21. Constant and comment mismatches

| Constant | Comment says | Value is |
| --- | --- | --- |
| `MIN_ARCHIVE_COMPACTABLE` (`.../fs/compactor.rs:31-33`) | 100 MiB | `10 << 20` = 10 MiB |
| Its two use sites (`.../fs/compactor.rs:472`, `:474`) | "under 100MiB" | the same 10 MiB |

(`default_intent_log_size` had the same mismatch and has been corrected to say 10 MiB.)

Also `RemoteTracing::Grpc` exports over HTTP (`trace.rs:33-37`, `.with_http()`), and
`FileSystemThroughputWriterConf::write_behind` — a count of buffers, defaulting to 4 — is
deserialized with `deserialize_byte_size` (`.../fs/conf.rs:154-156`). Its latency-sensitive twin
one struct up is *not* (`:68-69`), so the two writers disagree about what the same field name
means.

### 22. Size accounting inconsistencies

- `UnsortedPartition::new` sets `size = row.deep_size_of() + 17`
  (`.../tables/partitions.rs:228`); `UnsortedPartition::update` sets
  `size = self.deep_size_of()` (`:313`). Different bases for the same field.
- `SortedPartition` tombstones subtract the row's bytes but the tombstone still occupies a
  `BTreeMap` slot, so delete-heavy partitions under-report.
- Sorted partition sizes are maintained by delta and never recomputed, so they drift.
- Recovery adds a partition to the counter in *archive bytes* and eviction takes it off in
  *deep size*. `FileSystem::load_scanned` (`.../storage/fs.rs:242`) does
  `*memory_usage.borrow_mut() += archive.len()`, which matches `MaybeLoaded::size()` while the
  entry is `Accessible`. Replay then converts it to `MaybeLoaded::Loaded`
  (`.../persistent/sorted.rs:336`, `.../persistent/unsorted.rs`) adding only the update's `diff`,
  so `size()` starts answering
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
  (`shoal-core/src/client.rs:1001-1008`).
- `is_valid` / `has_broken` use `peer_addr()`, which does not probe the peer (still marked TODO).
  [F11](../features/error-channel.md) narrowed this rather than closing it: both now also refuse a
  connection whose *read half* has stopped, which is the case they were silently passing. A peer
  that is gone but whose socket has not been reset still looks healthy, and that needs a `Ping`.
  **[F16](../features/client-builder.md) narrowed it again from the other side** — a client now
  has other endpoints to fall back to when one stops answering — without closing it either, since
  detecting that it stopped is still what is missing.
- `Shoal::send` archives the bundle before `track_response` may regenerate its id
  (`shoal-core/src/client.rs:209-215`).
- Two large commented-out blocks remain (`shoal-core/src/client.rs:549-603`, `:1048-1114`).
- `suceeded` / `QuerySuceededOpts` are misspelled in the public API.

**Fix direction:** the health-check half is [D6](../direction/connection-pool.md#health-checks-that-work),
which needs [D2](../direction/framing.md#message-types)'s `Ping` — a discriminant the wire format
has and nothing is behind. The rest are local edits and need nothing.

### 24. shoalctl warnings

`TabState::next` / `prev` are dead code (`shoalctl/src/components/tab.rs:548`, `:558` — the
compiler says so on every build), and `submit_query` `tokio::task::spawn`s the query only to
`.await` the join handle immediately (`:275-279`), so the UI blocks for the round trip anyway.

That `.await` ends `.unwrap()`, which is worth naming separately: a panic inside `query_bar::run`
comes back as a `JoinError` and is unwrapped on the UI task, which is the exact shape
[item 24's fixed half](resolved/shoalctl-panic.md) was about — a panic that unwinds past
`ratatui::restore()` and leaves the terminal in raw mode. The fixed half moved the *parse* error
off that path; a panic from the query round trip still takes it.

### 28. SHQL "Unknown field" names the field it is listing as valid

A field declared on a table but marked neither `partition`, `sort`, nor `filter` — an
`#[shoal(update)]`-only field, for instance — has no `FieldRole`, so using it in a `WHERE`
clause fails. The message it fails with is confusing:

```
Unknown field 'data'. Valid fields are: ["id", "title", "data"]
```

`data` appears on both sides. The cause is that `field_names()` is generated from *all* fields
while the `get_field_role` match arms are generated only for fields that have a role
(`shoal-derive/src/traits/table_schema.rs`), so the two lists disagree by construction.

The error is now raised in **one** place rather than two: the sorted and unsorted parse arms share
a `check_conditions` quote (`shoal-derive/src/structs/client.rs:238-247`). This entry used to name
`:127-137` and `:198-208` as the two sites, and that is worth recording because it makes the fix
smaller than the entry implies — a single generated block to change instead of a matched pair to
keep in step.

**Fix direction:** two different errors are being conflated. Distinguish them — if
`field_names()` contains the name, say `Field 'data' cannot be used in a WHERE clause because it
is not a partition key, sort key, or filter`; only say `Unknown field` when it is genuinely
absent. Better still, generate a `queryable_field_names()` alongside `field_names()` so the
"valid fields" list is the set that can actually appear in a `WHERE` clause.

### 29. SHQL rejects exponent notation with a misleading error

`float_number` is `(opt(sign), digit1, ".", digit1)`
(`shoal-core/src/shared/queries/parser.rs:524`), so digits are required on both sides of the
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
(`shoal-derive/src/structs/client.rs:281`), which passes `0, query.len()`; the second from the
missing-partition-key arm beside it (`:507`), which does the same; the third from
`ParsedSelect::new` (`shoal-core/src/shared/queries/parser.rs:1312-1318`), which reports
`at_position(..., 0, query)` because the winnow parser it wraps returns a `ContextError`, and a
`ContextError` carries no input slice to recover an offset from.

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
gave up is the offset, and it is already in scope — literally on the line above, as
`select_start` (`parser.rs:1310`), computed for the projection's sake and then not used for this.

### 40. `UnsortedExists` still names a single partition

`UnsortedGet` now carries `partition_keys: Vec<u64>` (`shared/queries/unsorted.rs:131`) and splits
across shards like its sorted counterpart ([26, 39](resolved/partition-order.md)), but
`UnsortedExists` (`:194-196`) was left with a scalar `partition_key`, and `split_by_shard` routes
an exists with a single `find_shard` (`:56`) where a get gets `group_by_shard` (`:50`).
`SortedExists` has taken a `Vec` all along, so the two table kinds now disagree about what an
exists can ask.

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

`.../persistent/sorted.rs:314-352` — `load_partition` merges a freshly read archive extent into
whatever is resident, and its `Occupied` arm (`:320-322`) only matches `MaybeLoaded::Loaded`:

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

`.../fs/compactor.rs:232-241`

`shoal-core` is edition 2021, where the temporary `Ref` produced by `.borrow()` lives to the end
of the `if let` statement. So the shared borrow on `to_archive` is held across all three awaits,
during which other tasks on the same executor run.

It does not panic today, because the only `borrow_mut` callers — `set_partition` and
`remove_partition` (`.../fs/map.rs:442-459`) — are the compactor itself, which is not running
while it is parked here. It is listed because that is a property of who happens to call what, not
of anything enforced, and because the fix is already sitting next to it:
`ArchiveMap::find_partition` (`.../fs/map.rs:514-520`) exists to copy the entry out, `ArchiveEntry`
is `Copy`, and the loader path already uses it.

Worth reading against the edition too: this becomes correct for free under edition 2024's
temporary scoping, which means an edition bump would silently change the failure mode rather than
the code. **The workspace is already mixed** — `shoal-bench` and `shoalctl` are edition 2024 while
`shoal`, `shoal-core` and `shoal-derive` are 2021 — so the bump is a per-crate decision that has
already been made three times without this being considered.

### 43. The storage marker only guards the default storage root

`shoal-core/src/server.rs:87` claims `storage.default.filesystem.latency_sensitive.path`, and that
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

`StorageMeta::claim` (`server/meta.rs:79`) treats a missing `shoal-meta.json` as a directory
nothing has written to, creates one, and starts (`:105-111`):

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

`FileSystem::read_intent_log` (`.../storage/fs.rs:202-206`) counts a `truncated` reader
against `RecoveryStats::truncated_logs`, and it does so for the active log on the same terms as
for an inactive one:

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

`shoal-core/src/server/messages.rs:248-257`, and similarly for `Comms`
(`shoal-core/src/server/comms.rs:16-23`).

`ServerMsg::Partition` carries a glommio `ReadResult`, which is not `Send`. The whole enum
asserts `Send` anyway so it can travel kanal channels. The invariant — a `Partition` message
may only ever be sent on its own shard's channel — is upheld solely by `FsLoader` being
constructed with a clone of its own shard's sender and no other
(`.../fs/loader.rs:256-271`).

Give a loader, compactor, or any task a sender belonging to a different shard, and `Partition`
messages become cross-thread: undefined behaviour, no compiler error, and probably no test
failure. Any change touching loader construction should be read against this.

**There is now a second place that has to hold, and it is not the constructor.** `FsLoader` keeps
a `senders: Vec<AsyncSender<ServerMsg<D>>>` reuse pool: `spawn_task` pops a sender for the read
task to report on and falls back to cloning `self.shard_local_tx` (`.../fs/loader.rs:292-295`),
and a finished task hands its sender back into the pool (`:370-372`, `:386`). So the invariant is
no longer "the loader holds one sender it cloned at construction" but "**every** sender that ever
enters that pool is a clone of this shard's". It holds today because the only two things that put
one there are the fallback clone and a task that was handed one from the same pool. A future
change that seeds the pool from anywhere else — a shared pool across loaders, most obviously —
breaks the invariant without touching the constructor this section used to point at.

The related `messages.rs:223` panic is the same invariant seen from the other side: cloning a
`Gathered` message is refused because a broadcast would be the way one reached a shard it was not
addressed to.

---

## Suggested triage order

1. **Item 38** — not a production defect, but the test suite is what every other fix on this page
   is judged by, and right now **five** of its binaries can silently serve each other's traffic.
   Worth doing before the fixes below rather than after them. It said "two" until the
   [August 2026 review](review-2026-08.md) re-measured, which is the argument for doing it rather
   than tracking it: every new test binary that starts a server joins the collision by default.
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

`PersistentSortedTable::exists` (`.../persistent/sorted.rs:684-760`) walks the partition keys an
exists named. A key whose partition has to be read from disk pushes a clone of the whole query
into `self.blocked` — through `block_on_load`, which is where the clone is parked — and moves on
(`:711-725`):

```rust
if check_disk {
    // build a query for just this blocked partition
    let blocked_exists = SortedQuery::Exists(exists_query.to_blocked(*partition_key));
    // park this exists if this partition has to be read from disk first
    if self.block_on_load(*partition_key, &meta, blocked_exists).await {
        // remember that we are still waiting on this partition
        blocked.push(*partition_key);
        continue;
    }
}
```

A *later* key in the same loop that is resident and does hold a matching row returns straight
away (`:733-741`):

```rust
if partition.exists(exists_query, &mut seek) {
    return Some((meta.client, meta.id, meta.stamps, response));
}
```

That early return never reaches the `self.pending_exists.insert` at the bottom of the loop
(`:749`), and it does not remove the clone from `self.blocked`. When the partition finishes
loading the clone is replayed and answers the same `(id, index)` a second time. The client is owed
exactly one response per query, and an unordered stream will surface both.

The refactor that introduced `block_on_load` moved the parking out of this function and did not
change the shape of the defect: the clone is parked by the callee and the early return is still
the caller's.

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

The field is parsed and kept (`shoal-bench/src/model/hotpath.rs:35`) and never plotted or
tabulated, which is the right treatment. The trap is that it looks exactly like the
number anybody would reach for first. `total` — nanoseconds summed across every shard that entered
the scope — is the field to rank by, and it needs saying that it is a sum across shards rather
than a share of the wall clock, which is why the chart's axis on
[Benchmark Results](../performance/overview.md) says so.

**Established by reading the committed artifacts**, while building
[F7](../features/bench-runner.md). `shoal-bench` never plots or tabulates the field, and
`render::chart::hotpath_scopes::tests::the_unnormalised_percentage_is_never_drawn`
(`shoal-bench/src/render/chart/hotpath_scopes.rs:149-186`) pins that by feeding a scope a
`percent_total` of 12,530 and asserting the number never reaches the chart.
Fixing it properly is upstream in `hotpath`, or means dividing by the shard count that actually
touched each scope — which the profile does not record.

### 54. A schema still declares `rkyv` and `deepsize2` itself

`shoal-proto/src/shared/traits/sorted.rs:10`, `unsorted.rs:10` and `traits.rs`, the `DeepSizeOf`
supertrait bound; and every schema's own `#[derive(Archive, Serialize, Deserialize)]`

**Mostly fixed — see [Resolved #54](resolved/macro-emits-three-crates.md) for the part that is
done and why.** The macros no longer name any crate by path: they emit `::shoal::` and the facade
re-exports what they reach for, and [F15](../features/client-server-split.md) took glommio out of
a client's dependency graph rather than re-exporting it. `glommio`, `uuid` and `kanal` are gone
from the manifests that carried them under a comment saying they should not have been there.

What remains is not a macro path and could not be fixed by re-pointing one. A schema writes

```rust
use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
```

by hand, and both derives expand to absolute paths into their own crates, so both have to be
declared. `deepsize2` is there because `ShoalTableSupport` is bound on `DeepSizeOf` — the server
sizes a row to charge it against the memory budget — and that bound is visible to a client that
will never evict anything.

The original filing said the generated code "names `glommio`, `uuid` and `deepsize2` by path".
That was wrong about `deepsize2`, which no macro has ever emitted; the third name should have been
`kanal`. Corrected on the resolved page rather than here.

**Established by reproducing it**, while building [F8](../features/purpose-built-workloads.md).

**Fix direction:** two options, both filed in [TODOs](todos.md). Emit the `DeepSizeOf` impl from
`#[derive(ShoalSortedTable)]` by summing the row's fields, which removes the derive from the
schema author's hands but hand-rolls an accounting impl the eviction budget depends on being
right; or take the bound off the client-visible traits and put it where the server needs it.
`rkyv` is harder and may not be worth closing: a schema's types are rkyv types, and saying so is
arguably honest rather than leaky. `shoal::rkyv` and `shoal::deepsize2` exist as escape hatches
meanwhile.

### 55. A get that found nothing is reported as a query that failed

`shoal-core/src/client.rs:313`, `Shoal::send_one`

`send_one` calls `suceeded` (`:816`) on the response and turns a get that matched no rows into
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

**Unblocked, not closed, by [F11](../features/error-channel.md).** "Found nothing" and "failed" are
now different answers on the wire, and `suceeded` reports the second as `Errors::Server` whatever
the options say. What is left is the first half: letting `send_one` say that an empty get is
acceptable. It no longer has to, to tell the two apart — a caller can ask `response.error()`
directly — but it is still the ergonomic gap this item was filed for.

### 58. A shard that dies is not reported to whoever started the pool

```rust
pub fn exit(self) -> Result<(), ServerError> {
    self.should_shutdown.store(true, Ordering::Relaxed);
    for handle in self.shard_handles.join_all() {
        if let Err(error) = handle {
            event!(Level::ERROR, error = error.to_string());
        }
    }
    Ok(())
}
```

`.../server.rs:104-117`, `ShoalPool::exit`

A shard's `Result` is only ever looked at here, at shutdown, and looking at it does not change
what this returns. So a shard that died an hour ago is indistinguishable from one that ran
cleanly: `exit` says `Ok(())` either way, and there is nothing to ask before then — `ShoalPool`
holds the join handles and exposes no liveness at all.

The `ERROR` event is the only trace, and it is written to whatever subscriber the embedding
process installed. A caller that installed none — which is every test — gets silence.

This was found while fixing [item 57](resolved/missing-archive.md), where a shard died on a
partition read twenty seconds before `pool.exit()` returned `Ok(())`. That defect is fixed and
this one is why it could only be observed as a client that never got an answer.

**Fix direction:** the smallest honest version is for `exit` to return the first shard error
rather than swallow it, which changes a signature nothing currently relies on. The useful version
is a liveness check that does not wait for shutdown, since the interesting question is asked while
the server is meant to be running. Both are worth less than a shard that does not die at all,
which is what [item 16](#16-panics-on-the-hot-path) is about.

### 59. A shard that cannot free anything keeps trying, on every message, in silence

```rust
// check if we need to evict any data
if *self.memory_usage.borrow() > self.conf.resources.memory {
    // try to evict our least recently used data
    self.evict_data().await?;
}
```

`shard.rs:984-988`, and `Shard::evict_data` at `:847-882`

The trigger is a level, not an edge: it is re-tested at the bottom of **every** iteration of the
shard loop. `evict_data` answers it by draining the LRU until it has found 40% of current usage
worth of victims, or until the LRU runs out:

```rust
// we have no more rows we could evict even if we wanted too
None => break,
```

`shard.rs:872-873`

When the LRU is empty that arm is taken immediately, the function allocates a
`HashMap::with_capacity(10)` (`:852`), evicts nothing, and returns `Ok(())`. The condition above
it is still true, so it runs again on the next message, and on every message after that, for as
long as the process is over its limit. Nothing is logged: the `None` arm is the only exit and it
says nothing, and the eviction event ([Resolved #13](resolved/eviction-log-underflow.md)) is
emitted per *pass that dropped something*.

So a shard that is over its memory limit and has nothing it is allowed to drop looks, from
outside, exactly like a shard that is under it.

**This is reachable rather than theoretical, and [F9](../features/ephemeral-tables.md) is what
makes it so.** An ephemeral partition never enters the LRU, because `NoStorage` never sends a
`MarkEvictable` — which is the invariant ephemeral data being safe rests on, and is correct. But
ephemeral rows *are* counted in `memory_usage`. A database whose ephemeral tables alone exceed
`resources.memory` therefore sits permanently in this state. The ephemeral page says memory is not
bounded and that bounding it is the caller's problem
([Limitations](../features/ephemeral-tables.md#limitations)); what it does not say is that
exceeding it is silent and costs a pass per message.

The cost of the pass itself is small — a `RefCell` borrow, a comparison, one allocation and an
empty pop. It is filed here rather than in [Optimizations](optimizations.md) because the defect is
the silence, not the work.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md).
Nothing reproduces it, and the cheapest reproduction is an ephemeral table and a small
`resources.memory` — which `ephemeral_sorted_table.rs`'s "memory pressure evicts none of it" test
already sets up, and asserts the data survives rather than what the shard did to keep it.

**Fix direction:** distinguish "nothing needed freeing" from "nothing could be freed". The `None`
arm knows which it is — `need` is still non-zero — so it can emit one `WARN` naming the shortfall.
Rate-limiting it is the whole difficulty: at one per message it is worse than silence. The honest
shape is probably a latch, warning on entry to the state and again on leaving it. Note this should
*not* be fixed by making the trigger an edge — a level trigger is what lets a shard recover when a
partition becomes evictable later.

### 60. A result stream that is not drained to the end leaks its slot in the client

`ShoalResultStream::next` releases the client-side state for a query only when the response it
just returned was the last one:

```rust
if end {
    // remove this stream id from our channel map
    self.channel_map.pin().remove(&self.id);
    // take the ends of our channel
    if let (Some(tx), Some(rx)) = (self.response_tx.take(), self.response_rx.take()) {
        self.channel_queue_tx.send((tx, rx)).await?;
    }
}
```

`shoal-core/src/client.rs:1032-1039`, and the same block in `ShoalUnorderedResultStream::next`
at `:1231-1238`

Neither stream type implements `Drop`. `Shoal` does (`:467`) and it does not walk `channel_map`.
So there are three ways to end a stream without reaching that block:

| How | What is left behind |
| --- | --- |
| Drop the stream before its last response | One `channel_map` entry, and a channel pair that never returns to `channel_queue` |
| ~~`next()` returns `Err` — a failed `access`, a closed proxy~~ | ~~The same~~ — fixed by [F11](../features/error-channel.md): both `next()`s now run the release block on the `Err` path as well as the `end` path |
| `skip()` past the end, or any early `return` in the caller | The same |

The `channel_map` entry is what the proxy looks a response up in (`:536`), so the leak is not only
memory: every response the server later sends for that query id is delivered into a channel with
no reader, which is unbounded. And `channel_queue` is a reuse pool, so a leaked pair is a channel
the next query has to allocate instead of reusing (`:188-191`).

**This is the client-side twin of [item 32](#32-a-disconnected-client-is-never-cleaned-up-anywhere)**
— state keyed by something that goes away, with nothing told about it — and it has the same
consequence, which is that an ordinary well-behaved caller leaks. It is bounded differently,
though: the server's leak is per connection and this one is per query.

`send_one` and `exists` are safe today, and it is worth saying why, because it is not obvious. Each
sends a single-query bundle, so the one response it reads back *is* the end of the stream and the
block above runs. Change either of them to read one response out of a multi-query bundle and they
stop being safe.

**Established by reading the source**, during the [August 2026 review](review-2026-08.md). It has
never been observed, and it could not have been: the streaming APIs have
[no test at all](test-coverage.md#the-streaming-client-apis), and every integration test goes
through `send_one`, `exists`, or a fully drained `send`.

**Fix direction:** a `Drop` impl on both stream types doing what the `end` arm does. The obstacle
is that returning the channel pair to `channel_queue` is an `async` send, which `Drop` cannot
await — so either the queue gains a synchronous `try_send` for this path, or `Drop` removes the
map entry only and lets the pair be dropped. Removing the map entry is the half that matters;
losing a pooled channel is a missed reuse, not a leak.

**`Drop` alone fixes the client and leaves the server wrong**, which is worth knowing before taking
the easy half ([D6](../direction/connection-pool.md#drop-on-both-stream-types)). Once the entry is
gone, the responses the server is still producing arrive at a proxy that cannot find a channel for
them, ~~which today returns `Errors::ProtocolError` and kills the read task for that connection~~ —
[F11](../features/error-channel.md) made that a `WARN` and a `continue`, precisely because killing a
connection over one caller's leak takes every other query on it down too. So the easy half is no
longer actively harmful, and it is still half. The pair that is actually correct is `Drop` plus a `Cancel` message telling
the server to stop, and `Cancel` is a message type the wire format **has a discriminant for and no
wiring behind** since [F10](../features/framing-and-protocol-evolution.md) — so this is no longer
blocked on a flag day, only on the send and the handler
([D2](../direction/framing.md#message-types)).

### 62. A server that has exited leaves its client connections open

`ShoalPool::exit` (`shoal-core/src/server.rs:104-117`) sets `should_shutdown` and joins the shard
threads. The listener goes with them — a connection attempt to the port is refused afterwards — but
the sockets of connections that were already **established** are not closed. A client holding one
sees no `FIN`, so its read loop stays parked in `read_exact` forever and any query written to that
connection waits for a response no thread is left to produce.

The per-connection tasks `client_acceptor` spawns hold the `TcpStream`s, and nothing drains or
cancels them before the executor goes away. When the server runs in the same process as the client —
which every integration test does — there is no process exit to have the kernel clean up after it
either.

**Established by reproducing it**, while building [F11](../features/error-channel.md). A test that
started a server, called `pool.exit()`, and then sent one query never got an answer; instrumenting
`TcpProxy::start` showed the read loop had not returned for any of the pool's ten connections, so
the client-side sweep F11 added had nothing to fire on. Connecting to the port after `exit()`
returns `ECONNREFUSED`, which is what separates "the listener closed" from "the connections closed".

This is the server-side twin of the gap F11 closed on the client. F11 made a connection that
*ends* tell the queries it owed; this is a connection that never ends.

**Fix direction:** two halves, and the first is small. `client_acceptor`'s per-connection tasks
should be held rather than detached, and cancelled on shutdown, so the sockets are dropped before
the executor is — which turns this into an ordinary EOF that the client already handles.
The second half is that a client cannot rely on the peer being well behaved about it, so it wants a
deadline: a `Ping`/`Pong` health check ([item 23](#23-client-stream-and-pool-rough-edges),
`MessageType::Ping` is reserved and unwired) or a per-query timeout
([TODOs](todos.md)). Neither is the error channel — F11 gave a failure somewhere to go and cannot
invent a failure nobody detected.

### 63. Nothing limits how often a peer may guess a password

`server_auth` (`shoal-core/src/server/shard.rs`) runs an exchange for anything that reaches the
port and completes a handshake, and there is no counter anywhere — not per source address, not per
username, not per shard. A peer can open a connection, guess, be refused, and open another one, as
fast as it can complete three round trips.

The asymmetry is what makes it worth filing rather than shrugging at. Each guess costs the
**server** one PBKDF2-HMAC-SHA-256 at the credential's iteration count — on the order of a
millisecond of CPU on a shard that is single-threaded and is also serving queries — while costing
the client a `format!`, because a client that intends to fail does not have to derive anything. A
peer that sends a syntactically valid proof of the wrong 32 bytes gets the server to do all of the
work and does none of it.

That makes this two defects wearing one number. It is a **credential** exposure, and it is more
immediately a **denial of service** one: a few hundred connections a second is a shard spending
its time on PBKDF2 rather than on the table it owns, without ever authenticating.

**Established by reading the source**, while building
[F12](../features/authentication.md). Not reproduced, and the reproduction is the interesting part:
what a working `connect` workload ([O30](optimizations.md)) would have to show is how many refused
exchanges per second it takes for a resident get's p99 to move.

**Fix direction:** the cheap half is a per-connection cap of one exchange — a peer that fails is
already closed, so what this really bounds is reconnection, which belongs with a per-source-address
rate limit and does not exist. The half that removes the asymmetry is to do the derivation
**after** cheap validation and to bound concurrent in-flight exchanges per shard, so that a flood
queues rather than compounds. Note the decoy path ([F12](../features/authentication.md)) constrains
the shape of any fix: whatever is added must cost the same for a user that exists and one that does
not, or it becomes the enumeration oracle the decoy exists to prevent.

### 64. Four `direction/` pages cite the zero-copy read at a line it left two features ago

Every page in the direction chapter that argues from the zero-copy response path quotes the same
three lines and cites them as `shoal-core/src/client.rs:524-527`, `TcpProxy::start`. The read is now
at `client.rs:1063-1066` and lives in `TcpProxy::read_frame`, because
[F10](../features/framing-and-protocol-evolution.md) split frame decoding out of the relay loop. The
quoted code is also one refactor stale — it reads `len` where the tree reads `frame.rest_len`.

| Page | What it cites |
| --- | --- |
| `direction/transport.md` | `client.rs:524-527` — inside D1's argument that QUIC's crypto ends the zero-copy read |
| `direction/overview.md` | `client.rs:524-527` — inside "the constraint every page inherits", which every other page refers back to |
| `direction/authentication.md` | `conf.rs:132-138` for `Networking`, which is now `conf.rs:138-157` and has a third field |
| `api/client.md` | the same read, in the `ShoalResponse` section |

`direction/encryption.md` carried the same citation and is corrected, which is how this was found.
The other four are left as they are rather than fixed in passing, because a sweep that fixes one
stale citation and not the fifteen others on the same pages is the drift this item is about.

**Established by reading the source**, while correcting [D4](../direction/encryption.md).

**Fix direction:** the same treatment this page already gives itself — a
re-resolution pass over every `file:line` in `docs/src/direction/` and `docs/src/api/`, with the
symbol name kept beside each one so the next drift is greppable. The August 2026 review did this
for the appendix and did not cover the direction chapter, which had just been written and was
correct at the time.

### 65. Two `gxhash` majors, and partition keys hashed by the one without `deterministic`

`Cargo.toml:16` and `shoal-proto/Cargo.toml`, `shoal-core/Cargo.toml`; the hash itself is
`shoal-derive/src/traits/partition_key.rs`, `PartitionKeySupport::get_partition_key`

The workspace pins `gxhash = { version = "3", features = ["deterministic"] }`. `shoal-core` and
`shoal-proto` both pin `"2.2"`, which resolves to 2.3.1, **and neither enables `deterministic`**.
Every partition key in every schema is hashed by 2.3.1 without that feature; the workspace pin is
reachable from nothing and its `deterministic` is doing no work anywhere.

Two things are wrong here and they are worth separating. The smaller one is the dead pin. The
larger one is that **a partition key's hash is a persistence format** — it decides which partition
a row belongs to and therefore which file it is in — and nothing states which gxhash produces it,
whether that hash is stable across gxhash versions, or what `deterministic` would change if it
were turned on. Upgrading gxhash, or enabling that feature, would silently re-hash every key and
make every persisted dataset unreadable, and there is no test that would notice.

**Established by reading the manifests**, while deciding which crate should re-export gxhash for
[F15](../features/client-server-split.md). The split made the question live rather than
theoretical: `shoal-proto` had to own the re-export, because a client hashes its own partition
keys, and re-exporting the workspace pin would have put two majors in the graph with the facade's
choice deciding how every key is hashed. It pins the same major the engine does for that reason.

**Fix direction:** pin gxhash once in `[workspace.dependencies]` at the version already in use,
so the two crates cannot drift; then settle what `deterministic` guarantees and whether this
system needs it, and write the answer down next to the pin. A test that asserts a known key hashes
to a known tablet would turn the next accidental change into a failure instead of a data loss.

### 66. The release profile nothing has been reading

`shoalctl/Cargo.toml:28-30`, and the absence of a `[profile.release]` in `Cargo.toml`

`shoalctl` sets `codegen-units = 1` and `lto = true`. Cargo reads `[profile.*]` only from the
workspace root, so both lines have been ignored for as long as they have existed — cargo says so
on every build:

```
warning: profiles for the non root package will be ignored, specify profiles at the workspace root:
package:   /home/mcarson/projects/shoal/shoalctl/Cargo.toml
workspace: /home/mcarson/projects/shoal/Cargo.toml
```

There is no `[profile.release]` at the root either, so **every release build in this workspace —
the server, `shoal-workload`, and every benchmark capture ever taken — uses the default profile:
`lto = false`, `codegen-units = 16`.** This is the same class of trap the root manifest already
records about a `[build]` table being silently ignored there (`Cargo.toml:23-25`).

It is not only a missed optimization. Without LTO, rustc will not inline across a crate boundary
unless a function is generic, `const`, or carries `#[inline]` — which is why splitting the crate
in [F15](../features/client-server-split.md) had to add thirty `#[inline]` attributes, and why
doing so turned out to *improve* the header codec by up to 80%: those calls had always been
cross-crate from the benches and the client, and had never been inlinable.

**Established by reproducing it** — cargo emits the warning above on every command run in this
workspace.

**Fix direction:** decide what the release profile should be and put it at the root. Note that
adding one changes the codegen of every binary and therefore invalidates every stored capture, so
it needs its own before-and-after and a note on
[Performance Baseline](../performance/baseline.md). Whether `lto = "thin"` would let
the thirty `#[inline]` attributes be dropped again is the interesting half of the question.

### 69. Nothing in the workspace ever installs a tracing subscriber

`shoal-core/src/server/trace.rs:75`, `trace::setup`, and the absence of any caller

`trace.rs` builds a `tracing_subscriber` registry, reads `Tracing` out of the config, and wires an
OpenTelemetry exporter when `tracing.remote` names a gRPC endpoint. `pub fn setup(conf: &Conf)` is
its entry point and **nothing calls it** — not `ShoalPool::start`, not `shard::start`, not
`shoal-workload`, not `shoalctl`, not a test. `grep -rn "trace::setup"` over the workspace returns
its own definition and nothing else.

Three things follow, and the third is the one worth noticing:

- **The `tracing` section of `shoal.yml` configures nothing.** A deployment setting
  `tracing.level: Debug`, or pointing `tracing.remote` at a collector, gets the same behaviour as
  one that sets neither, which is silence.
- **Every `#[instrument]` and `event!` in the workspace goes to a no-op dispatcher.** That includes
  the server's — `Shard::reply`, `Shard::handle_query`, `Coordinator::send_to_shard` and the rest
  have never been switched on — and the `event!(Level::ERROR, ...)` calls the error paths use to
  report a dead connection or a refused frame. **An operator debugging a failure gets no output
  from any of them.**
- **It conditions what [F16](../features/client-builder.md) measured.** That feature added spans to
  the client and found they cost nothing across 144 metrics. That is true and narrower than it
  sounds: a span whose dispatcher finds no subscriber is close to free, so the measurement says
  what these spans cost *in this configuration*, and this configuration is the only one that
  currently exists. It is not a result about what the spans cost.

**Established by reading the source and confirming by grep**, while writing
[F16](../features/client-builder.md)'s *Performance* section — the question "what do these spans
cost?" turned into "what is listening?" and the answer was nothing.

**Fix direction:** call `trace::setup` from `ShoalPool::start` and hold its `SdkTracerProvider` for
the life of the pool, since `trace::shutdown` already exists and already takes one. Two things to
decide on the way. A library that installs a **global** subscriber takes that decision away from
the binary embedding it, so this may belong on `shoal-workload` and `shoalctl` rather than on
`ShoalPool`. And whatever the answer, a benchmark capture must keep getting the subscriber it has
always had — none — or every macro number moves for a reason that is not the code being measured.

### 70. An axis writes more digits than its ticks have room for, and twice labels two at the same number

`shoal-bench/src/render/chart/sweep.rs`, the `y_label_formatter` passed to `themed_mesh!`

`chart-grid-latency` draws `900.00 µs`, `1.00 ms` and `1.00 ms` as three consecutive y ticks, and
`chart-row-size-latency` does the same at several places on its axis. Two gridlines carrying the
same label is a reader counting decades wrong: the chart says the two lines are at the same value
and they are not.

The cause is rounding, not placement. plotters chooses key points on a logarithmic axis at ratios
that are not round numbers, and `fmt::duration_ns` writes two decimal places in each band, so
1,000,000 ns and 1,004,000 ns both come out `1.00 ms`. Nothing detects the collision because the
formatter sees one value at a time and has no memory of the previous tick.

[F19](../features/chart-legends.md) fixed this for the **x** axis of every sweep, by ticking at the
values the workloads were measured at instead of at a spacing plotters chose — there are never many
of those and they are exact. The y axis has no such list: it is a continuum of measured costs, and
picking ticks for it means choosing them rather than reusing them.

**The same formatter crowds an axis it does not duplicate.** `chart-hotpath-scopes` ticks at
`0.00 ns`, `50.000 s`, `100.000 s` … `450.000 s`, spaced 38 user units apart. `150.000 s` is nine
characters at a 9.68 unit font, so two adjacent labels want about 44 units between their centres
and have 38. Nothing here is wrong, only unreadable, and the cause is the same one: a formatter
that writes a fixed number of digits per band, chosen without reference to how much room the axis
gave it. Three decimals in the seconds band exist so that a 1.802 s measurement in a *table* keeps
its precision; on an axis whose ticks are round multiples of fifty they are three characters of
nothing. This half is older than item 70's other half and predates
[F19](../features/chart-legends.md) — `hotpath_scopes.rs` is untouched by it.

**Established by reading the generated pages.** `sed -n '/chart-grid-latency/,/<\/svg>/p'
docs/src/performance/grid.md | grep -E '^[0-9.]+ (ns|µs|ms|s)$' | uniq -d` prints `1.00 ms`. The
crowding is read off the tick anchors in `docs/src/performance/attribution.md`, which are `x="..."`
attributes 38 apart on the `y="348"` row.

**Fix direction:** give the value axis a key-point list of its own — round numbers of the unit the
range lands in, one per half decade — rather than deduplicating labels after the fact. Dropping the
duplicate leaves an unlabelled bold gridline, which is a smaller lie than the current one but is
still a chart that has a gridline it will not name. `sweep::Scale` already takes a tick list, so
the mechanism exists; what it needs is a generator for it.

Everything that has been fixed, and why it was fixed the way it was, is in
[Resolved Issues](resolved-issues.md). The SHQL parser has gained test coverage at both stages
([SHQL](../api/shql.md#testing)) — items 26–29 were found while writing it.
