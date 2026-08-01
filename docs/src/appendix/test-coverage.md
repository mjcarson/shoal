# Test Coverage

What the test suite reaches, what it does not, and the one place where it is unsound.

**Established by running it.** `cargo check --workspace --all-targets` passes with warnings and
`cargo test --workspace` passes: **115 integration tests** (one ignored), **129 `shoal-core` unit
tests**, **8 doctests**. That is up from 105 and 116 with the sort-key selection coverage added
with [item 8](resolved/sort-keys.md), and from 87, 95, and 6 before the row-order and `IN`/`OR`
coverage added with [items 26 and 39](resolved/partition-order.md). The two persistent-table
binaries take about 24 seconds each; everything else finishes in well under a second. That is with
the default parallelism — the sorted binary takes nearly four minutes under `--test-threads=1`,
because its restart and eviction tests each wait out a real server shutdown.

Defects found while writing this page are in [Known Issues](known-issues.md); performance findings
are in [Optimizations](optimizations.md).

---

## What is covered

### Integration — `shoal/tests/`

| Binary | Count | What it reaches |
| --- | --- | --- |
| `persistent_sorted_table.rs` | 36, one ignored | insert; `exists` true and false; delete; delete after restart; delete surviving restart; delete and update when the partition is not resident; delete and writes surviving eviction; update; update intent replay; acknowledgement surviving `SIGKILL`; five limit tests; two cross-shard tests; five row-order tests; six sort-key selection tests; two sort-key `exists` tests; two end-to-end SHQL tests |
| `persistent_unsorted_table.rs` | 12 | insert; delete; update; delete and update when not resident; delete surviving eviction; insert after delete when not resident; zero limit; three multi-partition tests |
| `shql.rs` | 38 | SHQL parsing and binding against a real schema, plus completion suggestions |
| `completion.rs` (`shoalctl`) | 22 | the completion menu, key handling, query wrapping, and rendering |
| `lib.rs` (`shoal`) | 7 | the bencher's percentile and summary statistics, and baseline file handling |

The restart, eviction, and `SIGKILL` tests are the valuable ones: they are the only tests that
exercise durability end to end, and they exist because
[items 1-3](resolved/durability.md), [4](resolved/unsorted-disk-consultation.md), and
[5](resolved/resurrected-deletes.md) needed them.

### Unit — `shoal-core`

| Module | Count | What it reaches |
| --- | --- | --- |
| `shared/queries/parser/tests.rs` | 48 | the SHQL grammar, including `IN` lists and `OR` folding |
| `shared/queries/parser/complete/tests.rs` | 24 | completion suggestion generation |
| `.../storage/fs/tests.rs` | 15 | the intent log reader against real files |
| `.../storage/fs/stream_tests.rs` | 13 | `StreamWriter` alignment, padding, and watermarks |
| `tables/partitions.rs` | 20 | tombstone bookkeeping, limits, sort-key selection on `get` and `exists`, `merge_from_disk` sizing |
| `shared/queries.rs` | 3 | sort-key normalization |
| `tables/storage.rs` | 4 | `PendingResponse` release against a durable watermark |
| `.../storage/fs/map.rs` | 1 | map intent replay |
| `tables/persistent.rs` | 1 | |

The storage tests run against a real filesystem on purpose — `TempDir::new_in(CARGO_TARGET_TMPDIR)`
rather than `/tmp` — because glommio silently disables `O_DIRECT` on tmpfs, which would make
alignment unenforced and `fdatasync` meaningless (`shoal/tests/utils.rs`, and the note in
[TODOs](todos.md#storage-engine-abstraction)).

---

## What is not covered

Ordered by what would find the most, soonest.

### Compaction and archive rotation

Nothing. `MIN_ARCHIVE_COMPACTABLE` is 10 MiB (`.../fs/compactor.rs:33`) and no test writes near
that, so `compact_archives` never does real work in the suite. That leaves the archive read path,
entry rewriting into a new active archive, the 50% utilization decision, archive deletion, and
`sort_by_load` all unexercised.

`build_pressured_config` (`shoal/tests/utils.rs`) shrinks the *intent log* to 4 KiB so generations
advance quickly, which is what the eviction tests need — but archives are a separate threshold and
nothing shrinks it. Making `MIN_ARCHIVE_COMPACTABLE` configurable is already an open TODO
([TODOs](todos.md#storage)) and is the cheapest way in.

This is the largest gap on the page: compaction is the only component that rewrites committed data.

### Multi-log recovery

Every recovery test starts from at most one inactive intent log. Nothing produces the two-or-more
case, which is exactly where
[item 31](known-issues.md#31-multi-log-recovery-discards-already-replayed-intents) loses data. A
test needs two `Shard-N-inactive-*` logs present at startup, the same partition touched in both,
and an `Update` intent in the later one.

### The streaming client APIs

`stream()`, `stream_unordered()`, `ShoalResultStream::skip`, and the out-of-order reassembly
through `pending: BTreeMap` / `BTreeSet` (`client.rs`) have **no test at all**. Every integration
test goes through `send`, `exec`, `send_one`, or `exists`.

That is where `skip(0)` panicking has been able to sit unnoticed
([item 23](known-issues.md#23-client-stream-and-pool-rough-edges)), and the reassembly logic is
the part of the client most likely to be wrong, since it is the only part that has to hold state
across responses.

### Concurrency and the connection pool

Nothing issues concurrent queries, exhausts the pool, or forces a reconnect. The pool is
configured for 10 idle and 50 maximum connections (`client.rs:140-148`) and every test uses one
query at a time, so `is_valid` / `has_broken` — already known not to detect a dead peer
([item 23](known-issues.md#23-client-stream-and-pool-rough-edges)) — are never exercised against
one.

### Filters, end to end

The SHQL tests confirm a filter is *parsed and bound* into the query
(`shoal/tests/shql.rs`, `binds_unsorted_filters` and friends), and the partition tests confirm
limits are applied. Nothing confirms the server actually excludes a row: no test asserts that a
get with a filter returns fewer rows than the same get without one.

### Unsorted tables lag sorted ones

Sorted tables now have `exists`, multi-partition gets, limits, and cross-shard coverage. Unsorted
tables have none of those — no `exists` test, no multi-partition get, no cross-shard test. The two
implementations have diverged before ([item 4](resolved/unsorted-disk-consultation.md) was
unsorted-only), so the asymmetry is worth closing.

### Lifecycle and hostile input

- **Client disconnect** — [item 32](known-issues.md#32-a-disconnected-client-is-never-cleaned-up-anywhere).
  No test opens a connection, closes it, and asserts anything was released.
- **A shard that never answers its share** — [item 33](known-issues.md#33-collected-split-query-state-has-no-expiry).
  The cross-shard tests only cover the happy path.
- **Malformed wire input** — [item 34](known-issues.md#34-the-request-length-prefix-is-unvalidated).
  Nothing sends a bad length prefix, a truncated body, or an oversized message.
- **Composite sort keys** — [item 42](known-issues.md#42-shql-cannot-express-a-composite-sort-key).
  Sort-key selection is covered on both scans and both table forms
  ([item 8](resolved/sort-keys.md)), but every table in the suite has a single-field sort key, so
  nothing exercises a tuple `Sort` through SHQL or through a seek.

---

## The suite cannot safely run its binaries in parallel

Filed as [item 38](known-issues.md#38-integration-test-binaries-all-bind-the-same-ports), and
repeated here because it bounds the confidence of everything above.

Ports come from a counter that is **per test binary** (`shoal/tests/utils.rs:48-53`):

```rust
static PORT_COUNTER: AtomicU16 = AtomicU16::new(13000);
fn get_unique_port() -> u16 { PORT_COUNTER.fetch_add(1, Ordering::SeqCst) }
```

Cargo runs binaries in parallel, so they all start at 13000 together. Capturing the `listening on`
line (`conf.rs:111`) from each binary in turn:

| Binary | Ports bound |
| --- | --- |
| `persistent_sorted_table` | 13000-13034, plus 13900 and 13901 |
| `persistent_unsorted_table` | 13000-13021 |

**Every port the unsorted binary binds is also bound by the sorted one.** The second bind does not
fail: glommio sets `SO_REUSEPORT` on listening sockets, so it succeeds silently and the kernel load
balances connections between the two servers. A client can be handed a server belonging to a
different test, with a different schema and a different temp dir, and nothing reports it.

`persistent_sorted_table.rs:626` also hardcodes `let port = 13900`, which collides with itself
across concurrent runs of that one binary.

**This has not been observed to fail.** The full suite was run four times while establishing the
baseline above and passed every time — the servers simply are not alive on the same port at the
same instant. Nothing arranges that.

Binding port 0 and reading back the assigned port would remove the shared namespace entirely,
which is the only fix that does not just relocate the problem to the next binary someone adds.
