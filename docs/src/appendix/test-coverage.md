# Test Coverage

What the test suite reaches, what it does not, and the one place where it is unsound.

**Established by running it.** `cargo check --workspace --all-targets` passes with warnings and
`cargo test --workspace` passes: **168 integration tests** (one ignored), **194 `shoal-core` unit
tests**, **11 doctests**. That is up from 157, 194, and 11 with the query error display coverage
added by [item 48](resolved/query-error-display.md) — eleven rendering tests and no unit tests,
because everything the fix does it does on screen. It is up from 136, 183, and 11 with the
projection coverage added by
[F2](../features/projections.md), and from 136, 178, and 11 with the compaction tail loss tests
added by [item 44](resolved/compaction-tail-loss.md) and the marker format test added by
[item 45](resolved/storage-marker-format.md) — both fixes are unit-testable end to end and
neither added an integration test, which is itself the
[observability gap](todos.md#observability) talking: no test can observe an event the server
emits. It is up from 135, 178, and 11 with the empty rotated log test added
by [item 14](resolved/empty-rotated-logs.md), from 135, 177, and 11 with the eviction accounting
test added by [item 13](resolved/eviction-log-underflow.md), from 133, 168, and 10 with the tablet map and
storage marker tests added by [items 11, 12 and 37](resolved/tablet-ring.md), from 132, 159, and 10 with the
multi-log recovery test added by [item 31](resolved/multi-log-recovery.md) and the recovery
counting tests added by [item 9](resolved/orphaned-update-intents.md), from 115, 129, and 8 with
the range coverage added by [F1](../features/sort-key-ranges.md), from 105 and 116 with the
sort-key selection coverage added with [item 8](resolved/sort-keys.md), and from 87, 95, and 6
before the row-order and `IN`/`OR` coverage added with [items 26 and 39](resolved/partition-order.md). The two persistent-table
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
| `persistent_sorted_table.rs` | 57, one ignored | insert; `exists` true and false; delete; delete after restart; delete surviving restart; delete and update when the partition is not resident; delete and writes surviving eviction; update; update intent replay; multi-log recovery; empty rotated log cleanup; acknowledgement surviving `SIGKILL`; five limit tests; two cross-shard tests; five row-order tests; six sort-key selection tests; two sort-key `exists` tests; six range tests including the archived seek and the memory/disk span; the paging walk; two range `exists` tests; three end-to-end SHQL tests; nine projection tests including the archived scan, the blocked disk read, the cross-partition order, and a projected and an unprojected get in one batch |
| `persistent_unsorted_table.rs` | 15 | insert; delete; update; delete and update when not resident; delete surviving eviction; insert after delete when not resident; zero limit; three multi-partition tests; three projection tests |
| `shql.rs` | 53 | SHQL parsing and binding against a real schema, including range binding and the role refusals, projection binding and its two refusals, plus completion suggestions |
| `storage_meta.rs` | 2 | that a storage directory restarts under the shard count that wrote it and refuses a changed one, end to end through a real server |
| `completion.rs` (`shoalctl`) | 34 | the completion menu, key handling, query wrapping, and rendering, including the projection slot; and the error box, the underline under the part of a query that failed to parse, the cases where that underline is refused as misleading, and that an error never becomes part of the query it describes |
| `lib.rs` (`shoal`) | 7 | the bencher's percentile and summary statistics, and baseline file handling |

The restart, eviction, and `SIGKILL` tests are the valuable ones: they are the only tests that
exercise durability end to end, and they exist because
[items 1-3](resolved/durability.md), [4](resolved/unsorted-disk-consultation.md), and
[5](resolved/resurrected-deletes.md) needed them.

### Unit — `shoal-core`

| Module | Count | What it reaches |
| --- | --- | --- |
| `shared/queries/parser/tests.rs` | 60 | the SHQL grammar, including `IN` lists, `OR` folding, each range operator, the folding and refusals around a range, and the projection slot with its offsets |
| `shared/queries/parser/complete/tests.rs` | 27 | completion suggestion generation, including the range operator tokens and a projection standing where the star does |
| `.../storage/fs/tests.rs` | 21 | the intent log reader against real files, including which tail shapes are damage and which are how a healthy log ends, and what a compaction is about to throw away with the log it deletes |
| `.../storage/fs/stream_tests.rs` | 13 | `StreamWriter` alignment, padding, and watermarks |
| `tables/partitions.rs` | 45 | tombstone bookkeeping, limits, sort-key selection and range selection on `get` and `exists`, the empty-range guard, `merge_from_disk` sizing, the recovery counting that separates a correctly dropped update from a lost one, and the projected scan across all three selections |
| `shared/queries.rs` | 10 | sort-key normalization, and `SortRange` emptiness and containment |
| `tables/storage.rs` | 6 | `PendingResponse` release against a durable watermark, and `RecoveryStats` merging and cleanliness |
| `server/ring.rs` | 6 | the tablet map: that an empty one cannot be built, that tablets are split evenly and no shard is starved, that ids come from the high bits so a split stays incremental, and that two independently built maps agree |
| `server/meta.rs` | 4 | claiming a storage directory, reopening it under the same shard count, refusing a changed one, and refusing a marker whose format this build does not know |
| `tables/persistent.rs` | 2 | the two pieces of arithmetic on the shard memory counter: that a shrink subtracts instead of wrapping, and that an eviction summarizes itself without underflowing on a drifted counter |
| `.../storage/fs/map.rs` | 1 | map intent replay |

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

*Intent* log compaction has one test now — `empty_rotated_intent_logs_are_deleted`, added with
[item 14](resolved/empty-rotated-logs.md) — but it asserts on what the compactor removed, not on
what it wrote. The archive side above is untouched by it.

### Multi-log recovery

**Now covered**, by `multi_log_recovery_keeps_earlier_intents`
(`persistent_sorted_table.rs`), written to reproduce
[item 31](resolved/multi-log-recovery.md). It is worth reading before writing another recovery
test, because it works around the thing that made this gap persist: the only way to leave an
inactive log behind is to interrupt a compaction, and a test cannot interrupt one reliably. So it
does not try. Two real single-shard servers write two genuine intent logs, and the test then
arranges them on disk — one renamed to `Shard-0-inactive-1`, the other copied in as the active
log — into the state an interrupted compaction leaves behind. No `SIGKILL`, no timing.

Writing it also corrected the gap's premise. Two inactive logs are not needed: the active log is
always replayed last, so one inactive log plus the active log is enough — a single interrupted
compaction, rather than two. ~~And that is a state a clean shutdown produces.~~ It was, while
every clean shutdown left an empty inactive log behind; since
[item 14](resolved/empty-rotated-logs.md) a clean shutdown leaves none, which is why the test
stages the log by hand rather than arranging for one.

What is still not covered is a recovery spanning *three or more* logs, and one where the same
partition is touched in three different generations.

### Anything that is only reported through `tracing`

`ShoalPool::start` does not initialize a subscriber — `trace::setup` is called by the example
binary, not by the server (`shoal/examples/tmdb.rs`). No integration test can therefore observe
any event the server emits, and none tries.

That is what the per-shard recovery summary added by
[item 9](resolved/orphaned-update-intents.md) runs into: the counting that feeds it is unit
tested from four directions, but the event itself — its level, its fields, and that it fires once
per shard — was verified by hand against the `tmdb` example and has no automated coverage. The
same is true of the compaction summary and every eviction event.

Closing this needs a subscriber a test can install and read back. The obstacle is that a
subscriber is process-global while these binaries run their tests in parallel threads
([below](#the-suite-cannot-safely-run-its-binaries-in-parallel)), so captured events would have
to be attributed to the test that caused them.

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
| `persistent_sorted_table` | 13000-13085, plus 13900 and 13901 |
| `persistent_unsorted_table` | 13000-13024 |

Both ranges have grown since they were first measured — 13034 and 13021 — because every test that
restarts a server binds another port. Re-measure them with `-- --nocapture` rather than trusting
the numbers above; the overlap is the point, not the endpoints.

**Every port the unsorted binary binds is also bound by the sorted one.** The second bind does not
fail: glommio sets `SO_REUSEPORT` on listening sockets, so it succeeds silently and the kernel load
balances connections between the two servers. A client can be handed a server belonging to a
different test, with a different schema and a different temp dir, and nothing reports it.

`persistent_sorted_table.rs:812` also hardcodes `let port = 13900`, which collides with itself
across concurrent runs of that one binary.

**This has not been observed to fail.** The full suite was run four times while establishing the
baseline above and passed every time — the servers simply are not alive on the same port at the
same instant. Nothing arranges that.

Binding port 0 and reading back the assigned port would remove the shared namespace entirely,
which is the only fix that does not just relocate the problem to the next binary someone adds.
