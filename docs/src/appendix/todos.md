# TODOs and Unbuilt Work

Two lists: the `TODO` markers actually present in the source, and the larger pieces that the
code implies but does not contain.

Open defects are catalogued separately in [Known Issues](known-issues.md), and fixed ones in
[Resolved Issues](resolved-issues.md). Several entries here overlap; where they do, the issue
entry has the detail.

## In-code TODOs

Ten `TODO` comments and one live `todo!()` outside `target/`. The two struck-through rows below
have been done and are kept for their links.

### Storage

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| ~~`.../fs/compactor.rs:201`~~ | ~~`does anything else need to be done to remove this partition from archive maps?`~~ | **Done.** The answer was yes. `MapIntent::Remove` was added, the prune path logs it and drops the entry from `to_archive` after the sync: [Resolved Issues #5](resolved/resurrected-deletes.md). |
| `.../fs/compactor.rs:343` | `make size configurable` | Move `MIN_ARCHIVE_COMPACTABLE` and the hardcoded 50% utilisation threshold into `FileSystemTableConf`. |
| `.../fs/map.rs:351` | `make issue about SerializedMap not needing to track active` | `SerializedMap` does not persist the active archive id, so every restart mints a new one and orphans the previous active archive until compaction reclaims it. Either persist it or document the churn as intended. |
| `.../fs/loader.rs:128` | `todo!("Add back onto loader channel")` | A live `todo!()`. A load request that fails to spawn should be requeued rather than panicking the loader — which currently strands every query blocked on that partition forever. |
| `.../fs/loader.rs:137` | `handle this error` | Loader task errors `panic!` during shutdown drain. |
| `.../fs/loader.rs:157` | `do something with this error` | Same, on the steady-state path. |

### Server

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| `shard.rs:60` | `do something with this error` | `client_rx_relay` panics on any non-EOF socket error. Should tear down the one connection. |
| `shard.rs:126` | `detect collisions?` | Client UUIDs are generated without checking `client_map`; a collision panics at `shard.rs:623`. The client does exactly this check for query ids (`client.rs:192-203`) and could be copied. |
| `shard.rs:132` | `do this with a task queue?` | Per-client relay tasks run on the executor's default queue, so client IO is unprioritised relative to background writes. |
| ~~`.../persistent/unsorted.rs:867`~~ | ~~`handling a partition missing`~~ | **Done.** The startup-path `panic!` is a `warn!` and a skipped intent now. See [Resolved Issues #9](resolved/orphaned-update-intents.md). |

### Client and UI

| Location | TODO | What finishing it involves |
| --- | --- | --- |
| `client.rs:82` | `implement a ping/pong type request?` | `is_valid` calls `peer_addr()`, which cannot detect a dead peer. Needs a protocol-level ping, which needs a message-type field the wire format does not have. |
| `client.rs:1272` | `make it so we don't need to do this` | `ShoalQueryStream::send` overwrites `queries.id` on every bundle. The stream's id should be set at construction. |
| `shoalctl/src/app.rs:436` | `Handle insert mode for editing rows` | Insert mode edits the query bar only; result rows are read-only. Writing would also need SHQL to parse mutations. |

## Larger unbuilt work

Implied by the code's shape but not present.

### Distribution

`ShardContact` has one variant:

```rust
pub enum ShardContact {
    /// This shard is on our current node
    Local(usize),
}
```

`shoal-core/src/server/shard.rs:167-172`

The `match` in `Comms::send` (`comms.rs:46-56`) has one arm. Everything above it — the ring,
`ShardInfo`, the `Join` broadcast — is already shaped for a multi-node cluster; the transport
and membership are missing. Adding a `Remote` variant is the seam.

Also needed for a real cluster: replication (there is exactly one copy of every partition),
membership and failure detection, and rebalancing.

### Rebalancing

Today the shard count is part of the on-disk format — intent logs are `Shard-N-active` and
each shard has its own archive map. Changing `resources.cores` between restarts silently
strands data ([Partitioning](../architecture/partitioning.md#limitations)). Any fix needs
partition migration between shards and a way to discover files belonging to shards that no
longer exist.

### Sort-key predicates

`SortedGet::sort_keys` is plumbed end to end and ignored by the server
([Known Issues #8](known-issues.md#8-sort-keys-are-accepted-and-ignored)). Since partitions
are `BTreeMap`s, point lookups and range scans are cheap to implement — the field, the wire
format, and the storage layout are all already in place. This is the single largest
capability gain available for the least work.

`limit` is in the same position ([Known Issues #7](known-issues.md#7-limit-is-ignored-by-persistent-sorted-tables)).

### An error channel in the protocol

`ResponseAction` can express only booleans and rows
(`shared/responses.rs:26-38`). A server-side failure has nowhere to go, which is why the
server is full of `panic!`s — there is no way to say "that query failed" to a client. Adding
an error variant would unlock replacing most hot-path panics with recoverable errors
([Known Issues #16](known-issues.md#16-panics-on-the-hot-path)).

### Backpressure

Every channel is unbounded ([Known Issues #15](known-issues.md#15-no-backpressure-anywhere)).
Bounding them requires deciding what to do when a shard is saturated — shed load, block the
coordinator, or reject the client — which requires the error channel above.

### Timeouts

Nothing anywhere has a deadline: no query timeout on the client, no timeout on a blocked
query waiting for a partition, and no timeout on the connection pool beyond the initial
connect. A partition load that never completes parks its queries permanently.

### Archive map reconstruction

Archives write a size prefix before each partition specifically so a map could be rebuilt by
scanning — the comment says so (`.../fs/compactor.rs:220-221`). No such path exists, so
`ShoalError::MapCorruption` is fatal even though every byte of data is intact.

### Archive checksums

Intent log records and the map snapshot are checksummed; archive payloads are not. Corruption
there is caught only if rkyv validation happens to reject it, and several call sites
`.unwrap()` that result.

### Storage engine abstraction

`StorageSupport` (`.../storage.rs:225`) and `Loaders` (`:189-193`) are written as extension
points but have exactly one implementation. Until a second exists, treat the abstraction as
unproven — a memory-backed engine for tests would be the natural first user. Note that the
storage tests deliberately do *not* want this: they run against a real filesystem on purpose,
because glommio silently disables O_DIRECT on tmpfs and a memory-backed engine would hide
exactly the alignment and `fdatasync` behaviour they exist to check.

### Build and packaging

- No `rust-toolchain.toml` despite requiring nightly, so the failure mode is a confusing
  compile error ([Building](../getting-started/building.md)).
- The `../glommio` path dependency makes the build non-reproducible from this repository
  alone and blocks publishing.
- No CI configuration in the repository.

### Tests

`shoal-core` has 87 unit tests: the SHQL grammar, the intent log reader and stream writer, the
archive map, and partition tombstone bookkeeping. The storage half of those were commented out
until recently ([Resolved Issues #20](resolved/storage-tests.md)).

The integration tests cover insert, get, exists, delete, and update on both table types, plus
restart behaviour, crash recovery under `SIGKILL`, mutating a partition that exists only in an
archive across enough restarts to catch a stale map entry
([Resolved Issues #4](resolved/unsorted-disk-consultation.md)), and — under a config with a one
byte memory limit — eviction and memory pressure
([Resolved Issues #5](resolved/resurrected-deletes.md)).

Not covered: archive compaction, multi-shard routing, streaming, and concurrency. Nothing
exercises a workload large enough to rotate archives rather than intent logs.

## Dead code

| Location | Status |
| --- | --- |
| `server/cursor.rs`, `server/response.rs` | Not in the module tree; reference removed APIs. |
| `client.rs:544-598`, `:1025-1091` | Large commented-out blocks. |
| `.../fs.rs:74-98` | The previous intent-log writer, commented out. |
| `shoalctl/src/components/tab.rs:527`, `:537` | `next`/`prev`, never called. |
| `EphemeralTable` | Cannot be used in a `#[db]` database ([Table Types](../tables/table-types.md#ephemeraltable)). |
| `shoal/examples/basic.rs.bak` | A `.bak` file in the source tree. |
