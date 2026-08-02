# Overview

Shoal is a single process containing N independent shards, one per usable CPU core. A shard
owns a slice of every table, its own write-ahead log, its own set of archive files, and its
own background tasks. Nothing is shared between shards except channels.

## The big picture

```
                        ┌──────────────────────────────────────┐
   client               │            shoal process             │
  ┌────────┐            │                                      │
  │ Shoal  │──TCP──────▶│  shard 0 (core 1)   shard 1 (core 2) │
  │ <S>    │            │  ┌───────────────┐  ┌──────────────┐ │
  │        │◀───────────│  │ tcp accept    │  │ tcp accept   │ │
  └────────┘            │  │ tablet routing│  │tablet routing│ │
   bb8 pool             │  ├───────────────┤  ├──────────────┤ │
   + response           │  │ tables        │  │ tables       │ │
     demux              │  │  partitions   │  │  partitions  │ │
                        │  ├───────────────┤  ├──────────────┤ │
                        │  │ intent log    │  │ intent log   │ │
                        │  │ compactor     │  │ compactor    │ │
                        │  │ loader        │  │ loader       │ │
                        │  └───────┬───────┘  └──────┬───────┘ │
                        │          │  kanal channels │         │
                        │          └────────◀───────▶┘         │
                        └──────────┼─────────────────┼─────────┘
                                   ▼                 ▼
                          Shard-0-active      Shard-1-active     (intent logs)
                          archives/<uuid>     archives/<uuid>    (archives)
                          maps/Shard-0        maps/Shard-1       (archive maps)
```

Every shard is symmetric. There is no leader, no coordinator process, and no shard that is
special. The word "coordinator" appears in the code (`Coordinator::send_to_shard`,
`shoal-core/src/server/shard.rs:413`) but it names a *role a shard plays for one query*, not
a distinct component: whichever shard accepted the client's TCP connection routes that
client's queries to their owning shards.

## Layering

```
   ┌───────────────────────────────────────────────────────────┐
   │  user code:  #[derive(ShoalSortedTable)]  #[db] struct     │
   └────────────────────────────┬──────────────────────────────┘
                                │ shoal-derive generates
   ┌────────────────────────────▼──────────────────────────────┐
   │  QueryKinds / ResponseKinds / TableNames / *Client         │
   │  impl ShoalDatabase  (the dispatch layer)                  │
   └────────────────────────────┬──────────────────────────────┘
                                │
   ┌────────────────────────────▼──────────────────────────────┐
   │  PersistentSortedTable | PersistentUnsortedTable           │
   │    partitions: HashMap<u64, MaybeLoaded<Partition>>        │
   │    pending / blocked / pending_data                        │
   └────────────────────────────┬──────────────────────────────┘
                                │ StorageSupport
   ┌────────────────────────────▼──────────────────────────────┐
   │  FileSystem<D>                                             │
   │    StreamWriter (intent log)   FsLoader (reads)            │
   │    ArchiveMap  ──▶ FileSystemCompactor                     │
   └───────────────────────────────────────────────────────────┘
```

The two seams worth noticing:

- **`ShoalDatabase`** (`shoal-core/src/shared/traits.rs:204`) is entirely generated. It is
  how a `QueryKinds` enum variant becomes a call on a concrete table field. Read
  `shoal-derive/src/traits/db.rs` to see it.
- **`StorageSupport`** (`shoal-core/src/server/tables/storage.rs:225`) is the storage engine
  abstraction. `FileSystem` is currently its only implementor, and `Loaders` — the enum
  naming storage engine kinds — has exactly one variant, `FileSystem`
  (`.../storage.rs:189-193`). The abstraction exists but has never been exercised by a
  second implementation.

## Module responsibilities

### `shared/`

Types that must agree across the wire.

| File | Contents |
| --- | --- |
| `traits.rs` | `ShoalDatabase`, `QuerySupport`, `ShoalTableSupport`, `RkyvSupport`, `PartitionKeySupport`, `TableNameSupport`. |
| `queries.rs` + `queries/{sorted,unsorted}.rs` | `Queries<S>` bundles and the per-table `SortedQuery`/`UnsortedQuery` enums. |
| `queries/parser.rs` | SHQL, the small SQL-like `SELECT` parser. |
| `responses.rs` | `Response<T>`, `ResponseAction<T>`, and success-checking. |

### `server/`

| File | Contents |
| --- | --- |
| `shard.rs` | The shard event loop, TCP relays, query fan-out, eviction trigger. The core of the server. |
| `ring.rs` | The tablet map: partition key → tablet → owning shard. |
| `comms.rs` | The channel mesh between shards. |
| `messages.rs` | `ServerMsg<D>`, the single enum every shard-local event flows through. |
| `conf.rs` | Configuration. |
| `tables/` | Table implementations, partitions, and storage engines. |
| `trace.rs` | `tracing` + OpenTelemetry setup. |

Two files in this directory — `cursor.rs` and `response.rs` — are **not part of the build**.
They are absent from the `mod` declarations in `shoal-core/src/server.rs:14-22` and reference
APIs that no longer exist (`crate::ShoalRow`, `rkyv::AlignedVec`). They are dead. See
[Known Issues](../appendix/known-issues.md#20-orphaned-source-files).

### `client/`

`client.rs` holds everything: the bb8 connection manager, the TCP response demultiplexer,
`ShoalResponse` (a self-referential zero-copy wrapper), and the three result stream types.
See [The Client](../api/client.md).

## The message enum

Every event on a shard — a new client, an inbound query, a partition arriving from disk, a
flush completing, a shutdown order — is a variant of one enum:

```rust
pub enum ServerMsg<D: ShoalDatabase> {
    Join(ShardInfo),
    NewClient { client: Uuid, client_tx: AsyncSender<(Uuid, Span, AlignedVec)> },
    Client { peer: Uuid, data: BytesMut },
    Query { meta: QueryMetadata, query: <D::ClientType as QuerySupport>::QueryKinds },
    Partition(LoadedPartitionKinds<D>),
    DataFlushed { table: D::TableNames, flushed: u64 },
    MarkEvictable { generation: u64, table: D::TableNames, partitions: Vec<u64> },
    Shutdown,
}
```

`shoal-core/src/server/messages.rs:55-94`

The shard's whole event loop is a `match` on this enum
(`shoal-core/src/server/shard.rs:611-665`). If you want to understand what a shard can do,
this enum is the complete list.

## Design notes

**Everything through one channel.** Rather than separate queues for queries, IO completions,
and control, all shard-local work arrives on a single `kanal` channel. This gives a natural
serialization point — the shard is a single-threaded state machine — and makes the
"flush when idle" optimisation possible: the loop checks `shard_local_rx.is_empty()` and only
then issues a flush (`shoal-core/src/server/shard.rs:655-657`), batching writes for free
under load.

The cost is that there is no prioritisation. A backlog of client queries delays IO
completions and shutdown alike.

**One enum, one `unsafe impl Send`.** Because `ServerMsg::Partition` carries a glommio
`ReadResult` that is not `Send`, the whole enum gets a hand-written `unsafe impl Send`
guarded by a comment rather than by the type system
(`shoal-core/src/server/messages.rs:131-140`). See
[Thread per Core](thread-per-core.md#the-send-escape-hatch).

## Limitations

- The storage abstraction has one implementation; treat `StorageSupport` as a refactoring
  aid rather than a proven extension point.
- No prioritisation between message kinds on the shard loop.
- All channels are unbounded, so there is no backpressure anywhere in the system
  ([Known Issues](../appendix/known-issues.md#15-no-backpressure-anywhere)).
