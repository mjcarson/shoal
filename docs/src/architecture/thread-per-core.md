# Thread per Core

Shoal has no locks on its data path. Not "few locks" — none. Every piece of table state is
owned by exactly one thread pinned to exactly one core, and reached only by that thread.
This is the architectural decision that everything else in the server bends around.

## Starting the shards

```rust
let executor_builder =
    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(cpus.len(), Some(cpus)));

let shards = executor_builder.on_all_shards(enclose!((comms, should_shutdown, shard_counter) move || {
    async move {
        let shard: Shard<S> = Shard::new(&conf, comms, &shard_counter).await?;
        shard.start(should_shutdown.clone()).await
    }
}))?;
```

`shoal-core/src/server/shard.rs:697-710`

`MaxSpread` places executors as far apart as the topology allows — spreading across sockets
and NUMA nodes before packing cores. Each executor is pinned; threads never migrate.

Shard identity comes from a shared counter, not from glommio's executor id:

```rust
let shard_id = shard_counter.fetch_add(1, Ordering::Relaxed);
let info = ShardInfo::new(shard_id);   // name = format!("Shard-{id}")
```

`shoal-core/src/server/shard.rs:287-289`

The comment explains why: "A counter to assign shard IDs starting from zero, independent of
executor IDs" (`shoal-core/src/server/shard.rs:695`). Shard names become filenames
(`Shard-0-active`), so they must be stable and dense across restarts. Executor ids are
neither.

> This makes shard identity **assignment-order dependent**. Shard 0 is whichever executor
> reached the counter first, not a fixed core. The id determines both which tablets the shard
> owns and its on-disk filenames, so this is fine within a run and fine across runs *as long
> as the shard count does not change* — which is now checked at startup rather than assumed.
> See [Partitioning](partitioning.md#limitations).
>
> Note the id, not the name, is what routing uses. The vnode ring hashed shard *names* into
> ring positions; the tablet map assigns by index, and `ShardInfo::name` now only names files
> and identifies a `Join`.

## What a shard owns

```rust
pub(super) struct Shard<D: ShoalDatabase> {
    info: ShardInfo,
    conf: Conf,
    ring: Ring,
    comms: Comms<D>,
    pub tables: D,                       // the user's #[db] struct
    table_map: FullArchiveMap<D::TableNames>,
    client_map: HashMap<Uuid, AsyncSender<(Uuid, Span, AlignedVec)>>,
    shard_local_tx: AsyncSender<ServerMsg<D>>,
    shard_local_rx: AsyncReceiver<ServerMsg<D>>,
    loader_channels: HashMap<Loaders, (AsyncSender<..>, AsyncReceiver<..>)>,
    flushed: Vec<(Uuid, Uuid, Span, ResponseKinds)>,
    high_priority: TaskQueueHandle,
    _medium_priority: TaskQueueHandle,
    tasks: Vec<Task<Result<(), ServerError>>>,
    memory_usage: Arc<RefCell<usize>>,
    lru: Arc<RefCell<LruCache<(D::TableNames, u64), usize, BuildHasherDefault<GxHasher>>>>,
}
```

`shoal-core/src/server/shard.rs:219-263`

Note `Arc<RefCell<..>>` for `memory_usage` and `lru`. That combination is normally a red
flag — `RefCell` is not `Sync`, so `Arc<RefCell<T>>` is not `Send` — and here it is
deliberate and correct: the `Arc` shares the cell between the shard and *its own* tables on
the same thread, never across threads. `RefCell` gives interior mutability with no atomic
cost. The shard borrows it directly (`*self.memory_usage.borrow()`,
`shoal-core/src/server/shard.rs:661`) with no synchronisation because there is no other
thread to synchronise with.

## Task queues

Each shard creates two glommio task queues with different scheduling characteristics:

```rust
let high_priority = executor.create_task_queue(
    Shares::Static(1000),
    Latency::Matters(Duration::from_micros(500)),
    &high_name,
);
let medium_priority = executor.create_task_queue(
    Shares::Static(500),
    Latency::Matters(Duration::from_millis(100)),
    &medium_name,
);
```

`shoal-core/src/server/shard.rs:294-304`

| Queue | Shares | Latency target | Carries |
| --- | --- | --- | --- |
| high | 1000 | 500 µs | The TCP acceptor (`shard.rs:360`) |
| medium | 500 | 100 ms | Compactor (`fs.rs:130`), loader (`fs.rs:482`), shutdown watcher (`shard.rs:403`) |

Glommio uses the shares ratio for proportional CPU time and the latency target to decide how
often to preempt. The intent is clear: accepting connections and serving queries must not be
starved by background compaction, but compaction must still make progress.

There is a third, unnamed queue — the executor's default — used by
`glommio::spawn_local` calls that do not specify a queue: the per-client rx/tx relays
(`shard.rs:133-134`, with a `// TODO: do this with a task queue?`) and the intent log's
background writes (`fs/stream.rs:189`). Write completion therefore competes with client IO
on the default queue rather than being explicitly prioritised.

`_medium_priority` is stored with a leading underscore because the field is only read when
spawning the shutdown watcher; the handle is otherwise passed to tables at construction.

## Accepting connections

Every shard binds the same address:

```rust
let tcp_sock = TcpListener::bind(self.conf.networking.to_addr())?;
```

`shoal-core/src/server/shard.rs:356`

N shards all binding `127.0.0.1:12000` works because glommio's `TcpListener` sets
`SO_REUSEPORT`, letting the kernel distribute incoming connections across the listeners. The
practical consequences:

- A client connection lands on an arbitrary shard, and that shard becomes the *coordinator*
  for every query on that connection.
- The coordinator is almost never the shard that owns the data. Most queries take one extra
  channel hop.
- Because the client keeps a pool of 10–50 connections
  ([The Client](../api/client.md)), a single client is spread across many coordinators.

## The channel mesh

`Comms<S>` holds one unbounded channel per shard, cloned into every shard so any shard can
reach any other:

```rust
pub(super) struct Comms<S: ShoalDatabase> {
    shards: Vec<(AsyncSender<ServerMsg<S>>, AsyncReceiver<ServerMsg<S>>)>,
}
```

`shoal-core/src/server/comms.rs:11-14`

Both ends of every channel are kept, so `get_shards_channels(i)` hands shard `i` a clone of
its own sender *and* receiver (`.../comms.rs:65-74`). The shard uses the sender to post
messages to itself — which is how loaders, compactors and the shutdown watcher get work back
onto the shard loop.

`broadcast` is used exactly twice: to announce a shard joining the ring
(`shard.rs:370-376`) and to announce a new client to every shard
(`shard.rs:136-138`). The latter is why every shard has a `client_map` entry for every
client: any shard may need to reply directly to any client without a return hop.

## The `Send` escape hatch

kanal channels require `Send`. `ServerMsg::Partition` carries a glommio `ReadResult`, which
is not `Send` because it is tied to the executor that produced it. Rather than restructuring,
the code asserts `Send` by hand:

```rust
/// # Safety
///
/// The Partition variant should not be sent across threads ever.
unsafe impl<D: ShoalDatabase> Send for ServerMsg<D> where ... {}
```

`shoal-core/src/server/messages.rs:131-140`

and again for the container:

```rust
/// # Safety
///
/// This is done to allow kanal mesh to be setup. This is only used internally
/// to Shoal and every variant is Send other then the partition variant which
/// is never sent across threads and that must be upheld by shoal developers
/// by ensuring that loaders only have a channel to their current shard. Loaders
/// should only load data for their shard and no others.
unsafe impl<D: ShoalDatabase> Send for Comms<D> where D::TableNames: Send {}
```

`shoal-core/src/server/comms.rs:16-23`

**This is the single most important invariant in the codebase, and it is enforced only by
convention.** The rule: a `ServerMsg::Partition` may only ever be sent on a shard's own
`shard_local_tx`. It is upheld because `FsLoader` is constructed with a clone of its shard's
sender and no other, and `read_partition` sends to that one channel.

`ServerMsg::PartitionLoadFailed` travels the same sender and so is covered by the same reasoning
([Resolved #16, 51](../appendix/resolved/partition-load-failure.md)). It carries no `ReadResult`,
so it would be safe to send across threads — but it is kept shard local anyway, because the value
of this rule is that it has no exceptions to remember.

If a future change gives a loader, compactor, or any task a sender belonging to a different
shard, `Partition` messages become cross-thread and the program has undefined behaviour with
no compiler error and probably no test failure. Any change touching loader construction
should be read against this invariant.

## Shutdown

Shutdown is polled rather than pushed. `ShoalPool::exit` flips an `AtomicBool`
(`shoal-core/src/server.rs:85-87`) and each shard runs a watcher that checks it every three
seconds:

```rust
loop {
    if should_shutdown.load(Ordering::Relaxed) {
        shard_local_tx.send(ServerMsg::Shutdown).await?;
        break;
    }
    glommio::timer::sleep(std::time::Duration::from_secs(3)).await;
}
```

`shoal-core/src/server/shard.rs:152-164`

So shutdown takes up to three seconds per shard, and the tests pay that cost on every
teardown. Once `Shutdown` reaches the loop, the shard signals its loaders, drains remaining
flushed responses, shuts down tables (which flushes and closes storage), then cancels its
spawned tasks (`shard.rs:645-672`).

## Design notes

**Why thread-per-core at all.** The pitch is that a partitioned workload does not need shared
mutable state, so paying for atomics, cache-line bouncing and lock contention is waste. Shoal
takes that seriously enough to use `Rc` and `RefCell` on the data path and to hand-write
`unsafe impl Send` rather than reach for `Arc<Mutex<_>>`.

**The cost is the routing hop.** Because connections land on arbitrary shards, most queries
cross a channel to reach their data. Shoal accepts this rather than trying to route
connections by key — which would require the client to know the ring.

**Unbounded channels are a deliberate simplification, and a debt.** Every channel in the
system is `kanal::unbounded_async` (`comms.rs:33`, `shard.rs:131`, `fs.rs:276`,
`sorted.rs:175`). Sends never block, so no shard can deadlock waiting on another. But
nothing throttles a client either: a shard that falls behind grows its queue until the
process runs out of memory.

## Limitations

- The `Send` invariant is a comment, not a type. See
  [Known Issues](../appendix/known-issues.md#unsafe-send-invariant).
- Shutdown latency is bounded below by the 3-second poll interval.
- No backpressure ([Known Issues](../appendix/known-issues.md#15-no-backpressure-anywhere)).
- Failures on the shard loop are handled by `panic!` rather than by degrading — a single
  malformed client message can take down a shard
  ([Known Issues](../appendix/known-issues.md#16-panics-on-the-hot-path)).
- Per-client relay tasks run on the default task queue rather than a prioritised one
  (`shard.rs:132`, marked TODO).
