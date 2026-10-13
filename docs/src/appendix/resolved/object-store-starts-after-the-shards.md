# 237. A put past the threshold before a node's object executors start was refused for good

A node starts its object executors once its shards answer ([F85](../../features/devices-and-pools.md)),
and its shards accept clients as soon as they listen. A put past its pool's inline threshold that
reached a shard in between was refused `TooLarge`, which says this node never writes stripes and
which a client does not try again. Found on the lab by [F86](../../features/replicated-pools.md)'s
one-node bench leg, and fixed before its figures were taken.

## Symptom

`shoaladm bench` against a one-node copy of the lab's cluster on titan failed its preload at
64 KiB, every put refused by name:

```text
failed: the object preload at s64k failed 32 puts: {"put": KindSummary { ok: 0, ... errors: {"TooLarge": 32},
samples: {"TooLarge": "Object { id: 01a1337b-ed51-7b53-b8d2-800417a58ad7, outcome: TooLarge, msg: \"an object of
65536 bytes is past its pool's inline threshold of 16384 bytes, and this node writes no stripes: its pool has no
devices, or it has no object executor\" }"}, bytes_received: 0 }}
```

The node had a device claimed and `object_cpus: [1]`, and a poster put through it by hand a
minute later was written as stripes. The bench connects as soon as a node answers. Its clients
authenticated in the same second the node printed `SHOAL_NODE_SERVING`, which it prints once
`ShoalPool::ready` returns. The three-node leg on the same build never met it.

## Cause

`ShoalPool::ready` waits for every shard to report, then starts the object store and sets the
cell the shards reach it through. A shard judged a put by whether that cell was set:
`ObjectGate::judge` passed `store.get().is_some()` to `judge_op` as "this node writes stripes",
and so did the operation's own judgement in `Shard::handle_object` and the task's `store()`.
Before the cell was set, a node that would write stripes a moment later was judged one that never
does. The refusal was the one meant for a node with no object executor, and `TooLarge` is not
retryable, so the client's 30 seconds of retries were never spent.

## Evidence

**Reproduced** by `a_put_before_the_object_executors_start_is_asked_again`, written first and run
on europa against `eb61fa6`. The test starts a node with a device and an object executor,
connects before calling `ready`, and puts 100,000 bytes once:

```text
test a_put_before_the_object_executors_start_is_asked_again ... FAILED
thread 'a_put_before_the_object_executors_start_is_asked_again' panicked at shoal/tests/objects.rs:1298:5:
assertion `left == right` failed: a put before the object executors start: Object { id: 01a13381-0022-7282-ac56-033d34abd247, outcome: TooLarge, msg: "an object of 100000 bytes is past its pool's inline threshold of 16384 bytes, and this node writes no stripes: its pool has no devices, or it has no object executor" }
  left: Some(TooLarge)
 right: Some(NotReady)
```

With the fix it is answered `NotReady`, and once `ready` has run the same put is done and reads
back.

## The fix

**A node is judged as it will be once its store runs.** A node that names
`resources.object_cpus` starts a store, and `objects::cpus::resolve` refuses to start a node
whose named cpus it cannot use, so a node that names any either runs a store or does not run at
all. The connection's gate and the operation's judgement pass `judge_op` "writes stripes" when the
store is set **or** the node names object cpus (`ObjectGate::store_starts`,
`ObjectTask::store_starts`). An operation that needs the store before it is set is refused
`NotReady` with "this node's object executors are starting; try it again"
(`objects::store_starting`), which a client tries again with its backoff. A node that names no
object cpu still refuses `TooLarge`, as before.

## Alternatives rejected

**Starting the object executors before the shards answer.** F85 starts them after, so their
rings never take a locked-memory budget a shard's was owed, as X9's executor did. Nothing about
that has changed.

**Holding the client listener until `ready` returns.** A shard accepts as soon as it listens, and
`ready` is the pool's wait, not the shards'. Holding accept would need a message from `ready` to
every shard. It would also hold tables and inline objects, which need no store and are served
correctly in between.

**Parking the operation until the store is set.** A shard would hold an operation, and its
bytes, for a store that may never start: `ObjectStore::start` failing ends the node. The client
already retries `NotReady` within its budget, so waiting belongs there.

**Refusing every object operation until the store runs.** An inline put, a stat and a delete
need no store, and are answered correctly before it starts.

## Invariants to uphold

- **"Names object cpus" means "runs a store".** `store_starts` is read from
  `conf.resources.object_cpus`. It is right only while every node that names any either starts a
  store in `ready` or fails to start. A store that can be absent on a node that names cpus, or
  that starts later than `ready`, needs `store_starts` from the plan instead.
- **A refusal a later try can turn into `Done` is `NotReady` or `Busy`.** Those are the outcomes
  `ObjectOutcome::retryable` names. `TooLarge` says the node never writes the put and is never
  answered for a state that passes.
- **An object lane's hello before the store runs closes its connection**, and the dialer's
  backoff is its retry. It is never answered as though the node served no slices.

## Still open

Nothing of this item.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_put_before_the_object_executors_start_is_asked_again` | `shoal/tests/objects.rs` | A put past the threshold before `ready` is refused `TooLarge`, and a client gives it up |

## Related

- [F86](../../features/replicated-pools.md), whose one-node bench leg found it.
- [F85](../../features/devices-and-pools.md), which starts the object executors after the shards.
- [F80](../../features/object-wire.md), the object outcomes and which a client retries.
