# 191. Every group's openraft channels were allocated to their bound, and a node's memory filled with them

Filed and fixed in one change, from round 15 of the lab testing
([memory at ten times the dataset](../../cluster-testing/performance.md#memory-at-ten-times-the-dataset)).

## Symptom

At ten copies of the TMDB dataset, lab nodes sat at their 8 GiB budget under the mixed bench and
held almost no rows: 11 to 24 MiB of an 8 GiB budget, where a node restarted with the same data
held 2.7 to 2.9 GiB in all. `#149`'s budget evicts rows to keep the process under it, so every
get read from disk. Less the archive maps and the tables' indexes, which `Stats` now reports, 5.6
to 5.9 GiB of each node was counted by nothing. It grew for about ten minutes after a start and
stopped only where the process met its budget. Round 14 had seen the same shape at one copy,
where it was small enough to be read as overhead: "rows are a tenth of a node's resident memory".

## Cause

openraft opens several bounded channels for every group in `Raft::new` (its `RaftMsg`,
`Notification`, `InstallFullSnapshotRequest` and state machine command channels), sized in the
thousands of values, and some of those values are hundreds of bytes. On glommio these channels
are ours (`control/runtime/channel.rs`), and `GlommioMpsc::channel` created each queue with
`VecDeque::with_capacity(buffer)`: the whole bound, allocated when the group opened.

The allocation was virtual until written. A `VecDeque` is a ring, and its head moves on with every
value, so a channel that never held more than a few values at once still wrote each slot of its
buffer in turn, and its pages became resident one by one. A node's thirty-seven groups (thirty-six
data groups and the control group) held about 2.1 GiB of such queues. Under load every page was
touched within minutes, and the process's resident size rose by that much while the rows it
could keep fell by the same amount. tokio's channel, which openraft was written against, allocates
blocks as values arrive, so nothing in openraft's own tests would show it.

## Evidence

**Established by a heap profile on the lab, and by a test that fails on the old allocation.** The
node program gained a `jemalloc-prof` feature; europa's node ran it with sampled profiling
(`_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19,lg_prof_interval:30`) under the mixed bench on the
ten copy cluster, and `target/lab/r15/prof/heap.py` symbolized its last dump:

```text
9.42 GiB live, sampled every 524288 bytes, 136 stacks
   2693.3 MiB  <hashbrown::map::HashMap>::insert <- ::new::{closure#0}::{closure#0}
   1288.1 MiB  <...PersistentUnsortedTable<tmdb_dataset::Movie, ...>>::apply
   1152.0 MiB  ::channel::<openraft::core::raft_msg::install_full_snapshot_request::InstallFullSnapshotRequest<...>>
                 <- <openraft::raft::Raft<...>>::new::::{closure#0}::{closure#0}
    768.0 MiB  ::alloc <- <rkyv::ser::allocator::alloc::ArenaHandle as rkyv::ser::allocator::Allocator>::push_alloc
    658.0 MiB  <tmdb_dataset::ArchivedMovie as rkyv::traits::Deserialize<...>>::deserialize
    504.0 MiB  ::channel::<openraft::core::notification::Notification> <- <openraft::raft::Raft<...>>::new
    480.0 MiB  <hashbrown::map::HashMap<u64, ...MaybeLoaded<...UnsortedPartition>...>>::insert
    432.0 MiB  ::channel::<openraft::core::raft_msg::RaftMsg> <- <openraft::raft::Raft<...>>::new
```

The new test, against the old line:

```text
thread '...channel::tests::a_channel_allocates_what_is_queued_and_gives_a_burst_back' panicked at
shoal-core/src/server/control/runtime/channel.rs:310:13:
assertion `left == right` failed
  left: 100000
 right: 0
```

On the fix, the same twenty minute arm (`target/lab/r15/mem.sh`) on the same cluster:

| Build | Rows a node, 20 min in | Counted by nothing |
| --- | --- | --- |
| `bbeb814`, before | 11–24 MiB | 5.6–5.9 GiB |
| a map saved without a clone, a sparse table map shrunk | 18–590 MiB | 4.8–6.0 GiB |
| this fix and O81's arena | 1.2 GiB on titan and hyperion | 2.6–3.6 GiB |

europa's profile on the fix held no channel at all among its allocations, and 1.9 GiB of rows.

## The fix

**A channel's queue grows as values arrive** (`VecDeque::new()`), so its bound is how many values
may wait and not what it allocates, as it is for tokio's. **A drained queue that grew past
`KEPT_SLOTS` (16) is shrunk back to them** (`Shared::pop`), so a burst of messages to a group
gives its memory back once it is handled rather than holding the high water for the life of the
group.

## Alternatives rejected

- **Smaller bounds in openraft's `Config`.** The bounds are openraft's back pressure, and some are
  not configurable. The defect was ours: a bound is not an allocation anywhere else.
- **Keeping the high water.** A burst that queued thousands of values would again leave its pages
  resident for good, one channel at a time. The shrink costs a reallocation when a queue drains
  after one, which is rare.
- **Counting the channels in the budget.** They were not state worth keeping, only empty rings.

## Invariants to uphold

- **A bound on a channel is never an allocation.** Any queue sized by a peer's or a library's
  limit grows with what it holds.
- **The runtime's channel keeps tokio's semantics**: a send on a full queue waits, a receive on an
  empty one waits, a weak sender keeps nothing alive. openraft's conformance suite over the runtime
  (`cargo test -p shoal-core control`) asserts them and still passes.

## Still open

- A lab node at ten copies still holds 2.3 GiB of archive map for 23 million partitions, which no
  budget counts and which only moving the index to disk would bound
  ([todos](../todos.md#a-nodes-archive-map-is-bounded-by-nothing)).
- europa's node on mimalloc showed 5.5 GiB counted by nothing in the fixed arm, where titan's and
  hyperion's showed 2.6 to 3.6; profiled on jemalloc the same node held no such thing. Recorded in
  [what is left](../../cluster-testing/todo.md), not explained.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `control::runtime::channel::tests::a_channel_allocates_what_is_queued_and_gives_a_burst_back` | A channel bounded at 100,000 allocates 100,000 slots before a value is sent, and a drained burst keeps them |
| openraft's runtime conformance suite, `cargo test -p shoal-core control` | The channel's semantics, which the change must not move |
| The lab arm, `target/lab/r15/mem.sh` | A node's memory counted by nothing climbs to its budget under the bench and pushes its rows out |

## Related

- [#149](node-memory-budget.md), the process budget that evicted the rows to make room.
- [O81](../optimizations.md#o81-a-map-save-kept-a-copy-of-the-map) and
  [O82](../optimizations.md#o82-a-tables-partition-index-kept-the-capacity-of-its-peak), the other
  memory the same profile found.
- [F37](../../features/node-identity-control-plane.md), where the glommio runtime for openraft was
  written.
