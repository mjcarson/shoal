# 133. A get naming two partitions crashed the node that coordinated it

## Symptom

On the three-node lab cluster ([distributed cluster testing](../../cluster-testing/correctness.md)),
the first `verify-acks` run killed europa's node with `SIGSEGV`. systemd restarted it, and it
crashed again 16 seconds later when the next run reached it. Titan and hyperion crashed the same
way as soon as a read was sent through them instead. Every crash was at the same instruction:

```text
traps: unnamed-6[3992307] general protection fault ip:62bbbc084688 sp:733f72bbdc90 error:0 in tmdb-dataset-node[173f688,62bbbae9b000+1248000]
unnamed-2[3994435]: segfault at ffffffffffffffff ip 00005d7549bc5688 sp 000078f4459bfde0 error 5 in tmdb-dataset-node[173f688,5d75489dc000+1248000]
```

File offset `0x173f688` is `_mi_heap_delayed_free_partial`, inside mimalloc. That is a heap that
was corrupted earlier, not the code that corrupted it. Narrowing it on the cluster:

| Read through one member | Result |
| --- | --- |
| 20,000 single id gets | answered |
| gets of 16, 64 and 256 ids | the member crashed |
| gets of 2 ids, at `One`, without the client's retry | the member crashed |
| gets of 2 ids on a standalone node | answered |

So any get naming two partitions crashed whichever cluster member coordinated it: a denial of
service open to any authenticated client. The TMDB load had not hit it, because its verify phase
reads one movie per get.

## Cause

`ReadPlan::tokens` was an `Rc<[SessionToken]>` ([F41](../../features/read-consistency.md)). A
`ReadPlan` rides inside `QueryMetadata`, and a get naming partitions on several shards is split
into shares. Each share's metadata is cloned on the coordinating shard and sent over the shard
mesh to the shard that owns the partition, which drops it when it answers. `Rc`'s count is not
atomic. Two shard threads incremented and decremented one count at once, lost updates, and freed
the token slice while another share still held it: a use-after-free, then a double free, in
whatever the allocator did next.

The compiler did not refuse this because `ServerMsg` asserts `Send` for the whole enum with an
`unsafe impl`. The Safety comment on that impl, and the
[Unsafe `Send` invariant](../known-issues.md#unsafe-send-invariant) section, named the variants that
must never cross a thread: a partition read and the tablet group variants. Nothing named
`QueryMetadata`, which crosses threads on every split query, so nothing checked what went into it.
F41 added the `Rc` as an optimization (O50 describes the clone as "a count and two words"), and it
compiled because the `unsafe impl` said it was fine.

A single-partition get stays on one shard, so its plan never crosses a thread. A standalone node
splits gets too, which is why this page does not claim the standalone path was safe. It raced
there as well, but no run caught it.

## Evidence

**Reproduced on the cluster, then located with AddressSanitizer.** The node program was built on
nightly with `-Zsanitizer=address -Zsanitizer-recover=address` and the system allocator (the new
`system-allocator` feature of `tmdb-dataset`, since ASan cannot see into mimalloc's heap). It was
swapped onto hyperion alone and sent the two id get. It reported:

```text
ERROR: AddressSanitizer: heap-use-after-free on address 0x6dbc53860910
READ of size 8 at 0x6dbc53860910 thread T3
  #0 core::ptr::drop_glue::<shoal_core::server::messages::QueryMetadata>
  #1 <Shard<tmdb_dataset::Tmdb>>::handle_gathered::{closure#0}::{closure#0}
freed by thread T3 here:
  #0 ___interceptor_free
  #1 <Shard<tmdb_dataset::Tmdb>>::handle_gathered::{closure#0}::{closure#0}
previously allocated by thread T3 here:
  #1 <alloc::rc::Rc<[SessionToken]>>::allocate_for_layout
  #2 <Shard<tmdb_dataset::Tmdb>>::read_plan
  #3 <Shard<tmdb_dataset::Tmdb>>::send_to_shard::{closure#0}

ERROR: AddressSanitizer: heap-use-after-free on address 0x6dbc536dc170
READ of size 8 at 0x6dbc536dc170 thread T3
  #0 <QueryMetadata as Clone>::clone
  #1 <Shard<tmdb_dataset::Tmdb>>::handle_released::{closure#0}::{closure#0}
freed by thread T4 here:
  #1 <Shard<tmdb_dataset::Tmdb>>::handle_gathered::{closure#0}::{closure#0}
```

The second report is the cross-thread case exactly: the slice was allocated and cloned on T3 and
freed on T4. The `global-buffer-overflow` and `stack-buffer-overflow` reports in the same log are
gxhash reading a whole 16-byte block past the end of a short key, which it does on purpose within
a page and which recover mode reports once per location. They are unrelated to this crash.

The regression test was then written and run against the unfixed tree. It does not compile, which
is the point:

```text
error[E0277]: `std::rc::Rc<[SessionToken]>` cannot be sent between threads safely
    --> shoal-core/src/server/messages.rs:1133:23
     |
1133 |         assert_send::<ReadPlan>();
     |                       ^^^^^^^^ `std::rc::Rc<[SessionToken]>` cannot be sent between threads safely
```

## The fix

`ReadPlan::tokens` is an `Arc<[SessionToken]>` (`server/messages.rs`, with the two places a plan
is built, `Shard::read_plan` in `server/shard/reads.rs` and the forwarded plan in
`server/shard.rs`). A new test asserts that `ReadPlan` and `QueryMetadata` are `Send` in their own
right, so the compiler checks what the `unsafe impl` on `ServerMsg` does not.

After a rolling upgrade to the fix ([F55](../../features/cluster-upgrade.md)), the two id get that
crashed every member returned all 2,000 rows. `verify-acks` read all 1,048,800 acknowledged
inserts back through each of the three members in turn, at `Quorum` in gets of 256, with no
restart.

## Alternatives rejected

- **Cloning the tokens per share (`Vec<SessionToken>`).** Also sound, but it allocates per share
  for what is usually an empty slice. An `Arc` of an empty slice is one allocation per query, as
  the `Rc` was, and its clone is one atomic increment.
- **Building the plan again on the shard that runs the share.** The plan carries the
  coordinator's resolution of level, deadline and attempt, and F41 decided to resolve it once.
  Re-resolving it per shard would let two shares of one query disagree about the level.
- **Narrowing the `unsafe impl Send for ServerMsg` to the variants that need it.** That is the
  right long-term shape and is filed below. It is a larger change: the enum is generic over the
  schema, and the variants that are not `Send` are the ones that most need to travel the same
  channel as the ones that are.

## Invariants to uphold

- **Anything carried in `QueryMetadata`, or in any message that crosses the shard mesh, is `Send`
  on its own.** The `unsafe impl` on `ServerMsg` does not check this, and
  `what_a_query_carries_between_shards_is_send` does for `ReadPlan` and `QueryMetadata`. A new
  cross-shard payload type belongs in that test.
- **`Rc`, `RefCell` and `Cell` are for state that stays on one shard.** The two `Rc` variants left
  in `ServerMsg` (a built snapshot and a group's machine state) are sent only to the shard they
  were built on, which is what their Safety comments say.

## Still open

- The `unsafe impl Send for ServerMsg` still covers every variant. A split into a `Send` mesh
  message and a shard-local message would let the compiler check all of them, not only the two
  types the test names. Filed in [todos](../todos.md).
- [Item 132](../known-issues.md#132-ephemeral_sorted_table-aborted-once-in-glibcs-thread-cache-teardown),
  a heap corruption abort in a standalone test binary, fits this cause but is not shown to be it:
  the same binary run under ASan on the unfixed tree reported no use-after-free, only gxhash's
  read past a short key. It stays open.
- The fixture's fan-out tests ran these gets on the unfixed tree for months without crashing. They
  run on glibc's allocator, which let the corruption pass. Nothing in the suite runs under a
  sanitizer.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `what_a_query_carries_between_shards_is_send` (`shoal-core/src/server/messages.rs`) | A plan or metadata that holds an `Rc` or any other non-`Send` field no longer compiles |
| `verify-acks --chunk 2` against the lab cluster ([cluster testing](../../cluster-testing/correctness.md)) | The coordinating member crashes on the first get of two ids |

## Related

- [F41](../../features/read-consistency.md), which introduced the plan and its tokens.
- [O50](../optimizations.md#o50-a-read-plan-is-built-and-cloned-per-share), which described the
  clone's cost, and now describes an atomic one.
- The [Unsafe `Send` invariant](../known-issues.md#unsafe-send-invariant), which this page extends.
