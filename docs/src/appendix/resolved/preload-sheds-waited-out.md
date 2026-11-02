# 357. A bench's preload failed inserts shed past every retry, and no node said what shed them

## Symptom

In item 353's fix's view run (2026-11-02, M23's exit's rounds on `0de7f3b5`,
[Resolved #353](capture-allocations.md)), round two's preload of 4,004,716 rows into a cluster
bootstrapped fresh sent every row and failed 21 inserts, and the run stopped:

```text
failed: the preload failed 21 inserts: {"Shedding": 21}
m23-lab: the capture m23-r2-load failed
```

Each of the 21 was refused `Shedding` on all nine of its sends, over about two seconds. The run's
exit trap put the hosts back. No node logged a warning between the bootstrap's elections and the
teardown, so what shed them is not on the record, and the capture kept no preload: the bench bails
before it records one. Every other preload of the session finished, ten of them on two builds, in
174 to 294 s.

## Cause

**Two things, one in the bench and one in the engine.**

**The preload gave up on a refusal it had every reason to wait out.** A shed write was turned away
before anything of it was applied, so sending it again is always safe
([Resolved #15](backlog-bounds.md)), and under a load the cluster cannot keep up with it is the
answer a client should expect. A group's leader lets a bounded number of its writes into openraft
and sheds one whose turn does not come within a quarter of its budget, 1.25 s at the default 5 s
([Resolved #129](overload-sheds.md)). That fix's own lab table says what a load needs from its
client: the tmdb loader's loads finished under the gate with retries unbounded
(`--retries 1000000`). A preload outruns the cluster by design: eight workers keep up to 256
inserts each in flight. But the bench's driver sent a retriable failure again `LOAD_RETRIES` (eight)
times, waiting 20 ms doubling to 500 ms, about 2.1 s in all
(`shoal-loadgen/src/driver.rs`). A run of sheds longer than that failed the preload, and with it the
run.

That run of sheds was not a rare event in kind, only in length. Every lab preload since M23's exit
sent hundreds or thousands of inserts again:

| Preloads | Inserts each sent again | Failed |
| --- | --- | --- |
| 50 of 4,004,716, 3,100,000 or 1,304,716 rows (`target/lab/m23*/captures/*-load`) | 213 to 5,047 | 1, the view run's round two |

**Nothing said which bound shed them.** A write is shed in six places, each before anything of it is
applied:

| Where | Cause | Counted before the fix |
| --- | --- | --- |
| A group's leader, `propose_through` in `server/shard/groups.rs` | Its gate gave the write no place in time (Resolved #129), its own or one hopped to it | By the gate (`ProposalGate::shed`), which no report read |
| The proposing shard, `propose_write` | The group's proposed and unanswered bytes past `replication.pending_bytes` | Lumped into `rejected` with every other refusal |
| The proposing shard, `propose_write` | The volatile logs past `replication.volatile_log_bytes` | The same |
| The proposing shard, `propose_write` | The write waited until its group forgot identities minted after its own ([Resolved #180](first-write-past-identity-memory.md)) | The same |
| The coordinating shard, `flush_forwards` in `server/shard.rs` | The queue of a data lane to the owning node was full | As frames, on the link's transport view |
| The read relay, `open_stream` | The shard's budget for assembling streamed bundles | No |
| A standalone table, `tables/persistent/{unsorted,sorted}.rs` | Its pending writes or its parked queries at their bound | No |

None of them logged anything. Each answer carries a message naming its bound, but the bench's failure
line printed only the count by code (`summary.insert.errors`), never the first message of each
(`samples`). So nothing on any host could say which of these refused the 21 for two seconds.

Which one did is not knowable after the fact. Whether it was the allocator's purge delay of 100 ms,
the one change on that build beside a capture's timing, is not shown either: ten of the eleven
preloads at 100 ms finished, and every one of the 39 at 10 ms or with no purge did. The lab's run of
this fix (Evidence) reads what sheds the same preload now that every node counts it.

## Evidence

**Established from the run's log and capture, then reproduced by a test.** The log is
`target/lab/m23-i353f-view/run/m23.log`, the capture `target/lab/m23-i353f-view/captures/m23-r2-load`
(its `error` the line above, no `preload`). The corpus of preloads is every `*-load` capture under
`target/lab/m23*`, read for its preload's `insert.retried`.

`a_preload_waits_out_a_shed_longer_than_its_retries` (`examples/bench_dataset/tests/preload_sheds.rs`)
starts a node in process, has it shed every write of the catalog's items for three seconds through the
path a table at its pending bound takes (`faults::shed_writes`, new), and preloads the committed
dataset through the driver. Against the tree before the fix, with the fault and the test alone:

```text
thread 'a_preload_waits_out_a_shed_longer_than_its_retries' panicked at examples/bench_dataset/tests/preload_sheds.rs:99:5:
assertion `left == right` failed: the preload failed 1000 inserts: {"Shedding": 1000} ({"Shedding": "Item holds 65536 writes waiting to be made durable and this one was not committed behind them"}), after 8000 retries in 3.01050013s
  left: 1000
 right: 0
```

Every item was shed on all nine of its sends and failed. With the fix:

```text
the preload loaded 2000 rows in 4.011061656s, sent 9632 shed inserts again ({"Shedding": 9632}), and the node counted 9632 shed
test a_preload_waits_out_a_shed_longer_than_its_retries ... ok
```

And on a cluster, `an_overloaded_group_sheds_rather_than_timing_out` floods one slowed group through
its leader and through a follower, whose writes hop to the leader. The leader's own figures, read
through `STATS`, count every shed its clients saw, all of them under its gate:

```text
through the leader: {"Shedding": 5455, "ok": 2545}; through a follower: {"Shedding": 8000}
the leader counted {"assembly":0,"forgotten_identity":0,"forward_queue":0,"gate":13455,"parked":0,"pending_bytes":0,"pending_writes":0,"volatile_log":0} of the 13455 writes its clients saw shed
```

**On the lab**, the user chose a run of the rounds' own preload alone before this item's box turned
green: `LOAD_ONLY=1 m23-lab.sh start`, four preloads, each on the `m23` side cluster bootstrapped
fresh, reading every member's write sheds by cause and the preload's retries by code
(`m23-preload-report.py`). Its figures are recorded with the box's change.

## The fix

**A preload and a read back wait out a shed.** A query refused `Shedding` is sent again past its
`LOAD_RETRIES` while it is within `SHED_PATIENCE`, a minute, of its first send (`sends_again`). A
minute is the driver's own judgement of a stream that has stopped answering (`HUNG_AFTER`): a node
that sheds one write for that long is stuck, not loaded. A query's first send travels with it through
every retry (`Staged::first`, `Sent::first`, `Retry::first`). Every other retriable code keeps its
count, and an arm keeps its own (`spec.retries`), with no patience at all: an arm's sheds are what
it measures.

**What sent a query again is counted by code.** A window counts its retries by the failure's code
(`retried_by`, beside `retried`), its summary and a capture carry them, and a run's line prints them.
A preload that fails says the first message of each code, its retries by code, and every member's
write sheds.

**Every node counts the writes it sheds by what shed them, and warns of them.** Each of the places
above calls `sheds::note` (`server/sheds.rs`) where it decides, with its cause (`WriteShedCause`)
and the message its client is told:

- **The count is the shard's**, kept on its executor's own thread: a shard is one executor, and the
  tables and the tasks it spawns to propose a write run on it. It is read into its replication report
  (`ShardReplication::write_sheds`), summed into the node's figures (`NodeStats::write_sheds`, left
  out of the frame while nothing was shed), and read on a standalone node through
  `ReadVerb::WriteSheds`.
- **The gate's is counted in `propose_through`**, so a write hopped to its leader and shed there is
  counted once, on the leader. A forward's is counted only for an entry that is a write, one with no
  read's plan; a table's parked bound only for an update, a delete or a conditional write. A streamed
  bundle refused at assembly is counted once, whatever it held, since it was refused before it was
  read.
- **The first shed of a cause in ten seconds is a WARN**, naming its cause and message and how many
  of that cause were shed since the last warning; the rest are DEBUG. One throttle a cause a node,
  F108's `Throttle` (`objects/inflight.rs`), reused.
- **A capture keeps them.** A server sample carries each member's write sheds
  (`ServerSample::write_sheds`), and a capture keeps every member's as they stood once its preload
  ended or failed (`Capture::preload_sheds`): a cluster the bench bootstrapped started with the
  preload, so they are the preload's.

A test may shed a table's writes on a standalone node for a while (`faults::shed_writes`), through the
path its pending bound takes, so the count and the warning are the real ones.

Three comments the code contradicted were corrected on the way: the gate waits a quarter of a write's
budget for a place, not half (`propose_through`); `ProposalOutcome::Shed` has four causes, not two; and
a write under `replication.append_reserve` is refused `NotLeader`, not `Shedding`, as
[Resolved #156](wal-failure-stops-the-node.md) says (`conf/cluster.rs`).

**What it costs a write that is not shed:** one relaxed load on a standalone table's write, the
fault's check, as every fault on a hot path costs; nothing on a cluster node. A shed pays a
thread-local add and one throttle's check. No before and after was taken on the lab for that load.

## Alternatives rejected

- **More retries.** A count is a length of time only through the backoff, and how long a cluster
  sheds is the cluster's business. A patience in time says what it means.
- **A shed sent again for ever.** A node that sheds one write for a minute is stuck, and a preload
  that waits on it for ever hangs a run nobody is watching. The minute is the driver's judgement of
  a hung stream already.
- **A patience for every retriable code.** `NotLeader`, `Unavailable` and an unknown outcome name an
  election, a fault or a commit not seen. A preload on a healthy cluster meets them in its first
  seconds ([F66](../../features/dataset-benchmarks.md)'s first event run), and a minute of them is a
  fault the run should stop on. A shed is the cluster's answer to load, which a preload brings.
- **A patience for an arm.** An arm measures what the cluster does under its load, sheds included.
- **The cause carried in `ProposalOutcome::Shed`.** It is postcard on a hop's answer, so a new field
  is a new frame between builds. Counting where the shed is decided needs no frame, and counts a hop
  once.
- **A handle passed to every site.** The tables are the schema's, built through `ShoalDatabase`, and
  `propose_through` takes twelve arguments already. A shard is its executor's thread, and the count
  is that thread's.
- **Item 319's run kept by its owner** ([Resolved #319](redial-warnings.md)): a link knows when its
  run ends, since it connects again, and a shed has no such moment. A throttle a cause lets the first
  shed after a quiet ten seconds through, which is the start of a run, and counts the rest into the
  next.
- **Every shed at WARN.** The overload row alone sheds 13,455 writes in four seconds.
- **The read admission bound's sheds counted here.** They are reads, counted on the transport view
  already ([Resolved #15](shard-mesh-admission.md)).
- **A preload under each purge delay before any fix**, as this item's direction first said. The
  corpus above shows nothing to tell them apart, and the fix holds whatever shed the writes; the
  counts name it on every run from now on.

## Invariants to uphold

- **A shed is counted where it is decided, once**: the gate's in `propose_through`, never in
  `answer_proposal`, which also answers a hop's shed decoded from its leader's reply.
- **`sheds::note` runs on the shard's own executor thread.** The count is that thread's, and a site
  moved to another thread, an object executor or the control thread, counts into one no report reads.
- **Only writes are counted.** A forward entry carrying a read's plan, and a get's or an exists' shed
  at the parked bound, are reads.
- **`NodeStats::write_sheds` and every capture field are serde default and skipped when empty**, so a
  build before the fix and one after read each other's frames and captures.
- **A shed is sent again past its retries only within its patience of its first send**, and the first
  send's time travels with it through every retry. An arm's patience stays none.

## Still open

- **The tmdb loader** (`examples/tmdb_dataset/src/load.rs`) sends a retriable failure again
  `--retries` times, eight by default, the same two seconds. Its lab loads pass a million; it was left
  as it is, its own tool with its own flag.
- **The gate's bound** is not on the replication report yet, [Resolved #129](overload-sheds.md)'s
  remaining half.
- **A streamed bundle refused at assembly** is counted once, as a write, whatever it held.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `a_preload_waits_out_a_shed_longer_than_its_retries` (`examples/bench_dataset/tests/preload_sheds.rs`) | A node that sheds a table's writes for three seconds fails every row the preload sent in them; nor does the node count them, nor the preload say what sent them again |
| `a_shed_is_sent_again_within_its_patience` (`shoal-loadgen/src/driver.rs`) | A shed past its retries is not sent again, or another code is, or an arm's is |
| `windows_add_and_summarize` (`shoal-loadgen/src/window.rs`) | A window's retries are not kept by code, summed or printed |
| `a_samples_write_sheds_round_trip` (`shoal-loadgen/src/results.rs`) | A server sample drops a member's write sheds, or writes the field with none |
| `write_sheds_are_counted_by_cause` (`shoal-proto/src/shared/protocol/stats.rs`) | A shed is counted under the wrong cause, shards' figures do not add, a node's figures from an older build do not read, or a node that shed nothing writes the field |
| `a_nodes_write_sheds_are_its_shards` (`shoal-core/src/server/control/stats.rs`) | A node's figures do not sum its shards' sheds |
| `a_shed_run_is_warned_of_once_then_every_interval` (`shoal-core/src/server/sheds.rs`) | Every shed is a WARN, or none is, or a cause's next warning does not say how many were shed since |
| `a_shards_count_is_its_own_threads` (the same file) | Another shard's sheds are counted in this one's |
| `a_preloads_sheds_name_each_member_and_cause` (`shoaladm/src/bench/orchestrate.rs`) | A failed preload's line does not name each member's causes |
| `an_overloaded_group_sheds_rather_than_timing_out` (`shoal/tests/cluster_fixture.rs`) | The leader's figures do not count every write its gate shed, its own and those hopped to it |

## Related

- [Resolved #129](overload-sheds.md), the gate whose sheds a load has to wait out, and whose shed
  count this puts on the report.
- [Resolved #15](backlog-bounds.md), why a shed is safe to send again.
- [Resolved #319](redial-warnings.md), a warning a run; [F108](../../features/object-work-in-flight.md),
  the throttle reused.
- [F66](../../features/dataset-benchmarks.md), the bench whose preload this is.
- [Resolved #353](capture-allocations.md), the run that met it.
