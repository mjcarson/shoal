# F117. A scan class on every table shard

Every table shard now has a **scan queue**, below its high and medium queues: 100 shares and a
100 µs latency goal, the object store's background queue's shape. A scan's page reads there, in
**steps** of 64 KiB of work that offer the core up between them, so a get's partition read, which
runs in the medium queue, never queues behind a scan's. And the shard's loop has a **second inbox**:
steps a background read takes on the loop itself, run only while no message waits in the main
inbox, or one ahead of a waiting message once a bound is due, so a shard that is never idle cannot
starve a scan for ever. Its first use is a scan page's capture: admitted on the loop as before,
then captured from the second inbox, where its copy is checked again and everything the page
reports is read. A driver's page, an operator's and every other kind of work run exactly as they
did.

## Context

This is [W1](../future-work/scans/prerequisites.md#a-scan-class-on-every-table-shard)'s scan class
row, one of the four the scans plan's M23 begins with, built before M23 for scan pages alone by the
user's decision of 2026-10-29.

A table shard had two queues of its own: high, 1,000 shares and a 500 µs goal, for listeners and
connections; medium, 500 shares and a 100 ms goal, for the compactor, the loader, the sweeper,
**every get's partition read**, and a walk page's reads. The loop runs in glommio's default queue,
1,000 shares and no goal. Nothing in table code yielded inside a computation, and glommio gives a
queue's core up at a yield only once a preemption is due, which only a queue with a goal makes
come ([S13](../object-storage/isolation.md#shared-executors-or-dedicated-ones)). So a scan built on
the walk as it stood would compete as an equal with the reads a get waits on, and its merge would
hold the core for a page at a time. [X9](../object-storage/table-latency.md) measured what holds a
table's tail: steps of 64 KiB under a 100 µs goal kept the reference cell's p99 under its line at
100 MiB/s, and a step of a whole mebibyte moved it 2.0 to 3.8 times.

And a page's capture, synchronous on the loop, ran as soon as its message did, ahead of every
message queued behind it: a capture of a large resident keyword partition held the loop for 4.7 ms
on the lab ([F87](tablet-walk.md#limitations)).

## What it does

### The queue

`Shard::new` (`shoal-core/src/server/shard.rs`) makes `ScanPriority:{shard}`,
`Shares::Static(100)` and `Latency::Matters(100 µs)`, beside the other two. When the shard is busy
the queue gets about a twenty-sixth of its core; when it is idle, all of it. A scan page's reads,
`PendingWalk::finish` (`shoal-core/src/server/tables/walk.rs`), are spawned there; a driver's stay
in the medium queue.

### Steps

`Steps` (`shoal-core/src/server/steps.rs`) counts the bytes a piece of work handled and calls
`glommio::yield_if_needed` once `STEP`, 64 KiB, is done since the last yield; every handling counts
at least 256 bytes, so a run of empty partitions ends a step too. `finish` takes one and counts every
partition it handles by the bytes it took, captured and read, never the rows it kept. A scan page's
are `Steps::yielding()`; a driver's are `Steps::unyielding()`, which count nothing, so a driver's
page is scheduled exactly as before.

### The second inbox

`IdleSteps` (`shoal-core/src/server/shard/idle.rs`) is a plain queue on the shard, since only the
loop fills or drains it. At the top of the loop, after the yield item 284 added, the loop runs the
queue's steps while no message waits in the main inbox:

- **The common case costs one length check a message**: the queue's own length is read before the
  main inbox's, whose `is_empty` takes the channel's lock.
- **A step runs ahead of a waiting message only once one is due**: no sooner than 10 ms after the
  last such step, nor than fifty times that step's hold, so forced steps take at most a fiftieth of
  a loop that is never idle. One forced step, then the message it ran ahead of.
- **A shard that is stopping refuses what waits** (`Unavailable`), never capturing a page.

### A scan page captured from it

A scan page is admitted on the loop as every page is - its table, bounds, host, copy and class's
count checked, so it is refused early by name - and then queued in the second inbox, counted
against the scan class from that moment. Its capture step checks the tablet is still hosted here
and the copy again (`walk_copy`: starting, down, installing, quarantined, behind `at_least`), and
only then captures, with nothing awaited between the copy check, the resident capture and the
archive's view. The page's group, installs, boundary and leadership are all read at the capture, so
a walk raising its floor to the page's boundary never steps back past what the copy held when it
was read, and the end check against an install begun under the page holds as it did. The answer
says how long the page waited for its capture (`WalkAnswer::queued_us`, left out at zero, which a
driver's page always is); the bench keeps it by member ([F115](bench-scans.md)).

## Design choices

**A queue of its own, never the medium one.** The medium queue carries every get's partition read;
a scan there would be an equal of the reads a get waits on.

**A 100 µs goal, the object background queue's.** It is the shape X9 measured. A goal is what
makes a yield real, and it is also what has the executor poll its I/O often while a scan is
runnable: disk completions are noticed only when a preemption is due (glommio's `run_task_queues`),
since disk reads go on the poll or the main ring and only a socket's receive goes on the latency
ring. A queue with no goal and an unconditional yield would keep other queues fed but could leave a
get's completed read unnoticed for up to 100 ms behind a busy step. The cost is named below.

**Steps by bytes handled.** A step's time is its work's, and its work is the bytes it read and
merged; a filter that keeps little still yields as often as one that keeps much, which is W5's rule.

**The second inbox is a queue on the shard, not a channel.** Only the loop fills it, so nothing has
to wake it, and no receive is raced against another (`shoal-channel/tests/no_raced_receives.rs`):
the loop takes a step at its top, before its one receive.

**A bound on starving a scan, scaled by its steps' holds.** W5 runs a page's steps only when the
loop has nothing else to do; a loop that always has something would starve a scan for good. A fixed
10 ms bound alone would let a 5 ms capture take half of a busy loop, so the bound scales with the
last forced step's hold.

**The capture checked again where it runs.** The copy can begin an install, be quarantined, or move
off the executor between admission and capture; checking at admission alone would capture from a
copy the page no longer may read, and reporting admission's boundary would let a floor step back.

**Drivers untouched.** By the user's decision of 2026-10-29: the drivers move onto the rebuilt walk
at M23, with their own figures measured then.

## Alternatives rejected

**Moving a scan page's reads off the shard.** The rows belong to the shard; another executor would
need its files and resident partitions, or their bytes hopped across
([W7](../future-work/scans/isolation.md#alternatives-rejected)).

**A second channel raced against the main inbox.** It would need a kept receiver on every message's
path, and a wake the loop has no need of: everything that queues a step runs on the loop already.

**Capturing a scan page at admission, as a driver's is.** Its capture would run ahead of every
message queued behind it, which is the hold this row removes for a scan.

**Splitting the capture into steps now.** Encoding resident partitions 64 KiB at a time, each step
with a fresh lookup, is W5's step 4 and M23's, with its three facts and their tests; done here it
would rebuild the walk ahead of X16's baseline and X17's measurements.

## Limitations

- **The capture is still whole.** A scan page's capture runs as one step from the second inbox: it
  waits for an idle loop, but once it runs it holds the loop as the walk's capture always did, up
  to milliseconds for a large resident partition. M23's plan and encode steps are what bound it.
- **A forced step can run ahead of a waiting message**, at most a fiftieth of a busy loop, by design.
- **A runnable scan task shortens every queue's slice.** While the scan queue has a task, glommio's
  preempt timer is its 100 µs where the high queue's 500 µs set it before, so every queue is
  preempted, and the executor polls its I/O, five times as often. X16's side with this row measures
  it beside a scan; X17 owns the switch between this, a longer goal and none.
- **A step's first stretch runs under the timer its last poll armed.** glommio arms the preempt
  timer at an I/O poll for the queues active then, so work that starts just after a poll made while
  no queue with a goal was active runs under that poll's timer, 100 ms, until it fires once. A scan's
  reads park on I/O often enough that this is a page's first stretch at most, and a socket's receive
  preempts it regardless; `a_scan_step_yields_to_the_foreground` measures past it.
- **Only a scan's page uses any of this.** Compaction, a backup's copy and the drivers' pages run as
  they did ([W1](../future-work/scans/prerequisites.md#a-scan-class-on-every-table-shard) lists them
  as what else gains).
- **Index pages a page's view reads are read before its steps**, in the read task, unstepped.

## Invariants to uphold

- **A scan's reads never run in the medium queue**, which carries every get's partition read.
- **Every partition a scan page handles is counted into its steps by the bytes it took**, and a step
  is never more than 64 KiB of work between two offers of the core.
- **Only the loop fills or drains the second inbox, and a step never awaits**; it spawns with
  `spawn_local_into`, which only schedules (item 242).
- **A scan page's copy, group, installs, boundary and leadership are read at its capture**, with
  nothing awaited between them and the capture, never carried from its admission.
- **A scan page waiting for its capture counts against the scan class**, so the queue is bounded by
  `scans.pages_in_flight` a shard.
- **An empty second inbox costs one length check a message.**

## Performance

On the query path: one length check of an empty queue at the top of every loop iteration. No
other path a query takes changed. Beside a scan, the scan queue's share of a busy core, the 100 µs
preempt timer while it is runnable, and a capture that waits for an idle loop: what those cost and
buy the foreground is measured on the lab beside the walk as it stood, with the other rows, in X16's
window ([the plan's lab window](../future-work/scans/spikes.md#x16-the-walk-beside-the-foreground)).
No figure is claimed here before it.

## Tests

| Test | Proves |
| --- | --- |
| `a_scan_step_yields_to_the_foreground` (`shoal-core/src/server/steps.rs`) | Work in steps in a queue of 100 shares at a 100 µs goal leaves a foreground task waiting for a step, never for the work left |
| `a_step_is_offered_up_every_64_kib` (same) | A step offers its core up every 64 KiB and every few hundred empty handlings; a driver's steps never do |
| `an_idle_step_runs_ahead_of_a_queued_message_only_when_due` (`shoal-core/src/server/shard/idle.rs`) | A forced step waits 10 ms and fifty holds after the last; an unforced step moves nothing |
| `queued_captures_drain_with_no_other_message` (`shoal/tests/scan_admission.rs`) | Eight scan pages queued together in a shard's second inbox are all captured with no other message to wake the loop |
| `a_scan_page_reports_the_boundary_of_its_capture` (`shoal/tests/cluster_fixture.rs`) | A scan page held in node zero's second inbox while writes land reports a boundary at or past the copy's applied index when it was let go, hands out the rows written while it waited, and says it waited |
| `every_kind_of_table_walks_in_key_order_as_a_scan` and four more (`shoal/tests/tablet_walk.rs`) | Every in-process walk test of F87, again with every page asked as a scan's: the contract kept with the capture deferred and the reads stepped |

The fixture's `HOLD_IDLE on|off|status` holds every shard's second inbox in a child and counts the
scan pages waiting for their capture (`faults::hold_idle_steps`, `faults::captures_waiting`).

## Related

[W7](../future-work/scans/isolation.md#cpu) and [W5](../future-work/scans/tablet-reader.md) for the
design; [X9](../object-storage/table-latency.md) for the evidence; [F116](scan-admission.md) for the
class a page is asked in; [F87](tablet-walk.md) for the walk.
