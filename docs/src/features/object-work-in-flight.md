# F108. Object work in flight, in a node's figures

## Context

[Item 305](../appendix/known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects)
was a cluster that took no put for minutes and said nothing:
- A capture's 1 MiB puts across hosts stopped completing.
- Its read back could not read ten objects.
- No node logged anything at WARN until the cluster was torn down.

Nothing a deployed node exposed could have said where the puts waited. A node's figures counted the
operations it answered, not the ones it was holding:
- **The object executors' budgets** were readable only by tests (`ObjectStore::budgets`).
- **An object lane's requests in flight and its link's state** were never shown.
- **The answers a served lane owed, and a shard's running object tasks,** were never counted for
  anyone to read.
- **The fixture's `OBJECTS` verb** read the lanes' figures through each executor's mailbox. On a
  wedged executor, the case that matters, it would have hung.
- **The refusals that end such a wait** said nothing: a lane refusing a request after its stall
  bound, a budget shedding a draw, a put given up, and a served lane at its bound of answers owed.

This is item 305's means, built first in wave three of the store optimizations so that lab 3b's run
of the build the stall was seen on can read it
([the plan](../object-storage/store-optimizations.md#wave-three)).

## What it does

**A node's figures carry its object work** (`NodeStats::objects`, an `ObjectWork`). A figure marked
"now" is read when the node reports; every other counts from its start.

| Figure | What it counts |
| --- | --- |
| `budget_used`, `budget_limit`, `budget_shed` | Bytes drawn from the object executors' budgets now, what they may hold, and the draws they shed |
| `links` | Object lanes this node dialled that are up now |
| `waiting` | Requests sent on those lanes and not answered yet, now |
| `sent`, `refused` | Requests sent on them, and refused by a lane: no bytes written for its stall bound, past its frame, or since [Resolved #316](../appendix/resolved/unanswered-read-bound.md) a reader's read with no answer within its bound |
| `served_lanes`, `owed`, `served` | Lanes served for other nodes now, the answers they owe now, and the requests they took |
| `foreground`, `staged_bytes`, `recovery`, `scrubs`, `applying` | Each slice's work in flight now, summed |
| `tasks`, `tasks_refused`, `puts_given_up` | Object operations the shards run now, refuse at their bound, and give up after a stripe failed |

The figures travel this way:
- **Each object executor shares an `InFlight` with the store's handle** (`server/objects/inflight.rs`).
  It is atomics its lanes count into: a link up, a request waiting, an answer owed, a lane served.
- **`ObjectStore::work()` sums them** with the budgets' atomics and every slice's.
- **Every shard's report reads that sum** (`ShardReplication::objects`) and adds its own tasks, the
  ones it refused, and the puts it gave up (`MetaClient::add_to`).
- **The control thread's tick** takes the store's figures from the freshest shard's copy, the one with
  the most requests sent and served, and sums the shards' own.

They are readable four ways:
- **`shoalctl`'s and `shoaladm cluster stats`' pools tab:** fourteen metrics, from "object tasks" to
  "applies", each with its help.
- **A capture:** every server sample keeps each member's `objects`, and
  `stores-ab-report.py --objects` prints the most each run held and what it refused, unmeasured runs
  included.
- **The fixture's `OBJECTS` verb:** it answers `work` straight from the atomics, and waits ten seconds
  at most for the part it asks the executors' mailboxes, naming an executor that did not answer.
- **WARN lines, each at most once every ten seconds with the count it held back:**
  - a lane that refused a request after writing nothing for its stall bound;
  - since [Resolved #316](../appendix/resolved/unanswered-read-bound.md), a lane that refused a reader's read with no answer within its bound,
    with what the link and its socket saw;
  - a budget that shed a draw;
  - a shard that refused an object operation at its bound;
  - a put given up;
  - a served lane that owes as many answers as it reads ahead of.

## Design choices

- **Atomics beside the thread-local figures, not in place of them.** The lanes' own figures
  (`LaneFigures`) stay where the fixture's rows already read them. `InFlight` is the copy any thread
  reads.
- **Every gauge is a guard** (`Held`), counted down by its drop:
  - a request's waiter carries one (`Pending`), so an answer, a refusal, a drain when its link drops,
    and the link's own drop all count it down;
  - an owed answer carries one from the moment its request is read until it is written;
  - a link holds one from its first `Up` until its `Down` or its drop.
- **The shards' reports carry the store's figures, as F104 carried its cache.** The control thread
  starts before the store does and holds no handle to it. Each shard already reads the store's
  atomics for its report, and a node under object load already reports every tick, because F104's
  cache figures move with every get.
- **The freshest copy is the one with the most requests sent and served.** Those only grow, and when
  none moved every copy is alike.
- **The pools tab, not a tab of its own.** It is already the objects' tab, and a ninth would move
  every tab after it.
- **A warning a kind, throttled where it is said** (`Throttle`, a static each). A stalled holder
  refuses every request sent to it, and one line every ten seconds with the count of the rest keeps
  the log readable and the count whole.

## Alternatives rejected

- **The executors' mailboxes**, as `OBJECTS` asked them. A wedged executor answers nothing, and that
  is the one whose figures matter.
- **A handle to the store in the control thread.** It would read the gauges at the tick itself. But
  the store starts after the control plane, so the handle would have to be handed over later, and
  the shards already read the same atomics for their reports.
- **Each link's queued bytes.** They live in the peer link's own state, which the object lane shares
  with every other lane. `waiting` and `refused` say what a full queue does: requests wait, then are
  refused.
- **Logging every refusal.** A stalled holder refuses every request after its first, and at a
  millisecond a request that floods the log a hundred lines a second.

## Limitations

- **A figure is as old as the last report of its shard,** and a node's figures reach the leader one
  report in four, about every two seconds by default.
- **`links` counts a lane once its handshake completed.** A link that never came up is not counted,
  and its requests are refused, not left waiting.
- **One warning of each kind every ten seconds for the whole process.** A second lane stalling inside
  that window is counted in the next line, not named.
- **`staged_bytes` counts a slice's staged records:** writes in place waiting for their commits. A
  put's chunks are written whole and are never staged.
- **The fixture's `OBJECTS` leaves a thread behind** when an executor does not answer. It is a test
  verb, so it does.

## Invariants to uphold

- **Nothing that reads these waits on an executor.** A figure is an atomic or a shard's own cell,
  never a mailbox's answer.
- **A gauge counted up is counted down on every way its work ends.** It is held by a guard, never by
  a pair of calls a cancelled task or an early return could split.
- **A new figure of `ObjectWork` gets a metric or a reason it has none, and a value in shoaladm's
  `every_figure`.** `ObjectWork` derives `Default` and is read with `#[serde(default)]`, so a build
  before it reads none.
- **A warning is throttled in a static of its own kind**, never per call site instance, so the count
  it holds back is the whole of them.

## Performance

The added work on an object request's path:
- a relaxed atomic increment when it is sent and when it is taken;
- an increment and a decrement for the guard of the request waiting and of the answer owed;
- an increment and a decrement of a served lane's guard, once a lane;
- the throttles' check, on the refusal paths alone.

Every capture of lab 3b runs with it on both sides, so F109's to F111's comparisons are judged with
F108 in place. Its own cost was not taken apart: a handful of relaxed atomics a request is below
what a capture resolves.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-proto` `object_work_from_a_build_before_it_reads_as_none` | A node's figures from a build before F108 do not decode, or decode as work in flight, or a node with none writes the field |
| `shoal-core` `objects::inflight::tests::a_gauge_is_held_until_its_guard_drops` | A gauge held is not counted, is not counted down by its guard's drop, or does not reach a node's figures |
| `shoal-core` `objects::inflight::tests::a_throttle_counts_what_it_holds_back` | A warning too soon after the last is logged, or the count held back is lost |
| `object_work_in_flight_is_read_while_held` (fixture) | A write in place held before its commit is not counted as a running task in the members' figures, or its staged units on its holders; requests to a stalled holder are not counted as waiting or refused; or a gauge stays up once nothing is held |
| `shoaladm` `every_metric_reaches_the_view` | A metric of the pools tab names a figure the view never reads |
| `shoal-loadgen` `a_samples_object_work_round_trips` | A capture's server sample loses a member's object work, or a sample from before F108 does not read back |

## Related

- [Item 305](../appendix/known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
  whose stall this makes readable.
- [F104](memory-in-a-capture.md), whose path through the shards' reports this takes.
- [F86](replicated-pools.md), the object lane, and [Resolved #238](../appendix/resolved/object-lane-backpressure.md),
  its stall bound.
- [F85](devices-and-pools.md), the budgets the object executors draw from.
