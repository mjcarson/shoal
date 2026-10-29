# W26. The lane's model

## Context

The writer lane is a replication protocol of its own: no consensus a write, a writer that orders
everything, three bars against the one before it, marks that excuse a silent position, epochs a
holder learns, a merge by labels at every copy. Each of those rules was found necessary by reading
([W17](writer-lane.md) to [W20](placement-under-a-writer.md)); none has yet been held to every
interleaving it allows. The object store's stripe protocol was in the same place before M11, and X1
built it as a model first, found ten rules the pages stated that did not hold, and repaired each
before a line of the store was written ([X1](../../object-storage/stripe-model.md)). The decision of
2026-10-26 does the same for the lane. This page is what that model must contain and check, so that
[X23](spikes.md#x23-the-lanes-model) is a spike with its answer stated in advance.

## What exists today

- **`shoal-model`**, the deterministic protocol model ([F36](../../features/cluster-harness.md)): a
  Raft-shaped tablet group held to P1–P6, and since X1 a second model beside it, `src/stripe/`, held to
  P7–P13 and P15–P17, its schedules under `shoal-model/schedules/stripe/`, kept as M11's acceptance
  tests. It depends on `serde` and `serde_json` alone and names no shoal crate, "since a schedule that
  needs the engine to replay is worth nothing".
- **Every saved file stays valid**: a new field anywhere in a saved schedule is
  `#[serde(default, skip_serializing_if = ...)]` at today's behaviour, and the generator draws nothing
  new for an old configuration, so every block digest in X1's records stays the same.
- **The search** runs seeds at volume on every thread and writes one record a host; `report` compares
  records block by block of 250 seeds (`stripe_search`).

## The design

### Where it lives

`shoal-model/src/volume/`, beside `src/stripe/`, with its schedules under
`shoal-model/schedules/volume/`, an example `regenerate_volume_schedules` and a search
`volume_search`, all under the crate's rules: `serde` and `serde_json` alone, nothing of the engine.

### What it models

| Actor | State | What it does |
| --- | --- | --- |
| The volume row | A register committed by conditional writes: writer number, attachment, tract bitmap, snapshots | Attach, takeover, tract marks and clears, snapshots, all conditioned |
| The stripe table's groups | Each a linearizable state: positions, epoch, move, marks, a bar a volume | Marks, bars, moves, rebuild commits, each conditioned |
| Holders | Each a slice: chunks of block labels, a journal of records durable or written, a bar a volume, installed epochs, marks it was told | Answer writes, reads, listings, raises and installs; crash and restart from what was durable |
| Writers | A writer number, a write count, writes in flight and unanswered, a newest boundary | Write, flush, read, mark, clear, snapshot, detach, take over |
| The move and rebuild drivers | A group's leader's | Install epochs, copy by labels, switch, abort, rebuild and commit |
| The network | Messages | Delay, drop, reorder, duplicate, and partition any set of actors |

**Bytes are not modelled**: a block's content is its label, so a read answers a label and every check
is about which write a block holds.

### What it checks

Each of [W27](contract.md#the-contract)'s properties, P27 to P34, as an oracle over a history:

| Property | The oracle |
| --- | --- |
| P27 | No write of a writer below the row's writer number is accepted at a current position of a stripe after the new writer first touched it, and none anywhere after the takeover completes |
| P28 | Every acknowledged write is held by every current position or by `k + f` with every other marked, and survives every crash within the redundancy |
| P29 | No read returns a block older than a write the reader's writer had acknowledged or read |
| P30 | Every quiet stripe's current positions hold the same labels |
| P31 | After a takeover, every block's label is at least every write the old writer had acknowledged |
| P32 | Every write acknowledged during a move is on every arrival after the switch, or marked there |
| P33 | A complete snapshot holds, for every block, the newest write at or below its boundary and nothing above, and never changes |
| P34 | A discarded block reads zeros and a reallocated stripe never shows its former blocks |

A progress check beside them: with the network healed and no crash for a bound, every write is
acknowledged, every takeover completes and every marked position is rebuilt.

### The settings

**The safe policy** is this subchapter's rules as written. **Every rule the review that shaped them
found necessary is also a setting that turns it off**, each with a saved schedule that breaks a
property when it is off, as X1 kept each of its ten:

| Unsafe setting | The rule it turns off | The property it should break |
| --- | --- | --- |
| `fence_cluster_wide` | The three bars, replaced by one fence through the control state that live members apply | P27, with a member partitioned on the old writer's side |
| `no_group_bar` | The groups' bar | P31: a late mark from the old writer names the new writer's position stale, and a rebuild copies old blocks over new |
| `no_holder_bar` | The holders' bar | P27 |
| `label_per_unit` | A label a block, replaced by one a 64 KiB unit kept by the maximum | P28: two block writes into one unit, delivered in opposite orders |
| `max_over_marked` | Marked positions excluded as sources | P31: an unacknowledged, superseded write brought back |
| `abandon_on_refusal` | Never abandoning a sent write | P29: reads that flip between two positions |
| `ack_without_mark` | The committed mark before an acknowledgement at `k + f` | P28 |
| `mark_a_stripe` | A mark a group and position | Progress: a commit nearly every write while a holder is down |
| `no_epoch_at_holders` | Epochs installed before the copier reads | P32: F88's `both_counts_the_old_holder_alone` |
| `backfill_trusts_rowless` | Backfill by listing | P32 |
| `mark_at_commit_for_reads` | A mark effective for reads once the holder is told | P29 |
| `snapshot_without_completion` | A snapshot's completion | P33: a late write below the boundary missing from one copy |
| `restart_resumes_count` | A restarted writer as a new writer number | P28: two payloads under one write number |

If a setting's schedule does not break its property, the rule it turns off is not needed and the page
that states it says so; if the safe policy breaks a property, the design changes before M29.

### The schedules that must exist

Beside each unsafe setting's: a writer killed with writes at every stage; a takeover racing the old
writer's writes, marks and tract clears; **a partition that leaves the old writer with every position
of a stripe** ([W18](attach-and-takeover.md#what-a-partition-does)); a holder lost between a write's
journal and its answer; a move's begin, switch and abort under writes; a rebuild racing a new mark; a
snapshot taken with writes in flight and a takeover before it completes; a stripe discarded while a
holder is down; and a write-back volume whose positions all restart between flushes.

### The search

`volume_search` runs the safe policy and every setting at three layouts, three copies on three nodes,
three copies on five, and two volumes sharing a pool, for at least as many seeds as X1's 1,200,000,
on europa and again on titan and hyperion from a `znver1` build, half of them seeds europa ran, and
`report` compares the records block by block, as X1's did.

## Alternatives rejected

**Building the lane and testing it in the fixture.** The fixture finds what a schedule it runs
happens to reach; the model reaches every interleaving its failure model allows, and the object
store's ten repairs were found there, not in the fixture.

**A model that carries bytes.** Every property is about which write a block holds; bytes would only
slow the search.

**A model in the engine's crates.** A schedule that needs the engine to replay is worth nothing once
the engine changes.

## What it costs

A week or two of the model ([X23](spikes.md#x23-the-lanes-model)); its schedules kept forever, as
acceptance tests, beside X1's.

## What it breaks

Nothing: X1's models and schedules are untouched, and every saved file still replays.

## Invariants to uphold

- **The model names no shoal crate** and depends on `serde` and `serde_json` alone.
- **Every rule of the lane that is a setting has a saved schedule** that breaks a property when it is
  off.
- **The model's schedules are acceptance tests from M29**, and a rule changed in the engine is
  changed in the model first.

## Prerequisites

None.

## How it would be measured

By X23 itself; its records go beside X1's.

## Acceptance tests

| Test | Gate | Proves |
| --- | --- | --- |
| `every_volume_schedule_replays_to_its_verdict` | M29 | Every saved schedule under `schedules/volume/` gives the verdict it was saved with |
| `the_safe_lane_policy_breaks_no_property` | M29 | The safe policy passes every saved schedule and a generated block of seeds |
| `every_lane_rule_is_needed` | M29 | Each unsafe setting's saved schedule breaks the property it names |
| `a_partitioned_old_writer_keeps_the_cut` | M29 | The partition schedule keeps P27 as a cut a stripe |

## Related

[X1](../../object-storage/stripe-model.md), the model this follows; [W27](contract.md) for the
properties; [X23](spikes.md#x23-the-lanes-model) for the spike that builds it.
