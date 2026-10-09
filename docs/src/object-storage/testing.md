# S16. Acceptance tests, the model and the fixture

## Context

The contract on [S18](contract.md#the-contract) is thirteen properties, and most of the ways
to violate them are interleavings: a stager paused across a leader change, a stage's answer
lost, a device swapped while its node was down. No number of tests written by hand covers
interleavings, and no process test can be trusted to reach a given one twice.

The Distributed chapter met that with two instruments used together: a pure model that
checks properties against schedules it can save and replay, and a fixture of real processes
with faults injected from outside. This page says what each has to gain. Both come before
any object byte is stored, as [M0](../distributed/milestones.md#m0-step-0-the-harness-and-the-facts)
came before any node could join another.

## What exists today

- **`shoal-model`** calls itself "A pure, deterministic model of Shoal's replication
  protocol": one Raft-shaped tablet group, stable storage kept apart from volatile state,
  driven by explicit events. Its policy has a safe setting, which is the contract, and unsafe
  settings that each reproduce one violation, so that the checks can be shown to fire
  (`shoal-model/src/lib.rs`, `src/policy.rs:98`). A schedule is a list saved as JSON,
  replayed and minimized; eight are saved under `shoal-model/schedules/`. It depends on
  serde alone and names no shoal crate, which is what lets it run while the engine does not
  build ([C11](../distributed/testing.md#the-protocol-model)).
- **It has no seam for a second protocol.** Its world drives its Raft node directly. A new
  protocol brings its own events, state machine, checker and oracle. The stripe model brought
  exactly that: since [X1](stripe-model.md)
  (2026-10-06) it lives in `shoal-model/src/stripe/` with its own world, events, checker, oracle,
  schedules and minimizer, sharing the seeded generator, the operation and node identifiers and
  the `Property` numbers, with twenty-six schedules saved under `schedules/stripe/`.
- **The fixture** runs real servers as children of one test binary, each allocated whole
  cores, with every link between them through a proxy that can be cut, delayed, throttled or
  blackholed (`shoal/tests/cluster/link.rs:102-144`), and children that can be paused,
  killed and restarted. A child takes verbs on its standard input.
- **Storage faults are few.** A crash point exits the process at a named line
  (`CrashPoint`, `shoal-core/src/server/replication/install.rs:217`). One hook fails a
  table's intent log write (`shoal-core/src/server/tables/storage.rs:900`). Three verbs
  damage an archive record (`CORRUPT`, `FORGET`, `ERASE`,
  `shoal/tests/cluster_fixture.rs:2362`). ~~There is no torn write, no full disk and no lost
  device, and [C15](../distributed/open-issues.md#filed-as-unbuilt) says so.~~ Since
  [F70](../features/storage-faults.md) a torn write, a full disk and a lost device are armed
  for a directory a test names (`FAULT_DIR`, `shoal::server::faults`), and a root-only test
  holds the full disk and the lost device to a real one behind device-mapper. An emptied device
  and a flipped bit are not built; a child still has one storage directory.
- **A kill proves nothing about durability.** "No storage durability claim is inferred from
  SIGKILL alone" ([M0](../distributed/milestones.md#m0-step-0-the-harness-and-the-facts)):
  the page cache survives a killed process.
- **The acceptance tables are checked.** `acceptance_tables_have_unique_tests_and_valid_milestones`
  holds every table of the Distributed chapter to its milestones page and every named test
  to a function in the workspace. It reads `docs/src/distributed/` and knows M0 to M10c
  (`shoal-bench/tests/acceptance_tables.rs`).

## The design

### The model

A second model in the same crate, held to the same rules: pure, seeded, serde alone, no
engine crate in its graph.

**What it models.** ~~One stripe, its row, its holders and its readers~~ One object: its
`ObjectMeta` entry and two or three of its stripes, each with its row, its holders and its
readers, with everything else abstracted to the property this part relies on. One stripe was
the first scope, and it cannot hold [Q18](contract.md#questions-to-answer): a truncate commits in
the object's entry and is checked by every stripe's commit, so the schedule that shaped it
([S7](write-path.md#the-schedules-that-shaped-it), schedule 4) needs the entry and at least two
stripes.

**Its layouts.** Replicated three ways (`k = 1`, two copies more), 2+1 and 4+2, each at `f = 1`,
with one stripe chunk a slice and the slices of a stripe on distinct devices. A saved schedule
names its layout, and a generated run draws one.

| Actor | State | Notes |
| --- | --- | --- |
| The object's entry | Size, the truncate epoch and its floors | An atomic object like a row. A truncate commits here, and a writer reads the epoch strongly before it stages ([S3](objects.md#size-holes-and-truncate)) |
| The row | Sequence, a label for each stripe chunk, truncate epoch, the placement group's generation and, since [X2](placement-simulation.md#positions), its positions, who missed what | An atomic object that applies conditional commits in one order. The tablet group is **not** modelled again: P1 to P6 are the contract it is held to, and a leader change appears here as a stager losing its view |
| A holder | A slice: a stripe chunk under a label; staged writes; what is durable and what is not | A crash loses what was not synced. Its device may be lost or replaced by an empty one, and every slice on it with it |
| A stager | What it read, what it staged, whether it has proposed | Several may act on one stripe |
| A reader | The row state it consulted, the chunks it was answered | At either read level |
| The pool map | Generations, the slices at each and the device each slice is on | Changes at any time |
| The reclaimer | Nothing of its own | Asks, and discards on what it is told |
| A rebuilder | The chunk it is rebuilding, what it asked its holder, the chunks it read | Rebuilds a stripe chunk the row says a slice missed: asks the holder what it holds, reads `k` current chunks if it must, writes, and commits the chunk current. Schedules 3 and 11 need it |

**Events** are what [S7](write-path.md#the-schedules-that-shaped-it)'s table is made of: a
stage sent, delivered, lost or duplicated; a holder's sync; a crash and a restart; a commit
proposed, applied or refused; an apply told or never told; a write torn; a device lost or
replaced, with every slice on it; a device filled, or given space back; the map changed; a
truncate; a rebuild asked, read, written or committed; a question about a row
answered by a replica that lags. Filling and the rebuild's events were added when the settings
below were held to S7's schedules one by one: schedule 13 cannot be written without the first,
nor 3 and 11 without the second.

**The policy** has a safe setting and one unsafe setting for each rule the design depends
on, each with a saved schedule that makes the checker fire:

| Unsafe setting | The clause it violates | [S7](write-path.md#the-schedules-that-shaped-it)'s schedules |
| --- | --- | --- |
| A sequence number as the label | P9, P10 | 1 |
| A commit with no condition | P8 | 2, 10 |
| A holder that discards on a timer | P16 | 6 |
| An acknowledgement after one stage | P11 | 7 |
| A parity staged as a patch | P15 | 8 |
| An apply before the commit | P9 | 15 |
| A reader that accepts a newer chunk | P10 | 16 |
| A discard on a lagging replica's view | P16 | 14 |
| A commit that ignores the generation | P8, P17 | 5 |
| A commit that ignores the truncate epoch | P13 | 4 |
| A returning slice taken as current, with no record of what it missed | P11, P17 | 3 |
| The staged copy dropped when its apply starts | P7, P9 | 9 |
| A device known by its path alone | P7, P17 | 12 |
| Space taken at the apply, not the stage | P7, P11 | 13 |

The last four, and schedules 15 and 16, were added on 2026-10-03, when each of S7's fourteen
schedules was looked for in this table and five were not there, and two settings had no
schedule. Every safety schedule of S7 now has a setting, and every setting a schedule.

Since [X1](stripe-model.md) each is one knob of `StripePolicy` (`shoal-model/src/stripe/policy.rs`),
named for what it does, and its schedule is saved as `schedules/stripe/sNN_*.json` with the
violation it records. Each fires a clause this table names for it.

**Ten rules the pages stated did not hold.** X1's search broke each under the safe policy,
and each was repaired by a local rule: none needs a primary, a vote among holders, or undo. Each
is a knob too, whose first variant is the repair and whose second is the rule as written, saved
with the schedule that breaks it, so a repair reverted is a test that fails:

| The rule as written | The clause it broke | The repair | Page |
| --- | --- | --- | --- |
| An untouched chunk counted on the row's word while its holder is believed up | P11 | Counted only on its holder's confirmation in the write's round | [S7](write-path.md#the-acknowledgement-rule) |
| The same, while its holder is down | P11 | The same | S7 |
| A stamp that moves to whatever its writer read | P13 | A stager does not commit on a row stamped past its epoch | [S3](objects.md#size-holes-and-truncate) |
| A write leaves the units a floor hides as they were | P13 | It writes them as zeros | S3 |
| A reclaimed row deleted | P17 | Left a tombstone a sequence past it | S3 |
| A reader hides by the entry it read | P12 | It reads the entry again when a row is stamped past it | [S9](read-path.md#which-state-a-read-returns) |
| A default read takes its row at `One` | P12 | At `Quorum`, after the entry | S9 |
| A tag from the request identity alone | P9 | A tag a try; the retry table recognises a retry | [S7](write-path.md#labels-not-numbers) |
| A truncate commits to the entry alone | P12 | It fences the stripe its cut falls inside first | S3 |
| A staged write discarded once the row moves past its base and names another label | P17 | Kept while a label the row names stands on it, until the chunk reaches that label | [S10](recovery.md#reclamation) |

**Q16's open point is a setting, run both ways.** Whether a chunk the write did not touch, on a
slice that is down, counts toward `k + f`. The safe policy is run with each answer. If counting
it lets the checker fire for P11, the schedule that shows it is saved and the rule is settled as
not counting it; if neither fires, Q16 is answered by cost. **Settled by X1, stricter than it
was asked**: both answers fired. Not counting it while its holder is down still counts it on the
row's word while its holder is believed up, and a disk can fail without a word. An untouched chunk
counts only when its holder confirms, in the write's round, that it holds the label, a third
answer ([X1](stripe-model.md#q16-both-ways)).

**A progress check beside the oracle.** The oracle checks safety, and two of
[X1](spikes.md#x1-the-stripe-protocol-as-a-model)'s expected results are about progress: a
reader of a stripe that is written continuously that cannot finish unless a holder keeps a
chunk's previous state, and stagers that starve each other without a reservation. So, for a
generated run whose faults stop at a step it names:

- every reader that began after that step finishes within a bound of steps;
- of several stagers on one stripe, one commits within a bound;
- a rebuild rewrites only a chunk its holder does not hold current.

A schedule that breaks one of these records the bound it exceeded, not a clause. One unsafe
setting is held to the progress check alone:

| Unsafe setting | What the progress check finds | S7's schedules |
| --- | --- | --- |
| A rebuild that does not ask the holder first | A current chunk rebuilt because the row called it stale | 11 |

Whether a holder keeps a chunk's previous state, and whether the leader reserves a stripe for
one stager, are settings of the safe policy that X1 runs both ways, and the progress check is
what tells the two apart. **X1 kept the previous state and left the reservation out**: without
the first, readers failed by name or ran past their bound as writers were added; without the
second, no stripe's stagers starved each other
([X1](stripe-model.md#progress-the-previous-state-and-the-reservation)).

**The oracle** is a sequential model of a stripe's bytes. An acknowledged write is in every
strong read that begins after it. A read returns a committed state, never a mixture. A write
whose outcome was refused changed nothing, and one whose outcome is unknown either happened
whole or did not: the three outcomes are held to three contracts, as the tablet oracle holds
them.

**What it checks is computed from durable facts**, not from any actor's opinion of itself,
as the existing checker computes what is committed.

It is [X1](spikes.md#x1-the-stripe-protocol-as-a-model), and it is the one spike whose
output is kept: the model and its schedules become M11's first acceptance test,
`object_model_preserves_acknowledged_bytes`, which [S18](contract.md#acceptance-tests) owns
as C13 owns the tablet model's. ✅ X1 reported on 2026-10-06 ([its record](stripe-model.md)),
and both of this part's model tests exist, in `shoal-model/tests/stripe_model.rs`.

### The fixture

**Devices and slices.** A child gets several directories as devices, each with its slices, so
one host can run a pool with a device failure domain, and five children can run 4+2 with a
host one. That is how a layout wider than the lab's three hosts is tested at all.

**Faults**, each a prerequisite of [S1](prerequisites.md#required) and each tested against
itself before anything relies on it:

| Fault | What it does | What it is for |
| --- | --- | --- |
| A torn write | Part of a write to a named stripe chunk reaches the device and the rest does not | P9 inside a chunk; [S6](device-store.md)'s replay |
| A full disk | A device refuses writes past a point | Space taken at the stage; nothing failing after a commit |
| A lost device | Every slice of a device answers every call with an error | P17; a rebuild |
| An empty replacement | A device's directory, every slice in it, is emptied while its child is down | [S4](pools-and-devices.md#a-device-has-slices) |
| A flipped bit | One byte of a chunk unit is changed on disk and synced | P15; a scrub |

**Crash points** for a stripe write: after a stage is synced and before its answer; after
the answers and before the proposal; proposed and not applied; committed and no holder told;
in the middle of an apply; applied and the staged copy not yet dropped.

**A byte ledger.** The fixture records, outside the code under test, every range written
with a checksum of its bytes and its outcome, and judges every read against it. It is the
write ledger of [C11](../distributed/testing.md#the-write-ledger-and-the-oracle) for ranges
of bytes.

**What a kill proves** is unchanged: nothing about durability. A durability test stops a
child, or uses a fault that withholds a sync.

### The acceptance index

Each S page owns the tests in its own table. This is the index, and it holds no second copy
of their names.

| Page | Scope of its tests |
| --- | --- |
| [S2](buckets.md#acceptance-tests) | The generated tables, the fingerprint, the client half |
| [S3](objects.md#acceptance-tests) | Path identity, inline objects, holes, truncate, replace |
| [S4](pools-and-devices.md#acceptance-tests) | Device and slice identity, committed policy, readiness, a rotational pool's redundancy |
| [S5](placement.md#acceptance-tests) | Failure domains, movement, generations, positions, seats, the score on every build |
| [S6](device-store.md#acceptance-tests) | Staging, applying, tearing, space, checksums |
| [S7](write-path.md#acceptance-tests) | Atomicity, fencing, acknowledgement, retries |
| [S8](erasure-coding.md#acceptance-tests) | Decoding, labels, partial overwrites |
| [S9](read-path.md#acceptance-tests) | Read levels, stale chunks, degraded reads |
| [S10](recovery.md#acceptance-tests) | Missed writes, backfill, moves, reclamation, a rebuild's pace |
| [S11](scrub.md#acceptance-tests) | Finding damage, never laundering it, a scrub's pace |
| [S12](wire-and-client.md#acceptance-tests) | Frames, windows, retried streams |
| [S13](isolation.md#acceptance-tests) | Tables surviving object work and device loss |
| [S14](operations.md#acceptance-tests) | Admin operations, readiness, refusing mismatched rows |
| [S15](performance.md#acceptance-tests) | The driver and its captures |
| [S16](#acceptance-tests) | The saved schedules, the fixture's faults, the byte ledger, and the check on these tables |
| [S18](contract.md#acceptance-tests) | The model, and the rows of the failure model no other page owns |

**The check joins at M11.** `acceptance_tables.rs` is extended then to read this directory
and to know M11 to M21, and it holds these tables to the rules it holds the Distributed
chapter's to:

- a test is named in one table and no other, across both chapters, since a name is looked
  up as a function in the whole workspace;
- a row names exactly one gate, and the gate's section on the
  [milestones](milestones.md) page names the row's page;
- every test a delivered gate names exists as a function.

The tables are written to the first two rules already, and a script run when this part was
written checked them; the script is not the test and was not kept. Until M11 these tables
are intentions, and nothing in the workspace asserts anything about them.

## Alternatives rejected

**Process tests alone.** The fault a design fears is an interleaving, and a process test
reaches one by luck.

**Raft modelled again inside the new model.** It is already held to P1 to P6. Modelling it
twice doubles the state to explore and proves nothing new.

**A model in another tool.** `shoal-model` exists because a schedule that needs anything
but the crate to replay is worth nothing a year on.

**Faults by killing processes.** See above.

## What it costs

A second model to keep true to the design as the design moves. Fault code in the device
store that only tests reach. Tests that each allocate whole cores, so the suite is as slow
as the machine is busy, as the cluster fixture's already is.

## What it breaks

- "The model is one Raft-shaped tablet group": it is two models.
- `acceptance_tables.rs` knows one chapter and one list of milestones.

## Invariants to uphold

- The model depends on serde alone and names no shoal crate.
- Every unsafe setting has a saved schedule that fires its check, and the safe setting has
  none.
- A property is checked from durable facts.
- The fixture's ledger shares no code with what it judges.
- No durability claim rests on a kill.
- Every fault is tested against itself.

## Prerequisites

~~[S1](prerequisites.md#required): the three device faults.~~ Delivered by
[F70](../features/storage-faults.md). The contract's draft, since a model checks properties and
has to be given them, and this page's model held to [S7](write-path.md)'s schedules, which it
was on 2026-10-03 ([What a spike needs first](spikes.md#what-a-spike-needs-first)).

## How it would be measured

The model is judged by what it catches: each row of the unsafe table above replays to its
violation, and a generated run of the safe policy finds none. X1's search ran each, at every
layout ([the record](stripe-model.md#what-was-run)). The fixture's faults are
judged by self-tests that show each does what its name says.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `every_unsafe_policy_has_a_saved_schedule` | Each unsafe setting replays to the violation recorded for it | M11 |
| `device_faults_do_what_they_say` | A torn write tears, a full disk refuses, a lost device errors, an emptied one is empty, a flipped bit is flipped | M11 |
| `byte_ledger_judges_the_three_outcomes` | Over histories written by hand, an acknowledged range must be read, a refused one must not be, and an unknown one may be whole or absent and never part | M11 |
| `object_acceptance_tables_name_real_tests_and_valid_gates` | Every row of every table in this part names an existing function, once its gate is delivered, and a gate on the milestones page | M11 |

## Related

[S18](contract.md) for what is checked; [S7](write-path.md#the-schedules-that-shaped-it)
for the schedules; [X1](spikes.md#x1-the-stripe-protocol-as-a-model) for building the
model first; [C11](../distributed/testing.md) and
[F36](../features/cluster-harness.md) for the instruments as they are.
