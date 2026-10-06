# X1. The stripe protocol, modelled

**Reported 2026-10-06.** This is the record of spike
[X1](spikes.md#x1-the-stripe-protocol-as-a-model). It builds [S7](write-path.md)'s preferred write
protocol (holders stage, one conditional commit of the stripe's row decides, holders apply) as a
second model in `shoal-model`, beside the tablet model and held to that crate's rules. It holds
the protocol to the contract's clauses P7–P13 and P15–P17 under every interleaving its failure
model allows. Every schedule of S7 is saved with the unsafe setting that makes it fire, and a
generated search ran the safe policy and every setting at three layouts: 1,740,000 runs on europa,
then titan and hyperion 348,000 each from a `znver1` build, half of them seeds europa had run,
which came out the same on every one. Alone among the spikes, its code is kept: the model and its
schedules are M11's acceptance tests.

It ends in a recommendation, which
[S18](contract.md#q16-and-q18-and-q14-q15-q19-in-part-the-stripe-protocol-modelled-2026-10-06)
records as the choice:

- **S7's direction holds.** With the repairs below, the safe policy broke no clause in
  1,200,000 generated runs at three layouts, and with a chunk's previous state kept every reader
  and every stripe's stagers finished within the progress check's bounds.
- **Ten rules the pages stated did not hold**, and each was repaired by a local rule. None needs
  a primary, a vote among holders, or undo, so the stop rule agreed before the spike never fired.
  Each rule as written is kept as a setting with a saved schedule that breaks it.
- **Q16 is settled stricter than it was asked.** A chunk the write did not touch counts toward
  `k + f` only when its holder confirms, in the write's round, that it holds the label. Counting
  it on the row's word broke P11 with its holder down, and with its holder believed up.
- **Q15**: the node that received the client's bytes stages. The leader's reservation is not
  needed for progress, and stays an optimization.
- **Q18**: truncate by epoch holds, with four repairs to how a commit stamps, how a write covers
  what a floor hides, how a reclaimed row is left, and how a truncate orders itself against the
  stripe its cut falls inside, and two to which entry and row a reader takes.
- **Q19's rest**: a commit compares the row's sequence and the placement group's generation, both
  by equality, which is what [F68](../features/conditional-writes.md) offers. Positions move with
  the generation and need no check of their own.
- **A holder keeps a chunk's previous state** until its next apply, which
  [S9](read-path.md#a-stripe-chunk-under-another-label) had left open.
- **The row keeps no digest of each chunk**, the question
  [Q21](contract.md#q21-in-part-the-checksum-2026-10-03) and Q25 left.

**The search had to run at volume to be believed.** Three thousand seeds of each configuration
found nothing under the safe policy. Twenty thousand found the last of the ten rules, two rules
the pages left unstated, and two faults of the model's own; a hundred thousand found a third rule
left unstated. Each showed in one to three runs of a configuration's tens of thousands
([below](#what-the-search-found-and-the-repairs)). The last of the ten lost an acknowledged
write's units from a holder, which none of the schedules written by hand had shown. At 100,000
seeds of each of the safe policy's twelve configurations, 1,200,000 runs, it broke nothing.

## The question

[Q14](contract.md#questions-to-answer) asks whether S7's preferred direction is safe; Q15, who
stages; Q16, what the acknowledgement rule is, and whether a chunk the write did not touch, on a
slice that is down, counts toward `k + f`; Q18, whether truncate by epoch holds across tablets.
[X2](placement-simulation.md) left the rest of Q19 to X1: how a commit checks a generation, and the
positions beside it. And Q21 and Q25 left one question between them: whether the row keeps a
digest of each chunk, to catch a write lost whole.

The spike's section named, before it ran, what would change the design: any violation of a clause
the model checks under the safe policy. It named three results expected to be close, each of
which would move a page:

- a reader of a stripe written continuously that cannot finish without a holder keeping a chunk's
  previous state, which would reopen S9;
- an untouched chunk on a slice that is down that cannot safely count toward `k + f`, which would
  tighten Q16;
- two stagers that starve each other without a reservation, which would make the leader's
  reservation part of the protocol.

## How it was judged

Stated before the search ran, from S16's specification of the model and the rule the user and the
spike agreed for what it found.

**Required.**

- Every safety schedule of S7 breaks a clause S16 names for it under its unsafe setting, and
  breaks nothing under the safe policy. The progress schedule breaks its bound under its setting
  and not under the safe policy.
- Every unsafe setting of S16's table has a saved schedule that replays to the violation it
  records (`every_unsafe_policy_has_a_saved_schedule`).
- Generated runs of the safe policy, at every layout and both ways of each progress setting, break
  no clause (`object_model_preserves_acknowledged_bytes`), and their coverage counts show crashes,
  device losses, fills, map changes, truncates, rebuilds, refusals, unknown outcomes and both read
  levels happened.
- A clause is judged from durable facts alone, never from an actor's opinion of itself.

**What a violation under the safe policy means**, agreed with the user before the search: it is
either a fault of the model, fixed in the model, or a hole in the design. A hole whose repair is
a local rule is repaired in the safe policy; the rule it replaces becomes a setting with the
schedule that breaks it, and the page that stated it is changed with the old text struck through.
A hole whose repair needs a primary, a vote among holders, or undo stops the spike and goes back to
the user. None did.

**Two readings are the model's own**, and are what a clause means here:

- **P11 is judged at each chunk's evidence.** A chunk counts when a synced stage answered for
  it, or when its holder confirmed in the write's round that it holds the label, and a loss
  after that is what `f` is for. A disk can fail the instant after it answers, so no protocol
  can be held to more.
- **A read is judged at a consistent cut** of the entry's commits and the stripe's. A write
  comes after any truncate whose epoch it read. A write that read an earlier epoch and wrote a
  unit past the cut comes before that truncate, since the floor hides that unit as though it
  did. An extension comes after the write it extends over. A default read may take any entry
  state; a strong read's cut is at or after every operation acknowledged before it began.

## What was run

### The model

`shoal-model/src/stripe/`, about 9,700 lines with its unit tests, depending on `serde` and
`serde_json` alone. It shares the seeded generator, `SplitMix64`, the identifiers of an operation
and a node, and the `Property` numbers with the tablet model, and nothing else: its world, events,
checker, oracle, schedules and minimizer are its own, as [S16](testing.md#the-model) asked.

| Module | Holds |
| --- | --- |
| `ids.rs`, `layout.rs` | Stripes, positions, slices, devices and disks; sequences, tags, labels, epochs and generations. Three layouts, replicated three ways, 2+1 and 4+2, each at `f = 1`, two units a chunk |
| `content.rs` | The bytes as an algebra: a data unit is the write that last wrote it, zeros, or torn; a parity unit is the data units it encodes. Folding a change in twice undoes it, and a decode over chunks of two writes is garbage, so a mixture is always seen |
| `policy.rs` | One knob a rule, the first variant the contract: fourteen of S16's safety settings, its progress setting, Q16 and the two progress questions, and the ten rules the search broke |
| `group.rs` | The entry and each stripe's row as atomic objects that apply commands in one order, judge a condition at apply, keep a retry table, and let a lagging replica answer with a committed prefix. P1–P6 are what they are held to, by the tablet model, and are not modelled again |
| `holder.rs` | A slice: chunks under labels, staged records synced and not, an apply in two steps a crash tears, a chunk's previous state when kept, recovery |
| `actors.rs` | Stagers, truncators, readers, rebuilders and movers, the reclaimer, and the leader's timer |
| `world.rs`, `event.rs` | The world every event is applied to, total and frozen after a violation |
| `check.rs`, `oracle.rs`, `progress.rs` | The clauses, the client history reads are judged by, and the progress check |
| `schedule.rs`, `scenarios.rs`, `minimize.rs` | Saved schedules, the generator, S7's sixteen built by hand, and the minimizer |

**What it models** is one object: its `ObjectMeta` entry and two stripes, each with its row, its
placement group's generation and positions, its holders and its readers. A node has one disk and
one slice, and two spare nodes take moves and rebuilds. The events are S16's: a message delivered,
lost, duplicated or answered by a replica that lags; a holder's sync and each step of its apply; a
holder asking about what it staged; a write, a retry, a truncate, a default or a strong read; a
client or a stager giving up; a coordinator dying; a node crashing and restarting; a disk failing
silently, reported, or replaced by an empty one at its path; a disk filling and given space back;
the leader's no-op; a rebuild, a move and a reclaim; a reservation lapsing.

### The checks

Every event is followed by the checks, from durable facts alone, specific first so that each
setting reports the clause S16's table names for it:

| Clause | Fires when |
| --- | --- |
| P8 | A commit in a row's history did not meet its condition at apply: its sequence, its generation |
| P11 | A write was acknowledged with fewer than `k + f` chunks current at their evidence |
| P15 | A record replayed onto a chunk already at its label changed it |
| P9 | A holder's chunk carries a label no committed row state names, or bytes that are not its label's, or is torn with no staged copy to write it again |
| P17 | The row calls a position current on an up node and a healthy disk whose slice cannot make that label |
| P16 | A holder discarded staged or current bytes with no committed fact excluding them |
| P7 | An apply of a committed write failed for want of space |
| P10 | A read used chunks whose labels are not the row state it consulted |
| P13, P12 | A read returned bytes a truncate cut, or hid a write no truncate could cut; or no consistent cut of the history gives what it returned |

A run that breaks no clause is then judged by the progress check: after the step its faults stop
at, every reader that began finishes within 600 steps, one of a stripe's stagers commits within 600,
and no rebuild rewrites a chunk its holder held throughout and could have been asked about.

### The schedules

Twenty-six files under `shoal-model/schedules/stripe/`, which the tablet model's loader never reads:
S7's sixteen built by hand, three more built by hand for Q16 and Q18, and seven found by the search
for the rules as written and minimized. `cargo run -p shoal-model --release --example
regenerate_stripe_schedules` writes them; the tests only load them, build the hand-written ones
again, and fail if a file no longer replays to what it records or is not byte for byte what
its builder makes.

`shoal-model/tests/stripe_model.rs` holds seven tests. The two M11 names:
`object_model_preserves_acknowledged_bytes` (three layouts, three variants, four seeds each, and
the coverage counts) and `every_unsafe_policy_has_a_saved_schedule`. Beside them: every S7
schedule saved and rejected by the safe policy; the rules as written breaking and their repairs
holding; every saved file replaying in canonical form; a failure minimizing to a reproducible
core; and a commit's condition comparing only the sequence and the generation. The test binary
runs in about three seconds in a debug build; volume lives in the search.

### The search

`cargo run -p shoal-model --release --example stripe_search -- all` runs every configuration over a
range of seeds on every thread: the safe policy at each layout with the previous state kept and
dropped and the reservation granted and not (twelve), each of S16's fifteen settings at each layout
(forty-five), and each of the ten rules as written at each layout (thirty). A run is 1,800 steps:
faults for 600, then calm, with at most four operations at once, one node down and one disk lost
at a time, and a replica up to three commits behind. Once the cluster has settled, two writers and
two readers work each stripe. Each configuration's seeds run in blocks of 250 with a digest of
every run's outcome, so two hosts' records can be compared block by block and their other blocks
added to one count; `report` does both. A run's outcome is its length, its coverage counters,
the steps every calm reader and writer took, and what it broke. The first digest folded the length
and what a run broke and nothing else, so two runs that broke nothing agreed whatever they did; it
was widened, and every record on this page taken again, before any was compared.

### Where, and from which builds

Nothing here is timed for a decision, so `shoal-tmdb` stayed up and every run went under `nice`.
The wall times are information only.

| Host | Build | Seeds | Threads | Wall |
| --- | --- | --- | --- | --- |
| europa | native, Zen4 | 0 to 20,000, every configuration | 30 | 753 s |
| europa | native, Zen4 | 0 to 100,000, the safe policy's twelve | 30 | 632 s |
| titan | `znver1`, Zen1 | 0 to 2,000 and 20,000 to 22,000, every configuration | 8 | 569 s and 598 s |
| hyperion | `znver1`, Zen1 | 0 to 2,000 and 22,000 to 24,000, every configuration | 8 | 575 s and 591 s |

**Every host found the same.** Titan and hyperion each ran 696 blocks of 250 seeds that europa
ran too, the first 2,000 seeds of all eighty-seven configurations, and every block's digest was the
same on all three hosts: a native build on Zen4 and a `znver1` build on Zen1, the same runs to the
step. Their other blocks are added to europa's, so every count below is of 24,000 runs a
configuration unless it says otherwise. The records are kept under `target/lab/x1/runs/`, which is
not committed; `stripe_search report` reads them.

## The sixteen schedules

Each is saved under the setting S16 names for it, and replays to what this table records. Under
the safe policy each finds nothing.

| # | Saved as | Setting | Layout | Events | What it records |
| --- | --- | --- | --- | --- | --- |
| 1 | `s01_two_stagers_on_one_base` | `sequence_as_label` | r3 | 41 | P9: slice 0 holds stripe 0 position 0 under 1/257, which no committed row state names |
| 2 | `s02_parity_from_a_stale_row` | `commit_with_no_condition` | 2+1 | 45 | P8: write 2 committed to stripe 0 at sequence 1, though it was staged against no row |
| 3 | `s03_returning_slice` | `returning_slice_taken_as_current` | r3 | 19 | P17: the row calls stripe 0 position 2 current at 1/257 on slice 2, which does not hold it |
| 4 | `s04_truncate_under_a_writer` | `commit_ignores_the_truncate_epoch` | r3 | 47 | P13: read 4 of stripe 1 returned unit 0, which a truncate cut |
| 5 | `s05_map_changes_during_a_write` | `commit_ignores_the_generation` | r3 | 29 | P8: write 1 committed to stripe 0 under generation 1, though it was staged under 0 |
| 6 | `s06_stager_timeout` | `holder_discards_on_a_stagers_word` | r3 | 18 | P16: slice 0 discarded stripe 0's staged 1/257 with no committed fact excluding it |
| 7 | `s07_ack_after_one_stage` | `ack_after_one_stage` | 4+2 | 30 | P11: write 1 was acknowledged with 4 current chunks where it needs 5 |
| 8 | `s08_patch_replayed` | `parity_staged_as_a_patch` | 2+1 | 25 | P15: slice 2 wrote a staged record again after a restart over a chunk that held it, and changed it |
| 9 | `s09_crash_during_an_apply` | `staged_copy_dropped_when_its_apply_starts` | r3 | 24 | P9: slice 0 holds a chunk torn under 1/257, with no staged copy to write it again |
| 10 | `s10_paused_stager` | `commit_with_no_condition` | r3 | 47 | P8: write 1 committed at sequence 1, though it was staged against no row |
| 11 | `s11_lost_stage_acknowledgement` | `rebuild_without_asking_the_holder` | r3 | 40 | The rebuild bound: rebuild 2 rewrote position 2, which its holder held current |
| 12 | `s12_empty_disk_at_the_same_path` | `device_known_by_its_path` | r3 | 33 | P17: the row calls position 1 current on slice 1, which does not hold it |
| 13 | `s13_disk_fills_before_the_apply` | `space_taken_at_the_apply` | r3 | 23 | P7: slice 0 could not apply a committed write: its device filled after the commit |
| 14 | `s14_lagging_replica_shows_no_row` | `discard_on_a_lagging_replicas_view` | r3 | 21 | P16: slice 2 discarded a staged record on a lagging replica's view |
| 15 | `s15_apply_before_the_commit` | `apply_before_the_commit` | r3 | 26 | P9: slice 0 holds a chunk under a label no committed row state names |
| 16 | `s16_reader_meets_a_newer_chunk` | `reader_accepts_a_newer_chunk` | 4+2 | 92 | P10: read 1 decoded position 4 at 2/769 beside a row that names 0/0 |

Three were written differently from S7's sketch, each because the sketch's story did not fire
the check under its setting:

- **15** first had the holder apply a write before its commit and the commit then refused. A
  write refused for a moved row is one whose holder already holds another write's chunk, so the
  apply of the loser is what shows: write 2 loses its stage on slice 0, commits on slices 1 and 2,
  and slice 0's early apply of write 1 is a label no row names.
- **16** needs two writes applied on the first parity holder before the reader asks it, and the
  second parity holder given the second write's apply too; with the previous state kept, one
  write applied leaves the reader its chunk.
- **11** breaks no clause, as S7 already said: what goes wrong is a rebuild nobody needed.

## Every setting, searched

Each of S16's settings at each layout, 24,000 runs, with the runs that broke a clause and which.
Every setting breaks a clause S16 names for it at some layout. Where one breaks nothing the layout
cannot show it: a 2+1 write needs every holder, so none is ever missed, and a replicated stripe has
no parity. The progress setting breaks the rebuild bound instead, wherever a rebuild happens.

| Setting | r3 | 2+1 | 4+2 |
| --- | --- | --- | --- |
| `sequence_as_label` | 22,967 (P12 1397, P13 28, P17 122, P9 21420) | 19,200 (P12 2102, P13 39, P17 40, P9 17019) | 18,844 (P12 1338, P13 53, P17 21, P9 17432) |
| `commit_with_no_condition` | 23,686 (P8) | 20,092 (P8) | 22,859 (P8) |
| `returning_slice_taken_as_current` | 21,211 (P17) | none | 16,528 (P17) |
| `commit_ignores_the_truncate_epoch` | 3,668 (P12 1175, P13 2493) | 2,807 (P12 818, P13 1989) | 2,688 (P12 945, P13 1743) |
| `commit_ignores_the_generation` | 3,325 (P8) | 944 (P8) | 1,442 (P8) |
| `holder_discards_on_a_stagers_word` | 17,937 (P16) | 16,618 (P16) | 14,989 (P16) |
| `ack_after_one_stage` | 23,991 (P11) | 23,957 (P11) | 23,717 (P11) |
| `parity_staged_as_a_patch` | none | 684 (P15 342, P9 342) | 547 (P15 261, P9 286) |
| `staged_copy_dropped_when_its_apply_starts` | 4,392 (P17 2089, P9 2303) | 1,278 (P17 597, P9 681) | 512 (P17 72, P9 440) |
| `device_known_by_its_path` | 2,837 (P17) | 2,879 (P17) | 3,629 (P17) |
| `space_taken_at_the_apply` | 12,366 (P7) | 5,999 (P7) | 4,746 (P7) |
| `discard_on_a_lagging_replicas_view` | 23,995 (P16) | 23,994 (P16) | 23,938 (P16) |
| `apply_before_the_commit` | 24,000 (P17 352, P9 23648) | 24,000 (P17 281, P9 23719) | 24,000 (P17 189, P9 23811) |
| `reader_accepts_a_newer_chunk` | 139 (P10) | 29 (P10) | 31 (P10) |
| `rebuild_without_asking_the_holder` | the rebuild bound, 20,491 | none | the rebuild bound, 19,539 |

## What the search found, and the repairs

### Ten rules the pages stated

Each broke a clause under the safe policy with only that rule as the page wrote it. Each is a knob
whose first variant is the repair and whose second is the rule as written, saved with the schedule
that breaks it.

| Rule as written | Page | Clause | Runs that broke it, r3 / 2+1 / 4+2 | The repair |
| --- | --- | --- | --- | --- |
| An untouched chunk counted on the row's word while its holder is believed up | [S7](write-path.md#the-acknowledgement-rule), Q16 | P11 | – / 83 / 85 | Counted only on its holder's confirmation in the write's round |
| The same while its holder is down | S7, Q16 | P11 | – / 2,886 / 4,004 | The same |
| A stamp that moves to whatever its writer read | [S3](objects.md#size-holes-and-truncate) | P13 | 1 / 0 / 0, and a schedule built by hand | A stager does not commit on a row stamped past the epoch it read |
| A write leaves the units a floor hides as they were | S3 | P13 | 1,576 / 1,798 / 2,231 | It writes them as zeros |
| A reclaimed row deleted | S3, [S10](recovery.md#reclamation) | P17 | 431 / 278 / 291 | A tombstone, a sequence past it |
| A reader hides by the entry it read | [S9](read-path.md#which-state-a-read-returns) | P12 | 2,286 / 2,224 / 1,855 | It reads the entry again for a row stamped past it |
| A default read takes its row at `One` | S9 | P12 | 759 / 739 / 692 | The row at `Quorum`, after the entry |
| A tag from the request identity alone | [S7](write-path.md#labels-not-numbers) | P9 | 99 / 41 / 25 | A tag a try; the retry table recognises a retry |
| A truncate commits to the entry alone | S3 | P12 | 43 / 27 / 17 | It fences the stripe its cut falls inside first |
| A staged write discarded once the row moves past its base and names another label | S10 | P17 | 95 / 25 / 2 | Kept while a label the row names stands on it |

A replicated write touches every copy, so r3 has no untouched chunk to count. Where a rule broke
more than one clause the count is of runs and the clause is the one its saved schedule records:
the reader's entry also broke P13, the deleted row P13 and P9 too, and the stamp's one generated run
broke P12.

What each was:

- **An untouched chunk counted on the row's word**, while its holder is believed up or while it
  is down ([Q16, both ways](#q16-both-ways)).
- **A stamp that moves to whatever its writer read** ([S3](objects.md#size-holes-and-truncate)).
  A writer that read the epoch before a truncate committed after a write made since, and stamped
  the stripe back below the floor. The newer write, acknowledged after the truncate, was then
  hidden by it. One generated run in 72,000 broke it, at r3; the saved schedule,
  `q18_stamp_moves_backwards`, is built by hand. The
  repair: a stager that finds its row stamped past the epoch it read does not commit, and reads
  the entry again.
- **A write leaves the units a floor hides as they were** (S3). A floor hides a stripe stamped
  below its epoch; a write into that stripe moves its stamp past the floor, so every unit of it
  is shown again, the cut ones included. The repair: the write writes the hidden units as zeros.
- **A reclaimed row deleted** (S3, [S10](recovery.md#reclamation)). Deleting the row set its
  sequence back to a stripe's with no row; the next write's labels then repeated sequences its
  holders had passed, and a holder never applies a label at or below its chunk's. The repair: a
  tombstone, a sequence past the row it replaces, stamped with the floor's epoch.
- **A reader hides by the entry it read** ([S9](read-path.md#which-state-a-read-returns)). A row
  stamped past that entry's epoch means a truncate committed in between, and hiding by the old
  floors showed bytes no committed state holds. The repair: the reader reads the entry again.
- **A default read takes its row at `One`** (S9). An extension commits after the write it extends
  over, so an entry showing the new size beside a row without the write is no state the object
  was in. The repair: the row at `Quorum`, after the entry.
- **A tag from the request identity alone** ([S7](write-path.md#labels-not-numbers)). A retry
  against the same row, after a truncate between its tries, staged other bytes than its first try
  under the same label. The repair: a tag a try, and the row group's retry table, which every
  tablet group keeps, says whether a write already committed.
- **A truncate that commits to the entry alone** (S3). A stripe the cut falls inside has units on
  both sides of it. A writer that read the old epoch committed after the truncate, and the floor
  hid its units past the cut as though it came before; a reader between them had seen the
  truncate without it. The repair: the truncate fences that stripe's row first, at the epoch it
  will commit, and commits only at the epoch and the size it read.
- **A staged write discarded once the row moves past its base and names another label**
  ([S10](recovery.md#reclamation)). This is the hole the volume search found, and the most
  serious of the ten. A change to part of a chunk is staged as the units it changed, over the
  label it expects. A holder held two committed records of one position, neither applied, the
  second staged over the first, and then learned the row at the second's sequence. The row had
  moved past the first's base and named another label, so the holder dropped the first: the
  units it alone carried were gone from that holder, though its write had been acknowledged
  counting them. The repair: a holder keeps every committed record a label the row names stands
  on, until its chunk reaches that label, and counts a label as held only if it can make it.

### Rules the model made precise

Each is a rule the pages implied and the safe policy needed stated, found as a violation under the
safe policy and fixed in the model. None has a setting of its own, since the pages never stated
the other way.

- **The staged copy outlives its apply, even once a committed fact excludes it**
  ([S6](device-store.md#staging-two-cases)). The holder's discard rule dropped a record while its
  apply in place was writing; a crash then tore the chunk with nothing to write it again. Found at
  r3 in one or two runs of each configuration's 20,000.
- **A holder answers for a label only if it can make it.** It had laid a committed record over
  whatever its chunk held, so it accepted a partial stage over a label it could no longer make.
  Once that was mended, it answered a rebuild's whole chunk as a repeat of a record of the same
  label whose base it had discarded, since "staging the same write twice is staging it once".
  Either way the commit then called current a chunk nobody held (P17). Now a partial stage is
  taken only over a label the holder can make, and a stage of a label whose record it cannot make
  replaces that record. Each was found at r3, in one to three runs of a configuration's 20,000,
  once the holder judged what it holds by what it can make.
- **A truncate commits at the size it read, as well as the epoch.** The size decides where its
  floor falls and so which stripe it fences, and an extension moves the size and not the epoch. A
  truncate that read a size whose cut fell between stripes fenced nothing; an extension then
  moved the cut inside a stripe, and a writer that had read the old epoch committed into it after
  the truncate (P12). Found at r3 and 2+1, in one or two runs of 80,000.
- **The no-op makes a row** on a stripe with none, since only a row that exists excludes a first
  write staged against no row ([S7](write-path.md#the-preferred-direction-step-by-step)).
- **A reclaim's discard names the sequence it reaches**, and a holder keeps a chunk a later write
  made.
- **A holder never applies a label at or below its chunk's**: a lagging view can name a label a
  newer write has passed.
- **A slice holds one position of a stripe and answers only for it**, and a move of a position
  the row calls stale switches it without copying ([S10](recovery.md#moves)).
- **A write abandoned before it proposed is refused only on its first try.** On a later try an
  earlier one's proposal may still be in flight, so its outcome is unknown.
- **A rebuild that cannot ask its holder stops**, rather than rebuild over a chunk that may be
  current.

### Faults of the model's own

Found the same way and fixed in the model, with no change to any page: the leader knew a writer
by its round rather than its request identity, so a stager that started a round again queued
behind its own reservation (4+2, one run in 20,000 with the reservation); the rebuild check took
"held when the row was read and holds now" for "held throughout", across a gap in which the holder
had said no (4+2, one run); and a number of faults of plumbing found before the search ran at
volume: messages to a slice that was down left unanswered, late decisions taken for current ones,
builders that looped on a world frozen by a violation.

## Q16, both ways

Two schedules built by hand show both answers that count an untouched chunk on the row's word
breaking P11, each at 4+2 with a write touching one data chunk:

- **`q16_counted_while_believed_up`**: the untouched chunk's disk fails silently, which
  [P7](contract.md#the-contract) allows, and its node stays up. The write counts the chunk on the
  row's word and is acknowledged with four current chunks where it needs five.
- **`q16_counted_when_down`**: the untouched chunk's node is down and its disk is swapped for an
  empty one meanwhile. The same.

The search found both too: counted while its holder was believed up in 83 runs of 24,000 at 2+1
and 85 at 4+2, and counted while it was down in 2,886 and 4,004. Under the safe policy the holder
of each untouched chunk the write counts confirms, in the write's round, that it holds the label
the row names, and its answer is the chunk's evidence. That costs a message to each, sent beside
the stages, and no durable round: the holder answers from what it holds. Only a partial write of
an erasure coded stripe has untouched chunks to count.

## Progress: the previous state and the reservation

From europa's run, the twelve safe configurations at the generator's calm load of two writers and
two readers a stripe:

| Layout | Previous state | Reservation | Runs | Stalls | Reader p50 / p99 / max | Failed reads | Writer p50 / p99 / max | Refused writes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| r3 | Dropped | None | 20,000 | reader 5 | 44 / 125 / 327 | 5 | 67 / 122 / 215 | 187,863 |
| r3 | Dropped | Granted | 20,000 | none | 39 / 114 / 275 | 0 | 91 / 165 / 280 | 99,073 |
| r3 | Kept | None | 20,000 | none | 43 / 112 / 264 | 0 | 67 / 122 / 199 | 186,387 |
| r3 | Kept | Granted | 20,000 | none | 39 / 103 / 222 | 0 | 91 / 165 / 298 | 98,197 |
| 2+1 | Dropped | None | 20,000 | reader 1 | 43 / 112 / 287 | 1 | 71 / 123 / 203 | 178,843 |
| 2+1 | Dropped | Granted | 20,000 | none | 40 / 103 / 229 | 0 | 91 / 169 / 314 | 105,932 |
| 2+1 | Kept | None | 20,000 | none | 43 / 100 / 191 | 0 | 71 / 123 / 191 | 177,675 |
| 2+1 | Kept | Granted | 20,000 | none | 39 / 92 / 188 | 0 | 91 / 169 / 314 | 105,230 |
| 4+2 | Dropped | None | 20,000 | reader 2 | 81 / 207 / 460 | 2 | 117 / 199 / 347 | 104,679 |
| 4+2 | Dropped | Granted | 20,000 | reader 2 | 73 / 187 / 415 | 2 | 138 / 260 / 444 | 73,979 |
| 4+2 | Kept | None | 20,000 | none | 80 / 173 / 372 | 0 | 117 / 198 / 351 | 102,770 |
| 4+2 | Kept | Granted | 20,000 | none | 72 / 159 / 283 | 0 | 138 / 258 / 444 | 72,719 |

Steps are the model's, not time: what a reader or a writer took from its invocation to its answer,
for those begun after the faults stopped. A refused write is a stage wasted: its row moved.

Under more writers, where the questions were expected to show, the same configurations ran
2,000 seeds each with four and six writers a stripe, beside the two-writer rows above. The previous
state, with no reservation, where a stall is a run with a reader past its bound or failed by name:

| Layout | Writers | Runs | Dropped: stalls | Dropped: reader p99 / max | Kept: stalls | Kept: reader p99 / max |
| --- | --- | --- | --- | --- | --- | --- |
| r3 | 2 | 20,000 | 5 | 125 / 327 | 0 | 112 / 264 |
| r3 | 4 | 2,000 | 1 | 205 / 469 | 0 | 175 / 293 |
| r3 | 6 | 2,000 | 7 | 287 / 551 | 0 | 236 / 407 |
| 2+1 | 2 | 20,000 | 1 | 112 / 287 | 0 | 100 / 191 |
| 2+1 | 4 | 2,000 | 1 | 188 / 351 | 0 | 158 / 288 |
| 2+1 | 6 | 2,000 | 1 | 266 / 540 | 0 | 215 / 353 |
| 4+2 | 2 | 20,000 | 2 | 207 / 460 | 0 | 173 / 372 |
| 4+2 | 4 | 2,000 | 5 | 329 / 607 | 0 | 265 / 491 |
| 4+2 | 6 | 2,000 | 30 | 461 / 754 | 0 | 358 / 580 |

The reservation, with the previous state kept; no configuration had a stager stall, with it or
without:

| Layout | Writers | None: writer p50 / p99 | None: refused a run | Granted: writer p50 / p99 | Granted: refused a run |
| --- | --- | --- | --- | --- | --- |
| r3 | 2 | 67 / 122 | 9.3 | 91 / 165 | 4.9 |
| r3 | 4 | 95 / 182 | 18.5 | 156 / 270 | 8.8 |
| r3 | 6 | 122 / 237 | 23.4 | 225 / 371 | 9.8 |
| 2+1 | 2 | 71 / 123 | 8.9 | 91 / 169 | 5.3 |
| 2+1 | 4 | 101 / 190 | 17.7 | 160 / 278 | 8.9 |
| 2+1 | 6 | 130 / 248 | 22.1 | 229 / 379 | 10.1 |
| 4+2 | 2 | 117 / 198 | 5.1 | 138 / 258 | 3.6 |
| 4+2 | 4 | 162 / 302 | 9.9 | 219 / 407 | 6.5 |
| 4+2 | 6 | 203 / 400 | 12.1 | 289 / 540 | 7.6 |

**A holder keeps a chunk's previous state.** Without it, readers ran past their bound or failed by
name at every layout, more as writers were added: at 4+2, 2 runs in 20,000 with two writers, 5 in
2,000 with four and 30 in 2,000 with six; across 100,000 seeds of two writers, 4 to 14 a
configuration. With it, none, at any layout or load, and a reader's p99 fell by about a fifth. A
reader one write behind is served the chunk's previous state, where without it the apply that
replaced it sent the reader back to its row. What it costs a device is [not settled
here](#what-x1-does-not-settle).

**The leader's reservation stays out of the protocol.** No stripe's stagers starved each other
within the bound in any configuration, with the reservation or without, up to six writers a
stripe. The reservation halved the stages wasted on a moved row, 23.4 a run to 9.8 at r3 with six
writers, and raised a writer's p99 by a third to over a half, 237 steps to 371 there: writers wait
their turn where they would have raced. The model's steps are not time, so whether a wasted stage
costs more than a turn waited is a measurement, M15's. The protocol does not need it.

## Truncate (Q18)

Truncate by epoch holds across tablets, with the epoch in the entry, a stamp in each row, and a
short stack of floors, once four rules about the row and two about the reader are repaired:

1. **A stamp never moves backwards.** A stager does not commit on a row stamped past the epoch it
   read.
2. **A write into a stripe a floor hides writes the hidden units as zeros.**
3. **A reclaimed row is a tombstone**, a sequence past what it replaces and stamped at the floor's
   epoch, and a later floor reclaims it again.
4. **A cut inside a stripe fences that stripe's row first**, at the epoch the truncate is about to
   commit, which moves the row's sequence; the truncate then commits only at the epoch and the
   size it read, and reads and fences again if either moved. A truncate that dies after fencing
   leaves the fence; the next writer moves the epoch past it in the entry, changing nothing else.
5. **A reader reads the entry again** when a row it read is stamped past the entry's epoch.
6. **A default read takes its rows at `Quorum`, after the entry.**

P13's text changed with them. "A write acknowledged after a truncate completed is never hidden by
it" is not what any protocol can give: a write concurrent with the truncate may be cut by it, and
it is then ordered before the truncate whenever it is acknowledged. What holds, and what the model
checks, is a write *invoked* after the truncate completed. Every such write reads the epoch
strongly, so it reads the truncate's.

The alternative Q18 named, an object's rows in one tablet, was not needed.

## The generation and positions (Q19)

A commit names the placement group's generation it was staged under and is refused at any other,
beside the sequence. Positions need no condition of their own: a move's switch records them in the
commit that moves the generation, so a write that names its generation has named them. A move
copies a chunk, then commits the switch on condition that the row's sequence, the generation and
the label at that position are as the move read them; a write committed during the copy refuses
the switch, the move fails, and a later one is asked for. A position the row calls stale is
switched without a copy, and stays stale for a rebuild to fill.

**What the group compares is two fields by equality.** The stamp and a truncate's fence also
decide whether a commit may land, but every command that changes either moves the sequence too,
so a stager judges them on the row it read and the group need not.
`a_commit_compares_only_the_sequence_and_the_generation` holds every generated history of the safe
policy to that. It is what F68's conditional write offers: equality on any number of filter
fields, and nothing else.

## The chunk digest

**The row keeps no digest of each chunk.** The model has no fault a digest catches and a label
and a checksum bound to its place do not:

- **A write lost whole** leaves the holder's chunk under its old label, which the row does not
  name, so a reader treats it as stale and reads around it, and a rebuild writes it whole.
- **A write lost after its holder answered** is a flush that lied, which the failure model puts
  outside the durability assumption, as C13's does.
- **A torn apply** is written again from the staged copy, which outlives it.
- **A write that landed in the wrong place** fails a checksum that binds a unit to its object,
  stripe, chunk and unit ([P15](contract.md#the-contract)).

X10 measured six digests at 64 bytes archived a row; they buy nothing the contract promises.
X14's other use for per-unit checksums, an S3 full-object checksum made by X5's combine, wants
them where the holder keeps them, not in the row.

## What would have changed the design

| Expected to be close | What came out |
| --- | --- |
| A reader of a stripe written continuously cannot finish without a holder keeping a chunk's previous state | It can finish without it, mostly: at two writers 4 to 14 readers in 100,000 runs ran past their bound or failed by name, and 30 in 2,000 at 4+2 with six writers. With it, none did. S9 now keeps the previous state |
| An untouched chunk on a slice that is down cannot count toward `k + f` | It cannot, and neither can one whose holder is believed up. Q16 is settled stricter than it was asked |
| Two stagers starve each other without a reservation | They did not, at any layout, up to six writers a stripe. The reservation stays an optimization |
| Any violation under the safe policy | Ten rules as written broke a clause, and each was repaired locally. None needed a primary, votes among holders, or undo |

## Recommendation

Build M15's stripe write as S7 has it, with these rules, each of which a saved schedule holds:

- **A commit** moves the row's sequence, sets the labels of the touched chunks, records the
  holders that did not stage, and stamps the epoch the writer read, on condition that the row's
  sequence and the placement group's generation are as the writer read them. A stager does not
  propose on a row stamped or fenced past its epoch.
- **A label** is the sequence and a tag a try; the row group's retry table recognises a retry.
- **The acknowledgement** follows `k + f` chunks with evidence in the write's round: a synced
  stage, or an untouched holder's confirmation.
- **A holder** stages new values, never a patch; takes a partial stage only over a label it can
  make, and lets a new stage replace a record it cannot make; keeps every committed record a named
  label stands on; keeps its staged copy until its apply is synced, whatever excludes it; never
  applies a label at or below its chunk's; holds one position of a stripe; keeps a chunk's
  previous state until its next apply; and discards only on a committed fact, read from the row,
  that excludes a record and that no named label stands on.
- **A truncate** fences the stripe its cut falls inside, then commits at the epoch and the size it
  read. A write into a hidden stripe zeros the hidden units. A reclaimed row is a tombstone.
- **A reader** takes the entry, then its rows at `Quorum`; reads the entry again for a row stamped
  past it; moves its row forward for a chunk newer than it; and never accepts one.
- **The leader's no-op** makes a row where there was none. **A rebuild** asks its holder first,
  and stops if it cannot.
- **No reservation** in the protocol. The row keeps **no chunk digest**.

## What X1 does not settle

- **What any of it costs.** Two durable rounds, a confirmation a write's round, a row at `Quorum`
  for every read, a previous state kept: [X3](spikes.md#x3-bytes-through-the-tablet-groups),
  [X8](spikes.md#x8-one-small-write-three-ways) and
  [X9](spikes.md#x9-table-latency-beside-object-work) price Q14, and M15 measures the rest. What
  keeping a previous state costs a partial write in place, a copy of the range it overwrites, is
  not priced; S6 has to.
- **A cheaper default read.** A row at `One` checked against a sequence the entry records for its
  last extension would need no barrier. It was not modelled.
- **[Q17](contract.md#questions-to-answer)'s record.** The model keeps a missed mark a position in
  the row. How many writes a record holds, at what granularity, and how it survives a checkpoint
  are M16's, with X10's figures and X12's.
- **The move's `Both` phase.** [S10](recovery.md#moves) stages on both generations during a move.
  The model's move copies and then switches, and a write that commits during the copy fails it,
  and a later move is asked for. Whether a move under continuous writes ever finishes is not
  checked.
- **The object lane's frames**: who sends a stage is settled, what it is framed as is M15's.
- **The rest of the contract.** P14 (path identity), P18 (bounded metadata) and P19 (what is not
  promised) are not checked here; P19 is the model's scope, one stripe write at a time.

## What it did not model

- **The tablet groups.** The entry and the rows are atomic objects whose replicas answer with a
  committed prefix. P1–P6 are the tablet model's, and a leader change appears here as a stager
  losing its view, a message lost, or a lagging answer.
- **Bytes.** A unit is the write that wrote it; erasure coding's arithmetic is an algebra that
  makes a mixture visible, not Reed-Solomon. A checksum is a unit marked torn or garbage.
- **A whole object's put** and an inline object. The object exists at the start, every chunk
  written by its put.
- **Bounded staging space** on a slice, the grace a retired object's readers get, and session
  tokens.
- **Several objects**, or more than two stripes of one.
- **Time.** Nothing here is decided by a clock, and the progress bounds are steps.

## Related

[S7](write-path.md) for the protocol; [S16](testing.md#the-model) for the model's specification;
[S18](contract.md) for the clauses and their record; [S3](objects.md), [S6](device-store.md),
[S9](read-path.md) and [S10](recovery.md) for the rules repaired here;
[C13](../distributed/protocol.md) and the [tablet model](../features/cluster-harness.md) for the
model this one sits beside; [X2](placement-simulation.md) for the positions this model checks.
