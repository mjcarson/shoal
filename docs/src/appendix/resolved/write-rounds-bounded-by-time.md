# 312. A write in place gave up on its stripe after sixty-four rounds, however short they were

A write in place commits each stripe it touches by a conditional write on the sequence of the
stripe's row that it read ([F86](../../features/replicated-pools.md)). When another write commits
first, this one loses: its commit is refused, or its holders are found past the row it read. It then
reads the row again and tries again. That is the server's work, and a client sends the write once.
The rounds were bounded by a count.

Filed from F109's rotational pass (2026-10-24); fixed in the flaky-test plan's first wave
(2026-10-27).

## Symptom

`small_writes_from_eight_writers_on_one_stripe` has eight writers each writing 4 KiB pieces of their
own 128 KiB of one 1 MiB object, forty times each, each write sent once. With every device a disk
(`SHOAL_TEST_ROTATIONAL=1`) a write was answered *the stripes of contended.bin moved under this write
too often; try it again* (`Busy`). That happened in F109's rotational pass and, alone on disks, in
four of eight runs at F109's build and four of eight at the commit before it. On SSDs the row passed
every time.

## Cause

`write_region` gave up after `MAX_TRIES * 4`, sixty-four rounds, whatever they took. A round reads
the stripes' rows at `Quorum`, confirms or stages on every holder, and proposes. A round is lost when
another write commits between its read and its proposal. The longer a round, the more often that
happens. Before [Resolved #313](record-chains-without-recursion.md), a disk's confirmation walked a
chunk's chain of records reading its header at every level, and the eight writers' small writes left
chains hundreds of records deep. So rounds on disks were long, and one writer could lose sixty-four
of them in a row to the other seven. A client waits sixty seconds on an attempt that makes no
progress (`Deadlines::object_attempt`). On an SSD the same sixty-four rounds took 51 ms.

## Evidence

**Established by running it, then reproduced.** The `Busy` answer now names how each round ended
and how long they took. That form was applied to F110's tree, the last before #313, and the row run
eight times on disks. One failed:

```text
writer 3 round 17: Object { …, outcome: Busy, msg: "the stripes of contended.bin moved under this write too often, 64 rounds in 6.591530073s: 43 commits refused, 0 short with the row moved, 0 placements moved, 0 rows made, 21 holders ahead, 0 fenced, 0 stamped; try it again" }
```

Each round was lost to another writer, about a tenth of a second apiece: by a commit first, or by
holders already past the row it read. Nothing failed, and nothing was waiting on a disk's apply
batch. The same eight runs at #313's commit all passed, as did eighteen alone and three runs of the
object suite at six threads at the tree before this fix. #313 had shortened the rounds enough to hide
the count, not removed it.

`a_write_that_keeps_losing_its_stripe_tries_for_its_time` was written first. A fault refuses a
write's next hundred stripe commits before they are proposed, as another writer's commit would be
(`faults::refuse_stripe_commits`, by path). On the tree before the fix:

```text
a write that lost 64 commits: Object { …, outcome: Busy, msg: "the stripes of losing.bin moved under this write too often, 64 rounds in 50.85039ms: 64 commits refused, 0 short with the row moved, 0 placements moved, 0 rows made, 0 holders ahead, 0 fenced, 0 stamped; try it again" }
```

## The fix

- **A write tries its stripes again for as long as one of its own queries may take**
  (`Meta::wait`, the node's query deadline and a second), never for a count of rounds. Past that,
  it is answered `Busy` as before.
- **A write that has lost eight rounds in a row paces the next one.** It waits a draw under a bound
  that doubles with each further loss, from a millisecond up to 50 ms (`pacing_after`). Writes that
  keep meeting on a stripe spread out rather than meet again, and a write's time is not spent
  spinning.
- **The refusal names its rounds:** how many there were, in how long, and how each ended
  (`RoundTally`: commits refused, short with the row moved, placements moved, rows made, holders
  ahead, fenced, stamped). The reason a stripe was tried again rides `StripeStep::Retry`.

## Alternatives rejected

- **A larger count.** Sixty-four rounds were 51 ms on one host and 6.6 s on another. Any count is a
  guess at how long a round takes.
- **Back off by the pool's apply batch on a rotational pool**, the item's first direction. The tally
  shows the rounds lost to other writers, not to an apply batch, and an SSD's rounds would have been
  lost the same way had they been longer.
- **Pace from the first loss.** Most writes that lose win the next round, and the eight writers'
  row takes as long with pacing from the ninth loss as with none (11.7 to 12.4 s against 11.9 to
  12.3 s on SSDs, 11.8 to 13.8 s against 11.8 to 12.1 s on disks).
- **Queue a stripe's writers in turn.** Fair, but the leader of the stripe's row would have to hold
  the queue, and the conditional commit already orders them.

## Invariants to uphold

- **A write's retries are bounded by time, never by a count of rounds.** The time is one of its
  own queries', which a client's attempt outlasts.
- **A write that keeps losing waits between rounds**, so its time is not a spin of strong reads
  against the stripe it lost.
- **A refusal for contention says what the contention was.** The tally is the only record of a
  write's rounds.

## Still open

- **A read's rounds are still a count.** A read whose stripes move under it sixteen times is
  answered `Busy` (`MAX_TRIES`), and so are the decide and put loops. None has been seen to fail,
  and none was changed here.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_write_that_keeps_losing_its_stripe_tries_for_its_time` (`objects.rs`) | A write that loses a hundred commits in a row is answered `Busy` at the sixty-fourth, in 51 ms |
| `a_losing_write_paces_its_rounds_under_a_doubling_bound` (`shoal-core`, `shard::objects::write`) | A losing write waits from its first loss, waits past the most, or every wait is the same |
| `small_writes_from_eight_writers_on_one_stripe` (`objects.rs`) | On disks before #313, a writer that loses sixty-four rounds to the other seven is answered `Busy` |

## Related

- [Resolved #313](record-chains-without-recursion.md), which shortened a disk's rounds and so hid
  the count.
- [Resolved #239](record-bases.md) and [#240](moved-row-not-ready.md), the other ways the eight
  writers' row failed.
- [F86](../../features/replicated-pools.md), the write in place and its conditional commit.
- [Tests That Fail Under Load](../flaky-tests.md).
