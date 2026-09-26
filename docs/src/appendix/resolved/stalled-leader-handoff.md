# 167. A stalling leader's core stopped before its lead could move

## Symptom

In a lab run of [#160](unreadable-partition-stalls-one-copy.md)'s fix, one of the copies that
stalled was titan's copy of group `d7487b4c14e8ebff`, and titan led the group. For the next 16
seconds, about a thousand writes a second of the bench were refused:

```text
Unavailable: writing to group d7487b4c14e8ebff: when Write Apply(LogId { … index: 72872 }):
  … the shard loop dropped an apply batch
```

That was 14,156 updates and 4,697 inserts over the run, all on the one group, through every
node. They stopped when an election moved the lead.

## Cause

`stall_copy` asked openraft to transfer the lead (`transfer_leader`) and then dropped the parked
batch at once. Dropping it ends the state machine's `apply` with an error, and openraft stops the
core on a storage error. The core stopped before the transfer had taken effect, so the other
members still took the dead core for the leader. Every write they forwarded to it, and every
write through titan's core, was answered by the core's fatal error until the members' election
timers ran out.

## Evidence

**Found on the lab, established from the bench's samples and titan's journal.**
`a_stalled_leader_hands_its_lead_on_first` (`shoal/tests/cluster_fixture.rs`) stalls a group's
leader in the fixture. It passes with the old order too: every write it sends is answered, the
first after about 2.5 s, and none is refused. The fixture's elections are fast enough that no write
reaches the dead core, so the test covers the path but not the defect. On the lab, the rerun of the
same fault on the fixed build had **12** `Unavailable` refusals where the run before had **18,853**.

## The fix

A copy that leads when it stalls hands the batch to a task. The task asks for the transfer, waits
until the copy's metrics show another leader, or for `STALL_HANDOFF_WAIT` (3 s), and then lets the
batch go. The core keeps running meanwhile, so it can carry out the transfer, and it applies
nothing. The successor is the voter whose matched log is furthest along, so it can win the election
it is asked to start at once.

## Alternatives rejected

- **Keep the batch, and the core, until a repair.** The copy would keep voting and leading. Writes
  through it would commit and never be answered, and the repair's restart would have to shut down
  a core whose state machine is stuck in `apply`.
- **Wait for the transfer with no bound.** A transfer that never lands, for example to a successor
  that is itself behind, would keep the group committing writes nobody answers. The bound hands the
  last word to an election, as before.

## Invariants to uphold

- **A stalled copy's batch is never answered with its entry applied.** The task only drops it.
- **A stalled leader stops being the leader before its core stops, or within the bound.**

## Still open

Nothing of this item. The 12 refusals left on the lab were the transfer's own moment.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_stalled_leader_hands_its_lead_on_first` (`shoal/tests/cluster_fixture.rs`) | Runs a stalled leader's path and asserts no write reaches its dead core. It does not reproduce the lab's window, as above |

## Related

- [Resolved #160](unreadable-partition-stalls-one-copy.md), the stall.
- [Resolved #139](leadership-handoff-on-stop.md), the hand-off a stopping shard makes.
