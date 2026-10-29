# 243. A failed device's moves gave themselves up, and loaded the control group with slots

A placement group's move is admitted by the control group, which holds a slot for it until its
driver frees it ([F88](../../features/recovery-and-moves.md#moves)). F88's first lab runs failed a
device on titan and found its two to three thousand moves crawling: about as many given up as
switched, the control group committing a slot change several times a second, and a refusal for
every admission a full device could not take. Found by F88's lab run after F88 was committed, and
filed and fixed in one change.

## Symptom

`shoaladm bench`'s `device_fail` arm on a copy of the lab's three nodes, at `402c1bc`, polled with
`pools` every five seconds beside it:

```text
07:05:09 target 6  moves 16  behind 2838  stale 157  rebuilt 18  switched 40  aborted 43  retired 1
07:13:34 target 6  moves 14  behind 2454  stale 912  rebuilt 95  switched 233  aborted 139  retired 1
```

titan's log gave the reason for each early abort as `why="its slot is gone"`, for moves its own
driver had been admitted to a moment before. Every node installed about 150 tablet maps a minute,
on each of six shards, while the moves ran. The fixture's `an_operator_fails_a_device_and_its_groups_move_off_it`,
which counted switches and never aborts, passed throughout.

## Cause

Four things, the first a defect and the rest costs that turned into one.

- **A driver judged its own fresh admission by a map that lagged it.** `begin` was admitted by the
  control group, began the move, and handed it to `resume`, which gives a move up when the shard's
  installed map holds no slot for it. A slot reaches a shard's map only with the control thread's
  next push, so a move begun straight after its admission was often given up at once.
- **Every slot moved the topology version.** `AdmitMove` and `MoveDone` each bumped it, so each
  move rebuilt the tablet map on every node and pushed it to every shard, which reinstalled it.
  A shard routes by nothing a slot says.
- **A full device's refusals were commits.** Nearly every group a failed device held moves onto
  its node's other device, which takes sixteen moves at once. Each shard's driver asked again every
  20 ms for every group behind, and every refused `AdmitMove` was a full commit of the control
  group before it was refused.
- **A move's listings waited for idle slices.** A move lists the old holders other than the moving
  position's, and every request a move makes was a background one, gated by its slice's idle time.
  Under eight 1 MiB operations in flight a slice is rarely idle, and an empty group's move waited
  on three of them.

## Evidence

**Reproduced** in the fixture by an assertion added to `an_operator_fails_a_device_and_its_groups_move_off_it`
that no move gives itself up, run against `402c1bc`'s `moves.rs`:

```text
test an_operator_fails_a_device_and_its_groups_move_off_it ... FAILED
assertion `left == right` failed: 28 moves gave themselves up beside 2047 switched
```

With the first fix alone, 6 to 9, every one `arrival ... is not serving`: the control leader's
detector called two members silent as the moves began, with phi 300, and their health came back
within 0.3 s. With all four, none gave up and no member fell silent, in a fixture run whose moves
took 2.5 minutes. The children installed 59 maps over the test, where each had installed thousands.

## The fix

- **`resume` is told whether the move's slot is held**, by the control thread's own answer: a move
  its driver was just admitted to holds it, and a move found moving is judged by the slots the
  control thread has applied (`ControlRequest::MoveSlots`), never by a pushed map.
- **A slot moves no topology version.** `AdmitMove` and `MoveDone` change the pool map's `moves`
  and nothing a shard reads from its map. `publish` returns before it rebuilds a map whose version
  and leader did not move, which it built and dropped for every applied command before.
- **A driver never proposes an admission a full device would refuse.** It counts the slots
  arriving on each device from the control thread's answer, skips a begin that needs a device
  with sixteen, and takes a device an admission was refused for as full for half a second. The
  control leader refuses an admission its applied slots say will not fit before writing it
  (`refused_before_writing`), so the shards that race for a slot just freed cost no commits but
  the winner's. The slots are read where the state is (`ControlStateMachine::with_state`), never
  through `state`, which copies all of it.
- **A confirmation and a listing are not held to idle time** (`waits_for_idle`). They read a chunk's
  header or a page of a directory and move no bytes; a stage, a write, a read and an apply still
  wait for an idle slice and the device's ceiling.
- **A slot whose group is not moving is freed** by the group's leader, so a `MoveDone` lost after
  its switch or abort committed does not hold a device's admission for good.

## Alternatives rejected

**Admitting moves without the control group.** Each group leader could cap its own moves, but no
leader sees what arrives on a device from another's groups, and a device's arrivals are what the
slot bounds.

**Batching admissions into one command a poll.** It would cut the commits by sixteen and still
commit every refusal; reading the applied slots first refuses nothing.

**Holding listings to idle time and starting fewer moves.** It slows the empty groups, which are
nearly all of a failed device's, to the foreground's gaps without saving the foreground anything
measurable.

## Invariants to uphold

- **A move's slot is judged by the control thread's applied state**, never by a shard's map, which
  no slot change moves.
- **Nothing a shard routes or places by changes without moving the topology version**, and nothing
  else moves it.
- **An admission is proposed only for devices with room**, as the applied slots count them.
- **Only requests that move no bytes skip the idle gate.**

## Still open

- **A load that saturates the lab's 1 GbE links makes the control leader call members silent**,
  moves or none, and a move onto a member called silent is given up and tried again: filed as
  [item 244](../known-issues.md#244-a-load-that-saturates-a-1-gbe-link-delays-the-control-lanes-reports-and-members-fall-silent).
- **Two control commits a move** ([O111](../optimizations.md#o111-a-placement-groups-move-costs-two-control-commits-beside-its-own-three)),
  each about 0.6 s in the fixture, whose control directories are on europa's btrfs root; the
  driver logs one slower than 250 ms (`a move's control proposal was slow`).

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `an_operator_fails_a_device_and_its_groups_move_off_it` | `shoal/tests/cluster_fixture.rs` | Now asserting that no move gave itself up: a driver judging its admission by a lagging map gives some up, and a control group loaded with slots calls members silent |
| `rebuild_waits_for_the_slices_idle_time` | `shoal/tests/device_store.rs` | A rebuild's piece runs beside a foreground one: the gate still holds every request that moves bytes |

## Related

- [F88](../../features/recovery-and-moves.md), whose lab run found it.
- [Resolved #242](stream-spawned-under-borrow.md), the defect the same run found first.
