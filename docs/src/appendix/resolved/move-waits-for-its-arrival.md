# 281. A placement group's move gave itself up when its arrival's node was called down for a moment

A pool's placement groups move to its target generation one move a group
([F88](../../features/recovery-and-moves.md#moves)): the control group admits the move and holds a
slot for it, the move begins, its rows and stripes are copied onto its arrivals, and once every
arrival is serving the switch commits the new generation and positions. A move given up leaves the
group where it was, for a later move to copy again.

Filed from F95's workspace run (2026-10-20); fixed in the flaky-test plan's first wave (2026-10-27).

## Symptom

`an_operator_fails_a_device_and_its_groups_move_off_it` failed in twelve loaded runs, from F90's to
W4's, and in a run of three rows of moves at three threads: *N moves gave themselves up beside*
about 2,050 *switched*, N from 1 to 12. Nothing had failed but the device the row fails on purpose,
and the child logs of the three-thread run gave every give-up's reason as an arrival not serving. A
move given up after its copy costs that copy again, and on the lab, where a full link delays the
control lane's reports ([item 244](../known-issues.md#244-a-load-that-saturates-a-1-gbe-link-delays-the-control-lanes-reports-and-members-fall-silent)),
a member is called down for a moment more often than in the fixture.

## Cause

Before its switch a move looked once at whether every arrival was serving: its device not failed,
and its node `Up` in the installed map (`Mover::serving`). A node the control leader's detector
called down for a moment under load failed that look, and the move was given up at once
(`moves.rs`, *arrival … is not serving*), its copy done. Every other step of a move rides out what
passes: a copy that fails is tried `COPY_TRIES` times, and F88's own rule is that nothing moves off
a down node inside its grace. Only the switch's look was final.

## Evidence

**Established by the loaded runs' child logs and reproduced.**
`a_move_waits_out_an_arrival_down_for_a_moment` was written first. Every node's move drivers find
their first 64 moves' arrivals down at the first look before the switch, as a node called down for
a moment is found (`FIND_ARRIVALS_DOWN`, `faults::find_arrivals_down`), and node two's first device
is failed by an operator, so about two thousand groups move. On the tree before the fix:

```text
assertion `left == right` failed: 192 moves gave themselves up beside 2078 switched
```

Every first look the row armed, 64 on each of three nodes, gave its move up.

## The fix

**A move whose copy is done waits for an arrival that is not serving**, up to `SWITCH_WAIT` (30 s),
reading the map as it is installed every `SWITCH_POLL` (500 ms). An arrival serving again lets the
switch go on; one still not serving at the end of the wait is a node that is down, and the move is
given up as before. The wait is inside the move's task, so the copy is not run again.

## Alternatives rejected

- **Give the move up and let the next poll begin it again.** That is the cost this fixes: every
  stripe copied again.
- **Return and let the next poll resume the move.** A resumed move runs its copy again before the
  switch, a pass over the group's rows and stripes for each look.
- **Judge an arrival not serving by how long its node has been down in the map.** The map carries a
  member's health, not since when, and the move's own clock is enough to tell a moment from a node
  that stays down.
- **Wait for the node's grace.** It is minutes, and a move holds its device's slot while it waits,
  of which a device has sixteen.

## Invariants to uphold

- **A move switches only once every arrival is serving**, as the map installed when it looks says.
- **A move whose arrival stays down past the wait is given up**, never switched onto a node that is
  down.
- **The wait never runs the copy again.**

## Still open

- A node down for longer than 30 s and shorter than its grace has its moves given up and begun
  again once it is back, as before.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_move_waits_out_an_arrival_down_for_a_moment` (`cluster_fixture`) | Every move whose first look found its arrival down gives itself up: 192 beside 2,078 switched |
| `an_operator_fails_a_device_and_its_groups_move_off_it` (`cluster_fixture`) | Under load, the moves whose arrival's node was called down for a moment give themselves up, as in its twelve loaded runs |

## Related

- [Item 244](../known-issues.md#244-a-load-that-saturates-a-1-gbe-link-delays-the-control-lanes-reports-and-members-fall-silent),
  why a member is called down for a moment on the lab.
- [Resolved #243](move-slots-on-the-map.md), a move that judged its own admission by a map
  that had not reached it yet, which the same row counts.
- [F88](../../features/recovery-and-moves.md).
