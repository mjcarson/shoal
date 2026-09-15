# 112. `certificate_rotation_binds_identity` failed about half its runs alone on the development host

## Symptom

The M10c certificate test failed about half its runs alone, in three shapes: `node 0 did not
join within 60s` with node zero `recovering` and three members up; `OutcomeUnknown: the
replication rpc timed out` at a write; `NotLeader: group … elected no leader within the
deadline` at another. One in four runs against the tree at `16f9a53`, five in eight against the
tree at [Resolved #100](clone-fencing-under-load.md), and it had passed in every six-thread
suite run before that day and failed in some after.

## Cause

The identity-mismatch step reissued node two's leaf naming node one, reloaded it, and
**restarted node zero** so that every handshake between the two was fresh. Node zero refused
node two's hello and node two's dials to node zero failed by name, which is what the step
asserts - but node zero's own rejoin needed the control leader, and after node zero's restart
the leader was whichever of nodes one and two won the election. When node two won, node zero
could not reach it, its observation went nowhere, and `wait_joined` timed out; and the data
groups node two led answered a write through node zero with the other two shapes. Filed by
[Resolved #100](clone-fencing-under-load.md), which met it while running the suite.

## Evidence

**Established by running it**, one failure in four and five in eight as above, and by reading
the step: which member leads after node zero restarts is the election's to decide, and the
step's assertions do not depend on it while its rejoin does. With the fix, eight runs of eight
pass - three alone and five beside two other restarting tests at three threads.

## The fix

The misnamed node is the one restarted. Node two, whose leaf names node one, comes back and
dials nodes zero and one, which refuse it (`identity does not match` on its links); they dial
it back for the groups they lead and fail verifying its leaf (`certificate names node`). The
same at the second step, with a leaf naming nobody (`not authorized`, `names no node`). Node
two cannot join while its leaf is wrong, which is the point, and is not waited on until its
leaf is reissued as itself; then the others' links to it come up, its control link does - its
join - and a write through it is served, which is its data lane. A replication link of its own
is dialled only once it leads something, and a node that came back twice into a refusal may
not for a while, so the test no longer waits for one.

## Alternatives rejected

**Move the control lead to node one before restarting node zero.** No verb transfers the
control lead, and adding one to make a test deterministic is a feature wearing a fix's
clothes.

**Restart node zero after node two's leaf is reissued as itself, and check the refusals with
node zero a live member.** A live member's links to node two were made before the reissue and
stay up until they drop; the fresh handshakes the step needs come from a restart, and the
restart that needs no rejoin through a refused member is the refused member's own.

## Invariants to uphold

- **No step of this test depends on which member leads.** A restart is of the node whose
  rejoin the step can afford to wait for, or not wait for at all.
- **A refused node is not waited on to join until its leaf is right**, and its own replication
  links are not waited on at all: the others' links to it and its control link are what say
  the handshakes are back.

## Still open

- Nothing. The behaviour under test was never wrong; the test's schedule was.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `certificate_rotation_binds_identity` | `shoal/tests/cluster_fixture.rs` | Fails about half its runs on which member leads after node zero restarts |

## Related

[F50. Cluster operations](../../features/cluster-operations.md), the test's feature;
[Resolved #100](clone-fencing-under-load.md), which filed this; [C11. Testing](../../distributed/testing.md).
