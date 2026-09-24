# 114. The cluster tab counted no voters and no learners in any cluster

## Symptom

`shoalctl`'s cluster tab reported `voters: 0` and `learners: 0` for every cluster it was
pointed at, whatever the control group held. The headline figure and the member rows were
right, since they come from other fields, so the zero read like a small cluster rather than a
wrong number. It surfaced when [F51](../../features/cluster-deployment.md)'s deployment began
waiting on the same model for "every node up and `min(control_voters, n)` voters": a one-node
cluster whose only member's row said `role: voter` never reached its one voter.

## Cause

`ClusterModel::from_frames` (`shoalctl/src/cluster/model.rs`) read `members["voters"]` and
`members["learners"]` with `as_u64`. The `Members` frame is `serde_json::to_value` of the
control plane's `TopologyView` (`shoal-core/src/server/control/plane.rs`, `AdminKind::Members`),
whose `voters` and `learners` are `Vec<NodeId>`, so both fields are arrays of node ids and
`as_u64` answered `None` for each, defaulted to zero. The model's unit test built its frame by
hand with `"voters": 2, "learners": 1`, the shape the reader expected rather than the shape the
server writes, so the reader and its test agreed with each other and not with the server.

## Evidence

**Reproduced**, twice. Against a real node, by `shoal-bench/tests/deploy_render.rs` on its first
run - a bootstrapped single-node cluster whose model read:

```text
the node never came up: ClusterModel { ..., voters: 0, learners: 0, ..., members: [MemberRow {
node: "e9882ab5-...", role: "voter", health: "up", ... }] ... }
```

and with the frame's real shape in a unit test run against the unfixed reader:

```text
test cluster::model::voter_tests::voters_and_learners_are_counted_from_the_lists_the_frame_carries ... FAILED
assertion `left == right` failed
  left: (0, 0)
 right: (3, 1)
```

## The fix

`count` reads the field as the length of its list, and still reads a number as a count, so
a frame that ever carries one is not read as none. The existing model test's frame now carries
lists, which is what the server writes.

## Alternatives rejected

**Count the member rows whose role is `voter`.** That is a second derivation of the same fact
from a different field, and the two disagree during a membership change: `members` is the
committed records, `voters` is the control group's configuration. The tab shows the latter by
name, so it reads the latter.

**Add a `voter_count` to `TopologyView`.** The frame is the server's committed view and the
list is already in it; a count beside a list is a second field to keep equal to the first.

## Invariants to uphold

- **A frame a test feeds the model is the frame the server writes.** Hand-built frames are
  fine, but a field's shape comes from the type the server serializes, not from what the reader
  wants. `deploy_render.rs` reads a real node's frames and is the check that catches the next one.
- **The model defaults, it does not fail.** A missing field is still the default; only a
  present field of another shape was the defect.

## Still open

- Nothing in this item. Every other field the model reads was checked against `TopologyView`'s
  serialized shape when this was fixed.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `voters_and_learners_are_counted_from_the_lists_the_frame_carries` | `shoalctl/src/cluster/model.rs` | Reads `(0, 0)` for three voters and a learner |
| `the_cluster_model_reads_the_admin_frames` | `shoalctl/src/cluster/model.rs` | Its frame now carries lists, so it reads no voters |
| `a_rendered_node_claims_starts_and_initializes` | `shoal-bench/tests/deploy_render.rs` | A real node's model never reaches its one voter |

## Related

[F50. Cluster operations](../../features/cluster-operations.md), which built the cluster tab;
[F51. Cluster deployment](../../features/cluster-deployment.md), which found this;
[shoalctl](../../operations/shoalctl.md).
