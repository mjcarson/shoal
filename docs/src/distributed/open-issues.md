# C15. What is still open

## Context

Every milestone is delivered, and every C page describes what runs. This page is the other
half: the defects filed against the cluster, the operations that are explicitly unsupported,
the remainders each decision record said it did not settle, what is measured only at smoke
scale, and what the rewrite of this chapter found wrong in it. Each row links to where it is
filed - [known issues](../appendix/known-issues.md), [todos](../appendix/todos.md),
[optimizations](../appendix/optimizations.md) or an F page - so this page is an index and not
a second copy. A new defect goes to known issues at the next free number, never here alone.

## Defects

| Item | What | Page |
| --- | --- | --- |
| [112](../appendix/known-issues.md#112-certificate_rotation_binds_identity-fails-about-half-its-runs-alone-on-the-development-host) | `certificate_rotation_binds_identity` fails about half its runs alone on the development host, on which member leads when node zero restarts | [C11](testing.md) |
| [106](../appendix/known-issues.md#106-a-member-isolated-on-every-lane-long-enough-to-inflate-its-term-trips-an-openraft-debug-assertion-when-healed) | A member isolated on every lane long enough to inflate its term trips an openraft debug assertion when healed | [C3](membership.md) |
| [109](../appendix/known-issues.md#109-a-volatile-groups-survivor-trips-an-openraft-debug-assertion-when-a-majority-loses-its-memory-log-at-once) | A volatile group's survivor trips an openraft debug assertion when a majority loses its memory log at once | [C5](replication.md) |
| [113](../appendix/known-issues.md#113-glommios-dmafileopen_at-unwraps-statfs-after-a-successful-open) | glommio's `DmaFile::open_at` unwraps `statfs` after a successful open; fixed in the fork's working tree, uncommitted there | [C11](testing.md) |
| [15](../appendix/known-issues.md#15-no-backpressure-anywhere) | The local kanal mesh between a node's shards is unbounded; only the peer lanes are bounded in bytes | [C2](transport.md) |

## Explicitly unsupported

Closed by a decision that says no, with the supported path beside it.

| Not supported | The supported path | Decided |
| --- | --- | --- |
| Issuing or distributing a certificate; a leaf before a node id exists | The id is minted at the first claim, the leaf issued for it by the operator, the node started under `cluster.tls` after | [Q11 at M10c](protocol.md#q11-at-m10c) |
| A schema change as a rolling operation | A new cluster and a restore of a backup or an export | [Q10 at M10a](protocol.md#q10-at-m10a) |
| A marker format migration in place | The build that wrote it serves it, or an export into a new cluster | [Q10 at M10a](protocol.md#q10-at-m10a) |
| Recovery of a lost majority to more than one survivor, or of a set the survivor never held | `force_recover` to one survivor; the rest from a backup | [Q12 at M10b](protocol.md#q12-at-m10b) |
| A restore into a populated cluster, into the cluster that cut the backup, of one table, or to a point in time | Once, whole, into a fresh empty cluster | [Q12 at M10b](protocol.md#q12-at-m10b) |
| A claim of physical N > RF scale-out | Nothing is measured on more nodes than the factor | [C10](performance.md#limitations) |
| A lease read (Q6) | The barrier is the only strong read | [C13](protocol.md#the-questions-and-where-each-was-decided) |
| A hotspot threshold in the planner (Q8's third half) | A single hot partition is as indivisible as it was | [Q7 and Q8 at M9b](protocol.md#q7-and-q8-at-m9b) |
| `Primary` as a read level | `Quorum` is the one strong level; executing at the leader is an additive routing choice, filed | [Q5 at M5](protocol.md#q5-at-m5) |
| A replication factor change, a per-table factor, a same-node move, an operator-chosen destination slot, cancelling a `Decommission`, a `SetPolicy` | None; the grace and the factor are the bootstrap's | [F46](../features/capacity-rebalancing.md#limitations) |
| A tablet split | Nothing schedules one; the token, the identity and the group are designed to survive one | [Q4 and Q5 at M9a](protocol.md#q4-and-q5-at-m9a) |
| Growing a node past the slots it claimed | A `Replace` onto a fresh identity claimed with more | [F47](../features/local-rehome.md#limitations) |
| `One` writes | Refused at validation until an accepted-or-pending result API exists | [Q4 at M4](protocol.md#q2-q3-and-q4-at-m4) |

## Not settled

The remainders the decision record named, one line each.

| Decision | Remainder |
| --- | --- |
| [Q2 at M4](protocol.md#q2-q3-and-q4-at-m4) | The shared WAL's own figure at sixty-four leaders, on the benchmark host |
| [Q3 at M7](protocol.md#q3-and-q9-at-m7) | A snapshot copies the archives rather than pinning them ([O52](../appendix/optimizations.md#o52-a-snapshot-copies-every-record-of-the-archives-into-one-file)); a snapshot is per group, so a returning node installs every tablet its set shares |
| [Q4 at M4](protocol.md#q2-q3-and-q4-at-m4) | Speculation before commit would have to specify dependencies, rollback and separate committed visibility |
| [Q4 at M6](protocol.md#q4-at-m6) | The reconnect backoff and floor are the transport's settings, not the policy's |
| [Q4 at M9a](protocol.md#q4-and-q5-at-m9a) | The identity watermark is replica-local; the retry window is a node's setting |
| [Q7 at M9b](protocol.md#q7-and-q8-at-m9b) | The grace is the policy's, not per member; there is no `SetPolicy`; a `Decommission` cannot be cancelled |
| [Q8 at M9b](protocol.md#q7-and-q8-at-m9b) | Per-device and per-pair budgets; a budget that adapts to the foreground's tail; resident bytes as a weight |
| [Q9 at M7](protocol.md#q3-and-q9-at-m7) | No time budget; a budget per shard rather than per stream; a stream that cannot keep up is fed snapshots repeatedly rather than told to stop |
| [Q11 at M10c](protocol.md#q11-at-m10c) | Nothing issues or distributes a certificate; a reload is per node; a shared leaf carries no identity |
| [Q12 at M8](protocol.md#q12-at-m8) | The interval a scheduled scrub should default to, since the arm ran where every partition was resident |
| [Q12 at M10b](protocol.md#q12-at-m10b) | A backup's shipping, retention and age; the backup arm's full-scale cost |
| [Q13 at M1 and M3](protocol.md#q11-and-q13-at-m3) | Budgets at a hundred members; the report's reachability list, which grows quadratically |

## Measured at smoke scale only

No full-scale capture of any cluster arm is committed; every number on a cluster F page was
taken at a hundredth of the data on the development host under the `powersave` governor
([C10](performance.md#the-numbers-so-far)). The arm families the design named that nobody
built: `groups/{idle,active}` (the spike stands in), `overhead/nodes/2`, `scaleout/nodes/{3,4,6}`,
`writes/{insert,update,delete,conditional,retry}`, a failover by pause or by partition, a
catch-up at several mutation rates, a rehome that grows, a write background under the read arms,
an open-loop schedule, and a restore's cost. The failover objective of base plus two seconds is
not met as set: two to three times the base ([C7](failover.md#the-window-and-what-a-client-sees)),
and measured at four on the development host at base one - the lease of twice the base, then a
timeout ([Resolved #110](../appendix/resolved/dead-primary-write-failures.md)).
A physical capture on unequal hardware has a launcher and a record and no run.

## Filed as unbuilt

The per-feature "left undone" lists on the [todos](../appendix/todos.md#distribution) page are
the complete record; the ones an operator meets first:

- A `shoalctl` verb for `SetControlVoters`, `SetTableReadPolicy` and `Move`; a cluster-wide
  `ReloadTls`; per-tablet readiness and a lag histogram in the cluster tab
  ([F50](../features/cluster-operations.md)).
- Leader hints and deltas on the map; leadership moved toward a reader or back to a returning
  node ([F41](../features/read-consistency.md), [F42](../features/primary-failover.md)).
- A coverage list on the response frame; a cross-tablet snapshot ([F41](../features/read-consistency.md)).
- A learner fed a log tail rather than a whole-group snapshot
  ([O55](../appendix/optimizations.md#o55-a-learner-inside-the-retained-log-is-fed-a-snapshot-when-the-leaders-cached-cut-is-newer-than-its-purge-point));
  a rehome that serves while it runs ([O59](../appendix/optimizations.md#o59-the-rehome-runs-on-one-core-and-blocks-the-start)).
- A failure domain on a member, so voters and copies can be spread over one ([F46](../features/capacity-rebalancing.md)).
- A frame-class fake transport in the fixture; disk-full and torn-archive-write faults
  ([F36](../features/cluster-harness.md), [F44](../features/repair.md)).
- A server binary and a `shoalctl` binary ([C14](deploying.md#limitations)).

## Found while rewriting this chapter

What the pass from a design to a description turned up, and what was done with each.

| Found | Done |
| --- | --- |
| C1's example seed `10.0.0.1:12001` named the data port; a seed is a control endpoint | Corrected to `:12002` on [C1](node-identity.md#the-cluster-block) and [C14](deploying.md) |
| The overview called the marker format 2; it has been format 3 since F39 | Corrected |
| C10's workload table named `groups/{idle,active}`, `overhead/nodes/2`, `scaleout/*` and `writes/*` beside the arms that exist | The table lists what exists; the rest is above |
| C9 said `shoalctl` did not draw replication debt and that there was no backup or restore | Rewritten from the admin kinds |
| Two acceptance rows named two tests in one cell (C9's cluster tab row, C10's remote row), which `acceptance_tables_have_unique_tests_and_valid_milestones` refuses, so the check had failed since F50 | Split into one row per test |
| F39's Limitations still said a certificate is not bound to a node | Struck on [F39](../features/membership.md#limitations) with the F50 pointer |
| F50's Limitations and the M10c section said the certificate test had not been run on this host | Struck; it has |
| C11 said the digest layout self-test was still to come; `canonical_digest_ignores_archive_layout_at_same_boundary` is it | Rewritten |
| The introduction said the chapter was a design record with nothing in it built | Rewritten |
| The cluster tab had no `initialize`, so runbook 1 could not be followed from `shoalctl` | Added, with its test ([C14](deploying.md#shoalctl)) |
| Two pages said the book has no mermaid preprocessor | It has one now; both pages say so |

## Related

[Known issues](../appendix/known-issues.md), [TODOs](../appendix/todos.md#distribution),
[Optimizations](../appendix/optimizations.md), [C13](protocol.md), [Milestones](milestones.md).
