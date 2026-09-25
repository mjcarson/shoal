# 134. A deployed node on the development host failed six fixture tests

## Symptom

The workspace run before this chapter's first commit failed six `cluster_fixture` tests, all the
same way:

```text
---- cluster_fixture_accounts_for_all_cores_and_endpoints stdout ----
Error: ChildFailed("node 0 (Server, pid 3972404) failed: shard 0 failed: IO(Os { code: 98, kind: AddrInUse, message: \"Address already in use\" })")
```

The other five were `control_core_respects_cpuset_and_smt_reservation`,
`fixture_faults_cover_directed_links_and_reconnects`,
`fixture_reports_bound_endpoints_without_port_race`,
`node_identity_persists_and_wrong_cluster_is_refused` and
`standalone_needs_no_peer_or_control_listener`. With the lab's europa node stopped, all six
passed.

## Cause

A fixture server that is not staged into a membership cluster (`Cluster::builder().server(..)`,
the M0 and M1 shape) still gets a `cluster:` block, bootstrapping a cluster of one. The child
filled in that block's ports only from `request.cluster`, which such a node does not have. So it
kept `ClusterConf`'s defaults, 12001 for the peer listener and 12002 for control. Those are the
ports every deployed node listens on ([C14](../../distributed/deploying.md)), and the lab's europa
node is a deployed node on the development host. [Resolved #102](fixture-port-block.md) had moved
every staged node's ports into the fixture's own block below the ephemeral floor. The unstaged
servers were left on the defaults, which nothing else on a development host had bound until now.

## Evidence

**Established by running it**: the six failures above with `tmdb-dataset-node` listening on
`0.0.0.0:12001` and `172.16.2.10:12002`, and the same six passing with it stopped
(`systemctl stop shoal-tmdb`, the six tests, `systemctl start shoal-tmdb`).

## The fix

`ChildRequest` carries `ports`, a data and a control port the parent takes from
`ports::next_pair` for any server it does not stage (`shoal/tests/cluster/node.rs`), and the child
sets them on its block before a staged cluster's ports would (`shoal/tests/cluster_fixture.rs`).
The six tests pass beside the running node.

## Alternatives rejected

- **Port zero for the unstaged servers.** The kernel would pick a free port, but from the
  ephemeral range. Keeping the fixture's listeners out of that range is the point of #102.
- **Documenting "stop any deployed node before running the suite".** The development host is
  also a lab node, and a test suite that needs the lab down to pass is one that will be run with
  it down, which hides the next collision instead of the last.

## Invariants to uphold

- **Every port a fixture child binds comes from the fixture's block, or is zero and reported.**
  A default from `Conf` or `ClusterConf` is a port something else on a development host may hold.

## Still open

Nothing.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| The six tests above (`shoal/tests/cluster_fixture.rs`) | Each fails with `AddrInUse` whenever anything on the host holds 12001 or 12002 |

## Related

- [Resolved #102](fixture-port-block.md), the port block this extends.
- [Distributed cluster testing](../../cluster-testing/findings.md), whose lab put a deployed node
  on the development host.
