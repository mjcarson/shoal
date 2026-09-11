# C1. Nodes, identity, and the cluster configuration

## Context

A node needs a persistent identity independent of its address, a cluster identity, and an embedded
control plane. Ordinary data work remains on Glommio shards. Membership and failover never
require a separately deployed service; [C13](protocol.md) states that constraint and the open gates.

## What exists today

`StorageMeta` (`shoal-core/src/server/meta.rs`) records format and shard count. `Resources::cpus`
(`server/conf.rs`) excludes logical CPU 0, but does not reserve its whole physical core. The
schema fingerprint detects mismatched generated schemas and currently includes the protocol
version. Networking has a client address/port and optional TLS, but no advertised peer identity.

## The design

### Two identities

Mint random NodeId once for an empty node directory and ClusterId once at explicit cluster
bootstrap. Persist both before admission. Seeds are discovery addresses, not identities. A
joiner adopts the authenticated cluster identity, and a directory naming a different cluster
is refused. An established directory with unreachable seeds must not create another cluster.

A directory lock prevents two local processes using one path; cloned disks require an additional
cluster-wide incarnation/fencing protocol (Q11). Address changes are authenticated membership
updates. A Removed identity remains tombstoned and must use an explicit replacement/import flow
rather than resume old voting state.

### The storage marker, format 2

The marker records node/cluster identity, storage format, shard-layout version and last observed
topology version. Tablet term/vote, committed configuration and checkpoint/log boundaries live
in their own durable manifests, not one node-wide topology counter. Wire, schema and disk-format
versions are separate concepts. Their exact encoding and upgrade compatibility are Q2/Q10.

Refuse unknown formats with instructions identifying supported migration/export tools. Before
operational readiness provide a tested path from existing single-node data to a cluster, including
checksums, row counts, cutover and rollback boundaries. An initial development build may refuse
format-1 data, but “delete the directory” is not a production upgrade procedure.

### The cluster block

Proposed configuration (not implemented):

```yaml
cluster:                          # absent: standalone local operation
  bootstrap: false               # explicit first creation only; no automatic bootstrap on seed loss
  seeds: ["10.0.0.1:12001"]
  advertise: "10.0.0.2"           # usable peer address; never default to 0.0.0.0
  port: 12001                     # data peer endpoint
  control_port: 12002             # independent control listener, included in seed discovery
  client_advertise: "10.0.0.2:12000"
  control_core: 0                 # validated against allowed CPUs; reserve its physical siblings
  control_voters: 3               # explicit policy; five supported where justified
  replication_factor: 3
  write_consistency: Quorum
  read_consistency: One
  failure_detector:
    interval_ms: 500
    phi_threshold: 8.0
  primary_failover_after: "5s"    # base data-election timeout, not a read lease or post-Down delay
  auto_remove_after: "30m"        # proposed default; null explicitly disables automatic removal
  admins: []
  tls:
    cert: "/etc/shoal/node.pem"
    key: "/etc/shoal/node.key"
    ca: "/etc/shoal/cluster-ca.pem"
```

The data peer seed endpoint returns the authenticated control endpoint during discovery; stored
control peers subsequently reconnect directly even if data shards stall. Q11 settles certificate
identity encoding and address changes before this handshake is finalized. Separate control and
data listeners must not create two independently configured membership systems.

Use `deny_unknown_fields`. Separate local resources/endpoints from cluster policy: the bootstrap
configuration seeds RF, defaults and grace policy into control-plane state; later changes are
versioned admin operations. Joining nodes cannot silently redefine those defaults with local YAML.
Reject incompatible local storage durability settings for admitted table policies.

Additional settings are specified by their owning pages before implementation: bounded peer and
pending bytes (C2/C5), read/write deadlines (C5/C6), snapshot/retention/transfer/disk budgets (C7/C8),
capacity/failure-domain weights (C8), maintenance and repair scheduling (C9). C13 records unresolved
values and gates. Per-table overrides require table identity and supported transitions; unknown
or unimplemented overrides fail validation.

RF=3 is a desired configuration, not a synonym for currently Up nodes. An initial node can expose
admin and incomplete readiness while waiting for enough distinct ready replicas. An explicit
standalone or RF=1 mode has its own weaker redundancy contract.

### The control-plane thread

Default to a current-thread Tokio runtime for embedded `openraft`, detector/admin work and the
leader's rebalancer. CPU 0 is the default, but validate it against container/cgroup affinity and
allow explicit allocation. Reserve a physical core including SMT siblings where isolation is
claimed. On small machines an explicitly shared core is allowed and recorded as shared.

Control networking is owned by this runtime on its control listener; data networking belongs to
shards. Exchange immutable metadata and bounded messages over channels. A shard never waits on
the control plane for an ordinary write/read, and a data-shard stall must not stop control pings.
Blocking storage operations must not block the control runtime's timers.

Standalone mode does not require a Raft group or control listener. A cluster node runs data
consensus under the owning shard/runtime adapter chosen in Q1. A metadata majority does not
replace that data protocol.

### The address of a shard

Use `(NodeId, shard_id)` in placement and logs, with table-qualified tablet identities in data
messages. A node advertises separate client, data peer and control endpoints. File paths remain
node-local and may keep shard prefixes; changing shard counts is gated by M9c's safe rehome.
Short identity strings are display conveniences, never protocol keys.

## Alternatives rejected

Address-derived identity, implicit rebootstrap, globally fixed CPU 0 in every process, and one
node-wide topology version as proof of tablet freshness. External coordination is excluded.
Format refusal is a safety check, not a substitute for a data migration/runbook milestone.

## What it costs

Control CPU and metadata storage, separate control/data peer endpoints, certificates, startup
reconciliation and expanded manifests. Shared SMT/CPU/device resources can affect data performance;
C10 measures that rather than describing the reserved core as inherently free.

## What it breaks

Configuration, storage metadata, startup readiness and diagnostics. Standalone behavior and
performance must be compared against an unchanged matched baseline; no frozen capture is rewritten.
The existing shard-layout refusal remains until rehome is implemented.

## Invariants to uphold

- Identities and terms cannot be guessed from addresses or stale topology markers.
- Existing directories never bootstrap independent authority on seed failure.
- Core allocations respect actual allowed CPUs and record any sharing.
- Cluster policy has one committed authority; local YAML cannot weaken quorum durability.
- Neither runtime depends on an external membership/failover service.

## Prerequisites

[C13](protocol.md) Q1/Q2/Q10/Q11. Fix
[item 65](../appendix/known-issues.md#65-two-gxhash-majors-and-partition-keys-hashed-by-the-one-without-deterministic)
before cross-node routing: stable hashing across supported builds/architectures is required.

## How it would be measured

Standalone versus one-node cluster with matched hardware/core budgets, plus idle metadata CPU.
C10 includes disjoint emulated control cores and intentionally shared-core cases separately.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `node_identity_persists_and_wrong_cluster_is_refused` | Restart retains identity; wrong-cluster seed never rewrites it | M1 |
| `unknown_configuration_and_storage_formats_are_refused` | Errors name the unsupported setting/format and migration path where one exists | M1 |
| `control_core_respects_cpuset_and_smt_reservation` | Restricted affinities and multiple processes cannot silently overlap reserved resources | M1 |
| `standalone_needs_no_peer_or_control_listener` | Absent cluster block retains standalone deployment shape | M1 |
| `documented_cluster_defaults_match_policy_bootstrap` | Defaults include Quorum/One, three voters and finite configurable auto-removal grace | M1 |
| `duplicate_node_identity_is_fenced` | Concurrent cloned identities cannot both join/serve as the same replica | M3 |
| `single_node_data_has_a_verified_cluster_migration_path` | Supported conversion/import preserves data and records cutover/rollback boundaries | M10 |

## Related

[C2](transport.md), [C3](membership.md), [C13](protocol.md),
[Configuration](../getting-started/configuration.md), [Storage marker](../appendix/resolved/storage-marker-format.md).
