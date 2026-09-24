# F50. Certificate rotation, the cluster tab, the runbooks and the physical cluster

## Context

The last third of [M10](../distributed/milestones.md#m10-operations-and-the-real-cluster) is
the part of "operations" that is neither an upgrade ([F48](rolling-compatibility.md)) nor a
recovery ([F49](backup-and-recovery.md)): the pieces an operator meets between them. Four had
been left open by name. [F38](inter-node-transport.md) wrote a `shoal-node://<id>` URI SAN
into every fixture certificate and read it nowhere, so `PeerRefusal::IdentityMismatch` and
`Unauthorized` were defined and never produced and Q11's certificate half stayed open through
five milestones; nothing reloaded a certificate, so a rotation was a restart. `shoalctl` was a
query tool with no view of the cluster it queried, though every admin frame it would need had
existed since [F39](membership.md). [C9](../distributed/operations.md)'s runbooks were twelve
items of intent with "At M9b" notes under them and no page an operator could follow.
And every cluster capture had been three processes on one machine, honestly marked `emulated`,
with the driver's environment recorded and nothing about the nodes' - so C10's physical
environments row had nothing to read, and no capture could say which machine a node ran on.

Two of those turned up defects the runbooks would have hit first. A member restarted at
another address was observed at its higher incarnation with its new record - the M3 rule -
and then never heard from again: the control group's replication dialled the address in the
*membership's* node data, which is the record the member was admitted with, so the leader
replicated into a closed port until the member fell silent. The M3 fencing test had passed
over the same gap for seven milestones: the clone that wins an identity from another address
was never replicated to afterwards, and once the leader did reach it, its log - a copy taken
before the entries the earlier run had acknowledged - was a reversion the control group's
configuration forbade, which stopped the leader's control thread on a debug assertion. And a
`Removing` member still draining was pinged by the leader every second whether or not it was
gone for good.

## What it does

### A certificate is bound to the node it claims, on both ends of every lane

Under `cluster.tls`, every peer handshake now ends by reading the leaf the authority verified
(`Handshaker::peer_leaf`, `Established.peer`) and the first `uniformResourceIdentifier` in its
`subjectAltName` of the form `shoal-node://<uuid>` (`shared::tls::node_identity_of`, a walk of
the DER with `yasna`, which `rcgen` already brought into the tree). What the leaf said is a
`PeerIdentity`: `Node(id)`, `Unnamed` for a chain with no node in its names, or `Plaintext`
where there was no certificate. Then the hello is judged against it (`bound_identity` in
`peer/handshake.rs`): a leaf naming another node than the hello claims is refused
`IdentityMismatch`, a leaf naming none is refused `Unauthorized`, and both are refused at the
listener - before the cluster, the membership or the version is looked at, since a claim the
certificate contradicts is not a claim - *and* by the dialler, which judges the acceptor's leaf
against the node it dialled before it says a word, and against the node that answered when it
dialled a seed. `cluster.tls.bind_identity` is on by default; off, the chain alone is trusted,
which is what every build before this checked, and the page says why an operator would turn it
off (a certificate shared by every node, which is not a node's identity and never was).
`ShoalError::CertificateIdentity { claimed, certified }` says which it was.

### A certificate is rotated on a live node, whole or not at all

The peer lanes' material is read once by the pool into a `PeerTlsHolder`
(`shared::tls`), which every executor's listener and every link read *at each handshake*
rather than keeping a copy. `ShoalPool::reload_tls` and the admin verb `ReloadTls` - node-local,
needing an admin principal, committing nothing - read the same paths again, build **both**
configs, and swap the pair only if both built: a key that does not parse leaves the node on the
material it had and answers why. Every handshake after the swap presents and checks the new
material; every established connection keeps its keys, which the kernel holds, so nothing is
dropped by a reload. The report says how many certificates the chain holds, how many
authorities the bundle holds, and which node the new leaf names, so an operator reads back
what they installed before anybody dials it. `ca` may be a bundle, which is the whole of the
authority rotation: trust both, reissue every leaf under the new one, retire the old.

### A member's address changes at a restart, and the cluster follows it

The Q11 rule from M3 stands: an address changes at a restart under a higher incarnation, the
member's new record is observed and committed, and a clone left at the old address is the same
identity at the same incarnation from another address, refused as a duplicate. What this page
adds is that the cluster *reaches* the member afterwards. The control network keeps the
committed records' addresses (`PeerNetwork::note_addresses`, written on every apply) and every
`ControlPeer` looks its link up by the member's current address at each RPC rather than holding
the link the library handed it a record for, so replication follows a member the moment its
observation applies. The leader also writes the moved addresses into the membership's node data
(`ChangeMembers::SetNodes`, `maybe_readdress`, one member at a time and never during a joint
configuration), so a leader elected later - whose first clients are built from the membership -
dials the right place too. The data and replication lanes already re-dialled from the pushed
map; the control lane was the one that did not. A member that won its identity from a copy of
its directory holds a shorter log than its earlier run acknowledged, and the control group now
allows that reversion the way the data groups have since
[#99](../appendix/resolved/durable-log-reversion.md): the leader resets the member's progress
and feeds it again. A tombstoned member is not pinged.

### A cluster tab in `shoalctl`

`space c` opens a cluster tab (`shoalctl/src/cluster/`). Its content is a `ClusterModel` built
every second from six admin reads through the connection the app has - `Members`, `Readiness`,
`Replication`, `Plans`, `Backups`, `Recoveries` - and rendered as lines: the cluster, the node
reached, the version and the leader; the headline an operator reads first, which is the M9b
figure said in one line (`2 of 3 copies, awaiting 1 member, 4 sets under-replicated; writes
refused: have 2 need 2`); the activated wire and the range the members speak; what this node
hosts and leads, its widest lag, what it is installing and has quarantined; every member with
its role, health, phase, incarnation, grace remaining, weight, free and held bytes, wire and
client address; every open plan with its steps moved and its blocked reason; the backups and
the recoveries. Every field is read defensively, so a frame from an older build leaves a
default rather than failing the tab.

Its command line takes an operation (`ClusterAction`): `initialize <node> [<node>...]` (the
placement, once, in the order typed), `decommission <node>`, `remove <node>
[replacement]`, `maintenance <node> on|off`, `rebalance`, `repair <table> [verify|repair]`,
`backup [table] <dir>`, `restore <dir>`, `activate <wire>`, `status <op>`, `reload-tls`, and
`help`. The first `Enter` on a mutation renders a **preview** under the model naming the
identity it touches as the model knows it, what will move, and the boundary that cannot be
undone (`decommission …: once every set has moved the identity is tombstoned and never
returns`); the second `Enter` on the same line sends it against the version the model was built
at, and `Esc` forgets it. An applied operation is then **followed** by its record - a plan, a
repair, a backup, a restore or a move - at the same cadence as the poll, its steps or groups
drawn under the model until the record says it is done. The model, the parses, the previews,
the requests and the follow-ups are pure and tested; the drawing is not.

### The runbooks

[`docs/src/operations/runbooks.md`](../operations/runbooks.md): C9's twelve procedures and the
two this milestone added, each as the exact operation, the configuration keys, what to wait on
and where the rollback point is. C9's list keeps its "At Mx" notes and points at the page.

### Every node's environment on a capture, and a node on another host

`shoal-workload serve` reads the machine it runs on as it comes up - hostname, CPU model and
count, governor, kernel, memory, SMT, NUMA nodes, the filesystem and device under its storage
directory, and a digest of its own binary (`fingerprint::node_environment`) - and prints it on
its ready line after the address. The parent reads it off the line (`environment_of`), refuses
a peer whose binary digest is not its own (a cluster is measured on one build, and a wrong
binary in a remote directory is the mistake this catches), and `placed_facts` records every
node's `NodeEnvFacts` under `cluster.environments` in node order, with `emulated` now
*derived*: false only when the hostnames differ. `compare` names the first node whose machine
differs between two captures and the fields that do (`environments_difference`), so a
physical capture is judged comparable node by node and not by the driver's machine alone; the
explorer index mirrors the record.

`shoal-bench run --remote <index>=<user@host>:<dir>` (repeatable) puts a node of every placed
arm on another host: the directory there holds a `shoal-workload` of this build and the
`shoal.yml` it resolves from, the node's staged file - which now carries its marker, since the
driver cannot write into another host's directory - is copied over with `scp`, the node is
started over `ssh` and writes a pid file the launcher kills it by, it listens on every interface
and advertises its host, and the other nodes dial it there and dial node zero at
`--driver-address`. A stale directory from an earlier capture is wiped by the node itself when
the marker it finds names another identity, which is the driver's wipe done remotely. Node zero
is always the driver's own process. `SHOAL_REMOTE_SMOKE=<user@host>:<dir>` runs a smoke capture
of the three node arm with node one on that host and checks the record; unset, the test says
so and passes.

### The gate: every M10-named debt

M10 asked for "all earlier open production gates resolved or explicitly unsupported". What the
earlier pages named for M10, and what became of each:

| Debt | Named by | Now |
| --- | --- | --- |
| The certificate-to-node binding | F38, Q11, C2 | **Resolved here**: the SAN is read and judged on both ends |
| Certificate and authority rotation | C1, Q11 | **Resolved here**: `ReloadTls`, a bundle, and a test across a live cluster |
| An address change authenticated | C1, Q11 | **Resolved here**: the M3 rule plus the cluster following the move; the certificate is the authentication |
| First-boot certificate provisioning before a node id exists | Q11 | ~~**Explicitly unsupported**: a leaf is issued for a node id, so the id comes first - `StorageMeta::claim` on an empty directory mints it, and the operator issues the leaf for it before the node joins; a node with no leaf yet runs plaintext or not at all. The runbook says so~~ **Resolved by [F51](cluster-deployment.md)**: the node program's `claim` prints the id before the first start, and `shoalctl cluster` issues the leaf for it |
| Rolling wire compatibility and activation | C2, C9 | Resolved by [F48](rolling-compatibility.md) |
| A schema change as a rolling operation | Q10 | **Explicitly unsupported** by F48: a new cluster and a restore |
| A marker format migration in place | C1, F37 | **Explicitly unsupported** by F48: the build that wrote it, or a restore |
| Backup and restore | C9 | Resolved by [F49](backup-and-recovery.md) |
| Permanent quorum loss | C9, Q12 | Resolved by F49, to one survivor; several survivors **unsupported** |
| Single-node data | C1 | Resolved by F49 as an export restored |
| Runbooks and a TUI | C9 | **Resolved here** |
| A physical capture with per-node facts | C10 | **Resolved here** as the record and the launcher; the capture itself is the benchmark host's to take, and none is committed by this page |
| Physical N > RF scale-out | C10 | **Explicitly unsupported** as a claim: no capture makes it, and `emulated` says whether one could |
| Item 100, fixture failures under load | known-issues | **Open**, not a gate: every failure passes alone, the suite is run at six threads, and the count says so |
| Item 102, a deferred fixture node losing its reserved port | known-issues | **Open**, not a gate: the fixture's port reservation, never a server's |
| Item 106, a debug assertion on a member healed after long isolation | known-issues | **Open**, not a gate: a `debug_assert` in the library on a term inflated by isolation, met by no row at six threads and absent from a release build; the runbook for a control quorum names a restart as the way back |
| Item 107, two table tests one run in five | known-issues | **Resolved** with item 91 ([Resolved #91, 107](../appendix/resolved/compaction-retry.md)): the compactor dying on the tests' fault, not a test's timing |
| Item 109, a volatile group's survivor when a majority loses its memory log at once | known-issues | **Open**, not a gate: an ephemeral table's rows are memory by contract, and the recovery this milestone built reinitializes a volatile group from the surviving members |
| Item 110, the kill arm's client failing a steady share while node one is dead | known-issues | **Open**, not a gate: the arm's client does not retry by design, so the record shows the outage rather than hiding it |

Nothing on the list was fixed silently; a debt not on it was never named for M10.

## Design choices

- **Bind on both ends.** The listener's check alone would let a member that presents another
  node's leaf be accepted by a dialler that checks only the chain; the dialler's check alone
  would let a joiner claim any node. Both read the same function.
- **Bind at the handshake, not at the join.** M2 deferred the binding to the joiner; the join
  is one door of four, and a certificate is a property of every connection. The hello is judged
  by it before anything committed is consulted.
- **Reload swaps a pair, never a half.** A server config on new material with a client config
  on old would have a node accepting under one leaf and presenting another; both are built
  first and swapped together, or neither.
- **The holder is read at every handshake.** The alternative - a message to every executor to
  swap its copy - is what the plan sketched; a shared holder with one `RwLock` read per
  handshake is simpler, reaches every executor at once, and a handshake is rare enough that the
  lock is never contended.
- **Addresses are followed at every RPC, and also written into the membership.** The first is
  what makes the running leader reach a moved member now; the second is what makes the next
  leader reach it. `SetNodes` was chosen over `AddNodes` because the node is already in the
  membership; the library warns that `SetNodes` used wrongly can split a group, which is why it
  is only ever written with the *committed* record of a member the state names.
- **The cluster tab is a model, then a view.** Everything an operator reads is computed from
  the frames into lines a test can assert on; the terminal gets the lines.
- **Preview, then submit.** An operation that tombstones an identity is typed once to read and
  once to send; `status`, a read, is sent at once.
- **The environment is read by the node.** The driver could `ssh` and read `/proc` on a remote
  host; the process that becomes the node already runs there, and it reads its own machine
  where the driver reads its own.
- **The binary digest refuses a mixed build.** A remote directory holding yesterday's
  `shoal-workload` would otherwise measure a cluster of two builds and record it as one.
- **`emulated` is derived.** It was a constant `true`; now it is what the hostnames say, so a
  physical capture is marked by evidence and a one-machine one stays honest.

## Alternatives rejected

- **A certificate per cluster rather than per node.** One leaf shared by every node proves
  membership in the authority's set and nothing about which node is speaking; the SAN was
  written at M2 for exactly this. `bind_identity: false` keeps the shared-leaf deployment
  possible and says what it gives up.
- **Reload by restart.** It is what worked before; it drops every connection the node holds and
  costs an election on a leader. A reload costs nothing that was up.
- **Rebuilding the replication streams on a `SetNodes`.** The library keeps a stream to a
  target it already has, so writing the address into the membership alone did not reach a
  moved member; following the committed address at every RPC does, and the write is kept for
  the next leader.
- **A cluster tab that draws widgets.** A table widget per section would be prettier and
  untestable without a terminal; lines are what the tests read.
- **A separate `shoalctl` binary for the cluster.** The schema is a compile-time construct
  ([F15](client-server-split.md)) and the admin frames ride the same connection; a tab is one
  key away.
- **Reading a remote host's environment over ssh from the driver.** It would read the machine
  the launcher reached, which is the machine the node runs on only by assumption; the node's
  own reading is the fact.
- **A remote launcher that copies the binary.** It would hide a mismatch by construction and
  cost a copy of the binary per node per capture; the operator places it once, and the digest
  refuses a stale one.

## Limitations

- **The binding needs a leaf per node.** A deployment whose nodes share one certificate has to
  set `bind_identity: false`, and then a member can speak as another with the shared leaf, as
  it always could. The runbook says which to choose.
- ~~**First-boot provisioning is manual.** The node id is minted at the first claim; the leaf
  for it is the operator's to issue before the node joins under the binding. Nothing issues a
  certificate.~~ Since [F51](cluster-deployment.md) `claim` gives the id before the first start
  and `shoalctl cluster` issues every leaf it deploys; by hand it is still the operator's.
- **A reload is per node.** Every node is reloaded by its own `ReloadTls`; nothing fans it out,
  and the cluster tab's `reload-tls` reaches the node the connection did.
- **The certificate test needs kTLS**, as every TLS test does, and skips by name without the
  kernel module. ~~It was written and reviewed on a host without one and has not been run
  there.~~ It has since been run on the development host with `modprobe tls`, through the
  admin verb `ReloadTls` rather than the pool's method, so the operator's path is the one driven.
- **The cluster tab reaches one node.** What it shows is that node's view - `Replication` is its
  own groups, and a `Members` from a follower is as current as its log. It does not choose a
  node; the connection does.
- **The remote launcher assumes ssh without a prompt, a `shoal.yml` on the host with local
  storage paths, and a binary of this build there.** Each is refused by name when it is not
  so, and none is set up by the tool. The driver's node zero is never remote.
- **No physical capture is committed.** The record and the launcher exist; the capture is the
  benchmark host's to take with real hosts, and the pages that would carry it are unchanged.
  What the smoke test proves is that the launcher works against a host that exists.

## Invariants to uphold

- **A hello's node and the leaf's node agree, or the hello is refused, on both ends.**
  `bound_identity` is the one function; a new lane, door or dialler calls it with what the
  handshake said.
- **`PeerIdentity` is read before the session is handed to the kernel.** After
  `into_kernel` there is no session to read; `finish` reads the leaf first.
- **A reload swaps both configs or neither**, and every handshake reads the holder rather than
  a copy of what it held.
- **The control network dials the committed address.** `note_addresses` runs on every apply
  and `ControlPeer::link` looks the address up per RPC; a client that caches a link to a
  member's address would reintroduce the silence this page found.
- **`SetNodes` writes only a committed record of a member the state names**, one member at a
  time, never during a joint configuration.
- **The control group allows a log reversion.** A clone that wins is reached now, and what it
  holds is shorter than what the leader recorded for the identity; without
  `allow_log_reversion` the leader's control thread stops on it.
- **A cluster tab's mutation is sent against the version it previewed at.** A stale version is
  refused by the server, which is the point.
- **The ready line is `SERVE_READY_LINE <addr> <json>`**, and a parent reads the environment
  only if it is there, so an older `serve` still starts.
- **`emulated` is derived from the environments** when there are any; a capture with none is
  the one machine it always was.
- **A remote node writes its marker only into a directory that is empty or its own**, and wipes
  one that names another node.

## Performance

Nothing here is on a query path. The identity check is a DER walk at the handshake; a reload
is two config builds under a write lock nothing else holds for long; the address lookup is a
`BTreeMap` read per control RPC; the cluster tab's poll is six admin reads a second on one
connection. No capture changed, and no bench arm was added: the physical capture this page
makes possible is the benchmark host's to take. The three node overhead arm was run once at
smoke scale on the development host with the record in place - three environments naming the
one machine, `emulated` true, one build digest - and the capture deleted.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_peer_certificate_names_its_node_and_a_reload_swaps_whole` | `shoal-proto/src/shared/tls/tests.rs` | The SAN is not read off the DER, a leaf with no node or another scheme is not `None`, non-certificate bytes are not an error, both ends of a finished handshake do not report the peer's node, or a holder does not swap both configs on good material and keep both on bad |
| `peer_rejects_wrong_cluster_identity_and_malformed_payload` | `shoal-core/src/server/peer/tests.rs` | Beside the earlier refusals: a leaf naming the hello's node is not accepted, one naming another node is not `IdentityMismatch`, one naming none is not `Unauthorized`, the binding off does not trust the chain alone, or a joiner's certificate is not bound |
| `certificate_rotation_binds_identity` | `shoal/tests/cluster_fixture.rs` | A reissued leaf is not used by the next handshakes after a reload, a bundle does not carry an authority rotation with the old one retired and a restart still joining, a leaf naming another node is not refused by both ends naming the certificate (the misnamed node is the one restarted at that step, so no step depends on which member leads - [Resolved #112](../appendix/resolved/certificate-test-leader.md)), one naming none is not unauthorized, bad material is not refused with nothing changed, or the cluster does not serve once the leaf is its own again. Every reload goes through the admin verb `ReloadTls`, so the operator's path is the one driven and its refusal wording is what is asserted. Skips by name without kTLS |
| `address_change_is_observed_and_a_stale_clone_is_fenced` | `shoal/tests/cluster_fixture.rs` | A member restarted at fresh ports is not observed by every member - itself included - at the new address and incarnation, its links do not come up both ways, writes through it do not commit, or a clone at the old address is not refused as a duplicate while it keeps serving |
| `duplicate_node_identity_is_fenced` | `shoal/tests/cluster_fixture.rs` | The M3 row, which now reaches the winning clone: the leader's control thread stops on the clone's shorter log if the control group's reversion allowance is removed |
| `the_cluster_model_reads_the_admin_frames` | `shoalctl/src/cluster/model.rs` | The M9b figure is not said in the headline, a member's phase, grace or bytes are lost, a done plan is shown as open or a blocked reason dropped, a backup or a recovery is not summarized, the wire is not read, or an older frame fails the tab |
| `an_action_previews_its_boundary_and_follows_its_record` | `shoalctl/src/cluster/actions.rs` | An operation does not parse from its line or a malformed one is not refused by name, a preview does not name the identity, the movement and the boundary, a request or its follow-up differs, or a record's done state and lines are wrong |
| `physical_cluster_records_each_node_environment` | `shoal-bench/src/model/macro_layer.rs` | Three environments do not round trip under the record in node order, `emulated` is not false only when the hostnames differ, a difference does not name the node and its fields, a build difference is treated as a machine difference, or an F47 record fails to load |
| `a_remote_spec_parses_and_builds_its_commands` | `shoal-bench/src/workloads/harness/cluster.rs` | A spec does not parse into node, target, host and directory, node zero or a relative directory is accepted, the copy, serve or kill command lines change, or a ready line with an environment is not read and one without is |
| `a_remote_node_serves_a_smoke_capture` | `shoal-bench/tests/remote_smoke.rs` | With `SHOAL_REMOTE_SMOKE` set: a node on the named host does not serve a smoke capture, or the record does not say which machine it ran on. Unset, it says so and passes |

## Related

[F38](inter-node-transport.md) the lanes, the SAN and the refusals this reads; [F39](membership.md)
the M3 identity rule and the admin frames the tab reads; [F48](rolling-compatibility.md) and
[F49](backup-and-recovery.md) the other two thirds of M10; [F36](cluster-harness.md) the harness
the launcher extends; [C1](../distributed/node-identity.md), [C2](../distributed/transport.md),
[C9](../distributed/operations.md), [C10](../distributed/performance.md);
[Q11 at M10c](../distributed/protocol.md#q11-at-m10c); the [runbooks](../operations/runbooks.md);
[M10c](../distributed/milestones.md#m10c-rotation-the-cluster-tab-runbooks-and-the-physical-cluster).
