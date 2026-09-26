# F48. Rolling compatibility and activation

## Context

Since [F38](inter-node-transport.md) the peer hello has carried three things the Q10 contract
([C13](../distributed/protocol.md#q10-and-q11-at-m2)) asked to be compared apart - a schema
id, a wire version *range* and a capability set - and compared each of them exactly.
`speaks_our_version` required the peer's range to contain `PROTOCOL_VERSION` and nothing else;
the range and the bit set were fields to negotiate over one day, with the codec that would make
an n−1 acceptance mean something left to M10. [C2](../distributed/transport.md#compatibility-and-the-wire-version)
said what that codec had to be: a negotiated version that is *actually encoded and decoded*, a
cluster-wide activated version persisted in control state, a rollback limit stated once a
storage format past it is written, and old and new binaries exchanging queries, replication,
snapshots and elections rather than one successful hello.
[M10](../distributed/milestones.md#m10-operations-and-the-real-cluster) is that gate, and this
page is its first third, M10a: the upgrade itself. Backup, restore and recovery are
[F49](backup-and-recovery.md); rotation, the cluster tab, runbooks and the physical capture are
[F50](cluster-operations.md).

Decided with the user on 2026-09-14: the protocol version moves 4 → 5 *keeping the version 4
codec*, so there is a real older version to speak; a node pinned at 4 by
`cluster.transport.wire_version` stands in for an unupgraded build in the suite; and a second
test takes an actual previous build of the test binary when one is named, which is the run
that was made before the page was written.

## What it does

### The wire is negotiated, not matched

~~`PROTOCOL_VERSION` is 5~~ `PROTOCOL_VERSION` is 6 since [#155](../appendix/resolved/restore-retry.md),
and `MIN_PEER_VERSION` is 4 (`shoal-proto/src/shared/protocol.rs`). Version 6 moved no frame's
encoding: a body at 6 is encoded as at 5. It exists because the control log gained
`RetryRestore` and a group's restore record gained `failed_in` and `generation`, which a replica
at 5 would refuse to decode or drop, so the command is refused until 6 is activated
(`RESTORE_RETRY_FROM_WIRE`). A stalled copy's `DigestAnswer::Stalled`
([#160](../appendix/resolved/unreadable-partition-stalls-one-copy.md)) is new at the same build
and is not gated: a leader at 5 that cannot decode it counts the member as not reporting, which is
what it did before.
Every peer hello advertises `wire_min..=wire_max` - the floor, and the build's newest unless a
pin holds it lower - and the two ends speak the highest version both ranges hold
(`PeerHello::negotiate`); ranges that share nothing are refused `NoCommonVersion`, from both
ends, since the dialler judges the ack by the rules the acceptor judged its hello by. The
hello frame itself is written at the floor so that any peer in the range reads it. What was
agreed is a [`Negotiated`](../../../shoal-core/src/server/peer/handshake.rs) - the version, the
intersected capability word, the peer's frame bound and the peer's own newest version - kept
on every link and every accepted connection.

**Every frame after the hello names the version its body is encoded at, and a receiver
decodes by the header.** The link's writer stamps its negotiated version over a frame built
with no version of its own (`Frame::new`), and leaves alone one built at a version of its own
(`Frame::at`), which a body that differs between versions is: encoded at what the link speaks
when it is built, or at the floor when the link is not up yet, so a frame queued across a
reconnect is still readable whatever version the reconnect settles on. A listener's relay
answers at the version it accepted. A frame *above* the negotiated version is refused by
version (`RawHeader::validate_at`) rather than misread, on both ends.

**One body differs between 4 and 5: the snapshot manifest.** At 5 a
[`SnapshotManifest`](../../../shoal-core/src/server/replication/snapshot.rs) carries the
cluster it was cut in, the node that cut it and when - what a backup file needs to identify
itself. At 4 it is the record every build before this read, `ManifestV4`, and since the
manifest sits in the middle of a postcard-encoded `SnapshotRpc::Begin` the older shape is a
type of its own rather than a default: `SnapshotRpc::encode_at(version)` and `decode_at(bytes,
version)` are the codec, the begin and the end are encoded afresh at the link's version on every
attempt (`snapshot_rpc_until`), and a receiver reading a v4 manifest fills the three fields
with its own cluster and the sender it heard from (`SnapshotManifest::filled`). A snapshot
crosses a mixed link in both directions in the tests below.

**The file format follows the activation.** A snapshot file gains a version 2 header - the
version 1 header plus the cluster, the schema id and the cut's time
(`SNAPSHOT_HEADER_LEN_V2`) - written only once the cluster has activated wire version 5
(`SnapshotProvenance::at`) and read by this build at either version; a build that meets a
version it does not read refuses it by name. Nothing is ever written that a member which
could still roll back would meet.

**Capabilities are intersected and load-bearing.** `Negotiated::capabilities` is `ours &
theirs`; ~~every bit this build defines is in `REQUIRED_CAPABILITIES`~~ every bit this build
defines but `CAP_PRE_VOTE_V1` is in `REQUIRED_CAPABILITIES`, since each is what some
version in the range acts on, and a peer without one is refused `CapabilityMissing` rather
than half served. A capability a later build adds as optional is left out of that set and
gated by `Negotiated::has` at the one place it is acted on, the way `CLIENT_CAP_READ_OPTIONS`
gates a client. The first was `CAP_PRE_VOTE_V1`
([Resolved #144](../appendix/resolved/post-heal-elections.md)): a pre-vote goes only to a peer
that granted it, and one to an older peer is granted locally.

**The client lane is exact at `CLIENT_WIRE_VERSION`, which stayed at 4.** The client lane took
no part in the change, so a client writes every frame at 4, a server answers a client at 4
whatever its peers speak, and the schema fingerprint the two compare folds `CLIENT_WIRE_VERSION`
rather than `PROTOCOL_VERSION` (`shoal-derive/src/traits/fingerprint.rs`). A client built
before this and one built after both talk to a server built before this and one built after: a
server reads a client's frame from 4 up to its own newest. The version moves when the framing
between a client and a server moves, and only then.

### The activation

The control state carries `activated: u8` (`ControlState::activated_wire`, the floor until an
operator moves it), pushed to every shard on the map and to every judge of a hello. `Activate {
wire }` is an admin mutation - versioned, remembered by operation, refused for a stale version,
answered `Applied` or `Refused` by name - and what it is judged by is the point of the design:

- **A member below the activated version is refused at every door**: the hello
  (`PeerRefusal::BelowActivatedWire`, from a shard's map and from the control thread's state),
  `ObserveMember` and `Admit`, and a node's own start, which reads its log and stops with
  `WireBelowActivated` before it serves anything if the version its cluster activated is one
  it cannot speak. The refusal names the activated version and the offered one.
- **It never lowers.** The boundary is what nobody rolls back past; an `Activate` at or below
  the current version is applied without moving anything or refused, and the floor is always
  activated.
- **It is judged by what the members' running builds say, carried in the command.** The
  leader reads every member's newest version from two live sources - the hellos its own
  control links completed (`PeerNetwork::wires`) and the status reports it receives
  (`StatusReport::wire_max`) - and writes them into the command as `members`; apply refuses
  unless every member in any phase but `Removed` is named at or above the version, and a
  member the leader has not heard from is refused by name before anything is proposed. The
  committed records are *not* what is judged, for a reason the previous-binary run found: a
  build from before the field persists its applied state without it, so a replica restored on
  such a build holds a record at the floor for a member that speaks more, and an apply that
  read the records would commit on one replica and refuse on another. The command's claim is
  the same on every replica, and applying it writes the versions into the records, which heals
  them. `Members.wire` reports the activated version, the floor and the newest this build
  speaks, and the lowest and highest a member speaks, from the records and what this node has
  heard, whichever says more.

Before the activation any node may be restarted on the previous build or pinned back at the
previous version; after it, none. The pin is the operator's rollback knob for the window
between: a node upgraded with `cluster.transport.wire_version` at the version the cluster runs
speaks nothing its unupgraded peers cannot, the pins are lifted one at a time, and the
activation is asked for once `Members.wire.min_member` is the new version. A pin outside the
build's range is refused at validation; a pin below the cluster's activated version refuses to
start.

### The rollback matrix

What is written by a build at 5 before and after the activation, and what a build at 4 does
with it:

| Where | Before the activation | After the activation |
| --- | --- | --- |
| Peer frames | Every link to a 4 speaks 4; a 4 reads everything it is sent | A 4 is refused at the hello (`BelowActivatedWire`) and refuses to start from its own log |
| Snapshot manifest on the wire | v4 shape on a 4 link, v5 on a 5 link | v5 only, since no 4 is spoken to |
| Snapshot file header | Version 1, which a 4 reads | Version 2, which a 4 refuses by name |
| Control log | `ObserveMember` records with the wire fields, which a 4 reads and drops on its own persisted state | Carries `Activate`, a command a 4 cannot decode; the log is the boundary too |
| Control log, 5 to 6 | Nothing a 5 cannot read: `RetryRestore` is refused before it is committed | May carry `RetryRestore`, which a 5 cannot decode |
| Checkpoint and control files | Unchanged at 5 | Unchanged at 5 |
| Storage marker | Format 3, unchanged | Format 3, unchanged |
| Client lane | 4, unchanged | 4, unchanged |

What is *not* covered: a schema change. A join with another `schema_id` stays refused, and a
schema change as a rolling operation is **explicitly unsupported** - the supported path is a new
cluster and a restore or an import ([F49](backup-and-recovery.md)). A marker format this build
does not read is the same: never migrated in place, served by the build that wrote it or brought
into a new directory. The refusals say so now rather than naming a milestone.

## Design choices

- **A real older version, not a mock.** The version moved to 5 with the manifest as the one
  changed body so that the v4 codec is exercised by every snapshot between a pinned and an
  unpinned node, in both directions, rather than by a test that pretends. A negotiation nothing
  ever encodes differently is a negotiation nothing tests.
- **The frame names the codec, not the connection.** Stamping the negotiated version at write
  time and decoding by the header lets one connection carry a frame at the floor beside one at
  the negotiated version, which is what a body encoded before the link came up needs; the
  alternative - re-encoding queued frames on every reconnect - would have put the codec in the
  link rather than at the one place the body is built.
- **The activation is a committed fact, judged by live builds.** A persisted activation is
  what C2 asked for, and every door reads it from committed state. The judgment behind it is
  by the reports and hellos and not the records, for the divergence the previous-binary run
  showed; the command carries the claim so that apply stays pure and every replica writes the
  same thing.
- **Doors before data.** A member below the activated version is refused before it can send
  anything, and stops itself before it serves anything, so a rolled-back node after an
  activation is a node that does not run rather than one that half runs.
- **The client lane opts out.** Folding `CLIENT_WIRE_VERSION` into the fingerprint costs one
  constant and keeps every existing client working; folding `PROTOCOL_VERSION` would have
  orphaned every client on a change that never touched it.
- **Required capabilities, not optional ones.** Every bit that exists is required, because
  every version in the range acts on all of them; making the set intersect and gate now is what
  lets a later build add an optional bit without a version bump.
- **The pin is a config knob and a fixture axis at once.** One field does what an operator
  needs during the window and what a test needs to make one binary stand in for two.

## Alternatives rejected

- **Accepting n−1 at the hello and speaking n.** The rule C2 wrote and the one rejected at M2
  for the same reason: a compatible handshake has to imply working payloads at the selected
  version.
- **Judging the activation by the committed records alone.** Pure and simple, and wrong once a
  replica's persisted state was written by a build without the field - the run from the real
  previous binary refused the activation on one follower and applied it on the leader. The
  records are healed by the activation instead of trusted for it.
- **Having every member re-observe itself when the leader changes**, to heal the records
  before an activation. The leader cannot see a follower's stale record, so it cannot know when
  the healing is done; the command-carried claim needs no healing to be right.
- **Keeping `Header::new` at `PROTOCOL_VERSION` and stamping the client lane.** Every client
  frame goes through a dozen encode paths; one constant the client lane writes at is one place.
- **A `#[serde(default)]` on the manifest's new fields as the codec.** Enough for the JSON
  pending marker, which it is used for, and nothing for postcard, which has no way to say a
  field is missing from the middle of an enum variant. The explicit v4 shape is the codec.
- **Writing version 2 snapshot files from the first 5 build.** A member that rolled back to 4
  before the activation would meet a file it cannot read on the next install; the file format
  follows the activation so that the rollback matrix has one boundary rather than two.

## Limitations

- **The suite's mixed cluster is one binary pinned two ways.** A pinned node speaks 4 and is
  refused past an activation exactly as an old build is, but it still *knows* every command and
  field a 5 does; what an old build drops on its own persisted state is exercised only by the
  previous-binary test, which runs when `SHOAL_PREVIOUS_TEST_BINARY` names one and prints a
  skip otherwise. It was run once against the fixture binary of the commit before this
  feature, and passed; it is not part of the count.
- **`min_member` can read the floor on a node that has not heard a member.** The records may
  be stale for the reason above until the activation heals them; the view folds in what this
  node heard on its own links and, on the leader, the reports, and the leader is what an
  operator asks. The activation itself never relies on the view.
- **A down member blocks the activation.** Not a defect: its version is unknown, and the
  refusal names it. Remove it or bring it back.
- **The control log is the boundary for a 4 too.** A build at 4 that reads a log carrying
  `Activate` fails to decode it, which is the intended stop, but it is a decode error rather
  than the `WireBelowActivated` a 5 build refuses with; a 4 build was never taught the
  refusal.
- **Schema changes and marker format migrations are unsupported**, stated rather than built.
- **No capability is optional yet.** The gate exists (`Negotiated::has`) and nothing uses it.
- **The version 2 file header is written and read and identifies a file; nothing consumes it
  until [F49](backup-and-recovery.md).**

## Invariants to uphold

- **A frame's body is encoded at the version its header names, and that version is at or
  below the connection's negotiated one.** A body that differs between versions is built with
  `Frame::at` at the version it was encoded at; a body that does not is built with `Frame::new`
  and stamped. Building a versioned body with `Frame::new` is the bug this page exists to
  prevent.
- **The hello is written at `MIN_PEER_VERSION`.** A hello at the newest version is one a peer
  at the floor refuses before it can negotiate.
- **The client lane writes at `CLIENT_WIRE_VERSION` and the fingerprint folds it.** Moving one
  without the other orphans every client.
- **`MIN_PEER_VERSION <= CLIENT_WIRE_VERSION <= PROTOCOL_VERSION`**, asserted at compile time.
- **`apply_activate` reads the command's `members` and the members' phases, never the records'
  wire fields.** The records are not the same on every replica; the command is.
- **The activated version never lowers**, and every door reads it from committed state: the
  map on a shard, the applied state on the control thread, the recovered state at a node's
  own start.
- **A file format past version 1 is written only when `activated_wire >=
  SNAPSHOT_V2_FROM_WIRE`.** The activation is the rollback boundary for disk because of this
  and nothing else.
- **Every capability this build defines is in `REQUIRED_CAPABILITIES`** until one is made
  optional on purpose, with a gate at the place it is acted on. `CAP_PRE_VOTE_V1` is the one
  made optional so far, gated in `GroupPeer::pre_vote` and `ControlPeer::pre_vote`
  ([Resolved #144](../appendix/resolved/post-heal-elections.md)).
- **A member's reported `wire_max` is its running build's, from `Local`, never copied from a
  record.**

## Performance

Nothing on the hot path changed in cost: the negotiation is one comparison at the hello, the
stamp is one byte written where the header already was, and the version check on read is the
same range check `validate` always made with a different ceiling. The snapshot begin encodes
its manifest afresh per attempt, which is a few hundred bytes per transfer. No capture was
taken and none is claimed; a change that touched the transport's bytes per frame would be
`--group cluster`'s to show.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `mixed_versions_exchange_real_cluster_operations` | `shoal/tests/cluster_fixture.rs` | A pinned member is not spoken to at the floor, a forward or a quorum write across the versions fails, a barrier read misses a write, a snapshot over a version 4 link does not install, an election among the members after the newest one dies does not commit, or the activation is not refused naming the pinned members |
| `rolling_upgrade_survives_operations_and_failure` | `shoal/tests/cluster_fixture.rs` | A node restarted at the newest under writers is not spoken to, a member killed inside the mixed window does not come back at the old version, the activation does not commit once every member reports the newest or is not seen by every node, a restart pinned below it starts, or the writers' history is not sequential |
| `rolling_upgrade_from_previous_binary` | `shoal/tests/cluster_fixture.rs` | With `SHOAL_PREVIOUS_TEST_BINARY` set: a previous build's node refuses the new build's hello, a snapshot or a write across the two builds fails, or the activation is refused on a replica restored on the previous build. Unset, it says so and passes |
| `a_version_range_negotiates_to_the_highest_shared` | `shoal-proto/src/shared/protocol/peer/tests.rs` | Two ranges negotiate to something other than the highest both hold, disjoint ranges negotiate, the capability words do not intersect, or a pin does not bound the advertised range |
| `a_header_below_the_floor_is_refused_and_one_in_range_is_kept` | `shoal-proto/src/shared/protocol/tests.rs` | A header in the range is refused or not kept as read, one below the floor is accepted, or one above a negotiated ceiling is accepted |
| `a_v4_manifest_round_trips_with_defaults` | `shoal-core/src/server/replication/snapshot.rs` | The v4 shape does not round trip or lift with defaults, a v5 begin encoded at 4 is the wrong shape, a pending marker from before this does not load, or a version 2 file header does not carry its cluster |
| `peer_rejects_wrong_cluster_identity_and_malformed_payload` | `shoal-core/src/server/peer/tests.rs` | Disjoint ranges are accepted, a missing required capability is accepted, a peer at the floor is refused before an activation or accepted after one |
| `activation_needs_every_member_at_the_wire` | `shoal-core/src/server/control/types.rs` | An activation applies with a member reported below it or not reported, lowers, re-judges a removed member, does not heal a record, or a member below the activated version is observed or admitted |
| `the_transport_block_refuses_a_pin_outside_the_range` | `shoal-core/src/server/conf/cluster.rs` | A pin outside the build's range validates |
| `an_unknown_format_is_refused` | `shoal-core/src/server/meta.rs` | A marker in another format is refused without saying it is never migrated in place and where the way out is |

## Related

[C2](../distributed/transport.md#compatibility-and-the-wire-version),
[C13 Q10](../distributed/protocol.md#q10-at-m10a), [M10](../distributed/milestones.md#m10-operations-and-the-real-cluster),
[F38](inter-node-transport.md), [F39](membership.md), [F43](node-recovery.md),
[F49](backup-and-recovery.md), [F50](cluster-operations.md),
[runbook 7](../operations/runbooks.md#7-rolling-upgrade).
