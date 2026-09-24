# F44. Repair

## Context

Every replica's rows are its table's archives, and until this feature an archive record was
`[size][payload]` with nothing to verify a read against: corruption was caught only if rkyv's
`bytecheck` happened to reject the bytes, and nothing marked a copy bad when it did
([todo: archive checksums](../appendix/todos.md#archive-checksums)). The only comparison
between replicas was the fixture's `DIGEST` verb, a fold of applied state taken whenever the
RPC landed - neither at a common committed boundary nor independent of the code under test
([C11](../distributed/testing.md#digests)). A durable follower that lost its log stopped the
*leader's* process ([item 99](../appendix/resolved/durable-log-reversion.md)). There was no
repair: `grep -i "quarantine\|repair"` over `shoal-core/src` was empty.

[M8](../distributed/milestones.md#m8-repair) is what [C9](../distributed/operations.md#repair)
asks for: persistent integrity metadata, a canonical digest at a common committed boundary,
quarantine and verified source selection, atomic snapshot repair through
[F43](node-recovery.md)'s install, authorization and progress through the admin frame, and
evidence preserved when divergence cannot be resolved. [Q12](../distributed/protocol.md#q12-at-m8)'s
scheduled half is decided with a cost measurement.

The plan for this milestone was drawn up with the user on 2026-09-13 and four decisions were
taken there: **item 99 is fixed inside M8**, reproduced first, with a resolved page of its own;
**a scrub interval setting, off by default**, plus **one background arm**
`macro/cluster/background/repair` smoke-run on the development host only; **the checksum lives
in the archive file beside the size prefix**; and **one commit per slice**.

## What it does

### Integrity metadata: every record checksummed

An archive is **format 2**: a sixteen byte header `[b"SHOALARC"][version u32 = 2][reserved]`,
then `[size u64][gxhash64 u64][payload]` per record, the checksum over the payload seeded like
the intent log's. `ArchiveEntry.offset` still points at the payload, so the checksum sits eight
bytes ahead of it and a read is one `read_at(offset - 8, size + 8)` - a hash and a slice of the
same buffer, no second read. `write_record` (`fs/map.rs`) is the one place a record is written
- a compaction, an archive compaction and a snapshot install all go through it - and
`ArchiveMap::read_record` the one place a record is read: the loader, the compactor's merge, an
archive compaction, a snapshot cut and a direct read. A record that does not hash is
`ShoalError::CorruptArchive` naming the archive and the partition, before a byte reaches rkyv;
the loader classes it `Fatal`, the queries parked on it hear `ErrorCode::CorruptArchive`, and
the map counts a checksum failure. A cut that meets one fails rather than sending the record
on, so a corrupt copy is never a source; an archive compaction that meets one fails rather than
rewriting it under a fresh checksum, so corruption is never laundered.

An archive from before the header is **format 1**, decided from its first sixteen bytes when
its handle is opened and fixed for the handle's life: read as it always was, counted as an
*unverified read*, and rewritten into the format 2 active archive by the next archive
compaction whatever its utilization - the fifty percent rule is waived for an archive with no
checksums, since rewriting is how its records come to have them. A restart mints a new active
archive, so a format 1 one is never written to again.

`checkpoint.json` carries `"checksum"`, gxhash64 over the compact JSON of its groups; zero is a
file from before, read unchecked with a warning. `retries.bin` is `[b"SHOALRTY"][gxhash64][postcard]`,
and so is `retries.next.bin`, the sidecar staged for a checkpoint not yet on disk
([Resolved #115](../appendix/resolved/retry-sidecar-crash-window.md)); a file without the magic is read as the bare postcard it was. Either mismatch fails the open by
name, the verdict `MapCorruption` already got: a checkpoint that cannot be trusted is not a
state to start a group from.

### The canonical digest at a common committed boundary

**A scrub is a log entry.** `Command::scrub(table, op)` is a command whose tablet is
`SCRUB_TABLET`, the one no tablet can be, whose request bundle is the operation and whose
payload is empty; it rides the log in the command's own encoding, so the WAL's frame format is
unchanged, and the WAL's index marks it log-alone like a blank, so it is never handed to a
compactor. The group's leader proposes it; every replica applies it in committed order at index
`B`, which is the one point at which each replica's state is exactly the entries at or below
`B` - no clock, no pause coordination.

**Applying it takes a cut, not a walk.** `D::canonical_cut(table, &tablets)` (derived beside
`digest_table`) hashes every *resident* partition of the group's tablets on the shard loop -
memory speed - and collects, for every non-resident archived key, its `ArchiveEntry` plus a
duplicated `DmaFile` per distinct archive. The applied position moves past `B` at once. A
spawned task then reads each collected record through the handle it was collected with,
verifies it, hashes it, and posts `ServerMsg::Digested`. An open handle survives the archive's
later unlink and a record's bytes are never rewritten in place - the map only repoints - so the
task reads the state at `B` however long it takes and however the map moves. The pause on the
loop is the resident pass alone.

**Canonical form.** Per partition `gxhash64(key ‖ row_count ‖ rows)`, each row its length and
its re-serialized rkyv bytes, in canonical order - an unsorted partition holds one row, a sorted
one iterates in sort-key order - and a partition with no live row contributes nothing, so a
tombstone here and no partition there agree. The group digest folds the partition hashes in key
order under the schema fingerprint and the tablet list. It never sees archive bytes, which is
what makes it layout-independent by construction: `canonical_fold_is_layout_and_order_independent`
is the pure statement of that, and `canonical_digest_ignores_archive_layout_at_same_boundary`
the one through three replicas merged thrice, once and never. The report is
`DigestReport { boundary, partitions, rows, digest, integrity: Verified | Invalid { checksum_failures },
unverified, bytes }`.

**Reports are polled, not pushed.** `ReplicateKind::Digest = 6` carries the operation; a
member answers `Pending` until its task has posted, then the report, and `Unknown` for a scrub
it never applied - a member that was down when the entry committed applies it when it catches
up, and is polled until then. `MachineState::digests` keeps the last eight by operation.
`scrub_group` (`shard/repair.rs`) proposes and polls until every member answered or
`cluster.repair.timeout` passed, answering a member that never did by name. The fixture's
`DIGEST` verb stays as it was: the independent fold the docs require, a different function on
purpose.

`IntegrityStats { checksum_failures, unverified_reads, log_lost, quarantined, scrubs,
scrub_bytes, scrub_partitions }` rides `ShardReplication` and `NodeReplication` beside
`SnapshotStats`, read by the `Replication` admin operation, readiness and the fixture's
`GROUPS`; `GroupReport::quarantined` says why a copy is.

### Quarantine: local first, committed second

A copy is quarantined **locally first**: by a record that failed its checksum on a read - the
shard maps the table and partition to the group holding it - or by a driver's verdict after a
scrub, sent over the lane as `ReplicateKind::Quarantine = 7` and applied on the loop.
`MachineState::quarantined: Option<Quarantine { reason: Checksum | Divergent | Operator, at, op }>`
is persisted as `wal/Shard-N/quarantine/<group>` with `write_atomic` and scanned at open beside
the install markers, so a restart finds the copy still quarantined. While it is set a read of
the group's tablets through the shard is refused with `ErrorCode::Quarantined` naming the
reason - the `installing_group` gate extended - while writes still propose, since the log is
checksummed and independent of the archives; readiness counts `quarantined` beside
`installing`.

It is committed **second**: the shard's report carries it to the control thread, which carries
it in the node's status report, and the leader commits `ControlCommand::ReportQuarantine` on
change - the `ReportShards` pattern - into `MemberState::quarantined`, from where
`TabletMap::from_state` puts it on `MapMember`, `TopologyMember` and every pushed frame. From
then `read_ring_for`, `preferred_holder` and `alternate_holder` pass over a holder whose copy of
a tablet is quarantined, so a `One` read through the holding node is routed to another replica
and served from there, and the local refusal is the backstop for the window before the map
carries it. A quarantine is lifted only by a repair that verified the copy or by an operator's
`Repair { release: true }` after an explicit outcome.

### The `Repair` operation and its record

`AdminKind::Repair { table, tablet: Option<u16>, mode: "verify" | "repair", source:
Option<NodeId>, release: bool }` is a mutation like the three before it: authorized against
`cluster.admins`, versioned, idempotent by operation id - `Applied { version }` is the handle,
and the same operation again is `Repeated` - and audited. `ControlState::apply` commits
`RepairRecord { op, table, tablet, mode, source, release, principal, requested_at, groups }`,
the groups derived from the placement at apply by `TabletMap::groups_of(table)` - every group
of the table, or the one serving the tablet - each at `GroupRepair { phase: Pending, driver,
boundary, reports, outcome }`; the last sixty-four records are kept. `AdminKind::RepairStatus { op }`
reads the record through any node. Pending records ride the pushed map as `TabletMap::repairs`,
which is how a group's leader learns of one; done records drop off it.

**The driver** runs on the shard that leads a group with a pending entry, as a task
(`drive_group`), one group at a time up to `cluster.repair.concurrent`, started from a map
install, a group coming up, a driver finishing and every deadline tick. Every step is committed
as `ControlCommand::RepairProgress { op, group, node, incarnation, progress }` - a node's
proposal, not an operator's - before the next, retried past a control leader change, so a
status read on any node sees it and a driver that dies leaves a phase the next leader resumes
from: `Pending` and `Scrubbing` are drivable, and the map being a moment behind a commit is
covered by what the shard itself committed last.

1. **Scrub**: commit `Scrubbing`, propose the entry, poll every member.
2. **Judge** (`judge`, pure and unit-tested): a copy whose report is invalid is quarantined
   `Checksum` whatever else is found. Among the verified copies a strict majority of the
   *replica set* agreeing on one digest is the trusted state, and every other verified copy is
   quarantined `Divergent`; an operator's `source` overrides the rule with its own verified
   digest, and a copy differing from it is quarantined `Operator`. With no majority and no
   source - a three way split, or too few verified copies - the outcome is
   `Unresolved { digests, invalid }`: **nothing more is quarantined and nothing is installed**,
   and the digests are the evidence. A member that never reported is named in
   `Clean { unreported }` rather than judged.
3. **Verify mode stops here**, `Done` with `Clean`, `Divergent { quarantined }` or `Unresolved`.
   Repair mode lifts the quarantine of any copy holding the trusted digest, verified - one a
   crashed install left quarantined by its marker - and goes on.

### Atomic snapshot repair

**The source is the leader.** A leader whose own copy is not among the trusted commits the
group back to `Pending`, calls `transfer_leader` to a trusted member, and leaves the group for
it: that is "removing a corrupted primary's serving eligibility and reestablishing authority on
a healthy quorum". A **volatile** target is repaired the way a restart repairs an ephemeral
group: `QuarantineAction::Rebuild` drops its memory log and resident partitions and builds the
group again empty, and the leader's replication feeds it whole.

A **durable** target is live and applied past any boundary the leader can cut, and openraft
refuses a snapshot at or below what a group has committed, so the install cannot go through
`install_full_snapshot`. It goes through the open path instead, committed as
`Installing { source, target }`:

1. The driver cuts its own snapshot (`network.build`, [F43](node-recovery.md)'s held or fresh
   cut) and sends it with `GroupPeer::repair_snapshot`: the M7 stream with
   `SnapshotRpc::Begin { repair: Some(op) }`.
2. The target accepts a repair stream only for a quarantined copy and only if the boundary is
   past its **checkpoint** rather than its applied position, else answers
   `SnapshotAnswer::Behind { checkpoint }`; from acceptance it holds its checkpoint
   (`hold_checkpoint_until`, skipped by `advance_checkpoints` until the restart or the
   transfer's deadline) so the log from the checkpoint stays and the boundary cannot be
   overtaken. On `Behind` the driver proposes an entry past the checkpoint - a scrub under a
   throwaway operation - has the loop rotate and sweep the WAL (`ServerMsg::RepairRotate`), waits
   for the compactor's merge to move its own checkpoint past, and cuts again, three times at
   most.
3. Chunks and `End` as M7; the partial is verified and the marker written; then instead of
   `install_full_snapshot` the loop runs `restart_group_for_install`: the live handle is shut
   down and built again as a process restart would build it - the resident partitions of the
   group's tablets dropped, the applied position set back to the held checkpoint `C < S`, the
   retry table re-seeded from the sidecar, the file pending past `C`, the apply batches parked
   for the old handle dropped - so openraft's startup finds a snapshot past `applied`, installs
   it through `handle_install_snapshot` → `CompactionJob::Install` → `SnapshotInstalled` →
   checkpoint `:= S` → cleanup, and then re-applies `S+1..committed` from the retained log.
   Proposals queue in `slot.waiting` meanwhile; the quarantine keeps the tablets from serving.
4. After the checkpoint carrying `S` is durable, the loop hands every already-handed sealed
   segment holding this group's frames above `S` to the compactor again as `Segment` jobs for
   this group alone (`rehand_segments`, `frames_in(gen, &[(group, S)])`), so writes the old
   generation had merged are merged into the new one before the resident copies the log
   rebuilds can be evicted.
5. The driver commits `Verifying`, scrubs again, judges again, lifts the quarantine of every
   copy that now holds the trusted digest, and commits `Done` with
   `Repaired { source, targets, boundary, verified }` when every target does, or
   `Failed { reason }` with the copy still quarantined.

The seven `CrashPoint`s apply unchanged: a target killed at any of them comes back with the
marker still quarantining the copy, `scan_pending` redoes the install at open when the marker
is past the checkpoint, the restart hands every sealed segment above the checkpoint to the
compactor again as [item 104](../appendix/resolved/segments-recompacted-after-restart.md)'s
rule already does, and the next repair finds the copy either installed and agreeing - lifted at
the judgement - or still corrupt and installs it again
(`repair_install_is_atomic_at_every_crash_point`).

### The scheduled scrub

`cluster.repair: { scrub_interval: none, timeout: 5m, concurrent: 1 }`
([configuration](../getting-started/configuration.md)). With `scrub_interval` set, every
group a shard leads is verified on the interval, the first pass staggered across it by the
group's identity, under a `Repair` the process proposes in verify mode naming the group's
first tablet, with the principal `scheduler` - recorded like an operator's. A scheduled pass
never installs: that is Q12's policy. `timeout` bounds one scrub and has to be no shorter than
`replication.write_timeout`, since the entry is a write; `scrub_interval` no shorter than
`timeout`; `concurrent` at least one.

### What the fixture can do now

The child answers `SCRUB <group>` - a scrub proposed through the node, which has to lead, with
every member's report - and three faults done by the compactor that owns the archives:
`CORRUPT <table> <key>` flips one byte of the partition's record through a buffered handle and
syncs it, so the next direct read meets it; `FORGET <table> <key>` logs `MapIntent::Remove` and
drops the entry, a missing partition with every checksum whole; `ERASE <table> <key>` rewrites
the partition as one with no live row under a valid checksum, changed content only the digest
can see. Each evicts the resident copy first. `REPAIR <json AdminKind>` asks as the process and
answers the operation, `REPAIR_STATUS <op>` reads the record; the builder gains
`scrub_interval`, `repair_timeout` and `snapshot_timeout`; `GROUPS` carries `integrity` and
`quarantined`, and `MEMBERS` every member's quarantined copies.

### The background arm

`macro/cluster/background/repair` is the kill arm's placement, mixture and client with nothing
killed and a `Repair` of the reference table in verify mode asked for a third of the way
through (`Workload::background`), its record polled each second until every group is done.
The record is `cluster.background: BackgroundFacts` - the marks, the groups and how many were
clean, what the scrubs hashed and read across every node from the integrity counters' change
over the run, the client's distribution before, during and after, and a per second series -
beside the fault arms' shape, in the family `cluster-background`, mirrored into the explorer.

## Design choices

**The scrub is an entry, and the boundary is where it applies.** Any other boundary - a clock,
a paused leader, a chosen index - needs every replica to agree about it out of band. A
committed entry is the one thing every replica already agrees about, and applying it is the
one moment each replica's state is exactly a prefix of the log.

**A cut on the loop and a read off it.** Reading every archived partition of a group on the
loop would pause the shard for the length of the group's disk; hashing them later from the map
would hash a state that moved. Collecting entries and handles on the loop pins the state at
`B` at the cost of a handle per archive, which unlink does not disturb, and the read is a task's.

**The digest never sees archive bytes.** Rows are re-serialized from what was read back, so a
partition resident on one replica and archived on another, or archived thrice on one and once
on another, hash the same. That is the whole of what "layout-independent" means here, and why
the fixture's `DIGEST` - which folds serialized rows too, but over applied state at no common
boundary - stays a separate function.

**A scrub is a command with an impossible tablet.** A new command kind would have changed the
frame encoding every existing WAL was written in. A tablet id is twelve bits, `u16::MAX` is
one no write can name, and every reader that matters - the apply, the WAL's index, the
compactor - asks `scrub_op()` first. The wire version went to 4 so a peer built before it is
refused at the hello rather than applying the entry as a write with no payload.

**Quarantine is decided where the evidence is and committed where routing is.** A checksum
failure is the shard's to see and act on at once, before the queries parked on the record are
answered; routing around the copy is the map's, and the map is committed state. The marker
beside the WAL is what makes the local half survive a restart, since the committed half alone
would route around a copy the shard itself no longer knew was bad.

**The verdict travels over the lane, not the map.** A verdict on the map would be re-applied on
every restart and every map install, and a copy repaired or released since would be quarantined
again by a record that still said divergent. The driver tells each copy once, and the record is
what an operator reads.

**A strict majority of the replica set, not of the reports.** Two of three agreeing while the
third is silent is a majority; one of three agreeing with nobody is not, however many reported.
A copy that did not report is neither trusted nor quarantined, and is named as unreported.

**Repair goes through the open path, not `install_full_snapshot`.** openraft's refusal of a
snapshot at or below committed is right for replication and wrong for repair, whose whole point
is replacing what a live group applied. Restarting the group from its checkpoint is exactly
what a process restart does with a pending marker, which [F43](node-recovery.md) already made
atomic at seven points; the repair adds the held checkpoint, the `Behind` round, and the
re-merge of what the old generation had merged above the boundary.

**Progress is a node's proposal, retried.** An operator's operation is versioned and refused
stale; a driver's progress carries no version, since it is not a request against a view, and is
retried until the control plane commits it, since a control leader killed mid-scrub is the
resumability test. What was committed last is remembered on the shard, because the map a
commit produces arrives a moment after the commit and a driver that re-read a stale map would
scrub twice.

**A scheduled pass verifies and stops.** The cost of a scrub is the group's disk once per
pass, which the background arm prices; the cost of an automatic install is a copy replaced by
a rule nobody asked to apply. The destructive half is an operator's, with the majority rule
and a named source, and never an automatic choice on an unresolved split.

## Alternatives rejected

**A digest of archive bytes.** Cheap and wrong: two replicas holding the same rows in
differently compacted archives would differ on every scrub, which is the false corruption
report [C9](../distributed/operations.md#repair) names first.

**Pausing writes for the scrub.** A pause coordinates the boundary at the cost of an outage per
scrub; the entry coordinates it at the cost of one log entry.

**Pushing reports to the leader.** A member's task finishing while the leader has changed would
push to nobody; polling by the leader survives a leader change, since the new leader restarts
the scrub from the record and polls under a new operation.

**A `ControlCommand::Repair` that installs by itself.** The control plane knows the placement
and nothing about a group's archives; the install is the group leader's, which is why the record
rides the map and the driver runs on the shard.

**Quarantine as a `MemberHealth`.** Health is per node and a quarantine is per copy; a node
with one bad copy of one table serves every other tablet as before.

**Repair-from-primary on any mismatch.** Superseded by [C9](../distributed/operations.md#alternatives-rejected)
before this was built: the primary is judged like every other copy, and `repair_detects_corrupt_primary_and_preserves_evidence`
corrupts it first.

**A new snapshot RPC for repair.** The stream, the chunks, the assembler, the marker and the
crash points are M7's; a repair stream differs in what the receiver judges it against and how
it installs, which one field on the begin carries.

## Limitations

- **Routing is per tablet, not per table.** `MapMember::quarantined` names the tablets of the
  quarantined group, and a holder is passed over for every table's reads of those tablets; the
  refusal at the holder is exact. On the fixture's placement every node holds every tablet, so
  a read through the holding node is routed to another replica rather than refused, and the
  refusal by name is observed in the window before the map carries the quarantine.
- **The local refusal window is not deterministic.** A read that meets a corrupt record
  answers `CorruptArchive` or, when the gather tried the share again after the failure,
  `Quarantined`; the tests accept either and assert the routed read afterwards.
- **A sorted table's canonical cut is unit-covered through the fold alone.** The fixture's
  schema has no persistent sorted table, so `PersistentSortedTable::canonical_cut` runs in no
  fixture test; its unsorted twin runs in all of them.
- **A cut of a corrupt record fails the cut, and the fixture proves it indirectly**: a corrupt
  copy is never chosen as a source because the judge quarantines it, so no repair test reaches
  the cut's own refusal.
- **`Repaired` records no `replaced` generation.** The archive generation is not a number the
  compactor tracks; the boundary and the verified index are what the record carries.
- **One repair per shard at a time** (`concurrent: 1`), and a record's other groups wait for
  it; a table of many groups is repaired one group at a time per leading shard.
- **A `Behind` round costs every replica a cut.** The entry that moves the source's checkpoint
  past the target's is a scrub under a throwaway operation, which every replica applies as a
  canonical cut and reports to nobody; a blank entry openraft would let a client propose would
  cost nothing, and it has none.
- **A scheduled scrub's operation may be refused stale** when the topology version moves
  between the tick and the proposal, and is not retried until the next interval.
- **A volatile copy is repaired untested.** `QuarantineAction::Rebuild` restarts an ephemeral
  group empty for the leader to feed, and no fixture test drives it, since every fault verb
  needs an archive to fault.
- **Format 1 archives stay unverified until archive compaction runs**, which is after the next
  segment merge; an installation that never writes again keeps them, and the count of
  unverified reads is what says so.
- **Smoke numbers only.** The background arm ran at smoke scale on the development host, where
  every partition is resident and the scrubs read nothing off the disk; the capture is the
  benchmark host's.
- **Item 99's remainder**: a log truncated to a shorter one that still holds frames is not
  detected at open; the leader meets it as a reversion and feeds the member, and nothing
  counts it on the member.

## Invariants to uphold

- **`write_record` and `read_record` are the only record paths.** A record written elsewhere
  has no checksum and a record read elsewhere is not verified; both counters would lie.
- **An archive's format is fixed for its handle's life and decided from its header.** A format
  1 archive is never written to again, and a format 2 one never loses its header.
- **A corrupt record is never a source and never laundered.** The cut and the archive
  compaction fail on it rather than pass it on.
- **The scrub is applied in committed order and its cut taken before `applied` moves.** The
  canonical cut is the state at `B` because nothing has applied `B+1` yet; the task's reads
  are of that state because the handles were collected then and no record is rewritten in
  place.
- **A scrub entry reaches no compactor.** The WAL's index marks it log-alone, and the
  compactor skips one that reached it anyway.
- **Quarantine is set before the parked queries are answered, persisted before it is
  answered over the lane, and lifted only by a verified repair or an operator.** The marker is
  what a restart reads; the committed copy is routing advice.
- **The judge trusts a strict majority of the replica set or the operator's source, and
  nothing else.** An unresolved split quarantines nothing but what the checksums said and
  installs nothing.
- **Progress is committed before the step it names is taken, and `Done` stays done.** A late
  driver cannot move a done group; a phase the shard committed outranks a map that is behind.
- **A repair stream is judged against the target's checkpoint, and the checkpoint is held
  from acceptance to the restart.** A boundary at or below the checkpoint is `Behind`, never
  installed.
- **The group is restarted from its checkpoint with the file pending, resident partitions
  dropped, and parked applies discarded.** Anything else is a state openraft's restore would
  not install into, or a batch applied to a state it was not read from.
- **The segments above a repair's boundary are handed to the compactor again for that group
  alone.** Without it the writes the old generation had merged live only in resident copies the
  next eviction drops.
- **A scheduled pass runs in verify mode.** The destructive half is an operator's.
- **The fixture's `DIGEST` is not the scrub's function.** C11 requires an independent fold.

## Performance

The background arm smoke-ran on the development host (europa: `powersave` governor, three
nodes of three shards, one run at a hundredth of the data, a twenty-four second run,
`--allow-dirty` to a scratch directory that was deleted). **Not a capture**: the shape of the
record and nothing about magnitude on the benchmark host, and the re-render is left for jove.
What the smoke run shows is on the [M8 section](../distributed/milestones.md#m8-repair) of the
milestones page; at that scale every partition is resident and `bytes` is zero, so the arm
priced the loop's hashing and the round of reports and not the disk.

The cost this feature adds to the ordinary path is one hash per partition read - gxhash64 over
the payload, on the loader's task - and eight bytes per record on disk. No capture was taken
for it: the read arms would show it, and the benchmark host's next capture is what does.
[O54](../appendix/optimizations.md#o54-a-scrub-reads-every-archived-partition-of-a-group-once-per-pass)
records the cost a scrub pays that an incremental digest kept in the map would not.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `repair_detects_corrupt_primary_and_preserves_evidence` | `shoal/tests/cluster_fixture.rs` | The corrupt primary repairs itself or keeps the lead, a three way split is resolved by a rule, something is installed on an unresolved split, a named source does not resolve it, or the history read through every node is not sequential |
| `canonical_digest_ignores_archive_layout_at_same_boundary` | `shoal/tests/cluster_fixture.rs` | Replicas merged differently disagree at one boundary, a forgotten or erased partition agrees, a corrupt record is not found invalid, a verify does not quarantine a divergent copy, the frame does not name it, a release does not lift it, or a quarantine does not outlive a restart |
| `corrupt_follower_is_quarantined_and_repaired_from_a_verified_source` | `shoal/tests/cluster_fixture.rs` | A corrupt record is served, the copy is not quarantined, the repair installs on the wrong node or from the wrong source, the quarantine is not lifted, the digests disagree afterwards, or the held checkpoint never moves again |
| `repair_install_is_atomic_at_every_crash_point` | `shoal/tests/cluster_fixture.rs` | A target killed at any of the seven points comes back unquarantined, with a mix, or does not converge under a second repair |
| `durable_log_reversion_is_fed_not_fatal` | `shoal/tests/cluster_fixture.rs` | [Item 99](../appendix/resolved/durable-log-reversion.md) returns, or a lost log is not counted |
| `repair_is_authorized_versioned_and_resumable_by_id` | `shoal/tests/cluster_fixture.rs` | A non-admin, a stale version or a repeat is answered wrongly, a record differs between nodes, or a control leader killed mid-scrub leaves the record unfinished |
| `scheduled_scrub_quarantines_without_an_operator` | `shoal/tests/cluster_fixture.rs` | A scheduled pass quarantines a clean copy, misses a divergent one, or installs |
| `archive_records_are_checksummed_and_a_flipped_byte_is_refused` | `shoal-core/src/server/tables/storage/fs/tests.rs` | A record loses its header or checksum, a flipped byte is served, or the failure is retried |
| `a_format_1_archive_reads_unverified_and_is_counted` | `shoal-core/src/server/tables/storage/fs/tests.rs` | An archive from before checksums stops reading, or its reads stop being counted |
| `a_torn_record_is_refused` | `shoal-core/src/server/tables/storage/fs/tests.rs` | A short read is served as a record |
| `checkpoint_and_retries_are_checksummed` | `shoal-core/src/server/wal/tests.rs` | Either file loses its checksum, a torn one is read, or one from before stops reading |
| `canonical_fold_is_layout_and_order_independent` | `shoal-core/src/server/replication/digest.rs` | The fold depends on insertion order, two rows run together, a tombstone counts, or the schema or the tablets stop being folded in |
| `the_judge_needs_a_majority_or_an_operator` | `shoal-core/src/server/shard/repair.rs` | The judge trusts less than a majority, ignores a source, resolves a split, or does not quarantine an invalid copy |
| `the_repair_block_parses_with_its_defaults`, `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | The block's defaults or bounds change |
| `background_capture_records_scrub_interference` | `shoal-bench/src/workloads/harness/background.rs` | The record loses its marks, windows or series, or an F43 record stops loading |
| `the_background_arm_shares_the_replication_placement` | `shoal-bench/src/workloads/cluster_background.rs` | The arm drifts off the durable placement, asks for a fault, or leaves registry order |
| `acceptance_tables_have_unique_tests_and_valid_milestones` | `shoal-bench/tests/acceptance_tables.rs` | An M8 row's test stops existing |

## Related

[C9. Operations](../distributed/operations.md#repair), [C7. Primary failover and recovering a node](../distributed/failover.md),
[C11. Testing](../distributed/testing.md#digests), [C10. Performance](../distributed/performance.md),
[C13. The protocol](../distributed/protocol.md#q12-at-m8), [F43. Recovering a node brought back online](node-recovery.md),
[F40. Replication and quorum writes](replication.md), [F39. Membership](membership.md),
[Resolved #99](../appendix/resolved/durable-log-reversion.md),
[Storage: archives and the map](../storage/archives-and-map.md),
[Configuration](../getting-started/configuration.md), [Observability](../operations/observability.md).
