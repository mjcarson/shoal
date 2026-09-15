# F47. Changing a node's core count: slots, hosting and the rehome

## Context

Since [item 11](../appendix/resolved/tablet-ring.md) a storage directory has carried the shard
count that wrote it, and since [F37](node-identity-control-plane.md) the marker refused a
directory reopened under another: `ShardCountMismatch`. The refusal was honest. A shard was
three things at once - the core it ran on, the name every one of its files carried
(`Shard-N`), and on a cluster node the shard in every address a peer recorded and the modulus of
the placement rule ([C4](../distributed/tablet-map.md)) - so changing the core count changed
where every partition was looked for and, on a cluster node, the identity of every replica set
on every peer. [C8](../distributed/rebalancing.md#slots-executors-and-the-rehome) said what it would
take to retire it: a startup executor over the vanished shards' files, a manifest describing
the rehome, transfer that is atomic and resumable, and the crash tests that prove it. [M9c](../distributed/milestones.md#m9c-change-local-shard-count)
is that gate, and asks for the resource and startup costs to be recorded.

Decided with the user on 2026-09-14, and the decision that shapes everything below: a cluster
node's *slots* are claimed once and never move, because every peer's WAL, checkpoint and
retry sidecar is keyed by group identities minted from them; what moves is which *executor*
hosts a slot, a table nobody else reads. Both directions are built. A standalone node hosts per
tablet, since it has no peers to keep anything still for. The cost evidence is the pool's own
report, printed by the fixture and carried by one C10 arm.

## What it does

### Slots and executors

A cluster node runs `resources.cores` **executors** and has `cluster.slots` **slots**, claimed
at the first claim of its directory and written into the marker's `shards`
(`shoal-core/src/server/meta.rs`). The slots are the `shards` in its member record, the shard
in every `ShardAddr` the rule mints for it, the rule's modulus `(t / N) % slots`, and so the
identity of every replica set it is in; a peer never learns anything else about it. Absent,
`cluster.slots` is one per core, which is today's layout at today's cost. Above the cores it
reserves headroom - a node claiming twelve slots on eight cores can later run twelve executors -
and below them it is refused (`SlotsBelowCores`), since every executor hosts at least one slot.
On an established directory a `cluster.slots` that differs from the claim is refused by name
(`SlotsFixed`), and so is a core count past the slots (`CoresExceedSlots`): growth past the
slots is [M9b](capacity-rebalancing.md)'s `Replace` onto a fresh identity. A standalone node's
slots are its executors; the marker's `shards` is only where its layout began.

The marker gains `physical: Option<usize>`, how many executors the files are laid out on;
absent means `shards`, which is what every marker before this meant, so the format stays 3. It
is the fourth field ever rewritten in place, by the rehome's finalize and by nothing else.
`Identity` carries `slots`, `physical` and `rehome: Option<PendingRehome>`.

### The hosting

`shoal-hosting.json` beside the marker (`shoal-core/src/server/hosting.rs`) says which executor
hosts each slot (`hosts`, a cluster node's dispatch table) and which owns each tablet
(`tablets`, a standalone node's ring). A directory without one is hosted as the identity:
slot `n` and tablet `t % n` on executor `n`, byte for byte what [`Ring::new`](../appendix/resolved/tablet-ring.md)
built before this existed, so a node that never changed its count routes exactly as it did.
`Hosting::plan(to, cluster)` is the deal: a shrink hands each item of a vanishing executor to
the least loaded survivor by count, in item order; a growth takes the highest item of the most
loaded executor and gives it to the least loaded until the counts differ by at most one. Ties go
to the lowest executor, so the plan is a function of the table and the count and a resumed
rehome plans exactly what the crashed one planned. On a cluster node the tablets are derived
from the slots afterwards so both halves agree.

On the hot path the hosting is read in five places. `Ring::with_placement` takes it, judges the
placement against the *slots*, and owns this node's tablets by `host_of_slot((t / N) % slots)`
while every remote slot stays its own contact (`ring.rs`); `TabletMap::{ring_for,
read_ring_for}` follow, mapping a local copy's slot to its host (`map.rs`). `rebuild_groups`
builds every group whose slot the executor hosts (`shard/groups.rs`), and the eight sites that
compared a leader or a move's source against the executor's address now use the group's own
member, `GroupSpec::me(node)`, which is the slot: `my_addr` is gone. The listener's four
dispatch sites - forwards, replication requests, snapshot begins and chunks - go through one
function, `dispatch_target`, which bounds a frame's slot by the slot count and hands it to the
host (`peer/listener.rs`). A dead executor is reported to the control plane once per slot it
hosted. `MemberRecord` gains `physical`, which `Members` shows and the planner's default weight
reads, since the cores are what do the work.

### The rehome

`ShoalPool::start` claims the directory with the cores and `cluster.slots`; a changed core
count is a `PendingRehome { from, to }` the claim reports rather than a refusal
(`a_changed_core_count_is_a_pending_rehome`). After the control plane starts - the map is what
says which slot every group is on - and before any shard opens a file, `Rehome::run`
(`shoal-core/src/server/rehome/mod.rs`) runs on a dedicated executor pinned to one of the
shards' cpus, blocking the start until it returns, under the directory's lock. The plan is a
`Manifest` (`shoal-core/src/server/rehome/manifest.rs`) at `shoal-rehome.json`: the
hosting before and after, the steps in order, the report so far, written whole before the first
file moves and rewritten whole and atomically after every step is durable.

The steps, and why the order is the order:

- **Fold** (standalone): a source executor's intent logs of a table - the inactive ones in
  generation order, then the active one - are compacted into its archives by a compactor built
  for the purpose over the executor's map and shut down after (`FileSystemCompactor::fold`),
  reached through the derive's new `ShoalDatabase::fold_intents` so the executor needs no
  table. Every fold before any copy, so a source's data is archives and a map when it is read.
- **Archives**: the source's records whose tablet (standalone) or slot (cluster) now belongs
  to a destination are read verified through `read_record` and written as fresh records through
  `write_record` into one new archive on the destination, whose map is then saved. The
  destination's own intent log is folded into its map first, so nothing older replays over the
  save. The archive's id is on the manifest before the first record: a redo finds it either in
  the destination's `all_archives` - the copy finished, and its records still count - or not,
  in which case the partial file is removed and the copy made again.
- **Log** (cluster): every group the source's WAL, checkpoint, sidecar, quarantine or retired
  marker names whose slot now hosts on the destination is appended to the destination's WAL
  through the same `GroupStore` openraft writes through - the entries above what the
  destination already holds, then the vote, the committed index and the purge point - and its
  checkpoint and sidecar entries and its markers moved with it, the sidecar written before the
  checkpoint as the shard writes them. A partial install (`install/<g>.pending`, `.part`) is
  dropped and counted: the leader feeds the group again. A group already at the source's last
  index appends nothing, which is what makes the step idempotent.
- **Reclaim**: a vanished source's files go whole - the archives its map names, the map, its
  intent logs, its WAL directory; a live donor's moved entries leave its map (`remove_partition`,
  then the map saved and its intent log fresh) and its moved groups are forgotten in its WAL with
  their checkpoint, sidecar and markers dropped. A donor's archives are not rewritten: they hold
  dead records until its own compaction ([O58](../appendix/optimizations.md#o58-a-rehomes-moved-records-are-copied-and-a-donors-archives-keep-the-dead-ones)).
- **Finalize**: the hosting after is written, the marker's `physical` moves, the manifest is
  removed.

A group or tablet the map does not name - a copy retired before the restart - belongs to no
slot; a vanishing source's are carried to its first destination so nothing is abandoned, a
donor's stay. Ephemeral tables move nothing: their groups are re-fed by their leaders. The
report - `from`, `to`, `tablets_moved`, `slots_moved`, `groups`, `records`, `bytes`, `folded`,
`installs_dropped`, `steps_redone`, `millis` - accumulates in the manifest, is logged at INFO
and is the pool's `rehome()` for the fixture's `REHOME` verb and the arm.

### The refusals that remain

A manifest on disk towards one count refuses a start under any other (`RehomeInProgress`,
naming the count to start with) and is resumed by its own: a start that dies at any point is
finished by the next start at the same count, with `steps_redone` counting the step it began
again. A hosting file whose slot count disagrees with the marker refuses the start. Nothing
else about a directory's count is refused any more.

### Found on the way

[Item 111](../appendix/resolved/archive-removal-borrow.md): `ArchiveMap::remove_archive` held
`loaded_archives.borrow_mut()` across `close().await`, so a read landing on the executor
meanwhile panicked it. Four slots on one executor put enough reads beside a compaction to hit
it in the matrix's second round; the handle is now taken out before the await, and the
reproduction runs against the map alone.

## Design choices

- **Slots are the unit peers know, executors the unit that runs.** Every alternative that let
  a node's recorded shard count move re-cut every set's identity on every peer, which is a
  cluster-wide migration for a local decision. Keeping the slots still makes a core count change
  invisible on the wire: no frame, no record and no identity carries an executor number.
- **Per slot on a cluster node, per tablet standalone.** A cluster node's frame names a slot and
  has to dispatch with no tablet knowledge - a replication request carries a group, not a key -
  so the hosting is per slot and the balance is per slot; twelve slots on eight executors is
  four executors with two slots and four with one. A standalone node has no such frame and
  hosts per tablet, which is exact at any count and unbounded in growth.
- **Default slots are the cores.** A node that never sets `cluster.slots` claims one per core,
  runs one per core, and pays nothing - the identity hosting is the ring of old. Headroom is a
  choice the operator makes at the first claim, in the config, and is refused everywhere else.
- **The plan is a manifest, planned whole, marked per step.** A crash between any two steps
  finds the plan it was in the middle of rather than recomputing one, and a resumed plan is the
  same plan since the deal is deterministic. The mark comes after the durable write, so every
  crash is a redo of exactly one step, and every step is idempotent when redone.
- **Copy, not share.** Archive files are table-wide and could have been re-pointed rather than
  copied. They were not: `all_archives` is per executor and a compactor deletes what it owns, so
  two maps naming one file would race to unlink it. The copy is the cost recorded as O58.
- **The rehome runs before the shards, on one core, blocking.** Nothing serves while files
  move, so nothing can read half a move; the executor pool is not started until it returns; and
  the directory's lock is already held. Concurrency would buy start time and cost the
  simplicity the crash matrix relies on ([O59](../appendix/optimizations.md#o59-the-rehome-runs-on-one-core-and-blocks-the-start)).
- **`physical` on the marker, not a layout bump.** The files are the same files on more or
  fewer executors; which executor owns a tablet is the hosting's to say. Layout 2 stays the
  cluster node's shared-WAL layout, and a rehome changes no layout.
- **Ephemeral tables move nothing.** Their memory log is gone at a restart either way, and
  their groups are re-fed by their leaders exactly as after any restart.

## Alternatives rejected

- **Letting the recorded shard count move with the cores.** Every peer's `GroupId::of(table,
  rule_members)` is a hash over `ShardAddr { node, shard }`; a node whose shard count changed
  would mint different identities for the same sets, and every peer's WAL, checkpoint and
  sidecar is keyed by the old ones. Re-cutting them is M9a's move for every set at once.
- **Rewriting the WAL instead of appending through the store.** A byte-level copy of a source
  segment's frames into the destination's segments would have to reproduce the index, the
  purge and truncate markers and the per group cache; appending through `GroupStore` reuses the
  one writer openraft uses and leaves the destination's segment rotation to itself.
- **Per tablet hosting on a cluster node.** Exact balance, but a frame naming a slot cannot be
  dispatched by a tablet it does not carry; every replication frame would need the tablet
  added, or the dispatch would need the group's tablets, which the listener does not have.
- **Keying every file by tablet instead of by executor.** Four thousand and ninety-six
  directories per table, a rewrite of every path in the engine, and a rehome that moves
  nothing at the price of everything else moving. The hosting achieves the same indirection in
  one file.
- **A live rehome while serving.** A move under writers is what M9a builds for a cluster; a
  local one would need the same lock and the same phases for a saving in start time that O59
  records and nobody has asked for.

## Limitations

- **The slots are a ceiling.** A cluster node cannot run more executors than it claimed slots
  at its first claim; past them is `CoresExceedSlots`, and the way up is a `Replace` onto a
  fresh identity claimed with more.
- **Balance is per slot on a cluster node.** Twelve slots on eight executors is uneven by
  construction - four executors carry two slots - and the deal counts slots, not bytes. A
  byte-weighted deal is a planned follow-up ([todos](../appendix/todos.md)).
- **`storage.tables` roots are not covered.** The rehome resolves a table's settings by name
  through `table_settings` and moves files under the table's own paths, but every crash test
  runs with one storage root; a table under a second root is untested
  ([item 43](../appendix/known-issues.md) stays open and says so).
- **Partial installs are dropped, not carried.** A snapshot half received when the node stopped
  is fed again by the leader; `installs_dropped` counts them.
- **Groups cost what they hold.** The Log step copies every retained entry above the purge
  point through the store; a group with a long retained log is a long step.
- **A donor's archives keep dead records** until its compactor rewrites them (O58).
- **Nothing serves during the rehome**, and it runs on one core (O59). The arm's `millis` is the
  price.
- **The cluster crash matrix runs on the fixture's cores**: node two at four slots on two cores,
  moved between one and two executors, not the plan's four on four; the vanishing and the live
  donor paths are both crossed at every point, at the smallest layout that has both.

## Invariants to uphold

- **A cluster node's `shards` in its marker is never rewritten**, and neither is the `shards` in
  its member record. Every identity a peer holds depends on it. `physical` is the field that
  moves.
- **A peer names slots and never executors.** No frame, record, identity or hello carries an
  executor number; `dispatch_target` is the one place a slot becomes an executor on the way in,
  and `host_of_slot` in the ring builders is the one place on the way out.
- **Every group's address on this node is `spec.me(node)`, the slot.** A comparison against
  `current_leader`, a move's `from`, a scrub's member or a report's `is_leader` that used the
  executor id would be wrong the first time an executor hosted two slots.
- **The manifest is written before the first file moves and after every step is durable, never
  between.** A step's effect is either not on disk or marked done; the crash points sit exactly
  in that gap, and every step redoes cleanly from it.
- **The deal is deterministic.** `Hosting::plan` is a function of the table and the count with
  no clock, no randomness and no dependence on the files; a resumed rehome must plan what the
  crashed one planned, and it does only because of this.
- **A vanishing source's every item goes somewhere.** An item the map does not place is carried
  to the source's first destination; nothing is left for the reclaim to delete unseen.
- **The Reclaim of a source comes after every Archives and Log step from it, and Finalize
  after every Reclaim.** The manifest plans them in that order and `next_step` runs them in
  that order; a reordering would delete what has not been copied or publish a hosting whose
  files are not yet where it says.
- **The archives step folds the destination's intent log before it saves.** A saved map with an
  older intent log beside it replays the log over the save; an entry in it for a key the copy
  just moved would point the key back at a stale or missing record.
- **`ArchiveMap::remove_archive` takes the handle out of the cache before it awaits** (item
  111). A borrow across that await panics whichever read lands during it.

## Performance

The cost is one arm, `macro/rehome/shrink`, under the `rehome` family
([C10](../distributed/performance.md)): the one node cluster arm seeded at twelve executors,
stopped, and started again at eight, so the start between runs a rehome of four executors'
files onto the eight that remain. `cluster.rehome` is the pool's report carried whole, and
`millis` - how long the start was held - is the number; the mixture after it is the reference
mixture on eight executors hosting twelve slots and is not read against the reference cell.
`ConfOverrides.restart_shards` is the axis the harness applies to the server that comes back.

**Smoke-run on the development host only** (europa, `--scale smoke --runs 1 --allow-dirty` into
a scratch directory under `target/` that was deleted): twelve to eight, four slots and eight
groups moved, no records - at smoke scale nothing was archived before the stop, so the copy was
the WAL half alone - in 589 ms of start. The fixture's matrix prints the same report on a node
of four slots and two tables: a shrink of two slots moves six groups and twenty-two to
thirty-two archived records in 226 to 309 ms on this host, and a growth the same in 235 to
301 ms. Shape, not magnitude; the capture is the benchmark host's.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `local_rehome_recovers_after_each_crash_point` | `shoal/tests/cluster_fixture.rs` | A node dies at one of the six points and does not finish the rehome on the next start, a key or the remembered identity is lost, a group's row counts disagree across the holders, a vanished executor's files are left, the hosting or the member record names the wrong counts, or the writers' history is not sequential |
| `standalone_rehome_rebalances_tablets_across_restarts` | `shoal/tests/cluster_fixture.rs` | A growth deals tablets unevenly or loses a row in an intent log, a shrink through two crashes does not redo both steps, or the vanished executors' files remain |
| `a_changed_core_count_rehomes_and_reads_back` | `shoal/tests/storage_meta.rs` | An in-process start at another count is refused, a row is not read back at 3 or at 1, or the survivor is not the only executor with files |
| `a_changed_core_count_is_a_pending_rehome` | `shoal-core/src/server/meta.rs` | A changed count is refused, `physical` moves before the finalize, a manifest towards another count is not refused by name or is not resumed by its own |
| `slots_are_claimed_once_and_bound_the_cores` | `shoal-core/src/server/meta.rs` | Slots below the cores, a changed slot count or cores past the slots start, or headroom is not recorded as laid out on the cores |
| `the_identity_hosting_is_the_ring`, `hosting_deals_vanished_shards_to_the_least_loaded`, `a_hosting_file_round_trips` | `shoal-core/src/server/hosting.rs` | The identity hosting differs from the ring, a deal is uneven, moves an item that did not have to move, is not deterministic, exceeds the slots or leaves an executor empty, or a torn file is read |
| `the_plan_orders_fold_archives_log_reclaim_finalize`, `a_manifest_resumes_at_its_step` | `shoal-core/src/server/rehome/manifest.rs` | A reclaim is planned before a copy, a fold after one, a log on a standalone node, a resumed manifest begins at the wrong step, or a finished one is not gone |
| `a_redone_archives_step_removes_its_partial_archive`, `a_redone_log_step_skips_a_group_already_moved`, `a_manifest_for_another_target_is_refused` | `shoal-core/src/server/rehome/tests.rs` | A partial archive is left or a finished copy is made again or not counted, a redone log step duplicates entries or touches another slot's group, or a claim under a third count starts over a manifest |
| `a_ring_from_hosting_routes_by_the_table`, `a_placement_hosts_slots_on_executors` | `shoal-core/src/server/ring.rs` | A standalone ring ignores the hosting, a placement judges the executors rather than the slots, or a local tablet is owned by its slot rather than its host |
| `a_placement_hosts_slots_on_executors` | `shoal-core/src/server/map.rs` | The read ring serves a local copy from the wrong executor, a remote copy moves, or the groups do not split across the executors by the hosting |
| `the_listener_dispatches_a_slot_to_its_host` | `shoal-core/src/server/peer/tests.rs` | A frame naming a slot reaches the wrong executor, or a slot past the count is accepted |
| `removing_an_archive_does_not_hold_the_handle_map_across_the_close` | `shoal-core/src/server/tables/storage/fs/map.rs` | Item 111 returns: a read during an archive's close panics the executor |
| `rehome_capture_records_the_move` | `shoal-bench/src/workloads/harness.rs` | The record drops a count or an older capture fails to load |
| `the_rehome_arm_restarts_at_fewer_shards` | `shoal-bench/src/workloads/cluster_rehome.rs` | The arm does not restart, restarts at as many shards, or is not a cluster of one |

## Related

[C1](../distributed/node-identity.md), [C4](../distributed/tablet-map.md),
[C8](../distributed/rebalancing.md#slots-executors-and-the-rehome),
[C10](../distributed/performance.md), [F37](node-identity-control-plane.md),
[F40](replication.md), [F45](replica-migration.md), [F46](capacity-rebalancing.md),
[item 11](../appendix/resolved/tablet-ring.md), [item 45](../appendix/resolved/storage-marker-format.md),
[item 111](../appendix/resolved/archive-removal-borrow.md),
[O58](../appendix/optimizations.md#o58-a-rehomes-moved-records-are-copied-and-a-donors-archives-keep-the-dead-ones),
[O59](../appendix/optimizations.md#o59-the-rehome-runs-on-one-core-and-blocks-the-start).
