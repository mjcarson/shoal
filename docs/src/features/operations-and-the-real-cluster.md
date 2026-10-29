# F93 — M21, operations and the real cluster

## Context

[M21](../object-storage/milestones.md#m21-operations-and-the-real-cluster) is the last gate of the
object store. Everything before it built what a cluster does with an object; this one builds what an
operator does with a cluster that holds them, and settles what the four operations that predate
objects mean once a bucket exists. [Q31](../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)
decided those four by design on 2026-10-17, after its lab run reproduced the collision:

- **a schema change** that appends tables or buckets is made rolling, judged by the schema's
  lineage, and committed by `ExtendSchema`;
- **a restore** is judged table by table, by each table's own identity, which closes
  [item 255](../appendix/resolved/restore-judged-by-table.md) for an addition;
- **a backup of a bucket** is a backup of its objects, never of its rows;
- **a stripe ahead of its row**, which only a `force_recover` that lost a write leaves, is refused
  by name and settled by an operator's `ResolveStripe`, which closes
  [item 256](../appendix/resolved/stripe-ahead-of-its-row.md).

The rest of the gate is the operator's surface [S14](../object-storage/operations.md) asked for:
every admin kind versioned, idempotent, authorized, audited and advertised; a bucket's figures beside
a device's and a pool's; pools in the wizard, the stats view and the cluster tab; activation on a
running cluster; and the six procedures of day two as [runbooks](../operations/runbooks.md).

Three decisions were the user's, on 2026-10-17:

1. **Activation is objects by an append.** A cluster of this tree whose schema has no bucket is
   rolled onto a build that appends one, and `ExtendSchema` commits its pool and its binding. No
   build from before F80 is proven: wire 15 keeps no older reader, as 11, 12 and 14 did, and a
   cluster on wire 14 is bootstrapped again, and since
   [Resolved #270](../appendix/resolved/no-committed-schema-refused.md) a node refuses to start, by name,
   on a log initialized with no committed schema.
2. **A scrub is narrowed to any of four targets**: a pool, a bucket, a device or a slice.
3. **`shoaladm upgrade` connects with an ancestor fingerprint**: the new admin program presents the
   committed schema's fingerprint, taken from its own lineage, to nodes that still run the old one.

The plan's recommended decisions were taken as they stood: the query enum's payloads boxed, a table's
own fingerprint everywhere a schema id stood for a table, `ExtendSchema` judged on committed member
records, the archive's frame fixed now, one state per archived object, the new refusals named, and
item 257 fixed here because the exit's restore leans on it.

## What it does

### A schema's lineage

`#[shoal::db]` already folded each field into a running FNV hash to make `SCHEMA_ID`. It now emits
the hash after every field as `SCHEMA_LINEAGE`, whose last element is `SCHEMA_ID`, and the same with
`CLIENT_WIRE_VERSION` mixed in as `FINGERPRINT_LINEAGE`, whose last element is the client's
fingerprint. An append, a table or a bucket declared after every existing field, extends the lineage
and keeps every element of it; any other change, a field removed, renamed, reordered or retyped,
changes it from that field on. `shoal::shared::lineage` judges two lineages:

| Judge | True when |
| --- | --- |
| `compatible(ours, theirs)` | one is a prefix of the other, either way round, judged where they overlap: a hello's lineage cut to its last 255 ids and a bare id are windows that end at their schema ([Resolved #271](../appendix/resolved/lineage-judged-where-it-overlaps.md)) |
| `holds(lineage, id)` | the lineage names the schema `id`, so the build serves that schema's tables |
| `ancestor(lineage, id)` | the prefix of the lineage ending at `id` |

Each **table** has an identity of its own, `table_fingerprint`: a hash of its name, its type's
spelling, its row's fingerprint and its projections, with `OBJECT_ROWS_LAYOUT` mixed in for a
bucket's generated tables. It stands where a schema id stood for one table: the snapshot header and
manifest, the install's check, the scrub digest's prefix, a backup's manifest and an export. So two
builds of one schema agree about every table they share, and disagree about a table whose row
changed.

### Bundles across builds

An appended table's row may be wider than any before it, and `Insert { key, row }` archived the row
inline in a `Vec<QueryKinds>`: an append changed every bundle's stride, so a v1 client's bundle read
by a v2 node, or forwarded between the two, was garbage. **Every `QueryKinds` variant's payload is
boxed** (`#[rkyv(with = AsBox)]`), so the archived enum is a tag and a relative pointer whatever its
tables, and a const assert holds its size and alignment to `QueryShape`'s. **`ResponseKinds` is
ordered a table then its projections**, where it had every table then every projection, so an
append renumbers nothing. `CLIENT_WIRE_VERSION` went from 4 to 5; every `SCHEMA_ID` kept its value
and every fingerprint was frozen again.

### The hello, the join and a client

- **A peer's hello** carries its sender's lineage in the hello's growth, and its ack carries the
  answerer's, since a dialler read the ack at its fixed length and dropped what followed. A peer is
  admitted when the two lineages are `compatible` and it `holds` the schema the cluster committed;
  one that does not is refused `BelowCommittedSchema` (16) by name.
- **A join** commits the joiner's lineage in its `MemberRecord`, and every start commits it again
  through `ObserveMember`, so the control state knows what each member's last start ran.
- **A node** whose own build does not hold the committed schema stops itself at its start, and on
  the apply that commits one past it, with `SchemaBelowCommitted`.
- **A client** is admitted when its fingerprint is in the server's `FINGERPRINT_LINEAGE`, and the ack
  echoes the client's own, so a v1 client connects to a v2 node and is served v1's tables.
  `ShoalBuilder::present_ancestor(fingerprint)` makes a v2 client connect to a v1 node as v1, and
  refuse locally any query of a table past that ancestor; `shoaladm`'s admin connections do this
  when a node answers `SchemaMismatch` naming a fingerprint in the build's own lineage.

### The committed schema and `ExtendSchema`

`ControlState.schema` is a `SchemaRecord`: the id, the lineage and every table's fingerprint.
`Initialize` commits the bootstrapper's; `ExtendSchema { schema, pools }` moves it, and is refused by
name unless, in order:

1. wire 15 is activated;
2. a schema is committed, and the request's version is the topology's (the same schema again is
   applied as a no-op);
3. the target `extends` the committed schema, its tables and buckets the committed ones and more;
4. every member that is not removed holds it, as its last start recorded;
5. no move or repair is open, since each records its groups when it is applied;
6. the pools sent keep every committed pool and binding as it is, and bind every bucket the target
   declares (`PoolsConf::check`).

Applied, the new tables enter the placement's table list, which is what places their groups, and
each new bucket is bound with a consumer id minted past every committed one. A new binding records
`since`, its pool's generation then, and a stripe of it places at `max(pin, since)`, so its groups do
not place at a pin older than the bucket. `fit_pools` keys its fit on the committed policy as well
as the generation, so a pool the extension adds is fitted at once. A node refuses a query of a table
its build declares and the cluster has not committed, `UncommittedTable` (57), and an object
operation on such a bucket `UnknownBucket`, naming `ExtendSchema`. Each shard reads which tables
those are from the map it starts with as well as from every map pushed after it
(`TabletMap::uncommitted`): until its first push it once routed such a query and answered
`StaleTopology` ([Resolved #325](../appendix/resolved/uncommitted-from-the-first-map.md)). `TopologyView.schema` carries the
committed id, and `Members` names each member's.

`shoaladm upgrade` writes the inventory's pools and bindings into each node's file with its new
program, and only those keys: a build that appends a bucket starts only with the bucket bound, and
the build before it refuses a binding its schema does not declare, so the binding cannot go ahead
of the program. `--activate` sends `Activate`, then `ExtendSchema` with the inventory's pools when
every member runs the build and it appends to the committed schema; without `--activate` it says
which schema the cluster can be extended to. An inventory names `auto_remove_after`, the grace a
down member is removed after, rendered into the cluster block. `--rollback` warns once a schema is committed, since a
node rolled back below it stops at its start. `extend-schema` is an action of `shoaladm admin` and
the cluster tab.

### A restore judged table by table

`scan_backup` returns each file's table and fingerprint, and each `.objects.json` beside them. The
restore's apply judges each file by `judge_table`: a bucket's generated table is refused by name,
whichever schema cut it; a table the destination serves at the same fingerprint is restored; a table
it serves at another fingerprint is refused, naming the table and both shapes; a table it does not
serve is refused. Nothing compares the two schemas' ids, so a backup cut before an addition restores
into the schema after it, its new tables empty, and the placement state a source group held is never
carried. A file of zero rows beside archives is a backup, where a backup of no `.snap` files was
refused.

### A bucket backed up as its objects

`apply_backup` marks every `StripeMeta` group `Skipped` with its reason and records every
`ObjectMeta` group in `BackupRecord.archives`. The group's leader walks the tablets it leads
([F87](tablet-walk.md)) and writes every live object to `<path>/<op>/<Bucket>/<group>.objects`, its
manifest of kind `objects` beside it, which since [Resolved #315](../appendix/resolved/archive-coverage.md) names the tablets the group
served:

```text
 header   b"SHOALOBJ" | format u16 = 1 | reserved (6 B) | bucket id u64 | group u64 | op (16 B)
 record   entry length u32 | entry (JSON) | object length u64 | the object's bytes | CRC-64/NVME u64
 trailer  b"SHOALEND" | records u64 | CRC-64/NVME of every byte before the trailer u64
```

The entry is kept whole except its inline bytes, which are the record's; nothing a cluster minted is
in it but the source's object id, which a restore does not keep. **Each object is read at one
state**: the entry and its stripe rows are read at `Quorum`, every stripe is read at the labels those
rows name, and the entry and rows are read again after the bytes; an object that moved is read again
from the start, cut back to its record's beginning, up to a bound, past which the group's archive
fails by name. Since
[Resolved #322, in part](../appendix/resolved/archive-refusals-retried.md) a read refused for now, or an object that kept moving,
leaves the group writing, and its archive is driven again from the start after five seconds, up to
twelve tries; a written archive says how many it took ~~(`retries`)~~ (`attempts`, since
[the rest of item 322](../appendix/resolved/backup-outcome-record.md), which tries a table group again
too). A restore deals the archives round robin over the destination's ~~placement~~ placeable
members once its tables are done, and again to another when an archive's node leaves
([Resolved #263](../appendix/resolved/archives-dealt-again.md)), and shard zero of each node named puts every object again through its own
coordinator, into the destination's pool, under an operation id derived from the restore, the
archive, the record and the attempt, so a retry is answered "done". Each archive's progress,
`ArchiveRestore`, is committed by its driver every 32 records, and `restore-retry` drives a failed
archive again. ~~so a retry is answered "done"~~ A retry is answered "done" while the path holds
what the restore put; since [Resolved #261](../appendix/resolved/archive-driven-once.md) a put again
goes over no live object it did not make, and an archive whose end a driver committed is not driven
again while its node's map is behind. Since [Resolved #315](../appendix/resolved/archive-coverage.md) the scan also judges every
bucket's archives whole before the restore is proposed: every tablet of its `ObjectMeta` table in
exactly one archive. A group whose archive failed leaves no manifest, so its tablets are a gap,
refused by name, where the restore used to finish without its objects. A bucket whose archives were
all written before they named their tablets restores as before, unjudged, with a warning. Since
[Resolved #322](../appendix/resolved/backup-outcome-record.md) the backup's record beside its files is
read first: a bucket every archive of which failed, which leaves no manifest at all, is refused by
the record's name of the failure, and an archive written and not gathered is refused as the backup
not gathered whole, naming the node that wrote it.

### A stripe ahead of its row

A label is applied only after its commit, so a holder ahead of the row it was read under means
another write committed meanwhile, or a commit the row lost. `Label::ahead_of` is the one judge:
not the label named, and not older. A read, a write, an erasure coded read and a rebuild that meet
one read the rows again at `Quorum`; rows unchanged are behind their chunks, which only a
`force_recover` that lost a write leaves, and the operation is refused **`Behind` (13)** by name, the
message naming each stripe's row sequence. A rebuild stops over an ahead holder rather than copying
over it, a light and a deep scrub count the chunk `behind` and keep it, and a move waits.

`ResolveStripe { bucket, path, stripe, accept | lost }` is answered by the shard the frame reached,
as an object operation is, for an admin alone. Its expected version is the stripe row's sequence the
refusal named, and its commit is conditional on it under the operation's own id:

- **`accept`** asks every holder what it applied, takes the label at least `k + f` positions hold
  past the row, commits it with every other position missed for recovery to rebuild, and claims the
  group's epoch;
- **`lost`** sets `StripeRow.lost`, after which every read and write of the stripe is answered
  **`Lost` (14)** and its bytes are reclaimed with its object.

A request against another sequence is answered by the operation's identity first: a repeat of a
committed resolution is `Repeated` at the row's sequence, and anything else is a stale version.

### The operator's surface

- **Every mutation is versioned and remembered.** `ScrubNow` and `FailDevice` gained an operation id,
  a principal and an expected version, so a scrub or a failure asked by an operator is refused
  against a stale version and answered `Repeated` for its id, as every other mutation is; the
  cluster's own, a lost journal's scrub and an I/O error's failure, carry the nil id and are not
  remembered.
- **Every mutation is advertised.** `TopologyView.admin_mutations` lists the kinds a node takes,
  beside `admin_reads`, and `shoaladm` and the cluster tab check a kind against it before sending a
  new one.
- **Every refusal is audited**, the shard's `Unauthorized` and `reload_tls` among them.
- **A scrub is narrowed** by `Scrub { pool, kind, target }`, the target a bucket, a device or a
  slice. The request still counts on its pool, so every group of the pool serves it and its pending
  figures stay right, and the newest request's target is kept with the count; a pass scrubs only the
  placement groups it names and counts only those. A target the pool does not hold is refused.
- **`BucketStats`**: each bucket's objects and bytes, counted by each shard's reclaimer pass over the
  entries it leads, and its operations by kind, with their bytes in and out, refusals and latency,
  counted where `ObjectTask::run` answers. Both ride each member's `StatusReport` and are summed by
  the control leader into the advertised `bucket_stats` read.

### The tools

- **`shoaladm admin` and the cluster tab** gain `extend-schema`, `resolve-stripe <bucket> <path>
  <stripe> accept|lost <sequence>`, `scrub <pool> [light|deep] [bucket <name> | device <id> |
  slice <id> <index>]`,
  `buckets [bucket]` and `devices`; the tab's model draws a line a pool from `pool_status` and the
  committed schema beside the wire.
- **The stats view** has a `pools` tab: each member's objects and bytes, its object operations,
  refusals and mean time, stale positions, moves behind, scrubs pending and their findings, and
  objects reclaimed.
- **The wizard** has a Storage page: the deployment's devices, the pools and the bindings, each one
  line of the inventory's own YAML, and devices and object cpus at every level. It judges them as a
  deploy does, against the buckets the project's schema declares, read from its source, and warns
  of a pool whose class no node's devices carry.

### Found on the way

- **[Resolved #257](../appendix/resolved/restore-waits-for-stream-budget.md)**: an install a member's
  stream budget refused failed the restore's group. `SnapshotAnswer::Busy` and `RepairSend::Busy`
  name the refusal, and the restore and repair drivers wait and try that member again within the
  transfer's deadline.
- **[Resolved #258](../appendix/resolved/idle-group-checkpoint.md)**: a group whose only frames in a
  segment are commands no compactor sees, an idle table's nudges, had its checkpoint moved only when
  the segment held no table's frames at all, so a backup of an idle table on a shard another table
  writes to never cut. The archive row met it.
- **[Resolved #259](../appendix/resolved/force-recover-applied-log.md)**: `force_recover` panicked on a
  control log the machine had applied to its end, which an idle control group leaves. The
  `Behind` row met it.

## Design choices

- **Lineage, not a version number.** A schema's lineage is computed from its declaration, so two
  builds agree on what they share without anybody numbering anything, and a change that is not an
  append is caught where it is made.
- **The extension is committed, never inferred.** Every node serves what its build declares only
  once the control state says the cluster does, so a rolled node of the new build serves nothing new
  until every member holds it, and no member rolls back past it once it is committed.
- **Members are judged on committed records.** `ExtendSchema` reads each member's lineage as its
  last start committed it, not as a hello reported it, so the judgment replays the same on every
  replica.
- **A table is judged by its own identity.** Snapshots, digests, backups and restores compare a
  table's fingerprint, never the schema's id, so an append changes nothing for the tables it leaves
  alone.
- **A resolution is versioned by the row it settles.** The topology's version moves for reasons
  that have nothing to do with a stripe; the row's sequence moves exactly when the stripe does.
- **A narrowed scrub rides its pool's request.** A target of its own would need its own pending
  count and cursor; ~~filtering the pass keeps every figure the pool already reports right~~
  filtering the pass keeps every figure the pool already reports right since
  [Resolved #266](../appendix/resolved/narrowed-scrub-widens.md): a narrowed pass keeps its groups'
  last pass times, and a request remembers where its target's run began, so one asked beside an
  unserved request of another target is a pass of the whole pool.

## Alternatives rejected

- **A schema version an operator bumps.** Nothing checks that the number moved when the schema did,
  and a forgotten bump is exactly the change that breaks a cluster.
- **Serving an appended table as soon as a node runs the build.** A table half the members serve
  splits every bundle that names it, and its groups would start on some members and not others.
- **Restoring a bucket's rows as table files.** A row restored without its bytes is older than every
  chunk the moment one changes, which P10 refuses, and a striped object is stranded at once;
  rejected by Q31.
- **Adopting a lost cluster's devices at restore.** It is an exception to the rule that a restore is
  a new cluster's; a backup of objects answers a lost cluster without one. Rejected by Q31.
- **Moving the row to the newest label on its own.** Nothing on a holder says what the object's
  entry, its size after an append among it, should be; an operator's word is the only thing that
  knows whether to trust the holders.
- **Writing archives on an object executor.** The plan put them there; they run on the executor's
  blocking pool instead, since a file's copy and its checksum block on the filesystem, and an object
  executor serves foreground I/O at a 100 µs goal. ~~Nothing of an archive runs on a shard's loop.~~
  A restore's checksums, its copy of an object into pieces and its manifest's read do run on shard
  zero's loop
  ([O117](../appendix/optimizations.md#o117-an-archive-restore-reads-each-archive-two-or-three-times-and-copies-each-object-on-every-try)).

## Limitations

- **Only an append is rolling.** A table removed, renamed or changed is a new cluster and a restore,
  recorded in [the todos](../appendix/todos.md#what-q31-left-for-later).
- **Two copies that each verify and disagree can only be declared lost.** `ResolveStripe` accepts a
  label past the row that `k + f` positions agree on, a short stripe's never stored chunks among them
  since [Resolved #265](../appendix/resolved/accept-counts-empty-chunks.md); it names no one copy, or an erasure coded
  stripe's data over its parity, to rewrite the others from
  ([todo](../appendix/todos.md#what-f93-left-for-later)).
- **A placement group holding a stripe behind its row waits.** Its move and its backfill wait on the
  stripe until an operator resolves it; nothing lists such stripes but a scrub's `behind` count and
  the refusals.
- **An archive is read through one coordinator.** A bucket's objects are read by the shard leading
  each `ObjectMeta` group, one object at a time, so a backup's rate is a group's, not a pool's.
- **A restore mints new object ids**, so anything outside the cluster that stored an object's id
  holds an id the destination never minted.
- **The wizard reads the schema's buckets from source** when the project can be read; a draft with a
  prebuilt program and no project is judged against its bindings alone, and says so.
- **No build from before F80 is proven**, by the user's decision: wire 15 keeps no older reader.

## Invariants to uphold

- **An append keeps every element of the lineage.** A derive change that moves a field's hash moves
  every schema's lineage, and every cluster would refuse every build of its own schema.
- **Nothing serves a table the committed schema does not name.** `refuse_uncommitted` runs on every
  client bundle before routing, and a bucket the policy does not bind is refused by name.
- **A table's fingerprint is the only identity a file or a digest carries.** A schema id in a
  snapshot header would refuse every install between two builds of one schema.
- **The query enum's archived size is fixed.** The const assert holds it to `QueryShape`; a variant
  archived inline would change every bundle's stride with the next append.
- **Every archived object is one state.** Its entry and rows are read on both sides of its bytes,
  and a record is cut back before the next is written; a record of two states is a corrupt object
  restored silently.
- **A restore judges every bucket's archives whole before it is proposed**, each tablet in exactly
  one, and an archive's manifest names the tablets its group served
  ([Resolved #315](../appendix/resolved/archive-coverage.md)). A restore that skipped a group it never heard of loses that group's
  objects and reports none failed. Since [Resolved #322](../appendix/resolved/backup-outcome-record.md)
  it judges the backup's record first, which names every group whether it wrote a file or not.
- **A resolution never invents a label** and never commits on a sequence it did not read; a repeat
  is judged by its identity before its condition.
- **A chunk ahead of its row is never removed.** A scrub, a reclamation and a rebuild keep it, since
  it may be the only copy of what the cluster acknowledged.

## Performance

### In the fixture

On europa at six test threads: the activation row in 7 s, the append under load in 18 s, the pool
admin row in 8 s, the table-by-table restore in 17 s, the archive row in 12 s with nine rounds of
writes in place during its backup, the stream budget row in 24 s, the narrowed scrub row in 134 s
(four light passes over 4,096 placement groups), and the stripe behind its row in 205 s, most of it
a deep pass.

### On the lab

*Recorded in a change of its own once the lab's runs are taken, as F86 to F92's were.*

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `cluster_fixture::objects_activate_by_an_append` | A cluster of a schema without a bucket cannot take one: a node of the build that appends it is refused, serves the bucket before it is committed, or never binds it |
| `cluster_fixture::an_appended_table_rolls_through_a_cluster_holding_objects` | A rolled node serves an appended table before `ExtendSchema`, places its groups early, loses a write of the load, or a node of the old build is admitted past the extension |
| `cluster_fixture::pool_admin_is_authorized_versioned_and_idempotent` | A pool or device mutation is applied for a non-admin, against a stale version, or twice for one id, or a node advertises no pool mutation |
| `cluster_fixture::a_restore_judges_each_table_by_its_own_identity` | A backup cut before an addition is refused, or one whose row changed is restored |
| `cluster_fixture::objects_survive_a_backup_and_restore_by_archive` | A bucket's backup holds no objects, an object restored is no state it held, or keeps the source's id |
| `cluster_fixture::rows_older_than_chunks_are_refused_by_name` | A stripe behind its chunks is served mixed, answered `Shedding`, its chunks removed, or a resolution applied twice or against a stale sequence; a backup holding a bucket's rows is restored |
| `cluster_fixture::a_narrowed_scrub_scrubs_only_its_target` | A scrub narrowed to a device or slice scrubs the whole pool, or an unknown target is accepted |
| `cluster_fixture::a_restore_waits_out_a_members_stream_budget` | A member at its stream budget fails a restore's group (item 257) |
| `cluster_fixture::an_idle_tables_backup_cuts_while_another_is_written` | An idle table's backup never cuts on a shard another table writes to (item 258) |
| `shoal-core control::store::tests::a_recovery_of_a_log_applied_to_its_end_applies_nothing_more` | `force_recover` panics on a log applied to its end (item 259) |
| `shoal-core control::types::tests::a_schema_extends_only_along_its_lineage_once_every_member_holds_it` | `ExtendSchema` commits a schema that does not append, or one a member does not hold, or forgets a consumer id |
| `shoal-core control::schema::tests::an_append_extends_its_base`, `a_file_is_judged_by_its_own_table` | A table is judged by the schema's id, or a bucket's rows pass as a table |
| `shoal-core objects::archive::tests::an_archive_holds_every_record_that_ended` | A torn record survives in an archive, or a bad checksum reads |
| `shoal-proto lineage::tests::a_prefix_is_compatible_and_a_change_is_not`, `a_lineage_holds_its_ancestors` | Two lineages that do not append are judged compatible |
| `shoal-proto peer` tests (`a_hello_carries_its_senders_lineage`) and `shoal-core peer::tests::a_peer_is_judged_by_its_lineage` | A hello drops its lineage, or a peer below the committed schema is admitted |
| `shoal fingerprint` tests (the `appended` module) | An append changes a table's fingerprint or a bundle's bytes, or a v1 bundle does not decode under v2 |
| `shoaladm` model, action and wizard tests | The tools send a kind a node does not advertise, render a read wrongly, or a draft's pools and bindings are judged otherwise than a deploy judges them |
| `shoaladm deploy::upgrade::reconfigure_tests::a_new_bucket_is_bound_in_the_file_and_nothing_else_moves` | An upgrade onto a build appending a bucket leaves the node's file without its binding, or rewrites more of it |

## Related

[Q31's record](../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)
for the decisions; [S14](../object-storage/operations.md) for the surface; [F49](backup-and-recovery.md)
for backups, restores and `force_recover`; [F48](rolling-compatibility.md) and
[F55](cluster-upgrade.md) for rolling upgrades and activation; [F79](bucket-schema.md) for a
bucket's tables; [F89](scrub-and-repair.md) for scrubs; [F92](reclamation-and-device-lifecycle.md)
for the device verbs; the [runbooks](../operations/runbooks.md) for day two.
