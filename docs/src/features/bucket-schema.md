# F79 — M12, buckets in the schema and the tables they generate

## Context

[M12](../object-storage/milestones.md#m12-tables-what-the-metadata-needs) is the object store's
second gate, and it stores nothing either. It makes a bucket something a schema declares: a field
of the `#[shoal::db]` struct beside its tables, which generates the two tables its metadata lives
in, an identity a pool binding and an object frame can name, and a place in the authorization
catalog [F77](authorization.md) left for it. [S2](../object-storage/buckets.md) is the design, and
its four acceptance rows are this gate's.

The gate's prerequisites had all landed: the conditional write the rows' commits are built on
([F68](conditional-writes.md)), the composite key `StripeMeta` is keyed by
([Resolved #92, #198](../appendix/resolved/composite-partition-key.md)) and the byte bound on an
append batch that inline objects lean on ([Resolved #202](../appendix/resolved/append-batch-bytes.md)).
[X10](../object-storage/stripe-row-costs.md) had priced rows shaped like S3's on the lab
([Q25, in part](../object-storage/contract.md#q25-in-part-the-metadata-rows-2026-10-04)) and left
two things to this gate: the rows' layout, which is persisted from the first object on, and
whether every bucket shares one stripe table.

Two decisions were taken with the user on 2026-10-11, recorded as
[Q25](../object-storage/contract.md#q25-the-rows-layout-and-the-stripe-table-2026-10-11):

- **one stripe table a bucket**, as S2 drew it, not one shared by every bucket;
- **an object id is a `u128`**, minted from `Uuid::now_v7`'s bits, not a `Uuid`.

## What it does

**A third kind of field.** A field whose type's first segment contains `Bucket` is a bucket; its
one generic argument is a marker, a unit struct whose name is the bucket's:

```rust
/// Names the bucket our posters are kept in
pub struct Posters;

/// The database our media service is built on
#[shoal::db]
pub struct Media {
    /// The movies we know about
    pub movies: PersistentUnsortedTable<Movie, FileSystem>,
    /// The posters for those movies
    pub posters: Bucket<Posters>,
}
```

The field is rewritten for a server as a table's is, `Bucket<Posters>` becoming
`Bucket<Posters, Media, MediaTableNames>`, and a client half reads `Bucket` for its name and
discards it, as it does a table type. A bucket that names no marker, a lifetime, more than one
argument or a marker with generics of its own, a type naming both a table and a bucket, a
projection declared on a bucket, the same marker twice, and a declared row named like a generated
one are compile errors with a span, never a panic of the macro.

**Two generated tables a bucket**, `PostersObjectMeta` and `PostersStripeMeta`, emitted in both
halves into the schema's own module as ordinary persistent unsorted tables under the ordinary
storage engine. They are two variants of the table enum, in the bucket's field's place, with
`TableId::of` their names, so they are in `QuerySupport::table_ids` and a cluster gives each the
groups it gives any table - eighteen each at the lab's shape of three nodes of six, the thirty-six
a bucket S2 counts - and in `persistent_tables`, so a node keeps them on disk and backs them up,
scrubs and repairs them as it does any table.

| `{Marker}ObjectMeta` | Role | Holds |
| --- | --- | --- |
| `key: u64` | partition | `objects::path_key(path)`: gxhash64 of the path under a frozen seed of zero |
| `version: u64` | filter, update | Moved by every committed change to the row, and what a change is conditional on |
| `entries: Vec<ObjectEntry>` | update | One entry an object whose path hashes to the key, nearly always one |

| `{Marker}StripeMeta` | Role | Holds |
| --- | --- | --- |
| `consumer: u64`, `object: u128`, `stripe: u64` | partition | The consumer its binding minted, the object, the stripe's index |
| `sequence: u64` | filter, update | How many writes committed, and what a commit is conditional on |
| `labels: Vec<Label>` | update | The label each stripe chunk must carry, by position: a sequence and a tag |
| `length: u64` | update | How much of the stripe holds bytes |
| `stamp: u64` | update | The object's truncate epoch when the row was last committed |
| `fence: u64` | update | The least epoch a writer must have read to commit: a truncate's fence ([X1](../object-storage/stripe-model.md#truncate-q18)) |
| `tombstone: bool` | update | A reclaimed stripe; its row is never deleted ([X1](../object-storage/stripe-model.md#what-the-search-found-and-the-repairs)) |
| `pending: Option<Box<PendingBytes>>` | update | Small writes' bytes that rode their commits, until `k + f` holders fold them ([Q27](../object-storage/contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)) |

**What the rows hold is defined once**, in `shoal::shared::objects` (`shoal-proto/src/shared/objects.rs`),
not in each schema. An `ObjectEntry` keeps the whole path, the object, a content version an ETag is
derived from (X14), the size, how much of the object its put wrote (the model's `created`), the
geometry (stripe and unit size, data and parity chunks; a replicated pool is one data chunk), the
truncate epoch and its floors, whether it is live with a replacement in flight and the ids retired,
two times in milliseconds for people, the attributes X14 asked for (content type, encoding, cache
control, user metadata, tags) and the object itself when it is held inline. `Label` and
`PendingBytes` are X1's model's. The bounds an object operation refuses past are named beside them
as constants: a path of 1 KiB, 2 KiB of user metadata, ten tags, sixteen floors, a pool's inline
threshold of 16 KiB and the small-write threshold of 64 KiB.

**A bucket's identity.** `BucketId::of(name)` is gxhash64 of the marker's name under
`BUCKET_ID_SEED`, a frozen seed of one, so a bucket and a table of one name never share an
identity's bits. A schema gets `{Db}Buckets`, one variant a bucket (uninhabited with none), with
`BucketNameSupport::{bucket_id, position, name}`, and each marker gets
`shoal::shared::objects::BucketRows`, naming its two rows, its schema's client and its identity.
`QuerySupport` gained `HAS_BUCKETS`, `BucketNames`, `bucket_names`, `bucket_ids`,
`generated_tables` and `served`, and `TableNameSupport` gained `bucket`.

**The rows' layout reaches the fingerprint.** A bucket folds its field's name, its rewritten type,
both rows' constants, and `OBJECT_ROWS_LAYOUT`: a row's own constant sees `Vec<ObjectEntry>` and not
what an entry holds, so the layout constant mixes the archived size and alignment of every type a
row nests and `OBJECT_ROWS_VERSION`, moved by hand for a change neither sees. A golden test of the
canonical rows' archived bytes fails on any such change.

**On a server a bucket holds its two tables.** `shoal::tables::Bucket<M, D, N>` is a plain holder
with public `objects` and `stripes` and an `assemble` constructor, and the generated
`ShoalDatabase` impl reaches each table through it, `self.posters.objects`, as it reaches a declared
table through its field.

**A client's query of a generated table is refused at the front door.** The coordinating shard
answers each query of a bucket's table `GeneratedTable` (code 56), naming the table and its bucket,
before anything is routed and for every connection: an admin, a principal granted `*`, and a
connection on a server requiring no authentication alike, which the grants' check skips
(`refuse_generated`, `shoal-core/src/server/shard.rs`). The rest of the bundle runs. A schema with no
bucket compiles the check away. The SHQL parser refuses the two tables by name too, completion
never offers them, and a benchmark's dataset never lists them. A forwarded share from a peer is not
checked: the coordinator that forwarded it already judged it, and M13's object operations will
write the generated tables through that path.

**A bucket's grants.** A principal's grants are a `buckets` map beside `tables`, under the same
`read` and `write`, `*` meaning every bucket:

```yaml
grants:
  viewer:
    buckets:
      Posters: [read]
  loader:
    tables:
      Movie: [read, write]
    buckets:
      '*': all
```

A table grant naming a generated table is refused, naming the bucket to grant on instead, and so is
a bucket grant naming no declared bucket: at a standalone node's start, at a proposal, in applied
control state and at `shoaladm`'s deploy (`Grants::check(&Served)`). An admin grant or revoke says
what it is on (`AdminKind::Grant { principal, on, name, privileges }`, `GrantOn::{Table, Bucket}`),
the cluster records each bucket and the tables it generated when its placement is initialized
(`BucketRecord`), and the command line takes `grant <who> bucket <name|*> <privs>`. A shard compiles
each principal's privileges on each bucket by position beside its tables', and a table a bucket
generated compiles to nothing for every principal but an admin, `*` or not.
`Access::on_bucket` and `allows_bucket` are what M13's object operations call; nothing calls them
yet.

**One model of a schema in the macro.** The macro's generators each read the struct's fields
themselves, a field being one table. A bucket is two, so the fields are read once into a model
(`shoal-derive/src/schema.rs`): the schema's tables, each with how the dispatch layer reaches it,
and the fields that hold them. Every generator reads the model. Schema ids taken before the
refactor are frozen in `schema_ids_are_frozen` and `the_client_halfs_ids_are_frozen`, and it moved
none of them.

**The lab's TMDB schema declares `posters: Bucket<Posters>`**, the tmdb cluster's `reader` holds
read on every bucket, and the loader's `posters` command sends, through every member, a bundle
mixing the bucket's tables with a movie's reads and checks every query of a generated table comes
back `GeneratedTable` while the movie's are answered. **X10's spike drives the generated rows**: `shoal-spike-rows` declares
`spike: Bucket<Spike>` and writes `SpikeObjectMeta` and `SpikeStripeMeta` itself, built with its
own `generated` feature, which turns on `shoal`'s `open-generated-tables` and opens the front door
to its driver alone.

## Design choices

**One stripe table a bucket.** X10 found commits spread over more groups paid in flushes, and
read that as favouring fewer groups, so a stripe table shared by every bucket was weighed. But the
flushes X10 counted are a shard's, and a shard's WAL is shared by every group on it: one bucket's
stripe writes hash over every tablet and reach every shard whether its table is shared or not, and
two buckets' tables sit on the same shards and share each shard's flush. What sharing saves is idle
groups - each table adds about as many group replicas to every shard as the replication factor,
whatever the cluster's size, against about four thousand groups a thread holds - and WAL index when
several buckets are busy at once. What it costs is isolation, since a cold row's read holds up its
group's batch (X10's 0.62×) and every bucket's stripe commits would share that group; a bucket's
own figures in `stats`; walks of one bucket's rows; and S2's rule that every generated name comes
from a marker. The saving shows at hundreds of buckets and the cost at two.

**An object id is a `u128`.** It is in every stripe row's persisted key and the golden key set
freezes both shapes, which are different partitions for the same bits. A `u128` filters, sizes and
serializes with no code of its own; deepsize2 has no `Uuid`. It is still minted from
`Uuid::now_v7`, so ids are time-ordered.

**The rows hold what the contract needs and nothing a later gate decides.** No chunk digest
([X1](../object-storage/stripe-model.md#the-chunk-digest)). No missed mask, which X10's stand-in
had: the record of which holders missed a write is the group's, derived at apply
([S10](../object-storage/recovery.md#how-a-slice-learns-what-it-missed)), and Q17 decides its
granularity. No placement group's generation or positions, which are the group's state. A fence and
a tombstone, which X1's repairs added. The pending bytes are boxed, since nearly every row has none
and an empty `Option<PendingBytes>` archives the whole struct inline: 240 bytes a stripe row became
192.

**A layout constant beside the rows' own constants.** The derive folds a field's spelling and
archived size, which a nested type's change does not move. Folding the nested types' sizes
automatically catches most changes, the hand-moved version catches the rest, and the golden bytes
make forgetting the version a failing test.

**`BucketRows` is not generic over the schema.** A schema parameter would put
`D::ClientType` projections into the field's type check; a non-generic trait with `type Client`
lets M13 bound `client.bucket::<M>()` on `M: BucketRows<Client = S>`, and makes one marker in two
schemas a conflicting impl, which is the error it should be.

**`Bucket` holds and does not forward.** `PersistentUnsortedTable`'s impl carries higher-ranked
bounds on its row; a generic forwarding method would repeat them for both rows. The generated impl
calls the concrete tables. `shutdown(self)` moves each table out of its bucket, which a `Drop` impl
would forbid.

**The refusal is a code of its own, before the grants.** `Unauthorized` tells a client to ask an
admin, and an admin cannot help; `InvalidRequest` names a malformed request, and this one is well
formed. It runs before `authorize` because `authorize` skips admins and unauthenticated connections,
and `authorize` now skips a query the front door already answered, so no query is answered twice.

**A grant says what it is on.** `GrantOn` and `name` replace `table` in the admin kinds and the
committed commands, with no compatibility kept for the control logs of clusters built before: a
schema with a bucket is a new cluster anyway.

## Alternatives rejected

**A stripe table shared by every bucket.** Above.

**A `Uuid` object id.** Above.

**Synthesized table fields**, rather than a model. The generators' helpers read a type's first path
segment, so a generated table's qualified type read as segment `shoal`: `From` impls silently
skipped and the dispatch layer panicking. An unqualified one relied on the author having imported
`PersistentUnsortedTable`.

**Letting an admin through the front door.** It would break S2's invariant that no client writes a
generated row; an admin can grant itself anything, so its holding everything protects nothing here.

**A runtime switch for the spike.** A setting a deployment can turn on is a back door; a feature no
default build compiles, and a node that says so when it starts, is not.

**`table` kept with an `on` beside it**, for the control logs of clusters built before. It names a
bucket `table`, for clusters that a schema change rebuilds anyway.

## Limitations

- **Nothing can be put in a bucket.** No operation reaches one, `Access::on_bucket` has no caller,
  and a consumer id is minted by a binding, which M13 commits.
- **The binding half of `bucket_declaration_moves_the_fingerprint_and_its_binding_does_not`** holds
  what a deployment says about a bucket today, its grants. A pool binding joins it at M13
  ([TODOs](../appendix/todos.md#a-pool-binding-in-the-fingerprint-test)).
- **The layout is fixed and the bounds are not enforced.** A change before M13 stores the first
  object is a move of `OBJECT_ROWS_VERSION`, not a migration; after, it is a migration. The bounds
  are constants M13 and M15 refuse past.
- **A schema crate still declares `rkyv` and `deepsize2`.** The generated rows name
  `::shoal::rkyv` and implement `DeepSizeOf` through `::shoal::deepsize2`, but the table derive's
  own helper structs name `::rkyv`, as every schema's do
  ([TODOs](../appendix/todos.md#the-derives-helper-structs-name-rkyv)).
- **A bucket is read off the first segment of its type's name**, as a table is: written
  `shoal::tables::Bucket<..>` it is not one.
- **Adding a bucket is a schema change**, so a new cluster and a restore, as S2 said.
- **`open-generated-tables` is in the tree.** A spike's switch; nothing in the product builds it.
- **Admin reads name generated tables.** `stats`, backup and repair are a table's, and see them.

## Invariants to uphold

- **Every client query of a generated table is answered `GeneratedTable` before it is routed, for
  every connection.** The check sits in `send_to_shard` ahead of `authorize`, which a new client
  path must also reach; a forwarded share is not checked.
- **A query is answered once.** `authorize` and write admission skip what the front door refused.
- **A generated table's name comes from its marker alone**, and a bucket's identity is the hash of
  its marker's name under `BUCKET_ID_SEED`; a position never leaves the node.
- **A change to anything `shoal::shared::objects` defines is a change to the layout**, and moves
  `OBJECT_ROWS_VERSION` and the golden bytes together.
- **Generated code names `::shoal::shared` paths**, so a client half with a bucket links no engine
  (`cargo check -p shoal-client-check --no-default-features`).
- **`Bucket` has no `Drop`.**
- **No table grant reaches a generated table**, `*` included, and the control state records each
  bucket's tables, which is what the applied check reads.
- **`open-generated-tables` is never a default**, and no workspace member enables it by default,
  since `cargo test --workspace` unifies features.
- **A schema with no bucket expands as it did before F79**; `schema_ids_are_frozen` holds it.

## Performance

**A schema with no bucket pays nothing at the front door**: the check is behind a constant that is
false. **One with a bucket pays, for each query, reading its table off the archive and a match** to
the variant's bucket; nothing is allocated unless something is refused.

**The generated rows against X10's stand-ins**, archived by rkyv, at X10's shapes (six labels; one
entry, a 64 byte path, nothing inline): a stripe row is **192 bytes against 176**, the stand-in less
its missed mask and plus a fence, a tombstone and a pointer's width for pending bytes; an object row
is **360 against 312**, plus a content version, the put's length, liveness and the attributes X14
asked for (`generated_rows_archive_to_their_recorded_sizes`).

**Groups**: a bucket's two tables have eighteen groups each at the lab's shape, the declared
table's count, and add no others (`a_buckets_two_tables_add_their_groups_and_no_others`).

The lab's figures - X10's rate and rows legs taken again on the generated rows, and the tmdb
cluster redeployed with its bucket - are recorded in the follow-up change.

## Tests

| Test | What breaks if F79 is reverted |
| --- | --- |
| `shoal` `buckets::bucket_field_generates_its_two_tables` | A bucket's two tables in `table_ids` and `persistent_tables`, and every query of them refused `GeneratedTable` on an open server and for a principal granted every table and bucket, while the declared table's queries in the same bundle are answered |
| `shoal` `buckets::bucket_declaration_moves_the_fingerprint_and_its_binding_does_not` | Declaring a bucket moves the fingerprint and the schema id, renaming its marker moves the id, and nodes deployed with different grants on the bucket accept the same client |
| `shoal` `buckets::bucket_ids_are_stable_under_field_order` | A reordered schema keeps every bucket and table identity while its schema id moves, and `BucketId::of("Posters")` and both generated `TableId`s are frozen |
| `shoal` `buckets::generated_rows_archive_to_their_recorded_sizes` | The generated rows archive to 192 and 360 bytes at X10's shapes |
| `shoal` `buckets::a_grant_on_a_generated_table_or_an_unknown_bucket_refuses_the_start` | A table grant on a generated table, and a grant on an undeclared bucket, refuse a node's start by name |
| `shoal-client-check` `client_half::client_half_with_a_bucket_links_no_engine` | A client half with a bucket names it, identifies it, generates its tables, refuses them in SHQL and leaves them out of datasets, in a build with no engine |
| `shoal-client-check` `client_half::the_client_halfs_ids_are_frozen` | CheckDb's constants, taken before the refactor and again with its bucket |
| `shoal` `fingerprint::schema_ids_are_frozen` | The refactor moving any schema's id or fingerprint |
| `shoal` doctests on `shoal::Bucket` | A bucket naming no marker, and a declared row named like a generated one, compile; a schema with a bucket does not |
| `shoal-proto` `objects::tests::generated_rows_archive_to_frozen_bytes` | The nested types' archived bytes, `OBJECT_ROWS_LAYOUT` and `OBJECT_ROWS_VERSION` moving apart |
| `shoal-proto` `objects::tests::path_keys_are_frozen` | `path_key`'s hash or seed |
| `shoal-proto` `identity::tests::bucket_ids_follow_their_names_and_are_frozen` | `BucketId`'s hash or seed, or sharing the table seed |
| `shoal-proto` `grants::tests::a_catalog_holds_bucket_grants_beside_table_grants` | A `buckets` map read, written back, kept by `prune`, and held apart from `tables` |
| `shoal-proto` `grants::tests::a_grant_on_a_generated_table_or_an_unknown_bucket_is_refused` | `Grants::check` against a schema's `Served` |
| `shoal-core` `authorize::tests::grants_compile_to_a_privilege_a_bucket_and_nothing_on_a_generated_table` | Bucket privileges by position, and `*` never reaching a generated table |
| `shoal-core` `control::types::tests::bucket_grants_are_committed_and_generated_tables_refused` | `BucketRecord`s at initialization, and the applied check of a kinded grant |
| `shoal-core` `map::tests::a_buckets_two_tables_add_their_groups_and_no_others` | Eighteen groups each for a bucket's tables at the lab's shape, and no others |
| `shoal-client` `client::tests` (the retry rules) | `GeneratedTable` retried |
| `shoaladm` `cluster::actions::tests::a_grant_parses_previews_and_renders` | `grant <who> bucket <name> <privs>`, its preview and its rendering |
| `shoaladm` `deploy::inventory::tests::users_and_grants_are_read_and_their_names_held_to_plain` | An inventory's bucket grants read and written back |

The workspace run that counted these, at six threads, failed one fixture test,
`canonical_digest_ignores_archive_layout_at_same_boundary`, on a peer that did not answer within
the deadline, as F77's and F78's runs did; it passed alone. The runbook's feature-gated lines found
one defect on the way, not this feature's:
[item 227](../appendix/known-issues.md#227-the-stage-profile-build-of-the-engine-has-not-compiled-since-f75),
the `stage-profile` build of the engine, which has not compiled since F75.

## Related

[S2](../object-storage/buckets.md), the design; [S3](../object-storage/objects.md), the rows;
[M12](../object-storage/milestones.md#m12-tables-what-the-metadata-needs), the gate;
[Q25](../object-storage/contract.md#q25-the-rows-layout-and-the-stripe-table-2026-10-11), the
decision; [X10](../object-storage/stripe-row-costs.md), the rows' first price; [F77](authorization.md),
the catalog the buckets map joins; [F15](client-server-split.md), the rule the client half keeps;
[Derive Macros](../api/derive-macros.md), the macro.
