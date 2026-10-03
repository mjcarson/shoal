# S2. Buckets in the schema

## Context

A bucket is declared where a table is declared, as a field of the `#[shoal::db]` struct (R8).
That is not a convenience: in Shoal the schema is the type system. There is no catalog and no
table created at run time, the two peers of every connection compare a fingerprint of the
schema before they exchange anything, and a cluster's tablet groups are derived from the
schema's table list. A bucket that lived anywhere else would be the first thing in Shoal the
fingerprint did not cover.

Two decisions of 2026-10-02 shape this page. The bucket's metadata tables are **hidden and
generated**: the author never declares them. And the storage pool and its redundancy are
**bound in the deployment**: the declaration names no hardware.

## What exists today

`#[shoal::db]` (`shoal-derive/src/lib.rs:309`) reads the struct's named fields and decides
what each one is from the name of its type.

- A field whose type name contains `Persistent` or `Ephemeral` is a table and is rewritten to
  name the database and its table enum; any other is skipped in silence
  (`shoal-derive/src/utils.rs:350-363`).
- A table is sorted or unsorted by the substrings `Sorted` and `Unsorted`, and a type with
  neither panics the expansion with `Failed to detect table kind`
  (`shoal-derive/src/tables.rs:204-222`).
- A variant's name is the first generic argument of the field's type, the row, and a field
  without one panics too (`shoal-derive/src/utils.rs:196-206`).

From the fields it emits the table enum, the client, the wire enums and, for a server, the
`ShoalDatabase` impl ([Derive Macros](../api/derive-macros.md#what-db-generates)). Four facts
about that output decide what a bucket can be:

- **There is no table trait.** The generated impl calls methods by name on each field's type,
  thirty of them (`shoal-core/src/server/database.rs:41`). Whatever sits in a field either
  answers to those names or the macro has to branch on its kind.
- **A table's identity is its row type's name.** The table enum's variants are the row idents,
  and `table_id` is `TableId::of` that name (`shoal-derive/src/traits/table_name.rs:13-28`,
  `shoal-proto/src/shared/identity.rs:136`). A cluster builds its tablet groups from
  `QuerySupport::table_ids` (`shoal-proto/src/shared/traits.rs:256`), so a table that is not
  in that list is not replicated.
- **The fingerprint folds every field's name and the whole spelling of its type**
  (`shoal-derive/src/traits/fingerprint.rs:206-207`), then each row's own constant. A client
  compares `SCHEMA_FINGERPRINT` in the hello and two nodes compare `SCHEMA_ID`
  (`shoal-proto/src/shared/traits.rs:170`, `:180`).
- **Nothing is emitted that the author did not declare.** Every generated item comes from
  walking the named fields; there is no hidden table anywhere in the tree.

`#[shoal::db(client)]` emits the wire half alone and names no engine path, which
`cargo check -p shoal-client-check --no-default-features` holds
([F15](../features/client-server-split.md)). And a schema change is not a rolling operation:
a node with another `schema_id` is refused at the hello, and the supported path is a new
cluster and a restore ([C15](../distributed/open-issues.md#explicitly-unsupported)).

## The design

### What the author writes

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

`Posters` is a marker: a unit struct whose name is the bucket's name. A marker is used, and
not the field's name, for the reason a table is named by its row: the type is what generated
code and a client can both name. `client.bucket::<Posters>()` is how a caller reaches it
([S12](wire-and-client.md#the-clients-handle)).

The declaration says nothing about a storage pool, a redundancy, a stripe size or a device.
Those are the deployment's: a binding makes the bucket one of a storage pool's consumers
([S4](pools-and-devices.md#pools-and-bindings-are-policy)), so the schema above starts on a
laptop with one directory and on the lab with three hosts, unchanged.

### What the macro generates

A field whose type name contains `Bucket` is a third kind, beside sorted and unsorted tables.
For each one the macro emits:

| Generated | Purpose |
| --- | --- |
| `PostersObjectMeta` and `PostersStripeMeta`, two row types | The bucket's metadata ([S3](objects.md#the-two-rows)). Named from the marker, so nothing here is called a "head" |
| A `PersistentUnsortedTable` for each, held inside the `Bucket` | They are ordinary unsorted tables under the ordinary storage engine. The cluster replicates them as it replicates `movies` |
| Two more variants of the table enum, with `TableId::of` their names | What puts the two tables in `table_ids`, and so gives them tablet groups |
| A bucket enum, one variant a bucket, with `BucketId::of` the marker's name | The identity a pool binding and a wire frame name, never the enum's position. A stripe chunk names the consumer id the binding mints instead ([S4](pools-and-devices.md#pools-and-bindings-are-policy)) |
| The bucket's part of the client: its name, its id, the operations of [S12](wire-and-client.md) | Emitted in both halves, naming `::shoal::shared` paths alone |
| A constant for the generated rows' layout, folded into the fingerprint | Two builds that generate different rows refuse each other at the hello, as two builds with different tables do |

The field is rewritten for a server as a table's is: `Bucket<Posters>` becomes
`Bucket<Posters, Media, MediaTableNames>`, which is what breaks the same circularity the
table rewrite breaks.

**The generated tables are ordinary variants of the wire enums, refused at the front door.**
A tablet group replicates a `Command` whose payload is the table's own intent, built from a
query of that table (`ShoalDatabase::write_command`), so the two tables need query kinds like
any other. What they must not be is writable by a client: a bundle naming a generated table
is refused by the coordinating shard, by name, before it is routed. The alternative, a second
command path that only the bucket can reach, would fork the one dispatch layer every table
goes through.

**The bucket type answers the names the dispatch layer calls**, by handing each to the table
it concerns. `Bucket` is therefore a small type: two tables, the bucket's id, and a handle to
the node's device store ([S6](device-store.md)). Everything about bytes lives behind that
handle, and nothing about bytes is in the macro.

### What does not reach the fingerprint

The pool a bucket is bound to, its redundancy and its stripe size are not in the schema, so
they cannot reach the fingerprint; they are committed cluster policy instead
([S4](pools-and-devices.md#pools-and-bindings-are-policy)). Nothing about benchmarking does
either, which is the rule [F66](../features/dataset-benchmarks.md#invariants-to-uphold)
already holds for `dataset`.

### Adding a bucket is a schema change

A new field moves the fingerprint and the schema id, so a node built with the bucket refuses a
node built without it. Today that makes adding a bucket to a deployed cluster a new cluster
and a restore, exactly as adding a table is. What a restore means when a cluster holds object
bytes is [Q31](contract.md#questions-to-answer), and it is the reason that question blocks the
last gate rather than being left as a limitation.

## Alternatives rejected

**A typed row the author derives**, with custom metadata fields that can be filtered and
updated. Offered on 2026-10-02 and not taken: the metadata is fixed system fields and a small
map of strings. It can be added later, but not for free, since the generated row is persisted
and a typed one would be a different row.

**A table the author declares and points the bucket at.** It splits the metadata between a
table the author owns and a stripe table that would still be hidden, and it lets an author
write a row the bucket's protocol depends on.

**The pool or the redundancy in the declaration.** Decided against on 2026-10-02. A schema
that says `erasure(4, 2)` cannot start on three hosts, and a redundancy in the fingerprint
makes changing it a schema change.

**The field's name as the bucket's name.** It breaks the rule that generated names come from
types, and it gives a client nothing to name.

**Buckets created at run time**, as S3 has. There is no catalog to put them in, R8 asks for
the static form, and a bucket nobody declared is a bucket the handshake cannot vouch for.

## What it costs

Two tables a bucket are two more sets of tablet groups: `N × slots` each, so thirty-six groups
a bucket on the lab's three nodes of six. Groups are not free. One thread held about a
thousand three-member groups at openraft's default timers and about four thousand at the
cluster's, with a few hundred kibibytes of memory each
([Q1 and Q13 at M1](../distributed/protocol.md#q1-and-q13-at-m1)). A schema of many buckets
pays that many times over, which is one reason
[Q25](contract.md#questions-to-answer) asks whether the stripe table could be shared.

The macro gains a field kind and a generator, and every expansion of a schema with a bucket
gets longer.

## What it breaks

- "It classifies a field by looking for `Sorted`/`Unsorted`/`Persistent`/`Ephemeral`"
  ([Derive Macros](../api/derive-macros.md#limitations), `CLAUDE.md`): there is a third kind.
- "A variant is named after the row type the author wrote": two variants a bucket are named
  after rows nobody wrote.
- "Every table can be queried by a client": two cannot.
- A schema with a bucket is not served by a build that does not know buckets, which is the
  ordinary consequence of the fingerprint and is listed only so that nobody is surprised.

## Invariants to uphold

- Generated code names `::shoal::` paths and nothing else, and the client half links no
  engine.
- A bucket's identity is the hash of its marker's name under a frozen seed, as a table's is of
  its row's, and never the enum's position.
- The generated tables' names are derived from the marker alone, so they are stable under
  reordering fields.
- No pool, device, redundancy or stripe size reaches the fingerprint or the schema id.
- A client cannot write a row of a generated table.
- A bucket type the macro cannot read is a compile error with a span, not a panic.

## Prerequisites

[S1](prerequisites.md#required): the conditional write, which the generated rows' updates are
built on (delivered, [F68](../features/conditional-writes.md)), and known issue 198, since `StripeMeta`'s key is more than one field (delivered with item 92, [Resolved #92, #198](../appendix/resolved/composite-partition-key.md)).

## How it would be measured

There is no speed to measure here. What is checked is structure: the client half compiles
with no engine in its graph, the fingerprint moves when a bucket is declared and does not
when its pool binding changes, and the count of groups a bucket adds is the count the map
derives. What those groups cost idle is already measured, and what a stripe row costs is
[X10](spikes.md#x10-what-a-stripe-row-costs).

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `bucket_field_generates_its_two_tables` | A schema with one bucket has two more table ids, each with tablet groups, and no query of a client reaches either | M12 |
| `bucket_declaration_moves_the_fingerprint_and_its_binding_does_not` | Declaring a bucket changes the fingerprint and the schema id; rebinding it to another pool changes neither | M12 |
| `client_half_with_a_bucket_links_no_engine` | `shoal-client-check` with a bucket compiles under `--no-default-features` | M12 |
| `bucket_ids_are_stable_under_field_order` | Reordering the struct's fields changes no bucket id and no generated table id | M12 |

## Related

[S3](objects.md) for the rows; [S4](pools-and-devices.md) for what the declaration leaves
out; [S12](wire-and-client.md) for the client's half; [Derive Macros](../api/derive-macros.md)
for the macro as it is; [F15](../features/client-server-split.md) for the rule the client
half keeps; [C4](../distributed/tablet-map.md) for how tables become groups.
