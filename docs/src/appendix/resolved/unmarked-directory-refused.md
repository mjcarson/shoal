# 46. An unmarked storage directory was claimed rather than refused

## Symptom

A storage root with no `shoal-meta.json` was taken to be one nothing had written to. The claim
minted a new node identity into it and the server started, whatever the directory held. So a
directory whose marker had been deleted, or one written before the marker existed, came up
under a new identity, and a node served it as though it were empty. The same was true of every
other root a node writes under: since [Resolved #43](marker-every-root.md) an unmarked one was
handed a copy of the primary's marker in silence. That included a table's root that had been
wiped, or a disk replaced by an empty one at the old path, under a node that had already written
rows there. The node started and served that table without its rows.

A second defect sat in the same code. The primary root's marker was written by `claim_root`
before any other root was looked at. So a start refused because a table's root belonged to
another server had already minted an identity into the primary and left it there.

## Cause

`StorageMeta::claim` read the marker and treated `NotFound` as a fresh directory:

```rust
// this directory has never been written to, so claim it for this node
None => {
```

"Has no marker" and "has never been written to" were the same statement only for as long as the
marker had existed, which was one commit when the item was filed. A guard that fires only when
it recognises the directory cannot fire on the one directory it does not recognise. The mirror
had the same blind spot: `StorageMeta::mirror` copied the marker onto any root without one. And
`ShoalPool::start` claimed the primary in `claim_root`, then locked and mirrored the other roots
in a loop afterwards. The order was write-then-judge, and `server::claim` (the `<node> claim`
subcommand a deployment runs) never reached the loop at all, so a claimed node's other roots
stayed unmarked until its first start.

## Evidence

**Reproduced.** Four tests in `shoal/tests/storage_meta.rs` were written first and run against
the unfixed tree at `657acd4`:

```text
running 4 tests
test claim_marks_every_root ... FAILED
test a_refused_root_leaves_the_primary_unmarked ... FAILED
test a_wiped_table_root_is_refused_once_the_node_is_established ... FAILED
test a_directory_with_data_and_no_marker_is_refused ... FAILED

---- claim_marks_every_root stdout ----
thread 'claim_marks_every_root' panicked at shoal/tests/storage_meta.rs:462:56:
the claim left the table's root unmarked

---- a_refused_root_leaves_the_primary_unmarked stdout ----
thread 'a_refused_root_leaves_the_primary_unmarked' panicked at shoal/tests/storage_meta.rs:436:5:
assertion `left == right` failed: a refused claim minted an identity into the primary root
  left: Some(StorageMeta { format: 3, shards: 2, physical: None, node: NodeId(87d4bbf9-…), cluster: None, layout: 1, topology: 0, mode: Standalone, incarnation: 1 })
 right: None

---- a_wiped_table_root_is_refused_once_the_node_is_established stdout ----
thread '…' panicked at shoal/tests/storage_meta.rs:314:13:
a node whose table root was wiped was started instead of refused

---- a_directory_with_data_and_no_marker_is_refused stdout ----
thread '…' panicked at shoal/tests/storage_meta.rs:314:13:
a directory holding data and no marker was started instead of refused
```

With the fix all four pass. On the lab (`tmdb_cluster.yaml`, 2026-10-03) the fixed build
bootstrapped a fresh cluster: `shoaladm deploy` claimed each host's empty roots and came up 3 of
3. titan's unit was then stopped, its `shoal-meta.json` alone removed and the unit started. It
was refused at every restart systemd made:

```text
Error: Shoal(StorageDirectoryNotEmpty { path: "/optane/shoal", found: ["Movie", "MovieByKeyword", "MovieRelease", "control", "wal"], more: 0 })
```

## The fix

**A claim has three outcomes.** `StorageMeta::claim_roots` looks at every root before it writes
to any of them. The private `survey` sorts each root into one of three states:

- **Empty.** The root holds nothing but what a start writes for itself: `shoal.lock` (taken
  before the claim looks), `shoal-meta.json.tmp` (what a crash during a marker's write leaves)
  and `lost+found` (the top of a freshly made ext4 filesystem).
- **Marked.** The root carries a marker in a format this build reads.
- **Unmarked.** Anything else. It is refused with `ShoalError::StorageDirectoryNotEmpty`, which
  names the root, up to five of its entries and how many more there are, and says the ways out:
  start the node that wrote it, restore its marker, or empty it.

**The primary remembers the roots it mirrored onto.** `StorageMeta` gains `roots`, the list of
every other root the node's marker was mirrored onto. It is optional and absent from every
earlier marker, so the format stays 3, as it did for `physical`. That list is what tells two
empty roots under a marked primary apart:

- A root the list names was written to. Found empty, it was wiped or replaced, and the claim
  refuses it with `ShoalError::StorageRootEmptied`.
- A root the list does not name has just been added to the configuration. It is mirrored and
  listed.

**Everything is decided before anything is written.** The primary is settled without a write:
`reopen` holds a marked primary to its marker as before, and `mint` builds the marker of an
empty one. Then every other root is judged:

- a marked root against the primary's identity, as `mirror` used to judge it, now
  `check_mirror`;
- an empty one against the list;
- an unmarked one is refused.

Only then is anything written, in this order:

1. The primary, still listing only the roots it had already mirrored onto.
2. Every mirror.
3. The primary again, once, if the list changed.

`claim_root` locks every root in `Storage::roots` and calls `claim_roots`. So `server::claim`
marks every root now, and the loop in `ShoalPool::start` is gone.

**A root inside another is refused by name.** One root nested in another holds the outer
one's files, or the outer one holds its directory, so one of them would read as somebody's
data. Until now only `shoaladm` refused nesting in an inventory. `Storage::nested_roots` finds
such a pair, and `claim_root` refuses it before anything is locked, naming both.

**What wrote into a root before the first claim was moved out of it.** Each of these would now
be refused:

- the integration tests' `TestCertificate`, which now gets a directory of its own;
- the bench's TLS transport arms, whose certificate moves to a sibling `<root>-tls`;
- one fixture test's `shoal.yml`.

## Alternatives rejected

**Refuse an empty root other than the primary whenever the primary is marked**, with no list.
This was the first form, and it broke three real cases:

- A crash between the primary's marker and a mirror leaves an established primary with an empty
  root that never held data, and every later start would be refused.
- A table root added to an established node, which Resolved #43 says joins it, would be refused
  with no way through.
- The fixture and `shoal-bench` stage a primary marker before a child starts, so a child with a
  second root would be refused at its first start.

The list fixes all three, because it is written last.

**Check for archives and `*-active` logs only**, as the item's fix direction proposed. That
names what a node's data looks like today. A directory holding a file of any kind and no marker
is somebody's, and a list of the names a build happens to write goes stale with the next file
it learns to write.

**Take an empty listed root as a new one, the way S4 takes a device.**
[S4](../../object-storage/pools-and-devices.md#a-device-has-slices) gives an empty device a new
id because its stripe chunks are rebuilt from the rest of the pool. A table's root has no
rebuild of its own on this node. Started empty, it serves the table with no rows, and on a
cluster node it then answers reads at `One` from a copy that holds nothing. Refusing sends the
operator to a restore or to `shoaladm rebuild`, which replaces the node properly.

**Write the mirrors first, then the primary.** A crash then leaves an empty primary beside
marked mirrors, and the next claim mints a new identity that every mirror refuses.

## Invariants to uphold

- **Nothing is written to any root until every root has been judged.** A refusal at any of
  them leaves all of them as they were found.
- **The primary's list is written after the mirrors it names.** A crash before that leaves the
  new roots unlisted, and the next claim takes them as new.
- **A root holding anything but the lock, a staged marker or `lost+found`, and no marker, is
  never claimed.** A new file a start writes into a root before the claim has to join
  `OWN_ENTRIES`, or every first start is refused.
- **`Storage::roots` is still the one list**, and `claim_roots` is the one place a root is
  claimed. Nothing writes into a root that is about to be claimed: certificates, configuration
  files and traces go beside it.

## Still open

- **A root removed from the configuration drops out of the list**, so the same path added back
  empty later is taken as new. Its data was already gone from the node's view the moment it
  left the configuration, so nothing new is lost.
- The device claim of [S4](../../object-storage/pools-and-devices.md#a-device-has-slices) is
  built on this one at M14. It adds a device id and a slice's own marker; the three outcomes
  are these.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_directory_with_data_and_no_marker_is_refused` | `shoal/tests/storage_meta.rs` | A node's directory with its marker deleted starts under a new identity |
| `a_wiped_table_root_is_refused_once_the_node_is_established` | `shoal/tests/storage_meta.rs` | A node starts with a table's root emptied under it |
| `a_refused_root_leaves_the_primary_unmarked` | `shoal/tests/storage_meta.rs` | A refused start leaves a minted identity in the primary root |
| `claim_marks_every_root` | `shoal/tests/storage_meta.rs` | `<node> claim` marks the primary alone |
| `an_unmarked_root_is_refused_naming_what_it_holds` | `shoal-core/src/server/meta.rs` | Files with no marker are claimed, at the primary or at another root |
| `an_empty_directory_ignores_the_lock_the_staged_marker_and_lost_found` | `shoal-core/src/server/meta.rs` | A fresh ext4 mount, or a directory a crashed claim left, is refused |
| `a_root_added_to_an_established_node_is_mirrored` | `shoal-core/src/server/meta.rs` | An added root is refused, or a wiped listed one is claimed |
| `a_claim_stopped_before_its_mirrors_is_finished_by_the_next` | `shoal-core/src/server/meta.rs` | A crash between the primary and its mirrors leaves a node that cannot start |
| `a_second_root_written_by_another_node_is_refused` | `shoal-core/src/server/meta.rs` | Another server's root is taken, or the refusal bumps our own marker |
| `nested_roots_are_found` | `shoal-core/src/server/conf.rs` | A throughput path inside the latency one reaches the claim and is refused as somebody's data, naming the wrong root |
| `a_rendered_node_claims_starts_and_initializes` | `shoal-bench/tests/deploy_render.rs` | A deployed node's second root carries no marker after its claim |

## Related

[Resolved #43](marker-every-root.md), which made every root carry the marker this fix now
requires. [Resolved #45](storage-marker-format.md), the other half of the guard: a marker that
is wrong rather than absent. [Items 11, 12, 37](tablet-ring.md), whose layout change made the
old blind spot dangerous. [S1](../../object-storage/prerequisites.md#required), which listed
this item as required before M14, and [S4](../../object-storage/pools-and-devices.md#a-device-has-slices),
whose device claim is built on it.
