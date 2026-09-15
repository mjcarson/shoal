# 43. The storage marker only guarded the default storage root

## Symptom

`ShoalPool::start` claimed `storage.default.filesystem.latency_sensitive.path`, and that path
alone, with the identity and the shard count the marker carries. A table with its own
`storage.tables` entry pointing somewhere else was not covered: its root carried no marker, was
under no lock, and could be handed to another server - or another server's root handed to it -
with nothing to say so. The same was true of a default `throughput_sensitive` path apart from
the latency one. Since [F47](../../features/local-rehome.md) a changed core count is a rehome
that moves every table's files under the table's own paths; the marker, the hosting and the
manifest stayed under the one root, and a second root was moved on the same manifest untested.

## Cause

The marker was built by [items 11, 12 and 37](tablet-ring.md) to guard the directory the
shard count was minted in, and the storage configuration can name more than one directory.
Covering the rest meant deciding what a marker means when two roots disagree, which is why it
was filed rather than fixed then.

## Evidence

**Reproduced.** `a_table_under_its_own_root_is_marked_and_guarded` in
`shoal/tests/storage_meta.rs` starts a server with `TestRecord` under a root of its own,
writes a row and stops it. Against the tree at `b411018`:

```text
thread 'a_table_under_its_own_root_is_marked_and_guarded' panicked at shoal/tests/storage_meta.rs:232:56:
the table's root has no marker
```

With the fix the root carries a marker naming the same node, a restart reads the row back
through it, and a root claimed by another server is refused at start with
`the storage root … was written by another server`.

## The fix

`Storage::roots` lists every distinct root a configuration writes under, the primary first:
the default latency path, then in a stable order every other path the default's throughput
writer or any table's own settings name. The primary is claimed as before and keeps the
hosting file and the rehome manifest. Every other root is locked with its own
`DirectoryLock`, held for the pool's lifetime beside the primary's, and takes a **mirror** of
the primary's marker through `StorageMeta::mirror`: an unmarked root takes a copy, and a
marked one has to name the same node, the same slot count and the same layout, and the same
cluster or none - a joiner's mirror is written before it is admitted - or the start is refused
with `ShoalError::StorageRootMismatch`, naming the root and both identities, before a shard
opens anything.

The mirror is refreshed on every claim. The fields that move between claims - the topology,
the incarnation, the mode, the executor count - are the primary's alone: the control plane
rewrites the topology on every version and reads it back from the primary, and no mirror is
read for anything but the identity check.

## Alternatives rejected

**Claim every root independently, with `StorageMeta::claim`.** Each root would mint its own
node id on first use and bump its own incarnation on every start, and two roots of one server
would disagree about who the server is. There is one identity; the other roots carry a copy.

**Keep the mirror current on every marker rewrite.** `observe_topology` runs on every
topology version, and a write per root per version buys nothing: the mirror's job is the
identity check at the claim, and the identity fields do not move between claims except by a
joiner's adoption, which the check allows for.

**A second rehome manifest under each root.** The rehome resolves each table's settings by
name and moves its files under the table's own paths from the one manifest; a manifest per root
would need a rule for which finishes first. The manifest stays one file, under the primary.

## Invariants to uphold

- **`Storage::roots` is the one list.** The primary is its first element and the marker, the
  hosting file and the manifest live there; anything that locks or marks a root walks the
  rest of the list, and a new path in the storage configuration joins it.
- **A mirror is read for its identity and nothing else.** Its topology, incarnation, mode and
  executor count are whatever the last claim copied.
- **A mismatch writes nothing.** `mirror` refuses before it writes, so a root another server
  owns is left as it was.

## Still open

- The rehome's crash matrix still runs with one root; a table under a second root is moved on
  the same manifest through the same steps and is untested there, and a second root on
  another device is untested twice. Filed on the [todos](../todos.md#distribution) page under
  the rehome's follow-ups.
- Item 46 is unchanged: a root with no marker is claimed rather than refused, for the mirror as
  for the primary.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_second_root_is_mirrored_with_the_same_identity` | `shoal-core/src/server/meta.rs` | A fresh root takes the primary's marker, a reclaim refreshes it, and a joiner's mirror from before admission is refreshed with the cluster |
| `a_second_root_written_by_another_node_is_refused` | `shoal-core/src/server/meta.rs` | Another node's root, and the same node at another slot count, are refused without a write |
| `a_table_under_its_own_root_is_marked_and_guarded` | `shoal/tests/storage_meta.rs` | The table's root has no marker; a root another server claimed is served as this table's |

## Related

[Items 11, 12, 37](tablet-ring.md), which built the marker; [Resolved #46's neighbour, the
storage marker format](storage-marker-format.md); [F47. Local rehome](../../features/local-rehome.md),
whose manifest stays under the primary; [item 46](../known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused).
