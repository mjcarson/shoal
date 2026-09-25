# 135. A node killed while saving an archive map could never start again

## Symptom

In the lab's first kill test ([cluster testing](../../cluster-testing/correctness.md#kill-a-follower)),
titan's node was sent `SIGKILL` under a mixed load. systemd restarted it five seconds later, and
every start from then on failed within ten seconds, the same way:

```text
ERROR Shard::new:PersistentTable::new:FileSystemCompactor::with_capacity:ArchiveMap::compact_map:SerializableMap::save: error=GlommioIO { source: Os { code: 17, kind: AlreadyExists, message: "File exists" }, op: "Creating", path: Some("/optane/shoal/Movie/maps/temp/Shard-1"), fd: None }
ERROR ShoalPool::ready: shoal_core::server: error=ShardFailed { shard: 1, ... }
shoal-tmdb.service: Main process exited, code=exited, status=1/FAILURE
```

Titan restarted nine times in two minutes and never served. Each failed start could add another
leftover: the one after the kill left `maps/temp/Shard-5` empty, because shard 5 was saving when
shard 1's failure ended the process.

## Cause

`SerializedMap::save` writes a table's whole archive map to `maps/temp/Shard-N` and renames it over
`maps/Shard-N`. It opened the temp file with `create_new(true)`. A process killed between the
create and the rename leaves the temp file behind, and every later save refuses to create it.
The first save runs at startup, in `FileSystemCompactor::with_capacity`, so the shard failed to
start and took the node with it. The temp file is never read: only the rename makes a map live,
so a leftover one holds nothing the committed map needs. `create_new` guarded against a second
writer, but only the shard's compactor task writes a shard's map, one save at a time.

## Evidence

**Reproduced on the lab, then against the unfixed tree.** On the lab: the journal above, nine
restarts, and the two temp files left on titan. In the tree, `a_leftover_temp_map_does_not_stop_a_save`
writes 12,345 bytes to the temp path and saves:

```text
test server::tables::storage::fs::map::tests::a_leftover_temp_map_does_not_stop_a_save ... FAILED
a leftover temp map stopped the save: GlommioIO { source: Os { code: 17, kind: AlreadyExists, message: "File exists" }, op: "Creating", path: Some(".../T/maps/temp/Shard-0"), fd: None }
```

## The fix

`save` removes a temp map it finds before creating its own, and logs at `WARN` that it did
(`shoal-core/src/server/tables/storage/fs/map.rs`). The create stays `create_new`: once the
leftover is gone, a file there would be a second writer, and that is still refused. The fixed
program reached titan through `cluster upgrade titan`, which needed item 136 to be allowed. Titan
logged the two warnings, removed both leftovers, and served.

## Alternatives rejected

- **`create(true).truncate(true)` instead of `create_new`.** That fixes the crash case and
  silently accepts a second writer too, which the remove-then-exclusive-create does not.
- **Removing the temp directory's contents at startup.** It works for this file, but it moves the
  knowledge of what the temp directory holds away from the one function that writes it.

## Invariants to uphold

- **The only thing that makes a map live is the rename.** A temp map is disposable at any point
  before it, which is what makes removing a leftover safe.
- **One writer per shard's map.** The remove relies on it; a second concurrent saver would have
  its half-written temp removed under it.

## Still open

- A crash between the rename and the intent log's removal (`compact_map`) leaves the new map and
  the old intent log, which is replayed onto a map that already holds it. That replay is
  idempotent for the intents the map writes, and it has not been exercised by a test here.
- The kill test itself is not in the fixture: the fixture's crash points are named ones in the
  code (`CRASH_AT`), and none of them sits inside `save`.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_leftover_temp_map_does_not_stop_a_save` (`shoal-core/src/server/tables/storage/fs/map.rs`) | A save refuses to run over a temp map a killed save left, and the shard fails to start |
| Kill a follower under load ([cluster testing](../../cluster-testing/correctness.md#kill-a-follower)) | A node killed mid-save crash-loops on every restart |

## Related

- [Resolved #136](upgrade-a-down-node.md), which was needed to deliver this fix to the node it
  was for.
- [Recovery](../../storage/recovery.md).
