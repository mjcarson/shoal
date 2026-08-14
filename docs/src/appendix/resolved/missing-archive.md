# 57. A missing archive was created empty rather than reported

## Symptom

A get whose archive is not on disk never comes back, and the shard that was reading it is gone.
Nothing says which archive went missing, and a zero-byte file is left behind at the name the read
looked for — so every later read of that partition finds the empty file and fails the same way.

The shard dies on an rkyv validation error over a partition that was written correctly:

```
shard dying on Rkyv(Error { inner: BoxedError { inner:
    InvalidSubtreePointer { address: 1, size: 24, subtree_range: 1..1 } } })
```

`subtree_range: 1..1` is the whole of it: the archive that partition was read out of had no bytes
in it, because the read had just created it.

## Cause

`ArchiveMap::get_archive` opened with `create(true)`:

```rust
let file = OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .dma_open(&path)
    .await?;
```

`.../fs/map.rs`, `ArchiveMap::get_archive`

So a read whose archive was gone did not fail. It made an empty one, `read_at(entry.offset,
entry.size)` came back short against it, and the bytes it came back with went on to
`ValidatedArchive::new` (`.../persistent/sorted.rs`), which is where the failure finally
surfaced — as corruption, over an archive that was never corrupt because it was never written.

That is the wrong place for it in three separate ways.

**It cannot name the file.** By the time the bytes are being validated, all that is left is that
they do not parse. The archive id and the path it was looked for at are three frames up.

**It ends the shard.** `ValidatedArchive::new`'s error propagates out of
`PersistentTable::load_partition`, out of `ShoalTables::load_partition`, and out of
`Shard::start`'s message loop — which returns it, ending the shard. One missing file becomes an
outage for every partition that shard owned. The classification and release path
[Resolved #16, 51](partition-load-failure.md) built exists precisely so a read that cannot be done
answers the queries parked on it instead, but this failure was never routed into it: it did not
happen in the loader, it happened two hops later in the table.

**It is silent.** `ShoalPool::exit` logs a shard's join result at `ERROR` and returns `Ok(())`
either way, so a shard that died takes nothing with it that a caller can observe. In a test with
no subscriber installed, `pool.exit()` returns `Ok(())` on a pool whose shard died twenty seconds
earlier.

Underneath all of that, the empty archive persists. The archive map still points the partition at
that id, and the file now exists, so the *next* read opens it successfully and fails the same way.
The defect converts a transient inconsistency into a permanent one.

**Reachable, not theoretical.** The compactor deletes archives it has rewritten
(`.../fs/compactor.rs`, at the end of `compact_archives`) after re-pointing their entries at the
new one. A read that found its entry before that re-point and opens it after the delete looks for
a file that is gone.

## Evidence

**Established by reading the source, then reproduced deterministically.**

Reproducing it needs no fault injection hook, for the same reason
[Resolved #16, 51](partition-load-failure.md#evidence) did not: archives live in their own
directory apart from the intent logs and the archive map. Moving the archive *files* out of that
directory — rather than taking the permissions off it, which is how the unreadable-archive test
produces an IO error — leaves the map saying exactly where a partition is and takes away the file
it names. That is the shape of the compactor race, and unlike the permissions trick it works as
root.

Against the unfixed tree, `a_get_whose_archive_is_missing_does_not_end_its_shard` produces all
three halves. The get never comes back:

```
thread 'a_get_whose_archive_is_missing_does_not_end_its_shard' panicked at
shoal/tests/persistent_sorted_table.rs:2564:5:
a get whose archive was missing never came back
```

The shard it was waiting on died on the validation error quoted under **Symptom**, and
`pool.exit()` on that pool still returned `Ok(())`. And the archive the read could not find had
been created, empty, at the name it looked for:

```
a missing archive was created empty rather than reported:
[("…/TestRecord/archives/167c3a88-faf4-44e8-be3a-d808c519ea95", 0)]
```

## The fix

**`get_archive` no longer creates.** It opens what must already be there, and translates the one
errno that means the file is not there into an error that names it:

```rust
let file = match OpenOptions::new().read(true).write(true).dma_open(&path).await {
    Ok(file) => file,
    // this archive is not on disk, so say which one instead of making an empty one
    Err(error) if is_not_found(&error) => {
        return Err(ServerError::Shoal(ShoalError::ArchiveMissing {
            archive: *archive_id,
            path,
        }))
    }
    // any other failure to open is reported as the IO error it is
    Err(error) => return Err(error.into()),
};
```

`is_not_found` is a free function beside it, because glommio reports the same errno as either
`GlommioError::IoError` or `GlommioError::EnhancedIoError` depending on whether it had a path to
attach, and an open by path can come back as either.

**The filed fix direction's premise was wrong, and the fix is smaller for it.** Item 57 said
`get_archive` "has two callers with opposite needs — the writer wants the file created", and
proposed splitting it. It has three callers and all three are reads:
`FileSystem::load_partition_direct`, `loader::read_partition_once`, and
`FileSystemCompactor::load_partitions_for_intents`. The writer is `ArchiveMap::get_active_writer`,
a sibling function with its own `create(true)` open, and it is the only thing that ever creates an
archive — which it does for the active archive only. `compact_archives` opens its candidates with
a bare `DmaFile::open`, which never created either. So nothing had to be split; `get_archive`
simply stopped creating.

**`ShoalError::ArchiveMissing { archive, path }` is a new variant**, carrying the id and the path
so the loader's existing `ERROR` log names the file that went missing.

**`classify` gives it `Fatal` explicitly.** It would reach `Fatal` through the catch-all anyway,
but the arm is spelled out because the arm *above* it is the one that matters: `IO`/`GlommioIO` is
`Retryable`, and an ENOENT is an IO error. Left to its errno, a missing archive would be attempted
three times, 2 ms apart, stalling every query parked behind it to arrive at the same answer.

**Nothing else changed.** `Fatal` was already routed: `read_partition` logs the give-up naming the
partition and the error, sends `ServerMsg::PartitionLoadFailed`, and the shard's `fail_partition`
releases the parked queries with `skip_disk` set. The failure now happens in the loader, which is
the one place that already knew what to do with it.

## Alternatives rejected

**Split into `get_archive` and `get_or_create_archive`, as filed.** There is no caller for the
creating half — `get_active_writer` does its own create and has to, since it also inserts into
`all_archives` and builds a `DmaStreamWriter`. A second creating function with no callers is a
loaded gun for the next person who reaches for the shorter name.

**Open the read path read-only.** The obvious tightening, and it breaks the writer. `get_archive`
caches its handle in `loaded_archives`, and `get_active_writer` serves the active archive out of
that same cache by duplicating whatever it finds there into a `DmaStreamWriter`. A read that
touched the active archive first would leave a read-only handle behind for the writer to find.
`write(true)` on the read path is load-bearing, which is why it has a comment saying so.

**Check `path.exists()` before opening.** A second syscall on every uncached archive read, and it
introduces the race it is meant to detect: the compactor can delete the archive between the check
and the open. Mapping the open's own errno has neither problem.

**Classify it `Retryable` and let the retries establish it.** Three attempts and 6 ms of stalled
queries to learn what the first ENOENT already said. Retrying is for a shortage that other reads
give back; a deleted file is not one.

**Report it from `ValidatedArchive::new` instead, where the failure was already surfacing.** That
is the wrong end. Validation sees bytes, not files — it cannot distinguish an archive that was
never written from one that was corrupted, which is a distinction the operator needs, and it is
past the point where the queries can still be released.

**Leave the create and delete the empty archive afterwards.** It would clear the stray file and
none of the rest: the read still comes back short, still fails as corruption, and still ends the
shard.

## Invariants to uphold

- **`get_archive` never creates and `get_active_writer` always does.** These are the only two
  functions that open an archive by id, and the split between them *is* the fix. A `create(true)`
  added back to the read path restores the whole defect, silently — every test here would still
  pass except the two that assert on the file not being there.
- **The read path keeps `write(true)`.** `loaded_archives` is one cache shared with
  `get_active_writer`, which duplicates what it finds there into a stream writer. Narrowing the
  read open to read-only gives the writer a handle it cannot write through, and the failure would
  appear as a broken write in a completely different code path.
- **`ArchiveMissing` stays out of `classify`'s `IO`/`GlommioIO` arm.** It is an ENOENT wearing a
  different name specifically so it does not classify by its errno. Folding it back in makes it
  retryable.
- **`ArchiveMissing` is not `Absent`.** A pruned partition is `Absent` because the entry is gone
  from the map and a replay answers correctly by finding nothing. Here the entry is still in the
  map pointing at a file that is not there, so a replay would park on the same failure — which is
  what `skip_disk` on the released query exists to prevent, and only `Fatal` sets it.
- **A read that fails must not leave anything behind on disk.** The empty archive was worse than
  the failed read, because it made the failure permanent. Anything added to this path that writes
  before it knows the read succeeded reintroduces that.

## Still open

- ~~[Item 56]: the get that could not find its archive is reported to the client exactly as an
  empty partition is. The server now names the missing file in its log; the client still cannot
  tell a missing archive from no such row. The integration tests here assert `QueryDidNotSucceed`,
  which is what that limitation looks like from outside.~~ **Done** —
  [Resolved #56, 61](response-error-channel.md). Those tests now assert
  `Errors::Server { code: ErrorCode::ArchiveMissing }`, so the class of failure this page is about
  is the class the client is told, and it is distinct from the `StorageRead` an unreadable archive
  gives. That the assertion had to change is the measure of it.
- `FileSystemCompactor::load_partitions_for_intents` is the third caller, and it now gets the named
  error rather than an rkyv failure over an empty file — but it propagates it out of the compaction
  job rather than handling it. That is an improvement in what the abort *says* and not in whether
  it aborts. It is **not filed as a defect**, because the read that this fix was about races the
  compactor and this one does not: the compactor is the only thing that deletes an archive, it runs
  on the shard that owns the map, and its jobs are sequential. Whether that reasoning has a hole in
  it is a claim about a running server, and this page does not make it.
- A shard that dies takes its error with it. `ShoalPool::exit` logs the join result at `ERROR` and
  returns `Ok(())` regardless, and nothing reports a shard's death before then — which is why the
  reproduction below could only see this defect as a timeout. This fix removes one way to kill a
  shard, not the blindness to it. Filed as
  [item 58](../known-issues.md#58-a-shard-that-dies-is-not-reported-to-whoever-started-the-pool).

## Tests

| Test | What breaks without it |
| --- | --- |
| `a_get_whose_archive_is_missing_does_not_end_its_shard` (`shoal/tests/persistent_sorted_table.rs`) | The whole path. Against the unfixed tree the get times out, because the shard that would have answered it died on the validation error. |
| `a_get_whose_archive_is_missing_does_not_end_its_shard` (`shoal/tests/persistent_unsorted_table.rs`) | The unsorted table's share. `get_archive` is shared, but the release path a `Fatal` failure lands on is not — the two tables park and release blocked queries through different code. |
| The `recreated()` assertion in both | The stray zero-byte archive, which is the half that outlives the read. A fix that reported the failure but still created the file would pass every other assertion here. |
| The second half of both, after the archives are put back | `check_disk` being left true. A failed read that convinced the table its memory copy was complete would pass the first half and fail here. |
| `a_missing_archive_is_reported_not_created` (`.../fs/tests.rs`) | `get_archive` itself, without a server around it: that the error names the archive, and that nothing is left at the path it looked at. |
| `a_missing_archive_is_not_retried` (`.../fs/tests.rs`) | The `classify` arm. Folding `ArchiveMissing` back into the `IO` arm would still answer correctly, just three attempts later, so no end-to-end test can see this. |

## Related

- [Resolved #16, 51](partition-load-failure.md) — the classification and release path this failure
  is routed into. Item 57 was filed on its "Still open" list, found while writing it.
- [Resolved #56, 61](response-error-channel.md) — the error channel, which is what let the released
  get finally say that it failed, and with which code.
- [Archives and the Archive Map](../../storage/archives-and-map.md) and
  [Compaction](../../storage/compaction.md) — where archives are written and when they are
  deleted.
