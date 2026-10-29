# 264 (in part). A corrupt length in an archive record ended shard zero's blocking thread

An archive of a bucket's objects is a run of records, each an entry's length, the entry, the
object's length, its bytes and its checksum ([F93](../../features/operations-and-the-real-cluster.md)).
A restore verifies the whole archive before it puts anything, and the reader allocated each object
straight from the length the record named. One flipped bit in that length, in a file of the right
size, allocated the length it named: past `isize::MAX` a capacity overflow panicked shard zero's
one blocking thread, and below it a length too large to map aborted the node in its allocator.
Filed by a code review of F87 to F93. **The first half is fixed**; the second, that a valid archive
holds each object whole in memory outside the budget a client's put is held to, stays on the
[known issues page](../known-issues.md#264-a-corrupt-length-in-an-archive-record-kills-shard-zeros-blocking-thread-and-its-restore-hangs).

## Symptom

A restore of a backup with one corrupt record length never finished and never failed: the answer
to `ArchiveReader::open` never came, so `driving_archives` stayed taken and no other archive on the
node was driven, and shard zero's next blocking call panicked on `failed to enqueue blocking
operation`, the pool's last receiver gone with the thread. A length that the allocator could not
map ended the node.

## Cause

`read_record` bounded an entry's length by `MAX_ENTRY_BYTES` and nothing else, and allocated its
object as `vec![0u8; usize::try_from(length).unwrap_or(usize::MAX)]`. `verify` checked the file's
length against its manifest before it read a record, so a truncated archive was refused by name,
but the trailer and the manifest's checksum are read only after every record, and a length changed
in place reached the allocation. The blocking pool runs its work bare, so the panic ended the thread.

## Evidence

**Reproduced** by `a_corrupt_record_length_is_refused_before_it_is_allocated`, written first and
run on the unfixed tree against `verify`, the work `open` hands the pool: an archive of one record
whose object length has its top bit set.

```text
thread 'server::objects::archive::tests::a_corrupt_record_length_is_refused_before_it_is_allocated'
panicked at library/alloc/src/raw_vec/mod.rs:28:5:
capacity overflow
```

The blocking thread's death and the node's next blocking call were read from the glommio fork and
not reproduced.

## The fix

**Each length is judged against what is left before the trailer** before anything is allocated for
it: `read_record` is handed where the records end, the entry's length and the object's are each
refused when they run past it, and the object's length is converted without a fallback. `verify`
passes the trailer's offset and `ArchiveReader` keeps it for `next`. A corrupt length is now the
archive refused by name, "does not verify against its manifest: an object runs past the archive's
records".

## Alternatives rejected

- **Catching the panic on the blocking pool.** The allocator's abort is not a panic, and a thread
  that caught one would still have asked for the memory.
- **Checking the trailer and the checksum first.** It needs every record read to compute, which is
  the read that allocates.

## Invariants to uphold

- **A length read from an archive is judged against the file before it is used.** A field added to
  a record is bounded the same way.

## Still open

A valid archive's objects are each held whole in memory, in `verify` and again in `next`, outside
the assembly budget a client's put is drawn from: a 10 GiB object asks for 10 GiB at once, and
[O117](../optimizations.md#o117-an-archive-restore-reads-each-archive-two-or-three-times-and-copies-each-object-on-every-try)'s
copies on every try ask for more. Reading an object in pieces drawn from that budget is the
remainder of item 264, and O117's change.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `objects::archive::tests::a_corrupt_record_length_is_refused_before_it_is_allocated` | A record length with its top bit set panics with a capacity overflow, and one past the file is read as far as it goes |

## Related

[F93](../../features/operations-and-the-real-cluster.md), which made the archive;
[Resolved #272](archive-step-comments.md), the same module's comments, in the same change;
[O117](../optimizations.md#o117-an-archive-restore-reads-each-archive-two-or-three-times-and-copies-each-object-on-every-try).
