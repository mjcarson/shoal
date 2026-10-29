# 228. A row holding a `u128` behind a pointer could end its shard at a restart

rkyv lays out a `u128` at a sixteen byte boundary of its buffer. Its validator refuses a buffer
whose start puts that field off one. The intent log and the archives wrote their records back to
back, so a record's payload started wherever the record before it ended. Until
[F80](../../features/object-wire.md), no row type the engine stored needed more than eight. An
object's entry carries its id as a `u128` inside the `Vec` of entries its path's row holds, which
made `{Bucket}ObjectMeta` the first row to need sixteen. It was found by F80's own acceptance test.

## Symptom

A standalone node holding a bucket failed at its restart, intermittently:

```text
ShardFailed { shard: 0, error: "Rkyv(... UnalignedPointer { address: …8, align: 16 } }, trace: StructCheckContext { struct_name: \"ArchivedImagesObjectMeta\", field_name: \"entries\" } ... UnnamedEnumVariantCheckContext { enum_name: \"ArchivedUnsortedIntents\", variant_name: \"Insert\", field_index: 1 } ...)" }
```

`examples/bench_dataset`'s `every_acknowledged_byte_is_read_back` puts four hundred objects
eight at a time, stops the node, and starts it again. It failed this way in about one run in five
to fifteen. The replay's `scan_keys` takes its intent with `?`, so one refused record failed the
shard and the node did not start. Nothing had been lost: every byte was on disk, and the node
could not read it.

The same refusal waited in the archives. A compaction writes partitions back to back through
`write_record`, so a partition loaded from an archive was refused as `CorruptArchive` naming its
partition. That happens on a cluster node as well, whose compactor folds the shared WAL into the
same archives.

## Cause

Both formats frame a record as eight bytes of size, eight of checksum and then the payload. They
place the next record at the byte after the payload. A reader slices the record out of a block it
read at a block boundary, so the payload lies at a boundary of its buffer exactly when it lies at
one in the file.

The payload is an archive whose length rkyv ends at its root, a multiple of the root's own
alignment. **A row with a `u128` of its own has a root of sixteen**, so every record of it is a
multiple of sixteen long and never moves the next one off a boundary. The rows that do are the
ones whose `u128` sits **behind a pointer**: in a `Vec`, a `Box` or a nested struct. Their root
is eight, so their length can end eight past a boundary, and the field inside still needs
sixteen. `ObjectMeta.entries: Vec<ObjectEntry>` is that shape. So is any sorted partition of a row
holding a `u128` at all, since a sorted partition keeps its rows in a `Vec`.

The intent log gives every staged buffer a block boundary of its own, so records sent one at a
time never shared one: each started at a boundary. That is why the defect hid. Eight puts at once
staged several records in one buffer, and a record behind one that ended eight past a boundary was
refused at replay. A probe in the reader during the failing run showed one record at
`pos % 16 = 8`, with every other record of the 805,376 byte log at a boundary.

A cluster node's apply never met it. Each table's `apply` copies a command's payload into an
`AlignedVec<16>` before validating it, as `partition_key_and_intent_checked` does for the
compactor, and a forwarded answer arrives in one. Its archives did meet it.

## Evidence

**Reproduced** by `shoal/tests/aligned_records.rs`, written first and run against the unfixed
tree. It inserts sixty-four rows a table in one bundle on one shard, each holding its `u128`s in a
`Vec` with a payload of every length mod sixteen. It then restarts, once for the intent log and
twice for an archive:

```text
test wide_rows_load_from_an_archive ... FAILED
test wide_rows_replay_from_an_intent_log ... FAILED
Error: Server(ShardFailed { shard: 0, error: "Rkyv(Error { inner: BoxedError { inner: ErrorWithTrace { error: BoxedError { inner: ErrorWithTrace { error: BoxedError { inner: UnalignedPointer { address: 127341841768552, align: 16 } }, trace: StructCheckContext { struct_name: \"ArchivedWideId\", field_name: \"ids\" } } }, trace: UnnamedEnumVariantCheckContext { enum_name: \"ArchivedUnsortedIntents\", variant_name: \"Insert\", field_index: 1 } } } })" })
test result: FAILED. 0 passed; 2 failed
```

The first draft of that test gave each row an inline `u128`, and the intent half passed: a
root of sixteen pads every record to a multiple of sixteen. The archive half failed anyway, on
the sorted table, whose partitions hold their rows in a `Vec`:

```text
test wide_rows_replay_from_an_intent_log ... ok
test wide_rows_load_from_an_archive ... FAILED
Error: Client(Server { query_id: Some(01a12c3b-789b-7240-abb9-b9a6d09b4d21), index: Some(0), code: CorruptArchive, msg: "partition 8578725530060225106 of SortedWideId could not be read" })
```

That draft is what showed the shape is a pointer to a `u128` under a root of eight, and not the
`u128` alone.

With the fix, both tests pass. `every_acknowledged_byte_is_read_back` then passed twenty runs of
twenty. With the archive padding alone disabled, `wide_rows_load_from_an_archive` and
`archive_records_start_at_a_boundary` both failed again.

## The fix

- **Every intent log record starts at a sixteen byte boundary.** `StreamWriter::prep_record`
  makes room for the record and the widest skip it could need (`RECORD_PAD_MAX`, 23 bytes).
  Where the buffer's position is off a boundary, it stages a skip there: the eight byte
  `ALIGN_SENTINEL` (`u64::MAX - 1`, one below the flush's `PAD_SENTINEL`) and zeros to the
  boundary at or after its end. `skip_region` writes the skip and `record_pad` sizes it, both pure
  and both tested. `FileSystem::commit` stages every record through it.
- **The reader steps over a skip.** `IntentLogReader::next_buff` reads `ALIGN_SENTINEL` as "the
  next record is at the boundary after these eight bytes" and reads on. A skip is never handed
  back as a record, and never marks the log damaged.
- **Every archive record starts at a boundary.** `write_record` writes zeros ahead of a record's
  prefix where the archive's position is off one. The header is sixteen bytes, so the first record
  needs none. Every archive read starts at an offset its map entry holds, so nothing reads the
  padding.
- **The padding is counted.** An archive's live bytes are each record's `record_footprint`, its
  prefix and its payload rounded up to the boundary, and no longer the prefix and payload alone.
  The live-percent test an archive pass applies therefore still compares like with like
  ([Resolved #179](archive-usage-prefix.md)).

A skip costs a record 8 to 23 bytes when it is needed, and nothing when the record is already at
a boundary, as every record of a root of sixteen is. An archive's padding is 0 to 15 bytes a
record.

## Alternatives rejected

**Copy a misaligned record into an aligned buffer when it is read.** The replay and the scan
take a `ReadResult`, and so does a loaded archive: a `ValidatedArchive<P, ReadResult>` holds the
buffer it validated, and a copy would change the cache's buffer type or add a copy to every
partition load. The writer knows where a record lands and pays a few bytes once. The reader would
pay a copy on every read, forever.

**Pad the payload's tail, and step the reader by the padded length.** rkyv finds an archive's root
at the end of its buffer, so trailing zeros move the root. A reader that steps by a padded length
cannot read any log written before it, where records end anywhere. A skip ahead of the record
keeps every existing log readable as it was.

**Use the flush's `PAD_SENTINEL`.** It skips to the next block boundary, 512 to 4,096 bytes away.
A record would get a block to itself, and that is the batching
[O34](../optimizations.md) exists to prevent.

**rkyv's `unaligned` feature.** It archives every primitive at an alignment of one. That changes
the archived form of every type on the wire and on disk, and with it every schema's fingerprint,
and it makes every access an unaligned load. The defect is the framing, not the archive.

**Store an object's id as two `u64`s or sixteen bytes.** It would have hidden the defect from the
one type that met it. Any schema may hold a `u128` in a `Vec` today, and a sorted table of a row
with any `u128` already met it in its archives. The engine stores what a schema declares.

## Invariants to uphold

- **A record of the intent log is staged through `prep_record`**, never `prep`, so it starts at
  `RECORD_ALIGN`. A buffer always starts at a block boundary, and a reader always reads from one,
  so a record at a boundary of the file is at a boundary of its buffer. A change that lets a buffer
  start elsewhere breaks this.
- **An archive record is written through `write_record`**, and read only at the offset its map
  entry names. A reader that walks an archive from one record to the next has to step over the
  padding, which is zeros and not a record. Nothing does today.
- **`RECORD_ALIGN` and `ARCHIVE_RECORD_ALIGN` are at least the widest alignment rkyv gives any
  archived type a schema can hold.** That is sixteen with rkyv 0.8's default features. A type of
  wider alignment needs them raised.
- **`ALIGN_SENTINEL` and `PAD_SENTINEL` are sizes no record can have.** Both are within two of
  `u64::MAX`.
- **An intent log this build wrote cannot be read by a build from before it.** An older reader
  reads a skip as a size header and does not survive it. Standalone nodes are never downgraded
  across this change with logs not yet replayed. Logs from before it read unchanged.
- **A command's payload is copied into an `AlignedVec<16>` before it is validated.** That copy is
  why a cluster node's apply never met this; the WAL's frames are not aligned.

## Still open

- Format 1 archives, from before [F44](../../features/repair.md), have `[size][payload]` records
  with no header and no padding. A format 1 archive of a row that needs sixteen would be refused.
  None can exist, since no such row was stored before F80, and an archive pass rewrites every
  format 1 archive as format 2.
- The map intent log (`read_intents`) is read through the same reader and is not staged through
  `prep_record`. A `MapIntent` is eight byte aligned and stays so.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `wide_rows_replay_from_an_intent_log` | `shoal/tests/aligned_records.rs` | The restart fails with `UnalignedPointer { align: 16 }` in `ArchivedUnsortedIntents::Insert` |
| `wide_rows_load_from_an_archive` | `shoal/tests/aligned_records.rs` | The same at the second start; with only the archive padding reverted, the third start's gets fail `CorruptArchive` |
| `aligned_records_round_trip_at_a_boundary` | `shoal-core/src/server/tables/storage/fs/stream_tests.rs` | Without the reader's skip, the first skip reads as a size and the log ends there, flagged damaged; without the writer's, a read's pointer is off a boundary |
| `record_pad_reaches_a_boundary` | `shoal-core/src/server/tables/storage/fs/stream_tests.rs` | A skip that misses the boundary, lacks room for its sentinel or outgrows `RECORD_PAD_MAX` |
| `archive_records_start_at_a_boundary` | `shoal-core/src/server/tables/storage/fs/tests.rs` | A payload's offset is off a boundary, or the counted bytes are not the records' footprints |
| `every_acknowledged_byte_is_read_back` | `examples/bench_dataset/tests/objects.rs` | Fails at its restart in about one run in five to fifteen |

## Related

- [F80](../../features/object-wire.md), whose acceptance test found this, and whose inline
  objects are the first rows to need it.
- [F79](../../features/bucket-schema.md), which generated the row shape.
- [Resolved #1–3](durability.md), the intent log's framing and padding.
- [Resolved #179](archive-usage-prefix.md), the record footprint the live bytes count.
- [Resolved #148](stale-intent-log-tail.md), the last change to how a log's end is read.
