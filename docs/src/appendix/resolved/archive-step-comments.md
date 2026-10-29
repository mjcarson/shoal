# 272. The object archive's reader and writer had no step comments

`shoal-core/src/server/objects/archive.rs` came with [F93](../../features/operations-and-the-real-cluster.md)
with ten functions that had no inline comment on any step - `read_record`, `ArchiveReader::open`,
`next` and `skip`, `ArchiveWriter::end`, `abort` and `finish`, `write_manifest`, `read_manifest`
and `manifest_path` - and four with some steps bare, against the repository's rule that every step
has one. Every item had its docstring. Filed by a code review of F87 to F93.

## Symptom

A reader of the module met its least obvious code, the record reader whose lengths come from the
file, as bare statements.

## Cause

The module was written in one change with the archive's format and its tests, and the comments of
its record half were not written with it.

## Evidence

**Established by reading the source.**

## The fix

A comment on every step of the fourteen functions, and `next`'s `# Errors` names the two failures it
left out: a record that runs past the archive's records and an object that fails its checksum. Made
in the change that bounded `read_record`'s lengths ([Resolved #264](archive-record-length-bound.md)).

## Alternatives rejected

**Leaving it for the next change to the module.** That change was item 264's, which rewrote the
function most in need of them.

## Invariants to uphold

- **Every step has its comment**, as CLAUDE.md asks of every function.

## Still open

Nothing.

## Tests

None: the change is comments, and the module's tests pass unchanged.

## Related

[Resolved #264](archive-record-length-bound.md), [F93](../../features/operations-and-the-real-cluster.md).
