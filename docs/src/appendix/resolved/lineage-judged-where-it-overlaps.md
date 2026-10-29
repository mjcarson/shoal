# 271. The lineage judge compared from the first id, so a lineage cut to 255 ids was another schema

A schema's lineage is its id after each of its fields, and two nodes are of compatible schemas when
one lineage is a prefix of the other ([F93](../../features/operations-and-the-real-cluster.md)). A
hello carries at most `MAX_LINEAGE`, 255, ids, the last ones, and a peer that sends none is judged by
its bare id as a lineage of one. `compatible` compared the two from their first ids, so a lineage cut
to its last ids, or a bare id against a whole lineage, compared one schema's first field with
another's last. Filed by a code review of F87 to F93.

## Symptom

- A schema of 256 fields or more never formed a cluster: between two nodes of one build every hello
  was refused `SchemaMismatch`, the control lane's too. A join, which carries its lineage whole, was
  unaffected. No schema here is near that size.
- A bare id, `[X]` against `[a, b, X]`, was another schema, against the module's own rule that a
  lineage of one element and a bare id are the same thing. Between F93 builds it never happens,
  since every hello and join carries its lineage; a pre-F93 peer of the identical schema, which F93
  does not support and `MIN_PEER_VERSION` still lets reach the judge, was refused as another schema
  rather than by its version.

## Cause

`compatible` took the shorter lineage's length and compared `ours[..min] == theirs[..min]`. A cut
lineage and a bare id are windows that end where their schema does, not prefixes that begin where it
began.

## Evidence

**Reproduced** by `a_bare_id_and_a_cut_lineage_are_judged_where_they_overlap`, written first and run
on the unfixed judge:

```text
thread 'shared::lineage::tests::a_bare_id_and_a_cut_lineage_are_judged_where_they_overlap' panicked
at shoal-proto/src/shared/lineage.rs:89:9:
assertion failed: compatible(&[1, 2, 9], &[9])
```

## The fix

**Two lineages are judged where they overlap.** `compatible` finds one side's last id in the other,
either way round, and compares the ids that end there as far back as both hold them. A whole
lineage that is a prefix of the other is the same judgment as before; a cut one, a bare id, and a
cut append are judged by the window they carry. An id in a lineage is the schema's after every field
before it, so a change to any field moves every id after it, and a window cut past the change still
differs.

## Alternatives rejected

- **Sending a `u16` count and never cutting.** A wire format change, and a new wire version, for a
  bound no schema here is near; the window was always enough to judge by.
- **Comparing only whether one side's last id is in the other.** The same answer for every lineage a
  schema makes, since each id holds every field before it; comparing the overlap costs nothing and
  does not lean on that alone.

## Invariants to uphold

- **A lineage is judged as a window that ends at its schema's id**, never as a prefix that starts at
  its first field: a hello's, cut to its last ids, and a bare id are both such windows.

## Still open

Nothing.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-proto lineage::tests::a_bare_id_and_a_cut_lineage_are_judged_where_they_overlap` | A bare id or a lineage cut to its last 255 ids is another schema |

## Related

[F93](../../features/operations-and-the-real-cluster.md), which made the lineage; the hello's
`write_lineage` and `read_lineage`.
