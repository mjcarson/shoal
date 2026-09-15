# 65. Two `gxhash` majors, and partition keys hashed by the one without `deterministic`

## Symptom

The workspace `Cargo.toml` pinned `gxhash = { version = "3", features = ["deterministic"] }`, and
`shoal-proto` and `shoal-core` - the two crates that hash anything - both pinned `"2.2"`, which
resolves to 2.3.1. Every partition key in every schema was hashed by 2.3.1, without that feature,
and the workspace pin was reachable from nothing: `cargo tree -i gxhash` named 2.3.1 alone, and the
`deterministic` feature was doing no work anywhere in the build.

The dead pin was the smaller half. The larger was that **a partition key's hash is a persistence
format** - it decides which partition a row is in and, through the top twelve bits, which tablet
owns it - and nothing said which gxhash produced it, whether that hash was stable across gxhash
versions, or what `deterministic` would change if it were turned on. Upgrading gxhash, or turning
the feature on, would silently re-hash every key and make every persisted directory unreadable,
and no test would have noticed. Two nodes built from different lockfiles would disagree about
which tablet a row is in, which is why [C1](../../distributed/node-identity.md#how-it-works)
made this a prerequisite of M1.

## Cause

Three manifests, each written at a different time, none of them read against the others. The
workspace entry was written first, at a major that was never resolved because no crate used the
workspace entry; the two crate entries were written to the major the code compiled against. The
claim in `architecture/partitioning.md` that the hash was "configured with the `deterministic`
feature" was true of the workspace file and false of the binary.

What `deterministic` would have done, had it reached anything: in gxhash 3 the feature affects
`GxBuildHasher` only - the seed a `HashMap` is built with - and nothing about `GxHasher::default()`,
which is what the derive uses (`shoal-derive/src/traits/partition_key.rs`). In gxhash 2.3.1 the
feature does not exist. `GxHasher::default()` is `with_state(create_empty())`, a zero state, in
both majors: stable within a major by construction, and the thing that decides where a row lives.

## Evidence

**Established by reading the manifests**, while deciding which crate should re-export gxhash for
[F15](../../features/client-server-split.md). `cargo tree --workspace -e normal -p gxhash` on the
unfixed tree printed one line, `gxhash v2.3.1`, and `cargo tree -i gxhash` traced it to
`shoal-core` and `shoal-proto` alone.

**The golden values were obtained by running the test on the unfixed tree.** The literals in
`shoal/tests/partition_keys.rs` were printed by a temporary variant of the test at
`shoal-derive`'s own `get_partition_key_from_values`, before the pin moved, and then pasted in:

```
GOLDEN u64 0 0x20bfea6723380d78 523
GOLDEN u64 1 0xd0aa3509a2d0dd24 3338
GOLDEN u64 42 0x3036ffe2cc259d89 771
GOLDEN u64 18446744073709551615 0x4c9f04f829df45f7 1225
GOLDEN str "" 0x57b58cc6750e034c 1403
GOLDEN str "a" 0x5d74a57f5e73eb79 1495
GOLDEN str "shoal" 0x74983e2946f5ce66 1865
GOLDEN str "the quick brown fox jumps over the lazy dog" 0x82396b81d80fbddd 2083
```

gxhash resolved to 2.3.1 before and after the pin moved - the lockfile did not change - so this
fix re-hashed nothing, and the test passed on both trees. That is the point of doing it now: the
next change to the major is the one that would move these, and it now cannot move them quietly.

Two things the test found on the way, filed and not fixed here: a table with two
`#[shoal(partition)]` fields does not compile, because the derive passes `&(&a, &b)` where
`(A, B)` is expected ([item 92](../known-issues.md)), and
`get_partition_key_from_archived_insert` hashes a string's bytes without the `0xff` terminator
`Hash for str` writes, so the archived and live paths would disagree for every string key if the
archived path had a caller ([item 93](../known-issues.md)).

## The fix

One pin, in `[workspace.dependencies]`, at the major already in use:

```toml
gxhash = "2.3"
```

with a comment saying what the pin is a promise about, and `gxhash = { workspace = true }` in
`shoal-proto` and `shoal-core`. The `deterministic` feature is gone from the workspace because
it does not exist at 2.x, and the docs that claimed it stop claiming it.

And the test, `partition_keys_hash_to_frozen_values`: two tables through the real derive, a `u64`
key and a `String` key, eight keys pinned to a literal hash *and* a literal tablet, through
`server::ring::TABLET_BITS`, which was made public for it. The tablet is asserted beside the hash
because the tablet is the number routing reads, and a change to `TABLET_BITS` moves rows the same
way a hash change does.

## Alternatives rejected

- **Moving to gxhash 3.** It re-hashes every key. That is a migration of every directory ever
  written, not a version bump, and there is no migration tool. Pinning where the data is was the
  only choice that could be made without one.
- **Turning `deterministic` on somewhere.** It does not exist at 2.x, and at 3.x it would not
  reach `GxHasher::default()`. The word was doing nothing but reassuring the reader.
- **A composite key in the golden set.** It does not compile; item 92. Fixing the derive is a
  small change to `partition_key.rs`, and it is a derive change that every schema in the workspace
  re-expands under, so it is its own fix with its own page rather than a line in this one.
- **Freezing the hash of an archived row too.** The archived path has no caller, and freezing a
  value nothing reads would pin the wrong answer (item 93) as the right one.

## Invariants to uphold

- **Exactly one `gxhash` resolves in the workspace.** `cargo tree --workspace -e normal -p gxhash`
  prints one line. A second major appearing anywhere - even unused - is the state this item was
  filed in.
- **`GxHasher::default()` is the hasher of every partition key**, on both peers. A seed, a
  `GxBuildHasher`, or a different hasher on either side is two peers hashing one key two ways.
- **The eight literals in `partition_keys.rs` are a persistence format.** Changing them is a
  migration and needs one; a build in which they fail cannot read an existing directory.
- **The tablet is part of the frozen set.** `TABLET_BITS` is public so the test can derive it
  the way the ring does; a change there is the same event as a hash change.

## Still open

**Item 92**, the composite partition key that does not compile, and **item 93**, the archived
string hash that disagrees with the live one. Both were found by this fix and neither is part of
it. **Item 46**, an unmarked directory being claimed, is unaffected.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `partition_keys::partition_keys_hash_to_frozen_values` | The pin moving to a major that hashes differently, or `GxHasher::default()` being replaced with a seeded one, fails eight assertions naming the key, the hash and the tablet |
| `shoal-client-check` — `a_row_hashes_its_own_partition_key` | A client hashing its keys through a gxhash the engine does not share; unchanged by this fix and the reason `shoal-proto` owns the re-export |

## Related

- [F37](../../features/node-identity-control-plane.md), which closed this as M1's prerequisite
- [F15](../../features/client-server-split.md), which made the question live
- [Partitioning](../../architecture/partitioning.md), whose claim about `deterministic` this
  corrected
- [C1](../../distributed/node-identity.md#how-it-works)
