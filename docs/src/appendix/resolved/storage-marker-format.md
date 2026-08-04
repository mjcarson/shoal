# 45. The storage marker's format field was written and never read

## Symptom

`shoal-meta.json` carries a `format` version. A marker written in any format at all — including
one a future Shoal invented, with different fields meaning different things — was accepted by
today's code, which then read the shard count out of it and started.

The failure this guards against is the one the marker itself exists to prevent, one level up. A
`format: 2` marker might record shards per *tablet range* rather than per node, or count from one
rather than zero. Reading `shards: 4` out of it and comparing that to `cpus.len()` is a
comparison between two numbers that do not mean the same thing, and it can just as easily pass as
fail. When it passes, the server starts and looks for every partition in the wrong place — which
is precisely what [items 11 and 12](tablet-ring.md) built this file to make impossible.

## Cause

`StorageMeta::claim` (`server/meta.rs`) parsed the marker and then checked one of its two fields:

```rust
Ok(raw) => {
    // parse the metadata we found
    let found: StorageMeta = serde_json::from_slice(&raw)?;
    // a different shard count would look for every partition in the wrong place
    if found.shards != shards {
        return Err(ServerError::Shoal(ShoalError::ShardCountMismatch { .. }));
    }
    Ok(())
}
```

Everything else was already there. The constant:

```rust
/// The version of this metadata file's own format
const META_FORMAT: u32 = 1;
```

the field, its doc comment, and `StorageMeta::new` setting it on every marker written. The struct
was designed to be versioned and the version was never consulted — `serde` deserialized it,
`PartialEq` compared it in the tests, and `claim` walked past it.

This is a common shape and worth naming: a version field is only a version field if something
refuses on it. Otherwise it is a comment that costs a `u32`.

## Evidence

**Reproduced.** `an_unknown_format_is_refused` was written against the unfixed tree, staging a
marker one format ahead of the one this build knows:

```rust
let future = StorageMeta { format: META_FORMAT + 1, shards: 4 };
std::fs::write(StorageMeta::path(dir.path()), serde_json::to_vec_pretty(&future)?)?;
let error = StorageMeta::claim(dir.path(), 4).expect_err("an unknown marker format started");
```

and it failed, because `claim` returned `Ok(())`:

```
---- server::meta::tests::an_unknown_format_is_refused stdout ----
thread 'server::meta::tests::an_unknown_format_is_refused' panicked at
shoal-core/src/server/meta.rs:169:47:
an unknown marker format started: ()
```

The `()` is the `Ok` value. A directory marked by a format this build has never seen was claimed
as a directory it could write to.

## The fix

The format is settled before anything is read out of the marker:

```rust
// a format we cannot read makes every field inside it a guess, including
// the shard count, so settle the format before anything is read from it
if found.format != META_FORMAT {
    return Err(ServerError::Shoal(ShoalError::StorageFormatMismatch {
        found: found.format,
        expected: META_FORMAT,
    }));
}
// a different shard count would look for every partition in the wrong place
if found.shards != shards { .. }
```

with `ShoalError::StorageFormatMismatch { found: u32, expected: u32 }` alongside
`ShardCountMismatch`, carrying both numbers so the message says which format it could not read
rather than only that it could not.

The ordering is the fix, not just the check. Checking the shard count first would compare a
number whose meaning is unknown, and a mismatch reported as `ShardCountMismatch` would send an
operator to look at `resources.cores` for a problem that is not there.

The comparison is `!=` rather than `>`. A marker from an *older* format is no more readable than
a newer one — this build knows one layout — and treating `format: 0` as safe to read would be the
same assumption in the other direction. If backward compatibility is ever wanted it will be a
match on known versions, not an inequality.

## Alternatives rejected

**Accept any format less than or equal to `META_FORMAT`.** Reads as forward-compatible and is
not: it asserts that every earlier format is a readable subset of this one, which is a claim about
formats that do not exist yet to be checked. There is one format. When there are two, the code
that reads both can say so explicitly.

**Ignore the format and validate the fields.** `serde` already fails on a marker whose fields
cannot be deserialized, so the temptation is to lean on that. It only catches a format that
changed *shape*. A format that keeps `shards: usize` and changes what it counts deserializes
perfectly and is exactly the case that matters.

**Rewrite an unknown marker with the current format.** This is the "claim it and move on" path,
and it is the behaviour being removed — it destroys the only record of what wrote the directory.

**Drop the field.** Honest, and it was considered, since an unused field is worse than no field.
Rejected because the module doc names the fields it expects to grow — a cluster id, a node id, a
topology epoch — and every one of them makes the format more likely to change, not less.

## Invariants to uphold

- **The format is checked before any other field.** Every field in the marker means what it means
  only within a format, so a format we do not understand makes the shard count in it a guess. A
  future check added to `claim` goes *after* the format check, not before.
- **`META_FORMAT` is bumped when the meaning of a field changes, not only when a field is added
  or removed.** `serde` catches the shape; only the version catches the meaning, and the meaning
  is the case that gets through.
- **A marker is never rewritten in place.** `claim` writes a marker on exactly one path — the one
  where no marker exists. Anything that "upgrades" a marker has to move the data it describes
  first, and no such migration exists ([items 11, 12](tablet-ring.md)).
- **`StorageMeta::new` is the only constructor used outside tests**, so every marker written
  carries the current `META_FORMAT`. The test that stages a future marker builds the struct
  literally, which is the only place that is correct.

## Still open

**The marker still only guards the default storage root.** A per-table `storage.tables` override
pointing elsewhere is unmarked, so this check does not run for it at all — filed as
[item 43](../known-issues.md#43-the-storage-marker-only-guards-the-default-storage-root) and
unaffected by this fix.

**A directory written before the marker existed has none.** `claim` treats a missing marker as a
new directory and claims it, which for a directory written by the vnode ring means starting and
routing every partition to the wrong shard. Filed as
[item 46](../known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused);
the format check does not help, because the problem is a marker that is absent rather than one
that is wrong.

**There is still exactly one format**, so this check has never refused anything in the field. It
is a guard against a change that has not happened yet, which is the only time it can be added
without a migration behind it.

## Tests

| Test | What it pins |
| --- | --- |
| `an_unknown_format_is_refused` (`server/meta.rs`) | A marker from a format this build does not know is refused, and refused *as* a format mismatch rather than as a shard count one |
| `the_same_shard_count_is_allowed_back` (`server/meta.rs`) | The ordinary restart still passes the new check — it would fail if `META_FORMAT` and `StorageMeta::new` ever disagreed |
| `an_unclaimed_directory_is_claimed` (`server/meta.rs`) | Every marker written carries the current format, which is what makes the check above meaningful |

## Related

- [Items 11, 12, 37](tablet-ring.md) built `StorageMeta` and explain why a storage directory can
  only be read back by what wrote it.
- [Item 43](../known-issues.md#43-the-storage-marker-only-guards-the-default-storage-root) and
  [item 46](../known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused)
  are the two remaining holes in the same guard.
- [Configuration](../../getting-started/configuration.md) describes the marker an operator finds
  in a storage directory.
