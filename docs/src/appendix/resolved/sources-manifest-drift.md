# 78. Six of the paths deciding whether a capture is stale did not exist

## Symptom

`shoal-bench status` reported every capture taken since [F15](../../features/client-server-split.md)
as **unaffected** by changes to the wire protocol and to the client — changes those captures were
affected by. Nothing looked wrong: the table printed a verdict for every layer of every capture, and
the verdicts were plausible.

## Cause

[F7](../../features/bench-runner.md) decides whether a committed capture still describes the current
code by hashing the sources each measurement layer measures. Which sources those are is a
hand-maintained list in `docs/perf/sources.json`, and the file says so in its own note:

> This list is a hand maintained approximation and it will drift.

[F15](../../features/client-server-split.md) split `shoal-core` into `shoal-proto`, `shoal-client`
and `shoal-core`. The list was not updated with it, and **six of its seventeen paths named files
that had stopped existing**:

| Layer | Named | Actually |
| --- | --- | --- |
| micro | `shoal-core/src/shared/protocol.rs`, `.../protocol` | `shoal-proto/src/shared/protocol*` |
| macro | `shoal-core/src/client.rs`, `shoal-core/src/client` | `shoal-client/src/client*` |
| macro | `shoal-core/src/shared.rs`, `shoal-core/src/shared` | `shoal-proto/src/shared*` |
| micro | `shoal-core/src/server/tables/partitions` | never existed at all — `partitions.rs` has no module directory |

A path that does not resolve is **skipped silently**. So the micro layer was hashing two bench files
and one source instead of the protocol module that half of `wire.rs` exists to measure, and the
macro layer was hashing no client at all.

The seventh path is worth its own line: `shoal-core/src/server/tables/partitions` never existed in
any version of the tree. It was presumably written alongside `partitions.rs` on the assumption that
a module directory accompanied it. Nothing has ever noticed, which is the same fact as the rest of
this item.

## Evidence

**Reproduced**, not read. The test written for the fix was run first against the unfixed manifest:

```
thread 'fingerprint::tests::every_source_the_manifest_names_exists' panicked at
shoal-bench/src/fingerprint.rs:517:9:
docs/perf/sources.json names paths that do not exist, so those layers are not being watched: [
    "Micro: shoal-core/src/server/tables/partitions",
    "Micro: shoal-core/src/shared/protocol.rs",
    "Micro: shoal-core/src/shared/protocol",
    "Macro: shoal-core/src/client.rs",
    "Macro: shoal-core/src/client",
    "Macro: shoal-core/src/shared.rs",
    "Macro: shoal-core/src/shared",
]
```

Seven entries, six distinct dead locations. The same test passes against the corrected manifest.

## The fix

`docs/perf/sources.json`, and one test in `shoal-bench/src/fingerprint.rs`.

The dead paths are repointed at where F15 put the code. `shoal/benches/routing.rs` and the `ring.rs`
and `routing.rs` it measures are registered at the same time, since
[F24](../../features/routing-benchmarks.md) adds them in the same change and a bench nothing watches
would be this defect again on the day it landed.

`every_source_the_manifest_names_exists` walks every path in the committed manifest and asserts it
resolves, naming all the failures at once rather than the first.

Correcting the file moves the manifest hash, which is recorded in every capture taken afterwards —
so every existing capture is now correctly reported as no longer describing the layers it names.
That is the mechanism working and is not something to engineer around; the file's note has said so
since F8 made the same kind of change.

## Alternatives rejected

**Globbing the crates instead of listing paths.** It would not drift, and it would make every layer
stale on every commit to any crate, which destroys the one thing the digests buy: the ability to
narrow *stale* to *unaffected* when a commit did not touch what a layer measures. The list is
deliberately narrower than the crate.

**Making a missing path an error at read time.** Tempting, and wrong for the tool: `status` and
`compare` are what somebody runs to find out whether an old capture still counts, often against a
checkout where a path legitimately does not exist yet. A hard error there turns a reporting command
into a failure. A test binds the *committed* manifest against the *committed* tree, which is the
only pairing that has to hold.

**Deriving the paths from the layers' Rust imports.** No mechanism exists to do this, and the
mapping is a judgement — `wire.rs` measures the framing and deliberately does not list the client,
which no import graph would know.

**Leaving `shoal-core/src/server/tables/partitions` in as harmless.** It is harmless and it is a
path that has never resolved, so keeping it would mean the new test had to permit exactly one
non-resolving entry, which is a hole the next drift fits through.

## Invariants to uphold

- **Every path in `sources.json` must resolve**, and the test enforces it. A path is either watched
  or it is not there; there is no third state, because the third state is this defect.
- **A missing path can only ever widen a verdict to `unaffected`, never narrow one to `fresh`.**
  That property is what made this defect survivable rather than catastrophic, and it is a property
  of `stale.rs` checking the commit hash *first*. Do not reorder that check.
- **Adding a bench to a layer means adding it here in the same change.** F10 did this for `wire.rs`,
  F24 for `routing.rs`; the gap between them is where this item lived.
- **Changing this file moves the manifest hash on purpose.** A change to what is being measured
  should be as visible as a change to the measured code.

## Still open

Nothing about this defect. Two neighbouring things that would have narrowed it further are filed in
[TODOs](../todos.md#benchmark-coverage-the-harness-does-not-have): a `--strict-stale` mode that
treats any commit move as stale regardless of digests, giving a way to distrust this list
deliberately; and the observation that no test checks the list is *complete* — only that what it
names exists. A layer that measures a source nobody listed is still reported as unaffected by
changes to it, and that failure has the same shape and no detector.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `fingerprint::tests::every_source_the_manifest_names_exists` | a renamed or moved source silently stops being watched, and captures affected by a change are reported as unaffected by it |

## Related

- [F7](../../features/bench-runner.md) — the staleness machinery this list feeds
- [F15](../../features/client-server-split.md) — the change that moved the paths
- [F24](../../features/routing-benchmarks.md) — the bench registered in the same fix
- [F10](../../features/framing-and-protocol-evolution.md) — the precedent for registering a bench with its sources
- [TODOs](../todos.md#benchmark-coverage-the-harness-does-not-have) — `--strict-stale`, and the completeness gap this fix does not close
