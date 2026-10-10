# F70. Storage faults for a directory a test names: a torn write, a full disk and a lost device

A test can now fail one directory of a node's storage, and nothing beside it, in three ways.
**A torn write**: the write that passes a mark reaches the device only in part, is synced, and
the process ends as a crash would. **A full disk**: the directory's files may grow by a budget,
and a write that would grow them further fails with `ENOSPC`. **A lost device**: every operation
under the directory fails with `EIO`, on a handle opened before the loss too. The faults are
armed in process, through a hook added to Shoal's glommio fork that every file operation asks
first. A root-only test holds them to what a real device behind device-mapper answers.

## Context

This is a required row of [S1](../object-storage/prerequisites.md#required). It lands before
[M11](../object-storage/milestones.md#m11-step-0-the-harness-and-the-facts), whose harness
asks for faults "for a directory a test names, each tested against itself". Object storage's
failure model ([P7](../object-storage/contract.md#the-contract)) adds three faults of a device
that its node outlives: a write that tears, a device that fills, and one that stops answering.
A clause no test can violate is not checked ([S16](../object-storage/testing.md)).

Before this feature the fixture had only coarser tools:

- it could end a process at a named line, a crash point
  (`shoal-core/src/server/replication/install.rs`);
- it could fail a table's intent log write in process (`LogFault`);
- it could damage one archive record ([F44](repair.md#what-the-fixture-can-do-now));
- it could fake the free bytes a node reports (`FREE_BYTES`);
- it could take a directory read-only by its permissions, which is how
  [Resolved #156](../appendix/resolved/wal-failure-stops-the-node.md)'s test filled a WAL.

[C15](../distributed/open-issues.md#filed-as-unbuilt) filed disk-full and torn archive writes as
unbuilt.

## What it does

### The hook, in glommio

`glommio::io::set_io_hook` sets one `IoHook` for the process. From then on these operations ask
it first, with the path of the file they are about and what they are about to do (`IoOp::Open`,
`Read`, `Write`, `Sync` or `Meta`):

- every open of a `DmaFile`, a `BufferedFile` or a `Directory`;
- every read and write of a `DmaFile` or a `BufferedFile`;
- every `fdatasync`, and every directory sync;
- every truncate and allocation;
- every `io::rename` and `io::remove`.

The hook answers with an `IoVerdict`:

- `Proceed`;
- `Fail(io::Error)`;
- `Tear(n)`: write only the first `n` bytes, rounded down to the file's alignment for direct
  I/O, sync them, and call `IoHook::torn`.

Every `DmaStreamWriter` writes and syncs through `DmaFile`, so archive writes are covered with
no change to Shoal. A refusal suspends once before it is returned, the way a real operation's
error comes back from the ring. Until a hook is set, an operation's only cost is one atomic load.

The hook is two commits on the fork's `ZeroCopyDmaStreamWriter` branch: `0308937` adds it, and
`f4643f7` makes a refusal suspend once before it is returned.

### The faults, in Shoal

`shoal::server::faults` is the policy. `arm(dir, fault)` sets the hook the first time it is
called and records a `Fault` for a directory: `Torn { after_bytes }`, `Full { budget }` or
`Lost`. `clear(dir)` lifts it. A path belongs to the deepest armed directory that holds it,
matched by component, so `/a/b` never covers `/a/bc`.

- **Torn** counts the bytes written under the directory since it was armed. The one write that
  crosses `after_bytes` keeps what is left below the mark, which is synced, and the process exits
  with 137, as a kill and a crash point do. Nothing after it is torn.
- **Full** charges what files *grow* by, not every byte written. Each file's size is seeded from
  disk the first time it is written. An intent log that rewrites its partial tail block grows by
  nothing. A write that would take the growth past the budget fails with `ENOSPC` and writes
  nothing. A rewrite of space a file already holds goes through. The directory's free bytes read
  as what is left of the budget (`capacity::free_bytes` asks `faults::free_bytes_under`), so a
  node sees its disk fill and sheds under `replication.append_reserve` before it fails, as it
  would on a real disk.
- **Lost** fails every operation with `EIO`, a handle opened before the loss included. The few
  small files a start reads and writes through `std::fs` - the storage marker, its lock and the
  hosting table - ask `faults::guard` themselves.

### In the fixture

`FAULT_DIR <dir> torn <bytes> | full <bytes> | lost | clear` arms a fault in a child, the
directory relative to that child's storage (`.` for all of it, `wal` for its WAL,
`Note/archives` for one table's archives). `Node::exit_code` reads the code a child ended with.

## Design choices

**In process, through glommio, rather than below the filesystem.** Every byte Shoal stores goes
through a glommio file, and glommio submits through `io_uring`, so an `LD_PRELOAD` shim on
`write` sees none of it. The kernel's own fault targets (`dm-error`, `dm-flakey`, a loop device
that fills) need root and a device a test owns. The suite runs without either. The hook needs
neither, reaches a handle opened before the fault, and runs at the fixture's six threads.

**The policy is Shoal's, the hook is glommio's.** The fork knows nothing about torn writes or
budgets. It asks a question and does what it is told. The table of faults, the growth
accounting and the exit live beside the crash points they resemble.

**Growth, not bytes.** A full disk refuses allocation, not traffic. A budget counted in bytes
written would fail an intent log for rewriting its own tail block, which a real disk never does.
The device test holds this to a real filesystem: the same number of mebibytes land before the
refusal, and a rewrite afterwards lands on both.

**A tear ends the process.** A torn write the process survives is not a torn write: whoever
issued it would retry or report it. What P7 names is a crash in the middle of a write, so the
hook exits with 137 once the prefix is synced. The module's own tests turn the exit off.

**A refusal suspends once.** The device test found this: a write refused at once made
`DmaStreamWriter`'s flush task take the writer's state while its caller still held it, a
`RefCell already borrowed` panic no real device could cause.

## Alternatives rejected

**dm-flakey and dm-error for the suite.** They are exact, but need root, a loop device for each
child, and a teardown that survives a test that panics. The suite would stop running anywhere
but the lab. They are kept for the one test that checks the in-process faults against them.

**A `GuardedWriter` around every `DmaStreamWriter` in Shoal**, with no change to glommio. About
fifty-five guard calls, a path carried through the compactor and the rehome. Even then a tear
would land at `write_all`, not where the stream writes the device.

**Comparing a server's reaction on a real device with its reaction in process.** That was the
first form of the device test, and it compared accidents. On a 64 MiB filesystem the intent log
met the full disk first. Under the in-process budget the compactor did. That first form is how
[item 91](../appendix/known-issues.md#91-a-compaction-that-fails-after-writing-ends-the-compactor)'s
remainder was reproduced. Operations are what the fault is, so operations are what the test
compares.

**A torn write that fails rather than exits.** That is a write error, which `Lost` already
covers. It is what the module's own tests use, and nothing else.

## Limitations

- **The torn device fault is not compared with the kernel's.** `dm-flakey`'s `drop_writes`
  drops acknowledged writes, which is a lying device and not a tear, and a tear that stops at a
  chosen byte needs `dm-log-writes` replayed to a mark. The in-process tear is held to its own
  description by `torn_cuts_one_write_once`, and to a node's recovery by the fixture.
- **A `std::fs` site other than the marker, the lock and the hosting table is not faulted.**
  Small sidecars, snapshot install markers, retry and floor files, and directory listings are
  read and written through `std::fs` and do not ask. A lost device fails every byte of data, but
  not every name.
- **One hook a process, set once.** A fault is lifted by `clear`, never by replacing the hook.
- **A child has one storage directory.** S16 asks for several directories as devices; until M14
  a fault names a subdirectory (`wal`, `Note/archives`) instead.
- **A full fault charges growth from its arming, not from the directory's size.** It is a
  budget, not a capacity, and a directory's existing files count as already paid for.
- **`read_many` and `ImmutableFile` are not hooked.** Shoal uses neither.

## Invariants to uphold

- **Every storage write, read and sync goes through a glommio file**, or asks `faults::guard`. A
  storage path written another way is one no fault reaches.
- **A hooked refusal suspends once before it returns.** A caller may rely on an operation
  suspending, as `DmaStreamWriter`'s flush task does.
- **A tear writes whole blocks for a direct file, syncs them, then ends the process.** What
  reaches the device is a prefix, durable, and nothing after it is written.
- **A full disk charges growth.** A rewrite of held space is never refused.
- **Nothing is armed unless a test arms it**, and the hook is not even set until then.

## Performance

None measured. A process that arms no fault never sets the hook, so a node pays one relaxed
atomic load an operation (`OnceLock::get`). Nothing on a node's path changed otherwise.

## Tests

| Test | Where | What breaks if reverted |
| --- | --- | --- |
| `a_hook_fails_and_tears_only_its_directory` | glommio, `src/io/hook.rs` | A hook is not asked, a tear is not cut to whole blocks, or a fault reaches a file beside its directory |
| `a_fault_matches_its_directory_and_nothing_beside_it` | `shoal-core/src/server/faults.rs` | A fault covers a sibling by name prefix, or the deepest fault does not win, or a spelled fault does not parse |
| `full_counts_growth_since_arming` | `shoal-core/src/server/faults.rs` | A full disk charges a rewrite, lets growth past its budget, or reports the wrong free bytes |
| `lost_fails_every_operation` | `shoal-core/src/server/faults.rs` | A lost device lets a write, read, sync, open or remove through, on a handle opened before the loss |
| `torn_cuts_one_write_once` | `shoal-core/src/server/faults.rs` | A tear keeps the wrong prefix, tears a second write, or keeps part of a block on a direct file |
| `device_faults_do_what_they_say` | `shoal/tests/cluster_fixture.rs` | A full disk does not make a node shed and serve, a lost WAL does not stop a node, or a torn WAL write does not end it with 137 and recover on restart |
| `device_faults_on_a_real_device_match` | `shoal/tests/kernel_faults.rs`, ignored, root | A full disk or a lost device armed in process answers an operation otherwise than a real device behind device-mapper does |

`device_faults_on_a_real_device_match` passed on europa and on titan (2026-10-03). There a
64 MiB ext4 filesystem took 54 MiB of direct writes and refused the next with `ENOSPC`, the
fault armed in process with the same budget did the same, and both let a rewrite through. A
`dm-error` table and the lost fault both answered a write, a sync and a direct read with `EIO`.

## Related

[F44](repair.md), whose archive faults damage a record rather than a device.
[Resolved #156](../appendix/resolved/wal-failure-stops-the-node.md), the node that stops when its
WAL cannot be written and sheds under its reserve, which the full and lost faults now cause the
way a disk does. [Item 91](../appendix/known-issues.md#91-a-compaction-that-fails-after-writing-ends-the-compactor),
whose remainder the full fault reproduced. [F69](driver-operation-kinds.md), the other M11
prerequisite. [S16](../object-storage/testing.md), which asks for these faults and two more, an
emptied device and a flipped bit, that this does not build.
