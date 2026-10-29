# Milestones

**Nothing below is delivered, and no gate has been set.** This page is an order of work, drawn before any
[spike](spikes.md) has reported, so that its dependencies can be argued with. It becomes a plan gate by gate:
each is set once the questions it waits on are decided on [W41](contract.md#questions-to-answer), with its
acceptance tests named on their owning pages and its evidence stated before its work starts.

M35–M39 continue the volumes' [M29–M34](../volumes/milestones.md) and are stable identifiers. Acceptance tests
live on their owning W pages, indexed by [W41](contract.md#the-acceptance-index). **M38 waits on the volumes'
M32**, since a file's bytes travel the lane; M35 to M37 can run beside the volumes' gates.

**Every gate keeps the cluster's rules.** No gate adds a consensus protocol, a coordination service or a clock
that decides anything ([R7](../../distributed/overview.md#what-is-asked-of-it)); no gate is passed by weakening
a clause of [the contract](contract.md#the-contract); and no gate a mount can reach is passed without
[W37](isolation.md#the-budget)'s budget measured beside it.

## How a gate is written

| Line | Holds |
| --- | --- |
| **Closed before it** | The questions of [W41](contract.md#questions-to-answer) decided, with their evidence on the record |
| **Lands first** | The required rows of [W28](prerequisites.md) placed at its start |
| **Delivers** | What exists afterwards |
| **Acceptance** | The owning pages' rows that name this gate |
| **Evidence/exit** | What is measured or demonstrated beyond tests passing, every number labelled |
| *Not at this gate* | What a reader might expect here and will not find |

## The gates at a glance

| Gate | In a line | Closed before it | Lands first |
| --- | --- | --- | --- |
| Before M35 | The contract agreed and the model passing | P36–P44; X31 | — |
| M35 | The engine's capabilities: the one-tablet command, supplied hashes, byte sort keys | — | The command, partition keys with a supplied hash, sort keys of a hash and bytes |
| M36 | The namespace on one node | Q54, Q55, Q58's default, Q61 | The namespace queue and limits, the resident cap, grants and bench kinds; the volumes' shared rows |
| M37 | Across groups: records, renames, `rmdir`, links, mounts | Q53, Q57 | Client registrations, server pushes |
| M38 | File data on the lane | Q56; the volumes' M32 | — |
| M39 | The FUSE mount | Q59; Q58 asked again | — |

## The gates

### Before M35: the contract agreed

**Not settled.** It is settled when P36–P44 on [W41](contract.md#the-contract) are agreed, each beside the
violation that breaks it and the tests that own it, and when [X31](spikes.md#x31-the-namespaces-model)'s model
passes its safe policy and breaks each unsafe setting's property. No type, frame or row format of a file tree
is added at this gate.

### M35. The engine's capabilities

**Closed before it.** The contract.

**Lands first.** A command of several writes in one tablet; partition keys with a supplied hash; sort keys of a
hash and bytes ([W28](prerequisites.md#required-the-filesystems)).

**Delivers.**
- The namespace command: conditions on any row of its tablet, all or nothing, decided once at apply, refused
  frames excluded from compaction and applied writes folded plain ([W30](atomic-commands.md)).
- Supplied hashes for a generated table's keys, and sort keys of `(u64, bytes)`.
- The namespace model's schedules as acceptance tests.

**Acceptance.** The M35 rows of [W30](atomic-commands.md#acceptance-tests), [W31](namespace.md#acceptance-tests),
[W40](model.md#acceptance-tests) and W41.

**Evidence/exit.** Every F68 and F61 test unchanged; the workload grid's reference cell before and after, on
the lab, within its intervals: a table that uses none of the capabilities runs as it did.

*Not at this gate*: a file tree. This gate can run beside the volumes' M30 to M34.

### M36. The namespace on one node

**Closed before it.** Q54, co-location, by X32; Q55, a part's bound, by X33; Q58's default; Q61, who enforces mode
bits.

**Lands first.** The namespace queue and its limits; the cap on a table's resident rows; grants for file trees and
the bench's filesystem kinds ([W28](prerequisites.md)); and the volumes' shared rows, bindings and the pool's
write policy above all.

**Delivers.**
- `FileTree<M>`, its generated tables, minted keys and inode numbers ([W31](namespace.md)).
- A directory's two parts, its parts and their splits, and `readdir` by cookies.
- The operations in one tablet through the client's handle, on a standalone node ([W32](one-tablet-operations.md)).
- The namespace queue, admission, limits and resident cap ([W37](isolation.md)).

**Acceptance.** The M36 rows of W31, [W32](one-tablet-operations.md#acceptance-tests), [W37](isolation.md#acceptance-tests)
and [W39](performance.md#acceptance-tests).

**Evidence/exit.** X32's figures retaken on what was built; the budget beside a metadata storm on a standalone node
on the lab; a `find` over a tree past its cap evicting no other table's rows.

*Not at this gate*: an operation across groups, a mount, a file's striped bytes, FUSE.

### M37. Across groups

**Closed before it.** Q53, the cross-group protocol, confirmed on what M36 built; Q57, held inodes, by X31 and X32.

**Lands first.** Client registrations; server pushes beyond topology ([W28](prerequisites.md)).

**Delivers.**
- Operation records, pending entries, drivers; `rmdir` with its mark; renames between directories and the rename
  row; hard links and the unlink of one ([W33](cross-group-operations.md)).
- Mounts: registered, ended by commits, barred in every group ([W35](mounts-and-locks.md#a-mount)).
- The file trees' wire version, activated as every addition since [F48](../../features/rolling-compatibility.md)
  is, and `trees:` grantable only behind it.

**Acceptance.** The M37 rows of W32, [W33](cross-group-operations.md#acceptance-tests),
[W35](mounts-and-locks.md#acceptance-tests) and W37.

**Evidence/exit.** Coordinators killed at every step of every operation across groups in the fixture, P36 to P38
checked by the oracle; the budget on the lab's three nodes with three mounts' clients working.

*Not at this gate*: a file's striped bytes, FUSE.

### M38. File data on the lane

**Closed before it.** Q56, call-backs, by X35; the volumes' [M32](../volumes/milestones.md#m32-placement-under-a-writer).

**Delivers.**
- Write leases, taken at a create or an open, called back and revoked ([W34](file-data.md#the-write-lease)).
- Inline files and their flip to stripes; close and fsync; close-to-open for every opener.
- Truncate, appends, holes; held inodes ([W35](mounts-and-locks.md#held-inodes)).

**Acceptance.** The M38 rows of [W34](file-data.md#acceptance-tests), W35 and W39.

**Evidence/exit.** P39 to P41 under mounts killed and call-backs revoked in the fixture; X35's figures retaken; a
file's 4 KiB write in place beside a volume's on the lab.

*Not at this gate*: FUSE, locks.

### M39. The FUSE mount

**Closed before it.** Q59, the transport, by X34; Q58 asked again, with what M36 to M38 measured.

**Delivers.**
- The daemon in the schema's program, `shoaladm tree mount` ([W36](fuse.md)).
- FUSE over io_uring where the host's kernel has it; the kernel's caches under the write-lease rule.
- `flock` and byte-range locks ([W35](mounts-and-locks.md#locks)).

**Acceptance.** The M39 rows of W35, [W36](fuse.md#acceptance-tests) and W39.

**Evidence/exit.** `pjdfstest` on a mount, every failure on W41's list; an `untar`, a parallel build, a `find` and a
`du` through mounts on the lab within the budget; mdtest beside them as a cross-check.

*Not at this gate*: an NFS gateway, read leases, snapshots.

## Optional, any time

None of these is needed by a gate, and each can be added later without changing anything built:

- **Read leases**, for caches exact until called back ([W35](mounts-and-locks.md#what-a-mount-caches)).
- **Creates done ahead by a mount** under a lease on its directory, as CephFS's kernel client does
  ([W29](prior-art.md#cephfs)).
- **Spreading a hot directory over tablets** ([W31](namespace.md#directory-parts)).
- **A change feed of the namespace**, as SeaweedFS's filer offers ([W29](prior-art.md#seaweedfs)).
- **Write leases on a byte range**, for many writers into one file ([W34](file-data.md#alternatives-rejected)).
- **An NFS gateway and virtiofs** ([W36](fuse.md#other-ways-onto-a-host)).
- **Snapshots of a tree, quotas, ACLs.**
- **The namespace on executors of its own**, should Q58 be answered so ([W37](isolation.md#executors-of-its-own)).

## The order is a claim

- **The model before any code**, as for the volumes' lane and X1's stripes.
- **The engine before the namespace**: its capabilities proven harmless to every other table first.
- **One tablet before many**: the common operations, and the layout that makes them one command, before the
  protocol for the few.
- **The namespace before the bytes**, and **the volumes' lane before a file's**: a file's bytes are the lane's.
- **The mount last**, the engine before the wire, as in every plan here.

M35 to M37 can run beside the volumes' gates; only M38 waits on them.

## Related

[W28](prerequisites.md) for the rows the gates begin with; [Spikes](spikes.md) for what has to report first;
[W41](contract.md) for the questions and the record; [What's left to do](whats-left-todo.md); [the volumes'
milestones](../volumes/milestones.md), which this follows.
