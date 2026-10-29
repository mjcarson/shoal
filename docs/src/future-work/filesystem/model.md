# W40. The namespace's model

## Context

The namespace protocol is the filesystem's own: commands in one tablet, operation records decided by a
commit that a driver can race, pending entries readers resolve, a rename row against cycles, a mark
against an `rmdir`'s race, write leases called back and revoked, mounts barred before their locks go.
Each rule was found necessary by reading ([W30](atomic-commands.md) to [W35](mounts-and-locks.md)); none
has yet been held to every interleaving it allows. The volumes' lane is modelled first for the same
reason ([volumes' W26](../volumes/model.md)), as X1 modelled the stripe protocol before M11
([X1](../../object-storage/stripe-model.md)). This page is what the namespace's model must contain and
check, so that [X31](spikes.md#x31-the-namespaces-model) is a spike with its answer stated in advance.

## What exists today

`shoal-model` and its rules ([volumes' W26](../volumes/model.md#what-exists-today)): `serde` and `serde_json`
alone, every saved file replaying for ever, a search compared block by block. A stripe model and, once the
volumes plan has built it, a lane model beside the tablet model.

## The design

### Where it lives

`shoal-model/src/namespace/`, its schedules under `shoal-model/schedules/namespace/`, an example
`regenerate_namespace_schedules` and a search `namespace_search`, under the crate's rules.

### What it models

| Actor | State | What it does |
| --- | --- | --- |
| Tablets | Each a linearizable state of rows: entries, inode rows, content rows, records, the rename row, locks, bars; a tablet's group commits commands all or nothing | Apply commands, judge every condition first |
| Coordinators | An operation in flight, its record if any | Build commands, write records and pending changes, decide, finalize |
| Drivers | Records past a grace | Abort by a conditional commit; finalize decided records |
| Mounts | Their caches, their write leases, their locks, their open files | Operations, opens, closes, writes, call-backs, locks; crash |
| The network | Messages | Delay, drop, reorder, duplicate, partition |

**Names and bytes are labels**: a file's content is its last close's label, and every check is about which
names exist, what they lead to, and what a reader saw.

### What it checks

Each of [W41](contract.md#the-contract)'s properties as an oracle over a history:

| Property | The oracle |
| --- | --- |
| P36 | Every operation is seen whole or not at all by every later operation; a renamed name at one place, never both or neither |
| P37 | Whenever no record is open, the namespace is a tree from its root with no cycle, every entry reachable, and every link count equal to its entries |
| P38 | No directory is removed with entries, and none is created into once its `rmdir` began |
| P39 | An open after a completed close or fsync sees that close's size and label |
| P40 | At most one mount holds a file's write lease; a revoked holder's writes are refused |
| P41 | No inode a live mount holds open is reclaimed |
| P42 | A listing resumed across a part's split returns every entry live throughout exactly once |
| P43 | A lookup answers the name asked, never another with its hash |

A progress check beside them: with the network healed and no crash for a bound, every record is decided and
finalized, every call-back answered or revoked, and every ended mount's locks released.

### The settings

**The safe policy** is these pages' rules as written. **Every rule is also a setting that turns it off**, each
with a saved schedule that breaks a property when it is off:

| Unsafe setting | The rule it turns off | The property it should break |
| --- | --- | --- |
| `group_scoped_command` | A command confined to one tablet, replaced by one of a group | P36, after a move deals the command's tablets to two groups |
| `rejudge_at_compaction` | A command decided once at apply | P36: a fold that cannot see another partition's row brings back a refused write |
| `two_commands_no_record` | Operation records, replaced by two one-tablet commands in a row | P37: half a rename after a crash |
| `abort_by_time_alone` | A driver's abort as a conditional commit | P36: a coordinator's late commit and a driver's abort both taken |
| `reader_ignores_pending` | Pending entries resolved by their record | P36: a name at both places |
| `record_deleted_early` | A record deleted only after every participant is finalized | P37: a pending change with no record to decide it |
| `no_rename_row` | The rename row for directories between directories | P37: a cycle from two renames |
| `rmdir_without_mark` | `rmdir`'s removing mark | P38: a file created into a removed directory |
| `locks_before_bar` | A mount barred before its locks are released | P40's twin for locks: a dead mount's lock taken again by its late command |
| `revoke_without_bar` | A forced revoke raising the writer number | P40 |
| `reclaim_without_asking` | Every live mount asked before an unlinked inode is reclaimed | P41 |
| `cookie_by_position` | Cookies as hashes | P42: an entry skipped or repeated across a split |

If a setting's schedule breaks nothing, the rule it turns off is not needed and its page says so; if the safe
policy breaks a property, the design changes before M35.

### The schedules that must exist

Beside each setting's: a coordinator crashed at every step of a rename between directories; a driver's abort
racing a coordinator's commit; renames of `a` into `b/x` and `b` into `a/y` at once; an `rmdir` racing a
create; a `link` and an unlink of a remote entry with crashes; a split of a part during a listing; a mount
ended while it holds write leases and locks; a call-back racing a forced revoke; a file unlinked by one mount
while another reads it.

### The search

`namespace_search` runs the safe policy and every setting at three layouts, a tree in one tablet, a tree over
five tablets on three groups, and a deep tree with renames between directories in many tablets, for at least
as many seeds as X1 ran, on europa and again on titan and hyperion from a `znver1` build, and `report` compares
the records block by block.

## Alternatives rejected

**Building the protocol and testing it in the fixture alone**, which finds what a run happens to reach.

**A model of the whole POSIX interface**: the model is of what can break the namespace's properties, not of
every call; pjdfstest holds the rest on a real mount ([W36](fuse.md#acceptance-tests)).

## What it costs

A week or two of the model ([X31](spikes.md#x31-the-namespaces-model)); its schedules kept as acceptance tests.

## What it breaks

Nothing.

## Invariants to uphold

- **The model names no shoal crate.**
- **Every rule of the protocol that is a setting has a saved schedule** that breaks a property when it is off.
- **A rule changed in the engine is changed in the model first.**

## Prerequisites

None.

## How it would be measured

By X31 itself; its records go beside X1's and the lane's.

## Acceptance tests

| Test | Gate | Proves |
| --- | --- | --- |
| `every_namespace_schedule_replays_to_its_verdict` | M35 | Every saved schedule under `schedules/namespace/` gives the verdict it was saved with |
| `the_safe_namespace_policy_breaks_no_property` | M35 | The safe policy passes every saved schedule and a generated block of seeds |
| `every_namespace_rule_is_needed` | M35 | Each unsafe setting's saved schedule breaks the property it names |

## Related

[The volumes' W26](../volumes/model.md) and [X1](../../object-storage/stripe-model.md), the models this follows;
[W41](contract.md) for the properties; [X31](spikes.md#x31-the-namespaces-model) for the spike that builds it.
