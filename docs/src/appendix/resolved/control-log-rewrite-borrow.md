# 245. The control log's rewrite held the log borrowed while replication read it

The control group's log store rewrites its file from memory when openraft purges or truncates it
([F37](../../features/node-identity-control-plane.md)). The rewrite held the log's state borrowed
across its file writes, on the reasoning that openraft drives the log store from one task. But a
leader's replication streams read the log through readers that share the same state, on the same
executor, and one that read while a purge rewrote the file panicked the control thread. Found by
[F88](../../features/recovery-and-moves.md)'s lab run, whose moves took the control log to its
first purge, and filed and fixed in one change.

## Symptom

On `shoaladm bench`'s copy of the lab's three nodes, europa's node logged, in every arm whose
placement group moves had committed about five thousand control entries:

```text
INFO openraft::engine::handler::log_handler: purge log, last_purged: None, purge_upto: T1-Nea7a6d12-...3999
thread 'shoal-control-1' (1579317) panicked at shoal-core/src/server/control/store.rs:397:32:
RefCell already mutably borrowed
```

Seven times on 2026-10-14, between 06:08 and 08:42, on every build of F88 and its fixes the lab
ran that day. The node's control thread was gone and the rest of the node went on: its shards served
clients, but nothing on it proposed, answered an admin read or a peer's control lane, or installed a
map. The new leader called the member silent and could not send it a snapshot (`the control rpc
timed out`, fifty times a minute), and the moves its shards led stopped: in the last such arm,
782 moves began across the cluster in the first minute, the other members' went on to 08:47, and
none began in the four minutes before the arm's time ran out.

## Cause

`ControlLog::rewrite` took `self.inner.borrow_mut()` and awaited the close of the old file, the
staged write of the new one and its reopen with that borrow held. `try_get_log_entries`, which a
replication stream calls to send entries to a follower, takes `self.inner.borrow()`. A stream
scheduled during one of the rewrite's awaits found the log mutably borrowed. The struct's own
comment says openraft "asks for a reader and a store and drives them from different tasks"; the
rewrite's comment said nothing else reached the log in between.

A purge comes at openraft's snapshot policy, after five thousand entries. Before F88 a control
log reached one only through five thousand membership, health, plan and device commits; a failed
device's moves commit two each, an admission and a release, so two to three thousand moves reach it
in minutes.

## Evidence

**Reproduced** by `a_purge_leaves_the_log_readable_while_it_rewrites`, written first and run on
europa against `895ca2b`: fifty entries appended, a reader reading the half the purge keeps on a
task of its own, and a purge of the other half.

```text
thread 'server::control::store::tests::a_purge_leaves_the_log_readable_while_it_rewrites' panicked at
  shoal-core/src/server/control/store.rs:397:32:
RefCell already mutably borrowed
test result: FAILED. 0 passed; 1 failed
```

The lab's line. With the fix the reader reads throughout and the log is the purge's, on disk too.

## The fix

**The rewrite borrows nothing across an await.** It encodes every frame and takes the old file
under one borrow let go at once, closes, writes, renames and reopens with nothing borrowed, and
puts the new file and its offset back under another. No append can come between: openraft appends
from the task that purges. A reader reads only the entries in memory, which are already the
purge's.

## Alternatives rejected

**A copy of the log for readers.** A reader that read a snapshot of the entries would never meet
the borrow, at the cost of a copy for every stream's read; the rewrite was the one writer that held
it.

**Fewer control commits, so the log purges later.** The moves' commits are O111's to cut, and a
log that purges once a week still purges.

## Invariants to uphold

- **Nothing on the control thread holds a `RefCell` borrow across an `.await`**, the control store
  included: openraft's tasks share its log and state machine on one executor, and any of them may
  run at any await. CLAUDE.md states the rule for the control core.

## Still open

Nothing of this defect.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_purge_leaves_the_log_readable_while_it_rewrites` | `shoal-core/src/server/control/store.rs` | A replication stream reading during a purge panics the control thread |
| `control_store_passes_the_openraft_storage_suite` | `shoal-core/src/server/control/store.rs` | The rewrite leaves a log the suite's purge and truncation cases read wrong |

## Related

- [F37](../../features/node-identity-control-plane.md), which built the control store.
- [F88](../../features/recovery-and-moves.md), whose moves made the first purge reachable in
  minutes, and [Resolved #243](move-slots-on-the-map.md), found in the same runs.
