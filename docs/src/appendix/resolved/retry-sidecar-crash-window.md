# 115. A crash between the retry sidecar and the checkpoint file forgot every identity below the checkpoint

## Symptom

`lost_response_retry_returns_original_result` failed under whole-workspace runs at six threads
and passed alone. It failed in the F52 run, and it had failed once before in a run for F51. At
its last step the retried delete was **applied as new**:

```text
panicked at shoal/tests/cluster_fixture.rs:10781:5:
after a checkpoint and a restart the identity was applied as new:
Err(QueryDidNotSucceed { id: 97fd4783-…, index: 0, kind: Delete, end: true })
```

In that step the delete's entry has been checkpointed on every node and every node has been
restarted. The identity is supposed to be answered from `retries.bin`. It was answered as a
delete of nothing, which is exactly what the retry table exists to prevent. No member was
called down in that run, so this was a different failure from the one beside it
([Resolved #116](map-version-test-snapshot.md)).

## Cause

A checkpoint write (`Shard::write_checkpoint`, `server/shard/groups.rs`) wrote the retry
sidecar **in place** first and the checkpoint file second, both on a spawned task.
`Retries::seed_for` seeds a group only from a sidecar written for exactly the checkpoint the
group starts from. The sidecar's doc comment said a crash between the two writes "leaves a
sidecar ahead of its checkpoint, which the seed rule ignores". That was true, but ignoring
that sidecar was not harmless. The sidecar that had matched the checkpoint on disk had just
been overwritten, so no file described that checkpoint any more. The group opened with an
**empty** retry table. The log replay then rebuilt only the entries above the checkpoint. Every
identity applied at or below the checkpoint was forgotten, and a retry of any of them was
applied a second time.

The test opened that window for itself. `GROUPS` reports a group's in-memory checkpoint, which
moves as soon as a compaction lands (`checkpoint_durable = false`); the file follows later.
`wait_checkpoint_past` returned when the index had passed, even if the file had not landed.
The test then SIGKILLed every node. On a loaded host the two writes take longer, and the kill
could land between them.

## Evidence

**Reproduced** by `retry_table_survives_a_crash_between_sidecar_and_checkpoint`, run against
the unfixed engine before the fix was written. The test uses one node at replication factor
one, so that node answers every retry itself. It deletes a note under an identity and waits for
the checkpoint to pass the delete **on disk**. It writes another note, arms the new
`sidecar_written` crash point, and rotates and compacts until the node dies there. It then
restarts the node and retries the delete under the same identity. The test failed three runs
out of three with the workspace run's message:

```text
panicked at shoal/tests/cluster_fixture.rs:10867:5:
after a crash between the sidecar and the checkpoint the identity was applied as new:
Err(QueryDidNotSucceed { id: 9089d69a-…, index: 0, kind: Delete, end: true })
```

With the fix, the same test passes three runs out of three.

## The fix

A sidecar that describes the checkpoint on disk is never overwritten until the next checkpoint
is on disk. A checkpoint write now happens in three steps:

1. **Stage** the new sidecar as `retries.next.bin` (`Retries::stage`, which is an atomic write).
2. Write `checkpoint.json`.
3. **Promote** the staged file by renaming it over `retries.bin` (`Retries::promote`, which
   also syncs the directory).

A crash before step 2 leaves `retries.bin` describing the checkpoint on disk. A crash between
steps 2 and 3 leaves `retries.next.bin` describing it.

At open, `Retries::recover` reads both files. For each group it takes the entries from
whichever file was written for that group's checkpoint on disk. If it found a staged file, it
settles the result as `retries.bin` and removes the staged file before any group starts. The
shard's open and both rehome steps now read the sidecar through `recover`. `Retries::write`,
which a rehome uses to write a whole sidecar, also removes any staged file.

The same change adds two small things. `GroupReport` gains `checkpoint_durable`, the flag the
shard already kept. `wait_checkpoint_past` now waits for that flag, so
`lost_response_retry_returns_original_result` restarts the nodes after the checkpoint file has
landed. The test then exercises the sidecar, as its docs say, and not a replay of the log.

## Alternatives rejected

**Only make the test wait for a durable checkpoint.** The test would stop failing, but the defect
it found would stay. A crash between the two writes is exactly what a retry table has to
survive, and a kill does not wait for a test's checks.

**Seed from a sidecar that is ahead, keeping only entries at or below the checkpoint.** Those
entries are what the newer table still remembered. Anything evicted between the two
checkpoints would be missing, so the seeded table would not be the one the checkpoint
described. Replaying the log would then evict differently from the first time.

**Put the retry table inside `checkpoint.json`.** That gives one atomic file. But the checkpoint
file is JSON, which operators and tests read, and the table can hold four thousand sixteen-byte
identities per group. That is why the sidecar is postcard in the first place.

**Name each sidecar by a checkpoint generation and point to it from the checkpoint file.**
That also works. But it changes the checkpoint file's format and needs old generations
collected. A staged name and a rename keep both formats and every reader as they were.

## Invariants to uphold

- **At every point of a checkpoint write, some file on disk holds a sidecar for exactly the
  checkpoint on disk.** Nothing writes `retries.bin` except as a promotion after the checkpoint
  file landed, or as a whole already resolved against the checkpoint (`recover` and rehome).
- **A staged sidecar found at open is settled before any group starts.** Otherwise the next
  checkpoint write would stage over the only file describing the checkpoint on disk.
- **A test that kills a node to prove what the sidecar held waits for `checkpoint_durable`,**
  not for the checkpoint index alone.
- **`sidecar_written` is not an install point.** It is in `CrashPoint::NAMED` but not in
  `CrashPoint::ALL`, which remains F43's install order.

## Still open

- Nothing in the sidecar. The rehome's copy step still writes the destination's sidecar before
  its checkpoint. It needs no staging there: every group already on the destination keeps the
  entries it had, and a crash between the two leaves the moved groups to the step's redo.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `retry_table_survives_a_crash_between_sidecar_and_checkpoint` | `shoal/tests/cluster_fixture.rs` | The retried delete is applied as new after a crash at `sidecar_written` |
| `a_stopped_checkpoint_write_leaves_a_sidecar_for_the_checkpoint_on_disk` | `shoal-core/src/server/wal/tests.rs` | `recover` seeds nothing when stopped before the checkpoint, or ignores the staged file when stopped after it |
| `lost_response_retry_returns_original_result` | `shoal/tests/cluster_fixture.rs` | Fails under load when the restarts land between the two writes |

## Related

[F42. Primary failover](../../features/primary-failover.md), which introduced the sidecar;
[F44. Repair](../../features/repair.md), which checksums it; [F47. Local rehome](../../features/local-rehome.md),
which moves it; [Resolved #116](map-version-test-snapshot.md), the other failure in the same run.
