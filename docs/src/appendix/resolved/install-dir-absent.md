# 153. A restore on a shard that had received no stream killed the shard

## Symptom

A restore of the lab's backup into a fresh cluster reported `Done`. One of the 36 groups had
failed, and 66,191 movies, one group's share, could not be read back. On hyperion, shard 1 died
in the middle of the restore and the node restarted:

```text
a snapshot could not be installed group=8b96c24baf8d5470 error="cleaning up the install: No such
file or directory (os error 2), op: Opening directory path: Some(\"/optane/shoal/wal/Shard-1/install\")"
a shard died shard=1 error="…tablet group 8b96c24baf8d5470 could not be built: building the group:
when Write Snapshot(…)…"
```

Its restart reset the peer connection of another group, whose restore was quarantining that
member at that moment. That group failed: `did not take the quarantine: ConnectionReset`.

## Cause

A snapshot install ends by removing its pending marker and partial file from the shard's install
directory (`wal/Shard-N/install`), with the directory synced between the two. The marker and the
part were each removed only if they existed. The directory was synced whether it existed or not.

The directory is created lazily, when a streamed snapshot's partial file is first written. A
restore installs the group leader's own copy from the file the leader built itself, and writes
nothing into the install directory. On a shard that had not yet received a stream, that
directory did not exist, and the sync failed. A failed install fails the group's rebuild, and a
group that cannot be built kills its shard.

## Evidence

**Found on the lab**, in hyperion's journal above. **Reproduced** by
`cleaning_up_an_install_needs_no_install_directory` (`shoal-core`, `shard/snapshots.rs`) with the
fix's guard taken out:

```text
an install that wrote nothing cleans up: Custom { kind: Other, error: "No such file or directory (os
error 2), op: Opening directory path: Some(\"/tmp/.tmpqaNA90/install\") with fd: None" }
```

## The fix

The clean-up (`remove_install_files`) syncs the install directory only if it exists. An install
that wrote nothing there has nothing to make durable.

## Alternatives rejected

- **Create the install directory when the shard starts.** It hides the case rather than handling
  it. The rule that matters is that a clean-up of files that were never written succeeds.
- **Stop a failed install from killing the shard.** A group that cannot be built is a fault the
  shard should not serve through. The fault here was the clean-up's, not the install's.

## Invariants to uphold

- **Clean-up tolerates everything it would remove being absent.** Marker, part and directory alike.

## Still open

- ~~A restore whose group fails cannot be retried (known issue 155).~~ Resolved: `RetryRestore`
  drives a finished restore's failed groups again ([Resolved #155](restore-retry.md)).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `cleaning_up_an_install_needs_no_install_directory` (`shoal-core`, `shard/snapshots.rs`) | An install that wrote nothing to an absent install directory fails its clean-up |
| Back up, destroy, restore ([cluster testing](../../cluster-testing/correctness.md#back-up-destroy-and-restore)) | A restore kills a shard and loses a group |

## Related

- [F49](../../features/backup-and-recovery.md), backup and restore.
- [Resolved #154](admin-hides-failed-groups.md), why the failure went unseen.
