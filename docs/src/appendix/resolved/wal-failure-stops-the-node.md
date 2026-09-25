# 156. A node whose WAL could not be written stayed up, serving nothing

*Partly resolved.* The node now stops, and a restart recovers it. What remains open is refusing
appends before the disk is full, so that it never gets that far
([known issues](../known-issues.md#156-a-full-disk-stops-every-group-on-a-node-until-it-is-restarted)).

## Symptom

On the lab hyperion's disk filled during a load
([cluster testing](../../cluster-testing/correctness.md#fill-a-nodes-disk)). Its WAL writes failed,
openraft stopped every group core on the node, and the node stayed up. Every copy was dead,
and every write through the node answered `Unavailable … when Write Log` until someone restarted
it. A loader connected to every member stopped at those writes.

## Cause

A failed WAL batch (`fail_batch`) records its error in the WAL for good: every later `stage` is
refused with it, so every group on the shard, not only the ones whose append failed, can take
nothing more. openraft stops each group's core on the error, and the shard's probe marks each copy
dead "until the process restarts". Nothing restarted it. The shard ran on, with a WAL that would
take nothing and cores that answered nothing.

Keeping it up could not have recovered it either. After a failed write or `fdatasync`, what the
WAL holds past its durable watermark is not known: a failed `fdatasync` in particular does not say
which pages reached the device, which is why PostgreSQL stops on one ("fsyncgate"). The state that
can be trusted is the state a restart reads back.

## Evidence

**Found on the lab.** **Reproduced** by `a_node_whose_wal_cannot_be_written_stops`
(`shoal/tests/cluster_fixture.rs`): three nodes with 64 KiB segments, node two's WAL directory
made read-only so its next segment cannot be created, which fails a batch the way a full disk
does, and wide writes through node zero. Against the unfixed tree:

```text
node two's wal could not be written for 2890 writes and the node is still up
```

With the fix: `node two stopped after 20 writes: Some("exited")`, and node zero's writes went on.

**On the lab**, with the fix deployed and hyperion's storage on a 2 GiB filesystem that the load
filled: hyperion exited 115 ms after its first failed batch, failed each start while the disk was
full (systemd's restart count 3 to 12 in a minute), and started on its own at the next restart
after the filesystem grew. Its copy then read back whole at `One`. See
[fill a node's disk](../../cluster-testing/correctness.md#fill-a-nodes-disk).

The first exit on the lab went through a different write from the fixture's: shard 3's group
checkpoint, which already ended its shard on failure, got there a few milliseconds before the
WAL check did. Both paths end in the same place. The WAL check is still what stops a node whose
only failed write is a WAL batch, which is what the fixture pins.

## The fix

On every segment sweep, after the core probe, the shard checks its WAL's `failure()`. A WAL that
failed ends the shard with `ServerError::ShardFailed` naming the error, and a node whose shard
fails exits. A supervisor restarts it, as the lab's `Restart=on-failure` does. A restart reads
back what is durable and catches up from its peers. If the disk is still full, it stops again and
says why.

## Alternatives rejected

- **Clear the error and carry on.** The WAL's tail past its watermark is unknown after a failed
  write or sync, and appending after it risks exactly the silent loss fsyncgate was.
- **Restart only the dead group cores.** Every group on the shard shares the failed WAL.
- **Refuse writes while leaving the node up.** It keeps a member in the placement that holds
  nothing new, and takes someone reading the logs to notice.

## Invariants to uphold

- **A shard never serves on after its WAL failed.**
- **Recovery from a failed WAL is a restart**, which reads back only what is durable.

## Still open

- **Nothing refuses appends before the disk is full.** A node stops, restarts and stops again
  until space returns. A reserve below which appends are shed would keep it serving reads and
  leading nothing. That is the remainder of item 156.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_node_whose_wal_cannot_be_written_stops` (`shoal/tests/cluster_fixture.rs`) | A node whose WAL cannot be written stays up, taking nothing |
| Fill a node's disk ([cluster testing](../../cluster-testing/correctness.md#fill-a-nodes-disk)) | The node lingers as a member whose copies are dead |

## Related

- [Resolved #151](purge-ahead-of-its-marker.md), a WAL durability rule of the same kind.
