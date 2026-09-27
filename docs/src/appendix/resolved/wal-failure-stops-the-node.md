# 156. A node whose WAL could not be written stayed up, serving nothing

*Resolved in two parts.* The node now stops, and a restart recovers it. ~~What remains open is
refusing appends before the disk is full, so that it never gets that far.~~ Since
[section 11](../../cluster-testing/correctness.md#11-overload-silence-and-a-nearly-full-disk) of
the lab testing, a node under `replication.append_reserve` leads nothing and takes no entries
into a durable log, so a nearly full disk stops filling and the node goes on serving reads.

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

### The second part: an append reserve

A node that stops serves no reads either, and a full disk restarted it into the same wall until
somebody made room. Below `replication.append_reserve` (512 MiB by default, half of
`migration.disk_reserve`) a node now leads nothing and appends nothing more, and keeps what it
holds readable. Each shard reads the storage's free bytes once a second on its tick (`check_disk`
in `server/shard/groups.rs`, the same `statvfs` the snapshot reserve reads). Under the reserve:

- **It leads nothing.** Every group the shard leads is handed to the voter furthest along
  (`hand_off_leadership`, the planned stop's handoff), again every five seconds for a lead an
  election gave back. It refuses `TransferLeader`, so the placement's handback
  ([O63](../optimizations.md#o63-leadership-never-returns-to-a-groups-placement-primary)) cannot
  bring one back, and the handback itself stands down on the node.
- **It appends nothing it would lead.** A write it would append as a leader is refused `NotLeader`
  before `client_write`. Writes through it to groups led elsewhere hop as ever.
- **It takes no entries.** An `AppendEntries` that carries entries for a durable group is answered
  with an error. The leader sees a failed append and tries again later. The group commits on its
  other voters, and this copy follows its leader's heartbeats and falls behind. Heartbeats, votes
  and volatile groups are unaffected.
- **It serves what it holds.** A read at `One` through it is its own copy, as for any copy that is
  behind. A read that needs an index the copy has not applied, a `Quorum` read or one with a session
  token, is refused `Unavailable` at once rather than held for an apply it will not get.

An eighth over the reserve, it leads and takes entries again, is fed what it missed from its
leaders' logs or, past their purge points, by snapshot, and the handback returns its placement's
leads. The WAL failure path above stays as the backstop, for a disk filled by something else.

**Reproduced** by `a_node_under_the_append_reserve_leads_nothing_and_serves`, which sets node one's
reported free bytes to 100 MiB (`FREE_BYTES`, the capacity override). With the reserve set to zero,
which is the tree before this change:

```text
node one still leads 4 groups under the append reserve
```

With it, node one led nothing within the test's poll, writes through it landed on the other two, a
read through it returned what it held, no lead came back in the 20 s the handback would have taken,
and once the override was lifted its copy caught up and all three digests matched.

**On the lab**, the disk-full run again (`target/lab/r11/diskfill.sh`): hyperion's storage on a
2 GiB ext4 loop filesystem, a fresh cluster, the whole csv loaded with retries unbounded.

- **All six of hyperion's shards went under the reserve at once**, 1,351 MB used and 525 MB free,
  and handed on their 12 leads within 0.6 s.
- **The disk stopped filling there.** For the remaining ten minutes of the run it read 478 MB
  free, with no restart and no `No space` in the journal. The same load on the tree before stopped
  the node 70 s in.
- **The load finished**: 2,193,788 rows at 14,023 a second, retrying 122 writes answered unknown
  and 327 refused `NotLeader` around the handoffs. The groups committed on europa and titan.
- **Hyperion served its copy while under the reserve.** Every movie read through it alone at
  `One`: 666,405 of 1,187,691 missing, **0 different**. What it held was a consistent prefix of
  what it had been sent.
- **Grown to 6 GiB online, it came back on its own.** All six shards logged they were back over
  the reserve at the next check. 120 s later, every movie read through hyperion alone equalled
  the csv: 0 missing, 0 different.

The run also found that a read with a session token, which the loader's sample read-back sends,
waited out its whole deadline on the frozen copy for an apply it would never get, and was retried
there without end. Such a read is now refused `Unavailable` at once when the copy has not applied
what it needs (`wait_on_group` in `server/shard/reads.rs`), so a client asks another member.

## Alternatives rejected

- **Clear the error and carry on.** The WAL's tail past its watermark is unknown after a failed
  write or sync, and appending after it risks exactly the silent loss fsyncgate was.
- **Restart only the dead group cores.** Every group on the shard shares the failed WAL.
- **Refuse writes while leaving the node up**, once its WAL has failed. It keeps a member in the
  placement that holds nothing new, and takes someone reading the logs to notice. Before the disk
  is full, the reserve does leave it up: its WAL has not failed, and what it holds is trusted.
- **Refusing appends inside the WAL (`stage`).** openraft takes a storage error as fatal to the
  group's core, which is the dead copy this item began with. The refusal is at the RPC, before
  openraft sees the entries.
- **Shedding only the writes the node proposes.** A follower appends every write its leaders
  commit, so its disk goes on filling at the same rate, and it stops anyway.
- **Asking the control leader to decommission the node.** A plan moves its sets off, which needs
  space on the others and minutes a set, and it is an operator's decision. The reserve is what
  holds while that decision is made.

## Invariants to uphold

- **A shard never serves on after its WAL failed.**
- **Recovery from a failed WAL is a restart**, which reads back only what is durable.
- **Under the reserve, nothing is refused after it reached openraft.** A write is refused before
  `client_write`, and entries are refused at the RPC before `append_entries`. A storage error
  inside openraft is still fatal to the group's core.
- **A copy under the reserve is a copy that is behind**, and is served as one: `One` reads its own
  state, and anything that needs it to apply waits for space.
- **The reserve is judged on the same path the WAL writes to**, `latency_sensitive`, and the
  release is an eighth above it, so a disk at the line does not hand leads back and forth.

## Still open

- ~~**Nothing refuses appends before the disk is full.**~~ Done: the append reserve.
- A node under the reserve says so in its journal only. `Stats` carries its free bytes, but no
  field says it has stopped taking entries.
- Two nodes under the reserve at once leave a factor-three group with no quorum, and its writes are
  refused until one has space: correct, and the right place for a decommission or more disk.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_node_whose_wal_cannot_be_written_stops` (`shoal/tests/cluster_fixture.rs`) | A node whose WAL cannot be written stays up, taking nothing |
| `a_node_under_the_append_reserve_leads_nothing_and_serves` (the same file) | A node under the reserve keeps its leads, takes one back, or does not catch up once there is space |
| Fill a node's disk ([cluster testing](../../cluster-testing/correctness.md#fill-a-nodes-disk)) | The node lingers as a member whose copies are dead |

## Related

- [Resolved #151](purge-ahead-of-its-marker.md), a WAL durability rule of the same kind.
