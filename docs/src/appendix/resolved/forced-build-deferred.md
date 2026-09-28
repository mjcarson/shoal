# 192. Past `hold_bytes` the retention sweep could not force a held group, and asked hundreds of times a second

Filed and fixed in one change, from round 15 of the lab testing
([a step that outlasts both retentions](../../cluster-testing/correctness.md#a-step-that-outlasts-both-retentions)).

## Symptom

hyperion rebuilt under the mixed bench on the ten copy cluster with every node's snapshot streams
throttled to 4 MiB/s, `retained_bytes: 20MiB` and `hold_bytes: 64MiB`, so a step (a set of about
900 MB) outlasted what either retention keeps. The rebuild did not finish: after 30 minutes three
of eight steps had moved, four had failed, 18.9 GiB had streamed for 2.6 GiB moved, and the plan
stood blocked with fifteen sets under the factor. europa logged
`the retention budget is passed; forcing a group past a sealed segment` 117,722 times in 17
minutes, and openraft logged 182,599 `purge_log` commands and 7,260 deferred builds beside them,
about 580 journal lines a second. Titan logged 20,799 of the same warnings. For eight minutes the
cluster served a tenth of its rate, with updates at 400 to 500 ms at the median, and the bench
was refused `NotLeader` 298,252 times. Nothing was lost.

Every warning named a group whose `purged` never moved while `held_bytes` stayed at 262 MB, three
times the 84 MiB the two settings allow:

```text
WARN ... msg="the retention budget is passed; forcing a group past a sealed segment" group=6c258d91db48bfbb
    generation=1856 checkpoint=11462490 purged=11336620 through=11342952 lag=125870
    held_bytes=262153209 budget=20971520 taking=true
WARN ... generation=1857 checkpoint=11462490 purged=11336620 through=11349240 ...
WARN ... generation=1858 checkpoint=11462490 purged=11336620 through=11355487 ...
```

## Cause

[#188](forced-purge-outruns-snapshot.md) had the retention sweep pass over a group a member is
taking a snapshot of while the shard's sealed WAL is within `hold_bytes` past `retained_bytes`,
and force every group alike past both, so "a snapshot that never ends cannot keep a shard's WAL
from being bounded". A force is `raft.trigger().snapshot()` and then `purge_log(through)`: the
snapshot first, since openraft never purges past its last one.

The build never happened. [#185](snapshot-outrun-by-purge.md) holds a held group's builds in
`GroupMachine::try_create_snapshot_builder`, except a forced one, and tells them apart by the
method's `force` flag. openraft 0.10.0-alpha.34 asks for every build with the flag false
(`core/sm/worker.rs`, `build_snapshot`: `try_create_snapshot_builder(false)`), a triggered one
included. So the forced build was deferred like any other, the purge was refused ("cannot purge
logs not in a snapshot"), the WAL stayed over its bound, and the next sweep, a few seconds later,
forced the group again: once for each sealed segment it had frames in, each force a spawned task
sending two commands to the group's core.

## Evidence

**Established by reproducing it on the lab and by reading openraft's source.** The run is
`target/lab/r15/rb3/` (`rebuild.sh` with `outlast.yaml`), and the counts above are from its
journals. openraft's worker:

```rust
async fn build_snapshot(&mut self, resp_tx: MpscSenderOf<C, Notification<C>>) {
    let builder = self.state_machine.try_create_snapshot_builder(false).await;
    let Some(mut builder) = builder else {
        tracing::info!("{}: snapshot building is refused by state machine", func_name!());
```

The new unit test fails on the old path: a build of a held group asked for as openraft asks is
deferred whatever the sweep did, since the sweep had no way to say it forced one.

## The fix

**The sweep's force reaches the machine through the holds.** `SnapshotHolds::force(group)` marks a
group before the sweep triggers its build; `SnapshotHolds::allow_build(group, force)`, which the
machine now asks, lets a build through if openraft's flag or the sweep's mark says it is forced,
takes the mark, and defers anything else of a held group as before.

**A group is forced once a sweep, and not again while its last force is in flight.** The sweep
gathers each group's furthest entry across the segments it drops and triggers one snapshot and one
purge through it; `force` refuses a group whose last force has not ended, and the spawned task
ends it a second after its commands are queued (`forced_done`). One warning a force, not one a
segment a sweep.

## Alternatives rejected

- **Passing `force` through openraft.** The worker's call is not configurable in this version, and
  the flag's meaning there is openraft's own.
- **Dropping the hold past `hold_bytes`.** The hold exists for #185, and the same group's
  unforced builds still have to wait for the member.
- **Purging without a snapshot.** openraft refuses it, and a purge past the last snapshot would
  leave a member nothing to be fed from.

## Invariants to uphold

- **A build the retention sweep forces is never deferred by a hold.** It is the only thing that
  bounds a shard's WAL while a member takes a snapshot too slowly.
- **Never rely on `try_create_snapshot_builder`'s `force` flag**: openraft does not set it.
- **The sweep asks once per group and waits for its answer.** A sweep that re-forces every tick
  turns a stuck group into a storm on its core.

## Still open

- A step that outlasts `retained_bytes + hold_bytes` still cannot finish: the forced purge takes
  the entries after its snapshot's boundary, and the member is sent another. That is the bound
  working, and the operator's to raise; the plan names the failing tablet.
- The dial noise a rebuild makes at the old identity's address, about twenty warnings a second, is
  filed as [#193](../known-issues.md#193-a-rebuild-dials-the-old-identitys-address-twenty-times-a-second).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `replication::network::tests::a_forced_build_goes_through_a_hold` | A build the sweep forced is deferred by a hold like any other, and a second force is asked while the first is in flight |
| The lab arm, `target/lab/r15/rebuild.sh` with `outlast.yaml` | Past both retentions the sweep livelocks: the WAL stays over its bound and the node logs hundreds of lines a second |

## Related

- [#188](forced-purge-outruns-snapshot.md), the allowance whose force this makes work.
- [#185](snapshot-outrun-by-purge.md), the hold on builds.
- [F43](../../features/node-recovery.md), a member fed by snapshot.
