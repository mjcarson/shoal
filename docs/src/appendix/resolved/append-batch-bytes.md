# 202. Nothing bounded an append batch in bytes

## Symptom

A tablet group feeds a member that is behind up to three hundred entries at a time, whatever
they weigh. A member that fell behind a run of wide rows was owed batches larger than a frame.
Those were never sent, and the member stayed behind for as long as the leader kept them. A
member cut off while two hundred writes of 128 KiB landed on one key never caught up once
healed: its copy of the group stayed at index 2 while the other two stood at 202.

Nothing was lost, and a group with a quorum went on committing. What was lost was the member's
ability to catch up, which is the redundancy. It stayed narrow only because the cluster arms and
the lab's dataset write rows of about a kibibyte. Object storage's inline objects are rows of
exactly the width that triggers it ([S1](../../object-storage/prerequisites.md#required)).

## Cause

`group_config` (`shoal-core/src/server/shard/groups.rs`) sets no `max_payload_entries`, so
openraft's default of 300 stands (`openraft-0.10.0-alpha.34/src/config/config.rs:67`). That is
a count. openraft's replication stream asks the log for `[start, min(end, start + 300))`
(`src/replication/stream_state.rs:253-257`), and `GroupPeer::send_append`
(`shoal-core/src/server/replication/network.rs`) encodes the whole request. `rpc_watched` frames
it against the link's `max_frame_bytes`, and a request past that is reported unreachable:

```rust
.map_err(|error| {
    RpcFailure::Unreachable(format!("framing a replication request: {error:?}"))
})?;
```

openraft 0.9 had `RPCError::PayloadTooLarge` with an entries hint, and retried with fewer. 0.10
removed it. Its `RPCError` has only `Timeout`, `Unreachable`, `Network` and `RemoteError`
(`src/errors/mod.rs:166-182`). After an `Unreachable`, `run_stream_session` backs off and builds
the same range again (`src/replication/mod.rs:290-386`), so the batch never shrinks. That
settles what the item left open: openraft does not send fewer entries of its own accord. The
member stays behind until its lag passes the purge point and a snapshot replaces the log it
could not be sent.

## Evidence

**Reproduced.** `a_member_behind_wide_rows_catches_up_by_appends` in
`shoal/tests/cluster_fixture.rs` sets up the failure:

1. It cuts node two off on every link of a three node cluster at a 16 MiB frame bound.
2. While it is cut off, two hundred writes of 128 KiB land on one key of the persistent table
   and one of the ephemeral table. That is about 25 MiB a group, against a 16 MiB frame.
3. It heals the links and waits for the digests to agree.

Against the tree at `a877cd8`, with the fixture's frame knob in place and the fix not:

```text
test a_member_behind_wide_rows_catches_up_by_appends has been running for over 60 seconds
test a_member_behind_wide_rows_catches_up_by_appends ... FAILED

thread 'a_member_behind_wide_rows_catches_up_by_appends' panicked at shoal/tests/cluster_fixture.rs:6345:9:
the member behind wide rows never caught up: NotReady("the digests of Note never agreed:
[{"groups": {"5a57188f4e799cc7": 202, …}, …}, {"groups": {"5a57188f4e799cc7": 202, …}, …},
 {"groups": {"5a57188f4e799cc7": 2, …}, …}]"); its groups: {…,"lag_max":200,…}
```

With the fix the test passes in fifteen seconds, and node two installs no snapshot: it was fed
from the log. The lab's A/B is under [the fix](#on-the-lab). The whole fixture suite at six
threads passed 145 of 146, and the one failure was
`single_node_data_has_a_verified_cluster_migration_path` at the #142 deadline, which passed alone
three times.

## The fix

**The batch is cut where openraft lets a log cut it.** openraft 0.10 asks the log reader through
`RaftLogReader::limited_get_log_entries(start, end)`. It documents the method as one that "may
return only the first few log entries to ensure the result is not excessively large", and it must
return at least one. Nothing in Shoal overrode it, so the default returned the whole range.
`GroupStore` now overrides it. `GroupStore::batch_end` walks the range and returns the longest
prefix whose frames fit the store's bound, and always one entry. It sizes each entry like this:

- On the shared WAL, an entry weighs the frame its slot records (`Loc.len`).
- In a volatile group's memory log, which has no frames, an entry weighs the frame the WAL would
  write for it. That is `frame::frame_len`, which is exactly what `encode_entry` writes.

An append carries each entry behind a log id and a length, which take less than a frame's fixed
part. So a batch is never larger on the wire than its weight, and
`an_entry_never_weighs_more_on_the_wire_than_in_the_log` holds the two together.

**The bound is a setting.** `cluster.replication.append_batch_bytes`, 8 MiB by default, is
handed to every group's store as it is built (`GroupStore::batch_bytes`). `validate` refuses
three values:

- zero;
- a bound that does not fit `networking.max_frame_bytes` with 4 KiB of request heads;
- a bound that does not fit `transport.replication_queue_bytes`, since a link sheds a frame
  larger than its queue every time it is sent.

### On the lab

The lab cluster (`tmdb_cluster.yaml`: europa, titan and hyperion, 1 GbE, the default 64 MiB
frame) was deployed fresh, 2026-10-03, once on each build: `a877cd8`, before the fix, and
`9b4167c`, with it. Each time titan's unit was stopped, and `tmdb-dataset-loader load` wrote
6,000 new movies with 256 KiB overviews, about 1.5 GiB. Titan was then started and left for
150 seconds, and `shoaladm stats --basic` read every member's figures:

| | europa | hyperion | titan |
| --- | --- | --- | --- |
| **Before**, archived | 506.2 MiB | 997.4 MiB | 51.6 MiB |
| **Before**, WAL segments | 278 | 156 | 12 |
| **Before**, titan's apply lag | | | 5,012 entries, the same two minutes later |
| **After**, archived | 1.4 GiB | 1.4 GiB | 1.4 GiB |
| **After**, partitions | 11,757 | 11,756 | 11,880 |
| **After**, WAL segments | 153 | 153 | 153 |
| **After**, titan's apply lag | | | 0 |

Before the fix titan applied nothing more once the leaders reached their first batch past the
frame. With it, titan held what the others held when the 150 seconds were up.

The load itself differed too. This was observed, not explained. On the build before the fix the
loader failed three times with leases that lapsed ("no quorum acknowledged it within 5s") and
writes answered `OutcomeUnknown`. Each attempt ran at 30 to 280 rows a second, and only 3,502 of
the 6,000 movies landed. On the build with it, the whole load ran once at 573 rows a second with
no failure. A plausible mechanism, established by reading and not by profiling: every retry to
the stopped member read and encoded a batch of three hundred entries, 75 MiB, on the shard that
also commits the group's writes. The bench's own catch-up mark could not be used to judge this:
it reads the lag of the node the bench's admin client is connected to, never the node that was
stopped
([item 209](../known-issues.md#209-a-bench-event-arms-converged-mark-reads-the-wrong-nodes-lag)).

## Alternatives rejected

**Derive `max_payload_entries` from the frame bound and the largest entry a group may hold.** It
is crude and safe, as the item said, but the largest entry a group may hold is a client's frame,
so the count would be one, and every member would be fed one entry a round trip. The bound
belongs on bytes because that is what it bounds.

**Have `send_append` send the longest prefix that fits and answer `PartialSuccess`.**
`AppendEntriesResponse::PartialSuccess` exists for a follower that accepted part of a request.
Sending it from the leader's own network layer means the leader reports something the follower
never said, and openraft's progress tracking would then rely on Shoal's network inventing a
response. Cutting the batch where it is read means it is never built too large.

**Use the whole frame bound as the budget, with no setting.** That fixes the stuck member but
leaves 64 MiB batches on the replication lane. On the lab's 1 GbE that is about half a second of
head-of-line blocking for every heartbeat and every other group sharing the link, near the
election timeout.

## Invariants to uphold

- **`limited_get_log_entries` never returns nothing for a range that is not empty.** openraft
  treats an empty answer as a broken contract and sleeps, so the member would be fed nothing.
  `batch_end` always takes the first entry.
- **An entry is weighed at no less than it costs in an append.** The shared log weighs a slot's
  frame, and the memory log weighs `frame_len`. Both include a fixed part larger than an
  append's per-entry overhead. A frame format that shrinks the fixed part below that breaks the
  bound in silence, which is what `an_entry_never_weighs_more_on_the_wire_than_in_the_log` is for.
- **`append_batch_bytes` plus the request's heads fits the frame and the replication queue.** The
  validation is the guard. A smaller frame bound on a cluster node has to come with a smaller
  batch bound.
- **The count bound is openraft's and is untouched.** A batch is still at most
  `max_payload_entries` entries; the byte bound only ever shortens it.

## Still open

- **One entry larger than a frame is still unsendable.** A batch carries it alone, and the frame
  refuses it. A write is bounded by the client's frame and the peer link by its own, and nothing
  checks that the command a write becomes fits the second. Filed as
  [item 208](../known-issues.md#208-a-write-that-fits-a-client-frame-can-make-a-log-entry-no-peer-frame-carries).
- **The bound weighs the log, not the wire.** The two agree to within a frame's fixed part per
  entry, in the safe direction. A batch of many tiny entries is therefore cut a little sooner than
  the wire would need.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_member_behind_wide_rows_catches_up_by_appends` | `shoal/tests/cluster_fixture.rs` | A member owed more than a frame of entries on the shared WAL and in memory never catches up |
| `a_limited_read_stops_at_its_byte_budget` | `shoal-core/src/server/wal/tests.rs` | A read for a member returns the whole range openraft asked for, on either backend |
| `a_limited_read_returns_one_entry_larger_than_its_budget` | `shoal-core/src/server/wal/tests.rs` | An entry wider than the bound is cut to nothing, and the member is fed nothing |
| `an_entry_never_weighs_more_on_the_wire_than_in_the_log` | `shoal-core/src/server/wal/tests.rs` | An entry's weight and its frame drift apart, or an append outweighs the frame it was weighed as |
| `validation_refuses_what_is_not_built` | `shoal-core/src/server/conf/cluster.rs` | A batch bound of zero, or one that does not fit the frame or the replication queue, is accepted |
| `documented_cluster_defaults_match_policy_bootstrap` | `shoal/tests/cluster_fixture.rs` | The configuration page's `append_batch_bytes` drifts from the default |

## Related

[F40. Replication](../../features/replication.md), whose groups this bounds;
[Resolved #190](append-answer-thrown-away.md), the other append path fix, which waits a late answer
out; [item 203](../known-issues.md#203-pending_bytes-bounds-a-group-and-the-configurations-own-comments-call-it-a-shards),
the bound on what a group holds proposed; [S1](../../object-storage/prerequisites.md#required),
which listed this item as required before M12.
