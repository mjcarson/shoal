# 138. Every write on a long-lived query stream was refused `IdentityExpired`

## Symptom

In the lab's first kill test ([cluster testing](../../cluster-testing/correctness.md#kill-a-follower)),
`IdentityExpired` appeared 17 seconds after the kill and did not stop. From about 30 seconds after
the kill until the end of the run it refused 20,000–26,000 operations a second: every update and
insert the bench sent, about a quarter of all operations. Reads were unaffected. Nothing was
wrong with the cluster by then: the members were up and writes on new streams went through.

## Cause

A query stream (`stream_with`, `stream_unordered`) overwrote each bundle's id with the stream's
own id, a UUIDv7 minted when the stream was opened. Every write the stream sent therefore
carried the same time-ordered identity, the stream's creation time, with a growing index. Since
[F45](../../features/replica-migration.md) a group judges a write's identity by that time before
proposing it (`MachineState::is_expired`) in two ways:

1. **Older than `replication.retry_window`** (five minutes by default) is refused. A stream open
   for longer than the window had every write refused, with no fault anywhere.
2. **Older than `expired_before`**, the mint time of the newest identity the group has evicted from
   its bounded dedup table, is refused. When a newer stream's entry was evicted, every stream
   opened before it was refused on that group from then on.

The kill test hit the second. Workers whose streams failed at the kill opened new ones, their
entries aged out of the dedup tables as the load went on, and each group that evicted one began
refusing the streams opened at the start of the run. The count grew as more groups did. Both
checks are right for what they were built for, a bundle retried under its identity. A stream
bundle is never retried under its identity, and an identity minted once for a stream's whole life
does not say when any of its writes was sent.

## Evidence

**Reproduced against the unfixed tree** with `a_stream_older_than_the_retry_window_still_writes`,
in a worktree of `8afdeac`. One node, a two second retry window, one write on a stream, a three
second wait, and a second write on the same stream:

```text
test a_stream_older_than_the_retry_window_still_writes ... FAILED
round 1 was refused: Some((IdentityExpired, "the identity of this write is older than the 2s retry window, or older than an identity group cac2d7a48a5658b8 has forgotten; a retry this late is not answered its first result"))
```

## The fix

`ShoalQueryStream::send` gives every bundle an identity of its own, `Uuid::now_v7()` at the moment
it is sent (`shoal-client/src/client.rs`). Each bundle gets its own slot in the channel map, routed
to the stream's channel and registered before the write so an answer that arrives at once finds
it. The slot carries the owed counts of [items 130 and 131](stream-connection-accounting.md) for
that bundle alone. Since nobody releases a bundle's slot, the relay removes it when the bundle's
last answer arrives or an error frame names it, and `fail_waiting` removes it when its connection
dies owing it answers. Indexes still run across the whole stream, so the stream's reorder logic
and a caller keyed by index are unchanged. The server is untouched.

## Alternatives rejected

- **Exempting streams from the expiry on the server.** The server cannot tell a stream bundle from
  a retried bundle: both are a bundle with an id. A flag on the wire would say so, but at the cost
  of the thing the identity is for: a group could then not refuse a genuinely late retry that
  arrived through a stream.
- **Tracking a per-bundle high-water index instead of a time watermark.** It would make a stream's
  single identity work: a retried (bundle, index) below the bundle's forgotten index is refused.
  But it costs state per identity where the watermark is one number per group, and it would
  change what a checkpoint and a snapshot carry.
- **A longer default retry window.** It only moves the first failure mode out to a longer stream,
  and does nothing about the second.

## Invariants to uphold

- **An identity is minted when the write it names is sent.** Every age check a group makes assumes
  it. A client path that reuses an identity across sends has to be a retry of the same bundle.
- **One response frame per query, with the query's own bundle id.** The relay counts a bundle's
  answers to know when its slot can go.
- **A query stream's indexes are global to the stream**, whatever identity each bundle carries,
  because callers key their bookkeeping by index (the loader's `Pipeline::outstanding`, the bench).

## Still open

- The dedup table remembers every write of a stream, like any other bundle's, and a stream's
  writes are never retried under their identity. A client flag saying "do not remember this"
  would spare the table, at the cost of a wire change.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_stream_older_than_the_retry_window_still_writes` (`shoal/tests/cluster_fixture.rs`) | A stream's write after the retry window is refused `IdentityExpired` |
| `a_stream_dropped_early_is_not_tracked` (`shoal/tests/ephemeral_unsorted_table.rs`) | A bundle's slot outlives its answers, or a dropped stream's bundles stay tracked |
| Kill a follower under load ([cluster testing](../../cluster-testing/correctness.md#kill-a-follower)) | A quarter of the operations after the fault are refused `IdentityExpired` |

## Related

- [F45](../../features/replica-migration.md), the retry identity and its window.
- [Resolved #130, 131](stream-connection-accounting.md), whose per-connection accounting each
  bundle's slot now carries.
