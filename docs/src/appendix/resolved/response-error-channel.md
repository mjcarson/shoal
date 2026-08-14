# 56, 61. A response cannot say that a read failed

## Symptom

Three different failures reached a client as the same non-answer, or as no answer at all.

**A read that failed looked like an empty partition.** A get whose only copy on disk could not be
read answered `Get(None)`, which is exactly what a get of a partition holding no rows answers.
`send_one` turned both into `Err(QueryDidNotSucceed { kind: Get })`. The server knew — the loader
logged the table, the partition and the errno at `ERROR` — and the client did not.

**A response too large to frame closed the connection in silence.** `client_tx_relay` built the
preamble for a response, found it larger than the frame bound this client had agreed to, logged it
and `break`'d. Every other query multiplexed on that connection died with it, and the client saw a
socket close with nothing on the wire to say which query had caused it.

**A connection whose read loop stopped left its callers waiting forever.** The `JoinHandle` for
`TcpProxy::start` was dropped at `client.rs:787`, so every failure in the read loop was discarded
with nothing written down anywhere. A `ShoalResultStream` holds a clone of its own sender, so the
channel it waits on never closes and the receiver never observes that nothing is coming.

## Cause

`ResponseAction` had five variants — `Insert(bool)`, `Get(Option<Vec<T>>)`, `Delete(bool)`,
`Update(bool)`, `Exists(bool)` — and none of them carried an error
(`shoal-core/src/shared/responses.rs:28-39`). That one gap decided the shape of every storage
failure that reached a query: the only expressible answers were "here are rows", "here are no
rows", and a boolean.

The frame format had a `MessageType::Error = 10` discriminant and a `Flags::IS_ERROR` bit, both
reserved by [F10](../../features/framing-and-protocol-evolution.md) and both unconstructed. Nothing
could build either, so `client_tx_relay` — which holds an opaque `AlignedVec` and does not know
which variant of the schema's response enum it belongs to — had no way to say anything at all.

And `channel_map` mapped a query id straight to a sender, with nothing recording which connection
the query had been written to. A read loop that died therefore could not tell which queries it owed
an answer to: the map is shared across the whole pool, so the only sweep available to it would have
failed every in-flight query on all fifty connections.

## Evidence

**Items 56 and 61 were established by reading the source**, during the August 2026 review and while
building F10 respectively.

**Item 51's remainder was established by reproducing it**, and the reproduction was already
committed: four integration tests asserted the broken behaviour and said so in their own comments.

```rust
// this get could not read the only copy of the row, so it finds nothing
//
// that is the limitation this fix knowingly carries: a read that failed is reported to
// the client the same way an empty partition is, because a response cannot yet say that
// a read failed
assert!(matches!(
    answered.expect("timed out"),
    Err(shoal_core::client::Errors::QueryDidNotSucceed {
        kind: shoal_core::shared::responses::ResponseActionNames::Get,
        ..
    })
));
```

`shoal/tests/persistent_sorted_table.rs:2488`, before this change

Those comments are the evidence, and they are stronger than a fresh reproduction would have been:
they were written by the person who knew the behaviour was wrong and could not fix it there.

**The connection-death half was established by reproducing it**, with a test that opened a socket
pair, dropped the server side, and asserted that a query registered on that connection was told
rather than left waiting. Against the unfixed tree it hangs.

## The fix

Two layers, because one failure has two shapes.

**`ResponseAction::Error(ResponseError)`** for a failure that belongs to a query.
`ResponseError { code: u16, msg: String }` holds the class as the raw number it is written as and a
line of prose. The class comes from `protocol::error::ErrorCode`, a `#[repr(u16)]` enum with pinned
discriminants banded by decade.

**A frame-level `Error` message** for a failure that cannot be a response. Its body is
`[query id 16][code 2][reserved 2][message]`, with the query id at exactly the offset a response
frame puts one, so the client reads one fixed preamble for both and dispatches afterwards.

Where they are produced:

| Site | What it does now | What it did |
| --- | --- | --- |
| `client_tx_relay`, `shard.rs` | writes an error frame naming the query and both sizes, then `continue`s | logged and `break`'d, killing the connection |
| `load_partition`, both tables | routes its own early exits into `fail_partition` with a `CorruptArchive` failure | returned `Err`, which ended the shard and left the parked queries in `blocked` forever |
| `fail_partition`, both tables | sets `meta.failed` alongside `meta.skip_disk` | set only `skip_disk`, so the replay answered from what was resident |
| `loader::client_error` | maps a `ServerError` to a code, and `LoadFailure::Absent` to `None` | did not exist |
| `TcpProxy::start` | on every exit path, fails the queries written to *its* connection and marks it dead | returned an `Err` into a dropped `JoinHandle` |

`load_partition` returns a new `PartitionLoad { Idle | Loaded | Failed }` so the derive can tell a
read that succeeded from one that gave up, and replay the released queries without the
`MarkEvictable` message a successful load sends.

The failure is applied in **one place per table**, at the tail of `handle`, by `apply_failure`. Not
at the sixteen sites that build a `Response`: doing it there would have had to decide the `end` flag
and the index sixteen times, and would have answered a query that was still parked on another
partition, putting a second response at an index that already had one.

On the client, `channel_map`'s value became a `Waiter { conn, tx }`, both `next()` implementations
release their slot on the `Err` path as well as the `end` path, and the pool learns about a
connection whose reader has stopped through a `dead_conns` set that `is_valid` and `has_broken`
drain as they read it.

## Alternatives rejected

**Only the frame-level error.** No wire format change at all, since the discriminant was already
reserved. It fails on indexing: a frame-level error names a bundle and a bundle is many queries, so
query 3 of 10 failing would fail the whole stream.

**Only `ResponseAction::Error`.** Closes 51 and 56 and cannot close 61, because the relay has an
`AlignedVec` and a `Uuid` and building a `ResponseKinds` from those means teaching it the schema it
exists to be ignorant of.

**Sharing `ErrorCode` into the archived payload directly.** Would need `#[derive(Archive)]` on the
enum, which drags rkyv into `protocol.rs` — the module that exists to frame the serialization
format, and so cannot depend on it. Two parallel enums instead would be two discriminant tables that
must be renumbered together with nothing failing if they drift.

**Forcing `end: true` on a failed response.** Tempting, because it looks like it closes item 60's
leak for free. It ends the stream at whichever index failed and orphans every later response in the
bundle into the unknown-query arm. The leak is on the `Err` return path and was fixed there instead.

**Answering a parked query the moment its read fails.** Wrong for a get naming partitions on several
shards, which may still be parked on another one.

**Sweeping the whole `channel_map` when a read loop dies.** It cannot tell whose queries it is
failing: the map is shared by fifty connections, and the sweep would fire on every ordinary bb8 idle
reap as well as on a real failure.

**Reporting a pruned partition as a failure.** `LoadFailure::Absent` is the compactor pruning a
partition out from under a read, which the loader deliberately allows. A query replayed against one
has correctly found everything there is to find, and reporting it would turn every ordinary delete
into an error.

## Invariants to uphold

**`ErrorCode` discriminants are never renumbered and never reused.** They are on the wire.

**Every server→client frame carries a 16-byte query id immediately after its header.** The client
reads one preamble for all of them.

**The preamble read and the body read stay two `read_exact` calls, with the type dispatch between
them.** Merging them puts a response payload at offset 24, which is never 16-byte aligned, and the
zero-copy read ends silently.

**An error response carries the `end` its query would have had, and there is exactly one response
per index.**

**The release block runs on the `Err` path as well as the `end` path.** Both `next()`s check
`matches!(outcome, Err(_) | Ok((true, _)))` before the `?` for this reason.

**A read loop marks its connection dead before it sweeps; a send checks that mark after it
registers.** Either order alone leaves a window where a query is registered on a connection nobody
is reading.

**`LoadFailure::Absent` carries no failure.** A pruned partition is not an error.

## Still open

**The eight `storage.commit(..).unwrap()` sites still panic**, and they are the ones with a
plausible non-adversarial trigger — a full disk on an ordinary insert. They now have somewhere to
put an error and do not use it. That is the largest remaining piece of
[item 16](../known-issues.md).

**`send_one` still treats an empty get as a failure** — [item 55](../known-issues.md), unblocked and
not closed.

**A server that stops answering on an open socket is still a hang** —
[item 62](../known-issues.md). Nothing here is a timeout.

**A query stream spanning several connections registers only its most recent one.**

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_response_too_large_to_frame_is_answered_with_an_error_naming_the_query` | Item 61 returns: the connection closes with nothing on the wire |
| `a_response_too_large_to_frame_leaves_the_connection_serving` | One oversize response ends a connection serving many queries |
| `a_get_whose_partition_cannot_be_read_does_not_hang` (both tables) | A read failure is answered as an empty partition again |
| `a_get_whose_archive_is_missing_does_not_end_its_shard` (both tables) | The two storage failure classes collapse into one answer |
| `a_get_that_found_nothing_is_not_reported_as_a_failure` | An empty partition starts reporting as a failed query |
| `delete_survives_restart` | A pruned partition reports as a failure, turning every delete into an error |
| `a_dead_connection_fails_the_queries_it_owed_and_no_others` | A dead connection leaves its callers waiting, or fails healthy queries on other connections |
| `an_error_frame_for_an_unknown_query_does_not_end_the_read_loop` | One dropped stream kills every other query on that connection |
| `an_error_share_wins_a_merge` | A split get hides one shard's failure behind three shards' rows |
| `an_error_response_never_succeeds_whatever_the_opts_say` | `QuerySuceededOpts` can turn a failure into a success |
| `every_error_code_round_trips_through_its_discriminant` | A code inserted into the enum renumbers the wire silently |
| `an_error_frames_query_id_sits_where_a_responses_does` | The client's shared preamble read starts reading a code as part of a uuid |
| `an_error_frame_does_not_disturb_the_response_read` | The type dispatch breaks the alignment of the frame behind it |

## Related

- [F11. The error channel](../../features/error-channel.md) — the built design in full
- [Resolved: A partition read that failed panicked its shard](partition-load-failure.md) — items 16
  and 51, whose *Still open* this closes
- [F10. Framing and protocol evolution](../../features/framing-and-protocol-evolution.md) — which
  reserved the message type and the flag bit this uses
- [D2. Framing and protocol evolution](../../direction/framing.md#the-error-channel) — where this
  was designed
