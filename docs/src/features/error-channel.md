# F11. The error channel

## Context

[D2](../direction/framing.md#the-error-channel) described a header, a handshake, a bounded length
and an error channel as one change. [F10](framing-and-protocol-evolution.md) took the first three
and left the last, which is the only part of that page that was scope rather than disagreement:
the error channel reaches the shard reply path, the storage layer, the response stream and
`send_one`, and doing it beside the header would have meant one change nobody could review.

What was left behind was a server that could work out that a query had failed and had no way to
say so. `ResponseAction` had five variants and none of them carried an error, so a get whose only
copy on disk could not be read answered `Get(None)` — the same answer an empty partition gives.
Two integration tests asserted exactly that, and said so in their own comments:

```rust
// this get could not read the only copy of the row, so it finds nothing
//
// that is the limitation this fix knowingly carries: a read that failed is reported to
// the client the same way an empty partition is, because a response cannot yet say that
// a read failed
```

`shoal/tests/persistent_sorted_table.rs`, before this change

Three open items wanted the same variant — [51](../appendix/known-issues.md), 56 and 61 — and a
fourth thing wanted it too: a client whose connection died waited forever, because the read loop's
`JoinHandle` was dropped and a result stream holds a clone of its own sender, so the channel it
waits on never closes.

## What it does

**A query that failed says so, and says what kind of failure it was.** `ResponseAction::Error`
carries a `ResponseError { code, msg }`, where the code is one of a pinned set
(`StorageRead`, `ArchiveMissing`, `CorruptArchive`, `ResponseTooLarge`, `ConnectionLost`, …). It
travels in an ordinary response frame, routed by query id like every other answer.

**A failure that cannot be a response is a frame of its own.** `MessageType::Error`, reserved and
unconstructed since F10, now carries a query id, a code and a message. It exists for the one place
that has a failure and no way to express it as a `ResponseKinds`: `client_tx_relay` holds an opaque
`AlignedVec` and does not know which variant of the schema's response enum it belongs to.

On the client:

| Call | What a failure looks like now | What it looked like before |
| --- | --- | --- |
| `send_one`, `exec` | `Err(Errors::Server { code, msg, .. })` | `Err(QueryDidNotSucceed { kind: Get })`, indistinguishable from an empty result |
| `exists` | `Err(Errors::Server { .. })` | `Err(UnexpectedResponseKind { expected: Exists, actual: … })`, naming the wrong problem |
| `next()` on either stream | `Err(Errors::Server { .. })`, with the stream's slot released | — |
| `response.error()` | `Some(&ArchivedResponseError)` | did not exist |
| `access::<T>()` on a failed query | `Err(Errors::Server { .. })` | `Err(WrongType("Wrong Type!"))` |

Three things stop being silent as a result. A response too large to frame is answered with an error
naming the query and both sizes, and **the connection keeps serving everything else on it** rather
than closing (item 61). A partition read that gives up part way through releases the queries parked
on it *with the failure*, instead of releasing them to answer from what is resident (item 51's
remainder). And a connection whose read loop stops fails the queries that were written to it, and
is taken out of the pool instead of being handed to the next query.

## Design choices

**One `ErrorCode`, and the archived payload holds the raw number.** `ResponseError.code` is a
`u16`, not an `ErrorCode`. Deriving `Archive` on the enum would drag rkyv into `protocol.rs`, and
that module exists to *frame* the serialization format — it cannot depend on it without making the
detector depend on the thing it detects, which is the same argument the handshake bodies rest on.
Two parallel enums would be worse: two discriminant tables that must be renumbered together, with
nothing failing if they drift. Storing the wire value and converting at the boundary
(`ResponseError::code()`) keeps one table, keeps rkyv out, and makes a code from a newer peer read
back as `Unknown` rather than as a number nothing matches.

**Every server→client frame carries a query id at offset 8.** An error frame's body starts with its
query id, in exactly the bytes a response frame puts one. That is what lets the client read a single
fixed `RESPONSE_PREAMBLE_LEN` preamble for both kinds and only then dispatch on the type. The
alternative — a preamble read sized per message type — would need the type before the read that
tells it the type, or a third `read_exact`. A nil id means the frame is about the connection rather
than about a query.

**`from_u16` fails open; `RefusalReason::from_byte` fails closed.** They answer different
questions. A refusal reason decides whether to trust a connection, so an unrecognised one must be
treated as a refusal. A code only classifies a failure that has already happened, and refusing to
decode it would throw away a message a person can still read to make a point.

**A failed share wins a merge.** `ResponseAction::merge` puts the two `Error` arms ahead of the
`Get` and `Exists` pairs. A get split across four shards where one could not read its partition
must not come back as the rows the other three found: the merge is the one place a partial answer
can be dressed up as a complete one, and that is precisely the defect item 56 describes.

**The failure travels on the query, not on the response site.** `QueryMetadata::failed` carries it,
and `apply_failure` swaps it in at the *tail of `handle`* — one place per table, rather than at the
sixteen sites that build a `Response`. Two things fall out of that. The `end` flag and the index
stay exactly what the query would have answered with, so a failure at index 3 of a 10-query bundle
does not end the stream at 3. And a query still parked on *another* partition produces nothing
there and carries its failure onward, which is what keeps it to exactly one response per index.

**An error response does not force `end: true`.** `end` means *last response of the bundle*, and
`channel_map` is keyed by the bundle id, so forcing it would end the stream early and orphan every
later index into the unknown-query arm. The leak that worried the design is real only on the `Err`
return path, and that is fixed directly: both `next()` implementations run the release block when
`wait_for_next_response` returns `Err`, not only when it returns an end.

**A pruned partition is not a failure.** `client_error` maps `LoadFailure::Absent` to `None`. The
compactor pruning a partition out from under a read is a race the loader deliberately allows, and a
query replayed against it has correctly found everything there is to find. Reporting it would turn
every ordinary delete into an error.

**A dead connection is identified, not guessed at.** `channel_map`'s value became a `Waiter`
carrying which connection the query was written to. A read loop that stops fails the queries owed
on *its* connection and no others — the map is shared by the whole pool, so a blanket sweep would
fail up to forty-nine other connections' healthy queries, and would fire on every ordinary bb8 idle
reap.

**The client is told less than the log records.** A failure names the table and the partition and
not the path, the archive id or the errno. With no authentication ([D3](../direction/authentication.md))
the server's filesystem layout is not something to hand to whoever opened a socket; the rich context
stays in the `ERROR` event the loader already emits.

## Alternatives rejected

**A frame-level error only, with no `ResponseAction::Error`.** Would have avoided a wire format
change entirely, since message type 10 was already reserved. It fails on indexing: a frame-level
error names a bundle, and a bundle is many queries, so query 3 of 10 failing would have to fail the
whole stream. The payload variant is what makes a per-query failure expressible at all.

**A `ResponseAction::Error` only, with no frame type.** Cheaper, and it closes 51 and 56. It cannot
close 61: `client_tx_relay` sees an `AlignedVec` and a `Uuid`, and building a `ResponseKinds` from
there would mean teaching the relay the schema it exists to be ignorant of.

**Setting `Flags::IS_ERROR` on response frames whose payload is an error.** The flag is set on every
`Error` frame and is deliberately *not* set on those. Doing it would mean threading a flag through
`client_tx_relay`'s `(Uuid, Span, StageStamps, AlignedVec)` tuple, and so through
`ServerMsg::NewClient`, `client_map` and `Shard::reply`, to tell the relay something about a payload
it exists to treat as opaque. Filed in [TODOs](../appendix/todos.md).

**Answering a parked query the moment its read fails.** Simpler than carrying the failure on the
metadata, and wrong for a get naming partitions on several shards: the query may still be parked on
another one, and answering here would put a second response at an index that already has one.

**A blanket sweep of `channel_map` when a read loop dies.** Rejected above — it cannot tell whose
queries it is failing.

**Failing a query the moment `is_valid` sees a dead connection, without the post-write check.**
Leaves a window: the pool can hand out a connection that dies before the write registers. The two
halves — the proxy marks the connection dead *before* it sweeps, and `send` checks the mark *after*
it registers — are what makes one of them always see the other.

## Limitations

**A frame-level error names a bundle, not a query.** The query id on the wire is a bundle id, so an
oversize response fails the whole result stream rather than the one query in it that was too large.
The two reserved bytes after the code are where an index would go.

**`send_one` still treats an empty get as a failure.** [Item 55](../appendix/known-issues.md) is
unblocked but not closed: "found nothing" and "failed" are now distinguishable on the wire, and what
remains is letting `send_one` take a `QuerySuceededOpts`.

**The eight `storage.commit(..).unwrap()` sites still panic.** A full disk on an ordinary insert now
has somewhere to go and does not yet use it. That is the largest remaining piece of
[item 16](../appendix/known-issues.md).

**A query stream registers only its most recent connection.** `ShoalQueryStream::send` takes
whatever connection the pool hands out per bundle and overwrites the waiter's connection each time.
If an earlier connection dies while a later one is healthy, the queries owed on the earlier one are
not failed. A single-bundle `send` is unaffected, which is every call except the streaming API.

**A server that stops answering on an open socket is still a hang.** Nothing here is a timeout. A
peer that accepts bytes and never replies is [item 62](../appendix/known-issues.md) and the
[Timeouts](../appendix/todos.md) entry, not this.

**`ResponseAction::Error` is a wire format change with no compatibility story.** The version byte is
the mechanism and it did not move: two peers built either side of this change disagree about the
layout of `ResponseKinds` and are caught, if at all, by the schema fingerprint or by `bytecheck`.
The same flag-day argument F10 made applies, and this is the second break.

## Invariants to uphold

**The client's preamble read and body read stay two `read_exact` calls, with the dispatch between
them.** The payload of a response must land at offset zero of a freshly allocated `AlignedVec<16>`,
which is what makes turning it into a response a pointer cast. The type dispatch was added in the
gap between the two reads precisely so it could not disturb that.
`the_response_payload_lands_on_a_sixteen_byte_boundary` and
`an_error_frame_does_not_disturb_the_response_read` are what catch a merge.

**Every frame a server sends a client carries a 16-byte query id immediately after its header.** A
new server→client message type must put its id there. `an_error_frames_query_id_sits_where_a_responses_does`
pins it.

**The message type is authoritative and `Flags::IS_ERROR` is never the sole test.** The flag is
redundant against the type byte on purpose, so that it stays correct when a response frame starts
setting it. A reader that branched on the flag alone would decode an error body as a payload the
moment that happens.

**`ErrorCode` discriminants are never renumbered and never reused.** Append inside the band.
`every_error_code_round_trips_through_its_discriminant` turns an insertion into a failing test
rather than a silent compatibility break.

**An error response carries the `end` its query would have had, and there is exactly one response
per index.** Do not set `end` on a failure, and do not answer a parked query on the spot.

**The release block runs on the `Err` path as well as the `end` path.** Both `next()`s use
`matches!(outcome, Err(_) | Ok((true, _)))` before the `?`, deliberately. A future `Err` path added
after the `?` would re-open item 60's leak.

**A read loop marks its connection dead before it sweeps, and a send checks the mark after it
registers.** Reversing either order opens the window where a query is registered on a connection
nobody is reading.

**`protocol.rs` and `protocol/error.rs` depend on `core`, `std::fmt` and `uuid`, and must not gain
rkyv.** That is what lets `ErrorCode` be shared with an archived payload without the framing
depending on the serialization it frames.

## Performance

Nothing on the success path changed. The response read does one extra branch on a byte it had
already decoded, `decode_response` is now `decode_server_frame` plus a type check — the same two
comparisons in the same order — and `ResponseAction` gained a variant, which rkyv encodes in a
discriminant that was already there.

`merge` gained two arms ahead of its existing ones, which costs a discriminant compare per merged
share on split gets only. `apply_failure` is one `Option` check per query at the tail of `handle`.
No benchmark was re-captured, because there is no number here to move: this is a path that only
runs when a query has already failed.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `every_error_code_round_trips_through_its_discriminant` | A code inserted into the enum renumbers the wire silently |
| `an_unknown_error_code_reads_as_unknown` | A newer peer's failure becomes a decode failure instead of a legible message |
| `an_error_frame_round_trips` | The frame layout drifts |
| `an_error_frames_query_id_sits_where_a_responses_does` | The client's shared preamble read starts reading a code as part of a uuid |
| `an_error_frame_shorter_than_its_own_fields_is_refused` | A short frame underflows its own length arithmetic |
| `a_message_over_the_message_bound_is_refused` | The error channel becomes an allocation channel inside the frame bound |
| `a_message_that_is_not_utf8_still_delivers_its_code` | A garbled message swallows the class of failure it came with |
| `an_error_share_wins_a_merge` | A split get hides one shard's failure behind three shards' rows |
| `an_error_response_never_succeeds_whatever_the_opts_say` | `QuerySuceededOpts` starts being able to turn a failure into a success |
| `a_truncate_leaves_an_error_alone` | A limit turns a failure into an empty get |
| `the_response_payload_lands_on_a_sixteen_byte_boundary` | The two reads are merged and the zero-copy response read ends silently |
| `an_error_frame_does_not_disturb_the_response_read` | The type dispatch breaks the alignment of the frame behind it |
| `an_error_frame_is_delivered_to_the_query_it_names` | An error frame decodes and is routed nowhere |
| `an_error_frame_for_an_unknown_query_does_not_end_the_read_loop` | One dropped stream kills every other query on that connection |
| `a_dead_connection_fails_the_queries_it_owed_and_no_others` | A dead connection either leaves its callers waiting forever, or fails healthy queries on other connections |
| `a_response_too_large_to_frame_is_answered_with_an_error_naming_the_query` | Item 61 returns: the server closes the socket with nothing on the wire |
| `a_response_too_large_to_frame_leaves_the_connection_serving` | One oversize response ends a connection serving many queries |
| `a_get_that_found_nothing_is_not_reported_as_a_failure` | An empty partition starts reporting as a failed query |
| `a_get_whose_partition_cannot_be_read_does_not_hang` (both tables) | A read failure is answered as an empty partition again |
| `a_get_whose_archive_is_missing_does_not_end_its_shard` (both tables) | The two storage failure classes collapse into one indistinguishable answer |
| `delete_survives_restart` | A pruned partition starts reporting as a failure, turning every delete into an error |

## Related

- [F10. Framing and protocol evolution](framing-and-protocol-evolution.md) — the header, the
  handshake and the reserved slots this fills in
- [D2. Framing and protocol evolution](../direction/framing.md#the-error-channel) — where this was
  designed, and what that page assumed that turned out to be wrong
- [Resolved: A response cannot say that a read failed](../appendix/resolved/response-error-channel.md)
  — items 56 and 61, and the remainder of 51
- [Resolved: A partition load that fails never releases its queries](../appendix/resolved/partition-load-failure.md)
  — the half of item 51 that landed first, whose *Still open* this closes
- [Wire Protocol](../architecture/wire-protocol.md) — the frame layouts as built
