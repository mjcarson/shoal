# F75. `Cancel` on the client wire

A client that stops reading a bundle now tells the server. `Cancel`, message type 12, was reserved
at [F10](framing-and-protocol-evolution.md) and refused by every server since: a frame of that type
ended the connection. Now a result stream that ends before its answers are all in - dropped, timed
out at the client's deadline, or ended by an error - sends one `Cancel` for its bundle on every
connection still owing it answers. The server takes back every answer of the bundle it has not
written, cuts a streamed answer it has begun between two of its frames, answers `Cancelled` instead
of running any of the bundle's reads still waiting on a shard - on its own node, or on a node it
forwarded them to - and acknowledges the cancel with one error frame. A write is never stopped: it
runs, and only its answer is dropped. A cancel covers the arrivals of the bundle on its connection
that came before it and none after, so a retry under the same id is answered in full.

## Context

This is the optional S1 prerequisite "`Cancel` on the client wire"
([S1](../object-storage/prerequisites.md#optional)): a reader abandoning a range it no longer
wants. The object store does not need it to be correct - a reader that seeks away stops by not
asking for the next range ([S12](../object-storage/wire-and-client.md#ranged-frames)) - so what a
cancel buys is the tail of the range in flight, and on today's tables the whole of an answer a
reader left. [F11](error-channel.md) had already removed the reason D6 once gave for needing it: an
answer nobody waits for is a `WARN` and a `continue` on the client, not a leak. What was left was
work and bytes, and the design sketch in [TODOs](../appendix/todos.md#cancel-and-what-it-would-actually-buy)
named two depths of it - dropping answers at the connection, and stopping the work at the shard -
and said the cheap one does not buy what the expensive one is for. Decided with the user: the full
depth, connection, shard and peer.

What was there:

- **A discriminant and nothing behind it.** `MessageType::Cancel = 12`, refused by
  `decode_client_request`, so the read relay ended the connection on one.
- **A bundle id, not a query id.** `Queries::default` mints one id for the bundle; every answer
  of it carries that id and an index ([F11](error-channel.md#limitations)). A query stream gives
  each bundle an id of its own ([Resolved #138](../appendix/resolved/stream-bundle-identity.md)).
- **An attempt per arrival.** The shard coordinating a connection mints every bundle it routes an
  attempt from one counter that only rises ([F41](read-consistency.md)), carried on every query's
  metadata, on every forward, and - once F75 passed it through - on every answer.
- **Streamed answers** ([F73](bodies-across-frames.md)): an answer longer than one data frame is
  an opener and data frames, which a client that left was sent to the last byte.
- **A connection returned to the pool as soon as its frames are written**, so on the client
  nothing held a bundle's connection by the time its stream was dropped.
- **A mesh of FIFO queues.** A message broadcast to every shard lands behind whatever is queued
  there, including the very query it means to stop.

## What it does

### The frame and the capability

`shoal-proto/src/shared/protocol/cancel.rs`: a cancel is a header of type 12 with no flags and a
sixteen byte body, the bundle's id. A client sends one only on a connection whose hello asked for,
and was granted, `CLIENT_CAP_CANCEL` (bit three of the capability byte, beside streams, read options
and leader hints); a server that did not grant it ends the connection on one, as every server did.
The flags stay clear and `check_cancel` refuses any flag or length but the cancel's own, so a
per-query cancel later is a flag and a capability of its own, never a guess. The client lane's
version stays at 4.

The server answers every cancel it reads with exactly one `Error` frame of the new code
`Cancelled` (33) under the bundle's id. It is the last frame the cancelled arrivals produce on that
connection: nothing of theirs is written after it.

### What a cancel covers

A cancel applies to every arrival of its bundle on its connection that came before it. The read
relay hands it to the shard coordinating the connection on that shard's own queue, behind every
bundle the relay read before it, so when the shard handles it, its attempt counter bounds exactly
those arrivals: each has a lower attempt, and a bundle sent again under the same id afterwards - a
retry, or a pinned identity - is minted a higher one. The cancel is recorded as that bound,
`before`, and every check compares an attempt against it. Attempt zero, an answer whose attempt
was not carried, is never covered.

### The board, and where work stops

`shoal-core/src/server/cancel.rs` holds the node's `CancelBoard`: every recorded cancel by the
connection and bundle it names, shared by every shard of the node through `Comms`. The
coordinator records the cancel there, with a life of twice the server's query deadline. A shard
reads it:

- at the top of `handle_query`, before the query is decoded, which is the check that skips work
  still waiting in a queue;
- in `handle_read_ready`, after a strong read's barrier and apply waits and before the read runs;

and a read it covers is answered `Cancelled` through the same path every refusal takes, never
dropped. So a share fails its gather's slot and the gather completes, a peer's forwarded query is
answered down its lane and frees the lane's byte budget, and the client's relay drops the answer
like any other of the bundle. A write it covers runs as it would have, and its answer, which
carries its attempt like every other, is dropped by the relay. The check is one atomic load while
nothing is recorded.

The board is read rather than told by message because the mesh is FIFO: a cancel broadcast to the
shards would land behind the queries it names, and every one would run first. Entries are swept on
the shards' ticks, a connection that ends takes its own with it, and a board holding 65,536
records nothing more: that cancel still stops the bundle's answers, and its work runs.

### The relay: answers taken back, a stream cut, one acknowledgement

Once it has recorded the cancel, the coordinator sends the connection's write relay an instruction,
`ReplyKind::Cancel { before }`, down the same channel the answers arrive on. The relay takes back
every answer of the bundle below the bound that it holds and has not written - whole answers, and
streams not yet begun - and cuts a stream it has begun between two of its frames
(`Outbox::cancel`), releasing each one's latency clock without counting it as an answer. Then it
writes the acknowledgement, before it takes anything queued after the instruction, which is what
puts the acknowledgement ahead of every frame of a retry. An answer of a covered arrival that
reaches the relay later is dropped the same way. A client knows a cut stream is over from the
acknowledgement, which ends the stream it was assembling.

The read relay now waits for room under `networking.max_queued_replies` after it reads a frame's
header rather than before, and never for a cancel: a connection that owes its bound still has its
cancels read, since what it owes is what a cancel takes back.

### Across nodes

A coordinator that forwarded shares of the bundle passes the cancel on: one `PeerCancel` - the
bundle and the bound - on the data lane to each node owed a share below the bound, behind the
forwards it names, to a peer whose lane negotiated the optional capability `CAP_CANCEL_V1`. Each
data lane carries one origin shard's forwards, and that shard minted every attempt on it, so the
origin's own bound is the right one at the far end. The receiving lane hands it to its shard, which
records it on its node's board under the lane's connection; the shares it has not run are answered
`Cancelled`, so the lane's accounting settles as it always does, and the origin drops those answers.
A peer without the capability is sent nothing: its shares run and their answers are dropped at the
origin, as a cancel dropped them before it reached peers. No wire version moved and nothing has to
be activated.

### The client

`shoal-client/src/client/cancel.rs`. A pooled connection's write half is now shared: a
`ConnShared` behind every `ShoalConnection`, entered by its id in a registry every pool's
manager shares, its writer behind a lock every frame written on the connection takes. When a result
stream ends early, `cancel_owed` gives its slot back and, for every connection its `Owed` says
still owes it answers, queues the cancel on that connection and spawns a write of it. Whoever next
takes the connection's lock - the next bundle, an admin request, or that write - writes every
queued cancel before its own frame, so a cancel always reaches the wire ahead of a retry sent on the
same connection. A query stream's result stream cancels every bundle still owed answers under its
flag. A connection whose server granted no cancels, or that the pool let go, is sent nothing.

The acknowledgement is never handed to a caller: a retry may hold the id by then. The reader ends
any stream of the bundle it was assembling and goes on, and a frame for a bundle cancelled lately
that arrives with nobody waiting is logged at `DEBUG` rather than as the orphan `WARN`.

### Choosing it

`ShoalBuilder::cancel_abandoned(false)` builds a client that only forgets an abandoned stream, as
every client did before F75. It is on by default; the switch is what lets one build measure what a
cancel saves.

### Seeing it

Each node counts its cancels in `NodeStats.cancels` - received, forwarded, queries refused, answers
dropped and their bytes, streams cut, and cancels the board could not record - with rates for the
first, the refusals and the bytes, left out of the frame while nothing was cancelled. shoaladm's
stats view charts `cancels/s`, `cancelled queries/s` and `cancelled bytes/s` in its queries tab, and
`tmdb-dataset-loader abandon` drives readers that stop reading through a deployed cluster, with and
without cancels, and prints what the members wrote and were spared.

## Design choices

- **A bundle, not a query.** Every caller that abandons anything abandons a result stream, which
  is one bundle, or one bundle at a time for a query stream. An index per query would need one on
  the wire and a lookup per answer, for no caller.
- **An attempt bound, not a set of cancelled ids.** A set keyed by bundle id would cancel a retry
  sent under the same id after the cancel, which `exec_with` does on every retry of a bundle that
  may have applied. The order of frames on one connection, and the coordinator's counter, make
  "before" exact.
- **A cancel never stops a write.** What a write does is what its sender wanted, and a sender
  that stopped waiting - a timeout around a send, a stream dropped without reading its answers -
  has not taken it back. Stopping it would silently lose a write a caller sent, which the
  fixture's paused-server test caught the first time F75 tried: a write raced against a two second
  timeout while the server was paused had to land once it resumed, and was refused. Its answer is
  still dropped, and a write that has to be withdrawn is a delete.
- **Refuse, never drop, at the shard.** Every structure that waits on a query - a gather's slot, a
  pending forward, a lane's byte budget, a meter clock, a backlog count - is settled by its answer.
  A cancelled query answered `Cancelled` settles all of them the way they already settle, and the
  relay drops the answer.
- **A board read across threads.** The only way a cancel overtakes a query still queued on a busy
  shard - the case it exists for - and one atomic load on the query path while it is empty,
  `server/faults.rs`'s shape.
- **Every cancel acknowledged, once.** Even one that covered nothing. The acknowledgement tells a
  client that a stream it was assembling is over, and a client that reads its cancels' answers in
  order knows nothing of the bundle follows; a cancel that might or might not be answered could not
  be relied on for either.
- **An optional peer capability, not wire version 8.** A data lane's reader refuses any frame but a
  forward by ending the lane, so a peer built before F75 must never be sent a cancel. The
  capability says exactly that at the hello, the way `CAP_PRE_VOTE_V1` does, and a mixed cluster
  works with no activation.
- **A shared write half on the client.** A send hands its connection back once its frames are
  written, and bb8 cannot check out a particular connection, so a cancel reaches the write half
  through a registry and a lock. One uncontended lock a frame.
- **Every answer carries the coordinator's attempt**, the flushed path's too: a standalone
  table answers a write once it is durable, from a tuple that carried no attempt until F75, and an
  answer with none is never covered, so such a write's answer arrived after the acknowledgement.
- **Cancel on an early end, not only on a drop.** A stream that ended on the client's deadline or
  an error leaves the server work nobody will read, which on one shard no deadline check reaches.

## Alternatives rejected

| Alternative | Why not |
| --- | --- |
| Dropping answers at the relay alone | The work still runs: the case a cancel exists for is a server grinding on queries nobody reads, and only stopping them at the shard addresses it |
| A cancel broadcast to every shard's queue | The mesh is FIFO, so it lands behind the queries it means to stop and can never skip one still queued |
| A set of cancelled bundle ids | Cancels a retry under the same id sent after the cancel; the attempt bound does not |
| Dropping a cancelled query silently at the shard | A gather would wait out its deadline for the slot, a forward its timeout, a peer lane's budget would never free |
| Refusing a released parked query | A parked get keeps the rows it found in `PendingGets` under its attempt, and refusing its replay leaks them |
| Refusing a write a cancel finds before it runs | A caller that stops waiting for a write has not withdrawn it, and a write dropped with its stream would be lost without a word; after it is proposed its outcome is unknown, so a refusal would claim what nobody knows |
| A per-query cancel, an index in a flag's section | No caller abandons one query of a bundle; the flags are kept clear so it can be added later |
| Wire version 8 for the peer frame | Needs an activation for nothing a capability does not already say at the hello |
| A writer task per client connection | Moves every write behind a channel hop to buy what one lock buys |
| A cancel on any connection to the node, looked up by bundle id node-wide | Lets one connection cancel another's work, and loses the order of frames that makes "before" exact |
| A stream the server pushes until told to stop, with credits | [S12](../object-storage/wire-and-client.md#alternatives-rejected)'s rejection stands: ranges asked for one at a time need neither |

## Limitations

- **Only reads are stopped.** Every write a cancel covers runs, and only its answer is dropped,
  so a cancel of a bundle of writes saves bytes and no work.
- **Work already running is not interrupted.** A read a shard has dequeued runs to its end; a
  strong read's barrier and apply waits run to their deadline before the read is refused; a read
  parked on a partition load replays and runs.
- **A bundle still being streamed in is not covered.** An arrival is the coordinator dequeuing the
  bundle, so a cancel read while its body is assembling covers nothing. This client never sends
  one: a send returns only once its frames are written.
- **An entry lives twice the server's query deadline**, so work queued longer than that runs, and
  its answers are written; the client drops them as frames of a bundle it cancelled.
- **A full board** - 65,536 cancels within one lifetime - records nothing more: answers stop,
  work runs, counted as `unrecorded`.
- **A cancel behind a bundle waiting for room waits too.** The read relay waits for room after
  a bundle's header, so a cancel written after one that is waiting is read when the bundle is.
- **Cross-thread visibility is best effort.** A shard that read the board an instant before the
  record runs the query, and its answer is dropped.
- **A peer cancel reaches only a link that is up and negotiated the capability**, and a share
  rerouted to another holder after it - a stale route, or a lost link - is not followed.
- **A client that leaves cancels nothing.** Its queued work runs and its answers go nowhere, as
  before ([TODOs](../appendix/todos.md#cancelling-a-departed-clients-work)).
- **A cancel can save nothing.** One sent for a bundle whose answers are all in flight costs a
  frame each way; a stream whose answers had all arrived sends none.
- **A client with no runtime left** queues its cancels on their connections, and they go with the
  connection's next frame, if there is one.

## Invariants to uphold

- **One shard coordinates every arrival on a connection, and its attempt counter only rises.** A
  cancel's bound is that counter's value when the cancel is handled; a second coordinator, or a
  counter that restarts, makes "before" mean nothing.
- **A client's cancel reaches its coordinator on the coordinator's own queue, behind the bundles
  read before it.** Delivered any other way it could be handled before the arrival it names.
- **Attempt zero is never covered.** A path that carries no attempt weakens a cancel; it must
  never stop a retry. Every reply that can be carried an attempt carries the coordinator's.
- **A cancelled read is refused, never dropped, and a write is never refused by a cancel.** The
  check at `handle_query` asks `archived_is_write` before it refuses anything.
- **The acknowledgement is written before anything queued after the instruction**, and every
  cancel is acknowledged exactly once.
- **An empty board costs one atomic load.** Anything added to `CancelBoard::covers` before that
  load is on every query's path.
- **A peer is sent a cancel only on a data lane that negotiated `CAP_CANCEL_V1`**, and the bound
  is the origin shard's own. One lane carries one origin shard's forwards.
- **Every frame a client writes on a pooled connection goes through `ConnShared::write`**, which
  writes queued cancels first. A write that bypassed it could put a retry ahead of its cancel, or a
  cancel in the middle of a frame.
- **A client sends a cancel only on a connection granted `CLIENT_CAP_CANCEL`**, and never hands
  the acknowledgement to a caller.

## Performance

_The lab before and after is measured against the commit that delivers F75 and recorded here in
the one after it, since a figure quoted from a tree no commit holds cannot be found again._

## Tests

| Test | What breaks if F75 is reverted |
| --- | --- |
| `shoal` `cancel::a_cancelled_bundle_is_refused_before_it_runs` | one held shard, 32 gets and their cancel written together: one acknowledgement and nothing else, 32 queries refused and 32 answers dropped; before F75 the cancel ends the connection |
| `shoal` `cancel::a_cancelled_write_is_still_applied` | a write and its cancel while the shard is held: only the acknowledgement written, the row there afterwards, nothing refused and one answer dropped - the write's, which arrives after the acknowledgement unless the flushed path carries its attempt |
| `shoal` `cancel::a_retry_after_a_cancel_is_answered` | the bundle, its cancel and the bundle again under the same id: the acknowledgement, then the retry's answer; a set of cancelled ids would drop the retry |
| `shoal` `cancel::a_cancel_cuts_a_stream_being_written` | a 16 MiB answer in 64 KiB frames cut between two of them, no `LAST`, the acknowledgement after its last frame, more than 8 MiB unwritten |
| `shoal` `cancel::a_cancel_is_read_while_answers_are_owed` | 64 answers owed past a bound of 8: the cancel recorded before the client reads a byte, and fewer than 64 answers written |
| `shoal` `cancel::a_cancel_without_the_capability_ends_the_connection` | a connection that did not ask for cancels is granted none and ended by one |
| `shoal` `cancel::every_cancel_is_answered_once` | a cancel of a bundle long answered, and of an id never sent: one acknowledgement each, nothing refused |
| `shoal` `cancel::a_split_get_settles_after_a_cancel` | a get split over two held shards: every share refused, only the acknowledgement written, no gather left resident |
| `shoal` `cancel::a_dropped_result_stream_cancels_its_queued_work` | a stream dropped through the real client while the shard is held: 32 gets refused; with `cancel_abandoned(false)` none |
| `shoal` `cancel::a_timed_out_try_is_cancelled_and_its_retry_answered` | a try timed out at the client is cancelled and refused, and its retry under the same id answered |
| `cluster_fixture` `a_cancel_follows_a_forward_to_the_node_that_holds_it` | two nodes at a factor of one, the holder held: its 16 forwarded gets refused on it, one peer cancel sent, 16 answers dropped at the origin, one acknowledgement |
| `shoal-core` `cancel::tests::{an_empty_board_takes_the_fast_path, only_attempts_before_it_are_covered, attempt_zero_is_never_covered, a_later_cancel_raises_before, entries_expire, a_full_board_records_nothing, forget_client_is_scoped}` | the board's bound, its expiry, its cap and its scope |
| `shoal-core` `outbox::tests::{cancel_takes_unstarted_answers_of_one_id, cancel_cuts_an_open_stream_and_opens_the_next, cancel_leaves_other_ids_and_later_attempts}` | what a cancel takes back of a connection's queue, and the stream of a retry opened in a cut one's place |
| `shoal-core` `meter::tests::an_abandoned_answer_releases_its_clock` | an unwritten answer releasing its frame's clock uncounted, and the cancel counters |
| `shoal-core` `control::stats::tests::tracker_derives_cancel_rates` | the cancel rates and totals, and a node nothing was cancelled on writing none |
| `shoal-client` `client::tests::a_dropped_stream_cancels_on_every_connection_that_owes_it` | a cancel on each connection owing answers and on no other, none for a settled stream, none from a client built not to |
| `shoal-client` `client::tests::a_connection_without_the_capability_is_sent_no_cancel` | a cancel kept off a server that would end the connection, and a let-go connection leaving the registry |
| `shoal-client` `client::tests::a_queued_cancel_goes_ahead_of_the_next_bundle` | the ordering that puts a retry behind its cancel |
| `shoal-client` `client::tests::a_cancelled_error_frame_is_never_delivered` | an acknowledgement read past, never handed to a waiter holding the id |
| `shoal-client` `client::tests::a_query_streams_unsettled_bundles_are_cancelled` | a query stream's own bundles cancelled, another stream's left alone |
| `shoal-client` `cancel::tests::recent_cancels_are_bounded` | the set that quiets late frames of a cancelled bundle |
| `shoal-proto` `cancel::tests::{a_cancel_frame_round_trips, a_cancel_of_the_wrong_shape_is_refused}` | the frame, and a flag or length refused |
| `shoal-proto` `protocol::tests::{client_caps_include_cancel, a_client_cancel_is_accepted_by_the_client_decoder, every_error_code_round_trips_through_its_discriminant}` | the capability bit, the decoder letting a cancel through, `Cancelled` pinned at 33 |
| `shoal-proto` `peer::tests::{cap_cancel_is_optional, a_peer_cancel_round_trips}` | the peer capability optional at bit seven, and the peer frame |
| `shoal-proto` `stats::tests::node_stats_from_before_f75_decode` | figures from before F75 decoding, none written while nothing was cancelled |
| `shoaladm` `metrics::tests::every_metric_and_column_has_help`, `view::tests::every_metric_reaches_the_view` | the three cancel metrics charted and explained |

## Related

[TODOs](../appendix/todos.md#cancel-and-what-it-would-actually-buy), where the two depths were
weighed; [F10](framing-and-protocol-evolution.md), which reserved the type; [F11](error-channel.md),
the error frame it is answered with; [F41](read-consistency.md), the attempt it is bounded by;
[F73](bodies-across-frames.md), the streams it cuts; [F74](client-routing.md), the runs it follows to
every connection; [Resolved #60, 130, 131](../appendix/resolved/stream-connection-accounting.md),
whose server half it is; [S12](../object-storage/wire-and-client.md#ranged-frames) and
[S1](../object-storage/prerequisites.md#optional), the prerequisite it closes.
