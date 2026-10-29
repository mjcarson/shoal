# F114. A cancel cuts a stream as its read relay reads it

Since this feature, a cancel stops a stream of its bundle that has already begun on the connection
it arrived on. The stream stops between two of its frames as soon as the connection's read relay
reads the cancel, rather than once the shard coordinating the connection hands the cancel on.

## Context

Since [F75](client-cancel.md), a cancel's stream cut has run at the write relay, once the shard
coordinating the connection passes it the cancel. In item 321's loop (2026-10-26), a client kept
reading at memory speed after cancelling. In four of fifteen runs, beside five other suites, it was
sent the whole of a 16 MiB stream; in a fifth, 254 of its 257 frames
([O138](../appendix/optimizations.md#o138-a-cancel-cuts-a-stream-only-once-its-shard-hands-it-on-which-a-client-reading-at-memory-speed-outruns-under-load),
[Resolved #321](../appendix/resolved/cancel-row-waits-for-its-cancel.md)). Every answer was right,
but the bytes the cancel could have saved were written.

The cut waited for two things:
- **The shard.** The read relay hands a cancel to the shard's own queue, behind every bundle it read
  before it, so the cancel's bound covers exactly those arrivals. The shard tells the write relay
  only once it reaches the cancel, and under load that can come after the stream's last frame.
- **The write relay itself.** It yielded only when its outbox was empty or a write blocked. A write
  that does not block returns at once, so against a client reading as fast as it is written, the
  read relay, on the same executor, could not even read the cancel until the stream was over.

O138's entry named the way out when it was filed. A stream already begun when the read relay reads
the cancel answered an arrival before the cancel, so the relay can cut it then. The shard is left to
bound what has not begun.

## What it does

- **The read relay leaves the cancel for the write relay as it reads it.** It does this on a
  connection granted streams (`ReplyBacklog::note_cancel`, `shard.rs`), then hands the cancel to the
  shard as before.
- **The write relay acts on it before its next frame** (`write_replies`):
  - It holds the bundle's streams (`Outbox::hold`), so none of them opens or goes on.
  - It cuts the bundle's stream whose opener is out (`Outbox::cut_begun`). The cut is counted as a
    cut through `abandon`, as F75's cut is, and its clock released.
- **The shard's word does the rest, as it always did.** `Outbox::cancel` takes back every answer
  below the bound not yet written. The acknowledgement is written, and the hold is let go
  (`Outbox::release`).
- **The write relay yields between stream frames** once its task queue's turn is spent
  (`glommio::yield_if_needed`). This lets the read relay run under a long stream to a fast reader.
- **A write relay with nothing it may write waits on its channel** (`Outbox::writable`): an outbox
  of held streams alone is not something to write.

Nothing on the wire moved. The client is unchanged: it already ended a stream it was assembling at
the acknowledgement, whenever the stream's frames stopped.

## Design choices

- **Only a stream begun is cut at the read relay's word.** A stream whose opener is out when the
  relay reads the cancel answered an arrival before it. A client writes its cancels ahead of any
  other frame on the connection (`ConnShared::write`), so a retry's bundle is always read after its
  cancel. A stream not yet begun could in principle be anyone's, and the shard's bound, the
  coordinator's attempt counter, is what tells a retry's answer from a cancelled one's.
- **The bundle's streams are held until the acknowledgement.** A client keeps a cut stream open
  until an error frame under its id arrives. It takes a second opener under that id as a broken
  stream (`StreamFault::Duplicate`) and ends the connection. So nothing of the bundle opens between
  the cut and the acknowledgement:
  - the shard's word takes back what its bound covers;
  - what it does not cover opens after the acknowledgement. An answer that carried no attempt is the
    only kind that can be held and still owed.
- **Streams alone are held.** Whole frames of the bundle go on being written until the shard's word,
  as before this feature. A whole frame breaks no stream, and the outbox does not know a whole
  frame's bundle, so holding them would need every answer's id in the outbox.
- **A cell between the relays, not a message.** Both relays run on the connection's executor, so
  the read relay's note is a push onto a `RefCell` that the write relay takes at the top of its
  loop. An empty note costs the write relay one borrow.
- **A hold per cancel, counted.** Two cancels of one bundle read before either was acknowledged
  (a cancel, a retry and a cancel of the retry) take two holds. The first acknowledgement leaves the
  bundle held for the second.
- **A yield only once the turn is spent.** `yield_if_needed` gives the executor up only when the
  task queue's latency goal says it should. A stream to a client that reads it at once costs a
  check a frame and nothing more.

## Alternatives rejected

- **The read relay cutting the stream itself.** The outbox is the write relay's alone, and a stream
  in the write relay's hands is mid-frame: only the write relay knows where a frame ends.
- **Telling the write relay on its channel.** The channel is first in, first out, so the note would
  wait behind every answer queued ahead of it. A cell read before every frame is sooner. The channel
  also carries the shard's word, whose bound the read relay does not know.
- **Taking back the bundle's answers not yet begun at the note.** That needs the shard's bound,
  which only the shard knows. Held, they are taken back by the shard's word as they always were.
- **Acknowledging at the note.** An answer of a cancelled arrival can reach the write relay after the
  note and before the shard has recorded the cancel on the board. It would be written after the
  acknowledgement, which F75 promises never happens.
- **An unconditional yield after every frame.** That is a trip through the scheduler on every frame
  of every stream, where `yield_if_needed` gives the turn up only when another task is owed it.

## Limitations

- **Only a connection granted streams.** Any other connection has no stream to cut, and cancels as
  it did.
- **A frame being written goes whole**, and what the kernel already holds of the stream reaches the
  client: the cut is between frames, and its bytes are what the socket's buffers held.
- **Whole frames of the bundle go on until the shard's word**, as before this feature.
- **The work is as F75 left it.** A read already dequeued runs to its end, and the shard still
  refuses the reads it has not run.
- **The read relay must run to read the cancel.** A shard thread busy outside both relays, a long
  compaction or a blocking call, still holds the cancel unread.

## Invariants to uphold

- **Only a stream begun before the read relay read a cancel is cut at its note.** Anything not yet
  begun is the shard's bound's to judge.
- **No stream of a cancelled bundle opens between its cut and its acknowledgement.** Each note takes
  a hold, and the acknowledgement of each cancel lets one go.
- **The read relay notes a cancel before it hands it to the shard**, so the write relay acts on the
  note no later than on the shard's word, and before any answer of a retry.
- **Every cancel is still acknowledged exactly once**, after every frame of the arrivals it covers.
- **The write relay waits on its channel when nothing may be written**, and never asks again in a
  loop.
- **An empty note costs one borrow, and `CancelBoard::covers` stays one atomic load.**

## Performance

Two questions, each measured on the lab after the window that built this (2026-10-27). These are
A/Bs, not captures. F114 is `bde2927b`, beside the build before it, `b436b0f6`.

### What it saves

`tmdb-dataset-loader abandon --cancel true` ran on a side cluster of the lab's three hosts, F75's
setup ([What a cancel saves](client-cancel.md#what-a-cancel-saves)). The cluster was `abandon`, with
ports and directories of its own on the SSD leg's devices, bootstrapped afresh each round from the
side's node:
- europa, titan and hyperion at a factor of three, the driver on europa, one gigabit between them;
- the `performance` governor, and tmdb's nodes stopped, for the runs, both put back after;
- four rounds a side, the first side alternating;
- six workers over the three members, each asking for bundles of sixteen gets, reading the first
  answer and dropping the stream, and a seventh asking for one small movie at a time on europa's
  member, twenty seconds a run.

Each figure is the median of a side's four runs, with their range. The rounds' output is
`shoal-spike/results/stores-ab/f114-abandon.txt`.

| Bundle | Figure | Before F114 | F114 | |
| --- | --- | ---: | ---: | --- |
| 16 gets of 16 movies, about 16 MiB | Bundles dropped a second | 271 [264–282] | 286 [275–297] | 1.06, within |
| | Answer bytes written a bundle | 13.7 MiB [13.6–13.9] | 11.4 MiB [10.5–12.1] | 0.83, disjoint |
| | Bytes left unwritten a bundle | 2.2 MiB [2.1–2.3] | 4.3 MiB [3.7–5.1] | 1.99, disjoint |
| | Streams cut a bundle | 0.17 [0.16–0.18] | 0.38 [0.32–0.45] | 2.28, disjoint |
| | Foreground reads a second; p99 | 524; 12.0 ms | 639; 10.1 ms | within |
| 16 gets of 64 movies, about 64 MiB | Bundles dropped a second | 74.9 [74.5–75.2] | 77.7 [76.2–77.8] | 1.04, disjoint |
| | Answer bytes written a bundle | 47.4 MiB [47.1–47.5] | 41.5 MiB [38.6–42.2] | 0.88, disjoint |
| | Bytes left unwritten a bundle | 16.3 MiB [16.2–16.5] | 21.9 MiB [21.2–24.7] | 1.34, disjoint |
| | Streams cut a bundle | 0.31 [0.30–0.32] | 0.42 [0.40–0.47] | 1.36, disjoint |
| | Foreground reads a second; p99 | 139; 45.8 ms | 156; 45.7 ms | within |

- **A cancel now cuts the stream it finds begun.** At 16 MiB it cut more than twice as many streams a
  bundle. There the shard's word used to arrive after most of a bundle's answers were written, and
  F75 cut about a thousand streams a run. At 64 MiB the answers outrun any socket buffer, so the
  shard's word already found more of them waiting, and F114 adds a third.
- **The bytes it spares are those streams' rest.** A bundle wrote 17% less at 16 MiB and 12% less at
  64 MiB, every range disjoint.
- **The members spent what they spared on more bundles.** At 64 MiB the workers dropped 4% more a
  second, the ranges disjoint; at 16 MiB 6% more, within. The foreground moved within its noise.
- **Every answer read was right.** No run failed a get. The only refusals were a cancel's own, the
  queries it covered before they ran: 21 in one round before and 15 in one round after, at 64 MiB.

### What it costs

The yield is on every streamed answer's path, and every object get is a stream. So object gets ran
on the SSD leg's 2+1 pool (`f114-ssd-ec21`): 64 KiB and 1 MiB at one and sixteen in flight, two
captures a side in the order before, after, after, before, four runs an arm in each. Gets a second:

| Arm | Before F114 | F114 | | p50 ms | p99 ms |
| --- | ---: | ---: | --- | --- | --- |
| 64 KiB, one in flight | 543 [507–568] | 552 [520–567] | 1.02, within | 1.71 → 1.68 | 2.92 → 2.93 |
| 64 KiB, sixteen | 2,085 [2,053–2,116] | 2,107 [2,073–2,156] | 1.01, within | 7.73 → 7.62 | 13.7 → 13.6 |
| 1 MiB, one | 81.1 [76.7–82.4] | 78.9 [71.9–83.9] | 0.97, within | 10.5 → 10.6 | 20.1 → 20.1 |
| 1 MiB, sixteen | 131 [130–133] | 130 [128–134] | 0.99, within | 84.2 → 87.3 | 462 → 462 |

**The yield costs nothing these arms can see.** Each capture read its 2,048 objects back at its end.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal` `cancel::a_cancel_cuts_a_stream_its_shard_has_not_handed_on` | One shard, a 16 MiB answer in 64 KiB frames, the shard held while the client cancels and reads at memory speed: the whole stream, 257 data frames, is read while the shard sleeps. With the cut kept and the yield reverted it still passes, since the small socket blocks the writer |
| `shoal` `cancel::an_early_cut_opens_no_other_stream_of_its_bundle` | Two streams of one bundle, the first cut: the second opens before the acknowledgement (two openers). With the cut kept and the hold reverted, the client's connection would be ended by the second opener |
| `shoal-core` `outbox::tests::an_early_cut_takes_a_begun_stream_and_holds_the_rest_of_its_bundle` | The begun stream is cut two hundred bytes short, the bundle's waiting stream stays shut while another bundle goes on, and opens once the hold is let go |
| `shoal-core` `outbox::tests::an_early_cut_leaves_an_unbegun_stream_to_the_shards_cancel` | A stream not begun is not cut, two holds take two releases, and a release of a bundle never held changes nothing |
| `shoal` `cancel::a_cancel_cuts_a_stream_being_written` | Unchanged: item 321's row, which waits for the shard's word before it reads |
| `shoal` `cancel` (the rest) and `cluster_fixture` `a_cancel_follows_a_forward_to_the_node_that_holds_it` | Unchanged: F75's acknowledgement, refusals, retries and peer cancels |

## Related

- [F75. `Cancel` on the client wire](client-cancel.md), the cut this moves earlier.
- [O138](../appendix/optimizations.md#o138-a-cancel-cuts-a-stream-only-once-its-shard-hands-it-on-which-a-client-reading-at-memory-speed-outruns-under-load),
  filed from [Resolved #321](../appendix/resolved/cancel-row-waits-for-its-cancel.md).
- [F73. More than one frame for one query](bodies-across-frames.md), the streams it cuts.
