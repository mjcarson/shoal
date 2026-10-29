# 316 (in part). A reader's read over an object lane waited for its answer with no bound

A get of a striped object reads each stripe's bytes from a holder its row calls current. A holder on
another node is asked over the object lane: the coordinating shard hands the request to an object
executor, which queues it on its link to the holder's slice and registers where its answer goes.
The answer comes back on the same lane under the request's id, or the link drops and refuses every
request it carried.

Twice on the lab, late in a 2+1 capture of the store comparison's second run, a get's read was
written on a lane that stayed up and no answer came. Nothing bounded the coordinator's wait, so the
get waited until its run ended, and nothing was logged.

This page is the half that is fixed: a reader's read is refused by name at a bound, and the refusal
says what the asking end saw. Which end held the request, and why, stays open as
[item 316](../known-issues.md#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound).

## Symptom

On the SSD leg (`shoal-ec21-drift`, 2026-10-26), a 1 MiB get at one in flight, started at 02:42:19.7
UTC in the third of `get100/s1m/c1`'s four runs, was never answered. Nothing else completed for the
rest of the run, so the run measured 407 gets where its others measured about 2,400. On the disk leg
(`shoal-ec21`), in the same arm's fourth run, a get started at 08:10:42 UTC and nothing completed
after it: 445 gets where the others measured about 1,800.

Each node's work in flight ([F108](../../features/object-work-in-flight.md)) said where it waited,
both times: europa ran one object task with one request waiting on one of its object links, while no
node sent or served a request and no served lane on titan or hyperion owed an answer. No node logged
anything about it.

## Cause

A coordinator waits for a holder's answer on the channel its request was sent with, with no bound:

```rust
pub async fn ask_holder(&self, holder: Holder, request: ChunkRequest) -> ChunkReply {
    match self.send(holder, request).await {
        Ok(answer) => answer_of(answer).await,
        Err(refused) => refused,
    }
}
```

`answer_of` waits for the answer or for the channel to close. The link holds the request's place
among its waiters until an answer arrives under its id, or the link drops and refuses them all. The
link drops when its connection does, and TCP ends a connection only when data goes unacknowledged
for `unacked_timeout` (5 s) or its keepalive probes go unanswered
([Resolved #310](peer-connections-keepalive.md)). Neither happens to a request that was delivered
and acknowledged and whose answer never came back, nor to one never handed to TCP.

A read of copies tries the next current holder only once a holder answers, and an erasure coded
gather (`erasure::read_into`) joins every position's read before it decodes. So one unanswered read
held its whole get.

## Evidence

**Established by the lab's captures and reproduced by a test.** The captures, their F108 figures and
the three nodes' journals are item 316's evidence, on [Known Issues](../known-issues.md#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound).

`a_read_whose_answer_does_not_come_is_refused_at_its_bound` (`shoal-core`'s `objects::lane::tests`)
was written before the fix. Two stores over a lane on loopback: B holds the answers its served lane
owes (`faults::hold_answers`), and A reads across the lane and waits. The hold stands in for an answer
that does not come: in the lab the holder owed nothing, but what the asker sees is the same, a lane
up, its request written, and no answer.

On the tree before the fix, with the transport's defaults since the bound did not exist yet:

```text
thread 'server::objects::lane::tests::a_read_whose_answer_does_not_come_is_refused_at_its_bound' (644581) panicked at shoal-core/src/server/objects/lane/tests.rs:1256:19:
a read whose answer was held was still waiting after 12.00009243s, its lane up
```

## The fix

**A reader's read is refused by name once its answer has not come within
`cluster.transport.object_read_timeout`**, ten seconds by default.
- **The asking link enforces it, on its object executor.** Each bare `ChunkRequest::Read` it queues
  carries its bound (`Bounded`). A sweep runs for each link while any bounded read waits: it sleeps
  until the soonest bound, takes every read past it from the link's waiters, and answers each
  `Unavailable`, "no answer to a read came from slice … within …".
- **The coordinator treats the refusal as a holder that did not answer.** A read of copies asks the
  next current holder, and an erasure coded gather decodes without the position.
- **A refused read is no longer counted waiting** in F108's figures, and is counted refused. Its
  gauge is given back before its reader hears.
- **A rebuild's, a move's and a scrub's reads and every write are not bounded.**
- **A bound under 100 ms is refused at the node's start.**

**The refusal says what the asking end saw**, at WARN once every ten seconds, with a count of the
rest:
- whether the link wrote the read: the frames written since it was queued reach its place;
- how many answers the link read since;
- the link's state and queue;
- the kernel's queues on its socket each way (`SocketView`, `peer/mod.rs`): bytes received and not
  read (`SIOCINQ`), bytes written and not acknowledged (`SIOCOUTQ`), bytes not yet sent, and the
  milliseconds since data was last sent and last received;
- since 2026-10-27, whether the kernel had anything for the link to read (`readable`, a zero-timeout
  `poll`): under kernel TLS the TLS layer may take a record's bytes off TCP's queue before it is
  read, so the queue reads empty while a record waits, and only `readable` says so.

Bytes left unread, or a socket readable, are this end's reader's to take: readable while it waits
is a wake it was not given. Bytes written and not acknowledged, or not yet sent, are the network's or
TCP's. Neither, with the read written and nothing readable, means the holder acknowledged
everything sent and sent nothing back.

**An answer that comes after is said to be late**, ~~at INFO~~ at WARN since 2026-10-27, so a node
logging at WARN, as the lab's do, keeps it, with how long after its read was queued.
A late answer means the read was held, not lost. A link remembers its last 1,024 refused reads, and
forgets them when it drops.

The test's warning, its holder's answers held:

```text
WARN shoal_core::server::objects::lane: msg="a read sent over an object lane had no answer within its bound and was refused, so its reader asks another holder" node=87d69e93-… slice=01a1790a-… id=2 chunk=ChunkId { consumer: 1, pg: PlacementGroup { tablet: 1, sub: 0 }, object: 80, stripe: 0, position: 1 } offset=0 len=262144 waited_ms=500 written=true frames_written_since=1 answers_read_since=0 still_waiting=0 state=up queued_bytes=0 socket=Some(SocketView { unread: 0, unacknowledged: 0, unsent: 0, retransmits: 0, total_retransmits: 0, last_sent_ms: 500, last_received_ms: 500, last_ack_ms: 459, rtt_us: 5177 }) refused_besides=0
INFO shoal_core::server::objects::lane: msg="an answer came for a read an object lane refused at its bound" node=87d69e93-… slice=01a1790a-… id=2 after_ms=503 late_besides=0
```

Written, acknowledged, and nothing unread: the holder's end held it, as the test's hold does.

## Alternatives rejected

- **A timeout around the coordinator's wait**, in `ask_holder`. The store's handle waits on a kanal
  channel and works on any runtime, and a test thread asks it outside glommio; a timer there needs
  the caller's executor. It would also leave the read counted waiting on the link, and it sees
  nothing of the link or its socket, which is what says where the read was held.
- **A timer task for each read.** At thousands of reads a second, each held ten seconds, tens of
  thousands of timers would be pending at once. A sweep for each link runs only while a bounded read
  waits, and wakes about once a bound.
- **Bounding every request.**
  - A rebuild's, a move's and a scrub's read waits behind its holder's foreground and its device's
    ceiling, which under load is long by design.
  - A stage refused at a timeout would be discarded without a definite refusal or a committed fact,
    which the write path forbids.
  - A put already gives up on a holder at its own deadlines (`straggler_wait`).
- **Dropping the link at the bound**, so its connection is dialled again. It would refuse every other
  request the link carries, answered or about to be, for one held answer. It would also lose
  whether answers after the read came on the same connection, which is half of what tells the ends
  apart.
- **A shorter bound.** The slowest whole get the comparison's second run measured took 6.8 s, 16 MiB
  at sixteen in flight on the SSD leg, and one holder's answer is a part of a get. A bound under a
  loaded lane's real answers would refuse live holders, and send their reads to holders no less
  loaded. Ten seconds is a sixth of a client's bound on an attempt that makes no progress
  ([Resolved #308](an-object-attempt-gives-up.md)), so the read reaches another holder well inside
  it.

## Invariants to uphold

- **Every request is still answered once.** A read is taken from the link's waiters before it is
  refused, so its answer, if it comes, finds no waiter and is let go.
- **Only a bare read is bounded.** A request wrapped in its class (`Background`, `Scrub`) and a write
  are never refused by the bound. A reader's read that should be bounded is sent as a bare `Read`.
- **The sweep never keeps its link alive.** It holds a weak reference, ends once the link is gone or
  nothing it bounds waits, and holds no borrow of the waiters across its sleep.
- **The bound is the asker's alone.** The holder still answers, and its late answer costs the lane a
  frame read.

## Still open

- **Which end held the request, and why.** Item 316 stays on Known Issues for that. The refusal's
  warning now says what the asking end saw at the moment it happened, and a late answer's line says
  when the answer came.
- **A local read has no bound.** A holder on the coordinator's own node is asked through its
  executor's mailbox, and waits on its device.
- **A rebuild's, a move's and a scrub's reads wait as before.** A held answer stalls a rebuild until
  its link drops.
- **A get held this way is answered about ten seconds late**, where before it was never answered.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_read_whose_answer_does_not_come_is_refused_at_its_bound` (`shoal-core`, `objects::lane::tests`) | A read across a lane whose holder holds its answer is still waiting after twelve seconds. With the bound kept and the rest reverted: the refused read is still counted waiting, the next read is not answered, or a rebuild's read is refused at a reader's bound |
| `a_sockets_view_says_whether_it_is_readable` (`shoal-core`, `peer::tests`) | A view that does not say a socket with bytes waiting is readable, or says an idle or drained one is |
| `answers_ready_together_go_in_one_write_within_a_byte_bound` (`shoal-core`, `objects::lane::tests`) | Unchanged: forty answers held together and let go are each answered, inside the default bound |
| `a_lane_whose_answers_cannot_be_written_ends` (`shoal-core`, `objects::lane::tests`) | Unchanged: a lane that ends refuses what it carried by name ([Resolved #307](a-lane-whose-writer-stops-ends.md)) |

## Related

- [Item 316](../known-issues.md#316-a-gets-request-on-an-object-lane-went-unanswered-and-its-coordinator-waited-with-no-bound),
  the open half: the end that held the request.
- [Resolved #308](an-object-attempt-gives-up.md): a client's attempt that makes no progress gives up
  by name, the same kind of bound one step out.
- [Resolved #307](a-lane-whose-writer-stops-ends.md): a served lane whose answer cannot be written
  ends, so its asker's requests are refused.
- [Resolved #310](peer-connections-keepalive.md): a connection whose peer vanished while it owed
  nothing is probed and ended.
- [Resolved #238](object-lane-backpressure.md): a request waiting for room at a full link, refused
  only once the link wrote nothing for `STALL_AFTER`.
- [F108](../../features/object-work-in-flight.md): the figures that said where the get waited.
