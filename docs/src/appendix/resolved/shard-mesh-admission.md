# 15. No backpressure anywhere - the shard mesh half

The oldest open item on the known issues page, and a partial fix: the half that bounds what a
client can put on a shard's queue is built; the channels that are bounded by the work already
admitted are left as they are and said so. The remainder stays on the known issues page under
the same number.

## Symptom

Every channel was `kanal::unbounded_async`: the shard mesh, the per-client response channels,
the compaction jobs, the loaders. Sends never blocked, so no shard could deadlock on another -
but nothing throttled a client either. A shard that fell behind grew its mesh queue until the
process was killed, every query on it waited for as long as that took, and a client had no way
to learn that the shard was over capacity rather than slow. [F11](../../features/error-channel.md)
reserved `ErrorCode::Shedding` for exactly this and nothing produced it on the mesh.

## Cause

The mesh has to stay unbounded: a send between shards that blocked - a share, a released
query, a loaded partition - would be a deadlock waiting to happen, since the shard whose queue
is full may be the one whose progress empties it. The bound was never going to be on the
channel. What was missing was the decision at the one place unbounded work enters the mesh:
the coordinator routing a client's bundle.

## Evidence

**Reproduced.** `a_query_for_a_shard_that_fell_behind_is_shed` in `shoal/tests/backpressure.rs`:
two shards, a bound of eight, shard one held for two seconds through a test hook, two hundred
gets over distinct keys through a pool of twenty connections. Against the tree at `73ab29f`
with the bound neutralised:

```text
0 gets shed, the fastest in 0ns; 200 answered; 2.004663521s since the hold
thread 'a_query_for_a_shard_that_fell_behind_is_shed' panicked at shoal/tests/backpressure.rs:120:5:
no query was shed while shard one was held
```

Every query to the held shard waited the hold out on a queue nothing drained. With the bound,
three runs: 48 to 83 gets shed, the fastest refusal inside two milliseconds, the rest answered
- some after the hold, on the connections whose coordinator was the held shard itself - the
held shard answering a get once released, a shed query repeated to success by the client's
retry, and the transport view counting what was turned away.

## The fix

`networking.max_queued_queries`, sixty-four thousand by default and `#[serde(default)]` since
the committed `shoal.yml` predates it. When the coordinator routes a query and a share of it
is bound for another local shard whose mesh queue already holds that many messages, the whole
query is shed: its gather withdrawn, nothing sent, and the client answered
`ErrorCode::Shedding` in the query's own table variant at once, naming the shard and the
bound. A share for the coordinator's own shard is never shed - that loop is the one draining
the queue, and what waits on it has waited already. The shard counts what it shed on the
transport view (`ShardTransportView::shed`), `ShoalPool::hold_shard` holds a shard's loop for
a test, and the client's retry treats `Shedding` as it treats `NotLeader`: refused before
anything ran, safe to ask again.

The mesh channel itself is unchanged. The bound is on what clients may put on it; the traffic
between shards - shares, releases, loads, evictions - is bounded by what was admitted.

## Alternatives rejected

**A bounded channel.** A blocking send between shards is the deadlock the item's own text
warned of; the mesh stays unbounded so that no shard waits on another to make room.

**Bytes rather than messages.** `max_frame_bytes` already caps a query's size, so the count
times the frame bound is the byte bound; a byte count would need every message weighed on
the way in for a number the count already implies.

**Bound the loader, compactor and response channels too.** The loader holds at most one
request per partition parked on it, the compactor one job per rotation, and a client's
response channel one answer per query the mesh admitted - each is bounded by the work this
bound admits, and a second bound on them would refuse work already accepted.

**Shed the coordinator's own shares too.** A bundle that sat in a held coordinator's queue
for the hold and was then shed on dequeue because the queue behind it was still long had
waited for nothing; the first cut did this and the test's slowest refusal was the hold.

## Invariants to uphold

- **The mesh channel stays unbounded.** The bound is judged at admission and never by a
  blocking send.
- **A shed query has run nowhere.** Nothing was sent when the client is answered, which is
  what makes `Shedding` retriable.
- **A share for the coordinator's own shard is never shed.** The judgement is on other shards'
  queues; a coordinator that is over capacity is draining its own.
- **The default is the documented one.** `a_config_without_an_admission_bound_gets_the_default`
  pins sixty-four thousand.

## Still open

~~The remainder under item 15:
`PendingResponse` grows with arrival rate times fsync latency under a slow device, a rotation
releases every pending response at once, and a per-client response channel holds every answer
until the client's relay writes it - each bounded by admitted work, none by a number of its
own. And the bound counts every message on a queue, not only client queries, so a shard busy
with releases and loads sheds sooner than one busy with clients alone.~~ Closed by
[the remainder](backlog-bounds.md): `max_pending_writes` and `max_parked_queries` shed a query
before it commits or parks, and `max_queued_replies` stops reading a client that is not reading
its answers. The bound still counts every message on a queue, and that page says why.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_query_for_a_shard_that_fell_behind_is_shed` | `shoal/tests/backpressure.rs` | Nothing is shed while a shard is held, a refusal is not immediate, the held shard does not answer once released, a shed query is not retried, or the transport view does not count it |
| `a_config_without_an_admission_bound_gets_the_default` | `shoal-core/src/server/conf.rs` | The default moves off sixty-four thousand or the key is required |
| `a_retry_repeats_only_what_says_to_try_again` | `shoal-client/src/client.rs` | `Shedding` stops being tried again |

## Related

[F11. The error channel](../../features/error-channel.md), which reserved the code;
[D6. Connection pool](../../direction/connection-pool.md), whose bounded channels section
this closes; [Resolved #33](gather-expiry.md), the deadline that bounds how long a client
waits; [C2. Transport](../../distributed/transport.md), the peer lanes' byte bounds beside
this one.
