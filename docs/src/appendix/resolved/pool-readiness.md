# 38, 58, 88. The pool returned before its shards had bound, and never said what became of them

Three items with one cause. `ShoalPool::start` spawned its shard threads and returned, holding
their join handles and nothing else: no moment at which every shard had bound, no address they
had bound to, and no word if one of them died. Everything that started a server worked around
that in its own way, and each workaround was one of these items.

## Symptom

**58.** A shard that died was indistinguishable from one that ran cleanly. Its `Result` was looked
at once, at `exit`, and only logged — to whatever subscriber the embedding process had installed,
which for every test was none. Worse at startup than at runtime: a shard that could not bind, or
found no `tls` kernel module, returned `Err` from `Shard::new`, and `start` had already returned
`Ok`. The only symptom was a readiness probe timing out thirty seconds later with
`ConnectionRefused`, which names a closed port and nothing about why it is closed.

**38.** Every integration test binary handed out ports from a counter starting at 13000, per
binary. Cargo runs binaries in parallel, glommio sets `SO_REUSEPORT` on its listeners, so the
second bind of 13000 succeeded silently and the kernel load-balanced connections between two
servers from two tests with two schemas and two temp directories. Never observed to fail, because
the binaries were never alive on one port at the same moment — which is timing, not a guarantee.

**88.** The benchmark's readiness probe connected in a loop until the server answered, and every
attempt before the bind landed on a closed port. The client reported each at `ERROR`, the pool
opened ten connections, and a workload emitted at least a dozen `ERROR` lines before measuring
anything — several thousand per capture, none of them an error, and the one that would be an
error printed the same third line.

## Cause

`shard::start` built a `LocalExecutorPoolBuilder`, spawned one shard per cpu with
`on_all_shards`, and returned the `PoolThreadHandles`. Each shard bound its listener inside
`Shard::init`, on its own thread, after `Shard::new` had replayed its logs; nothing crossed back
to the thread that called `start`. So the pool could not know when a shard was answering (58 and
88 are that gap seen from two callers), and a caller had to choose the port before `start` so
that it could connect afterwards, which is where the counter came from (38). The port could not
be zero, because every shard binds the same port with `SO_REUSEPORT` and zero handed to each of
them would give each its own.

## Evidence

**58, established by reproduction** on the tree at `fe21187`, the commit before the fix, with a
throwaway test that held a port with a plain listener and started a pool on it:

```
cargo test -p shoal --test repro58
---- a_shard_that_cannot_bind_is_reported stdout ----
listening on 127.0.0.1:38789
listening on 127.0.0.1:38789
thread 'a_shard_that_cannot_bind_is_reported' panicked at shoal/tests/repro58.rs:25:5:
ShoalPool::start returned Ok on a port held by another socket; a client connecting to it got Ok(()) (the holder, not a shard)
```

`start` returned `Ok`, both shards failed their bind after printing `listening on` — the line
`Networking::to_addr` printed *before* the bind, which this fix also removes — and a client
connecting to the port reached the socket that held it. The kept test,
`pool::a_shard_that_cannot_bind_is_reported`, asserts what the fixed tree does instead.

**38, established by reading the source** when filed, and by the fix leaving nothing to read:
`PORT_COUNTER` and `get_unique_port` are gone from `shoal/tests/utils.rs`, and no test under
`shoal/tests/` names a port. The `listening on` line the item measured its table with is gone too.

**88, established by reproduction** when filed — twelve `ERROR` lines from
`shoal-workload run --id macro/insert_ephemeral --scale smoke` — and by re-running after the fix:
`macro/get_ephemeral` at smoke scale wrote one `ERROR` line, and that one is a connection the
client still held when the pool exited, not a probe attempt ([Still open](#still-open)).

## The fix

[F36](../../features/cluster-harness.md) gave the pool a channel. Every shard sends
`ShardEvent::Ready { shard, addr }` once `Shard::init` has returned — its listener bound, its
join broadcast, its loaders started; recovery had already replayed in `Shard::new`, so a ready
shard is an answering one — and the spawn closure sends `ShardEvent::Failed { shard, error }` if
`Shard::new` or `Shard::start` returns `Err`, before returning it. The shard id is minted in the
closure rather than in `Shard::new`, so a failure in construction still has a name.

`ShoalPool` holds the receiver and gained three methods. `ready(timeout)` collects one `Ready`
per shard, returns the first `Failed` as `ServerError::ShardFailed { shard, error }`, and reports
a shard that is merely slow as `ReadyTimeout { ready, of, timeout }`; it caches success.
`failure()` is a non-blocking `try_recv` for a death after readiness. `bound_addr()` is where the
shards are. `exit()` returns the first shard error, from either half of a join result — the
thread failing to join, or the loop returning `Err` — where before it looked only at the first
and logged it.

A configuration asking for port `0` gets a real port before any shard is spawned: `start`
creates a `socket2` socket, sets `SO_REUSEPORT`, binds `interface:0`, reads the port back and
writes it into the configuration. The reservation is held — bound, never `listen`ed, so the
kernel never routes a connection to it — until `ready` has counted every shard, then dropped.
Every test helper in `shoal/tests/utils.rs` now builds its configuration on port zero and waits on
`ready`; the crash test's child binds zero and its parent restarts on zero; `errors.rs` and
`framing.rs` take the address from `bound_addr`. The benchmark harness calls `ready` before its
query probe, so the probe's first connection lands on a bound socket.

## Alternatives rejected

**Returning the first shard error from `exit` and nothing else.** Item 58's own "smallest honest
version". It answers the question at shutdown, and the interesting question is asked while the
server is meant to be running. `exit` does it too, but it is the least of the three methods.

**A distinct port base per test binary.** Item 38's fallback. It moves the collision to the next
binary somebody adds, and the fixture F36 built starts any number of servers per test.

**Bind zero, read the port, close, pass it on.** The race [C11](../../distributed/testing.md)
names, and on a machine running every test binary at once not a theoretical one. The reservation
is the same idea with the close moved to after the shards have bound, and `SO_REUSEPORT` on both
sides is what lets the reservation and the shards hold the port together.

**Filtering the client's `ERROR` below `WARN` for the probe's duration.** Item 88's own fix
direction. It hides the lines rather than removing their cause, and it needs the non-global
subscriber path item 69 wants. With `ready` before the probe the lines do not happen.

**Having `shard::start` join every shard before returning.** The shards never return while the
server runs; the join is what `exit` is for.

## Invariants to uphold

- **`Ready` is sent after `Shard::init` returns**, never earlier. The listener binds in `init`
  and the first query after `ready` assumes it is there.
- **The reservation never listens.** `listen` would make it a member the kernel offers
  connections to, and every connection routed there would be dropped on the floor.
- **`SO_REUSEPORT` is set before the reservation's bind.** The shards' reuse-port binds are
  refused otherwise, and the failure is `EADDRINUSE` from every shard at once.
- **The shard id is minted in the spawn closure.** `Shard::new` must not mint it, or a failure
  there cannot be reported under any id.
- **`ready` reports the first `Failed` and stops.** A pool with one dead shard is not the server
  the configuration described, whatever the others are doing.
- **No test under `shoal/tests/` chooses a port.** A helper that takes a port takes zero; the
  address comes from `ready` or `bound_addr` afterwards.
- **`exit` returns the first error it saw and still logs every one.** A caller that ignores the
  result loses nothing it had before.

## Still open

- **A connection the client still holds when the pool exits is logged at `ERROR`** — `failing
  the queries a dead connection owed`, one line per held connection with an owed query. It is the
  third line item 88 quoted and the one line the fixed run still prints, and it is a real event:
  the server closed under the client. Whether a client that outlives its server should say so at
  `ERROR` is a client decision, filed nowhere yet because the line is honest.
- **The honest `exit` surfaced a compactor that dies on its first failed job**, which is
  [item 91](../known-issues.md#91-a-compaction-that-fails-ends-the-compactor).
- **`stage_join.rs` still names a fixed port** above the capture range, because
  `RunRequest::port` is resolved into the harness's address before the pool exists.
- **Item 16** — a shard that does not die at all — is the version of 58 worth more than any
  report of a death, and is where it was.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `pool::a_shard_that_cannot_bind_is_reported` | 58: `ready` on a held port returning `ShardFailed` naming the shard and `in use`, rather than `start` returning `Ok` and a client reaching the holder |
| `pool::a_port_of_zero_resolves_to_one_every_shard_binds` | 38: `bound_addr` resolved before a shard binds, `ready` reporting the same address, `failure` empty, a client served there |
| `cluster_fixture::fixture_reports_bound_endpoints_without_port_race` | 38 at scale: four servers in two clusters started at once, four distinct endpoints, all answering |
| Every helper in `shoal/tests/utils.rs`, `auth.rs`, `handshake.rs`, `tls.rs` and `pool.rs` | The two-second sleep, which is what every test would wait on again, and the counter, which is what they would collide on |
| `persistent_sorted_table::ack_survives_sigkill` | The crash test's child and its parent both on port zero, with no port passed between them |

## Related

- [F36](../../features/cluster-harness.md), which needed this first
- [C11](../../distributed/testing.md#the-cluster-fixture), which named the bind-zero race
- [C9](../../distributed/operations.md#readiness), which asked for `ready()` and `shard_failed()`
- [Item 16](../known-issues.md#16-panics-on-the-hot-path), the shard that should not die
- [Item 69](../known-issues.md#69-shoalctl-and-the-tests-still-install-no-tracing-subscriber),
  why the tests saw none of the logging
- [F8](../../features/purpose-built-workloads.md), whose probe stays for the question it answers
