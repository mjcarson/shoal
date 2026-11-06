# 303. A dropped client's pool kept dialling its server, and logged each attempt as an ERROR

Dropping a `Shoal` stopped the proxy its connections are handed to, and nothing else. Its pool went
on opening connections for seconds, each one dialled, handshaken and subscribed, and then refused
by the stopped proxy with an ERROR line. Nothing a caller waited on failed, so it was seen only in
logs, on the lab and in committed ones.

Filed while reading lab 2's logs (2026-10-23) and fixed in wave three of the store optimizations
(2026-10-24).

## Symptom

- **Every run of `shoal-workload` ended with four or five ERROR lines**,
  `failed to send to proxy: send to a half closed channel`, logged by `connect_to`.
- **The server accepted as many connections, each closed at once.**
- **Lab 2's table path rounds on titan** logged them in eleven of twelve runs (2026-10-23,
  `target/lab/w2/vec/`), on the build before [F106](../../features/lane-copies.md) and the one after.
  They were 0.4, 0.8, 1.6 and 2.5 seconds apart.
- **[F85](../../features/devices-and-pools.md)'s committed object-load logs** have 50 of them
  (`shoal-spike/results/object-load/raw/`).

## Cause

- **The drop reached the proxy and not the pool.** `Drop for Shoal` set the client's shutting-down
  flag and aborted the proxy, the task that every connection's read half is handed to.
- **bb8 holds a pool for the connections it is opening.** It replenishes in tasks of its own: toward
  `min_idle`, in place of a connection returned broken, and to fill a node's pool built with
  `build_unchecked` ([F74](../../features/client-routing.md)).
  - Each task holds the pool strongly (`bb8` 0.8.6, `inner.rs:60-75`), so dropping the client's
    handle stops none of them.
  - Each calls the manager's `connect` again after a failure, with a delay doubling from 400 ms,
    until the pool's `connection_timeout` (`inner.rs:192-236`).
- **The manager could not see the drop.** The flag was made after the connection manager and was out
  of its reach.
  - So each attempt dialled, shook hands, subscribed to the topology, and failed at the hand-off to
    the stopped proxy.
  - `connect_to`'s `err(Debug)` logged every one at ERROR.

## Evidence

**Reproduced by `a_dropped_client_dials_nothing`** (`shoal/tests/pool.rs`), written before the fix.
Nothing in a server counts the connections it accepts, so the test puts a forwarder between a client
and a standalone node:
- the forwarder counts each connection as it arrives and holds it for a while before joining it to
  the server;
- the client is built through it with `min_idle` 4 and proved by a round trip;
- every connection it holds is cut, as a server going away would cut them;
- a query has its pool find them broken and replace them, each replacement held a second at the
  forwarder;
- the client is dropped while they are in flight, and the forwarder counts dials for three seconds.

On the unfixed tree, all three runs:

```
assertion `left == right` failed: a dropped client's pool dialled 4 times in the three seconds after it was dropped
  left: 4
 right: 0
```

## The fix

- **The flag is the manager's too.** `Shoal::connect`, which every builder ends in, makes the client's `Arc<AtomicBool>`
  before the connection manager, and the manager holds a clone
  (`ShoalConnectionManager::shutting_down`). Its clones reach every pool the client makes: the
  endpoint pool, the bulk pool, the router's template and each node's pool (`for_node` clones the
  manager).
- **A connection asked for after the drop is refused before any dial.** `connect` reads the flag
  first and returns `ConnectError::ClientDropped`, which it logs at no level. It is refused there,
  never in `connect_to`, whose every failure is an ERROR line.
- **bb8 goes on asking until its `connection_timeout`.** Each refusal costs no socket and no line.

## Alternatives rejected

- **Closing the pool.** bb8 0.8 has no `Pool::close`, and a replenishing task holds the pool whatever
  the client drops.
- **`retry_connection(false)`.** It is a setting of the whole pool, so a live client would lose its
  retries of an endpoint that refused it, which is how it fails over between endpoints.
- **The check in `connect_to`.** Its `#[instrument(err(Debug))]` logs every failure at ERROR, which is
  the noise this removes.
- **Waiting for the pool's tasks in `Drop`.** A drop cannot wait on async work, and the tasks are
  bb8's, not the client's.

## Invariants to uphold

- **One flag a client, shared by everything that opens its connections.** A manager made from another
  manager (`for_node`, a template) must keep the same `Arc`, never a fresh one, or a node's pool
  would dial on for a dropped client.
- **The flag is set before the proxy stops** (`Drop for Shoal`), so no connection is opened that its
  proxy will refuse.
- **`connect` refuses a dropped client before it opens a socket**, and the refusal is never an ERROR.

## Still open

- **bb8's task goes on asking until the pool's `connection_timeout`**, now at no cost but a sleep
  between refusals. It holds the pool's memory until then.
- **A result stream that outlives its client gets no new connection.** It never could: its answers
  went through the stopped proxy. It is now refused by name, where it was refused at the hand-off.
- ~~**A connection already past the guard at the drop** finishes its handshake, meets the stopped
  proxy at its hand-off and is logged as an ERROR by `connect_to`.~~ Resolved: the hand-off is outside
  that span and refuses a dropped client's connection quietly
  ([Resolved #364](dropped-client-connects-quietly.md)).

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_dropped_client_dials_nothing` (`shoal/tests/pool.rs`) | A dropped client's pool dials its server again while replacing connections it had found broken: four dials in the three seconds after the drop |

## Related

- [F74](../../features/client-routing.md): a node's pool is built unchecked and fills in the
  background.
- [Resolved #230](object-connection-held.md): an object operation's connection goes back to the pool
  once its frames are written.
- [Item 305](../known-issues.md#305-a-captures-1-mib-puts-across-hosts-stalled-whole-and-its-check-could-not-read-10-objects),
  filed in the same wave from the same lab's logs.
