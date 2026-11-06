# 364. A client dropped while its pool opened a connection logged an ERROR for it

[Resolved #303](dropped-client-dials-nothing.md) stopped a dropped client's pool from dialling, but
a connection already past its check, its socket connected and its handshake under way, went on to its
hand-off, found the proxy stopped, and was logged as an ERROR. Nothing failed: the connection was for a
client that no longer existed. But every `shoal-workload` run whose client went while its pool was
filling logged one or more, and a bench's log is where its failures are read.

Found in O156's runs on titan (2026-11-05) and fixed the next day, while the scans plan's *Found on
the way* was worked through.

## Symptom

- **Every `shoal-workload run` logged `ERROR ShoalConnectionManager::connect_to: ... failed to send
  to proxy: send to a half closed channel`** as its client went: 65 of them in O156's 48 runs on
  titan (`shoal-spike/results/o156/runs/*.log`, 2026-11-05), and one in a smoke run of the reference
  cell `macro/grid/unsorted/r50/1024` on europa.
- **The same line Resolved #303 named** ("every run of `shoal-workload` ended with four or five ERROR
  lines"), fewer of them: #303 stopped the redials, and what was left is the connections in flight at
  the drop.

## Cause

- **#303's guard is read once, as `connect` starts.** A connection already past it is somewhere in
  `connect_to`: its socket connecting, its TLS, hello or authentication handshake waiting on the
  server.
- **`Drop for Shoal` sets the flag and aborts the proxy at once**, the task every connection's read
  half is handed to.
- **The hand-off was inside `connect_to`**, after every handshake, and `connect_to` is
  `#[instrument(.., err(Debug))]`. A connection whose handshake finished after the drop met the
  stopped proxy, `send` failed with *send to a half closed channel*, and the span logged it at ERROR.

## Evidence

Found in the logs of O156's runs and **reproduced by a test** written before the fix,
`a_client_dropped_while_its_pool_connects_logs_no_error` (`shoal/tests/dropped_client_logs.rs`), in a
binary of its own since the subscriber that counts the client's ERRORs is process wide. It is #303's
scenario with that count: a client reaches a standalone node through a forwarder, every connection it
holds is cut, a query has its pool replace them while the forwarder holds each new connection a second
before its handshake is answered, the client is dropped meanwhile, and the client's ERRORs are counted
for three seconds after the drop. On the unfixed client:

```
assertion `left == right` failed: a client dropped while its pool connected logged 4 ERRORs for connections it no longer needed
  left: 4
 right: 0
```

## The fix

- **`connect_to` opens and shakes hands; it hands nothing over.** It returns the connection whose
  every handshake succeeded (`Opened`: the stream, the capabilities its hello granted, and what
  rustls kept of its session, held until the hand-off as before).
- **`hand_over` gives it to the proxy, outside `connect_to`'s logged span**
  (`shoal-client/src/client.rs`). It reads the flag first and refuses a dropped client's connection
  `ConnectError::ClientDropped`, as `connect` refuses one asked for after the drop, logged at no level;
  the socket is closed as it is dropped.
- **A hand-off that still meets a stopped proxy** (a drop after the check) is `ClientDropped` too, since
  the flag is set before the proxy is stopped. Only a proxy that ended some other way is an ERROR, now
  logged by `hand_over` itself.

## Alternatives rejected

- **Reading the flag inside `connect_to` and returning `ClientDropped` from it.** Its `err(Debug)` logs
  every `Err` it returns, so the line would stay, with another error in it.
- **Dropping `err(Debug)` from `connect_to`.** A connection that fails its connect or a handshake is a
  failure worth an ERROR, and the span is what names which endpoint failed how.
- **Joining the pool's tasks in `Drop`.** As #303 found, a drop cannot wait on async work, and the
  tasks are bb8's.

## Invariants to uphold

- **Nothing is handed to the proxy until every handshake has succeeded**, as before: `connect_to`
  returns no half-registered connection, so the next endpoint is tried safely.
- **Every `Err` `connect_to` returns is a failure of the connection**, never of its client's life; a
  dropped client is refused outside it.
- **The flag is set before the proxy stops** (`Drop for Shoal`), which is what lets a hand-off that met a
  stopped proxy be read as a drop.

## Still open

- **A connection in flight at the drop still finishes its handshakes** before it is refused, a few
  round trips its server answers for nothing. Cutting it short would need its handshake to watch the
  flag, which no caller waits on.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_client_dropped_while_its_pool_connects_logs_no_error` (`shoal/tests/dropped_client_logs.rs`) | A client dropped while its pool's replacements are in their handshakes logs an ERROR for each: four |
| `a_dropped_client_dials_nothing` (`shoal/tests/pool.rs`) | Unchanged, #303's: a dropped client's pool dials nothing after the drop |

## Related

- [Resolved #303](dropped-client-dials-nothing.md): the guard this one completes.
- [O156](../optimizations.md#o156-an-integer-sort-key-priced-beside-a-padded-string): the runs whose
  logs found it.
