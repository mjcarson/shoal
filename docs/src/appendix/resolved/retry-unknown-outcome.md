# 125. A retried write whose first try was `OutcomeUnknown` reported the last try's refusal

## Symptom

`Shoal::exec_with` with a retry budget could report a write that may have applied as one that
definitely did not. Suppose the first try came back `OutcomeUnknown`, meaning the write was
accepted and whether it applied is not known, and the retry was then refused by name, for
example with `StorageWrite` or `Shedding`. The caller received the refusal. It read "this did
not apply", when the first try may have applied, and on a cluster node might still commit after
the refusal.

Since [Resolved #122](intent-log-failure.md), a standalone table produces this sequence
directly. The write behind an intent log that failed is `OutcomeUnknown`, and its retry is
refused `StorageWrite`.

## Cause

The retry loop kept no memory of earlier tries. When it stopped, it returned the failure that
stopped it, whether that was a failure it would not retry or the last one once the budget ran
out. `OutcomeUnknown` is retriable, which is correct, since a repeat under the same identity is
safe. But the information it carried, that something may have applied, was dropped at the
next try.

## Evidence

**Reproduced against the unfixed tree** with a scripted server. The server shakes hands and
then answers each bundle with the next error code in a list (`shoal/tests/retry_outcome.rs`):

```text
test an_unknown_outcome_outlives_a_later_refusal ... FAILED
test refusals_alone_stay_refusals ... ok
test an_unknown_outcome_outlives_the_budget ... FAILED
a bundle that may have applied reached its caller as Server { .., code: StorageWrite,
  msg: "scripted StorageWrite on try 2" }
a bundle that may have applied reached its caller as Server { .., code: Shedding,
  msg: "scripted Shedding on try 4" }
```

The item was first **established by reading the source**, while choosing the error code for
item 122's pending writes. The scripted server is what reproduced it.

## The fix

**The loop remembers whether any earlier try's outcome was unknown.** When it stops on a
failure that does not itself say the bundle may have applied, it returns `OutcomeUnknown`
instead. That error keeps the query id and index of the last failure, and its message quotes
the last failure in full. A caller can now tell "never applied" apart from "may have applied,
and then a retry was refused".

- `outcome_unknown(error)` decides which failures count as "may have applied": `OutcomeUnknown`
  and `ConnectionLost`, the latter because a connection that ended before its answer may have
  carried the bundle to a group that committed it.
- `settle(error, unknown)` does the relabelling. A last failure that is itself one of those two
  is returned unchanged.
- The loop remembers only a retried try, because a try that is not retried ends the loop and
  is settled as it is.

The cost is a `bool` in the loop. A bundle that succeeds, or that fails with no unknown outcome
before the failure, is returned exactly as before.

## Alternatives rejected

- **Not retrying `OutcomeUnknown`.** A repeat under one identity is what resolves an unknown
  outcome: if the first try applied, the group answers the repeat with the first try's result.
  Giving that up would turn every transient unknown outcome into the caller's problem.
- **Returning every try's failure (`Errors::BulkError` or a new variant).** That is more
  information, but every caller would have to pattern-match a list to answer the one question
  that matters, which is whether the bundle may have applied. The single code answers it, and
  the message keeps the detail for a person reading it.
- **Counting `Timeout` as unknown.** A `Timeout` is only ever answered to a read. A write that
  runs past its deadline is answered `OutcomeUnknown`, so the code for "may have applied"
  already exists.
- **Counting a client-side `Errors::IO` as unknown.** The client cannot tell a bundle it never
  wrote from one the socket lost after writing. Relabelling every connect failure would call
  writes that were never sent "may have applied". This is recorded under Still open.

## Invariants to uphold

- **A definite refusal is only returned when no try's outcome was unknown.** Anything that makes
  the loop return early, such as a new early exit or a new non-retriable branch, has to go
  through `settle`.
- **The relabelled failure keeps the last failure's words.** Callers and operators read the
  refusal that stopped the loop from the message, and a relabelling that dropped it would hide
  why the retry failed.
- **`outcome_unknown` is narrower than `retriable`.** Every code it names is retriable, but many
  retriable codes (`Shedding`, `NotLeader`, `Unavailable`, `QuorumUnavailable`) are definite
  refusals and must not be remembered as unknown.

## Still open

A connection lost on the client side (`Errors::IO`) after the bundle was written is not
remembered as unknown, for the reason given under Alternatives rejected. Telling the two apart
would need the stream to record whether its frame reached the socket.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `an_unknown_outcome_outlives_a_later_refusal` (`shoal/tests/retry_outcome.rs`) | `OutcomeUnknown` and then `StorageWrite` is reported as `StorageWrite`. |
| `an_unknown_outcome_outlives_the_budget` | `OutcomeUnknown` and then `Shedding` until the budget runs out is reported as `Shedding`. |
| `refusals_alone_stay_refusals` | Guards the other direction: two sheds and then `StorageWrite` must still be `StorageWrite`. |
| `a_refusal_after_an_unknown_outcome_stays_unknown` (`shoal-client/src/client.rs`) | `outcome_unknown` or `settle` is changed: a wrong code counted, the query id, index or message dropped, or an unknown last failure relabelled. |

## Related

- [Resolved #122](intent-log-failure.md), whose `OutcomeUnknown` and `StorageWrite` answers
  made this reachable on a standalone node.
- [F42](../../features/primary-failover.md), which added the retry loop.
