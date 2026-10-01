# 152. A kanal receive raced against a timer lost the message handed to it

## Symptom

`a_compaction_that_meets_an_unreadable_archive_is_tried_again` in
`shoal/tests/persistent_unsorted_table.rs`, and its sorted twin, sometimes failed with *"the
rotated logs were never compacted once the archives were readable again: N left"*. The rate was
about one run in fifteen of the workspace suite at six threads, and never alone. Nothing in the
compactor's log said a job had failed. The rotated log was simply never merged.

## Cause

The compactor waits for its next job and for the earliest retry at the same time
(`tables/storage/fs/compactor.rs`, the job loop):

```rust
let mut recv = Box::pin(self.jobs_rx.recv()).fuse();
let mut timer = Box::pin(glommio::timer::sleep(wait)).fuse();
select! {
    job = recv => (job?, 0),
    () = timer => continue,
}
```

A kanal receive that is waiting is handed its value directly by the sender: the value is written
into the waiting future, not queued. kanal 0.1.1's `ReceiveFuture::drop` then says so itself:
*"got ownership of data that is not going to be used ever again, so drop it"*. So when a sender
hands the job over and the timer is also ready before the task is polled again, `select!` may
take the timer's branch. The receive future is dropped at `continue`, and the job with it. A lost
`IntentLog` job is a rotated log that nothing will merge until the next start folds it.

The wait only takes this path while a retry is pending. Since
[O74](../optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench),
an archive pass asked for within a minute of the last waits in the retry list, so after a
table's first pass almost every idle wait is this race. The test is the case that makes it
likely: merges fail against the unreadable archives and back off on short timers, while
rotations keep arriving.

The same shape was in fifteen places in the workspace:

- **The server:**
  - the compactor;
  - the shard's admin answer (`shard.rs`);
  - the control-plane answers awaited by backup, migration, restore and repair
    (`glommio::timer::timeout` around `rx.as_async().recv()`).
- **The client:** a stream's deadline (`tokio::time::timeout_at` around `response_rx.recv()`).
  Behind it, `ShoalResultStream::next` and `ShoalUnorderedResultStream::next` were themselves
  not safe to race: a caller that raced `next` against a timer and dropped it dropped the
  receive inside, and with it an answer already handed to it.
- **The rest:** the inventory wizard's `tokio::select!` over its probe and resolution answers,
  and seven test helpers.

## Evidence

**Established by reading the source, then reproduced below the compactor.** The compactor's
race needs a hand-off and a timer expiry between two polls, which a test cannot place. The
defect itself is kanal's behaviour, reproduced with no runtime at all by
`a_dropped_kanal_receive_loses_a_value_handed_to_it` in `shoal-channel/src/lib.rs`. A receive is
polled once so that it waits, a value is sent, and the receive is dropped: exactly what the
`select!` does when its timer wins. `try_recv` then finds the channel empty and the value gone.
The test passes, and is kept as the pin on kanal's behaviour.

The test that found it passed 36 of 36 runs alone on the unfixed tree
(`308313c`, six copies at once, `target/lab/r12/152/`). That is consistent with the rate the item
recorded and says nothing more: the window is two polls wide.

## The fix

**A receive that may be raced is kept by its receiver, not by the future that waits on it.**
`shoal-channel` is a new crate, and it depends on kanal alone, so the glommio server and the
tokio client both use it. Its `KeptReceiver<T>` (and `LocalKeptReceiver<T>`, for values that are
not `Send`, such as a test schema's `ServerMsg`) owns a receive in progress:

- `next` returns a future that polls that receive, and starts one if none is in progress.
  Dropping that future drops nothing, and the next call resumes the same receive.
- `try_next` takes nothing while a receive is in progress, since a sender hands its value to
  that receive first. Taking a later one would reorder the channel.

Every one of the fifteen sites now races a kept receiver's `next`:

- **The compactor** keeps its jobs channel.
- **The two client streams** keep their response channel. `next` on a stream is therefore safe
  to race, and a stream's deadline no longer drops an answer that has already arrived.
- **The shard's one-shot waits** keep their answer for as long as they wait.
- **The wizard** keeps both of its channels.

**A guard keeps it that way.** `shoal-channel/tests/no_raced_receives.rs` reads every Rust file in
the workspace and fails on either of two shapes:

- a race (`select!`, `select_biased!`, `future::select`, `race`, `timeout`, `timeout_at`) whose
  body names `.recv()`;
- a `.recv()` future that is kept rather than awaited where it is made (pinned, fused, bound,
  passed, or made a `select!` arm).

Run against copies of the unfixed files, it names all of them.

## Alternatives rejected

- **`select_biased!` with the receive first.** Bias decides which ready branch wins, but a timer
  can become ready between the hand-off and the poll that would have seen it. Or the receive
  can simply not be ready yet when the timer fires, and then be handed a value while the
  `continue` drops it.
- **kanal's `ReceiveStream`.** It keeps its future across polls, which is the right property.
  But it borrows the receiver, so it cannot be held beside it in the same struct (the
  compactor's `self`, a stream's fields) without a self-reference.
- **Wait on a timer, then `try_recv`.** It cannot lose a value, but it turns every wait into
  polling at the retry's granularity, or adds a separate notification channel beside every
  kanal channel.
- **Leave the one-shot waits bare.** Their waiter gives up for good when the timer wins, so a
  value lost there was going nowhere. They moved anyway, so that the rule has no exceptions and
  the guard needs no allow-list.

## Invariants to uphold

- **Never race a bare kanal receive.** That means no `select!`, no `timeout` and no `race`
  around a `.recv()` future, anywhere, tests included. Race a `KeptReceiver`'s `next`. The guard
  test fails otherwise.
- **A `KeptReceiver` must outlive every race it takes part in**, or the value in its pending
  receive is lost when it is dropped. Hold it in the struct or loop that owns the channel, not
  in a temporary inside the race. `into_receiver` drops a pending receive for the same reason,
  and is only for a channel whose current use is over (a stream's clean end, before the channel
  is recycled).
- **Do not mix `try_recv` on the bare receiver with a kept receive in progress.** Use
  `try_next`, which holds back while one is waiting. Otherwise a later value can be taken ahead
  of the one handed to the waiting receive.

## Still open

- Whether any lost message explains [#142](../known-issues.md#142-two-fixture-tests-fail-intermittently-on-an-idle-host)'s
  deadlines is not established. A lost merge would hold its WAL segment, and a held segment
  holds every group's checkpoint on its shard, which is the shape of the *"checkpoint never
  reached 5"* failure. The suite runs of [round 12](../../cluster-testing/correctness.md#13-round-12)
  record the rate on the fixed tree.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-channel`: `a_dropped_kanal_receive_loses_a_value_handed_to_it` | Pins kanal's behaviour. It fails if kanal stops losing the value, which is when this crate can be reconsidered |
| `shoal-channel`: `a_dropped_next_keeps_the_value_for_the_next_call` | A kept receiver that drops its receive with the `next` future loses the value, and one that lets `try_next` jump the queue returns 8 before 7 |
| `shoal-channel`: `a_local_receiver_keeps_a_value_that_is_not_send` | The same, for the local receiver |
| `shoal-channel`: `try_next_takes_what_is_queued_and_reports_a_close` | `try_next` stops taking queued values, or hides a closed channel |
| `shoal-channel`: `no_bare_kanal_receive_is_raced` | Any bare receive raced anywhere in the workspace, including the fifteen this fixed |
| `shoal-channel`: `the_scan_finds_each_shape` | The scan stops recognising a race, and the guard above passes vacuously |
| `persistent_unsorted_table` and `persistent_sorted_table`: `a_compaction_that_meets_an_unreadable_archive_is_tried_again` | The job loss this item was filed on, at the rate the suite meets it |

## Related

- [#174](snapshot-cut-queue.md), which made the compactor drain its channel ahead of a run of
  merges. `try_next` keeps that order.
- [O74](../optimizations.md#o74-a-zen1-nodes-compactor-falls-hundreds-of-jobs-behind-under-the-bench),
  whose paced archive pass made the racing wait the common one.
- [#158](runtime-waker-lists.md), the other defect found in a primitive's behaviour under
  cancellation.
