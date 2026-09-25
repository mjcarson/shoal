# 158. The glommio runtime's watch and mutex kept a waker per poll, and the mutex could lose a wakeup

## Symptom

Found while reading the runtime during the O64 investigation on the lab, not from a failure that
was pinned down. A load on a build with extra instrumentation in the apply wait stopped
committing entirely. Every write was refused with *"the lease of … lapsed: no quorum acknowledged
it within 10s"*, on a healthy cluster with no errors in any node's log, until the nodes were
restarted. The instrumentation was removed and never reproduced it, so that failure is not
attributed to this defect. It is what led to reading the runtime's primitives, where both defects
below were found and then reproduced.

## Cause

openraft runs on the runtime Shoal gives it (`server/control/runtime/`), and that runtime's
watch, mutex and channel are written in Shoal for one thread. Two of them registered a waker on
every poll that returned `Pending` and removed it only when they woke it:

- **The watch** (`watch.rs`). `Changed::poll` pushed a waker, and only a change drained the list.
  A `changed()` future that is dropped, or a receiver that goes away, left its waker behind.
  openraft's replication task calls `cancel_rx.changed().now_or_never()` once per stream session
  (`ensure_still_leading`), and that watch changes only when the task is cancelled. So each
  session added a waker to a list that is emptied only when the group's leadership ends. The
  growth is one waker, two words, per session for as long as a node leads.
- **The mutex** (`mutex.rs`). `Lock::poll` pushed a waker, and `Guard::drop` popped one, the
  newest, and woke it. A lock attempt dropped while it waited left its waker in the list. Say
  task B waits, then task A waits and A's attempt is dropped. The guard's drop pops A's stale
  waker, and B is never woken, with the mutex free. openraft takes this mutex inside the request
  stream of a replication session (`next_append_request`) and drops that stream whenever a
  session ends early. A replication task wedged this way stops feeding its follower, and a group
  with both followers wedged cannot acknowledge its leader's lease.

## Evidence

**Established by reading the source, then reproduced** by four tests written before the fix,
against the unfixed tree:

```text
an_unchanged_watch_keeps_one_waker_per_receiver: a receiver that polled 1000 times left 1000 wakers
a_dropped_receiver_leaves_no_waker: assertion `left == right` failed: a dropped receiver left its waker (left: 1)
a_waiter_polled_again_keeps_one_waker: one attempt left 1000 wakers
a_waiter_is_woken_past_an_abandoned_one: the waiting task was never woken with the mutex free
```

The last one is the lost wakeup, on a real glommio executor: a task waiting for the mutex is
still waiting a second after the guard dropped.

No lab failure is attributed to either defect. The lease failure described under *Symptom* has
the shape the mutex's lost wakeup would give, but it happened on a build that is gone, and it was
not reproduced.

## The fix

- **The watch keeps one waker slot per receiver.** A receiver gets an id when it is made, a
  poll replaces that receiver's waker in place, and a receiver's drop removes its slot. The list
  is bounded by the number of receivers.
- **The mutex keeps one slot per lock attempt and wakes the oldest.** An attempt takes a slot
  the first time it waits and replaces its waker on later polls. Taking the guard gives up the
  slot, and an attempt dropped while queued removes it. A guard's drop wakes the attempt that
  has waited longest, and removes its slot. An attempt woken that way and dropped before it runs
  wakes the next one, if the mutex is still free.

The channel (`channel.rs`) was checked and left as it is. Its sender wakers are drained on every
receive, so its list is bounded by the traffic, and a stale waker there costs one spurious wake,
not a lost one.

## Alternatives rejected

- **Deduplicate with `Waker::will_wake`.** It bounds a single task's repeated polls, but not
  wakers from futures that were dropped. And it does nothing for the mutex's lost wakeup, which
  comes from waking the wrong waiter, not from having too many.
- **Wake every waiter on a guard's drop.** It avoids the lost wakeup without slots. But every
  waiter but one finds the mutex held again and queues again, which costs O(n) wakes per unlock
  on a mutex that a replication stream takes for every request.
- **Replace the primitives with tokio's.** tokio's sync types are runtime-agnostic and correct
  under cancellation, but they are `Send + Sync` and atomic. This runtime exists so that a group
  on one core pays for none of that ([F37](../../features/node-identity-control-plane.md)).

## Invariants to uphold

- **A waker list is bounded by live waiters, not by polls.** Every primitive here that registers
  a waker has a slot per waiter, replaced on repoll and removed on drop.
- **A wake is never spent on a waiter that is gone.** A waiter woken to take a resource either
  takes it on its next poll or, if dropped first, passes the wake to the next waiter.
- **Cancellation is ordinary.** openraft drops futures from these primitives mid-wait as a
  matter of course: a stream whose session ended, a `now_or_never` poll, a lost `select!` arm.
  A primitive has to be correct when any of its futures is dropped at any `Pending`.

## Still open

- The lease failure under *Symptom* is unexplained. If it recurs on a build with this fix, it is
  not this.
- The runtime's conformance suite (`glommio_runtime_passes_the_openraft_suite`) passed on the
  unfixed tree too. It does not drop futures mid-wait, so it could not find either defect. The
  tests here cover that case for this runtime only.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `an_unchanged_watch_keeps_one_waker_per_receiver` (`shoal-core`, `runtime/watch.rs`) | A receiver polled on an unchanged watch adds a waker each time |
| `a_dropped_receiver_leaves_no_waker` (`shoal-core`, `runtime/watch.rs`) | A dropped receiver's waker stays in the list until the next change |
| `a_waiter_is_woken_past_an_abandoned_one` (`shoal-core`, `runtime/mutex.rs`) | A task waiting for the mutex sleeps with it free, after a later attempt was abandoned |
| `an_abandoned_wake_is_handed_on` (`shoal-core`, `runtime/mutex.rs`) | A wake given to an attempt that is then dropped is lost, and the next waiter sleeps |
| `a_waiter_polled_again_keeps_one_waker` (`shoal-core`, `runtime/mutex.rs`) | One waiting attempt adds a waker per poll |

## Related

- [F37](../../features/node-identity-control-plane.md), which wrote the runtime.
- [O64](../optimizations.md#o64-a-shorter-failover-base-halves-write-throughput-on-the-lab), the
  investigation that found it.
