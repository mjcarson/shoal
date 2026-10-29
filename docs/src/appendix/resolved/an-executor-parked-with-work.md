# 249. An executor that found a preemption due before its queues ran went to sleep with their tasks

Every object executor ([F85](../../features/devices-and-pools.md)) is a glommio `LocalExecutor`
with three task queues, `ObjectForeground`, `ObjectApply` and `ObjectBackground`, each with a
latency goal of 100 µs. glommio runs such an executor in a loop, in the fork beside the workspace:

1. `poll_io` submits and reaps I/O and, last, installs a preempt timer for the queues' goal.
2. `run_task_queues` runs the active queues until a preemption is due, which is whenever the
   latency ring holds a completion. The preempt timer firing is one.
3. When `run_task_queues` answers that nothing ran, `run` polls I/O once more without sleeping
   and parks until an event comes.

Filed from F90's stress (2026-10-15); fixed in the flaky-test plan's fourth wave (2026-10-27), in the
glommio fork.

## Symptom

- **F90's stress.** Eight copies at once of an objects test, each node's object executor pinned to
  cpu 31: about one copy in seven stopped. A write in place's applies were spawned on `ObjectApply`
  and never finished, while a read taken after them on `ObjectForeground` was answered in a
  millisecond. The executor's thread slept in `io_cqring_wait`, and the next write was refused
  `NotReady`.
- **The object lane's unit tests at six threads**, their stores' executors pinned to cpu 0: about
  one run in twenty-five hung. A read stopped inside its header's I/O, or before its range was read,
  with both stores' executors asleep. In W4's workspace run the lib suite hung for 31 minutes
  until it was killed.
- Every await a task stopped on waited for nothing but its own I/O or a yield.

## Cause

`run_task_queues` checked for a preemption at the top of its loop and answered whether anything
ran. A preemption could already be due at its first check, before any queue ran. It then answered
`false`, though its active queues held runnable tasks.

`run` took `false` to mean there was nothing to run, and parked. The completion that made the
preemption due was the preempt timer's, which wakes no task. It was reaped on the way to the park,
and a parked executor installs no preempt timer, since it means to run nothing. So the executor
slept with its tasks queued. They waited for an unrelated event: a completion, a timer, a wake from
another thread. Where none came, they waited for good.

The timer is due at that first check when the thread is held for longer than its 100 µs between
`poll_io` installing it and the check: descheduled, or held by interrupts. Every reproduction shared
one cpu among executors, eight nodes' on cpu 31 or six tests' stores on cpu 0, which makes that
common. A deployed node gives each object executor a cpu of its own, which makes it rare, not
impossible. Any executor with a queue whose latency matters can meet it. Every shard has one, its
high-priority queue at 500 µs (`shard.rs`), so a shard held that long goes to sleep with its work
too, until its next event. Upstream glommio's `main` has the same code (`3c92bcc`, 2026-03-24, as
last fetched here).

## Evidence

**Established by a probe, then reproduced by a test.**

- **The hang's state** (#249h, before the probe): both stores' object executors asleep in
  `io_cqring_wait`. Every ring had submitted what it prepared and reaped what had completed. So the
  stalled task had been woken, or should have been, and its executor never ran it
  ([item 249's record](../flaky-tests.md#runs)).
- **The sleep probe.** An executor about to sleep now reports, where `GLOMMIO_SLEEP_PROBE` names a
  file, any queue holding runnable tasks, any queue on the active list, and any wake from another
  thread not yet processed. In 120 runs of the lane tests at six threads (#249p), 4 hung. In each,
  the hung executor's last line was a sleep with a runnable queue:

  ```text
  executor 5 on thread Some("shoal-objects-0-5") sleeps with runnable queues [("ObjectForeground:0", 1)], 1 on the active list and 0 foreign wakes pending
  ```

  Four more runs slept with a runnable queue and did not hang: something else woke them.
- **Reading the loop** found one way for an executor to reach its park with an active queue: the
  answer above, given when a preemption was due before any queue ran.
- **The fork's test** `a_preemption_due_as_the_queues_run_begins_never_parks_with_work` starts the
  executor's first three runs of its queues with a preemption due. It counts the sleeps the reactor
  would take with work queued, which a test build declines to take. On the code before the fix:

  ```text
  assertion `left == right` failed: the executor went to sleep with runnable tasks queued
    left: 3
   right: 0
  ```

- **With the fix**, 160 runs of the lane tests at six threads (#249f) neither hung nor failed, and
  the probe found no sleep with a runnable or an active queue.
- **F90's stress again** (#249o): eight copies at once of `partial_overwrite_equals_a_fresh_encode`,
  each node's object executor on cpu 31, six rounds a side.
  - Without the fix, 7 of 48 copies failed, each on a write whose attempt made no progress for 60 s
    (*this attempt made no progress for 60s waiting for its answer*). Item 308's bound now turns
    the stall into that failure.
  - The probe counted 150 sleeps with work, in 45 of the 46 copies it wrote for. Every failed
    copy's included a task queued on `ObjectApply`, the item's first symptom.
  - With the fix, all 48 passed and the probe counted none.
  - A round took 27 to 28 s with the fix, against 75 to 107 s without it. The copies that passed
    had been sleeping with work too, each until something else woke it.

## The fix

In the fork, commit `7d68624` on `mjcarson/glommio` `ZeroCopyDmaStreamWriter`:

- **A run of the task queues that is preempted while a queue is still active answers that there is
  work to come back to.** `run` then polls I/O and runs the queues again, as it does after any
  preemption. It parks only when no queue is active.
- **The sleep probe stays.** Unset, it reads nothing. Set, it appends a line to its file as an
  executor goes to sleep with work it never ran, written straight to the file so that a hung test
  cannot hold it.

The fork's own `sync_background_basic` failed on every run here, at the commit that added it as
since. Its two yields never reach the reactor, so its fdatasync is neither submitted nor reaped. It
now waits on a timer (`c21f2d3`).

## Alternatives rejected

- **Make the executor's three queues one, or give them no latency goal**, the item's first
  direction for telling the scheduler from the I/O. A longer preempt timer makes the window rarer
  and leaves it open. The three queues are F85's design: a foreground read is never queued behind
  an apply or a rebuild.
- **Install the preempt timer in a park that finds a queue active.** The executor would wake 100 µs
  later rather than never, and would still spend a park's system calls for nothing.
- **Check the active queues in `run`, before its park.** That would work, but the defect is the
  answer itself: `run_task_queues` said nothing had run while work was queued, and every caller
  reads that answer.
- **Give each test store a cpu of its own.** The tests would stop reaching the race, and a
  deployed executor can still be held past 100 µs on its own cpu.

## Invariants to uphold

- **An executor never parks while a task queue is active.** `run_task_queues` answers `false` only
  when no queue is.
- **A preemption is a reason to come back to the queues, never a reason to sleep.**
- **The probe writes its file directly**, never through a test's captured output, which a hung test
  never prints.

## Still open

- **[Item 326](../known-issues.md#326-every-get-one-node-coordinated-stopped-held-on-a-read-of-its-own-disk-and-nothing-was-logged)**,
  the disk leg's stall, has not been shown to be this. An executor asleep with work wakes at its
  next event, and that stall outlasted the next arm's first get. Whether that get reached the
  executor is not known. Its next run on the disk leg, on a build with the fix, says.
- **Whether any other loaded failure was this.** A shard meets it at its 500 µs goal, and wakes at
  its next timer or message. The confirmation runs of [the flaky page](../flaky-tests.md) after
  this fix say whether any of its open rows stop failing.
- **What the fix is worth on a deployment is not measured.** In the stress a round took a third of
  the time with it, but there eight executors shared one cpu. A deployed executor that went to sleep
  with work waited for its next event, which on a busy node comes soon. The lab's object arms before
  and after would say whether any tail moved, beside [the shard loop's yield](../todos.md#the-shard-loops-yield-measured-on-the-lab).
- **Upstream glommio** has the same code. Whether to report it is the user's call.
- **`test_spin`** fails in the fork's lib suite at four threads, with the fix and without it, at
  about 51 ms of user time against its 90 ms threshold. It pins its executor to cpu 0, beside
  another test's, and its own comment records the same ~50 ms in CI. It was not changed here.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `a_preemption_due_as_the_queues_run_begins_never_parks_with_work` (glommio fork, `executor`) | The executor goes to sleep three times with its main task queued |
| `objects::lane::tests` (`shoal-core`), at six threads | About one run in thirty hangs, an executor asleep with a runnable queue |
| `partial_overwrite_equals_a_fresh_encode` (`objects.rs`), eight copies at once | 7 copies in 48 fail on a write whose attempt made no progress for 60 s, and a round takes three times as long |

## Related

- [F85](../../features/devices-and-pools.md), the object executors and their three queues.
- [Resolved #113](glommio-open-statfs.md) and [#148](stale-intent-log-tail.md), the fork's earlier
  fixes.
- [Item 326](../known-issues.md#326-every-get-one-node-coordinated-stopped-held-on-a-read-of-its-own-disk-and-nothing-was-logged),
  the disk leg's stall.
- [Tests That Fail Under Load](../flaky-tests.md).
