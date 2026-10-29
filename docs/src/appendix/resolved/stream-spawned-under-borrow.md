# 242. A failed slice's first piece ended its node's object executor

A whole chunk reaches a slice as a stream of pieces, and the slice's executor runs one task for
each stream, which `Streams::take` starts at its first piece ([F86](../../features/replicated-pools.md#the-object-lane)).
`take` started that task while it still held the map of open streams borrowed. glommio polls a task
it spawns before `spawn_local` returns, so when the first piece failed without awaiting anything,
as it does on a slice whose device has just failed, the task's way out borrowed the same map and
panicked. Found by [F88](../../features/recovery-and-moves.md)'s first lab run, an operator's
`fail-device` while puts went on, and filed and fixed in one change.

## Symptom

`shoaladm bench`'s `device_fail` arm on a copy of the lab's three nodes, `rw50` at 1 MiB, eight in
flight, at `b5a29b2`: titan's `/xfs` device was failed at 34.7 s and the arm ran on to its 900 s
event timeout without the moves settling. titan's node went on running, and its log said:

```text
ERROR shoal_core::server::devices: device=/xfs/shoal-tmdb-pool-bench id=01a13905-63ee-7ec3-bdd9-3b24f7c7bcc8
      msg="a pool device failed; its slices are failed with it and the node goes on" reason=failed by admin
thread 'shoal-objects-0-8' (3033202) panicked at shoal-core/src/server/objects/serve.rs:321:52:
RefCell already borrowed
```

The object executor `shoal-objects-0-8` was gone, and every slice it served with it. A move that
needed one of them waited for good, and an admin read of the bench's cluster timed out waiting for a
control thread.

## Cause

`take` borrowed the open-stream map mutably for its whole body. For a stream's first piece it made
the stream's channel, sent the piece down it, recorded the stream, and called
`glommio::spawn_local` for the stream's task, all under that borrow. glommio's `spawn_local` is
`spawn_and_run`: the new task is polled at once, until it first waits (`multitask.rs`,
`runnable.run_right_away()`). Its documentation says a spawned task may preempt the spawner
"immediately", and it does, inside the call.

The task's first await is normally the chunk file's I/O, so it returned to `take` before touching
the map, and the bug never showed. On a failed slice `begin_whole` returns `DeviceFailed` before
any I/O, and the task's error path marks the stream failed in the map (`self.open.borrow_mut()`)
while `take` still held it.

The fixture's operator test, `an_operator_fails_a_device_and_its_groups_move_off_it`, wrote nothing
to the device after it failed. The lab's arm had puts streaming pieces to it, placed before the
device's retirement reached their coordinators.

## Evidence

**Reproduced** by `a_first_piece_to_a_failed_slice_is_refused_and_panics_nothing`, written first
and run on europa against `b5a29b2`: a slice whose device has failed, given the first pieces of two
streams through `Streams::take`.

```text
thread 'unnamed-1' (1492795) panicked at shoal-core/src/server/objects/serve.rs:321:52:
RefCell already borrowed
thread 'server::objects::store::slice::tests::a_first_piece_to_a_failed_slice_is_refused_and_panics_nothing'
  panicked at shoal-core/src/server/objects/store/slice/tests.rs:90:10:
the test ran: Thread panicked { .. }
test result: FAILED. 0 passed; 1 failed
```

The same line as the lab's. With the fix both pieces are refused `DeviceFailed`, by the tasks they
started, and nothing panics.

## The fix

**`take` decides under the borrow and acts after it.** What becomes of a piece - refused, queued to
its running stream, or the start of a stream - is decided while the map is borrowed, the map's
change made there too, and the borrow dropped before a refusal is answered or a stream's task is
spawned (`Next` in `serve.rs`).

## Alternatives rejected

**`spawn_into` the executor's queue, which schedules rather than runs.** It would keep `take`'s
shape, and make every stream's first piece wait for the executor's next turn; a borrow held while
running other code is the defect whichever way glommio spawns.

**A `try_borrow_mut` in the task's error path.** It would hide the panic and lose the mark that
refuses the stream's later pieces.

## Invariants to uphold

- **Never call `glommio::spawn_local` while a `RefCell` borrow is alive** that the spawned task may
  take before its first await. glommio polls the task inside the call, so the task runs under the
  spawner's borrows. This is the control core's rule against a borrow across an `.await`, for
  spawning. A search of `shoal-core` for a spawn under a live borrow found no other.
- **A reply is answered after `take` lets go of its map**, so a replier is free to do anything.

## Still open

Nothing of this defect. A put whose holder's device fails mid-stream is refused for that holder, as
before, and the row marks the position missed.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_first_piece_to_a_failed_slice_is_refused_and_panics_nothing` | `shoal-core/src/server/objects/store/slice/tests.rs` | A stream's first piece to a failed slice panics its executor with the map borrowed twice |

## Related

- [F88](../../features/recovery-and-moves.md), whose `device_fail` arm found it.
- [F86](../../features/replicated-pools.md#the-object-lane), which built the streams.
