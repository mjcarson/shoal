# 213. X11's setup frame overwrote its first-in-first-out flag with the window

## Symptom

Spike [X11](../../object-storage/streamed-bodies.md) compared two ways a server's writer can order
what it sends on a connection that carries both a stream and small answers. It could write every
small frame queued before the next data frame (*small first*), or write frames in the order they
were queued (*first in first out*). Its record said the difference "changed little", and S18's
decision for Q26 repeated it ([Q26, in part](../../object-storage/contract.md#q26-in-part-streamed-bodies-2026-10-05)).

No cell measured small first on the server. Every connection X11 opened asked for a window from 1
to 16, and every one of them was served first in first out, whatever it asked for. Section 3's read
rows - a small request beside a read stream on the stream's own connection, the rows T1 was judged
on for reads - were all measured with the server writing first in first out. Its "small first"
column and its "first in first out" column measured the same thing.

## Cause

A connection's first frame is a `Setup` of sixteen bytes (`shoal-spike/src/stream/wire.rs`). Its
`encode` wrote five flags and two numbers:

```rust
body[3] = u8::from(self.verify);
body[4] = u8::from(self.fifo);
body[4..8].copy_from_slice(&self.window.to_le_bytes());
body[8..12].copy_from_slice(&self.lowat.to_le_bytes());
```

The window was written over the byte the flag had just been written to. `decode` read the flag
back from that byte, `fifo: body[4] != 0`, so the flag was the window's low byte, and true for
every window that is not a multiple of 256. The window itself came back whole.

`bodies_round_trip`, the test meant to catch this, set `fifo: true` with a window of 8. That is the
one combination the overlap cannot break.

The flag mattered only where the server writes data frames and small frames on one connection,
which is a read stream with small requests on its connection. X11's sections 1, 2 and 4 send no
small request beside a stream, and a write stream's data frames come from the client, whose writer
orders its own frames and never reads the setup. So only section 3's read rows were measured under
the wrong order.

## Evidence

**Reproduced.** Found by reading the code while planning [X13](../../object-storage/spikes.md#x13-the-benchmarks-shape),
which reuses X11's server; the agent planning X13 found it independently in the same reading.
`a_small_first_setup_round_trips` asks for small frames first at every window X11 ran. Against the
tree at `85ff2c2`:

```text
test stream::wire::tests::a_small_first_setup_round_trips ... FAILED
assertion `left == right` failed: window 1
  left: Setup { file: false, route: Direct, nopad: false, verify: false, fifo: true, window: 1, lowat: 0 }
 right: Setup { file: false, route: Direct, nopad: false, verify: false, fifo: false, window: 1, lowat: 0 }
```

With the fix it passes, and so does `bodies_round_trip`.

**What the server's order was worth, measured again.** Section 3's read cells ran four rounds on each
of the four legs that ran them, with the fix, from a `znver1` build under the `performance`
governor. A small request's p99 in µs beside a read stream, on the stream's own connection, with the
server writing small frames first, with that and `TCP_NOTSENT_LOWAT` at 16 KiB on both ends, and
first in first out, then on a connection of its own to the same executor:

| Leg | Stream | Small first | Small first, low water mark 16 KiB | First in first out | Own connection |
| --- | --- | --- | --- | --- | --- |
| europa loopback | 64 KiB kTLS | 152 [138–168] | 138 | 210 [208–211] | 191 |
| europa loopback | 256 KiB kTLS | 346 [331–353] | 350 | 500 [489–510] | 470 |
| europa loopback | 1 MiB kTLS | 2,036 [2,002–2,119] | 1,207 | 2,021 [1,912–2,126] | 291 |
| europa loopback | 8 MiB kTLS | 8,889 [5,460–9,145] | 9,084 | 13,716 [13,417–14,477] | 3,301 |
| titan loopback | 64 KiB kTLS | 321 [315–368] | 314 | 397 [375–434] | 363 |
| titan loopback | 256 KiB kTLS | 424 [422–427] | 423 | 703 [701–704] | 639 |
| titan loopback | 1 MiB kTLS | 4,897 [2,196–4,986] | 2,016 | 3,923 [3,230–5,084] | 851 |
| europa to titan | 256 KiB kTLS | 8,578 | 3,571 [3,116–4,018] | 8,599 | 1,093 |
| europa to titan | 1 MiB kTLS | 28,710 [27,113–32,594] | 10,127 [10,013–10,506] | 34,609 [34,594–34,650] | 1,613 |
| titan to hyperion | 1 MiB kTLS | 31,244 [29,065–33,631] | 10,418 [10,046–10,461] | 34,600 [34,552–34,688] | 1,645 |

Two of X11's findings did not survive it:

- **Small frames first is not "little".** Where a small answer waits behind frames still in the
  server's own queue, writing it first takes 20 to 40% off its p99: over loopback below 1 MiB, and
  beside 8 MiB frames on europa. Where it waits behind what the socket already holds, it changes
  nothing: beside 1 MiB frames over loopback, and across the network, where it took 34.6 ms to 28.7
  to 31.2.
- **`TCP_NOTSENT_LOWAT` helps a read.** X11 recorded that it could not, reasoning that a read's
  answer waits behind bytes already sent. It waited behind frames in the server's own queue, which
  no socket option reaches. With small frames first the low water mark cut the p99 beside 1 MiB
  frames across the network about three times, to 10.1 to 10.4 ms.

X11's decision stands. T1 still fires on every leg it fired on before, and on titan's loopback
read too, which held at 1.46 in X11's run and fired at 4.25 in the repeat; that row is mostly the
own connection's p99, 851 µs against 2,682 µs, in rounds that disagree with each other. Even with
both remedies, a small request on a stream's connection waited 6 to 10 times as long across the
network as on a connection of its own, so object bytes still get connections of their own.

## The fix

**The flag has a byte of its own.** `fifo` moved to `body[12]`, the first of the four bytes the
setup carried and never used, and the window and the low water mark keep `body[4..8]` and
`body[8..12]`. Nothing else in the frame moved, so a setup asking for first in first out is read as
it was, and one asking for small frames first is now read as asked.

**The cells it touched were run again.** `shoal-spike stream` gained `--streams read`, which keeps
only the cells of the named sections whose stream runs in a named direction, and the lab script an
`ONLY` that passes it to every leg, so section 3's read cells were repeated on the four legs that
ran them, four rounds each, with every arrangement re-paired round by round. The new records
replace the old section 3 read records in `shoal-spike/results/x11-*.json`; every other record
stands. The repeat's tables are `shoal-spike/results/x11-*-r*-tail-read.md`, and
`x11-report.md` is the report over the merged records.

## Alternatives rejected

**Correct the record without running again.** That would have said the small-first column was
never measured, and left the question it was asked to answer open. The cells took four rounds on
four legs, and they changed two of X11's findings, so the run was worth more than the note.

**Run all of X11 again.** Sections 1, 2 and 4 send no small request beside a stream, and a write
stream's frames are ordered by the client, which never read the setup. Their records do not depend
on the flag, so repeating them would have measured the same thing a second time.

**Grow the setup to carry the flag.** Sixteen bytes held four unused ones; a longer frame would
have meant a version check on both ends of a spike's own protocol.

## Invariants to uphold

- **Every field of the setup has bytes no other field writes.** `encode` writes each field once,
  to its own bytes, and `decode` reads each from the same bytes. A field added takes an unused byte
  and is added to `bodies_round_trip` with a value that is not its default.
- **A round trip test sets every field to a value that is not its default, and asks for each flag
  both ways.** A test that sets a flag only to true cannot see it overwritten by something that
  happens to be non-zero, which is how `bodies_round_trip` passed this defect for X11's whole run.
- **A repeat of part of a run is its own run.** It writes to its own directory locally and
  remotely, so no record of the first run is appended to, and its records replace the first run's
  for exactly the cells it repeated.

## Still open

- **A node sets no `TCP_NOTSENT_LOWAT`.** The repeat found that with small frames written first, a
  low water mark on the sending socket cuts a small answer's tail beside a stream several times,
  which X11 had recorded as impossible for a read. The product's outbox already writes small
  answers first ([F73](../../features/bodies-across-frames.md)); filed as
  [O94](../optimizations.md#o94-a-nodes-sockets-set-no-tcp_notsent_lowat).
- **The kTLS rounds still disagree with each other** on titan's loopback at 1 MiB, as X11 found,
  and the repeat did not find why.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_small_first_setup_round_trips` | `shoal-spike/src/stream/wire.rs` | A setup asking for small frames first at a window of 1 to 16 reads back as first in first out |
| `bodies_round_trip` | `shoal-spike/src/stream/wire.rs` | Kept: a setup with every flag set comes back as it went, so moving the flag to its own byte broke no other field |

## Related

[X11's record](../../object-storage/streamed-bodies.md#3-a-small-request-beside-a-stream), which
this corrects; [S18's Q26 entry](../../object-storage/contract.md#q26-in-part-streamed-bodies-2026-10-05),
corrected with it; [X13](../../object-storage/spikes.md#x13-the-benchmarks-shape), whose plan found it;
[F73](../../features/bodies-across-frames.md), whose outbox writes small answers first;
[O94](../optimizations.md#o94-a-nodes-sockets-set-no-tcp_notsent_lowat).
