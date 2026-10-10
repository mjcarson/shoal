# X11. Streamed bodies, measured

**Reported 2026-10-05, and corrected the same day by
[item 213](../appendix/resolved/x11-setup-fifo.md)**, which found that the server had written first
in first out in every cell, whatever a connection asked for. Section 3's read cells were run again
with it writing small frames first; every figure below that comes from them says so, and the
claims they overturned are struck through beside what replaced them. This is the record of spike
[X11](spikes.md#x11-streamed-bodies). It sent
frames of plain bytes between a glommio server and a tokio client, plaintext and under the
product's own kTLS, and measured:

- what one connection moves and what it costs in cpu a GiB, by frame size, to and from a file
  written with direct I/O and to and from memory;
- what a stream holds in memory at each window, and the rate each window reaches;
- what a small request waits for beside a stream, on the stream's own connection and on one of its
  own;
- whether a connection under kTLS can be handed from the executor that accepted it to another,
  and what a write's bytes cost when they cross executors as buffers instead.

It ran over loopback on europa, titan and hyperion, and across the lab's 1 GbE from europa to
titan and from titan to hyperion, four rounds each. It ends in a recommendation, which
[S18](contract.md#q26-in-part-streamed-bodies-2026-10-05) records as Q26, in part: **object bytes
travel on connections of their own, in frames of 1 MiB, four to a stream's window; a slice's
executor takes a connection rather than its bytes; and a stream that has to run at a device's rate
under kTLS is spread over more than one connection.**

All three triggers fired. Five facts decide it:

- **A small request behind a stream on its own connection waits for the stream.** Across 1 GbE its
  p99 beside a 1 MiB read stream was ~~34.6 ms against 1.1 ms on a connection of its own, 21 to 32
  times~~ 28.7 to 31.2 ms with the server writing small frames first, against 1.1 to 1.6 ms on a
  connection of its own, 18 to 26 times, and beside a write stream 27 to 36 ms. Over loopback it
  was ~~9.4~~ 7.2 times on europa under kTLS, 4.3 on titan, and 12 to 25 times beside writes. That
  is T1, and it fired on every leg.
- **~~Neither of the cheap remedies works~~ The cheap remedies narrow the gap and do not close
  it.** ~~`TCP_NOTSENT_LOWAT` left a read stream's neighbour where it was, since the bytes ahead of
  it were already sent and queued in flight and in the NIC's queue~~ With the server writing small
  frames first, `TCP_NOTSENT_LOWAT` at 16 KiB took a read stream's neighbour across 1 GbE from
  34.6 ms to 10.1 to 10.4 ms at its p99, still 6 to 10 times a connection of its own; on a write it
  narrowed the gap to 3 to 12 times; and under kTLS on the sending side it made the tail far worse,
  to seconds at 8 MiB frames. Writing small frames first ~~at a frame's boundary changed nothing
  measurable~~ helped by itself only over loopback, and not beside 1 MiB frames; its worth is that it
  lets the low water mark work. fq_codel schedules flows fairly, so a connection of its own does.
- **Handing the connection over is free; moving its bytes is not.** A connection under kTLS was
  handed between executors by `dup` and `TcpStream::from_raw_fd` in every round on every host, its
  bytes checked both ways, at 0.93 to 1.05 times the direct stream's rate and cpu. Moving the
  buffers instead cost 1.3 to 1.7 times the cpu a GiB at 1 MiB in plaintext, at 0.87 to 0.90 times
  the rate, and halved the rate at 64 KiB. That is T2, on its second clause.
- **One kTLS connection reads below the device.** A Zen1 core received about 650 MiB/s against the
  970 EVO's 857, and europa 1,730 against the Optane's 2,553. Writes reached the device in some
  rounds and not others. That is T3.
- **The frame is 1 MiB.** cpu a GiB falls steeply to 1 MiB and is nearly flat above it; at 64 KiB a
  GiB costs 1.3 to 2.8 times what it does at 1 MiB. Two frames in flight reach
  both devices in plaintext; kTLS's rates swung by half between rounds, and four is where they
  settle.

**What was found on the way**: under kTLS a large frame holds its executor while the kernel encrypts
it, so a small request on another connection to the same executor waited 2.7 ms at its p99 beside
1 MiB frames on titan, against 0.16 ms on another executor; telling the receiver that records carry
no padding (`TLS_RX_EXPECT_NO_PAD`), which Shoal does not, took 10 to 20% off a read's receiving
cpu; and the kernel holds up to 4 MiB of a stream on the sending socket whatever its window. What
X11 did not settle is under [What X11 does not settle](#what-x11-does-not-settle).

## The question

[Q26](contract.md#questions-to-answer) asks how an object's bytes cross the client wire: in what
frame, under what window, and at what cost to a connection that also carries small queries
([S12](wire-and-client.md#sharing-a-connection-with-queries)). S13 asks a second question of the
same bytes: whether a connection accepted by one executor can be handed to the executor that owns
the slice its bytes are for, or every frame has to cross between executors as a buffer
([S13](isolation.md#a-lane-for-object-bytes)).

The spike's section named three results in advance that would change the design:

- a small query's tail on a shared connection moves by more than its budget. Then object bytes
  get connections of their own in the client's connection pool;
- a connection cannot be handed from the executor that accepted it to the one that owns the slice,
  in the glommio fork, with kernel TLS on it. Then every frame crosses executors as a buffer, and
  that hop's cost is part of every write;
- kernel TLS bounds a stream below the device's rate.

## How it was judged

The lines below were set before the harness existed and agreed with the user on 2026-10-05; T1's
budget is X9's, so the two spikes judge a table's tail and a small request's alike.
`shoal-spike stream report` judges them as written. Every figure is an interval over four rounds:
the lowest and the highest round, with the median between. A ratio is taken round by round and
judged by where its whole interval lies, which is
[the lab's rule](../performance/benchmarking.md#before-and-after-on-the-lab).

| Trigger | Fires when |
| --- | --- |
| **T1. A shared connection hurts small queries** | At 1 MiB frames, a small request's p99 on the stream's own connection is above 1.25× its p99 on a connection of its own to the same executor, beside the same stream, in every round, on any leg |
| **T2. The hop** | (a) A connection under kTLS cannot be handed from the executor that accepted it to another; or (b) a 1 MiB write stream whose buffers cross to the other executor runs below 0.9× the rate of the same stream written where it was read, or costs above 1.25× its cpu a GiB summed over both executors, in every round |
| **T3. kTLS bounds a stream below a device** | One kTLS connection at 1 MiB frames, to and from memory over loopback, moves fewer MiB/s than the device X6 measured on that host, in every round: 2,501 MiB/s of writes and 2,553 of reads on europa's Optane, 722 and 857 on the 970 EVOs |

**Statements made in advance, beside the triggers**, to be reported and not judged:

- the frame is the smallest at which one kTLS connection's rate and its cpu a GiB are within a
  tenth of their best (expected: 1 MiB);
- the window is the smallest number of 1 MiB frames in flight at which a stream reaches nine tenths
  of its best rate (expected: four);
- whether `TCP_NOTSENT_LOWAT` makes a shared connection fit inside T1's budget, which would keep
  object bytes on shared connections.

T3 can only be judged over loopback: across the lab's 1 GbE every stream is bounded near
117 MiB/s, far below either device, and every figure across the network is the link's.

## What was run

### The harness

`shoal-spike stream`, a subcommand of the spike binary X2 and X6 live in
(`shoal-spike/src/stream/`). It adds edges to `tokio`, `rustls` and `rcgen` and no crate to the
workspace's lockfile. Its TLS is the product's own: rustls's handshake and the kernel's record
layer, through `shoal::server::tls::accept` on the server and `shoal::client::tls::connect` on the
client, as a Shoal node and client do it ([F14](../features/encryption-in-transit.md)). Like every
spike's code it is thrown away; its frames are not the product's or S12's, though they cost what
those would.

**The server** is glommio executors pinned one to a core, their blocking threads on the cores'
siblings, each listening on a plaintext port and a TLS port of its own. Shoal's shards share one
port and the kernel picks; here a client picks the executor by the port it dials, because every
arrangement compared needs to know which executor holds which connection. Each executor keeps a
file of 1 GiB written ahead with a seeded pattern, X6's pool, on the device under test.

**The frames** have Shoal's eight byte header and the data head S1's prerequisite would give a
stream's frames, a sixteen byte id and an offset: a stream opened, its bytes in
data frames of a sixteen byte id and an offset, `LAST` on the final one, a range asked for and
answered by one data frame, and a small request of 64 bytes answered with 1 KiB. A connection's
first frame says what it is for.

**A write stream.** Each data frame's payload is read straight into a buffer for direct I/O, which
glommio's non-buffered socket fills with no copy of its own, and written to the executor's file
with O_DIRECT, at most the window's worth of writes in flight; the reader waits for the oldest
once a window is out, and TCP pushes back on the client. Or it is dropped, which is the wire alone.

**A read stream.** The client keeps a window of ranges outstanding; the server reads each from its
file with O_DIRECT, or slices it from the pattern in memory, and writes it back as one data frame.

**A small request** is paced from the other client runtime at one a millisecond, and its latency
is counted from when it is queued to its connection to when its answer is read, so a timer that
fires late is not the connection's. The server answers it at once, with no I/O. Both sides' writers
write every small frame queued before the next data frame; bytes already handed to the socket are
not taken back.

**The routes to the other executor.** A write stream whose connection was accepted on executor A
and whose bytes belong to executor B:

| Route | What it does |
| --- | --- |
| `direct` | A reads and writes, the baseline: the bytes belong to A |
| `hop` | A reads each payload into a buffer for direct I/O from the global allocator, hands it to B over a channel, and B writes it and hands it back; no byte is copied |
| `hop-copy` | A reads into an ordinary allocation, and B copies it into a buffer of its own before writing: what the fork allows without `unsafe` |
| `handoff` | A accepts and does the TLS handshake, reads the first frame, `dup`s the descriptor, closes its own, and sends the duplicate to B, which adopts it with `TcpStream::from_raw_fd` and serves the connection from then on |

**The handoff check** runs before the routes are timed, plaintext and under kTLS: a connection
handed over has to answer its setup from B, still be under kTLS on B's side, carry two seconds of a
write stream that B checks byte for byte against the pattern, then two seconds of a read stream
the client checks the same way, on the same connection.

**Counting.** Each executor answers a stat with its thread's cpu time (`CLOCK_THREAD_CPUTIME_ID`),
the host's busy time over every cpu from `/proc/stat`, payload bytes it finished with, and the
most bytes it held in buffers and the most socket memory one of its connections held
(`SO_MEMINFO`, sampled every 5 ms) since the last stat. The client reads the same of its stream
runtime's thread and its socket. Over loopback the host's busy time is read once, and across the
network on both hosts.

**The sections.** Every cell warms up for 3 s and is measured for 5 s, a small request's for 10 s:

| Section | Cells | Sides |
| --- | --- | --- |
| **1. Rate** | write and read; frames of 64 KiB, 256 KiB, 1, 4 and 8 MiB; to and from the file or memory; a window of four | plaintext, kTLS, and kTLS with the receiving socket told records carry no padding (`TLS_RX_EXPECT_NO_PAD`) |
| **2. Window** | write and read; frames of 256 KiB, 1 and 4 MiB; windows of 1, 2, 4, 8 and 16; to and from the file | plaintext, kTLS |
| **3. A small request** | alone; and beside a read or write stream at 64 KiB, 256 KiB, 1 and 8 MiB under kTLS, and 1 MiB plaintext | on the stream's connection; the same with `TCP_NOTSENT_LOWAT` at 16 KiB and at 128 KiB on both ends; the same with the server writing first in first out (reads); on a connection of its own to the stream's executor; on one to the other executor |
| **4. Routes** | a write stream of 64 KiB and 1 MiB frames, to the file, plaintext and kTLS | `direct`, `hop`, `hop-copy`, `handoff` |

Across the network, sections 1 and 3 ran, at frames of 64 KiB, 1 and 8 MiB (64 KiB, 256 KiB and
1 MiB for section 3) and to and from the file.

**Rounds.** Four, with every cell's sides in the opposite order in even rounds.
`results/x11-lab.sh` runs them and writes the facts every table is labelled with;
`shoal-spike stream report` merges the rounds and judges the triggers, and its output is
`shoal-spike/results/x11-report.md`.

### Where, and on what

| Host | CPU | Device and filesystem | Server's executors | Client's runtimes |
| --- | --- | --- | --- | --- |
| europa | Ryzen 9 7945HX (Zen4), 16 cores, 32 threads | Intel Optane 900P, XFS at `/optane`, `/optane/x11` | cpus 8 and 9, their blocking threads on 24 and 25 | cpus 10 (the stream) and 11 (the pacing) |
| titan | Ryzen Embedded V1756B (Zen1), 4 cores, 8 threads | Samsung 970 EVO on one PCIe lane, the XFS volume X6 fitted at `/xfs`, `/xfs/x11` | cpus 2 and 4, blocking threads on 3 and 5 | cpus 6 and 1 |
| hyperion | The same as titan | The same, another unit | The same | The same |

- **Five legs**: each host over loopback, both ends in one process; europa driving a server on
  titan, and titan driving one on hyperion, across 1 GbE (172.16.2.0/24, 0.12 ms round trip,
  `fq_codel` on every link). hyperion's loopback ran sections 1 and 4 only, repeating titan's.
- **Governor `performance`** on every host for every run, and titan's and hyperion's `e2scrub_all`
  timer held; both put back afterwards (`powersave` on europa, `schedutil` on the Zen1 hosts). No
  shoal unit ran on any host, and `shoal-tmdb` was left inactive as it was found.
- **One `znver1` build**, rustc 1.100.0-nightly (2026-09-04), kernel 7.0.0 (`-31` on europa, `-34`
  on titan and hyperion) with the `tls` module loaded, the glommio fork at `f4643f7`;
  `net.ipv4.tcp_wmem` at its default, 4 MiB at most, and `tcp_rmem` 32 MiB.
- **No payload ever failed its check**: every handoff check matched the pattern byte for byte, both
  ways, in every round.
- europa is also the development host. Its loopback rounds ran beside titan's and hyperion's, which
  share nothing with it, and nothing was built on it while they ran; its network leg ran last, alone.

**Where the run departed from [the plan](spikes.md#x11-streamed-bodies).**

- The plan named frames of 64 KiB to 8 MiB and both directions; to it were added a receiving side
  told kTLS records carry no padding, `TCP_NOTSENT_LOWAT` at two settings, a server writing first in
  first out, and two ways of crossing executors besides handing the connection over. A design review
  of the plan asked for the last three, and for the product's data head, so the frames cost what the
  product's will.
- "Across the lab" became two legs, a Zen4 client to a Zen1 server and Zen1 to Zen1, of sections 1
  and 3: every rate across the network is the link's, so the window and the routes were measured on
  loopback alone.
- A key update after a handoff, which would show the kernel and rustls still agree on the session,
  was not sent: the product never sends one ([F14](../features/encryption-in-transit.md#limitations)).

**Section 3's read cells were run again after the rounds**, four rounds on each of the four legs
that ran them, once [item 213](../appendix/resolved/x11-setup-fifo.md) found that the setup frame had
overwritten each connection's request for small frames first with its window, so the server had
written first in first out throughout. The repeat used a `znver1` build of the same tree with the setup
fixed, the same cores, the governor and the lab's rules; its records replace the old read records
in `results/x11-*.json`, its tables are `results/x11-*-r*-tail-read.md`, and its facts
`results/x11-facts-rerun-*.txt`.

**Two things were changed after the quick run and before the rounds.** A quick run of every section
on every leg found both:

- the lab script ran every loopback host at once; it was split into phases, so the europa legs
  could run apart from the Zen1 ones. The europa loopback rounds were then run by hand with the
  script's own lines, under the governor the running script had set;
- the report's tables took their columns from each section's first cell; it was fixed after the
  rounds to take every side, which moves no record.

## How to read the tables

Every figure is the median of four rounds, with the lowest and the highest round in brackets.
"cpu ms/GiB" is milliseconds of cpu a GiB moved: the server's executors, the client's stream
thread, or every cpu of the host (both hosts across the network). A small request's latencies are
in microseconds. A ratio is taken round by round.

## 1. One connection: rate and cpu by frame

Over loopback, to and from the executor's file with direct I/O, a window of four:

| MiB/s | europa plaintext | europa kTLS | titan plaintext | titan kTLS |
| --- | --- | --- | --- | --- |
| write 64 KiB | 2,384 | 1,650 | 790 | 666 |
| write 1 MiB | 2,401 | 1,948 [1,552–2,401] | 791 | 690 [582–791] |
| write 8 MiB | 2,369 | 1,358 | 790 | 792 [640–792] |
| read 64 KiB | 1,494 | 951 | 761 | 451 |
| read 1 MiB | 2,616 | 1,542 | 863 | 618 [583–721] |
| read 8 MiB | 2,616 | 2,227 [1,438–2,615] | 863 | 643 [610–741] |

And to and from memory, where the device is out of it:

| MiB/s | europa plaintext | europa kTLS | europa kTLS, no padding | titan plaintext | titan kTLS | titan kTLS, no padding |
| --- | --- | --- | --- | --- | --- | --- |
| write 1 MiB | 6,521 | 2,331 [1,715–2,988] | 2,725 | 3,549 | 629 [557–1,039] | 782 [633–1,236] |
| read 1 MiB | 5,728 | 1,730 [1,655–1,748] | 1,921 | 3,234 | 660 [652–760] | 709 [701–846] |

- **Plaintext reaches both devices at every frame from 64 KiB for writes and 256 KiB for reads.**
  The Optane takes about 2,400 MiB/s of writes and gives 2,616 of reads; the 970 EVO 790 and 863.
- **kTLS costs a core a side on Zen1.** A connection under kTLS ran at 450 to 1,040 MiB/s on titan
  and hyperion, and its rounds disagreed by up to half: the same cell ran at 557 MiB/s in one round
  and 1,039 in another, with nothing else on the host. europa's swung the same way, between 1.7 and
  3.0 GiB/s.

cpu a GiB, the server's executor, over loopback (titan's; europa's falls the same way at a third of
the cost):

| ms/GiB | write, file, plaintext | write, file, kTLS | read, memory, plaintext | read, memory, kTLS | read, memory, kTLS, no padding |
| --- | --- | --- | --- | --- | --- |
| 64 KiB | 1,028 | 1,454 | 651 | 1,603 | 1,587 |
| 256 KiB | 501 | 1,191 | 380 | 1,056 | 1,161 |
| 1 MiB | 368 | 1,128 | 320 | 891 | 888 |
| 4 MiB | 320 | 1,092 | 361 | 875 | 871 |
| 8 MiB | 311 | 953 | 374 | 879 | 866 |

and the client's, which is where a read's decryption lands:

| ms/GiB, client | read, memory, kTLS | read, memory, kTLS, no padding |
| --- | --- | --- |
| titan, 1 MiB | 943 | 796 |
| titan, 4 MiB | 869 | 734 |
| europa, 1 MiB | 347 | 288 |
| europa, 4 MiB | 310 | 266 |

- **The frame.** cpu a GiB falls steeply to 1 MiB and is nearly flat above it, within about a
  sixth, in plaintext and under kTLS. 64 KiB costs 1.3 to 2.8 times what 1 MiB does, and 256 KiB a
  fifth to a third more in plaintext. The statement made in advance named 256 KiB for writes and 4 MiB for reads, by the best
  kTLS rate, which is the figure that swung by half between rounds; by the steadier cpu a GiB the
  frame is 1 MiB.
- **`TLS_RX_EXPECT_NO_PAD` takes 10 to 20% off the receiver's cpu.** With it the kernel decrypts a
  TLS 1.3 record straight into the reader's buffer. Shoal's kTLS does not set it
  ([O93](../appendix/optimizations.md#o93-ktls-receivers-are-not-told-records-carry-no-padding)).

Across 1 GbE every cell ran at the link's 112 MiB/s, plaintext and kTLS alike, from 64 KiB frames.
Its cpu a GiB is in the report: a Zen1 server spent 1.7 to 2.5 s of a core for every GiB written in
plaintext, a fifth to a quarter of a core at the link's rate, and half as much again under kTLS.

## 2. The window, and what a stream holds

At 1 MiB frames, to and from the file:

| MiB/s | europa write, plaintext | europa write, kTLS | europa read, plaintext | europa read, kTLS | titan write, plaintext | titan read, plaintext |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,882 | 1,164 | 1,578 | 955 | 620 | 588 |
| 2 | 2,378 | 2,044 | 2,615 | 1,417 | 791 | 863 |
| 4 | 2,380 | 2,007 | 2,615 | 1,554 | 791 | 863 |
| 8 | 2,376 | 2,195 | 2,615 | 2,192 | 791 | 863 |
| 16 | 2,362 | 1,994 | 2,615 | 2,411 | 791 | 863 |

- **Two frames in flight reach both devices in plaintext**, and one reaches 60 to 80% of them.
  Under kTLS europa's writes settled at two and its reads kept gaining to sixteen; titan's kTLS rates
  swung between rounds at every window (580 to 790 MiB/s).
- **A stream's memory is its window, and the kernel's buffers beside it.** The server held exactly
  window × frame of its own buffers for a write. The sending socket held 2.6 to 4.1 MiB, which is
  `tcp_wmem`'s ceiling, and the receiving one up to 1.9 MiB plaintext, whatever the window. So a
  stream costs its window plus up to about 4 MiB a side the process never sees, and a budget for
  object bytes that counts only buffers it allocated undercounts by that much
  ([S13](isolation.md#memory)).

## 3. A small request beside a stream

A small request's p99 in µs, with the ratio T1 judged, the stream's connection over a connection of
its own to the same executor, round by round. The read rows are item 213's repeat, with the server
writing small frames first as each connection asked; the figures X11 first recorded, every one of
them taken first in first out, are struck beside them. A write's frames come from the client, whose
writer always wrote small frames first, so the write rows stand:

| Leg | Stream | Alone | On the stream's connection, small frames first | With `TCP_NOTSENT_LOWAT` 16 KiB | The same, first in first out | Own connection, same executor | Own connection, other executor | Ratio |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa loopback | read 1 MiB kTLS | 46 | ~~2,217~~ 2,036 | ~~1,804~~ 1,207 | ~~2,259~~ 2,021 | ~~222~~ 291 | 46 | **~~9.4~~ 7.2** [6.8–7.6] |
| europa loopback | write 1 MiB kTLS | 46 | 2,872 | 34,522 | — | 213 | 47 | **13.5** [4.9–19.4] |
| europa loopback | write 1 MiB plaintext | 37 | 2,978 | 1,665 | — | 120 | 37 | **24.7** [23.0–33.1] |
| titan loopback | read 1 MiB kTLS | 123 | ~~3,525~~ 4,897 | ~~3,073~~ 2,016 | ~~4,756~~ 3,923 | ~~2,682~~ 851 | ~~155~~ 158 | ~~1.46 [1.20–3.71]~~ **4.25** [1.90–5.96] |
| titan loopback | write 1 MiB kTLS | 123 | 6,944 | 6,325 | — | 586 | 156 | **11.9** [3.4–15.8] |
| europa to titan | read 1 MiB kTLS | 185 | ~~34,602~~ 28,710 | ~~34,954~~ 10,127 | ~~34,614~~ 34,609 | ~~1,583~~ 1,613 | 1,089 | **~~21.9~~ 17.8** [16.8–29.7] |
| europa to titan | write 1 MiB plaintext | 164 | 32,830 | 11,959 | — | 3,202 | 3,206 | **10.3** [9.4–11.5] |
| titan to hyperion | read 1 MiB kTLS | 205 | ~~34,589~~ 31,244 | ~~34,937~~ 10,418 | ~~34,564~~ 34,600 | ~~1,612~~ 1,645 | 1,084 | **~~21.5~~ 18.2** [15.9–19.5] |
| titan to hyperion | write 1 MiB kTLS | 205 | 31,916 | 10,490 | — | 1,089 | 1,106 | **29.5** [25.3–33.5] |

- **T1 fires on every leg.** Across the network a read stream's neighbour waits ~~about 34.5 ms~~
  28.7 to 31.2 ms at its p99 beside 1 MiB frames, 8.6 ms at 256 KiB and 2.5 ms at 64 KiB, against
  1.1 to 1.6 ms on a connection of its own: the bytes queued ahead of it are three or four 1 MiB
  frames at 112 MiB/s. Over loopback titan's kTLS read now fires too, at 4.25 times, where X11 first
  measured 1.46: its own connection's p99 was 851 µs in the repeat against 2,682 µs before, and
  titan's kTLS rounds disagreed with each other in both runs.
- **~~`TCP_NOTSENT_LOWAT` cannot help a read~~ With small frames first, `TCP_NOTSENT_LOWAT` helps
  a read.** It bounds the bytes not yet sent, so once the server writes a small answer before the
  frames still in its own queue, the answer waits behind only what the socket holds: 10.1 to
  10.4 ms at 16 KiB across the network beside 1 MiB frames, against 34.6 ms first in first out and
  28.7 to 31.2 ms small first alone; 3.6 to 3.8 ms beside 256 KiB frames against 8.6; over loopback
  1.2 ms against 2.0 on europa and 2.0 against 4.9 on titan. It never reached a connection of its
  own, 6 to 10 times it across the network. X11 first recorded that it could not help a read at
  all, because the answer waited behind the window's frames in the server's own queue, which a
  socket option does not reach ([item 213](../appendix/resolved/x11-setup-fifo.md)), and a node
  sets none ([O94](../appendix/optimizations.md#o94-a-nodes-sockets-set-no-tcp_notsent_lowat)). On a
  write it bounds the client's own unsent bytes, and the gap narrowed to 3 to 12 times. Under kTLS on
  the sending side it was far worse than nothing: 34 ms at 1 MiB on europa's loopback, and seconds
  at 8 MiB (5.2 s at the p99 with 16 KiB). That looks like a record left half written until an ACK
  frees room for the rest; X11 did not trace it.
- **Writing small frames first at a frame's boundary, against first in first out, ~~changed little~~
  helps where the server's own queue is what a small answer waits behind, and not where the socket
  is.** Over loopback, under kTLS: 152 against 210 µs at the p99 beside 64 KiB frames and 346
  against 500 beside 256 KiB on europa, 321 against 397 and 424 against 703 on titan, and 8.9
  against 13.7 ms beside 8 MiB on europa; beside 1 MiB, 2.0 ms either way on europa and within
  titan's noise. Across the network 28.7 to 31.2 ms against 34.6 beside 1 MiB, and the same below
  it, because the socket held megabytes. X11 first recorded that it changed little from two
  columns that had both run first in first out ([item 213](../appendix/resolved/x11-setup-fifo.md)).
- **A connection of its own works because the queue is per flow.** `fq_codel` schedules flows
  fairly, so the neighbour's packets do not queue behind the stream's. Across the network a write
  stream still raised its neighbours' tail on the sending host (3.2 ms against 0.16 alone), where
  both flows share one NIC; that is a link's, not a connection's.
- **Under kTLS the executor itself is the queue.** The same executor's other connection waited
  2.7 ms at its p99 on titan beside 1 MiB frames (0.36 ms beside 64 KiB, 3.7 ms beside 8 MiB),
  against 0.16 ms on the other executor: a send of one frame is one syscall that encrypts the whole
  frame on the executor's core. Plaintext costs it 0.15 ms.
- **The largest frame at which a shared connection held T1's line** was 64 KiB on europa's
  loopback and 256 KiB on titan's, for reads under kTLS, and none across the network.

## 4. A connection handed over, and bytes that hop

The handoff check passed in all four rounds on all three hosts, plaintext and kTLS: executor B
answered the setup, `ktls::ulp_name` read `tls` on its socket, and two seconds of a write stream and
two of a read stream on the handed connection matched the pattern byte for byte.

A write stream accepted by executor A for executor B's file, over the direct stream:

| Leg | Frame | Hop: MiB/s | Hop: cpu a GiB | Hop with a copy: MiB/s | Hop with a copy: cpu a GiB | Handoff: MiB/s | Handoff: cpu a GiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| europa | 64 KiB plaintext | 0.46× | 2.77× | 0.45× | 2.80× | 1.03× | 0.99× |
| europa | 1 MiB plaintext | 0.90× | **1.72×** | 0.86× | 1.82× | 0.99× | 1.01× |
| europa | 1 MiB kTLS | 1.13× | 0.96× | 1.03× | 1.02× | 0.97× | 1.00× |
| titan | 64 KiB plaintext | 0.61× | 2.11× | 0.60× | 1.95× | 1.00× | 0.99× |
| titan | 1 MiB plaintext | 0.87× | **1.34×** | 0.77× | 2.11× | 1.00× | 1.00× |
| titan | 1 MiB kTLS | 0.84× | 1.27× | 0.88× | 1.37× | 0.93× | 1.06× |
| hyperion | 1 MiB plaintext | 0.88× | **1.30×** | 0.77× | 2.09× | 1.00× | 1.00× |

- **T2 fires on its second clause**: in plaintext at 1 MiB the hop cost 1.30 to 1.72 times the cpu a
  GiB of the two executors in every round, and at 64 KiB it halved the rate and doubled the cpu.
  Under kTLS the crypto is most of the cost, and the hop's share sank into its noise.
- **The copy the fork asks for costs another half.** A `DmaBuffer` is `!Send`, so moving one between
  executors takes an `unsafe` wrapper around one from the global allocator; without it the
  receiving executor copies the bytes into one of its own, at 2.1 times the direct stream's cpu a
  GiB on Zen1.
- **A handed connection costs what a direct one does.** Once B owns the descriptor the stream is B's
  alone, kTLS included: the kernel holds the session on the socket, not on the descriptor.

## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| A small query's tail on a shared connection moves by more than its budget | **Fires.** ~~9 to 32~~ 4 to 29 times at 1 MiB, by the median, on every leg but plaintext reads over loopback; ~~`TCP_NOTSENT_LOWAT` and writing small frames first do not bring it back~~ `TCP_NOTSENT_LOWAT` with small frames first narrows a read's to 2.4 to 10.5 times and does not bring it back ([item 213](../appendix/resolved/x11-setup-fifo.md)) | Object bytes get connections of their own in the client's pool, and so does any query stream past a frame |
| A connection cannot be handed to the executor that owns the slice, with kTLS on it | **Does not fire.** It can, with `dup` and `from_raw_fd`, in every round | The hop is a choice, not a cost every write has to pay |
| The hop is dear, which would make handing the connection worth building | **Fires**, in plaintext: 1.30 to 1.72 times the cpu a GiB at 1 MiB, twice at 64 KiB | S13 hands a lane's connections to the slice's executor; S1's optional row is worth building at M14 |
| kTLS bounds a stream below the device's rate | **Fires**, for reads: 650 MiB/s on a Zen1 core against 857, 1,730 against 2,553 | A stream that has to run at a device's rate under kTLS is spread over more than one connection |

## The comparison

| Option | Small requests beside it | Memory | Cost | Verdict |
| --- | --- | --- | --- | --- |
| Object frames on any pooled connection | ~~9 to 32~~ 4 to 29 times their p99 at 1 MiB | The window, and the kernel's buffers | Nothing more | Rejected by T1 |
| The same with `TCP_NOTSENT_LOWAT` | ~~Reads unchanged~~ Reads 2.4 to 10.5 times, with the server writing small frames first; writes 3 to 12 times; seconds under kTLS on the sender | Less kernel memory on the sender | A setting, and 1.2 to 1.8 times a read's server cpu a GiB across the network | Rejected: it narrows the gap, a connection of its own closes it |
| **Connections set apart for long streams (recommended)** | Unchanged: 1.1 ms across the network | The same, per connection | A few more connections a client | Taken |
| Smaller frames on a shared connection | 64 KiB held the line on loopback, nothing held it across the network | Less | 1.3 to 2.8 times the cpu a GiB at 64 KiB | Rejected |
| A separate port for object bytes | The same as connections set apart | The same | A second listener, its TLS and its auth | Rejected by S12, and nothing here argues for it |

| Option for bytes owned by another executor | Rate | cpu a GiB | Verdict |
| --- | --- | --- | --- |
| Hop: owned buffers across a channel | 0.87 to 0.90 at 1 MiB plaintext, half at 64 KiB | 1.30 to 1.72 times | What client connections pay, since one carries many slices' bytes |
| Hop with a copy | 0.77 to 0.86 | 1.8 to 2.1 times | Rejected: the copy is the fork's, not the design's |
| **Handoff of the connection (recommended for the object lane)** | The same as direct | The same as direct | Taken where a connection's bytes are all one slice's |

## Recommendation

**Object bytes travel on connections of their own**, recorded on
[S18](contract.md#q26-in-part-streamed-bodies-2026-10-05):

- **A client keeps connections apart for long streams**: an object's frames, a query bundle past
  the frame, a send its caller marks bulk. Two by default; S1's prerequisite for more than one frame
  a query ~~builds~~ built them for queries ([F73](../features/bodies-across-frames.md)), and M13's
  object operations use them.
- **A data frame is 1 MiB**, the frame a stream is cut into and M13's `ObjectData` carries: where cpu a GiB
  flattens, S12's proposal, and X6's chunk floor.
- **A stream's window is four frames**: two reach both SSDs in plaintext, and four absorbs kTLS's
  swings. A stream's memory is the window plus up to 4 MiB a side of socket buffers, which S13's
  budget counts.
- **The object lane hands each connection to the executor of the slice it names** (S13). A client
  connection, which carries many slices' bytes, hops its buffers, and an executor never moves bytes
  it could have read itself.
- **A stream that has to run at a device's rate under kTLS is spread over connections**: one Zen1
  core receives about 650 MiB/s of it. M15 spreads a long get's ranges over the client's connections
  set apart.
- **A kTLS receiver says records carry no padding**
  ([O93](../appendix/optimizations.md#o93-ktls-receivers-are-not-told-records-carry-no-padding)).
- **A small answer on a shared connection is helped by a low water mark, not saved by it**: with the
  server writing small frames first, `TCP_NOTSENT_LOWAT` cut a read's neighbour three times across
  the network and left it 6 to 10 times a connection of its own. A node sets none
  ([O94](../appendix/optimizations.md#o94-a-nodes-sockets-set-no-tcp_notsent_lowat)); it changes no
  decision here.
- **An executor writes object frames under kTLS in pieces short enough for its queue's goal**: one
  1 MiB send holds a Zen1 core about a millisecond
  ([todos](../appendix/todos.md#write-object-frames-under-ktls-in-pieces)).

## What X11 does not settle

- **The object lane's frames** between nodes: who sends a stage is Q14's and Q15's, and X1's.
  ✅ [X1](stripe-model.md) answered the second: the node the client's bytes arrived at, so stages
  leave every node and the lane carries them from any one.
- **The window as a setting and the memory budget's size**: M15, with S13, from this page's figures.
- **Ranges asked ahead on a read and their spread over connections**: M15's, against T3.
- **Whether kTLS's rounds disagree for a reason**: a cell ran at half its rate in one round and full
  in the next, with nothing else on the host. Neither the cpu's frequency nor the device was
  measured beside it.
- **Where object work runs**, Q24's: the executor encrypting a frame stalls its other work
  ([3](#3-a-small-request-beside-a-stream)), which is X9's to weigh with its other costs. ✅ X9
  put object work on executors of its own, and every loop in steps a latency goal can cut; it sent
  no frame, so a write under kTLS cut in pieces is still [filed](../appendix/todos.md#write-object-frames-under-ktls-in-pieces)
  ([the record](contract.md#q24-and-q15-in-part-table-latency-beside-object-work-2026-10-09)).

## What it did not measure

- **A link faster than 1 GbE.** Every rate across the network is the link's, and every number that
  says what a core does is from loopback.
- **Many streams at once**, on one connection or many; and a stream's effect on a table's queries
  through the engine. ~~X9 measures~~ [X9](table-latency.md) measured object work beside tables,
  sending nothing.
- **A key update** under kTLS, which a stream of many gibibytes on one connection will one day need
  ([F14](../features/encryption-in-transit.md#limitations)).
- **Other congestion control or queueing disciplines** than the kernel's defaults: CUBIC and
  `fq_codel`.
- **Crashes**, and a connection handed over mid-stream: the handoff happened before its first
  stream.

## What it found in the glommio fork

None of these is a defect, so none is filed on [Known Issues](../appendix/known-issues.md).

| Finding | Where |
| --- | --- |
| `TcpStream` has `FromRawFd` and no `IntoRawFd`, so handing one over takes a `dup` and a close; dropping a stream closes its descriptor and does not shut the socket down, so the duplicate lives on | `glommio/src/net/tcp_socket.rs`; the optional S1 row ([prerequisites](prerequisites.md#optional)) |
| `DmaBuffer` is `!Send` for its registered storage, though one from `allocate_dma_buffer_global` is a plain aligned allocation; moving it took an `unsafe` wrapper, and without one a hop copies | `glommio/src/sys/dma_buffer.rs` |
| A shared channel's `connect` waits for the other end's, so two executors that each connect their sender before their receiver wait for each other for ever; and its items must be `Sync` as well as `Send` | `glommio/src/channels/shared_channel.rs` |
| A non-buffered socket receives straight into the caller's slice, so a frame's payload lands in a buffer for direct I/O with no copy of the reactor's | `glommio/src/net/stream.rs` |

## Related

- [X11](spikes.md#x11-streamed-bodies) for what was planned; [S18](contract.md#q26-in-part-streamed-bodies-2026-10-05)
  for the decision; [Q26](contract.md#questions-to-answer).
- [S12](wire-and-client.md) and [S13](isolation.md), the pages these figures move.
- [S1](prerequisites.md#required), whose row for more than one frame a query is built to these
  figures, and [F73](../features/bodies-across-frames.md), which built it.
- [F14](../features/encryption-in-transit.md) for the kTLS measured here, and
  [X6's record](device-store-ssd.md) for the devices' rates T3 was judged against.
- `shoal-spike/results/x11-report.md`, every figure merged across rounds;
  `shoal-spike/results/x11-*-r*.md`, each round's tables.
