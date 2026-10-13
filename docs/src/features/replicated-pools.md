# F86 — M15, replicated pools across nodes

## Context

[M15](../object-storage/milestones.md#m15-replicated-pools-across-nodes) is the object store's
fifth gate, and the first at which a client's bytes reach a device. [F80](object-wire.md) held
every object in its path's `ObjectMeta` row and refused anything past its pool's threshold
`TooLarge`. [F85](devices-and-pools.md) built devices, slices, the pool map, placement and the
device store, and drove them only from tests. This gate wires
[S7](../object-storage/write-path.md)'s write path and [S9](../object-storage/read-path.md)'s read
path for **replicated** pools, across nodes, over a lane of their own.

Nothing in [S1](../object-storage/prerequisites.md) stood in front of it: its one open required row,
the tablet walk, is M16's. Q27, the small write, was closed by [X8](../object-storage/small-writes.md).
[X1](../object-storage/stripe-model.md) had agreed the rules this gate builds, each a repair of a
rule the pages stated that broke a clause: a tag a try, the row's tombstones, the truncate's fence,
a holder's confirmation for an untouched chunk, a chunk's previous state kept, rows read at
`Quorum`, and the small write in its commit.

Three decisions were taken with the user on 2026-10-13:

- **Where a placement group's generation lives, and the missed record.** A pool's placement is
  **pinned**: the control leader commits `PinPool` the first time a pool is ready, and every
  write and read of it places at that generation through the pool map's eras until M16 moves
  placement groups. A position a write missed is **marked in the row's label**, the write's
  label with the top bit of its tag set. No row layout changes; M16 replaces both with group
  state.
- **The small-write threshold.** `small_writes` on a pool defaults to 64 KiB when every device it
  places on has a volatile write cache, and to nothing otherwise; only a replicated pool takes it,
  and only while the rows' own groups survive the pool's `f`. The lab's inventory sets it, so the
  in-commit path runs on the lab beside staging.
- **The loopback leg.** Built into `shoaladm`: nodes that share a machine, and an inventory of
  three nodes on europa over `127.0.0.11–13` that `shoaladm bench` runs like any other.

The other decisions were mine, and are under **Design choices** with their reasons.

## What it does

### Configuration

Nothing new is required. A pool may name its small-write threshold; a node may change the timings
its object operations keep, and the byte bound of its object lanes:

```yaml
storage:
  pools:
    fast: {class: ssd, redundancy: {replicas: 3}, small_writes: 64KiB}   # 0 for none
  object_timings:
    decide_after: 30s        # a stager's time before its holder asks the row about its stage
    clear_wait: 2s           # how long a small write's writer waits for every holder to fold it
    straggler_wait: 200ms    # how long a stripe's other holders are waited for once k + f hold it
    inflight_stripes: 2      # a put's stripes awaiting their holders at once
    max_read: 4MiB           # the most bytes one read answers; a longer range is asked again
    put_window_frames: 4     # the frames of a put a connection reads ahead of its holders
cluster:
  transport:
    object_queue_bytes: 16MiB   # one link's queue to one slice of a peer
```

Wire version 9 is the object wire. It is what gates the pin, the lane and every stripe write: until
`activate 9`, which `shoaladm`'s bootstrap sends, a cluster keeps writing objects inline and refuses
a put past the threshold `NotReady`, naming the wire. A node with no object executor refuses one by
name too, `TooLarge`. A node that has one, reached before `ready` has started it, refuses one
`NotReady`, which a client tries again ([Resolved #237](../appendix/resolved/object-store-starts-after-the-shards.md)).

### A put past the threshold

At or under the pool's inline threshold (16 KiB by default) an object is held in its entry, as
F80 built it. Past it, a put is S7's **whole object in two commits**:

1. the entry is read strongly, the pool is judged by readiness and its kind, and the new object's
   id is **registered** in the entry as a replacement in flight - so no chunk is ever written for
   an id no row names;
2. the bytes are cut into stripes on one of the node's object executors and written straight into
   place on every holder under the put's label, `Label::PUT`; each 64 KiB unit's CRC-64/NVME is
   computed there once as the bytes arrive, folded into the put's own checksum, and sent with the
   unit so no holder reads the bytes again ([X5](../object-storage/checksums.md));
3. a stripe is held once `k + f` holders answered its last piece durably; the others are waited
   for `straggler_wait`, and a stripe that missed one is given a row naming that position missed;
4. once the folded checksum matches the one the client named, one conditional commit of the
   entry makes the id current, retires the one it replaced, and moves the content version.

A failure after the registration commits the id away, retired, for M20's reclamation. A reader sees
the old object or the new one ([P19](../object-storage/contract.md)), and a retried put finds its id
and finishes it.

The client's bytes arrive as F73's windowed stream, its first user: a put past the threshold is
handed to the pipeline frame by frame, and the connection reads at most `put_window_frames` ahead
of what the holders took, so a holder that stops reading stops the client rather than growing a
node's memory.

### Writing in place, appending, truncating

A write in place is S7's three steps on every stripe it touches:

1. **read** - the entry strongly, for the truncate epoch, then the stripe rows at `Quorum` through
   their groups' leaders; a row fenced past the epoch read is a truncate committing or dead, and
   the epoch is moved past it (`Advance`); a row stamped past it means the entry is read again;
2. **stage** - the new value of every unit the write changes, a unit changed in part read first and
   laid over, the units a floor hides written as zeros, and any pending bytes carried; a whole
   chunk staged in pieces, part of one as units over the labels the row names, every unit with its
   checksum; under a label of the write's own, a tag a try;
3. **commit** - once `k + f` holders staged durably, one conditional commit of the stripe's row on
   the sequence read, its identity the write's, the stripe's and the try's, and every holder that
   did not stage marked missed. The holders are told to apply after the answer.

A refused commit is a lost race: its stages are discarded, the row is read again, and the write
goes again over what is there now. A commit whose outcome is unknown is sent again byte for byte;
if it stays unknown the write is answered unknown and **nothing is discarded**, since the commit
may still land.

A write past the object's end commits its stripes first and its size after, conditional on the
epoch it read. An append claims its region first - the size moved on the entry's version, so two
appenders never claim one region - and then writes it. An inline object a write grows past the
threshold moves to stripes once, and never back.

A truncate fences the stripe its cut falls inside, then commits the entry at the epoch and size it
read, with the cut as a floor; if either moved it reads and fences again. A floor hides every byte
past it from every stripe stamped below it, so a stripe committed past a cut by a writer that read
the size before it never shows (P13). An entry keeps at most sixteen floors until M20 reclaims one;
a seventeenth is refused `TooManyFloors`.

### The small write in its commit

A write smaller than the pool's threshold rides inside its commit, as X1 modelled and X8 priced:

- every position's holder **confirms**, in this write's round, the chunk its bytes go over, and
  the write needs `k + f` confirmations;
- the commit carries the bytes as the row's **pending bytes**, merged over any already there and
  bounded by 64 KiB; a write that would take them past it is staged instead;
- the writer then has every holder **fold** them - journalled over a label of the bytes' chain and
  applied - and **clears** them from the row once `k + f` hold the label, marking the rest missed;
  it waits `clear_wait` for every holder before it clears with the ones that did;
- a writer that dies after its commit leaves the bytes in the row. A read that meets them there is
  served them laid over a chunk at their base, and a shard that has met the same pending label for
  `clear_wait` by **its own clock** drives the fold and the clear itself. Nothing is decided by a
  time a row carries.

### Reading

A read is S9's: the entry at the read's level - strong unless it says `One` - then one get of every
stripe row the range falls in at `Quorum`, then each stripe's bytes from a holder the row calls
current, a holder on this node first, at the label the row names or at a label its pending bytes
fold from with them laid over. A hole, and bytes a floor hides, read as zeros without asking any
holder. A holder that answers with a newer label moves the reader's row forward; an older or
unknown one is stale and another is asked; a marked position is never asked.

A read answers at most `max_read` bytes. The client asks for the next range from where the last
stopped, pins the object's id and content version across them, and starts again from the beginning
if either changes, at most eight times ([S12](../object-storage/wire-and-client.md)).

### Deciding what a stager left

A stager that died between its stages and its commit leaves bytes staged that no committed fact
excludes. Shard zero of every node asks its node's store, every half `decide_after`, for the stages
older than that and every stage a start found, and settles each by its row read at `Quorum`:

- the row names its label at its position: it committed, and is applied;
- the row's pending bytes fold from it: a committed record a named label stands on, kept;
- the row is at or past its sequence and names it nowhere: excluded, and discarded;
- otherwise nothing decided it, and a **no-op** moves the row's sequence past it - or makes the row
  at its object's labels, where there was none - and the row is read again.

Every proposal goes to the row's group's leader, so this is S7's leader's no-op, prompted by the
holder's timer. A refused no-op means the row moved and is read again; an error decides nothing.

### The pin

`ControlCommand::PinPool { pool, generation }` is committed by the control leader beside its pool
fits, the first time a pool's readiness says ready and once its fit at the map's generation has
finished. It is refused before wire 9, for a pool the policy does not define, at a generation the
map has not reached, and for a pool already pinned elsewhere; it moves the topology version, never
the pool map's generation. A pinned pool is never fitted again, and its weights and exceptions are
refused by name. Readiness is judged at the pin for a pinned pool, and `pools` says where each is
pinned. A standalone node pins in its own `shoal-poolmap.json` at its first ready start.

### The object lane

Bytes between nodes travel on a fifth lane, `Lane::Object`, behind the optional capability
`CAP_OBJECT_LANE_V1` and the object wire ([S13](../object-storage/isolation.md#a-lane-for-object-bytes)):

- **A connection a remote slice.** Each object executor dials one link to each remote slice it
  sends to, lazily, with the shard links' backoff, hello and kTLS, its queue bounded by
  `object_queue_bytes`. ~~A request that does not fit is refused at once, unavailable, which its
  sender counts as a holder that did not answer.~~ A request that does not fit waits for room while
  the link goes on writing, so a put goes at the rate its holders take bytes. It is refused,
  unavailable, only once the link has written nothing for `STALL_AFTER` (5 s), and a put then
  sends that holder nothing more and counts it missed ([Resolved #238](../appendix/resolved/object-lane-backpressure.md)).
- **The hello names the slice.** Only an object lane's hello grows, by the slice's sixteen bytes
  after the record, so every other lane's is as it was.
- **Handed to the slice's owner.** The shard that accepts an object lane gives the connection,
  whole and with nothing read past its hello, to the object executor that owns its slice, the way
  [F84](replication-lane-handoff.md) hands a replication lane to its slot's executor. That executor
  reads, serves and answers every request on it; **object work never runs on a table shard**.
- **Frames.** `ChunkRequest` (30) and `ChunkResponse` (31): fixed little-endian heads and never an
  archive. A request is a piece of a put's whole chunk, a piece of a staged whole chunk, the new
  values of some units, an apply, a discard, a confirmation, a fold or a read; an answer is done, a
  refusal by store code, a label, or bytes. **Every request is answered once**, a piece before its
  chunk's last once it is written. A frame that cannot be right ends its connection.
- **Bounded on both ends.** A served lane reads at most 256 requests ahead of its answers, and
  draws every request's buffers from its executor's budget.

A request sent from an object executor - a put's pipeline - goes out on that executor's own link;
one sent from a table shard is handed to the executor the holder is dealt to, so one caller's
requests to one holder keep their order.

### A chunk's previous state

A holder keeps a chunk's state before its last apply until its next one, so a reader one write
behind is served the label it names rather than sent to read its row again; X1 found readers of a
stripe being written failing by name without it. A whole chunk's file is **hard linked aside**
under the slice's `previous/` before the apply renames another over it, and its own header is read
only when a reader asks; a partial apply's overwritten units are **read and kept in memory**, drawn
from the executor's budget and not kept when it has no room. A state is let go at the chunk's next
apply, after 30 seconds, and at every start, which empties `previous/`.

### The standalone node

A standalone node runs the same coordinator, every holder local and its rows ordered by its
intent log. Its pool may only name `device` failure domains among one node's devices, and its own
pool map is pinned in its file at its first ready start.

### The client

`Objects` gains `write_at`, `append` and `truncate`, each retried under its identity while what
failed it may be retried; `NotReady` and `Busy` are retryable outcomes. `get` and `ObjectFile` read
in ranges as above. `ObjectFile` writes as `AsyncWrite`: what is written is buffered to a mebibyte
and flushed as a `write_at`.

### Operations

- `shoaladm pools` says where each pool is pinned, and `status` shows each device's staged bytes as
  F85 drew them.
- **Nodes that share a machine.** shoaladm reads `/etc/machine-id` with the cpu probe; nodes behind
  one ssh target each get a unit `shoal-<cluster>-<node>`, a directory
  `/opt/shoal-deploy/<cluster>/<node>`, listeners on their own address and the control core they
  name (`resources.control_core`), which they may share, as X3's and X8's loopback nodes did; they
  are refused by name if one's shards or object cpus, by the machine's topology, land on any cpu of
  another's, if their roots or devices overlap, or if two targets reach one machine. A
  node alone on its machine keeps `shoal-<cluster>`, so `shoal-tmdb` is unchanged.
  `shoaladm/inventories/loopback.yml` is three nodes on europa over `127.0.0.11–13`.
- **The bench.** An object mix may write in place: `patch100` (4 KiB) and `patch100-32k`, or a
  `patch:N` weight, each rewriting a preloaded object's own bytes at a 4 KiB boundary so reads and
  the verify pass stay exact. Every run of an arm that writes objects records `staged_left`, the
  bytes every slice still held staged a minute after its last answer, and says so when it is not
  zero. A run's device counters ([F71](bench-device-memory.md)) read each node's pool devices
  beside its storage roots, labelled `device`, since a stripe's bytes are written there and on the
  lab's Zen1 hosts that is a volume no root is on.

### The fixture

A child serves a second schema, `ObjectDb` - a persistent table and a bucket, `Reels` - when its
cluster's pools bind that bucket, so every test before M15 keeps the tables, ids and groups it was
written against. Its verbs: `STRIPE_CRASH_AT <point>|none` for S16's six crash points (a holder synced
before it answers, the coordinator answered, proposed and committed, a holder half way through and
once synced before it drops its record); `HOLD_STAGER <ms>`; `STALL_OBJECT_LANE on|off`;
`FORCE_OBJECT_ID <prefix> <id>`; `OBJECTS`, every record no row has decided, the lanes' figures and
the bytes reads answered. `decide_after` is shortened by `Cluster::builder().decide_after`, and
`restart_on_its_port` brings a node back where a client's retry can reach it.

### Found on the way

Three defects in this gate's own code, each found by an acceptance test before anything was
committed:

- a write in place covering a whole chunk staged it with no unit checksums, which a local holder
  computed and the lane rightly refused; a stage now cuts the chunk and computes every checksum
  once, for every holder;
- units staged or folded crossed with a checksum of zero, which nothing read; they now carry each
  unit's CRC, and a holder refuses units that do not match it;
- a stalled lane, the fixture's, still served the one request its reader was waiting on; it now
  holds a request read while the stall began.

## Design choices

- **A pin a pool, not a generation a placement group.** S5 gives every placement group a
  generation, which M16's moves need. Nothing moves before M16, so one generation a pool says the
  same thing, costs one map entry, and is committed once. A device that joins later is not placed
  on by a pinned pool until M16; a stripe never names two generations.
- **The missed mark in the label's top bit.** Every rule X1 reads as `labels[pos]` compares the
  unmarked label and every rule it reads as `missed[pos]` the bit, so the row keeps its layout and
  the conformance test holds both encodings to the model. A lane frame carrying a marked label is
  malformed: no holder holds that state.
- **Identity a try.** A stripe commit's bundle id is the write's, the stripe's and the try's; its
  tag adds the coordinating task's nonce, so two coordinators of one write never stage different
  bytes under one label. The retry table answers a reused identity on a generated table with what
  it was remembered as - `IdentityReused` "applied" or "refused" - so a retry on a new coordinator
  probes its tries from zero and learns which committed. A client's own reuse of an identity with
  another payload is still refused, as before.
- **The decide poll on the holder's node.** S7 asks for the leader's no-op; the leader of a row
  group knows nothing of a stage on a slice. The holder's node does, so its shard zero reads the
  row and proposes through the leader. No new message exists for it.
- **Reads in ranges.** A read answers at most `max_read`, assembled and checksummed whole as F80's
  were, so the answer's head keeps its CRC and the outbox stays as it was; S12 already described a
  long read as ranges asked for.
- **Checksums from the coordinating node.** The client's wire is unchanged: the object executor
  that cuts a put computes each unit's CRC once and holds the put to the head's. F80's todo for a
  client's checksum a unit stays open.
- **The previous state, linked and held.** A hard link keeps a whole chunk's old inode for a
  rename's price, nothing read; a partial apply reads only the units it overwrites. Both are
  readers' conveniences: dropped first under memory pressure, never persisted, never staged over.
- **Links per object executor.** A put's pipeline sends from the executor it runs on, on that
  executor's links, so its bytes never hop an executor ([X11](../object-storage/streamed-bodies.md));
  a node holds a link a remote slice for each executor that sends to it.
- **A second fixture schema.** A bucket in `TestDb` would have given every fixture test two more
  persistent tables, their groups and their tablets, changing what a hundred and seventy tests
  count; a schema chosen by the pools a cluster binds changes nothing for them.

## Alternatives rejected

- **A generation a placement group now.** It needs a place for group state the pool map does not
  have and M16 builds; pinned pools give the same answers until then.
- **A row field for the missed positions.** A layout change to every stripe row for a record M16
  replaces; the label's top bit carries it.
- **Discarding stages after a timeout.** A commit in flight may still land, and its holders would
  have nothing to apply ([P16](../object-storage/contract.md)); only a definite refusal discards.
- **The control leader deciding stages.** It does not know they exist; the holder's node does.
- **One link a remote slice for the whole node.** Every object executor's sends would hop to the
  link's executor, which X11 measured at 1.3 to 1.7 times the cpu a gibibyte.
- **Stalling a lane in the kernel.** A peer that stops reading is what a stall is; holding its
  requests unserved on the executor that owns them reproduces it without a network fault.
- **A time in the row for a dead writer's pending bytes.** S3 says nothing is decided by a time a
  row carries; each shard's own clock since it met them is enough, and a write meeting them carries
  them anyway.

## Limitations

- **A missed holder stays stale.** Reads go round it and nothing rebuilds it; that is M16, as are
  a rebuild and a move over pending bytes, which M15's deliverables named and this gate defers.
- **A pinned pool places on no device that joined after its pin**, until M16 moves placement
  groups; a consumer bound after the pin gets no exceptions fitted for it.
- **No erasure coded pool takes a write.** Readiness refuses first, then `Unsupported`: M18's.
- **Nothing is reclaimed but a losing stage.** A retired object's chunks, a truncated stripe and a
  floor stay until M20; an entry with sixteen floors refuses another truncate.
- **A stripe nobody touches keeps a dead writer's pending bytes** in its row, read correctly and
  bounded by 64 KiB, until a read or a write meets them.
- **A standalone node has no retry table**: a commit resent after an unknown outcome is applied
  again, the same bytes at the same place.
- **The previous state is a convenience**: lost at a start, not kept when the budget has no room,
  and a whole one's linked file holds its space for up to 30 seconds.
- **A long read is ranges on one connection**; spreading them over the set-apart connections is
  filed in `todos.md`. `put_from` and `get_into` are not built: `ObjectFile` reads and writes.
- **The inventory does not render `object_queue_bytes`**; a node's file may set it.
- **A put's window is counted in frames**, `put_window_frames`, not drawn from the object
  executor's budget: F85's `try_draw_stream` still has no caller.
- **The crash test's history is checked by the tablet oracle's `Ledger`**, a stripe as one key and
  each write as a value; S16's `ByteLedger`, which would check every byte range, has its first writer
  still to come.
- **A read with no level is strong**, `Quorum` for its entry as F80's reads were; S9's default read,
  the entry at `One`, is what a client asks for by naming `One`.

## Invariants to uphold

- **A stripe is placed at its pool's pin and nowhere else**, through the eras that keep the map's
  past, and a pinned pool's weights and exceptions never change.
- **No chunk is written for an id no row names**: a put registers its id before its first piece.
- **A stage is discarded only once nothing can commit it** - after a refusal, an answer naming
  another try, or never proposed - and only on a committed fact read from its row by the decide.
- **A holder is told to apply only the label its task committed**; one that a reused identity
  committed is learned from the row.
- **A write is acknowledged on its holders' word alone**: `k + f` durable answers in distinct
  domains, every other position marked missed in the commit that acknowledges it (P11).
- **A reader never takes a chunk ahead of its row** and never asks a marked position; it moves its
  row forward instead (P10).
- **Every chunk request is answered once**, and a link that drops answers every request it carried.
- **A sender waits until its request is on the lane's queue**, while the lane writes, and is
  refused only by a lane that wrote nothing for `STALL_AFTER`: a refusal means a stalled holder,
  never one that is behind ([Resolved #238](../appendix/resolved/object-lane-backpressure.md)).
- **Object work never runs on a table shard**: a lane is handed to its slice's owner before a
  request is read off it.
- **Nothing is decided by a time a row carries.**
- **A previous state is never staged over, confirmed or persisted.**
- **A record keeps every label it may be laid over**, and an apply drops every record at or
  below the sequence it applied ([Resolved #239](../appendix/resolved/record-bases.md)). A write
  whose holders could not take it reads its row again before it answers `NotReady`
  ([Resolved #240](../appendix/resolved/moved-row-not-ready.md)).

## Performance

### The previous state

`shoal-spike store --only previous` applies one 4 KiB unit in place, and a whole 1 MiB chunk over
one, back to back under the shared flush, with the state kept and not. On europa's Optane, quick, a
core under the `powersave` governor, the one-unit apply's p50 went from 174 µs to 209 µs - the unit
it overwrites read first - and the whole chunk's from 231 µs to 225 µs: the link costs nothing a
quick run can see. ~~The lab's figures are taken with the rest of this gate's evidence.~~

On the lab, `shoal-spike/results/previous-host.sh` ran four rounds a host. Each host had its node
stopped and the `performance` governor, and the spike ran on cpu 2. The records are in
`shoal-spike/results/store/previous-*.json`. Each figure below is the range of the four rounds'
figures, and a difference is quoted only where the two ranges are disjoint:

| Host, device | Apply | Not kept, p50 µs | Kept, p50 µs | Not kept, p99 µs | Kept, p99 µs |
| --- | --- | --- | --- | --- | --- |
| titan (V1756B), 970 EVO, XFS | one 4 KiB unit | 1,339–1,408 | **1,508–1,572** | 1,625–4,218 | 1,620–4,440 |
| titan | a whole 1 MiB chunk | 4,091–4,171 | 4,135–4,203 | 7,108–7,343 | 7,185–7,407 |
| europa (7945HX), Optane, XFS | one 4 KiB unit | 161–167 | **200–203** | 190–195 | **225–328** |
| europa | a whole 1 MiB chunk | 212–219 | 208–214 | 675–761 | 679–809 |

Keeping a unit's previous state costs its apply about 170 µs at p50 on the 970 EVO, and about
40 µs on the Optane, at p99 too. That cost is the read of the unit the apply overwrites, which is
kept in memory. A whole chunk's previous state is a hard link to the file it replaces, and costs
nothing either host's rounds can tell apart. The 970 EVO's p99 is its two flush regimes, as
[X6](../object-storage/device-store-ssd.md) found, in both columns.

### The metadata cost

`metadata_cost_is_linear_and_published` counts an archived `ObjectMeta` row at 556 bytes for a
short path, whatever its object's size, and a stripe row with no pending bytes at 192. The test
holds them to **1 KiB an object row and 256 bytes a stripe row**, which is the constant
[S3](../object-storage/objects.md) publishes: a put of any size is one row, and every stripe written
in place is one more and nothing else.

### On the lab

M15's evidence was taken with `shoaladm bench` from europa (a Ryzen 9 7945HX) on 2026-10-13. The
captures are under the bench worktree's `target/shoaladm-bench/` and are not committed. The node
program and the loader were built for znver1, and every node's host ran the `performance` governor
for its runs, which the provenance records as `governor_ran`.

- **The legs.** One node on titan (a Ryzen Embedded V1756B), Posters in a pool of one copy. Three
  nodes, the bench's own copy of `tmdb_cluster.yaml`: europa's Optane and titan's and hyperion's
  970 EVO, three copies a host apart. And the loopback leg, `shoaladm/inventories/loopback.yml`:
  three nodes on europa sharing its one Optane. Each node's pool device is a directory of its own
  on its host's SSD. On titan and hyperion that is the `/xfs` volume (`dm-1`) beside the roots'
  (`dm-0`), which the bench now counts too.
- **The arms.** F80's, unchanged: `put100` and `get100` at 4 KiB to 16 MiB, eight in flight,
  15 seconds measured after three of warmup, four runs, half of 64 seeded objects preloaded. The
  pool's threshold is its default 16 KiB, so every object from 64 KiB up is stripes. Every
  acknowledged put was read back, and none was lost or misread.
- **Which commit.** The put and get legs and the paced capture ran at `4566a64`, the patch leg at
  `6046094`, and the streaming put at `6046094`. `6046094` changed only writes in place
  ([Resolved #239, #240](../appendix/resolved/record-bases.md)), which those legs do not make.
  Four defects the lab found were fixed before these figures were taken; they are listed below.

Every figure is `median [lowest–highest]` across the runs. "Device bytes a byte" is what
`/proc/diskstats` counted written on every node's devices, roots and pool alike, over the run,
over the bytes the client sent.

#### Stripes against the M13 baseline

**One node** (titan, `f86-one-4566a64`):

| Arm | ops/s | MiB/s | p50 ms | p99 ms | worst second's p99 ms | device bytes a byte |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| put 4 KiB | 1,081 [1,032–1,091] | 4.22 | 6.92 [6.86–7.13] | 14.5 [14.4–14.8] | 17.8 [17.4–19.4] | 5.63 |
| put 64 KiB | 58.1 [56.8–58.7] | 3.63 | 119 [117–122] | 296 [290–300] | 342 [305–371] | 66.51 |
| put 256 KiB | 51.8 [48.0–55.5] | 12.9 | 129 [122–147] | 311 [297–318] | 334 [321–349] | 17.38 |
| put 1 MiB | 48.1 [45.9–49.7] | 48.1 | 141 [137–149] | 335 [326–337] | 371 [358–373] | 5.10 |
| put 4 MiB | 27.3 [27.1–27.4] | 109 | 286 [286–287] | 415 [406–426] | 466 [455–488] | 2.04 |
| put 16 MiB | 6.93 [6.93–6.93] | 111 | 1,142 [1,142–1,143] | 1,284 [1,250–1,493] | 1,319 [1,263–1,497] | 2.03 |
| get 4 KiB | 15,449 [11,036–18,600] | 60.3 | 0.53 [0.42–0.72] | 0.84 [0.64–1.15] | 0.91 [0.69–1.18] |  |
| get 64 KiB | 1,788 [1,787–1,788] | 112 | 4.47 [4.47–4.47] | 5.53 [5.05–6.01] | 6.40 [6.10–6.65] |  |
| get 256 KiB | 448 [448–448] | 112 | 17.9 [17.7–18.0] | 22.8 [22.2–23.5] | 23.3 [22.7–24.2] |  |
| get 1 MiB | 112 [112–112] | 112 | 71.2 [58.0–71.3] | 79.7 [73.2–90.2] | 81.0 [74.5–92.0] |  |
| get 4 MiB | 28.0 [28.0–28.1] | 112 | 258 [216–285] | 353 [289–376] | 355 [321–380] |  |
| get 16 MiB | 7.00 [6.93–7.00] | 112 | 1,139 [1,139–1,140] | 1,293 [1,159–1,692] | 1,413 [1,175–1,711] |  |

**Three nodes** (`f86-three-4566a64`):

| Arm | ops/s | MiB/s | p50 ms | p99 ms | worst second's p99 ms | device bytes a byte, all hosts (a host) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| put 4 KiB | 1,019 [949–1,038] | 3.98 | 7.20 [7.12–7.82] | 14.5 [14.2–16.8] | 16.5 [15.7–25.4] | 15.50 (5.17) |
| put 64 KiB | 59.1 [58.5–60.5] | 3.69 | 120 [119–121] | 264 [257–303] | 362 [297–375] | 199.63 (66.54) |
| put 256 KiB | 55.7 [55.3–56.6] | 13.9 | 127 [126–127] | 297 [277–314] | 374 [361–389] | 52.08 (17.36) |
| put 1 MiB | 48.6 [48.3–49.1] | 48.6 | 151 [147–152] | 314 [305–325] | 382 [328–417] | 15.28 (5.09) |
| put 4 MiB | 19.9 [17.3–21.9] | 79.7 | 278 [269–470] | 1,020 [810–1,185] | 1,193 [849–1,332] | 6.11 (2.04) |
| put 16 MiB | 4.67 [4.07–5.27] | 74.7 | 1,157 [966–1,829] | 4,954 [4,612–5,255] | 4,954 [4,612–5,255] | 6.08 (2.03) |
| get 4 KiB | 10,628 [9,336–11,683] | 41.5 | 0.68 [0.65–0.71] | 1.89 [1.43–2.09] | 2.19 [1.62–2.77] |  |
| get 64 KiB | 2,816 [2,076–3,926] | 176 | 2.63 [1.52–3.96] | 6.99 [5.75–7.55] | 7.61 [6.89–9.80] |  |
| get 256 KiB | 877 [646–1,127] | 219 | 8.88 [5.06–13.5] | 23.2 [20.6–27.2] | 26.4 [22.5–32.6] |  |
| get 1 MiB | 226 [189–397] | 226 | 21.9 [6.25–51.2] | 108 [87.6–115] | 117 [92.1–154] |  |
| get 4 MiB | 57.9 [48.8–73.8] | 232 | 102 [12.7–197] | 456 [438–481] | 556 [519–582] |  |
| get 16 MiB | 12.5 [8.80–16.4] | 199 | 528 [54.5–1,041] | 1,499 [1,395–1,678] | 1,635 [1,403–1,888] |  |

**Three nodes over loopback** (europa, `f86-loop-4566a64`):

| Arm | ops/s | MiB/s | p50 ms | p99 ms | worst second's p99 ms | device bytes a byte |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| put 4 KiB | 2,355 [2,351–2,370] | 9.20 | 3.10 [3.09–3.11] | 6.59 [6.56–6.61] | 8.49 [7.34–8.65] | 10.39 |
| put 64 KiB | 192 [190–194] | 12.0 | 44.8 [44.3–45.7] | 61.9 [61.5–63.7] | 66.5 [64.0–70.2] | 197.73 |
| put 256 KiB | 184 [183–184] | 45.9 | 46.8 [46.6–47.0] | 63.5 [62.8–64.2] | 71.4 [66.8–72.7] | 51.68 |
| put 1 MiB | 155 [152–156] | 155 | 55.1 [54.5–55.8] | 72.4 [71.6–73.3] | 76.6 [75.5–77.5] | 15.18 |
| put 4 MiB | 89.3 [88.4–90.4] | 357 | 92.8 [92.0–93.9] | 126 [124–127] | 145 [143–149] | 6.05 |
| put 16 MiB | 24.0 [23.1–24.0] | 383 | 335 [333–344] | 468 [462–480] | 523 [493–534] | 6.03 |
| get 4 KiB | 26,407 [26,067–26,445] | 103 | 0.29 [0.28–0.29] | 0.56 [0.55–0.56] | 0.57 [0.56–0.57] |  |
| get 64 KiB | 10,702 [10,602–10,845] | 669 | 0.73 [0.71–0.73] | 1.10 [1.09–1.14] | 1.15 [1.11–1.18] |  |
| get 256 KiB | 7,505 [7,498–7,585] | 1,876 | 1.03 [1.01–1.03] | 1.49 [1.49–1.50] | 1.56 [1.52–1.60] |  |
| get 1 MiB | 2,138 [2,126–2,260] | 2,138 | 3.69 [3.46–3.72] | 5.44 [5.16–5.48] | 5.59 [5.38–5.81] |  |
| get 4 MiB | 412 [407–421] | 1,649 | 19.3 [18.9–19.7] | 29.0 [28.8–29.6] | 30.1 [29.8–33.1] |  |
| get 16 MiB | 92.7 [92.5–94.5] | 1,483 | 75.8 [74.7–76.6] | 107 [106–108] | 122 [115–129] |  |

**From 64 KiB to 1 MiB, a put costs its device a whole chunk.** Every stripe takes a file of the
pool's 4 MiB chunk length, and the pool's refill writes another, so a 64 KiB put writes 66 bytes
for each byte sent, and puts of 64 KiB to 1 MiB run at 48 to 59 a second on the 970 EVO whatever
their size, against M13's rows at 726 a second at 64 KiB and 110 at 1 MiB on one node, and 540
and 63 on three. That is [O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk),
filed by this run. It is the store as M14 built it, with every chunk written whole, and no figure
here is network-bound below 4 MiB.

**From 4 MiB up, stripes beat rows.** On one node both are the network's, 109 to 111 MiB/s, but a
stored byte is written 2.0 times against the rows' 2.5. On three nodes, 4 MiB puts ran at
79.7 MiB/s against the rows' 61.1, and 16 MiB at 74.7 against 57.6. The tail M13 left unattributed
is mostly gone: a 4 MiB put's p99 is 1.0 s against 2.9 s, and the worst second's at 16 MiB is
5.0 s against 8.1 s and a run's worst of 15.6 s. Each host writes a stored byte 2.0 times against
2.6. The bound is europa's uplink: it carries the client's bytes to the puts titan and hyperion
coordinate, and two copies of every byte of the third it coordinates itself. With each node
coordinating a third, that is four thirds of the put rate on 112 MiB/s, about 84 MiB/s, which
these figures approach.

**Over loopback the device is the bound.** Three copies of every byte go to one Optane, each
written twice (O102): 383 MiB/s of puts at 16 MiB is about 2.3 GB/s on the device. The 64 KiB
puts, at 192 a second, write it at the same rate. [X3](../object-storage/bytes-through-groups.md)'s
rows on the same nodes stored 250 MiB/s at 64 KiB, 220 at 1 MiB and 165 at 4 MiB through its own
driver; stripes are past them from 4 MiB.

**Gets** are as M13's were: the network's on one node, 112 MiB/s, and on three up to 232 MiB/s,
since europa drives and holds a copy. Over loopback a 1 MiB get is 2.1 GB/s.

#### A streaming put

One put of 1 GiB at a time, two runs of 45 seconds after 15 of warmup, at `6046094`
(`f86-stream-three-6046094`, `f86-stream-loop-6046094`):

| Leg | A put took, s | A put's rate, MiB/s | Device bytes a byte, all hosts (a host) | What was busy |
| --- | ---: | ---: | ---: | --- |
| Three nodes | 18.8 [18.8–18.8] | 54.6 | 6.05 (2.02) | titan's and hyperion's 970 EVO 45 to 48% of the time, europa's Optane 5% |
| Three nodes over loopback | 3.24 [3.22–3.25] | 317 | 6.03 | europa's Optane 77% of the time |

**On the lab the bound is the network**: the coordinating node sends two copies of every byte over
its one 1 GbE uplink, 2 × 54.6 = 109 MiB/s of the link's 112, while the devices idled half the
time. **Over loopback it is the device**: three copies on one Optane, each written twice (O102),
six bytes for every byte put. The rate is one put's: 1 GiB over its duration. The bench's own
MiB/s for these arms, 68.3 and 319, divides the bytes of every put that ended in the window by the
window, and the lab's puts overlap its edges. Nothing was lost or left staged, and a put held no
more than its window: [Resolved #238](../appendix/resolved/object-lane-backpressure.md)'s lanes
held the coordinator to the rate its holders took bytes, where at M15's commit eight of these
puts at 16 MiB failed.

#### A small write in place beside a table write

Writes of 4 KiB and 32 KiB at a 4 KiB boundary into 32 objects of 1 MiB, on the three nodes, with
the pool's `small_writes` at 64 KiB, the lab's, so they ride their commits, and at 0, so they are
staged. The table write is the same capture's `insert100` of Movie rows, four in flight, read from
its three-second warmup, in which every row the dataset holds was written:

| Path | Write | In flight | ops/s | p50 ms | p99 ms |
| --- | --- | --- | ---: | ---: | ---: |
| In the commit | 4 KiB | 1 | 73.2 [70.6–74.8] | 12.8 [12.4–13.3] | 28.7 [28.6–30.9] |
| In the commit | 4 KiB | 8 | 201 [184–207] | 24.9 [24.4–27.6] | 195 [177–215] |
| In the commit | 32 KiB | 1 | 76.9 [75.3–78.7] | 12.0 [11.8–12.2] | 29.8 [28.6–31.1] |
| In the commit | 32 KiB | 8 | 208 [203–213] | 27.2 [26.5–28.4] | 181 [164–185] |
| Staged | 4 KiB | 1 | 92.9 [92.1–93.9] | 10.3 [10.2–10.4] | 18.9 [18.5–19.9] |
| Staged | 4 KiB | 8 | 263 [261–268] | 24.5 [24.5–24.8] | 102 [98.6–105] |
| Staged | 32 KiB | 1 | 90.2 [88.5–91.3] | 10.7 [10.6–10.9] | 18.1 [18.0–18.6] |
| Staged | 32 KiB | 8 | 248 [246–250] | 26.6 [26.2–26.7] | 110 [107–114] |
| A table insert | one row | 4 | 3,867 [3,809–4,002] | 8.11 [7.74–8.26] | 13.8 [13.6–15.0] |

**At one in flight a write in place is 1.3 to 1.6 times a table write's median**, inside S15's
twice; at eight it is three times one at four. **Riding the commit is the slower path on the lab**,
against X8's expectation: a row's pending bytes are whole 64 KiB units, so a 4 KiB write commits
64 KiB through the row's log on every member, the size at which X8 found a commit's bytes cost more
than a stage ([O108](../appendix/optimizations.md#o108-a-small-write-in-its-commit-carries-whole-units-and-is-slower-than-staging-it)).
Every write was acknowledged, every object read back as written, and nothing was left staged.

#### The reference cell beside an object load

A paced read of Movie at 50 a second on the client's other connections, beside each object arm on
the three nodes, as F80's capture ran it (`f86-paced-4566a64`):

| Beside | Its rate | Paced read p99 ms | Worst second's p99 ms |
| --- | --- | ---: | ---: |
| `stat100/s1m/c1`, the reference | 2,341 stats/s | 1.31 [0.90–1.47] | 1.73 [1.61–3.08] |
| `put100/s1m/c1` | 24.4 MiB/s | 1.50 [1.32–1.99] | 1.90 [1.47–2.10] |
| `put100/s1m/c8` | 48.5 MiB/s | 1.65 [1.30–1.92] | 2.90 [1.37–5.59] |

**Beside eight 1 MiB puts in flight, a small query's p99 is 1.26 times its reference, and the
ranges overlap**, so the difference is not a result. M13's rows held it to 1.7 times, disjoint,
beside 69 MiB/s. The object load here is lighter, O107's 48.5 MiB/s, and its bytes no longer pass
through the tablet groups the query reads. S15 asks this with the pool on devices of its own; the
lab's pool shares each host's SSD with the tables.

#### Nothing left staged

Every object arm records `staged_left`, the bytes every slice still held staged a minute after its
last answer. It was zero in every run of every leg above.

#### What the lab found

Four defects, each reproduced by a test written first, fixed, and filed on the resolved page, before
the figures above were taken:

- [Resolved #237](../appendix/resolved/object-store-starts-after-the-shards.md): a put past the
  threshold before a node's object executors started was refused `TooLarge`, which the bench's
  first preload after every restart met.
- [Resolved #238](../appendix/resolved/object-lane-backpressure.md): an object lane refused a
  request at its bound at once, so eight 16 MiB puts over 1 GbE lost their remote holders.
- [Resolved #239](../appendix/resolved/record-bases.md): a holder could not apply a write it had
  staged over a label its chunk later passed, so concurrent small writes stuck whole runs.
- [Resolved #240](../appendix/resolved/moved-row-not-ready.md): a write in place whose row moved
  under it was answered `NotReady`.

And two optimizations, [O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)
and [O108](../appendix/optimizations.md#o108-a-small-write-in-its-commit-carries-whole-units-and-is-slower-than-staging-it).
The bench also gained the pool devices in its counters.

#### S15's objectives

| Objective | Measured | Held |
| --- | --- | --- |
| Tables beside objects: the reference cell's p99 within 1.25 times, pools on their own devices | 1.26 times at the median, ranges overlapping, the pool on the tables' SSD | Not judged: the lab has no device of its own for a pool, and the ranges overlap |
| A streaming put bounded by the slower of the devices and the network, the bound named | 54.6 MiB/s a put on the lab, the coordinator's uplink carrying two copies at 109 of 112 MiB/s; 317 MiB/s over loopback, the Optane writing six bytes a byte | Yes, the bound named: the network on the lab, the device over loopback |
| A small write in place within twice a table write's median | 1.3 to 1.6 times at one in flight, either path | Yes at one in flight; at eight against four, three times |

#### The tmdb cluster at the commit

The tmdb cluster was destroyed and bootstrapped afresh at `eb61fa6` (`tmdb-dataset-loader
destroy`, then `bootstrap -i tmdb_cluster.yaml`), its inventory now setting `small_writes: 64KiB`
on `fast`. The bootstrap activated wire 9 itself. `tmdb-dataset-loader stripes`, the check F86
adds to the loader, put three posters past the threshold through each member: 20,000 bytes, 1 MiB,
and 9 MiB and eleven bytes. It read all nine back whole through every member at `One` and at
`Quorum`. It then wrote part of each in place through the next member, 4 KiB in its commit and
200 KB staged, and read them all again. Every read was what was written. The TMDB load wrote
1,188,548 movies, 186 queries retried after `NotLeader` in the first seconds of the leads, and its
read-back found all 10,073 movies it sampled; a second load over them wrote the same.

Once the lab's four fixes were in, the cluster was upgraded to `6046094` one node at a time
(`tmdb-dataset-loader upgrade`), each node back and caught up before the next, wire 9 kept. Then:

- `stripes` passed again, every poster read back whole through every member at both levels;
- a load of 50,000 movies read back all 10,000 it sampled;
- `status` showed 3 of 3 voters up, every member speaking 9, every device serving with nothing
  staged;
- `admin pools` read:

```text
fast  ssd width 3 needs 2 by host  devices 3/3  hosts 3/3  free 241.8GiB  usable -  pinned at 4  ready
```

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `shoal-proto` `stripe_rows_follow_the_model` | A stripe row's next state, its labels, marks or pending bytes, leaves the model's at some step of a saved or generated schedule |
| `stripes` `a_commit_labels_what_it_touched_and_marks_what_missed`, `a_missed_position_still_names_its_write`, `a_noop_and_a_fence_move_the_sequence`, `pending_bytes_merge_and_never_replace`, `a_clear_marks_every_holder_that_did_not_hold_the_label`, `a_reclaimed_row_is_a_tombstone` | A next-row function moves a label, a mark, the sequence or the pending bytes otherwise than X1's rules |
| `stripes` `a_mark_is_the_tags_top_bit_and_no_write_carries_it`, `a_tag_is_a_tries_and_a_commit_identity_names_no_task`, `stripe_identities_are_frozen` | A mark is lost or carried, two tries share a tag, or an identity moves |
| `stripes` `a_hole_is_past_the_put_and_named_by_nothing_since`, `a_floor_hides_a_stripe_stamped_below_it`, `a_range_falls_in_the_stripes_it_touches` | A hole or a floor reads as bytes, or a range asks the wrong stripes |
| `chunk` `every_request_round_trips`, `every_answer_round_trips`, `a_frame_that_cannot_be_right_is_malformed` | An object lane frame does not read back, or a malformed one is taken |
| `hello` `an_object_lanes_hello_names_its_slice_after_the_record` | An object lane's slice is lost, or another lane's hello grows |
| `poolmap` `a_pool_is_pinned_once_and_the_map_moves_on_without_it` | A pin moves, or moves the generation |
| `objects::lane` `a_chunk_crosses_the_object_lane_to_its_slices_owner` | An object lane is not handed to its slice's owner, or a chunk does not cross it whole |
| `slice` `a_whole_chunk_in_pieces_is_placed_or_staged`, `a_stage_a_fold_and_a_read_take_the_first_label_made`, `a_record_is_undecided_once_its_stager_had_its_time` | A put's pieces, a fold or the decide's records stop working on the store |
| `slice` `a_reader_one_write_behind_is_served`, `a_previous_state_is_kept_only_when_asked` | A reader one write behind is sent away, a state outlives a start, or is kept when told not to |
| `shoal/tests/objects.rs` `a_striped_object_round_trips`, `small_writes_ride_their_commits` | A standalone node's stripes, writes in place, appends, truncates or small writes read back wrong |
| `objects.rs` `a_hole_reads_as_zeros`, `truncated_bytes_never_return`, `replace_shares_nothing_with_its_predecessor`, `consumers_share_a_pool_and_nothing_else` | S3's and S4's rows on one node |
| `objects.rs` `a_stream_holds_no_more_than_its_window`, `a_seek_reads_only_what_it_asked_for` | S12's rows |
| `cluster_fixture` `a_striped_object_round_trips_across_nodes` | A stripe written through one node does not read through another |
| `cluster_fixture` `stripe_write_is_atomic_at_every_crash_point` | A crash at one of S16's six points, or a member killed between stage and commit, leaves bytes of two states, or a state older than a read before it |
| `cluster_fixture` `stale_stager_cannot_commit`, `uncommitted_bytes_never_replace_committed`, `staged_bytes_outlive_a_stagers_timeout`, `retried_write_is_the_same_write` | A stale or dead stager's bytes land, a stager's timeout loses a committed write, or a retried write changes a stripe twice |
| `cluster_fixture` `ack_requires_k_plus_f_current`, `whole_object_replace_is_atomic`, `append_race_loses_no_bytes`, `pending_bytes_leave_the_row_only_on_k_plus_f_holders` | S7's other rows |
| `cluster_fixture` `one_reads_never_mix_and_move_forward`, `strong_read_observes_prior_acknowledged_write`, `stale_chunk_is_never_served_as_current`, `read_overlays_pending_bytes` | S9's rows |
| `cluster_fixture` `stalled_object_lane_leaves_the_others_answering` | A stalled holder holds another lane, or a put waits on it |
| `cluster_fixture` `pool_short_of_failure_domains_refuses_writes_by_name` | A pool short of hosts takes a write, or refuses it without readiness's words |
| `cluster_fixture` `pool_map_holds_nothing_for_a_stripe` | A stripe or an object reaches the pushed pool map |
| `cluster_fixture` `object_writes_survive_control_quorum_loss`, `full_restart_decides_every_staged_write` | Object writes need the control group, or a full restart leaves a stage undecided or loses an acknowledged byte |
| `cluster_fixture` `metadata_cost_is_linear_and_published` | A row of either table grows past its published constant, or a put or a write in place makes more rows than S3 says |
| `actions` `pools_are_read_and_drawn` | `pools` stops saying where a pool is pinned |
| `objects` (loadgen) `mixes_read_and_spell_back` | A write in place mix is not read, or changes an older mix's name |

## Related

- [S3](../object-storage/objects.md), [S4](../object-storage/pools-and-devices.md),
  [S5](../object-storage/placement.md), [S6](../object-storage/device-store.md),
  [S7](../object-storage/write-path.md), [S9](../object-storage/read-path.md),
  [S12](../object-storage/wire-and-client.md), [S13](../object-storage/isolation.md),
  [S16](../object-storage/testing.md), [S18](../object-storage/contract.md): the design this builds.
- [X1](../object-storage/stripe-model.md): the rules it builds, held to the model by
  `stripe_rows_follow_the_model`. [X8](../object-storage/small-writes.md): the small write.
  [X11](../object-storage/streamed-bodies.md): a connection handed over.
- [F80](object-wire.md): the object frames and inline objects. [F85](devices-and-pools.md): the
  devices, placement and store this drives. [F84](replication-lane-handoff.md): the handoff the
  object lane copies. [F73](bodies-across-frames.md): the windowed stream a put is.
  [F68](conditional-writes.md): the conditional commit.
