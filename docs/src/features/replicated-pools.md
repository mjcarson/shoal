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
  `object_queue_bytes`. A request that does not fit is refused at once, unavailable, which its
  sender counts as a holder that did not answer.
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
  zero.

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
- **Object work never runs on a table shard**: a lane is handed to its slice's owner before a
  request is read off it.
- **Nothing is decided by a time a row carries.**
- **A previous state is never staged over, confirmed or persisted.**

## Performance

### The previous state

`shoal-spike store --only previous` applies one 4 KiB unit in place, and a whole 1 MiB chunk over
one, back to back under the shared flush, with the state kept and not. On europa's Optane, quick, a
core under the `powersave` governor, the one-unit apply's p50 went from 174 µs to 209 µs - the unit
it overwrites read first - and the whole chunk's from 231 µs to 225 µs: the link costs nothing a
quick run can see. The lab's figures are taken with the rest of this gate's evidence.

### The metadata cost

`metadata_cost_is_linear_and_published` counts an archived `ObjectMeta` row at 556 bytes for a
short path, whatever its object's size, and a stripe row with no pending bytes at 192. The test
holds them to **1 KiB an object row and 256 bytes a stripe row**, which is the constant
[S3](../object-storage/objects.md) publishes: a put of any size is one row, and every stripe written
in place is one more and nothing else.

### On the lab

M15's evidence - stripes against the M13 baseline on the lab and over loopback, a streaming put, a
small write in place against a table write, the reference cell beside an object load, and nothing
left staged - is taken on the lab at this gate's commit and recorded in the change after it.

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
