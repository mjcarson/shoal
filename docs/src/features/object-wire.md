# F80 — M13, the wire, pool policy, inline objects and the baseline

## Context

[M13](../object-storage/milestones.md#m13-the-wire-and-the-baseline) is the object store's third
gate, and the first that stores an object. [M12](bucket-schema.md) made a bucket something a schema
declares, with the two tables its metadata lives in, and refused every client's query of them.
Nothing could reach a bucket. This gate builds the way in, from the client's wire to the bucket's
rows:
- the object frames and the client's handle ([S12](../object-storage/wire-and-client.md));
- pools and bindings as committed policy ([S4](../object-storage/pools-and-devices.md#pools-and-bindings-are-policy));
- an object's metadata operations as conditional commits ([S3](../object-storage/objects.md));
- objects held in their rows, end to end;
- the bench's object arms over a described dataset ([S15](../object-storage/performance.md));
- the unit checksum in its final form ([X5](../object-storage/checksums.md)).

Every prerequisite had landed:
- streams of more than one frame, and connections set apart for them ([F73](bodies-across-frames.md));
- the checksum's choice (X5);
- the frame and the window ([X11](../object-storage/streamed-bodies.md));
- the dataset's shape and its generator ([X13](../object-storage/benchmark-shape.md)).

Four decisions were taken with the user on 2026-10-11:

- **D1, wire 7.** [F68](conditional-writes.md)'s rule stands: an object write on a cluster that has
  not activated wire 7 is answered `WireVersion`. `shoaladm`'s bootstrap and the bench's bring-up
  activate the build's wire on a fresh cluster.
- **D2, an unbound bucket.** It is refused by name at a standalone node's start and on a node
  started with `cluster.bootstrap: true`. A joiner's copy of the policy is parsed, unknown keys
  refused, and ignored.
- **D3, writing in place.** `write_at` and `truncate` are left to M15. The handle offers `put`,
  `get`, `read_at`, `stat`, `delete` and `open`, whose file reads and seeks.
- **D7, the baseline.** Bench copies on the lab: one node on titan, and three through
  `shoaladm bench`'s own copy of `tmdb_cluster.yaml` with Posters bound to a pool of 16 MiB inline.
  Sizes from 4 KiB to 16 MiB, four rounds.

The rest were mine, and are recorded with their reasons under **Design choices**:
- D4, the arms' ids;
- D5, a lost or misread object fails the run;
- D6, an object's answer is covered by [F75](client-cancel.md)'s cancel;
- D8, the tmdb cluster redeployed with a pool and Posters bound.

## What it does

### The wire

**A capability bit, granted only beside streams.** `CLIENT_CAP_OBJECTS` is bit four of the hello's
capability byte. A server grants it only to a client it also grants `CLIENT_CAP_STREAMS`, since a
put's and a read's bytes are a stream. A client offers it, and writes no object frame to a
connection that was not granted it: it refuses first, by name (`Errors::Config`). A server sent one
on such a connection closes that connection alone.

**Two message types; the bytes are F73's.** `ObjectOp` (28) is a fixed head of 56 bytes, then the
path, then a put's attributes:

| Field | Bytes | Holds |
| --- | --- | --- |
| id | 16 | The operation's identity, a v7 UUID for a put |
| bucket | 8 | `BucketId` |
| kind | 1 | `Put`, `Read`, `Stat`, `Delete` |
| read level | 1 | None, `One` or `Quorum` |
| path length, attributes length | 2, 4 | |
| offset, length, checksum | 8 each | A read's range; a put's length and the CRC-64/NVME of its bytes |

`ObjectAnswer` (29) is the id and an 80 byte head:
- the outcome and the kind;
- the object's id, its content version and its size;
- the range answered and its checksum;
- the two times.

A tail follows: a refusal's message, or a stat's attributes. A put of any bytes, and a read that
answers any, is an opener (`Flags::STREAMED`, its length the stream's). Its bytes follow in
F73's `Data` frames, named by the operation's id, judged by the same `stream::Inbound` and ended by
`LAST`. Neither head is an archive. Both are plain little-endian fields, written and read by
`shoal-proto/src/shared/protocol/object.rs`.

**An outcome is the object's; an error is the machinery's.** `Done`, `NotFound`, `TooLarge`,
`BadPath`, `BadAttributes`, `ChecksumMismatch` and `UnknownBucket` are answers, and the connection
goes on. A failure under the operation is an ordinary `Error` frame under the operation's id, with
the code a query would have had: no grant, no leader for the row's group, a commit whose outcome is
unknown, a cluster below wire 7. A client therefore judges an object's retry by the rules it judges
a query's by (`retriable`, `outcome_unknown`).

**A frame that cannot be right ends its connection**:
- a head too short for its own lengths;
- a kind or a flag this build does not read;
- a put that names an offset, since a put replaces the whole object;
- a put that names a read level;
- a put whose identity is not a v7 UUID;
- a stream declared where none can be, or one missing where it must be;
- a path that is not UTF-8;
- attributes past 16 KiB, the structural bound.

What is well formed and asks for something a bucket does not serve is answered by name instead
(`judge_op`):
- an empty path, or one past 1 KiB;
- user metadata past 2 KiB, more than ten tags, or an attribute past 1 KiB;
- a put past the bound pool's inline threshold, which is `TooLarge`, "no pool has a device".

The server judges a put before it reads a byte of it. One it refuses is drained with F73's
`refuse`, so the connection goes on.

### The checksum

`shoal::shared::checksum` is CRC-64/NVME through `crc-fast` `=1.10.0`, with default features off
and `std` alone:
- `crc64` over a slice, and `Crc64` fed in pieces;
- `combine(a, b, len_b)`, Shoal's own, written from zlib's method;
- a `Multiplier` for a fixed length, which one unit length makes once;
- `Chain`, which folds parts as they arrive.

The table `combine` reads is built at compile time. `checksum_is_stable_across_builds` freezes
X5's literals:
- the check value, `ae8b14860a799888`;
- 4 KiB of zeros, `6482d367eb22b64e`;
- lengths 0 to 257 folded through FNV, `386cf13d45d3100f`;
- a unit of each size from 4 KiB to 1 MiB;
- 1 MiB and 13 bytes, `b2187940c63bae90`.

A put names the CRC of its bytes in its head, and the server refuses a put whose bytes do not
match before it commits anything (`ChecksumMismatch`). A read's answer names the CRC of its range,
which the client checks through a `Chain` as the frames arrive; a range that does not match is an
error, never a value.

**`crc-fast` was read before it was added**, as M13's scope asked, for the CRC-64/NVME path on
x86_64 and the dispatch to it:
- **No memory-safety defect.** Every load is bounds-guarded for every length. There is no
  allocation, no thread, no `static mut` and no `unsafe impl Send` or `Sync`. One `OnceLock`
  holds the chosen tier.
- **One defect, inert on the lab.** The VPCLMULQDQ tier is chosen without checking `avx512bw`
  (`feature_detection.rs`, lines 190-194). Its kernel enables the feature, and the compiled NVME
  path runs `kmovd`, an AVX-512BW instruction, for inputs of about 271 bytes and more
  (`avx512_vpclmulqdq.rs`, lines 115 and 377). Every CPU with VPCLMULQDQ the lab or a cloud ships
  has BW, and Zen1 never picks the tier. A virtual CPU model that offers VL and VPCLMULQDQ without
  BW would end the process with `SIGILL`.
- **X5's "361 lines of `unsafe`"** is the count of lines containing the word. About 1,070 lines
  sit inside `unsafe fn` bodies on the lab's two tiers, the crate being edition 2021, where an
  `unsafe fn`'s body is unsafe throughout.

### Pools and bindings, as committed policy

A deployment says which pool each bucket stores its objects in. The schema says nothing about it:

```yaml
storage:
  pools:
    fast:
      class: ssd
      redundancy: {replicas: 3}       # or {data: 4, parity: 2}
      failure_domain: host
      inline: 16KiB                    # at or under this, an object stays in its row
  buckets:
    Posters: {pool: fast}
```

`shoal::shared::pools` holds the types a node's configuration, the control group's state, a
shard's map and `shoaladm`'s inventory all read, with unknown keys refused. A pool is checked
before anything else reads it (`PoolSpec::check`):
- an inline threshold under 16 MiB (`MAX_INLINE_THRESHOLD`), and under the stream bound the node
  assembles;
- a unit that divides its stripe;
- `f` of at least one, except in a pool of one copy, which survives none and is what an example and
  a test use.

A binding is checked against the schema's buckets: one is named for every declared bucket, to a
pool that is defined, and none for a bucket the schema does not declare (`PoolsConf::check`).

**On a cluster it is committed at bootstrap.** The bootstrapping node's `pools` and `buckets` ride
its `BootstrapPolicy` into the control group. `ObjectPolicy::bind` gives each binding its consumer
id there, minted once and never reused: sorted names from one, and later bindings past every id
ever given. The policy reaches every shard in the `TabletMap`. A joiner's copy is read, its unknown
keys refused, and ignored, as its replication factor is. `AdminKind::Pools` reads a node's applied
view, which is how the fixture shows a joiner holding the bootstrapper's policy and not its own.

**A standalone node reads its own.** It mints its consumer ids into its storage marker
(`StorageMeta.consumers`, a serde default, so an older marker reads), where they survive a
restart.

**An unbound bucket is refused by name** at a standalone node's start and at a bootstrap:
"bucket Posters is bound to no storage pool; there is no default pool…". The same goes for a
binding to an undefined pool, and for a pool whose threshold is past either bound.

### Objects held in their rows

**Every object is held inline at M13.** ~~No pool has a device~~ No pool had a device, and since
[F85](devices-and-pools.md) (M14) no client's byte reaches one, so an object at or under its pool's
threshold is kept in its path's entry of the bucket's `{Marker}ObjectMeta` row, and one past it is
refused `TooLarge`. An entry holds the whole path, so two paths whose keys collide are two entries
of one row, each found and compared whole on every operation (P14).

**An operation is a few ordinary queries of the generated table**:
- **a put** reads the row, then commits it with its entry added or replaced, conditional on the
  row it read: an insert `if_absent` for a new row, `if_matches` the version read for an existing
  one;
- **a delete** commits the row without the entry, or deletes the row when it was the last;
- **a read or a stat** gets the row at the operation's read level, `Quorum` unless it names one.

[F68](conditional-writes.md) judges each conditional commit at apply on every replica, in committed
order. A put whose commit is refused because another write to the row committed between its read
and its commit reads again, up to sixteen times, and is then answered `Shedding`, which a client
retries.

**A retried put is the same write.** An object's id is the put's v7 timestamp in its top 48 bits
and 80 bits of `gxhash128` over the put's identity and its path (`objects::object_id`). A retry
recomputes the same id. If it finds that id in the entry, it answers as the first try did and
commits nothing. A put whose commit's outcome was unknown, or timed out, reads the row and answers
`Done` if its id is there. A delete is idempotent: one of a path with no entry answers `Done` and
names no object, so a retried delete answers as the first did, and the client's `delete` says
whether it removed one.

**The server's object lane.** An operation runs as a task spawned on the shard that coordinates
its connection, and holds no borrow of the shard. Each shard registers one virtual client of its
own with every shard at start, by a `NewClient` broadcast, as a peer's data lane does. A
demultiplexer hands the task the answer to each bundle it sent. A task's queries go through
`ServerMsg::Internal`, which enters `send_to_shard` as `Front::Internal`. That front skips the one
check every client's query meets: the refusal of a generated table. Everything else a client's
bundle meets, an internal one meets too:
- routing to the row's tablet, forwarding to its leader on another node, replication;
- the gather and its deadline;
- write admission, `NotInitialized` and shedding.

The lane has no principal, so `authorize` has nothing to check. The operation's own grant is
checked first, at the shard: `Access::allows_bucket`, read for a read or a stat and write for a put
or a delete, F79's check's first caller. Every internal bundle carries a fresh v7 identity, never
the operation's, and its answer is read with a checked `rkyv::access`.

**A put's bytes are assembled whole** before its task starts. They go into F73's `BodyAssembly`,
counted against the shard's assembly budget and held there until the task ends. A shard runs at
most 1,024 object operations at once, and refuses past that `Shedding`.

**An answer is written by the connection's write relay.** An answer with bytes always streams:
its opener holds the head, and the range follows in data frames of the client's frame size. It is
never passed through the query path's `answered`, which would read its bytes as an archive. An
object's reply carries its coordinator's attempt, so a client's cancel of the operation covers its
answer as it covers a bundle's (D6). A write still runs, as F75 has it.

### The client

```rust
// reach the bucket our schema declared
let posters = client.bucket::<Posters>();
// put a whole object, checksummed, under an identity a retry reuses
let info = posters.put("alien/one-sheet.png", &bytes).await?;
// read it back whole, or a range of it into a buffer
let (info, bytes) = posters.get("alien/one-sheet.png").await?;
let read = posters.read_at("alien/one-sheet.png", 4096, &mut buf).await?;
// open it as a file that reads and seeks; a seek sends nothing
let mut poster = posters.open("alien/one-sheet.png").await?;
poster.seek(SeekFrom::Start(1 << 10)).await?;
poster.read_exact(&mut buf).await?;
```

`Shoal::bucket::<M>()` returns a `BucketHandle<'_, S, M>` for any marker
`M: BucketRows<Client = S>`, so a marker from another schema is a type error.
`Shoal::objects(BucketId)` is the untyped `Objects` the handle derefs to, and the bench drives
that. Its operations:
- `put` and `put_with` (`PutOptions`: attributes, an identity, a retry bound);
- `get`, `read_range` and `read_at`;
- `stat` and `delete`;
- `open`, whose `ObjectFile` implements `AsyncRead` and `AsyncSeek`, asking for at least 1 MiB at a
  time.

An operation travels on one of F73's connections set apart for long streams, which a client keeps
by default, and not on the pool small answers share. Only a client configured to keep none apart
uses that pool. On a client that routes, it goes to the node leading its row, chosen by
`Router::node_for`, which was factored out of `Router::plan`. An operation that fails retriably is
sent again under the same identity until `DEFAULT_OBJECT_RETRY`, thirty seconds, unless the caller
sets another bound.

### The bench

**A bucket's dataset is a description**, `<Bucket>.yaml` in the dataset folder, found by the
bucket's name:

```yaml
objects: 400
sizes: {uniform: [1KiB, 32KiB]}    # or fixed, doublings, or a table of weights
seed: 7
generator: splitmix64-ctr/1
```

Every object's size, path and bytes derive from its index. Word `i` of object `o` is SplitMix64's
mix of the seed's state for `o` advanced `i + 1` steps, so any object, or any range of one, is made
alone. `splitmix64_ctr_is_frozen` holds X13's three digests at seed `0x5831_3133`. A folder may
hold descriptions only. The description is kept whole in the capture with the dataset's digest.

**The object arms** are `{mix}/s{size}/c{concurrency}[/{override}]/none`:
- the mixes are `put100`, `get100` and `rw50`, or weights of put, get and stat;
- `sd` runs each description's own sizes;
- the arms follow every table arm, and a spec of tables alone writes none of the new keys, so its
  digest is unchanged.

`shoaladm bench` takes `--objects`, `--object-sizes` and `--concurrency`. Object mixes named alone,
by the flag or a spec file's `objects`, choose the run, so no wizard opens and no default table
workload is added. Beside them, `--paced <table>` reads that table at its rate, and it may be the
dataset's only table, since the object arms are the main load. That is how a small query's tail is
taken beside an object stream. A run preloads each size's
objects, runs the arm with the per-second windows of [F69](driver-operation-kinds.md)'s supplied
kinds (`put`, `get`, `stat`) and their bytes both ways, then reads back every object the arm
acknowledged. It makes each one again and compares it byte for byte, beside the wire's CRC. **A
lost or misread object fails the run** (D5), and the capture's `VerifyFacts` counts both.

### Deployments

An inventory carries `pools` and `buckets` at the deployment level. They are rendered into every
node's `storage`, kept by the wizard as they were, and checked against the schema's buckets before
anything is deployed. After the first node initializes, `shoaladm`'s bootstrap activates the wire
version the build speaks, as `upgrade --activate` does. Without that, D1 would refuse every object
write on a fresh cluster. The bench's bring-up does the same. `tmdb_cluster.yaml` binds Posters to
a pool `fast` of three replicas across hosts, and `shoal-spike-rows/inventory.yml` binds its
spike's bucket.

### Found on the way

- **[Item 228](../appendix/resolved/aligned-records.md), resolved.** A row holding a `u128`
  behind a pointer, as `ObjectMeta.entries` holds an object's id, could end its shard at a
  restart. The intent log and the archives laid records back to back, and rkyv validates a `u128`
  at a sixteen byte boundary of its buffer. Found by `every_acknowledged_byte_is_read_back`
  failing about one run in ten. Every record now starts at a boundary.
- **[Item 230](../appendix/resolved/object-connection-held.md), resolved in a change of its
  own.** The client held an operation's pooled connection until its answer came back, so the two
  connections set apart carried two operations at a time. The baseline's first arm found it, 4 KiB
  puts with a p99 of 609 ms, and it was fixed before the baseline was taken.
- **[Item 229](../appendix/resolved/hotpath-build.md), ~~open~~ resolved in a change of its
  own.** The `hotpath` build of the engine ~~does not compile, and has not since before F79~~
  did not compile from F76 on: the paged index's write under the archive map's commit took the
  compactor's futures past the compiler's query depth once `hotpath` wrapped each method. The
  map's commit is boxed, and a test now runs a grid arm under the feature.

## Design choices

**The bytes are F73's `Data` frames, and only two types are new.** S12 drew three types. F73 had
since built the third, with a receiver that judges each frame against its stream. A second kind of
data frame would have been a second judge of the same faults.

**The heads are fixed fields, not archives.** The server judges a put by its length before it reads
a byte of it, and refuses a path past 1 KiB without allocating it. S12's "an object's bytes are
never an archive" is extended to what describes them, so no validator walks anything on the object
path's front.

**A machinery failure is an `Error` frame.** Unauthorized, NotLeader, OutcomeUnknown and Shedding
already carry the client's retry rules. An outcome of the object's own would have needed a second
copy of them.

**An object's id is minted from its put and its path.** A retried put recomputes its id, and finds
in the entry whether it committed, with no table of retries on any node. The id is time-ordered
in its top bits, as S3 asks. It is not the put's identity itself: a client that reused an identity
on another path would then make two objects with one id. Hashing the path in makes those two ids
differ.

**Create, replace and delete are F68's conditional insert and delete of the whole row.** The row
holds every entry whose path hashes to its key. A conditional commit of the whole row, judged at
apply against the version read, is how a write to one path never replaces another (P14) and how
two writers to one path are ordered. F68 built the commit, and P14 is the read of the row's entries
by whole path, `ObjectRow::entry`.

**An object's operations run through the tables' own path, not beside it.** The lane is a
virtual client, so an object's row is routed, forwarded, replicated and gathered by the code every
query uses, with no second copy of any of it. What the lane skips is exactly the front door's
refusal of a generated table. Its operations are checked against the bucket's grant instead.

**A fresh identity for every internal bundle.** A gather is keyed by its bundle and index, and a
tablet group remembers a write's result by its `RequestId`, the bundle and index. A retry after a
refusal under the operation's identity would be answered the first refusal forever.

**No meter on an internal bundle.** A client's clock is released by its connection's write relay.
An internal bundle's answer goes to the lane, so a clock started for it would never be released,
and at `MAX_CLOCKS` the meter would stop timing real clients.

**Reads default to `Quorum`.** A row read at `One` can come from a replica behind its leader, and a
reader that trusted one could be shown an object older than an acknowledged write
([S9](../object-storage/read-path.md)). A caller may ask for `One`.

**Pools live in `shoal-proto`.** The engine's configuration, the control state, the map and the
inventory share one reader of a pool, so none can read one another way, and `deny_unknown_fields`
makes a key from a newer build an error rather than a pool missing it.

**Consumer ids are minted where the policy is committed.** On a cluster that is the control group's
apply of the bootstrap; on a standalone node, its storage marker. Either way, an id is never
reused, which S4 requires so that a stripe row's key never names two buckets.

**`write_at` and `truncate` wait for M15 (D3).** Every object is a row at M13, so writing in place
would rewrite the whole row. M15's stage, commit and apply is the design writing in place needs.
An `ObjectFile` that wrote would promise a cost model this gate cannot keep.

**Arm ids name the mix, the size and the concurrency (D4).** Those are the three axes the baseline
sweeps, and every later gate compares against an arm by its id. No delete arm exists, since an arm's
read-back needs what it wrote.

**A lost or misread object fails the run (D5).** A benchmark that counted losses and printed a
rate would make a number of a broken build. A table arm's losses keep F66's log line, since
F66 decided that before objects existed.

**An object's answer is cancellable (D6).** Its reply is one more kind the write relay writes, so
covering it costs the two places that judge a reply's kind, and nothing else.

## Alternatives rejected

**An object's id equal to its put's identity.** Above.

**Writing the rows from the shard directly**, with no lane: a put that found its row's leader on
another node would have needed its own forward, its own wait for apply, and its own answer to
`NotLeader`. All three exist for queries.

**A default pool.** A bucket that stored its objects somewhere nobody chose would make a pool's
redundancy something a deployment discovers. S4 has no default, and D2 refuses the bucket instead.

**An rkyv head.** It is validated, which means walking a structure for a frame whose fields are
fixed, and it puts an allocation before the judgement of a put's length.

**A checksum per unit on a put at M13.** A unit is a slice's, and no slice exists. One CRC of the
put's bytes is checked before anything commits. Units are M15's
([TODOs](../appendix/todos.md#per-unit-checksums-on-a-streamed-put)).

**Admitting object frames without streams.** A put's bytes and a read's answer are streams by the
codec. Granting objects to a client that cannot receive a stream would leave it a read it cannot
receive.

**Judging the read-back through [F78](object-harness.md)'s byte ledger.** F78 named
`every_acknowledged_byte_is_read_back` as the ledger's first user. But a described dataset is
already an oracle: every byte of every object is made again from its index. At M13 a write is one
whole object, acknowledged or not, so there is no partial outcome for the ledger to judge. The
ledger would hold a second copy of what the description says, and add a dependency on
`shoal-model` to the driver. Its first user is M15's fixture, whose writes cross stripes.

**Keeping the generated tables' refusal and adding an exemption list.** `Front::Internal` is one
place that skips one check, named by where the bundle came from. A list of exempt connections
would be a door a client could be put on.

## Limitations

- **Both peers hold a put whole, up to 16 MiB.** A put is assembled before its task runs, and a
  read's row is read whole before its range is answered. The bytes are copied two or three times on
  the server: into the entry's `Vec`, into the archived bundle, into the answer. M15's stage,
  commit and apply is where bytes first leave as they arrive. Until then a window bounds nothing.
- **No write in place, no truncate, no list, no rename** (D3; S12 for the last two).
- **One CRC for a put's bytes**, not one per unit ([TODOs](../appendix/todos.md#per-unit-checksums-on-a-streamed-put)).
- ~~**A pool's class, failure domain, geometry and `f` are read, checked and committed, and unused.**
  Nothing places a chunk before M14,~~ **A pool's policy places nothing a client wrote.** Since
  [F85](devices-and-pools.md) (M14) placement and readiness read a pool's class, failure domain and
  redundancy, but no client's byte reaches a device before M15, and the stripe and unit defaults
  are provisional until Q20.
- ~~**The admin tool cannot read the committed policy.** The `pools` admin read exists and the fixture
  uses it; `shoaladm` has no command for it yet
  ([TODOs](../appendix/todos.md#a-pools-command-in-the-admin-tool)).~~ Closed by
  [F85](devices-and-pools.md#operations): `pools` is a cluster action of `shoaladm` and shoalctl,
  read-only like `grants`, which draws each pool's policy as placement reads it, its devices and
  domains, its free and usable bytes and what it is short of. A binding's consumer id is still read
  only through the `pools` admin read.
- **The inventory wizard does not edit pools or bindings.** It keeps them as the file had them
  ([TODOs](../appendix/todos.md#pools-and-bindings-in-the-inventory-wizard)).
- **The bench has no delete arms, and attributes are never set.** A description makes bytes, not
  metadata, and a folder of real files is not a dataset yet
  ([TODOs](../appendix/todos.md#object-bench-gaps-after-m13)).
- **The driver's cpu figure is the process's**, a second, as F66's is: 100% is one core's worth
  across every thread. X13 asked for each thread's share
  ([TODOs](../appendix/todos.md#each-driver-threads-busy-share)).
- **A lost internal answer holds its task** until the query deadline and a second of slack. The
  timeout is the backstop, so any change to the paths that answer a bundle exactly once now
  reaches object operations too.
- **A rebinding is a new cluster.** The policy is committed once, at bootstrap. Changing a binding
  on a running cluster is an admin operation no gate has built yet, and the pool map that would make
  it mean something ~~is M14's~~ exists since [F85](devices-and-pools.md#the-pool-map).

## Invariants to uphold

- **An object frame is written only to a connection granted `CLIENT_CAP_OBJECTS`, and a server
  grants it only beside streams.** A server sent one on another connection closes it.
- **A frame that cannot be right ends one connection; a request a bucket does not serve is answered
  by name.** The line between them is `ObjectOp::decode` against `judge_op`.
- **A put commits nothing it has not checked**, and its object id is minted from its identity and
  its path, never from anything a retry could change. `FROZEN_OBJECT_ID` holds the function.
- **Every internal bundle has a fresh v7 identity and no meter**, and enters routing as
  `Front::Internal`, which skips the generated table's refusal and nothing else.
- **A lane's id is a version 8 UUID of a marker and its shard's number** (`objects::lane_id`), so
  the transport view's `clients` counts connections and leaves every shard's lane out. A connection's
  id is version 4. `client_disconnect.rs` counts connections exactly, and an id the lane could share
  would fail it.
- **No object operation holds a pooled connection while it waits for its answer** (item 230). Its
  frames are written while it holds one, a put's data frames included, and its answer is read
  from the channel registered under its id.
- **An answer read from the lane is validated.** A forwarded share's bytes reach it as a peer wrote
  them.
- **An object reply is never passed through `answered`** or the stage profiler's emit. Both read
  `archived` as a `ResponseKinds`, and an object reply's bytes are its answer and its range.
- **Every write to an object's row is conditional on the row the write read.** That is what keeps a
  write to one path from replacing another that shares its row.
- **Pools and bindings never reach the schema fingerprint**, and a consumer id is never reused.
  `bucket_declaration_moves_the_fingerprint_and_its_binding_does_not` binds a node to another pool.
- **A joiner's policy is never applied.** The committed one comes through the map.
- **The inline threshold is at most 16 MiB and at most the stream bound**, so `TooLarge` always wins
  over `RequestTooLarge`, and an inline object never makes a log entry no peer frame carries.

## Performance

**What M13 adds to a query that is not an object's**: one branch in the connection's read relay,
taken before the control frames, and nothing else. Every shard registers its lane at start, one
more entry in every shard's client map whether the schema declares a bucket or not, which the
transport view's count of connections leaves out. A schema with none
generates no `object_queries`, so an object frame sent to it is answered `UnknownBucket`.

**What an object costs, by construction**: a put is one read and one conditional commit of its
row, and a read or a stat is one read. A `Quorum` read is a barrier at its group's leader. A
put's bytes are copied into its entry, archived into the bundle and written through the group's
log, so candidate A writes an object's bytes once to each replica's WAL and again at each merge.

### The baseline

M13's exit evidence: candidate A of [S7](../object-storage/write-path.md#the-candidates), objects
as rows, through the product's own client and driver (D7). `shoaladm bench` ran on the lab on
2026-10-11. It drove the node program built at `33d1cbb` for znver1 (F80, with
[item 230](../appendix/resolved/object-connection-held.md)'s fix) from `tmdb-dataset-loader bench`
on europa. Europa is a Ryzen 9 7945HX, and it routed by topology.

- **One node**, on titan, a Ryzen Embedded V1756B of four cores and eight threads, with
  `resources.cores: 6` and an 8 GiB budget. Its storage was the root volume on the 970 EVO
  (`dm-0`), at a replication factor of one, and Posters in a pool of one copy, inline to 16 MiB.
- **Three nodes**, the bench's own copy of `tmdb_cluster.yaml`: europa's Optane under XFS, titan's
  and hyperion's 970 EVO, a factor of three, and Posters in a pool of three copies, inline to
  16 MiB. Europa was the driver and a node at once.
- **The arms.** `put100` and `get100` at 4 KiB to 16 MiB, eight in flight, 15 seconds measured
  after three of warmup, four runs. The dataset was a description of 64 objects
  (`splitmix64-ctr/1`, seed 13), half of them preloaded at each size. Every acknowledged object
  was read back and none was lost or misread.
- **The hosts.** The tmdb units were stopped and every node's host ran the `performance`
  governor; europa was set by hand. The capture's provenance records each host's governor as it
  was before `--governor` set it, so it says `schedutil`. Titan read `performance` on all eight
  cpus during the arms.

The lab is three hosts on 1 GbE, about 112 MiB/s each way a link. Every figure is `median
[lowest–highest]` across the four runs. "Device bytes a byte" is what `/proc/diskstats` counted
written on every node's storage device over the run, over the bytes the client sent. It is read
when an arm's last answer is in, before its merges finish
([TODOs](../appendix/todos.md#settle-a-bench-step-before-reading-its-device-counters)). The
captures are `f80-one-33d1cbb`, `f80-three-33d1cbb` and `f80-paced-33d1cbb` under the repository's
`target/shoaladm-bench/`, and are not committed.

**One node:**

| Arm | ops/s | MiB/s | p50 ms | p99 ms | worst second's p99 ms | device bytes a byte |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| put 4 KiB | 1,081 [1,032–1,101] | 4.2 | 6.95 [6.78–7.12] | 14.2 [14.0–14.8] | 17.6 [17.3–24.5] | 6.11 [5.87–6.26] |
| put 64 KiB | 726 [723–729] | 45.4 | 10.5 [10.5–10.6] | 22.3 [21.9–23.6] | 26.1 [24.1–36.2] | 1.71 [1.70–1.75] |
| put 256 KiB | 417 [414–420] | 104.3 | 18.5 [18.5–18.5] | 38.0 [35.1–40.5] | 45.5 [43.6–51.2] | 1.74 [1.73–1.76] |
| put 1 MiB | 110 [110–110] | 110.2 | 71.2 [71.2–71.3] | 122 [121–125] | 148 [139–159] | 2.44 [2.44–2.45] |
| put 4 MiB | 28.0 [27.9–28.0] | 111.9 | 286 [285–286] | 336 [329–337] | 375 [347–397] | 2.50 [2.50–2.52] |
| put 16 MiB | 6.9 [6.9–6.9] | 110.9 | 1,143 [1,143–1,146] | 1,185 [1,168–1,217] | 1,204 [1,169–1,223] | 2.59 [2.58–2.60] |
| get 4 KiB | 16,909 [14,319–18,736] | 66.1 | 0.47 [0.41–0.55] | 0.78 [0.66–1.02] | 0.90 [0.74–1.63] | |
| get 64 KiB | 1,791 [1,791–1,791] | 111.9 | 4.48 [4.47–4.55] | 6.04 [5.73–7.05] | 6.52 [5.96–7.60] | |
| get 256 KiB | 449 [449–449] | 112.2 | 14.3 [14.0–17.5] | 23.3 [22.8–27.5] | 23.3 [22.9–27.6] | |
| get 1 MiB | 112 [112–112] | 112.3 | 71.0 [71.0–71.4] | 72.0 [72.0–72.5] | 72.0 [72.0–72.5] | |
| get 4 MiB | 28.1 [28.0–28.1] | 112.4 | 224 [215–285] | 375 [293–427] | 375 [321–445] | |
| get 16 MiB | 7.0 [6.9–7.1] | 112.0 | 990 [823–1,139] | 1,529 [1,172–2,247] | 1,540 [1,177–2,247] | |

**Three nodes:**

| Arm | ops/s | MiB/s | p50 ms | p99 ms | worst second's p99 ms | device bytes a byte, all hosts (a host) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| put 4 KiB | 1,035 [927–1,051] | 4.0 | 7.11 [7.04–8.16] | 14.1 [13.3–18.0] | 15.7 [14.2–24.5] | 16.4 (5.47) |
| put 64 KiB | 540 [509–663] | 33.8 | 12.8 [11.6–13.2] | 32.5 [23.9–37.8] | 335 [26–911] | 5.20 (1.73) |
| put 256 KiB | 204 [185–240] | 51.0 | 30.7 [29.6–34.5] | 213 [71–354] | 1,427 [646–1,607] | 5.41 (1.80) |
| put 1 MiB | 63.4 [55.9–64.5] | 63.4 | 107 [107–119] | 633 [317–690] | 1,992 [1,159–2,691] | 7.48 (2.49) |
| put 4 MiB | 15.3 [11.5–16.4] | 61.1 | 433 [400–448] | 2,944 [2,363–4,065] | 3,515 [2,488–5,353] | 7.68 (2.56) |
| put 16 MiB | 3.6 [3.0–4.0] | 57.6 | 1,701 [1,466–1,744] | 8,055 [5,341–15,606] | 8,055 [5,341–15,606] | 7.88 (2.63) |
| get 4 KiB | 9,396 [8,085–9,831] | 36.7 | 0.81 [0.75–0.97] | 1.70 [1.68–2.06] | 2.06 [1.92–2.40] | |
| get 64 KiB | 2,694 [2,430–4,378] | 168 | 3.13 [1.22–3.49] | 6.00 [5.74–6.76] | 6.37 [6.28–7.18] | |
| get 256 KiB | 819 [615–897] | 205 | 9.96 [4.22–14.3] | 30.0 [28.5–32.8] | 34.1 [31.7–35.8] | |
| get 1 MiB | 274 [194–710] | 274 | 4.09 [3.22–47.6] | 119 [83–125] | 142 [94–316] | |
| get 4 MiB | 51.1 [35.0–69.0] | 205 | 180 [6–238] | 530 [479–549] | 651 [639–755] | |
| get 16 MiB | 12.7 [9.5–15.3] | 204 | 455 [20–998] | 1,898 [1,590–1,949] | 1,939 [1,629–2,068] | |

**From 64 KiB up, one node's figures are the network's.** Puts and gets alike reach 110 to 112
MiB/s, the link between europa and titan. With eight in flight on a full link, each operation
takes eight operations' transfer time, so a 16 MiB put's median is 1.14 s and its p99 is barely
above it. These numbers say what 1 GbE carries, not what the node can. At 4 KiB the node is the
limit: a put is a `Quorum` read and a conditional commit, about 7 ms and 1,081 a second at eight
in flight.

**On three nodes, gets beat one node's**, at 205 to 274 MiB/s, because europa is the driver and a
node: the rows europa leads are read without crossing the network. Their ranges are wide for the
same reason, since which node leads which group moves between the bench's resets.

**On three nodes, puts from 256 KiB up have a tail one node's do not.** Their p99 is 7 to 8 times
their median, and at 16 MiB a run's p99 reached 15.6 s. **The tail is not attributed.** What was
looked at:
- **No errors.** None were counted, and nothing was lost or misread.
- **Retries are not counted.** The object client retries a retriable failure under the same
  identity, so the bench sees a retry as latency only
  ([TODOs](../appendix/todos.md#object-bench-gaps-after-m13)).
- **Leads moved early in each window.** The nodes' logs show the groups' leads moving to their
  preferred leaders in the first two seconds of each measured window after the bench's reset. That
  happens at every size, and small puts show no tail.
- **Europa's uplink is shared.** At a lead weight of two, europa leads about half the groups, so its
  one uplink carried the client's bytes and much of the replication. Its link was not measured.
- **Maxima past eleven seconds** match an object task's internal query giving up at the query
  deadline and its second of slack, followed by the client's retry. Nothing logs or counts such a
  give-up ([TODOs](../appendix/todos.md#object-bench-gaps-after-m13)).

[X3](../object-storage/bytes-through-groups.md) found wide rows through the tablet groups costly,
and filed items 214 to 216 for what they do to the lanes, the feed and memory. This tail is
consistent with them and was not separated from them.

**Every stored byte is written about 2.5 times on each host at 1 MiB and up**: once to the WAL and
again when it is merged into an archive. At 4 KiB the multiple is 5.5 to 6. An object's entry
carries a few hundred bytes of metadata, and the WAL pads a group commit to a block; both weigh
against 4 KiB, but the multiple was not split between them. The 64 and 256 KiB arms read 1.7 to 1.8 because the counters are read before an arm's
merges finish, which undercounts the archive's share.

**A small query beside a stream.** A paced stream read Movie at 50 a second, on the client's
other connections, beside each object arm on three nodes (`--paced Movie --paced-rate 50`). The
bench cannot run the paced stream with no arm beside it. The reference is the lightest arm it
runs: a stat of a 1 MiB object, one in flight, at about 1,260 a second, which moves no object
bytes.

| Beside | Its rate | Paced read p99 ms | Worst second's p99 ms |
| --- | --- | ---: | ---: |
| `stat100/s1m/c1`, the reference | 1,265 stats/s | 1.83 [1.35–2.08] | 1.93 [1.74–3.92] |
| `put100/s1m/c1` | 23.5 MiB/s | 1.75 [1.69–1.96] | 4.03 [2.87–4.23] |
| `put100/s1m/c8` | 69.3 MiB/s | 3.17 [2.70–3.56] | 4.64 [3.87–7.66] |

**Beside a stream of 1 MiB puts at 69 MiB/s, a small query's p99 is 1.7 times its reference, and
the ranges are disjoint.** Beside one put in flight it is within noise. That is the query on its
own connections, as Q26 set: X11 measured 4 to 29 times on a shared one.

**What the baseline is for.** Every object figure at a later gate is read against these. M15's
stripes are where a large object stops being a row, and its first measurement is the same arms
on the same hosts.

### The tmdb cluster with its bucket bound (D8)

The tmdb cluster was destroyed and bootstrapped afresh at `33d1cbb` (`tmdb-dataset-loader
destroy`, then `bootstrap -i tmdb_cluster.yaml`). Its inventory binds Posters to a pool `fast`
of three copies across hosts. The bootstrap activated wire 7 itself, "wire 7 is activated; no node
rolls back below it". The first load wrote nearly all of the dataset, then printed `0 rows/s` for
ten minutes. It was started within a minute of the bootstrap, while the leads were being handed
back, as [item 225](../appendix/known-issues.md#225-a-load-that-had-every-row-acknowledged-never-finished)'s
one sighting was, and is its second. Run again at once, the load wrote 3,382,336 rows in 42.7 s
(79,171 rows a second, 61 retried after `Shedding`), and its read-back found all 10,073 movies it
sampled. All three members are up, at wire 7.

## Tests

| Test | What breaks if F80 is reverted |
| --- | --- |
| `shoal` `objects::object_frames_are_refused_without_the_capability` | A client without the bit refused by name and writing nothing; a raw connection without it closed on its first object frame while others answer; the bit withheld without streams |
| `shoal` `objects::retried_write_across_frames_is_the_same_write` | A put cut mid-stream and retried under its identity, and a put sent twice, leave one object with one id |
| `shoal` `objects::malformed_object_frame_ends_one_connection` | Each malformed shape closes its connection, and the client beside it is still answered |
| `shoal` `objects::colliding_paths_are_told_apart` | Two paths forced into one row stored, read, replaced and deleted independently |
| `shoal` `objects::inline_object_never_touches_a_device` | An object at the threshold written and read with no device, one past it refused `TooLarge` with its bytes drained |
| `shoal` `objects::unbound_bucket_is_refused_by_name` | A start with a bucket bound to nothing, or to an undefined pool, refused by name, standalone and bootstrapping |
| `shoal` `objects::an_inline_object_round_trips` | Put, get, ranges, stat, replace and delete through the handle |
| `shoal` `objects::an_inline_object_survives_a_restart` | Forty objects replayed from the intent log and read back |
| `shoal` `objects::object_operations_share_a_connection_while_they_wait` | Item 230: six stats on one connection set apart, every answer late, taking six round trips |
| `shoal` `cluster_fixture::pool_policy_is_committed_and_a_joiners_copy_is_ignored` | A joiner configured with another redundancy and another pool reads the bootstrapper's policy through `Pools` |
| `shoal` `buckets::bucket_declaration_moves_the_fingerprint_and_its_binding_does_not` | A node bound to another pool accepts the same client |
| `bench-dataset` `objects::object_arm_round_trips_against_one_node` | Preload, `put100`, `get100` and `rw50` against a node in process, nothing lost or misread |
| `bench-dataset` `objects::every_acknowledged_byte_is_read_back` | A byte of an acknowledged object flipped on disk fails the run |
| `bench-dataset` `objects::an_object_run_writes_a_capture` | An object run's capture keeps its description, its arms and their read-back |
| `bench-dataset` `objects::a_paced_stream_runs_beside_an_object_arm` | A paced stream over the folder's only table beside an object arm, and no table arm added |
| `shoaladm` `bench::args::tests::object_mixes_alone_choose_a_run_of_objects` | `--objects` or a spec's `objects` alone choosing the run with no table workload, and beside `--workloads` keeping both |
| `shoal-proto` `checksum::tests::checksum_is_stable_across_builds` | X5's literals |
| `shoal-proto` `checksum::tests::{a_checksum_fed_in_pieces_equals_one_call, the_combine_equals_one_call_over_the_whole, a_chain_of_units_equals_one_call}` | The pieces, the combine against `crc_fast`'s and one call, the chain |
| `shoal-proto` `protocol::object::tests::{operations_round_trip, answers_round_trip, attributes_round_trip_and_refuse_garbage}` | The codec |
| `shoal-proto` `protocol::object::tests::{an_operation_that_cannot_be_right_is_malformed, a_puts_identity_is_a_v7_uuid}` | The malformed line |
| `shoal-proto` `protocol::object::tests::a_bucket_refuses_by_name_what_it_does_not_serve` | `judge_op` |
| `shoal-proto` `pools::tests::*` (five) | Sizes, the documented shape and unknown keys, unbound and undefined, a pool's facts, consumer ids minted once |
| `shoal-proto` `objects::tests::{object_ids_follow_their_put_and_path_and_are_frozen, a_row_finds_an_entry_by_its_whole_path}` | `object_id`, and an entry found by its whole path |
| `shoal-proto` `protocol::tests` (the type table and the caps pin) | Types 28 and 29 and the objects bit |
| `shoal-loadgen` `objects::tests::{splitmix64_ctr_is_frozen, a_fill_seeks, descriptions_read_and_draw, mixes_read_and_spell_back}` | X13's digests, a range made alone, descriptions, mixes |
| `shoal-loadgen` `spec::tests::object_arm_ids_are_unchanged` | The arm ids and a spec's digest |
| `shoal-loadgen` `spec::tests::table_arm_ids_are_unchanged` | A spec of tables alone digesting as before |
| `shoal-core` `shard::objects::tests::a_lanes_id_is_never_a_connections` | A lane's id told from a connection's and a bundle's, and one a shard |
| `shoal` `client_disconnect::a_client_that_leaves_is_forgotten_by_every_shard` | Unchanged, and failed with the lanes counted as connections: "every shard should hold all eight connections, but they hold [10, 10]" |

The nine acceptance rows are the first seven `shoal` and fixture tests above and the first two
`bench-dataset` ones.

## Related

[S12](../object-storage/wire-and-client.md), [S3](../object-storage/objects.md),
[S4](../object-storage/pools-and-devices.md) and [S15](../object-storage/performance.md), the
designs; [M13](../object-storage/milestones.md#m13-the-wire-and-the-baseline), the gate;
[F73](bodies-across-frames.md), the streams; [F68](conditional-writes.md), the commits;
[F79](bucket-schema.md), the rows; [F74](client-routing.md), the routing; [F75](client-cancel.md),
the cancel; [X5](../object-storage/checksums.md), [X11](../object-storage/streamed-bodies.md) and
[X13](../object-storage/benchmark-shape.md), the spikes it rests on;
[Resolved #228](../appendix/resolved/aligned-records.md), found on the way.
