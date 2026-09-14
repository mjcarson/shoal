# Configuration

Shoal is configured by a `Conf` struct (`shoal-core/src/server/conf.rs`), which can be built
either from a YAML file or programmatically with builder methods. The server takes it once,
at `ShoalPool::start`, and clones it into every shard.

## Sources and precedence

```rust
Config::builder()
    .add_source(config::File::with_name(path).required(false))
    .add_source(config::Environment::with_prefix("shoal"))
    .build()?
```

`shoal-core/src/server/conf.rs:269-278`

Two things follow from this:

- The config file is **optional** (`required(false)`). A missing or misnamed `shoal.yml`
  produces defaults, not an error. A typo in the filename is silent.
- Environment variables prefixed `SHOAL_` override file values.

Unknown keys are rejected inside `resources`, `networking`, `auth` and `cluster`, which are
`deny_unknown_fields` — see [below](#the-exluded_cores-typo--fixed). Everywhere else they are
still ignored rather than rejected, so a misspelling outside those blocks is still silently
dropped.

## The full schema

```yaml
resources:
  cores: 16                          # optional; default is all usable cores
  exclude_cores: [28, 29, 30, 31]    # optional
  memory: "4Gi"                      # REQUIRED if `resources` is present

networking:
  interface: "127.0.0.1"             # default
  port: 12000                        # default
  max_frame_bytes: 67108864          # default; 64 MiB
  query_deadline: 10s                # default; how long a bundle may take in all
  tls:                               # optional; omitting it serves plaintext
    cert: "/etc/shoal/server.pem"    # certificate chain, leaf first
    key: "/etc/shoal/server.key"     # the private key for that chain

auth:                                # optional; omitting it requires nothing
  required: false                    # default
  mechanisms: ["SCRAM-SHA-256"]      # default; the only one that can be selected today
  iterations: 4096                   # default; PBKDF2 rounds for a password named below
  users:
    reader:
      password: "hunter2"            # derived at startup, then dropped
    service:
      scram_sha_256:                 # what a deployment with no password on disk writes
        salt: "mreM7hc9ajmfASxDVtMmYA=="
        iterations: 4096
        stored_key: "mclPl9SbAkgUFigfCfZXDextXlX9lbyXp2EkZKstVVg="
        server_key: "1nuhjMmXMbqkuTxZEZzCClfB/wX0AfVCw/pPihZ48Io="

tracing:
  level: Info                        # Trace | Debug | Info | Warn | Error | Off
                                     # RUST_LOG overrides this, per target
  remote:                            # optional; omit for stdout only
    Otlp:
      endpoint: "http://127.0.0.1:4318/v1/traces"   # the full URL, path included
      headers:                                      # optional
        X-Scope-OrgID: Shoal                        # the tenant, on a multi tenant collector
      timeout_secs: 10               # optional, how long one export may take
      batch_delay_ms: 1000           # optional, how often spans are shipped
      max_queue_size: 8192           # optional, spans queued before new ones are dropped
      sample_ratio: 0.001            # optional, fraction of traces exported; default all of them
  metrics:                           # optional; derived from `remote` when omitted
    endpoint: "http://127.0.0.1:4318/v1/metrics"    # the full URL, path included
    headers: {}                      # optional
    interval_secs: 10                # optional, how often recorded metrics are shipped
    timeout_secs: 10                 # optional, how long one export may take

storage:
  default:
    filesystem:
      latency_sensitive:
        path: "/opt/shoal"
        buffer_size: 512             # floor; bytes, or "4KiB" style strings
        max_buffer_size: "256KiB"    # ceiling the staging buffer sizes itself up to
        write_behind: 128
        intent_log_size: "10MiB"
        durability: Fsync            # Fsync (default) or Async
      throughput_sensitive:
        path: "/opt/shoal"
        buffer_size: "128KiB"
        write_behind: 4
  tables:                            # per-table overrides, keyed by table name
    movies:
      FS:
        latency_sensitive:
          path: "/mnt/fast"
cluster:                             # absent: a standalone node. see `### cluster` below
  bootstrap: true
```

### resources

`cores` caps how many shards are started. If unset, Shoal uses every usable CPU.

Two cores are always excluded from consideration:

```rust
let online = CpuSet::online()?
    .filter(|location| location.cpu != 0)
    .filter(|location| !self.exclude_cores.contains(&location.core));
```

`shoal-core/src/server/conf.rs:49-53`

CPU 0 is unconditionally reserved. The comment calls it "the coordinator cpu", though in
practice there is no separate coordinator process — see
[Request Lifecycle](../architecture/request-lifecycle.md) for what actually coordinates.

`memory` is the shard-wide budget that drives eviction. It accepts human sizes (`"4Gi"`,
`"512MiB"`) via a custom deserializer.

> **`memory` has no serde default.** It is the one field in `Resources` without a `#[serde(default)]`
> or a default function (`shoal-core/src/server/conf.rs:22-24`). If you write a `resources:`
> block and omit `memory`, deserialization fails outright. Omitting the whole `resources`
> block is fine — `Conf` marks the field `#[serde(default)]` — and gives you a memory budget
> of **0**, which means eviction triggers immediately and constantly.

### networking

Every shard binds the same address (`shoal-core/src/server/shard.rs:356`). See
[Thread per Core](../architecture/thread-per-core.md#accepting-connections) for how that
works and what it means for which shard receives your query.

~~`Networking::to_addr` prints to stdout as a side effect of formatting the address — a leftover
debug line, so you get a "listening on" message per shard.~~ It no longer does
([Resolved #17](../appendix/resolved/leftover-printlns.md)). The address the shards actually
bound — which differs from the configured one when `port` is `0` — is what `ShoalPool::ready`
and `ShoalPool::bound_addr` return ([F36](../features/cluster-harness.md)).

**Unknown keys in this block are refused.** That is deliberate and it is newer than the rest of the
section: a misspelled `tls:` key under a block that ignored it would produce a server that starts,
listens, and serves every query in clear, with nothing anywhere saying so
([F14](../features/encryption-in-transit.md)).

#### networking.query_deadline

**How long a bundle may take in all**, ten seconds by default, on a standalone node as on a
cluster one ([F41](../features/read-consistency.md)). The budget a split query's gather, a
forward to another node and a strong read's barrier and application waits all share, measured
from the moment the bundle's last byte came off the socket. A query still owed an answer when it
runs out is answered `Timeout` once, in its own table variant, and the state waiting for it is
released; before this existed a share that never arrived held its client forever
([Resolved #33](../appendix/resolved/gather-expiry.md)). A bundle may name a shorter budget of
its own through the client's `SendOptions::deadline` and never a longer one. Written the way
`cluster.primary_failover_after` is: `500ms`, `2s`, `1m`.

#### networking.tls

**Omitting this block serves plaintext**, which is what every deployment before
[F14](../features/encryption-in-transit.md) was and what the benchmark config still is. The same
shape `auth` has, and for the same reason: a capture taken against an encrypted server and one
taken against a plaintext server are not the same measurement, so encryption has to be something a
deployment opts into rather than something it gets.

Both fields are required when the block is present, and both are PEM files. The certificate is the
chain leaf first; the key is the private key for it.

**This needs the kernel's TLS module.** Shoal hands the negotiated keys to the kernel and lets it do
the record layer, which is what keeps the response read copy-free — see
[F14](../features/encryption-in-transit.md) for why that matters. Two consequences for a
deployment:

- the kernel needs `CONFIG_TLS` (it is a module on most distributions);
- **`setsockopt` does not autoload it**, so `modprobe tls` has to have run. A machine that has never
  used kTLS answers `ENOENT`, and a server configured for TLS on such a machine **refuses to
  start** rather than falling back to plaintext.

Make it persistent with `echo tls > /etc/modules-load.d/shoal.conf`.

A client reaches such a server with `Shoal::with_options`, naming the authority it trusts:

```rust
let client = Shoal::<MyDbClient>::with_options(
    "shoal.internal:12000",
    ClientOptions::new().tls(TlsClientOptions::new("/etc/shoal/ca.pem")),
)
.await?;
```

There is deliberately no system root store: the client trusts the authority named here and nothing
else. `TlsClientOptions::server_name` overrides which name the certificate is checked against, for
the case where a deployment connects by address to a certificate carrying a hostname.

### auth

**Omitting this block requires nothing of a client**, which is what every deployment before
[F12](../features/authentication.md) was and what the benchmark config still is — `shoal.yml` in
the repository root has no `auth` block on purpose, because turning it on would change what the
[frozen baseline](../performance/baseline.md) measured.

Setting `required: true` refuses every client that cannot do a mechanism this server accepts,
including one built before authentication existed. A client opts in with
`Shoal::with_credentials` rather than `Shoal::new` — see [The Client](../api/client.md).

Each user is spelled one of two ways, and the difference is where the password lives:

- **`password:`** is derived into a salted, iterated credential when the config is read, and the
  password is dropped. The server does not hold it afterwards. The *file* still does.
- **`scram_sha_256:`** is that derivation written out, so nothing on disk is a password. Generate
  one with `cargo run --example scram_credential -- <username>`, which reads the password off
  stdin and prints the block to paste in. A user that names both takes the derivation.

`stored_key` is not a password and cannot be turned back into one, but it **can** be replayed as a
login by anything that reads it. A config file carrying one wants permissions on it.

A user that names neither is a config error and refuses to start, naming the user.

`iterations` is the PBKDF2 cost, paid by the server once per connection. Raising it raises time to
first query — the pool opens ten connections before it is idle — rather than the cost of an offline
guess against a file an attacker has to have stolen first.

### tracing

The section is only as live as the binary reading it: `trace::setup` installs a **global**
subscriber and the application has to call it. The bundled example does, and so does
`shoal-workload` since [F34](../features/benchmark-tracing.md), which is what makes this section
configure a benchmark capture. `shoalctl` and the tests still install nothing, so this section is
inert for them ([item 69](../appendix/known-issues.md)).

`RUST_LOG` overrides `level` **entirely**, per target, and is what makes an export problem
diagnosable — the OTLP exporter reports every step of a POST at `DEBUG` on its own targets:

```bash
RUST_LOG=info,opentelemetry-otlp=debug,opentelemetry-sdk=debug,opentelemetry-http=debug
```

Two settings are easy to get backwards. **`level` is what costs and `sample_ratio` is what the
collector sees.** A sampler decides after `tracing` has built the span, so it bounds the export and
not the work; `#[instrument]` defaults to `INFO`, so `level: Info` puts a registry slab insert per
query on three per-query callsites in the server. Turn `sample_ratio` down to protect the collector,
and `level` down to protect throughput.

`metrics` may be omitted. With `remote` set, the metrics endpoint is derived from it by swapping
`/v1/traces` for `/v1/metrics`, carrying the tenant header across, because a collector serves both
on one host and port. An endpoint whose path is not the one that rewrite recognizes derives nothing
rather than guessing at a URL. Set the block explicitly when the two sinks are not on one host.

`Grpc: "<endpoint>"` is still accepted where `Otlp:` goes. It never spoke gRPC — it is the old name
for the same OTLP over HTTP exporter — and it widens into an `Otlp` sink with every other field
defaulted.

See [Observability](../operations/observability.md) for what is instrumented and how to read it.

### storage

Storage settings are resolved per table, falling back to the default:

```rust
match conf.storage.tables.get(R::name()) {
    Some(TableSettings::FS(table_conf)) => Ok(table_conf.clone()),
    None => Ok(conf.storage.default.filesystem.clone()),
}
```

`shoal-core/src/server/tables/storage/fs.rs:306-314`

The lookup key is the table's `name()`, generated by the derive macro from the struct name.

#### Two writer profiles

The filesystem engine splits its configuration in two, matching the two very different IO
patterns it has:

| Profile | Used for | Default buffer | Default write-behind |
| --- | --- | --- | --- |
| `latency_sensitive` | The intent log, the archive map, the map's own intent log | 512 B | 128 |
| `throughput_sensitive` | ~~Archives (compacted partition data)~~ **the archive map's intent log only** — see below | 128 KiB | 4 |

`shoal-core/src/server/tables/storage/fs/conf.rs:19-26`, `:96-102`

The intent log is on the critical path of every write, so it uses small buffers and deep
write-behind: get the bytes moving, keep queue depth high. Archives are written in bulk by a
background compactor where per-write latency is irrelevant, so they use large buffers and
shallow queue depth.

> **The second row is what the settings are *for*, not what they currently reach.**
> `ArchiveMap::get_active_writer` builds the archive writers with no `with_buffer_size` and no
> `with_write_behind` in either branch (`.../fs/map.rs:415`, `:433`), so the archives themselves are
> written at glommio's defaults and `throughput_sensitive` governs only the map's own intent log
> (`:403`). Filed as [item 71](../appendix/known-issues.md) and as
> [O33](../appendix/optimizations.md); the sweeps at
> [Configuration and what each setting is worth](../performance/configuration.md) are expected to be
> flat until it is fixed, and that flatness is the evidence.

`intent_log_size` is the rotation threshold — once the active intent log exceeds it,
compaction is triggered ([Compaction](../storage/compaction.md)). It defaults to 10 MiB.

`buffer_size` is a *floor*, and `max_buffer_size` is the ceiling above it. At startup the floor is
rounded up to at least one block of the backing device's direct IO alignment, because every write to
the intent log has to be block aligned. Setting it below the device block size therefore has no
effect, and setting it small costs write amplification at low load — see
[pad regions](../storage/intent-log.md#pad-regions).

Between the two, the writer sizes each staging buffer to hold about **eight** of the widest record
the previous one held ([F23](../features/self-sizing-staging-buffer.md)). So a table with 8 KiB rows
gets a 64 KiB buffer without being told to, and a table with kilobyte rows gets an 8 KiB one.

~~Setting it below your **row** size costs more than that: a record larger than the buffer is
never batched with another one, so every insert becomes its own aligned write and its own DMA
allocation. If your rows are wider than a few kilobytes, this is the first setting to move.~~
~~That advice is read from `StreamWriter::prep` and not from a measurement.~~ ~~It is measured now,
and it needs one correction: setting the buffer merely *above* your row size is not enough. Size
this to a **multiple** of your widest row, not just past it.~~ **The writer does that itself now.**
The measurement is what chose the multiple: the sweep at 8 KiB rows shows a buffer holding two
records is worth nothing over one holding none, and the gain arrives where 8 to 32 records share a
write — **1.22×** at 64 KiB rows ([F22](../features/row-size-benchmarks.md), captured as
`f22-row-size`).

**`max_buffer_size` is the one still worth setting**, and only if your rows are wide. It defaults to
256 KiB; above it the writer is back to one record per DMA write and one DMA allocation per insert,
which is exactly the behaviour the paragraph above describes. It is also the memory bound on the
staging half of the write path — a writer may hold up to `write_behind + 1` buffers of this size at
once, per table, per shard, and `write_behind` defaults to 128. Setting it equal to `buffer_size`
turns the sizing off and gives back the old allocation exactly. See
[Row size and what it costs](../tables/row-size.md) and
[Tuning](../operations/tuning.md#if-your-rows-are-wide).

#### `durability`

How durable a write has to be before its response is released to the client.

| Value | Acknowledgement means | Trade-off |
| --- | --- | --- |
| `Fsync` (default) | The record has been `fdatasync`ed | Survives power loss. Adds a device round trip, amortised by group commit under load. |
| `Async` | The kernel has accepted the write | Faster. A write can be acknowledged and then lost to power loss, since it may still be in the drive's volatile cache. |

Only one `fdatasync` is in flight at a time, so concurrent writes group commit behind the one
already running and the per-write cost falls as load rises. An isolated write still pays the
full round trip.

> The filesystem underneath matters more than this setting. btrfs is copy-on-write and commits
> a log tree on every `fdatasync`, which makes it a poor host for a write-ahead log; ext4 or
> XFS on a drive with power-loss protection is substantially faster. btrfs also silently falls
> back to buffered IO for a misaligned O_DIRECT write where ext4 and XFS return `EINVAL`.

> `FileSystemThroughputWriterConf::write_behind` is a *count*, but is annotated with
> `deserialize_with = "utils::deserialize_byte_size"`
> (`shoal-core/src/server/tables/storage/fs/conf.rs:116-118`). It works, but it means
> `write_behind: "4KiB"` is accepted and yields 4096 in-flight writes. Also
> [item 71](../appendix/known-issues.md), so the two are fixed together.

### cluster

Absent, the server is a standalone node and this page above is the whole of it. Present, the
node is a member of a cluster ([Distributed Shoal](../distributed/overview.md)): it mints a
node identity, bootstraps or joins a cluster, runs a control thread on its own core, and records
the replication policy the cluster was created with. Delivered by
[F37](../features/node-identity-control-plane.md), which built the identity, the bootstrap, the
control thread and the policy record, by [F38](../features/inter-node-transport.md), which
built the peer transport - the lanes, their bounds and their encryption - ~~against a static
placement~~, and by [F39](../features/membership.md), which built the membership: a node joins
through `seeds`, the leader promotes voters to `control_voters` and fences a duplicate identity,
the `failure_detector` calls a silent member down, writes are admitted against the write
consistency, and `admins` names who may change the cluster; and by
[F40](../features/replication.md), which built replication: every tablet has a Raft group
whose log is a shared WAL per shard, a default write waits for a durable majority, and the
`replication:` block below bounds what a proposal may hold and how long it may wait.
~~joining and enforcement of the policy are later milestones and~~ ~~Replication is a later
milestone, and~~ The settings that belong to a later milestone are **refused at startup by
name** rather than accepted and ignored.

This is the block with every default written out. A file that says only `cluster:\n  bootstrap:
true` gets exactly this, and `documented_cluster_defaults_match_policy_bootstrap` holds the
two to each other.

```yaml
cluster:
  bootstrap: true                 # create the cluster on an empty directory; keeps it on a claimed one
  seeds: []                       # control addresses to join through; a joiner names them, a bootstrapper none
  port: 12001                     # the data and bulk lanes, bound by every shard with SO_REUSEPORT
  control_port: 12002             # the control lane, bound by the control thread
  control_core: 0                 # the cpu the control thread is pinned to
  control_core_shared: false      # whether a shard may share that cpu's physical core
  control_voters: 3               # 1, 3 or 5
  replication_factor: 3
  write_consistency: Quorum       # One, Quorum or All
  read_consistency: One
  failure_detector:
    interval_ms: 500              # how often a member reports to the control leader
    phi_threshold: 8.0            # the suspicion at which the leader commits Down
    window: 100                   # report arrivals the leader keeps per member
    min_samples: 5                # arrivals the leader needs before it will suspect anybody
  primary_failover_after: "5s"    # base data election timeout
  auto_remove_after: "30m"        # null disables automatic removal of a Down node; counted by the leader in committed eighths and acted on since F46
  admins: []
  weight: null                    # this node's share of the cluster's bytes against the others' (F46); absent or 0 is its executor count
  slots: null                     # how many slots this node claims, once, at its first claim (F47); absent is one per core, above the cores is headroom, below them is refused, and changing it later is refused
  transport:                      # the peer lanes (F38); every bound is in bytes
    data_queue_bytes: "64MiB"     # queued to one peer on the data lane; a forward past it is shed
    control_queue_bytes: "8MiB"   # the control lane's queue to one peer
    bulk_queue_bytes: "64MiB"     # the bulk lane's queue to one peer
    inflight_bytes: "64MiB"       # forwarded bytes one accepted connection may hold unanswered
    forward_timeout: "5s"         # after this a forwarded query is answered OutcomeUnknown
    reconnect_min: "100ms"        # the first backoff after a lost link, with a quarter of jitter; a link a frame wants never waits longer than this to redial (F42)
    reconnect_max: "5s"           # the longest
    handshake_timeout: "10s"      # to dial and finish the hello
    ping_interval: "1s"           # how often a node pings each member over its control lane
    replication_queue_bytes: "64MiB" # queued to one peer on the replication lane; an append past it is refused and retried
    wire_version: null            # the newest wire version this node advertises (F48); null is the build's newest, a number holds it there through a rolling upgrade; never below the build's floor or the cluster's activated version
  replication:                    # the tablet groups (F40); every bound is in bytes, node-local
    write_timeout: "5s"           # after this a proposal is answered OutcomeUnknown; no longer than forward_timeout
    pending_bytes: "64MiB"        # proposed and unanswered bytes one shard holds per group; a write past it is shed
    segment_bytes: "10MiB"        # a WAL segment is sealed once it grows past this
    checkpoint_entries: 1024      # entries a group commits between snapshots at its checkpoint
    retained_entries: 10000       # entries kept behind the snapshot for a slow member to catch up from
    log_cache_bytes: "16MiB"      # entries the WAL keeps in memory past its durable tail
    volatile_log_bytes: "256MiB"  # every ephemeral table's in-memory log together; a write past it is shed
    snapshot_chunk_bytes: "1MiB"  # one chunk of a snapshot stream on the bulk lane (F43); under max_frame_bytes and bulk_queue_bytes
    snapshot_timeout: "5m"        # after this a snapshot transfer is given up and tried again; no shorter than write_timeout
    install_bytes: "2GiB"         # partial snapshots a shard holds on disk before it refuses a new stream
    retained_bytes: "1GiB"        # sealed WAL a shard keeps for slow members before it forces a snapshot and a purge; at least two segments
    retry_window: "5m"            # how long after a write's identity was minted a retry is still answered its first result (F45); no shorter than write_timeout
  repair:                         # scrubs and repairs (F44), node-local
    scrub_interval: null          # how often every group this node leads is verified on its own; absent or null is never, and a pass never installs
    timeout: "5m"                 # one scrub: the entry committed and every member's digest polled; no shorter than replication.write_timeout, and no longer than scrub_interval
    concurrent: 1                 # group repairs one shard drives at a time; at least one
  migration:                      # moves of a replica set (F45), node-local
    catchup_lag: 64               # entries behind the leader a move's learner may be when it is made a voter
    timeout: "10m"                # one phase of a move; no shorter than replication.snapshot_timeout, since the learner phase is a transfer
    retire_after: "5m"            # how long the source keeps a retired copy's files, refusing every query of them by name, before they are reclaimed
    concurrent: 1                 # group moves one shard drives at a time; at least one
    stream_bytes_per_sec: "64MiB" # one token bucket every snapshot stream this node sends draws on, across every group (F46); 0 is unlimited; no smaller than replication.snapshot_chunk_bytes
    concurrent_streams: 2         # snapshot streams one shard installs at a time; the rest refused at their begin and fed again; at least one
    disk_reserve: "1GiB"          # free bytes kept above what a stream would land, checked by the planner and by the receiver
  rebalance:                      # the plans the control leader drives (F46), read by this node when it leads
    moves_per_node: 1             # moves one member may be the source of, and the destination of, at a time; at least one
    hysteresis: 0.10              # the share of its target a member has to be over before a rebalance moves a set off it; at least 0 and under 1
    plan_interval: "5s"           # how often the leader looks at its open plans; no shorter than failure_detector.interval_ms
  backup:                         # backups this node's groups' leaders drive (F49), node-local
    concurrent: 1                 # group backups one shard drives at a time; at least one
    timeout: "10m"                # one group's backup, the cut and the copy together; no shorter than replication.snapshot_timeout
```

Two settings have no default and are absent above: `advertise`, the address peers reach this
node at, which defaults to `networking.interface` and **must be given** when that is `0.0.0.0`
or `::`; and `client_advertise`, the client address the topology reports if it differs from the
one bound. ~~`tls` - `cert`, `key`, `ca` - is refused until M2.~~ Two more are absent because
they have no default that means anything:

- `tls` - `cert`, `key`, `ca` - makes every peer lane mutual TLS 1.3 handed to the kernel,
  exactly as `networking.tls` does for clients: the listener requires a certificate chained to
  `ca`, the dialler presents its own. A file it names that cannot be read is refused at startup.
  Absent, the lanes are plaintext and **peer identity is trusted inside whatever boundary the
  deployment draws around them** - which is the honest statement of what a plaintext lane
  proves, and why C2 asks for it to be written down. The binding of a certificate to one node's
  identity is not checked yet ([F38, Limitations](../features/inter-node-transport.md#limitations)).
- ~~`placement` - a list of `{node, data, control, shards}` naming every node of the cluster by
  the identity in its marker.~~ Gone since [F39](../features/membership.md): the cluster's
  membership is what the control group commits, and a file that still carries the block is
  refused as an unknown field. Tablet `t` still belongs to `nodes[t % N]` and, on that node, to
  shard `(t / N) % shards` - over the nodes the one explicit `Initialize` admin operation named,
  in that order. Before it, the bootstrapping node holds every tablet and a joiner holds none.
- `dial` - a map from a member's node id to `{control, data}`, the addresses *this* node dials
  that member at instead of the ones it advertises, for a network where a member is reached
  through a different address from each side. Either half may be left out. The cluster fixture
  uses it to put a fault proxy on each direction of each lane.

**Two halves.** `advertise`, `port`, `control_port`, `client_advertise`, `control_core`,
`control_core_shared` and `weight` are this node's - the weight is recorded on the node's
member record when it observes itself, so changing it is a restart. `control_voters`, `replication_factor`, the two
consistencies, `failure_detector`, `primary_failover_after`, `auto_remove_after` and `admins`
are the cluster's: the bootstrapping node writes them into the control state as its
`BootstrapPolicy`, and after that a change is an admin operation, not an edit to a file. A
joiner's copy of them ~~will be~~ is ignored ([C1](../distributed/node-identity.md)). ~~**At M1 the policy is recorded and reported, not enforced**: a
one node cluster serves every read and write locally, exactly as a standalone node does, and the
topology view reports the desired replication factor beside the active one (which is 1).~~
**Since [F39](../features/membership.md) the policy is half enforced**: `control_voters` is
the count the leader promotes learners to and never past; `write_consistency` and
`replication_factor` decide how many members must be up before a write is admitted - one for
`One`, `rf / 2 + 1` for `Quorum`, `rf` for `All` - and a write that falls short is refused
`QuorumUnavailable` naming the shortfall, which readiness reports too. ~~What is written still
lives in one copy: the topology reports the desired factor beside the active one, which is 1
wherever a node is placed, until M4 replicates.~~ **Since [F40](../features/replication.md)
the factor is what a tablet is replicated at**: `min(replication_factor, nodes placed)`
copies, on distinct nodes, and the topology reports the desired factor beside that active
one; a `Quorum` write is acknowledged once a majority of the group has fsynced it and this
node applied it, an `All` write once every voter has it, and `One` is refused at startup
naming C5 - as is a persistent table configured `Async` on a cluster node, since a receipt
that precedes an fsync cannot make a durable quorum. **Since [F41](../features/read-consistency.md)
`read_consistency` is read**: it is the level a read is served at when neither the bundle nor
the table says - `One`, the local replica's applied state, or `Quorum`, a read barrier from the
group's leader and an application wait through it; `All` is refused at validation naming C6,
since the strong read level is `Quorum` and nothing waits on every replica. A table's own level
is versioned control state set by the `SetTableReadPolicy` admin operation - `one`, `quorum`,
or nothing to clear it - and never a YAML setting, so every coordinator resolves a table the
same way. `primary_failover_after` is the base the
groups' timers derive from: a heartbeat every tenth of it, an election between one and two of
it, and under 100 ms it is refused. Since [F42](../features/primary-failover.md) every node's
groups read it from the map rather than from their own file, and what it makes the failover
window is worth knowing before tuning it: a follower refuses every vote for twice the base
after it last heard from its leader, so a dead leader is replaced between two and three times
the base later - ten to fifteen seconds at the default - and a leader that returns sooner is
refused its old term until then ([C7](../distributed/failover.md#the-window-and-what-a-client-sees)).
`admins` names the principals an
authenticated client connection may change the cluster as; a mutation from anybody else is
refused `Unauthorized`. `failure_detector` is the leader's: every member reports at
`interval_ms`, the leader fits the last `window` arrivals ~~once it has `min_samples`~~ with the expected
interval standing in for any of `min_samples` it has not seen yet
([Resolved #101](../appendix/resolved/short-lived-member-detection.md)), and a
member past `phi_threshold` is committed `Down` until it reports again. `seeds` are control
addresses - `control_port`, not `port` - and a node names them or `bootstrap: true`, never
both. A joiner's directory says so in its marker until a leader admits it, and is refused by
`bootstrap: true` until then.

Durations are written with a unit - `500ms`, `5s`, `30m`, `2h` - and a bare number is refused.

**The control core.** `control_core` names a *cpu*, checked against the process's affinity -
a container's cpuset, a `taskset` - rather than against what is online, and refused by name if
it is outside it. Unless `control_core_shared` is set, that cpu's whole physical core, both SMT
threads, is kept away from the shards; on a machine too small for that, set it and the sharing
is recorded in the topology view and in every benchmark artifact rather than hidden. The default
of cpu 0 is the coordinator cpu the shards already leave alone - but standalone mode leaves
only cpu 0 alone and lets a shard run on its sibling, because the benchmark layout depends on
it, so a cluster node with default settings has one fewer shard candidate than a standalone one
on the same machine.

### What to set these to

This page says what each setting **is**. What each one is *worth* is measured — nine of them are
swept at [Configuration and what each setting is worth](../performance/configuration.md), which
names, per setting, the value that answered a query fastest, the value that answered the most of
them, and whether the difference between the best and worst arm is larger than the run-to-run noise
at all. [Tuning](../operations/tuning.md) turns that into advice per workload shape.

Read the *Real?* column before acting on any of it: several of these settings do not move anything
measurable for the reference workload, and knowing which is worth more than a recommendation.

## On-disk layout

The path helpers in `FileSystemTableConf` (`.../fs/conf.rs:205-268`) produce this tree per
table. Note that intents and maps hang off the *latency-sensitive* path while archives hang
off the *throughput-sensitive* path, so the two can live on different devices:

```
<latency_sensitive.path>/<table>/
├── intents/
│   ├── Shard-0-active                 # the live WAL for shard 0
│   ├── Shard-0-inactive-3             # rotated, awaiting compaction
│   └── Shard-1-active
└── maps/
    ├── Shard-0                        # checksummed archive map snapshot
    ├── Shard-1
    └── temp/
        └── Shard-0                    # staging file for atomic rename

<throughput_sensitive.path>/<table>/
└── archives/
    ├── <uuid>                         # compacted partition data
    ├── <uuid>
    └── intents/
        └── Shard-0                    # the archive map's own intent log
```

Every filename is prefixed by shard name. ~~This is the mechanism by which shard count becomes
part of the on-disk format~~ Since [F47](../features/local-rehome.md) the shard in the name is
an *executor*, one per core, and `shoal-hosting.json` beside the marker says which executor owns
each tablet and, on a cluster node, hosts each *slot* — see
[Partitioning](../architecture/partitioning.md#limitations).

~~Because of that, **changing `resources.cores` between restarts on the same data directory is
refused**. `shoal-meta.json` in the storage root records the shard count that wrote the
directory, and `ShoalPool::start` errors with `ShardCountMismatch` before any shard spawns
rather than starting and failing to find data that moved to another shard. There is no
migration: to change the core count, start from an empty directory.~~ **Changing
`resources.cores` between restarts on the same data directory runs a rehome** before any shard
starts ([F47](../features/local-rehome.md)): the executors that no longer run have their intent
logs folded into their archives, their records copied onto the executors that remain, their
tablet groups' logs moved, and their files reclaimed, under `shoal-rehome.json`, a manifest a
crash at any point resumes on the next start at the same count. The start is held for it - the
`rehome` group of the benchmarks prices the hold - and the pool logs what moved at INFO. Three
things are still refused by name: a start under a *third* count while a manifest towards a
second is on disk (`RehomeInProgress`, which names the count to start with), a `cluster.slots`
that differs from the one the directory was claimed with (`SlotsFixed`), and on a cluster node
more cores than slots (`CoresExceedSlots`; the way up is a `Replace` onto a fresh identity).

The marker is **format 2** since [F37](../features/node-identity-control-plane.md), format 3
since [F39](../features/membership.md), and since F47 carries an optional `physical`:

```json
{
  "format": 3,
  "shards": 12,             // the count the directory was laid out as; on a cluster node its slots, claimed once
  "physical": 8,            // F47: the executors the files are laid out on now; absent means shards
  "node": "5b1f…",          // minted the first time the directory was claimed, never changed
  "cluster": null,          // the cluster id a bootstrap minted, or null for a standalone node
  "layout": 1,              // the shard layout the data is under; 1 is tablet % shard_count
  "topology": 0,            // the last topology version the control plane observed
  "mode": "standalone",     // F39: standalone, cluster, or joining
  "incarnation": 3          // F39: how many times the directory has been started
}
```

The format is checked first and alone, and a marker in a format this build does not read is
refused before its shard count is trusted - a count read out of a layout we cannot interpret is
a guess, and a guess that happens to match starts the server
([item 45](../appendix/resolved/storage-marker-format.md)). That includes **format 1**, the shape
of every directory written before F37: the refusal names the format found, the formats this
build reads, and ~~that no migration between formats exists yet - M10 owns one, and "delete the
directory" is a development answer rather than an upgrade procedure~~ that a marker is never
migrated in place, which since [F48](../features/rolling-compatibility.md) is the supported
answer rather than a gap: a directory in a format this build does not read is served by the
build that wrote it, or its data brought into a new cluster by a restore of a backup or of an export. The
same marker is what refuses a mode change: a directory bootstrapped into a cluster is refused by
a config with no `cluster:` block, and a standalone directory is refused by one with a block,
naming ~~M10's migration~~ the export and restore that is the supported path
([F49](../features/backup-and-recovery.md)). ~~`topology` is the one field ever rewritten in place~~ `topology`, `incarnation`,
`physical` and - once, for a joiner - `cluster` are the fields rewritten in place; the identities,
the shard count and the layout are written once. Beside it, `shoal.lock` is an advisory lock a
running server holds, so a second process on the same directory is refused rather than claiming
the same node; `shoal-hosting.json` is the executor table, absent until a rehome or a claim with
headroom writes one; and `shoal-rehome.json` exists only while a rehome is in progress.

Two holes remain in the guard. It covers the default storage root only, not a per-table
`storage.tables` override, whose files a rehome moves untested
([item 43](../appendix/known-issues.md#43-the-storage-marker-only-guards-the-default-storage-root)),
and a directory with *no* marker is claimed rather than refused — which includes every directory
written before the marker existed
([item 46](../appendix/known-issues.md#46-an-unmarked-storage-directory-is-claimed-rather-than-refused)).
If you have a data directory older than the marker, start from an empty one.

Directories are created at startup by `setup_paths`, which walks each path component and
calls `Directory::create` on it (`.../fs/conf.rs:158-171`, `:276-284`). The parent path
(`/opt/shoal` by default) must already exist and be writable.

## Building a config in code

The tests do this rather than writing YAML, and it is the clearer approach for embedding:

```rust
Conf::default()
    .resources(
        Resources::default()
            .cores(2)
            .memory("100MiB")?,
    )
    .networking(Networking::default().port(port))
    .auth(Auth::default().required(true).user("reader", "hunter2"))
    .storage(
        Storage::default().default_settings(
            DefaultStorageSettings::default().filesystem(
                FileSystemTableConf::default()
                    .latency_sensitive(
                        FileSystemLatencyWriterConf::default().path(temp_dir.path()),
                    )
                    .throughput_sensitive(
                        FileSystemThroughputWriterConf::default().path(temp_dir.path()),
                    ),
            ),
        ),
    )
```

`shoal/tests/utils.rs`, `build_config` and `build_auth_config`. The `auth` line is what
`build_auth_config` adds; every other test omits it and gets a server that requires nothing.

## ~~The `exluded_cores` typo~~ — fixed

> ~~The checked-in `shoal.yml` sets `exluded_cores`, the struct field is `exclude_cores`, and
> because the `config` crate ignores unknown keys **this setting does nothing**.~~

Both halves of this are fixed, and the fix is wider than the typo:
`Resources` is now `#[serde(deny_unknown_fields)]`, so a misspelled resource setting **fails
the load and names the key it could not place** rather than being dropped. Correcting one
spelling would have closed the entry; it would not have closed the class.

Checking that the corrected key did anything then turned up a considerably larger defect —
which cpu each shard ran on was decided by a hash seed and changed on every process start. See
[Resolved #18, #50](../appendix/resolved/excluded-cores-typo.md).

Note the neighbouring hazard this does **not** remove: `Conf::from_file` marks the file
`required(false)`, so a typo in the config file's *path* still runs silently on defaults.
`deny_unknown_fields` catches a bad key inside a file that was found; it cannot catch a file
that was never read.

## Design notes

Making the config file optional and ignoring unknown keys is a deliberate
convenience — Shoal starts with zero configuration. The cost is that every configuration
mistake is silent: a misspelled filename, a misspelled key, and a missing file are all
indistinguishable from "the user wanted defaults". For a database where `memory` defaults to
0 and paths default to `/opt/shoal`, that is an expensive default.

## Limitations

- No config validation and no schema. Unknown keys are dropped silently, except in `resources`,
  `networking`, `auth` and `cluster`, which are `deny_unknown_fields`.
- Credentials are read once, at startup. There is no way to add, remove or rotate a user without
  restarting the server.
- No way to see the effective configuration at runtime; it is not logged at startup.
- `memory` defaulting to 0 when the `resources` block is absent means an unconfigured Shoal
  runs permanently under memory pressure.
- Per-table storage settings are keyed by a string that must match the macro-generated table
  name exactly, with no check that a key matched anything.
- `throughput_sensitive` reaches the archive map's intent log and not the archive writers
  ([item 71](../appendix/known-issues.md)), so tuning it today changes less than its name suggests.
- Nothing here is validated against what is measured. `shoal.yml` can be set to a value
  [Configuration and what each setting is worth](../performance/configuration.md) reports as the
  worst arm of its sweep, and no test minds — the committed file is the baseline every historical
  capture was taken under, so it is deliberately not chased.
