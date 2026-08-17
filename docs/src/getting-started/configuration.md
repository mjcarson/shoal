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

Unknown keys are rejected inside `resources`, which is `deny_unknown_fields` — see
[below](#the-exluded_cores-typo--fixed). Everywhere else they are still ignored rather than
rejected, so a misspelling outside that block is still silently dropped.

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
  remote:
    Grpc: "http://127.0.0.1:4318/v1/traces"   # optional

storage:
  default:
    filesystem:
      latency_sensitive:
        path: "/opt/shoal"
        buffer_size: 512             # bytes; accepts "4KiB" style strings
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

`Networking::to_addr` prints to stdout as a side effect of formatting the address
(`shoal-core/src/server/conf.rs:111`) — a leftover debug line, so you get a "listening on"
message per shard.

**Unknown keys in this block are refused.** That is deliberate and it is newer than the rest of the
section: a misspelled `tls:` key under a block that ignored it would produce a server that starts,
listens, and serves every query in clear, with nothing anywhere saying so
([F14](../features/encryption-in-transit.md)).

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

`buffer_size` is a *minimum*. At startup it is rounded up to at least one block of the backing
device's direct IO alignment, because every write to the intent log has to be block aligned.
Setting it below the device block size therefore has no effect, and setting it small costs
write amplification at low load — see [pad regions](../storage/intent-log.md#pad-regions).

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

Every filename is prefixed by shard name. This is the mechanism by which shard count becomes
part of the on-disk format — see
[Partitioning](../architecture/partitioning.md#limitations).

Because of that, **changing `resources.cores` between restarts on the same data directory is
refused**. `shoal-meta.json` in the storage root records the shard count that wrote the
directory, and `ShoalPool::start` errors with `ShardCountMismatch` before any shard spawns
rather than starting and failing to find data that moved to another shard. There is no
migration: to change the core count, start from an empty directory.

The marker also carries a `format` version, and a marker written in a format this build does not
know is refused before its shard count is read — a count read out of a layout we cannot interpret
is a guess, and a guess that happens to match starts the server
([item 45](../appendix/resolved/storage-marker-format.md)).

Two holes remain in the guard. It covers the default storage root only, not a per-table
`storage.tables` override
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

- No config validation and no schema. Unknown keys are dropped silently, except in `resources`
  and `auth`, which are `deny_unknown_fields`.
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
