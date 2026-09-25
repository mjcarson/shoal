# 126. A storage directory the node could not create was refused without its path

## Symptom

`cargo run --example tmdb_dataset` on a host with no `/opt/shoal` stopped before a shard started:

```text
config: shoal.yml
ERROR ShoalPool::start:server::claim_root{executors=12}: shoal_core::server: error=IO(Os { code: 13, kind: PermissionDenied, message: "Permission denied" })
ERROR ShoalPool::start: shoal_core::server: error=IO(Os { code: 13, kind: PermissionDenied, message: "Permission denied" })
thread 'main' panicked at shoal/examples/tmdb_dataset.rs:839:51:
failed to start shoal: IO(Os { code: 13, kind: PermissionDenied, message: "Permission denied" })
```

The error never mentioned `/opt/shoal`, the committed `shoal.yml`'s storage root. The span name
`claim_root` was the only hint that storage was involved. A configuration can name more than one
root (a throughput writer somewhere other than the primary, or a table's own root), so even a
reader who guessed "storage" could not tell which root had failed.

## Cause

`DirectoryLock::acquire` is the first thing a start writes. It calls `create_dir_all(root)` and
then opens the lock file, and both used `?`, so the `io::Error` became `ServerError::IO` with only
what the os said. The os does not name the path. `StorageMeta::write`, which mirrors the marker
onto every other root ([Resolved #43](marker-every-root.md)), and `StorageMeta::read` did the
same, so a second root that could not be created failed the same way.

The example also had no way to put its storage anywhere else. It loads the committed `shoal.yml`,
which is the benchmark configuration and is not edited for one run. The `SHOAL_` environment
overlay has no separator configured and cannot reach a nested key such as
`storage.default.filesystem.latency_sensitive.path`.

## Evidence

**Reproduced against the unfixed tree** on the development host. The failing run above is the
user's report. The two tests below each create a root under a parent made read only (`0o555`),
standing in for `/opt`, and failed before the fix with the same error:

```text
---- server::meta::tests::an_unusable_storage_directory_names_its_path stdout ----
refused without naming the directory: IO(Os { code: 13, kind: PermissionDenied, message: "Permission denied" })

---- server::meta::tests::an_unusable_mirror_root_names_its_path stdout ----
refused without naming the root: IO(Os { code: 13, kind: PermissionDenied, message: "Permission denied" })
```

## The fix

**A root that cannot be created or opened is refused as `ShoalError::StorageDirectoryUnusable {
path, error }`**, with the `io::Error` kept whole. It displays as `cannot use the storage
directory /opt/shoal: Permission denied (os error 13)`. The path is whichever one failed: the
root for `create_dir_all`, the lock file for its open, the staged marker for its create, and the
marker for a read that failed for any reason other than `NotFound`. A single helper, `unusable` in
`server/meta.rs`, builds the variant.

What stays a bare `IO` error, and why: the `flock` failure other than `EWOULDBLOCK`, and the
write, sync and rename of an already open marker. Those are errors from a file the node did open,
so they are about the device, not about whether the node may use the path.

~~The example gained two flags, so it runs without `/opt/shoal` and without the lab's network:~~
The example and both flags were removed by [F54](../../features/tmdb-dataset-deployment.md),
which made the dataset a deployed database whose nodes read a rendered `shoal.yml`. The
`StorageDirectoryUnusable` fix is unaffected. The flags were:

- `--storage <dir>` moves both the latency and throughput sensitive roots.
- `--local-tracing` drops the configured remote sink. The committed `shoal.yml` exports to the
  lab's collector, and off that network every run waited out the exporter's five second timeout
  at exit.

## Alternatives rejected

- **Adding context in `claim_root` alone**, for example by wrapping whatever `acquire` returned
  with the configured root. `claim_root` knows the primary root but not which step failed. It
  would also name the root for a lock file open that failed, and it would leave the mirror path
  (a different root) unfixed.
- **A pre-check in the example only.** That fixes the one program that hit it. Every other start
  (`shoal-node`, a benchmark server, a user's own binary) would still say `Permission denied`
  about nothing.
- **Converting every `ServerError::IO` in storage startup to carry a path.** Most of those are
  device errors on files already open, where the path adds little. The hosting and rehome
  manifests are the exceptions, and are listed under Still open instead of changed blind.
- **Editing `shoal.yml` to point somewhere under the user's home.** That file is the benchmark
  configuration. Changing it invalidates the baseline, and `/opt/shoal` is where the benchmark
  hosts keep their data.

## Invariants to uphold

- **A failure to create or open a storage path names that path.** Any new first touch of a
  root, such as a new sidecar created at claim time, has to go through `unusable` or an
  equivalent. A bare `?` on `create_dir_all` or an open brings this item back.
- **`StorageDirectoryUnusable` keeps the `io::Error`, not a string of it.** A caller that wants
  to tell `PermissionDenied` from `ReadOnlyFilesystem` or `NoSpace` reads `error.kind()`.
- **`NotFound` from the marker read is still `Ok(None)`.** A fresh root has no marker, and that
  is not an error.

## Still open

- `server/hosting.rs` and `server/rehome/manifest.rs` read their sidecars with a bare
  `ServerError::IO` on anything but `NotFound`. They are only reached after the lock is taken, so
  the root is known to be usable, but a sidecar made unreadable afterwards would fail the same
  way this item did.
- The `SHOAL_` environment overlay (`Conf::from_file`) has no separator, so no nested key can be
  overridden from the environment. Filed in [TODOs](../todos.md).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `an_unusable_storage_directory_names_its_path` (`shoal-core/src/server/meta.rs`) | `DirectoryLock::acquire` under a read-only parent returns `IO(PermissionDenied)` and no path. |
| `an_unusable_mirror_root_names_its_path` | `StorageMeta::mirror` onto an uncreatable second root returns `IO(PermissionDenied)` and no path. |

Both skip when run as root, which ignores directory permissions.

## Related

- [Resolved #43](marker-every-root.md), which added the marker mirror whose write is the first
  touch of a second root.
- [F37](../../features/node-identity-control-plane.md), which introduced the directory lock and
  the claim.
