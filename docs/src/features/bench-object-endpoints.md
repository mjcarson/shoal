# F94. The bench drives another store's S3 endpoints

`shoaladm bench` measures another object store with the driver that measures Shoal. Since this
feature its object driver runs over an **endpoint**: a Shoal member's client, or an S3 endpoint
of another store. `shoaladm bench s3` drives Ceph's RGW, MinIO, RustFS and SeaweedFS with the same
spec, arms, seeded bytes, windows, read back and capture as `shoaladm bench run` makes of a Shoal
cluster. What a store is given, and what it cost, is now in every capture of every store,
Shoal's included:

- the host's busy cpu-seconds;
- the bytes each network interface carried;
- the page cache and the memory available around the run;
- the store's own processes' cpu-seconds and resident high water mark;
- how full each device was before the run.

[Shoal beside other object stores](../object-storage/store-comparison.md) is what it was built
for.

## Context

Shoal speaks no S3. The gateway that would is unscheduled
([wire and client](../object-storage/wire-and-client.md#alternatives-rejected),
[milestones](../object-storage/milestones.md#later-and-unscheduled)), so no off-the-shelf S3
benchmark can drive Shoal. Two drivers - warp for the S3 stores and `shoaladm bench` for Shoal -
would differ in the loop, the bytes and how a latency is taken, and a difference between two
stores could be either driver's. So the user chose one driver with two backends, the decision
recorded on the comparison's page.

Before F94, `shoal-loadgen`'s `ObjectDriver<S>` held `Shoal<S>` clients, and four functions -
`put`, `patch`, `read` and `check` - named the client's untyped `Objects`. Everything around
them was already free of the client:

- the windows, `ArmClock` and the capture;
- `compare` and its interval rule;
- the seeded generator of X13, `splitmix64-ctr/1`, whose bytes are made on the stream that sends
  them and checked by making them again.

## What it does

### The endpoint

`ObjectEndpoint` (`shoal-loadgen/src/objects/endpoint.rs`) is the five operations an object arm
uses:

- `put` of a whole object, taking `bytes::Bytes` so a body is handed over without a copy;
- `get` of a whole object;
- `stat`;
- `delete`, used only between arms;
- `write_at`, which an endpoint without writes in place refuses by name (`writes_in_place`).

Each returns `impl Future + Send` and fails with an `EndpointError { kind, code, message }`. The
kind is `NotFound`, `Corrupt`, `Retriable` or `Other`, and is how the driver tells a miss and a
misread from a failure. The code and message are what a window counts, so a Shoal capture counts
exactly the codes and messages it counted before.

**A Shoal member's client is an endpoint** (`objects/member.rs`, `impl ObjectEndpoint for
Shoal<S>`): the same call of `Objects` each, `ObjectOutcome::NotFound` a miss and
`ChecksumMismatch` a misread. **An S3 endpoint is one** behind shoal-loadgen's `s3` feature
(`objects/s3.rs`):

- one `reqwest` client an endpoint, HTTP/1.1 only, plaintext, never through a proxy;
- path-style requests, `PUT`, `GET`, `HEAD` and `DELETE` on `/{bucket}/{path}`;
- each signed by SigV4 with `UNSIGNED-PAYLOAD` (`objects/sigv4.rs`), so no body is hashed for a
  signature;
- `NoSuchKey` read as a miss, `NoSuchBucket` as a failure, and a 5xx or `SlowDown` as retriable;
- a bucket made with a `LocationConstraint` when one is named, which is how Ceph is given a
  placement other than its default. A bucket that exists is refused, since a run starts empty.

`objects/s3_local.rs` is a store of buckets in memory behind a local port. It checks every
signature by making it again from the request as it arrived, the client's `Host` header included.
The endpoint's tests and `bench s3`'s run against it.

### One loop over any endpoint

`ObjectDriver<E: ObjectEndpoint>` is the loop F80 built. It ties stream `i` to endpoint
`i mod n`, so the streams go round the endpoints, members or gateways, in the order given. The
timer starts before an object's bytes are made and stops at its answer, and a get's comparison
comes after the time is taken. A Shoal capture's arm is the same measurement it was;
`bench run` builds `ObjectDriver<Shoal<S>>`.

### Between arms: reset, or delete

A store the bench did not deploy cannot be wiped between arms. A spec's `between_arms` says what
an object arm that wrote is followed by:

- **`reset`**, the default and F66's: the bench's cluster wiped, bootstrapped and preloaded again.
- **`delete`**:
  1. a sample of what the arm put is read back, at most 256 objects or 1 GiB, spread evenly;
  2. everything it put is deleted, thirty-two at a time (`ObjectDriver::clean`);
  3. after a run that put anything, a settle command the operator names runs, which gives the
     store's space back - RGW's garbage collection, SeaweedFS's vacuum, ~~Shoal's reclamation
     grace~~ Shoal's reclamation, its devices' chunk bytes read until they hold steady since
     [Resolved #301](../appendix/resolved/a-settle-waits-for-reclamation.md);
  4. the whole preload is read back once at the end (`Capture::verify_end`).

`bench s3` always deletes. `bench run --between-arms delete` runs a Shoal cluster the same way,
which is what makes a Shoal capture's spec digest equal another store's. Puts keep making
objects past the last one put, so no path is put again after its delete.

Around every run, in both commands (`bench/between.rs`):

- `--drop-caches` drops every host's page cache before the run;
- `--max-used` refuses an arm on a root whose filesystem is fuller than its share.

`--only` keeps the object arms whose ids start with a prefix, such as `stat100/s4k/`.

### `shoaladm bench s3`

Behind shoaladm's `s3` feature, off by default, so no schema's admin program compiles an HTTP
client. It runs in shoaladm itself, with no schema, and prints lines rather than drawing a
screen. It takes:

- the spec flags `bench run` takes, which it lays over a spec as `bench run` does
  (`s3_and_run_make_one_spec_digest` holds the two to one digest);
- `--endpoints`, `--bucket <dataset bucket>=<store bucket>`, `--location-constraint`;
- `--system`, `--version`, `--redundancy` and `--note`, recorded in `Provenance::store`;
- `--host`, `--device host:path` and `--process name`, for the counters;
- `--settle` and `--max-used`.

The key comes from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, and is in no spec, capture or
log. Preparing a host - a governor set, a unit stopped - is the lab script's, as it is for every
store.

### What a host and a store cost

The ssh read F71 makes of `/proc/diskstats` before and after each run now also prints:

- the `cpu` line of `/proc/stat`;
- `/proc/net/dev`, and which interfaces have a device under them;
- `MemAvailable` and `Cached`;
- `df` of each root;
- for each named program, every process whose command line's first word ends in that name, with
  its `/proc/<pid>/stat` ticks and its `VmRSS` and `VmHWM`.

A run's `HostDevices` gains `cpu`, `interfaces`, `memory`, `processes` and `usage`. A root that is
a block device resolves to that device, where before it resolved to the devtmpfs `/dev` is. Only a
physical interface and the loopback are counted, since a bridge carries what the device under it
carries: europa's `br0` over `enp4s0` counted every byte twice in the first lab run.

`compare` gains three metrics: `store cpu s / GiB` (moved both ways), `wire bytes / moved byte`
and `store peak resident MiB`. It also gains a fact, `system`: `shoal` for every capture of a
Shoal cluster, those from before F94 included, so a capture of another store is compared with a
Shoal one only with `--allow system` and the facts that differ.

### Slim captures

`shoaladm bench slim <capture> --keep-series <prefixes> -o <file>` writes a capture again with
every run's per-second series dropped but the arms named. Each run's byte totals are kept
(`RunResult::totals`), and every number `compare` reads is unchanged
(`slim_keeps_every_compared_number`). A full capture of the comparison is about 9 MB, too large
to commit; a slim one is what the comparison's charts are drawn from.

### The comparison's charts

`shoal-bench render-stores` reads the comparison's slim captures under
`shoal-spike/results/stores/<leg>/`, one a store, a leg and a redundancy group. It writes a fixed
set of fragments under `docs/src/object-storage/store-comparison/`, which the comparison's page
includes: a glance and a detail of each leg and group, and every capture's facts. Each is drawn
in the book's themed inline SVG (`render/chart`), through mirror structs of the capture, since the
runner half of shoal-bench names no loadgen type. It draws:

- a ratio chart, `render/chart/ratio.rs`: every arm a row, every other store a dot at its median
  over Shoal's, and a whisker that crosses 1× exactly when the verdict is within;
- size sweeps, and every cost: bars and sweeps given a colour a store (`draw_coloured`), so a store
  is one colour on every chart;
- verdict tables, by the lab's interval rule, with Shoal's interval the union of its first and its
  drift capture. A run that failed an operation or put an object twice measured something else: it
  is left out of its arm's median and interval, and its store's cell says how many were.

Once Shoal is measured again alone, its first run's captures are kept as
`shoal-first-<group>[-drift].json` and drawn as *Shoal, first run*: tabled and drawn beside today's
Shoal with no verdict, and set against today's Shoal on the ratio chart, where left of 1× is an arm
Shoal got faster at. Nothing is judged against it, counted beside it, or mixed into today's
interval.

It refuses two captures of a leg that asked for other arms. `--check`, and a test, refuse a page
whose `{{#include}}`s are not exactly the set, since mdbook renders an include of a missing file as
nothing.

## Design choices

**One trait at the four call sites, not a second driver.** The loop, the namespaces of paths, the
preload, the windows and the read back are each written once. A second driver for S3 would have
had to be kept the same as this one by hand, which is the property the comparison depends on.

**The trait is implemented on `Shoal<S>` itself**, so the driver holds the
`Vec<Arc<Shoal<S>>>` it held before, and no Shoal call moved out of its timed span.

**SigV4 written here, at the versions the lockfile resolves.** `reqwest` 0.12.24 with no default
feature, `hmac` 0.12 and `sha2` 0.10 were all resolved already, through opentelemetry-otlp and
shoal-proto, so the lockfile still holds 770 packages. Only `shoal-loadgen`'s dependency list
grew. The signer is checked against AWS's documented GET Object example and four requests shaped
as the bench sends them, each signed by botocore 1.40.72's `S3SigV4Auth`
(`shoal-spike/results/stores-sigv4-vectors.py`). botocore reproduces the documented example's
signature, which is what makes it the reference for the other four.

**`UNSIGNED-PAYLOAD`.** A signed payload would hash every byte of a put on the driver's core, and
the timer would count it. A Shoal client's CRC-64/NVME is counted, since it is part of what Shoal
is; SHA-256 for a signature is not part of any store.

**Delete rather than reset, for every store in a comparison.** Resetting Shoal while deleting the
others would give Shoal a fresh store before every arm and no other store one. Every store
deletes, so each arm starts on a store that holds its preload and nothing else.

**A sample read back, not every object.** A 4 KiB put arm at sixteen in flight puts a hundred
thousand objects or more, and reading all of them back would take longer than the arm. The sample
proves the store kept what it acknowledged; the whole preload, read at the end, proves it still
holds what every get read.

**The high water mark, not a sampler.** `VmHWM` is the most a process ever had resident, read in
the same two ssh reads, where a sampler would be a third ssh session every second of every arm.

## Alternatives rejected

**warp for the S3 stores.** The stores' own benchmark tool, which the comparison runs beside the
driver as a check. As the driver of the comparison it would be a second loop with a latency taken
its own way, and nothing in it drives Shoal.

**An S3 gateway in front of Shoal**, so one S3 tool drives all five. A gateway needs a listing
Shoal does not have. It would also measure Shoal plus a gateway hop rather than Shoal's own
client, whose routing by topology is part of what Shoal is.

**`aws-sdk-s3` or `rusty-s3`.** The SDK adds about a hundred packages to the lockfile and retries
on its own. `rusty-s3` adds packages for what is a page of code here.

**A per-second RSS sampler over ssh**, as the plan first had it. It is a third ssh session every
second of every arm, for a peak that `VmHWM` already holds.

## Limitations

- **A Shoal client routes by topology; an S3 client reaches a gateway.** The comparison is of
  each store as its clients reach it: a Shoal put goes to its group's leader, an S3 put to the
  endpoint its stream is tied to, which forwards it.
- **A Shoal client checks a CRC-64/NVME of every read** and an S3 client checks nothing but the
  bytes. The driver compares every get with its bytes made again either way.
- **No multipart upload, listing or ranged read.** A put is one `PutObject` of the whole object,
  up to the largest size measured, 16 MiB, and a get one whole `GetObject`.
- **`write_at` and the patch mixes are refused** for an S3 endpoint by name.
- **The high water mark is the process's since it started**, not the run's: the peak of a store
  that ran for several arms is its peak over all of them so far.
- **Retries are not made.** A 5xx counts where it happened, as a Shoal failure does. A Shoal
  client's own retries of a retriable refusal, inside its 30 s budget, are counted as latency.
- **The counters are of the hosts named.** A store whose processes run elsewhere is not counted.
- ~~**A Shoal get and stat name no read level**, so each is the server's default, a strong read.
  An arm at `One` would need the endpoint to carry a level, which nothing passes it
  ([O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows)).~~
  Since [F95](bench-object-read-levels.md) an object arm's gets and stats are read at the spec's
  `read_level`, the server's default unless `--read-level` names one; an S3 endpoint refuses a
  level by name.

## Invariants to uphold

- **The time of an operation is the same span for every endpoint**: from before its bytes are
  made to its answer, a get's comparison after the time is taken. A body is handed to an endpoint
  as a second reference, so its buffer is freed once the time is taken.
- **The keys reach the signer and nothing else**: never a spec, a capture, a log or a field
  printed by `Debug`.
- **The path signed is the path sent, encoded once**, and the host signed is the `Host` the
  client sends.
- **New capture fields are defaulted and skipped when empty**, so the format stays at 1 and every
  capture from before reads (`devices_memory_and_the_paced_stream_round_trip`,
  `a_store_capture_needs_system_waived`).
- **`between_arms`, `object_only` and `drop_caches` are left out of a spec at their defaults**,
  so every spec from before F94 digests as it did.
- **Every script sent a host stays shell neutral**: the extended device read runs under sh, bash,
  dash and zsh in `the_script_runs_under_every_login_shell`.
- **`shoal-loadgen` and `shoaladm` still link no engine with `s3` on**:
  `cargo tree -p shoal-loadgen --features s3 | grep -c glommio` is 0.

## Performance

Nothing on a node's path changed. A run adds what F71 added: two ssh reads a host, outside the
arm's clock, which now print a few more lines. The Shoal arm's measurement is the same span
through the same client call.

**Before and after, on europa's loopback** (2026-10-19). Shoal's object arms through the driver as it
was at `e440bb07`, the commit before this feature, and through the endpoint trait at `32bb0ad9`, the
commit every comparison capture ran. No engine, client or schema source differs between the two, so
both ran one node binary (`7fa2b71d7213`) on the store comparison's loop inventory, three copies,
each side's loader built for `znver1` from a clean worktree at its commit. Four rounds of one run an
arm, the first side alternating by round, the three loop devices made fresh and every cache dropped
before each run, the driver on cpus 13-15 and 29-31, governor `performance`; `put100` and `get100`
at 1 MiB, sixteen in flight, 30 s after 5 s of warmup, 1,024 objects preloaded (`--preload
0.006103515625%` on both sides, since before this feature a count meant half the description,
[Resolved #276](../appendix/resolved/object-preload-and-wrap.md)):

| Arm | Before, ops/s | After, ops/s | p99 before | p99 after |
| --- | --- | --- | --- | --- |
| `put100/s1m/c16` | 146.2 [145.8–146.4] | 145.9 [145.6–146.4] | 141.3 to 143.7 ms | 140.7 to 142.0 ms |
| `get100/s1m/c16` | 2,372 [2,328–2,387] | 2,343 [2,318–2,436] | 10.3 to 10.7 ms | 10.2 to 10.6 ms |

Every interval overlaps the other side's, so by the lab's rule the trait changed neither rate. The
captures are under `target/lab/f94ab/captures/`, not committed, as a lab A/B is not a capture.

The first captures, a quick pass on the lab's SSD devices (2026-10-17, `--allow-dirty`, not a
result), ran every store end to end through the same driver:
- MinIO at EC:1 put 64 KiB objects at 384/s, with p50 10 ms, through `bench s3`.
- A three-copy Shoal pool put the same at 67/s, writing 197 device bytes for each byte sent:
  [O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)'s
  whole 4 MiB chunk a stripe on each of its three holders.

## Tests

| Test | Where | What breaks if reverted |
| --- | --- | --- |
| `a_memory_endpoint_drives_every_mix` | `shoal-loadgen/src/objects.rs` | The driver no longer runs over an endpoint that is not a Shoal client, or its streams no longer go round its endpoints |
| `not_found_misses_and_corrupt_differs` | `shoal-loadgen/src/objects.rs` | An endpoint's `NotFound` is not a miss and a lost object, or other bytes are not a misread |
| `cleanup_deletes_only_what_arms_put` | `shoal-loadgen/src/objects.rs` | A cleanup deletes the preload, leaves an arm's objects, reads back past its bounds, or a later put reuses a deleted path |
| `a_wrapping_put_is_marked` | `shoal-loadgen/src/objects.rs` | A put past the description's last object goes unmarked (item 277) |
| `patch_is_refused_without_writes_in_place` | `shoal-loadgen/src/objects.rs` | A patch arm reaches an S3 endpoint, or a count of objects to preload is read as half of them (item 276) |
| `the_aws_get_object_example_signs` | `shoal-loadgen/src/objects/sigv4.rs` | The signer stops reproducing AWS's documented signature |
| `bench_requests_sign_as_botocore_signs_them` | `shoal-loadgen/src/objects/sigv4.rs` | A request the bench sends is signed otherwise than AWS's own signer signs it |
| `the_signed_host_is_what_hyper_sends` | `shoal-loadgen/src/objects/sigv4.rs` | A path is encoded twice or not at all, or a default port is signed |
| `a_secret_is_never_printed` | `shoal-loadgen/src/objects/sigv4.rs` | A key's secret reaches `Debug` |
| `amz_date_is_utc` | `shoal-loadgen/src/clock.rs` | SigV4's date or time is not UTC as AWS writes it |
| `a_round_trip_through_a_local_server` | `shoal-loadgen/src/objects/s3.rs` | A put, get, head or delete does not round trip, a signature does not verify against the Host sent, or a path is not path style |
| `an_existing_bucket_is_refused` | `shoal-loadgen/src/objects/s3.rs` | A run starts in a bucket that holds something |
| `error_codes_are_classified` | `shoal-loadgen/src/objects/s3.rs` | A status or S3 code is read as the wrong kind, or a refused connection is not `Connect` |
| `descriptions_open_without_a_schema` | `shoal-loadgen/src/dataset.rs` | A folder of descriptions is read otherwise without a schema than with one |
| `a_store_capture_needs_system_waived` | `shoal-loadgen/src/compare.rs` | Another store's capture compares with Shoal's unwaived, or a capture from before F94 is not `shoal` |
| `store_costs_are_compared` | `shoal-loadgen/src/compare.rs` | A store's cpu a GiB, wire bytes a byte or peak memory are not compared, or the loopback counts as the wire |
| `host_counters_parse_and_subtract` | `shoaladm/src/bench/devices.rs` | The cpu, interfaces, memory, processes or usage of two reads are misread, a bridge is counted, or a full root does not refuse an arm |
| `a_block_device_root_resolves_to_itself` | `shoaladm/src/bench/devices.rs` | A raw volume resolves to `/dev`'s devtmpfs |
| `processes_match_by_argv0` | `shoaladm/src/bench/devices.rs` | A program whose name is past the sixteen bytes `comm` keeps is not found |
| `a_settle_is_timed_and_its_status_kept` | `shoaladm/src/bench/between.rs` | A settle command's time or status is not kept |
| `s3_and_run_make_one_spec_digest` | `shoaladm/src/bench/s3.rs` | `bench s3` and `bench run` given the same flags make two specs |
| `patch_and_bad_names_are_refused_by_name` | `shoaladm/src/bench/s3.rs` | A patch mix, a missing endpoint, a store named in two words, or a device on no host is accepted |
| `a_run_against_local_servers_writes_a_capture` | `shoaladm/src/bench/s3.rs` | `bench s3` against a store does not make its bucket, preload, run, clean up and read back, or writes a capture with a key in it |
| `keys_never_reach_the_capture` | `shoaladm/src/bench/s3.rs` | A key reaches a spec, or a device is labelled otherwise than a pool device |
| `slim_keeps_every_compared_number` | `shoaladm/src/bench/store.rs` | A slim capture keeps a series it was told to drop, or `compare` reads another number from it |
| `every_fragment_renders_with_no_captures` | `shoal-bench/src/render/stores.rs` | A fragment of the set is missing, or one with nothing behind it does not say so |
| `rendering_is_deterministic_and_themed` | `shoal-bench/src/render/stores.rs` | Two renders of the same captures differ, a verdict is misjudged, or a colour is drawn past the palette |
| `differing_specs_refuse_to_render` | `shoal-bench/src/render/stores.rs` | Two captures of a leg that asked for other arms are drawn side by side |
| `includes_are_read_off_the_page` | `shoal-bench/src/render/stores.rs` | The page's includes are misread, so `--check` holds them to the wrong set |
| `verdicts_follow_the_interval_rule` | `shoal-bench/src/render/stores/verdict.rs` | A verdict is drawn from overlapping intervals, a latency is judged better higher, or a gap is rounded up or written as a share on one side and a factor on the other |
| `a_store_run_as_context_is_never_judged` | `shoal-bench/src/render/stores.rs` | SeaweedFS's unsynced default, run beside its compared setting, is given a verdict, a row of counts or a whisker |
| `shoals_first_run_is_the_record_and_never_judged` | `shoal-bench/src/render/stores.rs` | Shoal's first run is given a verdict or counted, mixed into today's interval, or left off the ratio chart |
| `a_run_left_out_says_so` | `shoal-bench/src/render/stores.rs` | A run that failed an operation is counted in its arm's median or interval, or left out with nothing in its cell saying so |
| `the_page_includes_exactly_the_fragments` | `shoal-bench/tests/store_comparison.rs` | The page misses a fragment, which mdbook renders as nothing, or includes one the renderer never writes |
| `the_committed_fragments_are_current` | `shoal-bench/tests/store_comparison.rs` | A committed fragment is not what the committed captures draw |
| `the_verdict_is_loadgens_judge` | `shoal-bench/tests/store_comparison.rs` | The page judges a pair of captures otherwise than `shoaladm bench compare` does |
| `the_mirror_reads_what_loadgen_writes` | `shoal-bench/tests/store_comparison.rs` | The renderer's mirror of a capture misreads a field loadgen writes, or a slim capture's totals |
| `it_names_every_row_and_series` | `shoal-bench/src/render/chart/ratio.rs` | A row or a series of the ratio chart is unnamed, or drawn as NaN or past the palette |

## Related

[Shoal beside other object stores](../object-storage/store-comparison.md), what this was built
for; [F66](dataset-benchmarks.md), the bench it extends; [F80](object-wire.md), whose object arms
it puts behind an endpoint; [F71](bench-device-memory.md), whose device read it extends;
[X13](../object-storage/benchmark-shape.md), the generator and the driver's ceiling;
[X14](../object-storage/ceph-and-s3-sources.md), Ceph and S3 read at the source;
[Resolved #276, 277](../appendix/resolved/object-preload-and-wrap.md), the two defects of the
object driver it found; [items 278 and 279](../appendix/known-issues.md), what it found and left
open: an inventory whose pool the engine refuses deploys, and shoal-bench's runner half does not
build without the engine.
