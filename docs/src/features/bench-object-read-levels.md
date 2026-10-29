# F95. Object reads at a level in the bench

Since this feature an object arm's gets and stats are read at the spec's `read_level`, which
`--read-level default|one|quorum` sets. That is the level a table arm's reads already took. A
capture of a Shoal cluster can now measure what a strong read costs an object read: `get100` and
`stat100` at `One` beside the same arms at `Quorum`.
[Every get and stat is a strong read](../object-storage/store-optimizations/strong-reads.md) and
[Small reads have a tail across hosts](../object-storage/store-optimizations/small-read-tail.md)
both start from that measurement.

## Context

[The store comparison](../object-storage/store-comparison.md) read every Shoal object at the
server's default, a strong read, because the driver named no level
([F94's limitations](bench-object-endpoints.md#limitations)). Two of the optimizations it filed
turn on what that costs:
- [O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows):
  a striped get makes two strong reads, one after the other.
- [O122](../appendix/optimizations.md#o122-a-small-reads-p99-across-hosts-is-six-times-its-mean-and-nobody-knows-why):
  a small read's p99 across hosts is six times its mean.

Each needs the same arm at `One`, which takes no round to a quorum, before anything about the
round can be said.

Everything else was already in place:
- **The spec** had `read_level` since [F66](dataset-benchmarks.md), and a table arm's reads were
  sent at it (`send_options`).
- **The wire** has carried an object read's level since [F80](object-wire.md): byte 25 of the
  operation's head, `None`, `One` or `Quorum`, and a stat may carry one.
- **The client** routes an object read at `One` to a holder of its path's row rather than to the
  row's leader ([F74](client-routing.md)).

Only the driver passed no level, and the client's `stat` took none.

## What it does

- **The driver reads at the spec's level.** `ObjectDriver::reading_at(level)`
  (`shoal-loadgen/src/objects.rs`) sets the level every arm's gets and stats are read at, and
  `shoaladm bench run` passes the spec's (`shoaladm/src/bench/orchestrate.rs`).
- **An endpoint is handed the level.** `ObjectEndpoint::get` and `stat`
  (`shoal-loadgen/src/objects/endpoint.rs`) take it as an argument.
  - **A Shoal member** reads with `Objects::read_range(path, 0, u64::MAX, level)` and
    `Objects::stat_at(path, level)` (`objects/member.rs`).
  - **At `One`** the client sends the operation to a holder of its path's row, and the server
    reads the entry there without a round to a quorum.
- **The read back is always at the default.** An acknowledged object is judged by what the store
  committed, so `check` reads at the default whatever the arms read at.
- **An S3 endpoint refuses any level but the default**, by name and before anything is sent
  (`EndpointError::no_level`). `shoaladm bench s3` refuses a spec that names one.
- **The client gains `Objects::stat_at(path, level)`.** `stat(path)` is now `stat_at(path, None)`.

## Design choices

- **The spec's existing `read_level`, not a field of its own.** Every committed capture of the
  comparison, 29 under `shoal-spike/results/stores/`, carries `"read_level":"default"`. Reusing
  the field moves no digest, and one flag reads every read of a run at one level, tables and
  objects alike.
- **The default is unchanged.** At `default` an object read is the server's default, `Quorum`,
  exactly what every capture before this feature measured.
- **The read back stays at the default.** A read at `One` may be served by a holder that has not
  yet applied a write its quorum committed, and a read back must never call an object lost when
  it is only late.
- **The level is an argument of the trait, not state an endpoint keeps.** Each call says what it
  asks for. An endpoint that cannot ask for it refuses by name, and the test endpoint records it.

## Alternatives rejected

- **A field of its own for objects** (`object_read_level`, skipped at its default as F94's fields
  are). It would let tables and objects read at different levels in one run, which nothing has
  asked for. It would also put a second key for one question into every spec that names it.
- **The level in the arm's id** (`get100/s4k/c16/one/none`). An arm's id is the key `compare`
  joins on, and a level is a whole run's setting, as routing is. Two captures at two levels
  already differ by their spec's digest, which `compare` reports.
- **An S3 endpoint that ignores a level.** Its capture would claim `One` for a store that read at
  its own default.

## Limitations

- **A striped get at `One` still makes one strong read.** It reads its stripe rows at `Quorum`
  whatever it asked (O118). For an object past the inline threshold, `One` takes away the entry's
  round alone.
- **`compare` between a `One` capture and a `Quorum` one** reports that their specs differ. Waive
  `spec` to read them side by side.
- **The level is a run's, not an arm's.** Reading the same arms at two levels takes two runs.
- **A read at `One` is answered by the node its stream's client was made for.** `bench run` makes
  a client a node and deals its streams round them, europa's first on the lab, and a read at `One`
  goes to its client's own node when that node holds a copy ([F74](client-routing.md)). With three
  copies on three nodes every node does, so at one in flight a read at `One` never leaves the
  driver's host. A read at `Quorum` goes to its leader, across the link for about half of them.
  The difference between the two levels is the round and the link together;
  [One beside Quorum](../object-storage/store-optimizations/strong-reads.md#one-beside-quorum)
  separates them by each host's interface bytes.

## Invariants to uphold

- **A read back is always at the default level.**
- **A spec at the default level digests as it did before this feature.** No field was added.
- **An endpoint that cannot ask for a level refuses it by name.** It never measures another level
  under the name of the one asked for.

## Performance

Nothing on a node's path changed. The measurement this enables, the SSD leg's small reads at `One`
beside `Quorum` from one in flight to sixteen, is the first step of O118 and O122. ~~Its figures go
on those optimizations' pages.~~ It ran on 2026-10-20. At `One` 4 KiB gets and stats ran 4.0 to 6.9
times `Quorum`'s rate and 64 KiB gets 1.24 to 2.68 times, and the small reads' tail went. The
figures are on
[Every get and stat is a strong read](../object-storage/store-optimizations/strong-reads.md#one-beside-quorum)
and [Small reads have a tail across hosts](../object-storage/store-optimizations/small-read-tail.md#at-one-and-by-depth).

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-loadgen` `objects::tests::arms_read_at_the_drivers_level_and_the_read_back_at_the_default` | An arm's gets and stats are asked at the driver's level, and the read back at the default |
| `shoal-loadgen --features s3` `objects::s3::tests::a_round_trip_through_a_local_server` | An S3 endpoint refuses a get at `One` and a stat at `Quorum` by name, and sends nothing for either |
| `shoaladm --features s3` `bench::s3::tests::a_read_level_is_refused_by_name` | `bench s3` refuses a spec at `One` or `Quorum` |
| `bench-dataset` `object_reads_at_one_against_one_node` | Gets and stats at `One` against a node in process are every one answered, none missed, and the read back finds every object |

## Related

- [F94. The bench drives another store's S3 endpoints](bench-object-endpoints.md), whose
  limitation this lifts.
- [F41. Read consistency](read-consistency.md), what `One` and `Quorum` are.
- [F74. Client routing](client-routing.md), where a read at `One` is sent.
- [F66. Dataset benchmarks](dataset-benchmarks.md), the spec.
- [Every get and stat is a strong read](../object-storage/store-optimizations/strong-reads.md) and
  [Small reads have a tail across hosts](../object-storage/store-optimizations/small-read-tail.md),
  which this measures for.
