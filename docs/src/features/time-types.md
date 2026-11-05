# F131. Times a row can hold

## Context

A row is archived with rkyv 0.8, and nothing chrono defines can be. chrono 0.4.45, its newest
release and the one in the lockfile, implements rkyv's traits for rkyv 0.7.43 alone, behind
features named `rkyv-16` to `rkyv-64`. rkyv 0.8 implements its own traits for a list of crates
(`uuid` among them) that chrono is not on. Because of the orphan rule, neither Shoal nor a schema
may implement a foreign trait for a foreign type, so a field of `DateTime<Utc>` cannot be in a row
at all, whatever role it plays.

That met the scans plan's optional row "sort keys of any `Ord` type"
([W1](../future-work/scans/prerequisites.md#optional)). The row's point was "an ordered read inside
one partition by an integer or a time without zero-padding", and its other half,
[Resolved #42, #207](../appendix/resolved/sort-keys-of-any-type.md), made a sort key any type whose
archived form orders as it does. A time was the one kind of key that half could not admit by an
impl, since its type is chrono's.

## What it does

`shoal::time` holds four types, each wrapping a chrono type, behind the `chrono` feature of
`shoal` (which forwards to `shoal-proto`'s), off by default:

| Type | Wraps | Archived as | A SHQL literal |
| --- | --- | --- | --- |
| `UtcDateTime` | `DateTime<Utc>` | `secs: i64` since the Unix epoch, then `nanos: u32` | `'2026-11-05T09:30:00Z'`, any RFC 3339 offset read as UTC |
| `NaiveDateTime` | `chrono::NaiveDateTime` | the same, read as if it were UTC | `'2026-11-05T09:30:00'` |
| `NaiveDate` | `chrono::NaiveDate` | `days: i32` since 0001-01-01, which is day one | `'2026-11-05'` |
| `NaiveTime` | `chrono::NaiveTime` | `secs: u32` since midnight, then `nanos: u32` | `'09:30:00'` |

Each one:

- **Holds its chrono value.** It is a public tuple struct over the chrono value, so it derefs to
  it, converts to and from it both ways (`From`, `into_inner`), and prints, compares and orders as
  chrono does.
- **Reads and writes through serde as chrono does** (`#[serde(transparent)]`). A dataset file's
  times and SHQL's literals are chrono's strings, with no parser change.
- **Archives as its *parts***, a small public struct of the integers above (`UtcDateTimeParts`, and
  so on). rkyv derives that struct's archive, named for the time type (`ArchivedUtcDateTime`),
  with an `Ord` field by field, which is chrono's order.
- **Is a `SortKey`.** It can be a sort key, alone or in a composite key. It can also be a partition
  key, a filter, an update or a plain field, since a filter compares an archived row's time against
  a value (`PartialEq<UtcDateTime> for ArchivedUtcDateTime`), and a row holding one derives
  `DeepSizeOf` (the types own their impl, of no heap).

```rust
use shoal::time::{NaiveDate, UtcDateTime};

#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Sensors")]
pub struct Reading {
    #[shoal(partition)]
    pub sensor: u64,
    #[shoal(sort)]
    pub at: UtcDateTime,
    #[shoal(filter)]
    pub day: NaiveDate,
}

// SELECT * FROM Reading WHERE sensor = 3 AND at >= '2026-11-05T00:00:00Z' AND day = '2026-11-05'
let start: UtcDateTime = taken_at.into(); // a DateTime<Utc> the writer holds
ReadingGet::new(vec![3]).sort_range(SortRange::starting_at(start));
```

`shoal::time::chrono` re-exports the chrono these were built against, as `shoal::uuid` does uuid's.

## Design choices

**Parts in chrono's order.** An instant's seconds then its nanoseconds is lexicographic in the same
order chrono gives instants, *leap seconds included*. chrono holds 23:59:60.5 as the second
23:59:59 with 1.5 billion nanoseconds, so its parts sort after every other nanosecond of 23:59:59
and before 00:00:00, as chrono's `Ord` does. A date is its day count and a time of day its seconds
then nanoseconds, each chrono's own representation of the order. `check_sort_key_order` holds all
four to it over chrono's least and greatest values, the epoch, a leap second and two thousand seeded
instants.

**Decoding refuses what chrono cannot hold, by error.** Validating an archived time checks its
bytes, and every bit pattern of two integers is valid bytes. Parts that name no time, such as two
billion nanoseconds, a leap second's nanoseconds at a second that ends no minute, or a day past
chrono's last, are refused where they are decoded, through chrono's `_opt` constructors, as
`UnrepresentableTime`. A client's crafted bundle therefore fails its own query. A decode that
panicked would have ended the shard. Comparisons of archived times stay total over any bits, so a
seek through crafted keys is merely a miss.

**A hash of the parts, not chrono's.** A time that is a partition key places its row by its hash,
which is persisted. chrono's derived `Hash` hashes how chrono happens to hold a value, which a
chrono release may change. These hash their parts, field by field, so the integers decide the
placement. `a_date_key_hashes_to_its_parts_and_frozen_values` freezes four dates' hashes and holds
them to a definition written without the derive.

**Behind a feature, off by default.** No client-only graph held chrono before this (`shoal-client`,
`shoal-loadgen`, `shoaladm`, `shoal-client-check`). An engine's graph did, through openraft's
`chrono-tz`, so the dependency adds no package to the lockfile. It is built with `std` and `serde`
alone: no `clock`, since a time a row holds is made by its writer, and none of chrono's rkyv
features, which are 0.7's. The `shoal::time` module exists either way and is empty without the
feature, so the facade re-exports it without a `cfg` of its own. `shoal`'s tests take the feature
through a dev-dependency, and `shoal-client-check` takes it to prove a time key in a client-only
build, which is also what has a `--workspace` run compile the time types into `shoal`.

## Alternatives rejected

- **`#[rkyv(with = shoal::time::AsTimestamp)]` on a chrono field, and `#[shoal(sort(key = ..))]`
  naming the key type.** The row would hold chrono's own type. But a sort key's type is archived on
  its own, in a `SortSelect` and a partition's tree, so it would still need a type of Shoal's. A
  filter or an update of the field would not compile at all, since the derives emit its type
  bare. And every chrono field would need two attributes to be right.
- **An attribute macro that rewrites a row's chrono fields.** It could add the `with` attribute
  where it sees `DateTime`, by name. It would be a new kind of macro on every row, guessing at types
  by their spelling, for what a type does with no guessing.
- **A fork of chrono, or a patch of it with rkyv 0.8 impls.** A `[patch]` reaches this workspace and
  no schema built outside it, and shoaladm builds schemas outside it.
- **One `i64` of nanoseconds.** Half the width of the instant's parts, but it covers only the years
  1677 to 2262, and a leap second's 1.5 billion nanoseconds would collide with the next second's
  half billion.
- **Admitting `DateTime<FixedOffset>`.** chrono orders and compares it by its instant alone, so two
  values with one instant and two offsets are equal. A partition's tree would keep one of them, and
  the offset would be neither ordered nor round tripped. Converting to UTC first says so.

## Limitations

- **A row holds Shoal's type, not chrono's.** `row.at` is a `UtcDateTime`, which derefs to the
  `DateTime<Utc>` inside it, and a row's archived field is an `ArchivedUtcDateTime` of parts.
  Reading it as chrono's means decoding it (`UtcDateTime::from_parts`).
- **No time with an offset, and no `Local`.**
- **No archived mirror.** A row written back out of its archive materializes a time field and
  serializes it again (`FieldShape::Opaque`), as it does a `Uuid`: a copy of integers, no
  allocation. `#[shoal(rearchive)]` cannot opt one in, since no `Rearchive` impl exists for them.
- **SHQL cannot write a nanosecond past a leap second's in a literal chrono would refuse**, nor
  anything chrono's parser refuses. The literal's grammar is chrono's.
- **A row holding a `Uuid` still writes its own `DeepSizeOf`**, as `ByUuid` does, since deepsize2 has
  no `uuid` impl. Rows sized without deepsize2 are a todo.

## Invariants to uphold

- **The parts order as chrono orders the value.** Every archived seek of a time key depends on it,
  and a change to which integers a type archives as is a change to its archive format. Rows written
  before it would decode as other times.
- **A time hashes as its parts, in declaration order.** A partition key's placement is persisted, and
  the four frozen literals are the format.
- **Decoding never panics.** An archived time may come from a client. Refuse through
  `UnrepresentableTime`.
- **`shoal::time` exists without the feature.** The facade re-exports it unconditionally, so gating
  the module itself would break every build without `chrono`.

## Performance

No figure is claimed and no capture was taken: no workload uses a time. What can be said from the
layout:

- **An instant archives as an `i64` and a `u32`**, read in place. A padded string of the same instant
  is an archived string's inline header and some twenty to thirty bytes beside it.
- **A seek compares two integers** where a string key compares bytes.
- **A decode is chrono's constructor**, a few integer divisions.

[O156](../appendix/optimizations.md#o156-an-integer-sort-key-priced-beside-a-padded-string) prices
an integer key beside the bench's padded string. That would be the benchmark to read a time key's
gain against too.

## Tests

| Test | Fails without |
| --- | --- |
| `shoal-proto` `time::types::tests::every_time_type_orders_as_it_archives` | The parts' order: chrono's least and greatest, the epoch, a leap second, and two thousand seeded instants, for all four types |
| `shoal-proto` `time::types::tests::every_time_type_round_trips` | The archive and serde: each type comes back as itself, and an RFC 3339 literal with an offset is read as UTC |
| `shoal-proto` `time::types::tests::parts_chrono_cannot_hold_fail_to_decode` | The fallible decode: two billion nanoseconds, a leap second's nanoseconds at a second ending no minute, and seconds and days past chrono's range validate, and are refused by error, not a panic |
| `shoal-proto` `time::types::tests::an_archived_time_equals_the_time_it_archived` | `PartialEq` against the native type, which an archived filter compares by |
| `shoal-proto` `time::types::tests::a_times_hash_is_its_parts` | A hash of the parts rather than of chrono's internals |
| `shoal` `sort_key_times::readings_answer_in_time_order` | A `UtcDateTime` sort key, a `NaiveDate` filter and a `NaiveDateTime` update, in memory, from an archive, and over a restart |
| `shoal` `sort_key_times::shql_names_times_by_their_strings` | A `NaiveDate` partition key and a `NaiveTime` sort key named in SHQL by chrono's strings, and a range bounded by RFC 3339 strings, one with an offset |
| `shoal` `sort_key_times::a_date_key_hashes_to_its_parts_and_frozen_values` | A date's partition hash: the four frozen values, and the definition beside them |
| `shoal-client-check` `client_half::shql_names_a_composite_sort_key_of_a_time` | A composite key of a `UtcDateTime` and a number, named by SHQL in a build with no engine |

## Related

- [Resolved #42, #207](../appendix/resolved/sort-keys-of-any-type.md): `SortKey`, which these
  implement, and the composite keys SHQL names
- [F1. Sort key ranges](sort-key-ranges.md), the ranges and cursors a time key pages by
- [Derive Macros](../api/derive-macros.md#sort-key-types) and [SHQL](../api/shql.md)
- [W1](../future-work/scans/prerequisites.md#optional), whose optional row this closes with the
  resolved items
