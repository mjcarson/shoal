//! Integration tests for the times a row can hold, as sort keys, partition keys, filters and
//! updates ([F131](../../docs/src/features/time-types.md))
//!
//! chrono archives only with rkyv 0.7, so `shoal::time` wraps its types and archives each as
//! integers in chrono's order. These hold that order to every place a row's time is read by its
//! archived form: a sorted partition sought on disk by time, a partition placed by a date's hash,
//! a filter on a time compared against an archived row, and an update of one. The times are
//! chosen so that their order is not their strings': one before 1970, a leap second, and an
//! offset a literal names.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::shared::queries::SortRange;
use shoal::shared::traits::{PartitionKeySupport, RkyvSupport};
use shoal::storage::FileSystem;
use shoal::tables::PersistentSortedTable;
use shoal::time::chrono::{DateTime, Utc};
use shoal::time::{NaiveDate, NaiveDateTime, NaiveTime, UtcDateTime};
use shoal::ShoalPool;
use shoal_derive::{db, ShoalSortedTable};
use std::ops::Bound;
use std::time::Duration;
use tempfile::TempDir;

mod utils;

use utils::TestError;

/// A sensor's reading, ordered by when it was taken
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TimeDb")]
pub struct Reading {
    /// The sensor that took it
    #[shoal(partition)]
    pub sensor: u64,
    /// When it was taken, which orders a sensor's readings
    #[shoal(sort)]
    pub at: UtcDateTime,
    /// The day it was filed under, which a get can filter on
    #[shoal(filter)]
    pub day: NaiveDate,
    /// When it was last looked at, which an update rewrites
    #[shoal(update)]
    pub seen: NaiveDateTime,
}

/// A shift, filed under its day and ordered by when it starts
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TimeDb")]
pub struct Shift {
    /// The day of the shift, which places it
    #[shoal(partition)]
    pub day: NaiveDate,
    /// When it starts, which orders a day's shifts
    #[shoal(sort)]
    pub starts: NaiveTime,
    /// Who works it
    pub worker: String,
}

/// The schema of times
#[db]
pub struct TimeDb {
    /// Readings, by sensor, in the order they were taken
    pub readings: PersistentSortedTable<Reading, FileSystem>,
    /// Shifts, by day, in the order they start
    pub shifts: PersistentSortedTable<Shift, FileSystem>,
}

/// The sensor every reading is taken by
const SENSOR: u64 = 3;

/// A UTC time from its seconds and nanoseconds, which a test knows chrono holds
///
/// # Arguments
///
/// * `secs` - Seconds since the Unix epoch
/// * `nanos` - Nanoseconds past them
fn utc(secs: i64, nanos: u32) -> UtcDateTime {
    UtcDateTime(DateTime::<Utc>::from_timestamp(secs, nanos).expect("a time chrono holds"))
}

/// A date chrono holds
///
/// # Arguments
///
/// * `year` - The year
/// * `month` - The month
/// * `day` - The day of the month
fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate(
        shoal::time::chrono::NaiveDate::from_ymd_opt(year, month, day)
            .expect("a date chrono holds"),
    )
}

/// A time of day chrono holds
///
/// # Arguments
///
/// * `hour` - The hour
/// * `minute` - The minute
fn of_day(hour: u32, minute: u32) -> NaiveTime {
    NaiveTime(
        shoal::time::chrono::NaiveTime::from_hms_opt(hour, minute, 0).expect("a time chrono holds"),
    )
}

/// Five readings' times, ascending: one before 1970, the epoch, a leap second's last
/// nanosecond's neighbours, and one past them
fn reading_times() -> Vec<UtcDateTime> {
    // 2016-12-31T23:59:59 UTC, whose minute held the last leap second
    let leap_minute = 1_483_228_799;
    vec![
        utc(-86_400 * 365, 5),
        utc(0, 0),
        utc(leap_minute, 999_999_999),
        utc(leap_minute, 1_500_000_000),
        utc(leap_minute + 1, 0),
    ]
}

/// A reading taken at a time, filed under its own day and never yet seen
///
/// # Arguments
///
/// * `at` - When it was taken
fn reading(at: UtcDateTime) -> Reading {
    Reading {
        sensor: SENSOR,
        at,
        day: NaiveDate(at.date_naive()),
        seen: NaiveDateTime(utc(0, 0).naive_utc()),
    }
}

/// The readings a get answered with, in the order it answered
///
/// # Arguments
///
/// * `client` - The client to send the get with
/// * `get` - The get to send
async fn answered(
    client: &Shoal<TimeDbClient>,
    get: ReadingGet,
) -> Result<Vec<Reading>, TestError> {
    // send the get and drain every response it produced
    let mut stream = client.send(client.query().add(get)).await?;
    let mut rows = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<Reading>()? {
            for row in found.iter() {
                rows.push(<Reading as RkyvSupport>::deserialize(row).expect("a reading decodes"));
            }
        }
    }
    Ok(rows)
}

/// The times of the readings a get answered with, in the order it answered
///
/// # Arguments
///
/// * `rows` - The readings
fn times_of(rows: &[Reading]) -> Vec<UtcDateTime> {
    rows.iter().map(|row| row.at).collect()
}

/// Read a sensor's readings back every way a get can ask for them by time and by day
///
/// # Arguments
///
/// * `client` - The client to read with
async fn check_reading_reads(client: &Shoal<TimeDbClient>) -> Result<(), TestError> {
    let times = reading_times();
    // every reading in time order, the one before 1970 first and the leap second in its place
    let all = answered(client, ReadingGet::new(vec![SENSOR])).await?;
    assert_eq!(times_of(&all), times, "a get of every reading");
    // a range of time takes the readings between its bounds
    let range = SortRange::new(Bound::Excluded(times[1]), Bound::Included(times[3]));
    let ranged = answered(client, ReadingGet::new(vec![SENSOR]).sort_range(range)).await?;
    assert_eq!(times_of(&ranged), times[2..4].to_vec(), "a range of time");
    // a named time is found, and one never written is a miss
    let named = ReadingGet::new(vec![SENSOR]).sort_keys(vec![times[3], utc(7, 7)]);
    assert_eq!(
        times_of(&answered(client, named).await?),
        vec![times[3]],
        "a named time"
    );
    // and a filter on the day compares each row's day, archived or resident
    let mut filtered = ReadingGet::new(vec![SENSOR]);
    filtered.filters = Some(ReadingFilter {
        day: Some(vec![date(2016, 12, 31)]),
    });
    let filtered = answered(client, filtered).await?;
    assert_eq!(
        times_of(&filtered),
        times[2..4].to_vec(),
        "a filter on a day"
    );
    Ok(())
}

/// Readings answer by time and by day in memory and from their archive, an update of a time
/// field holds across a restart, and each answer is in time order
#[tokio::test]
async fn readings_answer_in_time_order() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    // write the readings, last first, so their order is the table's
    let (client, pool) = start_one_shard(&temp_dir).await?;
    for at in reading_times().into_iter().rev() {
        let answer = client.send_one(reading(at)).await?;
        assert!(answer.error().is_none(), "a reading at {at} failed");
    }
    // every read sees them in time order while they are resident
    check_reading_reads(&client).await?;
    // and again once they are read back from an archive, sought by their archived times
    archive(&temp_dir, client, pool).await?;
    let (client, pool) = start_one_shard(&temp_dir).await?;
    check_reading_reads(&client).await?;
    // an update finds an archived reading by its time and rewrites a time field
    let times = reading_times();
    let seen = NaiveDateTime(times[4].naive_utc());
    let update = ReadingUpdate {
        partition_key: SENSOR,
        sort_key: times[2],
        seen: Some(seen),
    };
    let answer = client.send_one(update).await?;
    assert!(answer.error().is_none(), "an update failed");
    pool.exit()?;
    drop(client);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // which holds across a restart
    let (client, pool) = start_one_shard(&temp_dir).await?;
    let updated = answered(
        &client,
        ReadingGet::new(vec![SENSOR]).sort_keys(vec![times[2]]),
    )
    .await?;
    assert_eq!(updated.len(), 1, "the updated reading is missing");
    assert_eq!(updated[0].seen, seen, "the update did not hold");
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// Shifts are placed by their day and ordered by when they start, and SHQL names both by the
/// strings chrono prints, a UTC time by an RFC 3339 string with any offset
#[tokio::test]
async fn shql_names_times_by_their_strings() -> Result<(), TestError> {
    let temp_dir = utils::test_dir();
    let (client, pool) = utils::start::<TimeDb>(&temp_dir).await?;
    // three shifts on one day and one on the next, written out of order
    for (day, hour, worker) in [(5, 17, "c"), (5, 9, "a"), (6, 9, "d"), (5, 13, "b")] {
        let shift = Shift {
            day: date(2026, 11, day),
            starts: of_day(hour, 0),
            worker: worker.to_string(),
        };
        client.send_one(shift).await?;
    }
    // a day's shifts from nine, in the order they start, the next day's in a partition apart
    let query = "SELECT * FROM Shift WHERE day = '2026-11-05' AND starts > '09:00:00'";
    let queries = match client.query().parse(query) {
        Ok(queries) => queries,
        Err(error) => panic!("a date and a time of day did not bind: {}", error.message),
    };
    let mut stream = client.send(queries).await?;
    let mut workers = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(found) = response.access::<Shift>()? {
            workers.extend(found.iter().map(|row| row.worker.to_string()));
        }
    }
    assert_eq!(workers, vec!["b", "c"]);
    // and readings bounded by an RFC 3339 string, one of them with an offset, read as UTC
    for at in reading_times() {
        client.send_one(reading(at)).await?;
    }
    let query = "SELECT * FROM Reading WHERE sensor = 3 AND at >= '1970-01-01T02:00:00+02:00' \
                 AND at < '2017-01-01T00:00:00Z' AND day = '2016-12-31'";
    let queries = match client.query().parse(query) {
        Ok(queries) => queries,
        Err(error) => panic!("an RFC 3339 range did not bind: {}", error.message),
    };
    let mut stream = client.send(queries).await?;
    let mut found = Vec::new();
    while let Some(response) = stream.next().await? {
        if let Some(rows) = response.access::<Reading>()? {
            found.extend(
                rows.iter()
                    .map(|row| <Reading as RkyvSupport>::deserialize(row).unwrap().at),
            );
        }
    }
    assert_eq!(found, reading_times()[2..4].to_vec());
    // Shutdown server
    pool.exit()?;
    Ok(())
}

/// A date that places a partition hashes as its parts, by a definition written without the
/// derive, and to the values it hashed to when this was frozen: a partition's placement is
/// persisted, so it is never chrono's internals that decide it
#[test]
fn a_date_key_hashes_to_its_parts_and_frozen_values() {
    use std::hash::{Hash, Hasher};
    // chrono's first and last days, the first day of the era, and the epoch's
    //
    // printed by the tree that introduced the type, since no earlier tree could hold one, and
    // held to the definition below, written without the derive
    let cases: &[(NaiveDate, u64)] = &[
        (
            NaiveDate(shoal::time::chrono::NaiveDate::MIN),
            0x4e06_ad00_e668_fd28,
        ),
        (date(1, 1, 1), 0xc35a_801b_b5f6_9d2a),
        (date(1970, 1, 1), 0x70a0_7630_0d1d_df02),
        (
            NaiveDate(shoal::time::chrono::NaiveDate::MAX),
            0x064d_a4c2_4b88_4d19,
        ),
    ];
    for (day, frozen) in cases {
        // the definition: one hasher fed the day count's own `Hash`
        let mut hasher = shoal::gxhash::GxHasher::default();
        day.parts().days.hash(&mut hasher);
        let defined = hasher.finish();
        // the key's hash is the definition's, and what it was frozen at
        let hashed = Shift::get_partition_key_from_values(day);
        assert_eq!(hashed, defined, "{day} hashed apart from its parts");
        assert_eq!(hashed, *frozen, "{day} hashed to {hashed:#x}");
    }
}

/// Start a single shard server over a test's storage
///
/// # Arguments
///
/// * `temp_dir` - The storage to start over
async fn start_one_shard(
    temp_dir: &TempDir,
) -> Result<(Shoal<TimeDbClient>, ShoalPool<TimeDb>), TestError> {
    utils::start_with_conf::<TimeDb>(utils::build_single_shard_config(temp_dir)).await
}

/// Stop a single shard server, then cycle its storage so what it wrote is compacted into an
/// archive and nothing of it is resident when the next server starts
///
/// # Arguments
///
/// * `temp_dir` - The storage the server wrote into
/// * `client` - The server's client
/// * `pool` - The server
async fn archive(
    temp_dir: &TempDir,
    client: Shoal<TimeDbClient>,
    pool: ShoalPool<TimeDb>,
) -> Result<(), TestError> {
    // stop the server that wrote the rows
    pool.exit()?;
    drop(client);
    // wait for threads to fully clean up and the port to be released
    tokio::time::sleep(Duration::from_secs(1)).await;
    // cycle once more so these inserts have been compacted into an archive
    let (client, pool) = start_one_shard(temp_dir).await?;
    pool.exit()?;
    drop(client);
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}
