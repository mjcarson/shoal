//! Stamping a capture with the time it was taken
//!
//! Timestamps are written in the same shape `scripts/collect-micro.sh` wrote them,
//! `date -u +%Y-%m-%dT%H:%M:%SZ`, so a capture taken by this tool and one taken by the script it
//! replaces are the same string format. That matters beyond tidiness: captures are ordered on the
//! generated page by their timestamps, and an RFC 3339 timestamp fixed to UTC with a whole number
//! of seconds sorts correctly as a plain string, which is how they are ordered.
//!
//! # Why this is hand rolled
//!
//! `chrono` is not in this workspace's lockfile. `time` is, but enabling the `formatting` feature
//! it would need pulls `time-macros` in behind it, which is not. This crate is meant to add zero
//! packages to the lockfile, and the whole of what is needed from a date library here is one
//! civil-date conversion - a well understood forty lines that is easier to test than to depend on.

use std::time::{SystemTime, UNIX_EPOCH};

/// The current time as an RFC 3339 timestamp in UTC, to the second
///
/// A clock set before the epoch would produce a timestamp that sorts before every capture ever
/// taken, so it is clamped rather than wrapped.
pub fn now_rfc3339() -> String {
    // read the wall clock, treating a pre-epoch clock as the epoch
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0);
    format_unix_utc(secs)
}

/// Formats Unix seconds as an RFC 3339 timestamp in UTC
///
/// # Arguments
///
/// * `secs` - Seconds since the Unix epoch
///
/// # Examples
///
/// ```
/// use shoal_bench::clock::format_unix_utc;
///
/// assert_eq!(format_unix_utc(0), "1970-01-01T00:00:00Z");
/// assert_eq!(format_unix_utc(1_000_000_000), "2001-09-09T01:46:40Z");
/// ```
pub fn format_unix_utc(secs: i64) -> String {
    // split the timestamp into whole days and the seconds within that day. euclidean division
    // rather than truncating division, so a negative timestamp lands on the day before rather
    // than counting backwards through it.
    let days = secs.div_euclid(86_400);
    let within = secs.rem_euclid(86_400);
    // unpack the time of day
    let hour = within / 3_600;
    let minute = (within % 3_600) / 60;
    let second = within % 60;
    // and the civil date those days land on
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Converts a count of days since the Unix epoch into a proleptic Gregorian civil date
///
/// This is Howard Hinnant's `civil_from_days`, which works by shifting the epoch to the start of
/// a four hundred year cycle beginning in March. Starting the year at March puts the leap day at
/// the end of it, so the month length pattern becomes regular and the whole conversion is
/// arithmetic with no table and no branch per month.
///
/// # Arguments
///
/// * `days` - Days since 1970-01-01, which may be negative
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // shift the epoch from 1970-01-01 to 0000-03-01, the start of a four hundred year era
    let shifted = days + 719_468;
    // which era that day falls in, rounding toward negative infinity for dates before the shift
    let era = if shifted >= 0 {
        shifted / 146_097
    } else {
        (shifted - 146_096) / 146_097
    };
    // the day within that era, always in [0, 146096]
    let day_of_era = shifted - era * 146_097;
    // the year within that era, in [0, 399]. the corrections drop the leap days that the century
    // rule takes back out.
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    // the day within that year, counted from March, in [0, 365]
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    // months measured from March are a repeating five month pattern, so the month index is exact
    let month_shifted = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_shifted + 2) / 5 + 1;
    // rotate March-based months back onto the calendar, and carry January and February into the
    // following year
    let month = if month_shifted < 10 {
        month_shifted + 3
    } else {
        month_shifted - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The epoch itself formats as the epoch
    #[test]
    fn the_epoch_round_trips() {
        assert_eq!(format_unix_utc(0), "1970-01-01T00:00:00Z");
    }

    /// A timestamp with a time of day carries every field through
    #[test]
    fn a_time_of_day_is_carried_through() {
        assert_eq!(format_unix_utc(1_000_000_000), "2001-09-09T01:46:40Z");
        assert_eq!(format_unix_utc(1_770_604_487), "2026-02-09T02:34:47Z");
    }

    /// A leap day is a real day, in both the four year and the four hundred year rules
    #[test]
    fn leap_days_land_where_they_should() {
        // an ordinary leap year
        assert_eq!(format_unix_utc(1_709_164_800), "2024-02-29T00:00:00Z");
        // a century that is a leap year because it divides by four hundred
        assert_eq!(format_unix_utc(951_782_400), "2000-02-29T00:00:00Z");
        // and one that is not, since 2100 does not
        assert_eq!(format_unix_utc(4_102_444_800), "2100-01-01T00:00:00Z");
    }

    /// A timestamp before the epoch lands on the day before rather than counting backwards
    #[test]
    fn a_pre_epoch_timestamp_lands_on_the_previous_day() {
        assert_eq!(format_unix_utc(-1), "1969-12-31T23:59:59Z");
        assert_eq!(format_unix_utc(-86_400), "1969-12-31T00:00:00Z");
    }

    /// Every day of a four hundred year cycle converts back to the day it came from
    ///
    /// The conversion is arithmetic with several truncating divisions in it, so the cheapest way
    /// to be sure it is right is to walk a whole cycle rather than to reason about the corrections.
    #[test]
    fn a_whole_gregorian_cycle_round_trips() {
        // walk every day of the cycle containing the epoch
        for day in -25_567..(146_097 - 25_567) {
            let (year, month, dom) = civil_from_days(day);
            // reconstruct the day count from the civil date and check it lands back where it began
            assert_eq!(
                days_from_civil(year, month, dom),
                day,
                "{year:04}-{month:02}-{dom:02} did not round trip"
            );
        }
    }

    /// The inverse conversion, used only to check the forward one
    ///
    /// # Arguments
    ///
    /// * `year` - The civil year
    /// * `month` - The civil month, from one to twelve
    /// * `day` - The day of the month, from one
    fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
        // shift the year so it begins in March, which puts the leap day at the end of it
        let year = year - i64::from(month <= 2);
        // which four hundred year era that year falls in
        let era = if year >= 0 { year / 400 } else { (year - 399) / 400 };
        let year_of_era = year - era * 400;
        // the day within the year, counted from March
        let month_shifted = if month > 2 { month - 3 } else { month + 9 };
        let day_of_year = (153 * month_shifted + 2) / 5 + day - 1;
        // the day within the era, then back onto the Unix epoch
        let day_of_era =
            year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        era * 146_097 + day_of_era - 719_468
    }
}
