//! A time as the bench writes it: RFC 3339 in a capture, and SigV4's two forms for an S3 request
//! ([F94](../../docs/src/features/bench-object-endpoints.md))
//!
//! Kept free of any date crate, as it was in shoaladm before F94 moved it here so the S3 signer
//! could share it: every time is UTC, to the second, from a day count by Howard Hinnant's algorithm.

use std::time::{SystemTime, UNIX_EPOCH};

/// The civil date of a day count since 1970, by Howard Hinnant's algorithm
///
/// # Arguments
///
/// * `days` - Days since 1970-01-01
#[must_use]
pub fn civil(days: i64) -> (i64, u32, u32) {
    // shift the epoch to 0000-03-01, so a leap day ends a year
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// A time's date and the seconds into its day, in UTC
///
/// # Arguments
///
/// * `time` - The time
fn split(time: SystemTime) -> ((i64, u32, u32), i64) {
    // seconds since the epoch, split into the day and the time in it
    let secs = time.duration_since(UNIX_EPOCH).map_or(0, |since| since.as_secs()) as i64;
    (civil(secs.div_euclid(86_400)), secs.rem_euclid(86_400))
}

/// A time as RFC 3339, in UTC, to the second
///
/// # Arguments
///
/// * `time` - The time
#[must_use]
pub fn rfc3339(time: SystemTime) -> String {
    let ((year, month, day), of_day) = split(time);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        of_day / 3600,
        (of_day % 3600) / 60,
        of_day % 60
    )
}

/// A time as SigV4 writes it: `20130524T000000Z` for `x-amz-date`, and `20130524` for the scope
///
/// # Arguments
///
/// * `time` - The time
#[must_use]
pub fn amz_date(time: SystemTime) -> (String, String) {
    let ((year, month, day), of_day) = split(time);
    let date = format!("{year:04}{month:02}{day:02}");
    let stamp = format!(
        "{date}T{:02}{:02}{:02}Z",
        of_day / 3600,
        (of_day % 3600) / 60,
        of_day % 60
    );
    (stamp, date)
}

#[cfg(test)]
mod tests {
    use super::{amz_date, rfc3339};
    use std::time::{Duration, UNIX_EPOCH};

    /// SigV4's forms of a time are the UTC date and time, as AWS's examples write 2013-05-24
    #[test]
    fn amz_date_is_utc() {
        // AWS's examples are at midnight on 2013-05-24, 1369353600 seconds after the epoch
        let midnight = UNIX_EPOCH + Duration::from_secs(1_369_353_600);
        assert_eq!(amz_date(midnight), ("20130524T000000Z".to_string(), "20130524".to_string()));
        // and a second before a leap day's end, as RFC 3339 writes it too
        let leap = UNIX_EPOCH + Duration::from_secs(1_709_251_199);
        assert_eq!(amz_date(leap).0, "20240229T235959Z");
        assert_eq!(rfc3339(leap), "2024-02-29T23:59:59Z");
    }
}
