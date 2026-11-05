//! The four time types, and the parts each of them archives as
//!
//! Each type wraps a chrono value and archives as a small struct of integers, its *parts*, whose
//! archived form rkyv derives an `Ord` for field by field. The parts are chosen so that order is
//! chrono's: an instant is its seconds then its nanoseconds, a date its day count, a time of day
//! its seconds then its nanoseconds. Decoding the parts is fallible, since an archive a client
//! sent can carry parts no chrono value has, and a decode that panicked would end its shard.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use chrono::{DateTime, Datelike, Timelike, Utc};
use deepsize2::{Context, DeepSizeOf};
use rkyv::rancor::{Fallible, Source};
use rkyv::{Archive, Deserialize, Place, Serialize};

use crate::shared::traits::{RkyvSupport, SortKey};

/// Parts an archive carried that name no time chrono can hold
///
/// A row this process archived never holds such parts. Only bytes from elsewhere can, and they
/// are refused where they are decoded, as any archive that will not decode is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrepresentableTime {
    /// Which of the time types the parts were for
    pub kind: &'static str,
    /// The parts, as the archive carried them
    pub parts: String,
}

impl fmt::Display for UnrepresentableTime {
    /// Name the kind of time and the parts that do not name one
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} names no time chrono can hold",
            self.kind, self.parts
        )
    }
}

impl std::error::Error for UnrepresentableTime {}

/// What a [`UtcDateTime`] archives as: whole seconds since the Unix epoch, then the nanoseconds
/// past them
///
/// Its archived form orders by these fields in this order, which is the order of the instants
/// they name: a leap second's nanoseconds run past a billion, after every other nanosecond of
/// the second before it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(
    archived = ArchivedUtcDateTime,
    compare(PartialEq),
    derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)
)]
pub struct UtcDateTimeParts {
    /// Whole seconds since the Unix epoch, negative before it
    pub secs: i64,
    /// Nanoseconds past them, a billion or more within a leap second
    pub nanos: u32,
}

/// What a [`NaiveDateTime`] archives as: its seconds and nanoseconds read as if it were UTC
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(
    archived = ArchivedNaiveDateTime,
    compare(PartialEq),
    derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)
)]
pub struct NaiveDateTimeParts {
    /// Whole seconds since the Unix epoch, read as if the time were UTC, negative before it
    pub secs: i64,
    /// Nanoseconds past them, a billion or more within a leap second
    pub nanos: u32,
}

/// What a [`NaiveDate`] archives as: days since the first day of the common era
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(
    archived = ArchivedNaiveDate,
    compare(PartialEq),
    derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)
)]
pub struct NaiveDateParts {
    /// Days since 0001-01-01, which is day one, so negative before it
    pub days: i32,
}

/// What a [`NaiveTime`] archives as: seconds since midnight, then the nanoseconds past them
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(
    archived = ArchivedNaiveTime,
    compare(PartialEq),
    derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)
)]
pub struct NaiveTimeParts {
    /// Whole seconds since midnight
    pub secs: u32,
    /// Nanoseconds past them, a billion or more within a leap second
    pub nanos: u32,
}

/// A UTC time a row can hold, wrapping chrono's `DateTime<Utc>`
///
/// Ordered, compared and printed as chrono does; a SHQL literal is its RFC 3339 string
/// (`'2026-11-05T09:30:00Z'`), and an offset a literal names is converted to UTC.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct UtcDateTime(pub DateTime<Utc>);

impl UtcDateTime {
    /// The parts this time archives as
    #[must_use]
    pub fn parts(&self) -> UtcDateTimeParts {
        UtcDateTimeParts {
            secs: self.0.timestamp(),
            nanos: self.0.timestamp_subsec_nanos(),
        }
    }

    /// The time a set of parts names, if chrono can hold it
    ///
    /// # Arguments
    ///
    /// * `parts` - The parts to read
    #[must_use]
    pub fn from_parts(parts: UtcDateTimeParts) -> Option<Self> {
        DateTime::from_timestamp(parts.secs, parts.nanos).map(UtcDateTime)
    }
}

/// A date and time with no zone a row can hold, wrapping `chrono::NaiveDateTime`
///
/// Ordered, compared and printed as chrono does; a SHQL literal is its string
/// (`'2026-11-05T09:30:00'`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct NaiveDateTime(pub chrono::NaiveDateTime);

impl NaiveDateTime {
    /// The parts this date and time archives as, read as if it were UTC
    #[must_use]
    pub fn parts(&self) -> NaiveDateTimeParts {
        // as UTC, which only names the instant's seconds and never moves the value
        let utc = self.0.and_utc();
        NaiveDateTimeParts {
            secs: utc.timestamp(),
            nanos: utc.timestamp_subsec_nanos(),
        }
    }

    /// The date and time a set of parts names, if chrono can hold it
    ///
    /// # Arguments
    ///
    /// * `parts` - The parts to read
    #[must_use]
    pub fn from_parts(parts: NaiveDateTimeParts) -> Option<Self> {
        DateTime::from_timestamp(parts.secs, parts.nanos).map(|utc| NaiveDateTime(utc.naive_utc()))
    }
}

/// A date a row can hold, wrapping `chrono::NaiveDate`
///
/// Ordered, compared and printed as chrono does; a SHQL literal is its string (`'2026-11-05'`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct NaiveDate(pub chrono::NaiveDate);

impl NaiveDate {
    /// The parts this date archives as
    #[must_use]
    pub fn parts(&self) -> NaiveDateParts {
        NaiveDateParts {
            days: self.0.num_days_from_ce(),
        }
    }

    /// The date a set of parts names, if chrono can hold it
    ///
    /// # Arguments
    ///
    /// * `parts` - The parts to read
    #[must_use]
    pub fn from_parts(parts: NaiveDateParts) -> Option<Self> {
        chrono::NaiveDate::from_num_days_from_ce_opt(parts.days).map(NaiveDate)
    }
}

/// A time of day a row can hold, wrapping `chrono::NaiveTime`
///
/// Ordered, compared and printed as chrono does; a SHQL literal is its string (`'09:30:00'`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct NaiveTime(pub chrono::NaiveTime);

impl NaiveTime {
    /// The parts this time of day archives as
    #[must_use]
    pub fn parts(&self) -> NaiveTimeParts {
        NaiveTimeParts {
            secs: self.0.num_seconds_from_midnight(),
            nanos: self.0.nanosecond(),
        }
    }

    /// The time of day a set of parts names, if chrono can hold it
    ///
    /// # Arguments
    ///
    /// * `parts` - The parts to read
    #[must_use]
    pub fn from_parts(parts: NaiveTimeParts) -> Option<Self> {
        chrono::NaiveTime::from_num_seconds_from_midnight_opt(parts.secs, parts.nanos)
            .map(NaiveTime)
    }
}

/// Implement everything a time type shares, given its chrono type, its parts and their archived
/// form
///
/// The conversions to and from chrono, `Display`, a `Hash` over its parts, its archive as its
/// parts, a fallible decode, an archived comparison against it, a `DeepSizeOf` of nothing, and
/// [`RkyvSupport`] and [`SortKey`].
macro_rules! time_type {
    ($name:ident, $inner:ty, $parts:ident, $archived:ident, $kind:literal) => {
        impl $name {
            /// The chrono value this holds
            #[must_use]
            pub fn into_inner(self) -> $inner {
                self.0
            }
        }

        impl Deref for $name {
            type Target = $inner;

            /// The chrono value this holds
            fn deref(&self) -> &$inner {
                &self.0
            }
        }

        impl AsRef<$inner> for $name {
            /// The chrono value this holds
            fn as_ref(&self) -> &$inner {
                &self.0
            }
        }

        impl From<$inner> for $name {
            /// Hold a chrono value
            ///
            /// # Arguments
            ///
            /// * `value` - The chrono value to hold
            fn from(value: $inner) -> Self {
                $name(value)
            }
        }

        impl From<$name> for $inner {
            /// The chrono value a time holds
            ///
            /// # Arguments
            ///
            /// * `value` - The time to unwrap
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            /// Print as chrono prints the value
            ///
            /// # Arguments
            ///
            /// * `f` - The formatter to write to
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl Hash for $name {
            /// Hash the parts this time archives as, field by field
            ///
            /// Not chrono's own hash, which hashes how chrono happens to hold a value: a time
            /// that is a partition key places its row by this hash, which is persisted, so it is
            /// defined here, by integers whose meaning chrono cannot change.
            ///
            /// # Arguments
            ///
            /// * `state` - The hasher to feed
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.parts().hash(state);
            }
        }

        impl Archive for $name {
            type Archived = $archived;
            type Resolver = <$parts as Archive>::Resolver;

            /// Write this time's parts where its archive belongs
            ///
            /// # Arguments
            ///
            /// * `resolver` - What serializing its parts produced
            /// * `out` - Where its archive belongs
            fn resolve(&self, resolver: Self::Resolver, out: Place<Self::Archived>) {
                self.parts().resolve(resolver, out);
            }
        }

        impl<S: Fallible + ?Sized> Serialize<S> for $name {
            /// Serialize this time as its parts
            ///
            /// # Arguments
            ///
            /// * `serializer` - The serializer to write with
            fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                self.parts().serialize(serializer)
            }
        }

        impl<D: Fallible + ?Sized> Deserialize<$name, D> for $archived
        where
            D::Error: Source,
        {
            /// Decode the time these parts name, refusing parts that name none
            ///
            /// # Arguments
            ///
            /// * `deserializer` - The deserializer to read with
            fn deserialize(&self, deserializer: &mut D) -> Result<$name, D::Error> {
                // read the integers as they are
                let parts: $parts = Deserialize::<$parts, D>::deserialize(self, deserializer)?;
                // and refuse parts no chrono value has, rather than guess at one
                $name::from_parts(parts).ok_or_else(|| {
                    D::Error::new(UnrepresentableTime {
                        kind: $kind,
                        parts: format!("{parts:?}"),
                    })
                })
            }
        }

        impl PartialEq<$name> for $archived {
            /// Whether an archived time is the time a value holds, as an archived filter asks
            ///
            /// # Arguments
            ///
            /// * `other` - The time to compare against
            fn eq(&self, other: &$name) -> bool {
                *self == other.parts()
            }
        }

        impl DeepSizeOf for $name {
            /// A time is integers held inline, and owns nothing on the heap
            fn deep_size_of_children(&self, _: &mut Context) -> usize {
                0
            }
        }

        impl RkyvSupport for $name {}

        impl SortKey for $name {
            /// A time is integers held inline, and owns nothing on the heap
            #[inline]
            fn heap_bytes(&self) -> usize {
                0
            }
        }
    };
}

time_type!(
    UtcDateTime,
    DateTime<Utc>,
    UtcDateTimeParts,
    ArchivedUtcDateTime,
    "UtcDateTime"
);
time_type!(
    NaiveDateTime,
    chrono::NaiveDateTime,
    NaiveDateTimeParts,
    ArchivedNaiveDateTime,
    "NaiveDateTime"
);
time_type!(
    NaiveDate,
    chrono::NaiveDate,
    NaiveDateParts,
    ArchivedNaiveDate,
    "NaiveDate"
);
time_type!(
    NaiveTime,
    chrono::NaiveTime,
    NaiveTimeParts,
    ArchivedNaiveTime,
    "NaiveTime"
);

#[cfg(test)]
mod tests {
    use super::{
        NaiveDate, NaiveDateParts, NaiveDateTime, NaiveTime, NaiveTimeParts, UtcDateTime,
        UtcDateTimeParts,
    };
    use crate::shared::traits::{check_sort_key_order, RkyvSupport};
    use chrono::{DateTime, Utc};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    /// The seed every generated set is drawn from, so a failure names values a rerun finds again
    const SEED: u64 = 0x5eed_0131;

    /// How many generated values each type is checked over, beside its edges
    const GENERATED: usize = 2_000;

    /// A UTC time from its seconds and nanoseconds, which a test knows chrono holds
    ///
    /// # Arguments
    ///
    /// * `secs` - Seconds since the Unix epoch
    /// * `nanos` - Nanoseconds past them
    fn utc(secs: i64, nanos: u32) -> UtcDateTime {
        UtcDateTime(DateTime::from_timestamp(secs, nanos).expect("a time chrono holds"))
    }

    /// The edges of a UTC time: chrono's least and greatest, the epoch and either side of it, a
    /// second's last nanosecond, and a leap second's
    fn utc_edges() -> Vec<UtcDateTime> {
        // 2016-12-31T23:59:59 UTC, the last leap second, and its sixtieth second
        let leap_minute = 1_483_228_799;
        vec![
            UtcDateTime(DateTime::<Utc>::MIN_UTC),
            utc(-1, 999_999_999),
            utc(0, 0),
            utc(0, 1),
            utc(leap_minute, 999_999_999),
            utc(leap_minute, 1_000_000_000),
            utc(leap_minute, 1_999_999_999),
            utc(leap_minute + 1, 0),
            utc(1_793_000_000, 500),
            UtcDateTime(DateTime::<Utc>::MAX_UTC),
        ]
    }

    /// A seeded UTC time across most of chrono's range, now and then inside a leap second
    ///
    /// # Arguments
    ///
    /// * `rng` - The seeded generator to draw from
    fn any_utc(rng: &mut StdRng) -> UtcDateTime {
        // a second at the end of a minute may hold a leap second's nanoseconds
        let secs = rng.gen_range(-8_000_000_000_000i64..8_000_000_000_000) / 60 * 60 + 59;
        let nanos = rng.gen_range(0..2_000_000_000u32);
        utc(secs, nanos)
    }

    /// Every time type orders as it archives, at its edges and over a seeded spread
    #[test]
    fn every_time_type_orders_as_it_archives() {
        let mut rng = StdRng::seed_from_u64(SEED);
        // instants, leap seconds among them
        let mut instants = utc_edges();
        instants.extend((0..GENERATED).map(|_| any_utc(&mut rng)));
        check_sort_key_order(&instants).expect("a UTC time orders as it archives");
        // the same instants without their zone
        let naive: Vec<NaiveDateTime> = instants
            .iter()
            .map(|time| NaiveDateTime(time.naive_utc()))
            .collect();
        check_sort_key_order(&naive).expect("a date and time orders as it archives");
        // dates, chrono's least and greatest and either side of the first day of the era
        let mut dates: Vec<NaiveDate> = [chrono::NaiveDate::MIN, chrono::NaiveDate::MAX]
            .into_iter()
            .chain(
                [-1, 0, 1, 739_000]
                    .into_iter()
                    .filter_map(chrono::NaiveDate::from_num_days_from_ce_opt),
            )
            .map(NaiveDate)
            .collect();
        dates.extend(instants.iter().map(|time| NaiveDate(time.date_naive())));
        check_sort_key_order(&dates).expect("a date orders as it archives");
        // times of day, midnight, a leap second and the last nanosecond
        let mut times: Vec<NaiveTime> = [
            (0, 0),
            (86_399, 999_999_999),
            (86_399, 1_999_999_999),
            (43_200, 1),
        ]
        .into_iter()
        .filter_map(|(secs, nanos)| {
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
        })
        .map(NaiveTime)
        .collect();
        times.extend(instants.iter().map(|time| NaiveTime(time.time())));
        check_sort_key_order(&times).expect("a time of day orders as it archives");
    }

    /// Every time type comes back from its archive, and from serde, as the value it was
    #[test]
    fn every_time_type_round_trips() {
        for time in utc_edges() {
            // through its archive
            let bytes = RkyvSupport::serialize(&time).expect("a time archives");
            let archived = UtcDateTime::access(&bytes).expect("an archived time validates");
            assert_eq!(
                <UtcDateTime as RkyvSupport>::deserialize(archived).expect("a time decodes"),
                time
            );
            // through serde, as chrono's own string
            let value = serde_json::to_value(time).expect("a time serializes");
            assert!(value.is_string(), "{time:?} serialized as {value}");
            assert_eq!(
                serde_json::from_value::<UtcDateTime>(value).expect("a time reads back"),
                time
            );
            // and the three naive types the same way
            let naive = NaiveDateTime(time.naive_utc());
            let bytes = RkyvSupport::serialize(&naive).expect("a date and time archives");
            let archived =
                NaiveDateTime::access(&bytes).expect("an archived date and time validates");
            assert_eq!(
                <NaiveDateTime as RkyvSupport>::deserialize(archived)
                    .expect("a date and time decodes"),
                naive
            );
            let date = NaiveDate(time.date_naive());
            let bytes = RkyvSupport::serialize(&date).expect("a date archives");
            assert_eq!(
                <NaiveDate as RkyvSupport>::deserialize(NaiveDate::access(&bytes).unwrap())
                    .unwrap(),
                date
            );
            let of_day = NaiveTime(time.time());
            let bytes = RkyvSupport::serialize(&of_day).expect("a time of day archives");
            assert_eq!(
                <NaiveTime as RkyvSupport>::deserialize(NaiveTime::access(&bytes).unwrap())
                    .unwrap(),
                of_day
            );
        }
        // a SHQL literal with an offset is read as the instant it names, in UTC
        let parsed: UtcDateTime =
            serde_json::from_value(serde_json::json!("2026-11-05T11:30:00+02:00")).unwrap();
        assert_eq!(parsed.to_rfc3339(), "2026-11-05T09:30:00+00:00");
    }

    /// Parts no chrono value has validate, since every bit pattern of an integer does, and are
    /// refused when they are decoded rather than panicking
    #[test]
    fn parts_chrono_cannot_hold_fail_to_decode() {
        // a nanosecond count past a leap second's, and a second count past chrono's range
        for parts in [
            UtcDateTimeParts {
                secs: 0,
                nanos: 2_000_000_000,
            },
            UtcDateTimeParts {
                secs: 10,
                nanos: 1_000_000_000,
            },
            UtcDateTimeParts {
                secs: i64::MAX,
                nanos: 0,
            },
        ] {
            // archived as the parts a crafted archive would carry
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&parts).unwrap();
            let archived = UtcDateTime::access(&bytes).expect("any integers validate");
            let refused = <UtcDateTime as RkyvSupport>::deserialize(archived)
                .expect_err("parts chrono cannot hold decoded");
            assert!(
                refused.to_string().contains("UtcDateTime"),
                "refused as {refused}"
            );
        }
        // a day past chrono's last
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&NaiveDateParts { days: i32::MAX }).unwrap();
        assert!(
            <NaiveDate as RkyvSupport>::deserialize(NaiveDate::access(&bytes).unwrap()).is_err()
        );
        // and a second past midnight's last
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&NaiveTimeParts {
            secs: 86_400,
            nanos: 0,
        })
        .unwrap();
        assert!(
            <NaiveTime as RkyvSupport>::deserialize(NaiveTime::access(&bytes).unwrap()).is_err()
        );
    }

    /// An archived time equals the time it archived and no other, as an archived filter asks
    #[test]
    fn an_archived_time_equals_the_time_it_archived() {
        let time = utc(1_793_000_000, 500);
        let bytes = RkyvSupport::serialize(&time).unwrap();
        let archived = UtcDateTime::access(&bytes).unwrap();
        assert!(
            *archived == time,
            "an archived time is not the time it archived"
        );
        assert!(
            *archived != utc(1_793_000_000, 501),
            "an archived time equals another"
        );
    }

    /// A time hashes as its parts, field by field, whatever chrono holds inside it
    #[test]
    fn a_times_hash_is_its_parts() {
        // what a hasher fed the seconds and then the nanoseconds produces
        let time = utc(1_793_000_000, 500);
        let mut by_parts = DefaultHasher::new();
        1_793_000_000i64.hash(&mut by_parts);
        500u32.hash(&mut by_parts);
        // is what the time's own hash produces
        let mut by_time = DefaultHasher::new();
        time.hash(&mut by_time);
        assert_eq!(by_time.finish(), by_parts.finish());
    }
}
